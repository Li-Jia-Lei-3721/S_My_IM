// bench/bench_client.cpp
#include "../src/protocol/message.h"
#include "../third_party/json.hpp"

#include <algorithm>
#include <arpa/inet.h>
#include <atomic>
#include <cctype>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <fcntl.h>
#include <iostream>
#include <netinet/in.h>
#include <string>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <unordered_map>
#include <vector>

using namespace std;
using json = nlohmann::json;

static atomic<uint64_t> g_sent{0};
static atomic<uint64_t> g_recv{0};
static atomic<uint64_t> g_failed{0};
static atomic<uint64_t> g_login_ok{0};
static atomic<uint64_t> g_login_fail{0};
static atomic<bool> g_start{false};
static atomic<int64_t> g_start_us{0}; // 起点（epoch 微秒），全 worker 共享

struct Config {
	string host;
	int port;
	int count;
	int interval_ms;
	int duration_sec;
	int worker_count;
	string prefix = "bench_";
};

enum ConnState {
	CS_CONNECTING,
	CS_LOGIN_SENT,
	CS_LOGGED_IN,
	CS_CLOSED
};

struct ClientConn {
	int fd = -1;
	int idx = -1;
	ConnState state = CS_CONNECTING;
	string recv_buffer;
	string send_buffer;
	size_t send_offset = 0;
	chrono::steady_clock::time_point next_send_time;
	bool login_ok = false;
};

static string packPacket(MsgType type, const json &j) {
	string body = j.dump();
	MessageHeader h;
	h.magic = 0x12345678;
	h.length = body.size();
	h.type = type;
	h.pack();
	string pkt((char *)&h, sizeof(h));
	pkt += body;
	return pkt;
}

// ============ 单个 Worker ============
class Worker {
  public:
	Worker(int id, int wcount, const Config &cfg) : id_(id), wcount_(wcount), cfg_(cfg) {}

	~Worker() {
		for (auto &kv : conns_) {
			ClientConn *c = kv.second;
			if (c->fd != -1) {
				epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, c->fd, nullptr);
				close(c->fd);
			}
			delete c;
		}
		if (epoll_fd_ != -1) close(epoll_fd_);
	}

	void run() {
		epoll_fd_ = epoll_create1(0);
		if (epoll_fd_ == -1) {
			cerr << "[worker " << id_ << "] epoll_create1 失败\n";
			return;
		}

		// 交错分配 idx：worker 0 拿 0,8,16..., worker 1 拿 1,9,17...
		for (int i = id_; i < cfg_.count; i += wcount_) {
			createConnection(i);
		}

		waitAllLoggedIn();

		// 等主线程启动信号
		while (!g_start.load())
			this_thread::sleep_for(chrono::milliseconds(5));

		benchLoop();
	}

  private:
	void createConnection(int idx) {
		sockaddr_in addr{};
		addr.sin_family = AF_INET;
		addr.sin_port = htons(cfg_.port);
		inet_pton(AF_INET, cfg_.host.c_str(), &addr.sin_addr);

		int fd = socket(AF_INET, SOCK_STREAM, 0);
		if (fd < 0) {
			g_login_fail++;
			return;
		}

		int flags = fcntl(fd, F_GETFL, 0);
		fcntl(fd, F_SETFL, flags | O_NONBLOCK);

		// 【关键】调大接收缓冲，减少下行背压
		int bufsz = 2 * 1024 * 1024; // 2 MB
		setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &bufsz, sizeof(bufsz));
		setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &bufsz, sizeof(bufsz));

		int ret = connect(fd, (sockaddr *)&addr, sizeof(addr));
		if (ret < 0 && errno != EINPROGRESS) {
			cerr << "[connect失败] bench_" << idx << " : " << strerror(errno) << "\n";
			close(fd);
			g_login_fail++;
			return;
		}

		ClientConn *conn = new ClientConn();
		conn->fd = fd;
		conn->idx = idx;
		conn->state = CS_CONNECTING;

		epoll_event ev;
		ev.events = EPOLLOUT;
		ev.data.ptr = conn;
		if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, fd, &ev) == -1) {
			close(fd);
			delete conn;
			g_login_fail++;
			return;
		}
		conns_[fd] = conn;
	}

	void waitAllLoggedIn() {
		auto deadline = chrono::steady_clock::now() + chrono::seconds(120);
		while (chrono::steady_clock::now() < deadline) {
			int pending = 0;
			for (auto &kv : conns_) {
				auto s = kv.second->state;
				if (s == CS_CONNECTING || s == CS_LOGIN_SENT) pending++;
			}
			if (pending == 0) return;

			epoll_event events[512];
			int nfds = epoll_wait(epoll_fd_, events, 512, 500);
			if (nfds == -1) {
				if (errno == EINTR) continue;
				break;
			}
			for (int i = 0; i < nfds; i++) {
				ClientConn *c = (ClientConn *)events[i].data.ptr;
				if (!c || c->fd == -1) continue;
				processEvent(c, events[i].events);
			}
		}
		cerr << "[worker " << id_ << "] 登录阶段超时\n";
	}

	void benchLoop() {
		auto start = chrono::steady_clock::time_point(chrono::microseconds(g_start_us.load()));
		auto send_end = start + chrono::seconds(cfg_.duration_sec);
		auto loop_end = send_end + chrono::seconds(10); // 3 秒冷却

		// 活跃连接列表
		vector<ClientConn *> active;
		for (auto &kv : conns_) {
			if (kv.second->state == CS_LOGGED_IN) active.push_back(kv.second);
		}
		sort(active.begin(), active.end(), [](ClientConn *a, ClientConn *b) {
			return a->idx < b->idx;
		});
		// 错开首次发送
		for (size_t i = 0; i < active.size(); i++) {
			int64_t off_ms = (int64_t)i * cfg_.interval_ms / (int64_t)max<size_t>(active.size(), 1);
			active[i]->next_send_time = start + chrono::milliseconds(off_ms);
		}

		while (chrono::steady_clock::now() < loop_end) {
			auto now = chrono::steady_clock::now();

			int timeout_ms = 50;
			if (now < send_end) {
				for (auto *c : active) {
					auto dt = chrono::duration_cast<chrono::milliseconds>(c->next_send_time - now).count();
					int64_t t = dt <= 0 ? 1 : dt;
					if (t < timeout_ms) timeout_ms = (int)t;
				}
			}

			epoll_event events[512];
			int nfds = epoll_wait(epoll_fd_, events, 512, timeout_ms);
			if (nfds == -1) {
				if (errno == EINTR) continue;
				break;
			}
			for (int i = 0; i < nfds; i++) {
				ClientConn *c = (ClientConn *)events[i].data.ptr;
				if (!c || c->fd == -1) continue;
				processEvent(c, events[i].events);
			}

			// 到期发送
			auto now2 = chrono::steady_clock::now();
			if (now2 < send_end) {
				for (auto *c : active) {
					if (c->state != CS_LOGGED_IN) continue;
					while (c->next_send_time <= now2 && now2 < send_end) {
						if (!sendChatMsg(c)) break;
						c->next_send_time += chrono::milliseconds(cfg_.interval_ms);
					}
				}
			}
		}
	}

	void processEvent(ClientConn *c, uint32_t evs) {
		if (evs & (EPOLLHUP | EPOLLERR)) {
			closeConn(c);
			return;
		}
		if (evs & EPOLLOUT) {
			if (c->state == CS_CONNECTING) {
				int err = 0;
				socklen_t len = sizeof(err);
				getsockopt(c->fd, SOL_SOCKET, SO_ERROR, &err, &len);
				if (err != 0) {
					closeConn(c);
					return;
				}
				if (!sendLogin(c)) {
					closeConn(c);
					return;
				}
			}
			if (!c->send_buffer.empty() && c->send_offset < c->send_buffer.size()) {
				handleWrite(c);
			}
		}
		if ((evs & EPOLLIN) && c->fd != -1) {
			handleRead(c);
		}
	}

	bool sendLogin(ClientConn *c) {
		json req = {{"username", cfg_.prefix + to_string(c->idx)}, {"password", "123456"}};
		string pkt = packPacket(MSG_LOGIN, req);
		c->state = CS_LOGIN_SENT;

		int n = send(c->fd, pkt.data(), pkt.size(), MSG_NOSIGNAL);
		if (n < 0) {
			if (errno == EAGAIN || errno == EWOULDBLOCK) {
				c->send_buffer = pkt;
				c->send_offset = 0;
				updateEpoll(c, EPOLLIN | EPOLLOUT);
				return true;
			}
			return false;
		}
		if ((size_t)n < pkt.size()) {
			c->send_buffer = pkt;
			c->send_offset = n;
			updateEpoll(c, EPOLLIN | EPOLLOUT);
		}
		else {
			updateEpoll(c, EPOLLIN);
		}
		return true;
	}

	bool sendChatMsg(ClientConn *c) {
		if (c->state != CS_LOGGED_IN) return false;

		json j = {{"message", "bench_" + to_string(c->idx)}};
		string pkt = packPacket(MSG_CHAT, j);

		// 有残留积压：直接 append
		if (!c->send_buffer.empty() && c->send_offset < c->send_buffer.size()) {
			c->send_buffer += pkt;
			g_sent++;
			updateEpoll(c, EPOLLIN | EPOLLOUT);
			return true;
		}

		int n = send(c->fd, pkt.data(), pkt.size(), MSG_NOSIGNAL);
		if (n < 0) {
			if (errno == EAGAIN || errno == EWOULDBLOCK) {
				c->send_buffer = pkt;
				c->send_offset = 0;
				g_sent++;
				updateEpoll(c, EPOLLIN | EPOLLOUT);
				return true;
			}
			closeConn(c);
			return false;
		}
		g_sent++;
		if ((size_t)n < pkt.size()) {
			c->send_buffer = pkt;
			c->send_offset = n;
			updateEpoll(c, EPOLLIN | EPOLLOUT);
		}
		return true;
	}

	void handleRead(ClientConn *c) {
		char tmp[65536];
		while (true) {
			int n = recv(c->fd, tmp, sizeof(tmp), 0);
			if (n > 0) {
				c->recv_buffer.append(tmp, n);
				// 大包：一次 recv 完继续读，最多读 8 次（约 512KB）再让出给其他 socket
				// 避免单个 socket 霸占 worker
				if (c->recv_buffer.size() > 1024 * 1024) break;
			}
			else if (n == 0) {
				closeConn(c);
				return;
			}
			else {
				if (errno == EAGAIN || errno == EWOULDBLOCK) break;
				if (errno == EINTR) continue;
				closeConn(c);
				return;
			}
		}
		parsePackets(c);
	}

	void parsePackets(ClientConn *c) {
		while (c->recv_buffer.size() >= sizeof(MessageHeader)) {
			MessageHeader h;
			memcpy(&h, c->recv_buffer.data(), sizeof(h));
			h.unpack();

			if (h.magic != 0x12345678 || h.length > 10 * 1024 * 1024) {
				closeConn(c);
				return;
			}
			if (c->recv_buffer.size() < sizeof(MessageHeader) + h.length) break;

			string body = c->recv_buffer.substr(sizeof(MessageHeader), h.length);
			c->recv_buffer.erase(0, sizeof(MessageHeader) + h.length);

			if (c->state == CS_LOGIN_SENT) {
				if (h.type != MSG_SYSTEM) continue;
				try {
					json j = json::parse(body);
					string msg = j.value("message", "");
					if (msg.find("登录成功") != string::npos) {
						c->state = CS_LOGGED_IN;
						c->login_ok = true;
						g_login_ok++;
						updateEpoll(c, EPOLLIN);
					}
					else {
						cerr << "[登录失败] bench_" << c->idx << " : " << msg << "\n";
						closeConn(c);
						return;
					}
				}
				catch (...) {
					closeConn(c);
					return;
				}
			}
			else if (c->state == CS_LOGGED_IN) {
				if (h.type == MSG_CHAT) g_recv++;
			}
		}
	}

	void handleWrite(ClientConn *c) {
		if (c->send_buffer.empty() || c->send_offset >= c->send_buffer.size()) {
			c->send_buffer.clear();
			c->send_offset = 0;
			updateEpoll(c, EPOLLIN);
			return;
		}
		while (c->send_offset < c->send_buffer.size()) {
			int n = send(c->fd, c->send_buffer.data() + c->send_offset, c->send_buffer.size() - c->send_offset,
						 MSG_NOSIGNAL);
			if (n > 0) {
				c->send_offset += n;
			}
			else if (n < 0) {
				if (errno == EAGAIN || errno == EWOULDBLOCK) return;
				closeConn(c);
				return;
			}
			else {
				closeConn(c);
				return;
			}
		}
		c->send_buffer.clear();
		c->send_offset = 0;
		updateEpoll(c, EPOLLIN);
	}

	void updateEpoll(ClientConn *c, uint32_t evs) {
		if (c->fd == -1) return;
		epoll_event ev;
		ev.events = evs;
		ev.data.ptr = c;
		epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, c->fd, &ev);
	}

	void closeConn(ClientConn *c) {
		if (c->fd == -1) return;
		if (!c->login_ok) g_login_fail++;
		else g_failed++; // 登录后被断，也算失败
		epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, c->fd, nullptr);
		close(c->fd);
		c->fd = -1;
		c->state = CS_CLOSED;
	}

	int id_;
	int wcount_;
	Config cfg_;
	int epoll_fd_ = -1;
	unordered_map<int, ClientConn *> conns_;
};

// ============ 注册（阻塞 + 超时）============
static int connectBlocking(const string &host, int port) {
	int fd = socket(AF_INET, SOCK_STREAM, 0);
	if (fd < 0) return -1;
	sockaddr_in addr{};
	addr.sin_family = AF_INET;
	addr.sin_port = htons(port);
	inet_pton(AF_INET, host.c_str(), &addr.sin_addr);
	if (connect(fd, (sockaddr *)&addr, sizeof(addr)) < 0) {
		close(fd);
		return -1;
	}
	timeval tv{10, 0};
	setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
	setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
	return fd;
}

static bool recvFullPacket(int fd, MsgType &type, string &body) {
	MessageHeader h;
	size_t got = 0;
	while (got < sizeof(h)) {
		int n = recv(fd, (char *)&h + got, sizeof(h) - got, 0);
		if (n <= 0) return false;
		got += n;
	}
	h.unpack();
	if (h.magic != 0x12345678) return false;
	body.resize(h.length);
	got = 0;
	while (got < h.length) {
		int n = recv(fd, &body[got], h.length - got, 0);
		if (n <= 0) return false;
		got += n;
	}
	type = (MsgType)h.type;
	return true;
}

static bool sendFullPacket(int fd, MsgType type, const json &j) {
	string pkt = packPacket(type, j);
	size_t sent = 0;
	while (sent < pkt.size()) {
		int n = send(fd, pkt.data() + sent, pkt.size() - sent, MSG_NOSIGNAL);
		if (n <= 0) return false;
		sent += n;
	}
	return true;
}

static string extractCode(const string &msg) {
	string key = "验证码是: ";
	size_t pos = msg.find(key);
	if (pos == string::npos) return "";
	size_t start = pos + key.size();
	string code;
	for (size_t i = start; i < msg.size() && code.size() < 6; i++) {
		if (isdigit((unsigned char)msg[i])) code += msg[i];
		else if (!code.empty()) break;
	}
	return code.size() == 6 ? code : "";
}

static bool registerOne(const string &host, int port, const string &username, const string &password) {
	int fd = connectBlocking(host, port);
	if (fd < 0) {
		cerr << "[注册失败] " << username << " : connect 失败\n";
		return false;
	}
	json req = {{"username", username}};
	if (!sendFullPacket(fd, MSG_GET_CODE, req)) {
		close(fd);
		return false;
	}
	string code, sys_msg;
	while (true) {
		MsgType t;
		string b;
		if (!recvFullPacket(fd, t, b)) {
			close(fd);
			return false;
		}
		if (t != MSG_SYSTEM) continue;
		try {
			json resp = json::parse(b);
			sys_msg = resp.value("message", "");
			code = extractCode(sys_msg);
		}
		catch (...) {
			continue;
		}
		break;
	}
	if (code.empty()) {
		cerr << "[注册失败] " << username << " : " << sys_msg << "\n";
		close(fd);
		return false;
	}
	json reg = {{"username", username}, {"password", password}, {"code", code}};
	if (!sendFullPacket(fd, MSG_REGISTER, reg)) {
		close(fd);
		return false;
	}
	while (true) {
		MsgType t;
		string b;
		if (!recvFullPacket(fd, t, b)) {
			close(fd);
			return false;
		}
		if (t != MSG_SYSTEM) continue;
		try {
			json resp = json::parse(b);
			string msg = resp.value("message", "");
			bool ok = (msg.find("注册成功") != string::npos);
			if (!ok) cerr << "[注册失败] " << username << " : " << msg << "\n";
			close(fd);
			return ok;
		}
		catch (...) {
			continue;
		}
	}
}

// ============ main ============
int main(int argc, char **argv) {
	if (argc < 5) {
		cerr << "用法:\n"
			 << "  " << argv[0] << " register <host> <port> <count>\n"
			 << "  " << argv[0] << " bench <host> <port> <count> <interval_ms> <duration_sec> [worker_count]\n";
		return 1;
	}

	string mode = argv[1];
	string host = argv[2];
	int port = atoi(argv[3]);
	int count = atoi(argv[4]);

	if (mode == "register") {
		cout << "开始注册 " << count << " 个用户...\n";
		int ok = 0;
		for (int i = 0; i < count; i++) {
			string uname = "bench_" + to_string(i);
			if (registerOne(host, port, uname, "123456")) {
				ok++;
				cout << "[" << (i + 1) << "/" << count << "] " << uname << " 注册成功\n";
			}
			else {
				cerr << "[" << (i + 1) << "/" << count << "] " << uname << " 注册失败\n";
			}
		}
		cout << "注册完成：成功 " << ok << " / " << count << "\n";
		return 0;
	}

	if (mode != "bench" || argc < 7) {
		cerr << "bench 模式: <host> <port> <count> <interval_ms> <duration_sec> [worker_count=8]\n";
		return 1;
	}

	Config cfg;
	cfg.host = host;
	cfg.port = port;
	cfg.count = count;
	cfg.interval_ms = atoi(argv[5]);
	cfg.duration_sec = atoi(argv[6]);
	cfg.worker_count = (argc >= 8) ? atoi(argv[7]) : 8;

	cout << "========== 多线程 epoll 压测 ==========\n"
		 << "连接数: " << cfg.count << "  worker 数: " << cfg.worker_count << "\n"
		 << "间隔: " << cfg.interval_ms << "ms  时长: " << cfg.duration_sec << "s\n"
		 << "======================================\n";

	auto t0 = chrono::steady_clock::now();

	// 启动所有 worker（各自连接 + 登录）
	vector<unique_ptr<Worker>> workers;
	vector<thread> threads;
	for (int i = 0; i < cfg.worker_count; i++) {
		workers.push_back(make_unique<Worker>(i, cfg.worker_count, cfg));
		threads.emplace_back([w = workers.back().get()] {
			w->run();
		});
	}

	// 等所有 worker 登录完成（login_ok + login_fail == count）或超时
	auto login_deadline = chrono::steady_clock::now() + chrono::seconds(120);
	while (chrono::steady_clock::now() < login_deadline) {
		if (g_login_ok.load() + g_login_fail.load() >= (uint64_t)cfg.count) break;
		this_thread::sleep_for(chrono::milliseconds(50));
	}

	auto login_elapsed = chrono::duration<double>(chrono::steady_clock::now() - t0).count();
	cout << "[登录完成] 成功 " << g_login_ok.load() << " / " << cfg.count << "，耗时 " << login_elapsed << " 秒\n\n";

	if (g_login_ok.load() == 0) {
		cerr << "全部登录失败，退出\n";
		g_start = true;
		for (auto &t : threads)
			t.join();
		return 1;
	}

	// 统一起点
	auto start_tp = chrono::steady_clock::now();
	g_start_us.store(chrono::duration_cast<chrono::microseconds>(start_tp.time_since_epoch()).count());
	g_start.store(true);

	// 进度打印（主线程）
	int total_sec = cfg.duration_sec + 10;
	for (int s = 1; s <= total_sec; s++) {
		this_thread::sleep_for(chrono::seconds(1));
		cout << "[进度 " << s << "/" << total_sec << "] "
			 << "发送=" << g_sent.load() << " 接收=" << g_recv.load() << " 失败=" << g_failed.load() << "\n";
	}

	for (auto &t : threads)
		t.join();

	double wall = chrono::duration<double>(chrono::steady_clock::now() - t0).count();

	uint64_t sent = g_sent.load();
	uint64_t recv = g_recv.load();
	uint64_t failed = g_failed.load();
	uint64_t login_ok = g_login_ok.load();

	double qps_win = cfg.duration_sec;
	uint64_t expect = sent * (login_ok > 0 ? login_ok - 1 : 0);
	double loss = expect > 0 ? (1.0 - (double)recv / expect) * 100.0 : 0;

	cout << "\n========== 压测结果（多线程 epoll）==========\n"
		 << "总耗时: " << wall << " 秒\n"
		 << "在线连接数: " << login_ok << " / " << cfg.count << "\n"
		 << "发送消息数: " << sent << "\n"
		 << "发送 QPS: " << (sent / qps_win) << "\n"
		 << "接收消息数: " << recv << "\n"
		 << "接收 QPS: " << (recv / qps_win) << "\n"
		 << "理论接收数: " << expect << " (发送数 × " << (login_ok > 0 ? login_ok - 1 : 0) << ")\n"
		 << "丢包率: " << loss << " %\n"
		 << "失败数: " << failed << "\n"
		 << "============================================\n";
	return 0;
}

/*// bench/bench_client.cpp
// 压测客户端：模拟大量用户连接、登录、发消息
//
// 用法：
//   ./bench_client register <host> <port> <count>
//   ./bench_client bench    <host> <port> <count> <interval_ms> <duration_sec>

#include "../src/protocol/message.h"
#include "../third_party/json.hpp"

#include <arpa/inet.h>
#include <atomic>
#include <cctype>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <fcntl.h>
#include <iostream>
#include <netinet/in.h>
#include <string>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <vector>

using namespace std;
using json = nlohmann::json;

// ============ 全局统计（多线程共享，用 atomic 保证线程安全）============
atomic<uint64_t> g_sent{0};	  // 成功发出的消息数
atomic<uint64_t> g_recv{0};	  // 收到的 MSG_CHAT 消息数
atomic<uint64_t> g_failed{0}; // 失败次数（连接失败、登录失败、send 失败）

// ============ 连接服务器（带 3 秒超时），返回 fd，失败返回 -1 ============
int connectServer(const string &host, int port, int timeout_sec = 3) {
	int fd = socket(AF_INET, SOCK_STREAM, 0);
	if (fd < 0) return -1;

	int flags = fcntl(fd, F_GETFL, 0);
	fcntl(fd, F_SETFL, flags | O_NONBLOCK);

	sockaddr_in addr{};
	addr.sin_family = AF_INET;
	addr.sin_port = htons(port);
	inet_pton(AF_INET, host.c_str(), &addr.sin_addr);

	int ret = connect(fd, (sockaddr *)&addr, sizeof(addr));
	if (ret < 0) {
		if (errno != EINPROGRESS) {
			close(fd);
			return -1;
		}
		fd_set wfds;
		FD_ZERO(&wfds);
		FD_SET(fd, &wfds);
		timeval tv{timeout_sec, 0};
		int sel = select(fd + 1, nullptr, &wfds, nullptr, &tv);
		if (sel <= 0) {
			close(fd);
			return -1;
		}
		int err = 0;
		socklen_t len = sizeof(err);
		getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &len);
		if (err != 0) {
			close(fd);
			return -1;
		}
	}

	fcntl(fd, F_SETFL, flags);
	return fd;
}

// ============ 发一个完整包（阻塞直到发完）============
bool sendPacket(int fd, MsgType type, const json &j) {
	string body = j.dump();

	MessageHeader h;
	h.magic = 0x12345678;
	h.length = body.size();
	h.type = type;
	h.pack();

	string packet((char *)&h, sizeof(h));
	packet += body;

	size_t sent = 0;
	while (sent < packet.size()) {
		int n = send(fd, packet.data() + sent, packet.size() - sent, MSG_NOSIGNAL);
		if (n <= 0) return false;
		sent += n;
	}
	return true;
}

// ============ 阻塞收一个完整包 ============
bool recvPacket(int fd, MsgType &type, string &body) {
	MessageHeader h;
	size_t got = 0;
	while (got < sizeof(h)) {
		int n = recv(fd, (char *)&h + got, sizeof(h) - got, 0);
		if (n <= 0) return false;
		got += n;
	}
	h.unpack();
	if (h.magic != 0x12345678) return false;

	body.resize(h.length);
	got = 0;
	while (got < h.length) {
		int n = recv(fd, &body[got], h.length - got, 0);
		if (n <= 0) return false;
		got += n;
	}
	type = (MsgType)h.type;
	return true;
}

// ============ 从系统消息里提取验证码 ============
string extractCode(const string &msg) {
	string key = "验证码是: ";
	size_t pos = msg.find(key);
	if (pos == string::npos) return "";

	size_t start = pos + key.size();
	string code;
	for (size_t i = start; i < msg.size() && code.size() < 6; i++) {
		if (isdigit((unsigned char)msg[i])) code += msg[i];
		else if (!code.empty()) break;
	}
	return code.size() == 6 ? code : "";
}

// ============ 注册一个用户 ============
bool registerOne(const string &host, int port, const string &username, const string &password) {
	int fd = connectServer(host, port);
	if (fd < 0) {
		cerr << "[注册失败] " << username << " : connect 失败\n";
		return false;
	}

	json req = {{"username", username}};
	if (!sendPacket(fd, MSG_GET_CODE, req)) {
		cerr << "[注册失败] " << username << " : 请求验证码 send 失败\n";
		close(fd);
		return false;
	}

	// 循环读，只认 MSG_SYSTEM（跳过可能的群聊广播）
	string sys_msg, code;
	while (true) {
		MsgType t;
		string b;
		if (!recvPacket(fd, t, b)) {
			cerr << "[注册失败] " << username << " : 收验证码断开\n";
			close(fd);
			return false;
		}
		if (t != MSG_SYSTEM) continue;
		try {
			json resp = json::parse(b);
			sys_msg = resp.value("message", "");
			code = extractCode(sys_msg);
		}
		catch (...) {
			continue;
		}
		break;
	}

	if (code.empty()) {
		cerr << "[注册失败] " << username << " : 没拿到验证码，服务端回: " << sys_msg << "\n";
		close(fd);
		return false;
	}

	json reg = {{"username", username}, {"password", password}, {"code", code}};
	if (!sendPacket(fd, MSG_REGISTER, reg)) {
		cerr << "[注册失败] " << username << " : 发注册请求失败\n";
		close(fd);
		return false;
	}

	// 循环读，直到拿到注册结果
	while (true) {
		MsgType t;
		string b;
		if (!recvPacket(fd, t, b)) {
			cerr << "[注册失败] " << username << " : 收注册结果断开\n";
			close(fd);
			return false;
		}
		if (t != MSG_SYSTEM) continue;

		string reason;
		bool ok = false;
		try {
			json resp = json::parse(b);
			reason = resp.value("message", "");
			ok = (reason.find("注册成功") != string::npos);
		}
		catch (...) {
			continue;
		}

		close(fd);
		if (!ok) cerr << "[注册失败] " << username << " : " << reason << "\n";
		return ok;
	}
}

// ============ 登录一个用户（成功后返回 fd，失败 -1）============
// 【关键修复】循环 recv，只认 MSG_SYSTEM，跳过其他客户端广播的 MSG_CHAT
int loginOne(const string &host, int port, const string &username, const string &password) {
	int fd = connectServer(host, port);
	if (fd < 0) {
		cerr << "[登录失败] " << username << " : connect 失败（服务端没起？backlog 满？）\n";
		return -1;
	}

	json req = {{"username", username}, {"password", password}};
	if (!sendPacket(fd, MSG_LOGIN, req)) {
		cerr << "[登录失败] " << username << " : send 失败\n";
		close(fd);
		return -1;
	}

	while (true) {
		MsgType t;
		string b;
		if (!recvPacket(fd, t, b)) {
			cerr << "[登录失败] " << username << " : recv 断开\n";
			close(fd);
			return -1;
		}

		// ← 关键：不是系统消息，丢掉重读
		if (t != MSG_SYSTEM) continue;

		string reason;
		bool ok = false;
		try {
			json resp = json::parse(b);
			reason = resp.value("message", "");
			ok = (reason.find("登录成功") != string::npos);
		}
		catch (...) {
			continue;
		}

		if (ok) return fd;

		cerr << "[登录失败] " << username << " : " << reason << "\n";
		close(fd);
		return -1;
	}
}

// ============ 非阻塞收数据 + 切包统计 ============
void drainRecv(int fd, string &buf) {
	char tmp[65536];

	while (true) {
		int n = recv(fd, tmp, sizeof(tmp), MSG_DONTWAIT);
		if (n > 0) buf.append(tmp, n);
		else break;
	}

	while (buf.size() >= sizeof(MessageHeader)) {
		MessageHeader h;
		memcpy(&h, buf.data(), sizeof(h));
		h.unpack();

		if (h.magic != 0x12345678) {
			buf.clear();
			return;
		}
		if (buf.size() < sizeof(MessageHeader) + h.length) break;

		if (h.type == MSG_CHAT) g_recv++;

		buf.erase(0, sizeof(MessageHeader) + h.length);
	}
}

// ============ 主函数 ============
int main(int argc, char **argv) {
	if (argc < 5) {
		cerr << "用法:\n"
			 << "  " << argv[0] << " register <host> <port> <count>\n"
			 << "  " << argv[0] << " bench    <host> <port> <count> <interval_ms> <duration_sec>\n";
		return 1;
	}

	string mode = argv[1];
	string host = argv[2];
	int port = atoi(argv[3]);
	int count = atoi(argv[4]);
	string prefix = "bench_";

	// ---------- 注册模式 ----------
	if (mode == "register") {
		cout << "开始注册 " << count << " 个用户 (" << prefix << "0 ~ " << prefix << count - 1 << ")...\n";
		int ok = 0;
		for (int i = 0; i < count; i++) {
			string uname = prefix + to_string(i);
			if (registerOne(host, port, uname, "123456")) {
				ok++;
				cout << "[" << (i + 1) << "/" << count << "] " << uname << " 注册成功\n";
			}
			else {
				cerr << "[" << (i + 1) << "/" << count << "] " << uname << " 注册失败\n";
			}
		}
		cout << "注册完成：成功 " << ok << " / " << count << "\n";
		return 0;
	}

	// ---------- 压测模式 ----------
	if (mode != "bench") {
		cerr << "未知模式: " << mode << "\n";
		return 1;
	}
	if (argc < 7) {
		cerr << "bench 模式需要: <count> <interval_ms> <duration_sec>\n";
		return 1;
	}
	int interval_ms = atoi(argv[5]);
	int duration_sec = atoi(argv[6]);

	cout << "========== 压测开始 ==========\n"
		 << "连接数: " << count << "\n"
		 << "每连接发送间隔: " << interval_ms << " ms\n"
		 << "持续时间: " << duration_sec << " 秒\n"
		 << "预期发送 QPS ≈ " << (count * 1000.0 / interval_ms) << "\n"
		 << "==============================\n";

	auto start = chrono::steady_clock::now();

	// ============================================================
	// 阶段 1：全部连接登录（并发），全部完成后再进入阶段 2
	// 这样任何客户端在别人登录期间都不会发消息，loginOne 不会被干扰
	// ============================================================
	vector<int> fds(count, -1);
	{
		vector<thread> login_threads;
		login_threads.reserve(count);
		for (int i = 0; i < count; i++) {
			login_threads.emplace_back([&, i]() {
				fds[i] = loginOne(host, port, prefix + to_string(i), "123456");
				if (fds[i] < 0) g_failed++;
			});
		}
		for (auto &t : login_threads)
			t.join();
	}

	int login_ok = count - (int)g_failed.load();
	cout << "登录完成：成功 " << login_ok << " / " << count << "\n\n";

	if (login_ok == 0) {
		cerr << "所有连接都登录失败，退出\n";
		return 1;
	}

	// ============================================================
	// 阶段 2：所有登录成功的连接开始发消息
	// ============================================================
	vector<thread> bench_threads;
	bench_threads.reserve(login_ok);
	for (int i = 0; i < count; i++) {
		if (fds[i] < 0) continue;
		bench_threads.emplace_back([&, i]() {
			auto end_time = chrono::steady_clock::now() + chrono::seconds(duration_sec);
			string recv_buf;
			while (chrono::steady_clock::now() < end_time) {
				json msg = {{"message", "bench_" + to_string(i)}};
				if (sendPacket(fds[i], MSG_CHAT, msg)) g_sent++;
				else {
					g_failed++;
					break;
				}
				drainRecv(fds[i], recv_buf);
				this_thread::sleep_for(chrono::milliseconds(interval_ms));
			}
			// 【新增】冷却期：停止发送后，再读 2 秒，把在途消息收干净
			auto cool_end = chrono::steady_clock::now() + chrono::seconds(2);
			while (chrono::steady_clock::now() < cool_end) {
				drainRecv(fds[i], recv_buf);
				this_thread::sleep_for(chrono::milliseconds(50));
			}

			close(fds[i]);
		});
	}

	// 每秒打印进度
	for (int s = 0; s < duration_sec + 2; s++) {
		this_thread::sleep_for(chrono::seconds(1));
		cout << "[进度 " << (s + 1) << "/" << duration_sec << "] "
			 << "发送=" << g_sent.load() << " 接收=" << g_recv.load() << " 失败=" << g_failed.load() << "\n";
	}

	for (auto &t : bench_threads)
		t.join();

	double elapsed = chrono::duration<double>(chrono::steady_clock::now() - start).count();

	uint64_t sent = g_sent.load();
	uint64_t recv = g_recv.load();
	uint64_t failed = g_failed.load();

	// 理论接收数 = 发送数 × (在线数-1)，因为发送者收不到自己的群聊
	uint64_t expected_recv = sent * (login_ok - 1);
	double loss_rate = expected_recv > 0 ? (1.0 - (double)recv / expected_recv) * 100.0 : 0;

	cout << "\n========== 压测结果 ==========\n"
		 << "持续时间: " << elapsed << " 秒\n"
		 << "在线连接数: " << login_ok << " / " << count << "\n"
		 << "发送消息数: " << sent << "\n"
		 << "发送 QPS: " << (sent / elapsed) << "\n"
		 << "接收消息数: " << recv << "\n"
		 << "接收 QPS: " << (recv / elapsed) << "\n"
		 << "理论接收数: " << expected_recv << " (发送数 × " << (login_ok - 1) << ")\n"
		 << "丢包率: " << loss_rate << " %\n"
		 << "失败数: " << failed << "\n"
		 << "==============================\n";

	return 0;
}*/