#include "epoll_server.h"
#include <arpa/inet.h>
#include <chrono>
#include <cstring>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
// #include <algorithm>
#include "../../third_party/json.hpp"
#include <cerrno>
using json = nlohmann::json;
#include "../common/password_util.h"
#include "../common/session_manager.h"
#include "../db/mysql_pool.h"
#include "../db/redis_client.h"
// #include <iostream>
#include "../common/logger.h"
#include "../common/validator.h"
#include <random>
#include <utility>

EpollServer::EpollServer(int port) : port_(port), server_fd_(-1), epoll_fd_(-1) {}

EpollServer::~EpollServer() {
	if (server_fd_ != -1) close(server_fd_);
	if (epoll_fd_ != -1) close(epoll_fd_);
}

void EpollServer::closeConnection(int client_fd, uint64_t conn_id) {
	auto it = clients_.find(client_fd);

	// 连接不存在或者换人了  退出
	if (it == clients_.end() || it->second.conn_id != conn_id) {
		return;
	}

	delTimer(conn_id);

	auto &ctx = it->second;

	string username = SessionManager::getInstance().getUsernameByConnId(conn_id);
	if (!username.empty()) {
		if (SessionManager::getInstance().logout(username, conn_id)) {
			thread_pool_->enqueue([username, conn_id]() {
				RedisClient::getInstance().setOffline(username, conn_id);
			});
		}
	}

	// 清理内存中的  用户-房间 在线状态映射
	removeUserFromAllRooms(conn_id);

	epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, client_fd, nullptr);
	close(client_fd);
	clients_.erase(client_fd);
}

void EpollServer::setNonBlocking(int fd) {
	int flags = fcntl(fd, F_GETFL, 0);
	fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

bool EpollServer::start() {
	server_fd_ = socket(AF_INET, SOCK_STREAM, 0);
	int opt = 1;

	setsockopt(server_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

	struct sockaddr_in address;
	memset(&address, 0, sizeof(address));
	address.sin_family = AF_INET;
	address.sin_addr.s_addr = INADDR_ANY;
	address.sin_port = htons(port_);

	if (bind(server_fd_, (struct sockaddr *)&address, sizeof(address)) == -1) return false;
	if (listen(server_fd_, 10) == -1) return false;

	setNonBlocking(server_fd_);

	epoll_fd_ = epoll_create1(0);
	if (epoll_fd_ == -1) return false;

	struct epoll_event ev;
	ev.events = EPOLLIN | EPOLLET;
	ev.data.fd = server_fd_;
	epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, server_fd_, &ev);

	// event_fd  用于工作线程唤醒主线程
	event_fd_ = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
	if (event_fd_ == -1) {
		LOG_ERROR("eventfd create failed!");
		return false;
	}

	// 将event_fd也注册到epoll监听队列中
	struct epoll_event ev_eventfd;
	ev_eventfd.events = EPOLLIN | EPOLLET;
	ev_eventfd.data.fd = event_fd_;
	epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, event_fd_, &ev_eventfd);

	thread_pool_ = make_unique<ThreadPool>(10);
	auth_thread_pool_ = make_unique<ThreadPool>(3);

	LOG_INFO("Epoll Server started on port %d", port_);
	return true;
}

void EpollServer::run(atomic<bool> &running) {
	struct epoll_event events[1024]; // 存放就绪事件

	while (running) {
		int nfds = epoll_wait(epoll_fd_, events, 1024, 1000);
		if (nfds == -1) continue;

		for (int i = 0; i < nfds; i++) {
			int current_fd = events[i].data.fd;
			uint32_t evs = events[i].events;

			if (current_fd == event_fd_) {
				uint64_t val;
				read(event_fd_, &val, sizeof(val)); // 把计数器清零防止一直触发

				// 把任务全取出来在主线程执行
				queue<function<void()>> temp_tasks;
				{
					lock_guard<mutex> lock(task_mtx_);
					swap(temp_tasks, io_tasks_); // 快速交换 减小锁的持有时间
				}

				while (!temp_tasks.empty()) {
					temp_tasks.front()();

					temp_tasks.pop();
				}

				continue;
			}

			if (current_fd == server_fd_) {
				handleNewConnection();
				continue;
			}

			if (evs & EPOLLIN || evs & EPOLLRDHUP) handleClientData(current_fd);

			if (evs & EPOLLRDHUP || evs & EPOLLHUP || evs & EPOLLERR) {

				uint64_t cur_conn_id = 0;
				auto it = clients_.find(current_fd);

				if (it != clients_.end()) {
					cur_conn_id = it->second.conn_id;
					LOG_INFO("Client %d half-close/error.", current_fd);
					// cout << "Client " << current_fd << " half-close/error." << endl;
					closeConnection(current_fd, cur_conn_id);
				}

				continue;
			}

			if (evs & EPOLLOUT) handleWrite(current_fd);
		}

		auto now = chrono::steady_clock::now();

		// 有节点且过期 一直清理
		while (!timer_set_.empty()) {
			auto it = timer_set_.begin();

			if (it->expire_time > now) {
				break;
			}

			int fd = it->fd;
			uint64_t conn_id = it->conn_id;

			timer_map_.erase(conn_id);
			timer_set_.erase(it);

			// 判断一下这个连接是不是真的超时了
			// 因为有可能是由于某些原因遗留的极短旧节点，虽然上面 map 去重已经杜绝了，但保持防御性
			auto conn_it = clients_.find(fd);
			if (conn_it != clients_.end() && conn_it->second.conn_id == conn_id) {
				if (now - conn_it->second.last_active_time > chrono::seconds(60)) {
					LOG_INFO("[Heartbeat] Client %d timeout.", fd);

					closeConnection(fd, conn_id);
				}
			}
		}
	}

	LOG_INFO("EpollServer loop exited.");
}

void EpollServer::handleNewConnection() {
	struct sockaddr_in client_addr;

	while (true) {
		socklen_t len = sizeof(client_addr);

		int client_fd = accept(server_fd_, (struct sockaddr *)&client_addr, &len);
		if (client_fd == -1) {
			if (errno == EAGAIN || errno == EWOULDBLOCK) // 连接拿光了
				break;
			else {

				// perror("accept error"); // 其他错误如文件描述符耗尽等
				LOG_ERROR("accept error: %s", strerror(errno));
				break;
			}
		}
		setNonBlocking(client_fd);
		// cout << "New client connected! fd = " << client_fd << endl;

		uint64_t new_id = ++global_conn_id_;

		clients_.emplace(client_fd, ClientContext{client_fd, new_id, "", "", 0, 0, chrono::steady_clock::now()});

		struct epoll_event ev;
		ev.events = EPOLLIN | EPOLLET | EPOLLRDHUP;
		ev.data.fd = client_fd;
		epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, client_fd, &ev);

		// 刚连上的用户也要加入到定期器的小根堆
		addTimer(client_fd, new_id);
	}
}

void EpollServer::handleClientData(int client_fd) {
	char buffer[1024];

	while (true) {
		memset(buffer, 0, sizeof(buffer));

		// 非阻塞模式下 没有数据revc会返回-1  并把errno设为EAGAIN
		int bytes_read = recv(client_fd, buffer, sizeof(buffer), 0);

		if (bytes_read > 0) {
			// cout << "Client " << client_fd << " say: " << buffer << endl;
			auto it = clients_.find(client_fd);
			if (it != clients_.end()) {
				// 有数据  连接没死   刷新时间	用于方未校验
				it->second.last_active_time = chrono::steady_clock::now();
				// 往堆里塞时间节点
				addTimer(client_fd, it->second.conn_id);

				it->second.recv_buffer.append(buffer, bytes_read);
			}
		}
		else if (bytes_read == 0) { // 客户端断开连接
			uint64_t cur_conn_id = 0;
			auto it = clients_.find(client_fd);

			if (it != clients_.end()) {
				cur_conn_id = it->second.conn_id;
			}

			closeConnection(client_fd, cur_conn_id);
			return;
		}
		else {
			if (errno == EAGAIN || errno == EWOULDBLOCK) {
				break;
			}
			else if (errno == EINTR) { // 信号被中断
				continue;
			}
			else {
				uint64_t cur_conn_id = 0;
				auto it = clients_.find(client_fd);

				if (it != clients_.end()) cur_conn_id = it->second.conn_id;

				closeConnection(client_fd, cur_conn_id);
				return;
			}
		}
	}

	// 从缓冲区切出消息
	while (true) {
		string body;
		MessageHeader header;
		bool is_invaild_packet = false;
		uint64_t current_conn_id = 0;

		auto it = clients_.find(client_fd);
		if (it == clients_.end()) return;

		current_conn_id = it->second.conn_id; // 提前获取 conn_id 备用
		string &rb = it->second.recv_buffer;

		if (rb.size() < sizeof(MessageHeader) + it->second.recv_offset) break;

		memcpy(&header, rb.data() + it->second.recv_offset, sizeof(MessageHeader));

		// 变为本机字节序
		header.unpack();

		if (header.magic != 0x12345678) {
			// cerr << "Client " << client_fd << " sent invaild magic number! Disconnecting." << endl;
			rb.clear();
			it->second.recv_offset = 0;
			is_invaild_packet = true;
		}
		else if (header.length > 10 * 1024 * 1024) {
			// cerr << "Client " << client_fd << " sent too large packet! Disconnecting." << endl;
			rb.clear();
			it->second.recv_offset = 0;
			is_invaild_packet = true;
		}
		else if (rb.size() - it->second.recv_offset < sizeof(MessageHeader) + header.length) {
			break;
		}
		else {
			body = rb.substr(it->second.recv_offset + sizeof(MessageHeader), header.length);
			it->second.recv_offset += sizeof(MessageHeader) + header.length;

			if (it->second.recv_offset > 1024 * 1024) {
				rb.erase(0, it->second.recv_offset);
				it->second.recv_offset = 0;
			}
		}

		if (is_invaild_packet) {
			closeConnection(client_fd, current_conn_id);
			return;
		}

		if (header.type == MSG_REGISTER || header.type == MSG_LOGIN || header.type == MSG_GET_CODE) {
			// 按值捕获client_fd和body 防止垂悬引用
			auth_thread_pool_->enqueue([this, client_fd, current_conn_id, body, type = (MsgType)header.type]() {
				processAuthTask(client_fd, current_conn_id, body, type);
			});
		}
		else if (header.type == MSG_JOIN_ROOM) {
			auth_thread_pool_->enqueue([this, client_fd, current_conn_id, body]() {
				processJoinRoomTask(client_fd, current_conn_id, body);
			});
		}
		else if (header.type == MSG_ROOM_CHAT) {
			thread_pool_->enqueue([this, client_fd, current_conn_id, body]() {
				processRoomChatTask(client_fd, current_conn_id, body);
			});
		}
		else if (header.type == MSG_CHAT) {
			thread_pool_->enqueue([this, client_fd, current_conn_id, body]() {
				processChatTask(client_fd, current_conn_id, body);
			});
		}
	}

	auto it = clients_.find(client_fd);
	if (it != clients_.end() && it->second.recv_offset > 0) {
		it->second.recv_buffer.erase(0, it->second.recv_offset);
		it->second.recv_offset = 0;
	}
}

bool EpollServer::sendToClient(int client_fd, uint64_t conn_id, MsgType type, const string &body) {
	auto it = clients_.find(client_fd);

	// 连接还在且没有被复用
	if (it == clients_.end() || it->second.conn_id != conn_id) return false;

	auto &ctx = it->second;

	// 计算添加本条消息后的总积压大小
	size_t current_size = ctx.send_buffer.size();
	size_t add_size = sizeof(MessageHeader) + body.size();

	// 设定 10MB 的高压线
	if (current_size + add_size > 10 * 1024 * 1024) {
		// cerr << "[WARNING] Client " << client_fd << " send buffer overflow! Force closing." << endl;
		LOG_ERROR("[WARNING] Client %d send buffer overflow! Force closing.", client_fd);

		// 直接踢掉这个不读数据的客户端，防止拖垮服务器
		closeConnection(client_fd, conn_id);
		return false;
	}

	MessageHeader header;
	header.magic = 0x12345678;
	header.length = body.size();
	header.type = type;

	header.pack();

	ctx.send_buffer.append((char *)&header, sizeof(MessageHeader));
	ctx.send_buffer.append(body);

	while (ctx.send_offset < ctx.send_buffer.size()) {
		int bytes_sent = send(client_fd, ctx.send_buffer.data() + ctx.send_offset,
							  ctx.send_buffer.size() - ctx.send_offset, MSG_NOSIGNAL);
		if (bytes_sent > 0) {
			ctx.send_offset += bytes_sent;

			if (ctx.send_offset >= 1024 * 1024) {
				ctx.send_buffer.erase(0, ctx.send_offset);
				ctx.send_offset = 0;
			}
		}
		else if (bytes_sent == -1) {
			if (errno == EAGAIN || errno == EWOULDBLOCK) {
				struct epoll_event ev;
				ev.events = EPOLLIN | EPOLLOUT | EPOLLET | EPOLLRDHUP;
				ev.data.fd = client_fd;

				epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, client_fd, &ev);
				break;
			}
			else {
				closeConnection(client_fd, conn_id);
				return false;
			}
		}
	}

	if (ctx.send_offset >= ctx.send_buffer.size()) {
		ctx.send_buffer.clear();
		ctx.send_offset = 0;
	}
	return true;
}

void EpollServer::broadcastMessage(int sender_fd, const char *msg, int len) { // 丢失数据版本
	vector<pair<int, uint64_t>> dead_conns;									  // 存放抛弃的客户端

	for (auto &pair : clients_) {
		int fd = pair.first;
		if (fd == sender_fd) continue;

		auto &ctx = pair.second;

		if (SessionManager::getInstance().getUsernameByConnId(ctx.conn_id).empty()) {
			continue;
		}

		// 积压太多直接删掉防止内存被撑爆
		if (ctx.send_buffer.size() + len > 10 * 1024 * 1024) {
			dead_conns.emplace_back(fd, ctx.conn_id);
			continue;
		}

		ctx.send_buffer.append(msg, len);

		while (ctx.send_offset < ctx.send_buffer.size()) {
			int bytes_sent = send(fd, ctx.send_buffer.data() + ctx.send_offset,
								  ctx.send_buffer.size() - ctx.send_offset, MSG_NOSIGNAL);
			if (bytes_sent > 0) {
				ctx.send_offset += bytes_sent;

				if (ctx.send_offset >= 1024 * 1024) {
					ctx.send_buffer.erase(0, ctx.send_offset);
					ctx.send_offset = 0;
				}
			}
			else if (bytes_sent == -1) {
				if (errno == EAGAIN || errno == EWOULDBLOCK) {
					struct epoll_event ev;
					ev.events = EPOLLIN | EPOLLET | EPOLLOUT | EPOLLRDHUP;

					ev.data.fd = fd;
					epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, fd, &ev);

					break;
				}
				else { // 其它错误 断开连接
					dead_conns.emplace_back(fd, ctx.conn_id);
					break;
				}
			}
		}
		// 发完了 清理缓冲区
		if (ctx.send_offset >= ctx.send_buffer.size()) {
			ctx.send_buffer.clear();
			ctx.send_offset = 0;
		}
	}

	for (auto &p : dead_conns)
		closeConnection(p.first, p.second);
}

void EpollServer::handleWrite(int client_fd) {
	auto it = clients_.find(client_fd);
	if (it == clients_.end()) return;

	auto &ctx = it->second;
	uint64_t conn_id = ctx.conn_id;

	while (ctx.send_offset < ctx.send_buffer.size()) {
		int bytes_sent = send(client_fd, ctx.send_buffer.data() + ctx.send_offset,
							  ctx.send_buffer.size() - ctx.send_offset, MSG_NOSIGNAL);
		if (bytes_sent > 0) {
			ctx.send_offset += bytes_sent;

			if (ctx.send_offset >= 1024 * 1024) {
				ctx.send_buffer.erase(0, ctx.send_offset);
				ctx.send_offset = 0;
			}
		}
		else {
			if (errno == EAGAIN || errno == EWOULDBLOCK) return;
			else {
				closeConnection(client_fd, conn_id);
				return;
			}
		}
	}

	if (ctx.send_offset >= ctx.send_buffer.size()) {
		ctx.send_buffer.clear();
		ctx.send_offset = 0;

		struct epoll_event ev;
		ev.events = EPOLLIN | EPOLLET | EPOLLRDHUP;
		ev.data.fd = client_fd;
		epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, client_fd, &ev);
	}
}

void EpollServer::processAuthTask(int client_fd, uint64_t conn_id, const string &body, MsgType type) {
	try {
		json j = json::parse(body);
		string uname = j["username"];
		// 将password改为可选读取
		string pwd = j.contains("password") ? j["password"].get<string>() : "";

		if (!Validator::isValidUsername(uname)) {
			json res;
			res["message"] = "用户名格式非法(仅3-20位字母数字或下划线 )";
			string res_body = res.dump();

			postToIoThread([this, client_fd, conn_id, res_body]() {
				sendToClient(client_fd, conn_id, MSG_SYSTEM, res_body);
			});
			return;
		}

		MYSQL *conn = nullptr;
		MysqlConnRAII raii(&conn);

		if (!conn) {
			json res;
			res["message"] = "数据库系统繁忙 请稍后再试";
			string body = res.dump();

			postToIoThread([this, client_fd, conn_id, body]() {
				sendToClient(client_fd, conn_id, MSG_SYSTEM, body);
			});
			return;
		}

		char escape_uname[256] = {0};
		// char escape_pwd[256] = {0};
		mysql_real_escape_string(conn, escape_uname, uname.c_str(), uname.length());
		// mysql_real_escape_string(conn, escape_pwd, pwd.c_str(), pwd.length());

		if (type == MSG_GET_CODE) {
			string limit_key = "limit:code:" + uname;

			long long req_count = RedisClient::getInstance().incr(limit_key);
			if (req_count == 1) RedisClient::getInstance().expire(limit_key, 60);
			if (req_count > 1) {
				json res;
				res["message"] = "请求验证码过于频繁，请60秒后再试";
				postToIoThread([this, client_fd, conn_id, res_body = res.dump()]() {
					sendToClient(client_fd, conn_id, MSG_SYSTEM, res_body);
				});
				return;
			}

			// 线程安全随机数生成
			static thread_local mt19937 rng{random_device{}()};
			static thread_local uniform_int_distribution<int> dist(100000, 999999);
			int code_int = dist(rng);
			string code = to_string(code_int);

			// 存入redis 5分钟过期
			RedisClient::getInstance().setString("code:reg:" + uname, code, 300);

			// 存日志中方便验证
			LOG_INFO("user %s code is : %s", uname.c_str(), code.c_str());

			// ---------- 开发模式：验证码直接回给客户端 ----------
			json res;
			res["message"] = "【开发模式】您的验证码是: " + code + " (5分钟内有效)";
			postToIoThread([this, client_fd, conn_id, res_body = res.dump()]() {
				sendToClient(client_fd, conn_id, MSG_SYSTEM, res_body);
			});

			return;
		}

		if (type == MSG_REGISTER) {
			bool is_allowed = RedisClient::getInstance().isAllowedBySlidingWindow("limit:register:" + uname, 30, 5);

			if (!is_allowed) {
				json res;
				res["message"] = "请求过于频繁 请稍后再试";
				string body = res.dump();
				postToIoThread([this, client_fd, conn_id, body]() {
					sendToClient(client_fd, conn_id, MSG_SYSTEM, body);
				});
				return;
			}
			// 客户端必须带验证码过来
			string client_code = j.contains("code") ? j["code"].get<string>() : "";
			string redis_code_key = "code:reg:" + uname;

			// 【核心改造】：直接调用原子方法，一步到位！
			bool code_valid = RedisClient::getInstance().verifyAndConsumeCode(redis_code_key, client_code);

			if (!code_valid) {
				json res;
				res["message"] = "验证码错误或已过期";
				string body = res.dump();
				postToIoThread([this, client_fd, conn_id, body]() {
					sendToClient(client_fd, conn_id, MSG_SYSTEM, body);
				});
				return;
			}

			// 1.生成盐
			string salt = PasswordUtil::generateSalt();
			// 2.计算哈希密文
			string hashed_pwd = PasswordUtil::hashPassword(pwd, salt);

			string sql = "INSERT INTO users (username,password,salt) VALUES ('" + string(escape_uname) + "','" +
						 hashed_pwd + "','" + salt + "')";

			// 用mysql的唯一索引防并发重名
			if (mysql_query(conn, sql.c_str()) == 0) {

				RedisClient::getInstance().addRegisteredUser(uname);

				json res;
				res["message"] = "注册成功 请登录";
				string body = res.dump();
				postToIoThread([this, client_fd, conn_id, body]() {
					sendToClient(client_fd, conn_id, MSG_SYSTEM, body);
				});
			}
			else {
				if (mysql_errno(conn) == 1062) {
					json res;
					res["message"] = "注册失败 用户名已存在";
					string body = res.dump();
					postToIoThread([this, client_fd, conn_id, body]() {
						sendToClient(client_fd, conn_id, MSG_SYSTEM, body);
					});
				}
				else {
					json res;
					res["message"] = "注册失败 服务器内部错误";
					string body = res.dump();
					postToIoThread([this, client_fd, conn_id, body]() {
						sendToClient(client_fd, conn_id, MSG_SYSTEM, body);
					});
				}
			}
		}
		else if (type == MSG_LOGIN) {
			string sql = "SELECT id,password,salt FROM users WHERE username='" + string(escape_uname) + "'";

			if (mysql_query(conn, sql.c_str()) != 0) {
				json res;
				res["message"] = "数据库连接失败";
				string body = res.dump();

				postToIoThread([this, client_fd, conn_id, body]() {
					sendToClient(client_fd, conn_id, MSG_SYSTEM, body);
				});

				return;
			}

			MYSQL_RES *result = mysql_store_result(conn);
			if (result == nullptr) {
				json res;
				res["message"] = "服务器内部错误，获取数据失败";
				string body = res.dump();
				postToIoThread([this, client_fd, conn_id, body]() {
					sendToClient(client_fd, conn_id, MSG_SYSTEM, body);
				});

				// cerr << "mysql_store_result error: " << mysql_error(conn) << endl;
				LOG_ERROR("mysql_store_result error: %s", mysql_error(conn));

				return;
			}

			if (mysql_num_rows(result) == 0) {
				json res;
				res["message"] = "登录失败，账号或密码错误";
				string body = res.dump();
				postToIoThread([this, client_fd, conn_id, body]() {
					sendToClient(client_fd, conn_id, MSG_SYSTEM, body);
				});
				mysql_free_result(result);

				return;
			}

			MYSQL_ROW row = mysql_fetch_row(result);
			int db_user_id = atoi(row[0]);
			string db_hashed_pwd = row[1];
			string db_salt = row[2];
			string input_hashed = PasswordUtil::hashPassword(pwd, db_salt);

			mysql_free_result(result); // 对比完立即释放结果集  防后续return导致内存泄漏

			if (input_hashed != db_hashed_pwd) {
				json res;
				res["message"] = "登录失败，账号或密码错误";
				string body = res.dump();

				postToIoThread([this, client_fd, conn_id, body]() {
					sendToClient(client_fd, conn_id, MSG_SYSTEM, body);
				});
				return;
			}

			// int old_fd = -1;
			pair<int, uint64_t> old_session = SessionManager::getInstance().login(uname, conn_id, client_fd);
			int old_fd = old_session.first;
			uint64_t old_conn_id = old_session.second;

			if (old_fd != -1) {
				json kick_res;
				kick_res["message"] = "您的账号已在其他地方登录，您已被迫下线。";
				string body = kick_res.dump();
				postToIoThread([this, old_fd, old_conn_id, body]() {
					sendToClient(old_fd, old_conn_id, MSG_SYSTEM, body);

					// 关闭旧连接（此处会触发 closeConnection 里面的 logout 等清理逻辑）
					closeConnection(old_fd, old_conn_id);
				});
			}

			// 更新 Redis 缓存里的在线状态为最新的 conn_id
			RedisClient::getInstance().setOnline(uname, conn_id);

			json res;
			res["message"] = "登录成功，欢迎 " + uname;
			string success_body = res.dump();
			postToIoThread([this, client_fd, conn_id, success_body]() {
				sendToClient(client_fd, conn_id, MSG_SYSTEM, success_body);
			});

			// 登录成功后 将用户上线状态记录到 内存的 所有其加入的房间中
			auto rooms = getRoomsByUserId(db_user_id);
			for (auto &p : rooms) {
				addRoomOnline(p.first, conn_id);
			}

			vector<string> offline_msgs;

			RedisClient::getInstance().popOfflineMsgs(uname, offline_msgs);

			if (!offline_msgs.empty()) {
				json off_res;
				off_res["message"] = "您有 " + to_string(offline_msgs.size()) + " 条离线消息";
				string body = off_res.dump();
				postToIoThread([this, client_fd, conn_id, body]() {
					sendToClient(client_fd, conn_id, MSG_SYSTEM, body);
				});

				for (const auto &msg : offline_msgs) {
					string msg_body = msg;
					postToIoThread([this, client_fd, conn_id, msg_body, uname, msg]() {
						bool success = sendToClient(client_fd, conn_id, MSG_CHAT, msg_body);

						if (!success) {
							thread_pool_->enqueue([uname, msg]() {
								RedisClient::getInstance().pushOfflineMsg(uname, msg);
							});
						}
					});
				}
			}
		}
	}
	catch (const exception &e) {
		json res;
		res["message"] = "数据格式错误";
		string body = res.dump();
		postToIoThread([this, client_fd, conn_id, body]() {
			sendToClient(client_fd, conn_id, MSG_SYSTEM, body);
		});
	}
}

void EpollServer::processChatTask(int client_fd, uint64_t conn_id, const string &body) {
	string username = SessionManager::getInstance().getUsernameByConnId(conn_id);

	if (username.empty()) {
		// 如果查不到用户名，说明会话已失效
		json res;
		res["message"] = "会话失效 请重新登录";
		string body = res.dump();
		postToIoThread([this, client_fd, conn_id, body]() {
			sendToClient(client_fd, conn_id, MSG_SYSTEM, body);
		});
		return;
	}

	try {
		// 接收json
		json j = json::parse(body);
		string input_msg = j["message"];
		string target_name = j.contains("target") ? j["target"].get<string>() : "";

		// 发送json
		json out_json;
		out_json["sender"] = username;
		out_json["message"] = input_msg;
		string out_body = out_json.dump();

		if (!target_name.empty()) {
			if (target_name == username) {
				json res;
				res["message"] = "不能给自己发私聊！";
				string body = res.dump();

				postToIoThread([this, client_fd, conn_id, body]() {
					sendToClient(client_fd, conn_id, MSG_SYSTEM, body);
				});

				return;
			}

			bool is_registered = RedisClient::getInstance().isRegistered(target_name);

			if (!is_registered) {
				MYSQL *conn = nullptr;
				MysqlConnRAII raii(&conn);

				if (!conn) {
					json res;
					res["message"] = "数据库系统繁忙 请稍后!";
					string body = res.dump();
					postToIoThread([this, client_fd, conn_id, body]() {
						sendToClient(client_fd, conn_id, MSG_SYSTEM, body);
					});
					return;
				}

				if (conn) {
					char escape_target[256] = {0};
					mysql_real_escape_string(conn, escape_target, target_name.c_str(), target_name.length());

					string sql = "SELECT id FROM users WHERE username='" + string(escape_target) + "'";

					if (mysql_query(conn, sql.c_str()) == 0) {
						MYSQL_RES *result = mysql_store_result(conn);

						if (!result) {
							// cerr << "获取结果集失败: " << mysql_error(conn) << endl;
							LOG_ERROR("获取结果集失败: %s", mysql_error(conn));

							json res;
							res["message"] = "服务器内部错误";
							string body = res.dump();
							postToIoThread([this, client_fd, conn_id, body]() {
								sendToClient(client_fd, conn_id, MSG_SYSTEM, body);
							});
							return;
						}

						if (mysql_num_rows(result) > 0) {
							is_registered = true;
							RedisClient::getInstance().addRegisteredUser(target_name); // 回写 Redis
						}

						mysql_free_result(result);
					}
				}
			}

			if (!is_registered) {
				json res;
				res["message"] = "用户不存在";
				string body = res.dump();
				postToIoThread([this, client_fd, conn_id, body]() {
					sendToClient(client_fd, conn_id, MSG_SYSTEM, body);
				});
				return;
			}

			uint64_t target_conn_id = RedisClient::getInstance().getOnlineConnId(target_name);

			bool message_sent = false;

			if (target_conn_id != 0) {
				int target_fd = SessionManager::getInstance().getFdByConnId(target_conn_id);

				if (target_fd != -1) {
					// 投递给主线程
					postToIoThread([this, target_fd, target_conn_id, target_name, out_body]() {
						bool success = sendToClient(target_fd, target_conn_id, MSG_CHAT, out_body);

						if (!success) {
							thread_pool_->enqueue([target_name, target_conn_id, out_body]() {
								RedisClient::getInstance().pushOfflineMsg(target_name, out_body);
								RedisClient::getInstance().setOffline(target_name, target_conn_id);
							});
						}
					});
					return;
				}
			}

			// 没发出去均变为离线消息
			RedisClient::getInstance().pushOfflineMsg(target_name, out_body);

			if (target_conn_id != 0) {
				RedisClient::getInstance().setOffline(target_name, target_conn_id);
			}

			json res;
			res["message"] = "对方不在线 此消息为离线消息";
			string body = res.dump();
			postToIoThread([this, client_fd, conn_id, body]() {
				sendToClient(client_fd, conn_id, MSG_SYSTEM, body);
			});
		}
		else {
			MessageHeader out_header;
			out_header.magic = 0x12345678;
			out_header.length = out_body.length();
			out_header.type = MSG_CHAT;

			// 包头字节序转换
			out_header.pack();

			string out_packet = string((char *)&out_header, sizeof(MessageHeader)) + out_body;

			int sender_fd_copy = client_fd;
			postToIoThread([this, sender_fd_copy, out_packet]() {
				broadcastMessage(sender_fd_copy, out_packet.data(), out_packet.size());
			});
		}
	}
	catch (const exception &e) {
		json res;
		res["message"] = "数据格式错误";
		string body = res.dump();
		postToIoThread([this, client_fd, conn_id, body]() {
			sendToClient(client_fd, conn_id, MSG_SYSTEM, body);
		});
	}
}

void EpollServer::postToIoThread(function<void()> task) {
	{
		lock_guard<mutex> lock(task_mtx_);
		io_tasks_.push(std::move(task));
	}

	uint64_t one = 1;
	write(event_fd_, &one, sizeof(one));
}

void EpollServer::addTimer(int fd, uint64_t conn_id) {
	// 设定超时时间为180s
	auto expire = chrono::steady_clock::now() + chrono::seconds(180);

	// 该conn_id有定时器 就拿掉
	auto it = timer_map_.find(conn_id);
	if (it != timer_map_.end()) {
		timer_set_.erase(it->second);
		timer_map_.erase(it);
	}

	TimerNode node{expire, fd, conn_id};
	auto insert_it = timer_set_.insert(node).first;

	timer_map_[conn_id] = insert_it;
}

void EpollServer::delTimer(uint64_t conn_id) {
	auto it = timer_map_.find(conn_id);
	if (it != timer_map_.end()) {
		timer_set_.erase(it->second);
		timer_map_.erase(it);
	}
}

int EpollServer::getUserIdByUsername(const string &username) {
	MYSQL *conn = nullptr;
	MysqlConnRAII raii(&conn);

	if (!conn) return -1;

	char esc[256] = {0};
	mysql_real_escape_string(conn, esc, username.c_str(), username.length());
	string sql = "SELECT id FROM users WHERE username = '" + string(esc) + "'";

	if (mysql_ping(conn) != 0) {
		LOG_ERROR("mysql_ping failed before query: errno=%u err=%s", mysql_errno(conn), mysql_error(conn));
	}

	if (mysql_query(conn, sql.c_str()) != 0) {
		LOG_ERROR("query failed: errno=%u err=%s sql=[%s] sql_len=%zu", mysql_errno(conn), mysql_error(conn),
				  sql.c_str(), sql.size());
		return -1;
	}

	MYSQL_RES *res = mysql_store_result(conn);
	if (!res) return -1;

	int uid = -1;
	MYSQL_ROW row = mysql_fetch_row(res);
	if (row && row[0]) uid = atoi(row[0]);
	mysql_free_result(res);

	return uid;
}

int EpollServer::getRoomIdByName(const string &room_name) {
	MYSQL *conn = nullptr;
	MysqlConnRAII raii(&conn);
	if (!conn) return -1;

	char esc[256] = {0};
	mysql_real_escape_string(conn, esc, room_name.c_str(), room_name.length());
	string sql = "SELECT id FROM rooms WHERE room_name = '" + string(esc) + "'";

	if (mysql_query(conn, sql.c_str()) != 0) {
		LOG_ERROR("getRoomIdByName query failed: %s", mysql_error(conn));
		return -1;
	}

	MYSQL_RES *res = mysql_store_result(conn);
	if (!res) return -1;

	int rid = -1;
	MYSQL_ROW row = mysql_fetch_row(res);
	if (row && row[0]) rid = atoi(row[0]);
	mysql_free_result(res);
	return rid;
}

int EpollServer::createRoom(const string &room_name, int owner_id) {
	MYSQL *conn = nullptr;
	MysqlConnRAII raii(&conn);
	if (!conn) return -1;

	char esc[256] = {0};
	mysql_real_escape_string(conn, esc, room_name.c_str(), room_name.length());
	string sql1 = "INSERT INTO rooms (room_name,owner_id) VALUES ('" + string(esc) + "', " + to_string(owner_id) + ")";

	if (mysql_query(conn, sql1.c_str()) != 0) {
		if (mysql_errno(conn) == 1062) {
			LOG_INFO("createRoom: room '%s' already exists", room_name.c_str());
			return 0;
		}
		LOG_ERROR("createRoom failed: %s", mysql_error(conn));
		return -1;
	}

	int room_id = (int)mysql_insert_id(conn);
	// 房主加入成员表
	string sql2 =
		"INSERT INTO room_members (room_id,user_id) VALUES (" + to_string(room_id) + ", " + to_string(owner_id) + ")";

	if (mysql_query(conn, sql2.c_str()) != 0) {
		LOG_ERROR("createRoom: add owner member failed: %s", mysql_error(conn));
		// 房间已建，成员没加成功——返回 id 但记错误，调用方决定
	}

	return room_id;
}

bool EpollServer::isRoomMember(int room_id, int user_id) {
	MYSQL *conn = nullptr;
	MysqlConnRAII raii(&conn);
	if (!conn) return false;

	string sql = "SELECT 1 FROM room_members WHERE room_id=" + to_string(room_id) +
				 " AND user_id=" + to_string(user_id) + " LIMIT 1";
	if (mysql_query(conn, sql.c_str()) != 0) return false;

	MYSQL_RES *res = mysql_store_result(conn);
	if (!res) return false;

	bool member = (mysql_num_rows(res) > 0);
	mysql_free_result(res);
	return member;
}

bool EpollServer::addRoomMember(int room_id, int user_id) {
	MYSQL *conn = nullptr;
	MysqlConnRAII raii(&conn);
	if (!conn) return false;

	// ignore--c插入重复不报错 不回滚 返回0  继续往下走
	string sql = "INSERT IGNORE INTO room_members (room_id, user_id) VALUES (" + to_string(room_id) + ", " +
				 to_string(user_id) + ")";
	if (mysql_query(conn, sql.c_str()) != 0) {
		LOG_ERROR("addRoomMember failed: %s", mysql_error(conn));
		return false;
	}
	return true;
}

bool EpollServer::removeRoomMember(int room_id, int user_id) {
	MYSQL *conn = nullptr;
	MysqlConnRAII raii(&conn);
	if (!conn) return false;

	string sql =
		"DELETE FROM room_members WHERE room_id = " + to_string(room_id) + " AND user_id = " + to_string(user_id);

	if (mysql_query(conn, sql.c_str()) != 0) {
		LOG_ERROR("removeRoomMember failed: %s", mysql_error(conn));
		return false;
	}
	return true;
}

void EpollServer::addRoomOnline(int room_id, uint64_t conn_id) {
	lock_guard<mutex> lock(room_mtx_);
	room_online_users_[room_id].insert(conn_id);
}

void EpollServer::removeRoomOnline(int room_id, uint64_t conn_id) {
	lock_guard<mutex> lock(room_mtx_);

	auto it = room_online_users_.find(room_id);
	if (it == room_online_users_.end()) return;

	it->second.erase(conn_id);
	if (it->second.empty()) room_online_users_.erase(it);
}

unordered_set<uint64_t> EpollServer::getRoomOnlineUsers(int room_id) {
	lock_guard<mutex> lock(room_mtx_);

	auto it = room_online_users_.find(room_id);
	if (it == room_online_users_.end()) return {};

	return it->second;
}

void EpollServer::removeUserFromAllRooms(uint64_t conn_id) {
	lock_guard<mutex> lock(room_mtx_);

	for (auto &kv : room_online_users_) {
		kv.second.erase(conn_id);
	}
}

vector<pair<int, string>> EpollServer::getRoomsByUserId(int user_id) {
	vector<pair<int, string>> result;
	MYSQL *conn = nullptr;
	MysqlConnRAII raii(&conn);
	if (!conn) return result;

	string sql = "SELECT r.id,r.room_name FROM rooms r  "
				 "JOIN room_members rm ON r.id = rm.room_id "
				 "WHERE rm.user_id = " +
				 to_string(user_id);

	if (mysql_query(conn, sql.c_str()) != 0) {
		LOG_ERROR("getRoomsByUserId failed: %s", mysql_error(conn));
		return result;
	}

	MYSQL_RES *res = mysql_store_result(conn);
	if (!res) return result;

	MYSQL_ROW row;
	while ((row = mysql_fetch_row(res))) {
		int rid = atoi(row[0]);
		string rname = row[1] ? row[1] : "";
		result.emplace_back(rid, rname);
	}
	mysql_free_result(res);
	return result;
}

void EpollServer::processJoinRoomTask(int client_fd, uint64_t conn_id, const string &body) {
	auto reply = [this, client_fd, conn_id](const string &msg) {
		json res;
		res["message"] = msg;
		string b = res.dump();
		postToIoThread([this, client_fd, conn_id, b]() {
			sendToClient(client_fd, conn_id, MSG_SYSTEM, b);
		});
	};

	try {
		json j = json::parse(body);
		string room_name = j.contains("room_name") ? j["room_name"].get<string>() : "";

		//	1.必需登录
		string username = SessionManager::getInstance().getUsernameByConnId(conn_id);
		if (username.empty()) {
			reply("会话失效 请重新登录");
			return;
		}

		// 2.房间名校验
		if (room_name.empty() || room_name.length() > 50) {
			reply("房间名非法(1-50字符)");
			return;
		}

		// 3. 拿user_id
		int user_id = getUserIdByUsername(username);
		if (user_id < 0) {
			reply("用户不存在");
			return;
		}

		// 4.拿room_id
		int room_id = getRoomIdByName(room_name);
		if (room_id < 0) {
			room_id = createRoom(room_name, user_id);
			// 并发下可能刚被创建
			if (room_id <= 0) {
				room_id = getRoomIdByName(room_name);
			}

			if (room_id <= 0) {
				reply("创建房间失败");
				return;
			}
		}

		// 5.不是房间成员就加入房间
		if (!isRoomMember(room_id, user_id)) {
			if (!addRoomMember(room_id, user_id)) {
				reply("加入房间失败");
				return;
			}
		}

		// 6.内存在线登记
		addRoomOnline(room_id, conn_id);

		reply("加入房间 [" + room_name + "] 成功");
	}
	catch (const exception &e) {
		reply("数据格式错误");
	}
}

void EpollServer::processRoomChatTask(int client_fd, uint64_t conn_id, const string &body) {
	auto reply = [this, client_fd, conn_id](const string &msg) {
		json res;
		res["message"] = msg;
		string b = res.dump();
		postToIoThread([this, client_fd, conn_id, b]() {
			sendToClient(client_fd, conn_id, MSG_SYSTEM, b);
		});
	};

	try {
		json j = json::parse(body);
		string room_name = j.contains("room_name") ? j["room_name"].get<string>() : "";
		string input_msg = j.contains("message") ? j["message"].get<string>() : "";

		if (room_name.empty() || input_msg.empty()) {
			reply("房间名或消息不能为空");
			return;
		}

		// 1.必须登录
		string username = SessionManager::getInstance().getUsernameByConnId(conn_id);
		if (username.empty()) {
			reply("会话失效 请重新登录");
			return;
		}

		// 2.校验成员
		int user_id = getUserIdByUsername(username);
		int room_id = getRoomIdByName(room_name);
		if (room_id < 0 || !isRoomMember(room_id, user_id)) {
			reply("你不在该房间或房间不存在");
			return;
		}

		// 3.组装json包
		json out_json;
		out_json["room"] = room_name;
		out_json["sender"] = username;
		out_json["message"] = input_msg;
		string out_body = out_json.dump();

		// 4. 拿房间在线成员快照，投回主线程广播
		auto online = getRoomOnlineUsers(room_id);
		postToIoThread([this, online, out_body]() {
			for (uint64_t cid : online) {
				int fd = SessionManager::getInstance().getFdByConnId(cid);
				if (fd < 0) continue;
				// 需求 3：发送失败直接丢，不重试
				sendToClient(fd, cid, MSG_CHAT, out_body);
			}
		});
	}
	catch (const exception &e) {
		reply("数据格式错误");
	}
}
