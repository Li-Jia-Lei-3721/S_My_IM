#include "sub_reactor.h"
#include "../common/logger.h"
#include <cerrno>
#include <chrono>
#include <cstring>
#include <fcntl.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <unistd.h>

SubReactor::SubReactor(int id) : id_(id) {};

SubReactor::~SubReactor() {
	stop();
	if (thread_.joinable()) thread_.join();
	if (event_fd_ != -1) close(event_fd_);
	if (epoll_fd_ != -1) close(epoll_fd_);
}

bool SubReactor::init() {
	epoll_fd_ = epoll_create1(0);
	if (epoll_fd_ == -1) return false;

	event_fd_ = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
	if (event_fd_ == -1) return false;

	epoll_event ev;
	ev.events = EPOLLIN | EPOLLET;
	ev.data.fd = event_fd_;
	epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, event_fd_, &ev);

	LOG_INFO("SubReactor %d init ok", id_);
	return true;
}

void SubReactor::start() {
	thread_ = thread(&SubReactor::run, this);
}

void SubReactor::stop() {
	stop_ = true;
	uint64_t one = 1;
	if (event_fd_ != -1) write(event_fd_, &one, sizeof(one));
}

void SubReactor::setNonBlocking(int fd) {
	int flags = fcntl(fd, F_GETFL, 0);
	fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

void SubReactor::addConnection(int client_fd, uint64_t conn_id) {
	postTask([this, client_fd, conn_id]() {
		clients_.emplace(client_fd, ClientContext{client_fd, conn_id, "", "", 0, 0, chrono::steady_clock::now()});

		epoll_event ev;
		ev.events = EPOLLIN | EPOLLET | EPOLLRDHUP;
		ev.data.fd = client_fd;
		epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, client_fd, &ev);
		addTimer(client_fd, conn_id);
	});
}

void SubReactor::postTask(function<void()> task) {
	{
		lock_guard<mutex> lock(task_mtx_);
		io_tasks_.push(std::move(task));
	}
	uint64_t one = 1;
	write(event_fd_, &one, sizeof(one));
}

void SubReactor::handleEventfd() {
	uint64_t val;
	read(event_fd_, &val, sizeof(val));

	queue<function<void()>> temp;
	{
		lock_guard<mutex> lock(task_mtx_);
		swap(temp, io_tasks_);
	}

	while (!temp.empty()) {
		temp.front()();
		temp.pop();
	}
}

void SubReactor::run() {
	epoll_event events[1024];

	while (!stop_) {
		int nfds = epoll_wait(epoll_fd_, events, 1024, 1000);
		if (nfds == -1) continue;

		for (int i = 0; i < nfds; i++) {
			int fd = events[i].data.fd;
			uint32_t evs = events[i].events;

			if (fd == event_fd_) {
				handleEventfd();
				continue;
			}
			if (evs & EPOLLIN || evs & EPOLLRDHUP) {
				handleClientData(fd);
			}
			if (evs & (EPOLLRDHUP | EPOLLHUP | EPOLLERR)) {
				auto it = clients_.find(fd);
				if (it != clients_.end()) {
					LOG_INFO("Client %d half-close/error.", fd);
					closeConnection(fd, it->second.conn_id);
				}
				continue;
			}
			if (evs & EPOLLOUT) {
				handleWrite(fd);
				continue;
			}
		}
		checkTimers();
	}
	LOG_INFO("SubReactor %d exited", id_);
}

void SubReactor::handleClientData(int client_fd) {
	char buffer[1024];

	while (true) {
		memset(buffer, 0, sizeof(buffer));
		int bytes_read = recv(client_fd, buffer, sizeof(buffer), 0);

		if (bytes_read > 0) {
			auto it = clients_.find(client_fd);

			if (it != clients_.end()) {
				it->second.last_active_time = chrono::steady_clock::now();

				addTimer(client_fd, it->second.conn_id);

				it->second.recv_buffer.append(buffer, bytes_read);
			}
		}
		else if (bytes_read == 0) {
			uint64_t cur_conn_id = 0;
			auto it = clients_.find(client_fd);

			if (it != clients_.end()) cur_conn_id = it->second.conn_id;

			closeConnection(client_fd, cur_conn_id);
			return;
		}
		else {
			if (errno == EAGAIN || errno == EWOULDBLOCK) break;
			else if (errno == EINTR) continue;
			else {
				uint64_t cur_conn_id = 0;
				auto it = clients_.find(client_fd);
				if (it != clients_.end()) cur_conn_id = it->second.conn_id;
				closeConnection(client_fd, cur_conn_id);
				return;
			}
		}
	}

	// 从缓冲区切包
	while (true) {
		string body;
		MessageHeader header;
		bool is_invalid_packet = false;
		uint64_t current_conn_id = 0;

		auto it = clients_.find(client_fd);
		if (it == clients_.end()) return;

		current_conn_id = it->second.conn_id;
		string &rb = it->second.recv_buffer;

		if (rb.size() < sizeof(MessageHeader) + it->second.recv_offset) break;

		memcpy(&header, rb.data() + it->second.recv_offset, sizeof(MessageHeader));
		header.unpack();

		if (header.magic != 0x12345678) {
			rb.clear();
			it->second.recv_offset = 0;
			is_invalid_packet = true;
		}
		else if (header.length > 10 * 1024 * 1024) {
			rb.clear();
			it->second.recv_offset = 0;
			is_invalid_packet = true;
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

		if (is_invalid_packet) {
			closeConnection(client_fd, current_conn_id);
			return;
		}

		// 不再直接分发业务，交给业务层回调
		if (on_message_) on_message_(client_fd, current_conn_id, body, (MsgType)header.type);
	}

	auto it = clients_.find(client_fd);
	if (it != clients_.end() && it->second.recv_offset > 0) {
		it->second.recv_buffer.erase(0, it->second.recv_offset);
		it->second.recv_offset = 0;
	}
}
bool SubReactor::sendToClient(int client_fd, uint64_t conn_id, MsgType type, const string &body) {
	auto it = clients_.find(client_fd);
	if (it == clients_.end() || it->second.conn_id != conn_id) return false;

	auto &ctx = it->second;
	size_t current_size = ctx.send_buffer.size();
	size_t add_size = sizeof(MessageHeader) + body.size();

	if (current_size + add_size > 10 * 1024 * 1024) {
		LOG_ERROR("[WARNING] Client %d send buffer overflow! Force closing.", client_fd);
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
				epoll_event ev;
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

void SubReactor::handleWrite(int client_fd) {
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

		epoll_event ev;
		ev.events = EPOLLIN | EPOLLET | EPOLLRDHUP;
		ev.data.fd = client_fd;
		epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, client_fd, &ev);
	}
}

void SubReactor::broadcastMessage(uint64_t sender_conn_id, const char *msg, int len) {
	vector<pair<int, uint64_t>> dead_conns;

	for (auto &pair : clients_) {
		int fd = pair.first;
		auto &ctx = pair.second;
		if (ctx.conn_id == sender_conn_id) continue;

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
					epoll_event ev;
					ev.events = EPOLLIN | EPOLLET | EPOLLOUT | EPOLLRDHUP;
					ev.data.fd = fd;
					epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, fd, &ev);
					break;
				}
				else {
					dead_conns.emplace_back(fd, ctx.conn_id);
					break;
				}
			}
		}

		if (ctx.send_offset >= ctx.send_buffer.size()) {
			ctx.send_buffer.clear();
			ctx.send_offset = 0;
		}
	}

	for (auto &p : dead_conns) {
		closeConnection(p.first, p.second);
	}
}

void SubReactor::closeConnection(int client_fd, uint64_t conn_id) {
	auto it = clients_.find(client_fd);
	if (it == clients_.end() || it->second.conn_id != conn_id) return;

	delTimer(conn_id);
	epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, client_fd, nullptr);
	close(client_fd);
	clients_.erase(client_fd);

	// 通知业务层做 session/redis/room 清理
	if (on_close_) on_close_(client_fd, conn_id);
}

void SubReactor::addTimer(int fd, uint64_t conn_id) {
	auto expire = chrono::steady_clock::now() + chrono::seconds(180);

	auto it = timer_map_.find(conn_id);
	if (it != timer_map_.end()) {
		timer_set_.erase(it->second);
		timer_map_.erase(it);
	}

	TimerNode node{expire, fd, conn_id};
	auto insert_it = timer_set_.insert(node).first;
	timer_map_[conn_id] = insert_it;
}

void SubReactor::delTimer(uint64_t conn_id) {
	auto it = timer_map_.find(conn_id);
	if (it != timer_map_.end()) {
		timer_set_.erase(it->second);
		timer_map_.erase(it);
	}
}

void SubReactor::checkTimers() {
	auto now = chrono::steady_clock::now();
	while (!timer_set_.empty()) {
		auto it = timer_set_.begin();
		if (it->expire_time > now) break;

		int fd = it->fd;
		uint64_t conn_id = it->conn_id;

		timer_map_.erase(conn_id);
		timer_set_.erase(it);

		auto conn_it = clients_.find(fd);
		if (conn_it != clients_.end() && conn_it->second.conn_id == conn_id) {
			if (now - conn_it->second.last_active_time > chrono::seconds(180)) {
				LOG_INFO("[Heartbeat] Client %d timeout.", fd);
				closeConnection(fd, conn_id);
			}
		}
	}
}