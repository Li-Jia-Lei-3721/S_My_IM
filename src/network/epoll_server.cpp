#include "epoll_server.h"
#include <arpa/inet.h>
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
#include <iostream>
#include <utility>

EpollServer::EpollServer(int port) : port_(port), server_fd_(-1), epoll_fd_(-1) {}

EpollServer::~EpollServer() {
	if (server_fd_ != -1)
		close(server_fd_);
	if (epoll_fd_ != -1)
		close(epoll_fd_);
}

void EpollServer::closeConnection(int client_fd, uint64_t conn_id) {
	auto it = clients_.find(client_fd);

	// 连接不存在或者换人了  退出
	if (it == clients_.end() || it->second.conn_id != conn_id) {
		return;
	}

	string username = SessionManager::getInstance().getUsernameByConnId(conn_id);
	if (!username.empty()) {
		if (SessionManager::getInstance().logout(username, conn_id)) {
			thread_pool_->enqueue([username, conn_id]() { RedisClient::getInstance().setOffline(username, conn_id); });
		}
	}

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

	if (bind(server_fd_, (struct sockaddr *)&address, sizeof(address)) == -1)
		return false;
	if (listen(server_fd_, 10) == -1)
		return false;

	setNonBlocking(server_fd_);

	epoll_fd_ = epoll_create1(0);
	if (epoll_fd_ == -1)
		return false;

	struct epoll_event ev;
	ev.events = EPOLLIN | EPOLLET;
	ev.data.fd = server_fd_;
	epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, server_fd_, &ev);

	// event_fd  用于工作线程唤醒主线程
	event_fd_ = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
	if (event_fd_ == -1) {
		cerr << "eventfd create failed!" << endl;
		return false;
	}

	// 将event_fd也注册到epoll监听队列中
	struct epoll_event ev_eventfd;
	ev_eventfd.events = EPOLLIN | EPOLLET;
	ev_eventfd.data.fd = event_fd_;
	epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, event_fd_, &ev_eventfd);

	thread_pool_ = make_unique<ThreadPool>(10);

	cout << "Epoll Server started on port " << port_ << endl;
	return true;
}

void EpollServer::run() {
	struct epoll_event events[1024]; // 存放就绪事件

	while (true) {
		int nfds = epoll_wait(epoll_fd_, events, 1024, -1);
		if (nfds == -1)
			continue;

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

			if (evs & EPOLLRDHUP || evs & EPOLLHUP || evs & EPOLLERR) {
				cout << "Client " << current_fd << " half-close/error." << endl;

				uint64_t cur_conn_id = 0;
				auto it = clients_.find(current_fd);

				if (it != clients_.end()) {
					cur_conn_id = it->second.conn_id;
				}

				closeConnection(current_fd, cur_conn_id);
				continue;
			}

			if (evs & EPOLLIN)
				handleClientData(current_fd);

			if (evs & EPOLLOUT)
				handleWrite(current_fd);
		}
	}
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
				perror("accept error"); // 其他错误如文件描述符耗尽等
				break;
			}
		}
		setNonBlocking(client_fd);
		// cout << "New client connected! fd = " << client_fd << endl;

		uint64_t new_id = ++global_conn_id_;

		clients_.emplace(client_fd, ClientContext{client_fd, new_id, "", ""});

		struct epoll_event ev;
		ev.events = EPOLLIN | EPOLLET | EPOLLRDHUP;
		ev.data.fd = client_fd;
		epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, client_fd, &ev);
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
			if (it != clients_.end())
				it->second.recv_buffer.append(buffer, bytes_read);
		} else if (bytes_read == 0) { // 客户端断开连接
									  // cout << "Client " << client_fd << " disconnected." << endl;
			uint64_t cur_conn_id = 0;
			auto it = clients_.find(client_fd);

			if (it != clients_.end()) {
				cur_conn_id = it->second.conn_id;
			}

			closeConnection(client_fd, cur_conn_id);
			return;
		} else {
			if (errno == EAGAIN || errno == EWOULDBLOCK) {
				break;
			} else if (errno == EINTR) { // 信号被中断
				continue;
			} else {
				uint64_t cur_conn_id = 0;
				auto it = clients_.find(client_fd);

				if (it != clients_.end())
					cur_conn_id = it->second.conn_id;

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
		if (it == clients_.end())
			return;

		current_conn_id = it->second.conn_id; // 提前获取 conn_id 备用
		string &rb = it->second.recv_buffer;

		if (rb.size() < sizeof(MessageHeader))
			break;

		memcpy(&header, rb.data(), sizeof(MessageHeader));

		if (header.magic != 0x12345678) {
			// cerr << "Client " << client_fd << " sent invaild magic number! Disconnecting." << endl;
			rb.clear();
			is_invaild_packet = true;
		} else if (header.length > 10 * 1024 * 1024) {
			// cerr << "Client " << client_fd << " sent too large packet! Disconnecting." << endl;
			rb.clear();
			is_invaild_packet = true;
		} else if (rb.size() < sizeof(MessageHeader) + header.length) {
			break;
		} else {
			body = rb.substr(sizeof(MessageHeader), header.length);
			rb.erase(0, sizeof(MessageHeader) + header.length);
		}

		if (is_invaild_packet) {
			closeConnection(client_fd, current_conn_id);
			return;
		}

		if (header.type == MSG_REGISTER || header.type == MSG_LOGIN) {
			// 按值捕获client_fd和body 防止垂悬引用
			thread_pool_->enqueue([this, client_fd, current_conn_id, body, type = (MsgType)header.type]() {
				processAuthTask(client_fd, current_conn_id, body, type);
			});
		} else if (header.type == MSG_CHAT) {
			thread_pool_->enqueue(
				[this, client_fd, current_conn_id, body]() { processChatTask(client_fd, current_conn_id, body); });
		}
	}
}

bool EpollServer::sendToClient(int client_fd, uint64_t conn_id, MsgType type, const string &body) {
	auto it = clients_.find(client_fd);

	// 连接还在且没有被复用
	if (it == clients_.end() || it->second.conn_id != conn_id)
		return false;

	auto &ctx = it->second;

	// 计算添加本条消息后的总积压大小
	size_t current_size = ctx.send_buffer.size();
	size_t add_size = sizeof(MessageHeader) + body.size();

	// 设定 10MB 的高压线
	if (current_size + add_size > 10 * 1024 * 1024) {
		cerr << "[WARNING] Client " << client_fd << " send buffer overflow! Force closing." << endl;

		// 直接踢掉这个不读数据的客户端，防止拖垮服务器
		closeConnection(client_fd, conn_id);
		return false;
	}

	MessageHeader header;
	header.magic = 0x12345678;
	header.length = body.size();
	header.type = type;

	ctx.send_buffer.append((char *)&header, sizeof(MessageHeader));
	ctx.send_buffer.append(body);

	while (!ctx.send_buffer.empty()) {
		int bytes_sent = send(client_fd, ctx.send_buffer.data(), ctx.send_buffer.size(), MSG_NOSIGNAL);

		if (bytes_sent > 0) {
			ctx.send_buffer.erase(0, bytes_sent);
		} else if (bytes_sent == -1) {
			if (errno == EAGAIN || errno == EWOULDBLOCK) {
				struct epoll_event ev;
				ev.events = EPOLLIN | EPOLLOUT | EPOLLET | EPOLLRDHUP;
				ev.data.fd = client_fd;

				epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, client_fd, &ev);
				break;
			} else {
				closeConnection(client_fd, conn_id);

				return false;
			}
		}
	}
	return true;
}

void EpollServer::broadcastMessage(int sender_fd, const char *msg, int len) { // 丢失数据版本
	vector<pair<int, uint64_t>> dead_conns;									  // 存放抛弃的客户端

	for (auto &pair : clients_) {
		int fd = pair.first;
		if (fd == sender_fd)
			continue;

		auto &ctx = pair.second;

		// 积压太多直接删掉防止内存被撑爆
		if (ctx.send_buffer.size() > 10 * 1024 * 1024) {
			dead_conns.emplace_back(fd, ctx.conn_id);
			continue;
		}

		ctx.send_buffer.append(msg, len);

		while (!ctx.send_buffer.empty()) {
			int bytes_sent = send(fd, ctx.send_buffer.data(), ctx.send_buffer.size(), MSG_NOSIGNAL);

			if (bytes_sent > 0) {
				ctx.send_buffer.erase(0, bytes_sent);
			} else if (bytes_sent == -1) {
				if (errno == EAGAIN || errno == EWOULDBLOCK) {
					struct epoll_event ev;
					ev.events = EPOLLIN | EPOLLET | EPOLLOUT | EPOLLRDHUP;

					ev.data.fd = fd;
					epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, fd, &ev);

					break;
				} else { // 其它错误 断开连接
					dead_conns.emplace_back(fd, ctx.conn_id);
					break;
				}
			}
		}
	}

	for (auto &p : dead_conns)
		closeConnection(p.first, p.second);
}

void EpollServer::handleWrite(int client_fd) {
	auto it = clients_.find(client_fd);
	if (it == clients_.end())
		return;

	auto &ctx = it->second;
	uint64_t conn_id = ctx.conn_id;

	while (!ctx.send_buffer.empty()) {
		int bytes_sent = send(client_fd, ctx.send_buffer.data(), ctx.send_buffer.size(), MSG_NOSIGNAL);
		if (bytes_sent > 0)
			ctx.send_buffer.erase(0, bytes_sent);
		else {
			if (errno == EAGAIN || errno == EWOULDBLOCK)
				return;
			else {
				closeConnection(client_fd, conn_id);
				return;
			}
		}
	}

	if (ctx.send_buffer.empty()) {
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
		string pwd = j["password"];

		MYSQL *conn = nullptr;
		MysqlConnRAII raii(&conn);

		if (!conn) {
			json res;
			res["message"] = "数据库系统繁忙 请稍后再试";
			string body = res.dump();
			postToIoThread([this, client_fd, conn_id, body]() { sendToClient(client_fd, conn_id, MSG_SYSTEM, body); });
			return;
		}

		char escape_uname[256] = {0};
		// char escape_pwd[256] = {0};
		mysql_real_escape_string(conn, escape_uname, uname.c_str(), uname.length());
		// mysql_real_escape_string(conn, escape_pwd, pwd.c_str(), pwd.length());

		if (type == MSG_REGISTER) {
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
				postToIoThread(
					[this, client_fd, conn_id, body]() { sendToClient(client_fd, conn_id, MSG_SYSTEM, body); });
			} else {
				if (mysql_errno(conn) == 1062) {
					json res;
					res["message"] = "注册失败 用户名已存在";
					string body = res.dump();
					postToIoThread(
						[this, client_fd, conn_id, body]() { sendToClient(client_fd, conn_id, MSG_SYSTEM, body); });

				} else {
					json res;
					res["message"] = "注册失败 服务器内部错误";
					string body = res.dump();
					postToIoThread(
						[this, client_fd, conn_id, body]() { sendToClient(client_fd, conn_id, MSG_SYSTEM, body); });
				}
			}
		} else if (type == MSG_LOGIN) {
			string sql = "SELECT password,salt FROM users WHERE username='" + string(escape_uname) + "'";

			if (mysql_query(conn, sql.c_str()) != 0) {
				json res;
				res["message"] = "数据库连接失败";
				string body = res.dump();
				postToIoThread(
					[this, client_fd, conn_id, body]() { sendToClient(client_fd, conn_id, MSG_SYSTEM, body); });
				return;
			}

			MYSQL_RES *result = mysql_store_result(conn);
			if (result == nullptr) {
				json res;
				res["message"] = "服务器内部错误，获取数据失败";
				string body = res.dump();
				postToIoThread(
					[this, client_fd, conn_id, body]() { sendToClient(client_fd, conn_id, MSG_SYSTEM, body); });

				cerr << "mysql_store_result error: " << mysql_error(conn) << endl;
				return;
			}

			if (mysql_num_rows(result) == 0) {
				json res;
				res["message"] = "登录失败，账号或密码错误";
				string body = res.dump();
				postToIoThread(
					[this, client_fd, conn_id, body]() { sendToClient(client_fd, conn_id, MSG_SYSTEM, body); });
				mysql_free_result(result);

				return;
			}

			MYSQL_ROW row = mysql_fetch_row(result);
			string db_hashed_pwd = row[0];
			string db_salt = row[1];
			string input_hashed = PasswordUtil::hashPassword(pwd, db_salt);

			mysql_free_result(result); // 对比完立即释放结果集  防后续return导致内存泄漏

			if (input_hashed != db_hashed_pwd) {
				json res;
				res["message"] = "登录失败，账号或密码错误";
				string body = res.dump();
				postToIoThread(
					[this, client_fd, conn_id, body]() { sendToClient(client_fd, conn_id, MSG_SYSTEM, body); });
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

			vector<string> offline_msgs;

			RedisClient::getInstance().popOfflineMsgs(uname, offline_msgs);

			if (!offline_msgs.empty()) {
				json off_res;
				off_res["message"] = "您有 " + to_string(offline_msgs.size()) + " 条离线消息";
				string body = off_res.dump();
				postToIoThread(
					[this, client_fd, conn_id, body]() { sendToClient(client_fd, conn_id, MSG_SYSTEM, body); });

				for (const auto &msg : offline_msgs) {
					string msg_body = msg;
					postToIoThread([this, client_fd, conn_id, msg_body]() {
						sendToClient(client_fd, conn_id, MSG_CHAT, msg_body);
					});
				}
			}
		}
	} catch (const exception &e) {
		json res;
		res["message"] = "数据格式错误";
		string body = res.dump();
		postToIoThread([this, client_fd, conn_id, body]() { sendToClient(client_fd, conn_id, MSG_SYSTEM, body); });
	}
}

void EpollServer::processChatTask(int client_fd, uint64_t conn_id, const string &body) {
	string username = SessionManager::getInstance().getUsernameByConnId(conn_id);

	if (username.empty()) {
		// 如果查不到用户名，说明会话已失效
		json res;
		res["message"] = "会话失效 请重新登录";
		string body = res.dump();
		postToIoThread([this, client_fd, conn_id, body]() { sendToClient(client_fd, conn_id, MSG_SYSTEM, body); });
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
				postToIoThread(
					[this, client_fd, conn_id, body]() { sendToClient(client_fd, conn_id, MSG_SYSTEM, body); });
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
					postToIoThread(
						[this, client_fd, conn_id, body]() { sendToClient(client_fd, conn_id, MSG_SYSTEM, body); });
					return;
				}

				if (conn) {
					char escape_target[256] = {0};
					mysql_real_escape_string(conn, escape_target, target_name.c_str(), target_name.length());

					string sql = "SELECT id FROM users WHERE username='" + string(escape_target) + "'";

					if (mysql_query(conn, sql.c_str()) == 0) {
						MYSQL_RES *result = mysql_store_result(conn);

						if (!result) {
							cerr << "获取结果集失败: " << mysql_error(conn) << endl;
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
				postToIoThread(
					[this, client_fd, conn_id, body]() { sendToClient(client_fd, conn_id, MSG_SYSTEM, body); });
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
			postToIoThread([this, client_fd, conn_id, body]() { sendToClient(client_fd, conn_id, MSG_SYSTEM, body); });

		} else {
			MessageHeader out_header;
			out_header.magic = 0x12345678;
			out_header.length = out_body.length();
			out_header.type = MSG_CHAT;
			string out_packet = string((char *)&out_header, sizeof(MessageHeader)) + out_body;

			int sender_fd_copy = client_fd;
			postToIoThread([this, sender_fd_copy, out_packet]() {
				broadcastMessage(sender_fd_copy, out_packet.data(), out_packet.size());
			});
		}

	} catch (const exception &e) {
		json res;
		res["message"] = "数据格式错误";
		string body = res.dump();
		postToIoThread([this, client_fd, conn_id, body]() { sendToClient(client_fd, conn_id, MSG_SYSTEM, body); });
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