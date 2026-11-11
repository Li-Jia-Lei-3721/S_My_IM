#pragma once
#include <cstdint>
#include <mutex>
#include <string>
#include <sys/epoll.h>
// #include <vector>
using namespace std;
#include "../protocol/message.h"
// #include <map>
#include "../common/thread_pool.h"
#include <unordered_map>

struct ClientContext {
	int fd;
	uint64_t conn_id = 0;
	string recv_buffer;
	string send_buffer; // 缓冲发送
						// bool is_logged_in = false; // 是否登录
						// string username = "";	   // 用户名
};

class EpollServer {
  public:
	EpollServer(int port);
	~EpollServer();

	bool start();
	void run();

  private:
	void setNonBlocking(int fd);
	void handleNewConnection();
	void handleClientData(int client_fd);
	void broadcastMessage(int sender_fd, const char *msg, int len);
	void closeConnection(int client_fd);
	void handleWrite(int client_fd);

	void processAuthTask(int cliend_fd, uint64_t conn_id, const string &body, MsgType type);
	void processChatTask(int client_fd, uint64_t conn_id, const string &body);

	uint16_t port_;
	int server_fd_;
	int epoll_fd_;

	atomic<uint64_t> global_conn_id_{0};

	unordered_map<int, ClientContext> clients_;
	// unordered_map<uint64_t, int> conn_id_to_fd_; // conn_id->fd映射表

	mutex mtx_;

	unique_ptr<ThreadPool> thread_pool_;

	// map<string, string> user_db_;

	bool sendToClient(int client_fd, uint64_t conn_id, MsgType type, const string &body);
};
