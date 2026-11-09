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
#include "../db/mysql_pool.h"
#include "../db/redis_client.h"
#include <iostream>
#include <unordered_map>

struct ClientContext {
	int fd;
	string recv_buffer;
	string send_buffer;		   // 缓冲发送
	bool is_logged_in = false; // 是否登录
	string username = "";	   // 用户名
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

	void processAuthTask(int cliend_fd, const string &body, MsgType type);
	void processChatTask(int client_fd, const string &body);

	uint16_t port_;
	int server_fd_;
	int epoll_fd_;

	unordered_map<int, ClientContext> clients_;
	mutex mtx_;

	unique_ptr<ThreadPool> thread_pool_;

	// map<string, string> user_db_;

	void sendToClient(int client_fd, MsgType type, const string &body);
};
