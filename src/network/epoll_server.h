#pragma once
#include <iostream>
#include <vector>
#include <mutex>
#include <sys/epoll.h>
#include <cstdint>
using namespace std;
#include <unordered_map>
#include "../protocol/message.h"
#include <map>
#include "../db/mysql_pool.h"

struct ClientContext {
        int fd;
        string recv_buffer;
        string send_buffer; // 缓冲发送
        bool is_logged_in = false;	//是否登录
        string username = "";		//用户名
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
        
    uint16_t port_;
    int server_fd_;
    int epoll_fd_;

    unordered_map<int,ClientContext> clients_;
    mutex mtx_; // 保留以防后续扩展

    //map<string, string> user_db_;

    void sendToClient(int client_fd, MsgType type, const string &body);
};
