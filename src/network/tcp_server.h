#pragma once
#include <iostream>
#include <stdint.h>
using namespace std;
#include <thread>
#include <vector>
#include <mutex>
#include <algorithm>

class TcpServer {
  public:
    TcpServer(int port);
    ~TcpServer();

    bool start();

	void acceptLoop();

  private:
    void handleClient(int client_fd);

    void broadcastMessage(int sender_fd,const char* msg,int len);
    
    uint16_t port_;
    int server_fd_;

    vector<int> client_fds_;

    mutex mtx_;
};