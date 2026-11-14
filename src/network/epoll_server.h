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
#include <atomic>
#include <chrono>
#include <functional>
#include <queue>
#include <sys/eventfd.h>
#include <unordered_map>

// 用户空间
struct ClientContext {
	int fd;
	uint64_t conn_id = 0;
	string recv_buffer;
	string send_buffer; // 缓冲发送

	// bool is_logged_in = false; // 是否登录
	// string username = "";	   // 用户名

	// 最后一次收到数据的时间
	chrono::steady_clock::time_point last_active_time;
};

// 定时器
struct TimerNode {
	chrono::steady_clock::time_point expire_time;
	int fd;
	uint64_t conn_id;
};

struct TimerCmp {
	// 过期时间	 比较语义为Compare(a,b)=true 优先级a<b
	// 及return a>b实现更早的过期时间b优先级更高
	bool operator()(const TimerNode &a, const TimerNode &b) {
		return a.expire_time > b.expire_time;
	}
};

// 有序集合定时器节点
struct TimerNodeUp {
	chrono::steady_clock::time_point expire_time;
	int fd;
	uint64_t conn_id;

	bool operator<(const TimerNodeUp &other) const {
		return expire_time < other.expire_time;
	}
};

class EpollServer {
  public:
	EpollServer(int port);
	~EpollServer();

	bool start();
	void run(atomic<bool> &running);

  private:
	void setNonBlocking(int fd);
	void handleNewConnection();
	void handleClientData(int client_fd);
	void broadcastMessage(int sender_fd, const char *msg, int len);
	void closeConnection(int client_fd, uint64_t conn_id);
	void handleWrite(int client_fd);

	void processAuthTask(int cliend_fd, uint64_t conn_id, const string &body, MsgType type);
	void processChatTask(int client_fd, uint64_t conn_id, const string &body);

	uint16_t port_;
	int server_fd_;
	int epoll_fd_;

	atomic<uint64_t> global_conn_id_{0};

	unordered_map<int, ClientContext> clients_;
	// unordered_map<uint64_t, int> conn_id_to_fd_; // conn_id->fd映射表

	// mutex mtx_;

	unique_ptr<ThreadPool> thread_pool_;

	// map<string, string> user_db_;

	bool sendToClient(int client_fd, uint64_t conn_id, MsgType type, const string &body);

	int event_fd_;
	mutex task_mtx_;
	queue<function<void()>> io_tasks_;
	void postToIoThread(function<void()> task);

	// 定时器部分
	void addTimer(int fd, uint64_t);
	priority_queue<TimerNode, vector<TimerNode>, TimerCmp> timer_heap_;
};
