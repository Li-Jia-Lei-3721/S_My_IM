#pragma once
#include "../protocol/message.h"
#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <mutex>
#include <queue>
#include <set>
#include <string>
#include <sys/epoll.h>
#include <thread>
#include <unordered_map>

using namespace std;

// 用户空间
struct ClientContext {
	int fd;
	uint64_t conn_id = 0;
	string recv_buffer;
	string send_buffer; // 缓冲发送

	// 偏移量指针
	size_t recv_offset = 0;
	size_t send_offset = 0;

	// 最后一次收到数据的时间
	chrono::steady_clock::time_point last_active_time;
};

// 定时器
struct TimerNode {
	chrono::steady_clock::time_point expire_time;
	int fd;
	uint64_t conn_id;

	bool operator<(const TimerNode &other) const {
		return expire_time < other.expire_time;
	}
};

class SubReactor {
  public:
	using MessageCallback = function<void(int fd, uint64_t conn_id, const string &body, MsgType type)>;
	using CloseCallback = function<void(int fd, uint64_t conn_id)>;

	explicit SubReactor(int id); // explicit 不允许隐式类型转换 只对单参数有意义
	~SubReactor();

	bool init();  // 建epoll	+ eventfd
	void start(); // 起线程跑run()
	void stop();

	// clang-format off
	int id() const { return id_; }
	int epollFd() const { return epoll_fd_; }

	void setMessageCallback(MessageCallback cb) { on_message_ = std::move(cb); }
	void setCloseCallback(CloseCallback cb) { on_close_ = std::move(cb); }
	// clang-format on

	// 主线程调用(线程安全 内部投递到本 reactor 线程)
	void addConnection(int client_fd, uint64_t conn_id);

	// 任意线程调用 投递任务到本 reactor
	void postTask(function<void()> task);

	bool sendToClient(int client_fd, uint64_t conn_id, MsgType type, const string &body);
	void closeConnection(int client_fd, uint64_t conn_id);
	void broadcastMessage(uint64_t sender_conn_id, const char *msg, int len);

  private:
	void run();
	void handleClientData(int client_fd);
	void handleWrite(int client_fd);
	void handleEventfd();

	//
	void addTimer(int fd, uint64_t conn_id);
	void delTimer(uint64_t conn_id);
	void checkTimers();

	static void setNonBlocking(int fd);

	int id_;
	int epoll_fd_ = -1;
	int event_fd_ = -1;
	atomic<bool> stop_{false};
	thread thread_;

	unordered_map<int, ClientContext> clients_;
	set<TimerNode> timer_set_;
	unordered_map<uint64_t, set<TimerNode>::iterator> timer_map_;

	mutex task_mtx_;
	queue<function<void()>> io_tasks_;

	MessageCallback on_message_;
	CloseCallback on_close_;
};