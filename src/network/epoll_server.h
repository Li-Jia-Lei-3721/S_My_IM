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
#include <set>
#include <sys/eventfd.h>
#include <unordered_map>
#include <unordered_set>

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

	atomic<uint64_t> global_conn_id_{0}; // 全局自增 -- 连接唯一标号

	unordered_map<int, ClientContext> clients_; // fd-->(conn_id,缓冲区)

	unique_ptr<ThreadPool> thread_pool_;	  // 普通业务池
	unique_ptr<ThreadPool> auth_thread_pool_; // 处理加盐和查库的线程池

	bool sendToClient(int client_fd, uint64_t conn_id, MsgType type, const string &body);

	int event_fd_;
	mutex task_mtx_;
	queue<function<void()>> io_tasks_;
	void postToIoThread(function<void()> task);

	// 定时器部分
	void addTimer(int fd, uint64_t);
	void delTimer(uint64_t conn_id);
	set<TimerNode> timer_set_;
	unordered_map<uint64_t, set<TimerNode>::iterator> timer_map_;

	// MySQL 房间--用户归属
	int getUserIdByUsername(const string &username);
	int createRoom(const string &room_name, int owner_id); // 返回room_id  失败返回-1
	int getRoomIdByName(const string &room_name);
	bool isRoomMember(int room_id, int user_id);
	bool addRoomMember(int room_id, int user_id);
	bool removeRoomMember(int room_id, int user_id);

	// 内存--上线与离线--房间在线成员(conn_id集合)
	unordered_map<int, unordered_set<uint64_t>> room_online_users_; // 房间id->conn_id集合
	void addRoomOnline(int room_id, uint64_t conn_id);
	void removeRoomOnline(int room_id, uint64_t conn_id);
	unordered_set<uint64_t> getRoomOnlineUsers(int room_id);
	void removeUserFromAllRooms(uint64_t conn_id);
};
