#pragma once
#include <cstdint>
#include <mutex>
#include <string>
#include <sys/epoll.h>
using namespace std;
#include "../common/thread_pool.h"
#include "../protocol/message.h"
#include "sub_reactor.h"
#include <atomic>
#include <functional>
#include <sys/eventfd.h>
#include <unordered_map>
#include <unordered_set>

class EpollServer {
  public:
	EpollServer(int port);
	~EpollServer();

	bool start();
	void run(atomic<bool> &running);

  private:
	// ---------- 主 reactor ----------
	void setNonBlocking(int fd);
	void handleNewConnection();

	// ---------- 业务分发（SubReactor 回调进来） ----------
	void onMessage(int client_fd, uint64_t conn_id, const string &body, MsgType type);
	void onConnectionClosed(int client_fd, uint64_t conn_id);

	// ---------- 路由（业务层 -> SubReactor） ----------
	SubReactor *getReactor(uint64_t conn_id);
	bool sendToClient(int client_fd, uint64_t conn_id, MsgType type, const string &body);
	void postToIoThread(uint64_t conn_id, function<void()> task);
	void broadcastMessage(uint64_t sender_conn_id, const char *msg, int len);

	// ---------- 业务处理 ----------
	void processAuthTask(int client_fd, uint64_t conn_id, const string &body, MsgType type);
	void processChatTask(int client_fd, uint64_t conn_id, const string &body);
	void processJoinRoomTask(int client_fd, uint64_t conn_id, const string &body);
	void processRoomChatTask(int client_fd, uint64_t conn_id, const string &body);

	// ---------- 房间 DB ----------
	int getUserIdByUsername(const string &username);
	int createRoom(const string &room_name, int owner_id);
	int getRoomIdByName(const string &room_name);
	bool isRoomMember(int room_id, int user_id);
	bool addRoomMember(int room_id, int user_id);
	bool removeRoomMember(int room_id, int user_id);
	vector<pair<int, string>> getRoomsByUserId(int user_id);

	// ---------- 房间内存 ----------
	void addRoomOnline(int room_id, uint64_t conn_id);
	void removeRoomOnline(int room_id, uint64_t conn_id);
	unordered_set<uint64_t> getRoomOnlineUsers(int room_id);
	void removeUserFromAllRooms(uint64_t conn_id);
	unordered_map<int, unordered_set<uint64_t>> room_online_users_;
	mutable mutex room_mtx_;

	// ---------- 主 reactor 资源 ----------
	int port_;
	int server_fd_;
	int main_epoll_fd_;
	atomic<uint64_t> global_conn_id_{0};

	// ---------- 子 reactor ----------
	vector<unique_ptr<SubReactor>> sub_reactors_;

	// ---------- 线程池 ----------
	unique_ptr<ThreadPool> thread_pool_;	  // 普通业务池
	unique_ptr<ThreadPool> auth_thread_pool_; // 处理加盐和查库的线程池
};
