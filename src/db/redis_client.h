#pragma once
#include <hiredis/hiredis.h>
#include <mutex>
#include <string>
#include <vector>

using namespace std;

class RedisClient {
  public:
	static RedisClient &getInstance();
	bool connect(const string &host, int port);
	~RedisClient();

	void setOnline(const string &username, int fd);	 // 用户上线
	void setOffline(const string &username, int fd); // 用户离线
	int getOnlineFd(const string &username);		 // 查询用户状态  在线？离线？

	void pushOfflineMsg(const string &target, const string &msg);	   // 存离线消息
	void popOfflineMsgs(const string &username, vector<string> &msgs); // 发送离线消息

	void addRegisteredUser(const string &username);
	bool isRegistered(const string &username);

  private:
	RedisClient() = default; // 单例模式
	redisContext *context_ = nullptr;
	mutex mtx_;

	RedisClient(const RedisClient &) = delete;
	RedisClient operator=(const RedisClient &) = delete;
};