#pragma once
#include <chrono>
#include <condition_variable>
#include <hiredis/hiredis.h>
#include <mutex>
#include <queue>
#include <string>
#include <vector>

using namespace std;

class RedisClient {
  public:
	static RedisClient &getInstance();
	bool init(const string &host, int port, int poolSize = 10);
	~RedisClient();

	void setOnline(const string &username, uint64_t conn_id);  // 用户上线
	void setOffline(const string &username, uint64_t conn_id); // 用户离线
	uint64_t getOnlineConnId(const string &username);		   // 查询用户状态  在线？离线？

	void pushOfflineMsg(const string &target, const string &msg);	   // 存离线消息
	void popOfflineMsgs(const string &username, vector<string> &msgs); // 发送离线消息

	void addRegisteredUser(const string &username);
	bool isRegistered(const string &username);

	redisContext *getConnection();
	void releaseConnection(redisContext *conn);

  private:
	RedisClient() = default; // 单例模式
	mutex pool_mtx_;
	queue<redisContext *> pool_;

	condition_variable cv_;

	RedisClient(const RedisClient &) = delete;
	RedisClient operator=(const RedisClient &) = delete;

	int port_;
	string host_;
};

class RedisConnRAII {
  public:
	RedisConnRAII(redisContext **conn);
	~RedisConnRAII();

	RedisConnRAII(const RedisConnRAII &) = delete;
	RedisConnRAII &operator=(const RedisConnRAII &) = delete;

  private:
	redisContext *conn_;
};