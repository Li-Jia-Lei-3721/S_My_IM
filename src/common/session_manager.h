#pragma once
#include <array>
#include <cstdint>
#include <mutex>
#include <string>
#include <unordered_map>

using namespace std;

class SessionManager {
  public:
	static SessionManager &getInstance();

	// 登录：如果用户在线  返回旧的conn_id
	pair<int, uint64_t> login(const string &username, uint64_t conn_id, int fd);

	// 登出: 只有当conn_id匹配才删除  应对断线重连和fd复用
	bool logout(const string &username, uint64_t conn_id);

	// 查询: 通过用户名找fd
	int getFdByUsername(const string &username);

	// 查询: 通过conn_id找fd和用户名
	int getFdByConnId(uint64_t conn_id);
	string getUsernameByConnId(uint64_t conn_id);

	// 判断conn_id是否存活
	bool isConnIdAlive(const string &username, uint64_t conn_id);

  private:
	SessionManager() = default;
	mutex mtx_;

	static constexpr size_t SHARD_COUNT = 16; // 分片数 2的幂次方 方便%优化为位运算

	struct UserShard { // user相关:按username hash分片
		mutex mtx;
		unordered_map<string, uint64_t> user_to_conn;
	};

	struct ConnShard { // conn_id相关 两个map共用一把锁  都说按conn_id查
		mutex mtx;
		unordered_map<uint64_t, string> conn_to_user;
		unordered_map<uint64_t, int> conn_to_id;
	};

	array<UserShard, SHARD_COUNT> user_shards_;
	array<ConnShard, SHARD_COUNT> conn_shards_;

	UserShard &userShard(const string &u) {
		return user_shards_[hash<string>{}(u) % SHARD_COUNT];
	}

	ConnShard &connShard(uint64_t conn_id) {
		return conn_shards_[conn_id % SHARD_COUNT];
	}

	// 映射表
	unordered_map<string, uint64_t> user_to_conn_; // username->conn_id
	unordered_map<uint64_t, string> conn_to_user_; // conn_id->username
	unordered_map<uint64_t, int> conn_to_fd_;	   // conn_id->fd
};