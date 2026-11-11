#pragma once
#include <cstdint>
#include <mutex>
#include <string>
#include <unordered_map>

using namespace std;

class SessionManager {
  public:
	static SessionManager &getInstance();

	// 登录：如果用户在线  返回旧的conn_id
	uint64_t login(const string &username, uint64_t conn_id, int fd);

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

	// 映射表
	unordered_map<string, uint64_t> user_to_conn_; // username->conn_id
	unordered_map<uint64_t, string> conn_to_user_; // conn_id->username
	unordered_map<uint64_t, int> conn_to_fd_;	   // conn_id->fd
};