#include "session_manager.h"

SessionManager &SessionManager::getInstance() {
	static SessionManager instence;
	return instence;
}

pair<int, uint64_t> SessionManager::login(const string &username, uint64_t conn_id, int fd) {
	UserShard &us = userShard(username);
	unique_lock<mutex> ulk(us.mtx); // 先锁 user shard

	uint64_t old_conn_id = 0;
	int old_fd = -1;

	// 顶号
	auto it = us.user_to_conn_.find(username);
	if (it != user_to_conn_.end()) {
		old_conn_id = it->second;

		// 通过username得到旧conn_id
		ConnShard &old_cs = connShard(old_conn_id);
		{
			lock_guard<mutex> clk(old_cs.mtx);

			auto fit = old_cs.conn_to_fd_.find(old_conn_id);
			if (fit != old_cs.conn_to_fd_.end()) old_fd = fit->second;

			old_cs.conn_to_user_.erase(old_conn_id);
			old_cs.conn_to_fd_.erase(old_conn_id);
		}
	}

	{
		ConnShard &new_cs = connShard(conn_id);
		lock_guard<mutex> clk(new_cs.mtx);
		new_cs.conn_to_user_[conn_id] = username;
		new_cs.conn_to_fd_[conn_id] = fd;
	}

	us.user_to_conn_[username] = conn_id;
	return {old_fd, old_conn_id};
}

bool SessionManager::logout(const string &username, uint64_t conn_id) {
	UserShard &us = userShard(username);
	unique_lock<mutex> ulk(us.mtx);

	auto it = us.user_to_conn_.find(username);
	if (it == us.user_to_conn_.end() || it->second != conn_id) return false;

	us.user_to_conn_.erase(it);

	ConnShard &cs = connShard(conn_id);
	lock_guard<mutex> clk(cs.mtx);

	cs.conn_to_user_.erase(conn_id);
	cs.conn_to_fd_.erase(conn_id);

	return true;
}

int SessionManager::getFdByUsername(const string &username) {
	UserShard &us = userShard(username);
	unique_lock<mutex> ulk(us.mtx);

	auto it = us.user_to_conn_.find(username);
	if (it == us.user_to_conn_.end()) return -1;

	uint64_t conn_id = it->second;
	ConnShard &cs = connShard(conn_id);
	lock_guard<mutex> clk(cs.mtx);

	auto fd = cs.conn_to_fd_.find(conn_id);
	return fd != cs.conn_to_fd_.end() ? fd->second : -1;
}

int SessionManager::getFdByConnId(uint64_t conn_id) {
	ConnShard &cs = connShard(conn_id);
	lock_guard<mutex> clk(cs.mtx);

	auto it = cs.conn_to_fd_.find(conn_id);

	return it != cs.conn_to_fd_.end() ? it->second : -1;
}

string SessionManager::getUsernameByConnId(uint64_t conn_id) {
	ConnShard &cs = connShard(conn_id);
	lock_guard<mutex> clk(cs.mtx);

	auto it = cs.conn_to_user_.find(conn_id);

	return it != cs.conn_to_user_.end() ? it->second : "";
}

bool SessionManager::isConnIdAlive(const string &username, uint64_t conn_id) {
	UserShard &us = userShard(username);
	lock_guard<mutex> ulk(us.mtx);

	auto it = us.user_to_conn_.find(username);

	return it != us.user_to_conn_.end() && it->second == conn_id ? true : false;
}
