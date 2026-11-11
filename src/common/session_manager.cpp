#include "session_manager.h"

SessionManager &SessionManager::getInstance() {
	static SessionManager instence;
	return instence;
}

uint64_t SessionManager::login(const string &username, uint64_t conn_id, int fd) {
	lock_guard<mutex> lock(mtx_);
	uint64_t old_conn_id = 0;

	// 顶号
	auto it = user_to_conn_.find(username);
	if (it != user_to_conn_.end()) {
		old_conn_id = it->second;
		// 清理旧连接映射
		conn_to_user_.erase(old_conn_id);
		conn_to_fd_.erase(old_conn_id);
	}

	// 记录新映射
	user_to_conn_[username] = conn_id;
	conn_to_user_[conn_id] = username;
	conn_to_fd_[conn_id] = fd;

	return old_conn_id;
}

bool SessionManager::logout(const string &username, uint64_t conn_id) {
	lock_guard<mutex> lock(mtx_);

	auto it = user_to_conn_.find(username);
	if (it != user_to_conn_.end() && it->second == conn_id) {
		user_to_conn_.erase(it);
		conn_to_user_.erase(conn_id);
		conn_to_fd_.erase(conn_id);

		return true;
	}

	return false;
}

int SessionManager::getFdByUsername(const string &username) {
	lock_guard<mutex> lock(mtx_);

	auto it = user_to_conn_.find(username);
	if (it != user_to_conn_.end()) {
		uint64_t conn_id = it->second;

		auto fd_it = conn_to_fd_.find(conn_id);
		if (fd_it != conn_to_fd_.end()) {
			return fd_it->second;
		}
	}

	return -1;
}

int SessionManager::getFdByConnId(uint64_t conn_id) {
	lock_guard<mutex> lock(mtx_);

	auto it = conn_to_fd_.find(conn_id);

	return it != conn_to_fd_.end() ? it->second : -1;
}

string SessionManager::getUsernameByConnId(uint64_t conn_id) {
	lock_guard<mutex> lock(mtx_);

	auto it = conn_to_user_.find(conn_id);

	return it != conn_to_user_.end() ? it->second : "";
}

bool SessionManager::isConnIdAlive(const string &username, uint64_t conn_id) {
	lock_guard<mutex> lock(mtx_);

	auto it = user_to_conn_.find(username);

	return it != user_to_conn_.end() && it->second == conn_id ? true : false;
}
