#include "mysql_pool.h"
#include "../common/logger.h"
#include <chrono>
// #include <iostream>
#include "../common/logger.h"
#include <mutex>

MysqlPool &MysqlPool::getInstance() {
	static MysqlPool instance;
	return instance;
}

void MysqlPool::init(const string &host, const string &user, const string &pwd, const string &db, int port,
					 int poolSize) {
	for (int i = 0; i < poolSize; i++) {
		MYSQL *conn = mysql_init(nullptr);
		if (!conn) {
			LOG_ERROR("mysql_init_failed");
			continue;
		}

		int timeout = 3;
		mysql_options(conn, MYSQL_OPT_CONNECT_TIMEOUT, &timeout);

		if (!mysql_real_connect(conn, host.c_str(), user.c_str(), pwd.c_str(), db.c_str(), port, nullptr, 0)) {
			LOG_ERROR("mysql_real_connect_error:%s", mysql_error(conn));
			// cerr << "mysql_real_connect_error:" << mysql_error(conn) << endl;

			mysql_close(conn);
			continue;
		}

		if (mysql_set_character_set(conn, "utf8mb4") != 0) {
			// std::cerr << "Failed to set charset: " << mysql_error(conn) << std::endl;
			LOG_ERROR("Failed to set charset: %s", mysql_error(conn));

			mysql_close(conn);
			continue;
		}
		pool_.push(conn);
	}
	// cout << "MySQL Connection Pool Init Success! Size:" << pool_.size() << endl;
	LOG_INFO("MySQL Connection Pool Init Success! Size: %zu", pool_.size());

	if (pool_.empty()) {
		// 全部连接MySQL失败 程序退出
		// cerr << "CRITICAL ERROR: No MySQL connections available!" << endl;
		LOG_ERROR("CRITICAL ERROR: No MySQL connections available!");
		exit(1);
	}
}

MYSQL *MysqlPool::getConnection() {
	unique_lock<mutex> lock(mtx_);

	if (!cv_.wait_for(lock, chrono::seconds(3), [this]() {
		return !pool_.empty();
	})) {
		// cerr << "Get MySQL connection timeout!" << endl;
		LOG_ERROR("Get MySQL connection timeout!");
		return nullptr;
	}

	MYSQL *conn = pool_.front();
	pool_.pop();
	return conn;
}

void MysqlPool::releaseConnection(MYSQL *conn) {
	if (!conn)
		return;

	lock_guard<mutex> lock(mtx_);
	pool_.push(conn);
	cv_.notify_one();
}

MysqlPool::~MysqlPool() {
	lock_guard<mutex> lock(mtx_);
	while (!pool_.empty()) {
		MYSQL *conn = pool_.front();
		pool_.pop();
		mysql_close(conn);
	}
}

MysqlConnRAII::MysqlConnRAII(MYSQL **conn) {
	*conn = MysqlPool::getInstance().getConnection();
	conn_ = *conn;
}

MysqlConnRAII::~MysqlConnRAII() {
	if (conn_)
		MysqlPool::getInstance().releaseConnection(conn_);
}
