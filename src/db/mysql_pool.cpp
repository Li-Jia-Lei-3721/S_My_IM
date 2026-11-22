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

MYSQL *MysqlPool::createConnection() {
	MYSQL *conn = mysql_init(nullptr);
	if (!conn) return nullptr;

	int timeout = 3;
	mysql_options(conn, MYSQL_OPT_CONNECT_TIMEOUT, &timeout);

	if (!mysql_real_connect(conn, host_.c_str(), user_.c_str(), pwd_.c_str(), db_.c_str(), port_, nullptr, 0)) {
		LOG_ERROR("mysql_real_connect_error:%s", mysql_error(conn));
		mysql_close(conn);
		return nullptr;
	}

	if (mysql_set_character_set(conn, "utf8mb4") != 0) {
		LOG_ERROR("Failed to set charset: %s", mysql_error(conn));
		mysql_close(conn);
		return nullptr;
	}

	return conn;
}

void MysqlPool::init(const string &host, const string &user, const string &pwd, const string &db, int port,
					 int poolSize) {
	// clang-format off
	host_ = host;	user_ = user;	pwd_ = pwd;
	db_ = db;		port_ = port;	// 记录配置
	// clang-format on

	for (int i = 0; i < poolSize; i++) {
		MYSQL *conn = createConnection();
		if (conn) pool_.push(conn);
	}

	LOG_INFO("MySQL Connection Pool Init Success! Size: %zu", pool_.size());

	if (pool_.empty()) {
		// 全部连接MySQL失败 程序退出
		LOG_ERROR("CRITICAL ERROR: No MySQL connections available!");
		exit(1);
	}
}

MYSQL *MysqlPool::getConnection() {
	unique_lock<mutex> lock(mtx_);

	if (!cv_.wait_for(lock, chrono::seconds(3), [this]() {
		return !pool_.empty();
	})) {
		LOG_ERROR("Get MySQL connection timeout!");
		return nullptr;
	}

	MYSQL *conn = pool_.front();
	pool_.pop();

	if (mysql_ping(conn) != 0) { // 连接毁了 原地复活
		mysql_close(conn);
		conn = createConnection();

		if (conn) {
			pool_.front() = conn;
		}
		else {
			pool_.pop();
			return nullptr;
		}
	}
	else {
		pool_.pop();
	}

	return conn;
}

void MysqlPool::releaseConnection(MYSQL *conn) {
	if (!conn) return;

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
	if (conn_) {
		MysqlPool::getInstance().releaseConnection(conn_);
	}
}
