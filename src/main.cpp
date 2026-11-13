// #include "network/tcp_server.h"
#include "common/logger.h"
#include "db/mysql_pool.h"
#include "db/redis_client.h"
#include "network/epoll_server.h"
// #include <iostream>

int main() {

	// 初始化异步日志，日志将写入 server.log，最低级别为 INFO
	Logger::getInstance().init("server.log", L_INFO);
	LOG_INFO("IM Server Starting...");

	MysqlPool::getInstance().init("127.0.0.1", "root", "Root@123456", "im_db", 3306, 10);

	RedisClient::getInstance().init("127.0.0.1", 6379, 10);

	EpollServer server(8080);
	if (!server.start()) {
		return 1;
	}

	server.run();

	return 0;
}