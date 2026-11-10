// #include "network/tcp_server.h"
#include "db/mysql_pool.h"
#include "db/redis_client.h"
#include "network/epoll_server.h"

int main() {
	cout << "IM Server Starting..." << endl;

	MysqlPool::getInstance().init("127.0.0.1", "root", "Root@123456", "im_db", 3306, 10);

	RedisClient::getInstance().init("127.0.0.1", 6379, 10);

	EpollServer server(8080);
	if (!server.start()) {
		return 1;
	}

	server.run();

	return 0;
}