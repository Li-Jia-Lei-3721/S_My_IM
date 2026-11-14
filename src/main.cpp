// #include "network/tcp_server.h"
#include "common/logger.h"
#include "db/mysql_pool.h"
#include "db/redis_client.h"
#include "network/epoll_server.h"
// #include <iostream>
#include "common/config_manager.h"
#include <string>
using namespace std;
#include <atomic>
#include <csignal>
#include <iostream>

// 原子操作--进程的退出
atomic<bool> g_running{true};

void signalHandler(int signum) {
	// 信号处理函数内部不能进行cout/cerr输出
	g_running = false;
}

int main() {
	// 安全退出进程
	signal(SIGINT, signalHandler);

	// 加载配置文件
	if (!ConfigManager::getInstenance().load("config.json")) {
		return -1; // 加载失败退出
	}

	// 拿到json化的配置文件
	auto config = ConfigManager::getInstenance().getConfig();

	// 初始化异步日志，日志将写入 server.log，最低级别为 INFO
	string log_path = config["log"]["path"];
	Logger::getInstance().init(log_path, (LogLevel)config["log"]["level"].get<int>());
	LOG_INFO("IM Server Starting...");

	// 初始化MySQL连接池
	auto mysql_cfg = config["mysql"];
	MysqlPool::getInstance().init(mysql_cfg["host"], mysql_cfg["user"], mysql_cfg["password"], mysql_cfg["dbname"],
								  mysql_cfg["port"], mysql_cfg["pool_size"]);

	// 初始化Redis连接池
	auto redis_cfg = config["redis"];
	RedisClient::getInstance().init(redis_cfg["host"], redis_cfg["port"], redis_cfg["pool_size"]);

	// 启动服务器
	int port = config["server"]["port"];
	EpollServer server(port);
	if (!server.start()) {
		LOG_ERROR("Server faild to start");
		return 1;
	}

	server.run(g_running);

	LOG_INFO("IM Server down...");
	return 0;
}