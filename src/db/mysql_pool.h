#pragma once
#include <condition_variable>
// #include <memory>
#include <mutex>
#include <mysql/mysql.h>
#include <queue>
#include <string>
using namespace std;

class MysqlPool {
  public:
	static MysqlPool &getInstance();

	void init(const string &host, const string &user, const string &pwd, const string &db, int port, int poolSize = 10);

	MYSQL *getConnection();				 // 获取连接
	void releaseConnection(MYSQL *conn); // 归还连接

  private:
	MysqlPool() = default;
	~MysqlPool();
	MysqlPool(const MysqlPool &) = delete;
	MysqlPool &operator=(const MysqlPool &) = delete;

	queue<MYSQL *> pool_;
	mutex mtx_;
	condition_variable cv_;
};

// 自动获取和归还连接  防止死锁
class MysqlConnRAII {
  public:
	MysqlConnRAII(MYSQL **conn);
	~MysqlConnRAII();

  private:
	MYSQL *conn_;
};