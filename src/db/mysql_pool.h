#pragma once
#include <mysql/mysql.h>
#include <string>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <memory>
using namespace std;

class MysqlPool {
  public:
    static MysqlPool &getInstance();
    
    void init(const string &host, const string &user, const string &pwd, const string &db, int port, int poolSize = 10);

    MYSQL *getConnection();
    
    void releaseConnection(MYSQL* conn);

  private:
    MysqlPool() = default;
    ~MysqlPool();
    MysqlPool(const MysqlPool &) = delete;
    MysqlPool &operator=(const MysqlPool &) = delete;

    queue<MYSQL *> pool_;
    mutex mtx_;
    condition_variable cv_;
};

class MysqlConnRAII {
  public:
    MysqlConnRAII(MYSQL * *conn);
    ~MysqlConnRAII();

  private:
    MYSQL* conn_;
};