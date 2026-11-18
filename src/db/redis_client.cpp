#include "redis_client.h"
#include "../common/logger.h"
#include <atomic>
#include <cinttypes>
#include <cstdint>
#include <cstring>
#include <iostream>

static atomic<uint64_t> global_req_id{0}; // 全局自增id 防时间戳member碰撞

RedisClient &RedisClient::getInstance() {
	static RedisClient instance;
	return instance;
}

bool RedisClient::init(const string &host, int port, int poolSize) {
	host_ = host;
	port_ = port;

	for (int i = 0; i < poolSize; i++) {
		redisContext *ctx = redisConnect(host.c_str(), port);
		if (ctx == nullptr || ctx->err) {
			if (ctx) {
				cerr << "Redis connect error: " << ctx->errstr << endl;
				redisFree(ctx);
			}
			cerr << "Can't allocate redis context" << endl;
			continue;
		}
		pool_.push(ctx);
	}

	// cout << "Redis Connection Pool Init Success! Size: " << pool_.size() << endl;
	LOG_INFO("Redis Connection Pool Init Success! Size: %zu", pool_.size());
	return !pool_.empty();
}

RedisClient::~RedisClient() {
	while (!pool_.empty()) {
		redisContext *ctx = pool_.front();
		pool_.pop();

		redisFree(ctx);
	}
}

redisContext *RedisClient::getConnection() {
	unique_lock<mutex> lock(pool_mtx_);

	// 池子空了 等三秒  超时返回nullptr防止死等
	if (!cv_.wait_for(lock, chrono::seconds(3), [this]() {
		return !pool_.empty();
	})) {
		LOG_INFO("Get Redis connection timeout!");
		// cerr << "Get Redis connection timeout!" << endl;
		return nullptr;
	};

	redisContext *ctx = pool_.front();
	pool_.pop();

	// 检查连接是否死掉
	if (ctx->err) {
		LOG_ERROR("Redis connection lost,reconnecting...");
		redisFree(ctx);

		ctx = redisConnect(host_.c_str(), port_);
		if (ctx == nullptr || ctx->err) {
			if (ctx) {
				LOG_ERROR("Redis reconnect error: %s", ctx->errstr);
				redisFree(ctx);
			}
			return nullptr;
		}
	}

	return ctx;
}

void RedisClient::releaseConnection(redisContext *conn) {
	if (!conn) return;
	lock_guard<mutex> lock(pool_mtx_);

	pool_.push(conn);
	cv_.notify_one();
}

void RedisClient::setOnline(const string &username, uint64_t conn_id) {
	redisContext *conn;
	RedisConnRAII raii(&conn);

	if (!conn) return;

	char buf[32];
	snprintf(buf, sizeof(buf), "%" PRIu64, conn_id);
	redisReply *reply = (redisReply *)redisCommand(conn, "HSET online_users %s %s", username.c_str(), buf);

	if (reply) freeReplyObject(reply);
}

void RedisClient::setOffline(const string &username, uint64_t conn_id) {
	redisContext *conn;
	RedisConnRAII raii(&conn);

	if (!conn) return;

	const char *lua_script = "if redis.call('HGET', KEYS[1], ARGV[1]) == ARGV[2] then "
							 "  return redis.call('HDEL', KEYS[1], ARGV[1]) "
							 "else "
							 "  return 0 "
							 "end";

	char buf[32];
	snprintf(buf, sizeof(buf), "%" PRIu64, conn_id);

	// 使用 redisCommandArgv 安全传参
	// EVAL script numkeys key1 arg1 arg2
	const char *argv[6] = {"EVAL", lua_script, "1", "online_users", username.c_str(), buf};

	size_t argvlen[6] = {4, strlen(lua_script), 1, 12, username.length(), strlen(buf)};

	redisReply *reply = (redisReply *)redisCommandArgv(conn, 6, argv, argvlen);

	if (reply) freeReplyObject(reply);
}

uint64_t RedisClient::getOnlineConnId(const string &username) {
	redisContext *conn = nullptr;
	RedisConnRAII raii(&conn);

	if (!conn) return 0;
	redisReply *reply = (redisReply *)redisCommand(conn, "HGET online_users %s", username.c_str());

	uint64_t conn_id = 0; // 0表示不在线
	if (reply && reply->type == REDIS_REPLY_STRING) {
		conn_id = stoull(reply->str);
	}

	if (reply) freeReplyObject(reply);
	return conn_id;
}

void RedisClient::pushOfflineMsg(const string &target, const string &msg) {
	redisContext *conn = nullptr;
	RedisConnRAII raii(&conn);

	if (!conn) return;

	const char *lua_script = "redis.call('RPUSH', KEYS[1], ARGV[1]) "
							 "redis.call('LTRIM', KEYS[1], -100, -1) "
							 "redis.call('EXPIRE', KEYS[1], 604800) "
							 "return 1";

	string key = "offline_msg:" + target;

	// 同样使用 redisCommandArgv，防止 msg 里面有空格导致解析失败
	const char *argv[5];
	size_t argvlen[5];

	// clang-format off
	argv[0] = "EVAL";		argvlen[0] = 4;
	argv[1] = lua_script;	argvlen[1] = strlen(lua_script);
	argv[2] = "1";			argvlen[2] = 1;
	argv[3] = key.c_str();	argvlen[3] = key.length();
	argv[4] = msg.c_str();	argvlen[4] = msg.length();
	// clang-format on

	redisReply *reply = (redisReply *)redisCommandArgv(conn, 5, argv, argvlen);

	if (reply) freeReplyObject(reply);
}

void RedisClient::popOfflineMsgs(const string &username, vector<string> &msgs) {
	redisContext *conn = nullptr;
	RedisConnRAII raii(&conn);

	if (!conn) return;

	// 使用 Lua 脚本将 LRANGE 和 LTRIM 原子化
	const char *lua_script = "local msgs = redis.call('LRANGE', KEYS[1], 0, -1) "
							 "local len = #msgs "
							 "if len > 0 then "
							 "		redis.call('LTRIM',KEYS[1],len,-1) "
							 "end "
							 "return msgs";

	string key = "offline_msg:" + username;

	const char *argv[4] = {"EVAL", lua_script, "1", key.c_str()};
	size_t argvlen[4] = {4, strlen(lua_script), 1, key.length()};

	redisReply *reply = (redisReply *)redisCommandArgv(conn, 4, argv, argvlen);

	if (reply && reply->type == REDIS_REPLY_ARRAY) {
		for (size_t i = 0; i < reply->elements; i++) {
			msgs.push_back(reply->element[i]->str);
		}
	}

	if (reply) freeReplyObject(reply);
}

void RedisClient::addRegisteredUser(const string &username) {
	redisContext *conn = nullptr;
	RedisConnRAII raii(&conn);

	if (!conn) return;
	redisReply *reply = (redisReply *)redisCommand(conn, "SADD registered_users %s", username.c_str());

	if (reply) freeReplyObject(reply);
}

bool RedisClient::isRegistered(const string &username) {
	redisContext *conn = nullptr;
	RedisConnRAII raii(&conn);

	if (!conn) {
		// cerr << "Redis conn is null in isRegistered" << endl;
		LOG_ERROR("Redis conn is null in isRegistered");
		return false; // 默认当作未注册处理
	}

	redisReply *reply = (redisReply *)redisCommand(conn, "SISMEMBER registered_users %s", username.c_str());
	bool exists = false;
	if (reply && reply->type == REDIS_REPLY_INTEGER) {
		exists = (reply->integer == 1);
	}
	if (reply) freeReplyObject(reply);
	return exists;
}

RedisConnRAII::RedisConnRAII(redisContext **conn) {
	*conn = RedisClient::getInstance().getConnection();
	conn_ = *conn;
}

RedisConnRAII::~RedisConnRAII() {
	if (conn_) {
		RedisClient::getInstance().releaseConnection(conn_);
	}
}

long long RedisClient::incr(const string &key) {
	redisContext *conn = nullptr;
	RedisConnRAII raii(&conn);

	if (!conn) return 0;
	redisReply *reply = (redisReply *)redisCommand(conn, "INCR %s", key.c_str());

	long long val = 0;
	if (reply && reply->type == REDIS_REPLY_INTEGER) {
		val = reply->integer;
	}

	if (reply) freeReplyObject(reply);

	return val;
}

void RedisClient::expire(const string &key, int seconds) {
	redisContext *conn = nullptr;
	RedisConnRAII raii(&conn);

	if (!conn) return;
	redisReply *reply = (redisReply *)redisCommand(conn, "EXPIRE %s %d", key.c_str());

	if (reply) freeReplyObject(reply);
}

void RedisClient::setString(const string &key, const string &value, int expireSeconds) {
	redisContext *conn = nullptr;
	RedisConnRAII raii(&conn);

	if (!conn) return;
	redisReply *reply = (redisReply *)redisCommand(conn, "SETEX %s %d %s", key.c_str(), expireSeconds, value.c_str());

	if (reply) freeReplyObject(reply);
}

string RedisClient::getString(const string &key) {
	redisContext *conn = nullptr;
	RedisConnRAII raii(&conn);

	if (!conn) return "";
	redisReply *reply = (redisReply *)redisCommand(conn, "GET %s", key.c_str());

	string val = "";
	if (reply && reply->type == REDIS_REPLY_STRING) {
		val = reply->str;
	}

	if (reply) freeReplyObject(reply);
	return val;
}

void RedisClient::del(const string &key) {
	redisContext *conn = nullptr;
	RedisConnRAII raii(&conn);

	if (!conn) return;
	redisReply *reply = (redisReply *)redisCommand(conn, "DEL %s", key.c_str());

	if (reply) freeReplyObject(conn);
}

bool RedisClient::isAllowedBySlidingWindow(const string &key, int windowSeconds, int maxRequests) {
	redisContext *conn = nullptr;
	RedisConnRAII raii(&conn);

	if (!conn) return false;
	const char *lua_script =
		"local current_time = tonumber(ARGV[1]) " // 记录当前时间
		"local window = tonumber(ARGV[2]) "		  // 窗口大小
		"local max_req = tonumber(ARGV[3]) "	  // 最大请求数
		"local unique_id = ARGV[4] "			  // 绝对唯一id
		"-- 移除时间窗口之外的历史请求 "
		"redis.call('ZREMRANGEBYSCORE', KEYS[1], 0, current_time - window) " // 移除从0 到 以现在为基准窗口之前的数据
		"local count = redis.call('ZCARD',KEYS[1]) "						 // 请求数量
		"if count < max_req then "											 // 请求数量<最大数量允许请求
		"	redis.call('ZADD',KEYS[1], current_time, current_time .. ':' .. unique_id) " // 发起请求插入集合
		"	if count == 0 then "
		"		redis.call('PEXPIRE', KEYS[1], window + 900000) "
		"	end "
		"	return 1 "
		"else "
		"	return 0 "
		"end ";

	auto now = chrono::system_clock::now();
	auto time_ms = chrono::duration_cast<chrono::milliseconds>(now.time_since_epoch()).count();

	// 获取唯一id
	uint64_t req_id = ++global_req_id;

	string time_str = to_string(time_ms);
	string window_ms_str = to_string(windowSeconds * 1000);
	string max_str = to_string(maxRequests);
	string unique_id_str = to_string(req_id);

	const char *argv[5] = {"EVAL", lua_script, "1", key.c_str(), time_str.c_str()};
	size_t argvlen[5] = {4, strlen(lua_script), 1, key.length(), time_str.length()};

	// clang-format off
	const char *full_argv[8];				size_t full_argvlen[8];
	for (int i = 0; i < 5; i++) {
		full_argv[i] = argv[i];
		full_argvlen[i] = argvlen[i];
	}	
	full_argv[5] = window_ms_str.c_str(); 	full_argvlen[5] = window_ms_str.length();
    full_argv[6] = max_str.c_str(); 		full_argvlen[6] = max_str.length();
    full_argv[7] = unique_id_str.c_str(); 	full_argvlen[7] = unique_id_str.length();
	// clang-format on

	redisReply *reply = (redisReply *)redisCommandArgv(conn, 8, full_argv, full_argvlen);

	bool allowed = false;
	if (reply && reply->type == REDIS_REPLY_INTEGER) {
		allowed = (reply->integer == 1);
	}
	else if (reply->type == REDIS_REPLY_INTEGER) {
		LOG_ERROR("isAllowedBySlidingWindow Lua error: %s", reply->str);
	}

	if (reply) freeReplyObject(reply);

	return allowed;
}

bool RedisClient::verifyAndConsumeCode(const string &key, const string &client_code) {
	redisContext *conn = nullptr;
	RedisConnRAII raii(&conn);
	if (!conn) return false;

	// 【核心 Lua】：如果 Redis 里的值和我传进来的一样，就删掉它并返回 1；否则返回 0
	const char *lua_script = "if redis.call('GET', KEYS[1]) == ARGV[1] then "
							 "    redis.call('DEL', KEYS[1]) "
							 "    return 1 "
							 "else "
							 "    return 0 "
							 "end";

	const char *argv[4] = {"EVAL", lua_script, "1", key.c_str()};
	size_t argvlen[4] = {4, strlen(lua_script), 1, key.length()};

	// 拼接 client_code 参数
	const char *full_argv[5];
	size_t full_argvlen[5];
	for (int i = 0; i < 4; ++i) {
		full_argv[i] = argv[i];
		full_argvlen[i] = argvlen[i];
	}

	full_argv[4] = client_code.c_str();
	full_argvlen[4] = client_code.length();

	redisReply *reply = (redisReply *)redisCommandArgv(conn, 5, full_argv, full_argvlen);
	bool success = false;
	if (reply && reply->type == REDIS_REPLY_INTEGER) {
		success = (reply->integer == 1);
	}

	if (reply) freeReplyObject(reply);
	return success;
}