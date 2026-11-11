#include "redis_client.h"
#include <cinttypes>
#include <cstdint>
#include <cstring>
#include <iostream>

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

	cout << "Redis Connection Pool Init Success! Size: " << pool_.size() << endl;
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

	// 池子空了 等待
	cv_.wait(lock, [this]() { return !pool_.empty(); });
	redisContext *ctx = pool_.front();
	pool_.pop();
	return ctx;
}

void RedisClient::releaseConnection(redisContext *conn) {
	if (!conn)
		return;
	lock_guard<mutex> lock(pool_mtx_);
	pool_.push(conn);
	cv_.notify_one();
}

void RedisClient::setOnline(const string &username, uint64_t conn_id) {
	redisContext *conn;
	RedisConnRAII raii(&conn);

	char buf[32];
	snprintf(buf, sizeof(buf), "%" PRIu64, conn_id);
	redisReply *reply = (redisReply *)redisCommand(conn, "HSET online_users %s %s", username.c_str(), buf);
	if (reply)
		freeReplyObject(reply);
}

void RedisClient::setOffline(const string &username, uint64_t conn_id) {
	redisContext *conn;
	RedisConnRAII raii(&conn);

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

	if (reply) {
		freeReplyObject(reply);
	}
}

uint64_t RedisClient::getOnlineConnId(const string &username) {
	redisContext *conn = nullptr;
	RedisConnRAII raii(&conn);
	redisReply *reply = (redisReply *)redisCommand(conn, "HGET online_users %s", username.c_str());
	uint64_t conn_id = 0; // 0表示不在线
	if (reply && reply->type == REDIS_REPLY_STRING) {
		conn_id = stoull(reply->str);
	}
	if (reply)
		freeReplyObject(reply);
	return conn_id;
}

void RedisClient::pushOfflineMsg(const string &target, const string &msg) {
	redisContext *conn = nullptr;
	RedisConnRAII raii(&conn);

	const char *lua_script = "redis.call('RPUSH', KEYS[1], ARGV[1]) "
							 "redis.call('LTRIM', KEYS[1], -100, -1) "
							 "redis.call('EXPIRE', KEYS[1], 604800) "
							 "return 1";

	string key = "offline_msg:" + target;

	// 同样使用 redisCommandArgv，防止 msg 里面有空格导致解析失败
	const char *argv[5];
	size_t argvlen[5];

	argv[0] = "EVAL";
	argvlen[0] = 4;
	argv[1] = lua_script;
	argvlen[1] = strlen(lua_script);
	argv[2] = "1";
	argvlen[2] = 1;
	argv[3] = key.c_str();
	argvlen[3] = key.length();
	argv[4] = msg.c_str();
	argvlen[4] = msg.length();

	redisReply *reply = (redisReply *)redisCommandArgv(conn, 5, argv, argvlen);

	if (reply)
		freeReplyObject(reply);
}

void RedisClient::popOfflineMsgs(const string &username, vector<string> &msgs) {
	redisContext *conn = nullptr;
	RedisConnRAII raii(&conn);

	redisReply *reply = (redisReply *)redisCommand(conn, "LRANGE offline_msg:%s 0 -1", username.c_str());
	if (reply && reply->type == REDIS_REPLY_ARRAY) {
		for (size_t i = 0; i < reply->elements; i++) {
			msgs.push_back(reply->element[i]->str);
		}
	}
	if (reply)
		freeReplyObject(reply);
	// 删除列表防止重复推送
	redisReply *del_reply = (redisReply *)redisCommand(conn, "DEL offline_msg:%s", username.c_str());
	if (del_reply)
		freeReplyObject(del_reply);
}

void RedisClient::addRegisteredUser(const string &username) {
	redisContext *conn = nullptr;
	RedisConnRAII raii(&conn);
	redisReply *reply = (redisReply *)redisCommand(conn, "SADD registered_users %s", username.c_str());
	if (reply)
		freeReplyObject(reply);
}

bool RedisClient::isRegistered(const string &username) {
	redisContext *conn = nullptr;
	RedisConnRAII raii(&conn);
	redisReply *reply = (redisReply *)redisCommand(conn, "SISMEMBER registered_users %s", username.c_str());
	bool exists = false;
	if (reply && reply->type == REDIS_REPLY_INTEGER) {
		exists = (reply->integer == 1);
	}
	if (reply)
		freeReplyObject(reply);
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