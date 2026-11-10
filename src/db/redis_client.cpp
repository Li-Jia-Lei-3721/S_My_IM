#include "redis_client.h"
#include "hiredis.h"
#include <cstring>
#include <iostream>

RedisClient &RedisClient::getInstance() {
	static RedisClient instance;
	return instance;
}

bool RedisClient::connect(const string &host, int port) {
	context_ = redisConnect(host.c_str(), port);
	if (context_ == nullptr || context_->err) {
		if (context_) {
			cerr << "Redis connect error: " << context_->errstr << endl;
			redisFree(context_);
			context_ = nullptr;
		} else {
			cerr << "Redis connect error: can't allocate redis context" << endl;
		}
		return false;
	}
	return true;
}

RedisClient::~RedisClient() {
	if (context_)
		redisFree(context_);
}

void RedisClient::setOnline(const string &username, int fd) {
	lock_guard<mutex> lock(mtx_);
	redisReply *reply = (redisReply *)redisCommand(context_, "HSET online_users %s %d", username.c_str(), fd);
	if (reply)
		freeReplyObject(reply);
}

void RedisClient::setOffline(const string &username, int fd) {
	lock_guard<mutex> lock(mtx_);

	const char *lua_script = "if redis.call('HGET', KEYS[1], ARGV[1]) == ARGV[2] then "
							 "  return redis.call('HDEL', KEYS[1], ARGV[1]) "
							 "else "
							 "  return 0 "
							 "end";

	string fd_str = to_string(fd);

	// 使用 redisCommandArgv 安全传参
	// EVAL script numkeys key1 arg1 arg2
	const char *argv[5];
	size_t argvlen[5];

	argv[0] = "EVAL";
	argvlen[0] = 4;
	argv[1] = lua_script;
	argvlen[1] = strlen(lua_script);
	argv[2] = "1";
	argvlen[2] = 1;
	argv[3] = "online_users";
	argvlen[3] = 12;
	argv[4] = username.c_str();
	argvlen[4] = username.length();

	// 把 fd_str拼进去
	const char *argv_full[6];
	size_t argvlen_full[6];
	for (int i = 0; i < 5; i++) {
		argv_full[i] = argv[i];
		argvlen_full[i] = argvlen[i];
	}
	argv_full[5] = fd_str.c_str();
	argvlen_full[5] = fd_str.length();

	redisReply *reply = (redisReply *)redisCommandArgv(context_, 6, argv_full, argvlen_full);

	if (reply) {
		freeReplyObject(reply);
	}
}

int RedisClient::getOnlineFd(const string &username) {
	lock_guard<mutex> lock(mtx_);
	redisReply *reply = (redisReply *)redisCommand(context_, "HGET online_users %s", username.c_str());
	int fd = -1;
	if (reply && reply->type == REDIS_REPLY_STRING) {
		fd = stoi(reply->str);
	}
	if (reply)
		freeReplyObject(reply);
	return fd;
}

void RedisClient::pushOfflineMsg(const string &target, const string &msg) {
	lock_guard<mutex> lock(mtx_);

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

	redisReply *reply = (redisReply *)redisCommandArgv(context_, 5, argv, argvlen);

	if (reply)
		freeReplyObject(reply);
}

void RedisClient::popOfflineMsgs(const string &username, vector<string> &msgs) {
	lock_guard<mutex> lock(mtx_);
	redisReply *reply = (redisReply *)redisCommand(context_, "LRANGE offline_msg:%s 0 -1", username.c_str());
	if (reply && reply->type == REDIS_REPLY_ARRAY) {
		for (size_t i = 0; i < reply->elements; i++) {
			msgs.push_back(reply->element[i]->str);
		}
	}
	if (reply)
		freeReplyObject(reply);
	// 删除列表防止重复推送
	redisReply *del_reply = (redisReply *)redisCommand(context_, "DEL offline_msg:%s", username.c_str());
	if (del_reply)
		freeReplyObject(del_reply);
}

void RedisClient::addRegisteredUser(const string &username) {
	lock_guard<mutex> lock(mtx_);
	redisReply *reply = (redisReply *)redisCommand(context_, "SADD registered_users %s", username.c_str());
	if (reply)
		freeReplyObject(reply);
}

bool RedisClient::isRegistered(const string &username) {
	lock_guard<mutex> lock(mtx_);
	redisReply *reply = (redisReply *)redisCommand(context_, "SISMEMBER registered_users %s", username.c_str());
	bool exists = false;
	if (reply && reply->type == REDIS_REPLY_INTEGER) {
		exists = (reply->integer == 1);
	}
	if (reply)
		freeReplyObject(reply);
	return exists;
}