#include "redis_client.h"
#include "hiredis.h"
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

void RedisClient::setOffline(const string &username) {
	lock_guard<mutex> lock(mtx_);
	redisReply *reply = (redisReply *)redisCommand(context_, "HDEL online_users %s", username.c_str());
	if (reply)
		freeReplyObject(reply);
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
	redisReply *reply = (redisReply *)redisCommand(context_, "RPUSH offline_msg:%s %s", target.c_str(), msg.c_str());
	if (reply)
		freeReplyObject(reply);

	// 保留最近的100条
	reply = (redisReply *)redisCommand(context_, "LTRIM offline_msg:%s -100 -1", target.c_str());
	if (reply)
		freeReplyObject(reply);

	// 设置过期时间
	reply = (redisReply *)redisCommand(context_, "EXPIRE offline_msg:%s 604800", target.c_str());
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