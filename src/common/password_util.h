#pragma once
#include <string>
using namespace std;

class PasswordUtil {
  public:
	// 生成随机盐-16字节-转32位16进制字符串
	static string generateSalt();

	// 慢哈希计算(PBKDF2-HMAC-SHA256,迭代10000次)
	static string hashPassword(const string &password, const string &salt_hex);
};