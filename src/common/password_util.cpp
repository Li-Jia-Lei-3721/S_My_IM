#include "password_util.h"
#include <iomanip>
#include <openssl/evp.h>
#include <openssl/rand.h>
#include <sstream>
#include <vector>

static vector<unsigned char> hexToBytes(const string &hex) {
	vector<unsigned char> bytes;
	for (size_t i = 0; i < hex.length(); i += 2) {
		string byteString = hex.substr(i, 2);
		unsigned char byte = (unsigned char)strtol(byteString.c_str(), nullptr, 16);
		bytes.push_back(byte);
	}
	return bytes;
}

string PasswordUtil::generateSalt() {
	unsigned char salt[16];
	// OpenSSL的密码学安全随机数生成器
	if (RAND_bytes(salt, sizeof(salt)) != 1) {
		throw runtime_error("Failed to generate secure salt");
	}

	// 转化为的16进制字符串
	stringstream ss;
	for (int i = 0; i < 16; i++) {
		ss << hex << setw(2) << setfill('0') << (int)salt[i]; // 不可见字符->纯文本
	}

	return ss.str();
}

string PasswordUtil::hashPassword(const string &password, const string &salt_hex) {

	unsigned char hash[32];
	const int iterators = 600000; // 迭代次数
	const int key_len = 32;

	vector<unsigned char> salt_bin = hexToBytes(salt_hex);
	if (salt_bin.size() != 16) {
		throw runtime_error("Invalid salt length");
	}

	int rc = PKCS5_PBKDF2_HMAC(password.c_str(), password.length(), salt_bin.data(), salt_bin.size(), iterators,
							   EVP_sha256(), key_len, hash);

	if (rc != 1) {
		throw runtime_error("PKCS5_PBKDF2_HMAC faild");
	}

	stringstream ss;
	for (int i = 0; i < key_len; i++) {
		ss << hex << setw(2) << setfill('0') << (int)hash[i];
	}
	return ss.str();
}