#pragma once
#include <string>
using namespace std;

class Validator {
  public:
	// 校验用户名: 3-20位 仅限字母 数字 下划线
	static bool isValidUsername(const string &name);

	// 工具函数
};