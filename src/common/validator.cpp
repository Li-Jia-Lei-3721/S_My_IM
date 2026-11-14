#include "validator.h"
#include <cctype>

bool Validator::isValidUsername(const string &name) {
	if (name.length() < 3 || name.length() > 20) {
		return false;
	}

	for (char c : name) {
		// 把c变为无符号数传递给isalnum  用static_cast编译时期判断正误
		if (!isalnum(static_cast<unsigned char>(c)) && c != '_') {
			return false;
		}
	}

	return true;
}
