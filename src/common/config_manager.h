#pragma once
#include "../third_party/json.hpp"
#include <string>
using namespace std;
using json = nlohmann::json;

class ConfigManager {
  public:
	static ConfigManager &getInstenance();

	// 加载配置文件
	bool load(const string &filename);

	// 获取JSON节点  调用者提取字段
	json getConfig() const {
		return config_;
	}

  private:
	ConfigManager() = default;
	json config_;
};
