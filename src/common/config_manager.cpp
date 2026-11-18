#include "config_manager.h"
#include <fstream>
#include <iostream>

ConfigManager &ConfigManager::getInstenance() {
	static ConfigManager instance;
	return instance;
}

bool ConfigManager::load(const string &filename) {
	ifstream file(filename);
	if (!file.is_open()) {
		cerr << "[CRITICAL] Cannot open config file: " << filename << endl;
		return false;
	}

	try {
		file >> config_;
		cout << "Config loaded successfully from " << filename << endl;
		return true;
	}
	catch (const exception &e) {
		cerr << "[CRITICAL] Failed to parse config file: " << e.what() << endl;
		return false;
	}
}