#include "logger.h"
#include <chrono>
#include <cstdarg>
// #include <iomanip>
#include <iostream>
#include <sstream>

Logger &Logger::getInstance() {
	static Logger instance;
	return instance;
}

void Logger::init(const string &filename, LogLevel level) {
	std::lock_guard<std::mutex> lock(mtx_);
	current_level_ = level;
	file_stream_.open(filename, std::ios::app);
	if (!file_stream_.is_open()) {
		std::cerr << "Failed to open log file: " << filename << std::endl;
		return;
	}
	// 启动后台写盘线程
	write_thread_ = std::thread(&Logger::writeThread, this);
}

Logger::~Logger() {
	{
		lock_guard<mutex> lock(mtx_);
		stop_ = true;
	}

	cv_.notify_all();
	if (write_thread_.joinable()) {
		write_thread_.join();
	}
	if (file_stream_.is_open()) {
		file_stream_.close();
	}
}

void Logger::log(LogLevel level, const char *file, int line, const char *format, ...) {
	auto now = chrono::system_clock::now();
	time_t now_time = chrono::system_clock::to_time_t(now);

	struct tm *tm_info = localtime(&now_time);
	char time_str[32];
	strftime(time_str, sizeof(time_str), "%Y-%m-%d %H:%M:%S", tm_info);

	string level_str;
	switch (level) {
	case L_DEBUG:
		level_str = "[DEBUG] ";
		break;
	case L_INFO:
		level_str = "[INFO]  ";
		break;
	case L_WARN:
		level_str = "[WARN]  ";
		break;
	case L_ERROR:
		level_str = "[ERROR] ";
		break;
	}

	char buf[1024];
	va_list args;
	va_start(args, format);
	vsnprintf(buf, sizeof(buf), format, args);
	va_end(args);

	// 截取文件名
	string file_str(file);
	size_t pos = file_str.find_last_of("/\\");

	if (pos != std::string::npos)
		file_str = file_str.substr(pos + 1);

	std::ostringstream oss;
	oss << time_str << " " << level_str << file_str << ":" << line << " - " << buf;

	{
		std::lock_guard<std::mutex> lock(mtx_);
		log_queue_.push(oss.str());
	}

	cv_.notify_one();
}

void Logger::writeThread() {
	while (true) {
		std::string log_msg;
		{
			std::unique_lock<std::mutex> lock(mtx_);
			cv_.wait(lock, [this]() {
				return stop_ || !log_queue_.empty();
			});

			if (stop_ && log_queue_.empty())
				break;

			log_msg = log_queue_.front();
			log_queue_.pop();
		}
		// 在锁外面写磁盘！这就是异步的威力
		if (file_stream_.is_open()) {
			file_stream_ << log_msg << std::endl;
		}
	}
}