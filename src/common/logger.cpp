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
		std::cerr << "[FATAL ERROR] Failed to open log file: " << filename << std::endl;
		exit(1); // 文件打不开不跑系统直接退出
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

	// local非线程安全  多线程同时调用换成localtime_r
	struct tm *tm_info = localtime(&now_time);
	char time_str[32];
	// strtime函数  把tm类型格式化为人可读字符串
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
	va_start(args, format);					   // 抓取变参到args
	vsnprintf(buf, sizeof(buf), format, args); // 将变参与format文本结合
	//  v-->接收变参
	//  s-->safe 截断字符串防溢出

	va_end(args); // 释放args

	// 截取文件名  防止日志文件名太长把排版撑爆
	string file_str(file);
	// 路径名从后往前找 找到第一个 linux的/或者windows的\ 截取后面的文件名
	size_t pos = file_str.find_last_of("/\\");

	if (pos != std::string::npos)
		// 找到了分隔符 截取最后一个分隔符后面的部分
		file_str = file_str.substr(pos + 1);

	// 组装日志
	std::ostringstream oss;
	oss << time_str << " " << level_str << file_str << ":" << line << " - " << buf;

	{
		std::lock_guard<std::mutex> lock(mtx_);
		buffer_[current_buffer_idx_].emplace_back(oss.str());

		// 积压达到一定数量 唤醒后台线程
		if (buffer_[current_buffer_idx_].size() >= 1024) {
			cv_.notify_one();
		}
	}
}

void Logger::writeThread() {
	vector<string> write_buffer;

	while (true) {
		{
			std::unique_lock<std::mutex> lock(mtx_);

			cv_.wait_for(lock, chrono::seconds(1), [this]() {
				return stop_ || !buffer_[current_buffer_idx_].empty();
			});

			if (stop_ && buffer_[current_buffer_idx_].empty()) {
				break;
			}

			if (buffer_[current_buffer_idx_].empty()) {
				continue; // 超时醒来
			}

			// 将当前写满的buffer和后台空buffer交换
			write_buffer.swap(buffer_[current_buffer_idx_]);

			// 切换缓冲区索引
			current_buffer_idx_ = 1 - current_buffer_idx_;
		}

		// 在锁外面写磁盘
		if (file_stream_.is_open()) {
			for (const auto &msg : write_buffer) {
				file_stream_ << msg << '\n';
			}
			file_stream_.flush(); // 刷盘
		}

		write_buffer.clear();
	}
}