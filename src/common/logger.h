#pragma once
#include <condition_variable>
#include <fstream>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <vector>

using namespace std;

enum LogLevel {
	L_DEBUG = 0,
	L_INFO = 1,
	L_WARN = 2,
	L_ERROR = 3
};

class Logger {
  public:
	static Logger &getInstance();

	// 初始化 指定写入的文件名
	void init(const string &filename, LogLevel level = L_INFO);

	// 暴露判断接口，供宏前置判断使用
	bool shouldLog(LogLevel level) const {
		return level >= current_level_;
	}

	// 客户端接口  业务线程调用这个塞入日志
	void log(LogLevel level, const char *file, int line, const char *format, ...);

	~Logger();

  private:
	Logger() = default;

	void writeThread(); // 后台写盘线程

	ofstream file_stream_;

	// 双缓冲
	vector<string> buffers_[2];	 // 两个缓冲区
	int current_buffer_idx_ = 0; // 写入的缓冲区索引

	// 后台就绪队列
	queue<vector<string>> ready_queue_;

	mutex mtx_;
	condition_variable cv_;

	thread write_thread_;

	bool stop_ = false;

	LogLevel current_level_ = L_INFO;

	// 阈值，比如 1024 条或者积攒到一定大小就切换
	const size_t BUFFER_FLUSH_THRESHOLD = 1024;
};

// 尝试宏定义
#define LOG_BASE(level, format, ...)                                                                                   \
	do {                                                                                                               \
		if (Logger::getInstance().shouldLog(level)) {                                                                  \
			Logger::getInstance().log(level, __FILE__, __LINE__, format, ##__VA_ARGS__);                               \
		}                                                                                                              \
	} while (0)

// 对外暴露的简单宏
#define LOG_DEBUG(format, ...) LOG_BASE(L_DEBUG, format, ##__VA_ARGS__)
#define LOG_INFO(format, ...) LOG_BASE(L_INFO, format, ##__VA_ARGS__)
#define LOG_WARN(format, ...) LOG_BASE(L_WARN, format, ##__VA_ARGS__)
#define LOG_ERROR(format, ...) LOG_BASE(L_ERROR, format, ##__VA_ARGS__)