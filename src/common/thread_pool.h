#pragma once
#include <atomic>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>
using namespace std;

class ThreadPool {
  public:
	ThreadPool(size_t threads);
	~ThreadPool();

	void enqueue(function<void()> task);

  private:
	vector<thread> workers_;
	queue<function<void()>> tasks_;
	mutex mtx_;
	condition_variable cv_;
	atomic<bool> stop_;
};