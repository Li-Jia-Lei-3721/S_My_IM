#include "thread_pool.h"
#include <utility>

ThreadPool::ThreadPool(size_t threads) : stop_(false) {
	for (size_t i = 0; i < threads; i++) {
		workers_.emplace_back([this] {
			while (true) {
				function<void()> task;
				{
					unique_lock<mutex> lock(this->mtx_);
					this->cv_.wait(lock, [this]() { return this->stop_ || !this->tasks_.empty(); });
					if (this->stop_ && this->tasks_.empty())
						return;
					task = std::move(this->tasks_.front());
					this->tasks_.pop();
				}
				task();
			}
		});
	}
}

ThreadPool::~ThreadPool() {
	{
		unique_lock<mutex> lock(mtx_);
		stop_ = true;
	}
	cv_.notify_all();
	for (thread &worker : workers_) {
		if (worker.joinable())
			worker.join();
	}
}

void ThreadPool::enqueue(function<void()> task) {
	{
		unique_lock<mutex> lock(mtx_);
		tasks_.emplace(std::move(task));
	}
	cv_.notify_one();
}