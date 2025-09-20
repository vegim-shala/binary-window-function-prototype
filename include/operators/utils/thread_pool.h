// thread_pool.h
#pragma once
#include <vector>
#include <thread>
#include <future>
#include <functional>
#include <queue>
#include <mutex>
#include <condition_variable>

class ThreadPool {
public:
    explicit ThreadPool(size_t n = std::thread::hardware_concurrency()) {
        if (n == 0) n = 1;
        stop_flag = false;
        for (size_t i = 0; i < n; ++i) {
            workers.emplace_back([this]{
                for (;;) {
                    std::function<void()> task;
                    {
                        std::unique_lock<std::mutex> lock(this->mtx);
                        this->cv.wait(lock, [this] { return this->stop_flag || !this->tasks.empty(); });
                        if (this->stop_flag && this->tasks.empty()) return;
                        task = std::move(this->tasks.front());
                        this->tasks.pop();
                    }
                    task();
                }
            });
        }
    }

    ~ThreadPool() {
        {
            std::lock_guard<std::mutex> lock(mtx);
            stop_flag = true;
        }
        cv.notify_all();
        for (auto &t : workers) if (t.joinable()) t.join();
    }

    // submit a task, returns a future
    template<typename F, typename... Args>
    auto submit(F&& f, Args&&... args)
        -> std::future<std::invoke_result_t<F, Args...>> {
        using Ret = std::invoke_result_t<F, Args...>;
        auto task_ptr = std::make_shared<std::packaged_task<Ret()>>(
            std::bind(std::forward<F>(f), std::forward<Args>(args)...)
        );
        std::future<Ret> fut = task_ptr->get_future();
        {
            std::lock_guard<std::mutex> lock(mtx);
            if (stop_flag) throw std::runtime_error("submit on stopped ThreadPool");
            tasks.emplace([task_ptr]{ (*task_ptr)(); });
        }
        cv.notify_one();
        return fut;
    }

    size_t size() const { return workers.size(); }

private:
    std::vector<std::thread> workers;
    std::queue<std::function<void()>> tasks;
    mutable std::mutex mtx;
    std::condition_variable cv;
    bool stop_flag;
};
