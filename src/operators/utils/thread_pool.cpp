#include "operators/utils/thread_pool.h"

ThreadPool::ThreadPool(size_t num_threads) {
    for (size_t i = 0; i < num_threads; ++i) {
        workers.emplace_back([this] {
            for (;;) {
                std::function<void()> task;

                {
                    std::unique_lock<std::mutex> lock(queue_mutex);
                    condition.wait(lock, [this] {
                        return stop || !tasks.empty();
                    });

                    if (stop && tasks.empty())
                        return;

                    task = std::move(tasks.front());
                    tasks.pop();
                }
                task();
            }
        });
    }
}

ThreadPool::~ThreadPool() {
    {
        std::unique_lock<std::mutex> lock(queue_mutex);
        stop = true;
    }
    condition.notify_all();
    for (auto& worker : workers)
        worker.join();
}

template<class F, class... Args>
auto ThreadPool::enqueue(F&& f, Args&&... args)
    -> std::future<typename std::invoke_result<F, Args...>::type> {
    using return_type = typename std::invoke_result<F, Args...>::type;

    auto task = std::make_shared<std::packaged_task<return_type()>>(
        std::bind(std::forward<F>(f), std::forward<Args>(args)...));

    std::future<return_type> res = task->get_future();
    {
        std::unique_lock<std::mutex> lock(queue_mutex);
        if (stop)
            throw std::runtime_error("enqueue on stopped ThreadPool");
        tasks.emplace([task]() { (*task)(); });
    }
    condition.notify_one();
    return res;
}

void ThreadPool::wait() {
    // simple busy-wait loop, can be improved
    for (;;) {
        std::unique_lock<std::mutex> lock(queue_mutex);
        if (tasks.empty())
            return;
    }
}
