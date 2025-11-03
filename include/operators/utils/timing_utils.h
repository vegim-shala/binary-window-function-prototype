#pragma once
#include <atomic>
#include <chrono>
#include <thread>
#include <unordered_map>
#include <mutex>
#include <iostream>

class QueryTimer {
public:
    using Clock = std::chrono::steady_clock;

    static void add(uint64_t ns) {
        // Each thread accumulates locally to avoid contention
        thread_local uint64_t local_ns = 0;
        local_ns += ns;
        // Merge every ~1 ms worth to global counter
        if (local_ns > 1'000'000) {
            global_ns.fetch_add(local_ns, std::memory_order_relaxed);
            local_ns = 0;
        }
    }

    static void flush() {
        // Flush thread-local remainder
        thread_local uint64_t local_ns = 0;
        if (local_ns > 0) {
            global_ns.fetch_add(local_ns, std::memory_order_relaxed);
            local_ns = 0;
        }
    }

    static uint64_t total_ns() {
        return global_ns.load(std::memory_order_relaxed);
    }

    static void reset() {
        global_ns.store(0, std::memory_order_relaxed);
    }

private:
    inline static std::atomic<uint64_t> global_ns{0};
};

// Convenient scoped RAII helper for one timed block
class ScopedQueryTimer {
public:
    ScopedQueryTimer() : start(QueryTimer::Clock::now()) {}
    ~ScopedQueryTimer() {
        auto end = QueryTimer::Clock::now();
        QueryTimer::add(std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count());
    }
private:
    QueryTimer::Clock::time_point start;
};
