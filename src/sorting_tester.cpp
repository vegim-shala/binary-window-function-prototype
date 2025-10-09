#include <iostream>
#include <vector>
#include <algorithm> // Required for std::sort and std::generate
#include <chrono>    // Required for timing
#include <cstdlib>   // Required for std::rand

#include "data_io.h"
#include "ips2ra.hpp"
#include "ips4o/ips4o.hpp"

int compare_ints(const void *a, const void *b) {
    int arg1 = *static_cast<const int *>(a);
    int arg2 = *static_cast<const int *>(b);
    if (arg1 < arg2) return -1;
    if (arg1 > arg2) return 1;
    return 0;
}

// ------------------------------------------------------------------------------
// Sorting Algorithms
// ------------------------------------------------------------------------------

// ---------------------------    BASIC SORT    -------------------------------

/**
 * For 10 million records -> 600-700 ms
 * @param data
 */
void basic_sort(std::vector<size_t> &data) {
    std::sort(data.begin(), data.end());
};

// ---------------------------    QSORT    -------------------------------

/**
 * For 10 million records -> 800-900 ms
 * @param data
 */
void qsort(std::vector<size_t> &data) {
    std::qsort(data.data(), data.size(), sizeof(size_t), compare_ints);
};

// ---------------------------    STABLE SORT    -------------------------------

/**
 * For 10 million records -> ~ 500-600 ms
 * @param data
 */
void stable_sort(std::vector<size_t> &data) {
    std::stable_sort(data.begin(), data.end());
};

// ---------------------------    RADIX SORT    -------------------------------

// Helper function for Radix Sort
void counting_sort_by_digit(std::vector<size_t> &data, int exp) {
    std::vector<size_t> output(data.size());
    std::vector<int> count(10, 0); // Digits 0-9

    // Store count of occurrences in count[]
    for (int i: data) {
        int digit = (i / exp) % 10;
        count[digit]++;
    }

    // Change count[i] so that count[i] now contains the actual
    // position of this digit in output[]
    for (int i = 1; i < 10; i++) {
        count[i] += count[i - 1];
    }

    // Build the output array, processing the original array from the end to maintain stability
    for (int i = data.size() - 1; i >= 0; i--) {
        int digit = (data[i] / exp) % 10;
        output[count[digit] - 1] = data[i];
        count[digit]--;
    }

    // Copy the output array to data[], so that data[] now contains sorted numbers by current digit
    data = std::move(output);
}

/**
 * For 10 million records -> ~200-300 ms (Often significantly faster than std::sort for integers)
 * @param data
 */
void radix_sort(std::vector<size_t> &data) {
    if (data.empty()) return;

    // Find the maximum number to know the number of digits
    int max = *std::max_element(data.begin(), data.end());

    // Do counting sort for every digit. Note that instead of passing digit number,
    // we pass exp, which is 10^i where i is the current digit number.
    for (int exp = 1; max / exp > 0; exp *= 10) {
        counting_sort_by_digit(data, exp);
    }
}

// ---------------------------    COUNTING SORT    -------------------------------

/**
 * For 10 million records -> ~20-100 ms (Extremely fast if value range is small, e.g., 0-100)
 * WARNING: Will use massive memory and crash if max_val is large (e.g., RAND_MAX)!
 * @param data
 */
void counting_sort(std::vector<size_t> &data) {
    if (data.empty()) return;

    int min_val = *std::min_element(data.begin(), data.end());
    int max_val = *std::max_element(data.begin(), data.end());

    // Range of numbers
    int range = max_val - min_val + 1;

    // Check if the range is sane to prevent massive memory allocation
    // RAND_MAX is 32767 on some systems, which is a range of 65536 - manageable.
    // But if you have full 32-bit integers, the range is 4 billion - NOT manageable.
    if (range > 1000000) {
        // Arbitrary safety limit
        std::cerr << "Warning: Range too large for Counting Sort (" << range << "). Falling back to std::sort.\n";
        std::sort(data.begin(), data.end());
        return;
    }

    std::vector<int> count(range, 0);
    std::vector<size_t> output(data.size());

    // Store the count of each number, shifted by min_val
    for (int i: data) {
        count[i - min_val]++;
    }

    // Change count[i] so that it contains the actual position of this number in the output
    for (int i = 1; i < range; i++) {
        count[i] += count[i - 1];
    }

    // Build the output array (from the end to maintain stability)
    for (int i = data.size() - 1; i >= 0; i--) {
        int index = data[i] - min_val;
        output[count[index] - 1] = data[i];
        count[index]--;
    }

    // Copy the output back to the original array
    data = std::move(output);
}


// ---------------------------    IPS2RA SORT    -------------------------------

/**
 * For 10 million records -> 150-200 ms
 * @param data
 */
void ips2ra_sort(std::vector<size_t> &data) {
    ips2ra::sort(data.begin(), data.end(), [](size_t x) { return x; });
};

// ---------------------------    IPS2RA PARALLEL SORT    -------------------------------

/**
 * For 10 million records -> 30-70 ms for 4 threads
 * @param data
 */
void ips2ra_parallel_sort(std::vector<size_t> &data) {
    ips2ra::parallel::sort(data.begin(), data.end(), [](size_t x) { return x; });
};


// ---------------------------    IPS4O SORT    -------------------------------

/**
 * For 10 million records -> 200-250 ms
 * @param data
 */
void ips4o_sort(std::vector<size_t> &data) {
    ips4o::sort(data.begin(), data.end());
};

// ---------------------------    IPS4O PARALLEL SORT    -------------------------------

/**
 * For 10 million records -> 50-70 ms
 * @param data
 */
void ips4o_parallel_sort(std::vector<size_t> &data) {
    ips4o::parallel::sort(
        data.begin(),
        data.end(),
        std::less<>()
    );
};

void ips2ra_parallel_sort2(Dataset &data, const FileSchema &schema, const std::string &order_column, size_t threads) {
    size_t order_idx = schema.index_of(order_column);
    if (threads == 0) {
        ips2ra::parallel::sort(data.begin(), data.end(),
                               [&](const DataRow &row) {
                                   return static_cast<uint32_t>(row[order_idx]) ^ (1UL << 31);
                               });
    } else {
        ips2ra::parallel::sort(data.begin(), data.end(),
                               [&](const DataRow &row) {
                                   return static_cast<uint32_t>(row[order_idx]) ^ (1UL << 31);
                               }, threads);
    }
}

void debug_bias() {
    Dataset test_data = {{-5}, {-1}, {0}, {1}, {5}, {-10}};

    std::cout << "Value -> Bias transformation:\n";
    for (const auto &row: test_data) {
        int32_t value = row[0];
        uint32_t biased = static_cast<uint32_t>(value) + (1UL << 31);
        std::cout << value << " -> " << biased << "\n";
    }
}

#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <random>
#include <filesystem>
#include <unordered_set>

namespace fs = std::filesystem;

using namespace std;

#include <algorithm>
#include <random>
#include <vector>
#include <iostream>
#include <chrono>
#include "operators/utils/thread_pool.h"   // your thread pool

// Parallel sort using pool: split, sort, merge
template<typename T>
void parallel_sort(std::vector<T> &data, ThreadPool &pool) {
    size_t n = data.size();
    size_t num_threads = pool.size();
    if (num_threads < 2 || n < 100000) {
        // For very small input, fallback to sequential sort
        std::sort(data.begin(), data.end());
        return;
    }

    // 1. Split into num_threads chunks
    size_t chunk_size = (n + num_threads - 1) / num_threads;
    std::vector<std::future<void> > futures;
    futures.reserve(num_threads);

    for (size_t t = 0; t < num_threads; t++) {
        size_t start = t * chunk_size;
        size_t end = std::min(n, (t + 1) * chunk_size);
        if (start >= end) break;

        futures.emplace_back(pool.submit([&, start, end]() {
            std::sort(data.begin() + start, data.begin() + end);
        }));
    }

    // 2. Wait for all local sorts
    for (auto &f: futures) f.get();

    // 3. Merge chunks one by one
    //    For simplicity: iterative merge into a temp buffer
    std::vector<T> temp;
    temp.reserve(n);
    size_t step = chunk_size;
    size_t num_chunks = (n + chunk_size - 1) / chunk_size;

    auto merge_range = [&](size_t start, size_t mid, size_t end) {
        std::vector<T> tmp;
        tmp.reserve(end - start);
        std::merge(data.begin() + start, data.begin() + mid,
                   data.begin() + mid, data.begin() + end,
                   std::back_inserter(tmp));
        std::copy(tmp.begin(), tmp.end(), data.begin() + start);
    };

    // Iteratively merge chunk pairs
    size_t size_step = chunk_size;
    while (size_step < n) {
        for (size_t i = 0; i + size_step < n; i += 2 * size_step) {
            size_t mid = i + size_step;
            size_t end = std::min(i + 2 * size_step, n);
            merge_range(i, mid, end);
        }
        size_step *= 2;
    }
}

// int main() {
//     ThreadPool pool(std::thread::hardware_concurrency());
//
//     for (size_t N : {100'000, 200000, 500000, 1000000, 2000000, 4000000,5000000,6000000,7000000,7500000,8000000,9000000,
//          10000000,20000000,30000000,40000000,50000000,60000000,80000000,100000000, 200000000, 300000000, 400000000, 500000000, 1000000000}) {
//         std::vector<int> data(N);
//         std::mt19937 rng(42);
//         std::uniform_int_distribution<int> dist(0, 1000000);
//         for (auto &x : data) x = dist(rng);
//
//         auto start = std::chrono::high_resolution_clock::now();
//         parallel_sort(data, pool);
//         auto end = std::chrono::high_resolution_clock::now();
//
//         double ms = std::chrono::duration<double, std::milli>(end - start).count();
//         std::cout << "N=" << N << " time=" << ms << " ms\n";
//     }
// }


// Radix sort 32-bit keys with optional index permutation.
// Sorts ascending by SIGNED order if you pass normalize_signed=true.
// Stable. O(4N) passes. Requires temp buffers of same size as input.
void radix_sort_u32_with_perm(
    std::vector<uint32_t> &keys, // will be sorted (by normalized domain)
    std::vector<size_t> *perm = nullptr, // optional parallel permutation (row ids)
    bool normalize_signed = false
) {
    const size_t n = keys.size();
    if (n == 0) return;

    // Temp buffers
    std::vector<uint32_t> tmp_keys(n);
    std::vector<size_t> tmp_perm;
    if (perm) { tmp_perm.resize(n); }

    auto get_key = [&](uint32_t k) -> uint32_t {
        return normalize_signed ? (k ^ 0x80000000u) : k;
    };

    // Normalize once into tmp_keys to keep passes tight
    for (size_t i = 0; i < n; ++i) tmp_keys[i] = get_key(keys[i]);

    // If there is a permutation, ensure it’s initialized
    if (perm && perm->empty()) {
        perm->resize(n);
        std::iota(perm->begin(), perm->end(), 0);
    }

    // 4 LSD passes (8 bits each)
    constexpr int BYTES = 4;
    std::array<size_t, 256> count;

    // We’ll ping-pong (src -> dst)
    uint32_t *src_k = tmp_keys.data();
    uint32_t *dst_k = keys.data(); // at the end we'll write back normalized-sorted keys here

    size_t *src_p = perm ? perm->data() : nullptr;
    size_t *dst_p = perm ? tmp_perm.data() : nullptr;

    for (int pass = 0; pass < BYTES; ++pass) {
        count.fill(0);

        const int shift = pass * 8;

        // 1) histogram
        for (size_t i = 0; i < n; ++i) {
            ++count[(src_k[i] >> shift) & 0xFFu];
        }

        // 2) exclusive prefix sum
        size_t sum = 0;
        for (size_t b = 0; b < 256; ++b) {
            size_t c = count[b];
            count[b] = sum;
            sum += c;
        }

        // 3) stable scatter
        for (size_t i = 0; i < n; ++i) {
            const uint32_t kv = src_k[i];
            const uint8_t b = (kv >> shift) & 0xFFu;
            size_t pos = count[b]++;
            dst_k[pos] = kv;
            if (src_p) dst_p[pos] = src_p[i];
        }

        // 4) swap buffers for next pass
        std::swap(src_k, dst_k);
        if (src_p) std::swap(src_p, dst_p);
    }

    // After 4 passes, the sorted (normalized) keys are in src_k (because we swapped 4 times).
    // If src_k is tmp_keys, copy back to keys to leave caller with sorted-normalized keys.
    if (src_k != keys.data()) {
        std::memcpy(keys.data(), src_k, n * sizeof(uint32_t));
        if (src_p) std::memcpy(perm->data(), src_p, n * sizeof(size_t));
    }

    // keys[] now holds the normalized-sorted keys.
    // If you want the *original* (non-normalized) key order returned in `keys`, you can
    // de-normalize in place (not usually necessary if you only need the permutation).
    if (normalize_signed) {
        for (size_t i = 0; i < n; ++i) keys[i] ^= 0x80000000u;
    }
}

long long median_sort_time(size_t N, int repetitions = 5) {
    vector<long long> times;
    times.reserve(repetitions);

    for (int i = 0; i < repetitions; ++i) {
        vector<uint32_t> data(N);
        iota(data.begin(), data.end(), 0);
        shuffle(data.begin(), data.end(), mt19937{static_cast<unsigned>(123 + i)});

        auto start = chrono::high_resolution_clock::now();
        ips2ra::sort(data.begin(), data.end());
        auto end = chrono::high_resolution_clock::now();

        auto ms = chrono::duration_cast<chrono::microseconds>(end - start).count();
        times.push_back(ms);
    }

    sort(times.begin(), times.end());
    return times[times.size() / 2]; // median
}

template<typename T>
void parallel_sort(ThreadPool &pool, typename vector<T>::iterator begin, typename vector<T>::iterator end,
                   size_t depth = 0) {
    auto len = distance(begin, end);
    if (len < 100'000 || depth > 3) {
        // cutoff threshold and recursion limit
        sort(begin, end);
        return;
    }

    auto mid = begin + len / 2;
    auto left_future = pool.submit([&pool, begin, mid, depth] {
        parallel_sort<T>(pool, begin, mid, depth + 1);
    });
    parallel_sort<T>(pool, mid, end, depth + 1);
    left_future.wait();

    inplace_merge(begin, mid, end);
}

long long median_parallel_sort_time(size_t N, ThreadPool &pool, int repetitions = 5) {
    vector<long long> times;
    times.reserve(repetitions);

    for (int i = 0; i < repetitions; ++i) {
        vector<uint32_t> data(N);
        iota(data.begin(), data.end(), 0);
        shuffle(data.begin(), data.end(), mt19937{static_cast<unsigned>(123 + i)});

        auto start = chrono::high_resolution_clock::now();
        parallel_sort<uint32_t>(pool, data.begin(), data.end());
        auto end = chrono::high_resolution_clock::now();

        auto ms = chrono::duration_cast<chrono::microseconds>(end - start).count();
        times.push_back(ms);
    }

    sort(times.begin(), times.end());
    return times[times.size() / 2]; // median
}

// int main() {
//     for (size_t N: {
//              100'000, 200000, 500000, 1000000, 2000000, 4000000, 5000000, 6000000, 7000000, 7500000, 8000000, 9000000,
//              10000000, 20000000, 30000000, 40000000, 50000000, 60000000, 80000000, 100000000
//          }) {
//         long long median_us = median_sort_time(N);
//         cout << "Time Taken for SORTING: " << median_us << " ms\n\n\n";
//     }
// }
//
// int main() {
//     ThreadPool pool(thread::hardware_concurrency());
//     cout << "Using " << pool.size() << " threads.\n";
//
//     for (size_t N: {
//              100'000, 200000, 500000, 1000000, 2000000, 4000000, 5000000, 6000000, 7000000, 7500000, 8000000, 9000000,
//              10000000, 20000000, 30000000, 40000000, 50000000, 60000000, 80000000, 100000000, 200000000, 300000000,
//              400000000, 500000000, 1000000000
//          }) {
//         long long median_us = median_parallel_sort_time(N, pool);
//         cout << "Time Taken for SORTING: " << median_us << " ms\n\n\n";
//     }
// }

#include <iostream>
#include <fstream>
#include <random>
#include <string>
#include <vector>
#include <unordered_set>
#include <filesystem>

namespace fs = std::filesystem;

// int main() {
//     std::string folder_name = "Z5";
//     std::vector<int> test_index_list = {1, 2, 3, 4, 5, 6, 7, 8};
//     std::vector<int> N_PARTITIONS_LIST = {1, 2, 4, 8, 16, 32, 64, 128};
//
//     long long ROWS_PER_PART = 100'000;  // fixed rows per partition
//     long long PROBES_PER_PART = ROWS_PER_PART;
//
//     // Random engine
//     std::random_device rd;
//     std::mt19937_64 gen(rd());
//     std::uniform_int_distribution<int> value_dist(1, 100);
//
//     std::string base_path = "/Users/vegimshala/CLionProjects/binary-window-function-prototype/data/" + folder_name;
//     fs::create_directories(base_path);
//
//     for (size_t idx = 0; idx < test_index_list.size(); idx++) {
//         int test_index = test_index_list[idx];
//         int N_PARTITIONS = N_PARTITIONS_LIST[idx];
//
//         std::cout << "Running test_index=" << test_index
//                   << " with N_PARTITIONS=" << N_PARTITIONS
//                   << " and ROWS_PER_PART=" << ROWS_PER_PART << std::endl;
//
//         // --- Generate input.csv ---
//         std::string input_file = base_path + "/input" + std::to_string(test_index) + ".csv";
//         std::ofstream input_out(input_file);
//         input_out << "category,timestamp,value\n";
//
//         for (int cat = 0; cat < N_PARTITIONS; cat++) {
//             std::uniform_int_distribution<long long> ts_dist(1, ROWS_PER_PART);
//             for (long long r = 0; r < ROWS_PER_PART; r++) {
//                 long long t = ts_dist(gen);
//                 int value = value_dist(gen);
//                 input_out << cat << "," << t << "," << value << "\n";
//             }
//         }
//         input_out.close();
//         std::cout << "Wrote input" << test_index << ".csv" << std::endl;
//
//         // --- Generate probe.csv ---
//         std::string probe_file = base_path + "/probe" + std::to_string(test_index) + ".csv";
//         std::ofstream probe_out(probe_file);
//         probe_out << "category,begin_col,end_col\n";
//
//         for (int cat = 0; cat < N_PARTITIONS; cat++) {
//             std::unordered_set<std::string> seen;
//             std::uniform_int_distribution<long long> start_dist(1, ROWS_PER_PART - 10);
//             std::uniform_int_distribution<long long> len_dist(5, ROWS_PER_PART);
//
//             while ((long long)seen.size() < PROBES_PER_PART) {
//                 long long start = start_dist(gen);
//                 long long end = start + len_dist(gen);
//                 std::string key = std::to_string(start) + "-" + std::to_string(end);
//                 if (seen.insert(key).second) {
//                     probe_out << cat << "," << start << "," << end << "\n";
//                 }
//             }
//         }
//         probe_out.close();
//         std::cout << "Wrote probe" << test_index << ".csv" << std::endl;
//     }
//
//     return 0;
// }

// int main() {
//     std::string folder_name = "Z1";
//     std::vector<int> test_index_list = {
//         20,
//         // 23,
//         // 25
//     };
//
//     int N_PARTITIONS = 1;
//     std::vector<long long> ROWS_PER_PART_LIST = {
//         8'000'000,
//         // 500'000,
//         // 800'000
//     };
//     std::vector<long long> PROBES_PER_PART_LIST = ROWS_PER_PART_LIST; // identical in your Python code
//
//     // Random engine
//     std::random_device rd;
//     std::mt19937_64 gen(rd());
//     std::uniform_int_distribution<int> value_dist(1, 100);
//
//     std::string base_path = "/Users/vegimshala/CLionProjects/binary-window-function-prototype/data/" + folder_name;
//     fs::create_directories(base_path);
//
//     for (size_t idx = 0; idx < test_index_list.size(); idx++) {
//         int test_index = test_index_list[idx];
//         long long ROWS_PER_PART = ROWS_PER_PART_LIST[idx];
//         long long PROBES_PER_PART = PROBES_PER_PART_LIST[idx];
//
//         std::cout << "Running test_index=" << test_index
//                   << " with ROWS_PER_PART=" << ROWS_PER_PART << std::endl;
//
//         // --- Generate input.csv ---
//         std::string input_file = base_path + "/input" + std::to_string(test_index) + ".csv";
//         std::ofstream input_out(input_file);
//         input_out << "category,timestamp,value\n";
//
//         for (int cat = 0; cat < N_PARTITIONS; cat++) {
//             // Instead of shuffling the entire huge list, generate random timestamps on the fly
//             std::uniform_int_distribution<long long> ts_dist(1, ROWS_PER_PART);
//             for (long long r = 0; r < ROWS_PER_PART; r++) {
//                 long long t = ts_dist(gen);
//                 int value = value_dist(gen);
//                 input_out << cat << "," << t << "," << value << "\n";
//             }
//         }
//         input_out.close();
//         std::cout << "Wrote input" << test_index << ".csv" << std::endl;
//
//         // --- Generate probe.csv ---
//         std::string probe_file = base_path + "/probe" + std::to_string(test_index) + ".csv";
//         std::ofstream probe_out(probe_file);
//         probe_out << "category,begin_col,end_col\n";
//
//         for (int cat = 0; cat < N_PARTITIONS; cat++) {
//             std::unordered_set<std::string> seen;
//             std::uniform_int_distribution<long long> start_dist(1, ROWS_PER_PART - 10);
//             std::uniform_int_distribution<long long> len_dist(5, ROWS_PER_PART);
//
//             while ((long long)seen.size() < PROBES_PER_PART) {
//                 long long start = start_dist(gen);
//                 long long end = start + len_dist(gen);
//                 std::string key = std::to_string(start) + "-" + std::to_string(end);
//                 if (seen.insert(key).second) {
//                     probe_out << cat << "," << start << "," << end << "\n";
//                 }
//             }
//         }
//         probe_out.close();
//         std::cout << "Wrote probe" << test_index << ".csv" << std::endl;
//     }
//
//     return 0;
// }


#include <algorithm>
#include <chrono>
#include <future>
#include <iomanip>
#include <iostream>
#include <numeric>
#include <random>
#include <thread>
#include <vector>
#include "operators/utils/thread_pool.h" // your existing pool class

// --- Sequential std::sort benchmark ---
double benchmark_std_sort(std::vector<int>& data) {
    auto start = std::chrono::high_resolution_clock::now();
    std::sort(data.begin(), data.end());
    auto end = std::chrono::high_resolution_clock::now();
    return std::chrono::duration<double, std::milli>(end - start).count();
}

// --- Parallel sort using ThreadPool ---
double benchmark_parallel_sort(std::vector<int>& data, size_t num_threads) {
    ThreadPool pool(num_threads);
    size_t n = data.size();
    size_t chunk_size = (n + num_threads - 1) / num_threads;

    std::vector<std::pair<size_t, size_t>> ranges;
    for (size_t t = 0; t < num_threads; ++t) {
        size_t begin = t * chunk_size;
        size_t end = std::min(begin + chunk_size, n);
        if (begin >= n) break;
        ranges.emplace_back(begin, end);
    }

    auto start = std::chrono::high_resolution_clock::now();

    // --- Phase 1: Sort chunks in parallel ---
    std::vector<std::future<void>> futures;
    for (auto [begin, end] : ranges) {
        futures.emplace_back(pool.submit([&, begin, end]() {
            std::sort(data.begin() + begin, data.begin() + end);
        }));
    }
    for (auto& f : futures) f.get();

    // --- Phase 2: Parallel pairwise merging ---
    while (ranges.size() > 1) {
        std::vector<std::pair<size_t, size_t>> new_ranges;
        std::vector<std::future<void>> merge_futs;

        for (size_t i = 0; i + 1 < ranges.size(); i += 2) {
            auto [b1, e1] = ranges[i];
            auto [b2, e2] = ranges[i + 1];
            new_ranges.emplace_back(b1, e2);

            merge_futs.emplace_back(pool.submit([&, b1, e1, b2, e2]() {
                std::vector<int> tmp;
                tmp.reserve(e2 - b1);
                std::merge(data.begin() + b1, data.begin() + e1,
                           data.begin() + b2, data.begin() + e2,
                           std::back_inserter(tmp));
                std::copy(tmp.begin(), tmp.end(), data.begin() + b1);
            }));
        }

        // Handle an odd last chunk
        if (ranges.size() % 2 == 1)
            new_ranges.push_back(ranges.back());

        for (auto& f : merge_futs) f.get();
        ranges.swap(new_ranges);
    }

    auto end = std::chrono::high_resolution_clock::now();
    return std::chrono::duration<double, std::milli>(end - start).count();
}

// --- Generate random data ---
// std::vector<int> generate_data(size_t n) {
//     std::vector<int> data(n);
//     std::mt19937 rng(42);
//     std::uniform_int_distribution<int> dist(0, 1'000'000'000);
//     for (auto& v : data) v = dist(rng);
//     return data;
// }

// --- Main benchmark runner ---
// int main() {
//     const size_t N = 1'000'000; // 10 million elements
//     const int repeats = 3;
//
//     std::cout << "\n================ std::sort vs Parallel Sort Benchmark ================\n";
//     std::cout << "Threads |   std::sort (ms)  |  parallel_sort (ms)  |  Speedup\n";
//     std::cout << "-------------------------------------------------------------\n";
//
//     std::vector<int> data_original = generate_data(N);
//     double baseline_ms = -1.0;
//
//     // Sequential baseline
//     {
//         std::vector<int> data = data_original;
//         double total_ms = 0;
//         for (int r = 0; r < repeats; ++r) {
//             std::vector<int> tmp = data;
//             total_ms += benchmark_std_sort(tmp);
//         }
//         baseline_ms = total_ms / repeats;
//         std::cout << std::setw(7) << 1 << " | "
//                   << std::setw(18) << std::fixed << std::setprecision(2) << baseline_ms
//                   << " | " << std::setw(20) << "-" << " | " << std::setw(8) << "1.00x\n";
//     }
//
//     // Parallel scaling
//     for (size_t threads = 2; threads <= 8; ++threads) {
//         double total_ms = 0;
//         for (int r = 0; r < repeats; ++r) {
//             std::vector<int> data = data_original;
//             total_ms += benchmark_parallel_sort(data, threads);
//         }
//         double avg_ms = total_ms / repeats;
//         double speedup = baseline_ms / avg_ms;
//         std::cout << std::setw(7) << threads << " | "
//                   << std::setw(18) << baseline_ms << " | "
//                   << std::setw(20) << std::fixed << std::setprecision(2) << avg_ms << " | "
//                   << std::setw(8) << std::setprecision(2) << speedup << "x\n";
//     }
//
//     std::cout << "======================================================================\n";
//     return 0;
// }

double benchmark_ips2ra_sort(std::vector<uint32_t>& data) {
    auto start = std::chrono::high_resolution_clock::now();
    ips2ra::sort(data.begin(), data.end());
    auto end = std::chrono::high_resolution_clock::now();
    return std::chrono::duration<double, std::milli>(end - start).count();
}

// --- Simulate multi-threaded ips2ra sort (partition-based parallelization) ---
double benchmark_parallel_ips2ra_sort(std::vector<uint32_t>& data, size_t num_threads) {
    auto start = std::chrono::high_resolution_clock::now();
    ips2ra::parallel::sort(data.begin(), data.end(),[](size_t x) { return x; }, num_threads);
    auto end = std::chrono::high_resolution_clock::now();
    return std::chrono::duration<double, std::milli>(end - start).count();
}

// --- Generate random data ---
std::vector<uint32_t> generate_data(size_t n) {
    std::vector<uint32_t> data(n);
    std::mt19937 rng(42);
    std::uniform_int_distribution<uint32_t> dist(0, 1'000'000'000);
    for (auto& v : data) v = dist(rng);
    return data;
}

// --- Main benchmark runner ---
int main() {
    const size_t N = 10'000'000; // 10 million elements
    const int repeats = 3;

    std::cout << "\n================ ips2ra::sort Speedup Benchmark ================\n";
    std::cout << "Threads |   ips2ra (1-thread ms)  |  parallel_ips2ra (ms)  |  Speedup\n";
    std::cout << "-----------------------------------------------------------------\n";

    std::vector<uint32_t> data_original = generate_data(N);
    double baseline_ms = -1.0;

    // Sequential baseline
    {
        std::vector<uint32_t> data = data_original;
        double total_ms = 0;
        for (int r = 0; r < repeats; ++r) {
            std::vector<uint32_t> tmp = data;
            total_ms += benchmark_ips2ra_sort(tmp);
        }
        baseline_ms = total_ms / repeats;
        std::cout << std::setw(7) << 1 << " | "
                  << std::setw(24) << std::fixed << std::setprecision(2) << baseline_ms
                  << " | " << std::setw(22) << "-" << " | "
                  << std::setw(8) << "1.00x\n";
    }

    // Parallel scaling
    for (size_t threads = 2; threads <= 8; ++threads) {
        double total_ms = 0;
        for (int r = 0; r < repeats; ++r) {
            std::vector<uint32_t> data = data_original;
            total_ms += benchmark_parallel_ips2ra_sort(data, threads);
        }
        double avg_ms = total_ms / repeats;
        double speedup = baseline_ms / avg_ms;
        std::cout << std::setw(7) << threads << " | "
                  << std::setw(24) << baseline_ms << " | "
                  << std::setw(22) << std::fixed << std::setprecision(2) << avg_ms << " | "
                  << std::setw(8) << std::setprecision(2) << speedup << "x\n";
    }

    std::cout << "=================================================================\n";
    return 0;
}
