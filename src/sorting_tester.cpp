#include <iostream>
#include <vector>
#include <algorithm> // Required for std::sort and std::generate
#include <chrono>    // Required for timing
#include <cstdlib>   // Required for std::rand

#include "data_io.h"
#include "ips2ra.hpp"
#include "ips4o/ips4o.hpp"

int compare_ints(const void* a, const void* b) {
    int arg1 = *static_cast<const int*>(a);
    int arg2 = *static_cast<const int*>(b);
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
void basic_sort(std::vector<size_t>& data) {
    std::sort(data.begin(), data.end());
};

// ---------------------------    QSORT    -------------------------------

/**
 * For 10 million records -> 800-900 ms
 * @param data
 */
void qsort(std::vector<size_t>& data) {
    std::qsort(data.data(), data.size(), sizeof(size_t), compare_ints);
};

// ---------------------------    STABLE SORT    -------------------------------

/**
 * For 10 million records -> ~ 500-600 ms
 * @param data
 */
void stable_sort(std::vector<size_t>& data) {
    std::stable_sort(data.begin(), data.end());
};

// ---------------------------    RADIX SORT    -------------------------------

// Helper function for Radix Sort
void counting_sort_by_digit(std::vector<size_t>& data, int exp) {
    std::vector<size_t> output(data.size());
    std::vector<int> count(10, 0); // Digits 0-9

    // Store count of occurrences in count[]
    for (int i : data) {
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
void radix_sort(std::vector<size_t>& data) {
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
void counting_sort(std::vector<size_t>& data) {
    if (data.empty()) return;

    int min_val = *std::min_element(data.begin(), data.end());
    int max_val = *std::max_element(data.begin(), data.end());

    // Range of numbers
    int range = max_val - min_val + 1;

    // Check if the range is sane to prevent massive memory allocation
    // RAND_MAX is 32767 on some systems, which is a range of 65536 - manageable.
    // But if you have full 32-bit integers, the range is 4 billion - NOT manageable.
    if (range > 1000000) { // Arbitrary safety limit
        std::cerr << "Warning: Range too large for Counting Sort (" << range << "). Falling back to std::sort.\n";
        std::sort(data.begin(), data.end());
        return;
    }

    std::vector<int> count(range, 0);
    std::vector<size_t> output(data.size());

    // Store the count of each number, shifted by min_val
    for (int i : data) {
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
void ips2ra_sort(std::vector<size_t>& data) {
    ips2ra::sort(data.begin(), data.end(), [](size_t x) { return x; });
};

// ---------------------------    IPS2RA PARALLEL SORT    -------------------------------

/**
 * For 10 million records -> 30-70 ms for 4 threads
 * @param data
 */
void ips2ra_parallel_sort(std::vector<size_t>& data) {
    ips2ra::parallel::sort(data.begin(), data.end(), [](size_t x) { return x; });
};


// ---------------------------    IPS4O SORT    -------------------------------

/**
 * For 10 million records -> 200-250 ms
 * @param data
 */
void ips4o_sort(std::vector<size_t>& data) {
    ips4o::sort(data.begin(), data.end());
};

// ---------------------------    IPS4O PARALLEL SORT    -------------------------------

/**
 * For 10 million records -> 50-70 ms
 * @param data
 */
void ips4o_parallel_sort(std::vector<size_t>& data) {
    ips4o::parallel::sort(
        data.begin(),
        data.end(),
        std::less<>()
    );
};
void ips2ra_parallel_sort2(Dataset &data, const FileSchema &schema, const std::string &order_column, size_t threads) {
    size_t order_idx  = schema.index_of(order_column);
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
    for (const auto& row : test_data) {
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
#include <execution>
#include <random>
#include <vector>
#include <chrono>
#include <iostream>
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
    std::vector<std::future<void>> futures;
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
    for (auto &f : futures) f.get();

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
                   data.begin() + mid,   data.begin() + end,
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

int main() {
    ThreadPool pool(std::thread::hardware_concurrency());
    for (size_t N : {100'000, 200000, 500000, 1000000, 2000000, 4000000,5000000,6000000,7000000,7500000,8000000,9000000,10000000,20000000,30000000,40000000,50000000,60000000,80000000,100000000}) {
        std::vector<int> data(N);
        std::mt19937 rng(42);
        std::uniform_int_distribution<int> dist(0, 1000000);
        for (auto &x : data) x = dist(rng);

        auto start = std::chrono::high_resolution_clock::now();
        parallel_sort(data, pool);
        auto end = std::chrono::high_resolution_clock::now();

        auto ms = chrono::duration_cast<chrono::microseconds>(end - start).count();
        cout << "N=" << N << " time=" << ms << " T/N=" << double(ms)/N << "\n";
    }
}

// int main() {
//     std::string folder_name = "Z1";
//     std::vector<int> test_index_list = {
//         // 15,16,17,
//         // 18,19,20,21,22,
//         // 23,
//         // ,24,
//         25
//     };
//
//     int N_PARTITIONS = 1;
//     std::vector<long long> ROWS_PER_PART_LIST = {
//         // 6'000'000, 80'000'000, 100'000'000,
//         // 200'000'000, 7'000'000, 8'000'000, 9'000'000, 30'000'000,
//         // 50'000'000,
//         // , 60'000'000,
//         80'000'000
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

// int main() {
//     // Create a vector with 10 million elements (1e7 is 10 million, 1e6 is 1 million) and fill it with random integers
//     const int size = 1e7;
//     std::vector<size_t> data(size);
//     std::generate(data.begin(), data.end(), std::rand);
//
//     // Divide every element by size to reduce the range of values for counting sort
//     // for (auto& num : data) {
//     //     num = num % (size/1000); // Reduce range to 0-999 for counting sort
//     // }
//
//     // Time the sorting
//     auto start = std::chrono::high_resolution_clock::now();
//
//     // Sort the vector
//     // basic_sort(data);
//     // qsort(data);
//     // stable_sort(data);
//     // radix_sort(data);
//     // counting_sort(data);
//     // ips2ra_sort(data);
//     ips2ra_parallel_sort(data);
//     // ips4o_sort(data);
//     // ips4o_parallel_sort(data);
//
//     // Stop the timer
//     auto end = std::chrono::high_resolution_clock::now();
//
//     // Calculate the duration in milliseconds
//     auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
//
//     // Print the result
//     std::cout << "Time taken to sort " << size << " integers: " << duration.count() << " milliseconds" << std::endl;
//
//     // Test your current (broken) partitioning:
//     Dataset test_data = {
//         { -5, 100 },
//         { -1, 200 },
//         { 0, 300 },
//         { 1, 400 },
//         { 5, 500 },
//         { -10, 600 }
//     };
//
//     std::cout << "Original dataset:\n";
//     for (const auto& row : test_data) {
//         std::cout << "[" << row[0] << ", " << row[1] << "]\n";
//     }
//
//     // Create a simple schema for testing
//     FileSchema schema;
//     schema.add_column("value", "int32");
//     schema.add_column("other", "int32");
//     schema.build_index();
//
//     // Sort by first column
//     ips2ra_parallel_sort2(test_data, schema, "value", 0);
//
//     std::cout << "\nSorted dataset:\n";
//     for (const auto& row : test_data) {
//         std::cout << "[" << row[0] << ", " << row[1] << "]\n";
//     }
//
//     return 0;
// }