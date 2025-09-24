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

int main() {
    // Create a vector with 10 million elements (1e7 is 10 million, 1e6 is 1 million) and fill it with random integers
    const int size = 1e7;
    std::vector<size_t> data(size);
    std::generate(data.begin(), data.end(), std::rand);

    // Divide every element by size to reduce the range of values for counting sort
    // for (auto& num : data) {
    //     num = num % (size/1000); // Reduce range to 0-999 for counting sort
    // }

    // Time the sorting
    auto start = std::chrono::high_resolution_clock::now();

    // Sort the vector
    // basic_sort(data);
    // qsort(data);
    // stable_sort(data);
    // radix_sort(data);
    // counting_sort(data);
    // ips2ra_sort(data);
    ips2ra_parallel_sort(data);
    // ips4o_sort(data);
    // ips4o_parallel_sort(data);

    // Stop the timer
    auto end = std::chrono::high_resolution_clock::now();

    // Calculate the duration in milliseconds
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    // Print the result
    std::cout << "Time taken to sort " << size << " integers: " << duration.count() << " milliseconds" << std::endl;

    // Test your current (broken) partitioning:
    Dataset test_data = {
        { -5, 100 },
        { -1, 200 },
        { 0, 300 },
        { 1, 400 },
        { 5, 500 },
        { -10, 600 }
    };

    std::cout << "Original dataset:\n";
    for (const auto& row : test_data) {
        std::cout << "[" << row[0] << ", " << row[1] << "]\n";
    }

    // Create a simple schema for testing
    FileSchema schema;
    schema.add_column("value", "int32");
    schema.add_column("other", "int32");
    schema.build_index();

    // Sort by first column
    ips2ra_parallel_sort2(test_data, schema, "value", 0);

    std::cout << "\nSorted dataset:\n";
    for (const auto& row : test_data) {
        std::cout << "[" << row[0] << ", " << row[1] << "]\n";
    }

    return 0;
}