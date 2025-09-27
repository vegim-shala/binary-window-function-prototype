//
// Created by Vegim Shala on 11.7.25.
//
#include "operators/utils/sort_utils.h"


#include <iostream>

#include "ips2ra.hpp"
#include "ips4o/ips4o.hpp"


// Helper function for Radix Sort
void counting_sort_by_digit(std::vector<DataRow> &data, std::vector<int> &keys, int exp) {
    std::vector<DataRow> output_data(data.size());
    std::vector<int> output_keys(keys.size());
    std::vector<int> count(10, 0);

    // Count frequencies of digits
    for (int key: keys) {
        int digit = (key / exp) % 10;
        count[digit]++;
    }

    // Compute cumulative counts
    for (int i = 1; i < 10; i++) {
        count[i] += count[i - 1];
    }

    // Build output arrays from the end (for stability)
    for (int i = data.size() - 1; i >= 0; i--) {
        int digit = (keys[i] / exp) % 10;
        int new_pos = count[digit] - 1;

        output_data[new_pos] = std::move(data[i]);
        output_keys[new_pos] = keys[i];

        count[digit]--;
    }

    // Update the original arrays
    data = std::move(output_data);
    keys = std::move(output_keys);
}

void radix_sort_rows(std::vector<DataRow> &data, const FileSchema &schema, size_t &order_idx) {
    if (data.empty()) return;

    // 1. Extract keys and find the maximum value
    std::vector<int> keys;
    keys.reserve(data.size());

    for (const auto &row: data) {
        keys.push_back(row[order_idx]);
    }
    int max_key = *std::max_element(keys.begin(), keys.end());

    // 2. Perform LSD Radix Sort using the keys to determine the order of the data rows
    for (int exp = 1; max_key / exp > 0; exp *= 10) {
        counting_sort_by_digit(data, keys, exp);
    }
}

void counting_sort_rows(std::vector<DataRow> &data, const FileSchema &schema, size_t &order_idx) {
    // 1. First, extract all the keys into a vector.
    std::vector<int> keys;
    keys.reserve(data.size());

    for (const auto &row: data) {
        keys.push_back(row[order_idx]);
    }

    // 2. Find the min and max key to determine the range
    int min_key = *std::min_element(keys.begin(), keys.end());
    int max_key = *std::max_element(keys.begin(), keys.end());
    int range = max_key - min_key + 1;

    // 4. Perform Counting Sort on the INDICES
    // We will create a count array and then determine the correct position for each object.

    std::vector<int> count(range + 1, 0); // Count array, size k+1
    std::vector<DataRow> output(data.size()); // Output array

    // Step 1: Count frequencies
    for (int key: keys) {
        count[key - min_key]++;
    }

    // Step 2: Compute cumulative counts (starting positions)
    for (int i = 1; i < range; i++) {
        count[i] += count[i - 1];
    }

    // Step 3: Place items in sorted order (process original array backwards for stability)
    for (int i = data.size() - 1; i >= 0; i--) {
        int key_index = keys[i] - min_key;
        int output_index = count[key_index] - 1; // Get position for this key
        output[output_index] = std::move(data[i]); // Move the object
        count[key_index] = output_index; // Decrement the count
    }

    // 5. Replace the original data with the sorted output
    data = std::move(output);
}

// ---------------------------    IPS2RA SORT    -------------------------------

/**
 * For 10 million records -> 150-200 ms
 * @param data
 * @param order_column
 */
void ips2ra_sort(Dataset &data, const FileSchema &schema, const size_t &order_idx) {
    ips2ra::sort(data.begin(), data.end(),
                 [&](const DataRow &row) {
                     return static_cast<uint32_t>(row[order_idx]) ^ (1UL << 31);
                 });
};

// ---------------------------    IPS2RA PARALLEL SORT    -------------------------------

/**
 * For 10 million records -> 30-70 ms for 4 threads
 * @param data
 * @param order_column
 */
void ips2ra_parallel_sort(Dataset &data, const FileSchema &schema, const size_t &order_idx, size_t threads) {
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

// ---------------------------    IPS4O SORT    -------------------------------

/**
 * For 10 million records -> 200-250 ms
 * @param data
 * @param order_column
 */
void ips4o_sort(Dataset &data, const FileSchema &schema, const size_t &order_idx) {
    ips4o::sort(data.begin(), data.end(),
                [&](const DataRow &a, const DataRow &b) {
                    return a[order_idx] < b[order_idx];
                });
};

// ---------------------------    IPS4O PARALLEL SORT    -------------------------------

/**
 * For 10 million records -> 50-70 ms
 * @param data
 * @param order_column
 */
void ips4o_parallel_sort(Dataset &data, const FileSchema &schema, const size_t &order_idx) {
    ips4o::parallel::sort(
        data.begin(),
        data.end(),
        [&](const DataRow &a, const DataRow &b) {
            return a[order_idx] < b[order_idx];
        }
    );
};


void SortUtils::sort_dataset(Dataset &data, const FileSchema &schema, const size_t &order_idx,
                             size_t parallel_threads) {
    auto start = std::chrono::high_resolution_clock::now();

    ips2ra_parallel_sort(data, schema, order_idx, parallel_threads);

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    std::cout << "Time taken for SORTING: " << duration.count() << " ms" << std::endl;
}

void SortUtils::sort_dataset_indices(const Dataset &data, std::vector<size_t> &indices, const size_t &order_idx) {
    // auto start = std::chrono::high_resolution_clock::now();

    auto proj = [&](size_t idx) {
        return static_cast<uint32_t>(data[idx][order_idx]) ^ (1UL << 31);
    };

    ips2ra::sort(indices.begin(), indices.end(), proj);

    // auto end = std::chrono::high_resolution_clock::now();
    // auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    // std::cout << "Time taken for SORTING: " << duration.count() << " ms" << std::endl;
}
