//
// Created by Vegim Shala on 11.7.25.
//
#include "operators/utils/sort_utils2.h"


#include <iostream>

#include "ips2ra.hpp"
#include "ips4o/ips4o.hpp"


void SortUtils2::sort_dataset(Dataset &data, const FileSchema &schema, const size_t &order_idx,
                             size_t parallel_threads) {
    auto start = std::chrono::high_resolution_clock::now();

    // ips2ra_parallel_sort(data, schema, order_idx, parallel_threads);

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    std::cout << "Time taken for SORTING: " << duration.count() << " ms" << std::endl;
}


void SortUtils2::sort_dataset_indices(const Dataset &data, std::vector<size_t> &indices, const size_t &order_idx) {
    // auto start = std::chrono::high_resolution_clock::now();

    // auto proj = [&](size_t idx) {
    //     return static_cast<uint32_t>(data[idx][order_idx]) ^ (1UL << 31);
    // };
    //
    // ips2ra::sort(indices.begin(), indices.end(), proj);

    // std::cout << "DEBUG: Starting parallel sort. indices.size() = " << indices.size()
    //         << ", data.size() = " << data.size() << std::endl;
    //
    // std::vector<uint32_t> order_keys(data.size());
    // for (size_t i = 0; i < data.size(); ++i) {
    //     order_keys[i] = static_cast<uint32_t>(data[i][order_idx]) ^ (1UL << 31);
    // }
    //
    // // std::cout << "DEBUG: Keys computed successfully" << std::endl;
    //
    // ips2ra::sort(indices.begin(), indices.end(),
    //          [&](size_t row_id) { return order_keys[row_id]; });
    //
    // // std::cout << "DEBUG: Sort completed successfully" << std::endl;
    //

    std::vector<std::pair<uint32_t, size_t>> pairs;
    pairs.reserve(indices.size());
    for (unsigned long row_id : indices) {
        uint32_t key = static_cast<uint32_t>(data[row_id][order_idx]) ^ (1UL << 31);
        pairs.emplace_back(key, row_id); // store both
    }

    // Sort by the first (key) only
    ips2ra::sort(pairs.begin(), pairs.end(),
                 [](const auto &p) { return p.first; });

    // Write back row_ids
    for (size_t i = 0; i < indices.size(); ++i) {
        indices[i] = pairs[i].second;
    }

    //
    // auto end = std::chrono::high_resolution_clock::now();
    // auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    // std::cout << "Time taken for SORTING: " << duration.count() << " ms" << std::endl;
}


#include <boost/sort/spreadsort/spreadsort.hpp>
#include <boost/sort/spreadsort/integer_sort.hpp>

struct PairShift {
    uint32_t operator()(const std::pair<uint32_t, size_t> &p, unsigned offset) const {
        return p.first >> offset;
    }
};

void SortUtils2::sort_dataset_indices_boost(const Dataset &data,
                                           std::vector<size_t> &indices,
                                           const size_t &order_idx) {
    auto start = std::chrono::high_resolution_clock::now();

    // Build key-index pairs
    std::vector<std::pair<uint32_t, size_t> > key_index(indices.size());
    for (size_t i = 0; i < indices.size(); ++i) {
        uint32_t k = static_cast<uint32_t>(data[indices[i]][order_idx]) ^ (1U << 31);
        key_index[i] = {k, indices[i]};
    }

    // Spreadsort on the pairs, using the first element as key
    boost::sort::spreadsort::integer_sort(
        key_index.begin(), key_index.end(), PairShift()
    );

    // Write back sorted indices
    for (size_t i = 0; i < indices.size(); ++i) {
        indices[i] = key_index[i].second;
    }

    // auto end = std::chrono::high_resolution_clock::now();
    // std::cout << "Time taken for SORTING: "
    //         << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count()
    //         << " ms" << std::endl;
}

void SortUtils2::sort_dataset_global_keys(const std::vector<uint32_t> &global_keys,
                                         std::vector<size_t> &indices) {
    // auto start = std::chrono::high_resolution_clock::now();

    ips2ra::sort(indices.begin(), indices.end(),
                 [&](size_t row_id) {
                     return global_keys[row_id];
                 });

    // auto end = std::chrono::high_resolution_clock::now();
    // std::cout << "Time taken for SORTING: "
    //         << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count()
    //         << " ms" << std::endl;
}
