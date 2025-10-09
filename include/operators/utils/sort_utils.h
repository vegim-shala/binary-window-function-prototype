//
// Created by Vegim Shala on 11.7.25.
//

#pragma once

#include "data_io.h"
#include "thread_pool.h"

class SortUtils {
public:
    static void sort_dataset(Dataset &data, const FileSchema &schema, const size_t &order_idx, size_t parallel_threads = 0);
    static void sort_dataset_indices(const Dataset &data, std::vector<size_t> &indices, const size_t &order_idx);
    static void sort_dataset_indices_parallel(const Dataset &data, std::vector<size_t> &indices, const size_t &order_idx, ThreadPool &pool);
    static void sort_dataset_indices_boost(const Dataset &data, std::vector<size_t> &indices, const size_t &order_idx);
    static void sort_dataset_global_keys(const std::vector<uint32_t> &global_keys, std::vector<size_t> &indices);
};
