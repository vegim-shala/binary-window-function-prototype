//
// Created by Vegim Shala on 11.7.25.
//

#pragma once

#include "data_io.h"

class SortUtils {
public:
    static void sort_dataset(Dataset &data, const FileSchema &schema, const size_t &order_idx, size_t parallel_threads = 0);
    static void sort_dataset_indices(const Dataset &data, std::vector<size_t> &indices, const size_t &order_idx);
};