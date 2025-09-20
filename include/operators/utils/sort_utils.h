//
// Created by Vegim Shala on 11.7.25.
//

#pragma once

#include "data_io.h"

class SortUtils {
public:
    static void sort_dataset(Dataset &data, const FileSchema &schema, const std::string &order_column, size_t parallel_threads = 0);
};