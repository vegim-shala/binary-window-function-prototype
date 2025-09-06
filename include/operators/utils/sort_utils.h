//
// Created by Vegim Shala on 11.7.25.
//

#pragma once

#include "data_io.h"

class SortUtils {
public:
    static void sort_dataset(Dataset& data, const std::string& order_column);
    static void radix_sort_rows(std::vector<DataRow>& data, std::string order_column);
    static void counting_sort_rows(std::vector<DataRow>& data, std::string order_column);
};