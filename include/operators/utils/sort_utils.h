//
// Created by Vegim Shala on 11.7.25.
//

#pragma once

#include "data_io.h"

class SortUtils {
public:
    static void sort_dataset(Dataset& data, const std::vector<std::string>& order_columns);
};