//
// Created by Vegim Shala on 11.7.25.
//
#include "operators/utils/sort_utils.h"

void SortUtils::sort_dataset(Dataset& data, const std::string& order_column) {
    std::sort(data.begin(), data.end(), [&](const DataRow& a, const DataRow& b) {
        return extract_numeric(a.at(order_column)) < extract_numeric(b.at(order_column));
    });
}