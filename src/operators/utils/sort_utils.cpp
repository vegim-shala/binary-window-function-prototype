//
// Created by Vegim Shala on 11.7.25.
//
#include "operators/utils/sort_utils.h"

void SortUtils::sort_dataset(Dataset& data, const std::vector<std::string>& order_columns) {
    std::sort(data.begin(), data.end(), [&](const DataRow& a, const DataRow& b) {
        for (const auto& col : order_columns) {
            const auto& val_a = a.at(col);
            const auto& val_b = b.at(col);
            if (val_a != val_b) {
                if (std::holds_alternative<std::string>(val_a)) {
                    return std::get<std::string>(val_a) < std::get<std::string>(val_b);
                }
                else if (std::holds_alternative<double>(val_a)) {
                    return std::get<double>(val_a) < std::get<double>(val_b);
                }
                else if (std::holds_alternative<int>(val_a)) {
                    return std::get<int>(val_a) < std::get<int>(val_b);
                }
            }
        }
        return false;
    });
}