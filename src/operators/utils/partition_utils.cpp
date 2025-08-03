//
// Created by Vegim Shala on 11.7.25.
//

#include "operators/utils/partition_utils.h"

std::unordered_map<std::string, Dataset> PartitionUtils::partition_dataset(
    const Dataset& dataset,
    const std::vector<std::string>& partition_columns
) {
    std::unordered_map<std::string, Dataset> partitions;

    if (partition_columns.empty()) {
        partitions.insert({"__FULL_DATASET__", dataset});
        return partitions;
    }

    for (const auto& row : dataset) {
        std::string key;
        for (const auto& col : partition_columns) {
            key += std::get<std::string>(row.at(col)) + "|";
        }
        partitions[key].push_back(row);
    }

    return partitions;
}

