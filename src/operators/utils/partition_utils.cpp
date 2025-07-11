//
// Created by Vegim Shala on 11.7.25.
//

#include "operators/utils/partition_utils.h"

std::unordered_map<std::string, Dataset> PartitionUtils::partition_dataset(
    const Dataset& dataset,
    const std::string& partition_column
) {
    std::unordered_map<std::string, Dataset> partitions;

    if (partition_column.empty()) {
        partitions.insert({"__FULL_DATASET__", dataset});
        return partitions;
    }

    for (const auto& row : dataset) {
        const auto& key = std::get<std::string>(row.at(partition_column));


        partitions[key].push_back(row);
    }

    return partitions;
}

