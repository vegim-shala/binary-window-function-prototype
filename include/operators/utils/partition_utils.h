//
// Created by Vegim Shala on 11.7.25.
//
#pragma once

#include "data_io.h"

namespace PartitionUtils {

    using PartitionKey = std::string;

    std::unordered_map<PartitionKey, Dataset>
    partition_dataset(const Dataset& input, const std::vector<std::string>& partition_columns);
    std::unordered_map<std::string, Dataset> partition_dataset_parallel(
        const Dataset& dataset,
        const std::vector<std::string>& partition_columns,
        size_t num_threads
    );

}