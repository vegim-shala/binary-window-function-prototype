//
// Created by Vegim Shala on 11.7.25.
//
#pragma once

#include "data_io.h"

namespace PartitionUtils {

    using PartitionKey = std::string;

    std::unordered_map<PartitionKey, Dataset>
    partition_dataset(const Dataset& input, const std::string& partition_column);

}