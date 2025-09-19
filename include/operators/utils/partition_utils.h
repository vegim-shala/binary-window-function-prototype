//
// Created by Vegim Shala on 11.7.25.
//
#pragma once

#include "data_io.h"
#include <thread>

namespace PartitionUtils {

    using PartitionKey = std::string;

    // Custom hash for vector<int64_t>
    struct VecHash {
        std::size_t operator()(const std::vector<int32_t> &v) const noexcept {
            std::size_t h = 0;
            for (auto x: v) {
                // Simple hash combine - avoid expensive operations
                h ^= std::hash<int32_t>{}(x) + 0x9e3779b9 + (h << 6) + (h >> 2);
            }
            return h;
        }
    };

    struct VecEq {
        bool operator()(const std::vector<int32_t> &a,
                        const std::vector<int32_t> &b) const noexcept {
            return a == b;
        }
    };

    using PartitionResult = std::variant<
        std::unordered_map<int32_t, Dataset>,
        std::unordered_map<std::vector<int32_t>, Dataset, VecHash, VecEq>
    >;

    PartitionResult partition_dataset(
    Dataset &dataset,
    const FileSchema &schema,
    const std::vector<std::string> &partition_columns
);
    PartitionResult partition_dataset_morsel(
    Dataset &dataset,
    const FileSchema &schema,
    const std::vector<std::string> &partition_columns,
    size_t num_threads = std::thread::hardware_concurrency(),
    size_t morsel_size = 2048
);

}