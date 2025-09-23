//
// Created by Vegim Shala on 11.7.25.
//
#pragma once

#include "data_io.h"
#include <thread>
#include <unordered_set>

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

    using ProbeKeySet = std::variant<
        std::unordered_set<int32_t>,
        std::unordered_set<std::vector<int32_t>, VecHash, VecEq>
    >;

    // helper for static_assert
    template<class T>
    inline constexpr bool always_false = false;

    inline ProbeKeySet extract_probe_keys(const PartitionResult &probe_partitions) {
        return std::visit([](auto &&partitions) -> ProbeKeySet {
            using PartitionsT = std::decay_t<decltype(partitions)>;

            if constexpr (std::is_same_v<PartitionsT, std::unordered_map<int32_t, Dataset> >) {
                std::unordered_set<int32_t> keys;
                keys.reserve(partitions.size());
                for (const auto &kv: partitions) {
                    keys.insert(kv.first);
                }
                return keys;
            } else if constexpr (std::is_same_v<
                PartitionsT,
                std::unordered_map<std::vector<int32_t>, Dataset, VecHash, VecEq> >) {
                std::unordered_set<std::vector<int32_t>, VecHash, VecEq> keys;
                keys.reserve(partitions.size());
                for (const auto &kv: partitions) {
                    keys.insert(kv.first);
                }
                return keys;
            } else {
                static_assert(always_false<PartitionsT>, "Unsupported PartitionResult type");
            }
        }, probe_partitions);
    }


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

    PartitionResult partition_input(
        Dataset &dataset,
        const FileSchema &schema,
        const std::vector<std::string> &partition_columns,
        const ProbeKeySet &probe_keys
    );

    PartitionResult partition_input_morsel(
        Dataset &input,
        const FileSchema &schema,
        const std::vector<std::string> &partition_columns,
        const PartitionResult &probe_partitions,
        size_t num_threads = std::thread::hardware_concurrency(),
        size_t morsel_size = 2048
    );

    using IndexDataset = std::vector<size_t>; // indices into the original Dataset

    using PartitionIndexResult = std::variant<
        std::unordered_map<int32_t, IndexDataset>,
        std::unordered_map<std::vector<int32_t>, IndexDataset, VecHash, VecEq>
    >;

    // New function: partition but return indices (no row copies)
    PartitionIndexResult partition_dataset_index_morsel(
        const Dataset &dataset,
        const FileSchema &schema,
        const std::vector<std::string> &partition_columns,
        size_t num_threads = std::thread::hardware_concurrency(),
        size_t morsel_size = 2048
    );

    using RadixPartitionResult = std::vector<IndexDataset>;

    RadixPartitionResult partition_dataset_radix(
        const Dataset &dataset,
        const FileSchema &schema,
        const std::vector<std::string> &partition_columns,
        size_t num_threads,
        size_t radix_bits = 8 // 256 buckets by default
    );

    RadixPartitionResult partition_dataset_radix_advanced(
        const Dataset &dataset,
        const FileSchema &schema,
        const std::vector<std::string> &partition_columns,
        size_t num_threads,
        size_t radix_bits_per_pass = 6 // 64 buckets per pass
    );

    RadixPartitionResult partition_dataset_radix_morsel(
        const Dataset &dataset,
        const FileSchema &schema,
        const std::vector<std::string> &partition_columns,
        size_t num_threads,
        size_t radix_bits = 8, // 256 buckets
        size_t morsel_size = 2048 // 4KB morsels
    );
}
