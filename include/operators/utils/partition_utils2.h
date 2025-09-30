//
// Created by Vegim Shala on 11.7.25.
//
#pragma once

#include "data_io.h"
#include <thread>
#include <unordered_set>

namespace PartitionUtils2 {
    using PartitionKey = std::string;

    // Custom hash for vector<int64_t>
    struct VecHash {
        size_t operator()(const std::vector<int32_t>& v) const noexcept {
            // 64-bit FNV-1a-ish multiplicative mix
            uint64_t h = 1469598103934665603ull;
            for (int32_t x : v) {
                uint64_t u = static_cast<uint64_t>(static_cast<uint32_t>(x));
                h ^= u;
                h *= 1099511628211ull;
            }
            // final xor-fold for 64->size_t if needed
            if constexpr (sizeof(size_t) < 8) {
                h ^= (h >> 32);
            }
            return static_cast<size_t>(h);
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


    using IndexDataset = std::vector<size_t>; // indices into the original Dataset

    using PartitionIndexResult = std::variant<
        std::unordered_map<int32_t, IndexDataset>,
        std::unordered_map<std::vector<int32_t>, IndexDataset, VecHash, VecEq>
    >;

    inline std::vector<size_t> compute_col_indices(
        const FileSchema &schema,
        const std::vector<std::string> &partition_columns
    ) {
        std::vector<size_t> col_indices;
        col_indices.reserve(partition_columns.size());
        for (auto &c: partition_columns) {
            col_indices.push_back(schema.index_of(c));
        }
        return col_indices;
    }

    // --- Helper: process a row and update local map
    inline void process_row(
        const Dataset &dataset,
        size_t row_idx,
        const std::vector<size_t> &col_indices,
        std::unordered_map<std::vector<int32_t>, IndexDataset, VecHash, VecEq> &local_map
    ) {
        std::vector<int32_t> key;
        key.reserve(col_indices.size());
        for (auto idx: col_indices) {
            key.push_back(dataset[row_idx][idx]);
        }
        auto &vec = local_map[key];
        if (vec.empty()) {
            vec.reserve(64);
        }
        vec.push_back(row_idx);
    }

    inline void merge_maps(
        std::unordered_map<std::vector<int32_t>, IndexDataset, VecHash, VecEq> &global,
        std::vector<std::unordered_map<std::vector<int32_t>, IndexDataset, VecHash, VecEq> > &thread_parts
    ) {
        for (auto &tp: thread_parts) {
            for (auto &kv: tp) {
                auto &dst = global[kv.first];
                if (dst.empty()) {
                    dst.reserve(kv.second.size());
                }
                std::move(kv.second.begin(), kv.second.end(), std::back_inserter(dst));
            }
        }
    }

    PartitionIndexResult partition_dataset_index(
        const Dataset &dataset,
        const FileSchema &schema,
        const std::vector<std::string> &partition_columns
    );

    // New function: partition but return indices (no row copies)
    PartitionIndexResult partition_dataset_index_morsel(
        const Dataset &dataset,
        const FileSchema &schema,
        const std::vector<std::string> &partition_columns,
        size_t num_threads = std::thread::hardware_concurrency(),
        size_t morsel_size = 2048
    );

    using RadixPartitionResult = std::vector<IndexDataset>;

    struct RadixSetup {
        size_t n;
        size_t num_buckets;
        size_t mask;
        size_t col_idx;
    };

    inline RadixSetup radix_setup(
        const Dataset &dataset,
        const FileSchema &schema,
        const std::vector<std::string> &partition_columns,
        size_t radix_bits
    ) {
        RadixSetup s{};
        s.n = dataset.size();
        s.num_buckets = size_t(1) << radix_bits;
        s.mask = s.num_buckets - 1;
        s.col_idx = schema.index_of(partition_columns[0]);
        return s;
    }

    // --- Count worker for a given row range
    inline void radix_count_range(
        const Dataset &dataset,
        size_t start, size_t end,
        size_t col_idx, size_t mask,
        std::vector<size_t> &counts
    ) {
        for (size_t i = start; i < end; ++i) {
            auto value = static_cast<uint32_t>(dataset[i][col_idx]);
            size_t bucket = value & mask;
            counts[bucket]++;
        }
    }

    // --- Scatter worker for a given row range
    inline void radix_scatter_range(
        const Dataset &dataset,
        size_t start, size_t end,
        size_t col_idx, size_t mask,
        std::vector<size_t> &local_offsets,
        RadixPartitionResult &buckets
    ) {
        for (size_t i = start; i < end; ++i) {
            uint32_t value = static_cast<uint32_t>(dataset[i][col_idx]);
            size_t bucket = value & mask;
            buckets[bucket][local_offsets[bucket]++] = i;
        }
    }

    // --- Allocate buckets and compute global + per-morsel offsets
    inline void radix_prepare_buckets(
        const std::vector<std::vector<size_t> > &morsel_bucket_counts,
        size_t num_buckets,
        RadixPartitionResult &buckets,
        std::vector<std::vector<size_t> > &morsel_bucket_offsets
    ) {
        // Total per bucket
        std::vector<size_t> bucket_totals(num_buckets, 0);
        for (size_t b = 0; b < num_buckets; ++b) {
            for (auto &counts: morsel_bucket_counts) {
                bucket_totals[b] += counts[b];
            }
        }

        buckets.resize(num_buckets);
        for (size_t b = 0; b < num_buckets; ++b) {
            buckets[b].resize(bucket_totals[b]);
        }

        // Compute prefix offsets per morsel
        morsel_bucket_offsets.resize(morsel_bucket_counts.size(),
                                     std::vector<size_t>(num_buckets, 0));
        for (size_t b = 0; b < num_buckets; ++b) {
            size_t offset = 0;
            for (size_t m = 0; m < morsel_bucket_counts.size(); ++m) {
                morsel_bucket_offsets[m][b] = offset;
                offset += morsel_bucket_counts[m][b];
            }
        }
    }

    RadixPartitionResult partition_dataset_radix_morsel(
        const Dataset &dataset,
        const FileSchema &schema,
        const std::vector<std::string> &partition_columns,
        size_t num_threads,
        size_t radix_bits = 8, // 256 buckets
        size_t morsel_size = 2048 // 4KB morsels
    );

    RadixPartitionResult partition_dataset_radix_sequential(
        const Dataset &dataset,
        const FileSchema &schema,
        const std::vector<std::string> &partition_columns,
        size_t radix_bits
    );


    // --- Single-column helpers (fast path) ---
    using SingleKeyIndexMap =
    std::unordered_map<int32_t, IndexDataset>;

    inline void merge_maps_1col(
        SingleKeyIndexMap &global,
        std::vector<SingleKeyIndexMap> &thread_parts
    ) {
        for (auto &tp: thread_parts) {
            for (auto &kv: tp) {
                auto &dst = global[kv.first];
                if (dst.empty()) dst.reserve(kv.second.size());
                std::move(kv.second.begin(), kv.second.end(), std::back_inserter(dst));
            }
        }
    }

    // --- Post-process: buckets -> per-key partitions (1-col) ---
    // Sequential
    SingleKeyIndexMap radix_buckets_to_partitions_sequential(
        const Dataset &dataset,
        size_t col_idx,
        RadixPartitionResult &buckets
    );

    // Parallel over buckets
    SingleKeyIndexMap radix_buckets_to_partitions_morsel(
        const Dataset &dataset,
        size_t col_idx,
        RadixPartitionResult &buckets,
        size_t num_threads
    );

    // --- Unified dispatchers (what you’ll call from the algorithm) ---
    // Decide strategy based on number of partition columns.
    PartitionIndexResult partition_indices_sequential(
        const Dataset &dataset,
        const FileSchema &schema,
        const std::vector<std::string> &partition_columns,
        size_t radix_bits = 8 // used only for 1-col
    );

    PartitionIndexResult partition_indices_parallel(
        const Dataset &dataset,
        const FileSchema &schema,
        const std::vector<std::string> &partition_columns,
        size_t num_threads = std::thread::hardware_concurrency(),
        size_t morsel_size = 2048,
        size_t radix_bits = 8 // used only for 1-col
    );

    // Optional simple 64-bit mixer (cheap, adequate)
    static inline uint64_t mix64(uint64_t x) {
        x ^= x >> 33; x *= 0xff51afd7ed558ccdULL;
        x ^= x >> 33; x *= 0xc4ceb9fe1a85ec53ULL;
        x ^= x >> 33; return x;
    }

    PartitionIndexResult
    partition_dataset_hash_radix_sequential_multi(
        const Dataset &dataset,
        const FileSchema &schema,
        const std::vector<std::string> &partition_columns,
        size_t radix_bits /* e.g., 10 -> 1024 buckets */
    );



    // NEW CHANGES OVER HERE
    inline uint32_t encode_key(int32_t v) {
        return static_cast<uint32_t>(v) ^ 0x80000000u; // same trick you already use
    }

    struct PartitionPayload {
        std::vector<uint32_t> order_keys; // encoded order column
        std::vector<uint32_t> values;     // aggregation values
    };

    // Same key types as before, just mapping to payload instead of index vectors.
    using PayloadMap1 = std::unordered_map<int32_t, PartitionPayload>;
    using PayloadMapN = std::unordered_map<std::vector<int32_t>, PartitionPayload, VecHash, VecEq>;

    // Variant result, mirroring PartitionIndexResult
    using PartitionPayloadResult = std::variant<PayloadMap1, PayloadMapN>;

    PartitionPayloadResult partition_payloads_parallel(
    const Dataset &dataset,
    const FileSchema &schema,
    const std::vector<std::string> &partition_columns, // must be size()==1 in this radix path
    const std::string &order_column,
    const std::string &value_column,
    size_t num_threads,
    size_t morsel_size,
    size_t radix_bits);
}
