//
// Created by Vegim Shala on 11.7.25.
//

#include "operators/utils/partition_utils.h"
#include <thread>
#include <vector>
#include <unordered_map>
#include <string>
#include <atomic>
#include <numeric>

using namespace PartitionUtils;


PartitionIndexResult PartitionUtils::partition_dataset_index(
    const Dataset &dataset,
    const FileSchema &schema,
    const std::vector<std::string> &partition_columns
) {
    if (partition_columns.empty()) {
        std::unordered_map<std::vector<int32_t>, IndexDataset, VecHash, VecEq> result;
        result[{}].reserve(dataset.size());
        for (size_t i = 0; i < dataset.size(); ++i) {
            result[{}].push_back(i);
        }
        return result;
    }

    auto col_indices = compute_col_indices(schema, partition_columns);
    size_t n = dataset.size();

    std::unordered_map<std::vector<int32_t>, IndexDataset, VecHash, VecEq> global;
    global.reserve(n / 4);

    for (size_t i = 0; i < n; ++i) {
        process_row(dataset, i, col_indices, global);
    }

    return global;
}


PartitionIndexResult PartitionUtils::partition_dataset_index_morsel(
    const Dataset &dataset,
    const FileSchema &schema,
    const std::vector<std::string> &partition_columns,
    size_t num_threads,
    size_t morsel_size
) {
    if (partition_columns.empty()) {
        std::unordered_map<std::vector<int32_t>, IndexDataset, VecHash, VecEq> result;
        result[{}].reserve(dataset.size());
        for (size_t i = 0; i < dataset.size(); ++i) {
            result[{}].push_back(i);
        }
        return result;
    }

    auto col_indices = compute_col_indices(schema, partition_columns);
    size_t n = dataset.size();
    std::atomic<size_t> next_morsel(0);

    std::vector<std::unordered_map<std::vector<int32_t>, IndexDataset, VecHash, VecEq> > thread_parts(num_threads);

    auto worker = [&](size_t tid) {
        auto &local = thread_parts[tid];
        while (true) {
            size_t start = next_morsel.fetch_add(morsel_size);
            if (start >= n) break;
            size_t end = std::min(start + morsel_size, n);
            for (size_t i = start; i < end; ++i) {
                process_row(dataset, i, col_indices, local);
            }
        }
    };

    std::vector<std::thread> threads;
    threads.reserve(num_threads);
    for (size_t t = 0; t < num_threads; ++t) {
        threads.emplace_back(worker, t);
    }
    for (auto &th: threads) {
        th.join();
    }

    std::unordered_map<std::vector<int32_t>, IndexDataset, VecHash, VecEq> global;
    global.reserve(num_threads * 4);
    merge_maps(global, thread_parts);

    return global;
}


RadixPartitionResult PartitionUtils::partition_dataset_radix_morsel(
    const Dataset &dataset,
    const FileSchema &schema,
    const std::vector<std::string> &partition_columns,
    size_t num_threads,
    size_t radix_bits,
    size_t morsel_size
) {
    if (partition_columns.empty() || partition_columns.size() > 1) {
        RadixPartitionResult result(1);
        result[0].reserve(dataset.size());
        for (size_t i = 0; i < dataset.size(); ++i) result[0].push_back(i);
        return result;
    }

    auto s = radix_setup(dataset, schema, partition_columns, radix_bits);
    const size_t num_morsels = (s.n + morsel_size - 1) / morsel_size;

    // Pass 1: count per morsel
    std::vector<std::vector<size_t> > morsel_bucket_counts(
        num_morsels, std::vector<size_t>(s.num_buckets, 0)); {
        std::atomic<size_t> next_morsel(0);
        std::vector<std::thread> threads;
        threads.reserve(num_threads);
        for (size_t t = 0; t < num_threads; ++t) {
            threads.emplace_back([&]() {
                while (true) {
                    size_t m = next_morsel.fetch_add(1);
                    if (m >= num_morsels) break;
                    size_t start = m * morsel_size;
                    size_t end = std::min(start + morsel_size, s.n);
                    radix_count_range(dataset, start, end,
                                      s.col_idx, s.mask, morsel_bucket_counts[m]);
                }
            });
        }
        for (auto &th: threads) th.join();
    }

    // Prepare buckets + offsets
    RadixPartitionResult buckets;
    std::vector<std::vector<size_t> > morsel_bucket_offsets;
    radix_prepare_buckets(morsel_bucket_counts, s.num_buckets, buckets, morsel_bucket_offsets);

    // Pass 2: scatter
    {
        std::atomic<size_t> next_morsel(0);
        std::vector<std::thread> threads;
        threads.reserve(num_threads);
        for (size_t t = 0; t < num_threads; ++t) {
            threads.emplace_back([&]() {
                while (true) {
                    size_t m = next_morsel.fetch_add(1);
                    if (m >= num_morsels) break;
                    size_t start = m * morsel_size;
                    size_t end = std::min(start + morsel_size, s.n);
                    std::vector<size_t> local_offsets = morsel_bucket_offsets[m];
                    radix_scatter_range(dataset, start, end,
                                        s.col_idx, s.mask,
                                        local_offsets, buckets);
                }
            });
        }
        for (auto &th: threads) th.join();
    }

    return buckets;
}


RadixPartitionResult PartitionUtils::partition_dataset_radix_sequential(
    const Dataset &dataset,
    const FileSchema &schema,
    const std::vector<std::string> &partition_columns,
    size_t radix_bits
) {
    if (partition_columns.empty() || partition_columns.size() > 1) {
        RadixPartitionResult result(1);
        result[0].reserve(dataset.size());
        for (size_t i = 0; i < dataset.size(); ++i) {
            result[0].push_back(i);
        }
        return result;
    }

    auto s = radix_setup(dataset, schema, partition_columns, radix_bits);

    // ---- PASS 1: Count ----
    std::vector<size_t> counts(s.num_buckets, 0);
    radix_count_range(dataset, 0, s.n, s.col_idx, s.mask, counts);

    // ---- Allocate buckets ----
    RadixPartitionResult buckets(s.num_buckets);
    for (size_t b = 0; b < s.num_buckets; ++b) {
        buckets[b].resize(counts[b]);
    }

    // ---- Prefix sums -> offsets ----
    std::vector<size_t> offsets(s.num_buckets, 0);
    for (size_t b = 1; b < s.num_buckets; ++b) {
        offsets[b] = offsets[b - 1] + counts[b - 1];
    }

    // ---- Cursors (mutable write positions) ----
    std::vector<size_t> cursors = offsets;

    // ---- PASS 2: Scatter (via helper) ----
    radix_scatter_range(dataset, 0, s.n, s.col_idx, s.mask, cursors, buckets);

    return buckets;
}


// ---------- Buckets -> per-key partitions (sequential) ----------
SingleKeyIndexMap PartitionUtils::radix_buckets_to_partitions_sequential(
    const Dataset &dataset,
    size_t col_idx,
    RadixPartitionResult &buckets
) {
    // Option A (one-pass, may reallocate vectors): fastest in practice for many real datasets
    SingleKeyIndexMap out;
    // A light heuristic reserve: number of non-empty buckets
    size_t non_empty = 0;
    for (auto &b: buckets) if (!b.empty()) ++non_empty;
    out.reserve(non_empty * 2);

    for (auto &bucket: buckets) {
        for (size_t row_idx: bucket) {
            int32_t key = dataset[row_idx][col_idx];
            auto &vec = out[key];
            if (vec.empty()) vec.reserve(64);
            vec.push_back(row_idx);
        }
    }
    return out;
}

// ---------- Buckets -> per-key partitions (parallel over buckets) ----------
SingleKeyIndexMap PartitionUtils::radix_buckets_to_partitions_morsel(
    const Dataset &dataset,
    size_t col_idx,
    RadixPartitionResult &buckets,
    size_t num_threads
) {
    const size_t B = buckets.size();
    std::atomic<size_t> next_bucket{0};

    // Thread-local maps -> merge
    std::vector<SingleKeyIndexMap> thread_parts(num_threads);

    auto worker = [&](size_t tid) {
        auto &local = thread_parts[tid];
        // modest reserve to reduce early rehash
        local.reserve(64);

        while (true) {
            size_t b = next_bucket.fetch_add(1);
            if (b >= B) break;
            auto &bucket = buckets[b];
            for (size_t row_idx: bucket) {
                int32_t key = dataset[row_idx][col_idx];
                auto &vec = local[key];
                if (vec.empty()) vec.reserve(64);
                vec.push_back(row_idx);
            }
        }
    };

    std::vector<std::thread> threads;
    threads.reserve(num_threads);
    for (size_t t = 0; t < num_threads; ++t) threads.emplace_back(worker, t);
    for (auto &th: threads) th.join();

    SingleKeyIndexMap global;
    global.reserve(num_threads * 64);
    merge_maps_1col(global, thread_parts);
    return global;
}

PartitionIndexResult PartitionUtils::partition_indices_sequential(
    const Dataset &dataset,
    const FileSchema &schema,
    const std::vector<std::string> &partition_columns,
    size_t radix_bits
) {
    // No partition cols -> single empty key
    if (partition_columns.empty()) {
        std::unordered_map<std::vector<int32_t>, IndexDataset, VecHash, VecEq> result;
        result[{}].reserve(dataset.size());
        for (size_t i = 0; i < dataset.size(); ++i) result[{}].push_back(i);
        return result;
    }

    if (partition_columns.size() == 1) {
        // 1-col fast path: radix partition -> compress per key
        auto s = radix_setup(dataset, schema, partition_columns, radix_bits);
        auto buckets = partition_dataset_radix_sequential(dataset, schema, partition_columns, radix_bits);
        auto by_key = radix_buckets_to_partitions_sequential(dataset, s.col_idx, buckets);
        return by_key; // variant will hold unordered_map<int32_t, IndexDataset>
    } else {
        // multi-col: hash partition (existing sequential)
        return partition_dataset_index(dataset, schema, partition_columns);
    }
}

PartitionUtils::PartitionIndexResult PartitionUtils::partition_indices_parallel(
    const Dataset &dataset,
    const FileSchema &schema,
    const std::vector<std::string> &partition_columns,
    size_t num_threads,
    size_t morsel_size,
    size_t radix_bits
) {
    if (partition_columns.empty()) {
        std::unordered_map<std::vector<int32_t>, IndexDataset, VecHash, VecEq> result;
        result[{}].reserve(dataset.size());
        for (size_t i = 0; i < dataset.size(); ++i) result[{}].push_back(i);
        return result;
    }

    if (partition_columns.size() == 1) {
        auto s = radix_setup(dataset, schema, partition_columns, radix_bits);
        auto buckets = partition_dataset_radix_morsel(
            dataset, schema, partition_columns,
            num_threads, radix_bits, morsel_size
        );
        auto by_key = radix_buckets_to_partitions_morsel(dataset, s.col_idx, buckets, num_threads);
        return by_key;
    } else {
        return partition_dataset_index_morsel(dataset, schema, partition_columns, num_threads, morsel_size);
    }
}