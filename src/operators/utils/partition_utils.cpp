//
// Created by Vegim Shala on 11.7.25.
//

#include "operators/utils/partition_utils.h"
#include <thread>
#include <vector>
#include <unordered_map>
#include <string>
#include <atomic>
#include <future>
#include <iostream>
#include <numeric>
#include <ips2ra/ips2ra.hpp>
#include <operators/utils/thread_pool.h>

using namespace PartitionUtils;


PartitionIndexResult PartitionUtils::partition_dataset_index(
    const Dataset &dataset,
    const FileSchema &schema,
    const std::vector<std::string> &partition_columns
) {
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
    ThreadPool &pool,
    size_t num_threads,
    size_t morsel_size
) {
    auto col_indices = compute_col_indices(schema, partition_columns);
    size_t n = dataset.size();
    std::atomic<size_t> next_morsel(0);

    std::vector<std::unordered_map<std::vector<int32_t>, IndexDataset, VecHash, VecEq>> thread_parts(num_threads);

    {
        std::vector<std::future<void>> futs;
        futs.reserve(num_threads);

        for (size_t tid = 0; tid < num_threads; ++tid) {
            futs.push_back(pool.submit([&, tid] {
                auto &local = thread_parts[tid];
                while (true) {
                    size_t start = next_morsel.fetch_add(morsel_size);
                    if (start >= n) break;
                    size_t end = std::min(start + morsel_size, n);
                    for (size_t i = start; i < end; ++i) {
                        process_row(dataset, i, col_indices, local);
                    }
                }
            }));
        }
        for (auto &f : futs) f.get();
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
    ThreadPool &pool,
    size_t radix_bits,
    size_t morsel_size
) {
    auto s = radix_setup(dataset, schema, partition_columns, radix_bits);
    const size_t num_morsels = (s.n + morsel_size - 1) / morsel_size;

    // Pass 1: count per morsel
    std::vector<std::vector<size_t>> morsel_bucket_counts(
        num_morsels, std::vector<size_t>(s.num_buckets, 0));

    {
        std::vector<std::future<void>> futs;
        futs.reserve(num_threads);

        for (size_t tid = 0; tid < num_threads; ++tid) {
            futs.push_back(pool.submit([&, tid] {
                const size_t m_start = (num_morsels * tid) / num_threads;
                const size_t m_end   = (num_morsels * (tid + 1)) / num_threads;
                for (size_t m = m_start; m < m_end; ++m) {
                    const size_t start = m * morsel_size;
                    const size_t end   = std::min(start + morsel_size, s.n);
                    radix_count_range(dataset, start, end,
                                      s.col_idx, s.mask, morsel_bucket_counts[m]);
                }
            }));
        }
        for (auto &f : futs) f.get();
    }

    // Prepare buckets + offsets
    RadixPartitionResult buckets;
    std::vector<std::vector<size_t>> morsel_bucket_offsets;
    radix_prepare_buckets(morsel_bucket_counts, s.num_buckets, buckets, morsel_bucket_offsets);

    // Pass 2: scatter
    {
        std::vector<std::future<void>> futs;
        futs.reserve(num_threads);

        for (size_t tid = 0; tid < num_threads; ++tid) {
            futs.push_back(pool.submit([&, tid] {
                const size_t m_start = (num_morsels * tid) / num_threads;
                const size_t m_end   = (num_morsels * (tid + 1)) / num_threads;
                for (size_t m = m_start; m < m_end; ++m) {
                    const size_t start = m * morsel_size;
                    const size_t end   = std::min(start + morsel_size, s.n);
                    auto &local_offsets = morsel_bucket_offsets[m];
                    radix_scatter_range(dataset, start, end,
                                        s.col_idx, s.mask,
                                        local_offsets, buckets);
                }
            }));
        }
        for (auto &f : futs) f.get();
    }

    return buckets;
}


PartitionUtils::RadixPartitionResult PartitionUtils::partition_dataset_radix_sequential(
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

    // ---- PASS 2: Scatter ----
    // Per-bucket cursors must be relative to each bucket (0..counts[b]-1)
    std::vector<size_t> cursors(s.num_buckets, 0);
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
    out.reserve(buckets.size());

    for (auto &bucket: buckets) { // for every bucket
        for (size_t row_idx: bucket) { // for every row in the bucket
            int32_t key = dataset[row_idx][col_idx]; // extract the key
            auto &vec = out[key]; // on the first iteration out is empty, so this creates a new vector for the key and then
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
    size_t num_threads,
    ThreadPool &pool
) {
    const size_t B = buckets.size();
    std::atomic<size_t> next_bucket{0};
    std::vector<SingleKeyIndexMap> thread_parts(num_threads);

    {
        std::vector<std::future<void>> futs;
        futs.reserve(num_threads);

        for (size_t tid = 0; tid < num_threads; ++tid) {
            futs.push_back(pool.submit([&, tid] {
                auto &local = thread_parts[tid];
                local.reserve(64);

                while (true) {
                    size_t b = next_bucket.fetch_add(1);
                    if (b >= B) break;
                    auto &bucket = buckets[b];
                    for (size_t row_idx : bucket) {
                        int32_t key = dataset[row_idx][col_idx];
                        auto &vec = local[key];
                        if (vec.empty()) vec.reserve(64);
                        vec.push_back(row_idx);
                    }
                }
            }));
        }
        for (auto &f : futs) f.get();
    }

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

        auto start_bucketing = std::chrono::high_resolution_clock::now();
        auto buckets = partition_dataset_radix_sequential(dataset, schema, partition_columns, radix_bits);
        auto end_bucketing = std::chrono::high_resolution_clock::now();
        std::cout << "Radix bucketing wall time: "
                  << std::chrono::duration_cast<std::chrono::microseconds>(end_bucketing - start_bucketing).count()
                  << " ms" << std::endl;

        auto start_grouping = std::chrono::high_resolution_clock::now();
        auto by_key = radix_buckets_to_partitions_sequential(dataset, s.col_idx, buckets);
        auto end_grouping = std::chrono::high_resolution_clock::now();
        std::cout << "Radix grouping wall time: "
                  << std::chrono::duration_cast<std::chrono::microseconds>(end_grouping - start_grouping).count()
                  << " ms" << std::endl;
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
    ThreadPool &pool,
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

        auto start_bucketing = std::chrono::high_resolution_clock::now();
        auto buckets = partition_dataset_radix_morsel(
            dataset, schema, partition_columns,
            num_threads, pool, radix_bits, morsel_size
        );
        auto end_bucketing = std::chrono::high_resolution_clock::now();
        std::cout << "Radix bucketing wall time: "
                  << std::chrono::duration_cast<std::chrono::microseconds>(end_bucketing - start_bucketing).count()
                  << " ms" << std::endl;

        auto start_grouping = std::chrono::high_resolution_clock::now();
        auto by_key = radix_buckets_to_partitions_morsel(dataset, s.col_idx, buckets, 8, pool);
        auto end_grouping = std::chrono::high_resolution_clock::now();
        std::cout << "Radix grouping wall time: "
                  << std::chrono::duration_cast<std::chrono::microseconds>(end_grouping - start_grouping).count()
                  << " ms" << std::endl;
        return by_key;
    } else {
        return partition_dataset_index_morsel(dataset, schema, partition_columns, pool, num_threads, morsel_size);
    }
}

// This was used to showcase results in the paper for multi-column partitioning with radix hashing.
// Otherwise not used in the main cache
PartitionIndexResult
PartitionUtils::partition_dataset_hash_radix_sequential_multi(
    const Dataset &dataset,
    const FileSchema &schema,
    const std::vector<std::string> &partition_columns,
    size_t radix_bits /* e.g., 10 -> 1024 buckets */
) {
    // Expect multi-column; if not, fall back or assert as you prefer
    if (partition_columns.size() < 2) {
        // You can route to your 1-col radix, but for this test just use index-partition:
        return partition_dataset_index(dataset, schema, partition_columns);
    }

    const size_t n = dataset.size();
    const size_t num_buckets = size_t(1) << radix_bits;
    const size_t mask = num_buckets - 1;

    // Precompute col indices
    std::vector<size_t> col_indices = compute_col_indices(schema, partition_columns);
    const size_t K = col_indices.size();

    // ---- PASS 1: hash -> radix count ----
    std::vector<size_t> counts(num_buckets, 0);
    for (size_t i = 0; i < n; ++i) {
        uint64_t h = 0;
        // combine all columns; int32 -> uint64_t; mix after each step
        for (size_t j = 0; j < K; ++j) {
            h = mix64(h ^ (uint64_t)(uint32_t)dataset[i][col_indices[j]]);
        }
        counts[h & mask]++; // low bits define bucket
    }

    // ---- Allocate buckets ----
    RadixPartitionResult buckets(num_buckets);
    for (size_t b = 0; b < num_buckets; ++b) {
        buckets[b].resize(counts[b]);
    }

    // ---- PASS 2: hash -> radix scatter ----
    std::vector<size_t> cursors(num_buckets, 0);
    for (size_t i = 0; i < n; ++i) {
        uint64_t h = 0;
        for (size_t j = 0; j < K; ++j) {
            h = mix64(h ^ (uint64_t)(uint32_t)dataset[i][col_indices[j]]);
        }
        size_t b = h & mask;
        buckets[b][cursors[b]++] = i;
    }

    // ---- REFINE: per-bucket exact key grouping ----
    // Build global map< vector<int32_t>, IndexDataset >
    std::unordered_map<std::vector<int32_t>, IndexDataset, VecHash, VecEq> global;
    // Heuristic reserve: many buckets, some keys per bucket
    global.reserve(std::max<size_t>(64, n / 16));

    std::vector<int32_t> key_buf;
    key_buf.reserve(K);

    for (auto &bucket : buckets) {
        if (bucket.empty()) continue;

        // Local map per bucket to reduce rehashing pressure on 'global'
        std::unordered_map<std::vector<int32_t>, IndexDataset, VecHash, VecEq> local;
        // Heuristic: half the bucket might be unique keys; tune as needed
        local.reserve(std::max<size_t>(8, bucket.size() / 2));

        for (size_t row_idx : bucket) {
            key_buf.clear();
            for (size_t j = 0; j < K; ++j) {
                key_buf.push_back(dataset[row_idx][col_indices[j]]);
            }
            auto &vec = local[key_buf];      // copies key_buf into the key
            if (vec.empty()) vec.reserve(4); // small hint
            vec.push_back(row_idx);
        }

        // Merge local->global (move vectors to avoid copies)
        for (auto &kv : local) {
            auto &dst = global[kv.first];
            if (dst.empty()) dst.reserve(kv.second.size());
            std::move(kv.second.begin(), kv.second.end(), std::back_inserter(dst));
        }
    }

    return global;
}
