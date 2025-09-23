//
// Created by Vegim Shala on 11.7.25.
//

#include "operators/utils/partition_utils.h"
#include <thread>
#include <vector>
#include <unordered_map>
#include <string>
#include <atomic>
#include <iostream>
#include <numeric>
#include <unordered_set>

// Safe integer extractor
// inline int32_t get_int32(const ColumnValue &val) {
//     return std::visit([](auto &&v) -> int32_t {
//         using T = std::decay_t<decltype(v)>;
//         if constexpr (std::is_same_v<T, int>) {
//             return v;
//         } else if constexpr (std::is_same_v<T, double>) {
//             return static_cast<int32_t>(v);
//         } else if constexpr (std::is_same_v<T, std::string>) {
//             return std::stoi(v); // fallback (slow) – avoid if possible
//         } else {
//             throw std::runtime_error("Unsupported type in get_int32");
//         }
//     }, val);
// }

PartitionUtils::PartitionResult PartitionUtils::partition_dataset(
    Dataset &dataset,
    const FileSchema &schema,
    const std::vector<std::string> &partition_columns
) {
    if (partition_columns.empty()) {
        std::unordered_map<std::vector<int32_t>, Dataset, PartitionUtils::VecHash, PartitionUtils::VecEq> result;
        result[{}] = std::move(dataset);
        return result;
    }

    // Precompute indices
    std::vector<size_t> col_indices;
    col_indices.reserve(partition_columns.size());
    for (const auto &col : partition_columns) {
        col_indices.push_back(schema.index_of(col));
    }

    // Specialized case: 1 column
    if (partition_columns.size() == 1) {
        std::unordered_map<int32_t, Dataset> partitions;
        size_t col_idx = col_indices[0];
        for (auto &row : dataset) {
            // int32_t key = get_int32(row[col_idx]);
            partitions[row[col_idx]].emplace_back(std::move(row));
        }
        dataset.clear();
        return partitions;
    }

    // Generic case: multiple columns
    std::unordered_map<std::vector<int32_t>, Dataset, PartitionUtils::VecHash, PartitionUtils::VecEq> partitions;
    std::vector<int32_t> key;
    key.reserve(col_indices.size());
    for (auto &row : dataset) {
        key.clear();
        for (size_t idx : col_indices) {
            key.push_back(row[idx]);
        }
        partitions[key].emplace_back(std::move(row));
    }
    dataset.clear();
    return partitions;
}

PartitionUtils::PartitionResult PartitionUtils::partition_input(
    Dataset &dataset,
    const FileSchema &schema,
    const std::vector<std::string> &partition_columns,
    const ProbeKeySet &probe_keys
) {
    if (partition_columns.empty()) {
        // No partitioning: keep dataset only if probe is not empty
        bool keep = std::visit([](auto &&keys) { return !keys.empty(); }, probe_keys);
        if (!keep) {
            dataset.clear();
            return std::unordered_map<std::vector<int32_t>, Dataset, VecHash, VecEq>{};
        }
        std::unordered_map<std::vector<int32_t>, Dataset, VecHash, VecEq> result;
        result[{}] = std::move(dataset);
        return result;
    }

    // Precompute col indices
    std::vector<size_t> col_indices;
    col_indices.reserve(partition_columns.size());
    for (const auto &col : partition_columns) {
        col_indices.push_back(schema.index_of(col));
    }

    // Single partition column
    if (partition_columns.size() == 1) {
        std::unordered_map<int32_t, Dataset> partitions;
        size_t col_idx = col_indices[0];
        const auto *keyset = std::get_if<std::unordered_set<int32_t>>(&probe_keys);
        if (!keyset) throw std::runtime_error("PartitionInput: key type mismatch (expected int32_t set)");
        for (auto &row : dataset) {
            int32_t key = row[col_idx];
            if (keyset->find(key) != keyset->end()) {
                partitions[key].emplace_back(std::move(row));
            }
        }
        dataset.clear();
        return partitions;
    }

    // Multi-column partition
    std::unordered_map<std::vector<int32_t>, Dataset, VecHash, VecEq> partitions;
    const auto *keyset = std::get_if<std::unordered_set<std::vector<int32_t>, VecHash, VecEq>>(&probe_keys);
    if (!keyset) throw std::runtime_error("PartitionInput: key type mismatch (expected vector<int32_t> set)");

    std::vector<int32_t> key;
    key.reserve(col_indices.size());
    for (auto &row : dataset) {
        key.clear();
        for (size_t idx : col_indices) {
            key.push_back(row[idx]);
        }
        if (keyset->find(key) != keyset->end()) {
            partitions[key].emplace_back(std::move(row));
        }
    }
    dataset.clear();
    return partitions;
}





PartitionUtils::PartitionResult PartitionUtils::partition_dataset_morsel(
    Dataset &dataset,
    const FileSchema &schema,
    const std::vector<std::string> &partition_columns,
    size_t num_threads,
    size_t morsel_size
) {
    if (partition_columns.empty()) {
        std::unordered_map<std::vector<int32_t>, Dataset, PartitionUtils::VecHash, PartitionUtils::VecEq> result;
        result[{}] = std::move(dataset);
        return result;
    }

    // Precompute indices
    std::vector<size_t> col_indices;
    col_indices.reserve(partition_columns.size());
    for (const auto &col : partition_columns) {
        col_indices.push_back(schema.index_of(col));
    }

    size_t n = dataset.size();
    std::atomic<size_t> next_morsel(0);

    if (partition_columns.size() == 1) {
        std::vector<std::unordered_map<int32_t, Dataset>> thread_partitions(num_threads);

        auto worker = [&](size_t tid) {
            auto &local = thread_partitions[tid];
            while (true) {
                size_t start = next_morsel.fetch_add(morsel_size);
                if (start >= n) break;
                size_t end = std::min(start + morsel_size, n);

                for (size_t i = start; i < end; ++i) {
                    local[dataset[i][col_indices[0]]].emplace_back(std::move(dataset[i]));
                }
            }
        };

        // Launch threads
        std::vector<std::thread> threads;
        for (size_t t = 0; t < num_threads; ++t) {
            threads.emplace_back(worker, t);
        }
        for (auto &th : threads) th.join();

        // Merge
        std::unordered_map<int32_t, Dataset> global;
        for (auto &tp : thread_partitions) {
            for (auto &kv : tp) {
                auto &vec = global[kv.first];
                std::move(kv.second.begin(), kv.second.end(), std::back_inserter(vec));
            }
        }
        dataset.clear();
        return global;

    } else {
        std::vector<std::unordered_map<std::vector<int32_t>, Dataset, PartitionUtils::VecHash, PartitionUtils::VecEq>> thread_partitions(num_threads);

        auto worker = [&](size_t tid) {
            auto &local = thread_partitions[tid];
            std::vector<int32_t> key;
            key.reserve(col_indices.size());

            while (true) {
                size_t start = next_morsel.fetch_add(morsel_size);
                if (start >= n) break;
                size_t end = std::min(start + morsel_size, n);

                for (size_t i = start; i < end; ++i) {
                    key.clear();
                    for (size_t idx : col_indices) {
                        key.push_back(dataset[i][idx]);
                    }
                    local[key].emplace_back(std::move(dataset[i]));
                }
            }
        };

        std::vector<std::thread> threads;
        for (size_t t = 0; t < num_threads; ++t) {
            threads.emplace_back(worker, t);
        }
        for (auto &th : threads) th.join();

        // Merge
        std::unordered_map<std::vector<int32_t>, Dataset, PartitionUtils::VecHash, PartitionUtils::VecEq> global;
        for (auto &tp : thread_partitions) {
            for (auto &kv : tp) {
                auto &vec = global[kv.first];
                std::move(kv.second.begin(), kv.second.end(), std::back_inserter(vec));
            }
        }
        dataset.clear();
        return global;
    }
}

PartitionUtils::PartitionResult PartitionUtils::partition_input_morsel(
    Dataset &input,
    const FileSchema &schema,
    const std::vector<std::string> &partition_columns,
    const PartitionResult &probe_partitions,
    size_t num_threads,
    size_t morsel_size
) {
    if (partition_columns.empty()) {
        std::unordered_map<std::vector<int32_t>, Dataset, VecHash, VecEq> result;
        result[{}] = std::move(input);
        return result;
    }

    // Precompute column indices
    std::vector<size_t> col_indices;
    col_indices.reserve(partition_columns.size());
    for (const auto &col : partition_columns) {
        col_indices.push_back(schema.index_of(col));
    }

    size_t n = input.size();
    std::atomic<size_t> next_morsel(0);

    // ---------------- SINGLE COLUMN CASE ----------------
    if (partition_columns.size() == 1) {
        size_t col_idx = col_indices[0];

        // Build probe keys set
        std::unordered_set<int32_t> probe_keys;
        std::visit([&](auto &probe_map) {
            probe_keys.reserve(probe_map.size());
            for (auto &kv : probe_map) {
                if constexpr (std::is_same_v<typename std::decay_t<decltype(probe_map)>::key_type, int32_t>) {
                    probe_keys.insert(kv.first);
                }
            }
        }, probe_partitions);

        // Thread-local partitions
        std::vector<std::unordered_map<int32_t, Dataset>> thread_partitions(num_threads);

        auto worker = [&](size_t tid) {
            auto &local = thread_partitions[tid];
            while (true) {
                size_t start = next_morsel.fetch_add(morsel_size);
                if (start >= n) break;
                size_t end = std::min(start + morsel_size, n);

                for (size_t i = start; i < end; ++i) {
                    int32_t key = input[i][col_idx];
                    if (probe_keys.contains(key)) {
                        local[key].emplace_back(std::move(input[i]));
                    }
                }
            }
        };

        // Launch threads
        std::vector<std::thread> threads;
        for (size_t t = 0; t < num_threads; ++t) threads.emplace_back(worker, t);
        for (auto &th : threads) th.join();

        // Merge results
        std::unordered_map<int32_t, Dataset> global;
        global.reserve(probe_keys.size()); // reserve based on probe
        for (auto &tp : thread_partitions) {
            for (auto &kv : tp) {
                auto &vec = global[kv.first];
                std::move(kv.second.begin(), kv.second.end(), std::back_inserter(vec));
            }
        }

        input.clear();
        return global;
    }

    // ---------------- MULTI-COLUMN CASE ----------------
    // Extract probe keys into a set for quick lookup
    std::unordered_set<std::vector<int32_t>, VecHash, VecEq> probe_keys;
    std::visit([&](auto &probe_map) {
        for (auto &kv : probe_map) {
            if constexpr (std::is_same_v<typename std::decay_t<decltype(probe_map)>::key_type, std::vector<int32_t>>) {
                probe_keys.insert(kv.first);
            }
        }
    }, probe_partitions);

    std::vector<std::unordered_map<std::vector<int32_t>, Dataset, VecHash, VecEq>> thread_partitions(num_threads);

    auto worker = [&](size_t tid) {
        auto &local = thread_partitions[tid];
        std::vector<int32_t> key;
        key.reserve(col_indices.size());

        while (true) {
            size_t start = next_morsel.fetch_add(morsel_size);
            if (start >= n) break;
            size_t end = std::min(start + morsel_size, n);

            for (size_t i = start; i < end; ++i) {
                key.clear();
                for (size_t idx : col_indices) {
                    key.push_back(input[i][idx]);
                }
                if (probe_keys.contains(key)) {
                    local[key].emplace_back(std::move(input[i]));
                }
            }
        }
    };

    std::vector<std::thread> threads;
    for (size_t t = 0; t < num_threads; ++t) threads.emplace_back(worker, t);
    for (auto &th : threads) th.join();

    // Merge results
    std::unordered_map<std::vector<int32_t>, Dataset, VecHash, VecEq> global;
    global.reserve(probe_keys.size());
    for (auto &tp : thread_partitions) {
        for (auto &kv : tp) {
            auto &vec = global[kv.first];
            std::move(kv.second.begin(), kv.second.end(), std::back_inserter(vec));
        }
    }

    input.clear();
    return global;
}

// partition_utils.cpp (new function)
PartitionUtils::PartitionIndexResult PartitionUtils::partition_dataset_index_morsel(
    const Dataset &dataset,
    const FileSchema &schema,
    const std::vector<std::string> &partition_columns,
    size_t num_threads,
    size_t morsel_size
) {
    if (partition_columns.empty()) {
        std::unordered_map<std::vector<int32_t>, IndexDataset, VecHash, VecEq> result;
        result[{}].reserve(dataset.size());
        for (size_t i = 0; i < dataset.size(); ++i) result[{}].push_back(i);
        return result;
    }

    // Precompute indices of partition columns
    std::vector<size_t> col_indices;
    col_indices.reserve(partition_columns.size());
    for (auto &c : partition_columns) col_indices.push_back(schema.index_of(c));

    size_t n = dataset.size();
    std::atomic<size_t> next_morsel(0);

    if (partition_columns.size() == 1) {
        std::vector<std::unordered_map<int32_t, IndexDataset>> thread_parts(num_threads);

        auto worker = [&](size_t tid) {
            auto &local = thread_parts[tid];
            while (true) {
                size_t start = next_morsel.fetch_add(morsel_size);
                if (start >= n) break;
                size_t end = std::min(start + morsel_size, n);
                for (size_t i = start; i < end; ++i) {
                    auto &vec = local[dataset[i][col_indices[0]]];
                    if (vec.empty()) vec.reserve(4); // small hint
                    vec.push_back(i);
                }
            }
        };

        std::vector<std::thread> threads;
        for (size_t t = 0; t < num_threads; ++t) threads.emplace_back(worker, t);
        for (auto &th : threads) th.join();

        // Merge threads into global
        std::unordered_map<int32_t, IndexDataset> global;
        global.reserve(num_threads * 4);
        for (auto &tp : thread_parts) {
            for (auto &kv : tp) {
                auto &dst = global[kv.first];
                if (dst.empty()) dst.reserve(kv.second.size());
                std::move(kv.second.begin(), kv.second.end(), std::back_inserter(dst));
            }
        }
        return global;
    } else {
        std::vector<std::unordered_map<std::vector<int32_t>, IndexDataset, VecHash, VecEq>> thread_parts(num_threads);

        auto worker = [&](size_t tid) {
            auto &local = thread_parts[tid];
            std::vector<int32_t> key;
            key.reserve(col_indices.size());
            while (true) {
                size_t start = next_morsel.fetch_add(morsel_size);
                if (start >= n) break;
                size_t end = std::min(start + morsel_size, n);
                for (size_t i = start; i < end; ++i) {
                    key.clear();
                    for (auto idx : col_indices) key.push_back(dataset[i][idx]);
                    auto &vec = local[key];
                    if (vec.empty()) vec.reserve(4);
                    vec.push_back(i);
                }
            }
        };

        std::vector<std::thread> threads;
        for (size_t t = 0; t < num_threads; ++t) threads.emplace_back(worker, t);
        for (auto &th : threads) th.join();

        std::unordered_map<std::vector<int32_t>, IndexDataset, VecHash, VecEq> global;
        global.reserve(num_threads * 4);
        for (auto &tp : thread_parts) {
            for (auto &kv : tp) {
                auto &dst = global[kv.first];
                if (dst.empty()) dst.reserve(kv.second.size());
                std::move(kv.second.begin(), kv.second.end(), std::back_inserter(dst));
            }
        }
        return global;
    }
}

PartitionUtils::RadixPartitionResult PartitionUtils::partition_dataset_radix(
    const Dataset &dataset,
    const FileSchema &schema,
    const std::vector<std::string> &partition_columns,
    size_t num_threads,
    size_t radix_bits // 256 buckets by default
) {
    if (partition_columns.empty()) {
        // Single partition case
        RadixPartitionResult result(1);
        result[0].reserve(dataset.size());
        for (size_t i = 0; i < dataset.size(); ++i) result[0].push_back(i);
        return result;
    }

    // Precompute column indices
    std::vector<size_t> col_indices;
    col_indices.reserve(partition_columns.size());
    for (auto &col : partition_columns) {
        col_indices.push_back(schema.index_of(col));
    }

    const size_t n = dataset.size();
    const size_t num_buckets = 1 << radix_bits;  // 2^radix_bits
    const size_t mask = num_buckets - 1;

    // Single column radix partitioning (most common case)
    if (partition_columns.size() == 1) {
        // Phase 1: Count bucket sizes
        std::vector<std::atomic<size_t>> bucket_counts(num_buckets);
        for (auto& count : bucket_counts) count.store(0);

        auto count_worker = [&](size_t start, size_t end) {
            for (size_t i = start; i < end; ++i) {
                size_t bucket = (dataset[i][col_indices[0]] & mask);
                bucket_counts[bucket].fetch_add(1, std::memory_order_relaxed);
            }
        };

        // Parallel counting
        std::vector<std::thread> count_threads;
        size_t chunk_size = (n + num_threads - 1) / num_threads;
        for (size_t t = 0; t < num_threads; ++t) {
            size_t start = t * chunk_size;
            size_t end = std::min(start + chunk_size, n);
            count_threads.emplace_back(count_worker, start, end);
        }
        for (auto& th : count_threads) th.join();

        // Preallocate buckets
        RadixPartitionResult buckets(num_buckets);
        for (size_t i = 0; i < num_buckets; ++i) {
            buckets[i].reserve(bucket_counts[i].load());
        }

        // Phase 2: Scatter indices to buckets
        std::vector<std::atomic<size_t>> bucket_positions(num_buckets);
        for (auto& pos : bucket_positions) pos.store(0);

        auto scatter_worker = [&](size_t start, size_t end) {
            for (size_t i = start; i < end; ++i) {
                size_t bucket = (dataset[i][col_indices[0]] & mask);
                size_t pos = bucket_positions[bucket].fetch_add(1, std::memory_order_relaxed);
                buckets[bucket][pos] = i;
            }
        };

        // Parallel scattering
        std::vector<std::thread> scatter_threads;
        for (size_t t = 0; t < num_threads; ++t) {
            size_t start = t * chunk_size;
            size_t end = std::min(start + chunk_size, n);
            scatter_threads.emplace_back(scatter_worker, start, end);
        }
        for (auto& th : scatter_threads) th.join();

        return buckets;

    } else {
        // Multi-column case: fall back to hash partitioning
        // (Radix partitioning is less effective for multiple columns)
        std::vector<std::unordered_map<std::vector<int32_t>, IndexDataset, VecHash, VecEq>> thread_parts(num_threads);
        std::atomic<size_t> next_morsel(0);
        const size_t morsel_size = 4096;  // 4KB morsels

        auto worker = [&](size_t tid) {
            auto &local = thread_parts[tid];
            std::vector<int32_t> key;
            key.reserve(col_indices.size());

            while (true) {
                size_t start = next_morsel.fetch_add(morsel_size);
                if (start >= n) break;
                size_t end = std::min(start + morsel_size, n);

                for (size_t i = start; i < end; ++i) {
                    key.clear();
                    for (auto idx : col_indices) {
                        key.push_back(dataset[i][idx]);
                    }
                    auto &vec = local[key];
                    if (vec.empty()) vec.reserve(4);
                    vec.push_back(i);
                }
            }
        };

        std::vector<std::thread> threads;
        for (size_t t = 0; t < num_threads; ++t) {
            threads.emplace_back(worker, t);
        }
        for (auto &th : threads) th.join();

        // Merge thread results
        std::unordered_map<std::vector<int32_t>, IndexDataset, VecHash, VecEq> global;
        for (auto &tp : thread_parts) {
            for (auto &kv : tp) {
                auto &dst = global[kv.first];
                if (dst.empty()) dst.reserve(kv.second.size());
                std::move(kv.second.begin(), kv.second.end(), std::back_inserter(dst));
            }
        }

        // Convert to radix format (vector of buckets)
        RadixPartitionResult result(1);
        result[0].reserve(n);
        for (auto& kv : global) {
            for (auto idx : kv.second) {
                result[0].push_back(idx);
            }
        }
        return result;
    }
}

// Advanced multi-pass radix partitioning
PartitionUtils::RadixPartitionResult PartitionUtils::partition_dataset_radix_advanced(
    const Dataset &dataset,
    const FileSchema &schema,
    const std::vector<std::string> &partition_columns,
    size_t num_threads,
    size_t radix_bits_per_pass  // 64 buckets per pass
) {
    if (partition_columns.empty() || partition_columns.size() > 1) {
        // Use simple radix for single column, fallback for multi-column
        return partition_dataset_radix(dataset, schema, partition_columns, num_threads);
    }

    const size_t n = dataset.size();
    const size_t num_passes = (31 + radix_bits_per_pass - 1) / radix_bits_per_pass;
    const size_t buckets_per_pass = 1 << radix_bits_per_pass;
    const size_t mask = buckets_per_pass - 1;

    // Current permutation (initially 0..n-1)
    std::vector<size_t> current_indices(n);
    std::iota(current_indices.begin(), current_indices.end(), 0);

    std::vector<size_t> next_indices(n);

    auto col_idx = schema.index_of(partition_columns[0]);

    for (size_t pass = 0; pass < num_passes; ++pass) {
        const size_t shift = pass * radix_bits_per_pass;

        // Count bucket sizes for this pass
        std::vector<size_t> bucket_counts(buckets_per_pass, 0);

        auto count_worker = [&](size_t start, size_t end) {
            for (size_t i = start; i < end; ++i) {
                size_t idx = current_indices[i];
                size_t bucket = (dataset[idx][col_idx] >> shift) & mask;
                bucket_counts[bucket]++;
            }
        };

        // Parallel counting
        std::vector<std::thread> count_threads;
        size_t chunk_size = (n + num_threads - 1) / num_threads;
        for (size_t t = 0; t < num_threads; ++t) {
            size_t start = t * chunk_size;
            size_t end = std::min(start + chunk_size, n);
            count_threads.emplace_back(count_worker, start, end);
        }
        for (auto& th : count_threads) th.join();

        // Compute prefix sum
        std::vector<size_t> bucket_offsets(buckets_per_pass + 1, 0);
        for (size_t i = 0; i < buckets_per_pass; ++i) {
            bucket_offsets[i + 1] = bucket_offsets[i] + bucket_counts[i];
        }

        // Scatter indices
        auto scatter_worker = [&](size_t start, size_t end) {
            std::vector<size_t> local_offsets = bucket_offsets;

            for (size_t i = start; i < end; ++i) {
                size_t idx = current_indices[i];
                size_t bucket = (dataset[idx][col_idx] >> shift) & mask;
                size_t pos = local_offsets[bucket]++;
                next_indices[pos] = idx;
            }
        };

        // Parallel scattering
        std::vector<std::thread> scatter_threads;
        for (size_t t = 0; t < num_threads; ++t) {
            size_t start = t * chunk_size;
            size_t end = std::min(start + chunk_size, n);
            scatter_threads.emplace_back(scatter_worker, start, end);
        }
        for (auto& th : scatter_threads) th.join();

        std::swap(current_indices, next_indices);
    }

    // Convert to bucket format
    RadixPartitionResult result(1);
    result[0] = std::move(current_indices);
    return result;
}

// PartitionUtils::RadixPartitionResult PartitionUtils::partition_dataset_radix_morsel(
//     const Dataset &dataset,
//     const FileSchema &schema,
//     const std::vector<std::string> &partition_columns,
//     size_t num_threads,
//     size_t radix_bits,
//     size_t morsel_size
// ) {
//     if (partition_columns.empty()) {
//         RadixPartitionResult result(1);
//         result[0].reserve(dataset.size());
//         for (size_t i = 0; i < dataset.size(); ++i) result[0].push_back(i);
//         return result;
//     }
//
//     if (partition_columns.size() > 1) {
//         return RadixPartitionResult{};
//     }
//
//     const size_t n = dataset.size();
//     const size_t num_buckets = 1 << radix_bits;
//     const size_t mask = num_buckets - 1;
//     const size_t col_idx = schema.index_of(partition_columns[0]);
//
//     // Phase 1: Counting (same as before)
//     std::vector<std::vector<size_t>> thread_local_counts(num_threads);
//     for (auto& counts : thread_local_counts) {
//         counts.resize(num_buckets, 0);
//     }
//
//     std::atomic<size_t> next_morsel(0);
//
//     auto count_worker = [&](size_t tid) {
//         auto& local_counts = thread_local_counts[tid];
//         while (true) {
//             size_t start = next_morsel.fetch_add(morsel_size);
//             if (start >= n) break;
//             size_t end = std::min(start + morsel_size, n);
//             for (size_t i = start; i < end; ++i) {
//                 size_t bucket = (dataset[i][col_idx] & mask);
//                 local_counts[bucket]++;
//             }
//         }
//     };
//
//     std::vector<std::thread> count_threads;
//     for (size_t t = 0; t < num_threads; ++t) {
//         count_threads.emplace_back(count_worker, t);
//     }
//     for (auto& th : count_threads) th.join();
//
//     // Aggregate counts
//     std::vector<size_t> global_counts(num_buckets, 0);
//     for (auto& local_counts : thread_local_counts) {
//         for (size_t b = 0; b < num_buckets; ++b) {
//             global_counts[b] += local_counts[b];
//         }
//     }
//
//     // Preallocate buckets
//     RadixPartitionResult buckets(num_buckets);
//     for (size_t b = 0; b < num_buckets; ++b) {
//         buckets[b].resize(global_counts[b]);
//     }
//
//     // Phase 2: Scattering with ATOMIC offsets
//     std::vector<std::atomic<size_t>> bucket_offsets(num_buckets);
//
//     // Initialize atomic offsets with prefix sum
//     size_t current_offset = 0;
//     for (size_t b = 0; b < num_buckets; ++b) {
//         bucket_offsets[b].store(current_offset);
//         current_offset += global_counts[b];
//     }
//
//     next_morsel.store(0);  // Reset for scattering phase
//
//     auto scatter_worker = [&]() {
//         while (true) {
//             size_t start = next_morsel.fetch_add(morsel_size);
//             if (start >= n) break;
//             size_t end = std::min(start + morsel_size, n);
//
//             for (size_t i = start; i < end; ++i) {
//                 size_t bucket = (dataset[i][col_idx] & mask);
//                 size_t pos = bucket_offsets[bucket].fetch_add(1);
//
//                 // Add bounds check for safety
//                 if (pos < buckets[bucket].size()) {
//                     buckets[bucket][pos] = i;
//                 } else {
//                     // This should never happen if counting was correct
//                     std::cout << "Error: Bucket " << bucket << " overflow! "
//                               << "pos=" << pos << ", size=" << buckets[bucket].size()
//                               << std::endl;
//                 }
//             }
//         }
//     };
//
//     std::vector<std::thread> scatter_threads;
//     for (size_t t = 0; t < num_threads; ++t) {
//         scatter_threads.emplace_back(scatter_worker);
//     }
//     for (auto& th : scatter_threads) th.join();
//
//     return buckets;
// }

PartitionUtils::RadixPartitionResult PartitionUtils::partition_dataset_radix_morsel(
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

    const size_t n = dataset.size();
    const size_t num_buckets = 1 << radix_bits;
    const size_t mask = num_buckets - 1;
    const size_t col_idx = schema.index_of(partition_columns[0]);

    // ---------------------------
    // PASS 1: count per thread
    // ---------------------------
    std::vector<std::vector<size_t>> thread_bucket_counts(num_threads, std::vector<size_t>(num_buckets, 0));
    std::atomic<size_t> next_morsel(0);

    auto count_worker = [&](size_t tid) {
        while (true) {
            size_t start = next_morsel.fetch_add(morsel_size);
            if (start >= n) break;
            size_t end = std::min(start + morsel_size, n);

            for (size_t i = start; i < end; ++i) {
                int32_t value = dataset[i][col_idx];
                size_t bucket = (value & mask);
                thread_bucket_counts[tid][bucket]++;
            }
        }
    };

    {
        std::vector<std::thread> threads;
        for (size_t t = 0; t < num_threads; ++t)
            threads.emplace_back(count_worker, t);
        for (auto& th : threads) th.join();
    }

    // ---------------------------
    // Prefix sums (compute offsets)
    // ---------------------------
    std::vector<size_t> bucket_totals(num_buckets, 0);
    for (size_t b = 0; b < num_buckets; ++b) {
        for (size_t t = 0; t < num_threads; ++t)
            bucket_totals[b] += thread_bucket_counts[t][b];
    }

    RadixPartitionResult buckets(num_buckets);
    for (size_t b = 0; b < num_buckets; ++b)
        buckets[b].resize(bucket_totals[b]);

    // Offsets: for each thread, where does its slice for bucket b start?
    std::vector<std::vector<size_t>> thread_bucket_offsets(num_threads, std::vector<size_t>(num_buckets, 0));
    for (size_t b = 0; b < num_buckets; ++b) {
        size_t offset = 0;
        for (size_t t = 0; t < num_threads; ++t) {
            thread_bucket_offsets[t][b] = offset;
            offset += thread_bucket_counts[t][b];
        }
    }

    // ---------------------------
    // PASS 2: scatter into buckets
    // ---------------------------
    next_morsel.store(0);

    auto scatter_worker = [&](size_t tid) {
        // local write cursors per bucket
        std::vector<size_t> local_offsets = thread_bucket_offsets[tid];

        while (true) {
            size_t start = next_morsel.fetch_add(morsel_size);
            if (start >= n) break;
            size_t end = std::min(start + morsel_size, n);

            for (size_t i = start; i < end; ++i) {
                int32_t value = dataset[i][col_idx];
                size_t bucket = (value & mask);
                buckets[bucket][local_offsets[bucket]++] = i;
            }
        }
    };

    {
        std::vector<std::thread> threads;
        for (size_t t = 0; t < num_threads; ++t)
            threads.emplace_back(scatter_worker, t);
        for (auto& th : threads) th.join();
    }

    return buckets;
}



