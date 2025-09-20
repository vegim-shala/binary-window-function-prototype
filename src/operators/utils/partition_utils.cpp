//
// Created by Vegim Shala on 11.7.25.
//

#include "operators/utils/partition_utils.h"
#include <thread>
#include <vector>
#include <unordered_map>
#include <string>
#include <atomic>
#include <unordered_set>

// Safe integer extractor
inline int32_t get_int32(const ColumnValue &val) {
    return std::visit([](auto &&v) -> int32_t {
        using T = std::decay_t<decltype(v)>;
        if constexpr (std::is_same_v<T, int>) {
            return v;
        } else if constexpr (std::is_same_v<T, double>) {
            return static_cast<int32_t>(v);
        } else if constexpr (std::is_same_v<T, std::string>) {
            return std::stoi(v); // fallback (slow) – avoid if possible
        } else {
            throw std::runtime_error("Unsupported type in get_int32");
        }
    }, val);
}

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
            int32_t key = get_int32(row[col_idx]);
            partitions[key].emplace_back(std::move(row));
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
            key.push_back(get_int32(row[idx]));
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
            int32_t key = get_int32(row[col_idx]);
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
            key.push_back(get_int32(row[idx]));
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
                    int32_t key = get_int32(dataset[i][col_indices[0]]);
                    local[key].emplace_back(std::move(dataset[i]));
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
                        key.push_back(get_int32(dataset[i][idx]));
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
                    int32_t key = get_int32(input[i][col_idx]);
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
                    key.push_back(get_int32(input[i][idx]));
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

