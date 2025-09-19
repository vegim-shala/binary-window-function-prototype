//
// Created by Vegim Shala on 11.7.25.
//

#include "operators/utils/partition_utils.h"
#include <thread>
#include <mutex>
#include <vector>
#include <unordered_map>
#include <string>
#include <atomic>

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



