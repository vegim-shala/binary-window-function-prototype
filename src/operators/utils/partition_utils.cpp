//
// Created by Vegim Shala on 11.7.25.
//

#include "operators/utils/partition_utils.h"
#include <thread>
#include <mutex>
#include <vector>
#include <unordered_map>
#include <string>

std::unordered_map<std::string, Dataset> PartitionUtils::partition_dataset(
    const Dataset& dataset,
    const std::vector<std::string>& partition_columns
) {
    std::unordered_map<std::string, Dataset> partitions;

    if (partition_columns.empty()) {
        partitions.insert({"__FULL_DATASET__", dataset});
        return partitions;
    }

    for (const auto& row : dataset) {
        std::string key;
        for (const auto& col : partition_columns) {
            std::visit([&](const auto& value) {
                std::stringstream ss;
                ss << value;
                key += ss.str() + "|";
            }, row.at(col));
        }
        partitions[key].push_back(row);
    }

    return partitions;
}


std::unordered_map<std::string, Dataset> PartitionUtils::partition_dataset_parallel(
    const Dataset& dataset,
    const std::vector<std::string>& partition_columns,
    size_t num_threads
) {
    if (partition_columns.empty()) {
        return {{"__FULL_DATASET__", dataset}};
    }

    // We shard the partitions map into num_threads buckets
    std::vector<std::unordered_map<std::string, Dataset>> shards(num_threads);
    std::vector<std::mutex> shard_locks(num_threads);

    size_t n = dataset.size();
    size_t chunk_size = (n + num_threads - 1) / num_threads;

    auto worker = [&](size_t tid) {
        size_t start = tid * chunk_size;
        size_t end = std::min(start + chunk_size, n);

        for (size_t i = start; i < end; i++) {
            const auto& row = dataset[i];

            // Build partition key
            std::string key;
            for (const auto& col : partition_columns) {
                std::visit([&](const auto& value) {
                    std::stringstream ss;
                    ss << value;
                    key += ss.str() + "|";
                }, row.at(col));
            }

            // Choose shard (hash of key % num_threads)
            size_t shard_id = std::hash<std::string>{}(key) % num_threads;

            {
                std::lock_guard<std::mutex> lock(shard_locks[shard_id]);
                shards[shard_id][key].push_back(row);
            }
        }
    };

    std::vector<std::thread> threads;
    for (size_t t = 0; t < num_threads; t++) {
        threads.emplace_back(worker, t);
    }
    for (auto& th : threads) th.join();

    // Merge shards into final result
    std::unordered_map<std::string, Dataset> partitions;
    for (auto& shard : shards) {
        for (auto& [key, rows] : shard) {
            auto& dest = partitions[key];
            dest.insert(dest.end(), rows.begin(), rows.end());
        }
    }

    return partitions;
}



