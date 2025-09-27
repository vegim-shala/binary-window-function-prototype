#include "data_io.h"
#include <vector>
#include <iostream>
#include <chrono>
#include <thread>
#include <string>
#include <operators/utils/partition_utils.h>

using namespace PartitionUtils;
#include <algorithm>

bool compare_partitions(const PartitionIndexResult &a,
                        const PartitionIndexResult &b,
                        bool ignore_order = true) {
    // Case 1: int32_t key
    if (std::holds_alternative<std::unordered_map<int32_t, IndexDataset>>(a) &&
        std::holds_alternative<std::unordered_map<int32_t, IndexDataset>>(b)) {

        const auto &ma = std::get<std::unordered_map<int32_t, IndexDataset>>(a);
        const auto &mb = std::get<std::unordered_map<int32_t, IndexDataset>>(b);

        if (ma.size() != mb.size()) return false;

        for (const auto &kv : ma) {
            auto it = mb.find(kv.first);
            if (it == mb.end()) return false;

            const auto &va = kv.second;
            const auto &vb = it->second;
            if (va.size() != vb.size()) return false;

            if (ignore_order) {
                if (!std::is_permutation(va.begin(), va.end(), vb.begin()))
                    return false;
            } else {
                if (va != vb) return false;
            }
        }
        return true;
    }

    // Case 2: vector<int32_t> key
    if (std::holds_alternative<std::unordered_map<std::vector<int32_t>, IndexDataset, VecHash, VecEq>>(a) &&
        std::holds_alternative<std::unordered_map<std::vector<int32_t>, IndexDataset, VecHash, VecEq>>(b)) {

        const auto &ma = std::get<std::unordered_map<std::vector<int32_t>, IndexDataset, VecHash, VecEq>>(a);
        const auto &mb = std::get<std::unordered_map<std::vector<int32_t>, IndexDataset, VecHash, VecEq>>(b);

        if (ma.size() != mb.size()) return false;

        for (const auto &kv : ma) {
            auto it = mb.find(kv.first);
            if (it == mb.end()) return false;

            const auto &va = kv.second;
            const auto &vb = it->second;
            if (va.size() != vb.size()) return false;

            if (ignore_order) {
                if (!std::is_permutation(va.begin(), va.end(), vb.begin()))
                    return false;
            } else {
                if (va != vb) return false;
            }
        }
        return true;
    }

    // If types differ (e.g. one is int32 map, other is vector<int32> map)
    return false;
}

int main() {
    // cout << "START PROCESSING:" << endl;
    //
    auto [input, input_schema] = read_csv_optimized("official_duckdb_test/input1.csv");
    //
    // // Create a vector with one element called "category"
    std::vector<std::string> partition_columns = {"category"};
    //
    // // Time the partitioning
    auto startSecondPartitioning = std::chrono::high_resolution_clock::now();

    auto input_partitions_sequential2 = PartitionUtils::partition_dataset_index(input, input_schema, partition_columns);

    auto endSecondPartitioning = std::chrono::high_resolution_clock::now();
    auto durationSecondPartitioning = std::chrono::duration_cast<std::chrono::milliseconds>(
        endSecondPartitioning - startSecondPartitioning);
    std::cout << "Time taken to partition the input sequentially: " << durationSecondPartitioning.count() <<
            " milliseconds" << std::endl;

    auto [input2, input_schema2] = read_csv_optimized("official_duckdb_test/input1.csv");
    auto s = radix_setup(input2, input_schema2, partition_columns, 8);

    // Time the parallel partitioning
    auto startParallelPartitioning = std::chrono::high_resolution_clock::now();

    auto input_partitions_parallel = PartitionUtils::partition_indices_sequential(
        input, input_schema, partition_columns
    );

    auto endParallelPartitioning = std::chrono::high_resolution_clock::now();
    auto durationParallelPartitioning = std::chrono::duration_cast<std::chrono::milliseconds>(
        endParallelPartitioning - startParallelPartitioning);
    std::cout << "Time taken to partition the input in sequential radix: " << durationParallelPartitioning.count() <<
            " milliseconds" << std::endl;
    //
    // auto [input3, input_schema3] = read_csv_optimized("official_duckdb_test/input1.csv");
    //
    // // Time the parallel partitioning
    // auto startParallelPartitioningIndexMorsel = std::chrono::high_resolution_clock::now();
    //
    // auto input_partitions_parallel_index_morsel = PartitionUtils::partition_dataset_index_morsel(input3, input_schema3, partition_columns);
    //
    // auto endParallelPartitioningIndexMorsel = std::chrono::high_resolution_clock::now();
    // auto durationParallelPartitioningIndexMorsel = std::chrono::duration_cast<std::chrono::milliseconds>(
    //     endParallelPartitioningIndexMorsel - startParallelPartitioningIndexMorsel);
    // std::cout << "Time taken to partition the input in parallel morsel: " << durationParallelPartitioningIndexMorsel.count() <<
    //         " milliseconds" << std::endl;
    //
    // auto [input4, input_schema4] = read_csv_optimized("official_duckdb_test/input1.csv");
    //
    // // Time the parallel partitioning
    // auto startParallelPartitioningMorsel = std::chrono::high_resolution_clock::now();
    //
    // auto input_partitions_parallel_morsel = PartitionUtils::partition_dataset_radix_morsel(input4, input_schema4, partition_columns, 8, 8, 2048);
    //
    // auto endParallelPartitioningMorsel = std::chrono::high_resolution_clock::now();
    // auto durationParallelPartitioningMorsel = std::chrono::duration_cast<std::chrono::milliseconds>(
    //     endParallelPartitioningMorsel - startParallelPartitioningMorsel);
    // std::cout << "Time taken to partition the input in parallel radix morsel: " << durationParallelPartitioningMorsel.count() <<
    //         " milliseconds" << std::endl;
    //
    //
    // auto [input5, input_schema5] = read_csv_optimized("official_duckdb_test/input1.csv");

    // Time the parallel partitioning
    // auto startParallelPartitioningRadixMorsel = std::chrono::high_resolution_clock::now();
    //
    // auto input_partitions_parallel_radix_morsel = PartitionUtils::partition_dataset_hash_radix_sequential_multi(input5, input_schema5, partition_columns, 8);
    //
    // auto endParallelPartitioningRadixMorsel = std::chrono::high_resolution_clock::now();
    // auto durationParallelPartitioningRadixMorsel = std::chrono::duration_cast<std::chrono::milliseconds>(
    //     endParallelPartitioningRadixMorsel - startParallelPartitioningRadixMorsel);
    // std::cout << "Time taken to partition the input in radix multiple : " << durationParallelPartitioningRadixMorsel.count() <<
    //         " milliseconds" << std::endl;



    // Compare the outputs
    std::cout << "\nStarting comparison..." << std::endl;
    if (compare_partitions(input_partitions_sequential2, input_partitions_parallel)) {
        std::cout << "The two partitioning functions returned the same data. Test passed! ✅" << std::endl;
    } else {
        std::cout << "The two partitioning functions returned different data. Test failed! ❌" << std::endl;
    }

    return 0;
}
