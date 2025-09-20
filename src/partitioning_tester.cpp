#include "data_io.h"
#include <vector>
#include <iostream>
#include "data_processing.h"
#include <chrono>
#include <thread>
#include <mutex>
#include <unordered_map>
#include <string>

using namespace std;

#include <cstddef>
#include <cstdint>
#include <omp.h>

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
) {
    if (partition_columns.empty()) {
        std::unordered_map<std::vector<int32_t>, Dataset, VecHash, VecEq> result;
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
    std::unordered_map<std::vector<int32_t>, Dataset, VecHash, VecEq> partitions;
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


// Parallel partitioning
PartitionResult partition_dataset_parallel(
    Dataset &dataset,
    const FileSchema &schema,
    const std::vector<std::string> &partition_columns,
    size_t num_threads = std::thread::hardware_concurrency()
) {
    if (partition_columns.empty()) {
        std::unordered_map<std::vector<int32_t>, Dataset, VecHash, VecEq> result;
        result[{}] = std::move(dataset);
        return result;
    }

    // Precompute column indices
    std::vector<size_t> col_indices;
    col_indices.reserve(partition_columns.size());
    for (const auto &col : partition_columns) {
        col_indices.push_back(schema.index_of(col));
    }

    size_t n = dataset.size();
    size_t chunk_size = (n + num_threads - 1) / num_threads;

    if (partition_columns.size() == 1) {
        // Thread-local maps for int32 keys
        std::vector<std::unordered_map<int32_t, Dataset>> thread_partitions(num_threads);

        #pragma omp parallel num_threads(num_threads)
        {
            int tid = omp_get_thread_num();
            auto &local = thread_partitions[tid];
            size_t start = tid * chunk_size;
            size_t end = std::min(start + chunk_size, n);

            for (size_t i = start; i < end; ++i) {
                int32_t key = get_int32(dataset[i][col_indices[0]]);
                local[key].emplace_back(std::move(dataset[i]));
            }
        }

        // Merge results
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
        // Thread-local maps for vector<int32_t> keys
        std::vector<std::unordered_map<std::vector<int32_t>, Dataset, VecHash, VecEq>> thread_partitions(num_threads);

        #pragma omp parallel num_threads(num_threads)
        {
            int tid = omp_get_thread_num();
            auto &local = thread_partitions[tid];
            size_t start = tid * chunk_size;
            size_t end = std::min(start + chunk_size, n);

            std::vector<int32_t> key;
            key.reserve(col_indices.size());

            for (size_t i = start; i < end; ++i) {
                key.clear();
                for (size_t idx : col_indices) {
                    key.push_back(get_int32(dataset[i][idx]));
                }
                local[key].emplace_back(std::move(dataset[i]));
            }
        }

        // Merge results
        std::unordered_map<std::vector<int32_t>, Dataset, VecHash, VecEq> global;
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


bool compare_datasets(const Dataset& d1, const Dataset& d2) {
    if (d1.size() != d2.size()) {
        std::cout << "Datasets have different sizes." << std::endl;
        return false;
    }

    // Create copies to sort. Sorting by row to enable a consistent comparison.
    // Assuming Row has a way to be compared, e.g., a custom operator< or a way to convert to a sortable type.
    // For now, let's assume Row can be sorted, e.g., it is a vector of simple types.
    Dataset sorted_d1 = d1;
    Dataset sorted_d2 = d2;
    std::sort(sorted_d1.begin(), sorted_d1.end());
    std::sort(sorted_d2.begin(), sorted_d2.end());

    if (sorted_d1 != sorted_d2) {
        std::cout << "Datasets contain different rows." << std::endl;
        return false;
    }

    return true;
}

bool compare_partition_outputs(
    std::unordered_map<std::vector<int32_t>, Dataset, VecHash, VecEq>& p1,
    std::unordered_map<std::vector<int32_t>, Dataset, VecHash, VecEq>& p2)
{
    if (p1.size() != p2.size()) {
        std::cout << "Number of partitions is different." << std::endl;
        return false;
    }

    for (const auto& pair : p1) {
        const auto& key = pair.first;
        const auto& dataset1 = pair.second;

        // Check if the key exists in the second map
        if (p2.find(key) == p2.end()) {
            std::cout << "Key not found in second map." << std::endl;
            return false;
        }

        const auto& dataset2 = p2.at(key);

        // Compare the datasets for this key
        if (!compare_datasets(dataset1, dataset2)) {
            std::cout << "Datasets for key differ: ";
            for (int32_t val : key) {
                std::cout << val << " ";
            }
            std::cout << std::endl;
            return false;
        }
    }

    return true;
}





// ThIS COULD BE USED FOR DISPATCHING IF NEEDED AND COULD BE FASTER BY 15 MILLISECONDS -> to be checked for 10 millions
// // Generic version for multiple columns
// std::unordered_map<std::vector<int32_t>, Dataset, VecHash, VecEq>
// partition_dataset_impl(
//     Dataset &dataset,
//     const std::vector<size_t>& col_indices
// ) {
//     std::unordered_map<std::vector<int32_t>, Dataset, VecHash, VecEq> partitions;
//
//     // Reuse key vector
//     std::vector<int32_t> key;
//     key.reserve(col_indices.size());
//
//     for (auto &row: dataset) {
//         key.clear();
//         for (size_t idx: col_indices) {
//             key.push_back(get_int32(row[idx]));
//         }
//         partitions[key].emplace_back(std::move(row));
//     }
//
//     return partitions;
// }
//
// // Specialized version for single column
// std::unordered_map<int32_t, Dataset>
// partition_dataset_single_impl(
//     Dataset &dataset,
//     size_t col_index
// ) {
//     std::unordered_map<int32_t, Dataset> partitions;
//
//     for (auto &row: dataset) {
//         int32_t key = get_int32(row[col_index]);
//         partitions[key].emplace_back(std::move(row));
//     }
//
//     return partitions;
// }
//
// // Main dispatch function
// using PartitionResult = std::variant<
//     std::unordered_map<int32_t, Dataset>,
//     std::unordered_map<std::vector<int32_t>, Dataset, VecHash, VecEq>
// >;
//
// PartitionResult partition_dataset_main(
//     Dataset &dataset,
//     const FileSchema &schema,
//     const std::vector<std::string> &partition_columns
// ) {
//     if (partition_columns.empty()) {
//         std::unordered_map<std::vector<int32_t>, Dataset, VecHash, VecEq> result;
//         result[{}] = std::move(dataset);
//         return result;
//     }
//
//     // Precompute column indices
//     std::vector<size_t> col_indices;
//     col_indices.reserve(partition_columns.size());
//     for (const auto &col: partition_columns) {
//         col_indices.push_back(schema.index_of(col));
//     }
//
//     // Dispatch to specialized implementation
//     if (partition_columns.size() == 1) {
//         return partition_dataset_single_impl(dataset, col_indices[0]);
//     } else {
//         return partition_dataset_impl(dataset, col_indices);
//     }
// }

#include <atomic>

PartitionResult partition_dataset_morsel(
    Dataset &dataset,
    const FileSchema &schema,
    const std::vector<std::string> &partition_columns,
    size_t num_threads = std::thread::hardware_concurrency(),
    size_t morsel_size = 4096
) {
    if (partition_columns.empty()) {
        std::unordered_map<std::vector<int32_t>, Dataset, VecHash, VecEq> result;
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
        std::unordered_map<std::vector<int32_t>, Dataset, VecHash, VecEq> global;
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


#include <variant>



// Thread-local integer key partition map
using IntPartitionMap = std::unordered_map<int32_t, Dataset>;
// Thread-local multi-column key partition map
using VecPartitionMap = std::unordered_map<std::vector<int32_t>, Dataset, VecHash, VecEq>;

using PartitionResult2 = std::variant<
    std::vector<IntPartitionMap>,
    std::vector<VecPartitionMap>
>;

// Unified morsel-driven parallel partitioning
PartitionResult2 partition_dataset_parallel_mergefree(
    Dataset &dataset,
    const FileSchema &schema,
    const std::vector<std::string> &partition_columns,
    size_t num_threads = std::thread::hardware_concurrency(),
    size_t morsel_size = 2048
) {
    size_t n = dataset.size();
    if (partition_columns.empty()) {
        VecPartitionMap result;
        result[{}] = std::move(dataset);
        return std::vector<VecPartitionMap>{result};
    }

    // Precompute column indices
    std::vector<size_t> col_indices;
    col_indices.reserve(partition_columns.size());
    for (const auto &col : partition_columns) {
        col_indices.push_back(schema.index_of(col));
    }

    std::atomic<size_t> next_morsel(0);

    if (partition_columns.size() == 1) {
        // Thread-local storage
        std::vector<IntPartitionMap> thread_partitions(num_threads);

        auto worker = [&](size_t tid) {
            auto &local = thread_partitions[tid];
            size_t col_idx = col_indices[0];

            while (true) {
                size_t start = next_morsel.fetch_add(morsel_size);
                if (start >= n) break;
                size_t end = std::min(start + morsel_size, n);

                // Pre-reserve space for vectors if possible
                for (size_t i = start; i < end; ++i) {
                    int32_t key = get_int32(dataset[i][col_idx]);
                    auto &vec = local[key];
                    vec.reserve(vec.size() + 1); // optional, helps reduce reallocations
                    vec.emplace_back(std::move(dataset[i]));
                }
            }
        };

        // Launch threads
        std::vector<std::thread> threads;
        for (size_t t = 0; t < num_threads; ++t)
            threads.emplace_back(worker, t);
        for (auto &th : threads) th.join();

        dataset.clear();
        return thread_partitions; // return thread-local maps directly
    } else {
        // Multi-column keys
        std::vector<VecPartitionMap> thread_partitions(num_threads);

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
                    for (size_t idx : col_indices)
                        key.push_back(get_int32(dataset[i][idx]));
                    auto &vec = local[key];
                    vec.reserve(vec.size() + 1);
                    vec.emplace_back(std::move(dataset[i]));
                }
            }
        };

        std::vector<std::thread> threads;
        for (size_t t = 0; t < num_threads; ++t)
            threads.emplace_back(worker, t);
        for (auto &th : threads) th.join();

        dataset.clear();
        return thread_partitions; // return thread-local maps directly
    }
}


int main() {
    cout << "START PROCESSING:" << endl;

    auto [input, input_schema] = read_csv_optimized("official_duckdb_test/input3.csv");

    // Create a vector with one element called "category"
    std::vector<std::string> partition_columns = {"category"};

    // Time the partitioning
    auto startSecondPartitioning = std::chrono::high_resolution_clock::now();

    auto input_partitions_sequential2 = partition_dataset(input, input_schema, partition_columns);

    auto endSecondPartitioning = std::chrono::high_resolution_clock::now();
    auto durationSecondPartitioning = std::chrono::duration_cast<std::chrono::milliseconds>(
        endSecondPartitioning - startSecondPartitioning);
    std::cout << "Time taken to partition the input sequentially: " << durationSecondPartitioning.count() <<
            " milliseconds" << std::endl;

    auto [input2, input_schema2] = read_csv_optimized("official_duckdb_test/input3.csv");

    // Time the parallel partitioning
    auto startParallelPartitioning = std::chrono::high_resolution_clock::now();

    auto input_partitions_parallel = partition_dataset_parallel(input2, input_schema2, partition_columns);

    auto endParallelPartitioning = std::chrono::high_resolution_clock::now();
    auto durationParallelPartitioning = std::chrono::duration_cast<std::chrono::milliseconds>(
        endParallelPartitioning - startParallelPartitioning);
    std::cout << "Time taken to partition the input in parallel: " << durationParallelPartitioning.count() <<
            " milliseconds" << std::endl;


    auto [input3, input_schema3] = read_csv_optimized("official_duckdb_test/input3.csv");

    // Time the parallel partitioning
    auto startParallelPartitioningMorsel = std::chrono::high_resolution_clock::now();

    auto input_partitions_parallel_morsel = partition_dataset_parallel(input3, input_schema3, partition_columns);

    auto endParallelPartitioningMorsel = std::chrono::high_resolution_clock::now();
    auto durationParallelPartitioningMorsel = std::chrono::duration_cast<std::chrono::milliseconds>(
        endParallelPartitioningMorsel - startParallelPartitioningMorsel);
    std::cout << "Time taken to partition the input in parallel morsel: " << durationParallelPartitioningMorsel.count() <<
            " milliseconds" << std::endl;


    auto [input4, input_schema4] = read_csv_optimized("official_duckdb_test/input3.csv");

    // Time the parallel partitioning
    auto startParallelPartitioningMorselMergeFree = std::chrono::high_resolution_clock::now();

    auto input_partitions_parallel_morselMergeFree = partition_dataset_parallel_mergefree(input4, input_schema4, partition_columns);

    auto endParallelPartitioningMorselMergeFree = std::chrono::high_resolution_clock::now();
    auto durationParallelPartitioningMorselMergeFree = std::chrono::duration_cast<std::chrono::milliseconds>(
        endParallelPartitioningMorselMergeFree - startParallelPartitioningMorselMergeFree);
    std::cout << "Time taken to partition the input in parallel morsel: " << durationParallelPartitioningMorselMergeFree.count() <<
            " milliseconds" << std::endl;


    // Compare the outputs
    // std::cout << "\nStarting comparison..." << std::endl;
    // if (compare_partition_outputs(input_partitions_sequential2, input_partitions_parallel)) {
    //     std::cout << "The two partitioning functions returned the same data. Test passed! ✅" << std::endl;
    // } else {
    //     std::cout << "The two partitioning functions returned different data. Test failed! ❌" << std::endl;
    // }

    return 0;
}
