//
// Created by Vegim Shala on 14.7.25.
//
#include "operators/binary_window_function_operator.h"

#include <iostream>

#include "operators/utils/partition_utils.h"
#include "operators/utils/sort_utils.h"
#include "operators/utils/thread_pool.h"
#include <future>

using namespace std;

//Dataset BinaryWindowFunctionOperator::probe_parallel(
//    const Dataset &input_partition,
//    const Dataset &probe_partition,
//    const FileSchema &schema,
//    size_t num_threads)
//{
//    size_t n = probe_partition.size();
//    if (n == 0) return {};
//
//    if (n < 10000 || num_threads <= 1) {
//        // Small case: fall back to sequential
//        Dataset result;
//        result.reserve(n);
//        for (size_t i = 0; i < n; i++) {
//            double agg_sum = join_utils.compute_sum_range(
//                input_partition, probe_partition[i],
//                spec.join_spec.begin_column,
//                spec.join_spec.end_column);
//
//            DataRow output_row = probe_partition[i];
//            output_row[spec.output_column] = agg_sum;
//            result.push_back(std::move(output_row));
//        }
//        return result;
//    }
//
//    size_t chunk_size = (n + num_threads - 1) / num_threads;
//    std::vector<std::future<Dataset>> futures;
//
//    for (size_t t = 0; t < num_threads; t++) {
//        size_t start = t * chunk_size;
//        size_t end   = std::min(start + chunk_size, n);
//        if (start >= end) break;
//
//        futures.push_back(std::async(std::launch::async, [&, start, end]() {
//            Dataset local_result;
//            local_result.reserve(end - start);
//
//            for (size_t i = start; i < end; i++) {
//                double agg_sum = join_utils.compute_sum_range(
//                    input_partition, probe_partition[i],
//                    spec.join_spec.begin_column,
//                    spec.join_spec.end_column);
//
//                DataRow output_row = probe_partition[i];
//                output_row[spec.output_column] = agg_sum;
//                local_result.push_back(std::move(output_row));
//            }
//            return local_result;
//        }));
//    }
//
//    // Merge results in order
//    Dataset result;
//    result.reserve(n);
//    for (auto &f : futures) {
//        auto chunk = f.get();
//        result.insert(result.end(), std::make_move_iterator(chunk.begin()),
//                                   std::make_move_iterator(chunk.end()));
//    }
//
//    return result;
//}

pair<Dataset, FileSchema> BinaryWindowFunctionOperator::execute(Dataset& input, Dataset& probe, FileSchema input_schema, FileSchema probe_schema) {
    Dataset result;

    auto start = std::chrono::high_resolution_clock::now();

    // Partition input and probe
    auto input_partitions = PartitionUtils::partition_dataset_morsel(input, input_schema, spec.partition_columns);
    auto probe_partitions = PartitionUtils::partition_dataset_morsel(probe, probe_schema, spec.partition_columns);

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    std::cout << "Time taken for PARTITIONING: " << duration.count() << " ms" << std::endl;


    std::visit([&](auto& probe_map) {
    using MapType = std::decay_t<decltype(probe_map)>;

    for (auto& [partition_key, probe_partition] : probe_map) {
        // Get corresponding input partition
        if (!std::holds_alternative<MapType>(input_partitions)) {
            continue; // types don't match
        }
        auto& input_map = std::get<MapType>(input_partitions);

        auto input_it = input_map.find(partition_key);
        if (input_it == input_map.end()) continue;

        Dataset& input_partition = input_it->second;

        // Sort if needed
        SortUtils::sort_dataset(input_partition, input_schema, spec.order_column);

        // Build segment tree
        join_utils.build_index(input_partition, input_schema, spec.value_column);

        for (const auto& probe_row : probe_partition) {
            double agg_sum = join_utils.compute_sum_range(
                input_partition,
              	probe_schema,
                probe_row,
                spec.join_spec.begin_column,
                spec.join_spec.end_column
            );

            DataRow output_row = probe_row;
            output_row.push_back(agg_sum);
            result.push_back(std::move(output_row));
        }
    }
}, probe_partitions);



    // Schema extension
    probe_schema.add_column(spec.output_column, "double");
    return {result, probe_schema};
}
