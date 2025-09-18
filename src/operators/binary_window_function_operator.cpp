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

std::string BinaryWindowFunctionOperator::extract_partition_key(const DataRow& row) const {
    if (spec.partition_columns.empty()) return "__NOPART__";
    std::string key;
    for (const auto& col : spec.partition_columns) {
        key += std::get<std::string>(row.at(col)) + "|";
    }
    return key;
}

Dataset BinaryWindowFunctionOperator::probe_parallel(
    const Dataset &input_partition,
    const Dataset &probe_partition,
    const FileSchema &schema,
    size_t num_threads)
{
    size_t n = probe_partition.size();
    if (n == 0) return {};

    if (n < 10000 || num_threads <= 1) {
        // Small case: fall back to sequential
        Dataset result;
        result.reserve(n);
        for (size_t i = 0; i < n; i++) {
            double agg_sum = join_utils.compute_sum_range(
                input_partition, probe_partition[i],
                spec.join_spec.begin_column,
                spec.join_spec.end_column);

            DataRow output_row = probe_partition[i];
            output_row[spec.output_column] = agg_sum;
            result.push_back(std::move(output_row));
        }
        return result;
    }

    size_t chunk_size = (n + num_threads - 1) / num_threads;
    std::vector<std::future<Dataset>> futures;

    for (size_t t = 0; t < num_threads; t++) {
        size_t start = t * chunk_size;
        size_t end   = std::min(start + chunk_size, n);
        if (start >= end) break;

        futures.push_back(std::async(std::launch::async, [&, start, end]() {
            Dataset local_result;
            local_result.reserve(end - start);

            for (size_t i = start; i < end; i++) {
                double agg_sum = join_utils.compute_sum_range(
                    input_partition, probe_partition[i],
                    spec.join_spec.begin_column,
                    spec.join_spec.end_column);

                DataRow output_row = probe_partition[i];
                output_row[spec.output_column] = agg_sum;
                local_result.push_back(std::move(output_row));
            }
            return local_result;
        }));
    }

    // Merge results in order
    Dataset result;
    result.reserve(n);
    for (auto &f : futures) {
        auto chunk = f.get();
        result.insert(result.end(), std::make_move_iterator(chunk.begin()),
                                   std::make_move_iterator(chunk.end()));
    }

    return result;
}

pair<Dataset, FileSchema> BinaryWindowFunctionOperator::execute(const Dataset& input, const Dataset& probe, FileSchema schema) {
    Dataset result;

    auto start = std::chrono::high_resolution_clock::now();

    // Partition input and probe
    auto input_partitions = PartitionUtils::partition_dataset_parallel(input, spec.partition_columns, 8);
    auto probe_partitions = PartitionUtils::partition_dataset_parallel(probe, spec.partition_columns, 8);

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    std::cout << "Time taken for PARTITIONING: " << duration.count() << " ms" << std::endl;



    // for (const auto& [partition_key, probe_partition] : probe_partitions) {
    //     auto input_it = input_partitions.find(partition_key);
    //     if (input_it == input_partitions.end()) continue;
    //
    //     Dataset& input_partition = input_it->second;
    //     SortUtils::sort_dataset(input_partition, spec.order_column); // sometimes it might not be needed
    //     // SortUtils::counting_sort_rows(input_partition, spec.order_column); // sometimes it might not be needed
    //
    //     // auto startSegTreeTime = std::chrono::high_resolution_clock::now();
    //
    //     join_utils.build_index(input_partition, spec.value_column);
    //
    //     // auto endSegTreeTime = std::chrono::high_resolution_clock::now();
    //     // auto durationSegTreeTime = std::chrono::duration_cast<std::chrono::milliseconds>(endSegTreeTime - startSegTreeTime);
    //     // std::cout << "Time taken for BUILDING SEGMENT TREE: " << durationSegTreeTime.count() << " ms" << std::endl;
    //
    //     // std::cout << "PRINTING DATASET AFTER SORT" << std::endl;
    //     // print_dataset(input_partition, schema);
    //
    //     // auto start = std::chrono::high_resolution_clock::now();
    //
    //     // for (const auto& probe_row : probe_partition) {
    //     //     // auto start = std::chrono::high_resolution_clock::now();
    //     //
    //     //     // std::vector<size_t> indices = join_utils.compute_join(input_partition, probe_row);
    //     //
    //     //     double agg_sum = join_utils.compute_sum_range(
    //     //         input_partition,
    //     //         probe_row,
    //     //         spec.join_spec.begin_column,
    //     //         spec.join_spec.end_column
    //     //     );
    //     //
    //     //     // auto end = std::chrono::high_resolution_clock::now();
    //     //     // auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    //     //     // std::cout << "Time taken for ONE JOIN: " << duration.count() << " ms" << std::endl;
    //     //
    //     //     // std::vector<double> values;
    //     //     //
    //     //     // for (size_t idx : indices) {
    //     //     //     values.push_back(extract_numeric(input_partition[idx].at(spec.value_column)));
    //     //     // }
    //     //
    //     //     DataRow output_row = probe_row;
    //     //     output_row[spec.output_column] = agg_sum;
    //     //     result.push_back(std::move(output_row));
    //     // }
    //
    //     Dataset local_result = probe_parallel(input_partition, probe_partition, schema, 8);
    //
    //     // Append results to global output
    //     result.insert(result.end(),
    //                   std::make_move_iterator(local_result.begin()),
    //                   std::make_move_iterator(local_result.end()));
    //
    //     // auto end = std::chrono::high_resolution_clock::now();
    //     // auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    //     // std::cout << "Time taken for JOINING: " << duration.count() << " ms" << std::endl;
    //
    //
    // }

    ThreadPool pool(8); // or std::thread::hardware_concurrency()
    std::vector<std::future<Dataset>> futures;

    for (const auto& [partition_key, probe_partition] : probe_partitions) {
        auto input_it = input_partitions.find(partition_key);
        if (input_it == input_partitions.end()) continue;

        // copy for capture
        Dataset input_partition = input_it->second;
        auto spec_copy = spec;
        auto schema_copy = schema;

        futures.push_back(pool.enqueue([this, input_partition, probe_partition, spec_copy, schema_copy]() mutable {
            // sort
            SortUtils::sort_dataset(input_partition, spec_copy.order_column);

            // build index using the member
            this->join_utils.build_index(input_partition, spec_copy.value_column);

            Dataset local_result;
            local_result.reserve(probe_partition.size());

            for (const auto& probe_row : probe_partition) {
                double agg_sum = this->join_utils.compute_sum_range(
                    input_partition, probe_row,
                    spec_copy.join_spec.begin_column,
                    spec_copy.join_spec.end_column);

                DataRow output_row = probe_row;
                output_row[spec_copy.output_column] = agg_sum;
                local_result.push_back(std::move(output_row));
            }

            return local_result;
        }));
    }

    // Collect results
    // Dataset result;
    for (auto& f : futures) {
        Dataset chunk = f.get();
        result.insert(result.end(),
                      std::make_move_iterator(chunk.begin()),
                      std::make_move_iterator(chunk.end()));
    }


    // Schema extension
    schema.columns.push_back(spec.output_column);
    schema.column_types[spec.output_column] = "double";
    return {result, schema};
}
