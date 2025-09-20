//
// Created by Vegim Shala on 14.7.25.
//
#include "operators/binary_window_function_operator.h"

#include <iostream>

#include "operators/utils/partition_utils.h"
#include "operators/utils/sort_utils.h"
#include "operators/utils/thread_pool.h"
#include <mutex>
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

pair<Dataset, FileSchema> BinaryWindowFunctionOperator::execute(
    Dataset& input,
    Dataset& probe,
    FileSchema input_schema,
    FileSchema probe_schema
) {
    Dataset result;
    std::mutex result_mtx;

    // -------------------------
    // Total execution timer
    // -------------------------
    auto total_start = std::chrono::high_resolution_clock::now();

    // -------------------------
    // 1. Partitioning
    // -------------------------
    auto part_start = std::chrono::high_resolution_clock::now();

    auto input_partitions = PartitionUtils::partition_dataset_morsel(
        input, input_schema, spec.partition_columns);
    auto probe_partitions = PartitionUtils::partition_dataset_morsel(
        probe, probe_schema, spec.partition_columns);

    auto part_end = std::chrono::high_resolution_clock::now();
    auto duration_partition = std::chrono::duration_cast<std::chrono::milliseconds>(part_end - part_start);
    std::cout << "Partitioning wall time: " << duration_partition.count() << " ms" << std::endl;

    // -------------------------
    // 2. Joining
    // -------------------------
    auto join_start = std::chrono::high_resolution_clock::now();

    // Thread pool setup
    size_t pool_size = std::thread::hardware_concurrency();
    if (pool_size == 0) pool_size = 2;
    ThreadPool pool(pool_size);

    size_t batch_size = std::max<size_t>(1, pool_size / 2);
    std::cout << "ThreadPool size: " << pool_size << ", partition batch size: " << batch_size << std::endl;

    auto key_to_string = [](const auto &key) -> std::string {
        if constexpr (std::is_same_v<std::decay_t<decltype(key)>, std::string>) return key;
        else if constexpr (std::is_same_v<std::decay_t<decltype(key)>, int>) return std::to_string(key);
        else {
            std::string s;
            for (size_t i = 0; i < key.size(); i++) {
                if (i > 0) s += "_";
                s += std::to_string(key[i]);
            }
            return s;
        }
    };

    auto process_partition = [&](Dataset input_partition, Dataset probe_partition) -> void {
        size_t sort_threads = std::max<size_t>(1, pool_size / std::max<size_t>(1, batch_size));
        SortUtils::sort_dataset(input_partition, input_schema, spec.order_column, sort_threads);

        JoinUtils local_join_utils(spec.join_spec, spec.order_column);
        local_join_utils.build_index(input_partition, input_schema, spec.value_column);

        size_t morsel_size = 64;
        size_t probe_n = probe_partition.size();
        std::vector<std::future<std::vector<DataRow>>> futs;

        for (size_t mstart = 0; mstart < probe_n; mstart += morsel_size) {
            size_t mend = std::min(mstart + morsel_size, probe_n);
            futs.emplace_back(pool.submit(
                [mstart, mend, &input_partition, &probe_schema, &probe_partition, &local_join_utils, this]() -> std::vector<DataRow> {
                    std::vector<DataRow> local_out;
                    local_out.reserve(mend - mstart);
                    for (size_t i = mstart; i < mend; ++i) {
                        const DataRow &probe_row = probe_partition[i];
                        double agg_sum = local_join_utils.compute_sum_range(
                            input_partition,
                            probe_schema,
                            probe_row,
                            spec.join_spec.begin_column,
                            spec.join_spec.end_column
                        );
                        DataRow out = probe_row;
                        out.push_back(agg_sum);
                        local_out.emplace_back(std::move(out));
                    }
                    return local_out;
                }
            ));
        }

        for (auto &f : futs) {
            auto part_res = f.get();
            if (!part_res.empty()) {
                std::lock_guard<std::mutex> lk(result_mtx);
                std::move(part_res.begin(), part_res.end(), std::back_inserter(result));
            }
        }
    };

    // iterate over probe partitions
    std::visit([&](auto& probe_map) {
        using MapType = std::decay_t<decltype(probe_map)>;
        if (!std::holds_alternative<MapType>(input_partitions)) return;
        auto &input_map = std::get<MapType>(input_partitions);

        std::vector<std::pair<std::string, std::pair<Dataset, Dataset>>> worklist;
        worklist.reserve(probe_map.size());
        for (auto &kv : probe_map) {
            auto it = input_map.find(kv.first);
            if (it == input_map.end()) continue;
            worklist.emplace_back(key_to_string(kv.first),
                                  std::make_pair(std::move(it->second), std::move(kv.second)));
        }

        for (size_t pos = 0; pos < worklist.size(); pos += batch_size) {
            size_t endpos = std::min(worklist.size(), pos + batch_size);
            std::vector<std::future<void>> batch_futs;
            batch_futs.reserve(endpos - pos);
            for (size_t i = pos; i < endpos; ++i) {
                Dataset in_part = std::move(worklist[i].second.first);
                Dataset pr_part = std::move(worklist[i].second.second);
                batch_futs.emplace_back(pool.submit([in_part = std::move(in_part), pr_part = std::move(pr_part), &process_partition]() mutable {
                    process_partition(std::move(in_part), std::move(pr_part));
                }));
            }
            for (auto &f : batch_futs) f.get();
        }
    }, probe_partitions);

    auto join_end = std::chrono::high_resolution_clock::now();
    auto duration_join = std::chrono::duration_cast<std::chrono::milliseconds>(join_end - join_start);
    std::cout << "Joining wall time: " << duration_join.count() << " ms" << std::endl;

    // -------------------------
    // Total execution time
    // -------------------------
    auto total_end = std::chrono::high_resolution_clock::now();
    auto duration_total = std::chrono::duration_cast<std::chrono::milliseconds>(total_end - total_start);
    std::cout << "Total execute() wall time: " << duration_total.count() << " ms" << std::endl;

    // -------------------------
    // Extend schema and return
    // -------------------------
    probe_schema.add_column(spec.output_column, "double");
    return {std::move(result), probe_schema};
}

