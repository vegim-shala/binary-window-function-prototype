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
#include <numeric>
#include <ips2ra/ips2ra.hpp>

using namespace std;

/**
 * @brief Build a worklist of partitions to process.
 *
 * Each worklist entry contains a string key (for debugging) and a pair of
 * index vectors (input_indices, probe_indices) corresponding to that partition.
 *
 * @param input_idx_partitions Partitioned input dataset (indices per partition key).
 * @param probe_idx_partitions Partitioned probe dataset (indices per partition key).
 * @return A vector of worklist entries ready for parallel processing.
 */
std::vector<std::pair<PartitionUtils::IndexDataset, PartitionUtils::IndexDataset> >
BinaryWindowFunctionOperator::build_worklist(
    auto &input_idx_partitions,
    auto &probe_idx_partitions
) {
    using IndexDataset = PartitionUtils::IndexDataset;
    std::vector<std::pair<IndexDataset, IndexDataset> > worklist;
    worklist.reserve(1024);

    std::visit([&](auto &probe_map) {
        for (auto &kv: probe_map) {
            const auto &key = kv.first;
            const IndexDataset &pr_inds = kv.second;
            if (pr_inds.empty()) continue;

            IndexDataset in_inds;

            if constexpr (std::is_same_v<std::decay_t<decltype(key)>, int32_t>) {
                auto &input_map = std::get<std::unordered_map<int32_t, IndexDataset> >(input_idx_partitions);
                auto it = input_map.find(key);
                if (it != input_map.end()) {
                    // if the corresponding input partition exists, get its indices. Otherwise, we want to skip this probe partition.
                    in_inds = std::move(it->second);
                } else continue;
            } else {
                auto &input_map = std::get<
                    std::unordered_map<std::vector<int32_t>, IndexDataset,
                        PartitionUtils::VecHash, PartitionUtils::VecEq>
                >(input_idx_partitions);
                auto it = input_map.find(key);
                if (it != input_map.end()) {
                    in_inds = std::move(it->second);
                }
            }

            worklist.emplace_back(std::move(in_inds), pr_inds);
        }
    }, probe_idx_partitions);

    return worklist;
}

/**
 * @brief Process all partitions in the worklist using a thread pool.
 *
 * Each partition is handed off to `process_partition`, either in parallel
 * or inline depending on batch size and partition size.
 *
 * @param worklist Vector of partition tasks (input_indices, probe_indices).
 * @param input Input dataset.
 * @param probe Probe dataset.
 * @param input_schema Schema of the input dataset.
 * @param probe_schema Schema of the probe dataset.
 * @param result Result dataset (shared).
 * @param result_mtx Mutex protecting result appends.
 * @param pool Thread pool for parallel execution.
 * @param batch_size Number of partition tasks per batch.
 * @param morsel_size Minimum partition size to enable morsel parallelism.
 */
void BinaryWindowFunctionOperator::process_worklist(
    std::vector<std::pair<PartitionUtils::IndexDataset, PartitionUtils::IndexDataset> > &worklist,
    Dataset &input,
    Dataset &probe,
    FileSchema &input_schema,
    FileSchema &probe_schema,
    Dataset &result,
    std::mutex &result_mtx,
    ThreadPool &pool,
    size_t batch_size,
    size_t morsel_size
) {
    using IndexDataset = PartitionUtils::IndexDataset;

    // One batch contains multiple partitions, each processed in parallel.
    for (size_t pos = 0; pos < worklist.size(); pos += batch_size) {
        // for each batch
        size_t endpos = std::min(worklist.size(), pos + batch_size);
        std::vector<std::future<void> > batch_futs;
        batch_futs.reserve(endpos - pos); // at most one future per partition in batch

        for (size_t i = pos; i < endpos; ++i) {

            if (pool.size() == 1) {
                process_partition_new(std::move(worklist[i].first), std::move(worklist[i].second),
                                          input, probe, input_schema, probe_schema,
                                          result, result_mtx, pool, morsel_size);
                continue;
            }
            // for each partition in batch
            // Pass the partition to the thread pool and process it in parallel
            batch_futs.emplace_back(pool.submit(
                [this, in_inds = std::move(worklist[i].first), pr_inds = std::move(worklist[i].second),
                    &input, &probe, &input_schema, &probe_schema,
                    &result, &result_mtx, &pool, morsel_size]() mutable {
                    process_partition_new(std::move(in_inds), std::move(pr_inds),
                                          input, probe, input_schema, probe_schema,
                                          result, result_mtx, pool, morsel_size);
                }));
        }

        // Wait for batch
        for (auto &f: batch_futs) f.get();
    }
}

std::vector<DataRow> BinaryWindowFunctionOperator::process_probe_morsel_sort_probe_interleaving(
    size_t mstart,
    size_t mend,
    const PartitionUtils::IndexDataset &pr_indices,
    const Dataset &probe,
    const FileSchema &probe_schema,
    JoinUtils &local_join
) const {
    // ---- 1. Prepare probe tasks ----
    std::vector<ProbeTask> tasks;
    tasks.reserve(mend - mstart);

    size_t probe_begin_idx = probe_schema.index_of(spec.join_spec.begin_column);
    size_t probe_end_idx = probe_schema.index_of(spec.join_spec.end_column);

    for (size_t j = mstart; j < mend; ++j) {
        size_t probe_idx = pr_indices[j];
        const DataRow &probe_row = probe[probe_idx];

        ProbeTask t;
        t.orig_pos = j - mstart; // keep local order
        t.probe_idx = probe_idx;
        t.start = probe_row[probe_begin_idx];
        t.end = probe_row[probe_end_idx];
        tasks.emplace_back(t);
    }


    auto start_probing = std::chrono::high_resolution_clock::now();

    // ---- 2.5: Extract probe keys ---
    std::vector<int32_t> start_keys, end_keys;
    start_keys.reserve(tasks.size());
    end_keys.reserve(tasks.size());

    for (auto &t: tasks) {
        start_keys.push_back(t.start);
        end_keys.push_back(t.end);
    }

    // ---- 3: Batched binary searches ----
    std::vector<size_t> lo_positions = local_join.batched_lower_bound(start_keys);
    std::vector<size_t> hi_positions = local_join.batched_upper_bound(end_keys);

    // ---- 4: Compute aggregations ----
    std::vector<uint64_t> agg(mend - mstart);
    for (size_t i = 0; i < tasks.size(); i++) {
        uint64_t sum = 0;
        if (lo_positions[i] < hi_positions[i]) {
            sum = local_join.prefix_sums_query(lo_positions[i], hi_positions[i]);
        }
        agg[tasks[i].orig_pos] = sum; // scatter back
    }


    // auto end_probing = std::chrono::high_resolution_clock::now();
    // auto duration_probing = std::chrono::duration_cast<std::chrono::nanoseconds>(
    //     end_probing - start_probing);
    // std::cout << "Time taken for PROBING: " << duration_probing.count() << " ns" << std::endl;290097

    // ---- 4. Assemble DataRows in original order ----
    // we need this step because we need to append the aggregate to the probe rows
    // and we need to do it in the original order of the probe rows ->
    // We need to do it in the original order because
    std::vector<DataRow> local_out;
    local_out.reserve(mend - mstart);

    for (size_t j = mstart; j < mend; ++j) {
        size_t probe_idx = pr_indices[j];
        const DataRow &probe_row = probe[probe_idx];

        DataRow out = probe_row;
        out.push_back(agg[j - mstart]);
        local_out.emplace_back(std::move(out));
    }

    return local_out;
}

std::pair<Dataset, FileSchema> BinaryWindowFunctionOperator::execute_with_speedup_test_original(
    Dataset &input,
    Dataset &probe,
    FileSchema input_schema,
    FileSchema probe_schema
) {
    Dataset final_result;
    FileSchema final_schema = probe_schema;

    std::cout << "\n================ Parallel Scaling Test ================\n";
    std::cout << "Threads | Partition (ms) | Probe (ms) | Total (ms) | Speedup\n";
    std::cout << "--------------------------------------------------------------------------\n";


    const int repeats = 5;
    double baseline_total = -1.0;

    for (size_t num_threads = 1; num_threads <= 8; ++num_threads) {
        std::vector<double> total_times, part_times, build_times, probe_times;
        total_times.reserve(repeats);
        part_times.reserve(repeats);
        build_times.reserve(repeats);
        probe_times.reserve(repeats);


        for (int r = 0; r < repeats; ++r) {
            if (num_threads == 1) {
                // Run a fully sequential version (no thread pool)
                auto total_start = std::chrono::high_resolution_clock::now();

                ThreadPool dummy_pool(1);

                auto part_start = std::chrono::high_resolution_clock::now();
                auto input_partitions = PartitionUtils::partition_indices_parallel(
                    input, input_schema, spec.partition_columns, dummy_pool);
                auto probe_partitions_map = PartitionUtils::partition_indices_parallel(
                    probe, probe_schema, spec.partition_columns, dummy_pool);
                auto part_end = std::chrono::high_resolution_clock::now();
                double part_ms = std::chrono::duration<double, std::milli>(part_end - part_start).count();

                auto probe_start = std::chrono::high_resolution_clock::now();

                // Stage 3: Build the worklist => the partitions to process
                auto worklist = build_worklist(input_partitions, probe_partitions_map);


                // Stage 4: Process the partitions in parallel using the thread pool
                size_t batch_size = std::max<size_t>(1, std::min<size_t>(dummy_pool.size() / 2, worklist.size()));
                // leave threads for morsels

                Dataset result;
                std::mutex result_mtx;

                // We use the inline function calculate_median_probe_partition_size to calculate the probe_morsel_size
                size_t probe_morsel_size = std::min<size_t>(
                    65536, std::exp2(static_cast<size_t>(
                        std::floor(std::log2(std::max<size_t>(
                            (calculate_median_probe_partition_size(probe_partitions_map) / 32), 1UL))))));



                if (worklist.size() == 1) {
                    auto [in_inds, pr_inds] = std::move(worklist[0]);
                    process_partition_new(std::move(in_inds), std::move(pr_inds),
                                          input, probe, input_schema, probe_schema,
                                          result, result_mtx, dummy_pool, probe_morsel_size);
                    // return
                } else {
                    process_worklist(worklist, input, probe, input_schema, probe_schema,
                                     result, result_mtx, dummy_pool, batch_size, probe_morsel_size); // current path
                }


                auto probe_end = std::chrono::high_resolution_clock::now();
                double probe_ms = std::chrono::duration<double, std::milli>(probe_end - probe_start).count();

                auto total_end = std::chrono::high_resolution_clock::now();
                double total_ms = std::chrono::duration<double, std::milli>(total_end - total_start).count();

                // Store timing vectors like before...
                part_times.push_back(part_ms);
                probe_times.push_back(probe_ms);
                total_times.push_back(total_ms);

                continue; // Skip the parallel section below
            }
            ThreadPool pool(num_threads);

            Dataset result;
            result.reserve(probe.size());
            std::mutex result_mtx;

            auto total_start = std::chrono::high_resolution_clock::now();

            // --------------------------
            // 1. Partitioning
            // --------------------------
            auto part_start = std::chrono::high_resolution_clock::now();

            size_t input_partition_morsel_size = std::min<size_t>(
                65536, std::exp2(static_cast<size_t>(std::floor(std::log2(input.size() / 32)))));
            size_t probe_partition_morsel_size = std::min<size_t>(
                65536, std::exp2(static_cast<size_t>(std::floor(std::log2(probe.size() / 32)))));

            auto input_partitions = PartitionUtils::partition_indices_parallel(
                input, input_schema, spec.partition_columns, pool, num_threads, input_partition_morsel_size, 8);
            auto probe_partitions_map = PartitionUtils::partition_indices_parallel(
                probe, probe_schema, spec.partition_columns, pool, num_threads, probe_partition_morsel_size, 8);

            auto part_end = std::chrono::high_resolution_clock::now();
            double part_ms = std::chrono::duration<double, std::milli>(part_end - part_start).count();

            // if (num_threads == 2) {
            //     // or just once, outside scaling loop
            //     build_component_scaling_test(
            //         input, input_schema,
            //         input_partitions, probe_partitions_map,
            //         65536 /* morsel size */
            //     );
            //     exit(0);
            // }


            auto probe_start = std::chrono::high_resolution_clock::now();

            // Stage 3: Build the worklist => the partitions to process
            auto worklist = build_worklist(input_partitions, probe_partitions_map);

            // Stage 4: Process the partitions in parallel using the thread pool
            size_t batch_size = std::max<size_t>(1, std::min<size_t>(pool.size() / 2, worklist.size()));
            // leave threads for morsels

            // We use the inline function calculate_median_probe_partition_size to calculate the probe_morsel_size
            size_t probe_morsel_size = std::min<size_t>(
                65536, std::exp2(static_cast<size_t>(
                    std::floor(std::log2(std::max<size_t>(
                        (calculate_median_probe_partition_size(probe_partitions_map) / 32), 1UL))))));

            if (worklist.size() == 1) {
                auto [in_inds, pr_inds] = std::move(worklist[0]);
                process_partition_new(std::move(in_inds), std::move(pr_inds),
                                      input, probe, input_schema, probe_schema,
                                      result, result_mtx, pool, probe_morsel_size);
                // return
            } else {
                process_worklist(worklist, input, probe, input_schema, probe_schema,
                                 result, result_mtx, pool, batch_size, probe_morsel_size); // current path
            }

            auto probe_end = std::chrono::high_resolution_clock::now();
            double probe_ms = std::chrono::duration<double, std::milli>(probe_end - probe_start).count();

            auto total_end = std::chrono::high_resolution_clock::now();
            double total_ms = std::chrono::duration<double, std::milli>(total_end - total_start).count();


            // Collect timings
            part_times.push_back(part_ms);
            probe_times.push_back(probe_ms);
            total_times.push_back(total_ms);
        }

        // --------------------------
        // 4. Take medians
        // --------------------------
        auto median = [](std::vector<double> &v) {
            std::sort(v.begin(), v.end());
            return v[v.size() / 2];
        };

        double part_median = median(part_times);
        double probe_median = median(probe_times);
        double total_median = median(total_times);

        // --------------------------
        // 5. Compute speedup
        // --------------------------
        if (baseline_total < 0)
            baseline_total = total_median;

        double speedup = baseline_total / total_median;

        // --------------------------
        // 6. Print results
        // --------------------------
        std::cout << std::setw(7) << num_threads << " | "
                << std::setw(14) << std::fixed << std::setprecision(2) << part_median << " | "
                << std::setw(10) << probe_median << " | "
                << std::setw(10) << total_median << " | "
                << std::setw(7) << std::fixed << std::setprecision(2) << speedup << "x\n";

        if (num_threads == 8)
            final_result = std::move(final_result);
    }

    std::cout << "==========================================================================\n";

    final_schema.add_column(spec.output_column, "double");
    return {std::move(final_result), final_schema};
}


pair<Dataset, FileSchema> BinaryWindowFunctionOperator::execute(
    Dataset &input,
    Dataset &probe,
    FileSchema input_schema,
    FileSchema probe_schema
) {
    Dataset result;
    // TODO: Should we reserve this, or remove the rows that are not matched later?
    result.reserve(probe.size());
    std::mutex result_mtx;

    auto total_start = std::chrono::high_resolution_clock::now();

    // Stage 0: Setup thread pool
    size_t pool_size = std::thread::hardware_concurrency();
    if (pool_size == 0) pool_size = 2;
    ThreadPool pool(pool_size);


    //TODO: Evaluate and tune partition morsel size
    size_t input_partition_morsel_size = std::min<size_t>(
        65536, std::exp2(static_cast<size_t>(std::floor(std::log2(input.size() / 32)))));
    size_t probe_partition_morsel_size = std::min<size_t>(
        65536, std::exp2(static_cast<size_t>(std::floor(std::log2(probe.size() / 32)))));
    // std::cout << "Partition morsel size: " << partition_morsel_size << std::endl;

    // Stage 1: Pre-partition input and probe datasets
    auto part_start = std::chrono::high_resolution_clock::now();
    auto input_idx_partitions = PartitionUtils::partition_indices_parallel(
        input, input_schema, spec.partition_columns, pool, pool_size, input_partition_morsel_size, 8
    );
    auto probe_idx_partitions = PartitionUtils::partition_indices_parallel(
        probe, probe_schema, spec.partition_columns, pool, pool_size, probe_partition_morsel_size, 8
    );
    auto part_end = std::chrono::high_resolution_clock::now();
    std::cout << "Partitioning wall time: "
            << std::chrono::duration_cast<std::chrono::microseconds>(part_end - part_start).count()
            << " ms" << std::endl;


    // Stage 2: Process Build Partitions
    // auto build_start = std::chrono::high_resolution_clock::now();
    //
    // build_states_from_partitions(input, input_schema, input_idx_partitions, probe_idx_partitions, pool, 256);
    //
    //
    // // Stage 4: Probing
    // auto probe_start = std::chrono::high_resolution_clock::now();
    //
    // size_t batch_size = std::max<size_t>(1, std::min<size_t>(pool_size / 2, probe_partitions.size()));
    //
    // size_t probe_morsel_size = std::min<size_t>(
    //     65536, std::exp2(static_cast<size_t>(
    //         std::floor(std::log2(std::max<size_t>(
    //             (calculate_median_probe_partition_size(probe_partitions) / 32), 1UL))))));
    //
    // probe_all_partitions(probe, probe_schema, result, result_mtx, pool, batch_size, probe_morsel_size);
    //
    // auto probe_end = std::chrono::high_resolution_clock::now();
    // std::cout << "[Stage 4] Probe wall time: "
    //         << std::chrono::duration_cast<std::chrono::microseconds>(
    //             probe_end - probe_start)
    //         .count()
    //         << " ms\n";


    auto join_start = std::chrono::high_resolution_clock::now();

    // Stage 3: Build the worklist => the partitions to process
    auto worklist = build_worklist(input_idx_partitions, probe_idx_partitions);

    // Stage 4: Process the partitions in parallel using the thread pool
    size_t batch_size = std::max<size_t>(1, std::min<size_t>(pool_size / 2, worklist.size()));
    // leave threads for morsels

    // We use the inline function calculate_median_probe_partition_size to calculate the probe_morsel_size
    size_t probe_morsel_size = std::min<size_t>(
        65536, std::exp2(
            // static_cast<size_t>(std::floor(std::log2(calculate_median_probe_partition_size(worklist) / 32)))));
            static_cast<size_t>(std::floor(std::log2(calculate_median_probe_partition_size(probe_idx_partitions) / 32)))));

    if (worklist.size() == 1) {
        auto [in_inds, pr_inds] = std::move(worklist[0]);
        process_partition_new(std::move(in_inds), std::move(pr_inds),
                              input, probe, input_schema, probe_schema,
                              result, result_mtx, pool, probe_morsel_size);
        // return
    } else {
        process_worklist(worklist, input, probe, input_schema, probe_schema,
                         result, result_mtx, pool, batch_size, probe_morsel_size); // current path
    }


    auto join_end = std::chrono::high_resolution_clock::now();
    std::cout << "Join wall time: "
            << std::chrono::duration_cast<std::chrono::microseconds>(join_end - join_start).count()
            << " ms" << std::endl;

    auto total_end = std::chrono::high_resolution_clock::now();
    std::cout << "Total execute() wall time: "
            << std::chrono::duration_cast<std::chrono::microseconds>(total_end - total_start).count()
            << " ms" << std::endl;

    // Stage 5: Append output column to probe schema
    probe_schema.add_column(spec.output_column, "double");
    return {std::move(result), probe_schema};
}



// -------------------------------------- THE FIRST PART IS THE PARALLEL VERSION --------------------------------------
// -------------------------------------- -------------------------------------- --------------------------------------
// -------------------------------------- -------------------------------------- --------------------------------------
// -------------------------------------- -------------------------------------- --------------------------------------
// ------------------------------------- THE SECOND PART IS THE SEQUENTIAL VERSION --------------------------------------
// -------------------------------------- -------------------------------------- --------------------------------------
// -------------------------------------- -------------------------------------- --------------------------------------

pair<Dataset, FileSchema> BinaryWindowFunctionOperator::execute_sequential(
    Dataset &input,
    Dataset &probe,
    FileSchema input_schema,
    FileSchema probe_schema
) {
    Dataset result;
    result.reserve(probe.size());

    const auto total_start = std::chrono::high_resolution_clock::now();

    // --- Stage 1: partition (sequential) ---
    const auto partition_start = std::chrono::high_resolution_clock::now();
    auto input_idx_partitions = PartitionUtils::partition_indices_sequential(
        input, input_schema, spec.partition_columns);
    auto probe_idx_partitions = PartitionUtils::partition_indices_sequential(
        probe, probe_schema, spec.partition_columns);
    const auto partition_end = std::chrono::high_resolution_clock::now();
    std::cout << "Partitioning wall time: "
            << std::chrono::duration_cast<std::chrono::microseconds>(partition_end - partition_start).count()
            << " ms\n";

    // --- Stage 2: worklist ---
    const auto join_start = std::chrono::high_resolution_clock::now();
    auto worklist = build_worklist(input_idx_partitions, probe_idx_partitions);

    // --- Stage 3: process each partition sequentially (new path) ---
    for (auto &[in_inds, pr_inds]: worklist) {
        process_partition_sequential(std::move(in_inds), std::move(pr_inds),
                                     input, probe, input_schema, probe_schema,
                                     result);
    }

    const auto join_end = std::chrono::high_resolution_clock::now();
    std::cout << "Join wall time: "
            << std::chrono::duration_cast<std::chrono::microseconds>(join_end - join_start).count()
            << " ms\n";

    const auto total_end = std::chrono::high_resolution_clock::now();
    std::cout << "Total execute_sequential() wall time: "
            << std::chrono::duration_cast<std::chrono::microseconds>(total_end - total_start).count()
            << " ms\n";

    // --- Stage 4: schema ---
    probe_schema.add_column(spec.output_column, "double");
    return {std::move(result), probe_schema};
}


/**
 * @brief Sequential variant of process_partition (no morsel parallelism).
 *
 * Builds a sorted index and segment tree on the input partition,
 * then probes the probe partition sequentially.
 */
void BinaryWindowFunctionOperator::process_partition_sequential(
    PartitionUtils::IndexDataset in_indices,
    PartitionUtils::IndexDataset pr_indices,
    const Dataset &input,
    const Dataset &probe,
    const FileSchema &input_schema,
    const FileSchema &probe_schema,
    Dataset &result
) const {
    if (pr_indices.empty() || in_indices.empty()) return;

    // --- Build-side: sort by ORDER (use your radix-backed SortUtils) ---
    const size_t order_idx = input_schema.index_of(spec.order_column);
    SortUtils::sort_dataset_indices(input, in_indices, order_idx);

    // --- Build prefix sums index from sorted indices (signed keys, int64 prefix) ---
    const size_t value_idx = input_schema.index_of(spec.value_column);
    JoinUtils local_join(spec.join_spec, spec.order_column);
    std::vector<int32_t> values;
    local_join.build_keys_and_values(
        input, input_schema, in_indices, order_idx, value_idx, values);

    auto aggregator = create_aggregator(spec.agg_type);
    aggregator->build_from_values(values);

    // --- Probe-side column indexes ---
    const size_t begin_idx = probe_schema.index_of(spec.join_spec.begin_column);
    const size_t end_idx = probe_schema.index_of(spec.join_spec.end_column);

    // --- Single-morsel probe using the *same* routine as parallel path ---
    auto local_out = process_probe_morsel_new(
        0, pr_indices.size(), pr_indices, probe, local_join, *aggregator, begin_idx, end_idx);

    // --- Append (no mutex needed; single-thread) ---
    std::move(local_out.begin(), local_out.end(), std::back_inserter(result));
}


void BinaryWindowFunctionOperator::process_partition_new(
    PartitionUtils::IndexDataset in_indices,
    PartitionUtils::IndexDataset pr_indices,
    const Dataset &input,
    const Dataset &probe,
    const FileSchema &input_schema,
    const FileSchema &probe_schema,
    Dataset &result,
    std::mutex &result_mtx,
    ThreadPool &pool,
    size_t morsel_size
) const {

    // Skip empty partitions ( this might change if we change partitioning scheme )
    if (pr_indices.empty() || in_indices.empty()) return;

    // Stage 1: Sort input partition by order column using sort_utils.cpp ips2ra_sort
    const size_t order_idx = input_schema.index_of(spec.order_column);
    SortUtils::sort_dataset_indices(input, in_indices, order_idx);


    // 2. Build Index
    const size_t value_idx = input_schema.index_of(spec.value_column);

    JoinUtils local_join(spec.join_spec, spec.order_column);
    std::vector<int32_t> values;
    local_join.build_keys_and_values(
        input, input_schema, in_indices, order_idx, value_idx, values);

    auto start_building_index = std::chrono::high_resolution_clock::now();

    auto aggregator = create_aggregator(spec.agg_type);
    aggregator->build_from_values(values);
    // local_join.build_prefix_sums_from_sorted_indices(input, input_schema,
    //                                                  in_indices, order_idx, value_idx);
    // local_join.build_index_from_vectors_segtree(keys, values);
    // local_join.build_index_from_vectors_sqrttree(keys, values);
    // local_join.build_eytzinger();

    auto end_building_index = std::chrono::high_resolution_clock::now();
    auto duration_building_index = std::chrono::duration_cast<std::chrono::microseconds>(
        end_building_index - start_building_index);
    std::cout << "Time taken for BUILDING INDEX: " << duration_building_index.count() << " ms" << std::endl;

    // --------------------------- PROCESSING THE PROBE ------------------------------

    const size_t begin_col_idx = probe_schema.index_of(spec.join_spec.begin_column);
    const size_t end_col_idx = probe_schema.index_of(spec.join_spec.end_column);
    // SortUtils::sort_dataset_indices(probe, pr_indices, begin_col_idx);

    auto start_partition_processing = std::chrono::high_resolution_clock::now();

    // 3. Dispatch into morsels
    if (pr_indices.size() >= morsel_size * 2) {
        // TODO: Tune threshold
        process_probe_partition_parallel_new(pr_indices, probe, local_join, *aggregator, result, result_mtx, pool,
                                             morsel_size,
                                             begin_col_idx, end_col_idx);
    } else {
        process_probe_partition_inline_new(pr_indices, probe, local_join, *aggregator, result, result_mtx,
                                           begin_col_idx,
                                           end_col_idx);
    }

    auto end_partition_processing = std::chrono::high_resolution_clock::now();
    auto duration_partition_processing = std::chrono::duration_cast<std::chrono::microseconds>(
        end_partition_processing - start_partition_processing);
    std::cout << "Time taken for PROBING: " << duration_partition_processing.count() << " ms" << std::endl;
}

void BinaryWindowFunctionOperator::process_probe_partition_parallel_new(
    const PartitionUtils::IndexDataset &pr_indices,
    const Dataset &probe,
    const JoinUtils &local_join,
    const Aggregator &aggregator,
    Dataset &result,
    std::mutex &result_mtx,
    ThreadPool &pool,
    size_t morsel_size,
    size_t begin_idx,
    size_t end_idx
) const {
    std::vector<std::future<std::vector<DataRow> > > futs; // each future returns a vector of DataRow
    futs.reserve((pr_indices.size() + morsel_size - 1) / morsel_size); // round up to determine number of morsels

    for (size_t mstart = 0; mstart < pr_indices.size(); mstart += morsel_size) {
        // for each morsel
        size_t mend = std::min(mstart + morsel_size, pr_indices.size());

        if (pool.size() == 1) {
            auto test = process_probe_morsel_new(mstart, mend, pr_indices, probe, local_join, aggregator, begin_idx,
                                                 end_idx);
            std::move(test.begin(), test.end(), std::back_inserter(result));
        }

        futs.emplace_back(pool.submit(
            [this, mstart, mend, &pr_indices, &probe, &local_join, &aggregator, begin_idx, end_idx]() mutable {
                return process_probe_morsel_new(mstart, mend, pr_indices, probe, local_join, aggregator, begin_idx,
                                                end_idx);
            }));
    }

    for (auto &f: futs) {
        auto part_res = f.get();
        if (!part_res.empty()) {
            // if there are results to append
            std::lock_guard<std::mutex> lk(result_mtx); // protect result appends because result is shared
            std::move(part_res.begin(), part_res.end(), std::back_inserter(result));
        }
    }
}


void BinaryWindowFunctionOperator::process_probe_partition_inline_new(
    const PartitionUtils::IndexDataset &pr_indices,
    const Dataset &probe,
    const JoinUtils &local_join,
    const Aggregator &aggregator,
    Dataset &result,
    std::mutex &result_mtx,
    size_t begin_idx,
    size_t end_idx
) const {
    auto local_out = process_probe_morsel_new(0, pr_indices.size(),
                                              pr_indices, probe, local_join, aggregator, begin_idx, end_idx);
    std::lock_guard<std::mutex> lk(result_mtx);
    std::move(local_out.begin(), local_out.end(), std::back_inserter(result));
}


std::vector<DataRow> BinaryWindowFunctionOperator::process_probe_morsel_new(
    size_t mstart,
    size_t mend,
    const PartitionUtils::IndexDataset &pr_indices,
    const Dataset &probe,
    const JoinUtils &local_join,
    const Aggregator &aggregator,
    size_t begin_idx,
    size_t end_idx
) const {
    const size_t cnt = mend - mstart;
    if (cnt == 0) return {};

    // 1) Gather SoA
    std::vector<int32_t> q_starts;
    q_starts.reserve(cnt);
    std::vector<int32_t> q_ends;
    q_ends.reserve(cnt);
    std::vector<size_t> probe_ids;
    probe_ids.reserve(cnt);

    for (size_t j = mstart; j < mend; ++j) {
        const size_t pid = pr_indices[j];
        const DataRow &row = probe[pid];
        q_starts.push_back(static_cast<int32_t>(row[begin_idx]));
        q_ends.push_back(static_cast<int32_t>(row[end_idx]));
        probe_ids.push_back(pid);
    }

    // 2) lo with monotone forward scan (begin is nondecreasing across the morsel)
    // std::vector<size_t> lo(cnt);
    // size_t hint_lo = 0;
    // for (size_t i = 0; i < cnt; ++i) {
    //     hint_lo = local_join.lower_from_hint(hint_lo, q_starts[i]);
    //     lo[i] = hint_lo;
    // }
    std::vector<size_t> lo = local_join.batched_lower_bound_i32_interleaved(q_starts);

    //     // 3) Sort positions by end inside this morsel
    //     std::vector<size_t> by_end(cnt);
    //     std::iota(by_end.begin(), by_end.end(), 0);
    // #ifdef NDEBUG
    //     ips2ra::sort(by_end.begin(), by_end.end(),
    //                  [&](size_t i){ return static_cast<uint32_t>(q_ends[i]) ^ 0x80000000u; });
    // #else
    //     std::sort(by_end.begin(), by_end.end(),
    //               [&](size_t a, size_t b){ return q_ends[a] < q_ends[b]; });
    // #endif
    //
    //     // 4) hi with a *seeded first* + forward hints afterwards
    //     std::vector<size_t> hi(cnt);
    //
    //     // seed from the first (smallest end) item in this morsel:
    //     {
    //         const size_t i0 = by_end[0];
    //         const size_t start0 = lo[i0];                                  // never before lo
    //         // single binary search on [start0, n) to seed hi accurately:
    //         const size_t n = local_join.keys_size();                        // add a const accessor returning keys.size()
    //         size_t seeded = local_join.bounded_upper_bound_i32(start0, n, q_ends[i0]);
    //         hi[i0] = seeded;
    //
    //         // now walk forward for the rest
    //         size_t hint_hi = seeded;
    //         for (size_t k = 1; k < cnt; ++k) {
    //             const size_t i = by_end[k];
    //             const size_t start = (hint_hi < lo[i]) ? lo[i] : hint_hi;   // never go before lo
    //             hint_hi = local_join.upper_from_hint(start, q_ends[i]);     // forward gallop + bounded refine
    //             hi[i] = hint_hi;
    //         }
    //     }

    // hi via interleaved batched upper_bound seeded with lo
    std::vector<size_t> hi = local_join.batched_upper_bound_i32_interleaved_from_lo(q_ends, lo);

    // 5) Emit (no need to restore original order)
    std::vector<DataRow> out;
    out.reserve(cnt);
    for (size_t i = 0; i < cnt; ++i) {
        if (lo[i] >= hi[i]) continue;
        const int64_t sum = aggregator.query(lo[i], hi[i]);
        DataRow r = probe[probe_ids[i]];
        r.push_back(sum);
        out.emplace_back(std::move(r));
    }
    return out;
}