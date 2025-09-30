//
// Created by Vegim Shala on 14.7.25.
//
#include "operators/binary_window_function_operator2.h"

#include <iostream>

#include "operators/utils/partition_utils2.h"
#include "operators/utils/sort_utils2.h"
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
std::vector<std::pair<PartitionUtils2::PartitionPayload, PartitionUtils2::IndexDataset>>
BinaryWindowFunctionOperator2::build_worklist(
    PartitionUtils2::PartitionPayloadResult &input_payload_partitions,
    PartitionUtils2::PartitionIndexResult   &probe_idx_partitions
) {
    using PP = PartitionUtils2::PartitionPayload;
    std::vector<std::pair<PP, PartitionUtils2::IndexDataset>> worklist;
    worklist.reserve(1024);

    std::visit([&](auto &probe_map) {
        for (auto &kv : probe_map) {
            const auto &key = kv.first;
            const auto &pr_inds = kv.second;
            if (pr_inds.empty()) continue;

            // look up in input payloads by the same key
            if constexpr (std::is_same_v<std::decay_t<decltype(key)>, int32_t>) {
                auto &in_map = std::get<PartitionUtils2::PayloadMap1>(input_payload_partitions);
                auto it = in_map.find(key);
                if (it == in_map.end()) continue;
                worklist.emplace_back(std::move(it->second), pr_inds);
            } else {
                auto &in_map = std::get<PartitionUtils2::PayloadMapN>(input_payload_partitions);
                auto it = in_map.find(key);
                if (it == in_map.end()) continue;
                worklist.emplace_back(std::move(it->second), pr_inds);
            }
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
void BinaryWindowFunctionOperator2::process_worklist(
    std::vector<std::pair<PartitionUtils2::PartitionPayload, PartitionUtils2::IndexDataset>> &worklist,
    Dataset &input, Dataset &probe,
    FileSchema &input_schema, FileSchema &probe_schema,
    Dataset &result, std::mutex &result_mtx,
    ThreadPool &pool, size_t batch_size, size_t morsel_size)
{
    using PP = PartitionUtils2::PartitionPayload;

    for (size_t pos = 0; pos < worklist.size(); pos += batch_size) {
        size_t endpos = std::min(worklist.size(), pos + batch_size);
        std::vector<std::future<void>> batch_futs;
        batch_futs.reserve(endpos - pos);

        for (size_t i = pos; i < endpos; ++i) {
            PP in_payload   = std::move(worklist[i].first);
            auto pr_indices = std::move(worklist[i].second);

            batch_futs.emplace_back(pool.submit([this,
                                                  in_payload = std::move(in_payload),
                                                  pr_indices = std::move(pr_indices),
                                                  &input, &probe, &input_schema, &probe_schema,
                                                  &result, &result_mtx, &pool, morsel_size]() mutable {
                process_partition(std::move(in_payload), std::move(pr_indices),
                                  input, probe, input_schema, probe_schema,
                                  result, result_mtx, pool, morsel_size);
            }));
        }
        for (auto &f : batch_futs) f.get();
    }
}


/**
 * @brief Process a single partition (input + probe indices).
 *
 * Builds a sorted index and segment tree on the input partition,
 * then probes the probe partition using either morsel-parallelism
 * or inline sequential execution depending on size.
 */
void BinaryWindowFunctionOperator2::process_partition(
    PartitionUtils2::PartitionPayload in_payload,        // <-- changed
    PartitionUtils2::IndexDataset pr_indices,            // unchanged
    const Dataset &input, const Dataset &probe,
    const FileSchema &input_schema, const FileSchema &probe_schema,
    Dataset &result, std::mutex &result_mtx,
    ThreadPool &pool, size_t morsel_size) const
{
    if (pr_indices.empty() || in_payload.order_keys.empty()) return;

    // Build a sortable KV buffer
    std::vector<std::pair<uint32_t,uint32_t>> kv;
    kv.reserve(in_payload.order_keys.size());
    for (size_t i = 0; i < in_payload.order_keys.size(); ++i) {
        kv.emplace_back(in_payload.order_keys[i], in_payload.values[i]);
    }

    // Sort by key
    ips2ra::sort(kv.begin(), kv.end(), [](const auto &p){ return p.first; });

    // Split into separate arrays for binary search + index
    std::vector<uint32_t> keys;   keys.reserve(kv.size());
    std::vector<uint32_t> values; values.reserve(kv.size());
    for (auto &p : kv) { keys.push_back(p.first); values.push_back(p.second); }

    // Build segment tree (unchanged API)
    JoinUtils2 local_join(spec.join_spec, spec.order_column);
    local_join.build_index_from_vectors_segtree(keys, values);

    // Probe path unchanged (morsel or inline)
    if (pr_indices.size() >= morsel_size * 2) {
        process_probe_partition_parallel(pr_indices, probe, probe_schema,
                                         keys, local_join, result, result_mtx, pool, morsel_size);
    } else {
        process_probe_partition_inline(pr_indices, probe, probe_schema,
                                       keys, local_join, result, result_mtx);
    }
}

/**
 * @brief Probe a partition in parallel by splitting into morsels.
 */
void BinaryWindowFunctionOperator2::process_probe_partition_parallel(
    const PartitionUtils2::IndexDataset &pr_indices,
    const Dataset &probe,
    const FileSchema &probe_schema,
    const std::vector<uint32_t> &keys,
    JoinUtils2 &local_join,
    Dataset &result,
    std::mutex &result_mtx,
    ThreadPool &pool,
    size_t morsel_size
) const {
    std::vector<std::future<std::vector<DataRow> > > futs; // each future returns a vector of DataRow
    futs.reserve((pr_indices.size() + morsel_size - 1) / morsel_size); // round up to determine number of morsels

    for (size_t mstart = 0; mstart < pr_indices.size(); mstart += morsel_size) {
        // for each morsel
        size_t mend = std::min(mstart + morsel_size, pr_indices.size());

        futs.emplace_back(pool.submit(
            // we use the pool because we also use it for partition batches -> Q1 : hoes does the pool actually handle that
            [this, mstart, mend, &pr_indices, &probe, &probe_schema,
                &keys, &local_join]() -> std::vector<DataRow> {
                return process_probe_morsel(mstart, mend, pr_indices, probe, probe_schema,
                                            keys, local_join);
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

/**
 * @brief Probe a partition sequentially (no extra parallelism).
 */
void BinaryWindowFunctionOperator2::process_probe_partition_inline(
    const PartitionUtils2::IndexDataset &pr_indices,
    const Dataset &probe,
    const FileSchema &probe_schema,
    const std::vector<uint32_t> &keys,
    JoinUtils2 &local_join,
    Dataset &result,
    std::mutex &result_mtx
) const {
    auto local_out = process_probe_morsel(0, pr_indices.size(),
                                          pr_indices, probe, probe_schema,
                                          keys, local_join);
    std::lock_guard<std::mutex> lk(result_mtx); // we lock because we process batches in parallel
    // TODO: Can we somehow make this lock-free?
    std::move(local_out.begin(), local_out.end(), std::back_inserter(result));
}

/**
 * @brief Probe a morsel of probe rows against the input index.
 *
 * For each probe row, computes the aggregate over the range
 * [begin_column, end_column] using binary search + segment tree query.
 *
 * @return A vector of result rows for this morsel.
 */
std::vector<DataRow> BinaryWindowFunctionOperator2::process_probe_morsel(
    size_t mstart, size_t mend,
    const PartitionUtils2::IndexDataset &pr_indices,
    const Dataset &probe, const FileSchema &probe_schema,
    const std::vector<uint32_t> &keys,
    JoinUtils2 &local_join) const
{
    std::vector<DataRow> local_out;
    local_out.reserve(mend - mstart);

    const size_t probe_begin_idx = probe_schema.index_of(spec.join_spec.begin_column);
    const size_t probe_end_idx   = probe_schema.index_of(spec.join_spec.end_column);

    for (size_t j = mstart; j < mend; ++j) {
        size_t probe_idx = pr_indices[j];
        const DataRow &probe_row = probe[probe_idx];

        // IMPORTANT: encode to the same key space as 'keys'
        uint32_t start = PartitionUtils2::encode_key(static_cast<int32_t>(probe_row[probe_begin_idx]));
        uint32_t end   = PartitionUtils2::encode_key(static_cast<int32_t>(probe_row[probe_end_idx]));

        auto lo_it = std::lower_bound(keys.begin(), keys.end(), start);
        auto hi_it = std::upper_bound(keys.begin(), keys.end(), end);
        size_t lo = static_cast<size_t>(lo_it - keys.begin());
        size_t hi = static_cast<size_t>(hi_it - keys.begin());

        if (lo < hi) [[likely]] {
            double agg_sum = local_join.seg_query(lo, hi);
            DataRow out = probe_row;
            out.push_back(agg_sum);
            local_out.emplace_back(std::move(out));
        }
    }
    return local_out;
}



pair<Dataset, FileSchema> BinaryWindowFunctionOperator2::execute(
    Dataset &input, Dataset &probe,
    FileSchema input_schema, FileSchema probe_schema)
{
    Dataset result;
    result.reserve(probe.size());
    std::mutex result_mtx;

    auto total_start = std::chrono::high_resolution_clock::now();

    // Stage 1: Partition input (payloads) and probe (indices)
    auto part_start = std::chrono::high_resolution_clock::now();

    size_t hw = std::thread::hardware_concurrency();
    if (hw == 0) hw = 2;

    auto input_payload_partitions = PartitionUtils2::partition_payloads_parallel(
        input, input_schema, spec.partition_columns, spec.order_column, spec.value_column,
        hw, /*morsel_size*/2048, /*radix_bits*/8);

    auto probe_idx_partitions = PartitionUtils2::partition_indices_parallel(
        probe, probe_schema, spec.partition_columns,
        hw, /*morsel_size*/2048, /*radix_bits*/8);

    auto part_end = std::chrono::high_resolution_clock::now();
    std::cout << "Partitioning wall time: "
              << std::chrono::duration_cast<std::chrono::milliseconds>(part_end - part_start).count()
              << " ms" << std::endl;

    // Stage 2: Thread pool
    auto join_start = std::chrono::high_resolution_clock::now();
    ThreadPool pool(hw);
    size_t batch_size  = std::max<size_t>(1, hw / 2);
    const size_t morsel_size = 256;
    std::cout << "ThreadPool size: " << hw
              << ", partition batch size: " << batch_size
              << ", morsel_size: " << morsel_size << std::endl;

    // Stage 3: Build worklist (payload + probe indices)
    auto worklist = build_worklist(input_payload_partitions, probe_idx_partitions);

    // Stage 4: Process
    if (worklist.size() == 1) {
        std::cout << "Only one partition to process, processing inline." << std::endl;
        auto [in_payload, pr_inds] = std::move(worklist[0]);
        process_partition(std::move(in_payload), std::move(pr_inds),
                          input, probe, input_schema, probe_schema,
                          result, result_mtx, pool, morsel_size);
    } else {
        process_worklist(worklist, input, probe, input_schema, probe_schema,
                         result, result_mtx, pool, batch_size, morsel_size);
    }

    auto join_end = std::chrono::high_resolution_clock::now();
    std::cout << "Join wall time: "
              << std::chrono::duration_cast<std::chrono::milliseconds>(join_end - join_start).count()
              << " ms" << std::endl;

    auto total_end = std::chrono::high_resolution_clock::now();
    std::cout << "Total execute() wall time: "
              << std::chrono::duration_cast<std::chrono::milliseconds>(total_end - total_start).count()
              << " ms" << std::endl;

    // Output schema
    probe_schema.add_column(spec.output_column, "double");
    return {std::move(result), probe_schema};
}



