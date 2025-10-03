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
            // for each partition in batch
            IndexDataset in_inds = std::move(worklist[i].first);
            IndexDataset pr_inds = std::move(worklist[i].second);

            // Pass the partition to the thread pool and process it in parallel
            batch_futs.emplace_back(pool.submit(
                [this, in_inds = std::move(in_inds), pr_inds = std::move(pr_inds),
                    &input, &probe, &input_schema, &probe_schema,
                    &result, &result_mtx, &pool, morsel_size]() mutable {
                    process_partition(std::move(in_inds), std::move(pr_inds),
                                      input, probe, input_schema, probe_schema,
                                      result, result_mtx, pool, morsel_size);
                }));
        }

        // Wait for batch
        for (auto &f: batch_futs) f.get();
    }
}

/**
 * @brief Process a single partition (input + probe indices).
 *
 * Builds a sorted index and segment tree on the input partition,
 * then probes the probe partition using either morsel-parallelism
 * or inline sequential execution depending on size.
 */
void BinaryWindowFunctionOperator::process_partition(
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
    // TODO: Check the possibility for parallel sort if partition is large enough
    //       (or maybe always parallel if we have a thread pool?)
    const size_t order_idx = input_schema.index_of(spec.order_column);
    // size_t threads_for_sort = 0; // default: sequential
    // if (in_indices.size() > 1'000'000) {
    //     // give each partition maybe half the threads
    //     threads_for_sort = std::max<size_t>(1, std::thread::hardware_concurrency() / 2);
    // }

    SortUtils::sort_dataset_indices(input, in_indices, order_idx);

    auto start_building_index = std::chrono::high_resolution_clock::now();
    // 2. Build keys + values arrays
    const size_t value_idx = input_schema.index_of(spec.value_column);

    std::vector<uint32_t> keys, values;
    keys.reserve(in_indices.size());
    values.reserve(in_indices.size());

    for (size_t idx: in_indices) {
        keys.push_back(static_cast<uint32_t>(input[idx][order_idx]));
        values.push_back(static_cast<uint32_t>(input[idx][value_idx]));
    }


    // 3. Build segment tree
    JoinUtils local_join(spec.join_spec, spec.order_column);
    // local_join.build_index_from_vectors_segtree(keys, values);
    local_join.build_index_from_vectors_prefix_sums(keys, values);
    // local_join.build_index_from_vectors_sqrttree(keys, values);

    // local_join.build_eytzinger();

    // auto end_building_index = std::chrono::high_resolution_clock::now();
    // auto duration_building_index = std::chrono::duration_cast<std::chrono::microseconds>(
    //     end_building_index - start_building_index);
    // std::cout << "Time taken for BUILDING INDEX: " << duration_building_index.count() << " ms" << std::endl;

    auto start_partition_processing = std::chrono::high_resolution_clock::now();

    // 4. Probe partition (parallel or inline)
    if (pr_indices.size() >= morsel_size * 2) {
        // TODO: Tune threshold
        process_probe_partition_parallel(pr_indices, probe, probe_schema,
                                         keys, local_join, result, result_mtx, pool, morsel_size);
    } else {
        process_probe_partition_inline(pr_indices, probe, probe_schema,
                                       keys, local_join, result, result_mtx);
    }

    // auto end_partition_processing = std::chrono::high_resolution_clock::now();
    // auto duration_partition_processing = std::chrono::duration_cast<std::chrono::microseconds>(
    //     end_partition_processing - start_partition_processing);
    // std::cout << "Time taken for PARTITION PROCESSING: " << duration_partition_processing.count() << " ms" << std::endl;
}

/**
 * @brief Probe a partition in parallel by splitting into morsels.
 */
void BinaryWindowFunctionOperator::process_probe_partition_parallel(
    const PartitionUtils::IndexDataset &pr_indices,
    const Dataset &probe,
    const FileSchema &probe_schema,
    const std::vector<uint32_t> &keys,
    JoinUtils &local_join,
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
            [this, mstart, mend, &pr_indices, &probe, &probe_schema, &keys, &local_join]() mutable {
                return process_probe_morsel_sort_probe_interleaving(mstart, mend, pr_indices, probe, probe_schema,
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
void BinaryWindowFunctionOperator::process_probe_partition_inline(
    const PartitionUtils::IndexDataset &pr_indices,
    const Dataset &probe,
    const FileSchema &probe_schema,
    const std::vector<uint32_t> &keys,
    JoinUtils &local_join,
    Dataset &result,
    std::mutex &result_mtx
) const {
    auto local_out = process_probe_morsel_sort_probe_interleaving(0, pr_indices.size(),
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
std::vector<DataRow> BinaryWindowFunctionOperator::process_probe_morsel(
    size_t mstart,
    size_t mend,
    const PartitionUtils::IndexDataset &pr_indices,
    const Dataset &probe,
    const FileSchema &probe_schema,
    const std::vector<uint32_t> &keys,
    JoinUtils &local_join
) const {
    std::vector<DataRow> local_out; // each DataRow is probe_row + aggregate value
    local_out.reserve(mend - mstart); // at most one output row per probe row

    // TODO: Maybe re-allow unbounded frames (empty begin/end columns)
    // const bool has_begin = !spec.join_spec.begin_column.empty(); // we allow empty begin/end because of unbounded frames
    // const bool has_end = !spec.join_spec.end_column.empty();
    // size_t probe_begin_idx = has_begin ? probe_schema.index_of(spec.join_spec.begin_column) : SIZE_MAX;
    // size_t probe_end_idx = has_end ? probe_schema.index_of(spec.join_spec.end_column) : SIZE_MAX;

    size_t probe_begin_idx = probe_schema.index_of(spec.join_spec.begin_column);
    size_t probe_end_idx = probe_schema.index_of(spec.join_spec.end_column);

    for (size_t j = mstart; j < mend; ++j) {
        size_t probe_idx = pr_indices[j];
        const DataRow &probe_row = probe[probe_idx];

        // double start = has_begin ? probe_row[probe_begin_idx] : -std::numeric_limits<double>::infinity();
        // double end = has_end ? probe_row[probe_end_idx] : std::numeric_limits<double>::infinity();

        int32_t start = probe_row[probe_begin_idx];
        int32_t end = probe_row[probe_end_idx];

        auto lo_it = std::lower_bound(keys.begin(), keys.end(), start);
        // // lower_bound calculates the first element >= start.
        auto hi_it = std::upper_bound(keys.begin(), keys.end(), end); // upper_bound calculates the first element > end.
        size_t lo = lo_it - keys.begin(); // the index of the first element >= start
        size_t hi = hi_it - keys.begin(); // the index of the first element > end

        // size_t lo = local_join.eyt_lower(start);
        // size_t hi = local_join.eyt_upper(end);

        if (lo < hi) [[likely]] {
            // TODO: Check if we include lo == hi case (empty range), but if there's a row with exact match in the input, it should be included right?
            // uint64_t agg_sum = local_join.seg_query(lo, hi);
            uint64_t agg_sum = local_join.prefix_sums_query(lo, hi);
            // uint64_t agg_sum = local_join.sqrt_query(lo, hi);
            DataRow out = probe_row;
            out.push_back(agg_sum);
            local_out.emplace_back(std::move(out));
        }
    }
    return local_out;
}


std::vector<DataRow> BinaryWindowFunctionOperator::process_probe_morsel_sort_probe(
    size_t mstart,
    size_t mend,
    const PartitionUtils::IndexDataset &pr_indices,
    const Dataset &probe,
    const FileSchema &probe_schema,
    const std::vector<uint32_t> &keys,
    JoinUtils &local_join
) const {
    auto start_prepare_tasks = std::chrono::high_resolution_clock::now();
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

    // auto end_prepare_tasks = std::chrono::high_resolution_clock::now();
    // auto duration_prepare_tasks = std::chrono::duration_cast<std::chrono::nanoseconds>(
    //     end_prepare_tasks - start_prepare_tasks);
    // std::cout << "Time taken for PREPARING TASKS: " << duration_prepare_tasks.count() << " ns" << std::endl;

    auto start_sorting_tasks = std::chrono::high_resolution_clock::now();

    // ---- 2. Sort tasks by start (cache-friendly probing order) ----
#ifdef NDEBUG
    // ---------- FAST PATH (Release build with ips2ra) ----------
    ips2ra::sort(tasks.begin(), tasks.end(),
                 [](const ProbeTask &t) {
                     return static_cast<uint32_t>(t.start) ^ (1UL << 31);
                 });
#else
    // ---------- SAFE PATH (Debug build with std::sort) ----------
    std::sort(tasks.begin(), tasks.end(),
              [](const ProbeTask &lhs, const ProbeTask &rhs) {
                  uint32_t key_lhs = static_cast<uint32_t>(lhs.start) ^ (1UL << 31);
                  uint32_t key_rhs = static_cast<uint32_t>(rhs.start) ^ (1UL << 31);
                  return key_lhs < key_rhs;
              });
#endif

    // auto end_sorting_tasks = std::chrono::high_resolution_clock::now();
    // auto duration_sorting_tasks = std::chrono::duration_cast<std::chrono::nanoseconds>(
    //     end_sorting_tasks - start_sorting_tasks);
    // std::cout << "Time taken for SORTING TASKS: " << duration_sorting_tasks.count() << " ns" << std::endl;

    auto start_probing = std::chrono::high_resolution_clock::now();

    // ---- 3. Probe in sorted order ----
    std::vector<uint64_t> agg(mend - mstart); // results in original local order
    // JoinUtils::RangeCache cache; // thread-local cache

    // auto gallop_forward = [&](size_t pos, uint32_t target, bool upper) {
    //     // Exponential search forward from pos, then binary refine.
    //     // Precondition for lower: keys[pos] < target (we need >= target).
    //     // Precondition for upper: keys[pos] <= target (we need > target).
    //     size_t n = keys.size();
    //     size_t cur = pos;
    //     size_t step = 1;
    //
    //     if (upper) {
    //         // find first index > target
    //         while (cur + step < n && keys[cur + step] <= target) {
    //             cur += step;
    //             step <<= 1;
    //         }
    //         size_t left = cur + 1;
    //         size_t right = std::min(n, cur + step + 1);
    //         return std::upper_bound(keys.begin() + left, keys.begin() + right, target) - keys.begin();
    //         // return local_join.eyt_upper(target);
    //     } else {
    //         // find first index >= target
    //         while (cur + step < n && keys[cur + step] < target) {
    //             cur += step;
    //             step <<= 1;
    //         }
    //         size_t left = cur + 1;
    //         size_t right = std::min(n, cur + step + 1);
    //         return std::lower_bound(keys.begin() + left, keys.begin() + right, target) - keys.begin();
    //         // return local_join.eyt_lower(target);
    //     }
    // };

    // size_t lo = 0, hi = 0;
    // int32_t prev_start = std::numeric_limits<int32_t>::min();
    // int32_t prev_end   = std::numeric_limits<int32_t>::min();
    //
    // for (size_t i = 0; i < tasks.size(); i++) {
    //     const auto &task = tasks[i];
    //
    //     if (i == 0) {
    //         // First probe → regular Eytzinger search
    //         lo = local_join.eyt_lower(task.start);
    //         hi = local_join.eyt_upper(task.end);
    //     } else {
    //         // Advance lo only forward
    //         if (task.start > prev_start) {
    //             lo = local_join.eyt_gallop_lower(lo, task.start);
    //         }
    //         // else { // we don't move lo backwards
    //         //     // range shrank → bounded refine
    //         //     lo = local_join.bounded_lower_bound(lo, hi, task.start);
    //         // }
    //
    //         // Advance hi
    //         if (task.end >= prev_end) {
    //             hi = local_join.eyt_gallop_upper(hi, task.end);
    //         } else {
    //             // range shrank → bounded refine
    //             hi = local_join.bounded_upper_bound(lo, hi, task.end);
    //         }
    //     }
    //
    //     uint64_t sum = 0;
    //     if (lo < hi) {
    //         sum = local_join.prefix_sums_query(lo, hi);
    //         // or seg_query(lo, hi) / sqrt_query(lo, hi)
    //     }
    //
    //     agg[task.orig_pos] = sum;
    //     prev_start = task.start;
    //     prev_end   = task.end;
    // }

    for (const auto &task: tasks) {
        // size_t lo = local_join.branchless_lower_bound(keys, task.start);
        // size_t hi = local_join.branchless_upper_bound(keys, task.end);

        // size_t lo = local_join.eyt_lower(task.start);
        // size_t hi = local_join.eyt_upper(task.end);

        // size_t lo = local_join.bucket_lower(task.start);
        // size_t hi = local_join.bucket_upper(task.end);

        // advance lo until keys[lo] >= task.start
        // while (lo < nkeys && keys[lo] < task.start) {
        //     lo++;
        // }
        // // advance hi until keys[hi] > task.end
        // while (hi < nkeys && keys[hi] <= task.end) {
        //     hi++;
        // }

        auto [lo, hi] = local_join.safe_bounds(task.start, task.end);

        uint64_t sum = 0;
        if (lo < hi) {
            // if (!cache.lookup(lo, hi, sum)) {
            //     sum = local_join.seg_query(lo, hi);
            //     cache.insert(lo, hi, sum);
            // }
            // auto start_query = std::chrono::high_resolution_clock::now();
            // sum = local_join.seg_query(lo, hi);
            sum = local_join.prefix_sums_query(lo, hi);
            // sum = local_join.sqrt_query(lo, hi);

            // auto end_query = std::chrono::high_resolution_clock::now();
            // auto duration_query = std::chrono::duration_cast<std::chrono::nanoseconds>(
            //     end_query - start_query);
            // std::cout << "Time taken for one QUERY: " << duration_query.count() << " ns" << std::endl;
        }

        agg[task.orig_pos] = sum; // scatter back to original order
    }

    // auto end_probing = std::chrono::high_resolution_clock::now();
    // auto duration_probing = std::chrono::duration_cast<std::chrono::nanoseconds>(
    //     end_probing - start_probing);
    // std::cout << "Time taken for PROBING: " << duration_probing.count() << " ns" << std::endl;

    auto start_assembling = std::chrono::high_resolution_clock::now();

    // ---- 4. Assemble DataRows in original order ----
    std::vector<DataRow> local_out;
    local_out.reserve(mend - mstart);

    for (size_t j = mstart; j < mend; ++j) {
        size_t probe_idx = pr_indices[j];
        const DataRow &probe_row = probe[probe_idx];

        DataRow out = probe_row;
        out.push_back(agg[j - mstart]);
        local_out.emplace_back(std::move(out));
    }

    // auto end_assembling = std::chrono::high_resolution_clock::now();
    // auto duration_assembling = std::chrono::duration_cast<std::chrono::nanoseconds>(
    //     end_assembling - start_assembling);
    // std::cout << "Time taken for ASSEMBLING: " << duration_assembling.count() << " ns" << std::endl;

    return local_out;
}


std::vector<DataRow> BinaryWindowFunctionOperator::process_probe_morsel_sort_probe_interleaving(
    size_t mstart,
    size_t mend,
    const PartitionUtils::IndexDataset &pr_indices,
    const Dataset &probe,
    const FileSchema &probe_schema,
    const std::vector<uint32_t> &keys,
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

    // ---- 2. Sort tasks by start (cache-friendly probing order) ----
#ifdef NDEBUG
    // ---------- FAST PATH (Release build with ips2ra) ----------
    ips2ra::sort(tasks.begin(), tasks.end(),
                 [](const ProbeTask &t) {
                     return static_cast<uint32_t>(t.start) ^ (1UL << 31);
                 });
#else
    // ---------- SAFE PATH (Debug build with std::sort) ----------
    std::sort(tasks.begin(), tasks.end(),
              [](const ProbeTask &lhs, const ProbeTask &rhs) {
                  uint32_t key_lhs = static_cast<uint32_t>(lhs.start) ^ (1UL << 31);
                  uint32_t key_rhs = static_cast<uint32_t>(rhs.start) ^ (1UL << 31);
                  return key_lhs < key_rhs;
              });
#endif

    auto start_probing = std::chrono::high_resolution_clock::now();

    // ---- 2.5: Extract probe keys ---
    std::vector<uint32_t> start_keys, end_keys;
    start_keys.reserve(tasks.size());
    end_keys.reserve(tasks.size());

    for (auto &t : tasks) {
        start_keys.push_back(static_cast<uint32_t>(t.start));
        end_keys.push_back(static_cast<uint32_t>(t.end));
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

    //TODO: Evaluate and tune partition morsel size
    size_t partition_morsel_size = std::exp2(static_cast<size_t>(std::floor(std::log2(input.size() / 32))));
    // std::cout << "Partition morsel size: " << partition_morsel_size << std::endl;

    // Stage 1: Pre-partition input and probe datasets
    auto part_start = std::chrono::high_resolution_clock::now();
    auto input_idx_partitions = PartitionUtils::partition_indices_parallel(
        input, input_schema, spec.partition_columns,
        std::thread::hardware_concurrency(), partition_morsel_size, 8
    );
    auto probe_idx_partitions = PartitionUtils::partition_indices_parallel(
        probe, probe_schema, spec.partition_columns,
        std::thread::hardware_concurrency(), partition_morsel_size, 8
    );
    auto part_end = std::chrono::high_resolution_clock::now();
    std::cout << "Partitioning wall time: "
            << std::chrono::duration_cast<std::chrono::microseconds>(part_end - part_start).count()
            << " ms" << std::endl;


    // Stage 2: Setup thread pool that we'll use to process partitions in batches, and morsels within partitions
    auto join_start = std::chrono::high_resolution_clock::now();
    size_t pool_size = std::thread::hardware_concurrency();
    if (pool_size == 0) pool_size = 2;
    ThreadPool pool(pool_size);

    size_t batch_size = std::max<size_t>(1, pool_size / 2); // leave threads for morsels
    const size_t morsel_size = 2048; // tuneable
    // std::cout << "ThreadPool size: " << pool_size
    //         << ", partition batch size: " << batch_size
    //         << ", morsel_size: " << morsel_size << std::endl;

    // Stage 3: Build the worklist => the partitions to process
    auto worklist = build_worklist(input_idx_partitions, probe_idx_partitions);

    // std::cout << "build_worklist: " << std::endl;

    // Stage 4: Process the partitions in parallel using the thread pool
    if (worklist.size() == 1) {
        // cout << "Only one partition to process, processing inline." << endl;
        auto [in_inds, pr_inds] = std::move(worklist[0]);
        process_partition(std::move(in_inds), std::move(pr_inds),
                          input, probe, input_schema, probe_schema,
                          result, result_mtx, pool, morsel_size);
        // return
    } else {
        process_worklist(worklist, input, probe, input_schema, probe_schema,
                         result, result_mtx, pool, batch_size, partition_morsel_size); // current path
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
    result.reserve(probe.size()); // same as parallel
    std::mutex result_mtx; // unused in sequential, but keep for interface symmetry

    auto total_start = std::chrono::high_resolution_clock::now();

    // auto start_precompute = std::chrono::high_resolution_clock::now();
    //
    // // Precompute global keys for ordering
    // global_keys.reserve(input.size());
    //
    // const size_t order_idx = input_schema.index_of(spec.order_column);
    // for (size_t row_id = 0; row_id < input.size(); ++row_id) {
    //     global_keys[row_id] = static_cast<uint32_t>(input[row_id][order_idx]) ^ (1UL << 31);
    // }
    //
    // auto end_precompute = std::chrono::high_resolution_clock::now();
    // std::cout << "[SEQ] Precompute global keys wall time: "
    //         << std::chrono::duration_cast<std::chrono::milliseconds>(end_precompute - start_precompute).count()
    //         << " ms" << std::endl;

    // Stage 1: Pre-partition input and probe datasets
    auto part_start = std::chrono::high_resolution_clock::now();
    auto input_idx_partitions = PartitionUtils::partition_indices_parallel(
        input, input_schema, spec.partition_columns,
        1, 2048, 8 // force 1 thread since sequential
    );
    auto probe_idx_partitions = PartitionUtils::partition_indices_parallel(
        probe, probe_schema, spec.partition_columns,
        1, 2048, 8
    );
    auto part_end = std::chrono::high_resolution_clock::now();
    std::cout << "[SEQ] Partitioning wall time: "
            << std::chrono::duration_cast<std::chrono::milliseconds>(part_end - part_start).count()
            << " ms" << std::endl;

    // Stage 2: Build the worklist => the partitions to process
    auto join_start = std::chrono::high_resolution_clock::now();
    auto worklist = build_worklist(input_idx_partitions, probe_idx_partitions);

    // Stage 3: Process the partitions sequentially
    process_worklist_sequential(worklist, input, probe,
                                input_schema, probe_schema,
                                result, result_mtx);

    auto join_end = std::chrono::high_resolution_clock::now();
    std::cout << "[SEQ] Join wall time: "
            << std::chrono::duration_cast<std::chrono::milliseconds>(join_end - join_start).count()
            << " ms" << std::endl;

    auto total_end = std::chrono::high_resolution_clock::now();
    std::cout << "[SEQ] Total execute() wall time: "
            << std::chrono::duration_cast<std::chrono::milliseconds>(total_end - total_start).count()
            << " ms" << std::endl;

    // Stage 4: Append output column to probe schema
    probe_schema.add_column(spec.output_column, "double");
    return {std::move(result), probe_schema};
}

/**
 * @brief Sequential variant of process_worklist (no thread pool).
 *
 * Iterates partitions in the worklist and processes them one by one.
 * This avoids any oversubscription and makes it easier to debug.
 */
void BinaryWindowFunctionOperator::process_worklist_sequential(
    std::vector<std::pair<PartitionUtils::IndexDataset, PartitionUtils::IndexDataset> > &worklist,
    Dataset &input,
    Dataset &probe,
    FileSchema &input_schema,
    FileSchema &probe_schema,
    Dataset &result,
    std::mutex &result_mtx // unused, but kept for signature symmetry
) const {
    for (auto &pair: worklist) {
        PartitionUtils::IndexDataset in_inds = std::move(pair.first);
        PartitionUtils::IndexDataset pr_inds = std::move(pair.second);

        // Call sequential partition processor
        process_partition_sequential(std::move(in_inds), std::move(pr_inds),
                                     input, probe, input_schema, probe_schema,
                                     result);
    }
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
    // Stage 1: Sort input partition by order column
    const size_t order_idx = input_schema.index_of(spec.order_column);
    SortUtils::sort_dataset_indices(input, in_indices, order_idx);
    // SortUtils::sort_dataset_global_keys(global_keys, in_indices);

    auto start_building_index = std::chrono::high_resolution_clock::now();

    // Stage 2: Build keys + values arrays
    const size_t value_idx = input_schema.index_of(spec.value_column);

    std::vector<uint32_t> keys, values;
    keys.reserve(in_indices.size());
    values.reserve(in_indices.size());

    for (size_t idx: in_indices) {
        keys.emplace_back(static_cast<uint32_t>(input[idx][order_idx]));
        values.emplace_back(static_cast<uint32_t>(input[idx][value_idx]));
    }

    // Stage 3: Build segment tree
    JoinUtils local_join(spec.join_spec, spec.order_column);
    local_join.build_index_from_vectors_segtree(keys, values);

    // auto end_building_index = std::chrono::high_resolution_clock::now();
    // auto duration_building_index = std::chrono::duration_cast<std::chrono::milliseconds>(
    //     end_building_index - start_building_index);
    // std::cout << "Time taken for BUILDING INDEX: " << duration_building_index.count() << " ms" << std::endl;

    auto start_partition_process = std::chrono::high_resolution_clock::now();
    // Stage 4: Probe partition sequentially
    process_probe_partition_inline_sequential(pr_indices, probe, probe_schema,
                                              keys, local_join, result,
                                              *(new std::mutex())); // dummy mutex, never contended

    // auto end_partition_process = std::chrono::high_resolution_clock::now();
    // auto duration_partition_process = std::chrono::duration_cast<std::chrono::milliseconds>(
    //     end_partition_process - start_partition_process);
    // std::cout << "Time taken for PARTITION PROCESSING: " << duration_partition_process.count() << " ms" << std::endl;
}

void BinaryWindowFunctionOperator::process_probe_partition_inline_sequential(
    const PartitionUtils::IndexDataset &pr_indices,
    const Dataset &probe,
    const FileSchema &probe_schema,
    const std::vector<uint32_t> &keys,
    JoinUtils &local_join,
    Dataset &result,
    std::mutex &result_mtx
) const {
    size_t old_size = result.size();
    result.reserve(old_size + pr_indices.size()); // ensure enough space

    size_t probe_begin_idx = probe_schema.index_of(spec.join_spec.begin_column);
    size_t probe_end_idx = probe_schema.index_of(spec.join_spec.end_column);

    for (size_t j = 0; j < pr_indices.size(); ++j) {
        size_t probe_idx = pr_indices[j];
        const DataRow &probe_row = probe[probe_idx];

        int32_t start = probe_row[probe_begin_idx];
        int32_t end = probe_row[probe_end_idx];

        auto lo_it = std::lower_bound(keys.begin(), keys.end(), start);
        auto hi_it = std::upper_bound(keys.begin(), keys.end(), end);

        size_t lo = lo_it - keys.begin();
        size_t hi = hi_it - keys.begin();

        if (lo < hi) [[likely]] {
            uint64_t agg_sum = local_join.seg_query(lo, hi);
            DataRow out = probe_row;
            out.emplace_back(agg_sum);
            result.emplace_back(std::move(out));
        }
    }
}


// pair<Dataset, FileSchema> BinaryWindowFunctionOperator::execute2(
//     Dataset &input,
//     Dataset &probe,
//     FileSchema input_schema,
//     FileSchema probe_schema
// ) {
//     Dataset result;
//     result.reserve(probe.size());
//
//     auto total_start = std::chrono::high_resolution_clock::now();
//
//     // -------------------------
//     // 1. Sort input by partition + order (DuckDB does this once globally)
//     // -------------------------
//     std::vector<size_t> input_idx(input.size());
//     std::iota(input_idx.begin(), input_idx.end(), 0);
//
//     auto order_cols = spec.partition_columns;
//     order_cols.push_back(spec.order_column);
//
//     std::sort(input_idx.begin(), input_idx.end(),
//               [&](size_t a, size_t b) {
//                   for (auto &col: order_cols) {
//                       size_t col_idx = input_schema.index_of(col);
//                       auto va = extract_numeric(input[a][col_idx]);
//                       auto vb = extract_numeric(input[b][col_idx]);
//                       if (va < vb) return true;
//                       if (va > vb) return false;
//                   }
//                   return false;
//               });
//
//     // Pre-extract numeric arrays for efficiency
//     const size_t order_idx = input_schema.index_of(spec.order_column);
//     const size_t value_idx = input_schema.index_of(spec.value_column);
//
//     std::vector<double> order_vals(input.size());
//     std::vector<double> value_vals(input.size());
//     for (size_t i = 0; i < input_idx.size(); i++) {
//         order_vals[i] = extract_numeric(input[input_idx[i]][order_idx]);
//         value_vals[i] = extract_numeric(input[input_idx[i]][value_idx]);
//     }
//
//     // -------------------------
//     // 2. For each probe row, compute its frame [lo, hi)
//     // -------------------------
//     const bool has_begin = !spec.join_spec.begin_column.empty();
//     const bool has_end = !spec.join_spec.end_column.empty();
//     size_t probe_begin_idx = has_begin ? probe_schema.index_of(spec.join_spec.begin_column) : SIZE_MAX;
//     size_t probe_end_idx = has_end ? probe_schema.index_of(spec.join_spec.end_column) : SIZE_MAX;
//
//     for (const auto &probe_row: probe) {
//         double start = has_begin
//                            ? extract_numeric(probe_row[probe_begin_idx])
//                            : -std::numeric_limits<double>::infinity();
//         double end = has_end ? extract_numeric(probe_row[probe_end_idx]) : std::numeric_limits<double>::infinity();
//
//         auto lo_it = std::lower_bound(order_vals.begin(), order_vals.end(), start);
//         auto hi_it = std::upper_bound(order_vals.begin(), order_vals.end(), end);
//         size_t lo = lo_it - order_vals.begin();
//         size_t hi = hi_it - order_vals.begin();
//
//         double agg_sum = 0.0;
//         for (size_t i = lo; i < hi; i++) {
//             agg_sum += value_vals[i];
//         }
//
//         DataRow out = probe_row;
//         out.push_back(agg_sum);
//         result.emplace_back(std::move(out));
//     }
//
//     auto total_end = std::chrono::high_resolution_clock::now();
//     std::cout << "Total execute() wall time (DuckDB-style sequential): "
//             << std::chrono::duration_cast<std::chrono::milliseconds>(total_end - total_start).count()
//             << " ms" << std::endl;
//
//     probe_schema.add_column(spec.output_column, "double");
//     return {std::move(result), probe_schema};
// }
//
// pair<Dataset, FileSchema> BinaryWindowFunctionOperator::execute3(
//     Dataset &input,
//     Dataset &probe,
//     FileSchema input_schema,
//     FileSchema probe_schema
// ) {
//     Dataset result;
//     result.reserve(probe.size());
//     std::mutex result_mtx;
//
//     auto total_start = std::chrono::high_resolution_clock::now();
//
//     // -------------------------
//     // 1. Global sort on input
//     // -------------------------
//     std::vector<size_t> input_idx(input.size());
//     std::iota(input_idx.begin(), input_idx.end(), 0);
//
//     // order columns = partition_columns + order_column
//     auto order_cols = spec.partition_columns;
//     order_cols.push_back(spec.order_column);
//
//     // get column indices once
//     std::vector<size_t> order_col_indices;
//     for (auto &col: order_cols) {
//         order_col_indices.push_back(input_schema.index_of(col));
//     }
//
//     // parallel sort rows in place
//     ips2ra::parallel::sort(input.begin(), input.end(),
//                            [&](const DataRow &row) {
//                                // composite key -> hash it into size_t
//                                size_t h = 0;
//                                for (auto col_idx: order_col_indices) {
//                                    auto v = static_cast<size_t>(extract_numeric(row[col_idx]));
//                                    h ^= std::hash<size_t>{}(v) + 0x9e3779b97f4a7c15ULL + (h << 6) + (h >> 2);
//                                }
//                                return h;
//                            },
//                            std::thread::hardware_concurrency() // num threads
//     );
//
//     const size_t order_idx = input_schema.index_of(spec.order_column);
//     const size_t value_idx = input_schema.index_of(spec.value_column);
//
//     std::vector<double> order_vals(input.size());
//     std::vector<double> value_vals(input.size());
//     for (size_t i = 0; i < input_idx.size(); i++) {
//         order_vals[i] = extract_numeric(input[input_idx[i]][order_idx]);
//         value_vals[i] = extract_numeric(input[input_idx[i]][value_idx]);
//     }
//
//     // -------------------------
//     // 2. Parallel probe processing
//     // -------------------------
//     const bool has_begin = !spec.join_spec.begin_column.empty();
//     const bool has_end = !spec.join_spec.end_column.empty();
//     size_t probe_begin_idx = has_begin ? probe_schema.index_of(spec.join_spec.begin_column) : SIZE_MAX;
//     size_t probe_end_idx = has_end ? probe_schema.index_of(spec.join_spec.end_column) : SIZE_MAX;
//
//     size_t pool_size = std::thread::hardware_concurrency();
//     if (pool_size == 0) pool_size = 2;
//     ThreadPool pool(pool_size);
//
//     size_t morsel_size = 2048; // tune this
//     std::vector<std::future<std::vector<DataRow> > > futs;
//
//     for (size_t mstart = 0; mstart < probe.size(); mstart += morsel_size) {
//         size_t mend = std::min(mstart + morsel_size, probe.size());
//
//         futs.emplace_back(pool.submit([&, mstart, mend]() -> std::vector<DataRow> {
//             std::vector<DataRow> local_out;
//             local_out.reserve(mend - mstart);
//
//             for (size_t i = mstart; i < mend; i++) {
//                 const DataRow &probe_row = probe[i];
//                 double start = has_begin
//                                    ? extract_numeric(probe_row[probe_begin_idx])
//                                    : -std::numeric_limits<double>::infinity();
//                 double end = has_end
//                                  ? extract_numeric(probe_row[probe_end_idx])
//                                  : std::numeric_limits<double>::infinity();
//
//                 auto lo_it = std::lower_bound(order_vals.begin(), order_vals.end(), start);
//                 auto hi_it = std::upper_bound(order_vals.begin(), order_vals.end(), end);
//                 size_t lo = lo_it - order_vals.begin();
//                 size_t hi = hi_it - order_vals.begin();
//
//                 double agg_sum = 0.0;
//                 for (size_t j = lo; j < hi; j++) {
//                     agg_sum += value_vals[j];
//                 }
//
//                 DataRow out = probe_row;
//                 out.push_back(agg_sum);
//                 local_out.emplace_back(std::move(out));
//             }
//
//             return local_out;
//         }));
//     }
//
//     for (auto &f: futs) {
//         auto local_res = f.get();
//         if (!local_res.empty()) {
//             std::lock_guard<std::mutex> lk(result_mtx);
//             std::move(local_res.begin(), local_res.end(), std::back_inserter(result));
//         }
//     }
//
//     auto total_end = std::chrono::high_resolution_clock::now();
//     std::cout << "Total execute() wall time (DuckDB-style parallel): "
//             << std::chrono::duration_cast<std::chrono::milliseconds>(total_end - total_start).count()
//             << " ms" << std::endl;
//
//     probe_schema.add_column(spec.output_column, "double");
//     return {std::move(result), probe_schema};
// }
//
//
// pair<Dataset, FileSchema> BinaryWindowFunctionOperator::execute4(
//     Dataset &input,
//     Dataset &probe,
//     FileSchema input_schema,
//     FileSchema probe_schema
// ) {
//     Dataset result;
//     result.reserve(probe.size()); // rough reservation
//     std::mutex result_mtx;
//
//     using clk = std::chrono::high_resolution_clock;
//     auto total_start = clk::now();
//
//     // -------------------------
//     // quick index lookups
//     // -------------------------
//     const bool has_begin = !spec.join_spec.begin_column.empty();
//     const bool has_end = !spec.join_spec.end_column.empty();
//     const size_t probe_begin_idx = has_begin ? probe_schema.index_of(spec.join_spec.begin_column) : SIZE_MAX;
//     const size_t probe_end_idx = has_end ? probe_schema.index_of(spec.join_spec.end_column) : SIZE_MAX;
//     const size_t input_ts_idx = input_schema.index_of(spec.order_column); // timestamp col on input
//     const size_t input_value_idx = input_schema.index_of(spec.value_column); // value col on input
//     const size_t probe_cat_idx = probe_schema.index_of(spec.partition_columns.front());
//     // assume single partition col: category
//     const size_t input_cat_idx = input_schema.index_of(spec.partition_columns.front());
//
//     // Note: for multi-column partitioning you would need a compound key. For clarity, this matches your SQL (category).
//     // -------------------------
//     // 1) Build probe-side hash: category -> vector of (begin,end, group_id)
//     //    Also build mapping group_id -> key triple (category,begin,end)
//     // -------------------------
//     auto probe_build_start = clk::now();
//
//     struct RangeEntry {
//         int32_t begin;
//         int32_t end;
//         size_t group_id;
//     };
//
//     // category -> vector<RangeEntry>
//     std::unordered_map<int32_t, std::vector<RangeEntry> > cat_ranges;
//     // group_id -> tuple(category, begin, end)
//     struct GroupKey {
//         int32_t category, begin, end;
//     };
//     std::vector<GroupKey> group_keys;
//     group_keys.reserve(probe.size());
//
//     // helper: map exact (category,begin,end) to group_id to remove duplicates
//     struct Triple {
//         int32_t c, b, e;
//         bool operator==(Triple const &o) const { return c == o.c && b == o.b && e == o.e; }
//     };
//     struct TripleHash {
//         size_t operator()(Triple const &t) const noexcept {
//             // combine three int32s into size_t hash
//             uint64_t x = (static_cast<uint64_t>(static_cast<uint32_t>(t.c)) << 32)
//                          ^ (static_cast<uint64_t>(static_cast<uint32_t>(t.b)));
//             return std::hash<uint64_t>()(x) ^ (static_cast<uint32_t>(t.e) * 0x9e3779b97f4a7c15ULL);
//         }
//     };
//     std::unordered_map<Triple, size_t, TripleHash> triple_to_group;
//
//     for (size_t pi = 0; pi < probe.size(); ++pi) {
//         int32_t cat = static_cast<int32_t>(extract_numeric(probe[pi][probe_cat_idx]));
//         int32_t b = has_begin
//                         ? static_cast<int32_t>(extract_numeric(probe[pi][probe_begin_idx]))
//                         : std::numeric_limits<int32_t>::min();
//         int32_t e = has_end
//                         ? static_cast<int32_t>(extract_numeric(probe[pi][probe_end_idx]))
//                         : std::numeric_limits<int32_t>::max();
//
//         Triple t{cat, b, e};
//         auto itg = triple_to_group.find(t);
//         size_t gid;
//         if (itg == triple_to_group.end()) {
//             gid = group_keys.size();
//             group_keys.push_back({cat, b, e});
//             triple_to_group.emplace(t, gid);
//             cat_ranges[cat].push_back(RangeEntry{b, e, gid});
//         } else {
//             gid = itg->second;
//             // If duplicate probe rows (same triple) exist we still only need one group entry
//         }
//     }
//
//     size_t num_groups = group_keys.size();
//     auto probe_build_end = clk::now();
//     std::cout << "Probe-hash build wall time: "
//             << std::chrono::duration_cast<std::chrono::milliseconds>(probe_build_end - probe_build_start).count()
//             << " ms; unique groups: " << num_groups << std::endl;
//
//     if (num_groups == 1) {
//         // single probe optimization
//         const auto &gk = group_keys[0];
//         int64_t agg = 0;
//         bool found = false;
//
//         for (size_t i = 0; i < input.size(); i++) {
//             int32_t cat = (int32_t) extract_numeric(input[i][input_cat_idx]);
//             if (cat != gk.category) continue;
//
//             int32_t ts = (int32_t) extract_numeric(input[i][input_ts_idx]);
//             if (ts < gk.begin || ts > gk.end) continue;
//
//             int32_t val = (int32_t) extract_numeric(input[i][input_value_idx]);
//             agg += val;
//             found = true;
//         }
//
//         // output
//         DataRow out;
//         out.push_back((double) gk.category);
//         out.push_back((double) gk.begin);
//         out.push_back((double) gk.end);
//         out.push_back(found ? (double) agg : std::numeric_limits<double>::quiet_NaN());
//         result.push_back(std::move(out));
//
//         probe_schema.add_column(spec.output_column, "double");
//         return {std::move(result), probe_schema};
//     }
//
//
//     // -------------------------
//     // 2) Morsel-parallel scan of input:
//     //    For each input row, lookup cat_ranges[category] (if present) and test timestamp in [begin,end].
//     //    Each morsel returns local unordered_map<group_id, int64_t> of partial sums.
//     // -------------------------
//     auto scan_start = clk::now();
//
//     size_t pool_size = std::thread::hardware_concurrency();
//     if (pool_size == 0) pool_size = 2;
//     ThreadPool pool(pool_size);
//     const size_t morsel_size = 16384; // tuneable - larger morsel reduces task overhead for huge input
//     std::vector<std::future<std::unordered_map<size_t, int64_t> > > futs;
//
//     // divide input into contiguous morsels (index-based)
//     for (size_t mstart = 0; mstart < input.size(); mstart += morsel_size) {
//         size_t mend = std::min(mstart + morsel_size, input.size());
//         futs.emplace_back(pool.submit(
//             [mstart, mend, &input, input_ts_idx, input_value_idx, input_cat_idx, &cat_ranges
//             ]() -> std::unordered_map<size_t, int64_t> {
//                 std::unordered_map<size_t, int64_t> local_agg;
//                 local_agg.reserve(1024);
//
//                 for (size_t i = mstart; i < mend; ++i) {
//                     int32_t cat = static_cast<int32_t>(extract_numeric(input[i][input_cat_idx]));
//                     auto rit = cat_ranges.find(cat);
//                     if (rit == cat_ranges.end()) continue; // no probes for this category
//
//                     int32_t ts = static_cast<int32_t>(extract_numeric(input[i][input_ts_idx]));
//                     int32_t val = static_cast<int32_t>(extract_numeric(input[i][input_value_idx]));
//
//                     // linear scan of ranges for this category (simple & duckdb-like)
//                     auto &vec = rit->second;
//                     for (const RangeEntry &re: vec) {
//                         if ((uint32_t) (re.begin) <= (uint32_t) (ts) && (uint32_t) (ts) <= (uint32_t) (re.end)) {
//                             local_agg[re.group_id] += static_cast<int64_t>(val);
//                         }
//                     }
//                 }
//
//                 return local_agg;
//             }));
//     }
//
//     // collect all futures and merge into global_agg
//     std::unordered_map<size_t, int64_t> global_agg;
//     global_agg.reserve(num_groups ? std::min<size_t>(num_groups, 1 << 20) : 0);
//
//     for (auto &f: futs) {
//         auto local = f.get();
//         for (auto &kv: local) {
//             global_agg[kv.first] += kv.second;
//         }
//     }
//
//     auto scan_end = clk::now();
//     std::cout << "Input scan + local aggregation wall time: "
//             << std::chrono::duration_cast<std::chrono::milliseconds>(scan_end - scan_start).count()
//             << " ms" << std::endl;
//
//     // -------------------------
//     // 3) Merge is already done above (we merged as futures finished). If you want per-thread merge profiling:
//     // -------------------------
//     // (Already implicit) — print merged size
//     std::cout << "Merged groups with non-zero sum: " << global_agg.size() << std::endl;
//
//     // -------------------------
//     // 4) Assemble final result: one row per group (category, begin, end, SUM)
//     //    If a group does not exist in global_agg => no matching input rows => SQL SUM(NULL).
//     //    We use NaN to represent NULL here. Change to -1.0 if you prefer.
//     // -------------------------
//     auto assemble_start = clk::now();
//
//     for (size_t gid = 0; gid < group_keys.size(); ++gid) {
//         const GroupKey &gk = group_keys[gid];
//         DataRow out;
//         // push category, begin, end as numeric values (maintain same types you used elsewhere)
//         out.push_back(static_cast<double>(gk.category));
//         out.push_back(static_cast<double>(gk.begin));
//         out.push_back(static_cast<double>(gk.end));
//
//         auto it = global_agg.find(gid);
//         double sum_out;
//         if (it == global_agg.end()) {
//             // no matching input rows -> SQL SUM = NULL -> represent as NaN
//             sum_out = std::numeric_limits<double>::quiet_NaN();
//         } else {
//             sum_out = static_cast<double>(it->second);
//         }
//
//         out.push_back(sum_out);
//         result.emplace_back(std::move(out));
//     }
//
//     auto assemble_end = clk::now();
//     std::cout << "Assemble result wall time: "
//             << std::chrono::duration_cast<std::chrono::milliseconds>(assemble_end - assemble_start).count()
//             << " ms" << std::endl;
//
//     auto total_end = clk::now();
//     std::cout << "Total execute() wall time: "
//             << std::chrono::duration_cast<std::chrono::milliseconds>(total_end - total_start).count()
//             << " ms" << std::endl;
//
//     // extend schema and return - match DuckDB projection: category, begin_col, end_col, sum_value
//     probe_schema.add_column(spec.output_column, "double"); // appended column
//     return {std::move(result), probe_schema};
// }
