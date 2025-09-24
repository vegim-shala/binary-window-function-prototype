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

    // 1. Index-Based Partitioning -> morsel-parallelism
    auto part_start = std::chrono::high_resolution_clock::now();
    auto input_idx_partitions = PartitionUtils::partition_dataset_radix_morsel(
        input, input_schema, spec.partition_columns, 8, 8, 2048);
    auto probe_idx_partitions = PartitionUtils::partition_dataset_radix_morsel(
        probe, probe_schema, spec.partition_columns, 8, 8, 2048);
    auto part_end = std::chrono::high_resolution_clock::now();
    std::cout << "Partitioning wall time: "
            << std::chrono::duration_cast<std::chrono::milliseconds>(part_end - part_start).count()
            << " ms" << std::endl;


    auto join_start = std::chrono::high_resolution_clock::now();

    // 2. Create thread pool and config batching parameters
    size_t pool_size = std::thread::hardware_concurrency();
    if (pool_size == 0) pool_size = 2;
    ThreadPool pool(pool_size);

    size_t batch_size = std::max<size_t>(1, pool_size / 2); // leave threads for morsels
    const size_t morsel_size = 256; // tuneable
    std::cout << "ThreadPool size: " << pool_size << ", partition batch size: " << batch_size
            << ", morsel_size: " << morsel_size << std::endl;


    // 3. Callback Function -> Partition Task (as std::function to avoid templated lambda capture issues)
    //    It operates on index vectors (in_indices, pr_indices).
    using IndexDataset = PartitionUtils::IndexDataset;
    std::function<void(PartitionUtils::IndexDataset, PartitionUtils::IndexDataset)> partition_task;
    partition_task = [&](IndexDataset in_indices, IndexDataset pr_indices) {
        if (pr_indices.empty() || in_indices.empty()) return;

        // a) Sort the Input indices by order_column
        const size_t order_idx = input_schema.index_of(spec.order_column);
        std::sort(in_indices.begin(), in_indices.end(), [&](size_t a, size_t b) {
            double va = input[a][order_idx];
            double vb = input[b][order_idx];
            return va < vb;
        });

        // b) Build keys (order values) and values (value_column) arrays
        const size_t value_idx = input_schema.index_of(spec.value_column);
        std::vector<double> keys;
        std::vector<double> values;
        keys.reserve(in_indices.size());
        values.reserve(in_indices.size());
        for (size_t idx: in_indices) {
            keys.push_back(input[idx][order_idx]);
            values.push_back(input[idx][value_idx]);
        }

        // c) Build Segment Tree from keys/values
        JoinUtils local_join(spec.join_spec, spec.order_column);
        local_join.build_index_from_vectors_segtree(keys, values);

        // d) Process probe indices using morsels
        size_t probe_n = pr_indices.size();

        // If probe partition >>, then utilize threads
        // Otherwise process inline to avoid tiny-task overhead.
        if (probe_n >= morsel_size * 2) {
            std::vector<std::future<std::vector<DataRow> > > futs;
            futs.reserve((probe_n + morsel_size - 1) / morsel_size);

            for (size_t mstart = 0; mstart < probe_n; mstart += morsel_size) {
                size_t mend = std::min(mstart + morsel_size, probe_n);

                // move necessary captures into the morsel lambda
                futs.emplace_back(pool.submit(
                    [mstart, mend, &pr_indices, &probe, &keys, &local_join, &probe_schema, this
                    ]() -> std::vector<DataRow> {
                        std::vector<DataRow> local_out;
                        local_out.reserve(mend - mstart);

                        // Precompute begin/end column indices once (if present)
                        const bool has_begin = !this->spec.join_spec.begin_column.empty();
                        const bool has_end = !this->spec.join_spec.end_column.empty();
                        size_t probe_begin_idx = has_begin
                                                     ? probe_schema.index_of(this->spec.join_spec.begin_column)
                                                     : SIZE_MAX;
                        size_t probe_end_idx = has_end
                                                   ? probe_schema.index_of(this->spec.join_spec.end_column)
                                                   : SIZE_MAX;

                        for (size_t j = mstart; j < mend; ++j) {
                            size_t probe_idx = pr_indices[j];
                            const DataRow &probe_row = probe[probe_idx];

                            double start = has_begin
                                               ? probe_row[probe_begin_idx]
                                               : -std::numeric_limits<double>::infinity();
                            double end = has_end
                                             ? probe_row[probe_end_idx]
                                             : std::numeric_limits<double>::infinity();

                            auto lo_it = std::lower_bound(keys.begin(), keys.end(), start);
                            auto hi_it = std::upper_bound(keys.begin(), keys.end(), end);
                            size_t lo = lo_it - keys.begin();
                            size_t hi = hi_it - keys.begin();

                            double agg_sum = 0.0;
                            if (lo < hi) {
                                agg_sum = local_join.seg_query(lo, hi);

                                DataRow out = probe_row;
                                out.push_back(agg_sum);
                                local_out.emplace_back(std::move(out));
                            }
                        }
                        return local_out;
                    }
                ));
            }

            // collect morsel results and append (one lock per morsel future)
            for (auto &f: futs) {
                auto part_res = f.get();
                if (!part_res.empty()) {
                    std::lock_guard<std::mutex> lk(result_mtx);
                    std::move(part_res.begin(), part_res.end(), std::back_inserter(result));
                }
            }
        } else {
            // small partition -> process inline to avoid task overhead
            std::vector<DataRow> local_out;
            local_out.reserve(probe_n);

            const bool has_begin = !spec.join_spec.begin_column.empty();
            const bool has_end = !spec.join_spec.end_column.empty();
            size_t probe_begin_idx = has_begin ? probe_schema.index_of(spec.join_spec.begin_column) : SIZE_MAX;
            size_t probe_end_idx = has_end ? probe_schema.index_of(spec.join_spec.end_column) : SIZE_MAX;

            for (size_t j = 0; j < probe_n; ++j) {
                size_t probe_idx = pr_indices[j];
                const DataRow &probe_row = probe[probe_idx];

                double start = has_begin
                                   ? probe_row[probe_begin_idx]
                                   : -std::numeric_limits<double>::infinity();
                double end = has_end
                                 ? probe_row[probe_end_idx]
                                 : std::numeric_limits<double>::infinity();

                auto lo_it = std::lower_bound(keys.begin(), keys.end(), start);
                auto hi_it = std::upper_bound(keys.begin(), keys.end(), end);
                size_t lo = lo_it - keys.begin();
                size_t hi = hi_it - keys.begin();

                double agg_sum = 0.0;
                if (lo < hi) {
                    agg_sum = local_join.seg_query(lo, hi);

                    DataRow out = probe_row;
                    out.push_back(agg_sum);
                    local_out.emplace_back(std::move(out));
                }
            }

            // single flush
            std::lock_guard<std::mutex> lk(result_mtx);
            std::move(local_out.begin(), local_out.end(), std::back_inserter(result));
        }
    }; // end partition_task

    // 4. Build worklist (copy index vectors) and submit partition-level tasks in batches
    std::vector<std::pair<std::string, std::pair<IndexDataset, IndexDataset> > > worklist;
    worklist.reserve(1024);

    for (size_t bucket = 0; bucket < probe_idx_partitions.size(); ++bucket) {
        // probe indices for this bucket
        IndexDataset pr_inds = probe_idx_partitions[bucket];

        // skip empty probe buckets to save work
        if (pr_inds.empty()) continue;

        // matching input indices
        IndexDataset in_inds;
        if (bucket < input_idx_partitions.size()) {
            in_inds = input_idx_partitions[bucket];
        }

        // build a compact string representation of the bucket ID (debug)
        std::string key_str = std::to_string(bucket);

        worklist.emplace_back(std::move(key_str),
                              std::make_pair(std::move(in_inds), std::move(pr_inds)));
    }

    // submit partition tasks in batches (so we don't occupy entire pool with partition tasks, but we might change this cause sorting is the only competitor on thread utilization)
    for (size_t pos = 0; pos < worklist.size(); pos += batch_size) {
        size_t endpos = std::min(worklist.size(), pos + batch_size);
        std::vector<std::future<void> > batch_futs;
        batch_futs.reserve(endpos - pos);

        for (size_t i = pos; i < endpos; ++i) {
            IndexDataset in_inds = std::move(worklist[i].second.first);
            IndexDataset pr_inds = std::move(worklist[i].second.second);

            // submit partition-level task (moves in_inds/pr_inds into lambda)
            batch_futs.emplace_back(pool.submit(
                [in_inds = std::move(in_inds), pr_inds = std::move(pr_inds), &partition_task]() mutable {
                    partition_task(std::move(in_inds), std::move(pr_inds));
                }));
        }

        // wait for this batch to finish so morsels have room to run
        for (auto &f: batch_futs) f.get();
    }

    auto join_end = std::chrono::high_resolution_clock::now();
    std::cout << "Join wall time: "
            << std::chrono::duration_cast<std::chrono::milliseconds>(join_end - join_start).count()
            << " ms" << std::endl;

    auto total_end = std::chrono::high_resolution_clock::now();
    std::cout << "Total execute() wall time: "
            << std::chrono::duration_cast<std::chrono::milliseconds>(total_end - total_start).count()
            << " ms" << std::endl;

    // extend schema and return
    probe_schema.add_column(spec.output_column, "double");
    return {std::move(result), probe_schema};
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
