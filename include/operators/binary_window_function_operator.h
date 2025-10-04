#pragma once

#include <map>
#include <string>
#include <ips2ra/ips2ra.hpp>

#include "aggregators/factory.h"
#include "operators/utils/join_utils.h"
#include "operators/utils/partition_utils.h"
#include "data_io.h"
#include "utils/thread_pool.h"

struct BinaryWindowFunctionModel {
    std::string value_column;
    std::vector<std::string> partition_columns; // optional
    std::string order_column;
    std::string output_column;

    JoinSpec join_spec;
    AggregationType agg_type;
};

class BinaryWindowFunctionOperator {
public:
    explicit BinaryWindowFunctionOperator(BinaryWindowFunctionModel spec)
        : spec(std::move(spec)),
          aggregator(create_aggregator(this->spec.agg_type)),
          join_utils(this->spec.join_spec, this->spec.order_column) {
    }

    std::pair<Dataset, FileSchema> execute(Dataset &input, Dataset &probe, FileSchema input_schema,
                                           FileSchema probe_schema);

    std::pair<Dataset, FileSchema> execute_sequential(
        Dataset &input,
        Dataset &probe,
        FileSchema input_schema,
        FileSchema probe_schema
    );

private:
    BinaryWindowFunctionModel spec;
    std::unique_ptr<Aggregator> aggregator;
    JoinUtils join_utils;
    std::vector<uint32_t> global_keys;

    std::vector<std::pair<PartitionUtils::IndexDataset, PartitionUtils::IndexDataset> > build_worklist(
        auto &input_idx_partitions,
        auto &probe_idx_partitions
    );

    void process_worklist(
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
    );

    void process_partition(
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
    ) const;

    void process_probe_partition_parallel(
        const PartitionUtils::IndexDataset &pr_indices,
        const Dataset &probe,
        const FileSchema &probe_schema,
        JoinUtils &local_join,
        Dataset &result,
        std::mutex &result_mtx,
        ThreadPool &pool,
        size_t morsel_size
    ) const;

    void process_probe_partition_inline(
        const PartitionUtils::IndexDataset &pr_indices,
        const Dataset &probe,
        const FileSchema &probe_schema,
        JoinUtils &local_join,
        Dataset &result,
        std::mutex &result_mtx
    ) const;

    std::vector<DataRow> process_probe_morsel(
        size_t mstart,
        size_t mend,
        const PartitionUtils::IndexDataset &pr_indices,
        const Dataset &probe,
        const FileSchema &probe_schema,
        const std::vector<uint32_t> &keys,
        JoinUtils &local_join
    ) const;

    std::vector<DataRow> process_probe_morsel_sort_probe(
        size_t mstart,
        size_t mend,
        const PartitionUtils::IndexDataset &pr_indices,
        const Dataset &probe,
        const FileSchema &probe_schema,
        const std::vector<uint32_t> &keys,
        JoinUtils &local_join
    ) const;

    std::vector<DataRow> process_probe_morsel_sort_probe_interleaving(
        size_t mstart,
        size_t mend,
        const PartitionUtils::IndexDataset &pr_indices,
        const Dataset &probe,
        const FileSchema &probe_schema,
        JoinUtils &local_join
    ) const;

    // ------------------------ These functions are for sequential execution only ------------------------
    void process_partition_sequential(
        PartitionUtils::IndexDataset in_indices,
        PartitionUtils::IndexDataset pr_indices,
        const Dataset &input,
        const Dataset &probe,
        const FileSchema &input_schema,
        const FileSchema &probe_schema,
        Dataset &result
    ) const;

    struct ProbeTask {
        size_t orig_pos; // local position in [mstart..mend)
        size_t probe_idx; // index into probe dataset
        int32_t start;
        int32_t end;
    };


    void process_partition_new(
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
    ) const;


    void process_probe_partition_parallel_new(
        const PartitionUtils::IndexDataset &pr_indices,
        const Dataset &probe,
        const JoinUtils &local_join,
        Dataset &result,
        std::mutex &result_mtx,
        ThreadPool &pool,
        size_t morsel_size,
        size_t begin_idx,
        size_t end_idx
    ) const;

    void process_probe_partition_inline_new(
        const PartitionUtils::IndexDataset &pr_indices,
        const Dataset &probe,
        const JoinUtils &local_join,
        Dataset &result,
        std::mutex &result_mtx,
        size_t begin_idx,
        size_t end_idx
    ) const;

    std::vector<DataRow> process_probe_morsel_new(
        size_t mstart,
        size_t mend,
        const PartitionUtils::IndexDataset &pr_indices,
        const Dataset &probe,
        const JoinUtils &local_join,
        size_t begin_idx,
        size_t end_idx
    ) const;

    // Compute lo/hi for all rows using two monotone passes.
    // pr_indices: row IDs of this probe partition (unspecified order)
    // begin_idx/end_idx: column indices in probe_schema
    // local_join: built index with signed keys and int64 prefix
    inline void compute_lo_hi_two_pass(
        const PartitionUtils::IndexDataset &pr_indices,
        const Dataset &probe,
        size_t begin_idx,
        size_t end_idx,
        const JoinUtils &local_join,
        std::vector<size_t> &lo_out,
        std::vector<size_t> &hi_out
    ) const {
        const size_t m = pr_indices.size();
        lo_out.assign(m, 0);
        hi_out.assign(m, 0);

        // We sort *positions* [0..m) so we can write results back by position.
        std::vector<size_t> pos_by_begin(m), pos_by_end(m);
        std::iota(pos_by_begin.begin(), pos_by_begin.end(), 0);
        std::iota(pos_by_end.begin(), pos_by_end.end(), 0);

#ifdef NDEBUG
        // Sort positions by begin
        ips2ra::sort(pos_by_begin.begin(), pos_by_begin.end(),
                     [&](size_t p) {
                         const DataRow &r = probe[pr_indices[p]];
                         return static_cast<uint32_t>(static_cast<int32_t>(r[begin_idx])) ^ 0x80000000u;
                     });
        // Sort positions by end
        ips2ra::sort(pos_by_end.begin(), pos_by_end.end(),
                     [&](size_t p) {
                         const DataRow &r = probe[pr_indices[p]];
                         return static_cast<uint32_t>(static_cast<int32_t>(r[end_idx])) ^ 0x80000000u;
                     });
#else
    std::sort(pos_by_begin.begin(), pos_by_begin.end(),
              [&](size_t a, size_t b){
                  const auto& ra = probe[pr_indices[a]];
                  const auto& rb = probe[pr_indices[b]];
                  return static_cast<int32_t>(ra[begin_idx]) < static_cast<int32_t>(rb[begin_idx]);
              });
    std::sort(pos_by_end.begin(), pos_by_end.end(),
              [&](size_t a, size_t b){
                  const auto& ra = probe[pr_indices[a]];
                  const auto& rb = probe[pr_indices[b]];
                  return static_cast<int32_t>(ra[end_idx]) < static_cast<int32_t>(rb[end_idx]);
              });
#endif

        // Pass 1: monotone lower_bounds in begin order
        size_t hint_lo = 0;
        for (size_t k = 0; k < m; ++k) {
            const size_t p = pos_by_begin[k];
            const int32_t s = static_cast<int32_t>(probe[pr_indices[p]][begin_idx]);
            hint_lo = local_join.lower_from_hint(hint_lo, s);
            lo_out[p] = hint_lo;
        }

        // Pass 2: monotone upper_bounds in end order
        size_t hint_hi = 0;
        for (size_t k = 0; k < m; ++k) {
            const size_t p = pos_by_end[k];
            const int32_t e = static_cast<int32_t>(probe[pr_indices[p]][end_idx]);
            hint_hi = local_join.upper_from_hint(hint_hi, e);
            hi_out[p] = hint_hi;
        }
    };
};
