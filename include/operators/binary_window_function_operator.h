#pragma once

#include <map>
#include <string>
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
        const std::vector<uint32_t> &keys,
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
        const std::vector<uint32_t> &keys,
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
        const std::vector<uint32_t> &keys,
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
        Dataset &result,
        std::mutex &result_mtx
    ) const;

    struct ProbeTask {
        size_t orig_pos; // local position in [mstart..mend)
        size_t probe_idx; // index into probe dataset
        int32_t start;
        int32_t end;
    };
};
