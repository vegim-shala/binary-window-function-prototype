#pragma once

#include <map>
#include <string>
#include "aggregators/factory.h"
#include "operators/utils/join_utils2.h"
#include "operators/utils/partition_utils2.h"
#include "data_io.h"
#include "utils/thread_pool.h"

struct BinaryWindowFunctionModel2 {
    std::string value_column;
    std::vector<std::string> partition_columns; // optional
    std::string order_column;
    std::string output_column;

    JoinSpec2 join_spec;
    AggregationType agg_type;
};

class BinaryWindowFunctionOperator2 {
public:
    explicit BinaryWindowFunctionOperator2(BinaryWindowFunctionModel2 spec)
        : spec(std::move(spec)),
          aggregator(create_aggregator(this->spec.agg_type)),
          join_utils(this->spec.join_spec, this->spec.order_column) {
    }

    std::pair<Dataset, FileSchema> execute(Dataset &input, Dataset &probe,
                                           FileSchema input_schema, FileSchema probe_schema);

    std::pair<Dataset, FileSchema> execute2(Dataset &input, Dataset &probe, FileSchema input_schema,
                                            FileSchema probe_schema);

    std::pair<Dataset, FileSchema> execute3(Dataset &input, Dataset &probe, FileSchema input_schema,
                                            FileSchema probe_schema);

    std::pair<Dataset, FileSchema> execute4(Dataset &input, Dataset &probe, FileSchema input_schema,
                                            FileSchema probe_schema);

private:
    BinaryWindowFunctionModel2 spec;
    std::unique_ptr<Aggregator> aggregator;
    JoinUtils2 join_utils;
    std::vector<uint32_t> global_keys;

    std::string extract_partition_key(const DataRow &row) const;

    Dataset probe_parallel(const Dataset &input_partition, const Dataset &probe_partition, const FileSchema &schema,
                           size_t num_threads);

    std::vector<std::pair<PartitionUtils2::PartitionPayload, PartitionUtils2::IndexDataset> > build_worklist(
        PartitionUtils2::PartitionPayloadResult &input_payload_partitions,
        PartitionUtils2::PartitionIndexResult &probe_idx_partitions
    );

    void process_worklist(
        std::vector<std::pair<PartitionUtils2::PartitionPayload, PartitionUtils2::IndexDataset> > &worklist,
        Dataset &input, Dataset &probe,
        FileSchema &input_schema, FileSchema &probe_schema,
        Dataset &result, std::mutex &result_mtx,
        ThreadPool &pool, size_t batch_size, size_t morsel_size);

    void process_partition(
        PartitionUtils2::PartitionPayload in_payload, // <-- changed
        PartitionUtils2::IndexDataset pr_indices, // unchanged
        const Dataset &input, const Dataset &probe,
        const FileSchema &input_schema, const FileSchema &probe_schema,
        Dataset &result, std::mutex &result_mtx,
        ThreadPool &pool, size_t morsel_size) const;

    void process_probe_partition_parallel(
        const PartitionUtils2::IndexDataset &pr_indices,
        const Dataset &probe,
        const FileSchema &probe_schema,
        const std::vector<uint32_t> &keys,
        JoinUtils2 &local_join,
        Dataset &result,
        std::mutex &result_mtx,
        ThreadPool &pool,
        size_t morsel_size
    ) const;

    void process_probe_partition_inline(
        const PartitionUtils2::IndexDataset &pr_indices,
        const Dataset &probe,
        const FileSchema &probe_schema,
        const std::vector<uint32_t> &keys,
        JoinUtils2 &local_join,
        Dataset &result,
        std::mutex &result_mtx
    ) const;

    std::vector<DataRow> process_probe_morsel(
        size_t mstart,
        size_t mend,
        const PartitionUtils2::IndexDataset &pr_indices,
        const Dataset &probe,
        const FileSchema &probe_schema,
        const std::vector<uint32_t> &keys,
        JoinUtils2 &local_join
    ) const;
};
