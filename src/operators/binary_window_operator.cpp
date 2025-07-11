//
// Created by Vegim Shala on 10.7.25.
//
#include "operators/binary_window_operator.h"
#include "operators/utils/frame_utils.h"
#include "operators/utils/partition_utils.h"
#include "operators/utils/sort_utils.h"


using namespace std;

pair<Dataset, FileSchema> BinaryWindowOperator::execute(const Dataset& input, FileSchema schema) {
    Dataset result;
    FrameUtils frame_utils(spec.frame_spec, spec.order_column);

    // Let's first handle the partitioning and sorting
    std::vector<Dataset> partitions;

    auto partitioned = PartitionUtils::partition_dataset(input, spec.partition_column);
    for (auto& [_, partition] : partitioned) {
        SortUtils::sort_dataset(partition, spec.order_column);
        partitions.push_back(std::move(partition));
    }


    for (const auto& partition : partitions) {
        for (size_t i = 0; i < partition.size(); ++i) {
            const auto& current_row = partition[i];

            std::vector<size_t> indices = frame_utils.compute_range_indices(partition, i);

            std::vector<double> values;
            for (auto idx : indices) {
                values.push_back(extract_numeric(partition[idx].at(spec.value_column)));
            }

            DataRow new_row = current_row;
            new_row[spec.output_column] = aggregator->compute(values);
            result.push_back(std::move(new_row));
        }
    }


    schema.columns.push_back(spec.output_column);
    schema.column_types[spec.output_column] = "double";
    return {result, schema};
}