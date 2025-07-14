//
// Created by Vegim Shala on 14.7.25.
//
#include "operators/binary_window_function_operator.h"
#include "operators/utils/partition_utils.h"
#include "operators/utils/sort_utils.h"

using namespace std;

std::string BinaryWindowFunctionOperator::extract_partition_key(const DataRow& row) const {
    if (spec.partition_column.empty()) return "__NOPART__";
    return std::get<std::string>(row.at(spec.partition_column));
}

pair<Dataset, FileSchema> BinaryWindowFunctionOperator::execute(const Dataset& input, const Dataset& probe, FileSchema schema) {
    Dataset result;

    // Partition input and probe
    auto input_partitions = PartitionUtils::partition_dataset(input, spec.partition_column);
    auto probe_partitions = PartitionUtils::partition_dataset(probe, spec.partition_column);

    for (const auto& [partition_key, probe_partition] : probe_partitions) {
        auto input_it = input_partitions.find(partition_key);
        if (input_it == input_partitions.end()) continue;

        Dataset& input_partition = input_it->second;
        SortUtils::sort_dataset(input_partition, spec.order_column);

        for (const auto& probe_row : probe_partition) {
            std::vector<size_t> indices = frame_utils.compute_range_frame_binary(input_partition, probe_row);
            std::vector<double> values;

            for (size_t idx : indices) {
                values.push_back(extract_numeric(input_partition[idx].at(spec.value_column)));
            }

            DataRow output_row = probe_row;
            output_row[spec.output_column] = aggregator->compute(values);
            result.push_back(std::move(output_row));
        }
    }

    // Schema extension
    schema.columns.push_back(spec.output_column);
    schema.column_types[spec.output_column] = "double";
    return {result, schema};
}
