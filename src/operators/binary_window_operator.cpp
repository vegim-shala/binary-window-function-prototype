//
// Created by Vegim Shala on 10.7.25.
//
#include "operators/binary_window_operator.h"
#include "operators/utils/frame_utils.h"

using namespace std;

pair<Dataset, FileSchema> BinaryWindowOperator::execute(const Dataset& input, FileSchema schema) {
    Dataset result;
    FrameUtils frame_utils(spec.frame_spec, spec.order_column);

    for (size_t i = 0; i < input.size(); ++i) {
        const auto& current_row = input[i];

        std::vector<size_t> indices = frame_utils.compute_range_indices(input, i);

        std::vector<double> values;
        for (auto idx : indices) {
            values.push_back(extract_numeric(input[idx].at(spec.value_column)));
        }

        DataRow new_row = current_row;
        new_row[spec.output_column] = aggregator->compute(values);
        result.push_back(std::move(new_row));
    }

    schema.columns.push_back(spec.output_column);
    schema.column_types[spec.output_column] = "double";
    return {result, schema};
}