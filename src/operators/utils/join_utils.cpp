//
// Created by Vegim Shala on 10.7.25.
//

#include "operators/utils/join_utils.h"

using OrderKey = std::vector<double>;

OrderKey extract_order_key_join(const DataRow& row, const std::vector<std::string>& order_columns) {
    OrderKey key;
    for (const auto& col : order_columns) {
        key.push_back(extract_numeric(row.at(col)));
    }
    return key;
}

std::vector<size_t> JoinUtils::compute_range_join(
    const Dataset& input, const DataRow& probe_row,
    const std::string& start_col, const std::string& end_col) const
{
     std::vector<size_t> indices;

     double start = start_col.empty() ? std::numeric_limits<double>::min() : extract_numeric(probe_row.at(start_col));
     double end = end_col.empty() ? std::numeric_limits<double>::max() : extract_numeric(probe_row.at(end_col));

     for (size_t j = 0; j < input.size(); ++j) {
         double value = extract_numeric(input[j].at(order_column));
         if (value >= start && value <= end) {
             indices.push_back(j);
         }
     }

     return indices;
}

void JoinUtils::validate() const {
    // if (join_spec.begin_column.empty() || join_spec.end_column.empty()) {
    //     throw std::runtime_error("Invalid JoinSpec: Please specify both begin_column and end_column.");
    // }
    // TODO: Add further validations here...
}

std::vector<size_t> JoinUtils::compute_join(const Dataset& input, const DataRow& probe_row) const {
    validate();
    if (join_spec.type == JoinType::RANGE) {
        return compute_range_join(input, probe_row, join_spec.begin_column, join_spec.end_column);
    }
    // else if (join_spec.type == JoinType::RANGE) {
    //     return compute_rows_join(input, probe_row, join_spec.begin_column, join_spec.end_column);
    // }

    throw std::runtime_error("Unsupported or incomplete FrameSpec configuration for binary window execution.");
}


