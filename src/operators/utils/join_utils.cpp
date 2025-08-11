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
         double value = extract_numeric(input[j].at(order_columns[0]));
         if (value >= start && value <= end) {
             indices.push_back(j);
         }
     }

     return indices;
}

std::vector<size_t> JoinUtils::compute_rows_join(
    const Dataset& input, const DataRow& probe_row,
    const std::string& begin_col, const std::string& end_col) const
{
    std::vector<size_t> indices;

    OrderKey probe_key = extract_order_key_join(probe_row, order_columns);

    std::vector<OrderKey> order_keys;
    order_keys.reserve(input.size());
    for (const auto& row : input) {
        order_keys.push_back(extract_order_key_join(row, order_columns));
    }

    auto lower = std::lower_bound(order_keys.begin(), order_keys.end(), probe_key);
    size_t anchor_index = std::distance(order_keys.begin(), lower);

    int preceding = begin_col.empty()
        ? static_cast<int>(anchor_index)  // all rows before anchor
        : static_cast<int>(extract_numeric(probe_row.at(begin_col)));

    int following = end_col.empty()
        ? static_cast<int>(input.size() - anchor_index - 1) // all rows after anchor
        : static_cast<int>(extract_numeric(probe_row.at(end_col)));

    int start = std::max(0, static_cast<int>(anchor_index) - preceding);
    int end = std::min(static_cast<int>(input.size()) - 1, static_cast<int>(anchor_index) + following);

    for (int i = start; i <= end; ++i) {
        indices.push_back(i);
    }

    return indices;
}

void JoinUtils::validate() const {
    // if (join_spec.begin_column.empty() || join_spec.end_column.empty()) {
    //     throw std::runtime_error("Invalid JoinSpec: Please specify both begin_column and end_column.");
    // }

    // For RANGE frames we can only have one ordering column
    if (join_spec.type == JoinType::RANGE) {
        if (order_columns.size() > 1) {
            throw std::runtime_error("Invalid JoinSpec: only one ordering column can be used for RANGE frames.");
        }
    }

    // TODO: Add further validations here...
}

std::vector<size_t> JoinUtils::compute_join(const Dataset& input, const DataRow& probe_row) const {
    validate();
    if (join_spec.type == JoinType::ROWS) {
        return compute_rows_join(input, probe_row, join_spec.begin_column, join_spec.end_column);
    } else if (join_spec.type == JoinType::RANGE) {
        return compute_range_join(input, probe_row, join_spec.begin_column, join_spec.end_column);
    }

    throw std::runtime_error("Unsupported or incomplete FrameSpec configuration for binary window execution.");
}


