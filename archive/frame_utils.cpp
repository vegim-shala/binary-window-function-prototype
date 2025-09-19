//
// Created by Vegim Shala on 10.7.25.
//

#include "operators/utils/frame_utils.h"

using OrderKey = std::vector<double>;

OrderKey extract_order_key(const DataRow& row, const std::vector<std::string>& order_columns) {
    OrderKey key;
    for (const auto& col : order_columns) {
        key.push_back(extract_numeric(row.at(col)));
    }
    return key;
}

bool order_key_less_equal(const OrderKey& a, const OrderKey& b) {
    return std::ranges::lexicographical_compare(a, b) || a == b;
}

bool order_key_within_range(const OrderKey& key, const OrderKey& start, const OrderKey& end) {
    return key >= start && key <= end;
}


// std::vector<size_t> FrameUtils::compute_range_frame_static_binary(const Dataset& input, const DataRow& probe_row) const {
//     std::vector<size_t> indices;
//
//     OrderKey center = extract_order_key(probe_row, order_columns);
//
//     // Modify only the last component
//     double base_time = center.back();
//     double start_val = base_time - frame_spec.preceding.value();
//     double end_val = base_time + frame_spec.following.value();
//
//     OrderKey start = center;
//     OrderKey end = center;
//     start.back() = start_val;
//     end.back() = end_val;
//
//     for (size_t j = 0; j < input.size(); ++j) {
//         OrderKey current = extract_order_key(input[j], order_columns);
//         if (order_key_within_range(current, start, end)) {
//             indices.push_back(j);
//         }
//     }
//
//     return indices;
// }

// std::vector<size_t> FrameUtils::compute_row_frame_static_binary(const Dataset& input, const DataRow& probe_row) const {
//     std::vector<size_t> indices;
//
//     OrderKey probe_key = extract_order_key(probe_row, order_columns);
//
//     std::vector<OrderKey> order_keys;
//     order_keys.reserve(input.size());
//     for (const auto& row : input) {
//         order_keys.push_back(extract_order_key(row, order_columns));
//     }
//
//     auto lower = std::lower_bound(order_keys.begin(), order_keys.end(), probe_key);
//     size_t anchor_index = std::distance(order_keys.begin(), lower);
//
//     int start = 0;
//     int end = 0;
//
//     if (frame_spec.preceding >= 0 && frame_spec.following >= 0) {
//         start = std::max(0, static_cast<int>(anchor_index) - frame_spec.preceding.value());
//         end = std::min(static_cast<int>(input.size()) - 1, static_cast<int>(anchor_index) + frame_spec.following.value());
//     } else {
//         throw std::runtime_error("Only positive ROWS frames supported in this example.");
//     }
//
//     for (int i = start; i <= end; ++i) {
//         indices.push_back(i);
//     }
//
//     return indices;
// }

std::vector<size_t> FrameUtils::compute_range_frame_dynamic_binary(
    const Dataset& input, const DataRow& probe_row,
    const std::string& start_col, const std::string& end_col) const
{
    // std::vector<size_t> indices;
    //
    // double start = extract_numeric(probe_row.at(start_col));
    // double end = extract_numeric(probe_row.at(end_col));
    //
    // for (size_t j = 0; j < input.size(); ++j) {
    //     double value = extract_numeric(input[j].at(order_column));
    //     if (value >= start && value <= end) {
    //         indices.push_back(j);
    //     }
    // }
    //
    // return indices;
    std::vector<size_t> indices;

    OrderKey center = extract_order_key(probe_row, order_columns);
    OrderKey start = center;
    OrderKey end = center;

    start.back() = extract_numeric(probe_row.at(start_col));
    end.back() = extract_numeric(probe_row.at(end_col));

    for (size_t j = 0; j < input.size(); ++j) {
        OrderKey key = extract_order_key(input[j], order_columns);
        if (order_key_within_range(key, start, end)) {
            indices.push_back(j);
        }
    }

    return indices;
}

// std::vector<size_t> FrameUtils::compute_row_frame_dynamic_binary(
//     const Dataset& input, const DataRow& probe_row,
//     const std::string& begin_col, const std::string& end_col) const
// {
//     std::vector<size_t> indices;
//
//     OrderKey probe_key = extract_order_key(probe_row, order_columns);
//
//     std::vector<OrderKey> order_keys;
//     order_keys.reserve(input.size());
//     for (const auto& row : input) {
//         order_keys.push_back(extract_order_key(row, order_columns));
//     }
//
//     auto lower = std::lower_bound(order_keys.begin(), order_keys.end(), probe_key);
//     size_t anchor_index = std::distance(order_keys.begin(), lower);
//
//     int preceding = static_cast<int>(extract_numeric(probe_row.at(begin_col)));
//     int following = static_cast<int>(extract_numeric(probe_row.at(end_col)));
//
//     int start = std::max(0, static_cast<int>(anchor_index) - preceding);
//     int end = std::min(static_cast<int>(input.size()) - 1, static_cast<int>(anchor_index) + following);
//
//     for (int i = start; i <= end; ++i) {
//         indices.push_back(i);
//     }
//
//     return indices;
// }

//TODO: Include this validation function once we have a final version
void FrameUtils::validate() const {
    // We allow either (preceding, following) or (begin_column, end_column) from the probe, not both
    if (frame_spec.type == FrameType::ROWS || frame_spec.type == FrameType::RANGE) {
        if (!frame_spec.preceding.has_value() || !frame_spec.following.has_value()) {
            if (frame_spec.begin_column.empty() || frame_spec.end_column.empty()) {
                throw std::runtime_error("Invalid FrameSpec: must define either static or dynamic bounds.");
            }
        }
    }

    // For RANGE frames we can only have one ordering column
    if (frame_spec.type == FrameType::RANGE) {
        if (order_columns.size() > 1) {
            throw std::runtime_error("Invalid FrameSpec: only one ordering column can be used for RANGE frames.");
        }
    }
}

std::vector<size_t> FrameUtils::compute_binary_frame_indices(const Dataset& input, const DataRow& probe_row) const {
    validate();
    if (frame_spec.type == FrameType::RANGE) {
        return compute_range_frame_dynamic_binary(input, probe_row, frame_spec.begin_column, frame_spec.end_column);
    }
    // else if (frame_spec.type == FrameType::ROWS) {
    //      return compute_row_frame_dynamic_binary(input, probe_row, frame_spec.begin_column, frame_spec.end_column);
    // }

    throw std::runtime_error("Unsupported or incomplete FrameSpec configuration for binary window execution.");
}


