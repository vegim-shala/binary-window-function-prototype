//
// Created by Vegim Shala on 10.7.25.
//

#include "operators/utils/frame_utils.h"

std::vector<size_t> FrameUtils::compute_row_frame(const Dataset& input, size_t current_index) const {
    std::vector<size_t> indices;
    int start = std::max(0, static_cast<int>(current_index) - frame_spec.preceding.value());
    int end = std::min(static_cast<int>(input.size()) - 1, static_cast<int>(current_index) + frame_spec.following.value());

    for (int i = start; i <= end; ++i) {
        indices.push_back(i);
    }
    return indices;
}

std::vector<size_t> FrameUtils::compute_range_frame(const Dataset& input, size_t current_index) const{
    std::vector<size_t> indices;
    const auto& center_row = input[current_index];
    double center_time = extract_numeric(center_row.at(order_column));

    double start_range = center_time - frame_spec.preceding.value();
    double end_range = center_time + frame_spec.following.value();

    for (size_t j = 0; j < input.size(); ++j) {
        double time = extract_numeric(input[j].at(order_column));
        if (time >= start_range && time <= end_range) {
            indices.push_back(j);
        }
    }

    return indices;
}

std::vector<size_t> FrameUtils::compute_range_indices(const Dataset& input, size_t current_index) const {
    if (frame_spec.type == FrameType::ROWS) {
        return compute_row_frame(input, current_index);
    } else if (frame_spec.type == FrameType::RANGE) {
        return compute_range_frame(input, current_index);
    } else {
      throw std::runtime_error("Unsupported frame type");
      }
}

std::vector<size_t> FrameUtils::compute_range_frame_static_binary(const Dataset& input, const DataRow& probe_row) const {
    std::vector<size_t> indices;
    double center_time = extract_numeric(probe_row.at(order_column));
    double start_range = center_time - frame_spec.preceding.value();
    double end_range = center_time + frame_spec.following.value();

    for (size_t j = 0; j < input.size(); ++j) {
        double time = extract_numeric(input[j].at(order_column));
        if (time >= start_range && time <= end_range) {
            indices.push_back(j);
        }
    }
    return indices;
}

std::vector<size_t> FrameUtils::compute_row_frame_static_binary(const Dataset& input, const DataRow& probe_row) const {
    std::vector<size_t> indices;

    double probe_order_value = extract_numeric(probe_row.at(order_column));

    // Build vector of order_column values (assuming input is sorted on order_column)
    std::vector<double> order_values;
    order_values.reserve(input.size());
    for (const auto& row : input) {
        order_values.push_back(extract_numeric(row.at(order_column)));
    }

    // Find logical position where probe_order_value would be inserted
    auto lower = std::lower_bound(order_values.begin(), order_values.end(), probe_order_value);
    size_t anchor_index = std::distance(order_values.begin(), lower);

    int start = 0;
    int end = 0;

    if (frame_spec.preceding >= 0 && frame_spec.following >= 0) {
        start = std::max(0, static_cast<int>(anchor_index) - frame_spec.preceding.value());
        end = std::min(static_cast<int>(input.size()) - 1, static_cast<int>(anchor_index) + frame_spec.following.value());
    } else {
        throw std::runtime_error("Only positive ROWS frames supported in this example.");
    }

    for (int i = start; i <= end; ++i) {
        indices.push_back(i);
    }

    return indices;
}

std::vector<size_t> FrameUtils::compute_range_frame_dynamic_binary(
    const Dataset& input, const DataRow& probe_row,
    const std::string& start_col, const std::string& end_col,
    const std::string& order_column) const
{
    std::vector<size_t> indices;

    double start = extract_numeric(probe_row.at(start_col));
    double end = extract_numeric(probe_row.at(end_col));

    for (size_t j = 0; j < input.size(); ++j) {
        double value = extract_numeric(input[j].at(order_column));
        if (value >= start && value <= end) {
            indices.push_back(j);
        }
    }

    return indices;
}

std::vector<size_t> FrameUtils::compute_row_frame_dynamic_binary(
    const Dataset& input, const DataRow& probe_row,
    const std::string& begin_col, const std::string& end_col) const
{
    std::vector<size_t> indices;

    double probe_order_value = extract_numeric(probe_row.at(order_column));

    std::vector<double> order_values;
    order_values.reserve(input.size());
    for (const auto& row : input) {
        order_values.push_back(extract_numeric(row.at(order_column)));
    }

    auto lower = std::lower_bound(order_values.begin(), order_values.end(), probe_order_value);
    size_t anchor_index = std::distance(order_values.begin(), lower);

    int preceding = static_cast<int>(extract_numeric(probe_row.at(begin_col)));
    int following = static_cast<int>(extract_numeric(probe_row.at(end_col)));

    int start = std::max(0, static_cast<int>(anchor_index) - preceding);
    int end = std::min(static_cast<int>(input.size()) - 1, static_cast<int>(anchor_index) + following);

    for (int i = start; i <= end; ++i) {
        indices.push_back(i);
    }

    return indices;
}

std::vector<size_t> FrameUtils::compute_binary_frame_indices(const Dataset& input, const DataRow& probe_row) const {
    if (frame_spec.type == FrameType::ROWS) {
        if (frame_spec.preceding.has_value() && frame_spec.following.has_value()) {
            return compute_row_frame_static_binary(input, probe_row);
        } else if (!frame_spec.begin_column.empty() && !frame_spec.end_column.empty()) {
            return compute_row_frame_dynamic_binary(input, probe_row, frame_spec.begin_column, frame_spec.end_column);
        }
    } else if (frame_spec.type == FrameType::RANGE) {
        if (frame_spec.preceding.has_value() && frame_spec.following.has_value()) {
            return compute_range_frame_static_binary(input, probe_row);
        } else if (!frame_spec.begin_column.empty() && !frame_spec.end_column.empty()) {
            return compute_range_frame_dynamic_binary(input, probe_row, frame_spec.begin_column, frame_spec.end_column, order_column);
        }
    }

    throw std::runtime_error("Unsupported or incomplete FrameSpec configuration for binary window execution.");
}


//TODO: Include this validation function once we have a final version
// void FrameSpec::validate() const {
//     if (type == FrameType::ROWS || type == FrameType::RANGE) {
//         if (!preceding.has_value() || !following.has_value()) {
//             if (begin_column.empty() || end_column.empty()) {
//                 throw std::runtime_error("Invalid FrameSpec: must define either static or dynamic bounds.");
//             }
//         }
//     }
// }