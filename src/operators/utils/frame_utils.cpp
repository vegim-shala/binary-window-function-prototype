//
// Created by Vegim Shala on 10.7.25.
//

#include "operators/utils/frame_utils.h"

std::vector<size_t> FrameUtils::compute_row_frame(const Dataset& input, size_t current_index) const {
    std::vector<size_t> indices;
    int start = std::max(0, static_cast<int>(current_index) - frame_spec.preceding);
    int end = std::min(static_cast<int>(input.size()) - 1, static_cast<int>(current_index) + frame_spec.following);

    for (int i = start; i <= end; ++i) {
        indices.push_back(i);
    }
    return indices;
}

std::vector<size_t> FrameUtils::compute_range_frame(const Dataset& input, size_t current_index) const{
    std::vector<size_t> indices;
    const auto& center_row = input[current_index];
    double center_time = extract_numeric(center_row.at(order_column));

    double start_range = center_time - frame_spec.preceding;
    double end_range = center_time + frame_spec.following;

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

std::vector<size_t> FrameUtils::compute_range_frame_binary(const Dataset& input, const DataRow& probe_row) const {
    std::vector<size_t> indices;
    double center_time = extract_numeric(probe_row.at(order_column));
    double start_range = center_time - frame_spec.preceding;
    double end_range = center_time + frame_spec.following;

    for (size_t j = 0; j < input.size(); ++j) {
        double time = extract_numeric(input[j].at(order_column));
        if (time >= start_range && time <= end_range) {
            indices.push_back(j);
        }
    }
    return indices;
}

std::vector<size_t> FrameUtils::compute_row_frame_binary(const Dataset& input, const DataRow& probe_row) const {
    std::vector<size_t> indices;

    // Find the index of probe_row in input (pointer identity won't work, so use timestamp or id)
    double probe_order_value = extract_numeric(probe_row.at(order_column));

    size_t probe_index = 0;
    bool found = false;
    for (size_t i = 0; i < input.size(); ++i) {
        double input_order_value = extract_numeric(input[i].at(order_column));
        if (input_order_value == probe_order_value) {
            probe_index = i;
            found = true;
            break;
        }
    }

    if (!found) {
        throw std::runtime_error("Probe row not found in input (order_column mismatch).");
    }

    int start = std::max(0, static_cast<int>(probe_index) - frame_spec.preceding);
    int end = std::min(static_cast<int>(input.size()) - 1, static_cast<int>(probe_index) + frame_spec.following);

    for (int i = start; i <= end; ++i) {
        indices.push_back(i);
    }

    return indices;
}

std::vector<size_t> FrameUtils::compute_binary_frame_indices(const Dataset& input, const DataRow& probe_row) const {
    if (frame_spec.type == FrameType::ROWS) {
        return compute_row_frame_binary(input, probe_row);
    } else if (frame_spec.type == FrameType::RANGE) {
        return compute_range_frame_binary(input, probe_row);
    } else {
        throw std::runtime_error("Unsupported frame type in binary window execution.");
    }
}