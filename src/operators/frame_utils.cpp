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