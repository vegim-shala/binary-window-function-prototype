//
// Created by Vegim Shala on 10.7.25.
//

#pragma once
#include <map>
#include <map>
#include <map>

#include "data_io.h"

enum class FrameType { RANGE, ROWS };

struct FrameSpec {
    FrameType type;
    int preceding;
    int following;
};

class FrameUtils {
public:
    explicit FrameUtils(const FrameSpec &frame_spec, const std::string &order_column)
          : frame_spec(frame_spec), order_column(order_column) {}

    std::vector<size_t> compute_range_indices(
        const Dataset &input,
        size_t current_index) const;

    std::vector<size_t> compute_range_frame_binary(const Dataset &input, const DataRow &probe_row) const;

    std::vector<size_t> compute_row_frame_binary(const Dataset &input, const DataRow &probe_row) const;

    std::vector<size_t> compute_binary_frame_indices(const Dataset &input, const DataRow &probe_row) const;

    std::vector<size_t> compute_row_frame(
        const Dataset &input,
        size_t current_index) const;

    std::vector<size_t> compute_range_frame(
        const Dataset &input,
        size_t current_index) const;

private:
    FrameSpec frame_spec;
    std::string order_column;
};
