//
// Created by Vegim Shala on 10.7.25.
//

#pragma once
#include <map>
#include <map>
#include <map>
#include <map>

#include "data_io.h"

enum class FrameType { RANGE, ROWS };

struct FrameSpec {
    FrameType type;
    std::optional<int> preceding;
    std::optional<int> following;
    std::string begin_column;
    std::string end_column;
};

class FrameUtils {
public:
    explicit FrameUtils(const FrameSpec &frame_spec, const std::vector<std::string> &order_columns)
          : frame_spec(frame_spec), order_columns(order_columns) {}

    std::vector<size_t> compute_range_indices(
        const Dataset &input,
        size_t current_index) const;

    std::vector<size_t> compute_row_frame_static_binary(const Dataset& input, const DataRow& probe_row) const;
    std::vector<size_t> compute_range_frame_static_binary(const Dataset& input, const DataRow& probe_row) const;

    std::vector<size_t> compute_row_frame_dynamic_binary(
        const Dataset& input, const DataRow& probe_row,
        const std::string& begin_col, const std::string& end_col) const;

    std::vector<size_t> compute_range_frame_dynamic_binary(
        const Dataset& input, const DataRow& probe_row,
        const std::string& begin_col, const std::string& end_col) const;

    std::vector<size_t> compute_binary_frame_indices(const Dataset& input, const DataRow& probe_row) const;

    std::vector<size_t> compute_row_frame(
        const Dataset &input,
        size_t current_index) const;

    std::vector<size_t> compute_range_frame(
        const Dataset &input,
        size_t current_index) const;

    void validate() const;

private:
    FrameSpec frame_spec;
    const std::vector<std::string> order_columns;
};
