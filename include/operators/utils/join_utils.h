//
// Created by Vegim Shala on 10.7.25.
//

#pragma once

#include "data_io.h"

enum class JoinType { RANGE, ROWS };

struct JoinSpec {
    JoinType type;
    std::string begin_column;
    std::string end_column;
};

class JoinUtils {
public:
    explicit JoinUtils(const JoinSpec &join_spec, const std::vector<std::string> &order_columns)
          : join_spec(join_spec), order_columns(order_columns) {}

    std::vector<size_t> compute_range_join(
        const Dataset& input, const DataRow& probe_row,
        const std::string& begin_col, const std::string& end_col) const;

    std::vector<size_t> compute_rows_join(
        const Dataset& input, const DataRow& probe_row,
        const std::string& begin_col, const std::string& end_col) const;

    std::vector<size_t> compute_join(const Dataset& input, const DataRow& probe_row) const;

    void validate() const;

private:
    JoinSpec join_spec;
    const std::vector<std::string> order_columns;
};
