//
// Created by Vegim Shala on 10.7.25.
//

#pragma once

#include "data_io.h"

enum class JoinType { RANGE, ROWS };

struct JoinSpec {
    JoinType type = JoinType::RANGE;
    std::string begin_column;
    std::string end_column;
};

class JoinUtils {
public:
    explicit JoinUtils(const JoinSpec &join_spec, const std::string &order_column)
          : join_spec(join_spec), order_column(order_column) {}

    void pretty_print_segment_tree() const;

    void build_index(const Dataset& input, const FileSchema &schema, std::string& value_column);

    // New: Direct SUM computation via binary search + segment tree
    double compute_sum_range(const Dataset& input, const FileSchema &schema, const DataRow& probe_row,
                             const std::string& start_col, const std::string& end_col) const;


    std::vector<size_t> compute_range_join(
        const Dataset& input, const FileSchema &schema, const DataRow& probe_row,
        const std::string& begin_col, const std::string& end_col) const;

    std::vector<size_t> compute_join(const Dataset& input, const FileSchema &schema, const DataRow& probe_row) const;

    void validate() const;

private:
    JoinSpec join_spec;
    const std::string order_column;

    // For fast queries
    std::vector<double> keys;    // sorted order column values
    std::vector<double> segtree; // flat segment tree storing sums
    size_t n = 0;                // number of elements

    double seg_query(size_t l, size_t r) const;
};
