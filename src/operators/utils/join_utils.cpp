//
// Created by Vegim Shala on 10.7.25.
//

#include "operators/utils/join_utils.h"

#include <iostream>

using OrderKey = std::vector<double>;

OrderKey extract_order_key_join(const DataRow& row, const std::vector<std::string>& order_columns) {
    OrderKey key;
    for (const auto& col : order_columns) {
        key.push_back(extract_numeric(row.at(col)));
    }
    return key;
}

void JoinUtils::pretty_print_segment_tree() const {
    if (segtree.empty()) {
        std::cout << "Segment tree is empty!" << std::endl;
        return;
    }

    // Calculate tree height
    size_t height = 0;
    size_t nodes = segtree.size() - 1; // Exclude index 0 if using 1-based
    while (nodes > 0) {
        height++;
        nodes /= 2;
    }

    std::cout << "Segment Tree Structure:" << std::endl;

    size_t level_start = 1;
    size_t level_size = 1;

    for (size_t level = 0; level < height; level++) {
        std::cout << "Level " << level << ": ";

        for (size_t i = 0; i < level_size && level_start + i < segtree.size(); i++) {
            std::cout << segtree[level_start + i] << " ";
        }
        std::cout << std::endl;

        level_start += level_size;
        level_size *= 2;
    }
}

void JoinUtils::build_index(const Dataset& input, std::string& value_column) {
    n = input.size();
    keys.resize(n);
    std::vector<double> values(n);

    for (size_t i = 0; i < n; i++) {
        keys[i] = extract_numeric(input[i].at(order_column));
        values[i] = extract_numeric(input[i].at(value_column));
        // default: sum over value_column, adjust if needed
    }

    // Build segment tree for SUM
    segtree.assign(2 * n, 0.0);
    for (size_t i = 0; i < n; i++) {
        segtree[n + i] = values[i];
    }
    for (size_t i = n - 1; i > 0; --i) {
        segtree[i] = segtree[i << 1] + segtree[i << 1 | 1];
    }

    // pretty_print_segment_tree();

}



double JoinUtils::seg_query(size_t l, size_t r) const {
    double res = 0.0;
    for (l += n, r += n; l < r; l >>= 1, r >>= 1) {
        if (l & 1) res += segtree[l++];
        if (r & 1) res += segtree[--r];
    }
    return res;
}

double JoinUtils::compute_sum_range(const Dataset& input, const DataRow& probe_row,
                                    const std::string& start_col, const std::string& end_col) const {
    if (n == 0) return 0.0;

    double start = start_col.empty() ? -std::numeric_limits<double>::infinity()
                                     : extract_numeric(probe_row.at(start_col));
    double end   = end_col.empty()   ? std::numeric_limits<double>::infinity()
                                     : extract_numeric(probe_row.at(end_col));

    auto lo_it = std::lower_bound(keys.begin(), keys.end(), start);
    auto hi_it = std::upper_bound(keys.begin(), keys.end(), end);

    size_t lo = lo_it - keys.begin();
    size_t hi = hi_it - keys.begin();

    if (lo >= hi) return 0.0;
    return seg_query(lo, hi); // fast SUM
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


