//
// Created by Vegim Shala on 10.7.25.
//

#pragma once

#include "data_io.h"

enum class JoinType2 { RANGE, ROWS };

struct JoinSpec2 {
    JoinType2 type = JoinType2::RANGE;
    std::string begin_column;
    std::string end_column;
};

class JoinUtils2 {
public:
    explicit JoinUtils2(const JoinSpec2 &join_spec, const std::string &order_column)
          : join_spec(join_spec), order_column(order_column) {}

    void pretty_print_segment_tree() const;

    void build_index(const Dataset& input, const FileSchema &schema, std::string& value_column);
    void build_index_from_vectors_segtree(const std::vector<uint32_t> &sorted_keys, const std::vector<uint32_t> &values);
    void build_index_from_vectors_prefix_sums(const std::vector<uint32_t> &sorted_keys, const std::vector<uint32_t> &values);
    void build_index_from_vectors_sqrt_tree(const std::vector<uint32_t> &sorted_keys, const std::vector<uint32_t> &values);
    void build_index_from_vectors_two_pointer_sweep(const std::vector<uint32_t> &sorted_keys, const std::vector<uint32_t> &values);

    std::vector<uint32_t> sweep_query(const std::vector<std::pair<uint32_t, uint32_t>> &probe_ranges) const;

    // New: Direct SUM computation via binary search + segment tree
    double compute_sum_range(const Dataset& input, const FileSchema &schema, const DataRow& probe_row,
                             const std::string& start_col, const std::string& end_col) const;


    std::vector<size_t> compute_range_join(
        const Dataset& input, const FileSchema &schema, const DataRow& probe_row,
        const std::string& begin_col, const std::string& end_col) const;

    std::vector<size_t> compute_join(const Dataset& input, const FileSchema &schema, const DataRow& probe_row) const;

    void validate() const;
    double seg_query(size_t l, size_t r) const;

    void build_index_from_vectors_segtree_top_down(const std::vector<uint32_t> &sorted_keys,
                                                   const std::vector<uint32_t> &values);

    double seg_query_top_down(size_t ql, size_t qr) const;

    double prefix_sums_query(size_t l, size_t r) const;
    double sqrt_query(size_t l, size_t r) const;

private:
    JoinSpec2 join_spec;
    const std::string order_column;

    // For Segment Tree
    std::vector<uint32_t> keys;    // sorted order column values
    std::vector<uint32_t> segtree; // flat segment tree storing sums

    // For Prefix Sums
    std::vector<uint32_t> prefix_sums;

    // For SQRT Tree
    std::vector<uint32_t> values;       // store raw values
    std::vector<uint32_t> block_sums;   // sums per block
    size_t block_size = 0;
    size_t n = 0;                // number of elements


};
