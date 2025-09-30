//
// Created by Vegim Shala on 10.7.25.
//

#pragma once

#include "data_io.h"
#include "thread_pool.h"

enum class JoinType { RANGE, ROWS };

struct JoinSpec {
    JoinType type = JoinType::RANGE;
    std::string begin_column;
    std::string end_column;
};

class JoinUtils {
public:
    explicit JoinUtils(const JoinSpec &join_spec, const std::string &order_column)
        : join_spec(join_spec), order_column(order_column) {
    }

    void build_index(const Dataset &input, const FileSchema &schema, std::string &value_column);

    void build_index_from_vectors_segtree(const std::vector<uint32_t> &sorted_keys,
                                          const std::vector<uint32_t> &values);

    // void build_index_from_vectors_segtree_parallel(const std::vector<uint32_t>& sorted_keys, const std::vector<uint32_t>& values,
    //                                                           ThreadPool& pool);

    void build_index_from_vectors_prefix_sums(const std::vector<uint32_t> &sorted_keys,
                                              const std::vector<uint32_t> &values);

    void build_index_from_vectors_sqrt_tree(const std::vector<uint32_t> &sorted_keys,
                                            const std::vector<uint32_t> &values);

    void build_index_from_vectors_two_pointer_sweep(const std::vector<uint32_t> &sorted_keys,
                                                    const std::vector<uint32_t> &values);


    inline uint64_t JoinUtils::seg_query(size_t l, size_t r) const {
        uint64_t res = 0;
        for (l += n, r += n; l < r; l >>= 1, r >>= 1) {
            if (l & 1) res += segtree[l++];
            if (r & 1) res += segtree[--r];
        }
        return res;
    }

    void build_index_from_vectors_segtree_top_down(const std::vector<uint32_t> &sorted_keys,
                                                   const std::vector<uint32_t> &values);

    double seg_query_top_down(size_t ql, size_t qr) const;

    double prefix_sums_query(size_t l, size_t r) const;

    double sqrt_query(size_t l, size_t r) const;

private:
    JoinSpec join_spec;
    const std::string order_column;

    // For Segment Tree
    size_t n = 0;
    std::vector<uint32_t> keys; // sorted probe/build keys
    std::vector<uint64_t> segtree; // 1..(2n-1) used (0 unused)

    // For Prefix Sums
    std::vector<uint32_t> prefix_sums;

    // For SQRT Tree
    std::vector<uint32_t> values; // store raw values
    std::vector<uint32_t> block_sums; // sums per block
    size_t block_size = 0;
};
