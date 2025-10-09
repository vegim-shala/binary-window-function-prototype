//
// Created by Vegim Shala on 10.7.25.
//

#pragma once
#include <vector>
#include <span>
#include <operators/utils/thread_pool.h>

#include "data_io.h"

class Aggregator {
public:
    virtual ~Aggregator() = default;

    // Build from spans (no copy)
    virtual void build_from_values(const std::vector<int32_t> &values) = 0;

    virtual void build_from_values_segtree(const std::vector<int32_t> &values) = 0;

    virtual void build_from_values_sqrt_tree(const std::vector<int32_t> &vals) = 0;

    virtual void build_from_values_parallel(const std::vector<int32_t> &values,
                                            ThreadPool &pool,
                                            size_t morsel_size) {
        (void) pool;
        (void) morsel_size;
        build_from_values(values);
    }

    virtual int64_t query(size_t lo, size_t hi) const = 0;
    virtual int64_t seg_query(size_t lo, size_t hi) const = 0;
    virtual int64_t sqrt_query(size_t lo, size_t hi) const = 0;

};
