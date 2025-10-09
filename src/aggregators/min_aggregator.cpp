//
// Created by Vegim Shala on 10.7.25.
//
#include "aggregators/min_aggregator.h"
#include <numeric>

void MinAggregator::build_from_values(const std::vector<int32_t> &values) {
    n = values.size();
    segtree.assign(2 * n, std::numeric_limits<int32_t>::max());
    for (size_t i = 0; i < n; ++i) segtree[n + i] = values[i];
    for (size_t i = n - 1; i > 0; --i)
        segtree[i] = std::min(segtree[i << 1], segtree[i << 1 | 1]);
}

void MinAggregator::build_from_values_segtree(const std::vector<int32_t> &values) {
    return;
}

int64_t MinAggregator::seg_query(size_t lo, size_t hi) const {
    return 1;
}

void MinAggregator::build_from_values_sqrt_tree(const std::vector<int32_t> &vals) {
    return;
}

int64_t MinAggregator::sqrt_query(size_t lo, size_t hi) const {
    return 1;
}

int64_t MinAggregator::query(size_t l, size_t r) const {
    int64_t res = std::numeric_limits<int32_t>::max();
    for (l += n, r += n; l < r; l >>= 1, r >>= 1) {
        if (l & 1) res = std::min(res, segtree[l++]);
        if (r & 1) res = std::min(res, segtree[--r]);
    }
    return res;
}