//
// Created by Vegim Shala on 10.7.25.
//
#include "aggregators/max_aggregator.h"
#include <numeric>

void MaxAggregator::build_from_values(const std::vector<int32_t> &values) {
    n = values.size();
    segtree.assign(2 * n, std::numeric_limits<int32_t>::lowest());
    for (size_t i = 0; i < n; ++i) segtree[n + i] = values[i];
    for (size_t i = n - 1; i > 0; --i)
        segtree[i] = std::max(segtree[i << 1], segtree[i << 1 | 1]);
}

int64_t MaxAggregator::query(size_t l, size_t r) const {
    int64_t res = std::numeric_limits<int64_t>::lowest();
    for (l += n, r += n; l < r; l >>= 1, r >>= 1) {
        if (l & 1) res = std::max(res, segtree[l++]);
        if (r & 1) res = std::max(res, segtree[--r]);
    }
    return res;
}