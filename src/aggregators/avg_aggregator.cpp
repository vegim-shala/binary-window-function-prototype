//
// Created by Vegim Shala on 10.7.25.
//
#include "aggregators/avg_aggregator.h"
#include <numeric>

void AvgAggregator::build_from_values(const std::vector<int32_t> &values) {
    sum.build_from_values(values);
    count.build_from_values(values);
}

int64_t AvgAggregator::query(size_t lo, size_t hi) const {
    int64_t c = count.query(lo, hi);
    return (c == 0) ? 0 : sum.query(lo, hi) / c;
}


void AvgAggregator::build_from_values_segtree(const std::vector<int32_t> &values) {
    return;
}

int64_t AvgAggregator::seg_query(size_t lo, size_t hi) const {
    return 1;
}

void AvgAggregator::build_from_values_sqrt_tree(const std::vector<int32_t> &vals) {
    return;
}

int64_t AvgAggregator::sqrt_query(size_t lo, size_t hi) const {
    return 1;
}