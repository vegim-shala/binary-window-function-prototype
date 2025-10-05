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