//
// Created by Vegim Shala on 10.7.25.
//
#include "aggregators/count_aggregator.h"
#include <numeric>

void CountAggregator::build_from_values(const std::vector<int32_t> &values) {
    prefix.resize(values.size() + 1);
    for (size_t i = 0; i < values.size() + 1; ++i) prefix[i] = i;
}

int64_t CountAggregator::query(size_t lo, size_t hi) const {
    return prefix[hi] - prefix[lo];
}