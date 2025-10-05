//
// Created by Vegim Shala on 10.7.25.
//
#include "aggregators/sum_aggregator.h"

void SumAggregator::build_from_values(const std::vector<int32_t> &values) {
    prefix.resize(values.size() + 1);
    prefix[0] = 0;
    for (size_t i = 0; i < values.size(); ++i)
        prefix[i + 1] = prefix[i] + values[i];
}

int64_t SumAggregator::query(size_t lo, size_t hi) const {
    return prefix[hi] - prefix[lo];
}