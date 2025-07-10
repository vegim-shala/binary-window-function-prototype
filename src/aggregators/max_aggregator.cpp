//
// Created by Vegim Shala on 10.7.25.
//
#include "aggregators/max_aggregator.h"
#include <numeric>

double MaxAggregator::compute(const std::vector<double>& values) const {
    if (values.empty()) return 0.0;
    return *std::max_element(values.begin(), values.end());
}