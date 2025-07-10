//
// Created by Vegim Shala on 10.7.25.
//
#include "aggregators/min_aggregator.h"
#include <numeric>

double MinAggregator::compute(const std::vector<double>& values) const {
    if (values.empty()) return 0.0;
    return *std::min_element(values.begin(), values.end());
}