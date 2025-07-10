//
// Created by Vegim Shala on 10.7.25.
//
#include "aggregators/avg_aggregator.h"
#include <numeric>

double AvgAggregator::compute(const std::vector<double>& values) const {
    if (values.empty()) return 0.0;
    return std::accumulate(values.begin(), values.end(), 0.0) / values.size();
}