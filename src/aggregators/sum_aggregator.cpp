//
// Created by Vegim Shala on 10.7.25.
//
#include "aggregators/sum_aggregator.h"
#include <numeric>

double SumAggregator::compute(const std::vector<double>& values) const {
    return std::accumulate(values.begin(), values.end(), 0.0);
}