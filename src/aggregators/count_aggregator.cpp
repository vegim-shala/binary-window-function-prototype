//
// Created by Vegim Shala on 10.7.25.
//
#include "aggregators/count_aggregator.h"
#include <numeric>

double CountAggregator::compute(const std::vector<double>& values) const {
    return static_cast<double>(values.size());
}