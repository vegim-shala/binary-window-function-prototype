//
// Created by Vegim Shala on 10.7.25.
//

#pragma once
#include "aggregator.h"
#include <numeric>

class MaxAggregator : public Aggregator {
public:
    double compute(const std::vector<double>& values) const override;
};