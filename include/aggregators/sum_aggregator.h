//
// Created by Vegim Shala on 10.7.25.
//

#pragma once
#include "aggregator.h"
#include <numeric>

class SumAggregator : public Aggregator {
public:
    void build_from_values(const std::vector<int32_t> &values) override;

    int64_t query(size_t lo, size_t hi) const override;

private:
    std::vector<int64_t> prefix;
};