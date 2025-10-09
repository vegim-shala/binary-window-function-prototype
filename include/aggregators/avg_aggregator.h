//
// Created by Vegim Shala on 10.7.25.
//

#pragma once
#include "aggregator.h"
#include "count_aggregator.h"
#include "sum_aggregator.h"

class AvgAggregator : public Aggregator {
public:
    void build_from_values(const std::vector<int32_t> &values) override;

    void build_from_values_segtree(const std::vector<int32_t> &values) override;

    void build_from_values_sqrt_tree(const std::vector<int32_t> &vals) override;

    int64_t query(size_t lo, size_t hi) const override;

    int64_t seg_query(size_t lo, size_t hi) const override;

    int64_t sqrt_query(size_t lo, size_t hi) const override;

private:
    SumAggregator sum;
    CountAggregator count;
};
