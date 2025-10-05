//
// Created by Vegim Shala on 10.7.25.
//

#pragma once
#include <vector>
#include <span>

#include "data_io.h"

class Aggregator {
public:
    virtual ~Aggregator() = default;

    // Build from spans (no copy)
    virtual void build_from_values(const std::vector<int32_t> &values) = 0;

    virtual int64_t query(size_t lo, size_t hi) const = 0;
};
