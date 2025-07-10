//
// Created by Vegim Shala on 10.7.25.
//

#pragma once
#include <vector>

class Aggregator {
public:
    virtual ~Aggregator() = default;
    virtual double compute(const std::vector<double>& values) const = 0;
};