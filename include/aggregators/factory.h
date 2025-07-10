//
// Created by Vegim Shala on 10.7.25.
//

#pragma once
#include "aggregator.h"

enum class AggregationType { AVG, SUM, COUNT, MIN, MAX };

std::unique_ptr<Aggregator> create_aggregator(AggregationType type);