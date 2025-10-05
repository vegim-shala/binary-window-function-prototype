//
// Created by Vegim Shala on 10.7.25.
//
#include "aggregators/factory.h"
#include "aggregators/avg_aggregator.h"
#include "aggregators/sum_aggregator.h"
#include "aggregators/count_aggregator.h"
#include "aggregators/min_aggregator.h"
#include "aggregators/max_aggregator.h"
#include <stdexcept>

std::unique_ptr<Aggregator> create_aggregator(AggregationType type) {
    switch (type) {
        case AggregationType::SUM: return std::make_unique<SumAggregator>();
        case AggregationType::COUNT: return std::make_unique<CountAggregator>();
        case AggregationType::AVG: return std::make_unique<AvgAggregator>();
        case AggregationType::MIN: return std::make_unique<MinAggregator>();
        case AggregationType::MAX: return std::make_unique<MaxAggregator>();
        default: throw std::invalid_argument("Unsupported aggregation type");
    }
}