#pragma once

#include <string>
#include "aggregators/factory.h"
#include "operators/utils/frame_utils.h"
#include "data_io.h"

struct BinaryWindowFunctionModel {
    std::string value_column;
    std::string partition_column; // optional
    std::string order_column;
    std::string output_column;

    FrameSpec frame_spec;
    AggregationType agg_type;
};

class BinaryWindowFunctionOperator {
public:
    explicit BinaryWindowFunctionOperator(BinaryWindowFunctionModel spec)
        : spec(std::move(spec)),
          aggregator(create_aggregator(this->spec.agg_type)),
          frame_utils(this->spec.frame_spec, this->spec.order_column) {
    }

    std::pair<Dataset, FileSchema> execute(const Dataset& input, const Dataset& probe, FileSchema schema);

private:
    BinaryWindowFunctionModel spec;
    std::unique_ptr<Aggregator> aggregator;
    FrameUtils frame_utils;

    std::string extract_partition_key(const DataRow& row) const;
};
