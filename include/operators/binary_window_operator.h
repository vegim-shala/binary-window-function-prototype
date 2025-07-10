//
// Created by Vegim Shala on 10.7.25.
//

#pragma once
#include <string>
#include "aggregators/factory.h"
#include "operators/utils/frame_utils.h"
#include "data_io.h"

struct BinaryWindowSpec {
    std::string value_column;
    std::string partition_column; // optional
    std::string order_column;
    std::string target_column; // the column we aggregate on
    std::string output_column;

    FrameSpec frame_spec;
    AggregationType agg_type;
};

class BinaryWindowOperator {
public:
    explicit BinaryWindowOperator(BinaryWindowSpec spec)
        : spec(std::move(spec)),
          aggregator(create_aggregator(this->spec.agg_type)) {
    }

    std::pair<Dataset, FileSchema> execute(const Dataset &input, FileSchema schema);

private:
    BinaryWindowSpec spec;
    std::unique_ptr<Aggregator> aggregator;

    // double apply_function(const std::vector<double> &values) const;
};