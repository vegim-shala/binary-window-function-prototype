//
// Created by Vegim Shala on 10.7.25.
//

#pragma once
#include <string>
#include "aggregators/factory.h"
#include "data_io.h"

struct WindowFunctionModel {
    std::string value_column;
    std::vector<std::string> partition_columns;  // optional
    std::vector<std::string> order_columns;
    std::string target_column; // the column we aggregate on
    std::string output_column;

    FrameSpec frame_spec;
    AggregationType agg_type;
};

class WindowFunctionOperator {
public:
    explicit WindowFunctionOperator(WindowFunctionModel spec)
        : spec(std::move(spec)),
          aggregator(create_aggregator(this->spec.agg_type)) {
    }

    std::pair<Dataset, FileSchema> execute(const Dataset &input, FileSchema schema);

private:
    WindowFunctionModel spec;
    std::unique_ptr<Aggregator> aggregator;

    // double apply_function(const std::vector<double> &values) const;
};