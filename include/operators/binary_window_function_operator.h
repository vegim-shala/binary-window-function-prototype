#pragma once

#include <string>
#include "aggregators/factory.h"
#include "operators/utils/join_utils.h"
#include "data_io.h"

struct BinaryWindowFunctionModel {
    std::string value_column;
    std::vector<std::string> partition_columns; // optional
    std::string order_column;
    std::string output_column;

    JoinSpec join_spec;
    AggregationType agg_type;
};

class BinaryWindowFunctionOperator {
public:
    explicit BinaryWindowFunctionOperator(BinaryWindowFunctionModel spec)
        : spec(std::move(spec)),
          aggregator(create_aggregator(this->spec.agg_type)),
          join_utils(this->spec.join_spec, this->spec.order_column) {
    }

    std::pair<Dataset, FileSchema> execute(const Dataset& input, const Dataset& probe, FileSchema schema);

private:
    BinaryWindowFunctionModel spec;
    std::unique_ptr<Aggregator> aggregator;
    JoinUtils join_utils;

    std::string extract_partition_key(const DataRow& row) const;
};
