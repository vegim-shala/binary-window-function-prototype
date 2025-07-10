//
// Created by Vegim Shala on 6.7.25.
//

#ifndef DATA_PROCESSING_H
#define DATA_PROCESSING_H
#include <numeric>
#include "aggregators/factory.h"
#include "operators/utils/frame_utils.h"

#endif //DATA_PROCESSING_H

#pragma once
#include "data_io.h"
#include <optional>

std::pair<Dataset, FileSchema> compute_moving_avg(
    const Dataset &data,
    const std::string &value_column, // column in which the moving average is calculated
    const std::string &output_column, // name of the output column
    int window_size,
    FileSchema &schema);






