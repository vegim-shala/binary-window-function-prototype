#include "data_io.h"
#include <vector>
#include <fstream>
#include <sstream>
#include <iostream>
#include "data_processing.h"
#include <numeric>
#include <operators/binary_window_function_operator.h>
#include <chrono>
#include <cmath>

using namespace std;

void print_raw_data(const std::vector<Row>& data) {
    std::cout << "Raw data:" << std::endl;
    std::cout << "id,value:" << std::endl;
    for (const auto& row : data) {
        std::cout << row.id << "," << row.value << std::endl;
    }
}


int main() {
    auto start = std::chrono::high_resolution_clock::now();

    auto [input, input_schema] = read_csv("UC1_input.csv");
    auto [probe, probe_schema] = read_csv("UC1_input.csv");
    // verify_binary_file("dynamic_columns.bin");
    // auto [data, schema] = read_binary("sensor.bin");

    cout << "Input: " << endl;
    print_dataset(input, input_schema, 100);
    cout << "Probe: " << endl;
    print_dataset(probe, probe_schema, 100);

    BinaryWindowFunctionModel model;
    model.value_column = "value"; // Column used for aggregation
    model.partition_column = "category"; // Optional, can leave empty for global
    model.order_column = "timestamp";
    model.output_column = "sum_result";
    model.frame_spec = FrameSpec{
        .type = FrameType::ROWS,
        .preceding = 1,  // Time range
        .following = 0
    };
    model.agg_type = AggregationType::SUM;

    BinaryWindowFunctionOperator op(model);

    auto [result, new_schema] = op.execute(input, probe, probe_schema);

    print_dataset(result, new_schema, 100);

    cout << "Output: " << endl;
    write_csv("UC1_result.csv", result);

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    std::cout << "Execution Time: " << duration.count() << " ms\n";

    return 0;
}