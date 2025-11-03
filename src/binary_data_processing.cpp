#include "data_io.h"
#include <vector>
#include <sstream>
#include <iostream>
#include <operators/binary_window_function_operator.h>
#include <chrono>

using namespace std;

int main() {
    cout << "START PROCESSING:" << endl;

    int num_threads = std::thread::hardware_concurrency();

    auto start_reading = std::chrono::high_resolution_clock::now();

    auto [input, input_schema] = read_csv_fast_parallel("Z1/input10.csv", num_threads);
    auto [probe, probe_schema] = read_csv_fast_parallel("Z1/probe10.csv", num_threads);
    // verify_binary_file("dynamic_columns.bin");
    // auto [data, schema] = read_binary("sensor.bin");

    auto end_reading = std::chrono::high_resolution_clock::now();
    auto duration_reading = std::chrono::duration_cast<std::chrono::milliseconds>(end_reading - start_reading);
    std::cout << "Time taken for READING: " << duration_reading.count() << " ms" << std::endl;

    // cout << "Input: " << endl;
    // print_dataset(input, input_schema, 100);
    // cout << "Probe: " << endl;
    // print_dataset(probe, probe_schema, 100);

    BinaryWindowFunctionModel model;
    // BinaryWindowFunctionModel2 model;

    model.value_column = "value";
    model.partition_columns = {"category"}; // For one partitioning column
    // model.partition_columns = {"category1", "category2"}; // For many partitioning columns
    model.order_column = "timestamp";
    model.output_column = "sum_result";

    // RANGE frame based on begin_col / end_col in the probe
    model.join_spec = JoinSpec{
    // model.join_spec = JoinSpec2{
        .begin_column = "begin_col",
        .end_column = "end_col"
    };

    model.agg_type = AggregationType::SUM;

    BinaryWindowFunctionOperator op(model);
    // BinaryWindowFunctionOperator2 op(model);

    auto start = std::chrono::high_resolution_clock::now();

    auto [result, new_schema] = op.execute(input, probe, input_schema, probe_schema);

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    std::cout << "Execution Time: " << duration.count() << " milliseconds\n";

    // print_dataset(result, new_schema, 100);

    cout << "Output: " << endl;
    write_csv("official_duckdb_test/output.csv", result, new_schema);

    return 0;
}