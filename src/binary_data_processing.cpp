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

int main() {
    cout << "START PROCESSING:" << endl;

    auto [input, input_schema] = read_csv_optimized("first_test/input4.csv");
    auto [probe, probe_schema] = read_csv_optimized("first_test/probe4.csv");
    // verify_binary_file("dynamic_columns.bin");
    // auto [data, schema] = read_binary("sensor.bin");

    cout << "Input: " << endl;
    print_dataset(input, input_schema, 100);
    cout << "Probe: " << endl;
    print_dataset(probe, probe_schema, 100);

    BinaryWindowFunctionModel model;
    model.value_column = "value";
    model.partition_columns = {"category"};
    model.order_column = "timestamp";
    model.output_column = "sum_result";

    // RANGE frame based on begin_col / end_col in the probe
    model.join_spec = JoinSpec{
        .begin_column = "begin_col",
        .end_column = "end_col"
    };

    model.agg_type = AggregationType::SUM;

    BinaryWindowFunctionOperator op(model);

    auto start = std::chrono::high_resolution_clock::now();

    auto [result, new_schema] = op.execute(input, probe, input_schema, probe_schema);

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    std::cout << "Execution Time: " << duration.count() << " milliseconds\n";

    print_dataset(result, new_schema, 100);

    cout << "Output: " << endl;
    write_csv("first_test/output3.csv", result, new_schema);

    return 0;
}