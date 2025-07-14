#include "data_io.h"
#include <vector>
#include <fstream>
#include <sstream>
#include <iostream>
#include "data_processing.h"
#include <numeric>
#include <operators/window_function_operator.h>
#include <cmath>

using namespace std;

void print_raw_data(const std::vector<Row>& data) {
    std::cout << "Raw data:" << std::endl;
    std::cout << "id,value:" << std::endl;
    for (const auto& row : data) {
        std::cout << row.id << "," << row.value << std::endl;
    }
}

std::pair<Dataset, FileSchema>  compute_moving_avg(
    const Dataset& data,
    const string& value_column, // column in which the moving average is calculated
    const string& output_column, // name of the output column
    int window_size,
    FileSchema& schema)
{
    if (data.empty()) throw std::runtime_error("data is empty");

    Dataset result; // we want to return a dataset
    vector<double> window_values; // For each row, we will have a window over a few rows on which we will perform calculations
    window_values.reserve(window_size); // Reserve only the required size for this data structure

    // Verify the value column exists and is numeric
    bool column_valid = false;
    bool is_double = false;

    // Check first row for column type
    if (!data[0].count(value_column)) {
        cerr << "Column '" << value_column << "' not found in data\n";
        throw std::runtime_error("Column '" + value_column + "' not found in data");
    }

    try {
        get<double>(data[0].at(value_column));
        is_double = true;
        column_valid = true;
    } catch (const bad_variant_access&) {
        try {
            get<int>(data[0].at(value_column));
            column_valid = true;
        } catch (const bad_variant_access&) {
            cerr << "Column '" << value_column << "' is not numeric\n";
            throw std::runtime_error("Column '" + value_column + "' is not numeric");
        }
    }

    if (!column_valid) throw std::runtime_error("Column '" + value_column + "' is invalid");

    for (size_t i = 0; i < data.size(); ++i) { // iterate through the data rows
        DataRow new_row = data[i]; // current row
        double current_value = 0.0;

        // Extract the numeric value
        try {
            if (is_double) {
                current_value = get<double>(data[i].at(value_column));
            } else {
                current_value = static_cast<double>(get<int>(data[i].at(value_column)));
            }
        } catch (...) {
            cerr << "Error reading value from row " << i << endl;
            throw std::runtime_error("Error reading value from row ");
        }

        // Maintain sliding window
        window_values.push_back(current_value); // add the current value to the window
        if (window_values.size() > static_cast<size_t>(window_size)) {
            window_values.erase(window_values.begin()); // if window has more elements that it's supposed to, erase the first element
        }

        // Calculate average
        double avg = accumulate(window_values.begin(), window_values.end(), 0.0) / window_values.size();

        // Add to result
        new_row[output_column] = avg;
        result.push_back(std::move(new_row));
    }

    schema.columns.push_back(output_column);
    schema.column_types[output_column] = "int32";

    return {result, schema};
}


int main() {
   auto [data, schema] = read_csv("sensor_w_partitions.csv");
    // verify_binary_file("dynamic_columns.bin");
    // auto [data, schema] = read_binary("sensor.bin");

    print_dataset(data, schema, 100);
    WindowFunctionModel spec;
    spec.partition_column = "region"; // Optional
    spec.order_column = "timestamp";
    spec.value_column = "temperature";
    spec.output_column = "temp_avg";
    spec.agg_type = AggregationType::AVG;
    spec.frame_spec = {FrameType::ROWS, 1, 1};
    WindowFunctionOperator op(spec);

    auto [result, new_schema] = op.execute(data, schema);
    // auto [result, resultSchema] = compute_moving_avg(data, "age", "window_avg", 2, schema);
    print_dataset(result, new_schema, 100);
    // if (result) {
    //     // Success - use *result
    write_csv("sensor_w_partitions_output.csv", result);
    // } else {
    //     // Handle error
    // }
    return 0;
}