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

#include <string>
#include <filesystem>
#include <operators/binary_window_function_operator.h>

namespace fs = std::filesystem;

class Benchmark {
public:
    void run_benchmarks() {
        std::cout << "=== Binary Window Function Benchmark Suite ===\n\n";

        // Test different scenarios
        // test_large_partitions();
        test_many_small_partitions();
        // test_mixed_scenarios();

        std::cout << "\n=== Benchmark Complete ===\n";
    }

private:

    BinaryWindowFunctionModel create_test_model() {
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
        return model;
    }

    void test_large_partitions() {
        std::cout << "1. Scenario: Few Large Partitions\n";

        std::vector<std::pair<std::string, std::string>> files = {
            {"official_duckdb_test/input1.csv", "official_duckdb_test/probe1.csv"},
            {"official_duckdb_test/input2.csv", "official_duckdb_test/probe2.csv"},
            {"official_duckdb_test/input3.csv", "official_duckdb_test/probe3.csv"},
            {"official_duckdb_test/input4.csv", "official_duckdb_test/probe4.csv"}
        };

        for(int i = 0; i < files.size(); i++) {
            auto [input, input_schema] = read_csv_optimized(files[i].first);
            auto [probe, probe_schema] = read_csv_optimized(files[i].second);

            std::cout << "Processing " << files[i].first << " and " << files[i].second << "\n\n";

            BinaryWindowFunctionModel model = create_test_model();
            BinaryWindowFunctionOperator op(model);

            auto [result, new_schema] = op.execute(input, probe, input_schema, probe_schema);
            std::cout << "DONE WITH " << files[i].first << " and " << files[i].second << "\n\n";
        }
    }

    void test_many_small_partitions() {
        std::cout << "2. Scenario: Many Small Partitions\n";

        std::vector<std::pair<std::string, std::string>> files = {
            {"small_partitions/input1.csv", "small_partitions/probe1.csv"},
            {"small_partitions/input2.csv", "small_partitions/probe2.csv"},
            {"small_partitions/input3.csv", "small_partitions/probe3.csv"},
            {"small_partitions/input4.csv", "small_partitions/probe4.csv"}
        };

        for(int i = 0; i < files.size(); i++) {
            auto [input, input_schema] = read_csv_optimized(files[i].first);
            auto [probe, probe_schema] = read_csv_optimized(files[i].second);

            std::cout << "Processing " << files[i].first << " and " << files[i].second << "\n\n";

            BinaryWindowFunctionModel model = create_test_model();
            BinaryWindowFunctionOperator op(model);

            auto [result, new_schema] = op.execute(input, probe, input_schema, probe_schema);
            std::cout << "DONE WITH " << files[i].first << " and " << files[i].second << "\n\n";
        }
    }

    void test_mixed_scenarios() {
//        std::cout << "3. Testing MIXED scenarios\n";
//
//        std::vector<std::pair<int, int>> scenarios = {
//            {100, 1000},    // 100 partitions, 1K rows
//            {1000, 100},    // 1K partitions, 100 rows
//            {500, 500},     // Balanced
//            {50, 10000}     // Very large partitions
//        };
//
//        for (size_t i = 0; i < scenarios.size(); ++i) {
//            auto [partitions, rows_per_part] = scenarios[i];
//            std::string name = "test_mixed_" + std::to_string(i);
//
//            std::cout << "   Scenario " << i+1 << ": " << partitions
//                      << " partitions, " << rows_per_part << " rows each\n";
//
//            generate_test_data(name, partitions, rows_per_part);
//
//            auto [input, schema] = read_csv(name + "_input.csv");
//            auto [probe, _] = read_csv(name + "_probe.csv");
//
//            BinaryWindowFunctionOperator op;
//            auto spec = create_test_spec();
//
//            auto start = std::chrono::high_resolution_clock::now();
//            auto [result, result_schema] = op.execute(input, probe, schema, spec);
//            auto end = std::chrono::high_resolution_clock::now();
//
//            auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
//            std::cout << "   Time: " << duration.count() << " ms, "
//                      << "Results: " << result.size() << "\n";
//
//            cleanup_test_data(name);
//        }
//        std::cout << "\n";
    }

//    void generate_test_data(const std::string& base_name, int n_partitions, int rows_per_part) {
//        // Use Python script or C++ data generation
//        std::string cmd = "python generate_test_data.py " +
//                         std::to_string(n_partitions) + " " +
//                         std::to_string(rows_per_part) + " " +
//                         base_name;
//        system(cmd.c_str());
//    }
//
//    void cleanup_test_data(const std::string& base_name) {
//        fs::remove(base_name + "_input.csv");
//        fs::remove(base_name + "_probe.csv");
//    }
};

int main() {
    Benchmark benchmark;
    benchmark.run_benchmarks();
    return 0;
}