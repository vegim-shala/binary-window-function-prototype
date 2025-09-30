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
#include <operators/binary_window_function_operator2.h>

namespace fs = std::filesystem;

class Benchmark {
public:
    void run_benchmarks(int algo_ind) {
        std::cout << "=== Binary Window Function Benchmark Suite ===\n\n";

        // Test different scenarios
        test_few_large_partitions(algo_ind); // A1
        test_many_small_partitions(algo_ind); // A2
        test_equal_partitions_and_rows(algo_ind); // A3
        test_less_partitions_in_probe(algo_ind); // B1
        test_less_partitions_in_input(algo_ind); // B2
        test_one_row_probe(algo_ind); // C1
        test_A1_bigger_ranges(algo_ind); // D1
        test_A1_very_small_ranges(algo_ind); // D2
        test_multiple_partitioning_columns(algo_ind); // E1

        std::cout << "\n=== Benchmark Complete ===\n";
    }

private:

    BinaryWindowFunctionModel create_test_model(int num_partition_cols) {
        BinaryWindowFunctionModel model;
        model.value_column = "value";
        model.partition_columns = {"category"};
        if (num_partition_cols > 1) {
            model.partition_columns = {"category1", "category2"};
        }
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

    BinaryWindowFunctionModel2 create_test_model2(int num_partition_cols) {
        BinaryWindowFunctionModel2 model;
        model.value_column = "value";
        model.partition_columns = {"category"};
        if (num_partition_cols > 1) {
            model.partition_columns = {"category1", "category2"};
        }
        model.order_column = "timestamp";
        model.output_column = "sum_result";

        // RANGE frame based on begin_col / end_col in the probe
        model.join_spec = JoinSpec2{
            .begin_column = "begin_col",
            .end_column = "end_col"
        };

        model.agg_type = AggregationType::SUM;
        return model;
    }

    void test_few_large_partitions(int algo_ind) {
        std::cout << "---------------------------------------------------------------------------------\n";
        std::cout << "A1. Scenario: Few Large Partitions\n";

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

            if (algo_ind == 1) {
                BinaryWindowFunctionModel model = create_test_model(1);
                BinaryWindowFunctionOperator op(model);

                auto [result, new_schema] = op.execute(input, probe, input_schema, probe_schema);
            } else if (algo_ind == 2) {
                BinaryWindowFunctionModel2 model = create_test_model2(1);
                BinaryWindowFunctionOperator2 op(model);

                auto [result, new_schema] = op.execute(input, probe, input_schema, probe_schema);
            }

            std::cout << "DONE WITH " << files[i].first << " and " << files[i].second << "\n\n";
        }
        std::cout << "---------------------------------------------------------------------------------\n";
    }

    void test_many_small_partitions(int algo_ind) {
        std::cout << "---------------------------------------------------------------------------------\n";
        std::cout << "A2. Scenario: Many Small Partitions\n";

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

            if (algo_ind == 1) {
                BinaryWindowFunctionModel model = create_test_model(1);
                BinaryWindowFunctionOperator op(model);

                auto [result, new_schema] = op.execute(input, probe, input_schema, probe_schema);
            } else if (algo_ind == 2) {
                BinaryWindowFunctionModel2 model = create_test_model2(1);
                BinaryWindowFunctionOperator2 op(model);

                auto [result, new_schema] = op.execute(input, probe, input_schema, probe_schema);
            }

            std::cout << "DONE WITH " << files[i].first << " and " << files[i].second << "\n\n";
        }
        std::cout << "---------------------------------------------------------------------------------\n";
    }

    void test_equal_partitions_and_rows(int algo_ind) {
        std::cout << "---------------------------------------------------------------------------------\n";
        std::cout << "A3. Scenario: Number of partitions and rows per partitions is equal\n";

        std::vector<std::pair<std::string, std::string>> files = {
            {"A3/input1.csv", "A3/probe1.csv"}, // 100 x 100
            {"A3/input2.csv", "A3/probe2.csv"}, // 320 x 320
            {"A3/input3.csv", "A3/probe3.csv"}, // 1000 x 1000
            {"A3/input4.csv", "A3/probe4.csv"} // 3200 x 3200
        };

        for(int i = 0; i < files.size(); i++) {
            auto [input, input_schema] = read_csv_optimized(files[i].first);
            auto [probe, probe_schema] = read_csv_optimized(files[i].second);

            std::cout << "Processing " << files[i].first << " and " << files[i].second << "\n\n";

            if (algo_ind == 1) {
                BinaryWindowFunctionModel model = create_test_model(1);
                BinaryWindowFunctionOperator op(model);

                auto [result, new_schema] = op.execute(input, probe, input_schema, probe_schema);
            } else if (algo_ind == 2) {
                BinaryWindowFunctionModel2 model = create_test_model2(1);
                BinaryWindowFunctionOperator2 op(model);

                auto [result, new_schema] = op.execute(input, probe, input_schema, probe_schema);
            }

            std::cout << "DONE WITH " << files[i].first << " and " << files[i].second << "\n\n";
        }
        std::cout << "---------------------------------------------------------------------------------\n";
    }

    void test_less_partitions_in_probe(int algo_ind) {
        std::cout << "---------------------------------------------------------------------------------\n";
        std::cout << "B1. Scenario: Probe has fewer partitions than Input (same partition sizes)\n";

        std::vector<std::pair<std::string, std::string>> files = {
            {"B1/input1.csv", "B1/probe1.csv"}, // 100x100 vs 10x100
            {"B1/input2.csv", "B1/probe2.csv"}, // 320x320 vs 32x320
            {"B1/input3.csv", "B1/probe3.csv"}, // 1000x1000 vs 100x1000
            {"B1/input4.csv", "B1/probe4.csv"} // 3200x3200 vs 320x3200
        };

        for(int i = 0; i < files.size(); i++) {
            auto [input, input_schema] = read_csv_optimized(files[i].first);
            auto [probe, probe_schema] = read_csv_optimized(files[i].second);

            std::cout << "Processing " << files[i].first << " and " << files[i].second << "\n\n";

            if (algo_ind == 1) {
                BinaryWindowFunctionModel model = create_test_model(1);
                BinaryWindowFunctionOperator op(model);

                auto [result, new_schema] = op.execute(input, probe, input_schema, probe_schema);
            } else if (algo_ind == 2) {
                BinaryWindowFunctionModel2 model = create_test_model2(1);
                BinaryWindowFunctionOperator2 op(model);

                auto [result, new_schema] = op.execute(input, probe, input_schema, probe_schema);
            }

            std::cout << "DONE WITH " << files[i].first << " and " << files[i].second << "\n\n";
        }
        std::cout << "---------------------------------------------------------------------------------\n";
    }

    void test_less_partitions_in_input(int algo_ind) {
        std::cout << "---------------------------------------------------------------------------------\n";
        std::cout << "B2. Scenario: Input has fewer partitions than Probe (same partition sizes)\n";

        std::vector<std::pair<std::string, std::string>> files = {
            {"B2/input1.csv", "B2/probe1.csv"}, // 10x100 vs 100x100
            {"B2/input2.csv", "B2/probe2.csv"}, // 32x320 vs 320x320
            {"B2/input3.csv", "B2/probe3.csv"}, // 100x1000 vs 1000x1000
            {"B2/input4.csv", "B2/probe4.csv"}  // 320x3200 vs 3200x3200
        };

        for(int i = 0; i < files.size(); i++) {
            auto [input, input_schema] = read_csv_optimized(files[i].first);
            auto [probe, probe_schema] = read_csv_optimized(files[i].second);

            std::cout << "Processing " << files[i].first << " and " << files[i].second << "\n\n";

            if (algo_ind == 1) {
                BinaryWindowFunctionModel model = create_test_model(1);
                BinaryWindowFunctionOperator op(model);

                auto [result, new_schema] = op.execute(input, probe, input_schema, probe_schema);
            } else if (algo_ind == 2) {
                BinaryWindowFunctionModel2 model = create_test_model2(1);
                BinaryWindowFunctionOperator2 op(model);

                auto [result, new_schema] = op.execute(input, probe, input_schema, probe_schema);
            }

            std::cout << "DONE WITH " << files[i].first << " and " << files[i].second << "\n\n";
        }
        std::cout << "---------------------------------------------------------------------------------\n";
    }

    void test_one_row_probe(int algo_ind) {
        std::cout << "---------------------------------------------------------------------------------\n";
        std::cout << "C1. Scenario: Probe has only one row (checks for sensor/system logs)\n";

        std::vector<std::pair<std::string, std::string>> files = {
            {"C1/input1.csv", "C1/probe1.csv"}, // 100x100
            {"C1/input2.csv", "C1/probe2.csv"}, //b320x320
            {"C1/input3.csv", "C1/probe3.csv"}, // 1000x1000
            {"C1/input4.csv", "C1/probe4.csv"} // 3200x3200
        };

        for(int i = 0; i < files.size(); i++) {
            auto [input, input_schema] = read_csv_optimized(files[i].first);
            auto [probe, probe_schema] = read_csv_optimized(files[i].second);

            std::cout << "Processing " << files[i].first << " and " << files[i].second << "\n\n";

            if (algo_ind == 1) {
                BinaryWindowFunctionModel model = create_test_model(1);
                BinaryWindowFunctionOperator op(model);

                auto [result, new_schema] = op.execute(input, probe, input_schema, probe_schema);
            } else if (algo_ind == 2) {
                BinaryWindowFunctionModel2 model = create_test_model2(1);
                BinaryWindowFunctionOperator2 op(model);

                auto [result, new_schema] = op.execute(input, probe, input_schema, probe_schema);
            }

            std::cout << "DONE WITH " << files[i].first << " and " << files[i].second << "\n";
        }
        std::cout << "---------------------------------------------------------------------------------\n";
    }

    void test_A1_bigger_ranges(int algo_ind) {
        std::cout << "---------------------------------------------------------------------------------\n";
        std::cout << "D1. Scenario: Testing for bigger ranges (same config as in A1))\n";

        std::vector<std::pair<std::string, std::string>> files = {
            {"D1/input1.csv", "D1/probe1.csv"}, // 10x1000, Window Size: 800
            {"D1/input2.csv", "D1/probe2.csv"}, // 10x10000, Window Size: 8000
            {"D1/input3.csv", "D1/probe3.csv"}, // 100x10000, Window Size: 8000
            {"D1/input4.csv", "D1/probe4.csv"} // 100x100000, Window Size: 80000
        };

        for(int i = 0; i < files.size(); i++) {
            auto [input, input_schema] = read_csv_optimized(files[i].first);
            auto [probe, probe_schema] = read_csv_optimized(files[i].second);

            std::cout << "Processing " << files[i].first << " and " << files[i].second << "\n\n";

            if (algo_ind == 1) {
                BinaryWindowFunctionModel model = create_test_model(1);
                BinaryWindowFunctionOperator op(model);

                auto [result, new_schema] = op.execute(input, probe, input_schema, probe_schema);
            } else if (algo_ind == 2) {
                BinaryWindowFunctionModel2 model = create_test_model2(1);
                BinaryWindowFunctionOperator2 op(model);

                auto [result, new_schema] = op.execute(input, probe, input_schema, probe_schema);
            }

            std::cout << "DONE WITH " << files[i].first << " and " << files[i].second << "\n";
        }
        std::cout << "---------------------------------------------------------------------------------\n";
    }

    void test_A1_very_small_ranges(int algo_ind) {
        std::cout << "---------------------------------------------------------------------------------\n";
        std::cout << "D2. Scenario: Testing for tiny ranges (allowing for duplicates in probe))\n";

        std::vector<std::pair<std::string, std::string>> files = {
            {"D2/input1.csv", "D2/probe1.csv"}, // 10x1000, Window Size: 1-3
            {"D2/input2.csv", "D2/probe2.csv"}, // 10x10000, Window Size: 10-12
            {"D2/input3.csv", "D2/probe3.csv"}, // 100x10000, Window Size: 10-12
            {"D2/input4.csv", "D2/probe4.csv"} // 100x100000, Window Size: 10-12
        };

        for(int i = 0; i < files.size(); i++) {
            auto [input, input_schema] = read_csv_optimized(files[i].first);
            auto [probe, probe_schema] = read_csv_optimized(files[i].second);

            std::cout << "Processing " << files[i].first << " and " << files[i].second << "\n\n";

            if (algo_ind == 1) {
                BinaryWindowFunctionModel model = create_test_model(1);
                BinaryWindowFunctionOperator op(model);

                auto [result, new_schema] = op.execute(input, probe, input_schema, probe_schema);
            } else if (algo_ind == 2) {
                BinaryWindowFunctionModel2 model = create_test_model2(1);
                BinaryWindowFunctionOperator2 op(model);

                auto [result, new_schema] = op.execute(input, probe, input_schema, probe_schema);
            }

            std::cout << "DONE WITH " << files[i].first << " and " << files[i].second << "\n";
        }
        std::cout << "---------------------------------------------------------------------------------\n";
    }

    void test_multiple_partitioning_columns(int algo_ind) {
        std::cout << "---------------------------------------------------------------------------------\n";
        std::cout << "E1. Scenario: A1 with multiple partitioning columns)\n";

        std::vector<std::pair<std::string, std::string>> files = {
            {"many_partitioning_cols/input1.csv", "many_partitioning_cols/probe1.csv"}, // 10x1000, Partitions: 1x10
            {"many_partitioning_cols/input2.csv", "many_partitioning_cols/probe2.csv"}, // 10x10000, Partitions: 1x10
            {"many_partitioning_cols/input3.csv", "many_partitioning_cols/probe3.csv"}, // 100x10000, Partitions: 10x10
            {"many_partitioning_cols/input4.csv", "many_partitioning_cols/probe4.csv"} // 100x100000, Partitions: 10x10
        };

        for(int i = 0; i < files.size(); i++) {
            auto [input, input_schema] = read_csv_optimized(files[i].first);
            auto [probe, probe_schema] = read_csv_optimized(files[i].second);

            std::cout << "Processing " << files[i].first << " and " << files[i].second << "\n\n";

            if (algo_ind == 1) {
                BinaryWindowFunctionModel model = create_test_model(2);
                BinaryWindowFunctionOperator op(model);

                auto [result, new_schema] = op.execute(input, probe, input_schema, probe_schema);
            } else if (algo_ind == 2) {
                BinaryWindowFunctionModel2 model = create_test_model2(2);
                BinaryWindowFunctionOperator2 op(model);

                auto [result, new_schema] = op.execute(input, probe, input_schema, probe_schema);
            }

            std::cout << "DONE WITH " << files[i].first << " and " << files[i].second << "\n";
        }
        std::cout << "---------------------------------------------------------------------------------\n";
    }


};

int main() {
    Benchmark benchmark;
    benchmark.run_benchmarks(1);
    // benchmark.run_benchmarks(2);
    return 0;
}