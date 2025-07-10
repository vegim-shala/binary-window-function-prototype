#include "data_io.h"
#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>

// void create_sample_binary(const std::string& bin_filename) {
//     std::vector<Row> sample_data = {
//         {1, 10},
//         {2, 20},
//         {3, 30},
//         {4, 40},
//         {5, 50}
//     };
//     write_binary(bin_filename, sample_data);
//     std::cout << "Created binary file " << bin_filename << " from hardcoded data\n";
// }

bool convert_csv_to_binary(const std::string &csv_filename, const std::string& bin_filename) {
    bool success = false;
    try {
        // Read CSV data
        auto [csv_data, schema2] = read_csv(csv_filename);

        // print_dataset(csv_data, schema2, 100);

        if (csv_data.empty()) {
            std::cerr << "Warning: Empty CSV file - no data to convert" << std::endl;
            return false;
        }

        // Create schema from the first row
        FileSchema schema;
        for (const auto& [col_name, value] : csv_data[0]) {
            schema.columns.push_back(col_name);

            // Determine column type
            std::visit([&schema, &col_name](auto&& arg) {
                using T = std::decay_t<decltype(arg)>;
                if constexpr (std::is_same_v<T, int>) {
                    schema.column_types[col_name] = "int32";
                } else if constexpr (std::is_same_v<T, double>) {
                    schema.column_types[col_name] = "double";
                } else if constexpr (std::is_same_v<T, std::string>) {
                    schema.column_types[col_name] = "string";
                }
            }, value);
        }

        // Write binary file with schema
        write_binary(bin_filename, csv_data, schema);
        print_dataset(read_binary(bin_filename).first, schema, 100);
        success = true;
        std::cout << "Successfully converted " << csv_filename
                  << " to binary format " << bin_filename << std::endl;

    } catch (const std::exception& e) {
        std::cerr << "CSV to Binary Conversion Failed: " << e.what() << std::endl;
    }
    return success;
}


int main() {
    convert_csv_to_binary("sensor.csv", "sensor.bin");
    return 0;
}