#include "data_io.h"
#include <filesystem>
#include <fstream>
#include <sstream>
#include <iostream>
#include <algorithm>
#include <vector>
#include <string>
#include <charconv>

namespace fs = std::filesystem;

// Helper function to get data directory path
fs::path get_data_path() {
    // First try path relative to executable (for development)
    fs::path exe_path = fs::current_path();
    fs::path data_path = exe_path / "data";

    // If not found, try one level up (common in build systems)
    if (!fs::exists(data_path)) {
        data_path = exe_path.parent_path() / "data";
    }

    if (!fs::exists(data_path)) {
        throw std::runtime_error("Cannot find data directory");
    }

    return data_path;
}

// double extract_numeric(const ColumnValue& value) {
//     if (holds_alternative<int>(value)) {
//         return static_cast<double>(get<int>(value));
//     } else if (holds_alternative<double>(value)) {
//         return get<double>(value);
//     } else {
//         throw std::runtime_error("Expected numeric column, found string.");
//     }
// }

// ------------------------------ BINARY IMPLEMENTATION ------------------------------ //

// Binary write implementation
// void write_binary(const std::string& filename, const Dataset& dataset, const FileSchema& schema) {
//     fs::path filepath = get_data_path() / filename;
//     std::ofstream file(filepath, std::ios::binary);
//
//     // Write HEADER
//     int32_t col_count = schema.columns.size(); // first the column count
//     file.write(reinterpret_cast<const char*>(&col_count), sizeof(col_count));
//
//     for (const auto& col : schema.columns) { //then we write the schema one by one to conclude the header
//         // Write column name
//         int32_t name_len = col.size();
//         file.write(reinterpret_cast<const char*>(&name_len), sizeof(name_len));
//         file.write(col.data(), name_len);
//
//         // Write column type
//         std::string type_str = schema.column_types.at(col);
//         int32_t type_len = type_str.size();
//         file.write(reinterpret_cast<const char*>(&type_len), sizeof(type_len));
//         file.write(type_str.data(), type_len);
//     }
//
//     // Write data
//     for (const auto& row : dataset) {
//         for (size_t i = 0; i < row.size(); ++i) {
//             const auto& value = row[i];
//             std::visit([&file](auto&& arg) {
//                 using T = std::decay_t<decltype(arg)>;
//                 if constexpr (std::is_same_v<T, int>) {
//                     file.write(reinterpret_cast<const char*>(&arg), sizeof(arg));
//                 } else if constexpr (std::is_same_v<T, double>) {
//                     file.write(reinterpret_cast<const char*>(&arg), sizeof(arg));
//                 } else if constexpr (std::is_same_v<T, std::string>) {
//                     int32_t len = arg.size();
//                     file.write(reinterpret_cast<const char*>(&len), sizeof(len));
//                     file.write(arg.data(), len);
//                 }
//             }, value);
//         }
//     }
// }
//
// void verify_binary_file(const std::string& filename) {
//     fs::path filepath = get_data_path() / filename;
//     std::ifstream file(filepath, std::ios::binary | std::ios::ate);
//     if (!file) {
//         std::cerr << "Cannot open file: " << filepath << "\n";
//         return;
//     }
//
//     size_t file_size = file.tellg();
//     file.seekg(0);
//
//     std::cout << "Binary File Verification (" << filename << ")\n";
//     std::cout << "File size: " << file_size << " bytes\n\n";
//
//     try {
//         // Read column count
//         int32_t column_count;
//         file.read(reinterpret_cast<char*>(&column_count), sizeof(column_count));
//         std::cout << "Column count: " << column_count << "\n";
//
//         // Read schema
//         for (int i = 0; i < column_count; ++i) {
//             int32_t name_len, type_len;
//             file.read(reinterpret_cast<char*>(&name_len), sizeof(name_len));
//             std::vector<char> name_buf(name_len);
//             file.read(name_buf.data(), name_len);
//
//             file.read(reinterpret_cast<char*>(&type_len), sizeof(type_len));
//             std::vector<char> type_buf(type_len);
//             file.read(type_buf.data(), type_len);
//
//             std::cout << "Column " << i+1 << ": "
//                       << std::string(name_buf.begin(), name_buf.end())
//                       << " (" << std::string(type_buf.begin(), type_buf.end()) << ")\n";
//         }
//
//         // Estimate row count
//         size_t header_size = file.tellg();
//         if (header_size < file_size) {
//             size_t remaining = file_size - header_size;
//             size_t row_size = column_count * sizeof(int32_t); // Adjust if you have other types
//             if (row_size > 0) {
//                 size_t estimated_rows = remaining / row_size;
//                 std::cout << "\nEstimated row count: " << estimated_rows << "\n";
//             }
//         }
//     } catch (const std::exception& e) {
//         std::cerr << "Verification error: " << e.what() << "\n";
//     }
// }

// std::pair<Dataset, FileSchema> read_binary(const std::string& filename) {
//     Dataset dataset;
//     FileSchema schema;
//
//     fs::path filepath = get_data_path() / filename;
//     std::ifstream file(filepath, std::ios::binary); // binary part prevents text translation (problems with \n in Windows)
//     if (!file) {
//         throw std::runtime_error("Cannot open binary file: " + filename);
//     }
//
//     // Helper function to read strings with length prefix
//     auto read_string = [&file]() {
//         int32_t length; // reserve 4 bytes of memory
//         file.read(reinterpret_cast<char*>(&length), sizeof(length)); // treat the memory address of length as a pointer to bytes. Reads exactly 4 bytes into length
//         if (length <= 0 || length > 1000) { // Sanity check
//             throw std::runtime_error("Invalid string length in binary file");
//         }
//         std::string str(length, '\0'); // create a string with length characters, and initialize them all to null
//         file.read(&str[0], length);
//         return str;
//     };
//
//     try {
//         // Read column count
//         int32_t column_count;
//         file.read(reinterpret_cast<char*>(&column_count), sizeof(column_count)); // first 4 bytes are the column count
//
//         // Read schema
//         for (int i = 0; i < column_count; ++i) { // for every column
//
//             // Read the Header
//             std::string col_name = read_string(); // Next 4 bytes are col_name
//             std::string col_type = read_string(); // Next 4 bytes are col_type
//
//             schema.columns.push_back(col_name);
//             schema.column_types[col_name] = col_type;
//         }
//
//         // Read data rows
//         while (file.peek() != EOF) {
//             DataRow row;
//             for (const auto& col : schema.columns) { // for every column in schema
//                 const std::string& type = schema.column_types[col];
//
//                 if (type == "int32") {
//                     int32_t val;
//                     file.read(reinterpret_cast<char*>(&val), sizeof(val));
//                     row.push_back(val);
//                 }
//                 else if (type == "double") {
//                     double val;
//                     file.read(reinterpret_cast<char*>(&val), sizeof(val));
//                     row.push_back(val);
//                 }
//                 else if (type == "string") {
//                     row.push_back(read_string());
//                 }
//                 else {
//                     throw std::runtime_error("Unknown column type: " + type);
//                 }
//             }
//             dataset.push_back(row);
//         }
//     } catch (const std::exception& e) {
//         throw std::runtime_error("Error reading binary file: " + std::string(e.what()));
//     }
//
//     return {dataset, schema};
// }


// ------------------------------ CSV IMPLEMENTATION ------------------------------ //
/** Use the first_line of a csv file to determine the schema of that CSV file
 * remove_if shifts all whitespace characters to the end of the string. erase then removes them completely
 *
 * @param first_line - the parsed first line of the CSV file
 * @return the detected schema
 */
FileSchema detect_schema(const std::string& header_line) {
    FileSchema schema;
    std::istringstream ss(header_line);
    std::string column;

    while (std::getline(ss, column, ',')) {
        column.erase(remove_if(column.begin(), column.end(), isspace), column.end());
        schema.columns.push_back(column);
        schema.column_types[column] = "int32"; // Always int32
    }

    return schema;
}


// very straightforward implementation.
// ColumnValue parse_value(const std::string& value, const std::string& column_type) {
//     try {
//         if (column_type == "int32") {
//             return std::stoi(value);
//         }
//         if (column_type == "double") {
//             return std::stod(value);
//         }
//         return value;
//     } catch (...) {
//         return value;
//     }
// }


// CSV read implementation
// std::pair<Dataset, FileSchema>  read_csv(const std::string& filename) {
//     Dataset dataset; // result variable
//     fs::path filepath = get_data_path() / filename; // construct filepath for reading
//     std::ifstream file(filepath); // open the file for reading
//
//     if (!file) {
//         throw std::runtime_error("Cannot open CSV file");
//     }
//
//     std::string line;
//     if (!std::getline(file, line)) {
//         throw std::runtime_error("CSV file is empty");
//     }
//
//     FileSchema schema = detect_schema(line);
//     schema.build_index();
//
//     while (std::getline(file, line)) {
//         std::istringstream ss(line);
//         DataRow row; // empty row to store values
//         std::string value;
//         size_t count = 0; // column counter
//         while (std::getline(ss, value, ',')) { // for every column
//             if (count >= schema.columns.size()) break; // if we are done with the columns, continue on the next line
//
//             std::string column_name = schema.columns[count]; // get current column name from schema
//             std::string column_type = "string"; // use the default type... TODO: Maybe change this to INT32
//             if (schema.column_types.find(column_name) != schema.column_types.end()) {
//                 column_type = schema.column_types[column_name]; // if specified, get the schema type
//             }
//
//             row.push_back(parse_value(value, column_type)); // parse the value and store it in the DataRow
//             count++;
//         }
//
//         dataset.push_back(row);
//     }
//
//     return {dataset, schema};
// }

int32_t parse_value_optimized(const std::string& value) {
    // Fast integer parsing - no type checking needed
    int32_t result = 0;
    auto [ptr, ec] = std::from_chars(value.data(), value.data() + value.size(), result);
    if (ec == std::errc()) {
        return result;
    }
    return 0; // Fallback for parsing errors
}

std::pair<Dataset, FileSchema> read_csv_optimized(const std::string& filename) {
    Dataset dataset;
    fs::path filepath = get_data_path() / filename;

    std::ifstream file(filepath);
    if (!file) {
        throw std::runtime_error("Cannot open CSV file: " + filepath.string());
    }

    // Set larger buffer size
    constexpr size_t BUFFER_SIZE = 64 * 1024;
    char buffer[BUFFER_SIZE];
    file.rdbuf()->pubsetbuf(buffer, BUFFER_SIZE);

    std::string line;
    if (!std::getline(file, line)) {
        throw std::runtime_error("CSV file is empty");
    }

    FileSchema schema = detect_schema(line);
    schema.build_index();

    dataset.reserve(10000000);

    // Reuse variables
    std::string value;
    value.reserve(256);

    size_t line_count = 0;
    const size_t num_columns = schema.columns.size();

    while (std::getline(file, line)) {
        DataRow row;
        row.reserve(num_columns);

        std::istringstream ss(std::move(line));
        size_t count = 0;

        while (std::getline(ss, value, ',')) {
            if (count >= num_columns) break;
            row.push_back(parse_value_optimized(value));
            count++;
        }

        // Fill missing columns with 0
        while (row.size() < num_columns) {
            row.push_back(0);
        }

        dataset.push_back(std::move(row));

        if (++line_count % 100000 == 0) {
            dataset.reserve(dataset.size() + 100000);
        }
    }

    return {dataset, schema};
}

void write_csv(const std::string& filename, const Dataset& dataset, const FileSchema& schema) {
    if (dataset.empty()) {
        throw std::runtime_error("Empty dataset, nothing to write");
    }

    fs::path filepath = get_data_path() / filename;
    std::ofstream file(filepath);

    if (!file.is_open()) {
        throw std::runtime_error("Failed to open file: " + filepath.string());
    }

    // Write header row (column names)
    bool first_column = true;
    for (const auto& col_name : schema.columns) {
        if (!first_column) file << ",";
        file << col_name;
        first_column = false;
    }
    file << "\n";

    // Write data rows - MUCH SIMPLER!
    for (const auto& row : dataset) {
        bool first_in_row = true;
        for (const auto& value : row) {
            if (!first_in_row) file << ",";
            file << value;  // Direct int32_t output - no variant overhead!
            first_in_row = false;
        }
        file << "\n";
    }
}

void print_dataset(const Dataset& data, const FileSchema& schema, size_t max_rows) {
    if (data.empty()) {
        std::cout << "Dataset is empty\n";
        return;
    }

    // Print schema information
    std::cout << "Data Schema (" << data.size() << " rows):\n";
    for (const auto& col : schema.columns) {
        std::cout << "- " << col << " (" << schema.column_types.at(col) << ")\n";
    }
    std::cout << "\n";

    // Print header
    for (const auto& col : schema.columns) {
        std::cout << std::setw(15) << col << " |";
    }
    std::cout << "\n";

    // Print separator
    for (size_t i = 0; i < schema.columns.size(); ++i) {
        std::cout << "-----------------+";
    }
    std::cout << "\n";

    // Print data rows
    size_t rows_to_print = std::min(data.size(), max_rows);
    for (size_t i = 0; i < rows_to_print; ++i) {
        const auto& row = data[i];
        for (size_t j = 0; j < row.size(); ++j) {
            // Since we're using int32_t directly, no need for std::visit
            std::cout << std::setw(15) << row[j] << " |";
        }
        std::cout << "\n";
    }

    if (data.size() > max_rows) {
        std::cout << "\n... showing " << max_rows << " of " << data.size() << " rows ...\n";
    }
}