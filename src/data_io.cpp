#include "data_io.h"
#include <filesystem>
#include <fstream>
#include <sstream>
#include <iostream>
#include <algorithm>
#include <vector>
#include <string>
#include <charconv>
#include <cstdio>
#include <string>
#include <vector>
#include <stdexcept>
#include <utility>
#include <cctype>
#include <iostream>
#include <fstream>
#include <thread>
#include <future>
#include <vector>
#include <cstring>
#include <stdexcept>
#include <filesystem>
#include <mutex>

namespace fs = std::filesystem;

// ----------- FAST INTEGER PARSER (replaces std::from_chars) ----------
inline int32_t fast_atoi(const char* start, const char* end) {
    int32_t value = 0;
    bool neg = false;
    if (start < end && *start == '-') {
        neg = true;
        ++start;
    }
    while (start < end) {
        value = value * 10 + (*start - '0');
        ++start;
    }
    return neg ? -value : value;
}

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

// ----------- OPTIMIZED CSV READER ------------
std::pair<Dataset, FileSchema> read_csv_fast(const std::string& filename) {
    Dataset dataset;

    FILE* f = fopen((get_data_path() / filename).c_str(), "rb");
    if (!f) {
        throw std::runtime_error("Cannot open CSV file: " + filename);
    }

    constexpr size_t BUFFER_SIZE = 8 * 1024 * 1024; // 8 MB buffer
    std::vector<char> buffer(BUFFER_SIZE);

    // Read header line (first line)
    if (!fgets(buffer.data(), BUFFER_SIZE, f)) {
        fclose(f);
        throw std::runtime_error("CSV file is empty");
    }

    FileSchema schema = detect_schema(buffer.data());
    schema.build_index();
    size_t num_columns = schema.columns.size();

    dataset.reserve(10'000'000);

    std::vector<int32_t> row;
    row.reserve(num_columns);

    size_t line_count = 0;

    // Process line by line
    while (fgets(buffer.data(), BUFFER_SIZE, f)) {
        char* p = buffer.data();
        row.clear();

        for (size_t col = 0; col < num_columns; col++) {
            char* start = p;
            while (*p != ',' && *p != '\n' && *p != '\r' && *p != '\0') {
                ++p;
            }

            // Trim CR/LF before parsing
            char* end = p;
            while (end > start && (end[-1] == '\r' || end[-1] == '\n')) {
                --end;
            }

            row.push_back(fast_atoi(start, end));

            if (*p == ',') ++p; // skip comma
        }

        // Fill missing columns
        while (row.size() < num_columns) {
            row.push_back(0);
        }

        dataset.push_back(row);

        if (++line_count % 100000 == 0) {
            dataset.reserve(dataset.size() + 100000);
        }
    }

    fclose(f);
    return {dataset, schema};
}

constexpr size_t CHUNK_SIZE = 64 * 1024 * 1024; // 64 MB per read

struct ParsedChunk {
    Dataset data;
};



std::pair<Dataset, FileSchema> read_csv_fast_parallel(const std::string& filename, int num_threads) {
    Dataset dataset;

    FILE* f = fopen((get_data_path() / filename).c_str(), "rb");
    if (!f) {
        throw std::runtime_error("Cannot open CSV file: " + filename);
    }

    // --- Read header ---
    std::string header;
    {
        char buf[4096];
        if (!fgets(buf, sizeof(buf), f)) {
            fclose(f);
            throw std::runtime_error("CSV file is empty");
        }
        header = buf;
    }

    FileSchema schema = detect_schema(header.c_str());
    schema.build_index();
    size_t num_columns = schema.columns.size();

    // --- Thread pool style execution ---
    std::vector<std::future<ParsedChunk>> futures;

    std::vector<char> buffer(CHUNK_SIZE);

    size_t offset = ftell(f);

    while (true) {
        size_t bytes_read = fread(buffer.data(), 1, CHUNK_SIZE, f);
        if (bytes_read == 0) break;

        // ensure we end on a newline, otherwise backtrack
        size_t end = bytes_read;
        while (end > 0 && buffer[end - 1] != '\n') {
            --end;
        }
        fseek(f, static_cast<long>(end - bytes_read), SEEK_CUR);

        // Copy the chunk into a separate buffer to hand off to thread
        auto chunk_data = std::make_shared<std::vector<char>>(buffer.begin(), buffer.begin() + end);

        // Dispatch parse task
        futures.push_back(std::async(std::launch::async, [chunk_data, num_columns]() -> ParsedChunk {
            ParsedChunk result;
            result.data.reserve(1'000'000);

            char* data = chunk_data->data();
            char* line_start = data;
            std::vector<int32_t> row;
            row.reserve(num_columns);

            for (size_t i = 0; i < chunk_data->size(); i++) {
                if (data[i] == '\n' || data[i] == '\0') {
                    char* p = line_start;
                    row.clear();
                    for (size_t col = 0; col < num_columns; col++) {
                        char* start = p;
                        while (*p != ',' && *p != '\n' && *p != '\r' && *p != '\0') ++p;
                        char* end = p;
                        while (end > start && (end[-1] == '\r' || end[-1] == '\n')) --end;
                        row.push_back(fast_atoi(start, end));
                        if (*p == ',') ++p;
                    }
                    while (row.size() < num_columns) row.push_back(0);
                    result.data.push_back(row);
                    line_start = data + i + 1;
                }
            }
            return result;
        }));
    }

    fclose(f);

    // Merge results
    for (auto& fut : futures) {
        ParsedChunk chunk = fut.get();
        dataset.insert(dataset.end(),
                       std::make_move_iterator(chunk.data.begin()),
                       std::make_move_iterator(chunk.data.end()));
    }

    return {dataset, schema};
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