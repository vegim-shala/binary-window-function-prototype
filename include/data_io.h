//
// Created by Vegim Shala on 6.7.25.
//


#pragma once

#include <vector>
#include <string>
#include <stdexcept>
#include <map>
#include <variant>
#include <filesystem>

using ColumnValue = std::variant<int, double, std::string>;
using DataRow = std::map<std::string, ColumnValue>;
using Dataset = std::vector<DataRow>;

struct FileSchema {
    std::vector<std::string> columns;
    std::map<std::string, std::string> column_types; // "int", "double", "string"  -- ALTHOUGH WE CAN ASSUME INT32 for everyone
};

// struct Row {
//     int id;
//     double value;
// };

void print_dataset(const Dataset& data, const FileSchema& schema, size_t max_rows = 10);
double extract_numeric(const ColumnValue& value);

// Binary IO functions
void verify_binary_file(const std::string& filename);
void write_binary(const std::string& filename, const Dataset& dataset, const FileSchema& schema);
std::pair<Dataset, FileSchema> read_binary(const std::string& filename);

// CSV functions
std::pair<Dataset, FileSchema>  read_csv(const std::string& filename);
void write_csv(const std::string& filename, const Dataset& dataset);

