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

using ColumnValue = int32_t;
using DataRow = std::vector<int32_t>;
using Dataset = std::vector<DataRow>;

struct FileSchema {
    std::vector<std::string> columns;
    std::map<std::string, std::string> column_types;
    std::unordered_map<std::string, size_t> column_index; // NEW

    void build_index() {
        column_index.clear();
        for (size_t i = 0; i < columns.size(); i++) {
            column_index[columns[i]] = i;
        }
    }

    size_t index_of(const std::string &col) const {
        auto it = column_index.find(col);
        if (it == column_index.end()) {
            throw std::runtime_error("Column not found: " + col);
        }
        return it->second;
    }

    void add_column(const std::string &col_name, const std::string &type) {
        columns.push_back(col_name);
        column_types[col_name] = type;
        column_index[col_name] = columns.size() - 1; // <-- index is new column position
    }
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
std::pair<Dataset, FileSchema> read_csv_optimized(const std::string& filename);
std::pair<Dataset, FileSchema> read_csv_fast(const std::string& filename);
std::pair<Dataset, FileSchema> read_csv_fast_parallel(const std::string& filename, int num_threads);
void write_csv(const std::string& filename, const Dataset& dataset, const FileSchema& schema);

