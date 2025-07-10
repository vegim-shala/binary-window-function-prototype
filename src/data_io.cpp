#include "data_io.h"
#include <filesystem>
#include <fstream>
#include <sstream>
#include <iostream>
#include <algorithm>

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

double extract_numeric(const ColumnValue& value) {
    if (holds_alternative<int>(value)) {
        return static_cast<double>(get<int>(value));
    } else if (holds_alternative<double>(value)) {
        return get<double>(value);
    } else {
        throw std::runtime_error("Expected numeric column, found string.");
    }
}

// ------------------------------ BINARY IMPLEMENTATION ------------------------------ //

// Binary write implementation
void write_binary(const std::string& filename, const Dataset& dataset, const FileSchema& schema) {
    fs::path filepath = get_data_path() / filename;
    std::ofstream file(filepath, std::ios::binary);

    // Write HEADER
    int32_t col_count = schema.columns.size(); // first the column count
    file.write(reinterpret_cast<const char*>(&col_count), sizeof(col_count));

    for (const auto& col : schema.columns) { //then we write the schema one by one to conclude the header
        // Write column name
        int32_t name_len = col.size();
        file.write(reinterpret_cast<const char*>(&name_len), sizeof(name_len));
        file.write(col.data(), name_len);

        // Write column type
        std::string type_str = schema.column_types.at(col);
        int32_t type_len = type_str.size();
        file.write(reinterpret_cast<const char*>(&type_len), sizeof(type_len));
        file.write(type_str.data(), type_len);
    }

    // Write data
    for (const auto& row : dataset) {
        for (const auto& col : schema.columns) {
            const auto& value = row.at(col);
            std::visit([&file](auto&& arg) {
                using T = std::decay_t<decltype(arg)>;
                if constexpr (std::is_same_v<T, int>) {
                    file.write(reinterpret_cast<const char*>(&arg), sizeof(arg));
                } else if constexpr (std::is_same_v<T, double>) {
                    file.write(reinterpret_cast<const char*>(&arg), sizeof(arg));
                } else if constexpr (std::is_same_v<T, std::string>) {
                    int32_t len = arg.size();
                    file.write(reinterpret_cast<const char*>(&len), sizeof(len));
                    file.write(arg.data(), len);
                }
            }, value);
        }
    }
}

void verify_binary_file(const std::string& filename) {
    fs::path filepath = get_data_path() / filename;
    std::ifstream file(filepath, std::ios::binary | std::ios::ate);
    if (!file) {
        std::cerr << "Cannot open file: " << filepath << "\n";
        return;
    }

    size_t file_size = file.tellg();
    file.seekg(0);

    std::cout << "Binary File Verification (" << filename << ")\n";
    std::cout << "File size: " << file_size << " bytes\n\n";

    try {
        // Read column count
        int32_t column_count;
        file.read(reinterpret_cast<char*>(&column_count), sizeof(column_count));
        std::cout << "Column count: " << column_count << "\n";

        // Read schema
        for (int i = 0; i < column_count; ++i) {
            int32_t name_len, type_len;
            file.read(reinterpret_cast<char*>(&name_len), sizeof(name_len));
            std::vector<char> name_buf(name_len);
            file.read(name_buf.data(), name_len);

            file.read(reinterpret_cast<char*>(&type_len), sizeof(type_len));
            std::vector<char> type_buf(type_len);
            file.read(type_buf.data(), type_len);

            std::cout << "Column " << i+1 << ": "
                      << std::string(name_buf.begin(), name_buf.end())
                      << " (" << std::string(type_buf.begin(), type_buf.end()) << ")\n";
        }

        // Estimate row count
        size_t header_size = file.tellg();
        if (header_size < file_size) {
            size_t remaining = file_size - header_size;
            size_t row_size = column_count * sizeof(int32_t); // Adjust if you have other types
            if (row_size > 0) {
                size_t estimated_rows = remaining / row_size;
                std::cout << "\nEstimated row count: " << estimated_rows << "\n";
            }
        }
    } catch (const std::exception& e) {
        std::cerr << "Verification error: " << e.what() << "\n";
    }
}

std::pair<Dataset, FileSchema> read_binary(const std::string& filename) {
    Dataset dataset;
    FileSchema schema;

    fs::path filepath = get_data_path() / filename;
    std::ifstream file(filepath, std::ios::binary); // binary part prevents text translation (problems with \n in Windows)
    if (!file) {
        throw std::runtime_error("Cannot open binary file: " + filename);
    }

    // Helper function to read strings with length prefix
    auto read_string = [&file]() {
        int32_t length; // reserve 4 bytes of memory
        file.read(reinterpret_cast<char*>(&length), sizeof(length)); // treat the memory address of length as a pointer to bytes. Reads exactly 4 bytes into length
        if (length <= 0 || length > 1000) { // Sanity check
            throw std::runtime_error("Invalid string length in binary file");
        }
        std::string str(length, '\0'); // create a string with length characters, and initialize them all to null
        file.read(&str[0], length);
        return str;
    };

    try {
        // Read column count
        int32_t column_count;
        file.read(reinterpret_cast<char*>(&column_count), sizeof(column_count)); // first 4 bytes are the column count

        // Read schema
        for (int i = 0; i < column_count; ++i) { // for every column

            // Read the Header
            std::string col_name = read_string(); // Next 4 bytes are col_name
            std::string col_type = read_string(); // Next 4 bytes are col_type

            schema.columns.push_back(col_name);
            schema.column_types[col_name] = col_type;
        }

        // Read data rows
        while (file.peek() != EOF) {
            DataRow row;
            for (const auto& col : schema.columns) { // for every column in schema
                const std::string& type = schema.column_types[col];

                if (type == "int32") {
                    int32_t val;
                    file.read(reinterpret_cast<char*>(&val), sizeof(val));
                    row[col] = val;
                }
                else if (type == "double") {
                    double val;
                    file.read(reinterpret_cast<char*>(&val), sizeof(val));
                    row[col] = val;
                }
                else if (type == "string") {
                    row[col] = read_string();
                }
                else {
                    throw std::runtime_error("Unknown column type: " + type);
                }
            }
            dataset.push_back(row);
        }
    } catch (const std::exception& e) {
        throw std::runtime_error("Error reading binary file: " + std::string(e.what()));
    }

    return {dataset, schema};
}


// ------------------------------ CSV IMPLEMENTATION ------------------------------ //
/** Use the first_line of a csv file to determine the schema of that CSV file
 * remove_if shifts all whitespace characters to the end of the string. erase then removes them completely
 *
 * @param first_line - the parsed first line of the CSV file
 * @return the detected schema
 */
FileSchema detect_schema(const std::string& first_line) {
    FileSchema schema;
    std::istringstream ss(first_line); // create an input string stream from the first_line
    std::string column;

    while (std::getline(ss, column, ',')) { // read from ss into column separating on each ","
        column.erase(remove_if(column.begin(), column.end(), isspace), column.end());
        schema.columns.push_back(column);
        schema.column_types[column] = "int32"; // simplification
    };

    return schema;
}


// very straightforward implementation.
ColumnValue parse_value(const std::string& value, const std::string& column_type) {
    try {
        if (column_type == "int32") {
            return std::stoi(value);
        }
        if (column_type == "double") {
            return std::stod(value);
        }
        return value;
    } catch (...) {
        return value;
    }
}


// CSV read implementation
std::pair<Dataset, FileSchema>  read_csv(const std::string& filename) {
    Dataset dataset; // result variable
    fs::path filepath = get_data_path() / filename; // construct filepath for reading
    std::ifstream file(filepath); // open the file for reading

    if (!file) {
        throw std::runtime_error("Cannot open CSV file");
    }

    std::string line;
    if (!std::getline(file, line)) {
        throw std::runtime_error("CSV file is empty");
    }

    FileSchema schema = detect_schema(line);

    while (std::getline(file, line)) {
        std::istringstream ss(line);
        DataRow row; // empty row to store values
        std::string value;
        size_t count = 0; // column counter
        while (std::getline(ss, value, ',')) { // for every column
            if (count >= schema.columns.size()) break; // if we are done with the columns, continue on the next line

            std::string column_name = schema.columns[count]; // get current column name from schema
            std::string column_type = "string"; // use the default type... TODO: Maybe change this to INT32
            if (schema.column_types.find(column_name) != schema.column_types.end()) {
                column_type = schema.column_types[column_name]; // if specified, get the schema type
            }

            row[column_name] = parse_value(value, column_type); // parse the value and store it in the DataRow
            count++;
        }

        dataset.push_back(row);
    }

    return {dataset, schema};
}

void write_csv(const std::string& filename, const Dataset& dataset) {
    if (dataset.empty()) {
        throw std::runtime_error("Empty dataset, nothing to write");
    }

    fs::path filepath = get_data_path() / filename;
    std::ofstream file(filepath); // opens file for writing

    if (!file.is_open()) {
        throw std::runtime_error("Failed to open file: " + filepath.string());
    }

    // Write header row (column names)
    const auto& first_row = dataset[0];
    bool first_column = true; // used to avoid trailing comma
    for (const auto& [col_name, _] : first_row) {
        if (!first_column) file << ","; // if it's not the first column, output a comma
        file << col_name; // output column name
        first_column = false;
    }
    file << "\n";

    // Write data rows
    for (const auto& row : dataset) {
        first_column = true;
        for (const auto& [col_name, value] : row) {
            if (!first_column) file << ",";

            // looks inside the variant, captures the output file stream by reference, and arg will match whatever type is in the variant
            std::visit([&file](auto&& arg) {
                using T = std::decay_t<decltype(arg)>; // gets the type of arg, and cleans it up, removing references, const etc. using T creates a type alias for the clean type
                if constexpr (std::is_same_v<T, std::string>) { // if the type is string
                    // Escape strings if needed (e.g., handle commas)
                    if (arg.find(',') != std::string::npos ||
                        arg.find('"') != std::string::npos ||
                        arg.find('\n') != std::string::npos) {
                        file << '"';
                        for (char c : arg) {
                            if (c == '"') file << '"'; // Double quotes
                            file << c;
                        }
                        file << '"';
                    } else {
                        file << arg;
                    }
                } else {
                    file << arg; // Numbers don't need escaping
                }
            }, value);

            first_column = false;
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
        for (const auto& col : schema.columns) {
            std::visit([&](auto&& arg) {
                using T = std::decay_t<decltype(arg)>;
                if constexpr (std::is_same_v<T, int>) {
                    std::cout << std::setw(15) << arg << " |";
                } else if constexpr (std::is_same_v<T, double>) {
                    std::cout << std::setw(15) << std::setprecision(6) << arg << " |";
                } else if constexpr (std::is_same_v<T, std::string>) {
                    std::string truncated = arg.substr(0, 12);
                    if (arg.length() > 12) truncated += "...";
                    std::cout << std::setw(15) << truncated << " |";
                }
            }, row.at(col));
        }
        std::cout << "\n";
    }

    if (data.size() > max_rows) {
        std::cout << "\n... showing " << max_rows << " of " << data.size() << " rows ...\n";
    }
}