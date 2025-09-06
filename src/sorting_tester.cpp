#include <iostream>
#include <vector>
#include <algorithm> // Required for std::sort and std::generate
#include <chrono>    // Required for timing
#include <cstdlib>   // Required for std::rand

int compare_ints(const void* a, const void* b) {
    int arg1 = *static_cast<const int*>(a);
    int arg2 = *static_cast<const int*>(b);
    if (arg1 < arg2) return -1;
    if (arg1 > arg2) return 1;
    return 0;
}

// ------------------------------------------------------------------------------
// Sorting Algorithms
// ------------------------------------------------------------------------------

// ---------------------------    BASIC SORT    -------------------------------

/**
 * For 10 million records -> 600-700 ms
 * @param data
 */
void basic_sort(std::vector<int>& data) {
    std::sort(data.begin(), data.end());
};

// ---------------------------    QSORT    -------------------------------

/**
 * For 10 million records -> 800-900 ms
 * @param data
 */
void qsort(std::vector<int>& data) {
    std::qsort(data.data(), data.size(), sizeof(int), compare_ints);
};

// ---------------------------    STABLE SORT    -------------------------------

/**
 * For 10 million records -> ~ 500-600 ms
 * @param data
 */
void stable_sort(std::vector<int>& data) {
    std::stable_sort(data.begin(), data.end());
};

// ---------------------------    RADIX SORT    -------------------------------

// Helper function for Radix Sort
void counting_sort_by_digit(std::vector<int>& data, int exp) {
    std::vector<int> output(data.size());
    std::vector<int> count(10, 0); // Digits 0-9

    // Store count of occurrences in count[]
    for (int i : data) {
        int digit = (i / exp) % 10;
        count[digit]++;
    }

    // Change count[i] so that count[i] now contains the actual
    // position of this digit in output[]
    for (int i = 1; i < 10; i++) {
        count[i] += count[i - 1];
    }

    // Build the output array, processing the original array from the end to maintain stability
    for (int i = data.size() - 1; i >= 0; i--) {
        int digit = (data[i] / exp) % 10;
        output[count[digit] - 1] = data[i];
        count[digit]--;
    }

    // Copy the output array to data[], so that data[] now contains sorted numbers by current digit
    data = std::move(output);
}

/**
 * For 10 million records -> ~200-300 ms (Often significantly faster than std::sort for integers)
 * @param data
 */
void radix_sort(std::vector<int>& data) {
    if (data.empty()) return;

    // Find the maximum number to know the number of digits
    int max = *std::max_element(data.begin(), data.end());

    // Do counting sort for every digit. Note that instead of passing digit number,
    // we pass exp, which is 10^i where i is the current digit number.
    for (int exp = 1; max / exp > 0; exp *= 10) {
        counting_sort_by_digit(data, exp);
    }
}

// ---------------------------    COUNTING SORT    -------------------------------

/**
 * For 10 million records -> ~20-100 ms (Extremely fast if value range is small, e.g., 0-100)
 * WARNING: Will use massive memory and crash if max_val is large (e.g., RAND_MAX)!
 * @param data
 */
void counting_sort(std::vector<int>& data) {
    if (data.empty()) return;

    int min_val = *std::min_element(data.begin(), data.end());
    int max_val = *std::max_element(data.begin(), data.end());

    // Range of numbers
    int range = max_val - min_val + 1;

    // Check if the range is sane to prevent massive memory allocation
    // RAND_MAX is 32767 on some systems, which is a range of 65536 - manageable.
    // But if you have full 32-bit integers, the range is 4 billion - NOT manageable.
    if (range > 1000000) { // Arbitrary safety limit
        std::cerr << "Warning: Range too large for Counting Sort (" << range << "). Falling back to std::sort.\n";
        std::sort(data.begin(), data.end());
        return;
    }

    std::vector<int> count(range, 0);
    std::vector<int> output(data.size());

    // Store the count of each number, shifted by min_val
    for (int i : data) {
        count[i - min_val]++;
    }

    // Change count[i] so that it contains the actual position of this number in the output
    for (int i = 1; i < range; i++) {
        count[i] += count[i - 1];
    }

    // Build the output array (from the end to maintain stability)
    for (int i = data.size() - 1; i >= 0; i--) {
        int index = data[i] - min_val;
        output[count[index] - 1] = data[i];
        count[index]--;
    }

    // Copy the output back to the original array
    data = std::move(output);
}

int main() {
    // Create a vector with 10 million elements (1e7 is 10 million, 1e6 is 1 million) and fill it with random integers
    const int size = 1e7;
    std::vector<int> data(size);
    std::generate(data.begin(), data.end(), std::rand);

    // Divide every element by size to reduce the range of values for counting sort
    for (auto& num : data) {
        num = num % (size/10); // Reduce range to 0-999 for counting sort
    }

    // Time the sorting
    auto start = std::chrono::high_resolution_clock::now();

    // Sort the vector
    // basic_sort(data); // Basic Sort
    // qsort(data); // QuickSort
    // stable_sort(data); // QuickSort
    // radix_sort(data); // QuickSort
    counting_sort(data); // QuickSort

    // Stop the timer
    auto end = std::chrono::high_resolution_clock::now();

    // Calculate the duration in milliseconds
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    // Print the result
    std::cout << "Time taken to sort " << size << " integers: " << duration.count() << " milliseconds" << std::endl;

    return 0;
}