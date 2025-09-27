//
// Created by Vegim Shala on 10.7.25.
//

#include "operators/utils/join_utils.h"

#include <iostream>



void JoinUtils::pretty_print_segment_tree() const {
    if (segtree.empty()) {
        std::cout << "Segment tree is empty!" << std::endl;
        return;
    }

    // Calculate tree height
    size_t height = 0;
    size_t nodes = segtree.size() - 1; // Exclude index 0 if using 1-based
    while (nodes > 0) {
        height++;
        nodes /= 2;
    }

    std::cout << "Segment Tree Structure:" << std::endl;

    size_t level_start = 1;
    size_t level_size = 1;

    for (size_t level = 0; level < height; level++) {
        std::cout << "Level " << level << ": ";

        for (size_t i = 0; i < level_size && level_start + i < segtree.size(); i++) {
            std::cout << segtree[level_start + i] << " ";
        }
        std::cout << std::endl;

        level_start += level_size;
        level_size *= 2;
    }
}

void JoinUtils::build_index(const Dataset& input, const FileSchema &schema, std::string& value_column) {
    // auto start = std::chrono::high_resolution_clock::now();

    n = input.size();
    keys.resize(n);
    std::vector<double> values(n);

    size_t order_idx = schema.index_of(order_column);
    size_t value_idx = schema.index_of(value_column);

    for (size_t i = 0; i < n; i++) {
        keys[i] = input[i][order_idx];
        values[i] = input[i][value_idx];
        // default: sum over value_column, adjust if needed
    }

    // Build segment tree for SUM
    segtree.assign(2 * n, 0.0);
    for (size_t i = 0; i < n; i++) {
        segtree[n + i] = values[i];
    }
    for (size_t i = n - 1; i > 0; --i) {
        segtree[i] = segtree[i << 1] + segtree[i << 1 | 1];
    }

    // auto end = std::chrono::high_resolution_clock::now();
    // auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    // std::cout << "Time taken for BUILD_INDEX: " << duration.count() << " ms" << std::endl;

    // pretty_print_segment_tree();

}

void JoinUtils::build_index_from_vectors_segtree(const std::vector<uint32_t> &sorted_keys, const std::vector<uint32_t> &values) {
    n = sorted_keys.size();
    keys = sorted_keys; // copy
    // values must be same size
    segtree.assign(2 * n, 0.0);
    for (size_t i = 0; i < n; ++i) segtree[n + i] = values[i];
    for (size_t i = n - 1; i > 0; --i) segtree[i] = segtree[i << 1] + segtree[i << 1 | 1];
}

double JoinUtils::seg_query(size_t l, size_t r) const {
    double res = 0.0;
    for (l += n, r += n; l < r; l >>= 1, r >>= 1) {
        if (l & 1) res += segtree[l++];
        if (r & 1) res += segtree[--r];
    }
    return res;
}

void JoinUtils::build_index_from_vectors_segtree_top_down(const std::vector<uint32_t> &sorted_keys, const std::vector<uint32_t> &values) {
    n = sorted_keys.size();
    keys = sorted_keys; // copy
    segtree.assign(4 * n, 0.0); // allocate enough space for recursion

    std::function<void(size_t, size_t, size_t)> build = [&](size_t node, size_t l, size_t r) {
        if (l == r) {
            segtree[node] = values[l];
        } else {
            size_t mid = (l + r) / 2;
            build(2 * node, l, mid);
            build(2 * node + 1, mid + 1, r);
            segtree[node] = segtree[2 * node] + segtree[2 * node + 1];
        }
    };

    if (n > 0) {
        build(1, 0, n - 1);
    }
}

double JoinUtils::seg_query_top_down(size_t ql, size_t qr) const {
    std::function<uint32_t(size_t, size_t, size_t)> query = [&](size_t node, size_t l, size_t r) -> uint32_t {
        if (qr < l || ql > r) {
            return 0.0; // disjoint
        }
        if (ql <= l && r <= qr) {
            return segtree[node]; // fully covered
        }
        size_t mid = (l + r) / 2;
        return query(2 * node, l, mid) + query(2 * node + 1, mid + 1, r);
    };

    if (n == 0 || ql > qr) return 0.0;
    return query(1, 0, n - 1);
}


void JoinUtils::build_index_from_vectors_prefix_sums(const std::vector<uint32_t> &sorted_keys,
                                         const std::vector<uint32_t> &values) {
    n = sorted_keys.size();
    keys = sorted_keys; // copy

    prefix_sums.assign(n + 1, 0.0);
    for (size_t i = 0; i < n; ++i) {
        prefix_sums[i + 1] = prefix_sums[i] + values[i];
    }
}

double JoinUtils::prefix_sums_query(size_t l, size_t r) const {
    // r is exclusive
    return prefix_sums[r] - prefix_sums[l];
}

void JoinUtils::build_index_from_vectors_sqrt_tree(const std::vector<uint32_t> &sorted_keys,
                                         const std::vector<uint32_t> &vals) {
    n = sorted_keys.size();
    keys = sorted_keys;
    values = vals;

    if (n == 0) return;

    block_size = static_cast<size_t>(std::ceil(std::sqrt(n)));
    size_t num_blocks = (n + block_size - 1) / block_size;

    block_sums.assign(num_blocks, 0.0);

    for (size_t i = 0; i < n; i++) {
        block_sums[i / block_size] += values[i];
    }
}

double JoinUtils::sqrt_query(size_t l, size_t r) const {
    if (l >= r || n == 0) return 0.0;

    double res = 0.0;

    size_t start_block = l / block_size;
    size_t end_block   = (r - 1) / block_size;

    if (start_block == end_block) {
        // all inside one block → sum directly
        for (size_t i = l; i < r; i++) res += values[i];
        return res;
    }

    // left partial block
    size_t left_end = (start_block + 1) * block_size;
    for (size_t i = l; i < left_end; i++) res += values[i];

    // full blocks
    for (size_t b = start_block + 1; b < end_block; b++) {
        res += block_sums[b];
    }

    // right partial block
    size_t right_start = end_block * block_size;
    for (size_t i = right_start; i < r; i++) res += values[i];

    return res;
}


void JoinUtils::build_index_from_vectors_two_pointer_sweep(const std::vector<uint32_t> &sorted_keys,
                                         const std::vector<uint32_t> &vals) {
    n = sorted_keys.size();
    keys = sorted_keys;
    values = vals;
}

// std::vector<uint32_t> JoinUtils::sweep_query(
//     const std::vector<std::pair<uint32_t,uint32_t>> &probe_ranges  // [(start, end)]
// ) const {
//     std::vector<uint32_t> results(probe_ranges.size());
//
//     // Indices into input
//     size_t lo_ptr = 0;
//     size_t hi_ptr = 0;
//     uint32_t running_sum = 0;
//
//     // Prepare: sort probes by start (but keep original index to restore order)
//     struct ProbeInfo {
//         double start, end;
//         size_t idx;
//     };
//     std::vector<ProbeInfo> sorted_probes;
//     sorted_probes.reserve(probe_ranges.size());
//     for (size_t i = 0; i < probe_ranges.size(); i++) {
//         sorted_probes.push_back({probe_ranges[i].first, probe_ranges[i].second, i});
//     }
//     std::sort(sorted_probes.begin(), sorted_probes.end(),
//               [](auto &a, auto &b){ return a.start < b.start; });
//
//     // Sweep input alongside probes
//     for (auto &pr : sorted_probes) {
//         double start = pr.start;
//         double end   = pr.end;
//
//         // advance lo_ptr to exclude old rows
//         while (lo_ptr < n && keys[lo_ptr] < start) {
//             running_sum -= values[lo_ptr];
//             lo_ptr++;
//         }
//
//         // advance hi_ptr to include new rows
//         while (hi_ptr < n && keys[hi_ptr] <= end) {
//             running_sum += values[hi_ptr];
//             hi_ptr++;
//         }
//
//         results[pr.idx] = running_sum;
//     }
//
//     return results;
// }






double JoinUtils::compute_sum_range(const Dataset& input, const FileSchema &schema, const DataRow& probe_row,
                                    const std::string& start_col, const std::string& end_col) const {
    if (n == 0) return 0.0;

    size_t start_idx = start_col.empty() ? SIZE_MAX : schema.index_of(start_col);
    size_t end_idx   = end_col.empty()   ? SIZE_MAX : schema.index_of(end_col);

    double start = start_col.empty() ? -std::numeric_limits<double>::infinity()
                                     : probe_row[start_idx];
    double end   = end_col.empty()   ? std::numeric_limits<double>::infinity()
                                     : probe_row[end_idx];

    auto lo_it = std::lower_bound(keys.begin(), keys.end(), start);
    auto hi_it = std::upper_bound(keys.begin(), keys.end(), end);

    size_t lo = lo_it - keys.begin();
    size_t hi = hi_it - keys.begin();

    if (lo >= hi) return 0.0;
    return seg_query(lo, hi); // fast SUM
}

std::vector<size_t> JoinUtils::compute_range_join(
    const Dataset& input, const FileSchema &schema, const DataRow& probe_row,
    const std::string& start_col, const std::string& end_col) const
{
     std::vector<size_t> indices;

     size_t start_idx = start_col.empty() ? SIZE_MAX : schema.index_of(start_col);
     size_t end_idx   = end_col.empty()   ? SIZE_MAX : schema.index_of(end_col);


     double start = start_col.empty() ? std::numeric_limits<double>::min() : probe_row[start_idx];
     double end = end_col.empty() ? std::numeric_limits<double>::max() : probe_row[end_idx];

     size_t order_idx  = schema.index_of(order_column);

     for (size_t j = 0; j < input.size(); ++j) {
         double value = input[j][order_idx];
         if (value >= start && value <= end) {
             indices.push_back(j);
         }
     }

     return indices;
}

void JoinUtils::validate() const {
    // if (join_spec.begin_column.empty() || join_spec.end_column.empty()) {
    //     throw std::runtime_error("Invalid JoinSpec: Please specify both begin_column and end_column.");
    // }
    // TODO: Add further validations here...
}

std::vector<size_t> JoinUtils::compute_join(const Dataset& input, const FileSchema &schema, const DataRow& probe_row) const {
    validate();
    if (join_spec.type == JoinType::RANGE) {
        return compute_range_join(input, schema, probe_row, join_spec.begin_column, join_spec.end_column);
    }
    // else if (join_spec.type == JoinType::RANGE) {
    //     return compute_rows_join(input, probe_row, join_spec.begin_column, join_spec.end_column);
    // }

    throw std::runtime_error("Unsupported or incomplete FrameSpec configuration for binary window execution.");
}


