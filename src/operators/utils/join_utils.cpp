//
// Created by Vegim Shala on 10.7.25.
//

#include "operators/utils/join_utils.h"

#include <future>
#include <iostream>
#include <operators/utils/thread_pool.h>



void JoinUtils::build_index_from_vectors_segtree(
    const std::vector<uint32_t> &sorted_keys,
    const std::vector<uint32_t> &values
) {
    n = sorted_keys.size();
    keys = sorted_keys;

    segtree.resize(2 * n);

    // leaves
    for (size_t i = 0; i < n; ++i) {
        segtree[n + i] = static_cast<uint64_t>(values[i]);
    }

    // parents
    for (size_t i = n - 1; i > 0; --i) {
        segtree[i] = segtree[i << 1] + segtree[i << 1 | 1];
    }
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
    size_t end_block = (r - 1) / block_size;

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

