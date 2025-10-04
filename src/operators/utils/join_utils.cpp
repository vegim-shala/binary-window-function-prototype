//
// Created by Vegim Shala on 10.7.25.
//

#include "operators/utils/join_utils.h"

#include <future>
#include <iostream>
#include <operators/utils/thread_pool.h>



void JoinUtils::build_index_from_vectors_segtree(
    const std::vector<int32_t> &sorted_keys,
    const std::vector<int32_t> &values
) {
    n = sorted_keys.size();
    keys = sorted_keys;

    segtree.resize(2 * n);

    // leaves
    for (size_t i = 0; i < n; ++i) {
        segtree[n + i] = static_cast<int64_t>(values[i]);
    }

    // parents
    for (size_t i = n - 1; i > 0; --i) {
        segtree[i] = segtree[i << 1] + segtree[i << 1 | 1];
    }
}

void JoinUtils::build_index_from_vectors_prefix_sums(
    const std::vector<int32_t> &sorted_keys,
    const std::vector<int32_t> &values
) {
    n = sorted_keys.size();
    keys = sorted_keys;

    prefix.resize(n + 1);
    prefix[0] = 0;
    for (size_t i = 0; i < n; ++i) {
        prefix[i + 1] = prefix[i] + values[i];
    }
}

void JoinUtils::build_prefix_sums_from_sorted_indices(const Dataset& input,
                                      const FileSchema& schema,
                                      const std::vector<size_t>& sorted_indices,
                                      size_t order_idx,
                                      size_t value_idx) {
    const size_t n_local = sorted_indices.size();
    n = n_local;

    keys.resize(n_local);
    prefix.resize(n_local + 1);
    prefix[0] = 0;

    for (size_t i = 0; i < n_local; ++i) {
        size_t row_id = sorted_indices[i];

        // Read once per row (SoA later would be even better)
        const DataRow& row = input[row_id];

        keys[i]     = row[order_idx];
        prefix[i+1] = prefix[i] + row[value_idx];
    }
}


void JoinUtils::build_eytzinger() {
    size_t n = keys.size();
    eyt.resize(n + 1);
    pos_to_orig.resize(n + 1);

    std::function<void(size_t, size_t, size_t)> build = [&](size_t l, size_t r, size_t k) {
        if (l > r || k > n) return;
        size_t m = (l + r) / 2;
        eyt[k] = keys[m];
        pos_to_orig[k] = m;
        build(l, m - 1, 2 * k);
        build(m + 1, r, 2 * k + 1);
    };
    build(0, n - 1, 1);
}

void JoinUtils::build_buckets() {
    if (keys.empty()) return;

    size_t nblocks = (keys.size() + bucket_size - 1) / bucket_size;
    block_mins.resize(nblocks);

    for (size_t b = 0; b < nblocks; ++b) {
        block_mins[b] = keys[b * bucket_size];
    }
}

void JoinUtils::build_index_from_vectors_sqrttree(
    const std::vector<int32_t> &sorted_keys,
    const std::vector<int32_t> &vals
) {
    keys = sorted_keys;
    values.assign(vals.begin(), vals.end());
    size_t n = values.size();

    block_size = static_cast<size_t>(std::sqrt(n)) + 1;
    num_blocks = (n + block_size - 1) / block_size;

    block_sum.assign(num_blocks, 0);
    sqrt_prefix.resize(n);
    sqrt_suffix.resize(n);

    for (size_t b = 0; b < num_blocks; ++b) {
        size_t start = b * block_size;
        size_t end = std::min(start + block_size, n);

        int64_t s = 0;
        for (size_t i = start; i < end; i++) {
            s += values[i];
            sqrt_prefix[i] = s;
        }
        block_sum[b] = s;

        int64_t suf = 0;
        for (size_t i = end; i-- > start;) {
            suf += values[i];
            sqrt_suffix[i] = suf;
        }
    }
}

void JoinUtils::build_index_from_vectors_sqrt_tree(const std::vector<int32_t> &sorted_keys,
                                                   const std::vector<int32_t> &vals) {
    n = sorted_keys.size();
    keys = sorted_keys;
    values = vals;

    if (n == 0) return;

    block_size = static_cast<size_t>(std::ceil(std::sqrt(n)));
    size_t num_blocks = (n + block_size - 1) / block_size;

    block_sum.assign(num_blocks, 0.0);

    for (size_t i = 0; i < n; i++) {
        block_sum[i / block_size] += values[i];
    }
}