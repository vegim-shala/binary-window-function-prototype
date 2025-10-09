//
// Created by Vegim Shala on 10.7.25.
//
#include "aggregators/sum_aggregator.h"

void SumAggregator::build_from_values(const std::vector<int32_t> &values) {
    prefix.resize(values.size() + 1);
    prefix[0] = 0;
    for (size_t i = 0; i < values.size(); ++i)
        prefix[i + 1] = prefix[i] + values[i];
}

void SumAggregator::build_from_values_segtree(const std::vector<int32_t> &values) {
    n = values.size();
    // Tree needs 2 * 2^ceil(log2(n)) capacity
    size_t size = 1;
    while (size < n) size <<= 1;
    seg_tree.assign(2 * size, 0);

    // Fill leaves
    for (size_t i = 0; i < n; i++)
        seg_tree[size + i] = values[i];

    // Build internal nodes
    for (size_t i = size - 1; i > 0; --i)
       seg_tree[i] = seg_tree[seg_left(i)] + seg_tree[seg_right(i)];
}

int64_t SumAggregator::seg_query(size_t lo, size_t hi) const {
    // Range [lo, hi)
    int64_t sum = 0;
    size_t size = seg_tree.size() / 2;
    lo += size;
    hi += size;
    while (lo < hi) {
        if (lo & 1) sum += seg_tree[lo++];
        if (hi & 1) sum += seg_tree[--hi];
        lo >>= 1;
        hi >>= 1;
    }
    return sum;
}

void SumAggregator::build_from_values_sqrt_tree(const std::vector<int32_t> &vals) {
    values = vals;
    const size_t n = values.size();
    if (n == 0) return;

    block_size = static_cast<size_t>(std::sqrt(n)) + 1;
    num_blocks = (n + block_size - 1) / block_size;

    block_sum.assign(num_blocks, 0);
    sqrt_prefix.assign(n, 0);
    sqrt_suffix.assign(n, 0);

    for (size_t b = 0; b < num_blocks; ++b) {
        size_t start = b * block_size;
        size_t end = std::min(start + block_size, n);

        int64_t s = 0;
        for (size_t i = start; i < end; ++i) {
            s += values[i];
            sqrt_prefix[i] = s;
        }
        block_sum[b] = s;

        int64_t suf = 0;
        for (size_t i = end; i-- > start; ) {
            suf += values[i];
            sqrt_suffix[i] = suf;
        }
    }
}

int64_t SumAggregator::sqrt_query(size_t lo, size_t hi) const {
    const size_t n = values.size();
    if (n == 0 || lo >= hi || lo >= n) return 0;
    if (hi > n) hi = n;

    const size_t bl = lo / block_size;
    const size_t br = (hi - 1) / block_size;

    // Case 1: within the same block
    if (bl == br) {
        int64_t s = 0;
        for (size_t i = lo; i < hi; ++i)
            s += values[i];
        return s;
    }

    // Case 2: across multiple blocks
    int64_t res = sqrt_suffix[lo]; // remainder of left block

    // full blocks in between
    for (size_t b = bl + 1; b < br; ++b)
        res += block_sum[b];

    // right block partial
    // safe since r > 0 and we guard r <= n
    res += sqrt_prefix[hi - 1];

    return res;
}

int64_t SumAggregator::query(size_t lo, size_t hi) const {
    return prefix[hi] - prefix[lo];
}





