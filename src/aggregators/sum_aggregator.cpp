//
// Created by Vegim Shala on 10.7.25.
//
#include "aggregators/sum_aggregator.h"

#include <iostream>

void SumAggregator::build_from_values(const std::vector<int32_t> &values) {
    prefix.resize(values.size() + 1);
    prefix[0] = 0;
    for (size_t i = 0; i < values.size(); ++i)
        prefix[i + 1] = prefix[i] + values[i];
}

void SumAggregator::build_from_values_segtree(const std::vector<int32_t> &values) {
    n = values.size();
    seg_tree.resize(2 * n);
    std::copy(values.begin(), values.end(), seg_tree.begin() + n);

    for (size_t i = n - 1; i > 0; --i)
        seg_tree[i] = seg_tree[i << 1] + seg_tree[i << 1 | 1];
}

int64_t SumAggregator::seg_query(size_t lo, size_t hi) const {
    if (hi <= lo) return 0;

    const int64_t *t = seg_tree.data();
    size_t base = n;
    lo += base;
    hi += base;
    int64_t sum = 0;

    // Correct iterative loop
    while (lo < hi) {
        if (lo & 1)
            sum += t[lo++];
        if (hi & 1)
            sum += t[--hi];
        lo >>= 1;
        hi >>= 1;
    }

    return sum;
}

// void SumAggregator::build_from_values_sqrt_tree(const std::vector<int32_t> &vals) {
//     values = vals;
//     const size_t n = values.size();
//     if (n == 0) return;
//
//     block_size = static_cast<size_t>(std::sqrt(n)) + 1;
//     num_blocks = (n + block_size - 1) / block_size;
//
//     block_sum.assign(num_blocks, 0);
//     sqrt_prefix.assign(n, 0);
//     sqrt_suffix.assign(n, 0);
//
//     for (size_t b = 0; b < num_blocks; ++b) {
//         size_t start = b * block_size;
//         size_t end = std::min(start + block_size, n);
//
//         int64_t s = 0;
//         for (size_t i = start; i < end; ++i) {
//             s += values[i];
//             sqrt_prefix[i] = s;
//         }
//         block_sum[b] = s;
//
//         int64_t suf = 0;
//         for (size_t i = end; i-- > start; ) {
//             suf += values[i];
//             sqrt_suffix[i] = suf;
//         }
//     }
// }
//
// int64_t SumAggregator::sqrt_query(size_t lo, size_t hi) const {
//     const size_t n = values.size();
//     if (n == 0 || lo >= hi || lo >= n) return 0;
//     if (hi > n) hi = n;
//
//     const size_t bl = lo / block_size;
//     const size_t br = (hi - 1) / block_size;
//
//     // Case 1: within the same block
//     if (bl == br) {
//         int64_t s = 0;
//         for (size_t i = lo; i < hi; ++i)
//             s += values[i];
//         return s;
//     }
//
//     // Case 2: across multiple blocks
//     int64_t res = sqrt_suffix[lo]; // remainder of left block
//
//     // full blocks in between
//     for (size_t b = bl + 1; b < br; ++b)
//         res += block_sum[b];
//
//     // right block partial
//     // safe since r > 0 and we guard r <= n
//     res += sqrt_prefix[hi - 1];
//
//     return res;
// }

int64_t SumAggregator::query(size_t lo, size_t hi) const {
    return prefix[hi] - prefix[lo];
}






void SumAggregator::SqrtTree::build_block(int layer, int l, int r) {
    pref[layer][l] = v[l];
    for (int i = l + 1; i < r; i++)
        pref[layer][i] = op(pref[layer][i - 1], v[i]);

    suf[layer][r - 1] = v[r - 1];
    for (int i = r - 2; i >= l; i--)
        suf[layer][i] = op(v[i], suf[layer][i + 1]);
}

void SumAggregator::SqrtTree::build_between(int layer, int lBound, int rBound, int betweenOffs) {
    int bSzLog = (layers[layer] + 1) >> 1;
    int bCntLog = layers[layer] >> 1;
    int bSz = 1 << bSzLog;
    int bCnt = (rBound - lBound + bSz - 1) >> bSzLog;

    for (int i = 0; i < bCnt; i++) {
        Item ans = 0;
        for (int j = i; j < bCnt; j++) {
            Item add = suf[layer][lBound + (j << bSzLog)];
            ans = (i == j) ? add : op(ans, add);
            between[layer - 1][betweenOffs + lBound + (i << bCntLog) + j] = ans;
        }
    }
}

void SumAggregator::SqrtTree::build_between_zero() {
    int bSzLog = (lg + 1) >> 1;
    for (int i = 0; i < index_sz; i++)
        v[n + i] = suf[0][i << bSzLog];
    build(1, n, n + index_sz, (1 << lg) - n);
}

void SumAggregator::SqrtTree::build(int layer, int lBound, int rBound, int betweenOffs) {
    if (layer >= (int)layers.size()) return;
    int bSz = 1 << ((layers[layer] + 1) >> 1);
    for (int l = lBound; l < rBound; l += bSz) {
        int r = std::min(l + bSz, rBound);
        build_block(layer, l, r);
        build(layer + 1, l, r, betweenOffs);
    }
    if (layer == 0)
        build_between_zero();
    else
        build_between(layer, lBound, rBound, betweenOffs);
}

SumAggregator::SqrtTree::Item
SumAggregator::SqrtTree::query(int l, int r, int betweenOffs, int base) const {
    if (l == r) return v[l];
    if (l + 1 == r) return op(v[l], v[r]);

    int layer = on_layer[clz[(l - base) ^ (r - base)]];
    int bSzLog = (layers[layer] + 1) >> 1;
    int bCntLog = layers[layer] >> 1;
    int lBound = (((l - base) >> layers[layer]) << layers[layer]) + base;
    int lBlock = ((l - lBound) >> bSzLog) + 1;
    int rBlock = ((r - lBound) >> bSzLog) - 1;

    Item ans = suf[layer][l];
    if (lBlock <= rBlock) {
        Item add = (layer == 0)
                       ? query(n + lBlock, n + rBlock, (1 << lg) - n, n)
                       : between[layer - 1][betweenOffs + lBound + (lBlock << bCntLog) + rBlock];
        ans = op(ans, add);
    }
    ans = op(ans, pref[layer][r]);
    return ans;
}

SumAggregator::SqrtTree::Item
SumAggregator::SqrtTree::query(int l, int r) const {
    return query(l, r, 0, 0);
}

void SumAggregator::SqrtTree::build(const std::vector<int32_t> &a) {
    n = (int)a.size();
    if (n == 0) return;

    v.assign(a.begin(), a.end());
    lg = log2up(n);
    clz.assign(1 << lg, 0);
    for (int i = 1; i < (1 << lg); ++i)
        clz[i] = clz[i >> 1] + 1;

    int tlg = lg;
    while (tlg > 1) {
        on_layer.resize(lg + 1);
        on_layer[tlg] = (int)layers.size();
        layers.push_back(tlg);
        tlg = (tlg + 1) >> 1;
    }

    for (int i = lg - 1; i >= 0; i--)
        on_layer[i] = std::max(on_layer[i], on_layer[i + 1]);

    int betweenLayers = std::max(0, (int)layers.size() - 1);
    int bSzLog = (lg + 1) >> 1;
    int bSz = 1 << bSzLog;
    index_sz = (n + bSz - 1) >> bSzLog;

    v.resize(n + index_sz);
    pref.assign(layers.size(), std::vector<Item>(n + index_sz));
    suf.assign(layers.size(), std::vector<Item>(n + index_sz));
    between.assign(betweenLayers, std::vector<Item>((1 << lg) + bSz));

    build(0, 0, n, 0);
}

void SumAggregator::build_from_values_sqrt_tree(const std::vector<int32_t> &vals) {
    sqrt_tree.build(vals);
}

int64_t SumAggregator::sqrt_query(size_t lo, size_t hi) const {
    if (lo >= hi) return 0;
    return sqrt_tree.query((int)lo, (int)(hi - 1));
}
