//
// Created by Vegim Shala on 10.7.25.
//

#pragma once
#include "aggregator.h"
#include <numeric>

class SumAggregator : public Aggregator {
public:
    void build_from_values(const std::vector<int32_t> &values) override;

    void build_from_values_segtree(const std::vector<int32_t> &values) override;

    void build_from_values_sqrt_tree(const std::vector<int32_t> &vals) override;

    int64_t query(size_t lo, size_t hi) const override;

    int64_t seg_query(size_t lo, size_t hi) const override;

    int64_t sqrt_query(size_t lo, size_t hi) const override;

private:
    std::vector<int64_t> prefix;

    // For Seg Tree
    std::vector<int64_t> seg_tree;
    size_t n = 0;

    inline size_t seg_left(size_t i) const { return 2 * i; }
    inline size_t seg_right(size_t i) const { return 2 * i + 1; }

    // For SQRT Tree
    size_t block_size = 0;
    size_t num_blocks = 0;
    std::vector<int32_t> values;
    std::vector<int64_t> block_sum; // total sum per block
    std::vector<int64_t> sqrt_prefix; // prefix sum inside each block
    std::vector<int64_t> sqrt_suffix; // suffix sum inside each block


    std::vector<int32_t> arr;
    std::vector<int64_t> /*prefix,*/ suffix;
    std::vector<std::vector<int64_t>> between; // precomputed block-to-block sums
    // size_t n = 0;
    // size_t block_size = 0;
    // size_t num_blocks = 0;


    struct SqrtTree {
        using Item = int64_t;

        int n = 0;
        int lg = 0;
        int index_sz = 0;

        std::vector<Item> v;
        std::vector<int> clz;
        std::vector<int> layers;
        std::vector<int> on_layer;
        std::vector<std::vector<Item>> pref, suf, between;

        void build(const std::vector<int32_t> &a);
        Item query(int l, int r) const;

    private:
        static Item op(Item a, Item b) { return a + b; }

        inline int log2up(int n) const {
            int res = 0;
            while ((1 << res) < n) res++;
            return res;
        }

        void build_block(int layer, int l, int r);
        void build_between(int layer, int lBound, int rBound, int betweenOffs);
        void build_between_zero();
        void build(int layer, int lBound, int rBound, int betweenOffs);
        Item query(int l, int r, int betweenOffs, int base) const;
    };

    SqrtTree sqrt_tree;

};