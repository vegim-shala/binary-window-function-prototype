//
// Created by Vegim Shala on 10.7.25.
//

#pragma once

#include "data_io.h"
#include "thread_pool.h"
#include <unordered_map>

// Portable prefetch macro
#if defined(__GNUC__) || defined(__clang__)
#define PREFETCH(addr) __builtin_prefetch((addr), 0, 1)
#else
  #define PREFETCH(addr) ((void)0)
#endif


struct JoinSpec {
    std::string begin_column;
    std::string end_column;
};

class JoinUtils {
public:
    explicit JoinUtils(const JoinSpec &join_spec, const std::string &order_column)
        : join_spec(join_spec), order_column(order_column) {
    }

    inline void build_keys_and_values(
    const Dataset &input,
    const FileSchema &schema,
    const std::vector<size_t> &sorted_indices,
    size_t order_idx,
    size_t value_idx,
    std::vector<int32_t> &out_values
) {
        keys.clear();
        keys.reserve(sorted_indices.size());
        out_values.clear();
        out_values.reserve(sorted_indices.size());

        for (size_t pos : sorted_indices) {
            const auto &row = input[pos];
            keys.push_back(row[order_idx]);
            out_values.push_back(row[value_idx]);
        }
    }

    void build_index(const Dataset &input, const FileSchema &schema, std::string &value_column);

    void build_index_from_vectors_segtree(const std::vector<int32_t> &sorted_keys,
                                          const std::vector<int32_t> &values);

    // void build_index_from_vectors_segtree_parallel(const std::vector<int32_t>& sorted_keys, const std::vector<int32_t>& values,
    //                                                           ThreadPool& pool);

    void build_index_from_vectors_prefix_sums(const std::vector<int32_t> &sorted_keys,
                                              const std::vector<int32_t> &values);

    void build_prefix_sums_from_sorted_indices(const Dataset &input,
                                               const FileSchema &schema,
                                               const std::vector<size_t> &sorted_indices,
                                               size_t order_idx,
                                               size_t value_idx);

    void build_index_from_vectors_sqrt_tree(const std::vector<int32_t> &sorted_keys,
                                            const std::vector<int32_t> &values);

    void build_index_from_vectors_two_pointer_sweep(const std::vector<int32_t> &sorted_keys,
                                                    const std::vector<int32_t> &values);

    void build_eytzinger();


    inline int64_t seg_query(size_t l, size_t r) const {
        int64_t res = 0;
        for (l += n, r += n; l < r; l >>= 1, r >>= 1) {
            // we start from leaves and go up, l>>=1 means moving to the parent
            if (l & 1) res += segtree[l++];
            // if l is odd, we include segtree[l] and move to the next. In the tree this means we move to the right sibling
            if (r & 1) res += segtree[--r]; // if r is odd, we move back and include segtree[r]
        }
        return res;
    }

    inline int64_t prefix_sums_query(size_t l, size_t r) const {
        return prefix[r] - prefix[l]; // O(1)
    }

    // Branchless lower_bound: first index >= x
    inline size_t branchless_lower_bound(const std::vector<int32_t> &arr, int32_t x) const {
        size_t n = arr.size();
        size_t pos = 0;
        size_t step = 1ull << (63 - __builtin_clzll(n)); // largest power of 2 <= n

        while (step) {
            size_t next = pos + step;
            if (next <= n && arr[next - 1] < x) {
                pos = next;
            }
            step >>= 1;
        }
        return pos;
    }

    // Branchless upper_bound: first index > x
    inline size_t branchless_upper_bound(const std::vector<int32_t> &arr, int32_t x) const {
        size_t n = arr.size();
        size_t pos = 0;
        size_t step = 1ull << (63 - __builtin_clzll(n)); // largest power of 2 <= n

        while (step) {
            size_t next = pos + step;
            if (next <= n && arr[next - 1] <= x) {
                pos = next;
            }
            step >>= 1;
        }
        return pos;
    }


    inline size_t eyt_lower(int32_t x) const {
        size_t n = keys.size();
        size_t k = 1; // root
        size_t res = n; // default = not found

        while (k <= n) {
            PREFETCH(&eyt[2 * k]);
            if (x <= eyt[k]) {
                res = pos_to_orig[k]; // candidate
                k = 2 * k; // go left
            } else {
                k = 2 * k + 1; // go right
            }
        }
        return res;
    }

    inline size_t eyt_upper(int32_t x) const {
        size_t n = keys.size();
        size_t k = 1; // root
        size_t res = n; // default = not found

        while (k <= n) {
            PREFETCH(&eyt[2 * k]);
            if (x < eyt[k]) {
                res = pos_to_orig[k]; // candidate
                k = 2 * k; // go left
            } else {
                k = 2 * k + 1; // go right
            }
        }
        return res;
    }

    // Returns first index i in [L, R) with a[i] >= x
    inline size_t bounded_lower_bound(size_t L, size_t R, int32_t x) const {
        if (L >= R) return L;
        size_t n = R - L;
        size_t pos = 0;
        // largest power of two <= n
#if defined(__GNUC__) || defined(__clang__)
        size_t step = 1ull << (63 - __builtin_clzll(n));
#else
                size_t step = 1;
                while ((step << 1) <= n) step <<= 1;
#endif
        while (step) {
            size_t nxt = pos + step;
            if (nxt <= n && keys[L + nxt - 1] < x) pos = nxt;
            step >>= 1;
        }
        return L + pos;
    }

    // Returns first index i in [L, R) with a[i] > x
    inline size_t bounded_upper_bound(size_t L, size_t R, int32_t x) const {
        if (L >= R) return L;
        size_t n = R - L;
        size_t pos = 0;
#if defined(__GNUC__) || defined(__clang__)
        size_t step = 1ull << (63 - __builtin_clzll(n));
#else
                size_t step = 1;
                while ((step << 1) <= n) step <<= 1;
#endif
        while (step) {
            size_t nxt = pos + step;
            if (nxt <= n && keys[L + nxt - 1] <= x) pos = nxt;
            step >>= 1;
        }
        return L + pos;
    }

    // Forward gallop to lower_bound starting from `pos`, then bounded refine.
    inline size_t eyt_gallop_lower(size_t start, int32_t x) const {
        size_t n = keys.size();
        if (start >= n || keys[start] >= x) return start;

        size_t cur = start, step = 1;
        while (cur + step < n && keys[cur + step] < x) {
            PREFETCH(&keys[cur + step + step]);
            cur += step;
            step <<= 1;
        }
        size_t L = cur + 1;
        size_t R = std::min(n, cur + step + 1);
        return bounded_lower_bound(L, R, x);
    }

    // Forward gallop to upper_bound starting from `pos`, then bounded refine.
    inline size_t eyt_gallop_upper(size_t start, int32_t x) const {
        size_t n = keys.size();
        if (start >= n || keys[start] > x) return start;

        size_t cur = start, step = 1;
        while (cur + step < n && keys[cur + step] <= x) {
            PREFETCH(&keys[cur + step + step]);
            cur += step;
            step <<= 1;
        }
        size_t L = cur + 1;
        size_t R = std::min(n, cur + step + 1);
        return bounded_upper_bound(L, R, x);
    }

    inline std::pair<size_t, size_t>
    safe_bounds(size_t start, size_t end) const {
        auto lo_it = std::lower_bound(keys.begin(), keys.end(), start);
        auto hi_it = std::upper_bound(keys.begin(), keys.end(), end);

        size_t lo = static_cast<size_t>(lo_it - keys.begin());
        size_t hi = static_cast<size_t>(hi_it - keys.begin());

        if (hi > keys.size()) {
            throw std::runtime_error("safe_bounds: hi out of range ("
                                     + std::to_string(hi) + "/" + std::to_string(keys.size()) + ")");
        }
        return {lo, hi};
    }

    // Minimal batched lower_bound
    inline std::vector<size_t> batched_lower_bound(
        const std::vector<int32_t> &queries
    ) const {
        size_t n = keys.size();
        size_t q = queries.size();

        std::vector<size_t> lows(q, 0);
        std::vector<size_t> highs(q, n);

        while (true) {
            bool updated = false;
            for (size_t i = 0; i < q; i++) {
                if (lows[i] < highs[i]) {
                    size_t mid = (lows[i] + highs[i]) >> 1;
                    if (keys[mid] < queries[i]) {
                        lows[i] = mid + 1;
                    } else {
                        highs[i] = mid;
                    }
                    updated = true;
                }
            }
            if (!updated) break;
        }
        return lows; // lows[i] == first index >= queries[i]
    }

    // Minimal batched upper_bound
    inline std::vector<size_t> batched_upper_bound(
        const std::vector<int32_t> &queries
    ) const {
        size_t n = keys.size();
        size_t q = queries.size();

        std::vector<size_t> lows(q, 0);
        std::vector<size_t> highs(q, n);

        while (true) {
            bool updated = false;
            for (size_t i = 0; i < q; i++) {
                if (lows[i] < highs[i]) {
                    size_t mid = (lows[i] + highs[i]) >> 1;
                    if (keys[mid] <= queries[i]) {
                        lows[i] = mid + 1;
                    } else {
                        highs[i] = mid;
                    }
                    updated = true;
                }
            }
            if (!updated) break;
        }
        return lows; // lows[i] == first index > queries[i]
    }

    inline std::vector<size_t> batched_lower_bound_bitwise(
        const std::vector<int32_t> &queries
    ) const {
        size_t n = keys.size();
        size_t q = queries.size();

        std::vector<size_t> pos(q, 0);

        // Largest power of two ≤ n
        size_t step = 1ULL << (63 - __builtin_clzll(n));

        while (step > 0) {
            for (size_t i = 0; i < q; i++) {
                size_t next = pos[i] + step;
                if (next < n && keys[next] < queries[i]) {
                    pos[i] = next;
                }
            }
            step >>= 1;
        }

        // After loop, pos[i] points to the greatest index where arr[pos[i]] < query
        // So the true lower_bound is pos[i] + 1
        for (size_t i = 0; i < q; i++) {
            if (pos[i] < n && keys[pos[i]] < queries[i]) {
                pos[i]++;
            }
        }

        return pos;
    }

    inline std::vector<size_t> batched_upper_bound_bitwise(
        const std::vector<int32_t> &queries
    ) const {
        size_t n = keys.size();
        size_t q = queries.size();

        std::vector<size_t> pos(q, 0);

        size_t step = 1ULL << (63 - __builtin_clzll(n));

        while (step > 0) {
            for (size_t i = 0; i < q; i++) {
                size_t next = pos[i] + step;
                if (next < n && keys[next] <= queries[i]) {
                    pos[i] = next;
                }
            }
            step >>= 1;
        }

        // For upper_bound, true position is pos[i] + 1
        for (size_t i = 0; i < q; i++) {
            if (pos[i] < n && keys[pos[i]] <= queries[i]) {
                pos[i]++;
            }
        }

        return pos;
    }

    inline size_t lower_from_hint(size_t start, int32_t x) const {
        const size_t n = keys.size();
        if (start >= n || keys[start] >= x) return start;
        size_t cur = start, step = 1;
        while (cur + step < n && keys[cur + step] < x) {
            PREFETCH(&keys[cur + (step<<1)]);
            cur += step;
            step <<= 1;
        }
        // bounded lower_bound in (cur, cur+step]
        size_t L = cur + 1, R = std::min(n, cur + step + 1), len = R - L, pos = 0;
#if defined(__GNUC__) || defined(__clang__)
        size_t s = 1ull << (63 - __builtin_clzll(len ? len : 1));
#else
        size_t s = 1; while ((s<<1) <= len) s <<= 1;
#endif
        while (s) {
            size_t nxt = pos + s;
            if (nxt <= len && keys[L + nxt - 1] < x) pos = nxt;
            s >>= 1;
        }
        return L + pos;
    }

    inline size_t upper_from_hint(size_t start, int32_t x) const {
        const size_t n = keys.size();
        if (start >= n || keys[start] > x) return start;
        size_t cur = start, step = 1;
        while (cur + step < n && keys[cur + step] <= x) {
            PREFETCH(&keys[cur + (step<<1)]);
            cur += step;
            step <<= 1;
        }
        size_t L = cur + 1, R = std::min(n, cur + step + 1), len = R - L, pos = 0;
#if defined(__GNUC__) || defined(__clang__)
        size_t s = 1ull << (63 - __builtin_clzll(len ? len : 1));
#else
        size_t s = 1; while ((s<<1) <= len) s <<= 1;
#endif
        while (s) {
            size_t nxt = pos + s;
            if (nxt <= len && keys[L + nxt - 1] <= x) pos = nxt;
            s >>= 1;
        }
        return L + pos;
    }

    // Bounded upper_bound on [L, R): first i with keys[i] > x, with L <= i <= R
    inline size_t bounded_upper_bound_i32(size_t L, size_t R, int32_t x) const {
        if (L >= R) return L;
        size_t n = R - L, pos = 0;
#if defined(__GNUC__) || defined(__clang__)
        size_t s = 1ull << (63 - __builtin_clzll(n ? n : 1));
#else
        size_t s = 1; while ((s<<1) <= n) s <<= 1;
#endif
        while (s) {
            size_t nxt = pos + s;
            if (nxt <= n && keys[L + nxt - 1] <= x) pos = nxt;
            s >>= 1;
        }
        return L + pos;
    }


    size_t keys_size() const { return keys.size(); }

     // Interleaved batched upper_bound on [lo[i], n) for each query q[i] (int32_t).
    // Returns hi[i] = first index j >= lo[i] with keys[j] > q[i] (or n if none).
    inline std::vector<size_t> batched_upper_bound_i32_interleaved_from_lo(
        const std::vector<int32_t> &q,
        const std::vector<size_t> &lo
    ) const {
        const size_t n = keys.size();
        const size_t m = q.size();
        std::vector<size_t> pos(m);

        if (m == 0 || n == 0) return pos; // all zeros

        // Initialize each search to "last position <= q[i]" but never before lo[i].
        // Trick: start at lo[i] - 1 (clamped at 0), then the bitwise walk only moves forward.
        for (size_t i = 0; i < m; ++i) {
            pos[i] = (lo[i] == 0) ? 0 : (lo[i] - 1);
        }

        // Global step: largest power of two <= n
#if defined(__GNUC__) || defined(__clang__)
        size_t step = 1ull << (63 - __builtin_clzll(n));
#else
        size_t step = 1; while ((step << 1) <= n) step <<= 1;
#endif

        while (step > 0) {
            for (size_t i = 0; i < m; ++i) {
                const size_t next = pos[i] + step;
                // Stay inside array; classic upper_bound <= comparison
                if (next < n && keys[next] <= q[i]) {
                    pos[i] = next;
                }
            }
            step >>= 1;
        }

        // Convert "last <= q[i]" into "first > q[i]"
        for (size_t i = 0; i < m; ++i) {
            if (pos[i] < n && keys[pos[i]] <= q[i]) {
                ++pos[i];
            }
        }
        return pos; // this is hi[]
    }

    // Interleaved batched LOWER bound: first index >= q[i]
    inline std::vector<size_t> batched_lower_bound_i32_interleaved(
        const std::vector<int32_t> &q
    ) const {
        const size_t n = keys.size(), m = q.size();
        std::vector<size_t> pos(m, 0);

        if (m == 0 || n == 0) return pos;

#if defined(__GNUC__) || defined(__clang__)
        size_t step = 1ull << (63 - __builtin_clzll(n));
#else
        size_t step = 1; while ((step << 1) <= n) step <<= 1;
#endif

        // Find greatest index with keys[pos] < q[i]
        while (step > 0) {
            for (size_t i = 0; i < m; ++i) {
                const size_t next = pos[i] + step;
                if (next < n && keys[next] < q[i]) pos[i] = next;
            }
            step >>= 1;
        }

        // Convert to first index >= q[i]
        for (size_t i = 0; i < m; ++i) {
            if (pos[i] < n && keys[pos[i]] < q[i]) ++pos[i];
        }
        return pos;
    }

    // --- For bucketed search ---
    size_t bucket_size = 1024; // tuneable
    std::vector<int32_t> block_mins; // first key of each block

    void build_buckets();

    inline size_t bucket_lower(int32_t x) const {
        if (keys.empty()) return 0;

        // Step 1: find candidate block
        auto it = std::upper_bound(block_mins.begin(), block_mins.end(), x);
        size_t block = (it == block_mins.begin()) ? 0 : (it - block_mins.begin() - 1);

        // Step 2: search inside block
        size_t start = block * bucket_size;
        size_t end = std::min(start + bucket_size, keys.size());

        auto inner = std::lower_bound(keys.begin() + start, keys.begin() + end, x);
        return inner - keys.begin();
    }

    inline size_t bucket_upper(int32_t x) const {
        if (keys.empty()) return 0;

        auto it = std::upper_bound(block_mins.begin(), block_mins.end(), x);
        size_t block = (it == block_mins.begin()) ? 0 : (it - block_mins.begin() - 1);

        size_t start = block * bucket_size;
        size_t end = std::min(start + bucket_size, keys.size());

        auto inner = std::upper_bound(keys.begin() + start, keys.begin() + end, x);
        return inner - keys.begin();
    }

    // --- sqrt decomposition storage ---
    // build & query
    // For SQRT Tree
    size_t block_size = 0;
    size_t num_blocks = 0;
    std::vector<int32_t> values;
    std::vector<int64_t> block_sum; // total sum per block
    std::vector<int64_t> sqrt_prefix; // prefix sum inside each block
    std::vector<int64_t> sqrt_suffix; // suffix sum inside each block
    void build_index_from_vectors_sqrttree(const std::vector<int32_t> &sorted_keys,
                                           const std::vector<int32_t> &vals);

    inline int64_t sqrt_query(size_t l, size_t r) const {
        if (l >= r) return 0;
        size_t bl = l / block_size;
        size_t br = (r - 1) / block_size;

        if (bl == br) {
            // within one block → sum directly
            int64_t s = 0;
            for (size_t i = l; i < r; i++) {
                s += values[i];
            }
            return s;
        }

        int64_t res = sqrt_suffix[l]; // remainder of left block
        for (size_t b = bl + 1; b < br; ++b) {
            res += block_sum[b]; // full blocks
        }
        res += sqrt_prefix[r - 1]; // prefix of right block
        return res;
    }

private:
    JoinSpec join_spec;
    const std::string order_column;

    // For Segment Tree
    size_t n = 0;
    std::vector<int32_t> keys; // sorted probe/build keys
    std::vector<int64_t> segtree; // 1..(2n-1) used (0 unused)

    // For Prefix Sums
    std::vector<int64_t> prefix;


    // Eytzinger
    // std::vector<int32_t> eyt_keys;
    std::vector<int32_t> eyt;
    std::vector<size_t> pos_to_orig;
};
