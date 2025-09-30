//
// Created by Vegim Shala on 11.7.25.
//

#include "operators/utils/partition_utils2.h"
#include <thread>
#include <vector>
#include <unordered_map>
#include <string>
#include <atomic>
#include <numeric>

using namespace PartitionUtils2;


PartitionIndexResult PartitionUtils2::partition_dataset_index(
    const Dataset &dataset,
    const FileSchema &schema,
    const std::vector<std::string> &partition_columns
) {
    auto col_indices = compute_col_indices(schema, partition_columns);
    size_t n = dataset.size();

    std::unordered_map<std::vector<int32_t>, IndexDataset, VecHash, VecEq> global;
    global.reserve(n / 4);

    for (size_t i = 0; i < n; ++i) {
        process_row(dataset, i, col_indices, global);
    }

    return global;
}


PartitionIndexResult PartitionUtils2::partition_dataset_index_morsel(
    const Dataset &dataset,
    const FileSchema &schema,
    const std::vector<std::string> &partition_columns,
    size_t num_threads,
    size_t morsel_size
) {
    auto col_indices = compute_col_indices(schema, partition_columns);
    size_t n = dataset.size();
    std::atomic<size_t> next_morsel(0);

    std::vector<std::unordered_map<std::vector<int32_t>, IndexDataset, VecHash, VecEq> > thread_parts(num_threads);

    auto worker = [&](size_t tid) {
        auto &local = thread_parts[tid];
        while (true) {
            size_t start = next_morsel.fetch_add(morsel_size);
            if (start >= n) break;
            size_t end = std::min(start + morsel_size, n);
            for (size_t i = start; i < end; ++i) {
                process_row(dataset, i, col_indices, local);
            }
        }
    };

    std::vector<std::thread> threads;
    threads.reserve(num_threads);
    for (size_t t = 0; t < num_threads; ++t) {
        threads.emplace_back(worker, t);
    }
    for (auto &th: threads) {
        th.join();
    }

    std::unordered_map<std::vector<int32_t>, IndexDataset, VecHash, VecEq> global;
    global.reserve(num_threads * 4);
    merge_maps(global, thread_parts);

    return global;
}


RadixPartitionResult PartitionUtils2::partition_dataset_radix_morsel(
    const Dataset &dataset,
    const FileSchema &schema,
    const std::vector<std::string> &partition_columns,
    size_t num_threads,
    size_t radix_bits,
    size_t morsel_size
) {
    auto s = radix_setup(dataset, schema, partition_columns, radix_bits);
    const size_t num_morsels = (s.n + morsel_size - 1) / morsel_size;

    // Pass 1: count per morsel
    std::vector<std::vector<size_t> > morsel_bucket_counts(
        num_morsels, std::vector<size_t>(s.num_buckets, 0));
    {
        std::vector<std::thread> threads;
        threads.reserve(num_threads);

        auto worker = [&](size_t tid) {
            // static chunk assignment: contiguous morsels per thread
            const size_t m_start = (num_morsels * tid) / num_threads;
            const size_t m_end   = (num_morsels * (tid + 1)) / num_threads;

            for (size_t m = m_start; m < m_end; ++m) {
                const size_t start = m * morsel_size;
                const size_t end   = std::min(start + morsel_size, s.n);
                radix_count_range(dataset, start, end,
                                  s.col_idx, s.mask, morsel_bucket_counts[m]);
            }
        };

        for (size_t t = 0; t < num_threads; ++t) {
            threads.emplace_back(worker, t);
        }
        for (auto &th : threads) th.join();
    }

    // Prepare buckets + offsets
    RadixPartitionResult buckets;
    std::vector<std::vector<size_t> > morsel_bucket_offsets;
    radix_prepare_buckets(morsel_bucket_counts, s.num_buckets, buckets, morsel_bucket_offsets);

    // Pass 2: scatter
    {
        std::vector<std::thread> threads;
        threads.reserve(num_threads);

        auto worker = [&](size_t tid) {
            const size_t m_start = (num_morsels * tid) / num_threads;
            const size_t m_end   = (num_morsels * (tid + 1)) / num_threads;

            for (size_t m = m_start; m < m_end; ++m) {
                const size_t start = m * morsel_size;
                const size_t end   = std::min(start + morsel_size, s.n);

                // IMPORTANT: use a reference, not a copy
                auto &local_offsets = morsel_bucket_offsets[m];

                radix_scatter_range(dataset, start, end,
                                    s.col_idx, s.mask,
                                    local_offsets, buckets);
            }
        };

        for (size_t t = 0; t < num_threads; ++t) {
            threads.emplace_back(worker, t);
        }
        for (auto &th : threads) th.join();
    }

    return buckets;
}


PartitionUtils2::RadixPartitionResult PartitionUtils2::partition_dataset_radix_sequential(
    const Dataset &dataset,
    const FileSchema &schema,
    const std::vector<std::string> &partition_columns,
    size_t radix_bits
) {
    if (partition_columns.empty() || partition_columns.size() > 1) {
        RadixPartitionResult result(1);
        result[0].reserve(dataset.size());
        for (size_t i = 0; i < dataset.size(); ++i) {
            result[0].push_back(i);
        }
        return result;
    }

    auto s = radix_setup(dataset, schema, partition_columns, radix_bits);

    // ---- PASS 1: Count ----
    std::vector<size_t> counts(s.num_buckets, 0);
    radix_count_range(dataset, 0, s.n, s.col_idx, s.mask, counts);

    // ---- Allocate buckets ----
    RadixPartitionResult buckets(s.num_buckets);
    for (size_t b = 0; b < s.num_buckets; ++b) {
        buckets[b].resize(counts[b]);
    }

    // ---- PASS 2: Scatter ----
    // Per-bucket cursors must be relative to each bucket (0..counts[b]-1)
    std::vector<size_t> cursors(s.num_buckets, 0);
    radix_scatter_range(dataset, 0, s.n, s.col_idx, s.mask, cursors, buckets);

    return buckets;
}


// ---------- Buckets -> per-key partitions (sequential) ----------
SingleKeyIndexMap PartitionUtils2::radix_buckets_to_partitions_sequential(
    const Dataset &dataset,
    size_t col_idx,
    RadixPartitionResult &buckets
) {
    // Option A (one-pass, may reallocate vectors): fastest in practice for many real datasets
    SingleKeyIndexMap out;
    // A light heuristic reserve: number of non-empty buckets
    size_t non_empty = 0;
    for (auto &b: buckets) if (!b.empty()) ++non_empty;
    out.reserve(non_empty * 2);

    for (auto &bucket: buckets) {
        for (size_t row_idx: bucket) {
            int32_t key = dataset[row_idx][col_idx];
            auto &vec = out[key];
            if (vec.empty()) vec.reserve(64);
            vec.push_back(row_idx);
        }
    }
    return out;
}

// ---------- Buckets -> per-key partitions (parallel over buckets) ----------
SingleKeyIndexMap PartitionUtils2::radix_buckets_to_partitions_morsel(
    const Dataset &dataset,
    size_t col_idx,
    RadixPartitionResult &buckets,
    size_t num_threads
) {
    const size_t B = buckets.size();
    std::atomic<size_t> next_bucket{0};

    // Thread-local maps -> merge
    std::vector<SingleKeyIndexMap> thread_parts(num_threads);

    auto worker = [&](size_t tid) {
        auto &local = thread_parts[tid];
        // modest reserve to reduce early rehash
        local.reserve(64);

        while (true) {
            size_t b = next_bucket.fetch_add(1);
            if (b >= B) break;
            auto &bucket = buckets[b];
            for (size_t row_idx: bucket) {
                int32_t key = dataset[row_idx][col_idx];
                auto &vec = local[key];
                if (vec.empty()) vec.reserve(64);
                vec.push_back(row_idx);
            }
        }
    };

    std::vector<std::thread> threads;
    threads.reserve(num_threads);
    for (size_t t = 0; t < num_threads; ++t) threads.emplace_back(worker, t);
    for (auto &th: threads) th.join();

    SingleKeyIndexMap global;
    global.reserve(num_threads * 64);
    merge_maps_1col(global, thread_parts);
    return global;
}

PartitionIndexResult PartitionUtils2::partition_indices_sequential(
    const Dataset &dataset,
    const FileSchema &schema,
    const std::vector<std::string> &partition_columns,
    size_t radix_bits
) {
    // No partition cols -> single empty key
    if (partition_columns.empty()) {
        std::unordered_map<std::vector<int32_t>, IndexDataset, VecHash, VecEq> result;
        result[{}].reserve(dataset.size());
        for (size_t i = 0; i < dataset.size(); ++i) result[{}].push_back(i);
        return result;
    }

    if (partition_columns.size() == 1) {
        // 1-col fast path: radix partition -> compress per key
        auto s = radix_setup(dataset, schema, partition_columns, radix_bits);
        auto buckets = partition_dataset_radix_sequential(dataset, schema, partition_columns, radix_bits);
        auto by_key = radix_buckets_to_partitions_sequential(dataset, s.col_idx, buckets);
        return by_key; // variant will hold unordered_map<int32_t, IndexDataset>
    } else {
        // multi-col: hash partition (existing sequential)
        return partition_dataset_index(dataset, schema, partition_columns);
    }
}

PartitionUtils2::PartitionIndexResult PartitionUtils2::partition_indices_parallel(
    const Dataset &dataset,
    const FileSchema &schema,
    const std::vector<std::string> &partition_columns,
    size_t num_threads,
    size_t morsel_size,
    size_t radix_bits
) {
    if (partition_columns.empty()) {
        std::unordered_map<std::vector<int32_t>, IndexDataset, VecHash, VecEq> result;
        result[{}].reserve(dataset.size());
        for (size_t i = 0; i < dataset.size(); ++i) result[{}].push_back(i);
        return result;
    }

    if (partition_columns.size() == 1) {
        auto s = radix_setup(dataset, schema, partition_columns, radix_bits);
        auto buckets = partition_dataset_radix_morsel(
            dataset, schema, partition_columns,
            num_threads, radix_bits, morsel_size
        );
        auto by_key = radix_buckets_to_partitions_morsel(dataset, s.col_idx, buckets, num_threads);
        return by_key;
    } else {
        return partition_dataset_index_morsel(dataset, schema, partition_columns, num_threads, morsel_size);
    }
}

// This was used to showcase results in the paper for multi-column partitioning with radix hashing.
// Otherwise not used in the main cache
PartitionIndexResult
PartitionUtils2::partition_dataset_hash_radix_sequential_multi(
    const Dataset &dataset,
    const FileSchema &schema,
    const std::vector<std::string> &partition_columns,
    size_t radix_bits /* e.g., 10 -> 1024 buckets */
) {
    // Expect multi-column; if not, fall back or assert as you prefer
    if (partition_columns.size() < 2) {
        // You can route to your 1-col radix, but for this test just use index-partition:
        return partition_dataset_index(dataset, schema, partition_columns);
    }

    const size_t n = dataset.size();
    const size_t num_buckets = size_t(1) << radix_bits;
    const size_t mask = num_buckets - 1;

    // Precompute col indices
    std::vector<size_t> col_indices = compute_col_indices(schema, partition_columns);
    const size_t K = col_indices.size();

    // ---- PASS 1: hash -> radix count ----
    std::vector<size_t> counts(num_buckets, 0);
    for (size_t i = 0; i < n; ++i) {
        uint64_t h = 0;
        // combine all columns; int32 -> uint64_t; mix after each step
        for (size_t j = 0; j < K; ++j) {
            h = mix64(h ^ (uint64_t)(uint32_t)dataset[i][col_indices[j]]);
        }
        counts[h & mask]++; // low bits define bucket
    }

    // ---- Allocate buckets ----
    RadixPartitionResult buckets(num_buckets);
    for (size_t b = 0; b < num_buckets; ++b) {
        buckets[b].resize(counts[b]);
    }

    // ---- PASS 2: hash -> radix scatter ----
    std::vector<size_t> cursors(num_buckets, 0);
    for (size_t i = 0; i < n; ++i) {
        uint64_t h = 0;
        for (size_t j = 0; j < K; ++j) {
            h = mix64(h ^ (uint64_t)(uint32_t)dataset[i][col_indices[j]]);
        }
        size_t b = h & mask;
        buckets[b][cursors[b]++] = i;
    }

    // ---- REFINE: per-bucket exact key grouping ----
    // Build global map< vector<int32_t>, IndexDataset >
    std::unordered_map<std::vector<int32_t>, IndexDataset, VecHash, VecEq> global;
    // Heuristic reserve: many buckets, some keys per bucket
    global.reserve(std::max<size_t>(64, n / 16));

    std::vector<int32_t> key_buf;
    key_buf.reserve(K);

    for (auto &bucket : buckets) {
        if (bucket.empty()) continue;

        // Local map per bucket to reduce rehashing pressure on 'global'
        std::unordered_map<std::vector<int32_t>, IndexDataset, VecHash, VecEq> local;
        // Heuristic: half the bucket might be unique keys; tune as needed
        local.reserve(std::max<size_t>(8, bucket.size() / 2));

        for (size_t row_idx : bucket) {
            key_buf.clear();
            for (size_t j = 0; j < K; ++j) {
                key_buf.push_back(dataset[row_idx][col_indices[j]]);
            }
            auto &vec = local[key_buf];      // copies key_buf into the key
            if (vec.empty()) vec.reserve(4); // small hint
            vec.push_back(row_idx);
        }

        // Merge local->global (move vectors to avoid copies)
        for (auto &kv : local) {
            auto &dst = global[kv.first];
            if (dst.empty()) dst.reserve(kv.second.size());
            std::move(kv.second.begin(), kv.second.end(), std::back_inserter(dst));
        }
    }

    return global;
}




// NEW CODE HERE


using RadixPayloadBuckets = struct {
    // One vector per bucket; all are pre-sized.
    std::vector<std::vector<int32_t>> part_keys;   // the true partition key (needed to split bucket -> exact keys)
    std::vector<std::vector<uint32_t>> order_keys; // encoded order col
    std::vector<std::vector<uint32_t>> values;     // agg values
};


static void radix_prepare_payload_buckets(
    const std::vector<std::vector<size_t>> &morsel_bucket_counts,
    size_t num_buckets,
    RadixPayloadBuckets &buckets,
    std::vector<std::vector<size_t>> &morsel_bucket_offsets)
{
    const size_t M = morsel_bucket_counts.size();
    morsel_bucket_offsets.assign(M, std::vector<size_t>(num_buckets, 0));

    std::vector<size_t> bucket_totals(num_buckets, 0);
    for (size_t m = 0; m < M; ++m)
        for (size_t b = 0; b < num_buckets; ++b)
            bucket_totals[b] += morsel_bucket_counts[m][b];

    // prefix per bucket to compute starting offsets per morsel
    for (size_t b = 0; b < num_buckets; ++b) {
        size_t acc = 0;
        for (size_t m = 0; m < M; ++m) {
            morsel_bucket_offsets[m][b] = acc;
            acc += morsel_bucket_counts[m][b];
        }
    }

    buckets.part_keys.resize(num_buckets);
    buckets.order_keys.resize(num_buckets);
    buckets.values.resize(num_buckets);
    for (size_t b = 0; b < num_buckets; ++b) {
        buckets.part_keys[b].assign(bucket_totals[b], 0);
        buckets.order_keys[b].assign(bucket_totals[b], 0);
        buckets.values[b].assign(bucket_totals[b], 0);
    }
}

static void radix_scatter_payload_range(
    const Dataset &dataset, size_t start, size_t end,
    size_t part_col_idx, // partition column index (single col case)
    size_t order_col_idx,
    size_t value_col_idx,
    uint32_t mask,
    std::vector<size_t> &local_offsets, // writable per-morsel offsets
    RadixPayloadBuckets &buckets)
{
    for (size_t i = start; i < end; ++i) {
        int32_t pkey  = dataset[i][part_col_idx];
        uint32_t b    = static_cast<uint32_t>(pkey) & mask; // same bucketing as count
        uint32_t okey = PartitionUtils2::encode_key(static_cast<int32_t>(dataset[i][order_col_idx]));
        uint32_t val  = static_cast<uint32_t>(dataset[i][value_col_idx]);

        size_t pos = local_offsets[b]++;
        buckets.part_keys[b][pos]  = pkey;
        buckets.order_keys[b][pos] = okey;
        buckets.values[b][pos]     = val;
    }
}

PartitionUtils2::PartitionPayloadResult PartitionUtils2::partition_payloads_parallel(
    const Dataset &dataset,
    const FileSchema &schema,
    const std::vector<std::string> &partition_columns, // must be size()==1 in this radix path
    const std::string &order_column,
    const std::string &value_column,
    size_t num_threads,
    size_t morsel_size,
    size_t radix_bits)
{
    if (partition_columns.empty()) {
        // No partitioning: a single payload keyed by {}
        PayloadMapN out;
        PartitionPayload payload;
        payload.order_keys.reserve(dataset.size());
        payload.values.reserve(dataset.size());
        const size_t order_idx = schema.index_of(order_column);
        const size_t value_idx = schema.index_of(value_column);
        for (size_t i = 0; i < dataset.size(); ++i) {
            payload.order_keys.push_back(PartitionUtils2::encode_key(dataset[i][order_idx]));
            payload.values.push_back(static_cast<uint32_t>(dataset[i][value_idx]));
        }
        out[{}] = std::move(payload);
        return out;
    }

    if (partition_columns.size() == 1) {
        // Setup (same as your radix_setup)
        auto s = radix_setup(dataset, schema, partition_columns, radix_bits);
        const size_t num_morsels = (s.n + morsel_size - 1) / morsel_size;

        // PASS 1: count per morsel (unchanged)
        std::vector<std::vector<size_t>> morsel_bucket_counts(
            num_morsels, std::vector<size_t>(s.num_buckets, 0));

        {
            std::vector<std::thread> threads;
            threads.reserve(num_threads);
            auto worker = [&](size_t tid) {
                const size_t m_start = (num_morsels * tid) / num_threads;
                const size_t m_end   = (num_morsels * (tid + 1)) / num_threads;
                for (size_t m = m_start; m < m_end; ++m) {
                    const size_t start = m * morsel_size;
                    const size_t end   = std::min(start + morsel_size, s.n);
                    radix_count_range(dataset, start, end,
                                      s.col_idx, s.mask, morsel_bucket_counts[m]);
                }
            };
            for (size_t t = 0; t < num_threads; ++t) threads.emplace_back(worker, t);
            for (auto &th : threads) th.join();
        }

        // Prepare payload buckets + offsets
        RadixPayloadBuckets buckets;
        std::vector<std::vector<size_t>> morsel_bucket_offsets;
        radix_prepare_payload_buckets(morsel_bucket_counts, s.num_buckets, buckets, morsel_bucket_offsets);

        // PASS 2: scatter payload (instead of indices)
        {
            std::vector<std::thread> threads;
            threads.reserve(num_threads);
            const size_t order_idx = schema.index_of(order_column);
            const size_t value_idx = schema.index_of(value_column);

            auto worker = [&](size_t tid) {
                const size_t m_start = (num_morsels * tid) / num_threads;
                const size_t m_end   = (num_morsels * (tid + 1)) / num_threads;
                for (size_t m = m_start; m < m_end; ++m) {
                    const size_t start = m * morsel_size;
                    const size_t end   = std::min(start + morsel_size, s.n);
                    auto &local_offsets = morsel_bucket_offsets[m];
                    radix_scatter_payload_range(dataset, start, end,
                                                s.col_idx, order_idx, value_idx,
                                                s.mask, local_offsets, buckets);
                }
            };
            for (size_t t = 0; t < num_threads; ++t) threads.emplace_back(worker, t);
            for (auto &th : threads) th.join();
        }

        // Convert buckets -> final per-key payload maps in parallel (like your radix_buckets_to_partitions_morsel)
        const size_t B = buckets.part_keys.size();
        std::atomic<size_t> next_bucket{0};

        std::vector<PayloadMap1> thread_maps(num_threads);

        auto worker2 = [&](size_t tid) {
            auto &local = thread_maps[tid];
            local.reserve(64);
            while (true) {
                size_t b = next_bucket.fetch_add(1);
                if (b >= B) break;
                auto &pk  = buckets.part_keys[b];
                auto &ok  = buckets.order_keys[b];
                auto &val = buckets.values[b];
                const size_t n = pk.size();
                for (size_t i = 0; i < n; ++i) {
                    int32_t key = pk[i];
                    auto &pp = local[key];
                    if (pp.order_keys.empty()) {
                        // modest reserve to avoid early rehash/realloc
                        pp.order_keys.reserve(64);
                        pp.values.reserve(64);
                    }
                    pp.order_keys.push_back(ok[i]);
                    pp.values.push_back(val[i]);
                }
            }
        };

        {
            std::vector<std::thread> threads;
            threads.reserve(num_threads);
            for (size_t t = 0; t < num_threads; ++t) threads.emplace_back(worker2, t);
            for (auto &th : threads) th.join();
        }

        // Merge thread_maps into a single global map
        PayloadMap1 global;
        global.reserve(num_threads * 64);

        auto append_payload = [](PartitionPayload &dst, PartitionPayload &src) {
            if (!src.order_keys.empty()) {
                dst.order_keys.reserve(dst.order_keys.size() + src.order_keys.size());
                dst.values.reserve(dst.values.size() + src.values.size());
                std::move(src.order_keys.begin(), src.order_keys.end(), std::back_inserter(dst.order_keys));
                std::move(src.values.begin(), src.values.end(), std::back_inserter(dst.values));
            }
        };

        for (auto &m : thread_maps) {
            for (auto &kv : m) {
                append_payload(global[kv.first], kv.second);
            }
        }

        return global; // variant will hold PayloadMap1
    }
    // Multi-column path: keep the existing index-based implementation for now,
    // or implement a similar "build map<vec<int32_t>, PartitionPayload> in parallel".
    // Minimal compatibility fallback: materialize from existing index maps.
    {
        // Fallback: build with indices; single extra pass (only when multi-col)
        PartitionIndexResult idxres = partition_dataset_index_morsel(dataset, schema, partition_columns, num_threads, morsel_size);
        PayloadMapN out;
        const size_t order_idx = schema.index_of(order_column);
        const size_t value_idx = schema.index_of(value_column);

        auto visitor = [&](auto &imap) {
            using MapT = std::decay_t<decltype(imap)>;
            for (auto &kv : imap) {
                auto &inds = kv.second;
                PartitionPayload p;
                p.order_keys.reserve(inds.size());
                p.values.reserve(inds.size());
                for (size_t row_id : inds) {
                    p.order_keys.push_back(encode_key(dataset[row_id][order_idx]));
                    p.values.push_back(static_cast<uint32_t>(dataset[row_id][value_idx]));
                }
                out.emplace(kv.first, std::move(p));
            }
        };
        std::visit(visitor, idxres);
        return out;
    }
}