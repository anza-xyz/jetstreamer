//! Parallel read-throughput scaling benchmark for a horizon archive.
//!
//! The archive is bucket-parallel: every 128-slot bucket is a self-contained
//! zstd frame with its own encoder reset, so disjoint slot sub-ranges decode
//! independently. This benchmark finds the aggregate decode-throughput
//! ceiling on the current machine, sweeping thread count *past* the core
//! count — decode has stalls (memory bandwidth, allocation, zstd) that
//! oversubscription can hide, so the sweet spot is often a few× the cores.
//!
//! Method (strong scaling, cache-hot):
//! 1. Pick a contiguous mid-epoch working region, bucket-aligned, sized to
//!    stay resident in the page cache and to have at least one bucket per
//!    thread at the top of the sweep.
//! 2. Measure a single-core baseline on a small warm chunk.
//! 3. Warm the whole region, then for each thread count N split it into N
//!    disjoint bucket-aligned sub-ranges (one fresh `ArchiveReader` each) and
//!    decode. Total work is constant, so aggregate tx/s rises until the
//!    machine saturates; the peak N is the sweet spot.
//!
//! Usage:
//! `cargo run --release -p jetstreamer-horizon --example bench_parallel_read -- <path> [region_buckets] [threads_max]`

use std::io::BufReader;
use std::time::Instant;

use jetstreamer_horizon::archive::{ArchiveReader, BlockNotification, EntryRecord, SlotVisitor};
use jetstreamer_horizon::transactions::Transaction;

const BUCKET_SLOTS: u64 = 128;

#[derive(Default)]
struct Counter {
    transactions: u64,
}

impl SlotVisitor for Counter {
    fn on_transaction(&mut self, _slot: u64, _tx_index: u32, _tx: &Transaction) {
        self.transactions += 1;
    }
    fn on_block(&mut self, _n: &BlockNotification, _e: &[EntryRecord]) {}
}

/// Decodes `[start, start + count)` from `path` with a fresh reader; returns
/// the transactions decoded.
fn decode_range(path: &str, start: u64, count: u64) -> u64 {
    let file = std::fs::File::open(path).expect("open");
    let source = BufReader::with_capacity(8 << 20, file);
    let mut reader = ArchiveReader::open(source).expect("open archive");
    let mut counter = Counter::default();
    reader
        .read_slots(start, count, &mut counter)
        .expect("read_slots");
    counter.transactions
}

/// Splits `region_buckets` into `n` contiguous bucket groups and decodes them
/// concurrently (one reader per thread); returns (wall_seconds, total_txs).
fn parallel_decode(path: &str, region_start: u64, region_buckets: u64, n: usize) -> (f64, u64) {
    let base = region_buckets / n as u64;
    let extra = region_buckets % n as u64;
    let start = Instant::now();
    let totals: Vec<u64> = std::thread::scope(|scope| {
        let mut handles = Vec::new();
        let mut cursor = 0u64;
        for t in 0..n as u64 {
            let buckets = base + if t < extra { 1 } else { 0 };
            if buckets == 0 {
                continue;
            }
            let sub_start = region_start + cursor * BUCKET_SLOTS;
            let sub_count = buckets * BUCKET_SLOTS;
            cursor += buckets;
            let path = path.to_string();
            handles.push(scope.spawn(move || decode_range(&path, sub_start, sub_count)));
        }
        handles
            .into_iter()
            .map(|h| h.join().expect("thread"))
            .collect()
    });
    (start.elapsed().as_secs_f64(), totals.iter().sum())
}

fn main() {
    let mut args = std::env::args().skip(1);
    let path = args
        .next()
        .expect("usage: bench_parallel_read <path> [region_buckets] [threads_max]");
    let cores = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(8);
    // Enough buckets to keep every thread fed at the top of the sweep, while
    // staying cache-resident. Default ≈ 256 buckets (~15 GiB of file).
    let region_buckets: u64 = args
        .next()
        .map(|v| v.parse().expect("region_buckets"))
        .unwrap_or(256);
    // Sweep through heavy oversubscription (default up to 8× cores).
    let threads_max: usize = args
        .next()
        .map(|v| v.parse().expect("threads_max"))
        .unwrap_or(cores * 8);

    let (slot_start, slot_count) = {
        let file = std::fs::File::open(&path).expect("open");
        let reader = ArchiveReader::open(BufReader::new(file)).expect("open archive for header");
        let h = reader.header();
        (h.slot_start, h.slot_count)
    };
    let region_buckets = region_buckets.min(slot_count / BUCKET_SLOTS);
    let region_slots = region_buckets * BUCKET_SLOTS;
    // Centre the region mid-epoch (bucket-aligned; slot_start is aligned).
    let mid = slot_start + (slot_count / 2 / BUCKET_SLOTS) * BUCKET_SLOTS;
    let region_start = mid.min(slot_start + slot_count - region_slots);

    eprintln!(
        "machine: {cores} cores | region: slots {region_start}..{} ({region_buckets} buckets, ~{:.1} GiB of file)",
        region_start + region_slots,
        region_slots as f64 * 470_000.0 / (1u64 << 30) as f64,
    );

    // Single-core baseline on a small warm chunk (16 buckets).
    let base_buckets = region_buckets.min(16);
    decode_range(&path, region_start, base_buckets * BUCKET_SLOTS); // warm
    let bstart = Instant::now();
    let base_txs = decode_range(&path, region_start, base_buckets * BUCKET_SLOTS);
    let base_secs = bstart.elapsed().as_secs_f64();
    let baseline_tps = base_txs as f64 / base_secs;
    eprintln!(
        "single-core baseline: {:.0} tx/s ({base_txs} txs / {base_secs:.2}s over {base_buckets} buckets)\n",
        baseline_tps,
    );

    // Warm the whole region (parallel, fast) and learn its tx count.
    let (warm_wall, region_txs) = parallel_decode(&path, region_start, region_buckets, cores);
    eprintln!(
        "warmed region: {region_txs} txs (parallel warm {warm_wall:.1}s)\n",
        region_txs = region_txs,
    );

    // Start at 8 — the per-core figure comes from the baseline, and low N on
    // a large region is needlessly slow (strong scaling = more work/thread).
    let mut sweep: Vec<usize> = vec![8, 12, 16, 24, 32, 48, 64, 96, 128, 192, 255, 384, 512]
        .into_iter()
        .filter(|&n| n <= threads_max && (n as u64) <= region_buckets)
        .collect();
    if !sweep.contains(&cores) && cores <= threads_max {
        sweep.push(cores);
        sweep.sort_unstable();
    }

    println!(
        "{:>7}  {:>9}  {:>14}  {:>12}",
        "threads", "wall (s)", "agg tx/s", "core-equiv"
    );
    let mut best = (0usize, 0.0f64);
    for &n in &sweep {
        let (wall, txs) = parallel_decode(&path, region_start, region_buckets, n);
        assert_eq!(
            txs, region_txs,
            "parallel decode lost/duplicated transactions"
        );
        let agg = txs as f64 / wall;
        if agg > best.1 {
            best = (n, agg);
        }
        println!(
            "{:>7}  {:>9.2}  {:>14.0}  {:>11.1}x",
            n,
            wall,
            agg,
            agg / baseline_tps.max(1.0),
        );
    }

    println!(
        "\npeak: {:.0} tx/s at {} threads ({:.1}x a single core, on {} cores)",
        best.1,
        best.0,
        best.1 / baseline_tps.max(1.0),
        cores,
    );
}
