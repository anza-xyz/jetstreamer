//! Parallel read-throughput scaling benchmark for a horizon archive.
//!
//! The archive is bucket-parallel: every 128-slot bucket is a self-contained
//! zstd frame with its own encoder reset, so disjoint slot sub-ranges decode
//! independently. This benchmark finds the aggregate decode-throughput
//! ceiling on the current machine.
//!
//! Zero-config: it auto-detects the core count and sweeps thread counts as
//! multiples of it (1×, 2×, **4×**, 6×, 8×), centered on the empirical sweet
//! spot of ~4× cores — decode has stalls (memory bandwidth, per-blob
//! allocation, zstd) that oversubscription hides, so the peak typically sits
//! a few× past the core count. The working region is sized and warmed into
//! the page cache so the sweep measures the CPU/memory ceiling, not disk.
//!
//! Usage (no args needed):
//! `cargo run --release -p jetstreamer-horizon --example bench_parallel_read -- <path>`
//!
//! Optional overrides: `<path> [region_buckets] [max_threads]`.

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
        .expect("usage: bench_parallel_read <path> [region_buckets] [max_threads]");
    let cores = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(8);

    // Thread counts swept as multiples of the core count, centered on 4×.
    let multipliers = [1usize, 2, 4, 6, 8];
    let max_threads: usize = args
        .next() // (skipped if only path given)
        .and_then(|v| v.parse().ok())
        .unwrap_or(cores * multipliers.last().copied().unwrap_or(8));
    // Region: enough buckets that every thread gets at least one at the top
    // of the sweep, with a couple to spare; default ~8× cores (min 256).
    let region_buckets_arg: Option<u64> = args.next().and_then(|v| v.parse().ok());

    let (slot_start, slot_count) = {
        let file = std::fs::File::open(&path).expect("open");
        let reader = ArchiveReader::open(BufReader::new(file)).expect("open archive for header");
        let h = reader.header();
        (h.slot_start, h.slot_count)
    };
    let total_buckets = slot_count / BUCKET_SLOTS;
    let region_buckets = region_buckets_arg
        .unwrap_or_else(|| (max_threads as u64).max(256))
        .min(total_buckets);
    let region_slots = region_buckets * BUCKET_SLOTS;
    // Centre the region mid-epoch (bucket-aligned; slot_start is aligned).
    let mid = slot_start + (slot_count / 2 / BUCKET_SLOTS) * BUCKET_SLOTS;
    let region_start = mid.min(slot_start + slot_count - region_slots);

    eprintln!(
        "machine: {cores} cores | region: {region_buckets} buckets (~{:.1} GiB of file), \
         sweeping threads = {{1,2,4,6,8}}× cores (4× = {} highlighted)",
        region_slots as f64 * 470_000.0 / (1u64 << 30) as f64,
        cores * 4,
    );

    // Single-core baseline (warm a small chunk, then time it).
    let base_buckets = region_buckets.min(16);
    decode_range(&path, region_start, base_buckets * BUCKET_SLOTS);
    let bstart = Instant::now();
    let base_txs = decode_range(&path, region_start, base_buckets * BUCKET_SLOTS);
    let baseline_tps = base_txs as f64 / bstart.elapsed().as_secs_f64();
    eprintln!("single-core baseline: {:.0} tx/s", baseline_tps);

    // Warm the whole region into cache (and learn its tx count).
    let (_, region_txs) = parallel_decode(&path, region_start, region_buckets, cores);
    eprintln!("warmed region: {region_txs} txs\n");

    // Build the sweep from the multipliers, clamped to region/arg limits.
    let mut sweep: Vec<usize> = multipliers
        .iter()
        .map(|&m| cores * m)
        .filter(|&n| n <= max_threads && (n as u64) <= region_buckets)
        .collect();
    sweep.dedup();

    println!(
        "{:>8}  {:>9}  {:>14}  {:>11}",
        "threads", "wall (s)", "agg tx/s", "core-equiv"
    );
    let mut best = (0usize, 0.0f64);
    let mut at_4x = 0.0f64;
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
        if n == cores * 4 {
            at_4x = agg;
        }
        let mark = if n == cores * 4 { "  <- 4x" } else { "" };
        println!(
            "{:>8}  {:>9.2}  {:>14.0}  {:>10.1}x{mark}",
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
    if at_4x > 0.0 {
        println!(
            "4x cores ({} threads): {:.0} tx/s ({:.1}x single core)",
            cores * 4,
            at_4x,
            at_4x / baseline_tps.max(1.0),
        );
    }
    // Whole-epoch projection at the measured peak.
    let full_epoch_txs = 514_000_000u64;
    println!(
        "=> a full ~514M-tx epoch reads back in ~{:.1} min at peak ({:.0} tx/s)",
        full_epoch_txs as f64 / best.1 / 60.0,
        best.1,
    );
}
