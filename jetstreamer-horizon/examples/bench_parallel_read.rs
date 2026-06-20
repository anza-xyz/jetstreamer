//! Parallel read-throughput benchmark for a horizon archive — maps the
//! throughput-vs-threads curve from low thread counts up, then reports the
//! plateau and its midpoint.
//!
//! The archive is bucket-parallel: every 128-slot bucket is a self-contained
//! zstd frame, so disjoint slot sub-ranges decode independently. Aggregate
//! decode throughput rises as threads are added, reaches a broad plateau,
//! then falls once contention (memory bandwidth, per-blob allocation)
//! dominates. The peak is machine-specific and is often *below* the core
//! count for cache-hot decode, so the sweep starts low.
//!
//! Each thread decodes a fixed, small number of buckets (so low-N probes are
//! cheap and the sweep can start at one thread); aggregate throughput is
//! `total_txs / wall`. The sweep brackets the curve, a median of repeated
//! runs suppresses noise, and the plateau (thread counts within a few % of
//! peak) is reported with its midpoint as the recommended thread count.
//!
//! Zero-config. Usage:
//! `cargo run --release -p jetstreamer-horizon --example bench_parallel_read -- <path> [runs] [buckets_per_thread]`

use std::io::BufReader;
use std::time::Instant;

use jetstreamer_horizon::archive::{ArchiveReader, BlockNotification, EntryRecord, SlotVisitor};
use jetstreamer_horizon::transactions::Transaction;

const BUCKET_SLOTS: u64 = 128;
/// A thread count is "on the plateau" if its median throughput is within this
/// fraction of the best median.
const PLATEAU_BAND: f64 = 0.95;

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

/// Runs `n` threads, each decoding its own `per_thread` buckets from a
/// distinct offset (thread `t` takes buckets `[t*per_thread, ..)`). Fixed
/// per-thread work, so the wall time is roughly constant across `n` until
/// contention sets in — total throughput = total_txs / wall. Returns
/// (wall_seconds, total_txs).
fn measure(path: &str, region_start: u64, n: usize, per_thread: u64) -> (f64, u64) {
    let start = Instant::now();
    let totals: Vec<u64> = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..n as u64)
            .map(|t| {
                let path = path.to_string();
                let sub_start = region_start + t * per_thread * BUCKET_SLOTS;
                let sub_count = per_thread * BUCKET_SLOTS;
                scope.spawn(move || decode_range(&path, sub_start, sub_count))
            })
            .collect();
        handles
            .into_iter()
            .map(|h| h.join().expect("thread"))
            .collect()
    });
    (start.elapsed().as_secs_f64(), totals.iter().sum())
}

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[v.len() / 2]
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let path = args
        .first()
        .cloned()
        .expect("usage: bench_parallel_read <path> [runs] [buckets_per_thread]");
    let runs: usize = args.get(1).and_then(|v| v.parse().ok()).unwrap_or(3).max(1);
    let per_thread: u64 = args.get(2).and_then(|v| v.parse().ok()).unwrap_or(2).max(1);
    let cores = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(8);

    let (slot_start, slot_count) = {
        let file = std::fs::File::open(&path).expect("open");
        let reader = ArchiveReader::open(BufReader::new(file)).expect("open archive for header");
        let h = reader.header();
        (h.slot_start, h.slot_count)
    };
    let total_buckets = slot_count / BUCKET_SLOTS;

    // Upper bound on threads we might probe (hard cap so the region and
    // memory stay bounded); the adaptive sweep usually stops well before it.
    // Capped at 192 regardless of core count: high-core servers peak *below*
    // their core count, and even low-core machines peak by a few× cores, so
    // there is no reason to materialise a region for thousands of threads.
    let max_threads = (cores * 8).clamp(8, 192);
    // Region holds one distinct chunk per thread at the hard cap.
    let region_buckets = ((max_threads as u64) * per_thread).min(total_buckets);
    let max_threads = (region_buckets / per_thread) as usize; // re-clamp to region
    let region_slots = region_buckets * BUCKET_SLOTS;
    let mid = slot_start + (slot_count / 2 / BUCKET_SLOTS) * BUCKET_SLOTS;
    let region_start = mid.min(slot_start + slot_count - region_slots);

    eprintln!(
        "machine: {cores} cores | region: {region_buckets} buckets (~{:.1} GiB) | \
         {per_thread} buckets/thread, {runs} runs/point (median) | adaptive sweep, cap {max_threads} threads",
        region_slots as f64 * 470_000.0 / (1u64 << 30) as f64,
    );

    // Single-core baseline (warm, then time `per_thread` buckets on one thread).
    decode_range(&path, region_start, per_thread * BUCKET_SLOTS);
    let bstart = Instant::now();
    let base_txs = decode_range(&path, region_start, per_thread * BUCKET_SLOTS);
    let baseline_tps = base_txs as f64 / bstart.elapsed().as_secs_f64();
    eprintln!("single-core baseline: {baseline_tps:.0} tx/s\n");

    println!(
        "{:>8}  {:>7}  {:>13}  {:>11}  {:>17}",
        "threads", "x cores", "median tx/s", "core-equiv", "spread (min..max)"
    );

    // Adaptive geometric sweep from 1 thread up: keep climbing while
    // throughput is still improving, and stop once it has clearly rolled
    // over (two consecutive points below 90% of the best seen). This finds
    // the peak whether it sits below the core count (big NUMA servers) or
    // well above it (unified-memory machines) without a fixed ceiling.
    let mut results: Vec<(usize, f64)> = Vec::new();
    let mut best_tps = 0.0f64;
    let mut best_n = 1usize;
    let mut declines = 0;
    let mut n = 1usize;
    loop {
        // One warm run (drop, populates page cache) then `runs` measured.
        measure(&path, region_start, n, per_thread);
        let samples: Vec<f64> = (0..runs)
            .map(|_| {
                let (wall, txs) = measure(&path, region_start, n, per_thread);
                txs as f64 / wall
            })
            .collect();
        let lo = samples.iter().cloned().fold(f64::INFINITY, f64::min);
        let hi = samples.iter().cloned().fold(0.0, f64::max);
        let med = median(samples);
        results.push((n, med));
        println!(
            "{:>8}  {:>6.2}x  {:>13.0}  {:>10.1}x  {:>8.2}M..{:.2}M",
            n,
            n as f64 / cores as f64,
            med,
            med / baseline_tps.max(1.0),
            lo / 1e6,
            hi / 1e6,
        );

        if med > best_tps {
            best_tps = med;
            best_n = n;
        }
        declines = if med < best_tps * 0.90 {
            declines + 1
        } else {
            0
        };
        // Stop once throughput has clearly rolled over past the peak (two
        // consecutive points below 90% of the best seen — robust to a single
        // noisy dip, since a recovery resets the counter), OR once we are
        // well past the peak thread count and into a flat sub-peak tail (so a
        // gently-declining plateau doesn't drag the sweep to the cap).
        let rolled_over = declines >= 2;
        let flat_tail = n >= best_n * 2 && med < best_tps * PLATEAU_BAND;
        let next = ((n as f64 * 1.4).ceil() as usize).min(max_threads);
        if rolled_over || flat_tail || n >= max_threads || results.iter().any(|&(m, _)| m == next) {
            break;
        }
        n = next;
    }
    // Did the sweep observe the roll-over, or did it stop at the cap still
    // climbing? (The latter means the true peak may be higher.)
    let capped_while_climbing = results.last().unwrap().1 >= best_tps * 0.97;

    // Peak, then walk outward to the contiguous plateau (>= PLATEAU_BAND of peak).
    let peak_idx = results
        .iter()
        .enumerate()
        .max_by(|a, b| a.1.1.partial_cmp(&b.1.1).unwrap())
        .unwrap()
        .0;
    let peak_tps = results[peak_idx].1;
    let threshold = peak_tps * PLATEAU_BAND;
    let mut lo_i = peak_idx;
    while lo_i > 0 && results[lo_i - 1].1 >= threshold {
        lo_i -= 1;
    }
    let mut hi_i = peak_idx;
    while hi_i + 1 < results.len() && results[hi_i + 1].1 >= threshold {
        hi_i += 1;
    }
    let (plat_lo, plat_hi) = (results[lo_i].0, results[hi_i].0);
    // Geometric midpoint of the plateau — the robust recommendation, away
    // from the noisy edges.
    let mid_threads = ((plat_lo as f64 * plat_hi as f64).sqrt().round() as usize).max(1);

    println!(
        "\npeak: {peak_tps:.0} tx/s (~{:.1}x single core, {:.0}% of {cores} cores)",
        peak_tps / baseline_tps.max(1.0),
        peak_tps / baseline_tps.max(1.0) / cores as f64 * 100.0,
    );
    println!(
        "plateau (within {:.0}% of peak): {plat_lo}..{plat_hi} threads ({:.2}x..{:.2}x cores)",
        (1.0 - PLATEAU_BAND) * 100.0,
        plat_lo as f64 / cores as f64,
        plat_hi as f64 / cores as f64,
    );
    if capped_while_climbing && plat_hi == *results.iter().map(|(n, _)| n).max().unwrap() {
        println!(
            "note: throughput was still climbing at the {plat_hi}-thread cap — the true peak may be \
             higher; re-run with a larger cap (raise buckets/thread or pass more threads)."
        );
    }
    println!(
        "recommended: {mid_threads} threads (~{:.2}x cores) — midpoint of the plateau",
        mid_threads as f64 / cores as f64,
    );
    println!(
        "=> a full ~514M-tx epoch reads back in ~{:.1} min at peak",
        514_000_000.0 / peak_tps / 60.0,
    );
}
