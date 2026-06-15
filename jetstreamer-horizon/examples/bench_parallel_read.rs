//! Parallel read-throughput benchmark for a horizon archive — maps the
//! throughput-vs-threads curve and reports the plateau.
//!
//! The archive is bucket-parallel: every 128-slot bucket is a self-contained
//! zstd frame with its own encoder reset, so disjoint slot sub-ranges decode
//! independently. Aggregate decode throughput rises as threads are added,
//! then flattens into a broad plateau and slowly falls under heavy
//! oversubscription (contention: memory bandwidth, per-blob allocation). The
//! plateau is wide and a little noisy, so there is no single "optimal" thread
//! count to converge on — this benchmark instead sweeps fixed multiples of
//! the core count, takes a median of repeated runs to suppress noise, and
//! reports the plateau range plus a recommended thread count (the smallest
//! that reaches peak throughput, for the least memory/overhead).
//!
//! Zero-config. Usage:
//! `cargo run --release -p jetstreamer-horizon --example bench_parallel_read -- <path> [runs] [region_buckets]`

use std::io::BufReader;
use std::time::Instant;

use jetstreamer_horizon::archive::{ArchiveReader, BlockNotification, EntryRecord, SlotVisitor};
use jetstreamer_horizon::transactions::Transaction;

const BUCKET_SLOTS: u64 = 128;
/// Thread counts swept, as multiples of the core count (dense around 1×,
/// where the peak sits, with a couple of oversubscribed points to show the
/// falloff).
const MULTIPLIERS: &[f64] = &[0.5, 0.75, 1.0, 1.25, 1.5, 2.0, 3.0];
/// A point counts as "on the plateau" if its median is within this fraction
/// of the best median.
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

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[v.len() / 2]
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let path = args
        .first()
        .cloned()
        .expect("usage: bench_parallel_read <path> [runs] [region_buckets]");
    let runs: usize = args.get(1).and_then(|v| v.parse().ok()).unwrap_or(3).max(1);
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

    // Distinct thread counts from the multipliers.
    let mut thread_counts: Vec<usize> = MULTIPLIERS
        .iter()
        .map(|m| ((m * cores as f64).round() as usize).max(1))
        .collect();
    thread_counts.dedup();
    let max_threads = *thread_counts.iter().max().unwrap();

    // Region: at least one bucket per thread at the top of the sweep, sized
    // to stay resident in the page cache.
    let region_buckets = args
        .get(2)
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or((max_threads as u64).max(256))
        .min(total_buckets);
    thread_counts.retain(|&n| (n as u64) <= region_buckets);
    let region_slots = region_buckets * BUCKET_SLOTS;
    let mid = slot_start + (slot_count / 2 / BUCKET_SLOTS) * BUCKET_SLOTS;
    let region_start = mid.min(slot_start + slot_count - region_slots);

    eprintln!(
        "machine: {cores} cores | region: {region_buckets} buckets (~{:.1} GiB of file) | \
         {runs} runs/point, median reported",
        region_slots as f64 * 470_000.0 / (1u64 << 30) as f64,
    );

    // Single-core baseline (warm, then time).
    let base_buckets = region_buckets.min(16);
    decode_range(&path, region_start, base_buckets * BUCKET_SLOTS);
    let bstart = Instant::now();
    let base_txs = decode_range(&path, region_start, base_buckets * BUCKET_SLOTS);
    let baseline_tps = base_txs as f64 / bstart.elapsed().as_secs_f64();
    eprintln!("single-core baseline: {baseline_tps:.0} tx/s");

    // Warm the whole region into cache and learn its tx count.
    let (_, region_txs) = parallel_decode(&path, region_start, region_buckets, cores);
    eprintln!("warmed region: {region_txs} txs\n");

    println!(
        "{:>8}  {:>7}  {:>13}  {:>11}  {:>17}",
        "threads", "x cores", "median tx/s", "core-equiv", "spread (min..max)"
    );
    let mut results: Vec<(usize, f64)> = Vec::new();
    for &n in &thread_counts {
        let samples: Vec<f64> = (0..runs)
            .map(|_| {
                let (wall, txs) = parallel_decode(&path, region_start, region_buckets, n);
                assert_eq!(
                    txs, region_txs,
                    "parallel decode lost/duplicated transactions"
                );
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
    }

    // Peak (best median) and the plateau (points within PLATEAU_BAND of it).
    let (peak_n, peak_tps) = results
        .iter()
        .cloned()
        .max_by(|a, b| a.1.partial_cmp(&b.1).unwrap())
        .unwrap();
    let plateau: Vec<usize> = results
        .iter()
        .filter(|(_, tps)| *tps >= peak_tps * PLATEAU_BAND)
        .map(|(n, _)| *n)
        .collect();
    let (plat_lo, plat_hi) = (
        *plateau.iter().min().unwrap(),
        *plateau.iter().max().unwrap(),
    );

    println!(
        "\npeak: {peak_tps:.0} tx/s (~{:.1}x single core, {:.0}% of {cores} cores) around {peak_n} threads",
        peak_tps / baseline_tps.max(1.0),
        peak_tps / baseline_tps.max(1.0) / cores as f64 * 100.0,
    );
    println!(
        "plateau (within {:.0}% of peak): {plat_lo}..{plat_hi} threads ({:.2}x..{:.2}x cores)",
        (1.0 - PLATEAU_BAND) * 100.0,
        plat_lo as f64 / cores as f64,
        plat_hi as f64 / cores as f64,
    );
    println!(
        "recommended: {plat_lo} threads (~{:.2}x cores) — smallest that reaches peak throughput",
        plat_lo as f64 / cores as f64,
    );
    println!(
        "=> a full ~514M-tx epoch reads back in ~{:.1} min at peak",
        514_000_000.0 / peak_tps / 60.0,
    );
}
