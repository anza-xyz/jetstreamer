//! Parallel read-throughput benchmark for a horizon archive — auto-finds the
//! optimal thread count.
//!
//! The archive is bucket-parallel: every 128-slot bucket is a self-contained
//! zstd frame with its own encoder reset, so disjoint slot sub-ranges decode
//! independently. Aggregate decode throughput vs thread count is unimodal —
//! it rises as threads are added, peaks, then falls once contention (memory
//! bandwidth, per-blob allocation) outweighs the parallelism. The peak is
//! machine-specific (high-core servers tend to peak near 1× cores; latency-
//! bound setups peak under oversubscription), so this benchmark **searches**
//! for it with golden-section search (the unimodal-max analog of binary
//! search) rather than assuming a fixed multiple.
//!
//! Zero-config: detects the core count, sizes and warms a cache-resident
//! working region, then probes thread counts to converge on the throughput
//! peak. Each probe decodes the whole region (strong scaling), so the search
//! naturally avoids the slow low-thread end unless the peak is there.
//!
//! Usage:
//! `cargo run --release -p jetstreamer-horizon --example bench_parallel_read -- <path> [region_buckets] [max_threads]`

use std::collections::HashMap;
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

struct Search<'a> {
    path: &'a str,
    region_start: u64,
    region_buckets: u64,
    region_txs: u64,
    baseline_tps: f64,
    cache: HashMap<usize, f64>,
    best: (usize, f64),
}

impl Search<'_> {
    /// Aggregate tx/s at `n` threads (memoised); prints each fresh probe.
    fn eval(&mut self, n: usize) -> f64 {
        if let Some(&tps) = self.cache.get(&n) {
            return tps;
        }
        let (wall, txs) = parallel_decode(self.path, self.region_start, self.region_buckets, n);
        assert_eq!(
            txs, self.region_txs,
            "parallel decode lost/duplicated transactions"
        );
        let tps = txs as f64 / wall;
        println!(
            "  probe {n:>4} threads: {tps:>12.0} tx/s  ({:>5.1}x single core, {wall:.2}s)",
            tps / self.baseline_tps.max(1.0),
        );
        self.cache.insert(n, tps);
        if tps > self.best.1 {
            self.best = (n, tps);
        }
        tps
    }
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let path = args
        .first()
        .cloned()
        .expect("usage: bench_parallel_read <path> [region_buckets] [max_threads]");
    let region_buckets_arg: Option<u64> = args.get(1).and_then(|v| v.parse().ok());
    let max_threads_arg: Option<usize> = args.get(2).and_then(|v| v.parse().ok());
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

    // Search bounds: from well below the core count (some machines peak
    // sub-core) up to 6× (covers oversubscription-favoring setups).
    let lo = (cores / 2).max(2);
    let hi = max_threads_arg
        .unwrap_or(cores * 6)
        .min(total_buckets as usize)
        .max(lo + 1);
    // Region: one bucket per thread at the top of the search, plus headroom.
    let region_buckets = region_buckets_arg
        .unwrap_or((hi as u64).max(256))
        .min(total_buckets);
    let region_slots = region_buckets * BUCKET_SLOTS;
    let mid = slot_start + (slot_count / 2 / BUCKET_SLOTS) * BUCKET_SLOTS;
    let region_start = mid.min(slot_start + slot_count - region_slots);

    eprintln!(
        "machine: {cores} cores | region: {region_buckets} buckets (~{:.1} GiB of file) | \
         searching thread counts in [{lo}, {hi}]",
        region_slots as f64 * 470_000.0 / (1u64 << 30) as f64,
    );

    // Single-core baseline (warm a small chunk, then time it).
    let base_buckets = region_buckets.min(16);
    decode_range(&path, region_start, base_buckets * BUCKET_SLOTS);
    let bstart = Instant::now();
    let base_txs = decode_range(&path, region_start, base_buckets * BUCKET_SLOTS);
    let baseline_tps = base_txs as f64 / bstart.elapsed().as_secs_f64();
    eprintln!("single-core baseline: {baseline_tps:.0} tx/s");

    // Warm the whole region into cache and learn its tx count.
    let (_, region_txs) = parallel_decode(&path, region_start, region_buckets, cores);
    eprintln!("warmed region: {region_txs} txs\n");

    let mut search = Search {
        path: &path,
        region_start,
        region_buckets,
        region_txs,
        baseline_tps,
        cache: HashMap::new(),
        best: (0, 0.0),
    };

    // Golden-section search for the throughput peak over [lo, hi]. Each step
    // reuses one interior point, so it converges in ~log_phi(range) probes;
    // the global best across all probes is reported (robust to the broad,
    // slightly-noisy plateau near the peak).
    println!("searching for the optimal thread count (golden-section):");
    let invphi: f64 = 0.618_033_988_75;
    let tol = (cores / 8).max(4);
    let (mut a, mut b) = (lo, hi);
    let span = |a: usize, b: usize| ((b - a) as f64 * invphi).round() as usize;
    let mut c = (b - span(a, b)).clamp(a, b);
    let mut d = (a + span(a, b)).clamp(a, b);
    let mut fc = search.eval(c);
    let mut fd = search.eval(d);
    while b - a > tol {
        if fc < fd {
            a = c;
            c = d;
            fc = fd;
            d = (a + span(a, b)).clamp(a, b);
            if d == c {
                break;
            }
            fd = search.eval(d);
        } else {
            b = d;
            d = c;
            fd = fc;
            c = (b - span(a, b)).clamp(a, b);
            if c == d {
                break;
            }
            fc = search.eval(c);
        }
    }

    let (best_n, best_tps) = search.best;
    println!(
        "\npeak: {best_tps:.0} tx/s at {best_n} threads ({:.1}x a single core, on {cores} cores, \
         {:.0}% scaling efficiency)",
        best_tps / baseline_tps.max(1.0),
        best_tps / baseline_tps.max(1.0) / cores as f64 * 100.0,
    );
    println!(
        "=> a full ~514M-tx epoch reads back in ~{:.1} min at this peak",
        514_000_000.0 / best_tps / 60.0,
    );
}
