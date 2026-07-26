//! Runs horizon-native plugins over `.jet` archives — the `.jet` counterpart
//! to the main `jetstreamer` firehose→plugin CLI. Reads each epoch's archive
//! in parallel (locally or over the network via `rseek`) and persists plugin
//! output to ClickHouse.
//!
//! ```text
//! horizon-pipeline <epoch|start:end> <jet-dir-or-base-url> \
//!     [--threads N] [--clickhouse-dsn URL] [--bench]
//! ```
//!
//! `<jet-dir-or-base-url>` is a local directory of `epoch-<N>.jet` files, or
//! an `http(s)://` base URL serving them. `--bench` swaps the real plugins for
//! a counting-only plugin — the full framework path (dispatch, workers, flush
//! cycle, ClickHouse spawned and connected as usual) with no data written —
//! and reports decode+dispatch throughput.
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use jetstreamer_firehose::epochs::epoch_to_slot_range;
use jetstreamer_firehose::firehose_horizon::JetSource;
use jetstreamer_horizon::archive::{BlockNotification, EntryRecord};
use jetstreamer_horizon::transactions::Transaction;
use jetstreamer_plugin::horizon::{
    Consumption, HorizonPlugin, HorizonPluginRunner, Output, PluginWorker,
};
use jetstreamer_plugin::plugins::account_writes::AccountWritesPlugin;
use jetstreamer_plugin::plugins::pubkey_stats_horizon::PubkeyStatsHorizonPlugin;

// jemalloc, for the same reason as jetstreamer-node: the decode path churns
// huge short-lived allocations across many threads, where glibc malloc costs
// kernel time (mmap/munmap per allocation) that grows with thread count.
#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

const DEFAULT_DSN: &str = "http://localhost:8123";

/// Same policy as the main runner: the embedded ClickHouse helper is spawned
/// only when the DSN points at this machine.
fn should_spawn_for_dsn(dsn: &str) -> bool {
    let lower = dsn.to_ascii_lowercase();
    lower.contains("localhost") || lower.contains("127.0.0.1")
}

fn usage() -> ! {
    eprintln!(
        "usage: horizon-pipeline <epoch|start:end> <jet-dir-or-base-url> \
         [--threads N] [--clickhouse-dsn URL] [--bench] [--no-account-bytes] [--no-preload]\n\
         --no-account-bytes (with --bench): declare account-update data \
         unconsumed so the decoder skips diff reconstruction — benchmarks \
         the metadata-only fast path (update counts stay real, bytes read 0)\n\
         --no-preload (with --bench): read from disk even when the epoch \
         file would fit in RAM (by default the bench preloads it and \
         measures pure decode)"
    );
    std::process::exit(2);
}

/// Epoch-scoped totals the bench workers drain into at each flush.
#[derive(Default)]
struct BenchCounters {
    slots: AtomicU64,
    blocks: AtomicU64,
    txs: AtomicU64,
    tx_updates: AtomicU64,
    orphan_updates: AtomicU64,
    update_bytes: AtomicU64,
}

/// `--bench` worker: counts locally on the hot path (no shared-state
/// contention) and drains into the shared totals on the framework's normal
/// flush cadence. Touches every account update's bytes so the measured rate
/// is the full decode + dispatch path; writes nothing.
#[derive(Default)]
struct BenchWorker {
    counters: Arc<BenchCounters>,
    slots: u64,
    blocks: u64,
    txs: u64,
    tx_updates: u64,
    orphan_updates: u64,
    update_bytes: u64,
}

impl PluginWorker for BenchWorker {
    fn on_transaction(&mut self, _slot: u64, _tx_index: u32, tx: &Transaction) {
        self.txs += 1;
        for (_meta, data) in tx.iter_account_updates() {
            self.tx_updates += 1;
            self.update_bytes += data.len() as u64;
        }
    }

    fn on_block(&mut self, notification: &BlockNotification, _entries: &[EntryRecord]) {
        self.slots += 1;
        if let BlockNotification::Block(meta) = notification {
            self.blocks += 1;
            for (_m, d) in meta.pre_updates.iter().chain(meta.post_updates.iter()) {
                self.orphan_updates += 1;
                self.update_bytes += d.len() as u64;
            }
        }
    }

    fn flush(&mut self, _out: &Output) {
        let c = &self.counters;
        c.slots
            .fetch_add(std::mem::take(&mut self.slots), Ordering::Relaxed);
        c.blocks
            .fetch_add(std::mem::take(&mut self.blocks), Ordering::Relaxed);
        c.txs
            .fetch_add(std::mem::take(&mut self.txs), Ordering::Relaxed);
        c.tx_updates
            .fetch_add(std::mem::take(&mut self.tx_updates), Ordering::Relaxed);
        c.orphan_updates
            .fetch_add(std::mem::take(&mut self.orphan_updates), Ordering::Relaxed);
        c.update_bytes
            .fetch_add(std::mem::take(&mut self.update_bytes), Ordering::Relaxed);
    }
}

/// The counting-only plugin `--bench` swaps in for the real ones. Runs through
/// the standard `HorizonPluginRunner` (ClickHouse connected as usual) but
/// creates no tables and submits no inserts.
struct BenchPlugin {
    counters: Arc<BenchCounters>,
    /// With `--no-account-bytes` this is false: the plugin declares it does
    /// not consume account-update data, so the decoder skips the diff
    /// reconstruction — benchmarking the metadata-only fast path. Update
    /// COUNTS stay real (metas still decode); `update_bytes` reads 0.
    consume_account_bytes: bool,
}

impl HorizonPlugin for BenchPlugin {
    fn name(&self) -> &'static str {
        "TPS Bench"
    }

    fn spawn_worker(&self, _thread_id: usize) -> Box<dyn PluginWorker> {
        Box::new(BenchWorker {
            counters: self.counters.clone(),
            ..Default::default()
        })
    }

    fn consumption(&self) -> Consumption {
        if self.consume_account_bytes {
            Consumption::all()
        } else {
            Consumption::all().without_account_update_data()
        }
    }
}

/// `MemAvailable` from `/proc/meminfo` (Linux; `None` elsewhere) — the
/// kernel's estimate of memory allocatable without swapping.
fn mem_available_bytes() -> Option<u64> {
    let meminfo = std::fs::read_to_string("/proc/meminfo").ok()?;
    for line in meminfo.lines() {
        if let Some(rest) = line.strip_prefix("MemAvailable:") {
            let kb: u64 = rest.trim().trim_end_matches("kB").trim().parse().ok()?;
            return Some(kb * 1024);
        }
    }
    None
}

/// Headroom the preload leaves for decode scratches, the page cache, and
/// everything else on the box; the file is only preloaded when
/// `MemAvailable` covers its size plus this.
const PRELOAD_HEADROOM_BYTES: u64 = 48 * 1024 * 1024 * 1024;

/// Bench-only: if `epoch`'s `.jet` fits in RAM (with headroom), read it
/// fully into memory and return an in-memory source, so the measured rate
/// is pure decode with zero disk I/O. Returns `None` (with a log line
/// saying why) when it doesn't apply; the caller falls back to `src`.
async fn preload_epoch(src: &JetSource, epoch: u64) -> Option<JetSource> {
    let JetSource::LocalDir(dir) = src else {
        return None; // preloading only makes sense for local files
    };
    let path = dir.join(format!("epoch-{epoch}.jet"));
    let file_len = std::fs::metadata(&path).ok()?.len();
    let Some(available) = mem_available_bytes() else {
        log::info!("bench: no MemAvailable (non-Linux?); reading from disk");
        return None;
    };
    if available < file_len + PRELOAD_HEADROOM_BYTES {
        log::info!(
            "bench: epoch {epoch} not preloaded ({} file vs {} available); reading from disk",
            commas(file_len),
            commas(available)
        );
        return None;
    }
    log::info!(
        "bench: preloading epoch {epoch} into RAM ({} bytes)...",
        commas(file_len)
    );
    let start = Instant::now();
    let bytes = tokio::task::spawn_blocking(move || std::fs::read(&path))
        .await
        .ok()?
        .ok()?;
    let secs = start.elapsed().as_secs_f64();
    log::info!(
        "bench: preloaded epoch {epoch} in {secs:.1}s ({:.2} GB/s); decode will be disk-free",
        (bytes.len() as f64 / secs) / 1e9
    );
    Some(JetSource::in_memory(epoch, bytes))
}

/// Thousands separators for benchmark readability.
fn commas(n: u64) -> String {
    let s = n.to_string();
    let mut out = String::with_capacity(s.len() + s.len() / 3);
    for (i, c) in s.chars().enumerate() {
        if i > 0 && (s.len() - i).is_multiple_of(3) {
            out.push(',');
        }
        out.push(c);
    }
    out
}

fn parse_range(s: &str) -> (u64, u64) {
    let parse = |v: &str| v.parse::<u64>().unwrap_or_else(|_| usage());
    match s.split_once(':') {
        Some((a, b)) => (parse(a), parse(b)),
        None => {
            let e = parse(s);
            (e, e)
        }
    }
}

#[tokio::main]
async fn main() {
    solana_logger::setup_with_default("info");

    let mut args = std::env::args().skip(1);
    let range = args.next().unwrap_or_else(|| usage());
    let location = args.next().unwrap_or_else(|| usage());
    // Default: cores + 1/8 oversubscription. Decode scales with threads until
    // every core is saturated and then holds flat (measured on a 64-core
    // EPYC: throughput climbs to ~72 workers and stays within ~2% out to
    // 128), and the slight oversubscription covers per-thread stalls —
    // 72 threads was the measured optimum on that box.
    let mut threads = std::thread::available_parallelism()
        .map(|n| n.get() + n.get().div_ceil(8))
        .unwrap_or(8);
    let mut dsn =
        std::env::var("JETSTREAMER_CLICKHOUSE_DSN").unwrap_or_else(|_| DEFAULT_DSN.to_string());
    let mut bench = false;
    let mut no_account_bytes = false;
    let mut no_preload = false;
    while let Some(flag) = args.next() {
        match flag.as_str() {
            "--threads" => {
                threads = args
                    .next()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or_else(|| usage());
            }
            "--clickhouse-dsn" => dsn = args.next().unwrap_or_else(|| usage()),
            "--bench" => bench = true,
            "--no-account-bytes" => no_account_bytes = true,
            "--no-preload" => no_preload = true,
            _ => usage(),
        }
    }
    if no_account_bytes && !bench {
        // The real plugins declare their own consumption; forcing bytes off
        // under them would silently zero account_write_stats.
        eprintln!("--no-account-bytes only applies to --bench");
        usage();
    }
    if no_preload && !bench {
        eprintln!("--no-preload only applies to --bench (preloading is bench-only)");
        usage();
    }

    // First CTRL+C aborts the epoch loop (the embedded ClickHouse still gets
    // its graceful stop below); a second force-exits.
    let shutdown = Arc::new(tokio::sync::Notify::new());
    {
        let shutdown = shutdown.clone();
        tokio::spawn(async move {
            let mut first = true;
            while tokio::signal::ctrl_c().await.is_ok() {
                if first {
                    first = false;
                    eprintln!("CTRL+C received, shutting down... (press again to force-exit)");
                    shutdown.notify_one();
                } else {
                    eprintln!("CTRL+C received again, force-exiting");
                    std::process::exit(130);
                }
            }
        });
    }

    let (start_epoch, end_epoch) = parse_range(&range);
    let src = if location.starts_with("http://") || location.starts_with("https://") {
        JetSource::http(&location).unwrap_or_else(|err| {
            eprintln!("error: {err}");
            std::process::exit(1);
        })
    } else {
        JetSource::local(&location)
    };

    // Local DSN → spawn the embedded ClickHouse helper (binary unpacked into
    // bin/, same as the main jetstreamer runner) and wait until it's ready.
    // Bench mode keeps this: the point is the full framework path with the
    // connection present, just no data written to it.
    let spawn_clickhouse = should_spawn_for_dsn(&dsn);
    let mut clickhouse_task = None;
    if spawn_clickhouse {
        let (mut ready_rx, clickhouse_future) =
            jetstreamer_utils::start().await.unwrap_or_else(|err| {
                eprintln!("error: failed to start embedded clickhouse: {err}");
                std::process::exit(1);
            });
        if ready_rx.recv().await.is_none() {
            eprintln!("error: clickhouse readiness channel closed unexpectedly");
            std::process::exit(1);
        }
        clickhouse_task = Some(tokio::spawn(async move {
            match clickhouse_future.await {
                Ok(()) => log::info!("ClickHouse process exited gracefully."),
                Err(()) => log::error!("ClickHouse process exited with an error."),
            }
        }));
    } else {
        log::info!("using external ClickHouse at {dsn} (no embedded spawn)");
    }

    let mut runner = HorizonPluginRunner::new(dsn, threads);
    let bench_counters = Arc::new(BenchCounters::default());
    if bench {
        runner.add_plugin(Arc::new(BenchPlugin {
            counters: bench_counters.clone(),
            consume_account_bytes: !no_account_bytes,
        }));
    } else {
        runner.add_plugin(Arc::new(PubkeyStatsHorizonPlugin::new()));
        runner.add_plugin(Arc::new(AccountWritesPlugin::new()));
    }

    let mut failure = None;
    for epoch in start_epoch..=end_epoch {
        let (lo, hi) = epoch_to_slot_range(epoch);
        log::info!("horizon pipeline: epoch {epoch} (slots {lo}..={hi}) with {threads} threads");
        // Bench: preload the epoch into RAM when it fits, so the timed
        // window below measures pure decode. The buffer is dropped when the
        // epoch's run completes, before the next preload.
        let epoch_src = if bench && !no_preload {
            preload_epoch(&src, epoch)
                .await
                .unwrap_or_else(|| src.clone())
        } else {
            src.clone()
        };
        let before = (
            bench_counters.slots.load(Ordering::Relaxed),
            bench_counters.blocks.load(Ordering::Relaxed),
            bench_counters.txs.load(Ordering::Relaxed),
            bench_counters.tx_updates.load(Ordering::Relaxed),
            bench_counters.orphan_updates.load(Ordering::Relaxed),
            bench_counters.update_bytes.load(Ordering::Relaxed),
        );
        let start = Instant::now();
        tokio::select! {
            res = runner.run(epoch_src, epoch, lo..hi + 1) => {
                if let Err(err) = res {
                    failure = Some(format!("epoch {epoch}: {err}"));
                    break;
                }
            }
            _ = shutdown.notified() => {
                failure = Some(format!("interrupted by CTRL+C during epoch {epoch}"));
                break;
            }
        }
        if bench {
            let elapsed = start.elapsed().as_secs_f64();
            let slots = bench_counters.slots.load(Ordering::Relaxed) - before.0;
            let blocks = bench_counters.blocks.load(Ordering::Relaxed) - before.1;
            let txs = bench_counters.txs.load(Ordering::Relaxed) - before.2;
            let tx_updates = bench_counters.tx_updates.load(Ordering::Relaxed) - before.3;
            let orphan_updates = bench_counters.orphan_updates.load(Ordering::Relaxed) - before.4;
            let update_bytes = bench_counters.update_bytes.load(Ordering::Relaxed) - before.5;
            let rate = |n: u64| commas((n as f64 / elapsed) as u64);
            log::info!(
                "bench: epoch {epoch} done in {elapsed:.1}s — slots={} blocks={} txs={} \
                 tx_updates={} orphan_updates={} update_bytes={}",
                commas(slots),
                commas(blocks),
                commas(txs),
                commas(tx_updates),
                commas(orphan_updates),
                commas(update_bytes),
            );
            log::info!(
                "bench: epoch {epoch} rates — slots/s={} TPS={} updates/s={} update_MB/s={:.1}",
                rate(slots),
                rate(txs),
                rate(tx_updates + orphan_updates),
                (update_bytes as f64 / elapsed) / 1_000_000.0,
            );
        } else {
            log::info!("horizon pipeline: epoch {epoch} complete");
        }
    }

    // Stop the embedded server before exiting either way, so data is flushed
    // and the port is released.
    if spawn_clickhouse {
        jetstreamer_utils::stop().await;
        if let Some(task) = clickhouse_task {
            let _ = task.await;
        }
    }
    if let Some(err) = failure {
        eprintln!("error: {err}");
        std::process::exit(1);
    }
}
