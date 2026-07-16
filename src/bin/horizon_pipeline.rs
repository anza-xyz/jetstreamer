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
//! an `http(s)://` base URL serving them. `--bench` skips plugins and
//! ClickHouse entirely and just measures decode throughput.
use std::sync::Arc;
use std::time::Instant;

use jetstreamer_firehose::epochs::epoch_to_slot_range;
use jetstreamer_firehose::firehose_horizon::{JetSource, firehose_horizon};
use jetstreamer_horizon::archive::{BlockNotification, EntryRecord, SlotVisitor};
use jetstreamer_horizon::transactions::Transaction;
use jetstreamer_plugin::horizon::HorizonPluginRunner;
use jetstreamer_plugin::plugins::account_writes::AccountWritesPlugin;
use jetstreamer_plugin::plugins::pubkey_stats_horizon::PubkeyStatsHorizonPlugin;

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
         [--threads N] [--clickhouse-dsn URL] [--bench]"
    );
    std::process::exit(2);
}

/// `--bench` visitor: touches every decoded record (including each account
/// update's bytes) and counts, so the measured rate is the full zero-copy
/// decode + access path with no sink attached.
#[derive(Default)]
struct BenchVisitor {
    slots: u64,
    blocks: u64,
    txs: u64,
    tx_updates: u64,
    orphan_updates: u64,
    update_bytes: u64,
}

impl SlotVisitor for BenchVisitor {
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

/// Reads epochs with a counting visitor only — no plugins, no ClickHouse —
/// and reports decode throughput.
async fn run_bench(src: JetSource, start_epoch: u64, end_epoch: u64, threads: usize) {
    for epoch in start_epoch..=end_epoch {
        let (lo, hi) = epoch_to_slot_range(epoch);
        log::info!("bench: epoch {epoch} (slots {lo}..={hi}) with {threads} threads");
        let start = Instant::now();
        let visitors = match firehose_horizon(threads, src.clone(), epoch, lo..hi + 1, |_| {
            BenchVisitor::default()
        })
        .await
        {
            Ok(v) => v,
            Err(err) => {
                eprintln!("error: epoch {epoch}: {err}");
                std::process::exit(1);
            }
        };
        let elapsed = start.elapsed().as_secs_f64();
        let mut total = BenchVisitor::default();
        for v in visitors {
            total.slots += v.slots;
            total.blocks += v.blocks;
            total.txs += v.txs;
            total.tx_updates += v.tx_updates;
            total.orphan_updates += v.orphan_updates;
            total.update_bytes += v.update_bytes;
        }
        let rate = |n: u64| commas((n as f64 / elapsed) as u64);
        log::info!(
            "bench: epoch {epoch} done in {elapsed:.1}s — slots={} blocks={} txs={} \
             tx_updates={} orphan_updates={} update_bytes={}",
            commas(total.slots),
            commas(total.blocks),
            commas(total.txs),
            commas(total.tx_updates),
            commas(total.orphan_updates),
            commas(total.update_bytes),
        );
        log::info!(
            "bench: epoch {epoch} rates — slots/s={} txs/s={} updates/s={} update_MB/s={:.1}",
            rate(total.slots),
            rate(total.txs),
            rate(total.tx_updates + total.orphan_updates),
            (total.update_bytes as f64 / elapsed) / 1_000_000.0,
        );
    }
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
    let mut threads = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(8);
    let mut dsn = std::env::var("JETSTREAMER_CLICKHOUSE_DSN")
        .unwrap_or_else(|_| DEFAULT_DSN.to_string());
    let mut bench = false;
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
            _ => usage(),
        }
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

    if bench {
        run_bench(src, start_epoch, end_epoch, threads).await;
        return;
    }

    // Local DSN → spawn the embedded ClickHouse helper (binary unpacked into
    // bin/, same as the main jetstreamer runner) and wait until it's ready.
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
    runner.add_plugin(Arc::new(PubkeyStatsHorizonPlugin::new()));
    runner.add_plugin(Arc::new(AccountWritesPlugin::new()));

    let mut failure = None;
    for epoch in start_epoch..=end_epoch {
        let (lo, hi) = epoch_to_slot_range(epoch);
        log::info!("horizon pipeline: epoch {epoch} (slots {lo}..={hi}) with {threads} threads");
        if let Err(err) = runner.run(src.clone(), epoch, lo..hi + 1).await {
            failure = Some(format!("epoch {epoch}: {err}"));
            break;
        }
        log::info!("horizon pipeline: epoch {epoch} complete");
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
