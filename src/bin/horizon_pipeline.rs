//! Runs horizon-native plugins over `.jet` archives — the `.jet` counterpart
//! to the main `jetstreamer` firehose→plugin CLI. Reads each epoch's archive
//! in parallel (locally or over the network via `rseek`) and persists plugin
//! output to ClickHouse.
//!
//! ```text
//! horizon-pipeline <epoch|start:end> <jet-dir-or-base-url> \
//!     [--threads N] [--clickhouse-dsn URL]
//! ```
//!
//! `<jet-dir-or-base-url>` is a local directory of `epoch-<N>.jet` files, or
//! an `http(s)://` base URL serving them.
use std::sync::Arc;

use jetstreamer_firehose::epochs::epoch_to_slot_range;
use jetstreamer_firehose::firehose_horizon::JetSource;
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
         [--threads N] [--clickhouse-dsn URL]"
    );
    std::process::exit(2);
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
    while let Some(flag) = args.next() {
        match flag.as_str() {
            "--threads" => {
                threads = args
                    .next()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or_else(|| usage());
            }
            "--clickhouse-dsn" => dsn = args.next().unwrap_or_else(|| usage()),
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
