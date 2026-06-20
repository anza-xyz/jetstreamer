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
    let mut dsn = DEFAULT_DSN.to_string();
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

    let mut runner = HorizonPluginRunner::new(dsn, threads);
    runner.add_plugin(Arc::new(PubkeyStatsHorizonPlugin::new()));
    runner.add_plugin(Arc::new(AccountWritesPlugin::new()));

    for epoch in start_epoch..=end_epoch {
        let (lo, hi) = epoch_to_slot_range(epoch);
        log::info!("horizon pipeline: epoch {epoch} (slots {lo}..={hi}) with {threads} threads");
        if let Err(err) = runner.run(src.clone(), epoch, lo..hi + 1).await {
            eprintln!("error: epoch {epoch}: {err}");
            std::process::exit(1);
        }
        log::info!("horizon pipeline: epoch {epoch} complete");
    }
}
