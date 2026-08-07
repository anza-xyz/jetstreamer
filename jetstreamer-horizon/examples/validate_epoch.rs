//! Preflight conversion scan for a horizon archive run.
//!
//! Streams every transaction of an epoch (or a slot sub-range) straight
//! from old-faithful — no snapshot, no replay — and runs the same
//! [`jetstreamer_horizon::convert::populate_transaction`] conversion the
//! node's archive recorder uses. Any transaction that would abort a multi-
//! hour replay (a capacity limit in [`jetstreamer_horizon::limits`]
//! exceeded, an unparseable field, …) is reported here in network-bound
//! time instead, together with shape maxima useful for sizing limits.
//!
//! Usage:
//!
//! ```text
//! cargo run --release -p jetstreamer-horizon --example validate_epoch -- \
//!     <epoch> [offset_slots] [slot_count]
//! ```
//!
//! With no range arguments the whole epoch is scanned.

use std::cell::RefCell;
use std::sync::atomic::{AtomicU64, Ordering};

use futures_util::FutureExt;
use jetstreamer_firehose::epochs::epoch_to_slot_range;
use jetstreamer_firehose::firehose::{
    OnBlockFn, OnEntryFn, OnErrorFn, OnRewardFn, OnStatsTrackingFn, TransactionData, firehose,
};
use jetstreamer_horizon::convert::populate_transaction;
use jetstreamer_horizon::transactions::Transaction;

static TXS_SCANNED: AtomicU64 = AtomicU64::new(0);
static FAILURES: AtomicU64 = AtomicU64::new(0);

/// Tracked maxima: (label, observed max, extractor).
static MAX_SIGNATURES: AtomicU64 = AtomicU64::new(0);
static MAX_ACCOUNT_KEYS: AtomicU64 = AtomicU64::new(0);
static MAX_INSTRUCTIONS: AtomicU64 = AtomicU64::new(0);
static MAX_IX_DATA: AtomicU64 = AtomicU64::new(0);
static MAX_IX_ACCOUNTS: AtomicU64 = AtomicU64::new(0);
static MAX_ADDR_LOOKUPS: AtomicU64 = AtomicU64::new(0);
static MAX_LOADED_ADDRESSES: AtomicU64 = AtomicU64::new(0);
static MAX_INNER_IX: AtomicU64 = AtomicU64::new(0);
static MAX_LOG_LINES: AtomicU64 = AtomicU64::new(0);
static MAX_LOG_BYTES: AtomicU64 = AtomicU64::new(0);
static MAX_LOG_LINE_LEN: AtomicU64 = AtomicU64::new(0);
static MAX_TOKEN_BALANCES: AtomicU64 = AtomicU64::new(0);
static MAX_REWARDS: AtomicU64 = AtomicU64::new(0);
static MAX_RETURN_DATA: AtomicU64 = AtomicU64::new(0);

thread_local! {
    static SCRATCH: RefCell<Box<Transaction>> = RefCell::new(Transaction::new_boxed());
}

fn track_max(slot_max: &AtomicU64, value: usize) {
    slot_max.fetch_max(value as u64, Ordering::Relaxed);
}

fn observe(td: &TransactionData) {
    let message = &td.transaction.message;
    let meta = &td.transaction_status_meta;
    track_max(&MAX_SIGNATURES, td.transaction.signatures.len());
    track_max(&MAX_ACCOUNT_KEYS, message.static_account_keys().len());
    track_max(&MAX_INSTRUCTIONS, message.instructions().len());
    for ix in message.instructions() {
        track_max(&MAX_IX_DATA, ix.data.len());
        track_max(&MAX_IX_ACCOUNTS, ix.accounts.len());
    }
    track_max(
        &MAX_ADDR_LOOKUPS,
        message
            .address_table_lookups()
            .map(|l| l.len())
            .unwrap_or(0),
    );
    track_max(
        &MAX_LOADED_ADDRESSES,
        meta.loaded_addresses.writable.len() + meta.loaded_addresses.readonly.len(),
    );
    if let Some(groups) = &meta.inner_instructions {
        track_max(
            &MAX_INNER_IX,
            groups.iter().map(|g| g.instructions.len()).sum::<usize>(),
        );
        for group in groups {
            for inner in &group.instructions {
                track_max(&MAX_IX_DATA, inner.instruction.data.len());
                track_max(&MAX_IX_ACCOUNTS, inner.instruction.accounts.len());
            }
        }
    }
    if let Some(logs) = &meta.log_messages {
        track_max(&MAX_LOG_LINES, logs.len());
        track_max(&MAX_LOG_BYTES, logs.iter().map(String::len).sum::<usize>());
        for line in logs {
            track_max(&MAX_LOG_LINE_LEN, line.len());
        }
    }
    track_max(
        &MAX_TOKEN_BALANCES,
        meta.pre_token_balances
            .as_ref()
            .map(Vec::len)
            .unwrap_or(0)
            .max(meta.post_token_balances.as_ref().map(Vec::len).unwrap_or(0)),
    );
    track_max(
        &MAX_REWARDS,
        meta.rewards.as_ref().map(Vec::len).unwrap_or(0),
    );
    track_max(
        &MAX_RETURN_DATA,
        meta.return_data
            .as_ref()
            .map(|rd| rd.data.len())
            .unwrap_or(0),
    );
}

fn main() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let mut args = std::env::args().skip(1);
    let epoch: u64 = args
        .next()
        .expect("usage: validate_epoch <epoch> [offset_slots] [slot_count]")
        .parse()
        .expect("epoch must be a number");
    let offset: u64 = args.next().map(|v| v.parse().expect("offset")).unwrap_or(0);
    let (epoch_start, epoch_end_inclusive) = epoch_to_slot_range(epoch);
    let default_count = epoch_end_inclusive - epoch_start + 1 - offset;
    let count: u64 = args
        .next()
        .map(|v| v.parse().expect("slot_count"))
        .unwrap_or(default_count);
    let slot_range = (epoch_start + offset)..(epoch_start + offset + count);
    eprintln!(
        "scanning epoch {epoch}, slots {}..{} ({count} slots)",
        slot_range.start, slot_range.end
    );

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");

    let start = std::time::Instant::now();
    runtime
        .block_on(firehose(
            8,
            false,
            false,
            None,
            slot_range,
            None::<OnBlockFn>,
            Some(move |_thread_id: usize, td: TransactionData| {
                async move {
                    TXS_SCANNED.fetch_add(1, Ordering::Relaxed);
                    observe(&td);
                    SCRATCH.with(|scratch| {
                        let mut scratch = scratch.borrow_mut();
                        if let Err(err) = populate_transaction(
                            &mut scratch,
                            &td.transaction,
                            &td.transaction_status_meta,
                        ) {
                            FAILURES.fetch_add(1, Ordering::Relaxed);
                            eprintln!(
                                "CONVERSION FAILURE slot={} index={} sig={}: {err}",
                                td.slot, td.transaction_slot_index, td.signature
                            );
                        }
                    });
                    Ok(())
                }
                .boxed()
            }),
            None::<OnEntryFn>,
            None::<OnRewardFn>,
            None::<OnErrorFn>,
            None::<OnStatsTrackingFn>,
            None,
        ))
        .unwrap_or_else(|(err, slot)| panic!("firehose failed at slot {slot}: {err}"));

    let txs = TXS_SCANNED.load(Ordering::Relaxed);
    let failures = FAILURES.load(Ordering::Relaxed);
    let elapsed = start.elapsed().as_secs_f64();
    println!("\n=== validate_epoch summary ===");
    println!(
        "scanned {txs} transactions in {elapsed:.1}s ({:.0} tx/s)",
        txs as f64 / elapsed.max(0.001)
    );
    println!("conversion failures: {failures}");
    println!("observed shape maxima (vs limits):");
    let report = [
        (
            "signatures",
            &MAX_SIGNATURES,
            jetstreamer_horizon::limits::MAX_TX_SIGS,
        ),
        (
            "static account keys",
            &MAX_ACCOUNT_KEYS,
            jetstreamer_horizon::limits::MAX_TX_ACCOUNTS,
        ),
        (
            "top-level instructions",
            &MAX_INSTRUCTIONS,
            jetstreamer_horizon::limits::MAX_TX_INSTRUCTIONS,
        ),
        (
            "instruction data bytes",
            &MAX_IX_DATA,
            jetstreamer_horizon::limits::MAX_IX_DATA_LEN,
        ),
        (
            "instruction accounts",
            &MAX_IX_ACCOUNTS,
            jetstreamer_horizon::limits::MAX_IX_ACCOUNTS,
        ),
        (
            "address-table lookups",
            &MAX_ADDR_LOOKUPS,
            jetstreamer_horizon::limits::MAX_TX_ADDR_LOOKUPS,
        ),
        (
            "loaded addresses",
            &MAX_LOADED_ADDRESSES,
            jetstreamer_horizon::limits::MAX_TX_ACCOUNTS,
        ),
        (
            "inner instructions",
            &MAX_INNER_IX,
            jetstreamer_horizon::limits::MAX_TX_INNER_IX,
        ),
        (
            "log lines",
            &MAX_LOG_LINES,
            jetstreamer_horizon::limits::MAX_TX_LOG_MSGS,
        ),
        (
            "log bytes (total)",
            &MAX_LOG_BYTES,
            jetstreamer_horizon::limits::MAX_TX_LOG_DATA,
        ),
        (
            "log line length",
            &MAX_LOG_LINE_LEN,
            jetstreamer_horizon::limits::MAX_TX_LOG_DATA,
        ),
        (
            "token balances",
            &MAX_TOKEN_BALANCES,
            jetstreamer_horizon::limits::MAX_TX_TOKEN_BALANCES,
        ),
        (
            "tx rewards",
            &MAX_REWARDS,
            jetstreamer_horizon::limits::MAX_TX_REWARDS,
        ),
        (
            "return data bytes",
            &MAX_RETURN_DATA,
            jetstreamer_horizon::limits::MAX_RETURN_DATA_LEN,
        ),
    ];
    for (label, value, limit) in report {
        let value = value.load(Ordering::Relaxed);
        let pct = value as f64 * 100.0 / limit as f64;
        println!("  {label:24} {value:>9} / {limit:<9} ({pct:.1}%)");
    }
    if failures > 0 {
        println!(
            "\nRESULT: FAIL — {failures} transaction(s) cannot be archived with current limits"
        );
        std::process::exit(1);
    }
    println!("\nRESULT: OK — every scanned transaction converts cleanly");
}
