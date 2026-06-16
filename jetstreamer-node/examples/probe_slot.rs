//! Probe whether a small window of slots can be fully fetched from
//! old-faithful, using the *exact* firehose path the replay uses. For each
//! slot it compares the block-metadata-declared transaction/entry counts
//! against what actually streamed, so a slot the index marks "present" but
//! whose CAR data is incomplete — the `MissingBlockData` failure mode that
//! aborted the epoch-951 run — shows up as INCOMPLETE or NO DATA.
//!
//! This is the diagnostic for "is slot N a genuine old-faithful gap (a re-run
//! will hit the same wall) or a transient blip (a re-run recovers)?" Run it a
//! couple of times: consistently INCOMPLETE/NO DATA for a slot the replay
//! flagged present ⇒ genuine gap; sometimes COMPLETE ⇒ transient.
//!
//! Usage:
//!   cargo run --release -p jetstreamer-node --example probe_slot -- [start_slot] [count]
//! Defaults to slots 411195440..=411195445 (around the slot that failed).

use std::collections::BTreeMap;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};

use crossbeam_channel::unbounded;
use jetstreamer_firehose::epochs::{BASE_URL, slot_to_epoch};
use jetstreamer_firehose::firehose::{GeyserNotifiers, firehose_geyser_with_notifiers};
use reqwest::{Client, Url};
use solana_clock::{Slot, UnixTimestamp};
use solana_entry::entry::EntrySummary;
use solana_geyser_plugin_manager::block_metadata_notifier_interface::BlockMetadataNotifier;
use solana_hash::Hash;
use solana_ledger::entry_notifier_interface::EntryNotifier;
use solana_rpc::transaction_notifier_interface::TransactionNotifier;
use solana_runtime::bank::KeyedRewardsAndNumPartitions;
use solana_signature::Signature;
use solana_transaction::versioned::VersionedTransaction;
use solana_transaction_status::TransactionStatusMeta;

#[derive(Default)]
struct SlotTally {
    /// Transactions actually delivered by the firehose.
    delivered_txs: u64,
    /// Entries actually delivered.
    delivered_entries: u64,
    /// Whether the block-metadata frame arrived (closes the block).
    block_meta_seen: bool,
    /// Counts the block claims it contains (from the block metadata).
    declared_txs: u64,
    declared_entries: u64,
}

/// Shared, thread-safe tally keyed by slot — the firehose notifies from
/// several worker threads.
#[derive(Default)]
struct Probe {
    slots: Mutex<BTreeMap<Slot, SlotTally>>,
}

impl TransactionNotifier for Probe {
    fn notify_transaction(
        &self,
        slot: Slot,
        _tx_index: usize,
        _signature: &Signature,
        _message_hash: &Hash,
        _is_vote: bool,
        _status_meta: &TransactionStatusMeta,
        _transaction: &VersionedTransaction,
    ) {
        self.slots.lock().unwrap().entry(slot).or_default().delivered_txs += 1;
    }
}

impl EntryNotifier for Probe {
    fn notify_entry(
        &self,
        slot: Slot,
        _index: usize,
        _entry: &EntrySummary,
        _starting_transaction_index: usize,
    ) {
        self.slots
            .lock()
            .unwrap()
            .entry(slot)
            .or_default()
            .delivered_entries += 1;
    }
}

impl BlockMetadataNotifier for Probe {
    fn notify_block_metadata(
        &self,
        _parent_slot: u64,
        _parent_blockhash: &str,
        slot: u64,
        _blockhash: &str,
        _rewards: &KeyedRewardsAndNumPartitions,
        _block_time: Option<UnixTimestamp>,
        _block_height: Option<u64>,
        executed_transaction_count: u64,
        entry_count: u64,
    ) {
        if slot == u64::MAX {
            return; // unload sentinel
        }
        let mut g = self.slots.lock().unwrap();
        let t = g.entry(slot).or_default();
        t.block_meta_seen = true;
        t.declared_txs = executed_transaction_count;
        t.declared_entries = entry_count;
    }
}

fn main() {
    env_logger::Builder::from_default_env()
        .filter_level(log::LevelFilter::Info)
        .format_timestamp_millis()
        .init();

    let args: Vec<String> = std::env::args().skip(1).collect();
    let start: u64 = args
        .first()
        .and_then(|v| v.parse().ok())
        .unwrap_or(411_195_440);
    let count: u64 = args.get(1).and_then(|v| v.parse().ok()).unwrap_or(6).max(1);
    let end = start + count; // exclusive
    let epoch = slot_to_epoch(start);
    eprintln!("probing slots {start}..{end} (epoch {epoch}) via firehose from {BASE_URL}\n");

    let rt = Arc::new(tokio::runtime::Runtime::new().expect("tokio runtime"));
    let client = Client::new();
    let index_base_url = Url::parse(BASE_URL).expect("base url");
    let shutdown = Arc::new(AtomicBool::new(false));
    let probe = Arc::new(Probe::default());

    // Drain the confirmed-bank notifications (unused for the probe).
    let (confirmed_tx, confirmed_rx) = unbounded();
    let drain = std::thread::spawn(move || while confirmed_rx.recv().is_ok() {});

    let txn: Arc<dyn TransactionNotifier + Send + Sync> = probe.clone();
    let ent: Arc<dyn EntryNotifier + Send + Sync> = probe.clone();
    let blk: Arc<dyn BlockMetadataNotifier + Send + Sync> = probe.clone();
    let notifiers = GeyserNotifiers {
        transaction_notifier: Some(txn),
        entry_notifier: Some(ent),
        block_metadata_notifier: Some(blk),
    };

    let result = firehose_geyser_with_notifiers(
        rt.clone(),
        start..end,
        notifiers,
        confirmed_tx,
        &index_base_url,
        &client,
        shutdown.clone(),
        async { Ok(()) },
        16,
        true,
        None,
    );
    let _ = drain.join();

    match &result {
        Ok(()) => eprintln!("\nfirehose finished cleanly"),
        Err((err, slot)) => {
            eprintln!("\nfirehose errored at slot {slot}: {err}");
            eprintln!("(an error here is itself evidence the slot's data could not be fetched)");
        }
    }

    println!("\n{:>12}  {:<9}  {:>13}  {:>15}  verdict", "slot", "block-meta", "txs (got/decl)", "entries (got/decl)");
    let g = probe.slots.lock().unwrap();
    let mut complete = 0u64;
    let mut problem = Vec::new();
    for slot in start..end {
        let (meta, txs, entries, verdict) = match g.get(&slot) {
            None => (
                "—".to_string(),
                "0/?".to_string(),
                "0/?".to_string(),
                "NO DATA (leader-skipped, or present-but-missing)".to_string(),
            ),
            Some(t) => {
                let meta = if t.block_meta_seen { "yes" } else { "no" }.to_string();
                let txs = format!("{}/{}", t.delivered_txs, t.declared_txs);
                let entries = format!("{}/{}", t.delivered_entries, t.declared_entries);
                let verdict = if t.block_meta_seen
                    && t.delivered_txs == t.declared_txs
                    && t.delivered_entries == t.declared_entries
                {
                    complete += 1;
                    "COMPLETE".to_string()
                } else if !t.block_meta_seen {
                    "INCOMPLETE (no block-metadata frame)".to_string()
                } else {
                    "INCOMPLETE (fewer txs/entries than declared)".to_string()
                };
                (meta, txs, entries, verdict)
            }
        };
        if !verdict.starts_with("COMPLETE") {
            problem.push(slot);
        }
        println!("{slot:>12}  {meta:<9}  {txs:>13}  {entries:>15}  {verdict}");
    }
    drop(g);

    println!("\n{complete}/{count} slots COMPLETE.");
    if problem.is_empty() {
        println!(
            "All probed slots fetched cleanly — slot {} was likely a transient failure; a re-run should clear it.",
            start + 2
        );
    } else {
        println!(
            "Problem slots: {problem:?}. If a slot the replay flagged present (e.g. 411195442) is \
             consistently INCOMPLETE/NO DATA here while its neighbours are COMPLETE, it is a genuine \
             old-faithful data gap — a re-run will hit the same wall and you need a fallback source \
             for that slot. Run this probe again to confirm it is consistent (genuine) vs intermittent \
             (transient)."
        );
        std::process::exit(1);
    }
}
