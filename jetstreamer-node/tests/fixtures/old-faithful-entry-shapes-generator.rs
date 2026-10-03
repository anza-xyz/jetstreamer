use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    ops::Range,
    sync::{Arc, Mutex},
};

use futures_util::FutureExt;
use jetstreamer_firehose::{
    SharedError,
    firehose::{
        BlockData, EntryData, OnErrorFn, OnRewardFn, OnStatsTrackingFn, TransactionData,
        firehose,
    },
};
use serde::{Deserialize, Serialize};

const REPORT: &str =
    "/home/sol/.jetstreamer-private/status-audit-20260911-v1/epochs24-100.json";

#[derive(Deserialize)]
struct AuditReport {
    missing_statuses: Vec<MissingStatus>,
}

#[derive(Deserialize)]
struct MissingStatus {
    slot: u64,
    transaction_slot_index: usize,
}

#[derive(Clone, Debug, Default)]
struct SlotShape {
    declared_transaction_count: Option<u64>,
    declared_entry_count: Option<u64>,
    entries: BTreeMap<usize, Range<usize>>,
    source_status_available: BTreeMap<usize, bool>,
}

#[derive(Debug, Serialize)]
struct EntryReport {
    entry_index: usize,
    transaction_start_index: usize,
    transaction_end_exclusive: usize,
    transaction_count: usize,
    missing_indexes: Vec<usize>,
    missing_count: usize,
    observed_count: usize,
    classification: &'static str,
}

#[derive(Debug, Serialize)]
struct SlotReport {
    slot: u64,
    transaction_count: u64,
    entry_count: u64,
    missing_count: usize,
    observed_count: usize,
    affected_entries: Vec<EntryReport>,
    report_matches_car: bool,
}

#[tokio::main]
async fn main() {
    let report: AuditReport = serde_json::from_slice(&fs::read(REPORT).unwrap()).unwrap();
    let mut expected = BTreeMap::<u64, BTreeSet<usize>>::new();
    for missing in report.missing_statuses {
        expected
            .entry(missing.slot)
            .or_default()
            .insert(missing.transaction_slot_index);
    }

    let clusters = [
        13_334_463..13_334_464,
        17_003_522..17_003_523,
        21_322_647..21_322_652,
        21_341_950..21_341_951,
        21_722_819..21_722_820,
        21_813_778..21_813_781,
        21_833_806..21_833_808,
    ];
    let shapes = Arc::new(Mutex::new(BTreeMap::<u64, SlotShape>::new()));

    for cluster in clusters {
        let on_block_shapes = Arc::clone(&shapes);
        let on_block = move |_thread: usize, block: BlockData| {
            let shapes = Arc::clone(&on_block_shapes);
            async move {
                if let BlockData::Block {
                    slot,
                    executed_transaction_count,
                    entry_count,
                    ..
                } = block
                {
                    let mut guard = shapes.lock().unwrap();
                    let shape = guard.entry(slot).or_default();
                    shape.declared_transaction_count = Some(executed_transaction_count);
                    shape.declared_entry_count = Some(entry_count);
                }
                Ok::<(), SharedError>(())
            }
            .boxed()
        };

        let on_tx_shapes = Arc::clone(&shapes);
        let on_tx = move |_thread: usize, transaction: TransactionData| {
            let shapes = Arc::clone(&on_tx_shapes);
            async move {
                shapes
                    .lock()
                    .unwrap()
                    .entry(transaction.slot)
                    .or_default()
                    .source_status_available
                    .insert(
                        transaction.transaction_slot_index,
                        transaction.status_meta_available,
                    );
                Ok::<(), SharedError>(())
            }
            .boxed()
        };

        let on_entry_shapes = Arc::clone(&shapes);
        let on_entry = move |_thread: usize, entry: EntryData| {
            let shapes = Arc::clone(&on_entry_shapes);
            async move {
                shapes
                    .lock()
                    .unwrap()
                    .entry(entry.slot)
                    .or_default()
                    .entries
                    .insert(entry.entry_index, entry.transaction_indexes);
                Ok::<(), SharedError>(())
            }
            .boxed()
        };

        let threads = (cluster.end - cluster.start).max(1);
        firehose(
            threads,
            false,
            false,
            None,
            cluster,
            Some(on_block),
            Some(on_tx),
            Some(on_entry),
            None::<OnRewardFn>,
            None::<OnErrorFn>,
            None::<OnStatsTrackingFn>,
            None,
        )
        .await
        .unwrap();
    }

    let guard = shapes.lock().unwrap();
    let mut output = Vec::new();
    for (slot, expected_missing) in expected {
        let shape = guard.get(&slot).unwrap_or_else(|| panic!("missing slot {slot}"));
        let actual_missing = shape
            .source_status_available
            .iter()
            .filter_map(|(index, available)| (!available).then_some(*index))
            .collect::<BTreeSet<_>>();
        let mut affected_entries = Vec::new();
        for (entry_index, transaction_indexes) in &shape.entries {
            let missing_indexes = transaction_indexes
                .clone()
                .filter(|index| actual_missing.contains(index))
                .collect::<Vec<_>>();
            if missing_indexes.is_empty() {
                continue;
            }
            let transaction_count = transaction_indexes.len();
            let missing_count = missing_indexes.len();
            let observed_count = transaction_count - missing_count;
            affected_entries.push(EntryReport {
                entry_index: *entry_index,
                transaction_start_index: transaction_indexes.start,
                transaction_end_exclusive: transaction_indexes.end,
                transaction_count,
                missing_indexes,
                missing_count,
                observed_count,
                classification: if observed_count == 0 {
                    "whole-entry-missing"
                } else {
                    "mixed"
                },
            });
        }
        output.push(SlotReport {
            slot,
            transaction_count: shape.declared_transaction_count.unwrap(),
            entry_count: shape.declared_entry_count.unwrap(),
            missing_count: actual_missing.len(),
            observed_count: shape.source_status_available.len() - actual_missing.len(),
            affected_entries,
            report_matches_car: expected_missing == actual_missing,
        });
    }
    println!("{}", serde_json::to_string_pretty(&output).unwrap());
}
