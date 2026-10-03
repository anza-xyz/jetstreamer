//! Read-only verification for the Horizon plugin path.
//!
//! The plugin consumes every decoded record and account-data byte while
//! checking the invariants visible at the plugin boundary. It writes no
//! database rows. `on_finish` fails the run if delivery was incomplete,
//! duplicated, out of order within a worker, or internally inconsistent.

use std::io;
use std::sync::{Arc, Mutex, MutexGuard};

use clickhouse::Client;
use futures_util::FutureExt;
use sha2::{Digest, Sha256};

use jetstreamer_firehose::epochs::epoch_to_slot_range;
use jetstreamer_horizon::account_updates::AccountUpdateMeta;
use jetstreamer_horizon::archive::{BlockNotification, EntryRecord, EpochMeta};
use jetstreamer_horizon::transactions::Transaction;

use crate::PluginFuture;
use crate::horizon::{HorizonPlugin, Output, PluginWorker};

const MAX_RECORDED_ERRORS_PER_WORKER: usize = 8;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct Counts {
    slots: u64,
    blocks: u64,
    skipped: u64,
    entries: u64,
    transactions: u64,
    transaction_updates: u64,
    orphan_updates: u64,
    epoch_updates: u64,
    account_data_bytes: u64,
    slot_sum: u128,
    slot_square_sum: u128,
    slot_xor: u64,
}

impl Counts {
    fn merge(&mut self, other: &Self) {
        self.slots += other.slots;
        self.blocks += other.blocks;
        self.skipped += other.skipped;
        self.entries += other.entries;
        self.transactions += other.transactions;
        self.transaction_updates += other.transaction_updates;
        self.orphan_updates += other.orphan_updates;
        self.epoch_updates += other.epoch_updates;
        self.account_data_bytes += other.account_data_bytes;
        self.slot_sum += other.slot_sum;
        self.slot_square_sum += other.slot_square_sum;
        self.slot_xor ^= other.slot_xor;
    }
}

#[derive(Clone, Debug)]
struct WorkerResult {
    thread_id: usize,
    counts: Counts,
    digest: [u8; 32],
    errors: Vec<String>,
}

#[derive(Clone, Debug, Default)]
struct RunState {
    active: bool,
    epoch: u64,
    start_slot: u64,
    end_slot_exclusive: u64,
    epoch_notifications: u64,
    workers: Vec<WorkerResult>,
}

impl RunState {
    fn for_epoch(epoch: u64) -> Self {
        let (start_slot, end_slot) = epoch_to_slot_range(epoch);
        Self {
            active: true,
            epoch,
            start_slot,
            end_slot_exclusive: end_slot + 1,
            epoch_notifications: 0,
            workers: Vec::new(),
        }
    }
}

/// Aggregated result from one verified epoch.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VerificationReport {
    /// Epoch whose callbacks were verified.
    pub epoch: u64,
    /// Number of parallel plugin workers that participated.
    pub worker_count: usize,
    /// Slot notifications delivered to the plugin.
    pub slots: u64,
    /// Non-skipped block notifications.
    pub blocks: u64,
    /// Leader-skipped slot notifications.
    pub skipped: u64,
    /// PoH entry records attached to blocks.
    pub entries: u64,
    /// Transaction callbacks.
    pub transactions: u64,
    /// Account updates attributed to transactions.
    pub transaction_updates: u64,
    /// Runtime-direct account updates attributed to blocks.
    pub orphan_updates: u64,
    /// Account updates attributed to the epoch boundary.
    pub epoch_updates: u64,
    /// Account-data bytes consumed across every update category.
    pub account_data_bytes: u64,
    /// SHA-256 over the ordered list of per-worker stream digests.
    pub stream_digest: [u8; 32],
}

/// Verifies delivery through [`crate::horizon::HorizonPluginRunner`].
///
/// This is intentionally read-only. It is suitable for release and archive
/// acceptance checks where writing analytics rows would obscure whether the
/// decode and plugin dispatch path itself completed.
#[derive(Clone, Default)]
pub struct VerificationPlugin {
    state: Arc<Mutex<RunState>>,
    last_report: Arc<Mutex<Option<VerificationReport>>>,
}

impl VerificationPlugin {
    /// Creates an idle verifier ready for one or more sequential epoch runs.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the last successfully completed epoch report.
    pub fn last_report(&self) -> Option<VerificationReport> {
        lock_unpoisoned(&self.last_report).clone()
    }
}

impl HorizonPlugin for VerificationPlugin {
    fn name(&self) -> &'static str {
        "Horizon verification"
    }

    fn spawn_worker(&self, thread_id: usize) -> Box<dyn PluginWorker> {
        Box::new(VerificationWorker::new(thread_id, self.state.clone()))
    }

    fn on_start(&self, _db: Arc<Client>, epoch: u64) -> PluginFuture<'_> {
        let state = self.state.clone();
        let last_report = self.last_report.clone();
        async move {
            let mut run = lock_unpoisoned(&state);
            if run.active {
                return Err(io::Error::other(format!(
                    "verification run for epoch {} is still active",
                    run.epoch
                ))
                .into());
            }
            *run = RunState::for_epoch(epoch);
            *lock_unpoisoned(&last_report) = None;
            Ok(())
        }
        .boxed()
    }

    fn on_finish(&self, _db: Arc<Client>, epoch: u64) -> PluginFuture<'_> {
        let state = self.state.clone();
        let last_report = self.last_report.clone();
        async move {
            let mut run = lock_unpoisoned(&state);
            if !run.active || run.epoch != epoch {
                return Err(io::Error::other(format!(
                    "verification lifecycle mismatch: finishing epoch {epoch}, active={}",
                    run.active
                ))
                .into());
            }
            let report = validate_run(&run).map_err(io::Error::other)?;
            run.active = false;
            log::info!(
                target: "jetstreamer_horizon_verification",
                "verified epoch {} through {} workers: slots={} blocks={} skipped={} \
                 entries={} transactions={} tx_updates={} orphan_updates={} \
                 epoch_updates={} account_bytes={} stream_sha256={}",
                report.epoch,
                report.worker_count,
                report.slots,
                report.blocks,
                report.skipped,
                report.entries,
                report.transactions,
                report.transaction_updates,
                report.orphan_updates,
                report.epoch_updates,
                report.account_data_bytes,
                hex(&report.stream_digest),
            );
            *lock_unpoisoned(&last_report) = Some(report);
            Ok(())
        }
        .boxed()
    }
}

struct VerificationWorker {
    thread_id: usize,
    shared: Arc<Mutex<RunState>>,
    epoch: u64,
    start_slot: u64,
    end_slot_exclusive: u64,
    counts: Counts,
    last_block_slot: Option<u64>,
    pending_transaction_slot: Option<u64>,
    pending_transaction_count: u64,
    errors: Vec<String>,
    hasher: Sha256,
}

impl VerificationWorker {
    fn new(thread_id: usize, shared: Arc<Mutex<RunState>>) -> Self {
        let run = lock_unpoisoned(&shared);
        let epoch = run.epoch;
        let start_slot = run.start_slot;
        let end_slot_exclusive = run.end_slot_exclusive;
        let active = run.active;
        drop(run);
        let mut worker = Self {
            thread_id,
            shared,
            epoch,
            start_slot,
            end_slot_exclusive,
            counts: Counts::default(),
            last_block_slot: None,
            pending_transaction_slot: None,
            pending_transaction_count: 0,
            errors: Vec::new(),
            hasher: Sha256::new(),
        };
        if !active {
            worker.record_error("worker spawned outside an active verification run".into());
        }
        worker
    }

    fn record_error(&mut self, message: String) {
        if self.errors.len() < MAX_RECORDED_ERRORS_PER_WORKER {
            self.errors
                .push(format!("worker {}: {message}", self.thread_id));
        }
    }

    fn check_slot(&mut self, slot: u64, context: &str) {
        if !(self.start_slot..self.end_slot_exclusive).contains(&slot) {
            self.record_error(format!(
                "{context} slot {slot} is outside {}..{}",
                self.start_slot, self.end_slot_exclusive
            ));
        }
    }

    fn hash_update(&mut self, meta: &AccountUpdateMeta, data: &[u8]) {
        self.hasher.update(meta.pubkey.as_ref());
        self.hasher.update(meta.lamports.to_le_bytes());
        self.hasher.update(meta.owner.as_ref());
        self.hasher.update([u8::from(meta.executable)]);
        self.hasher.update(meta.rent_epoch.to_le_bytes());
        self.hasher.update(meta.write_version.to_le_bytes());
        self.hasher.update((data.len() as u64).to_le_bytes());
        self.hasher.update(data);
    }
}

impl PluginWorker for VerificationWorker {
    fn on_epoch(&mut self, meta: &EpochMeta) {
        if meta.epoch != self.epoch
            || meta.start_slot != self.start_slot
            || meta.slot_count != self.end_slot_exclusive - self.start_slot
            || !(self.start_slot..self.end_slot_exclusive).contains(&meta.first_block_slot)
        {
            self.record_error(format!(
                "invalid epoch notification: epoch={} start={} count={} first_block={}",
                meta.epoch, meta.start_slot, meta.slot_count, meta.first_block_slot
            ));
        }
        lock_unpoisoned(&self.shared).epoch_notifications += 1;
        self.hasher.update(b"epoch");
        self.hasher.update(meta.epoch.to_le_bytes());
        self.hasher.update(meta.start_slot.to_le_bytes());
        self.hasher.update(meta.slot_count.to_le_bytes());
        self.hasher.update(meta.first_block_slot.to_le_bytes());
        for (update, data) in meta.updates.iter() {
            self.counts.epoch_updates += 1;
            self.counts.account_data_bytes += data.len() as u64;
            self.hash_update(update, data);
        }
    }

    fn on_transaction(&mut self, slot: u64, tx_index: u32, tx: &Transaction) {
        self.check_slot(slot, "transaction");
        match self.pending_transaction_slot {
            None => {
                self.pending_transaction_slot = Some(slot);
                if tx_index != 0 {
                    self.record_error(format!(
                        "first transaction in slot {slot} has index {tx_index}"
                    ));
                }
            }
            Some(current) if current != slot => self.record_error(format!(
                "transaction slot advanced from {current} to {slot} before block notification"
            )),
            Some(_) => {
                if u64::from(tx_index) != self.pending_transaction_count {
                    self.record_error(format!(
                        "slot {slot} transaction index {tx_index} followed count {}",
                        self.pending_transaction_count
                    ));
                }
            }
        }
        self.pending_transaction_count += 1;
        self.counts.transactions += 1;
        self.hasher.update(b"transaction");
        self.hasher.update(slot.to_le_bytes());
        self.hasher.update(tx_index.to_le_bytes());
        self.hasher.update(tx.fee.to_le_bytes());
        for signature in tx.signatures.iter() {
            self.hasher.update(signature.as_ref());
        }
        for (update, data) in tx.iter_account_updates() {
            self.counts.transaction_updates += 1;
            self.counts.account_data_bytes += data.len() as u64;
            self.hash_update(update, data);
        }
    }

    fn on_block(&mut self, notification: &BlockNotification, entries: &[EntryRecord]) {
        let slot = notification.slot();
        self.check_slot(slot, "block");
        if self
            .last_block_slot
            .is_some_and(|previous| slot <= previous)
        {
            self.record_error(format!(
                "block slot {slot} is not greater than prior slot {}",
                self.last_block_slot.unwrap_or_default()
            ));
        }
        self.last_block_slot = Some(slot);
        self.counts.slots += 1;
        self.counts.slot_sum += u128::from(slot);
        self.counts.slot_square_sum += u128::from(slot) * u128::from(slot);
        self.counts.slot_xor ^= slot;
        self.hasher.update(b"slot");
        self.hasher.update(slot.to_le_bytes());

        match notification {
            BlockNotification::Skipped(_) => {
                self.counts.skipped += 1;
                if !entries.is_empty() || self.pending_transaction_count != 0 {
                    self.record_error(format!(
                        "skipped slot {slot} carried {} entries and {} transactions",
                        entries.len(),
                        self.pending_transaction_count
                    ));
                }
            }
            BlockNotification::Block(meta) => {
                self.counts.blocks += 1;
                let entry_transactions: u64 =
                    entries.iter().map(|entry| u64::from(entry.tx_count)).sum();
                if meta.entry_count != entries.len() as u64 {
                    self.record_error(format!(
                        "slot {slot} declares {} entries but delivered {}",
                        meta.entry_count,
                        entries.len()
                    ));
                }
                if meta.executed_transaction_count != self.pending_transaction_count
                    || entry_transactions != self.pending_transaction_count
                {
                    self.record_error(format!(
                        "slot {slot} transaction counts disagree: block={} entries={} callbacks={}",
                        meta.executed_transaction_count,
                        entry_transactions,
                        self.pending_transaction_count
                    ));
                }
                if self.pending_transaction_count != 0
                    && self.pending_transaction_slot != Some(slot)
                {
                    self.record_error(format!(
                        "slot {slot} ended transactions attributed to {:?}",
                        self.pending_transaction_slot
                    ));
                }
                self.hasher.update(meta.parent_slot.to_le_bytes());
                self.hasher.update(meta.parent_blockhash.as_ref());
                self.hasher.update(meta.blockhash.as_ref());
                for entry in entries {
                    self.hasher.update(entry.num_hashes.to_le_bytes());
                    self.hasher.update(entry.tx_count.to_le_bytes());
                }
                self.counts.entries += entries.len() as u64;
                for (update, data) in meta.pre_updates.iter().chain(meta.post_updates.iter()) {
                    self.counts.orphan_updates += 1;
                    self.counts.account_data_bytes += data.len() as u64;
                    self.hash_update(update, data);
                }
            }
        }
        self.pending_transaction_slot = None;
        self.pending_transaction_count = 0;
    }

    fn flush(&mut self, _out: &Output) {
        // Verification state stays worker-local until Drop. Periodic plugin
        // flushes must not reset ordering or digest state.
    }
}

impl Drop for VerificationWorker {
    fn drop(&mut self) {
        if let Some(slot) = self.pending_transaction_slot {
            self.record_error(format!(
                "stream ended with {} transactions pending for slot {slot}",
                self.pending_transaction_count
            ));
        }
        let digest: [u8; 32] = self.hasher.clone().finalize().into();
        lock_unpoisoned(&self.shared).workers.push(WorkerResult {
            thread_id: self.thread_id,
            counts: self.counts.clone(),
            digest,
            errors: std::mem::take(&mut self.errors),
        });
    }
}

fn validate_run(run: &RunState) -> Result<VerificationReport, String> {
    if run.workers.is_empty() {
        return Err(format!(
            "epoch {} spawned no verification workers",
            run.epoch
        ));
    }
    let mut workers = run.workers.clone();
    workers.sort_unstable_by_key(|worker| worker.thread_id);
    let mut counts = Counts::default();
    let mut errors = Vec::new();
    let mut stream = Sha256::new();
    for worker in &workers {
        counts.merge(&worker.counts);
        errors.extend(worker.errors.iter().cloned());
        stream.update((worker.thread_id as u64).to_le_bytes());
        stream.update(worker.digest);
    }

    let mut expected_sum = 0u128;
    let mut expected_square_sum = 0u128;
    let mut expected_xor = 0u64;
    for slot in run.start_slot..run.end_slot_exclusive {
        expected_sum += u128::from(slot);
        expected_square_sum += u128::from(slot) * u128::from(slot);
        expected_xor ^= slot;
    }
    let expected_slots = run.end_slot_exclusive - run.start_slot;
    if counts.slots != expected_slots
        || counts.blocks + counts.skipped != expected_slots
        || counts.slot_sum != expected_sum
        || counts.slot_square_sum != expected_square_sum
        || counts.slot_xor != expected_xor
    {
        errors.push(format!(
            "slot coverage mismatch: delivered={} blocks={} skipped={} sum={} squares={} xor={}; \
             expected={} sum={} squares={} xor={}",
            counts.slots,
            counts.blocks,
            counts.skipped,
            counts.slot_sum,
            counts.slot_square_sum,
            counts.slot_xor,
            expected_slots,
            expected_sum,
            expected_square_sum,
            expected_xor,
        ));
    }
    if run.epoch_notifications != 1 {
        errors.push(format!(
            "expected one epoch notification, received {}",
            run.epoch_notifications
        ));
    }
    if !errors.is_empty() {
        return Err(format!(
            "epoch {} Horizon plugin verification failed: {}",
            run.epoch,
            errors.join("; ")
        ));
    }

    Ok(VerificationReport {
        epoch: run.epoch,
        worker_count: workers.len(),
        slots: counts.slots,
        blocks: counts.blocks,
        skipped: counts.skipped,
        entries: counts.entries,
        transactions: counts.transactions,
        transaction_updates: counts.transaction_updates,
        orphan_updates: counts.orphan_updates,
        epoch_updates: counts.epoch_updates,
        account_data_bytes: counts.account_data_bytes,
        stream_digest: stream.finalize().into(),
    })
}

fn lock_unpoisoned<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(output, "{byte:02x}");
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    fn complete_run() -> RunState {
        let mut run = RunState {
            active: true,
            epoch: 9,
            start_slot: 100,
            end_slot_exclusive: 103,
            epoch_notifications: 1,
            workers: Vec::new(),
        };
        run.workers.push(WorkerResult {
            thread_id: 0,
            counts: Counts {
                slots: 2,
                blocks: 1,
                skipped: 1,
                slot_sum: 201,
                slot_square_sum: 20_201,
                slot_xor: 1,
                ..Counts::default()
            },
            digest: [1; 32],
            errors: Vec::new(),
        });
        run.workers.push(WorkerResult {
            thread_id: 1,
            counts: Counts {
                slots: 1,
                blocks: 1,
                slot_sum: 102,
                slot_square_sum: 10_404,
                slot_xor: 102,
                ..Counts::default()
            },
            digest: [2; 32],
            errors: Vec::new(),
        });
        run
    }

    #[test]
    fn accepts_complete_partitioned_slot_coverage() {
        let report = validate_run(&complete_run()).unwrap();
        assert_eq!(report.epoch, 9);
        assert_eq!(report.worker_count, 2);
        assert_eq!(report.slots, 3);
        assert_eq!(report.blocks, 2);
        assert_eq!(report.skipped, 1);
    }

    #[test]
    fn rejects_duplicated_or_missing_slot_coverage() {
        let mut run = complete_run();
        run.workers[1].counts.slot_sum = 101;
        let error = validate_run(&run).unwrap_err();
        assert!(error.contains("slot coverage mismatch"));
    }

    #[test]
    fn rejects_worker_callback_errors() {
        let mut run = complete_run();
        run.workers[0].errors.push("bad callback".into());
        let error = validate_run(&run).unwrap_err();
        assert!(error.contains("bad callback"));
    }
}
