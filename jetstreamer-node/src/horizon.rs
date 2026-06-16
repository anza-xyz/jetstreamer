//! Horizon archive recorder.
//!
//! Assembles the two data streams the node produces into per-slot horizon
//! archive frames and streams them to an
//! [`ArchiveWriter`](jetstreamer_horizon::archive::ArchiveWriter):
//!
//! - **Input side (firehose thread)**: the original chain's block metadata
//!   (rewards, blockhashes, block time/height) via
//!   [`record_block_meta`](HorizonRecorder::record_block_meta). Transactions
//!   and entries also originate here but travel through the
//!   `TransactionScheduler` so they reach the recorder on the replay thread
//!   in execution order, paired with their original `TransactionStatusMeta`.
//! - **Replay side (ready-entry thread)**: geyser account updates in true
//!   bank execution order via
//!   [`record_account_update`](HorizonRecorder::record_account_update)
//!   (transaction-owned when the store carried a `txn`, orphan otherwise),
//!   and committed entries with their transactions via
//!   [`record_committed_entry`](HorizonRecorder::record_committed_entry).
//!
//! # Slot lifecycle
//!
//! A slot's frame can only close once its bank froze (the freeze emits the
//! end-of-slot "post" orphan updates: fee distribution, incinerator, …),
//! and the bank for slot `S` freezes when the bank for the next replayed
//! slot is created. Both recorder entry points therefore double as the
//! advance signal: any activity tagged with slot `S` proves every earlier
//! slot is complete, so buffered assemblies `< S` are emitted. Stream
//! order also guarantees the input-side block metadata for those slots
//! arrived before any later slot's entries, so emission never has to wait.
//!
//! Frames are written in canonical wire order (epoch meta → pre-orphans →
//! transactions → post-orphans → block meta → entries); gap slots between
//! consecutive blocks are emitted as leader-skipped frames after checking
//! the slot-presence map (a gap that old-faithful says *should* have a
//! block means the archive would be silently incomplete — fatal).
//!
//! Integrity violations panic: the archive is the product of the run, and
//! the node's replay machinery already treats divergence as fatal.

use std::collections::{BTreeMap, HashMap};
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Seek, SeekFrom};
use std::path::Path;
use std::str::FromStr;
use std::sync::{Arc, Mutex, RwLock};

use jetstreamer_horizon::account_updates::AccountUpdateView;
use jetstreamer_horizon::archive::{
    ArchiveCheckpoint, ArchiveStats, ArchiveWriter, ArchiveWriterConfig, BlockMeta, EntryRecord,
    EpochMeta,
};
use jetstreamer_horizon::convert;
use jetstreamer_horizon::transactions::Transaction;
use log::info;
use solana_account::{AccountSharedData, ReadableAccount};
use solana_address::Address;
use solana_clock::Slot;
use solana_hash::Hash;
use solana_runtime::bank::KeyedRewardsAndNumPartitions;
use solana_signature::Signature;
use solana_transaction::versioned::VersionedTransaction;
use solana_transaction_status::TransactionStatusMeta;

use crate::{SlotPresenceMap, SlotPresenceState};

/// The active archive recorder. Swappable so one process can record an entire
/// range of epochs back to back, writing an independent `.jet` per epoch: each
/// epoch installs a fresh recorder via [`init`] and tears it down via
/// [`finish`]. The holder is only written at epoch boundaries (when replay is
/// paused), so reads on the account-update hot path are effectively
/// uncontended.
static RECORDER: RwLock<Option<Arc<HorizonRecorder>>> = RwLock::new(None);

// Diagnostic counters for the per-account-update hot path. The recorder
// mutex is taken once per account write (~30k/s) from whatever thread is
// committing the batch — i.e. concurrently from the parallel execution
// workers — so if `held`/`wait` approach replay wall-time, the recorder is
// serializing execution at its own mutex rather than execution being the
// true floor.
static RECORDER_HELD_US: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static RECORDER_WAIT_US: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// (mutex-held µs, mutex-wait µs) accumulated across the account-update hot
/// path since process start. Read by the progress logger.
pub fn recorder_contention_us() -> (u64, u64) {
    use std::sync::atomic::Ordering;
    (
        RECORDER_HELD_US.load(Ordering::Relaxed),
        RECORDER_WAIT_US.load(Ordering::Relaxed),
    )
}

/// Lock-free mirror of the archive's running on-disk size, updated as
/// buckets flush. Zero until recording is enabled and the first bucket
/// flushes.
static ARCHIVE_BYTES_WRITTEN: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Current horizon archive size in bytes (0 if recording is disabled).
/// Lock-free — safe to call from the progress thread.
pub fn archive_bytes_written() -> u64 {
    ARCHIVE_BYTES_WRITTEN.load(std::sync::atomic::Ordering::Relaxed)
}

/// Installs the active recorder for one epoch's archive. Call before that
/// epoch's replay starts; pair with [`finish`] when it ends. Errors if a
/// recorder is still installed (the previous epoch must be finished first).
pub fn init(
    path: &Path,
    epoch: u64,
    slot_start: Slot,
    slot_count: u64,
    presence: Arc<SlotPresenceMap>,
) -> Result<(), String> {
    let recorder = HorizonRecorder::create(path, epoch, slot_start, slot_count, presence)?;
    let mut slot = RECORDER
        .write()
        .map_err(|_| "horizon recorder lock poisoned".to_string())?;
    if slot.is_some() {
        return Err("horizon recorder already initialized".to_string());
    }
    *slot = Some(Arc::new(recorder));
    Ok(())
}

/// Finalizes and removes the active recorder (writes the footer + bucket
/// index), returning its stats. No-op returning `Ok(None)` when recording is
/// disabled. Clears the slot so the next epoch's [`init`] can install a fresh
/// archive.
pub fn finish() -> Result<Option<ArchiveStats>, String> {
    let recorder = RECORDER
        .write()
        .map_err(|_| "horizon recorder lock poisoned".to_string())?
        .take();
    match recorder {
        Some(recorder) => recorder.finish().map(Some),
        None => Ok(None),
    }
}

/// Installs a recorder that resumes appending to an existing archive from a
/// crash checkpoint, instead of creating a fresh one (cf. [`init`]). The header
/// is not rewritten; recording continues from `checkpoint.last_emitted + 1`.
pub fn resume_init(
    path: &Path,
    epoch: u64,
    slot_start: Slot,
    slot_count: u64,
    presence: Arc<SlotPresenceMap>,
    checkpoint: RecorderCheckpoint,
) -> Result<(), String> {
    let recorder =
        HorizonRecorder::resume(path, epoch, slot_start, slot_count, presence, checkpoint)?;
    let mut slot = RECORDER
        .write()
        .map_err(|_| "horizon recorder lock poisoned".to_string())?;
    if slot.is_some() {
        return Err("horizon recorder already initialized".to_string());
    }
    *slot = Some(Arc::new(recorder));
    Ok(())
}

/// Checkpoints the active recorder at `resume_slot`: flushes everything through
/// `resume_slot - 1` into a durable, fsync'd archive prefix and returns the
/// state needed to resume after a crash. The recorder stays installed and keeps
/// recording. Returns `Ok(None)` when recording is disabled.
pub fn checkpoint(resume_slot: Slot) -> Result<Option<RecorderCheckpoint>, String> {
    let guard = RECORDER
        .read()
        .map_err(|_| "horizon recorder lock poisoned".to_string())?;
    match guard.as_ref() {
        Some(recorder) => recorder.checkpoint(resume_slot).map(Some),
        None => Ok(None),
    }
}

/// Returns the active recorder when archive output is enabled. The returned
/// handle keeps the recorder alive for the duration of the call even if a
/// concurrent [`finish`] runs (which it never does during replay).
#[inline]
pub fn recorder() -> Option<Arc<HorizonRecorder>> {
    RECORDER.read().ok()?.clone()
}

/// One account update, owned (the geyser notification's buffers are only
/// borrowed for the duration of the callback).
struct OwnedAccountUpdate {
    pubkey: Address,
    lamports: u64,
    owner: Address,
    executable: bool,
    rent_epoch: u64,
    write_version: u64,
    data: Vec<u8>,
}

impl OwnedAccountUpdate {
    /// Copies a live geyser notification into an owned update (the only
    /// allocation/copy on the account-update path — the `data` memcpy).
    fn capture(
        pubkey: &solana_pubkey::Pubkey,
        account: &AccountSharedData,
        write_version: u64,
    ) -> Self {
        Self {
            pubkey: Address::new_from_array(pubkey.to_bytes()),
            lamports: account.lamports(),
            owner: Address::new_from_array(account.owner().to_bytes()),
            executable: account.executable(),
            rent_epoch: account.rent_epoch(),
            write_version,
            data: account.data().to_vec(),
        }
    }

    fn as_view(&self) -> AccountUpdateView<'_> {
        AccountUpdateView {
            pubkey: self.pubkey,
            lamports: self.lamports,
            owner: self.owner,
            executable: self.executable,
            rent_epoch: self.rent_epoch,
            write_version: self.write_version,
            data: &self.data,
        }
    }
}

/// A captured account update awaiting merge into the recorder, tagged with
/// its slot and owning-transaction signature (`None` for orphan writes).
pub struct CapturedUpdate {
    slot: Slot,
    signature: Option<Signature>,
    update: OwnedAccountUpdate,
}

thread_local! {
    /// Per-thread capture buffer. When `Some`, account-update notifications
    /// land here instead of taking the recorder mutex; the same thread that
    /// enabled capture drains it (see [`begin_capture`] / [`take_captured`]).
    /// Each batch executes on a single thread, so the captured updates are
    /// perfectly ordered with no cross-thread merge.
    static CAPTURE: std::cell::RefCell<Option<Vec<CapturedUpdate>>> =
        const { std::cell::RefCell::new(None) };
}

/// Enables capture on the current thread (call before executing a batch).
pub fn begin_capture() {
    CAPTURE.with(|c| *c.borrow_mut() = Some(Vec::new()));
}

/// Disables capture on the current thread and returns what was collected.
pub fn take_captured() -> Vec<CapturedUpdate> {
    CAPTURE.with(|c| c.borrow_mut().take().unwrap_or_default())
}

/// Records one geyser account update. On a capturing thread (inside batch
/// execution) it pushes lock-free into the thread-local buffer; otherwise
/// (runtime-direct orphan writes on the coordinator) it falls back to the
/// recorder's locking path. No-op when recording is disabled.
pub fn note_account_update(
    slot: Slot,
    pubkey: &solana_pubkey::Pubkey,
    account: &AccountSharedData,
    signature: Option<Signature>,
    write_version: u64,
) {
    let Some(recorder) = recorder() else {
        return;
    };
    let update = OwnedAccountUpdate::capture(pubkey, account, write_version);
    let captured = CAPTURE.with(|c| {
        if let Some(buf) = c.borrow_mut().as_mut() {
            buf.push(CapturedUpdate {
                slot,
                signature,
                update,
            });
            None
        } else {
            Some(update)
        }
    });
    // Not on a capturing thread → orphan write; record directly.
    if let Some(update) = captured {
        recorder.record_owned_update(slot, signature, update);
    }
}

/// A committed transaction paired with its original chain metadata.
struct CommittedTx {
    tx: VersionedTransaction,
    meta: TransactionStatusMeta,
}

/// Everything buffered for one in-flight slot.
#[derive(Default)]
struct SlotAssembly {
    pre_orphans: Vec<OwnedAccountUpdate>,
    post_orphans: Vec<OwnedAccountUpdate>,
    /// Updates keyed by the owning transaction's first signature.
    tx_updates: HashMap<Signature, Vec<OwnedAccountUpdate>>,
    /// Committed transactions in slot-index order.
    txs: Vec<CommittedTx>,
    /// Entry records in entry-index order (ticks included).
    entries: Vec<EntryRecord>,
}

/// Original chain block metadata buffered from the input side.
struct BufferedBlockMeta {
    parent_slot: Slot,
    parent_blockhash: Hash,
    blockhash: Hash,
    rewards: Vec<(Address, solana_reward_info::RewardInfo)>,
    num_partitions: Option<u64>,
    block_time: Option<i64>,
    block_height: Option<u64>,
    executed_transaction_count: u64,
    entry_count: u64,
}

struct RecorderState {
    writer: ArchiveWriter<BufWriter<File>>,
    epoch: u64,
    slot_start: Slot,
    slot_end_inclusive: Slot,
    assemblies: BTreeMap<Slot, SlotAssembly>,
    block_metas: BTreeMap<Slot, BufferedBlockMeta>,
    last_emitted: Option<Slot>,
    epoch_meta_written: bool,
    // Reusable encode scratches (large; allocated once on the heap).
    tx_scratch: Box<Transaction>,
    meta_scratch: Box<BlockMeta>,
    epoch_scratch: Box<EpochMeta>,
    presence: std::sync::Arc<SlotPresenceMap>,
    finished: bool,
}

/// Everything needed to resume appending to an archive after a crash: the
/// writer-level [`ArchiveCheckpoint`] plus the recorder's slot-tracking state.
/// The owning run persists this (alongside the bank snapshot and resume slot)
/// in the checkpoint manifest.
pub struct RecorderCheckpoint {
    pub archive: ArchiveCheckpoint,
    /// Last slot emitted into the durable prefix (`resume_slot - 1`).
    pub last_emitted: Slot,
    /// Whether the epoch-meta frame was already written (rides the first block).
    pub epoch_meta_written: bool,
}

pub struct HorizonRecorder {
    state: Mutex<RecorderState>,
}

impl HorizonRecorder {
    /// Opens the archive file and builds a recorder for `slot_start ..
    /// slot_start + slot_count`.
    fn create(
        path: &Path,
        epoch: u64,
        slot_start: Slot,
        slot_count: u64,
        presence: std::sync::Arc<SlotPresenceMap>,
    ) -> Result<Self, String> {
        let file = File::create(path)
            .map_err(|err| format!("failed to create horizon archive {}: {err}", path.display()))?;
        let writer = ArchiveWriter::new(
            BufWriter::with_capacity(8 << 20, file),
            epoch,
            slot_start,
            slot_count,
            ArchiveWriterConfig::default(),
        )
        .map_err(|err| format!("failed to initialize horizon archive writer: {err}"))?;
        Ok(HorizonRecorder {
            state: Mutex::new(RecorderState {
                writer,
                epoch,
                slot_start,
                slot_end_inclusive: slot_start + slot_count - 1,
                assemblies: BTreeMap::new(),
                block_metas: BTreeMap::new(),
                last_emitted: None,
                epoch_meta_written: false,
                tx_scratch: Transaction::new_boxed(),
                meta_scratch: BlockMeta::new_boxed(),
                epoch_scratch: EpochMeta::new_boxed(),
                presence,
                finished: false,
            }),
        })
    }

    /// Resumes appending to an existing archive file from a [`RecorderCheckpoint`].
    /// The file is truncated back to the durable prefix (dropping any partial
    /// bucket from the crashed run) and reopened for appending; the header is
    /// not rewritten. `slot_start`/`slot_count` are the *whole-epoch* archive
    /// coordinates (the archive remains one file for the epoch).
    fn resume(
        path: &Path,
        epoch: u64,
        slot_start: Slot,
        slot_count: u64,
        presence: std::sync::Arc<SlotPresenceMap>,
        checkpoint: RecorderCheckpoint,
    ) -> Result<Self, String> {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .map_err(|err| {
                format!(
                    "failed to open horizon archive {} for resume: {err}",
                    path.display()
                )
            })?;
        file.set_len(checkpoint.archive.file_offset).map_err(|err| {
            format!("failed to truncate horizon archive to checkpoint prefix: {err}")
        })?;
        file.seek(SeekFrom::End(0))
            .map_err(|err| format!("failed to seek to end of horizon archive: {err}"))?;
        let writer = ArchiveWriter::resume(
            BufWriter::with_capacity(8 << 20, file),
            slot_start,
            slot_count,
            ArchiveWriterConfig::default(),
            checkpoint.archive,
        );
        Ok(HorizonRecorder {
            state: Mutex::new(RecorderState {
                writer,
                epoch,
                slot_start,
                slot_end_inclusive: slot_start + slot_count - 1,
                assemblies: BTreeMap::new(),
                block_metas: BTreeMap::new(),
                last_emitted: Some(checkpoint.last_emitted),
                epoch_meta_written: checkpoint.epoch_meta_written,
                tx_scratch: Transaction::new_boxed(),
                meta_scratch: BlockMeta::new_boxed(),
                epoch_scratch: EpochMeta::new_boxed(),
                presence,
                finished: false,
            }),
        })
    }

    /// Flushes everything through `resume_slot - 1` (so the archive prefix is
    /// complete up to there), checkpoints + fsyncs the writer, and returns the
    /// state needed to resume after a crash. The recorder stays installed and
    /// keeps recording from `resume_slot` onward.
    ///
    /// The caller must freeze the bank at `resume_slot - 1` first (so the last
    /// slot's post-transaction orphan updates are recorded) and must have
    /// quiesced the replay (no data for slots >= `resume_slot` buffered).
    pub fn checkpoint(&self, resume_slot: Slot) -> Result<RecorderCheckpoint, String> {
        let mut state = self.lock();
        if state.finished {
            return Err("horizon recorder already finished".to_string());
        }
        // Emit all complete assemblies for slots < resume_slot.
        state.emit_complete_below(resume_slot);
        // Fill any trailing leader-skipped slots up to resume_slot - 1.
        let next = state
            .last_emitted
            .map(|s| s + 1)
            .unwrap_or(state.slot_start);
        for slot in next..resume_slot {
            state.check_gap_slot_skipped(slot);
            state.writer.write_skipped_slot(slot).map_err(|err| {
                format!("horizon: failed to write skipped slot {slot} at checkpoint: {err}")
            })?;
        }
        // Anything still buffered means the replay was not quiesced — refuse
        // rather than checkpoint an inconsistent prefix.
        if let Some((&slot, _)) = state.assemblies.first_key_value() {
            return Err(format!(
                "horizon: cannot checkpoint at slot {resume_slot}: slot {slot} still buffered \
                 (replay not quiesced)"
            ));
        }
        if let Some((&slot, _)) = state.block_metas.first_key_value() {
            return Err(format!(
                "horizon: cannot checkpoint at slot {resume_slot}: block metadata for slot {slot} \
                 was never emitted"
            ));
        }
        // The durable prefix now covers [slot_start, resume_slot - 1].
        state.last_emitted = Some(resume_slot - 1);
        let archive = state
            .writer
            .checkpoint()
            .map_err(|err| format!("horizon: writer checkpoint failed: {err}"))?;
        state
            .writer
            .sink_ref()
            .get_ref()
            .sync_all()
            .map_err(|err| format!("horizon: failed to fsync archive at checkpoint: {err}"))?;
        Ok(RecorderCheckpoint {
            archive,
            last_emitted: resume_slot - 1,
            epoch_meta_written: state.epoch_meta_written,
        })
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, RecorderState> {
        self.state.lock().expect("horizon recorder lock poisoned")
    }

    /// Logs writer progress every `LOG_PROGRESS_EVERY_N_SLOTS` slots.
    /// Called from the block-metadata notifier alongside the existing
    /// periodic replay log.
    pub fn maybe_log_progress(&self, slot: Slot) {
        const LOG_PROGRESS_EVERY_N_SLOTS: u64 = 100;
        if !slot.is_multiple_of(LOG_PROGRESS_EVERY_N_SLOTS) {
            return;
        }
        let state = self.lock();
        let stats = *state.writer.stats();
        let emitted = state.last_emitted;
        drop(state);
        let ratio = if stats.uncompressed_payload_bytes > 0 {
            format!(
                "{:.1}%",
                stats.bytes_written as f64 * 100.0 / stats.uncompressed_payload_bytes as f64
            )
        } else {
            "n/a".to_string()
        };
        info!(
            target: "jetstreamer_node_horizon",
            "horizon progress: emitted_slot={} slots={} blocks={} txs={} tx_updates={} orphan_updates={} written={} ({} of raw payload)",
            emitted.map(|s| s.to_string()).unwrap_or_else(|| "none".to_string()),
            stats.slots,
            stats.blocks,
            stats.transactions,
            stats.account_updates,
            stats.orphan_account_updates,
            stats.bytes_written,
            ratio,
        );
    }

    /// Buffers the original chain's block metadata (firehose thread).
    /// Idempotent per slot — firehose restarts may re-deliver.
    #[allow(clippy::too_many_arguments)] // mirrors the geyser notification's field set
    pub fn record_block_meta(
        &self,
        slot: Slot,
        parent_slot: Slot,
        parent_blockhash: &str,
        blockhash: &str,
        rewards: &KeyedRewardsAndNumPartitions,
        block_time: Option<i64>,
        block_height: Option<u64>,
        executed_transaction_count: u64,
        entry_count: u64,
    ) {
        let parent_blockhash = Hash::from_str(parent_blockhash).unwrap_or_else(|err| {
            panic!("horizon: unparseable parent blockhash for slot {slot}: {err}")
        });
        let blockhash = Hash::from_str(blockhash)
            .unwrap_or_else(|err| panic!("horizon: unparseable blockhash for slot {slot}: {err}"));
        let meta = BufferedBlockMeta {
            parent_slot,
            parent_blockhash,
            blockhash,
            rewards: rewards
                .keyed_rewards
                .iter()
                .map(|(pubkey, info)| (Address::new_from_array(pubkey.to_bytes()), *info))
                .collect(),
            num_partitions: rewards.num_partitions,
            block_time,
            block_height,
            executed_transaction_count,
            entry_count,
        };
        let mut state = self.lock();
        if state.finished {
            return;
        }
        // Re-delivery after a firehose restart replaces the buffered copy;
        // already-emitted slots are simply dropped.
        if state.last_emitted.is_some_and(|last| slot <= last) {
            return;
        }
        state.block_metas.insert(slot, meta);
    }

    /// Records one geyser account update (replay thread, bank execution
    /// order). `txn_signature` is the owning transaction's first signature
    /// for transaction stores, `None` for runtime-direct (orphan) writes.
    ///
    /// Direct (locking) record entry point. In production the orphan path
    /// reaches the recorder via [`note_account_update`] and transaction
    /// updates via the lock-free capture path, so this is exercised mainly
    /// by tests driving a recorder instance directly.
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn record_account_update(
        &self,
        slot: Slot,
        pubkey: &solana_pubkey::Pubkey,
        account: &AccountSharedData,
        txn_signature: Option<Signature>,
        write_version: u64,
    ) {
        let update = OwnedAccountUpdate::capture(pubkey, account, write_version);
        self.record_owned_update(slot, txn_signature, update);
    }

    /// Records a pre-built owned update through the recorder mutex. Shared
    /// by the orphan direct path and the batched capture-merge path.
    fn record_owned_update(
        &self,
        slot: Slot,
        signature: Option<Signature>,
        update: OwnedAccountUpdate,
    ) {
        use std::sync::atomic::Ordering;
        let wait_start = std::time::Instant::now();
        let mut state = self.lock();
        let held_start = std::time::Instant::now();
        RECORDER_WAIT_US.fetch_add(
            held_start.duration_since(wait_start).as_micros() as u64,
            Ordering::Relaxed,
        );
        if !state.finished {
            state.route_update(slot, signature, update);
        }
        RECORDER_HELD_US.fetch_add(held_start.elapsed().as_micros() as u64, Ordering::Relaxed);
    }

    /// Records a whole batch's worth of transaction-owned account updates,
    /// captured lock-free during execution (see [`begin_capture`]). Taken
    /// once per batch on the coordinator thread — off the hot path — so the
    /// recorder mutex sees per-entry frequency, not per-account-write.
    pub fn record_captured_updates(&self, captured: Vec<CapturedUpdate>) {
        if captured.is_empty() {
            return;
        }
        use std::sync::atomic::Ordering;
        let wait_start = std::time::Instant::now();
        let mut state = self.lock();
        let held_start = std::time::Instant::now();
        RECORDER_WAIT_US.fetch_add(
            held_start.duration_since(wait_start).as_micros() as u64,
            Ordering::Relaxed,
        );
        if !state.finished {
            for c in captured {
                state.route_update(c.slot, c.signature, c.update);
            }
        }
        RECORDER_HELD_US.fetch_add(held_start.elapsed().as_micros() as u64, Ordering::Relaxed);
    }

    /// Records a verified, committed entry (replay thread, entry order).
    /// Ticks pass an empty `txs`.
    pub fn record_committed_entry(
        &self,
        slot: Slot,
        entry_index: usize,
        num_hashes: u64,
        txs: Vec<(VersionedTransaction, TransactionStatusMeta)>,
    ) {
        let mut state = self.lock();
        if state.finished {
            return;
        }
        state.emit_complete_below(slot);
        let assembly = state.assemblies.entry(slot).or_default();
        if assembly.entries.len() != entry_index {
            panic!(
                "horizon: entry discontinuity at slot {slot}: got entry {entry_index}, expected {} \
                 (mid-slot resume is not supported while recording an archive)",
                assembly.entries.len()
            );
        }
        assembly.entries.push(EntryRecord {
            num_hashes,
            tx_count: txs.len() as u32,
        });
        for (tx, meta) in txs {
            assembly.txs.push(CommittedTx { tx, meta });
        }
    }

    /// Emits everything still buffered (including trailing leader-skipped
    /// slots), finalizes the archive, and returns the writer stats.
    ///
    /// The caller must freeze the final replayed bank first so the last
    /// slot's post-transaction orphan updates have been recorded.
    pub fn finish(&self) -> Result<ArchiveStats, String> {
        let mut state = self.lock();
        if state.finished {
            return Err("horizon recorder already finished".to_string());
        }
        state.emit_complete_below(Slot::MAX);
        // Trailing leader-skipped slots through the end of the range.
        let next = state
            .last_emitted
            .map(|s| s + 1)
            .unwrap_or(state.slot_start);
        for slot in next..=state.slot_end_inclusive {
            state.check_gap_slot_skipped(slot);
            state.writer.write_skipped_slot(slot).map_err(|err| {
                format!("horizon: failed to write trailing skipped slot {slot}: {err}")
            })?;
        }
        state.finished = true;
        if !state.block_metas.is_empty() {
            let leftover: Vec<Slot> = state.block_metas.keys().copied().collect();
            return Err(format!(
                "horizon: {} block metadata record(s) were never emitted (slots {:?}…)",
                leftover.len(),
                &leftover[..leftover.len().min(8)]
            ));
        }
        // `finish` consumes the writer; swap in a placeholder writer is not
        // possible, so rebuild via take. The writer is moved out by value.
        let state = &mut *state;
        let writer = std::mem::replace(
            &mut state.writer,
            ArchiveWriter::new(
                BufWriter::new(File::create("/dev/null").map_err(|err| err.to_string())?),
                state.epoch,
                state.slot_start,
                1,
                ArchiveWriterConfig::default(),
            )
            .map_err(|err| err.to_string())?,
        );
        let (sink, stats) = writer
            .finish()
            .map_err(|err| format!("horizon: failed to finalize archive: {err}"))?;
        sink.into_inner()
            .map_err(|err| format!("horizon: failed to flush archive: {err}"))?;
        info!(
            "horizon archive complete: slots={} blocks={} txs={} tx_updates={} orphan_updates={} \
             epochs={} buckets={} bytes={} (payload {} uncompressed)",
            stats.slots,
            stats.blocks,
            stats.transactions,
            stats.account_updates,
            stats.orphan_account_updates,
            stats.epochs,
            stats.buckets,
            stats.bytes_written,
            stats.uncompressed_payload_bytes,
        );
        Ok(stats)
    }
}

impl RecorderState {
    /// Files one update into the right slot assembly: keyed by signature for
    /// transaction-owned writes, or pre/post-transaction orphan groups for
    /// runtime-direct writes (phase decided by whether the slot has seen its
    /// first committed transaction yet — matching the writer's wire order).
    fn route_update(
        &mut self,
        slot: Slot,
        signature: Option<Signature>,
        update: OwnedAccountUpdate,
    ) {
        self.emit_complete_below(slot);
        let assembly = self.assemblies.entry(slot).or_default();
        match signature {
            Some(sig) => assembly.tx_updates.entry(sig).or_default().push(update),
            None if assembly.txs.is_empty() => assembly.pre_orphans.push(update),
            None => assembly.post_orphans.push(update),
        }
    }

    /// Emits every buffered assembly with slot < `boundary`. Activity for
    /// `boundary` proves those slots' banks froze (their post-orphans are
    /// in), and stream order guarantees their input-side block metadata
    /// already arrived.
    fn emit_complete_below(&mut self, boundary: Slot) {
        while let Some((&slot, _)) = self.assemblies.first_key_value() {
            if slot >= boundary {
                break;
            }
            let assembly = self.assemblies.remove(&slot).expect("assembly present");
            self.emit_slot(slot, assembly);
        }
    }

    /// Verifies a gap slot really was leader-skipped according to the
    /// old-faithful index; a present-but-missing block would silently
    /// corrupt the archive.
    fn check_gap_slot_skipped(&self, slot: Slot) {
        if self.presence.state(slot) == Some(SlotPresenceState::Present) {
            panic!(
                "horizon: slot {slot} has block data in the old-faithful index but was never \
                 replayed; refusing to record it as leader-skipped"
            );
        }
    }

    fn emit_slot(&mut self, slot: Slot, mut assembly: SlotAssembly) {
        // Leader-skipped frames for the gap since the previous block.
        let next = self.last_emitted.map(|s| s + 1).unwrap_or(self.slot_start);
        for gap_slot in next..slot {
            self.check_gap_slot_skipped(gap_slot);
            self.writer
                .write_skipped_slot(gap_slot)
                .unwrap_or_else(|err| {
                    panic!("horizon: failed to write skipped slot {gap_slot}: {err}")
                });
        }

        let block_meta = self.block_metas.remove(&slot).unwrap_or_else(|| {
            panic!("horizon: no block metadata buffered for replayed slot {slot}")
        });

        self.writer
            .begin_slot(slot)
            .unwrap_or_else(|err| panic!("horizon: begin_slot({slot}) failed: {err}"));

        // The epoch notification rides the epoch's first block frame.
        if !self.epoch_meta_written {
            let em = &mut self.epoch_scratch;
            em.clear();
            em.epoch = self.epoch;
            em.start_slot = self.slot_start;
            em.slot_count = self.slot_end_inclusive - self.slot_start + 1;
            em.first_block_slot = slot;
            em.num_reward_partitions = block_meta.num_partitions;
            self.writer
                .write_epoch_meta(em)
                .unwrap_or_else(|err| panic!("horizon: write_epoch_meta failed: {err}"));
            self.epoch_meta_written = true;
        }

        for update in &assembly.pre_orphans {
            self.writer
                .write_orphan_update(&update.as_view())
                .unwrap_or_else(|err| {
                    panic!("horizon: pre-orphan update failed at slot {slot}: {err}")
                });
        }

        for (tx_index, committed) in assembly.txs.iter().enumerate() {
            let scratch = &mut self.tx_scratch;
            convert::populate_transaction(scratch, &committed.tx, &committed.meta).unwrap_or_else(
                |err| {
                    panic!(
                        "horizon: transaction conversion failed at slot {slot} index {tx_index} \
                         sig {:?}: {err}",
                        committed.tx.signatures.first()
                    )
                },
            );
            let sig = committed.tx.signatures.first().unwrap_or_else(|| {
                panic!("horizon: transaction without signature at slot {slot} index {tx_index}")
            });
            if let Some(updates) = assembly.tx_updates.remove(sig) {
                for update in &updates {
                    scratch
                        .push_account_update(&update.as_view())
                        .unwrap_or_else(|err| {
                            panic!(
                                "horizon: account update overflow at slot {slot} sig {sig}: {err}"
                            )
                        });
                }
            }
            self.writer
                .write_transaction(scratch)
                .unwrap_or_else(|err| {
                    panic!(
                        "horizon: write_transaction failed at slot {slot} index {tx_index}: {err}"
                    )
                });
        }

        if !assembly.tx_updates.is_empty() {
            let stray: Vec<String> = assembly
                .tx_updates
                .keys()
                .take(4)
                .map(|sig| sig.to_string())
                .collect();
            panic!(
                "horizon: {} account update group(s) at slot {slot} reference transactions that \
                 were never committed (e.g. {stray:?})",
                assembly.tx_updates.len()
            );
        }

        for update in &assembly.post_orphans {
            self.writer
                .write_orphan_update(&update.as_view())
                .unwrap_or_else(|err| {
                    panic!("horizon: post-orphan update failed at slot {slot}: {err}")
                });
        }

        let meta = &mut self.meta_scratch;
        meta.clear();
        meta.slot = slot;
        meta.parent_slot = block_meta.parent_slot;
        meta.parent_blockhash = block_meta.parent_blockhash;
        meta.blockhash = block_meta.blockhash;
        meta.block_time = block_meta.block_time;
        meta.block_height = block_meta.block_height;
        meta.executed_transaction_count = block_meta.executed_transaction_count;
        meta.entry_count = block_meta.entry_count;
        meta.num_partitions = block_meta.num_partitions;
        for (pubkey, info) in &block_meta.rewards {
            meta.rewards
                .try_push(convert::reward_from_info(*pubkey, info))
                .unwrap_or_else(|_| {
                    panic!(
                        "horizon: block rewards overflow at slot {slot} ({} rewards)",
                        block_meta.rewards.len()
                    )
                });
        }

        self.writer
            .end_slot(meta, &assembly.entries)
            .unwrap_or_else(|err| panic!("horizon: end_slot({slot}) failed: {err}"));
        // Mirror the running file size into a lock-free atomic so the
        // progress thread can read it without contending the recorder mutex.
        // `bytes_written` steps up as buckets flush (every ~128 slots).
        ARCHIVE_BYTES_WRITTEN.store(
            self.writer.stats().bytes_written,
            std::sync::atomic::Ordering::Relaxed,
        );
        self.last_emitted = Some(slot);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use jetstreamer_horizon::archive::{ArchiveReader, BlockNotification, SlotVisitor};
    use solana_account::Account;
    use solana_pubkey::Pubkey;

    fn presence_map(start: Slot, states: &[SlotPresenceState]) -> std::sync::Arc<SlotPresenceMap> {
        let end_inclusive = start + states.len() as u64 - 1;
        let mut next_present_after = vec![None; states.len()];
        let mut next: Option<Slot> = None;
        for (i, state) in states.iter().enumerate().rev() {
            next_present_after[i] = next;
            if *state == SlotPresenceState::Present {
                next = Some(start + i as u64);
            }
        }
        std::sync::Arc::new(SlotPresenceMap {
            start,
            end_inclusive,
            states: states.to_vec(),
            next_present_after,
        })
    }

    fn pk(byte: u8) -> Pubkey {
        Pubkey::new_from_array([byte; 32])
    }

    fn account(lamports: u64, data: &[u8], owner: u8) -> AccountSharedData {
        AccountSharedData::from(Account {
            lamports,
            data: data.to_vec(),
            owner: pk(owner),
            executable: false,
            rent_epoch: u64::MAX,
        })
    }

    fn legacy_tx(sig_byte: u8, key: u8) -> (VersionedTransaction, TransactionStatusMeta) {
        let tx = VersionedTransaction {
            signatures: vec![Signature::from([sig_byte; 64])],
            message: solana_message::VersionedMessage::Legacy(solana_message::legacy::Message {
                header: solana_message::MessageHeader {
                    num_required_signatures: 1,
                    ..Default::default()
                },
                account_keys: vec![pk(key)],
                recent_blockhash: solana_hash::Hash::new_unique(),
                instructions: vec![],
            }),
        };
        let meta = TransactionStatusMeta {
            fee: 5_000 + sig_byte as u64,
            pre_balances: vec![100],
            post_balances: vec![90],
            ..Default::default()
        };
        (tx, meta)
    }

    /// (slot, blockhash, n_rewards, pre write_versions, post
    /// write_versions, entries as (num_hashes, tx_count)).
    type BlockSnapshot = (Slot, Hash, usize, Vec<u64>, Vec<u64>, Vec<(u64, u32)>);
    /// (slot, tx_index, sig, fee, updates as (pubkey, write_version, data)).
    type TxSnapshot = (Slot, u32, Signature, u64, Vec<(Address, u64, Vec<u8>)>);

    #[derive(Default)]
    struct Collected {
        epochs: Vec<(u64, u64, u64, Option<u64>)>, // (epoch, first_block_slot, slot_count, partitions)
        skipped: Vec<Slot>,
        blocks: Vec<BlockSnapshot>,
        txs: Vec<TxSnapshot>,
    }

    impl SlotVisitor for Collected {
        fn on_epoch(&mut self, meta: &EpochMeta) {
            self.epochs.push((
                meta.epoch,
                meta.first_block_slot,
                meta.slot_count,
                meta.num_reward_partitions,
            ));
        }

        fn on_transaction(&mut self, slot: Slot, tx_index: u32, tx: &Transaction) {
            let updates = tx
                .iter_account_updates()
                .map(|(m, d)| (m.pubkey, m.write_version, d.to_vec()))
                .collect();
            self.txs
                .push((slot, tx_index, tx.signatures[0], tx.fee, updates));
        }

        fn on_block(&mut self, notification: &BlockNotification, entries: &[EntryRecord]) {
            match notification {
                BlockNotification::Skipped(s) => self.skipped.push(s.slot),
                BlockNotification::Block(meta) => self.blocks.push((
                    meta.slot,
                    meta.blockhash,
                    meta.rewards.len(),
                    meta.pre_updates
                        .iter()
                        .map(|(m, _)| m.write_version)
                        .collect(),
                    meta.post_updates
                        .iter()
                        .map(|(m, _)| m.write_version)
                        .collect(),
                    entries.iter().map(|e| (e.num_hashes, e.tx_count)).collect(),
                )),
            }
        }
    }

    /// Drives the recorder through a realistic two-block scenario (in true
    /// replay event order) and verifies the archive reads back exactly.
    #[test]
    fn recorder_end_to_end() {
        std::thread::Builder::new()
            .stack_size(128 * 1024 * 1024)
            .spawn(recorder_end_to_end_body)
            .expect("spawn test thread")
            .join()
            .expect("join test thread");
    }

    fn recorder_end_to_end_body() {
        // Slots 100..=109: blocks at 100 and 105, everything else
        // leader-skipped.
        let mut states = vec![SlotPresenceState::Missing; 10];
        states[0] = SlotPresenceState::Present;
        states[5] = SlotPresenceState::Present;
        let presence = presence_map(100, &states);

        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("test.jet");
        let recorder =
            HorizonRecorder::create(&path, 42, 100, 10, presence).expect("create recorder");

        let bh_100 = solana_hash::Hash::new_unique();
        let bh_105 = solana_hash::Hash::new_unique();
        let (tx1, meta1) = legacy_tx(0xA1, 1);
        let (tx2, meta2) = legacy_tx(0xA2, 2);
        let (tx3, meta3) = legacy_tx(0xB1, 3);
        let sig1 = tx1.signatures[0];
        let sig2 = tx2.signatures[0];
        let sig3 = tx3.signatures[0];

        // --- input side: block metadata arrives ahead of replay ---
        recorder.record_block_meta(
            100,
            99,
            &Hash::default().to_string(),
            &bh_100.to_string(),
            &KeyedRewardsAndNumPartitions {
                keyed_rewards: vec![(
                    pk(9),
                    solana_reward_info::RewardInfo {
                        reward_type: solana_reward_info::RewardType::Voting,
                        lamports: 10,
                        post_balance: 100,
                        commission: Some(5),
                    },
                )],
                num_partitions: Some(7),
            },
            Some(111),
            Some(50),
            2,
            2,
        );

        // --- replay side, slot 100 ---
        // Bank creation: slot-start orphan (sysvar rewrite).
        recorder.record_account_update(100, &pk(10), &account(1, b"sysvar", 20), None, 1);
        // Entry 0: tick.
        recorder.record_committed_entry(100, 0, 8, Vec::new());
        // Entry 1: two transactions; their stores arrive during execution.
        recorder.record_account_update(100, &pk(1), &account(90, b"alpha", 21), Some(sig1), 2);
        recorder.record_account_update(100, &pk(2), &account(80, b"beta", 21), Some(sig2), 3);
        recorder.record_committed_entry(100, 1, 4, vec![(tx1, meta1), (tx2, meta2)]);
        // Freeze of slot 100 (during slot 105's bank creation): slot-end orphan.
        recorder.record_account_update(100, &pk(11), &account(7, b"fees", 20), None, 4);

        // --- input side: slot 105 block metadata ---
        recorder.record_block_meta(
            105,
            100,
            &bh_100.to_string(),
            &bh_105.to_string(),
            &KeyedRewardsAndNumPartitions {
                keyed_rewards: vec![],
                num_partitions: None,
            },
            Some(222),
            Some(51),
            1,
            1,
        );

        // --- replay side, slot 105 (pre-orphan triggers emit of slot 100) ---
        recorder.record_account_update(105, &pk(10), &account(2, b"sysvar2", 20), None, 5);
        recorder.record_account_update(105, &pk(3), &account(70, b"gamma", 21), Some(sig3), 6);
        recorder.record_committed_entry(105, 0, 9, vec![(tx3, meta3)]);
        // Final freeze (driven by `freeze_latest_bank` in the real flow).
        recorder.record_account_update(105, &pk(11), &account(8, b"fees2", 20), None, 7);

        let stats = recorder.finish().expect("finish");
        assert_eq!(stats.slots, 10);
        assert_eq!(stats.blocks, 2);
        assert_eq!(stats.transactions, 3);
        assert_eq!(stats.account_updates, 3);
        assert_eq!(stats.orphan_account_updates, 4);
        assert_eq!(stats.epochs, 1);

        // --- read back and verify ---
        let bytes = std::fs::read(&path).expect("read archive");
        let mut reader = ArchiveReader::open(std::io::Cursor::new(bytes)).expect("open archive");
        let mut collected = Collected::default();
        let visited = reader.read_slots(100, 64, &mut collected).expect("read");
        assert_eq!(visited, 10);

        assert_eq!(collected.epochs, vec![(42, 100, 10, Some(7))]);
        assert_eq!(
            collected.skipped,
            vec![101, 102, 103, 104, 106, 107, 108, 109]
        );

        assert_eq!(collected.blocks.len(), 2);
        let (slot, blockhash, n_rewards, pre, post, entries) = &collected.blocks[0];
        assert_eq!(*slot, 100);
        assert_eq!(*blockhash, bh_100);
        assert_eq!(*n_rewards, 1);
        assert_eq!(pre, &[1]);
        assert_eq!(post, &[4]);
        assert_eq!(entries, &[(8, 0), (4, 2)]);
        let (slot, blockhash, n_rewards, pre, post, entries) = &collected.blocks[1];
        assert_eq!(*slot, 105);
        assert_eq!(*blockhash, bh_105);
        assert_eq!(*n_rewards, 0);
        assert_eq!(pre, &[5]);
        assert_eq!(post, &[7]);
        assert_eq!(entries, &[(9, 1)]);

        assert_eq!(collected.txs.len(), 3);
        let (slot, tx_index, sig, fee, updates) = &collected.txs[0];
        assert_eq!((*slot, *tx_index, *sig, *fee), (100, 0, sig1, 5_000 + 0xA1));
        assert_eq!(
            updates,
            &[(Address::new_from_array([1; 32]), 2, b"alpha".to_vec())]
        );
        let (slot, tx_index, sig, _, updates) = &collected.txs[1];
        assert_eq!((*slot, *tx_index, *sig), (100, 1, sig2));
        assert_eq!(updates[0].2, b"beta".to_vec());
        let (slot, tx_index, sig, _, updates) = &collected.txs[2];
        assert_eq!((*slot, *tx_index, *sig), (105, 0, sig3));
        assert_eq!(updates[0].2, b"gamma".to_vec());
    }

    /// A gap slot that old-faithful says has a block must abort the run
    /// rather than silently recording it as leader-skipped.
    #[test]
    #[should_panic(expected = "refusing to record it as leader-skipped")]
    fn refuses_to_skip_present_slot() {
        let states = vec![SlotPresenceState::Present; 3];
        let presence = presence_map(200, &states);
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("bad.jet");
        let recorder =
            HorizonRecorder::create(&path, 1, 200, 3, presence).expect("create recorder");
        // Only slot 202 gets data; 200-201 are gaps the index says exist.
        recorder.record_block_meta(
            202,
            199,
            &Hash::default().to_string(),
            &Hash::default().to_string(),
            &KeyedRewardsAndNumPartitions {
                keyed_rewards: vec![],
                num_partitions: None,
            },
            None,
            None,
            0,
            1,
        );
        recorder.record_committed_entry(202, 0, 1, Vec::new());
        let _ = recorder.finish();
    }
}
