use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    env, fs,
    path::{Path, PathBuf},
    process::{Stdio, exit},
    str::FromStr,
    sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use agave_snapshots::{
    ArchiveFormat, ArchiveFormatDecompressor,
    snapshot_archive_info::{FullSnapshotArchiveInfo, SnapshotArchiveInfoGetter},
    snapshot_hash::SnapshotHash,
    streaming_unarchive_snapshot,
};
use bs58;
use cid::{Cid, multibase::Base};
use crossbeam_channel::unbounded;
use dashmap::DashMap;
use jetstreamer_firehose::{
    epochs::{BASE_URL, epoch_to_slot_range, slot_to_epoch},
    firehose::{FirehoseError, GeyserNotifiers, firehose_geyser_with_notifiers},
};
use jetstreamer_node::snapshots::{
    DEFAULT_BUCKET, download_snapshot_at_or_before_slot, list_epoch_snapshots,
};
use log::{info, warn};
use reqwest::{Client, Url, header::RANGE};
use serde_cbor::Value;
use solana_account::AccountSharedData;
use solana_accounts_db::{
    accounts_db::AccountsDbConfig,
    accounts_update_notifier_interface::{
        AccountForGeyser, AccountsUpdateNotifier, AccountsUpdateNotifierInterface,
    },
};
use solana_clock::{Slot, MAX_PROCESSING_AGE};
use solana_genesis_utils::{MAX_GENESIS_ARCHIVE_UNPACKED_SIZE, open_genesis_config};
use solana_geyser_plugin_manager::block_metadata_notifier_interface::{
    BlockMetadataNotifier, BlockMetadataNotifierArc,
};
use solana_geyser_plugin_manager::geyser_plugin_service::GeyserPluginService;
use solana_hash::Hash;
use solana_ledger::{
    blockstore_processor::set_alpenglow_ticks, entry_notifier_interface::EntryNotifier,
    leader_schedule_cache::LeaderScheduleCache,
};
use solana_rpc::transaction_notifier_interface::TransactionNotifier;
use solana_runtime::{
    bank::Bank, bank_forks::BankForks, installed_scheduler_pool::BankWithScheduler,
    runtime_config::RuntimeConfig, snapshot_bank_utils, snapshot_utils,
};
use solana_signature::Signature;
use solana_svm::transaction_processor::ExecutionRecordingConfig;
use solana_svm_timings::ExecuteTimings;
use solana_transaction::{
    TransactionError, sanitized::SanitizedTransaction, versioned::VersionedTransaction,
};
use solana_transaction_status::TransactionStatusMeta;
use tar::Archive as TarArchive;
use tempfile::Builder as TempDirBuilder;
use tokio::process::Command;
use xxhash_rust::xxh64::xxh64;

const RIPGET_LOG_INTERVAL_SECS: u64 = 5;
const SNAPSHOT_UNPACK_LOG_INTERVAL_SECS: u64 = 5;
const SNAPSHOT_PRECOUNT_BUFFER_BYTES: usize = 16 * 1024 * 1024;
const DEFAULT_LOG_FILTER: &str = "info,solana_metrics=off,solana_runtime::bank=off";
const COMPACT_INDEX_MAGIC: &[u8; 8] = b"compiszd";
const BUCKET_HEADER_SIZE: usize = 16;
const HASH_PREFIX_SIZE: usize = 32;
const SLOT_TO_CID_KIND: &[u8] = b"slot-to-cid";
const METADATA_KEY_KIND: &[u8] = b"index_kind";
const METADATA_KEY_EPOCH: &[u8] = b"epoch";
const CAR_HEADER_PREFETCH_BYTES: u64 = 4 * 1024;

const PLUGIN_NAME: &str = "jetstreamer-node-geyser";
const PLUGIN_LIB_BASENAME: &str = "jetstreamer_node_geyser";
const BANK_SNAPSHOTS_DIR: &str = "snapshots";
const ACCOUNTS_HARDLINKS_DIR: &str = "accounts_hardlinks";
const GENESIS_ARCHIVE: &str = "genesis.tar.bz2";
const SNAPSHOT_VERSION_FILE: &str = "version";
const SNAPSHOT_STATUS_CACHE_FILE: &str = "status_cache";
const ACCOUNTS_SNAPSHOT_DIR: &str = "snapshot";
const ACCOUNTS_RUN_DIR: &str = "run";
const ARCHIVE_ACCOUNTS_DIR: &str = "accounts-run";
const DEFAULT_ROOT_INTERVAL: u64 = 1024;
const ENTRY_EXEC_WARN_AFTER: Duration = Duration::from_secs(5);
const ENTRY_EXEC_FAIL_AFTER: Duration = Duration::from_secs(300);
const BANK_FOR_SLOT_WARN_AFTER: Duration = Duration::from_secs(5);
const ENABLE_PROGRAM_CACHE_PRUNE: bool = true;
static LOGGED_STALL_TX: AtomicBool = AtomicBool::new(false);
static LOGGED_FIRST_ACCOUNT_UPDATE: AtomicBool = AtomicBool::new(false);

struct SnapshotVerifier {
    expected: DashMap<Slot, SnapshotHash>,
    errors: DashMap<usize, String>,
    error_count: AtomicUsize,
    shutdown: Option<Arc<AtomicBool>>,
}

impl SnapshotVerifier {
    fn new(expected: BTreeMap<Slot, SnapshotHash>, shutdown: Option<Arc<AtomicBool>>) -> Self {
        let expected_map = DashMap::new();
        for (slot, hash) in expected {
            expected_map.insert(slot, hash);
        }
        Self {
            expected: expected_map,
            errors: DashMap::new(),
            error_count: AtomicUsize::new(0),
            shutdown,
        }
    }

    fn verify_bank(&self, bank: &Bank) {
        let slot = bank.slot();
        let expected_hash = self.expected.remove(&slot).map(|(_, hash)| hash);
        let Some(expected_hash) = expected_hash else {
            return;
        };

        let actual_hash = bank.get_snapshot_hash();
        if actual_hash != expected_hash {
            let message = format!(
                "snapshot hash mismatch at slot {slot}: expected {}, got {}",
                expected_hash.0, actual_hash.0
            );
            warn!("{message}");
            self.record_error(message);
        } else {
            info!("verified snapshot hash at slot {slot}");
        }
    }

    fn finish(&self) -> Result<(), String> {
        let total_errors = self.error_count.load(Ordering::Relaxed);
        if total_errors > 0 {
            let mut entries: Vec<(usize, String)> = self
                .errors
                .iter()
                .map(|entry| (*entry.key(), entry.value().clone()))
                .collect();
            entries.sort_by_key(|(idx, _)| *idx);
            let total = total_errors.max(entries.len());
            let mut message = String::from("snapshot verification failed:");
            for (_, error) in entries.iter().take(5) {
                message.push_str("\n- ");
                message.push_str(error);
            }
            if total > 5 {
                message.push_str(&format!("\n- ... {} more", total - 5));
            }
            return Err(message);
        }

        let mut missing: Vec<Slot> = self.expected.iter().map(|entry| *entry.key()).collect();
        if !missing.is_empty() {
            missing.sort_unstable();
            let preview: Vec<Slot> = missing.iter().copied().take(10).collect();
            return Err(format!(
                "snapshot verification incomplete: missing {} snapshot slot(s) (first {}: {:?})",
                missing.len(),
                preview.len(),
                preview
            ));
        }

        Ok(())
    }

    fn record_error(&self, message: String) {
        let idx = self.error_count.fetch_add(1, Ordering::Relaxed);
        self.errors.insert(idx, message);
        if let Some(shutdown) = &self.shutdown {
            shutdown.store(true, Ordering::SeqCst);
        }
    }
}

struct BankReplay {
    bank_forks: Arc<RwLock<BankForks>>,
    snapshot_verifier: Option<Arc<SnapshotVerifier>>,
    root_interval: Option<u64>,
    leader_schedule_cache: LeaderScheduleCache,
    failure: Arc<ReplayFailure>,
    cursor: Arc<ReplayCursor>,
    debug_signature: Option<Signature>,
    prune_inflight: Arc<AtomicBool>,
    cached_bank: Mutex<Option<CachedBank>>,
    execution_gate: Arc<Mutex<()>>,
}

#[derive(Debug)]
struct CachedBank {
    slot: Slot,
    bank: Arc<Bank>,
}

struct InFlightGuard {
    cursor: Arc<ReplayCursor>,
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        self.cursor.finish_inflight();
    }
}

impl BankReplay {
    fn new(
        bank: Bank,
        snapshot_verifier: Option<Arc<SnapshotVerifier>>,
        root_interval: Option<u64>,
        failure: Arc<ReplayFailure>,
        cursor: Arc<ReplayCursor>,
    ) -> Self {
        let mut leader_schedule_cache = LeaderScheduleCache::new_from_bank(&bank);
        leader_schedule_cache.set_max_schedules(usize::MAX);
        let debug_signature = env::var("JETSTREAMER_DEBUG_SIG")
            .ok()
            .and_then(|value| Signature::from_str(value.trim()).ok());
        let bank_forks = BankForks::new_rw_arc(bank);
        let cached_bank = {
            let guard = bank_forks
                .read()
                .expect("bank forks lock poisoned during init");
            let bank = guard.working_bank();
            Mutex::new(Some(CachedBank {
                slot: bank.slot(),
                bank,
            }))
        };
        Self {
            bank_forks,
            snapshot_verifier,
            root_interval,
            leader_schedule_cache,
            failure,
            cursor,
            debug_signature,
            prune_inflight: Arc::new(AtomicBool::new(false)),
            cached_bank,
            execution_gate: Arc::new(Mutex::new(())),
        }
    }

    fn cached_bank_for_slot(&self, slot: Slot) -> Option<Arc<Bank>> {
        let guard = self.cached_bank.lock().ok()?;
        guard
            .as_ref()
            .and_then(|cached| (cached.slot == slot).then(|| Arc::clone(&cached.bank)))
    }

    fn update_cached_bank(&self, bank: Arc<Bank>) {
        if let Ok(mut guard) = self.cached_bank.lock() {
            *guard = Some(CachedBank {
                slot: bank.slot(),
                bank,
            });
        }
    }

    fn bank_for_slot(&self, slot: Slot) -> Result<Arc<Bank>, String> {
        if let Some(bank) = self.cached_bank_for_slot(slot) {
            return Ok(bank);
        }
        self.cursor.update_inflight_stage("bank_for_slot_lock_try");
        let lock_start = Instant::now();
        let mut guard = match self.bank_forks.try_write() {
            Ok(guard) => guard,
            Err(_) => {
                self.cursor.update_inflight_stage("bank_for_slot_lock_wait");
                let guard = self
                    .bank_forks
                    .write()
                    .map_err(|_| "bank forks lock poisoned".to_string())?;
                let waited = lock_start.elapsed();
                if waited >= BANK_FOR_SLOT_WARN_AFTER {
                    warn!(
                        "bank_for_slot lock waited {:.3}s for slot {}",
                        waited.as_secs_f64(),
                        slot
                    );
                }
                guard
            }
        };
        self.cursor.update_inflight_stage("bank_for_slot_locked");
        let current_slot = guard.highest_slot();
        if slot < current_slot {
            return Err(format!(
                "slot {slot} behind current bank slot {current_slot}"
            ));
        }
        if slot > current_slot {
            let step_start = Instant::now();
            let parent = guard.working_bank();
            self.cursor.update_inflight_stage("freeze_parent");
            parent.freeze();
            let frozen_bank = parent.clone();
            let parent_slot = parent.slot();
            let mut prune_request = None::<Slot>;
            self.cursor.update_inflight_stage("set_root");
            self.leader_schedule_cache.set_root(&frozen_bank);
            self.cursor.update_inflight_stage("slot_leader_at");
            let collector_id = self
                .leader_schedule_cache
                .slot_leader_at(slot, Some(&parent))
                .unwrap_or_else(|| *parent.collector_id());
            self.cursor.update_inflight_stage("new_from_parent");
            let next_bank = Bank::new_from_parent(parent, &collector_id, slot);
            self.cursor.update_inflight_stage("set_alpenglow_ticks");
            set_alpenglow_ticks(&next_bank);
            self.cursor.update_inflight_stage("insert_bank");
            let bank_with_scheduler = guard.insert(next_bank);
            if let Some(interval) = self.root_interval {
                if interval > 0 && parent_slot % interval == 0 {
                    self.cursor.update_inflight_stage("root_set");
                    let root_start = Instant::now();
                    guard.set_root(parent_slot, None, None);
                    let set_elapsed = root_start.elapsed();
                    if set_elapsed >= BANK_FOR_SLOT_WARN_AFTER {
                        warn!(
                            "bank_for_slot root set slow: slot {} took {:.3}s",
                            parent_slot,
                            set_elapsed.as_secs_f64()
                        );
                    }
                    if ENABLE_PROGRAM_CACHE_PRUNE {
                        prune_request = Some(parent_slot);
                    } else {
                        warn!(
                            "bank_for_slot skipping program cache prune at slot {} (debug)",
                            parent_slot
                        );
                    }
                }
            }
            self.cursor.update_inflight_stage("clone_without_scheduler");
            let next_bank = bank_with_scheduler.clone_without_scheduler();
            drop(guard);
            if let Some(prune_slot) = prune_request {
                let inflight = self.prune_inflight.clone();
                let bank_forks = Arc::clone(&self.bank_forks);
                let execution_gate = Arc::clone(&self.execution_gate);
                if inflight
                    .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok()
                {
                    std::thread::spawn(move || {
                        let _gate = execution_gate
                            .lock()
                            .expect("execution gate lock poisoned");
                        let start = Instant::now();
                        let guard = bank_forks
                            .write()
                            .expect("bank forks lock poisoned");
                        guard.prune_program_cache(prune_slot);
                        let elapsed = start.elapsed();
                        if elapsed >= BANK_FOR_SLOT_WARN_AFTER {
                            warn!(
                                "bank_for_slot program cache prune async: slot {} took {:.3}s",
                                prune_slot,
                                elapsed.as_secs_f64()
                            );
                        }
                        inflight.store(false, Ordering::SeqCst);
                    });
                } else {
                    warn!(
                        "bank_for_slot skipping program cache prune at slot {} (already running)",
                        prune_slot
                    );
                }
            }
            if let Some(verifier) = self.snapshot_verifier.as_ref() {
                self.cursor.update_inflight_stage("verify_bank");
                verifier.verify_bank(&frozen_bank);
            }
            let elapsed = step_start.elapsed();
            if elapsed >= BANK_FOR_SLOT_WARN_AFTER {
                warn!(
                    "bank_for_slot slow path: slot {} took {:.3}s",
                    slot,
                    elapsed.as_secs_f64()
                );
            }
            self.update_cached_bank(Arc::clone(&next_bank));
            return Ok(next_bank);
        }
        let bank = guard.working_bank();
        self.update_cached_bank(Arc::clone(&bank));
        Ok(bank)
    }

    fn register_tick(&self, slot: Slot, hash: Hash) -> Result<(), String> {
        let bank = self.bank_for_slot(slot)?;
        let bank_with_scheduler = BankWithScheduler::new_without_scheduler(bank);
        bank_with_scheduler.register_tick(&hash);
        Ok(())
    }

    fn note_entry_duration(
        &self,
        entry: &ReadyEntry,
        elapsed: Duration,
        signature: Option<&str>,
    ) {
        if elapsed >= ENTRY_EXEC_WARN_AFTER {
            warn!(
                "slow entry execution: slot {} entry {} txs={} elapsed={:.3}s sig={}",
                entry.slot,
                entry.entry_index,
                entry.tx_count,
                elapsed.as_secs_f64(),
                signature.unwrap_or("<none>")
            );
        }
        if elapsed >= ENTRY_EXEC_FAIL_AFTER {
            let message = format!(
                "entry execution exceeded timeout: slot {} entry {} txs={} elapsed={:.3}s sig={}",
                entry.slot,
                entry.entry_index,
                entry.tx_count,
                elapsed.as_secs_f64(),
                signature.unwrap_or("<none>")
            );
            self.failure.record(message.clone());
            panic!("{message}");
        }
    }

    fn log_slow_entry_details(&self, entry: &ReadyEntry) {
        if entry.tx_count == 0 {
            return;
        }
        warn!(
            "slow entry details: slot {} entry {} tx_start={} tx_count={}",
            entry.slot, entry.entry_index, entry.start_index, entry.tx_count
        );
        for (offset, scheduled) in entry.txs.iter().enumerate() {
            let signature = scheduled
                .tx
                .signatures
                .first()
                .map(|sig| sig.to_string())
                .unwrap_or_else(|| "<missing-signature>".to_string());
            let message = &scheduled.tx.message;
            let static_keys = message.static_account_keys();
            let mut program_ids = Vec::with_capacity(message.instructions().len());
            for ix in message.instructions() {
                let program_id = static_keys
                    .get(ix.program_id_index as usize)
                    .map(|key| bs58::encode(key.to_bytes()).into_string())
                    .unwrap_or_else(|| "<unknown>".to_string());
                program_ids.push(program_id);
            }
            let tx_index = entry.start_index.saturating_add(offset);
            warn!(
                "slow entry tx: slot {} entry {} tx_index={} sig={} instrs={} accounts={} programs={:?}",
                entry.slot,
                entry.entry_index,
                tx_index,
                signature,
                message.instructions().len(),
                static_keys.len(),
                program_ids
            );
        }
    }

    fn process_ready_entries(&self, entries: Vec<ReadyEntry>) {
        for entry in entries {
            let gate_start = Instant::now();
            let _execution_guard = self
                .execution_gate
                .lock()
                .expect("execution gate lock poisoned");
            let gate_wait = gate_start.elapsed();
            if gate_wait >= BANK_FOR_SLOT_WARN_AFTER {
                warn!(
                    "entry execution gate waited {:.3}s (slot {} entry {})",
                    gate_wait.as_secs_f64(),
                    entry.slot,
                    entry.entry_index
                );
            }
            let signature = entry
                .txs
                .first()
                .and_then(|scheduled| scheduled.tx.signatures.first())
                .map(|sig| sig.to_string());
            self.cursor.start_inflight(
                entry.slot,
                entry.entry_index,
                entry.start_index,
                entry.tx_count,
                signature.clone(),
            );
            let _inflight_guard = InFlightGuard {
                cursor: self.cursor.clone(),
            };
            if entry.tx_count == 0 {
                self.cursor.update_inflight_stage("register_tick");
                let start = Instant::now();
                if let Err(err) = self.register_tick(entry.slot, entry.hash) {
                    log::debug!(
                        "failed to register tick for slot {} entry {}: {}",
                        entry.slot,
                        entry.entry_index,
                        err
                    );
                }
                let elapsed = start.elapsed();
                self.note_entry_duration(&entry, elapsed, None);
                continue;
            }

            self.cursor.update_inflight_stage("bank_for_slot");
            let start = Instant::now();
            match self.bank_for_slot(entry.slot) {
                Ok(bank) => {
                    self.cursor.update_inflight_stage("try_process");
                    if entry.slot == 393_521_153
                        && entry.entry_index == 0
                        && !LOGGED_STALL_TX.swap(true, Ordering::SeqCst)
                    {
                        let mut details = String::new();
                        details.push_str(&format!(
                            "stalling entry pre-exec dump: slot {} entry {} tx_start={} tx_count={}",
                            entry.slot, entry.entry_index, entry.start_index, entry.tx_count
                        ));
                        for (offset, scheduled) in entry.txs.iter().enumerate() {
                            let signature = scheduled
                                .tx
                                .signatures
                                .first()
                                .map(|sig| sig.to_string())
                                .unwrap_or_else(|| "<missing-signature>".to_string());
                            let message = &scheduled.tx.message;
                            let static_keys = message.static_account_keys();
                            let mut program_ids =
                                Vec::with_capacity(message.instructions().len());
                            for ix in message.instructions() {
                                let program_id = static_keys
                                    .get(ix.program_id_index as usize)
                                    .map(|key| bs58::encode(key.to_bytes()).into_string())
                                    .unwrap_or_else(|| "<unknown>".to_string());
                                program_ids.push(program_id);
                            }
                            let tx_index = entry.start_index.saturating_add(offset);
                            details.push_str(&format!(
                                "\nstalling tx: slot {} entry {} tx_index={} sig={} instrs={} accounts={} programs={:?} recent_blockhash={}",
                                entry.slot,
                                entry.entry_index,
                                tx_index,
                                signature,
                                message.instructions().len(),
                                static_keys.len(),
                                program_ids,
                                message.recent_blockhash()
                            ));
                        }
                        warn!("{details}");
                        self.cursor.set_inflight_details(details);
                    }
                    self.cursor.update(
                        entry.slot,
                        entry.entry_index,
                        entry.start_index,
                        entry.tx_count,
                        signature.clone(),
                    );
                    if let Some(debug_sig) = self.debug_signature.as_ref() {
                        for (offset, scheduled) in entry.txs.iter().enumerate() {
                            if scheduled.tx.signatures.first() == Some(debug_sig) {
                                let tx_index = entry.start_index.saturating_add(offset);
                                let message = &scheduled.tx.message;
                                let static_keys = message.static_account_keys();
                                let mut program_ids =
                                    Vec::with_capacity(message.instructions().len());
                                for ix in message.instructions() {
                                    let program_id = static_keys
                                        .get(ix.program_id_index as usize)
                                        .map(|key| bs58::encode(key.to_bytes()).into_string())
                                        .unwrap_or_else(|| "<unknown>".to_string());
                                    program_ids.push(program_id);
                                }
                                info!(
                                    "debug tx match: slot={} entry={} tx_index={} sig={} instrs={} programs={:?}",
                                    entry.slot,
                                    entry.entry_index,
                                    tx_index,
                                    debug_sig,
                                    message.instructions().len(),
                                    program_ids
                                );
                                info!(
                                    "debug tx accounts: slot={} entry={} tx_index={} keys={}",
                                    entry.slot,
                                    entry.entry_index,
                                    tx_index,
                                    static_keys.len()
                                );
                            }
                        }
                    }
                    let txs: Vec<VersionedTransaction> = entry
                        .txs
                        .iter()
                        .map(|scheduled| scheduled.tx.clone())
                        .collect();
                    let expected_statuses: Vec<Result<(), TransactionError>> = entry
                        .txs
                        .iter()
                        .map(|scheduled| scheduled.expected_status.clone())
                        .collect();
                    self.cursor.update_inflight_stage("prepare_entry_batch");
                    let batch = match bank.prepare_entry_batch(txs) {
                        Ok(batch) => batch,
                        Err(err) => {
                            let message = format!(
                                "transaction batch prepare failed at slot {} entry {}: {}",
                                entry.slot, entry.entry_index, err
                            );
                            self.failure.record(message.clone());
                            panic!("{message}");
                        }
                    };
                    self.cursor.update_inflight_stage("load_execute_and_commit");
                    let mut timings = ExecuteTimings::default();
                    let (commit_results, _balance_collector) =
                        bank.load_execute_and_commit_transactions(
                            &batch,
                            MAX_PROCESSING_AGE,
                            ExecutionRecordingConfig::new_single_setting(false),
                            &mut timings,
                            None,
                        );
                    if entry.slot == 393_521_153 && entry.entry_index == 0 {
                        info!(
                            "entry timings: slot {} entry {} timings={:?}",
                            entry.slot, entry.entry_index, timings
                        );
                    }
                    let results: Vec<Result<(), TransactionError>> = commit_results
                        .into_iter()
                        .map(|commit_result| commit_result.and_then(|committed| committed.status))
                        .collect();
                    self.cursor.update_inflight_stage("post_process");

                    if results.len() != expected_statuses.len() {
                        let message = format!(
                            "transaction result length mismatch at slot {} entry {}: expected {} results, got {}",
                            entry.slot,
                            entry.entry_index,
                            expected_statuses.len(),
                            results.len()
                        );
                        self.failure.record(message.clone());
                        panic!("{message}");
                    }

                    for (offset, (actual, expected)) in
                        results.into_iter().zip(expected_statuses).enumerate()
                    {
                        if actual != expected {
                            let tx_index = entry.start_index.saturating_add(offset);
                            let signature = entry
                                .txs
                                .get(offset)
                                .and_then(|scheduled| scheduled.tx.signatures.first())
                                .map(|sig| sig.to_string())
                                .unwrap_or_else(|| "<missing-signature>".to_string());
                            let message = format!(
                                "transaction execution mismatch at slot {} entry {} index {} sig {}: expected {:?}, got {:?}",
                                entry.slot,
                                entry.entry_index,
                                tx_index,
                                signature,
                                expected,
                                actual
                            );
                            self.failure.record(message.clone());
                            panic!("{message}");
                        }
                    }
                    let elapsed = start.elapsed();
                    self.note_entry_duration(&entry, elapsed, signature.as_deref());
                    if elapsed >= ENTRY_EXEC_WARN_AFTER {
                        self.log_slow_entry_details(&entry);
                    }
                }
                Err(err) => {
                    log::debug!(
                        "skipping transaction execution at slot {} entry {}: {}",
                        entry.slot,
                        entry.entry_index,
                        err
                    );
                    let elapsed = start.elapsed();
                    self.note_entry_duration(&entry, elapsed, None);
                }
            }
        }
    }

    fn verify_latest_bank(&self) -> Result<(), String> {
        let bank = {
            let guard = self
                .bank_forks
                .write()
                .map_err(|_| "bank forks lock poisoned".to_string())?;
            let bank = guard.working_bank();
            bank.freeze();
            bank.clone()
        };
        if let Some(verifier) = self.snapshot_verifier.as_ref() {
            verifier.verify_bank(&bank);
        }
        Ok(())
    }
}

#[derive(Debug)]
struct ReplayProgress {
    latest_slot: AtomicU64,
    tx_count: AtomicU64,
    account_update_count: AtomicU64,
    last_tx_slot: AtomicU64,
    last_entry_slot: AtomicU64,
    last_block_meta_slot: AtomicU64,
    last_account_update_slot: AtomicU64,
}

impl ReplayProgress {
    fn new(start_slot: Slot) -> Self {
        let initial = start_slot.saturating_sub(1);
        Self {
            latest_slot: AtomicU64::new(initial),
            tx_count: AtomicU64::new(0),
            account_update_count: AtomicU64::new(0),
            last_tx_slot: AtomicU64::new(initial),
            last_entry_slot: AtomicU64::new(initial),
            last_block_meta_slot: AtomicU64::new(initial),
            last_account_update_slot: AtomicU64::new(initial),
        }
    }

    fn reset_counts(&self) {
        self.tx_count.store(0, Ordering::Relaxed);
        self.account_update_count.store(0, Ordering::Relaxed);
    }

    fn reset_last_slots(&self, slot: Slot) {
        self.latest_slot.store(slot, Ordering::Relaxed);
        self.last_tx_slot.store(slot, Ordering::Relaxed);
        self.last_entry_slot.store(slot, Ordering::Relaxed);
        self.last_block_meta_slot.store(slot, Ordering::Relaxed);
        self.last_account_update_slot
            .store(slot, Ordering::Relaxed);
    }

    fn note_slot(&self, slot: Slot) {
        Self::update_max(&self.latest_slot, slot);
    }

    fn note_tx_slot(&self, slot: Slot) {
        self.note_slot(slot);
        Self::update_max(&self.last_tx_slot, slot);
    }

    fn note_entry_slot(&self, slot: Slot) {
        self.note_slot(slot);
        Self::update_max(&self.last_entry_slot, slot);
    }

    fn note_block_meta_slot(&self, slot: Slot) {
        self.note_slot(slot);
        Self::update_max(&self.last_block_meta_slot, slot);
    }

    fn note_account_update_slot(&self, slot: Slot) {
        self.note_slot(slot);
        Self::update_max(&self.last_account_update_slot, slot);
    }

    fn update_max(target: &AtomicU64, slot: Slot) {
        let mut current = target.load(Ordering::Relaxed);
        while slot > current {
            match target.compare_exchange(current, slot, Ordering::Relaxed, Ordering::Relaxed) {
                Ok(_) => break,
                Err(next) => current = next,
            }
        }
    }

    fn inc_tx(&self) {
        self.tx_count.fetch_add(1, Ordering::Relaxed);
    }

    fn inc_account_update(&self) {
        self.account_update_count.fetch_add(1, Ordering::Relaxed);
    }
}

#[derive(Debug)]
struct ReplayFailure {
    shutdown: Arc<AtomicBool>,
    error: Mutex<Option<String>>,
}

#[derive(Debug)]
struct ReplayCursor {
    slot: AtomicU64,
    entry_index: AtomicU64,
    tx_start: AtomicU64,
    tx_count: AtomicU64,
    signature: Mutex<Option<String>>,
    inflight: Mutex<Option<InFlightEntry>>,
}

impl ReplayCursor {
    fn new() -> Self {
        Self {
            slot: AtomicU64::new(0),
            entry_index: AtomicU64::new(0),
            tx_start: AtomicU64::new(0),
            tx_count: AtomicU64::new(0),
            signature: Mutex::new(None),
            inflight: Mutex::new(None),
        }
    }

    fn update(
        &self,
        slot: Slot,
        entry_index: usize,
        tx_start: usize,
        tx_count: usize,
        signature: Option<String>,
    ) {
        self.slot.store(slot, Ordering::Relaxed);
        self.entry_index
            .store(entry_index as u64, Ordering::Relaxed);
        self.tx_start.store(tx_start as u64, Ordering::Relaxed);
        self.tx_count.store(tx_count as u64, Ordering::Relaxed);
        if let Ok(mut guard) = self.signature.lock() {
            *guard = signature;
        }
    }

    fn snapshot(&self) -> (u64, u64, u64, u64, Option<String>) {
        let slot = self.slot.load(Ordering::Relaxed);
        let entry_index = self.entry_index.load(Ordering::Relaxed);
        let tx_start = self.tx_start.load(Ordering::Relaxed);
        let tx_count = self.tx_count.load(Ordering::Relaxed);
        let signature = self.signature.lock().ok().and_then(|guard| guard.clone());
        (slot, entry_index, tx_start, tx_count, signature)
    }

    fn start_inflight(
        &self,
        slot: Slot,
        entry_index: usize,
        tx_start: usize,
        tx_count: usize,
        signature: Option<String>,
    ) {
        if let Ok(mut guard) = self.inflight.lock() {
            *guard = Some(InFlightEntry {
                slot,
                entry_index,
                tx_start,
                tx_count,
                signature,
                stage: "start",
                details: None,
                started_at: Instant::now(),
            });
        }
    }

    fn update_inflight_stage(&self, stage: &'static str) {
        if let Ok(mut guard) = self.inflight.lock() {
            if let Some(ref mut entry) = *guard {
                entry.stage = stage;
            }
        }
    }

    fn set_inflight_details(&self, details: String) {
        if let Ok(mut guard) = self.inflight.lock() {
            if let Some(ref mut entry) = *guard {
                entry.details = Some(details);
            }
        }
    }

    fn take_inflight_details(&self) -> Option<String> {
        if let Ok(mut guard) = self.inflight.lock() {
            if let Some(ref mut entry) = *guard {
                return entry.details.take();
            }
        }
        None
    }

    fn finish_inflight(&self) {
        if let Ok(mut guard) = self.inflight.lock() {
            *guard = None;
        }
    }

    fn inflight_snapshot(
        &self,
    ) -> Option<(u64, u64, u64, u64, Option<String>, &'static str, Duration)> {
        let guard = self.inflight.lock().ok()?;
        guard.as_ref().map(|entry| {
            (
                entry.slot,
                entry.entry_index as u64,
                entry.tx_start as u64,
                entry.tx_count as u64,
                entry.signature.clone(),
                entry.stage,
                entry.started_at.elapsed(),
            )
        })
    }
}

#[derive(Debug)]
struct InFlightEntry {
    slot: Slot,
    entry_index: usize,
    tx_start: usize,
    tx_count: usize,
    signature: Option<String>,
    stage: &'static str,
    details: Option<String>,
    started_at: Instant,
}

struct AbortOnErrorLogger {
    inner: env_logger::Logger,
    shutdown: Arc<AtomicBool>,
    abort_on_error: bool,
    cursor: Arc<ReplayCursor>,
}

impl log::Log for AbortOnErrorLogger {
    fn enabled(&self, metadata: &log::Metadata) -> bool {
        if metadata.level() == log::Level::Error {
            return true;
        }
        self.inner.enabled(metadata)
    }

    fn log(&self, record: &log::Record) {
        self.inner.log(record);
        if self.abort_on_error && record.level() == log::Level::Error {
            let (slot, entry_index, tx_start, tx_count, signature) = self.cursor.snapshot();
            eprintln!(
                "replay cursor at error: slot={slot} entry={entry_index} tx_start={tx_start} tx_count={tx_count} sig={}",
                signature.as_deref().unwrap_or("<unknown>")
            );
            self.shutdown.store(true, Ordering::SeqCst);
            std::process::exit(1);
        }
    }

    fn flush(&self) {}
}

fn setup_logger(shutdown: Arc<AtomicBool>, cursor: Arc<ReplayCursor>) {
    let abort_on_error = match env::var("JETSTREAMER_ABORT_ON_ERROR_LOG") {
        Ok(value) => {
            let value = value.trim().to_ascii_lowercase();
            !matches!(value.as_str(), "" | "0" | "false" | "no")
        }
        Err(_) => true,
    };
    let logger =
        env_logger::Builder::from_env(env_logger::Env::new().default_filter_or(DEFAULT_LOG_FILTER))
            .format_timestamp_nanos()
            .build();
    let max_level = logger.filter();
    let install = log::set_boxed_logger(Box::new(AbortOnErrorLogger {
        inner: logger,
        shutdown,
        abort_on_error,
        cursor,
    }));
    match install {
        Ok(()) => {
            log::set_max_level(max_level);
            eprintln!("jetstreamer logger installed (abort_on_error={abort_on_error})");
        }
        Err(_) => {
            eprintln!("jetstreamer logger already initialized; abort_on_error may be disabled");
        }
    }
}

impl ReplayFailure {
    fn new(shutdown: Arc<AtomicBool>) -> Self {
        Self {
            shutdown,
            error: Mutex::new(None),
        }
    }

    fn record(&self, message: String) {
        let mut guard = self.error.lock().expect("replay failure lock");
        if guard.is_none() {
            warn!("replay failure: {message}");
            *guard = Some(message);
            self.shutdown.store(true, Ordering::SeqCst);
        }
    }

    fn error_message(&self) -> Option<String> {
        self.error.lock().expect("replay failure lock").clone()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SlotPresenceState {
    Present,
    Missing,
}

#[derive(Debug)]
struct SlotPresenceMap {
    start: Slot,
    end_inclusive: Slot,
    states: Vec<SlotPresenceState>,
    next_present_after: Vec<Option<Slot>>,
}

impl SlotPresenceMap {
    fn state(&self, slot: Slot) -> Option<SlotPresenceState> {
        if slot < self.start || slot > self.end_inclusive {
            return None;
        }
        let idx = (slot - self.start) as usize;
        self.states.get(idx).copied()
    }

    fn is_missing(&self, slot: Slot) -> Option<bool> {
        self.state(slot)
            .map(|state| state == SlotPresenceState::Missing)
    }

    fn next_present_after(&self, slot: Slot) -> Option<Slot> {
        if slot < self.start || slot > self.end_inclusive {
            return None;
        }
        let idx = (slot - self.start) as usize;
        self.next_present_after.get(idx).copied().flatten()
    }
}

struct RipgetProgress {
    label: String,
    log_interval: Duration,
    start: Instant,
    total: AtomicU64,
    downloaded: AtomicU64,
    threads: AtomicUsize,
    last_log_ms: AtomicU64,
}

impl RipgetProgress {
    fn new(label: String, log_interval: Duration) -> Self {
        Self {
            label,
            log_interval,
            start: Instant::now(),
            total: AtomicU64::new(0),
            downloaded: AtomicU64::new(0),
            threads: AtomicUsize::new(0),
            last_log_ms: AtomicU64::new(0),
        }
    }

    fn log_maybe(&self) {
        let elapsed_ms = self.start.elapsed().as_millis() as u64;
        let min_ms = self.log_interval.as_millis() as u64;
        if min_ms == 0 {
            return;
        }
        let last = self.last_log_ms.load(Ordering::Relaxed);
        if elapsed_ms.saturating_sub(last) < min_ms {
            return;
        }
        if self
            .last_log_ms
            .compare_exchange(last, elapsed_ms, Ordering::Relaxed, Ordering::Relaxed)
            .is_err()
        {
            return;
        }
        let total = self.total.load(Ordering::Relaxed);
        let downloaded = self.downloaded.load(Ordering::Relaxed);
        let threads = self.threads.load(Ordering::Relaxed);
        let elapsed = self.start.elapsed().as_secs_f64();
        let rate = if elapsed > 0.0 {
            downloaded as f64 / elapsed
        } else {
            0.0
        };
        let percent = if total == 0 {
            0.0
        } else {
            (downloaded as f64) * 100.0 / (total as f64)
        };
        let eta = if rate > 0.0 && total > downloaded {
            let remaining = total.saturating_sub(downloaded);
            let eta_secs = (remaining as f64 / rate).ceil() as u64;
            format_eta(Duration::from_secs(eta_secs))
        } else {
            "unknown".to_string()
        };
        info!(
            "{} progress: {}/{} ({:.2}%) threads={} rate={:.1}B/s eta={}",
            self.label,
            format_bytes(downloaded),
            format_bytes(total),
            percent,
            threads,
            rate,
            eta
        );
    }
}

impl ripget::ProgressReporter for RipgetProgress {
    fn init(&self, total: u64) {
        self.total.store(total, Ordering::Relaxed);
        self.log_maybe();
    }

    fn add(&self, delta: u64) {
        self.downloaded.fetch_add(delta, Ordering::Relaxed);
        self.log_maybe();
    }

    fn set_threads(&self, threads: usize) {
        self.threads.store(threads, Ordering::Relaxed);
        self.log_maybe();
    }
}

struct LocalIndexFile {
    data: Arc<[u8]>,
}

impl LocalIndexFile {
    fn new(data: Vec<u8>) -> Self {
        Self {
            data: Arc::from(data),
        }
    }

    fn slice(&self, offset: usize, len: usize) -> Result<&[u8], String> {
        let end = offset
            .checked_add(len)
            .ok_or_else(|| format!("slice range overflowed usize (offset {offset}, len {len})"))?;
        if end > self.data.len() {
            return Err(format!(
                "slice {offset}-{end} exceeds file size {}",
                self.data.len()
            ));
        }
        Ok(&self.data[offset..end])
    }
}

struct CompactIndexHeader {
    value_size: u64,
    num_buckets: u32,
    header_size: u64,
    metadata: HashMap<Vec<u8>, Vec<u8>>,
}

impl CompactIndexHeader {
    fn metadata_epoch(&self) -> Option<u64> {
        self.metadata
            .get(METADATA_KEY_EPOCH)
            .and_then(|bytes| bytes.get(..8))
            .map(|slice| {
                let mut buf = [0u8; 8];
                buf.copy_from_slice(slice);
                u64::from_le_bytes(buf)
            })
    }
}

#[derive(Clone, Copy)]
struct BucketHeader {
    hash_domain: u32,
    num_entries: u32,
    hash_len: u8,
    file_offset: u64,
}

impl BucketHeader {
    fn from_bytes(bytes: [u8; BUCKET_HEADER_SIZE]) -> Self {
        let hash_domain = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
        let num_entries = u32::from_le_bytes(bytes[4..8].try_into().unwrap());
        let hash_len = bytes[8];
        let mut offset_bytes = [0u8; 8];
        offset_bytes[..6].copy_from_slice(&bytes[10..16]);
        let file_offset = u64::from_le_bytes(offset_bytes);
        Self {
            hash_domain,
            num_entries,
            hash_len,
            file_offset,
        }
    }
}

struct LocalSlotIndex {
    file: Arc<LocalIndexFile>,
    header: CompactIndexHeader,
    buckets: Vec<BucketHeader>,
}

impl LocalSlotIndex {
    fn from_bytes(bytes: Vec<u8>) -> Result<Self, String> {
        let file = Arc::new(LocalIndexFile::new(bytes));
        let header = parse_compact_index_header(file.data.as_ref(), SLOT_TO_CID_KIND)?;
        let mut buckets = Vec::with_capacity(header.num_buckets as usize);
        for idx in 0..header.num_buckets {
            let offset = header.header_size as usize + (idx as usize) * BUCKET_HEADER_SIZE;
            let raw = file.slice(offset, BUCKET_HEADER_SIZE)?;
            let mut buf = [0u8; BUCKET_HEADER_SIZE];
            buf.copy_from_slice(raw);
            buckets.push(BucketHeader::from_bytes(buf));
        }
        Ok(Self {
            file,
            header,
            buckets,
        })
    }

    fn contains_slot(&self, slot: u64) -> Result<bool, String> {
        let key = slot.to_le_bytes();
        let bucket_index = self.bucket_hash(&key) as usize;
        let header = self
            .buckets
            .get(bucket_index)
            .ok_or_else(|| format!("bucket index {bucket_index} out of bounds"))?;
        if header.num_entries == 0 {
            return Ok(false);
        }
        let target_hash = truncated_entry_hash(header.hash_domain, &key, header.hash_len);
        let max = header.num_entries as usize;
        let hash_len = header.hash_len as usize;
        let stride = hash_len + self.header.value_size as usize;
        let base: usize = header
            .file_offset
            .try_into()
            .map_err(|_| "bucket file offset exceeds usize".to_string())?;

        let mut index = 0usize;
        while index < max {
            let offset = base + index * stride;
            let hash_slice = self.file.slice(offset, hash_len)?;
            let hash = read_hash(hash_slice);
            if hash == target_hash {
                return Ok(true);
            }
            index = (index << 1) | 1;
            if hash < target_hash {
                index += 1;
            }
        }
        Ok(false)
    }

    fn bucket_hash(&self, key: &[u8]) -> u32 {
        let h = xxh64(key, 0);
        let n = self.header.num_buckets as u64;
        let mut u = h % n;
        if ((h - u) / n) < u {
            u = hash_uint64(u);
        }
        (u % n) as u32
    }
}

fn read_hash(bytes: &[u8]) -> u64 {
    let mut buf = 0u64;
    for (i, b) in bytes.iter().enumerate() {
        buf |= (*b as u64) << (8 * i);
    }
    buf
}

fn truncated_entry_hash(hash_domain: u32, key: &[u8], hash_len: u8) -> u64 {
    let raw = entry_hash64(hash_domain, key);
    if hash_len >= 8 {
        raw
    } else {
        let bits = (hash_len as usize) * 8;
        let mask = if bits == 64 {
            u64::MAX
        } else {
            (1u64 << bits) - 1
        };
        raw & mask
    }
}

fn entry_hash64(prefix: u32, key: &[u8]) -> u64 {
    let mut block = [0u8; HASH_PREFIX_SIZE];
    block[..4].copy_from_slice(&prefix.to_le_bytes());
    let mut data = Vec::with_capacity(HASH_PREFIX_SIZE + key.len());
    data.extend_from_slice(&block);
    data.extend_from_slice(key);
    xxh64(&data, 0)
}

const fn hash_uint64(mut x: u64) -> u64 {
    x ^= x >> 33;
    x = x.wrapping_mul(0xff51afd7ed558ccd);
    x ^= x >> 33;
    x = x.wrapping_mul(0xc4ceb9fe1a85ec53);
    x ^= x >> 33;
    x
}

fn parse_compact_index_header(
    data: &[u8],
    expected_kind: &[u8],
) -> Result<CompactIndexHeader, String> {
    if data.len() < 12 {
        return Err("index header shorter than 12 bytes".into());
    }
    if data[..8] != COMPACT_INDEX_MAGIC[..] {
        return Err("invalid compactindex magic".into());
    }
    let header_len = u32::from_le_bytes(data[8..12].try_into().unwrap()) as usize;
    let total_header_size = 8 + 4 + header_len;
    if data.len() < total_header_size {
        return Err(format!(
            "incomplete index header: expected {total_header_size} bytes, got {}",
            data.len()
        ));
    }
    let value_size = u64::from_le_bytes(data[12..20].try_into().unwrap());
    let num_buckets = u32::from_le_bytes(data[20..24].try_into().unwrap());
    let version = data[24];
    if version != 1 {
        return Err(format!("unsupported compactindex version {version}"));
    }
    let metadata_slice = &data[25..total_header_size];
    let metadata = parse_metadata(metadata_slice)?;
    if let Some(kind) = metadata.get(METADATA_KEY_KIND)
        && kind.as_slice() != expected_kind
    {
        return Err(format!(
            "wrong index kind: expected {:?}, got {:?}",
            expected_kind, kind
        ));
    }
    Ok(CompactIndexHeader {
        value_size,
        num_buckets,
        header_size: total_header_size as u64,
        metadata,
    })
}

fn parse_metadata(data: &[u8]) -> Result<HashMap<Vec<u8>, Vec<u8>>, String> {
    if data.is_empty() {
        return Ok(HashMap::new());
    }
    let mut map = HashMap::new();
    let mut offset = 0;
    let num_pairs = data[offset] as usize;
    offset += 1;
    for _ in 0..num_pairs {
        if offset >= data.len() {
            return Err("unexpected end while reading metadata key length".into());
        }
        let key_len = data[offset] as usize;
        offset += 1;
        if offset + key_len > data.len() {
            return Err("metadata key length out of bounds".into());
        }
        let key = data[offset..offset + key_len].to_vec();
        offset += key_len;
        if offset >= data.len() {
            return Err("unexpected end while reading metadata value length".into());
        }
        let value_len = data[offset] as usize;
        offset += 1;
        if offset + value_len > data.len() {
            return Err("metadata value length out of bounds".into());
        }
        let value = data[offset..offset + value_len].to_vec();
        offset += value_len;
        map.insert(key, value);
    }
    Ok(map)
}

fn decode_varint(bytes: &[u8]) -> Result<(u64, usize), String> {
    let mut value = 0u64;
    let mut shift = 0u32;
    for (idx, b) in bytes.iter().enumerate() {
        let byte = *b as u64;
        if byte < 0x80 {
            value |= byte << shift;
            return Ok((value, idx + 1));
        }
        value |= (byte & 0x7f) << shift;
        shift += 7;
        if shift > 63 {
            return Err("varint overflow".into());
        }
    }
    Err("buffer ended before varint terminated".into())
}

fn extract_root_cid(value: &Value) -> Result<Cid, String> {
    let map_entries = match value {
        Value::Map(entries) => entries,
        _ => return Err("CAR header is not a map".into()),
    };
    let roots_value = map_entries
        .iter()
        .find(|(k, _)| matches!(k, Value::Text(s) if s == "roots"))
        .map(|(_, v)| v)
        .ok_or_else(|| "CAR header missing 'roots'".to_string())?;
    let roots = match roots_value {
        Value::Array(items) => items,
        _ => return Err("CAR header 'roots' not an array".into()),
    };
    let first = roots
        .first()
        .ok_or_else(|| "CAR header 'roots' array empty".to_string())?;
    match first {
        Value::Tag(42, boxed) => match boxed.as_ref() {
            Value::Bytes(bytes) => decode_cid_bytes(bytes),
            _ => Err("CID tag did not contain bytes".into()),
        },
        Value::Bytes(bytes) => decode_cid_bytes(bytes),
        _ => Err("unexpected CID encoding in CAR header".into()),
    }
}

fn decode_cid_bytes(bytes: &[u8]) -> Result<Cid, String> {
    if bytes.is_empty() {
        return Err("CID bytes were empty".into());
    }
    let mut candidates: Vec<&[u8]> = Vec::with_capacity(2);
    if bytes[0] == 0 && bytes.len() > 1 {
        candidates.push(&bytes[1..]);
    }
    candidates.push(bytes);
    let mut last_err = None;
    for slice in candidates {
        match Cid::try_from(slice.to_vec()) {
            Ok(cid) => return Ok(cid),
            Err(err) => last_err = Some(err),
        }
    }
    Err(last_err
        .map(|err| format!("invalid CID: {err}"))
        .unwrap_or_else(|| "invalid CID bytes".into()))
}

#[derive(Debug)]
struct TransactionScheduler {
    state: Mutex<SchedulerState>,
    presence: Arc<SlotPresenceMap>,
}

#[derive(Debug)]
struct SchedulerState {
    last_finalized_slot: Slot,
    current_slot: Slot,
    slots: HashMap<Slot, SlotExecutionBuffer>,
    inferred_blocks: HashMap<Slot, (u64, u64)>,
    highest_seen_slot: Slot,
}

#[allow(dead_code)]
#[derive(Debug)]
struct SlotBufferSnapshot {
    expected_tx_count: Option<u64>,
    expected_entry_count: Option<u64>,
    processed_tx_count: u64,
    processed_entry_count: u64,
    pending_entries: usize,
    buffered_txs: usize,
    next_entry_index: usize,
}

#[derive(Debug)]
struct SchedulerSnapshot {
    current_slot: Slot,
    last_finalized_slot: Slot,
    buffered_slots: usize,
    highest_seen_slot: Slot,
    presence: Option<SlotPresenceState>,
    buffer: Option<SlotBufferSnapshot>,
}

impl TransactionScheduler {
    fn new(start_slot: Slot, presence: Arc<SlotPresenceMap>) -> Self {
        Self {
            state: Mutex::new(SchedulerState {
                last_finalized_slot: start_slot.saturating_sub(1),
                current_slot: start_slot,
                slots: HashMap::new(),
                inferred_blocks: HashMap::new(),
                highest_seen_slot: start_slot.saturating_sub(1),
            }),
            presence,
        }
    }

    fn insert_transaction(
        &self,
        slot: Slot,
        index: usize,
        tx: VersionedTransaction,
        expected_status: Result<(), TransactionError>,
    ) -> Result<Vec<ReadyEntry>, String> {
        let mut state = self.state.lock().expect("transaction scheduler lock");
        if slot <= state.last_finalized_slot {
            return Err(format!(
                "late transaction for slot {slot} (last finalized slot {})",
                state.last_finalized_slot
            ));
        }
        if slot > state.highest_seen_slot {
            state.highest_seen_slot = slot;
        }
        let buffer = state
            .slots
            .entry(slot)
            .or_insert_with(SlotExecutionBuffer::default);
        buffer.insert_transaction(index, tx, expected_status)?;
        self.advance_ready_locked(&mut state)
    }

    fn push_entry(
        &self,
        slot: Slot,
        entry_index: usize,
        start_index: usize,
        tx_count: usize,
        hash: Hash,
    ) -> Result<Vec<ReadyEntry>, String> {
        let mut state = self.state.lock().expect("transaction scheduler lock");
        if slot <= state.last_finalized_slot {
            return Err(format!(
                "late entry for slot {slot} (last finalized slot {})",
                state.last_finalized_slot
            ));
        }
        if slot > state.highest_seen_slot {
            state.highest_seen_slot = slot;
        }
        let buffer = state
            .slots
            .entry(slot)
            .or_insert_with(SlotExecutionBuffer::default);
        buffer.push_entry(entry_index, start_index, tx_count, hash)?;
        self.advance_ready_locked(&mut state)
    }

    fn record_block_metadata(
        &self,
        slot: Slot,
        expected_tx_count: u64,
        expected_entry_count: u64,
    ) -> Result<Vec<ReadyEntry>, String> {
        let mut state = self.state.lock().expect("transaction scheduler lock");
        if slot <= state.last_finalized_slot {
            if let Some((inferred_tx, inferred_entry)) = state.inferred_blocks.remove(&slot) {
                if inferred_tx == expected_tx_count && inferred_entry == expected_entry_count {
                    return Ok(Vec::new());
                }
                return Err(format!(
                    "late block metadata mismatch for slot {slot}: inferred txs {inferred_tx} entries {inferred_entry}, got txs {expected_tx_count} entries {expected_entry_count}"
                ));
            }
            return Err(format!(
                "late block metadata for slot {slot} (last finalized slot {})",
                state.last_finalized_slot
            ));
        }
        if slot > state.highest_seen_slot {
            state.highest_seen_slot = slot;
        }
        if let Some((inferred_tx, inferred_entry)) = state.inferred_blocks.remove(&slot) {
            if inferred_tx != expected_tx_count || inferred_entry != expected_entry_count {
                return Err(format!(
                    "block metadata mismatch for slot {slot}: inferred txs {inferred_tx} entries {inferred_entry}, got txs {expected_tx_count} entries {expected_entry_count}"
                ));
            }
        }
        let buffer = state
            .slots
            .entry(slot)
            .or_insert_with(SlotExecutionBuffer::default);
        buffer.set_expected_counts(expected_tx_count, expected_entry_count)?;
        self.advance_ready_locked(&mut state)
    }

    fn drain_ready_entries(&self) -> Result<Vec<ReadyEntry>, String> {
        let mut state = self.state.lock().expect("transaction scheduler lock");
        self.advance_ready_locked(&mut state)
    }

    fn verify_complete(&self, end_inclusive: Slot) -> Result<(), String> {
        let state = self.state.lock().expect("transaction scheduler lock");
        for (slot, buffer) in state.slots.iter() {
            if *slot <= end_inclusive && buffer.has_any_data() {
                return Err(format!(
                    "replay incomplete: slot {slot} still has buffered data"
                ));
            }
            if *slot > end_inclusive && buffer.has_any_data() {
                return Err(format!(
                    "replay received data for slot {slot} beyond end slot {end_inclusive}"
                ));
            }
        }

        let mut slot = state.current_slot;
        while slot <= end_inclusive {
            match self.presence.state(slot) {
                Some(SlotPresenceState::Missing) => slot = slot.saturating_add(1),
                Some(SlotPresenceState::Present) => {
                    return Err(format!(
                        "replay incomplete: missing block data for slot {slot}"
                    ));
                }
                None => break,
            }
        }

        Ok(())
    }

    fn snapshot(&self) -> SchedulerSnapshot {
        let state = self.state.lock().expect("transaction scheduler lock");
        let current_slot = state.current_slot;
        let presence = self.presence.state(current_slot);
        let buffer = state
            .slots
            .get(&current_slot)
            .map(|slot_buffer| SlotBufferSnapshot {
                expected_tx_count: slot_buffer.expected_tx_count,
                expected_entry_count: slot_buffer.expected_entry_count,
                processed_tx_count: slot_buffer.processed_tx_count,
                processed_entry_count: slot_buffer.processed_entry_count,
                pending_entries: slot_buffer.pending_entries.len(),
                buffered_txs: slot_buffer.buffered_transaction_count(),
                next_entry_index: slot_buffer.next_entry_index,
            });
        SchedulerSnapshot {
            current_slot,
            last_finalized_slot: state.last_finalized_slot,
            buffered_slots: state.slots.len(),
            highest_seen_slot: state.highest_seen_slot,
            presence,
            buffer,
        }
    }

    fn expected_block_metadata_after(&self, slot: Slot) -> Option<Slot> {
        self.presence.next_present_after(slot)
    }

    fn advance_ready_locked(&self, state: &mut SchedulerState) -> Result<Vec<ReadyEntry>, String> {
        let mut ready = Vec::new();
        loop {
            let current_slot = state.current_slot;
            match self.presence.is_missing(current_slot) {
                Some(true) => {
                    if let Some(buffer) = state.slots.remove(&current_slot) {
                        if buffer.has_any_data() {
                            return Err(format!(
                                "slot {} marked leader skipped but contains buffered data",
                                current_slot
                            ));
                        }
                    }
                    state.last_finalized_slot = current_slot;
                    state.current_slot = current_slot.saturating_add(1);
                    continue;
                }
                Some(false) => {}
                None => break,
            }

            let Some(buffer) = state.slots.get_mut(&current_slot) else {
                break;
            };

            let mut drained = buffer.drain_ready_entries(current_slot)?;
            ready.append(&mut drained);
            if state.highest_seen_slot > current_slot {
                if let Some((txs, entries)) = buffer.infer_expected_counts_if_missing(current_slot) {
                    state.inferred_blocks.insert(current_slot, (txs, entries));
                }
            }

            let should_finalize = buffer.should_finalize(current_slot)?;
            if !should_finalize {
                break;
            }

            state.slots.remove(&current_slot);
            state.last_finalized_slot = current_slot;
            state.current_slot = current_slot.saturating_add(1);
        }

        Ok(ready)
    }
}

#[derive(Debug, Default)]
struct SlotExecutionBuffer {
    txs: Vec<Option<ScheduledTransaction>>,
    pending_entries: VecDeque<PendingEntry>,
    next_entry_index: usize,
    processed_tx_count: u64,
    processed_entry_count: u64,
    expected_tx_count: Option<u64>,
    expected_entry_count: Option<u64>,
}

impl SlotExecutionBuffer {
    fn infer_expected_counts_if_missing(&mut self, slot: Slot) -> Option<(u64, u64)> {
        if self.expected_tx_count.is_some() || self.expected_entry_count.is_some() {
            return None;
        }
        if self.processed_tx_count == 0 && self.processed_entry_count == 0 {
            return None;
        }
        if !self.pending_entries.is_empty() || self.buffered_transaction_count() > 0 {
            return None;
        }
        self.expected_tx_count = Some(self.processed_tx_count);
        self.expected_entry_count = Some(self.processed_entry_count);
        warn!(
            "missing block metadata: inferring expected counts for slot {} txs={} entries={}",
            slot, self.processed_tx_count, self.processed_entry_count
        );
        Some((self.processed_tx_count, self.processed_entry_count))
    }

    fn set_expected_counts(
        &mut self,
        expected_tx_count: u64,
        expected_entry_count: u64,
    ) -> Result<(), String> {
        match (self.expected_tx_count, self.expected_entry_count) {
            (Some(existing_tx), Some(existing_entry))
                if existing_tx != expected_tx_count || existing_entry != expected_entry_count =>
            {
                return Err(format!(
                    "block metadata mismatch: expected txs {existing_tx} entries {existing_entry}, got txs {expected_tx_count} entries {expected_entry_count}"
                ));
            }
            (Some(_), Some(_)) => {}
            _ => {
                self.expected_tx_count = Some(expected_tx_count);
                self.expected_entry_count = Some(expected_entry_count);
            }
        }
        Ok(())
    }

    fn insert_transaction(
        &mut self,
        index: usize,
        tx: VersionedTransaction,
        expected_status: Result<(), TransactionError>,
    ) -> Result<(), String> {
        if self.txs.len() <= index {
            self.txs.resize_with(index + 1, || None);
        }
        if self.txs[index].is_some() {
            return Err(format!("duplicate transaction at index {index}"));
        }
        self.txs[index] = Some(ScheduledTransaction {
            tx,
            expected_status,
        });
        Ok(())
    }

    fn push_entry(
        &mut self,
        entry_index: usize,
        start_index: usize,
        tx_count: usize,
        hash: Hash,
    ) -> Result<(), String> {
        if entry_index != self.next_entry_index {
            return Err(format!(
                "entry index out of order: expected {}, got {}",
                self.next_entry_index, entry_index
            ));
        }
        self.next_entry_index = self.next_entry_index.saturating_add(1);
        self.pending_entries.push_back(PendingEntry {
            entry_index,
            start_index,
            tx_count,
            hash,
        });
        Ok(())
    }

    fn drain_ready_entries(&mut self, slot: Slot) -> Result<Vec<ReadyEntry>, String> {
        let mut ready = Vec::new();
        loop {
            let Some(entry) = self.pending_entries.front() else {
                break;
            };
            if entry.tx_count == 0 {
                let entry = self.pending_entries.pop_front().expect("pending entry");
                self.processed_entry_count = self.processed_entry_count.saturating_add(1);
                ready.push(ReadyEntry {
                    slot,
                    entry_index: entry.entry_index,
                    start_index: entry.start_index,
                    txs: Vec::new(),
                    hash: entry.hash,
                    tx_count: 0,
                });
                continue;
            }

            let end = entry.start_index.saturating_add(entry.tx_count);
            if self.txs.len() < end {
                break;
            }
            let mut missing = false;
            for idx in entry.start_index..end {
                if self.txs[idx].is_none() {
                    missing = true;
                    break;
                }
            }
            if missing {
                break;
            }

            let entry = self.pending_entries.pop_front().expect("pending entry");
            let mut txs = Vec::with_capacity(entry.tx_count);
            for idx in entry.start_index..end {
                let tx = self.txs[idx].take().expect("transaction present");
                txs.push(tx);
            }
            self.processed_entry_count = self.processed_entry_count.saturating_add(1);
            self.processed_tx_count = self
                .processed_tx_count
                .saturating_add(entry.tx_count as u64);
            ready.push(ReadyEntry {
                slot,
                entry_index: entry.entry_index,
                start_index: entry.start_index,
                txs,
                hash: entry.hash,
                tx_count: entry.tx_count,
            });
        }
        Ok(ready)
    }

    fn buffered_transaction_count(&self) -> usize {
        self.txs.iter().filter(|value| value.is_some()).count()
    }

    fn has_any_data(&self) -> bool {
        self.expected_tx_count.is_some()
            || self.expected_entry_count.is_some()
            || !self.pending_entries.is_empty()
            || self.txs.iter().any(|value| value.is_some())
            || self.processed_tx_count > 0
            || self.processed_entry_count > 0
    }

    fn should_finalize(&self, slot: Slot) -> Result<bool, String> {
        let (Some(expected_tx), Some(expected_entry)) =
            (self.expected_tx_count, self.expected_entry_count)
        else {
            return Ok(false);
        };

        if self.processed_tx_count > expected_tx || self.processed_entry_count > expected_entry {
            return Err(format!(
                "slot {} replay mismatch: processed txs {}/{} entries {}/{}",
                slot,
                self.processed_tx_count,
                expected_tx,
                self.processed_entry_count,
                expected_entry
            ));
        }

        let pending_entries = self.pending_entries.len();
        let buffered_transactions = self.buffered_transaction_count();
        if self.processed_tx_count == expected_tx && self.processed_entry_count == expected_entry {
            if pending_entries > 0 || buffered_transactions > 0 {
                return Err(format!(
                    "slot {} replay mismatch: processed txs {}/{} entries {}/{} pending_entries={} buffered_txs={}",
                    slot,
                    self.processed_tx_count,
                    expected_tx,
                    self.processed_entry_count,
                    expected_entry,
                    pending_entries,
                    buffered_transactions,
                ));
            }
            return Ok(true);
        }

        Ok(false)
    }
}

#[derive(Debug)]
struct PendingEntry {
    entry_index: usize,
    start_index: usize,
    tx_count: usize,
    hash: Hash,
}

#[derive(Debug)]
struct ScheduledTransaction {
    tx: VersionedTransaction,
    expected_status: Result<(), TransactionError>,
}

#[derive(Debug)]
struct ReadyEntry {
    slot: Slot,
    entry_index: usize,
    start_index: usize,
    txs: Vec<ScheduledTransaction>,
    hash: Hash,
    tx_count: usize,
}

#[derive(Debug)]
struct ProgressAccountsUpdateNotifier {
    progress: Arc<ReplayProgress>,
    delegate: Option<AccountsUpdateNotifier>,
    live_start_slot: Slot,
}

impl AccountsUpdateNotifierInterface for ProgressAccountsUpdateNotifier {
    fn snapshot_notifications_enabled(&self) -> bool {
        self.delegate
            .as_ref()
            .map(|delegate| delegate.snapshot_notifications_enabled())
            .unwrap_or(false)
    }

    fn notify_account_update(
        &self,
        slot: Slot,
        account: &AccountSharedData,
        txn: &Option<&SanitizedTransaction>,
        pubkey: &solana_pubkey::Pubkey,
        write_version: u64,
    ) {
        if !LOGGED_FIRST_ACCOUNT_UPDATE.swap(true, Ordering::SeqCst) {
            info!(
                "first account update: slot={} pubkey={} write_version={}",
                slot,
                pubkey,
                write_version
            );
        }
        self.progress.note_account_update_slot(slot);
        self.progress.inc_account_update();
        if slot >= self.live_start_slot {
            if let Some(delegate) = self.delegate.as_ref() {
                delegate.notify_account_update(slot, account, txn, pubkey, write_version);
            }
        }
    }

    fn notify_account_restore_from_snapshot(
        &self,
        slot: Slot,
        write_version: u64,
        account: &AccountForGeyser<'_>,
    ) {
        if let Some(delegate) = self.delegate.as_ref() {
            delegate.notify_account_restore_from_snapshot(slot, write_version, account);
        }
    }

    fn notify_end_of_restore_from_snapshot(&self) {
        if let Some(delegate) = self.delegate.as_ref() {
            delegate.notify_end_of_restore_from_snapshot();
        }
    }
}

struct BankTransactionNotifier {
    progress: Arc<ReplayProgress>,
    scheduler: Arc<TransactionScheduler>,
    failure: Arc<ReplayFailure>,
    ready_sender: crossbeam_channel::Sender<Vec<ReadyEntry>>,
    delegate: Option<Arc<dyn TransactionNotifier + Send + Sync + 'static>>,
    live_start_slot: Slot,
}

impl TransactionNotifier for BankTransactionNotifier {
    fn notify_transaction(
        &self,
        slot: Slot,
        transaction_slot_index: usize,
        signature: &Signature,
        message_hash: &Hash,
        is_vote: bool,
        transaction_status_meta: &TransactionStatusMeta,
        transaction: &VersionedTransaction,
    ) {
        self.progress.note_tx_slot(slot);
        self.progress.inc_tx();
        match self.scheduler.insert_transaction(
            slot,
            transaction_slot_index,
            transaction.clone(),
            transaction_status_meta.status.clone(),
        ) {
            Ok(ready_entries) => {
                if !ready_entries.is_empty() {
                    if let Err(err) = self.ready_sender.send(ready_entries) {
                        self.failure
                            .record(format!("ready entry channel closed: {err}"));
                    }
                }
            }
            Err(err) => self.failure.record(err),
        }

        if let Some(delegate) = self.delegate.as_ref() {
            if slot >= self.live_start_slot {
                delegate.notify_transaction(
                    slot,
                    transaction_slot_index,
                    signature,
                    message_hash,
                    is_vote,
                    transaction_status_meta,
                    transaction,
                );
            }
        }
    }
}

struct BankEntryNotifier {
    progress: Arc<ReplayProgress>,
    scheduler: Arc<TransactionScheduler>,
    failure: Arc<ReplayFailure>,
    ready_sender: crossbeam_channel::Sender<Vec<ReadyEntry>>,
    delegate: Option<Arc<dyn EntryNotifier + Send + Sync + 'static>>,
    live_start_slot: Slot,
}

impl EntryNotifier for BankEntryNotifier {
    fn notify_entry(
        &self,
        slot: Slot,
        index: usize,
        entry: &solana_entry::entry::EntrySummary,
        starting_transaction_index: usize,
    ) {
        self.progress.note_entry_slot(slot);
        match self.scheduler.push_entry(
            slot,
            index,
            starting_transaction_index,
            entry.num_transactions as usize,
            entry.hash,
        ) {
            Ok(ready_entries) => {
                if !ready_entries.is_empty() {
                    if let Err(err) = self.ready_sender.send(ready_entries) {
                        self.failure
                            .record(format!("ready entry channel closed: {err}"));
                    }
                }
            }
            Err(err) => self.failure.record(err),
        }

        if let Some(delegate) = self.delegate.as_ref() {
            if slot >= self.live_start_slot {
                delegate.notify_entry(slot, index, entry, starting_transaction_index);
            }
        }
    }
}

struct BankBlockMetadataNotifier {
    scheduler: Arc<TransactionScheduler>,
    progress: Arc<ReplayProgress>,
    failure: Arc<ReplayFailure>,
    ready_sender: crossbeam_channel::Sender<Vec<ReadyEntry>>,
    delegate: Option<BlockMetadataNotifierArc>,
    live_start_slot: Slot,
}

impl BlockMetadataNotifier for BankBlockMetadataNotifier {
    fn notify_block_metadata(
        &self,
        parent_slot: u64,
        parent_blockhash: &str,
        slot: u64,
        blockhash: &str,
        rewards: &solana_runtime::bank::KeyedRewardsAndNumPartitions,
        block_time: Option<solana_clock::UnixTimestamp>,
        block_height: Option<u64>,
        executed_transaction_count: u64,
        entry_count: u64,
    ) {
        if slot == u64::MAX {
            if let Some(delegate) = self.delegate.as_ref() {
                delegate.notify_block_metadata(
                    parent_slot,
                    parent_blockhash,
                    slot,
                    blockhash,
                    rewards,
                    block_time,
                    block_height,
                    executed_transaction_count,
                    entry_count,
                );
            }
            return;
        }
        self.progress.note_block_meta_slot(slot);
        match self
            .scheduler
            .record_block_metadata(slot, executed_transaction_count, entry_count)
        {
            Ok(ready_entries) => {
                if !ready_entries.is_empty() {
                    if let Err(err) = self.ready_sender.send(ready_entries) {
                        self.failure
                            .record(format!("ready entry channel closed: {err}"));
                    }
                }
            }
            Err(err) => self.failure.record(err),
        }

        if let Some(delegate) = self.delegate.as_ref() {
            if slot >= self.live_start_slot {
                delegate.notify_block_metadata(
                    parent_slot,
                    parent_blockhash,
                    slot,
                    blockhash,
                    rewards,
                    block_time,
                    block_height,
                    executed_transaction_count,
                    entry_count,
                );
            }
        }
    }
}

fn format_eta(duration: Duration) -> String {
    let total_secs = duration.as_secs();
    let hours = total_secs / 3600;
    let minutes = (total_secs % 3600) / 60;
    let seconds = total_secs % 60;
    format!("{hours:02}:{minutes:02}:{seconds:02}")
}

fn format_bytes(bytes: u64) -> String {
    const KB: f64 = 1024.0;
    const MB: f64 = KB * 1024.0;
    const GB: f64 = MB * 1024.0;
    let value = bytes as f64;
    if value >= GB {
        format!("{:.2}GiB", value / GB)
    } else if value >= MB {
        format!("{:.2}MiB", value / MB)
    } else if value >= KB {
        format!("{:.2}KiB", value / KB)
    } else {
        format!("{bytes}B")
    }
}

fn epoch_to_slot(epoch: u64) -> u64 {
    epoch_to_slot_range(epoch).0
}

fn usage(program: &str) -> String {
    format!("Usage: {program} <epoch> [dest-dir] [--verify]")
}

fn snapshot_filename(uri: &str) -> Result<&str, String> {
    uri.rsplit('/')
        .next()
        .filter(|name| !name.is_empty())
        .ok_or_else(|| format!("snapshot uri missing filename: {uri}"))
}

fn parse_snapshot_archive_name(name: &str) -> Result<(Slot, SnapshotHash), String> {
    let name = name
        .strip_prefix("snapshot-")
        .ok_or_else(|| format!("snapshot filename missing prefix: {name}"))?;
    let (slot_str, rest) = name
        .split_once('-')
        .ok_or_else(|| format!("snapshot filename missing slot/hash separator: {name}"))?;
    let slot: Slot = slot_str
        .parse()
        .map_err(|err| format!("invalid snapshot slot '{slot_str}': {err}"))?;
    let hash_str = rest
        .strip_suffix(".tar.zst")
        .or_else(|| rest.strip_suffix(".tar.lz4"))
        .or_else(|| rest.strip_suffix(".tar.bz2"))
        .ok_or_else(|| format!("snapshot filename missing archive extension: {name}"))?;
    let hash: Hash = hash_str
        .parse()
        .map_err(|err| format!("invalid snapshot hash '{hash_str}': {err}"))?;
    Ok((slot, SnapshotHash(hash)))
}

struct SnapshotArchiveCandidate {
    path: PathBuf,
    slot: Slot,
}

fn find_existing_snapshot_archive(
    dest_dir: &Path,
    target_slot: Slot,
) -> Result<Option<SnapshotArchiveCandidate>, String> {
    if !dest_dir.is_dir() {
        return Ok(None);
    }
    let read_dir = fs::read_dir(dest_dir)
        .map_err(|err| format!("failed to read {}: {err}", dest_dir.display()))?;
    let mut best: Option<SnapshotArchiveCandidate> = None;
    for entry in read_dir {
        let entry = entry.map_err(|err| format!("failed to read dir entry: {err}"))?;
        let file_type = entry
            .file_type()
            .map_err(|err| format!("failed to read file type: {err}"))?;
        if !file_type.is_file() {
            continue;
        }
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        let (slot, _) = match parse_snapshot_archive_name(name) {
            Ok(parsed) => parsed,
            Err(_) => continue,
        };
        if slot > target_slot {
            continue;
        }
        let metadata = entry
            .metadata()
            .map_err(|err| format!("failed to read metadata for {}: {err}", name))?;
        if metadata.len() == 0 {
            continue;
        }
        let candidate = SnapshotArchiveCandidate {
            path: entry.path(),
            slot,
        };
        match best.as_ref() {
            Some(current) if current.slot >= candidate.slot => {}
            _ => best = Some(candidate),
        }
    }
    Ok(best)
}

fn has_extracted_snapshot(dest_dir: &Path, slot: Slot) -> Result<bool, String> {
    let snapshots_dir = dest_dir.join(BANK_SNAPSHOTS_DIR);
    let slot_dir = snapshots_dir.join(slot.to_string());
    if !slot_dir.is_dir() {
        return Ok(false);
    }
    let slot_has_files = slot_dir
        .read_dir()
        .map_err(|err| format!("failed to read {}: {err}", slot_dir.display()))?
        .next()
        .is_some();
    if !slot_has_files {
        return Ok(false);
    }
    let accounts_dir = dest_dir.join("accounts");
    if !accounts_dir.is_dir() {
        return Ok(false);
    }
    let snapshot_accounts_dir = accounts_dir
        .join(ACCOUNTS_SNAPSHOT_DIR)
        .join(slot.to_string());
    if snapshot_accounts_dir.is_dir() {
        let has_files = snapshot_accounts_dir
            .read_dir()
            .map_err(|err| format!("failed to read {}: {err}", snapshot_accounts_dir.display()))?
            .next()
            .is_some();
        if has_files {
            return Ok(true);
        }
    }
    let read_dir = fs::read_dir(&accounts_dir)
        .map_err(|err| format!("failed to read {}: {err}", accounts_dir.display()))?;
    for entry in read_dir {
        let entry = entry.map_err(|err| format!("failed to read dir entry: {err}"))?;
        let file_type = entry
            .file_type()
            .map_err(|err| format!("failed to read file type: {err}"))?;
        if !file_type.is_file() {
            continue;
        }
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if looks_like_appendvec(name) {
            return Ok(true);
        }
    }
    Ok(false)
}

async fn snapshot_expectations_for_epoch(
    epoch: u64,
) -> Result<BTreeMap<Slot, SnapshotHash>, String> {
    let snapshots = list_epoch_snapshots(epoch)
        .await
        .map_err(|err| format!("failed to list epoch {epoch} snapshots: {err}"))?;
    let mut expected = BTreeMap::new();
    for snapshot in snapshots {
        let name = snapshot_filename(&snapshot.snapshot_uri)?;
        let (slot, hash) = parse_snapshot_archive_name(name)?;
        if slot != snapshot.slot_dir {
            return Err(format!(
                "snapshot filename slot {slot} does not match directory {}",
                snapshot.slot_dir
            ));
        }
        if expected.insert(slot, hash).is_some() {
            return Err(format!("duplicate snapshot entry for slot {slot}"));
        }
    }
    Ok(expected)
}

fn plugin_library_filename() -> String {
    if cfg!(target_os = "windows") {
        format!("{PLUGIN_LIB_BASENAME}.dll")
    } else if cfg!(target_os = "macos") {
        format!("lib{PLUGIN_LIB_BASENAME}.dylib")
    } else {
        format!("lib{PLUGIN_LIB_BASENAME}.so")
    }
}

fn suggested_plugin_build_command(profile_dir: &Path) -> String {
    let profile = profile_dir.file_name().and_then(|name| name.to_str());
    if matches!(profile, Some("release")) {
        "cargo build -p jetstreamer-node-geyser --release".to_string()
    } else {
        "cargo build -p jetstreamer-node-geyser".to_string()
    }
}

fn plugin_library_path() -> Result<PathBuf, String> {
    if let Ok(path) = env::var("JETSTREAMER_NODE_GEYSER_LIB") {
        return Ok(PathBuf::from(path));
    }

    let exe = env::current_exe()
        .map_err(|err| format!("failed to read current executable path: {err}"))?;
    let profile_dir = exe
        .parent()
        .ok_or_else(|| "failed to resolve executable directory".to_string())?;
    let lib_path = profile_dir.join(plugin_library_filename());
    if lib_path.exists() {
        return Ok(lib_path);
    }

    Err(format!(
        "geyser plugin library not found at {} (build with `{}`)",
        lib_path.display(),
        suggested_plugin_build_command(profile_dir),
    ))
}

fn write_geyser_config(dest_dir: &Path, libpath: &Path) -> Result<PathBuf, String> {
    let config_path = dest_dir.join("jetstreamer-node-geyser.json");
    let config = serde_json::json!({
        "libpath": libpath.display().to_string(),
        "name": PLUGIN_NAME,
    });
    let contents =
        serde_json::to_string_pretty(&config).map_err(|err| format!("config json error: {err}"))?;
    fs::write(&config_path, contents).map_err(|err| format!("failed to write config: {err}"))?;
    Ok(config_path)
}

async fn ensure_genesis_archive(ledger_dir: &Path) -> Result<(), String> {
    let genesis_bin = ledger_dir.join("genesis.bin");
    let genesis_archive = ledger_dir.join(GENESIS_ARCHIVE);
    if genesis_bin.exists() || genesis_archive.exists() {
        return Ok(());
    }

    let uri = format!("{DEFAULT_BUCKET}/{GENESIS_ARCHIVE}");
    let status = Command::new("gcloud")
        .arg("storage")
        .arg("cp")
        .arg(&uri)
        .arg(&genesis_archive)
        .env("CLOUDSDK_CORE_DISABLE_PROMPTS", "1")
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .status()
        .await
        .map_err(|err| format!("failed to run gcloud: {err}"))?;

    if !status.success() {
        return Err(format!(
            "gcloud storage cp {uri} {} failed",
            genesis_archive.display(),
        ));
    }

    Ok(())
}

fn account_run_paths_from_snapshot(bank_snapshot_dir: &Path) -> Result<Vec<PathBuf>, String> {
    let hardlinks_dir = bank_snapshot_dir.join(ACCOUNTS_HARDLINKS_DIR);
    let read_dir = fs::read_dir(&hardlinks_dir)
        .map_err(|err| format!("failed to read {}: {err}", hardlinks_dir.display()))?;

    let mut run_paths = Vec::new();
    for entry in read_dir {
        let entry = entry.map_err(|err| format!("failed to read dir entry: {err}"))?;
        let symlink_path = entry.path();
        let link_target = fs::read_link(&symlink_path)
            .map_err(|err| format!("failed to read link {}: {err}", symlink_path.display()))?;
        let target = if link_target.is_absolute() {
            link_target
        } else {
            let parent = symlink_path
                .parent()
                .ok_or_else(|| "missing symlink parent".to_string())?;
            parent.join(link_target)
        };
        let run_path = target
            .parent()
            .and_then(|parent| parent.parent())
            .ok_or_else(|| format!("invalid account snapshot path {}", target.display()))?
            .join("run");
        run_paths.push(run_path);
    }

    run_paths.sort();
    run_paths.dedup();
    if run_paths.is_empty() {
        return Err(format!(
            "no account paths found under {}",
            hardlinks_dir.display()
        ));
    }
    Ok(run_paths)
}

fn link_or_copy(src: &Path, dest: &Path) -> Result<(), String> {
    if let Err(err) = fs::hard_link(src, dest) {
        fs::copy(src, dest)
            .map_err(|copy_err| {
                format!(
                    "failed to link {} -> {}: {err}; copy failed: {copy_err}",
                    src.display(),
                    dest.display()
                )
            })
            .map(|_| ())?;
    }
    Ok(())
}

#[cfg(unix)]
fn symlink_dir(src: &Path, dest: &Path) -> Result<(), String> {
    std::os::unix::fs::symlink(src, dest).map_err(|err| {
        format!(
            "failed to symlink {} -> {}: {err}",
            src.display(),
            dest.display()
        )
    })
}

#[cfg(windows)]
fn symlink_dir(src: &Path, dest: &Path) -> Result<(), String> {
    std::os::windows::fs::symlink_dir(src, dest).map_err(|err| {
        format!(
            "failed to symlink {} -> {}: {err}",
            src.display(),
            dest.display()
        )
    })
}

fn ensure_snapshot_meta_files(ledger_dir: &Path) -> Result<bool, String> {
    let snapshots_dir = ledger_dir.join(BANK_SNAPSHOTS_DIR);
    if !snapshots_dir.is_dir() {
        return Ok(false);
    }

    let version_src = [
        ledger_dir.join(SNAPSHOT_VERSION_FILE),
        snapshots_dir.join(SNAPSHOT_VERSION_FILE),
    ]
    .into_iter()
    .find(|path| path.exists())
    .ok_or_else(|| {
        format!(
            "missing snapshot version file under {}",
            ledger_dir.display()
        )
    })?;
    let status_src = snapshots_dir.join(SNAPSHOT_STATUS_CACHE_FILE);
    if !status_src.exists() {
        return Err(format!(
            "missing snapshot status cache file {}",
            status_src.display()
        ));
    }

    let mut slot_dirs = Vec::new();
    let read_dir = fs::read_dir(&snapshots_dir)
        .map_err(|err| format!("failed to read {}: {err}", snapshots_dir.display()))?;
    for entry in read_dir {
        let entry = entry.map_err(|err| format!("failed to read dir entry: {err}"))?;
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if name.parse::<u64>().is_ok() {
            slot_dirs.push(path);
        }
    }

    if slot_dirs.is_empty() {
        return Ok(false);
    }

    let mut changed = false;
    for slot_dir in slot_dirs {
        let version_dest = slot_dir.join(SNAPSHOT_VERSION_FILE);
        if !version_dest.exists() {
            link_or_copy(&version_src, &version_dest)?;
            changed = true;
        }
        let status_dest = slot_dir.join(SNAPSHOT_STATUS_CACHE_FILE);
        if !status_dest.exists() {
            link_or_copy(&status_src, &status_dest)?;
            changed = true;
        }
    }

    Ok(changed)
}

fn ensure_accounts_hardlinks(
    ledger_dir: &Path,
    bank_snapshot: &snapshot_utils::BankSnapshotInfo,
) -> Result<bool, String> {
    let hardlinks_dir = bank_snapshot.snapshot_dir.join(ACCOUNTS_HARDLINKS_DIR);
    let mut needs_cleanup = false;
    if hardlinks_dir.is_dir() {
        let entries = fs::read_dir(&hardlinks_dir)
            .map_err(|err| format!("failed to read {}: {err}", hardlinks_dir.display()))?;
        for entry in entries {
            let entry = entry.map_err(|err| format!("failed to read dir entry: {err}"))?;
            let file_type = entry
                .file_type()
                .map_err(|err| format!("failed to read file type: {err}"))?;
            if !file_type.is_symlink() {
                needs_cleanup = true;
                break;
            }
            let link_target = fs::read_link(entry.path())
                .map_err(|err| format!("failed to read link {}: {err}", entry.path().display()))?;
            if link_target.is_relative() || !link_target.exists() {
                needs_cleanup = true;
                break;
            }
        }
    } else {
        fs::create_dir_all(&hardlinks_dir)
            .map_err(|err| format!("failed to create {}: {err}", hardlinks_dir.display()))?;
        needs_cleanup = true;
    }

    if needs_cleanup {
        let entries = fs::read_dir(&hardlinks_dir)
            .map_err(|err| format!("failed to read {}: {err}", hardlinks_dir.display()))?;
        for entry in entries {
            let entry = entry.map_err(|err| format!("failed to read dir entry: {err}"))?;
            let path = entry.path();
            let file_type = entry
                .file_type()
                .map_err(|err| format!("failed to read file type: {err}"))?;
            if file_type.is_dir() && !file_type.is_symlink() {
                fs::remove_dir_all(&path)
                    .map_err(|err| format!("failed to remove {}: {err}", path.display()))?;
            } else {
                fs::remove_file(&path)
                    .map_err(|err| format!("failed to remove {}: {err}", path.display()))?;
            }
        }
    }

    let slot_dir = bank_snapshot.slot.to_string();
    let mut account_paths = Vec::new();
    let mut changed = false;
    let read_dir = fs::read_dir(ledger_dir)
        .map_err(|err| format!("failed to read {}: {err}", ledger_dir.display()))?;
    for entry in read_dir {
        let entry = entry.map_err(|err| format!("failed to read dir entry: {err}"))?;
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        if path == ledger_dir.join(BANK_SNAPSHOTS_DIR) {
            continue;
        }
        let snapshot_dir = path.join(ACCOUNTS_SNAPSHOT_DIR).join(&slot_dir);
        let snapshot_has_files = snapshot_dir
            .read_dir()
            .ok()
            .and_then(|mut dir| dir.next())
            .is_some();
        if snapshot_dir.is_dir() && snapshot_has_files {
            account_paths.push((path, snapshot_dir));
            continue;
        }

        let mut has_appendvecs = false;
        let mut appendvecs = Vec::new();
        let dir_entries = fs::read_dir(&path)
            .map_err(|err| format!("failed to read {}: {err}", path.display()))?;
        for entry in dir_entries {
            let entry = entry.map_err(|err| format!("failed to read dir entry: {err}"))?;
            let file_type = entry
                .file_type()
                .map_err(|err| format!("failed to read file type: {err}"))?;
            if !file_type.is_file() {
                continue;
            }
            let Some(name) = entry.file_name().to_str().map(|s| s.to_string()) else {
                continue;
            };
            if looks_like_appendvec(&name) {
                has_appendvecs = true;
                appendvecs.push(entry.path());
            }
        }

        if has_appendvecs {
            fs::create_dir_all(&snapshot_dir).map_err(|err| {
                format!(
                    "failed to create snapshot dir {}: {err}",
                    snapshot_dir.display()
                )
            })?;
            for file_path in appendvecs {
                let Some(file_name) = file_path.file_name() else {
                    continue;
                };
                let dest_path = snapshot_dir.join(file_name);
                if dest_path.exists() {
                    continue;
                }
                link_or_copy(&file_path, &dest_path)?;
                changed = true;
            }
            let run_path = path.join(ACCOUNTS_RUN_DIR);
            fs::create_dir_all(&run_path)
                .map_err(|err| format!("failed to create {}: {err}", run_path.display()))?;
            account_paths.push((path, snapshot_dir));
        }
    }

    if account_paths.is_empty() {
        return Err(format!(
            "no account snapshot dirs found under {} for slot {}",
            ledger_dir.display(),
            slot_dir
        ));
    }

    for (idx, (account_path, snapshot_dir)) in account_paths.into_iter().enumerate() {
        let run_path = account_path.join(ACCOUNTS_RUN_DIR);
        fs::create_dir_all(&run_path)
            .map_err(|err| format!("failed to create {}: {err}", run_path.display()))?;
        let link_path = hardlinks_dir.join(format!("account_path_{idx}"));
        if link_path.exists() {
            continue;
        }
        let link_target = snapshot_dir
            .canonicalize()
            .unwrap_or_else(|_| snapshot_dir.clone());
        symlink_dir(&link_target, &link_path)?;
        changed = true;
    }

    Ok(changed || needs_cleanup)
}

fn looks_like_appendvec(name: &str) -> bool {
    let mut parts = name.split('.');
    let Some(slot) = parts.next() else {
        return false;
    };
    let Some(id) = parts.next() else {
        return false;
    };
    if parts.next().is_some() {
        return false;
    }
    !slot.is_empty()
        && !id.is_empty()
        && slot.chars().all(|c| c.is_ascii_digit())
        && id.chars().all(|c| c.is_ascii_digit())
}

fn env_truthy(var: &str) -> bool {
    match env::var(var) {
        Ok(value) => {
            let value = value.trim().to_ascii_lowercase();
            !matches!(value.as_str(), "" | "0" | "false" | "no")
        }
        Err(_) => false,
    }
}

fn count_snapshot_entries(
    snapshot_archive: &Path,
    archive_format: ArchiveFormat,
) -> Result<u64, String> {
    let file = fs::File::open(snapshot_archive)
        .map_err(|err| format!("failed to open snapshot archive for counting: {err}"))?;
    let reader = std::io::BufReader::with_capacity(SNAPSHOT_PRECOUNT_BUFFER_BYTES, file);
    let decompressor = ArchiveFormatDecompressor::new(archive_format, reader)
        .map_err(|err| format!("failed to create snapshot archive reader: {err}"))?;
    let mut archive = TarArchive::new(decompressor);
    let entries = archive
        .entries()
        .map_err(|err| format!("failed to read snapshot archive entries: {err}"))?;
    let mut count = 0u64;
    for entry in entries {
        entry.map_err(|err| format!("failed to read snapshot archive entry: {err}"))?;
        count = count.saturating_add(1);
    }
    Ok(count)
}

fn bank_root_interval() -> Option<u64> {
    match env::var("JETSTREAMER_ROOT_INTERVAL") {
        Ok(value) => {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                return None;
            }
            match trimmed.parse::<u64>() {
                Ok(0) => None,
                Ok(interval) => Some(interval),
                Err(err) => {
                    warn!(
                        "invalid JETSTREAMER_ROOT_INTERVAL '{value}': {err}; disabling root pruning"
                    );
                    None
                }
            }
        }
        Err(_) => Some(DEFAULT_ROOT_INTERVAL),
    }
}

fn skip_snapshot_verify() -> bool {
    env_truthy("JETSTREAMER_SKIP_SNAPSHOT_VERIFY")
}

fn reset_dir(path: &Path) -> Result<(), String> {
    if path.exists() {
        fs::remove_dir_all(path)
            .map_err(|err| format!("failed to remove {}: {err}", path.display()))?;
    }
    fs::create_dir_all(path)
        .map_err(|err| format!("failed to create {}: {err}", path.display()))?;
    Ok(())
}

fn ensure_accounts_hardlinks_for_archive(
    bank_snapshot: &snapshot_utils::BankSnapshotInfo,
    account_dir: &Path,
) -> Result<(), String> {
    let slot_dir = bank_snapshot.slot.to_string();
    let snapshot_dir = account_dir.join(ACCOUNTS_SNAPSHOT_DIR).join(&slot_dir);
    if snapshot_dir.exists() {
        fs::remove_dir_all(&snapshot_dir)
            .map_err(|err| format!("failed to remove {}: {err}", snapshot_dir.display()))?;
    }
    fs::create_dir_all(&snapshot_dir)
        .map_err(|err| format!("failed to create {}: {err}", snapshot_dir.display()))?;

    let mut linked_any = false;
    let read_dir = fs::read_dir(account_dir)
        .map_err(|err| format!("failed to read {}: {err}", account_dir.display()))?;
    for entry in read_dir {
        let entry = entry.map_err(|err| format!("failed to read dir entry: {err}"))?;
        let file_type = entry
            .file_type()
            .map_err(|err| format!("failed to read file type: {err}"))?;
        if !file_type.is_file() {
            continue;
        }
        let Some(name) = entry.file_name().to_str().map(|s| s.to_string()) else {
            continue;
        };
        if !looks_like_appendvec(&name) {
            continue;
        }
        let dest_path = snapshot_dir.join(&name);
        if dest_path.exists() {
            continue;
        }
        link_or_copy(&entry.path(), &dest_path)?;
        linked_any = true;
    }

    if !linked_any {
        return Err(format!(
            "no appendvec files found under {}",
            account_dir.display()
        ));
    }

    let run_dir = account_dir.join(ACCOUNTS_RUN_DIR);
    fs::create_dir_all(&run_dir)
        .map_err(|err| format!("failed to create {}: {err}", run_dir.display()))?;

    let hardlinks_dir = bank_snapshot.snapshot_dir.join(ACCOUNTS_HARDLINKS_DIR);
    reset_dir(&hardlinks_dir)?;
    let link_path = hardlinks_dir.join("account_path_0");
    if link_path.exists() {
        fs::remove_file(&link_path)
            .map_err(|err| format!("failed to remove {}: {err}", link_path.display()))?;
    }
    let link_target = snapshot_dir
        .canonicalize()
        .unwrap_or_else(|_| snapshot_dir.clone());
    symlink_dir(&link_target, &link_path)?;
    Ok(())
}

fn load_bank_from_snapshot(
    ledger_dir: &Path,
    accounts_update_notifier: Option<AccountsUpdateNotifier>,
) -> Result<Bank, String> {
    let bank_snapshots_dir = ledger_dir.join(BANK_SNAPSHOTS_DIR);
    let mut bank_snapshot = snapshot_utils::get_highest_bank_snapshot(&bank_snapshots_dir);
    if bank_snapshot.is_none() {
        if ensure_snapshot_meta_files(ledger_dir)? {
            info!(
                "repaired snapshot metadata in {}",
                bank_snapshots_dir.display()
            );
        }
        bank_snapshot = snapshot_utils::get_highest_bank_snapshot(&bank_snapshots_dir);
    }
    let bank_snapshot = bank_snapshot.ok_or_else(|| {
        format!(
            "no bank snapshots found in {}",
            bank_snapshots_dir.display()
        )
    })?;

    if ensure_accounts_hardlinks(ledger_dir, &bank_snapshot)? {
        info!(
            "repaired accounts hardlinks in {}",
            bank_snapshot.snapshot_dir.display()
        );
    }
    let account_run_paths = account_run_paths_from_snapshot(&bank_snapshot.snapshot_dir)?;
    for path in &account_run_paths {
        fs::create_dir_all(path)
            .map_err(|err| format!("failed to create {}: {err}", path.display()))?;
    }

    let genesis_config = open_genesis_config(ledger_dir, MAX_GENESIS_ARCHIVE_UNPACKED_SIZE)
        .map_err(|err| format!("failed to load genesis config: {err}"))?;
    let runtime_config = RuntimeConfig::default();
    let accounts_db_config = AccountsDbConfig::default();
    let exit = Arc::new(AtomicBool::new(false));
    let limit_load_slot_count_from_snapshot = if skip_snapshot_verify() {
        info!("snapshot verification disabled via JETSTREAMER_SKIP_SNAPSHOT_VERIFY");
        Some(usize::MAX)
    } else {
        None
    };

    snapshot_bank_utils::bank_from_snapshot_dir(
        &account_run_paths,
        &bank_snapshot,
        &genesis_config,
        &runtime_config,
        None,
        limit_load_slot_count_from_snapshot,
        false,
        accounts_db_config,
        accounts_update_notifier,
        exit,
    )
    .map_err(|err| format!("failed to build bank from snapshot: {err}"))
}

fn load_bank_from_snapshot_archive(
    ledger_dir: &Path,
    snapshot_archive: &Path,
    accounts_update_notifier: Option<AccountsUpdateNotifier>,
) -> Result<Bank, String> {
    let bank_snapshots_dir = ledger_dir.join(BANK_SNAPSHOTS_DIR);
    fs::create_dir_all(&bank_snapshots_dir)
        .map_err(|err| format!("failed to create {}: {err}", bank_snapshots_dir.display()))?;
    let account_run_dir = ledger_dir.join(ARCHIVE_ACCOUNTS_DIR);
    reset_dir(&account_run_dir)?;

    let full_snapshot = FullSnapshotArchiveInfo::new_from_path(snapshot_archive.to_path_buf())
        .map_err(|err| format!("failed to parse snapshot archive: {err}"))?;
    let genesis_config = open_genesis_config(ledger_dir, MAX_GENESIS_ARCHIVE_UNPACKED_SIZE)
        .map_err(|err| format!("failed to load genesis config: {err}"))?;
    let runtime_config = RuntimeConfig::default();
    let accounts_db_config = AccountsDbConfig::default();
    let exit = Arc::new(AtomicBool::new(false));
    let limit_load_slot_count_from_snapshot = if skip_snapshot_verify() {
        info!("snapshot verification disabled via JETSTREAMER_SKIP_SNAPSHOT_VERIFY");
        Some(usize::MAX)
    } else {
        None
    };

    let temp_dir = TempDirBuilder::new()
        .prefix("tmp-snapshot-archive-")
        .tempdir_in(&bank_snapshots_dir)
        .map_err(|err| {
            format!(
                "failed to create temp dir in {}: {err}",
                bank_snapshots_dir.display()
            )
        })?;
    let unpack_dir = temp_dir.path().to_path_buf();
    let (sender, receiver) = unbounded::<PathBuf>();
    let log_interval = env::var("JETSTREAMER_SNAPSHOT_UNPACK_LOG_INTERVAL_SECS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .map(Duration::from_secs)
        .or_else(|| Some(Duration::from_secs(SNAPSHOT_UNPACK_LOG_INTERVAL_SECS)));
    let percent_enabled = !env_truthy("JETSTREAMER_SNAPSHOT_UNPACK_NO_PERCENT");
    let total_entries = if percent_enabled {
        info!(
            "counting snapshot archive entries for progress percent ({})",
            snapshot_archive.display()
        );
        let start = Instant::now();
        match count_snapshot_entries(snapshot_archive, full_snapshot.archive_format()) {
            Ok(total) => {
                info!(
                    "snapshot archive entries: total={total} (counted in {:.2}s)",
                    start.elapsed().as_secs_f64()
                );
                Some(total)
            }
            Err(err) => {
                warn!("snapshot entry counting failed: {err} (percent disabled)");
                None
            }
        }
    } else {
        None
    };
    if let Some(interval) = log_interval {
        info!(
            "unpacking snapshot archive {} (log interval {}s)",
            snapshot_archive.display(),
            interval.as_secs()
        );
    } else {
        info!("unpacking snapshot archive {}", snapshot_archive.display());
    }
    let drain = std::thread::spawn(move || {
        let mut count = 0u64;
        let mut last_log = Instant::now();
        let start = Instant::now();
        for path in receiver.iter() {
            count += 1;
            if let Some(interval) = log_interval {
                if last_log.elapsed() >= interval {
                    let elapsed = start.elapsed().as_secs_f64();
                    let rate = if elapsed > 0.0 {
                        count as f64 / elapsed
                    } else {
                        0.0
                    };
                    let last_display = path.display().to_string();
                    if let Some(total) = total_entries {
                        let percent = if total > 0 {
                            (count as f64 * 100.0 / total as f64).min(100.0)
                        } else {
                            0.0
                        };
                        info!(
                            "snapshot unpack progress: files={count}/{total} ({percent:.2}%) rate={rate:.1} files/s last={last_display}"
                        );
                    } else {
                        info!(
                            "snapshot unpack progress: files={count} rate={rate:.1} files/s last={last_display}"
                        );
                    }
                    last_log = Instant::now();
                }
            }
        }
        if count > 0 {
            let elapsed = start.elapsed().as_secs_f64();
            let rate = if elapsed > 0.0 {
                count as f64 / elapsed
            } else {
                0.0
            };
            if let Some(total) = total_entries {
                let percent = if total > 0 {
                    (count as f64 * 100.0 / total as f64).min(100.0)
                } else {
                    0.0
                };
                info!(
                    "snapshot unpack complete: files={count}/{total} ({percent:.2}%) rate={rate:.1} files/s"
                );
            } else {
                info!("snapshot unpack complete: files={count} rate={rate:.1} files/s");
            }
        }
    });
    let handle = streaming_unarchive_snapshot(
        sender,
        vec![account_run_dir.clone()],
        unpack_dir.clone(),
        snapshot_archive.to_path_buf(),
        full_snapshot.archive_format(),
        0,
    );
    let result = handle
        .join()
        .map_err(|_| "snapshot unarchive thread panicked".to_string())?;
    result.map_err(|err| format!("snapshot unarchive failed: {err}"))?;
    let _ = drain.join();

    if ensure_snapshot_meta_files(&unpack_dir)? {
        info!(
            "repaired snapshot metadata in {}",
            unpack_dir.join(BANK_SNAPSHOTS_DIR).display()
        );
    }

    let bank_snapshots_dir = unpack_dir.join(BANK_SNAPSHOTS_DIR);
    let bank_snapshot =
        snapshot_utils::get_highest_bank_snapshot(&bank_snapshots_dir).ok_or_else(|| {
            format!(
                "no bank snapshots found in {}",
                bank_snapshots_dir.display()
            )
        })?;

    ensure_accounts_hardlinks_for_archive(&bank_snapshot, &account_run_dir)?;
    let account_run_paths = account_run_paths_from_snapshot(&bank_snapshot.snapshot_dir)?;
    for path in &account_run_paths {
        fs::create_dir_all(path)
            .map_err(|err| format!("failed to create {}: {err}", path.display()))?;
    }

    let bank = snapshot_bank_utils::bank_from_snapshot_dir(
        &account_run_paths,
        &bank_snapshot,
        &genesis_config,
        &runtime_config,
        None,
        limit_load_slot_count_from_snapshot,
        false,
        accounts_db_config,
        accounts_update_notifier,
        exit,
    )
    .map_err(|err| format!("failed to build bank from snapshot archive: {err}"))?;

    let bank_hash = bank.get_snapshot_hash();
    let archive_hash = *full_snapshot.hash();
    if bank_hash != archive_hash {
        if env_truthy("JETSTREAMER_ENFORCE_ARCHIVE_HASH") {
            return Err(format!(
                "snapshot archive hash mismatch: deserialized bank: {bank_hash:?}, snapshot archive: {archive_hash:?}"
            ));
        }
        warn!(
            "snapshot archive hash mismatch: deserialized bank: {bank_hash:?}, snapshot archive: {archive_hash:?}"
        );
    }

    Ok(bank)
}

fn firehose_threads() -> u64 {
    env::var("JETSTREAMER_THREADS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(1)
}

fn local_index_path(cache_dir: &Path, url: &Url) -> Result<PathBuf, String> {
    let path = url.path().trim_start_matches('/');
    if path.is_empty() {
        return Err(format!("index url missing path: {url}"));
    }
    Ok(cache_dir.join(path))
}

fn resolve_remote_index_base_url() -> Result<Url, String> {
    if let Ok(value) = env::var("JETSTREAMER_COMPACT_INDEX_BASE_URL") {
        return Url::parse(&value)
            .map_err(|err| format!("invalid JETSTREAMER_COMPACT_INDEX_BASE_URL: {err}"));
    }
    if let Ok(value) = env::var("JETSTREAMER_ARCHIVE_BASE") {
        return Url::parse(&value)
            .map_err(|err| format!("invalid JETSTREAMER_ARCHIVE_BASE: {err}"));
    }
    if let Ok(value) = env::var("JETSTREAMER_HTTP_BASE_URL") {
        return Url::parse(&value)
            .map_err(|err| format!("invalid JETSTREAMER_HTTP_BASE_URL: {err}"));
    }
    Url::parse(BASE_URL).map_err(|err| format!("invalid default index base url: {err}"))
}

async fn fetch_url_range(
    client: &Client,
    url: &Url,
    start: u64,
    end: u64,
) -> Result<Vec<u8>, String> {
    if end < start {
        return Ok(Vec::new());
    }
    let range = format!("bytes={start}-{end}");
    let response = client
        .get(url.clone())
        .header(RANGE, range)
        .send()
        .await
        .map_err(|err| format!("failed to fetch {}: {err}", url.as_str()))?;
    if !response.status().is_success() {
        return Err(format!(
            "unexpected HTTP status {} fetching {}",
            response.status(),
            url.as_str()
        ));
    }
    let bytes = response
        .bytes()
        .await
        .map_err(|err| format!("failed to read {}: {err}", url.as_str()))?;
    Ok(bytes.to_vec())
}

async fn fetch_epoch_root_cid(client: &Client, car_url: &Url) -> Result<Cid, String> {
    let mut bytes = fetch_url_range(client, car_url, 0, CAR_HEADER_PREFETCH_BYTES - 1).await?;
    let (header_len, prefix) = decode_varint(&bytes)?;
    let total_needed = prefix + header_len as usize;
    if bytes.len() < total_needed {
        bytes = fetch_url_range(client, car_url, 0, total_needed as u64 - 1).await?;
        if bytes.len() < total_needed {
            return Err(format!(
                "incomplete CAR header: expected {total_needed} bytes, got {}",
                bytes.len()
            ));
        }
    }
    let header_bytes = &bytes[prefix..total_needed];
    let value: Value = serde_cbor::from_slice(header_bytes)
        .map_err(|err| format!("failed to decode CBOR: {err}"))?;
    extract_root_cid(&value)
}

async fn resolve_compact_index_urls(
    epoch: u64,
    base_url: &Url,
    client: &Client,
) -> Result<(Url, Url), String> {
    let car_path = format!("{epoch}/epoch-{epoch}.car");
    let car_url = base_url
        .join(&car_path)
        .map_err(|err| format!("invalid car url {car_path}: {err}"))?;
    let root_cid = fetch_epoch_root_cid(client, &car_url).await?;
    let root_base32 = root_cid
        .to_string_of_base(Base::Base32Lower)
        .map_err(|err| format!("failed to encode root cid: {err}"))?;
    let network = env::var("JETSTREAMER_NETWORK").unwrap_or_else(|_| "mainnet".to_string());
    let slot_index_path = format!(
        "{0}/epoch-{0}-{1}-{2}-slot-to-cid.index",
        epoch, root_base32, network
    );
    let cid_index_path = format!(
        "{0}/epoch-{0}-{1}-{2}-cid-to-offset-and-size.index",
        epoch, root_base32, network
    );
    let slot_url = base_url
        .join(&slot_index_path)
        .map_err(|err| format!("invalid slot index url: {err}"))?;
    let cid_url = base_url
        .join(&cid_index_path)
        .map_err(|err| format!("invalid cid index url: {err}"))?;
    Ok((slot_url, cid_url))
}

async fn download_with_ripget(
    url: &Url,
    dest: &Path,
    shutdown: Arc<AtomicBool>,
) -> Result<(), String> {
    if let Ok(metadata) = fs::metadata(dest) {
        if metadata.len() > 0 {
            info!("compact index already cached at {}", dest.display());
            return Ok(());
        }
    }
    if let Some(parent) = dest.parent() {
        fs::create_dir_all(parent)
            .map_err(|err| format!("failed to create {}: {err}", parent.display()))?;
    }
    let ripget_threads = env::var("JETSTREAMER_RIPGET_THREADS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(255);
    let log_interval = env::var("JETSTREAMER_RIPGET_LOG_INTERVAL_SECS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(RIPGET_LOG_INTERVAL_SECS);
    info!(
        "downloading compact index {} (ripget threads={} log_interval={}s)",
        url.as_str(),
        ripget_threads,
        log_interval
    );
    let shutdown_wait = async {
        while !shutdown.load(Ordering::Relaxed) {
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    };

    let progress = Arc::new(RipgetProgress::new(
        format!("ripget {}", dest.display()),
        Duration::from_secs(log_interval),
    ));
    let result = tokio::select! {
        _ = shutdown_wait => {
            return Err("shutdown requested during compact index download".to_string());
        }
        result = ripget::download_url_with_progress(
            url.as_str(),
            dest,
            Some(ripget_threads),
            None,
            Some(progress),
            None,
        ) => result,
    };

    let report = result.map_err(|err| format!("ripget failed for {}: {err}", url.as_str()))?;
    info!(
        "ripget finished: {} -> {} ({})",
        url.as_str(),
        report.path.display(),
        format_bytes(report.bytes),
    );

    Ok(())
}

async fn ensure_compact_indexes_cached(
    epoch: u64,
    ledger_dir: &Path,
    shutdown: Arc<AtomicBool>,
) -> Result<PathBuf, String> {
    let remote_base = resolve_remote_index_base_url()?;
    let client = Client::new();
    let (slot_url, cid_url) = resolve_compact_index_urls(epoch, &remote_base, &client).await?;

    let cache_dir = ledger_dir.join("compact-indexes");
    fs::create_dir_all(&cache_dir)
        .map_err(|err| format!("failed to create {}: {err}", cache_dir.display()))?;
    let slot_path = local_index_path(&cache_dir, &slot_url)?;
    let cid_path = local_index_path(&cache_dir, &cid_url)?;

    download_with_ripget(&slot_url, &slot_path, shutdown.clone()).await?;
    download_with_ripget(&cid_url, &cid_path, shutdown).await?;

    fs::canonicalize(&slot_path)
        .map_err(|err| format!("failed to canonicalize {}: {err}", slot_path.display()))
}

async fn ensure_slot_index_cached(
    epoch: u64,
    ledger_dir: &Path,
    shutdown: Arc<AtomicBool>,
) -> Result<PathBuf, String> {
    let remote_base = resolve_remote_index_base_url()?;
    let client = Client::new();
    let (slot_url, _cid_url) = resolve_compact_index_urls(epoch, &remote_base, &client).await?;

    let cache_dir = ledger_dir.join("compact-indexes");
    fs::create_dir_all(&cache_dir)
        .map_err(|err| format!("failed to create {}: {err}", cache_dir.display()))?;
    let slot_path = local_index_path(&cache_dir, &slot_url)?;

    download_with_ripget(&slot_url, &slot_path, shutdown).await?;

    fs::canonicalize(&slot_path)
        .map_err(|err| format!("failed to canonicalize {}: {err}", slot_path.display()))
}

async fn build_slot_presence_map(
    start_slot: Slot,
    end_inclusive: Slot,
    shutdown: Arc<AtomicBool>,
    ledger_dir: &Path,
    target_epoch: u64,
) -> Result<Arc<SlotPresenceMap>, String> {
    if end_inclusive < start_slot {
        return Err(format!(
            "invalid slot range: {start_slot}..={end_inclusive}"
        ));
    }

    let mut epoch_indexes: Vec<(u64, Slot, Slot, PathBuf)> = Vec::new();
    let start_epoch = slot_to_epoch(start_slot);
    let end_epoch = slot_to_epoch(end_inclusive);
    for epoch in start_epoch..=end_epoch {
        let (epoch_start, epoch_end_inclusive) = epoch_to_slot_range(epoch);
        let range_start = start_slot.max(epoch_start);
        let range_end = end_inclusive.min(epoch_end_inclusive);
        if range_start > range_end {
            continue;
        }
        let slot_index_path = if epoch == target_epoch {
            ensure_compact_indexes_cached(epoch, ledger_dir, shutdown.clone()).await?
        } else {
            ensure_slot_index_cached(epoch, ledger_dir, shutdown.clone()).await?
        };
        epoch_indexes.push((epoch, range_start, range_end, slot_index_path));
    }

    let shutdown = shutdown.clone();
    tokio::task::spawn_blocking(move || {
        let total_slots = end_inclusive.saturating_sub(start_slot).saturating_add(1);
        let total_len = usize::try_from(total_slots)
            .map_err(|_| "slot range too large to index".to_string())?;
        let mut states = vec![SlotPresenceState::Missing; total_len];
        let mut present = 0usize;
        let mut missing = 0usize;
        let log_interval = env::var("JETSTREAMER_SLOT_PRESENCE_LOG_INTERVAL_SECS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(5);

        info!(
            "building slot presence map for slots {}..={} ({} total)",
            start_slot, end_inclusive, total_slots
        );

        let start_time = Instant::now();
        let mut last_log = Instant::now();
        let mut processed = 0u64;

        for (epoch, range_start, range_end, slot_index_path) in epoch_indexes {
            info!(
                "slot-to-cid index cached for epoch {} at {}",
                epoch,
                slot_index_path.display()
            );
            info!(
                "loading slot-to-cid index for epoch {} from {}",
                epoch,
                slot_index_path.display()
            );
            let data = fs::read(&slot_index_path)
                .map_err(|err| format!("failed to read {}: {err}", slot_index_path.display()))?;
            let index = LocalSlotIndex::from_bytes(data)?;
            if let Some(meta_epoch) = index.header.metadata_epoch() {
                if meta_epoch != epoch {
                    warn!(
                        "slot-to-cid index metadata epoch mismatch: expected {}, got {}",
                        epoch, meta_epoch
                    );
                } else {
                    info!("slot-to-cid index metadata epoch: {}", meta_epoch);
                }
            }

            for slot in range_start..=range_end {
                if shutdown.load(Ordering::Relaxed) {
                    return Err("shutdown requested during slot presence scan".to_string());
                }
                let idx = (slot - start_slot) as usize;
                if index.contains_slot(slot)? {
                    states[idx] = SlotPresenceState::Present;
                    present += 1;
                } else {
                    states[idx] = SlotPresenceState::Missing;
                    missing += 1;
                }
                processed += 1;
                if last_log.elapsed() >= Duration::from_secs(log_interval) {
                    let elapsed = start_time.elapsed().as_secs_f64();
                    let rate = if elapsed > 0.0 {
                        processed as f64 / elapsed
                    } else {
                        0.0
                    };
                    let remaining = total_slots.saturating_sub(processed);
                    let eta = if rate > 0.0 {
                        let eta_secs = (remaining as f64 / rate).ceil() as u64;
                        format_eta(Duration::from_secs(eta_secs))
                    } else {
                        "unknown".to_string()
                    };
                    let percent = if total_slots == 0 {
                        100.0
                    } else {
                        (processed as f64) * 100.0 / (total_slots as f64)
                    };
                    info!(
                        "slot presence progress: {processed}/{total_slots} ({percent:.2}%) present={present} missing={missing} rate={rate:.1} slots/s eta={eta}"
                    );
                    last_log = Instant::now();
                }
            }
        }

        let mut next_present_after: Vec<Option<Slot>> = vec![None; states.len()];
        let mut next_present: Option<Slot> = None;
        for idx in (0..states.len()).rev() {
            next_present_after[idx] = next_present;
            if states[idx] == SlotPresenceState::Present {
                let slot = start_slot.saturating_add(idx as Slot);
                next_present = Some(slot);
            }
        }

        info!("slot presence map built: present={present}, missing={missing}");
        Ok(Arc::new(SlotPresenceMap {
            start: start_slot,
            end_inclusive,
            states,
            next_present_after,
        }))
    })
    .await
    .map_err(|err| format!("slot presence task failed: {err}"))?
}

async fn run_geyser_replay(
    epoch: u64,
    ledger_dir: &Path,
    snapshot_archive: &Path,
    shutdown: Arc<AtomicBool>,
    cursor: Arc<ReplayCursor>,
    snapshot_verifier: Option<Arc<SnapshotVerifier>>,
) -> Result<(), String> {
    let libpath = plugin_library_path()?;
    let config_path = write_geyser_config(ledger_dir, &libpath)?;
    let (confirmed_bank_sender, confirmed_bank_receiver) = unbounded();
    let config_files = [config_path];

    let service = GeyserPluginService::new(confirmed_bank_receiver, true, &config_files)
        .map_err(|err| format!("failed to load geyser plugin: {err}"))?;
    let (epoch_start, end_inclusive) = epoch_to_slot_range(epoch);
    let progress = Arc::new(ReplayProgress::new(epoch_start));
    let failure = Arc::new(ReplayFailure::new(shutdown.clone()));
    let accounts_update_notifier = service.get_accounts_update_notifier().map(|delegate| {
        info!(
            "geyser account updates enabled: snapshot_notifications={}",
            delegate.snapshot_notifications_enabled()
        );
        Arc::new(ProgressAccountsUpdateNotifier {
            progress: progress.clone(),
            delegate: Some(delegate),
            live_start_slot: epoch_start,
        }) as AccountsUpdateNotifier
    });
    info!(
        "accounts update notifier wired into snapshot load: {}",
        accounts_update_notifier.is_some()
    );

    ensure_genesis_archive(ledger_dir).await?;
    info!("loading bank from snapshot");
    let ledger_dir = ledger_dir.to_path_buf();
    let ledger_dir_for_load = ledger_dir.clone();
    let snapshot_archive = snapshot_archive.to_path_buf();
    let use_dir_loader = env_truthy("JETSTREAMER_LOAD_FROM_DIR");
    let bank = tokio::task::spawn_blocking(move || {
        if use_dir_loader {
            load_bank_from_snapshot(&ledger_dir_for_load, accounts_update_notifier)
        } else {
            load_bank_from_snapshot_archive(
                &ledger_dir_for_load,
                &snapshot_archive,
                accounts_update_notifier,
            )
        }
    })
    .await
    .map_err(|err| format!("snapshot load task failed: {err}"))??;
    info!(
        "bank accounts update notifier active: {}",
        bank.rc.accounts.accounts_db.has_accounts_update_notifier()
    );
    let root_interval = bank_root_interval();
    if let Some(interval) = root_interval {
        info!("bank root pruning interval: {interval}");
    } else {
        info!("bank root pruning disabled");
    }
    let snapshot_slot = bank.slot();
    let replay_start = if snapshot_slot.saturating_add(1) < epoch_start {
        snapshot_slot.saturating_add(1)
    } else {
        epoch_start
    };
    if replay_start < epoch_start {
        info!(
            "warming up replay from slot {} to {} (epoch {} starts at {})",
            replay_start,
            epoch_start.saturating_sub(1),
            epoch,
            epoch_start
        );
    } else {
        info!("starting replay at epoch {} slot {}", epoch, replay_start);
    }
    progress.reset_last_slots(replay_start.saturating_sub(1));

    let slot_presence = build_slot_presence_map(
        replay_start,
        end_inclusive,
        shutdown.clone(),
        &ledger_dir,
        epoch,
    )
    .await?;
    let scheduler = Arc::new(TransactionScheduler::new(replay_start, slot_presence));
    let bank_replay = Arc::new(BankReplay::new(
        bank,
        snapshot_verifier.clone(),
        root_interval,
        failure.clone(),
        cursor.clone(),
    ));
    let (ready_sender, ready_receiver) = unbounded::<Vec<ReadyEntry>>();
    let ready_shutdown = shutdown.clone();
    let ready_bank_replay = bank_replay.clone();
    let ready_handle = std::thread::spawn(move || {
        while let Ok(entries) = ready_receiver.recv() {
            if ready_shutdown.load(Ordering::Relaxed) {
                break;
            }
            ready_bank_replay.process_ready_entries(entries);
        }
    });
    let slot_range = replay_start..(end_inclusive + 1);

    let client = Client::new();
    let index_base_url = resolve_remote_index_base_url()?;
    let transaction_notifier = Arc::new(BankTransactionNotifier {
        progress: progress.clone(),
        scheduler: scheduler.clone(),
        failure: failure.clone(),
        ready_sender: ready_sender.clone(),
        delegate: service.get_transaction_notifier(),
        live_start_slot: epoch_start,
    });
    let entry_notifier = Arc::new(BankEntryNotifier {
        progress: progress.clone(),
        scheduler: scheduler.clone(),
        failure: failure.clone(),
        ready_sender: ready_sender.clone(),
        delegate: service.get_entry_notifier(),
        live_start_slot: epoch_start,
    });
    let block_metadata_notifier = Arc::new(BankBlockMetadataNotifier {
        scheduler: scheduler.clone(),
        progress: progress.clone(),
        failure: failure.clone(),
        ready_sender: ready_sender.clone(),
        delegate: service.get_block_metadata_notifier(),
        live_start_slot: epoch_start,
    });
    let notifiers = GeyserNotifiers {
        transaction_notifier: Some(transaction_notifier.clone()),
        entry_notifier: Some(entry_notifier.clone()),
        block_metadata_notifier: Some(block_metadata_notifier.clone()),
    };
    let mut threads = firehose_threads();
    if threads > 1 {
        info!("forcing firehose threads to 1 for bank replay");
        threads = 1;
    }

    let progress_done = Arc::new(AtomicBool::new(false));
    let progress_handle = {
        let progress = progress.clone();
        let scheduler = scheduler.clone();
        let cursor = cursor.clone();
        let failure = failure.clone();
        let progress_done = progress_done.clone();
        let shutdown = shutdown.clone();
        std::thread::spawn(move || {
            let has_warmup = replay_start < epoch_start;
            let warmup_end = epoch_start.saturating_sub(1);
            let warmup_total = if has_warmup {
                warmup_end.saturating_sub(replay_start).saturating_add(1)
            } else {
                0
            };
            let main_total = end_inclusive.saturating_sub(epoch_start).saturating_add(1);
            let stall_interval = env::var("JETSTREAMER_STALL_LOG_SECS")
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
                .map(Duration::from_secs)
                .unwrap_or(Duration::from_secs(30));
            let inflight_warn_after = ENTRY_EXEC_WARN_AFTER;
            let inflight_fail_after = ENTRY_EXEC_FAIL_AFTER;
            let mut phase_start = None::<Instant>;
            let mut in_warmup = has_warmup;
            let mut last_seen_slot = progress.latest_slot.load(Ordering::Relaxed);
            let mut last_seen_change = Instant::now();
            let mut last_stall_log = Instant::now();
            while !progress_done.load(Ordering::Relaxed) && !shutdown.load(Ordering::Relaxed) {
                std::thread::sleep(Duration::from_secs(3));
                if progress_done.load(Ordering::Relaxed) || shutdown.load(Ordering::Relaxed) {
                    break;
                }
                if phase_start.is_none() {
                    phase_start = Some(Instant::now());
                }
                let latest = progress.latest_slot.load(Ordering::Relaxed);
                let tx_count = progress.tx_count.load(Ordering::Relaxed);
                let account_updates = progress.account_update_count.load(Ordering::Relaxed);
                if latest != last_seen_slot {
                    last_seen_slot = latest;
                    last_seen_change = Instant::now();
                } else {
                    let stalled_for = last_seen_change.elapsed();
                    if stalled_for >= stall_interval && last_stall_log.elapsed() >= stall_interval {
                        let snapshot = scheduler.snapshot();
                        let (cursor_slot, cursor_entry, cursor_tx_start, cursor_tx_count, cursor_sig) =
                            cursor.snapshot();
                        let mut expected_after_slot: Option<Slot> = None;
                        if let Some(buffer) = snapshot.buffer.as_ref() {
                            if buffer.expected_tx_count.is_none()
                                && buffer.expected_entry_count.is_none()
                                && buffer.processed_entry_count > 0
                                && buffer.pending_entries == 0
                                && buffer.buffered_txs == 0
                            {
                                expected_after_slot =
                                    scheduler.expected_block_metadata_after(snapshot.current_slot);
                            }
                        }
                        let mut inflight_slot = 0;
                        let mut inflight_entry = 0;
                        let mut inflight_tx_start = 0;
                        let mut inflight_tx_count = 0;
                        let mut inflight_sig: Option<String> = None;
                        let mut inflight_stage = "<none>";
                        let mut inflight_elapsed: Option<Duration> = None;
                        if let Some((slot, entry, tx_start, tx_count, sig, stage, elapsed)) =
                            cursor.inflight_snapshot()
                        {
                            inflight_slot = slot;
                            inflight_entry = entry;
                            inflight_tx_start = tx_start;
                            inflight_tx_count = tx_count;
                            inflight_sig = sig.clone();
                            inflight_stage = stage;
                            inflight_elapsed = Some(elapsed);
                            if elapsed >= inflight_warn_after {
                                warn!(
                                    "entry execution in-flight: slot {} entry {} tx_start={} tx_count={} stage={} elapsed={:.3}s sig={}",
                                    slot,
                                    entry,
                                    tx_start,
                                    tx_count,
                                    stage,
                                    elapsed.as_secs_f64(),
                                    sig.as_deref().unwrap_or("<none>"),
                                );
                            }
                            if elapsed >= inflight_fail_after {
                                let message = format!(
                                    "entry execution exceeded timeout: slot {} entry {} tx_start={} tx_count={} stage={} elapsed={:.3}s sig={}",
                                    slot,
                                    entry,
                                    tx_start,
                                    tx_count,
                                    stage,
                                    elapsed.as_secs_f64(),
                                    sig.as_deref().unwrap_or("<none>"),
                                );
                                failure.record(message);
                            }
                        }
                        if inflight_slot == 393_521_153 && inflight_entry == 0 {
                            if let Some(details) = cursor.take_inflight_details() {
                                warn!("{details}");
                            }
                        }
                        let last_tx_slot = progress.last_tx_slot.load(Ordering::Relaxed);
                        let last_entry_slot = progress.last_entry_slot.load(Ordering::Relaxed);
                        let last_block_meta_slot =
                            progress.last_block_meta_slot.load(Ordering::Relaxed);
                        let last_account_update_slot =
                            progress.last_account_update_slot.load(Ordering::Relaxed);
                        let phase = if in_warmup && latest < epoch_start {
                            "warmup"
                        } else {
                            "main"
                        };
                        let expected_after_display = expected_after_slot
                            .map(|slot| slot.to_string())
                            .unwrap_or_else(|| "<none>".to_string());
                        let expected_after_eta = if let Some(expected_after) = expected_after_slot {
                            let remaining_slots = expected_after.saturating_sub(latest);
                            if remaining_slots == 0 {
                                "00:00:00".to_string()
                            } else if let Some(start) = phase_start {
                                let processed = if in_warmup && latest < epoch_start {
                                    if latest < replay_start {
                                        0
                                    } else {
                                        latest.saturating_sub(replay_start).saturating_add(1)
                                    }
                                } else {
                                    let display_slot = latest.clamp(epoch_start, end_inclusive);
                                    if display_slot < epoch_start {
                                        0
                                    } else {
                                        display_slot
                                            .saturating_sub(epoch_start)
                                            .saturating_add(1)
                                    }
                                };
                                let elapsed = start.elapsed().as_secs_f64();
                                let rate = if elapsed > 0.0 {
                                    processed as f64 / elapsed
                                } else {
                                    0.0
                                };
                                if rate > 0.0 {
                                    let eta_secs = ((remaining_slots as f64) / rate).ceil() as u64;
                                    format_eta(Duration::from_secs(eta_secs))
                                } else {
                                    "unknown".to_string()
                                }
                            } else {
                                "unknown".to_string()
                            }
                        } else {
                            "<none>".to_string()
                        };
                        info!(
                            "replay stall ({phase}): slot {latest} unchanged for {:.1}s; scheduler current_slot={} last_finalized={} buffered_slots={} highest_seen_slot={} presence={:?} buffer={:?} expected_after_slot={} expected_after_eta={} last_tx_slot={} last_entry_slot={} last_block_meta_slot={} last_account_update_slot={} cursor_slot={} cursor_entry={} cursor_tx_start={} cursor_tx_count={} cursor_sig={} inflight_slot={} inflight_entry={} inflight_tx_start={} inflight_tx_count={} inflight_stage={} inflight_elapsed={} inflight_sig={}",
                            stalled_for.as_secs_f64(),
                            snapshot.current_slot,
                            snapshot.last_finalized_slot,
                            snapshot.buffered_slots,
                            snapshot.highest_seen_slot,
                            snapshot.presence,
                            snapshot.buffer,
                            expected_after_display,
                            expected_after_eta,
                            last_tx_slot,
                            last_entry_slot,
                            last_block_meta_slot,
                            last_account_update_slot,
                            cursor_slot,
                            cursor_entry,
                            cursor_tx_start,
                            cursor_tx_count,
                            cursor_sig.as_deref().unwrap_or("<unknown>"),
                            inflight_slot,
                            inflight_entry,
                            inflight_tx_start,
                            inflight_tx_count,
                            inflight_stage,
                            inflight_elapsed
                                .map(|duration| format!("{:.3}s", duration.as_secs_f64()))
                                .unwrap_or_else(|| "<none>".to_string()),
                            inflight_sig.as_deref().unwrap_or("<none>"),
                        );
                        last_stall_log = Instant::now();
                    }
                }
                if in_warmup && latest < epoch_start {
                    let processed = if latest < replay_start {
                        0
                    } else {
                        latest.saturating_sub(replay_start).saturating_add(1)
                    };
                    let percent = if warmup_total == 0 {
                        100.0
                    } else {
                        (processed as f64) * 100.0 / (warmup_total as f64)
                    };
                    let display_slot = if latest < replay_start {
                        replay_start
                    } else {
                        latest
                    };
                    let eta = if processed == 0 {
                        "unknown".to_string()
                    } else if let Some(start) = phase_start {
                        let elapsed = start.elapsed().as_secs_f64();
                        if elapsed <= 0.0 {
                            "unknown".to_string()
                        } else {
                            let rate = (processed as f64) / elapsed;
                            if rate <= 0.0 {
                                "unknown".to_string()
                            } else {
                                let remaining = warmup_total.saturating_sub(processed);
                                let eta_secs = ((remaining as f64) / rate).ceil() as u64;
                                format_eta(Duration::from_secs(eta_secs))
                            }
                        }
                    } else {
                        "unknown".to_string()
                    };
                    info!(
                        "warmup slot {display_slot}/{warmup_end} ({percent:.2}%) txs={tx_count} accounts={account_updates} eta={eta} (epoch {epoch} starts at {epoch_start})"
                    );
                } else {
                    if in_warmup {
                        in_warmup = false;
                        phase_start = Some(Instant::now());
                        progress.reset_counts();
                    }
                    let display_slot = latest.clamp(epoch_start, end_inclusive);
                    let processed = if display_slot < epoch_start {
                        0
                    } else {
                        display_slot.saturating_sub(epoch_start).saturating_add(1)
                    };
                    let percent = if main_total == 0 {
                        100.0
                    } else {
                        (processed as f64) * 100.0 / (main_total as f64)
                    };
                    let eta = if processed == 0 {
                        "unknown".to_string()
                    } else if let Some(start) = phase_start {
                        let elapsed = start.elapsed().as_secs_f64();
                        if elapsed <= 0.0 {
                            "unknown".to_string()
                        } else {
                            let rate = (processed as f64) / elapsed;
                            if rate <= 0.0 {
                                "unknown".to_string()
                            } else {
                                let remaining = main_total.saturating_sub(processed);
                                let eta_secs = ((remaining as f64) / rate).ceil() as u64;
                                format_eta(Duration::from_secs(eta_secs))
                            }
                        }
                    } else {
                        "unknown".to_string()
                    };
                    info!(
                        "progress slot {display_slot}/{end_inclusive} ({percent:.2}%) txs={tx_count} accounts={account_updates} eta={eta}"
                    );
                }
            }
        })
    };

    info!("starting firehose replay with {} thread(s)", threads);
    let firehose_result = tokio::task::spawn_blocking({
        let slot_range = slot_range.clone();
        let notifiers = notifiers;
        let confirmed_bank_sender = confirmed_bank_sender.clone();
        let index_base_url = index_base_url.clone();
        let client = client.clone();
        let shutdown = shutdown.clone();
        move || {
            let rt = match tokio::runtime::Runtime::new() {
                Ok(rt) => Arc::new(rt),
                Err(err) => {
                    return Err((FirehoseError::OnLoadError(Box::new(err)), slot_range.start));
                }
            };
            firehose_geyser_with_notifiers(
                rt,
                slot_range,
                notifiers,
                confirmed_bank_sender,
                &index_base_url,
                &client,
                shutdown,
                async { Ok(()) },
                threads,
            )
        }
    })
    .await
    .map_err(|err| format!("firehose task failed: {err}"))?;
    progress_done.store(true, Ordering::Relaxed);
    let _ = progress_handle.join();
    firehose_result.map_err(|(err, slot)| format!("firehose error at slot {slot}: {err}"))?;

    match scheduler.drain_ready_entries() {
        Ok(ready_entries) => {
            if !ready_entries.is_empty() {
                if let Err(err) = ready_sender.send(ready_entries) {
                    failure.record(format!("ready entry channel closed: {err}"));
                }
            }
        }
        Err(err) => failure.record(err),
    }
    drop(transaction_notifier);
    drop(entry_notifier);
    drop(block_metadata_notifier);
    drop(ready_sender);
    let _ = ready_handle.join();
    if let Err(err) = scheduler.verify_complete(end_inclusive) {
        failure.record(err);
    }

    // Allow slot status observer to exit cleanly on shutdown.
    drop(confirmed_bank_sender);
    service
        .join()
        .map_err(|err| format!("geyser service join failed: {err:?}"))?;

    if let Some(message) = failure.error_message() {
        return Err(message);
    }

    if let Some(verifier) = snapshot_verifier {
        bank_replay.verify_latest_bank()?;
        verifier.finish()?;
        info!("snapshot verification complete");
    }

    Ok(())
}

async fn extract_tarball(archive: &Path, dest_dir: &Path) -> Result<(), String> {
    let archive = archive.to_path_buf();
    let dest_dir = dest_dir.to_path_buf();
    tokio::task::spawn_blocking(move || {
        fn remove_path_if_exists(path: &Path) -> Result<(), String> {
            if !path.exists() {
                return Ok(());
            }
            let metadata = fs::symlink_metadata(path)
                .map_err(|err| format!("failed to read {}: {err}", path.display()))?;
            if metadata.is_dir() {
                fs::remove_dir_all(path)
                    .map_err(|err| format!("failed to remove {}: {err}", path.display()))?;
            } else {
                fs::remove_file(path)
                    .map_err(|err| format!("failed to remove {}: {err}", path.display()))?;
            }
            Ok(())
        }

        fs::create_dir_all(&dest_dir)
            .map_err(|err| format!("failed to create {}: {err}", dest_dir.display()))?;
        remove_path_if_exists(&dest_dir.join("accounts"))?;
        remove_path_if_exists(&dest_dir.join(BANK_SNAPSHOTS_DIR))?;
        remove_path_if_exists(&dest_dir.join(SNAPSHOT_VERSION_FILE))?;

        let account_path = dest_dir.join("accounts");
        fs::create_dir_all(&account_path)
            .map_err(|err| format!("failed to create {}: {err}", account_path.display()))?;

        if let Ok(archive_info) = FullSnapshotArchiveInfo::new_from_path(archive.clone()) {
            let (sender, receiver) = unbounded();
            let drain = std::thread::spawn(move || for _ in receiver.iter() {});
            let handle = streaming_unarchive_snapshot(
                sender,
                vec![account_path],
                dest_dir.clone(),
                archive,
                archive_info.archive_format(),
                0,
            );
            let result = handle
                .join()
                .map_err(|_| "snapshot unarchive thread panicked".to_string())?;
            result.map_err(|err| format!("snapshot unarchive failed: {err}"))?;
            let _ = drain.join();
            Ok(())
        } else {
            let output = std::process::Command::new("tar")
                .arg("-xf")
                .arg(&archive)
                .arg("-C")
                .arg(&dest_dir)
                .output()
                .map_err(|err| format!("failed to run tar: {err}"))?;
            if !output.status.success() {
                let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
                let command = format!("tar -xf {} -C {}", archive.display(), dest_dir.display());
                if stderr.is_empty() {
                    return Err(format!("{command} failed"));
                }
                return Err(format!("{command} failed: {stderr}"));
            }
            Ok(())
        }
    })
    .await
    .map_err(|err| format!("snapshot unarchive task failed: {err}"))?
}

#[tokio::main]
async fn main() {
    let shutdown = Arc::new(AtomicBool::new(false));
    let cursor = Arc::new(ReplayCursor::new());
    setup_logger(shutdown.clone(), cursor.clone());
    {
        let shutdown = shutdown.clone();
        if let Err(err) = ctrlc::set_handler(move || {
            if !shutdown.swap(true, Ordering::SeqCst) {
                eprintln!("CTRL+C received, shutting down...");
            }
        }) {
            eprintln!("failed to set CTRL+C handler: {err}");
        }
    }
    let mut args = env::args();
    let program = args
        .next()
        .unwrap_or_else(|| "jetstreamer-node".to_string());
    let Some(epoch_arg) = args.next() else {
        eprintln!("{}", usage(&program));
        exit(2);
    };

    if epoch_arg == "-h" || epoch_arg == "--help" {
        println!("{}", usage(&program));
        return;
    }

    let epoch: u64 = match epoch_arg.parse() {
        Ok(epoch) => epoch,
        Err(err) => {
            eprintln!("invalid epoch '{epoch_arg}': {err}");
            eprintln!("{}", usage(&program));
            exit(2);
        }
    };

    let mut dest_dir_arg = None;
    let mut verify_snapshots = false;
    for arg in args {
        if arg == "--verify" {
            verify_snapshots = true;
        } else if arg.starts_with('-') {
            eprintln!("unknown option '{arg}'");
            eprintln!("{}", usage(&program));
            exit(2);
        } else if dest_dir_arg.is_none() {
            dest_dir_arg = Some(PathBuf::from(arg));
        } else {
            eprintln!("unexpected argument '{arg}'");
            eprintln!("{}", usage(&program));
            exit(2);
        }
    }

    let dest_dir = match dest_dir_arg {
        Some(path) => path,
        None => match env::current_dir() {
            Ok(path) => path,
            Err(err) => {
                eprintln!("failed to read current directory: {err}");
                exit(1);
            }
        },
    };

    let target_slot = epoch_to_slot(epoch).saturating_sub(1);
    let mut extracted_snapshot = false;
    let dest_path = match find_existing_snapshot_archive(&dest_dir, target_slot) {
        Ok(Some(candidate)) => {
            extracted_snapshot = match has_extracted_snapshot(&dest_dir, candidate.slot) {
                Ok(has_snapshot) => has_snapshot,
                Err(err) => {
                    eprintln!("error: {err}");
                    exit(1);
                }
            };
            if extracted_snapshot {
                println!(
                    "Found existing snapshot archive at {} with extracted data; skipping download",
                    candidate.path.display()
                );
            } else {
                println!(
                    "Found existing snapshot archive at {}; skipping download",
                    candidate.path.display()
                );
            }
            candidate.path
        }
        Ok(None) => {
            match download_snapshot_at_or_before_slot(epoch, target_slot, &dest_dir).await {
                Ok(path) => {
                    println!("Downloaded snapshot to {}", path.display());
                    path
                }
                Err(err) => {
                    eprintln!("error: {err}");
                    exit(1);
                }
            }
        }
        Err(err) => {
            eprintln!("error: {err}");
            exit(1);
        }
    };

    if env_truthy("JETSTREAMER_SKIP_EXTRACT") {
        println!("Skipping extraction because JETSTREAMER_SKIP_EXTRACT is set");
    } else if extracted_snapshot {
        println!(
            "Skipping extraction because snapshot data already exists in {}",
            dest_dir.display()
        );
    } else {
        println!("Extracting snapshot into {}", dest_dir.display());
        if let Err(err) = extract_tarball(&dest_path, &dest_dir).await {
            eprintln!("error: {err}");
            exit(1);
        }
        println!("Extraction complete");
    }

    let snapshot_verifier = if verify_snapshots {
        info!("collecting canonical snapshot hashes for epoch {epoch}");
        let expected = match snapshot_expectations_for_epoch(epoch).await {
            Ok(expected) => expected,
            Err(err) => {
                eprintln!("error: {err}");
                exit(1);
            }
        };
        info!(
            "snapshot verification enabled for {} snapshot(s)",
            expected.len()
        );
        Some(Arc::new(SnapshotVerifier::new(
            expected,
            Some(shutdown.clone()),
        )))
    } else {
        None
    };

    if let Err(err) = run_geyser_replay(
        epoch,
        &dest_dir,
        &dest_path,
        shutdown,
        cursor,
        snapshot_verifier,
    )
    .await
    {
        eprintln!("error: {err}");
        exit(1);
    }
}
