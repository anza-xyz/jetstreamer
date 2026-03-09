use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    env, fs,
    path::{Path, PathBuf},
    process::{Stdio, exit},
    str::FromStr,
    sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use agave_snapshots::{
    ArchiveFormat, ArchiveFormatDecompressor,
    snapshot_archive_info::{FullSnapshotArchiveInfo, SnapshotArchiveInfoGetter},
    snapshot_hash::SnapshotHash,
    streaming_unarchive_snapshot,
};
use cid::{Cid, multibase::Base};
use crossbeam_channel::{bounded, unbounded};
use dashmap::DashMap;
use jetstreamer_firehose::{
    epochs::{BASE_URL, epoch_to_slot_range, slot_to_epoch},
    firehose::{FirehoseError, GeyserNotifiers, firehose_geyser_with_notifiers},
};
use jetstreamer_node::snapshots::{
    DEFAULT_BUCKET, download_snapshot_at_or_before_slot, list_epoch_snapshots,
};
use log::{error, info, warn};
use reqwest::{Client, Url, header::RANGE};
use serde_cbor::Value;
use solana_account::AccountSharedData;
use solana_accounts_db::{
    accounts_db::AccountsDbConfig,
    accounts_index::{AccountsIndexConfig, IndexLimitMb},
    accounts_update_notifier_interface::{
        AccountForGeyser, AccountsUpdateNotifier, AccountsUpdateNotifierInterface,
    },
};
use solana_address::Address;
use solana_clock::{MAX_PROCESSING_AGE, Slot};
use solana_genesis_utils::{MAX_GENESIS_ARCHIVE_UNPACKED_SIZE, open_genesis_config};
use solana_geyser_plugin_manager::block_metadata_notifier_interface::BlockMetadataNotifier;
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
use solana_svm::{
    transaction_error_metrics::TransactionErrorMetrics,
    transaction_processing_result::TransactionProcessingResultExtensions,
    transaction_processor::{ExecutionRecordingConfig, TransactionProcessingConfig},
};
use solana_svm_timings::ExecuteTimings;
use solana_transaction::{
    TransactionError, VersionedMessage, sanitized::SanitizedTransaction,
    versioned::VersionedTransaction,
};
use solana_transaction_status::TransactionStatusMeta;
use tar::Archive as TarArchive;
use tokio::process::Command;
use xxhash_rust::xxh64::xxh64;

mod plugin;

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
const PROGRAM_CACHE_PRUNE_PROGRESS_INTERVAL: Duration = Duration::from_secs(30);
const ACCOUNTS_MAINTENANCE_PROGRESS_INTERVAL: Duration = Duration::from_secs(30);
const DEFAULT_PROGRAM_CACHE_PRUNE_ENABLED: bool = true;
const DEFAULT_ACCOUNTS_MAINTENANCE_ENABLED: bool = true;
const DEFAULT_ACCOUNTS_MAINTENANCE_ROOT_STRIDE: u64 = 1;
const DEFAULT_ACCOUNTS_INDEX_ON_DISK: bool = false;
const DEFAULT_READY_ENTRY_QUEUE_CAPACITY: usize = 8192;
const MISMATCH_RETRY_ATTEMPTS: usize = 10;
const DEFAULT_POST_FIREHOSE_INCOMPLETE_RETRY_ATTEMPTS: usize = 16;
const DEFAULT_EMPTY_SLOT_BUFFER_GAP_LIMIT: u64 = 0;
const DEFAULT_FIREHOSE_BACKPRESSURE_SLOT_GAP_LIMIT: u64 = 128;
const DEFAULT_FORCE_MISSING_BLOCK_RETRY_LIMIT: usize = 4;
static LOGGED_FIRST_ACCOUNT_UPDATE: AtomicBool = AtomicBool::new(false);
static LOGGED_PROGRAM_CACHE_ASSIGN_FAIL: AtomicBool = AtomicBool::new(false);
static PROGRAM_CACHE_ASSIGN_FAIL_COUNT: AtomicU64 = AtomicU64::new(0);
static PROGRAM_CACHE_PRUNE_DEPLOYMENT_SLOT: AtomicU64 = AtomicU64::new(0);

// Phase timing counters (cumulative microseconds)
static PHASE_GATE_WAIT_US: AtomicU64 = AtomicU64::new(0);
static PHASE_BANK_FOR_SLOT_US: AtomicU64 = AtomicU64::new(0);
static PHASE_PREPARE_BATCH_US: AtomicU64 = AtomicU64::new(0);
static PHASE_EXECUTE_US: AtomicU64 = AtomicU64::new(0);
static PHASE_POST_PROCESS_US: AtomicU64 = AtomicU64::new(0);
static PHASE_ENTRY_COUNT: AtomicU64 = AtomicU64::new(0);

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
    scheduler: Arc<TransactionScheduler>,
    debug_signature: Option<Signature>,
    prune_inflight: Arc<AtomicBool>,
    accounts_maintenance_inflight: Arc<AtomicBool>,
    last_root_set: Arc<AtomicU64>,
    cached_bank: Mutex<Option<CachedBank>>,
    execution_gate: Arc<Mutex<()>>,
    firehose_gate: Arc<Mutex<()>>,
    live_start_slot: Slot,
    enable_program_cache_prune: bool,
    enable_accounts_maintenance: bool,
    accounts_maintenance_root_stride: u64,
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
        mut bank: Bank,
        snapshot_verifier: Option<Arc<SnapshotVerifier>>,
        root_interval: Option<u64>,
        failure: Arc<ReplayFailure>,
        cursor: Arc<ReplayCursor>,
        scheduler: Arc<TransactionScheduler>,
        firehose_gate: Arc<Mutex<()>>,
        live_start_slot: Slot,
        enable_program_cache_prune: bool,
        enable_accounts_maintenance: bool,
        accounts_maintenance_root_stride: u64,
    ) -> Self {
        // Ensure program cache respects deployment slots during replay.
        bank.set_check_program_modification_slot(true);
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
            scheduler,
            debug_signature,
            prune_inflight: Arc::new(AtomicBool::new(false)),
            accounts_maintenance_inflight: Arc::new(AtomicBool::new(false)),
            last_root_set: Arc::new(AtomicU64::new(0)),
            cached_bank,
            execution_gate: Arc::new(Mutex::new(())),
            firehose_gate,
            live_start_slot,
            enable_program_cache_prune,
            enable_accounts_maintenance,
            accounts_maintenance_root_stride: accounts_maintenance_root_stride.max(1),
        }
    }

    fn cached_bank_for_slot(&self, slot: Slot) -> Option<Arc<Bank>> {
        let guard = self.cached_bank.lock().ok()?;
        guard
            .as_ref()
            .and_then(|cached| (cached.slot == slot).then(|| Arc::clone(&cached.bank)))
    }

    fn maybe_prune_program_cache_by_deployment_slot(&self, bank: &Bank) {
        let slot = PROGRAM_CACHE_PRUNE_DEPLOYMENT_SLOT.swap(0, Ordering::Relaxed);
        if slot == 0 {
            return;
        }
        warn!("pruning program cache by deployment slot {}", slot);
        bank.prune_program_cache_by_deployment_slot(slot);
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
            let mut accounts_maintenance_request = None::<Slot>;
            self.cursor.update_inflight_stage("set_root");
            self.leader_schedule_cache.set_root(&frozen_bank);
            self.cursor.update_inflight_stage("slot_leader_at");
            let collector_id = self
                .leader_schedule_cache
                .slot_leader_at(slot, Some(&parent))
                .unwrap_or_else(|| *parent.collector_id());
            self.cursor.update_inflight_stage("new_from_parent");
            let mut next_bank = Bank::new_from_parent(parent, &collector_id, slot);
            next_bank.set_check_program_modification_slot(true);
            self.cursor.update_inflight_stage("set_alpenglow_ticks");
            set_alpenglow_ticks(&next_bank);
            self.cursor.update_inflight_stage("insert_bank");
            let bank_with_scheduler = guard.insert(next_bank);
            if let Some(interval) = self.root_interval
                && interval > 0
            {
                let root_slot = parent_slot.saturating_sub(parent_slot % interval);
                let last_root = self.last_root_set.load(Ordering::Relaxed);
                if root_slot > 0 && root_slot > last_root {
                    self.cursor.update_inflight_stage("root_set");
                    let root_start = Instant::now();
                    if guard.get(root_slot).is_some() {
                        guard.set_root(root_slot, None, None);
                    } else {
                        warn!(
                            "bank_for_slot program cache prune skipped: missing root bank slot {}",
                            root_slot
                        );
                    }
                    self.last_root_set.store(root_slot, Ordering::Relaxed);
                    let set_elapsed = root_start.elapsed();
                    if set_elapsed >= BANK_FOR_SLOT_WARN_AFTER {
                        warn!(
                            "bank_for_slot root set slow: slot {} took {:.3}s",
                            root_slot,
                            set_elapsed.as_secs_f64()
                        );
                    }
                    if self.enable_program_cache_prune {
                        prune_request = Some(root_slot);
                    } else {
                        warn!(
                            "bank_for_slot skipping program cache prune at slot {} (debug)",
                            root_slot
                        );
                    }
                    if self.enable_accounts_maintenance {
                        let root_index = root_slot / interval;
                        if root_index % self.accounts_maintenance_root_stride == 0 {
                            accounts_maintenance_request = Some(root_slot);
                        }
                    }
                }
            }
            self.cursor.update_inflight_stage("clone_without_scheduler");
            let next_bank = bank_with_scheduler.clone_without_scheduler();
            drop(guard);
            if let Some(root_slot) = accounts_maintenance_request {
                let inflight = self.accounts_maintenance_inflight.clone();
                let bank_forks = Arc::clone(&self.bank_forks);
                if inflight
                    .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok()
                {
                    std::thread::spawn(move || {
                        let start = Instant::now();
                        let mut last_progress = Instant::now();
                        let root_bank = loop {
                            match bank_forks.try_read() {
                                Ok(guard) => {
                                    let bank = guard.get(root_slot);
                                    drop(guard);
                                    break bank;
                                }
                                Err(_) => {
                                    if last_progress.elapsed()
                                        >= ACCOUNTS_MAINTENANCE_PROGRESS_INTERVAL
                                    {
                                        warn!(
                                            "accounts maintenance waiting on bank forks read lock: slot {}",
                                            root_slot
                                        );
                                        last_progress = Instant::now();
                                    }
                                    std::thread::sleep(Duration::from_millis(10));
                                }
                            }
                        };
                        let Some(root_bank) = root_bank else {
                            warn!(
                                "accounts maintenance skipped: missing root bank slot {}",
                                root_slot
                            );
                            inflight.store(false, Ordering::SeqCst);
                            return;
                        };
                        info!("accounts maintenance starting at root slot {}", root_slot);
                        root_bank.force_flush_accounts_cache();
                        // Use the previous rooted bank as the clean anchor so cleaning stops at
                        // `root_slot - 2`, which is slightly more conservative.
                        let clean_bank = bank_forks
                            .read()
                            .ok()
                            .and_then(|guard| guard.get(root_slot.saturating_sub(1)))
                            .unwrap_or_else(|| Arc::clone(&root_bank));
                        let clean_anchor_slot = clean_bank.slot();
                        clean_bank.clean_accounts();
                        let shrunk_slots = root_bank.shrink_candidate_slots();
                        info!(
                            "accounts maintenance finished at root slot {} in {:.3}s (clean_anchor_slot={} shrunk_slots={})",
                            root_slot,
                            start.elapsed().as_secs_f64(),
                            clean_anchor_slot,
                            shrunk_slots
                        );
                        inflight.store(false, Ordering::SeqCst);
                    });
                } else {
                    warn!(
                        "accounts maintenance skipped at root slot {} (already running)",
                        root_slot
                    );
                }
            }
            if let Some(prune_slot) = prune_request {
                let inflight = self.prune_inflight.clone();
                let bank_forks = Arc::clone(&self.bank_forks);
                let execution_gate = self.execution_gate.clone();
                let firehose_gate = self.firehose_gate.clone();
                let _cursor = self.cursor.clone();
                let scheduler = self.scheduler.clone();
                if inflight
                    .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok()
                {
                    std::thread::spawn(move || {
                        let start = Instant::now();
                        let mut last_progress = Instant::now();
                        // Acquire firehose_gate FIRST to pause firehose delivery,
                        // allowing the ready_sender channel to drain. If we acquired
                        // execution_gate first, replay would stall, the channel would
                        // fill, and the firehose notifier would block while holding
                        // firehose_gate — deadlocking with us.
                        let _firehose_guard = loop {
                            if let Ok(guard) = firehose_gate.try_lock() {
                                break guard;
                            }
                            if last_progress.elapsed() >= PROGRAM_CACHE_PRUNE_PROGRESS_INTERVAL {
                                warn!(
                                    "bank_for_slot waiting on firehose gate for program cache prune: slot {}",
                                    prune_slot
                                );
                                last_progress = Instant::now();
                            }
                            std::thread::sleep(Duration::from_millis(10));
                        };
                        last_progress = Instant::now();
                        let _execution_guard = loop {
                            if let Ok(guard) = execution_gate.try_lock() {
                                break guard;
                            }
                            if last_progress.elapsed() >= PROGRAM_CACHE_PRUNE_PROGRESS_INTERVAL {
                                warn!(
                                    "bank_for_slot waiting on execution gate for program cache prune: slot {}",
                                    prune_slot
                                );
                                last_progress = Instant::now();
                            }
                            std::thread::sleep(Duration::from_millis(10));
                        };
                        // Pruning pauses firehose delivery but does not imply a restart. Avoid
                        // recording a resume target here to prevent skipping data on normal resume.
                        scheduler.clear_resume_target();
                        let (done_tx, done_rx) = std::sync::mpsc::channel();
                        let bank_forks = Arc::clone(&bank_forks);
                        std::thread::spawn(move || {
                            let mut last_progress = Instant::now();
                            let bank = loop {
                                match bank_forks.try_read() {
                                    Ok(guard) => {
                                        let bank = guard.get(prune_slot);
                                        drop(guard);
                                        break bank;
                                    }
                                    Err(_) => {
                                        if last_progress.elapsed()
                                            >= PROGRAM_CACHE_PRUNE_PROGRESS_INTERVAL
                                        {
                                            warn!(
                                                "bank_for_slot waiting on bank forks read lock for program cache prune: slot {}",
                                                prune_slot
                                            );
                                            last_progress = Instant::now();
                                        }
                                        std::thread::sleep(Duration::from_millis(10));
                                    }
                                }
                            };
                            let Some(bank) = bank else {
                                warn!(
                                    "bank_for_slot program cache prune skipped: missing root bank slot {}",
                                    prune_slot
                                );
                                inflight.store(false, Ordering::SeqCst);
                                let _ = done_tx.send(());
                                return;
                            };
                            info!(
                                "bank_for_slot program cache prune starting: slot {}",
                                prune_slot
                            );
                            let rss_before = read_rss_bytes();
                            bank.prune_program_cache(prune_slot, bank.epoch());
                            let rss_after = read_rss_bytes();
                            let rss_saved = match (rss_before, rss_after) {
                                (Some(before), Some(after)) if before > after => {
                                    format_bytes(before - after)
                                }
                                (Some(_), Some(_)) => "0B".to_string(),
                                _ => "n/a".to_string(),
                            };
                            let elapsed = start.elapsed();
                            info!(
                                "bank_for_slot program cache prune finished: slot {} took {:.3}s rss_saved={}",
                                prune_slot,
                                elapsed.as_secs_f64(),
                                rss_saved
                            );
                            if elapsed >= BANK_FOR_SLOT_WARN_AFTER {
                                warn!(
                                    "bank_for_slot program cache prune async: slot {} took {:.3}s rss_saved={}",
                                    prune_slot,
                                    elapsed.as_secs_f64(),
                                    rss_saved
                                );
                            }
                            inflight.store(false, Ordering::SeqCst);
                            let _ = done_tx.send(());
                        });
                        loop {
                            match done_rx.recv_timeout(PROGRAM_CACHE_PRUNE_PROGRESS_INTERVAL) {
                                Ok(()) => break,
                                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                                    warn!(
                                        "bank_for_slot program cache prune still running after {:.3}s: slot {}",
                                        start.elapsed().as_secs_f64(),
                                        prune_slot
                                    );
                                }
                                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                                    warn!(
                                        "bank_for_slot program cache prune worker disconnected: slot {}",
                                        prune_slot
                                    );
                                    break;
                                }
                            }
                        }
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

    fn note_entry_duration(&self, entry: &ReadyEntry, elapsed: Duration, signature: Option<&str>) {
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

    fn log_mismatch_details(
        &self,
        bank: &Bank,
        entry: &ReadyEntry,
        tx_index: usize,
        offset: usize,
        expected: &Result<(), TransactionError>,
        actual: &Result<(), TransactionError>,
        tx: &VersionedTransaction,
    ) {
        let signatures: Vec<String> = tx.signatures.iter().map(|sig| sig.to_string()).collect();
        let message = &tx.message;
        let version = match message {
            VersionedMessage::Legacy(_) => "legacy",
            VersionedMessage::V0(_) => "v0",
        };
        let header = message.header();
        let static_keys = message.static_account_keys();
        warn!(
            "mismatch detail: slot={} entry={} tx_index={} offset={} bank_slot={} expected={:?} actual={:?} sigs={:?}",
            entry.slot,
            entry.entry_index,
            tx_index,
            offset,
            bank.slot(),
            expected,
            actual,
            signatures
        );
        warn!(
            "mismatch detail: message_version={} recent_blockhash={} header(required_signatures={}, readonly_signed={}, readonly_unsigned={})",
            version,
            message.recent_blockhash(),
            header.num_required_signatures,
            header.num_readonly_signed_accounts,
            header.num_readonly_unsigned_accounts
        );
        warn!(
            "mismatch detail: entry_start_index={} entry_tx_count={} entry_hash={}",
            entry.start_index, entry.tx_count, entry.hash
        );

        let (cursor_slot, cursor_entry, cursor_tx_start, cursor_tx_count, cursor_sig) =
            self.cursor.snapshot();
        warn!(
            "mismatch detail: cursor slot={} entry={} tx_start={} tx_count={} sig={}",
            cursor_slot,
            cursor_entry,
            cursor_tx_start,
            cursor_tx_count,
            cursor_sig.as_deref().unwrap_or("<unknown>")
        );
        if let Some((slot, entry_idx, tx_start, tx_count, sig, stage, elapsed)) =
            self.cursor.inflight_snapshot()
        {
            warn!(
                "mismatch detail: inflight slot={} entry={} tx_start={} tx_count={} stage={} elapsed={:.3}s sig={}",
                slot,
                entry_idx,
                tx_start,
                tx_count,
                stage,
                elapsed.as_secs_f64(),
                sig.as_deref().unwrap_or("<unknown>")
            );
        }
        let snapshot = self.scheduler.snapshot();
        warn!(
            "mismatch detail: scheduler current_slot={} last_finalized={} buffered_slots={} highest_seen_slot={} presence={:?} buffer={:?}",
            snapshot.current_slot,
            snapshot.last_finalized_slot,
            snapshot.buffered_slots,
            snapshot.highest_seen_slot,
            snapshot.presence,
            snapshot.buffer
        );

        for (index, key) in static_keys.iter().enumerate() {
            let signer = message.is_signer(index);
            let writable = message.is_maybe_writable(index, None);
            let invoked = message.is_invoked(index);
            warn!(
                "mismatch detail: key[{}]={} signer={} writable={} invoked_as_program={} source=static",
                index, key, signer, writable, invoked
            );
        }

        if let Some(lookups) = message.address_table_lookups() {
            for (idx, lookup) in lookups.iter().enumerate() {
                warn!(
                    "mismatch detail: address_table_lookup[{}] account_key={} writable_indexes={:?} readonly_indexes={:?}",
                    idx, lookup.account_key, lookup.writable_indexes, lookup.readonly_indexes
                );
            }
        }

        for (ix_idx, ix) in message.instructions().iter().enumerate() {
            let program_index = ix.program_id_index as usize;
            let program_id = static_keys
                .get(program_index)
                .map(|key| key.to_string())
                .unwrap_or_else(|| format!("<lookup:{}>", program_index));
            warn!(
                "mismatch detail: ix[{}] program_index={} program={} accounts={:?} data_len={} data_xxh64={:x}",
                ix_idx,
                program_index,
                program_id,
                ix.accounts,
                ix.data.len(),
                xxh64(&ix.data, 0)
            );
        }
    }

    fn dump_mismatch_artifacts(
        &self,
        bank: &Bank,
        entry: &ReadyEntry,
        tx_index: usize,
        offset: usize,
        expected: &Result<(), TransactionError>,
        actual: &Result<(), TransactionError>,
        tx: Option<&VersionedTransaction>,
    ) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        let (cursor_slot, cursor_entry, cursor_tx_start, cursor_tx_count, cursor_sig) =
            self.cursor.snapshot();
        let inflight = self.cursor.inflight_snapshot();
        let scheduler = self.scheduler.snapshot();

        let mut dump = serde_json::json!({
            "version": 1,
            "timestamp_unix": now.as_secs(),
            "timestamp_nanos": now.subsec_nanos(),
            "slot": entry.slot,
            "entry_index": entry.entry_index,
            "tx_index": tx_index,
            "offset": offset,
            "bank_slot": bank.slot(),
            "expected": format!("{expected:?}"),
            "actual": format!("{actual:?}"),
            "entry": {
                "start_index": entry.start_index,
                "tx_count": entry.tx_count,
                "hash": entry.hash.to_string(),
            },
            "cursor": {
                "slot": cursor_slot,
                "entry": cursor_entry,
                "tx_start": cursor_tx_start,
                "tx_count": cursor_tx_count,
                "sig": cursor_sig.as_deref().unwrap_or("<unknown>"),
            },
            "inflight": inflight.as_ref().map(|(slot, entry_idx, tx_start, tx_count, sig, stage, elapsed)| {
                serde_json::json!({
                    "slot": slot,
                    "entry": entry_idx,
                    "tx_start": tx_start,
                    "tx_count": tx_count,
                    "sig": sig.as_deref().unwrap_or("<unknown>"),
                    "stage": stage,
                    "elapsed_secs": elapsed.as_secs_f64(),
                })
            }),
            "scheduler": {
                "current_slot": scheduler.current_slot,
                "last_finalized": scheduler.last_finalized_slot,
                "buffered_slots": scheduler.buffered_slots,
                "highest_seen_slot": scheduler.highest_seen_slot,
                "presence": format!("{:?}", scheduler.presence),
                "buffer": format!("{:?}", scheduler.buffer),
            },
        });

        if let Some(tx) = tx {
            let signatures: Vec<String> = tx.signatures.iter().map(|sig| sig.to_string()).collect();
            let message = &tx.message;
            let version = match message {
                VersionedMessage::Legacy(_) => "legacy",
                VersionedMessage::V0(_) => "v0",
            };
            let header = message.header();
            let static_keys = message.static_account_keys();
            let static_keys_dump: Vec<serde_json::Value> = static_keys
                .iter()
                .enumerate()
                .map(|(index, key)| {
                    serde_json::json!({
                        "index": index,
                        "key": key.to_string(),
                        "signer": message.is_signer(index),
                        "writable": message.is_maybe_writable(index, None),
                        "invoked_as_program": message.is_invoked(index),
                        "source": "static",
                    })
                })
                .collect();
            let lookup_dump: Vec<serde_json::Value> = message
                .address_table_lookups()
                .map(|lookups| {
                    lookups
                        .iter()
                        .enumerate()
                        .map(|(idx, lookup)| {
                            serde_json::json!({
                                "index": idx,
                                "account_key": lookup.account_key.to_string(),
                                "writable_indexes": lookup.writable_indexes,
                                "readonly_indexes": lookup.readonly_indexes,
                            })
                        })
                        .collect()
                })
                .unwrap_or_default();
            let instructions_dump: Vec<serde_json::Value> = message
                .instructions()
                .iter()
                .enumerate()
                .map(|(ix_idx, ix)| {
                    let program_index = ix.program_id_index as usize;
                    let program_id = static_keys
                        .get(program_index)
                        .map(|key| key.to_string())
                        .unwrap_or_else(|| format!("<lookup:{}>", program_index));
                    serde_json::json!({
                        "index": ix_idx,
                        "program_index": program_index,
                        "program": program_id,
                        "accounts": ix.accounts,
                        "data_len": ix.data.len(),
                        "data_xxh64": format!("{:x}", xxh64(&ix.data, 0)),
                        "data_bs58": bs58::encode(&ix.data).into_string(),
                    })
                })
                .collect();

            dump["transaction"] = serde_json::json!({
                "signatures": signatures,
                "message_version": version,
                "recent_blockhash": message.recent_blockhash().to_string(),
                "header": {
                    "required_signatures": header.num_required_signatures,
                    "readonly_signed": header.num_readonly_signed_accounts,
                    "readonly_unsigned": header.num_readonly_unsigned_accounts,
                },
                "static_keys": static_keys_dump,
                "address_table_lookups": lookup_dump,
                "instructions": instructions_dump,
            });
        }

        let sig_for_name = tx
            .and_then(|tx| tx.signatures.first())
            .map(|sig| sig.to_string())
            .unwrap_or_else(|| "unknown".to_string());
        let dump_dir = Path::new("mismatch-dumps");
        if let Err(err) = fs::create_dir_all(dump_dir) {
            warn!(
                "mismatch dump: failed to create {}: {}",
                dump_dir.display(),
                err
            );
            return;
        }
        let file_name = format!(
            "mismatch-slot{}-entry{}-tx{}-offset{}-sig{}-{}.json",
            entry.slot,
            entry.entry_index,
            tx_index,
            offset,
            sig_for_name,
            now.as_secs()
        );
        let path = dump_dir.join(file_name);
        match serde_json::to_string_pretty(&dump) {
            Ok(serialized) => {
                if let Err(err) = fs::write(&path, serialized) {
                    warn!("mismatch dump: failed to write {}: {}", path.display(), err);
                } else {
                    warn!("mismatch dump: wrote {}", path.display());
                }
            }
            Err(err) => {
                warn!("mismatch dump: failed to serialize: {err}");
            }
        }
    }

    fn retry_mismatch_transaction(
        &self,
        bank: &Bank,
        entry: &ReadyEntry,
        tx_index: usize,
        offset: usize,
        attempt: usize,
        expected: &Result<(), TransactionError>,
        tx: &VersionedTransaction,
    ) -> Option<Result<(), TransactionError>> {
        self.cursor.update_inflight_stage("mismatch_retry_prepare");
        let batch = match bank.prepare_entry_batch(vec![tx.clone()]) {
            Ok(batch) => batch,
            Err(err) => {
                warn!(
                    "mismatch retry: failed to prepare batch at slot {} entry {} tx_index {} attempt {}: {}",
                    entry.slot, entry.entry_index, tx_index, attempt, err
                );
                return None;
            }
        };
        self.cursor.update_inflight_stage("mismatch_retry_execute");
        let mut timings = ExecuteTimings::default();
        let mut error_metrics = TransactionErrorMetrics::default();
        let output = bank.load_and_execute_transactions(
            &batch,
            MAX_PROCESSING_AGE,
            &mut timings,
            &mut error_metrics,
            TransactionProcessingConfig {
                recording_config: ExecutionRecordingConfig::new_single_setting(false),
                ..TransactionProcessingConfig::default()
            },
        );
        let retry_result = output
            .processing_results
            .into_iter()
            .next()
            .map(|processing_result| processing_result.flattened_result());
        match retry_result {
            Some(actual) => {
                warn!(
                    "mismatch retry: slot={} entry={} tx_index={} offset={} attempt={} expected={:?} actual={:?}",
                    entry.slot, entry.entry_index, tx_index, offset, attempt, expected, actual
                );
                Some(actual)
            }
            None => {
                warn!(
                    "mismatch retry: no result at slot {} entry {} tx_index {} attempt {}",
                    entry.slot, entry.entry_index, tx_index, attempt
                );
                None
            }
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
            PHASE_GATE_WAIT_US.fetch_add(gate_wait.as_micros() as u64, Ordering::Relaxed);
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
                } else {
                    // Only advance the replay cursor after a successful tick registration.
                    self.cursor.update(
                        entry.slot,
                        entry.entry_index,
                        entry.start_index,
                        entry.tx_count,
                        signature.clone(),
                    );
                }
                let elapsed = start.elapsed();
                self.note_entry_duration(&entry, elapsed, None);
                continue;
            }

            self.cursor.update_inflight_stage("bank_for_slot");
            let start = Instant::now();
            let phase_bank_start = Instant::now();
            match self.bank_for_slot(entry.slot) {
                Ok(bank) => {
                    let bank_for_slot_us = phase_bank_start.elapsed().as_micros() as u64;
                    PHASE_BANK_FOR_SLOT_US.fetch_add(bank_for_slot_us, Ordering::Relaxed);
                    self.maybe_prune_program_cache_by_deployment_slot(&bank);
                    self.cursor.update_inflight_stage("try_process");
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
                    let phase_prepare_start = Instant::now();
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
                    PHASE_PREPARE_BATCH_US.fetch_add(
                        phase_prepare_start.elapsed().as_micros() as u64,
                        Ordering::Relaxed,
                    );
                    self.cursor.update_inflight_stage("load_execute_and_commit");
                    let phase_exec_start = Instant::now();
                    let mut timings = ExecuteTimings::default();
                    let (commit_results, _balance_collector) = bank
                        .load_execute_and_commit_transactions(
                            &batch,
                            MAX_PROCESSING_AGE,
                            ExecutionRecordingConfig::new_single_setting(false),
                            &mut timings,
                            None,
                        );
                    let results: Vec<Result<(), TransactionError>> = commit_results
                        .into_iter()
                        .map(|commit_result| commit_result.and_then(|committed| committed.status))
                        .collect();
                    PHASE_EXECUTE_US.fetch_add(
                        phase_exec_start.elapsed().as_micros() as u64,
                        Ordering::Relaxed,
                    );
                    drop(batch);
                    self.cursor.update_inflight_stage("post_process");
                    let phase_post_start = Instant::now();

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
                            let mut resolved = false;
                            if MISMATCH_RETRY_ATTEMPTS > 0
                                && let Some(scheduled) = entry.txs.get(offset)
                            {
                                for attempt in 1..=MISMATCH_RETRY_ATTEMPTS {
                                    if attempt > 1 {
                                        let backoff_ms =
                                            50_u64.saturating_mul(1_u64 << (attempt - 2));
                                        std::thread::sleep(Duration::from_millis(
                                            backoff_ms.min(300_000),
                                        ));
                                    }
                                    if let Some(retry_result) = self.retry_mismatch_transaction(
                                        &bank,
                                        &entry,
                                        tx_index,
                                        offset,
                                        attempt,
                                        &expected,
                                        &scheduled.tx,
                                    ) {
                                        if retry_result == expected {
                                            warn!(
                                                "mismatch resolved after retry: slot {} entry {} tx_index {} sig {}",
                                                entry.slot, entry.entry_index, tx_index, signature
                                            );
                                            resolved = true;
                                            break;
                                        }
                                    } else {
                                        break;
                                    }
                                }
                            }
                            if resolved {
                                continue;
                            }
                            if let Some(scheduled) = entry.txs.get(offset) {
                                self.log_mismatch_details(
                                    &bank,
                                    &entry,
                                    tx_index,
                                    offset,
                                    &expected,
                                    &actual,
                                    &scheduled.tx,
                                );
                                self.dump_mismatch_artifacts(
                                    &bank,
                                    &entry,
                                    tx_index,
                                    offset,
                                    &expected,
                                    &actual,
                                    Some(&scheduled.tx),
                                );
                            } else {
                                error!(
                                    "mismatch detail: missing scheduled tx at offset {} (slot {} entry {})",
                                    offset, entry.slot, entry.entry_index
                                );
                                self.dump_mismatch_artifacts(
                                    &bank, &entry, tx_index, offset, &expected, &actual, None,
                                );
                            }
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
                    if entry.slot >= self.live_start_slot {
                        plugin::notify_transaction_range(
                            entry.slot,
                            entry.start_index,
                            entry.tx_count,
                        );
                    }
                    PHASE_POST_PROCESS_US.fetch_add(
                        phase_post_start.elapsed().as_micros() as u64,
                        Ordering::Relaxed,
                    );
                    PHASE_ENTRY_COUNT.fetch_add(1, Ordering::Relaxed);
                    // Advance the replay cursor only after successful execution/verification.
                    self.cursor.update(
                        entry.slot,
                        entry.entry_index,
                        entry.start_index,
                        entry.tx_count,
                        signature.clone(),
                    );
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
        self.last_account_update_slot.store(slot, Ordering::Relaxed);
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

#[derive(Debug)]
struct RestartTracker {
    pending: AtomicBool,
    slot: AtomicU64,
    entry_index: AtomicU64,
    tx_start: AtomicU64,
    allow_stale_skip: AtomicBool,
}

impl RestartTracker {
    fn new() -> Self {
        Self {
            pending: AtomicBool::new(false),
            slot: AtomicU64::new(0),
            entry_index: AtomicU64::new(0),
            tx_start: AtomicU64::new(0),
            allow_stale_skip: AtomicBool::new(false),
        }
    }

    fn mark_restart(
        &self,
        slot: Slot,
        entry_index: usize,
        tx_start: usize,
        allow_stale_skip: bool,
    ) {
        self.slot.store(slot, Ordering::Relaxed);
        self.entry_index
            .store(entry_index as u64, Ordering::Relaxed);
        self.tx_start.store(tx_start as u64, Ordering::Relaxed);
        self.allow_stale_skip
            .store(allow_stale_skip, Ordering::Relaxed);
        self.pending.store(true, Ordering::Relaxed);
    }

    fn take_if_applicable(&self, slot: Slot) -> Option<ResumeTarget> {
        if !self.pending.load(Ordering::Relaxed) {
            return None;
        }
        let target_slot = self.slot.load(Ordering::Relaxed);
        let target_entry_index = self.entry_index.load(Ordering::Relaxed) as usize;
        let target_tx_start = self.tx_start.load(Ordering::Relaxed) as usize;
        let allow_stale_skip = self.allow_stale_skip.load(Ordering::Relaxed);
        self.pending.store(false, Ordering::Relaxed);
        // Firehose can report a restart slot and then resume from a later slot. If we have no
        // resume cursor for that restart target, rolling scheduler state back to that slot
        // creates an empty "present" slot that never receives data and blocks replay forever.
        if allow_stale_skip && slot > target_slot && target_entry_index == 0 && target_tx_start == 0
        {
            warn!(
                "ignoring stale restart target: target_slot={} current_slot={} (no resume cursor)",
                target_slot, slot
            );
            return None;
        }
        let resume_slot = if slot < target_slot {
            slot
        } else {
            target_slot
        };
        let (entry_index, tx_start) = if resume_slot == target_slot {
            (target_entry_index, target_tx_start)
        } else {
            (0, 0)
        };
        Some(ResumeTarget {
            slot: resume_slot,
            entry_index,
            tx_start,
        })
    }
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
                started_at: Instant::now(),
            });
        }
    }

    fn update_inflight_stage(&self, stage: &'static str) {
        if let Ok(mut guard) = self.inflight.lock()
            && let Some(ref mut entry) = *guard
        {
            entry.stage = stage;
        }
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

fn extract_program_cache_deployment_slot(message: &str) -> Option<u64> {
    let marker = "entry=ProgramCacheEntry";
    let (_, after_marker) = message.split_once(marker)?;
    let key = "deployment_slot:";
    let (_, after_key) = after_marker.split_once(key)?;
    let digits: String = after_key
        .trim_start()
        .chars()
        .take_while(|ch| ch.is_ascii_digit())
        .collect();
    if digits.is_empty() {
        None
    } else {
        digits.parse().ok()
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
    started_at: Instant,
}

struct AbortOnErrorLogger {
    inner: env_logger::Logger,
    shutdown: Arc<AtomicBool>,
    abort_on_error: bool,
    cursor: Arc<ReplayCursor>,
    restart_tracker: Arc<RestartTracker>,
}

impl log::Log for AbortOnErrorLogger {
    fn enabled(&self, metadata: &log::Metadata) -> bool {
        if metadata.level() == log::Level::Error {
            return true;
        }
        self.inner.enabled(metadata)
    }

    fn log(&self, record: &log::Record) {
        if record.target().starts_with("jetstreamer::firehose") {
            let message = record.args().to_string();
            if let Some((restart_slot, _item_index)) = parse_firehose_restart_info(&message) {
                let mut entry_index = 0usize;
                let mut tx_start = 0usize;
                if let Some((slot, entry, tx_start_snapshot, _tx_count, _sig, _stage, _elapsed)) =
                    self.cursor.inflight_snapshot()
                {
                    if slot == restart_slot {
                        entry_index = entry as usize;
                        tx_start = tx_start_snapshot as usize;
                    }
                } else {
                    let (slot, entry, tx_start_snapshot, tx_count, _sig) = self.cursor.snapshot();
                    if slot == restart_slot {
                        entry_index = entry.saturating_add(1) as usize;
                        tx_start = tx_start_snapshot.saturating_add(tx_count) as usize;
                    }
                }
                self.restart_tracker
                    .mark_restart(restart_slot, entry_index, tx_start, true);
            }
        }
        if record.target() == "solana_program_runtime::loaded_programs"
            && record
                .args()
                .to_string()
                .contains("ProgramCache::assign_program() failed")
        {
            PROGRAM_CACHE_ASSIGN_FAIL_COUNT.fetch_add(1, Ordering::Relaxed);
            let message = record.args().to_string();
            if let Some(deployment_slot) = extract_program_cache_deployment_slot(&message) {
                PROGRAM_CACHE_PRUNE_DEPLOYMENT_SLOT.store(deployment_slot, Ordering::Relaxed);
            }
            if LOGGED_PROGRAM_CACHE_ASSIGN_FAIL.swap(true, Ordering::SeqCst) {
                return;
            }
            if let Some(deployment_slot) = extract_program_cache_deployment_slot(&message) {
                eprintln!(
                    "suppressing repeated ProgramCache::assign_program() failed logs (will prune deployment_slot={deployment_slot})"
                );
            } else {
                eprintln!(
                    "suppressing repeated ProgramCache::assign_program() failed logs (benign)"
                );
            }
            return;
        }

        if record.target() == "solana_accounts_db::accounts_db" {
            let message = record.args().to_string();
            if message.starts_with("remove_dead_slots_metadata") {
                return;
            }
        }

        self.inner.log(record);
        if self.abort_on_error && record.level() == log::Level::Error {
            if record.target().starts_with("jetstreamer::firehose") {
                // Firehose handles retries/rollbacks internally; do not abort the process on
                // any firehose error logs.
                return;
            }
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

fn setup_logger(
    shutdown: Arc<AtomicBool>,
    cursor: Arc<ReplayCursor>,
    restart_tracker: Arc<RestartTracker>,
) {
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
        restart_tracker,
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

fn parse_firehose_restart_info(message: &str) -> Option<(u64, u64)> {
    let needle = "restarting from slot ";
    let start = message.find(needle)? + needle.len();
    let after = &message[start..];
    let slot_digits: String = after.chars().take_while(|ch| ch.is_ascii_digit()).collect();
    let slot: u64 = slot_digits.parse().ok()?;
    let index = if let Some(index_start) = after.find(" at index ") {
        let after_index = &after[index_start + " at index ".len()..];
        let index_digits: String = after_index
            .chars()
            .take_while(|ch| ch.is_ascii_digit())
            .collect();
        index_digits.parse().unwrap_or(0)
    } else {
        0
    };
    Some((slot, index))
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
    resume_target: Mutex<Option<ResumeTarget>>,
    restart_tracker: Arc<RestartTracker>,
    empty_slot_buffer_gap_limit: u64,
}

#[derive(Debug)]
struct SchedulerState {
    last_finalized_slot: Slot,
    current_slot: Slot,
    slots: HashMap<Slot, SlotExecutionBuffer>,
    inferred_blocks: HashMap<Slot, (u64, u64)>,
    forced_missing: HashSet<Slot>,
    highest_seen_slot: Slot,
}

#[derive(Debug, Clone, Copy)]
struct ResumeTarget {
    slot: Slot,
    entry_index: usize,
    tx_start: usize,
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

#[derive(Debug, Clone, Copy)]
enum IncompleteReason {
    BufferedData,
    MissingBlockData,
}

#[derive(Debug)]
struct IncompleteSlotInfo {
    slot: Slot,
    entry_index: usize,
    tx_start: usize,
    reason: IncompleteReason,
    snapshot: Option<SlotBufferSnapshot>,
    presence: Option<SlotPresenceState>,
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
    fn new(
        start_slot: Slot,
        presence: Arc<SlotPresenceMap>,
        restart_tracker: Arc<RestartTracker>,
        empty_slot_buffer_gap_limit: u64,
    ) -> Self {
        Self {
            state: Mutex::new(SchedulerState {
                last_finalized_slot: start_slot.saturating_sub(1),
                current_slot: start_slot,
                slots: HashMap::new(),
                inferred_blocks: HashMap::new(),
                forced_missing: HashSet::new(),
                highest_seen_slot: start_slot.saturating_sub(1),
            }),
            presence,
            resume_target: Mutex::new(None),
            restart_tracker,
            empty_slot_buffer_gap_limit,
        }
    }

    #[allow(dead_code)]
    fn set_resume_target(&self, slot: Slot, entry_index: usize, tx_start: usize) {
        if slot == 0 {
            return;
        }
        let mut guard = self.resume_target.lock().expect("resume target lock");
        *guard = Some(ResumeTarget {
            slot,
            entry_index,
            tx_start,
        });
        info!(
            "firehose pause recorded resume target: slot {} entry {} tx_index {}",
            slot, entry_index, tx_start
        );
    }

    fn clear_resume_target(&self) {
        let mut guard = self.resume_target.lock().expect("resume target lock");
        *guard = None;
    }

    fn slot_presence_locked(
        &self,
        state: &SchedulerState,
        slot: Slot,
    ) -> Option<SlotPresenceState> {
        if state.forced_missing.contains(&slot) {
            return Some(SlotPresenceState::Missing);
        }
        self.presence.state(slot)
    }

    fn is_missing_locked(&self, state: &SchedulerState, slot: Slot) -> Option<bool> {
        self.slot_presence_locked(state, slot)
            .map(|presence| presence == SlotPresenceState::Missing)
    }

    fn apply_restart_locked(&self, state: &mut SchedulerState, target: ResumeTarget) {
        let restart_slot = target.slot;
        let resume_entry = target.entry_index;
        let resume_tx = target.tx_start;
        state.slots.retain(|slot, _| *slot < restart_slot);
        state.inferred_blocks.retain(|slot, _| *slot < restart_slot);
        if state.current_slot >= restart_slot {
            state.current_slot = restart_slot;
        }
        if state.last_finalized_slot >= restart_slot {
            state.last_finalized_slot = restart_slot.saturating_sub(1);
        }
        state.highest_seen_slot = state
            .slots
            .keys()
            .copied()
            .max()
            .unwrap_or(state.last_finalized_slot);
        let buffer = state.slots.entry(restart_slot).or_default();
        buffer.reset_for_restart();
        buffer.apply_resume_counts(resume_entry, resume_tx);
        info!(
            "firehose restart detected; restarting slot {} from entry {} tx_index {}",
            restart_slot, resume_entry, resume_tx
        );
    }

    #[allow(dead_code)]
    fn current_resume_target(&self) -> Option<ResumeTarget> {
        let state = self.state.lock().expect("transaction scheduler lock");
        let slot = state.current_slot;
        if slot == 0 {
            return None;
        }
        if let Some(buffer) = state.slots.get(&slot) {
            return Some(ResumeTarget {
                slot,
                entry_index: buffer.processed_entry_count as usize,
                tx_start: buffer.processed_tx_count as usize,
            });
        }
        Some(ResumeTarget {
            slot,
            entry_index: 0,
            tx_start: 0,
        })
    }

    fn take_resume_target(&self, slot: Slot) -> Option<ResumeTarget> {
        let mut guard = self.resume_target.lock().expect("resume target lock");
        let target = guard.as_ref()?;
        if target.slot == slot {
            return guard.take();
        }
        if target.slot < slot {
            warn!(
                "resume target missed: target_slot={} current_slot={}",
                target.slot, slot
            );
            *guard = None;
        }
        None
    }

    fn insert_transaction(
        &self,
        slot: Slot,
        index: usize,
        tx: VersionedTransaction,
        expected_status: Result<(), TransactionError>,
    ) -> Result<(Vec<ReadyEntry>, bool), String> {
        let mut state = self.state.lock().expect("transaction scheduler lock");
        if let Some(target) = self.restart_tracker.take_if_applicable(slot) {
            self.apply_restart_locked(&mut state, target);
        } else if let Some(target) = self.take_resume_target(slot) {
            state.inferred_blocks.remove(&slot);
            let buffer = state.slots.entry(slot).or_default();
            buffer.reset_for_restart();
            buffer.apply_resume_counts(target.entry_index, target.tx_start);
            info!(
                "firehose resume detected; resuming slot {} from entry {} tx_index {}",
                slot, target.entry_index, target.tx_start
            );
        }
        if slot <= state.last_finalized_slot {
            return Err(format!(
                "late transaction for slot {slot} (last finalized slot {})",
                state.last_finalized_slot
            ));
        }
        if slot > state.highest_seen_slot {
            state.highest_seen_slot = slot;
        }
        let buffer = state.slots.entry(slot).or_default();
        let inserted = buffer.insert_transaction(index, tx, expected_status)?;
        let ready = self.advance_ready_locked(&mut state)?;
        Ok((ready, inserted))
    }

    fn push_entry(
        &self,
        slot: Slot,
        entry_index: usize,
        start_index: usize,
        tx_count: usize,
        hash: Hash,
    ) -> Result<(Vec<ReadyEntry>, bool), String> {
        let mut state = self.state.lock().expect("transaction scheduler lock");
        if let Some(target) = self.restart_tracker.take_if_applicable(slot) {
            self.apply_restart_locked(&mut state, target);
        } else if let Some(target) = self.take_resume_target(slot) {
            state.inferred_blocks.remove(&slot);
            let buffer = state.slots.entry(slot).or_default();
            buffer.reset_for_restart();
            buffer.apply_resume_counts(target.entry_index, target.tx_start);
            info!(
                "firehose resume detected; resuming slot {} from entry {} tx_index {}",
                slot, target.entry_index, target.tx_start
            );
        }
        if slot <= state.last_finalized_slot {
            return Err(format!(
                "late entry for slot {slot} (last finalized slot {})",
                state.last_finalized_slot
            ));
        }
        if slot > state.highest_seen_slot {
            state.highest_seen_slot = slot;
        }
        let buffer = state.slots.entry(slot).or_default();
        let inserted = buffer.push_entry(entry_index, start_index, tx_count, hash)?;
        let ready = self.advance_ready_locked(&mut state)?;
        Ok((ready, inserted))
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
        if let Some((inferred_tx, inferred_entry)) = state.inferred_blocks.remove(&slot)
            && (inferred_tx != expected_tx_count || inferred_entry != expected_entry_count)
        {
            return Err(format!(
                "block metadata mismatch for slot {slot}: inferred txs {inferred_tx} entries {inferred_entry}, got txs {expected_tx_count} entries {expected_entry_count}"
            ));
        }
        let buffer = state.slots.entry(slot).or_default();
        buffer.set_expected_counts(expected_tx_count, expected_entry_count)?;
        self.advance_ready_locked(&mut state)
    }

    fn drain_ready_entries(&self) -> Result<Vec<ReadyEntry>, String> {
        let mut state = self.state.lock().expect("transaction scheduler lock");
        self.advance_ready_locked(&mut state)
    }

    fn force_mark_missing(&self, slot: Slot) -> Result<Vec<ReadyEntry>, String> {
        let mut state = self.state.lock().expect("transaction scheduler lock");
        let presence = self.presence.state(slot);
        if presence.is_none() {
            return Err(format!(
                "cannot force missing for slot {} outside scheduler range",
                slot
            ));
        }
        if let Some(buffer) = state.slots.get(&slot)
            && buffer.has_any_data()
        {
            return Err(format!(
                "cannot force missing for slot {} because buffered data exists",
                slot
            ));
        }
        state.forced_missing.insert(slot);
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
            match self.slot_presence_locked(&state, slot) {
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

    fn first_incomplete_slot(&self, end_inclusive: Slot) -> Option<IncompleteSlotInfo> {
        let state = self.state.lock().expect("transaction scheduler lock");
        let mut buffered: Option<(Slot, SlotBufferSnapshot, usize, usize)> = None;
        for (slot, buffer) in state.slots.iter() {
            if *slot <= end_inclusive && buffer.has_any_data() {
                let snapshot = SlotBufferSnapshot {
                    expected_tx_count: buffer.expected_tx_count,
                    expected_entry_count: buffer.expected_entry_count,
                    processed_tx_count: buffer.processed_tx_count,
                    processed_entry_count: buffer.processed_entry_count,
                    pending_entries: buffer.pending_entries.len(),
                    buffered_txs: buffer.buffered_transaction_count(),
                    next_entry_index: buffer.next_entry_index,
                };
                let entry_index = buffer.processed_entry_count as usize;
                let tx_start = buffer.processed_tx_count as usize;
                let replace = buffered
                    .as_ref()
                    .map(|(existing_slot, _, _, _)| *slot < *existing_slot)
                    .unwrap_or(true);
                if replace {
                    buffered = Some((*slot, snapshot, entry_index, tx_start));
                }
            }
        }

        let mut slot = state.current_slot;
        let mut missing: Option<IncompleteSlotInfo> = None;
        while slot <= end_inclusive {
            match self.slot_presence_locked(&state, slot) {
                Some(SlotPresenceState::Missing) => slot = slot.saturating_add(1),
                Some(SlotPresenceState::Present) => {
                    missing = Some(IncompleteSlotInfo {
                        slot,
                        entry_index: 0,
                        tx_start: 0,
                        reason: IncompleteReason::MissingBlockData,
                        snapshot: None,
                        presence: Some(SlotPresenceState::Present),
                    });
                    break;
                }
                None => break,
            }
        }

        match (buffered, missing) {
            (Some((slot, snapshot, entry_index, tx_start)), Some(missing)) => {
                if missing.slot <= slot {
                    Some(missing)
                } else {
                    Some(IncompleteSlotInfo {
                        slot,
                        entry_index,
                        tx_start,
                        reason: IncompleteReason::BufferedData,
                        snapshot: Some(snapshot),
                        presence: self.slot_presence_locked(&state, slot),
                    })
                }
            }
            (Some((slot, snapshot, entry_index, tx_start)), None) => Some(IncompleteSlotInfo {
                slot,
                entry_index,
                tx_start,
                reason: IncompleteReason::BufferedData,
                snapshot: Some(snapshot),
                presence: self.slot_presence_locked(&state, slot),
            }),
            (None, Some(missing)) => Some(missing),
            (None, None) => None,
        }
    }

    fn snapshot(&self) -> SchedulerSnapshot {
        let state = self.state.lock().expect("transaction scheduler lock");
        let current_slot = state.current_slot;
        let presence = self.slot_presence_locked(&state, current_slot);
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
            if state.highest_seen_slot > current_slot {
                let current_has_data = state
                    .slots
                    .get(&current_slot)
                    .map(|buffer| buffer.has_any_data())
                    .unwrap_or(false);
                if !current_has_data {
                    let gap = state.highest_seen_slot.saturating_sub(current_slot);
                    if self.empty_slot_buffer_gap_limit > 0
                        && gap >= self.empty_slot_buffer_gap_limit
                    {
                        return Err(format!(
                            "scheduler stalled: current slot {} has no buffered data while highest seen slot is {} (gap {}, limit {})",
                            current_slot,
                            state.highest_seen_slot,
                            gap,
                            self.empty_slot_buffer_gap_limit
                        ));
                    }
                }
            }
            match self.is_missing_locked(state, current_slot) {
                Some(true) => {
                    if let Some(buffer) = state.slots.remove(&current_slot)
                        && buffer.has_any_data()
                    {
                        return Err(format!(
                            "slot {} marked leader skipped but contains buffered data",
                            current_slot
                        ));
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
            if state.highest_seen_slot > current_slot
                && let Some((txs, entries)) = buffer.infer_expected_counts_if_missing(current_slot)
            {
                state.inferred_blocks.insert(current_slot, (txs, entries));
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
    fn reset_for_restart(&mut self) {
        self.txs.clear();
        self.pending_entries.clear();
        self.next_entry_index = 0;
        self.processed_tx_count = 0;
        self.processed_entry_count = 0;
        self.expected_tx_count = None;
        self.expected_entry_count = None;
    }

    fn apply_resume_counts(&mut self, entry_index: usize, tx_start: usize) {
        self.next_entry_index = entry_index;
        self.processed_entry_count = entry_index as u64;
        self.processed_tx_count = tx_start as u64;
    }

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
    ) -> Result<bool, String> {
        if (index as u64) < self.processed_tx_count {
            // Duplicate transaction after a firehose restart; already processed.
            return Ok(false);
        }
        if self.txs.len() <= index {
            self.txs.resize_with(index + 1, || None);
        }
        if let Some(existing) = &self.txs[index] {
            let existing_sig = existing.tx.signatures.first();
            let incoming_sig = tx.signatures.first();
            if existing_sig == incoming_sig && existing.expected_status == expected_status {
                // Duplicate delivery of the same transaction; ignore.
                return Ok(false);
            }
            return Err(format!(
                "duplicate transaction at index {index} (existing_sig={:?}, incoming_sig={:?})",
                existing_sig, incoming_sig
            ));
        }
        self.txs[index] = Some(ScheduledTransaction {
            tx,
            expected_status,
        });
        Ok(true)
    }

    fn push_entry(
        &mut self,
        entry_index: usize,
        start_index: usize,
        tx_count: usize,
        hash: Hash,
    ) -> Result<bool, String> {
        if entry_index < self.next_entry_index {
            // Duplicate entry after a firehose restart; already processed.
            return Ok(false);
        }
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
        Ok(true)
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
            let processed_tx = self.processed_tx_count as usize;
            if processed_tx >= end {
                let _entry = self.pending_entries.pop_front().expect("pending entry");
                self.processed_entry_count = self.processed_entry_count.saturating_add(1);
                continue;
            }
            let effective_start = entry.start_index.max(processed_tx);
            let effective_count = end.saturating_sub(effective_start);
            if self.txs.len() < end {
                break;
            }
            let mut missing = false;
            for idx in effective_start..end {
                if self.txs[idx].is_none() {
                    missing = true;
                    break;
                }
            }
            if missing {
                break;
            }

            let entry = self.pending_entries.pop_front().expect("pending entry");
            let mut txs = Vec::with_capacity(effective_count);
            for idx in effective_start..end {
                let tx = self.txs[idx].take().expect("transaction present");
                txs.push(tx);
            }
            self.processed_entry_count = self.processed_entry_count.saturating_add(1);
            self.processed_tx_count = self
                .processed_tx_count
                .saturating_add(effective_count as u64);
            ready.push(ReadyEntry {
                slot,
                entry_index: entry.entry_index,
                start_index: effective_start,
                txs,
                hash: entry.hash,
                tx_count: effective_count,
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
    live_start_slot: Slot,
}

impl AccountsUpdateNotifierInterface for ProgressAccountsUpdateNotifier {
    fn snapshot_notifications_enabled(&self) -> bool {
        true
    }

    fn notify_account_update(
        &self,
        slot: Slot,
        account: &AccountSharedData,
        txn: &Option<&SanitizedTransaction>,
        pubkey: &solana_pubkey::Pubkey,
        write_version: u64,
    ) {
        let address = Address::new_from_array(pubkey.to_bytes());
        if !LOGGED_FIRST_ACCOUNT_UPDATE.swap(true, Ordering::SeqCst) {
            info!(
                "first account update: slot={} pubkey={} write_version={}",
                slot, address, write_version
            );
        }
        self.progress.note_account_update_slot(slot);
        self.progress.inc_account_update();
        if slot >= self.live_start_slot {
            plugin::notify_account_update(slot, account, txn, pubkey, write_version);
        }
    }

    fn notify_account_restore_from_snapshot(
        &self,
        _slot: Slot,
        _write_version: u64,
        _account: &AccountForGeyser<'_>,
    ) {
        plugin::notify_startup_account();
    }

    fn notify_end_of_restore_from_snapshot(&self) {
        plugin::notify_end_of_startup();
    }
}

struct BankTransactionNotifier {
    progress: Arc<ReplayProgress>,
    scheduler: Arc<TransactionScheduler>,
    failure: Arc<ReplayFailure>,
    ready_sender: crossbeam_channel::Sender<Vec<ReadyEntry>>,
    shutdown: Arc<AtomicBool>,
    active_firehose_stop: Arc<Mutex<Option<Arc<AtomicBool>>>>,
    backpressure_stop_requested: Arc<AtomicBool>,
    firehose_backpressure_slot_gap_limit: u64,
    firehose_gate: Arc<Mutex<()>>,
}

impl TransactionNotifier for BankTransactionNotifier {
    fn notify_transaction(
        &self,
        slot: Slot,
        transaction_slot_index: usize,
        _signature: &Signature,
        _message_hash: &Hash,
        _is_vote: bool,
        transaction_status_meta: &TransactionStatusMeta,
        transaction: &VersionedTransaction,
    ) {
        if !enforce_firehose_backpressure(
            slot,
            &self.scheduler,
            &self.shutdown,
            &self.active_firehose_stop,
            &self.backpressure_stop_requested,
            self.firehose_backpressure_slot_gap_limit,
        ) {
            return;
        }
        let _firehose_guard = self
            .firehose_gate
            .lock()
            .expect("firehose gate lock poisoned");
        match self.scheduler.insert_transaction(
            slot,
            transaction_slot_index,
            transaction.clone(),
            transaction_status_meta.status.clone(),
        ) {
            Ok((ready_entries, inserted)) => {
                if inserted {
                    self.progress.note_tx_slot(slot);
                    self.progress.inc_tx();
                }
                send_ready_entries(&self.ready_sender, &self.failure, ready_entries);
            }
            Err(err) => self.failure.record(err),
        }
    }
}

struct BankEntryNotifier {
    progress: Arc<ReplayProgress>,
    scheduler: Arc<TransactionScheduler>,
    failure: Arc<ReplayFailure>,
    ready_sender: crossbeam_channel::Sender<Vec<ReadyEntry>>,
    shutdown: Arc<AtomicBool>,
    active_firehose_stop: Arc<Mutex<Option<Arc<AtomicBool>>>>,
    backpressure_stop_requested: Arc<AtomicBool>,
    firehose_backpressure_slot_gap_limit: u64,
    firehose_gate: Arc<Mutex<()>>,
}

impl EntryNotifier for BankEntryNotifier {
    fn notify_entry(
        &self,
        slot: Slot,
        index: usize,
        entry: &solana_entry::entry::EntrySummary,
        starting_transaction_index: usize,
    ) {
        if !enforce_firehose_backpressure(
            slot,
            &self.scheduler,
            &self.shutdown,
            &self.active_firehose_stop,
            &self.backpressure_stop_requested,
            self.firehose_backpressure_slot_gap_limit,
        ) {
            return;
        }
        let _firehose_guard = self
            .firehose_gate
            .lock()
            .expect("firehose gate lock poisoned");
        match self.scheduler.push_entry(
            slot,
            index,
            starting_transaction_index,
            entry.num_transactions as usize,
            entry.hash,
        ) {
            Ok((ready_entries, inserted)) => {
                if inserted {
                    self.progress.note_entry_slot(slot);
                }
                send_ready_entries(&self.ready_sender, &self.failure, ready_entries);
            }
            Err(err) => self.failure.record(err),
        }
    }
}

struct BankBlockMetadataNotifier {
    scheduler: Arc<TransactionScheduler>,
    progress: Arc<ReplayProgress>,
    failure: Arc<ReplayFailure>,
    ready_sender: crossbeam_channel::Sender<Vec<ReadyEntry>>,
    live_start_slot: Slot,
    shutdown: Arc<AtomicBool>,
    active_firehose_stop: Arc<Mutex<Option<Arc<AtomicBool>>>>,
    backpressure_stop_requested: Arc<AtomicBool>,
    firehose_backpressure_slot_gap_limit: u64,
    firehose_gate: Arc<Mutex<()>>,
}

impl BlockMetadataNotifier for BankBlockMetadataNotifier {
    fn notify_block_metadata(
        &self,
        _parent_slot: u64,
        _parent_blockhash: &str,
        slot: u64,
        _blockhash: &str,
        _rewards: &solana_runtime::bank::KeyedRewardsAndNumPartitions,
        _block_time: Option<solana_clock::UnixTimestamp>,
        _block_height: Option<u64>,
        executed_transaction_count: u64,
        entry_count: u64,
    ) {
        if slot == u64::MAX {
            return;
        }
        if !enforce_firehose_backpressure(
            slot,
            &self.scheduler,
            &self.shutdown,
            &self.active_firehose_stop,
            &self.backpressure_stop_requested,
            self.firehose_backpressure_slot_gap_limit,
        ) {
            return;
        }
        let _firehose_guard = self
            .firehose_gate
            .lock()
            .expect("firehose gate lock poisoned");
        self.progress.note_block_meta_slot(slot);
        match self
            .scheduler
            .record_block_metadata(slot, executed_transaction_count, entry_count)
        {
            Ok(ready_entries) => {
                send_ready_entries(&self.ready_sender, &self.failure, ready_entries);
            }
            Err(err) => self.failure.record(err),
        }

        if slot >= self.live_start_slot {
            plugin::notify_block(slot);
        }
    }
}

fn send_ready_entries(
    ready_sender: &crossbeam_channel::Sender<Vec<ReadyEntry>>,
    failure: &ReplayFailure,
    ready_entries: Vec<ReadyEntry>,
) {
    if !ready_entries.is_empty()
        && let Err(err) = ready_sender.send(ready_entries)
    {
        failure.record(format!("ready entry channel closed: {err}"));
    }
}

fn enforce_firehose_backpressure(
    incoming_slot: Slot,
    scheduler: &TransactionScheduler,
    shutdown: &Arc<AtomicBool>,
    active_firehose_stop: &Arc<Mutex<Option<Arc<AtomicBool>>>>,
    backpressure_stop_requested: &Arc<AtomicBool>,
    slot_gap_limit: u64,
) -> bool {
    if shutdown.load(Ordering::Relaxed) {
        return false;
    }
    if slot_gap_limit == 0 {
        return true;
    }
    if backpressure_stop_requested.load(Ordering::Relaxed) {
        return false;
    }

    let snapshot = scheduler.snapshot();
    let scheduler_gap = snapshot
        .highest_seen_slot
        .saturating_sub(snapshot.current_slot);
    let incoming_gap = incoming_slot.saturating_sub(snapshot.current_slot);
    let effective_gap = scheduler_gap.max(incoming_gap);
    if effective_gap < slot_gap_limit {
        return true;
    }

    if !backpressure_stop_requested.swap(true, Ordering::SeqCst) {
        warn!(
            "firehose backpressure triggered: incoming_slot={} scheduler_current_slot={} highest_seen_slot={} gap={} limit={} buffered_slots={} (requesting firehose stop)",
            incoming_slot,
            snapshot.current_slot,
            snapshot.highest_seen_slot,
            effective_gap,
            slot_gap_limit,
            snapshot.buffered_slots
        );
        if let Ok(guard) = active_firehose_stop.lock()
            && let Some(stop_signal) = guard.as_ref()
        {
            stop_signal.store(true, Ordering::SeqCst);
        }
    }

    false
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

fn read_rss_bytes() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    for line in status.lines() {
        if let Some(value) = line.strip_prefix("VmRSS:") {
            let kb = value.split_whitespace().next()?.parse::<u64>().ok()?;
            return kb.checked_mul(1024);
        }
    }
    None
}

fn epoch_to_slot(epoch: u64) -> u64 {
    epoch_to_slot_range(epoch).0
}

fn usage(program: &str) -> String {
    format!("Usage: {program} <epoch> [dest-dir] [--verify|--no-verify] [--packing|--no-packing]")
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

fn env_truthy_default(var: &str, default: bool) -> bool {
    match env::var(var) {
        Ok(value) => {
            let value = value.trim().to_ascii_lowercase();
            !matches!(value.as_str(), "" | "0" | "false" | "no")
        }
        Err(_) => default,
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

fn accounts_maintenance_root_stride() -> u64 {
    match env::var("JETSTREAMER_ACCOUNTS_MAINTENANCE_ROOT_STRIDE") {
        Ok(value) => {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                return DEFAULT_ACCOUNTS_MAINTENANCE_ROOT_STRIDE;
            }
            match trimmed.parse::<u64>() {
                Ok(0) => 1,
                Ok(stride) => stride,
                Err(err) => {
                    warn!(
                        "invalid JETSTREAMER_ACCOUNTS_MAINTENANCE_ROOT_STRIDE '{value}': {err}; using default {}",
                        DEFAULT_ACCOUNTS_MAINTENANCE_ROOT_STRIDE
                    );
                    DEFAULT_ACCOUNTS_MAINTENANCE_ROOT_STRIDE
                }
            }
        }
        Err(_) => DEFAULT_ACCOUNTS_MAINTENANCE_ROOT_STRIDE,
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

fn stage_marker(dir: &Path, name: &str) -> PathBuf {
    dir.join(format!(".stage_{name}"))
}

fn write_stage_marker(path: &Path) -> Result<(), String> {
    fs::write(path, "ok\n").map_err(|err| format!("failed to write {}: {err}", path.display()))
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
    let accounts_db_config = accounts_db_config_for_ledger(ledger_dir)?;
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
    let full_snapshot = FullSnapshotArchiveInfo::new_from_path(snapshot_archive.to_path_buf())
        .map_err(|err| format!("failed to parse snapshot archive: {err}"))?;
    let genesis_config = open_genesis_config(ledger_dir, MAX_GENESIS_ARCHIVE_UNPACKED_SIZE)
        .map_err(|err| format!("failed to load genesis config: {err}"))?;
    let runtime_config = RuntimeConfig::default();
    let accounts_db_config = accounts_db_config_for_ledger(ledger_dir)?;
    let exit = Arc::new(AtomicBool::new(false));
    let limit_load_slot_count_from_snapshot = if skip_snapshot_verify() {
        info!("snapshot verification disabled via JETSTREAMER_SKIP_SNAPSHOT_VERIFY");
        Some(usize::MAX)
    } else {
        None
    };

    let archive_tag = snapshot_archive
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("snapshot");
    let unpack_dir = ledger_dir.join(format!(".snapshot-extract-{archive_tag}"));
    fs::create_dir_all(&unpack_dir)
        .map_err(|err| format!("failed to create {}: {err}", unpack_dir.display()))?;
    let unpack_marker = stage_marker(&unpack_dir, "unpacked");
    let meta_marker = stage_marker(&unpack_dir, "meta_fixed");
    let hardlinks_marker = stage_marker(&unpack_dir, "hardlinks");
    let run_paths_marker = stage_marker(&unpack_dir, "account_paths");
    let account_run_dir = ledger_dir.join(ARCHIVE_ACCOUNTS_DIR);
    let bank_snapshots_dir = unpack_dir.join(BANK_SNAPSHOTS_DIR);
    let mut unpack_done = unpack_marker.is_file();
    if unpack_done {
        if !bank_snapshots_dir.is_dir() {
            warn!(
                "snapshot unpack marker found but {} is missing; re-unpacking",
                bank_snapshots_dir.display()
            );
            let _ = fs::remove_file(&unpack_marker);
            unpack_done = false;
        } else if !account_run_dir.is_dir() {
            warn!(
                "snapshot unpack marker found but {} is missing; re-unpacking",
                account_run_dir.display()
            );
            let _ = fs::remove_file(&unpack_marker);
            unpack_done = false;
        } else {
            let mut has_appendvec = false;
            let read_dir = fs::read_dir(&account_run_dir)
                .map_err(|err| format!("failed to read {}: {err}", account_run_dir.display()))?;
            for entry in read_dir {
                let entry = entry.map_err(|err| format!("failed to read dir entry: {err}"))?;
                if !entry
                    .file_type()
                    .map_err(|err| format!("failed to read file type: {err}"))?
                    .is_file()
                {
                    continue;
                }
                let file_name = entry.file_name();
                let Some(name) = file_name.to_str() else {
                    continue;
                };
                if looks_like_appendvec(name) {
                    has_appendvec = true;
                    break;
                }
            }
            if !has_appendvec {
                warn!(
                    "snapshot unpack marker found but no appendvec files under {}; re-unpacking",
                    account_run_dir.display()
                );
                let _ = fs::remove_file(&unpack_marker);
                unpack_done = false;
            }
        }
    }

    if !unpack_done {
        reset_dir(&unpack_dir)?;
        reset_dir(&account_run_dir)?;
    }
    if !unpack_done {
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
                if let Some(interval) = log_interval
                    && last_log.elapsed() >= interval
                {
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
        write_stage_marker(&unpack_marker)?;
    } else {
        info!(
            "snapshot archive already unpacked at {}; skipping",
            unpack_dir.display()
        );
    }

    let mut meta_done = meta_marker.is_file();
    if meta_done && !bank_snapshots_dir.is_dir() {
        warn!(
            "snapshot metadata marker found but {} is missing; re-running metadata fix",
            bank_snapshots_dir.display()
        );
        let _ = fs::remove_file(&meta_marker);
        meta_done = false;
    }
    if !meta_done {
        if ensure_snapshot_meta_files(&unpack_dir)? {
            info!(
                "repaired snapshot metadata in {}",
                unpack_dir.join(BANK_SNAPSHOTS_DIR).display()
            );
        }
        write_stage_marker(&meta_marker)?;
    }

    let bank_snapshot =
        snapshot_utils::get_highest_bank_snapshot(&bank_snapshots_dir).ok_or_else(|| {
            format!(
                "no bank snapshots found in {}",
                bank_snapshots_dir.display()
            )
        })?;

    let hardlinks_dir = bank_snapshot.snapshot_dir.join(ACCOUNTS_HARDLINKS_DIR);
    let mut hardlinks_done = hardlinks_marker.is_file();
    if hardlinks_done && !hardlinks_dir.is_dir() {
        warn!(
            "hardlinks marker found but {} is missing; re-linking accounts",
            hardlinks_dir.display()
        );
        let _ = fs::remove_file(&hardlinks_marker);
        hardlinks_done = false;
    }
    if !hardlinks_done {
        ensure_accounts_hardlinks_for_archive(&bank_snapshot, &account_run_dir)?;
        write_stage_marker(&hardlinks_marker)?;
    }

    let account_run_paths = account_run_paths_from_snapshot(&bank_snapshot.snapshot_dir)?;
    let mut run_paths_done = run_paths_marker.is_file();
    if run_paths_done {
        for path in &account_run_paths {
            if !path.is_dir() {
                warn!(
                    "account paths marker found but {} is missing; recreating",
                    path.display()
                );
                let _ = fs::remove_file(&run_paths_marker);
                run_paths_done = false;
                break;
            }
        }
    }
    if !run_paths_done {
        for path in &account_run_paths {
            fs::create_dir_all(path)
                .map_err(|err| format!("failed to create {}: {err}", path.display()))?;
        }
        write_stage_marker(&run_paths_marker)?;
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
        if env_truthy_default("JETSTREAMER_ENFORCE_ARCHIVE_HASH", true) {
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

fn accounts_db_config_for_ledger(ledger_dir: &Path) -> Result<AccountsDbConfig, String> {
    let on_disk_index = env_truthy_default(
        "JETSTREAMER_ACCOUNTS_INDEX_ON_DISK",
        DEFAULT_ACCOUNTS_INDEX_ON_DISK,
    );
    if !on_disk_index {
        info!("accounts index configured in-memory");
        return Ok(AccountsDbConfig::default());
    }

    let index_path = ledger_dir.join("accounts-index");
    fs::create_dir_all(&index_path)
        .map_err(|err| format!("failed to create {}: {err}", index_path.display()))?;
    info!(
        "accounts index configured for disk at {}",
        index_path.display()
    );

    let accounts_index_config = AccountsIndexConfig {
        drives: Some(vec![index_path]),
        index_limit_mb: IndexLimitMb::Minimal,
        ..AccountsIndexConfig::default()
    };

    Ok(AccountsDbConfig {
        index: Some(accounts_index_config),
        ..AccountsDbConfig::default()
    })
}

fn firehose_threads() -> u64 {
    env::var("JETSTREAMER_THREADS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(16)
}

fn firehose_buffer_window_bytes() -> Option<u64> {
    env::var("JETSTREAMER_BUFFER_WINDOW")
        .ok()
        .and_then(|raw| jetstreamer_firehose::system::parse_buffer_window_bytes(&raw))
}

fn ready_entry_queue_capacity() -> usize {
    env::var("JETSTREAMER_READY_ENTRY_QUEUE_CAPACITY")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_READY_ENTRY_QUEUE_CAPACITY)
}

fn post_firehose_incomplete_retry_attempts() -> usize {
    env::var("JETSTREAMER_POST_FIREHOSE_INCOMPLETE_RETRY_ATTEMPTS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(DEFAULT_POST_FIREHOSE_INCOMPLETE_RETRY_ATTEMPTS)
}

fn firehose_backpressure_slot_gap_limit() -> u64 {
    match env::var("JETSTREAMER_FIREHOSE_BACKPRESSURE_SLOT_GAP_LIMIT") {
        Ok(value) => {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                return DEFAULT_FIREHOSE_BACKPRESSURE_SLOT_GAP_LIMIT;
            }
            match trimmed.parse::<u64>() {
                Ok(limit) => limit,
                Err(err) => {
                    warn!(
                        "invalid JETSTREAMER_FIREHOSE_BACKPRESSURE_SLOT_GAP_LIMIT '{value}': {err}; using default {}",
                        DEFAULT_FIREHOSE_BACKPRESSURE_SLOT_GAP_LIMIT
                    );
                    DEFAULT_FIREHOSE_BACKPRESSURE_SLOT_GAP_LIMIT
                }
            }
        }
        Err(_) => DEFAULT_FIREHOSE_BACKPRESSURE_SLOT_GAP_LIMIT,
    }
}

fn force_missing_block_retry_limit() -> usize {
    match env::var("JETSTREAMER_FORCE_MISSING_BLOCK_RETRY_LIMIT") {
        Ok(value) => {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                return DEFAULT_FORCE_MISSING_BLOCK_RETRY_LIMIT;
            }
            match trimmed.parse::<usize>() {
                Ok(limit) if limit > 0 => limit,
                Ok(_) => DEFAULT_FORCE_MISSING_BLOCK_RETRY_LIMIT,
                Err(err) => {
                    warn!(
                        "invalid JETSTREAMER_FORCE_MISSING_BLOCK_RETRY_LIMIT '{value}': {err}; using default {}",
                        DEFAULT_FORCE_MISSING_BLOCK_RETRY_LIMIT
                    );
                    DEFAULT_FORCE_MISSING_BLOCK_RETRY_LIMIT
                }
            }
        }
        Err(_) => DEFAULT_FORCE_MISSING_BLOCK_RETRY_LIMIT,
    }
}

fn empty_slot_buffer_gap_limit() -> u64 {
    match env::var("JETSTREAMER_EMPTY_SLOT_BUFFER_GAP_LIMIT") {
        Ok(value) => {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                return DEFAULT_EMPTY_SLOT_BUFFER_GAP_LIMIT;
            }
            match trimmed.parse::<u64>() {
                Ok(limit) => limit,
                Err(err) => {
                    warn!(
                        "invalid JETSTREAMER_EMPTY_SLOT_BUFFER_GAP_LIMIT '{value}': {err}; using default {}",
                        DEFAULT_EMPTY_SLOT_BUFFER_GAP_LIMIT
                    );
                    DEFAULT_EMPTY_SLOT_BUFFER_GAP_LIMIT
                }
            }
        }
        Err(_) => DEFAULT_EMPTY_SLOT_BUFFER_GAP_LIMIT,
    }
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
    if let Ok(metadata) = fs::metadata(dest)
        && metadata.len() > 0
    {
        info!("compact index already cached at {}", dest.display());
        return Ok(());
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
    restart_tracker: Arc<RestartTracker>,
    packing_enabled: bool,
    snapshot_verifier: Option<Arc<SnapshotVerifier>>,
) -> Result<(), String> {
    let (confirmed_bank_sender, confirmed_bank_receiver) = unbounded();
    let confirmed_bank_handle =
        std::thread::spawn(move || while confirmed_bank_receiver.recv().is_ok() {});
    let (epoch_start, end_inclusive) = epoch_to_slot_range(epoch);
    let progress = Arc::new(ReplayProgress::new(epoch_start));
    let failure = Arc::new(ReplayFailure::new(shutdown.clone()));
    plugin::set_packing_enabled(packing_enabled);
    plugin::reset();
    info!("direct in-process plugin notifier enabled");
    info!(
        "account update packing {}",
        if packing_enabled {
            "enabled"
        } else {
            "disabled (--no-packing)"
        }
    );
    let accounts_update_notifier: Option<AccountsUpdateNotifier> =
        Some(Arc::new(ProgressAccountsUpdateNotifier {
            progress: progress.clone(),
            live_start_slot: epoch_start,
        }) as AccountsUpdateNotifier);
    info!("accounts update notifier wired into snapshot load: true");

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
    let gap_limit = empty_slot_buffer_gap_limit();
    if gap_limit > 0 {
        info!("empty-slot gap guard enabled (limit={gap_limit})");
    } else {
        info!("empty-slot gap guard disabled");
    }
    let scheduler = Arc::new(TransactionScheduler::new(
        replay_start,
        slot_presence,
        restart_tracker.clone(),
        gap_limit,
    ));
    let firehose_backpressure_slot_gap_limit = firehose_backpressure_slot_gap_limit();
    if firehose_backpressure_slot_gap_limit > 0 {
        info!(
            "firehose backpressure enabled (slot_gap_limit={})",
            firehose_backpressure_slot_gap_limit
        );
    } else {
        info!("firehose backpressure disabled");
    }
    let firehose_gate = Arc::new(Mutex::new(()));
    let enable_program_cache_prune = env_truthy_default(
        "JETSTREAMER_PROGRAM_CACHE_PRUNE",
        DEFAULT_PROGRAM_CACHE_PRUNE_ENABLED,
    );
    if enable_program_cache_prune {
        info!("program cache pruning enabled");
        info!("firehose paused during program cache pruning");
    } else {
        info!("program cache pruning disabled");
    }
    let enable_accounts_maintenance = env_truthy_default(
        "JETSTREAMER_ACCOUNTS_MAINTENANCE",
        DEFAULT_ACCOUNTS_MAINTENANCE_ENABLED,
    );
    let accounts_maintenance_root_stride = accounts_maintenance_root_stride();
    if enable_accounts_maintenance {
        if root_interval.is_some() {
            info!(
                "accounts maintenance enabled (flush+clean+shrink every {} root(s))",
                accounts_maintenance_root_stride
            );
        } else {
            info!("accounts maintenance enabled but rooting is disabled");
        }
    } else {
        info!("accounts maintenance disabled");
    }
    let bank_replay = Arc::new(BankReplay::new(
        bank,
        snapshot_verifier.clone(),
        root_interval,
        failure.clone(),
        cursor.clone(),
        scheduler.clone(),
        firehose_gate.clone(),
        epoch_start,
        enable_program_cache_prune,
        enable_accounts_maintenance,
        accounts_maintenance_root_stride,
    ));
    let ready_queue_capacity = ready_entry_queue_capacity();
    info!("ready entry queue capacity: {}", ready_queue_capacity);
    let (ready_sender, ready_receiver) = bounded::<Vec<ReadyEntry>>(ready_queue_capacity);
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
    let active_firehose_stop = Arc::new(Mutex::new(None::<Arc<AtomicBool>>));
    let backpressure_stop_requested = Arc::new(AtomicBool::new(false));
    let transaction_notifier = Arc::new(BankTransactionNotifier {
        progress: progress.clone(),
        scheduler: scheduler.clone(),
        failure: failure.clone(),
        ready_sender: ready_sender.clone(),
        shutdown: shutdown.clone(),
        active_firehose_stop: active_firehose_stop.clone(),
        backpressure_stop_requested: backpressure_stop_requested.clone(),
        firehose_backpressure_slot_gap_limit,
        firehose_gate: firehose_gate.clone(),
    });
    let entry_notifier = Arc::new(BankEntryNotifier {
        progress: progress.clone(),
        scheduler: scheduler.clone(),
        failure: failure.clone(),
        ready_sender: ready_sender.clone(),
        shutdown: shutdown.clone(),
        active_firehose_stop: active_firehose_stop.clone(),
        backpressure_stop_requested: backpressure_stop_requested.clone(),
        firehose_backpressure_slot_gap_limit,
        firehose_gate: firehose_gate.clone(),
    });
    let block_metadata_notifier = Arc::new(BankBlockMetadataNotifier {
        scheduler: scheduler.clone(),
        progress: progress.clone(),
        failure: failure.clone(),
        ready_sender: ready_sender.clone(),
        live_start_slot: epoch_start,
        shutdown: shutdown.clone(),
        active_firehose_stop: active_firehose_stop.clone(),
        backpressure_stop_requested: backpressure_stop_requested.clone(),
        firehose_backpressure_slot_gap_limit,
        firehose_gate: firehose_gate.clone(),
    });
    let notifiers = GeyserNotifiers {
        transaction_notifier: Some(transaction_notifier.clone()),
        entry_notifier: Some(entry_notifier.clone()),
        block_metadata_notifier: Some(block_metadata_notifier.clone()),
    };
    let threads = firehose_threads();
    let buffer_window_bytes = firehose_buffer_window_bytes();
    info!(
        "firehose: sequential=true, ripget_threads={}, buffer_window={}",
        threads,
        buffer_window_bytes
            .map(|b| jetstreamer_firehose::system::format_byte_size(b))
            .unwrap_or_else(|| "default".to_string())
    );
    let post_firehose_retries = post_firehose_incomplete_retry_attempts();
    let force_missing_retry_limit = force_missing_block_retry_limit();
    info!(
        "post-firehose incomplete retry attempts: {}",
        post_firehose_retries
    );
    info!(
        "force-missing retry limit for persistent missing block data: {}",
        force_missing_retry_limit
    );

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
                .unwrap_or(Duration::from_secs(1800));
            let inflight_warn_after = ENTRY_EXEC_WARN_AFTER;
            let inflight_fail_after = ENTRY_EXEC_FAIL_AFTER;
            let mut phase_start = None::<Instant>;
            let mut in_warmup = has_warmup;
            let mut last_seen_slot = progress.latest_slot.load(Ordering::Relaxed);
            let mut last_seen_change = Instant::now();
            let mut last_stall_log = Instant::now();
            let mut last_seen_tx_count = progress.tx_count.load(Ordering::Relaxed);
            let mut last_account_updates = progress.account_update_count.load(Ordering::Relaxed);
            let mut last_account_update_slot_seen =
                progress.last_account_update_slot.load(Ordering::Relaxed);
            let mut last_account_change = Instant::now();
            let mut last_account_log = Instant::now();
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
                let last_account_update_slot =
                    progress.last_account_update_slot.load(Ordering::Relaxed);
                let phase = if in_warmup && latest < epoch_start {
                    "warmup"
                } else {
                    "main"
                };
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
                let prev_tx_count = last_seen_tx_count;
                let tx_advanced = tx_count > last_seen_tx_count;
                if latest != last_seen_slot {
                    last_seen_slot = latest;
                    last_seen_change = Instant::now();
                } else {
                    let stalled_for = last_seen_change.elapsed();
                    if stalled_for >= stall_interval && last_stall_log.elapsed() >= stall_interval {
                        let snapshot = scheduler.snapshot();
                        let (
                            cursor_slot,
                            cursor_entry,
                            cursor_tx_start,
                            cursor_tx_count,
                            cursor_sig,
                        ) = cursor.snapshot();
                        let mut expected_after_slot: Option<Slot> = None;
                        if let Some(buffer) = snapshot.buffer.as_ref()
                            && buffer.expected_tx_count.is_none()
                            && buffer.expected_entry_count.is_none()
                            && buffer.processed_entry_count > 0
                            && buffer.pending_entries == 0
                            && buffer.buffered_txs == 0
                        {
                            expected_after_slot =
                                scheduler.expected_block_metadata_after(snapshot.current_slot);
                        }
                        let last_tx_slot = progress.last_tx_slot.load(Ordering::Relaxed);
                        let last_entry_slot = progress.last_entry_slot.load(Ordering::Relaxed);
                        let last_block_meta_slot =
                            progress.last_block_meta_slot.load(Ordering::Relaxed);
                        let last_account_update_slot =
                            progress.last_account_update_slot.load(Ordering::Relaxed);
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
                                        display_slot.saturating_sub(epoch_start).saturating_add(1)
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
                if account_updates < last_account_updates {
                    last_account_updates = account_updates;
                    last_account_update_slot_seen = last_account_update_slot;
                    last_account_change = Instant::now();
                    last_account_log = Instant::now();
                } else if account_updates != last_account_updates {
                    last_account_updates = account_updates;
                    last_account_update_slot_seen = last_account_update_slot;
                    last_account_change = Instant::now();
                } else if tx_advanced {
                    let stalled_slots = latest.saturating_sub(last_account_update_slot_seen);
                    let stalled_for = last_account_change.elapsed();
                    if stalled_slots > 5 && latest > last_account_update_slot_seen {
                        let inflight_overdue = inflight_elapsed
                            .map(|elapsed| elapsed >= inflight_fail_after)
                            .unwrap_or(false);
                        if inflight_elapsed.is_some() && !inflight_overdue {
                            warn!(
                                "account updates stalled ({phase}): count {account_updates} unchanged for {stalled_slots} slots but entry still in-flight (stage={inflight_stage} elapsed={})",
                                inflight_elapsed
                                    .map(|elapsed| format!("{:.3}s", elapsed.as_secs_f64()))
                                    .unwrap_or_else(|| "<none>".to_string())
                            );
                        } else if stalled_for >= inflight_fail_after {
                            let snapshot = scheduler.snapshot();
                            let abort_on_stall =
                                snapshot.buffered_slots == 0 && snapshot.buffer.is_none();
                            let (
                                cursor_slot,
                                cursor_entry,
                                cursor_tx_start,
                                cursor_tx_count,
                                cursor_sig,
                            ) = cursor.snapshot();
                            let message = format!(
                                "account updates stalled ({phase}): count {account_updates} unchanged for {stalled_slots} slots (latest_slot={latest} last_account_update_slot={last_account_update_slot_seen}) scheduler current_slot={} last_finalized={} buffered_slots={} highest_seen_slot={} presence={:?} buffer={:?} last_tx_slot={} last_entry_slot={} last_block_meta_slot={} last_account_update_slot={} cursor_slot={} cursor_entry={} cursor_tx_start={} cursor_tx_count={} cursor_sig={} inflight_slot={} inflight_entry={} inflight_tx_start={} inflight_tx_count={} inflight_stage={} inflight_elapsed={} inflight_sig={}",
                                snapshot.current_slot,
                                snapshot.last_finalized_slot,
                                snapshot.buffered_slots,
                                snapshot.highest_seen_slot,
                                snapshot.presence,
                                snapshot.buffer,
                                progress.last_tx_slot.load(Ordering::Relaxed),
                                progress.last_entry_slot.load(Ordering::Relaxed),
                                progress.last_block_meta_slot.load(Ordering::Relaxed),
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
                                    .map(|elapsed| format!("{:.3}s", elapsed.as_secs_f64()))
                                    .unwrap_or_else(|| "<none>".to_string()),
                                inflight_sig.as_deref().unwrap_or("<none>"),
                            );
                            if abort_on_stall {
                                failure.record(message.clone());
                                eprintln!("{message}");
                                std::process::exit(1);
                            } else {
                                warn!(
                                    "{message} (skipping abort; buffered_slots={} buffer_present={})",
                                    snapshot.buffered_slots,
                                    snapshot.buffer.is_some()
                                );
                                last_account_change = Instant::now();
                                last_account_log = Instant::now();
                            }
                        }
                    }
                    if stalled_for >= stall_interval && last_account_log.elapsed() >= stall_interval
                    {
                        let snapshot = scheduler.snapshot();
                        let (
                            cursor_slot,
                            cursor_entry,
                            cursor_tx_start,
                            cursor_tx_count,
                            cursor_sig,
                        ) = cursor.snapshot();
                        let last_tx_slot = progress.last_tx_slot.load(Ordering::Relaxed);
                        let last_entry_slot = progress.last_entry_slot.load(Ordering::Relaxed);
                        let last_block_meta_slot =
                            progress.last_block_meta_slot.load(Ordering::Relaxed);
                        let last_account_update_slot =
                            progress.last_account_update_slot.load(Ordering::Relaxed);
                        info!(
                            "account updates stalled ({phase}): count {account_updates} unchanged for {:.1}s while txs advanced ({} -> {}), latest_slot={} scheduler current_slot={} last_finalized={} buffered_slots={} highest_seen_slot={} presence={:?} buffer={:?} last_tx_slot={} last_entry_slot={} last_block_meta_slot={} last_account_update_slot={} cursor_slot={} cursor_entry={} cursor_tx_start={} cursor_tx_count={} cursor_sig={} inflight_slot={} inflight_entry={} inflight_tx_start={} inflight_tx_count={} inflight_stage={} inflight_elapsed={} inflight_sig={}",
                            stalled_for.as_secs_f64(),
                            prev_tx_count,
                            tx_count,
                            latest,
                            snapshot.current_slot,
                            snapshot.last_finalized_slot,
                            snapshot.buffered_slots,
                            snapshot.highest_seen_slot,
                            snapshot.presence,
                            snapshot.buffer,
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
                        last_account_log = Instant::now();
                    }
                }
                last_seen_tx_count = tx_count;
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
                    let accounts_per_sec = if account_updates == 0 {
                        "n/a".to_string()
                    } else if let Some(start) = phase_start {
                        let elapsed = start.elapsed().as_secs_f64();
                        if elapsed <= 0.0 {
                            "n/a".to_string()
                        } else {
                            format!("{:.2}", (account_updates as f64) / elapsed)
                        }
                    } else {
                        "n/a".to_string()
                    };
                    let slots_per_sec = if processed == 0 {
                        "n/a".to_string()
                    } else if let Some(start) = phase_start {
                        let elapsed = start.elapsed().as_secs_f64();
                        if elapsed <= 0.0 {
                            "n/a".to_string()
                        } else {
                            format!("{:.2}", (processed as f64) / elapsed)
                        }
                    } else {
                        "n/a".to_string()
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
                    let entry_count = PHASE_ENTRY_COUNT.load(Ordering::Relaxed);
                    if entry_count > 0 {
                        let gate_ms = PHASE_GATE_WAIT_US.load(Ordering::Relaxed) / 1000;
                        let bank_ms = PHASE_BANK_FOR_SLOT_US.load(Ordering::Relaxed) / 1000;
                        let prep_ms = PHASE_PREPARE_BATCH_US.load(Ordering::Relaxed) / 1000;
                        let exec_ms = PHASE_EXECUTE_US.load(Ordering::Relaxed) / 1000;
                        let post_ms = PHASE_POST_PROCESS_US.load(Ordering::Relaxed) / 1000;
                        let total_ms = gate_ms + bank_ms + prep_ms + exec_ms + post_ms;
                        let pct = |v: u64| {
                            if total_ms > 0 {
                                (v as f64 / total_ms as f64) * 100.0
                            } else {
                                0.0
                            }
                        };
                        info!(
                            "warmup slot {display_slot}/{warmup_end} ({percent:.2}%) txs={tx_count} accounts={account_updates} slots_per_sec={slots_per_sec} accounts_per_sec={accounts_per_sec} eta={eta} (epoch {epoch} starts at {epoch_start})"
                        );
                        info!(
                            "  phases ({entry_count} entries): gate={gate_ms}ms({:.0}%) bank={bank_ms}ms({:.0}%) prep={prep_ms}ms({:.0}%) exec={exec_ms}ms({:.0}%) post={post_ms}ms({:.0}%)",
                            pct(gate_ms),
                            pct(bank_ms),
                            pct(prep_ms),
                            pct(exec_ms),
                            pct(post_ms)
                        );
                    } else {
                        info!(
                            "warmup slot {display_slot}/{warmup_end} ({percent:.2}%) txs={tx_count} accounts={account_updates} slots_per_sec={slots_per_sec} accounts_per_sec={accounts_per_sec} eta={eta} (epoch {epoch} starts at {epoch_start})"
                        );
                    }
                } else {
                    if in_warmup {
                        in_warmup = false;
                        phase_start = Some(Instant::now());
                        progress.reset_counts();
                        last_seen_tx_count = 0;
                        last_account_updates = 0;
                        last_account_update_slot_seen = latest;
                        last_account_change = Instant::now();
                        last_account_log = Instant::now();
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
                    let slots_per_sec = if processed == 0 {
                        "n/a".to_string()
                    } else if let Some(start) = phase_start {
                        let elapsed = start.elapsed().as_secs_f64();
                        if elapsed <= 0.0 {
                            "n/a".to_string()
                        } else {
                            format!("{:.2}", (processed as f64) / elapsed)
                        }
                    } else {
                        "n/a".to_string()
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
                        "progress slot {display_slot}/{end_inclusive} ({percent:.2}%) txs={tx_count} accounts={account_updates} slots_per_sec={slots_per_sec} eta={eta}"
                    );
                }

                let assign_fail = PROGRAM_CACHE_ASSIGN_FAIL_COUNT.load(Ordering::Relaxed);
                if assign_fail > 0 {
                    info!("program cache assign failures so far: {assign_fail}");
                }
            }
        })
    };

    let mut firehose_start = slot_range.start;
    let mut incomplete_retries = 0usize;
    let mut firehose_error: Option<String> = None;
    let mut persistent_missing_slot: Option<Slot> = None;
    let mut persistent_missing_count = 0usize;
    loop {
        backpressure_stop_requested.store(false, Ordering::SeqCst);

        info!(
            "starting firehose replay with {} thread(s) (attempt {} from slot {})",
            threads,
            incomplete_retries.saturating_add(1),
            firehose_start
        );
        let firehose_stop = Arc::new(AtomicBool::new(false));
        if let Ok(mut guard) = active_firehose_stop.lock() {
            *guard = Some(firehose_stop.clone());
        }
        let shutdown_watcher = {
            let shutdown = shutdown.clone();
            let firehose_stop = firehose_stop.clone();
            std::thread::spawn(move || {
                while !firehose_stop.load(Ordering::Relaxed) {
                    if shutdown.load(Ordering::Relaxed) {
                        firehose_stop.store(true, Ordering::SeqCst);
                        break;
                    }
                    std::thread::sleep(Duration::from_millis(50));
                }
            })
        };
        let firehose_result = tokio::task::spawn_blocking({
            let slot_range = firehose_start..slot_range.end;
            let notifiers = GeyserNotifiers {
                transaction_notifier: notifiers.transaction_notifier.clone(),
                entry_notifier: notifiers.entry_notifier.clone(),
                block_metadata_notifier: notifiers.block_metadata_notifier.clone(),
            };
            let confirmed_bank_sender = confirmed_bank_sender.clone();
            let index_base_url = index_base_url.clone();
            let client = client.clone();
            let firehose_stop = firehose_stop.clone();
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
                    firehose_stop,
                    async { Ok(()) },
                    threads,
                    true,
                    buffer_window_bytes,
                )
            }
        })
        .await
        .map_err(|err| format!("firehose task failed: {err}"))?;
        firehose_stop.store(true, Ordering::SeqCst);
        let _ = shutdown_watcher.join();
        if let Ok(mut guard) = active_firehose_stop.lock() {
            *guard = None;
        }

        let stopped_by_backpressure = backpressure_stop_requested.load(Ordering::Relaxed);

        if let Err((err, slot)) = firehose_result {
            firehose_error = Some(format!("firehose error at slot {slot}: {err}"));
            break;
        }

        match scheduler.drain_ready_entries() {
            Ok(ready_entries) => {
                send_ready_entries(&ready_sender, &failure, ready_entries);
            }
            Err(err) => failure.record(err),
        }

        if let Err(err) = scheduler.verify_complete(end_inclusive) {
            if let Some(incomplete) = scheduler.first_incomplete_slot(end_inclusive) {
                if matches!(incomplete.reason, IncompleteReason::MissingBlockData) {
                    if persistent_missing_slot == Some(incomplete.slot) {
                        persistent_missing_count = persistent_missing_count.saturating_add(1);
                    } else {
                        persistent_missing_slot = Some(incomplete.slot);
                        persistent_missing_count = 1;
                    }
                } else {
                    persistent_missing_slot = None;
                    persistent_missing_count = 0;
                }

                if matches!(incomplete.reason, IncompleteReason::MissingBlockData)
                    && persistent_missing_count >= force_missing_retry_limit
                {
                    warn!(
                        "forcing slot {} to missing after {} consecutive missing-block-data retries",
                        incomplete.slot, persistent_missing_count
                    );
                    match scheduler.force_mark_missing(incomplete.slot) {
                        Ok(ready_entries) => {
                            send_ready_entries(&ready_sender, &failure, ready_entries);
                            persistent_missing_slot = None;
                            persistent_missing_count = 0;
                            firehose_start = scheduler.snapshot().current_slot;
                            continue;
                        }
                        Err(force_err) => {
                            failure.record(format!(
                                "failed to force slot {} missing after persistent missing block data: {}",
                                incomplete.slot, force_err
                            ));
                            break;
                        }
                    }
                }

                let consume_retry_budget = !stopped_by_backpressure;
                if consume_retry_budget && incomplete_retries >= post_firehose_retries {
                    failure.record(err);
                    break;
                }
                if stopped_by_backpressure {
                    warn!(
                        "firehose run stopped by backpressure; retrying from earliest incomplete slot"
                    );
                }
                warn!(
                    "post-firehose replay incomplete: {err}; retrying from slot {} entry {} tx_index {} reason={:?} presence={:?}",
                    incomplete.slot,
                    incomplete.entry_index,
                    incomplete.tx_start,
                    incomplete.reason,
                    incomplete.presence
                );
                if let Some(snapshot) = &incomplete.snapshot {
                    warn!(
                        "post-firehose replay incomplete: slot {} expected_txs={:?} expected_entries={:?} processed_txs={} processed_entries={} pending_entries={} buffered_txs={} next_entry_index={}",
                        incomplete.slot,
                        snapshot.expected_tx_count,
                        snapshot.expected_entry_count,
                        snapshot.processed_tx_count,
                        snapshot.processed_entry_count,
                        snapshot.pending_entries,
                        snapshot.buffered_txs,
                        snapshot.next_entry_index
                    );
                }
                restart_tracker.mark_restart(
                    incomplete.slot,
                    incomplete.entry_index,
                    incomplete.tx_start,
                    false,
                );
                firehose_start = incomplete.slot;
                if consume_retry_budget {
                    incomplete_retries = incomplete_retries.saturating_add(1);
                }
                continue;
            }
            failure.record(err);
        }
        break;
    }
    progress_done.store(true, Ordering::Relaxed);
    let _ = progress_handle.join();
    if let Some(err) = firehose_error {
        failure.record(err);
    }
    drop(transaction_notifier);
    drop(entry_notifier);
    drop(block_metadata_notifier);
    // Drop the notifier bundle too; it holds Arc clones that keep ready_sender alive.
    drop(notifiers);
    drop(ready_sender);
    let _ = ready_handle.join();
    if let Err(err) = scheduler.verify_complete(end_inclusive) {
        failure.record(err);
    }

    // Allow slot status observer to exit cleanly on shutdown.
    drop(confirmed_bank_sender);
    let _ = confirmed_bank_handle.join();

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

fn clear_ledger_accounts_state(ledger_dir: &Path) -> Result<(), String> {
    let mut cleared = Vec::new();
    for name in [
        "accounts",
        "accounts-index",
        ARCHIVE_ACCOUNTS_DIR,
        BANK_SNAPSHOTS_DIR,
    ] {
        let path = ledger_dir.join(name);
        if path.exists() {
            remove_path_if_exists(&path)?;
            cleared.push(path);
        }
    }
    let version_file = ledger_dir.join(SNAPSHOT_VERSION_FILE);
    if version_file.exists() {
        remove_path_if_exists(&version_file)?;
        cleared.push(version_file);
    }
    if ledger_dir.is_dir() {
        let read_dir = fs::read_dir(ledger_dir)
            .map_err(|err| format!("failed to read {}: {err}", ledger_dir.display()))?;
        for entry in read_dir {
            let entry =
                entry.map_err(|err| format!("failed to read {}: {err}", ledger_dir.display()))?;
            let file_name = entry.file_name();
            let name = file_name.to_string_lossy();
            if name.starts_with(".snapshot-extract-") {
                let path = entry.path();
                remove_path_if_exists(&path)?;
                cleared.push(path);
            }
        }
    }
    if cleared.is_empty() {
        info!(
            "ledger accounts state already clean in {}",
            ledger_dir.display()
        );
    } else {
        info!(
            "cleared ledger accounts state ({} path(s)) in {}",
            cleared.len(),
            ledger_dir.display()
        );
    }
    Ok(())
}

#[tokio::main]
async fn main() {
    let shutdown = Arc::new(AtomicBool::new(false));
    let cursor = Arc::new(ReplayCursor::new());
    let restart_tracker = Arc::new(RestartTracker::new());
    setup_logger(shutdown.clone(), cursor.clone(), restart_tracker.clone());
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
    let mut verify_snapshots = env_truthy_default("JETSTREAMER_VERIFY_SNAPSHOTS", true);
    let mut packing_enabled = true;
    for arg in args {
        if arg == "--verify" {
            verify_snapshots = true;
        } else if arg == "--no-verify" {
            verify_snapshots = false;
        } else if arg == "--packing" {
            packing_enabled = true;
        } else if arg == "--no-packing" {
            packing_enabled = false;
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

    if env_truthy_default("JETSTREAMER_CLEAR_ACCOUNTS_ON_START", false) {
        if let Err(err) = clear_ledger_accounts_state(&dest_dir) {
            eprintln!("error: {err}");
            exit(1);
        }
    } else {
        info!("ledger accounts cleanup disabled via JETSTREAMER_CLEAR_ACCOUNTS_ON_START");
    }

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
        restart_tracker,
        packing_enabled,
        snapshot_verifier,
    )
    .await
    {
        eprintln!("error: {err}");
        exit(1);
    }
}
