use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    env,
    ffi::OsString,
    fs,
    path::{Path, PathBuf},
    process::{Stdio, exit},
    str::FromStr,
    sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

// Same allocator choice as agave-validator, for the same reason: replay churns
// through short-lived allocations on many threads, and glibc malloc retains
// freed memory in per-thread arenas indefinitely — anon RSS grew ~520 GiB per
// replayed epoch and OOM'd the box. jemalloc's decay returns freed pages to
// the OS.
#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

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
    firehose::{
        FirehoseError, GeyserNotifiers, SourcedTransaction, SourcedTransactionNotifier,
        SourcedTransactionStatus, firehose_geyser_with_notifiers,
    },
};
use jetstreamer_horizon::archive::{
    AccountsHashKind, ArchiveProvenance, ArchiveProvenanceV1, ArchiveProvenanceV2,
    ArchiveProvenanceV3, ArchiveWriterConfig, BootstrapStateKind, RuntimeAdmission,
    RuntimeHandoffProvenance, RuntimeSegmentProvenance, RuntimeSegmentSource,
    RuntimeStateCheckpoint, SemanticDigest, StateCommitment, StateCommitmentKind,
    TransactionMetadataPolicy, WriteVersionNormalization, merge_runtime_segments,
};
use jetstreamer_node::handoff_snapshot::{
    HANDOFF_SNAPSHOT_MANIFEST_SCHEMA_VERSION, HistoricalHandoffSnapshotManifest,
    handoff_snapshot_manifest_path, read_and_validate_handoff_snapshot_manifest,
    write_handoff_snapshot_manifest,
};
use jetstreamer_node::segment_manifest::{
    HistoricalSegmentManifest, SegmentCheckpointSummary, SegmentRuntimeAdmission,
    SegmentRuntimeIdentity, read_and_validate_segment_manifest, write_segment_manifest,
};
use jetstreamer_node::snapshots::{
    DEFAULT_BUCKET, download_snapshot_at_or_before_slot_matching,
    list_snapshots_in_slot_range_matching,
};
use log::{error, info, warn};
use rayon::prelude::*;
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
use solana_runtime::installed_scheduler_pool::InstalledSchedulerPoolArc;
use solana_runtime::prioritization_fee_cache::PrioritizationFeeCache;
use solana_runtime::{
    bank::Bank, bank_forks::BankForks, installed_scheduler_pool::BankWithScheduler,
    runtime_config::RuntimeConfig, snapshot_bank_utils, snapshot_utils,
    transaction_batch::TransactionBatch,
};
use solana_runtime_transaction::runtime_transaction::RuntimeTransaction;
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
use solana_unified_scheduler_pool::DefaultSchedulerPool;
use tar::Archive as TarArchive;
use tokio::process::Command;
use xxhash_rust::xxh64::xxh64;

mod compatibility;
mod historical;
mod historical_replay;
mod horizon;
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
/// Wall-clock budget for a single *in-flight* entry before the replay aborts
/// (a genuinely hung transaction). Pathological-but-finite mainnet txs have
/// been observed to take ~6-7 minutes and then complete with verified results
/// (e.g. epoch 944 slot 407838634 at 422s), so the default is generous — a
/// false abort costs a multi-day range run, a true hang costs one hour.
/// Entries that complete are never aborted regardless of how long they took.
/// Override with `JETSTREAMER_ENTRY_EXEC_TIMEOUT_SECS`.
static ENTRY_EXEC_FAIL_AFTER: std::sync::LazyLock<Duration> = std::sync::LazyLock::new(|| {
    let secs = std::env::var("JETSTREAMER_ENTRY_EXEC_TIMEOUT_SECS")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .filter(|&s| s > 0)
        .unwrap_or(3600);
    Duration::from_secs(secs)
});
const BANK_FOR_SLOT_WARN_AFTER: Duration = Duration::from_secs(5);
const PROGRAM_CACHE_PRUNE_PROGRESS_INTERVAL: Duration = Duration::from_secs(30);
const ACCOUNTS_MAINTENANCE_PROGRESS_INTERVAL: Duration = Duration::from_secs(30);
const DEFAULT_PROGRAM_CACHE_PRUNE_ENABLED: bool = true;
const DEFAULT_ACCOUNTS_MAINTENANCE_ENABLED: bool = true;
// Roots are set every `DEFAULT_ROOT_INTERVAL` slots; running the accounts
// maintenance pass (flush + clean + shrink, ~30-40s of replay-visible drag)
// on every 10th root spaces it to roughly 10k slots.
const DEFAULT_ACCOUNTS_MAINTENANCE_ROOT_STRIDE: u64 = 10;
const DEFAULT_ACCOUNTS_INDEX_ON_DISK: bool = false;
const DEFAULT_READY_ENTRY_QUEUE_CAPACITY: usize = 8192;
const MISMATCH_RETRY_ATTEMPTS: usize = 10;
/// Max prepared entry batches accumulated into one parallel execution wave.
const MAX_WAVE_BATCHES: usize = 64;
/// Max transactions accumulated into one parallel execution wave.
const MAX_WAVE_TXS: usize = 256;
const DEFAULT_POST_FIREHOSE_INCOMPLETE_RETRY_ATTEMPTS: usize = 16;
const DEFAULT_EMPTY_SLOT_BUFFER_GAP_LIMIT: u64 = 0;
const DEFAULT_FIREHOSE_BACKPRESSURE_SLOT_GAP_LIMIT: u64 = 128;
const DEFAULT_FORCE_MISSING_BLOCK_RETRY_LIMIT: usize = 16;
static LOGGED_FIRST_ACCOUNT_UPDATE: AtomicBool = AtomicBool::new(false);
static LOGGED_PROGRAM_CACHE_ASSIGN_FAIL: AtomicBool = AtomicBool::new(false);
static PROGRAM_CACHE_ASSIGN_FAIL_COUNT: AtomicU64 = AtomicU64::new(0);
static PROGRAM_CACHE_PRUNE_DEPLOYMENT_SLOT: AtomicU64 = AtomicU64::new(0);

// Phase timing counters (cumulative microseconds)
static PHASE_GATE_WAIT_US: AtomicU64 = AtomicU64::new(0);
static PHASE_BANK_FOR_SLOT_US: AtomicU64 = AtomicU64::new(0);
static PHASE_PREPARE_BATCH_US: AtomicU64 = AtomicU64::new(0);
/// Wall-clock of the parallel per-slot sanitize phase (subset of prep).
static PHASE_SANITIZE_US: AtomicU64 = AtomicU64::new(0);
/// Serial per-entry account-locking time (subset of prep).
static PHASE_LOCK_US: AtomicU64 = AtomicU64::new(0);
static PHASE_EXECUTE_US: AtomicU64 = AtomicU64::new(0);
static PHASE_POST_PROCESS_US: AtomicU64 = AtomicU64::new(0);
static PHASE_ENTRY_COUNT: AtomicU64 = AtomicU64::new(0);
static PHASE_WAVE_COUNT: AtomicU64 = AtomicU64::new(0);
static PHASE_WAVE_BATCHES: AtomicU64 = AtomicU64::new(0);
/// Time the ready-entry thread spends blocked waiting for the firehose to
/// deliver entries. High vs. the accounted busy time ⇒ input-bound.
static PHASE_RECV_WAIT_US: AtomicU64 = AtomicU64::new(0);
/// Sum of ready-channel backlog sampled at each blocking recv, and the
/// number of samples — their ratio is the average pending depth.
static PHASE_RECV_BACKLOG: AtomicU64 = AtomicU64::new(0);
static PHASE_RECV_COUNT: AtomicU64 = AtomicU64::new(0);

/// One-line summary of the replay phase counters (None until any entry
/// has been processed). Shared by the warmup and main progress logs.
fn phases_summary() -> Option<String> {
    let entry_count = PHASE_ENTRY_COUNT.load(Ordering::Relaxed);
    if entry_count == 0 {
        return None;
    }
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
    let waves = PHASE_WAVE_COUNT.load(Ordering::Relaxed);
    let avg_wave = if waves > 0 {
        PHASE_WAVE_BATCHES.load(Ordering::Relaxed) as f64 / waves as f64
    } else {
        0.0
    };
    let sanitize_ms = PHASE_SANITIZE_US.load(Ordering::Relaxed) / 1000;
    let lock_ms = PHASE_LOCK_US.load(Ordering::Relaxed) / 1000;
    // Input-vs-compute diagnostic: recv_wait is time the coordinator was
    // blocked waiting for firehose input (idle), so `busy%` =
    // accounted / (accounted + recv_wait) is its duty cycle. Near 100% ⇒
    // compute-bound; well below ⇒ starved by firehose I/O. `backlog` is the
    // average ready-channel depth at each wait (deep ⇒ compute-bound).
    let recv_wait_ms = PHASE_RECV_WAIT_US.load(Ordering::Relaxed) / 1000;
    let recv_count = PHASE_RECV_COUNT.load(Ordering::Relaxed).max(1);
    let avg_backlog = PHASE_RECV_BACKLOG.load(Ordering::Relaxed) as f64 / recv_count as f64;
    let busy_pct = if total_ms + recv_wait_ms > 0 {
        total_ms as f64 * 100.0 / (total_ms + recv_wait_ms) as f64
    } else {
        0.0
    };
    // Recorder-mutex held/wait, summed across the parallel exec workers.
    // `held` is serial (one holder at a time) so held ≈ exec ⇒ the recorder
    // mutex is serializing execution; `wait` ≫ 0 ⇒ workers are blocking on it.
    let (rec_held_us, rec_wait_us) = horizon::recorder_contention_us();
    let rec_held_ms = rec_held_us / 1000;
    let rec_wait_ms = rec_wait_us / 1000;
    Some(format!(
        "  phases ({entry_count} entries, {waves} waves, avg {avg_wave:.1} batches/wave, \
         busy={busy_pct:.0}% recv_wait={recv_wait_ms}ms avg_backlog={avg_backlog:.0} \
         recorder_held={rec_held_ms}ms recorder_wait={rec_wait_ms}ms): \
         gate={gate_ms}ms({:.0}%) bank={bank_ms}ms({:.0}%) \
         prep={prep_ms}ms({:.0}%)[sanitize={sanitize_ms}ms({:.0}%) lock={lock_ms}ms({:.0}%)] \
         exec={exec_ms}ms({:.0}%) post={post_ms}ms({:.0}%)",
        pct(gate_ms),
        pct(bank_ms),
        pct(prep_ms),
        pct(sanitize_ms),
        pct(lock_ms),
        pct(exec_ms),
        pct(post_ms)
    ))
}

/// This process's anonymous resident memory from `/proc/self/status`
/// (Linux-only; `None` elsewhere). Anon RSS is the number that OOM-kills us:
/// appendvecs are file-backed and reclaimable, the heap is not.
fn process_anon_rss_bytes() -> Option<u64> {
    let status = fs::read_to_string("/proc/self/status").ok()?;
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix("RssAnon:") {
            let kb: u64 = rest.trim().trim_end_matches("kB").trim().parse().ok()?;
            return Some(kb * 1024);
        }
    }
    None
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BankHashExpectation {
    AccountsLtHash(SnapshotHash),
    LegacyAccountsHash(Hash),
}

struct SnapshotVerifier {
    expected: DashMap<Slot, BankHashExpectation>,
    errors: DashMap<usize, String>,
    error_count: AtomicUsize,
    shutdown: Option<Arc<AtomicBool>>,
}

impl SnapshotVerifier {
    fn new(
        expected: BTreeMap<Slot, BankHashExpectation>,
        shutdown: Option<Arc<AtomicBool>>,
    ) -> Self {
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

        let (expected_hash, actual_hash, kind) = match expected_hash {
            BankHashExpectation::AccountsLtHash(expected) => {
                (expected.0, bank.get_snapshot_hash().0, "accounts-lt")
            }
            BankHashExpectation::LegacyAccountsHash(expected) => {
                self.record_error(format!(
                    "legacy accounts-hash checkpoint {expected} at slot {slot} was routed to the \
                     Agave verifier; runtime selection must dispatch this span to a historical backend"
                ));
                return;
            }
        };
        if actual_hash != expected_hash {
            let message = format!(
                "{kind} hash mismatch at slot {slot}: expected {expected_hash}, got {actual_hash}"
            );
            warn!("{message}");
            self.record_error(message);
        } else {
            info!("verified {kind} hash at slot {slot}");
        }
    }

    fn legacy_checkpoint_slots(&self) -> Vec<Slot> {
        let mut slots: Vec<_> = self
            .expected
            .iter()
            .filter_map(|entry| {
                matches!(entry.value(), BankHashExpectation::LegacyAccountsHash(_))
                    .then_some(*entry.key())
            })
            .collect();
        slots.sort_unstable();
        slots
    }

    fn checkpoint_count_in_range(&self, start: Slot, end_inclusive: Slot) -> usize {
        self.expected
            .iter()
            .filter(|entry| (start..=end_inclusive).contains(entry.key()))
            .count()
    }

    fn verify_legacy_accounts_hash(&self, slot: Slot, actual_hash: Hash) {
        let expected_hash = self.expected.remove(&slot).map(|(_, hash)| hash);
        let Some(expected_hash) = expected_hash else {
            return;
        };
        let BankHashExpectation::LegacyAccountsHash(expected_hash) = expected_hash else {
            self.record_error(format!(
                "accounts-lt checkpoint at slot {slot} was routed to a historical verifier"
            ));
            return;
        };
        if actual_hash != expected_hash {
            self.record_error(format!(
                "legacy accounts hash mismatch at slot {slot}: expected {expected_hash}, got {actual_hash}"
            ));
        } else {
            info!("verified legacy accounts hash at slot {slot}");
        }
    }

    fn finish(&self) -> Result<(), String> {
        if let Some(message) = self.error_summary() {
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

    fn error_summary(&self) -> Option<String> {
        let total_errors = self.error_count.load(Ordering::Relaxed);
        if total_errors == 0 {
            return None;
        }
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
        Some(message)
    }

    fn record_error(&self, message: String) {
        warn!("snapshot verification failure: {message}");
        let idx = self.error_count.fetch_add(1, Ordering::Relaxed);
        self.errors.insert(idx, message);
        if let Some(shutdown) = &self.shutdown {
            shutdown.store(true, Ordering::SeqCst);
        }
    }
}

/// Which executor drives slot replay (`JETSTREAMER_SCHEDULER`).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum SchedulerMode {
    /// Agave's unified scheduler: per-transaction dependency scheduling with
    /// no round barriers ([`BankReplay::replay_slot_unified`]).
    Unified,
    /// Whole-slot parallel round scheduler ([`BankReplay::replay_slot_rounds`]).
    Rounds,
    /// Legacy wave walk ([`BankReplay::replay_slot_waves`]).
    Waves,
}

fn scheduler_mode_from_env() -> SchedulerMode {
    match env::var("JETSTREAMER_SCHEDULER")
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "unified" => SchedulerMode::Unified,
        "rounds" => SchedulerMode::Rounds,
        "waves" => SchedulerMode::Waves,
        "" => {
            // Back-compat with the previous toggle: =0 selected the wave walk.
            if env_truthy_default("JETSTREAMER_PARALLEL_SCHEDULER", true) {
                SchedulerMode::Rounds
            } else {
                SchedulerMode::Waves
            }
        }
        other => {
            warn!("unknown JETSTREAMER_SCHEDULER '{other}'; using rounds");
            SchedulerMode::Rounds
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
    /// Pool for executing mutually non-conflicting entry batches in
    /// parallel (see [`BankReplay::process_slot_entries`]).
    replay_pool: rayon::ThreadPool,
    /// Which slot executor to use (see [`SchedulerMode`]).
    scheduler_mode: SchedulerMode,
    /// Scheduler pool for [`SchedulerMode::Unified`]; schedulers are checked
    /// out per slot in [`BankReplay::replay_slot_unified`] and returned by its
    /// completion wait.
    unified_pool: Option<InstalledSchedulerPoolArc>,
}

/// The account footprint of one entry (the union across its transactions):
/// `writes` are accounts the entry locks writably, `reads` are read-only
/// locks. The two sets are disjoint — an account written by any of the
/// entry's transactions is classified `writes`, never `reads`.
#[derive(Debug, Default, Clone)]
struct EntryAccounts {
    writes: std::collections::HashSet<Address>,
    reads: std::collections::HashSet<Address>,
}

impl EntryAccounts {
    /// Two entries conflict if they share an account that at least one of
    /// them writes (write-write or write-read). Read-read is not a conflict.
    fn conflicts_with(&self, other: &EntryAccounts) -> bool {
        self.writes
            .iter()
            .any(|a| other.writes.contains(a) || other.reads.contains(a))
            || self.reads.iter().any(|a| other.writes.contains(a))
    }
}

/// Assigns each entry to an execution round (level) so that:
/// 1. entries within a round are mutually conflict-free (safe to execute in
///    parallel), and
/// 2. for any conflicting pair `i < j`, `i` lands in a strictly earlier
///    round than `j` — preserving per-account write order, which the bank
///    state and the horizon archive both depend on.
///
/// Returns rounds in execution order; entry indices within a round stay
/// ascending. This is the agave-equivalent "execute non-conflicting
/// transactions in parallel, conflicting ones in sequence" rule, but
/// computed over a whole slot at once rather than greedily flushing on the
/// first conflict (which is what limited the old wave scheduler to ~3-4
/// batches when ~30 threads were available).
///
/// Greedy multi-pass: each pass scans the still-unscheduled entries in
/// order, placing an entry in the current round unless it conflicts with a
/// round member *or* with an entry already deferred this pass (the
/// `blocked` set, which preserves order — a later entry conflicting with a
/// deferred earlier one must also defer).
fn assign_rounds(entries: &[EntryAccounts]) -> Vec<Vec<usize>> {
    let mut rounds: Vec<Vec<usize>> = Vec::new();
    let mut remaining: Vec<usize> = (0..entries.len()).collect();
    while !remaining.is_empty() {
        let mut round: Vec<usize> = Vec::new();
        let mut deferred: Vec<usize> = Vec::new();
        let mut round_acc = EntryAccounts::default();
        let mut blocked = EntryAccounts::default();
        for &i in &remaining {
            let e = &entries[i];
            if e.conflicts_with(&blocked) || e.conflicts_with(&round_acc) {
                // Must run after something not yet scheduled this round.
                blocked.writes.extend(e.writes.iter().copied());
                blocked.reads.extend(e.reads.iter().copied());
                deferred.push(i);
            } else {
                round_acc.writes.extend(e.writes.iter().copied());
                round_acc.reads.extend(e.reads.iter().copied());
                round.push(i);
            }
        }
        rounds.push(round);
        remaining = deferred;
    }
    rounds
}

/// Computes one entry's account footprint from its sanitized transactions,
/// using the same writability the bank uses to take locks (reserved-account
/// demotion already applied during sanitization). Writes take precedence: an
/// account written by any of the entry's transactions is never classified as
/// a read, so [`EntryAccounts::conflicts_with`] sees write-write/write-read
/// conflicts exactly as the bank's lock table would.
fn entry_accounts(txs: &[RuntimeTransaction<SanitizedTransaction>]) -> EntryAccounts {
    let mut ea = EntryAccounts::default();
    for tx in txs {
        let message = tx.message();
        let keys = message.account_keys();
        for (i, key) in keys.iter().enumerate() {
            let addr = Address::new_from_array(key.to_bytes());
            if message.is_writable(i) {
                ea.writes.insert(addr);
            } else {
                ea.reads.insert(addr);
            }
        }
    }
    // Keep the sets disjoint — an account written by one transaction and read
    // by another in the same entry is a write for conflict purposes.
    ea.reads.retain(|a| !ea.writes.contains(a));
    ea
}

/// A prepared entry batch awaiting wave execution. Holds its account
/// locks (via the contained [`TransactionBatch`]) from preparation until
/// the wave flushes, which is what makes lock failures on later entries a
/// reliable conflict signal.
struct PendingEntryBatch<'a, 'b> {
    entry: ReadyEntry,
    batch: TransactionBatch<'a, 'b, RuntimeTransaction<SanitizedTransaction>>,
}

/// Executes one prepared entry batch and flattens commit results into
/// per-transaction statuses, returning them alongside the account updates
/// captured during commit. Safe to call concurrently for batches whose
/// account locks do not overlap (the bank commits batches independently).
///
/// Capture is thread-local: the geyser notifier appends each
/// transaction-owned account write to this thread's buffer (lock-free),
/// which we drain here — so a whole batch's updates come back ordered with
/// no recorder-mutex contention between the parallel execution workers.
fn execute_entry_batch(
    bank: &Bank,
    batch: &TransactionBatch<'_, '_, RuntimeTransaction<SanitizedTransaction>>,
    recording: bool,
) -> (
    Vec<Result<(), TransactionError>>,
    Vec<horizon::CapturedUpdate>,
) {
    if recording {
        horizon::begin_capture();
    }
    let mut timings = ExecuteTimings::default();
    let (commit_results, _balance_collector) = bank.load_execute_and_commit_transactions(
        batch,
        MAX_PROCESSING_AGE,
        ExecutionRecordingConfig::new_single_setting(false),
        &mut timings,
        None,
    );
    let results = commit_results
        .into_iter()
        .map(|commit_result| commit_result.and_then(|committed| committed.status))
        .collect();
    let captured = if recording {
        horizon::take_captured()
    } else {
        Vec::new()
    };
    (results, captured)
}

/// Number of threads for the wave-execution pool. Defaults to all cores
/// minus a little headroom for the firehose/recorder threads.
fn replay_thread_count() -> usize {
    env::var("JETSTREAMER_REPLAY_THREADS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(8)
                .saturating_sub(2)
                .clamp(1, 32)
        })
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
    #[allow(clippy::too_many_arguments)] // replay wiring carries its full context set
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
        let bank_forks = BankForks::new_rw_arc(bank);
        Self::from_bank_forks(
            bank_forks,
            snapshot_verifier,
            root_interval,
            failure,
            cursor,
            scheduler,
            firehose_gate,
            live_start_slot,
            enable_program_cache_prune,
            enable_accounts_maintenance,
            accounts_maintenance_root_stride,
        )
    }

    /// Builds a replayer over an existing `bank_forks`, reusing the in-memory
    /// bank instead of loading a snapshot. This is how a multi-epoch run
    /// chains epochs: epoch N finishes with its working bank at the N/N+1
    /// boundary, and that same `bank_forks` is handed straight to epoch N+1 —
    /// no snapshot reload, no warmup replay.
    #[allow(clippy::too_many_arguments)]
    fn from_bank_forks(
        bank_forks: Arc<RwLock<BankForks>>,
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
        let (mut leader_schedule_cache, cached_bank) = {
            let guard = bank_forks
                .read()
                .expect("bank forks lock poisoned during init");
            let bank = guard.working_bank();
            let leader_schedule_cache = LeaderScheduleCache::new_from_bank(&bank);
            let cached_bank = Mutex::new(Some(CachedBank {
                slot: bank.slot(),
                bank,
            }));
            (leader_schedule_cache, cached_bank)
        };
        leader_schedule_cache.set_max_schedules(usize::MAX);
        let debug_signature = env::var("JETSTREAMER_DEBUG_SIG")
            .ok()
            .and_then(|value| Signature::from_str(value.trim()).ok());
        let replay_threads = replay_thread_count();
        let scheduler_mode = scheduler_mode_from_env();
        info!(
            "replay execution threads: {replay_threads}; scheduler: {scheduler_mode:?} \
             (JETSTREAMER_SCHEDULER=unified|rounds|waves)"
        );
        // Handler threads execute and commit; completions land in the status
        // cache (read back for expected-status verification), and account
        // updates reach the horizon recorder through the signature-attributed
        // fallback path in `note_account_update`. Schedulers are checked out
        // of the pool per slot rather than attached at bank insert, keeping
        // bank-forks bookkeeping identical across all three modes.
        let unified_pool = (scheduler_mode == SchedulerMode::Unified).then(|| {
            info!("unified scheduler pool created (handlers={replay_threads})");
            DefaultSchedulerPool::new_dyn(
                Some(replay_threads),
                None,
                None,
                None,
                Arc::new(PrioritizationFeeCache::default()),
            )
        });
        let replay_pool = rayon::ThreadPoolBuilder::new()
            .num_threads(replay_threads)
            .thread_name(|i| format!("replayExec{i:02}"))
            .stack_size(16 * 1024 * 1024)
            .build()
            .expect("failed to build replay thread pool");
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
            replay_pool,
            scheduler_mode,
            unified_pool,
        }
    }

    /// The shared bank forks, so a finished epoch can hand its live bank to the
    /// next epoch in a range run.
    fn bank_forks(&self) -> Arc<RwLock<BankForks>> {
        self.bank_forks.clone()
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
        // This runs after the entry has *completed*: its results are committed
        // and verified against expected statuses like any other entry, so no
        // matter how far past the budget it ran, aborting here would only
        // discard finished work (a 422s tx once killed a 40h range run this
        // way). The timeout abort lives in the progress thread's in-flight
        // detector, which is the only place a true hang is observable.
        if elapsed >= *ENTRY_EXEC_FAIL_AFTER {
            warn!(
                "entry execution exceeded timeout budget but completed: slot {} entry {} txs={} elapsed={:.3}s sig={}",
                entry.slot,
                entry.entry_index,
                entry.tx_count,
                elapsed.as_secs_f64(),
                signature.unwrap_or("<none>")
            );
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

    #[allow(clippy::too_many_arguments)]
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

    #[allow(clippy::too_many_arguments)]
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

    #[allow(clippy::too_many_arguments)]
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
        // Entries arrive strictly slot-ordered; process each slot's
        // contiguous run as one group so its bank is fetched once and
        // entry batches can be wave-scheduled against shared locks.
        let mut entries = entries.into_iter().peekable();
        while let Some(first) = entries.peek() {
            // Bail between slot groups on shutdown — a drained batch can
            // hold minutes of execution work, and an aborting run has no
            // use for it (in-memory replay state is not resumable).
            if self.failure.shutdown_requested() {
                return;
            }
            let slot = first.slot;
            let mut group = Vec::new();
            while entries.peek().is_some_and(|entry| entry.slot == slot) {
                group.push(entries.next().expect("peeked entry"));
            }
            self.process_slot_entries(slot, group);
        }
    }

    /// Replays one slot's contiguous run of ready entries.
    ///
    /// Mirrors agave's `blockstore_processor` replay strategy: consecutive
    /// entry batches are *prepared* in order — each batch holding its
    /// account locks — and accumulated into a wave. The bank's lock table
    /// is the conflict detector: a lock failure means the new entry
    /// conflicts with a pending batch, so the wave is flushed (executed)
    /// first. Batches within a wave are therefore mutually non-conflicting
    /// and safe to execute in parallel, while conflicting transactions
    /// retain their original cross-wave order — preserving per-account
    /// write order, which both consensus state and the horizon archive
    /// depend on. Ticks act as barriers, exactly as in agave.
    ///
    /// All result verification, transaction-range notification, cursor
    /// advancement, and archive recording happen sequentially in original
    /// entry order after each wave executes.
    fn process_slot_entries(&self, slot: Slot, group: Vec<ReadyEntry>) {
        let gate_start = Instant::now();
        let _execution_guard = self
            .execution_gate
            .lock()
            .expect("execution gate lock poisoned");
        let gate_wait = gate_start.elapsed();
        PHASE_GATE_WAIT_US.fetch_add(gate_wait.as_micros() as u64, Ordering::Relaxed);
        if gate_wait >= BANK_FOR_SLOT_WARN_AFTER {
            warn!(
                "entry execution gate waited {:.3}s (slot {})",
                gate_wait.as_secs_f64(),
                slot
            );
        }

        let phase_bank_start = Instant::now();
        let bank = match self.bank_for_slot(slot) {
            Ok(bank) => bank,
            Err(err) => {
                log::debug!(
                    "skipping {} ready entries at slot {}: {}",
                    group.len(),
                    slot,
                    err
                );
                return;
            }
        };
        PHASE_BANK_FOR_SLOT_US.fetch_add(
            phase_bank_start.elapsed().as_micros() as u64,
            Ordering::Relaxed,
        );
        self.maybe_prune_program_cache_by_deployment_slot(&bank);

        // Phase 1: sanitize every transaction in the slot in parallel — the
        // CPU-heavy message-hash + address-table-resolution step that
        // dominated serial replay. This is safe to do up front: ALT
        // resolution within slot S depends only on table state from
        // slots <= S-1 (same-slot extensions aren't usable until the next
        // slot), so the result is identical to sanitizing each entry just
        // before it executes. Ticks (empty entries) get an empty Vec.
        let phase_prepare_start = Instant::now();
        let sanitized: Vec<Vec<RuntimeTransaction<SanitizedTransaction>>> = self
            .replay_pool
            .install(|| {
                group
                    .par_iter()
                    .map(|entry| {
                        entry
                            .txs
                            .iter()
                            .map(|scheduled| bank.sanitize_entry_transaction(scheduled.tx.clone()))
                            .collect::<Result<Vec<_>, _>>()
                    })
                    .collect::<Vec<_>>()
            })
            .into_iter()
            .zip(group.iter())
            .map(|(result, entry)| {
                result.unwrap_or_else(|err| {
                    let message = format!(
                        "transaction sanitize failed at slot {} entry {}: {}",
                        entry.slot, entry.entry_index, err
                    );
                    self.failure.record(message.clone());
                    panic!("{message}");
                })
            })
            .collect();
        let sanitize_us = phase_prepare_start.elapsed().as_micros() as u64;
        PHASE_SANITIZE_US.fetch_add(sanitize_us, Ordering::Relaxed);
        PHASE_PREPARE_BATCH_US.fetch_add(sanitize_us, Ordering::Relaxed);

        // Phase 2: execute the slot's transactions.
        match self.scheduler_mode {
            SchedulerMode::Unified => self.replay_slot_unified(&bank, group, sanitized),
            SchedulerMode::Rounds => self.replay_slot_rounds(&bank, group, sanitized),
            SchedulerMode::Waves => self.replay_slot_waves(&bank, group, sanitized),
        }
    }

    /// Legacy wave scheduler: walks entries in order, locking pre-sanitized
    /// batches into waves. The bank's lock table is the conflict detector; a
    /// lock failure flushes the pending wave so the conflicting entry sees
    /// its predecessors committed. Batches borrow `sanitized`, which outlives
    /// every wave, so a re-lock after a flush is free (no re-sanitize). Ticks
    /// act as barriers.
    fn replay_slot_waves(
        &self,
        bank: &Arc<Bank>,
        group: Vec<ReadyEntry>,
        sanitized: Vec<Vec<RuntimeTransaction<SanitizedTransaction>>>,
    ) {
        let mut pending: Vec<PendingEntryBatch<'_, '_>> = Vec::new();
        let mut pending_tx_count = 0usize;
        for (i, entry) in group.into_iter().enumerate() {
            if entry.tx_count == 0 {
                // Tick barrier: everything that PoH-precedes the tick must
                // be committed before it registers.
                self.flush_pending(bank, &mut pending, &mut pending_tx_count);
                self.register_tick_entry(entry);
                continue;
            }

            if let Some(debug_sig) = self.debug_signature.as_ref() {
                for (offset, scheduled) in entry.txs.iter().enumerate() {
                    if scheduled.tx.signatures.first() == Some(debug_sig) {
                        let tx_index = entry.start_index.saturating_add(offset);
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

            let sanitized_txs = sanitized[i].as_slice();
            let lock_start = Instant::now();
            let mut batch = bank.prepare_sanitized_batch(sanitized_txs);
            let mut lock_us = lock_start.elapsed().as_micros() as u64;
            if batch.lock_results().iter().any(|result| result.is_err()) {
                // Conflict with a lock held by a pending batch: flush the
                // wave (committing everything that PoH-precedes this entry),
                // then re-lock. If lock errors persist with no pending locks
                // held, the entry conflicts with itself — fall through and
                // let result verification surface it.
                drop(batch);
                self.flush_pending(bank, &mut pending, &mut pending_tx_count);
                let relock_start = Instant::now();
                batch = bank.prepare_sanitized_batch(sanitized_txs);
                lock_us += relock_start.elapsed().as_micros() as u64;
            }
            PHASE_LOCK_US.fetch_add(lock_us, Ordering::Relaxed);
            PHASE_PREPARE_BATCH_US.fetch_add(lock_us, Ordering::Relaxed);

            pending_tx_count += entry.tx_count;
            pending.push(PendingEntryBatch { entry, batch });
            if pending.len() >= MAX_WAVE_BATCHES || pending_tx_count >= MAX_WAVE_TXS {
                self.flush_pending(bank, &mut pending, &mut pending_tx_count);
            }
        }
        self.flush_pending(bank, &mut pending, &mut pending_tx_count);
    }

    /// Registers one tick entry (an entry with no transactions): advances
    /// PoH on the bank, records the tick on the archive, and advances the
    /// replay cursor. Shared by both schedulers.
    ///
    /// In the parallel scheduler this is called after every transaction of
    /// the slot has committed, which is the only ordering rule ticks impose:
    /// mid-slot ticks have no transaction-visible effect, and the
    /// block-boundary tick (which registers the slot's recent blockhash and
    /// completes the bank) must follow all commits.
    fn register_tick_entry(&self, entry: ReadyEntry) {
        self.cursor
            .start_inflight(entry.slot, entry.entry_index, entry.start_index, 0, None);
        let _inflight_guard = InFlightGuard {
            cursor: self.cursor.clone(),
        };
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
            if entry.slot >= self.live_start_slot
                && let Some(recorder) = horizon::recorder()
            {
                recorder.record_committed_entry(
                    entry.slot,
                    entry.entry_index,
                    entry.num_hashes,
                    Vec::new(),
                );
            }
            // Only advance the replay cursor after a successful tick registration.
            self.cursor.update(
                entry.slot,
                entry.entry_index,
                entry.start_index,
                entry.tx_count,
                None,
            );
        }
        self.note_entry_duration(&entry, start.elapsed(), None);
    }

    /// Parallel scheduler: assigns the slot's transaction entries to
    /// conflict-respecting rounds ([`assign_rounds`]) and executes each round
    /// across the replay pool, exploiting the full thread count instead of
    /// the ~3-4 batches the wave scheduler managed. Ticks are deferred to
    /// after all transactions commit and then registered in entry order (see
    /// [`Self::register_tick_entry`]).
    ///
    /// Correctness rests on two guarantees: (1) a round is conflict-free by
    /// construction, so executing its batches in parallel is equivalent to
    /// any sequential order — verified at lock time, where any divergence
    /// from the bank's own lock semantics panics rather than corrupts; and
    /// (2) cross-round order matches stream order for every conflicting pair,
    /// preserving per-account write order. All verification, notification,
    /// cursor, and archive side effects run afterward in strict entry order.
    fn replay_slot_rounds(
        &self,
        bank: &Arc<Bank>,
        group: Vec<ReadyEntry>,
        sanitized: Vec<Vec<RuntimeTransaction<SanitizedTransaction>>>,
    ) {
        let tx_positions: Vec<usize> = (0..group.len())
            .filter(|&i| group[i].tx_count > 0)
            .collect();
        let footprints: Vec<EntryAccounts> = tx_positions
            .iter()
            .map(|&i| entry_accounts(&sanitized[i]))
            .collect();
        let rounds = assign_rounds(&footprints);

        let recording = horizon::recorder().is_some();
        let mut results: Vec<Option<Vec<Result<(), TransactionError>>>> =
            (0..group.len()).map(|_| None).collect();
        let mut captured: Vec<Vec<horizon::CapturedUpdate>> =
            (0..group.len()).map(|_| Vec::new()).collect();

        // Track the slot as in-flight for the stall monitor across the whole
        // execute + post-process span (mirrors the wave path's guard).
        if let Some(first) = group.first() {
            let signature = first
                .txs
                .first()
                .and_then(|scheduled| scheduled.tx.signatures.first())
                .map(|sig| sig.to_string());
            self.cursor.start_inflight(
                first.slot,
                first.entry_index,
                first.start_index,
                group.iter().map(|e| e.tx_count).sum(),
                signature,
            );
        }
        let _inflight_guard = InFlightGuard {
            cursor: self.cursor.clone(),
        };

        self.cursor.update_inflight_stage("execute_rounds");
        let exec_start = Instant::now();
        for round in &rounds {
            PHASE_WAVE_COUNT.fetch_add(1, Ordering::Relaxed);
            PHASE_WAVE_BATCHES.fetch_add(round.len() as u64, Ordering::Relaxed);
            // Lock each entry's batch. A round is conflict-free by
            // construction, so cross-entry locks should all succeed; any
            // lock error is therefore either inherent to the transaction
            // (e.g. AccountLoadedTwice — matches the chain) or, if conflict
            // detection were buggy, an AccountInUse that can never match a
            // real committed result and is caught by the expected-status
            // verification in `post_process_entry`. Either way we let the
            // batch execute and the result speak — no special-casing here.
            type RoundBatch<'a> = (
                usize,
                TransactionBatch<'a, 'a, RuntimeTransaction<SanitizedTransaction>>,
            );
            let batches: Vec<RoundBatch<'_>> = round
                .iter()
                .map(|&k| {
                    let pos = tx_positions[k];
                    (pos, bank.prepare_sanitized_batch(sanitized[pos].as_slice()))
                })
                .collect();

            type BatchOutput = (
                Vec<Result<(), TransactionError>>,
                Vec<horizon::CapturedUpdate>,
            );
            let outputs: Vec<BatchOutput> = if batches.len() == 1 {
                vec![execute_entry_batch(bank, &batches[0].1, recording)]
            } else {
                self.replay_pool.install(|| {
                    batches
                        .par_iter()
                        .map(|(_, batch)| execute_entry_batch(bank, batch, recording))
                        .collect()
                })
            };

            for ((pos, batch), output) in batches.into_iter().zip(outputs) {
                drop(batch); // release this entry's account locks
                results[pos] = Some(output.0);
                captured[pos] = output.1;
            }
        }
        let slot_exec = exec_start.elapsed();
        PHASE_EXECUTE_US.fetch_add(slot_exec.as_micros() as u64, Ordering::Relaxed);

        // Side effects in strict entry order: ticks register (safe now that
        // every transaction has committed), transactions verify and record.
        for (pos, entry) in group.into_iter().enumerate() {
            if entry.tx_count == 0 {
                self.register_tick_entry(entry);
                continue;
            }
            let entry_results = results[pos]
                .take()
                .expect("transaction entry must have executed");
            if let Some(recorder) = horizon::recorder() {
                recorder.record_captured_updates(std::mem::take(&mut captured[pos]));
            }
            self.post_process_entry(bank, entry, entry_results, slot_exec);
        }
    }

    /// Unified-scheduler execution: submits every transaction entry to the
    /// bank's installed Agave scheduler (per-transaction dependency
    /// scheduling — no round barriers, so one long conflict chain no longer
    /// stalls unrelated transactions), waits for the slot to complete, then
    /// performs side effects in strict entry order. Task ids are the
    /// slot-global transaction indexes, which preserves per-account
    /// commit order across entries. Account updates reach the horizon
    /// recorder during commit via `note_account_update`'s
    /// signature-attributed fallback path; per-transaction statuses are read
    /// back from the bank's status cache for expected-status verification.
    fn replay_slot_unified(
        &self,
        bank: &Arc<Bank>,
        group: Vec<ReadyEntry>,
        mut sanitized: Vec<Vec<RuntimeTransaction<SanitizedTransaction>>>,
    ) {
        let Some(first) = group.first() else {
            return;
        };
        let slot = first.slot;
        let Some(pool) = self.unified_pool.as_ref() else {
            let message =
                format!("unified scheduler mode active but no scheduler pool exists (slot {slot})");
            self.failure.record(message.clone());
            panic!("{message}");
        };
        let bank_ws = BankWithScheduler::new_for_verification_replay(bank.clone(), pool);

        let signature = first
            .txs
            .first()
            .and_then(|scheduled| scheduled.tx.signatures.first())
            .map(|sig| sig.to_string());
        self.cursor.start_inflight(
            slot,
            first.entry_index,
            first.start_index,
            group.iter().map(|e| e.tx_count).sum(),
            signature,
        );
        let _inflight_guard = InFlightGuard {
            cursor: self.cursor.clone(),
        };

        self.cursor.update_inflight_stage("unified_submit");
        let exec_start = Instant::now();
        PHASE_WAVE_COUNT.fetch_add(1, Ordering::Relaxed);
        let mut submit_error = false;
        for (pos, entry) in group.iter().enumerate() {
            if entry.tx_count == 0 {
                continue; // ticks register in order after completion
            }
            PHASE_WAVE_BATCHES.fetch_add(1, Ordering::Relaxed);
            let txs = std::mem::take(&mut sanitized[pos]);
            let base = entry.start_index as u128;
            if bank_ws
                .schedule_transaction_executions(
                    txs.into_iter()
                        .enumerate()
                        .map(|(offset, tx)| (tx, base + offset as u128)),
                )
                .is_err()
            {
                // Scheduler aborted; the true error surfaces from the wait.
                submit_error = true;
                break;
            }
        }

        self.cursor.update_inflight_stage("unified_wait");
        let wait_result = bank_ws.wait_for_completed_scheduler();
        let slot_exec = exec_start.elapsed();
        PHASE_EXECUTE_US.fetch_add(slot_exec.as_micros() as u64, Ordering::Relaxed);
        match wait_result {
            Some((Ok(()), _timings)) => {}
            Some((Err(err), _timings)) => {
                let message = format!("unified scheduler failed in slot {slot}: {err:?}");
                self.failure.record(message.clone());
                panic!("{message}");
            }
            None => {
                let message = format!(
                    "unified scheduler returned no result for slot {slot} \
                     (submit_error={submit_error})"
                );
                self.failure.record(message.clone());
                panic!("{message}");
            }
        }

        // Side effects in strict entry order: ticks register, transactions
        // verify against expected statuses and notify.
        for entry in group {
            if entry.tx_count == 0 {
                self.register_tick_entry(entry);
                continue;
            }
            let results: Vec<Result<(), TransactionError>> = entry
                .txs
                .iter()
                .map(|scheduled| {
                    scheduled
                        .tx
                        .signatures
                        .first()
                        .and_then(|sig| bank.get_signature_status(sig))
                        // Missing from the status cache after a clean wait
                        // means the transaction never committed; surface it
                        // through the mismatch machinery.
                        .unwrap_or(Err(TransactionError::CommitCancelled))
                })
                .collect();
            self.post_process_entry(bank, entry, results, slot_exec);
        }
    }

    /// Executes the accumulated wave of mutually non-conflicting entry
    /// batches (in parallel when there is more than one), then verifies,
    /// notifies, and records each entry sequentially in original order.
    fn flush_pending(
        &self,
        bank: &Arc<Bank>,
        pending: &mut Vec<PendingEntryBatch<'_, '_>>,
        pending_tx_count: &mut usize,
    ) {
        if pending.is_empty() {
            return;
        }
        *pending_tx_count = 0;
        let wave = std::mem::take(pending);
        PHASE_WAVE_COUNT.fetch_add(1, Ordering::Relaxed);
        PHASE_WAVE_BATCHES.fetch_add(wave.len() as u64, Ordering::Relaxed);

        let first = &wave[0].entry;
        let signature = first
            .txs
            .first()
            .and_then(|scheduled| scheduled.tx.signatures.first())
            .map(|sig| sig.to_string());
        self.cursor.start_inflight(
            first.slot,
            first.entry_index,
            first.start_index,
            wave.iter().map(|pb| pb.entry.tx_count).sum(),
            signature,
        );
        let _inflight_guard = InFlightGuard {
            cursor: self.cursor.clone(),
        };
        self.cursor.update_inflight_stage("execute_wave");

        let recording = horizon::recorder().is_some();
        let phase_exec_start = Instant::now();
        type BatchOutput = (
            Vec<Result<(), TransactionError>>,
            Vec<horizon::CapturedUpdate>,
        );
        let outputs: Vec<BatchOutput> = if wave.len() == 1 {
            vec![execute_entry_batch(bank, &wave[0].batch, recording)]
        } else {
            self.replay_pool.install(|| {
                wave.par_iter()
                    .map(|pb| execute_entry_batch(bank, &pb.batch, recording))
                    .collect()
            })
        };
        let wave_elapsed = phase_exec_start.elapsed();
        PHASE_EXECUTE_US.fetch_add(wave_elapsed.as_micros() as u64, Ordering::Relaxed);

        self.cursor.update_inflight_stage("post_process");
        for (pb, (results, captured)) in wave.into_iter().zip(outputs) {
            let PendingEntryBatch { entry, batch } = pb;
            drop(batch); // release the entry's account locks
            // Merge this batch's lock-free-captured account updates on the
            // coordinator (serial, off the parallel execution path) before
            // recording the entry.
            if let Some(recorder) = horizon::recorder() {
                recorder.record_captured_updates(captured);
            }
            self.post_process_entry(bank, entry, results, wave_elapsed);
        }
    }

    /// Verifies one executed entry against its expected statuses and
    /// performs the in-order side effects: transaction-range notification,
    /// cursor advancement, and archive recording.
    fn post_process_entry(
        &self,
        bank: &Arc<Bank>,
        mut entry: ReadyEntry,
        results: Vec<Result<(), TransactionError>>,
        elapsed: Duration,
    ) {
        let phase_post_start = Instant::now();
        if results.len() != entry.txs.len() {
            let message = format!(
                "transaction result length mismatch at slot {} entry {}: expected {} results, got {}",
                entry.slot,
                entry.entry_index,
                entry.txs.len(),
                results.len()
            );
            self.failure.record(message.clone());
            panic!("{message}");
        }

        for (offset, actual) in results.into_iter().enumerate() {
            let Some(expected) = entry.txs[offset].expected_status.clone() else {
                // Old Faithful's earliest transactions may carry an empty
                // metadata frame. Replay is the source of truth in that case;
                // do not reinterpret TransactionStatusMeta::default() as an
                // observed success.
                entry.txs[offset].status_meta.status = actual;
                continue;
            };
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
                            let backoff_ms = 50_u64.saturating_mul(1_u64 << (attempt - 2));
                            std::thread::sleep(Duration::from_millis(backoff_ms.min(300_000)));
                        }
                        if let Some(retry_result) = self.retry_mismatch_transaction(
                            bank,
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
                        bank,
                        &entry,
                        tx_index,
                        offset,
                        &expected,
                        &actual,
                        &scheduled.tx,
                    );
                    self.dump_mismatch_artifacts(
                        bank,
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
                        bank, &entry, tx_index, offset, &expected, &actual, None,
                    );
                }
                let message = format!(
                    "transaction execution mismatch at slot {} entry {} index {} sig {}: expected {:?}, got {:?}",
                    entry.slot, entry.entry_index, tx_index, signature, expected, actual
                );
                self.failure.record(message.clone());
                panic!("{message}");
            }
        }

        if entry.slot >= self.live_start_slot {
            plugin::notify_transaction_range(entry.slot, entry.start_index, entry.tx_count);
        }
        PHASE_POST_PROCESS_US.fetch_add(
            phase_post_start.elapsed().as_micros() as u64,
            Ordering::Relaxed,
        );
        PHASE_ENTRY_COUNT.fetch_add(1, Ordering::Relaxed);

        let signature = entry
            .txs
            .first()
            .and_then(|scheduled| scheduled.tx.signatures.first())
            .map(|sig| sig.to_string());
        // Advance the replay cursor only after successful execution/verification.
        self.cursor.update(
            entry.slot,
            entry.entry_index,
            entry.start_index,
            entry.tx_count,
            signature.clone(),
        );
        self.note_entry_duration(&entry, elapsed, signature.as_deref());
        if elapsed >= ENTRY_EXEC_WARN_AFTER {
            self.log_slow_entry_details(&entry);
        }
        if entry.slot >= self.live_start_slot
            && let Some(recorder) = horizon::recorder()
        {
            let txs = std::mem::take(&mut entry.txs)
                .into_iter()
                .map(|scheduled| (scheduled.tx, scheduled.status_meta))
                .collect();
            recorder.record_committed_entry(entry.slot, entry.entry_index, entry.num_hashes, txs);
        }
    }

    /// Freezes the most recently replayed bank so its end-of-slot account
    /// updates (fee distribution, …) reach geyser. Idempotent.
    fn freeze_latest_bank(&self) -> Result<(), String> {
        let bank = {
            let guard = self
                .bank_forks
                .read()
                .map_err(|_| "bank forks lock poisoned".to_string())?;
            guard.working_bank()
        };
        bank.freeze();
        Ok(())
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

/// Runtime-neutral operations used by the ordered ready-entry consumer.
/// Backend construction remains explicit so Solana SDK types never cross the
/// historical worker boundary.
trait ReplayExecutor: Send + Sync {
    fn process_ready_entries(&self, entries: Vec<ReadyEntry>);
    fn verify_latest_bank(&self) -> Result<(), String>;
    fn freeze_latest_bank(&self) -> Result<(), String>;
    fn historical_evidence(
        &self,
    ) -> Result<Option<historical_replay::HistoricalReplayEvidence>, String> {
        Ok(None)
    }
    fn export_historical_snapshot(
        &self,
        _slot: Slot,
        _output_directory: &Path,
        _expected_accounts_hash: [u8; 32],
    ) -> Result<Option<historical::HistoricalSnapshotExport>, String> {
        Ok(None)
    }
    fn shutdown(&self) -> Result<(), String> {
        Ok(())
    }
}

impl ReplayExecutor for BankReplay {
    fn process_ready_entries(&self, entries: Vec<ReadyEntry>) {
        BankReplay::process_ready_entries(self, entries);
    }

    fn verify_latest_bank(&self) -> Result<(), String> {
        BankReplay::verify_latest_bank(self)
    }

    fn freeze_latest_bank(&self) -> Result<(), String> {
        BankReplay::freeze_latest_bank(self)
    }
}

impl ReplayExecutor for historical_replay::HistoricalReplay {
    fn process_ready_entries(&self, entries: Vec<ReadyEntry>) {
        historical_replay::HistoricalReplay::process_ready_entries(self, entries);
    }

    fn verify_latest_bank(&self) -> Result<(), String> {
        historical_replay::HistoricalReplay::verify_latest_bank(self)
    }

    fn freeze_latest_bank(&self) -> Result<(), String> {
        historical_replay::HistoricalReplay::freeze_latest_bank(self)
    }

    fn historical_evidence(
        &self,
    ) -> Result<Option<historical_replay::HistoricalReplayEvidence>, String> {
        historical_replay::HistoricalReplay::evidence(self).map(Some)
    }

    fn export_historical_snapshot(
        &self,
        slot: Slot,
        output_directory: &Path,
        expected_accounts_hash: [u8; 32],
    ) -> Result<Option<historical::HistoricalSnapshotExport>, String> {
        historical_replay::HistoricalReplay::export_snapshot(
            self,
            slot,
            output_directory,
            expected_accounts_hash,
        )
        .map(Some)
    }

    fn shutdown(&self) -> Result<(), String> {
        historical_replay::HistoricalReplay::shutdown(self)
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

/// Cross-epoch progress for a multi-epoch range run, shared across every
/// `run_geyser_replay` call so the per-epoch progress thread can also report
/// overall span progress + ETA. `None` for a single-epoch run.
struct RangeProgress {
    /// First slot of the first epoch in the (resumed) range.
    overall_start_slot: Slot,
    /// Last slot of the last epoch in the range.
    overall_end_slot: Slot,
    /// First and total epoch counts (for the "epoch i/N" readout). `first_epoch`
    /// is the resume point, so `total_epochs` counts only epochs actually run.
    first_epoch: u64,
    total_epochs: u64,
    /// `(wall baseline, slot baseline)` captured at the first post-warmup tick of
    /// the whole run; overall rate = `(latest - slot_baseline) / elapsed`. Set
    /// once and persisted across epochs so the ETA reflects the true multi-epoch
    /// pace rather than just the current epoch.
    baseline: Mutex<Option<(Instant, Slot)>>,
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

    #[allow(clippy::type_complexity)] // diagnostic snapshot tuple, single internal caller
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

    fn shutdown_requested(&self) -> bool {
        self.shutdown.load(Ordering::Relaxed)
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
    has_finalized_slot: bool,
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
                has_finalized_slot: start_slot > 0,
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
        if state.has_finalized_slot && state.last_finalized_slot >= restart_slot {
            if let Some(previous_slot) = restart_slot.checked_sub(1) {
                state.last_finalized_slot = previous_slot;
            } else {
                state.last_finalized_slot = 0;
                state.has_finalized_slot = false;
            }
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
        status_meta: Option<TransactionStatusMeta>,
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
        if state.has_finalized_slot && slot <= state.last_finalized_slot {
            return Err(format!(
                "late transaction for slot {slot} (last finalized slot {})",
                state.last_finalized_slot
            ));
        }
        if slot > state.highest_seen_slot {
            state.highest_seen_slot = slot;
        }
        let buffer = state.slots.entry(slot).or_default();
        let inserted = buffer.insert_transaction(index, tx, status_meta)?;
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
        num_hashes: u64,
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
        if state.has_finalized_slot && slot <= state.last_finalized_slot {
            return Err(format!(
                "late entry for slot {slot} (last finalized slot {})",
                state.last_finalized_slot
            ));
        }
        if slot > state.highest_seen_slot {
            state.highest_seen_slot = slot;
        }
        let buffer = state.slots.entry(slot).or_default();
        let inserted = buffer.push_entry(entry_index, start_index, tx_count, hash, num_hashes)?;
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
        if state.has_finalized_slot && slot <= state.last_finalized_slot {
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
        // Tell the recorder this slot is a deliberate force-skip (its block
        // data is genuinely unavailable from old-faithful despite the index
        // marking it present) so the gap-fill records it as leader-skipped
        // rather than panicking on a seemingly-dropped present block.
        horizon::note_force_skipped(slot);
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
                    state.has_finalized_slot = true;
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
            state.has_finalized_slot = true;
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
        status_meta: Option<TransactionStatusMeta>,
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
            if existing_sig == incoming_sig
                && existing.expected_status
                    == status_meta.as_ref().map(|metadata| metadata.status.clone())
            {
                // Duplicate delivery of the same transaction; ignore.
                return Ok(false);
            }
            return Err(format!(
                "duplicate transaction at index {index} (existing_sig={:?}, incoming_sig={:?})",
                existing_sig, incoming_sig
            ));
        }
        let expected_status = status_meta.as_ref().map(|metadata| metadata.status.clone());
        self.txs[index] = Some(ScheduledTransaction {
            tx,
            expected_status,
            status_meta: status_meta.unwrap_or_default(),
        });
        Ok(true)
    }

    fn push_entry(
        &mut self,
        entry_index: usize,
        start_index: usize,
        tx_count: usize,
        hash: Hash,
        num_hashes: u64,
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
            num_hashes,
        });
        Ok(true)
    }

    fn drain_ready_entries(&mut self, slot: Slot) -> Result<Vec<ReadyEntry>, String> {
        let mut ready = Vec::new();
        while let Some(entry) = self.pending_entries.front() {
            if entry.tx_count == 0 {
                let entry = self.pending_entries.pop_front().expect("pending entry");
                self.processed_entry_count = self.processed_entry_count.saturating_add(1);
                ready.push(ReadyEntry {
                    slot,
                    entry_index: entry.entry_index,
                    start_index: entry.start_index,
                    txs: Vec::new(),
                    hash: entry.hash,
                    num_hashes: entry.num_hashes,
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
                num_hashes: entry.num_hashes,
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
    num_hashes: u64,
}

#[derive(Debug)]
struct ScheduledTransaction {
    tx: VersionedTransaction,
    /// `None` means the source carried no transaction metadata. In that case
    /// replay determines the status; a default `Ok(())` must not be mistaken
    /// for observed ground truth.
    expected_status: Option<Result<(), TransactionError>>,
    /// Full original chain metadata (from the CAR stream), carried through
    /// so the horizon recorder can archive it alongside replay output.
    status_meta: TransactionStatusMeta,
}

#[derive(Debug)]
struct ReadyEntry {
    slot: Slot,
    entry_index: usize,
    start_index: usize,
    txs: Vec<ScheduledTransaction>,
    hash: Hash,
    num_hashes: u64,
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
            plugin::notify_account_update(account);
            // Lock-free on the hot path: transaction-owned writes land in a
            // thread-local capture buffer (drained per batch by the
            // executor); only runtime-direct orphan writes take the lock.
            let txn_signature = txn.map(|tx| *tx.signature());
            horizon::note_account_update(slot, pubkey, account, txn_signature, write_version);
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

fn validate_transaction_status_presence(
    policy: compatibility::MissingTransactionStatus,
    available: bool,
    slot: Slot,
    transaction_slot_index: usize,
    signature: &Signature,
) -> Result<(), String> {
    if available || policy == compatibility::MissingTransactionStatus::Reconstruct {
        return Ok(());
    }
    Err(format!(
        "transaction status metadata is required at slot {slot} index {transaction_slot_index} signature {signature}, but the source frame is empty"
    ))
}

impl SourcedTransactionNotifier for BankTransactionNotifier {
    fn notify_transaction(&self, transaction: SourcedTransaction<'_>) {
        let SourcedTransaction {
            slot,
            transaction_slot_index,
            signature,
            status,
            transaction,
            ..
        } = transaction;
        let transaction_status_meta = match status {
            SourcedTransactionStatus::Observed(status_meta) => Some(status_meta.clone()),
            SourcedTransactionStatus::Missing => None,
        };
        if let Err(error) = validate_transaction_status_presence(
            compatibility::missing_transaction_status_at(slot),
            transaction_status_meta.is_some(),
            slot,
            transaction_slot_index,
            signature,
        ) {
            self.failure.record(error);
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
        match self.scheduler.insert_transaction(
            slot,
            transaction_slot_index,
            transaction.clone(),
            transaction_status_meta,
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
            entry.num_hashes,
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
            if let Some(recorder) = horizon::recorder() {
                recorder.record_block_meta(
                    slot,
                    _parent_slot,
                    _parent_blockhash,
                    _blockhash,
                    _rewards,
                    _block_time,
                    _block_height,
                    executed_transaction_count,
                    entry_count,
                );
                recorder.maybe_log_progress(slot);
            }
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

/// Parses the epoch argument as either a single epoch (`950`) or an inclusive
/// range (`950-955`; the Rust forms `950..=955` inclusive and `950..955`
/// exclusive are also accepted). Returns `(start, end_inclusive)`.
fn parse_epoch_range(arg: &str) -> Result<(u64, u64), String> {
    let parse = |s: &str| -> Result<u64, String> {
        s.trim()
            .parse::<u64>()
            .map_err(|err| format!("invalid epoch '{}': {err}", s.trim()))
    };
    let (start, end) = if let Some((a, b)) = arg.split_once("..=") {
        (parse(a)?, parse(b)?)
    } else if let Some((a, b)) = arg.split_once("..") {
        let (start, end) = (parse(a)?, parse(b)?);
        if end <= start {
            return Err(format!(
                "empty epoch range '{arg}': end must be greater than start"
            ));
        }
        (start, end - 1)
    } else if let Some((a, b)) = arg.split_once('-') {
        (parse(a)?, parse(b)?)
    } else {
        let only = parse(arg)?;
        (only, only)
    };
    if end < start {
        return Err(format!(
            "invalid epoch range '{arg}': end {end} is before start {start}"
        ));
    }
    const SLOTS_PER_EPOCH: u64 = 432_000;
    if end
        .checked_mul(SLOTS_PER_EPOCH)
        .and_then(|slot| slot.checked_add(SLOTS_PER_EPOCH))
        .is_none()
    {
        return Err(format!(
            "invalid epoch range '{arg}': epoch {end} exceeds the representable slot range"
        ));
    }
    Ok((start, end))
}

fn usage(program: &str) -> String {
    format!(
        "Usage: {program} <epoch|range> [dest-dir] [--verify|--no-verify] [--horizon-output=PATH]\n\
         \x20      [--qualification-end-slot=SLOT]\n\
         \n\
         <epoch|range> is a single epoch (950) or an inclusive range (950-955).\n\
         Replays each epoch and writes a horizon archive to <dest-dir>/epoch-<N>.jet.\n\
         A range pre-downloads every epoch's boundary snapshot and snapshot hashes\n\
         up front (the only gcloud/GCS access), then runs each epoch in its own\n\
         child process so replay memory is fully released at every epoch boundary.\n\
         Set JETSTREAMER_EPOCH_ISOLATION=0 to restore single-process chaining (the\n\
         bank stays in memory across epochs; no per-epoch snapshot reload).\n\
         JETSTREAMER_EPOCH_ATTEMPTS (default 2) bounds retries of a crashed epoch;\n\
         JETSTREAMER_PRUNE_EPOCH_SNAPSHOTS=0 keeps each boundary snapshot archive\n\
         after its epoch finalizes (default: deleted to reclaim disk).\n\
         --horizon-output applies only to a single epoch.\n\
         --qualification-end-slot runs a focused qualification from an explicit\n\
         snapshot in the target or preceding epoch through SLOT. Recording starts\n\
         at the later of the target epoch boundary and the snapshot successor. It\n\
         requires a single epoch, --verify,\n\
         --snapshot-archive, --epoch-hashes, and --horizon-output.\n\
         --epoch-hashes=PATH, --snapshot-archive=PATH, and --range-info=A-B are\n\
         otherwise internal flags passed by the range supervisor to its children."
    )
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

/// A deliberately narrow partial replay used to qualify one runtime span
/// against a canonical post-bootstrap checkpoint.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct QualificationPlan {
    epoch: u64,
    bootstrap_slot: Slot,
    replay_start: Slot,
    output_slot_start: Slot,
    end_inclusive: Slot,
}

impl QualificationPlan {
    fn runtime_range(self) -> std::ops::Range<Slot> {
        self.replay_start..self.end_inclusive.saturating_add(1)
    }

    fn slot_count(self) -> u64 {
        self.end_inclusive - self.output_slot_start + 1
    }
}

fn require_qualification_path<'a>(name: &str, value: Option<&'a Path>) -> Result<&'a Path, String> {
    value
        .filter(|path| !path.as_os_str().is_empty())
        .ok_or_else(|| format!("--qualification-end-slot requires --{name}=PATH"))
}

/// Validates all qualification-only CLI invariants before snapshot state,
/// workers, output files, or network clients are touched.
fn qualification_plan(
    start_epoch: u64,
    end_epoch: u64,
    end_slot: Option<Slot>,
    explicit_verify: Option<bool>,
    snapshot_archive: Option<&Path>,
    epoch_hashes: Option<&Path>,
    horizon_output: Option<&Path>,
) -> Result<Option<QualificationPlan>, String> {
    let Some(end_inclusive) = end_slot else {
        return Ok(None);
    };
    if start_epoch != end_epoch {
        return Err(format!(
            "--qualification-end-slot requires one epoch, not {start_epoch}-{end_epoch}"
        ));
    }
    if explicit_verify != Some(true) {
        return Err(
            "--qualification-end-slot requires an explicit --verify (environment defaults do not qualify)"
                .to_string(),
        );
    }
    let snapshot_archive = require_qualification_path("snapshot-archive", snapshot_archive)?;
    require_qualification_path("epoch-hashes", epoch_hashes)?;
    require_qualification_path("horizon-output", horizon_output)?;

    let (epoch_start, epoch_end) = epoch_to_slot_range(start_epoch);
    if !(epoch_start..=epoch_end).contains(&end_inclusive) {
        return Err(format!(
            "qualification end slot {end_inclusive} is outside epoch {start_epoch} ({epoch_start}..={epoch_end})"
        ));
    }
    let snapshot_name = snapshot_archive
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            format!(
                "snapshot path has no UTF-8 filename: {}",
                snapshot_archive.display()
            )
        })?;
    let (bootstrap_slot, _) = parse_snapshot_archive_name(snapshot_name)?;
    let earliest_bootstrap = if start_epoch == 0 {
        epoch_start
    } else {
        epoch_to_slot_range(start_epoch - 1).0
    };
    if !(earliest_bootstrap..=epoch_end).contains(&bootstrap_slot) {
        return Err(format!(
            "qualification snapshot slot {bootstrap_slot} is outside the target or preceding epoch ({earliest_bootstrap}..={epoch_end})"
        ));
    }
    let replay_start = bootstrap_slot
        .checked_add(1)
        .ok_or_else(|| format!("qualification bootstrap slot {bootstrap_slot} has no successor"))?;
    if replay_start > end_inclusive {
        return Err(format!(
            "qualification end slot {end_inclusive} must be after bootstrap slot {bootstrap_slot}"
        ));
    }
    let output_slot_start = replay_start.max(epoch_start);
    Ok(Some(QualificationPlan {
        epoch: start_epoch,
        bootstrap_slot,
        replay_start,
        output_slot_start,
        end_inclusive,
    }))
}

fn runtime_slot_range(
    epoch: u64,
    qualification: Option<QualificationPlan>,
) -> std::ops::Range<Slot> {
    if let Some(plan) = qualification {
        debug_assert_eq!(plan.epoch, epoch);
        plan.runtime_range()
    } else {
        let (start, end_inclusive) = epoch_to_slot_range(epoch);
        start..end_inclusive.saturating_add(1)
    }
}

fn runtime_span_selection(
    span: &compatibility::RuntimeSpan,
) -> Result<compatibility::RuntimeSelection, String> {
    let compatibility::EraBackend::Available(descriptor) = span.execution.backend else {
        return Err(format!(
            "runtime span {}..{} unexpectedly retained unsupported era {}",
            span.slots.start, span.slots.end, span.execution.name
        ));
    };
    Ok(compatibility::RuntimeSelection {
        backend: descriptor.backend,
        descriptor,
        admission: span.execution.admission,
    })
}

/// Selects the runtime that owns the first replayed slot in a range.
///
/// This is deliberately narrower than `select_runtime`: callers may use the
/// result only for bootstrap concerns which must be decided before a
/// multi-runtime range is split (principally the accepted snapshot archive
/// format). Execution still follows every span returned by
/// `plan_runtime_spans`.
fn bootstrap_runtime_selection(
    slot_range: std::ops::Range<Slot>,
    allow_candidate_runtime: bool,
) -> Result<compatibility::RuntimeSelection, String> {
    let spans = compatibility::plan_runtime_spans(slot_range, allow_candidate_runtime)?;
    runtime_span_selection(spans.first().expect("runtime planner rejects empty ranges"))
}

/// Resolves range isolation while preserving the operator opt-out only when
/// every epoch can be handled by one runtime. The optional epoch identifies a
/// runtime boundary that forced isolation despite the opt-out.
fn epoch_isolation_plan(
    start_epoch: u64,
    end_epoch: u64,
    configured_isolation: bool,
    allow_candidate_runtime: bool,
) -> Result<(bool, Option<u64>), String> {
    if start_epoch == end_epoch {
        return Ok((false, None));
    }
    if configured_isolation {
        return Ok((true, None));
    }
    for epoch in start_epoch..=end_epoch {
        let (slot_start, slot_end_inclusive) = epoch_to_slot_range(epoch);
        let spans = compatibility::plan_runtime_spans(
            slot_start..slot_end_inclusive.saturating_add(1),
            allow_candidate_runtime,
        )?;
        if spans.len() > 1 {
            return Ok((true, Some(epoch)));
        }
    }
    Ok((false, None))
}

/// Whether this is a historical Solana 1.0 snapshot archive.  This classifies
/// the persisted format only; runtime selection is independently driven by
/// the requested slot range in `compatibility`.
fn is_legacy_snapshot_archive(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name.ends_with(".tar.bz2"))
}

fn legacy_boundary_expectation(path: &Path) -> Result<BTreeMap<Slot, BankHashExpectation>, String> {
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| format!("snapshot path has no UTF-8 filename: {}", path.display()))?;
    if !name.ends_with(".tar.bz2") {
        return Err(format!(
            "legacy accounts-hash expectation requires a .tar.bz2 snapshot: {}",
            path.display()
        ));
    }
    let (slot, hash) = parse_snapshot_archive_name(name)?;
    Ok(BTreeMap::from([(
        slot,
        BankHashExpectation::LegacyAccountsHash(hash.0),
    )]))
}

fn initial_replay_slot(bootstrap_slot: Slot, epoch_start: Slot) -> Result<Slot, String> {
    if bootstrap_slot >= epoch_start {
        return Err(format!(
            "bootstrap state at slot {bootstrap_slot} is not earlier than output epoch start {epoch_start}"
        ));
    }
    bootstrap_slot.checked_add(1).ok_or_else(|| {
        format!("bootstrap slot {bootstrap_slot} has no representable successor slot")
    })
}

fn archive_generation_profile() -> String {
    format!(
        "jetstreamer-node/{}/old-faithful-to-horizon-v2@{}",
        env!("CARGO_PKG_VERSION"),
        env!("JETSTREAMER_BUILD_REVISION")
    )
}

fn archive_assembly_profile() -> String {
    format!(
        "jetstreamer-node/{}/verified-runtime-segment-assembly-v1@{}",
        env!("CARGO_PKG_VERSION"),
        env!("JETSTREAMER_BUILD_REVISION")
    )
}

fn archive_transaction_metadata_policy(slot_start: Slot) -> TransactionMetadataPolicy {
    if slot_start < compatibility::OLD_FAITHFUL_STATUS_REQUIRED_START_SLOT {
        TransactionMetadataPolicy::runtime_reconstructed_before(
            compatibility::OLD_FAITHFUL_STATUS_REQUIRED_START_SLOT,
        )
    } else {
        TransactionMetadataPolicy::observed()
    }
}

fn archive_runtime_toolchain(identity: compatibility::RuntimeIdentity) -> String {
    match identity.target {
        Some(target) => format!("{}; target={target}", identity.rust_toolchain),
        None => identity.rust_toolchain.to_owned(),
    }
}

fn archive_runtime_admission(admission: compatibility::AdmissionLevel) -> RuntimeAdmission {
    match admission {
        compatibility::AdmissionLevel::Verified => RuntimeAdmission::Verified,
        compatibility::AdmissionLevel::Candidate => RuntimeAdmission::Candidate,
    }
}

fn build_archive_provenance(
    selection: compatibility::RuntimeSelection,
    worker_executable_sha256: Option<[u8; 32]>,
    bootstrap_state_kind: BootstrapStateKind,
    bootstrap_slot: Slot,
    bootstrap_state_hash: Hash,
    requested_slot_start: Slot,
    requested_slot_count: u64,
) -> Result<ArchiveProvenance, String> {
    let identity = selection.descriptor.identity;
    if selection.descriptor.worker.is_some() != worker_executable_sha256.is_some() {
        return Err(format!(
            "runtime profile {} worker configuration does not match executable provenance",
            identity.name
        ));
    }
    let genesis_hash = identity.genesis_hash.parse::<Hash>().map_err(|err| {
        format!(
            "runtime descriptor {} has invalid genesis hash {}: {err}",
            identity.name, identity.genesis_hash
        )
    })?;
    let base = ArchiveProvenanceV1 {
        generation_profile: archive_generation_profile(),
        runtime_profile: identity.name.to_owned(),
        runtime_admission: archive_runtime_admission(selection.admission),
        runtime_revision: identity.revision.to_owned(),
        runtime_toolchain: archive_runtime_toolchain(identity),
        genesis_hash,
        bootstrap_state_kind,
        bootstrap_slot,
        bootstrap_state_hash,
        requested_slot_start,
        requested_slot_count,
        transaction_metadata: archive_transaction_metadata_policy(requested_slot_start),
    };
    Ok(match worker_executable_sha256 {
        Some(worker_executable_sha256) => ArchiveProvenanceV2 {
            base,
            worker_executable_sha256,
        }
        .into(),
        None => base.into(),
    })
}

fn archive_worker_executable_matches(
    provenance: &ArchiveProvenance,
    expected_sha256: Option<[u8; 32]>,
) -> bool {
    provenance.single_runtime_worker_executable_sha256() == Some(expected_sha256)
}

struct ValidatedRuntimeSegment {
    archive_path: PathBuf,
    manifest: HistoricalSegmentManifest,
    provenance: ArchiveProvenanceV2,
}

fn load_validated_runtime_segment(
    archive_path: &Path,
    epoch: u64,
    span: &compatibility::RuntimeSpan,
) -> Result<ValidatedRuntimeSegment, String> {
    let manifest = read_and_validate_segment_manifest(archive_path).map_err(|err| {
        format!(
            "runtime segment {} failed durable validation: {err}",
            archive_path.display()
        )
    })?;
    let selection = runtime_span_selection(span)?;
    let identity = selection.descriptor.identity;
    let expected_count = span.slots.end - span.slots.start;
    if manifest.epoch != epoch
        || manifest.output_slot_start != span.slots.start
        || manifest.output_slot_count != expected_count
        || manifest.terminal.slot != span.slots.end - 1
        || manifest.runtime.generation_profile != archive_generation_profile()
        || manifest.runtime.runtime_profile != identity.name
        || manifest.runtime.runtime_revision != identity.revision
        || manifest.runtime.runtime_toolchain != archive_runtime_toolchain(identity)
        || manifest.runtime.runtime_target != identity.target.unwrap_or_default()
        || manifest.runtime.genesis_hash != identity.genesis_hash
        || manifest.bootstrap_archive_sha256.is_some() != span.handoff.is_some()
        || manifest.runtime.runtime_admission
            != match selection.admission {
                compatibility::AdmissionLevel::Verified => SegmentRuntimeAdmission::Verified,
                compatibility::AdmissionLevel::Candidate => SegmentRuntimeAdmission::Candidate,
            }
    {
        return Err(format!(
            "runtime segment {} does not match registry span {}..{} ({})",
            archive_path.display(),
            span.slots.start,
            span.slots.end,
            identity.name
        ));
    }
    let executable = configured_historical_worker_executable(selection.descriptor)?;
    let expected_worker_sha256 =
        historical::measure_executable_sha256(&executable).map_err(|err| {
            format!(
                "failed to measure configured historical worker {}: {err}",
                executable.display()
            )
        })?;
    if manifest.worker_executable_sha256 != expected_worker_sha256 {
        return Err(format!(
            "runtime segment {} was produced by a different worker executable",
            archive_path.display()
        ));
    }
    let file = fs::File::open(archive_path).map_err(|err| {
        format!(
            "failed to open runtime segment {}: {err}",
            archive_path.display()
        )
    })?;
    let reader = jetstreamer_horizon::archive::ArchiveReader::open(std::io::BufReader::new(file))
        .map_err(|err| {
        format!(
            "failed to open runtime segment {}: {err}",
            archive_path.display()
        )
    })?;
    let provenance = reader
        .provenance()
        .map_err(|err| format!("invalid provenance in {}: {err}", archive_path.display()))?
        .ok_or_else(|| {
            format!(
                "runtime segment {} has no provenance",
                archive_path.display()
            )
        })?;
    let ArchiveProvenance::V2(provenance) = provenance else {
        return Err(format!(
            "runtime segment {} does not carry single-runtime V2 provenance",
            archive_path.display()
        ));
    };
    Ok(ValidatedRuntimeSegment {
        archive_path: archive_path.to_path_buf(),
        manifest,
        provenance,
    })
}

fn runtime_checkpoint_from_manifest(
    checkpoint: &SegmentCheckpointSummary,
) -> Result<RuntimeStateCheckpoint, String> {
    Ok(RuntimeStateCheckpoint {
        slot: checkpoint.slot,
        bank_hash: checkpoint
            .bank_hash_value()
            .map_err(|err| format!("invalid checkpoint bank hash: {err}"))?,
        accounts_hash_kind: AccountsHashKind::LegacyAccountsHash,
        accounts_hash: checkpoint
            .accounts_hash_value()
            .map_err(|err| format!("invalid checkpoint accounts hash: {err}"))?,
        last_blockhash: checkpoint
            .last_blockhash_value()
            .map_err(|err| format!("invalid checkpoint last blockhash: {err}"))?,
        capitalization: checkpoint.capitalization,
        transaction_count: checkpoint.transaction_count,
        tick_height: checkpoint.tick_height,
        slot_complete: checkpoint.slot_complete,
        next_write_version: checkpoint.next_write_version,
    })
}

fn build_multi_runtime_provenance(
    epoch: u64,
    spans: &[compatibility::RuntimeSpan],
    segments: &[ValidatedRuntimeSegment],
    handoff_manifests: &[HistoricalHandoffSnapshotManifest],
) -> Result<ArchiveProvenanceV3, String> {
    if spans.len() < 2
        || spans.len() != segments.len()
        || handoff_manifests.len() != segments.len() - 1
    {
        return Err(format!(
            "multi-runtime assembly received {} spans, {} validated sources, and {} handoff manifests",
            spans.len(),
            segments.len(),
            handoff_manifests.len()
        ));
    }
    let (epoch_start, epoch_end_inclusive) = epoch_to_slot_range(epoch);
    if spans
        .first()
        .is_none_or(|span| span.slots.start != epoch_start)
        || spans
            .last()
            .is_none_or(|span| span.slots.end != epoch_end_inclusive + 1)
    {
        return Err(format!(
            "runtime spans do not exactly cover epoch {epoch} ({epoch_start}..={epoch_end_inclusive})"
        ));
    }

    let first = &segments[0];
    let mut archive_write_start = 0u64;
    let mut runtime_segments = Vec::with_capacity(segments.len());
    for (span, segment) in spans.iter().zip(segments) {
        let base = &segment.provenance.base;
        let raw = &segment.manifest.emitted_raw_write_versions;
        runtime_segments.push(RuntimeSegmentProvenance {
            slot_start: span.slots.start,
            slot_count: span.slots.end - span.slots.start,
            generation_profile: base.generation_profile.clone(),
            runtime_profile: base.runtime_profile.clone(),
            runtime_admission: base.runtime_admission,
            runtime_revision: base.runtime_revision.clone(),
            runtime_toolchain: base.runtime_toolchain.clone(),
            worker_executable_sha256: Some(segment.provenance.worker_executable_sha256),
            write_versions: WriteVersionNormalization {
                worker_start: raw.start,
                worker_end_exclusive: raw.end,
                archive_start: archive_write_start,
            },
        });
        archive_write_start = archive_write_start
            .checked_add(raw.end.checked_sub(raw.start).ok_or_else(|| {
                format!(
                    "runtime segment {} has an inverted write range",
                    segment.archive_path.display()
                )
            })?)
            .ok_or_else(|| "normalized archive write-version range overflows u64".to_string())?;
    }

    let mut handoffs = Vec::with_capacity(segments.len() - 1);
    for index in 0..segments.len() - 1 {
        let predecessor = &segments[index];
        let successor = &segments[index + 1];
        let handoff_manifest = &handoff_manifests[index];
        let registry_handoff = spans[index + 1].handoff.ok_or_else(|| {
            format!(
                "runtime transition at slot {} has no registered handoff",
                spans[index + 1].slots.start
            )
        })?;
        let predecessor_checkpoint =
            runtime_checkpoint_from_manifest(&predecessor.manifest.terminal)?;
        let successor_checkpoint = runtime_checkpoint_from_manifest(&successor.manifest.bootstrap)?;
        let expected_accounts_hash = registry_handoff.snapshot.accounts_hash()?;
        if handoff_manifest.boundary_slot != registry_handoff.boundary_slot
            || handoff_manifest.snapshot_slot != registry_handoff.snapshot.slot
            || handoff_manifest.accounts_hash != registry_handoff.snapshot.accounts_hash_base58
            || handoff_manifest.source_runtime != predecessor.manifest.runtime
            || handoff_manifest.source_worker_executable_sha256
                != predecessor.manifest.worker_executable_sha256
            || handoff_manifest.terminal != predecessor.manifest.terminal
            || successor.manifest.bootstrap_archive_sha256 != Some(handoff_manifest.archive_sha256)
        {
            return Err(format!(
                "runtime handoff evidence at slot {} does not match its predecessor and successor segment evidence",
                registry_handoff.boundary_slot
            ));
        }
        if predecessor_checkpoint.accounts_hash != expected_accounts_hash
            || successor_checkpoint.accounts_hash != expected_accounts_hash
        {
            return Err(format!(
                "runtime handoff at slot {} does not match canonical snapshot {}",
                registry_handoff.boundary_slot,
                registry_handoff.snapshot.archive_name()
            ));
        }
        handoffs.push(RuntimeHandoffProvenance {
            boundary_slot: registry_handoff.boundary_slot,
            predecessor: predecessor_checkpoint,
            successor: successor_checkpoint,
            successor_bootstrap_kind: successor.provenance.base.bootstrap_state_kind,
            successor_bootstrap_archive_sha256: successor
                .manifest
                .bootstrap_archive_sha256
                .expect("validated against handoff evidence above"),
            successor_bootstrap_write_count: successor.manifest.bootstrap.write_count,
        });
    }

    let provenance = ArchiveProvenanceV3 {
        assembly_profile: archive_assembly_profile(),
        genesis_hash: first.provenance.base.genesis_hash,
        bootstrap_state_kind: first.provenance.base.bootstrap_state_kind,
        bootstrap_state: StateCommitment {
            slot: first.provenance.base.bootstrap_slot,
            kind: StateCommitmentKind::LegacyAccountsHash,
            hash: first.provenance.base.bootstrap_state_hash,
        },
        requested_slot_start: epoch_start,
        requested_slot_count: epoch_end_inclusive - epoch_start + 1,
        transaction_metadata: first.provenance.base.transaction_metadata,
        runtime_segments,
        handoffs,
    };
    provenance
        .validate()
        .map_err(|err| format!("constructed multi-runtime provenance is invalid: {err}"))?;
    Ok(provenance)
}

fn historical_worker_profile(
    descriptor: &compatibility::RuntimeDescriptor,
) -> Result<historical::WorkerProfile, String> {
    let profile = match descriptor.backend {
        compatibility::RuntimeBackend::SolanaV1_0_7 => historical::SOLANA_V1_0_7_CANDIDATE,
        compatibility::RuntimeBackend::SolanaV1_0_8 => historical::SOLANA_V1_0_8_CANDIDATE,
        compatibility::RuntimeBackend::SolanaV1_0_24 => historical::SOLANA_V1_0_24_CANDIDATE,
        compatibility::RuntimeBackend::AgaveV3 => {
            return Err(
                "the in-process Agave runtime has no historical worker profile".to_string(),
            );
        }
    };
    let identity = descriptor.identity;
    if profile.backend_id != identity.name
        || profile.solana_commit != identity.revision
        || profile.rust_toolchain != identity.rust_toolchain
        || Some(profile.target) != identity.target
        || profile.required_genesis_hash != identity.genesis_hash
    {
        return Err(format!(
            "runtime descriptor {} does not match its compiled historical worker profile",
            identity.name
        ));
    }
    Ok(profile)
}

fn configured_historical_worker_executable(
    descriptor: &compatibility::RuntimeDescriptor,
) -> Result<PathBuf, String> {
    let worker = descriptor.worker.ok_or_else(|| {
        format!(
            "runtime profile {} has no registered worker executable",
            descriptor.identity.name
        )
    })?;
    Ok(env::var_os(worker.environment_override)
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(worker.default_manifest_relative_path)
        }))
}

struct SnapshotArchiveCandidate {
    path: PathBuf,
    slot: Slot,
}

fn snapshot_archive_candidate(path: PathBuf) -> Result<SnapshotArchiveCandidate, String> {
    let metadata = fs::metadata(&path)
        .map_err(|err| format!("failed to read snapshot {}: {err}", path.display()))?;
    if !metadata.is_file() || metadata.len() == 0 {
        return Err(format!(
            "snapshot override must name a non-empty regular file: {}",
            path.display()
        ));
    }
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| format!("snapshot path has no UTF-8 filename: {}", path.display()))?;
    let (slot, _) = parse_snapshot_archive_name(name)?;
    Ok(SnapshotArchiveCandidate { path, slot })
}

fn find_existing_snapshot_archive(
    dest_dir: &Path,
    target_slot: Slot,
    archive_extensions: &[&str],
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
        if !archive_extensions
            .iter()
            .any(|extension| name.ends_with(extension))
        {
            continue;
        }
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

async fn snapshot_expectations_for_span(
    start_slot: Slot,
    end_slot_inclusive: Slot,
    archive_extensions: &[&str],
) -> Result<BTreeMap<Slot, BankHashExpectation>, String> {
    let snapshots = list_snapshots_in_slot_range_matching(
        start_slot,
        end_slot_inclusive,
        archive_extensions,
    )
    .await
    .map_err(|err| {
        format!("failed to list snapshots in slot range {start_slot}..={end_slot_inclusive}: {err}")
    })?;
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
        let expectation = if name.ends_with(".tar.bz2") {
            BankHashExpectation::LegacyAccountsHash(hash.0)
        } else {
            BankHashExpectation::AccountsLtHash(hash)
        };
        if expected.insert(slot, expectation).is_some() {
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

/// Parses a security-sensitive opt-in. Unknown spellings are errors so a typo
/// cannot silently enable an unverified compatibility backend.
fn strict_opt_in(var: &str) -> Result<bool, String> {
    match env::var(var) {
        Err(env::VarError::NotPresent) => parse_strict_opt_in(var, None),
        Err(err) => Err(format!("invalid {var}: {err}")),
        Ok(value) => parse_strict_opt_in(var, Some(&value)),
    }
}

fn parse_strict_opt_in(var: &str, value: Option<&str>) -> Result<bool, String> {
    match value {
        None => Ok(false),
        Some(value) => match value.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" => Ok(true),
            "" | "0" | "false" | "no" => Ok(false),
            _ => Err(format!(
                "invalid {var}={value:?}; expected one of 1/true/yes or 0/false/no"
            )),
        },
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

    // Replay consumes the hardlink farm and live run dirs in place
    // (clean/shrink unlink farm entries; new appendvecs land in run/), so
    // both are rebuilt from the pristine unpacked appendvecs on every
    // load — that is what makes restarts safe without re-extracting. The
    // legacy stage markers for these steps are ignored and removed.
    let _ = fs::remove_file(&hardlinks_marker);
    let _ = fs::remove_file(&run_paths_marker);
    let farm_start = Instant::now();
    ensure_accounts_hardlinks_for_archive(&bank_snapshot, &account_run_dir)?;
    let account_run_paths = account_run_paths_from_snapshot(&bank_snapshot.snapshot_dir)?;
    for path in &account_run_paths {
        reset_dir(path)?;
    }
    info!(
        "rebuilt accounts hardlink farm and run dirs in {:.1}s",
        farm_start.elapsed().as_secs_f64()
    );

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
    let parent = dest.parent().ok_or_else(|| {
        format!(
            "compact index destination has no parent: {}",
            dest.display()
        )
    })?;
    // ripget preallocates the destination to its final length before filling
    // ranges.  Publishing that file directly lets a killed download look
    // complete on the next run.  Keep it under an uncacheable temporary name
    // and atomically publish it only after every range succeeds and is synced.
    let partial = tempfile::Builder::new()
        .prefix(".compact-index-")
        .suffix(".partial")
        .tempfile_in(parent)
        .map_err(|err| {
            format!(
                "failed to create temporary compact index in {}: {err}",
                parent.display()
            )
        })?;
    let partial_path = partial.path().to_path_buf();
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
            &partial_path,
            Some(ripget_threads),
            None,
            Some(progress),
            None,
        ) => result,
    };

    let report = result.map_err(|err| format!("ripget failed for {}: {err}", url.as_str()))?;
    partial
        .as_file()
        .sync_all()
        .map_err(|err| format!("failed to sync compact index {}: {err}", dest.display()))?;
    partial.persist(dest).map_err(|err| {
        format!(
            "failed to publish compact index {}: {}",
            dest.display(),
            err.error
        )
    })?;
    info!(
        "ripget finished: {} -> {} ({})",
        url.as_str(),
        dest.display(),
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

struct CarriedRuntimeState {
    backend: compatibility::RuntimeBackend,
    completed_epoch: u64,
    bank_forks: Arc<RwLock<BankForks>>,
}

struct ReplayRunResult {
    carried_state: Option<CarriedRuntimeState>,
    historical_evidence: Option<historical_replay::HistoricalReplayEvidence>,
    historical_worker_executable_sha256: Option<[u8; 32]>,
    /// Exact registered handoff archive admitted and privately copied before
    /// this segment's historical worker initialized.
    bootstrap_handoff_archive_sha256: Option<[u8; 32]>,
}

fn segment_checkpoint_summary(
    checkpoint: &historical_replay::HistoricalCheckpointSummary,
) -> SegmentCheckpointSummary {
    SegmentCheckpointSummary {
        slot: checkpoint.slot,
        bank_hash: Hash::new_from_array(checkpoint.bank_hash).to_string(),
        accounts_hash: Hash::new_from_array(checkpoint.accounts_hash).to_string(),
        last_blockhash: Hash::new_from_array(checkpoint.last_blockhash).to_string(),
        capitalization: checkpoint.capitalization,
        transaction_count: checkpoint.transaction_count,
        tick_height: checkpoint.tick_height,
        slot_complete: checkpoint.slot_complete,
        write_count: checkpoint.write_count,
        next_write_version: checkpoint.next_write_version,
    }
}

fn segment_runtime_identity(
    selection: compatibility::RuntimeSelection,
) -> Result<SegmentRuntimeIdentity, String> {
    let identity = selection.descriptor.identity;
    let target = identity.target.ok_or_else(|| {
        format!(
            "historical runtime profile {} has no target triple",
            identity.name
        )
    })?;
    Ok(SegmentRuntimeIdentity {
        generation_profile: archive_generation_profile(),
        runtime_profile: identity.name.to_owned(),
        runtime_admission: match selection.admission {
            compatibility::AdmissionLevel::Verified => SegmentRuntimeAdmission::Verified,
            compatibility::AdmissionLevel::Candidate => SegmentRuntimeAdmission::Candidate,
        },
        runtime_revision: identity.revision.to_owned(),
        runtime_toolchain: archive_runtime_toolchain(identity),
        runtime_target: target.to_owned(),
        genesis_hash: identity.genesis_hash.to_owned(),
    })
}

fn publish_historical_segment_manifest(
    epoch: u64,
    plan: QualificationPlan,
    archive_path: &Path,
    result: &ReplayRunResult,
    allow_candidate_runtime: bool,
) -> Result<(), String> {
    let evidence = result.historical_evidence.as_ref().ok_or_else(|| {
        "focused historical segment completed without checkpoint evidence".to_string()
    })?;
    if evidence.bootstrap.slot != plan.bootstrap_slot {
        return Err(format!(
            "historical segment bootstrap evidence is for slot {}, expected {}",
            evidence.bootstrap.slot, plan.bootstrap_slot
        ));
    }
    if evidence.terminal.slot != plan.end_inclusive {
        return Err(format!(
            "historical segment terminal evidence is for slot {}, expected {}",
            evidence.terminal.slot, plan.end_inclusive
        ));
    }
    let selection = compatibility::select_runtime(plan.runtime_range(), allow_candidate_runtime)?;
    let identity = selection.descriptor.identity;
    let worker_executable_sha256 = result.historical_worker_executable_sha256.ok_or_else(|| {
        format!(
            "historical runtime profile {} has no worker digest",
            identity.name
        )
    })?;
    fs::File::open(archive_path)
        .and_then(|archive| archive.sync_all())
        .map_err(|err| {
            format!(
                "failed to sync completed segment {}: {err}",
                archive_path.display()
            )
        })?;
    let manifest = HistoricalSegmentManifest {
        schema_version: jetstreamer_node::segment_manifest::SEGMENT_MANIFEST_SCHEMA_VERSION,
        epoch,
        output_slot_start: plan.output_slot_start,
        output_slot_count: plan.slot_count(),
        runtime: segment_runtime_identity(selection)?,
        worker_executable_sha256,
        // Replaced with the authoritative digest only after a full source
        // validation pass in `write_segment_manifest`.
        archive_sha256: [0; 32],
        bootstrap_archive_sha256: result.bootstrap_handoff_archive_sha256,
        bootstrap: segment_checkpoint_summary(&evidence.bootstrap),
        terminal: segment_checkpoint_summary(&evidence.terminal),
        emitted_raw_write_versions: evidence.emitted_write_versions.clone(),
    };
    let (path, _) = write_segment_manifest(archive_path, manifest)
        .map_err(|err| format!("failed to publish segment evidence: {err}"))?;
    // Exercise the durable read path before telling a supervising parent that
    // this child succeeded.
    read_and_validate_segment_manifest(archive_path).map_err(|err| {
        format!(
            "failed to re-read segment evidence {}: {err}",
            path.display()
        )
    })?;
    info!(
        "published verified historical segment evidence {}",
        path.display()
    );
    Ok(())
}

#[allow(clippy::too_many_arguments)] // top-level replay entry point: CLI params map 1:1
async fn run_geyser_replay(
    epoch: u64,
    allow_candidate_runtime: bool,
    ledger_dir: &Path,
    snapshot_archive: &Path,
    shutdown: Arc<AtomicBool>,
    cursor: Arc<ReplayCursor>,
    restart_tracker: Arc<RestartTracker>,
    snapshot_verifier: Option<Arc<SnapshotVerifier>>,
    horizon_output: PathBuf,
    // Focused qualification records exactly bootstrap+1..=end instead of a
    // complete epoch. Normal production callers pass None.
    qualification: Option<QualificationPlan>,
    // When set, reuse this bank (carried from the previous epoch in a range
    // run) instead of loading a snapshot — no reload, no warmup.
    carried_state: Option<CarriedRuntimeState>,
    // Cross-epoch progress for a multi-epoch range run (None for single epoch).
    range_progress: Option<Arc<RangeProgress>>,
    // Progress counters shared across every epoch of a range. The accounts-db
    // update notifier is created once (at the first epoch's snapshot load) and
    // carried with the reused bank, so it forever points at whatever
    // `ReplayProgress` it captured. Sharing one instance keeps every chained
    // epoch's counter (and the account-update stall watchdog) live; a per-epoch
    // instance would leave chained epochs stuck at accounts=0 and falsely abort.
    // Counters are reset per epoch below, so sharing does not accumulate.
    carried_progress: Option<Arc<ReplayProgress>>,
) -> Result<ReplayRunResult, String> {
    if qualification.is_some() && carried_state.is_some() {
        return Err("focused qualification cannot reuse carried runtime state".to_string());
    }
    // Resolve the exact state span before starting a worker or loading a bank.
    // Snapshot filename/format supplies only the bootstrap slot and loader
    // details; execution semantics come exclusively from the slot registry.
    let bootstrap_slot = match carried_state.as_ref() {
        Some(state) => state
            .bank_forks
            .read()
            .map_err(|_| "bank forks lock poisoned".to_string())?
            .working_bank()
            .slot(),
        None => {
            let archive_name = snapshot_archive
                .file_name()
                .and_then(|name| name.to_str())
                .ok_or_else(|| {
                    format!(
                        "snapshot path has no UTF-8 filename: {}",
                        snapshot_archive.display()
                    )
                })?;
            parse_snapshot_archive_name(archive_name)?.0
        }
    };
    let (epoch_start, epoch_end_inclusive) = epoch_to_slot_range(epoch);
    let (output_slot_start, replay_start, end_inclusive) = match qualification {
        Some(plan) => {
            if plan.epoch != epoch {
                return Err(format!(
                    "qualification plan targets epoch {}, but replay requested epoch {epoch}",
                    plan.epoch
                ));
            }
            if plan.bootstrap_slot != bootstrap_slot {
                return Err(format!(
                    "qualification plan bootstraps at slot {}, but loaded archive names slot {bootstrap_slot}",
                    plan.bootstrap_slot
                ));
            }
            (
                plan.output_slot_start,
                plan.replay_start,
                plan.end_inclusive,
            )
        }
        None => (
            epoch_start,
            initial_replay_slot(bootstrap_slot, epoch_start)?,
            epoch_end_inclusive,
        ),
    };
    let execution = compatibility::select_runtime(
        replay_start..end_inclusive.saturating_add(1),
        allow_candidate_runtime,
    )?;
    if qualification.is_some() || execution.admission == compatibility::AdmissionLevel::Candidate {
        let Some(verifier) = snapshot_verifier.as_ref() else {
            return Err(format!(
                "runtime profile {} requires snapshot verification for this {} run",
                execution.backend,
                if qualification.is_some() {
                    "qualification"
                } else {
                    "candidate"
                }
            ));
        };
        let checkpoint_count = verifier.checkpoint_count_in_range(replay_start, end_inclusive);
        if checkpoint_count == 0 {
            return Err(format!(
                "runtime profile {} requires at least one trusted post-bootstrap checkpoint in replay range {}..={}; the supplied checkpoint set is empty for that range",
                execution.backend, replay_start, end_inclusive
            ));
        }
        info!(
            "runtime profile {} has {} post-bootstrap checkpoint(s) in replay range",
            execution.backend, checkpoint_count
        );
    }
    let runtime_backend = execution.backend;
    let runtime_descriptor = execution.descriptor;
    if let Some(state) = carried_state.as_ref() {
        if state.completed_epoch.checked_add(1) != Some(epoch) {
            return Err(format!(
                "carried runtime state completed epoch {}, but replay requested epoch {}",
                state.completed_epoch, epoch
            ));
        }
        if state.backend != runtime_backend {
            return Err(format!(
                "carried runtime state uses {}, but slot range selected {}",
                state.backend, runtime_backend
            ));
        }
        let (completed_start, completed_end) = epoch_to_slot_range(state.completed_epoch);
        if !(completed_start..=completed_end).contains(&bootstrap_slot) {
            return Err(format!(
                "carried runtime state at slot {bootstrap_slot} is outside completed epoch {} ({}..={})",
                state.completed_epoch, completed_start, completed_end
            ));
        }
        if !runtime_descriptor.bootstrap.permits_in_memory_handoff {
            return Err(format!(
                "runtime profile {} does not permit an in-memory bank handoff",
                runtime_descriptor.identity.name
            ));
        }
    } else {
        validate_runtime_bootstrap_archive(runtime_descriptor, snapshot_archive)?;
    }
    // Registered generated handoffs require an adjacent evidence sidecar. The
    // complete archive digest covers replay-relevant state that the historical
    // accounts hash does not, including the transaction status cache.
    let bootstrap_handoff_manifest = if carried_state.is_none() {
        registered_handoff_bootstrap(runtime_descriptor, snapshot_archive)?
    } else {
        None
    };
    info!(
        "slot registry selected execution profile {} ({:?}) for slots {}..{}",
        runtime_backend,
        execution.admission,
        replay_start,
        end_inclusive.saturating_add(1),
    );

    let (confirmed_bank_sender, confirmed_bank_receiver) = unbounded();
    let confirmed_bank_handle =
        std::thread::spawn(move || while confirmed_bank_receiver.recv().is_ok() {});
    let progress =
        carried_progress.unwrap_or_else(|| Arc::new(ReplayProgress::new(output_slot_start)));
    // Fresh per-epoch baseline: a chained epoch reuses the shared instance, so
    // clear last-run counts before this epoch's replay begins (no notifications
    // are in flight here — the previous epoch's replay has fully returned).
    progress.reset_counts();
    let failure = Arc::new(ReplayFailure::new(shutdown.clone()));
    plugin::reset();
    info!("direct plugin notifier enabled");
    let ledger_dir = ledger_dir.to_path_buf();
    let root_interval = bank_root_interval();
    if let Some(interval) = root_interval {
        info!("bank root pruning interval: {interval}");
    } else {
        info!("bank root pruning disabled");
    }

    // Runtime selection is driven by the requested slots. Archive type only
    // selects the corresponding state loader after that decision has been made.
    enum BankSource {
        Reuse(Arc<RwLock<BankForks>>),
        Fresh(Box<Bank>),
    }
    enum ReplaySource {
        Agave(BankSource),
        Historical(Box<historical::HistoricalRuntimeClient>),
    }
    let (replay_source, snapshot_slot, bootstrap_state_kind, bootstrap_state_hash) =
        match runtime_backend {
            compatibility::RuntimeBackend::AgaveV3 => match carried_state {
                Some(state) => {
                    let bank_forks = state.bank_forks;
                    let (slot, state_hash) = {
                        let bank_forks = bank_forks
                            .read()
                            .map_err(|_| "bank forks lock poisoned".to_string())?;
                        let bank = bank_forks.working_bank();
                        (bank.slot(), bank.get_snapshot_hash().0)
                    };
                    info!(
                        "reusing in-memory bank from previous epoch at slot {slot}; skipping snapshot load"
                    );
                    (
                        ReplaySource::Agave(BankSource::Reuse(bank_forks)),
                        slot,
                        BootstrapStateKind::CarriedBank,
                        state_hash,
                    )
                }
                None => {
                    let accounts_update_notifier: Option<AccountsUpdateNotifier> =
                        Some(Arc::new(ProgressAccountsUpdateNotifier {
                            progress: progress.clone(),
                            live_start_slot: output_slot_start,
                        }) as AccountsUpdateNotifier);
                    info!("accounts update notifier wired into snapshot load: true");
                    info!("loading Agave bank from snapshot");
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
                    // Qualification checkpoint sets retain the bootstrap entry
                    // so the loaded state itself is proven before replaying its
                    // successor. Historical workers perform the same check as
                    // part of `HistoricalReplay::new`.
                    if qualification.is_some()
                        && let Some(verifier) = snapshot_verifier.as_ref()
                    {
                        verifier.verify_bank(&bank);
                        if let Some(message) = verifier.error_summary() {
                            return Err(message);
                        }
                    }
                    let slot = bank.slot();
                    let state_hash = bank.get_snapshot_hash().0;
                    (
                        ReplaySource::Agave(BankSource::Fresh(Box::new(bank))),
                        slot,
                        BootstrapStateKind::SnapshotArchive,
                        state_hash,
                    )
                }
            },
            compatibility::RuntimeBackend::SolanaV1_0_7
            | compatibility::RuntimeBackend::SolanaV1_0_8
            | compatibility::RuntimeBackend::SolanaV1_0_24 => {
                if carried_state.is_some() {
                    return Err(
                        "an Agave in-memory bank cannot be handed to a Solana v1 worker"
                            .to_string(),
                    );
                }
                let archive_name = snapshot_archive
                    .file_name()
                    .and_then(|name| name.to_str())
                    .ok_or_else(|| {
                        format!(
                            "historical snapshot path has no UTF-8 filename: {}",
                            snapshot_archive.display()
                        )
                    })?;
                let (expected_slot, expected_hash) = parse_snapshot_archive_name(archive_name)?;
                let bootstrap_state_hash = expected_hash.0;
                let worker_profile = historical_worker_profile(runtime_descriptor)?;
                let executable = configured_historical_worker_executable(runtime_descriptor)?;
                let scratch_parent = ledger_dir.join(".historical-runtime");
                fs::create_dir_all(&scratch_parent).map_err(|err| {
                    format!(
                        "failed to create historical runtime scratch directory {}: {err}",
                        scratch_parent.display()
                    )
                })?;
                info!(
                    "starting isolated {} worker {}",
                    runtime_backend,
                    executable.display()
                );
                let spawn = historical::WorkerSpawn {
                    executable,
                    initialization: historical::SnapshotInitialization {
                        ledger_path: ledger_dir.clone(),
                        archive_path: snapshot_archive.to_path_buf(),
                        expected_slot,
                        expected_accounts_hash: expected_hash.0.to_bytes(),
                        expected_archive_sha256: bootstrap_handoff_manifest
                            .as_ref()
                            .map(|manifest| manifest.archive_sha256),
                        expected_archive_size: bootstrap_handoff_manifest
                            .as_ref()
                            .map(|manifest| manifest.archive_size),
                        scratch_parent: Some(scratch_parent),
                    },
                };
                let client = tokio::task::spawn_blocking(move || {
                    historical::HistoricalRuntimeClient::spawn(worker_profile, spawn)
                })
                .await
                .map_err(|err| format!("historical worker startup task failed: {err}"))?
                .map_err(|err| format!("historical worker startup failed: {err}"))?;
                let slot = client.initialized().slot;
                info!(
                    "historical worker initialized at slot {} (last_blockhash={}, ticks_per_slot={}, next_write_version={})",
                    slot,
                    Hash::new_from_array(client.initialized().last_blockhash),
                    client.initialized().ticks_per_slot,
                    client.initialized().next_write_version,
                );
                (
                    ReplaySource::Historical(Box::new(client)),
                    slot,
                    BootstrapStateKind::SnapshotArchive,
                    bootstrap_state_hash,
                )
            }
        };
    if snapshot_slot != bootstrap_slot {
        return Err(format!(
            "loaded state slot {} does not match the preflighted bootstrap slot {}",
            snapshot_slot, bootstrap_slot,
        ));
    }
    if replay_start < output_slot_start {
        info!(
            "warming up replay from slot {} to {} (recording starts at {})",
            replay_start,
            output_slot_start.saturating_sub(1),
            output_slot_start
        );
    } else {
        info!("starting replay at epoch {epoch} slot {replay_start}");
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
    let output_slot_count = qualification
        .map(QualificationPlan::slot_count)
        .unwrap_or_else(|| end_inclusive - output_slot_start + 1);
    let worker_executable_sha256 = match &replay_source {
        ReplaySource::Historical(client) => Some(client.executable_sha256()),
        ReplaySource::Agave(_) => None,
    };
    let archive_provenance = build_archive_provenance(
        execution,
        worker_executable_sha256,
        bootstrap_state_kind,
        snapshot_slot,
        bootstrap_state_hash,
        output_slot_start,
        output_slot_count,
    )?;
    horizon::init(
        &horizon_output,
        epoch,
        output_slot_start,
        output_slot_count,
        slot_presence.clone(),
        &archive_provenance,
    )?;
    info!(
        "horizon archive recording to {} (epoch {}, slots {}..={}, runtime={}, admission={:?}, bootstrap={:?}@{})",
        horizon_output.display(),
        epoch,
        output_slot_start,
        end_inclusive,
        execution.descriptor.identity.name,
        execution.admission,
        bootstrap_state_kind,
        snapshot_slot,
    );
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
    let mut agave_bank_replay = None;
    let replay_executor: Arc<dyn ReplayExecutor> = match replay_source {
        ReplaySource::Agave(bank_source) => {
            let replay = Arc::new(match bank_source {
                BankSource::Fresh(bank) => BankReplay::new(
                    *bank,
                    snapshot_verifier.clone(),
                    root_interval,
                    failure.clone(),
                    cursor.clone(),
                    scheduler.clone(),
                    firehose_gate.clone(),
                    output_slot_start,
                    enable_program_cache_prune,
                    enable_accounts_maintenance,
                    accounts_maintenance_root_stride,
                ),
                BankSource::Reuse(bank_forks) => BankReplay::from_bank_forks(
                    bank_forks,
                    snapshot_verifier.clone(),
                    root_interval,
                    failure.clone(),
                    cursor.clone(),
                    scheduler.clone(),
                    firehose_gate.clone(),
                    output_slot_start,
                    enable_program_cache_prune,
                    enable_accounts_maintenance,
                    accounts_maintenance_root_stride,
                ),
            });
            agave_bank_replay = Some(replay.clone());
            replay
        }
        ReplaySource::Historical(client) => Arc::new(historical_replay::HistoricalReplay::new(
            *client,
            snapshot_verifier.clone(),
            failure.clone(),
            cursor.clone(),
            progress.clone(),
            output_slot_start,
        )?),
    };
    let ready_queue_capacity = ready_entry_queue_capacity();
    info!("ready entry queue capacity: {}", ready_queue_capacity);
    let (ready_sender, ready_receiver) = bounded::<Vec<ReadyEntry>>(ready_queue_capacity);
    let ready_shutdown = shutdown.clone();
    let ready_replay_executor = replay_executor.clone();
    let ready_handle = std::thread::Builder::new()
        .name("readyEntries".to_string())
        .stack_size(64 * 1024 * 1024)
        .spawn(move || {
            // The scheduler drains ready entries per firehose notification,
            // so individual messages typically hold only a couple of
            // entries. Coalesce everything already queued before replaying
            // so slot groups span whole stretches of the slot — that is
            // what gives the wave scheduler real parallelism to exploit.
            const COALESCE_CAP: usize = 4096;
            loop {
                // Time the blocking wait separately: if this dominates wall
                // time, replay is starved by firehose input (download/decode),
                // not bound by execution. The backlog depth right after the
                // wait is the corroborating signal — a deep queue means
                // compute-bound, a near-empty one means input-bound.
                let recv_start = Instant::now();
                let mut entries = match ready_receiver.recv() {
                    Ok(entries) => entries,
                    Err(_) => break,
                };
                PHASE_RECV_WAIT_US
                    .fetch_add(recv_start.elapsed().as_micros() as u64, Ordering::Relaxed);
                PHASE_RECV_BACKLOG.fetch_add(ready_receiver.len() as u64, Ordering::Relaxed);
                PHASE_RECV_COUNT.fetch_add(1, Ordering::Relaxed);
                if ready_shutdown.load(Ordering::Relaxed) {
                    break;
                }
                while entries.len() < COALESCE_CAP {
                    match ready_receiver.try_recv() {
                        Ok(more) => entries.extend(more),
                        Err(_) => break,
                    }
                }
                ready_replay_executor.process_ready_entries(entries);
            }
        })
        .expect("failed to spawn ready entry thread");
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
        live_start_slot: output_slot_start,
        shutdown: shutdown.clone(),
        active_firehose_stop: active_firehose_stop.clone(),
        backpressure_stop_requested: backpressure_stop_requested.clone(),
        firehose_backpressure_slot_gap_limit,
        firehose_gate: firehose_gate.clone(),
    });
    let notifiers = GeyserNotifiers {
        transaction_notifier: None,
        sourced_transaction_notifier: Some(transaction_notifier.clone()),
        entry_notifier: Some(entry_notifier.clone()),
        block_metadata_notifier: Some(block_metadata_notifier.clone()),
    };
    let threads = firehose_threads();
    let buffer_window_bytes = firehose_buffer_window_bytes();
    info!(
        "firehose: sequential=true, ripget_threads={}, buffer_window={}",
        threads,
        buffer_window_bytes
            .map(jetstreamer_firehose::system::format_byte_size)
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
        let range_progress = range_progress.clone();
        std::thread::spawn(move || {
            let has_warmup = replay_start < output_slot_start;
            let warmup_end = output_slot_start.saturating_sub(1);
            let warmup_total = if has_warmup {
                warmup_end.saturating_sub(replay_start).saturating_add(1)
            } else {
                0
            };
            let main_total = end_inclusive
                .saturating_sub(output_slot_start)
                .saturating_add(1);
            let stall_interval = env::var("JETSTREAMER_STALL_LOG_SECS")
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
                .map(Duration::from_secs)
                .unwrap_or(Duration::from_secs(1800));
            // The per-wave phase breakdown is verbose; emit it far less often
            // than the progress line (which still ticks every loop). 0 disables.
            let phases_interval = env::var("JETSTREAMER_PHASES_LOG_SECS")
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
                .map(Duration::from_secs)
                .unwrap_or(Duration::from_secs(60));
            let mut last_phases_log: Option<Instant> = None;
            let mut maybe_log_phases = |force: bool| {
                if phases_interval.is_zero() {
                    return;
                }
                let due =
                    last_phases_log.is_none_or(|last: Instant| last.elapsed() >= phases_interval);
                if (force || due)
                    && let Some(phases) = phases_summary()
                {
                    info!("{phases}");
                    last_phases_log = Some(Instant::now());
                }
            };
            let inflight_warn_after = ENTRY_EXEC_WARN_AFTER;
            let inflight_fail_after = *ENTRY_EXEC_FAIL_AFTER;
            let mut phase_start = None::<Instant>;
            let mut in_warmup = has_warmup;
            // Slots already replayed when the main phase's timer started;
            // excluded from the rate so the first post-warmup ticks don't
            // divide warmup-era progress by near-zero elapsed time.
            let mut main_rate_baseline: u64 = 0;
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
                let phase = if in_warmup && latest < output_slot_start {
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
                                let processed = if in_warmup && latest < output_slot_start {
                                    if latest < replay_start {
                                        0
                                    } else {
                                        latest.saturating_sub(replay_start).saturating_add(1)
                                    }
                                } else {
                                    let display_slot =
                                        latest.clamp(output_slot_start, end_inclusive);
                                    if display_slot < output_slot_start {
                                        0
                                    } else {
                                        display_slot
                                            .saturating_sub(output_slot_start)
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
                if in_warmup && latest < output_slot_start {
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
                    info!(
                        "warmup slot {display_slot}/{warmup_end} ({percent:.2}%) txs={tx_count} accounts={account_updates} slots_per_sec={slots_per_sec} accounts_per_sec={accounts_per_sec} eta={eta} (recording starts at {output_slot_start})"
                    );
                    maybe_log_phases(false);
                } else {
                    if in_warmup {
                        in_warmup = false;
                        phase_start = Some(Instant::now());
                        main_rate_baseline = latest
                            .clamp(output_slot_start, end_inclusive)
                            .saturating_sub(output_slot_start)
                            .saturating_add(1);
                        progress.reset_counts();
                        last_seen_tx_count = 0;
                        last_account_updates = 0;
                        last_account_update_slot_seen = latest;
                        last_account_change = Instant::now();
                        last_account_log = Instant::now();
                    }
                    let display_slot = latest.clamp(output_slot_start, end_inclusive);
                    let processed = if display_slot < output_slot_start {
                        0
                    } else {
                        display_slot
                            .saturating_sub(output_slot_start)
                            .saturating_add(1)
                    };
                    let percent = if main_total == 0 {
                        100.0
                    } else {
                        (processed as f64) * 100.0 / (main_total as f64)
                    };
                    let processed_this_phase = processed.saturating_sub(main_rate_baseline);
                    let slots_per_sec = if processed_this_phase == 0 {
                        "n/a".to_string()
                    } else if let Some(start) = phase_start {
                        let elapsed = start.elapsed().as_secs_f64();
                        if elapsed <= 0.0 {
                            "n/a".to_string()
                        } else {
                            format!("{:.2}", (processed_this_phase as f64) / elapsed)
                        }
                    } else {
                        "n/a".to_string()
                    };
                    let eta = if processed_this_phase == 0 {
                        "unknown".to_string()
                    } else if let Some(start) = phase_start {
                        let elapsed = start.elapsed().as_secs_f64();
                        if elapsed <= 0.0 {
                            "unknown".to_string()
                        } else {
                            let rate = (processed_this_phase as f64) / elapsed;
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
                    let horizon_size = jetstreamer_firehose::system::format_byte_size(
                        horizon::archive_bytes_written(),
                    );
                    let anon_rss = process_anon_rss_bytes()
                        .map(jetstreamer_firehose::system::format_byte_size)
                        .unwrap_or_else(|| "n/a".to_string());
                    info!(
                        "progress slot {display_slot}/{end_inclusive} ({percent:.2}%) txs={tx_count} accounts={account_updates} slots_per_sec={slots_per_sec} eta={eta} horizon={horizon_size} mem={anon_rss}"
                    );
                    // Overall span progress + ETA across the whole multi-epoch
                    // run, on its own line. The rate baseline is captured once
                    // (first main-phase tick of the run) and shared across
                    // epochs, so the ETA reflects the true range-wide pace.
                    if let Some(rp) = range_progress.as_ref() {
                        let overall_total = rp
                            .overall_end_slot
                            .saturating_sub(rp.overall_start_slot)
                            .saturating_add(1);
                        let overall_done = display_slot
                            .saturating_sub(rp.overall_start_slot)
                            .saturating_add(1);
                        let overall_percent = if overall_total == 0 {
                            100.0
                        } else {
                            (overall_done as f64) * 100.0 / (overall_total as f64)
                        };
                        let epoch_idx = epoch.saturating_sub(rp.first_epoch).saturating_add(1);
                        let mut baseline = rp.baseline.lock().unwrap();
                        let (base_instant, base_slot) =
                            *baseline.get_or_insert((Instant::now(), display_slot));
                        drop(baseline);
                        let elapsed = base_instant.elapsed().as_secs_f64();
                        let advanced = display_slot.saturating_sub(base_slot);
                        let overall_eta = if advanced == 0 || elapsed <= 0.0 {
                            "unknown".to_string()
                        } else {
                            let rate = (advanced as f64) / elapsed;
                            let remaining = rp.overall_end_slot.saturating_sub(display_slot);
                            format_eta(Duration::from_secs(
                                ((remaining as f64) / rate).ceil() as u64
                            ))
                        };
                        info!(
                            "overall slot {display_slot}/{} ({overall_percent:.2}%) epoch {epoch_idx}/{} eta={overall_eta}",
                            rp.overall_end_slot, rp.total_epochs
                        );
                    }
                    maybe_log_phases(false);
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
        if shutdown.load(Ordering::Relaxed) {
            info!("shutdown requested; abandoning replay");
            break;
        }
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
                sourced_transaction_notifier: notifiers.sourced_transaction_notifier.clone(),
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

        if shutdown.load(Ordering::Relaxed) {
            info!("shutdown requested; abandoning replay");
            break;
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

    // A checkpoint mismatch deliberately stops the shared replay pipeline,
    // which can make producers observe a secondary closed-channel error.
    // Preserve the consensus-relevant verification failure as the primary
    // diagnostic instead of letting shutdown plumbing obscure it.
    if let Some(message) = snapshot_verifier
        .as_ref()
        .and_then(|verifier| verifier.error_summary())
    {
        return Err(message);
    }

    if let Some(message) = failure.error_message() {
        return Err(message);
    }

    if let Some(verifier) = snapshot_verifier {
        replay_executor.verify_latest_bank()?;
        verifier.finish()?;
        info!("snapshot verification complete");
    }

    if horizon::recorder().is_some() {
        // Freeze the final bank so its end-of-slot account updates are
        // recorded before the archive closes.
        replay_executor.freeze_latest_bank()?;
        // Finalize and tear down this epoch's archive so the next epoch in a
        // range run can install its own.
        horizon::finish()?;
    }

    let carried_state = agave_bank_replay.map(|replay| CarriedRuntimeState {
        backend: runtime_backend,
        completed_epoch: epoch,
        bank_forks: replay.bank_forks(),
    });
    let historical_evidence = replay_executor.historical_evidence()?;
    if let Some(output_directory) = env::var_os("JETSTREAMER_EXPORT_HANDOFF_SNAPSHOT_DIR") {
        let output_directory = PathBuf::from(output_directory);
        let plan = qualification.ok_or_else(|| {
            "handoff snapshot export is allowed only for a focused qualification run".to_string()
        })?;
        let handoff = compatibility::RUNTIME_HANDOFFS
            .iter()
            .copied()
            .find(|handoff| {
                handoff.snapshot.slot == plan.end_inclusive
                    && std::ptr::eq(handoff.source, runtime_descriptor)
            })
            .ok_or_else(|| {
                format!(
                    "no registered handoff snapshot follows runtime {} at slot {}",
                    runtime_descriptor.identity.name, plan.end_inclusive
                )
            })?;
        let evidence = historical_evidence.as_ref().ok_or_else(|| {
            "handoff snapshot export requires historical checkpoint evidence".to_string()
        })?;
        let expected_accounts_hash = handoff.snapshot.accounts_hash()?;
        if evidence.terminal.accounts_hash != expected_accounts_hash.to_bytes() {
            return Err(format!(
                "terminal checkpoint at slot {} does not match registered handoff accounts hash {}",
                evidence.terminal.slot, expected_accounts_hash
            ));
        }
        let exported = replay_executor
            .export_historical_snapshot(
                handoff.snapshot.slot,
                &output_directory,
                expected_accounts_hash.to_bytes(),
            )?
            .ok_or_else(|| {
                format!(
                    "runtime {} cannot export a historical handoff snapshot",
                    runtime_descriptor.identity.name
                )
            })?;
        let expected_path = fs::canonicalize(&output_directory)
            .map_err(|err| {
                format!(
                    "failed to canonicalize handoff output directory {}: {err}",
                    output_directory.display()
                )
            })?
            .join(handoff.snapshot.archive_name());
        if exported.archive_path != expected_path {
            return Err(format!(
                "historical worker exported handoff snapshot to {}, expected {}",
                exported.archive_path.display(),
                expected_path.display()
            ));
        }
        if exported.archive_size == 0 || exported.archive_sha256 == [0; 32] {
            return Err("historical worker returned empty snapshot evidence".to_string());
        }
        let source_worker_executable_sha256 = worker_executable_sha256.ok_or_else(|| {
            format!(
                "runtime {} exported a snapshot without a measured worker executable",
                runtime_descriptor.identity.name
            )
        })?;
        let manifest = HistoricalHandoffSnapshotManifest {
            schema_version: HANDOFF_SNAPSHOT_MANIFEST_SCHEMA_VERSION,
            boundary_slot: handoff.boundary_slot,
            snapshot_slot: handoff.snapshot.slot,
            accounts_hash: expected_accounts_hash.to_string(),
            archive_path: String::new(),
            archive_size: exported.archive_size,
            archive_sha256: exported.archive_sha256,
            source_runtime: segment_runtime_identity(execution)?,
            source_worker_executable_sha256,
            terminal: segment_checkpoint_summary(&evidence.terminal),
        };
        let (manifest_path, published) =
            write_handoff_snapshot_manifest(&exported.archive_path, manifest)
                .map_err(|error| format!("failed to publish handoff snapshot evidence: {error}"))?;
        if published.archive_size != exported.archive_size
            || published.archive_sha256 != exported.archive_sha256
        {
            return Err(format!(
                "handoff snapshot {} changed between worker export validation and evidence publication",
                exported.archive_path.display()
            ));
        }
        let admitted = validate_canonical_handoff_snapshot(&exported.archive_path, handoff)?;
        if admitted != published {
            return Err(format!(
                "handoff snapshot evidence {} changed during publication",
                manifest_path.display()
            ));
        }
        info!(
            "exported canonical runtime handoff snapshot {} with evidence {} ({} bytes, sha256={})",
            exported.archive_path.display(),
            manifest_path.display(),
            exported.archive_size,
            jetstreamer_node::segment_manifest::sha256_hex_string(&exported.archive_sha256),
        );
    }
    replay_executor.shutdown()?;
    Ok(ReplayRunResult {
        carried_state,
        historical_evidence,
        historical_worker_executable_sha256: worker_executable_sha256,
        bootstrap_handoff_archive_sha256: bootstrap_handoff_manifest
            .map(|manifest| manifest.archive_sha256),
    })
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
    for name in ["accounts", "accounts-index", BANK_SNAPSHOTS_DIR] {
        let path = ledger_dir.join(name);
        if path.exists() {
            remove_path_if_exists(&path)?;
            cleared.push(path);
        }
    }
    // The archive loader's unpacked appendvecs (accounts-run root files)
    // and bank-snapshot staging (.snapshot-extract-*) are pristine: replay
    // only mutates the hardlink farm (accounts-run/snapshot), the live run
    // dir (accounts-run/run), and dir-loader artifacts. Keeping the
    // pristine unpack lets restarts skip the multi-minute re-extract — the
    // loader rebuilds the hardlink farm and run dirs fresh on every load.
    let archive_accounts = ledger_dir.join(ARCHIVE_ACCOUNTS_DIR);
    for sub in [ACCOUNTS_SNAPSHOT_DIR, "run"] {
        let path = archive_accounts.join(sub);
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

/// Determines whether a finalized archive can be reused by range resume.
/// Structural completeness alone is insufficient: a valid file generated by
/// another runtime profile, source policy, or slot range must not bypass the
/// current compatibility selection.
fn epoch_archive_reusable(
    path: &Path,
    epoch: u64,
    selection: compatibility::RuntimeSelection,
) -> Result<bool, String> {
    let file = match fs::File::open(path) {
        Ok(file) => file,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(err) => return Err(format!("failed to open {}: {err}", path.display())),
    };
    let reader =
        match jetstreamer_horizon::archive::ArchiveReader::open(std::io::BufReader::new(file)) {
            Ok(reader) => reader,
            Err(_) => return Ok(false),
        };
    let (slot_start, slot_end_inclusive) = epoch_to_slot_range(epoch);
    let expected_slot_count = slot_end_inclusive - slot_start + 1;
    let header = reader.header();
    if header.epoch != epoch
        || header.slot_start != slot_start
        || header.slot_count != expected_slot_count
    {
        return Err(format!(
            "completed archive {} has header epoch={} slots={}+{}, expected epoch={} slots={}+{}",
            path.display(),
            header.epoch,
            header.slot_start,
            header.slot_count,
            epoch,
            slot_start,
            expected_slot_count
        ));
    }
    let expected_bucket_count = expected_slot_count.div_ceil(u64::from(header.bucket_slots));
    if reader.bucket_count() as u64 != expected_bucket_count {
        return Err(format!(
            "completed archive {} has {} buckets, expected {} for {} slots at {} slots per bucket",
            path.display(),
            reader.bucket_count(),
            expected_bucket_count,
            expected_slot_count,
            header.bucket_slots
        ));
    }
    let provenance = reader
        .provenance()
        .map_err(|err| {
            format!(
                "completed archive {} has invalid provenance: {err}",
                path.display()
            )
        })?
        .ok_or_else(|| {
            format!(
                "completed archive {} predates required runtime provenance; refusing to skip it",
                path.display()
            )
        })?;
    let provenance_v1 = provenance.single_runtime_v1().ok_or_else(|| {
        format!(
            "completed archive {} has multi-runtime provenance V{}; resuming from assembled archives is not implemented yet",
            path.display(),
            provenance.version(),
        )
    })?;
    let recorded_worker_executable_sha256 = provenance
        .single_runtime_worker_executable_sha256()
        .expect("single-runtime provenance has a single worker-digest field");
    let identity = selection.descriptor.identity;
    let expected_genesis = identity.genesis_hash.parse::<Hash>().map_err(|err| {
        format!(
            "runtime descriptor {} has invalid genesis hash {}: {err}",
            identity.name, identity.genesis_hash
        )
    })?;
    let expected_toolchain = archive_runtime_toolchain(identity);
    let expected_metadata = archive_transaction_metadata_policy(slot_start);
    let expected_generation_profile = archive_generation_profile();
    if provenance_v1.generation_profile != expected_generation_profile
        || provenance_v1.runtime_profile != identity.name
        || provenance_v1.runtime_admission != archive_runtime_admission(selection.admission)
        || provenance_v1.runtime_revision != identity.revision
        || provenance_v1.runtime_toolchain != expected_toolchain
        || provenance_v1.genesis_hash != expected_genesis
        || provenance_v1.transaction_metadata != expected_metadata
    {
        return Err(format!(
            "completed archive {} was generated with incompatible provenance: {:?}",
            path.display(),
            provenance_v1
        ));
    }

    let expected_worker_executable_sha256 = if selection.descriptor.worker.is_some() {
        let executable = configured_historical_worker_executable(selection.descriptor)?;
        Some(
            historical::measure_executable_sha256(&executable).map_err(|err| {
                format!(
                    "failed to measure configured historical worker {}: {err}",
                    executable.display()
                )
            })?,
        )
    } else {
        None
    };
    if !archive_worker_executable_matches(&provenance, expected_worker_executable_sha256) {
        return Err(format!(
            "completed archive {} has historical worker executable SHA-256 {:?}, expected {:?}",
            path.display(),
            recorded_worker_executable_sha256,
            expected_worker_executable_sha256,
        ));
    }

    match provenance_v1.bootstrap_state_kind {
        BootstrapStateKind::Genesis => {
            if epoch != 0 || provenance_v1.bootstrap_slot != 0 {
                return Err(format!(
                    "completed archive {} claims an invalid genesis bootstrap at slot {} for epoch {}",
                    path.display(),
                    provenance_v1.bootstrap_slot,
                    epoch
                ));
            }
        }
        BootstrapStateKind::SnapshotArchive | BootstrapStateKind::CarriedBank => {
            if epoch == 0 {
                return Err(format!(
                    "completed epoch-0 archive {} did not bootstrap from genesis",
                    path.display()
                ));
            }
            let min_bootstrap_slot = epoch_to_slot(epoch - 1);
            let max_bootstrap_slot = slot_start - 1;
            if !(min_bootstrap_slot..=max_bootstrap_slot).contains(&provenance_v1.bootstrap_slot) {
                return Err(format!(
                    "completed archive {} has bootstrap slot {}, expected {}..={} for epoch {}",
                    path.display(),
                    provenance_v1.bootstrap_slot,
                    min_bootstrap_slot,
                    max_bootstrap_slot,
                    epoch
                ));
            }
            if provenance_v1.bootstrap_state_kind == BootstrapStateKind::CarriedBank
                && !selection.descriptor.bootstrap.permits_in_memory_handoff
            {
                return Err(format!(
                    "completed archive {} claims a carried-bank bootstrap for runtime {}",
                    path.display(),
                    identity.name
                ));
            }
        }
    }
    Ok(true)
}

/// Determines whether an assembled multi-runtime epoch is safe to reuse.
///
/// V3 provenance is checked against the live slot registry and the executable
/// bytes currently configured for every historical worker. A structurally
/// valid archive made with an older routing decision therefore cannot silently
/// bypass a newly tightened compatibility boundary.
fn epoch_archive_reusable_multi_runtime(
    path: &Path,
    epoch: u64,
    spans: &[compatibility::RuntimeSpan],
) -> Result<bool, String> {
    if spans.len() < 2 {
        return Err("multi-runtime archive validation requires at least two spans".to_string());
    }
    let file = match fs::File::open(path) {
        Ok(file) => file,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(err) => return Err(format!("failed to open {}: {err}", path.display())),
    };
    let reader =
        match jetstreamer_horizon::archive::ArchiveReader::open(std::io::BufReader::new(file)) {
            Ok(reader) => reader,
            Err(_) => return Ok(false),
        };
    let (slot_start, slot_end_inclusive) = epoch_to_slot_range(epoch);
    let slot_count = slot_end_inclusive - slot_start + 1;
    let header = reader.header();
    if header.epoch != epoch || header.slot_start != slot_start || header.slot_count != slot_count {
        return Err(format!(
            "assembled archive {} has header epoch={} slots={}+{}, expected epoch={} slots={}+{}",
            path.display(),
            header.epoch,
            header.slot_start,
            header.slot_count,
            epoch,
            slot_start,
            slot_count,
        ));
    }
    let provenance = reader
        .provenance()
        .map_err(|err| {
            format!(
                "assembled archive {} has invalid provenance: {err}",
                path.display()
            )
        })?
        .ok_or_else(|| {
            format!(
                "assembled archive {} has no runtime provenance",
                path.display()
            )
        })?;
    let ArchiveProvenance::V3(provenance) = provenance else {
        return Err(format!(
            "assembled archive {} has provenance V{}, expected V3",
            path.display(),
            provenance.version(),
        ));
    };
    if provenance.assembly_profile != archive_assembly_profile() {
        return Err(format!(
            "assembled archive {} uses assembly profile {:?}, expected {:?}",
            path.display(),
            provenance.assembly_profile,
            archive_assembly_profile(),
        ));
    }
    validate_multi_runtime_genesis(path, provenance.genesis_hash)?;
    if provenance.transaction_metadata != archive_transaction_metadata_policy(slot_start) {
        return Err(format!(
            "assembled archive {} has incompatible transaction metadata policy",
            path.display()
        ));
    }
    let first_selection = runtime_span_selection(
        spans
            .first()
            .expect("multi-runtime validation requires at least two spans"),
    )?;
    match provenance.bootstrap_state_kind {
        BootstrapStateKind::Genesis => {
            if epoch != 0 || provenance.bootstrap_state.slot != 0 {
                return Err(format!(
                    "assembled archive {} claims an invalid genesis bootstrap at slot {} for epoch {}",
                    path.display(),
                    provenance.bootstrap_state.slot,
                    epoch
                ));
            }
        }
        BootstrapStateKind::SnapshotArchive | BootstrapStateKind::CarriedBank => {
            if epoch == 0 {
                return Err(format!(
                    "assembled epoch-0 archive {} did not bootstrap from genesis",
                    path.display()
                ));
            }
            let min_bootstrap_slot = epoch_to_slot(epoch - 1);
            let max_bootstrap_slot = slot_start - 1;
            if !(min_bootstrap_slot..=max_bootstrap_slot).contains(&provenance.bootstrap_state.slot)
            {
                return Err(format!(
                    "assembled archive {} has bootstrap slot {}, expected {}..={} for epoch {}",
                    path.display(),
                    provenance.bootstrap_state.slot,
                    min_bootstrap_slot,
                    max_bootstrap_slot,
                    epoch
                ));
            }
            if provenance.bootstrap_state_kind == BootstrapStateKind::CarriedBank
                && !first_selection
                    .descriptor
                    .bootstrap
                    .permits_in_memory_handoff
            {
                return Err(format!(
                    "assembled archive {} claims a carried-bank bootstrap for runtime {}",
                    path.display(),
                    first_selection.descriptor.identity.name
                ));
            }
        }
    }
    let expected_commitment_kind = match first_selection.backend {
        compatibility::RuntimeBackend::SolanaV1_0_7
        | compatibility::RuntimeBackend::SolanaV1_0_8
        | compatibility::RuntimeBackend::SolanaV1_0_24 => StateCommitmentKind::LegacyAccountsHash,
        compatibility::RuntimeBackend::AgaveV3 => StateCommitmentKind::AccountsLtHash,
    };
    if provenance.bootstrap_state.kind != expected_commitment_kind {
        return Err(format!(
            "assembled archive {} has {:?} bootstrap commitment, expected {:?} for runtime {}",
            path.display(),
            provenance.bootstrap_state.kind,
            expected_commitment_kind,
            first_selection.descriptor.identity.name
        ));
    }
    if provenance.runtime_segments.len() != spans.len() {
        return Err(format!(
            "assembled archive {} records {} runtime spans, registry requires {}",
            path.display(),
            provenance.runtime_segments.len(),
            spans.len(),
        ));
    }
    if provenance.handoffs.len() != spans.len() - 1 {
        return Err(format!(
            "assembled archive {} records {} runtime handoffs, registry requires {}",
            path.display(),
            provenance.handoffs.len(),
            spans.len() - 1,
        ));
    }

    for (index, (recorded, expected_span)) in
        provenance.runtime_segments.iter().zip(spans).enumerate()
    {
        let selection = runtime_span_selection(expected_span)?;
        let identity = selection.descriptor.identity;
        let expected_count = expected_span.slots.end - expected_span.slots.start;
        let expected_worker_sha256 = if selection.descriptor.worker.is_some() {
            let executable = configured_historical_worker_executable(selection.descriptor)?;
            Some(
                historical::measure_executable_sha256(&executable).map_err(|err| {
                    format!(
                        "failed to measure configured historical worker {}: {err}",
                        executable.display()
                    )
                })?,
            )
        } else {
            None
        };
        if recorded.slot_start != expected_span.slots.start
            || recorded.slot_count != expected_count
            || recorded.generation_profile != archive_generation_profile()
            || recorded.runtime_profile != identity.name
            || recorded.runtime_admission != archive_runtime_admission(selection.admission)
            || recorded.runtime_revision != identity.revision
            || recorded.runtime_toolchain != archive_runtime_toolchain(identity)
            || recorded.worker_executable_sha256 != expected_worker_sha256
        {
            return Err(format!(
                "assembled archive {} runtime segment {index} does not match the active slot registry",
                path.display()
            ));
        }
    }

    for (index, (recorded, successor_span)) in
        provenance.handoffs.iter().zip(&spans[1..]).enumerate()
    {
        let expected = successor_span.handoff.ok_or_else(|| {
            format!(
                "runtime span {}..{} has no registered handoff",
                successor_span.slots.start, successor_span.slots.end
            )
        })?;
        let expected_accounts_hash = expected.snapshot.accounts_hash()?;
        if recorded.boundary_slot != expected.boundary_slot
            || recorded.predecessor.slot != expected.snapshot.slot
            || recorded.successor.slot != expected.snapshot.slot
            || recorded.predecessor.accounts_hash_kind != AccountsHashKind::LegacyAccountsHash
            || recorded.successor.accounts_hash_kind != AccountsHashKind::LegacyAccountsHash
            || recorded.predecessor.accounts_hash != expected_accounts_hash
            || recorded.successor.accounts_hash != expected_accounts_hash
            || recorded.successor_bootstrap_kind != BootstrapStateKind::SnapshotArchive
            || recorded.successor_bootstrap_write_count != 0
        {
            return Err(format!(
                "assembled archive {} handoff {index} does not match the canonical snapshot committed by the registry",
                path.display()
            ));
        }
    }
    // Opening the footer and validating provenance is not enough for resume:
    // bucket payloads may have been truncated or corrupted after publication.
    // Reuse is an admission decision, so pay the one full sequential decode.
    verify_assembled_runtime_archive(path, epoch, &provenance)?;
    Ok(true)
}

fn validate_multi_runtime_genesis(path: &Path, actual: Hash) -> Result<(), String> {
    let expected = compatibility::MAINNET_GENESIS_HASH
        .parse::<Hash>()
        .map_err(|err| format!("runtime registry has an invalid mainnet genesis hash: {err}"))?;
    if actual == expected {
        return Ok(());
    }
    Err(format!(
        "assembled archive {} has genesis {}, expected registered mainnet genesis {}",
        path.display(),
        actual,
        expected,
    ))
}

fn validate_epoch_bootstrap_snapshot(epoch: u64, path: &Path) -> Result<Slot, String> {
    let target_slot = epoch_to_slot(epoch).saturating_sub(1);
    let min_slot = epoch_to_slot(epoch.saturating_sub(1));
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| format!("snapshot path has no UTF-8 filename: {}", path.display()))?;
    let (slot, _) = parse_snapshot_archive_name(name)?;
    if slot < min_slot || slot > target_slot {
        return Err(format!(
            "epoch {epoch} requires a bootstrap snapshot in slots {min_slot}..={target_slot}, got slot {slot} ({})",
            path.display()
        ));
    }
    Ok(slot)
}

fn validate_runtime_bootstrap_archive(
    descriptor: &compatibility::RuntimeDescriptor,
    path: &Path,
) -> Result<(), String> {
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| format!("snapshot path has no UTF-8 filename: {}", path.display()))?;
    if descriptor.accepts_bootstrap_archive_name(name) {
        return Ok(());
    }
    Err(format!(
        "runtime profile {} accepts snapshot formats {:?}, but selected {}",
        descriptor.identity.name,
        descriptor.bootstrap.archive_extensions,
        path.display()
    ))
}

/// Locates epoch `epoch`'s boundary snapshot archive in `dest_dir`, downloading
/// it (gcloud/GCS) if absent. Only a candidate landing within the previous
/// epoch qualifies — an older archive would force a warmup replay across every
/// slot between it and the epoch boundary (see the `min_snapshot_slot`
/// rationale where `effective_start`'s snapshot is resolved).
async fn ensure_epoch_boundary_snapshot(
    epoch: u64,
    dest_dir: &Path,
    archive_extensions: &[&str],
) -> Result<PathBuf, String> {
    let target_slot = epoch_to_slot(epoch).saturating_sub(1);
    let min_snapshot_slot = epoch_to_slot(epoch.saturating_sub(1));
    if let Some(candidate) =
        find_existing_snapshot_archive(dest_dir, target_slot, archive_extensions)?
        && candidate.slot >= min_snapshot_slot
    {
        info!(
            "epoch {epoch}: boundary snapshot already present at {}",
            candidate.path.display()
        );
        return Ok(candidate.path);
    }
    info!("epoch {epoch}: downloading boundary snapshot (target slot {target_slot})");
    let path = download_snapshot_at_or_before_slot_matching(
        epoch,
        target_slot,
        dest_dir,
        archive_extensions,
    )
    .await
    .map_err(|err| format!("failed to download epoch {epoch} boundary snapshot: {err}"))?;
    validate_epoch_bootstrap_snapshot(epoch, &path)?;
    info!(
        "epoch {epoch}: downloaded boundary snapshot to {}",
        path.display()
    );
    Ok(path)
}

/// Reads a supervisor-written per-epoch hash file (one canonical snapshot
/// archive filename per line) back into the expectations map. The extension
/// preserves the hash scheme: legacy `.tar.bz2` names contain the historical
/// full accounts Merkle hash, while current snapshot names contain
/// `AccountsLtHash`.
fn read_epoch_hashes_file(
    path: &Path,
    archive_extensions: &[&str],
) -> Result<BTreeMap<Slot, BankHashExpectation>, String> {
    let contents = fs::read_to_string(path).map_err(|err| {
        format!(
            "failed to read snapshot hashes file {}: {err}",
            path.display()
        )
    })?;
    let mut expected = BTreeMap::new();
    for line in contents.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        if !archive_extensions
            .iter()
            .any(|extension| line.ends_with(extension))
        {
            return Err(format!(
                "snapshot entry {line:?} in {} is incompatible with the slot-selected runtime",
                path.display()
            ));
        }
        let (slot, hash) = parse_snapshot_archive_name(line)?;
        let expectation = if line.ends_with(".tar.bz2") {
            BankHashExpectation::LegacyAccountsHash(hash.0)
        } else {
            BankHashExpectation::AccountsLtHash(hash)
        };
        if expected.insert(slot, expectation).is_some() {
            return Err(format!(
                "duplicate snapshot entry for slot {slot} in {}",
                path.display()
            ));
        }
    }
    Ok(expected)
}

fn write_epoch_hashes_file(
    path: &Path,
    expected: &BTreeMap<Slot, BankHashExpectation>,
) -> Result<(), String> {
    let mut contents = String::new();
    for (slot, hash) in expected {
        match hash {
            BankHashExpectation::AccountsLtHash(hash) => {
                contents.push_str(&format!("snapshot-{slot}-{}.tar.zst\n", hash.0));
            }
            BankHashExpectation::LegacyAccountsHash(hash) => {
                contents.push_str(&format!("snapshot-{slot}-{hash}.tar.bz2\n"));
            }
        }
    }
    fs::write(path, contents).map_err(|err| format!("failed to write {}: {err}", path.display()))
}

fn snapshot_path_expectation(path: &Path) -> Result<(Slot, BankHashExpectation), String> {
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| format!("snapshot path has no UTF-8 filename: {}", path.display()))?;
    let (slot, hash) = parse_snapshot_archive_name(name)?;
    let expectation = if name.ends_with(".tar.bz2") {
        BankHashExpectation::LegacyAccountsHash(hash.0)
    } else {
        BankHashExpectation::AccountsLtHash(hash)
    };
    Ok((slot, expectation))
}

/// Adds registry-committed handoff snapshots to a canonical checkpoint set.
/// A storage listing may omit an archive that we can deterministically export
/// from the predecessor runtime; the registry hash remains the trust anchor.
fn add_runtime_handoff_expectations(
    range: std::ops::Range<Slot>,
    expected: &mut BTreeMap<Slot, BankHashExpectation>,
) -> Result<(), String> {
    for handoff in compatibility::RUNTIME_HANDOFFS {
        if !range.contains(&handoff.snapshot.slot) {
            continue;
        }
        let hash = handoff.snapshot.accounts_hash()?;
        let value = match handoff.snapshot.archive_extension {
            ".tar.bz2" => BankHashExpectation::LegacyAccountsHash(hash),
            ".tar.zst" | ".tar.lz4" => BankHashExpectation::AccountsLtHash(SnapshotHash(hash)),
            extension => {
                return Err(format!(
                    "runtime handoff at slot {} uses unsupported snapshot extension {extension:?}",
                    handoff.boundary_slot
                ));
            }
        };
        match expected.get(&handoff.snapshot.slot) {
            Some(existing) if *existing != value => {
                return Err(format!(
                    "canonical snapshot listing conflicts with the runtime registry at handoff slot {}: listing={existing:?}, registry={value:?}",
                    handoff.snapshot.slot
                ));
            }
            Some(_) => {}
            None => {
                info!(
                    "adding registry-committed handoff checkpoint {}",
                    handoff.snapshot.archive_name()
                );
                expected.insert(handoff.snapshot.slot, value);
            }
        }
    }
    Ok(())
}

/// Restricts a canonical epoch checkpoint list to one focused qualification
/// span. The bootstrap filename must itself be present in the trusted list,
/// and at least one independently generated checkpoint must follow it.
fn qualification_expectations(
    mut expected: BTreeMap<Slot, BankHashExpectation>,
    plan: QualificationPlan,
    snapshot_archive: &Path,
) -> Result<BTreeMap<Slot, BankHashExpectation>, String> {
    let (snapshot_slot, snapshot_expectation) = snapshot_path_expectation(snapshot_archive)?;
    if snapshot_slot != plan.bootstrap_slot {
        return Err(format!(
            "qualification snapshot changed after CLI validation: expected slot {}, got {snapshot_slot}",
            plan.bootstrap_slot
        ));
    }
    match expected.get(&snapshot_slot) {
        Some(expected_hash) if *expected_hash == snapshot_expectation => {}
        Some(expected_hash) => {
            return Err(format!(
                "qualification snapshot {} does not match the canonical checkpoint entry at slot {snapshot_slot}: file={snapshot_expectation:?}, checkpoint={expected_hash:?}",
                snapshot_archive.display()
            ));
        }
        None => {
            return Err(format!(
                "qualification snapshot slot {snapshot_slot} is absent from the supplied checkpoint file"
            ));
        }
    }

    expected.retain(|slot, _| (plan.bootstrap_slot..=plan.end_inclusive).contains(slot));
    let post_bootstrap = expected
        .range(plan.replay_start..=plan.end_inclusive)
        .count();
    if post_bootstrap == 0 {
        return Err(format!(
            "qualification requires a canonical checkpoint in replay range {}..={}; none was supplied",
            plan.replay_start, plan.end_inclusive
        ));
    }
    Ok(expected)
}

fn runtime_segment_work_dir(final_output: &Path) -> Result<PathBuf, String> {
    let name = final_output.file_name().ok_or_else(|| {
        format!(
            "multi-runtime output path has no filename: {}",
            final_output.display()
        )
    })?;
    let mut directory_name = OsString::from(".");
    directory_name.push(name);
    directory_name.push(".runtime-segments");
    Ok(final_output.with_file_name(directory_name))
}

fn runtime_segment_archive_path(
    work_dir: &Path,
    epoch: u64,
    index: usize,
    span: &compatibility::RuntimeSpan,
) -> PathBuf {
    work_dir.join(format!(
        "epoch-{epoch}.segment-{index:02}-{}-{}.jet",
        span.slots.start, span.slots.end
    ))
}

/// Removes private segment sources after complete pairs have been re-admitted
/// against the active registry. The only accepted partial state is an archive
/// whose sidecar was already removed by an interrupted invocation of this
/// function. Unknown entries, links, and sidecar-only pairs fail closed, so a
/// retry never broadens deletion beyond the exact files the supervisor named.
fn cleanup_runtime_segment_work_dir(
    work_dir: &Path,
    epoch: u64,
    spans: &[compatibility::RuntimeSpan],
) -> Result<(), String> {
    if env_truthy_default("JETSTREAMER_RETAIN_RUNTIME_SEGMENTS", false) {
        info!(
            "retaining verified runtime segment artifacts in {}",
            work_dir.display()
        );
        return Ok(());
    }

    let work_metadata = match fs::symlink_metadata(work_dir) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => {
            return Err(format!(
                "failed to inspect runtime segment work directory {}: {error}",
                work_dir.display()
            ));
        }
    };
    if !work_metadata.file_type().is_dir() {
        return Err(format!(
            "refusing to clean non-directory runtime segment work path {}",
            work_dir.display()
        ));
    }

    let mut expected = HashSet::with_capacity(spans.len() * 2);
    let mut expected_pairs = Vec::with_capacity(spans.len());
    for (index, span) in spans.iter().enumerate() {
        let archive = runtime_segment_archive_path(work_dir, epoch, index, span);
        let sidecar = jetstreamer_node::segment_manifest::segment_manifest_path(&archive).map_err(
            |error| {
                format!(
                    "failed to resolve runtime segment sidecar for {}: {error}",
                    archive.display()
                )
            },
        )?;
        expected.insert(archive.clone());
        expected.insert(sidecar.clone());
        expected_pairs.push((span, archive, sidecar));
    }

    let mut actual = HashSet::with_capacity(expected.len());
    for entry in fs::read_dir(work_dir)
        .map_err(|error| format!("failed to read {}: {error}", work_dir.display()))?
    {
        let entry = entry.map_err(|error| {
            format!(
                "failed to read entry in runtime segment directory {}: {error}",
                work_dir.display()
            )
        })?;
        let path = entry.path();
        let file_type = entry.file_type().map_err(|error| {
            format!(
                "failed to inspect runtime segment artifact {}: {error}",
                path.display()
            )
        })?;
        if !file_type.is_file() || !expected.contains(&path) {
            return Err(format!(
                "refusing to clean runtime segment directory {} with unexpected entry {}",
                work_dir.display(),
                path.display()
            ));
        }
        actual.insert(path);
    }

    for (span, archive, sidecar) in &expected_pairs {
        let archive_present = actual.contains(archive);
        let sidecar_present = actual.contains(sidecar);
        match (archive_present, sidecar_present) {
            (true, true) => {
                load_validated_runtime_segment(archive, epoch, span).map_err(|error| {
                    format!(
                        "refusing to clean unvalidated runtime segment {}: {error}",
                        archive.display()
                    )
                })?;
            }
            (true, false) | (false, false) => {
                // `cleanup_runtime_segment_work_dir` always removes a sidecar
                // before its archive. These are its two durable partial
                // states, and both contain only exact regular paths admitted
                // by the directory scan above.
            }
            (false, true) => {
                return Err(format!(
                    "refusing to clean runtime segment directory {} with sidecar {} but no archive; this is not a valid cleanup state",
                    work_dir.display(),
                    sidecar.display()
                ));
            }
        }
    }

    // Removing the sidecar first makes an interrupted cleanup archive
    // ineligible for reuse. On retry the archive-only state above is safe to
    // finish because it is an exact expected regular file in the private dir.
    for (_, archive, sidecar) in expected_pairs {
        if actual.contains(&sidecar) {
            fs::remove_file(&sidecar)
                .map_err(|error| format!("failed to remove {}: {error}", sidecar.display()))?;
        }
        if actual.contains(&archive) {
            fs::remove_file(&archive)
                .map_err(|error| format!("failed to remove {}: {error}", archive.display()))?;
        }
    }
    fs::File::open(work_dir)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| format!("failed to sync {}: {error}", work_dir.display()))?;
    fs::remove_dir(work_dir)
        .map_err(|error| format!("failed to remove {}: {error}", work_dir.display()))?;
    let parent = work_dir.parent().unwrap_or_else(|| Path::new("."));
    fs::File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| format!("failed to sync {}: {error}", parent.display()))?;
    info!(
        "removed verified private runtime segment artifacts from {}",
        work_dir.display()
    );
    Ok(())
}

struct HandoffSnapshotExpectation {
    source_runtime: SegmentRuntimeIdentity,
    source_worker_executable_sha256: [u8; 32],
}

fn handoff_snapshot_expectation(
    handoff: &compatibility::RuntimeHandoff,
) -> Result<HandoffSnapshotExpectation, String> {
    let selection =
        compatibility::select_runtime(handoff.snapshot.slot..handoff.boundary_slot, true)?;
    if !std::ptr::eq(selection.descriptor, handoff.source) {
        return Err(format!(
            "runtime registry selects {} at handoff slot {}, expected source {}",
            selection.descriptor.identity.name, handoff.snapshot.slot, handoff.source.identity.name
        ));
    }
    let executable = configured_historical_worker_executable(selection.descriptor)?;
    let source_worker_executable_sha256 = historical::measure_executable_sha256(&executable)
        .map_err(|error| {
            format!(
                "failed to measure configured handoff source worker {}: {error}",
                executable.display()
            )
        })?;
    Ok(HandoffSnapshotExpectation {
        source_runtime: segment_runtime_identity(selection)?,
        source_worker_executable_sha256,
    })
}

fn handoff_snapshot_matches_predecessor(
    manifest: &HistoricalHandoffSnapshotManifest,
    predecessor: &ValidatedRuntimeSegment,
) -> bool {
    manifest.source_runtime == predecessor.manifest.runtime
        && manifest.source_worker_executable_sha256 == predecessor.manifest.worker_executable_sha256
        && manifest.terminal == predecessor.manifest.terminal
}

fn validate_canonical_handoff_snapshot_with_expectation(
    path: &Path,
    handoff: &compatibility::RuntimeHandoff,
    expectation: &HandoffSnapshotExpectation,
) -> Result<HistoricalHandoffSnapshotManifest, String> {
    let expected_name = handoff.snapshot.archive_name();
    if path.file_name().and_then(|name| name.to_str()) != Some(expected_name.as_str()) {
        return Err(format!(
            "handoff snapshot path {} does not name registry object {expected_name}",
            path.display()
        ));
    }
    let (slot, hash) = parse_snapshot_archive_name(&expected_name)?;
    if slot != handoff.snapshot.slot || hash.0 != handoff.snapshot.accounts_hash()? {
        return Err(format!(
            "handoff snapshot identity does not match the runtime registry: {expected_name}"
        ));
    }
    let manifest = read_and_validate_handoff_snapshot_manifest(path).map_err(|error| {
        format!(
            "handoff snapshot pair {} failed durable validation: {error}",
            path.display()
        )
    })?;
    if manifest.boundary_slot != handoff.boundary_slot
        || manifest.snapshot_slot != handoff.snapshot.slot
        || manifest.accounts_hash != handoff.snapshot.accounts_hash_base58
        || manifest.source_runtime != expectation.source_runtime
        || manifest.source_worker_executable_sha256 != expectation.source_worker_executable_sha256
        || manifest.terminal.slot != handoff.snapshot.slot
        || manifest.terminal.accounts_hash != handoff.snapshot.accounts_hash_base58
        || !manifest.terminal.slot_complete
    {
        return Err(format!(
            "handoff snapshot evidence {} does not match the active runtime registry, source worker, or canonical checkpoint",
            handoff_snapshot_manifest_path(path)
                .map_err(|error| error.to_string())?
                .display()
        ));
    }
    Ok(manifest)
}

fn validate_canonical_handoff_snapshot(
    path: &Path,
    handoff: &compatibility::RuntimeHandoff,
) -> Result<HistoricalHandoffSnapshotManifest, String> {
    let expectation = handoff_snapshot_expectation(handoff)?;
    validate_canonical_handoff_snapshot_with_expectation(path, handoff, &expectation)
}

fn registered_handoff_bootstrap(
    descriptor: &compatibility::RuntimeDescriptor,
    path: &Path,
) -> Result<Option<HistoricalHandoffSnapshotManifest>, String> {
    let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
        return Err(format!(
            "snapshot path has no UTF-8 filename: {}",
            path.display()
        ));
    };
    let Some(handoff) = compatibility::RUNTIME_HANDOFFS
        .iter()
        .copied()
        .find(|handoff| {
            std::ptr::eq(handoff.destination, descriptor) && handoff.snapshot.archive_name() == name
        })
    else {
        return Ok(None);
    };
    validate_canonical_handoff_snapshot(path, handoff).map(Some)
}

fn path_exists_without_following(path: &Path) -> Result<bool, String> {
    match fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(format!("failed to inspect {}: {error}", path.display())),
    }
}

/// Moves every present member of a handoff archive/evidence pair to unique,
/// recoverable names without following either path.
fn preserve_handoff_snapshot_pair(path: &Path) -> Result<Vec<(PathBuf, PathBuf)>, String> {
    let sidecar = handoff_snapshot_manifest_path(path).map_err(|error| error.to_string())?;
    let archive_present = path_exists_without_following(path)?;
    let sidecar_present = path_exists_without_following(&sidecar)?;
    if !archive_present && !sidecar_present {
        return Ok(Vec::new());
    }
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let archive_name = path
        .file_name()
        .ok_or_else(|| format!("handoff snapshot has no filename: {}", path.display()))?;
    for sequence in 0..100u32 {
        let mut quarantine_name = archive_name.to_os_string();
        quarantine_name.push(format!(
            ".replaced-{}-{timestamp}-{sequence}",
            std::process::id()
        ));
        let quarantine = parent.join(quarantine_name);
        match fs::create_dir(&quarantine) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => {
                return Err(format!(
                    "failed to create handoff quarantine {}: {error}",
                    quarantine.display()
                ));
            }
        }
        let archive_backup = quarantine.join(archive_name);
        let sidecar_name = sidecar
            .file_name()
            .ok_or_else(|| format!("handoff evidence has no filename: {}", sidecar.display()))?;
        let sidecar_backup = quarantine.join(sidecar_name);
        let mut moved = Vec::with_capacity(2);
        // Move the evidence first. If moving the archive then fails, put the
        // evidence back so the canonical namespace does not retain a split
        // pair because of a normal filesystem error.
        if sidecar_present {
            fs::rename(&sidecar, &sidecar_backup).map_err(|error| {
                let _ = fs::remove_dir(&quarantine);
                format!(
                    "failed to preserve handoff evidence {} as {}: {error}",
                    sidecar.display(),
                    sidecar_backup.display()
                )
            })?;
            moved.push((sidecar.clone(), sidecar_backup.clone()));
        }
        if archive_present && let Err(error) = fs::rename(path, &archive_backup) {
            let rollback = if sidecar_present {
                fs::rename(&sidecar_backup, &sidecar).err()
            } else {
                None
            };
            let _ = fs::remove_dir(&quarantine);
            return Err(match rollback {
                Some(rollback) => format!(
                    "failed to preserve handoff snapshot {} as {}: {error}; also failed to restore evidence {}: {rollback}",
                    path.display(),
                    archive_backup.display(),
                    sidecar.display()
                ),
                None => format!(
                    "failed to preserve handoff snapshot {} as {}: {error}",
                    path.display(),
                    archive_backup.display()
                ),
            });
        }
        if archive_present {
            moved.push((path.to_path_buf(), archive_backup));
        }
        fs::File::open(&quarantine)
            .and_then(|directory| directory.sync_all())
            .map_err(|error| {
                format!(
                    "failed to sync handoff quarantine {}: {error}",
                    quarantine.display()
                )
            })?;
        fs::File::open(parent)
            .and_then(|directory| directory.sync_all())
            .map_err(|error| format!("failed to sync {}: {error}", parent.display()))?;
        return Ok(moved);
    }
    Err(format!(
        "could not choose a unique backup name for handoff pair {}",
        path.display()
    ))
}

fn remove_quarantined_handoff_snapshot_pair(moved: Vec<(PathBuf, PathBuf)>) -> Result<(), String> {
    if moved.len() != 2 {
        return Err(format!(
            "handoff pair quarantine moved {} artifacts, expected exactly two",
            moved.len()
        ));
    }
    let quarantine = moved[0]
        .1
        .parent()
        .ok_or_else(|| "quarantined handoff artifact has no parent directory".to_string())?
        .to_path_buf();
    if moved
        .iter()
        .any(|(_, backup)| backup.parent() != Some(quarantine.as_path()))
    {
        return Err("quarantined handoff artifacts do not share one directory".to_string());
    }
    for (_, backup) in moved {
        fs::remove_file(&backup)
            .map_err(|error| format!("failed to prune {}: {error}", backup.display()))?;
    }
    fs::remove_dir(&quarantine)
        .map_err(|error| format!("failed to remove {}: {error}", quarantine.display()))?;
    Ok(())
}

/// Prunes one completed range child's bootstrap artifact without splitting a
/// generated handoff archive from its durable evidence. A registered handoff
/// pair is validated, quarantined as a unit, and only then removed; an unknown
/// companion fails closed.
fn prune_epoch_boundary_snapshot(
    path: &Path,
    descriptor: &compatibility::RuntimeDescriptor,
) -> Result<(), String> {
    let metadata = fs::symlink_metadata(path).map_err(|error| {
        format!(
            "failed to inspect boundary snapshot {}: {error}",
            path.display()
        )
    })?;
    if !metadata.file_type().is_file() {
        return Err(format!(
            "refusing to prune non-regular boundary snapshot {}",
            path.display()
        ));
    }
    let sidecar = handoff_snapshot_manifest_path(path).map_err(|error| error.to_string())?;
    if path_exists_without_following(&sidecar)? {
        if registered_handoff_bootstrap(descriptor, path)?.is_none() {
            return Err(format!(
                "refusing to prune boundary snapshot {} with an unregistered handoff companion {}",
                path.display(),
                sidecar.display()
            ));
        }
        let moved = preserve_handoff_snapshot_pair(path)?;
        remove_quarantined_handoff_snapshot_pair(moved)?;
    } else {
        fs::remove_file(path)
            .map_err(|error| format!("failed to prune {}: {error}", path.display()))?;
    }
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    fs::File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| format!("failed to sync {}: {error}", parent.display()))?;
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum HandoffSnapshotReadiness {
    Missing,
    Invalid,
    Ready,
}

fn handoff_snapshot_needs_export(readiness: HandoffSnapshotReadiness) -> bool {
    readiness != HandoffSnapshotReadiness::Ready
}

/// Re-observes the handoff immediately before a segment attempt. A ready pair
/// must be bound to the exact validated predecessor segment; registry-valid but
/// stale evidence is quarantined so replay regenerates it.
fn prepare_handoff_snapshot_export(
    successor_snapshot: Option<&(&compatibility::RuntimeHandoff, PathBuf)>,
    predecessor: Option<&ValidatedRuntimeSegment>,
) -> Result<bool, String> {
    let Some((handoff, path)) = successor_snapshot else {
        return Ok(false);
    };
    // Resolve configuration before classifying existing bytes. A missing or
    // changed worker executable is a fatal setup error, not a reason to
    // quarantine otherwise valid artifacts.
    let expectation = handoff_snapshot_expectation(handoff)?;
    let sidecar = handoff_snapshot_manifest_path(path).map_err(|error| error.to_string())?;
    let archive_present = path_exists_without_following(path)?;
    let sidecar_present = path_exists_without_following(&sidecar)?;
    let readiness = match (archive_present, sidecar_present) {
        (false, false) => HandoffSnapshotReadiness::Missing,
        (true, true) => {
            match validate_canonical_handoff_snapshot_with_expectation(path, handoff, &expectation)
            {
                Ok(manifest)
                    if predecessor.is_some_and(|segment| {
                        handoff_snapshot_matches_predecessor(&manifest, segment)
                    }) =>
                {
                    HandoffSnapshotReadiness::Ready
                }
                Ok(_) => {
                    let moved = preserve_handoff_snapshot_pair(path)?;
                    warn!(
                        "preserved stale handoff snapshot pair {} as {:?}: it is not bound to the exact validated predecessor segment",
                        path.display(),
                        moved
                    );
                    HandoffSnapshotReadiness::Invalid
                }
                Err(validation_error) => {
                    let moved = preserve_handoff_snapshot_pair(path)?;
                    warn!(
                        "preserved invalid handoff snapshot pair {} as {:?}: {validation_error}",
                        path.display(),
                        moved
                    );
                    HandoffSnapshotReadiness::Invalid
                }
            }
        }
        state => {
            let moved = preserve_handoff_snapshot_pair(path)?;
            warn!(
                "preserved incomplete handoff snapshot pair {} as {:?} (archive/sidecar presence: {state:?})",
                path.display(),
                moved
            );
            HandoffSnapshotReadiness::Invalid
        }
    };
    Ok(handoff_snapshot_needs_export(readiness))
}

fn require_regular_file_or_absent(path: &Path, description: &str) -> Result<(), String> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_file() => Ok(()),
        Ok(_) => Err(format!(
            "{description} path is neither absent nor a regular file: {}",
            path.display()
        )),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(format!(
            "failed to inspect {description} path {}: {error}",
            path.display()
        )),
    }
}

fn verify_assembled_runtime_archive(
    path: &Path,
    epoch: u64,
    provenance: &ArchiveProvenanceV3,
) -> Result<(), String> {
    let (epoch_start, epoch_end) = epoch_to_slot_range(epoch);
    verify_assembled_runtime_archive_range(
        path,
        epoch,
        epoch_start,
        epoch_end - epoch_start + 1,
        provenance,
    )
}

fn verify_assembled_runtime_archive_range(
    path: &Path,
    epoch: u64,
    expected_start: Slot,
    expected_count: u64,
    provenance: &ArchiveProvenanceV3,
) -> Result<(), String> {
    let file = fs::File::open(path)
        .map_err(|err| format!("failed to open assembled archive {}: {err}", path.display()))?;
    let mut reader =
        jetstreamer_horizon::archive::ArchiveReader::open(std::io::BufReader::new(file))
            .map_err(|err| format!("failed to open assembled archive {}: {err}", path.display()))?;
    let header = reader.header().clone();
    if header.epoch != epoch
        || header.slot_start != expected_start
        || header.slot_count != expected_count
    {
        return Err(format!(
            "assembled archive {} has unexpected header epoch={} slots={}+{}",
            path.display(),
            header.epoch,
            header.slot_start,
            header.slot_count
        ));
    }
    let recorded = reader
        .provenance()
        .map_err(|err| {
            format!(
                "assembled archive {} has invalid provenance: {err}",
                path.display()
            )
        })?
        .ok_or_else(|| format!("assembled archive {} has no provenance", path.display()))?;
    if recorded != ArchiveProvenance::V3(provenance.clone()) {
        return Err(format!(
            "assembled archive {} provenance changed during publication",
            path.display()
        ));
    }
    reader.verify_chain = true;
    let mut digest = SemanticDigest::new();
    let mut visited = 0u64;
    for bucket in 0..reader.bucket_count() {
        visited = visited
            .checked_add(reader.read_bucket(bucket, &mut digest).map_err(|err| {
                format!(
                    "assembled archive {} failed full decode: {err}",
                    path.display()
                )
            })?)
            .ok_or_else(|| "assembled archive decoded slot count overflow".to_string())?;
    }
    digest
        .finish()
        .map_err(|err| format!("assembled archive semantic verification failed: {err}"))?;
    if visited != expected_count {
        return Err(format!(
            "assembled archive {} decoded {visited} slots, expected {expected_count}",
            path.display()
        ));
    }
    Ok(())
}

fn preserve_existing_output(path: &Path) -> Result<Option<PathBuf>, String> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(format!("failed to inspect {}: {err}", path.display())),
    };
    if !metadata.file_type().is_file() {
        return Err(format!(
            "refusing to replace non-regular output path {}",
            path.display()
        ));
    }
    let name = path
        .file_name()
        .ok_or_else(|| format!("output path has no filename: {}", path.display()))?;
    for sequence in 0..100u32 {
        let mut backup_name = name.to_os_string();
        backup_name.push(format!(
            ".replaced-{}-{}-{sequence}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs()
        ));
        let backup = path.with_file_name(backup_name);
        if backup.exists() {
            continue;
        }
        fs::rename(path, &backup).map_err(|err| {
            format!(
                "failed to preserve existing output {} as {}: {err}",
                path.display(),
                backup.display()
            )
        })?;
        if let Ok(sidecar) = jetstreamer_node::segment_manifest::segment_manifest_path(path)
            && sidecar.exists()
            && let Some(backup_name) = backup.file_name()
        {
            let mut backup_sidecar_name = backup_name.to_os_string();
            backup_sidecar_name.push(jetstreamer_node::segment_manifest::SEGMENT_MANIFEST_SUFFIX);
            let backup_sidecar = backup.with_file_name(backup_sidecar_name);
            fs::rename(&sidecar, &backup_sidecar).map_err(|err| {
                format!(
                    "preserved {} but failed to preserve its sidecar {}: {err}",
                    path.display(),
                    sidecar.display()
                )
            })?;
        }
        return Ok(Some(backup));
    }
    Err(format!(
        "could not choose a collision-free backup name for {}",
        path.display()
    ))
}

/// Runs and assembles every runtime span inside one epoch. Segment archives
/// and manifests remain in a private adjacent directory until the complete V3
/// output is verified and atomically published, so interruption is resumable.
#[allow(clippy::too_many_arguments)]
async fn run_multi_runtime_epoch_supervisor(
    epoch: u64,
    dest_dir: &Path,
    initial_snapshot: &Path,
    epoch_hashes: &Path,
    final_output: &Path,
    allow_candidate_runtime: bool,
    shutdown: Arc<AtomicBool>,
) -> Result<(), String> {
    let (epoch_start, epoch_end) = epoch_to_slot_range(epoch);
    let spans = compatibility::plan_runtime_spans(
        epoch_start..epoch_end.saturating_add(1),
        allow_candidate_runtime,
    )?;
    if spans.len() < 2 {
        return Err(format!(
            "epoch {epoch} does not cross a registered runtime boundary"
        ));
    }
    let work_dir = runtime_segment_work_dir(final_output)?;
    match epoch_archive_reusable_multi_runtime(final_output, epoch, &spans) {
        Ok(true) => {
            cleanup_runtime_segment_work_dir(&work_dir, epoch, &spans)?;
            info!(
                "multi-runtime epoch {epoch} archive {} is already complete",
                final_output.display()
            );
            return Ok(());
        }
        Ok(false) => {}
        Err(err) => warn!(
            "existing output {} is not reusable and will be preserved after replacement: {err}",
            final_output.display()
        ),
    }
    if !epoch_hashes.is_file() {
        return Err(format!(
            "multi-runtime epoch {epoch} requires a local checkpoint file: {}",
            epoch_hashes.display()
        ));
    }
    fs::create_dir_all(&work_dir)
        .map_err(|err| format!("failed to create {}: {err}", work_dir.display()))?;
    let work_metadata = fs::symlink_metadata(&work_dir)
        .map_err(|err| format!("failed to inspect {}: {err}", work_dir.display()))?;
    if !work_metadata.file_type().is_dir() {
        return Err(format!(
            "runtime segment work path is not a real directory: {}",
            work_dir.display()
        ));
    }
    let exe =
        env::current_exe().map_err(|err| format!("failed to resolve current executable: {err}"))?;
    let attempts = env::var("JETSTREAMER_SEGMENT_ATTEMPTS")
        .ok()
        .and_then(|value| value.trim().parse::<u32>().ok())
        .filter(|attempts| *attempts > 0)
        .unwrap_or(2);
    let mut regenerated_handoffs = HashSet::new();
    let source_paths = 'segment_pass: loop {
        let mut source_paths = Vec::with_capacity(spans.len());
        for (index, span) in spans.iter().enumerate() {
            if shutdown.load(Ordering::SeqCst) {
                return Ok(());
            }
            let selection = runtime_span_selection(span)?;
            let bootstrap = if index == 0 {
                initial_snapshot.to_path_buf()
            } else {
                let handoff = span.handoff.ok_or_else(|| {
                    format!(
                        "runtime span {}..{} has no bootstrap handoff",
                        span.slots.start, span.slots.end
                    )
                })?;
                dest_dir.join(handoff.snapshot.archive_name())
            };
            let bootstrap_handoff_manifest = if index > 0 {
                Some(validate_canonical_handoff_snapshot(
                    &bootstrap,
                    span.handoff.expect("checked above"),
                )?)
            } else {
                None
            };
            validate_runtime_bootstrap_archive(selection.descriptor, &bootstrap)?;

            let archive_path = runtime_segment_archive_path(&work_dir, epoch, index, span);
            require_regular_file_or_absent(&archive_path, "runtime segment archive")?;
            let successor_snapshot = spans.get(index + 1).and_then(|successor| {
                successor
                    .handoff
                    .map(|handoff| (handoff, dest_dir.join(handoff.snapshot.archive_name())))
            });
            let validated_segment = load_validated_runtime_segment(&archive_path, epoch, span);
            let mut reusable = validated_segment.is_ok();
            if let (Ok(segment), Some(handoff_manifest)) =
                (&validated_segment, &bootstrap_handoff_manifest)
                && segment.manifest.bootstrap_archive_sha256
                    != Some(handoff_manifest.archive_sha256)
            {
                warn!(
                    "epoch {epoch}: runtime segment {index} was produced from a different handoff snapshot and will be replayed"
                );
                reusable = false;
            }
            let successor_snapshot_needs_export = prepare_handoff_snapshot_export(
                successor_snapshot.as_ref(),
                reusable.then(|| {
                    validated_segment
                        .as_ref()
                        .expect("reusable segment was validated")
                }),
            )?;
            if successor_snapshot_needs_export {
                // A completed segment is insufficient to recreate runtime state;
                // replay it once more when its successor snapshot is missing.
                reusable = false;
            }
            if reusable {
                info!(
                    "epoch {epoch}: runtime segment {index} {}..{} already verified",
                    span.slots.start, span.slots.end
                );
                source_paths.push(archive_path);
                continue;
            }

            let mut completed = false;
            for attempt in 1..=attempts {
                if shutdown.load(Ordering::SeqCst) {
                    return Ok(());
                }
                // This attempt will replace the predecessor segment. Never
                // carry an outgoing handoff bound to the previous bytes across
                // that replacement; regenerate both pieces in the same run.
                let needs_export_now =
                    prepare_handoff_snapshot_export(successor_snapshot.as_ref(), None)?;
                info!(
                    "epoch {epoch}: spawning runtime segment {index} {}..{} with {} (attempt {attempt}/{attempts})",
                    span.slots.start, span.slots.end, selection.descriptor.identity.name,
                );
                let mut command = Command::new(&exe);
                command
                    .env_remove("JETSTREAMER_EXPORT_HANDOFF_SNAPSHOT_DIR")
                    .arg(epoch.to_string())
                    .arg(dest_dir)
                    .arg("--verify")
                    .arg(format!("--qualification-end-slot={}", span.slots.end - 1));
                let mut hashes_arg = OsString::from("--epoch-hashes=");
                hashes_arg.push(epoch_hashes);
                command.arg(hashes_arg);
                let mut snapshot_arg = OsString::from("--snapshot-archive=");
                snapshot_arg.push(&bootstrap);
                command.arg(snapshot_arg);
                let mut output_arg = OsString::from("--horizon-output=");
                output_arg.push(&archive_path);
                command.arg(output_arg);
                if needs_export_now {
                    command.env("JETSTREAMER_EXPORT_HANDOFF_SNAPSHOT_DIR", dest_dir);
                }
                let status = command.status().await.map_err(|err| {
                    format!(
                        "failed to spawn runtime segment child {}..{}: {err}",
                        span.slots.start, span.slots.end
                    )
                })?;
                if shutdown.load(Ordering::SeqCst) {
                    return Ok(());
                }
                if status.success() {
                    match load_validated_runtime_segment(&archive_path, epoch, span) {
                        Ok(segment) => {
                            if let Some((handoff, path)) = successor_snapshot.as_ref() {
                                let manifest = validate_canonical_handoff_snapshot(path, handoff)?;
                                if !handoff_snapshot_matches_predecessor(&manifest, &segment) {
                                    let moved = preserve_handoff_snapshot_pair(path)?;
                                    warn!(
                                        "epoch {epoch}: runtime segment {index} produced handoff evidence that does not match its exact terminal checkpoint; preserved {:?} and retrying",
                                        moved
                                    );
                                    continue;
                                }
                            }
                            completed = true;
                            break;
                        }
                        Err(err) => warn!(
                            "epoch {epoch}: segment child exited successfully but evidence validation failed: {err}"
                        ),
                    }
                } else {
                    warn!("epoch {epoch}: runtime segment {index} child failed with {status}");
                }
            }
            if !completed {
                if let Some(handoff) = span.handoff
                    && regenerated_handoffs.insert(handoff.boundary_slot)
                {
                    let quarantined = preserve_handoff_snapshot_pair(&bootstrap)?;
                    if quarantined.is_empty() {
                        warn!(
                            "epoch {epoch}: handoff snapshot {} disappeared after successor runtime failure; regenerating it once from {}",
                            bootstrap.display(),
                            handoff.source.identity.name,
                        );
                    } else {
                        warn!(
                            "epoch {epoch}: quarantined handoff snapshot pair {} as {:?} after successor runtime failed to load/replay; regenerating it once from {}",
                            bootstrap.display(),
                            quarantined,
                            handoff.source.identity.name,
                        );
                    }
                    continue 'segment_pass;
                }
                return Err(format!(
                    "epoch {epoch} runtime segment {index} failed after {attempts} attempt(s)"
                ));
            }
            source_paths.push(archive_path);
        }
        break source_paths;
    };

    let segments = source_paths
        .iter()
        .zip(&spans)
        .map(|(path, span)| load_validated_runtime_segment(path, epoch, span))
        .collect::<Result<Vec<_>, _>>()?;
    let handoff_manifests = spans[1..]
        .iter()
        .map(|span| {
            let handoff = span.handoff.ok_or_else(|| {
                format!(
                    "runtime span {}..{} has no registered handoff",
                    span.slots.start, span.slots.end
                )
            })?;
            validate_canonical_handoff_snapshot(
                &dest_dir.join(handoff.snapshot.archive_name()),
                handoff,
            )
        })
        .collect::<Result<Vec<_>, String>>()?;
    let provenance = build_multi_runtime_provenance(epoch, &spans, &segments, &handoff_manifests)?;
    let output_parent = final_output.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(output_parent)
        .map_err(|err| format!("failed to create {}: {err}", output_parent.display()))?;
    let temporary = tempfile::Builder::new()
        .prefix(".jetstreamer-runtime-assembly-")
        .suffix(".partial")
        .tempfile_in(output_parent)
        .map_err(|err| format!("failed to create assembly tempfile: {err}"))?;
    let sink = temporary
        .reopen()
        .map_err(|err| format!("failed to open assembly tempfile: {err}"))?;
    let sources = segments
        .iter()
        .map(|segment| {
            let terminal_last_blockhash = segment
                .manifest
                .terminal
                .last_blockhash_value()
                .map_err(|error| {
                    format!(
                        "runtime segment {} has an invalid terminal last blockhash: {error}",
                        segment.archive_path.display()
                    )
                })?;
            fs::File::open(&segment.archive_path)
                .map(std::io::BufReader::new)
                .map(|reader| {
                    RuntimeSegmentSource::new(
                        reader,
                        segment.manifest.archive_sha256,
                        terminal_last_blockhash,
                    )
                })
                .map_err(|err| {
                    format!(
                        "failed to open runtime segment {} for assembly: {err}",
                        segment.archive_path.display()
                    )
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let (sink, stats) =
        merge_runtime_segments(sources, sink, ArchiveWriterConfig::default(), &provenance)
            .map_err(|err| format!("verified runtime-segment assembly failed: {err}"))?;
    sink.sync_all()
        .map_err(|err| format!("failed to sync assembled archive: {err}"))?;
    drop(sink);
    verify_assembled_runtime_archive(temporary.path(), epoch, &provenance)?;
    let preserved_output = preserve_existing_output(final_output)?;
    if let Some(backup) = preserved_output.as_ref() {
        warn!(
            "preserved previous output {} as {}",
            final_output.display(),
            backup.display()
        );
    }
    let published = match temporary.persist_noclobber(final_output) {
        Ok(published) => published,
        Err(error) => {
            if let Some(backup) = preserved_output.as_ref()
                && !final_output.exists()
                && let Err(restore_error) = fs::rename(backup, final_output)
            {
                return Err(format!(
                    "failed to publish assembled archive {}: {}; also failed to restore preserved output {}: {restore_error}",
                    final_output.display(),
                    error.error,
                    backup.display()
                ));
            }
            return Err(format!(
                "failed to atomically publish assembled archive {}: {}",
                final_output.display(),
                error.error
            ));
        }
    };
    published
        .sync_all()
        .map_err(|err| format!("failed to sync {}: {err}", final_output.display()))?;
    fs::File::open(output_parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|err| format!("failed to sync {}: {err}", output_parent.display()))?;
    if !epoch_archive_reusable_multi_runtime(final_output, epoch, &spans)? {
        return Err(format!(
            "published archive {} did not pass final registry validation",
            final_output.display()
        ));
    }
    cleanup_runtime_segment_work_dir(&work_dir, epoch, &spans)?;
    info!(
        "epoch {epoch}: published verified multi-runtime archive {} (segments={}, slots={}, account_updates_rebased={}, bytes={})",
        final_output.display(),
        stats.source_archives,
        stats.slots_merged,
        stats.account_updates_rebased,
        stats.output.bytes_written,
    );
    Ok(())
}

/// Runs each epoch of a range in its own child process (this same binary,
/// invoked as `<exe> <epoch> <dest-dir> --epoch-hashes=… --range-info=…`), so
/// every byte of replay memory — accounts-db growth, caches, the bank itself —
/// is returned to the OS at each epoch boundary. In-process chaining was
/// observed OOM-killed at ~745 GiB RSS two epochs into a range; isolation
/// trades one boundary-snapshot load per epoch for a hard per-epoch memory
/// cap. Children never touch gcloud: snapshots and hash files were staged by
/// the caller before this runs.
async fn run_epoch_range_supervisor(
    effective_start: u64,
    end_epoch: u64,
    dest_dir: &Path,
    verify_snapshots: bool,
    allow_candidate_runtime: bool,
    shutdown: Arc<AtomicBool>,
    boundary_snapshots: BTreeMap<u64, PathBuf>,
) -> Result<(), String> {
    let exe =
        env::current_exe().map_err(|err| format!("failed to resolve current executable: {err}"))?;
    let attempts_per_epoch = env::var("JETSTREAMER_EPOCH_ATTEMPTS")
        .ok()
        .and_then(|v| v.trim().parse::<u32>().ok())
        .filter(|&a| a > 0)
        .unwrap_or(2);
    let prune_snapshots = env_truthy_default("JETSTREAMER_PRUNE_EPOCH_SNAPSHOTS", true);
    let total = end_epoch - effective_start + 1;
    for epoch in effective_start..=end_epoch {
        if shutdown.load(Ordering::SeqCst) {
            info!("shutdown requested; stopping before epoch {epoch}");
            return Ok(());
        }
        let (slot_start, slot_end_inclusive) = epoch_to_slot_range(epoch);
        let slot_range = slot_start..slot_end_inclusive.saturating_add(1);
        let spans = compatibility::plan_runtime_spans(slot_range.clone(), allow_candidate_runtime)?;
        let selection = runtime_span_selection(
            spans
                .first()
                .expect("runtime planner rejects empty epoch ranges"),
        )?;
        let jet_path = dest_dir.join(format!("epoch-{epoch}.jet"));
        let reusable = if spans.len() == 1 {
            epoch_archive_reusable(&jet_path, epoch, selection)?
        } else {
            match epoch_archive_reusable_multi_runtime(&jet_path, epoch, &spans) {
                Ok(reusable) => reusable,
                Err(err) => {
                    warn!(
                        "epoch {epoch}: pre-existing multi-runtime output {} is not reusable; regenerating it: {err}",
                        jet_path.display()
                    );
                    false
                }
            }
        };
        if reusable {
            info!("epoch {epoch} already complete; skipping");
            continue;
        }
        let hashes_path = dest_dir.join(format!("epoch-hashes-{epoch}.txt"));
        let snapshot_path = boundary_snapshots.get(&epoch).ok_or_else(|| {
            format!("range supervisor has no staged boundary snapshot for epoch {epoch}")
        })?;
        let mut attempt = 0u32;
        loop {
            attempt += 1;
            info!(
                "=== epoch {epoch} ({}/{total}): spawning child process (attempt \
                 {attempt}/{attempts_per_epoch}) ===",
                epoch - effective_start + 1
            );
            let mut cmd = Command::new(&exe);
            cmd.env_remove("JETSTREAMER_EXPORT_HANDOFF_SNAPSHOT_DIR")
                .arg(epoch.to_string())
                .arg(dest_dir.as_os_str())
                .arg(if verify_snapshots {
                    "--verify"
                } else {
                    "--no-verify"
                })
                .arg(format!("--range-info={effective_start}-{end_epoch}"));
            if verify_snapshots {
                cmd.arg(format!("--epoch-hashes={}", hashes_path.display()));
            }
            let mut snapshot_arg = OsString::from("--snapshot-archive=");
            snapshot_arg.push(snapshot_path.as_os_str());
            cmd.arg(snapshot_arg);
            let status = cmd
                .status()
                .await
                .map_err(|err| format!("failed to spawn child for epoch {epoch}: {err}"))?;
            // The epoch is done only if its `.jet` finalized — a child
            // interrupted by ctrl-c shuts down gracefully and exits 0 without
            // finishing, so the exit code alone can't be trusted.
            let reusable = if !status.success() {
                false
            } else if spans.len() == 1 {
                epoch_archive_reusable(&jet_path, epoch, selection)?
            } else {
                epoch_archive_reusable_multi_runtime(&jet_path, epoch, &spans)?
            };
            if status.success() && reusable {
                info!("epoch {epoch} child completed");
                let _ = fs::remove_file(&hashes_path);
                if prune_snapshots && let Some(path) = boundary_snapshots.get(&epoch) {
                    match prune_epoch_boundary_snapshot(path, selection.descriptor) {
                        Ok(()) => {
                            info!("pruned boundary snapshot artifacts for {}", path.display())
                        }
                        Err(err) => {
                            warn!(
                                "failed to prune boundary snapshot artifacts for {}: {err}",
                                path.display()
                            )
                        }
                    }
                }
                break;
            }
            // Ctrl-C goes to the whole process group, so the child dies with a
            // non-success status while our own flag is set — that's a shutdown,
            // not a crash; don't burn a retry on it.
            if shutdown.load(Ordering::SeqCst) {
                info!("epoch {epoch} child stopped by shutdown request");
                return Ok(());
            }
            if attempt >= attempts_per_epoch {
                return Err(format!(
                    "epoch {epoch} failed after {attempts_per_epoch} attempt(s) (last exit: {status})"
                ));
            }
            warn!("epoch {epoch} child failed (exit: {status}); retrying");
        }
    }
    info!("=== all epochs {effective_start}-{end_epoch} complete ===");
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
                eprintln!("CTRL+C received, shutting down... (press again to force-exit)");
            } else {
                eprintln!("CTRL+C received again, force-exiting");
                exit(130);
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

    let (start_epoch, end_epoch) = match parse_epoch_range(&epoch_arg) {
        Ok(range) => range,
        Err(err) => {
            eprintln!("{err}");
            eprintln!("{}", usage(&program));
            exit(2);
        }
    };

    let mut dest_dir_arg = None;
    let mut verify_snapshots = env_truthy_default("JETSTREAMER_VERIFY_SNAPSHOTS", true);
    // Qualification requires a deliberate CLI opt-in, not merely the default
    // value inherited from the environment.
    let mut explicit_verify: Option<bool> = None;
    let mut horizon_output: Option<PathBuf> = None;
    let mut qualification_end_slot: Option<Slot> = None;
    // Internal flags set by the range supervisor when spawning per-epoch
    // children: pre-fetched snapshot hashes (so the child never touches
    // gcloud) and the overall range for the child's overall-progress line.
    let mut epoch_hashes: Option<PathBuf> = None;
    let mut snapshot_archive_override: Option<PathBuf> = None;
    let mut range_info: Option<(u64, u64)> = None;
    for arg in args {
        if arg == "--verify" {
            verify_snapshots = true;
            explicit_verify = Some(true);
        } else if arg == "--no-verify" {
            verify_snapshots = false;
            explicit_verify = Some(false);
        } else if let Some(path) = arg.strip_prefix("--horizon-output=") {
            horizon_output = Some(PathBuf::from(path));
        } else if let Some(slot) = arg.strip_prefix("--qualification-end-slot=") {
            if qualification_end_slot.is_some() {
                eprintln!("duplicate --qualification-end-slot option");
                exit(2);
            }
            match slot.parse::<Slot>() {
                Ok(slot) => qualification_end_slot = Some(slot),
                Err(err) => {
                    eprintln!("invalid --qualification-end-slot '{slot}': {err}");
                    exit(2);
                }
            }
        } else if let Some(path) = arg.strip_prefix("--epoch-hashes=") {
            epoch_hashes = Some(PathBuf::from(path));
        } else if let Some(path) = arg.strip_prefix("--snapshot-archive=") {
            snapshot_archive_override = Some(PathBuf::from(path));
        } else if let Some(spec) = arg.strip_prefix("--range-info=") {
            match parse_epoch_range(spec) {
                Ok(range) => range_info = Some(range),
                Err(err) => {
                    eprintln!("invalid --range-info: {err}");
                    exit(2);
                }
            }
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

    if horizon_output.is_some() && start_epoch != end_epoch {
        eprintln!(
            "--horizon-output cannot be used with an epoch range ({start_epoch}-{end_epoch}); \
             each epoch is written to <dest-dir>/epoch-<N>.jet"
        );
        exit(2);
    }
    if epoch_hashes.is_some() && start_epoch != end_epoch {
        eprintln!("--epoch-hashes applies only to a single-epoch (child) invocation");
        exit(2);
    }
    if snapshot_archive_override.is_some() && start_epoch != end_epoch {
        eprintln!("--snapshot-archive applies only to a single-epoch (child) invocation");
        exit(2);
    }
    if qualification_end_slot.is_some() && range_info.is_some() {
        eprintln!("--qualification-end-slot cannot be combined with --range-info");
        exit(2);
    }
    let qualification = match qualification_plan(
        start_epoch,
        end_epoch,
        qualification_end_slot,
        explicit_verify,
        snapshot_archive_override.as_deref(),
        epoch_hashes.as_deref(),
        horizon_output.as_deref(),
    ) {
        Ok(plan) => plan,
        Err(err) => {
            eprintln!("error: {err}");
            exit(2);
        }
    };
    if let Some(plan) = qualification {
        info!(
            "focused qualification requested: epoch {}, bootstrap {}, replay {}..={}, output {}",
            plan.epoch,
            plan.bootstrap_slot,
            plan.replay_start,
            plan.end_inclusive,
            horizon_output
                .as_deref()
                .expect("qualification output was validated")
                .display()
        );
    }
    let horizon_output_override = horizon_output;

    // Candidate admission is checked for the complete requested range before
    // resume considers any existing output. This prevents a stale archive
    // from bypassing the runtime policy simply because its footer is valid.
    let allow_candidate_runtime = match strict_opt_in("JETSTREAMER_ALLOW_CANDIDATE_RUNTIME") {
        Ok(value) => value,
        Err(err) => {
            eprintln!("error: {err}");
            exit(2);
        }
    };
    for epoch in start_epoch..=end_epoch {
        let slot_range = runtime_slot_range(epoch, qualification);
        let spans = match compatibility::plan_runtime_spans(slot_range, allow_candidate_runtime) {
            Ok(spans) => spans,
            Err(err) => {
                eprintln!("error: epoch {epoch}: {err}");
                exit(1);
            }
        };
        for span in spans {
            if span.execution.admission == compatibility::AdmissionLevel::Candidate
                && !verify_snapshots
            {
                let selection = runtime_span_selection(&span)
                    .expect("runtime planner cannot return an unsupported span");
                eprintln!(
                    "error: epoch {epoch}: candidate runtime profile {} requires snapshot verification; --no-verify is not allowed",
                    selection.backend
                );
                exit(1);
            }
        }
    }

    // Epoch-level resume for ranges: if an earlier run already finalized some
    // leading epochs (their `.jet` has a valid footer), skip them and restart at
    // the first incomplete one. Completed epochs are immutable, so this only
    // avoids redoing them; the resumed epoch bootstraps from a boundary snapshot
    // exactly like a fresh start (the in-memory bank from the prior epoch is gone
    // after a crash). Scoped to ranges so a single-epoch re-run still regenerates.
    let effective_start = if start_epoch != end_epoch {
        let mut first_incomplete = start_epoch;
        while first_incomplete <= end_epoch {
            let (slot_start, slot_end_inclusive) = epoch_to_slot_range(first_incomplete);
            let spans = compatibility::plan_runtime_spans(
                slot_start..slot_end_inclusive.saturating_add(1),
                allow_candidate_runtime,
            )
            .expect("requested range was preflighted above");
            let path = dest_dir.join(format!("epoch-{first_incomplete}.jet"));
            let reusable = if spans.len() == 1 {
                let selection = runtime_span_selection(&spans[0])
                    .expect("runtime planner cannot return an unsupported span");
                epoch_archive_reusable(&path, first_incomplete, selection)
            } else {
                match epoch_archive_reusable_multi_runtime(&path, first_incomplete, &spans) {
                    Ok(reusable) => Ok(reusable),
                    Err(err) => {
                        warn!(
                            "epoch {first_incomplete}: pre-existing multi-runtime output {} is not reusable; regenerating it: {err}",
                            path.display()
                        );
                        Ok(false)
                    }
                }
            };
            match reusable {
                Ok(true) => {
                    println!("epoch {first_incomplete} already complete; skipping");
                    first_incomplete += 1;
                }
                Ok(false) => break,
                Err(err) => {
                    eprintln!("error: {err}");
                    exit(1);
                }
            }
        }
        if first_incomplete > end_epoch {
            println!(
                "all epochs {start_epoch}-{end_epoch} already complete in {}; nothing to do",
                dest_dir.display()
            );
            return;
        }
        if first_incomplete != start_epoch {
            println!(
                "resuming range at epoch {first_incomplete} (epochs {start_epoch}-{} already done)",
                first_incomplete - 1
            );
        }
        first_incomplete
    } else {
        start_epoch
    };

    // Resolve execution semantics before touching snapshot state or the
    // network.  The archive format has its own compatibility rules and must
    // never be used as a proxy for the consensus runtime.  A range supervisor
    // repeats this check in each child, so the selected era is always derived
    // from the exact output slots that child owns.
    if allow_candidate_runtime {
        warn!(
            "candidate historical runtimes enabled for differential replay; output remains \
             non-canonical until every configured checkpoint passes"
        );
    }
    for epoch in effective_start..=end_epoch {
        let slot_range = runtime_slot_range(epoch, qualification);
        match compatibility::plan_runtime_spans(slot_range.clone(), allow_candidate_runtime) {
            Ok(runtime_spans) => {
                for span in runtime_spans {
                    let selection = runtime_span_selection(&span)
                        .expect("runtime planner cannot return an unsupported span");
                    if selection.admission == compatibility::AdmissionLevel::Candidate
                        && !verify_snapshots
                    {
                        eprintln!(
                            "error: epoch {epoch}: candidate runtime profile {} requires snapshot verification; --no-verify is not allowed",
                            selection.backend
                        );
                        exit(1);
                    }
                    info!(
                        "epoch {epoch}: selected execution profile {} ({:?}) from slots {}..{}",
                        selection.backend, selection.admission, span.slots.start, span.slots.end
                    );
                    if let Some(artifact) = selection.backend.historical_artifact() {
                        info!(
                            "epoch {epoch}: historical artifact revision={} rust={} target={} genesis={}",
                            artifact.upstream_revision,
                            artifact.rust_toolchain,
                            artifact.target,
                            artifact.genesis_hash,
                        );
                    }
                }
                match compatibility::plan_replay(slot_range, allow_candidate_runtime) {
                    Ok(segments) => {
                        for segment in segments {
                            info!(
                                "epoch {epoch}: compatibility segment {}..{} input={:?} \
                                 missing-status={:?} execution={} output={:?}",
                                segment.slots.start,
                                segment.slots.end,
                                segment.input_metadata,
                                segment.missing_transaction_status,
                                segment.execution.name,
                                segment.output,
                            );
                        }
                    }
                    Err(err) => {
                        eprintln!("error: epoch {epoch}: {err}");
                        exit(1);
                    }
                }
            }
            Err(err) => {
                eprintln!("error: epoch {epoch}: {err}");
                exit(1);
            }
        }
    }
    let effective_runtime = bootstrap_runtime_selection(
        runtime_slot_range(effective_start, qualification),
        allow_candidate_runtime,
    )
    .expect("requested range was preflighted above");
    let effective_archive_extensions = effective_runtime.descriptor.bootstrap.archive_extensions;

    // Replay mutates the unpacked snapshot state in place (new appendvecs,
    // accounts index), so a crashed run leaves the staging dirs dirty.
    // Start every run from a clean unpack of the retained snapshot archive;
    // set JETSTREAMER_CLEAR_ACCOUNTS_ON_START=false to skip.
    if env_truthy_default("JETSTREAMER_CLEAR_ACCOUNTS_ON_START", true) {
        if let Err(err) = clear_ledger_accounts_state(&dest_dir) {
            eprintln!("error: {err}");
            exit(1);
        }
    } else {
        info!("ledger accounts cleanup disabled via JETSTREAMER_CLEAR_ACCOUNTS_ON_START=false");
    }

    // The snapshot bootstraps only the first epoch actually run (effective_start);
    // later epochs in a range chain off the in-memory bank.
    let target_slot = qualification
        .map(|plan| plan.bootstrap_slot)
        .unwrap_or_else(|| epoch_to_slot(effective_start).saturating_sub(1));
    // A local snapshot only bootstraps `effective_start` cheaply if it lands
    // within the previous epoch (at or after the start of `effective_start - 1`).
    // Anything older is still usable, but it forces a warmup replay across every
    // slot between the snapshot and the epoch boundary — e.g. reusing epoch N's
    // boundary snapshot to run epoch N+1 re-executes all of epoch N first
    // (~432k slots, roughly doubling the run). `find_existing_snapshot_archive`
    // only bounds the candidate from above (slot <= target_slot), so a leftover
    // snapshot from the prior epoch's run gets picked up here; reject it and
    // download the real boundary snapshot instead.
    let min_snapshot_slot = qualification
        .map(|plan| plan.bootstrap_slot)
        .unwrap_or_else(|| epoch_to_slot(effective_start.saturating_sub(1)));
    let discovered_snapshot = match snapshot_archive_override {
        Some(path) => match snapshot_archive_candidate(path) {
            Ok(candidate) => Some(candidate),
            Err(err) => {
                eprintln!("error: {err}");
                exit(1);
            }
        },
        None => match find_existing_snapshot_archive(
            &dest_dir,
            target_slot,
            effective_archive_extensions,
        ) {
            Ok(candidate) => candidate,
            Err(err) => {
                eprintln!("error: {err}");
                exit(1);
            }
        },
    };
    let existing_snapshot = match discovered_snapshot {
        Some(candidate)
            if qualification.is_some_and(|plan| candidate.slot != plan.bootstrap_slot) =>
        {
            eprintln!(
                "error: qualification snapshot resolved to slot {}, expected {}",
                candidate.slot,
                qualification
                    .expect("qualification guard established a plan")
                    .bootstrap_slot
            );
            exit(1);
        }
        Some(candidate) if candidate.slot < min_snapshot_slot => {
            println!(
                "Ignoring snapshot {} at slot {}: epoch {} needs a snapshot at or after slot {} \
                 (reusing it would warm up across {} slots); downloading the boundary snapshot",
                candidate.path.display(),
                candidate.slot,
                effective_start,
                min_snapshot_slot,
                epoch_to_slot(effective_start).saturating_sub(candidate.slot),
            );
            None
        }
        other => other,
    };
    let mut extracted_snapshot = false;
    let dest_path = match existing_snapshot {
        Some(candidate) => {
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
        None => {
            // A per-epoch child must never reach for gcloud (the supervisor
            // pre-downloads every boundary snapshot while the session is
            // fresh); a missing archive here is a supervisor bug, not a
            // download opportunity.
            if epoch_hashes.is_some() {
                eprintln!(
                    "error: no boundary snapshot archive for epoch {effective_start} in {} \
                     (child mode: expected the range supervisor to have pre-downloaded it)",
                    dest_dir.display()
                );
                exit(1);
            }
            match download_snapshot_at_or_before_slot_matching(
                effective_start,
                target_slot,
                &dest_dir,
                effective_archive_extensions,
            )
            .await
            {
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
    };
    if let Err(err) = validate_runtime_bootstrap_archive(effective_runtime.descriptor, &dest_path) {
        eprintln!("error: {err}");
        exit(1);
    }
    if qualification.is_none()
        && let Err(err) = validate_epoch_bootstrap_snapshot(effective_start, &dest_path)
    {
        eprintln!("error: {err}");
        exit(1);
    }

    if !env_truthy("JETSTREAMER_LOAD_FROM_DIR") {
        // The default archive loader unpacks into its own staging
        // (`.snapshot-extract-*` + accounts-run); the ledger-root
        // extraction below only feeds the dir loader.
        println!("Skipping ledger-root extraction (archive loader manages its own staging)");
    } else if env_truthy("JETSTREAMER_SKIP_EXTRACT") {
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

    // Fetch the genesis archive up front too (idempotent; gcloud/GCS), so the
    // replay never shells out to gcloud once it starts. After this point, no
    // code path touches gcloud — only old-faithful and local files.
    if let Err(err) = ensure_genesis_archive(&dest_dir).await {
        eprintln!("error: {err}");
        exit(1);
    }

    // Range runs default to per-epoch process isolation: each epoch replays in
    // its own child process so all replay memory (accounts-db growth, caches)
    // is released at every epoch boundary — in-process chaining was observed
    // OOM-killed at ~745 GiB RSS two epochs into a range. Isolation needs every
    // epoch's boundary snapshot on disk, so download them all now while the
    // gcloud/GCS session is fresh (a range runs for days; mid-run gcloud access
    // is forbidden). `dest_path` above already covers `effective_start`.
    let total_epochs = end_epoch - effective_start + 1;
    let configured_epoch_isolation = env_truthy_default("JETSTREAMER_EPOCH_ISOLATION", true);
    let (epoch_isolation, forced_multi_runtime_epoch) = match epoch_isolation_plan(
        effective_start,
        end_epoch,
        configured_epoch_isolation,
        allow_candidate_runtime,
    ) {
        Ok(plan) => plan,
        Err(err) => {
            eprintln!("error: failed to plan epoch isolation: {err}");
            exit(1);
        }
    };
    if let Some(epoch) = forced_multi_runtime_epoch {
        warn!(
            "JETSTREAMER_EPOCH_ISOLATION=0 cannot chain range {effective_start}-{end_epoch} in one process because epoch {epoch} crosses an execution-runtime boundary; forcing per-epoch process isolation"
        );
    }
    let mut boundary_snapshots: BTreeMap<u64, PathBuf> = BTreeMap::new();
    if epoch_isolation {
        boundary_snapshots.insert(effective_start, dest_path.clone());
        info!(
            "=== per-epoch process isolation enabled; pre-downloading boundary snapshots for \
             epochs {}-{end_epoch} (gcloud/GCS) ===",
            effective_start + 1
        );
        for epoch in effective_start + 1..=end_epoch {
            let (slot_start, slot_end) = epoch_to_slot_range(epoch);
            let selection = bootstrap_runtime_selection(
                slot_start..slot_end.saturating_add(1),
                allow_candidate_runtime,
            )
            .expect("requested range was preflighted above");
            match ensure_epoch_boundary_snapshot(
                epoch,
                &dest_dir,
                selection.descriptor.bootstrap.archive_extensions,
            )
            .await
            {
                Ok(path) => {
                    boundary_snapshots.insert(epoch, path);
                }
                Err(err) => {
                    eprintln!("error: {err}");
                    exit(1);
                }
            }
        }
    }

    // Pre-fetch every epoch's canonical snapshot hashes up front, while the
    // gcloud/GCS session is fresh. A multi-epoch range can run for days, and
    // listing epoch N+1's snapshots mid-run risks a stale session aborting the
    // whole range long after the replay no longer needs the network. Empty when
    // verification is disabled.
    let mut snapshot_expectations: BTreeMap<u64, BTreeMap<Slot, BankHashExpectation>> =
        BTreeMap::new();
    if verify_snapshots {
        if let Some(hashes_path) = &epoch_hashes {
            // Per-epoch child: the supervisor prefetched this epoch's hashes to
            // a file, so the child stays gcloud-free.
            match read_epoch_hashes_file(hashes_path, effective_archive_extensions) {
                Ok(mut expected) => {
                    if let Err(err) = add_runtime_handoff_expectations(
                        runtime_slot_range(effective_start, qualification),
                        &mut expected,
                    ) {
                        eprintln!("error: {err}");
                        exit(1);
                    }
                    let expected = if let Some(plan) = qualification {
                        match qualification_expectations(expected, plan, &dest_path) {
                            Ok(expected) => expected,
                            Err(err) => {
                                eprintln!("error: {err}");
                                exit(1);
                            }
                        }
                    } else {
                        expected
                    };
                    info!(
                        "snapshot verification: {} snapshot hash(es) loaded from {}",
                        expected.len(),
                        hashes_path.display()
                    );
                    snapshot_expectations.insert(effective_start, expected);
                }
                Err(err) => {
                    eprintln!("error: {err}");
                    exit(1);
                }
            }
        } else {
            info!(
                "=== prefetching all snapshot metadata for epochs {effective_start}-{end_epoch} up \
                 front; this is the last gcloud/GCS access — the replay below uses only old-faithful \
                 and local files ==="
            );
            for epoch in effective_start..=end_epoch {
                let (epoch_start_slot, epoch_end_slot) = epoch_to_slot_range(epoch);
                let selection = bootstrap_runtime_selection(
                    epoch_start_slot..epoch_end_slot.saturating_add(1),
                    allow_candidate_runtime,
                )
                .expect("requested range was preflighted above");
                let archive_extensions = selection.descriptor.bootstrap.archive_extensions;
                let boundary_path = boundary_snapshots
                    .get(&epoch)
                    .or_else(|| (epoch == effective_start).then_some(&dest_path));
                let verification_start = match boundary_path {
                    Some(path) => {
                        let name = match path.file_name().and_then(|name| name.to_str()) {
                            Some(name) => name,
                            None => {
                                eprintln!(
                                    "error: snapshot path has no UTF-8 filename: {}",
                                    path.display()
                                );
                                exit(1);
                            }
                        };
                        match parse_snapshot_archive_name(name) {
                            Ok((slot, _)) => slot,
                            Err(err) => {
                                eprintln!("error: {err}");
                                exit(1);
                            }
                        }
                    }
                    None => epoch_start_slot,
                };
                info!(
                    "collecting canonical snapshot hashes for slots {}..={} (output epoch {epoch})",
                    verification_start, epoch_end_slot
                );
                let mut expected = match snapshot_expectations_for_span(
                    verification_start,
                    epoch_end_slot,
                    archive_extensions,
                )
                .await
                {
                    Ok(expected) => expected,
                    Err(err) => {
                        eprintln!("error: {err}");
                        exit(1);
                    }
                };
                if let Some(boundary_path) = boundary_path
                    && is_legacy_snapshot_archive(boundary_path)
                {
                    match legacy_boundary_expectation(boundary_path) {
                        Ok(boundary_expected) => {
                            for (slot, hash) in boundary_expected {
                                match expected.get(&slot) {
                                    Some(existing) if *existing != hash => {
                                        eprintln!(
                                            "error: conflicting legacy snapshot hashes at slot \
                                             {slot}: listing={existing:?} boundary={hash:?}"
                                        );
                                        exit(1);
                                    }
                                    Some(_) => {}
                                    None => {
                                        warn!(
                                            "snapshot listing omitted boundary file {}; adding \
                                             its legacy accounts hash at slot {slot}",
                                            boundary_path.display()
                                        );
                                        expected.insert(slot, hash);
                                    }
                                }
                            }
                        }
                        Err(err) => {
                            eprintln!("error: {err}");
                            exit(1);
                        }
                    }
                }
                if let Err(err) = add_runtime_handoff_expectations(
                    epoch_start_slot..epoch_end_slot.saturating_add(1),
                    &mut expected,
                ) {
                    eprintln!("error: {err}");
                    exit(1);
                }
                info!(
                    "snapshot verification: {} snapshot(s) prefetched for epoch {epoch}",
                    expected.len()
                );
                snapshot_expectations.insert(epoch, expected);
            }
            info!(
                "=== gcloud/GCS prefetch complete; no further gcloud access for the rest of the run ==="
            );
        }
    }

    // A runtime transition inside one epoch cannot be represented by a bank
    // carried across the process boundary: each historical Solana release is
    // intentionally isolated in its own executable and dependency graph.
    // Split the epoch into registry-owned qualification children, prove the
    // canonical snapshot transition independently on both sides, and assemble
    // their complete V2 archives into one V3 archive.
    let effective_epoch_spans = compatibility::plan_runtime_spans(
        runtime_slot_range(effective_start, qualification),
        allow_candidate_runtime,
    )
    .expect("requested range was preflighted above");
    if qualification.is_none() && effective_start == end_epoch && effective_epoch_spans.len() > 1 {
        if !verify_snapshots {
            eprintln!("error: multi-runtime epoch assembly requires --verify");
            exit(1);
        }
        let hashes_path = match epoch_hashes.clone() {
            Some(path) => path,
            None => {
                let path = dest_dir.join(format!(
                    ".epoch-{effective_start}-runtime-segment-hashes.txt"
                ));
                let Some(expected) = snapshot_expectations.get(&effective_start) else {
                    eprintln!(
                        "error: no canonical checkpoint set was prepared for multi-runtime epoch {effective_start}"
                    );
                    exit(1);
                };
                if let Err(err) = write_epoch_hashes_file(&path, expected) {
                    eprintln!("error: {err}");
                    exit(1);
                }
                path
            }
        };
        let final_output = horizon_output_override
            .clone()
            .unwrap_or_else(|| dest_dir.join(format!("epoch-{effective_start}.jet")));
        if let Err(err) = run_multi_runtime_epoch_supervisor(
            effective_start,
            &dest_dir,
            &dest_path,
            &hashes_path,
            &final_output,
            allow_candidate_runtime,
            shutdown.clone(),
        )
        .await
        {
            eprintln!("error: {err}");
            exit(1);
        }
        return;
    }

    // Per-epoch process isolation: hand the prefetched hashes to disk (one file
    // per epoch, read back by each child) and supervise one child process per
    // epoch. Everything a child needs is now local; no child ever touches
    // gcloud/GCS.
    if epoch_isolation {
        for (epoch, expected) in &snapshot_expectations {
            let path = dest_dir.join(format!("epoch-hashes-{epoch}.txt"));
            if let Err(err) = write_epoch_hashes_file(&path, expected) {
                eprintln!("error: {err}");
                exit(1);
            }
        }
        if let Err(err) = run_epoch_range_supervisor(
            effective_start,
            end_epoch,
            &dest_dir,
            verify_snapshots,
            allow_candidate_runtime,
            shutdown.clone(),
            boundary_snapshots,
        )
        .await
        {
            eprintln!("error: {err}");
            exit(1);
        }
        return;
    }

    // Single-process fallback (JETSTREAMER_EPOCH_ISOLATION=0), or a per-epoch
    // child of the supervisor. A chained range replays each epoch in turn: the
    // first epoch loads the snapshot; each subsequent epoch reuses the
    // typed runtime state handed back by the previous one, so
    // there is no snapshot reload and no warmup between epochs. Verification
    // hashes were prefetched above; the `.jet` output path is resolved per
    // epoch.
    //
    // Cross-epoch progress so the per-epoch progress thread can also report
    // overall span % + ETA. A supervisor child learns the overall range via
    // --range-info (its own range is a single epoch); a chained range builds it
    // from its own bounds.
    let range_progress = if let Some((first, last)) = range_info {
        Some(Arc::new(RangeProgress {
            overall_start_slot: epoch_to_slot_range(first).0,
            overall_end_slot: epoch_to_slot_range(last).1,
            first_epoch: first,
            total_epochs: last - first + 1,
            baseline: Mutex::new(None),
        }))
    } else if total_epochs > 1 {
        Some(Arc::new(RangeProgress {
            overall_start_slot: epoch_to_slot_range(effective_start).0,
            overall_end_slot: epoch_to_slot_range(end_epoch).1,
            first_epoch: effective_start,
            total_epochs,
            baseline: Mutex::new(None),
        }))
    } else {
        None
    };
    // One progress instance shared across every epoch in the range. The
    // accounts-db notifier, created at the first epoch's load and carried with
    // the reused bank, captures this and keeps updating it for chained epochs —
    // otherwise their per-epoch counter stays at 0 and the stall watchdog aborts.
    let shared_progress = Arc::new(ReplayProgress::new(
        qualification
            .map(|plan| plan.replay_start)
            .unwrap_or_else(|| epoch_to_slot_range(effective_start).0),
    ));
    let mut carried_state: Option<CarriedRuntimeState> = None;
    for epoch in effective_start..=end_epoch {
        if shutdown.load(Ordering::SeqCst) {
            info!("shutdown requested; stopping before epoch {epoch}");
            break;
        }
        if total_epochs > 1 {
            info!(
                "=== epoch {epoch} ({}/{total_epochs}) ===",
                epoch - effective_start + 1
            );
        }
        let horizon_output = horizon_output_override
            .clone()
            .unwrap_or_else(|| dest_dir.join(format!("epoch-{epoch}.jet")));

        let snapshot_verifier = if verify_snapshots {
            let expected = snapshot_expectations.remove(&epoch).unwrap_or_default();
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

        let result = run_geyser_replay(
            epoch,
            allow_candidate_runtime,
            &dest_dir,
            &dest_path,
            shutdown.clone(),
            cursor.clone(),
            restart_tracker.clone(),
            snapshot_verifier,
            horizon_output.clone(),
            qualification,
            carried_state.take(),
            range_progress.clone(),
            Some(shared_progress.clone()),
        )
        .await;
        match result {
            Ok(result) => {
                if let Some(plan) = qualification
                    && result.historical_evidence.is_some()
                {
                    if let Err(err) = publish_historical_segment_manifest(
                        epoch,
                        plan,
                        &horizon_output,
                        &result,
                        allow_candidate_runtime,
                    ) {
                        eprintln!("error: {err}");
                        exit(1);
                    }
                } else if result.historical_evidence.is_some() {
                    info!("historical replay evidence captured for epoch {epoch}");
                }
                carried_state = result.carried_state;
            }
            Err(err) => {
                eprintln!("error: {err}");
                exit(1);
            }
        }
    }
}

#[cfg(test)]
mod early_snapshot_tests {
    use super::*;

    #[test]
    fn legacy_bzip2_snapshot_format_is_classified_without_selecting_a_runtime() {
        let hash = Hash::default();
        let legacy = PathBuf::from(format!("snapshot-416012-{hash}.tar.bz2"));
        let modern = PathBuf::from(format!("snapshot-416012-{hash}.tar.zst"));

        assert!(is_legacy_snapshot_archive(&legacy));
        assert!(!is_legacy_snapshot_archive(&modern));
    }

    #[test]
    fn legacy_snapshot_filename_uses_accounts_hash_verification() {
        let hash = Hash::new_unique();
        let path = PathBuf::from(format!("snapshot-416012-{hash}.tar.bz2"));
        let expected = legacy_boundary_expectation(&path).expect("valid legacy snapshot");

        assert!(matches!(
            expected.get(&416_012),
            Some(BankHashExpectation::LegacyAccountsHash(actual)) if actual == &hash
        ));
    }

    #[test]
    fn snapshot_bootstrap_starts_after_the_snapshot() {
        assert_eq!(initial_replay_slot(416_012, 432_000).unwrap(), 416_013);
        assert!(initial_replay_slot(432_000, 432_000).is_err());
        assert!(initial_replay_slot(432_001, 432_000).is_err());
    }

    #[test]
    fn candidate_opt_in_rejects_unknown_spellings() {
        assert!(!parse_strict_opt_in("CANDIDATE", None).unwrap());
        assert!(parse_strict_opt_in("CANDIDATE", Some("yes")).unwrap());
        assert!(!parse_strict_opt_in("CANDIDATE", Some("false")).unwrap());
        assert!(parse_strict_opt_in("CANDIDATE", Some("off")).is_err());
        assert!(parse_strict_opt_in("CANDIDATE", Some("tru")).is_err());
    }

    #[test]
    fn epoch_range_rejects_slot_arithmetic_overflow() {
        assert!(parse_epoch_range(&u64::MAX.to_string()).is_err());
        assert_eq!(parse_epoch_range("1-10").unwrap(), (1, 10));
    }

    fn qualification_snapshot(slot: Slot, hash: Hash) -> PathBuf {
        PathBuf::from(format!("snapshot-{slot}-{hash}.tar.bz2"))
    }

    #[test]
    fn focused_qualification_uses_only_the_post_bootstrap_span() {
        let snapshot = qualification_snapshot(515_912, Hash::new_unique());
        let hashes = Path::new("epoch-hashes-1.txt");
        let output = Path::new("qualification-515913-534248.jet");
        let plan = qualification_plan(
            1,
            1,
            Some(534_248),
            Some(true),
            Some(&snapshot),
            Some(hashes),
            Some(output),
        )
        .unwrap()
        .unwrap();

        assert_eq!(plan.bootstrap_slot, 515_912);
        assert_eq!(plan.replay_start, 515_913);
        assert_eq!(plan.output_slot_start, 515_913);
        assert_eq!(plan.end_inclusive, 534_248);
        assert_eq!(plan.slot_count(), 18_336);
        assert_eq!(runtime_slot_range(1, Some(plan)), 515_913..534_249);
        assert_eq!(runtime_slot_range(1, None), 432_000..864_000);
    }

    #[test]
    fn runtime_boundary_detection_forces_epoch_one_range_isolation() {
        assert_eq!(
            epoch_isolation_plan(1, 2, false, true).unwrap(),
            (true, Some(1))
        );
        assert_eq!(
            epoch_isolation_plan(2, 3, false, true).unwrap(),
            (false, None)
        );
        assert_eq!(
            epoch_isolation_plan(2, 3, true, true).unwrap(),
            (true, None)
        );
        assert_eq!(
            epoch_isolation_plan(1, 1, true, true).unwrap(),
            (false, None)
        );
    }

    #[test]
    fn focused_qualification_requires_explicit_safe_inputs() {
        let hash = Hash::new_unique();
        let snapshot = qualification_snapshot(515_912, hash);
        let hashes = Path::new("epoch-hashes-1.txt");
        let output = Path::new("qualification.jet");

        assert!(
            qualification_plan(
                1,
                2,
                Some(534_248),
                Some(true),
                Some(&snapshot),
                Some(hashes),
                Some(output),
            )
            .unwrap_err()
            .contains("requires one epoch")
        );
        assert!(
            qualification_plan(
                1,
                1,
                Some(534_248),
                None,
                Some(&snapshot),
                Some(hashes),
                Some(output),
            )
            .unwrap_err()
            .contains("explicit --verify")
        );
        assert!(
            qualification_plan(
                1,
                1,
                Some(534_248),
                Some(true),
                None,
                Some(hashes),
                Some(output),
            )
            .unwrap_err()
            .contains("--snapshot-archive")
        );
        assert!(
            qualification_plan(
                1,
                1,
                Some(534_248),
                Some(true),
                Some(&snapshot),
                None,
                Some(output),
            )
            .unwrap_err()
            .contains("--epoch-hashes")
        );
        assert!(
            qualification_plan(
                1,
                1,
                Some(534_248),
                Some(true),
                Some(&snapshot),
                Some(hashes),
                None,
            )
            .unwrap_err()
            .contains("--horizon-output")
        );
        assert!(
            qualification_plan(
                1,
                1,
                Some(515_912),
                Some(true),
                Some(&snapshot),
                Some(hashes),
                Some(output),
            )
            .unwrap_err()
            .contains("must be after bootstrap")
        );

        let previous_epoch_snapshot = qualification_snapshot(416_012, hash);
        let previous_epoch_plan = qualification_plan(
            1,
            1,
            Some(534_248),
            Some(true),
            Some(&previous_epoch_snapshot),
            Some(hashes),
            Some(output),
        )
        .unwrap()
        .unwrap();
        assert_eq!(previous_epoch_plan.bootstrap_slot, 416_012);
        assert_eq!(previous_epoch_plan.replay_start, 416_013);
        assert_eq!(previous_epoch_plan.output_slot_start, 432_000);
        assert_eq!(previous_epoch_plan.slot_count(), 102_249);

        assert!(
            qualification_plan(
                2,
                2,
                Some(900_000),
                Some(true),
                Some(&previous_epoch_snapshot),
                Some(hashes),
                Some(output),
            )
            .unwrap_err()
            .contains("snapshot slot 416012 is outside the target or preceding epoch")
        );
    }

    #[test]
    fn focused_qualification_filters_and_requires_canonical_checkpoints() {
        let bootstrap_hash = Hash::new_unique();
        let post_hash = Hash::new_unique();
        let snapshot = qualification_snapshot(515_912, bootstrap_hash);
        let plan = QualificationPlan {
            epoch: 1,
            bootstrap_slot: 515_912,
            replay_start: 515_913,
            output_slot_start: 515_913,
            end_inclusive: 534_248,
        };
        let expected = BTreeMap::from([
            (
                515_912,
                BankHashExpectation::LegacyAccountsHash(bootstrap_hash),
            ),
            (534_248, BankHashExpectation::LegacyAccountsHash(post_hash)),
            (
                619_848,
                BankHashExpectation::LegacyAccountsHash(Hash::new_unique()),
            ),
        ]);

        let filtered = qualification_expectations(expected, plan, &snapshot).unwrap();
        assert_eq!(filtered.len(), 2);
        assert!(filtered.contains_key(&515_912));
        assert!(filtered.contains_key(&534_248));
        assert!(!filtered.contains_key(&619_848));

        let bootstrap_only = BTreeMap::from([(
            515_912,
            BankHashExpectation::LegacyAccountsHash(bootstrap_hash),
        )]);
        assert!(
            qualification_expectations(bootstrap_only, plan, &snapshot)
                .unwrap_err()
                .contains("requires a canonical checkpoint")
        );

        let wrong_bootstrap = BTreeMap::from([
            (
                515_912,
                BankHashExpectation::LegacyAccountsHash(Hash::new_unique()),
            ),
            (534_248, BankHashExpectation::LegacyAccountsHash(post_hash)),
        ]);
        assert!(
            qualification_expectations(wrong_bootstrap, plan, &snapshot)
                .unwrap_err()
                .contains("does not match the canonical checkpoint")
        );
    }

    #[test]
    fn candidate_checkpoint_gate_excludes_the_bootstrap_slot() {
        let bootstrap_only = BTreeMap::from([(
            416_012,
            BankHashExpectation::LegacyAccountsHash(Hash::new_unique()),
        )]);
        let verifier = SnapshotVerifier::new(bootstrap_only, None);
        assert_eq!(verifier.checkpoint_count_in_range(416_013, 863_999), 0);

        verifier.expected.insert(
            515_912,
            BankHashExpectation::LegacyAccountsHash(Hash::new_unique()),
        );
        assert_eq!(verifier.checkpoint_count_in_range(416_013, 863_999), 1);
    }

    #[test]
    fn snapshot_verifier_retains_the_checkpoint_mismatch_diagnostic() {
        let expected = Hash::new_unique();
        let actual = Hash::new_unique();
        let verifier = SnapshotVerifier::new(
            BTreeMap::from([(515_912, BankHashExpectation::LegacyAccountsHash(expected))]),
            None,
        );

        verifier.verify_legacy_accounts_hash(515_912, actual);

        let error = verifier.error_summary().unwrap();
        assert!(error.contains("legacy accounts hash mismatch at slot 515912"));
        assert!(error.contains(&expected.to_string()));
        assert!(error.contains(&actual.to_string()));
    }

    #[test]
    fn archive_provenance_records_the_slot_selected_runtime() {
        let selection = compatibility::select_runtime(
            432_000..compatibility::SOLANA_V1_0_8_ROUTING_START_SLOT,
            true,
        )
        .unwrap();
        let state_hash = Hash::new_unique();
        let provenance = build_archive_provenance(
            selection,
            Some([0x5a; 32]),
            BootstrapStateKind::SnapshotArchive,
            416_012,
            state_hash,
            432_000,
            compatibility::SOLANA_V1_0_8_ROUTING_START_SLOT - 432_000,
        )
        .unwrap();
        assert_eq!(
            provenance.single_runtime_worker_executable_sha256(),
            Some(Some([0x5a; 32]))
        );
        assert_eq!(provenance.version(), 2);
        let provenance = provenance.single_runtime_v1().unwrap();
        assert_eq!(provenance.runtime_profile, "solana-v1.0.7");
        assert_eq!(provenance.runtime_admission, RuntimeAdmission::Candidate);
        assert_eq!(
            provenance.runtime_revision,
            compatibility::SOLANA_V1_0_7_REVISION
        );
        assert_eq!(provenance.bootstrap_slot, 416_012);
        assert_eq!(provenance.bootstrap_state_hash, state_hash);
        assert_eq!(
            provenance.transaction_metadata,
            TransactionMetadataPolicy::runtime_reconstructed_before(
                compatibility::OLD_FAITHFUL_STATUS_REQUIRED_START_SLOT
            )
        );
    }

    #[test]
    fn historical_archive_provenance_requires_the_measured_worker_digest() {
        let selection = compatibility::select_runtime(
            432_000..compatibility::SOLANA_V1_0_8_ROUTING_START_SLOT,
            true,
        )
        .unwrap();
        let error = build_archive_provenance(
            selection,
            None,
            BootstrapStateKind::SnapshotArchive,
            416_012,
            Hash::new_unique(),
            432_000,
            compatibility::SOLANA_V1_0_8_ROUTING_START_SLOT - 432_000,
        )
        .unwrap_err();
        assert!(error.contains("does not match executable provenance"));
    }

    #[test]
    fn stale_worker_digest_is_not_compatible_with_archive_provenance() {
        let selection = compatibility::select_runtime(
            432_000..compatibility::SOLANA_V1_0_8_ROUTING_START_SLOT,
            true,
        )
        .unwrap();
        let provenance = build_archive_provenance(
            selection,
            Some([0x11; 32]),
            BootstrapStateKind::SnapshotArchive,
            416_012,
            Hash::new_unique(),
            432_000,
            compatibility::SOLANA_V1_0_8_ROUTING_START_SLOT - 432_000,
        )
        .unwrap();
        assert!(archive_worker_executable_matches(
            &provenance,
            Some([0x11; 32])
        ));
        assert!(!archive_worker_executable_matches(
            &provenance,
            Some([0x12; 32])
        ));
        assert!(!archive_worker_executable_matches(&provenance, None));
    }

    #[test]
    fn epoch_bootstrap_snapshot_must_land_in_the_previous_epoch() {
        let hash = Hash::default();
        let valid = PathBuf::from(format!("snapshot-416012-{hash}.tar.bz2"));
        assert_eq!(
            validate_epoch_bootstrap_snapshot(1, &valid).unwrap(),
            416_012
        );

        let too_old = PathBuf::from(format!("snapshot-104612-{hash}.tar.bz2"));
        assert!(
            validate_epoch_bootstrap_snapshot(2, &too_old)
                .unwrap_err()
                .contains("requires a bootstrap snapshot")
        );

        let too_new = PathBuf::from(format!("snapshot-432000-{hash}.tar.bz2"));
        assert!(
            validate_epoch_bootstrap_snapshot(1, &too_new)
                .unwrap_err()
                .contains("requires a bootstrap snapshot")
        );
    }

    #[test]
    fn missing_transaction_status_is_allowed_only_for_qualified_reconstruction() {
        let signature = Signature::default();
        assert!(
            validate_transaction_status_presence(
                compatibility::MissingTransactionStatus::Reconstruct,
                false,
                416_013,
                0,
                &signature,
            )
            .is_ok()
        );
        assert!(
            validate_transaction_status_presence(
                compatibility::MissingTransactionStatus::Reject,
                true,
                406_080_000,
                0,
                &signature,
            )
            .is_ok()
        );
        let error = validate_transaction_status_presence(
            compatibility::MissingTransactionStatus::Reject,
            false,
            406_080_000,
            7,
            &signature,
        )
        .unwrap_err();
        assert!(error.contains("status metadata is required"));
    }

    #[test]
    fn multi_runtime_provenance_rebases_writes_and_proves_the_handoff() {
        let spans = compatibility::plan_runtime_spans(432_000..864_000, true).unwrap();
        assert_eq!(spans.len(), 2);
        let first_selection = runtime_span_selection(&spans[0]).unwrap();
        let second_selection = runtime_span_selection(&spans[1]).unwrap();
        let initial_accounts_hash = Hash::new_unique();
        let handoff_accounts_hash = spans[1].handoff.unwrap().snapshot.accounts_hash().unwrap();
        let handoff_bank_hash = Hash::new_unique();
        let handoff_last_blockhash = Hash::new_unique();

        let provenance = |selection: compatibility::RuntimeSelection,
                          digest: [u8; 32],
                          bootstrap_slot: Slot,
                          bootstrap_hash: Hash,
                          start: Slot,
                          count: u64| {
            let value = build_archive_provenance(
                selection,
                Some(digest),
                BootstrapStateKind::SnapshotArchive,
                bootstrap_slot,
                bootstrap_hash,
                start,
                count,
            )
            .unwrap();
            let ArchiveProvenance::V2(value) = value else {
                panic!("historical provenance must be V2");
            };
            value
        };
        let checkpoint =
            |slot: Slot,
             bank_hash: Hash,
             accounts_hash: Hash,
             last_blockhash: Hash,
             next_write_version: u64| SegmentCheckpointSummary {
                slot,
                bank_hash: bank_hash.to_string(),
                accounts_hash: accounts_hash.to_string(),
                last_blockhash: last_blockhash.to_string(),
                capitalization: 42,
                transaction_count: 84,
                tick_height: 126,
                slot_complete: true,
                write_count: 0,
                next_write_version,
            };
        let runtime_identity = |selection: compatibility::RuntimeSelection| {
            let identity = selection.descriptor.identity;
            SegmentRuntimeIdentity {
                generation_profile: archive_generation_profile(),
                runtime_profile: identity.name.to_owned(),
                runtime_admission: SegmentRuntimeAdmission::Candidate,
                runtime_revision: identity.revision.to_owned(),
                runtime_toolchain: archive_runtime_toolchain(identity),
                runtime_target: identity.target.unwrap().to_owned(),
                genesis_hash: identity.genesis_hash.to_owned(),
            }
        };

        let first_terminal = checkpoint(
            619_848,
            handoff_bank_hash,
            handoff_accounts_hash,
            handoff_last_blockhash,
            900,
        );
        let second_bootstrap = checkpoint(
            619_848,
            handoff_bank_hash,
            handoff_accounts_hash,
            handoff_last_blockhash,
            10,
        );
        let segments = vec![
            ValidatedRuntimeSegment {
                archive_path: PathBuf::from("first.jet"),
                manifest: HistoricalSegmentManifest {
                    schema_version:
                        jetstreamer_node::segment_manifest::SEGMENT_MANIFEST_SCHEMA_VERSION,
                    epoch: 1,
                    output_slot_start: 432_000,
                    output_slot_count: 187_849,
                    runtime: runtime_identity(first_selection),
                    worker_executable_sha256: [1; 32],
                    archive_sha256: [11; 32],
                    bootstrap_archive_sha256: None,
                    bootstrap: checkpoint(
                        416_012,
                        Hash::new_unique(),
                        initial_accounts_hash,
                        Hash::new_unique(),
                        50,
                    ),
                    terminal: first_terminal,
                    emitted_raw_write_versions: 100..900,
                },
                provenance: provenance(
                    first_selection,
                    [1; 32],
                    416_012,
                    initial_accounts_hash,
                    432_000,
                    187_849,
                ),
            },
            ValidatedRuntimeSegment {
                archive_path: PathBuf::from("second.jet"),
                manifest: HistoricalSegmentManifest {
                    schema_version:
                        jetstreamer_node::segment_manifest::SEGMENT_MANIFEST_SCHEMA_VERSION,
                    epoch: 1,
                    output_slot_start: 619_849,
                    output_slot_count: 244_151,
                    runtime: runtime_identity(second_selection),
                    worker_executable_sha256: [2; 32],
                    archive_sha256: [22; 32],
                    bootstrap_archive_sha256: Some([0xcc; 32]),
                    bootstrap: second_bootstrap,
                    terminal: checkpoint(
                        863_999,
                        Hash::new_unique(),
                        Hash::new_unique(),
                        Hash::new_unique(),
                        20,
                    ),
                    emitted_raw_write_versions: 10..20,
                },
                provenance: provenance(
                    second_selection,
                    [2; 32],
                    619_848,
                    handoff_accounts_hash,
                    619_849,
                    244_151,
                ),
            },
        ];

        let handoff_manifest = HistoricalHandoffSnapshotManifest {
            schema_version: HANDOFF_SNAPSHOT_MANIFEST_SCHEMA_VERSION,
            boundary_slot: 619_849,
            snapshot_slot: 619_848,
            accounts_hash: handoff_accounts_hash.to_string(),
            archive_path: "/test/snapshot-619848-canonical.tar.bz2".into(),
            archive_size: 123,
            archive_sha256: [0xcc; 32],
            source_runtime: segments[0].manifest.runtime.clone(),
            source_worker_executable_sha256: segments[0].manifest.worker_executable_sha256,
            terminal: segments[0].manifest.terminal.clone(),
        };
        assert!(handoff_snapshot_matches_predecessor(
            &handoff_manifest,
            &segments[0]
        ));
        let mut stale_runtime = handoff_manifest.clone();
        stale_runtime
            .source_runtime
            .runtime_revision
            .push_str("-stale");
        assert!(!handoff_snapshot_matches_predecessor(
            &stale_runtime,
            &segments[0]
        ));
        let mut stale_worker = handoff_manifest.clone();
        stale_worker.source_worker_executable_sha256 = [0xee; 32];
        assert!(!handoff_snapshot_matches_predecessor(
            &stale_worker,
            &segments[0]
        ));
        let mut stale_terminal = handoff_manifest.clone();
        stale_terminal.terminal.last_blockhash = Hash::new_unique().to_string();
        assert!(!handoff_snapshot_matches_predecessor(
            &stale_terminal,
            &segments[0]
        ));
        let combined = build_multi_runtime_provenance(
            1,
            &spans,
            &segments,
            std::slice::from_ref(&handoff_manifest),
        )
        .unwrap();
        assert_eq!(combined.runtime_segments[0].write_versions.archive_start, 0);
        assert_eq!(
            combined.runtime_segments[1].write_versions.archive_start,
            800
        );
        assert_eq!(combined.handoffs[0].boundary_slot, 619_849);
        assert_eq!(
            combined.handoffs[0].predecessor.accounts_hash,
            handoff_accounts_hash
        );
        assert_eq!(
            combined.handoffs[0].successor.accounts_hash,
            handoff_accounts_hash
        );
        assert_eq!(
            combined.handoffs[0].successor_bootstrap_archive_sha256,
            handoff_manifest.archive_sha256
        );

        let mut replaced_handoff = handoff_manifest.clone();
        replaced_handoff.archive_sha256 = [0xdd; 32];
        assert!(
            build_multi_runtime_provenance(1, &spans, &segments, &[replaced_handoff])
                .unwrap_err()
                .contains("does not match its predecessor and successor segment evidence")
        );

        let mut invalid = segments;
        invalid[1].manifest.bootstrap.write_count = 1;
        assert!(
            build_multi_runtime_provenance(1, &spans, &invalid, &[handoff_manifest])
                .unwrap_err()
                .contains("successor bootstrap emitted")
        );
    }

    fn tiny_multi_runtime_provenance(slot_start: Slot) -> ArchiveProvenanceV3 {
        let boundary = slot_start + 1;
        let checkpoint = RuntimeStateCheckpoint {
            slot: slot_start,
            bank_hash: Hash::new_from_array([0x11; 32]),
            accounts_hash_kind: AccountsHashKind::LegacyAccountsHash,
            accounts_hash: Hash::new_from_array([0x22; 32]),
            last_blockhash: Hash::new_from_array([0x33; 32]),
            capitalization: 42,
            transaction_count: 84,
            tick_height: 126,
            slot_complete: true,
            next_write_version: 5,
        };
        ArchiveProvenanceV3 {
            assembly_profile: "test/multi-runtime-assembly".into(),
            genesis_hash: compatibility::MAINNET_GENESIS_HASH.parse().unwrap(),
            bootstrap_state_kind: BootstrapStateKind::SnapshotArchive,
            bootstrap_state: StateCommitment {
                slot: slot_start - 1,
                kind: StateCommitmentKind::LegacyAccountsHash,
                hash: Hash::new_from_array([0x44; 32]),
            },
            requested_slot_start: slot_start,
            requested_slot_count: 2,
            transaction_metadata: TransactionMetadataPolicy::observed(),
            runtime_segments: vec![
                RuntimeSegmentProvenance {
                    slot_start,
                    slot_count: 1,
                    generation_profile: "test/producer".into(),
                    runtime_profile: "test/runtime-a".into(),
                    runtime_admission: RuntimeAdmission::Candidate,
                    runtime_revision: "revision-a".into(),
                    runtime_toolchain: "toolchain-a".into(),
                    worker_executable_sha256: Some([0xaa; 32]),
                    write_versions: WriteVersionNormalization {
                        worker_start: 5,
                        worker_end_exclusive: 5,
                        archive_start: 0,
                    },
                },
                RuntimeSegmentProvenance {
                    slot_start: boundary,
                    slot_count: 1,
                    generation_profile: "test/producer".into(),
                    runtime_profile: "test/runtime-b".into(),
                    runtime_admission: RuntimeAdmission::Candidate,
                    runtime_revision: "revision-b".into(),
                    runtime_toolchain: "toolchain-b".into(),
                    worker_executable_sha256: Some([0xbb; 32]),
                    write_versions: WriteVersionNormalization {
                        worker_start: 9,
                        worker_end_exclusive: 9,
                        archive_start: 0,
                    },
                },
            ],
            handoffs: vec![RuntimeHandoffProvenance {
                boundary_slot: boundary,
                predecessor: checkpoint,
                successor: RuntimeStateCheckpoint {
                    next_write_version: 9,
                    ..checkpoint
                },
                successor_bootstrap_kind: BootstrapStateKind::SnapshotArchive,
                successor_bootstrap_archive_sha256: [0xcc; 32],
                successor_bootstrap_write_count: 0,
            }],
        }
    }

    #[test]
    fn assembled_archive_verifier_rejects_bucket_corruption() {
        let directory = tempfile::TempDir::new().unwrap();
        let path = directory.path().join("tiny-v3.jet");
        let slot_start = 432_000;
        let provenance = tiny_multi_runtime_provenance(slot_start);
        let mut writer = jetstreamer_horizon::archive::ArchiveWriter::new_with_provenance(
            Vec::new(),
            1,
            slot_start,
            2,
            ArchiveWriterConfig::default(),
            &ArchiveProvenance::V3(provenance.clone()),
        )
        .unwrap();
        writer.write_skipped_slot(slot_start).unwrap();
        writer.write_skipped_slot(slot_start + 1).unwrap();
        let (mut bytes, _) = writer.finish().unwrap();
        fs::write(&path, &bytes).unwrap();
        verify_assembled_runtime_archive_range(&path, 1, slot_start, 2, &provenance).unwrap();

        let reader = jetstreamer_horizon::archive::ArchiveReader::open(std::io::Cursor::new(
            bytes.as_slice(),
        ))
        .unwrap();
        let first_bucket = reader.bucket_index()[0];
        let corrupt_at = usize::try_from(first_bucket.offset + first_bucket.len - 1).unwrap();
        bytes[corrupt_at] ^= 0x80;
        fs::write(&path, bytes).unwrap();
        let error = verify_assembled_runtime_archive_range(&path, 1, slot_start, 2, &provenance)
            .unwrap_err();
        assert!(error.contains("failed full decode"), "{error}");
    }

    #[test]
    fn multi_runtime_reuse_rejects_non_mainnet_genesis() {
        let wrong = Hash::new_from_array([0xff; 32]);
        let error = validate_multi_runtime_genesis(Path::new("epoch-1.jet"), wrong).unwrap_err();
        assert!(error.contains("expected registered mainnet genesis"));
        assert!(
            validate_multi_runtime_genesis(
                Path::new("epoch-1.jet"),
                compatibility::MAINNET_GENESIS_HASH.parse().unwrap(),
            )
            .is_ok()
        );
    }

    #[test]
    fn handoff_export_retry_stops_exporting_once_snapshot_is_ready() {
        let observations = [
            HandoffSnapshotReadiness::Missing,
            HandoffSnapshotReadiness::Ready,
        ];
        let export_by_attempt = observations.map(handoff_snapshot_needs_export);
        assert_eq!(export_by_attempt, [true, false]);
        assert!(handoff_snapshot_needs_export(
            HandoffSnapshotReadiness::Invalid
        ));
    }

    #[test]
    fn handoff_pair_quarantine_handles_every_partial_state() {
        for (archive_present, sidecar_present) in [(true, true), (true, false), (false, true)] {
            let directory = tempfile::TempDir::new().unwrap();
            let archive = directory.path().join("snapshot-1-hash.tar.bz2");
            let sidecar = handoff_snapshot_manifest_path(&archive).unwrap();
            if archive_present {
                fs::write(&archive, b"archive").unwrap();
            }
            if sidecar_present {
                fs::write(&sidecar, b"evidence").unwrap();
            }

            let moved = preserve_handoff_snapshot_pair(&archive).unwrap();
            assert_eq!(
                moved.len(),
                usize::from(archive_present) + usize::from(sidecar_present)
            );
            assert!(!path_exists_without_following(&archive).unwrap());
            assert!(!path_exists_without_following(&sidecar).unwrap());
            for (_, backup) in moved {
                assert!(path_exists_without_following(&backup).unwrap());
            }
        }
    }

    #[test]
    fn boundary_snapshot_pruning_removes_pairs_and_rejects_unknown_companions() {
        let directory = tempfile::TempDir::new().unwrap();
        let archive = directory.path().join("snapshot-1-hash.tar.bz2");
        let sidecar = handoff_snapshot_manifest_path(&archive).unwrap();
        fs::write(&archive, b"archive").unwrap();
        fs::write(&sidecar, b"evidence").unwrap();
        let moved = preserve_handoff_snapshot_pair(&archive).unwrap();
        let quarantine = moved[0].1.parent().unwrap().to_path_buf();
        remove_quarantined_handoff_snapshot_pair(moved).unwrap();
        assert!(!quarantine.exists());

        fs::write(&archive, b"archive").unwrap();
        fs::write(&sidecar, b"unregistered evidence").unwrap();
        let error = prune_epoch_boundary_snapshot(&archive, &compatibility::SOLANA_V1_0_8_RUNTIME)
            .unwrap_err();
        assert!(error.contains("unregistered handoff companion"));
        assert!(archive.is_file());
        assert!(sidecar.is_file());

        fs::remove_file(&sidecar).unwrap();
        prune_epoch_boundary_snapshot(&archive, &compatibility::SOLANA_V1_0_8_RUNTIME).unwrap();
        assert!(!archive.exists());
    }

    #[test]
    fn scheduler_accepts_slot_zero_as_its_first_slot() {
        let presence = Arc::new(SlotPresenceMap {
            start: 0,
            end_inclusive: 0,
            states: vec![SlotPresenceState::Present],
            next_present_after: vec![None],
        });
        let scheduler = TransactionScheduler::new(0, presence, Arc::new(RestartTracker::new()), 0);

        let ready = scheduler
            .record_block_metadata(0, 0, 0)
            .expect("slot zero must not be considered already finalized");
        assert!(ready.is_empty());
        let snapshot = scheduler.snapshot();
        assert_eq!(snapshot.current_slot, 1);
        assert_eq!(snapshot.last_finalized_slot, 0);
    }
}

#[cfg(test)]
mod scheduler_tests {
    use super::{EntryAccounts, SlotExecutionBuffer, assign_rounds};
    use solana_address::Address;
    use solana_transaction::versioned::VersionedTransaction;
    use solana_transaction_status::TransactionStatusMeta;
    use std::collections::HashSet;

    fn addr(byte: u8) -> Address {
        Address::new_from_array([byte; 32])
    }

    fn entry(writes: &[u8], reads: &[u8]) -> EntryAccounts {
        EntryAccounts {
            writes: writes.iter().map(|&b| addr(b)).collect(),
            reads: reads.iter().map(|&b| addr(b)).collect(),
        }
    }

    #[test]
    fn scheduler_preserves_missing_status_provenance() {
        let mut missing = SlotExecutionBuffer::default();
        missing
            .insert_transaction(0, VersionedTransaction::default(), None)
            .unwrap();
        assert!(missing.txs[0].as_ref().unwrap().expected_status.is_none());

        let mut observed = SlotExecutionBuffer::default();
        observed
            .insert_transaction(
                0,
                VersionedTransaction::default(),
                Some(TransactionStatusMeta::default()),
            )
            .unwrap();
        assert_eq!(
            observed.txs[0].as_ref().unwrap().expected_status,
            Some(Ok(()))
        );
    }

    /// Flattens rounds back to (entry_index -> round_index) for assertions.
    fn round_of(rounds: &[Vec<usize>]) -> std::collections::HashMap<usize, usize> {
        let mut map = std::collections::HashMap::new();
        for (r, members) in rounds.iter().enumerate() {
            for &i in members {
                map.insert(i, r);
            }
        }
        map
    }

    /// Every entry is scheduled exactly once, and round membership is ascending.
    fn check_partition(entries: &[EntryAccounts], rounds: &[Vec<usize>]) {
        let mut seen = HashSet::new();
        for members in rounds {
            assert!(
                members.windows(2).all(|w| w[0] < w[1]),
                "round not ascending"
            );
            for &i in members {
                assert!(seen.insert(i), "entry {i} scheduled twice");
            }
        }
        assert_eq!(seen.len(), entries.len(), "not all entries scheduled");
    }

    /// The two core invariants: (a) no two entries in the same round
    /// conflict; (b) any conflicting pair i<j has round(i) < round(j).
    fn check_invariants(entries: &[EntryAccounts], rounds: &[Vec<usize>]) {
        check_partition(entries, rounds);
        // (a) intra-round conflict-free.
        for members in rounds {
            for (a, &i) in members.iter().enumerate() {
                for &j in &members[a + 1..] {
                    assert!(
                        !entries[i].conflicts_with(&entries[j]),
                        "entries {i},{j} conflict but share a round"
                    );
                }
            }
        }
        // (b) conflicting pairs are strictly ordered across rounds.
        let r = round_of(rounds);
        for i in 0..entries.len() {
            for j in (i + 1)..entries.len() {
                if entries[i].conflicts_with(&entries[j]) {
                    assert!(
                        r[&i] < r[&j],
                        "conflicting {i}<{j} not ordered: round({i})={}, round({j})={}",
                        r[&i],
                        r[&j]
                    );
                }
            }
        }
    }

    #[test]
    fn all_independent_one_round() {
        let entries = vec![entry(&[1], &[]), entry(&[2], &[]), entry(&[3], &[])];
        let rounds = assign_rounds(&entries);
        assert_eq!(rounds.len(), 1, "independent entries should be one round");
        check_invariants(&entries, &rounds);
    }

    #[test]
    fn write_write_chain_serializes() {
        // All write X → must be N rounds in order.
        let entries = vec![entry(&[1], &[]), entry(&[1], &[]), entry(&[1], &[])];
        let rounds = assign_rounds(&entries);
        assert_eq!(rounds.len(), 3);
        check_invariants(&entries, &rounds);
    }

    #[test]
    fn readers_share_round_after_writer() {
        // A writes X; B,C read X. Expect round0=[A], round1=[B,C].
        let entries = vec![entry(&[1], &[]), entry(&[], &[1]), entry(&[], &[1])];
        let rounds = assign_rounds(&entries);
        check_invariants(&entries, &rounds);
        assert_eq!(rounds, vec![vec![0], vec![1, 2]]);
    }

    #[test]
    fn read_read_no_conflict() {
        let entries = vec![entry(&[], &[1]), entry(&[], &[1]), entry(&[], &[1])];
        let rounds = assign_rounds(&entries);
        assert_eq!(rounds.len(), 1, "shared reads must not serialize");
        check_invariants(&entries, &rounds);
    }

    #[test]
    fn independent_entry_jumps_ahead_of_conflicting_one() {
        // A writes X, B writes X (conflicts A), C writes Y (independent).
        // C should join A in round 0; B alone in round 1.
        let entries = vec![entry(&[1], &[]), entry(&[1], &[]), entry(&[2], &[])];
        let rounds = assign_rounds(&entries);
        check_invariants(&entries, &rounds);
        assert_eq!(rounds, vec![vec![0, 2], vec![1]]);
    }

    #[test]
    fn order_preserved_through_transitive_block() {
        // A writes X; B reads X (defer past A); C writes X (defer past B).
        // Hot-account chain → 3 ordered rounds.
        let entries = vec![entry(&[1], &[]), entry(&[], &[1]), entry(&[1], &[])];
        let rounds = assign_rounds(&entries);
        check_invariants(&entries, &rounds);
    }

    #[test]
    fn shared_read_then_late_writer_defers_correctly() {
        // A reads X, B reads X (round 0 with A), C writes X (must be after both).
        let entries = vec![entry(&[], &[1]), entry(&[], &[1]), entry(&[1], &[])];
        let rounds = assign_rounds(&entries);
        check_invariants(&entries, &rounds);
        assert_eq!(rounds[0], vec![0, 1]);
        assert_eq!(rounds[1], vec![2]);
    }

    #[test]
    fn mixed_realistic_pattern_holds_invariants() {
        // A spread of independent + hot-account entries; just assert the
        // invariants hold (exact partition is an implementation detail).
        let entries = vec![
            entry(&[1], &[10]),
            entry(&[2], &[10]),
            entry(&[1], &[]),
            entry(&[3], &[2]),
            entry(&[], &[10]),
            entry(&[2], &[1]),
            entry(&[4], &[]),
        ];
        let rounds = assign_rounds(&entries);
        check_invariants(&entries, &rounds);
    }

    #[test]
    fn empty_input() {
        let rounds = assign_rounds(&[]);
        assert!(rounds.is_empty());
    }
}
