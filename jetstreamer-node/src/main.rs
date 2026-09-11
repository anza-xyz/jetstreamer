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
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use cid::{Cid, multibase::Base};
use crc::{CRC_32_ISCSI, Crc};
use crossbeam_channel::{bounded, unbounded};
use dashmap::DashMap;
use jetstreamer_firehose::{
    epochs::{BASE_URL, epoch_to_slot_range, fetch_epoch_stream, slot_to_epoch},
    firehose::{
        BlockParentNotifier, FirehoseError, GeyserNotifiers, SourcedTransaction,
        SourcedTransactionNotifier, SourcedTransactionStatus,
        firehose_geyser_with_notifiers_and_block_parent,
    },
    node_reader::NodeReader,
};
use jetstreamer_horizon::archive::{
    AccountsHashKind, ArchiveFormatError, ArchiveProvenance, ArchiveProvenanceV1,
    ArchiveProvenanceV2, ArchiveProvenanceV3, ArchiveWriterConfig, BootstrapStateKind,
    BucketHeader as HorizonBucketHeader, Consumption, RuntimeAdmission, RuntimeHandoffProvenance,
    RuntimeSegmentProvenance, RuntimeSegmentSource, RuntimeStateCheckpoint, SemanticDigest,
    SlotKind, SlotVisitor, StateCommitment, StateCommitmentKind, TransactionMetadataPolicy,
    WriteVersionNormalization, merge_runtime_segments,
};
use jetstreamer_horizon::{
    account_updates::AccountUpdateView, block_metas::BlockNotification, entries::EntryRecord,
    epochs::EpochMeta, transactions::Transaction as HorizonTransaction,
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
    DEFAULT_BUCKET, download_exact_snapshot_generation,
    download_snapshot_at_or_before_slot_matching, list_snapshots_in_slot_range_matching,
};
use log::{error, info, warn};
use rayon::prelude::*;
use reqwest::{Client, Url, header::RANGE};
use serde::Deserialize;
use serde_cbor::Value;
use sha2::{Digest as _, Sha256};
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
use tokio::process::{Child, Command};
use xxhash_rust::xxh64::xxh64;

mod adaptive_epoch;
mod cohort_publication;
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
// This pins the canonical mainnet-beta raw serialization in addition to the
// semantic genesis hash; accepting merely equivalent bincode would reopen the
// gap between admission by the current runtime and decoding by an old worker.
const MAINNET_GENESIS_BIN_SIZE: u64 = 132_347;
const MAINNET_GENESIS_BIN_SHA256: [u8; 32] = [
    0x45, 0x29, 0x69, 0x98, 0xa6, 0xf8, 0xe2, 0xa7, 0x84, 0xdb, 0x5d, 0x9f, 0x95, 0xe1, 0x8f, 0xc2,
    0x3f, 0x70, 0x44, 0x1a, 0x10, 0x39, 0x44, 0x68, 0x01, 0x08, 0x98, 0x79, 0xb0, 0x8c, 0x7e, 0xf0,
];
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

        match entry_source_status_multiset(&entry.txs) {
            Ok(Some(source_statuses)) if !status_multisets_equal(&source_statuses, &results) => {
                let message = format!(
                    "transaction status multiset mismatch at slot {} entry {}: source {:?}, replay {:?}",
                    entry.slot, entry.entry_index, source_statuses, results
                );
                self.failure.record(message.clone());
                panic!("{message}");
            }
            Ok(_) => {}
            Err(error) => {
                let message = format!(
                    "incomplete transaction status multiset at slot {} entry {}: {error}",
                    entry.slot, entry.entry_index
                );
                self.failure.record(message.clone());
                panic!("{message}");
            }
        }

        for (offset, actual) in results.into_iter().enumerate() {
            let Some(expected) = entry.txs[offset].expected_status.clone() else {
                // Missing and misassociated source statuses both defer to
                // replay. Do not reinterpret TransactionStatusMeta::default()
                // as observed success.
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
    fn take_historical_client(
        &self,
    ) -> Result<Option<historical::HistoricalRuntimeClient>, String> {
        Ok(None)
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

    fn take_historical_client(
        &self,
    ) -> Result<Option<historical::HistoricalRuntimeClient>, String> {
        historical_replay::HistoricalReplay::take_client(self).map(Some)
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CanonicalSlotState {
    Unknown,
    RequiredPresent,
    ProvenSkipped,
}

#[derive(Debug)]
struct SchedulerState {
    last_finalized_slot: Slot,
    has_finalized_slot: bool,
    current_slot: Slot,
    slots: HashMap<Slot, SlotExecutionBuffer>,
    inferred_blocks: HashMap<Slot, (u64, u64)>,
    /// Canonical state derived only from decoded blocks and parent edges. The
    /// indexes remain useful for seeking, but never populate this table.
    canonical_slots: Vec<CanonicalSlotState>,
    /// Parent slots from decoded block nodes, retained to reject conflicting
    /// metadata when a firehose retry re-delivers a block.
    decoded_parents: Vec<Option<Slot>>,
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
        let canonical_slot_count = presence.states.len();
        Self {
            state: Mutex::new(SchedulerState {
                last_finalized_slot: start_slot.saturating_sub(1),
                has_finalized_slot: start_slot > 0,
                current_slot: start_slot,
                slots: HashMap::new(),
                inferred_blocks: HashMap::new(),
                canonical_slots: vec![CanonicalSlotState::Unknown; canonical_slot_count],
                decoded_parents: vec![None; canonical_slot_count],
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
        match self.canonical_slot_state_locked(state, slot)? {
            CanonicalSlotState::RequiredPresent => Some(SlotPresenceState::Present),
            CanonicalSlotState::ProvenSkipped => Some(SlotPresenceState::Missing),
            CanonicalSlotState::Unknown => self.presence.state(slot),
        }
    }

    fn canonical_slot_index(&self, slot: Slot) -> Option<usize> {
        if slot < self.presence.start || slot > self.presence.end_inclusive {
            return None;
        }
        usize::try_from(slot - self.presence.start).ok()
    }

    fn canonical_slot_state_locked(
        &self,
        state: &SchedulerState,
        slot: Slot,
    ) -> Option<CanonicalSlotState> {
        let index = self.canonical_slot_index(slot)?;
        state.canonical_slots.get(index).copied()
    }

    fn record_present_evidence_locked(
        &self,
        state: &mut SchedulerState,
        slot: Slot,
        source: &str,
    ) -> Result<(), String> {
        let index = self
            .canonical_slot_index(slot)
            .ok_or_else(|| format!("{source} is outside the replay range at slot {slot}"))?;
        match state.canonical_slots[index] {
            CanonicalSlotState::Unknown => {
                state.canonical_slots[index] = CanonicalSlotState::RequiredPresent;
                Ok(())
            }
            CanonicalSlotState::RequiredPresent => Ok(()),
            CanonicalSlotState::ProvenSkipped => Err(format!(
                "{source} proves slot {slot} contains a block, but a decoded parent edge proved it skipped"
            )),
        }
    }

    /// Applies the canonical gap implied by a decoded block edge. Indexes are
    /// only seek hints: a block at `slot` with parent `parent_slot` proves every
    /// intervening slot was skipped and proves its parent contained a block.
    fn record_parent_edge_locked(
        &self,
        state: &mut SchedulerState,
        parent_slot: Slot,
        slot: Slot,
    ) -> Result<(), String> {
        if slot == 0 {
            if parent_slot != 0 {
                return Err(format!(
                    "genesis block at slot 0 has invalid parent slot {parent_slot}"
                ));
            }
            let index = self
                .canonical_slot_index(slot)
                .ok_or_else(|| "genesis block is outside the replay range".to_string())?;
            if state.canonical_slots[index] == CanonicalSlotState::ProvenSkipped {
                return Err("decoded genesis block was previously proven skipped".to_string());
            }
            if state.decoded_parents[index].is_some_and(|parent| parent != 0) {
                return Err("decoded genesis block has conflicting parent metadata".to_string());
            }
            state.canonical_slots[index] = CanonicalSlotState::RequiredPresent;
            state.decoded_parents[index] = Some(0);
            return Ok(());
        }
        if parent_slot >= slot {
            return Err(format!(
                "block metadata for slot {slot} has non-preceding parent slot {parent_slot}"
            ));
        }

        let child_index = self.canonical_slot_index(slot);
        let parent_index = self.canonical_slot_index(parent_slot);

        if let Some(index) = child_index {
            if state.canonical_slots[index] == CanonicalSlotState::ProvenSkipped {
                return Err(format!(
                    "decoded block metadata proves slot {slot} present after a parent edge proved it skipped"
                ));
            }
            if let Some(existing_parent) = state.decoded_parents[index]
                && existing_parent != parent_slot
            {
                return Err(format!(
                    "conflicting parent metadata for slot {slot}: first {existing_parent}, then {parent_slot}"
                ));
            }
        } else if slot <= self.presence.end_inclusive {
            return Err(format!(
                "decoded block slot {slot} precedes replay range start {}",
                self.presence.start
            ));
        }

        if let Some(index) = parent_index {
            if state.canonical_slots[index] == CanonicalSlotState::ProvenSkipped {
                return Err(format!(
                    "block metadata for slot {slot} names slot {parent_slot} as its parent after another parent edge proved it skipped"
                ));
            }
            if parent_slot < state.current_slot
                && state.canonical_slots[index] != CanonicalSlotState::RequiredPresent
            {
                return Err(format!(
                    "block metadata for slot {slot} names slot {parent_slot} as its parent after that slot was finalized without block data"
                ));
            }
        }

        let gap_start = parent_slot.saturating_add(1).max(self.presence.start);
        let gap_end = slot.min(self.presence.end_inclusive.saturating_add(1));

        for skipped in gap_start..gap_end {
            let index = self
                .canonical_slot_index(skipped)
                .expect("parent gap clipped to scheduler range");
            let buffered = state
                .slots
                .get(&skipped)
                .is_some_and(SlotExecutionBuffer::has_any_data);
            if state.canonical_slots[index] == CanonicalSlotState::RequiredPresent || buffered {
                return Err(format!(
                    "block metadata edge {parent_slot}->{slot} marks slot {skipped} skipped, but decoded block data proves it present"
                ));
            }
            if skipped < state.current_slot
                && state.canonical_slots[index] != CanonicalSlotState::ProvenSkipped
            {
                return Err(format!(
                    "block metadata edge {parent_slot}->{slot} marks already-finalized slot {skipped} skipped without prior parent proof"
                ));
            }
        }

        let mut index_overrides = 0usize;
        let mut newly_proven = Vec::new();
        if let Some(index) = child_index {
            state.canonical_slots[index] = CanonicalSlotState::RequiredPresent;
            state.decoded_parents[index] = Some(parent_slot);
        }
        if let Some(index) = parent_index {
            state.canonical_slots[index] = CanonicalSlotState::RequiredPresent;
        }
        for skipped in gap_start..gap_end {
            let index = self
                .canonical_slot_index(skipped)
                .expect("parent gap clipped to scheduler range");
            if state.canonical_slots[index] != CanonicalSlotState::ProvenSkipped {
                state.canonical_slots[index] = CanonicalSlotState::ProvenSkipped;
                newly_proven.push(skipped);
                if self.presence.state(skipped) == Some(SlotPresenceState::Present) {
                    index_overrides = index_overrides.saturating_add(1);
                }
            }
        }
        if child_index.is_some() {
            state.highest_seen_slot = state.highest_seen_slot.max(slot);
        }
        for skipped in newly_proven {
            horizon::note_chain_confirmed_skipped(skipped);
        }
        if index_overrides > 0 {
            warn!(
                "block metadata edge {}->{} proved {} index-present slot(s) skipped",
                parent_slot, slot, index_overrides
            );
        }
        Ok(())
    }

    fn record_block_parent(
        &self,
        parent_slot: Slot,
        slot: Slot,
    ) -> Result<Vec<ReadyEntry>, String> {
        let mut state = self.state.lock().expect("transaction scheduler lock");
        self.record_parent_edge_locked(&mut state, parent_slot, slot)?;
        self.advance_ready_locked(&mut state)
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
        self.record_present_evidence_locked(&mut state, slot, "transaction data")?;
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
        let inserted = buffer.insert_transaction(
            index,
            tx,
            status_meta,
            compatibility::transaction_status_validation_at(slot),
        )?;
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
        self.record_present_evidence_locked(&mut state, slot, "entry data")?;
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
        parent_slot: Slot,
        slot: Slot,
        expected_tx_count: u64,
        expected_entry_count: u64,
    ) -> Result<Vec<ReadyEntry>, String> {
        let mut state = self.state.lock().expect("transaction scheduler lock");
        self.record_parent_edge_locked(&mut state, parent_slot, slot)?;
        self.record_present_evidence_locked(&mut state, slot, "block metadata")?;
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

    fn verify_complete(&self, end_inclusive: Slot) -> Result<(), String> {
        let state = self.state.lock().expect("transaction scheduler lock");
        let mut slot = state.current_slot;
        while slot <= end_inclusive {
            match self.canonical_slot_state_locked(&state, slot) {
                Some(CanonicalSlotState::ProvenSkipped) => slot = slot.saturating_add(1),
                Some(CanonicalSlotState::RequiredPresent) => {
                    return Err(format!(
                        "replay incomplete: decoded chain evidence requires block data for slot {slot}"
                    ));
                }
                Some(CanonicalSlotState::Unknown) => {
                    return Err(format!(
                        "replay incomplete: slot {slot} has no decoded block or parent-edge evidence (index hint: {:?})",
                        self.presence.state(slot)
                    ));
                }
                None => break,
            }
        }

        if let Some(slot) = state
            .slots
            .iter()
            .filter(|(_, buffer)| buffer.has_any_data())
            .map(|(slot, _)| *slot)
            .min()
        {
            if slot <= end_inclusive {
                return Err(format!(
                    "replay incomplete: slot {slot} still has buffered data"
                ));
            }
            return Err(format!(
                "replay received data for slot {slot} beyond end slot {end_inclusive}"
            ));
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
            match self.canonical_slot_state_locked(&state, slot) {
                Some(CanonicalSlotState::ProvenSkipped) => slot = slot.saturating_add(1),
                Some(CanonicalSlotState::RequiredPresent | CanonicalSlotState::Unknown) => {
                    missing = Some(IncompleteSlotInfo {
                        slot,
                        entry_index: 0,
                        tx_start: 0,
                        reason: IncompleteReason::MissingBlockData,
                        snapshot: None,
                        presence: self.slot_presence_locked(&state, slot),
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
            match self.canonical_slot_state_locked(state, current_slot) {
                Some(CanonicalSlotState::ProvenSkipped) => {
                    if let Some(buffer) = state.slots.remove(&current_slot)
                        && buffer.has_any_data()
                    {
                        return Err(format!(
                            "slot {} is proven skipped but contains buffered data",
                            current_slot
                        ));
                    }
                    state.last_finalized_slot = current_slot;
                    state.has_finalized_slot = true;
                    state.current_slot = current_slot.saturating_add(1);
                    continue;
                }
                Some(CanonicalSlotState::RequiredPresent) => {}
                Some(CanonicalSlotState::Unknown) | None => break,
            }

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
        status_validation: compatibility::TransactionStatusValidation,
    ) -> Result<bool, String> {
        if (index as u64) < self.processed_tx_count {
            // Duplicate transaction after a firehose restart; already processed.
            return Ok(false);
        }
        if self.txs.len() <= index {
            self.txs.resize_with(index + 1, || None);
        }
        let source_status = status_meta.as_ref().map(|metadata| metadata.status.clone());
        let (expected_status, source_entry_status) = match status_validation {
            compatibility::TransactionStatusValidation::RuntimeOnly => (None, None),
            compatibility::TransactionStatusValidation::RuntimeWithSourceEntryMultiset => {
                let status = source_status.clone().ok_or_else(|| {
                    "entry-multiset status validation requires source metadata".to_string()
                })?;
                (None, Some(status))
            }
            compatibility::TransactionStatusValidation::SourceExact => {
                let status = source_status.clone().ok_or_else(|| {
                    "exact transaction status validation requires source metadata".to_string()
                })?;
                (Some(status), None)
            }
        };
        if let Some(existing) = &self.txs[index] {
            let existing_sig = existing.tx.signatures.first();
            let incoming_sig = tx.signatures.first();
            if existing_sig == incoming_sig
                && existing.expected_status == expected_status
                && existing.source_entry_status == source_entry_status
            {
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
            source_entry_status,
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
    /// Individually associated source status. `None` means replay supplies the
    /// transaction's status.
    expected_status: Option<Result<(), TransactionError>>,
    /// Source status retained only for an entry-wide multiset check. Solana
    /// v1.0 persisted these results in randomized rather than transaction
    /// order, so replay restores their per-transaction association.
    source_entry_status: Option<Result<(), TransactionError>>,
    /// Full original chain metadata (from the CAR stream), carried through
    /// so the horizon recorder can archive it alongside replay output.
    status_meta: TransactionStatusMeta,
}

fn entry_source_status_multiset(
    transactions: &[ScheduledTransaction],
) -> Result<Option<Vec<Result<(), TransactionError>>>, String> {
    let status_count = transactions
        .iter()
        .filter(|transaction| transaction.source_entry_status.is_some())
        .count();
    if status_count == 0 {
        return Ok(None);
    }
    if status_count != transactions.len() {
        return Err(format!(
            "entry has source status for {status_count}/{} transactions",
            transactions.len()
        ));
    }
    Ok(Some(
        transactions
            .iter()
            .map(|transaction| {
                transaction
                    .source_entry_status
                    .clone()
                    .expect("complete source-status multiset checked above")
            })
            .collect(),
    ))
}

fn status_multisets_equal<T: PartialEq>(left: &[T], right: &[T]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    let mut matched = vec![false; right.len()];
    left.iter().all(|left_status| {
        let Some(index) = right
            .iter()
            .enumerate()
            .position(|(index, right_status)| !matched[index] && left_status == right_status)
        else {
            return false;
        };
        matched[index] = true;
        true
    })
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

struct BankBlockParentNotifier {
    scheduler: Arc<TransactionScheduler>,
    failure: Arc<ReplayFailure>,
    ready_sender: crossbeam_channel::Sender<Vec<ReadyEntry>>,
    firehose_gate: Arc<Mutex<()>>,
}

impl BlockParentNotifier for BankBlockParentNotifier {
    fn notify_block_parent(&self, parent_slot: u64, slot: u64) {
        let _firehose_guard = self
            .firehose_gate
            .lock()
            .expect("firehose gate lock poisoned");
        match self.scheduler.record_block_parent(parent_slot, slot) {
            Ok(ready_entries) => {
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

/// Owns every notifier that can enqueue replay work.
///
/// Keeping these producers in one value makes channel shutdown structural:
/// dropping the bundle releases the block-parent sender together with the
/// ordinary Geyser senders before the consumer thread is joined.
struct ReadyEntryProducers<T, E, P, M> {
    transaction: T,
    entry: E,
    block_parent: P,
    block_metadata: M,
}

type ReplayReadyEntryProducers = ReadyEntryProducers<
    Arc<BankTransactionNotifier>,
    Arc<BankEntryNotifier>,
    Arc<BankBlockParentNotifier>,
    Arc<BankBlockMetadataNotifier>,
>;

impl ReplayReadyEntryProducers {
    fn geyser_notifiers(&self) -> GeyserNotifiers {
        GeyserNotifiers {
            transaction_notifier: None,
            sourced_transaction_notifier: Some(self.transaction.clone()),
            entry_notifier: Some(self.entry.clone()),
            block_metadata_notifier: Some(self.block_metadata.clone()),
        }
    }
}

fn close_ready_entry_channel<N, P, T>(
    notifiers: N,
    producers: P,
    sender: crossbeam_channel::Sender<T>,
    consumer: std::thread::JoinHandle<()>,
) -> std::thread::Result<()> {
    drop(notifiers);
    drop(producers);
    drop(sender);
    consumer.join()
}

impl BlockMetadataNotifier for BankBlockMetadataNotifier {
    fn notify_block_metadata(
        &self,
        parent_slot: u64,
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
        match self.scheduler.record_block_metadata(
            parent_slot,
            slot,
            executed_transaction_count,
            entry_count,
        ) {
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
                    parent_slot,
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
         \x20      [--qualification-end-slot=SLOT] [--root-checkpoint-cohort]\n\
         \x20      [--cohort-manifest=PATH --cohort-manifest-fingerprint=sha256:HEX]\n\
         \x20      [--recover-staged-only]\n\
         \n\
         <epoch|range> is a single epoch (950) or an inclusive range (950-955).\n\
         Replays each epoch and writes a horizon archive to <dest-dir>/epoch-<N>.jet.\n\
         Epoch 0 starts from the validated mainnet genesis; later epochs use a\n\
         predecessor snapshot. A range pre-downloads those boundary snapshots\n\
         and snapshot hashes up front (the only gcloud/GCS access), then runs each\n\
         epoch in its own child process so memory is released at each boundary.\n\
         Set JETSTREAMER_EPOCH_ISOLATION=0 to restore single-process chaining (the\n\
         bank stays in memory across epochs; no per-epoch snapshot reload).\n\
         JETSTREAMER_EPOCH_ATTEMPTS (default 2) bounds retries of a crashed epoch;\n\
         JETSTREAMER_ADAPTIVE_EPOCH_CONCURRENCY=yes explicitly enables RAM/CPU-gated\n\
         isolated epoch fanout (default off; finite inherited cgroup memory.max\n\
         is required above one child; Agave directory loading remains serial).\n\
         Parallel fanout also requires explicit, measured bounds via\n\
         JETSTREAMER_EPOCH_MEMORY_RESERVATION_GIB and\n\
         JETSTREAMER_EPOCH_DISK_RESERVATION_GIB, each acknowledged with its\n\
         matching *_QUALIFIED=yes flag, plus explicit runtime thread counts.\n\
         JETSTREAMER_PROTECTED_MEMORY_GIB defaults to 400;\n\
         JETSTREAMER_ADAPTIVE_EPOCH_MAX is capped at 3. Private attempts and\n\
         leases live under the owner-only JETSTREAMER_PRIVATE_RUN_ROOT, or an\n\
         owner-only default beside the destination.\n\
         JETSTREAMER_PRUNE_EPOCH_SNAPSHOTS=0 keeps each boundary snapshot archive\n\
         after its epoch finalizes (default: kept). Set it to 1 to reclaim disk.\n\
         --horizon-output applies only to a single epoch.\n\
         --qualification-end-slot runs a focused qualification from an explicit\n\
         snapshot in the target or preceding epoch through SLOT. Recording starts\n\
         at the later of the target epoch boundary and the snapshot successor. It\n\
         requires a single epoch, --verify,\n\
         --snapshot-archive, --epoch-hashes, and --horizon-output.\n\
         --epoch-hashes=PATH, --snapshot-archive=PATH, --range-info=A-B, and\n\
         --replay-scratch=PATH are\n\
         otherwise internal flags passed by the range supervisor to its children.\n\
         --root-checkpoint-cohort runs a manifest-defined nonzero epoch cohort from one\n\
         predecessor root snapshot through a root checkpoint in the final epoch. It requires\n\
         explicit --verify, a sealed preflight manifest and its audited fingerprint,\n\
         one unchanged historical runtime; ranges use in-memory state handoff.\n\
         Every archive remains in owner-only staging until the complete cohort\n\
         and every staged archive pass validation.\n\
         --recover-staged-only accepts one nonzero epoch and explicit --verify.\n\
         It fully validates and transactionally publishes an existing private\n\
         adaptive staging candidate, but never starts replay or removes a failed\n\
         candidate."
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

/// Durable state used to initialize the first runtime in a replay process.
/// Execution-version selection remains exclusively slot-derived; this value
/// chooses only the matching state loader after that selection is complete.
#[derive(Clone, Debug)]
enum ReplayBootstrap {
    Genesis {
        genesis_bin_path: PathBuf,
        identity: historical::GenesisFileIdentity,
        /// Keeps the admitted private genesis copy alive for every clone that
        /// may initialize an in-process historical runtime.
        _private_dir: Arc<tempfile::TempDir>,
    },
    SnapshotArchive(PathBuf),
}

impl ReplayBootstrap {
    fn snapshot_archive(&self) -> Option<&Path> {
        match self {
            Self::Genesis { .. } => None,
            Self::SnapshotArchive(path) => Some(path),
        }
    }

    fn slot(&self) -> Result<Slot, String> {
        match self {
            Self::Genesis { .. } => Ok(0),
            Self::SnapshotArchive(path) => {
                let name = path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .ok_or_else(|| {
                        format!("snapshot path has no UTF-8 filename: {}", path.display())
                    })?;
                parse_snapshot_archive_name(name).map(|(slot, _)| slot)
            }
        }
    }
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
    let mut previous_descriptor: Option<&'static compatibility::RuntimeDescriptor> = None;
    for epoch in start_epoch..=end_epoch {
        let (slot_start, slot_end_inclusive) = epoch_to_slot_range(epoch);
        let spans = compatibility::plan_runtime_spans(
            slot_start..slot_end_inclusive.saturating_add(1),
            allow_candidate_runtime,
        )?;
        if spans.len() > 1 {
            return Ok((true, Some(epoch)));
        }
        let selection = runtime_span_selection(
            spans
                .first()
                .expect("runtime planner rejects an empty epoch range"),
        )?;
        if previous_descriptor.is_some_and(|previous| !std::ptr::eq(previous, selection.descriptor))
        {
            // Historical executors cannot carry a typed Bank across process
            // or release boundaries. A fresh verified predecessor-epoch
            // snapshot is therefore mandatory even when the switch happens
            // exactly between two output epochs.
            return Ok((true, Some(epoch)));
        }
        previous_descriptor = Some(selection.descriptor);
    }
    Ok((false, None))
}

/// Keeps the operator-only staged recovery route deliberately narrower than a
/// normal replay invocation. Environment defaults cannot supply the
/// verification opt-in, and no child, cohort, or output override may redirect
/// the artifact being admitted.
fn validate_staged_recovery_mode(
    enabled: bool,
    start_epoch: u64,
    end_epoch: u64,
    explicit_verify: Option<bool>,
    verify_option_count: usize,
    inherited_epoch_lease: bool,
    has_conflicting_override: bool,
) -> Result<(), String> {
    if !enabled {
        return Ok(());
    }
    if start_epoch != end_epoch || start_epoch == 0 {
        return Err("--recover-staged-only requires exactly one nonzero epoch".to_string());
    }
    if explicit_verify != Some(true) || verify_option_count != 1 {
        return Err(
            "--recover-staged-only requires exactly one explicit --verify (environment defaults, --no-verify, and duplicate verification options do not qualify)"
                .to_string(),
        );
    }
    if inherited_epoch_lease {
        return Err("--recover-staged-only is valid only in a top-level invocation".to_string());
    }
    if has_conflicting_override {
        return Err(
            "--recover-staged-only cannot be combined with cohort, output, qualification, or internal child overrides"
                .to_string(),
        );
    }
    Ok(())
}

/// Proves that an epoch range can be replayed as one uninterrupted
/// root-checkpoint cohort. Every member must use the same runtime descriptor.
/// Multi-epoch cohorts additionally require that descriptor's live state
/// handoff support.
fn root_checkpoint_cohort_runtime(
    start_epoch: u64,
    end_epoch: u64,
    allow_candidate_runtime: bool,
) -> Result<compatibility::RuntimeSelection, String> {
    if start_epoch == 0 || start_epoch > end_epoch {
        return Err("a root-checkpoint cohort requires one or more nonzero epochs".to_string());
    }
    let mut cohort_selection: Option<compatibility::RuntimeSelection> = None;
    for epoch in start_epoch..=end_epoch {
        let (start, end_inclusive) = epoch_to_slot_range(epoch);
        let spans = compatibility::plan_runtime_spans(
            start..end_inclusive.saturating_add(1),
            allow_candidate_runtime,
        )?;
        if spans.len() != 1 {
            return Err(format!(
                "root-checkpoint cohort epoch {epoch} crosses a runtime boundary"
            ));
        }
        let selection = runtime_span_selection(&spans[0])?;
        if selection.backend == compatibility::RuntimeBackend::AgaveV3
            || selection.descriptor.worker.is_none()
            || selection.descriptor.bootstrap.loader
                != compatibility::BootstrapStateLoader::HistoricalWorkerSnapshotArchive
        {
            return Err(format!(
                "root-checkpoint cohorts require an isolated historical Solana worker; epoch {epoch} selected {}",
                selection.descriptor.identity.name
            ));
        }
        if let Some(first) = cohort_selection
            && !std::ptr::eq(first.descriptor, selection.descriptor)
        {
            return Err(format!(
                "root-checkpoint cohort changes runtime at epoch {epoch}: {} to {}",
                first.backend, selection.backend
            ));
        }
        if start_epoch < end_epoch && !selection.descriptor.permits_live_epoch_handoff() {
            return Err(format!(
                "runtime profile {} does not permit the live state handoff required by a root-checkpoint cohort",
                selection.descriptor.identity.name
            ));
        }
        cohort_selection = Some(selection);
    }
    Ok(cohort_selection.expect("nonempty cohort has a runtime"))
}

const COHORT_MANIFEST_SCHEMA: &str = "jetstreamer-gcs-snapshot-preflight-v2";
const COHORT_PUBLICATION_GATE: &str = "all-archives-validated-and-final-root-verified";
const COHORT_MANIFEST_MAX_BYTES: u64 = 16 * 1024 * 1024;

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CohortManifestSnapshot {
    accounts_hash: String,
    anchor_slot: Slot,
    crc32c: String,
    extension: String,
    generation: u64,
    size: u64,
    slot: Slot,
    source: String,
    uri: String,
    versioned_uri: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CohortManifestEntry {
    accepted_extensions: Vec<String>,
    bootstrap: CohortManifestSnapshot,
    first_epoch: u64,
    last_epoch: u64,
    publication_gate: String,
    root_checkpoints: Vec<CohortManifestSnapshot>,
    runtime: String,
}

#[derive(Debug, Deserialize)]
struct CohortManifestBody {
    bucket: String,
    epoch_slots: u64,
    first_epoch: Option<u64>,
    last_epoch: Option<u64>,
    schema: String,
    verification_cohorts: Vec<CohortManifestEntry>,
}

#[derive(Debug, Deserialize)]
struct CohortManifestReport {
    manifest: serde_json::Value,
    manifest_fingerprint: String,
}

#[derive(Clone, Debug)]
struct RootCheckpointCohortPlan {
    fingerprint: String,
    bootstrap: CohortManifestSnapshot,
    root_checkpoints: Vec<CohortManifestSnapshot>,
}

fn canonical_json(value: &serde_json::Value, output: &mut Vec<u8>) -> Result<(), String> {
    match value {
        serde_json::Value::Null => output.extend_from_slice(b"null"),
        serde_json::Value::Bool(value) => {
            output.extend_from_slice(if *value { b"true" } else { b"false" })
        }
        serde_json::Value::Number(value) => {
            if !value.is_i64() && !value.is_u64() {
                return Err("cohort manifest contains a non-integer JSON number".to_string());
            }
            output.extend_from_slice(value.to_string().as_bytes());
        }
        serde_json::Value::String(value) => {
            if !value.is_ascii() {
                return Err("cohort manifest contains a non-ASCII string".to_string());
            }
            serde_json::to_writer(output, value)
                .map_err(|error| format!("failed to canonicalize manifest string: {error}"))?;
        }
        serde_json::Value::Array(values) => {
            output.push(b'[');
            for (index, value) in values.iter().enumerate() {
                if index != 0 {
                    output.push(b',');
                }
                canonical_json(value, output)?;
            }
            output.push(b']');
        }
        serde_json::Value::Object(values) => {
            output.push(b'{');
            let mut entries: Vec<_> = values.iter().collect();
            entries.sort_unstable_by(|left, right| left.0.cmp(right.0));
            for (index, (key, value)) in entries.into_iter().enumerate() {
                if !key.is_ascii() {
                    return Err("cohort manifest contains a non-ASCII object key".to_string());
                }
                if index != 0 {
                    output.push(b',');
                }
                serde_json::to_writer(&mut *output, key).map_err(|error| {
                    format!("failed to canonicalize manifest object key: {error}")
                })?;
                output.push(b':');
                canonical_json(value, output)?;
            }
            output.push(b'}');
        }
    }
    Ok(())
}

fn cohort_manifest_fingerprint(manifest: &serde_json::Value) -> Result<String, String> {
    let mut canonical = Vec::new();
    canonical_json(manifest, &mut canonical)?;
    Ok(format!(
        "sha256:{}",
        jetstreamer_node::segment_manifest::sha256_hex_string(&Sha256::digest(canonical).into())
    ))
}

fn validate_manifest_fingerprint(value: &str) -> Result<(), String> {
    let Some(hex) = value.strip_prefix("sha256:") else {
        return Err("cohort manifest fingerprint must start with sha256:".to_string());
    };
    if hex.len() != 64
        || !hex
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(
            "cohort manifest fingerprint must contain 64 lowercase hexadecimal digits".to_string(),
        );
    }
    Ok(())
}

fn manifest_fingerprint_digest(value: &str) -> Result<[u8; 32], String> {
    validate_manifest_fingerprint(value)?;
    let hex = value
        .strip_prefix("sha256:")
        .expect("validated manifest fingerprint has its algorithm prefix");
    let mut digest = [0u8; 32];
    for (index, byte) in digest.iter_mut().enumerate() {
        let offset = index * 2;
        *byte = u8::from_str_radix(&hex[offset..offset + 2], 16)
            .map_err(|error| format!("invalid cohort manifest fingerprint: {error}"))?;
    }
    Ok(digest)
}

fn validate_cohort_manifest_snapshot(
    item: &CohortManifestSnapshot,
    required_source: &str,
    accepted_extensions: &[&str],
) -> Result<(String, Hash), String> {
    if item.source != required_source {
        return Err(format!(
            "cohort manifest object {} has source {}, expected {required_source}",
            item.versioned_uri, item.source
        ));
    }
    if item.size == 0 || item.generation == 0 {
        return Err(format!(
            "cohort manifest object {} has an empty size or generation",
            item.versioned_uri
        ));
    }
    if item.anchor_slot != item.slot {
        return Err(format!(
            "root checkpoint {} has anchor slot {}, expected {}",
            item.versioned_uri, item.anchor_slot, item.slot
        ));
    }
    if !accepted_extensions
        .iter()
        .any(|extension| *extension == item.extension)
    {
        return Err(format!(
            "cohort manifest object {} has runtime-incompatible extension {}",
            item.versioned_uri, item.extension
        ));
    }
    let filename = snapshot_filename(&item.uri)?.to_owned();
    let expected_uri = format!("{DEFAULT_BUCKET}/{}/{}", item.anchor_slot, filename);
    if item.uri != expected_uri || item.versioned_uri != format!("{}#{}", item.uri, item.generation)
    {
        return Err(format!(
            "cohort manifest object URI or generation is not canonical: {}",
            item.versioned_uri
        ));
    }
    let (filename_slot, filename_hash) = parse_snapshot_archive_name(&filename)?;
    let accounts_hash = item.accounts_hash.parse::<Hash>().map_err(|error| {
        format!(
            "cohort manifest object {} has invalid accounts hash: {error}",
            item.versioned_uri
        )
    })?;
    if filename_slot != item.slot || filename_hash.0 != accounts_hash {
        return Err(format!(
            "cohort manifest object {} disagrees with its snapshot filename",
            item.versioned_uri
        ));
    }
    let decoded_crc = BASE64_STANDARD.decode(&item.crc32c).map_err(|error| {
        format!(
            "cohort manifest object {} has invalid CRC32C: {error}",
            item.versioned_uri
        )
    })?;
    if decoded_crc.len() != 4 {
        return Err(format!(
            "cohort manifest object {} CRC32C does not decode to four bytes",
            item.versioned_uri
        ));
    }
    Ok((filename, accounts_hash))
}

fn root_checkpoint_cohort_plan_from_report(
    report_value: serde_json::Value,
    expected_fingerprint: &str,
    start_epoch: u64,
    end_epoch: u64,
    selection: compatibility::RuntimeSelection,
) -> Result<RootCheckpointCohortPlan, String> {
    if selection.backend == compatibility::RuntimeBackend::AgaveV3
        || selection.descriptor.worker.is_none()
        || selection.descriptor.bootstrap.loader
            != compatibility::BootstrapStateLoader::HistoricalWorkerSnapshotArchive
    {
        return Err(
            "root-checkpoint cohort manifest selected a non-historical runtime".to_string(),
        );
    }
    validate_manifest_fingerprint(expected_fingerprint)?;
    let report: CohortManifestReport = serde_json::from_value(report_value)
        .map_err(|error| format!("invalid cohort preflight report: {error}"))?;
    validate_manifest_fingerprint(&report.manifest_fingerprint)?;
    let actual_fingerprint = cohort_manifest_fingerprint(&report.manifest)?;
    if actual_fingerprint != report.manifest_fingerprint
        || actual_fingerprint != expected_fingerprint
    {
        return Err(format!(
            "cohort manifest fingerprint mismatch: expected {expected_fingerprint}, embedded {}, computed {actual_fingerprint}",
            report.manifest_fingerprint
        ));
    }
    let body: CohortManifestBody = serde_json::from_value(report.manifest)
        .map_err(|error| format!("invalid cohort manifest: {error}"))?;
    if body.schema != COHORT_MANIFEST_SCHEMA
        || body.bucket != DEFAULT_BUCKET
        || body.epoch_slots != 432_000
    {
        return Err(format!(
            "cohort manifest registry identity is incompatible (schema={}, bucket={}, epoch_slots={})",
            body.schema, body.bucket, body.epoch_slots
        ));
    }
    if body.first_epoch.is_none_or(|first| first > start_epoch)
        || body.last_epoch.is_none_or(|last| last < end_epoch)
    {
        return Err(format!(
            "cohort manifest does not cover requested epochs {start_epoch}-{end_epoch}"
        ));
    }
    let overlapping: Vec<_> = body
        .verification_cohorts
        .into_iter()
        .filter(|entry| entry.first_epoch <= end_epoch && start_epoch <= entry.last_epoch)
        .collect();
    if overlapping.len() != 1
        || overlapping[0].first_epoch != start_epoch
        || overlapping[0].last_epoch != end_epoch
    {
        return Err(format!(
            "cohort manifest must contain exactly one whole entry for epochs {start_epoch}-{end_epoch}"
        ));
    }
    let entry = overlapping
        .into_iter()
        .next()
        .expect("one overlap was required");
    if entry.runtime != selection.descriptor.identity.name {
        return Err(format!(
            "cohort manifest runtime {} does not match selected runtime {}",
            entry.runtime, selection.descriptor.identity.name
        ));
    }
    if entry.publication_gate != COHORT_PUBLICATION_GATE {
        return Err(format!(
            "cohort manifest has unsupported publication gate {}",
            entry.publication_gate
        ));
    }
    let manifest_extensions: HashSet<_> = entry
        .accepted_extensions
        .iter()
        .map(String::as_str)
        .collect();
    let runtime_extensions: HashSet<_> = selection
        .descriptor
        .bootstrap
        .archive_extensions
        .iter()
        .copied()
        .collect();
    if manifest_extensions != runtime_extensions
        || entry.accepted_extensions.len() != manifest_extensions.len()
    {
        return Err(
            "cohort manifest archive extensions do not match the runtime registry".to_string(),
        );
    }
    let (_, bootstrap_hash) = validate_cohort_manifest_snapshot(
        &entry.bootstrap,
        "root",
        selection.descriptor.bootstrap.archive_extensions,
    )?;
    let bounds = normal_epoch_bootstrap_bounds(start_epoch)?;
    if !bounds.accepts(entry.bootstrap.slot, bootstrap_hash) {
        return Err(format!(
            "cohort manifest bootstrap slot {} does not satisfy epoch {start_epoch} bootstrap policy",
            entry.bootstrap.slot
        ));
    }
    if entry.root_checkpoints.is_empty() {
        return Err("cohort manifest has no root checkpoints".to_string());
    }
    let (_, cohort_end) = epoch_to_slot_range(end_epoch);
    let (final_start, final_end) = epoch_to_slot_range(end_epoch);
    let mut previous_slot = entry.bootstrap.slot;
    let mut has_final_root = false;
    for checkpoint in &entry.root_checkpoints {
        validate_cohort_manifest_snapshot(
            checkpoint,
            "root",
            selection.descriptor.bootstrap.archive_extensions,
        )?;
        if checkpoint.slot <= previous_slot || checkpoint.slot > cohort_end {
            return Err(format!(
                "cohort manifest root checkpoint slots are not strictly ordered through epoch {end_epoch}"
            ));
        }
        has_final_root |= (final_start..=final_end).contains(&checkpoint.slot);
        previous_slot = checkpoint.slot;
    }
    if !has_final_root {
        return Err(format!(
            "cohort manifest {start_epoch}-{end_epoch} has no root checkpoint in its final epoch"
        ));
    }
    Ok(RootCheckpointCohortPlan {
        fingerprint: actual_fingerprint,
        bootstrap: entry.bootstrap,
        root_checkpoints: entry.root_checkpoints,
    })
}

fn load_root_checkpoint_cohort_plan(
    path: &Path,
    expected_fingerprint: &str,
    start_epoch: u64,
    end_epoch: u64,
    selection: compatibility::RuntimeSelection,
) -> Result<RootCheckpointCohortPlan, String> {
    use std::{io::Read as _, os::unix::fs::MetadataExt as _};

    let mut file = jetstreamer_node::archive_checksum::open_regular_nofollow(path)
        .map_err(|error| format!("failed to open cohort manifest {}: {error}", path.display()))?;
    let metadata = file.metadata().map_err(|error| {
        format!(
            "failed to inspect cohort manifest {}: {error}",
            path.display()
        )
    })?;
    if metadata.uid() != effective_user_id() || metadata.mode() & 0o022 != 0 {
        return Err(format!(
            "cohort manifest must be owned by this user and not group/world writable: {}",
            path.display()
        ));
    }
    if metadata.len() == 0 || metadata.len() > COHORT_MANIFEST_MAX_BYTES {
        return Err(format!(
            "cohort manifest size {} is outside 1..={COHORT_MANIFEST_MAX_BYTES}: {}",
            metadata.len(),
            path.display()
        ));
    }
    let before = jetstreamer_node::archive_checksum::archive_file_identity(&file)
        .map_err(|error| format!("failed to bind cohort manifest {}: {error}", path.display()))?;
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    file.read_to_end(&mut bytes)
        .map_err(|error| format!("failed to read cohort manifest {}: {error}", path.display()))?;
    let after =
        jetstreamer_node::archive_checksum::archive_file_identity(&file).map_err(|error| {
            format!(
                "failed to recheck cohort manifest {}: {error}",
                path.display()
            )
        })?;
    if before != after
        || !jetstreamer_node::archive_checksum::path_matches_archive_identity(path, before)
            .map_err(|error| {
                format!(
                    "failed to recheck cohort manifest path {}: {error}",
                    path.display()
                )
            })?
    {
        return Err(format!(
            "cohort manifest changed while it was read: {}",
            path.display()
        ));
    }
    let report = serde_json::from_slice(&bytes)
        .map_err(|error| format!("invalid cohort manifest JSON {}: {error}", path.display()))?;
    root_checkpoint_cohort_plan_from_report(
        report,
        expected_fingerprint,
        start_epoch,
        end_epoch,
        selection,
    )
}

#[derive(Debug)]
struct BoundCohortSnapshot {
    path: PathBuf,
    file: fs::File,
    evidence: jetstreamer_node::archive_checksum::ValidatedArchiveFile,
    size: u64,
}

impl BoundCohortSnapshot {
    fn revalidate(&self) -> Result<(), String> {
        let current = jetstreamer_node::archive_checksum::archive_file_identity(&self.file)
            .map_err(|error| {
                format!(
                    "failed to recheck bound cohort bootstrap {}: {error}",
                    self.path.display()
                )
            })?;
        if current != self.evidence.identity
            || !jetstreamer_node::archive_checksum::path_matches_archive_identity(
                &self.path,
                self.evidence.identity,
            )
            .map_err(|error| {
                format!(
                    "failed to recheck cohort bootstrap path {}: {error}",
                    self.path.display()
                )
            })?
        {
            return Err(format!(
                "cohort bootstrap changed after it was bound: {}",
                self.path.display()
            ));
        }
        Ok(())
    }
}

fn bind_cohort_snapshot_download(
    path: &Path,
    manifest: &CohortManifestSnapshot,
) -> Result<BoundCohortSnapshot, String> {
    use std::os::unix::fs::FileExt as _;

    let file =
        jetstreamer_node::archive_checksum::open_regular_nofollow(path).map_err(|error| {
            format!(
                "failed to open downloaded cohort bootstrap {}: {error}",
                path.display()
            )
        })?;
    let identity =
        jetstreamer_node::archive_checksum::archive_file_identity(&file).map_err(|error| {
            format!(
                "failed to identify downloaded cohort bootstrap {}: {error}",
                path.display()
            )
        })?;
    let metadata = file.metadata().map_err(|error| {
        format!(
            "failed to inspect downloaded cohort bootstrap {}: {error}",
            path.display()
        )
    })?;
    if metadata.len() != manifest.size {
        return Err(format!(
            "downloaded cohort bootstrap size mismatch for {}: manifest {}, local {}",
            path.display(),
            manifest.size,
            metadata.len()
        ));
    }
    let mut sha256 = Sha256::new();
    let crc32c = Crc::<u32>::new(&CRC_32_ISCSI);
    let mut crc_digest = crc32c.digest();
    let mut buffer = [0u8; 128 * 1024];
    let mut offset = 0u64;
    while offset < manifest.size {
        let remaining = usize::try_from((manifest.size - offset).min(buffer.len() as u64))
            .expect("bounded by fixed checksum buffer");
        let read = file
            .read_at(&mut buffer[..remaining], offset)
            .map_err(|error| {
                format!(
                    "failed to measure downloaded cohort bootstrap {}: {error}",
                    path.display()
                )
            })?;
        if read == 0 {
            return Err(format!(
                "downloaded cohort bootstrap ended early while measuring {}",
                path.display()
            ));
        }
        sha256.update(&buffer[..read]);
        crc_digest.update(&buffer[..read]);
        offset += read as u64;
    }
    let actual_crc32c = BASE64_STANDARD.encode(crc_digest.finalize().to_be_bytes());
    if actual_crc32c != manifest.crc32c {
        return Err(format!(
            "downloaded cohort bootstrap CRC32C mismatch for {}: manifest {}, local {}",
            path.display(),
            manifest.crc32c,
            actual_crc32c
        ));
    }
    if jetstreamer_node::archive_checksum::archive_file_identity(&file).map_err(|error| {
        format!(
            "failed to recheck downloaded cohort bootstrap {}: {error}",
            path.display()
        )
    })? != identity
        || !jetstreamer_node::archive_checksum::path_matches_archive_identity(path, identity)
            .map_err(|error| {
                format!(
                    "failed to recheck downloaded cohort bootstrap path {}: {error}",
                    path.display()
                )
            })?
    {
        return Err(format!(
            "downloaded cohort bootstrap changed while it was measured: {}",
            path.display()
        ));
    }
    Ok(BoundCohortSnapshot {
        path: path.to_path_buf(),
        file,
        evidence: jetstreamer_node::archive_checksum::ValidatedArchiveFile {
            identity,
            sha256: sha256.finalize().into(),
        },
        size: manifest.size,
    })
}

fn validate_root_checkpoint_cohort_expectations(
    start_epoch: u64,
    end_epoch: u64,
    bootstrap_slot: Slot,
    bootstrap_expectation: BankHashExpectation,
    expected: &BTreeMap<Slot, BankHashExpectation>,
) -> Result<(), String> {
    if expected.get(&bootstrap_slot) != Some(&bootstrap_expectation) {
        return Err(format!(
            "root-checkpoint cohort does not bind its bootstrap root at slot {bootstrap_slot}"
        ));
    }
    let (first_start, _) = epoch_to_slot_range(start_epoch);
    if bootstrap_slot >= first_start {
        return Err(format!(
            "root-checkpoint cohort bootstrap slot {bootstrap_slot} is not before epoch {start_epoch}"
        ));
    }
    let (final_start, final_end) = epoch_to_slot_range(end_epoch);
    if expected.range(final_start..=final_end).next().is_none() {
        return Err(format!(
            "root-checkpoint cohort {start_epoch}-{end_epoch} has no trusted root checkpoint in its final epoch"
        ));
    }
    Ok(())
}

fn runtime_supports_private_replay_scratch(
    selection: compatibility::RuntimeSelection,
    load_from_dir: bool,
) -> bool {
    selection.descriptor.worker.is_some()
        || (selection.backend == compatibility::RuntimeBackend::AgaveV3 && !load_from_dir)
}

fn runtime_spans_support_private_replay_scratch(
    spans: &[compatibility::RuntimeSpan],
    load_from_dir: bool,
) -> Result<bool, String> {
    for span in spans {
        if !runtime_supports_private_replay_scratch(runtime_span_selection(span)?, load_from_dir) {
            return Ok(false);
        }
    }
    Ok(true)
}

fn range_supports_private_replay_scratch(
    start_epoch: u64,
    end_epoch: u64,
    allow_candidate_runtime: bool,
    load_from_dir: bool,
) -> Result<bool, String> {
    for epoch in start_epoch..=end_epoch {
        let (slot_start, slot_end_inclusive) = epoch_to_slot_range(epoch);
        let spans = compatibility::plan_runtime_spans(
            slot_start..slot_end_inclusive.saturating_add(1),
            allow_candidate_runtime,
        )?;
        if !runtime_spans_support_private_replay_scratch(&spans, load_from_dir)? {
            return Ok(false);
        }
    }
    Ok(true)
}

fn range_runtime_kinds(
    start_epoch: u64,
    end_epoch: u64,
    allow_candidate_runtime: bool,
) -> Result<(bool, bool), String> {
    let mut has_historical_worker = false;
    let mut has_agave = false;
    for epoch in start_epoch..=end_epoch {
        let (slot_start, slot_end_inclusive) = epoch_to_slot_range(epoch);
        let spans = compatibility::plan_runtime_spans(
            slot_start..slot_end_inclusive.saturating_add(1),
            allow_candidate_runtime,
        )?;
        for span in &spans {
            let selection = runtime_span_selection(span)?;
            has_historical_worker |= selection.descriptor.worker.is_some();
            has_agave |= selection.backend == compatibility::RuntimeBackend::AgaveV3;
        }
    }
    Ok((has_historical_worker, has_agave))
}

fn snapshot_hash_expectation(
    hash: SnapshotHash,
    kind: compatibility::SnapshotHashKind,
) -> BankHashExpectation {
    match kind {
        compatibility::SnapshotHashKind::LegacyAccountsHash => {
            BankHashExpectation::LegacyAccountsHash(hash.0)
        }
        compatibility::SnapshotHashKind::AccountsLtHash => {
            BankHashExpectation::AccountsLtHash(hash)
        }
    }
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

fn replay_start_for_bootstrap(
    bootstrap: &ReplayBootstrap,
    epoch: u64,
    epoch_start: Slot,
) -> Result<Slot, String> {
    match bootstrap {
        ReplayBootstrap::Genesis { .. } if epoch == 0 && epoch_start == 0 => Ok(0),
        ReplayBootstrap::Genesis { .. } => Err(format!(
            "genesis bootstrap is valid only for epoch 0, not epoch {epoch}"
        )),
        ReplayBootstrap::SnapshotArchive(_) => initial_replay_slot(bootstrap.slot()?, epoch_start),
    }
}

fn archive_generation_profile() -> String {
    format!(
        "jetstreamer-node/{}/old-faithful-to-horizon-v2@{}",
        env!("CARGO_PKG_VERSION"),
        env!("JETSTREAMER_BUILD_REVISION")
    )
}

// This producer created the canonical v1.0.7 runtime segment and handoff
// snapshot already in use. Compatibility is deliberately exact and scoped to
// that runtime; the worker digest, runtime revision/toolchain/target, genesis,
// archive digest, and checkpoint tuple are still validated independently.
const COMPATIBLE_V1_0_7_GENERATION_PROFILES: &[&str] =
    &["jetstreamer-node/0.7.0/old-faithful-to-horizon-v2@00f3f6e622128cbe9e57a68ce67981fa4b3f2d95"];
// 8154bc9 changed only worker-exit diagnostics. The following scheduler and
// executable-binding changes likewise do not affect historical replay bytes.
// Keep the last profile that produced verified v1.0.8 archives reusable while
// every independent runtime, bootstrap, checkpoint, and archive digest check
// remains mandatory.
const COMPATIBLE_V1_0_8_GENERATION_PROFILES: &[&str] =
    &["jetstreamer-node/0.7.0/old-faithful-to-horizon-v2@8154bc9b0057a138afa6e8833468f6ffba39a6a1"];

// This is not a generation-profile allowlist. Each record identifies one
// completed private archive whose full bytes and production inputs were
// audited after its producer exited. Private recovery and later reuse may
// consult these identities, but no validation capability is returned until
// the archive has passed a full decode, semantic/PoH-chain verification, and
// an inode-bound exact size and SHA-256 check. No other output from either
// producer profile is admitted.
struct AuditedArchiveRecovery {
    epoch: u64,
    generation_profile: &'static str,
    archive_bytes: u64,
    archive_sha256: [u8; 32],
    worker_path: &'static str,
    worker_sha256: [u8; 32],
    runtime_descriptor: &'static compatibility::RuntimeDescriptor,
    bootstrap_slot: Slot,
    bootstrap_hash: &'static str,
    requested_slot_start: Slot,
    requested_slot_count: u64,
    transaction_metadata: TransactionMetadataPolicy,
}

const AUDITED_DIRTY_RECOVERY_PROFILE: &str = "jetstreamer-node/0.7.0/old-faithful-to-horizon-v2@ba260be5771123de874438d89822fc6b121842a8-dirty-d74af39026d3f16cfbb43d76";
const AUDITED_EPOCH_11_RECOVERY_PROFILE: &str =
    "jetstreamer-node/0.7.0/old-faithful-to-horizon-v2@0a8ec77094ddf2b21ff22e6f4a55fef836f8f2c6";
static AUDITED_ARCHIVE_RECOVERIES: &[AuditedArchiveRecovery] = &[
    AuditedArchiveRecovery {
        epoch: 7,
        generation_profile: AUDITED_DIRTY_RECOVERY_PROFILE,
        archive_bytes: 2_641_008_144,
        archive_sha256: [
            0x95, 0xfb, 0x73, 0xb6, 0x6e, 0xaf, 0x0b, 0x9d, 0x42, 0xd3, 0x2e, 0x0f, 0x62, 0x5c,
            0xa8, 0xbc, 0x7b, 0xdf, 0x1d, 0x4f, 0xb8, 0x52, 0x59, 0x0d, 0x4e, 0xab, 0x98, 0x48,
            0x53, 0x96, 0xb5, 0xea,
        ],
        worker_path: "/home/sol/.jetstreamer-private/deploy-epochs7-100-20260911-v1/\
jetstreamer-historical-worker-v1-0-8",
        worker_sha256: [
            0x6f, 0xd3, 0xcb, 0xc6, 0x14, 0xb1, 0xd2, 0x0d, 0xce, 0x57, 0x59, 0xec, 0x32, 0xc1,
            0x35, 0xb2, 0x44, 0xd5, 0x9d, 0x92, 0xac, 0xbb, 0xa2, 0x7f, 0xf0, 0x6e, 0x05, 0x0b,
            0x8f, 0x47, 0x4b, 0xb4,
        ],
        runtime_descriptor: &compatibility::SOLANA_V1_0_8_RUNTIME,
        bootstrap_slot: 2_908_740,
        bootstrap_hash: "AYUia12SdC6EnauaynZJna2Pf3WNcmmFhpUJU6XritdT",
        requested_slot_start: 3_024_000,
        requested_slot_count: 432_000,
        transaction_metadata: TransactionMetadataPolicy::runtime_reconstructed_before(4_258_776),
    },
    AuditedArchiveRecovery {
        epoch: 8,
        generation_profile: AUDITED_DIRTY_RECOVERY_PROFILE,
        archive_bytes: 2_620_596_851,
        archive_sha256: [
            0x69, 0x07, 0xab, 0xc3, 0xcf, 0x34, 0x68, 0xe5, 0xb4, 0x85, 0xb0, 0xec, 0x40, 0x53,
            0x8a, 0x95, 0x00, 0xef, 0xad, 0x51, 0xb6, 0x7d, 0x65, 0x65, 0xd6, 0xe3, 0x79, 0x5a,
            0x80, 0x1f, 0x22, 0x46,
        ],
        worker_path: "/home/sol/.jetstreamer-private/deploy-epochs7-100-20260911-v1/\
jetstreamer-historical-worker-v1-0-13",
        worker_sha256: [
            0xc1, 0x18, 0xc6, 0xd7, 0xc5, 0x25, 0x4f, 0xca, 0x54, 0x1b, 0x7b, 0x45, 0xdb, 0x52,
            0x9b, 0x22, 0x33, 0x95, 0xc3, 0xb9, 0x35, 0x73, 0x93, 0x7f, 0x64, 0x1d, 0x33, 0x0f,
            0x0a, 0x61, 0x07, 0x0a,
        ],
        runtime_descriptor: &compatibility::SOLANA_V1_0_13_RUNTIME,
        bootstrap_slot: 3_455_940,
        bootstrap_hash: "aCgGdj7tmTZQPF118GKxe7hn7yBv7MgVdn7dWL1Qcbw",
        requested_slot_start: 3_456_000,
        requested_slot_count: 432_000,
        transaction_metadata: TransactionMetadataPolicy::runtime_reconstructed_before(4_258_776),
    },
    AuditedArchiveRecovery {
        epoch: 9,
        generation_profile: AUDITED_DIRTY_RECOVERY_PROFILE,
        archive_bytes: 2_602_836_112,
        archive_sha256: [
            0x4d, 0x60, 0x46, 0xc9, 0xde, 0xa1, 0xb8, 0x97, 0xb6, 0x77, 0x04, 0xd3, 0x97, 0x72,
            0xaa, 0x58, 0x08, 0xf8, 0xe5, 0xbd, 0x82, 0x05, 0xff, 0xe4, 0x99, 0xa7, 0x12, 0x6e,
            0x06, 0xb0, 0xa9, 0xaf,
        ],
        worker_path: "/home/sol/.jetstreamer-private/deploy-epochs7-100-20260911-v1/\
jetstreamer-historical-worker-v1-0-14",
        worker_sha256: [
            0x86, 0xa9, 0xf2, 0x0f, 0x27, 0x37, 0x7b, 0xf1, 0xd6, 0xd8, 0xe4, 0xed, 0x2c, 0x5e,
            0x23, 0xc2, 0xe0, 0x58, 0xeb, 0x14, 0x73, 0x2e, 0xa5, 0x96, 0x68, 0x39, 0x08, 0xa0,
            0x6b, 0xf4, 0xa9, 0x4c,
        ],
        runtime_descriptor: &compatibility::SOLANA_V1_0_14_RUNTIME,
        bootstrap_slot: 3_887_911,
        bootstrap_hash: "BsYGwLpE5NBbehe1utx657A1b9vsT4nJ9scKbff1awbN",
        requested_slot_start: 3_888_000,
        requested_slot_count: 432_000,
        transaction_metadata: TransactionMetadataPolicy::runtime_reconstructed_with_fee_from(
            4_258_776,
        ),
    },
    AuditedArchiveRecovery {
        epoch: 10,
        generation_profile: AUDITED_DIRTY_RECOVERY_PROFILE,
        archive_bytes: 2_788_065_078,
        archive_sha256: [
            0xa5, 0x3d, 0x51, 0xd3, 0xba, 0x5d, 0x05, 0xf8, 0x21, 0x5e, 0x1a, 0xc2, 0xd7, 0x9f,
            0xee, 0x4b, 0x93, 0x83, 0xb9, 0xb9, 0x0e, 0x3d, 0x6b, 0xd7, 0x74, 0xea, 0xd2, 0xc8,
            0x4b, 0x52, 0x2a, 0x4b,
        ],
        worker_path: "/home/sol/.jetstreamer-private/deploy-epochs7-100-20260911-v1/\
jetstreamer-historical-worker-v1-0-14",
        worker_sha256: [
            0x86, 0xa9, 0xf2, 0x0f, 0x27, 0x37, 0x7b, 0xf1, 0xd6, 0xd8, 0xe4, 0xed, 0x2c, 0x5e,
            0x23, 0xc2, 0xe0, 0x58, 0xeb, 0x14, 0x73, 0x2e, 0xa5, 0x96, 0x68, 0x39, 0x08, 0xa0,
            0x6b, 0xf4, 0xa9, 0x4c,
        ],
        runtime_descriptor: &compatibility::SOLANA_V1_0_14_RUNTIME,
        bootstrap_slot: 4_319_880,
        bootstrap_hash: "9wnXMY186BWwsUq7QjURqasDGKY4bG6mm5R4rCJD6ar",
        requested_slot_start: 4_320_000,
        requested_slot_count: 432_000,
        transaction_metadata: TransactionMetadataPolicy::runtime_reconstructed_status_and_fee(),
    },
    AuditedArchiveRecovery {
        epoch: 11,
        generation_profile: AUDITED_EPOCH_11_RECOVERY_PROFILE,
        archive_bytes: 2_765_674_556,
        archive_sha256: [
            0x85, 0xad, 0x2e, 0x93, 0xd5, 0x03, 0xaa, 0x17, 0xa6, 0x33, 0x40, 0x6e, 0x7d, 0x63,
            0x83, 0x69, 0xa3, 0xe7, 0x86, 0xd4, 0x08, 0x0c, 0x05, 0x6d, 0x68, 0x9f, 0xd3, 0xe5,
            0x81, 0x46, 0xde, 0xda,
        ],
        worker_path: "/home/sol/.jetstreamer-private/deploy-epoch11-full-v1014-20260910-v3/\
jetstreamer-historical-worker-v1-0-14",
        worker_sha256: [
            0x89, 0xdb, 0xd1, 0x4b, 0x7b, 0xef, 0x4b, 0xd9, 0xcc, 0x6a, 0x49, 0xbc, 0x92, 0x6e,
            0x70, 0x29, 0x00, 0x58, 0x63, 0x94, 0x98, 0xf9, 0xba, 0x51, 0xe4, 0x47, 0xbb, 0x78,
            0x92, 0xa4, 0xb9, 0x96,
        ],
        runtime_descriptor: &compatibility::SOLANA_V1_0_14_RUNTIME,
        bootstrap_slot: 4_751_796,
        bootstrap_hash: "6vJ22rwAfXfr4hFUJ7AtLupR6LHWBWX114AhKJqPYejb",
        requested_slot_start: 4_752_000,
        requested_slot_count: 432_000,
        transaction_metadata: TransactionMetadataPolicy::runtime_reconstructed_status_and_fee(),
    },
    AuditedArchiveRecovery {
        epoch: 20,
        generation_profile: AUDITED_DIRTY_RECOVERY_PROFILE,
        archive_bytes: 2_940_249_886,
        archive_sha256: [
            0xd1, 0x43, 0x43, 0xee, 0x1f, 0x7a, 0xd7, 0x0f, 0x4f, 0x04, 0x2a, 0xa4, 0x13, 0x4d,
            0xff, 0xd9, 0x5c, 0x87, 0xe8, 0x33, 0x46, 0xd4, 0x46, 0x36, 0x96, 0x13, 0x31, 0x89,
            0xa9, 0x7f, 0x91, 0x55,
        ],
        worker_path: "/home/sol/.jetstreamer-private/deploy-epochs7-100-20260911-v1/\
jetstreamer-historical-worker-v1-0-23",
        worker_sha256: [
            0xd0, 0x5d, 0x99, 0xe0, 0xfb, 0xfb, 0xb4, 0x9a, 0xc2, 0x65, 0x4f, 0xe9, 0x97, 0x5e,
            0x7b, 0x3d, 0xf5, 0xed, 0x27, 0xf5, 0x22, 0x6d, 0x2c, 0x9c, 0x6e, 0x34, 0xf9, 0x6c,
            0x2b, 0x31, 0x8b, 0xed,
        ],
        runtime_descriptor: &compatibility::SOLANA_V1_0_23_RUNTIME,
        bootstrap_slot: 8_639_740,
        bootstrap_hash: "6Mqw6TTrvCHZDyfovexiLB5yiyeYnWsFXtnxn4yST8g6",
        requested_slot_start: 8_640_000,
        requested_slot_count: 432_000,
        transaction_metadata: TransactionMetadataPolicy::runtime_reconstructed_status_and_fee(),
    },
];

fn runtime_generation_profile_is_compatible(
    runtime_profile: &str,
    recorded_generation_profile: &str,
) -> bool {
    recorded_generation_profile == archive_generation_profile()
        || (runtime_profile == historical::SOLANA_V1_0_7_CANDIDATE.backend_id
            && COMPATIBLE_V1_0_7_GENERATION_PROFILES.contains(&recorded_generation_profile))
        || (runtime_profile == historical::SOLANA_V1_0_8_CANDIDATE.backend_id
            && COMPATIBLE_V1_0_8_GENERATION_PROFILES.contains(&recorded_generation_profile))
}

fn audited_archive_recovery_provenance_matches(
    epoch: u64,
    provenance: &ArchiveProvenance,
) -> Option<&'static AuditedArchiveRecovery> {
    let recovery = AUDITED_ARCHIVE_RECOVERIES
        .iter()
        .find(|candidate| candidate.epoch == epoch)?;
    let ArchiveProvenance::V2(provenance) = provenance else {
        return None;
    };
    let Ok(bootstrap_hash) = recovery.bootstrap_hash.parse::<Hash>() else {
        return None;
    };
    let (epoch_start, epoch_end_inclusive) = epoch_to_slot_range(recovery.epoch);
    if recovery.requested_slot_start != epoch_start
        || recovery.requested_slot_count != epoch_end_inclusive - epoch_start + 1
    {
        return None;
    }
    let runtime = recovery.runtime_descriptor.identity;
    (provenance.base.generation_profile == recovery.generation_profile
        && provenance.base.runtime_profile == runtime.name
        && provenance.base.runtime_admission == RuntimeAdmission::Candidate
        && provenance.base.runtime_revision == runtime.revision
        && provenance.base.runtime_toolchain == archive_runtime_toolchain(runtime)
        && provenance.base.genesis_hash
            == compatibility::MAINNET_GENESIS_HASH
                .parse::<Hash>()
                .expect("compiled mainnet genesis hash is valid")
        && provenance.base.bootstrap_state_kind == BootstrapStateKind::SnapshotArchive
        && provenance.base.bootstrap_slot == recovery.bootstrap_slot
        && provenance.base.bootstrap_state_hash == bootstrap_hash
        && provenance.base.requested_slot_start == recovery.requested_slot_start
        && provenance.base.requested_slot_count == recovery.requested_slot_count
        && provenance.base.transaction_metadata == recovery.transaction_metadata
        && provenance.worker_executable_sha256 == recovery.worker_sha256)
        .then_some(recovery)
}

fn audited_archive_worker_binding_matches(
    recovery: &AuditedArchiveRecovery,
    canonical_path: &Path,
    sha256: [u8; 32],
) -> bool {
    canonical_path == Path::new(recovery.worker_path) && sha256 == recovery.worker_sha256
}

fn measure_audited_archive_recovery_worker(
    recovery: &AuditedArchiveRecovery,
    descriptor: &compatibility::RuntimeDescriptor,
) -> Result<[u8; 32], String> {
    if !std::ptr::eq(descriptor, recovery.runtime_descriptor) {
        return Err(format!(
            "audited epoch-{} recovery requires runtime {}, got {}",
            recovery.epoch, recovery.runtime_descriptor.identity.name, descriptor.identity.name,
        ));
    }
    let configured = configured_historical_worker_executable(descriptor)?;
    let canonical = fs::canonicalize(&configured).map_err(|error| {
        format!(
            "failed to resolve configured audited epoch-{} worker {}: {error}",
            recovery.epoch,
            configured.display()
        )
    })?;
    if canonical != Path::new(recovery.worker_path) {
        return Err(format!(
            "audited epoch-{} recovery requires frozen worker {}, got {}",
            recovery.epoch,
            recovery.worker_path,
            canonical.display(),
        ));
    }
    let sha256 = historical::measure_executable_sha256(&canonical).map_err(|error| {
        format!(
            "failed to measure frozen audited epoch-{} worker {}: {error}",
            recovery.epoch,
            canonical.display()
        )
    })?;
    if !audited_archive_worker_binding_matches(recovery, &canonical, sha256) {
        return Err(format!(
            "frozen audited epoch-{} worker {} has an unexpected SHA-256",
            recovery.epoch,
            canonical.display()
        ));
    }
    Ok(sha256)
}

fn audited_archive_content_matches(
    recovery: &AuditedArchiveRecovery,
    bytes: u64,
    sha256: [u8; 32],
) -> bool {
    bytes == recovery.archive_bytes && sha256 == recovery.archive_sha256
}

fn segment_runtime_identity_is_compatible(
    recorded: &SegmentRuntimeIdentity,
    expected: &SegmentRuntimeIdentity,
) -> bool {
    recorded.runtime_profile == expected.runtime_profile
        && runtime_generation_profile_is_compatible(
            &recorded.runtime_profile,
            &recorded.generation_profile,
        )
        && recorded.runtime_admission == expected.runtime_admission
        && recorded.runtime_revision == expected.runtime_revision
        && recorded.runtime_toolchain == expected.runtime_toolchain
        && recorded.runtime_target == expected.runtime_target
        && recorded.genesis_hash == expected.genesis_hash
}

fn archive_assembly_profile() -> String {
    format!(
        "jetstreamer-node/{}/verified-runtime-segment-assembly-v1@{}",
        env!("CARGO_PKG_VERSION"),
        env!("JETSTREAMER_BUILD_REVISION")
    )
}

fn archive_transaction_metadata_policy(
    slot_start: Slot,
    slot_count: u64,
) -> TransactionMetadataPolicy {
    let slot_end_exclusive = slot_start.saturating_add(slot_count);
    if slot_end_exclusive <= compatibility::OLD_FAITHFUL_STATUS_REQUIRED_START_SLOT {
        // Preserve the existing provenance encoding for archives wholly below
        // the source-presence boundary. Its in-range semantics are identical
        // to an all-reconstructed policy.
        TransactionMetadataPolicy::runtime_reconstructed_before(
            compatibility::OLD_FAITHFUL_STATUS_REQUIRED_START_SLOT,
        )
    } else if slot_start < compatibility::OLD_FAITHFUL_STATUS_REQUIRED_START_SLOT {
        TransactionMetadataPolicy::runtime_reconstructed_with_fee_from(
            compatibility::OLD_FAITHFUL_STATUS_REQUIRED_START_SLOT,
        )
    } else if slot_start
        < compatibility::OLD_FAITHFUL_UNTRUSTED_STATUS_ASSOCIATION_END_SLOT_EXCLUSIVE
    {
        TransactionMetadataPolicy::runtime_reconstructed_status_and_fee()
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
        transaction_metadata: archive_transaction_metadata_policy(
            requested_slot_start,
            requested_slot_count,
        ),
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
        || !runtime_generation_profile_is_compatible(
            &manifest.runtime.runtime_profile,
            &manifest.runtime.generation_profile,
        )
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
        compatibility::RuntimeBackend::SolanaV1_0_13 => historical::SOLANA_V1_0_13_CANDIDATE,
        compatibility::RuntimeBackend::SolanaV1_0_14 => historical::SOLANA_V1_0_14_CANDIDATE,
        compatibility::RuntimeBackend::SolanaV1_0_17 => historical::SOLANA_V1_0_17_CANDIDATE,
        compatibility::RuntimeBackend::SolanaV1_0_18 => historical::SOLANA_V1_0_18_CANDIDATE,
        compatibility::RuntimeBackend::SolanaV1_0_23 => historical::SOLANA_V1_0_23_CANDIDATE,
        compatibility::RuntimeBackend::SolanaV1_0_24 => historical::SOLANA_V1_0_24_CANDIDATE,
        compatibility::RuntimeBackend::SolanaV1_1_23 => historical::SOLANA_V1_1_23_CANDIDATE,
        compatibility::RuntimeBackend::SolanaV1_2_32 => historical::SOLANA_V1_2_32_CANDIDATE,
        compatibility::RuntimeBackend::SolanaV1_3_19 => historical::SOLANA_V1_3_19_CANDIDATE,
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
        || profile.snapshot_archive_extensions != descriptor.bootstrap.archive_extensions
        || descriptor.bootstrap.snapshot_hash_kind
            != compatibility::SnapshotHashKind::LegacyAccountsHash
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

#[derive(Debug)]
struct SnapshotArchiveCandidate {
    path: PathBuf,
    slot: Slot,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct SnapshotBootstrapBounds {
    min_slot: Slot,
    max_slot: Slot,
    required_accounts_hash: Option<Hash>,
}

impl SnapshotBootstrapBounds {
    fn exact(slot: Slot, required_accounts_hash: Option<Hash>) -> Self {
        Self {
            min_slot: slot,
            max_slot: slot,
            required_accounts_hash,
        }
    }

    fn accepts(self, slot: Slot, accounts_hash: Hash) -> bool {
        (self.min_slot..=self.max_slot).contains(&slot)
            && self
                .required_accounts_hash
                .is_none_or(|required_hash| accounts_hash == required_hash)
    }
}

/// Returns the complete bootstrap policy for a normal (non-qualification)
/// epoch run. Most epochs accept any snapshot from the predecessor epoch.
/// Epoch 12 is intentionally narrower: its v1.0.23 route starts from one exact
/// canonical anchor, so a later epoch-11 snapshot must not silently shorten
/// the registered warmup span.
fn normal_epoch_bootstrap_bounds(epoch: u64) -> Result<SnapshotBootstrapBounds, String> {
    if epoch == 0 {
        return Err("epoch 0 bootstraps from genesis, not a snapshot".to_string());
    }
    let min_slot = epoch_to_slot(epoch - 1);
    let max_slot = epoch_to_slot(epoch)
        .checked_sub(1)
        .ok_or_else(|| format!("epoch {epoch} has no predecessor snapshot slot"))?;
    if epoch_to_slot(epoch) == compatibility::SOLANA_V1_0_23_CANDIDATE_START_SLOT {
        let slot = compatibility::SOLANA_V1_0_23_INITIAL_SNAPSHOT_SLOT;
        if !(min_slot..=max_slot).contains(&slot) {
            return Err(format!(
                "runtime registry epoch-12 bootstrap slot {slot} is outside predecessor bounds {min_slot}..={max_slot}"
            ));
        }
        if slot.checked_add(1) != Some(compatibility::SOLANA_V1_0_23_INITIAL_REPLAY_SLOT) {
            return Err(
                "runtime registry epoch-12 bootstrap and initial replay slots are inconsistent"
                    .to_string(),
            );
        }
        let accounts_hash = compatibility::SOLANA_V1_0_23_INITIAL_SNAPSHOT_ACCOUNTS_HASH
            .parse::<Hash>()
            .map_err(|err| {
                format!("runtime registry has an invalid epoch-12 bootstrap accounts hash: {err}")
            })?;
        return Ok(SnapshotBootstrapBounds::exact(slot, Some(accounts_hash)));
    }
    Ok(SnapshotBootstrapBounds {
        min_slot,
        max_slot,
        required_accounts_hash: None,
    })
}

fn validate_epoch_bootstrap_identity(
    epoch: u64,
    slot: Slot,
    accounts_hash: Hash,
) -> Result<(), String> {
    let bounds = normal_epoch_bootstrap_bounds(epoch)?;
    if !(bounds.min_slot..=bounds.max_slot).contains(&slot) {
        return Err(format!(
            "epoch {epoch} requires a bootstrap snapshot in slots {}..={}, got slot {slot}",
            bounds.min_slot, bounds.max_slot,
        ));
    }
    if let Some(required_hash) = bounds.required_accounts_hash
        && accounts_hash != required_hash
    {
        return Err(format!(
            "epoch {epoch} requires bootstrap snapshot accounts hash {required_hash} at slot {slot}, got {accounts_hash}",
        ));
    }
    Ok(())
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
    bounds: SnapshotBootstrapBounds,
    archive_extensions: &[&str],
) -> Result<Option<SnapshotArchiveCandidate>, String> {
    if !dest_dir.is_dir() {
        return Ok(None);
    }
    let read_dir = fs::read_dir(dest_dir)
        .map_err(|err| format!("failed to read {}: {err}", dest_dir.display()))?;
    let mut best: Vec<SnapshotArchiveCandidate> = Vec::new();
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
        let (slot, SnapshotHash(accounts_hash)) = match parse_snapshot_archive_name(name) {
            Ok(parsed) => parsed,
            Err(_) => continue,
        };
        if !bounds.accepts(slot, accounts_hash) {
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
        match best.first() {
            Some(current) if current.slot > candidate.slot => {}
            Some(current) if current.slot == candidate.slot => best.push(candidate),
            _ => {
                best.clear();
                best.push(candidate);
            }
        }
    }
    match best.len() {
        0 => Ok(None),
        1 => Ok(best.pop()),
        _ => {
            let slot = best[0].slot;
            let mut paths = best
                .into_iter()
                .map(|candidate| candidate.path.display().to_string())
                .collect::<Vec<_>>();
            paths.sort_unstable();
            Err(format!(
                "multiple local snapshot archives match newest slot {slot}: {}",
                paths.join(", ")
            ))
        }
    }
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
    bootstrap: compatibility::BootstrapState,
) -> Result<BTreeMap<Slot, BankHashExpectation>, String> {
    let snapshots = list_snapshots_in_slot_range_matching(
        start_slot,
        end_slot_inclusive,
        bootstrap.archive_extensions,
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
        let expectation = snapshot_hash_expectation(hash, bootstrap.snapshot_hash_kind);
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

#[cfg(test)]
fn validate_mainnet_genesis(ledger_dir: &Path) -> Result<ReplayBootstrap, String> {
    validate_mainnet_genesis_in(ledger_dir, ledger_dir)
}

fn validate_mainnet_genesis_in(
    ledger_dir: &Path,
    private_parent: &Path,
) -> Result<ReplayBootstrap, String> {
    fs::create_dir_all(private_parent).map_err(|err| {
        format!(
            "failed to create private genesis parent {}: {err}",
            private_parent.display()
        )
    })?;
    let private_dir = Arc::new(
        tempfile::Builder::new()
            .prefix(".jetstreamer-genesis-")
            .tempdir_in(private_parent)
            .map_err(|err| {
                format!(
                    "failed to create private genesis admission directory in {}: {err}",
                    private_parent.display()
                )
            })?,
    );
    let shared_genesis = ledger_dir.join("genesis.bin");
    let source_path = match fs::symlink_metadata(&shared_genesis) {
        Ok(_) => shared_genesis,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            let archive = ledger_dir.join(GENESIS_ARCHIVE);
            agave_snapshots::unpack_genesis_archive(
                &archive,
                private_dir.path(),
                MAX_GENESIS_ARCHIVE_UNPACKED_SIZE,
            )
            .map_err(|err| {
                format!(
                    "failed to unpack {} into private genesis admission storage: {err}",
                    archive.display()
                )
            })?;
            private_dir.path().join("genesis.bin")
        }
        Err(err) => {
            return Err(format!(
                "failed to inspect canonical genesis input {}: {err}",
                shared_genesis.display()
            ));
        }
    };

    // Admission is based on one nofollow-opened inode: copy and hash the same
    // bounded byte stream, and reject every serialization except the pinned
    // mainnet genesis.bin before asking Agave to decode it.
    let genesis_bin_path = historical::bind_genesis_bin(
        &source_path,
        private_dir.path(),
        MAINNET_GENESIS_BIN_SIZE,
        MAINNET_GENESIS_BIN_SHA256,
    )
    .map_err(|err| format!("failed to admit canonical genesis.bin: {err}"))?;
    let admitted_ledger = genesis_bin_path
        .parent()
        .expect("bound genesis.bin always has a parent directory");
    let genesis_config = open_genesis_config(admitted_ledger, MAX_GENESIS_ARCHIVE_UNPACKED_SIZE)
        .map_err(|err| {
            format!(
                "failed to decode admitted canonical genesis from {}: {err}",
                genesis_bin_path.display()
            )
        })?;
    validate_mainnet_genesis_hash(genesis_config.hash())?;

    Ok(ReplayBootstrap::Genesis {
        genesis_bin_path,
        identity: historical::GenesisFileIdentity {
            size: MAINNET_GENESIS_BIN_SIZE,
            sha256: MAINNET_GENESIS_BIN_SHA256,
        },
        _private_dir: private_dir,
    })
}

fn validate_mainnet_genesis_hash(actual: Hash) -> Result<Hash, String> {
    let expected = compatibility::MAINNET_GENESIS_HASH
        .parse::<Hash>()
        .map_err(|err| {
            format!(
                "runtime registry has invalid mainnet genesis hash {}: {err}",
                compatibility::MAINNET_GENESIS_HASH
            )
        })?;
    if actual != expected {
        return Err(format!(
            "local genesis hash {actual} does not match required mainnet hash {expected}"
        ));
    }
    Ok(actual)
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
    replay_scratch_dir: &Path,
    snapshot_archive: &Path,
    accounts_update_notifier: Option<AccountsUpdateNotifier>,
) -> Result<Bank, String> {
    let full_snapshot = FullSnapshotArchiveInfo::new_from_path(snapshot_archive.to_path_buf())
        .map_err(|err| format!("failed to parse snapshot archive: {err}"))?;
    let genesis_config = open_genesis_config(ledger_dir, MAX_GENESIS_ARCHIVE_UNPACKED_SIZE)
        .map_err(|err| format!("failed to load genesis config: {err}"))?;
    let runtime_config = RuntimeConfig::default();
    let accounts_db_config = accounts_db_config_for_ledger(replay_scratch_dir)?;
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
    let unpack_dir = replay_scratch_dir.join(format!(".snapshot-extract-{archive_tag}"));
    fs::create_dir_all(&unpack_dir)
        .map_err(|err| format!("failed to create {}: {err}", unpack_dir.display()))?;
    let unpack_marker = stage_marker(&unpack_dir, "unpacked");
    let meta_marker = stage_marker(&unpack_dir, "meta_fixed");
    let hardlinks_marker = stage_marker(&unpack_dir, "hardlinks");
    let run_paths_marker = stage_marker(&unpack_dir, "account_paths");
    let account_run_dir = replay_scratch_dir.join(ARCHIVE_ACCOUNTS_DIR);
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

async fn fetch_first_block_edge_at_or_after(
    start_slot: Slot,
    client: &Client,
) -> Result<(Slot, Slot), String> {
    const READ_TIMEOUT: Duration = Duration::from_secs(180);

    let epoch = slot_to_epoch(start_slot);
    let (epoch_start, epoch_end_inclusive) = epoch_to_slot_range(epoch);
    let stream = fetch_epoch_stream(epoch, client).await;
    let mut reader = NodeReader::new(stream);
    tokio::time::timeout(READ_TIMEOUT, reader.read_raw_header())
        .await
        .map_err(|_| format!("timed out reading epoch {epoch} CAR header"))?
        .map_err(|err| format!("failed to read epoch {epoch} CAR header: {err}"))?;
    if start_slot > epoch_start {
        tokio::time::timeout(READ_TIMEOUT, reader.seek_to_slot(start_slot))
            .await
            .map_err(|_| format!("timed out seeking epoch {epoch} to slot {start_slot}"))?
            .map_err(|err| format!("failed to seek epoch {epoch} to slot {start_slot}: {err}"))?;
    }

    loop {
        let nodes = tokio::time::timeout(READ_TIMEOUT, reader.read_until_block())
            .await
            .map_err(|_| {
                format!("timed out reading the first block at or after slot {start_slot}")
            })?
            .map_err(|err| {
                format!("failed to read the first block at or after slot {start_slot}: {err}")
            })?;
        if nodes.0.is_empty() {
            return Err(format!(
                "epoch {epoch} CAR ended before a block at or after slot {start_slot}"
            ));
        }
        let block = nodes
            .get_block()
            .map_err(|err| format!("invalid block group in epoch {epoch} CAR: {err}"))?;
        if !(epoch_start..=epoch_end_inclusive).contains(&block.slot) {
            return Err(format!(
                "epoch {epoch} CAR yielded out-of-range block {}",
                block.slot
            ));
        }
        if block.slot < start_slot {
            continue;
        }
        if block.slot > 0 && block.meta.parent_slot >= block.slot {
            return Err(format!(
                "block {} has non-preceding parent {}",
                block.slot, block.meta.parent_slot
            ));
        }
        return Ok((block.meta.parent_slot, block.slot));
    }
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

enum CarriedRuntimeState {
    Agave {
        backend: compatibility::RuntimeBackend,
        completed_epoch: u64,
        bank_forks: Arc<RwLock<BankForks>>,
    },
    Historical {
        backend: compatibility::RuntimeBackend,
        completed_epoch: u64,
        client: Box<historical::HistoricalRuntimeClient>,
        terminal: historical_replay::HistoricalCheckpointSummary,
        worker_executable_sha256: [u8; 32],
    },
}

impl CarriedRuntimeState {
    fn backend(&self) -> compatibility::RuntimeBackend {
        match self {
            Self::Agave { backend, .. } | Self::Historical { backend, .. } => *backend,
        }
    }

    fn completed_epoch(&self) -> u64 {
        match self {
            Self::Agave {
                completed_epoch, ..
            }
            | Self::Historical {
                completed_epoch, ..
            } => *completed_epoch,
        }
    }

    fn slot(&self) -> Result<Slot, String> {
        match self {
            Self::Agave { bank_forks, .. } => bank_forks
                .read()
                .map_err(|_| "bank forks lock poisoned".to_string())
                .map(|forks| forks.working_bank().slot()),
            Self::Historical { terminal, .. } => Ok(terminal.slot),
        }
    }
}

struct ReplayRunResult {
    carried_state: Option<CarriedRuntimeState>,
    historical_evidence: Option<historical_replay::HistoricalReplayEvidence>,
    historical_worker_executable_sha256: Option<[u8; 32]>,
    /// Exact registered handoff archive admitted and privately copied before
    /// this segment's historical worker initialized.
    bootstrap_handoff_archive_sha256: Option<[u8; 32]>,
}

struct CompletedCohortEpoch {
    epoch: u64,
    staged_output: PathBuf,
    final_output: PathBuf,
    historical_evidence: historical_replay::HistoricalReplayEvidence,
    archive_chain: ArchiveChainEvidence,
    validated: jetstreamer_node::archive_checksum::ValidatedArchiveFile,
}

fn publish_completed_root_cohort_transactionally(
    completed: &[CompletedCohortEpoch],
    expected_epochs: &[u64],
    manifest_fingerprint: [u8; 32],
    destination: &BoundDestination,
    receipt_directory: &Path,
) -> Result<cohort_publication::CompletedPublication, String> {
    let completed_epochs = completed
        .iter()
        .map(|archive| archive.epoch)
        .collect::<Vec<_>>();
    validate_root_cohort_epoch_membership(&completed_epochs, expected_epochs)?;
    for archive in completed {
        if !jetstreamer_node::archive_checksum::path_matches_archive_identity(
            &archive.staged_output,
            archive.validated.identity,
        )
        .map_err(|error| {
            format!(
                "failed to rebind staged cohort archive {}: {error}",
                archive.staged_output.display()
            )
        })? {
            return Err(format!(
                "staged cohort archive changed after validation: {}",
                archive.staged_output.display()
            ));
        }
    }
    destination.revalidate()?;
    let items = completed
        .iter()
        .map(
            |archive| jetstreamer_node::archive_publish::ArchiveBatchItem {
                epoch: archive.epoch,
                staged_archive: archive.staged_output.clone(),
                destination_archive: archive.final_output.clone(),
                evidence: archive.validated,
            },
        )
        .collect::<Vec<_>>();
    let expected = completed
        .iter()
        .map(|archive| cohort_publication::ExpectedCohortArchive {
            epoch: archive.epoch,
            destination_archive: &archive.final_output,
            sha256: archive.validated.sha256,
        })
        .collect::<Vec<_>>();
    let publication = jetstreamer_node::archive_publish::publish_verified_archive_batch(
        manifest_fingerprint,
        expected_epochs,
        &items,
    )
    .map_err(|error| format!("transactional cohort publication failed: {error}"))?;
    cohort_publication::finish_committed_publication(
        destination.publication_binding(),
        receipt_directory,
        manifest_fingerprint,
        &expected,
        &publication,
    )
}

fn validate_root_cohort_epoch_membership(
    actual: &[u64],
    expected_epochs: &[u64],
) -> Result<(), String> {
    if expected_epochs.is_empty()
        || expected_epochs
            .windows(2)
            .any(|epochs| epochs[0].checked_add(1) != Some(epochs[1]))
    {
        return Err(
            "root-checkpoint cohort expected epoch range is empty or noncontiguous".to_string(),
        );
    }
    if actual != expected_epochs {
        return Err(format!(
            "root-checkpoint cohort archive membership {:?} does not match expected epochs {:?}",
            actual, expected_epochs
        ));
    }
    Ok(())
}

fn after_root_cohort_publication_gate<T>(
    completed_archives: usize,
    expected_archives: usize,
    shutdown_requested: bool,
    terminal_verification: Result<(), String>,
    publish: impl FnOnce() -> Result<T, String>,
) -> Result<T, String> {
    if completed_archives != expected_archives {
        return Err(format!(
            "root-checkpoint cohort produced {completed_archives} archives, expected {expected_archives}"
        ));
    }
    if shutdown_requested {
        return Err("root-checkpoint cohort was interrupted before publication".to_string());
    }
    terminal_verification.map_err(|error| {
        format!("root-checkpoint cohort terminal verification is incomplete: {error}")
    })?;
    publish()
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ArchiveFinalizationRoute {
    RootCheckpointCohort,
    OrdinaryTopLevel,
    InternalOrQualification,
}

fn archive_finalization_route(
    root_checkpoint_cohort: bool,
    qualification: Option<QualificationPlan>,
    range_info: Option<(u64, u64)>,
) -> ArchiveFinalizationRoute {
    if root_checkpoint_cohort {
        ArchiveFinalizationRoute::RootCheckpointCohort
    } else if qualification.is_none() && range_info.is_none() {
        ArchiveFinalizationRoute::OrdinaryTopLevel
    } else {
        ArchiveFinalizationRoute::InternalOrQualification
    }
}

fn validate_historical_cohort_evidence<'a>(
    trusted_bootstrap_slot: Slot,
    trusted_bootstrap_accounts_hash: Hash,
    completed: impl IntoIterator<
        Item = (
            u64,
            &'a historical_replay::HistoricalReplayEvidence,
            ArchiveChainEvidence,
        ),
    >,
) -> Result<(), String> {
    let mut completed = completed.into_iter();
    let Some((first_epoch, first_evidence, first_chain)) = completed.next() else {
        return Err("root-checkpoint cohort produced no epoch evidence".to_string());
    };
    if first_evidence.bootstrap.slot != trusted_bootstrap_slot
        || first_evidence.bootstrap.accounts_hash != trusted_bootstrap_accounts_hash.to_bytes()
    {
        return Err(format!(
            "first cohort archive bootstrap evidence does not match trusted root {} at slot {}",
            trusted_bootstrap_accounts_hash, trusted_bootstrap_slot
        ));
    }
    let mut previous = (first_epoch, first_evidence, first_chain);
    for (epoch, evidence, chain) in std::iter::once(previous).chain(completed) {
        let (epoch_start, epoch_end) = epoch_to_slot_range(epoch);
        if !evidence.terminal.slot_complete {
            return Err(format!(
                "cohort epoch {} terminal checkpoint at slot {} is incomplete",
                epoch, evidence.terminal.slot
            ));
        }
        match chain.terminal_block {
            Some(block) => {
                if !(epoch_start..=epoch_end).contains(&block.slot) {
                    return Err(format!(
                        "cohort epoch {epoch} archive terminal block {} is outside {epoch_start}..={epoch_end}",
                        block.slot
                    ));
                }
                if evidence.terminal.slot != block.slot
                    || evidence.terminal.last_blockhash != block.blockhash.to_bytes()
                {
                    return Err(format!(
                        "cohort epoch {epoch} terminal checkpoint does not match archive terminal block {} ({})",
                        block.slot, block.blockhash
                    ));
                }
            }
            None => {
                return Err(format!(
                    "cohort epoch {epoch} archive contains no present block to bind its terminal checkpoint"
                ));
            }
        }
        if epoch != previous.0 {
            if epoch != previous.0.saturating_add(1) {
                return Err(format!(
                    "cohort evidence skips from epoch {} to {}",
                    previous.0, epoch
                ));
            }
            if evidence.bootstrap != previous.1.terminal {
                return Err(format!(
                    "cohort state handoff from epoch {} to {} changed checkpoint evidence",
                    previous.0, epoch
                ));
            }
            if evidence.emitted_write_versions.start != evidence.bootstrap.next_write_version {
                return Err(format!(
                    "cohort epoch {} write versions start at {}, expected carried cursor {}",
                    epoch,
                    evidence.emitted_write_versions.start,
                    evidence.bootstrap.next_write_version
                ));
            }
        }
        previous = (epoch, evidence, chain);
    }
    Ok(())
}

fn validate_cohort_archive_evidence_binding(
    completed: &CompletedCohortEpoch,
    first_epoch: bool,
) -> Result<(), String> {
    let file = jetstreamer_node::archive_checksum::open_regular_nofollow(&completed.staged_output)
        .map_err(|error| {
            format!(
                "failed to bind staged cohort archive {}: {error}",
                completed.staged_output.display()
            )
        })?;
    let reader = jetstreamer_horizon::archive::ArchiveReader::open(std::io::BufReader::new(file))
        .map_err(|error| {
        format!(
            "failed to open staged cohort archive {}: {error}",
            completed.staged_output.display()
        )
    })?;
    let provenance = reader
        .provenance()
        .map_err(|error| format!("invalid cohort archive provenance: {error}"))?
        .ok_or_else(|| "cohort archive has no provenance".to_string())?;
    let provenance = provenance
        .single_runtime_v1()
        .ok_or_else(|| "cohort archive does not have single-runtime provenance".to_string())?;
    let expected_kind = if first_epoch {
        BootstrapStateKind::SnapshotArchive
    } else {
        BootstrapStateKind::CarriedBank
    };
    let evidence = &completed.historical_evidence.bootstrap;
    if provenance.bootstrap_state_kind != expected_kind
        || provenance.bootstrap_slot != evidence.slot
        || provenance.bootstrap_state_hash != Hash::new_from_array(evidence.accounts_hash)
    {
        return Err(format!(
            "cohort epoch {} archive bootstrap provenance does not match runtime evidence",
            completed.epoch
        ));
    }
    if !first_epoch {
        validate_carried_archive_anchor(completed.epoch, evidence, completed.archive_chain)?;
    }
    Ok(())
}

fn validate_carried_archive_anchor(
    epoch: u64,
    checkpoint: &historical_replay::HistoricalCheckpointSummary,
    chain: ArchiveChainEvidence,
) -> Result<(), String> {
    let expected = Hash::new_from_array(checkpoint.last_blockhash);
    let actual = chain
        .initial_poh_anchor
        .ok_or_else(|| format!("cohort epoch {epoch} archive has no initial PoH anchor"))?;
    if actual != expected {
        return Err(format!(
            "cohort epoch {epoch} starts from PoH hash {actual}, expected carried checkpoint hash {expected}"
        ));
    }
    if let Some(first_block) = chain.first_block
        && (first_block.parent_slot != checkpoint.slot || first_block.parent_blockhash != expected)
    {
        return Err(format!(
            "cohort epoch {epoch} first block {} names parent {} ({}), expected carried checkpoint {} ({})",
            first_block.slot,
            first_block.parent_slot,
            first_block.parent_blockhash,
            checkpoint.slot,
            expected
        ));
    }
    Ok(())
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
    replay_scratch_dir: &Path,
    bootstrap: &ReplayBootstrap,
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
    // Generation-pinned bootstrap evidence from a sealed cohort manifest.
    // This is valid only for a fresh historical-worker initialization.
    audited_cohort_bootstrap: Option<&BoundCohortSnapshot>,
    // A root-checkpoint cohort keeps one verifier across several epochs. The
    // terminal slot is used for candidate admission; only the final member
    // consumes the verifier with `finish`.
    verification_end_inclusive: Option<Slot>,
    retain_runtime_state: bool,
) -> Result<ReplayRunResult, String> {
    if qualification.is_some() && carried_state.is_some() {
        return Err("focused qualification cannot reuse carried runtime state".to_string());
    }
    if audited_cohort_bootstrap.is_some() && carried_state.is_some() {
        return Err(
            "an audited cohort bootstrap cannot accompany carried runtime state".to_string(),
        );
    }
    if let Some(binding) = audited_cohort_bootstrap {
        let Some(snapshot_path) = bootstrap.snapshot_archive() else {
            return Err("an audited cohort bootstrap requires a snapshot archive".to_string());
        };
        if snapshot_path != binding.path {
            return Err("audited cohort bootstrap evidence is bound to another path".to_string());
        }
    }
    // Resolve the exact state span before starting a worker or loading a bank.
    // Snapshot filename/format supplies only the bootstrap slot and loader
    // details; execution semantics come exclusively from the slot registry.
    let bootstrap_slot = match carried_state.as_ref() {
        Some(state) => state.slot()?,
        None => bootstrap.slot()?,
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
        None => {
            let replay_start = if carried_state.is_some() {
                initial_replay_slot(bootstrap_slot, epoch_start)?
            } else {
                replay_start_for_bootstrap(bootstrap, epoch, epoch_start)?
            };
            (epoch_start, replay_start, epoch_end_inclusive)
        }
    };
    let replay_end = end_inclusive.saturating_add(1);
    let execution = if carried_state.is_none() && bootstrap.snapshot_archive().is_some() {
        compatibility::select_runtime_with_snapshot_warmup(
            replay_start,
            output_slot_start..replay_end,
            allow_candidate_runtime,
        )?
    } else {
        compatibility::select_runtime(replay_start..replay_end, allow_candidate_runtime)?
    };
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
        let verification_end = verification_end_inclusive.unwrap_or(end_inclusive);
        if verification_end < end_inclusive {
            return Err(format!(
                "snapshot verification end {verification_end} precedes replay end {end_inclusive}"
            ));
        }
        let checkpoint_count = verifier.checkpoint_count_in_range(replay_start, verification_end);
        if checkpoint_count == 0 {
            return Err(format!(
                "runtime profile {} requires at least one trusted post-bootstrap checkpoint in verification range {}..={}; the supplied checkpoint set is empty for that range",
                execution.backend, replay_start, verification_end
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
        if state.completed_epoch().checked_add(1) != Some(epoch) {
            return Err(format!(
                "carried runtime state completed epoch {}, but replay requested epoch {}",
                state.completed_epoch(),
                epoch
            ));
        }
        if state.backend() != runtime_backend {
            return Err(format!(
                "carried runtime state uses {}, but slot range selected {}",
                state.backend(),
                runtime_backend
            ));
        }
        let (completed_start, completed_end) = epoch_to_slot_range(state.completed_epoch());
        if !(completed_start..=completed_end).contains(&bootstrap_slot) {
            return Err(format!(
                "carried runtime state at slot {bootstrap_slot} is outside completed epoch {} ({}..={})",
                state.completed_epoch(),
                completed_start,
                completed_end
            ));
        }
        if !runtime_descriptor.permits_live_epoch_handoff() {
            return Err(format!(
                "runtime profile {} does not permit a live epoch state handoff",
                runtime_descriptor.identity.name
            ));
        }
    } else if let Some(snapshot_archive) = bootstrap.snapshot_archive() {
        validate_runtime_bootstrap_archive(runtime_descriptor, snapshot_archive)?;
    } else if epoch != 0 || bootstrap_slot != 0 {
        return Err(format!(
            "genesis bootstrap is valid only for epoch 0 at slot 0, got epoch {epoch} slot {bootstrap_slot}"
        ));
    }
    // Registered generated handoffs require an adjacent evidence sidecar. The
    // complete archive digest covers replay-relevant state that the historical
    // accounts hash does not, including the transaction status cache.
    let bootstrap_handoff_manifest = match (carried_state.is_none(), bootstrap.snapshot_archive()) {
        (true, Some(snapshot_archive)) => {
            registered_handoff_bootstrap(runtime_descriptor, snapshot_archive)?
        }
        _ => None,
    };
    if audited_cohort_bootstrap.is_some() && bootstrap_handoff_manifest.is_some() {
        return Err(
            "a cohort bootstrap cannot also be admitted as a generated handoff snapshot"
                .to_string(),
        );
    }
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
        Historical {
            client: Box<historical::HistoricalRuntimeClient>,
            bootstrap_checkpoint: Option<historical_replay::HistoricalCheckpointSummary>,
        },
    }
    let (replay_source, snapshot_slot, bootstrap_state_kind, bootstrap_state_hash) =
        match runtime_backend {
            compatibility::RuntimeBackend::AgaveV3 => match carried_state {
                Some(CarriedRuntimeState::Agave { bank_forks, .. }) => {
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
                Some(CarriedRuntimeState::Historical { .. }) => {
                    return Err(
                        "a historical worker cannot be handed to the Agave runtime".to_string()
                    );
                }
                None => {
                    let snapshot_archive = bootstrap.snapshot_archive().ok_or_else(|| {
                        "the in-process Agave runtime cannot initialize from historical genesis"
                            .to_string()
                    })?;
                    let accounts_update_notifier: Option<AccountsUpdateNotifier> =
                        Some(Arc::new(ProgressAccountsUpdateNotifier {
                            progress: progress.clone(),
                            live_start_slot: output_slot_start,
                        }) as AccountsUpdateNotifier);
                    info!("accounts update notifier wired into snapshot load: true");
                    info!("loading Agave bank from snapshot");
                    let ledger_dir_for_load = ledger_dir.clone();
                    let replay_scratch_for_load = replay_scratch_dir.to_path_buf();
                    let snapshot_archive = snapshot_archive.to_path_buf();
                    let use_dir_loader = env_truthy("JETSTREAMER_LOAD_FROM_DIR");
                    let bank = tokio::task::spawn_blocking(move || {
                        if use_dir_loader {
                            load_bank_from_snapshot(&ledger_dir_for_load, accounts_update_notifier)
                        } else {
                            load_bank_from_snapshot_archive(
                                &ledger_dir_for_load,
                                &replay_scratch_for_load,
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
            | compatibility::RuntimeBackend::SolanaV1_0_13
            | compatibility::RuntimeBackend::SolanaV1_0_14
            | compatibility::RuntimeBackend::SolanaV1_0_17
            | compatibility::RuntimeBackend::SolanaV1_0_18
            | compatibility::RuntimeBackend::SolanaV1_0_23
            | compatibility::RuntimeBackend::SolanaV1_0_24
            | compatibility::RuntimeBackend::SolanaV1_1_23
            | compatibility::RuntimeBackend::SolanaV1_2_32
            | compatibility::RuntimeBackend::SolanaV1_3_19 => {
                let worker_profile = historical_worker_profile(runtime_descriptor)?;
                if let Some(CarriedRuntimeState::Historical {
                    client,
                    terminal,
                    worker_executable_sha256,
                    ..
                }) = carried_state
                {
                    if client.executable_sha256() != worker_executable_sha256 {
                        return Err(
                            "carried historical worker executable digest changed".to_string()
                        );
                    }
                    let state_hash = Hash::new_from_array(terminal.accounts_hash);
                    info!(
                        "reusing historical worker from previous epoch at slot {}; skipping snapshot load",
                        terminal.slot
                    );
                    (
                        ReplaySource::Historical {
                            client,
                            bootstrap_checkpoint: Some(terminal),
                        },
                        bootstrap_slot,
                        BootstrapStateKind::CarriedBank,
                        state_hash,
                    )
                } else {
                    if carried_state.is_some() {
                        return Err(
                            "an Agave in-memory bank cannot be handed to a Solana v1 worker"
                                .to_string(),
                        );
                    }
                    let executable = configured_historical_worker_executable(runtime_descriptor)?;
                    let scratch_parent = replay_scratch_dir.join(".historical-runtime");
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
                    if let Some(binding) = audited_cohort_bootstrap {
                        binding.revalidate()?;
                    }
                    let (initialization, bootstrap_state_kind, bootstrap_state_hash) =
                        match bootstrap {
                            ReplayBootstrap::Genesis {
                                genesis_bin_path,
                                identity,
                                ..
                            } => {
                                let genesis_hash = runtime_descriptor
                            .identity
                            .genesis_hash
                            .parse::<Hash>()
                            .map_err(|err| {
                                format!(
                                    "runtime descriptor {} has invalid genesis hash {}: {err}",
                                    runtime_descriptor.identity.name,
                                    runtime_descriptor.identity.genesis_hash
                                )
                            })?;
                                (
                                    historical::HistoricalInitialization::Genesis(
                                        historical::GenesisInitialization {
                                            genesis_bin_path: genesis_bin_path.clone(),
                                            expected_size: identity.size,
                                            expected_sha256: identity.sha256,
                                            scratch_parent: Some(scratch_parent),
                                        },
                                    ),
                                    BootstrapStateKind::Genesis,
                                    genesis_hash,
                                )
                            }
                            ReplayBootstrap::SnapshotArchive(snapshot_archive) => {
                                let archive_name = snapshot_archive
                                    .file_name()
                                    .and_then(|name| name.to_str())
                                    .ok_or_else(|| {
                                        format!(
                                            "historical snapshot path has no UTF-8 filename: {}",
                                            snapshot_archive.display()
                                        )
                                    })?;
                                let (expected_slot, expected_hash) =
                                    parse_snapshot_archive_name(archive_name)?;
                                (
                                    historical::HistoricalInitialization::SnapshotArchive(
                                        historical::SnapshotInitialization {
                                            ledger_path: ledger_dir.clone(),
                                            archive_path: snapshot_archive.clone(),
                                            expected_slot,
                                            expected_accounts_hash: expected_hash.0.to_bytes(),
                                            expected_archive_sha256: audited_cohort_bootstrap
                                                .map(|binding| binding.evidence.sha256)
                                                .or_else(|| {
                                                    bootstrap_handoff_manifest
                                                        .as_ref()
                                                        .map(|manifest| manifest.archive_sha256)
                                                }),
                                            expected_archive_size: audited_cohort_bootstrap
                                                .map(|binding| binding.size)
                                                .or_else(|| {
                                                    bootstrap_handoff_manifest
                                                        .as_ref()
                                                        .map(|manifest| manifest.archive_size)
                                                }),
                                            scratch_parent: Some(scratch_parent),
                                        },
                                    ),
                                    BootstrapStateKind::SnapshotArchive,
                                    expected_hash.0,
                                )
                            }
                        };
                    let spawn = historical::WorkerSpawn {
                        executable,
                        initialization,
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
                        ReplaySource::Historical {
                            client: Box::new(client),
                            bootstrap_checkpoint: None,
                        },
                        slot,
                        bootstrap_state_kind,
                        bootstrap_state_hash,
                    )
                }
            }
        };
    let bootstrap_last_blockhash = match &replay_source {
        ReplaySource::Agave(BankSource::Reuse(bank_forks)) => bank_forks
            .read()
            .map_err(|_| "bank forks lock poisoned".to_string())?
            .working_bank()
            .last_blockhash(),
        ReplaySource::Agave(BankSource::Fresh(bank)) => bank.last_blockhash(),
        ReplaySource::Historical {
            client,
            bootstrap_checkpoint,
        } => bootstrap_checkpoint
            .as_ref()
            .map(|checkpoint| Hash::new_from_array(checkpoint.last_blockhash))
            .unwrap_or_else(|| Hash::new_from_array(client.initialized().last_blockhash)),
    };
    if snapshot_slot != bootstrap_slot {
        return Err(format!(
            "loaded state slot {} does not match the preflighted bootstrap slot {}",
            snapshot_slot, bootstrap_slot,
        ));
    }
    let bootstrap_parent_anchor = replay_start
        .checked_sub(1)
        .filter(|parent_slot| *parent_slot == snapshot_slot)
        .map(|parent_slot| (parent_slot, bootstrap_last_blockhash));
    let expected_initial_parent = (replay_start == output_slot_start)
        .then_some(bootstrap_parent_anchor)
        .flatten();
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
        ReplaySource::Historical { client, .. } => Some(client.executable_sha256()),
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
        expected_initial_parent,
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
        ReplaySource::Historical {
            client,
            bootstrap_checkpoint,
        } => Arc::new(match bootstrap_checkpoint {
            Some(checkpoint) => historical_replay::HistoricalReplay::from_carried(
                *client,
                checkpoint,
                snapshot_verifier.clone(),
                failure.clone(),
                cursor.clone(),
                progress.clone(),
                output_slot_start,
            )?,
            None => historical_replay::HistoricalReplay::new(
                *client,
                snapshot_verifier.clone(),
                failure.clone(),
                cursor.clone(),
                progress.clone(),
                output_slot_start,
            )?,
        }),
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
    let ready_producers = ReplayReadyEntryProducers {
        transaction: Arc::new(BankTransactionNotifier {
            progress: progress.clone(),
            scheduler: scheduler.clone(),
            failure: failure.clone(),
            ready_sender: ready_sender.clone(),
            shutdown: shutdown.clone(),
            active_firehose_stop: active_firehose_stop.clone(),
            backpressure_stop_requested: backpressure_stop_requested.clone(),
            firehose_backpressure_slot_gap_limit,
            firehose_gate: firehose_gate.clone(),
        }),
        entry: Arc::new(BankEntryNotifier {
            progress: progress.clone(),
            scheduler: scheduler.clone(),
            failure: failure.clone(),
            ready_sender: ready_sender.clone(),
            shutdown: shutdown.clone(),
            active_firehose_stop: active_firehose_stop.clone(),
            backpressure_stop_requested: backpressure_stop_requested.clone(),
            firehose_backpressure_slot_gap_limit,
            firehose_gate: firehose_gate.clone(),
        }),
        block_parent: Arc::new(BankBlockParentNotifier {
            scheduler: scheduler.clone(),
            failure: failure.clone(),
            ready_sender: ready_sender.clone(),
            firehose_gate: firehose_gate.clone(),
        }),
        block_metadata: Arc::new(BankBlockMetadataNotifier {
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
        }),
    };
    let notifiers = ready_producers.geyser_notifiers();
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
    info!(
        "post-firehose incomplete retry attempts: {}",
        post_firehose_retries
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
    let mut boundary_parent_edge: Option<(Slot, Slot)> = None;
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
            let block_parent_notifier = ready_producers.block_parent.clone();
            let initial_parent = if firehose_start == replay_start {
                bootstrap_parent_anchor
            } else {
                None
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
                firehose_geyser_with_notifiers_and_block_parent(
                    rt,
                    slot_range,
                    notifiers,
                    Some(block_parent_notifier),
                    initial_parent,
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

        if !stopped_by_backpressure && scheduler.snapshot().current_slot <= end_inclusive {
            if boundary_parent_edge.is_none() {
                let boundary_start = end_inclusive.checked_add(1).ok_or_else(|| {
                    format!("end slot {end_inclusive} has no representable successor")
                })?;
                let mut last_error = None;
                for attempt in 1..=3 {
                    if shutdown.load(Ordering::Relaxed) {
                        break;
                    }
                    match fetch_first_block_edge_at_or_after(boundary_start, &client).await {
                        Ok(edge) => {
                            boundary_parent_edge = Some(edge);
                            break;
                        }
                        Err(err) => {
                            warn!(
                                "failed to fetch boundary parent edge after slot {end_inclusive} (attempt {attempt}/3): {err}"
                            );
                            last_error = Some(err);
                            if attempt < 3 {
                                tokio::time::sleep(Duration::from_secs(attempt)).await;
                            }
                        }
                    }
                }
                if boundary_parent_edge.is_none() && !shutdown.load(Ordering::Relaxed) {
                    failure.record(format!(
                        "could not prove the replay tail after slot {end_inclusive}: {}",
                        last_error.unwrap_or_else(|| "boundary scan stopped".to_string())
                    ));
                }
            }

            if let Some((parent_slot, child_slot)) = boundary_parent_edge {
                if parent_slot > end_inclusive {
                    failure.record(format!(
                        "boundary scan reached block {child_slot} with parent {parent_slot}; it skipped the first canonical successor after replay end {end_inclusive}"
                    ));
                } else {
                    info!(
                        "boundary block {child_slot} parent {parent_slot} supplies decoded tail evidence through slot {end_inclusive}"
                    );
                    match scheduler.record_block_parent(parent_slot, child_slot) {
                        Ok(ready_entries) => {
                            send_ready_entries(&ready_sender, &failure, ready_entries);
                        }
                        Err(err) => failure.record(format!(
                            "boundary parent edge {parent_slot}->{child_slot} was rejected: {err}"
                        )),
                    }
                }
            }
        }

        if let Err(err) = scheduler.verify_complete(end_inclusive) {
            if let Some(incomplete) = scheduler.first_incomplete_slot(end_inclusive) {
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
    // Drop every producer before joining the channel consumer. The explicit
    // owner also covers the block-parent notifier, which is not part of
    // `GeyserNotifiers`.
    let _ = close_ready_entry_channel(notifiers, ready_producers, ready_sender, ready_handle);
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

    if let Some(verifier) = snapshot_verifier.as_ref() {
        replay_executor.verify_latest_bank()?;
        if verification_end_inclusive
            .is_none_or(|verification_end| end_inclusive == verification_end)
        {
            verifier.finish()?;
            info!("snapshot verification complete");
        } else {
            info!(
                "snapshot verification remains open through slot {}",
                verification_end_inclusive.expect("deferred verification has a terminal slot")
            );
        }
    }

    if horizon::recorder().is_some() {
        // Freeze the final bank so its end-of-slot account updates are
        // recorded before the archive closes.
        replay_executor.freeze_latest_bank()?;
        // Finalize and tear down this epoch's archive so the next epoch in a
        // range run can install its own.
        horizon::finish()?;
    }

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
    let carried_state = if retain_runtime_state {
        if let Some(replay) = agave_bank_replay {
            Some(CarriedRuntimeState::Agave {
                backend: runtime_backend,
                completed_epoch: epoch,
                bank_forks: replay.bank_forks(),
            })
        } else {
            let evidence = historical_evidence.as_ref().ok_or_else(|| {
                "historical runtime carry requires terminal checkpoint evidence".to_string()
            })?;
            let client = replay_executor.take_historical_client()?.ok_or_else(|| {
                "historical runtime did not expose a carryable worker".to_string()
            })?;
            Some(CarriedRuntimeState::Historical {
                backend: runtime_backend,
                completed_epoch: epoch,
                client: Box::new(client),
                terminal: evidence.terminal.clone(),
                worker_executable_sha256: worker_executable_sha256.ok_or_else(|| {
                    "historical runtime carry has no worker executable digest".to_string()
                })?,
            })
        }
    } else {
        replay_executor.shutdown()?;
        None
    };
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
fn require_archive_batch_closed_for_reuse(path: &Path) -> Result<(), String> {
    let destination = path.parent().unwrap_or_else(|| Path::new("."));
    if jetstreamer_node::archive_checksum::archive_batch_publication_in_progress(destination)
        .map_err(|error| {
            format!(
                "failed to inspect archive batch state in {}: {error}",
                destination.display()
            )
        })?
    {
        return Err(format!(
            "destination {} has an active or unacknowledged archive batch; recovery must finish before archive reuse",
            destination.display()
        ));
    }
    Ok(())
}

fn validated_epoch_archive(
    path: &Path,
    epoch: u64,
    selection: compatibility::RuntimeSelection,
    cancellation: Option<&AtomicBool>,
    chain_evidence: Option<&mut ArchiveChainEvidence>,
) -> Result<Option<jetstreamer_node::archive_checksum::ValidatedArchiveFile>, String> {
    require_archive_batch_closed_for_reuse(path)?;
    let file = match jetstreamer_node::archive_checksum::open_regular_nofollow(path) {
        Ok(file) => file,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(format!("failed to open {}: {err}", path.display())),
    };
    let initial_bytes = file
        .metadata()
        .map_err(|err| format!("failed to inspect archive {}: {err}", path.display()))?
        .len();
    let initial_identity = jetstreamer_node::archive_checksum::archive_file_identity(&file)
        .map_err(|err| format!("failed to identify archive {}: {err}", path.display()))?;
    let measurement_file = file
        .try_clone()
        .map_err(|err| format!("failed to bind archive {}: {err}", path.display()))?;
    let reader =
        match jetstreamer_horizon::archive::ArchiveReader::open(std::io::BufReader::new(file)) {
            Ok(reader) => reader,
            Err(
                error @ (ArchiveFormatError::Io(_) | ArchiveFormatError::AllocationFailed { .. }),
            ) => {
                return Err(format!(
                    "failed to read archive framing {}: {error}",
                    path.display()
                ));
            }
            Err(_) => return Ok(None),
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
    let audited_recovery = audited_archive_recovery_provenance_matches(epoch, &provenance);
    if let Some(recovery) = audited_recovery
        && initial_bytes != recovery.archive_bytes
    {
        return Err(format!(
            "epoch-{} archive {} has {} bytes, expected {} for the audited content identity",
            recovery.epoch,
            path.display(),
            initial_bytes,
            recovery.archive_bytes,
        ));
    }
    let identity = selection.descriptor.identity;
    let expected_genesis = identity.genesis_hash.parse::<Hash>().map_err(|err| {
        format!(
            "runtime descriptor {} has invalid genesis hash {}: {err}",
            identity.name, identity.genesis_hash
        )
    })?;
    let expected_toolchain = archive_runtime_toolchain(identity);
    let expected_metadata = archive_transaction_metadata_policy(slot_start, expected_slot_count);
    if !(runtime_generation_profile_is_compatible(
        &provenance_v1.runtime_profile,
        &provenance_v1.generation_profile,
    ) || audited_recovery.is_some())
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

    let expected_worker_executable_sha256 = if let Some(recovery) = audited_recovery {
        Some(measure_audited_archive_recovery_worker(
            recovery,
            selection.descriptor,
        )?)
    } else if selection.descriptor.worker.is_some() {
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
            validate_genesis_bootstrap_commitment(
                path,
                epoch,
                provenance_v1.bootstrap_slot,
                provenance_v1.bootstrap_state_hash,
                expected_genesis,
            )?;
        }
        BootstrapStateKind::SnapshotArchive | BootstrapStateKind::CarriedBank => {
            if epoch == 0 {
                return Err(format!(
                    "completed epoch-0 archive {} did not bootstrap from genesis",
                    path.display()
                ));
            }
            validate_epoch_bootstrap_identity(
                epoch,
                provenance_v1.bootstrap_slot,
                provenance_v1.bootstrap_state_hash,
            )
            .map_err(|err| {
                format!(
                    "completed archive {} has invalid bootstrap identity: {err}",
                    path.display()
                )
            })?;
            if provenance_v1.bootstrap_state_kind == BootstrapStateKind::CarriedBank
                && !selection.descriptor.permits_live_epoch_handoff()
            {
                return Err(format!(
                    "completed archive {} claims a carried-bank bootstrap for runtime {}",
                    path.display(),
                    identity.name
                ));
            }
        }
    }
    let validated = verify_open_archive_payload(
        path,
        reader,
        &measurement_file,
        epoch,
        slot_start,
        expected_slot_count,
        &provenance,
        initial_identity,
        cancellation,
        chain_evidence,
    )?;
    if let Some(recovery) = audited_recovery
        && !audited_archive_content_matches(recovery, initial_bytes, validated.sha256)
    {
        return Err(format!(
            "epoch-{} archive {} does not match the audited content identity",
            recovery.epoch,
            path.display(),
        ));
    }
    Ok(Some(validated))
}

fn epoch_archive_reusable(
    path: &Path,
    epoch: u64,
    selection: compatibility::RuntimeSelection,
) -> Result<bool, String> {
    validated_epoch_archive(path, epoch, selection, None, None).map(|validated| validated.is_some())
}

/// Determines whether an assembled multi-runtime epoch is safe to reuse.
///
/// V3 provenance is checked against the live slot registry and the executable
/// bytes currently configured for every historical worker. A structurally
/// valid archive made with an older routing decision therefore cannot silently
/// bypass a newly tightened compatibility boundary.
fn validated_epoch_archive_multi_runtime(
    path: &Path,
    epoch: u64,
    spans: &[compatibility::RuntimeSpan],
    cancellation: Option<&AtomicBool>,
) -> Result<Option<jetstreamer_node::archive_checksum::ValidatedArchiveFile>, String> {
    require_archive_batch_closed_for_reuse(path)?;
    if spans.len() < 2 {
        return Err("multi-runtime archive validation requires at least two spans".to_string());
    }
    let file = match jetstreamer_node::archive_checksum::open_regular_nofollow(path) {
        Ok(file) => file,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(format!("failed to open {}: {err}", path.display())),
    };
    let initial_identity = jetstreamer_node::archive_checksum::archive_file_identity(&file)
        .map_err(|err| format!("failed to identify archive {}: {err}", path.display()))?;
    let measurement_file = file
        .try_clone()
        .map_err(|err| format!("failed to bind archive {}: {err}", path.display()))?;
    let reader =
        match jetstreamer_horizon::archive::ArchiveReader::open(std::io::BufReader::new(file)) {
            Ok(reader) => reader,
            Err(
                error @ (ArchiveFormatError::Io(_) | ArchiveFormatError::AllocationFailed { .. }),
            ) => {
                return Err(format!(
                    "failed to read archive framing {}: {error}",
                    path.display()
                ));
            }
            Err(_) => return Ok(None),
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
    if provenance.transaction_metadata
        != archive_transaction_metadata_policy(slot_start, slot_count)
    {
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
            validate_genesis_bootstrap_commitment(
                path,
                epoch,
                provenance.bootstrap_state.slot,
                provenance.bootstrap_state.hash,
                provenance.genesis_hash,
            )?;
        }
        BootstrapStateKind::SnapshotArchive | BootstrapStateKind::CarriedBank => {
            if epoch == 0 {
                return Err(format!(
                    "assembled epoch-0 archive {} did not bootstrap from genesis",
                    path.display()
                ));
            }
            validate_epoch_bootstrap_identity(
                epoch,
                provenance.bootstrap_state.slot,
                provenance.bootstrap_state.hash,
            )
            .map_err(|err| {
                format!(
                    "assembled archive {} has invalid bootstrap identity: {err}",
                    path.display()
                )
            })?;
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
        | compatibility::RuntimeBackend::SolanaV1_0_13
        | compatibility::RuntimeBackend::SolanaV1_0_14
        | compatibility::RuntimeBackend::SolanaV1_0_17
        | compatibility::RuntimeBackend::SolanaV1_0_18
        | compatibility::RuntimeBackend::SolanaV1_0_23
        | compatibility::RuntimeBackend::SolanaV1_0_24
        | compatibility::RuntimeBackend::SolanaV1_1_23
        | compatibility::RuntimeBackend::SolanaV1_2_32
        | compatibility::RuntimeBackend::SolanaV1_3_19 => StateCommitmentKind::LegacyAccountsHash,
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
            || !runtime_generation_profile_is_compatible(
                &recorded.runtime_profile,
                &recorded.generation_profile,
            )
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
    verify_open_archive_payload(
        path,
        reader,
        &measurement_file,
        epoch,
        slot_start,
        slot_count,
        &ArchiveProvenance::V3(provenance),
        initial_identity,
        cancellation,
        None,
    )
    .map(Some)
}

fn epoch_archive_reusable_multi_runtime(
    path: &Path,
    epoch: u64,
    spans: &[compatibility::RuntimeSpan],
) -> Result<bool, String> {
    validated_epoch_archive_multi_runtime(path, epoch, spans, None)
        .map(|validated| validated.is_some())
}

fn validate_genesis_bootstrap_commitment(
    path: &Path,
    epoch: u64,
    bootstrap_slot: Slot,
    bootstrap_hash: Hash,
    expected_genesis: Hash,
) -> Result<(), String> {
    if epoch != 0 || bootstrap_slot != 0 {
        return Err(format!(
            "completed archive {} claims an invalid genesis bootstrap at slot {} for epoch {}",
            path.display(),
            bootstrap_slot,
            epoch
        ));
    }
    if bootstrap_hash != expected_genesis {
        return Err(format!(
            "completed epoch-0 archive {} has genesis bootstrap commitment {}, expected registered mainnet genesis {}",
            path.display(),
            bootstrap_hash,
            expected_genesis,
        ));
    }
    Ok(())
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
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| format!("snapshot path has no UTF-8 filename: {}", path.display()))?;
    let (slot, SnapshotHash(accounts_hash)) = parse_snapshot_archive_name(name)?;
    validate_epoch_bootstrap_identity(epoch, slot, accounts_hash)
        .map_err(|err| format!("{err} ({})", path.display()))?;
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
    let bounds = normal_epoch_bootstrap_bounds(epoch)?;
    if let Some(candidate) = find_existing_snapshot_archive(dest_dir, bounds, archive_extensions)? {
        validate_epoch_bootstrap_snapshot(epoch, &candidate.path)?;
        info!(
            "epoch {epoch}: boundary snapshot already present at {}",
            candidate.path.display()
        );
        return Ok(candidate.path);
    }
    info!(
        "epoch {epoch}: downloading boundary snapshot (target slot {})",
        bounds.max_slot
    );
    let path = download_snapshot_at_or_before_slot_matching(
        epoch,
        bounds.max_slot,
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
/// archive filename per line) back into the expectations map. The selected
/// runtime defines both the accepted containers and the filename hash scheme;
/// compression alone carries no hash semantics.
fn read_epoch_hashes_file(
    path: &Path,
    bootstrap: compatibility::BootstrapState,
) -> Result<BTreeMap<Slot, BankHashExpectation>, String> {
    use std::io::Read as _;

    const MAX_EPOCH_HASHES_FILE_BYTES: u64 = 1024 * 1024;
    let mut file =
        jetstreamer_node::archive_checksum::open_regular_nofollow(path).map_err(|err| {
            format!(
                "failed to open snapshot hashes file {}: {err}",
                path.display()
            )
        })?;
    let identity =
        jetstreamer_node::archive_checksum::archive_file_identity(&file).map_err(|err| {
            format!(
                "failed to identify snapshot hashes file {}: {err}",
                path.display()
            )
        })?;
    let length = file
        .metadata()
        .map_err(|err| {
            format!(
                "failed to inspect snapshot hashes file {}: {err}",
                path.display()
            )
        })?
        .len();
    if length > MAX_EPOCH_HASHES_FILE_BYTES {
        return Err(format!(
            "snapshot hashes file {} is {length} bytes (limit {MAX_EPOCH_HASHES_FILE_BYTES})",
            path.display()
        ));
    }
    let mut contents = String::with_capacity(usize::try_from(length).unwrap_or(0));
    file.read_to_string(&mut contents).map_err(|err| {
        format!(
            "failed to read snapshot hashes file {}: {err}",
            path.display()
        )
    })?;
    if contents.len() as u64 != length
        || jetstreamer_node::archive_checksum::archive_file_identity(&file).map_err(|err| {
            format!(
                "failed to recheck snapshot hashes file {}: {err}",
                path.display()
            )
        })? != identity
    {
        return Err(format!(
            "snapshot hashes file changed while it was read: {}",
            path.display()
        ));
    }
    let mut expected = BTreeMap::new();
    for line in contents.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        if !bootstrap.accepts_archive_name(line) {
            return Err(format!(
                "snapshot entry {line:?} in {} is incompatible with the slot-selected runtime",
                path.display()
            ));
        }
        let (slot, hash) = parse_snapshot_archive_name(line)?;
        let expectation = snapshot_hash_expectation(hash, bootstrap.snapshot_hash_kind);
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

fn snapshot_path_expectation(
    path: &Path,
    bootstrap: compatibility::BootstrapState,
) -> Result<(Slot, BankHashExpectation), String> {
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| format!("snapshot path has no UTF-8 filename: {}", path.display()))?;
    if !bootstrap.accepts_archive_name(name) {
        return Err(format!(
            "snapshot {} is incompatible with the slot-selected runtime",
            path.display()
        ));
    }
    let (slot, hash) = parse_snapshot_archive_name(name)?;
    let expectation = snapshot_hash_expectation(hash, bootstrap.snapshot_hash_kind);
    Ok((slot, expectation))
}

fn add_boundary_snapshot_expectation(
    expected: &mut BTreeMap<Slot, BankHashExpectation>,
    boundary_path: &Path,
    bootstrap: compatibility::BootstrapState,
) -> Result<(), String> {
    let (slot, expectation) = snapshot_path_expectation(boundary_path, bootstrap)?;
    match expected.get(&slot) {
        Some(existing) if *existing != expectation => Err(format!(
            "canonical snapshot listing conflicts with boundary snapshot at slot {slot}: listing={existing:?}, boundary={expectation:?}"
        )),
        Some(_) => Ok(()),
        None => {
            warn!(
                "canonical snapshot listing omitted boundary file {}; adding its {:?} commitment at slot {slot}",
                boundary_path.display(),
                bootstrap.snapshot_hash_kind
            );
            expected.insert(slot, expectation);
            Ok(())
        }
    }
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
        let bootstrap = handoff.destination.bootstrap;
        if !bootstrap.accepts_archive_name(handoff.snapshot.archive_name().as_str()) {
            return Err(format!(
                "runtime handoff at slot {} uses snapshot format {:?}, which destination runtime {} does not accept",
                handoff.boundary_slot,
                handoff.snapshot.archive_extension,
                handoff.destination.identity.name
            ));
        }
        let hash = handoff.snapshot.accounts_hash()?;
        let value = snapshot_hash_expectation(
            SnapshotHash(hash),
            handoff.destination.bootstrap.snapshot_hash_kind,
        );
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
    bootstrap: compatibility::BootstrapState,
) -> Result<BTreeMap<Slot, BankHashExpectation>, String> {
    let (snapshot_slot, snapshot_expectation) =
        snapshot_path_expectation(snapshot_archive, bootstrap)?;
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
        || !segment_runtime_identity_is_compatible(
            &manifest.source_runtime,
            &expectation.source_runtime,
        )
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
) -> Result<jetstreamer_node::archive_checksum::ValidatedArchiveFile, String> {
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
) -> Result<jetstreamer_node::archive_checksum::ValidatedArchiveFile, String> {
    let file = jetstreamer_node::archive_checksum::open_regular_nofollow(path)
        .map_err(|err| format!("failed to open assembled archive {}: {err}", path.display()))?;
    let initial_identity = jetstreamer_node::archive_checksum::archive_file_identity(&file)
        .map_err(|err| format!("failed to identify archive {}: {err}", path.display()))?;
    let measurement_file = file
        .try_clone()
        .map_err(|err| format!("failed to bind assembled archive {}: {err}", path.display()))?;
    let reader =
        jetstreamer_horizon::archive::ArchiveReader::open(std::io::BufReader::new(file))
            .map_err(|err| format!("failed to open assembled archive {}: {err}", path.display()))?;
    verify_open_archive_payload(
        path,
        reader,
        &measurement_file,
        epoch,
        expected_start,
        expected_count,
        &ArchiveProvenance::V3(provenance.clone()),
        initial_identity,
        None,
        None,
    )
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ArchiveBlockEvidence {
    slot: Slot,
    parent_slot: Slot,
    parent_blockhash: Hash,
    blockhash: Hash,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct ArchiveChainEvidence {
    initial_poh_anchor: Option<Hash>,
    first_block: Option<ArchiveBlockEvidence>,
    terminal_block: Option<ArchiveBlockEvidence>,
}

struct ArchiveVerificationVisitor {
    digest: SemanticDigest,
    chain: ArchiveChainEvidence,
}

impl ArchiveVerificationVisitor {
    fn new() -> Self {
        Self {
            digest: SemanticDigest::new(),
            chain: ArchiveChainEvidence::default(),
        }
    }

    fn on_bucket_header(
        &mut self,
        header: &HorizonBucketHeader,
    ) -> Result<(), jetstreamer_horizon::archive::ArchiveFormatError> {
        let expected = self
            .chain
            .terminal_block
            .map(|block| block.blockhash)
            .or(self.chain.initial_poh_anchor);
        match expected {
            Some(expected) if header.poh_start_hash != expected => Err(
                jetstreamer_horizon::archive::ArchiveFormatError::PohMismatch {
                    slot: header.first_slot,
                },
            ),
            Some(_) => Ok(()),
            None => {
                self.chain.initial_poh_anchor = Some(header.poh_start_hash);
                Ok(())
            }
        }
    }
}

impl SlotVisitor for ArchiveVerificationVisitor {
    fn on_slot_start(&mut self, slot: u64, kind: SlotKind) {
        self.digest.on_slot_start(slot, kind);
    }

    fn on_epoch(&mut self, meta: &EpochMeta) {
        self.digest.on_epoch(meta);
    }

    fn on_pre_account_update(&mut self, slot: u64, update: &AccountUpdateView<'_>) {
        self.digest.on_pre_account_update(slot, update);
    }

    fn on_transaction(&mut self, slot: u64, tx_index: u32, tx: &HorizonTransaction) {
        self.digest.on_transaction(slot, tx_index, tx);
    }

    fn on_post_account_update(&mut self, slot: u64, update: &AccountUpdateView<'_>) {
        self.digest.on_post_account_update(slot, update);
    }

    fn on_block(&mut self, notification: &BlockNotification, entries: &[EntryRecord]) {
        self.digest.on_block(notification, entries);
        if let BlockNotification::Block(meta) = notification {
            let block = ArchiveBlockEvidence {
                slot: meta.slot,
                parent_slot: meta.parent_slot,
                parent_blockhash: meta.parent_blockhash,
                blockhash: meta.blockhash,
            };
            self.chain.first_block.get_or_insert(block);
            self.chain.terminal_block = Some(block);
        }
    }

    fn consumption(&self) -> Consumption {
        self.digest.consumption()
    }
}

#[allow(clippy::too_many_arguments)]
fn verify_open_archive_payload(
    path: &Path,
    mut reader: jetstreamer_horizon::archive::ArchiveReader<std::io::BufReader<fs::File>>,
    measurement_file: &fs::File,
    expected_epoch: u64,
    expected_start: Slot,
    expected_count: u64,
    expected_provenance: &ArchiveProvenance,
    initial_identity: jetstreamer_node::archive_checksum::ArchiveFileIdentity,
    cancellation: Option<&AtomicBool>,
    chain_evidence: Option<&mut ArchiveChainEvidence>,
) -> Result<jetstreamer_node::archive_checksum::ValidatedArchiveFile, String> {
    let header = reader.header().clone();
    if header.epoch != expected_epoch
        || header.slot_start != expected_start
        || header.slot_count != expected_count
    {
        return Err(format!(
            "archive {} has unexpected header epoch={} slots={}+{}",
            path.display(),
            header.epoch,
            header.slot_start,
            header.slot_count
        ));
    }
    let recorded = reader
        .provenance()
        .map_err(|err| format!("archive {} has invalid provenance: {err}", path.display()))?
        .ok_or_else(|| format!("archive {} has no provenance", path.display()))?;
    if &recorded != expected_provenance {
        return Err(format!(
            "archive {} provenance changed during validation",
            path.display()
        ));
    }
    reader.verify_chain = true;
    let mut visitor = ArchiveVerificationVisitor::new();
    let mut visited = 0u64;
    for bucket in 0..reader.bucket_count() {
        if cancellation.is_some_and(|cancelled| cancelled.load(Ordering::Relaxed)) {
            return Err(format!("archive validation cancelled: {}", path.display()));
        }
        visited = visited
            .checked_add(
                reader
                    .read_bucket_with_header(bucket, &mut visitor, |header, visitor| {
                        visitor.on_bucket_header(header)
                    })
                    .map_err(|err| {
                        format!("archive {} failed full decode: {err}", path.display())
                    })?,
            )
            .ok_or_else(|| "assembled archive decoded slot count overflow".to_string())?;
    }
    let ArchiveVerificationVisitor { digest, chain } = visitor;
    digest
        .finish()
        .map_err(|err| format!("archive semantic verification failed: {err}"))?;
    if visited != expected_count {
        return Err(format!(
            "archive {} decoded {visited} slots, expected {expected_count}",
            path.display()
        ));
    }
    let validated = match cancellation {
        Some(cancelled) => jetstreamer_node::archive_checksum::measure_open_archive_cancellable(
            measurement_file,
            cancelled,
        ),
        None => jetstreamer_node::archive_checksum::measure_open_archive(measurement_file),
    }
    .map_err(|err| {
        format!(
            "failed to hash fully validated archive {}: {err}",
            path.display()
        )
    })?;
    if validated.identity != initial_identity {
        return Err(format!(
            "archive inode {} changed between admission, full decode, and hashing",
            path.display()
        ));
    }
    if !jetstreamer_node::archive_checksum::path_matches_archive_identity(path, validated.identity)
        .map_err(|err| format!("failed to recheck archive {}: {err}", path.display()))?
    {
        return Err(format!(
            "archive path {} changed during full validation",
            path.display()
        ));
    }
    if let Some(output) = chain_evidence {
        *output = chain;
    }
    Ok(validated)
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
        if let Ok(checksum) = jetstreamer_node::archive_checksum::archive_checksum_path(path)
            && checksum.exists()
        {
            let backup_checksum = jetstreamer_node::archive_checksum::archive_checksum_path(
                &backup,
            )
            .map_err(|err| {
                format!(
                    "failed to resolve checksum backup for {}: {err}",
                    backup.display()
                )
            })?;
            fs::rename(&checksum, &backup_checksum).map_err(|err| {
                format!(
                    "preserved {} but failed to preserve its checksum {}: {err}",
                    path.display(),
                    checksum.display()
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

fn publish_verified_runtime_assembly(
    epoch: u64,
    staged_archive: &Path,
    final_output: &Path,
    evidence: jetstreamer_node::archive_checksum::ValidatedArchiveFile,
) -> Result<jetstreamer_node::archive_publish::ArchivePublication, String> {
    jetstreamer_node::archive_publish::publish_verified_archive(
        staged_archive,
        final_output,
        evidence,
    )
    .map_err(|error| {
        format!(
            "failed to transactionally publish verified multi-runtime epoch {epoch} archive (commit_state={:?}): {error}",
            error.commit_state()
        )
    })
}

/// Runs and assembles every runtime span inside one epoch. Segment archives
/// and manifests remain in a private adjacent directory until the complete V3
/// output is verified and atomically published, so interruption is resumable.
#[allow(clippy::too_many_arguments)]
async fn run_multi_runtime_epoch_supervisor(
    epoch: u64,
    dest_dir: &Path,
    replay_scratch: Option<&Path>,
    initial_snapshot: &Path,
    epoch_hashes: &Path,
    final_output: &Path,
    allow_candidate_runtime: bool,
    publish_checksum: bool,
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
    let handoff_dir = replay_scratch
        .map(|scratch| scratch.join("runtime-handoffs"))
        .unwrap_or_else(|| dest_dir.to_path_buf());
    fs::create_dir_all(&handoff_dir)
        .map_err(|err| format!("failed to create {}: {err}", handoff_dir.display()))?;
    if publish_checksum
        && jetstreamer_node::archive_checksum::archive_checksum_is_publication_sentinel(
            final_output,
        )
        .map_err(|error| {
            format!("failed to inspect multi-runtime epoch {epoch} publication marker: {error}")
        })?
    {
        return Err(format!(
            "multi-runtime epoch {epoch} has an interrupted publication sentinel at {}; manual transaction recovery is required",
            jetstreamer_node::archive_checksum::archive_checksum_path(final_output)
                .map_err(|error| format!("failed to resolve checksum path: {error}"))?
                .display()
        ));
    }
    match epoch_archive_reusable_multi_runtime(final_output, epoch, &spans) {
        Ok(true) => {
            if publish_checksum {
                jetstreamer_node::archive_checksum::ensure_archive_checksum(final_output)
                    .map_err(|err| {
                        format!(
                            "failed to validate or repair checksum for reusable epoch {epoch} archive {}: {err}",
                            final_output.display()
                        )
                    })?;
            }
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
                handoff_dir.join(handoff.snapshot.archive_name())
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
                    .map(|handoff| (handoff, handoff_dir.join(handoff.snapshot.archive_name())))
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
                let mut attempt_scratch = None;
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
                    .env(
                        "JETSTREAMER_PARENT_EPOCH_LEASE_PID",
                        std::process::id().to_string(),
                    )
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
                if let Some(replay_scratch) = replay_scratch {
                    let segment_scratch = replay_scratch.join(format!(
                        "segment-{index}-attempt-{attempt}-{}",
                        SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_nanos()
                    ));
                    create_fresh_private_directory(&segment_scratch)?;
                    let mut scratch_arg = OsString::from("--replay-scratch=");
                    scratch_arg.push(&segment_scratch);
                    command.arg(scratch_arg);
                    attempt_scratch = Some(segment_scratch);
                }
                if needs_export_now {
                    command.env("JETSTREAMER_EXPORT_HANDOFF_SNAPSHOT_DIR", &handoff_dir);
                }
                command.kill_on_drop(true);
                let mut child = command.spawn().map_err(|err| {
                    format!(
                        "failed to spawn runtime segment child {}..{}: {err}",
                        span.slots.start, span.slots.end
                    )
                })?;
                let status = child.wait().await.map_err(|err| {
                    format!(
                        "failed to wait for runtime segment child {}..{}: {err}",
                        span.slots.start, span.slots.end
                    )
                })?;
                if let Some(attempt_scratch) = &attempt_scratch {
                    remove_path_if_exists(attempt_scratch)?;
                }
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
                &handoff_dir.join(handoff.snapshot.archive_name()),
                handoff,
            )
        })
        .collect::<Result<Vec<_>, String>>()?;
    let provenance = build_multi_runtime_provenance(epoch, &spans, &segments, &handoff_manifests)?;
    let output_parent = final_output.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(output_parent)
        .map_err(|err| format!("failed to create {}: {err}", output_parent.display()))?;
    let assembly_directory = tempfile::Builder::new()
        .prefix(".jetstreamer-runtime-assembly-")
        .tempdir_in(output_parent)
        .map_err(|err| format!("failed to create private assembly directory: {err}"))?;
    create_or_validate_private_directory(assembly_directory.path())?;
    if publish_checksum {
        jetstreamer_node::archive_publish::preflight_archive_publication(
            assembly_directory.path(),
            output_parent,
        )
        .map_err(|error| {
            format!("multi-runtime archive publication capability preflight failed: {error}")
        })?;
    }
    let output_name = final_output.file_name().ok_or_else(|| {
        format!(
            "multi-runtime output path has no filename: {}",
            final_output.display()
        )
    })?;
    let staged_assembly = assembly_directory.path().join(output_name);
    let sink = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(&staged_assembly)
        .map_err(|err| {
            format!(
                "failed to create staged assembly {}: {err}",
                staged_assembly.display()
            )
        })?;
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
    if publish_checksum {
        jetstreamer_node::archive_checksum::prepare_archive_permissions(&sink, output_parent)
            .map_err(|err| {
                format!("failed to prepare assembled epoch {epoch} archive permissions: {err}")
            })?;
    }
    sink.sync_all()
        .map_err(|err| format!("failed to sync assembled archive: {err}"))?;
    drop(sink);
    let retained_assembly_directory = assembly_directory.keep();
    let evidence = verify_assembled_runtime_archive(&staged_assembly, epoch, &provenance).map_err(
        |error| {
            format!(
                "{error}; retaining assembled archive for inspection in {}",
                retained_assembly_directory.display()
            )
        },
    )?;
    if publish_checksum {
        let publication =
            publish_verified_runtime_assembly(epoch, &staged_assembly, final_output, evidence)?;
        if let Some(recovery) = publication.recovery_directory {
            warn!(
                "epoch {epoch}: retained replaced multi-runtime archive artifacts in {}",
                recovery.display()
            );
        }
    } else {
        let preserved_output = preserve_existing_output(final_output)?;
        if let Some(backup) = preserved_output.as_ref() {
            warn!(
                "preserved previous private output {} as {}",
                final_output.display(),
                backup.display()
            );
        }
        if let Err(error) = fs::rename(&staged_assembly, final_output) {
            if let Some(backup) = preserved_output.as_ref()
                && !final_output.exists()
                && let Err(restore_error) = fs::rename(backup, final_output)
            {
                return Err(format!(
                    "failed to publish private assembled archive {}: {error}; also failed to restore preserved output {}: {restore_error}",
                    final_output.display(),
                    backup.display()
                ));
            }
            return Err(format!(
                "failed to publish private assembled archive {}: {error}; retained assembly directory {}",
                final_output.display(),
                retained_assembly_directory.display()
            ));
        }
        fs::File::open(final_output)
            .and_then(|file| file.sync_all())
            .map_err(|err| format!("failed to sync {}: {err}", final_output.display()))?;
        fs::File::open(output_parent)
            .and_then(|directory| directory.sync_all())
            .map_err(|err| format!("failed to sync {}: {err}", output_parent.display()))?;
    }
    if !epoch_archive_reusable_multi_runtime(final_output, epoch, &spans)? {
        return Err(format!(
            "published archive {} did not pass final registry validation",
            final_output.display()
        ));
    }
    if let Err(error) = fs::remove_dir(&retained_assembly_directory) {
        warn!(
            "epoch {epoch}: published assembled archive, but failed to remove empty staging directory {}: {error}",
            retained_assembly_directory.display()
        );
    } else if let Err(error) =
        fs::File::open(output_parent).and_then(|directory| directory.sync_all())
    {
        warn!(
            "epoch {epoch}: removed assembly staging directory, but failed to sync {}: {error}",
            output_parent.display()
        );
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

const ADAPTIVE_EPOCH_HARD_MAX_CONCURRENCY: usize = 3;
const DEFAULT_ADAPTIVE_EPOCH_SETTLE_SECS: u64 = 60;
const ADAPTIVE_EPOCH_POLL_INTERVAL: Duration = Duration::from_secs(1);
const ADAPTIVE_PROCESS_GROUP_GRACE: Duration = Duration::from_secs(15);
const ADAPTIVE_PROCESS_GROUP_KILL_GRACE: Duration = Duration::from_secs(5);
const PRIVATE_RUN_ROOT_ENV: &str = "JETSTREAMER_PRIVATE_RUN_ROOT";

struct EpochLease {
    _file: fs::File,
}

struct BoundDestination {
    path: PathBuf,
    directory: fs::File,
    dev: u64,
    ino: u64,
}

impl BoundDestination {
    fn bind(path: &Path) -> Result<Self, String> {
        use std::os::unix::fs::{MetadataExt as _, OpenOptionsExt as _};

        let path = path.canonicalize().map_err(|error| {
            format!(
                "failed to canonicalize epoch destination {}: {error}",
                path.display()
            )
        })?;
        let directory = fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_CLOEXEC | libc::O_DIRECTORY | libc::O_NOFOLLOW)
            .open(&path)
            .map_err(|error| {
                format!(
                    "failed to bind epoch destination {}: {error}",
                    path.display()
                )
            })?;
        let metadata = directory.metadata().map_err(|error| {
            format!(
                "failed to inspect epoch destination {}: {error}",
                path.display()
            )
        })?;
        if !metadata.file_type().is_dir() {
            return Err(format!(
                "epoch destination is not a directory: {}",
                path.display()
            ));
        }
        Ok(Self {
            path,
            directory,
            dev: metadata.dev(),
            ino: metadata.ino(),
        })
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn publication_binding(&self) -> cohort_publication::DestinationBinding<'_> {
        cohort_publication::DestinationBinding {
            path: &self.path,
            device: self.dev,
            inode: self.ino,
        }
    }

    fn revalidate(&self) -> Result<(), String> {
        use std::os::unix::fs::MetadataExt as _;

        let descriptor = self.directory.metadata().map_err(|error| {
            format!(
                "failed to recheck bound epoch destination {}: {error}",
                self.path.display()
            )
        })?;
        let path = fs::symlink_metadata(&self.path).map_err(|error| {
            format!(
                "failed to recheck epoch destination path {}: {error}",
                self.path.display()
            )
        })?;
        if !path.file_type().is_dir()
            || descriptor.dev() != self.dev
            || descriptor.ino() != self.ino
            || path.dev() != self.dev
            || path.ino() != self.ino
        {
            return Err(format!(
                "epoch destination changed after it was bound: {}",
                self.path.display()
            ));
        }
        Ok(())
    }
}

const COHORT_RUN_STATE_FILE: &str = "cohort-state.json";

struct CohortRunDirectory {
    path: PathBuf,
    parent: PathBuf,
}

impl CohortRunDirectory {
    fn create(
        parent: PathBuf,
        start_epoch: u64,
        end_epoch: u64,
        manifest_fingerprint: &str,
    ) -> Result<Self, String> {
        use std::{io::Write as _, os::unix::fs::OpenOptionsExt as _};

        create_or_validate_private_directory(&parent)?;
        let path = parent.join(format!(
            "run-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ));
        create_fresh_private_directory(&path)?;
        let state_path = path.join(COHORT_RUN_STATE_FILE);
        let state = serde_json::json!({
            "end_epoch": end_epoch,
            "manifest_fingerprint": manifest_fingerprint,
            "schema": "jetstreamer-root-cohort-run-v1",
            "start_epoch": start_epoch,
            "status": "running-private"
        });
        let bytes = serde_json::to_vec_pretty(&state)
            .map_err(|error| format!("failed to encode cohort run state: {error}"))?;
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW)
            .open(&state_path)
            .map_err(|error| {
                format!(
                    "failed to create cohort run state {}: {error}",
                    state_path.display()
                )
            })?;
        file.write_all(&bytes)
            .and_then(|()| file.sync_all())
            .map_err(|error| {
                format!(
                    "failed to sync cohort run state {}: {error}",
                    state_path.display()
                )
            })?;
        fs::File::open(&path)
            .and_then(|directory| directory.sync_all())
            .map_err(|error| format!("failed to sync cohort run directory: {error}"))?;
        Ok(Self { path, parent })
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn archives_path(&self) -> PathBuf {
        self.path.join("archives")
    }

    fn preflight_publication(&self, destination: &BoundDestination) -> Result<(), String> {
        destination.revalidate()?;
        let archives = self.archives_path();
        create_or_validate_private_directory(&archives)?;
        jetstreamer_node::archive_publish::preflight_archive_publication(
            &archives,
            destination.path(),
        )
        .map_err(|error| {
            format!("root-checkpoint cohort publication capability preflight failed: {error}")
        })
    }

    fn cleanup_after_commit(self) -> Result<(), String> {
        create_or_validate_private_directory(&self.parent)?;
        create_or_validate_private_directory(&self.path)?;
        fs::remove_dir_all(&self.path).map_err(|error| {
            format!(
                "failed to remove committed cohort run {}: {error}",
                self.path.display()
            )
        })?;
        fs::File::open(&self.parent)
            .and_then(|directory| directory.sync_all())
            .map_err(|error| {
                format!(
                    "failed to sync cohort run parent {}: {error}",
                    self.parent.display()
                )
            })
    }
}

fn effective_user_id() -> u32 {
    // SAFETY: geteuid has no preconditions and does not dereference memory.
    unsafe { libc::geteuid() }
}

fn create_or_validate_private_directory(path: &Path) -> Result<(), String> {
    use std::os::unix::fs::{DirBuilderExt as _, MetadataExt as _, PermissionsExt as _};

    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            if !metadata.file_type().is_dir()
                || metadata.uid() != effective_user_id()
                || metadata.permissions().mode() & 0o077 != 0
            {
                return Err(format!(
                    "private run path must be an owner-only real directory: {}",
                    path.display()
                ));
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let mut builder = fs::DirBuilder::new();
            builder.mode(0o700);
            match builder.create(path) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                    return create_or_validate_private_directory(path);
                }
                Err(error) => {
                    return Err(format!(
                        "failed to create private run directory {}: {error}",
                        path.display()
                    ));
                }
            }
        }
        Err(error) => {
            return Err(format!(
                "failed to inspect private run directory {}: {error}",
                path.display()
            ));
        }
    }
    Ok(())
}

fn create_fresh_private_directory(path: &Path) -> Result<(), String> {
    use std::os::unix::fs::DirBuilderExt as _;

    let mut builder = fs::DirBuilder::new();
    builder.mode(0o700);
    builder.create(path).map_err(|error| {
        format!(
            "failed to create fresh private directory {}: {error}",
            path.display()
        )
    })
}

/// Resolves an owner-only control/work tree on the output filesystem. Keeping
/// it outside the group-writable destination prevents another Horizon writer
/// from renaming a supposedly private scratch directory out from under us.
fn private_epoch_scope(dest_dir: &Path) -> Result<PathBuf, String> {
    use std::os::unix::{ffi::OsStrExt as _, fs::MetadataExt as _};

    let destination = dest_dir.canonicalize().map_err(|error| {
        format!(
            "failed to canonicalize epoch destination {}: {error}",
            dest_dir.display()
        )
    })?;
    let destination_metadata = fs::metadata(&destination).map_err(|error| {
        format!(
            "failed to inspect epoch destination {}: {error}",
            destination.display()
        )
    })?;
    if !destination_metadata.file_type().is_dir() {
        return Err(format!(
            "epoch destination is not a directory: {}",
            destination.display()
        ));
    }

    let root = match env::var_os(PRIVATE_RUN_ROOT_ENV) {
        Some(path) if !path.is_empty() => PathBuf::from(path),
        Some(_) => return Err(format!("{PRIVATE_RUN_ROOT_ENV} must not be empty")),
        None => destination
            .parent()
            .ok_or_else(|| {
                format!(
                    "epoch destination has no parent for private state: {}",
                    destination.display()
                )
            })?
            .join(".jetstreamer-private"),
    };
    if !root.is_absolute() {
        return Err(format!(
            "{PRIVATE_RUN_ROOT_ENV} must resolve to an absolute path: {}",
            root.display()
        ));
    }
    let parent = root.parent().ok_or_else(|| {
        format!(
            "private run root has no parent directory: {}",
            root.display()
        )
    })?;
    let parent_metadata = fs::metadata(parent).map_err(|error| {
        format!(
            "failed to inspect private run parent {}: {error}",
            parent.display()
        )
    })?;
    if !parent_metadata.file_type().is_dir()
        || parent_metadata.uid() != effective_user_id()
        || parent_metadata.mode() & 0o022 != 0
    {
        return Err(format!(
            "private run root parent must be owned by this user and not group/world writable: {}",
            parent.display()
        ));
    }
    create_or_validate_private_directory(&root)?;
    let root = root.canonicalize().map_err(|error| {
        format!(
            "failed to canonicalize private run root {}: {error}",
            root.display()
        )
    })?;
    let root_metadata = fs::metadata(&root)
        .map_err(|error| format!("failed to inspect {}: {error}", root.display()))?;
    if root_metadata.dev() != destination_metadata.dev() {
        return Err(format!(
            "private run root {} and epoch destination {} are on different filesystems",
            root.display(),
            destination.display()
        ));
    }

    let digest: [u8; 32] = Sha256::digest(destination.as_os_str().as_bytes()).into();
    let scope_name = format!(
        "destination-{}",
        jetstreamer_node::segment_manifest::sha256_hex_string(&digest)
    );
    let scope = root.join(scope_name);
    create_or_validate_private_directory(&scope)?;
    for child in ["evidence", "locks", "work"] {
        create_or_validate_private_directory(&scope.join(child))?;
    }
    Ok(scope)
}

fn cohort_batch_receipt_directory(destination: &BoundDestination) -> Result<PathBuf, String> {
    destination.revalidate()?;
    let evidence = private_epoch_scope(destination.path())?.join("evidence");
    let batches = evidence.join("archive-batches");
    create_or_validate_private_directory(&batches)?;
    let identity = batches.join(format!(
        "destination-{:016x}-{:016x}",
        destination.dev, destination.ino
    ));
    create_or_validate_private_directory(&identity)?;
    Ok(identity)
}

/// Reuses only the immutable, owner-only checkpoint file produced by a prior
/// adaptive prefetch. This lets a detached replay restart after credentials
/// expire without silently weakening verification or contacting GCS again.
fn cached_private_epoch_hashes(
    dest_dir: &Path,
    epoch: u64,
    bootstrap: compatibility::BootstrapState,
) -> Result<Option<BTreeMap<Slot, BankHashExpectation>>, String> {
    use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};

    let work_dir = private_epoch_scope(dest_dir)?
        .join("work")
        .join(format!("epoch-{epoch}"));
    let input_dir = work_dir.join("inputs");
    for directory in [&work_dir, &input_dir] {
        match fs::symlink_metadata(directory) {
            Ok(_) => create_or_validate_private_directory(directory)?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => {
                return Err(format!(
                    "failed to inspect cached epoch {epoch} input directory {}: {error}",
                    directory.display()
                ));
            }
        }
    }
    let path = input_dir.join("epoch-hashes.txt");
    let metadata = match fs::symlink_metadata(&path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(format!(
                "failed to inspect cached epoch {epoch} hashes {}: {error}",
                path.display()
            ));
        }
    };
    if !metadata.file_type().is_file()
        || metadata.uid() != effective_user_id()
        || metadata.permissions().mode() & 0o077 != 0
    {
        return Err(format!(
            "cached epoch {epoch} hashes must be an owner-only regular file: {}",
            path.display()
        ));
    }
    read_epoch_hashes_file(&path, bootstrap).map(Some)
}

fn acquire_epoch_leases(
    dest_dir: &Path,
    start_epoch: u64,
    end_epoch: u64,
) -> Result<Vec<EpochLease>, String> {
    use std::os::{fd::AsRawFd as _, unix::fs::OpenOptionsExt as _};

    let lock_dir = private_epoch_scope(dest_dir)?.join("locks");
    let mut leases = Vec::new();
    for epoch in start_epoch..=end_epoch {
        let path = lock_dir.join(format!("epoch-{epoch}.lock"));
        let file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .mode(0o600)
            .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(&path)
            .map_err(|error| {
                format!(
                    "failed to open epoch {epoch} lease {}: {error}",
                    path.display()
                )
            })?;
        if !file
            .metadata()
            .map_err(|error| format!("failed to inspect epoch lease {}: {error}", path.display()))?
            .file_type()
            .is_file()
        {
            return Err(format!(
                "epoch lease is not a regular file: {}",
                path.display()
            ));
        }
        // SAFETY: the descriptor is live and LOCK_NB makes coordination fail
        // closed instead of hanging behind another producer.
        if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
            let error = std::io::Error::last_os_error();
            return Err(format!(
                "epoch {epoch} is already owned by another producer ({}): {error}",
                path.display()
            ));
        }
        leases.push(EpochLease { _file: file });
    }
    Ok(leases)
}

fn inherited_epoch_lease_authorized() -> bool {
    let Ok(parent) = env::var("JETSTREAMER_PARENT_EPOCH_LEASE_PID") else {
        return false;
    };
    let Ok(parent) = parent.parse::<u32>() else {
        return false;
    };
    // SAFETY: getppid has no preconditions and does not dereference memory.
    parent == unsafe { libc::getppid() } as u32
}

struct AdaptiveEpochJob {
    epoch: u64,
    spans: Vec<compatibility::RuntimeSpan>,
    selection: compatibility::RuntimeSelection,
    source_bootstrap: ReplayBootstrap,
    bound_bootstrap: Option<ReplayBootstrap>,
    source_snapshot: Option<PathBuf>,
    hashes_path: PathBuf,
    final_output: PathBuf,
    staged_output: PathBuf,
    scratch_dir: PathBuf,
    work_dir: PathBuf,
    attempt: u32,
}

struct RunningAdaptiveEpoch {
    job: AdaptiveEpochJob,
    child: Child,
    process_group: i32,
}

struct ValidatedAdaptiveEpoch {
    job: AdaptiveEpochJob,
    evidence: jetstreamer_node::archive_checksum::ValidatedArchiveFile,
}

struct AdaptiveValidationResult {
    job: AdaptiveEpochJob,
    result:
        Result<jetstreamer_node::archive_checksum::ValidatedArchiveFile, AdaptiveValidationError>,
}

#[derive(Debug)]
enum AdaptiveValidationError {
    InvalidArtifact(String),
    RetainStaging(String),
}

impl std::fmt::Display for AdaptiveValidationError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidArtifact(message) | Self::RetainStaging(message) => {
                formatter.write_str(message)
            }
        }
    }
}

enum AdaptiveValidationFailureAction {
    Retry(String),
    Terminal(String),
}

enum AdaptiveChildFailureAction {
    Validate(String),
    Retry(String),
    Terminal(String),
}

/// A terminal epoch failure closes admission without turning it into a global
/// shutdown. Existing children and validation tasks retain the live shutdown
/// token so they can finish, prove their archives, and publish them before the
/// supervisor reports the range failure.
#[derive(Default)]
struct AdaptiveFailureDrain {
    failures: Vec<String>,
}

impl AdaptiveFailureDrain {
    fn is_active(&self) -> bool {
        !self.failures.is_empty()
    }

    fn may_retry(&self, attempt: u32, attempts_per_epoch: u32) -> bool {
        !self.is_active() && attempt < attempts_per_epoch
    }

    fn is_drained(
        &self,
        running_epochs: usize,
        validating_epochs: usize,
        ready_epochs: usize,
    ) -> bool {
        self.is_active() && running_epochs == 0 && validating_epochs == 0 && ready_epochs == 0
    }

    /// Returns true when this is the failure that closed admission.
    fn record(&mut self, failure: String) -> bool {
        let first = self.failures.is_empty();
        self.failures.push(failure);
        first
    }

    fn into_error(self) -> String {
        self.failures.join("; ")
    }
}

fn record_adaptive_terminal_failure(
    drain: &mut AdaptiveFailureDrain,
    failure: String,
    running_epochs: usize,
    validating_epochs: usize,
) {
    if drain.record(failure.clone()) {
        warn!(
            "{failure}; closing adaptive admission and draining {running_epochs} already-running child(ren) plus {validating_epochs} validation task(s)"
        );
    } else {
        warn!("additional adaptive epoch failure while draining: {failure}");
    }
}

/// In normal operation publication remains contiguous. Once a terminal
/// failure creates an unavoidable gap, independently validated siblings are
/// still safe to publish and must not be discarded merely because their epoch
/// number follows the failed child.
fn next_adaptive_publication_epoch<T>(
    next_publish_epoch: u64,
    ready: &BTreeMap<u64, T>,
    draining_failure: bool,
) -> Option<u64> {
    if ready.contains_key(&next_publish_epoch) {
        Some(next_publish_epoch)
    } else if draining_failure {
        ready.first_key_value().map(|(epoch, _)| *epoch)
    } else {
        None
    }
}

/// Resource-bearing epoch work. A validated archive waiting for ordered
/// publication retains validation evidence and staged disk bytes, but no
/// worker or validation reservation. Its actual disk use is already reflected
/// by `statvfs`, so counting it here can deadlock a serial supervisor when a
/// later epoch finishes before `next_publish_epoch`.
fn adaptive_resource_occupancy(
    running_epochs: usize,
    validating_epochs: usize,
    _ready_epochs: usize,
) -> usize {
    running_epochs.saturating_add(validating_epochs)
}

/// Merge retries with untouched work in publication order. Prioritizing every
/// recovered retry ahead of lower untouched epochs can waste hours rebuilding
/// an archive that cannot yet be published.
fn merge_adaptive_retries(
    pending: &mut VecDeque<AdaptiveEpochJob>,
    retries: Vec<AdaptiveEpochJob>,
) {
    pending.extend(retries);
    pending
        .make_contiguous()
        .sort_unstable_by_key(|job| job.epoch);
}

fn adaptive_env_u64(name: &str) -> Result<Option<u64>, String> {
    let Some(value) = env::var_os(name) else {
        return Ok(None);
    };
    let value = value
        .into_string()
        .map_err(|_| format!("{name} must contain a decimal integer"))?;
    value
        .trim()
        .parse::<u64>()
        .map(Some)
        .map_err(|err| format!("invalid {name} value {value:?}: {err}"))
}

fn write_private_epoch_hashes(
    input_dir: &Path,
    expected: &BTreeMap<Slot, BankHashExpectation>,
    bootstrap: compatibility::BootstrapState,
) -> Result<PathBuf, String> {
    use std::{
        io::Write as _,
        os::unix::fs::{MetadataExt as _, PermissionsExt as _},
    };

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
    let path = input_dir.join("epoch-hashes.txt");
    match fs::symlink_metadata(&path) {
        Ok(metadata) => {
            if !metadata.file_type().is_file()
                || metadata.uid() != effective_user_id()
                || metadata.permissions().mode() & 0o077 != 0
            {
                return Err(format!(
                    "existing private epoch hashes must be an owner-only regular file: {}",
                    path.display()
                ));
            }
            let cached = read_epoch_hashes_file(&path, bootstrap)?;
            if cached != *expected {
                return Err(format!(
                    "existing private epoch hashes differ from the prefetched checkpoint set: {}",
                    path.display()
                ));
            }
            return Ok(path);
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(format!(
                "failed to inspect private epoch hashes {}: {error}",
                path.display()
            ));
        }
    }
    let mut temporary = tempfile::Builder::new()
        .prefix(".epoch-hashes-")
        .suffix(".partial")
        .tempfile_in(input_dir)
        .map_err(|error| {
            format!(
                "failed to create private epoch-hash file in {}: {error}",
                input_dir.display()
            )
        })?;
    temporary
        .write_all(contents.as_bytes())
        .and_then(|()| temporary.flush())
        .and_then(|()| {
            temporary
                .as_file()
                .set_permissions(fs::Permissions::from_mode(0o400))
        })
        .and_then(|()| temporary.as_file().sync_all())
        .map_err(|error| format!("failed to sync private epoch hashes: {error}"))?;
    temporary
        .persist(&path)
        .map_err(|error| format!("failed to publish private epoch hashes: {}", error.error))?;
    fs::File::open(input_dir)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| format!("failed to sync {}: {error}", input_dir.display()))?;
    Ok(path)
}

fn bind_snapshot_for_adaptive_job(
    bootstrap: &ReplayBootstrap,
    input_dir: &Path,
) -> Result<ReplayBootstrap, String> {
    use std::{
        io::Write as _,
        os::unix::fs::{MetadataExt as _, PermissionsExt as _},
    };

    let ReplayBootstrap::SnapshotArchive(source_path) = bootstrap else {
        return Ok(bootstrap.clone());
    };
    let file_name = source_path.file_name().ok_or_else(|| {
        format!(
            "snapshot path has no filename for private binding: {}",
            source_path.display()
        )
    })?;
    let destination = input_dir.join(file_name);
    let mut source = jetstreamer_node::archive_checksum::open_regular_nofollow(source_path)
        .map_err(|error| {
            format!(
                "failed to bind boundary snapshot {}: {error}",
                source_path.display()
            )
        })?;
    let source_identity = jetstreamer_node::archive_checksum::archive_file_identity(&source)
        .map_err(|error| format!("failed to identify {}: {error}", source_path.display()))?;
    match fs::symlink_metadata(&destination) {
        Ok(metadata) => {
            if !metadata.file_type().is_file()
                || metadata.uid() != effective_user_id()
                || metadata.permissions().mode() & 0o077 != 0
            {
                return Err(format!(
                    "existing private boundary snapshot must be an owner-only regular file: {}",
                    destination.display()
                ));
            }
            let bound = jetstreamer_node::archive_checksum::open_regular_nofollow(&destination)
                .map_err(|error| {
                    format!(
                        "failed to open existing private boundary snapshot {}: {error}",
                        destination.display()
                    )
                })?;
            let source_measurement = jetstreamer_node::archive_checksum::measure_open_archive(
                &source,
            )
            .map_err(|error| {
                format!(
                    "failed to measure boundary snapshot {}: {error}",
                    source_path.display()
                )
            })?;
            let bound_measurement = jetstreamer_node::archive_checksum::measure_open_archive(
                &bound,
            )
            .map_err(|error| {
                format!(
                    "failed to measure existing private boundary snapshot {}: {error}",
                    destination.display()
                )
            })?;
            if source_measurement.sha256 != bound_measurement.sha256 {
                return Err(format!(
                    "existing private boundary snapshot does not match its source: {}",
                    destination.display()
                ));
            }
            return Ok(ReplayBootstrap::SnapshotArchive(destination));
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(format!(
                "failed to inspect private boundary snapshot {}: {error}",
                destination.display()
            ));
        }
    }
    let mut temporary = tempfile::Builder::new()
        .prefix(".boundary-snapshot-")
        .suffix(".partial")
        .tempfile_in(input_dir)
        .map_err(|error| {
            format!(
                "failed to create private boundary snapshot in {}: {error}",
                input_dir.display()
            )
        })?;
    std::io::copy(&mut source, &mut temporary).map_err(|error| {
        format!(
            "failed to copy boundary snapshot {} into private staging: {error}",
            source_path.display()
        )
    })?;
    temporary
        .flush()
        .and_then(|()| {
            temporary
                .as_file()
                .set_permissions(fs::Permissions::from_mode(0o400))
        })
        .and_then(|()| temporary.as_file().sync_all())
        .map_err(|error| format!("failed to sync private boundary snapshot: {error}"))?;
    if jetstreamer_node::archive_checksum::archive_file_identity(&source)
        .map_err(|error| format!("failed to recheck {}: {error}", source_path.display()))?
        != source_identity
    {
        return Err(format!(
            "boundary snapshot changed while it was copied: {}",
            source_path.display()
        ));
    }
    temporary.persist(&destination).map_err(|error| {
        format!(
            "failed to publish private boundary snapshot {}: {}",
            destination.display(),
            error.error
        )
    })?;
    fs::File::open(input_dir)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| format!("failed to sync {}: {error}", input_dir.display()))?;
    Ok(ReplayBootstrap::SnapshotArchive(destination))
}

fn prepare_adaptive_attempt(job: &mut AdaptiveEpochJob) -> Result<(), String> {
    // Boundary snapshots can be large. Bind only work that has passed memory,
    // CPU, and disk admission instead of eagerly duplicating every prefetched
    // range input. The Option keeps child spawning fail-closed if this step is
    // ever accidentally skipped.
    let input_dir = job.work_dir.join("inputs");
    create_or_validate_private_directory(&input_dir)?;
    job.bound_bootstrap = Some(bind_snapshot_for_adaptive_job(
        &job.source_bootstrap,
        &input_dir,
    )?);
    job.attempt = job
        .attempt
        .checked_add(1)
        .ok_or_else(|| format!("epoch {} attempt counter overflow", job.epoch))?;
    let attempt_dir = job.work_dir.join(format!(
        "attempt-{}-{}-{}",
        std::process::id(),
        job.attempt,
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    ));
    create_fresh_private_directory(&attempt_dir)?;
    job.scratch_dir = attempt_dir.join("scratch");
    create_or_validate_private_directory(&job.scratch_dir)?;
    job.staged_output = attempt_dir.join(format!("epoch-{}.jet", job.epoch));
    Ok(())
}

fn cleanup_adaptive_attempt(job: &AdaptiveEpochJob) -> Result<(), String> {
    let Some(attempt_dir) = job.staged_output.parent() else {
        return Ok(());
    };
    if attempt_dir.parent() != Some(job.work_dir.as_path())
        || !attempt_dir
            .file_name()
            .is_some_and(|name| name.to_string_lossy().starts_with("attempt-"))
    {
        return Err(format!(
            "refusing to clean unbound adaptive attempt path {}",
            attempt_dir.display()
        ));
    }
    remove_path_if_exists(attempt_dir)
}

fn staged_archive_candidate(work_dir: &Path, epoch: u64) -> Result<Option<PathBuf>, String> {
    let mut candidates = Vec::new();
    for entry in fs::read_dir(work_dir)
        .map_err(|error| format!("failed to scan {}: {error}", work_dir.display()))?
    {
        let entry = entry
            .map_err(|error| format!("failed to scan entry in {}: {error}", work_dir.display()))?;
        let file_type = entry
            .file_type()
            .map_err(|error| format!("failed to inspect {}: {error}", entry.path().display()))?;
        if !file_type.is_dir() || !entry.file_name().to_string_lossy().starts_with("attempt-") {
            continue;
        }
        let candidate = entry.path().join(format!("epoch-{epoch}.jet"));
        match fs::symlink_metadata(&candidate) {
            Ok(metadata) if metadata.file_type().is_file() => {
                candidates.push((metadata.modified().ok(), candidate));
            }
            Ok(_) => {
                return Err(format!(
                    "staged archive candidate is not a regular file: {}",
                    candidate.display()
                ));
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(format!(
                    "failed to inspect staged archive {}: {error}",
                    candidate.display()
                ));
            }
        }
    }
    candidates.sort_by_key(|(modified, path)| (*modified, path.clone()));
    Ok(candidates.pop().map(|(_, path)| path))
}

fn require_existing_private_directory(path: &Path) -> Result<(), String> {
    use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};

    let metadata = fs::symlink_metadata(path).map_err(|error| {
        format!(
            "failed to inspect private directory {}: {error}",
            path.display()
        )
    })?;
    if !metadata.file_type().is_dir()
        || metadata.uid() != effective_user_id()
        || metadata.permissions().mode() & 0o077 != 0
    {
        return Err(format!(
            "staged-only recovery requires an existing owner-only real directory: {}",
            path.display()
        ));
    }
    Ok(())
}

/// Unlike normal adaptive crash recovery, the explicit operator recovery route
/// never guesses between attempts. Its namespace must contain exactly one
/// direct, immutable-enough candidate and nothing else.
#[derive(Debug)]
struct StrictStagedArchiveCandidate {
    path: PathBuf,
    file: fs::File,
    identity: jetstreamer_node::archive_checksum::ArchiveFileIdentity,
}

fn strict_staged_archive_candidate(
    work_dir: &Path,
    epoch: u64,
) -> Result<StrictStagedArchiveCandidate, String> {
    use std::os::unix::{
        ffi::OsStrExt as _,
        fs::{MetadataExt as _, PermissionsExt as _},
    };

    require_existing_private_directory(work_dir)?;
    let expected_name = OsString::from(format!("epoch-{epoch}.jet"));
    let mut candidate = None;
    for entry in fs::read_dir(work_dir)
        .map_err(|error| format!("failed to scan {}: {error}", work_dir.display()))?
    {
        let entry =
            entry.map_err(|error| format!("failed to scan {}: {error}", work_dir.display()))?;
        let attempt_name = entry.file_name();
        if attempt_name == "inputs" {
            require_existing_private_directory(&entry.path())?;
            continue;
        }
        if !attempt_name.as_bytes().starts_with(b"attempt-") {
            return Err(format!(
                "staged-only recovery found an unexpected entry in {}: {:?}",
                work_dir.display(),
                attempt_name
            ));
        }
        let attempt_dir = entry.path();
        require_existing_private_directory(&attempt_dir)?;
        let mut attempt_candidate = None;
        for member in fs::read_dir(&attempt_dir).map_err(|error| {
            format!(
                "failed to scan staged attempt {}: {error}",
                attempt_dir.display()
            )
        })? {
            let member = member.map_err(|error| {
                format!(
                    "failed to scan staged attempt {}: {error}",
                    attempt_dir.display()
                )
            })?;
            if member.file_name() == "scratch" {
                require_existing_private_directory(&member.path())?;
                continue;
            }
            if member.file_name() != expected_name {
                return Err(format!(
                    "staged-only recovery found an unexpected entry in {}: {:?}",
                    attempt_dir.display(),
                    member.file_name()
                ));
            }
            let path = member.path();
            let file = jetstreamer_node::archive_checksum::open_regular_nofollow(&path).map_err(
                |error| format!("failed to bind staged archive {}: {error}", path.display()),
            )?;
            let metadata = file.metadata().map_err(|error| {
                format!(
                    "failed to inspect staged archive {}: {error}",
                    path.display()
                )
            })?;
            let mode = metadata.permissions().mode();
            if !metadata.file_type().is_file()
                || metadata.uid() != effective_user_id()
                || metadata.nlink() != 1
                || mode & 0o7111 != 0
            {
                return Err(format!(
                    "staged archive must be a single-link regular file owned by the caller with no executable or special permission bits: {}",
                    path.display()
                ));
            }
            let identity = jetstreamer_node::archive_checksum::archive_file_identity(&file)
                .map_err(|error| {
                    format!(
                        "failed to identify staged archive {}: {error}",
                        path.display()
                    )
                })?;
            if !jetstreamer_node::archive_checksum::path_matches_archive_identity(&path, identity)
                .map_err(|error| {
                format!(
                    "failed to recheck staged archive {}: {error}",
                    path.display()
                )
            })? {
                return Err(format!(
                    "staged archive changed while it was admitted: {}",
                    path.display()
                ));
            }
            if attempt_candidate
                .replace(StrictStagedArchiveCandidate {
                    path,
                    file,
                    identity,
                })
                .is_some()
            {
                return Err(format!(
                    "staged attempt contains more than one epoch-{epoch} archive: {}",
                    attempt_dir.display()
                ));
            }
        }
        let attempt_candidate = attempt_candidate.ok_or_else(|| {
            format!(
                "staged attempt contains no epoch-{epoch} archive: {}",
                attempt_dir.display()
            )
        })?;
        if candidate.replace(attempt_candidate).is_some() {
            return Err(format!(
                "staged-only recovery found multiple attempt directories in {}",
                work_dir.display()
            ));
        }
    }
    candidate.ok_or_else(|| {
        format!(
            "epoch {epoch}: no private staged archive candidate exists in {}; staged-only recovery will not start replay",
            work_dir.display()
        )
    })
}

fn require_recovery_destination_namespace_absent(archive: &Path) -> Result<(), String> {
    let manifest = jetstreamer_node::segment_manifest::segment_manifest_path(archive)
        .map_err(|error| format!("failed to resolve archive manifest path: {error}"))?;
    let checksum = jetstreamer_node::archive_checksum::archive_checksum_path(archive)
        .map_err(|error| format!("failed to resolve archive checksum path: {error}"))?;
    for (kind, path) in [
        ("archive", archive.to_path_buf()),
        ("segment manifest", manifest),
        ("checksum", checksum),
    ] {
        match fs::symlink_metadata(&path) {
            Ok(_) => {
                return Err(format!(
                    "staged-only recovery refuses to replace an existing destination {kind}: {}",
                    path.display()
                ));
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(format!(
                    "failed to inspect destination {kind} {}: {error}",
                    path.display()
                ));
            }
        }
    }
    Ok(())
}

async fn recover_staged_epoch_archive_only(
    epoch: u64,
    destination: &BoundDestination,
    allow_candidate_runtime: bool,
    shutdown: Arc<AtomicBool>,
) -> Result<(), String> {
    destination.revalidate()?;
    let final_output = destination.path().join(format!("epoch-{epoch}.jet"));
    require_recovery_destination_namespace_absent(&final_output)?;

    let (slot_start, slot_end_inclusive) = epoch_to_slot_range(epoch);
    let spans = compatibility::plan_runtime_spans(
        slot_start..slot_end_inclusive.saturating_add(1),
        allow_candidate_runtime,
    )?;
    let selection = runtime_span_selection(
        spans
            .first()
            .expect("runtime planner rejects an empty epoch range"),
    )?;
    let work_dir = private_epoch_scope(destination.path())?
        .join("work")
        .join(format!("epoch-{epoch}"));
    let candidate = strict_staged_archive_candidate(&work_dir, epoch)?;
    let staged_output = candidate.path.clone();
    let attempt_dir = staged_output
        .parent()
        .expect("strict candidate has an attempt parent")
        .to_path_buf();
    jetstreamer_node::archive_publish::preflight_archive_publication(
        &attempt_dir,
        destination.path(),
    )
    .map_err(|error| format!("archive publication capability preflight failed: {error}"))?;

    info!(
        "epoch {epoch}: staged-only recovery is fully validating {} without replay",
        staged_output.display()
    );
    let validation_path = staged_output.clone();
    let destination_parent = destination.path().to_path_buf();
    let validation_shutdown = shutdown.clone();
    let admitted_file = candidate.file;
    let admitted_identity = candidate.identity;
    let validated = tokio::task::spawn_blocking(move || {
        staged_epoch_archive_validated(
            epoch,
            &spans,
            selection,
            &validation_path,
            &destination_parent,
            &validation_shutdown,
            Some((admitted_file, admitted_identity)),
        )
    })
    .await
    .map_err(|error| {
        format!(
            "epoch {epoch}: staged archive validation task failed: {error}; preserving {}",
            staged_output.display()
        )
    })?
    .map_err(|error| {
        format!(
            "epoch {epoch}: staged archive did not pass full validation: {error}; preserving {} and refusing replay",
            staged_output.display()
        )
    })?;
    if shutdown.load(Ordering::SeqCst) {
        return Err(format!(
            "epoch {epoch}: shutdown requested after validation; preserving {} without publication",
            staged_output.display()
        ));
    }
    destination.revalidate()?;
    let publication =
        jetstreamer_node::archive_publish::publish_verified_archive_if_absent(
            &staged_output,
            &final_output,
            validated,
        )
        .map_err(|error| {
            format!(
                "epoch {epoch}: failed to transactionally publish staged archive (commit_state={:?}, recovery={:?}): {error}; refusing replay",
                error.commit_state(),
                error.recovery_directory(),
            )
        })?;
    if let Some(recovery) = publication.recovery_directory {
        warn!(
            "epoch {epoch}: retained publication recovery artifacts in {}",
            recovery.display()
        );
    }
    info!(
        "epoch {epoch}: staged-only recovery transactionally published verified archive {}; private control directories remain at {}",
        final_output.display(),
        work_dir.display(),
    );
    Ok(())
}

fn staged_epoch_archive_validated(
    epoch: u64,
    spans: &[compatibility::RuntimeSpan],
    selection: compatibility::RuntimeSelection,
    path: &Path,
    destination_parent: &Path,
    shutdown: &AtomicBool,
    admitted: Option<(
        fs::File,
        jetstreamer_node::archive_checksum::ArchiveFileIdentity,
    )>,
) -> Result<jetstreamer_node::archive_checksum::ValidatedArchiveFile, AdaptiveValidationError> {
    let file = match admitted {
        Some((file, admitted_identity)) => {
            let current = jetstreamer_node::archive_checksum::archive_file_identity(&file)
                .map_err(|error| {
                    AdaptiveValidationError::RetainStaging(format!(
                        "failed to reidentify admitted staged epoch {epoch} archive: {error}"
                    ))
                })?;
            if current != admitted_identity {
                return Err(AdaptiveValidationError::RetainStaging(format!(
                    "admitted staged epoch {epoch} archive changed before permission preparation"
                )));
            }
            file
        }
        None => {
            jetstreamer_node::archive_checksum::open_regular_nofollow(path).map_err(|error| {
                AdaptiveValidationError::RetainStaging(format!(
                    "failed to open staged epoch {epoch} archive: {error}"
                ))
            })?
        }
    };
    let opened_identity = jetstreamer_node::archive_checksum::archive_file_identity(&file)
        .map_err(|error| {
            AdaptiveValidationError::RetainStaging(format!(
                "failed to identify staged epoch {epoch} archive: {error}"
            ))
        })?;
    if !jetstreamer_node::archive_checksum::path_matches_archive_identity(path, opened_identity)
        .map_err(|error| {
            AdaptiveValidationError::RetainStaging(format!(
                "failed to bind staged epoch {epoch} archive path before permission preparation: {error}"
            ))
        })?
    {
        return Err(AdaptiveValidationError::RetainStaging(format!(
            "staged epoch {epoch} archive path changed before permission preparation"
        )));
    }
    jetstreamer_node::archive_checksum::prepare_archive_permissions(&file, destination_parent)
        .map_err(|error| {
            AdaptiveValidationError::RetainStaging(format!(
                "failed to prepare staged epoch {epoch} permissions: {error}"
            ))
        })?;
    file.sync_all().map_err(|error| {
        AdaptiveValidationError::RetainStaging(format!(
            "failed to sync staged epoch {epoch} archive: {error}"
        ))
    })?;
    let prepared_identity = jetstreamer_node::archive_checksum::archive_file_identity(&file)
        .map_err(|error| {
            AdaptiveValidationError::RetainStaging(format!(
                "failed to reidentify staged epoch {epoch} archive after permission preparation: {error}"
            ))
        })?;
    if !jetstreamer_node::archive_checksum::path_matches_archive_identity(path, prepared_identity)
        .map_err(|error| {
            AdaptiveValidationError::RetainStaging(format!(
                "failed to rebind staged epoch {epoch} archive path after permission preparation: {error}"
            ))
        })?
    {
        return Err(AdaptiveValidationError::RetainStaging(format!(
            "staged epoch {epoch} archive path changed during permission preparation"
        )));
    }
    let validated = if spans.len() == 1 {
        validated_epoch_archive(path, epoch, selection, Some(shutdown), None)
    } else {
        validated_epoch_archive_multi_runtime(path, epoch, spans, Some(shutdown))
    };
    let validated = classify_adaptive_deep_validation(validated, epoch, path)?;
    let current_identity = jetstreamer_node::archive_checksum::archive_file_identity(&file)
        .map_err(|error| {
            AdaptiveValidationError::RetainStaging(format!(
                "failed to reidentify staged epoch {epoch} archive after validation: {error}"
            ))
        })?;
    if validated.identity != prepared_identity
        || current_identity != prepared_identity
        || !jetstreamer_node::archive_checksum::path_matches_archive_identity(
            path,
            prepared_identity,
        )
        .map_err(|error| {
            AdaptiveValidationError::RetainStaging(format!(
                "failed to rebind staged epoch {epoch} archive path after validation: {error}"
            ))
        })?
    {
        return Err(AdaptiveValidationError::RetainStaging(format!(
            "staged epoch {epoch} archive changed during validation"
        )));
    }
    Ok(validated)
}

fn adaptive_epoch_archive_validated(
    job: &AdaptiveEpochJob,
    path: &Path,
    shutdown: &AtomicBool,
) -> Result<jetstreamer_node::archive_checksum::ValidatedArchiveFile, AdaptiveValidationError> {
    staged_epoch_archive_validated(
        job.epoch,
        &job.spans,
        job.selection,
        path,
        job.final_output.parent().unwrap_or_else(|| Path::new(".")),
        shutdown,
        None,
    )
}

fn classify_adaptive_deep_validation<T>(
    result: Result<Option<T>, String>,
    epoch: u64,
    path: &Path,
) -> Result<T, AdaptiveValidationError> {
    match result {
        Ok(Some(validated)) => Ok(validated),
        Ok(None) => Err(AdaptiveValidationError::InvalidArtifact(format!(
            "staged epoch {epoch} archive {} is incomplete or has invalid framing",
            path.display()
        ))),
        Err(error) => Err(AdaptiveValidationError::RetainStaging(format!(
            "deep validation failed without proving the staged epoch {epoch} archive invalid: {error}"
        ))),
    }
}

fn spawn_adaptive_validation(
    job: AdaptiveEpochJob,
    shutdown: Arc<AtomicBool>,
) -> tokio::task::JoinHandle<AdaptiveValidationResult> {
    tokio::task::spawn_blocking(move || {
        let result = adaptive_epoch_archive_validated(&job, &job.staged_output, &shutdown);
        AdaptiveValidationResult { job, result }
    })
}

fn handle_adaptive_validation_failure(
    job: &AdaptiveEpochJob,
    error: AdaptiveValidationError,
    may_retry: bool,
    attempts_per_epoch: u32,
    draining_failure: bool,
) -> AdaptiveValidationFailureAction {
    match error {
        AdaptiveValidationError::RetainStaging(error) => {
            AdaptiveValidationFailureAction::Terminal(format!(
                "epoch {} validation could not safely classify the staged archive: {error}; preserving staged attempt at {}",
                job.epoch,
                job.staged_output
                    .parent()
                    .unwrap_or(job.staged_output.as_path())
                    .display()
            ))
        }
        AdaptiveValidationError::InvalidArtifact(error) => {
            let cleanup = cleanup_adaptive_attempt(job);
            if may_retry && cleanup.is_ok() {
                return AdaptiveValidationFailureAction::Retry(error);
            }
            let mut failure = if job.attempt >= attempts_per_epoch {
                format!(
                    "epoch {} failed validation after {attempts_per_epoch} attempt(s): {error}",
                    job.epoch
                )
            } else if draining_failure {
                format!(
                    "epoch {} failed validation while adaptive admission was draining: {error}; retry suppressed",
                    job.epoch
                )
            } else {
                format!(
                    "epoch {} staged output failed validation: {error}",
                    job.epoch
                )
            };
            if let Err(cleanup_error) = cleanup {
                failure.push_str(&format!("; additionally failed cleanup: {cleanup_error}"));
            }
            AdaptiveValidationFailureAction::Terminal(failure)
        }
    }
}

fn handle_adaptive_child_failure(
    job: &AdaptiveEpochJob,
    exit_status: &str,
    may_retry: bool,
    attempts_per_epoch: u32,
    draining_failure: bool,
) -> AdaptiveChildFailureAction {
    match fs::symlink_metadata(&job.staged_output) {
        Ok(metadata) if metadata.file_type().is_file() => {
            AdaptiveChildFailureAction::Validate(format!(
                "epoch {} child failed (exit: {exit_status}) after creating a regular staged archive {}; running full validation before deciding whether it can be published or cleaned",
                job.epoch,
                job.staged_output.display()
            ))
        }
        Ok(_) => AdaptiveChildFailureAction::Terminal(format!(
            "epoch {} child failed (exit: {exit_status}) and its staged output is not a regular file; preserving staged attempt at {}",
            job.epoch,
            job.staged_output
                .parent()
                .unwrap_or(job.staged_output.as_path())
                .display()
        )),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let cleanup = cleanup_adaptive_attempt(job);
            if may_retry && cleanup.is_ok() {
                return AdaptiveChildFailureAction::Retry(format!(
                    "epoch {} child failed without producing a staged archive (exit: {exit_status})",
                    job.epoch
                ));
            }
            let mut failure = if job.attempt >= attempts_per_epoch {
                format!(
                    "epoch {} failed after {attempts_per_epoch} attempt(s) (exit: {exit_status})",
                    job.epoch
                )
            } else if draining_failure {
                format!(
                    "epoch {} child failed while adaptive admission was draining (exit: {exit_status}); retry suppressed",
                    job.epoch
                )
            } else {
                format!("epoch {} child failed (exit: {exit_status})", job.epoch)
            };
            if let Err(error) = cleanup {
                failure.push_str(&format!("; additionally failed cleanup: {error}"));
            }
            AdaptiveChildFailureAction::Terminal(failure)
        }
        Err(error) => AdaptiveChildFailureAction::Terminal(format!(
            "epoch {} child failed (exit: {exit_status}) and its staged archive could not be inspected: {error}; preserving staged attempt at {}",
            job.epoch,
            job.staged_output
                .parent()
                .unwrap_or(job.staged_output.as_path())
                .display()
        )),
    }
}

fn configured_adaptive_epoch_policy(
    has_historical_worker: bool,
    has_agave: bool,
) -> Result<(adaptive_epoch::EpochAdmissionPolicy, Duration, u64), String> {
    let mut policy = adaptive_epoch::EpochAdmissionPolicy::default();
    let explicit_memory_bound = env::var_os("JETSTREAMER_EPOCH_MEMORY_RESERVATION_GIB").is_some();
    if let Some(protected_gib) = adaptive_env_u64("JETSTREAMER_PROTECTED_MEMORY_GIB")? {
        policy.protected_memory_bytes = protected_gib
            .checked_mul(adaptive_epoch::GIB)
            .ok_or_else(|| "JETSTREAMER_PROTECTED_MEMORY_GIB is too large".to_string())?;
    }
    if let Some(min_system_gib) = adaptive_env_u64("JETSTREAMER_SYSTEM_MEMORY_RESERVE_GIB")? {
        policy.min_system_reserve_bytes = min_system_gib
            .checked_mul(adaptive_epoch::GIB)
            .ok_or_else(|| "JETSTREAMER_SYSTEM_MEMORY_RESERVE_GIB is too large".to_string())?;
    }
    if let Some(epoch_gib) = adaptive_env_u64("JETSTREAMER_EPOCH_MEMORY_RESERVATION_GIB")? {
        policy.epoch_memory_reservation_bytes =
            Some(epoch_gib.checked_mul(adaptive_epoch::GIB).ok_or_else(|| {
                "JETSTREAMER_EPOCH_MEMORY_RESERVATION_GIB is too large".to_string()
            })?);
    }
    if let Some(max_concurrency) = adaptive_env_u64("JETSTREAMER_ADAPTIVE_EPOCH_MAX")? {
        let max_concurrency = usize::try_from(max_concurrency)
            .map_err(|_| "JETSTREAMER_ADAPTIVE_EPOCH_MAX is too large".to_string())?;
        if !(1..=ADAPTIVE_EPOCH_HARD_MAX_CONCURRENCY).contains(&max_concurrency) {
            return Err(format!(
                "JETSTREAMER_ADAPTIVE_EPOCH_MAX must be in 1..={ADAPTIVE_EPOCH_HARD_MAX_CONCURRENCY}"
            ));
        }
        policy.max_concurrency = max_concurrency;
    }
    let disk_reservation_gib =
        adaptive_env_u64("JETSTREAMER_EPOCH_DISK_RESERVATION_GIB")?.unwrap_or(128);
    let disk_reservation_bytes = disk_reservation_gib
        .checked_mul(adaptive_epoch::GIB)
        .ok_or_else(|| "JETSTREAMER_EPOCH_DISK_RESERVATION_GIB is too large".to_string())?;
    if disk_reservation_bytes == 0 {
        return Err("JETSTREAMER_EPOCH_DISK_RESERVATION_GIB must be positive".to_string());
    }
    let requested_parallelism = policy.max_concurrency;
    policy.max_concurrency = adaptive_epoch::qualified_parallelism(
        requested_parallelism,
        explicit_memory_bound,
        strict_opt_in("JETSTREAMER_EPOCH_MEMORY_RESERVATION_QUALIFIED")?,
        env::var_os("JETSTREAMER_EPOCH_DISK_RESERVATION_GIB").is_some(),
        strict_opt_in("JETSTREAMER_EPOCH_DISK_RESERVATION_QUALIFIED")?,
    );
    if policy.max_concurrency == 1 && requested_parallelism > 1 {
        warn!(
            "adaptive parallelism held at one: concurrency above one requires explicit, qualified per-epoch memory and disk bounds"
        );
    }

    // Explicit runtime thread settings are inherited unchanged by every child
    // and are the only basis for multi-worker CPU admission. Charge two logical
    // CPUs per configured execution thread so an unset/default 32-thread worker
    // remains serial on a 64-thread host, while a qualified 16-thread setting
    // may use two workers.
    let historical_threads = if has_historical_worker {
        adaptive_env_u64("JETSTREAMER_HISTORICAL_POH_THREADS")?
    } else {
        None
    };
    let agave_threads = if has_agave {
        adaptive_env_u64("JETSTREAMER_REPLAY_THREADS")?
    } else {
        None
    };
    if historical_threads == Some(0) {
        return Err("JETSTREAMER_HISTORICAL_POH_THREADS must be positive".to_string());
    }
    if agave_threads == Some(0) {
        return Err("JETSTREAMER_REPLAY_THREADS must be positive".to_string());
    }
    let all_required_threads_are_explicit = (!has_historical_worker
        || historical_threads.is_some())
        && (!has_agave || agave_threads.is_some());
    if all_required_threads_are_explicit {
        let configured_threads = historical_threads
            .into_iter()
            .chain(agave_threads)
            .max()
            .expect("an adaptive range contains at least one runtime");
        let configured_threads = usize::try_from(configured_threads)
            .map_err(|_| "configured replay thread count is too large".to_string())?;
        policy.threads_per_epoch = configured_threads.saturating_mul(2);
    } else {
        policy.max_concurrency = 1;
    }

    let settle_secs = adaptive_env_u64("JETSTREAMER_ADAPTIVE_EPOCH_SETTLE_SECS")?
        .unwrap_or(DEFAULT_ADAPTIVE_EPOCH_SETTLE_SECS);
    Ok((
        policy,
        Duration::from_secs(settle_secs),
        disk_reservation_bytes,
    ))
}

fn spawn_adaptive_epoch_child(
    exe: &Path,
    effective_start: u64,
    end_epoch: u64,
    dest_dir: &Path,
    verify_snapshots: bool,
    job: &AdaptiveEpochJob,
) -> Result<(Child, i32), String> {
    use std::os::unix::process::CommandExt as _;

    let mut command = Command::new(exe);
    command
        .kill_on_drop(true)
        .env_remove("JETSTREAMER_EXPORT_HANDOFF_SNAPSHOT_DIR")
        .env(
            "JETSTREAMER_PARENT_EPOCH_LEASE_PID",
            std::process::id().to_string(),
        )
        // A retry must never inherit an operator override that reuses
        // crash-mutated accounts state.
        .env("JETSTREAMER_CLEAR_ACCOUNTS_ON_START", "true")
        .arg(job.epoch.to_string())
        .arg(dest_dir)
        .arg(if verify_snapshots {
            "--verify"
        } else {
            "--no-verify"
        })
        .arg(format!("--range-info={effective_start}-{end_epoch}"));
    if verify_snapshots {
        let mut hashes_arg = OsString::from("--epoch-hashes=");
        hashes_arg.push(&job.hashes_path);
        command.arg(hashes_arg);
    }
    let bound_bootstrap = job.bound_bootstrap.as_ref().ok_or_else(|| {
        format!(
            "refusing to spawn epoch {} without an admitted private bootstrap",
            job.epoch
        )
    })?;
    if let ReplayBootstrap::SnapshotArchive(snapshot_path) = bound_bootstrap {
        let mut snapshot_arg = OsString::from("--snapshot-archive=");
        snapshot_arg.push(snapshot_path);
        command.arg(snapshot_arg);
    }
    let mut output_arg = OsString::from("--horizon-output=");
    output_arg.push(&job.staged_output);
    command.arg(output_arg);
    let mut scratch_arg = OsString::from("--replay-scratch=");
    scratch_arg.push(&job.scratch_dir);
    command.arg(scratch_arg);
    // Give every epoch and all descendants one owned process group. This is
    // what lets shutdown/retry terminate historical workers as well as their
    // immediate coordinator.
    // SAFETY: only async-signal-safe libc operations run between fork/exec.
    unsafe {
        command.as_std_mut().pre_exec(|| {
            let parent = libc::getppid();
            if libc::setpgid(0, 0) != 0 {
                return Err(std::io::Error::last_os_error());
            }
            #[cfg(target_os = "linux")]
            {
                if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL) != 0 {
                    return Err(std::io::Error::last_os_error());
                }
                if parent == 1 || libc::getppid() != parent {
                    // Use a raw errno here: allocation and formatting are not
                    // async-signal-safe between fork and exec.
                    return Err(std::io::Error::from_raw_os_error(libc::ESRCH));
                }
            }
            Ok(())
        });
    }
    let child = command
        .spawn()
        .map_err(|err| format!("failed to spawn child for epoch {}: {err}", job.epoch))?;
    // Tokio guarantees a freshly spawned, unwaited Child has an id, and Linux
    // process IDs are positive pid_t values (well below i32::MAX).
    let process_group = child
        .id()
        .map(|pid| pid as libc::pid_t)
        .expect("a freshly spawned epoch child has a process id");
    Ok((child, process_group))
}

fn publish_staged_epoch_archive(validated: &ValidatedAdaptiveEpoch) -> Result<(), String> {
    let job = &validated.job;
    let publication = jetstreamer_node::archive_publish::publish_verified_archive(
        &job.staged_output,
        &job.final_output,
        validated.evidence,
    )
    .map_err(|error| {
        format!(
            "failed to transactionally publish verified epoch {} archive (commit_state={:?}): {error}",
            job.epoch,
            error.commit_state(),
        )
    })?;
    if let Some(recovery) = publication.recovery_directory {
        warn!(
            "epoch {}: retained replaced archive artifacts in {}",
            job.epoch,
            recovery.display()
        );
    }
    Ok(())
}

fn signal_process_group(process_group: i32, signal: i32) -> Result<(), String> {
    // SAFETY: a negative pid addresses exactly the process group created for
    // the owned child. Signal numbers are compile-time libc constants.
    if unsafe { libc::kill(-process_group, signal) } == 0 {
        return Ok(());
    }
    let error = std::io::Error::last_os_error();
    if error.raw_os_error() == Some(libc::ESRCH) {
        Ok(())
    } else {
        Err(format!(
            "failed to signal owned process group {process_group}: {error}"
        ))
    }
}

fn process_group_exists(process_group: i32) -> bool {
    // SAFETY: signal zero only checks existence/permission.
    if unsafe { libc::kill(-process_group, 0) } == 0 {
        return true;
    }
    std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
}

async fn wait_for_process_group_exit(process_group: i32, grace: Duration) -> bool {
    let deadline = Instant::now() + grace;
    while process_group_exists(process_group) && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    !process_group_exists(process_group)
}

async fn retain_until_process_group_exit(epoch: u64, process_group: i32) {
    if wait_for_process_group_exit(process_group, ADAPTIVE_PROCESS_GROUP_KILL_GRACE).await {
        return;
    }
    warn!(
        "owned epoch {epoch} process group {process_group} still has SIGKILL pending; retaining epoch leases until it disappears"
    );
    let mut last_warning = Instant::now();
    while process_group_exists(process_group) {
        tokio::time::sleep(Duration::from_secs(1)).await;
        if last_warning.elapsed() >= Duration::from_secs(60) {
            warn!(
                "owned epoch {epoch} process group {process_group} remains in the kernel; publication leases are still retained"
            );
            last_warning = Instant::now();
        }
    }
}

async fn kill_remaining_adaptive_process_group(process_group: i32) -> Result<(), String> {
    signal_process_group(process_group, libc::SIGKILL)?;
    if wait_for_process_group_exit(process_group, ADAPTIVE_PROCESS_GROUP_KILL_GRACE).await {
        Ok(())
    } else {
        Err(format!(
            "owned process group {process_group} remained after SIGKILL"
        ))
    }
}

async fn stop_adaptive_epoch_children(
    running: &mut BTreeMap<u64, RunningAdaptiveEpoch>,
) -> Result<(), String> {
    let process_groups = running
        .values()
        .map(|running_epoch| (running_epoch.job.epoch, running_epoch.process_group))
        .collect::<Vec<_>>();
    let mut failures = Vec::new();
    for running_epoch in running.values_mut() {
        if let Err(error) = signal_process_group(running_epoch.process_group, libc::SIGTERM) {
            failures.push(format!(
                "failed to stop owned epoch {} process tree: {error}",
                running_epoch.job.epoch,
            ));
        }
    }
    let deadline = Instant::now() + ADAPTIVE_PROCESS_GROUP_GRACE;
    while Instant::now() < deadline
        && running
            .values()
            .any(|child| process_group_exists(child.process_group))
    {
        // Reap exited leaders as we wait. Otherwise their zombie entries can
        // make kill(-pgid, 0) report a live process group for the full grace
        // period even when no descendant remains.
        for running_epoch in running.values_mut() {
            if let Err(error) = running_epoch.child.try_wait() {
                warn!(
                    "failed to poll owned epoch {} child while stopping: {error}",
                    running_epoch.job.epoch
                );
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    // Queue SIGKILL for every surviving group before waiting for any direct
    // leader. Waiting first can leave a later process tree running needlessly.
    for (epoch, process_group) in &process_groups {
        if process_group_exists(*process_group)
            && let Err(error) = signal_process_group(*process_group, libc::SIGKILL)
        {
            failures.push(format!(
                "failed to kill owned epoch {epoch} process tree: {error}"
            ));
        }
    }
    let mut children = std::mem::take(running);
    for running_epoch in children.values_mut() {
        if let Err(err) = running_epoch.child.wait().await {
            failures.push(format!(
                "failed to reap owned epoch {} child: {err}",
                running_epoch.job.epoch,
            ));
        }
    }
    // Reaping the group leaders before this check prevents zombie leaders
    // from masquerading as live groups. Never release an epoch lease while a
    // descendant is still visible, even if it is stuck in uninterruptible
    // kernel sleep with SIGKILL pending; systemd cgroup teardown remains the
    // operator escape hatch.
    for (epoch, process_group) in process_groups {
        retain_until_process_group_exit(epoch, process_group).await;
    }
    if failures.is_empty() {
        Ok(())
    } else {
        Err(failures.join("; "))
    }
}

async fn cancel_adaptive_epoch_work(
    shutdown: &Arc<AtomicBool>,
    running: &mut BTreeMap<u64, RunningAdaptiveEpoch>,
    validating: &mut BTreeMap<u64, tokio::task::JoinHandle<AdaptiveValidationResult>>,
) -> Result<(), String> {
    // Validation runs in spawn_blocking, so dropping a JoinHandle would detach
    // it. Request cooperative cancellation first, then drain every task before
    // the supervisor releases its epoch leases or private work directories.
    shutdown.store(true, Ordering::SeqCst);
    let mut failures = Vec::new();
    if let Err(error) = stop_adaptive_epoch_children(running).await {
        failures.push(error);
    }
    for (epoch, validation) in std::mem::take(validating) {
        if let Err(error) = validation.await {
            failures.push(format!(
                "epoch {epoch} validation task failed while draining: {error}"
            ));
        }
    }
    if failures.is_empty() {
        Ok(())
    } else {
        Err(failures.join("; "))
    }
}

fn append_cleanup_error(primary: String, cleanup: Result<(), String>) -> String {
    match cleanup {
        Ok(()) => primary,
        Err(cleanup) => format!("{primary}; cleanup did not fully quiesce: {cleanup}"),
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_epoch_range_supervisor_adaptive(
    effective_start: u64,
    end_epoch: u64,
    dest_dir: &Path,
    verify_snapshots: bool,
    allow_candidate_runtime: bool,
    shutdown: Arc<AtomicBool>,
    boundary_bootstraps: BTreeMap<u64, ReplayBootstrap>,
    snapshot_expectations: BTreeMap<u64, BTreeMap<Slot, BankHashExpectation>>,
) -> Result<(), String> {
    let load_from_dir = env_truthy("JETSTREAMER_LOAD_FROM_DIR");
    if !range_supports_private_replay_scratch(
        effective_start,
        end_epoch,
        allow_candidate_runtime,
        load_from_dir,
    )? {
        return Err(
            "adaptive epoch concurrency requires every runtime to support private replay scratch"
                .to_string(),
        );
    }
    let exe =
        env::current_exe().map_err(|err| format!("failed to resolve current executable: {err}"))?;
    let attempts_per_epoch = env::var("JETSTREAMER_EPOCH_ATTEMPTS")
        .ok()
        .and_then(|value| value.trim().parse::<u32>().ok())
        .filter(|attempts| *attempts > 0)
        .unwrap_or(2);
    let (has_historical_worker, has_agave) =
        range_runtime_kinds(effective_start, end_epoch, allow_candidate_runtime)?;
    let (policy, settle_interval, disk_reservation_bytes) =
        configured_adaptive_epoch_policy(has_historical_worker, has_agave)?;
    let prune_snapshots = env_truthy_default("JETSTREAMER_PRUNE_EPOCH_SNAPSHOTS", false);
    let range_work = private_epoch_scope(dest_dir)?.join("work");
    jetstreamer_node::archive_publish::preflight_archive_publication(&range_work, dest_dir)
        .map_err(|error| format!("archive publication capability preflight failed: {error}"))?;
    let initial_cgroup = adaptive_epoch::read_current_cgroup_memory(&[]);
    let cgroup_baseline_bytes = initial_cgroup.map_or(0, |snapshot| snapshot.current_bytes);
    let mut pending = VecDeque::new();
    let mut recovery_jobs = Vec::new();
    let mut published_epochs = HashSet::new();
    for epoch in effective_start..=end_epoch {
        let (slot_start, slot_end_inclusive) = epoch_to_slot_range(epoch);
        let spans = compatibility::plan_runtime_spans(
            slot_start..slot_end_inclusive.saturating_add(1),
            allow_candidate_runtime,
        )?;
        let selection = runtime_span_selection(
            spans
                .first()
                .expect("runtime planner rejects empty epoch ranges"),
        )?;
        let final_output = dest_dir.join(format!("epoch-{epoch}.jet"));
        if jetstreamer_node::archive_checksum::archive_checksum_is_publication_sentinel(
            &final_output,
        )
        .map_err(|error| format!("failed to inspect epoch {epoch} publication marker: {error}"))?
        {
            return Err(format!(
                "epoch {epoch} has an interrupted publication sentinel at {}; manual transaction recovery is required",
                jetstreamer_node::archive_checksum::archive_checksum_path(&final_output)
                    .map_err(|error| format!("failed to resolve checksum path: {error}"))?
                    .display()
            ));
        }
        let reusable = if spans.len() == 1 {
            validated_epoch_archive(&final_output, epoch, selection, None, None)
        } else {
            validated_epoch_archive_multi_runtime(&final_output, epoch, &spans, None)
        };
        match reusable {
            Ok(Some(evidence)) => {
                jetstreamer_node::archive_checksum::ensure_archive_checksum_for_validated(
                    &final_output,
                    evidence,
                )
                .map_err(|err| {
                    format!(
                        "failed to validate or repair checksum for reusable epoch {epoch} archive {}: {err}",
                        final_output.display()
                    )
                })?;
                published_epochs.insert(epoch);
                continue;
            }
            Ok(None) => {}
            Err(error) => warn!(
                "epoch {epoch}: pre-existing output {} is not reusable; regenerating it: {error}",
                final_output.display()
            ),
        }
        let source_bootstrap = boundary_bootstraps.get(&epoch).cloned().ok_or_else(|| {
            format!("range supervisor has no staged bootstrap state for epoch {epoch}")
        })?;
        let work_dir = range_work.join(format!("epoch-{epoch}"));
        create_or_validate_private_directory(&work_dir)?;
        let input_dir = work_dir.join("inputs");
        create_or_validate_private_directory(&input_dir)?;
        let hashes_path = if verify_snapshots {
            let expected = snapshot_expectations.get(&epoch).ok_or_else(|| {
                format!("range supervisor has no in-memory checkpoint set for epoch {epoch}")
            })?;
            write_private_epoch_hashes(&input_dir, expected, selection.descriptor.bootstrap)?
        } else {
            input_dir.join("epoch-hashes.disabled")
        };
        let source_snapshot = source_bootstrap.snapshot_archive().map(Path::to_path_buf);
        let staged_candidate = staged_archive_candidate(&work_dir, epoch)?;
        let mut job = AdaptiveEpochJob {
            epoch,
            spans,
            selection,
            source_bootstrap,
            bound_bootstrap: None,
            source_snapshot,
            hashes_path,
            final_output,
            staged_output: PathBuf::new(),
            scratch_dir: PathBuf::new(),
            work_dir,
            attempt: 0,
        };
        if let Some(candidate) = staged_candidate {
            info!(
                "epoch {epoch}: validating crash-recovered staged candidate {}",
                candidate.display()
            );
            job.staged_output = candidate;
            recovery_jobs.push(job);
        } else {
            pending.push_back(job);
        }
    }

    let mut running: BTreeMap<u64, RunningAdaptiveEpoch> = BTreeMap::new();
    let mut validating = BTreeMap::new();
    for job in recovery_jobs {
        validating.insert(job.epoch, spawn_adaptive_validation(job, shutdown.clone()));
    }
    let mut ready: BTreeMap<u64, ValidatedAdaptiveEpoch> = BTreeMap::new();
    let mut next_publish_epoch = effective_start;
    let mut cohort_started = (!validating.is_empty()).then(Instant::now);
    let mut last_admission: Option<(bool, bool, usize)> = None;
    let mut failure_drain = AdaptiveFailureDrain::default();
    info!(
        "adaptive epoch supervisor enabled (historical={}, agave={}, max={}, settle={}s, protected={} GiB, system-min={} GiB, memory-reservation={} GiB, disk-reservation={} GiB, private-work={}, pruning={})",
        has_historical_worker,
        has_agave,
        policy.max_concurrency,
        settle_interval.as_secs(),
        policy.protected_memory_bytes / adaptive_epoch::GIB,
        policy.min_system_reserve_bytes / adaptive_epoch::GIB,
        policy
            .evaluate(adaptive_epoch::read_host_telemetry(), 0)
            .epoch_reservation_bytes
            / adaptive_epoch::GIB,
        disk_reservation_bytes / adaptive_epoch::GIB,
        range_work.display(),
        prune_snapshots,
    );

    loop {
        if shutdown.load(Ordering::SeqCst) {
            info!(
                "shutdown requested; stopping {} owned epoch child(ren) and {} validation task(s)",
                running.len(),
                validating.len(),
            );
            cancel_adaptive_epoch_work(&shutdown, &mut running, &mut validating).await?;
            return Ok(());
        }

        let mut retries = Vec::new();
        let running_epochs = running.keys().copied().collect::<Vec<_>>();
        for epoch in running_epochs {
            let status = match running
                .get_mut(&epoch)
                .expect("running epoch key came from this map")
                .child
                .try_wait()
            {
                Ok(status) => status,
                Err(err) => {
                    let primary = format!("failed to poll child for epoch {epoch}: {err}");
                    let cleanup =
                        cancel_adaptive_epoch_work(&shutdown, &mut running, &mut validating).await;
                    return Err(append_cleanup_error(primary, cleanup));
                }
            };
            let Some(status) = status else {
                continue;
            };
            let completed = running
                .remove(&epoch)
                .expect("completed epoch remained in the running map");
            // The leader exited, so no legitimate descendant should remain.
            // Kill any orphan before releasing this attempt's capacity/scratch.
            if let Err(error) = kill_remaining_adaptive_process_group(completed.process_group).await
            {
                // Put the completed leader back under owned cleanup so its
                // surviving descendants receive a second fail-closed pass.
                running.insert(epoch, completed);
                let cleanup =
                    cancel_adaptive_epoch_work(&shutdown, &mut running, &mut validating).await;
                return Err(append_cleanup_error(error, cleanup));
            }
            if status.success() {
                info!(
                    "epoch {epoch} child completed; validating and hashing private staging off the supervisor loop (attempt {}/{attempts_per_epoch})",
                    completed.job.attempt
                );
                validating.insert(
                    epoch,
                    spawn_adaptive_validation(completed.job, shutdown.clone()),
                );
            } else {
                let job = completed.job;
                let may_retry = failure_drain.may_retry(job.attempt, attempts_per_epoch);
                match handle_adaptive_child_failure(
                    &job,
                    &status.to_string(),
                    may_retry,
                    attempts_per_epoch,
                    failure_drain.is_active(),
                ) {
                    AdaptiveChildFailureAction::Validate(message) => {
                        warn!("{message}");
                        validating.insert(epoch, spawn_adaptive_validation(job, shutdown.clone()));
                    }
                    AdaptiveChildFailureAction::Retry(message) => {
                        warn!("{message}; retrying cleanly");
                        retries.push(job);
                    }
                    AdaptiveChildFailureAction::Terminal(failure) => {
                        record_adaptive_terminal_failure(
                            &mut failure_drain,
                            failure,
                            running.len(),
                            validating.len(),
                        );
                    }
                }
            }
        }

        let completed_validations = validating
            .iter()
            .filter_map(|(epoch, validation)| validation.is_finished().then_some(*epoch))
            .collect::<Vec<_>>();
        for epoch in completed_validations {
            let validation = validating
                .remove(&epoch)
                .expect("completed validation key came from this map");
            let outcome = match validation.await {
                Ok(outcome) => outcome,
                Err(error) => {
                    record_adaptive_terminal_failure(
                        &mut failure_drain,
                        format!("epoch {epoch} validation task failed: {error}"),
                        running.len(),
                        validating.len(),
                    );
                    continue;
                }
            };
            match outcome.result {
                Ok(evidence) => {
                    info!(
                        "epoch {epoch}: full decode, semantic chain, slot count, provenance, and SHA-256 validation passed"
                    );
                    ready.insert(
                        epoch,
                        ValidatedAdaptiveEpoch {
                            job: outcome.job,
                            evidence,
                        },
                    );
                }
                Err(error) => {
                    let job = outcome.job;
                    if shutdown.load(Ordering::SeqCst) {
                        continue;
                    }
                    let may_retry = failure_drain.may_retry(job.attempt, attempts_per_epoch);
                    match handle_adaptive_validation_failure(
                        &job,
                        error,
                        may_retry,
                        attempts_per_epoch,
                        failure_drain.is_active(),
                    ) {
                        AdaptiveValidationFailureAction::Retry(error) => {
                            warn!(
                                "epoch {epoch} staged output failed validation: {error}; retrying cleanly"
                            );
                            retries.push(job);
                        }
                        AdaptiveValidationFailureAction::Terminal(failure) => {
                            record_adaptive_terminal_failure(
                                &mut failure_drain,
                                failure,
                                running.len(),
                                validating.len(),
                            );
                        }
                    }
                }
            }
        }
        if failure_drain.is_active() {
            // Every retry candidate has already had its failed attempt cleaned.
            // Dropping it here prevents new work from entering a failed cohort.
            retries.clear();
        } else {
            merge_adaptive_retries(&mut pending, retries);
        }
        if adaptive_resource_occupancy(running.len(), validating.len(), ready.len()) == 0 {
            cohort_started = None;
        }

        loop {
            while next_publish_epoch <= end_epoch && published_epochs.remove(&next_publish_epoch) {
                next_publish_epoch += 1;
            }
            let Some(epoch) = next_adaptive_publication_epoch(
                next_publish_epoch,
                &ready,
                failure_drain.is_active(),
            ) else {
                break;
            };
            let in_order = epoch == next_publish_epoch;
            let validated = ready
                .remove(&epoch)
                .expect("publication epoch came from the ready map");
            if let Err(err) = publish_staged_epoch_archive(&validated) {
                if failure_drain.is_active() {
                    record_adaptive_terminal_failure(
                        &mut failure_drain,
                        err,
                        running.len(),
                        validating.len(),
                    );
                    continue;
                }
                let cleanup =
                    cancel_adaptive_epoch_work(&shutdown, &mut running, &mut validating).await;
                return Err(append_cleanup_error(err, cleanup));
            }
            let job = &validated.job;
            info!(
                "epoch {}: transactionally published verified archive {}",
                job.epoch,
                job.final_output.display()
            );
            let _ = fs::remove_file(&job.hashes_path);
            if prune_snapshots && let Some(path) = &job.source_snapshot {
                match prune_epoch_boundary_snapshot(path, job.selection.descriptor) {
                    Ok(()) => info!("pruned boundary snapshot artifacts for {}", path.display()),
                    Err(err) => warn!(
                        "failed to prune boundary snapshot artifacts for {}: {err}",
                        path.display()
                    ),
                }
            }
            if let Err(err) = fs::remove_dir_all(&job.work_dir) {
                warn!(
                    "epoch {} was published, but private work cleanup failed for {}: {err}",
                    job.epoch,
                    job.work_dir.display()
                );
            }
            if in_order {
                next_publish_epoch += 1;
            }
        }

        if next_publish_epoch > end_epoch
            && running.is_empty()
            && validating.is_empty()
            && ready.is_empty()
            && pending.is_empty()
        {
            info!("=== all epochs {effective_start}-{end_epoch} complete ===");
            return Ok(());
        }

        if failure_drain.is_active() {
            if failure_drain.is_drained(running.len(), validating.len(), ready.len()) {
                return Err(std::mem::take(&mut failure_drain).into_error());
            }
            // Admission is permanently closed for this supervisor invocation.
            // Do not let retries or untouched pending epochs replace the
            // resources released by a draining sibling.
            tokio::time::sleep(ADAPTIVE_EPOCH_POLL_INTERVAL).await;
            continue;
        }

        let resource_occupied =
            adaptive_resource_occupancy(running.len(), validating.len(), ready.len());
        let owned_process_groups = running
            .values()
            .map(|child| child.process_group as u32)
            .collect::<Vec<_>>();
        let telemetry = adaptive_epoch::read_host_telemetry();
        let admission = policy.evaluate(telemetry, resource_occupied);
        let cgroup = adaptive_epoch::read_current_cgroup_memory(&owned_process_groups);
        let contained = cgroup.is_some_and(|snapshot| {
            snapshot.dedicated && snapshot.max_bytes <= admission.owned_budget_bytes
        });
        let effective_capacity = adaptive_epoch::contained_capacity(
            admission,
            cgroup,
            cgroup_baseline_bytes,
            resource_occupied,
        );
        let disk_available = adaptive_epoch::read_filesystem_available_bytes(&range_work);
        let disk_admitted = adaptive_epoch::disk_allows_additional_epoch(
            disk_available,
            disk_reservation_bytes,
            resource_occupied,
        );
        let admission_key = (
            admission.telemetry_reliable,
            contained && disk_admitted,
            effective_capacity,
        );
        if last_admission != Some(admission_key) {
            if admission.telemetry_reliable {
                info!(
                    "adaptive epoch admission capacity={} resource-occupied={} (running={}, validating={}, ready={}) host-available={} GiB cgroup-current={} GiB memory-reservation={} GiB disk-available={} GiB disk-reservation={} GiB",
                    effective_capacity,
                    resource_occupied,
                    running.len(),
                    validating.len(),
                    ready.len(),
                    telemetry
                        .expect("reliable admission retained telemetry")
                        .memory_available_bytes
                        / adaptive_epoch::GIB,
                    cgroup.map_or(0, |snapshot| snapshot.current_bytes) / adaptive_epoch::GIB,
                    admission.epoch_reservation_bytes / adaptive_epoch::GIB,
                    disk_available.unwrap_or(0) / adaptive_epoch::GIB,
                    disk_reservation_bytes / adaptive_epoch::GIB,
                );
            } else {
                warn!("host memory/CPU telemetry unavailable; falling back to serial epochs");
            }
            if !contained {
                warn!(
                    "finite inherited cgroup memory.max is absent or exceeds the owned budget; adaptive epoch concurrency is held at one"
                );
            }
            if !disk_admitted {
                warn!(
                    "filesystem headroom is below the conservative reservation for another epoch; admission is paused"
                );
            }
            last_admission = Some(admission_key);
        }
        let elapsed = cohort_started
            .map(|started| started.elapsed())
            .unwrap_or(Duration::ZERO);
        let target_capacity = adaptive_epoch::ramped_capacity(
            effective_capacity,
            resource_occupied,
            elapsed,
            settle_interval,
        );
        while adaptive_resource_occupancy(running.len(), validating.len(), ready.len())
            < target_capacity
        {
            let occupied_now =
                adaptive_resource_occupancy(running.len(), validating.len(), ready.len());
            if !adaptive_epoch::disk_allows_additional_epoch(
                adaptive_epoch::read_filesystem_available_bytes(&range_work),
                disk_reservation_bytes,
                occupied_now,
            ) {
                break;
            }
            let Some(mut job) = pending.pop_front() else {
                break;
            };
            if let Err(error) = prepare_adaptive_attempt(&mut job) {
                record_adaptive_terminal_failure(
                    &mut failure_drain,
                    error,
                    running.len(),
                    validating.len(),
                );
                break;
            }
            info!(
                "=== epoch {}: spawning fresh private staged child (attempt {}/{attempts_per_epoch}, occupied={}/{target_capacity}) ===",
                job.epoch,
                job.attempt,
                occupied_now + 1,
            );
            match spawn_adaptive_epoch_child(
                &exe,
                effective_start,
                end_epoch,
                dest_dir,
                verify_snapshots,
                &job,
            ) {
                Ok((child, process_group)) => {
                    cohort_started = Some(Instant::now());
                    running.insert(
                        job.epoch,
                        RunningAdaptiveEpoch {
                            job,
                            child,
                            process_group,
                        },
                    );
                }
                Err(err) if job.attempt < attempts_per_epoch => {
                    warn!("{err}; retrying epoch {}", job.epoch);
                    if let Err(cleanup_error) = cleanup_adaptive_attempt(&job) {
                        record_adaptive_terminal_failure(
                            &mut failure_drain,
                            format!("{err}; additionally failed cleanup: {cleanup_error}"),
                            running.len(),
                            validating.len(),
                        );
                        break;
                    }
                    pending.push_front(job);
                    break;
                }
                Err(err) => {
                    let cleanup_error = cleanup_adaptive_attempt(&job).err();
                    let failure = cleanup_error.map_or(err.clone(), |cleanup_error| {
                        format!("{err}; additionally failed cleanup: {cleanup_error}")
                    });
                    record_adaptive_terminal_failure(
                        &mut failure_drain,
                        failure,
                        running.len(),
                        validating.len(),
                    );
                    break;
                }
            }
        }

        tokio::time::sleep(ADAPTIVE_EPOCH_POLL_INTERVAL).await;
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_epoch_range_supervisor(
    effective_start: u64,
    end_epoch: u64,
    dest_dir: &Path,
    verify_snapshots: bool,
    allow_candidate_runtime: bool,
    adaptive_epoch_concurrency: bool,
    shutdown: Arc<AtomicBool>,
    boundary_bootstraps: BTreeMap<u64, ReplayBootstrap>,
    snapshot_expectations: BTreeMap<u64, BTreeMap<Slot, BankHashExpectation>>,
) -> Result<(), String> {
    if adaptive_epoch_concurrency {
        run_epoch_range_supervisor_adaptive(
            effective_start,
            end_epoch,
            dest_dir,
            verify_snapshots,
            allow_candidate_runtime,
            shutdown,
            boundary_bootstraps,
            snapshot_expectations,
        )
        .await
    } else {
        run_epoch_range_supervisor_serial(
            effective_start,
            end_epoch,
            dest_dir,
            verify_snapshots,
            allow_candidate_runtime,
            shutdown,
            boundary_bootstraps,
        )
        .await
    }
}

/// Runs each epoch of a range in its own child process (this same binary,
/// invoked as `<exe> <epoch> <dest-dir> --epoch-hashes=… --range-info=…`), so
/// every byte of replay memory — accounts-db growth, caches, the bank itself —
/// is returned to the OS at each epoch boundary. In-process chaining was
/// observed OOM-killed at ~745 GiB RSS two epochs into a range; isolation
/// trades one state load per epoch for a hard per-epoch memory cap. Children
/// never touch gcloud: genesis, snapshots, and hash files were staged by the
/// caller before this runs.
async fn run_epoch_range_supervisor_serial(
    effective_start: u64,
    end_epoch: u64,
    dest_dir: &Path,
    verify_snapshots: bool,
    allow_candidate_runtime: bool,
    shutdown: Arc<AtomicBool>,
    boundary_bootstraps: BTreeMap<u64, ReplayBootstrap>,
) -> Result<(), String> {
    let exe =
        env::current_exe().map_err(|err| format!("failed to resolve current executable: {err}"))?;
    let attempts_per_epoch = env::var("JETSTREAMER_EPOCH_ATTEMPTS")
        .ok()
        .and_then(|v| v.trim().parse::<u32>().ok())
        .filter(|&a| a > 0)
        .unwrap_or(2);
    let prune_snapshots = env_truthy_default("JETSTREAMER_PRUNE_EPOCH_SNAPSHOTS", false);
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
            jetstreamer_node::archive_checksum::ensure_archive_checksum(&jet_path).map_err(
                |err| {
                    format!(
                        "failed to validate or repair checksum for reusable epoch {epoch} archive {}: {err}",
                        jet_path.display()
                    )
                },
            )?;
            info!("epoch {epoch} already complete; skipping");
            continue;
        }
        let hashes_path = dest_dir.join(format!("epoch-hashes-{epoch}.txt"));
        let bootstrap = boundary_bootstraps.get(&epoch).ok_or_else(|| {
            format!("range supervisor has no staged bootstrap state for epoch {epoch}")
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
                .env(
                    "JETSTREAMER_PARENT_EPOCH_LEASE_PID",
                    std::process::id().to_string(),
                )
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
            if let ReplayBootstrap::SnapshotArchive(snapshot_path) = bootstrap {
                let mut snapshot_arg = OsString::from("--snapshot-archive=");
                snapshot_arg.push(snapshot_path.as_os_str());
                cmd.arg(snapshot_arg);
            }
            cmd.kill_on_drop(true);
            let mut child = cmd
                .spawn()
                .map_err(|err| format!("failed to spawn child for epoch {epoch}: {err}"))?;
            let status = child
                .wait()
                .await
                .map_err(|err| format!("failed to wait for child for epoch {epoch}: {err}"))?;
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
                jetstreamer_node::archive_checksum::ensure_archive_checksum(&jet_path).map_err(
                    |err| {
                        format!(
                            "failed to publish checksum for verified epoch {epoch} archive {}: {err}",
                            jet_path.display()
                        )
                    },
                )?;
                info!("epoch {epoch} child completed");
                let _ = fs::remove_file(&hashes_path);
                if prune_snapshots
                    && let Some(ReplayBootstrap::SnapshotArchive(path)) =
                        boundary_bootstraps.get(&epoch)
                {
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
    let mut verify_option_count = 0usize;
    let mut horizon_output: Option<PathBuf> = None;
    let mut qualification_end_slot: Option<Slot> = None;
    // Internal flags set by the range supervisor when spawning per-epoch
    // children: pre-fetched snapshot hashes (so the child never touches
    // gcloud) and the overall range for the child's overall-progress line.
    let mut epoch_hashes: Option<PathBuf> = None;
    let mut snapshot_archive_override: Option<PathBuf> = None;
    let mut range_info: Option<(u64, u64)> = None;
    let mut replay_scratch: Option<PathBuf> = None;
    let mut root_checkpoint_cohort = false;
    let mut recover_staged_only = false;
    let mut cohort_manifest: Option<PathBuf> = None;
    let mut cohort_manifest_fingerprint: Option<String> = None;
    for arg in args {
        if arg == "--verify" {
            verify_snapshots = true;
            explicit_verify = Some(true);
            verify_option_count = verify_option_count.saturating_add(1);
        } else if arg == "--no-verify" {
            verify_snapshots = false;
            explicit_verify = Some(false);
            verify_option_count = verify_option_count.saturating_add(1);
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
        } else if let Some(path) = arg.strip_prefix("--replay-scratch=") {
            replay_scratch = Some(PathBuf::from(path));
        } else if arg == "--root-checkpoint-cohort" {
            if root_checkpoint_cohort {
                eprintln!("duplicate --root-checkpoint-cohort option");
                exit(2);
            }
            root_checkpoint_cohort = true;
        } else if arg == "--recover-staged-only" {
            if recover_staged_only {
                eprintln!("duplicate --recover-staged-only option");
                exit(2);
            }
            recover_staged_only = true;
        } else if let Some(path) = arg.strip_prefix("--cohort-manifest=") {
            if cohort_manifest.replace(PathBuf::from(path)).is_some() {
                eprintln!("duplicate --cohort-manifest option");
                exit(2);
            }
        } else if let Some(fingerprint) = arg.strip_prefix("--cohort-manifest-fingerprint=") {
            if cohort_manifest_fingerprint
                .replace(fingerprint.to_owned())
                .is_some()
            {
                eprintln!("duplicate --cohort-manifest-fingerprint option");
                exit(2);
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
    let inherited_epoch_lease = inherited_epoch_lease_authorized();

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
    if replay_scratch.is_some() && start_epoch != end_epoch {
        eprintln!("--replay-scratch applies only to a single-epoch (child) invocation");
        exit(2);
    }
    if let Err(error) = validate_staged_recovery_mode(
        recover_staged_only,
        start_epoch,
        end_epoch,
        explicit_verify,
        verify_option_count,
        inherited_epoch_lease,
        root_checkpoint_cohort
            || horizon_output.is_some()
            || qualification_end_slot.is_some()
            || epoch_hashes.is_some()
            || snapshot_archive_override.is_some()
            || range_info.is_some()
            || replay_scratch.is_some()
            || cohort_manifest.is_some()
            || cohort_manifest_fingerprint.is_some(),
    ) {
        eprintln!("{error}");
        exit(2);
    }
    if qualification_end_slot.is_some() && range_info.is_some() {
        eprintln!("--qualification-end-slot cannot be combined with --range-info");
        exit(2);
    }
    if root_checkpoint_cohort {
        if explicit_verify != Some(true) {
            eprintln!("--root-checkpoint-cohort requires explicit --verify");
            exit(2);
        }
        if cohort_manifest.is_none() || cohort_manifest_fingerprint.is_none() {
            eprintln!(
                "--root-checkpoint-cohort requires --cohort-manifest and --cohort-manifest-fingerprint"
            );
            exit(2);
        }
        if horizon_output.is_some()
            || qualification_end_slot.is_some()
            || epoch_hashes.is_some()
            || snapshot_archive_override.is_some()
            || range_info.is_some()
            || replay_scratch.is_some()
        {
            eprintln!(
                "--root-checkpoint-cohort cannot be combined with output, qualification, or internal child overrides"
            );
            exit(2);
        }
    } else if cohort_manifest.is_some() || cohort_manifest_fingerprint.is_some() {
        eprintln!("cohort manifest options require --root-checkpoint-cohort");
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
    if let Err(err) = fs::create_dir_all(&dest_dir) {
        eprintln!(
            "error: failed to create destination directory {}: {err}",
            dest_dir.display()
        );
        exit(1);
    }
    let destination_binding = match BoundDestination::bind(&dest_dir) {
        Ok(binding) => binding,
        Err(err) => {
            eprintln!("error: {err}");
            exit(1);
        }
    };
    // Resolve the CLI path once. Leases, private scopes, scratch paths, and
    // final publication all use this canonical path even if the original CLI
    // path was a symlink that is retargeted later.
    let dest_dir = destination_binding.path().to_path_buf();
    // Top-level producers hold one advisory lease per output epoch from before
    // bootstrap/hash binding through final checksum publication. Internal
    // children are covered by their parent's still-open lease.
    let _epoch_leases = if !inherited_epoch_lease {
        match acquire_epoch_leases(&dest_dir, start_epoch, end_epoch) {
            Ok(leases) => {
                info!(
                    "acquired {} epoch publication lease(s) for {start_epoch}-{end_epoch}",
                    leases.len()
                );
                leases
            }
            Err(error) => {
                eprintln!("error: {error}");
                exit(1);
            }
        }
    } else {
        Vec::new()
    };

    // A durable batch outcome is delivered before this invocation inspects or
    // reuses any archive. Recovery is a complete invocation boundary: persist
    // the exact private receipt, acknowledge its transaction ID, and stop.
    let batch_receipt_directory = if inherited_epoch_lease {
        None
    } else {
        Some(match cohort_batch_receipt_directory(&destination_binding) {
            Ok(path) => path,
            Err(error) => {
                eprintln!("error: {error}");
                exit(1);
            }
        })
    };
    if let Some(receipt_directory) = batch_receipt_directory.as_deref() {
        match cohort_publication::recover_pending_batch(
            destination_binding.publication_binding(),
            receipt_directory,
        ) {
            Ok(cohort_publication::RecoveryDisposition::None) => {}
            Ok(cohort_publication::RecoveryDisposition::Stop {
                outcome,
                receipt_path,
            }) => {
                match (outcome, receipt_path) {
                    (Some(outcome), Some(receipt_path)) => println!(
                        "archive batch recovery completed with {outcome:?}; receipt {} is durable; rerun the command to continue",
                        receipt_path.display()
                    ),
                    (None, None) => println!(
                        "cleared an unarmed archive batch marker; rerun the command to continue"
                    ),
                    _ => unreachable!("recovery outcome and receipt are paired"),
                }
                return;
            }
            Err(error) => {
                eprintln!("error: {error}; archive batch state remains closed");
                exit(1);
            }
        }
    }

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
    if recover_staged_only {
        if let Err(error) = recover_staged_epoch_archive_only(
            start_epoch,
            &destination_binding,
            allow_candidate_runtime,
            shutdown.clone(),
        )
        .await
        {
            eprintln!("error: {error}");
            exit(1);
        }
        return;
    }
    let cohort_runtime = if root_checkpoint_cohort {
        match root_checkpoint_cohort_runtime(start_epoch, end_epoch, allow_candidate_runtime) {
            Ok(selection) => Some(selection),
            Err(err) => {
                eprintln!("error: {err}");
                exit(1);
            }
        }
    } else {
        None
    };
    let cohort_plan = if root_checkpoint_cohort {
        let manifest_path = cohort_manifest
            .as_deref()
            .expect("cohort manifest option was required");
        let fingerprint = cohort_manifest_fingerprint
            .as_deref()
            .expect("cohort manifest fingerprint was required");
        match load_root_checkpoint_cohort_plan(
            manifest_path,
            fingerprint,
            start_epoch,
            end_epoch,
            cohort_runtime.expect("cohort runtime was validated"),
        ) {
            Ok(plan) => {
                info!(
                    "root-checkpoint cohort {}-{} bound sealed preflight {}",
                    start_epoch, end_epoch, plan.fingerprint
                );
                Some(plan)
            }
            Err(err) => {
                eprintln!("error: {err}");
                exit(1);
            }
        }
    } else {
        None
    };
    let mut cohort_run = if let Some(plan) = cohort_plan.as_ref() {
        let scope = match private_epoch_scope(&dest_dir) {
            Ok(scope) => scope,
            Err(err) => {
                eprintln!("error: {err}");
                exit(1);
            }
        };
        let cohort_root = scope
            .join("work")
            .join(format!("root-cohort-{start_epoch}-{end_epoch}"));
        match CohortRunDirectory::create(cohort_root, start_epoch, end_epoch, &plan.fingerprint) {
            Ok(run) => {
                if let Err(err) = run.preflight_publication(&destination_binding) {
                    eprintln!(
                        "error: {err}; private run retained at {}",
                        run.path().display()
                    );
                    exit(1);
                }
                Some(run)
            }
            Err(err) => {
                eprintln!("error: {err}");
                exit(1);
            }
        }
    } else {
        None
    };
    let replay_scratch_path = replay_scratch.clone().unwrap_or_else(|| {
        cohort_run
            .as_ref()
            .map(|run| run.path().join("scratch"))
            .unwrap_or_else(|| dest_dir.clone())
    });
    let replay_scratch_dir = replay_scratch_path.as_path();
    if let Err(err) = fs::create_dir_all(replay_scratch_dir) {
        eprintln!(
            "error: failed to create replay scratch directory {}: {err}",
            replay_scratch_dir.display()
        );
        exit(1);
    }
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
    if replay_scratch.is_some() {
        let spans = compatibility::plan_runtime_spans(
            runtime_slot_range(start_epoch, qualification),
            allow_candidate_runtime,
        )
        .expect("requested range was preflighted above");
        if !runtime_spans_support_private_replay_scratch(
            &spans,
            env_truthy("JETSTREAMER_LOAD_FROM_DIR"),
        )
        .expect("requested range was preflighted above")
        {
            eprintln!(
                "error: --replay-scratch is incompatible with a runtime that cannot isolate mutable replay state (the Agave directory loader is not isolated)"
            );
            exit(2);
        }
    }

    // Epoch-level resume for ranges: if an earlier run already finalized some
    // leading epochs (their `.jet` has a valid footer), skip them and restart at
    // the first incomplete one. Completed epochs are immutable, so this only
    // avoids redoing them; the resumed epoch bootstraps from a boundary snapshot
    // exactly like a fresh start (the in-memory bank from the prior epoch is gone
    // after a crash). Scoped to ranges so a single-epoch re-run still regenerates.
    let effective_start = if start_epoch != end_epoch && !root_checkpoint_cohort {
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
                    if let Err(err) =
                        jetstreamer_node::archive_checksum::ensure_archive_checksum(&path)
                    {
                        eprintln!(
                            "error: failed to validate or repair checksum for reusable epoch {first_incomplete} archive {}: {err}",
                            path.display()
                        );
                        exit(1);
                    }
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
                                 missing-status={:?} status-validation={:?} execution={} output={:?}",
                                segment.slots.start,
                                segment.slots.end,
                                segment.input_metadata,
                                segment.missing_transaction_status,
                                segment.transaction_status_validation,
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

    let total_epochs = end_epoch - effective_start + 1;
    let configured_epoch_isolation =
        !root_checkpoint_cohort && env_truthy_default("JETSTREAMER_EPOCH_ISOLATION", true);
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
        if root_checkpoint_cohort {
            eprintln!(
                "error: root-checkpoint cohort cannot cross the runtime boundary in epoch {epoch}"
            );
            exit(1);
        }
        warn!(
            "JETSTREAMER_EPOCH_ISOLATION=0 cannot chain range {effective_start}-{end_epoch} in one process because epoch {epoch} crosses an execution-runtime boundary; forcing per-epoch process isolation"
        );
    }
    let adaptive_requested = match strict_opt_in("JETSTREAMER_ADAPTIVE_EPOCH_CONCURRENCY") {
        Ok(requested) => requested,
        Err(err) => {
            eprintln!("error: {err}");
            exit(2);
        }
    };
    let private_scratch_capable = range_supports_private_replay_scratch(
        effective_start,
        end_epoch,
        allow_candidate_runtime,
        env_truthy("JETSTREAMER_LOAD_FROM_DIR"),
    )
    .expect("requested range was preflighted above");
    if adaptive_requested && !private_scratch_capable {
        warn!(
            "adaptive epoch concurrency requires private mutable state; JETSTREAMER_LOAD_FROM_DIR keeps Agave state in the shared ledger, so the serial range supervisor will be used"
        );
    }
    let adaptive_epoch_concurrency =
        epoch_isolation && adaptive_requested && private_scratch_capable;

    // Replay mutates the unpacked snapshot state in place (new appendvecs,
    // accounts index), so a crashed run leaves the staging dirs dirty.
    // Start every run from a clean unpack of the retained snapshot archive;
    // set JETSTREAMER_CLEAR_ACCOUNTS_ON_START=false to skip.
    if adaptive_epoch_concurrency {
        info!("shared ledger cleanup deferred: range children use private replay scratch");
    } else if env_truthy_default("JETSTREAMER_CLEAR_ACCOUNTS_ON_START", true) {
        if let Err(err) = clear_ledger_accounts_state(replay_scratch_dir) {
            eprintln!("error: {err}");
            exit(1);
        }
    } else {
        info!("ledger accounts cleanup disabled via JETSTREAMER_CLEAR_ACCOUNTS_ON_START=false");
    }

    // Epoch 0 has no predecessor state. It starts from the canonical mainnet
    // genesis and replays slot 0 itself; every later epoch still starts from a
    // predecessor snapshot (or an in-memory bank within this process).
    let genesis_bootstrap = effective_start == 0 && qualification.is_none();
    if genesis_bootstrap && snapshot_archive_override.is_some() {
        eprintln!(
            "error: normal epoch-0 replay must bootstrap from genesis; --snapshot-archive is reserved for focused qualification"
        );
        exit(2);
    }
    let mut cohort_bootstrap_binding = None;
    let bootstrap = if genesis_bootstrap {
        // Materialize genesis through Agave's hardened bounded unpacker before
        // measuring the exact file handed to the historical client.
        if let Err(err) = ensure_genesis_archive(&dest_dir).await {
            eprintln!("error: {err}");
            exit(1);
        }
        info!("epoch 0: selecting canonical local genesis bootstrap at slot 0");
        match validate_mainnet_genesis_in(&dest_dir, replay_scratch_dir) {
            Ok(bootstrap) => bootstrap,
            Err(err) => {
                eprintln!("error: {err}");
                exit(1);
            }
        }
    } else if root_checkpoint_cohort {
        let selection = cohort_runtime.expect("cohort runtime was validated");
        let plan = cohort_plan.as_ref().expect("cohort plan was validated");
        let name = match snapshot_filename(&plan.bootstrap.uri) {
            Ok(name) => name.to_owned(),
            Err(err) => {
                eprintln!("error: {err}");
                exit(1);
            }
        };
        let (root_slot, _) = match parse_snapshot_archive_name(&name) {
            Ok(identity) => identity,
            Err(err) => {
                eprintln!("error: {err}");
                exit(1);
            }
        };
        let input_dir = cohort_run
            .as_ref()
            .expect("cohort run directory was created")
            .path()
            .join("inputs");
        if let Err(err) = create_or_validate_private_directory(&input_dir) {
            eprintln!("error: {err}");
            exit(1);
        }
        let snapshot_path = match download_exact_snapshot_generation(
            root_slot,
            &name,
            plan.bootstrap.generation,
            &input_dir,
        )
        .await
        {
            Ok(path) => path,
            Err(err) => {
                eprintln!(
                    "error: failed to download audited predecessor root generation {}: {err}",
                    plan.bootstrap.generation
                );
                exit(1);
            }
        };
        let binding = match bind_cohort_snapshot_download(&snapshot_path, &plan.bootstrap) {
            Ok(binding) => binding,
            Err(err) => {
                eprintln!("error: {err}");
                exit(1);
            }
        };
        if let Err(err) = validate_runtime_bootstrap_archive(selection.descriptor, &snapshot_path) {
            eprintln!("error: {err}");
            exit(1);
        }
        if let Err(err) = validate_epoch_bootstrap_snapshot(effective_start, &snapshot_path) {
            eprintln!("error: {err}");
            exit(1);
        }
        info!(
            "root-checkpoint cohort {}-{} bound predecessor root {} generation {} at slot {}",
            effective_start, end_epoch, name, plan.bootstrap.generation, root_slot
        );
        cohort_bootstrap_binding = Some(binding);
        ReplayBootstrap::SnapshotArchive(snapshot_path)
    } else {
        // The snapshot bootstraps only the first epoch actually run
        // (`effective_start`); later epochs in an in-process range chain off
        // the working bank.
        let bootstrap_bounds = match qualification {
            Some(plan) => SnapshotBootstrapBounds::exact(plan.bootstrap_slot, None),
            None => match normal_epoch_bootstrap_bounds(effective_start) {
                Ok(bounds) => bounds,
                Err(err) => {
                    eprintln!("error: {err}");
                    exit(1);
                }
            },
        };
        let target_slot = bootstrap_bounds.max_slot;
        let min_snapshot_slot = bootstrap_bounds.min_slot;
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
                bootstrap_bounds,
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
        let snapshot_path = match existing_snapshot {
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
                // A per-epoch child must never reach for gcloud (the
                // supervisor stages every boundary snapshot while its session
                // is fresh).
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
        if let Err(err) =
            validate_runtime_bootstrap_archive(effective_runtime.descriptor, &snapshot_path)
        {
            eprintln!("error: {err}");
            exit(1);
        }
        if qualification.is_none()
            && let Err(err) = validate_epoch_bootstrap_snapshot(effective_start, &snapshot_path)
        {
            eprintln!("error: {err}");
            exit(1);
        }

        if !env_truthy("JETSTREAMER_LOAD_FROM_DIR") {
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
            if let Err(err) = extract_tarball(&snapshot_path, &dest_dir).await {
                eprintln!("error: {err}");
                exit(1);
            }
            println!("Extraction complete");
        }
        ReplayBootstrap::SnapshotArchive(snapshot_path)
    };

    // Snapshot-based runtimes also require genesis. Preserve the existing
    // ordering: resolve and validate their snapshot before fetching genesis.
    if !genesis_bootstrap && let Err(err) = ensure_genesis_archive(&dest_dir).await {
        eprintln!("error: {err}");
        exit(1);
    }

    // Range runs default to per-epoch process isolation: each epoch replays in
    // its own child process so all replay memory (accounts-db growth, caches)
    // is released at every epoch boundary — in-process chaining was observed
    // OOM-killed at ~745 GiB RSS two epochs into a range. Isolation needs every
    // epoch's boundary snapshot on disk, so download them all now while the
    // gcloud/GCS session is fresh (a range runs for days; mid-run gcloud access
    // is forbidden). `bootstrap` above already covers `effective_start`.
    let mut boundary_bootstraps: BTreeMap<u64, ReplayBootstrap> = BTreeMap::new();
    if epoch_isolation {
        boundary_bootstraps.insert(effective_start, bootstrap.clone());
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
                    boundary_bootstraps.insert(epoch, ReplayBootstrap::SnapshotArchive(path));
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
        if root_checkpoint_cohort {
            let selection = cohort_runtime.expect("cohort runtime was validated");
            let plan = cohort_plan.as_ref().expect("cohort plan was validated");
            let bootstrap_hash = plan
                .bootstrap
                .accounts_hash
                .parse::<Hash>()
                .expect("cohort manifest bootstrap hash was validated");
            let bootstrap_slot = plan.bootstrap.slot;
            let bootstrap_expectation = snapshot_hash_expectation(
                SnapshotHash(bootstrap_hash),
                selection.descriptor.bootstrap.snapshot_hash_kind,
            );
            let mut expected = BTreeMap::from([(bootstrap_slot, bootstrap_expectation)]);
            for checkpoint in &plan.root_checkpoints {
                let hash = checkpoint
                    .accounts_hash
                    .parse::<Hash>()
                    .expect("cohort manifest checkpoint hash was validated");
                if expected
                    .insert(
                        checkpoint.slot,
                        snapshot_hash_expectation(
                            SnapshotHash(hash),
                            selection.descriptor.bootstrap.snapshot_hash_kind,
                        ),
                    )
                    .is_some()
                {
                    eprintln!(
                        "error: cohort manifest repeats root checkpoint slot {}",
                        checkpoint.slot
                    );
                    exit(1);
                }
            }
            if let Err(err) = validate_root_checkpoint_cohort_expectations(
                effective_start,
                end_epoch,
                bootstrap_slot,
                bootstrap_expectation,
                &expected,
            ) {
                eprintln!("error: {err}");
                exit(1);
            }
            info!(
                "root-checkpoint cohort verification bound its bootstrap and {} root checkpoint(s) through epoch {}",
                plan.root_checkpoints.len(),
                end_epoch
            );
            snapshot_expectations.insert(effective_start, expected);
        } else if let Some(hashes_path) = &epoch_hashes {
            // Per-epoch child: the supervisor prefetched this epoch's hashes to
            // a file, so the child stays gcloud-free.
            match read_epoch_hashes_file(hashes_path, effective_runtime.descriptor.bootstrap) {
                Ok(mut expected) => {
                    if let Err(err) = add_runtime_handoff_expectations(
                        runtime_slot_range(effective_start, qualification),
                        &mut expected,
                    ) {
                        eprintln!("error: {err}");
                        exit(1);
                    }
                    let expected = if let Some(plan) = qualification {
                        let snapshot_path = bootstrap.snapshot_archive().expect(
                            "qualification validation requires an explicit snapshot archive",
                        );
                        match qualification_expectations(
                            expected,
                            plan,
                            snapshot_path,
                            effective_runtime.descriptor.bootstrap,
                        ) {
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
                let bootstrap_state = selection.descriptor.bootstrap;
                let boundary_path = boundary_bootstraps
                    .get(&epoch)
                    .or_else(|| (epoch == effective_start).then_some(&bootstrap))
                    .and_then(ReplayBootstrap::snapshot_archive);
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
                if adaptive_epoch_concurrency {
                    let cached =
                        match cached_private_epoch_hashes(&dest_dir, epoch, bootstrap_state) {
                            Ok(cached) => cached,
                            Err(err) => {
                                eprintln!("error: {err}");
                                exit(1);
                            }
                        };
                    if let Some(mut expected) = cached {
                        if expected.is_empty()
                            || expected
                                .keys()
                                .any(|slot| !(verification_start..=epoch_end_slot).contains(slot))
                        {
                            eprintln!(
                                "error: cached epoch {epoch} checkpoint set is empty or outside {}..={epoch_end_slot}",
                                verification_start
                            );
                            exit(1);
                        }
                        if let Some(boundary_path) = boundary_path {
                            let (boundary_slot, boundary_expectation) =
                                match snapshot_path_expectation(boundary_path, bootstrap_state) {
                                    Ok(expectation) => expectation,
                                    Err(err) => {
                                        eprintln!("error: {err}");
                                        exit(1);
                                    }
                                };
                            if expected.get(&boundary_slot) != Some(&boundary_expectation) {
                                eprintln!(
                                    "error: cached epoch {epoch} checkpoint set does not bind boundary snapshot {}",
                                    boundary_path.display()
                                );
                                exit(1);
                            }
                        }
                        let before_handoffs = expected.clone();
                        if let Err(err) = add_runtime_handoff_expectations(
                            epoch_start_slot..epoch_end_slot.saturating_add(1),
                            &mut expected,
                        ) {
                            eprintln!("error: {err}");
                            exit(1);
                        }
                        if expected != before_handoffs {
                            eprintln!(
                                "error: cached epoch {epoch} checkpoint set omits a registry-committed runtime handoff"
                            );
                            exit(1);
                        }
                        let post_bootstrap_start = verification_start.saturating_add(1);
                        if expected
                            .range(post_bootstrap_start..=epoch_end_slot)
                            .next()
                            .is_none()
                        {
                            eprintln!(
                                "error: cached epoch {epoch} checkpoint set has no post-bootstrap checkpoint in {post_bootstrap_start}..={epoch_end_slot}"
                            );
                            exit(1);
                        }
                        info!(
                            "snapshot verification: reusing {} immutable private checkpoint(s) for epoch {epoch}; no GCS refresh required",
                            expected.len()
                        );
                        snapshot_expectations.insert(epoch, expected);
                        continue;
                    }
                }
                let mut expected = match snapshot_expectations_for_span(
                    verification_start,
                    epoch_end_slot,
                    bootstrap_state,
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
                    && let Err(err) = add_boundary_snapshot_expectation(
                        &mut expected,
                        boundary_path,
                        bootstrap_state,
                    )
                {
                    eprintln!("error: {err}");
                    exit(1);
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
        let Some(snapshot_path) = bootstrap.snapshot_archive() else {
            eprintln!("error: multi-runtime epoch assembly cannot bootstrap from genesis");
            exit(1);
        };
        if let Err(err) = run_multi_runtime_epoch_supervisor(
            effective_start,
            &dest_dir,
            replay_scratch.as_deref(),
            snapshot_path,
            &hashes_path,
            &final_output,
            allow_candidate_runtime,
            range_info.is_none(),
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
        if !adaptive_epoch_concurrency {
            for (epoch, expected) in &snapshot_expectations {
                let path = dest_dir.join(format!("epoch-hashes-{epoch}.txt"));
                if let Err(err) = write_epoch_hashes_file(&path, expected) {
                    eprintln!("error: {err}");
                    exit(1);
                }
            }
        }
        if let Err(err) = run_epoch_range_supervisor(
            effective_start,
            end_epoch,
            &dest_dir,
            verify_snapshots,
            allow_candidate_runtime,
            adaptive_epoch_concurrency,
            shutdown.clone(),
            boundary_bootstraps,
            snapshot_expectations,
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
    let cohort_verifier = if root_checkpoint_cohort {
        let expected = snapshot_expectations
            .remove(&effective_start)
            .unwrap_or_default();
        Some(Arc::new(SnapshotVerifier::new(
            expected,
            Some(shutdown.clone()),
        )))
    } else {
        None
    };
    let cohort_trusted_bootstrap = if root_checkpoint_cohort {
        let snapshot_path = bootstrap
            .snapshot_archive()
            .expect("root-checkpoint cohort has a snapshot bootstrap");
        let name = snapshot_path
            .file_name()
            .and_then(|name| name.to_str())
            .expect("validated snapshot path has a UTF-8 filename");
        Some(
            parse_snapshot_archive_name(name)
                .expect("validated root-checkpoint bootstrap has a snapshot identity"),
        )
    } else {
        None
    };
    let mut completed_cohort = Vec::new();
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
        if let Err(err) = destination_binding.revalidate() {
            eprintln!("error: {err}");
            exit(1);
        }
        let final_output = dest_dir.join(format!("epoch-{epoch}.jet"));
        let horizon_output = if root_checkpoint_cohort {
            cohort_run
                .as_ref()
                .expect("cohort run directory was created")
                .archives_path()
                .join(format!("epoch-{epoch}.jet"))
        } else {
            horizon_output_override
                .clone()
                .unwrap_or_else(|| final_output.clone())
        };

        let snapshot_verifier = if let Some(verifier) = cohort_verifier.as_ref() {
            Some(verifier.clone())
        } else if verify_snapshots {
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
            replay_scratch_dir,
            &bootstrap,
            shutdown.clone(),
            cursor.clone(),
            restart_tracker.clone(),
            snapshot_verifier,
            horizon_output.clone(),
            qualification,
            carried_state.take(),
            range_progress.clone(),
            Some(shared_progress.clone()),
            if epoch == effective_start {
                cohort_bootstrap_binding.as_ref()
            } else {
                None
            },
            root_checkpoint_cohort.then(|| epoch_to_slot_range(end_epoch).1),
            epoch < end_epoch,
        )
        .await;
        match result {
            Ok(result) => {
                let finalization_route =
                    archive_finalization_route(root_checkpoint_cohort, qualification, range_info);
                if finalization_route == ArchiveFinalizationRoute::RootCheckpointCohort {
                    let historical_evidence = match result.historical_evidence.clone() {
                        Some(evidence) => evidence,
                        None => {
                            eprintln!(
                                "error: root-checkpoint cohort epoch {epoch} produced no historical checkpoint evidence"
                            );
                            exit(1);
                        }
                    };
                    let selection = cohort_runtime.expect("cohort runtime was validated");
                    let staged_file =
                        match jetstreamer_node::archive_checksum::open_regular_nofollow(
                            &horizon_output,
                        ) {
                            Ok(file) => file,
                            Err(err) => {
                                eprintln!(
                                    "error: failed to open staged cohort archive {}: {err}",
                                    horizon_output.display()
                                );
                                exit(1);
                            }
                        };
                    if let Err(err) =
                        jetstreamer_node::archive_checksum::prepare_archive_permissions(
                            &staged_file,
                            &dest_dir,
                        )
                        .and_then(|()| staged_file.sync_all())
                    {
                        eprintln!(
                            "error: failed to prepare staged cohort archive {}: {err}",
                            horizon_output.display()
                        );
                        exit(1);
                    }
                    drop(staged_file);
                    let mut archive_chain = ArchiveChainEvidence::default();
                    let validated = match validated_epoch_archive(
                        &horizon_output,
                        epoch,
                        selection,
                        Some(&shutdown),
                        Some(&mut archive_chain),
                    ) {
                        Ok(Some(validated)) => validated,
                        Ok(None) => {
                            eprintln!(
                                "error: staged cohort archive {} is incomplete",
                                horizon_output.display()
                            );
                            exit(1);
                        }
                        Err(err) => {
                            eprintln!("error: {err}");
                            exit(1);
                        }
                    };
                    let completed = CompletedCohortEpoch {
                        epoch,
                        staged_output: horizon_output.clone(),
                        final_output,
                        historical_evidence,
                        archive_chain,
                        validated,
                    };
                    if let Err(err) = validate_cohort_archive_evidence_binding(
                        &completed,
                        epoch == effective_start,
                    ) {
                        eprintln!("error: {err}");
                        exit(1);
                    }
                    completed_cohort.push(completed);
                    let (trusted_slot, trusted_hash) =
                        cohort_trusted_bootstrap.expect("cohort bootstrap was parsed");
                    if let Err(err) = validate_historical_cohort_evidence(
                        trusted_slot,
                        trusted_hash.0,
                        completed_cohort.iter().map(|completed| {
                            (
                                completed.epoch,
                                &completed.historical_evidence,
                                completed.archive_chain,
                            )
                        }),
                    ) {
                        eprintln!("error: {err}");
                        exit(1);
                    }
                    info!(
                        "root-checkpoint cohort retained validated epoch {epoch} privately; publication remains closed"
                    );
                } else if let Some(plan) = qualification
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
                if finalization_route == ArchiveFinalizationRoute::OrdinaryTopLevel {
                    let (slot_start, slot_end_inclusive) = epoch_to_slot_range(epoch);
                    let spans = compatibility::plan_runtime_spans(
                        slot_start..slot_end_inclusive.saturating_add(1),
                        allow_candidate_runtime,
                    )
                    .expect("requested range was preflighted above");
                    let reusable = if spans.len() == 1 {
                        let selection = runtime_span_selection(&spans[0])
                            .expect("requested range was preflighted above");
                        epoch_archive_reusable(&horizon_output, epoch, selection)
                    } else {
                        epoch_archive_reusable_multi_runtime(&horizon_output, epoch, &spans)
                    };
                    match reusable {
                        Ok(true) => {}
                        Ok(false) => {
                            eprintln!(
                                "error: completed epoch {epoch} archive {} did not pass final registry validation",
                                horizon_output.display()
                            );
                            exit(1);
                        }
                        Err(err) => {
                            eprintln!("error: {err}");
                            exit(1);
                        }
                    }
                    if let Err(err) =
                        jetstreamer_node::archive_checksum::ensure_archive_checksum(&horizon_output)
                    {
                        eprintln!(
                            "error: failed to publish checksum for verified epoch {epoch} archive {}: {err}",
                            horizon_output.display()
                        );
                        exit(1);
                    }
                }
                carried_state = result.carried_state;
            }
            Err(err) => {
                eprintln!("error: {err}");
                exit(1);
            }
        }
    }
    if root_checkpoint_cohort {
        if let Err(err) = destination_binding.revalidate() {
            eprintln!("error: {err}; publication remains closed");
            exit(1);
        }
        let (trusted_slot, trusted_hash) =
            cohort_trusted_bootstrap.expect("cohort bootstrap was parsed");
        if let Err(err) = validate_historical_cohort_evidence(
            trusted_slot,
            trusted_hash.0,
            completed_cohort.iter().map(|completed| {
                (
                    completed.epoch,
                    &completed.historical_evidence,
                    completed.archive_chain,
                )
            }),
        ) {
            eprintln!("error: {err}");
            exit(1);
        }
        let terminal_verification = cohort_verifier
            .as_ref()
            .expect("cohort verifier was created")
            .finish();
        let manifest_fingerprint = match manifest_fingerprint_digest(
            &cohort_plan
                .as_ref()
                .expect("cohort plan was validated")
                .fingerprint,
        ) {
            Ok(fingerprint) => fingerprint,
            Err(err) => {
                eprintln!("error: {err}; publication remains closed");
                exit(1);
            }
        };
        let receipt_directory = batch_receipt_directory
            .as_deref()
            .expect("top-level cohort has a private receipt directory");
        let expected_epochs = (effective_start..=end_epoch).collect::<Vec<_>>();
        let publication = after_root_cohort_publication_gate(
            completed_cohort.len(),
            total_epochs as usize,
            shutdown.load(Ordering::SeqCst),
            terminal_verification,
            || {
                info!(
                    "root-checkpoint cohort {}-{} passed its terminal root and all archive checks; opening publication gate",
                    effective_start, end_epoch
                );
                publish_completed_root_cohort_transactionally(
                    &completed_cohort,
                    &expected_epochs,
                    manifest_fingerprint,
                    &destination_binding,
                    receipt_directory,
                )
            },
        );
        let publication = match publication {
            Ok(publication) => publication,
            Err(err) => {
                eprintln!(
                    "error: {err}; private run retained at {}",
                    cohort_run
                        .as_ref()
                        .expect("cohort run directory was created")
                        .path()
                        .display()
                );
                exit(1);
            }
        };
        if let Err(err) = cohort_run
            .take()
            .expect("cohort run directory was created")
            .cleanup_after_commit()
        {
            eprintln!("error: {err}");
            exit(1);
        }
        info!(
            "root-checkpoint cohort {}-{} published {} archive(s) with checksums in transaction {}; receipt {} is durable",
            effective_start,
            end_epoch,
            publication.archive_count,
            jetstreamer_node::segment_manifest::sha256_hex_string(&publication.transaction_id),
            publication.receipt_path.display()
        );
    }
}

#[cfg(test)]
mod early_snapshot_tests {
    use super::*;

    fn historical_checkpoint(
        slot: Slot,
        accounts_hash: Hash,
        last_blockhash: Hash,
        next_write_version: u64,
    ) -> historical_replay::HistoricalCheckpointSummary {
        historical_replay::HistoricalCheckpointSummary {
            slot,
            bank_hash: Hash::new_unique().to_bytes(),
            accounts_hash: accounts_hash.to_bytes(),
            last_blockhash: last_blockhash.to_bytes(),
            capitalization: 1,
            transaction_count: 2,
            tick_height: 3,
            slot_complete: true,
            write_count: 0,
            next_write_version,
        }
    }

    fn archive_chain_for(
        evidence: &historical_replay::HistoricalReplayEvidence,
    ) -> ArchiveChainEvidence {
        let terminal = ArchiveBlockEvidence {
            slot: evidence.terminal.slot,
            parent_slot: evidence.bootstrap.slot,
            parent_blockhash: Hash::new_from_array(evidence.bootstrap.last_blockhash),
            blockhash: Hash::new_from_array(evidence.terminal.last_blockhash),
        };
        ArchiveChainEvidence {
            initial_poh_anchor: Some(Hash::new_from_array(evidence.bootstrap.last_blockhash)),
            first_block: Some(terminal),
            terminal_block: Some(terminal),
        }
    }

    #[test]
    fn epochs_17_through_19_form_one_carryable_runtime_cohort() {
        let selection = root_checkpoint_cohort_runtime(17, 19, true).unwrap();
        assert_eq!(
            selection.backend,
            compatibility::RuntimeBackend::SolanaV1_0_23
        );
        assert!(selection.descriptor.permits_live_epoch_handoff());

        let error = root_checkpoint_cohort_runtime(11, 12, true).unwrap_err();
        assert!(error.contains("changes runtime at epoch 12"), "{error}");

        let error = root_checkpoint_cohort_runtime(954, 955, false).unwrap_err();
        assert!(
            error.contains("isolated historical Solana worker"),
            "{error}"
        );

        let singleton = root_checkpoint_cohort_runtime(20, 20, true).unwrap();
        assert_eq!(
            singleton.backend,
            compatibility::RuntimeBackend::SolanaV1_0_23
        );
        let error = root_checkpoint_cohort_runtime(0, 0, true).unwrap_err();
        assert!(error.contains("one or more nonzero epochs"), "{error}");
    }

    fn cohort_manifest_object(
        slot: Slot,
        hash: Hash,
        generation: u64,
        bytes: &[u8],
    ) -> serde_json::Value {
        let name = format!("snapshot-{slot}-{hash}.tar.bz2");
        let uri = format!("{DEFAULT_BUCKET}/{slot}/{name}");
        let crc = Crc::<u32>::new(&CRC_32_ISCSI).checksum(bytes);
        serde_json::json!({
            "accounts_hash": hash.to_string(),
            "anchor_slot": slot,
            "crc32c": BASE64_STANDARD.encode(crc.to_be_bytes()),
            "extension": ".tar.bz2",
            "generation": generation,
            "size": bytes.len(),
            "slot": slot,
            "source": "root",
            "uri": uri,
            "versioned_uri": format!("{uri}#{generation}")
        })
    }

    fn cohort_manifest_report(bytes: &[u8]) -> (serde_json::Value, String) {
        let bootstrap_hash = Hash::new_from_array([0x11; 32]);
        let final_hash = Hash::new_from_array([0x22; 32]);
        let manifest = serde_json::json!({
            "bucket": DEFAULT_BUCKET,
            "epoch_slots": 432_000,
            "first_epoch": 1,
            "last_epoch": 100,
            "schema": COHORT_MANIFEST_SCHEMA,
            "verification_cohorts": [{
                "accepted_extensions": [".tar.bz2"],
                "bootstrap": cohort_manifest_object(7_343_776, bootstrap_hash, 101, bytes),
                "first_epoch": 17,
                "last_epoch": 19,
                "publication_gate": COHORT_PUBLICATION_GATE,
                "root_checkpoints": [cohort_manifest_object(
                    8_213_950,
                    final_hash,
                    102,
                    b"checkpoint metadata is not downloaded"
                )],
                "runtime": "solana-v1.0.23"
            }]
        });
        let fingerprint = cohort_manifest_fingerprint(&manifest).unwrap();
        (
            serde_json::json!({
                "manifest": manifest,
                "manifest_fingerprint": fingerprint
            }),
            fingerprint,
        )
    }

    #[test]
    fn sealed_cohort_manifest_binds_runtime_roots_and_fingerprint() {
        let bytes = b"audited generation one";
        let (report, fingerprint) = cohort_manifest_report(bytes);
        let selection = root_checkpoint_cohort_runtime(17, 19, true).unwrap();
        let plan = root_checkpoint_cohort_plan_from_report(
            report.clone(),
            &fingerprint,
            17,
            19,
            selection,
        )
        .unwrap();
        assert_eq!(plan.bootstrap.slot, 7_343_776);
        assert_eq!(plan.bootstrap.generation, 101);
        assert_eq!(plan.root_checkpoints.len(), 1);

        let mut changed = report;
        changed["manifest"]["verification_cohorts"][0]["bootstrap"]["generation"] =
            serde_json::json!(202);
        let error =
            root_checkpoint_cohort_plan_from_report(changed, &fingerprint, 17, 19, selection)
                .unwrap_err();
        assert!(error.contains("fingerprint mismatch"), "{error}");
    }

    #[test]
    fn sealed_manifest_supports_a_single_epoch_root_cohort() {
        let bytes = b"audited singleton generation";
        let bootstrap_hash = Hash::new_from_array([0x33; 32]);
        let final_hash = Hash::new_from_array([0x44; 32]);
        let manifest = serde_json::json!({
            "bucket": DEFAULT_BUCKET,
            "epoch_slots": 432_000,
            "first_epoch": 1,
            "last_epoch": 100,
            "schema": COHORT_MANIFEST_SCHEMA,
            "verification_cohorts": [{
                "accepted_extensions": [".tar.bz2"],
                "bootstrap": cohort_manifest_object(
                    8_639_740,
                    bootstrap_hash,
                    201,
                    bytes,
                ),
                "first_epoch": 20,
                "last_epoch": 20,
                "publication_gate": COHORT_PUBLICATION_GATE,
                "root_checkpoints": [cohort_manifest_object(
                    8_900_000,
                    final_hash,
                    202,
                    b"singleton checkpoint metadata is not downloaded",
                )],
                "runtime": "solana-v1.0.23",
            }]
        });
        let fingerprint = cohort_manifest_fingerprint(&manifest).unwrap();
        let report = serde_json::json!({
            "manifest": manifest,
            "manifest_fingerprint": fingerprint,
        });
        let plan = root_checkpoint_cohort_plan_from_report(
            report,
            &fingerprint,
            20,
            20,
            root_checkpoint_cohort_runtime(20, 20, true).unwrap(),
        )
        .unwrap();
        assert_eq!(plan.bootstrap.slot, 8_639_740);
        assert_eq!(plan.root_checkpoints.len(), 1);
        assert_eq!(plan.root_checkpoints[0].slot, 8_900_000);
    }

    #[test]
    fn cohort_manifest_fingerprint_matches_python_canonical_json() {
        let manifest = serde_json::json!({
            "z": [3, true, null],
            "a": {"x": "ASCII", "n": 17}
        });
        let expected = "sha256:07d38896ccb84d2b0e52fffb78e70f293ded9e3988277f9332ab96469323c435";
        assert_eq!(cohort_manifest_fingerprint(&manifest).unwrap(), expected);
        assert_eq!(
            jetstreamer_node::segment_manifest::sha256_hex_string(
                &manifest_fingerprint_digest(expected).unwrap()
            ),
            expected.strip_prefix("sha256:").unwrap()
        );
    }

    #[test]
    fn downloaded_cohort_bootstrap_rejects_g1_to_g2_path_replacement() {
        let generation_one = b"audited generation one";
        let generation_two = b"replacement version 2!";
        assert_eq!(generation_one.len(), generation_two.len());
        let (report, fingerprint) = cohort_manifest_report(generation_one);
        let plan = root_checkpoint_cohort_plan_from_report(
            report,
            &fingerprint,
            17,
            19,
            root_checkpoint_cohort_runtime(17, 19, true).unwrap(),
        )
        .unwrap();
        let directory = tempfile::tempdir().unwrap();
        let path = directory
            .path()
            .join(snapshot_filename(&plan.bootstrap.uri).unwrap());
        fs::write(&path, generation_one).unwrap();
        let binding = bind_cohort_snapshot_download(&path, &plan.bootstrap).unwrap();
        binding.revalidate().unwrap();

        fs::remove_file(&path).unwrap();
        fs::write(&path, generation_two).unwrap();
        let error = binding.revalidate().unwrap_err();
        assert!(error.contains("changed after it was bound"), "{error}");

        let error = bind_cohort_snapshot_download(&path, &plan.bootstrap).unwrap_err();
        assert!(error.contains("CRC32C mismatch"), "{error}");
    }

    #[test]
    fn root_cohort_finalization_has_one_private_validation_path() {
        let route = archive_finalization_route(true, None, None);
        let mut validation_passes = 0;
        let mut immediate_checksum_publications = 0;
        match route {
            ArchiveFinalizationRoute::RootCheckpointCohort => validation_passes += 1,
            ArchiveFinalizationRoute::OrdinaryTopLevel => {
                validation_passes += 1;
                immediate_checksum_publications += 1;
            }
            ArchiveFinalizationRoute::InternalOrQualification => {}
        }
        assert_eq!(validation_passes, 1);
        assert_eq!(immediate_checksum_publications, 0);
    }

    #[test]
    fn root_cohort_publication_requires_exact_ordered_epoch_membership() {
        assert!(validate_root_cohort_epoch_membership(&[20], &[20]).is_ok());
        assert!(validate_root_cohort_epoch_membership(&[17, 18, 19], &[17, 18, 19]).is_ok());
        for actual in [&[17, 19][..], &[17, 19, 18], &[17, 18, 19, 19]] {
            let error = validate_root_cohort_epoch_membership(actual, &[17, 18, 19]).unwrap_err();
            assert!(error.contains("does not match"), "{error}");
        }
        let error = validate_root_cohort_epoch_membership(&[], &[]).unwrap_err();
        assert!(error.contains("empty or noncontiguous"), "{error}");
        let error = validate_root_cohort_epoch_membership(&[17, 19], &[17, 19]).unwrap_err();
        assert!(error.contains("empty or noncontiguous"), "{error}");
        let error =
            validate_root_cohort_epoch_membership(&[u64::MAX, u64::MAX], &[u64::MAX, u64::MAX])
                .unwrap_err();
        assert!(error.contains("empty or noncontiguous"), "{error}");
    }

    #[test]
    fn archive_reuse_refuses_destination_wide_batch_state() {
        let directory = tempfile::tempdir().unwrap();
        fs::create_dir(
            directory
                .path()
                .join(jetstreamer_node::archive_checksum::ARCHIVE_BATCH_OUTCOME_DIRECTORY),
        )
        .unwrap();
        let archive = directory.path().join("epoch-17.jet");
        let error = epoch_archive_reusable(
            &archive,
            17,
            root_checkpoint_cohort_runtime(17, 19, true).unwrap(),
        )
        .unwrap_err();
        assert!(error.contains("unacknowledged archive batch"), "{error}");

        let error = epoch_archive_reusable_multi_runtime(&archive, 17, &[]).unwrap_err();
        assert!(error.contains("unacknowledged archive batch"), "{error}");
    }

    #[test]
    fn interruption_and_root_mismatch_never_open_the_publication_gate() {
        let calls = AtomicUsize::new(0);
        let directory = tempfile::tempdir().unwrap();
        let archive = directory.path().join("epoch-17.jet");
        fs::write(&archive, b"privately staged archive").unwrap();
        let checksum = jetstreamer_node::archive_checksum::archive_checksum_path(&archive).unwrap();
        let interrupted = after_root_cohort_publication_gate(3, 3, true, Ok(()), || {
            calls.fetch_add(1, Ordering::SeqCst);
            fs::write(&checksum, b"must remain unreachable").unwrap();
            Ok(())
        })
        .unwrap_err();
        assert!(interrupted.contains("interrupted"));
        let mismatch = after_root_cohort_publication_gate(
            3,
            3,
            false,
            Err("accounts hash mismatch".to_string()),
            || {
                calls.fetch_add(1, Ordering::SeqCst);
                fs::write(&checksum, b"must remain unreachable").unwrap();
                Ok(())
            },
        )
        .unwrap_err();
        assert!(mismatch.contains("accounts hash mismatch"));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert!(!checksum.exists());
        assert!(
            !directory
                .path()
                .join(jetstreamer_node::archive_checksum::ARCHIVE_BATCH_TRANSACTION_DIRECTORY)
                .exists()
        );
        assert!(
            !directory
                .path()
                .join(jetstreamer_node::archive_checksum::ARCHIVE_BATCH_OUTCOME_DIRECTORY)
                .exists()
        );
    }

    #[test]
    fn cohort_expectations_require_the_bootstrap_root_and_a_final_epoch_root() {
        let bootstrap_hash = Hash::new_unique();
        let bootstrap = BankHashExpectation::LegacyAccountsHash(bootstrap_hash);
        let final_root_hash = Hash::new_unique();
        let mut expected = BTreeMap::from([
            (7_343_776, bootstrap),
            (
                8_213_950,
                BankHashExpectation::LegacyAccountsHash(final_root_hash),
            ),
        ]);
        validate_root_checkpoint_cohort_expectations(17, 19, 7_343_776, bootstrap, &expected)
            .unwrap();

        expected.remove(&8_213_950);
        let error =
            validate_root_checkpoint_cohort_expectations(17, 19, 7_343_776, bootstrap, &expected)
                .unwrap_err();
        assert!(error.contains("no trusted root checkpoint in its final epoch"));
    }

    #[test]
    fn cohort_evidence_requires_exact_cross_epoch_state_handoffs() {
        let trusted_hash = Hash::new_unique();
        let root = historical_checkpoint(7_343_776, trusted_hash, Hash::new_unique(), 10);
        let end_17 = historical_checkpoint(
            epoch_to_slot_range(17).1,
            Hash::new_unique(),
            Hash::new_unique(),
            20,
        );
        let end_18 = historical_checkpoint(
            epoch_to_slot_range(18).1,
            Hash::new_unique(),
            Hash::new_unique(),
            30,
        );
        let end_19 = historical_checkpoint(
            epoch_to_slot_range(19).1,
            Hash::new_unique(),
            Hash::new_unique(),
            40,
        );
        let evidence = vec![
            (
                17,
                historical_replay::HistoricalReplayEvidence {
                    bootstrap: root,
                    terminal: end_17.clone(),
                    emitted_write_versions: 10..20,
                },
            ),
            (
                18,
                historical_replay::HistoricalReplayEvidence {
                    bootstrap: end_17,
                    terminal: end_18.clone(),
                    emitted_write_versions: 20..30,
                },
            ),
            (
                19,
                historical_replay::HistoricalReplayEvidence {
                    bootstrap: end_18,
                    terminal: end_19,
                    emitted_write_versions: 30..40,
                },
            ),
        ];
        validate_historical_cohort_evidence(
            7_343_776,
            trusted_hash,
            evidence
                .iter()
                .map(|(epoch, evidence)| (*epoch, evidence, archive_chain_for(evidence))),
        )
        .unwrap();

        let mut broken = evidence.clone();
        broken[1].1.bootstrap.bank_hash = Hash::new_unique().to_bytes();
        let error = validate_historical_cohort_evidence(
            7_343_776,
            trusted_hash,
            broken
                .iter()
                .map(|(epoch, evidence)| (*epoch, evidence, archive_chain_for(evidence))),
        )
        .unwrap_err();
        assert!(error.contains("changed checkpoint evidence"), "{error}");
    }

    #[test]
    fn cohort_terminal_evidence_tracks_the_last_present_block_before_trailing_skips() {
        let trusted_hash = Hash::new_unique();
        let root = historical_checkpoint(7_343_776, trusted_hash, Hash::new_unique(), 10);
        let epoch = 18;
        let epoch_end = epoch_to_slot_range(epoch).1;
        let terminal =
            historical_checkpoint(epoch_end - 1, Hash::new_unique(), Hash::new_unique(), 20);
        let evidence = historical_replay::HistoricalReplayEvidence {
            bootstrap: root,
            terminal,
            emitted_write_versions: 10..20,
        };
        validate_historical_cohort_evidence(
            7_343_776,
            trusted_hash,
            [(epoch, &evidence, archive_chain_for(&evidence))],
        )
        .unwrap();
        validate_carried_archive_anchor(epoch, &evidence.bootstrap, archive_chain_for(&evidence))
            .unwrap();

        let mut wrong_chain = archive_chain_for(&evidence);
        wrong_chain.terminal_block.as_mut().unwrap().slot = epoch_end;
        let error = validate_historical_cohort_evidence(
            7_343_776,
            trusted_hash,
            [(epoch, &evidence, wrong_chain)],
        )
        .unwrap_err();
        assert!(
            error.contains("does not match archive terminal block"),
            "{error}"
        );

        let mut wrong_parent = archive_chain_for(&evidence);
        wrong_parent.first_block.as_mut().unwrap().parent_slot += 1;
        let error =
            validate_carried_archive_anchor(epoch, &evidence.bootstrap, wrong_parent).unwrap_err();
        assert!(error.contains("expected carried checkpoint"), "{error}");
    }

    #[test]
    fn ready_entry_teardown_unblocks_and_joins_consumer() {
        let (sender, receiver) = crossbeam_channel::bounded::<()>(1);
        let producers = ReadyEntryProducers {
            transaction: sender.clone(),
            entry: sender.clone(),
            block_parent: sender.clone(),
            block_metadata: sender.clone(),
        };
        let notifier_sender = sender.clone();
        let consumer = std::thread::spawn(move || {
            assert_eq!(receiver.recv(), Err(crossbeam_channel::RecvError));
        });
        let (done_sender, done_receiver) = crossbeam_channel::bounded(1);
        std::thread::spawn(move || {
            close_ready_entry_channel(notifier_sender, producers, sender, consumer)
                .expect("consumer exits cleanly");
            done_sender.send(()).expect("test remains alive");
        });
        done_receiver
            .recv_timeout(Duration::from_secs(1))
            .expect("ready-entry teardown must not deadlock");
    }

    #[test]
    fn prior_runtime_generation_profile_allowlists_are_exact_and_scoped() {
        let prior_v1_0_7 = COMPATIBLE_V1_0_7_GENERATION_PROFILES[0];
        let prior_v1_0_8 = COMPATIBLE_V1_0_8_GENERATION_PROFILES[0];
        for (runtime, prior) in [
            (historical::SOLANA_V1_0_7_CANDIDATE.backend_id, prior_v1_0_7),
            (historical::SOLANA_V1_0_8_CANDIDATE.backend_id, prior_v1_0_8),
        ] {
            assert!(runtime_generation_profile_is_compatible(runtime, prior));

            let mut one_character_mutation = prior.as_bytes().to_vec();
            *one_character_mutation.last_mut().unwrap() =
                if prior.ends_with('0') { b'1' } else { b'0' };
            let one_character_mutation = String::from_utf8(one_character_mutation).unwrap();
            assert!(!runtime_generation_profile_is_compatible(
                runtime,
                &one_character_mutation,
            ));
            assert!(!runtime_generation_profile_is_compatible(
                runtime,
                &format!("{prior}-dirty-deadbeef"),
            ));
        }
        assert!(runtime_generation_profile_is_compatible(
            historical::SOLANA_V1_0_8_CANDIDATE.backend_id,
            &archive_generation_profile(),
        ));
        assert!(!runtime_generation_profile_is_compatible(
            historical::SOLANA_V1_0_7_CANDIDATE.backend_id,
            "unknown",
        ));
        assert!(!runtime_generation_profile_is_compatible(
            historical::SOLANA_V1_0_8_CANDIDATE.backend_id,
            prior_v1_0_7,
        ));
        assert!(!runtime_generation_profile_is_compatible(
            historical::SOLANA_V1_0_7_CANDIDATE.backend_id,
            prior_v1_0_8,
        ));
        assert!(!runtime_generation_profile_is_compatible(
            historical::SOLANA_V1_0_14_CANDIDATE.backend_id,
            AUDITED_EPOCH_11_RECOVERY_PROFILE,
        ));
    }

    fn audited_archive_recovery_provenance(recovery: &AuditedArchiveRecovery) -> ArchiveProvenance {
        let output_start = epoch_to_slot_range(recovery.epoch).0;
        let output_end = epoch_to_slot_range(recovery.epoch).1 + 1;
        let selection = compatibility::select_runtime(output_start..output_end, true).unwrap();
        assert!(std::ptr::eq(
            selection.descriptor,
            recovery.runtime_descriptor
        ));
        let mut provenance = build_archive_provenance(
            selection,
            Some(recovery.worker_sha256),
            BootstrapStateKind::SnapshotArchive,
            recovery.bootstrap_slot,
            recovery.bootstrap_hash.parse().unwrap(),
            output_start,
            output_end - output_start,
        )
        .unwrap();
        let ArchiveProvenance::V2(provenance_v2) = &mut provenance else {
            panic!("historical worker provenance must be V2");
        };
        provenance_v2.base.generation_profile = recovery.generation_profile.to_owned();
        provenance
    }

    fn assert_audited_archive_provenance_mutation_rejected(
        recovery: &AuditedArchiveRecovery,
        provenance: &ArchiveProvenance,
        mutation: impl FnOnce(&mut ArchiveProvenanceV2),
    ) {
        let mut changed = provenance.clone();
        let ArchiveProvenance::V2(changed_v2) = &mut changed else {
            panic!("audited archive fixture must be V2");
        };
        mutation(changed_v2);
        assert!(
            audited_archive_recovery_provenance_matches(recovery.epoch, &changed).is_none(),
            "mutated epoch-{} provenance was admitted",
            recovery.epoch,
        );
    }

    #[test]
    fn audited_archive_recovery_is_exact_and_not_general_compatibility() {
        assert!(
            AUDITED_ARCHIVE_RECOVERIES
                .windows(2)
                .all(|pair| pair[0].epoch < pair[1].epoch),
            "audited recovery epochs must be unique and sorted",
        );
        for recovery in AUDITED_ARCHIVE_RECOVERIES {
            let provenance = audited_archive_recovery_provenance(recovery);
            assert_eq!(
                audited_archive_recovery_provenance_matches(recovery.epoch, &provenance)
                    .map(|matched| matched.epoch),
                Some(recovery.epoch),
            );
            assert!(
                audited_archive_recovery_provenance_matches(recovery.epoch + 1_000, &provenance)
                    .is_none()
            );
            let ArchiveProvenance::V2(v2) = &provenance else {
                unreachable!();
            };
            assert!(
                audited_archive_recovery_provenance_matches(
                    recovery.epoch,
                    &ArchiveProvenance::V1(v2.base.clone()),
                )
                .is_none()
            );

            assert_audited_archive_provenance_mutation_rejected(recovery, &provenance, |changed| {
                changed.base.generation_profile.push('0')
            });
            assert_audited_archive_provenance_mutation_rejected(recovery, &provenance, |changed| {
                changed.base.runtime_profile.push_str("-different")
            });
            assert_audited_archive_provenance_mutation_rejected(recovery, &provenance, |changed| {
                changed.base.runtime_admission = RuntimeAdmission::Verified
            });
            assert_audited_archive_provenance_mutation_rejected(recovery, &provenance, |changed| {
                changed.base.runtime_revision.push('0')
            });
            assert_audited_archive_provenance_mutation_rejected(recovery, &provenance, |changed| {
                changed.base.runtime_toolchain.push('0')
            });
            assert_audited_archive_provenance_mutation_rejected(recovery, &provenance, |changed| {
                changed.base.genesis_hash = Hash::new_unique()
            });
            assert_audited_archive_provenance_mutation_rejected(recovery, &provenance, |changed| {
                changed.base.bootstrap_state_kind = BootstrapStateKind::CarriedBank
            });
            assert_audited_archive_provenance_mutation_rejected(recovery, &provenance, |changed| {
                changed.base.bootstrap_slot += 1
            });
            assert_audited_archive_provenance_mutation_rejected(recovery, &provenance, |changed| {
                changed.base.bootstrap_state_hash = Hash::new_unique()
            });
            assert_audited_archive_provenance_mutation_rejected(recovery, &provenance, |changed| {
                changed.base.requested_slot_start += 1
            });
            assert_audited_archive_provenance_mutation_rejected(recovery, &provenance, |changed| {
                changed.base.requested_slot_count -= 1
            });
            assert_audited_archive_provenance_mutation_rejected(recovery, &provenance, |changed| {
                changed.base.transaction_metadata = TransactionMetadataPolicy::observed()
            });
            assert_audited_archive_provenance_mutation_rejected(recovery, &provenance, |changed| {
                changed.worker_executable_sha256[0] ^= 1
            });
            assert!(!runtime_generation_profile_is_compatible(
                recovery.runtime_descriptor.identity.name,
                recovery.generation_profile,
            ));
        }
    }

    #[test]
    fn audited_archive_recovery_binds_archive_and_frozen_worker_bytes() {
        for recovery in AUDITED_ARCHIVE_RECOVERIES {
            assert!(audited_archive_content_matches(
                recovery,
                recovery.archive_bytes,
                recovery.archive_sha256,
            ));
            assert!(!audited_archive_content_matches(
                recovery,
                recovery.archive_bytes - 1,
                recovery.archive_sha256,
            ));
            let mut changed_archive = recovery.archive_sha256;
            changed_archive[0] ^= 1;
            assert!(!audited_archive_content_matches(
                recovery,
                recovery.archive_bytes,
                changed_archive,
            ));

            let path = Path::new(recovery.worker_path);
            assert!(audited_archive_worker_binding_matches(
                recovery,
                path,
                recovery.worker_sha256,
            ));
            assert!(!audited_archive_worker_binding_matches(
                recovery,
                &path.with_file_name("different-worker"),
                recovery.worker_sha256,
            ));
            let mut changed_worker = recovery.worker_sha256;
            changed_worker[0] ^= 1;
            assert!(!audited_archive_worker_binding_matches(
                recovery,
                path,
                changed_worker,
            ));
        }
    }

    #[test]
    fn snapshot_hash_semantics_are_runtime_driven_not_compression_driven() {
        let hash = Hash::new_unique();
        let path = PathBuf::from(format!("snapshot-416012-{hash}.tar.zst"));
        let legacy =
            snapshot_path_expectation(&path, compatibility::SOLANA_V1_2_32_RUNTIME.bootstrap)
                .expect("v1.2 zstd snapshot");
        let agave = snapshot_path_expectation(&path, compatibility::AGAVE_V3_RUNTIME.bootstrap)
            .expect("Agave zstd snapshot");

        assert!(matches!(
            legacy,
            (416_012, BankHashExpectation::LegacyAccountsHash(actual)) if actual == hash
        ));
        assert!(matches!(
            agave,
            (416_012, BankHashExpectation::AccountsLtHash(actual)) if actual.0 == hash
        ));
    }

    #[test]
    fn epoch_hash_files_apply_the_selected_runtime_semantics() {
        let directory = tempfile::TempDir::new().unwrap();
        let hash = Hash::new_unique();
        let path = directory.path().join("epoch-hashes.txt");
        fs::write(&path, format!("snapshot-39743950-{hash}.tar.zst\n")).unwrap();

        let legacy =
            read_epoch_hashes_file(&path, compatibility::SOLANA_V1_2_32_RUNTIME.bootstrap).unwrap();
        let agave =
            read_epoch_hashes_file(&path, compatibility::AGAVE_V3_RUNTIME.bootstrap).unwrap();

        assert!(matches!(
            legacy.get(&39_743_950),
            Some(BankHashExpectation::LegacyAccountsHash(actual)) if actual == &hash
        ));
        assert!(matches!(
            agave.get(&39_743_950),
            Some(BankHashExpectation::AccountsLtHash(actual)) if actual.0 == hash
        ));
        assert!(
            read_epoch_hashes_file(&path, compatibility::SOLANA_V1_1_23_RUNTIME.bootstrap,)
                .unwrap_err()
                .contains("incompatible with the slot-selected runtime")
        );
    }

    #[test]
    fn local_snapshot_selection_rejects_same_slot_ambiguity() {
        let directory = tempfile::TempDir::new().unwrap();
        let slot = 39_743_950;
        let bzip2 = directory
            .path()
            .join(format!("snapshot-{slot}-{}.tar.bz2", Hash::new_unique()));
        let zstd = directory
            .path()
            .join(format!("snapshot-{slot}-{}.tar.zst", Hash::new_unique()));
        fs::write(&bzip2, b"bzip2 placeholder").unwrap();
        fs::write(&zstd, b"zstd placeholder").unwrap();

        let error = find_existing_snapshot_archive(
            directory.path(),
            normal_epoch_bootstrap_bounds(92).unwrap(),
            compatibility::SOLANA_V1_2_32_RUNTIME
                .bootstrap
                .archive_extensions,
        )
        .unwrap_err();

        assert!(
            error.contains("multiple local snapshot archives"),
            "{error}"
        );
        assert!(error.contains(&bzip2.display().to_string()), "{error}");
        assert!(error.contains(&zstd.display().to_string()), "{error}");
    }

    #[test]
    fn epoch_12_local_discovery_uses_the_exact_registered_anchor() {
        let directory = tempfile::TempDir::new().unwrap();
        let anchor_hash = compatibility::SOLANA_V1_0_23_INITIAL_SNAPSHOT_ACCOUNTS_HASH;
        let anchor = directory.path().join(format!(
            "snapshot-{}-{}.tar.bz2",
            compatibility::SOLANA_V1_0_23_INITIAL_SNAPSHOT_SLOT,
            anchor_hash,
        ));
        let wrong_anchor = directory.path().join(format!(
            "snapshot-{}-{}.tar.bz2",
            compatibility::SOLANA_V1_0_23_INITIAL_SNAPSHOT_SLOT,
            Hash::new_unique(),
        ));
        let later = directory
            .path()
            .join(format!("snapshot-5183999-{anchor_hash}.tar.bz2"));
        fs::write(&anchor, b"canonical anchor placeholder").unwrap();
        fs::write(&wrong_anchor, b"wrong hash placeholder").unwrap();
        fs::write(&later, b"later snapshot placeholder").unwrap();

        let selected = find_existing_snapshot_archive(
            directory.path(),
            normal_epoch_bootstrap_bounds(12).unwrap(),
            compatibility::SOLANA_V1_0_23_RUNTIME
                .bootstrap
                .archive_extensions,
        )
        .unwrap()
        .unwrap();

        assert_eq!(selected.path, anchor);
        assert_eq!(
            selected.slot,
            compatibility::SOLANA_V1_0_23_INITIAL_SNAPSHOT_SLOT
        );
    }

    #[test]
    fn hourly_boundary_is_added_to_the_root_checkpoint_set() {
        let hash = Hash::new_unique();
        let path = PathBuf::from(format!("snapshot-3464856-{hash}.tar.bz2"));
        let bootstrap = compatibility::SOLANA_V1_0_13_RUNTIME.bootstrap;
        let mut expected = BTreeMap::new();

        add_boundary_snapshot_expectation(&mut expected, &path, bootstrap).unwrap();

        assert!(matches!(
            expected.get(&3_464_856),
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
    fn epoch_12_bootstrap_warms_the_post_snapshot_epoch_11_tail() {
        let snapshot = ReplayBootstrap::SnapshotArchive(PathBuf::from(
            "snapshot-5183736-BUqwiSm2GgH9ByKrBDF6epXHYK9RRh3vyZDKtUqtMXfR.tar.bz2",
        ));
        let epoch_start = compatibility::SOLANA_V1_0_23_CANDIDATE_START_SLOT;
        assert_eq!(snapshot.slot().unwrap(), 5_183_736);
        assert_eq!(
            validate_epoch_bootstrap_snapshot(12, snapshot.snapshot_archive().unwrap()).unwrap(),
            compatibility::SOLANA_V1_0_23_INITIAL_SNAPSHOT_SLOT
        );
        let replay_start = replay_start_for_bootstrap(&snapshot, 12, epoch_start).unwrap();
        assert_eq!(
            replay_start,
            compatibility::SOLANA_V1_0_23_INITIAL_REPLAY_SLOT
        );
        assert_eq!(epoch_start - replay_start, 263);

        let selection = compatibility::select_runtime_with_snapshot_warmup(
            replay_start,
            epoch_start..epoch_to_slot_range(12).1 + 1,
            true,
        )
        .unwrap();
        assert_eq!(
            selection.backend,
            compatibility::RuntimeBackend::SolanaV1_0_23
        );
        assert!(std::ptr::eq(
            selection.descriptor,
            &compatibility::SOLANA_V1_0_23_RUNTIME
        ));
    }

    #[test]
    fn epoch_12_normal_bootstrap_rejects_other_slots_and_hashes() {
        let expected_hash: Hash = compatibility::SOLANA_V1_0_23_INITIAL_SNAPSHOT_ACCOUNTS_HASH
            .parse()
            .unwrap();
        assert!(
            validate_epoch_bootstrap_identity(
                12,
                compatibility::SOLANA_V1_0_23_INITIAL_SNAPSHOT_SLOT,
                expected_hash,
            )
            .is_ok()
        );
        for slot in [5_183_737, 5_183_999] {
            let path = PathBuf::from(format!("snapshot-{slot}-{expected_hash}.tar.bz2"));
            let error = validate_epoch_bootstrap_snapshot(12, &path).unwrap_err();
            assert!(error.contains("5183736..=5183736"), "{error}");
            assert!(validate_epoch_bootstrap_identity(12, slot, expected_hash).is_err());
        }

        let wrong_hash = Hash::new_unique();
        assert_ne!(wrong_hash, expected_hash);
        let path = PathBuf::from(format!(
            "snapshot-{}-{wrong_hash}.tar.bz2",
            compatibility::SOLANA_V1_0_23_INITIAL_SNAPSHOT_SLOT,
        ));
        let error = validate_epoch_bootstrap_snapshot(12, &path).unwrap_err();
        assert!(error.contains(&expected_hash.to_string()), "{error}");
        assert!(error.contains(&wrong_hash.to_string()), "{error}");
        assert!(
            validate_epoch_bootstrap_identity(
                12,
                compatibility::SOLANA_V1_0_23_INITIAL_SNAPSHOT_SLOT,
                wrong_hash,
            )
            .is_err()
        );
    }

    #[test]
    fn genesis_bootstrap_starts_epoch_zero_at_slot_zero_only() {
        let private_dir = Arc::new(tempfile::TempDir::new().unwrap());
        let genesis_bin_path = private_dir.path().join("genesis.bin");
        fs::write(&genesis_bin_path, [1]).unwrap();
        let bootstrap = ReplayBootstrap::Genesis {
            genesis_bin_path,
            identity: historical::GenesisFileIdentity {
                size: 1,
                sha256: [1; 32],
            },
            _private_dir: private_dir,
        };
        assert_eq!(bootstrap.slot().unwrap(), 0);
        assert!(bootstrap.snapshot_archive().is_none());
        assert_eq!(replay_start_for_bootstrap(&bootstrap, 0, 0).unwrap(), 0);
        assert!(replay_start_for_bootstrap(&bootstrap, 1, 432_000).is_err());
    }

    #[test]
    fn genesis_bootstrap_clone_retains_private_admission_directory() {
        let private_dir = Arc::new(tempfile::TempDir::new().unwrap());
        let retained_path = private_dir.path().to_path_buf();
        let genesis_bin_path = retained_path.join("genesis.bin");
        fs::write(&genesis_bin_path, [1]).unwrap();
        let bootstrap = ReplayBootstrap::Genesis {
            genesis_bin_path,
            identity: historical::GenesisFileIdentity {
                size: 1,
                sha256: [1; 32],
            },
            _private_dir: private_dir,
        };
        let retained = bootstrap.clone();

        drop(bootstrap);
        assert!(retained_path.exists());
        drop(retained);
        assert!(!retained_path.exists());
    }

    #[test]
    fn mainnet_genesis_rejects_unpinned_raw_serializations_before_decode() {
        let ledger_dir = tempfile::TempDir::new().unwrap();
        let genesis_bin = ledger_dir.path().join("genesis.bin");
        fs::write(&genesis_bin, b"not a genesis config").unwrap();

        let wrong_size = validate_mainnet_genesis(ledger_dir.path()).unwrap_err();
        assert!(wrong_size.contains("genesis.bin size mismatch"));
        assert!(!wrong_size.contains("failed to decode"));

        let file = fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&genesis_bin)
            .unwrap();
        file.set_len(MAINNET_GENESIS_BIN_SIZE).unwrap();
        drop(file);

        let wrong_digest = validate_mainnet_genesis(ledger_dir.path()).unwrap_err();
        assert!(wrong_digest.contains("genesis.bin SHA-256 mismatch"));
        assert!(!wrong_digest.contains("failed to decode"));
    }

    #[test]
    fn local_genesis_identity_must_match_mainnet() {
        let expected: Hash = compatibility::MAINNET_GENESIS_HASH.parse().unwrap();
        assert_eq!(validate_mainnet_genesis_hash(expected).unwrap(), expected);
        assert!(
            validate_mainnet_genesis_hash(Hash::new_unique())
                .unwrap_err()
                .contains("does not match required mainnet hash")
        );
    }

    #[test]
    fn reusable_genesis_bootstrap_requires_the_registered_commitment() {
        let path = Path::new("epoch-0.jet");
        let expected: Hash = compatibility::MAINNET_GENESIS_HASH.parse().unwrap();
        assert!(validate_genesis_bootstrap_commitment(path, 0, 0, expected, expected).is_ok());

        let wrong = Hash::new_unique();
        let error = validate_genesis_bootstrap_commitment(path, 0, 0, wrong, expected).unwrap_err();
        assert!(error.contains("genesis bootstrap commitment"));
        assert!(error.contains(&wrong.to_string()));
        assert!(error.contains(&expected.to_string()));

        assert!(validate_genesis_bootstrap_commitment(path, 1, 0, expected, expected).is_err());
        assert!(validate_genesis_bootstrap_commitment(path, 0, 1, expected, expected).is_err());
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

    #[test]
    fn staged_recovery_requires_one_nonzero_explicitly_verified_epoch() {
        assert!(validate_staged_recovery_mode(true, 11, 11, Some(true), 1, false, false).is_ok());
        assert!(validate_staged_recovery_mode(false, 0, 10, None, 0, true, true).is_ok());

        for (start, end) in [(0, 0), (11, 12)] {
            let error =
                validate_staged_recovery_mode(true, start, end, Some(true), 1, false, false)
                    .unwrap_err();
            assert!(error.contains("exactly one nonzero epoch"));
        }
        for (explicit_verify, count) in [(None, 0), (Some(false), 1), (Some(true), 2)] {
            let error =
                validate_staged_recovery_mode(true, 11, 11, explicit_verify, count, false, false)
                    .unwrap_err();
            assert!(error.contains("explicit --verify"));
        }
        let error =
            validate_staged_recovery_mode(true, 11, 11, Some(true), 1, true, false).unwrap_err();
        assert!(error.contains("top-level invocation"));
        let error =
            validate_staged_recovery_mode(true, 11, 11, Some(true), 1, false, true).unwrap_err();
        assert!(error.contains("cannot be combined"));
    }

    fn make_private_test_directory(path: &Path) {
        use std::os::unix::fs::PermissionsExt as _;

        fs::create_dir(path).unwrap();
        fs::set_permissions(path, fs::Permissions::from_mode(0o700)).unwrap();
    }

    fn add_staged_test_candidate(work: &Path, attempt: &str, epoch: u64) -> PathBuf {
        use std::os::unix::fs::PermissionsExt as _;

        let attempt = work.join(attempt);
        make_private_test_directory(&attempt);
        let candidate = attempt.join(format!("epoch-{epoch}.jet"));
        fs::write(&candidate, b"candidate").unwrap();
        fs::set_permissions(&candidate, fs::Permissions::from_mode(0o664)).unwrap();
        candidate
    }

    #[test]
    fn staged_recovery_candidate_namespace_is_exact_and_unambiguous() {
        let root = tempfile::TempDir::new().unwrap();
        let work = root.path().join("epoch-11");
        make_private_test_directory(&work);
        make_private_test_directory(&work.join("inputs"));
        let candidate = add_staged_test_candidate(&work, "attempt-import-a", 11);
        make_private_test_directory(&candidate.parent().unwrap().join("scratch"));
        assert_eq!(
            strict_staged_archive_candidate(&work, 11).unwrap().path,
            candidate
        );

        add_staged_test_candidate(&work, "attempt-import-b", 11);
        let error = strict_staged_archive_candidate(&work, 11).unwrap_err();
        assert!(error.contains("multiple attempt directories"), "{error}");
    }

    #[test]
    fn staged_recovery_candidate_rejects_missing_or_unsafe_entries() {
        use std::os::unix::{fs::PermissionsExt as _, fs::symlink};

        let root = tempfile::TempDir::new().unwrap();
        let empty = root.path().join("empty");
        make_private_test_directory(&empty);
        let error = strict_staged_archive_candidate(&empty, 11).unwrap_err();
        assert!(error.contains("no private staged archive"), "{error}");

        let unexpected = root.path().join("unexpected");
        make_private_test_directory(&unexpected);
        fs::write(unexpected.join("junk"), b"not admitted").unwrap();
        let error = strict_staged_archive_candidate(&unexpected, 11).unwrap_err();
        assert!(error.contains("unexpected entry"), "{error}");

        let linked = root.path().join("linked");
        make_private_test_directory(&linked);
        let attempt = linked.join("attempt-import");
        make_private_test_directory(&attempt);
        let target = root.path().join("target.jet");
        fs::write(&target, b"candidate").unwrap();
        symlink(&target, attempt.join("epoch-11.jet")).unwrap();
        let error = strict_staged_archive_candidate(&linked, 11).unwrap_err();
        assert!(error.contains("failed to bind staged archive"), "{error}");

        let executable = root.path().join("executable");
        make_private_test_directory(&executable);
        let candidate = add_staged_test_candidate(&executable, "attempt-import", 11);
        fs::set_permissions(&candidate, fs::Permissions::from_mode(0o750)).unwrap();
        let error = strict_staged_archive_candidate(&executable, 11).unwrap_err();
        assert!(error.contains("executable or special"), "{error}");
    }

    #[test]
    fn staged_recovery_never_mutates_a_path_replacement() {
        use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};

        let root = tempfile::TempDir::new().unwrap();
        let work = root.path().join("epoch-11");
        make_private_test_directory(&work);
        let path = add_staged_test_candidate(&work, "attempt-import", 11);
        let admitted = strict_staged_archive_candidate(&work, 11).unwrap();
        let displaced = root.path().join("displaced.jet");
        fs::rename(&path, &displaced).unwrap();
        let replacement = root.path().join("replacement.jet");
        fs::write(&replacement, b"unrelated").unwrap();
        fs::set_permissions(&replacement, fs::Permissions::from_mode(0o600)).unwrap();
        fs::hard_link(&replacement, &path).unwrap();
        let before = fs::metadata(&replacement).unwrap();

        let (start, end) = epoch_to_slot_range(11);
        let spans = compatibility::plan_runtime_spans(start..end + 1, true).unwrap();
        let selection = runtime_span_selection(&spans[0]).unwrap();
        let shutdown = AtomicBool::new(false);
        let error = staged_epoch_archive_validated(
            11,
            &spans,
            selection,
            &path,
            root.path(),
            &shutdown,
            Some((admitted.file, admitted.identity)),
        )
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("changed before permission preparation"),
            "{error}"
        );
        let after = fs::metadata(&replacement).unwrap();
        assert_eq!(after.ino(), before.ino());
        assert_eq!(after.permissions().mode(), before.permissions().mode());
        assert_eq!(after.nlink(), before.nlink());
    }

    #[test]
    fn staged_recovery_refuses_any_existing_public_namespace_component() {
        let root = tempfile::TempDir::new().unwrap();
        let archive = root.path().join("epoch-11.jet");
        assert!(require_recovery_destination_namespace_absent(&archive).is_ok());
        for path in [
            archive.clone(),
            jetstreamer_node::segment_manifest::segment_manifest_path(&archive).unwrap(),
            jetstreamer_node::archive_checksum::archive_checksum_path(&archive).unwrap(),
        ] {
            fs::write(&path, b"occupied").unwrap();
            let error = require_recovery_destination_namespace_absent(&archive).unwrap_err();
            assert!(error.contains("refuses to replace"), "{error}");
            fs::remove_file(path).unwrap();
        }
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
    fn focused_epoch_12_qualification_can_use_a_later_bootstrap() {
        let snapshot = qualification_snapshot(5_183_999, Hash::new_unique());
        let plan = qualification_plan(
            12,
            12,
            Some(5_184_010),
            Some(true),
            Some(&snapshot),
            Some(Path::new("epoch-hashes-12.txt")),
            Some(Path::new("qualification-5184000-5184010.jet")),
        )
        .unwrap()
        .unwrap();

        assert_eq!(plan.bootstrap_slot, 5_183_999);
        assert_eq!(plan.replay_start, 5_184_000);
        assert_eq!(plan.output_slot_start, 5_184_000);
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
            epoch_isolation_plan(7, 8, false, true).unwrap(),
            (true, Some(8))
        );
        assert_eq!(
            epoch_isolation_plan(2, 3, true, true).unwrap(),
            (true, None)
        );
        assert_eq!(
            epoch_isolation_plan(1, 1, true, true).unwrap(),
            (false, None)
        );
        assert_eq!(
            epoch_isolation_plan(9, 11, false, true).unwrap(),
            (false, None)
        );
        assert_eq!(
            epoch_isolation_plan(11, 12, false, true).unwrap(),
            (true, Some(12))
        );
    }

    #[test]
    fn private_replay_scratch_is_capability_based_not_epoch_based() {
        let historical = compatibility::RuntimeSelection {
            backend: compatibility::RuntimeBackend::SolanaV1_0_7,
            descriptor: &compatibility::SOLANA_V1_0_7_RUNTIME,
            admission: compatibility::AdmissionLevel::Verified,
        };
        let agave = compatibility::RuntimeSelection {
            backend: compatibility::RuntimeBackend::AgaveV3,
            descriptor: &compatibility::AGAVE_V3_RUNTIME,
            admission: compatibility::AdmissionLevel::Verified,
        };

        assert!(runtime_supports_private_replay_scratch(historical, false));
        assert!(runtime_supports_private_replay_scratch(historical, true));
        assert!(runtime_supports_private_replay_scratch(agave, false));
        assert!(!runtime_supports_private_replay_scratch(agave, true));
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

        let bootstrap = compatibility::SOLANA_V1_0_13_RUNTIME.bootstrap;
        let filtered = qualification_expectations(expected, plan, &snapshot, bootstrap).unwrap();
        assert_eq!(filtered.len(), 2);
        assert!(filtered.contains_key(&515_912));
        assert!(filtered.contains_key(&534_248));
        assert!(!filtered.contains_key(&619_848));

        let bootstrap_only = BTreeMap::from([(
            515_912,
            BankHashExpectation::LegacyAccountsHash(bootstrap_hash),
        )]);
        assert!(
            qualification_expectations(bootstrap_only, plan, &snapshot, bootstrap)
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
            qualification_expectations(wrong_bootstrap, plan, &snapshot, bootstrap)
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
    fn cohort_verifier_stays_open_until_the_terminal_root_is_consumed() {
        let bootstrap_hash = Hash::new_unique();
        let terminal_hash = Hash::new_unique();
        let verifier = SnapshotVerifier::new(
            BTreeMap::from([
                (
                    7_343_776,
                    BankHashExpectation::LegacyAccountsHash(bootstrap_hash),
                ),
                (
                    8_213_950,
                    BankHashExpectation::LegacyAccountsHash(terminal_hash),
                ),
            ]),
            None,
        );
        verifier.verify_legacy_accounts_hash(7_343_776, bootstrap_hash);
        let deferred = verifier.finish().unwrap_err();
        assert!(deferred.contains("8213950"), "{deferred}");

        verifier.verify_legacy_accounts_hash(8_213_950, terminal_hash);
        verifier.finish().unwrap();
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
    fn epoch_11_provenance_records_the_v1_0_14_worker() {
        let output_start = 4_752_000;
        let output_end = compatibility::SOLANA_V1_0_14_CANDIDATE_END_SLOT_EXCLUSIVE;
        let selection = compatibility::select_runtime(output_start..output_end, true).unwrap();
        let provenance = build_archive_provenance(
            selection,
            Some([0x14; 32]),
            BootstrapStateKind::SnapshotArchive,
            4_751_796,
            "6vJ22rwAfXfr4hFUJ7AtLupR6LHWBWX114AhKJqPYejb"
                .parse()
                .unwrap(),
            output_start,
            output_end - output_start,
        )
        .unwrap();

        assert_eq!(provenance.version(), 2);
        assert_eq!(
            provenance.single_runtime_worker_executable_sha256(),
            Some(Some([0x14; 32]))
        );
        let provenance = provenance.single_runtime_v1().unwrap();
        assert_eq!(provenance.runtime_profile, "solana-v1.0.14");
        assert_eq!(
            provenance.runtime_revision,
            compatibility::SOLANA_V1_0_14_REVISION
        );
        assert_eq!(provenance.bootstrap_slot, 4_751_796);
        assert_eq!(provenance.requested_slot_start, output_start);
        assert_eq!(provenance.requested_slot_count, 432_000);
    }

    #[test]
    fn epoch_zero_provenance_commits_genesis_at_slot_zero() {
        let selection = compatibility::select_runtime(0..432_000, true).unwrap();
        let genesis_hash: Hash = compatibility::MAINNET_GENESIS_HASH.parse().unwrap();
        let provenance = build_archive_provenance(
            selection,
            Some([0x5a; 32]),
            BootstrapStateKind::Genesis,
            0,
            genesis_hash,
            0,
            432_000,
        )
        .unwrap();
        let provenance = provenance.single_runtime_v1().unwrap();
        assert_eq!(provenance.bootstrap_state_kind, BootstrapStateKind::Genesis);
        assert_eq!(provenance.bootstrap_slot, 0);
        assert_eq!(provenance.bootstrap_state_hash, genesis_hash);
        assert_eq!(provenance.requested_slot_start, 0);
    }

    #[test]
    fn archive_status_provenance_tracks_source_writer_eras() {
        let epoch_slots = 432_000;
        assert_eq!(
            archive_transaction_metadata_policy(8 * epoch_slots, epoch_slots),
            TransactionMetadataPolicy::runtime_reconstructed_before(
                compatibility::OLD_FAITHFUL_STATUS_REQUIRED_START_SLOT
            )
        );
        assert_eq!(
            archive_transaction_metadata_policy(9 * epoch_slots, epoch_slots),
            TransactionMetadataPolicy::runtime_reconstructed_with_fee_from(
                compatibility::OLD_FAITHFUL_STATUS_REQUIRED_START_SLOT
            )
        );
        assert_eq!(
            archive_transaction_metadata_policy(29 * epoch_slots, epoch_slots),
            TransactionMetadataPolicy::runtime_reconstructed_status_and_fee()
        );
        assert_eq!(
            archive_transaction_metadata_policy(30 * epoch_slots, epoch_slots),
            TransactionMetadataPolicy::runtime_reconstructed_status_and_fee()
        );
        assert_eq!(
            archive_transaction_metadata_policy(100 * epoch_slots, epoch_slots),
            TransactionMetadataPolicy::runtime_reconstructed_status_and_fee()
        );
        assert_eq!(
            archive_transaction_metadata_policy(101 * epoch_slots, epoch_slots),
            TransactionMetadataPolicy::observed()
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

        let epoch_13_predecessor =
            PathBuf::from(format!("snapshot-5600000-{}.tar.bz2", Hash::new_unique()));
        assert_eq!(
            validate_epoch_bootstrap_snapshot(13, &epoch_13_predecessor).unwrap(),
            5_600_000
        );
        assert!(validate_epoch_bootstrap_identity(13, 5_600_000, Hash::new_unique()).is_ok());
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
    fn direct_multi_runtime_publication_uses_transactional_checksum_commit() {
        use std::os::unix::fs::PermissionsExt as _;

        let root = tempfile::TempDir::new().unwrap();
        let source = root.path().join("source");
        let destination = root.path().join("destination");
        fs::create_dir(&source).unwrap();
        fs::create_dir(&destination).unwrap();
        fs::set_permissions(&source, fs::Permissions::from_mode(0o700)).unwrap();
        fs::set_permissions(&destination, fs::Permissions::from_mode(0o3770)).unwrap();
        let staged = source.join("epoch-17.jet");
        let published = destination.join("epoch-17.jet");
        fs::write(&staged, b"verified multi-runtime assembly").unwrap();
        fs::write(&published, b"previous multi-runtime archive").unwrap();
        fs::set_permissions(&published, fs::Permissions::from_mode(0o440)).unwrap();
        let old_checksum =
            jetstreamer_node::archive_checksum::archive_checksum_path(&published).unwrap();
        fs::write(&old_checksum, b"previous checksum\n").unwrap();
        fs::set_permissions(&old_checksum, fs::Permissions::from_mode(0o440)).unwrap();

        let staged_file =
            jetstreamer_node::archive_checksum::open_regular_nofollow(&staged).unwrap();
        jetstreamer_node::archive_checksum::prepare_archive_permissions(&staged_file, &destination)
            .unwrap();
        staged_file.sync_all().unwrap();
        let evidence =
            jetstreamer_node::archive_checksum::measure_open_archive(&staged_file).unwrap();

        let result = publish_verified_runtime_assembly(17, &staged, &published, evidence).unwrap();

        assert_eq!(
            fs::read(&published).unwrap(),
            b"verified multi-runtime assembly"
        );
        assert_eq!(
            fs::read_to_string(&result.checksum_path).unwrap(),
            jetstreamer_node::archive_checksum::archive_checksum_line(
                &evidence.sha256,
                published.file_name().unwrap()
            )
            .unwrap()
        );
        let recovery = result.recovery_directory.unwrap();
        assert_eq!(
            fs::read(recovery.join("previous-archive.jet")).unwrap(),
            b"previous multi-runtime archive"
        );
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

    fn private_destination_fixture() -> (tempfile::TempDir, PathBuf) {
        use std::os::unix::fs::{DirBuilderExt as _, PermissionsExt as _};

        // The private-root policy deliberately rejects a group-writable
        // parent such as /tmp. Put an owner-only fixture below the test's
        // working directory so the test exercises the production default.
        let fixture = tempfile::Builder::new()
            .prefix(".adaptive-private-scope-test-")
            .tempdir_in(".")
            .unwrap();
        fs::set_permissions(fixture.path(), fs::Permissions::from_mode(0o700)).unwrap();
        let destination = fixture.path().join("horizon");
        let mut builder = fs::DirBuilder::new();
        builder.mode(0o770).create(&destination).unwrap();
        (fixture, destination)
    }

    #[test]
    fn destination_binding_ignores_cli_symlink_retargeting() {
        use std::os::unix::fs::symlink;

        let fixture = tempfile::tempdir_in(".").unwrap();
        let first = fixture.path().join("first");
        let second = fixture.path().join("second");
        fs::create_dir(&first).unwrap();
        fs::create_dir(&second).unwrap();
        let selected = fixture.path().join("selected");
        symlink(&first, &selected).unwrap();

        let binding = BoundDestination::bind(&selected).unwrap();
        let bound_path = first.canonicalize().unwrap();
        assert_eq!(binding.path(), bound_path);
        fs::remove_file(&selected).unwrap();
        symlink(&second, &selected).unwrap();

        assert_eq!(
            binding.path().join("epoch-17.jet"),
            bound_path.join("epoch-17.jet")
        );
        binding.revalidate().unwrap();
    }

    #[test]
    fn cohort_run_directory_survives_until_committed_cleanup() {
        use std::os::unix::fs::PermissionsExt as _;

        let fixture = tempfile::tempdir_in(".").unwrap();
        let parent = fixture.path().join("cohort");
        let run = CohortRunDirectory::create(
            parent,
            17,
            19,
            "sha256:888df3d89187e3fb8cd307e65eab1a4770153887965f3defd74f048504fc3f1a",
        )
        .unwrap();
        let run_path = run.path().to_path_buf();
        let archive_dir = run_path.join("archives");
        create_or_validate_private_directory(&archive_dir).unwrap();
        fs::write(archive_dir.join("epoch-17.jet"), b"private archive").unwrap();

        let state = fs::read_to_string(run_path.join(COHORT_RUN_STATE_FILE)).unwrap();
        assert!(state.contains("running-private"));
        assert_eq!(
            fs::metadata(run_path.join(COHORT_RUN_STATE_FILE))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
        assert!(archive_dir.join("epoch-17.jet").is_file());

        run.cleanup_after_commit().unwrap();
        assert!(!run_path.exists());
    }

    #[test]
    fn cohort_receipts_live_in_private_destination_identity_scope() {
        use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};

        let (_fixture, destination) = private_destination_fixture();
        let binding = BoundDestination::bind(&destination).unwrap();
        let receipts = cohort_batch_receipt_directory(&binding).unwrap();
        assert!(!receipts.starts_with(binding.path()));
        assert!(receipts.starts_with(private_epoch_scope(binding.path()).unwrap()));
        let expected_name = format!("destination-{:016x}-{:016x}", binding.dev, binding.ino);
        assert_eq!(receipts.file_name().unwrap(), OsString::from(expected_name));
        let metadata = fs::symlink_metadata(receipts).unwrap();
        assert!(metadata.file_type().is_dir());
        assert_eq!(metadata.uid(), effective_user_id());
        assert_eq!(metadata.permissions().mode() & 0o777, 0o700);
    }

    fn assert_private_scope_and_epoch_lease_guards() {
        use std::{
            ffi::CString,
            os::unix::{
                ffi::OsStrExt as _,
                fs::{PermissionsExt as _, symlink},
            },
            time::Instant,
        };

        let (_fixture, destination) = private_destination_fixture();
        let scope = private_epoch_scope(&destination).unwrap();
        for directory in [
            scope.parent().unwrap().to_path_buf(),
            scope.clone(),
            scope.join("locks"),
            scope.join("work"),
        ] {
            let metadata = fs::symlink_metadata(&directory).unwrap();
            assert!(metadata.file_type().is_dir(), "{}", directory.display());
            assert_eq!(
                metadata.permissions().mode() & 0o777,
                0o700,
                "{}",
                directory.display()
            );
        }

        let first = acquire_epoch_leases(&destination, 41, 42).unwrap();
        for epoch in 41..=42 {
            let lock = scope.join("locks").join(format!("epoch-{epoch}.lock"));
            assert_eq!(
                fs::metadata(lock).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
        let contention = match acquire_epoch_leases(&destination, 41, 42) {
            Ok(_) => panic!("a second producer acquired an already-held epoch lease"),
            Err(error) => error,
        };
        assert!(
            contention.contains("epoch 41 is already owned"),
            "{contention}"
        );
        drop(first);
        drop(acquire_epoch_leases(&destination, 41, 42).unwrap());

        let (_fixture, destination) = private_destination_fixture();
        let lock_dir = private_epoch_scope(&destination).unwrap().join("locks");
        let lock_path = lock_dir.join("epoch-7.lock");
        let symlink_target = destination.join("attacker-controlled-lock");
        fs::write(&symlink_target, b"not a lock").unwrap();
        symlink(&symlink_target, &lock_path).unwrap();
        let symlink_error = match acquire_epoch_leases(&destination, 7, 7) {
            Ok(_) => panic!("a symlink was accepted as an epoch lease"),
            Err(error) => error,
        };
        assert!(symlink_error.contains("failed to open epoch 7 lease"));
        fs::remove_file(&lock_path).unwrap();

        let lock_path_c = CString::new(lock_path.as_os_str().as_bytes()).unwrap();
        // SAFETY: `lock_path_c` is a valid, NUL-terminated path inside an
        // owner-only test directory.
        assert_eq!(unsafe { libc::mkfifo(lock_path_c.as_ptr(), 0o600) }, 0);
        let started = Instant::now();
        let fifo_error = match acquire_epoch_leases(&destination, 7, 7) {
            Ok(_) => panic!("a FIFO was accepted as an epoch lease"),
            Err(error) => error,
        };
        assert!(fifo_error.contains("epoch lease is not a regular file"));
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "opening an attacker-created lease FIFO blocked"
        );

        let (fixture, destination) = private_destination_fixture();
        let target = fixture.path().join("redirected-private-root");
        fs::create_dir(&target).unwrap();
        fs::set_permissions(&target, fs::Permissions::from_mode(0o700)).unwrap();
        symlink(&target, fixture.path().join(".jetstreamer-private")).unwrap();
        let error = private_epoch_scope(&destination).unwrap_err();
        assert!(error.contains("owner-only real directory"), "{error}");

        let (fixture, destination) = private_destination_fixture();
        let insecure_root = fixture.path().join(".jetstreamer-private");
        fs::create_dir(&insecure_root).unwrap();
        fs::set_permissions(&insecure_root, fs::Permissions::from_mode(0o770)).unwrap();
        let error = private_epoch_scope(&destination).unwrap_err();
        assert!(error.contains("owner-only real directory"), "{error}");
    }

    #[test]
    fn adaptive_private_scope_and_epoch_leases_fail_closed() {
        const CHILD_MARKER: &str = "JETSTREAMER_PRIVATE_SCOPE_TEST_CHILD";
        if env::var_os(CHILD_MARKER).is_some() {
            assert!(env::var_os(PRIVATE_RUN_ROOT_ENV).is_none());
            assert_private_scope_and_epoch_lease_guards();
            return;
        }

        // Isolate the environment override without mutating this parallel
        // test process's environment.
        let output = std::process::Command::new(env::current_exe().unwrap())
            .arg("--exact")
            .arg("early_snapshot_tests::adaptive_private_scope_and_epoch_leases_fail_closed")
            .arg("--nocapture")
            .env(CHILD_MARKER, "1")
            .env_remove(PRIVATE_RUN_ROOT_ENV)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "private-scope subprocess failed\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    #[test]
    fn adaptive_restart_reuses_only_identical_private_inputs() {
        let (_fixture, destination) = private_destination_fixture();
        let epoch = 8;
        let input_dir = private_epoch_scope(&destination)
            .unwrap()
            .join("work")
            .join(format!("epoch-{epoch}"))
            .join("inputs");
        create_or_validate_private_directory(input_dir.parent().unwrap()).unwrap();
        create_or_validate_private_directory(&input_dir).unwrap();

        let expected = BTreeMap::from([
            (
                3_455_940,
                BankHashExpectation::LegacyAccountsHash(Hash::new_from_array([0x11; 32])),
            ),
            (
                3_887_911,
                BankHashExpectation::LegacyAccountsHash(Hash::new_from_array([0x22; 32])),
            ),
        ]);
        let bootstrap = compatibility::SOLANA_V1_0_13_RUNTIME.bootstrap;
        let first_hashes = write_private_epoch_hashes(&input_dir, &expected, bootstrap).unwrap();
        let second_hashes = write_private_epoch_hashes(&input_dir, &expected, bootstrap).unwrap();
        assert_eq!(first_hashes, second_hashes);
        assert_eq!(
            cached_private_epoch_hashes(&destination, epoch, bootstrap)
                .unwrap()
                .unwrap(),
            expected
        );

        let different = BTreeMap::from([(
            3_455_940,
            BankHashExpectation::LegacyAccountsHash(Hash::new_from_array([0x33; 32])),
        )]);
        assert!(
            write_private_epoch_hashes(&input_dir, &different, bootstrap)
                .unwrap_err()
                .contains("differ")
        );

        let source = destination.join("snapshot-3455940-11111111111111111111111111111111.tar.bz2");
        fs::write(&source, b"canonical snapshot bytes").unwrap();
        let bootstrap = ReplayBootstrap::SnapshotArchive(source.clone());
        let first_bound = bind_snapshot_for_adaptive_job(&bootstrap, &input_dir).unwrap();
        let second_bound = bind_snapshot_for_adaptive_job(&bootstrap, &input_dir).unwrap();
        assert_eq!(
            first_bound.snapshot_archive(),
            second_bound.snapshot_archive()
        );

        fs::write(&source, b"changed source bytes").unwrap();
        assert!(
            bind_snapshot_for_adaptive_job(&bootstrap, &input_dir)
                .unwrap_err()
                .contains("does not match")
        );
    }

    fn adaptive_test_job(epoch: u64, work_dir: PathBuf) -> AdaptiveEpochJob {
        let selection = compatibility::RuntimeSelection {
            backend: compatibility::RuntimeBackend::SolanaV1_0_8,
            descriptor: &compatibility::SOLANA_V1_0_8_RUNTIME,
            admission: compatibility::AdmissionLevel::Verified,
        };
        let private_dir = Arc::new(tempfile::TempDir::new().unwrap());
        let genesis_bin_path = private_dir.path().join("genesis.bin");
        fs::write(&genesis_bin_path, [0]).unwrap();
        let source_bootstrap = ReplayBootstrap::Genesis {
            genesis_bin_path,
            identity: historical::GenesisFileIdentity {
                size: 1,
                sha256: [0; 32],
            },
            _private_dir: private_dir,
        };
        AdaptiveEpochJob {
            epoch,
            spans: Vec::new(),
            selection,
            source_bootstrap,
            bound_bootstrap: None,
            source_snapshot: None,
            hashes_path: PathBuf::from("epoch-hashes.txt"),
            final_output: PathBuf::from(format!("epoch-{epoch}.jet")),
            staged_output: PathBuf::new(),
            scratch_dir: PathBuf::new(),
            work_dir,
            attempt: 0,
        }
    }

    #[test]
    fn adaptive_ready_archive_does_not_consume_resource_capacity() {
        // Regress the serial-admission deadlock: epoch 4 may validate before
        // epoch 2 and wait in `ready`, but epoch 2 must still be able to start.
        let resource_occupied = adaptive_resource_occupancy(0, 0, 1);
        let target_capacity = adaptive_epoch::ramped_capacity(
            1,
            resource_occupied,
            Duration::from_secs(600),
            Duration::from_secs(60),
        );

        assert_eq!(resource_occupied, 0);
        assert_eq!(target_capacity, 1);
        assert!(resource_occupied < target_capacity);

        // Validation remains resource-bearing until its blocking task drains.
        assert_eq!(adaptive_resource_occupancy(0, 1, 1), 1);
        assert_eq!(adaptive_resource_occupancy(1, 0, 1), 1);
    }

    #[test]
    fn adaptive_retries_merge_with_untouched_work_in_epoch_order() {
        let mut pending = VecDeque::from([
            adaptive_test_job(2, PathBuf::from("epoch-2")),
            adaptive_test_job(3, PathBuf::from("epoch-3")),
            adaptive_test_job(5, PathBuf::from("epoch-5")),
            adaptive_test_job(9, PathBuf::from("epoch-9")),
        ]);
        let retries = vec![
            adaptive_test_job(7, PathBuf::from("epoch-7")),
            adaptive_test_job(4, PathBuf::from("epoch-4")),
        ];

        merge_adaptive_retries(&mut pending, retries);

        assert_eq!(
            pending.iter().map(|job| job.epoch).collect::<Vec<_>>(),
            vec![2, 3, 4, 5, 7, 9]
        );
    }

    #[test]
    fn adaptive_terminal_failure_closes_retries_until_existing_work_drains() {
        let mut drain = AdaptiveFailureDrain::default();
        assert!(drain.may_retry(1, 4));
        assert!(!drain.is_drained(0, 0, 0));

        assert!(drain.record("epoch 8 exhausted its retries".to_string()));
        assert!(!drain.may_retry(1, 4));
        assert!(!drain.is_drained(1, 0, 0));
        assert!(!drain.is_drained(0, 1, 0));
        assert!(!drain.is_drained(0, 0, 1));
        assert!(drain.is_drained(0, 0, 0));

        assert!(!drain.record("epoch 9 also failed while draining".to_string()));
        assert_eq!(
            drain.into_error(),
            "epoch 8 exhausted its retries; epoch 9 also failed while draining"
        );
    }

    #[test]
    fn adaptive_failure_drain_publishes_validated_siblings_across_failed_gap() {
        let mut ready = BTreeMap::from([(7, ()), (9, ())]);

        assert_eq!(next_adaptive_publication_epoch(7, &ready, false), Some(7));
        ready.remove(&7);
        assert_eq!(next_adaptive_publication_epoch(8, &ready, false), None);
        assert_eq!(next_adaptive_publication_epoch(8, &ready, true), Some(9));
        ready.remove(&9);
        assert_eq!(next_adaptive_publication_epoch(8, &ready, true), None);
    }

    #[test]
    fn adaptive_retries_use_fresh_private_attempt_directories() {
        use std::os::unix::fs::PermissionsExt as _;

        let directory = tempfile::TempDir::new().unwrap();
        let work_dir = directory.path().join("epoch-2");
        fs::create_dir(&work_dir).unwrap();
        fs::set_permissions(&work_dir, fs::Permissions::from_mode(0o700)).unwrap();
        let mut job = adaptive_test_job(2, work_dir);

        prepare_adaptive_attempt(&mut job).unwrap();
        let first_attempt = job.staged_output.parent().unwrap().to_path_buf();
        let first_scratch = job.scratch_dir.clone();
        fs::write(
            first_scratch.join("crash-mutated-state"),
            b"must not survive",
        )
        .unwrap();
        assert_eq!(job.attempt, 1);
        assert_eq!(first_scratch.parent(), Some(first_attempt.as_path()));
        assert_eq!(
            fs::metadata(&first_scratch).unwrap().permissions().mode() & 0o777,
            0o700
        );
        cleanup_adaptive_attempt(&job).unwrap();
        assert!(!first_attempt.exists());

        prepare_adaptive_attempt(&mut job).unwrap();
        let second_attempt = job.staged_output.parent().unwrap().to_path_buf();
        assert_eq!(job.attempt, 2);
        assert_ne!(second_attempt, first_attempt);
        assert_ne!(job.scratch_dir, first_scratch);
        assert!(!job.scratch_dir.join("crash-mutated-state").exists());
        cleanup_adaptive_attempt(&job).unwrap();
        assert!(!second_attempt.exists());
    }

    #[test]
    fn adaptive_late_child_failure_sends_regular_staging_to_deep_validation() {
        use std::os::unix::fs::PermissionsExt as _;

        let directory = tempfile::TempDir::new().unwrap();
        let work_dir = directory.path().join("epoch-19");
        fs::create_dir(&work_dir).unwrap();
        fs::set_permissions(&work_dir, fs::Permissions::from_mode(0o700)).unwrap();
        let mut job = adaptive_test_job(19, work_dir);
        prepare_adaptive_attempt(&mut job).unwrap();
        fs::write(&job.staged_output, b"complete archive awaiting validation").unwrap();
        let staged_output = job.staged_output.clone();

        let action = handle_adaptive_child_failure(&job, "exit status: 1", true, 2, false);

        let AdaptiveChildFailureAction::Validate(message) = action else {
            panic!("regular staging was not retained for validation");
        };
        assert!(message.contains("running full validation"), "{message}");
        assert_eq!(
            fs::read(staged_output).unwrap(),
            b"complete archive awaiting validation"
        );
        assert!(
            classify_adaptive_deep_validation(Ok(Some(())), job.epoch, &job.staged_output).is_ok()
        );
    }

    #[test]
    fn adaptive_child_failure_cleans_only_an_absent_staged_archive() {
        use std::os::unix::fs::PermissionsExt as _;

        let directory = tempfile::TempDir::new().unwrap();
        let work_dir = directory.path().join("epoch-19");
        fs::create_dir(&work_dir).unwrap();
        fs::set_permissions(&work_dir, fs::Permissions::from_mode(0o700)).unwrap();
        let mut job = adaptive_test_job(19, work_dir);
        prepare_adaptive_attempt(&mut job).unwrap();
        let attempt = job.staged_output.parent().unwrap().to_path_buf();

        let action = handle_adaptive_child_failure(&job, "exit status: 1", true, 2, false);

        assert!(matches!(action, AdaptiveChildFailureAction::Retry(_)));
        assert!(!attempt.exists());
    }

    #[test]
    fn adaptive_infrastructure_failure_preserves_completed_staged_attempt() {
        use std::os::unix::fs::PermissionsExt as _;

        let directory = tempfile::TempDir::new().unwrap();
        let work_dir = directory.path().join("epoch-20");
        fs::create_dir(&work_dir).unwrap();
        fs::set_permissions(&work_dir, fs::Permissions::from_mode(0o700)).unwrap();
        let mut job = adaptive_test_job(20, work_dir);
        prepare_adaptive_attempt(&mut job).unwrap();
        fs::write(&job.staged_output, b"fully completed staged archive").unwrap();
        let staged_output = job.staged_output.clone();

        let action = handle_adaptive_validation_failure(
            &job,
            AdaptiveValidationError::RetainStaging(
                "failed to prepare staged epoch 20 permissions: Operation not permitted"
                    .to_string(),
            ),
            true,
            2,
            false,
        );

        let AdaptiveValidationFailureAction::Terminal(message) = action else {
            panic!("infrastructure failure was incorrectly classified as retryable");
        };
        assert!(message.contains("could not safely classify"));
        assert!(message.contains("preserving staged attempt"));
        assert_eq!(
            fs::read(staged_output).unwrap(),
            b"fully completed staged archive"
        );
    }

    #[test]
    fn adaptive_deep_validation_error_preserves_completed_staged_attempt() {
        use std::os::unix::fs::PermissionsExt as _;

        let directory = tempfile::TempDir::new().unwrap();
        let work_dir = directory.path().join("epoch-21");
        fs::create_dir(&work_dir).unwrap();
        fs::set_permissions(&work_dir, fs::Permissions::from_mode(0o700)).unwrap();
        let mut job = adaptive_test_job(21, work_dir);
        prepare_adaptive_attempt(&mut job).unwrap();
        fs::write(&job.staged_output, b"fully completed staged archive").unwrap();
        let staged_output = job.staged_output.clone();
        let error = classify_adaptive_deep_validation::<()>(
            Err("failed full decode: Input/output error (os error 5)".to_string()),
            job.epoch,
            &job.staged_output,
        )
        .unwrap_err();

        let action = handle_adaptive_validation_failure(&job, error, true, 2, false);

        let AdaptiveValidationFailureAction::Terminal(message) = action else {
            panic!("deep validation error was incorrectly classified as retryable");
        };
        assert!(message.contains("without proving"), "{message}");
        assert!(message.contains("preserving staged attempt"), "{message}");
        assert_eq!(
            fs::read(staged_output).unwrap(),
            b"fully completed staged archive"
        );
    }

    #[test]
    fn adaptive_positively_invalid_framing_remains_retryable() {
        use std::os::unix::fs::PermissionsExt as _;

        let directory = tempfile::TempDir::new().unwrap();
        let work_dir = directory.path().join("epoch-22");
        fs::create_dir(&work_dir).unwrap();
        fs::set_permissions(&work_dir, fs::Permissions::from_mode(0o700)).unwrap();
        let mut job = adaptive_test_job(22, work_dir);
        prepare_adaptive_attempt(&mut job).unwrap();
        fs::write(&job.staged_output, b"not an archive").unwrap();
        let attempt = job.staged_output.parent().unwrap().to_path_buf();
        let error =
            classify_adaptive_deep_validation::<()>(Ok(None), job.epoch, &job.staged_output)
                .unwrap_err();

        let action = handle_adaptive_validation_failure(&job, error, true, 2, false);

        assert!(matches!(action, AdaptiveValidationFailureAction::Retry(_)));
        assert!(!attempt.exists());
    }

    #[test]
    fn adaptive_snapshot_binding_is_deferred_until_admission() {
        use std::os::unix::fs::PermissionsExt as _;

        let directory = tempfile::TempDir::new().unwrap();
        let source = directory
            .path()
            .join("snapshot-3455940-11111111111111111111111111111111.tar.bz2");
        fs::write(&source, b"canonical boundary snapshot").unwrap();
        let work_dir = directory.path().join("epoch-8");
        fs::create_dir(&work_dir).unwrap();
        fs::set_permissions(&work_dir, fs::Permissions::from_mode(0o700)).unwrap();
        let mut job = adaptive_test_job(8, work_dir.clone());
        job.source_bootstrap = ReplayBootstrap::SnapshotArchive(source.clone());
        job.source_snapshot = Some(source);

        let bound_path = work_dir
            .join("inputs")
            .join("snapshot-3455940-11111111111111111111111111111111.tar.bz2");
        assert!(!bound_path.exists());
        assert!(job.bound_bootstrap.is_none());

        prepare_adaptive_attempt(&mut job).unwrap();

        assert_eq!(
            job.bound_bootstrap.as_ref().unwrap().snapshot_archive(),
            Some(bound_path.as_path())
        );
        assert_eq!(
            fs::read(&bound_path).unwrap(),
            b"canonical boundary snapshot"
        );
        assert_eq!(
            fs::metadata(&bound_path).unwrap().permissions().mode() & 0o777,
            0o400
        );
        cleanup_adaptive_attempt(&job).unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn adaptive_shutdown_terminates_leader_and_descendant() {
        use std::os::unix::process::CommandExt as _;

        struct ProcessGroupCleanup(i32);
        impl Drop for ProcessGroupCleanup {
            fn drop(&mut self) {
                if self.0 > 0 {
                    // SAFETY: this test created this isolated process group.
                    unsafe {
                        libc::kill(-self.0, libc::SIGKILL);
                    }
                }
            }
        }

        let directory = tempfile::TempDir::new().unwrap();
        let descendant_pid_path = directory.path().join("descendant.pid");
        let mut command = Command::new("/bin/sh");
        command
            .kill_on_drop(true)
            .arg("-c")
            .arg("trap 'wait; exit 0' TERM; sleep 300 & echo $! > \"$1\"; wait")
            .arg("adaptive-process-tree-test")
            .arg(&descendant_pid_path);
        // SAFETY: only async-signal-safe `setpgid` runs between fork/exec.
        unsafe {
            command.as_std_mut().pre_exec(|| {
                if libc::setpgid(0, 0) != 0 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
        let leader = command.spawn().unwrap();
        let process_group = i32::try_from(leader.id().unwrap()).unwrap();
        let mut cleanup = ProcessGroupCleanup(process_group);

        let descendant_pid = tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if let Ok(contents) = fs::read_to_string(&descendant_pid_path)
                    && let Ok(pid) = contents.trim().parse::<i32>()
                {
                    break pid;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("descendant did not start");
        // SAFETY: the descendant PID came from the live child process.
        assert_eq!(unsafe { libc::getpgid(descendant_pid) }, process_group);

        let mut running = BTreeMap::from([(
            99,
            RunningAdaptiveEpoch {
                job: adaptive_test_job(99, directory.path().join("work")),
                child: leader,
                process_group,
            },
        )]);
        tokio::time::timeout(
            Duration::from_secs(3),
            stop_adaptive_epoch_children(&mut running),
        )
        .await
        .expect("process-group termination timed out")
        .expect("adaptive shutdown failed");
        assert!(running.is_empty());
        tokio::time::timeout(Duration::from_secs(3), async {
            while process_group_exists(process_group) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("descendant survived process-group termination");
        cleanup.0 = 0;

        // Regress the forced-kill ordering separately with a short deadline:
        // the leader must be reaped before checking whether its group reached
        // ESRCH, or its zombie can look like a surviving process group.
        let mut command = Command::new("/bin/sh");
        command.kill_on_drop(true).arg("-c").arg("sleep 300 & wait");
        // SAFETY: only async-signal-safe `setpgid` runs between fork/exec.
        unsafe {
            command.as_std_mut().pre_exec(|| {
                if libc::setpgid(0, 0) != 0 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
        let mut leader = command.spawn().unwrap();
        let process_group = i32::try_from(leader.id().unwrap()).unwrap();
        let mut cleanup = ProcessGroupCleanup(process_group);
        signal_process_group(process_group, libc::SIGKILL).unwrap();
        tokio::time::timeout(Duration::from_secs(3), leader.wait())
            .await
            .expect("forced-kill leader reap timed out")
            .unwrap();
        assert!(
            wait_for_process_group_exit(process_group, Duration::from_secs(3)).await,
            "process group remained visible after its killed leader was reaped"
        );
        cleanup.0 = 0;
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
            .record_block_metadata(0, 0, 0, 0)
            .expect("slot zero must not be considered already finalized");
        assert!(ready.is_empty());
        let snapshot = scheduler.snapshot();
        assert_eq!(snapshot.current_slot, 1);
        assert_eq!(snapshot.last_finalized_slot, 0);
    }

    fn test_scheduler(start: Slot, states: Vec<SlotPresenceState>) -> TransactionScheduler {
        let end_inclusive = start + states.len() as Slot - 1;
        TransactionScheduler::new(
            start,
            Arc::new(SlotPresenceMap {
                start,
                end_inclusive,
                next_present_after: vec![None; states.len()],
                states,
            }),
            Arc::new(RestartTracker::new()),
            0,
        )
    }

    #[test]
    fn parent_edge_overrides_false_present_index_hints() {
        let scheduler = test_scheduler(183, vec![SlotPresenceState::Present; 6]);
        scheduler
            .record_block_metadata(182, 183, 0, 0)
            .expect("first block");

        let ready = scheduler
            .record_block_metadata(183, 188, 0, 0)
            .expect("parent edge proves slots 184 through 187 skipped");

        assert!(ready.is_empty());
        let snapshot = scheduler.snapshot();
        assert_eq!(snapshot.current_slot, 189);
        assert_eq!(snapshot.last_finalized_slot, 188);
        let state = scheduler.state.lock().unwrap();
        assert_eq!(
            &state.canonical_slots[1..5],
            &[
                CanonicalSlotState::ProvenSkipped,
                CanonicalSlotState::ProvenSkipped,
                CanonicalSlotState::ProvenSkipped,
                CanonicalSlotState::ProvenSkipped,
            ]
        );
    }

    #[test]
    fn decoded_block_overrides_false_missing_index_hint() {
        let scheduler = test_scheduler(
            183,
            vec![
                SlotPresenceState::Present,
                SlotPresenceState::Missing,
                SlotPresenceState::Missing,
                SlotPresenceState::Missing,
                SlotPresenceState::Missing,
                SlotPresenceState::Missing,
            ],
        );
        scheduler
            .record_block_metadata(182, 183, 0, 0)
            .expect("first block");
        let snapshot = scheduler.snapshot();
        assert_eq!(snapshot.current_slot, 184);
        assert_eq!(snapshot.last_finalized_slot, 183);

        scheduler
            .record_block_metadata(183, 188, 0, 0)
            .expect("decoded metadata proves slot 188 present");

        let snapshot = scheduler.snapshot();
        assert_eq!(snapshot.current_slot, 189);
        assert_eq!(snapshot.last_finalized_slot, 188);
        assert_eq!(
            scheduler.state.lock().unwrap().canonical_slots[5],
            CanonicalSlotState::RequiredPresent
        );
    }

    #[test]
    fn parent_edge_rejects_block_data_in_its_skipped_interval() {
        let scheduler = test_scheduler(183, vec![SlotPresenceState::Present; 6]);
        scheduler
            .record_block_metadata(182, 183, 0, 0)
            .expect("first block");
        scheduler
            .push_entry(185, 0, 0, 0, Hash::new_unique(), 1)
            .expect("buffer contradictory entry");

        let error = scheduler
            .record_block_metadata(183, 188, 0, 0)
            .expect_err("parent edge must not override decoded data");
        assert!(error.contains("slot 185"), "{error}");
        assert!(error.contains("proves it present"), "{error}");
        let state = scheduler.state.lock().unwrap();
        assert_eq!(
            state.canonical_slots,
            vec![
                CanonicalSlotState::RequiredPresent,
                CanonicalSlotState::Unknown,
                CanonicalSlotState::RequiredPresent,
                CanonicalSlotState::Unknown,
                CanonicalSlotState::Unknown,
                CanonicalSlotState::Unknown,
            ]
        );
    }

    #[test]
    fn payload_before_parent_edge_does_not_finalize_index_hints() {
        let scheduler = test_scheduler(183, vec![SlotPresenceState::Missing; 6]);
        scheduler
            .record_block_metadata(182, 183, 0, 0)
            .expect("first block");
        scheduler
            .push_entry(188, 0, 0, 0, Hash::new_unique(), 1)
            .expect("child payload may arrive before parent evidence");
        assert_eq!(scheduler.snapshot().current_slot, 184);

        scheduler
            .record_block_parent(183, 188)
            .expect("parent edge classifies only the intervening slots");
        scheduler
            .record_block_metadata(183, 188, 0, 1)
            .expect("child metadata completes the block");
        assert_eq!(scheduler.snapshot().current_slot, 189);
    }

    #[test]
    fn child_parent_link_does_not_classify_unrelated_unknown_slots() {
        let scheduler = test_scheduler(183, vec![SlotPresenceState::Missing; 6]);

        scheduler
            .record_block_metadata(187, 188, 0, 0)
            .expect("child metadata records required parent");

        let snapshot = scheduler.snapshot();
        assert_eq!(snapshot.current_slot, 183);
        assert_eq!(snapshot.last_finalized_slot, 182);
        let state = scheduler.state.lock().unwrap();
        assert_eq!(
            state.canonical_slots,
            vec![
                CanonicalSlotState::Unknown,
                CanonicalSlotState::Unknown,
                CanonicalSlotState::Unknown,
                CanonicalSlotState::Unknown,
                CanonicalSlotState::RequiredPresent,
                CanonicalSlotState::RequiredPresent,
            ]
        );
        drop(state);
        let error = scheduler
            .verify_complete(188)
            .expect_err("index-missing slots cannot certify themselves");
        assert!(error.contains("slot 183 has no decoded"), "{error}");
    }

    #[test]
    fn parent_edge_rejects_non_preceding_parent() {
        let scheduler = test_scheduler(183, vec![SlotPresenceState::Present; 6]);
        let error = scheduler
            .record_block_metadata(188, 188, 0, 0)
            .expect_err("a block cannot parent itself");
        assert!(error.contains("non-preceding parent"), "{error}");
    }

    #[test]
    fn parent_edge_resolves_gap_larger_than_backpressure_limit() {
        let start = 100;
        let states = vec![SlotPresenceState::Present; 201];
        let end_inclusive = start + states.len() as Slot - 1;
        let scheduler = TransactionScheduler::new(
            start,
            Arc::new(SlotPresenceMap {
                start,
                end_inclusive,
                next_present_after: vec![None; states.len()],
                states,
            }),
            Arc::new(RestartTracker::new()),
            128,
        );

        scheduler
            .record_block_parent(99, 300)
            .expect("parent evidence must be applied before the gap guard");
        assert_eq!(scheduler.snapshot().current_slot, 300);
        scheduler
            .record_block_metadata(99, 300, 0, 0)
            .expect("empty child block");
        assert_eq!(scheduler.snapshot().current_slot, 301);
    }

    #[test]
    fn conflicting_redelivered_parent_is_rejected() {
        let scheduler = test_scheduler(183, vec![SlotPresenceState::Present; 6]);
        scheduler
            .record_block_parent(183, 188)
            .expect("first parent edge");
        let error = scheduler
            .record_block_parent(182, 188)
            .expect_err("redelivery cannot change a decoded parent");
        assert!(error.contains("conflicting parent metadata"), "{error}");
    }

    #[test]
    fn successor_edge_proves_epoch_tail() {
        let scheduler = test_scheduler(100, vec![SlotPresenceState::Missing; 6]);
        scheduler
            .record_block_metadata(99, 100, 0, 0)
            .expect("first block");
        scheduler
            .record_block_parent(100, 110)
            .expect("first successor proves the trailing gap");

        let snapshot = scheduler.snapshot();
        assert_eq!(snapshot.current_slot, 106);
        assert_eq!(snapshot.highest_seen_slot, 100);
        scheduler
            .verify_complete(105)
            .expect("every trailing slot has parent-edge proof");
    }

    #[test]
    fn successor_edge_exposes_missing_required_tail_parent() {
        let scheduler = test_scheduler(100, vec![SlotPresenceState::Missing; 6]);
        scheduler
            .record_block_metadata(99, 100, 0, 0)
            .expect("first block");
        scheduler
            .record_block_parent(104, 110)
            .expect("successor names an in-range parent");

        let snapshot = scheduler.snapshot();
        assert_eq!(snapshot.current_slot, 101);
        assert_eq!(snapshot.highest_seen_slot, 100);
        let error = scheduler
            .verify_complete(105)
            .expect_err("the successor alone cannot prove the gap before its parent");
        assert!(error.contains("slot 101 has no decoded"), "{error}");
    }
}

#[cfg(test)]
mod scheduler_tests {
    use super::{
        EntryAccounts, SlotExecutionBuffer, assign_rounds,
        compatibility::TransactionStatusValidation, status_multisets_equal,
    };
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
            .insert_transaction(
                0,
                VersionedTransaction::default(),
                None,
                TransactionStatusValidation::RuntimeOnly,
            )
            .unwrap();
        assert!(missing.txs[0].as_ref().unwrap().expected_status.is_none());

        let mut observed = SlotExecutionBuffer::default();
        observed
            .insert_transaction(
                0,
                VersionedTransaction::default(),
                Some(TransactionStatusMeta::default()),
                TransactionStatusValidation::SourceExact,
            )
            .unwrap();
        assert_eq!(
            observed.txs[0].as_ref().unwrap().expected_status,
            Some(Ok(()))
        );

        let mut permuted = SlotExecutionBuffer::default();
        permuted
            .insert_transaction(
                0,
                VersionedTransaction::default(),
                Some(TransactionStatusMeta::default()),
                TransactionStatusValidation::RuntimeWithSourceEntryMultiset,
            )
            .unwrap();
        let scheduled = permuted.txs[0].as_ref().unwrap();
        assert!(scheduled.expected_status.is_none());
        assert_eq!(scheduled.source_entry_status, Some(Ok(())));
    }

    #[test]
    fn status_multiset_comparison_preserves_duplicate_counts() {
        assert!(status_multisets_equal(&[1, 2, 2, 3], &[2, 3, 2, 1]));
        assert!(!status_multisets_equal(&[1, 2, 2, 3], &[1, 2, 3, 3]));
        assert!(!status_multisets_equal(&[1, 2], &[1]));
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
