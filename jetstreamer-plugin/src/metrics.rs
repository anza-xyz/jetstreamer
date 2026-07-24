//! Global runtime metrics shared with frontends such as the CLI `--tui` mode.
//!
//! The plugin runner stamps per-thread activity as data flows through its handlers and
//! records a structured snapshot of every stats pulse; a frontend polls these from its render
//! loop instead of scraping log lines.

use dashmap::DashMap;
use once_cell::sync::Lazy;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Instant;

static ORIGIN: Lazy<Instant> = Lazy::new(Instant::now);
static THREAD_COUNT: AtomicUsize = AtomicUsize::new(0);
static THREAD_LAST_ACTIVITY_MS: Lazy<DashMap<usize, u64, ahash::RandomState>> =
    Lazy::new(|| DashMap::with_hasher(ahash::RandomState::new()));
static THREAD_TX_COUNTS: Lazy<DashMap<usize, u64, ahash::RandomState>> =
    Lazy::new(|| DashMap::with_hasher(ahash::RandomState::new()));
static LATEST_PULSE: Mutex<Option<PulseSnapshot>> = Mutex::new(None);
static RUN_SLOT_RANGE: Mutex<Option<(u64, u64)>> = Mutex::new(None);
static DB_RETRIES: AtomicU64 = AtomicU64::new(0);
static DB_DROPPED: AtomicU64 = AtomicU64::new(0);

/// Structured copy of the numbers a stats pulse logs, for frontends to render.
#[derive(Clone, Debug, Default)]
pub struct PulseSnapshot {
    /// Overall progress in percent, clamped to `[0, 100]`.
    pub progress_pct: f64,
    /// Human-readable ETA, if computable.
    pub eta: Option<String>,
    /// Transactions per second measured between the last two pulses.
    pub tps: f64,
    /// Aggregate slots processed, capped to the run's total.
    pub slots_processed: u64,
    /// Aggregate blocks processed.
    pub blocks_processed: u64,
    /// Aggregate transactions processed.
    pub transactions_processed: u64,
    /// Aggregate entries processed.
    pub entries_processed: u64,
    /// Aggregate rewards processed.
    pub rewards_processed: u64,
    /// Total number of slots in the run's range.
    pub total_slots: u64,
    /// Seconds elapsed since the run started.
    pub elapsed_secs: f64,
}

/// Milliseconds since metrics tracking began (a process-wide monotonic clock).
pub fn now_ms() -> u64 {
    ORIGIN.elapsed().as_millis() as u64
}

/// Prepares metrics for a new run with `thread_count` firehose threads.
pub fn init(thread_count: usize) {
    Lazy::force(&ORIGIN);
    THREAD_COUNT.store(thread_count, Ordering::Relaxed);
    THREAD_LAST_ACTIVITY_MS.clear();
    THREAD_TX_COUNTS.clear();
    *LATEST_PULSE.lock().unwrap() = None;
    *RUN_SLOT_RANGE.lock().unwrap() = None;
    DB_RETRIES.store(0, Ordering::Relaxed);
    DB_DROPPED.store(0, Ordering::Relaxed);
}

/// Records one retried ClickHouse write attempt.
pub fn note_db_retry() {
    DB_RETRIES.fetch_add(1, Ordering::Relaxed);
}

/// Total ClickHouse write retries this run.
pub fn db_retry_count() -> u64 {
    DB_RETRIES.load(Ordering::Relaxed)
}

/// Records a ClickHouse write dropped after exhausting its retry horizon.
pub fn note_db_dropped() {
    DB_DROPPED.fetch_add(1, Ordering::Relaxed);
}

/// Total ClickHouse writes dropped this run (should be zero).
pub fn db_dropped_count() -> u64 {
    DB_DROPPED.load(Ordering::Relaxed)
}

/// Records the half-open slot range `[start, end)` the current run covers.
pub fn set_run_slot_range(start: u64, end: u64) {
    *RUN_SLOT_RANGE.lock().unwrap() = Some((start, end));
}

/// The half-open slot range `[start, end)` of the current run, if one is active.
pub fn run_slot_range() -> Option<(u64, u64)> {
    *RUN_SLOT_RANGE.lock().unwrap()
}

/// Number of firehose threads in the current run.
pub fn thread_count() -> usize {
    THREAD_COUNT.load(Ordering::Relaxed)
}

/// Records that data flowed through `thread_id` just now.
pub fn note_thread_activity(thread_id: usize) {
    THREAD_LAST_ACTIVITY_MS.insert(thread_id, now_ms());
}

/// Records one processed transaction on `thread_id` (also stamps activity).
pub fn note_thread_transaction(thread_id: usize) {
    note_thread_activity(thread_id);
    *THREAD_TX_COUNTS.entry(thread_id).or_insert(0) += 1;
}

/// Total transactions processed by `thread_id` so far.
pub fn thread_tx_count(thread_id: usize) -> u64 {
    THREAD_TX_COUNTS
        .get(&thread_id)
        .map(|count| *count)
        .unwrap_or(0)
}

/// Milliseconds since data last flowed through `thread_id`, or `None` if the thread has not
/// reported any data yet.
pub fn thread_idle_ms(thread_id: usize) -> Option<u64> {
    THREAD_LAST_ACTIVITY_MS
        .get(&thread_id)
        .map(|stamp| now_ms().saturating_sub(*stamp))
}

/// Stores the latest stats pulse.
pub fn record_pulse(pulse: PulseSnapshot) {
    *LATEST_PULSE.lock().unwrap() = Some(pulse);
}

/// Returns the most recent stats pulse, if any.
pub fn latest_pulse() -> Option<PulseSnapshot> {
    LATEST_PULSE.lock().unwrap().clone()
}
