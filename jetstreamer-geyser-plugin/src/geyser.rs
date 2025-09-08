use agave_geyser_plugin_interface::geyser_plugin_interface::{
    GeyserPlugin, GeyserPluginError, ReplicaBlockInfoVersions, ReplicaTransactionInfoVersions,
    Result,
};
use core::ops::Range;
use crossbeam_utils::CachePadded;
use geyser_replay::{epochs::slot_to_epoch, firehose::generate_subranges};
use rangemap::RangeMap;
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};
use std::{
    cell::RefCell,
    error::Error,
    sync::Arc,
    sync::atomic::{AtomicBool, AtomicU8, AtomicU32, AtomicU64, Ordering},
    time::Instant,
};
use thousands::Separable;
use tokio::runtime::Runtime;

use crate::clickhouse_utils;

/// Formats a duration in a human-readable way without allocating
struct DurationFormatter {
    duration: Duration,
}

impl DurationFormatter {
    fn new(duration: Duration) -> Self {
        Self { duration }
    }
}

impl std::fmt::Display for DurationFormatter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let total_seconds = self.duration.as_secs();
        let days = total_seconds / 86400;
        let hours = (total_seconds % 86400) / 3600;
        let minutes = (total_seconds % 3600) / 60;
        let seconds = total_seconds % 60;

        if days > 0 {
            write!(f, "{}d {}h {}m", days, hours, minutes)
        } else if hours > 0 {
            write!(f, "{}h {}m {}s", hours, minutes, seconds)
        } else if minutes > 0 {
            write!(f, "{}m {}s", minutes, seconds)
        } else {
            write!(f, "{}s", seconds)
        }
    }
}

use crossbeam_channel::{SendError, SendTimeoutError};
use jetstreamer_plugin::{
    bridge::{Block, Transaction},
    ipc::JetstreamerMessage,
};

#[cfg(feature = "plugin-runner")]
use crossbeam_channel::{Receiver, Sender, bounded};

#[cfg(feature = "plugin-runner")]
use jetstreamer_plugin::ipc::spawn_socket_server;

#[cfg(not(feature = "plugin-runner"))]
use jetstreamer_plugin::{Plugin, plugins::program_tracking::ProgramTrackingPlugin};

thread_local! {
    static TOKIO_RUNTIME: RefCell<Runtime> = RefCell::new(Runtime::new().unwrap());
    static DB_CLIENT: RefCell<Arc<::clickhouse::Client>> = RefCell::new(Arc::new(::clickhouse::Client::default()
            .with_url("http://localhost:8123")
            .with_option("async_insert", "1")
            .with_option("wait_for_async_insert", "0")));
    #[cfg(not(feature = "plugin-runner"))]
    static PLUGIN: RefCell<ProgramTrackingPlugin> = const { RefCell::new(ProgramTrackingPlugin) };
}
#[cfg(feature = "plugin-runner")]
static IPC_SENDERS: once_cell::sync::OnceCell<Vec<Sender<JetstreamerMessage>>> =
    once_cell::sync::OnceCell::new();
#[cfg(feature = "plugin-runner")]
static IPC_TASKS: once_cell::sync::OnceCell<Vec<tokio::task::JoinHandle<()>>> =
    once_cell::sync::OnceCell::new();

static EXIT: CachePadded<AtomicBool> = CachePadded::new(AtomicBool::new(false));
static PROCESSED_TRANSACTIONS: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
static PROCESSED_SLOTS: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
static NUM_VOTES: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
static COMPLETE_THREADS: CachePadded<AtomicU8> = CachePadded::new(AtomicU8::new(0));
static CLICKHOUSE_INITIALIZED: CachePadded<AtomicBool> = CachePadded::new(AtomicBool::new(false));

static START_TIME: once_cell::sync::OnceCell<Instant> = once_cell::sync::OnceCell::new();
// Cached TPS fetched from ClickHouse (store as micros to avoid f64 atomics). Updated periodically.
static CLICKHOUSE_TPS_CACHE_MICRO: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
static CLICKHOUSE_TPS_LAST_QUERY_SEC: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
static SLOT_RANGE: once_cell::sync::OnceCell<Range<u64>> = once_cell::sync::OnceCell::new();

static THREAD_CURRENT_SLOT: [CachePadded<AtomicU64>; 256] =
    [const { CachePadded::new(AtomicU64::new(u64::MAX)) }; 256];
static THREAD_SLOT_RANGE_START: [CachePadded<AtomicU64>; 256] =
    [const { CachePadded::new(AtomicU64::new(u64::MAX)) }; 256];
static THREAD_SLOT_RANGE_END: [CachePadded<AtomicU64>; 256] =
    [const { CachePadded::new(AtomicU64::new(u64::MAX)) }; 256];
// Tracks how many slots each thread has actually accounted for toward completion of its own range.
// This prevents large forward jumps in a single thread from being double-counted toward overall
// progress (which previously could cause premature unload while other threads were still running).
static THREAD_PROCESSED_SLOTS: [CachePadded<AtomicU64>; 256] =
    [const { CachePadded::new(AtomicU64::new(0)) }; 256];
// Last monotonic progress timestamp (nanos since START_TIME) per thread, for stall detection / diagnostics
static THREAD_LAST_PROGRESS_NS: [CachePadded<AtomicU64>; 256] =
    [const { CachePadded::new(AtomicU64::new(0)) }; 256];
static THREAD_CURRENT_TX_INDEX: [CachePadded<AtomicU32>; 256] =
    [const { CachePadded::new(AtomicU32::new(u32::MAX)) }; 256];
static THREAD_INFO: once_cell::sync::OnceCell<RangeMap<u64, u8>> = once_cell::sync::OnceCell::new();

#[inline(always)]
pub fn thread_current_slot(thread_id: u8) -> u64 {
    THREAD_CURRENT_SLOT[thread_id as usize].load(Ordering::SeqCst)
}

#[inline(always)]
pub fn thread_bump_current_slot(thread_id: u8) -> u64 {
    THREAD_CURRENT_SLOT[thread_id as usize].fetch_add(1, Ordering::SeqCst) + 1
}

#[inline(always)]
pub fn thread_set_current_slot(thread_id: u8, slot: u64) {
    THREAD_CURRENT_SLOT[thread_id as usize].store(slot, Ordering::SeqCst);
}

#[inline(always)]
pub fn thread_slot_range_start(thread_id: u8) -> u64 {
    THREAD_SLOT_RANGE_START[thread_id as usize].load(Ordering::SeqCst)
}

#[inline(always)]
pub fn thread_slot_range_end(thread_id: u8) -> u64 {
    THREAD_SLOT_RANGE_END[thread_id as usize].load(Ordering::SeqCst)
}

#[inline(always)]
pub fn thread_current_tx_index(thread_id: u8) -> u32 {
    THREAD_CURRENT_TX_INDEX[thread_id as usize].load(Ordering::SeqCst)
}

#[inline(always)]
pub fn thread_set_slot_range(thread_id: u8, start: u64, end: u64) {
    THREAD_SLOT_RANGE_START[thread_id as usize].store(start, Ordering::SeqCst);
    THREAD_SLOT_RANGE_END[thread_id as usize].store(end, Ordering::SeqCst);
}

#[inline(always)]
pub fn thread_bump_tx_index(thread_id: u8) -> u32 {
    THREAD_CURRENT_TX_INDEX[thread_id as usize].fetch_add(1, Ordering::SeqCst) + 1
}

#[inline(always)]
pub fn thread_set_current_tx_index(thread_id: u8, index: u32) {
    THREAD_CURRENT_TX_INDEX[thread_id as usize].store(index, Ordering::SeqCst);
}

fn maybe_update_clickhouse_tps(processed_slots: u64) {
    // Only attempt every 100 processed slot aggregations (same cadence as some logs) or if stale.
    if processed_slots % 100 != 0 {
        return;
    }
    let now_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let last = CLICKHOUSE_TPS_LAST_QUERY_SEC.load(Ordering::Relaxed);
    let min_interval = 1; // seconds
    if now_secs.saturating_sub(last) < min_interval {
        return;
    }

    let sql = format!(
        "SELECT sum(transaction_count) AS txs, greatest(1, dateDiff('second', min({c}), max({c}))) AS span FROM jetstreamer_slot_status WHERE {c} >= now() - INTERVAL {w} SECOND",
        c = "ingest_time",
        w = 15, // window seconds
    );

    DB_CLIENT.with_borrow(|db| {
        TOKIO_RUNTIME.with_borrow(|rt| {
            let _ = rt.block_on(async {
                #[derive(::clickhouse::Row, serde::Deserialize)]
                struct Row {
                    txs: u64,
                    span: u64,
                }
                if let Ok(row) = db.query(&sql).fetch_one::<Row>().await {
                    if row.span > 0 {
                        let tps = (row.txs as f64) / (row.span as f64);
                        let micros = (tps * 1_000_000.0) as u64;
                        CLICKHOUSE_TPS_CACHE_MICRO.store(micros, Ordering::Relaxed);
                        CLICKHOUSE_TPS_LAST_QUERY_SEC.store(now_secs, Ordering::Relaxed);
                    }
                } else {
                    log::debug!("ClickHouse TPS query failed (sql: {})", sql);
                }
            });
        });
    });
}

#[derive(Clone, Debug, Default)]
pub struct Jetstreamer;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JetstreamerError {
    ClickHouseError(String),
    ClickHouseInitializationFailed,
}

impl Error for JetstreamerError {}

impl std::fmt::Display for JetstreamerError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            JetstreamerError::ClickHouseError(msg) => write!(f, "ClickHouse error: {}", msg),
            JetstreamerError::ClickHouseInitializationFailed => {
                write!(f, "ClickHouse initialization failed")
            }
        }
    }
}

impl From<JetstreamerError> for GeyserPluginError {
    fn from(err: JetstreamerError) -> Self {
        GeyserPluginError::Custom(Box::new(err))
    }
}

#[derive(Debug)]
pub enum IpcSendError {
    Timeout,
    Disconnected,
    Other(String),
}

impl std::fmt::Display for IpcSendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IpcSendError::Timeout => write!(f, "Timeout occurred"),
            IpcSendError::Disconnected => write!(f, "Sender disconnected"),
            IpcSendError::Other(msg) => write!(f, "Other error: {}", msg),
        }
    }
}

impl From<SendTimeoutError<JetstreamerMessage>> for IpcSendError {
    fn from(err: SendTimeoutError<JetstreamerMessage>) -> Self {
        match err {
            SendTimeoutError::Timeout(_) => IpcSendError::Timeout,
            SendTimeoutError::Disconnected(_) => IpcSendError::Disconnected,
        }
    }
}

impl From<SendError<JetstreamerMessage>> for IpcSendError {
    fn from(err: SendError<JetstreamerMessage>) -> Self {
        IpcSendError::Other(err.to_string())
    }
}

#[inline(always)]
#[allow(unused_variables)]
pub fn ipc_send(thread_id: usize, msg: JetstreamerMessage) {
    #[cfg(feature = "plugin-runner")]
    {
        let senders = IPC_SENDERS.get().expect("IPC_SENDERS not initialized");
        let sender = &senders[thread_id];
        let is_exit = msg == JetstreamerMessage::Exit;

        let _: std::result::Result<(), IpcSendError> = if is_exit {
            sender
                .send_timeout(msg, Duration::from_millis(50))
                .map_err(|e| e.into())
        } else {
            sender.send(msg).map_err(|e| e.into())
        };

        if is_exit {
            log::info!("sent exit signal to client socket {}", thread_id);
        }
    }
    #[cfg(not(feature = "plugin-runner"))]
    {
        PLUGIN.with_borrow(|plugin| {
            DB_CLIENT.with_borrow(|db| {
                TOKIO_RUNTIME.with_borrow(|rt| {
                    rt.block_on(async move {
                        if let Err(e) =
                            jetstreamer_plugin::handle_message(plugin, db.clone(), msg, plugin.id())
                                .await
                        {
                            // Don't panic on ClickHouse connection errors during shutdown
                            if exiting() {
                                log::debug!("Ignoring ClickHouse error during shutdown: {}", e);
                            } else {
                                log::error!("ClickHouse error: {}", e);
                            }
                        }
                    });
                });
            });
        });
    }
}

fn parse_range(slot_range: impl AsRef<str>) -> Option<Range<u64>> {
    let range_str = slot_range.as_ref();
    if range_str.is_empty() {
        return None;
    }
    let parts: Vec<&str> = range_str.split("..").collect();
    if parts.len() != 2 {
        log::error!("Invalid slot range format: {}", range_str);
        return None;
    }
    let start = parts[0].parse::<u64>().ok()?;
    let end = parts[1].parse::<u64>().ok()?;
    Some(start..end)
}

impl GeyserPlugin for Jetstreamer {
    fn name(&self) -> &'static str {
        "GeyserPluginJetstreamer"
    }

    fn on_load(&mut self, config_file: &str, _is_reload: bool) -> Result<()> {
        solana_logger::setup_with_default("info");
        if SLOT_RANGE.get().is_some() {
            log::info!("done, exiting...");
            unload();
            return Ok(());
        }
        log::info!("jetstreamer loading...");

        // process config
        let config_raw_json =
            std::fs::read_to_string(config_file).map_err(GeyserPluginError::from)?;
        let config_json = serde_json::from_str::<serde_json::Value>(&config_raw_json)
            .map_err(|e| GeyserPluginError::ConfigFileReadError { msg: e.to_string() })?;
        log::info!("config file loaded: {:#?}", config_file);
        let spawn_clickhouse = config_json
            .get("clickhouse")
            .unwrap()
            .get("spawn")
            .unwrap()
            .as_bool()
            .unwrap_or(true);
        let threads = config_json
            .get("jetstreamer")
            .unwrap()
            .get("threads")
            .unwrap()
            .as_u64()
            .unwrap_or(1) as u8;
        log::info!("jetstreamer threads: {}", threads);
        let slot_range = config_json
            .get("jetstreamer")
            .unwrap()
            .get("slot_range")
            .unwrap()
            .as_str()
            .unwrap();
        let slot_range = parse_range(slot_range).unwrap();
        log::info!("jetstreamer slot range: {:?}", slot_range);
        SLOT_RANGE.set(slot_range.clone()).unwrap();

        // init subranges + thread info
        let sub_ranges = generate_subranges(&slot_range, threads);
        log::info!("jetstreamer slot sub-ranges: {:?}", sub_ranges);
        let mut thread_info_map = RangeMap::new();
        sub_ranges.iter().enumerate().for_each(|(i, range)| {
            let thread_id = i as u8;
            let inclusive_end = range.end - 1;
            let initial_current_slot = range.start.checked_sub(1).unwrap_or(u64::MAX);

            thread_info_map.insert(range.clone(), thread_id);
            thread_set_slot_range(thread_id, range.start, inclusive_end);
            thread_set_current_slot(thread_id, initial_current_slot);
            thread_set_current_tx_index(thread_id, 0);

            // Ensure that the range is fully covered since we can't use RangeInclusive
            assert!(*thread_info_map.get(&range.start).unwrap() == thread_id);
            assert!(*thread_info_map.get(&inclusive_end).unwrap() == thread_id);
        });
        log::info!("thread info map: {:#?}", thread_info_map);
        THREAD_INFO.set(thread_info_map).unwrap();

        // start tokio runtime + spawn clickhouse if enabled + initialize IPC bridge
        TOKIO_RUNTIME
            .with(|rt_cell| {
                let rt = rt_cell.borrow();
                rt.block_on(async {
                    if spawn_clickhouse {
                        // Check if ClickHouse has already been initialized
                        if CLICKHOUSE_INITIALIZED.load(Ordering::Relaxed) {
                            log::info!("ClickHouse already initialized, skipping spawn.");
                        } else {
                            log::info!("automatic ClickHouse spawning enabled, starting ClickHouse...");
                            let start_result = clickhouse_utils::start().await
                                .map_err(|e| JetstreamerError::ClickHouseError(e.to_string()))?;

                            let (mut ready_rx, clickhouse_future) = start_result;

                            if ready_rx.recv().await.is_some() {
                                log::info!("ClickHouse initialization complete.");
                                rt.spawn(clickhouse_future);
                                CLICKHOUSE_INITIALIZED.store(true, Ordering::Relaxed);
                            } else {
                                return Err(JetstreamerError::ClickHouseInitializationFailed);
                            }
                        }
                    } else {
                        log::info!("automatic ClickHouse spawning disabled, skipping ClickHouse initialization.");
                    }

                    #[cfg(feature = "plugin-runner")]
                    {
                        log::info!("setting up IPC bridges...");
                        let mut ipc_senders = Vec::new();
                        let mut ipc_tasks = Vec::new();

                        for socket_id in 0..threads {
                            let (sender, receiver): (Sender<JetstreamerMessage>, Receiver<JetstreamerMessage>) = bounded(1);
                            let handle = spawn_socket_server(receiver, socket_id as usize)
                                .await
                                .map_err(|e| JetstreamerError::ClickHouseError(e.to_string()))?;
                            ipc_senders.push(sender);
                            ipc_tasks.push(handle);
                        }

                        IPC_SENDERS.set(ipc_senders).unwrap();
                        IPC_TASKS.set(ipc_tasks).unwrap();
                        log::info!("IPC bridges initialized.");
                    }

                    #[cfg(not(feature = "plugin-runner"))]
                    {
                        log::info!("initializing program tracking plugin...");
                        let db = DB_CLIENT.with_borrow(|db| db.clone());
                        let plugin = PLUGIN.with_borrow(|plugin| plugin.clone());
                        if let Err(e) = plugin.on_load(db).await {
                            log::error!("Failed to initialize plugin: {}", e);
                            return Err(JetstreamerError::ClickHouseError(e.to_string()));
                        }
                        log::info!("program tracking plugin initialized.");
                    }

                    START_TIME.set(Instant::now()).unwrap();
                    log::info!("jetstreamer loaded");

                    ctrlc::set_handler(|| {
                        unload();
                    })
                    .unwrap();
                    Ok(())
                })
            })
            .map_err(|e| {
                log::error!("Error loading jetstreamer: {:?}", e);
                GeyserPluginError::from(e)
            })
    }

    fn on_unload(&mut self) {
        unload();
    }

    fn notify_block_metadata(&self, blockinfo: ReplicaBlockInfoVersions) -> Result<()> {
        if exiting() {
            return Ok(());
        }
        let slot = match blockinfo {
            ReplicaBlockInfoVersions::V0_0_1(block_info) => block_info.slot,
            ReplicaBlockInfoVersions::V0_0_2(block_info) => block_info.slot,
            ReplicaBlockInfoVersions::V0_0_3(block_info) => block_info.slot,
            ReplicaBlockInfoVersions::V0_0_4(block_info) => block_info.slot,
        };

        let range_map = THREAD_INFO.get().unwrap();
        let thread_id = *range_map.get(&slot).unwrap();
        let last_slot = thread_current_slot(thread_id);

        // Per-thread progress accounting ---------------------------------------------------------
        let range_start = thread_slot_range_start(thread_id);
        let range_end = thread_slot_range_end(thread_id);
        let thread_total = if range_start == u64::MAX || range_end == u64::MAX {
            0
        } else {
            range_end.saturating_sub(range_start) + 1
        };

        // Compute new contribution from this notification for this thread only.
        let raw_increment = if last_slot == u64::MAX {
            // First observed slot for this thread (may jump forward). Only count in-range distance.
            if slot >= range_start {
                slot - range_start + 1
            } else {
                0
            }
        } else if slot > last_slot {
            slot - last_slot
        } else if slot == last_slot {
            0 // duplicate
        } else {
            // Backwards restart: count this slot if within range and we previously had u64::MAX (handled above)
            if last_slot - slot > 1000 {
                log::warn!(
                    "Thread {} restarted: went from slot {} back to {} (gap: {})",
                    thread_id,
                    last_slot,
                    slot,
                    last_slot - slot
                );
            }
            0
        };

        // Cap per-thread total at its assigned range length to avoid over-counting when jumps skip gaps.
        let mut applied_increment = 0u64;
        if raw_increment > 0 && thread_total > 0 {
            let current_total = THREAD_PROCESSED_SLOTS[thread_id as usize].load(Ordering::SeqCst);
            if current_total < thread_total {
                let remaining = thread_total - current_total;
                applied_increment = raw_increment.min(remaining);
                if applied_increment > 0 {
                    THREAD_PROCESSED_SLOTS[thread_id as usize]
                        .fetch_add(applied_increment, Ordering::SeqCst);
                }
            }
        }
        if applied_increment > 1000 {
            log::debug!(
                "Thread {} large applied increment: {} (raw: {}, last_slot: {}, new_slot: {}, range: {}..={})",
                thread_id,
                applied_increment,
                raw_increment,
                if last_slot == u64::MAX { 0 } else { last_slot },
                slot,
                range_start,
                range_end
            );
        }

        // Incremental global processed slot accounting to reduce contention.
        let processed_slots = if applied_increment > 0 {
            // Update last progress timestamp (nanos since start)
            if let Some(start) = START_TIME.get() {
                let nanos = start.elapsed().as_nanos() as u64;
                THREAD_LAST_PROGRESS_NS[thread_id as usize].store(nanos, Ordering::Relaxed);
            }
            PROCESSED_SLOTS.fetch_add(applied_increment, Ordering::AcqRel) + applied_increment
        } else {
            PROCESSED_SLOTS.load(Ordering::Acquire)
        };
        thread_set_current_slot(thread_id, slot);
        let range_end = thread_slot_range_end(thread_id);

        if slot >= range_end || processed_slots % 100 == 0 {
            let processed_txs = PROCESSED_TRANSACTIONS.load(Ordering::SeqCst);
            let num_votes = NUM_VOTES.load(Ordering::SeqCst);

            maybe_update_clickhouse_tps(processed_slots);
            let overall_tps = {
                let cached = CLICKHOUSE_TPS_CACHE_MICRO.load(Ordering::Relaxed);
                if cached > 0 {
                    cached as f64 / 1_000_000.0
                } else {
                    let start_time = START_TIME.get().unwrap();
                    let elapsed = start_time.elapsed().as_secs_f64();
                    processed_txs as f64 / elapsed
                }
            };

            let epoch = slot_to_epoch(slot);

            let overall_slot_range = SLOT_RANGE.get().unwrap();
            let overall_total_slots = overall_slot_range.end - overall_slot_range.start;
            // processed_slots counts individual slot progressions across all threads
            // overall_total_slots is the total slot range being processed
            // These are the same value, so progress should never exceed 100%
            let overall_percent = processed_slots as f64 / overall_total_slots as f64 * 100.0;

            // Calculate estimated time remaining based on slowest thread
            let start_time = START_TIME.get().unwrap();
            let elapsed = start_time.elapsed();

            // Find the slowest thread (minimum progress percentage)
            let mut slowest_progress = 100.0f64;
            let mut slowest_thread_id = 255u8;
            let mut active_threads = 0u32;

            for thread_id in 0u8..=255 {
                let current_slot = THREAD_CURRENT_SLOT[thread_id as usize].load(Ordering::SeqCst);
                let range_start =
                    THREAD_SLOT_RANGE_START[thread_id as usize].load(Ordering::SeqCst);
                let range_end = THREAD_SLOT_RANGE_END[thread_id as usize].load(Ordering::SeqCst);

                // Skip threads that haven't been initialized or are inactive
                if current_slot == u64::MAX || range_start == u64::MAX || range_end == u64::MAX {
                    continue;
                }

                active_threads += 1;
                let thread_total_slots = range_end - range_start + 1;
                let thread_slots_processed = if current_slot == u64::MAX {
                    // Thread hasn't started yet
                    0
                } else if current_slot < range_start {
                    // Thread is at its initial position (range_start - 1)
                    0
                } else {
                    // Thread has processed slots from range_start to current_slot (inclusive)
                    (current_slot - range_start + 1).min(thread_total_slots)
                };
                let thread_progress =
                    thread_slots_processed as f64 / thread_total_slots as f64 * 100.0;

                // Sanity check for thread progress
                if thread_progress > 100.0 {
                    log::warn!(
                        "Thread {} has invalid progress: {:.2}% ({} processed / {} total, current_slot: {}, range: {}..{})",
                        thread_id,
                        thread_progress,
                        thread_slots_processed,
                        thread_total_slots,
                        current_slot,
                        range_start,
                        range_end
                    );
                    continue;
                }

                if thread_progress < slowest_progress {
                    slowest_progress = thread_progress;
                    slowest_thread_id = thread_id;
                }
            }

            let estimated_time_remaining = {
                // Calculate ETA based on slowest thread progress
                if slowest_progress > 0.0 && active_threads > 0 {
                    let estimated_total_duration =
                        elapsed.as_secs_f64() / (slowest_progress / 100.0);
                    let remaining_seconds = estimated_total_duration - elapsed.as_secs_f64();
                    let remaining_duration = if remaining_seconds > 0.0 {
                        Duration::from_secs_f64(remaining_seconds)
                    } else {
                        Duration::ZERO
                    };
                    DurationFormatter::new(remaining_duration)
                } else {
                    DurationFormatter::new(Duration::ZERO)
                }
            };

            let thread_slot_range_start = thread_slot_range_start(thread_id);
            let thread_total_slots = range_end - thread_slot_range_start + 1;

            // Use consistent logic for both ETA and display calculations
            let thread_slots_processed = if slot < thread_slot_range_start {
                // Thread is at its initial position (range_start - 1) due to restart
                0
            } else if slot > range_end {
                // Thread has completed its range
                thread_total_slots
            } else {
                // Thread has processed slots from range_start to current slot (exclusive)
                slot - thread_slot_range_start + 1
            };
            let thread_percent = thread_slots_processed as f64 / thread_total_slots as f64 * 100.0;

            log::info!(
                "T{} {}:{} | {}/{} ({:.2}%) | overall {}/{} ({:.2}%) | {} txs ({} non-vote) | TPS: {:.3} | ETA: {} (slowest: T{}@{:.1}%)",
                thread_id,
                epoch,
                slot,
                thread_slots_processed.separate_with_commas(),
                thread_total_slots.separate_with_commas(),
                thread_percent,
                processed_slots.separate_with_commas(),
                overall_total_slots.separate_with_commas(),
                overall_percent,
                processed_txs.separate_with_commas(),
                (processed_txs - num_votes).separate_with_commas(),
                overall_tps,
                estimated_time_remaining,
                slowest_thread_id,
                slowest_progress
            );
        }

        let blk = Block::from_replica(blockinfo);

        let transaction_count = thread_current_tx_index(thread_id) + 1;

        ipc_send(
            thread_id as usize,
            JetstreamerMessage::Block(blk.clone(), transaction_count),
        );

        DB_CLIENT.with_borrow(|db| {
            TOKIO_RUNTIME.with_borrow(|rt| {
                rt.block_on(async {
                    #[derive(Copy, Clone, serde::Serialize, ::clickhouse::Row)]
                    struct SlotStatusRow {
                        slot: u64,
                        transaction_count: u32,
                        thread_id: u8,
                    }

                    match db.insert("jetstreamer_slot_status") {
                        Ok(mut insert) => {
                            if let Err(e) = insert
                                .write(&SlotStatusRow {
                                    slot: blk.slot,
                                    transaction_count,
                                    thread_id: thread_id as u8,
                                })
                                .await
                            {
                                log::error!(
                                    "slot_status write error slot={} thread={} err={}",
                                    blk.slot,
                                    thread_id,
                                    e
                                );
                            }
                            if let Err(e) = insert.end().await {
                                log::error!(
                                    "slot_status end error slot={} thread={} err={}",
                                    blk.slot,
                                    thread_id,
                                    e
                                );
                            } /*else {
                            log::info!(
                            "slot_status inserted slot={} txs={} thread={}",
                            blk.slot,
                            transaction_count,
                            thread_id,
                            );
                            }*/
                        }
                        Err(e) => {
                            log::error!(
                                "slot_status inserter init failed slot={} thread={} err={}",
                                blk.slot,
                                thread_id,
                                e
                            );
                        }
                    }
                });
            });
        });

        thread_set_current_tx_index(thread_id, 0);

        if slot >= range_end {
            log::info!(
                "thread {} finished processing slot {} and has completed its work",
                thread_id,
                slot
            );

            let complete_threads = COMPLETE_THREADS.fetch_add(1, Ordering::SeqCst) + 1;
            let jetstreamer_threads = range_map.len() as u8;
            let total_slots = {
                let slot_range = SLOT_RANGE.get().unwrap();
                slot_range.end - slot_range.start
            };

            // Only unload once ALL threads have completed their ranges (or have been explicitly marked complete).
            if complete_threads >= jetstreamer_threads {
                log::info!("all work has completed, unloading jetstreamer...");
                // Now send Exit IPC once to notify client(s) after all threads complete.
                ipc_send(thread_id as usize, JetstreamerMessage::Exit);
                unload();
            } else {
                log::info!(
                    "waiting for {} more threads to complete their work ({} total processed / {} total slots)",
                    jetstreamer_threads - complete_threads,
                    processed_slots,
                    total_slots
                );

                // Debug: Log status of incomplete threads
                if let Some(start) = START_TIME.get() {
                    let now_ns = start.elapsed().as_nanos() as u64;
                    for (_slot, thread_id) in range_map.iter() {
                        let current_slot = thread_current_slot(*thread_id);
                        let r_start = thread_slot_range_start(*thread_id);
                        let r_end = thread_slot_range_end(*thread_id);
                        if current_slot < r_end {
                            let progress = if current_slot == u64::MAX {
                                0.0
                            } else if current_slot > r_end {
                                100.0
                            } else {
                                (current_slot - r_start + 1) as f64 / (r_end - r_start + 1) as f64
                                    * 100.0
                            };
                            let last_ns = THREAD_LAST_PROGRESS_NS[*thread_id as usize]
                                .load(Ordering::Relaxed);
                            let stale_ms = if last_ns == 0 {
                                0
                            } else {
                                (now_ns.saturating_sub(last_ns)) / 1_000_000
                            };
                            log::info!(
                                "Thread {} incomplete: current_slot={}, range={}..{}, progress={:.2}%, last_progress={}ms ago",
                                thread_id,
                                current_slot,
                                r_start,
                                r_end,
                                progress,
                                stale_ms
                            );
                        }
                    }
                }
            }
        }

        // Tail-phase CPU friendliness: if most threads have finished, yield occasionally to let stragglers progress.
        std::thread::yield_now();

        Ok(())
    }

    fn notify_transaction(
        &self,
        transaction: ReplicaTransactionInfoVersions,
        slot: u64,
    ) -> Result<()> {
        if exiting() {
            return Ok(());
        }
        PROCESSED_TRANSACTIONS.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let tx = Transaction::from_replica(slot, transaction);
        if tx.is_vote {
            NUM_VOTES.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
        let thread_id = *THREAD_INFO
            .get()
            .unwrap()
            .get(&slot)
            .expect("thread id not found for slot");
        let thread_current_tx_index = thread_bump_tx_index(thread_id);
        ipc_send(
            thread_id as usize,
            JetstreamerMessage::Transaction(tx, thread_current_tx_index),
        );
        Ok(())
    }

    fn transaction_notifications_enabled(&self) -> bool {
        true
    }

    fn account_data_notifications_enabled(&self) -> bool {
        true
    }

    fn entry_notifications_enabled(&self) -> bool {
        true
    }
}

#[inline(always)]
fn exiting() -> bool {
    EXIT.load(std::sync::atomic::Ordering::SeqCst)
}

#[cfg(feature = "plugin-runner")]
#[inline(always)]
fn stop_ipc_bridge() {
    log::info!("stopping IPC bridges...");
    if let Some(handles) = IPC_TASKS.get() {
        for handle in handles {
            handle.abort(); // kills writer + accept loops
        }
    }
    log::info!("IPC bridges stopped.");
}

#[inline(always)]
fn stop_tx_queue() {
    log::info!("stopping queueing of transactions...");
    EXIT.store(true, std::sync::atomic::Ordering::SeqCst);
    log::info!("queueing of transactions has stopped.");
}

#[cfg(feature = "plugin-runner")]
#[inline]
fn send_exit_signal_to_clients() {
    log::info!("sending exit signal to clients...");
    let senders = IPC_SENDERS.get().expect("IPC_SENDERS not initialized");
    for (thread_id, _) in senders.iter().enumerate() {
        log::info!("sending exit signal to client socket {}", thread_id);
        ipc_send(thread_id, JetstreamerMessage::Exit);
    }
    log::info!("exit signal sent to clients.");
}

#[inline(always)]
fn stop_clickhouse() {
    log::info!("stopping ClickHouse...");
    clickhouse_utils::stop_sync();
    log::info!("ClickHouse stopped.");
}

#[cfg(feature = "plugin-runner")]
#[inline(always)]
fn clear_domain_sockets() {
    log::info!("clearing domain sockets...");
    let senders = IPC_SENDERS.get().expect("IPC_SENDERS not initialized");
    for socket_id in 0..senders.len() {
        let socket_path = format!("/tmp/jetstreamer_{}.sock", socket_id);
        let _ = std::fs::remove_file(&socket_path);
    }
    log::info!("domain sockets cleared.");
}

#[inline(always)]
fn unload() {
    log::info!("jetstreamer unloading...");
    stop_tx_queue();
    #[cfg(feature = "plugin-runner")]
    send_exit_signal_to_clients();
    #[cfg(feature = "plugin-runner")]
    stop_ipc_bridge();
    stop_clickhouse();
    #[cfg(feature = "plugin-runner")]
    clear_domain_sockets();
    log::info!("jetstreamer has successfully unloaded.");
    std::process::exit(0);
}

pub trait SolscanUrl {
    /// Returns the Solscan URL associated with `self`
    fn solscan_url(&self) -> String;
}

impl SolscanUrl for ReplicaTransactionInfoVersions<'_> {
    fn solscan_url(&self) -> String {
        match self {
            ReplicaTransactionInfoVersions::V0_0_1(tx) => {
                format!("https://solscan.io/tx/{}", tx.signature)
            }
            ReplicaTransactionInfoVersions::V0_0_2(tx) => {
                format!("https://solscan.io/tx/{}", tx.signature)
            }
        }
    }
}
