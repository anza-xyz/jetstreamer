#![deny(missing_docs)]
//! Trait-based framework for building structured observers on top of Jetstreamer's firehose.
//!
//! # Overview
//! Plugins let you react to every block, transaction, reward, entry, and stats update emitted by
//! [`jetstreamer_firehose`](https://crates.io/crates/jetstreamer-firehose). Combined with the
//! [`JetstreamerRunner`](https://docs.rs/jetstreamer/latest/jetstreamer/struct.JetstreamerRunner.html),
//! they provide a high-throughput analytics pipeline capable of exceeding 2.7 million transactions
//! per second on the right hardware. All events originate from Old Faithful's CAR archive and are
//! streamed over the network into your local runner.
//!
//! The framework offers:
//! - A [`Plugin`] trait with async hook points for each data type.
//! - [`PluginRunner`] for coordinating multiple plugins with shared ClickHouse connections
//!   (used internally by `JetstreamerRunner`).
//! - Built-in plugins under [`plugins`] that demonstrate common batching strategies and metrics.
//! - See `JetstreamerRunner` in the `jetstreamer` crate for the easiest way to run plugins.
//!
//! # ClickHouse Integration
//! Jetstreamer plugins are typically paired with ClickHouse for persistence. Runner instances honour
//! the following environment variables:
//! - `JETSTREAMER_CLICKHOUSE_DSN` (default `http://localhost:8123`): HTTP(S) DSN handed to every
//!   plugin that requests a database handle.
//! - `JETSTREAMER_CLICKHOUSE_MODE` (default `auto`): toggles the bundled ClickHouse helper. Set to
//!   `remote` to opt out of spawning the helper while still writing to a cluster, `local` to always
//!   spawn, or `off` to disable ClickHouse entirely.
//!
//! When the mode is `auto`, Jetstreamer inspects the DSN at runtime and only launches the embedded
//! helper for local endpoints, enabling native clustering workflows out of the box.
//!
//! # Examples
//! ## Defining a Plugin
//! ```no_run
//! use std::sync::Arc;
//! use clickhouse::Client;
//! use futures_util::FutureExt;
//! use jetstreamer_firehose::firehose::TransactionData;
//! use jetstreamer_plugin::{Plugin, PluginFuture};
//!
//! struct CountingPlugin;
//!
//! impl Plugin for CountingPlugin {
//!     fn name(&self) -> &'static str { "counting" }
//!
//!     fn on_transaction<'a>(
//!         &'a self,
//!         _thread_id: usize,
//!         _db: Option<Arc<Client>>,
//!         transaction: &'a TransactionData,
//!     ) -> PluginFuture<'a> {
//!         async move {
//!             println!("saw tx {} in slot {}", transaction.signature, transaction.slot);
//!             Ok(())
//!         }
//!         .boxed()
//!     }
//! }
//! # let _plugin = CountingPlugin;
//! ```
//!
//! ## Running Plugins with `PluginRunner`
//! ```no_run
//! use std::sync::Arc;
//! use jetstreamer_firehose::epochs;
//! use jetstreamer_plugin::{Plugin, PluginRunner};
//!
//! struct LoggingPlugin;
//!
//! impl Plugin for LoggingPlugin {
//!     fn name(&self) -> &'static str { "logging" }
//! }
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let mut runner = PluginRunner::new("http://localhost:8123", 1);
//!     runner.register(Box::new(LoggingPlugin));
//!     let runner = Arc::new(runner);
//!
//!     let (start, _) = epochs::epoch_to_slot_range(800);
//!     let (_, end_inclusive) = epochs::epoch_to_slot_range(805);
//!     runner
//!         .clone()
//!         .run(start..(end_inclusive + 1), false)
//!         .await?;
//!     Ok(())
//! }
//! ```

/// Built-in plugin implementations that ship with Jetstreamer.
pub mod plugins;

use std::{
    fmt::Display,
    future::Future,
    ops::Range,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use clickhouse::{Client, Row};
use dashmap::DashMap;
use futures_util::FutureExt;
use jetstreamer_firehose::firehose::{
    BlockData, EntryData, RewardsData, Stats, StatsTracking, TransactionData, firehose,
};
use serde::Serialize;
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::{signal, sync::broadcast};

#[derive(Clone, Copy)]
struct Snapshot {
    time: std::time::Instant,
    slots: u64,
    txs: u64,
}

#[derive(Clone, Copy, Default)]
struct Contribution {
    slot_delta: u64,
    tx_delta: u64,
    dt: f64,
}

#[derive(Clone, Copy, Default)]
struct Rates {
    slot_rate: f64,
    tx_rate: f64,
}

#[derive(Default)]
struct SnapshotWindow {
    entries: [Option<Contribution>; 5],
    idx: usize,
    len: usize,
    last: Option<Snapshot>,
}

impl SnapshotWindow {
    fn update(&mut self, snapshot: Snapshot) -> Option<Rates> {
        if let Some(prev) = self.last
            && let Some(dt) = snapshot
                .time
                .checked_duration_since(prev.time)
                .map(|d| d.as_secs_f64())
            && dt > 0.0
        {
            let contrib = Contribution {
                slot_delta: snapshot.slots.saturating_sub(prev.slots),
                tx_delta: snapshot.txs.saturating_sub(prev.txs),
                dt,
            };
            self.entries[self.idx] = Some(contrib);
            self.idx = (self.idx + 1) % self.entries.len();
            if self.len < self.entries.len() {
                self.len += 1;
            }
        }
        self.last = Some(snapshot);

        let mut total_slots = 0u64;
        let mut total_txs = 0u64;
        let mut total_dt = 0.0;
        for entry in self.entries.iter().flatten() {
            total_slots = total_slots.saturating_add(entry.slot_delta);
            total_txs = total_txs.saturating_add(entry.tx_delta);
            total_dt += entry.dt;
        }

        if self.len < self.entries.len() || total_dt <= 0.0 {
            return None;
        }

        Some(Rates {
            slot_rate: total_slots as f64 / total_dt,
            tx_rate: total_txs as f64 / total_dt,
        })
    }
}

/// Re-exported statistics types produced by [`firehose`].
pub use jetstreamer_firehose::firehose::{Stats as FirehoseStats, ThreadStats};

/// Convenience alias for the boxed future returned by plugin hooks.
pub type PluginFuture<'a> = Pin<
    Box<
        dyn Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync + 'static>>>
            + Send
            + 'a,
    >,
>;

/// Trait implemented by plugins that consume firehose events.
///
/// See the crate-level documentation for usage examples.
pub trait Plugin: Send + Sync + 'static {
    /// Human-friendly plugin name used in logs and persisted metadata.
    fn name(&self) -> &'static str;

    /// Semantic version for the plugin; defaults to `1`.
    fn version(&self) -> u16 {
        1
    }

    /// Deterministic identifier derived from [`Plugin::name`].
    fn id(&self) -> u16 {
        let hash = Sha256::digest(self.name());
        let mut res = 1u16;
        for byte in hash {
            res = res.wrapping_mul(31).wrapping_add(byte as u16);
        }
        res
    }

    /// Called for every transaction seen by the firehose.
    fn on_transaction<'a>(
        &'a self,
        _thread_id: usize,
        _db: Option<Arc<Client>>,
        _transaction: &'a TransactionData,
    ) -> PluginFuture<'a> {
        async move { Ok(()) }.boxed()
    }

    /// Called for every block observed by the firehose.
    fn on_block<'a>(
        &'a self,
        _thread_id: usize,
        _db: Option<Arc<Client>>,
        _block: &'a BlockData,
    ) -> PluginFuture<'a> {
        async move { Ok(()) }.boxed()
    }

    /// Called for every entry observed by the firehose when entry notifications are enabled.
    fn on_entry<'a>(
        &'a self,
        _thread_id: usize,
        _db: Option<Arc<Client>>,
        _entry: &'a EntryData,
    ) -> PluginFuture<'a> {
        async move { Ok(()) }.boxed()
    }

    /// Called for reward updates associated with processed blocks.
    fn on_reward<'a>(
        &'a self,
        _thread_id: usize,
        _db: Option<Arc<Client>>,
        _reward: &'a RewardsData,
    ) -> PluginFuture<'a> {
        async move { Ok(()) }.boxed()
    }

    /// Invoked once before the firehose starts streaming events.
    fn on_load(&self, _db: Option<Arc<Client>>) -> PluginFuture<'_> {
        async move { Ok(()) }.boxed()
    }

    /// Invoked once after the firehose finishes or shuts down.
    fn on_exit(&self, _db: Option<Arc<Client>>) -> PluginFuture<'_> {
        async move { Ok(()) }.boxed()
    }
}

/// Coordinates plugin execution and ClickHouse persistence.
///
/// See the crate-level documentation for usage examples.
#[derive(Clone)]
pub struct PluginRunner {
    plugins: Arc<Vec<Arc<dyn Plugin>>>,
    clickhouse_dsn: String,
    num_threads: usize,
    db_update_interval_slots: u64,
}

impl PluginRunner {
    /// Creates a new runner that writes to `clickhouse_dsn` using `num_threads`.
    pub fn new(clickhouse_dsn: impl Display, num_threads: usize) -> Self {
        Self {
            plugins: Arc::new(Vec::new()),
            clickhouse_dsn: clickhouse_dsn.to_string(),
            num_threads: std::cmp::max(1, num_threads),
            db_update_interval_slots: 100,
        }
    }

    /// Registers an additional plugin.
    pub fn register(&mut self, plugin: Box<dyn Plugin>) {
        Arc::get_mut(&mut self.plugins)
            .expect("cannot register plugins after the runner has started")
            .push(Arc::from(plugin));
    }

    /// Runs the firehose across the specified slot range, optionally writing to ClickHouse.
    pub async fn run(
        self: Arc<Self>,
        slot_range: Range<u64>,
        clickhouse_enabled: bool,
    ) -> Result<(), PluginRunnerError> {
        let db_update_interval = self.db_update_interval_slots.max(1);
        let plugin_handles: Arc<Vec<PluginHandle>> = Arc::new(
            self.plugins
                .iter()
                .cloned()
                .map(PluginHandle::from)
                .collect(),
        );

        let clickhouse = if clickhouse_enabled {
            let client = Arc::new(
                Client::default()
                    .with_url(&self.clickhouse_dsn)
                    .with_option("async_insert", "1")
                    .with_option("wait_for_async_insert", "0"),
            );
            ensure_clickhouse_tables(client.as_ref()).await?;
            upsert_plugins(client.as_ref(), plugin_handles.as_ref()).await?;
            Some(client)
        } else {
            None
        };

        for handle in plugin_handles.iter() {
            if let Err(error) = handle
                .plugin
                .on_load(clickhouse.clone())
                .await
                .map_err(|e| e.to_string())
            {
                return Err(PluginRunnerError::PluginLifecycle {
                    plugin: handle.name,
                    stage: "on_load",
                    details: error,
                });
            }
        }

        let shutting_down = Arc::new(AtomicBool::new(false));
        let slot_buffer: Arc<DashMap<u16, Vec<PluginSlotRow>>> = Arc::new(DashMap::new());
        let clickhouse_enabled = clickhouse.is_some();
        let slots_since_flush = Arc::new(AtomicU64::new(0));

        let on_block = {
            let plugin_handles = plugin_handles.clone();
            let clickhouse = clickhouse.clone();
            let slot_buffer = slot_buffer.clone();
            let slots_since_flush = slots_since_flush.clone();
            let shutting_down = shutting_down.clone();
            move |thread_id: usize, block: BlockData| {
                let plugin_handles = plugin_handles.clone();
                let clickhouse = clickhouse.clone();
                let slot_buffer = slot_buffer.clone();
                let slots_since_flush = slots_since_flush.clone();
                let shutting_down = shutting_down.clone();
                async move {
                    if plugin_handles.is_empty() {
                        return Ok(());
                    }
                    if shutting_down.load(Ordering::SeqCst) {
                        log::debug!("ignoring block while shutdown is in progress");
                        return Ok(());
                    }
                    let block = Arc::new(block);
                    for handle in plugin_handles.iter() {
                        let db = clickhouse.clone();
                        if let Err(err) = handle
                            .plugin
                            .on_block(thread_id, db.clone(), block.as_ref())
                            .await
                        {
                            log::error!("plugin {} on_block error: {}", handle.name, err);
                            continue;
                        }
                        if let (Some(db_client), BlockData::Block { slot, .. }) =
                            (clickhouse.clone(), block.as_ref())
                        {
                            if clickhouse_enabled {
                                slot_buffer
                                    .entry(handle.id)
                                    .or_default()
                                    .push(PluginSlotRow {
                                        plugin_id: handle.id as u32,
                                        slot: *slot,
                                    });
                            } else if let Err(err) =
                                record_plugin_slot(db_client, handle.id, *slot).await
                            {
                                log::error!(
                                    "failed to record plugin slot for {}: {}",
                                    handle.name,
                                    err
                                );
                            }
                        }
                    }
                    if clickhouse_enabled {
                        let current = slots_since_flush
                            .fetch_add(1, Ordering::Relaxed)
                            .wrapping_add(1);
                        if current.is_multiple_of(db_update_interval)
                            && let Some(db_client) = clickhouse.clone()
                        {
                            let buffer = slot_buffer.clone();
                            tokio::spawn(async move {
                                if let Err(err) = flush_slot_buffer(db_client, buffer).await {
                                    log::error!("failed to flush buffered plugin slots: {}", err);
                                }
                            });
                        }
                    }
                    if let Some(db_client) = clickhouse.clone() {
                        match block.as_ref() {
                            BlockData::Block {
                                slot,
                                executed_transaction_count,
                                ..
                            } => {
                                if let Err(err) = record_slot_status(
                                    db_client,
                                    *slot,
                                    thread_id,
                                    *executed_transaction_count,
                                )
                                .await
                                {
                                    log::error!("failed to record slot status: {}", err);
                                }
                            }
                            BlockData::LeaderSkipped { slot } => {
                                if let Err(err) =
                                    record_slot_status(db_client, *slot, thread_id, 0).await
                                {
                                    log::error!("failed to record slot status: {}", err);
                                }
                            }
                        }
                    }
                    Ok(())
                }
                .boxed()
            }
        };

        let on_transaction = {
            let plugin_handles = plugin_handles.clone();
            let clickhouse = clickhouse.clone();
            let shutting_down = shutting_down.clone();
            move |thread_id: usize, transaction: TransactionData| {
                let plugin_handles = plugin_handles.clone();
                let clickhouse = clickhouse.clone();
                let shutting_down = shutting_down.clone();
                async move {
                    if plugin_handles.is_empty() {
                        return Ok(());
                    }
                    if shutting_down.load(Ordering::SeqCst) {
                        log::debug!("ignoring transaction while shutdown is in progress");
                        return Ok(());
                    }
                    let transaction = Arc::new(transaction);
                    for handle in plugin_handles.iter() {
                        if let Err(err) = handle
                            .plugin
                            .on_transaction(thread_id, clickhouse.clone(), transaction.as_ref())
                            .await
                        {
                            log::error!("plugin {} on_transaction error: {}", handle.name, err);
                        }
                    }
                    Ok(())
                }
                .boxed()
            }
        };

        let on_entry = {
            let plugin_handles = plugin_handles.clone();
            let clickhouse = clickhouse.clone();
            let shutting_down = shutting_down.clone();
            move |thread_id: usize, entry: EntryData| {
                let plugin_handles = plugin_handles.clone();
                let clickhouse = clickhouse.clone();
                let shutting_down = shutting_down.clone();
                async move {
                    if plugin_handles.is_empty() {
                        return Ok(());
                    }
                    if shutting_down.load(Ordering::SeqCst) {
                        log::debug!("ignoring entry while shutdown is in progress");
                        return Ok(());
                    }
                    let entry = Arc::new(entry);
                    for handle in plugin_handles.iter() {
                        if let Err(err) = handle
                            .plugin
                            .on_entry(thread_id, clickhouse.clone(), entry.as_ref())
                            .await
                        {
                            log::error!("plugin {} on_entry error: {}", handle.name, err);
                        }
                    }
                    Ok(())
                }
                .boxed()
            }
        };

        let on_reward = {
            let plugin_handles = plugin_handles.clone();
            let clickhouse = clickhouse.clone();
            let shutting_down = shutting_down.clone();
            move |thread_id: usize, reward: RewardsData| {
                let plugin_handles = plugin_handles.clone();
                let clickhouse = clickhouse.clone();
                let shutting_down = shutting_down.clone();
                async move {
                    if plugin_handles.is_empty() {
                        return Ok(());
                    }
                    if shutting_down.load(Ordering::SeqCst) {
                        log::debug!("ignoring reward while shutdown is in progress");
                        return Ok(());
                    }
                    let reward = Arc::new(reward);
                    for handle in plugin_handles.iter() {
                        if let Err(err) = handle
                            .plugin
                            .on_reward(thread_id, clickhouse.clone(), reward.as_ref())
                            .await
                        {
                            log::error!("plugin {} on_reward error: {}", handle.name, err);
                        }
                    }
                    Ok(())
                }
                .boxed()
            }
        };

        let total_slot_count = slot_range.end.saturating_sub(slot_range.start);

        let total_slot_count_capture = total_slot_count;
        let stats_tracking = clickhouse.clone().map(|_db| {
            let shutting_down = shutting_down.clone();
            let last_snapshot: Arc<Mutex<SnapshotWindow>> =
                Arc::new(Mutex::new(SnapshotWindow::default()));
            StatsTracking {
                on_stats: {
                    let last_snapshot = last_snapshot.clone();
                    let total_slot_count = total_slot_count_capture;
                    move |thread_id: usize, stats: Stats| {
                        let shutting_down = shutting_down.clone();
                        let last_snapshot = last_snapshot.clone();
                        async move {
                            if shutting_down.load(Ordering::SeqCst) {
                                log::debug!("skipping stats write during shutdown");
                                return Ok(());
                            }
                            let finish_at = stats
                                .finish_time
                                .unwrap_or_else(std::time::Instant::now);
                            let elapsed = finish_at.saturating_duration_since(stats.start_time);
                            let elapsed_secs = elapsed.as_secs_f64();
                            let mut tps = if elapsed_secs > 0.0 {
                                stats.transactions_processed as f64 / elapsed_secs
                            } else {
                                0.0
                            };
                            let thread_stats = &stats.thread_stats;
                            let processed_slots = stats.slots_processed.min(total_slot_count);
                            let progress_fraction = if total_slot_count > 0 {
                                processed_slots as f64 / total_slot_count as f64
                            } else {
                                1.0
                            };
                            let overall_progress = (progress_fraction * 100.0).clamp(0.0, 100.0);
                            let thread_total_slots = thread_stats
                                .slot_range
                                .end
                                .saturating_sub(thread_stats.slot_range.start);
                            let thread_progress = if thread_total_slots > 0 {
                                (thread_stats.slots_processed as f64 / thread_total_slots as f64)
                                    .clamp(0.0, 1.0)
                                    * 100.0
                            } else {
                                100.0
                            };
                            let mut overall_eta = None;
                            if let Ok(mut window) = last_snapshot.lock()
                                && let Some(rates) = window.update(Snapshot {
                                    time: finish_at,
                                    slots: stats.slots_processed,
                                    txs: stats.transactions_processed,
                                }) {
                                    tps = rates.tx_rate;
                                    let remaining_slots =
                                        total_slot_count.saturating_sub(processed_slots);
                                    if rates.slot_rate > 0.0 && remaining_slots > 0 {
                                        overall_eta = Some(human_readable_duration(
                                            remaining_slots as f64 / rates.slot_rate,
                                        ));
                                    }
                                }
                            if overall_eta.is_none() {
                                if progress_fraction > 0.0 && progress_fraction < 1.0 {
                                    if let Some(elapsed_total) = finish_at
                                        .checked_duration_since(stats.start_time)
                                        .map(|d| d.as_secs_f64())
                                        && elapsed_total > 0.0 {
                                            let remaining_secs =
                                                elapsed_total * (1.0 / progress_fraction - 1.0);
                                            overall_eta = Some(human_readable_duration(remaining_secs));
                                        }
                                } else if progress_fraction >= 1.0 {
                                    overall_eta = Some("0s".into());
                                }
                            }
                            log::info!(
                                "stats pulse: thread={thread_id} slots={} blocks={} txs={} tps={tps:.2} progress={overall_progress:.1}% thread_slots={} thread_txs={} thread_progress={thread_progress:.1}% eta={}",
                                processed_slots,
                                stats.blocks_processed,
                                stats.transactions_processed,
                                thread_stats.slots_processed,
                                thread_stats.transactions_processed,
                                overall_eta.unwrap_or_else(|| "n/a".into())
                            );
                            Ok(())
                        }
                        .boxed()
                    }
                },
                tracking_interval_slots: 100,
            }
        });

        let (shutdown_tx, _) = broadcast::channel::<()>(1);

        let mut firehose_future = Box::pin(firehose(
            self.num_threads as u64,
            slot_range,
            Some(on_block),
            Some(on_transaction),
            Some(on_entry),
            Some(on_reward),
            stats_tracking,
            Some(shutdown_tx.subscribe()),
        ));

        let firehose_result = tokio::select! {
            res = &mut firehose_future => res,
            ctrl = signal::ctrl_c() => {
                match ctrl {
                    Ok(()) => log::info!("CTRL+C received; initiating shutdown"),
                    Err(err) => log::error!("failed to listen for CTRL+C: {}", err),
                }
                shutting_down.store(true, Ordering::SeqCst);
                let _ = shutdown_tx.send(());
                firehose_future.await
            }
        };

        if clickhouse_enabled
            && let Some(db_client) = clickhouse.clone()
            && let Err(err) = flush_slot_buffer(db_client, slot_buffer.clone()).await
        {
            log::error!("failed to flush buffered plugin slots: {}", err);
        }

        for handle in plugin_handles.iter() {
            if let Err(error) = handle
                .plugin
                .on_exit(clickhouse.clone())
                .await
                .map_err(|e| e.to_string())
            {
                log::error!("plugin {} on_exit error: {}", handle.name, error);
            }
        }

        match firehose_result {
            Ok(()) => Ok(()),
            Err((error, slot)) => Err(PluginRunnerError::Firehose {
                details: error.to_string(),
                slot,
            }),
        }
    }
}

/// Errors that can arise while running plugins against the firehose.
#[derive(Debug, Error)]
pub enum PluginRunnerError {
    /// ClickHouse client returned an error.
    #[error("clickhouse error: {0}")]
    Clickhouse(#[from] clickhouse::error::Error),
    /// Firehose streaming failed at the specified slot.
    #[error("firehose error at slot {slot}: {details}")]
    Firehose {
        /// Human-readable description of the firehose failure.
        details: String,
        /// Slot where the firehose encountered the error.
        slot: u64,
    },
    /// Lifecycle hook on a plugin returned an error.
    #[error("plugin {plugin} failed during {stage}: {details}")]
    PluginLifecycle {
        /// Name of the plugin that failed.
        plugin: &'static str,
        /// Lifecycle stage where the failure occurred.
        stage: &'static str,
        /// Textual error details.
        details: String,
    },
}

#[derive(Clone)]
struct PluginHandle {
    plugin: Arc<dyn Plugin>,
    id: u16,
    name: &'static str,
    version: u16,
}

impl From<Arc<dyn Plugin>> for PluginHandle {
    fn from(plugin: Arc<dyn Plugin>) -> Self {
        let id = plugin.id();
        let name = plugin.name();
        let version = plugin.version();
        Self {
            plugin,
            id,
            name,
            version,
        }
    }
}

#[derive(Row, Serialize)]
struct PluginRow<'a> {
    id: u32,
    name: &'a str,
    version: u32,
}

#[derive(Row, Serialize)]
struct PluginSlotRow {
    plugin_id: u32,
    slot: u64,
}

#[derive(Row, Serialize)]
struct SlotStatusRow {
    slot: u64,
    transaction_count: u32,
    thread_id: u8,
}

async fn ensure_clickhouse_tables(db: &Client) -> Result<(), clickhouse::error::Error> {
    db.query(
        r#"CREATE TABLE IF NOT EXISTS jetstreamer_slot_status (
            slot UInt64,
            transaction_count UInt32 DEFAULT 0,
            thread_id UInt8 DEFAULT 0,
            indexed_at DateTime('UTC') DEFAULT now()
        ) ENGINE = ReplacingMergeTree
        ORDER BY (slot, thread_id)"#,
    )
    .execute()
    .await?;

    db.query(
        r#"CREATE TABLE IF NOT EXISTS jetstreamer_plugins (
            id UInt32,
            name String,
            version UInt32
        ) ENGINE = ReplacingMergeTree
        ORDER BY id"#,
    )
    .execute()
    .await?;

    db.query(
        r#"CREATE TABLE IF NOT EXISTS jetstreamer_plugin_slots (
            plugin_id UInt32,
            slot UInt64,
            indexed_at DateTime('UTC') DEFAULT now()
        ) ENGINE = ReplacingMergeTree
        ORDER BY (plugin_id, slot)"#,
    )
    .execute()
    .await?;

    Ok(())
}

async fn upsert_plugins(
    db: &Client,
    plugins: &[PluginHandle],
) -> Result<(), clickhouse::error::Error> {
    if plugins.is_empty() {
        return Ok(());
    }
    let mut insert = db.insert("jetstreamer_plugins")?;
    for handle in plugins {
        insert
            .write(&PluginRow {
                id: handle.id as u32,
                name: handle.name,
                version: handle.version as u32,
            })
            .await?;
    }
    insert.end().await?;
    Ok(())
}

async fn record_plugin_slot(
    db: Arc<Client>,
    plugin_id: u16,
    slot: u64,
) -> Result<(), clickhouse::error::Error> {
    let mut insert = db.insert("jetstreamer_plugin_slots")?;
    insert
        .write(&PluginSlotRow {
            plugin_id: plugin_id as u32,
            slot,
        })
        .await?;
    insert.end().await?;
    Ok(())
}

async fn flush_slot_buffer(
    db: Arc<Client>,
    buffer: Arc<DashMap<u16, Vec<PluginSlotRow>>>,
) -> Result<(), clickhouse::error::Error> {
    let mut rows = Vec::new();
    buffer.iter_mut().for_each(|mut entry| {
        if !entry.value().is_empty() {
            rows.append(entry.value_mut());
        }
    });

    if rows.is_empty() {
        return Ok(());
    }

    let mut insert = db.insert("jetstreamer_plugin_slots")?;
    for row in rows {
        insert.write(&row).await?;
    }
    insert.end().await?;
    Ok(())
}

async fn record_slot_status(
    db: Arc<Client>,
    slot: u64,
    thread_id: usize,
    transaction_count: u64,
) -> Result<(), clickhouse::error::Error> {
    let mut insert = db.insert("jetstreamer_slot_status")?;
    insert
        .write(&SlotStatusRow {
            slot,
            transaction_count: transaction_count.min(u32::MAX as u64) as u32,
            thread_id: thread_id.try_into().unwrap_or(u8::MAX),
        })
        .await?;
    insert.end().await?;
    Ok(())
}

// Ensure PluginRunnerError is Send + Sync + 'static
trait _CanSend: Send + Sync + 'static {}
impl _CanSend for PluginRunnerError {}

fn human_readable_duration(seconds: f64) -> String {
    if !seconds.is_finite() {
        return "n/a".into();
    }
    if seconds <= 0.0 {
        return "0s".into();
    }
    if seconds < 60.0 {
        return format!("{:.1}s", seconds);
    }
    let duration = Duration::from_secs(seconds.round() as u64);
    let secs = duration.as_secs();
    let days = secs / 86_400;
    let hours = (secs % 86_400) / 3_600;
    let minutes = (secs % 3_600) / 60;
    let seconds_rem = secs % 60;
    if days > 0 {
        if hours > 0 {
            format!("{}d{}h", days, hours)
        } else {
            format!("{}d", days)
        }
    } else if hours > 0 {
        format!("{}h{}m", hours, minutes)
    } else {
        format!("{}m{}s", minutes, seconds_rem)
    }
}
