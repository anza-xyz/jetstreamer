pub mod plugins;

use std::{
    fmt::Display,
    future::Future,
    ops::Range,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use clickhouse::{Client, Row};
use futures_util::FutureExt;
use jetstreamer_firehose::firehose::{
    BlockData, EntryData, RewardsData, Stats, StatsTracking, TransactionData, firehose,
};
use serde::Serialize;
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::{
    signal,
    sync::{Notify, Semaphore, broadcast},
    task,
};

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

#[derive(Default)]
struct SnapshotWindow {
    entries: [Option<Contribution>; 5],
    idx: usize,
    len: usize,
    last: Option<Snapshot>,
}

impl SnapshotWindow {
    fn update(&mut self, snapshot: Snapshot) -> (Option<f64>, Option<f64>) {
        if let Some(prev) = self.last {
            if let Some(dt) = snapshot
                .time
                .checked_duration_since(prev.time)
                .map(|d| d.as_secs_f64())
            {
                if dt > 0.0 {
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
            return (None, None);
        }

        (
            Some(total_slots as f64 / total_dt),
            Some(total_txs as f64 / total_dt),
        )
    }
}

pub use jetstreamer_firehose::firehose::{Stats as FirehoseStats, ThreadStats};

pub type PluginFuture<'a> = Pin<
    Box<
        dyn Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync + 'static>>>
            + Send
            + 'a,
    >,
>;

pub trait Plugin: Send + Sync + 'static {
    fn name(&self) -> &'static str;

    fn version(&self) -> u16 {
        1
    }

    fn id(&self) -> u16 {
        let hash = Sha256::digest(self.name());
        let mut res = 1u16;
        for byte in hash {
            res = res.wrapping_mul(31).wrapping_add(byte as u16);
        }
        res
    }

    fn on_transaction<'a>(
        &'a self,
        _thread_id: usize,
        _db: Option<Arc<Client>>,
        _transaction: &'a TransactionData,
    ) -> PluginFuture<'a> {
        async move { Ok(()) }.boxed()
    }

    fn on_block<'a>(
        &'a self,
        _thread_id: usize,
        _db: Option<Arc<Client>>,
        _block: &'a BlockData,
    ) -> PluginFuture<'a> {
        async move { Ok(()) }.boxed()
    }

    fn on_entry<'a>(
        &'a self,
        _thread_id: usize,
        _db: Option<Arc<Client>>,
        _entry: &'a EntryData,
    ) -> PluginFuture<'a> {
        async move { Ok(()) }.boxed()
    }

    fn on_reward<'a>(
        &'a self,
        _thread_id: usize,
        _db: Option<Arc<Client>>,
        _reward: &'a RewardsData,
    ) -> PluginFuture<'a> {
        async move { Ok(()) }.boxed()
    }

    fn on_load(&self, _db: Option<Arc<Client>>) -> PluginFuture<'_> {
        async move { Ok(()) }.boxed()
    }

    fn on_exit(&self, _db: Option<Arc<Client>>) -> PluginFuture<'_> {
        async move { Ok(()) }.boxed()
    }
}

#[derive(Clone)]
pub struct PluginRunner {
    plugins: Arc<Vec<Arc<dyn Plugin>>>,
    clickhouse_dsn: String,
    num_threads: usize,
}

impl PluginRunner {
    pub fn new(clickhouse_dsn: impl Display, num_threads: usize) -> Self {
        Self {
            plugins: Arc::new(Vec::new()),
            clickhouse_dsn: clickhouse_dsn.to_string(),
            num_threads: std::cmp::max(1, num_threads),
        }
    }

    pub fn register(&mut self, plugin: Box<dyn Plugin>) {
        Arc::get_mut(&mut self.plugins)
            .expect("cannot register plugins after the runner has started")
            .push(Arc::from(plugin));
    }

    pub async fn run(
        self: Arc<Self>,
        slot_range: Range<u64>,
        clickhouse_enabled: bool,
    ) -> Result<(), PluginRunnerError> {
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

        let concurrency = Arc::new(Semaphore::new(self.num_threads));
        let inflight = Arc::new(AtomicUsize::new(0));
        let notify = Arc::new(Notify::new());
        let shutting_down = Arc::new(AtomicBool::new(false));

        let on_block = {
            let plugin_handles = plugin_handles.clone();
            let clickhouse = clickhouse.clone();
            let concurrency = concurrency.clone();
            let inflight = inflight.clone();
            let notify = notify.clone();
            let shutting_down = shutting_down.clone();
            move |thread_id: usize,
                  block: BlockData|
                  -> Result<(), Box<dyn std::error::Error + Send>> {
                if plugin_handles.is_empty() {
                    return Ok(());
                }
                if shutting_down.load(Ordering::SeqCst) {
                    log::debug!("ignoring block while shutdown is in progress");
                    return Ok(());
                }
                inflight.fetch_add(1, Ordering::SeqCst);
                let plugin_handles = plugin_handles.clone();
                let clickhouse = clickhouse.clone();
                let concurrency = concurrency.clone();
                let inflight = inflight.clone();
                let notify = notify.clone();
                task::spawn(async move {
                    let permit = concurrency.acquire_owned().await.ok();
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
                            if let Err(err) = record_plugin_slot(db_client, handle.id, *slot).await
                            {
                                log::error!(
                                    "failed to record plugin slot for {}: {}",
                                    handle.name,
                                    err
                                );
                            }
                        }
                    }
                    drop(permit);
                    if inflight.fetch_sub(1, Ordering::SeqCst) == 1 {
                        notify.notify_waiters();
                    }
                });
                Ok(())
            }
        };

        let on_transaction = {
            let plugin_handles = plugin_handles.clone();
            let clickhouse = clickhouse.clone();
            let concurrency = concurrency.clone();
            let inflight = inflight.clone();
            let notify = notify.clone();
            let shutting_down = shutting_down.clone();
            move |thread_id: usize,
                  transaction: TransactionData|
                  -> Result<(), Box<dyn std::error::Error + Send>> {
                if plugin_handles.is_empty() {
                    return Ok(());
                }
                if shutting_down.load(Ordering::SeqCst) {
                    log::debug!("ignoring transaction while shutdown is in progress");
                    return Ok(());
                }
                inflight.fetch_add(1, Ordering::SeqCst);
                let plugin_handles = plugin_handles.clone();
                let clickhouse = clickhouse.clone();
                let concurrency = concurrency.clone();
                let inflight = inflight.clone();
                let notify = notify.clone();
                task::spawn(async move {
                    let permit = concurrency.acquire_owned().await.ok();
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
                    drop(permit);
                    if inflight.fetch_sub(1, Ordering::SeqCst) == 1 {
                        notify.notify_waiters();
                    }
                });
                Ok(())
            }
        };

        let on_entry = {
            let plugin_handles = plugin_handles.clone();
            let clickhouse = clickhouse.clone();
            let concurrency = concurrency.clone();
            let inflight = inflight.clone();
            let notify = notify.clone();
            let shutting_down = shutting_down.clone();
            move |thread_id: usize,
                  entry: EntryData|
                  -> Result<(), Box<dyn std::error::Error + Send>> {
                if plugin_handles.is_empty() {
                    return Ok(());
                }
                if shutting_down.load(Ordering::SeqCst) {
                    log::debug!("ignoring entry while shutdown is in progress");
                    return Ok(());
                }
                inflight.fetch_add(1, Ordering::SeqCst);
                let plugin_handles = plugin_handles.clone();
                let clickhouse = clickhouse.clone();
                let concurrency = concurrency.clone();
                let inflight = inflight.clone();
                let notify = notify.clone();
                task::spawn(async move {
                    let permit = concurrency.acquire_owned().await.ok();
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
                    drop(permit);
                    if inflight.fetch_sub(1, Ordering::SeqCst) == 1 {
                        notify.notify_waiters();
                    }
                });
                Ok(())
            }
        };

        let on_reward = {
            let plugin_handles = plugin_handles.clone();
            let clickhouse = clickhouse.clone();
            let concurrency = concurrency.clone();
            let inflight = inflight.clone();
            let notify = notify.clone();
            let shutting_down = shutting_down.clone();
            move |thread_id: usize,
                  reward: RewardsData|
                  -> Result<(), Box<dyn std::error::Error + Send>> {
                if plugin_handles.is_empty() {
                    return Ok(());
                }
                if shutting_down.load(Ordering::SeqCst) {
                    log::debug!("ignoring reward while shutdown is in progress");
                    return Ok(());
                }
                inflight.fetch_add(1, Ordering::SeqCst);
                let plugin_handles = plugin_handles.clone();
                let clickhouse = clickhouse.clone();
                let concurrency = concurrency.clone();
                let inflight = inflight.clone();
                let notify = notify.clone();
                task::spawn(async move {
                    let permit = concurrency.acquire_owned().await.ok();
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
                    drop(permit);
                    if inflight.fetch_sub(1, Ordering::SeqCst) == 1 {
                        notify.notify_waiters();
                    }
                });
                Ok(())
            }
        };

        let stats_tracking = clickhouse.clone().map(|db| {
            let inflight = inflight.clone();
            let notify = notify.clone();
            let concurrency = concurrency.clone();
            let shutting_down = shutting_down.clone();
            let last_snapshot: Arc<Mutex<SnapshotWindow>> =
                Arc::new(Mutex::new(SnapshotWindow::default()));
            StatsTracking {
                on_stats: {
                    let last_snapshot = last_snapshot.clone();
                    move |thread_id: usize, stats: Stats| {
                        if shutting_down.load(Ordering::SeqCst) {
                            return Ok(());
                        }
                        inflight.fetch_add(1, Ordering::SeqCst);
                        let db = db.clone();
                        let inflight = inflight.clone();
                        let notify = notify.clone();
                        let concurrency = concurrency.clone();
                        let shutting_down = shutting_down.clone();
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
                        let overall_total_slots =
                            stats.slot_range.end.saturating_sub(stats.slot_range.start);
                        let overall_progress = if overall_total_slots > 0 {
                            (stats.slots_processed as f64 / overall_total_slots as f64)
                                .clamp(0.0, 1.0)
                                * 100.0
                        } else {
                            100.0
                        };
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
                        let mut slot_rate = if elapsed_secs > 0.0 {
                            stats.slots_processed as f64 / elapsed_secs
                        } else {
                            0.0
                        };
                        if let Ok(mut window) = last_snapshot.lock() {
                            let (slot_opt, tx_opt) = window.update(Snapshot {
                                time: finish_at,
                                slots: stats.slots_processed,
                                txs: stats.transactions_processed,
                            });
                            if let Some(rate) = slot_opt {
                                slot_rate = rate;
                            }
                            if let Some(rate) = tx_opt {
                                tps = rate;
                            }
                        }
                        let remaining_slots = overall_total_slots.saturating_sub(stats.slots_processed);
                        let overall_eta = if slot_rate > 0.0 && remaining_slots > 0 {
                            Some(human_readable_duration(remaining_slots as f64 / slot_rate))
                        } else {
                            None
                        };
                        log::info!(
                            "stats pulse: thread={thread_id} slots={} blocks={} txs={} tps={tps:.2} progress={overall_progress:.1}% thread_slots={} thread_txs={} thread_progress={thread_progress:.1}% eta={}",
                            stats.slots_processed,
                            stats.blocks_processed,
                            stats.transactions_processed,
                            thread_stats.slots_processed,
                            thread_stats.transactions_processed,
                            overall_eta.unwrap_or_else(|| "n/a".into())
                        );
                        task::spawn(async move {
                            let permit = concurrency.acquire_owned().await.ok();
                            if shutting_down.load(Ordering::SeqCst) {
                                log::debug!("skipping stats write during shutdown");
                            } else if let Err(err) =
                                record_slot_status(db.clone(), stats.clone(), thread_id).await
                            {
                                log::error!("failed to record slot status: {}", err);
                            }
                            drop(permit);
                            if inflight.fetch_sub(1, Ordering::SeqCst) == 1 {
                                notify.notify_waiters();
                            }
                        });
                        Ok(())
                    }
                },
                tracking_interval_slots: 10,
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
                notify.notify_waiters();
                let _ = shutdown_tx.send(());
                firehose_future.await
            }
        };

        while inflight.load(Ordering::SeqCst) > 0 {
            notify.notified().await;
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

#[derive(Debug, Error)]
pub enum PluginRunnerError {
    #[error("clickhouse error: {0}")]
    Clickhouse(#[from] clickhouse::error::Error),
    #[error("firehose error at slot {slot}: {details}")]
    Firehose { details: String, slot: u64 },
    #[error("plugin {plugin} failed during {stage}: {details}")]
    PluginLifecycle {
        plugin: &'static str,
        stage: &'static str,
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

async fn record_slot_status(
    db: Arc<Client>,
    stats: Stats,
    thread_id: usize,
) -> Result<(), clickhouse::error::Error> {
    let mut insert = db.insert("jetstreamer_slot_status")?;
    insert
        .write(&SlotStatusRow {
            slot: stats.thread_stats.current_slot,
            transaction_count: stats
                .thread_stats
                .transactions_processed
                .min(u32::MAX as u64) as u32,
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
