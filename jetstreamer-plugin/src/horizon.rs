//! Horizon-native plugin framework — the zero-copy counterpart to the
//! Old-Faithful [`Plugin`](crate::Plugin) system.
//!
//! Plugins here consume horizon's own record types directly (`&Transaction`,
//! `&BlockNotification`, `&EpochMeta`) with no conversion back to Solana
//! runtime types, so the whole `.jet` → ClickHouse path is zero-copy. Each
//! `.jet` is read in parallel by [`firehose_horizon`]; one
//! [`PluginWorker`] is spawned per reader thread and owns its own mutable
//! state, so the hot path is lock-free. Workers accumulate rows and `flush`
//! them to an [`Output`] that hands inserts to a background async writer —
//! the decode threads never block on ClickHouse I/O.
use std::future::Future;
use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use clickhouse::Client;
use futures_util::FutureExt;

use jetstreamer_firehose::firehose_horizon::{JetSource, firehose_horizon};
use jetstreamer_horizon::archive::{BlockNotification, EntryRecord, EpochMeta, SlotVisitor};
use jetstreamer_horizon::transactions::Transaction;

pub use jetstreamer_horizon::archive::Consumption;

use crate::PluginFuture;

/// Slots a worker decodes between row flushes (bounds in-memory accumulation).
const FLUSH_INTERVAL_SLOTS: u32 = 1024;

const LOG_TARGET: &str = "jetstreamer_horizon_plugin";

/// Clamps a Solana `block_time` (Unix seconds, `i64`) to the `UInt32` ClickHouse
/// `DateTime` column. `None`/negative → 0; overflow → `u32::MAX`.
pub fn clamp_block_time(block_time: Option<i64>) -> u32 {
    match block_time {
        Some(t) if t < 0 => 0,
        Some(t) if t > u32::MAX as i64 => u32::MAX,
        Some(t) => t as u32,
        None => 0,
    }
}

/// Clamps a slot to the `UInt32` ClickHouse column used by these tables.
pub fn clamp_slot(slot: u64) -> u32 {
    slot.min(u32::MAX as u64) as u32
}

/// A boxed ClickHouse insert handed to the background writer.
type WriteJob = Pin<Box<dyn Future<Output = Result<(), clickhouse::error::Error>> + Send>>;

/// Insert jobs the queue holds before `submit` applies backpressure. Sized to
/// absorb a full flush burst (every worker flushing both plugins at once)
/// without stalling decode; beyond that, a ClickHouse that can't keep up is
/// *supposed* to slow ingestion rather than let unacknowledged work pile up
/// silently — that failure mode (unbounded async buildup, then dropped
/// batches) is exactly what this design retires.
const WRITE_QUEUE_CAP: usize = 128;

/// Inserts the writer runs concurrently. Durable acks
/// (`wait_for_async_insert=1`) make each insert wait for the server's flush,
/// so a strictly serial writer would bottleneck on flush latency; a small
/// pool keeps throughput without unbounded concurrency.
const MAX_CONCURRENT_INSERTS: usize = 8;

/// Sink handle given to a [`PluginWorker`] at flush time. Holds a ClickHouse
/// client for building inserts and a bounded channel to the background
/// writer: submission is non-blocking while the writer keeps up and applies
/// backpressure (briefly blocking the decode thread) when it doesn't.
#[derive(Clone)]
pub struct Output {
    tx: tokio::sync::mpsc::Sender<WriteJob>,
    db: Arc<Client>,
    /// Writes submitted but not yet completed by the writer. Lets a driver
    /// wait out the tail of the write pipeline (e.g. on ctrl-c) before
    /// stopping the ClickHouse server under it.
    backlog: Arc<AtomicU64>,
}

impl Output {
    /// A ClickHouse client handle for building an insert.
    pub fn db(&self) -> Arc<Client> {
        self.db.clone()
    }

    /// Hands an insert future to the background writer. Build it with
    /// [`Output::db`]; the future runs off the decode thread. Non-blocking
    /// while the write queue has room; otherwise blocks the calling (decode)
    /// thread until it does — deliberate backpressure. Must be called from a
    /// non-async context (plugin `flush` runs on blocking decode threads;
    /// the runner wraps its final flush pass in `block_in_place`).
    pub fn submit<Fut>(&self, job: Fut)
    where
        Fut: Future<Output = Result<(), clickhouse::error::Error>> + Send + 'static,
    {
        self.backlog.fetch_add(1, Ordering::SeqCst);
        if self.tx.blocking_send(Box::pin(job)).is_err() {
            // Writer already gone (aborted run); nothing will run this job.
            self.backlog.fetch_sub(1, Ordering::SeqCst);
        }
    }
}

/// Per-reader-thread plugin state. Mirrors horizon's [`SlotVisitor`] plus a
/// `flush`. Callbacks borrow the decoder's reusable scratch — valid only for
/// the call, so copy out what you keep. No async, no locking on the hot path.
///
/// Account updates are not a separate hook: they are bundled with the record
/// that produced them — tx-owned writes via [`Transaction::iter_account_updates`],
/// runtime-direct writes via the block's pre/post arenas, epoch writes via
/// [`EpochMeta`].
pub trait PluginWorker: Send {
    /// Epoch notification (boundary slots only).
    fn on_epoch(&mut self, _meta: &EpochMeta) {}
    /// One decoded transaction, with its nested account updates.
    fn on_transaction(&mut self, _slot: u64, _tx_index: u32, _tx: &Transaction) {}
    /// End of a slot frame: block notification (with grouped orphan updates)
    /// or a leader-skipped marker, plus the block's PoH entry records.
    fn on_block(&mut self, _notification: &BlockNotification, _entries: &[EntryRecord]) {}
    /// Emit accumulated rows to `out`. Called periodically while reading and
    /// once at the end.
    fn flush(&mut self, _out: &Output) {}
}

/// A horizon plugin: a shared factory that spawns one [`PluginWorker`] per
/// reader thread, plus async lifecycle hooks for table setup and backfill.
pub trait HorizonPlugin: Send + Sync + 'static {
    /// Human-friendly name used in logs.
    fn name(&self) -> &'static str;

    /// Plugin schema version; defaults to `1`.
    fn version(&self) -> u16 {
        1
    }

    /// Creates this plugin's per-thread worker. Called once per reader thread.
    fn spawn_worker(&self, thread_id: usize) -> Box<dyn PluginWorker>;

    /// Declares what this plugin's workers consume from the decoded stream.
    /// Defaults to everything (safe). Plugins that never read account-update
    /// data bytes should override to
    /// `Consumption::all().without_account_update_data()` — the runner
    /// combines all plugins' declarations, and when none consume update
    /// data the decoder skips the per-account diff reconstruction entirely
    /// (in state-heavy archives that is most of the decode work). With data
    /// dropped, updates still arrive with full metadata and correct counts,
    /// but every `data` slice is empty — a plugin that reads `data` (even
    /// just `data.len()`) must keep the default.
    fn consumption(&self) -> Consumption {
        Consumption::all()
    }

    /// Runs once before reading starts (e.g. `CREATE TABLE IF NOT EXISTS`).
    fn on_start(&self, _db: Arc<Client>, _epoch: u64) -> PluginFuture<'_> {
        async move { Ok(()) }.boxed()
    }

    /// Runs once after all rows are written (e.g. timestamp backfill).
    fn on_finish(&self, _db: Arc<Client>, _epoch: u64) -> PluginFuture<'_> {
        async move { Ok(()) }.boxed()
    }
}

/// The per-thread [`SlotVisitor`] the runner hands to [`firehose_horizon`].
/// Forwards each callback to every worker and triggers periodic flushes.
struct Dispatch {
    workers: Vec<Box<dyn PluginWorker>>,
    output: Output,
    since_flush: u32,
    /// Union of every registered plugin's declared consumption, computed
    /// once by the runner: the decoder materializes a stream field iff at
    /// least one plugin consumes it.
    consumption: Consumption,
}

impl Dispatch {
    /// Final flush of every worker (called once after reading completes).
    fn finish(mut self) {
        for w in &mut self.workers {
            w.flush(&self.output);
        }
    }
}

impl SlotVisitor for Dispatch {
    fn on_epoch(&mut self, meta: &EpochMeta) {
        for w in &mut self.workers {
            w.on_epoch(meta);
        }
    }

    fn on_transaction(&mut self, slot: u64, tx_index: u32, tx: &Transaction) {
        for w in &mut self.workers {
            w.on_transaction(slot, tx_index, tx);
        }
    }

    fn on_block(&mut self, notification: &BlockNotification, entries: &[EntryRecord]) {
        for w in &mut self.workers {
            w.on_block(notification, entries);
        }
        self.since_flush += 1;
        if self.since_flush >= FLUSH_INTERVAL_SLOTS {
            self.since_flush = 0;
            for w in &mut self.workers {
                w.flush(&self.output);
            }
        }
    }

    fn consumption(&self) -> Consumption {
        self.consumption
    }
}

/// Runs a set of [`HorizonPlugin`]s over `.jet` archives, parallel-reading
/// each epoch with [`firehose_horizon`] and persisting to ClickHouse.
pub struct HorizonPluginRunner {
    plugins: Vec<Arc<dyn HorizonPlugin>>,
    dsn: String,
    threads: usize,
    /// Writes submitted but not yet completed, shared with every `Output`
    /// this runner hands out (see [`Self::outstanding_writes`]).
    write_backlog: Arc<AtomicU64>,
}

impl HorizonPluginRunner {
    /// Creates a runner targeting the ClickHouse `dsn`, reading each `.jet`
    /// with `threads` parallel workers.
    pub fn new(dsn: impl Into<String>, threads: usize) -> Self {
        Self {
            plugins: Vec::new(),
            dsn: dsn.into(),
            threads: threads.max(1),
            write_backlog: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Registers a plugin.
    pub fn add_plugin(&mut self, plugin: Arc<dyn HorizonPlugin>) -> &mut Self {
        self.plugins.push(plugin);
        self
    }

    /// Writes submitted but not yet completed by the background writer. A
    /// graceful [`run`](Self::run) returns only after this reaches zero; a
    /// driver that *cancels* `run` (ctrl-c) can poll this to give the
    /// detached writer a bounded window to finish acknowledged-durable
    /// inserts before tearing down the ClickHouse server they target.
    pub fn outstanding_writes(&self) -> u64 {
        self.write_backlog.load(Ordering::SeqCst)
    }

    /// Reads `epoch`'s `.jet` from `src` over `slot_range`, driving all
    /// plugins and persisting their rows. Calls `on_start` before reading and
    /// `on_finish` after all writes have drained.
    pub async fn run(
        &self,
        src: JetSource,
        epoch: u64,
        slot_range: Range<u64>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let db = Arc::new(
            crate::build_clickhouse_client(&self.dsn)
                .with_option("async_insert", "1")
                // Durable acks: an insert completes only once the server has
                // flushed it. Anything less lets unacknowledged batches build
                // up server-side and fail silently under pressure; with this
                // setting, a ClickHouse that can't keep up surfaces as
                // backpressure (via the bounded queue) instead of data loss.
                .with_option("wait_for_async_insert", "1"),
        );

        for plugin in &self.plugins {
            plugin.on_start(db.clone(), epoch).await?;
        }

        // Background writer: runs insert jobs with bounded concurrency until
        // every sender is dropped, then drains to empty. Failures are logged,
        // never silent; the backlog counter tracks every submitted job to
        // completion.
        let (tx, mut rx) = tokio::sync::mpsc::channel::<WriteJob>(WRITE_QUEUE_CAP);
        let backlog = self.write_backlog.clone();
        let writer = tokio::spawn(async move {
            let mut inflight = tokio::task::JoinSet::new();
            let log_result = |res: Result<
                Result<(), clickhouse::error::Error>,
                tokio::task::JoinError,
            >| {
                match res {
                    Ok(Ok(())) => {}
                    Ok(Err(err)) => {
                        log::warn!(target: LOG_TARGET, "clickhouse insert failed: {err}");
                    }
                    Err(join_err) => {
                        log::warn!(target: LOG_TARGET, "clickhouse insert task died: {join_err}");
                    }
                }
            };
            while let Some(job) = rx.recv().await {
                while inflight.len() >= MAX_CONCURRENT_INSERTS {
                    if let Some(res) = inflight.join_next().await {
                        log_result(res);
                    }
                }
                let backlog = backlog.clone();
                inflight.spawn(async move {
                    let res = job.await;
                    backlog.fetch_sub(1, Ordering::SeqCst);
                    res
                });
            }
            while let Some(res) = inflight.join_next().await {
                log_result(res);
            }
        });

        let output = Output {
            tx,
            db: db.clone(),
            backlog: self.write_backlog.clone(),
        };
        // A stream field is materialized iff at least one plugin consumes
        // it; with no plugins nothing is consumed. Fold starts from
        // "consumes nothing" and unions each declaration in.
        let combined = self.plugins.iter().fold(
            Consumption::all().without_account_update_data(),
            |acc, p| acc.union(p.consumption()),
        );
        log::info!(
            target: LOG_TARGET,
            "combined plugin consumption: account_update_data={}",
            combined.account_update_data
        );
        let plugins = self.plugins.clone();
        let make_visitor = move |thread_id: usize| Dispatch {
            workers: plugins.iter().map(|p| p.spawn_worker(thread_id)).collect(),
            output: output.clone(),
            since_flush: 0,
            consumption: combined,
        };

        // `make_visitor` (and its captured `output`) is dropped when this
        // await returns; each returned dispatch holds its own `output` clone
        // until `finish()`, so the channel closes only once all final flushes
        // are submitted — then the writer drains. The final flush pass runs
        // under `block_in_place` because `Output::submit` may block on queue
        // backpressure (requires the multi-thread runtime).
        let dispatches =
            firehose_horizon(self.threads, src, epoch, slot_range, make_visitor).await?;
        tokio::task::block_in_place(|| {
            for dispatch in dispatches {
                dispatch.finish();
            }
        });
        writer.await?;

        for plugin in &self.plugins {
            plugin.on_finish(db.clone(), epoch).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use jetstreamer_horizon::archive::{BlockNotification, EntryRecord};
    use std::sync::atomic::{AtomicU32, Ordering};

    /// Worker that records how many blocks and flushes it received.
    struct CountingWorker {
        blocks: Arc<AtomicU32>,
        flushes: Arc<AtomicU32>,
    }

    impl PluginWorker for CountingWorker {
        fn on_block(&mut self, _n: &BlockNotification, _e: &[EntryRecord]) {
            self.blocks.fetch_add(1, Ordering::Relaxed);
        }
        fn flush(&mut self, _out: &Output) {
            self.flushes.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[test]
    fn dispatch_forwards_blocks_and_flushes_on_interval_and_finish() {
        let blocks = Arc::new(AtomicU32::new(0));
        let flushes = Arc::new(AtomicU32::new(0));
        let (tx, _rx) = tokio::sync::mpsc::channel(WRITE_QUEUE_CAP);
        let output = Output {
            tx,
            backlog: Arc::new(AtomicU64::new(0)),
            db: Arc::new(Client::default()),
        };
        let mut dispatch = Dispatch {
            workers: vec![Box::new(CountingWorker {
                blocks: blocks.clone(),
                flushes: flushes.clone(),
            })],
            output,
            since_flush: 0,
            consumption: Consumption::all(),
        };

        let note = BlockNotification::new_boxed();
        for _ in 0..FLUSH_INTERVAL_SLOTS {
            dispatch.on_block(&note, &[]);
        }
        // Every block forwarded; exactly one periodic flush at the interval.
        assert_eq!(blocks.load(Ordering::Relaxed), FLUSH_INTERVAL_SLOTS);
        assert_eq!(flushes.load(Ordering::Relaxed), 1);

        // finish() triggers the final flush.
        dispatch.finish();
        assert_eq!(flushes.load(Ordering::Relaxed), 2);
    }

    /// The runner materializes a stream field iff at least one plugin
    /// consumes it (declarations default to everything).
    #[test]
    fn consumption_union_is_any_consumer_wins() {
        struct Declares(Consumption);
        impl HorizonPlugin for Declares {
            fn name(&self) -> &'static str {
                "declares"
            }
            fn spawn_worker(&self, _thread_id: usize) -> Box<dyn PluginWorker> {
                unreachable!("not spawned in this test")
            }
            fn consumption(&self) -> Consumption {
                self.0
            }
        }
        struct Defaulted;
        impl HorizonPlugin for Defaulted {
            fn name(&self) -> &'static str {
                "defaulted"
            }
            fn spawn_worker(&self, _thread_id: usize) -> Box<dyn PluginWorker> {
                unreachable!("not spawned in this test")
            }
        }

        let none = Consumption::all().without_account_update_data();
        let combine = |plugins: &[&dyn HorizonPlugin]| {
            plugins
                .iter()
                .fold(none, |acc, p| acc.union(p.consumption()))
        };

        // No plugins → nothing consumed.
        assert!(!combine(&[]).account_update_data);
        // All opted out → still nothing consumed.
        assert!(!combine(&[&Declares(none), &Declares(none)]).account_update_data);
        // One defaulted (consumes everything) plugin flips the union on.
        assert!(combine(&[&Declares(none), &Defaulted]).account_update_data);
    }
}
