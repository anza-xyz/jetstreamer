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

use clickhouse::Client;
use futures_util::FutureExt;

use jetstreamer_firehose::firehose_horizon::{JetSource, firehose_horizon};
use jetstreamer_horizon::archive::{BlockNotification, EntryRecord, EpochMeta, SlotVisitor};
use jetstreamer_horizon::transactions::Transaction;

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

/// Sink handle given to a [`PluginWorker`] at flush time. Holds a ClickHouse
/// client for building inserts and a channel to the background writer, so the
/// (sync, on a decode thread) worker never awaits I/O.
#[derive(Clone)]
pub struct Output {
    tx: tokio::sync::mpsc::UnboundedSender<WriteJob>,
    db: Arc<Client>,
}

impl Output {
    /// A ClickHouse client handle for building an insert.
    pub fn db(&self) -> Arc<Client> {
        self.db.clone()
    }

    /// Hands an insert future to the background writer (non-blocking). Build
    /// it with [`Output::db`]; the future runs off the decode thread.
    pub fn submit<Fut>(&self, job: Fut)
    where
        Fut: Future<Output = Result<(), clickhouse::error::Error>> + Send + 'static,
    {
        let _ = self.tx.send(Box::pin(job));
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
}

/// Runs a set of [`HorizonPlugin`]s over `.jet` archives, parallel-reading
/// each epoch with [`firehose_horizon`] and persisting to ClickHouse.
pub struct HorizonPluginRunner {
    plugins: Vec<Arc<dyn HorizonPlugin>>,
    dsn: String,
    threads: usize,
}

impl HorizonPluginRunner {
    /// Creates a runner targeting the ClickHouse `dsn`, reading each `.jet`
    /// with `threads` parallel workers.
    pub fn new(dsn: impl Into<String>, threads: usize) -> Self {
        Self {
            plugins: Vec::new(),
            dsn: dsn.into(),
            threads: threads.max(1),
        }
    }

    /// Registers a plugin.
    pub fn add_plugin(&mut self, plugin: Arc<dyn HorizonPlugin>) -> &mut Self {
        self.plugins.push(plugin);
        self
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
                .with_option("wait_for_async_insert", "0"),
        );

        for plugin in &self.plugins {
            plugin.on_start(db.clone(), epoch).await?;
        }

        // Background writer: drains insert jobs until every sender is dropped.
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<WriteJob>();
        let writer = tokio::spawn(async move {
            while let Some(job) = rx.recv().await {
                if let Err(err) = job.await {
                    log::warn!(target: LOG_TARGET, "clickhouse insert failed: {err}");
                }
            }
        });

        let output = Output {
            tx,
            db: db.clone(),
        };
        let plugins = self.plugins.clone();
        let make_visitor = move |thread_id: usize| Dispatch {
            workers: plugins.iter().map(|p| p.spawn_worker(thread_id)).collect(),
            output: output.clone(),
            since_flush: 0,
        };

        // `make_visitor` (and its captured `output`) is dropped when this
        // await returns; each returned dispatch holds its own `output` clone
        // until `finish()`, so the channel closes only once all final flushes
        // are submitted — then the writer drains.
        let dispatches =
            firehose_horizon(self.threads, src, epoch, slot_range, make_visitor).await?;
        for dispatch in dispatches {
            dispatch.finish();
        }
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
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        let output = Output {
            tx,
            db: Arc::new(Client::default()),
        };
        let mut dispatch = Dispatch {
            workers: vec![Box::new(CountingWorker {
                blocks: blocks.clone(),
                flushes: flushes.clone(),
            })],
            output,
            since_flush: 0,
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
}
