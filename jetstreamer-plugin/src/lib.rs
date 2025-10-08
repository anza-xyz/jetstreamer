pub mod plugins;

use std::{
    fmt::Display,
    ops::Range,
    pin::Pin,
    sync::{Arc, Ordering},
};

use ::clickhouse::{Client, Row};
use futures_util::FutureExt;
use jetstreamer_firehose::firehose::*;
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, BufReader};

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Used to work around the fact that `async fn` cannot be used in an object-safe trait.
pub type PluginFuture<'a> = BoxFuture<'a, Result<(), Box<dyn std::error::Error>>>;

pub trait Plugin: Send + Sync + 'static {
    fn name(&self) -> &'static str;

    /// Bump this when plugin schema or semantics change.
    fn version(&self) -> u16 {
        1
    }

    /// A unique identifier for the plugin, derived from the Sha256 hash of its name folded
    /// into a u16 to save space, can be overridden if you get a collision with another plugin,
    /// but this is unlikely.
    fn id(&self) -> u16 {
        let hash = Sha256::digest(self.name());
        let mut res = 1u16;
        for byte in hash {
            res = res.wrapping_mul(31).wrapping_add(byte as u16);
        }
        res
    }

    fn on_transaction(
        &self,
        db: Arc<Client>,
        transaction: &TransactionData,
        tx_index: u32,
    ) -> PluginFuture<'_>;
    fn on_block(&self, db: Option<Arc<Client>>, block: &BlockData) -> PluginFuture<'_>;
    fn on_entry(&self, db: Option<Arc<Client>>, entry: &EntryData) -> PluginFuture<'_>;
    fn on_reward(&self, db: Option<Arc<Client>>, reward: &RewardData) -> PluginFuture<'_>;
    fn on_load(&self, _db: Option<Arc<Client>>) -> PluginFuture<'_> {
        async move { Ok(()) }.boxed()
    }
    fn on_exit(&self, _db: Arc<Client>) -> PluginFuture<'_> {
        async move { Ok(()) }.boxed()
    }

    fn clone_plugin(&self) -> Box<dyn Plugin>;
}

pub struct PluginRunner {
    plugins: Arc<Vec<Box<dyn Plugin>>>,
    clickhouse_dsn: String,
    num_threads: usize,
}

impl Clone for PluginRunner {
    fn clone(&self) -> Self {
        Self {
            plugins: {
                Arc::new(
                    self.plugins
                        .iter()
                        .map(|p| p.clone_plugin())
                        .collect::<Vec<_>>(),
                )
            },
            clickhouse_dsn: self.clickhouse_dsn.clone(),
            num_threads: self.num_threads,
        }
    }
}

impl PluginRunner {
    pub fn new(clickhouse_dsn: impl Display, num_threads: usize) -> Self {
        Self {
            plugins: Arc::new(Vec::new()),
            clickhouse_dsn: clickhouse_dsn.to_string(),
            num_threads,
        }
    }

    pub fn register(&mut self, plugin: Box<dyn Plugin>) {
        Arc::get_mut(&mut self.plugins)
            .expect("cannot register plugins after the runner has started!")
            .push(plugin);
    }

    /// Starts the [`PluginRunner`], forwarding all firehose data to all registered plugins.
    pub async fn run(
        self: Arc<Self>,
        slot_range: Range<u64>,
        clickhouse_enabled: bool,
    ) -> Result<(), PluginRunnerError> {
        let db = if clickhouse_enabled {
            log::info!("connecting to ClickHouse at {}", self.clickhouse_dsn);
            let db = Client::default()
                .with_url(&self.clickhouse_dsn)
                .with_option("async_insert", "1")
                .with_option("wait_for_async_insert", "0");

            // Initialize plugin management tables
            // NOTE: Include thread_id in ORDER BY so each thread's status row per slot is retained.
            // Previously ORDER BY slot alone caused logical last-write-wins behavior under ReplacingMergeTree
            // making per-thread rows appear "missing" when querying.
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
            Some(db)
        } else {
            None
        };

        firehose(
            self.num_threads,
            slot_range.clone(),
            Some(|thread_id: usize, block: BlockData| {
                if prev > 0 {
                    assert_eq!(prev + 1, block.slot());
                }
                if block.was_skipped() {
                    NUM_SKIPPED_BLOCKS.fetch_add(1, Ordering::Relaxed);
                } else {
                    NUM_BLOCKS.fetch_add(1, Ordering::Relaxed);
                }
                Ok(())
            }),
            None::<OnTxFn>,
            None::<OnEntryFn>,
            None::<OnRewardFn>,
        )
        .await
        .unwrap();
    }
}

/*
#[inline(always)]
pub async fn handle_message(
    plugin: &dyn Plugin,
    db: Arc<Client>,
    msg: JetstreamerMessage,
    plugin_id: u16,
) -> Result<(), PluginRunnerError> {
    match msg {
        JetstreamerMessage::Block(block, _) => {
            let slot = block.slot;
            if let Err(e) = plugin.on_block(db.clone(), block).await {
                log::error!("plugin {} on_block error: {e}", plugin.name());
            }
            #[derive(Row, serde::Serialize)]
            struct PluginSlotRow {
                plugin_id: u32,
                slot: u64,
            }
            let mut insert = db.insert("jetstreamer_plugin_slots").unwrap();
            insert
                .write(&PluginSlotRow {
                    plugin_id: plugin_id as u32,
                    slot,
                })
                .await
                .unwrap();
            insert.end().await.unwrap();
        }
        JetstreamerMessage::Transaction(tx, tx_index) => {
            if let Err(e) = plugin.on_transaction(db.clone(), tx, tx_index).await {
                log::error!("plugin {} on_transaction error: {e}", plugin.name());
            }
        }
        JetstreamerMessage::Exit => {
            if let Err(e) = plugin.on_exit(db.clone()).await {
                log::error!("plugin {} on_exit error: {e}", plugin.name());
            }
        }
    }
    Ok(())
}

// ensure that PluginRunnerError is Send + Sync + 'static
trait _CanSend: Send + Sync + 'static {}
impl _CanSend for PluginRunnerError {}

#[derive(Debug, thiserror::Error)]
pub enum PluginRunnerError {
    ClickhouseError(clickhouse::error::Error),
    FirehoseError(FirehoseError),
}

impl Display for PluginRunnerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PluginRunnerError::ClickhouseError(error) => {
                write!(f, "clickhouse error: {}", error)
            }
            PluginRunnerError::FirehoseError(error) => write!(f, "firehose error: {}", error),
        }
    }
}

impl From<clickhouse::error::Error> for PluginRunnerError {
    fn from(error: clickhouse::error::Error) -> Self {
        PluginRunnerError::ClickhouseError(error)
    }
}

impl From<FirehoseError> for PluginRunnerError {
    fn from(error: FirehoseError) -> Self {
        PluginRunnerError::FirehoseError(error)
    }
}
*/
