pub mod bridge;
pub mod ipc;
pub mod plugins;

use std::{fmt::Display, pin::Pin, sync::Arc};

use ::clickhouse::{Client, Row};
use futures_util::FutureExt;
use interprocess::local_socket::{GenericNamespaced, ToNsName, tokio::prelude::*};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, BufReader};

use crate::{
    bridge::{Block, Transaction},
    ipc::JetstreamerMessage,
};

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
        transaction: Transaction,
        tx_index: u32,
    ) -> PluginFuture<'_>;
    fn on_block(&self, db: Arc<Client>, block: Block) -> PluginFuture<'_>;
    fn on_load(&self, _db: Arc<Client>) -> PluginFuture<'_> {
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
    num_threads: usize, // New field to store the number of threads
    socket_name: String,
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
            socket_name: self.socket_name.clone(),
        }
    }
}

impl PluginRunner {
    pub fn new(clickhouse_dsn: impl Into<String>, num_threads: usize) -> Self {
        Self {
            plugins: Arc::new(Vec::new()),
            clickhouse_dsn: clickhouse_dsn.into(),
            num_threads, // Initialize the number of threads
            socket_name: String::new(),
        }
    }

    pub fn register(&mut self, plugin: Box<dyn Plugin>) {
        Arc::get_mut(&mut self.plugins)
            .expect("cannot register plugins after the runner has started!")
            .push(plugin);
    }

    pub fn socket_name(mut self, name: impl Into<String>) -> Self {
        self.socket_name = name.into();
        self
    }

    /// Dial the IPC socket and forward every message to every plugin.
    pub async fn run(self: Arc<Self>) -> Result<(), PluginRunnerError> {
        log::info!("connecting to ClickHouse at {}", self.clickhouse_dsn);
        let db = Client::default()
            .with_url(&self.clickhouse_dsn)
            .with_option("async_insert", "0")
            .with_option("wait_for_async_insert", "1");

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

        let mut handles = Vec::new();
        let db = Arc::new(db);

        for thread_id in 0..self.num_threads {
            let db = db.clone();
            let socket_name = format!("jetstreamer_{}.sock", thread_id);
            let ns_name = socket_name
                .to_ns_name::<GenericNamespaced>()
                .map_err(|e| PluginRunnerError::UnsupportedSocketName(e.to_string()))?;

            let plugins = self.plugins.clone();
            let db = db.clone();
            for plugin in plugins.iter() {
                plugin.on_load(db.clone()).await.unwrap();
            }

            let handle = tokio::spawn(async move {
                let stream: LocalSocketStream = LocalSocketStream::connect(ns_name)
                    .await
                    .map_err(|e| PluginRunnerError::SocketConnectError(e.to_string()))?;
                let mut reader = BufReader::new(stream);

                let plugin_ids = plugins.iter().map(|p| p.id()).collect::<Vec<_>>();
                let fast_path = plugins.len() == 1;

                loop {
                    let mut len_buf = [0u8; 4];
                    if reader.read_exact(&mut len_buf).await.is_err() {
                        break; // EOF
                    }
                    let len = u32::from_le_bytes(len_buf) as usize;

                    let mut buf = vec![0u8; len];
                    reader
                        .read_exact(&mut buf)
                        .await
                        .map_err(|e| PluginRunnerError::PayloadReadError(e.to_string()))?;

                    let msg: JetstreamerMessage = bincode::deserialize(&buf)?;

                    if fast_path {
                        let plugin = &plugins[0];
                        let plugin_id = plugin_ids[0];
                        handle_message(plugin.as_ref(), db.clone(), msg.clone(), plugin_id).await?;
                    } else {
                        futures::future::join_all(plugins.iter().enumerate().map(|(i, plugin)| {
                            let plugin_id = plugin_ids[i];
                            let db = db.clone();
                            let msg = msg.clone();
                            async move { handle_message(plugin.as_ref(), db, msg, plugin_id).await }
                        }))
                        .await
                        .into_iter()
                        .collect::<Result<Vec<_>, _>>()?;
                    }

                    if let JetstreamerMessage::Block(blk, transaction_count) = &msg {
                        #[derive(Copy, Clone, Row, serde::Serialize)]
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
                                        transaction_count: *transaction_count,
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
                    }

                    if matches!(msg, JetstreamerMessage::Exit) {
                        log::info!(
                            "plugin runner thread {} received exit message, shutting down",
                            thread_id
                        );
                        break;
                    }
                }

                Ok::<(), PluginRunnerError>(())
            });

            handles.push(handle);
        }

        futures::future::join_all(handles)
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;

        Ok(())
    }
}

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
    UnsupportedSocketName(String),
    SocketConnectError(String),
    PayloadReadError(String),
    ClickhouseError(clickhouse::error::Error),
    Bincode(bincode::Error),
}

impl Display for PluginRunnerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PluginRunnerError::UnsupportedSocketName(s) => {
                write!(f, "unsupported domain socket name: {}", s)
            }
            PluginRunnerError::ClickhouseError(error) => {
                write!(f, "clickhouse error: {}", error)
            }
            PluginRunnerError::SocketConnectError(s) => {
                write!(f, "failed to connect to domain socket: {}", s)
            }
            PluginRunnerError::PayloadReadError(s) => {
                write!(f, "failed to read payload bytes from domain socket: {}", s)
            }
            PluginRunnerError::Bincode(error) => write!(f, "bincode error: {}", error),
        }
    }
}

impl From<clickhouse::error::Error> for PluginRunnerError {
    fn from(error: clickhouse::error::Error) -> Self {
        PluginRunnerError::ClickhouseError(error)
    }
}

impl From<bincode::Error> for PluginRunnerError {
    fn from(error: bincode::Error) -> Self {
        PluginRunnerError::Bincode(error)
    }
}

impl From<tokio::task::JoinError> for PluginRunnerError {
    fn from(error: tokio::task::JoinError) -> Self {
        PluginRunnerError::SocketConnectError(format!("Thread join error: {error}"))
    }
}
