pub mod bridge;
pub mod ipc;
pub mod plugins;

use std::{
    fmt::Display,
    pin::Pin,
    sync::{Arc, Mutex},
};

use ::clickhouse::{Client, Row};
use futures_util::FutureExt;
use interprocess::local_socket::{GenericNamespaced, ToNsName, tokio::prelude::*};
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
    fn version(&self) -> u32 {
        1
    }
    fn on_transaction(
        &self,
        db: Client,
        transaction: Transaction,
        tx_index: u32,
    ) -> PluginFuture<'_>;
    fn on_block(&self, db: Client, block: Block) -> PluginFuture<'_>;
    fn on_load(&self, _db: Client) -> PluginFuture<'_> {
        async move { Ok(()) }.boxed()
    }
    fn on_exit(&self, _db: Client) -> PluginFuture<'_> {
        async move { Ok(()) }.boxed()
    }

    fn clone_plugin(&self) -> Box<dyn Plugin>;
}

pub struct PluginRunner {
    plugins: Mutex<Vec<Box<dyn Plugin>>>,
    clickhouse_dsn: String,
    socket_name: String,
}

impl Clone for PluginRunner {
    fn clone(&self) -> Self {
        Self {
            plugins: {
                let plugins = self.plugins.lock().unwrap();
                Mutex::new(plugins.iter().map(|p| p.clone_plugin()).collect::<Vec<_>>())
            },
            clickhouse_dsn: self.clickhouse_dsn.clone(),
            socket_name: self.socket_name.clone(),
        }
    }
}

impl PluginRunner {
    pub fn new(clickhouse_dsn: impl Into<String>) -> Self {
        Self {
            plugins: Mutex::new(Vec::new()),
            clickhouse_dsn: clickhouse_dsn.into(),
            socket_name: "jetstreamer.sock".into(),
        }
    }

    pub fn socket_name(mut self, name: impl Into<String>) -> Self {
        self.socket_name = name.into();
        self
    }

    pub fn register(&mut self, plugin: Box<dyn Plugin>) {
        self.plugins.lock().unwrap().push(plugin);
    }

    /// Dial the IPC socket and forward every message to every plugin.
    pub async fn run(self: Arc<Self>) -> Result<(), PluginRunnerError> {
        log::info!("connecting to ClickHouse at {}", self.clickhouse_dsn);
        // let db = Client::default().with_url(&self.clickhouse_dsn);
        // log::info!("checking if database exists + creating if it does not...");
        // db.query("CREATE DATABASE IF NOT EXISTS jetstreamer")
        //     .execute()
        //     .await?;
        // log::info!("done.");
        log::info!("connecting to ClickHouse at {}", self.clickhouse_dsn);
        let db = Client::default()
            .with_url(&self.clickhouse_dsn)
            .with_option("async_insert", "1")
            .with_option("wait_for_async_insert", "0");

        // Initialize plugin management tables
        db.query(
            r#"CREATE TABLE IF NOT EXISTS jetstreamer_slot_status (
                slot UInt64
            ) ENGINE = ReplacingMergeTree
            ORDER BY slot"#,
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

        // Connect to the domain socket
        let ns_name = self
            .socket_name
            .clone()
            .to_ns_name::<GenericNamespaced>()
            .map_err(|e| PluginRunnerError::UnsupportedSocketName(e.to_string()))?;
        let stream: LocalSocketStream = LocalSocketStream::connect(ns_name)
            .await
            .map_err(|e| PluginRunnerError::SocketConnectError(e.to_string()))?;
        let mut reader = BufReader::new(stream);

        // plugin set is locked while the plugin runner is running
        let mut plugins: Vec<Box<dyn Plugin>> = {
            let mut guard = self.plugins.lock().unwrap();
            std::mem::take(&mut *guard)
        };

        // Insert plugin metadata into jetstreamer_plugins table
        #[derive(Row, serde::Serialize)]
        struct PluginRow<'a> {
            id: u32,
            name: &'a str,
            version: u32,
        }
        let mut insert = db.insert("jetstreamer_plugins")?;
        for (i, p) in plugins.iter().enumerate() {
            insert
                .write(&PluginRow {
                    id: i as u32,
                    name: p.name(),
                    version: p.version(),
                })
                .await?;
        }
        insert.end().await?;

        for p in plugins.iter_mut() {
            if let Err(e) = p.on_load(db.clone()).await {
                log::error!("plugin {} on_load error: {e}", p.name());
            }
        }

        log::info!("plugin runner loaded, waiting for transactions...");

        loop {
            // ── length-prefix (u32 little-endian) ─────────────────────────
            let mut len_buf = [0u8; 4];
            if reader.read_exact(&mut len_buf).await.is_err() {
                break; // EOF
            }
            let len = u32::from_le_bytes(len_buf) as usize;

            // ── payload ──────────────────────────────────────────────────
            let mut buf = vec![0u8; len];
            reader
                .read_exact(&mut buf)
                .await
                .map_err(|e| PluginRunnerError::PayloadReadError(e.to_string()))?;

            // ── decode ───────────────────────────────────────────────────
            let msg: JetstreamerMessage = bincode::deserialize(&buf)?;

            // ── dispatch ─────────────────────────────────────────────────
            match msg {
                JetstreamerMessage::Block(block) => {
                    for (id, p) in plugins.iter_mut().enumerate() {
                        if let Err(e) = p.on_block(db.clone(), block.clone()).await {
                            log::error!("plugin {} on_block error: {e}", p.name());
                        }
                        #[derive(Row, serde::Serialize)]
                        struct PluginSlotRow {
                            plugin_id: u32,
                            slot: u64,
                        }
                        let mut insert = db.insert("jetstreamer_plugin_slots")?;
                        insert
                            .write(&PluginSlotRow {
                                plugin_id: id as u32,
                                slot: block.slot,
                            })
                            .await?;
                        insert.end().await?;
                    }
                }
                JetstreamerMessage::Transaction(tx, tx_index) => {
                    for p in plugins.iter_mut() {
                        if let Err(e) = p.on_transaction(db.clone(), tx.clone(), tx_index).await {
                            log::error!("plugin {} on_transaction error: {e}", p.name());
                        }
                    }
                }
                JetstreamerMessage::Exit => {
                    log::info!(
                        "received exit message from jetstreamer, shutting down plugin runner..."
                    );
                    for p in plugins.iter_mut() {
                        if let Err(e) = p.on_exit(db.clone()).await {
                            log::error!("plugin {} on_exit error: {e}", p.name());
                        }
                    }
                    break;
                }
            }
        }

        // put the plugins back in the mutex
        self.plugins.lock().unwrap().extend(plugins);

        Ok(())
    }
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
