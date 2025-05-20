pub mod bridge;
pub mod ipc;
pub mod plugins;

use std::pin::Pin;

use ::clickhouse::Client;
use futures_util::FutureExt;
use interprocess::local_socket::{GenericNamespaced, ToNsName, tokio::prelude::*};
use tokio::io::{AsyncReadExt, BufReader};

use crate::{
    bridge::{Block, Transaction},
    ipc::SoliraMessage,
};

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Used to work around the fact that `async fn` cannot be used in an object-safe trait.
pub type PluginFuture<'a> = BoxFuture<'a, Result<(), Box<dyn std::error::Error>>>;

pub trait Plugin: Send + Sync + 'static {
    fn name(&self) -> &'static str;
    fn on_transaction(
        &mut self,
        db: Client,
        transaction: Transaction,
        tx_index: u32,
    ) -> PluginFuture<'_>;
    fn on_block(&mut self, db: Client, block: Block) -> PluginFuture<'_>;
    fn on_load(&mut self, _db: Client) -> PluginFuture<'_> {
        async move { Ok(()) }.boxed()
    }
    fn on_exit(&mut self, _db: Client) -> PluginFuture<'_> {
        async move { Ok(()) }.boxed()
    }
}

pub struct PluginRunner {
    plugins: Vec<Box<dyn Plugin>>,
    clickhouse_dsn: String,
    socket_name: String,
}

impl PluginRunner {
    pub fn new(clickhouse_dsn: impl Into<String>) -> Self {
        Self {
            plugins: Vec::new(),
            clickhouse_dsn: clickhouse_dsn.into(),
            socket_name: "solira.sock".into(),
        }
    }

    pub fn socket_name(mut self, name: impl Into<String>) -> Self {
        self.socket_name = name.into();
        self
    }

    pub fn register(&mut self, plugin: Box<dyn Plugin>) {
        self.plugins.push(plugin);
    }

    /// Dial the IPC socket and forward every message to every plugin.
    pub async fn run(mut self) -> Result<(), Box<dyn std::error::Error>> {
        log::info!("connecting to ClickHouse at {}", self.clickhouse_dsn);
        let db = Client::default().with_url(&self.clickhouse_dsn);
        log::info!("checking if database exists + creating if it does not...");
        db.query("CREATE DATABASE IF NOT EXISTS solira")
            .execute()
            .await?;
        log::info!("done.");
        log::info!("connecting to ClickHouse at {}", self.clickhouse_dsn);
        let db = Client::default()
            .with_url(&self.clickhouse_dsn)
            .with_database("solira")
            .with_option("async_insert", "1")
            .with_option("wait_for_async_insert", "0");

        // Connect to the domain socket
        let ns_name = self.socket_name.to_ns_name::<GenericNamespaced>()?;
        let stream: LocalSocketStream = LocalSocketStream::connect(ns_name).await?;
        let mut reader = BufReader::new(stream);

        for p in &mut self.plugins {
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
            reader.read_exact(&mut buf).await?;

            // ── decode ───────────────────────────────────────────────────
            let msg: SoliraMessage = bincode::deserialize(&buf)?;

            // ── dispatch ─────────────────────────────────────────────────
            match msg {
                SoliraMessage::Block(block) => {
                    for p in &mut self.plugins {
                        if let Err(e) = p.on_block(db.clone(), block.clone()).await {
                            log::error!("plugin {} on_block error: {e}", p.name());
                        }
                    }
                }
                SoliraMessage::Transaction(tx, tx_index) => {
                    for p in &mut self.plugins {
                        if let Err(e) = p.on_transaction(db.clone(), tx.clone(), tx_index).await {
                            log::error!("plugin {} on_transaction error: {e}", p.name());
                        }
                    }
                }
                SoliraMessage::Exit => {
                    log::info!("received exit message from solira, shutting down plugin runner...");
                    for p in &mut self.plugins {
                        if let Err(e) = p.on_exit(db.clone()).await {
                            log::error!("plugin {} on_exit error: {e}", p.name());
                        }
                    }
                    break;
                }
            }
        }

        Ok(())
    }
}
