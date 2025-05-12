pub mod bridge;
pub mod ipc;

use ::clickhouse::Client;
use bincode;
use interprocess::local_socket::{GenericNamespaced, ToNsName, tokio::prelude::*};
use tokio::io::{AsyncReadExt, BufReader};

use crate::{
    bridge::{Block, Transaction},
    ipc::SoliraMessage,
};

pub trait Plugin: Send + Sync + 'static {
    fn name(&self) -> &'static str;
    fn on_transaction(
        &mut self,
        db: &Client,
        transaction: Transaction,
    ) -> Result<(), Box<dyn std::error::Error>>;
    fn on_block(&mut self, db: &Client, block: Block) -> Result<(), Box<dyn std::error::Error>>;
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
        // Shared ClickHouse client
        let db = Client::default().with_url(&self.clickhouse_dsn);

        // Connect to the domain socket
        let ns_name = self.socket_name.to_ns_name::<GenericNamespaced>()?;
        let stream: LocalSocketStream = LocalSocketStream::connect(ns_name).await?;
        let mut reader = BufReader::new(stream);

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
                        if let Err(e) = p.on_block(&db, block.clone()) {
                            log::error!("plugin {} on_block error: {e}", p.name());
                        }
                    }
                }
                SoliraMessage::Transaction(tx) => {
                    for p in &mut self.plugins {
                        if let Err(e) = p.on_transaction(&db, tx.clone()) {
                            log::error!("plugin {} on_transaction error: {e}", p.name());
                        }
                    }
                }
                SoliraMessage::Exit => {
                    log::info!("received exit message from solira, shutting down...");
                    break;
                }
            }
        }

        Ok(())
    }
}
