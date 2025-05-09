use ::clickhouse::Client;
use agave_geyser_plugin_interface::geyser_plugin_interface::GeyserPluginError;
use interprocess::local_socket::{tokio::prelude::*, GenericNamespaced, ToNsName};

use crate::bridge::{Block, Transaction};

pub trait Plugin: Send + Sync + 'static {
    /// The name of this [`Plugin`]
    ///
    /// Used for logging and debugging purposes.
    fn name(&self) -> &'static str;

    /// Called by Solira whenever a transaction is received from the firehose.
    ///
    /// `db` contains a reference to the ClickHouse database client.
    fn on_transaction(
        &mut self,
        db: &Client,
        transaction: Transaction,
    ) -> Result<(), GeyserPluginError>;

    /// Called by Solira whenever a new block is received from the firehose.
    ///
    /// This will precede the `on_transaction` call for each transaction in the block.
    fn on_block(&mut self, db: &Client, block: Block) -> Result<(), GeyserPluginError>;
}

pub struct PluginRunner {
    plugins: Vec<Box<dyn Plugin>>,
}

impl PluginRunner {
    pub fn new() -> Self {
        Self {
            plugins: Vec::new(),
        }
    }

    pub fn register_plugin<P: Plugin>(&mut self, plugin: P) {
        self.plugins.push(Box::new(plugin));
    }

    pub async fn run(&mut self) -> Result<(), GeyserPluginError> {
        todo!()
    }
}
