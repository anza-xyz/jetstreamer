pub use agave_geyser_plugin_interface::geyser_plugin_interface::GeyserPluginError;
pub use agave_geyser_plugin_interface::geyser_plugin_interface::ReplicaBlockInfoVersions as Block;
pub use agave_geyser_plugin_interface::geyser_plugin_interface::ReplicaTransactionInfoVersions as Transaction;

use ::clickhouse::Client;

pub trait Plugin {
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
        slot: u64,
        epoch: u16,
    ) -> Result<(), GeyserPluginError>;

    /// Called by Solira whenever a new block is received from the firehose.
    ///
    /// This will precede the `on_transaction` call for each transaction in the block.
    fn on_block(
        &mut self,
        db: &Client,
        block: Block,
        slot: u64,
        epoch: u16,
    ) -> Result<(), GeyserPluginError>;
}
