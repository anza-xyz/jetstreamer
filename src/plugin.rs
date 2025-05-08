pub use agave_geyser_plugin_interface::geyser_plugin_interface::GeyserPluginError;
pub use agave_geyser_plugin_interface::geyser_plugin_interface::ReplicaBlockInfoVersions as Block;
pub use agave_geyser_plugin_interface::geyser_plugin_interface::ReplicaTransactionInfoVersions as Transaction;

pub struct PluginState {}

pub trait Plugin {
    fn name() -> &'static str;
    fn on_transaction(
        &mut self,
        transaction: Transaction,
        slot: u64,
        epoch: u16,
    ) -> Result<(), GeyserPluginError>;
    fn on_block(&mut self, block: Block, slot: u64, epoch: u16) -> Result<(), GeyserPluginError>;
}
