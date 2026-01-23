use agave_geyser_plugin_interface::geyser_plugin_interface::{
    GeyserPlugin, ReplicaBlockInfoVersions, Result,
};
use jetstreamer_horizon::account_updates::AccountUpdate;
use log::info;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug)]
struct JetstreamerNodeGeyserPlugin {
    startup_accounts: AtomicU64,
    account_updates: AtomicU64,
    transactions: AtomicU64,
}

impl Default for JetstreamerNodeGeyserPlugin {
    fn default() -> Self {
        Self {
            startup_accounts: AtomicU64::new(0),
            account_updates: AtomicU64::new(0),
            transactions: AtomicU64::new(0),
        }
    }
}

impl GeyserPlugin for JetstreamerNodeGeyserPlugin {
    fn setup_logger(&self, logger: &'static dyn log::Log, level: log::LevelFilter) -> Result<()> {
        log::set_max_level(level);
        // Ignore errors if the logger is already set (e.g., plugin reload).
        let _ = log::set_logger(logger);
        Ok(())
    }

    fn name(&self) -> &'static str {
        "jetstreamer-node-geyser"
    }

    fn on_load(&mut self, config_file: &str, is_reload: bool) -> Result<()> {
        info!(
            "loaded geyser plugin config={} reload={}",
            config_file, is_reload
        );
        Ok(())
    }

    fn update_account(
        &self,
        account: agave_geyser_plugin_interface::geyser_plugin_interface::ReplicaAccountInfoVersions,
        _slot: u64,
        is_startup: bool,
    ) -> Result<()> {
        if is_startup {
            self.startup_accounts.fetch_add(1, Ordering::Relaxed);
            return Ok(());
        }
        self.account_updates.fetch_add(1, Ordering::Relaxed);
        let ac: AccountUpdate = account.into();
        info!("account update: {:?}", ac);
        Ok(())
    }

    fn notify_end_of_startup(&self) -> Result<()> {
        let total = self.startup_accounts.load(Ordering::Relaxed);
        info!("startup account updates: {}", total);
        Ok(())
    }

    fn notify_transaction(
        &self,
        _transaction: agave_geyser_plugin_interface::geyser_plugin_interface::ReplicaTransactionInfoVersions,
        _slot: u64,
    ) -> Result<()> {
        self.transactions.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    fn notify_block_metadata(&self, blockinfo: ReplicaBlockInfoVersions) -> Result<()> {
        let slot = match blockinfo {
            ReplicaBlockInfoVersions::V0_0_1(info) => info.slot,
            ReplicaBlockInfoVersions::V0_0_2(info) => info.slot,
            ReplicaBlockInfoVersions::V0_0_3(info) => info.slot,
            ReplicaBlockInfoVersions::V0_0_4(info) => info.slot,
        };
        if slot == u64::MAX {
            return Ok(());
        }
        let transactions = self.transactions.load(Ordering::Relaxed);
        let account_updates = self.account_updates.load(Ordering::Relaxed);
        info!(
            "block slot {} total_txs={} total_account_updates={}",
            slot, transactions, account_updates
        );
        Ok(())
    }

    fn transaction_notifications_enabled(&self) -> bool {
        true
    }
}

#[allow(improper_ctypes_definitions)]
#[unsafe(no_mangle)]
pub extern "C" fn _create_plugin() -> *mut dyn GeyserPlugin {
    let plugin: Box<dyn GeyserPlugin> = Box::new(JetstreamerNodeGeyserPlugin::default());
    Box::into_raw(plugin)
}
