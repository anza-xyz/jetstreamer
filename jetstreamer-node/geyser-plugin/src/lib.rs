use agave_geyser_plugin_interface::geyser_plugin_interface::{
    GeyserPlugin, ReplicaBlockInfoVersions, Result,
};
use log::info;
use std::{
    collections::HashMap,
    sync::{
        Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

#[derive(Debug, Default)]
struct SlotCounters {
    transactions: u64,
    account_updates: u64,
}

#[derive(Debug)]
struct JetstreamerNodeGeyserPlugin {
    per_slot: Mutex<HashMap<u64, SlotCounters>>,
    startup_accounts: AtomicU64,
}

impl Default for JetstreamerNodeGeyserPlugin {
    fn default() -> Self {
        Self {
            per_slot: Mutex::new(HashMap::new()),
            startup_accounts: AtomicU64::new(0),
        }
    }
}

impl GeyserPlugin for JetstreamerNodeGeyserPlugin {
    fn name(&self) -> &'static str {
        "jetstreamer-node-geyser"
    }

    fn update_account(
        &self,
        _account: agave_geyser_plugin_interface::geyser_plugin_interface::ReplicaAccountInfoVersions,
        slot: u64,
        is_startup: bool,
    ) -> Result<()> {
        if is_startup {
            self.startup_accounts.fetch_add(1, Ordering::Relaxed);
            return Ok(());
        }
        let mut guard = self
            .per_slot
            .lock()
            .map_err(|_| {
                agave_geyser_plugin_interface::geyser_plugin_interface::GeyserPluginError::Custom(
                    "slot counter lock poisoned".into(),
                )
            })?;
        let counters = guard.entry(slot).or_default();
        counters.account_updates = counters.account_updates.saturating_add(1);
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
        slot: u64,
    ) -> Result<()> {
        let mut guard = self
            .per_slot
            .lock()
            .map_err(|_| {
                agave_geyser_plugin_interface::geyser_plugin_interface::GeyserPluginError::Custom(
                    "slot counter lock poisoned".into(),
                )
            })?;
        let counters = guard.entry(slot).or_default();
        counters.transactions = counters.transactions.saturating_add(1);
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
        let (transactions, account_updates) = {
            let mut guard = self
                .per_slot
                .lock()
                .map_err(|_| {
                    agave_geyser_plugin_interface::geyser_plugin_interface::GeyserPluginError::Custom(
                        "slot counter lock poisoned".into(),
                    )
                })?;
            let counters = guard.remove(&slot).unwrap_or_default();
            (counters.transactions, counters.account_updates)
        };
        info!(
            "block slot {} txs={} account_updates={}",
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
