use agave_geyser_plugin_interface::geyser_plugin_interface::{
    GeyserPlugin, ReplicaBlockInfoVersions, Result,
};
use jetstreamer_horizon::account_updates::AccountUpdate;
use lencode::prelude::*;
use log::info;
use std::{
    cell::RefCell,
    sync::atomic::{AtomicU64, Ordering},
};

const ACCOUNT_UPDATE_ENCODE_BUFFER_SIZE: usize = 1024;

thread_local! {
    static ENCODER: RefCell<DedupeEncoder> = RefCell::new(DedupeEncoder::new());
    static ACCOUNT_UPDATE_ENCODE_BUFFER: RefCell<[u8; ACCOUNT_UPDATE_ENCODE_BUFFER_SIZE]> =
        RefCell::new([0u8; ACCOUNT_UPDATE_ENCODE_BUFFER_SIZE]);
}

#[derive(Debug)]
struct JetstreamerNodeGeyserPlugin {
    startup_accounts: AtomicU64,
    account_updates: AtomicU64,
    transactions: AtomicU64,
    total_in_memory_account_update_size: AtomicU64,
    total_encoded_account_update_size: AtomicU64,
}

impl Default for JetstreamerNodeGeyserPlugin {
    fn default() -> Self {
        Self {
            startup_accounts: AtomicU64::new(0),
            account_updates: AtomicU64::new(0),
            transactions: AtomicU64::new(0),
            total_in_memory_account_update_size: AtomicU64::new(0),
            total_encoded_account_update_size: AtomicU64::new(0),
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
        ENCODER.with(|encoder| {
            let mut encoder = encoder.borrow_mut();
            encoder.clear();
        });
        Ok(())
    }

    fn update_account(
        &self,
        account: agave_geyser_plugin_interface::geyser_plugin_interface::ReplicaAccountInfoVersions,
        slot: u64,
        is_startup: bool,
    ) -> Result<()> {
        if is_startup {
            self.startup_accounts.fetch_add(1, Ordering::Relaxed);
            return Ok(());
        }
        self.account_updates.fetch_add(1, Ordering::Relaxed);
        let ac: AccountUpdate = account.into();
        //info!("account update: {:?}", ac);
        let encoded_len = ACCOUNT_UPDATE_ENCODE_BUFFER.with(|buffer| {
            let mut buffer = buffer.borrow_mut();
            let mut cursor = Cursor::new(&mut buffer[..]);

            let encoded_len = ENCODER.with(|encoder| {
                match ac.encode_ext(&mut cursor, Some(&mut *encoder.borrow_mut())) {
                    Ok(len) => len,
                    Err(Error::WriterOutOfSpace) => {
                        panic!(
                            "account update for slot {slot} exceeded {} byte encode buffer",
                            ACCOUNT_UPDATE_ENCODE_BUFFER_SIZE
                        )
                    }
                    Err(err) => panic!("failed to encode account update for slot {slot}: {err:?}"),
                }
            });

            debug_assert_eq!(encoded_len, cursor.position());
            encoded_len
        });

        self.total_in_memory_account_update_size
            .fetch_add(ac.memory_size() as u64, Ordering::Relaxed);

        self.total_encoded_account_update_size
            .fetch_add(encoded_len as u64, Ordering::Relaxed);
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
            "block slot {} total_txs={} total_account_updates={} memory_size={} encoded_size={}",
            slot,
            transactions,
            account_updates,
            self.total_in_memory_account_update_size
                .load(Ordering::Relaxed),
            self.total_encoded_account_update_size
                .load(Ordering::Relaxed)
        );
        Ok(())
    }

    fn account_data_notifications_enabled(&self) -> bool {
        true
    }

    fn account_data_snapshot_notifications_enabled(&self) -> bool {
        true
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
