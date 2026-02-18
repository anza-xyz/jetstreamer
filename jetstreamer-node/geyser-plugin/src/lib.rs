use agave_geyser_plugin_interface::geyser_plugin_interface::{
    GeyserPlugin, ReplicaBlockInfoVersions, Result,
};
use jetstreamer_horizon::account_updates::AccountUpdate;
use lencode::prelude::*;
use log::info;
use std::{
    cell::RefCell,
    sync::{
        Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};

// Solana account data can be up to 10 MiB. Keep a safety margin for encoding overhead.
const ACCOUNT_UPDATE_ENCODE_BUFFER_SIZE: usize = 12 * 1024 * 1024;

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
    last_throughput_sample: Mutex<Option<ThroughputSample>>,
}

#[derive(Debug, Clone, Copy)]
struct ThroughputSample {
    at: Instant,
    transactions: u64,
    account_updates: u64,
    memory_size: u64,
    encoded_size: u64,
}

fn format_bytes_f64(bytes: f64) -> String {
    const UNITS: [&str; 6] = ["B", "KB", "MB", "GB", "TB", "PB"];
    if bytes < 1024.0 {
        return format!("{bytes:.0} B");
    }

    let mut value = bytes;
    let mut unit_index = 0usize;
    while value >= 1024.0 && unit_index + 1 < UNITS.len() {
        value /= 1024.0;
        unit_index += 1;
    }

    format!("{value:.2} {}", UNITS[unit_index])
}

fn format_bytes(bytes: u64) -> String {
    format_bytes_f64(bytes as f64)
}

impl Default for JetstreamerNodeGeyserPlugin {
    fn default() -> Self {
        Self {
            startup_accounts: AtomicU64::new(0),
            account_updates: AtomicU64::new(0),
            transactions: AtomicU64::new(0),
            total_in_memory_account_update_size: AtomicU64::new(0),
            total_encoded_account_update_size: AtomicU64::new(0),
            last_throughput_sample: Mutex::new(None),
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
                            "account update exceeded encode buffer: slot={slot} pubkey={} data_len={} buffer_len={}",
                            ac.pubkey,
                            ac.data.len(),
                            ACCOUNT_UPDATE_ENCODE_BUFFER_SIZE,
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
        let total_memory_size = self
            .total_in_memory_account_update_size
            .load(Ordering::Relaxed);
        let total_encoded_size = self
            .total_encoded_account_update_size
            .load(Ordering::Relaxed);
        let now = Instant::now();
        let throughput = self
            .last_throughput_sample
            .lock()
            .ok()
            .and_then(|mut last_sample| {
                let rates = last_sample.as_ref().and_then(|previous| {
                    let elapsed = now.duration_since(previous.at).as_secs_f64();
                    if elapsed <= 0.0 {
                        return None;
                    }

                    Some((
                        transactions.saturating_sub(previous.transactions) as f64 / elapsed,
                        account_updates.saturating_sub(previous.account_updates) as f64 / elapsed,
                        total_memory_size.saturating_sub(previous.memory_size) as f64 / elapsed,
                        total_encoded_size.saturating_sub(previous.encoded_size) as f64 / elapsed,
                    ))
                });

                *last_sample = Some(ThroughputSample {
                    at: now,
                    transactions,
                    account_updates,
                    memory_size: total_memory_size,
                    encoded_size: total_encoded_size,
                });

                rates
            });

        let (txs_per_sec, accounts_per_sec, memory_per_sec, encoded_per_sec) = match throughput {
            Some((txs, accounts, memory, encoded)) => (
                format!("{txs:.2}"),
                format!("{accounts:.2}"),
                format!("{}/s", format_bytes_f64(memory)),
                format!("{}/s", format_bytes_f64(encoded)),
            ),
            None => (
                "n/a".to_string(),
                "n/a".to_string(),
                "n/a".to_string(),
                "n/a".to_string(),
            ),
        };
        info!(
            "block slot {} total_txs={} total_account_updates={} memory_size={} ({}) encoded_size={} ({}) txs_per_sec={} accounts_per_sec={} memory_per_sec={} encoded_per_sec={}",
            slot,
            transactions,
            account_updates,
            format_bytes(total_memory_size),
            total_memory_size,
            format_bytes(total_encoded_size),
            total_encoded_size,
            txs_per_sec,
            accounts_per_sec,
            memory_per_sec,
            encoded_per_sec,
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
