use agave_geyser_plugin_interface::geyser_plugin_interface::{
    GeyserPlugin, ReplicaBlockInfoVersions, Result,
};
use jetstreamer_horizon::account_updates::AccountUpdate;
use lencode::prelude::*;
use log::{info, warn};
use std::{
    cell::RefCell,
    fs,
    sync::{
        Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};

// Solana account data can be up to 10 MiB. Keep a safety margin for encoding overhead.
const ACCOUNT_UPDATE_ENCODE_BUFFER_SIZE: usize = 12 * 1024 * 1024;
const DEFAULT_LOG_EVERY_N_BLOCKS: u64 = 10;

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
    no_packing: bool,
    log_every_n_blocks: u64,
    last_throughput_sample: Mutex<Option<ThroughputSample>>,
}

#[derive(Debug, Clone, Copy)]
struct ThroughputSample {
    at: Instant,
    transactions: u64,
    account_updates: u64,
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

fn account_data_len(
    account: &agave_geyser_plugin_interface::geyser_plugin_interface::ReplicaAccountInfoVersions<
        '_,
    >,
) -> usize {
    match account {
        agave_geyser_plugin_interface::geyser_plugin_interface::ReplicaAccountInfoVersions::V0_0_1(info) => info.data.len(),
        agave_geyser_plugin_interface::geyser_plugin_interface::ReplicaAccountInfoVersions::V0_0_2(info) => info.data.len(),
        agave_geyser_plugin_interface::geyser_plugin_interface::ReplicaAccountInfoVersions::V0_0_3(info) => info.data.len(),
    }
}

impl Default for JetstreamerNodeGeyserPlugin {
    fn default() -> Self {
        Self {
            startup_accounts: AtomicU64::new(0),
            account_updates: AtomicU64::new(0),
            transactions: AtomicU64::new(0),
            total_in_memory_account_update_size: AtomicU64::new(0),
            total_encoded_account_update_size: AtomicU64::new(0),
            no_packing: false,
            log_every_n_blocks: DEFAULT_LOG_EVERY_N_BLOCKS,
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
        let mut no_packing = false;
        let mut log_every_n_blocks = DEFAULT_LOG_EVERY_N_BLOCKS;
        match fs::read_to_string(config_file) {
            Ok(contents) => match serde_json::from_str::<serde_json::Value>(&contents) {
                Ok(config) => {
                    no_packing = config
                        .get("no_packing")
                        .and_then(serde_json::Value::as_bool)
                        .unwrap_or(false);
                    log_every_n_blocks = config
                        .get("log_every_n_blocks")
                        .and_then(serde_json::Value::as_u64)
                        .filter(|value| *value > 0)
                        .unwrap_or(DEFAULT_LOG_EVERY_N_BLOCKS);
                }
                Err(err) => {
                    warn!(
                        "failed to parse geyser config {}; using defaults: {}",
                        config_file, err
                    );
                }
            },
            Err(err) => {
                warn!(
                    "failed to read geyser config {}; using defaults: {}",
                    config_file, err
                );
            }
        }

        self.no_packing = no_packing;
        self.log_every_n_blocks = log_every_n_blocks;
        info!(
            "loaded geyser plugin config={} reload={} no_packing={} log_every_n_blocks={}",
            config_file, is_reload, self.no_packing, self.log_every_n_blocks
        );
        ENCODER.with(|encoder| {
            let mut encoder = encoder.borrow_mut();
            encoder.clear();
        });
        if let Ok(mut sample) = self.last_throughput_sample.lock() {
            *sample = None;
        }
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
        let memory_size = core::mem::size_of::<AccountUpdate>() + account_data_len(&account);
        self.total_in_memory_account_update_size
            .fetch_add(memory_size as u64, Ordering::Relaxed);

        if self.no_packing {
            return Ok(());
        }

        let ac: AccountUpdate = account.into();
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

        let log_every_n_blocks = self.log_every_n_blocks.max(1);
        if slot % log_every_n_blocks != 0 {
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
                    ))
                });

                *last_sample = Some(ThroughputSample {
                    at: now,
                    transactions,
                    account_updates,
                });

                rates
            });

        let (txs_per_sec, accounts_per_sec) = match throughput {
            Some((txs, accounts)) => (format!("{txs:.2}"), format!("{accounts:.2}")),
            None => ("n/a".to_string(), "n/a".to_string()),
        };
        info!(
            "block slot {} total_txs={} total_account_updates={} memory_size={} encoded_size={} txs_per_sec={} accounts_per_sec={}",
            slot,
            transactions,
            account_updates,
            format_bytes(total_memory_size),
            format_bytes(total_encoded_size),
            txs_per_sec,
            accounts_per_sec,
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
