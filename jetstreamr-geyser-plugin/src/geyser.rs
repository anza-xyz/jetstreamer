use agave_geyser_plugin_interface::geyser_plugin_interface::{
    GeyserPlugin, GeyserPluginError, ReplicaBlockInfoVersions, ReplicaTransactionInfoVersions,
    Result,
};
use core::ops::Range;
use geyser_replay::{epochs::slot_to_epoch, firehose::generate_subranges};
use mpsc::Sender;
use rangemap::RangeMap;
use std::{
    cell::RefCell,
    error::Error,
    sync::{
        Mutex,
        atomic::{AtomicBool, AtomicU8, AtomicU32, AtomicU64, Ordering},
    },
    time::Instant,
    u64,
};
use thousands::Separable;
use tokio::runtime::Runtime;
use tokio::sync::mpsc;

use crate::clickhouse;
use jetstreamr_plugin::{
    Plugin,
    bridge::{Block, Transaction},
    ipc::{JetstreamrMessage, spawn_socket_server},
    plugins::program_tracking::ProgramTrackingPlugin,
};

thread_local! {
    static TOKIO_RUNTIME: RefCell<Runtime> = RefCell::new(Runtime::new().unwrap());
    static DB_CLIENT: RefCell<::clickhouse::Client> = RefCell::new(::clickhouse::Client::default()
            .with_url("http://localhost:8123")
            .with_option("async_insert", "1")
            .with_option("wait_for_async_insert", "0"));
    static PLUGIN: RefCell<ProgramTrackingPlugin> = const { RefCell::new(ProgramTrackingPlugin) };
}
static IPC_TX: once_cell::sync::OnceCell<Sender<JetstreamrMessage>> =
    once_cell::sync::OnceCell::new();
static IPC_TASK: once_cell::sync::OnceCell<tokio::task::JoinHandle<()>> =
    once_cell::sync::OnceCell::new();

static EXIT: AtomicBool = AtomicBool::new(false);
static PROCESSED_TRANSACTIONS: AtomicU64 = AtomicU64::new(0);
static PROCESSED_SLOTS: AtomicU64 = AtomicU64::new(0);
static NUM_VOTES: AtomicU64 = AtomicU64::new(0);
static COMPLETE_THREADS: AtomicU8 = AtomicU8::new(0);

static COMPUTE_CONSUMED: Mutex<u128> = Mutex::new(0);

static START_TIME: once_cell::sync::OnceCell<Instant> = once_cell::sync::OnceCell::new();
static SLOT_RANGE: once_cell::sync::OnceCell<Range<u64>> = once_cell::sync::OnceCell::new();

static THREAD_CURRENT_SLOT: [AtomicU64; 256] = [const { AtomicU64::new(u64::MAX) }; 256];
static THREAD_SLOT_RANGE_START: [AtomicU64; 256] = [const { AtomicU64::new(u64::MAX) }; 256];
static THREAD_SLOT_RANGE_END: [AtomicU64; 256] = [const { AtomicU64::new(u64::MAX) }; 256];
static THREAD_CURRENT_TX_INDEX: [AtomicU32; 256] = [const { AtomicU32::new(u32::MAX) }; 256];
static THREAD_INFO: once_cell::sync::OnceCell<RangeMap<u64, u8>> = once_cell::sync::OnceCell::new();

pub fn thread_current_slot(thread_id: u8) -> u64 {
    THREAD_CURRENT_SLOT[thread_id as usize].load(Ordering::SeqCst)
}

pub fn thread_bump_current_slot(thread_id: u8) -> u64 {
    THREAD_CURRENT_SLOT[thread_id as usize].fetch_add(1, Ordering::SeqCst) + 1
}

pub fn thread_set_current_slot(thread_id: u8, slot: u64) {
    THREAD_CURRENT_SLOT[thread_id as usize].store(slot, Ordering::SeqCst);
}

pub fn thread_slot_range_start(thread_id: u8) -> u64 {
    THREAD_SLOT_RANGE_START[thread_id as usize].load(Ordering::SeqCst)
}

pub fn thread_slot_range_end(thread_id: u8) -> u64 {
    THREAD_SLOT_RANGE_END[thread_id as usize].load(Ordering::SeqCst)
}

pub fn thread_current_tx_index(thread_id: u8) -> u32 {
    THREAD_CURRENT_TX_INDEX[thread_id as usize].load(Ordering::SeqCst)
}

pub fn thread_set_slot_range(thread_id: u8, start: u64, end: u64) {
    THREAD_SLOT_RANGE_START[thread_id as usize].store(start, Ordering::SeqCst);
    THREAD_SLOT_RANGE_END[thread_id as usize].store(end, Ordering::SeqCst);
}

pub fn thread_bump_tx_index(thread_id: u8) -> u32 {
    THREAD_CURRENT_TX_INDEX[thread_id as usize].fetch_add(1, Ordering::SeqCst) + 1
}

pub fn thread_set_current_tx_index(thread_id: u8, index: u32) {
    THREAD_CURRENT_TX_INDEX[thread_id as usize].store(index, Ordering::SeqCst);
}

#[derive(Clone, Debug, Default)]
pub struct Jetstreamr;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JetstreamrError {
    ClickHouseError(String),
    ClickHouseInitializationFailed,
}

impl Error for JetstreamrError {}

impl std::fmt::Display for JetstreamrError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            JetstreamrError::ClickHouseError(msg) => write!(f, "ClickHouse error: {}", msg),
            JetstreamrError::ClickHouseInitializationFailed => {
                write!(f, "ClickHouse initialization failed")
            }
        }
    }
}

impl From<JetstreamrError> for GeyserPluginError {
    fn from(err: JetstreamrError) -> Self {
        GeyserPluginError::Custom(Box::new(err))
    }
}

pub fn ipc_send(msg: JetstreamrMessage) {
    DB_CLIENT.with(|db| {
        let db = db.borrow();
        PLUGIN.with(|plugin| {
            let plugin = plugin.borrow();
            TOKIO_RUNTIME.with(|rt_cell| {
                let rt = rt_cell.borrow();
                match msg {
                    JetstreamrMessage::Transaction(tx, tx_index) => {
                        if let Err(e) = rt.block_on(plugin.on_transaction(db.clone(), tx, tx_index))
                        {
                            log::error!("plugin {} on_transaction error: {}", plugin.name(), e);
                        }
                    }
                    JetstreamrMessage::Block(block) => {
                        if let Err(e) = rt.block_on(plugin.on_block(db.clone(), block)) {
                            log::error!("plugin {} on_block error: {}", plugin.name(), e);
                        }
                    }
                    JetstreamrMessage::Exit => {
                        if let Err(e) = rt.block_on(plugin.on_exit(db.clone())) {
                            log::error!("plugin {} on_exit error: {}", plugin.name(), e);
                        }
                    }
                }
            });
        });
    });
    // let tx = IPC_TX.get().expect("IPC_TX not initialized");
    // let is_exit = msg == JetstreamrMessage::Exit;
    // if let Err(err) = tx.blocking_send(msg) {
    //     panic!("IPC channel error: {:?}", err);
    // }
    // if is_exit {
    //     log::info!("sent exit signal to clients");
    // }
}

fn parse_range(slot_range: impl AsRef<str>) -> Option<Range<u64>> {
    let range_str = slot_range.as_ref();
    if range_str.is_empty() {
        return None;
    }
    let parts: Vec<&str> = range_str.split("..").collect();
    if parts.len() != 2 {
        log::error!("Invalid slot range format: {}", range_str);
        return None;
    }
    let start = parts[0].parse::<u64>().ok()?;
    let end = parts[1].parse::<u64>().ok()?;
    Some(start..end)
}

impl GeyserPlugin for Jetstreamr {
    fn name(&self) -> &'static str {
        "GeyserPluginJetstreamr"
    }

    fn on_load(&mut self, config_file: &str, _is_reload: bool) -> Result<()> {
        solana_logger::setup_with_default("info");
        if SLOT_RANGE.get().is_some() {
            log::info!("done, exiting...");
            unload();
            return Ok(());
        }
        log::info!("jetstreamr loading...");

        // process config
        let config_raw_json =
            std::fs::read_to_string(config_file).map_err(GeyserPluginError::from)?;
        let config_json = serde_json::from_str::<serde_json::Value>(&config_raw_json)
            .map_err(|e| GeyserPluginError::ConfigFileReadError { msg: e.to_string() })?;
        log::info!("config file loaded: {:#?}", config_file);
        let spawn_clickhouse = config_json
            .get("clickhouse")
            .unwrap()
            .get("spawn")
            .unwrap()
            .as_bool()
            .unwrap_or(true);
        let threads = config_json
            .get("jetstreamr")
            .unwrap()
            .get("threads")
            .unwrap()
            .as_u64()
            .unwrap_or(1) as u8;
        log::info!("jetstreamr threads: {}", threads);
        let slot_range = config_json
            .get("jetstreamr")
            .unwrap()
            .get("slot_range")
            .unwrap()
            .as_str()
            .unwrap();
        let slot_range = parse_range(slot_range).unwrap();
        log::info!("jetstreamr slot range: {:?}", slot_range);
        SLOT_RANGE.set(slot_range.clone()).unwrap();

        // init subranges + thread info
        let sub_ranges = generate_subranges(&slot_range, threads);
        log::info!("jetstreamr slot sub-ranges: {:?}", sub_ranges);
        let mut thread_info_map = RangeMap::new();
        sub_ranges.iter().enumerate().for_each(|(i, range)| {
            let thread_id = i as u8;
            let mut thread_slot_range = range.start..range.end + 1;
            let thread_current_slot = range.start;

            thread_info_map.insert(thread_slot_range.clone(), thread_id);
            thread_slot_range.end -= 1;
            thread_set_slot_range(thread_id, thread_slot_range.start, thread_slot_range.end);
            thread_set_current_slot(thread_id, thread_current_slot);
            thread_set_current_tx_index(thread_id, 0);

            // Ensure that the range is fully covered since we can't use RangeInclusive
            assert!(*thread_info_map.get(&thread_slot_range.start).unwrap() == thread_id);
            assert!(*thread_info_map.get(&thread_slot_range.end).unwrap() == thread_id);
        });
        log::info!("thread info map: {:#?}", thread_info_map);
        THREAD_INFO.set(thread_info_map).unwrap();

        // start tokio runtime + spawn clickhouse if enabled + initialize IPC bridge
        TOKIO_RUNTIME
            .with(|rt_cell| {
                let rt = rt_cell.borrow();
                rt.block_on(async {
                    if spawn_clickhouse {
                        log::info!("automatic ClickHouse spawning enabled, starting ClickHouse...");
                        let start_result = clickhouse::start().await
                            .map_err(|e| JetstreamrError::ClickHouseError(e.to_string()))?;

                        let (mut ready_rx, clickhouse_future) = start_result;

                        if ready_rx.recv().await.is_some() {
                            log::info!("ClickHouse initialization complete.");
                            rt.spawn(clickhouse_future);
                        } else {
                            return Err(JetstreamrError::ClickHouseInitializationFailed);
                        }
                    } else {
                        log::info!("automatic ClickHouse spawning disabled, skipping ClickHouse initialization.");
                    }

                    log::info!("setting up IPC bridge...");
                    let (tx, rx) = mpsc::channel::<JetstreamrMessage>(1);
                    let handle = spawn_socket_server(rx)
                        .await
                        .map_err(|e| JetstreamrError::ClickHouseError(e.to_string()))?;
                    IPC_TX.set(tx).unwrap();
                    IPC_TASK.set(handle).unwrap();
                    log::info!("IPC bridge initialized.");
                    log::info!("initializing program tracking plugin...");
                    let db = DB_CLIENT.with_borrow(|db| db.clone());
                    let plugin = PLUGIN.with_borrow(|plugin| plugin.clone());
                    plugin.on_load(db).await.unwrap();
                    log::info!("program tracking plugin initialized.");
                    START_TIME.set(Instant::now()).unwrap();
                    log::info!("jetstreamr loaded");

                    ctrlc::set_handler(|| {
                        unload();
                    })
                    .unwrap();
                    Ok(())
                })
            })
            .map_err(|e| {
                log::error!("Error loading jetstreamr: {:?}", e);
                GeyserPluginError::from(e)
            })
    }

    fn on_unload(&mut self) {
        unload();
    }

    fn notify_block_metadata(&self, blockinfo: ReplicaBlockInfoVersions) -> Result<()> {
        if exiting() {
            return Ok(());
        }
        // TX_INDEX.with_borrow_mut(|index| {
        //     *index = 0;
        // });
        let slot = match blockinfo {
            ReplicaBlockInfoVersions::V0_0_1(block_info) => block_info.slot,
            ReplicaBlockInfoVersions::V0_0_2(block_info) => block_info.slot,
            ReplicaBlockInfoVersions::V0_0_3(block_info) => block_info.slot,
            ReplicaBlockInfoVersions::V0_0_4(block_info) => block_info.slot,
        };
        let processed_slots = PROCESSED_SLOTS.fetch_add(1, Ordering::SeqCst) + 1;

        let range_map = THREAD_INFO.get().unwrap();
        let thread_id = *range_map.get(&slot).unwrap();
        let thread_slot_range_end = thread_slot_range_end(thread_id);
        thread_set_current_slot(thread_id, slot);

        if slot >= thread_slot_range_end || processed_slots % 100 == 0 {
            let processed_txs = PROCESSED_TRANSACTIONS.load(Ordering::SeqCst);
            let num_votes = NUM_VOTES.load(Ordering::SeqCst);
            let compute_consumed = *COMPUTE_CONSUMED.lock().unwrap();

            let overall_tps = {
                let start_time = START_TIME.get().unwrap();
                let elapsed = start_time.elapsed().as_secs_f64();
                processed_txs as f64 / elapsed
            };

            let epoch = slot_to_epoch(slot);

            // let overall_slot_range = SLOT_RANGE.get().unwrap();
            // let percent = processed_slots as f64
            //     / (overall_slot_range.end - overall_slot_range.start) as f64
            //     * 100.0;

            let thread_slot_range_start = thread_slot_range_start(thread_id);
            let thread_percent = {
                (slot - thread_slot_range_start) as f64
                    / (thread_slot_range_end - thread_slot_range_start) as f64
                    * 100.0
            };

            log::info!(
                "thread {} at slot {} epoch {} ({:.2}%), processed {} txs ({} non-vote) using {} CU across {} slots | AVG TPS: {:.3}",
                thread_id,
                slot,
                epoch,
                thread_percent,
                processed_txs.separate_with_commas(),
                (processed_txs - num_votes).separate_with_commas(),
                compute_consumed.separate_with_commas(),
                processed_slots.separate_with_commas(),
                overall_tps
            );
        }

        let blk = Block::from_replica(blockinfo);
        ipc_send(JetstreamrMessage::Block(blk));

        if slot >= thread_slot_range_end {
            log::info!(
                "thread {} finished processing slot {} and has completed its work",
                thread_id,
                slot
            );

            let complete_threads = COMPLETE_THREADS.fetch_add(1, Ordering::SeqCst) + 1;
            let jetstreamr_threads = range_map.len() as u8;
            if complete_threads >= jetstreamr_threads {
                log::info!(
                    "all {} threads have completed their work, unloading jetstreamr...",
                    jetstreamr_threads
                );
                unload();
            } else {
                log::info!(
                    "waiting for {} more threads to complete their work",
                    jetstreamr_threads - complete_threads
                );
            }
        }

        Ok(())
    }

    fn notify_transaction(
        &self,
        transaction: ReplicaTransactionInfoVersions,
        slot: u64,
    ) -> Result<()> {
        if exiting() {
            return Ok(());
        }
        match transaction {
            ReplicaTransactionInfoVersions::V0_0_1(tx) => {
                if let Some(consumed) = tx.transaction_status_meta.compute_units_consumed {
                    *COMPUTE_CONSUMED.lock().unwrap() += u128::from(consumed);
                }
            }
            ReplicaTransactionInfoVersions::V0_0_2(tx) => {
                if let Some(consumed) = tx.transaction_status_meta.compute_units_consumed {
                    *COMPUTE_CONSUMED.lock().unwrap() += u128::from(consumed);
                }
            }
        }
        PROCESSED_TRANSACTIONS.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let tx = Transaction::from_replica(slot, transaction);
        if tx.is_vote {
            NUM_VOTES.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
        let thread_id = *THREAD_INFO
            .get()
            .unwrap()
            .get(&slot)
            .expect("thread id not found for slot");
        let thread_current_tx_index = thread_bump_tx_index(thread_id);
        ipc_send(JetstreamrMessage::Transaction(tx, thread_current_tx_index));
        Ok(())
    }

    fn transaction_notifications_enabled(&self) -> bool {
        true
    }

    fn account_data_notifications_enabled(&self) -> bool {
        true
    }

    fn entry_notifications_enabled(&self) -> bool {
        true
    }
}

#[inline(always)]
fn exiting() -> bool {
    EXIT.load(std::sync::atomic::Ordering::SeqCst)
}

fn stop_ipc_bridge() {
    log::info!("stopping IPC bridge...");
    if let Some(handle) = IPC_TASK.get() {
        handle.abort(); // kills writer + accept loops
    }
    log::info!("IPC bridge stopped.");
}

fn stop_tx_queue() {
    log::info!("stopping queueing of transactions...");
    EXIT.store(true, std::sync::atomic::Ordering::SeqCst);
    log::info!("queueing of transactions has stopped.");
}

fn send_exit_signal_to_clients() {
    log::info!("sending exit signal to clients...");
    ipc_send(JetstreamrMessage::Exit);
    log::info!("exit signal sent to clients.");
}

fn stop_clickhouse() {
    log::info!("stopping ClickHouse...");
    clickhouse::stop_sync();
    log::info!("ClickHouse stopped.");
}

fn clear_domain_socket() {
    log::info!("clearing domain socket...");
    std::fs::remove_file("/tmp/jetstreamr.sock").unwrap_or_else(|_| {
        log::warn!("failed to remove domain socket");
    });
    log::info!("domain socket cleared.");
}

#[inline(always)]
fn unload() {
    log::info!("jetstreamr unloading...");
    stop_tx_queue();
    send_exit_signal_to_clients();
    stop_ipc_bridge();
    stop_clickhouse();
    clear_domain_socket();
    log::info!("jetstreamr has successfully unloaded.");
    std::process::exit(0);
}

pub trait SolscanUrl {
    /// Returns the Solscan URL associated with `self`
    fn solscan_url(&self) -> String;
}

impl SolscanUrl for ReplicaTransactionInfoVersions<'_> {
    fn solscan_url(&self) -> String {
        match self {
            ReplicaTransactionInfoVersions::V0_0_1(tx) => {
                format!("https://solscan.io/tx/{}", tx.signature)
            }
            ReplicaTransactionInfoVersions::V0_0_2(tx) => {
                format!("https://solscan.io/tx/{}", tx.signature)
            }
        }
    }
}
