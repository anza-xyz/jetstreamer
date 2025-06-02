use agave_geyser_plugin_interface::geyser_plugin_interface::{
    GeyserPlugin, GeyserPluginError, ReplicaBlockInfoVersions, ReplicaTransactionInfoVersions,
    Result,
};
use core::ops::Range;
use geyser_replay::{epochs::slot_to_epoch, firehose::generate_subranges};
use mpsc::Sender;
use once_cell::unsync::OnceCell;
use rangemap::RangeMap;
use std::{
    cell::RefCell,
    error::Error,
    sync::{
        Mutex,
        atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering},
    },
    time::Instant,
};
use thousands::Separable;
use tokio::runtime::Runtime;
use tokio::sync::mpsc;

use crate::clickhouse;
use solira_plugin::{
    bridge::{Block, Transaction},
    ipc::{SoliraMessage, spawn_socket_server},
};

thread_local! {
    static TOKIO_RUNTIME: RefCell<Runtime> = RefCell::new(Runtime::new().unwrap());
    static IPC_TX: OnceCell<Sender<SoliraMessage>> = OnceCell::new();
    static IPC_TASK: OnceCell<tokio::task::JoinHandle<()>> = OnceCell::new();
}

static EXIT: AtomicBool = AtomicBool::new(false);
static PROCESSED_TRANSACTIONS: AtomicU64 = AtomicU64::new(0);
static PROCESSED_SLOTS: AtomicU64 = AtomicU64::new(0);
static NUM_VOTES: AtomicU64 = AtomicU64::new(0);
static COMPLETE_THREADS: AtomicU8 = AtomicU8::new(0);

static COMPUTE_CONSUMED: Mutex<u128> = Mutex::new(0);

static START_TIME: once_cell::sync::OnceCell<Instant> = once_cell::sync::OnceCell::new();
static SLOT_RANGE: once_cell::sync::OnceCell<Range<u64>> = once_cell::sync::OnceCell::new();
static THREAD_INFO: once_cell::sync::OnceCell<RangeMap<u64, ThreadInfo>> =
    once_cell::sync::OnceCell::new();

#[derive(Clone, Debug, Default)]
pub struct Solira;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SoliraError {
    ClickHouseError(String),
    ClickHouseInitializationFailed,
}

impl Error for SoliraError {}

impl std::fmt::Display for SoliraError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            SoliraError::ClickHouseError(msg) => write!(f, "ClickHouse error: {}", msg),
            SoliraError::ClickHouseInitializationFailed => {
                write!(f, "ClickHouse initialization failed")
            }
        }
    }
}

impl From<SoliraError> for GeyserPluginError {
    fn from(err: SoliraError) -> Self {
        GeyserPluginError::Custom(Box::new(err))
    }
}

#[derive(Copy, Clone, PartialEq, Eq, Debug)]
struct ThreadInfo {
    thread_id: u8,
    slot_range: (u64, u64),
    // current_slot: u64,
    //current_tx_index: u32,
}

pub fn ipc_send(msg: SoliraMessage) {
    IPC_TX.with(|cell| {
        if let Some(tx) = cell.get() {
            let is_exit = msg == SoliraMessage::Exit;
            if let Err(err) = tx.blocking_send(msg) {
                panic!("IPC channel error: {:?}", err);
            }
            if is_exit {
                log::info!("sent exit signal to clients");
            }
        }
    });
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

impl GeyserPlugin for Solira {
    fn name(&self) -> &'static str {
        "GeyserPluginSolira"
    }

    fn on_load(&mut self, config_file: &str, _is_reload: bool) -> Result<()> {
        solana_logger::setup_with_default("info");
        if SLOT_RANGE.get().is_some() {
            log::info!("done, exiting...");
            unload();
            return Ok(());
        }
        log::info!("solira loading...");

        // process config
        let config_raw_json =
            std::fs::read_to_string(config_file).map_err(|e| GeyserPluginError::from(e))?;
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
            .get("solira")
            .unwrap()
            .get("threads")
            .unwrap()
            .as_u64()
            .unwrap_or(1) as u8;
        log::info!("solira threads: {}", threads);
        let slot_range = config_json
            .get("solira")
            .unwrap()
            .get("slot_range")
            .unwrap()
            .as_str()
            .unwrap();
        let slot_range = parse_range(slot_range).unwrap();
        log::info!("solira slot range: {:?}", slot_range);
        SLOT_RANGE.set(slot_range.clone()).unwrap();

        // init subranges + thread info
        let sub_ranges = generate_subranges(&slot_range, threads);
        log::info!("solira slot sub-ranges: {:?}", sub_ranges);
        let mut thread_info_map = RangeMap::new();
        sub_ranges
            .iter()
            .enumerate()
            .map(|(i, range)| ThreadInfo {
                thread_id: i as u8,
                slot_range: (range.start, range.end),
                // current_slot: range.start,
                // current_tx_index: 0,
            })
            .for_each(|info| {
                thread_info_map.insert(info.slot_range.0..info.slot_range.1 + 1, info);
                // Ensure that the range is fully covered since we can't use RangeInclusive
                assert!(
                    thread_info_map.get(&info.slot_range.0).unwrap().thread_id == info.thread_id
                );
                assert!(
                    thread_info_map.get(&info.slot_range.1).unwrap().thread_id == info.thread_id
                );
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
                        let (mut ready_rx, clickhouse_future) = clickhouse::start()
                            .await
                            .map_err(|e| SoliraError::ClickHouseError(e.to_string()))?;

                        if ready_rx.recv().await.is_some() {
                            log::info!("ClickHouse initialization complete.");
                            rt.spawn(clickhouse_future);
                        } else {
                            return Err(SoliraError::ClickHouseInitializationFailed);
                        }
                    } else {
                        log::info!("automatic ClickHouse spawning disabled, skipping ClickHouse initialization.");
                    }

                    log::info!("setting up IPC bridge...");
                    let (tx, rx) = mpsc::channel::<SoliraMessage>(1);
                    let handle = spawn_socket_server(rx)
                        .await
                        .map_err(|e| SoliraError::ClickHouseError(e.to_string()))?;
                    IPC_TX.with(|cell| {
                        let _ = cell.set(tx);
                    }); // <- inside `with`
                    IPC_TASK.with(|cell| {
                        let _ = cell.set(handle);
                    });
                    log::info!("IPC bridge initialized.");

                    START_TIME.set(Instant::now()).unwrap();
                    log::info!("solira loaded");

                    ctrlc::set_handler(|| {
                        unload();
                    })
                    .unwrap();
                    Ok(())
                })
            })
            .map_err(|e| {
                log::error!("Error loading solira: {:?}", e);
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
        let thread_info = range_map.get(&slot).unwrap();

        if slot >= thread_info.slot_range.1 - 1 || processed_slots % 100 == 0 {
            let processed_txs = PROCESSED_TRANSACTIONS.load(Ordering::SeqCst);
            let num_votes = NUM_VOTES.load(Ordering::SeqCst);
            let compute_consumed = COMPUTE_CONSUMED.lock().unwrap().clone();

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

            let thread_percent = {
                let thread_range = thread_info.slot_range;
                (slot - thread_range.0) as f64 / (thread_range.1 - thread_range.0) as f64 * 100.0
            };

            log::info!(
                "thread {} at slot {} epoch {} ({:.2}%), processed {} txs ({} non-vote) using {} CU across {} slots | AVG TPS: {:.2}",
                thread_info.thread_id,
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
        ipc_send(SoliraMessage::Block(blk));

        if slot >= thread_info.slot_range.1 - 1 {
            log::info!(
                "thread {} finished processing slot {} and has completed its work",
                thread_info.thread_id,
                slot
            );

            let complete_threads = COMPLETE_THREADS.fetch_add(1, Ordering::SeqCst) + 1;
            let solira_threads = range_map.len() as u8;
            if complete_threads >= solira_threads {
                log::info!(
                    "all {} threads have completed their work, unloading solira...",
                    solira_threads
                );
                unload();
            } else {
                log::info!(
                    "waiting for {} more threads to complete their work",
                    solira_threads - complete_threads
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
        // TODO: tx index
        ipc_send(SoliraMessage::Transaction(tx, 0));
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
    IPC_TASK.with(|cell| {
        if let Some(handle) = cell.get() {
            handle.abort(); // kills writer + accept loops
        }
    });
    log::info!("IPC bridge stopped.");
}

fn stop_tx_queue() {
    log::info!("stopping queueing of transactions...");
    EXIT.store(true, std::sync::atomic::Ordering::SeqCst);
    log::info!("queueing of transactions has stopped.");
}

fn send_exit_signal_to_clients() {
    log::info!("sending exit signal to clients...");
    ipc_send(SoliraMessage::Exit);
    log::info!("exit signal sent to clients.");
}

fn stop_clickhouse() {
    log::info!("stopping ClickHouse...");
    clickhouse::stop_sync();
    log::info!("ClickHouse stopped.");
}

fn clear_domain_socket() {
    log::info!("clearing domain socket...");
    std::fs::remove_file("/tmp/solira.sock").unwrap_or_else(|_| {
        log::warn!("failed to remove domain socket");
    });
    log::info!("domain socket cleared.");
}

#[inline(always)]
fn unload() {
    log::info!("solira unloading...");
    stop_tx_queue();
    send_exit_signal_to_clients();
    stop_ipc_bridge();
    stop_clickhouse();
    clear_domain_socket();
    log::info!("solira has successfully unloaded.");
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
