use agave_geyser_plugin_interface::geyser_plugin_interface::{
    GeyserPlugin, GeyserPluginError, ReplicaBlockInfoVersions, ReplicaTransactionInfoVersions,
    Result,
};
use mpsc::Sender;
use once_cell::unsync::OnceCell;
use std::{
    cell::RefCell,
    error::Error,
    sync::atomic::{AtomicBool, AtomicU64},
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
    static COMPUTE_CONSUMED: RefCell<u128> = const { RefCell::new(0) };
    static START_TIME: std::cell::RefCell<Option<Instant>> = const { RefCell::new(None) };
    static IPC_TX: OnceCell<Sender<SoliraMessage>> = const { OnceCell::new() };
    static IPC_TASK: OnceCell<tokio::task::JoinHandle<()>> = const { OnceCell::new() };
    static TX_INDEX: RefCell<u32> = const { RefCell::new(0) };
}

static EXIT: AtomicBool = AtomicBool::new(false);
static PROCESSED_TRANSACTIONS: AtomicU64 = AtomicU64::new(0);
static SLOT_NUM: AtomicU64 = AtomicU64::new(0);
static PROCESSED_SLOTS: AtomicU64 = AtomicU64::new(0);
static NUM_VOTES: AtomicU64 = AtomicU64::new(0);

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

impl GeyserPlugin for Solira {
    fn name(&self) -> &'static str {
        "GeyserPluginSolira"
    }

    fn on_load(&mut self, _config_file: &str, _is_reload: bool) -> Result<()> {
        solana_logger::setup_with_default("info");
        log::info!("solira loading...");

        TOKIO_RUNTIME
            .with(|rt_cell| {
                let rt = rt_cell.borrow();
                rt.block_on(async {
                    let (mut ready_rx, clickhouse_future) = clickhouse::start()
                        .await
                        .map_err(|e| SoliraError::ClickHouseError(e.to_string()))?;

                    if ready_rx.recv().await.is_some() {
                        log::info!("ClickHouse initialization complete.");
                        rt.spawn(clickhouse_future);

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

                        START_TIME.with(|st| *st.borrow_mut() = Some(Instant::now()));
                        log::info!("solira loaded");

                        ctrlc::set_handler(|| {
                            unload();
                        })
                        .unwrap();
                        Ok(())
                    } else {
                        Err(SoliraError::ClickHouseInitializationFailed)
                    }
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
        TX_INDEX.with_borrow_mut(|index| {
            *index = 0;
        });
        let slot = match blockinfo {
            ReplicaBlockInfoVersions::V0_0_1(block_info) => block_info.slot,
            ReplicaBlockInfoVersions::V0_0_2(block_info) => block_info.slot,
            ReplicaBlockInfoVersions::V0_0_3(block_info) => block_info.slot,
            ReplicaBlockInfoVersions::V0_0_4(block_info) => block_info.slot,
        };
        SLOT_NUM.store(slot, std::sync::atomic::Ordering::SeqCst);
        let processed_slots = PROCESSED_SLOTS.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
        if processed_slots % 10 == 0 {
            let processed_txs = PROCESSED_TRANSACTIONS.load(std::sync::atomic::Ordering::SeqCst);
            // let num_votes = NUM_VOTES.with_borrow(|votes| *votes);
            let compute_consumed = COMPUTE_CONSUMED.with_borrow(|compute| *compute);

            let overall_tps = START_TIME.with(|start_time| {
                let mut start_time = start_time.borrow_mut();
                if start_time.is_none() {
                    *start_time = Some(Instant::now());
                }
                let elapsed = start_time.unwrap().elapsed().as_secs_f64();
                processed_txs as f64 / elapsed
            });

            log::info!(
                "at slot {}, processed {} transactions consuming {} CU across {} slots | Overall TPS: {:.2}",
                slot,
                processed_txs.separate_with_commas(),
                compute_consumed.separate_with_commas(),
                processed_slots.separate_with_commas(),
                overall_tps
            );
        }

        let blk = Block::from_replica(blockinfo);
        ipc_send(SoliraMessage::Block(blk));

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
                if tx.is_vote {
                    NUM_VOTES.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                }
                if let Some(consumed) = tx.transaction_status_meta.compute_units_consumed {
                    COMPUTE_CONSUMED.with_borrow_mut(|compute| {
                        *compute += u128::from(consumed);
                    });
                }
            }
            ReplicaTransactionInfoVersions::V0_0_2(tx) => {
                if tx.is_vote {
                    NUM_VOTES.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                }
                if let Some(consumed) = tx.transaction_status_meta.compute_units_consumed {
                    COMPUTE_CONSUMED.with_borrow_mut(|compute| {
                        *compute += u128::from(consumed);
                    });
                }
            }
        }
        PROCESSED_TRANSACTIONS.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let tx = Transaction::from_replica(slot, transaction);
        TX_INDEX.with_borrow_mut(|index| {
            ipc_send(SoliraMessage::Transaction(tx, *index));
            *index += 1;
        });
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
