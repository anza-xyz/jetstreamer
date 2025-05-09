use agave_geyser_plugin_interface::geyser_plugin_interface::{
    GeyserPlugin, GeyserPluginError, ReplicaBlockInfoVersions, ReplicaTransactionInfoVersions,
    Result,
};
use once_cell::unsync::OnceCell;
use std::{cell::RefCell, error::Error, time::Instant};
use thousands::Separable;
use tokio::runtime::Runtime;
use tokio::sync::broadcast;

use crate::bridge::{Block, Transaction};
use crate::clickhouse;
use crate::ipc::SoliraMessage;

thread_local! {
    static TOKIO_RUNTIME: RefCell<Runtime> = RefCell::new(Runtime::new().unwrap());
    static PROCESSED_TRANSACTIONS: RefCell<u64> = const { RefCell::new(0) };
    static SLOT_NUM: RefCell<u64> = const { RefCell::new(0) };
    static PROCESSED_SLOTS: RefCell<u64> = const { RefCell::new(0) };
    static NUM_VOTES: RefCell<u64> = const { RefCell::new(0) };
    static COMPUTE_CONSUMED: RefCell<u128> = const { RefCell::new(0) };
    static START_TIME: std::cell::RefCell<Option<Instant>> = const { RefCell::new(None) };
    static IPC_TX:     OnceCell<broadcast::Sender<SoliraMessage>> = OnceCell::new();
    static IPC_TASK:   OnceCell<tokio::task::JoinHandle<()>>    = OnceCell::new();
}

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
            let _ = tx.send(msg); // drop errors if channel is full / no receivers
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
                        let (tx, _rx) = broadcast::channel::<SoliraMessage>(16_384);
                        let handle = crate::ipc::spawn_socket_server(tx.clone())
                            .await
                            .map_err(|e| SoliraError::ClickHouseError(e.to_string()))?;
                        IPC_TX.with(|cell| {
                            let _ = cell.set(tx);
                        });
                        IPC_TASK.with(|cell| {
                            let _ = cell.set(handle);
                        });
                        log::info!("IPC bridge initialized.");

                        START_TIME.with(|st| *st.borrow_mut() = Some(Instant::now()));
                        log::info!("solira loaded");
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
        log::info!("solira unloading...");
        clickhouse::stop_sync();
    }

    fn notify_block_metadata(&self, blockinfo: ReplicaBlockInfoVersions) -> Result<()> {
        let slot = match blockinfo {
            ReplicaBlockInfoVersions::V0_0_1(block_info) => block_info.slot,
            ReplicaBlockInfoVersions::V0_0_2(block_info) => block_info.slot,
            ReplicaBlockInfoVersions::V0_0_3(block_info) => block_info.slot,
            ReplicaBlockInfoVersions::V0_0_4(block_info) => block_info.slot,
        };
        SLOT_NUM.set(slot);
        let processed_slots = PROCESSED_SLOTS.with_borrow_mut(|slots| {
            *slots += 1;
            *slots
        });
        if processed_slots % 100 == 0 {
            let processed_txs = PROCESSED_TRANSACTIONS.with_borrow(|txs| *txs);
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
        match transaction {
            ReplicaTransactionInfoVersions::V0_0_1(tx) => {
                if tx.is_vote {
                    NUM_VOTES.with_borrow_mut(|votes| {
                        *votes += 1;
                    });
                }
                if let Some(consumed) = tx.transaction_status_meta.compute_units_consumed {
                    COMPUTE_CONSUMED.with_borrow_mut(|compute| {
                        *compute += u128::from(consumed);
                    });
                }
            }
            ReplicaTransactionInfoVersions::V0_0_2(tx) => {
                if tx.is_vote {
                    NUM_VOTES.with_borrow_mut(|votes| {
                        *votes += 1;
                    });
                }
                if let Some(consumed) = tx.transaction_status_meta.compute_units_consumed {
                    COMPUTE_CONSUMED.with_borrow_mut(|compute| {
                        *compute += u128::from(consumed);
                    });
                }
            }
        }
        PROCESSED_TRANSACTIONS.with_borrow_mut(|tx_count| {
            *tx_count += 1;
        });
        let tx = Transaction::from_replica(slot, transaction);
        ipc_send(SoliraMessage::Transaction(tx));
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
