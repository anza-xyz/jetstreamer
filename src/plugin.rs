use agave_geyser_plugin_interface::geyser_plugin_interface::{
    GeyserPlugin, GeyserPluginError, ReplicaAccountInfoVersions, ReplicaBlockInfoVersions,
    ReplicaTransactionInfoVersions, Result,
};
use solana_program::pubkey::Pubkey;
use std::{cell::RefCell, error::Error};
use tokio::runtime::Runtime;

use crate::clickhouse;

thread_local! {
    static TOKIO_RUNTIME: RefCell<Runtime> = RefCell::new(Runtime::new().unwrap());
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
                    let (mut ready_rx, clickhouse_future) =
                        clickhouse::spawn_click_house()
                            .await
                            .map_err(|e| SoliraError::ClickHouseError(e.to_string()))?;

                    if ready_rx.recv().await.is_some() {
                        log::info!("ClickHouse initialization complete.");
                        rt.spawn(clickhouse_future);
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
    }

    fn notify_block_metadata(&self, _blockinfo: ReplicaBlockInfoVersions) -> Result<()> {
        log::info!("got block metadata");
        Ok(())
    }

    fn update_account(
        &self,
        account: ReplicaAccountInfoVersions,
        slot: u64,
        _is_startup: bool,
    ) -> Result<()> {
        let pubkey_bytes = match account {
            ReplicaAccountInfoVersions::V0_0_1(account_info) => account_info.pubkey,
            ReplicaAccountInfoVersions::V0_0_2(account_info) => account_info.pubkey,
            ReplicaAccountInfoVersions::V0_0_3(account_info) => account_info.pubkey,
        };

        log::info!(
            "account {:?} updated at slot {}!",
            Pubkey::try_from(pubkey_bytes).unwrap(),
            slot
        );

        Ok(())
    }

    fn notify_transaction(
        &self,
        _transaction: ReplicaTransactionInfoVersions,
        _slot: u64,
    ) -> Result<()> {
        log::info!("got transaction");
        Ok(())
    }

    fn transaction_notifications_enabled(&self) -> bool {
        true
    }

    fn account_data_notifications_enabled(&self) -> bool {
        true
    }
}
