use solana_geyser_plugin_interface::geyser_plugin_interface::{
    GeyserPlugin, GeyserPluginError, Result,
};
use std::{cell::RefCell, error::Error};
use tokio::runtime::Runtime;

pub mod db;

thread_local! {
    static TOKIO_RUNTIME: RefCell<Runtime> = RefCell::new(Runtime::new().unwrap());
}

#[derive(Clone, Debug, Default)]
pub struct Solira;

#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub enum SoliraError {
    ClickHouseThreadSpawnFailed,
    ClickHouseInitializationFailed,
}

impl Error for SoliraError {}

impl std::fmt::Display for SoliraError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            SoliraError::ClickHouseThreadSpawnFailed => {
                write!(f, "Failed to spawn ClickHouse thread")
            }
            SoliraError::ClickHouseInitializationFailed => {
                write!(f, "ClickHouse initialization failed")
            }
        }
    }
}

impl GeyserPlugin for Solira {
    fn name(&self) -> &'static str {
        "GeyserPluginSolira"
    }

    fn on_load(&mut self, _config_file: &str, _is_reload: bool) -> Result<()> {
        solana_logger::setup_with_default("info");

        // Use a closure with a return type `Result<()>`
        TOKIO_RUNTIME.with(|rt_cell| {
            let rt = rt_cell.borrow();
            rt.block_on(async {
                let (mut ready_rx, clickhouse_future) =
                    db::spawn_click_house().await.map_err(|e| {
                        log::error!("error: {}", e);
                        GeyserPluginError::Custom(Box::new(
                            SoliraError::ClickHouseThreadSpawnFailed,
                        ))
                    })?;

                if ready_rx.recv().await.is_some() {
                    log::info!("ClickHouse initialization complete.");
                    rt.spawn(clickhouse_future);
                    Ok(())
                } else {
                    Err(GeyserPluginError::Custom(Box::new(
                        SoliraError::ClickHouseInitializationFailed,
                    )))
                }
            })
        })
    }
}
