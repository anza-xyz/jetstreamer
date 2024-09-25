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

#[derive(Clone, PartialEq, Eq, Debug)]
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

        TOKIO_RUNTIME.with(|rt_cell| {
            let rt = rt_cell.borrow();
            rt.block_on(async {
                let (mut ready_rx, clickhouse_future) =
                    db::spawn_click_house().await.map_err(|e| {
                        GeyserPluginError::from(SoliraError::ClickHouseError(e.to_string()))
                    })?;

                if ready_rx.recv().await.is_some() {
                    log::info!("ClickHouse initialization complete.");
                    rt.spawn(clickhouse_future);
                    Ok(())
                } else {
                    Err(SoliraError::ClickHouseInitializationFailed.into())
                }
            })
        })
    }
}
