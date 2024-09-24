use std::error::Error;

use solana_geyser_plugin_interface::geyser_plugin_interface::{
    GeyserPlugin, GeyserPluginError, Result,
};
use tokio::runtime::Runtime;

pub mod db;

#[derive(Clone, Debug, Default)]
pub struct Solira;

#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub enum SoliraError {
    ClickHouseInitializationFailed,
}

impl Error for SoliraError {}

impl std::fmt::Display for SoliraError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "ClickHouse initialization failed")
    }
}

impl GeyserPlugin for Solira {
    fn name(&self) -> &'static str {
        "GeyserPluginSolira"
    }

    fn on_load(&mut self, _config_file: &str, _is_reload: bool) -> Result<()> {
        solana_logger::setup_with_default("info");

        let rt = Runtime::new()?;

        // Spawn a background task without blocking
        rt.block_on(async {
            // Spawn the ClickHouse process and await the readiness signal
            let (mut ready_rx, clickhouse_future) = db::spawn_click_house().await.unwrap();

            // Wait until ClickHouse is ready to accept connections
            if ready_rx.recv().await.is_some() {
                log::info!("ClickHouse initialization complete.");

                // Optionally, spawn the ClickHouse process in the background
                tokio::spawn(clickhouse_future);
            } else {
                // return Err(GeyserPluginError::Custom(Box::new(SoliraError::ClickHouseInitializationFailed)));
            }
        });

        Ok(())
    }
}
