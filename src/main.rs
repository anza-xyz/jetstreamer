use std::{env, process::exit};

pub mod dev_db;

#[tokio::main]
async fn main() {
    solana_logger::setup_with_default("info");
    let _ = dotenvy::from_path(".env");

    if env::var("DEV_MODE").is_ok() {
        log::info!("dev mode is enabled");
        // Spawn the ClickHouse process and await the readiness signal
        let (mut ready_rx, clickhouse_future) = dev_db::spawn_click_house().await.unwrap();

        // Wait until ClickHouse is ready to accept connections
        if ready_rx.recv().await.is_some() {
            log::info!("ClickHouse dev-mode initialization complete.");

            // Optionally, spawn the ClickHouse process in the background
            tokio::spawn(clickhouse_future);
        } else {
            log::error!("Failed to receive ClickHouse readiness signal.");
            exit(1)
        }
    }

    // "hello world" is logged at the end of the `main` function, regardless of DEV_MODE
    log::info!("solira initialized");
    loop {
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    }
}
