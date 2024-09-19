use std::env;

pub mod dev_db;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt().init();
    let _ = dotenvy::from_path(".env");

    if env::var("DEV_MODE").is_ok() {
        tracing::info!("dev mode is enabled");
        // Spawn the ClickHouse process and await the readiness signal
        let (mut ready_rx, clickhouse_future) = dev_db::spawn_click_house().await.unwrap();

        // Wait until ClickHouse is ready to accept connections
        if ready_rx.recv().await.is_some() {
            tracing::info!("ClickHouse is ready, proceeding with application...");

            // Optionally, spawn the ClickHouse process in the background
            tokio::spawn(clickhouse_future);
        } else {
            tracing::error!("Failed to receive ClickHouse readiness signal.");
        }
    }

    // "hello world" is logged at the end of the `main` function, regardless of DEV_MODE
    tracing::info!("hello world");
    loop {
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    }
}
