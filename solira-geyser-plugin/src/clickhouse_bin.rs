use solira::clickhouse::*;

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    solana_logger::setup_with_default("info");
    ctrlc::set_handler(|| {
        stop_sync();
    })
    .unwrap();
    let (mut ready_rx, clickhouse_future) = start().await.unwrap();
    log::info!("Waiting for ClickHouse to be ready...");
    if let Some(_) = ready_rx.recv().await {
        log::info!("ClickHouse is ready!");
    }
    // Wait for the ClickHouse process to finish
    clickhouse_future.await.unwrap();
}
