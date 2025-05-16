#[tokio::main(flavor = "multi_thread")]
async fn main() {
    solana_logger::setup_with_default("info");
    println!();
    solira::clickhouse::start_client().await;
}
