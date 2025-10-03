use jetstreamer_firehose::index::build_missing_indexes;

#[tokio::main(worker_threads = 64, flavor = "multi_thread")]
async fn main() {
    solana_logger::setup_with_default("info");
    let idx_dir = std::path::PathBuf::from("./src/index");
    build_missing_indexes(&idx_dir).await.unwrap();
}
