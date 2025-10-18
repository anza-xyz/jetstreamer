#[tokio::main(flavor = "multi_thread")]
async fn main() {
    solana_logger::setup_with_default("info");
    jetstreamer_utils::start_client().await.unwrap();
}
