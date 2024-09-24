pub mod db;

#[tokio::main]
async fn main() {
    log::info!("solira initialized");
    loop {
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    }
}
