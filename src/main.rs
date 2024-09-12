use std::env;

pub mod dev_db;

fn main() {
    tracing_subscriber::fmt().init();
    let _ = dotenvy::from_path(".env");
    if env::var("DEV_MODE").is_ok() {
        dev_db::spawn_click_house();
    }
    tracing::info!("hello world");
}
