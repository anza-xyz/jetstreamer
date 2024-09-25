use std::time::Duration;

use solana_geyser_plugin_interface::geyser_plugin_interface::GeyserPlugin;
use solira::*;

pub mod db;

fn main() {
    log::info!("solira launched in local dev mode");
    let mut solira = Solira::default();
    solira.on_load("", false).unwrap();
    log::info!("on_load complete");
    loop {
        std::thread::sleep(Duration::from_secs(1));
    }
}
