use crossbeam_channel::unbounded;
use solana_geyser_plugin_manager::geyser_plugin_service::GeyserPluginService;
use solana_logger::setup_with_default;
use solana_rpc::optimistically_confirmed_bank_tracker::SlotNotification;
use solana_test_validator::TestValidatorGenesis;
use std::{
    env,
    path::PathBuf,
    sync::{atomic::AtomicBool, Arc, RwLock},
};

fn main() {
    // Initialize logging
    setup_with_default("info");

    // Initialize the Solana test validator
    let test_validator = TestValidatorGenesis::default().start();

    let out_dir = env::var("OUT_DIR").expect("OUT_DIR not set");
    let plugin_config_file = PathBuf::from(format!("{}/plugin_config.json", out_dir));

    // Channel to receive slot notifications
    let (slot_sender, slot_receiver) = unbounded::<SlotNotification>();

    // Set up the Geyser Plugin Service and inject the solira plugin
    let geyser_plugin_service = GeyserPluginService::new(
        slot_receiver,
        &[plugin_config_file], // Inject the solira plugin config here
    )
    .expect("Failed to initialize Geyser Plugin Service");

    println!("Geyser plugin service initialized with solira plugin.");

    // Keep the validator running to continue testing the plugin
    std::thread::park();
}
