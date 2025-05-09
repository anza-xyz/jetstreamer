pub mod bridge;
pub mod clickhouse;
pub mod geyser;
pub mod ipc;
pub mod plugin;

pub use geyser::Solira;

use agave_geyser_plugin_interface::geyser_plugin_interface::GeyserPlugin;

/// # Safety
///
/// The Solana validator and this plugin must be compiled with the same Rust compiler version and Solana core version.
/// Loading this plugin with mismatching versions is undefined behavior and will likely cause memory corruption.
#[no_mangle]
#[allow(improper_ctypes_definitions)]
pub unsafe extern "C" fn _create_plugin() -> *mut dyn GeyserPlugin {
    ctrlc::set_handler(|| {
        log::info!("Ctrl-C received — exiting cleanly.");
        #[cfg(unix)]
        let _ = std::fs::remove_file("/tmp/solira.sock");
        std::process::exit(0);
    })
    .unwrap();
    let plugin: Box<dyn GeyserPlugin> = Box::new(Solira);
    Box::into_raw(plugin)
}
