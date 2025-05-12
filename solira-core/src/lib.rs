pub mod bridge;
pub mod clickhouse;
pub mod config;
pub mod geyser;
pub mod ipc;
pub mod plugins;

use agave_geyser_plugin_interface::geyser_plugin_interface::GeyserPlugin;

/// # Safety
///
/// The Solana validator and this plugin must be compiled with the same Rust compiler version and Solana core version.
/// Loading this plugin with mismatching versions is undefined behavior and will likely cause memory corruption.
#[unsafe(no_mangle)]
#[allow(improper_ctypes_definitions)]
pub unsafe extern "C" fn _create_plugin() -> *mut dyn GeyserPlugin {
    let plugin: Box<dyn GeyserPlugin> = Box::new(geyser::Solira);
    Box::into_raw(plugin)
}
