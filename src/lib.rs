use solana_geyser_plugin_interface::geyser_plugin_interface::{GeyserPlugin, Result};

pub mod dev_db;

#[derive(Clone, Debug, Default)]
pub struct Solira;

impl GeyserPlugin for Solira {
    fn name(&self) -> &'static str {
        "GeyserPluginSolira"
    }

    fn on_load(&mut self, _config_file: &str, _is_reload: bool) -> Result<()> {
        solana_logger::setup_with_default("info");
        Ok(())
    }
}
