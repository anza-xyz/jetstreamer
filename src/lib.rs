use solana_geyser_plugin_interface::geyser_plugin_interface::GeyserPlugin;

#[derive(Clone, Debug, Default)]
pub struct Solira;

impl GeyserPlugin for Solira {
    fn name(&self) -> &'static str {
        "GeyserPluginSolira"
    }
}
