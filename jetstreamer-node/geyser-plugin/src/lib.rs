use agave_geyser_plugin_interface::geyser_plugin_interface::{
    GeyserPlugin, ReplicaBlockInfoVersions, Result,
};
use log::info;

#[derive(Debug, Default)]
struct JetstreamerNodeGeyserPlugin;

impl GeyserPlugin for JetstreamerNodeGeyserPlugin {
    fn name(&self) -> &'static str {
        "jetstreamer-node-geyser"
    }

    fn notify_block_metadata(&self, blockinfo: ReplicaBlockInfoVersions) -> Result<()> {
        let slot = match blockinfo {
            ReplicaBlockInfoVersions::V0_0_1(info) => info.slot,
            ReplicaBlockInfoVersions::V0_0_2(info) => info.slot,
            ReplicaBlockInfoVersions::V0_0_3(info) => info.slot,
            ReplicaBlockInfoVersions::V0_0_4(info) => info.slot,
        };
        info!("block slot {}", slot);
        Ok(())
    }

    fn transaction_notifications_enabled(&self) -> bool {
        true
    }
}

#[allow(improper_ctypes_definitions)]
#[unsafe(no_mangle)]
pub extern "C" fn _create_plugin() -> *mut dyn GeyserPlugin {
    let plugin: Box<dyn GeyserPlugin> = Box::new(JetstreamerNodeGeyserPlugin::default());
    Box::into_raw(plugin)
}
