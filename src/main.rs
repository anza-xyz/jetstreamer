//! solira-runner/src/main.rs
use geyser_replay::{firehose::firehose, index::get_index_dir};
use serde_json::json;
use std::{fs::File, io::Write, os::unix::fs::PermissionsExt, path::PathBuf};
use tempfile::NamedTempFile;

include!(concat!(env!("OUT_DIR"), "/embed.rs")); // ⇢ brings in SOLIRA_CDYLIB

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    solana_logger::setup_with_default("info");

    /* ────────────────────────────────────────────────────────────────────
    1. materialise libsolira.{so|dylib|dll} on disk
    ──────────────────────────────────────────────────────────────────── */
    let cdylib_path = {
        // pick an extension for this OS
        let ext = if cfg!(target_os = "windows") {
            "dll"
        } else if cfg!(target_os = "macos") {
            "dylib"
        } else {
            "so"
        };

        // create a temp file with that suffix and write the bytes
        let file = NamedTempFile::with_suffix(format!("-solira.{ext}"))?
            .into_temp_path()
            .keep()?;
        File::create(&file)?.write_all(SOLIRA_CDYLIB)?;
        // executable permission for Unix
        #[cfg(unix)]
        std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o755))?;

        file.as_path().to_path_buf()
    };

    /* ────────────────────────────────────────────────────────────────────
    2. build a transient plugin_config.json
    ──────────────────────────────────────────────────────────────────── */
    let cfg_path = {
        let cfg = json!({
            "libpath": cdylib_path,
            "name":    "GeyserPluginSolira",
            "clickhouse": {
                "host": "127.0.0.1",
                "port": 8123,
                "database": "default",
                "username": "default",
                "password": ""
            },
            "log_level": "info"
        });
        println!("GeyserPluginService config: {:?}", cfg);
        let tmp = NamedTempFile::with_suffix("-solira-plugin-config.json")?.into_temp_path();
        serde_json::to_writer_pretty(&File::create(&tmp)?, &cfg)?;
        tmp.keep()? // same lifetime rule
    };

    /* ────────────────────────────────────────────────────────────────────
    3. launch GeyserPluginService exactly as usual
    ──────────────────────────────────────────────────────────────────── */
    let geyser_config_files: &[PathBuf] = &[cfg_path.clone()];

    log::debug!("GeyserPluginService config: {:?}", geyser_config_files);

    let client = reqwest::Client::new();
    let index_dir = get_index_dir();
    let first_arg = std::env::args().nth(1).expect("no first argument given");
    let slot_range = if first_arg.contains(':') {
        let (slot_a, slot_b) = first_arg
            .split_once(':')
            .expect("failed to parse slot range, expected format: <start>:<end> or a single epoch");
        let slot_a: u64 = slot_a.parse().expect("failed to parse first slot");
        let slot_b: u64 = slot_b.parse().expect("failed to parse second slot");
        slot_a..(slot_b + 1)
    } else {
        let epoch: u64 = first_arg.parse().expect("failed to parse epoch");
        log::info!("epoch: {}", epoch);
        let (start_slot, end_slot) = geyser_replay::epochs::epoch_to_slot_range(epoch);
        start_slot..end_slot
    };
    log::info!("slot index dir: {:?}", index_dir);
    log::info!("geyser config files: {:?}", geyser_config_files);
    firehose(slot_range, Some(geyser_config_files), index_dir, &client).await?;
    Ok(())
}
