use core::ops::Range;
use geyser_replay::{
    epochs::slot_to_epoch,
    firehose::{firehose, GeyserReplayError},
    index::get_index_dir,
};
use serde_json::json;
use solira_plugin::{Plugin, PluginRunner};
use std::{fs::File, io::Write, os::unix::fs::PermissionsExt, path::PathBuf, sync::Arc};
use tempfile::NamedTempFile;

include!(concat!(env!("OUT_DIR"), "/embed.rs")); // brings in SOLIRA_CDYLIB

pub struct SoliraRunner {
    log_level: String,
    plugins: Vec<Box<dyn Plugin>>,
    slot_range: Option<Range<u64>>,
    index_dir: PathBuf,
    geyser_config_files: Vec<PathBuf>,
}

impl Default for SoliraRunner {
    fn default() -> Self {
        Self {
            log_level: "info".to_string(),
            plugins: Vec::new(),
            slot_range: Default::default(),
            index_dir: get_index_dir(),
            geyser_config_files: Vec::new(),
        }
    }
}

impl SoliraRunner {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_log_level(mut self, log_level: impl Into<String>) -> Self {
        self.log_level = log_level.into();
        solana_logger::setup_with_default(&self.log_level);
        self
    }

    pub fn with_plugin(mut self, plugin: Box<dyn Plugin>) -> Self {
        self.plugins.push(plugin);
        self
    }

    pub fn with_slot_range(mut self, slot_range: Range<u64>) -> Self {
        self.slot_range = Some(slot_range);
        self
    }

    pub fn parse_cli_args(mut self) -> Result<Self, Box<dyn std::error::Error>> {
        self.slot_range = Some(parse_cli_args()?);
        Ok(self)
    }

    pub fn with_index_dir(mut self, index_dir: PathBuf) -> Self {
        self.index_dir = index_dir;
        self
    }

    pub async fn with_automatic_geyser_config(mut self) -> Self {
        // TODO: customization of clickhouse config and have plugin respect this
        let geyser_config = setup_geyser().await.unwrap();
        self.geyser_config_files.push(geyser_config);
        self
    }

    pub fn with_geyser_config(mut self, config: PathBuf) -> Self {
        self.geyser_config_files.push(config);
        self
    }

    pub async fn run(self) -> Result<(), GeyserReplayError> {
        solana_logger::setup_with_default(&self.log_level);
        let geyser_config_files: &[PathBuf] = &self.geyser_config_files;
        log::debug!("GeyserPluginService config: {:?}", geyser_config_files);
        let client = reqwest::Client::new();
        let index_dir = self.index_dir;
        log::info!("slot index dir: {:?}", index_dir);
        let mut slot_range = self.slot_range.unwrap_or_default();
        log::info!("geyser config files: {:?}", geyser_config_files);
        let mut plugin_runner =
            PluginRunner::new("http://localhost:8123").socket_name("solira.sock");
        for plugin in self.plugins {
            plugin_runner.register(plugin);
        }
        let plugin_runner = Arc::new(plugin_runner);
        loop {
            let runner = plugin_runner.clone();
            if let Err((err, slot)) = firehose(
                slot_range.clone(),
                Some(geyser_config_files),
                &index_dir,
                &client,
                async move {
                    runner
                        .run() // see below: run takes self: Arc<Self>
                        .await
                        .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + 'static>)
                },
            )
            .await
            {
                // handle error or break if needed
                log::error!(
                    "🔥🔥🔥 firehose encountered an error at slot {} in epoch {}: {}",
                    slot,
                    slot_to_epoch(slot),
                    err
                );
                let slot = slot.saturating_sub(1);
                slot_range = slot..slot_range.end;
                log::warn!(
                    "restarting from slot {}..{}",
                    slot_range.start,
                    slot_range.end
                );
            }
        }
    }
}

/// Sets up the environment for the Solira geyser plugin, returning the path of an ephemeral
/// geyser plugin config file pointing to a copy of the Solira geyser plugin shared library.
pub async fn setup_geyser() -> Result<PathBuf, Box<dyn std::error::Error>> {
    // materialize libsolira.{so|dylib|dll} on disk
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

    // build a transient plugin_config.json
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
    Ok(cfg_path)
}

pub fn parse_cli_args() -> Result<Range<u64>, Box<dyn std::error::Error>> {
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
    Ok(slot_range)
}
