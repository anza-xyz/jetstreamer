use core::ops::Range;
use jetstreamer_firehose::{epochs::slot_to_epoch, index::get_index_base_url};
use jetstreamer_plugin::{Plugin, PluginRunner, PluginRunnerError};
use jetstreamer_utils::{self};
use std::sync::Arc;

const WORKER_THREAD_MULTIPLIER: usize = 4; // each plugin thread gets 4 worker threads

pub struct JetstreamerRunner {
    log_level: String,
    plugins: Vec<Box<dyn Plugin>>,
    clickhouse_dsn: String,
    config: Config,
}

impl Default for JetstreamerRunner {
    fn default() -> Self {
        let clickhouse_dsn = std::env::var("JETSTREAMER_CLICKHOUSE_DSN")
            .unwrap_or_else(|_| "http://localhost:8123".to_string());
        let spawn_clickhouse = should_spawn_for_dsn(&clickhouse_dsn);
        Self {
            log_level: "info".to_string(),
            plugins: Vec::new(),
            clickhouse_dsn,
            config: Config {
                threads: 1,
                slot_range: 0..0,
                clickhouse_enabled: true,
                spawn_clickhouse,
            },
        }
    }
}

impl JetstreamerRunner {
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
        self.config.slot_range = slot_range;
        self
    }

    pub fn with_clickhouse_dsn(mut self, clickhouse_dsn: impl Into<String>) -> Self {
        self.clickhouse_dsn = clickhouse_dsn.into();
        self
    }

    pub fn parse_cli_args(mut self) -> Result<Self, Box<dyn std::error::Error>> {
        self.config = parse_cli_args()?;
        Ok(self)
    }

    pub fn run(self) -> Result<(), PluginRunnerError> {
        solana_logger::setup_with_default(&self.log_level);

        if let Ok(index_url) = get_index_base_url() {
            log::info!("slot index base url: {}", index_url);
        }

        let threads = std::cmp::max(1, self.config.threads as usize);
        let clickhouse_enabled =
            self.config.clickhouse_enabled && !self.clickhouse_dsn.trim().is_empty();
        let slot_range = self.config.slot_range.clone();
        let spawn_clickhouse = clickhouse_enabled
            && self.config.spawn_clickhouse
            && should_spawn_for_dsn(&self.clickhouse_dsn);

        log::info!(
            "processing slots [{}..{}) with {} firehose threads (clickhouse_enabled={})",
            slot_range.start,
            slot_range.end,
            threads,
            clickhouse_enabled
        );

        let mut runner = PluginRunner::new(&self.clickhouse_dsn, threads);
        for plugin in self.plugins {
            runner.register(plugin);
        }

        let runner = Arc::new(runner);
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(std::cmp::max(
                1,
                threads.saturating_mul(WORKER_THREAD_MULTIPLIER),
            ))
            .enable_all()
            .thread_name("jetstreamer")
            .build()
            .expect("failed to build plugin runtime");

        if spawn_clickhouse {
            runtime.block_on(async {
                let (mut ready_rx, clickhouse_future) =
                    jetstreamer_utils::start().await.map_err(|err| {
                        PluginRunnerError::PluginLifecycle {
                            plugin: "clickhouse",
                            stage: "start",
                            details: err.to_string(),
                        }
                    })?;

                ready_rx
                    .recv()
                    .await
                    .ok_or_else(|| PluginRunnerError::PluginLifecycle {
                        plugin: "clickhouse",
                        stage: "ready",
                        details: "ClickHouse readiness signal channel closed unexpectedly".into(),
                    })?;

                tokio::spawn(async move {
                    match clickhouse_future.await {
                        Ok(()) => log::info!("ClickHouse process exited gracefully."),
                        Err(()) => log::error!("ClickHouse process exited with an error."),
                    }
                });

                Ok::<(), PluginRunnerError>(())
            })?;
        } else if clickhouse_enabled {
            if !self.config.spawn_clickhouse {
                log::info!(
                    "ClickHouse auto-spawn disabled via configuration; using existing instance at {}",
                    self.clickhouse_dsn
                );
            } else {
                log::info!(
                    "ClickHouse DSN {} not recognized as local; skipping embedded ClickHouse spawn",
                    self.clickhouse_dsn
                );
            }
        }

        let result = runtime.block_on(runner.run(slot_range.clone(), clickhouse_enabled));

        if spawn_clickhouse {
            jetstreamer_utils::stop_sync();
        }

        match result {
            Ok(()) => Ok(()),
            Err(err) => {
                if let PluginRunnerError::Firehose { slot, details } = &err {
                    log::error!(
                        "firehose failed at slot {} in epoch {}: {}",
                        slot,
                        slot_to_epoch(*slot),
                        details
                    );
                }
                Err(err)
            }
        }
    }
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Config {
    /// Number of simultaneous firehose streams to spawn.
    pub threads: u8,
    /// The range of slots to process, inclusive of the start and exclusive of the end slot.
    pub slot_range: Range<u64>,
    /// Whether to connect to ClickHouse for plugin output.
    pub clickhouse_enabled: bool,
    /// Whether to spawn a local ClickHouse instance automatically.
    pub spawn_clickhouse: bool,
}

pub fn parse_cli_args() -> Result<Config, Box<dyn std::error::Error>> {
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
        let (start_slot, end_slot) = jetstreamer_firehose::epochs::epoch_to_slot_range(epoch);
        start_slot..end_slot
    };

    let clickhouse_enabled = std::env::var("JETSTREAMER_NO_CLICKHOUSE")
        .map(|v| v != "1" && v.to_ascii_lowercase() != "true")
        .unwrap_or(true);

    let threads = std::env::var("JETSTREAMER_THREADS")
        .ok()
        .and_then(|s| s.parse::<u8>().ok())
        .unwrap_or(1);

    let spawn_clickhouse = std::env::var("JETSTREAMER_SPAWN_CLICKHOUSE")
        .map(|v| v != "0" && v.to_ascii_lowercase() != "false")
        .unwrap_or(true);

    Ok(Config {
        threads,
        slot_range,
        clickhouse_enabled,
        spawn_clickhouse,
    })
}

fn should_spawn_for_dsn(dsn: &str) -> bool {
    let lower = dsn.to_ascii_lowercase();
    lower.contains("localhost") || lower.contains("127.0.0.1")
}
