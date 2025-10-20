#![deny(missing_docs)]
//! High-throughput Solana backfilling and research runner built on Jetstreamer firehose plugins.
//!
//! # Overview
//! Jetstreamer streams historical Solana ledger data directly from Project Yellowstone's [Old
//! Faithful](https://old-faithful.net/) archive — a complete collection of every transaction
//! from genesis to the current chain tip. With adequate hardware and bandwidth, Jetstreamer
//! can sustain well over 2.7 million transactions per second of replay throughput streaming
//! historical Old Faithful data to your local plugins or consumers for analysis and/or
//! backfilling.
//!
//! Jetstreamer ships as a trio of companion crates:
//! - `jetstreamer`: this facade crate that wires firehose ingestion into your plugins.
//! - [`jetstreamer_firehose`](crate::firehose): async helpers for downloading, compacting, and
//!   replaying Old Faithful CAR archives at scale.
//! - [`jetstreamer_plugin`](crate::plugin): trait-driven framework for building structured
//!   observers with ClickHouse-friendly batching and runtime metrics.
//!
//! All three crates are re-exported from this facade, which keeps most applications reliant on
//! a single dependency.
//!
//! # Quick Start
//! Install the CLI by cloning the repository and running the bundled demo runner:
//!
//! ```bash
//! # Replay all transactions in epoch 800 using eight HTTP multiplexing workers.
//! JETSTREAMER_THREADS=8 cargo run --release -- 800
//!
//! # Or replay an explicit slot range (slot ranges may cross epoch boundaries).
//! JETSTREAMER_THREADS=8 cargo run --release -- 358560000:367631999
//! ```
//!
//! The CLI accepts either `<start>:<end>` slot ranges or a single epoch. See
//! [`JetstreamerRunner::parse_cli_args`] for the precise argument grammar.
//!
//! When `JETSTREAMER_CLICKHOUSE_MODE` is `auto` (the default) the runner inspects the DSN to
//! decide whether to launch the bundled ClickHouse helper or connect to an external cluster.
//!
//! # Environment Variables
//! `JetstreamerRunner` honours several environment variables for runtime tuning:
//! - `JETSTREAMER_THREADS` (default `1`): number of firehose ingestion threads. Increase this to
//!   multiplex Old Faithful HTTP requests across more cores.
//! - `JETSTREAMER_CLICKHOUSE_DSN` (default `http://localhost:8123`): DSN passed to plugin
//!   instances that emit ClickHouse writes.
//! - `JETSTREAMER_CLICKHOUSE_MODE` (default `auto`): controls ClickHouse integration. Accepted
//!   values are `auto`, `remote`, `local`, and `off`.
//!
//! Additional firehose-specific knobs such as `JETSTREAMER_COMPACT_INDEX_BASE_URL` and
//! `JETSTREAMER_NETWORK` live in [`jetstreamer_firehose`](crate::firehose).
//!
//! # Limitations
//! Jetstreamer can replay every block, transaction, epoch, and reward recorded in Old Faithful's
//! archive. Account updates, however, are not yet available because the upstream archive does not
//! expose them. A companion project is planned to fill this gap.
//!
//! # Epoch Feature Availability
//! Old Faithful snapshots expose different metadata across the network's history. Use the table
//! below to choose replay windows that match your requirements:
//!
//! | Epoch range | Slot range    | Comment |
//! |-------------|---------------|--------------------------------------------------|
//! | 0–156       | 0–?           | Incompatible with modern Geyser plugins          |
//! | 157+        | ?             | Compatible with modern Geyser plugins            |
//! | 0–449       | 0–194184610   | CU tracking not available (reported as `0`)      |
//! | 450+        | 194184611+    | CU tracking fully available                      |
//!
//! Epochs at or above `157` work with the bundled Geyser plugin interface, while compute unit
//! accounting first appears at epoch `450`.
//!
//! # Related Modules
//! - [`firehose`] exposes the underlying streaming primitives.
//! - [`plugin`] provides the trait surface for authoring plugins.
//! - [`utils`] hosts shared helpers used across the Jetstreamer ecosystem.

pub use jetstreamer_firehose as firehose;
pub use jetstreamer_plugin as plugin;
pub use jetstreamer_utils as utils;

use core::ops::Range;
use jetstreamer_firehose::{epochs::slot_to_epoch, index::get_index_base_url};
use jetstreamer_plugin::{Plugin, PluginRunner, PluginRunnerError};
use std::sync::Arc;

const WORKER_THREAD_MULTIPLIER: usize = 4; // each plugin thread gets 4 worker threads

#[derive(Clone, Copy)]
struct ClickhouseSettings {
    enabled: bool,
    spawn_helper: bool,
}

impl ClickhouseSettings {
    const fn new(enabled: bool, spawn_helper: bool) -> Self {
        Self {
            enabled,
            spawn_helper,
        }
    }
}

#[derive(Clone, Copy)]
enum ClickhouseMode {
    Auto,
    Disabled,
    RemoteOnly,
    Local,
}

fn resolve_clickhouse_settings(default_spawn_helper: bool) -> ClickhouseSettings {
    let default_settings = ClickhouseSettings::new(true, default_spawn_helper);

    match std::env::var("JETSTREAMER_CLICKHOUSE_MODE") {
        Ok(raw_mode) => match parse_clickhouse_mode(&raw_mode) {
            Some(ClickhouseMode::Auto) => default_settings,
            Some(ClickhouseMode::Disabled) => ClickhouseSettings::new(false, false),
            Some(ClickhouseMode::RemoteOnly) => ClickhouseSettings::new(true, false),
            Some(ClickhouseMode::Local) => ClickhouseSettings::new(true, true),
            None => {
                log::warn!(
                    "Unrecognized JETSTREAMER_CLICKHOUSE_MODE value '{}'; falling back to default settings",
                    raw_mode
                );
                default_settings
            }
        },
        Err(_) => default_settings,
    }
}

fn parse_clickhouse_mode(value: &str) -> Option<ClickhouseMode> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Some(ClickhouseMode::Auto);
    }

    let lowered = trimmed.to_ascii_lowercase();
    match lowered.as_str() {
        "auto" | "default" | "on" | "true" | "1" => Some(ClickhouseMode::Auto),
        "off" | "disable" | "disabled" | "0" | "false" | "none" | "no" => {
            Some(ClickhouseMode::Disabled)
        }
        "remote" | "external" | "no-spawn" | "no_spawn" | "nospawn" => {
            Some(ClickhouseMode::RemoteOnly)
        }
        "local" | "spawn" | "helper" | "auto-spawn" | "autospawn" => Some(ClickhouseMode::Local),
        _ => None,
    }
}

/// Coordinates plugin execution against the firehose.
///
/// Configure the runner with the builder-style methods and finish by calling
/// [`JetstreamerRunner::run`]. The runner also honours the process-level environment variables
/// documented at the module level
///
/// ### Environment variables
///
/// [`JetstreamerRunner`] inspects a handful of environment variables at startup to fine-tune
/// runtime behaviour:
///
/// - `JETSTREAMER_THREADS`: Number of firehose ingestion threads, defaulting to `1`.
/// - `JETSTREAMER_CLICKHOUSE_DSN`: DSN for ClickHouse ingestion; defaults to
///   `http://localhost:8123`.
/// - `JETSTREAMER_CLICKHOUSE_MODE`: Controls ClickHouse integration. Accepted values are
///   `auto` (default: enable output and spawn the helper only for local DSNs), `remote`
///   (enable output but never spawn the helper), `local` (always request the helper), and
///   `off` (disable ClickHouse entirely).
///
/// ### Example
///
/// ```no_run
/// use std::sync::Arc;
///
/// use clickhouse::Client;
/// use jetstreamer::{
///     JetstreamerRunner,
///     firehose::firehose::{BlockData, TransactionData},
///     plugin::{Plugin, PluginFuture},
/// };
///
/// struct Dummy;
///
/// impl Plugin for Dummy {
///     fn name(&self) -> &'static str {
///         "dummy"
///     }
///
///     fn on_transaction<'a>(
///         &'a self,
///         _thread_id: usize,
///         _db: Option<Arc<Client>>,
///         tx: &'a TransactionData,
///     ) -> PluginFuture<'a> {
///         Box::pin(async move {
///             println!("tx {} landed in slot {}", tx.signature, tx.slot);
///             Ok(())
///         })
///     }
///
///     fn on_block<'a>(
///         &'a self,
///         _thread_id: usize,
///         _db: Option<Arc<Client>>,
///         block: &'a BlockData,
///     ) -> PluginFuture<'a> {
///         Box::pin(async move {
///             if block.was_skipped() {
///                 println!("slot {} was skipped", block.slot());
///             } else {
///                 println!("processed block at slot {}", block.slot());
///             }
///             Ok(())
///         })
///     }
/// }
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// // Increase ingest parallelism and opt out of the embedded ClickHouse helper.
/// unsafe {
///     std::env::set_var("JETSTREAMER_THREADS", "4");
///     std::env::set_var("JETSTREAMER_CLICKHOUSE_MODE", "remote");
/// }
///
/// let runner = JetstreamerRunner::new()
///     .with_plugin(Box::new(Dummy))
///     .parse_cli_args()?;
///
/// runner.run().expect("runner execution");
/// # Ok(())
/// # }
/// ```
///
/// ## Multiplexing and Throughput
///
/// By default `JETSTREAMER_THREADS` is set to `1` which there is no HTTP multiplexing of the
/// underlying [`firehose`](jetstreamer_firehose::firehose::firehose) stream. The way
/// multiplexing works is multiple threads connect to different subsections of the underlying
/// slot range being streamed from Old Faithful, and handle this subrange in parallel with
/// other threads, achieving embarrasingly parallel throughput increases up to the limit of
/// your CPU and internet connection. A good rule of thumb is to expect about 250 Mbps of
/// bandwidth and significant one-core compute per thread. On a 16 core system with a 1 Gbps
/// network connection, setting `JETSTREAMER_THREADS` to 4-5 should yield optimal results.
///
/// To achieve 2M TPS+, you will need a 20+ Gbps network connection and at least a 64 core CPU.
/// On our benchmark hardware we currently have a 100 Gbps connection and 64 cores, which has
/// led to a record of 2.7M TPS of the course of a 12 hour run.
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
        let default_spawn = should_spawn_for_dsn(&clickhouse_dsn);
        let clickhouse_settings = resolve_clickhouse_settings(default_spawn);
        Self {
            log_level: "info".to_string(),
            plugins: Vec::new(),
            clickhouse_dsn,
            config: Config {
                threads: 1,
                slot_range: 0..0,
                clickhouse_enabled: clickhouse_settings.enabled,
                spawn_clickhouse: clickhouse_settings.spawn_helper && clickhouse_settings.enabled,
            },
        }
    }
}

impl JetstreamerRunner {
    /// Creates a [`JetstreamerRunner`] with default configuration.
    pub fn new() -> Self {
        Self::default()
    }

    /// Overrides the log level used when initializing `solana_logger`.
    pub fn with_log_level(mut self, log_level: impl Into<String>) -> Self {
        self.log_level = log_level.into();
        solana_logger::setup_with_default(&self.log_level);
        self
    }

    /// Registers an additional [`Plugin`] to receive firehose events.
    pub fn with_plugin(mut self, plugin: Box<dyn Plugin>) -> Self {
        self.plugins.push(plugin);
        self
    }

    /// Restricts [`JetstreamerRunner::run`] to a specific slot range.
    pub fn with_slot_range(mut self, slot_range: Range<u64>) -> Self {
        self.config.slot_range = slot_range;
        self
    }

    /// Sets the ClickHouse DSN passed to [`PluginRunner::new`].
    pub fn with_clickhouse_dsn(mut self, clickhouse_dsn: impl Into<String>) -> Self {
        self.clickhouse_dsn = clickhouse_dsn.into();
        self
    }

    /// Replaces the current [`Config`] with values parsed from CLI arguments and the environment.
    pub fn parse_cli_args(mut self) -> Result<Self, Box<dyn std::error::Error>> {
        self.config = parse_cli_args()?;
        Ok(self)
    }

    /// Builds the plugin runtime and streams blocks through every registered [`Plugin`].
    pub fn run(self) -> Result<(), PluginRunnerError> {
        solana_logger::setup_with_default(&self.log_level);

        if let Ok(index_url) = get_index_base_url() {
            log::info!("slot index base url: {}", index_url);
        }

        let threads = std::cmp::max(1, self.config.threads);
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

        let mut clickhouse_task: Option<tokio::task::JoinHandle<Result<(), ()>>> = None;

        if spawn_clickhouse {
            clickhouse_task = Some(runtime.block_on(async {
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

                Ok::<_, PluginRunnerError>(tokio::spawn(async move {
                    match clickhouse_future.await {
                        Ok(()) => {
                            log::info!("ClickHouse process exited gracefully.");
                            Ok(())
                        }
                        Err(()) => {
                            log::error!("ClickHouse process exited with an error.");
                            Err(())
                        }
                    }
                }))
            })?);
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
            let handle = clickhouse_task.take();
            runtime.block_on(async move {
                jetstreamer_utils::stop().await;
                if let Some(handle) = handle
                    && let Err(err) = handle.await
                {
                    log::warn!("ClickHouse monitor task aborted: {}", err);
                }
            });
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

/// Runtime configuration for [`JetstreamerRunner`].
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Config {
    /// Number of simultaneous firehose streams to spawn.
    pub threads: usize,
    /// The range of slots to process, inclusive of the start and exclusive of the end slot.
    pub slot_range: Range<u64>,
    /// Whether to connect to ClickHouse for plugin output.
    pub clickhouse_enabled: bool,
    /// Whether to spawn a local ClickHouse instance automatically.
    pub spawn_clickhouse: bool,
}

/// Parses command-line arguments and environment variables into a [`Config`].
///
/// The following environment variables are inspected:
/// - `JETSTREAMER_CLICKHOUSE_MODE`: Controls ClickHouse integration. Accepts `auto`, `remote`,
///   `local`, or `off`.
/// - `JETSTREAMER_THREADS`: Number of firehose ingestion threads.
///
/// # Examples
///
/// ```no_run
/// # use jetstreamer::parse_cli_args;
/// # unsafe {
/// #     std::env::set_var("JETSTREAMER_THREADS", "3");
/// #     std::env::set_var("JETSTREAMER_CLICKHOUSE_MODE", "off");
/// # }
/// let config = parse_cli_args().expect("env and CLI parsed");
/// assert_eq!(config.threads, 3);
/// assert!(!config.clickhouse_enabled);
/// ```
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

    let clickhouse_settings = resolve_clickhouse_settings(true);
    let clickhouse_enabled = clickhouse_settings.enabled;

    let threads = std::env::var("JETSTREAMER_THREADS")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(1);

    let spawn_clickhouse = clickhouse_settings.spawn_helper && clickhouse_enabled;

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
