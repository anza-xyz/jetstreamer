//! Solana DEX trades decoder plugin for Jetstreamer.
//!
//! Decodes swap transactions from 28+ Solana DEX programs into a unified
//! [`SwapRecord`] protobuf format, suitable for analytics pipelines,
//! data lakes, and research tooling.
//!
//! # Supported DEXes
//!
//! Pump Fun, Pump AMM, Raydium (AMM V4, CPMM, CLMM, LaunchLab),
//! Orca (V2, Whirlpool), Meteora (DAMM V2, DBC, DLMM, Pools), Manifest,
//! Aquifer, Lifinity (V1/V2), PancakeSwap CLMM, Byreal CLMM, Futarchy AMM,
//! AlphaQ, BisonFi, ZeroFi, GoonFi, HumidiFi, Tessera, Obric V2,
//! SolFi (V1/V2), Stabble (Stable/Weighted), and more.
//!
//! # Output Formats
//!
//! - **Protobuf** (primary): compact, language-agnostic [`SwapRecord`] messages
//! - **ClickHouse** (optional, behind `clickhouse-output` feature): direct table writes
//!
//! # Usage as a Plugin
//!
//! ```ignore
//! use jetstreamer::JetstreamerRunner;
//! use jetstreamer_dex_trades::DexTradesPlugin;
//!
//! JetstreamerRunner::new()
//!     .with_plugin(Box::new(DexTradesPlugin::new()))
//!     .parse_cli_args().unwrap()
//!     .run().unwrap();
//! ```
//!
//! # Usage as a Library
//!
//! ```no_run
//! use jetstreamer_dex_trades::registry::DexRegistry;
//! // Bring the extension trait into scope to call .all_instructions() etc.
//! use jetstreamer_dex_trades::instruction_iter::InstructionIteratorExt;
//!
//! let registry = DexRegistry::new();
//! // registry.decode_transaction(&tx_data) returns Vec<SwapRecord>
//! ```

pub mod instruction_iter;
pub mod transaction_ext;
pub mod log_parser;
pub mod token_transfers;
pub mod types;
pub mod registry;
pub mod plugin;
pub mod decoders;

/// Generated protobuf types.
pub mod proto {
    include!(concat!(env!("OUT_DIR"), "/dex_trades.rs"));
}

pub use plugin::DexTradesPlugin;
pub use registry::DexRegistry;
pub use types::SwapRecord;
