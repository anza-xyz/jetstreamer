#![deny(missing_docs)]
#![recursion_limit = "512"]
//! Core data structures and streaming utilities for Jetstreamer firehose processing.
//!
//! # Overview
//! The firehose crate streams data live over the network directly from Project Yellowstone's
//! [Old Faithful](https://old-faithful.net/) archive of CAR files, which hosts the complete
//! history of every Solana transaction. Data only flows outward from Old Faithful to your
//! local consumer; nothing is ever uploaded back to the archive. With sufficient CPU and
//! network headroom the pipeline can exceed 2.7 million transactions per second while decoding
//! the stream for analysis and backfilling workloads.
//!
//! Firehose is the foundation that powers
//! [`jetstreamer`](https://crates.io/crates/jetstreamer) and
//! [`jetstreamer-plugin`](https://crates.io/crates/jetstreamer-plugin), but it can also be
//! consumed directly to build bespoke replay pipelines. The crate exposes:
//! - Async readers for Old Faithful CAR archives via [`firehose`].
//! - Rich data models for blocks, entries, rewards, and transactions.
//! - Epoch helpers for reasoning about slot ranges and availability windows.
//!
//! # Configuration
//! Several environment variables influence how the firehose locates and caches data:
//! - `JETSTREAMER_COMPACT_INDEX_BASE_URL` (default `https://files.old-faithful.net`): base URL
//!   for compact CAR index artifacts. Point this at your own mirror to reduce load on the
//!   public Old Faithful deployment.
//! - `JETSTREAMER_NETWORK` (default `mainnet`): suffix appended to cache namespaces and index
//!   filenames so you can swap between clusters without purging local state.
//!
//! # Limitations
//! Old Faithful currently publishes blocks, transactions, epochs, and reward metadata but does
//! not ship account updates. The firehose mirrors that limitation; plan on a separate data
//! source if you require account updates.
//!
//! # Epoch Feature Availability
//! Old Faithful snapshots expose different metadata as the Solana protocol evolved. Use the
//! table below to decide which replay windows fit your requirements:
//!
//! | Epoch range | Slot range    | Comment |
//! |-------------|---------------|--------------------------------------------------|
//! | 0–156       | 0–?           | Incompatible with modern Geyser plugins           |
//! | 157+        | ?             | Compatible with modern Geyser plugins             |
//! | 0–449       | 0–194184610   | CU tracking not available (reported as `0`)       |
//! | 450+        | 194184611+    | CU tracking fully available                       |
//!
//! Detailed helpers for translating between epochs and slots live in the [`epochs`] module.
//!
//! # Examples
//! Run the firehose with handlers for every data type:
//! ```no_run
//! use futures_util::FutureExt;
//! use jetstreamer_firehose::{
//!     epochs,
//!     firehose::{self, BlockData, EntryData, RewardsData, Stats, StatsTracking, TransactionData},
//! };
//!
//! fn block_handler() -> impl firehose::Handler<BlockData> {
//!     move |_thread_id, block| async move {
//!         println!("block slot {}", block.slot());
//!         Ok(())
//!     }
//!     .boxed()
//! }
//!
//! fn tx_handler() -> impl firehose::Handler<TransactionData> {
//!     move |_thread_id, tx| async move {
//!         println!("tx {} in slot {}", tx.signature, tx.slot);
//!         Ok(())
//!     }
//!     .boxed()
//! }
//!
//! fn entry_handler() -> impl firehose::Handler<EntryData> {
//!     move |_thread_id, entry| async move {
//!         println!("entry {} covering transactions {:?}", entry.entry_index, entry.transaction_indexes);
//!         Ok(())
//!     }
//!     .boxed()
//! }
//!
//! fn reward_handler() -> impl firehose::Handler<RewardsData> {
//!     move |_thread_id, rewards| async move {
//!         println!("rewards in slot {} -> {} accounts", rewards.slot, rewards.rewards.len());
//!         Ok(())
//!     }
//!     .boxed()
//! }
//!
//! fn stats_handler() -> impl firehose::Handler<Stats> {
//!     move |_thread_id, stats| async move {
//!         println!("processed {} slots so far", stats.slots_processed);
//!         Ok(())
//!     }
//!     .boxed()
//! }
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let stats = StatsTracking {
//!         on_stats: stats_handler(),
//!         tracking_interval_slots: 100,
//!     };
//!
//!     let (start, _) = epochs::epoch_to_slot_range(800);
//!     let (_, end_inclusive) = epochs::epoch_to_slot_range(805);
//!     let slot_range = start..(end_inclusive + 1);
//!
//!     firehose::firehose(
//!         4,
//!         slot_range,
//!         Some(block_handler()),
//!         Some(tx_handler()),
//!         Some(entry_handler()),
//!         Some(reward_handler()),
//!         Some(stats),
//!         None,
//!     )
//!     .await
//!     .map_err(|(err, slot)| -> Box<dyn std::error::Error> {
//!         format!("firehose failed at slot {slot}: {err}").into()
//!     })?;
//!     Ok(())
//! }
//! ```

/// Types for decoding block-level records emitted by the firehose.
pub mod block;
/// Encodes and decodes arbitrary binary [`DataFrame`](dataframe::DataFrame) nodes.
pub mod dataframe;
/// Parsing and serialization helpers for [`Entry`](entry::Entry) nodes.
pub mod entry;
/// Structures for the top-level [`Epoch`](epoch::Epoch) node type.
pub mod epoch;
/// Epoch utilities such as [`epoch_to_slot_range`](epochs::epoch_to_slot_range).
pub mod epochs;
/// Streaming interface for fetching and parsing firehose blocks.
pub mod firehose;
/// Slot offset index client for locating blocks in Old Faithful CAR archives.
pub mod index;
/// Helpers for working with network metadata and endpoints.
pub mod network;
/// Core node tree definitions shared across firehose types.
pub mod node;
/// Reader utilities for decoding Old Faithful CAR node streams.
pub mod node_reader;
/// Reward decoding primitives and helpers.
pub mod rewards;
/// Utilities for working with subset nodes.
pub mod subset;
/// Transaction decoding and helpers.
pub mod transaction;
/// Shared helpers used throughout the firehose crate.
pub mod utils;
