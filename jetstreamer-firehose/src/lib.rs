#![deny(missing_docs)]
#![recursion_limit = "512"]
//! Core data structures and streaming utilities for Jetstreamer firehose processing.
//!
//! All CAR archive access refers to data hosted in Triton One's Old Faithful
//! archive of Solana ledger snapshots. References below may use the shorthand
//! “Old Faithful” for brevity.
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
