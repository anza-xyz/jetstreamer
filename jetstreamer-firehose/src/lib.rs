#![deny(missing_docs)]
#![recursion_limit = "512"]
//! Core data structures and streaming utilities for Jetstreamer firehose processing.
//!
//! All CAR archive access refers to data hosted in Triton One's Old Faithful
//! archive of Solana ledger snapshots. References below may use the shorthand
//! “Old Faithful” for brevity.

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
