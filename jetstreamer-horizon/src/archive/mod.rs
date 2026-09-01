//! Horizon archive container format: compact, seekable, and PoH-material
//! preserving
//! storage for replayed Solana ledger data (blocks, entries, transactions,
//! and their account updates).
//!
//! # Layout
//!
//! ```text
//! File   := FileHeader ++ Bucket* ++ BucketIndex ++ Footer
//! Bucket := BucketHeader ++ payload            (payload = SlotFrame*, optionally zstd/LZ4)
//! SlotFrame := slot, kind, [BlockMeta, EntryRecord*, TxRecord*]
//! TxRecord  := signatures, message, status/meta, AccountUpdateRecord*
//! ```
//!
//! The file is written strictly forward (streaming-sink friendly); the
//! bucket index lives ahead of the footer, Parquet-style, so readers grab
//! the fixed-size [`Footer`] from the end of the file first, then the
//! index, then seek directly to any bucket.
//!
//! # Buckets: the unit of seek, compression, and encoder state
//!
//! All stateful encoding, including the pubkey-deduplication scratch layer and
//! account-data diff compression, resets at bucket boundaries. A reader
//! can therefore start decoding at any bucket header with nothing but the
//! globally-known prime table. Seeking to an arbitrary slot means jumping
//! to its bucket and decoding forward (at most `bucket_slots - 1` slot
//! frames, ~100 ms worst case at current decode throughput).
//!
//! `bucket_slots` is a format parameter recorded in the header:
//!
//! * `1`: encoder state resets every slot; every slot is independently
//!   decodable. Maximum seekability, but account-data diff compression is
//!   effectively disabled (diffs only apply within a single slot).
//! * `128` (default): diff compression works across the 128 consecutive
//!   slots of a bucket. Since the dominant update volume is the same
//!   accounts being rewritten slot after slot with tiny changes (vote
//!   states especially), this recovers the ~8× account-data savings
//!   measured in the diff benchmarks while keeping seeks cheap.
//!
//! # Compression stack (per bucket)
//!
//! 1. **Pubkey dedup**: every [`Address`](solana_address::Address) in
//!    transaction messages, rewards, and account-update metadata is
//!    encoded through a [`DedupeEncoder`](lencode::dedupe::DedupeEncoder)
//!    backed by the frozen 65 535-entry
//!    [`POPULAR_PUBKEYS`](crate::pubkey_prime::POPULAR_PUBKEYS) prime
//!    table (1-3 byte IDs instead of 32-byte keys; measured 29.3 % saving
//!    on real mainnet traffic). V1 uses native lencode integer IDs; V2 uses
//!    canonical unsigned LEB128 IDs, selected strictly by the file header.
//!    The scratch layer (novel pubkeys) resets per bucket.
//! 2. **Account-data diff**: each account update's `data` blob is encoded
//!    through a [`DiffEncoder`](lencode::diff::DiffEncoder) keyed by
//!    `xxh64(pubkey)`. Re-updates of the same account within a bucket emit
//!    compact RLE patches or, in V2's outer-compression-aware policy, raw XOR
//!    deltas when those are smaller and the bucket compressor can exploit
//!    their structure. Key collisions are harmless: encoder and decoder
//!    key their blob stores identically, so a collision merely degrades
//!    compression for that pair, never correctness.
//! 3. **Optional whole-bucket compression**: zstd (`compression = 1`) is the
//!    durable-size default; raw-block LZ4 (`compression = 2`, V2 only) trades
//!    some ratio for throughput. The per-bucket discriminant allows raw
//!    fallback when compression would expand a payload.
//!
//! # Wire versions
//!
//! Readers support deployed V1 archives and V2. Unknown versions, flags, V2
//! codecs in a V1 container, non-canonical lengths, oversized sections, and
//! trailing payload bytes fail closed. New writers default to V2; the
//! re-encoder can still emit V1 for compatibility controls (historical writer
//! releases are not promised to reproduce byte-identical payloads).
//!
//! # Re-encoding
//!
//! [`reencode_archive`] converts V1 or V2 input bucket by bucket, preserving
//! event order and resetting codec state at the same independently decodable
//! boundaries. The `reencode_archive` example wraps it in a migration CLI: it
//! opens the source read-only, writes a unique partial beside the requested
//! destination, syncs and fully decodes that output, then publishes it with an
//! atomic no-replace hard link. It never replaces or removes the source.
//!
//! Use `estimate_reencode` first to compare codecs on a deterministic,
//! size-stratified sample without writing a candidate archive. For example:
//!
//! ```text
//! cargo run --release -p jetstreamer-horizon --example estimate_reencode -- \
//!   /path/to/archives \
//!   --candidates v2-zstd:9+outer,v2-lz4+adaptive
//!
//! cargo run --release -p jetstreamer-horizon --example reencode_archive -- \
//!   epoch-v1.jet epoch-v2.jet \
//!   --format v2 --compression zstd --zstd-level 9 --diff outer
//! ```
//!
//! # PoH material and continuity checking
//!
//! Storing every entry hash would cost 32 B times about 800 entries times
//! 432,000 slots, or about 11 GB per epoch. Instead each [`EntryRecord`] stores only
//! `num_hashes` (varint, 1-3 B) and `tx_count` (varint); the slot's final
//! entry hash is the blockhash, already present in [`BlockMeta`]. Together
//! those fields retain the material needed for a future full SHA-256 PoH
//! recomputation. The current `verify_chain` option checks parent-blockhash
//! continuity against the preceding decoded block (or the bucket's stored
//! `poh_start_hash` anchor); it does **not** recompute PoH, and a bucket anchor
//! is not independently authenticated. Strict mismatch rejection is the
//! default; [`ChainMismatchPolicy::AllowZeroParentResume`] additionally
//! recognizes and counts the zero-parent placeholders emitted by historical
//! writers after a mid-epoch restart, without accepting nonzero mismatches.
//!
//! # Integrity
//!
//! Every bucket payload carries an xxh64 checksum (computed over the stored,
//! possibly-compressed bytes), and the index carries its own xxh64. These
//! detect accidental corruption but are not cryptographic authentication.
//! Callers must authenticate the source archive separately. The re-encoder's
//! semantic SHA-256 proves that its decoded output matches the decoded source;
//! it does not prove that the source itself is authentic.
//!
//! # Streaming reads
//!
//! [`ArchiveReader`] iterates buckets and slot frames in order, emitting
//! firehose-style callbacks (block, entries, transactions with account
//! updates) without ever seeking backward, which is suitable for HTTP range
//! readers as well as local files.
mod bucket;
mod format;
mod reader;
mod reencode;
mod writer;

#[cfg(test)]
mod tests;

pub use bucket::*;
pub use format::*;
pub use reader::*;
pub use reencode::*;
pub use writer::*;
