//! Horizon archive container format — compact, seekable, PoH-verifiable
//! storage for replayed Solana ledger data (blocks, entries, transactions,
//! and their account updates).
//!
//! # Layout
//!
//! ```text
//! File   := FileHeader ++ Bucket* ++ BucketIndex ++ Footer
//! Bucket := BucketHeader ++ payload            (payload = SlotFrame*, optionally zstd)
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
//! All stateful encoding — pubkey deduplication (scratch layer) and
//! account-data diff compression — resets at bucket boundaries. A reader
//! can therefore start decoding at any bucket header with nothing but the
//! globally-known prime table. Seeking to an arbitrary slot means jumping
//! to its bucket and decoding forward (at most `bucket_slots - 1` slot
//! frames, ~100 ms worst case at current decode throughput).
//!
//! `bucket_slots` is a format parameter recorded in the header:
//!
//! * `1` — encoder state resets every slot; every slot is independently
//!   decodable. Maximum seekability, but account-data diff compression is
//!   effectively disabled (diffs only apply within a single slot).
//! * `128` (default) — diff compression works across the 128 consecutive
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
//!    on real mainnet traffic). The scratch layer (novel pubkeys) resets
//!    per bucket.
//! 2. **Account-data diff**: each account update's `data` blob is encoded
//!    through a [`DiffEncoder`](lencode::diff::DiffEncoder) keyed by
//!    `xxh64(pubkey)`. Re-updates of the same account within a bucket
//!    emit RLE or XOR+zstd deltas (measured 87.6 % saving on realistic
//!    mutation streams). Key collisions are harmless: encoder and decoder
//!    key their blob stores identically, so a collision merely degrades
//!    compression for that pair, never correctness.
//! 3. **Optional whole-bucket zstd** (`compression = 1` in the bucket
//!    header): squeezes the residual structure (entry records, varint
//!    runs, log messages). Per-bucket flag so files can mix and the
//!    tradeoff can be measured empirically.
//!
//! # PoH verification without storing per-entry hashes
//!
//! Storing every entry hash would cost 32 B × ~800 entries × 432 000
//! slots ≈ 11 GB/epoch. Instead each [`EntryRecord`] stores only
//! `num_hashes` (varint, 1-3 B) and `tx_count` (varint); the slot's final
//! entry hash is the blockhash, already present in [`BlockMeta`]. A
//! verifying reader recomputes the SHA-256 chain per slot — seeded from
//! `parent_blockhash`, folding `num_hashes` and the transaction mixins of
//! each entry — and asserts the result equals `blockhash`. Slots verify
//! independently (each is anchored on its parent's blockhash), so
//! verification parallelizes across cores. Each bucket header also
//! carries `poh_start_hash` so a bucket is verifiable without reading its
//! predecessor.
//!
//! # Integrity
//!
//! Every bucket payload carries an xxh64 checksum (computed over the
//! stored, possibly-compressed bytes). The index carries its own xxh64.
//! Cryptographic integrity of the *content* comes from PoH verification.
//!
//! # Streaming reads
//!
//! [`ArchiveReader`] iterates buckets and slot frames in order, emitting
//! firehose-style callbacks (block, entries, transactions with account
//! updates) without ever seeking backward — suitable for HTTP range
//! readers as well as local files.
mod format;
mod reader;
mod writer;

#[cfg(test)]
mod tests;

pub use format::*;
pub use reader::*;
pub use writer::*;
