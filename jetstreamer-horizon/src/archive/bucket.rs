//! Source-agnostic bucket decoder and archive framing parsers.
//!
//! This is the *decode* half of the reader, lifted off any specific I/O
//! source. [`ArchiveReader`](super::ArchiveReader) drives it over a
//! `Read + Seek` source for local/sequential reads; an async driver (e.g.
//! the horizon firehose) can fetch raw bucket bytes over the network and
//! feed them to the same decoder, so there is exactly one decode
//! implementation. Decoding is zero-alloc on the hot path except for the
//! diff decoder's reconstructed account-data blobs.
use lencode::context::DecoderContext;
use lencode::dedupe::DedupeIdCodec;
use lencode::diff::DiffDecoder;
use lencode::prelude::*;
use solana_address::Address;
use solana_hash::Hash;
use xxhash_rust::xxh64::xxh64;

use crate::account_updates::{AccountUpdateView, PushAccountUpdateError};
use crate::dedupe::{new_decoder_context_with_codec, reset_decoder};
use crate::limits::{
    MAX_ACCOUNT_DATA_LEN, MAX_EPOCH_UPDATE_DATA, MAX_EPOCH_UPDATES, MAX_SLOT_POST_UPDATE_DATA,
    MAX_SLOT_POST_UPDATES, MAX_SLOT_PRE_UPDATE_DATA, MAX_SLOT_PRE_UPDATES,
    MAX_TX_ACCOUNT_UPDATE_DATA, MAX_TX_ACCOUNT_UPDATES,
};
use crate::transactions::{
    Transaction, TransactionStatus, decode_option_log_messages_into, decode_option_zerovec_into,
};

use super::format::*;

/// Hard allocation bound for one decompressed bucket. The observed 19-epoch
/// corpus maximum is 668,740,835 bytes (~637.8 MiB); 768 MiB leaves ~130.2
/// MiB (20.4%) of headroom while keeping hostile-input amplification bounded.
pub const MAX_BUCKET_UNCOMPRESSED_BYTES: u64 = 768 << 20;

/// Maximum indexed bucket frame (encoded header plus stored payload). Four
/// MiB above the raw ceiling covers the worst-case Zstd/LZ4 compression bound
/// at 768 MiB as well as the small bucket header.
pub const MAX_BUCKET_STORED_BYTES: u64 = MAX_BUCKET_UNCOMPRESSED_BYTES + (4 << 20);

/// Persistent keyed account-data capacity retained by the diff codec for one
/// bucket. This is distinct from per-slot semantic work: the cache survives
/// all slot frames until the bucket boundary resets it. The raw-largest
/// measured stress case (epoch 941 bucket 1) reached 280,235,301 bytes, so
/// 768 MiB leaves 187.4% headroom over that case.
pub const MAX_BUCKET_DIFF_CACHE_BYTES: usize = MAX_BUCKET_UNCOMPRESSED_BYTES as usize;

/// Maximum distinct account-data histories retained in one bucket. A
/// separate count is required because zero-length values consume hash-table
/// storage without consuming the byte budget. That measured stress case
/// retained 595,411 keys; two million leaves 235.9% headroom over it while
/// keeping hash-table metadata independently bounded.
pub const MAX_BUCKET_DIFF_CACHE_KEYS: usize = 2_000_000;

/// Maximum collection element count accepted inside one slot frame.
pub const MAX_FRAME_SEQUENCE_ELEMENTS: usize = 1_000_000;

/// Cumulative semantic output/work claims accepted while decoding one slot
/// frame. This is deliberately separate from resident allocation: epoch 960,
/// bucket 137, slot offset 100 streams 3,992,568,327 claimed bytes through
/// reused account-data buffers. Six GiB leaves 61.4% headroom over that
/// measured maximum while still bounding amplification from hostile inputs.
pub const MAX_FRAME_CUMULATIVE_DECODE_BYTES: usize = if usize::BITS >= 64 {
    (6u64 << 30) as usize
} else {
    usize::MAX
};

/// Cumulative semantic decode work allowed across all frames in one bucket.
/// The per-frame limit above remains in force; each new frame receives only
/// the smaller of its 6 GiB allowance and this bucket's remaining budget.
/// The measured maximum is epoch 946 bucket 39 at 38,766,100,655 claimed bytes
/// (36.103 GiB) across 128 frames. Sixty-four GiB leaves 77.3% headroom while
/// keeping a hostile bucket's aggregate semantic work bounded independently
/// of its 768 MiB resident payload ceiling.
pub const MAX_BUCKET_CUMULATIVE_DECODE_BYTES: u64 = 64 << 30;

/// Cumulative allocation claims accepted inside the small file header.
pub const MAX_HEADER_ALLOCATION_BYTES: usize = 4 << 20;

/// Decodes a file header while bounding both encoded input and decompressed
/// byte blobs. Header metadata can contain an independently compressed
/// provenance blob, so its decoded length may legitimately exceed the
/// encoded header length.
pub(crate) fn decode_file_header_bytes(
    header_bytes: &[u8],
) -> Result<FileHeader, ArchiveFormatError> {
    let limits = DecodeLimits::new(
        header_bytes.len(),
        header_bytes.len(),
        MAX_HEADER_ALLOCATION_BYTES,
    );
    let cursor = lencode::io::Cursor::new(header_bytes);
    let mut reader =
        LimitedReader::new(cursor, limits).with_max_blob_bytes(MAX_HEADER_ALLOCATION_BYTES);
    let header = FileHeader::decode_ext(&mut reader, None)?;
    if reader.inner().position() != header_bytes.len() {
        return Err(ArchiveFormatError::Encode(lencode::io::Error::TrailingData));
    }
    Ok(header)
}

/// What a [`SlotVisitor`] actually consumes from the decoded stream, declared
/// up front so the decoder can skip work whose output nobody reads.
///
/// Defaults to everything (safe). Build with [`Consumption::all`] and drop
/// what you don't need:
///
/// ```
/// use jetstreamer_horizon::archive::Consumption;
/// let metadata_only = Consumption::all().without_account_update_data();
/// assert!(!metadata_only.account_update_data);
/// ```
///
/// Extensible: more skippable dimensions may be added, so construct via the
/// builders rather than struct literals.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct Consumption {
    /// Materialize account-update data bytes (the per-account diff
    /// reconstruction). When `false`, every update still arrives with full
    /// metadata (pubkey, lamports, owner, executable, rent_epoch,
    /// write_version) and correct counts, but its `data` slice is empty;
    /// the decoder validates canonical local diff framing and every declared
    /// size/aggregate limit, then skips reconstruction. It intentionally does
    /// not validate stateful relationships to prior keyed values or the inner
    /// contents of compressed diff payloads. In archives dominated by account
    /// state this skips the bulk of decode memory traffic.
    pub account_update_data: bool,
    /// Populate the grouped pre/post account-update arenas on the terminal
    /// [`BlockNotification`]. Ordered visitors that consume
    /// [`SlotVisitor::on_pre_account_update`] and
    /// [`SlotVisitor::on_post_account_update`] directly can disable this to
    /// avoid copying each reconstructed data blob into decoder scratch.
    pub block_account_update_arenas: bool,
}

impl Consumption {
    /// Consume everything the archive stores (the default).
    pub const fn all() -> Self {
        Self {
            account_update_data: true,
            block_account_update_arenas: true,
        }
    }

    /// Drop account-update data bytes: updates keep metadata and counts but
    /// their `data` slices decode empty.
    pub const fn without_account_update_data(mut self) -> Self {
        self.account_update_data = false;
        self
    }

    /// Deliver ordered pre/post callbacks without also copying those updates
    /// into the block notification's grouped arenas.
    pub const fn without_block_account_update_arenas(mut self) -> Self {
        self.block_account_update_arenas = false;
        self
    }

    /// The union of two declarations: a stream field is consumed if either
    /// side consumes it. Used by fan-out visitors to combine their
    /// children's declarations.
    pub const fn union(self, other: Self) -> Self {
        Self {
            account_update_data: self.account_update_data || other.account_update_data,
            block_account_update_arenas: self.block_account_update_arenas
                || other.block_account_update_arenas,
        }
    }
}

impl Default for Consumption {
    fn default() -> Self {
        Self::all()
    }
}

/// How parent-blockhash mismatches are handled when chain verification is
/// enabled.
///
/// Strict rejection remains the default. The historical compatibility mode
/// exists for old archives written across a mid-epoch firehose restart: those
/// writers could store a zero `parent_blockhash` on the first block after the
/// restart even though the preceding block and its hash are present. No other
/// mismatch is accepted.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ChainMismatchPolicy {
    /// Reject every mismatch against the preceding block or bucket anchor.
    #[default]
    Reject,
    /// Accept a mismatch only when the stored parent hash is zero, counting it
    /// as a historical writer-resume artifact.
    AllowZeroParentResume,
}

/// Callbacks invoked while decoding slot frames, in wire order: slot start,
/// epoch notification (boundary slots), pre-transaction runtime updates,
/// every transaction, post-transaction runtime updates, then the slot's
/// block notification. By default, runtime-direct ("orphan") updates are
/// also grouped on the [`BlockNotification`]'s pre/post arenas, and each
/// transaction's own updates are reachable via
/// [`Transaction::iter_account_updates`].
pub trait SlotVisitor {
    /// Start of a decoded slot frame, before any epoch metadata, orphan
    /// account updates, or transactions are emitted. Ordered consumers such
    /// as archive re-encoders use this to open the destination slot without
    /// buffering its (potentially very large) contents.
    fn on_slot_start(&mut self, _slot: u64, _kind: SlotKind) {}
    /// Epoch notification (fires before the boundary slot's transactions).
    /// `meta` points at the decoder's reusable scratch; copy out what you
    /// need.
    fn on_epoch(&mut self, _meta: &EpochMeta) {}
    /// One runtime-direct account update applied before this slot's
    /// transactions. The view borrows decoder scratch and is valid only for
    /// the callback.
    fn on_pre_account_update(&mut self, _slot: u64, _update: &AccountUpdateView<'_>) {}
    /// One decoded transaction (with nested account updates). `tx` points
    /// at the decoder's reusable scratch buffer; copy out what you need.
    fn on_transaction(&mut self, _slot: u64, _tx_index: u32, _tx: &Transaction) {}
    /// One runtime-direct account update applied after this slot's
    /// transactions. The view borrows decoder scratch and is valid only for
    /// the callback.
    fn on_post_account_update(&mut self, _slot: u64, _update: &AccountUpdateView<'_>) {}
    /// End of a slot frame: the block notification (full block with
    /// metadata + grouped orphan updates, or a leader-skipped marker) plus
    /// the block's PoH entry records (empty for skipped slots).
    fn on_block(&mut self, _notification: &BlockNotification, _entries: &[EntryRecord]) {}
    /// Declares what this visitor consumes; drivers sample it and configure
    /// the decoder to skip unconsumed work. Defaults to everything (safe).
    /// Must be stable for the duration of a decode drive. Drivers latch it
    /// at bucket boundaries.
    fn consumption(&self) -> Consumption {
        Consumption::all()
    }
}

/// Per-category breakdown of the *uncompressed payload* bytes a decoder has
/// processed: the deduped and diff-encoded stream within buckets, before the
/// per-bucket zstd. Accumulated across every [`BucketDecoder::decode_slot_frame`]
/// call; query [`BucketDecoder::byte_stats`]. This is the in-archive
/// representation (account data is already diff-encoded here), not raw bytes.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct PayloadByteStats {
    /// Transaction-record bytes, excluding each transaction's own account
    /// updates (signatures, message, status/meta, balances, logs, etc.).
    pub transaction_bytes: u64,
    /// Account-update bytes: the account-state write stream (metadata +
    /// diff-encoded data blobs) across tx-owned, runtime-direct orphan, and
    /// epoch updates.
    pub account_update_bytes: u64,
    /// Everything else: block metadata + rewards, entry records, epoch
    /// scalars, and per-slot framing.
    pub other_bytes: u64,
}

impl PayloadByteStats {
    /// Total payload bytes accounted for (the sum of the three categories).
    pub fn total(&self) -> u64 {
        self.transaction_bytes + self.account_update_bytes + self.other_bytes
    }
}

/// Decodes slot frames from a single bucket's raw bytes, holding the
/// per-bucket reset state (dedupe scratch, diff decoder, payload buffer,
/// and reusable record scratches). Reuse one instance across many buckets:
/// [`load_bucket_bytes`](Self::load_bucket_bytes) resets all state for the
/// next bucket, so the heap scratches are allocated once.
pub struct BucketDecoder {
    /// Verify blockhash chain continuity (parent_blockhash linkage) while
    /// decoding. Full SHA-256 PoH recomputation is a planned follow-up; the
    /// format already stores everything it needs.
    pub verify_chain: bool,
    /// Policy for a detected parent-blockhash mismatch. This is consulted only
    /// when [`Self::verify_chain`] is enabled and defaults to strict rejection.
    pub chain_mismatch_policy: ChainMismatchPolicy,
    /// Materialize account-update data (the per-account diff
    /// reconstruction). When `false`, updates decode with full metadata but
    /// empty `data` slices, skipping reconstruction and the diff store
    /// entirely. Canonical local framing and declared semantic size limits are
    /// still checked; stateful diff relationships and compressed inner bytes
    /// are not. Latched per bucket at
    /// [`load_bucket_bytes`](Self::load_bucket_bytes): diff records resolve
    /// against blobs stored earlier in the *same* bucket, so flipping
    /// mid-bucket would corrupt or fail reconstruction. The effective
    /// setting changes only when a bucket is (re)loaded.
    pub materialize_account_data: bool,
    /// Populate pre/post account-update arenas on block notifications. This
    /// does not affect the ordered borrowed update callbacks.
    pub materialize_block_account_update_arenas: bool,

    archive_version: ArchiveVersion,
    bucket_slots: Option<u16>,
    archive_slot_range: Option<(u64, u64)>,

    /// The materialization setting latched when the current bucket was
    /// loaded (see [`Self::materialize_account_data`]).
    bucket_materialize: bool,
    /// Reused zstd decompression context (see `load_bucket_bytes`).
    zstd: zstd::bulk::Decompressor<'static>,
    payload: Vec<u8>,
    pos: usize,
    slots_remaining: u32,
    /// Slot of the last frame decoded (whether or not it was emitted). Lets
    /// a streaming driver continue in place on forward reads.
    last_decoded_slot: Option<u64>,
    next_expected_slot: u64,
    dec_ctx: DecoderContext,
    diff: DiffDecoder,
    scratch: Box<Transaction>,
    // Two permanent notification scratches, with one pinned to each variant.
    // Swapping a single scratch's variant would memset the whole ~40 MiB
    // enum on every skipped-to-block boundary (measured at about 400 us per swap);
    // with pinned variants the zeroing happens exactly twice, here at
    // construction.
    block_scratch: Box<BlockNotification>,
    skipped_scratch: Box<BlockNotification>,
    epoch_scratch: Box<EpochMeta>,
    entries_scratch: Vec<EntryRecord>,
    last_blockhash: Hash,
    /// Historical zero-parent resume artifacts accepted across all decoded
    /// frames since construction. Bucket loads intentionally do not reset it.
    zero_parent_resume_artifacts: u64,
    /// Header of the currently loaded bucket. Retained so archive
    /// re-encoders can preserve an independently decodable bucket's PoH
    /// anchor without reading or buffering the bucket twice.
    bucket_header: Option<BucketHeader>,
    /// Running per-category payload-byte tally (cheap; per-section, not
    /// per-record). Queried via [`Self::byte_stats`].
    byte_stats: PayloadByteStats,
    /// Semantic allocation/work claims consumed by the loaded bucket. Unlike
    /// `LimitedReader`'s frame-local counter, this persists across slots.
    bucket_decode_work_bytes: u64,
    /// Configured per-bucket work ceiling (constant in production; injectable
    /// in tests so amplification failures do not require multi-GiB fixtures).
    max_bucket_decode_work_bytes: u64,
    /// Production phase limits, stored so tests can exercise identical
    /// validation paths with compact fixtures.
    max_pre_updates: usize,
    max_pre_data_bytes: usize,
    max_post_updates: usize,
    max_post_data_bytes: usize,
    max_epoch_updates: usize,
    max_epoch_data_bytes: usize,
    max_tx_updates: usize,
    max_tx_data_bytes: usize,
}

impl Default for BucketDecoder {
    fn default() -> Self {
        Self::new()
    }
}

impl BucketDecoder {
    /// Allocates the reusable decode scratches once. Cheap to keep around;
    /// expensive to recreate (the notification scratches are ~40 MiB each).
    /// This retains the historical V1 default for raw-bucket API callers;
    /// archive readers should use [`Self::for_file_header`].
    pub fn new() -> Self {
        Self::for_archive_version(ArchiveVersion::V1)
    }

    /// Allocates a decoder configured for an already validated archive wire
    /// version.
    pub fn for_archive_version(version: ArchiveVersion) -> Self {
        Self {
            verify_chain: false,
            chain_mismatch_policy: ChainMismatchPolicy::Reject,
            materialize_account_data: true,
            materialize_block_account_update_arenas: true,
            archive_version: version,
            bucket_slots: None,
            archive_slot_range: None,
            bucket_materialize: true,
            zstd: zstd::bulk::Decompressor::new().expect("failed to create zstd context"),
            payload: Vec::new(),
            pos: 0,
            slots_remaining: 0,
            last_decoded_slot: None,
            next_expected_slot: 0,
            dec_ctx: new_decoder_context_with_codec(match version {
                ArchiveVersion::V1 => DedupeIdCodec::Lencode,
                ArchiveVersion::V2 => DedupeIdCodec::UnsignedLeb128,
            }),
            diff: DiffDecoder::with_capacity_mode_and_cache_limits(
                64 * 1024,
                match version {
                    ArchiveVersion::V1 => 2,
                    ArchiveVersion::V2 => 3,
                },
                MAX_BUCKET_DIFF_CACHE_BYTES,
                MAX_BUCKET_DIFF_CACHE_KEYS,
            ),
            scratch: Transaction::new_boxed(),
            block_scratch: {
                // Pin to the Block variant once (one-time 40 MiB zeroing).
                let mut b = BlockNotification::new_boxed();
                set_notification_block(&mut b);
                b
            },
            skipped_scratch: BlockNotification::new_boxed(), // already Skipped
            epoch_scratch: EpochMeta::new_boxed(),
            entries_scratch: Vec::with_capacity(2048),
            last_blockhash: Hash::default(),
            zero_parent_resume_artifacts: 0,
            bucket_header: None,
            byte_stats: PayloadByteStats::default(),
            bucket_decode_work_bytes: 0,
            max_bucket_decode_work_bytes: MAX_BUCKET_CUMULATIVE_DECODE_BYTES,
            max_pre_updates: MAX_SLOT_PRE_UPDATES,
            max_pre_data_bytes: MAX_SLOT_PRE_UPDATE_DATA,
            max_post_updates: MAX_SLOT_POST_UPDATES,
            max_post_data_bytes: MAX_SLOT_POST_UPDATE_DATA,
            max_epoch_updates: MAX_EPOCH_UPDATES,
            max_epoch_data_bytes: MAX_EPOCH_UPDATE_DATA,
            max_tx_updates: MAX_TX_ACCOUNT_UPDATES,
            max_tx_data_bytes: MAX_TX_ACCOUNT_UPDATE_DATA,
        }
    }

    /// Allocates a decoder bound to a validated file header's version and
    /// slot geometry.
    pub fn for_file_header(header: &FileHeader) -> Result<Self, ArchiveFormatError> {
        let version = validate_archive_version(header.format_version, header.flags)?;
        if header.bucket_slots == 0 {
            return Err(ArchiveFormatError::InvalidContainerLayout(
                "bucket_slots must be nonzero",
            ));
        }
        let slot_end = header.slot_start.checked_add(header.slot_count).ok_or(
            ArchiveFormatError::InvalidContainerLayout("slot range overflows u64"),
        )?;
        let mut decoder = Self::for_archive_version(version);
        decoder.bucket_slots = Some(header.bucket_slots);
        decoder.archive_slot_range = Some((header.slot_start, slot_end));
        Ok(decoder)
    }

    /// The running per-category payload-byte tally accumulated across every
    /// [`decode_slot_frame`](Self::decode_slot_frame) since construction.
    pub fn byte_stats(&self) -> PayloadByteStats {
        self.byte_stats
    }

    /// Number of zero-parent chain mismatches accepted under
    /// [`ChainMismatchPolicy::AllowZeroParentResume`] since construction.
    ///
    /// This counts decoded observations, so explicitly re-reading a bucket
    /// counts its artifacts again.
    pub fn zero_parent_resume_artifacts(&self) -> u64 {
        self.zero_parent_resume_artifacts
    }

    /// Loads and validates one bucket frame (`BucketHeader ++ stored
    /// payload`), decompressing into the internal payload buffer and
    /// resetting all decoder state for the new bucket. After this,
    /// [`decode_slot_frame`](Self::decode_slot_frame) yields the bucket's
    /// frames in order.
    pub fn load_bucket_bytes(&mut self, raw: &[u8]) -> Result<(), ArchiveFormatError> {
        self.load_bucket_bytes_inner(raw, None)
    }

    /// Loads a bucket while binding its decoded header to the authenticated
    /// container index entry used to fetch it.
    pub fn load_indexed_bucket_bytes(
        &mut self,
        raw: &[u8],
        entry: BucketIndexEntry,
    ) -> Result<(), ArchiveFormatError> {
        self.load_bucket_bytes_inner(raw, Some(entry))
    }

    fn load_bucket_bytes_inner(
        &mut self,
        raw: &[u8],
        expected: Option<BucketIndexEntry>,
    ) -> Result<(), ArchiveFormatError> {
        self.bucket_header = None;
        if let Some(entry) = expected
            && raw.len() as u64 != entry.len
        {
            return Err(ArchiveFormatError::InvalidContainerLayout(
                "fetched bucket length disagrees with index",
            ));
        }
        let mut cur = lencode::io::Cursor::new(raw);
        let header = BucketHeader::decode_ext(&mut cur, None)?;
        if let Some(entry) = expected
            && header.first_slot != entry.first_slot
        {
            return Err(ArchiveFormatError::BucketHeaderMismatch {
                indexed: entry.first_slot,
                decoded: header.first_slot,
            });
        }
        if header.slot_count == 0 {
            return Err(ArchiveFormatError::InvalidBucketSlotCount {
                first_slot: header.first_slot,
                slot_count: header.slot_count,
            });
        }
        if self.archive_version == ArchiveVersion::V1 && header.compression == Compression::Lz4 {
            return Err(ArchiveFormatError::UnsupportedCompressionForVersion {
                version: self.archive_version.as_u16(),
                compression: header.compression,
            });
        }
        if let (Some(bucket_slots), Some((slot_start, slot_end))) =
            (self.bucket_slots, self.archive_slot_range)
        {
            let valid_first = header.first_slot >= slot_start
                && header.first_slot < slot_end
                && (header.first_slot - slot_start) % u64::from(bucket_slots) == 0;
            let remaining = slot_end.saturating_sub(header.first_slot);
            let max_slots = u64::from(bucket_slots).min(remaining);
            if !valid_first || u64::from(header.slot_count) > max_slots {
                return Err(ArchiveFormatError::InvalidBucketSlotCount {
                    first_slot: header.first_slot,
                    slot_count: header.slot_count,
                });
            }
        }
        let header_len = cur.position();
        let stored = &raw[header_len..];
        if stored.len() as u64 != header.stored_len {
            return Err(ArchiveFormatError::BucketChecksum {
                first_slot: header.first_slot,
            });
        }
        if xxh64(stored, 0) != header.xxh64 {
            return Err(ArchiveFormatError::BucketChecksum {
                first_slot: header.first_slot,
            });
        }
        if header.uncompressed_len > MAX_BUCKET_UNCOMPRESSED_BYTES {
            return Err(ArchiveFormatError::BucketTooLarge {
                first_slot: header.first_slot,
                bytes: header.uncompressed_len,
            });
        }
        let uncompressed_len = usize::try_from(header.uncompressed_len).map_err(|_| {
            ArchiveFormatError::BucketTooLarge {
                first_slot: header.first_slot,
                bytes: header.uncompressed_len,
            }
        })?;

        try_reserve_archive_buffer(
            &mut self.payload,
            uncompressed_len,
            "uncompressed bucket payload",
        )?;
        match header.compression {
            Compression::None => {
                if stored.len() != uncompressed_len {
                    return Err(ArchiveFormatError::BucketChecksum {
                        first_slot: header.first_slot,
                    });
                }
                self.payload.extend_from_slice(stored);
            }
            Compression::Zstd => {
                let frame_size =
                    zstd::zstd_safe::find_frame_compressed_size(stored).map_err(|_| {
                        ArchiveFormatError::BucketChecksum {
                            first_slot: header.first_slot,
                        }
                    })?;
                if frame_size != stored.len() {
                    return Err(ArchiveFormatError::TrailingBucketBytes {
                        first_slot: header.first_slot,
                        bytes: stored.len().abs_diff(frame_size),
                    });
                }
                // Decompress into the retained buffer with a reused context:
                // a fresh ~600 MB Vec per bucket means thousands of huge
                // mmap/munmap cycles per epoch, whose kernel-side cost grows
                // with thread count. Steady state this allocates nothing.
                self.payload.resize(uncompressed_len, 0);
                let written = self
                    .zstd
                    .decompress_to_buffer(stored, &mut self.payload[..])
                    .map_err(ArchiveFormatError::Io)?;
                if written as u64 != header.uncompressed_len {
                    return Err(ArchiveFormatError::BucketChecksum {
                        first_slot: header.first_slot,
                    });
                }
            }
            Compression::Lz4 => {
                if stored.len() > i32::MAX as usize {
                    return Err(ArchiveFormatError::BucketTooLarge {
                        first_slot: header.first_slot,
                        bytes: stored.len() as u64,
                    });
                }
                self.payload.resize(uncompressed_len, 0);
                let written = lz4::block::decompress_to_buffer(
                    stored,
                    Some(uncompressed_len as i32),
                    &mut self.payload,
                )
                .map_err(ArchiveFormatError::Io)?;
                if written != uncompressed_len {
                    return Err(ArchiveFormatError::BucketChecksum {
                        first_slot: header.first_slot,
                    });
                }
            }
        }
        if self.payload.len() != uncompressed_len {
            return Err(ArchiveFormatError::BucketChecksum {
                first_slot: header.first_slot,
            });
        }

        self.pos = 0;
        self.slots_remaining = header.slot_count;
        self.last_decoded_slot = None;
        self.next_expected_slot = header.first_slot;
        self.last_blockhash = header.poh_start_hash;
        reset_decoder(&mut self.dec_ctx);
        self.diff.clear();
        self.bucket_decode_work_bytes = 0;
        // Latch the materialization mode for this whole bucket: the diff
        // store just reset, so a bucket decoded end-to-end without
        // materialization never misses a stored blob, and the next bucket
        // starts clean either way.
        self.bucket_materialize = self.materialize_account_data;
        self.bucket_header = Some(header);
        Ok(())
    }

    /// Header of the currently loaded bucket, if loading completed
    /// successfully.
    pub(crate) fn current_bucket_header(&self) -> Option<&BucketHeader> {
        self.bucket_header.as_ref()
    }

    /// Number of slot frames not yet decoded from the loaded bucket.
    pub fn slots_remaining(&self) -> u32 {
        self.slots_remaining
    }

    /// Slot of the last frame decoded from the loaded bucket, if any.
    pub fn last_decoded_slot(&self) -> Option<u64> {
        self.last_decoded_slot
    }

    /// The materialization setting latched when the current bucket was
    /// loaded. Drivers that keep a bucket resident across visitor changes
    /// (e.g. [`ArchiveReader`](super::ArchiveReader)'s continue-in-place
    /// path) must reload when this disagrees with the requested
    /// [`Self::materialize_account_data`].
    pub fn bucket_materializes_account_data(&self) -> bool {
        self.bucket_materialize
    }

    /// Cumulative semantic allocation/work claims consumed by the currently
    /// loaded bucket.
    pub fn bucket_decode_work_bytes(&self) -> u64 {
        self.bucket_decode_work_bytes
    }

    /// Peak retained account-data capacity reached while decoding the loaded
    /// bucket.
    pub fn bucket_diff_cache_peak_bytes(&self) -> usize {
        self.diff.peak_cache_capacity_bytes()
    }

    /// Peak number of keyed account histories reached in the loaded bucket.
    pub fn bucket_diff_cache_peak_keys(&self) -> usize {
        self.diff.peak_cache_keys()
    }

    #[cfg(test)]
    pub(super) fn set_bucket_decode_work_limit_for_test(&mut self, bytes: u64) {
        self.max_bucket_decode_work_bytes = bytes;
    }

    #[cfg(test)]
    pub(super) fn set_pre_update_limits_for_test(&mut self, count: usize, bytes: usize) {
        self.max_pre_updates = count;
        self.max_pre_data_bytes = bytes;
    }

    #[cfg(test)]
    pub(super) fn set_phase_limits_for_test(
        &mut self,
        epoch: (usize, usize),
        transaction: (usize, usize),
        pre: (usize, usize),
        post: (usize, usize),
    ) {
        (self.max_epoch_updates, self.max_epoch_data_bytes) = epoch;
        (self.max_tx_updates, self.max_tx_data_bytes) = transaction;
        (self.max_pre_updates, self.max_pre_data_bytes) = pre;
        (self.max_post_updates, self.max_post_data_bytes) = post;
    }

    /// Convenience: load `raw` then decode every remaining frame whose slot
    /// is `>= start_slot`, emitting at most `max_slots` of them. Returns the
    /// number emitted. When the caller needs a tighter upper bound (e.g. stop
    /// at a specific end slot), drive the granular
    /// [`load_bucket_bytes`](Self::load_bucket_bytes) and
    /// [`decode_slot_frame`](Self::decode_slot_frame) pair directly.
    pub fn decode_bucket<V: SlotVisitor>(
        &mut self,
        raw: &[u8],
        start_slot: u64,
        max_slots: u64,
        visitor: &mut V,
    ) -> Result<u64, ArchiveFormatError> {
        self.load_bucket_bytes(raw)?;
        let mut visited = 0u64;
        while visited < max_slots && self.slots_remaining > 0 {
            if self.decode_slot_frame(start_slot, visitor)? {
                visited += 1;
            }
        }
        Ok(visited)
    }

    /// Decodes one slot frame from the loaded bucket. Emits callbacks only
    /// when `slot >= start_slot`; returns whether callbacks fired. Frames
    /// below `start_slot` are still decoded (their bytes must flow through
    /// the dedupe/diff decoders to reproduce encoder state) but not emitted.
    pub fn decode_slot_frame<V: SlotVisitor>(
        &mut self,
        start_slot: u64,
        visitor: &mut V,
    ) -> Result<bool, ArchiveFormatError> {
        let remaining = &self.payload[self.pos..];
        let cursor = lencode::io::Cursor::new(remaining);
        let remaining_bucket_work = self
            .max_bucket_decode_work_bytes
            .saturating_sub(self.bucket_decode_work_bytes);
        let frame_work_limit =
            remaining_bucket_work.min(MAX_FRAME_CUMULATIVE_DECODE_BYTES as u64) as usize;
        let mut cur = LimitedReader::new(
            cursor,
            DecodeLimits::new(
                remaining.len(),
                MAX_FRAME_SEQUENCE_ELEMENTS,
                frame_work_limit,
            ),
        )
        .with_max_blob_bytes(MAX_ACCOUNT_DATA_LEN);
        let slot = u64::decode_ext(&mut cur, None)?;
        if slot != self.next_expected_slot {
            return Err(ArchiveFormatError::UnexpectedBucketSlot {
                expected: self.next_expected_slot,
                decoded: slot,
            });
        }
        self.next_expected_slot = self.next_expected_slot.checked_add(1).ok_or(
            ArchiveFormatError::InvalidContainerLayout("bucket slot sequence overflows u64"),
        )?;
        let mut kind = [0u8; 1];
        cur.read(&mut kind)?;
        let kind = SlotKind::try_from(kind[0])?;
        let emit = slot >= start_slot;
        let materialize = self.bucket_materialize;

        if emit {
            visitor.on_slot_start(slot, kind);
        }

        // Per-category payload byte tally for this frame (account updates and
        // transaction fields; everything else is derived at the end).
        let mut au_bytes: usize = 0;
        let mut tx_field_bytes: usize = 0;

        match kind {
            SlotKind::Skipped => {
                set_notification_skipped(&mut self.skipped_scratch, slot);
                self.entries_scratch.clear();
                if emit {
                    visitor.on_block(&self.skipped_scratch, &self.entries_scratch);
                }
            }
            SlotKind::Block => {
                let meta = set_notification_block(&mut self.block_scratch);
                meta.clear();
                meta.slot = slot;

                // Section 1: optional epoch notification.
                let mut flag = [0u8; 1];
                cur.read(&mut flag)?;
                if flag[0] > 1 {
                    return Err(ArchiveFormatError::InvalidContainerLayout(
                        "epoch presence flag must be 0 or 1",
                    ));
                }
                if flag[0] == 1 {
                    self.epoch_scratch.clear();
                    self.epoch_scratch.epoch = u64::decode_ext(&mut cur, None)?;
                    self.epoch_scratch.start_slot = u64::decode_ext(&mut cur, None)?;
                    self.epoch_scratch.slot_count = u64::decode_ext(&mut cur, None)?;
                    self.epoch_scratch.first_block_slot = u64::decode_ext(&mut cur, None)?;
                    self.epoch_scratch.num_reward_partitions =
                        Option::<u64>::decode_ext(&mut cur, None)?;
                    let n = decode_bounded_count(&mut cur, 1)?;
                    enforce_phase_count("epoch account updates", n, self.max_epoch_updates)?;
                    let upd_start = cur.consumed();
                    let mut epoch_data_bytes = 0usize;
                    for _ in 0..n {
                        let updates = &mut self.epoch_scratch.updates;
                        let data_len = decode_update_record_into(
                            &mut cur,
                            &mut self.dec_ctx,
                            &mut self.diff,
                            materialize,
                            |view| updates.push(view),
                        )?;
                        add_phase_data(
                            "epoch account-update data",
                            &mut epoch_data_bytes,
                            data_len,
                            self.max_epoch_data_bytes,
                        )?;
                    }
                    au_bytes += cur.consumed() - upd_start;
                    if emit {
                        visitor.on_epoch(&self.epoch_scratch);
                    }
                }

                // Section 2: pre-transaction orphan updates, grouped onto
                // the notification's pre arena.
                let pre_count = decode_bounded_count(&mut cur, 1)?;
                enforce_phase_count("pre-transaction updates", pre_count, self.max_pre_updates)?;
                let pre_start = cur.consumed();
                let mut pre_data_bytes = 0usize;
                for _ in 0..pre_count {
                    let pre = &mut meta.pre_updates;
                    let materialize_arena = self.materialize_block_account_update_arenas;
                    let data_len = decode_update_record_into(
                        &mut cur,
                        &mut self.dec_ctx,
                        &mut self.diff,
                        materialize,
                        |view| {
                            if emit {
                                visitor.on_pre_account_update(slot, view);
                            }
                            if materialize_arena {
                                pre.push(view)
                            } else {
                                Ok(())
                            }
                        },
                    )?;
                    add_phase_data(
                        "pre-transaction account-update data",
                        &mut pre_data_bytes,
                        data_len,
                        self.max_pre_data_bytes,
                    )?;
                }
                au_bytes += cur.consumed() - pre_start;

                // Section 3: transactions.
                let tx_count_raw = u64::decode_ext(&mut cur, None)?;
                let tx_count = u32::try_from(tx_count_raw).map_err(|_| {
                    ArchiveFormatError::Encode(lencode::io::Error::DecodeLimitExceeded)
                })?;
                cur.claim_sequence(tx_count as usize, 1)?;
                for tx_index in 0..tx_count {
                    let tx_start = cur.consumed();
                    let tx_au = read_tx_record(
                        &mut cur,
                        &mut self.scratch,
                        &mut self.dec_ctx,
                        &mut self.diff,
                        materialize,
                        self.max_tx_updates,
                        self.max_tx_data_bytes,
                    )?;
                    tx_field_bytes += (cur.consumed() - tx_start) - tx_au;
                    au_bytes += tx_au;
                    if emit {
                        visitor.on_transaction(slot, tx_index, &self.scratch);
                    }
                }

                // Section 4: post-transaction orphan updates.
                let meta = match &mut *self.block_scratch {
                    BlockNotification::Block(m) => m,
                    _ => unreachable!(),
                };
                let post_count = decode_bounded_count(&mut cur, 1)?;
                enforce_phase_count(
                    "post-transaction updates",
                    post_count,
                    self.max_post_updates,
                )?;
                let post_start = cur.consumed();
                let mut post_data_bytes = 0usize;
                for _ in 0..post_count {
                    let post = &mut meta.post_updates;
                    let materialize_arena = self.materialize_block_account_update_arenas;
                    let data_len = decode_update_record_into(
                        &mut cur,
                        &mut self.dec_ctx,
                        &mut self.diff,
                        materialize,
                        |view| {
                            if emit {
                                visitor.on_post_account_update(slot, view);
                            }
                            if materialize_arena {
                                post.push(view)
                            } else {
                                Ok(())
                            }
                        },
                    )?;
                    add_phase_data(
                        "post-transaction account-update data",
                        &mut post_data_bytes,
                        data_len,
                        self.max_post_data_bytes,
                    )?;
                }
                au_bytes += cur.consumed() - post_start;

                // Section 5: block metadata scalars + rewards.
                meta.parent_slot = u64::decode_ext(&mut cur, None)?;
                meta.parent_blockhash = Hash::decode_ext(&mut cur, None)?;
                meta.blockhash = Hash::decode_ext(&mut cur, None)?;
                meta.block_time = Option::<i64>::decode_ext(&mut cur, None)?;
                meta.block_height = Option::<u64>::decode_ext(&mut cur, None)?;
                meta.executed_transaction_count = u64::decode_ext(&mut cur, None)?;
                meta.entry_count = u64::decode_ext(&mut cur, None)?;
                meta.rewards.decode_into(&mut cur, None)?;
                meta.num_partitions = Option::<u64>::decode_ext(&mut cur, None)?;

                // Section 6: entry records.
                let entry_count =
                    decode_bounded_count(&mut cur, core::mem::size_of::<EntryRecord>())?;
                self.entries_scratch.clear();
                self.entries_scratch
                    .try_reserve_exact(entry_count)
                    .map_err(|_| ArchiveFormatError::AllocationFailed {
                        section: "entry records",
                        bytes: entry_count
                            .checked_mul(core::mem::size_of::<EntryRecord>())
                            .map_or(u64::MAX, |bytes| bytes as u64),
                    })?;
                for _ in 0..entry_count {
                    self.entries_scratch
                        .push(EntryRecord::decode_ext(&mut cur, None)?);
                }

                if self.verify_chain
                    && self.last_blockhash != Hash::default()
                    && meta.parent_blockhash != self.last_blockhash
                {
                    if self.chain_mismatch_policy == ChainMismatchPolicy::AllowZeroParentResume
                        && meta.parent_blockhash == Hash::default()
                    {
                        self.zero_parent_resume_artifacts += 1;
                    } else {
                        return Err(ArchiveFormatError::PohMismatch { slot });
                    }
                }
                self.last_blockhash = meta.blockhash;

                if emit {
                    visitor.on_block(&self.block_scratch, &self.entries_scratch);
                }
            }
        }

        let frame_bytes = cur.consumed();
        self.bucket_decode_work_bytes = self
            .bucket_decode_work_bytes
            .checked_add(cur.claimed_allocation() as u64)
            .ok_or(ArchiveFormatError::Encode(
                lencode::io::Error::DecodeLimitExceeded,
            ))?;
        self.byte_stats.account_update_bytes += au_bytes as u64;
        self.byte_stats.transaction_bytes += tx_field_bytes as u64;
        self.byte_stats.other_bytes += (frame_bytes - au_bytes - tx_field_bytes) as u64;
        self.pos += frame_bytes;
        self.slots_remaining -= 1;
        self.last_decoded_slot = Some(slot);
        if self.slots_remaining == 0 && self.pos != self.payload.len() {
            return Err(ArchiveFormatError::TrailingBucketBytes {
                first_slot: self
                    .bucket_header
                    .as_ref()
                    .map_or(slot, |header| header.first_slot),
                bytes: self.payload.len().abs_diff(self.pos),
            });
        }
        Ok(emit)
    }
}

/// Parses and validates the archive's [`FileHeader`] from a prefix of the
/// file (magic ++ varint(header_len) ++ header). The prefix must cover the
/// whole header section; a few KiB is always enough. Returns the header and
/// the number of bytes the header section occupies. Used by network drivers
/// that fetch the file's front bytes via a range request.
pub fn parse_file_header(prefix: &[u8]) -> Result<(FileHeader, usize), ArchiveFormatError> {
    let mut cur = std::io::Cursor::new(prefix);
    let mut magic = [0u8; 8];
    std::io::Read::read_exact(&mut cur, &mut magic)?;
    if magic != MAGIC {
        return Err(ArchiveFormatError::BadMagic);
    }
    let header_len = read_io_varint(&mut cur)?;
    if header_len > MAX_FILE_HEADER_BYTES {
        return Err(ArchiveFormatError::SectionTooLarge {
            section: "header",
            bytes: header_len,
            limit: MAX_FILE_HEADER_BYTES,
        });
    }
    let header_len = usize::try_from(header_len).map_err(|_| {
        ArchiveFormatError::InvalidContainerLayout("header length does not fit usize")
    })?;
    let mut header_bytes = Vec::new();
    try_reserve_archive_buffer(&mut header_bytes, header_len, "file header")?;
    header_bytes.resize(header_len, 0);
    std::io::Read::read_exact(&mut cur, &mut header_bytes)?;
    let header = decode_file_header_bytes(&header_bytes)?;
    validate_archive_version(header.format_version, header.flags)?;
    if header.bucket_slots == 0 {
        return Err(ArchiveFormatError::InvalidContainerLayout(
            "bucket_slots must be nonzero",
        ));
    }
    header.slot_start.checked_add(header.slot_count).ok_or(
        ArchiveFormatError::InvalidContainerLayout("slot range overflows u64"),
    )?;
    if header.prime_table_id != *PRIME_TABLE_ID {
        return Err(ArchiveFormatError::PrimeTableMismatch {
            file: header.prime_table_id,
            compiled: *PRIME_TABLE_ID,
        });
    }
    Ok((header, cur.position() as usize))
}

/// Parses the bucket index from its raw bytes, validating the checksum
/// recorded in `footer`. The bytes are `[footer.index_offset ..
/// footer.index_offset + footer.index_len]`. Used by network drivers that
/// fetch the index via a range request after reading the footer.
pub fn parse_bucket_index(
    index_bytes: &[u8],
    footer: &Footer,
) -> Result<Vec<BucketIndexEntry>, ArchiveFormatError> {
    if footer.index_len > MAX_BUCKET_INDEX_BYTES {
        return Err(ArchiveFormatError::SectionTooLarge {
            section: "bucket index",
            bytes: footer.index_len,
            limit: MAX_BUCKET_INDEX_BYTES,
        });
    }
    if index_bytes.len() as u64 != footer.index_len {
        return Err(ArchiveFormatError::InvalidContainerLayout(
            "bucket index length disagrees with footer",
        ));
    }
    if xxh64(index_bytes, 0) != footer.index_xxh64 {
        return Err(ArchiveFormatError::IndexChecksum);
    }
    let mut cur = lencode::io::Cursor::new(index_bytes);
    let count = u64::decode_ext(&mut cur, None)?;
    if count != footer.bucket_count {
        return Err(ArchiveFormatError::InvalidContainerLayout(
            "bucket count disagrees with footer",
        ));
    }
    if count > MAX_BUCKET_INDEX_ENTRIES || count > index_bytes.len() as u64 {
        return Err(ArchiveFormatError::SectionTooLarge {
            section: "bucket count",
            bytes: count,
            limit: MAX_BUCKET_INDEX_ENTRIES,
        });
    }
    let count = usize::try_from(count).map_err(|_| {
        ArchiveFormatError::InvalidContainerLayout("bucket count does not fit usize")
    })?;
    let mut index = Vec::new();
    index
        .try_reserve_exact(count)
        .map_err(|_| ArchiveFormatError::AllocationFailed {
            section: "bucket index entries",
            bytes: count
                .checked_mul(core::mem::size_of::<BucketIndexEntry>())
                .map_or(u64::MAX, |bytes| bytes as u64),
        })?;
    for _ in 0..count {
        index.push(BucketIndexEntry::decode_ext(&mut cur, None)?);
    }
    if cur.position() != index_bytes.len() {
        return Err(ArchiveFormatError::InvalidContainerLayout(
            "bucket index has trailing bytes",
        ));
    }
    Ok(index)
}

/// Validates footer offsets before allocating or fetching the index.
pub fn validate_footer_layout(
    file_len: u64,
    header_end: u64,
    footer: &Footer,
) -> Result<(), ArchiveFormatError> {
    if footer.index_len > MAX_BUCKET_INDEX_BYTES {
        return Err(ArchiveFormatError::SectionTooLarge {
            section: "bucket index",
            bytes: footer.index_len,
            limit: MAX_BUCKET_INDEX_BYTES,
        });
    }
    let footer_start = file_len.checked_sub(FOOTER_LEN as u64).ok_or(
        ArchiveFormatError::InvalidContainerLayout("file is shorter than footer"),
    )?;
    let index_end = footer.index_offset.checked_add(footer.index_len).ok_or(
        ArchiveFormatError::InvalidContainerLayout("bucket index range overflows u64"),
    )?;
    if footer.index_offset < header_end || index_end != footer_start {
        return Err(ArchiveFormatError::InvalidContainerLayout(
            "bucket index is not between buckets and footer",
        ));
    }
    Ok(())
}

/// Validates that bucket frames occupy the exact region between the file
/// header and index and that their declared slot windows are ordered.
pub fn validate_bucket_index_layout(
    header: &FileHeader,
    header_end: u64,
    index_offset: u64,
    index: &[BucketIndexEntry],
) -> Result<(), ArchiveFormatError> {
    let slot_end = header.slot_start.checked_add(header.slot_count).ok_or(
        ArchiveFormatError::InvalidContainerLayout("slot range overflows u64"),
    )?;
    let mut expected_offset = header_end;
    let mut previous_slot = None;
    for entry in index {
        if entry.len > MAX_BUCKET_STORED_BYTES {
            return Err(ArchiveFormatError::SectionTooLarge {
                section: "bucket frame",
                bytes: entry.len,
                limit: MAX_BUCKET_STORED_BYTES,
            });
        }
        if entry.offset != expected_offset || entry.len == 0 {
            return Err(ArchiveFormatError::InvalidContainerLayout(
                "bucket frames are not contiguous",
            ));
        }
        expected_offset = entry.offset.checked_add(entry.len).ok_or(
            ArchiveFormatError::InvalidContainerLayout("bucket range overflows u64"),
        )?;
        if entry.first_slot < header.slot_start
            || entry.first_slot >= slot_end
            || !(entry.first_slot - header.slot_start)
                .is_multiple_of(u64::from(header.bucket_slots))
            || previous_slot.is_some_and(|previous| entry.first_slot <= previous)
        {
            return Err(ArchiveFormatError::InvalidContainerLayout(
                "bucket slot indexes are invalid",
            ));
        }
        previous_slot = Some(entry.first_slot);
    }
    if expected_offset != index_offset {
        return Err(ArchiveFormatError::InvalidContainerLayout(
            "bucket frames do not end at the index",
        ));
    }
    Ok(())
}

/// Index position of the bucket whose slot window contains `slot`.
///
/// Buckets are aligned to `header.slot_start` in fixed `header.bucket_slots`
/// windows, so the position is computed directly (no search). For sparse
/// files the computed position may overshoot; we walk back to the last
/// bucket whose `first_slot <= slot`, with zero iterations for dense archives.
/// `index` must be non-empty.
pub fn bucket_containing(header: &FileHeader, index: &[BucketIndexEntry], slot: u64) -> usize {
    let id = (slot.saturating_sub(header.slot_start) / header.bucket_slots as u64) as usize;
    let mut i = id.min(index.len().saturating_sub(1));
    while i > 0 && index[i].first_slot > slot {
        i -= 1;
    }
    i
}

/// Forces the notification scratch into the `Skipped` variant in place
/// (no ~40 MiB stack temporary) and sets the slot.
fn set_notification_skipped(scratch: &mut BlockNotification, slot: u64) {
    if !matches!(scratch, BlockNotification::Skipped(_)) {
        // SAFETY: `#[repr(C, u8)]` pins the discriminant at byte 0; zeroed
        // storage = Skipped(slot 0), a valid value.
        unsafe {
            core::ptr::drop_in_place(scratch as *mut BlockNotification);
            core::ptr::write_bytes(scratch as *mut BlockNotification, 0, 1);
        }
    }
    match scratch {
        BlockNotification::Skipped(s) => s.slot = slot,
        _ => unreachable!(),
    }
}

/// Forces the notification scratch into the `Block` variant in place and
/// returns a mutable reference to its `BlockMeta`.
fn set_notification_block(scratch: &mut BlockNotification) -> &mut BlockMeta {
    if !matches!(scratch, BlockNotification::Block(_)) {
        // SAFETY: as above; zero the storage then flip the discriminant to
        // 1 (Block): an all-zero BlockMeta payload is valid.
        unsafe {
            core::ptr::drop_in_place(scratch as *mut BlockNotification);
            core::ptr::write_bytes(scratch as *mut BlockNotification, 0, 1);
            *(scratch as *mut BlockNotification as *mut u8) = 1;
        }
    }
    match scratch {
        BlockNotification::Block(m) => m,
        _ => unreachable!(),
    }
}

/// Decodes one account-update record (metadata via the dedupe context,
/// data blob via the diff decoder) and hands it to `store` as a borrowed
/// view. Exact mirror of the writer's `encode_update_record`.
///
/// The metadata decode is unconditional: pubkey/owner flow through the
/// shared dedupe context, whose scratch table assigns positional IDs that
/// later records in the bucket (including transaction fields) reference;
/// skipping them would desynchronize the whole bucket. When `materialize`
/// is `false` only the data blob is skipped: its framing is parsed to
/// advance the cursor over exactly the bytes `DiffDecoder::decode_blob`
/// would consume, no reconstruction happens, the diff store stays empty,
/// and `store` receives the view with an empty `data` slice.
fn decode_update_record_into(
    reader: &mut impl Read,
    ctx: &mut DecoderContext,
    diff: &mut DiffDecoder,
    materialize: bool,
    store: impl FnOnce(&AccountUpdateView<'_>) -> Result<(), PushAccountUpdateError>,
) -> Result<usize, ArchiveFormatError> {
    let pubkey = Address::decode_ext(reader, Some(ctx))?;
    let lamports = u64::decode_ext(reader, Some(ctx))?;
    let owner = Address::decode_ext(reader, Some(ctx))?;
    let executable = bool::decode_ext(reader, Some(ctx))?;
    let rent_epoch = u64::decode_ext(reader, Some(ctx))?;
    let write_version = u64::decode_ext(reader, Some(ctx))?;
    let (data_slice, data_len): (&[u8], usize) = if materialize {
        diff.set_key(account_diff_key(&pubkey));
        // Borrowed decode (lencode 1.2): the reconstruction stays in the
        // diff store's slot and is copied exactly once by `store`, into
        // the arena. No per-update allocation.
        let data = diff.decode_blob_ref(reader)?;
        (data, data.len())
    } else {
        let data_len = skip_diff_blob_with_max_mode(reader, diff.max_supported_mode())?;
        (&[], data_len)
    };
    store(&AccountUpdateView {
        pubkey,
        lamports,
        owner,
        executable,
        rent_epoch,
        write_version,
        data: data_slice,
    })
    .map_err(|_| ArchiveFormatError::Encode(lencode::io::Error::InvalidData))?;
    Ok(data_len)
}

fn enforce_phase_count(
    section: &'static str,
    count: usize,
    limit: usize,
) -> Result<(), ArchiveFormatError> {
    if count > limit {
        Err(ArchiveFormatError::SectionTooLarge {
            section,
            bytes: count as u64,
            limit: limit as u64,
        })
    } else {
        Ok(())
    }
}

fn add_phase_data(
    section: &'static str,
    total: &mut usize,
    bytes: usize,
    limit: usize,
) -> Result<(), ArchiveFormatError> {
    let next = total
        .checked_add(bytes)
        .ok_or(ArchiveFormatError::SectionTooLarge {
            section,
            bytes: u64::MAX,
            limit: limit as u64,
        })?;
    if next > limit {
        return Err(ArchiveFormatError::SectionTooLarge {
            section,
            bytes: next as u64,
            limit: limit as u64,
        });
    }
    *total = next;
    Ok(())
}

fn decode_bounded_count(
    reader: &mut impl Read,
    element_size: usize,
) -> Result<usize, ArchiveFormatError> {
    let count = usize::try_from(u64::decode_ext(reader, None)?)
        .map_err(|_| ArchiveFormatError::Encode(lencode::io::Error::DecodeLimitExceeded))?;
    reader.claim_sequence(count, element_size)?;
    Ok(count)
}

/// Advances `reader` over one diff-encoded blob without reconstructing it.
/// Framing and hostile-input validation live in lencode so new diff modes do
/// not require Horizon to maintain a byte-level mirror of the codec.
#[cfg(test)]
pub(crate) fn skip_diff_blob(reader: &mut impl Read) -> Result<(), ArchiveFormatError> {
    skip_diff_blob_with_max_mode(reader, 3).map(|_| ())
}

fn skip_diff_blob_with_max_mode(
    reader: &mut impl Read,
    max_mode: u8,
) -> Result<usize, ArchiveFormatError> {
    lencode::diff::skip_diff_blob_frame_with_max_mode_and_len(reader, max_mode)
        .map_err(ArchiveFormatError::Encode)
}

/// Decodes one transaction record into `scratch`. Exact mirror of
/// [`ArchiveWriter::write_transaction`](super::ArchiveWriter::write_transaction).
/// Returns the byte span consumed by the transaction's own account updates, so
/// the caller can split transaction-field bytes from account-update bytes.
fn read_tx_record<R: Read>(
    cur: &mut LimitedReader<R>,
    scratch: &mut Transaction,
    ctx: &mut DecoderContext,
    diff: &mut DiffDecoder,
    materialize: bool,
    max_account_updates: usize,
    max_account_data_bytes: usize,
) -> Result<usize, ArchiveFormatError> {
    scratch.clear();

    scratch.signatures.decode_into(cur, Some(ctx))?;
    scratch.message.decode_into(cur, Some(ctx))?;
    scratch.status = TransactionStatus::decode_ext(cur, Some(ctx))?;
    scratch.fee = u64::decode_ext(cur, Some(ctx))?;
    scratch.pre_balances.decode_into(cur, Some(ctx))?;
    scratch.post_balances.decode_into(cur, Some(ctx))?;
    scratch
        .loaded_writable_addresses
        .decode_into(cur, Some(ctx))?;
    scratch
        .loaded_readonly_addresses
        .decode_into(cur, Some(ctx))?;
    decode_option_zerovec_into(&mut scratch.inner_instructions, cur, Some(ctx))?;
    decode_option_log_messages_into(&mut scratch.log_messages, cur, Some(ctx))?;
    decode_option_zerovec_into(&mut scratch.pre_token_balances, cur, Some(ctx))?;
    decode_option_zerovec_into(&mut scratch.post_token_balances, cur, Some(ctx))?;
    decode_option_zerovec_into(&mut scratch.rewards, cur, Some(ctx))?;
    scratch.return_data = Option::decode_ext(cur, Some(ctx))?;
    scratch.compute_units_consumed = Option::decode_ext(cur, Some(ctx))?;
    scratch.cost_units = Option::decode_ext(cur, Some(ctx))?;

    let au_count = decode_bounded_count(cur, 1)?;
    enforce_phase_count("transaction account updates", au_count, max_account_updates)?;
    let au_start = cur.consumed();
    let mut account_data_bytes = 0usize;
    for _ in 0..au_count {
        let data_len = decode_update_record_into(cur, ctx, diff, materialize, |view| {
            scratch.push_account_update(view)
        })?;
        add_phase_data(
            "transaction account-update data",
            &mut account_data_bytes,
            data_len,
            max_account_data_bytes,
        )?;
    }
    Ok(cur.consumed() - au_start)
}

/// Reads a lencode varint directly from a `std::io::Read` stream.
pub(crate) fn read_io_varint(r: &mut impl std::io::Read) -> Result<u64, ArchiveFormatError> {
    let mut first = [0u8; 1];
    r.read_exact(&mut first)?;
    if first[0] & 0x80 == 0 {
        return Ok(first[0] as u64);
    }
    let n = (first[0] & 0x7F) as usize;
    if n == 0 || n > 8 {
        return Err(ArchiveFormatError::Encode(lencode::io::Error::InvalidData));
    }
    let mut bytes = [0u8; 8];
    r.read_exact(&mut bytes[..n])?;
    if bytes[n - 1] == 0 || (n == 1 && bytes[0] <= 0x7F) {
        return Err(ArchiveFormatError::Encode(lencode::io::Error::InvalidData));
    }
    Ok(u64::from_le_bytes(bytes))
}
