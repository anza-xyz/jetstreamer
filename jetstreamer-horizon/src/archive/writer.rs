//! Streaming writer for the horizon archive container format.
//!
//! Writes strictly forward (no seeks), buffering one bucket at a time so
//! the payload can be checksummed and optionally zstd/LZ4-compressed before it
//! hits the sink. All stateful encoders (pubkey dedupe scratch, account
//! diff store) reset at bucket boundaries; see the [module docs](super).
use lencode::context::EncoderContext;
use lencode::dedupe::DedupeIdCodec;
use lencode::diff::{DiffEncoder, DiffPolicy};
use lencode::prelude::*;
use solana_hash::Hash;
use xxhash_rust::xxh64::xxh64;

use crate::account_updates::AccountUpdateView;
use crate::dedupe::{new_encoder_context_with_codec, reset_encoder};
use crate::limits::{
    MAX_ACCOUNT_DATA_LEN, MAX_SLOT_POST_UPDATE_DATA, MAX_SLOT_POST_UPDATES,
    MAX_SLOT_PRE_UPDATE_DATA, MAX_SLOT_PRE_UPDATES,
};
use crate::transactions::Transaction;

use super::bucket::{
    MAX_BUCKET_DIFF_CACHE_BYTES, MAX_BUCKET_DIFF_CACHE_KEYS, MAX_BUCKET_STORED_BYTES,
    MAX_BUCKET_UNCOMPRESSED_BYTES, MAX_FRAME_SEQUENCE_ELEMENTS,
};
use super::format::*;
use super::provenance::{ArchiveProvenance, encode_archive_provenance};

#[derive(Debug, Clone, Copy)]
enum BufferGrowthFailure {
    Limit { attempted: usize },
    Allocation { attempted: usize },
}

/// Fallible, hard-bounded adapter over one of the writer's retained Vecs.
/// Lencode's ordinary `Vec<u8>` sink grows infallibly; archive creation uses
/// this adapter so untrusted re-encoding input cannot allocate or append past
/// the bucket budget before the eventual flush check.
struct BoundedVecWriter<'a> {
    buffer: &'a mut Vec<u8>,
    max_len: usize,
    failure: Option<BufferGrowthFailure>,
}

impl<'a> BoundedVecWriter<'a> {
    fn new(buffer: &'a mut Vec<u8>, max_len: usize) -> Self {
        Self {
            buffer,
            max_len,
            failure: None,
        }
    }
}

impl lencode::io::Write for BoundedVecWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> lencode::Result<usize> {
        let Some(new_len) = self.buffer.len().checked_add(bytes.len()) else {
            self.failure = Some(BufferGrowthFailure::Limit {
                attempted: usize::MAX,
            });
            return Err(lencode::io::Error::WriterOutOfSpace);
        };
        if new_len > self.max_len {
            self.failure = Some(BufferGrowthFailure::Limit { attempted: new_len });
            return Err(lencode::io::Error::WriterOutOfSpace);
        }
        if self.buffer.capacity() - self.buffer.len() < bytes.len()
            && self.buffer.try_reserve_exact(bytes.len()).is_err()
        {
            self.failure = Some(BufferGrowthFailure::Allocation { attempted: new_len });
            return Err(lencode::io::Error::WriterOutOfSpace);
        }
        self.buffer.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> lencode::Result<()> {
        Ok(())
    }
}

fn encode_bounded<T>(
    buffer: &mut Vec<u8>,
    max_len: usize,
    first_slot: u64,
    section: &'static str,
    encode: impl FnOnce(&mut BoundedVecWriter<'_>) -> lencode::Result<T>,
) -> Result<T, ArchiveFormatError> {
    let mut writer = BoundedVecWriter::new(buffer, max_len);
    match encode(&mut writer) {
        Ok(value) => Ok(value),
        Err(error) => match writer.failure {
            Some(BufferGrowthFailure::Limit { attempted }) => {
                Err(ArchiveFormatError::BucketTooLarge {
                    first_slot,
                    bytes: attempted as u64,
                })
            }
            Some(BufferGrowthFailure::Allocation { attempted }) => {
                Err(ArchiveFormatError::AllocationFailed {
                    section,
                    bytes: attempted as u64,
                })
            }
            None => Err(ArchiveFormatError::Encode(error)),
        },
    }
}

fn allocate_codec_output(
    bound: usize,
    first_slot: u64,
    section: &'static str,
) -> Result<Vec<u8>, ArchiveFormatError> {
    if bound as u64 > MAX_BUCKET_STORED_BYTES {
        return Err(ArchiveFormatError::BucketTooLarge {
            first_slot,
            bytes: bound as u64,
        });
    }
    let mut output = Vec::new();
    output
        .try_reserve_exact(bound)
        .map_err(|_| ArchiveFormatError::AllocationFailed {
            section,
            bytes: bound as u64,
        })?;
    Ok(output)
}

fn compress_zstd_bounded(
    input: &[u8],
    level: i32,
    first_slot: u64,
) -> Result<Vec<u8>, ArchiveFormatError> {
    let bound = zstd::zstd_safe::compress_bound(input.len());
    let mut output = allocate_codec_output(bound, first_slot, "zstd bucket output")?;
    let mut compressor = zstd::bulk::Compressor::new(level).map_err(ArchiveFormatError::Io)?;
    compressor
        .compress_to_buffer(input, &mut output)
        .map_err(ArchiveFormatError::Io)?;
    Ok(output)
}

fn compress_lz4_bounded(input: &[u8], first_slot: u64) -> Result<Vec<u8>, ArchiveFormatError> {
    let bound = lz4::block::compress_bound(input.len()).map_err(ArchiveFormatError::Io)?;
    let mut output = allocate_codec_output(bound, first_slot, "LZ4 bucket output")?;
    // `compress_to_buffer` accepts an initialized Rust slice, so expose no
    // uninitialized spare capacity even though the C codec overwrites it.
    // The fallible reserve above makes this resize allocation-free.
    output.resize(bound, 0);
    let written = lz4::block::compress_to_buffer(
        input,
        Some(lz4::block::CompressionMode::FAST(1)),
        false,
        &mut output,
    )
    .map_err(ArchiveFormatError::Io)?;
    output.truncate(written);
    Ok(output)
}

/// Encodes one account-update record (transaction-owned or orphan): the
/// metadata fields through the dedupe context, then the data blob through
/// the per-account diff encoder. Single wire shape shared by every update
/// section in a slot frame.
fn encode_update_record(
    view: &AccountUpdateView<'_>,
    buf: &mut impl lencode::io::Write,
    ctx: &mut EncoderContext,
    diff: &mut DiffEncoder,
) -> lencode::Result<()> {
    view.pubkey.encode_ext(buf, Some(ctx))?;
    view.lamports.encode_ext(buf, Some(ctx))?;
    view.owner.encode_ext(buf, Some(ctx))?;
    view.executable.encode_ext(buf, Some(ctx))?;
    view.rent_epoch.encode_ext(buf, Some(ctx))?;
    view.write_version.encode_ext(buf, Some(ctx))?;
    diff.set_key(account_diff_key(&view.pubkey));
    diff.encode_blob(view.data, buf)?;
    Ok(())
}

/// Encodes a block's metadata scalars + rewards (everything except the
/// orphan-update arenas, which travel as their own frame sections, and the
/// slot, which the frame header carries).
fn encode_block_meta_fields(
    meta: &BlockMeta,
    buf: &mut impl lencode::io::Write,
) -> lencode::Result<()> {
    meta.parent_slot.encode_ext(buf, None)?;
    meta.parent_blockhash.encode_ext(buf, None)?;
    meta.blockhash.encode_ext(buf, None)?;
    meta.block_time.encode_ext(buf, None)?;
    meta.block_height.encode_ext(buf, None)?;
    meta.executed_transaction_count.encode_ext(buf, None)?;
    meta.entry_count.encode_ext(buf, None)?;
    meta.rewards.encode_ext(buf, None)?;
    meta.num_partitions.encode_ext(buf, None)?;
    Ok(())
}

/// Configuration for [`ArchiveWriter`].
#[derive(Debug, Clone)]
pub struct ArchiveWriterConfig {
    /// Destination wire version. V1 remains available for same-codec controls;
    /// historical writer releases may not reproduce byte-identical payloads.
    /// New archives default to V2.
    pub format: ArchiveVersion,
    /// Slots per bucket (encoder reset / seek granularity). `1` disables
    /// cross-slot diff compression; [`DEFAULT_BUCKET_SLOTS`] balances
    /// compression and seek latency.
    pub bucket_slots: u16,
    /// Bucket payload compression.
    pub compression: Compression,
    /// Account-data diff strategy. V2 defaults to raw XOR deltas so the
    /// outer bucket compressor sees cross-record redundancy instead of
    /// receiving thousands of independently compressed inner frames.
    pub diff_policy: DiffPolicy,
    /// Zstd level when `compression == Zstd`. Level 9 is the measured
    /// archival-size knee for Horizon data.
    pub zstd_level: i32,
}

impl Default for ArchiveWriterConfig {
    fn default() -> Self {
        Self {
            format: ArchiveVersion::V2,
            bucket_slots: DEFAULT_BUCKET_SLOTS,
            compression: Compression::Zstd,
            diff_policy: DiffPolicy::OuterCompressed,
            zstd_level: 9,
        }
    }
}

/// Aggregate counters reported by [`ArchiveWriter::finish`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ArchiveStats {
    /// Total bytes written to the sink (header + buckets + index + footer).
    pub bytes_written: u64,
    /// Sum of uncompressed bucket payload bytes (pre-zstd).
    pub uncompressed_payload_bytes: u64,
    /// Number of buckets flushed.
    pub buckets: u64,
    /// Sum of bucket header + stored payload bytes. This excludes the file
    /// header, bucket index, and footer so re-encoders can compare selected
    /// source and destination buckets without container-overhead noise.
    pub bucket_bytes_written: u64,
    /// Number of slot frames written (blocks + skipped).
    pub slots: u64,
    /// Number of non-skipped block frames.
    pub blocks: u64,
    /// Number of transactions written.
    pub transactions: u64,
    /// Number of transaction-owned account updates written.
    pub account_updates: u64,
    /// Number of runtime-direct ("orphan") account updates written:
    /// block pre/post sections plus epoch-attributed updates.
    pub orphan_account_updates: u64,
    /// Number of epoch notifications written.
    pub epochs: u64,
    /// Sum of raw account-update data bytes presented to the diff encoder.
    pub account_data_bytes_in: u64,
}

/// Streaming archive writer over any [`std::io::Write`] sink.
///
/// # Usage
///
/// ```ignore
/// let mut w = ArchiveWriter::new(file, 900, slot_start, 432_000, Default::default())?;
/// for slot in slots {
///     match slot {
///         Replayed::Skipped(s) => w.write_skipped_slot(s)?,
///         Replayed::Block(b) => {
///             w.begin_slot(b.slot)?;
///             for tx in b.transactions {        // scratch-populated horizon Transactions
///                 w.write_transaction(&tx)?;
///             }
///             w.end_slot(&b.meta, &b.entries)?;
///         }
///     }
/// }
/// let (file, stats) = w.finish()?;
/// ```
pub struct ArchiveWriter<W: std::io::Write> {
    sink: W,
    config: ArchiveWriterConfig,
    slot_start: u64,
    slot_end: u64,
    file_offset: u64,
    index: Vec<BucketIndexEntry>,
    stats: ArchiveStats,

    // --- bucket state (reset per bucket) ---
    bucket_buf: Vec<u8>,
    bucket_first_slot: Option<u64>,
    bucket_slot_count: u32,
    bucket_poh_anchor: Hash,
    enc_ctx: EncoderContext,
    diff: DiffEncoder,

    // --- running chain state ---
    last_blockhash: Hash,
    last_slot: Option<u64>,

    // --- per-slot staging ---
    //
    // Four section buffers staged in arrival order: epoch, pre-orphans,
    // transactions, then post-orphans. They share the bucket's dedupe/diff encoder
    // state, so arrival order *is* wire order. The decoder replays the
    // same sequence.
    staging_slot: Option<u64>,
    staging_has_epoch: bool,
    staging_epoch_bytes: Vec<u8>,
    staging_pre_count: u32,
    staging_pre_data_bytes: usize,
    staging_pre_bytes: Vec<u8>,
    staging_tx_count: u32,
    staging_tx_bytes: Vec<u8>,
    staging_post_count: u32,
    staging_post_data_bytes: usize,
    staging_post_bytes: Vec<u8>,
    /// A failed codec write may leave section bytes and dedupe state partially
    /// advanced. Fail closed instead of allowing a caller to continue from an
    /// ambiguous state.
    poisoned: bool,
    #[cfg(test)]
    bucket_limit_bytes: usize,
    #[cfg(test)]
    frame_sequence_limit: usize,
}

impl<W: std::io::Write> ArchiveWriter<W> {
    /// Creates a writer and emits the magic + file header to `sink`.
    pub fn new(
        sink: W,
        epoch: u64,
        slot_start: u64,
        slot_count: u64,
        config: ArchiveWriterConfig,
    ) -> Result<Self, ArchiveFormatError> {
        Self::new_with_reserved(sink, epoch, slot_start, slot_count, config, Vec::new())
    }

    /// Creates a writer whose file header carries typed generation
    /// provenance. Existing callers can continue to use [`Self::new`], which
    /// emits the legacy empty `ArchiveMeta::reserved` field.
    pub fn new_with_provenance(
        sink: W,
        epoch: u64,
        slot_start: u64,
        slot_count: u64,
        config: ArchiveWriterConfig,
        provenance: &ArchiveProvenance,
    ) -> Result<Self, ArchiveFormatError> {
        provenance.validate_for_archive(slot_start, slot_count)?;
        let reserved = encode_archive_provenance(provenance)?;
        Self::new_with_reserved(sink, epoch, slot_start, slot_count, config, reserved)
    }

    /// Internal constructor used when a semantic-preserving transformation
    /// has already validated opaque header metadata and must retain its exact
    /// bytes instead of decoding and re-encoding them.
    pub(crate) fn new_with_reserved(
        mut sink: W,
        epoch: u64,
        slot_start: u64,
        slot_count: u64,
        config: ArchiveWriterConfig,
        reserved: Vec<u8>,
    ) -> Result<Self, ArchiveFormatError> {
        if config.bucket_slots == 0 {
            return Err(ArchiveFormatError::InvalidContainerLayout(
                "bucket_slots must be nonzero",
            ));
        }
        let format = config.format;
        let diff_policy = config.diff_policy;
        if format == ArchiveVersion::V1 && config.compression == Compression::Lz4 {
            return Err(ArchiveFormatError::UnsupportedCompressionForVersion {
                version: format.as_u16(),
                compression: config.compression,
            });
        }
        if format == ArchiveVersion::V1 && config.diff_policy == DiffPolicy::OuterCompressed {
            return Err(ArchiveFormatError::UnsupportedDiffPolicyForVersion {
                version: format.as_u16(),
                policy: config.diff_policy,
            });
        }
        let slot_end = slot_start.checked_add(slot_count).ok_or(
            ArchiveFormatError::InvalidContainerLayout("slot range overflows u64"),
        )?;
        let header = FileHeader {
            format_version: format.as_u16(),
            bucket_slots: config.bucket_slots,
            epoch,
            slot_start,
            slot_count,
            prime_table_id: *PRIME_TABLE_ID,
            flags: format.required_flags(),
            meta: ArchiveMeta {
                created_unix_ms: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_millis() as u64)
                    .unwrap_or(0),
                writer_version: env!("CARGO_PKG_VERSION").as_bytes().to_vec(),
                reserved,
            },
        };

        // MAGIC ++ varint(len) ++ lencode(header)
        let mut header_bytes: Vec<u8> = Vec::with_capacity(256);
        header.encode_ext(&mut header_bytes, None)?;
        sink.write_all(&MAGIC)?;
        let mut len_prefix: Vec<u8> = Vec::with_capacity(4);
        (header_bytes.len() as u64).encode_ext(&mut len_prefix, None)?;
        sink.write_all(&len_prefix)?;
        sink.write_all(&header_bytes)?;
        let file_offset = (MAGIC.len() + len_prefix.len() + header_bytes.len()) as u64;

        Ok(Self {
            sink,
            slot_start,
            slot_end,
            config,
            file_offset,
            index: Vec::with_capacity(4096),
            stats: ArchiveStats {
                bytes_written: file_offset,
                ..Default::default()
            },
            bucket_buf: Vec::with_capacity(8 << 20),
            bucket_first_slot: None,
            bucket_slot_count: 0,
            bucket_poh_anchor: Hash::default(),
            enc_ctx: new_encoder_context_with_codec(match format {
                ArchiveVersion::V1 => DedupeIdCodec::Lencode,
                ArchiveVersion::V2 => DedupeIdCodec::UnsignedLeb128,
            }),
            diff: DiffEncoder::with_capacity_policy_and_cache_limits(
                64 * 1024,
                diff_policy,
                MAX_BUCKET_DIFF_CACHE_BYTES,
                MAX_BUCKET_DIFF_CACHE_KEYS,
            ),
            last_blockhash: Hash::default(),
            last_slot: None,
            staging_slot: None,
            staging_has_epoch: false,
            staging_epoch_bytes: Vec::with_capacity(64 << 10),
            staging_pre_count: 0,
            staging_pre_data_bytes: 0,
            staging_pre_bytes: Vec::with_capacity(1 << 20),
            staging_tx_count: 0,
            staging_tx_bytes: Vec::with_capacity(4 << 20),
            staging_post_count: 0,
            staging_post_data_bytes: 0,
            staging_post_bytes: Vec::with_capacity(256 << 10),
            poisoned: false,
            #[cfg(test)]
            bucket_limit_bytes: MAX_BUCKET_UNCOMPRESSED_BYTES as usize,
            #[cfg(test)]
            frame_sequence_limit: MAX_FRAME_SEQUENCE_ELEMENTS,
        })
    }

    /// Returns the bucket ordinal for a slot, relative to `slot_start`.
    #[inline]
    fn bucket_id(&self, slot: u64) -> u64 {
        (slot - self.slot_start) / self.config.bucket_slots as u64
    }

    fn staged_payload_len(&self) -> Result<usize, ArchiveFormatError> {
        let total = [
            self.bucket_buf.len(),
            self.staging_epoch_bytes.len(),
            self.staging_pre_bytes.len(),
            self.staging_tx_bytes.len(),
            self.staging_post_bytes.len(),
        ]
        .into_iter()
        .try_fold(0usize, |total, bytes| total.checked_add(bytes))
        .ok_or(ArchiveFormatError::BucketTooLarge {
            first_slot: self
                .bucket_first_slot
                .or(self.staging_slot)
                .unwrap_or(self.slot_start),
            bytes: u64::MAX,
        })?;
        if total > self.bucket_limit_bytes() {
            return Err(ArchiveFormatError::BucketTooLarge {
                first_slot: self
                    .bucket_first_slot
                    .or(self.staging_slot)
                    .unwrap_or(self.slot_start),
                bytes: total as u64,
            });
        }
        Ok(total)
    }

    /// Maximum length one retained section may grow to while keeping the sum
    /// of the bucket and all open-slot staging buffers inside the raw bucket
    /// ceiling.
    fn section_max_len(&self, current_len: usize) -> Result<usize, ArchiveFormatError> {
        let total = self.staged_payload_len()?;
        let other =
            total
                .checked_sub(current_len)
                .ok_or(ArchiveFormatError::InvalidContainerLayout(
                    "writer staging length accounting failed",
                ))?;
        self.bucket_limit_bytes()
            .checked_sub(other)
            .ok_or(ArchiveFormatError::BucketTooLarge {
                first_slot: self
                    .bucket_first_slot
                    .or(self.staging_slot)
                    .unwrap_or(self.slot_start),
                bytes: total as u64,
            })
    }

    fn current_bucket_first_slot(&self) -> u64 {
        self.bucket_first_slot
            .or(self.staging_slot)
            .unwrap_or(self.slot_start)
    }

    fn ensure_writable(&self) -> Result<(), ArchiveFormatError> {
        if self.poisoned {
            Err(ArchiveFormatError::InvalidContainerLayout(
                "archive writer is unusable after an earlier encode failure",
            ))
        } else {
            Ok(())
        }
    }

    fn bucket_limit_bytes(&self) -> usize {
        #[cfg(test)]
        {
            self.bucket_limit_bytes
        }
        #[cfg(not(test))]
        {
            MAX_BUCKET_UNCOMPRESSED_BYTES as usize
        }
    }

    fn frame_sequence_limit(&self) -> usize {
        #[cfg(test)]
        {
            self.frame_sequence_limit
        }
        #[cfg(not(test))]
        {
            MAX_FRAME_SEQUENCE_ELEMENTS
        }
    }

    fn validate_frame_sequence_count(
        &self,
        section: &'static str,
        count: u64,
    ) -> Result<(), ArchiveFormatError> {
        let limit = self.frame_sequence_limit() as u64;
        if count > limit {
            return Err(ArchiveFormatError::SectionTooLarge {
                section,
                bytes: count,
                limit,
            });
        }
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn set_bucket_limit_for_test(&mut self, bytes: usize) {
        assert!(self.bucket_first_slot.is_none());
        assert!(self.staging_slot.is_none());
        self.bucket_limit_bytes = bytes;
        self.trim_empty_payload_capacities();
    }

    #[cfg(test)]
    pub(super) fn set_frame_sequence_limit_for_test(&mut self, elements: usize) {
        self.frame_sequence_limit = elements;
    }

    #[cfg(test)]
    pub(super) fn frame_sequence_limit_for_test(&self) -> usize {
        self.frame_sequence_limit()
    }

    #[cfg(test)]
    pub(super) fn retained_payload_capacity_for_test(&self) -> usize {
        [
            self.bucket_buf.capacity(),
            self.staging_epoch_bytes.capacity(),
            self.staging_pre_bytes.capacity(),
            self.staging_tx_bytes.capacity(),
            self.staging_post_bytes.capacity(),
        ]
        .into_iter()
        .fold(0usize, usize::saturating_add)
    }

    /// Drops the largest empty retained payload buffers until their combined
    /// capacity fits the same ceiling as the logical bucket payload. This
    /// preserves normal steady-state reuse while preventing successive slots
    /// or buckets from leaving one near-ceiling allocation behind in each of
    /// the epoch/pre/transaction/post staging classes.
    fn trim_empty_payload_capacities(&mut self) {
        loop {
            let capacities = [
                self.bucket_buf.capacity(),
                self.staging_epoch_bytes.capacity(),
                self.staging_pre_bytes.capacity(),
                self.staging_tx_bytes.capacity(),
                self.staging_post_bytes.capacity(),
            ];
            let total = capacities.into_iter().fold(0usize, usize::saturating_add);
            if total <= self.bucket_limit_bytes() {
                break;
            }
            let lengths = [
                self.bucket_buf.len(),
                self.staging_epoch_bytes.len(),
                self.staging_pre_bytes.len(),
                self.staging_tx_bytes.len(),
                self.staging_post_bytes.len(),
            ];
            let Some((index, _)) = capacities
                .into_iter()
                .enumerate()
                .filter(|(index, capacity)| lengths[*index] == 0 && *capacity != 0)
                .max_by_key(|(_, capacity)| *capacity)
            else {
                // Every retained allocation contains live payload. Their
                // lengths are independently hard-bounded; any small allocator
                // rounding overhead disappears as soon as one section clears.
                break;
            };
            match index {
                0 => self.bucket_buf = Vec::new(),
                1 => self.staging_epoch_bytes = Vec::new(),
                2 => self.staging_pre_bytes = Vec::new(),
                3 => self.staging_tx_bytes = Vec::new(),
                4 => self.staging_post_bytes = Vec::new(),
                _ => unreachable!(),
            }
        }
    }

    fn check_slot(&mut self, slot: u64) -> Result<(), ArchiveFormatError> {
        if slot < self.slot_start || slot >= self.slot_end {
            return Err(ArchiveFormatError::SlotOutOfRange {
                slot,
                start: self.slot_start,
                end: self.slot_end,
            });
        }
        if let Some(last) = self.last_slot
            && slot <= last
        {
            return Err(ArchiveFormatError::NonMonotonicSlot { got: slot, last });
        }

        let slot_bucket = self.bucket_id(slot);
        let opens_bucket = self
            .bucket_first_slot
            .is_none_or(|first| self.bucket_id(first) != slot_bucket);
        if opens_bucket {
            // Sparse archives may omit whole buckets, but every stored bucket
            // must start on the archive's bucket grid so its index and decoder
            // state agree on the independently decodable boundary.
            let offset = slot - self.slot_start;
            let expected = slot - offset % u64::from(self.config.bucket_slots);
            if slot != expected {
                return Err(ArchiveFormatError::UnexpectedBucketSlot {
                    expected,
                    decoded: slot,
                });
            }
            if self.bucket_first_slot.is_some() {
                self.flush_bucket()?;
            }
        } else if let Some(last) = self.last_slot {
            let expected = last + 1;
            if slot != expected {
                return Err(ArchiveFormatError::UnexpectedBucketSlot {
                    expected,
                    decoded: slot,
                });
            }
        }

        if self.bucket_first_slot.is_none() {
            self.bucket_first_slot = Some(slot);
            self.bucket_poh_anchor = self.last_blockhash;
        }
        self.last_slot = Some(slot);
        Ok(())
    }

    /// Seeds an archive with the blockhash immediately preceding its declared
    /// slot range, before any destination bucket is staged.
    ///
    /// Producers use the parent blockhash of their first observed block (which
    /// also anchors any leading skipped slots). Re-encoders use the first
    /// source bucket's stored anchor. The state checks make late or repeated
    /// changes impossible.
    pub fn preserve_initial_poh_anchor(
        &mut self,
        source_first_slot: u64,
        poh_start_hash: Hash,
    ) -> Result<(), ArchiveFormatError> {
        if source_first_slot != self.slot_start
            || self.last_slot.is_some()
            || self.bucket_first_slot.is_some()
        {
            return Err(ArchiveFormatError::ReencodeBucketLayoutMismatch {
                source_first_slot,
                destination_first_slot: Some(self.slot_start),
            });
        }
        self.last_blockhash = poh_start_hash;
        Ok(())
    }

    /// Replaces the PoH anchor of the bucket currently being staged.
    ///
    /// A sparse re-encoder calls this after replaying one source bucket and
    /// before the next bucket causes a flush. Its previous selected bucket
    /// is not necessarily the source bucket's predecessor, so deriving the
    /// anchor from `last_blockhash` would make the sampled bucket fail
    /// independent chain-continuity checking.
    pub(super) fn preserve_current_bucket_poh_anchor(
        &mut self,
        source_first_slot: u64,
        poh_start_hash: Hash,
    ) -> Result<(), ArchiveFormatError> {
        if self.bucket_first_slot != Some(source_first_slot) {
            return Err(ArchiveFormatError::ReencodeBucketLayoutMismatch {
                source_first_slot,
                destination_first_slot: self.bucket_first_slot,
            });
        }
        self.bucket_poh_anchor = poh_start_hash;
        Ok(())
    }

    /// Records a leader-skipped slot (tiny frame; no block payload).
    pub fn write_skipped_slot(&mut self, slot: u64) -> Result<(), ArchiveFormatError> {
        self.ensure_writable()?;
        assert!(
            self.staging_slot.is_none(),
            "write_skipped_slot called inside begin_slot/end_slot"
        );
        self.check_slot(slot)?;
        let max_len = self.section_max_len(self.bucket_buf.len())?;
        let first_slot = self.current_bucket_first_slot();
        let result = encode_bounded(
            &mut self.bucket_buf,
            max_len,
            first_slot,
            "bucket payload",
            |buf| {
                slot.encode_ext(buf, None)?;
                buf.write(&[SlotKind::Skipped as u8])?;
                Ok(())
            },
        );
        if let Err(error) = result {
            self.poisoned = true;
            return Err(error);
        }
        self.bucket_slot_count += 1;
        self.stats.slots += 1;
        Ok(())
    }

    /// Opens a block frame for `slot`. Follow with (in arrival order): an
    /// optional [`write_epoch_meta`](Self::write_epoch_meta), any number of
    /// [`write_orphan_update`](Self::write_orphan_update) /
    /// [`write_transaction`](Self::write_transaction) calls, then
    /// [`end_slot`](Self::end_slot).
    pub fn begin_slot(&mut self, slot: u64) -> Result<(), ArchiveFormatError> {
        self.ensure_writable()?;
        assert!(
            self.staging_slot.is_none(),
            "begin_slot called twice without end_slot"
        );
        self.check_slot(slot)?;
        self.staging_slot = Some(slot);
        self.staging_has_epoch = false;
        self.staging_epoch_bytes.clear();
        self.staging_pre_count = 0;
        self.staging_pre_data_bytes = 0;
        self.staging_pre_bytes.clear();
        self.staging_tx_count = 0;
        self.staging_tx_bytes.clear();
        self.staging_post_count = 0;
        self.staging_post_data_bytes = 0;
        self.staging_post_bytes.clear();
        self.trim_empty_payload_capacities();
        Ok(())
    }

    /// Records the epoch notification on this slot's frame (the epoch's
    /// first block). Must be called before any orphan update or transaction
    /// of the slot. Epoch transition work precedes everything else in the
    /// bank, and the wire stream preserves that order.
    ///
    /// The meta's scalar fields encode plainly; its `updates` arena goes
    /// through the bucket's dedupe + diff encoders like any other account
    /// updates.
    pub fn write_epoch_meta(&mut self, meta: &EpochMeta) -> Result<(), ArchiveFormatError> {
        self.write_epoch_meta_with_write_version_map(meta, |write_version| write_version)
    }

    /// Re-encodes epoch metadata while mapping its nested account-update
    /// write versions. The multi-segment merger validates the complete raw
    /// sequence before invoking this infallible mapping hook.
    pub(super) fn write_epoch_meta_with_write_version_map(
        &mut self,
        meta: &EpochMeta,
        mut map_write_version: impl FnMut(u64) -> u64,
    ) -> Result<(), ArchiveFormatError> {
        self.write_normalized_epoch_meta_with_write_version_map(
            meta,
            meta.epoch,
            meta.start_slot,
            meta.slot_count,
            meta.first_block_slot,
            &mut map_write_version,
        )
    }

    /// Re-encodes epoch metadata with normalized scalar range fields while
    /// mapping its nested account-update write versions. This avoids copying
    /// the large epoch update arena when assembling segment-local archives.
    pub(super) fn write_normalized_epoch_meta_with_write_version_map(
        &mut self,
        meta: &EpochMeta,
        epoch: u64,
        start_slot: u64,
        slot_count: u64,
        first_block_slot: u64,
        mut map_write_version: impl FnMut(u64) -> u64,
    ) -> Result<(), ArchiveFormatError> {
        self.ensure_writable()?;
        assert!(
            self.staging_slot.is_some(),
            "write_epoch_meta called outside begin_slot/end_slot"
        );
        assert!(
            !self.staging_has_epoch,
            "write_epoch_meta called twice for one slot"
        );
        assert!(
            self.staging_pre_count == 0 && self.staging_tx_count == 0,
            "write_epoch_meta must precede orphan updates and transactions"
        );
        if meta
            .updates
            .iter()
            .any(|(_, data)| data.len() > MAX_ACCOUNT_DATA_LEN)
        {
            return Err(ArchiveFormatError::SectionTooLarge {
                section: "account data",
                bytes: meta
                    .updates
                    .iter()
                    .map(|(_, data)| data.len() as u64)
                    .max()
                    .unwrap_or(0),
                limit: MAX_ACCOUNT_DATA_LEN as u64,
            });
        }
        let max_len = self.section_max_len(self.staging_epoch_bytes.len())?;
        let first_slot = self.current_bucket_first_slot();
        let buf = &mut self.staging_epoch_bytes;
        let ctx = &mut self.enc_ctx;
        let diff = &mut self.diff;
        let result = encode_bounded(buf, max_len, first_slot, "epoch staging", |buf| {
            epoch.encode_ext(buf, None)?;
            start_slot.encode_ext(buf, None)?;
            slot_count.encode_ext(buf, None)?;
            first_block_slot.encode_ext(buf, None)?;
            meta.num_reward_partitions.encode_ext(buf, None)?;
            (meta.updates.len() as u64).encode_ext(buf, None)?;
            for (update, data) in meta.updates.iter() {
                let view = AccountUpdateView {
                    pubkey: update.pubkey,
                    lamports: update.lamports,
                    owner: update.owner,
                    executable: update.executable,
                    rent_epoch: update.rent_epoch,
                    write_version: map_write_version(update.write_version),
                    data,
                };
                encode_update_record(&view, buf, ctx, diff)?;
            }
            Ok(())
        });
        if let Err(error) = result {
            self.poisoned = true;
            return Err(error);
        }
        self.stats.orphan_account_updates += meta.updates.len() as u64;
        self.stats.account_data_bytes_in += meta
            .updates
            .iter()
            .map(|(_, data)| data.len() as u64)
            .sum::<u64>();
        self.staging_has_epoch = true;
        self.trim_empty_payload_capacities();
        self.stats.epochs += 1;
        Ok(())
    }

    /// Records one runtime-direct ("orphan") account update, which is a write the
    /// bank performed with no owning transaction.
    ///
    /// Phase is automatic: updates written before the slot's first
    /// transaction land in the block's pre-transaction group (sysvar
    /// rewrites, epoch-reward credits); updates written after land in the
    /// post-transaction group (fee distribution, incinerator, historical
    /// rent). This matches geyser notification arrival order, so callers
    /// simply forward updates as they arrive.
    pub fn write_orphan_update(
        &mut self,
        view: &AccountUpdateView<'_>,
    ) -> Result<(), ArchiveFormatError> {
        if self.staging_slot.is_none() {
            return Err(ArchiveFormatError::InvalidContainerLayout(
                "write_orphan_update called outside begin_slot/end_slot",
            ));
        }
        let post = self.staging_tx_count != 0 || self.staging_post_count != 0;
        self.write_orphan_update_in_phase(view, post)
    }

    /// Writes a pre-transaction orphan update whose phase was explicit in a
    /// decoded source archive. Unlike [`Self::write_orphan_update`], this does
    /// not infer phase from whether the slot contains any transactions.
    pub(super) fn write_reencoded_pre_update(
        &mut self,
        view: &AccountUpdateView<'_>,
    ) -> Result<(), ArchiveFormatError> {
        if self.staging_slot.is_none() {
            return Err(ArchiveFormatError::InvalidContainerLayout(
                "pre-transaction update called outside begin_slot/end_slot",
            ));
        }
        if self.staging_tx_count != 0 || self.staging_post_count != 0 {
            return Err(ArchiveFormatError::InvalidContainerLayout(
                "pre-transaction update must precede transactions and post-transaction updates",
            ));
        }
        self.write_orphan_update_in_phase(view, false)
    }

    /// Writes a post-transaction orphan update whose phase was explicit in a
    /// decoded source archive. This remains post-transaction even when the
    /// source slot contains zero transactions.
    pub(super) fn write_reencoded_post_update(
        &mut self,
        view: &AccountUpdateView<'_>,
    ) -> Result<(), ArchiveFormatError> {
        if self.staging_slot.is_none() {
            return Err(ArchiveFormatError::InvalidContainerLayout(
                "post-transaction update called outside begin_slot/end_slot",
            ));
        }
        self.write_orphan_update_in_phase(view, true)
    }

    fn write_orphan_update_in_phase(
        &mut self,
        view: &AccountUpdateView<'_>,
        post: bool,
    ) -> Result<(), ArchiveFormatError> {
        self.ensure_writable()?;
        if view.data.len() > MAX_ACCOUNT_DATA_LEN {
            return Err(ArchiveFormatError::SectionTooLarge {
                section: "account data",
                bytes: view.data.len() as u64,
                limit: MAX_ACCOUNT_DATA_LEN as u64,
            });
        }
        let (phase_count, phase_data, count_limit, data_limit, phase_name) = if post {
            (
                self.staging_post_count as usize,
                self.staging_post_data_bytes,
                MAX_SLOT_POST_UPDATES,
                MAX_SLOT_POST_UPDATE_DATA,
                "post-transaction account updates",
            )
        } else {
            (
                self.staging_pre_count as usize,
                self.staging_pre_data_bytes,
                MAX_SLOT_PRE_UPDATES,
                MAX_SLOT_PRE_UPDATE_DATA,
                "pre-transaction account updates",
            )
        };
        if phase_count >= count_limit {
            return Err(ArchiveFormatError::SectionTooLarge {
                section: phase_name,
                bytes: (phase_count + 1) as u64,
                limit: count_limit as u64,
            });
        }
        let next_phase_data =
            phase_data
                .checked_add(view.data.len())
                .ok_or(ArchiveFormatError::SectionTooLarge {
                    section: phase_name,
                    bytes: u64::MAX,
                    limit: data_limit as u64,
                })?;
        if next_phase_data > data_limit {
            return Err(ArchiveFormatError::SectionTooLarge {
                section: phase_name,
                bytes: next_phase_data as u64,
                limit: data_limit as u64,
            });
        }
        let current_len = if post {
            self.staging_post_bytes.len()
        } else {
            self.staging_pre_bytes.len()
        };
        let max_len = self.section_max_len(current_len)?;
        let first_slot = self.current_bucket_first_slot();
        let (buf, count) = if post {
            (&mut self.staging_post_bytes, &mut self.staging_post_count)
        } else {
            (&mut self.staging_pre_bytes, &mut self.staging_pre_count)
        };
        let ctx = &mut self.enc_ctx;
        let diff = &mut self.diff;
        let result = encode_bounded(buf, max_len, first_slot, "orphan update staging", |buf| {
            encode_update_record(view, buf, ctx, diff)
        });
        if let Err(error) = result {
            self.poisoned = true;
            return Err(error);
        }
        *count = count
            .checked_add(1)
            .ok_or(ArchiveFormatError::InvalidContainerLayout(
                "orphan update count overflows u32",
            ))?;
        if post {
            self.staging_post_data_bytes = next_phase_data;
        } else {
            self.staging_pre_data_bytes = next_phase_data;
        }
        self.trim_empty_payload_capacities();
        self.stats.orphan_account_updates += 1;
        self.stats.account_data_bytes_in += view.data.len() as u64;
        Ok(())
    }

    /// Encodes one transaction (with its nested account updates) into the
    /// current slot frame.
    ///
    /// Signatures, message, and metadata encode through the bucket's
    /// dedupe context; each account update's data blob goes through the
    /// bucket's diff encoder keyed by `xxh64(pubkey)`.
    pub fn write_transaction(&mut self, tx: &Transaction) -> Result<(), ArchiveFormatError> {
        self.write_transaction_with_write_version_map(tx, |write_version| write_version)
    }

    /// Re-encodes a transaction while mapping every nested account-update
    /// write version. The multi-segment merger validates the raw sequence
    /// before invoking this infallible mapping hook.
    pub(super) fn write_transaction_with_write_version_map(
        &mut self,
        tx: &Transaction,
        mut map_write_version: impl FnMut(u64) -> u64,
    ) -> Result<(), ArchiveFormatError> {
        self.ensure_writable()?;
        if self.staging_slot.is_none() {
            return Err(ArchiveFormatError::InvalidContainerLayout(
                "write_transaction called outside begin_slot/end_slot",
            ));
        }
        let next_tx_count = u64::from(self.staging_tx_count) + 1;
        self.validate_frame_sequence_count("transactions", next_tx_count)?;
        if self.staging_post_count != 0 {
            return Err(ArchiveFormatError::InvalidContainerLayout(
                "transaction cannot follow a post-transaction orphan update",
            ));
        }
        if let Some((_, data)) = tx
            .iter_account_updates()
            .find(|(_, data)| data.len() > MAX_ACCOUNT_DATA_LEN)
        {
            return Err(ArchiveFormatError::SectionTooLarge {
                section: "account data",
                bytes: data.len() as u64,
                limit: MAX_ACCOUNT_DATA_LEN as u64,
            });
        }
        let max_len = self.section_max_len(self.staging_tx_bytes.len())?;
        let first_slot = self.current_bucket_first_slot();
        let buf = &mut self.staging_tx_bytes;
        let ctx = &mut self.enc_ctx;
        let diff = &mut self.diff;

        let result = encode_bounded(buf, max_len, first_slot, "transaction staging", |buf| {
            // Wire shape mirrors `Transaction`'s field order, minus the account
            // update arena (which is re-encoded per-update through the diff
            // encoder below).
            tx.signatures.encode_ext(buf, Some(ctx))?;
            tx.message.encode_ext(buf, Some(ctx))?;
            tx.status.encode_ext(buf, Some(ctx))?;
            tx.fee.encode_ext(buf, Some(ctx))?;
            tx.pre_balances.encode_ext(buf, Some(ctx))?;
            tx.post_balances.encode_ext(buf, Some(ctx))?;
            tx.loaded_writable_addresses.encode_ext(buf, Some(ctx))?;
            tx.loaded_readonly_addresses.encode_ext(buf, Some(ctx))?;
            tx.inner_instructions.encode_ext(buf, Some(ctx))?;
            tx.log_messages.encode_ext(buf, Some(ctx))?;
            tx.pre_token_balances.encode_ext(buf, Some(ctx))?;
            tx.post_token_balances.encode_ext(buf, Some(ctx))?;
            tx.rewards.encode_ext(buf, Some(ctx))?;
            tx.return_data.encode_ext(buf, Some(ctx))?;
            tx.compute_units_consumed.encode_ext(buf, Some(ctx))?;
            tx.cost_units.encode_ext(buf, Some(ctx))?;

            (tx.account_updates().len() as u64).encode_ext(buf, Some(ctx))?;
            for (meta, data) in tx.iter_account_updates() {
                let view = AccountUpdateView {
                    pubkey: meta.pubkey,
                    lamports: meta.lamports,
                    owner: meta.owner,
                    executable: meta.executable,
                    rent_epoch: meta.rent_epoch,
                    write_version: map_write_version(meta.write_version),
                    data,
                };
                encode_update_record(&view, buf, ctx, diff)?;
            }
            Ok(())
        });
        if let Err(error) = result {
            self.poisoned = true;
            return Err(error);
        }

        self.staging_tx_count = self.staging_tx_count.checked_add(1).ok_or(
            ArchiveFormatError::InvalidContainerLayout("transaction count overflows u32"),
        )?;
        self.trim_empty_payload_capacities();
        self.stats.account_updates += tx.account_updates().len() as u64;
        self.stats.account_data_bytes_in += tx
            .iter_account_updates()
            .map(|(_, data)| data.len() as u64)
            .sum::<u64>();
        self.stats.transactions += 1;
        Ok(())
    }

    /// Closes the current slot frame: assembles the staged sections in
    /// arrival order (epoch, pre-orphans, transactions, post-orphans), then
    /// the block metadata scalars and entry records.
    ///
    /// The passed `meta`'s `pre_updates` / `post_updates` arenas are **not**
    /// encoded. Orphan updates enter the frame exclusively through
    /// [`write_orphan_update`](Self::write_orphan_update) so their dedupe /
    /// diff encoder state matches wire order. (Readers reconstruct the
    /// arenas from the sections, so consumers still see them grouped.)
    pub fn end_slot(
        &mut self,
        meta: &BlockMeta,
        entries: &[EntryRecord],
    ) -> Result<(), ArchiveFormatError> {
        debug_assert!(
            meta.pre_updates.is_empty() && meta.post_updates.is_empty(),
            "end_slot ignores meta's orphan arenas; use write_orphan_update"
        );
        self.end_slot_inner(meta, entries)
    }

    /// Closes a slot replayed by the archive decoder. Its `BlockMeta`
    /// intentionally contains the reconstructed pre/post arenas, which the
    /// ordered update callbacks have already written to this encoder.
    pub(super) fn end_reencoded_slot(
        &mut self,
        meta: &BlockMeta,
        entries: &[EntryRecord],
    ) -> Result<(), ArchiveFormatError> {
        self.end_slot_inner(meta, entries)
    }

    fn end_slot_inner(
        &mut self,
        meta: &BlockMeta,
        entries: &[EntryRecord],
    ) -> Result<(), ArchiveFormatError> {
        self.ensure_writable()?;
        self.validate_frame_sequence_count("entry records", entries.len() as u64)?;
        let slot = self
            .staging_slot
            .take()
            .ok_or(ArchiveFormatError::InvalidContainerLayout(
                "end_slot called without begin_slot",
            ))?;

        // Validate the already-retained payload total before copying any
        // staged section into its final bucket position. Each section is
        // cleared immediately after a successful copy, keeping logical
        // bucket+staging growth inside the same ceiling throughout assembly.
        self.staged_payload_len()?;
        let first_slot = self.current_bucket_first_slot();
        let bucket_limit = self.bucket_limit_bytes();
        let buf = &mut self.bucket_buf;
        let epoch = &mut self.staging_epoch_bytes;
        let pre = &mut self.staging_pre_bytes;
        let tx = &mut self.staging_tx_bytes;
        let post = &mut self.staging_post_bytes;
        let staging_has_epoch = self.staging_has_epoch;
        let pre_count = self.staging_pre_count;
        let tx_count = self.staging_tx_count;
        let post_count = self.staging_post_count;
        let result = encode_bounded(buf, bucket_limit, first_slot, "bucket payload", |buf| {
            slot.encode_ext(buf, None)?;
            buf.write(&[SlotKind::Block as u8])?;

            // Section 1: optional epoch notification.
            buf.write(&[staging_has_epoch as u8])?;
            buf.write(epoch)?;
            epoch.clear();

            // Section 2: pre-transaction orphan updates.
            (pre_count as u64).encode_ext(buf, None)?;
            buf.write(pre)?;
            pre.clear();

            // Section 3: transactions.
            (tx_count as u64).encode_ext(buf, None)?;
            buf.write(tx)?;
            tx.clear();

            // Section 4: post-transaction orphan updates.
            (post_count as u64).encode_ext(buf, None)?;
            buf.write(post)?;
            post.clear();

            // Section 5: block metadata scalars + rewards (stateless; the
            // frame's slot is authoritative, so meta.slot isn't re-encoded).
            encode_block_meta_fields(meta, buf)?;

            // Section 6: entry records.
            (entries.len() as u64).encode_ext(buf, None)?;
            for e in entries {
                e.encode_ext(buf, None)?;
            }
            Ok(())
        });
        if let Err(error) = result {
            self.poisoned = true;
            return Err(error);
        }
        self.trim_empty_payload_capacities();

        self.last_blockhash = meta.blockhash;
        self.bucket_slot_count += 1;
        self.stats.slots += 1;
        self.stats.blocks += 1;
        Ok(())
    }

    /// Flushes the in-progress bucket to the sink (header + payload) and
    /// resets all bucket-scoped encoder state.
    fn flush_bucket(&mut self) -> Result<(), ArchiveFormatError> {
        self.ensure_writable()?;
        let result = self.flush_bucket_inner();
        if result.is_err() {
            self.poisoned = true;
        }
        result
    }

    fn flush_bucket_inner(&mut self) -> Result<(), ArchiveFormatError> {
        let Some(first_slot) = self.bucket_first_slot.take() else {
            return Ok(());
        };

        if self.index.len() as u64 >= MAX_BUCKET_INDEX_ENTRIES {
            self.poisoned = true;
            return Err(ArchiveFormatError::SectionTooLarge {
                section: "bucket count",
                bytes: self.index.len() as u64 + 1,
                limit: MAX_BUCKET_INDEX_ENTRIES,
            });
        }
        self.index
            .try_reserve(1)
            .map_err(|_| ArchiveFormatError::AllocationFailed {
                section: "bucket index entries",
                bytes: (self.index.len() + 1)
                    .saturating_mul(core::mem::size_of::<BucketIndexEntry>())
                    as u64,
            })?;

        let uncompressed_len = self.bucket_buf.len() as u64;
        if uncompressed_len > self.bucket_limit_bytes() as u64 {
            return Err(ArchiveFormatError::BucketTooLarge {
                first_slot,
                bytes: uncompressed_len,
            });
        }
        let (compression, stored): (Compression, std::borrow::Cow<'_, [u8]>) = match self
            .config
            .compression
        {
            Compression::None => (
                Compression::None,
                std::borrow::Cow::Borrowed(&self.bucket_buf),
            ),
            Compression::Zstd => {
                let compressed =
                    compress_zstd_bounded(&self.bucket_buf, self.config.zstd_level, first_slot)?;
                if self.config.format == ArchiveVersion::V1
                    || compressed.len() < self.bucket_buf.len()
                {
                    (Compression::Zstd, std::borrow::Cow::Owned(compressed))
                } else {
                    (
                        Compression::None,
                        std::borrow::Cow::Borrowed(&self.bucket_buf),
                    )
                }
            }
            Compression::Lz4 => {
                let compressed = compress_lz4_bounded(&self.bucket_buf, first_slot)?;
                if compressed.len() < self.bucket_buf.len() {
                    (Compression::Lz4, std::borrow::Cow::Owned(compressed))
                } else {
                    (
                        Compression::None,
                        std::borrow::Cow::Borrowed(&self.bucket_buf),
                    )
                }
            }
        };

        if stored.len() as u64 > MAX_BUCKET_STORED_BYTES {
            self.poisoned = true;
            return Err(ArchiveFormatError::BucketTooLarge {
                first_slot,
                bytes: stored.len() as u64,
            });
        }

        let header = BucketHeader {
            first_slot,
            slot_count: self.bucket_slot_count,
            compression,
            uncompressed_len,
            stored_len: stored.len() as u64,
            xxh64: xxh64(&stored, 0),
            poh_start_hash: self.bucket_poh_anchor,
        };
        let mut header_bytes: Vec<u8> = Vec::with_capacity(128);
        header.encode_ext(&mut header_bytes, None)?;

        self.sink.write_all(&header_bytes)?;
        self.sink.write_all(&stored)?;
        let total_len = (header_bytes.len() + stored.len()) as u64;
        drop(stored);

        self.index.push(BucketIndexEntry {
            first_slot,
            offset: self.file_offset,
            len: total_len,
        });
        self.file_offset += total_len;
        self.stats.bytes_written += total_len;
        self.stats.bucket_bytes_written += total_len;
        self.stats.uncompressed_payload_bytes += uncompressed_len;
        self.stats.buckets += 1;

        // Reset bucket-scoped state.
        self.bucket_buf.clear();
        self.bucket_slot_count = 0;
        reset_encoder(&mut self.enc_ctx);
        self.diff.clear();
        self.trim_empty_payload_capacities();
        Ok(())
    }

    /// Flushes the final bucket, writes the bucket index and footer, and
    /// returns the sink plus aggregate stats.
    pub fn finish(mut self) -> Result<(W, ArchiveStats), ArchiveFormatError> {
        self.ensure_writable()?;
        assert!(
            self.staging_slot.is_none(),
            "finish called with an open slot frame (missing end_slot)"
        );
        self.flush_bucket()?;

        let index_offset = self.file_offset;
        let mut index_bytes: Vec<u8> = Vec::with_capacity(self.index.len() * 24 + 8);
        (self.index.len() as u64).encode_ext(&mut index_bytes, None)?;
        for entry in &self.index {
            entry.encode_ext(&mut index_bytes, None)?;
        }
        self.sink.write_all(&index_bytes)?;

        let footer = Footer {
            index_offset,
            index_len: index_bytes.len() as u64,
            bucket_count: self.index.len() as u64,
            index_xxh64: xxh64(&index_bytes, 0),
        };
        self.sink.write_all(&footer.to_bytes())?;
        self.sink.flush()?;

        self.stats.bytes_written += index_bytes.len() as u64 + FOOTER_LEN as u64;
        Ok((self.sink, self.stats))
    }

    /// Read-only view of the running stats.
    pub fn stats(&self) -> &ArchiveStats {
        &self.stats
    }
}
