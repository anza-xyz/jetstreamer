//! Streaming writer for the horizon archive container format.
//!
//! Writes strictly forward (no seeks), buffering one bucket at a time so
//! the payload can be checksummed and optionally zstd-compressed before it
//! hits the sink. All stateful encoders (pubkey dedupe scratch, account
//! diff store) reset at bucket boundaries — see the [module docs](super).
use lencode::context::EncoderContext;
use lencode::diff::DiffEncoder;
use lencode::prelude::*;
use solana_hash::Hash;
use xxhash_rust::xxh64::xxh64;

use crate::dedupe::{new_encoder_context, reset_encoder};
use crate::transactions::Transaction;

use super::format::*;

/// Configuration for [`ArchiveWriter`].
#[derive(Debug, Clone)]
pub struct ArchiveWriterConfig {
    /// Slots per bucket (encoder reset / seek granularity). `1` disables
    /// cross-slot diff compression; [`DEFAULT_BUCKET_SLOTS`] balances
    /// compression and seek latency.
    pub bucket_slots: u16,
    /// Bucket payload compression.
    pub compression: Compression,
    /// zstd level when `compression == Zstd` (3 is the sweet spot for
    /// write throughput; higher levels buy little on already-deduped data).
    pub zstd_level: i32,
}

impl Default for ArchiveWriterConfig {
    fn default() -> Self {
        Self {
            bucket_slots: DEFAULT_BUCKET_SLOTS,
            compression: Compression::Zstd,
            zstd_level: 3,
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
    /// Number of slot frames written (blocks + skipped).
    pub slots: u64,
    /// Number of non-skipped block frames.
    pub blocks: u64,
    /// Number of transactions written.
    pub transactions: u64,
    /// Number of account updates written.
    pub account_updates: u64,
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
    staging_slot: Option<u64>,
    staging_tx_count: u32,
    staging_tx_bytes: Vec<u8>,
}

impl<W: std::io::Write> ArchiveWriter<W> {
    /// Creates a writer and emits the magic + file header to `sink`.
    pub fn new(
        mut sink: W,
        epoch: u64,
        slot_start: u64,
        slot_count: u64,
        config: ArchiveWriterConfig,
    ) -> Result<Self, ArchiveFormatError> {
        assert!(config.bucket_slots >= 1, "bucket_slots must be >= 1");
        let header = FileHeader {
            format_version: FORMAT_VERSION,
            bucket_slots: config.bucket_slots,
            epoch,
            slot_start,
            slot_count,
            prime_table_id: *PRIME_TABLE_ID,
            flags: 0,
            meta: EpochMeta {
                created_unix_ms: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_millis() as u64)
                    .unwrap_or(0),
                writer_version: env!("CARGO_PKG_VERSION").as_bytes().to_vec(),
                reserved: vec![],
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
            slot_end: slot_start + slot_count,
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
            enc_ctx: new_encoder_context(),
            diff: DiffEncoder::with_capacity(64 * 1024),
            last_blockhash: Hash::default(),
            last_slot: None,
            staging_slot: None,
            staging_tx_count: 0,
            staging_tx_bytes: Vec::with_capacity(4 << 20),
        })
    }

    /// Returns the bucket ordinal for a slot, relative to `slot_start`.
    #[inline]
    fn bucket_id(&self, slot: u64) -> u64 {
        (slot - self.slot_start) / self.config.bucket_slots as u64
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
        // Crossing into a new bucket? Flush the previous one first.
        if let Some(first) = self.bucket_first_slot
            && self.bucket_id(first) != self.bucket_id(slot)
        {
            self.flush_bucket()?;
        }
        if self.bucket_first_slot.is_none() {
            self.bucket_first_slot = Some(slot);
            self.bucket_poh_anchor = self.last_blockhash;
        }
        self.last_slot = Some(slot);
        Ok(())
    }

    /// Records a leader-skipped slot (tiny frame; no block payload).
    pub fn write_skipped_slot(&mut self, slot: u64) -> Result<(), ArchiveFormatError> {
        assert!(
            self.staging_slot.is_none(),
            "write_skipped_slot called inside begin_slot/end_slot"
        );
        self.check_slot(slot)?;
        slot.encode_ext(&mut self.bucket_buf, None)?;
        self.bucket_buf.push(SlotKind::Skipped as u8);
        self.bucket_slot_count += 1;
        self.stats.slots += 1;
        Ok(())
    }

    /// Opens a block frame for `slot`. Follow with any number of
    /// [`write_transaction`](Self::write_transaction) calls, then
    /// [`end_slot`](Self::end_slot).
    pub fn begin_slot(&mut self, slot: u64) -> Result<(), ArchiveFormatError> {
        assert!(
            self.staging_slot.is_none(),
            "begin_slot called twice without end_slot"
        );
        self.check_slot(slot)?;
        self.staging_slot = Some(slot);
        self.staging_tx_count = 0;
        self.staging_tx_bytes.clear();
        Ok(())
    }

    /// Encodes one transaction (with its nested account updates) into the
    /// current slot frame.
    ///
    /// Signatures, message, and metadata encode through the bucket's
    /// dedupe context; each account update's data blob goes through the
    /// bucket's diff encoder keyed by `xxh64(pubkey)`.
    pub fn write_transaction(&mut self, tx: &Transaction) -> Result<(), ArchiveFormatError> {
        assert!(
            self.staging_slot.is_some(),
            "write_transaction called outside begin_slot/end_slot"
        );
        let buf = &mut self.staging_tx_bytes;
        let ctx = &mut self.enc_ctx;

        // Wire shape mirrors `Transaction`'s field order, minus the account
        // update arena (which is re-encoded per-update through the diff
        // encoder below).
        tx.signatures.encode_ext(buf, Some(ctx))?;
        tx.message.encode_ext(buf, Some(ctx))?;
        tx.status.encode_ext(buf, Some(ctx))?;
        tx.fee.encode_ext(buf, Some(ctx))?;
        tx.pre_balances.encode_ext(buf, Some(ctx))?;
        tx.post_balances.encode_ext(buf, Some(ctx))?;
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
            meta.pubkey.encode_ext(buf, Some(ctx))?;
            meta.lamports.encode_ext(buf, Some(ctx))?;
            meta.owner.encode_ext(buf, Some(ctx))?;
            meta.executable.encode_ext(buf, Some(ctx))?;
            meta.rent_epoch.encode_ext(buf, Some(ctx))?;
            meta.write_version.encode_ext(buf, Some(ctx))?;
            self.diff.set_key(account_diff_key(&meta.pubkey));
            self.diff.encode_blob(data, buf)?;
            self.stats.account_updates += 1;
            self.stats.account_data_bytes_in += data.len() as u64;
        }

        self.staging_tx_count += 1;
        self.stats.transactions += 1;
        Ok(())
    }

    /// Closes the current slot frame: appends the staged transaction
    /// bytes, block metadata, and entry records to the bucket.
    pub fn end_slot(
        &mut self,
        meta: &BlockMeta,
        entries: &[EntryRecord],
    ) -> Result<(), ArchiveFormatError> {
        let slot = self
            .staging_slot
            .take()
            .expect("end_slot called without begin_slot");

        let buf = &mut self.bucket_buf;
        slot.encode_ext(buf, None)?;
        buf.push(SlotKind::Block as u8);
        (self.staging_tx_count as u64).encode_ext(buf, None)?;
        buf.extend_from_slice(&self.staging_tx_bytes);
        // BlockMeta and entries are stateless (no dedupe/diff context); see
        // `BlockMeta` docs for why.
        meta.encode_ext(buf, None)?;
        (entries.len() as u64).encode_ext(buf, None)?;
        for e in entries {
            e.encode_ext(buf, None)?;
        }

        self.last_blockhash = meta.blockhash;
        self.bucket_slot_count += 1;
        self.stats.slots += 1;
        self.stats.blocks += 1;
        Ok(())
    }

    /// Flushes the in-progress bucket to the sink (header + payload) and
    /// resets all bucket-scoped encoder state.
    fn flush_bucket(&mut self) -> Result<(), ArchiveFormatError> {
        let Some(first_slot) = self.bucket_first_slot.take() else {
            return Ok(());
        };

        let uncompressed_len = self.bucket_buf.len() as u64;
        let stored: std::borrow::Cow<'_, [u8]> = match self.config.compression {
            Compression::None => std::borrow::Cow::Borrowed(&self.bucket_buf),
            Compression::Zstd => std::borrow::Cow::Owned(
                zstd::bulk::compress(&self.bucket_buf, self.config.zstd_level)
                    .map_err(ArchiveFormatError::Io)?,
            ),
        };

        let header = BucketHeader {
            first_slot,
            slot_count: self.bucket_slot_count,
            compression: self.config.compression,
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
        self.stats.uncompressed_payload_bytes += uncompressed_len;
        self.stats.buckets += 1;

        // Reset bucket-scoped state.
        self.bucket_buf.clear();
        self.bucket_slot_count = 0;
        reset_encoder(&mut self.enc_ctx);
        self.diff.clear();
        Ok(())
    }

    /// Flushes the final bucket, writes the bucket index and footer, and
    /// returns the sink plus aggregate stats.
    pub fn finish(mut self) -> Result<(W, ArchiveStats), ArchiveFormatError> {
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
