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
use lencode::diff::DiffDecoder;
use lencode::prelude::*;
use solana_address::Address;
use solana_hash::Hash;
use xxhash_rust::xxh64::xxh64;

use crate::account_updates::{AccountUpdateView, PushAccountUpdateError};
use crate::dedupe::{new_decoder_context, reset_decoder};
use crate::transactions::{
    Transaction, TransactionStatus, decode_option_log_messages_into, decode_option_zerovec_into,
};

use super::format::*;

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
    /// write_version) and correct counts, but its `data` slice is empty —
    /// the decoder parses the diff framing to advance and skips
    /// reconstruction entirely. In archives dominated by account state this
    /// skips the bulk of decode memory traffic.
    pub account_update_data: bool,
}

impl Consumption {
    /// Consume everything the archive stores (the default).
    pub const fn all() -> Self {
        Self {
            account_update_data: true,
        }
    }

    /// Drop account-update data bytes: updates keep metadata and counts but
    /// their `data` slices decode empty.
    pub const fn without_account_update_data(mut self) -> Self {
        self.account_update_data = false;
        self
    }

    /// The union of two declarations: a stream field is consumed if either
    /// side consumes it. Used by fan-out visitors to combine their
    /// children's declarations.
    pub const fn union(self, other: Self) -> Self {
        Self {
            account_update_data: self.account_update_data || other.account_update_data,
        }
    }
}

impl Default for Consumption {
    fn default() -> Self {
        Self::all()
    }
}

/// Callbacks invoked while decoding slot frames, in firehose order:
/// epoch notification first (boundary slots), then every transaction of
/// the slot, then the slot's block notification — with the block's
/// runtime-direct ("orphan") account updates delivered grouped on the
/// [`BlockNotification`]'s pre/post arenas, and each transaction's own
/// account updates reachable via [`Transaction::iter_account_updates`].
pub trait SlotVisitor {
    /// Epoch notification (fires before the boundary slot's transactions).
    /// `meta` points at the decoder's reusable scratch — copy out what you
    /// need.
    fn on_epoch(&mut self, _meta: &EpochMeta) {}
    /// One decoded transaction (with nested account updates). `tx` points
    /// at the decoder's reusable scratch buffer — copy out what you need.
    fn on_transaction(&mut self, _slot: u64, _tx_index: u32, _tx: &Transaction) {}
    /// End of a slot frame: the block notification (full block with
    /// metadata + grouped orphan updates, or a leader-skipped marker) plus
    /// the block's PoH entry records (empty for skipped slots).
    fn on_block(&mut self, _notification: &BlockNotification, _entries: &[EntryRecord]) {}
    /// Declares what this visitor consumes; drivers sample it and configure
    /// the decoder to skip unconsumed work. Defaults to everything (safe).
    /// Must be stable for the duration of a decode drive — drivers latch it
    /// at bucket boundaries.
    fn consumption(&self) -> Consumption {
        Consumption::all()
    }
}

/// Per-category breakdown of the *uncompressed payload* bytes a decoder has
/// processed — the deduped + diff-encoded stream within buckets, before the
/// per-bucket zstd. Accumulated across every [`BucketDecoder::decode_slot_frame`]
/// call; query [`BucketDecoder::byte_stats`]. This is the in-archive
/// representation (account data is already diff-encoded here), not raw bytes.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct PayloadByteStats {
    /// Transaction-record bytes, excluding each transaction's own account
    /// updates (signatures, message, status/meta, balances, logs, etc.).
    pub transaction_bytes: u64,
    /// Account-update bytes — the account-state write stream (metadata +
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
    /// Materialize account-update data (the per-account diff
    /// reconstruction). When `false`, updates decode with full metadata but
    /// empty `data` slices, skipping reconstruction and the diff store
    /// entirely. Latched per bucket at
    /// [`load_bucket_bytes`](Self::load_bucket_bytes): diff records resolve
    /// against blobs stored earlier in the *same* bucket, so flipping
    /// mid-bucket would corrupt or fail reconstruction — the effective
    /// setting changes only when a bucket is (re)loaded.
    pub materialize_account_data: bool,

    /// The materialization setting latched when the current bucket was
    /// loaded (see [`Self::materialize_account_data`]).
    bucket_materialize: bool,
    payload: Vec<u8>,
    pos: usize,
    slots_remaining: u32,
    /// Slot of the last frame decoded (whether or not it was emitted). Lets
    /// a streaming driver continue in place on forward reads.
    last_decoded_slot: Option<u64>,
    dec_ctx: DecoderContext,
    diff: DiffDecoder,
    scratch: Box<Transaction>,
    // Two permanent notification scratches — one pinned to each variant.
    // Swapping a single scratch's variant would memset the whole ~40 MiB
    // enum on every skipped→block boundary (measured ~400 µs per swap);
    // with pinned variants the zeroing happens exactly twice, here at
    // construction.
    block_scratch: Box<BlockNotification>,
    skipped_scratch: Box<BlockNotification>,
    epoch_scratch: Box<EpochMeta>,
    entries_scratch: Vec<EntryRecord>,
    last_blockhash: Hash,
    /// Running per-category payload-byte tally (cheap; per-section, not
    /// per-record). Queried via [`Self::byte_stats`].
    byte_stats: PayloadByteStats,
}

impl Default for BucketDecoder {
    fn default() -> Self {
        Self::new()
    }
}

impl BucketDecoder {
    /// Allocates the reusable decode scratches once. Cheap to keep around;
    /// expensive to recreate (the notification scratches are ~40 MiB each).
    pub fn new() -> Self {
        Self {
            verify_chain: false,
            materialize_account_data: true,
            bucket_materialize: true,
            payload: Vec::new(),
            pos: 0,
            slots_remaining: 0,
            last_decoded_slot: None,
            dec_ctx: new_decoder_context(),
            diff: DiffDecoder::with_capacity(64 * 1024),
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
            byte_stats: PayloadByteStats::default(),
        }
    }

    /// The running per-category payload-byte tally accumulated across every
    /// [`decode_slot_frame`](Self::decode_slot_frame) since construction.
    pub fn byte_stats(&self) -> PayloadByteStats {
        self.byte_stats
    }

    /// Loads and validates one bucket frame (`BucketHeader ++ stored
    /// payload`), decompressing into the internal payload buffer and
    /// resetting all decoder state for the new bucket. After this,
    /// [`decode_slot_frame`](Self::decode_slot_frame) yields the bucket's
    /// frames in order.
    pub fn load_bucket_bytes(&mut self, raw: &[u8]) -> Result<(), ArchiveFormatError> {
        let mut cur = lencode::io::Cursor::new(raw);
        let header = BucketHeader::decode_ext(&mut cur, None)?;
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

        self.payload.clear();
        match header.compression {
            Compression::None => self.payload.extend_from_slice(stored),
            Compression::Zstd => {
                self.payload = zstd::bulk::decompress(stored, header.uncompressed_len as usize)
                    .map_err(ArchiveFormatError::Io)?;
            }
        }

        self.pos = 0;
        self.slots_remaining = header.slot_count;
        self.last_decoded_slot = None;
        self.last_blockhash = header.poh_start_hash;
        reset_decoder(&mut self.dec_ctx);
        self.diff.clear();
        // Latch the materialization mode for this whole bucket: the diff
        // store just reset, so a bucket decoded end-to-end without
        // materialization never misses a stored blob, and the next bucket
        // starts clean either way.
        self.bucket_materialize = self.materialize_account_data;
        Ok(())
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
        let mut cur = lencode::io::Cursor::new(&self.payload[self.pos..]);
        let slot = u64::decode_ext(&mut cur, None)?;
        let mut kind = [0u8; 1];
        cur.read(&mut kind)?;
        let kind = SlotKind::try_from(kind[0])?;
        let emit = slot >= start_slot;
        let materialize = self.bucket_materialize;

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
                if flag[0] == 1 {
                    self.epoch_scratch.clear();
                    self.epoch_scratch.epoch = u64::decode_ext(&mut cur, None)?;
                    self.epoch_scratch.start_slot = u64::decode_ext(&mut cur, None)?;
                    self.epoch_scratch.slot_count = u64::decode_ext(&mut cur, None)?;
                    self.epoch_scratch.first_block_slot = u64::decode_ext(&mut cur, None)?;
                    self.epoch_scratch.num_reward_partitions =
                        Option::<u64>::decode_ext(&mut cur, None)?;
                    let n = u64::decode_ext(&mut cur, None)?;
                    let upd_start = cur.position();
                    for _ in 0..n {
                        let updates = &mut self.epoch_scratch.updates;
                        decode_update_record_into(
                            &mut cur,
                            &mut self.dec_ctx,
                            &mut self.diff,
                            materialize,
                            |view| updates.push(view),
                        )?;
                    }
                    au_bytes += cur.position() - upd_start;
                    if emit {
                        visitor.on_epoch(&self.epoch_scratch);
                    }
                }

                // Section 2: pre-transaction orphan updates → grouped onto
                // the notification's pre arena.
                let pre_count = u64::decode_ext(&mut cur, None)?;
                let pre_start = cur.position();
                for _ in 0..pre_count {
                    let pre = &mut meta.pre_updates;
                    decode_update_record_into(
                        &mut cur,
                        &mut self.dec_ctx,
                        &mut self.diff,
                        materialize,
                        |view| pre.push(view),
                    )?;
                }
                au_bytes += cur.position() - pre_start;

                // Section 3: transactions.
                let tx_count = u64::decode_ext(&mut cur, None)? as u32;
                for tx_index in 0..tx_count {
                    let tx_start = cur.position();
                    let tx_au = read_tx_record(
                        &mut cur,
                        &mut self.scratch,
                        &mut self.dec_ctx,
                        &mut self.diff,
                        materialize,
                    )?;
                    tx_field_bytes += (cur.position() - tx_start) - tx_au;
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
                let post_count = u64::decode_ext(&mut cur, None)?;
                let post_start = cur.position();
                for _ in 0..post_count {
                    let post = &mut meta.post_updates;
                    decode_update_record_into(
                        &mut cur,
                        &mut self.dec_ctx,
                        &mut self.diff,
                        materialize,
                        |view| post.push(view),
                    )?;
                }
                au_bytes += cur.position() - post_start;

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
                let entry_count = u64::decode_ext(&mut cur, None)? as usize;
                self.entries_scratch.clear();
                for _ in 0..entry_count {
                    self.entries_scratch
                        .push(EntryRecord::decode_ext(&mut cur, None)?);
                }

                if self.verify_chain
                    && self.last_blockhash != Hash::default()
                    && meta.parent_blockhash != self.last_blockhash
                {
                    return Err(ArchiveFormatError::PohMismatch { slot });
                }
                self.last_blockhash = meta.blockhash;

                if emit {
                    visitor.on_block(&self.block_scratch, &self.entries_scratch);
                }
            }
        }

        let frame_bytes = cur.position();
        self.byte_stats.account_update_bytes += au_bytes as u64;
        self.byte_stats.transaction_bytes += tx_field_bytes as u64;
        self.byte_stats.other_bytes += (frame_bytes - au_bytes - tx_field_bytes) as u64;
        self.pos += frame_bytes;
        self.slots_remaining -= 1;
        self.last_decoded_slot = Some(slot);
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
    let header_len = read_io_varint(&mut cur)? as usize;
    let mut header_bytes = vec![0u8; header_len];
    std::io::Read::read_exact(&mut cur, &mut header_bytes)?;
    let mut lc = lencode::io::Cursor::new(&header_bytes[..]);
    let header = FileHeader::decode_ext(&mut lc, None)?;
    if header.format_version != FORMAT_VERSION {
        return Err(ArchiveFormatError::UnsupportedVersion(
            header.format_version,
        ));
    }
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
    if xxh64(index_bytes, 0) != footer.index_xxh64 {
        return Err(ArchiveFormatError::IndexChecksum);
    }
    let mut cur = lencode::io::Cursor::new(index_bytes);
    let count = u64::decode_ext(&mut cur, None)? as usize;
    let mut index = Vec::with_capacity(count);
    for _ in 0..count {
        index.push(BucketIndexEntry::decode_ext(&mut cur, None)?);
    }
    Ok(index)
}

/// Index position of the bucket whose slot window contains `slot`.
///
/// Buckets are aligned to `header.slot_start` in fixed `header.bucket_slots`
/// windows, so the position is computed directly (no search). For sparse
/// files the computed position may overshoot; we walk back to the last
/// bucket whose `first_slot <= slot` — zero iterations for dense archives.
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
        // 1 (Block) — an all-zero BlockMeta payload is valid.
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
/// later records in the bucket (including transaction fields) reference —
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
) -> Result<(), ArchiveFormatError> {
    let pubkey = Address::decode_ext(reader, Some(ctx))?;
    let lamports = u64::decode_ext(reader, Some(ctx))?;
    let owner = Address::decode_ext(reader, Some(ctx))?;
    let executable = bool::decode_ext(reader, Some(ctx))?;
    let rent_epoch = u64::decode_ext(reader, Some(ctx))?;
    let write_version = u64::decode_ext(reader, Some(ctx))?;
    let data: Vec<u8>;
    let data_slice: &[u8] = if materialize {
        diff.set_key(account_diff_key(&pubkey));
        data = diff.decode_blob(reader)?;
        &data
    } else {
        skip_diff_blob(reader)?;
        &[]
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
    Ok(())
}

/// Advances `reader` over one diff-encoded blob without reconstructing it.
///
/// Byte-exact framing mirror of lencode 1.1's `DiffDecoder::decode_blob`
/// (which has no skip API): varint mode, then — mode 0: varint len + len
/// raw bytes; mode 1: varint new_len, varint num_patches, then per patch
/// varint gap + varint patch_len + patch_len raw bytes (gaps carry no
/// stream bytes); mode 2: varint new_len, varint compressed_len +
/// compressed_len raw bytes. The varints here are `u64::decode_ext`, which
/// is the same `Lencode::decode_varint_u64` codec `decode_blob` uses, so
/// the cursor advances over exactly the span the materializing path
/// consumes. Touches no diff-store state; correctness relies on the
/// bucket-scoped latch (see [`BucketDecoder::materialize_account_data`]).
pub(crate) fn skip_diff_blob(reader: &mut impl Read) -> Result<(), ArchiveFormatError> {
    let mode = u64::decode_ext(reader, None)?;
    match mode {
        0 => {
            let len = u64::decode_ext(reader, None)? as usize;
            skip_reader_bytes(reader, len)
        }
        1 => {
            let _new_len = u64::decode_ext(reader, None)?;
            let num_patches = u64::decode_ext(reader, None)? as usize;
            for _ in 0..num_patches {
                let _gap = u64::decode_ext(reader, None)?;
                let patch_len = u64::decode_ext(reader, None)? as usize;
                skip_reader_bytes(reader, patch_len)?;
            }
            Ok(())
        }
        2 => {
            let _new_len = u64::decode_ext(reader, None)?;
            let compressed_len = u64::decode_ext(reader, None)? as usize;
            skip_reader_bytes(reader, compressed_len)
        }
        _ => Err(ArchiveFormatError::Encode(lencode::io::Error::InvalidData)),
    }
}

/// Advances `reader` by `n` bytes without copying when it exposes its
/// buffer (the in-memory bucket cursor always does), falling back to
/// chunked discard reads otherwise.
fn skip_reader_bytes(reader: &mut impl Read, n: usize) -> Result<(), ArchiveFormatError> {
    match reader.buf() {
        Some(buf) => {
            if buf.len() < n {
                return Err(ArchiveFormatError::Encode(
                    lencode::io::Error::ReaderOutOfData,
                ));
            }
            reader.advance(n);
            Ok(())
        }
        None => {
            let mut remaining = n;
            let mut scratch = [0u8; 4096];
            while remaining > 0 {
                let take = remaining.min(scratch.len());
                let got = reader.read(&mut scratch[..take])?;
                if got != take {
                    return Err(ArchiveFormatError::Encode(
                        lencode::io::Error::ReaderOutOfData,
                    ));
                }
                remaining -= take;
            }
            Ok(())
        }
    }
}

/// Decodes one transaction record into `scratch`. Exact mirror of
/// [`ArchiveWriter::write_transaction`](super::ArchiveWriter::write_transaction).
/// Returns the byte span consumed by the transaction's own account updates, so
/// the caller can split transaction-field bytes from account-update bytes.
fn read_tx_record(
    cur: &mut lencode::io::Cursor<&[u8]>,
    scratch: &mut Transaction,
    ctx: &mut DecoderContext,
    diff: &mut DiffDecoder,
    materialize: bool,
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

    let au_count = u64::decode_ext(cur, Some(ctx))?;
    let au_start = cur.position();
    for _ in 0..au_count {
        decode_update_record_into(cur, ctx, diff, materialize, |view| {
            scratch.push_account_update(view)
        })?;
    }
    Ok(cur.position() - au_start)
}

/// Reads a lencode varint directly from a `std::io::Read` stream.
pub(crate) fn read_io_varint(r: &mut impl std::io::Read) -> Result<u64, ArchiveFormatError> {
    let mut first = [0u8; 1];
    r.read_exact(&mut first)?;
    if first[0] & 0x80 == 0 {
        return Ok(first[0] as u64);
    }
    let n = (first[0] & 0x7F) as usize;
    if n > 8 {
        return Err(ArchiveFormatError::Encode(lencode::io::Error::InvalidData));
    }
    let mut bytes = [0u8; 8];
    r.read_exact(&mut bytes[..n])?;
    Ok(u64::from_le_bytes(bytes))
}
