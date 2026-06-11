//! Streaming reader for the horizon archive container format.
//!
//! Mirrors [`ArchiveWriter`](super::ArchiveWriter)'s wire layout exactly;
//! the two modules together define the format contract for slot frames.
//! Decoding is zero-alloc on the hot path except for the diff decoder's
//! reconstructed account-data blobs.
use lencode::context::DecoderContext;
use lencode::diff::DiffDecoder;
use lencode::prelude::*;
use solana_address::Address;
use solana_hash::Hash;
use xxhash_rust::xxh64::xxh64;

use crate::account_updates::AccountUpdateView;
use crate::dedupe::{new_decoder_context, reset_decoder};
use crate::transactions::{Transaction, TransactionStatus, decode_option_zerovec_into};

use super::format::*;

/// Callbacks invoked by [`ArchiveReader::read_slots`], in firehose order:
/// every transaction of a slot first, then the slot's block metadata.
pub trait SlotVisitor {
    /// A leader-skipped slot.
    fn on_skipped(&mut self, _slot: u64) {}
    /// One decoded transaction (with nested account updates). `tx` points
    /// at the reader's reusable scratch buffer — copy out what you need.
    fn on_transaction(&mut self, _slot: u64, _tx_index: u32, _tx: &Transaction) {}
    /// End of a block frame: metadata + PoH entry records.
    fn on_block(&mut self, _slot: u64, _meta: &BlockMeta, _entries: &[EntryRecord]) {}
}

/// Streaming archive reader over any `Read + Seek` source.
pub struct ArchiveReader<R: std::io::Read + std::io::Seek> {
    source: R,
    header: FileHeader,
    index: Vec<BucketIndexEntry>,
    /// Verify blockhash chain continuity (parent_blockhash linkage) while
    /// streaming. Full SHA-256 PoH recomputation is a planned follow-up;
    /// the format already stores everything it needs (`num_hashes` +
    /// per-entry tx grouping + blockhashes).
    pub verify_chain: bool,

    // --- current bucket decode state ---
    /// Index of the currently loaded bucket, if any.
    current_bucket: Option<usize>,
    /// Slot of the last frame decoded from the current bucket (whether or
    /// not it was emitted). Used to continue in place on forward reads.
    last_decoded_slot: Option<u64>,
    /// Number of bucket loads performed (seek + checksum + decompress).
    /// Diagnostic: sequential forward reads should keep this near
    /// `bucket_count`, not `O(read_slots calls)`.
    bucket_loads: u64,
    payload: Vec<u8>,
    pos: usize,
    slots_remaining: u32,
    dec_ctx: DecoderContext,
    diff: DiffDecoder,
    scratch: Box<Transaction>,
    entries_scratch: Vec<EntryRecord>,
    last_blockhash: Hash,
}

impl<R: std::io::Read + std::io::Seek> ArchiveReader<R> {
    /// Opens an archive: validates magic, reads the file header, footer,
    /// and bucket index, and checks prime-table compatibility.
    pub fn open(mut source: R) -> Result<Self, ArchiveFormatError> {
        use std::io::SeekFrom;

        // --- file header ---
        source.seek(SeekFrom::Start(0))?;
        let mut magic = [0u8; 8];
        source.read_exact(&mut magic)?;
        if magic != MAGIC {
            return Err(ArchiveFormatError::BadMagic);
        }
        let header_len = read_io_varint(&mut source)? as usize;
        let mut header_bytes = vec![0u8; header_len];
        source.read_exact(&mut header_bytes)?;
        let mut cur = lencode::io::Cursor::new(&header_bytes[..]);
        let header = FileHeader::decode_ext(&mut cur, None)?;
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

        // --- footer + index ---
        source.seek(SeekFrom::End(-(FOOTER_LEN as i64)))?;
        let mut footer_bytes = [0u8; FOOTER_LEN];
        source.read_exact(&mut footer_bytes)?;
        let footer = Footer::from_bytes(&footer_bytes)?;

        source.seek(SeekFrom::Start(footer.index_offset))?;
        let mut index_bytes = vec![0u8; footer.index_len as usize];
        source.read_exact(&mut index_bytes)?;
        if xxh64(&index_bytes, 0) != footer.index_xxh64 {
            return Err(ArchiveFormatError::IndexChecksum);
        }
        let mut cur = lencode::io::Cursor::new(&index_bytes[..]);
        let count = u64::decode_ext(&mut cur, None)? as usize;
        let mut index = Vec::with_capacity(count);
        for _ in 0..count {
            index.push(BucketIndexEntry::decode_ext(&mut cur, None)?);
        }

        Ok(Self {
            source,
            header,
            index,
            verify_chain: false,
            current_bucket: None,
            last_decoded_slot: None,
            bucket_loads: 0,
            payload: Vec::new(),
            pos: 0,
            slots_remaining: 0,
            dec_ctx: new_decoder_context(),
            diff: DiffDecoder::with_capacity(64 * 1024),
            scratch: Transaction::new_boxed(),
            entries_scratch: Vec::with_capacity(2048),
            last_blockhash: Hash::default(),
        })
    }

    /// The archive's file header.
    pub fn header(&self) -> &FileHeader {
        &self.header
    }

    /// Number of buckets in the archive.
    pub fn bucket_count(&self) -> usize {
        self.index.len()
    }

    /// Returns the index position of the bucket whose slot window contains
    /// `slot`.
    ///
    /// Buckets are aligned to `slot_start` in fixed `bucket_slots` windows,
    /// so the position is computed directly (no search). For sparse files
    /// (windows with no frames at all) the computed position may overshoot;
    /// we walk back to the last bucket whose `first_slot <= slot` — zero
    /// iterations for dense archives.
    fn bucket_containing(&self, slot: u64) -> usize {
        let id = (slot.saturating_sub(self.header.slot_start) / self.header.bucket_slots as u64)
            as usize;
        let mut i = id.min(self.index.len() - 1);
        while i > 0 && self.index[i].first_slot > slot {
            i -= 1;
        }
        i
    }

    /// Streams slots to `visitor`, starting at the first stored slot ≥
    /// `start_slot`, for at most `max_slots` slot frames. Returns the
    /// number of slot frames visited.
    ///
    /// The reader is stateful: when `start_slot` lies ahead of the current
    /// decode position (the common sequential-consumption pattern), it
    /// simply continues streaming forward — no re-seek, no re-decompress,
    /// no re-decode. A bucket is (re)loaded only when the target is in a
    /// different bucket or behind the current position. Frames between the
    /// current position and `start_slot` are decoded without emission
    /// (their bytes must flow through the dedupe/diff decoders to
    /// reproduce encoder state) — "start from the nearest slot before the
    /// target and stream through".
    pub fn read_slots<V: SlotVisitor>(
        &mut self,
        start_slot: u64,
        max_slots: u64,
        visitor: &mut V,
    ) -> Result<u64, ArchiveFormatError> {
        if self.index.is_empty() || max_slots == 0 {
            return Ok(0);
        }
        let target_bucket = self.bucket_containing(start_slot);
        let continue_in_place = match self.current_bucket {
            // Right bucket already loaded and we haven't decoded past the
            // target: keep streaming from where we are.
            Some(cur) if cur == target_bucket => {
                self.last_decoded_slot.is_none_or(|last| last < start_slot)
            }
            // Target is in a later bucket: jumping is strictly cheaper than
            // streaming through — encoder state resets per bucket, so the
            // intermediate buckets contribute nothing.
            _ => false,
        };
        if !continue_in_place {
            self.load_bucket(target_bucket)?;
        }

        let mut visited = 0u64;
        while visited < max_slots {
            if self.slots_remaining == 0 {
                let Some(cur) = self.current_bucket else {
                    break;
                };
                if cur + 1 >= self.index.len() {
                    break;
                }
                self.load_bucket(cur + 1)?;
            }
            let emitted = self.decode_slot_frame(start_slot, visitor)?;
            if emitted {
                visited += 1;
            }
        }
        Ok(visited)
    }

    /// Number of bucket loads (seek + checksum + decompress) performed so
    /// far. Sequential forward consumption should keep this near the number
    /// of distinct buckets traversed, independent of how many `read_slots`
    /// calls were made.
    pub fn bucket_loads(&self) -> u64 {
        self.bucket_loads
    }

    /// Loads and validates bucket `idx`, resetting all decoder state.
    fn load_bucket(&mut self, idx: usize) -> Result<(), ArchiveFormatError> {
        use std::io::SeekFrom;
        let entry = self.index[idx];
        self.source.seek(SeekFrom::Start(entry.offset))?;
        let mut raw = vec![0u8; entry.len as usize];
        self.source.read_exact(&mut raw)?;

        let mut cur = lencode::io::Cursor::new(&raw[..]);
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

        self.current_bucket = Some(idx);
        self.last_decoded_slot = None;
        self.bucket_loads += 1;
        self.pos = 0;
        self.slots_remaining = header.slot_count;
        self.last_blockhash = header.poh_start_hash;
        reset_decoder(&mut self.dec_ctx);
        self.diff.clear();
        Ok(())
    }

    /// Decodes one slot frame from the current bucket. Emits callbacks
    /// only when `slot >= start_slot`; returns whether callbacks fired.
    fn decode_slot_frame<V: SlotVisitor>(
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

        match kind {
            SlotKind::Skipped => {
                if emit {
                    visitor.on_skipped(slot);
                }
            }
            SlotKind::Block => {
                let tx_count = u64::decode_ext(&mut cur, None)? as u32;
                for tx_index in 0..tx_count {
                    read_tx_record(
                        &mut cur,
                        &mut self.scratch,
                        &mut self.dec_ctx,
                        &mut self.diff,
                    )?;
                    if emit {
                        visitor.on_transaction(slot, tx_index, &self.scratch);
                    }
                }
                let meta = BlockMeta::decode_ext(&mut cur, None)?;
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
                    visitor.on_block(slot, &meta, &self.entries_scratch);
                }
            }
        }

        self.pos += cur.position();
        self.slots_remaining -= 1;
        self.last_decoded_slot = Some(slot);
        Ok(emit)
    }
}

/// Decodes one transaction record into `scratch`. Exact mirror of
/// [`ArchiveWriter::write_transaction`](super::ArchiveWriter::write_transaction).
fn read_tx_record(
    reader: &mut impl Read,
    scratch: &mut Transaction,
    ctx: &mut DecoderContext,
    diff: &mut DiffDecoder,
) -> Result<(), ArchiveFormatError> {
    scratch.clear();

    scratch.signatures.decode_into(reader, Some(ctx))?;
    scratch.message.decode_into(reader, Some(ctx))?;
    scratch.status = TransactionStatus::decode_ext(reader, Some(ctx))?;
    scratch.fee = u64::decode_ext(reader, Some(ctx))?;
    scratch.pre_balances.decode_into(reader, Some(ctx))?;
    scratch.post_balances.decode_into(reader, Some(ctx))?;
    decode_option_zerovec_into(&mut scratch.inner_instructions, reader, Some(ctx))?;
    decode_option_zerovec_into(&mut scratch.log_messages, reader, Some(ctx))?;
    decode_option_zerovec_into(&mut scratch.pre_token_balances, reader, Some(ctx))?;
    decode_option_zerovec_into(&mut scratch.post_token_balances, reader, Some(ctx))?;
    decode_option_zerovec_into(&mut scratch.rewards, reader, Some(ctx))?;
    scratch.return_data = Option::decode_ext(reader, Some(ctx))?;
    scratch.compute_units_consumed = Option::decode_ext(reader, Some(ctx))?;
    scratch.cost_units = Option::decode_ext(reader, Some(ctx))?;

    let au_count = u64::decode_ext(reader, Some(ctx))?;
    for _ in 0..au_count {
        let pubkey = Address::decode_ext(reader, Some(ctx))?;
        let lamports = u64::decode_ext(reader, Some(ctx))?;
        let owner = Address::decode_ext(reader, Some(ctx))?;
        let executable = bool::decode_ext(reader, Some(ctx))?;
        let rent_epoch = u64::decode_ext(reader, Some(ctx))?;
        let write_version = u64::decode_ext(reader, Some(ctx))?;
        diff.set_key(account_diff_key(&pubkey));
        let data = diff.decode_blob(reader)?;
        scratch
            .push_account_update(&AccountUpdateView {
                pubkey,
                lamports,
                owner,
                executable,
                rent_epoch,
                write_version,
                data: &data,
            })
            .map_err(|_| ArchiveFormatError::Encode(lencode::io::Error::InvalidData))?;
    }
    Ok(())
}

/// Reads a lencode varint directly from a `std::io::Read` stream.
fn read_io_varint(r: &mut impl std::io::Read) -> Result<u64, ArchiveFormatError> {
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
