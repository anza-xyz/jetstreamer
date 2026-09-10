//! Streaming reader for the horizon archive container format over any
//! `Read + Seek` source.
//!
//! Framing (header, footer, bucket index) is read here; slot-frame decoding
//! is delegated to [`BucketDecoder`](super::BucketDecoder), so this sync
//! reader and any async/network driver share one byte-identical decode
//! path. Mirrors [`ArchiveWriter`](super::ArchiveWriter)'s wire layout.
use lencode::prelude::*;

use super::bucket::{
    BucketDecoder, ChainMismatchPolicy, PayloadByteStats, SlotVisitor, bucket_containing,
    decode_file_header_bytes, parse_bucket_index, read_io_varint, validate_bucket_index_layout,
    validate_footer_layout,
};
use super::format::*;
use super::provenance::{ArchiveProvenance, ArchiveProvenanceError};

/// Streaming archive reader over any `Read + Seek` source.
pub struct ArchiveReader<R: std::io::Read + std::io::Seek> {
    source: R,
    header: FileHeader,
    index: Vec<BucketIndexEntry>,
    /// Verify blockhash chain continuity (parent_blockhash linkage) while
    /// streaming. Forwarded to the [`BucketDecoder`] on every read.
    pub verify_chain: bool,
    /// Policy for a detected parent-blockhash mismatch. This is consulted only
    /// when [`Self::verify_chain`] is enabled and defaults to strict rejection.
    pub chain_mismatch_policy: ChainMismatchPolicy,

    /// Index of the currently loaded bucket, if any.
    current_bucket: Option<usize>,
    /// Number of bucket loads performed (seek + checksum + decompress).
    /// Diagnostic: sequential forward reads should keep this near
    /// `bucket_count`, not `O(read_slots calls)`.
    bucket_loads: u64,
    /// Reused stored-frame buffer. Epoch conversion reads thousands of large
    /// buckets; retaining this allocation avoids a fresh allocation/free
    /// cycle for every bucket while the decoder separately reuses its
    /// decompressed payload buffer.
    stored_bucket: Vec<u8>,
    decoder: BucketDecoder,
}

impl<R: std::io::Read + std::io::Seek> ArchiveReader<R> {
    /// Opens an archive: validates magic, reads the file header, footer,
    /// and bucket index, and checks prime-table compatibility.
    pub fn open(mut source: R) -> Result<Self, ArchiveFormatError> {
        use std::io::SeekFrom;

        let file_len = source.seek(SeekFrom::End(0))?;
        if file_len < (MAGIC.len() + FOOTER_LEN) as u64 {
            return Err(ArchiveFormatError::InvalidContainerLayout(
                "file is shorter than its fixed framing",
            ));
        }

        // --- file header ---
        source.seek(SeekFrom::Start(0))?;
        let mut magic = [0u8; 8];
        source.read_exact(&mut magic)?;
        if magic != MAGIC {
            return Err(ArchiveFormatError::BadMagic);
        }
        let header_len = read_io_varint(&mut source)?;
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
        source.read_exact(&mut header_bytes)?;
        let header_end = source.stream_position()?;
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

        // --- footer + index ---
        source.seek(SeekFrom::End(-(FOOTER_LEN as i64)))?;
        let mut footer_bytes = [0u8; FOOTER_LEN];
        source.read_exact(&mut footer_bytes)?;
        let footer = Footer::from_bytes(&footer_bytes)?;
        validate_footer_layout(file_len, header_end, &footer)?;

        source.seek(SeekFrom::Start(footer.index_offset))?;
        let index_len = usize::try_from(footer.index_len).map_err(|_| {
            ArchiveFormatError::InvalidContainerLayout("index length does not fit usize")
        })?;
        let mut index_bytes = Vec::new();
        try_reserve_archive_buffer(&mut index_bytes, index_len, "bucket index")?;
        index_bytes.resize(index_len, 0);
        source.read_exact(&mut index_bytes)?;
        let index = parse_bucket_index(&index_bytes, &footer)?;
        validate_bucket_index_layout(&header, header_end, footer.index_offset, &index)?;
        let decoder = BucketDecoder::for_file_header(&header)?;

        Ok(Self {
            source,
            header,
            index,
            verify_chain: false,
            chain_mismatch_policy: ChainMismatchPolicy::Reject,
            current_bucket: None,
            bucket_loads: 0,
            stored_bucket: Vec::new(),
            decoder,
        })
    }

    /// The archive's file header.
    pub fn header(&self) -> &FileHeader {
        &self.header
    }

    /// Explicitly parses the optional, versioned generation provenance in the
    /// file header. Opening an archive does not parse this field, preserving
    /// compatibility with old empty-reserved archives and opaque future uses.
    pub fn provenance(&self) -> Result<Option<ArchiveProvenance>, ArchiveProvenanceError> {
        let provenance = self.header.meta.provenance()?;
        if let Some(value) = &provenance {
            value.validate_for_archive(self.header.slot_start, self.header.slot_count)?;
        }
        Ok(provenance)
    }

    /// Number of buckets in the archive.
    pub fn bucket_count(&self) -> usize {
        self.index.len()
    }

    /// Returns whether indexed bucket headers cover every declared slot
    /// exactly once, without decompressing their payloads.
    ///
    /// This is stricter than comparing the index count and first-slot keys:
    /// an interrupted writer can leave the expected bucket keys while one of
    /// those buckets contains fewer frames than its declared range requires.
    pub fn has_complete_slot_coverage(&mut self) -> Result<bool, ArchiveFormatError> {
        use std::io::SeekFrom;

        let expected_buckets = self
            .header
            .slot_count
            .div_ceil(u64::from(self.header.bucket_slots));
        if u64::try_from(self.index.len()).ok() != Some(expected_buckets) {
            return Ok(false);
        }
        let slot_end = self.header.slot_start + self.header.slot_count;
        for (index, entry) in self.index.iter().enumerate() {
            let expected_first = self.header.slot_start
                + u64::try_from(index).map_err(|_| {
                    ArchiveFormatError::InvalidContainerLayout("bucket index does not fit u64")
                })? * u64::from(self.header.bucket_slots);
            let expected_count =
                (slot_end - expected_first).min(u64::from(self.header.bucket_slots));
            self.source.seek(SeekFrom::Start(entry.offset))?;
            let mut indexed_bytes = std::io::Read::take(&mut self.source, entry.len);
            let bucket = BucketHeader::decode_ext(&mut indexed_bytes, None)?;
            if bucket.first_slot != expected_first || u64::from(bucket.slot_count) != expected_count
            {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// The archive's bucket index (`first_slot`, byte `offset`, byte `len`
    /// per bucket). Useful for an external driver that fetches and decodes
    /// buckets itself via a [`BucketDecoder`](super::BucketDecoder).
    pub fn bucket_index(&self) -> &[BucketIndexEntry] {
        &self.index
    }

    /// Number of bucket loads (seek + checksum + decompress) performed so
    /// far. Sequential forward consumption should keep this near the number
    /// of distinct buckets traversed, independent of how many `read_slots`
    /// calls were made.
    pub fn bucket_loads(&self) -> u64 {
        self.bucket_loads
    }

    /// Per-category breakdown of the uncompressed-payload bytes decoded so far
    /// (transaction-field bytes vs account-update bytes vs everything else).
    /// After a full pass over the archive this is the whole file's split.
    pub fn payload_byte_stats(&self) -> PayloadByteStats {
        self.decoder.byte_stats()
    }

    /// Number of zero-parent chain mismatches accepted under
    /// [`ChainMismatchPolicy::AllowZeroParentResume`] across decoded frames.
    pub fn zero_parent_resume_artifacts(&self) -> u64 {
        self.decoder.zero_parent_resume_artifacts()
    }

    /// Header of the currently loaded bucket, if any.
    pub(crate) fn current_bucket_header(&self) -> Option<&BucketHeader> {
        self.decoder.current_bucket_header()
    }

    /// Decodes exactly one indexed bucket from its first frame through its
    /// last, emitting every frame to `visitor` in wire order.
    ///
    /// Unlike [`Self::read_slots`], this never spills into the next bucket.
    /// It is useful for parallel readers, deterministic archive sampling,
    /// and re-encoding because all stateful codecs reset at bucket
    /// boundaries.
    pub fn read_bucket<V: SlotVisitor>(
        &mut self,
        index: usize,
        visitor: &mut V,
    ) -> Result<u64, ArchiveFormatError> {
        self.read_bucket_with_header(index, visitor, |_, _| Ok(()))
    }

    /// Decodes one bucket after giving an internal caller access to its
    /// validated header, before any slot callbacks can advance destination
    /// state. The re-encoder uses this to seed a changed bucket geometry with
    /// the source archive's initial PoH anchor.
    pub(crate) fn read_bucket_with_header<V, F>(
        &mut self,
        index: usize,
        visitor: &mut V,
        before_slots: F,
    ) -> Result<u64, ArchiveFormatError>
    where
        V: SlotVisitor,
        F: FnOnce(&BucketHeader, &mut V) -> Result<(), ArchiveFormatError>,
    {
        if index >= self.index.len() {
            return Err(ArchiveFormatError::BucketOutOfRange {
                index,
                count: self.index.len(),
            });
        }
        self.decoder.verify_chain = self.verify_chain;
        self.decoder.chain_mismatch_policy = self.chain_mismatch_policy;
        let consumption = visitor.consumption();
        self.decoder.materialize_account_data = consumption.account_update_data;
        self.decoder.materialize_block_account_update_arenas =
            consumption.block_account_update_arenas;
        self.load_bucket(index)?;
        let bucket_header = self.decoder.current_bucket_header().ok_or(
            ArchiveFormatError::InvalidContainerLayout(
                "reader did not retain the decoded bucket header",
            ),
        )?;
        before_slots(bucket_header, visitor)?;

        let mut visited = 0u64;
        while self.decoder.slots_remaining() > 0 {
            // Every valid slot is >= 0, so every decoded frame is emitted
            // while the bucket boundary remains hard.
            if self.decoder.decode_slot_frame(0, visitor)? {
                visited += 1;
            }
        }
        Ok(visited)
    }

    /// Streams slots to `visitor`, starting at the first stored slot ≥
    /// `start_slot`, for at most `max_slots` slot frames. Returns the
    /// number of slot frames visited.
    ///
    /// The reader is stateful: when `start_slot` lies ahead of the current
    /// decode position (the common sequential-consumption pattern), it
    /// simply continues streaming forward with no re-seek, re-decompression,
    /// no re-decode. A bucket is (re)loaded only when the target is in a
    /// different bucket or behind the current position. Frames between the
    /// current position and `start_slot` are decoded without emission
    /// (their bytes must flow through the dedupe/diff decoders to reproduce
    /// encoder state): "start from the nearest slot before the target and
    /// stream through".
    pub fn read_slots<V: SlotVisitor>(
        &mut self,
        start_slot: u64,
        max_slots: u64,
        visitor: &mut V,
    ) -> Result<u64, ArchiveFormatError> {
        if self.index.is_empty() || max_slots == 0 {
            return Ok(0);
        }
        self.decoder.verify_chain = self.verify_chain;
        self.decoder.chain_mismatch_policy = self.chain_mismatch_policy;
        let consumption = visitor.consumption();
        self.decoder.materialize_account_data = consumption.account_update_data;
        self.decoder.materialize_block_account_update_arenas =
            consumption.block_account_update_arenas;
        let target_bucket = bucket_containing(&self.header, &self.index, start_slot);
        let continue_in_place = match self.current_bucket {
            // Right bucket already loaded and we haven't decoded past the
            // target: keep streaming from where we are unless the loaded
            // bucket was latched with a different account-data
            // materialization mode than this visitor wants, in which case a
            // reload re-decodes it under the right mode (diff state is
            // bucket-scoped, so a bytes-wanting visitor must not continue
            // inside a bucket whose earlier records were skipped).
            Some(cur) if cur == target_bucket => {
                self.decoder.bucket_materializes_account_data()
                    == self.decoder.materialize_account_data
                    && self
                        .decoder
                        .last_decoded_slot()
                        .is_none_or(|last| last < start_slot)
            }
            // Target is in a later bucket: jumping is strictly cheaper than
            // streaming through. Encoder state resets per bucket, so the
            // intermediate buckets contribute nothing.
            _ => false,
        };
        if !continue_in_place {
            self.load_bucket(target_bucket)?;
        }

        let mut visited = 0u64;
        while visited < max_slots {
            if self.decoder.slots_remaining() == 0 {
                let Some(cur) = self.current_bucket else {
                    break;
                };
                if cur + 1 >= self.index.len() {
                    break;
                }
                self.load_bucket(cur + 1)?;
            }
            if self.decoder.decode_slot_frame(start_slot, visitor)? {
                visited += 1;
            }
        }
        Ok(visited)
    }

    /// Reads bucket `idx`'s raw bytes from the source and hands them to the
    /// decoder, which validates and decompresses them.
    fn load_bucket(&mut self, idx: usize) -> Result<(), ArchiveFormatError> {
        use std::io::SeekFrom;
        let entry = self.index[idx];
        self.source.seek(SeekFrom::Start(entry.offset))?;
        let raw_len = usize::try_from(entry.len).map_err(|_| {
            ArchiveFormatError::InvalidContainerLayout("bucket length does not fit usize")
        })?;
        try_reserve_archive_buffer(&mut self.stored_bucket, raw_len, "stored bucket")?;
        self.stored_bucket.resize(raw_len, 0);
        self.source.read_exact(&mut self.stored_bucket)?;
        self.decoder
            .load_indexed_bucket_bytes(&self.stored_bucket, entry)?;
        self.current_bucket = Some(idx);
        self.bucket_loads += 1;
        Ok(())
    }
}
