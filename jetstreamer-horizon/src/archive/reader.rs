//! Streaming reader for the horizon archive container format over any
//! `Read + Seek` source.
//!
//! Framing (header, footer, bucket index) is read here; slot-frame decoding
//! is delegated to [`BucketDecoder`](super::BucketDecoder), so this sync
//! reader and any async/network driver share one byte-identical decode
//! path. Mirrors [`ArchiveWriter`](super::ArchiveWriter)'s wire layout.
use lencode::prelude::*;

use super::bucket::{
    BucketDecoder, PayloadByteStats, SlotVisitor, bucket_containing, parse_bucket_index,
    read_io_varint,
};
use super::format::*;

/// Streaming archive reader over any `Read + Seek` source.
pub struct ArchiveReader<R: std::io::Read + std::io::Seek> {
    source: R,
    header: FileHeader,
    index: Vec<BucketIndexEntry>,
    /// Verify blockhash chain continuity (parent_blockhash linkage) while
    /// streaming. Forwarded to the [`BucketDecoder`] on every read.
    pub verify_chain: bool,

    /// Index of the currently loaded bucket, if any.
    current_bucket: Option<usize>,
    /// Number of bucket loads performed (seek + checksum + decompress).
    /// Diagnostic: sequential forward reads should keep this near
    /// `bucket_count`, not `O(read_slots calls)`.
    bucket_loads: u64,
    decoder: BucketDecoder,
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
        let index = parse_bucket_index(&index_bytes, &footer)?;

        Ok(Self {
            source,
            header,
            index,
            verify_chain: false,
            current_bucket: None,
            bucket_loads: 0,
            decoder: BucketDecoder::new(),
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
    /// (their bytes must flow through the dedupe/diff decoders to reproduce
    /// encoder state) — "start from the nearest slot before the target and
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
        let target_bucket = bucket_containing(&self.header, &self.index, start_slot);
        let continue_in_place = match self.current_bucket {
            // Right bucket already loaded and we haven't decoded past the
            // target: keep streaming from where we are.
            Some(cur) if cur == target_bucket => self
                .decoder
                .last_decoded_slot()
                .is_none_or(|last| last < start_slot),
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
        let mut raw = vec![0u8; entry.len as usize];
        self.source.read_exact(&mut raw)?;
        self.decoder.load_bucket_bytes(&raw)?;
        self.current_bucket = Some(idx);
        self.bucket_loads += 1;
        Ok(())
    }
}
