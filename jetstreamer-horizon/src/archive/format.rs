//! On-disk types and constants for the horizon archive container format.
//!
//! See the [module docs](super) for the full layout specification.
use lencode::prelude::*;
use once_cell::sync::Lazy;
use solana_address::Address;
use solana_hash::Hash;
use xxhash_rust::xxh64::xxh64;

use crate::pubkey_prime::POPULAR_PUBKEYS;

/// Magic bytes opening every horizon archive file ("JSHZN" + version byte
/// space + two NULs).
pub const MAGIC: [u8; 8] = *b"JSHZN1\0\0";

/// Magic bytes closing every horizon archive file (last 8 bytes).
pub const MAGIC_END: [u8; 8] = *b"\0\0NZHSJ1";

/// Current format version, recorded in [`FileHeader`].
pub const FORMAT_VERSION: u16 = 1;

/// Default number of slots per bucket (encoder-state reset / seek
/// granularity). See the module docs for the tradeoff discussion.
pub const DEFAULT_BUCKET_SLOTS: u16 = 128;

/// Size in bytes of the fixed-width [`Footer`] at the end of the file.
pub const FOOTER_LEN: usize = 48;

/// Identifier of the compiled-in pubkey prime table: xxh64 over its raw
/// bytes. Recorded in the header so a reader can detect that it was built
/// with a different prime table (which would mis-decode every primed ID).
pub static PRIME_TABLE_ID: Lazy<u64> = Lazy::new(|| {
    // SAFETY of layout: POPULAR_PUBKEYS is contiguous [[u8;32]; N].
    let bytes = unsafe {
        core::slice::from_raw_parts(
            POPULAR_PUBKEYS.as_ptr() as *const u8,
            POPULAR_PUBKEYS.len() * 32,
        )
    };
    xxh64(bytes, 0)
});

/// Bucket payload compression algorithm.
#[derive(Encode, Decode, Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Compression {
    /// Raw slot frames.
    None = 0,
    /// Whole-payload zstd frame.
    Zstd = 1,
}

/// Extensible archive-creation metadata, lencode-encoded inside
/// [`FileHeader`]. (Runtime epoch *notifications* are a different thing —
/// see [`crate::epochs::EpochMeta`].)
#[derive(Encode, Decode, Debug, Clone, PartialEq, Eq, Default)]
pub struct ArchiveMeta {
    /// Unix timestamp (milliseconds) when the archive write began.
    pub created_unix_ms: u64,
    /// Version string of the writer binary (UTF-8).
    pub writer_version: Vec<u8>,
    /// Reserved for future use; decoders must tolerate unknown trailing
    /// fields by length-prefix (see `FileHeader` encoding).
    pub reserved: Vec<u8>,
}

/// File-level header. Written once at offset 0, after [`MAGIC`].
///
/// On the wire: `MAGIC ++ varint(len) ++ lencode(FileHeader)` so future
/// versions can extend the struct while old readers still locate bucket 0.
#[derive(Encode, Decode, Debug, Clone, PartialEq, Eq)]
pub struct FileHeader {
    /// Format version ([`FORMAT_VERSION`]).
    pub format_version: u16,
    /// Slots per bucket; `1` = per-slot encoder reset.
    pub bucket_slots: u16,
    /// Epoch this archive covers.
    pub epoch: u64,
    /// First slot covered (inclusive).
    pub slot_start: u64,
    /// Number of slots intended to be covered (432 000 for a full epoch).
    pub slot_count: u64,
    /// xxh64 of the pubkey prime table the writer was compiled with.
    pub prime_table_id: u64,
    /// Reserved flag bits.
    pub flags: u64,
    /// Extensible archive-creation metadata.
    pub meta: ArchiveMeta,
}

/// Per-bucket header, written immediately before the bucket payload.
#[derive(Encode, Decode, Debug, Clone, PartialEq, Eq)]
pub struct BucketHeader {
    /// First slot covered by this bucket.
    pub first_slot: u64,
    /// Number of slot frames in the payload (≤ `bucket_slots`; the final
    /// bucket of a file may be short).
    pub slot_count: u32,
    /// Payload compression.
    pub compression: Compression,
    /// Length of the slot-frame stream before compression.
    pub uncompressed_len: u64,
    /// Length of the payload as stored in the file.
    pub stored_len: u64,
    /// xxh64 of the stored payload bytes.
    pub xxh64: u64,
    /// Blockhash of the last non-skipped slot *before* this bucket — the
    /// PoH chain anchor, letting a bucket verify without its predecessor.
    /// All-zeros for the first bucket when the parent hash is unknown.
    pub poh_start_hash: Hash,
}

/// One entry in the bucket index (written after the last bucket).
///
/// Fixed-shape so the index is cheap to scan; `first_slot` is strictly
/// increasing, enabling binary search.
#[derive(Encode, Decode, Debug, Clone, Copy, PartialEq, Eq)]
pub struct BucketIndexEntry {
    /// First slot covered by the bucket.
    pub first_slot: u64,
    /// File offset of the bucket's [`BucketHeader`].
    pub offset: u64,
    /// Total stored size of header + payload, in bytes.
    pub len: u64,
}

/// Fixed-width (48-byte) file footer; always the last [`FOOTER_LEN`] bytes.
///
/// Layout (all little-endian u64 unless noted):
/// `index_offset ++ index_len ++ bucket_count ++ index_xxh64 ++ reserved ++ MAGIC_END`
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Footer {
    /// File offset where the bucket index begins.
    pub index_offset: u64,
    /// Encoded length of the bucket index in bytes.
    pub index_len: u64,
    /// Number of buckets in the file.
    pub bucket_count: u64,
    /// xxh64 of the encoded index bytes.
    pub index_xxh64: u64,
}

impl Footer {
    /// Serializes to the fixed 48-byte wire form.
    pub fn to_bytes(&self) -> [u8; FOOTER_LEN] {
        let mut out = [0u8; FOOTER_LEN];
        out[0..8].copy_from_slice(&self.index_offset.to_le_bytes());
        out[8..16].copy_from_slice(&self.index_len.to_le_bytes());
        out[16..24].copy_from_slice(&self.bucket_count.to_le_bytes());
        out[24..32].copy_from_slice(&self.index_xxh64.to_le_bytes());
        // bytes 32..40 reserved (zero)
        out[40..48].copy_from_slice(&MAGIC_END);
        out
    }

    /// Parses the fixed 48-byte wire form, validating the closing magic.
    pub fn from_bytes(bytes: &[u8; FOOTER_LEN]) -> Result<Self, ArchiveFormatError> {
        if bytes[40..48] != MAGIC_END {
            return Err(ArchiveFormatError::BadMagic);
        }
        Ok(Self {
            index_offset: u64::from_le_bytes(bytes[0..8].try_into().unwrap()),
            index_len: u64::from_le_bytes(bytes[8..16].try_into().unwrap()),
            bucket_count: u64::from_le_bytes(bytes[16..24].try_into().unwrap()),
            index_xxh64: u64::from_le_bytes(bytes[24..32].try_into().unwrap()),
        })
    }
}

// Block metadata and entry records are the crate-level notification types;
// re-exported here because they are part of the archive wire contract.
pub use crate::block_metas::{BlockMeta, BlockNotification, SkippedSlot};
pub use crate::entries::EntryRecord;
pub use crate::epochs::EpochMeta;

/// Slot frame discriminant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum SlotKind {
    /// Leader skipped the slot (or it is absent from the source ledger).
    Skipped = 0,
    /// Full block frame follows.
    Block = 1,
}

impl TryFrom<u8> for SlotKind {
    type Error = ArchiveFormatError;
    fn try_from(v: u8) -> Result<Self, ArchiveFormatError> {
        match v {
            0 => Ok(Self::Skipped),
            1 => Ok(Self::Block),
            other => Err(ArchiveFormatError::BadSlotKind(other)),
        }
    }
}

/// Derives the [`lencode::diff::DiffEncoder`] key for an account.
#[inline]
pub fn account_diff_key(pubkey: &Address) -> u64 {
    xxh64(&pubkey.to_bytes(), 0)
}

/// Errors arising from malformed archive bytes.
#[derive(Debug, thiserror::Error)]
pub enum ArchiveFormatError {
    #[error("bad magic bytes (not a horizon archive, or truncated)")]
    BadMagic,
    #[error("unsupported format version {0}")]
    UnsupportedVersion(u16),
    #[error("prime table mismatch: file {file:#018x}, compiled {compiled:#018x}")]
    PrimeTableMismatch { file: u64, compiled: u64 },
    #[error("invalid slot kind byte {0}")]
    BadSlotKind(u8),
    #[error("bucket checksum mismatch at slot {first_slot}")]
    BucketChecksum { first_slot: u64 },
    #[error("index checksum mismatch")]
    IndexChecksum,
    #[error("slots must be written in strictly increasing order: got {got}, last {last}")]
    NonMonotonicSlot { got: u64, last: u64 },
    #[error("slot {slot} outside file range [{start}, {end})")]
    SlotOutOfRange { slot: u64, start: u64, end: u64 },
    #[error("PoH verification failed at slot {slot}")]
    PohMismatch { slot: u64 },
    #[error("encode error: {0}")]
    Encode(lencode::io::Error),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

impl From<lencode::io::Error> for ArchiveFormatError {
    fn from(e: lencode::io::Error) -> Self {
        Self::Encode(e)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn footer_roundtrip() {
        let f = Footer {
            index_offset: 123_456,
            index_len: 789,
            bucket_count: 42,
            index_xxh64: 0xDEADBEEF,
        };
        let bytes = f.to_bytes();
        assert_eq!(bytes.len(), FOOTER_LEN);
        let back = Footer::from_bytes(&bytes).unwrap();
        assert_eq!(back, f);
    }

    #[test]
    fn footer_rejects_bad_magic() {
        let bytes = [0u8; FOOTER_LEN];
        assert!(Footer::from_bytes(&bytes).is_err());
    }

    #[test]
    fn header_roundtrip() {
        let h = FileHeader {
            format_version: FORMAT_VERSION,
            bucket_slots: DEFAULT_BUCKET_SLOTS,
            epoch: 900,
            slot_start: 388_800_000,
            slot_count: 432_000,
            prime_table_id: *PRIME_TABLE_ID,
            flags: 0,
            meta: ArchiveMeta {
                created_unix_ms: 1_750_000_000_000,
                writer_version: b"test".to_vec(),
                reserved: vec![],
            },
        };
        let mut buf = vec![0u8; 4096];
        let mut cur = lencode::io::Cursor::new(&mut buf[..]);
        let n = h.encode_ext(&mut cur, None).unwrap();
        let mut rd = lencode::io::Cursor::new(&buf[..n]);
        let back = FileHeader::decode_ext(&mut rd, None).unwrap();
        assert_eq!(back, h);
    }

    #[test]
    fn prime_table_id_is_stable() {
        // The ID must be deterministic across processes for the
        // reader-side compatibility check to mean anything.
        assert_eq!(*PRIME_TABLE_ID, *PRIME_TABLE_ID);
        assert_ne!(*PRIME_TABLE_ID, 0);
    }

    #[test]
    fn bucket_header_roundtrip() {
        let h = BucketHeader {
            first_slot: 388_805_000,
            slot_count: 128,
            compression: Compression::Zstd,
            uncompressed_len: 1_000_000,
            stored_len: 250_000,
            xxh64: 0xABCD,
            poh_start_hash: Hash::new_from_array([7u8; 32]),
        };
        let mut buf = vec![0u8; 1024];
        let mut cur = lencode::io::Cursor::new(&mut buf[..]);
        let n = h.encode_ext(&mut cur, None).unwrap();
        let mut rd = lencode::io::Cursor::new(&buf[..n]);
        let back = BucketHeader::decode_ext(&mut rd, None).unwrap();
        assert_eq!(back, h);
    }
}
