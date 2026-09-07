//! Zero-allocation PoH entry notification types.
//!
//! [`Entry`] is the full per-entry notification (what a consumer sees when
//! streaming), mirroring the firehose `EntryData` shape. [`EntryRecord`] is
//! its compact wire sibling used inside archive slot frames: just
//! `num_hashes` + `tx_count`, ~2-4 bytes per entry instead of 40+ — the
//! entry hash is recomputable from the PoH chain (see the archive module
//! docs), and the transaction range follows from the running tx index.
//!
//! Entries do not own account updates (every account write during entry
//! replay belongs to a transaction), so these types carry no update arena.
use lencode::prelude::*;
use solana_hash::Hash;

use crate::zero_vec::{ZeroAlloc, assert_zero_alloc};

/// Full PoH entry notification.
#[derive(Encode, Decode, Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(C)]
pub struct Entry {
    /// Slot the entry belongs to.
    pub slot: u64,
    /// Index of the entry within the slot.
    pub entry_index: u32,
    /// Number of PoH hashes since the previous entry.
    pub num_hashes: u64,
    /// The entry hash. May be all-zeros when streaming from an archive
    /// without PoH verification (hashes are recomputed only in verify
    /// mode).
    pub hash: Hash,
    /// Index of the entry's first transaction within the slot.
    pub starting_transaction_index: u32,
    /// Number of transactions in this entry (0 = tick).
    pub transaction_count: u32,
}

impl Entry {
    /// `true` if this entry is a tick (carries no transactions).
    #[inline]
    pub const fn is_tick(&self) -> bool {
        self.transaction_count == 0
    }
}

/// Compact wire form of an entry stored in archive slot frames.
///
/// Pairs with the slot's transaction stream: entry *i* covers the next
/// `tx_count` transactions after those covered by entries `0..i`. The
/// entry hash is omitted (recomputable from `num_hashes` + the covered
/// transactions + the previous entry's hash).
#[derive(Encode, Decode, Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(C)]
pub struct EntryRecord {
    /// Number of PoH hashes since the previous entry.
    pub num_hashes: u64,
    /// Number of transactions in this entry (0 = tick).
    pub tx_count: u32,
}

impl EntryRecord {
    /// `true` if this entry is a tick (carries no transactions).
    #[inline]
    pub const fn is_tick(&self) -> bool {
        self.tx_count == 0
    }
}

impl From<&Entry> for EntryRecord {
    #[inline]
    fn from(e: &Entry) -> Self {
        Self {
            num_hashes: e.num_hashes,
            tx_count: e.transaction_count,
        }
    }
}

// --- ZeroAlloc proofs ---

impl ZeroAlloc for Entry {}
impl ZeroAlloc for EntryRecord {}

const _: fn() = || {
    // `Entry` fields
    assert_zero_alloc::<u64>(); // slot / num_hashes
    assert_zero_alloc::<u32>(); // entry_index / tx indices
    assert_zero_alloc::<Hash>(); // hash

    assert_zero_alloc::<Entry>();
    assert_zero_alloc::<EntryRecord>();
};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn entry_roundtrip() {
        let e = Entry {
            slot: 123,
            entry_index: 7,
            num_hashes: 12_500,
            hash: Hash::new_from_array([3u8; 32]),
            starting_transaction_index: 42,
            transaction_count: 16,
        };
        let mut buf = vec![0u8; 256];
        let mut cur = lencode::io::Cursor::new(&mut buf[..]);
        let n = e.encode_ext(&mut cur, None).unwrap();
        let mut rd = lencode::io::Cursor::new(&buf[..n]);
        let back = Entry::decode_ext(&mut rd, None).unwrap();
        assert_eq!(back, e);
        assert!(!e.is_tick());
    }

    #[test]
    fn record_from_entry() {
        let e = Entry {
            num_hashes: 800,
            transaction_count: 0,
            ..Default::default()
        };
        let r = EntryRecord::from(&e);
        assert_eq!(r.num_hashes, 800);
        assert!(r.is_tick());
    }

    #[test]
    fn record_roundtrip() {
        let r = EntryRecord {
            num_hashes: 12_500,
            tx_count: 3,
        };
        let mut buf = [0u8; 64];
        let mut cur = lencode::io::Cursor::new(&mut buf[..]);
        let n = r.encode_ext(&mut cur, None).unwrap();
        let mut rd = lencode::io::Cursor::new(&buf[..n]);
        assert_eq!(EntryRecord::decode_ext(&mut rd, None).unwrap(), r);
    }
}
