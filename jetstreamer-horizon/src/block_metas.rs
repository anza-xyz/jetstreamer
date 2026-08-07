//! Zero-allocation block notification types.
//!
//! [`BlockNotification`] is the slot-level event: either a full
//! [`BlockMeta`] or a [`SkippedSlot`] (leader skipped the slot) — skipped
//! slots are a first-class variant rather than a separate channel so
//! consumers see exactly one notification per slot.
//!
//! [`BlockMeta`] groups everything the runtime attributes to the block
//! itself rather than to any transaction: ledger metadata (hashes, time,
//! height, counts), the block reward list, and the two phases of
//! runtime-direct ("orphan") account updates — writes the bank performs
//! with no owning transaction:
//!
//! * [`pre_updates`](BlockMeta::pre_updates) — slot-start writes performed
//!   during `Bank::new_from_parent`, before any transaction executes:
//!   sysvar rewrites (`Clock`, `SlotHashes`, …), feature activations, and
//!   partitioned epoch-reward credits.
//! * [`post_updates`](BlockMeta::post_updates) — freeze-time writes after
//!   all transactions: transaction-fee distribution to the leader,
//!   incinerator burns, and historical rent collection.
//!
//! Keeping the two phases separate preserves true application order: an
//! account can be written by a slot-start sysvar update, then by
//! transactions, then by fee distribution — and a consumer reconstructing
//! state must apply them in that order. (`write_version` on every update
//! provides the authoritative total order as a cross-check.)
//!
//! Like [`Transaction`](crate::transactions::Transaction), instances are
//! large (~40 MiB, dominated by the orphan-update arenas) and must be
//! heap-allocated via [`BlockNotification::new_boxed`] /
//! [`BlockMeta::new_boxed`] and reused.
use lencode::prelude::*;
use solana_hash::Hash;

use crate::account_updates::AccountUpdates;
use crate::limits::{
    MAX_BLOCK_REWARDS, MAX_SLOT_POST_UPDATE_DATA, MAX_SLOT_POST_UPDATES, MAX_SLOT_PRE_UPDATE_DATA,
    MAX_SLOT_PRE_UPDATES,
};
use crate::transactions::Reward;
use crate::zero_vec::{ZeroAlloc, ZeroVec, assert_zero_alloc};

/// Arena type for a block's slot-start (pre-transaction) orphan updates.
pub type PreUpdates = AccountUpdates<MAX_SLOT_PRE_UPDATES, MAX_SLOT_PRE_UPDATE_DATA>;

/// Arena type for a block's freeze-time (post-transaction) orphan updates.
pub type PostUpdates = AccountUpdates<MAX_SLOT_POST_UPDATES, MAX_SLOT_POST_UPDATE_DATA>;

/// Marker for a leader-skipped slot.
#[derive(Encode, Decode, Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(C)]
pub struct SkippedSlot {
    /// The slot that produced no block.
    pub slot: u64,
}

/// Block-level metadata plus the block's runtime-direct account updates.
///
/// `Clone` is intentionally **not** derived — the orphan arenas make this
/// a ~40 MiB struct; cloning through the stack would overflow.
#[derive(Encode, Decode, Debug, PartialEq, Eq)]
#[repr(C)]
pub struct BlockMeta {
    /// The block's slot.
    pub slot: u64,
    /// Parent slot number.
    pub parent_slot: u64,
    /// Parent block's blockhash (PoH chain anchor for this slot).
    pub parent_blockhash: Hash,
    /// This block's blockhash (== hash of the slot's final entry).
    pub blockhash: Hash,
    /// Optional Unix timestamp.
    pub block_time: Option<i64>,
    /// Optional ledger block height.
    pub block_height: Option<u64>,
    /// Number of executed transactions in the block.
    pub executed_transaction_count: u64,
    /// Number of PoH entries in the block.
    pub entry_count: u64,
    /// Rewards issued in this block (epoch-boundary slots).
    pub rewards: ZeroVec<MAX_BLOCK_REWARDS, Reward>,
    /// Number of reward partitions, when partitioned rewards are active.
    pub num_partitions: Option<u64>,
    /// Runtime-direct account updates applied at slot start, before any
    /// transaction (sysvars, feature activations, epoch-reward credits).
    pub pre_updates: PreUpdates,
    /// Runtime-direct account updates applied at freeze, after all
    /// transactions (fee distribution, incinerator, historical rent).
    pub post_updates: PostUpdates,
}

impl Default for BlockMeta {
    /// **Warning:** ~40 MiB by value; prefer [`Self::new_boxed`].
    fn default() -> Self {
        Self {
            slot: 0,
            parent_slot: 0,
            parent_blockhash: Hash::default(),
            blockhash: Hash::default(),
            block_time: None,
            block_height: None,
            executed_transaction_count: 0,
            entry_count: 0,
            rewards: ZeroVec::new(),
            num_partitions: None,
            pre_updates: AccountUpdates::default(),
            post_updates: AccountUpdates::default(),
        }
    }
}

impl BlockMeta {
    /// Allocates a fresh zero-initialised `BlockMeta` directly on the heap,
    /// without routing the ~40 MiB struct through the stack.
    pub fn new_boxed() -> Box<Self> {
        use std::alloc::{Layout, alloc_zeroed, handle_alloc_error};
        let layout = Layout::new::<Self>();
        // SAFETY: every field accepts the all-zero bit pattern as its
        // default state (scalars, Option::None, Hash zeroes, empty
        // ZeroVec / AccountUpdates).
        unsafe {
            let ptr = alloc_zeroed(layout) as *mut Self;
            if ptr.is_null() {
                handle_alloc_error(layout);
            }
            Box::from_raw(ptr)
        }
    }

    /// In-place reset to the default state without a stack temporary.
    pub fn clear(&mut self) {
        self.slot = 0;
        self.parent_slot = 0;
        self.parent_blockhash = Hash::default();
        self.blockhash = Hash::default();
        self.block_time = None;
        self.block_height = None;
        self.executed_transaction_count = 0;
        self.entry_count = 0;
        self.rewards.clear();
        self.num_partitions = None;
        self.pre_updates.clear();
        self.post_updates.clear();
    }

    /// Decodes the wire form into `self` without stack-allocating the
    /// struct. Field order matches `#[derive(Encode, Decode)]`.
    pub fn decode_into<R: Read>(
        &mut self,
        reader: &mut R,
        mut ctx: Option<&mut lencode::context::DecoderContext>,
    ) -> lencode::Result<()> {
        self.slot = u64::decode_ext(reader, ctx.as_deref_mut())?;
        self.parent_slot = u64::decode_ext(reader, ctx.as_deref_mut())?;
        self.parent_blockhash = Hash::decode_ext(reader, ctx.as_deref_mut())?;
        self.blockhash = Hash::decode_ext(reader, ctx.as_deref_mut())?;
        self.block_time = Option::<i64>::decode_ext(reader, ctx.as_deref_mut())?;
        self.block_height = Option::<u64>::decode_ext(reader, ctx.as_deref_mut())?;
        self.executed_transaction_count = u64::decode_ext(reader, ctx.as_deref_mut())?;
        self.entry_count = u64::decode_ext(reader, ctx.as_deref_mut())?;
        self.rewards.decode_into(reader, ctx.as_deref_mut())?;
        self.num_partitions = Option::<u64>::decode_ext(reader, ctx.as_deref_mut())?;
        self.pre_updates.decode_into(reader, ctx.as_deref_mut())?;
        self.post_updates.decode_into(reader, ctx)?;
        Ok(())
    }
}

/// Slot-level notification: exactly one per slot — either a full block or
/// a leader-skipped marker.
#[derive(Encode, Decode, Debug, PartialEq, Eq)]
// `#[repr(C, u8)]` pins the discriminant to byte 0, enabling the in-place
// variant swap in [`Self::decode_into`] (same technique as
// `VersionedMessage`).
//
// The variant size gap (8 B vs ~40 MiB) is intentional: boxing `BlockMeta`
// would reintroduce a heap indirection and break the zero-alloc /
// `decode_into`-in-place design. Instances are always heap-pinned via
// `new_boxed()` and reused, so the size is paid once per scratch, not per
// value.
#[allow(clippy::large_enum_variant)]
#[repr(C, u8)]
pub enum BlockNotification {
    /// Leader skipped the slot (or it is absent from the source ledger).
    Skipped(SkippedSlot) = 0,
    /// Full block.
    Block(BlockMeta) = 1,
}

impl Default for BlockNotification {
    /// **Warning:** ~40 MiB by value; prefer [`Self::new_boxed`].
    fn default() -> Self {
        Self::Skipped(SkippedSlot::default())
    }
}

impl BlockNotification {
    /// Allocates a fresh `BlockNotification` (Skipped(0)) directly on the
    /// heap without routing the ~40 MiB enum through the stack.
    pub fn new_boxed() -> Box<Self> {
        use std::alloc::{Layout, alloc_zeroed, handle_alloc_error};
        let layout = Layout::new::<Self>();
        // SAFETY: all-zero bytes = discriminant 0 (Skipped) with slot 0 —
        // a valid value.
        unsafe {
            let ptr = alloc_zeroed(layout) as *mut Self;
            if ptr.is_null() {
                handle_alloc_error(layout);
            }
            Box::from_raw(ptr)
        }
    }

    /// The slot this notification is about.
    #[inline]
    pub const fn slot(&self) -> u64 {
        match self {
            Self::Skipped(s) => s.slot,
            Self::Block(m) => m.slot,
        }
    }

    /// `true` if the slot was leader-skipped.
    #[inline]
    pub const fn is_skipped(&self) -> bool {
        matches!(self, Self::Skipped(_))
    }

    /// In-place reset: clears the current variant's contents without
    /// stack-allocating a new variant.
    pub fn clear(&mut self) {
        match self {
            Self::Skipped(s) => *s = SkippedSlot::default(),
            Self::Block(m) => m.clear(),
        }
    }

    /// Decodes the wire form into `self` without stack-allocating the
    /// ~40 MiB enum, swapping variants in place when the incoming
    /// discriminant differs from the active one.
    pub fn decode_into<R: Read>(
        &mut self,
        reader: &mut R,
        ctx: Option<&mut lencode::context::DecoderContext>,
    ) -> lencode::Result<()> {
        let disc = <Self as Decode>::decode_discriminant(reader)?;
        match disc {
            0 => {
                if !matches!(self, Self::Skipped(_)) {
                    // SAFETY: `#[repr(C, u8)]` puts the discriminant at byte
                    // 0; zeroing the storage yields Skipped(slot=0), valid.
                    unsafe {
                        core::ptr::drop_in_place(self as *mut Self);
                        core::ptr::write_bytes(self as *mut Self, 0, 1);
                    }
                }
                match self {
                    Self::Skipped(s) => {
                        s.slot = u64::decode_ext(reader, ctx)?;
                        Ok(())
                    }
                    _ => unreachable!(),
                }
            }
            1 => {
                if !matches!(self, Self::Block(_)) {
                    // SAFETY: as above; then flip the discriminant byte to 1
                    // (Block). All-zero BlockMeta payload is valid.
                    unsafe {
                        core::ptr::drop_in_place(self as *mut Self);
                        core::ptr::write_bytes(self as *mut Self, 0, 1);
                        *(self as *mut Self as *mut u8) = 1;
                    }
                }
                match self {
                    Self::Block(m) => m.decode_into(reader, ctx),
                    _ => unreachable!(),
                }
            }
            _ => Err(lencode::io::Error::InvalidData),
        }
    }
}

// --- ZeroAlloc proofs ---

impl ZeroAlloc for SkippedSlot {}
impl ZeroAlloc for BlockMeta {}
impl ZeroAlloc for BlockNotification {}

const _: fn() = || {
    // `SkippedSlot`
    assert_zero_alloc::<u64>();

    // `BlockMeta` fields
    assert_zero_alloc::<Hash>(); // parent_blockhash / blockhash
    assert_zero_alloc::<Option<i64>>(); // block_time
    assert_zero_alloc::<Option<u64>>(); // block_height / num_partitions
    assert_zero_alloc::<ZeroVec<MAX_BLOCK_REWARDS, Reward>>(); // rewards
    assert_zero_alloc::<PreUpdates>(); // pre_updates
    assert_zero_alloc::<PostUpdates>(); // post_updates

    // Top-level types
    assert_zero_alloc::<SkippedSlot>();
    assert_zero_alloc::<BlockMeta>();
    assert_zero_alloc::<BlockNotification>();
};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::account_updates::AccountUpdateView;
    use solana_address::Address;

    fn view<'a>(pk: u8, data: &'a [u8]) -> AccountUpdateView<'a> {
        AccountUpdateView {
            pubkey: Address::new_from_array([pk; 32]),
            lamports: 1,
            owner: Address::new_from_array([2u8; 32]),
            executable: false,
            rent_epoch: 0,
            write_version: pk as u64,
            data,
        }
    }

    #[test]
    fn new_boxed_defaults() {
        let n = BlockNotification::new_boxed();
        assert!(n.is_skipped());
        assert_eq!(n.slot(), 0);

        let m = BlockMeta::new_boxed();
        assert_eq!(m.slot, 0);
        assert!(m.pre_updates.is_empty());
        assert!(m.post_updates.is_empty());
        assert!(m.rewards.is_empty());
    }

    #[test]
    fn block_meta_roundtrip_with_orphans() {
        let mut src = BlockMeta::new_boxed();
        src.slot = 1234;
        src.parent_slot = 1233;
        src.blockhash = Hash::new_from_array([7u8; 32]);
        src.parent_blockhash = Hash::new_from_array([6u8; 32]);
        src.block_time = Some(1_750_000_000);
        src.executed_transaction_count = 42;
        src.entry_count = 96;
        src.pre_updates.push(&view(1, b"clock-sysvar")).unwrap();
        src.pre_updates.push(&view(2, b"slot-hashes")).unwrap();
        src.post_updates.push(&view(3, b"leader-fees")).unwrap();

        let mut buf = vec![0u8; 1 << 16];
        let mut cur = lencode::io::Cursor::new(&mut buf[..]);
        let n = src.encode_ext(&mut cur, None).unwrap();

        let mut dst = BlockMeta::new_boxed();
        let mut rd = lencode::io::Cursor::new(&buf[..n]);
        dst.decode_into(&mut rd, None).unwrap();
        assert_eq!(*dst, *src);

        let pre: Vec<_> = dst.pre_updates.iter().map(|(_, d)| d.to_vec()).collect();
        assert_eq!(pre, vec![b"clock-sysvar".to_vec(), b"slot-hashes".to_vec()]);
    }

    #[test]
    fn notification_decode_into_swaps_variants() {
        // Encode a Block, decode into a Skipped-initialised scratch, then
        // encode a Skipped and decode back over the Block.
        let mut block = BlockMeta::new_boxed();
        block.slot = 55;
        block.blockhash = Hash::new_from_array([9u8; 32]);

        let mut buf = vec![0u8; 1 << 16];
        // Compose the enum encoding manually (discriminant + payload) so we
        // never stack-allocate a full BlockNotification by value.
        let n_block = {
            let mut cur = lencode::io::Cursor::new(&mut buf[..]);
            // discriminant 1 then BlockMeta fields — same as derived Encode.
            <BlockNotification as Encode>::encode_discriminant(1, &mut cur).unwrap()
                + block.encode_ext(&mut cur, None).unwrap()
        };

        let mut scratch = BlockNotification::new_boxed();
        let mut rd = lencode::io::Cursor::new(&buf[..n_block]);
        scratch.decode_into(&mut rd, None).unwrap();
        match &*scratch {
            BlockNotification::Block(m) => {
                assert_eq!(m.slot, 55);
                assert_eq!(m.blockhash, Hash::new_from_array([9u8; 32]));
            }
            _ => panic!("expected Block variant"),
        }

        // Now a skipped frame over the Block-occupied scratch.
        let n_skip = {
            let mut cur = lencode::io::Cursor::new(&mut buf[..]);
            <BlockNotification as Encode>::encode_discriminant(0, &mut cur).unwrap()
                + 77u64.encode_ext(&mut cur, None).unwrap()
        };
        let mut rd = lencode::io::Cursor::new(&buf[..n_skip]);
        scratch.decode_into(&mut rd, None).unwrap();
        assert!(scratch.is_skipped());
        assert_eq!(scratch.slot(), 77);
    }

    #[test]
    fn clear_resets_in_place() {
        let mut m = BlockMeta::new_boxed();
        m.slot = 9;
        m.pre_updates.push(&view(1, b"x")).unwrap();
        m.rewards.push(Reward::default());
        m.clear();
        assert_eq!(m.slot, 0);
        assert!(m.pre_updates.is_empty());
        assert!(m.rewards.is_empty());
    }

    #[test]
    fn notification_scratch_survives_many_variant_cycles() {
        // Simulates the reader's per-slot pattern: one scratch
        // BlockNotification decoded over and over, alternating between
        // skipped frames and block frames with varying orphan loads. State
        // from one decode must never leak into the next.
        let mut scratch = BlockNotification::new_boxed();
        let mut buf = vec![0u8; 1 << 20];

        for round in 0u64..50 {
            if round % 3 == 0 {
                // Skipped frame.
                let n = {
                    let mut cur = lencode::io::Cursor::new(&mut buf[..]);
                    <BlockNotification as Encode>::encode_discriminant(0, &mut cur).unwrap()
                        + round.encode_ext(&mut cur, None).unwrap()
                };
                let mut rd = lencode::io::Cursor::new(&buf[..n]);
                scratch.decode_into(&mut rd, None).unwrap();
                assert!(scratch.is_skipped());
                assert_eq!(scratch.slot(), round);
            } else {
                // Block frame with round-dependent orphan counts.
                let mut src = BlockMeta::new_boxed();
                src.slot = round;
                src.blockhash = Hash::new_from_array([round as u8; 32]);
                let n_pre = (round % 7) as u8;
                for i in 0..n_pre {
                    src.pre_updates.push(&view(i + 1, &[i; 24])).unwrap();
                }
                if round % 2 == 0 {
                    src.post_updates.push(&view(99, b"fees")).unwrap();
                }
                let n = {
                    let mut cur = lencode::io::Cursor::new(&mut buf[..]);
                    <BlockNotification as Encode>::encode_discriminant(1, &mut cur).unwrap()
                        + src.encode_ext(&mut cur, None).unwrap()
                };
                let mut rd = lencode::io::Cursor::new(&buf[..n]);
                scratch.decode_into(&mut rd, None).unwrap();
                match &*scratch {
                    BlockNotification::Block(m) => {
                        assert_eq!(*m, *src, "round {round}: decoded block must match");
                        assert_eq!(m.pre_updates.len(), n_pre as usize);
                    }
                    _ => panic!("expected Block at round {round}"),
                }
            }
        }
    }

    #[test]
    fn block_meta_rewards_at_capacity() {
        // MAX_BLOCK_REWARDS-deep reward list roundtrips intact.
        let mut src = BlockMeta::new_boxed();
        src.slot = 1;
        for i in 0..MAX_BLOCK_REWARDS {
            src.rewards.push(Reward {
                pubkey: Address::new_from_array([(i % 251) as u8; 32]),
                lamports: i as i64,
                post_balance: i as u64 * 2,
                reward_type: None,
                commission: None,
            });
        }
        let mut buf = vec![0u8; 4 << 20];
        let mut cur = lencode::io::Cursor::new(&mut buf[..]);
        let n = src.encode_ext(&mut cur, None).unwrap();

        let mut dst = BlockMeta::new_boxed();
        let mut rd = lencode::io::Cursor::new(&buf[..n]);
        dst.decode_into(&mut rd, None).unwrap();
        assert_eq!(dst.rewards.len(), MAX_BLOCK_REWARDS);
        assert_eq!(*dst, *src);
    }

    #[test]
    fn block_meta_heavy_orphan_load_roundtrip() {
        // Epoch-boundary-shaped load: ~1000 vote-state-sized pre updates.
        let payload = vec![0x5Au8; 3_762];
        let mut src = BlockMeta::new_boxed();
        src.slot = 432_000;
        for i in 0..1_000u32 {
            let mut pk = [7u8; 32];
            pk[..4].copy_from_slice(&i.to_le_bytes());
            src.pre_updates
                .push(&AccountUpdateView {
                    pubkey: Address::new_from_array(pk),
                    lamports: i as u64,
                    owner: Address::new_from_array([1u8; 32]),
                    executable: false,
                    rent_epoch: u64::MAX,
                    write_version: i as u64,
                    data: &payload,
                })
                .unwrap();
        }
        assert_eq!(src.pre_updates.data_len(), 3_762 * 1_000);

        let mut buf = vec![0u8; 8 << 20];
        let mut cur = lencode::io::Cursor::new(&mut buf[..]);
        let n = src.encode_ext(&mut cur, None).unwrap();

        let mut dst = BlockMeta::new_boxed();
        let mut rd = lencode::io::Cursor::new(&buf[..n]);
        dst.decode_into(&mut rd, None).unwrap();
        assert_eq!(*dst, *src);
    }
}
