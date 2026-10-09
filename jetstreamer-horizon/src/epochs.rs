//! Zero-allocation epoch notification types.
//!
//! [`EpochMeta`] is emitted once per epoch, at its first (boundary) block.
//! It carries the epoch-level facts plus any account updates attributed
//! directly to the epoch transition itself — feature activations and
//! builtin program migrations performed by the boundary bank.
//!
//! Note the attribution split for boundary writes: the *bulk* writes of an
//! epoch transition (vote-reward credits, partitioned stake-reward
//! distribution) are slot-start writes on ordinary blocks and live in each
//! block's [`pre_updates`](crate::block_metas::BlockMeta::pre_updates) —
//! they're per-slot events spread over the distribution window. Only
//! writes that are conceptually one-per-epoch belong here.
use lencode::prelude::*;

use crate::account_updates::AccountUpdates;
use crate::limits::{MAX_EPOCH_UPDATE_DATA, MAX_EPOCH_UPDATES};
use crate::zero_vec::{ZeroAlloc, assert_zero_alloc};

/// Arena type for epoch-attributed account updates.
pub type EpochUpdates = AccountUpdates<MAX_EPOCH_UPDATES, MAX_EPOCH_UPDATE_DATA>;

/// Epoch-level notification, emitted at the epoch's first block.
///
/// `Clone` is intentionally **not** derived — the update arena makes this
/// a ~8 MiB struct; heap-allocate via [`Self::new_boxed`] and reuse.
#[derive(Encode, Decode, Debug, PartialEq, Eq)]
#[repr(C)]
pub struct EpochMeta {
    /// The epoch number beginning at this boundary.
    pub epoch: u64,
    /// First slot of the epoch (epoch_number × slots_per_epoch).
    pub start_slot: u64,
    /// Number of slots in the epoch (432 000 on mainnet).
    pub slot_count: u64,
    /// The first slot of this epoch that actually produced a block (the
    /// boundary bank's slot; later than `start_slot` when leaders skipped).
    pub first_block_slot: u64,
    /// Number of partitions the epoch's stake rewards are distributed
    /// over, when partitioned rewards are active.
    pub num_reward_partitions: Option<u64>,
    /// Account updates attributed to the epoch transition itself (feature
    /// activations, builtin migrations).
    pub updates: EpochUpdates,
}

impl Default for EpochMeta {
    /// **Warning:** ~8 MiB by value; prefer [`Self::new_boxed`].
    fn default() -> Self {
        Self {
            epoch: 0,
            start_slot: 0,
            slot_count: 0,
            first_block_slot: 0,
            num_reward_partitions: None,
            updates: AccountUpdates::default(),
        }
    }
}

impl EpochMeta {
    /// Allocates a fresh zero-initialised `EpochMeta` directly on the heap,
    /// without routing the ~8 MiB struct through the stack.
    pub fn new_boxed() -> Box<Self> {
        use std::alloc::{Layout, alloc_zeroed, handle_alloc_error};
        let layout = Layout::new::<Self>();
        // SAFETY: every field accepts the all-zero bit pattern (scalars,
        // Option::None, empty AccountUpdates).
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
        self.epoch = 0;
        self.start_slot = 0;
        self.slot_count = 0;
        self.first_block_slot = 0;
        self.num_reward_partitions = None;
        self.updates.clear();
    }

    /// Decodes the wire form into `self` without stack-allocating the
    /// struct. Field order matches `#[derive(Encode, Decode)]`.
    pub fn decode_into<R: Read>(
        &mut self,
        reader: &mut R,
        mut ctx: Option<&mut lencode::context::DecoderContext>,
    ) -> lencode::Result<()> {
        self.epoch = u64::decode_ext(reader, ctx.as_deref_mut())?;
        self.start_slot = u64::decode_ext(reader, ctx.as_deref_mut())?;
        self.slot_count = u64::decode_ext(reader, ctx.as_deref_mut())?;
        self.first_block_slot = u64::decode_ext(reader, ctx.as_deref_mut())?;
        self.num_reward_partitions = Option::<u64>::decode_ext(reader, ctx.as_deref_mut())?;
        self.updates.decode_into(reader, ctx)?;
        Ok(())
    }
}

// --- ZeroAlloc proofs ---

impl ZeroAlloc for EpochMeta {}

const _: fn() = || {
    assert_zero_alloc::<u64>(); // epoch / start_slot / slot_count / first_block_slot
    assert_zero_alloc::<Option<u64>>(); // num_reward_partitions
    assert_zero_alloc::<EpochUpdates>(); // updates
    assert_zero_alloc::<EpochMeta>();
};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::account_updates::AccountUpdateView;
    use solana_address::Address;

    #[test]
    fn roundtrip() {
        let mut src = EpochMeta::new_boxed();
        src.epoch = 900;
        src.start_slot = 388_800_000;
        src.slot_count = 432_000;
        src.first_block_slot = 388_800_001;
        src.num_reward_partitions = Some(341);
        src.updates
            .push(&AccountUpdateView {
                pubkey: Address::new_from_array([1u8; 32]),
                lamports: 1,
                owner: Address::new_from_array([2u8; 32]),
                executable: true,
                rent_epoch: u64::MAX,
                write_version: 7,
                data: b"migrated-builtin",
            })
            .unwrap();

        let mut buf = vec![0u8; 4096];
        let mut cur = lencode::io::Cursor::new(&mut buf[..]);
        let n = src.encode_ext(&mut cur, None).unwrap();

        let mut dst = EpochMeta::new_boxed();
        let mut rd = lencode::io::Cursor::new(&buf[..n]);
        dst.decode_into(&mut rd, None).unwrap();
        assert_eq!(*dst, *src);
        let (meta, data) = dst.updates.iter().next().unwrap();
        assert_eq!(meta.write_version, 7);
        assert_eq!(data, b"migrated-builtin");
    }

    #[test]
    fn clear_resets() {
        let mut m = EpochMeta::new_boxed();
        m.epoch = 1;
        m.updates
            .push(&AccountUpdateView {
                pubkey: Address::new_from_array([1u8; 32]),
                lamports: 0,
                owner: Address::new_from_array([2u8; 32]),
                executable: false,
                rent_epoch: 0,
                write_version: 0,
                data: b"x",
            })
            .unwrap();
        m.clear();
        assert_eq!(m.epoch, 0);
        assert!(m.updates.is_empty());
    }
}
