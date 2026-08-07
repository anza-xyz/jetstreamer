//! Zero-allocation account update record.
//!
//! [`AccountUpdate`] mirrors `agave-geyser-plugin-interface`'s
//! `ReplicaAccountInfo*` shapes but stores the account's `data` inline in a
//! fixed-capacity [`ZeroVec`][crate::zero_vec::ZeroVec] sized to the
//! Solana-enforced `MAX_PERMITTED_DATA_LENGTH`. Memory footprint is
//! ~10 MiB per instance, so callers should allocate *one* per thread via a
//! `thread_local!` and reuse it via [`AccountUpdate::fill_from_versions`] (or
//! the version-specific `fill_from_v*` helpers) rather than creating new
//! instances on the stack.
//!
//! # Example
//!
//! ```ignore
//! use std::cell::RefCell;
//! use jetstreamer_horizon::account_updates::AccountUpdate;
//!
//! thread_local! {
//!     static REUSABLE: RefCell<Box<AccountUpdate>> =
//!         RefCell::new(AccountUpdate::new_boxed());
//! }
//!
//! REUSABLE.with(|u| {
//!     let mut u = u.borrow_mut();
//!     u.fill_from_versions(info);
//!     // ... encode u ...
//! });
//! ```
use agave_geyser_plugin_interface::geyser_plugin_interface::{
    ReplicaAccountInfo, ReplicaAccountInfoV2, ReplicaAccountInfoV3, ReplicaAccountInfoVersions,
};
use lencode::prelude::*;
use solana_address::Address;

use crate::limits::MAX_ACCOUNT_DATA_LEN;
use crate::zero_vec::{ZeroAlloc, ZeroVec, assert_zero_alloc};

/// Account update record with inline-stored account data.
///
/// Allocates no heap memory. Callers reuse a single instance per thread and
/// overwrite via the `fill_from_*` methods to avoid repeated stack
/// initialisation of the 10 MiB inline buffer.
/// Borrowed view of an account update: all the metadata fields of
/// [`AccountUpdate`] plus a `&[u8]` into some externally owned data buffer.
///
/// Use this to push an account update into a [`crate::transactions::Transaction`]
/// *without* first staging through a full 10 MiB [`AccountUpdate`] buffer —
/// the `data` slice is copied straight from its source into the
/// transaction's shared arena, a single memcpy. Useful in the geyser
/// notify hot path where the `ReplicaAccountInfo*`'s data slice is already
/// live.
#[derive(Debug, Clone, Copy)]
pub struct AccountUpdateView<'a> {
    pub pubkey: Address,
    pub lamports: u64,
    pub owner: Address,
    pub executable: bool,
    pub rent_epoch: u64,
    pub write_version: u64,
    pub data: &'a [u8],
}

impl<'a> AccountUpdateView<'a> {
    /// Builds a view from a v1 replica info record.
    #[inline]
    pub fn from_replica_v1(info: &'a ReplicaAccountInfo<'a>) -> Self {
        Self {
            pubkey: Address::new_from_array(info.pubkey[..32].try_into().unwrap_or_default()),
            lamports: info.lamports,
            owner: Address::new_from_array(info.owner[..32].try_into().unwrap_or_default()),
            executable: info.executable,
            rent_epoch: info.rent_epoch,
            write_version: info.write_version,
            data: info.data,
        }
    }

    /// Builds a view from a v2 replica info record.
    #[inline]
    pub fn from_replica_v2(info: &'a ReplicaAccountInfoV2<'a>) -> Self {
        Self {
            pubkey: Address::new_from_array(info.pubkey[..32].try_into().unwrap_or_default()),
            lamports: info.lamports,
            owner: Address::new_from_array(info.owner[..32].try_into().unwrap_or_default()),
            executable: info.executable,
            rent_epoch: info.rent_epoch,
            write_version: info.write_version,
            data: info.data,
        }
    }

    /// Builds a view from a v3 replica info record.
    #[inline]
    pub fn from_replica_v3(info: &'a ReplicaAccountInfoV3<'a>) -> Self {
        Self {
            pubkey: Address::new_from_array(info.pubkey[..32].try_into().unwrap_or_default()),
            lamports: info.lamports,
            owner: Address::new_from_array(info.owner[..32].try_into().unwrap_or_default()),
            executable: info.executable,
            rent_epoch: info.rent_epoch,
            write_version: info.write_version,
            data: info.data,
        }
    }

    /// Dispatches to [`from_replica_v1`](Self::from_replica_v1) / `_v2` / `_v3`.
    #[inline]
    pub fn from_replica_versions(info: &'a ReplicaAccountInfoVersions<'a>) -> Self {
        match info {
            ReplicaAccountInfoVersions::V0_0_1(v1) => Self::from_replica_v1(v1),
            ReplicaAccountInfoVersions::V0_0_2(v2) => Self::from_replica_v2(v2),
            ReplicaAccountInfoVersions::V0_0_3(v3) => Self::from_replica_v3(v3),
        }
    }
}

impl<'a> From<&'a AccountUpdate> for AccountUpdateView<'a> {
    #[inline]
    fn from(u: &'a AccountUpdate) -> Self {
        Self {
            pubkey: u.pubkey,
            lamports: u.lamports,
            owner: u.owner,
            executable: u.executable,
            rent_epoch: u.rent_epoch,
            write_version: u.write_version,
            data: u.data.as_slice(),
        }
    }
}

/// Account update record with inline-stored account data.
///
/// Allocates no heap memory. Callers reuse a single instance per thread and
/// overwrite via the `fill_from_*` methods to avoid repeated stack
/// initialisation of the 10 MiB inline buffer.
///
/// `Clone` is intentionally **not** derived — cloning would move a ~10 MiB
/// struct through the stack and almost always overflow. If you genuinely
/// need a duplicate, allocate a fresh buffer with [`Self::new_boxed`] and
/// copy the fields explicitly.
#[derive(Encode, Decode, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(C)]
pub struct AccountUpdate {
    /// The address for the account.
    pub pubkey: Address,
    /// The lamports for the account.
    pub lamports: u64,
    /// The address of the owner program account.
    pub owner: Address,
    /// This account's data contains a loaded program (and is now read-only).
    pub executable: bool,
    /// The epoch at which this account will next owe rent.
    pub rent_epoch: u64,
    /// The data held in this account (up to `MAX_PERMITTED_DATA_LENGTH` = 10 MiB).
    pub data: ZeroVec<MAX_ACCOUNT_DATA_LEN, u8>,
    /// A global monotonically increasing atomic number for ordering updates to
    /// the same account within a slot.
    pub write_version: u64,
}

impl Default for AccountUpdate {
    /// Creates a zero-initialised `AccountUpdate` with an empty `data` buffer.
    ///
    /// **Warning:** the struct is ~10 MiB and stack-allocating it is likely to
    /// overflow a typical thread stack (8 MiB on Linux). Prefer
    /// [`AccountUpdate::new_boxed`] which avoids the intermediate stack copy.
    #[inline(always)]
    fn default() -> Self {
        Self {
            pubkey: Address::default(),
            lamports: 0,
            owner: Address::default(),
            executable: false,
            rent_epoch: 0,
            data: ZeroVec::new(),
            write_version: 0,
        }
    }
}

impl AccountUpdate {
    /// Allocates a fresh zero-initialised `AccountUpdate` directly on the
    /// heap, without routing the (~10 MiB) struct through the stack.
    ///
    /// Uses the global allocator directly to avoid `Box::new_uninit`'s
    /// stack-based `MaybeUninit::<T>::uninit()` intermediate, which would
    /// overflow a small thread stack.
    pub fn new_boxed() -> Box<Self> {
        use std::alloc::{Layout, alloc_zeroed, handle_alloc_error};
        let layout = Layout::new::<Self>();
        // SAFETY: layout size is > 0 (struct has non-zero fields) and alignment
        // matches `Self`. Zero-init is sound: `ZeroVec`'s `buf` is
        // `[MaybeUninit<T>; N]` which accepts any bit pattern, `len = 0`
        // means no buffer slot is ever read, and every non-ZeroVec field
        // has 0 as a valid default (u64, bool, Address, …).
        unsafe {
            let ptr = alloc_zeroed(layout) as *mut Self;
            if ptr.is_null() {
                handle_alloc_error(layout);
            }
            Box::from_raw(ptr)
        }
    }

    /// Returns the total in-memory size of this update including the used
    /// portion of `data` (not the full inline capacity).
    #[inline(always)]
    pub fn memory_size(&self) -> usize {
        core::mem::size_of::<Self>() + self.data.len()
    }

    /// Returns a borrowed view of this update for zero-copy transfer into a
    /// [`crate::transactions::Transaction::push_account_update`] call.
    #[inline]
    pub fn as_view(&self) -> AccountUpdateView<'_> {
        AccountUpdateView::from(self)
    }

    /// Overwrites this update from a v1 replica info record, in place, with
    /// no heap allocations.
    #[inline]
    pub fn fill_from_v1(&mut self, info: &ReplicaAccountInfo<'_>) {
        self.pubkey = Address::new_from_array(info.pubkey[..32].try_into().unwrap_or_default());
        self.lamports = info.lamports;
        self.owner = Address::new_from_array(info.owner[..32].try_into().unwrap_or_default());
        self.executable = info.executable;
        self.rent_epoch = info.rent_epoch;
        self.data.set(info.data);
        self.write_version = info.write_version;
    }

    /// Overwrites this update from a v2 replica info record, in place, with
    /// no heap allocations.
    #[inline]
    pub fn fill_from_v2(&mut self, info: &ReplicaAccountInfoV2<'_>) {
        self.pubkey = Address::new_from_array(info.pubkey[..32].try_into().unwrap_or_default());
        self.lamports = info.lamports;
        self.owner = Address::new_from_array(info.owner[..32].try_into().unwrap_or_default());
        self.executable = info.executable;
        self.rent_epoch = info.rent_epoch;
        self.data.set(info.data);
        self.write_version = info.write_version;
    }

    /// Overwrites this update from a v3 replica info record, in place, with
    /// no heap allocations.
    #[inline]
    pub fn fill_from_v3(&mut self, info: &ReplicaAccountInfoV3<'_>) {
        self.pubkey = Address::new_from_array(info.pubkey[..32].try_into().unwrap_or_default());
        self.lamports = info.lamports;
        self.owner = Address::new_from_array(info.owner[..32].try_into().unwrap_or_default());
        self.executable = info.executable;
        self.rent_epoch = info.rent_epoch;
        self.data.set(info.data);
        self.write_version = info.write_version;
    }

    /// Dispatches to [`fill_from_v1`](Self::fill_from_v1) / `_v2` / `_v3` based
    /// on the enum variant.
    #[inline]
    pub fn fill_from_versions(&mut self, info: ReplicaAccountInfoVersions<'_>) {
        match info {
            ReplicaAccountInfoVersions::V0_0_1(v1) => self.fill_from_v1(v1),
            ReplicaAccountInfoVersions::V0_0_2(v2) => self.fill_from_v2(v2),
            ReplicaAccountInfoVersions::V0_0_3(v3) => self.fill_from_v3(v3),
        }
    }

    /// Decodes a wire-encoded `AccountUpdate` directly into `self` without
    /// routing the ~10 MiB struct through the stack.
    ///
    /// This is the required entry point for any tight-loop decoding: the
    /// stock [`Decode::decode_ext`] would place the entire struct on the
    /// stack as a by-value return, blowing most thread stacks. `decode_into`
    /// writes each field directly into the already-allocated destination —
    /// in particular, the 10 MiB `data` buffer is filled via
    /// [`ZeroVec::decode_into`] with zero stack pressure.
    ///
    /// The field order must match what [`Encode::encode_ext`] produces;
    /// because both paths are derived from the source field order, they
    /// stay in sync automatically.
    pub fn decode_into<R: Read>(
        &mut self,
        reader: &mut R,
        mut ctx: Option<&mut lencode::context::DecoderContext>,
    ) -> lencode::Result<()> {
        self.pubkey = Address::decode_ext(reader, ctx.as_deref_mut())?;
        self.lamports = u64::decode_ext(reader, ctx.as_deref_mut())?;
        self.owner = Address::decode_ext(reader, ctx.as_deref_mut())?;
        self.executable = bool::decode_ext(reader, ctx.as_deref_mut())?;
        self.rent_epoch = u64::decode_ext(reader, ctx.as_deref_mut())?;
        self.data.decode_into(reader, ctx.as_deref_mut())?;
        self.write_version = u64::decode_ext(reader, ctx)?;
        Ok(())
    }
}

// --- Flat account-update arena (metadata table + shared data arena) ---

/// Metadata entry describing one account update stored in an
/// [`AccountUpdates`] arena.
///
/// The account's actual `data` bytes live in the arena's shared data
/// buffer. External callers cannot construct valid offset/length fields
/// pointing into that private buffer; they must go through
/// [`AccountUpdates::push`] to record an update and
/// [`AccountUpdates::iter`] to read back the metadata paired with its
/// data slice.
#[derive(Encode, Decode, Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(C)]
pub struct AccountUpdateMeta {
    pub pubkey: Address,
    pub lamports: u64,
    pub owner: Address,
    pub executable: bool,
    pub rent_epoch: u64,
    pub write_version: u64,
    // Offsets into the owning arena's data buffer; only meaningful when
    // paired with it — kept crate-private so external callers can't resolve
    // them against the wrong buffer.
    pub(crate) data_offset: u32,
    pub(crate) data_len: u32,
}

impl AccountUpdateMeta {
    /// Decodes the wire form into `self` without relying on the derived
    /// `Decode::decode_ext`'s by-value return path, so whole records can
    /// land directly in their `ZeroVec` slots via
    /// `decode_zerovec_in_place`.
    #[inline]
    pub fn decode_into<R: Read>(
        &mut self,
        reader: &mut R,
        mut ctx: Option<&mut lencode::context::DecoderContext>,
    ) -> lencode::Result<()> {
        self.pubkey = Address::decode_ext(reader, ctx.as_deref_mut())?;
        self.lamports = u64::decode_ext(reader, ctx.as_deref_mut())?;
        self.owner = Address::decode_ext(reader, ctx.as_deref_mut())?;
        self.executable = bool::decode_ext(reader, ctx.as_deref_mut())?;
        self.rent_epoch = u64::decode_ext(reader, ctx.as_deref_mut())?;
        self.write_version = u64::decode_ext(reader, ctx.as_deref_mut())?;
        self.data_offset = u32::decode_ext(reader, ctx.as_deref_mut())?;
        self.data_len = u32::decode_ext(reader, ctx)?;
        Ok(())
    }
}

impl crate::zero_vec::DecodeInto for AccountUpdateMeta {
    #[inline]
    fn decode_into<R: Read>(
        &mut self,
        reader: &mut R,
        ctx: Option<&mut lencode::context::DecoderContext>,
    ) -> lencode::Result<()> {
        Self::decode_into(self, reader, ctx)
    }
}

/// Error returned by [`AccountUpdates::push`] when either the metadata
/// table or the shared data arena is exhausted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PushAccountUpdateError {
    /// Metadata table already holds its maximum number of entries.
    MetaFull,
    /// Appending this update's `data` would exceed the arena's data
    /// capacity.
    DataArenaFull,
}

impl core::fmt::Display for PushAccountUpdateError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::MetaFull => write!(f, "account-update metadata table is full"),
            Self::DataArenaFull => write!(f, "account-update data arena is full"),
        }
    }
}

impl std::error::Error for PushAccountUpdateError {}

/// Fixed-capacity set of account updates: a metadata table plus a shared
/// data arena, fully inline (zero heap allocations).
///
/// This is the flat layout used wherever account updates are grouped under
/// an owning notification — nested inside a
/// [`Transaction`](crate::transactions::Transaction), or attached to a
/// block / epoch notification for runtime-direct ("orphan") writes such as
/// sysvar rewrites, fee distribution, and epoch reward credits.
///
/// `MAX` bounds the number of updates; `DATA` bounds the *combined* data
/// bytes across all of them, so memory stays proportional to real usage
/// instead of `MAX × worst-case-account-size`.
#[derive(Encode, Decode, Debug, PartialEq, Eq)]
#[repr(C)]
pub struct AccountUpdates<const MAX: usize, const DATA: usize> {
    meta: ZeroVec<MAX, AccountUpdateMeta>,
    data: ZeroVec<DATA, u8>,
}

impl<const MAX: usize, const DATA: usize> Default for AccountUpdates<MAX, DATA> {
    fn default() -> Self {
        Self {
            meta: ZeroVec::new(),
            data: ZeroVec::new(),
        }
    }
}

impl<const MAX: usize, const DATA: usize> AccountUpdates<MAX, DATA> {
    /// Appends an account update, copying its `data` bytes into the shared
    /// arena and recording a metadata entry referencing them.
    pub fn push(&mut self, view: &AccountUpdateView<'_>) -> Result<(), PushAccountUpdateError> {
        if self.meta.len() >= MAX {
            return Err(PushAccountUpdateError::MetaFull);
        }
        let data_len = view.data.len();
        if self.data.len() + data_len > DATA {
            return Err(PushAccountUpdateError::DataArenaFull);
        }
        let data_offset = self.data.len() as u32;
        self.data.extend_from_slice(view.data);
        self.meta.push(AccountUpdateMeta {
            pubkey: view.pubkey,
            lamports: view.lamports,
            owner: view.owner,
            executable: view.executable,
            rent_epoch: view.rent_epoch,
            write_version: view.write_version,
            data_offset,
            data_len: data_len as u32,
        });
        Ok(())
    }

    /// Read-only view of the metadata table.
    #[inline]
    pub fn updates(&self) -> &[AccountUpdateMeta] {
        self.meta.as_slice()
    }

    /// Number of updates stored.
    #[inline]
    pub fn len(&self) -> usize {
        self.meta.len()
    }

    /// `true` if no updates are stored.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.meta.is_empty()
    }

    /// Bytes currently used in the shared data arena.
    #[inline]
    pub fn data_len(&self) -> usize {
        self.data.len()
    }

    /// Iterates updates, yielding each metadata entry paired with its data
    /// slice.
    pub fn iter(&self) -> impl Iterator<Item = (&AccountUpdateMeta, &[u8])> {
        let data = self.data.as_slice();
        self.meta.iter().map(move |meta| {
            let start = meta.data_offset as usize;
            let end = start + meta.data_len as usize;
            (meta, &data[start..end])
        })
    }

    /// Empties the metadata table and data arena without heap activity.
    #[inline]
    pub fn clear(&mut self) {
        self.meta.clear();
        self.data.clear();
    }

    /// Decodes the wire form into `self` without stack-allocating the
    /// (potentially huge) arena.
    pub fn decode_into<R: Read>(
        &mut self,
        reader: &mut R,
        mut ctx: Option<&mut lencode::context::DecoderContext>,
    ) -> lencode::Result<()> {
        // SAFETY: `AccountUpdateMeta` is a composition of `Address`, scalar
        // integers, and `bool`; every field accepts the all-zero bit pattern.
        unsafe {
            crate::zero_vec::decode_zerovec_in_place(&mut self.meta, reader, ctx.as_deref_mut())?;
        }
        self.data.decode_into(reader, ctx)?;
        Ok(())
    }
}

// --- ZeroAlloc proofs ---
//
// `solana_address::Address` is a 32-byte newtype; we assert it's inline
// here so our types can transitively depend on it. `AccountUpdate` and
// `AccountUpdateView` each get a manual `ZeroAlloc` impl paired with
// compile-time per-field assertions — if any field grows a heap-owning
// type, the corresponding `assert_zero_alloc` line fails to compile.
impl ZeroAlloc for Address {}
impl ZeroAlloc for AccountUpdate {}
impl ZeroAlloc for AccountUpdateView<'_> {}
impl ZeroAlloc for AccountUpdateMeta {}
impl ZeroAlloc for PushAccountUpdateError {}
impl<const MAX: usize, const DATA: usize> ZeroAlloc for AccountUpdates<MAX, DATA> {}

const _: fn() = || {
    // `AccountUpdate` fields
    assert_zero_alloc::<Address>(); // pubkey
    assert_zero_alloc::<u64>(); // lamports
    assert_zero_alloc::<Address>(); // owner
    assert_zero_alloc::<bool>(); // executable
    assert_zero_alloc::<u64>(); // rent_epoch
    assert_zero_alloc::<ZeroVec<MAX_ACCOUNT_DATA_LEN, u8>>(); // data
    assert_zero_alloc::<u64>(); // write_version

    // `AccountUpdateView<'_>` fields (mostly the same, with `&'a [u8]` for data)
    assert_zero_alloc::<&[u8]>(); // data

    // `AccountUpdateMeta` fields: Address ×2, scalars, u32 offsets.
    assert_zero_alloc::<u32>(); // data_offset / data_len

    // `AccountUpdates<MAX, DATA>` fields (capacities arbitrary for the proof).
    assert_zero_alloc::<ZeroVec<4, AccountUpdateMeta>>(); // meta
    assert_zero_alloc::<ZeroVec<4, u8>>(); // data
    assert_zero_alloc::<AccountUpdates<4, 4>>();
};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_boxed_is_zero_initialised() {
        let u = AccountUpdate::new_boxed();
        assert_eq!(u.pubkey, Address::default());
        assert_eq!(u.lamports, 0);
        assert_eq!(u.owner, Address::default());
        assert!(!u.executable);
        assert_eq!(u.rent_epoch, 0);
        assert_eq!(u.data.len(), 0);
        assert_eq!(u.write_version, 0);
    }

    #[test]
    fn fill_from_v1_populates_fields() {
        let pk = [1u8; 32];
        let owner = [2u8; 32];
        let data = vec![0xAAu8; 100];
        let info = ReplicaAccountInfo {
            pubkey: &pk,
            lamports: 42,
            owner: &owner,
            executable: true,
            rent_epoch: 7,
            data: &data,
            write_version: 99,
        };
        let mut u = AccountUpdate::new_boxed();
        u.fill_from_v1(&info);
        assert_eq!(u.pubkey, Address::new_from_array(pk));
        assert_eq!(u.lamports, 42);
        assert_eq!(u.owner, Address::new_from_array(owner));
        assert!(u.executable);
        assert_eq!(u.rent_epoch, 7);
        assert_eq!(u.data.as_slice(), &data[..]);
        assert_eq!(u.write_version, 99);
    }

    #[test]
    fn fill_overwrites_existing_data() {
        let pk = [1u8; 32];
        let owner = [2u8; 32];
        let first = vec![0xAAu8; 50];
        let second = vec![0xBBu8; 80];
        let mut u = AccountUpdate::new_boxed();
        u.fill_from_v1(&ReplicaAccountInfo {
            pubkey: &pk,
            lamports: 1,
            owner: &owner,
            executable: false,
            rent_epoch: 0,
            data: &first,
            write_version: 1,
        });
        u.fill_from_v1(&ReplicaAccountInfo {
            pubkey: &pk,
            lamports: 2,
            owner: &owner,
            executable: false,
            rent_epoch: 0,
            data: &second,
            write_version: 2,
        });
        assert_eq!(u.lamports, 2);
        assert_eq!(u.data.as_slice(), &second[..]);
    }

    #[test]
    fn decode_into_roundtrip_on_default_stack() {
        // This test runs on the default test-thread stack (~2 MiB). The
        // plain by-value `Decode::decode_ext` would overflow it; `decode_into`
        // must fit because every big field goes through `ZeroVec::decode_into`.
        let pk = [7u8; 32];
        let owner = [8u8; 32];
        let data = vec![0xBEu8; 2048];

        let mut src = AccountUpdate::new_boxed();
        src.fill_from_v1(&ReplicaAccountInfo {
            pubkey: &pk,
            lamports: 500_000,
            owner: &owner,
            executable: true,
            rent_epoch: 12,
            data: &data,
            write_version: 77,
        });

        let mut buf = vec![0u8; 1 << 13];
        let mut cursor = lencode::io::Cursor::new(&mut buf[..]);
        let written = src.encode_ext(&mut cursor, None).unwrap();

        let mut dst = AccountUpdate::new_boxed();
        let mut read_cursor = lencode::io::Cursor::new(&buf[..written]);
        dst.decode_into(&mut read_cursor, None)
            .expect("decode_into");

        assert_eq!(dst.pubkey, src.pubkey);
        assert_eq!(dst.lamports, src.lamports);
        assert_eq!(dst.owner, src.owner);
        assert_eq!(dst.executable, src.executable);
        assert_eq!(dst.rent_epoch, src.rent_epoch);
        assert_eq!(dst.data.as_slice(), src.data.as_slice());
        assert_eq!(dst.write_version, src.write_version);
    }

    #[test]
    fn decode_into_clears_existing_data() {
        // A second decode_into call must fully overwrite the destination
        // (same buffer, new contents) without stale bytes leaking through.
        let mut dst = AccountUpdate::new_boxed();
        dst.fill_from_v1(&ReplicaAccountInfo {
            pubkey: &[1u8; 32],
            lamports: 1,
            owner: &[2u8; 32],
            executable: false,
            rent_epoch: 0,
            data: &vec![0xFFu8; 4096],
            write_version: 1,
        });

        let mut src = AccountUpdate::new_boxed();
        src.fill_from_v1(&ReplicaAccountInfo {
            pubkey: &[3u8; 32],
            lamports: 99,
            owner: &[4u8; 32],
            executable: true,
            rent_epoch: 9,
            data: b"short",
            write_version: 42,
        });

        let mut buf = vec![0u8; 1 << 12];
        let mut cursor = lencode::io::Cursor::new(&mut buf[..]);
        let written = src.encode_ext(&mut cursor, None).unwrap();

        let mut read_cursor = lencode::io::Cursor::new(&buf[..written]);
        dst.decode_into(&mut read_cursor, None)
            .expect("decode_into");

        assert_eq!(dst.pubkey, src.pubkey);
        assert_eq!(dst.data.as_slice(), b"short");
        assert_eq!(dst.data.len(), 5);
    }

    #[test]
    fn encode_produces_expected_prefix() {
        // We verify encoding runs without stack overflow (via heap-boxed
        // buffer + encode-through-reference), and that the output starts
        // with the pubkey bytes. Decode-by-value is intentionally *not*
        // tested here: at ~10 MiB per instance, `AccountUpdate::decode_ext`
        // would require callers to supply a pre-boxed destination (a
        // `decode_into` helper), which is out of scope for this struct.
        let pk = [3u8; 32];
        let owner = [4u8; 32];
        let data = vec![0xCDu8; 1024];
        let mut u = AccountUpdate::new_boxed();
        u.fill_from_v1(&ReplicaAccountInfo {
            pubkey: &pk,
            lamports: 1_000_000,
            owner: &owner,
            executable: false,
            rent_epoch: 42,
            data: &data,
            write_version: 999,
        });

        let mut buf = vec![0u8; 4096];
        let mut cursor = lencode::io::Cursor::new(&mut buf[..]);
        let written = u.encode_ext(&mut cursor, None).unwrap();
        // At minimum: two 32-byte Addresses + 1 bool + ~3-byte data header +
        // 1024 payload bytes = ~1092. Numerics are varint-compressed so the
        // upper bound is the full fixed-width size (~1109).
        assert!(written >= 32 + 32 + 1 + 3 + 1024);
        assert!(written <= 32 + 32 + 1 + 3 + 1024 + 8 + 8 + 8); // worst-case varints
        assert_eq!(&buf[..32], &pk);
    }

    // --- AccountUpdates arena ---

    fn arena_view<'a>(tag: u8, data: &'a [u8]) -> AccountUpdateView<'a> {
        AccountUpdateView {
            pubkey: Address::new_from_array([tag; 32]),
            lamports: tag as u64 * 1_000,
            owner: Address::new_from_array([0xEE; 32]),
            executable: tag.is_multiple_of(2),
            rent_epoch: u64::MAX,
            write_version: tag as u64,
            data,
        }
    }

    #[test]
    fn arena_push_iter_roundtrip() {
        let mut a: AccountUpdates<8, 1024> = AccountUpdates::default();
        a.push(&arena_view(1, b"first")).unwrap();
        a.push(&arena_view(2, b"second-data")).unwrap();
        a.push(&arena_view(3, b"")).unwrap(); // empty data is legal

        assert_eq!(a.len(), 3);
        assert_eq!(a.data_len(), 5 + 11);
        let collected: Vec<(u64, Vec<u8>)> = a
            .iter()
            .map(|(m, d)| (m.write_version, d.to_vec()))
            .collect();
        assert_eq!(
            collected,
            vec![
                (1, b"first".to_vec()),
                (2, b"second-data".to_vec()),
                (3, vec![]),
            ]
        );

        // Wire roundtrip via derived Encode + decode_into.
        let mut buf = vec![0u8; 8192];
        let mut cur = lencode::io::Cursor::new(&mut buf[..]);
        let n = a.encode_ext(&mut cur, None).unwrap();
        let mut b: AccountUpdates<8, 1024> = AccountUpdates::default();
        let mut rd = lencode::io::Cursor::new(&buf[..n]);
        b.decode_into(&mut rd, None).unwrap();
        assert_eq!(a, b);
    }

    #[test]
    fn arena_meta_overflow() {
        let mut a: AccountUpdates<2, 1024> = AccountUpdates::default();
        a.push(&arena_view(1, b"x")).unwrap();
        a.push(&arena_view(2, b"y")).unwrap();
        assert_eq!(
            a.push(&arena_view(3, b"z")),
            Err(PushAccountUpdateError::MetaFull)
        );
        // Failed push must not corrupt state.
        assert_eq!(a.len(), 2);
        assert_eq!(a.data_len(), 2);
    }

    #[test]
    fn arena_data_overflow() {
        let mut a: AccountUpdates<8, 10> = AccountUpdates::default();
        a.push(&arena_view(1, b"12345678")).unwrap();
        assert_eq!(
            a.push(&arena_view(2, b"abc")),
            Err(PushAccountUpdateError::DataArenaFull)
        );
        assert_eq!(a.len(), 1);
        assert_eq!(a.data_len(), 8);
        // Exactly filling the remainder is fine.
        a.push(&arena_view(3, b"ab")).unwrap();
        assert_eq!(a.data_len(), 10);
    }

    #[test]
    fn arena_clear_and_reuse() {
        let mut a: AccountUpdates<4, 256> = AccountUpdates::default();
        for round in 0..10u8 {
            a.clear();
            assert!(a.is_empty());
            a.push(&arena_view(round, &[round; 32])).unwrap();
            a.push(&arena_view(round.wrapping_add(1), &[round; 16]))
                .unwrap();
            assert_eq!(a.len(), 2);
            assert_eq!(a.data_len(), 48);
            let (m, d) = a.iter().next().unwrap();
            assert_eq!(m.write_version, round as u64);
            assert_eq!(d, &[round; 32][..]);
        }
    }

    #[test]
    fn arena_decode_into_shrinks_and_grows() {
        // decode_into must handle a scratch that previously held MORE
        // updates (shrink) and FEWER updates (grow) than the incoming wire.
        let mut big: AccountUpdates<8, 1024> = AccountUpdates::default();
        for i in 0..6u8 {
            big.push(&arena_view(i, &[i; 8])).unwrap();
        }
        let mut small: AccountUpdates<8, 1024> = AccountUpdates::default();
        small.push(&arena_view(9, b"only-one")).unwrap();

        let mut buf = vec![0u8; 8192];

        // big wire → scratch holding small (grow path)
        let mut cur = lencode::io::Cursor::new(&mut buf[..]);
        let n = big.encode_ext(&mut cur, None).unwrap();
        let mut scratch = small;
        let mut rd = lencode::io::Cursor::new(&buf[..n]);
        scratch.decode_into(&mut rd, None).unwrap();
        assert_eq!(scratch, big);

        // small wire → scratch holding big (shrink path)
        let mut small2: AccountUpdates<8, 1024> = AccountUpdates::default();
        small2.push(&arena_view(7, b"tiny")).unwrap();
        let mut cur = lencode::io::Cursor::new(&mut buf[..]);
        let n = small2.encode_ext(&mut cur, None).unwrap();
        let mut rd = lencode::io::Cursor::new(&buf[..n]);
        scratch.decode_into(&mut rd, None).unwrap();
        assert_eq!(scratch, small2);
    }

    #[test]
    fn arena_at_full_capacity() {
        let mut a: AccountUpdates<3, 96> = AccountUpdates::default();
        for i in 0..3u8 {
            a.push(&arena_view(i, &[i; 32])).unwrap();
        }
        assert_eq!(a.len(), 3);
        assert_eq!(a.data_len(), 96);
        // Roundtrip at exact capacity.
        let mut buf = vec![0u8; 4096];
        let mut cur = lencode::io::Cursor::new(&mut buf[..]);
        let n = a.encode_ext(&mut cur, None).unwrap();
        let mut b: AccountUpdates<3, 96> = AccountUpdates::default();
        let mut rd = lencode::io::Cursor::new(&buf[..n]);
        b.decode_into(&mut rd, None).unwrap();
        assert_eq!(a, b);
    }
}
