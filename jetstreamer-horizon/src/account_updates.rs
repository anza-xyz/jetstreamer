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
}
