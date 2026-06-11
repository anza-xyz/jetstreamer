//! Zero-allocation transaction record with all fields inline-bounded.
//!
//! Every `Vec<T>` / `String` that appears in the original Solana
//! `TransactionStatusMeta` / `VersionedMessage` shapes is replaced here with a
//! fixed-capacity [`ZeroVec`][crate::zero_vec::ZeroVec] sized from
//! [`crate::limits`]. Types in the `solana-transaction-*` crates that
//! transitively hold `Vec` or `String` (`Reward`, `TransactionReturnData`,
//! `InnerInstructions`, …) are replaced by zero-alloc counterparts defined
//! here.
//!
//! These structures are large by design — for example [`Transaction`]'s max
//! inline size is on the order of a few hundred KiB once you account for
//! signatures, per-instruction data, and log messages. Callers should keep
//! one instance per worker thread (thread-local + reuse) rather than creating
//! many on the stack.
use lencode::prelude::*;
use solana_address::Address;
use solana_hash::Hash;
use solana_signature::Signature;

use crate::account_updates::AccountUpdateView;
use crate::limits::{
    MAX_ACCOUNT_DATA_LEN, MAX_CUSTOM_ERROR_LEN, MAX_IX_ACCOUNTS, MAX_IX_DATA_LEN, MAX_LOG_MSG_LEN,
    MAX_RETURN_DATA_LEN, MAX_TX_ACCOUNT_UPDATES, MAX_TX_ACCOUNTS, MAX_TX_ADDR_LOOKUPS,
    MAX_TX_INNER_IX, MAX_TX_INSTRUCTIONS, MAX_TX_LOG_MSGS, MAX_TX_REWARDS, MAX_TX_SIGS,
    MAX_TX_TOKEN_BALANCES,
};
use crate::zero_vec::{ZeroAlloc, ZeroVec, assert_zero_alloc};

// --- Message types ---

#[derive(Encode, Decode, Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(C)]
pub struct MessageHeader {
    pub num_required_signatures: u8,
    pub num_readonly_signed_accounts: u8,
    pub num_readonly_unsigned_accounts: u8,
}

#[derive(Encode, Decode, Debug, Clone, PartialEq, Eq, Default)]
#[repr(C)]
pub struct CompiledInstruction {
    pub program_id_index: u8,
    pub accounts: ZeroVec<MAX_IX_ACCOUNTS, u8>,
    pub data: ZeroVec<MAX_IX_DATA_LEN, u8>,
}

impl CompiledInstruction {
    /// Resets this instruction's scratch state in place — no stack-allocated
    /// temporary, unlike `*self = Self::default()` which would copy the
    /// ~10 KiB inline data buffer via the stack.
    #[inline]
    pub fn clear(&mut self) {
        self.program_id_index = 0;
        self.accounts.clear();
        self.data.clear();
    }

    /// Decodes the wire form into `self` without stack-allocating the
    /// ~10 KiB struct. Each field is decoded directly into the existing
    /// allocation via [`ZeroVec::decode_into`] — the alternative
    /// `#[derive(Decode)]` path would return `Self` by value, forcing a
    /// ~10 KiB stack temporary per instruction.
    #[inline]
    pub fn decode_into<R: Read>(
        &mut self,
        reader: &mut R,
        mut ctx: Option<&mut lencode::context::DecoderContext>,
    ) -> lencode::Result<()> {
        self.program_id_index = u8::decode_ext(reader, ctx.as_deref_mut())?;
        self.accounts.decode_into(reader, ctx.as_deref_mut())?;
        self.data.decode_into(reader, ctx.as_deref_mut())?;
        Ok(())
    }
}

#[derive(Encode, Decode, Debug, Clone, PartialEq, Eq, Default)]
#[repr(C)]
pub struct MessageAddressTableLookup {
    pub account_key: Address,
    pub writable_indexes: ZeroVec<MAX_TX_ACCOUNTS, u8>,
    pub readonly_indexes: ZeroVec<MAX_TX_ACCOUNTS, u8>,
}

impl MessageAddressTableLookup {
    /// In-place clear — resets to the default empty state without a
    /// ~576 B stack temporary.
    #[inline]
    pub fn clear(&mut self) {
        self.account_key = Address::default();
        self.writable_indexes.clear();
        self.readonly_indexes.clear();
    }

    /// Decodes the wire form into `self` without stack-allocating the
    /// ~576 B struct. Companion to [`CompiledInstruction::decode_into`].
    #[inline]
    pub fn decode_into<R: Read>(
        &mut self,
        reader: &mut R,
        mut ctx: Option<&mut lencode::context::DecoderContext>,
    ) -> lencode::Result<()> {
        self.account_key = Address::decode_ext(reader, ctx.as_deref_mut())?;
        self.writable_indexes
            .decode_into(reader, ctx.as_deref_mut())?;
        self.readonly_indexes
            .decode_into(reader, ctx.as_deref_mut())?;
        Ok(())
    }
}

#[derive(Encode, Decode, Debug, Clone, PartialEq, Eq, Default)]
#[repr(C)]
pub struct LegacyMessage {
    pub header: MessageHeader,
    pub account_keys: ZeroVec<MAX_TX_ACCOUNTS, Address>,
    pub recent_blockhash: Hash,
    pub instructions: ZeroVec<MAX_TX_INSTRUCTIONS, CompiledInstruction>,
}

impl LegacyMessage {
    /// In-place reset to match `Default::default()` without a stack-allocated
    /// temporary.
    #[inline]
    pub fn clear(&mut self) {
        self.header = MessageHeader::default();
        self.account_keys.clear();
        self.recent_blockhash = Hash::default();
        // Drop each instruction in-place (keeps the ZeroVec allocation).
        for ix in self.instructions.iter_mut() {
            ix.clear();
        }
        self.instructions.clear();
    }

    /// Decodes the wire form into `self` without stack-allocating the
    /// ~680 KiB struct. Each field is decoded directly into the existing
    /// allocation via [`ZeroVec::decode_into`].
    pub fn decode_into<R: Read>(
        &mut self,
        reader: &mut R,
        mut ctx: Option<&mut lencode::context::DecoderContext>,
    ) -> lencode::Result<()> {
        self.header = MessageHeader::decode_ext(reader, ctx.as_deref_mut())?;
        self.account_keys.decode_into(reader, ctx.as_deref_mut())?;
        self.recent_blockhash = Hash::decode_ext(reader, ctx.as_deref_mut())?;
        // SAFETY: `CompiledInstruction` is a composition of `u8` plus two
        // `ZeroVec<_, u8>` fields; every field accepts the all-zero bit
        // pattern as a valid default state.
        unsafe {
            decode_zerovec_in_place(&mut self.instructions, reader, ctx.as_deref_mut())?;
        }
        Ok(())
    }
}

#[derive(Encode, Decode, Debug, Clone, PartialEq, Eq, Default)]
#[repr(C)]
pub struct V0Message {
    pub header: MessageHeader,
    pub account_keys: ZeroVec<MAX_TX_ACCOUNTS, Address>,
    pub recent_blockhash: Hash,
    pub instructions: ZeroVec<MAX_TX_INSTRUCTIONS, CompiledInstruction>,
    pub address_table_lookups: ZeroVec<MAX_TX_ADDR_LOOKUPS, MessageAddressTableLookup>,
}

impl V0Message {
    /// In-place reset to match `Default::default()` without a stack-allocated
    /// temporary.
    #[inline]
    pub fn clear(&mut self) {
        self.header = MessageHeader::default();
        self.account_keys.clear();
        self.recent_blockhash = Hash::default();
        for ix in self.instructions.iter_mut() {
            ix.clear();
        }
        self.instructions.clear();
        self.address_table_lookups.clear();
    }

    /// Decodes the wire form into `self` without stack-allocating the
    /// ~750 KiB struct. Each field is decoded directly into the existing
    /// allocation via [`ZeroVec::decode_into`].
    pub fn decode_into<R: Read>(
        &mut self,
        reader: &mut R,
        mut ctx: Option<&mut lencode::context::DecoderContext>,
    ) -> lencode::Result<()> {
        self.header = MessageHeader::decode_ext(reader, ctx.as_deref_mut())?;
        self.account_keys.decode_into(reader, ctx.as_deref_mut())?;
        self.recent_blockhash = Hash::decode_ext(reader, ctx.as_deref_mut())?;
        // SAFETY: both `CompiledInstruction` and `MessageAddressTableLookup`
        // accept the all-zero bit pattern as a valid default (every field is
        // either a scalar default, `Address::default()` = zero bytes, or a
        // `ZeroVec<_, u8>` whose empty state is `len = 0` over an uninit
        // buffer).
        unsafe {
            decode_zerovec_in_place(&mut self.instructions, reader, ctx.as_deref_mut())?;
            decode_zerovec_in_place(&mut self.address_table_lookups, reader, ctx.as_deref_mut())?;
        }
        Ok(())
    }
}

#[derive(Encode, Decode, Debug, Clone, PartialEq, Eq)]
// `#[repr(C, u8)]` pins the discriminant to offset 0 with u8 size. That
// layout guarantee is what makes [`VersionedMessage::decode_into`]'s
// in-place variant swap (via `ptr::write_bytes` + a byte write to the
// discriminant) safe — without it, Rust could niche-optimise the
// discriminant anywhere in the struct.
#[repr(C, u8)]
pub enum VersionedMessage {
    Legacy(LegacyMessage) = 0,
    V0(V0Message) = 1,
}

impl Default for VersionedMessage {
    fn default() -> Self {
        Self::Legacy(LegacyMessage::default())
    }
}

impl VersionedMessage {
    /// In-place reset: clears the current variant's contents without
    /// stack-allocating a new variant.
    #[inline]
    pub fn clear(&mut self) {
        match self {
            Self::Legacy(m) => m.clear(),
            Self::V0(m) => m.clear(),
        }
    }

    /// Decodes the wire form into `self` without stack-allocating the
    /// ~750 KiB struct.
    ///
    /// If the incoming discriminant matches the currently-active variant,
    /// decodes straight into that variant's payload in place. Otherwise,
    /// drops the current variant, zeroes the storage, sets the
    /// discriminant byte directly, and decodes into the new variant — no
    /// temporary `VersionedMessage` ever lives on the stack.
    pub fn decode_into<R: Read>(
        &mut self,
        reader: &mut R,
        ctx: Option<&mut lencode::context::DecoderContext>,
    ) -> lencode::Result<()> {
        let disc = <Self as Decode>::decode_discriminant(reader)?;
        match disc {
            0 => {
                // Ensure `self` is the Legacy variant.
                if !matches!(self, Self::Legacy(_)) {
                    // SAFETY: `#[repr(C, u8)]` guarantees discriminant at
                    // byte 0. Drop the current variant, zero every byte of
                    // the enum storage (discriminant becomes 0 = Legacy,
                    // payload becomes an all-zero `LegacyMessage` — all of
                    // its fields accept zero bit patterns). No
                    // stack-allocated `LegacyMessage` temporary is needed.
                    unsafe {
                        core::ptr::drop_in_place(self as *mut Self);
                        core::ptr::write_bytes(self as *mut Self, 0, 1);
                    }
                }
                match self {
                    Self::Legacy(m) => m.decode_into(reader, ctx),
                    _ => unreachable!(),
                }
            }
            1 => {
                if !matches!(self, Self::V0(_)) {
                    unsafe {
                        core::ptr::drop_in_place(self as *mut Self);
                        core::ptr::write_bytes(self as *mut Self, 0, 1);
                        // Flip the discriminant byte from 0 (Legacy) to 1 (V0).
                        *(self as *mut Self as *mut u8) = 1;
                    }
                }
                match self {
                    Self::V0(m) => m.decode_into(reader, ctx),
                    _ => unreachable!(),
                }
            }
            other => {
                let _ = other;
                Err(lencode::io::Error::InvalidData)
            }
        }
    }
}

// --- Metadata sub-types (zero-alloc replacements for solana-transaction-* heap types) ---

/// Inner instruction record. Flat representation (each entry carries the
/// `outer_index` of the top-level instruction it descends from), so the
/// whole trace fits in a single `ZeroVec<MAX_TX_INNER_IX, InnerInstruction>`
/// rather than a nested `Vec<Vec<_>>`.
#[derive(Encode, Decode, Debug, Clone, PartialEq, Eq, Default)]
#[repr(C)]
pub struct InnerInstruction {
    pub outer_index: u8,
    pub instruction: CompiledInstruction,
    pub stack_height: Option<u32>,
}

impl InnerInstruction {
    /// In-place clear — avoids the ~10 KiB stack temporary
    /// `*self = Self::default()` would create.
    #[inline]
    pub fn clear(&mut self) {
        self.outer_index = 0;
        self.instruction.clear();
        self.stack_height = None;
    }

    /// Decodes the wire form into `self` without stack-allocating the
    /// ~10 KiB struct. Companion to [`CompiledInstruction::decode_into`].
    #[inline]
    pub fn decode_into<R: Read>(
        &mut self,
        reader: &mut R,
        mut ctx: Option<&mut lencode::context::DecoderContext>,
    ) -> lencode::Result<()> {
        self.outer_index = u8::decode_ext(reader, ctx.as_deref_mut())?;
        self.instruction.decode_into(reader, ctx.as_deref_mut())?;
        self.stack_height = Option::<u32>::decode_ext(reader, ctx.as_deref_mut())?;
        Ok(())
    }
}

/// Single log line emitted by the transaction. Stored as UTF-8 bytes (strings
/// are heap-allocated by default).
#[derive(Encode, Decode, Debug, Clone, PartialEq, Eq, Default)]
#[repr(C)]
pub struct LogMessage {
    pub bytes: ZeroVec<MAX_LOG_MSG_LEN, u8>,
}

/// Return data from a transaction. Inline-bounded counterpart to
/// `solana_transaction_context::TransactionReturnData`.
#[derive(Encode, Decode, Debug, Clone, PartialEq, Eq, Default)]
#[repr(C)]
pub struct ReturnData {
    pub program_id: Address,
    pub data: ZeroVec<MAX_RETURN_DATA_LEN, u8>,
}

/// Reward record. Inline-bounded counterpart to
/// `solana_transaction_status::Reward` (replaces its `pubkey: String` with an
/// [`Address`]).
#[derive(Encode, Decode, Debug, Clone, PartialEq, Eq, Default)]
#[repr(C)]
pub struct Reward {
    pub pubkey: Address,
    pub lamports: i64,
    pub post_balance: u64,
    pub reward_type: Option<RewardType>,
    pub commission: Option<u8>,
}

#[derive(Encode, Decode, Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum RewardType {
    Fee,
    Rent,
    Staking,
    Voting,
}

/// Status of a transaction after execution.
///
/// Zero-alloc replacement for `Result<(), TransactionError>` — the subset of
/// variants we encode inline, with string payloads stored in a fixed-capacity
/// buffer.
#[derive(Encode, Decode, Debug, Clone, PartialEq, Eq, Default)]
#[repr(C)]
pub struct TransactionStatus {
    /// Non-zero if the transaction failed. Matches the discriminant of
    /// `solana_transaction_error::TransactionError` plus 1 (0 = success).
    pub error_code: u16,
    /// For errors that carry an instruction index (`InstructionError`,
    /// `InsufficientFundsForRent`, …), the index; otherwise 0.
    pub instruction_index: u8,
    /// For `InstructionError::Custom(u32)`, the program-defined code;
    /// otherwise 0.
    pub custom_error_code: u32,
    /// For errors carrying a human-readable string (`Custom(String)`-style),
    /// the bytes; otherwise empty.
    pub error_message: ZeroVec<MAX_CUSTOM_ERROR_LEN, u8>,
}

impl TransactionStatus {
    /// `Ok` status.
    pub const OK: Self = Self {
        error_code: 0,
        instruction_index: 0,
        custom_error_code: 0,
        error_message: ZeroVec::new(),
    };

    #[inline(always)]
    pub const fn is_ok(&self) -> bool {
        self.error_code == 0
    }

    /// Resets this status in place to the `Ok` state without a
    /// stack-allocated temporary.
    #[inline]
    pub fn clear(&mut self) {
        self.error_code = 0;
        self.instruction_index = 0;
        self.custom_error_code = 0;
        self.error_message.clear();
    }
}

// --- Nested account update (flat arena layout) ---

/// Metadata entry describing a single account update nested inside a
/// [`Transaction`].
///
/// The account's actual `data` bytes live in the transaction's shared
/// arena. External callers cannot construct valid offset/length fields
/// pointing into that private arena; they must go through
/// [`Transaction::push_account_update`] to record an update and
/// [`Transaction::iter_account_updates`] to read back the metadata paired
/// with its data slice.
#[derive(Encode, Decode, Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(C)]
pub struct TxAccountUpdate {
    pub pubkey: Address,
    pub lamports: u64,
    pub owner: Address,
    pub executable: bool,
    pub rent_epoch: u64,
    pub write_version: u64,
    // These are offsets into `Transaction::account_updates_data` (a private
    // field) and only make sense when paired with it. Keep them crate-private
    // to stop external callers from trying to resolve them against their own
    // buffer.
    pub(crate) data_offset: u32,
    pub(crate) data_len: u32,
}

impl TxAccountUpdate {
    /// Decodes the wire form into `self` without relying on the derived
    /// `Decode::decode_ext`'s by-value return path. With a dozen+ account
    /// updates per transaction, decoding in place saves a per-element stack
    /// roundtrip and lets the whole record land directly in its
    /// `ZeroVec` slot via `decode_zerovec_in_place`.
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
        self.data_len = u32::decode_ext(reader, ctx.as_deref_mut())?;
        Ok(())
    }
}

/// Error returned by [`Transaction::push_account_update`] when either the
/// per-transaction metadata slot limit or the shared data arena is exhausted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PushAccountUpdateError {
    /// Metadata array already holds [`MAX_TX_ACCOUNT_UPDATES`] entries.
    MetaFull,
    /// Appending this update's `data` would exceed [`MAX_ACCOUNT_DATA_LEN`]
    /// across the whole transaction.
    DataArenaFull,
}

impl core::fmt::Display for PushAccountUpdateError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::MetaFull => write!(
                f,
                "transaction already has MAX_TX_ACCOUNT_UPDATES={} account updates",
                MAX_TX_ACCOUNT_UPDATES
            ),
            Self::DataArenaFull => write!(
                f,
                "account-update data arena is full (MAX_ACCOUNT_DATA_LEN={} bytes)",
                MAX_ACCOUNT_DATA_LEN
            ),
        }
    }
}

impl std::error::Error for PushAccountUpdateError {}

// --- TransactionTokenBalance ---

#[derive(Encode, Decode, Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(C)]
pub struct TokenAmount {
    pub amount: u64,
    pub decimals: u8,
}

#[derive(Encode, Decode, Debug, Clone, PartialEq, Eq, Default)]
#[repr(C)]
pub struct TransactionTokenBalance {
    pub account_index: u8,
    pub mint: Address,
    pub ui_token_amount: TokenAmount,
    pub owner: Option<Address>,
    pub program_id: Option<Address>,
}

// --- Transaction ---

/// A Solana transaction with all associated metadata, stored inline with no
/// heap allocations.
///
/// Size: on the order of ~12 MiB (dominated by the nested account-update
/// data arena). Callers should keep one instance per worker thread and reuse
/// via [`Transaction::clear`], not stack-allocate per transaction. Use
/// [`Transaction::new_boxed`] to avoid the initial stack copy.
///
/// `Clone` is intentionally **not** derived — cloning would move the whole
/// struct through the stack and almost always overflow. For duplication,
/// allocate a fresh buffer with [`Self::new_boxed`] and rebuild its state
/// field-by-field.
#[derive(Encode, Decode, Debug)]
#[repr(C)]
pub struct Transaction {
    pub status: TransactionStatus,
    pub fee: u64,
    pub pre_balances: ZeroVec<MAX_TX_ACCOUNTS, u64>,
    pub post_balances: ZeroVec<MAX_TX_ACCOUNTS, u64>,
    /// Inner-instruction trace, flattened (each entry carries its outer index).
    pub inner_instructions: Option<ZeroVec<MAX_TX_INNER_IX, InnerInstruction>>,
    pub log_messages: Option<ZeroVec<MAX_TX_LOG_MSGS, LogMessage>>,
    pub pre_token_balances: Option<ZeroVec<MAX_TX_TOKEN_BALANCES, TransactionTokenBalance>>,
    pub post_token_balances: Option<ZeroVec<MAX_TX_TOKEN_BALANCES, TransactionTokenBalance>>,
    pub rewards: Option<ZeroVec<MAX_TX_REWARDS, Reward>>,
    pub return_data: Option<ReturnData>,
    pub compute_units_consumed: Option<u64>,
    pub cost_units: Option<u64>,
    /// Signatures for the transaction.
    pub signatures: ZeroVec<MAX_TX_SIGS, Signature>,
    /// Message to sign.
    pub message: VersionedMessage,
    // --- Account-update arena (flat layout, private) ---
    //
    // These two fields implement the flat metadata-plus-shared-data-arena
    // layout. They are kept private so callers cannot put them into an
    // inconsistent state (e.g. a metadata entry whose offset/length points
    // outside the arena). Use [`Self::push_account_update`], the read-only
    // accessors, and [`Self::iter_account_updates`] to work with them.
    account_updates: ZeroVec<MAX_TX_ACCOUNT_UPDATES, TxAccountUpdate>,
    account_updates_data: ZeroVec<MAX_ACCOUNT_DATA_LEN, u8>,
}

impl Default for Transaction {
    fn default() -> Self {
        Self {
            status: TransactionStatus::default(),
            fee: 0,
            pre_balances: ZeroVec::new(),
            post_balances: ZeroVec::new(),
            inner_instructions: None,
            log_messages: None,
            pre_token_balances: None,
            post_token_balances: None,
            rewards: None,
            return_data: None,
            compute_units_consumed: None,
            cost_units: None,
            signatures: ZeroVec::new(),
            message: VersionedMessage::default(),
            account_updates: ZeroVec::new(),
            account_updates_data: ZeroVec::new(),
        }
    }
}

impl Transaction {
    /// Allocates a fresh zero-initialised `Transaction` directly on the
    /// heap, without routing the large (~12 MiB) struct through the stack.
    ///
    /// Uses the global allocator directly to avoid `Box::new_uninit`'s
    /// stack-based `MaybeUninit::<T>::uninit()` intermediate, which would
    /// overflow a small thread stack.
    pub fn new_boxed() -> Box<Self> {
        use std::alloc::{Layout, alloc_zeroed, handle_alloc_error};
        let layout = Layout::new::<Self>();
        // SAFETY: every `ZeroVec` field is safe with `len = 0` over an uninit
        // `[MaybeUninit<T>; N]`, zero is a valid bit-pattern for all
        // primitive / `Option::None` / `repr(u8)` enum discriminants used
        // here, and `TransactionStatus::OK` is all-zero.
        unsafe {
            let ptr = alloc_zeroed(layout) as *mut Self;
            if ptr.is_null() {
                handle_alloc_error(layout);
            }
            Box::from_raw(ptr)
        }
    }

    /// Resets the per-transaction scratch state (metadata lists + data
    /// arena) so this `Transaction` instance can be reused for the next
    /// transaction without heap allocation *and* without stack-allocating
    /// any large intermediate values — every sub-field is cleared in place.
    pub fn clear(&mut self) {
        self.status.clear();
        self.fee = 0;
        self.pre_balances.clear();
        self.post_balances.clear();
        self.inner_instructions = None;
        self.log_messages = None;
        self.pre_token_balances = None;
        self.post_token_balances = None;
        self.rewards = None;
        self.return_data = None;
        self.compute_units_consumed = None;
        self.cost_units = None;
        self.signatures.clear();
        self.message.clear();
        self.clear_account_updates();
    }

    /// Appends an account update to this transaction, copying its `data`
    /// bytes into the shared arena and recording a metadata entry that
    /// references them by offset/length.
    ///
    /// Accepts an [`AccountUpdateView`] — a cheap borrowed view over the
    /// metadata + a `&[u8]` data slice. The view lets you push directly
    /// from a live `ReplicaAccountInfo*` record or an existing
    /// [`AccountUpdate`] (via [`AccountUpdate::as_view`]) without staging
    /// through an intermediate 10 MiB buffer.
    ///
    /// Returns an error if either the metadata table or the data arena
    /// doesn't have room for the new entry.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use jetstreamer_horizon::account_updates::AccountUpdateView;
    ///
    /// // From a live geyser notification: one memcpy (info.data → arena).
    /// tx.push_account_update(&AccountUpdateView::from_replica_v3(info))?;
    ///
    /// // From an existing AccountUpdate:
    /// tx.push_account_update(&update.as_view())?;
    /// ```
    pub fn push_account_update(
        &mut self,
        view: &AccountUpdateView<'_>,
    ) -> Result<(), PushAccountUpdateError> {
        if self.account_updates.len() >= MAX_TX_ACCOUNT_UPDATES {
            return Err(PushAccountUpdateError::MetaFull);
        }
        let data_len = view.data.len();
        if self.account_updates_data.len() + data_len > MAX_ACCOUNT_DATA_LEN {
            return Err(PushAccountUpdateError::DataArenaFull);
        }
        let data_offset = self.account_updates_data.len() as u32;
        self.account_updates_data.extend_from_slice(view.data);
        self.account_updates.push(TxAccountUpdate {
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

    /// Returns a read-only view of the account-update metadata table. Use
    /// standard slice APIs (`.len()`, `.is_empty()`, `.iter()`, indexing,
    /// …) to inspect it.
    ///
    /// Each entry's `data_offset` / `data_len` fields are offsets into a
    /// private arena and aren't useful on their own — use
    /// [`Self::iter_account_updates`] to walk metadata paired with data.
    #[inline]
    pub fn account_updates(&self) -> &[TxAccountUpdate] {
        self.account_updates.as_slice()
    }

    /// Iterates over this transaction's account updates, yielding each
    /// metadata entry paired with its data slice.
    pub fn iter_account_updates(&self) -> impl Iterator<Item = (&TxAccountUpdate, &[u8])> {
        let data = self.account_updates_data.as_slice();
        self.account_updates.iter().map(move |meta| {
            let start = meta.data_offset as usize;
            let end = start + meta.data_len as usize;
            (meta, &data[start..end])
        })
    }

    /// Empties the account-update metadata table and shared data arena
    /// without heap allocation.
    #[inline]
    pub fn clear_account_updates(&mut self) {
        self.account_updates.clear();
        self.account_updates_data.clear();
    }

    /// Decodes a wire-encoded `Transaction` directly into `self` without
    /// routing the ~12 MiB struct through the stack.
    ///
    /// This is the required entry point for any tight-loop decoding: the
    /// stock [`Decode::decode_ext`] would place the entire struct on the
    /// stack as a by-value return, far exceeding any reasonable thread
    /// stack. `decode_into` writes each field directly into the
    /// already-allocated destination, using
    /// [`ZeroVec::decode_into`][crate::zero_vec::ZeroVec::decode_into]
    /// for the large inline buffers (most importantly the 10 MiB
    /// `account_updates_data` arena).
    ///
    /// The field order must stay in sync with `#[derive(Encode, Decode)]`'s
    /// output; both read the struct's source field order.
    pub fn decode_into<R: Read>(
        &mut self,
        reader: &mut R,
        mut ctx: Option<&mut lencode::context::DecoderContext>,
    ) -> lencode::Result<()> {
        // Small scalar-ish fields: decode_ext by value is fine (≤ a few KiB
        // on the stack per call, returned into self's location).
        self.status = TransactionStatus::decode_ext(reader, ctx.as_deref_mut())?;
        self.fee = u64::decode_ext(reader, ctx.as_deref_mut())?;
        self.pre_balances.decode_into(reader, ctx.as_deref_mut())?;
        self.post_balances.decode_into(reader, ctx.as_deref_mut())?;

        // Option<ZeroVec<_, _>>: decode the tag, then decode-in-place on
        // the inner ZeroVec so we never stack-alloc the full inner buffer.
        //
        // `inner_instructions` uses the DecodeInto-aware variant so each
        // ~10 KiB `InnerInstruction` is decoded directly into its slot in
        // the vec rather than routed through the stack as a by-value return.
        //
        // SAFETY: `InnerInstruction` accepts the all-zero bit pattern as a
        // valid default (zero `outer_index`, zero-initialised
        // `CompiledInstruction`, `stack_height` = `None`).
        unsafe {
            decode_option_zerovec_in_place(
                &mut self.inner_instructions,
                reader,
                ctx.as_deref_mut(),
            )?;
        }
        decode_option_zerovec_into(&mut self.log_messages, reader, ctx.as_deref_mut())?;
        decode_option_zerovec_into(&mut self.pre_token_balances, reader, ctx.as_deref_mut())?;
        decode_option_zerovec_into(&mut self.post_token_balances, reader, ctx.as_deref_mut())?;
        decode_option_zerovec_into(&mut self.rewards, reader, ctx.as_deref_mut())?;

        // Small Option<T> fields — by-value is fine.
        self.return_data = Option::<ReturnData>::decode_ext(reader, ctx.as_deref_mut())?;
        self.compute_units_consumed = Option::<u64>::decode_ext(reader, ctx.as_deref_mut())?;
        self.cost_units = Option::<u64>::decode_ext(reader, ctx.as_deref_mut())?;

        // Signatures (≤ 19 × 64 bytes): decode-in-place for uniformity.
        self.signatures.decode_into(reader, ctx.as_deref_mut())?;

        // VersionedMessage decodes directly into `self.message`'s storage
        // with variant-swapping done in place via a byte-level discriminant
        // write — no 750 KiB stack temporary.
        self.message.decode_into(reader, ctx.as_deref_mut())?;

        // Account-update metadata table + shared data arena.
        // SAFETY: `TxAccountUpdate`'s every field (`Address`, `u64`,
        // `bool`, `u32`) accepts the all-zero bit pattern as a valid
        // default — zero-priming an uninitialised slot before
        // `decode_into` overwrites its fields is sound.
        unsafe {
            decode_zerovec_in_place(&mut self.account_updates, reader, ctx.as_deref_mut())?;
        }
        self.account_updates_data
            .decode_into(reader, ctx.as_deref_mut())?;
        Ok(())
    }
}

/// Decodes an `Option<ZeroVec<N, T>>` in place, reading the `Option` tag then
/// either clearing the slot or filling the contained `ZeroVec` via
/// [`ZeroVec::decode_into`]. Avoids by-value movement of the potentially-
/// large inner buffer.
pub(crate) fn decode_option_zerovec_into<R, const N: usize, T>(
    slot: &mut Option<ZeroVec<N, T>>,
    reader: &mut R,
    mut ctx: Option<&mut lencode::context::DecoderContext>,
) -> lencode::Result<()>
where
    R: Read,
    T: Decode + 'static,
{
    let tag = u8::decode_ext(reader, ctx.as_deref_mut())?;
    match tag {
        0 => {
            *slot = None;
        }
        1 => {
            let zv = slot.get_or_insert_with(ZeroVec::new);
            zv.decode_into(reader, ctx.as_deref_mut())?;
        }
        _ => return Err(lencode::io::Error::InvalidData),
    }
    Ok(())
}

/// Like [`decode_option_zerovec_into`] but for element types that support
/// in-place decoding via [`DecodeInto`]. Fills the contained `ZeroVec` via
/// [`decode_zerovec_in_place`], avoiding the per-element stack roundtrip
/// that `ZeroVec::decode_into`'s generic non-u8 path would force.
///
/// # Safety
///
/// Same contract as [`decode_zerovec_in_place`]: `T` must accept the
/// all-zero bit pattern as a valid state.
#[inline]
unsafe fn decode_option_zerovec_in_place<R, const N: usize, T>(
    slot: &mut Option<ZeroVec<N, T>>,
    reader: &mut R,
    mut ctx: Option<&mut lencode::context::DecoderContext>,
) -> lencode::Result<()>
where
    R: Read,
    T: DecodeInto + Decode + 'static,
{
    let tag = u8::decode_ext(reader, ctx.as_deref_mut())?;
    match tag {
        0 => {
            *slot = None;
        }
        1 => {
            let zv = slot.get_or_insert_with(ZeroVec::new);
            // SAFETY: forwarded from the caller's contract on `T`.
            unsafe {
                decode_zerovec_in_place(zv, reader, ctx.as_deref_mut())?;
            }
        }
        _ => return Err(lencode::io::Error::InvalidData),
    }
    Ok(())
}

/// Trait for types that support in-place wire decoding into an already-
/// allocated destination — skipping the stack temporary that
/// `Decode::decode_ext` would otherwise create as a by-value return.
///
/// Types with a large inline footprint (e.g. [`CompiledInstruction`] at
/// ~10 KiB) implement this so their container types (`ZeroVec<_, T>`) can
/// decode each element directly into its buffer slot, avoiding a per-element
/// stack roundtrip that the derived `Decode` otherwise forces.
pub(crate) trait DecodeInto {
    fn decode_into<R: Read>(
        &mut self,
        reader: &mut R,
        ctx: Option<&mut lencode::context::DecoderContext>,
    ) -> lencode::Result<()>;
}

impl DecodeInto for CompiledInstruction {
    #[inline]
    fn decode_into<R: Read>(
        &mut self,
        reader: &mut R,
        ctx: Option<&mut lencode::context::DecoderContext>,
    ) -> lencode::Result<()> {
        Self::decode_into(self, reader, ctx)
    }
}

impl DecodeInto for MessageAddressTableLookup {
    #[inline]
    fn decode_into<R: Read>(
        &mut self,
        reader: &mut R,
        ctx: Option<&mut lencode::context::DecoderContext>,
    ) -> lencode::Result<()> {
        Self::decode_into(self, reader, ctx)
    }
}

impl DecodeInto for InnerInstruction {
    #[inline]
    fn decode_into<R: Read>(
        &mut self,
        reader: &mut R,
        ctx: Option<&mut lencode::context::DecoderContext>,
    ) -> lencode::Result<()> {
        Self::decode_into(self, reader, ctx)
    }
}

impl DecodeInto for TxAccountUpdate {
    #[inline]
    fn decode_into<R: Read>(
        &mut self,
        reader: &mut R,
        ctx: Option<&mut lencode::context::DecoderContext>,
    ) -> lencode::Result<()> {
        Self::decode_into(self, reader, ctx)
    }
}

/// Decodes a `ZeroVec<N, T>` in place where `T` has an in-place decoder
/// and accepts the all-zero bit pattern as a valid default state (so
/// uninitialised slots can be zero-primed before `decode_into` writes over
/// their fields). Used for the large-element ZeroVec fields in
/// transactions/messages where routing each element through the by-value
/// `Decode::decode_ext` path would force a per-element stack roundtrip of
/// several KiB.
///
/// # Safety
///
/// The caller asserts that the all-zero bit pattern is a valid `T` — i.e.
/// `T` is some composition of primitive scalars, `Option<_>`, and
/// `ZeroVec`s (all of which treat zero as the empty/default state).
#[inline]
unsafe fn decode_zerovec_in_place<R, const N: usize, T>(
    vec: &mut ZeroVec<N, T>,
    reader: &mut R,
    mut ctx: Option<&mut lencode::context::DecoderContext>,
) -> lencode::Result<()>
where
    R: Read,
    T: DecodeInto + Decode + 'static,
{
    let new_len = ZeroVec::<N, T>::decode_len(reader)?;
    assert!(
        new_len <= N,
        "decoded length too large for ZeroVec: {new_len} > capacity {N}"
    );

    let old_len = vec.len();

    // Drop trailing slots if the new length is smaller.
    if new_len < old_len {
        unsafe {
            let tail =
                core::slice::from_raw_parts_mut(vec.as_mut_ptr().add(new_len), old_len - new_len);
            core::ptr::drop_in_place(tail);
            vec.set_len(new_len);
        }
    }

    // Reuse existing slots in-place (they are already initialised).
    let reuse = new_len.min(old_len);
    for i in 0..reuse {
        // SAFETY: i < reuse ≤ old_len, so the slot is initialised.
        let slot = unsafe { &mut *(vec.as_mut_ptr().add(i)) };
        slot.decode_into(reader, ctx.as_deref_mut())?;
    }

    // Initialise and decode new slots one at a time so that a mid-stream
    // error leaves `vec.len()` pointing at only the fully-initialised prefix.
    while vec.len() < new_len {
        let idx = vec.len();
        // SAFETY: caller guarantees `T` accepts the all-zero bit pattern as a
        // valid state, so we can zero the slot before handing out a `&mut T`.
        // `idx < new_len ≤ N`, so the slot is in-bounds.
        let slot_ptr = unsafe { vec.as_mut_ptr().add(idx) };
        unsafe {
            core::ptr::write_bytes(slot_ptr, 0, 1);
        }
        let slot: &mut T = unsafe { &mut *slot_ptr };
        slot.decode_into(reader, ctx.as_deref_mut())?;
        // SAFETY: slot at `idx` is now initialised; advance the length.
        unsafe {
            vec.set_len(idx + 1);
        }
    }

    Ok(())
}

// --- ZeroAlloc proofs ---
//
// Declare that `Hash`, `Signature`, and every type defined in this module
// contain no heap-owning fields. Each `impl ZeroAlloc` is paired with
// compile-time `assert_zero_alloc::<FieldType>()` calls below that verify
// every listed field type is itself `ZeroAlloc`. If a field is renamed /
// retyped to something non-inline (e.g. a `Vec<u8>` slips back in), the
// corresponding line fails to compile.

impl ZeroAlloc for Hash {}
impl ZeroAlloc for Signature {}

impl ZeroAlloc for MessageHeader {}
impl ZeroAlloc for CompiledInstruction {}
impl ZeroAlloc for MessageAddressTableLookup {}
impl ZeroAlloc for LegacyMessage {}
impl ZeroAlloc for V0Message {}
impl ZeroAlloc for VersionedMessage {}
impl ZeroAlloc for InnerInstruction {}
impl ZeroAlloc for LogMessage {}
impl ZeroAlloc for ReturnData {}
impl ZeroAlloc for Reward {}
impl ZeroAlloc for RewardType {}
impl ZeroAlloc for TransactionStatus {}
impl ZeroAlloc for TokenAmount {}
impl ZeroAlloc for TransactionTokenBalance {}
impl ZeroAlloc for TxAccountUpdate {}
impl ZeroAlloc for PushAccountUpdateError {}
impl ZeroAlloc for Transaction {}

const _: fn() = || {
    // `MessageHeader` fields
    assert_zero_alloc::<u8>(); // num_required_signatures
    assert_zero_alloc::<u8>(); // num_readonly_signed_accounts
    assert_zero_alloc::<u8>(); // num_readonly_unsigned_accounts

    // `CompiledInstruction` fields
    assert_zero_alloc::<u8>(); // program_id_index
    assert_zero_alloc::<ZeroVec<MAX_IX_ACCOUNTS, u8>>(); // accounts
    assert_zero_alloc::<ZeroVec<MAX_IX_DATA_LEN, u8>>(); // data

    // `MessageAddressTableLookup` fields
    assert_zero_alloc::<Address>(); // account_key
    assert_zero_alloc::<ZeroVec<MAX_TX_ACCOUNTS, u8>>(); // writable_indexes
    assert_zero_alloc::<ZeroVec<MAX_TX_ACCOUNTS, u8>>(); // readonly_indexes

    // `LegacyMessage` fields
    assert_zero_alloc::<MessageHeader>();
    assert_zero_alloc::<ZeroVec<MAX_TX_ACCOUNTS, Address>>();
    assert_zero_alloc::<Hash>();
    assert_zero_alloc::<ZeroVec<MAX_TX_INSTRUCTIONS, CompiledInstruction>>();

    // `V0Message` fields (superset of LegacyMessage)
    assert_zero_alloc::<ZeroVec<MAX_TX_ADDR_LOOKUPS, MessageAddressTableLookup>>();

    // `VersionedMessage` is an enum of Legacy/V0 — both already asserted.

    // `InnerInstruction` fields
    assert_zero_alloc::<u8>(); // outer_index
    assert_zero_alloc::<CompiledInstruction>(); // instruction
    assert_zero_alloc::<Option<u32>>(); // stack_height

    // `LogMessage` fields
    assert_zero_alloc::<ZeroVec<MAX_LOG_MSG_LEN, u8>>();

    // `ReturnData` fields
    assert_zero_alloc::<Address>(); // program_id
    assert_zero_alloc::<ZeroVec<MAX_RETURN_DATA_LEN, u8>>(); // data

    // `Reward` fields
    assert_zero_alloc::<i64>();
    assert_zero_alloc::<Option<RewardType>>();
    assert_zero_alloc::<Option<u8>>();

    // `TransactionStatus` fields
    assert_zero_alloc::<u16>(); // error_code
    assert_zero_alloc::<u8>(); // instruction_index
    assert_zero_alloc::<u32>(); // custom_error_code
    assert_zero_alloc::<ZeroVec<MAX_CUSTOM_ERROR_LEN, u8>>(); // error_message

    // `TokenAmount` fields: u64 + u8 (already asserted).

    // `TransactionTokenBalance` fields
    assert_zero_alloc::<TokenAmount>();
    assert_zero_alloc::<Option<Address>>();

    // `TxAccountUpdate` fields: Address + u64 + u64 + bool + u32 (asserted).

    // `PushAccountUpdateError`: fieldless enum; trivially inline.

    // `Transaction` fields
    assert_zero_alloc::<TransactionStatus>();
    assert_zero_alloc::<u64>(); // fee
    assert_zero_alloc::<ZeroVec<MAX_TX_ACCOUNTS, u64>>(); // pre/post balances
    assert_zero_alloc::<Option<ZeroVec<MAX_TX_INNER_IX, InnerInstruction>>>();
    assert_zero_alloc::<Option<ZeroVec<MAX_TX_LOG_MSGS, LogMessage>>>();
    assert_zero_alloc::<Option<ZeroVec<MAX_TX_TOKEN_BALANCES, TransactionTokenBalance>>>();
    assert_zero_alloc::<Option<ZeroVec<MAX_TX_REWARDS, Reward>>>();
    assert_zero_alloc::<Option<ReturnData>>();
    assert_zero_alloc::<Option<u64>>();
    assert_zero_alloc::<ZeroVec<MAX_TX_SIGS, Signature>>();
    assert_zero_alloc::<VersionedMessage>();
    assert_zero_alloc::<ZeroVec<MAX_TX_ACCOUNT_UPDATES, TxAccountUpdate>>();
    assert_zero_alloc::<ZeroVec<MAX_ACCOUNT_DATA_LEN, u8>>(); // account_updates_data

    // Finally, assert the top-level types themselves are ZeroAlloc.
    assert_zero_alloc::<MessageHeader>();
    assert_zero_alloc::<CompiledInstruction>();
    assert_zero_alloc::<MessageAddressTableLookup>();
    assert_zero_alloc::<LegacyMessage>();
    assert_zero_alloc::<V0Message>();
    assert_zero_alloc::<VersionedMessage>();
    assert_zero_alloc::<InnerInstruction>();
    assert_zero_alloc::<LogMessage>();
    assert_zero_alloc::<ReturnData>();
    assert_zero_alloc::<Reward>();
    assert_zero_alloc::<RewardType>();
    assert_zero_alloc::<TransactionStatus>();
    assert_zero_alloc::<TokenAmount>();
    assert_zero_alloc::<TransactionTokenBalance>();
    assert_zero_alloc::<TxAccountUpdate>();
    assert_zero_alloc::<PushAccountUpdateError>();
    assert_zero_alloc::<Transaction>();
};

#[cfg(test)]
mod tests {
    use super::*;

    /// Runs a test closure on a worker thread with a 128 MiB stack.
    ///
    /// Several zero-alloc types in this module are hundreds of KiB to a few
    /// MiB in size. Rust's default test thread stack (2 MiB) is not large
    /// enough to hold intermediate copies during `Default::default()` /
    /// `Decode::decode_ext` even when the final value ends up boxed. In
    /// debug builds (no RVO) these intermediates compound quickly, so we
    /// reserve a generous stack for the worker.
    fn run_big_stack<F: FnOnce() + Send + 'static>(f: F) {
        std::thread::Builder::new()
            .stack_size(128 * 1024 * 1024)
            .spawn(f)
            .expect("spawn")
            .join()
            .expect("join");
    }

    #[test]
    fn new_boxed_has_default_values() {
        // new_boxed allocates directly on the heap; no big-stack worker needed.
        let tx = Transaction::new_boxed();
        assert!(tx.status.is_ok());
        assert_eq!(tx.fee, 0);
        assert_eq!(tx.pre_balances.len(), 0);
        assert_eq!(tx.post_balances.len(), 0);
        assert!(tx.inner_instructions.is_none());
        assert!(tx.log_messages.is_none());
        assert_eq!(tx.signatures.len(), 0);
        assert!(tx.account_updates().is_empty());
        assert!(tx.account_updates_data.is_empty());
    }

    #[test]
    fn compiled_instruction_roundtrip() {
        run_big_stack(|| {
            let mut ix = CompiledInstruction::default();
            ix.program_id_index = 5;
            ix.accounts.extend_from_slice(&[1u8, 2, 3]);
            ix.data.extend_from_slice(b"hello program");

            let mut buf = vec![0u8; 4096];
            let mut cursor = lencode::io::Cursor::new(&mut buf[..]);
            let written = ix.encode_ext(&mut cursor, None).unwrap();

            let mut read_cursor = lencode::io::Cursor::new(&buf[..written]);
            let decoded = CompiledInstruction::decode_ext(&mut read_cursor, None).unwrap();
            assert_eq!(decoded, ix);
        });
    }

    #[test]
    fn legacy_message_roundtrip() {
        run_big_stack(|| {
            let mut msg = LegacyMessage::default();
            msg.header.num_required_signatures = 1;
            msg.account_keys
                .extend_from_slice(&[Address::from([1u8; 32]), Address::from([2u8; 32])]);
            msg.recent_blockhash = Hash::new_from_array([42u8; 32]);

            let mut ix = CompiledInstruction::default();
            ix.program_id_index = 0;
            ix.accounts.extend_from_slice(&[0u8, 1]);
            ix.data.extend_from_slice(b"test");
            msg.instructions.push(ix);

            let mut buf = vec![0u8; 4096];
            let mut cursor = lencode::io::Cursor::new(&mut buf[..]);
            let written = msg.encode_ext(&mut cursor, None).unwrap();

            let mut read_cursor = lencode::io::Cursor::new(&buf[..written]);
            let decoded = LegacyMessage::decode_ext(&mut read_cursor, None).unwrap();
            assert_eq!(decoded, msg);
        });
    }

    #[test]
    fn versioned_message_variants_roundtrip() {
        run_big_stack(|| {
            let legacy = VersionedMessage::Legacy(LegacyMessage::default());
            let v0 = VersionedMessage::V0(V0Message::default());

            for original in [legacy, v0] {
                let mut buf = vec![0u8; 4096];
                let mut cursor = lencode::io::Cursor::new(&mut buf[..]);
                let written = original.encode_ext(&mut cursor, None).unwrap();

                let mut read_cursor = lencode::io::Cursor::new(&buf[..written]);
                let decoded = VersionedMessage::decode_ext(&mut read_cursor, None).unwrap();
                assert_eq!(decoded, original);
            }
        });
    }

    #[test]
    fn inner_instruction_roundtrip() {
        run_big_stack(|| {
            let mut ii = InnerInstruction::default();
            ii.outer_index = 2;
            ii.instruction.program_id_index = 0;
            ii.instruction.data.extend_from_slice(b"ping");
            ii.stack_height = Some(3);

            let mut buf = vec![0u8; 4096];
            let mut cursor = lencode::io::Cursor::new(&mut buf[..]);
            let written = ii.encode_ext(&mut cursor, None).unwrap();

            let mut read_cursor = lencode::io::Cursor::new(&buf[..written]);
            let decoded = InnerInstruction::decode_ext(&mut read_cursor, None).unwrap();
            assert_eq!(decoded, ii);
        });
    }

    /// Builds a tiny `AccountUpdateView` — pure stack, no 10 MiB buffer.
    fn view<'a>(pubkey: [u8; 32], data: &'a [u8]) -> AccountUpdateView<'a> {
        AccountUpdateView {
            pubkey: Address::new_from_array(pubkey),
            lamports: 1_000,
            owner: Address::new_from_array([2u8; 32]),
            executable: false,
            rent_epoch: 0,
            write_version: 1,
            data,
        }
    }

    #[test]
    fn push_account_update_appends_meta_and_data() {
        let mut tx = Transaction::new_boxed();
        let a_pk = [1u8; 32];
        let b_pk = [2u8; 32];

        tx.push_account_update(&view(a_pk, b"first")).unwrap();
        tx.push_account_update(&view(b_pk, b"second data")).unwrap();

        assert_eq!(tx.account_updates().len(), 2);
        // Private arena: the two data blobs are concatenated back-to-back.
        assert_eq!(tx.account_updates_data.len(), 5 + 11);
        assert_eq!(
            tx.account_updates()[0].pubkey,
            Address::new_from_array(a_pk)
        );
        assert_eq!(
            tx.account_updates()[1].pubkey,
            Address::new_from_array(b_pk)
        );
        assert!(tx.account_updates().get(2).is_none());

        let collected: Vec<(Address, &[u8])> = tx
            .iter_account_updates()
            .map(|(m, d)| (m.pubkey, d))
            .collect();
        assert_eq!(collected.len(), 2);
        assert_eq!(collected[0].0, Address::new_from_array(a_pk));
        assert_eq!(collected[0].1, b"first");
        assert_eq!(collected[1].0, Address::new_from_array(b_pk));
        assert_eq!(collected[1].1, b"second data");
    }

    #[test]
    fn push_account_update_returns_meta_full() {
        let mut tx = Transaction::new_boxed();
        let empty = view([9u8; 32], &[]);
        for _ in 0..MAX_TX_ACCOUNT_UPDATES {
            tx.push_account_update(&empty).unwrap();
        }
        let overflow = tx.push_account_update(&empty);
        assert_eq!(overflow, Err(PushAccountUpdateError::MetaFull));
    }

    #[test]
    fn push_error_impls_display_and_error() {
        let e = PushAccountUpdateError::DataArenaFull;
        let _: &dyn std::error::Error = &e;
        assert!(format!("{e}").contains("data arena"));
    }

    #[test]
    fn clear_resets_account_update_state() {
        let mut tx = Transaction::new_boxed();
        tx.push_account_update(&view([5u8; 32], b"payload"))
            .unwrap();
        assert_eq!(tx.account_updates().len(), 1);
        assert_eq!(tx.account_updates_data.len(), 7);

        tx.clear();
        assert!(tx.account_updates().is_empty());
        assert!(tx.account_updates_data.is_empty());
    }

    #[test]
    fn transaction_decode_into_roundtrip_on_default_stack() {
        // Proves `Transaction::decode_into` works on the default 2 MiB
        // test-thread stack that the stock `Decode::decode_ext` would
        // overflow. The full decode path — including `VersionedMessage`
        // variant swapping — is in-place.
        let mut src = Transaction::new_boxed();
        src.fee = 5_000;
        src.status.error_code = 0;
        src.signatures.push(Signature::default());
        src.push_account_update(&view([33u8; 32], b"hello"))
            .unwrap();
        src.push_account_update(&view([44u8; 32], b"world!"))
            .unwrap();

        let mut buf = vec![0u8; 1 << 20];
        let mut cursor = lencode::io::Cursor::new(&mut buf[..]);
        let written = src.encode_ext(&mut cursor, None).expect("encode");

        let mut dst = Transaction::new_boxed();
        let mut read_cursor = lencode::io::Cursor::new(&buf[..written]);
        dst.decode_into(&mut read_cursor, None)
            .expect("decode_into");

        assert_eq!(dst.fee, src.fee);
        assert_eq!(dst.account_updates().len(), 2);
        let collected: Vec<(Address, &[u8])> = dst
            .iter_account_updates()
            .map(|(m, d)| (m.pubkey, d))
            .collect();
        assert_eq!(collected[0].0, Address::new_from_array([33u8; 32]));
        assert_eq!(collected[0].1, b"hello");
        assert_eq!(collected[1].0, Address::new_from_array([44u8; 32]));
        assert_eq!(collected[1].1, b"world!");
    }

    #[test]
    fn transaction_decode_into_is_reusable_across_decodes() {
        // Tight-loop simulation: decode many wire-encoded transactions
        // into the same Box<Transaction> buffer on the default test
        // stack, with no per-iteration heap allocations.
        let mut dst = Transaction::new_boxed();
        for i in 0..8u8 {
            let mut src = Transaction::new_boxed();
            src.fee = (i as u64) * 1000;
            src.push_account_update(&view([i; 32], &[i; 64])).unwrap();

            let mut buf = vec![0u8; 1 << 16];
            let mut cursor = lencode::io::Cursor::new(&mut buf[..]);
            let written = src.encode_ext(&mut cursor, None).unwrap();

            let mut read_cursor = lencode::io::Cursor::new(&buf[..written]);
            dst.decode_into(&mut read_cursor, None)
                .expect("decode_into");

            assert_eq!(dst.fee, (i as u64) * 1000);
            assert_eq!(dst.account_updates().len(), 1);
            let (meta, data) = dst.iter_account_updates().next().unwrap();
            assert_eq!(meta.pubkey, Address::new_from_array([i; 32]));
            assert_eq!(data, &[i; 64][..]);
        }
    }

    /// Heap-allocates a `VersionedMessage` without routing the ~750 KiB
    /// struct through the stack. Used by `decode_into` tests that can't
    /// afford to put a full message on the test thread's 2 MiB stack.
    fn new_boxed_message() -> Box<VersionedMessage> {
        use std::alloc::{Layout, alloc_zeroed, handle_alloc_error};
        let layout = Layout::new::<VersionedMessage>();
        unsafe {
            let ptr = alloc_zeroed(layout) as *mut VersionedMessage;
            if ptr.is_null() {
                handle_alloc_error(layout);
            }
            // SAFETY: `#[repr(C, u8)]` puts the discriminant at byte 0.
            // All-zero bytes = Legacy variant with a zero-init LegacyMessage
            // (every field has 0 as a valid bit pattern).
            Box::from_raw(ptr)
        }
    }

    #[test]
    fn versioned_message_decode_into_swaps_variants_in_place() {
        // Start from Legacy, decode a V0 wire payload into it, then
        // decode a Legacy wire payload back into the same slot. Neither
        // direction stack-allocates a full message.
        let mut dst = new_boxed_message();

        // --- Legacy slot → decode V0 (variant swap) ---
        let mut src_v0 = new_boxed_message();
        // Flip the heap-resident default (Legacy) into V0 in place.
        unsafe {
            core::ptr::drop_in_place(&mut *src_v0 as *mut VersionedMessage);
            core::ptr::write_bytes(&mut *src_v0 as *mut VersionedMessage, 0, 1);
            *(&mut *src_v0 as *mut VersionedMessage as *mut u8) = 1;
        }
        if let VersionedMessage::V0(m) = &mut *src_v0 {
            m.header.num_required_signatures = 3;
            m.recent_blockhash = Hash::new_from_array([9u8; 32]);
        } else {
            panic!("expected V0 after variant write");
        }

        let mut buf = vec![0u8; 4096];
        let mut cursor = lencode::io::Cursor::new(&mut buf[..]);
        let written = src_v0.encode_ext(&mut cursor, None).expect("encode v0");

        let mut read_cursor = lencode::io::Cursor::new(&buf[..written]);
        dst.decode_into(&mut read_cursor, None)
            .expect("decode_into v0");
        match &*dst {
            VersionedMessage::V0(m) => {
                assert_eq!(m.header.num_required_signatures, 3);
                assert_eq!(m.recent_blockhash, Hash::new_from_array([9u8; 32]));
            }
            _ => panic!("expected V0 after swap"),
        }

        // --- V0 slot → decode Legacy (swap back) ---
        let mut src_legacy = new_boxed_message();
        if let VersionedMessage::Legacy(m) = &mut *src_legacy {
            m.header.num_required_signatures = 7;
        }
        let mut cursor = lencode::io::Cursor::new(&mut buf[..]);
        let written = src_legacy
            .encode_ext(&mut cursor, None)
            .expect("encode legacy");

        let mut read_cursor = lencode::io::Cursor::new(&buf[..written]);
        dst.decode_into(&mut read_cursor, None)
            .expect("decode_into legacy");
        match &*dst {
            VersionedMessage::Legacy(m) => {
                assert_eq!(m.header.num_required_signatures, 7);
            }
            _ => panic!("expected Legacy after swap back"),
        }
    }

    #[test]
    fn clear_account_updates_only_touches_the_arena() {
        // clear_account_updates should reset the flat arena but leave the
        // rest of the transaction (fee, signatures, …) alone.
        let mut tx = Transaction::new_boxed();
        tx.fee = 42;
        tx.push_account_update(&view([7u8; 32], b"x")).unwrap();
        assert_eq!(tx.account_updates().len(), 1);

        tx.clear_account_updates();
        assert!(tx.account_updates().is_empty());
        assert!(tx.account_updates_data.is_empty());
        assert_eq!(tx.fee, 42);
    }

    #[test]
    fn transaction_status_ok_and_error() {
        // TransactionStatus is small (inline_error_message ≤ 256 bytes) — no big
        // stack needed.
        let ok = TransactionStatus::default();
        assert!(ok.is_ok());

        let mut err = TransactionStatus::default();
        err.error_code = 1;
        err.instruction_index = 3;
        err.error_message.extend_from_slice(b"oops");
        assert!(!err.is_ok());

        for status in [ok, err] {
            let mut buf = vec![0u8; 512];
            let mut cursor = lencode::io::Cursor::new(&mut buf[..]);
            let written = status.encode_ext(&mut cursor, None).unwrap();

            let mut read_cursor = lencode::io::Cursor::new(&buf[..written]);
            let decoded = TransactionStatus::decode_ext(&mut read_cursor, None).unwrap();
            assert_eq!(decoded, status);
        }
    }
}
