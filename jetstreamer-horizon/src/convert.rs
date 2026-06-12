//! Lossless conversions from upstream Solana runtime types into horizon's
//! zero-alloc archive types.
//!
//! The entry point is [`populate_transaction`], which fills a reusable
//! [`Transaction`] scratch from a `VersionedTransaction` plus its original
//! [`TransactionStatusMeta`] (as delivered by geyser / old-faithful CAR
//! data). All sub-conversions are exposed for reuse.
//!
//! # Error-status codec
//!
//! `Result<(), TransactionError>` maps to the compact
//! [`TransactionStatus`] wire form as follows:
//!
//! - `error_code == 0` ⇒ `Ok(())`.
//! - low byte of `error_code` = `TransactionError` variant code + 1
//!   (see [`tx_error_code`]).
//! - high byte of `error_code` = `InstructionError` variant code + 1 when
//!   the transaction error is `InstructionError`, else 0.
//! - `instruction_index` carries the `u8` payload of `InstructionError`,
//!   `DuplicateInstruction`, `InsufficientFundsForRent`, and
//!   `ProgramExecutionTemporarilyRestricted`.
//! - `custom_error_code` carries the `u32` payload of
//!   `InstructionError(_, Custom(code))`.
//!
//! The variant code tables are frozen — they are part of the archive wire
//! format and must never be renumbered, even if upstream reorders its
//! enums. [`status_from_result`] / [`result_from_status`] round-trip every
//! representable error (covered by tests).

use core::str::FromStr;

use solana_address::Address;
use solana_instruction_error::InstructionError;
use solana_transaction::versioned::VersionedTransaction;
use solana_transaction_error::TransactionError;
use solana_transaction_status::{
    TransactionStatusMeta, TransactionTokenBalance as UpstreamTokenBalance,
};

use crate::limits::{
    MAX_IX_ACCOUNTS, MAX_IX_DATA_LEN, MAX_RETURN_DATA_LEN, MAX_TX_ACCOUNTS, MAX_TX_ADDR_LOOKUPS,
    MAX_TX_INSTRUCTIONS, MAX_TX_LOG_MSGS, MAX_TX_REWARDS, MAX_TX_SIGS, MAX_TX_TOKEN_BALANCES,
};
use crate::transactions::{
    CompiledInstruction, InnerInstruction, LegacyMessage, Reward, RewardType, TokenAmount,
    Transaction, TransactionStatus, TransactionTokenBalance, V0Message, VersionedMessage,
};
use crate::zero_vec::ZeroVec;

/// Error converting an upstream Solana value into a horizon type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConvertError {
    /// A list or blob exceeds the fixed capacity reserved for it. The
    /// archive format never truncates silently — hitting this means the
    /// corresponding limit in [`crate::limits`] needs raising.
    CapacityExceeded {
        field: &'static str,
        len: usize,
        max: usize,
    },
    /// A base58 pubkey string failed to parse.
    InvalidAddress { field: &'static str },
    /// A token-amount string failed to parse as a raw `u64`.
    InvalidTokenAmount,
    /// A [`TransactionStatus`] carries an error code outside the frozen
    /// codec tables (corrupt data or a newer writer).
    UnknownErrorCode { error_code: u16 },
}

impl core::fmt::Display for ConvertError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::CapacityExceeded { field, len, max } => {
                write!(f, "{field}: length {len} exceeds inline capacity {max}")
            }
            Self::InvalidAddress { field } => write!(f, "{field}: invalid base58 address"),
            Self::InvalidTokenAmount => write!(f, "token amount is not a valid u64"),
            Self::UnknownErrorCode { error_code } => {
                write!(f, "unknown transaction status error code {error_code:#06x}")
            }
        }
    }
}

impl std::error::Error for ConvertError {}

#[inline]
fn check_capacity(field: &'static str, len: usize, max: usize) -> Result<(), ConvertError> {
    if len > max {
        return Err(ConvertError::CapacityExceeded { field, len, max });
    }
    Ok(())
}

/// Appends a default-initialised element to `zv` and returns a mutable
/// reference to it, writing directly into the spare slot so the (possibly
/// multi-KiB) element is never constructed as a by-value stack temporary
/// and moved.
fn push_in_place<'a, const N: usize, T: Default>(
    zv: &'a mut ZeroVec<N, T>,
    field: &'static str,
) -> Result<&'a mut T, ConvertError> {
    let len = zv.len();
    if len >= N {
        return Err(ConvertError::CapacityExceeded {
            field,
            len: len + 1,
            max: N,
        });
    }
    zv.spare_capacity_mut()[0].write(T::default());
    // SAFETY: the slot at `len` was just initialised above.
    unsafe { zv.set_len(len + 1) };
    Ok(zv.get_mut(len).expect("element just pushed"))
}

#[inline]
fn address_from_pubkey(pubkey: &solana_pubkey::Pubkey) -> Address {
    Address::new_from_array(pubkey.to_bytes())
}

#[inline]
fn parse_address(s: &str, field: &'static str) -> Result<Address, ConvertError> {
    Address::from_str(s).map_err(|_| ConvertError::InvalidAddress { field })
}

/// Parses an optional address field that upstream encodes as a `String`,
/// with the empty string meaning "absent".
#[inline]
fn parse_optional_address(s: &str, field: &'static str) -> Result<Option<Address>, ConvertError> {
    if s.is_empty() {
        return Ok(None);
    }
    parse_address(s, field).map(Some)
}

// ---------------------------------------------------------------------
// Error-status codec
// ---------------------------------------------------------------------

/// Frozen wire code for each `TransactionError` variant. Part of the
/// archive format — never renumber.
pub fn tx_error_code(err: &TransactionError) -> u16 {
    match err {
        TransactionError::AccountInUse => 0,
        TransactionError::AccountLoadedTwice => 1,
        TransactionError::AccountNotFound => 2,
        TransactionError::ProgramAccountNotFound => 3,
        TransactionError::InsufficientFundsForFee => 4,
        TransactionError::InvalidAccountForFee => 5,
        TransactionError::AlreadyProcessed => 6,
        TransactionError::BlockhashNotFound => 7,
        TransactionError::InstructionError(..) => 8,
        TransactionError::CallChainTooDeep => 9,
        TransactionError::MissingSignatureForFee => 10,
        TransactionError::InvalidAccountIndex => 11,
        TransactionError::SignatureFailure => 12,
        TransactionError::InvalidProgramForExecution => 13,
        TransactionError::SanitizeFailure => 14,
        TransactionError::ClusterMaintenance => 15,
        TransactionError::AccountBorrowOutstanding => 16,
        TransactionError::WouldExceedMaxBlockCostLimit => 17,
        TransactionError::UnsupportedVersion => 18,
        TransactionError::InvalidWritableAccount => 19,
        TransactionError::WouldExceedMaxAccountCostLimit => 20,
        TransactionError::WouldExceedAccountDataBlockLimit => 21,
        TransactionError::TooManyAccountLocks => 22,
        TransactionError::AddressLookupTableNotFound => 23,
        TransactionError::InvalidAddressLookupTableOwner => 24,
        TransactionError::InvalidAddressLookupTableData => 25,
        TransactionError::InvalidAddressLookupTableIndex => 26,
        TransactionError::InvalidRentPayingAccount => 27,
        TransactionError::WouldExceedMaxVoteCostLimit => 28,
        TransactionError::WouldExceedAccountDataTotalLimit => 29,
        TransactionError::DuplicateInstruction(..) => 30,
        TransactionError::InsufficientFundsForRent { .. } => 31,
        TransactionError::MaxLoadedAccountsDataSizeExceeded => 32,
        TransactionError::InvalidLoadedAccountsDataSizeLimit => 33,
        TransactionError::ResanitizationNeeded => 34,
        TransactionError::ProgramExecutionTemporarilyRestricted { .. } => 35,
        TransactionError::UnbalancedTransaction => 36,
        TransactionError::ProgramCacheHitMaxLimit => 37,
        TransactionError::CommitCancelled => 38,
    }
}

/// Frozen wire code for each `InstructionError` variant. Part of the
/// archive format — never renumber.
// Deprecated variants (e.g. `NotEnoughAccountKeys`) still occur in
// historical chain data, so the codec must keep representing them.
#[allow(deprecated)]
pub fn instruction_error_code(err: &InstructionError) -> u16 {
    match err {
        InstructionError::GenericError => 0,
        InstructionError::InvalidArgument => 1,
        InstructionError::InvalidInstructionData => 2,
        InstructionError::InvalidAccountData => 3,
        InstructionError::AccountDataTooSmall => 4,
        InstructionError::InsufficientFunds => 5,
        InstructionError::IncorrectProgramId => 6,
        InstructionError::MissingRequiredSignature => 7,
        InstructionError::AccountAlreadyInitialized => 8,
        InstructionError::UninitializedAccount => 9,
        InstructionError::UnbalancedInstruction => 10,
        InstructionError::ModifiedProgramId => 11,
        InstructionError::ExternalAccountLamportSpend => 12,
        InstructionError::ExternalAccountDataModified => 13,
        InstructionError::ReadonlyLamportChange => 14,
        InstructionError::ReadonlyDataModified => 15,
        InstructionError::DuplicateAccountIndex => 16,
        InstructionError::ExecutableModified => 17,
        InstructionError::RentEpochModified => 18,
        InstructionError::NotEnoughAccountKeys => 19,
        InstructionError::AccountDataSizeChanged => 20,
        InstructionError::AccountNotExecutable => 21,
        InstructionError::AccountBorrowFailed => 22,
        InstructionError::AccountBorrowOutstanding => 23,
        InstructionError::DuplicateAccountOutOfSync => 24,
        InstructionError::Custom(..) => 25,
        InstructionError::InvalidError => 26,
        InstructionError::ExecutableDataModified => 27,
        InstructionError::ExecutableLamportChange => 28,
        InstructionError::ExecutableAccountNotRentExempt => 29,
        InstructionError::UnsupportedProgramId => 30,
        InstructionError::CallDepth => 31,
        InstructionError::MissingAccount => 32,
        InstructionError::ReentrancyNotAllowed => 33,
        InstructionError::MaxSeedLengthExceeded => 34,
        InstructionError::InvalidSeeds => 35,
        InstructionError::InvalidRealloc => 36,
        InstructionError::ComputationalBudgetExceeded => 37,
        InstructionError::PrivilegeEscalation => 38,
        InstructionError::ProgramEnvironmentSetupFailure => 39,
        InstructionError::ProgramFailedToComplete => 40,
        InstructionError::ProgramFailedToCompile => 41,
        InstructionError::Immutable => 42,
        InstructionError::IncorrectAuthority => 43,
        InstructionError::BorshIoError => 44,
        InstructionError::AccountNotRentExempt => 45,
        InstructionError::InvalidAccountOwner => 46,
        InstructionError::ArithmeticOverflow => 47,
        InstructionError::UnsupportedSysvar => 48,
        InstructionError::IllegalOwner => 49,
        InstructionError::MaxAccountsDataAllocationsExceeded => 50,
        InstructionError::MaxAccountsExceeded => 51,
        InstructionError::MaxInstructionTraceLengthExceeded => 52,
        InstructionError::BuiltinProgramsMustConsumeComputeUnits => 53,
    }
}

// Deprecated variants still occur in historical chain data; see
// `instruction_error_code`.
#[allow(deprecated)]
fn instruction_error_from_code(
    code: u16,
    custom_error_code: u32,
) -> Result<InstructionError, ConvertError> {
    Ok(match code {
        0 => InstructionError::GenericError,
        1 => InstructionError::InvalidArgument,
        2 => InstructionError::InvalidInstructionData,
        3 => InstructionError::InvalidAccountData,
        4 => InstructionError::AccountDataTooSmall,
        5 => InstructionError::InsufficientFunds,
        6 => InstructionError::IncorrectProgramId,
        7 => InstructionError::MissingRequiredSignature,
        8 => InstructionError::AccountAlreadyInitialized,
        9 => InstructionError::UninitializedAccount,
        10 => InstructionError::UnbalancedInstruction,
        11 => InstructionError::ModifiedProgramId,
        12 => InstructionError::ExternalAccountLamportSpend,
        13 => InstructionError::ExternalAccountDataModified,
        14 => InstructionError::ReadonlyLamportChange,
        15 => InstructionError::ReadonlyDataModified,
        16 => InstructionError::DuplicateAccountIndex,
        17 => InstructionError::ExecutableModified,
        18 => InstructionError::RentEpochModified,
        19 => InstructionError::NotEnoughAccountKeys,
        20 => InstructionError::AccountDataSizeChanged,
        21 => InstructionError::AccountNotExecutable,
        22 => InstructionError::AccountBorrowFailed,
        23 => InstructionError::AccountBorrowOutstanding,
        24 => InstructionError::DuplicateAccountOutOfSync,
        25 => InstructionError::Custom(custom_error_code),
        26 => InstructionError::InvalidError,
        27 => InstructionError::ExecutableDataModified,
        28 => InstructionError::ExecutableLamportChange,
        29 => InstructionError::ExecutableAccountNotRentExempt,
        30 => InstructionError::UnsupportedProgramId,
        31 => InstructionError::CallDepth,
        32 => InstructionError::MissingAccount,
        33 => InstructionError::ReentrancyNotAllowed,
        34 => InstructionError::MaxSeedLengthExceeded,
        35 => InstructionError::InvalidSeeds,
        36 => InstructionError::InvalidRealloc,
        37 => InstructionError::ComputationalBudgetExceeded,
        38 => InstructionError::PrivilegeEscalation,
        39 => InstructionError::ProgramEnvironmentSetupFailure,
        40 => InstructionError::ProgramFailedToComplete,
        41 => InstructionError::ProgramFailedToCompile,
        42 => InstructionError::Immutable,
        43 => InstructionError::IncorrectAuthority,
        44 => InstructionError::BorshIoError,
        45 => InstructionError::AccountNotRentExempt,
        46 => InstructionError::InvalidAccountOwner,
        47 => InstructionError::ArithmeticOverflow,
        48 => InstructionError::UnsupportedSysvar,
        49 => InstructionError::IllegalOwner,
        50 => InstructionError::MaxAccountsDataAllocationsExceeded,
        51 => InstructionError::MaxAccountsExceeded,
        52 => InstructionError::MaxInstructionTraceLengthExceeded,
        53 => InstructionError::BuiltinProgramsMustConsumeComputeUnits,
        other => {
            return Err(ConvertError::UnknownErrorCode {
                error_code: (other + 1) << 8,
            });
        }
    })
}

/// Encodes an execution result into the compact [`TransactionStatus`]
/// wire form. Total function — every upstream error is representable.
pub fn status_from_result(result: &Result<(), TransactionError>) -> TransactionStatus {
    let err = match result {
        Ok(()) => return TransactionStatus::OK,
        Err(err) => err,
    };
    let mut status = TransactionStatus {
        error_code: tx_error_code(err) + 1,
        instruction_index: 0,
        custom_error_code: 0,
        error_message: ZeroVec::new(),
    };
    match err {
        TransactionError::InstructionError(index, ie) => {
            status.error_code |= (instruction_error_code(ie) + 1) << 8;
            status.instruction_index = *index;
            if let InstructionError::Custom(code) = ie {
                status.custom_error_code = *code;
            }
        }
        TransactionError::DuplicateInstruction(index) => status.instruction_index = *index,
        TransactionError::InsufficientFundsForRent { account_index }
        | TransactionError::ProgramExecutionTemporarilyRestricted { account_index } => {
            status.instruction_index = *account_index;
        }
        _ => {}
    }
    status
}

/// Decodes a [`TransactionStatus`] back into the upstream execution
/// result. Exact inverse of [`status_from_result`].
pub fn result_from_status(
    status: &TransactionStatus,
) -> Result<Result<(), TransactionError>, ConvertError> {
    if status.error_code == 0 {
        return Ok(Ok(()));
    }
    let tx_code = (status.error_code & 0xFF) - 1;
    let ie_code = status.error_code >> 8;
    let err = match tx_code {
        0 => TransactionError::AccountInUse,
        1 => TransactionError::AccountLoadedTwice,
        2 => TransactionError::AccountNotFound,
        3 => TransactionError::ProgramAccountNotFound,
        4 => TransactionError::InsufficientFundsForFee,
        5 => TransactionError::InvalidAccountForFee,
        6 => TransactionError::AlreadyProcessed,
        7 => TransactionError::BlockhashNotFound,
        8 => {
            if ie_code == 0 {
                return Err(ConvertError::UnknownErrorCode {
                    error_code: status.error_code,
                });
            }
            TransactionError::InstructionError(
                status.instruction_index,
                instruction_error_from_code(ie_code - 1, status.custom_error_code)?,
            )
        }
        9 => TransactionError::CallChainTooDeep,
        10 => TransactionError::MissingSignatureForFee,
        11 => TransactionError::InvalidAccountIndex,
        12 => TransactionError::SignatureFailure,
        13 => TransactionError::InvalidProgramForExecution,
        14 => TransactionError::SanitizeFailure,
        15 => TransactionError::ClusterMaintenance,
        16 => TransactionError::AccountBorrowOutstanding,
        17 => TransactionError::WouldExceedMaxBlockCostLimit,
        18 => TransactionError::UnsupportedVersion,
        19 => TransactionError::InvalidWritableAccount,
        20 => TransactionError::WouldExceedMaxAccountCostLimit,
        21 => TransactionError::WouldExceedAccountDataBlockLimit,
        22 => TransactionError::TooManyAccountLocks,
        23 => TransactionError::AddressLookupTableNotFound,
        24 => TransactionError::InvalidAddressLookupTableOwner,
        25 => TransactionError::InvalidAddressLookupTableData,
        26 => TransactionError::InvalidAddressLookupTableIndex,
        27 => TransactionError::InvalidRentPayingAccount,
        28 => TransactionError::WouldExceedMaxVoteCostLimit,
        29 => TransactionError::WouldExceedAccountDataTotalLimit,
        30 => TransactionError::DuplicateInstruction(status.instruction_index),
        31 => TransactionError::InsufficientFundsForRent {
            account_index: status.instruction_index,
        },
        32 => TransactionError::MaxLoadedAccountsDataSizeExceeded,
        33 => TransactionError::InvalidLoadedAccountsDataSizeLimit,
        34 => TransactionError::ResanitizationNeeded,
        35 => TransactionError::ProgramExecutionTemporarilyRestricted {
            account_index: status.instruction_index,
        },
        36 => TransactionError::UnbalancedTransaction,
        37 => TransactionError::ProgramCacheHitMaxLimit,
        38 => TransactionError::CommitCancelled,
        _ => {
            return Err(ConvertError::UnknownErrorCode {
                error_code: status.error_code,
            });
        }
    };
    // Reject a stray instruction-error code on a non-InstructionError
    // variant — it would be silently dropped on re-encode.
    if tx_code != 8 && ie_code != 0 {
        return Err(ConvertError::UnknownErrorCode {
            error_code: status.error_code,
        });
    }
    Ok(Err(err))
}

// ---------------------------------------------------------------------
// Reward / token-balance / message conversions
// ---------------------------------------------------------------------

/// Converts an upstream reward-type tag.
pub fn reward_type_from_upstream(value: solana_reward_info::RewardType) -> RewardType {
    match value {
        solana_reward_info::RewardType::Fee => RewardType::Fee,
        solana_reward_info::RewardType::Rent => RewardType::Rent,
        solana_reward_info::RewardType::Staking => RewardType::Staking,
        solana_reward_info::RewardType::Voting => RewardType::Voting,
    }
}

/// Converts a `solana_transaction_status::Reward` (string pubkey) record.
pub fn reward_from_status_reward(
    src: &solana_transaction_status::Reward,
) -> Result<Reward, ConvertError> {
    Ok(Reward {
        pubkey: parse_address(&src.pubkey, "reward.pubkey")?,
        lamports: src.lamports,
        post_balance: src.post_balance,
        reward_type: src.reward_type.map(reward_type_from_upstream),
        commission: src.commission,
    })
}

/// Builds a horizon reward record from a keyed `RewardInfo` (the shape the
/// runtime emits at block boundaries).
pub fn reward_from_info(pubkey: Address, info: &solana_reward_info::RewardInfo) -> Reward {
    Reward {
        pubkey,
        lamports: info.lamports,
        post_balance: info.post_balance,
        reward_type: Some(reward_type_from_upstream(info.reward_type)),
        commission: info.commission,
    }
}

fn token_balance_from_upstream(
    src: &UpstreamTokenBalance,
) -> Result<TransactionTokenBalance, ConvertError> {
    Ok(TransactionTokenBalance {
        account_index: src.account_index,
        mint: parse_address(&src.mint, "token_balance.mint")?,
        ui_token_amount: TokenAmount {
            amount: src
                .ui_token_amount
                .amount
                .parse()
                .map_err(|_| ConvertError::InvalidTokenAmount)?,
            decimals: src.ui_token_amount.decimals,
        },
        owner: parse_optional_address(&src.owner, "token_balance.owner")?,
        program_id: parse_optional_address(&src.program_id, "token_balance.program_id")?,
    })
}

fn populate_compiled_instruction(
    dst: &mut CompiledInstruction,
    src: &solana_message::compiled_instruction::CompiledInstruction,
) -> Result<(), ConvertError> {
    check_capacity("instruction.accounts", src.accounts.len(), MAX_IX_ACCOUNTS)?;
    check_capacity("instruction.data", src.data.len(), MAX_IX_DATA_LEN)?;
    dst.program_id_index = src.program_id_index;
    dst.accounts.set(&src.accounts);
    dst.data.set(&src.data);
    Ok(())
}

fn populate_message_common(
    account_keys: &mut ZeroVec<MAX_TX_ACCOUNTS, Address>,
    instructions: &mut ZeroVec<MAX_TX_INSTRUCTIONS, CompiledInstruction>,
    src_keys: &[solana_pubkey::Pubkey],
    src_instructions: &[solana_message::compiled_instruction::CompiledInstruction],
) -> Result<(), ConvertError> {
    check_capacity("message.account_keys", src_keys.len(), MAX_TX_ACCOUNTS)?;
    for key in src_keys {
        account_keys.push(address_from_pubkey(key));
    }
    for ix in src_instructions {
        let slot = push_in_place(instructions, "message.instructions")?;
        populate_compiled_instruction(slot, ix)?;
    }
    Ok(())
}

/// Fills a horizon [`VersionedMessage`] in place from the upstream message,
/// preserving the legacy/v0 distinction.
pub fn populate_message(
    dst: &mut VersionedMessage,
    src: &solana_message::VersionedMessage,
) -> Result<(), ConvertError> {
    match src {
        solana_message::VersionedMessage::Legacy(m) => {
            let dst: &mut LegacyMessage = dst.force_legacy_mut();
            dst.header.num_required_signatures = m.header.num_required_signatures;
            dst.header.num_readonly_signed_accounts = m.header.num_readonly_signed_accounts;
            dst.header.num_readonly_unsigned_accounts = m.header.num_readonly_unsigned_accounts;
            dst.recent_blockhash = m.recent_blockhash;
            populate_message_common(
                &mut dst.account_keys,
                &mut dst.instructions,
                &m.account_keys,
                &m.instructions,
            )?;
        }
        solana_message::VersionedMessage::V0(m) => {
            let dst: &mut V0Message = dst.force_v0_mut();
            dst.header.num_required_signatures = m.header.num_required_signatures;
            dst.header.num_readonly_signed_accounts = m.header.num_readonly_signed_accounts;
            dst.header.num_readonly_unsigned_accounts = m.header.num_readonly_unsigned_accounts;
            dst.recent_blockhash = m.recent_blockhash;
            populate_message_common(
                &mut dst.account_keys,
                &mut dst.instructions,
                &m.account_keys,
                &m.instructions,
            )?;
            check_capacity(
                "message.address_table_lookups",
                m.address_table_lookups.len(),
                MAX_TX_ADDR_LOOKUPS,
            )?;
            for lookup in &m.address_table_lookups {
                check_capacity(
                    "lookup.writable_indexes",
                    lookup.writable_indexes.len(),
                    MAX_TX_ACCOUNTS,
                )?;
                check_capacity(
                    "lookup.readonly_indexes",
                    lookup.readonly_indexes.len(),
                    MAX_TX_ACCOUNTS,
                )?;
                let slot = push_in_place(&mut dst.address_table_lookups, "address_table_lookups")?;
                slot.account_key = address_from_pubkey(&lookup.account_key);
                slot.writable_indexes.set(&lookup.writable_indexes);
                slot.readonly_indexes.set(&lookup.readonly_indexes);
            }
        }
    }
    Ok(())
}

// The `populate_*` helpers below each materialise one large
// `Option<ZeroVec<…>>` payload. They are deliberately separate,
// never-inlined functions: `get_or_insert_with(ZeroVec::new)` reserves the
// full inline buffer (hundreds of KiB for inner instructions / logs) in
// the calling frame, and inlining them all into `populate_transaction`
// would stack those reservations up in a single frame — overflow on
// default-size threads in debug builds.

#[inline(never)]
fn populate_inner_instructions(
    dst: &mut Option<ZeroVec<{ crate::limits::MAX_TX_INNER_IX }, InnerInstruction>>,
    groups: &[solana_transaction_status::InnerInstructions],
) -> Result<(), ConvertError> {
    let list = dst.get_or_insert_with(ZeroVec::new);
    list.clear();
    for group in groups {
        for inner in &group.instructions {
            let slot: &mut InnerInstruction = push_in_place(list, "inner_instructions")?;
            slot.outer_index = group.index;
            slot.stack_height = inner.stack_height;
            populate_compiled_instruction(&mut slot.instruction, &inner.instruction)?;
        }
    }
    Ok(())
}

#[inline(never)]
fn populate_log_messages(
    dst: &mut Option<crate::transactions::LogMessages>,
    logs: &[String],
) -> Result<(), ConvertError> {
    check_capacity("log_messages", logs.len(), MAX_TX_LOG_MSGS)?;
    let arena = dst.get_or_insert_with(Default::default);
    arena.clear();
    let mut total = 0usize;
    for line in logs {
        total += line.len();
        arena
            .push(line.as_bytes())
            .map_err(|_| ConvertError::CapacityExceeded {
                field: "log_messages.data",
                len: total,
                max: crate::limits::MAX_TX_LOG_DATA,
            })?;
    }
    Ok(())
}

#[inline(never)]
fn populate_token_balances(
    dst: &mut Option<ZeroVec<MAX_TX_TOKEN_BALANCES, TransactionTokenBalance>>,
    balances: &[UpstreamTokenBalance],
    field: &'static str,
) -> Result<(), ConvertError> {
    check_capacity(field, balances.len(), MAX_TX_TOKEN_BALANCES)?;
    let list = dst.get_or_insert_with(ZeroVec::new);
    list.clear();
    for balance in balances {
        list.push(token_balance_from_upstream(balance)?);
    }
    Ok(())
}

#[inline(never)]
fn populate_rewards(
    dst: &mut Option<ZeroVec<MAX_TX_REWARDS, Reward>>,
    rewards: &[solana_transaction_status::Reward],
) -> Result<(), ConvertError> {
    check_capacity("rewards", rewards.len(), MAX_TX_REWARDS)?;
    let list = dst.get_or_insert_with(ZeroVec::new);
    list.clear();
    for reward in rewards {
        list.push(reward_from_status_reward(reward)?);
    }
    Ok(())
}

/// Fills a reusable horizon [`Transaction`] scratch from an upstream
/// transaction plus its original status metadata.
///
/// The scratch is fully [`Transaction::clear`]ed first; the account-update
/// arena is left empty for the caller to fill (updates come from replay,
/// not from the status meta).
pub fn populate_transaction(
    dst: &mut Transaction,
    tx: &VersionedTransaction,
    meta: &TransactionStatusMeta,
) -> Result<(), ConvertError> {
    dst.clear();

    // --- identity ---
    check_capacity("signatures", tx.signatures.len(), MAX_TX_SIGS)?;
    for sig in &tx.signatures {
        dst.signatures.push(*sig);
    }
    populate_message(&mut dst.message, &tx.message)?;

    // --- status metadata ---
    dst.status = status_from_result(&meta.status);
    dst.fee = meta.fee;
    check_capacity("pre_balances", meta.pre_balances.len(), MAX_TX_ACCOUNTS)?;
    dst.pre_balances.set(&meta.pre_balances);
    check_capacity("post_balances", meta.post_balances.len(), MAX_TX_ACCOUNTS)?;
    dst.post_balances.set(&meta.post_balances);

    check_capacity(
        "loaded_addresses.writable",
        meta.loaded_addresses.writable.len(),
        MAX_TX_ACCOUNTS,
    )?;
    for key in &meta.loaded_addresses.writable {
        dst.loaded_writable_addresses.push(address_from_pubkey(key));
    }
    check_capacity(
        "loaded_addresses.readonly",
        meta.loaded_addresses.readonly.len(),
        MAX_TX_ACCOUNTS,
    )?;
    for key in &meta.loaded_addresses.readonly {
        dst.loaded_readonly_addresses.push(address_from_pubkey(key));
    }

    if let Some(groups) = &meta.inner_instructions {
        populate_inner_instructions(&mut dst.inner_instructions, groups)?;
    }
    if let Some(logs) = &meta.log_messages {
        populate_log_messages(&mut dst.log_messages, logs)?;
    }
    if let Some(balances) = &meta.pre_token_balances {
        populate_token_balances(&mut dst.pre_token_balances, balances, "pre_token_balances")?;
    }
    if let Some(balances) = &meta.post_token_balances {
        populate_token_balances(
            &mut dst.post_token_balances,
            balances,
            "post_token_balances",
        )?;
    }
    if let Some(rewards) = &meta.rewards {
        populate_rewards(&mut dst.rewards, rewards)?;
    }

    if let Some(return_data) = &meta.return_data {
        check_capacity("return_data", return_data.data.len(), MAX_RETURN_DATA_LEN)?;
        let dst_rd = dst
            .return_data
            .get_or_insert_with(crate::transactions::ReturnData::default);
        dst_rd.program_id = address_from_pubkey(&return_data.program_id);
        dst_rd.data.set(&return_data.data);
    }

    dst.compute_units_consumed = meta.compute_units_consumed;
    dst.cost_units = meta.cost_units;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_account_decoder_client_types::token::UiTokenAmount;
    use solana_hash::Hash;
    use solana_message::compiled_instruction::CompiledInstruction as UpstreamIx;
    use solana_message::{MessageHeader, v0};
    use solana_signature::Signature;
    use solana_transaction_context::TransactionReturnData;
    use solana_transaction_status::{
        InnerInstruction as UpstreamInnerIx, InnerInstructions as UpstreamInnerIxGroup,
        TransactionTokenBalance as TbUpstream,
    };

    fn pk(byte: u8) -> solana_pubkey::Pubkey {
        solana_pubkey::Pubkey::new_from_array([byte; 32])
    }

    /// Every `TransactionError` wire code round-trips bit-exactly,
    /// including the full `InstructionError` sweep nested under code 8.
    #[test]
    fn error_status_codec_roundtrips_exhaustively() {
        // Sweep every TransactionError variant code.
        for tx_code in 0u16..=38 {
            let (ie_codes, index): (std::ops::RangeInclusive<u16>, u8) = if tx_code == 8 {
                (1..=54, 3) // nested instruction-error sweep
            } else {
                (0..=0, 7)
            };
            for ie_code in ie_codes {
                let status = TransactionStatus {
                    error_code: (tx_code + 1) | (ie_code << 8),
                    instruction_index: index,
                    custom_error_code: if ie_code == 26 { 0xDEAD_BEEF } else { 0 },
                    error_message: ZeroVec::new(),
                };
                let result = result_from_status(&status)
                    .unwrap_or_else(|e| panic!("decode failed for tx_code {tx_code}: {e}"));
                let reencoded = status_from_result(&result);
                // Payload-less variants encode index 0; mask that out for
                // variants that don't carry an index.
                let expect_index = matches!(tx_code, 8 | 30 | 31 | 35);
                assert_eq!(reencoded.error_code, status.error_code, "tx_code {tx_code}");
                if expect_index {
                    assert_eq!(reencoded.instruction_index, status.instruction_index);
                }
                assert_eq!(reencoded.custom_error_code, status.custom_error_code);
            }
        }
        // Ok roundtrip.
        assert_eq!(
            result_from_status(&TransactionStatus::OK).unwrap(),
            Ok(()) as Result<(), TransactionError>
        );
        // Custom error code payload.
        let custom = Err(TransactionError::InstructionError(
            9,
            InstructionError::Custom(42),
        ));
        let status = status_from_result(&custom);
        assert_eq!(status.instruction_index, 9);
        assert_eq!(status.custom_error_code, 42);
        assert_eq!(result_from_status(&status).unwrap(), custom);
    }

    #[test]
    fn error_status_codec_rejects_garbage() {
        let bad = TransactionStatus {
            error_code: 200, // tx_code 199: out of table
            ..TransactionStatus::OK
        };
        assert!(matches!(
            result_from_status(&bad),
            Err(ConvertError::UnknownErrorCode { .. })
        ));
        let stray_ie = TransactionStatus {
            error_code: 1 | (3 << 8), // AccountInUse with a stray instruction code
            ..TransactionStatus::OK
        };
        assert!(result_from_status(&stray_ie).is_err());
    }

    fn sample_meta() -> TransactionStatusMeta {
        TransactionStatusMeta {
            status: Err(TransactionError::InstructionError(
                1,
                InstructionError::Custom(7),
            )),
            fee: 5_000,
            pre_balances: vec![10, 20, 30],
            post_balances: vec![5, 25, 30],
            inner_instructions: Some(vec![UpstreamInnerIxGroup {
                index: 1,
                instructions: vec![UpstreamInnerIx {
                    instruction: UpstreamIx {
                        program_id_index: 2,
                        accounts: vec![0, 1],
                        data: vec![9, 9, 9],
                    },
                    stack_height: Some(2),
                }],
            }]),
            log_messages: Some(vec!["Program log: hi".to_string()]),
            pre_token_balances: Some(vec![TbUpstream {
                account_index: 1,
                mint: pk(0xAA).to_string(),
                ui_token_amount: UiTokenAmount {
                    ui_amount: Some(1.5),
                    decimals: 6,
                    amount: "1500000".to_string(),
                    ui_amount_string: "1.5".to_string(),
                },
                owner: pk(0xBB).to_string(),
                program_id: String::new(),
            }]),
            post_token_balances: Some(vec![]),
            rewards: Some(vec![solana_transaction_status::Reward {
                pubkey: pk(0xCC).to_string(),
                lamports: -42,
                post_balance: 1_000,
                reward_type: Some(solana_reward_info::RewardType::Rent),
                commission: None,
            }]),
            loaded_addresses: Default::default(),
            return_data: Some(TransactionReturnData {
                program_id: pk(0xDD),
                data: vec![1, 2, 3, 4],
            }),
            compute_units_consumed: Some(12_345),
            cost_units: Some(999),
        }
    }

    /// Runs `f` on a thread with a large stack. Debug builds reserve the
    /// full inline capacity of `Option<ZeroVec<…>>` temporaries in the
    /// populate helpers' frames (release elides them); same pattern as the
    /// `transactions` test module.
    fn run_big_stack<F: FnOnce() + Send + 'static>(f: F) {
        std::thread::Builder::new()
            .stack_size(128 * 1024 * 1024)
            .spawn(f)
            .expect("spawn test thread")
            .join()
            .expect("join test thread");
    }

    #[test]
    fn populate_transaction_roundtrips_v0() {
        run_big_stack(populate_transaction_roundtrips_v0_body);
    }

    fn populate_transaction_roundtrips_v0_body() {
        let message = v0::Message {
            header: MessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 1,
            },
            account_keys: vec![pk(1), pk(2), pk(3)],
            recent_blockhash: Hash::new_unique(),
            instructions: vec![UpstreamIx {
                program_id_index: 2,
                accounts: vec![0, 1],
                data: vec![7; 16],
            }],
            address_table_lookups: vec![solana_message::v0::MessageAddressTableLookup {
                account_key: pk(8),
                writable_indexes: vec![0, 3],
                readonly_indexes: vec![1],
            }],
        };
        let tx = VersionedTransaction {
            signatures: vec![Signature::from([5u8; 64])],
            message: solana_message::VersionedMessage::V0(message),
        };
        let mut meta = sample_meta();
        meta.loaded_addresses = solana_message::v0::LoadedAddresses {
            writable: vec![pk(0xE0), pk(0xE1)],
            readonly: vec![pk(0xE2)],
        };

        let mut dst = Transaction::new_boxed();
        populate_transaction(&mut dst, &tx, &meta).expect("populate");

        assert_eq!(dst.signatures.as_slice(), &[Signature::from([5u8; 64])]);
        assert_eq!(dst.fee, 5_000);
        assert_eq!(dst.pre_balances.as_slice(), &[10, 20, 30]);
        assert_eq!(dst.post_balances.as_slice(), &[5, 25, 30]);
        assert_eq!(dst.loaded_writable_addresses.len(), 2);
        assert_eq!(dst.loaded_readonly_addresses.len(), 1);
        assert_eq!(
            dst.loaded_writable_addresses.as_slice()[0],
            Address::new_from_array([0xE0; 32])
        );
        assert_eq!(
            result_from_status(&dst.status).unwrap(),
            Err(TransactionError::InstructionError(
                1,
                InstructionError::Custom(7)
            ))
        );

        let inner = dst.inner_instructions.as_ref().expect("inner ix");
        assert_eq!(inner.len(), 1);
        assert_eq!(inner.as_slice()[0].outer_index, 1);
        assert_eq!(inner.as_slice()[0].stack_height, Some(2));
        assert_eq!(inner.as_slice()[0].instruction.data.as_slice(), &[9, 9, 9]);

        let logs = dst.log_messages.as_ref().expect("logs");
        assert_eq!(logs.iter().next().unwrap(), b"Program log: hi");

        let pre_tb = dst.pre_token_balances.as_ref().expect("token balances");
        assert_eq!(pre_tb.as_slice()[0].ui_token_amount.amount, 1_500_000);
        assert_eq!(
            pre_tb.as_slice()[0].owner,
            Some(Address::new_from_array([0xBB; 32]))
        );
        assert_eq!(pre_tb.as_slice()[0].program_id, None);
        assert_eq!(dst.post_token_balances.as_ref().unwrap().len(), 0);

        let rewards = dst.rewards.as_ref().expect("rewards");
        assert_eq!(rewards.as_slice()[0].lamports, -42);
        assert_eq!(rewards.as_slice()[0].reward_type, Some(RewardType::Rent));

        let rd = dst.return_data.as_ref().expect("return data");
        assert_eq!(rd.data.as_slice(), &[1, 2, 3, 4]);
        assert_eq!(dst.compute_units_consumed, Some(12_345));
        assert_eq!(dst.cost_units, Some(999));

        match &dst.message {
            VersionedMessage::V0(m) => {
                assert_eq!(m.account_keys.len(), 3);
                assert_eq!(m.instructions.len(), 1);
                assert_eq!(m.address_table_lookups.len(), 1);
                assert_eq!(
                    m.address_table_lookups.as_slice()[0]
                        .writable_indexes
                        .as_slice(),
                    &[0, 3]
                );
            }
            _ => panic!("expected v0 message"),
        }

        // Scratch reuse: convert a legacy transaction into the same buffer.
        let legacy_tx = VersionedTransaction {
            signatures: vec![Signature::from([6u8; 64])],
            message: solana_message::VersionedMessage::Legacy(solana_message::legacy::Message {
                header: MessageHeader::default(),
                account_keys: vec![pk(9)],
                recent_blockhash: Hash::new_unique(),
                instructions: vec![],
            }),
        };
        let empty_meta = TransactionStatusMeta::default();
        populate_transaction(&mut dst, &legacy_tx, &empty_meta).expect("repopulate");
        assert!(matches!(dst.message, VersionedMessage::Legacy(_)));
        assert!(dst.status.is_ok());
        assert_eq!(dst.loaded_writable_addresses.len(), 0);
        assert!(dst.inner_instructions.is_none());
    }

    #[test]
    fn populate_transaction_rejects_overflow() {
        run_big_stack(populate_transaction_rejects_overflow_body);
    }

    fn populate_transaction_rejects_overflow_body() {
        let tx = VersionedTransaction {
            signatures: vec![Signature::from([1u8; 64])],
            message: solana_message::VersionedMessage::Legacy(solana_message::legacy::Message {
                header: MessageHeader::default(),
                account_keys: vec![pk(1)],
                recent_blockhash: Hash::new_unique(),
                instructions: vec![],
            }),
        };
        let meta = TransactionStatusMeta {
            log_messages: Some(vec![
                "x".repeat(crate::limits::MAX_TX_LOG_DATA / 2 + 1),
                "y".repeat(crate::limits::MAX_TX_LOG_DATA / 2 + 1),
            ]),
            ..TransactionStatusMeta::default()
        };
        let mut dst = Transaction::new_boxed();
        assert!(matches!(
            populate_transaction(&mut dst, &tx, &meta),
            Err(ConvertError::CapacityExceeded {
                field: "log_messages.data",
                ..
            })
        ));
    }
}
