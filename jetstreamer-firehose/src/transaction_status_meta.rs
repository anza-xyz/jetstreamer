//! Versioned decoding for transaction metadata stored in Old Faithful.
//!
//! The pre-protobuf archive spans three Solana bincode schemas. V1.5.9 briefly
//! made `UiTokenAmount::ui_amount` a string in backport
//! `1f1dd58c78c3f4d543fb96526a0f6ee6d8a7f969`; v1.5.10 reverted it. Backport
//! `16ded2115c02b5fa514f35b03cdcb98845506fc8` then changed the nested value
//! from three fields to four for v1.5.13. Records without token balances are
//! byte-compatible across these changes, while populated token balances are
//! not. Keep all three historical wire schemas explicit here and compare any
//! overlapping successful decodes before converting them into current public
//! Solana types.
//!
//! Solana commit `7e6528972948c3f35b3ce21ae202ffd3155c9ba6`
//! switched Blockstore transaction-status writes to protobuf on 2021-03-05.
//! Its immediate parent, `bd13262b420779fba2e6103600bb806fcb3e96e4`,
//! defines the last bincode schema: the eight fields and enum ranges copied
//! below. Old Faithful contains bincode and protobuf records during the
//! producer transition. The pre-cutoff route tries all three exact bincode schemas
//! and guarded protobuf, accepting only one normalized result. Later fields
//! and variants must not make malformed legacy records valid.

use {
    crate::{SharedError, epochs::slot_to_epoch},
    bincode::Options as _,
    serde::Deserialize,
    solana_account_decoder_client_types::token::{UiTokenAmount, real_number_string_trimmed},
    solana_instruction_error::InstructionError,
    solana_message::{compiled_instruction::CompiledInstruction, v0::LoadedAddresses},
    solana_transaction_error::TransactionError,
    solana_transaction_status::{
        InnerInstruction, InnerInstructions, TransactionStatusMeta, TransactionTokenBalance,
    },
    std::io,
};

/// Wire-compatible copy of the transaction error stored in legacy metadata.
///
/// Solana kept these discriminants stable, but the nested `BorshIoError`
/// payload changed from `String` to a unit variant. This follows Solana's own
/// snapshot compatibility adapter rather than decoding historical bytes into
/// today's enum directly. The grammar stops at the last variant available at
/// the archive cutoff, so later discriminants remain invalid legacy input.
#[derive(Deserialize)]
#[cfg_attr(test, derive(serde::Serialize))]
enum LegacyTransactionError {
    AccountInUse,
    AccountLoadedTwice,
    AccountNotFound,
    ProgramAccountNotFound,
    InsufficientFundsForFee,
    InvalidAccountForFee,
    DuplicateSignature,
    BlockhashNotFound,
    InstructionError(u8, LegacyInstructionError),
    CallChainTooDeep,
    MissingSignatureForFee,
    InvalidAccountIndex,
    SignatureFailure,
    InvalidProgramForExecution,
    SanitizeFailure,
    ClusterMaintenance,
}

impl From<LegacyTransactionError> for TransactionError {
    fn from(error: LegacyTransactionError) -> Self {
        match error {
            LegacyTransactionError::AccountInUse => Self::AccountInUse,
            LegacyTransactionError::AccountLoadedTwice => Self::AccountLoadedTwice,
            LegacyTransactionError::AccountNotFound => Self::AccountNotFound,
            LegacyTransactionError::ProgramAccountNotFound => Self::ProgramAccountNotFound,
            LegacyTransactionError::InsufficientFundsForFee => Self::InsufficientFundsForFee,
            LegacyTransactionError::InvalidAccountForFee => Self::InvalidAccountForFee,
            LegacyTransactionError::DuplicateSignature => Self::AlreadyProcessed,
            LegacyTransactionError::BlockhashNotFound => Self::BlockhashNotFound,
            LegacyTransactionError::InstructionError(index, error) => {
                Self::InstructionError(index, error.into())
            }
            LegacyTransactionError::CallChainTooDeep => Self::CallChainTooDeep,
            LegacyTransactionError::MissingSignatureForFee => Self::MissingSignatureForFee,
            LegacyTransactionError::InvalidAccountIndex => Self::InvalidAccountIndex,
            LegacyTransactionError::SignatureFailure => Self::SignatureFailure,
            LegacyTransactionError::InvalidProgramForExecution => Self::InvalidProgramForExecution,
            LegacyTransactionError::SanitizeFailure => Self::SanitizeFailure,
            LegacyTransactionError::ClusterMaintenance => Self::ClusterMaintenance,
        }
    }
}

/// Wire-compatible copy of the instruction error stored in legacy metadata.
/// It likewise excludes variants introduced after the archive cutoff.
#[derive(Deserialize)]
#[cfg_attr(test, derive(serde::Serialize))]
enum LegacyInstructionError {
    GenericError,
    InvalidArgument,
    InvalidInstructionData,
    InvalidAccountData,
    AccountDataTooSmall,
    InsufficientFunds,
    IncorrectProgramId,
    MissingRequiredSignature,
    AccountAlreadyInitialized,
    UninitializedAccount,
    UnbalancedInstruction,
    ModifiedProgramId,
    ExternalAccountLamportSpend,
    ExternalAccountDataModified,
    ReadonlyLamportChange,
    ReadonlyDataModified,
    DuplicateAccountIndex,
    ExecutableModified,
    RentEpochModified,
    NotEnoughAccountKeys,
    AccountDataSizeChanged,
    AccountNotExecutable,
    AccountBorrowFailed,
    AccountBorrowOutstanding,
    DuplicateAccountOutOfSync,
    Custom(u32),
    InvalidError,
    ExecutableDataModified,
    ExecutableLamportChange,
    ExecutableAccountNotRentExempt,
    UnsupportedProgramId,
    CallDepth,
    MissingAccount,
    ReentrancyNotAllowed,
    MaxSeedLengthExceeded,
    InvalidSeeds,
    InvalidRealloc,
    ComputationalBudgetExceeded,
    PrivilegeEscalation,
    ProgramEnvironmentSetupFailure,
    ProgramFailedToComplete,
    ProgramFailedToCompile,
    Immutable,
    IncorrectAuthority,
    BorshIoError(String),
    AccountNotRentExempt,
    InvalidAccountOwner,
}

#[allow(deprecated)]
impl From<LegacyInstructionError> for InstructionError {
    fn from(error: LegacyInstructionError) -> Self {
        match error {
            LegacyInstructionError::GenericError => Self::GenericError,
            LegacyInstructionError::InvalidArgument => Self::InvalidArgument,
            LegacyInstructionError::InvalidInstructionData => Self::InvalidInstructionData,
            LegacyInstructionError::InvalidAccountData => Self::InvalidAccountData,
            LegacyInstructionError::AccountDataTooSmall => Self::AccountDataTooSmall,
            LegacyInstructionError::InsufficientFunds => Self::InsufficientFunds,
            LegacyInstructionError::IncorrectProgramId => Self::IncorrectProgramId,
            LegacyInstructionError::MissingRequiredSignature => Self::MissingRequiredSignature,
            LegacyInstructionError::AccountAlreadyInitialized => Self::AccountAlreadyInitialized,
            LegacyInstructionError::UninitializedAccount => Self::UninitializedAccount,
            LegacyInstructionError::UnbalancedInstruction => Self::UnbalancedInstruction,
            LegacyInstructionError::ModifiedProgramId => Self::ModifiedProgramId,
            LegacyInstructionError::ExternalAccountLamportSpend => {
                Self::ExternalAccountLamportSpend
            }
            LegacyInstructionError::ExternalAccountDataModified => {
                Self::ExternalAccountDataModified
            }
            LegacyInstructionError::ReadonlyLamportChange => Self::ReadonlyLamportChange,
            LegacyInstructionError::ReadonlyDataModified => Self::ReadonlyDataModified,
            LegacyInstructionError::DuplicateAccountIndex => Self::DuplicateAccountIndex,
            LegacyInstructionError::ExecutableModified => Self::ExecutableModified,
            LegacyInstructionError::RentEpochModified => Self::RentEpochModified,
            LegacyInstructionError::NotEnoughAccountKeys => Self::NotEnoughAccountKeys,
            LegacyInstructionError::AccountDataSizeChanged => Self::AccountDataSizeChanged,
            LegacyInstructionError::AccountNotExecutable => Self::AccountNotExecutable,
            LegacyInstructionError::AccountBorrowFailed => Self::AccountBorrowFailed,
            LegacyInstructionError::AccountBorrowOutstanding => Self::AccountBorrowOutstanding,
            LegacyInstructionError::DuplicateAccountOutOfSync => Self::DuplicateAccountOutOfSync,
            LegacyInstructionError::Custom(code) => Self::Custom(code),
            LegacyInstructionError::InvalidError => Self::InvalidError,
            LegacyInstructionError::ExecutableDataModified => Self::ExecutableDataModified,
            LegacyInstructionError::ExecutableLamportChange => Self::ExecutableLamportChange,
            LegacyInstructionError::ExecutableAccountNotRentExempt => {
                Self::ExecutableAccountNotRentExempt
            }
            LegacyInstructionError::UnsupportedProgramId => Self::UnsupportedProgramId,
            LegacyInstructionError::CallDepth => Self::CallDepth,
            LegacyInstructionError::MissingAccount => Self::MissingAccount,
            LegacyInstructionError::ReentrancyNotAllowed => Self::ReentrancyNotAllowed,
            LegacyInstructionError::MaxSeedLengthExceeded => Self::MaxSeedLengthExceeded,
            LegacyInstructionError::InvalidSeeds => Self::InvalidSeeds,
            LegacyInstructionError::InvalidRealloc => Self::InvalidRealloc,
            LegacyInstructionError::ComputationalBudgetExceeded => {
                Self::ComputationalBudgetExceeded
            }
            LegacyInstructionError::PrivilegeEscalation => Self::PrivilegeEscalation,
            LegacyInstructionError::ProgramEnvironmentSetupFailure => {
                Self::ProgramEnvironmentSetupFailure
            }
            LegacyInstructionError::ProgramFailedToComplete => Self::ProgramFailedToComplete,
            LegacyInstructionError::ProgramFailedToCompile => Self::ProgramFailedToCompile,
            LegacyInstructionError::Immutable => Self::Immutable,
            LegacyInstructionError::IncorrectAuthority => Self::IncorrectAuthority,
            LegacyInstructionError::BorshIoError(_message) => Self::BorshIoError,
            LegacyInstructionError::AccountNotRentExempt => Self::AccountNotRentExempt,
            LegacyInstructionError::InvalidAccountOwner => Self::InvalidAccountOwner,
        }
    }
}

#[derive(Deserialize)]
struct LegacyInnerInstructions {
    index: u8,
    instructions: Vec<CompiledInstruction>,
}

impl From<LegacyInnerInstructions> for InnerInstructions {
    fn from(value: LegacyInnerInstructions) -> Self {
        Self {
            index: value.index,
            instructions: value
                .instructions
                .into_iter()
                .map(|instruction| InnerInstruction {
                    instruction,
                    stack_height: None,
                })
                .collect(),
        }
    }
}

/// First slot after which Old Faithful transaction-status metadata is
/// consistently stored as protobuf. Earlier archive data is predominantly
/// legacy bincode, but contains protobuf records near the transition. This is
/// an archive-input boundary, independent of the runtime that executed a slot.
pub const OLD_FAITHFUL_PROTOBUF_META_START_SLOT: u64 = 157 * 432_000;

/// Transaction-metadata encoding selected from the source slot.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OldFaithfulMetaEncoding {
    /// Try the Solana f64, v1.5.9 string, and v1.5.13 bincode schemas plus
    /// guarded protobuf, rejecting successful decodes that disagree.
    BincodeWithProtobufFallback,
    /// Decode protobuf directly.
    Protobuf,
}

/// Selects the Old Faithful transaction-metadata decoder solely from `slot`.
#[inline]
pub const fn old_faithful_meta_encoding(slot: u64) -> OldFaithfulMetaEncoding {
    if slot < OLD_FAITHFUL_PROTOBUF_META_START_SLOT {
        OldFaithfulMetaEncoding::BincodeWithProtobufFallback
    } else {
        OldFaithfulMetaEncoding::Protobuf
    }
}

#[derive(Deserialize)]
struct LegacyV1_5_12UiTokenAmount {
    ui_amount: f64,
    decimals: u8,
    amount: String,
}

#[derive(Deserialize)]
struct LegacyV1_5_9UiTokenAmount {
    ui_amount: String,
    decimals: u8,
    amount: String,
}

#[derive(Deserialize)]
struct LegacyV1_5_13UiTokenAmount {
    ui_amount: Option<f64>,
    decimals: u8,
    amount: String,
    ui_amount_string: String,
}

trait NormalizeLegacyUiTokenAmount {
    fn normalize(self) -> Result<UiTokenAmount, String>;
}

fn parse_canonical_token_amount(amount: &str) -> Result<u64, String> {
    let parsed = amount
        .parse::<u64>()
        .map_err(|_| "token amount is not a u64".to_owned())?;
    if parsed.to_string() != amount {
        return Err("token amount is not in canonical decimal form".to_owned());
    }
    Ok(parsed)
}

fn normalized_ui_amount(amount: u64, decimals: u8) -> Option<f64> {
    10_usize
        .checked_pow(u32::from(decimals))
        .map(|divisor| amount as f64 / divisor as f64)
}

fn legacy_v1_5_12_ui_amount(amount: u64, decimals: u8) -> f64 {
    // These releases computed the divisor with unchecked `usize::pow` in an
    // optimized x86_64 build. Preserve its 64-bit wrapping behavior when
    // validating the redundant historical field, even on another host.
    amount as f64 / 10_u64.wrapping_pow(u32::from(decimals)) as f64
}

fn normalized_ui_token_amount(amount: u64, decimals: u8, amount_string: String) -> UiTokenAmount {
    UiTokenAmount {
        ui_amount: normalized_ui_amount(amount, decimals),
        decimals,
        ui_amount_string: real_number_string_trimmed(amount, decimals),
        amount: amount_string,
    }
}

fn legacy_real_number_string_trimmed(amount: u64, decimals: u8) -> String {
    let decimals = usize::from(decimals);
    let mut value = if decimals == 0 {
        amount.to_string()
    } else {
        let width = decimals + 1;
        let mut value = format!("{amount:0width$}");
        value.insert(value.len() - decimals, '.');
        value
    };
    let trimmed_len = value.trim_end_matches('0').trim_end_matches('.').len();
    value.truncate(trimmed_len);
    value
}

fn optional_f64_bits_equal(left: Option<f64>, right: Option<f64>) -> bool {
    match (left, right) {
        (Some(left), Some(right)) => left.to_bits() == right.to_bits(),
        (None, None) => true,
        _ => false,
    }
}

impl NormalizeLegacyUiTokenAmount for LegacyV1_5_12UiTokenAmount {
    fn normalize(self) -> Result<UiTokenAmount, String> {
        let amount = parse_canonical_token_amount(&self.amount)?;
        let expected_ui_amount = legacy_v1_5_12_ui_amount(amount, self.decimals);
        if self.ui_amount.to_bits() != expected_ui_amount.to_bits() {
            return Err("f64 token amount disagrees with amount and decimals".to_owned());
        }

        Ok(normalized_ui_token_amount(
            amount,
            self.decimals,
            self.amount,
        ))
    }
}

impl NormalizeLegacyUiTokenAmount for LegacyV1_5_9UiTokenAmount {
    fn normalize(self) -> Result<UiTokenAmount, String> {
        let amount = parse_canonical_token_amount(&self.amount)?;
        if self.ui_amount != legacy_real_number_string_trimmed(amount, self.decimals) {
            return Err("string token amount disagrees with amount and decimals".to_owned());
        }

        Ok(normalized_ui_token_amount(
            amount,
            self.decimals,
            self.amount,
        ))
    }
}

impl NormalizeLegacyUiTokenAmount for LegacyV1_5_13UiTokenAmount {
    fn normalize(self) -> Result<UiTokenAmount, String> {
        let amount = parse_canonical_token_amount(&self.amount)?;
        let expected_ui_amount = normalized_ui_amount(amount, self.decimals);
        if !optional_f64_bits_equal(self.ui_amount, expected_ui_amount) {
            return Err("optional f64 token amount disagrees with amount and decimals".to_owned());
        }
        if self.ui_amount_string != legacy_real_number_string_trimmed(amount, self.decimals) {
            return Err("token amount string disagrees with amount and decimals".to_owned());
        }

        Ok(normalized_ui_token_amount(
            amount,
            self.decimals,
            self.amount,
        ))
    }
}

#[derive(Deserialize)]
struct LegacyTransactionTokenBalance<A> {
    account_index: u8,
    mint: String,
    ui_token_amount: A,
}

impl<A: NormalizeLegacyUiTokenAmount> LegacyTransactionTokenBalance<A> {
    fn normalize(self) -> Result<TransactionTokenBalance, String> {
        Ok(TransactionTokenBalance {
            account_index: self.account_index,
            mint: self.mint,
            ui_token_amount: self.ui_token_amount.normalize()?,
            owner: String::new(),
            program_id: String::new(),
        })
    }
}

#[derive(Deserialize)]
#[serde(bound(deserialize = "A: Deserialize<'de>"))]
struct LegacyTransactionStatusMeta<A> {
    status: Result<(), LegacyTransactionError>,
    fee: u64,
    pre_balances: Vec<u64>,
    post_balances: Vec<u64>,
    #[serde(deserialize_with = "solana_serde::default_on_eof")]
    inner_instructions: Option<Vec<LegacyInnerInstructions>>,
    #[serde(deserialize_with = "solana_serde::default_on_eof")]
    log_messages: Option<Vec<String>>,
    #[serde(deserialize_with = "solana_serde::default_on_eof")]
    pre_token_balances: Option<Vec<LegacyTransactionTokenBalance<A>>>,
    #[serde(deserialize_with = "solana_serde::default_on_eof")]
    post_token_balances: Option<Vec<LegacyTransactionTokenBalance<A>>>,
}

impl<A: NormalizeLegacyUiTokenAmount> LegacyTransactionStatusMeta<A> {
    fn normalize(self) -> Result<TransactionStatusMeta, String> {
        Ok(TransactionStatusMeta {
            status: self.status.map_err(Into::into),
            fee: self.fee,
            pre_balances: self.pre_balances,
            post_balances: self.post_balances,
            inner_instructions: self
                .inner_instructions
                .map(|groups| groups.into_iter().map(Into::into).collect()),
            log_messages: self.log_messages,
            pre_token_balances: self
                .pre_token_balances
                .map(|balances| {
                    balances
                        .into_iter()
                        .map(LegacyTransactionTokenBalance::normalize)
                        .collect()
                })
                .transpose()?,
            post_token_balances: self
                .post_token_balances
                .map(|balances| {
                    balances
                        .into_iter()
                        .map(LegacyTransactionTokenBalance::normalize)
                        .collect()
                })
                .transpose()?,
            rewards: None,
            loaded_addresses: LoadedAddresses::default(),
            return_data: None,
            compute_units_consumed: None,
            cost_units: None,
        })
    }
}

fn decode_legacy_bincode<A>(metadata_bytes: &[u8]) -> Result<TransactionStatusMeta, String>
where
    A: for<'de> Deserialize<'de> + NormalizeLegacyUiTokenAmount,
{
    // Match the historical fixed-integer encoding, require the complete
    // cutoff-era schema, and bind allocations to the CID-verified input frame.
    bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .reject_trailing_bytes()
        .with_limit(metadata_bytes.len() as u64)
        .deserialize::<LegacyTransactionStatusMeta<A>>(metadata_bytes)
        .map_err(|error| error.to_string())?
        .normalize()
}

fn validate_decoded_metadata(
    slot: u64,
    epoch: u64,
    encoding: &str,
    metadata: TransactionStatusMeta,
) -> Result<TransactionStatusMeta, String> {
    // Every stored transaction has at least its fee payer, and both balance
    // snapshots cover the same account-key list. Besides checking source
    // integrity, this prevents a permissive decoder from accepting a frame
    // that only happens to match part of its grammar.
    if metadata.pre_balances.is_empty()
        || metadata.pre_balances.len() != metadata.post_balances.len()
    {
        return Err(format!(
            "{encoding} transaction metadata has invalid balance vectors (slot {slot}, epoch {epoch}, pre={}, post={})",
            metadata.pre_balances.len(),
            metadata.post_balances.len(),
        ));
    }

    let account_count = metadata.pre_balances.len();
    if metadata
        .pre_token_balances
        .iter()
        .chain(&metadata.post_token_balances)
        .flatten()
        .any(|balance| usize::from(balance.account_index) >= account_count)
    {
        return Err(format!(
            "{encoding} transaction metadata has an out-of-range token balance account index (slot {slot}, epoch {epoch}, accounts={account_count})"
        ));
    }

    Ok(metadata)
}

fn decode_legacy_candidate<A>(
    slot: u64,
    epoch: u64,
    encoding: &str,
    metadata_bytes: &[u8],
) -> Result<TransactionStatusMeta, String>
where
    A: for<'de> Deserialize<'de> + NormalizeLegacyUiTokenAmount,
{
    decode_legacy_bincode::<A>(metadata_bytes)
        .and_then(|metadata| validate_decoded_metadata(slot, epoch, encoding, metadata))
}

fn decode_v1_5_9_candidate(
    slot: u64,
    epoch: u64,
    metadata_bytes: &[u8],
) -> Result<TransactionStatusMeta, String> {
    let metadata = decode_legacy_candidate::<LegacyV1_5_9UiTokenAmount>(
        slot,
        epoch,
        "Solana v1.5.9 bincode",
        metadata_bytes,
    )?;

    // InvalidAccountOwner was added after v1.5.9 and has no valid pairing
    // with that release's short-lived string token-amount schema.
    if matches!(
        metadata.status,
        Err(TransactionError::InstructionError(
            _,
            InstructionError::InvalidAccountOwner
        ))
    ) {
        return Err(format!(
            "Solana v1.5.9 bincode contains post-v1.5.9 InvalidAccountOwner (slot {slot}, epoch {epoch})"
        ));
    }

    Ok(metadata)
}

fn decode_protobuf(
    slot: u64,
    epoch: u64,
    metadata_bytes: &[u8],
) -> Result<TransactionStatusMeta, SharedError> {
    let proto: solana_storage_proto::convert::generated::TransactionStatusMeta =
        prost_011::Message::decode(metadata_bytes).map_err(|error| {
            Box::new(io::Error::other(format!(
                "protobuf decode transaction metadata (slot {slot}, epoch {epoch}): {error}"
            ))) as SharedError
        })?;
    let metadata: TransactionStatusMeta = proto.try_into().map_err(|error| {
        Box::new(io::Error::other(format!(
            "convert transaction metadata proto (slot {slot}, epoch {epoch}): {error}"
        ))) as SharedError
    })?;

    validate_decoded_metadata(slot, epoch, "protobuf", metadata)
        .map_err(|error| Box::new(io::Error::other(error)) as SharedError)
}

fn select_unique_metadata<const N: usize>(
    slot: u64,
    epoch: u64,
    candidates: [(&'static str, Result<TransactionStatusMeta, String>); N],
) -> Result<TransactionStatusMeta, SharedError> {
    let mut selected: Option<(&'static str, TransactionStatusMeta)> = None;
    let mut failures = Vec::with_capacity(N);

    for (encoding, candidate) in candidates {
        match candidate {
            Ok(metadata) => match &selected {
                Some((selected_encoding, selected_metadata)) if selected_metadata != &metadata => {
                    return Err(Box::new(io::Error::other(format!(
                        "ambiguous transaction metadata (slot {slot}, epoch {epoch}): {selected_encoding} and {encoding} decoded to different values"
                    ))));
                }
                Some(_) => {}
                None => selected = Some((encoding, metadata)),
            },
            Err(error) => failures.push(format!("{encoding}: {error}")),
        }
    }

    selected.map(|(_, metadata)| metadata).ok_or_else(|| {
        Box::new(io::Error::other(format!(
            "decode transaction metadata (slot {slot}, epoch {epoch}): {}",
            failures.join("; ")
        ))) as SharedError
    })
}

pub(crate) fn decode_transaction_status_meta(
    slot: u64,
    metadata_bytes: &[u8],
) -> Result<TransactionStatusMeta, SharedError> {
    let epoch = slot_to_epoch(slot);
    match old_faithful_meta_encoding(slot) {
        OldFaithfulMetaEncoding::BincodeWithProtobufFallback => select_unique_metadata(
            slot,
            epoch,
            [
                (
                    "Solana v1.5.12 bincode",
                    decode_legacy_candidate::<LegacyV1_5_12UiTokenAmount>(
                        slot,
                        epoch,
                        "Solana v1.5.12 bincode",
                        metadata_bytes,
                    ),
                ),
                (
                    "Solana v1.5.9 bincode",
                    decode_v1_5_9_candidate(slot, epoch, metadata_bytes),
                ),
                (
                    "Solana v1.5.13 bincode",
                    decode_legacy_candidate::<LegacyV1_5_13UiTokenAmount>(
                        slot,
                        epoch,
                        "Solana v1.5.13 bincode",
                        metadata_bytes,
                    ),
                ),
                (
                    "protobuf",
                    decode_protobuf(slot, epoch, metadata_bytes).map_err(|error| error.to_string()),
                ),
            ],
        ),
        OldFaithfulMetaEncoding::Protobuf => decode_protobuf(slot, epoch, metadata_bytes),
    }
}

#[cfg(test)]
mod tests {
    use {
        super::{
            LegacyInnerInstructions, LegacyInstructionError, LegacyTransactionError,
            LegacyTransactionStatusMeta, LegacyTransactionTokenBalance, LegacyV1_5_9UiTokenAmount,
            LegacyV1_5_12UiTokenAmount, LegacyV1_5_13UiTokenAmount,
            OLD_FAITHFUL_PROTOBUF_META_START_SLOT, OldFaithfulMetaEncoding, decode_legacy_bincode,
            decode_transaction_status_meta, decode_v1_5_9_candidate, legacy_v1_5_12_ui_amount,
            old_faithful_meta_encoding, select_unique_metadata,
        },
        serde::Serialize,
        sha2::{Digest as _, Sha256},
        solana_instruction_error::InstructionError,
        solana_message::compiled_instruction::CompiledInstruction,
        solana_transaction_error::TransactionError,
        solana_transaction_status::TransactionStatusMeta,
        std::fmt::Write as _,
    };

    #[derive(Serialize)]
    struct LegacyV1_5_12WireUiTokenAmount {
        ui_amount: f64,
        decimals: u8,
        amount: String,
    }

    #[derive(Serialize)]
    struct LegacyWireTokenBalance<'a> {
        account_index: u8,
        mint: &'a str,
        ui_token_amount: LegacyV1_5_12WireUiTokenAmount,
    }

    #[derive(Serialize)]
    struct LegacyWireInnerInstructions {
        index: u8,
        instructions: Vec<CompiledInstruction>,
    }

    #[derive(Serialize)]
    struct LegacyWireMeta<'a> {
        status: Result<(), LegacyTransactionError>,
        fee: u64,
        pre_balances: Vec<u64>,
        post_balances: Vec<u64>,
        inner_instructions: Option<Vec<LegacyWireInnerInstructions>>,
        log_messages: Option<Vec<String>>,
        pre_token_balances: Option<Vec<LegacyWireTokenBalance<'a>>>,
        post_token_balances: Option<Vec<LegacyWireTokenBalance<'a>>>,
    }

    #[derive(Serialize)]
    struct LegacyV1_5_9WireUiTokenAmount<'a> {
        ui_amount: &'a str,
        decimals: u8,
        amount: &'a str,
    }

    #[derive(Serialize)]
    struct LegacyV1_5_9WireTokenBalance<'a> {
        account_index: u8,
        mint: &'a str,
        ui_token_amount: LegacyV1_5_9WireUiTokenAmount<'a>,
    }

    #[derive(Serialize)]
    struct LegacyV1_5_9WireMeta<'a> {
        status: Result<(), LegacyTransactionError>,
        fee: u64,
        pre_balances: Vec<u64>,
        post_balances: Vec<u64>,
        inner_instructions: Option<Vec<LegacyWireInnerInstructions>>,
        log_messages: Option<Vec<String>>,
        pre_token_balances: Option<Vec<LegacyV1_5_9WireTokenBalance<'a>>>,
        post_token_balances: Option<Vec<LegacyV1_5_9WireTokenBalance<'a>>>,
    }

    fn token_balance(account_index: u8, mint: &'static str) -> LegacyWireTokenBalance<'static> {
        LegacyWireTokenBalance {
            account_index,
            mint,
            ui_token_amount: LegacyV1_5_12WireUiTokenAmount {
                ui_amount: 1.25,
                decimals: 2,
                amount: "125".to_owned(),
            },
        }
    }

    fn decode_hex_fixture(text: &str) -> Vec<u8> {
        let compact: String = text
            .chars()
            .filter(|character| !character.is_whitespace())
            .collect();
        assert_eq!(compact.len() % 2, 0, "hex fixture has an odd length");
        (0..compact.len())
            .step_by(2)
            .map(|offset| {
                u8::from_str_radix(&compact[offset..offset + 2], 16)
                    .expect("fixture contains only hexadecimal bytes")
            })
            .collect()
    }

    fn sha256_hex(bytes: &[u8]) -> String {
        let mut output = String::with_capacity(64);
        for byte in Sha256::digest(bytes) {
            write!(&mut output, "{byte:02x}").expect("writing to a string cannot fail");
        }
        output
    }

    #[test]
    fn metadata_encoding_switches_at_the_configured_slot() {
        assert_eq!(
            old_faithful_meta_encoding(OLD_FAITHFUL_PROTOBUF_META_START_SLOT - 1),
            OldFaithfulMetaEncoding::BincodeWithProtobufFallback
        );
        assert_eq!(
            old_faithful_meta_encoding(OLD_FAITHFUL_PROTOBUF_META_START_SLOT),
            OldFaithfulMetaEncoding::Protobuf
        );
    }

    #[test]
    fn protobuf_transition_record_uses_the_bounded_fallback() {
        let expected = TransactionStatusMeta {
            fee: 5_000,
            pre_balances: vec![10_000, 20_000],
            post_balances: vec![5_000, 25_000],
            pre_token_balances: Some(Vec::new()),
            post_token_balances: Some(Vec::new()),
            rewards: Some(Vec::new()),
            ..TransactionStatusMeta::default()
        };
        let proto: solana_storage_proto::convert::generated::TransactionStatusMeta =
            expected.clone().into();
        let bytes = prost_011::Message::encode_to_vec(&proto);

        assert_eq!(
            decode_transaction_status_meta(OLD_FAITHFUL_PROTOBUF_META_START_SLOT - 1, &bytes)
                .unwrap(),
            expected
        );
    }

    #[test]
    fn protobuf_fallback_rejects_an_unknown_only_message() {
        // Unknown protobuf field 100 with a varint value of one. Prost accepts
        // and discards it, but it is not transaction metadata.
        let unknown_only = [0xa0, 0x06, 0x01];
        let error = decode_transaction_status_meta(
            OLD_FAITHFUL_PROTOBUF_META_START_SLOT - 1,
            &unknown_only,
        )
        .unwrap_err();

        assert!(error.to_string().contains("invalid balance vectors"));
    }

    #[test]
    fn legacy_bincode_token_balances_decode_without_newer_fields() {
        let wire = LegacyWireMeta {
            status: Ok(()),
            fee: 5_000,
            pre_balances: vec![10_000, 20_000],
            post_balances: vec![5_000, 25_000],
            inner_instructions: Some(vec![LegacyWireInnerInstructions {
                index: 0,
                instructions: vec![
                    CompiledInstruction::new_from_raw_parts(12, vec![9, 6, 11, 9], vec![3]),
                    CompiledInstruction::new_from_raw_parts(2, vec![10, 7, 11, 9], vec![3]),
                ],
            }]),
            log_messages: Some(vec!["legacy".to_owned()]),
            pre_token_balances: Some(vec![token_balance(0, "mint-a"), token_balance(1, "mint-b")]),
            post_token_balances: Some(vec![token_balance(1, "mint-b")]),
        };
        let bytes = bincode::serialize(&wire).expect("serialize legacy fixture");

        let decoded = decode_transaction_status_meta(38_879_999, &bytes).expect("decode");

        assert_eq!(decoded.fee, 5_000);
        assert_eq!(
            decoded.log_messages.as_deref(),
            Some(["legacy".to_owned()].as_slice())
        );
        let pre = decoded.pre_token_balances.expect("pre token balances");
        assert_eq!(pre.len(), 2);
        assert_eq!(pre[0].mint, "mint-a");
        assert_eq!(pre[0].ui_token_amount.amount, "125");
        assert!(pre.iter().all(|balance| balance.owner.is_empty()));
        assert!(pre.iter().all(|balance| balance.program_id.is_empty()));
        let post = decoded.post_token_balances.expect("post token balances");
        assert_eq!(post.len(), 1);
        assert_eq!(post[0].mint, "mint-b");
        let inner = decoded.inner_instructions.expect("inner instructions");
        assert_eq!(inner.len(), 1);
        assert_eq!(inner[0].instructions.len(), 2);
        assert_eq!(inner[0].instructions[0].instruction.program_id_index, 12);
        assert!(
            inner[0]
                .instructions
                .iter()
                .all(|instruction| instruction.stack_height.is_none())
        );
    }

    #[test]
    fn legacy_f64_token_amount_preserves_release_wrapping_semantics() {
        let decimals = 20;
        let wire = LegacyWireMeta {
            status: Ok(()),
            fee: 1,
            pre_balances: vec![2],
            post_balances: vec![1],
            inner_instructions: None,
            log_messages: None,
            pre_token_balances: Some(vec![LegacyWireTokenBalance {
                account_index: 0,
                mint: "legacy-mint",
                ui_token_amount: LegacyV1_5_12WireUiTokenAmount {
                    ui_amount: legacy_v1_5_12_ui_amount(1, decimals),
                    decimals,
                    amount: "1".to_owned(),
                },
            }]),
            post_token_balances: None,
        };
        let bytes = bincode::serialize(&wire).expect("serialize legacy fixture");

        let decoded = decode_legacy_bincode::<LegacyV1_5_12UiTokenAmount>(&bytes)
            .expect("decode release-generated overflowing divisor");
        let amount = &decoded.pre_token_balances.expect("token balance")[0].ui_token_amount;

        assert_eq!(amount.ui_amount, None);
        assert_eq!(amount.decimals, decimals);
        assert_eq!(amount.amount, "1");
        assert_eq!(amount.ui_amount_string, "0.00000000000000000001");
    }

    #[test]
    fn v1_5_9_string_token_amount_decodes_into_current_solana_type() {
        let token_balance = || LegacyV1_5_9WireTokenBalance {
            account_index: 0,
            mint: "mint-v1.5.9",
            ui_token_amount: LegacyV1_5_9WireUiTokenAmount {
                ui_amount: "1.25",
                decimals: 2,
                amount: "125",
            },
        };
        let wire = LegacyV1_5_9WireMeta {
            status: Ok(()),
            fee: 5_000,
            pre_balances: vec![10_000],
            post_balances: vec![5_000],
            inner_instructions: None,
            log_messages: Some(vec!["v1.5.9".to_owned()]),
            pre_token_balances: Some(vec![token_balance()]),
            post_token_balances: Some(vec![token_balance()]),
        };
        let bytes = bincode::serialize(&wire).expect("serialize v1.5.9 fixture");

        assert!(decode_legacy_bincode::<LegacyV1_5_12UiTokenAmount>(&bytes).is_err());
        assert!(decode_legacy_bincode::<LegacyV1_5_13UiTokenAmount>(&bytes).is_err());
        let decoded = decode_transaction_status_meta(66_000_000, &bytes).expect("decode");

        let balance = &decoded.pre_token_balances.expect("token balances")[0];
        assert_eq!(balance.ui_token_amount.ui_amount, Some(1.25));
        assert_eq!(balance.ui_token_amount.amount, "125");
        assert_eq!(balance.ui_token_amount.ui_amount_string, "1.25");
    }

    #[test]
    fn v1_5_9_schema_rejects_later_instruction_error_variant() {
        let token_balance = || LegacyV1_5_9WireTokenBalance {
            account_index: 0,
            mint: "mint-v1.5.9",
            ui_token_amount: LegacyV1_5_9WireUiTokenAmount {
                ui_amount: "1.25",
                decimals: 2,
                amount: "125",
            },
        };
        let wire = LegacyV1_5_9WireMeta {
            status: Err(LegacyTransactionError::InstructionError(
                0,
                LegacyInstructionError::InvalidAccountOwner,
            )),
            fee: 5_000,
            pre_balances: vec![10_000],
            post_balances: vec![5_000],
            inner_instructions: None,
            log_messages: None,
            pre_token_balances: Some(vec![token_balance()]),
            post_token_balances: Some(vec![token_balance()]),
        };
        let bytes = bincode::serialize(&wire).expect("serialize impossible hybrid");

        assert!(decode_legacy_bincode::<LegacyV1_5_9UiTokenAmount>(&bytes).is_ok());
        let error = decode_v1_5_9_candidate(66_000_000, 152, &bytes)
            .expect_err("the release-specific validator rejects the later variant");
        assert!(error.contains("post-v1.5.9 InvalidAccountOwner"));
        assert!(decode_transaction_status_meta(66_000_000, &bytes).is_err());
    }

    #[test]
    fn legacy_schema_defaults_fields_introduced_after_the_cutoff() {
        let wire = LegacyWireMeta {
            status: Ok(()),
            fee: 1,
            pre_balances: vec![2],
            post_balances: vec![1],
            inner_instructions: None,
            log_messages: None,
            pre_token_balances: None,
            post_token_balances: None,
        };
        let bytes = bincode::serialize(&wire).expect("serialize legacy fixture");

        let decoded = decode_transaction_status_meta(0, &bytes).expect("decode");

        assert_eq!(
            decoded,
            TransactionStatusMeta {
                fee: 1,
                pre_balances: vec![2],
                post_balances: vec![1],
                ..TransactionStatusMeta::default()
            }
        );
    }

    #[test]
    fn legacy_schema_rejects_bytes_after_post_token_balances() {
        let wire = LegacyWireMeta {
            status: Ok(()),
            fee: 1,
            pre_balances: Vec::new(),
            post_balances: Vec::new(),
            inner_instructions: None,
            log_messages: None,
            pre_token_balances: None,
            post_token_balances: None,
        };
        let mut bytes = bincode::serialize(&wire).expect("serialize legacy fixture");
        bytes.push(0);

        assert!(decode_legacy_bincode::<LegacyV1_5_12UiTokenAmount>(&bytes).is_err());
    }

    #[test]
    fn legacy_borsh_error_string_does_not_shift_following_fields() {
        let wire = LegacyWireMeta {
            status: Err(LegacyTransactionError::InstructionError(
                3,
                LegacyInstructionError::BorshIoError("historic borsh error".to_owned()),
            )),
            fee: 9_999,
            pre_balances: vec![10, 20],
            post_balances: vec![5, 25],
            inner_instructions: None,
            log_messages: Some(vec!["after the error payload".to_owned()]),
            pre_token_balances: None,
            post_token_balances: None,
        };
        let bytes = bincode::serialize(&wire).expect("serialize legacy fixture");
        // Lock this regression test to the historical bincode ABI instead of
        // merely round-tripping our compatibility enums. The prefix is:
        // Result::Err(1), TransactionError::InstructionError(8), instruction
        // index 3, InstructionError::BorshIoError(44), string length 20.
        assert_eq!(
            &bytes[..21],
            &[
                1, 0, 0, 0, 8, 0, 0, 0, 3, 44, 0, 0, 0, 20, 0, 0, 0, 0, 0, 0, 0,
            ]
        );
        assert_eq!(&bytes[21..41], b"historic borsh error");

        let decoded = decode_transaction_status_meta(0, &bytes).expect("decode");

        assert_eq!(
            decoded.status,
            Err(TransactionError::InstructionError(
                3,
                InstructionError::BorshIoError,
            ))
        );
        assert_eq!(decoded.fee, 9_999);
        assert_eq!(decoded.pre_balances, [10, 20]);
        assert_eq!(decoded.post_balances, [5, 25]);
        assert_eq!(
            decoded.log_messages.as_deref(),
            Some(["after the error payload".to_owned()].as_slice())
        );
    }

    #[test]
    fn legacy_duplicate_signature_tag_maps_without_shifting_following_fields() {
        let wire = LegacyWireMeta {
            status: Err(LegacyTransactionError::DuplicateSignature),
            fee: 7_777,
            pre_balances: vec![9, 8],
            post_balances: vec![7, 10],
            inner_instructions: None,
            log_messages: Some(vec!["after duplicate signature".to_owned()]),
            pre_token_balances: None,
            post_token_balances: None,
        };
        let bytes = bincode::serialize(&wire).expect("serialize legacy fixture");
        assert_eq!(&bytes[..8], &[1, 0, 0, 0, 6, 0, 0, 0]);

        let decoded = decode_transaction_status_meta(0, &bytes).expect("decode");

        assert_eq!(decoded.status, Err(TransactionError::AlreadyProcessed));
        assert_eq!(decoded.fee, 7_777);
        assert_eq!(decoded.pre_balances, [9, 8]);
        assert_eq!(decoded.post_balances, [7, 10]);
        assert_eq!(
            decoded.log_messages.as_deref(),
            Some(["after duplicate signature".to_owned()].as_slice())
        );
    }

    #[test]
    fn legacy_error_grammar_rejects_post_cutoff_discriminants() {
        // Result::Err followed by the first post-cutoff TransactionError tag.
        assert!(
            decode_legacy_bincode::<LegacyV1_5_12UiTokenAmount>(&[1, 0, 0, 0, 16, 0, 0, 0,])
                .is_err()
        );
        // Result::Err, TransactionError::InstructionError, index 0, followed
        // by the first post-cutoff InstructionError tag.
        assert!(
            decode_legacy_bincode::<LegacyV1_5_12UiTokenAmount>(&[
                1, 0, 0, 0, 8, 0, 0, 0, 0, 47, 0, 0, 0,
            ])
            .is_err()
        );
    }

    #[test]
    fn decodes_cid_verified_epoch_89_metadata_fixture() {
        // Decompressed bytes SHA-256:
        // 6c51568e4763c336dc5261d76220cf1360217eae3aa027970a81e9c642153f87
        let bytes = decode_hex_fixture(include_str!(
            "../tests/fixtures/old-faithful-meta-slot-38879999.hex"
        ));

        let decoded = decode_transaction_status_meta(38_879_999, &bytes).expect("decode fixture");

        assert_eq!(decoded.fee, 5_000);
        assert_eq!(decoded.pre_balances.len(), 14);
        assert_eq!(decoded.post_balances.len(), 14);
        let groups = decoded.inner_instructions.expect("inner instructions");
        assert_eq!(groups.len(), 5);
        assert_eq!(groups[0].instructions[0].instruction.program_id_index, 12);
        assert!(
            groups
                .iter()
                .flat_map(|group| &group.instructions)
                .all(|instruction| instruction.stack_height.is_none())
        );
    }

    #[test]
    fn decodes_cid_verified_epoch_93_metadata_fixture() {
        // Decompressed bytes SHA-256:
        // 8335de6abd0f462a9a93e20122e343009a90301846ab4beee4d33502f27e31fe
        let bytes = decode_hex_fixture(include_str!(
            "../tests/fixtures/old-faithful-meta-slot-40607999.hex"
        ));

        let decoded = decode_transaction_status_meta(40_607_999, &bytes).expect("decode fixture");

        assert_eq!(decoded.fee, 5_000);
        assert_eq!(decoded.pre_balances.len(), 14);
        assert_eq!(decoded.post_balances.len(), 14);
        let groups = decoded.inner_instructions.expect("inner instructions");
        assert_eq!(groups.len(), 3);
        assert_eq!(groups[0].instructions[0].instruction.program_id_index, 10);
        assert!(
            groups
                .iter()
                .flat_map(|group| &group.instructions)
                .all(|instruction| instruction.stack_height.is_none())
        );
    }

    #[test]
    fn canonicalizes_cid_verified_dual_shape_zero_token_amount() {
        // Eight zero bytes are both f64 0.0 and a zero-length bincode String.
        // Solana's amount and decimals fields disambiguate the valid producer
        // representation and define the current normalized value.
        let bytes = decode_hex_fixture(include_str!(
            "../tests/fixtures/old-faithful-meta-slot-66550000-zero-token.hex"
        ));

        assert!(decode_legacy_bincode::<LegacyV1_5_12UiTokenAmount>(&bytes).is_ok());
        assert!(decode_legacy_bincode::<LegacyV1_5_9UiTokenAmount>(&bytes).is_err());
        let decoded = decode_transaction_status_meta(66_550_000, &bytes).expect("decode fixture");

        assert_eq!(decoded.fee, 15_000);
        assert_eq!(decoded.pre_balances.len(), 7);
        assert_eq!(decoded.post_balances.len(), 7);
        let token_balances: Vec<_> = decoded
            .pre_token_balances
            .iter()
            .chain(&decoded.post_token_balances)
            .flatten()
            .collect();
        assert_eq!(token_balances.len(), 1);
        let balance = token_balances[0];
        assert_eq!(balance.ui_token_amount.ui_amount, Some(0.0));
        assert_eq!(balance.ui_token_amount.decimals, 5);
        assert_eq!(balance.ui_token_amount.amount, "0");
        assert_eq!(balance.ui_token_amount.ui_amount_string, "0");
    }

    #[test]
    fn decodes_cid_verified_pre_cutoff_protobuf_fixture() {
        // Decompressed bytes SHA-256:
        // 13546d3b3e2f2e924661a39c647211cb42664362cb49fe2d37907c8b85da4d20
        let bytes = decode_hex_fixture(include_str!(
            "../tests/fixtures/old-faithful-meta-slot-67823992-protobuf.hex"
        ));

        assert!(decode_legacy_bincode::<LegacyV1_5_12UiTokenAmount>(&bytes).is_err());
        assert!(decode_legacy_bincode::<LegacyV1_5_13UiTokenAmount>(&bytes).is_err());
        let decoded = decode_transaction_status_meta(67_823_992, &bytes).expect("decode fixture");

        assert_eq!(decoded.fee, 5_000);
        assert_eq!(
            decoded.pre_balances,
            [
                139_347_285_000,
                23_357_760,
                3_591_360,
                7_299_063_360,
                1_141_440,
                1
            ]
        );
        assert_eq!(
            decoded.post_balances,
            [
                139_347_280_000,
                23_357_760,
                3_591_360,
                7_299_063_360,
                1_141_440,
                1
            ]
        );
    }

    #[test]
    fn decodes_cid_verified_v1_5_13_bincode_fixture() {
        // Decompressed bytes SHA-256:
        // db8cc6332ca542423c86f7924727dc3200d0213427b4f72e32e43e48f0685aa8
        let bytes = decode_hex_fixture(include_str!(
            "../tests/fixtures/old-faithful-meta-slot-67700000-v1.5.13.hex"
        ));

        assert!(decode_legacy_bincode::<LegacyV1_5_12UiTokenAmount>(&bytes).is_err());
        let direct = decode_legacy_bincode::<LegacyV1_5_13UiTokenAmount>(&bytes)
            .expect("decode exact Solana v1.5.13 schema");
        let decoded = decode_transaction_status_meta(67_700_000, &bytes).expect("decode fixture");

        assert_eq!(decoded, direct);
        assert_eq!(decoded.fee, 10_000);
        assert_eq!(decoded.pre_balances.len(), 6);
        assert_eq!(decoded.post_balances.len(), 6);
        let pre = decoded.pre_token_balances.expect("pre token balances");
        assert_eq!(pre.len(), 2);
        assert_eq!(pre[0].ui_token_amount.ui_amount, Some(425.0));
        assert_eq!(pre[0].ui_token_amount.amount, "42500000");
        assert_eq!(pre[0].ui_token_amount.ui_amount_string, "425");
        assert!(pre.iter().all(|balance| balance.owner.is_empty()));
        assert!(pre.iter().all(|balance| balance.program_id.is_empty()));
    }

    #[test]
    fn rejects_different_successful_schema_results() {
        let first = TransactionStatusMeta {
            fee: 1,
            ..TransactionStatusMeta::default()
        };
        let second = TransactionStatusMeta {
            fee: 2,
            ..TransactionStatusMeta::default()
        };

        let error = select_unique_metadata(
            67_700_000,
            156,
            [("first schema", Ok(first)), ("second schema", Ok(second))],
        )
        .expect_err("different successful decodes are ambiguous");

        assert!(error.to_string().contains("ambiguous transaction metadata"));
    }

    #[test]
    fn fixture_provenance_manifest_matches_the_checked_in_bytes() {
        let manifest: serde_json::Value = serde_json::from_str(include_str!(
            "../tests/fixtures/old-faithful-meta-provenance.json"
        ))
        .expect("valid fixture provenance manifest");
        let fixtures = manifest["fixtures"].as_array().expect("fixture array");
        assert_eq!(fixtures.len(), 5);

        for (expected_slot, fixture_path) in [
            (
                38_879_999,
                include_str!("../tests/fixtures/old-faithful-meta-slot-38879999.hex"),
            ),
            (
                40_607_999,
                include_str!("../tests/fixtures/old-faithful-meta-slot-40607999.hex"),
            ),
            (
                66_550_000,
                include_str!("../tests/fixtures/old-faithful-meta-slot-66550000-zero-token.hex"),
            ),
            (
                67_700_000,
                include_str!("../tests/fixtures/old-faithful-meta-slot-67700000-v1.5.13.hex"),
            ),
            (
                67_823_992,
                include_str!("../tests/fixtures/old-faithful-meta-slot-67823992-protobuf.hex"),
            ),
        ] {
            let provenance = fixtures
                .iter()
                .find(|entry| entry["slot"].as_u64() == Some(expected_slot))
                .expect("fixture slot has provenance");
            let bytes = decode_hex_fixture(fixture_path);

            assert_eq!(
                provenance["decompressed_metadata"]["length"].as_u64(),
                Some(bytes.len() as u64)
            );
            assert_eq!(
                provenance["decompressed_metadata"]["sha256"].as_str(),
                Some(sha256_hex(&bytes).as_str())
            );
            assert_eq!(provenance["transaction_node"]["cid_verified"], true);
        }
    }

    #[test]
    fn legacy_wire_types_match_decode_types() {
        fn assert_deserialize<T: for<'de> serde::Deserialize<'de>>() {}
        assert_deserialize::<LegacyInnerInstructions>();
        assert_deserialize::<LegacyV1_5_12UiTokenAmount>();
        assert_deserialize::<LegacyV1_5_9UiTokenAmount>();
        assert_deserialize::<LegacyV1_5_13UiTokenAmount>();
        assert_deserialize::<LegacyTransactionTokenBalance<LegacyV1_5_12UiTokenAmount>>();
        assert_deserialize::<LegacyTransactionTokenBalance<LegacyV1_5_9UiTokenAmount>>();
        assert_deserialize::<LegacyTransactionTokenBalance<LegacyV1_5_13UiTokenAmount>>();
        assert_deserialize::<LegacyTransactionStatusMeta<LegacyV1_5_12UiTokenAmount>>();
        assert_deserialize::<LegacyTransactionStatusMeta<LegacyV1_5_9UiTokenAmount>>();
        assert_deserialize::<LegacyTransactionStatusMeta<LegacyV1_5_13UiTokenAmount>>();
    }
}
