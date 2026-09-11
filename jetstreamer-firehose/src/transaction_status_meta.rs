//! Versioned decoding for transaction metadata stored in Old Faithful.
//!
//! The pre-protobuf archive bytes use the Solana storage schema from before
//! token balances gained `owner` and `program_id`. Deserializing those bytes
//! into today's `StoredTransactionStatusMeta` is not backward compatible when
//! a token-balance vector is nonempty: the added fields consume bytes that
//! belong to the next balance or the next metadata field. Keep that historical
//! wire schema explicit here and convert it into current public Solana types
//! only after decoding.
//!
//! Solana commit `7e6528972948c3f35b3ce21ae202ffd3155c9ba6`
//! switched Blockstore transaction-status writes to protobuf on 2021-03-05.
//! Its immediate parent, `bd13262b420779fba2e6103600bb806fcb3e96e4`,
//! defines the last bincode schema: the eight fields and enum ranges copied
//! below. Later fields and variants were added after the archive's bincode
//! cutoff and must not make malformed early records valid.

use {
    crate::{SharedError, epochs::slot_to_epoch},
    bincode::Options as _,
    serde::Deserialize,
    solana_instruction_error::InstructionError,
    solana_message::{compiled_instruction::CompiledInstruction, v0::LoadedAddresses},
    solana_storage_proto::StoredTokenAmount,
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

/// First slot whose Old Faithful transaction-status metadata is stored as
/// protobuf. Earlier metadata uses the legacy bincode storage schema. This is
/// an archive-input boundary, independent of the runtime that executed a slot.
pub const OLD_FAITHFUL_PROTOBUF_META_START_SLOT: u64 = 157 * 432_000;

/// Transaction-metadata encoding selected from the source slot.
///
/// Each side of the archive boundary accepts only its documented encoding.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OldFaithfulMetaEncoding {
    /// Decode the pre-token-owner bincode schema.
    Bincode,
    /// Decode protobuf directly.
    Protobuf,
}

/// Selects the Old Faithful transaction-metadata decoder solely from `slot`.
#[inline]
pub const fn old_faithful_meta_encoding(slot: u64) -> OldFaithfulMetaEncoding {
    if slot < OLD_FAITHFUL_PROTOBUF_META_START_SLOT {
        OldFaithfulMetaEncoding::Bincode
    } else {
        OldFaithfulMetaEncoding::Protobuf
    }
}

#[derive(Deserialize)]
struct LegacyStoredTransactionTokenBalance {
    account_index: u8,
    mint: String,
    ui_token_amount: StoredTokenAmount,
}

impl From<LegacyStoredTransactionTokenBalance> for TransactionTokenBalance {
    fn from(value: LegacyStoredTransactionTokenBalance) -> Self {
        Self {
            account_index: value.account_index,
            mint: value.mint,
            ui_token_amount: value.ui_token_amount.into(),
            owner: String::new(),
            program_id: String::new(),
        }
    }
}

#[derive(Deserialize)]
struct LegacyStoredTransactionStatusMeta {
    status: Result<(), LegacyTransactionError>,
    fee: u64,
    pre_balances: Vec<u64>,
    post_balances: Vec<u64>,
    #[serde(deserialize_with = "solana_serde::default_on_eof")]
    inner_instructions: Option<Vec<LegacyInnerInstructions>>,
    #[serde(deserialize_with = "solana_serde::default_on_eof")]
    log_messages: Option<Vec<String>>,
    #[serde(deserialize_with = "solana_serde::default_on_eof")]
    pre_token_balances: Option<Vec<LegacyStoredTransactionTokenBalance>>,
    #[serde(deserialize_with = "solana_serde::default_on_eof")]
    post_token_balances: Option<Vec<LegacyStoredTransactionTokenBalance>>,
}

impl From<LegacyStoredTransactionStatusMeta> for TransactionStatusMeta {
    fn from(value: LegacyStoredTransactionStatusMeta) -> Self {
        Self {
            status: value.status.map_err(Into::into),
            fee: value.fee,
            pre_balances: value.pre_balances,
            post_balances: value.post_balances,
            inner_instructions: value
                .inner_instructions
                .map(|groups| groups.into_iter().map(Into::into).collect()),
            log_messages: value.log_messages,
            pre_token_balances: value
                .pre_token_balances
                .map(|balances| balances.into_iter().map(Into::into).collect()),
            post_token_balances: value
                .post_token_balances
                .map(|balances| balances.into_iter().map(Into::into).collect()),
            rewards: None,
            loaded_addresses: LoadedAddresses::default(),
            return_data: None,
            compute_units_consumed: None,
            cost_units: None,
        }
    }
}

fn decode_legacy_bincode(metadata_bytes: &[u8]) -> Result<TransactionStatusMeta, bincode::Error> {
    // Match the historical fixed-integer encoding, require the complete
    // cutoff-era schema, and bind allocations to the CID-verified input frame.
    bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .reject_trailing_bytes()
        .with_limit(metadata_bytes.len() as u64)
        .deserialize::<LegacyStoredTransactionStatusMeta>(metadata_bytes)
        .map(Into::into)
}

pub(crate) fn decode_transaction_status_meta(
    slot: u64,
    metadata_bytes: &[u8],
) -> Result<TransactionStatusMeta, SharedError> {
    let epoch = slot_to_epoch(slot);
    match old_faithful_meta_encoding(slot) {
        OldFaithfulMetaEncoding::Bincode => decode_legacy_bincode(metadata_bytes).map_err(|error| {
            Box::new(io::Error::other(format!(
                "decode legacy bincode transaction metadata (slot {slot}, epoch {epoch}): {error}"
            ))) as SharedError
        }),
        OldFaithfulMetaEncoding::Protobuf => {
            let proto: solana_storage_proto::convert::generated::TransactionStatusMeta =
                prost_011::Message::decode(metadata_bytes).map_err(|error| {
                    Box::new(io::Error::other(format!(
                        "protobuf decode transaction metadata (slot {slot}, epoch {epoch}): {error}"
                    ))) as SharedError
                })?;

            proto.try_into().map_err(|error| {
                Box::new(io::Error::other(format!(
                    "convert transaction metadata proto (slot {slot}, epoch {epoch}): {error}"
                ))) as SharedError
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::{
            LegacyInnerInstructions, LegacyInstructionError, LegacyStoredTransactionStatusMeta,
            LegacyStoredTransactionTokenBalance, LegacyTransactionError,
            OLD_FAITHFUL_PROTOBUF_META_START_SLOT, OldFaithfulMetaEncoding, decode_legacy_bincode,
            decode_transaction_status_meta, old_faithful_meta_encoding,
        },
        serde::Serialize,
        sha2::{Digest as _, Sha256},
        solana_instruction_error::InstructionError,
        solana_message::compiled_instruction::CompiledInstruction,
        solana_storage_proto::StoredTokenAmount,
        solana_transaction_error::TransactionError,
        solana_transaction_status::TransactionStatusMeta,
        std::fmt::Write as _,
    };

    #[derive(Serialize)]
    struct LegacyWireTokenBalance<'a> {
        account_index: u8,
        mint: &'a str,
        ui_token_amount: StoredTokenAmount,
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

    fn token_balance(account_index: u8, mint: &'static str) -> LegacyWireTokenBalance<'static> {
        LegacyWireTokenBalance {
            account_index,
            mint,
            ui_token_amount: StoredTokenAmount {
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
            OldFaithfulMetaEncoding::Bincode
        );
        assert_eq!(
            old_faithful_meta_encoding(OLD_FAITHFUL_PROTOBUF_META_START_SLOT),
            OldFaithfulMetaEncoding::Protobuf
        );
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
    fn legacy_schema_defaults_fields_introduced_after_the_cutoff() {
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
        let bytes = bincode::serialize(&wire).expect("serialize legacy fixture");

        let decoded = decode_transaction_status_meta(0, &bytes).expect("decode");

        assert_eq!(
            decoded,
            TransactionStatusMeta {
                fee: 1,
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

        assert!(decode_legacy_bincode(&bytes).is_err());
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
        assert!(decode_legacy_bincode(&[1, 0, 0, 0, 16, 0, 0, 0]).is_err());
        // Result::Err, TransactionError::InstructionError, index 0, followed
        // by the first post-cutoff InstructionError tag.
        assert!(decode_legacy_bincode(&[1, 0, 0, 0, 8, 0, 0, 0, 0, 47, 0, 0, 0]).is_err());
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
    fn fixture_provenance_manifest_matches_the_checked_in_bytes() {
        let manifest: serde_json::Value = serde_json::from_str(include_str!(
            "../tests/fixtures/old-faithful-meta-provenance.json"
        ))
        .expect("valid fixture provenance manifest");
        let fixtures = manifest["fixtures"].as_array().expect("fixture array");
        assert_eq!(fixtures.len(), 2);

        for (expected_slot, fixture_path) in [
            (
                38_879_999,
                include_str!("../tests/fixtures/old-faithful-meta-slot-38879999.hex"),
            ),
            (
                40_607_999,
                include_str!("../tests/fixtures/old-faithful-meta-slot-40607999.hex"),
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
        assert_deserialize::<LegacyStoredTransactionTokenBalance>();
        assert_deserialize::<LegacyStoredTransactionStatusMeta>();
    }
}
