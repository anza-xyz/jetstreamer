use crate::account_updates::AccountUpdate;
use lencode::prelude::*;
use solana_address::Address;
use solana_hash::Hash;
use solana_signature::Signature;
use solana_transaction::TransactionResult;
use solana_transaction_context::TransactionReturnData;
use solana_transaction_status::{InnerInstructions, Rewards};

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
    pub accounts: Vec<u8>,
    pub data: Vec<u8>,
}

#[derive(Encode, Decode, Debug, Clone, PartialEq, Eq, Default)]
#[repr(C)]
pub struct MessageAddressTableLookup {
    pub account_key: Address,
    pub writable_indexes: Vec<u8>,
    pub readonly_indexes: Vec<u8>,
}

#[derive(Encode, Decode, Debug, Clone, PartialEq, Eq, Default)]
#[repr(C)]
pub struct LegacyMessage {
    pub header: MessageHeader,
    pub account_keys: Vec<Address>,
    pub recent_blockhash: Hash,
    pub instructions: Vec<CompiledInstruction>,
}

#[derive(Encode, Decode, Debug, Clone, PartialEq, Eq, Default)]
#[repr(C)]
pub struct V0Message {
    pub header: MessageHeader,
    pub account_keys: Vec<Address>,
    pub recent_blockhash: Hash,
    pub instructions: Vec<CompiledInstruction>,
    pub address_table_lookups: Vec<MessageAddressTableLookup>,
}

#[derive(Encode, Decode, Debug, Clone, PartialEq, Eq)]
#[repr(u8)]
pub enum VersionedMessage {
    Legacy(LegacyMessage),
    V0(V0Message),
}

impl Default for VersionedMessage {
    fn default() -> Self {
        Self::Legacy(LegacyMessage::default())
    }
}

/// A Solana transaction with associated metadata and account updates.
#[derive(Encode, Decode, Debug, Clone)]
#[repr(C)]
pub struct Transaction {
    pub status: TransactionResult<()>,
    pub fee: u64,
    pub pre_balances: Vec<u64>,
    pub post_balances: Vec<u64>,
    pub inner_instructions: Option<Vec<InnerInstructions>>,
    pub log_messages: Option<Vec<String>>,
    pub pre_token_balances: Option<Vec<TransactionTokenBalance>>,
    pub post_token_balances: Option<Vec<TransactionTokenBalance>>,
    pub rewards: Option<Rewards>,
    pub return_data: Option<TransactionReturnData>,
    pub compute_units_consumed: Option<u64>,
    pub cost_units: Option<u64>,
    /// Signatures for the transaction.
    pub signatures: Vec<Signature>,
    /// Message to sign.
    pub message: VersionedMessage,
    /// Account updates associated with this transaction.
    pub account_updates: Vec<AccountUpdate>,
}

#[derive(Encode, Decode, Debug, Clone, PartialEq)]
#[repr(C)]
pub struct TransactionTokenBalance {
    pub account_index: u8,
    pub mint: Address,
    pub ui_token_amount: TokenAmount,
    pub owner: Option<Address>,
    pub program_id: Option<Address>,
}

#[derive(Encode, Decode, Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(C)]
pub struct TokenAmount {
    pub amount: u64,
    pub decimals: u8,
}
