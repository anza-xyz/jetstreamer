use crate::account_updates::AccountUpdate;
use lencode::prelude::*;
use solana_hash::Hash;
use solana_message::v0::LoadedAddresses;
use solana_transaction::{TransactionResult, versioned::VersionedTransaction};
use solana_transaction_context::TransactionReturnData;
use solana_transaction_status::{InnerInstructions, Rewards, TransactionTokenBalance};

/// A Solana transaction with associated metadata and account updates.
#[derive(Encode, Decode, Debug, Clone)]
#[repr(C)]
pub struct Transaction {
    /// Slot that contains the [`Transaction`]
    pub slot: u64,
    /// Index of the transaction within the slot.
    pub index: usize,
    /// Transaction signature.
    pub signature: solana_signature::Signature,
    /// Hash of the transaction message.
    pub message_hash: Hash,
    /// Indicates whether the transaction is a vote.
    pub is_vote: bool,
    pub status: TransactionResult<()>,
    pub fee: u64,
    pub pre_balances: Vec<u64>,
    pub post_balances: Vec<u64>,
    pub inner_instructions: Option<Vec<InnerInstructions>>,
    pub log_messages: Option<Vec<String>>,
    pub pre_token_balances: Option<Vec<TransactionTokenBalance>>,
    pub post_token_balances: Option<Vec<TransactionTokenBalance>>,
    pub rewards: Option<Rewards>,
    pub loaded_addresses: LoadedAddresses,
    pub return_data: Option<TransactionReturnData>,
    pub compute_units_consumed: Option<u64>,
    pub cost_units: Option<u64>,
    /// Fully decoded transaction.
    pub transaction: VersionedTransaction,
    /// Account updates associated with this transaction.
    pub account_updates: Vec<AccountUpdate>,
}
