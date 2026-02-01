use lencode::prelude::*;
use solana_hash::Hash;
use solana_transaction::versioned::VersionedTransaction;
use solana_transaction_status::TransactionStatusMeta;

use crate::account_updates::AccountUpdate;

/// A Solana transaction with associated metadata and account updates.
#[derive(Default, Encode, Decode, Debug, Clone)]
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
    /// Status metadata returned by the Solana runtime.
    pub transaction_status_meta: TransactionStatusMeta,
    /// Fully decoded transaction.
    pub transaction: VersionedTransaction,
    /// Account updates associated with this transaction.
    pub account_updates: Vec<AccountUpdate>,
}
