//! Fixed upper bounds for Solana transaction / account shapes, used to size
//! inline [`crate::zero_vec::ZeroVec`] buffers throughout this crate.
//!
//! Wherever an upstream Solana crate exposes a matching `const`, we re-export
//! it here with an `as usize` cast so a runtime bump in upstream (via a
//! dependency refresh) automatically bumps our compile-time caps too. Values
//! marked "practical" are conservative local bounds — the protocol allows
//! higher values but the transaction packet size (1 232 bytes) makes
//! exceeding them impossible in a single transaction.

/// Max account data length. Mirrors
/// [`solana_system_interface::MAX_PERMITTED_DATA_LENGTH`].
pub const MAX_ACCOUNT_DATA_LEN: usize = solana_system_interface::MAX_PERMITTED_DATA_LENGTH as usize;

/// Max number of distinct account keys referenced by a transaction (static
/// account keys + address-table-loaded keys). Mirrors
/// [`solana_transaction_context::MAX_ACCOUNTS_PER_TRANSACTION`].
pub const MAX_TX_ACCOUNTS: usize = solana_transaction_context::MAX_ACCOUNTS_PER_TRANSACTION;

/// Max accounts referenced by a single instruction (u8::MAX - 1 after the
/// program_id slot). Mirrors
/// [`solana_transaction_context::MAX_ACCOUNTS_PER_INSTRUCTION`].
pub const MAX_IX_ACCOUNTS: usize = solana_transaction_context::MAX_ACCOUNTS_PER_INSTRUCTION;

/// Max per-instruction data length. Mirrors
/// [`solana_transaction_context::MAX_INSTRUCTION_DATA_LEN`].
pub const MAX_IX_DATA_LEN: usize = solana_transaction_context::MAX_INSTRUCTION_DATA_LEN;

/// Max total inner-instruction trace length across a whole transaction
/// (top-level + CPI combined). Mirrors
/// [`solana_transaction_context::MAX_INSTRUCTION_TRACE_LENGTH`].
pub const MAX_TX_INNER_IX: usize = solana_transaction_context::MAX_INSTRUCTION_TRACE_LENGTH;

/// Max number of address-table lookups in a v0 message. Upper-bounded by
/// the account-lock limit since each lookup contributes loaded keys.
/// Mirrors [`solana_transaction::sanitized::MAX_TX_ACCOUNT_LOCKS`].
pub const MAX_TX_ADDR_LOOKUPS: usize = solana_transaction::sanitized::MAX_TX_ACCOUNT_LOCKS;

/// Max number of per-transaction account updates (meta entries in the flat
/// account-update arena). Matches
/// [`solana_transaction::sanitized::MAX_TX_ACCOUNT_LOCKS`] since only locked
/// accounts can be mutated by a transaction.
pub const MAX_TX_ACCOUNT_UPDATES: usize = solana_transaction::sanitized::MAX_TX_ACCOUNT_LOCKS;

/// Max return-data payload. Mirrors [`solana_cpi::MAX_RETURN_DATA`].
pub const MAX_RETURN_DATA_LEN: usize = solana_cpi::MAX_RETURN_DATA;

/// Max number of top-level instructions in a transaction message. Practical:
/// bounded by packet size (~1 232 bytes) — 64 is comfortably higher than any
/// real transaction.
pub const MAX_TX_INSTRUCTIONS: usize = 64;

/// Max number of signatures in a transaction. Practical: the message header
/// field is u8 but packet size caps this far lower.
pub const MAX_TX_SIGS: usize = 19;

/// Max bytes in a single log message. Practical bound; actual limit is CU-driven.
pub const MAX_LOG_MSG_LEN: usize = 2048;

/// Max number of log messages per transaction. Practical bound; the CU budget
/// (~1.4M) and per-log CU cost cap real-world usage far below this.
pub const MAX_TX_LOG_MSGS: usize = 256;

/// Max number of rewards attached to a transaction. Practical — real
/// transactions attach at most one or two (vote/stake).
pub const MAX_TX_REWARDS: usize = 16;

/// Max number of token balance entries (pre or post). Upper bound matches
/// [`MAX_TX_ACCOUNTS`] since each token balance is keyed by account index.
pub const MAX_TX_TOKEN_BALANCES: usize = MAX_TX_ACCOUNTS;

/// Max length of a `TransactionError::Custom`-like string payload. Practical;
/// typical custom error strings are tiny program-defined messages.
pub const MAX_CUSTOM_ERROR_LEN: usize = 256;

#[cfg(test)]
mod tests {
    use super::*;

    /// Sanity-check that the upstream-sourced constants still match the
    /// values we assumed when sizing buffers. If upstream bumps any of these,
    /// this test will fail and prompt a review of any assumptions downstream.
    #[test]
    fn upstream_constants_match_expected_values() {
        assert_eq!(MAX_ACCOUNT_DATA_LEN, 10 * 1024 * 1024);
        assert_eq!(MAX_TX_ACCOUNTS, 256);
        assert_eq!(MAX_IX_ACCOUNTS, 255);
        assert_eq!(MAX_IX_DATA_LEN, 10 * 1024);
        assert_eq!(MAX_TX_INNER_IX, 64);
        assert_eq!(MAX_TX_ADDR_LOOKUPS, 128);
        assert_eq!(MAX_TX_ACCOUNT_UPDATES, 128);
        assert_eq!(MAX_RETURN_DATA_LEN, 1024);
    }
}
