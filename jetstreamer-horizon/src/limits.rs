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

/// Combined data-byte budget for *all* of a transaction's account updates
/// (the shared arena behind [`MAX_TX_ACCOUNT_UPDATES`] meta entries). This
/// is the *sum* of every written account's post-state data, distinct from
/// [`MAX_ACCOUNT_DATA_LEN`], which bounds a *single* account. Provable
/// bound: written accounts are a subset of a transaction's loaded
/// accounts, whose total data is capped at 64 MiB by
/// `MAX_LOADED_ACCOUNTS_DATA_SIZE_BYTES`; sized at 2× that for
/// realloc/account-creation headroom. Account updates come from replay
/// geyser (not the CAR), so this cannot be preflight-scanned — the bound
/// must hold by protocol.
pub const MAX_TX_ACCOUNT_UPDATE_DATA: usize = 128 * 1024 * 1024;

/// Max return-data payload. Mirrors [`solana_cpi::MAX_RETURN_DATA`].
pub const MAX_RETURN_DATA_LEN: usize = solana_cpi::MAX_RETURN_DATA;

/// Max number of top-level instructions in a transaction message.
/// Practical: matches the execution-trace depth limit, which bounds every
/// transaction that *executes*. Known gap: the packet alone would allow
/// ~366 minimal (3-byte) instructions in a committed-but-failed spam
/// transaction. Instructions are stored inline (~10.5 KiB per slot, twice —
/// once per message variant), so raising this multiplies the in-memory
/// `Transaction` size and the variant-swap memset cost; if a preflight
/// scan of a target epoch ever reports a transaction above this cap, the
/// fix is to restructure instructions as a flat arena (like
/// [`crate::transactions::LogMessages`]), not to bump this constant.
pub const MAX_TX_INSTRUCTIONS: usize = 64;

/// Max number of signatures in a transaction. Practical: the message header
/// field is u8 but packet size caps this far lower.
pub const MAX_TX_SIGS: usize = 19;

/// Max number of log lines per transaction. Provable bound: `sol_log`
/// consumes at least `syscall_base_cost` (100 CU) per call, so the 1.4M CU
/// budget caps charged lines at ~14 000; runtime-generated invoke/success
/// lines add at most one pair per traced instruction.
pub const MAX_TX_LOG_MSGS: usize = 16_384;

/// Combined byte cap for all of a transaction's log lines (stored in a flat
/// arena — see [`crate::transactions::LogMessages`] — so neither line count
/// nor single-line length is independently capped). Provable bound for
/// modern epochs: log syscalls consume at least one CU per byte, so the
/// 1.4M CU budget caps charged bytes at ~1.4 MiB; uncharged runtime lines
/// (invoke/success/return-data) add a few KiB. Sized ~3× that ceiling for
/// headroom on epochs with older compute-cost models.
pub const MAX_TX_LOG_DATA: usize = 4 * 1024 * 1024;

/// Max number of rewards attached to a transaction. Practical — real
/// transactions attach at most one or two (vote/stake).
pub const MAX_TX_REWARDS: usize = 16;

/// Max number of token balance entries (pre or post). Upper bound matches
/// [`MAX_TX_ACCOUNTS`] since each token balance is keyed by account index.
pub const MAX_TX_TOKEN_BALANCES: usize = MAX_TX_ACCOUNTS;

/// Max length of a `TransactionError::Custom`-like string payload. Practical;
/// typical custom error strings are tiny program-defined messages.
pub const MAX_CUSTOM_ERROR_LEN: usize = 256;

/// Max rewards in a single block's reward list. Practical: partitioned
/// epoch-reward distribution credits at most 4 096 stake accounts per slot;
/// headroom for the boundary slot's vote-reward burst.
pub const MAX_BLOCK_REWARDS: usize = 8_192;

/// Max runtime-direct ("orphan") account updates attached to one block's
/// pre-transaction phase. Practical: dominated by the epoch-boundary
/// vote-reward burst (one write per vote account) plus per-slot sysvars and
/// a 4 096-entry stake reward partition.
pub const MAX_SLOT_PRE_UPDATES: usize = 16_384;

/// Combined data-byte cap for one block's pre-transaction orphan updates.
/// Practical: worst case ≈ vote-reward burst (thousands of ~3.7 KiB vote
/// states) + SlotHashes (~20 KiB) + a stake partition (~1 MiB).
pub const MAX_SLOT_PRE_UPDATE_DATA: usize = 32 * 1024 * 1024;

/// Max orphan updates in one block's post-transaction (freeze) phase.
/// Practical: fee distribution to the leader, incinerator, and a historical
/// rent-collection partition.
pub const MAX_SLOT_POST_UPDATES: usize = 4_096;

/// Combined data-byte cap for one block's post-transaction orphan updates.
pub const MAX_SLOT_POST_UPDATE_DATA: usize = 8 * 1024 * 1024;

/// Max account updates attributed directly to an epoch notification
/// (feature activations, builtin migrations at the boundary bank).
pub const MAX_EPOCH_UPDATES: usize = 4_096;

/// Combined data-byte cap for epoch-attributed account updates.
pub const MAX_EPOCH_UPDATE_DATA: usize = 8 * 1024 * 1024;

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
