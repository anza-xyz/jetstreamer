//! Token transfer extraction utilities for DEX swap decoding.
//!
//! Parses SPL Token and Token-2022 Transfer/TransferChecked instructions from
//! inner instructions to determine which tokens were bought/sold and in what
//! amounts.

use jetstreamer_firehose::firehose::TransactionData;
use solana_transaction_status::TransactionTokenBalance;

const SPL_TOKEN_PROGRAM: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
const TOKEN_2022_PROGRAM: &str = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb";

const TRANSFER_DISCRIMINATOR: u8 = 3;
const TRANSFER_CHECKED_DISCRIMINATOR: u8 = 12;

/// Token account info resolved from pre/post token balances.
#[derive(Debug, Clone)]
pub struct TokenAccountInfo {
    pub mint: String,
    pub decimals: u8,
    pub pre_balance: u64,
    pub post_balance: u64,
}

impl TokenAccountInfo {
    pub fn post_balance_scaled(&self) -> f64 {
        self.post_balance as f64 / 10f64.powi(self.decimals as i32)
    }
}

/// A single transfer detected in inner instructions.
#[derive(Debug, Clone)]
pub struct TransferInfo {
    pub mint: String,
    pub vault: String,
    pub amount: u64,
    pub decimals: u8,
}

impl TransferInfo {
    pub fn amount_scaled(&self) -> f64 {
        self.amount as f64 / 10f64.powi(self.decimals as i32)
    }
}

/// Resolved swap amounts with direction.
#[derive(Debug, Clone)]
pub struct SwapAmounts {
    /// Token the user sent (into vault).
    pub sold: TransferInfo,
    /// Token the user received (from vault).
    pub bought: TransferInfo,
}

/// Partial swap amounts (one side may be missing in direct routing).
#[derive(Debug, Clone)]
pub struct PartialSwapAmounts {
    pub sold: Option<TransferInfo>,
    pub bought: Option<TransferInfo>,
}

/// Get token account info from pre/post token balances.
pub fn get_token_account_info(tx: &TransactionData, account: &str) -> Option<TokenAccountInfo> {
    let account_keys = get_all_account_keys(tx);
    let account_index = account_keys
        .iter()
        .position(|k| k.to_string() == account)?;

    let pre_balances = tx
        .transaction_status_meta
        .pre_token_balances
        .as_ref()
        .unwrap_or(&Vec::new())
        .clone();
    let post_balances = tx
        .transaction_status_meta
        .post_token_balances
        .as_ref()
        .unwrap_or(&Vec::new())
        .clone();

    let post_entry = post_balances
        .iter()
        .find(|b| b.account_index as usize == account_index);
    let pre_entry = pre_balances
        .iter()
        .find(|b| b.account_index as usize == account_index);

    let entry = post_entry.or(pre_entry)?;
    let mint = entry.mint.clone();
    let decimals = entry.ui_token_amount.decimals;

    let pre_amount = pre_entry
        .and_then(|e| e.ui_token_amount.amount.parse::<u64>().ok())
        .unwrap_or(0);
    let post_amount = post_entry
        .and_then(|e| e.ui_token_amount.amount.parse::<u64>().ok())
        .unwrap_or(0);

    Some(TokenAccountInfo {
        mint,
        decimals,
        pre_balance: pre_amount,
        post_balance: post_amount,
    })
}

/// Get swap amounts by analyzing SPL Token transfers in inner instructions.
///
/// - `vault_a` / `vault_b`: the two pool vault addresses
/// - `exclude_accounts`: accounts to skip (e.g. fee accounts)
/// - `take_largest`: if true, take the largest transfer for each vault
///   (needed for Stabble which sends multiple transfers)
pub fn get_swap_amounts(
    tx: &TransactionData,
    outer_idx: usize,
    _inner_idx: usize,
    vault_a: &str,
    vault_b: &str,
    exclude_accounts: Option<&[&str]>,
    take_largest: bool,
) -> Option<SwapAmounts> {
    let transfers = extract_transfers(tx, outer_idx, exclude_accounts);

    let mut vault_a_in: Option<(u64, String, String)> = None; // amount, source, mint
    let mut vault_a_out: Option<(u64, String, String)> = None;
    let mut vault_b_in: Option<(u64, String, String)> = None;
    let mut vault_b_out: Option<(u64, String, String)> = None;

    for t in &transfers {
        if t.destination == vault_a {
            let entry = &mut vault_a_in;
            if take_largest {
                if entry.as_ref().map_or(true, |e| t.amount > e.0) {
                    *entry = Some((t.amount, t.source.clone(), t.mint.clone()));
                }
            } else if entry.is_none() {
                *entry = Some((t.amount, t.source.clone(), t.mint.clone()));
            }
        } else if t.source == vault_a {
            let entry = &mut vault_a_out;
            if take_largest {
                if entry.as_ref().map_or(true, |e| t.amount > e.0) {
                    *entry = Some((t.amount, t.destination.clone(), t.mint.clone()));
                }
            } else if entry.is_none() {
                *entry = Some((t.amount, t.destination.clone(), t.mint.clone()));
            }
        }

        if t.destination == vault_b {
            let entry = &mut vault_b_in;
            if take_largest {
                if entry.as_ref().map_or(true, |e| t.amount > e.0) {
                    *entry = Some((t.amount, t.source.clone(), t.mint.clone()));
                }
            } else if entry.is_none() {
                *entry = Some((t.amount, t.source.clone(), t.mint.clone()));
            }
        } else if t.source == vault_b {
            let entry = &mut vault_b_out;
            if take_largest {
                if entry.as_ref().map_or(true, |e| t.amount > e.0) {
                    *entry = Some((t.amount, t.destination.clone(), t.mint.clone()));
                }
            } else if entry.is_none() {
                *entry = Some((t.amount, t.destination.clone(), t.mint.clone()));
            }
        }
    }

    // Determine direction: user sends to one vault, receives from the other
    let (sold_vault, sold_amount, sold_mint, bought_vault, bought_amount, bought_mint) =
        if vault_a_in.is_some() && vault_b_out.is_some() {
            let a_in = vault_a_in.unwrap();
            let b_out = vault_b_out.unwrap();
            (
                vault_a.to_string(),
                a_in.0,
                a_in.2,
                vault_b.to_string(),
                b_out.0,
                b_out.2,
            )
        } else if vault_b_in.is_some() && vault_a_out.is_some() {
            let b_in = vault_b_in.unwrap();
            let a_out = vault_a_out.unwrap();
            (
                vault_b.to_string(),
                b_in.0,
                b_in.2,
                vault_a.to_string(),
                a_out.0,
                a_out.2,
            )
        } else {
            log::debug!(
                "get_swap_amounts: no matching transfer pattern (vault_a_in={}, vault_a_out={}, vault_b_in={}, vault_b_out={})",
                vault_a_in.is_some(), vault_a_out.is_some(), vault_b_in.is_some(), vault_b_out.is_some()
            );
            return None;
        };

    let sold_info = get_token_account_info(tx, &sold_vault)?;
    let bought_info = get_token_account_info(tx, &bought_vault)?;

    Some(SwapAmounts {
        sold: TransferInfo {
            mint: sold_mint,
            vault: sold_vault,
            amount: sold_amount,
            decimals: sold_info.decimals,
        },
        bought: TransferInfo {
            mint: bought_mint,
            vault: bought_vault,
            amount: bought_amount,
            decimals: bought_info.decimals,
        },
    })
}

/// Get swap amounts where one side may be missing (for Obric V2 direct routing).
pub fn get_swap_amounts_partial(
    tx: &TransactionData,
    outer_idx: usize,
    _inner_idx: usize,
    vault_a: &str,
    vault_b: &str,
    vault_a_info: &TokenAccountInfo,
    vault_b_info: &TokenAccountInfo,
) -> Option<PartialSwapAmounts> {
    let transfers = extract_transfers(tx, outer_idx, None);

    let mut sold: Option<TransferInfo> = None;
    let mut bought: Option<TransferInfo> = None;

    for t in &transfers {
        // Transfer TO vault = user selling
        if t.destination == vault_a && sold.is_none() {
            sold = Some(TransferInfo {
                mint: vault_a_info.mint.clone(),
                vault: vault_a.to_string(),
                amount: t.amount,
                decimals: vault_a_info.decimals,
            });
        } else if t.destination == vault_b && sold.is_none() {
            sold = Some(TransferInfo {
                mint: vault_b_info.mint.clone(),
                vault: vault_b.to_string(),
                amount: t.amount,
                decimals: vault_b_info.decimals,
            });
        }
        // Transfer FROM vault = user buying
        if t.source == vault_a && bought.is_none() {
            bought = Some(TransferInfo {
                mint: vault_a_info.mint.clone(),
                vault: vault_a.to_string(),
                amount: t.amount,
                decimals: vault_a_info.decimals,
            });
        } else if t.source == vault_b && bought.is_none() {
            bought = Some(TransferInfo {
                mint: vault_b_info.mint.clone(),
                vault: vault_b.to_string(),
                amount: t.amount,
                decimals: vault_b_info.decimals,
            });
        }
    }

    if sold.is_none() && bought.is_none() {
        return None;
    }

    Some(PartialSwapAmounts { sold, bought })
}

// Internal: raw transfer parsed from inner instructions
#[derive(Debug)]
struct RawTransfer {
    source: String,
    destination: String,
    amount: u64,
    mint: String,
}

fn extract_transfers(
    tx: &TransactionData,
    outer_idx: usize,
    exclude_accounts: Option<&[&str]>,
) -> Vec<RawTransfer> {
    let account_keys = get_all_account_keys(tx);
    let inner_instructions = tx
        .transaction_status_meta
        .inner_instructions
        .as_ref()
        .unwrap_or(&Vec::new())
        .clone();

    // Find the inner instruction set matching this outer instruction (0-based index)
    let inner_set = inner_instructions
        .iter()
        .find(|s| s.index as usize == outer_idx - 1); // outer_idx is 1-based

    let inner_set = match inner_set {
        Some(s) => s,
        None => return Vec::new(),
    };

    let mut results = Vec::new();

    for inner_ix in &inner_set.instructions {
        let program_id = account_keys
            .get(inner_ix.instruction.program_id_index as usize)
            .map(|a| a.to_string())
            .unwrap_or_default();

        if program_id != SPL_TOKEN_PROGRAM && program_id != TOKEN_2022_PROGRAM {
            continue;
        }

        let data = &inner_ix.instruction.data;
        if data.is_empty() {
            continue;
        }

        let (source_idx, dest_idx, amount) = match data[0] {
            TRANSFER_DISCRIMINATOR if data.len() >= 9 => {
                let accounts = &inner_ix.instruction.accounts;
                if accounts.len() < 3 {
                    continue;
                }
                let amount = u64::from_le_bytes(data[1..9].try_into().unwrap());
                (accounts[0] as usize, accounts[1] as usize, amount)
            }
            TRANSFER_CHECKED_DISCRIMINATOR if data.len() >= 9 => {
                let accounts = &inner_ix.instruction.accounts;
                if accounts.len() < 4 {
                    continue;
                }
                let amount = u64::from_le_bytes(data[1..9].try_into().unwrap());
                (accounts[0] as usize, accounts[2] as usize, amount)
            }
            _ => continue,
        };

        let source = account_keys
            .get(source_idx)
            .map(|a| a.to_string())
            .unwrap_or_default();
        let destination = account_keys
            .get(dest_idx)
            .map(|a| a.to_string())
            .unwrap_or_default();

        if let Some(excluded) = exclude_accounts {
            if excluded.contains(&source.as_str()) || excluded.contains(&destination.as_str()) {
                continue;
            }
        }

        // Resolve mint from token balances
        let mint = resolve_mint_for_account(tx, &account_keys, source_idx)
            .or_else(|| resolve_mint_for_account(tx, &account_keys, dest_idx))
            .unwrap_or_default();

        results.push(RawTransfer {
            source,
            destination,
            amount,
            mint,
        });
    }

    results
}

fn resolve_mint_for_account(
    tx: &TransactionData,
    _account_keys: &[solana_address::Address],
    account_index: usize,
) -> Option<String> {
    let empty: Vec<TransactionTokenBalance> = Vec::new();
    let post = tx
        .transaction_status_meta
        .post_token_balances
        .as_ref()
        .unwrap_or(&empty);
    let pre = tx
        .transaction_status_meta
        .pre_token_balances
        .as_ref()
        .unwrap_or(&empty);
    let all_balances: Vec<&TransactionTokenBalance> = post.iter().chain(pre.iter()).collect();

    all_balances
        .iter()
        .find(|b| b.account_index as usize == account_index)
        .map(|b| b.mint.clone())
}

fn get_all_account_keys(tx: &TransactionData) -> Vec<solana_address::Address> {
    use solana_message::VersionedMessage;
    match &tx.transaction.message {
        VersionedMessage::Legacy(msg) => msg.account_keys.clone(),
        VersionedMessage::V0(msg) => {
            let mut keys = msg.account_keys.clone();
            let loaded = &tx.transaction_status_meta.loaded_addresses;
            keys.extend_from_slice(&loaded.writable);
            keys.extend_from_slice(&loaded.readonly);
            keys
        }
    }
}
