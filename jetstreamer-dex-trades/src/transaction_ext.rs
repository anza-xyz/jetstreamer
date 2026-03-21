//! Extension methods on [`TransactionData`] for common queries.

use jetstreamer_firehose::firehose::TransactionData;
use solana_address::Address;
use solana_message::VersionedMessage;

pub trait TransactionExt {
    fn is_successful(&self) -> bool;
    fn is_failed(&self) -> bool;
    fn is_vote_transaction(&self) -> bool;
    fn account_keys(&self) -> Vec<Address>;
    fn get_logs(&self) -> Vec<String>;
    fn get_total_fee(&self) -> u64;
    fn get_base_fee(&self) -> u64;
    fn get_priority_fee(&self) -> u64;
    fn get_jito_tips(&self) -> u64;
}

const JITO_TIP_ACCOUNTS: &[&str] = &[
    "ADaUMid9yfUytqMBgopwjb2DTLSokTSzL1zt6iGPaS49",
    "Cw8CFyM9FkoMi7K7Crf6HNQqf4uEMzpKw6QNghXLvLkY",
    "HFqU5x63VTqvQss8hp11i4wVV8bD44PvwucfZ2bU7gRe",
    "DfXygSm4jCyNCybVYYK6DwvWqjKee8pbDmJGcLWNDXjh",
    "96gYZGLnJYVFmbjzopPSU6QiEV5fGqZNyN9nmNhvrZU5",
    "ADuUkR4vqLUMWXxW9gh6D6L8pMSawimctcNZ5pGwDcEt",
    "3AVi9Tg9Uo68tJfuvoKvqKNWKkC5wPdSSdeBnizKZ6jT",
    "DttWaMuVvTiduZRnguLF7jNxTgiMBZ1hyAumKUiL2KRL",
];

impl TransactionExt for TransactionData {
    fn is_successful(&self) -> bool {
        self.transaction_status_meta.status.is_ok()
    }

    fn is_failed(&self) -> bool {
        self.transaction_status_meta.status.is_err()
    }

    fn is_vote_transaction(&self) -> bool {
        self.is_vote
    }

    fn account_keys(&self) -> Vec<Address> {
        match &self.transaction.message {
            VersionedMessage::Legacy(msg) => msg.account_keys.clone(),
            VersionedMessage::V0(msg) => {
                let mut keys = msg.account_keys.clone();
                let loaded = &self.transaction_status_meta.loaded_addresses;
                keys.extend_from_slice(&loaded.writable);
                keys.extend_from_slice(&loaded.readonly);
                keys
            }
        }
    }

    fn get_logs(&self) -> Vec<String> {
        self.transaction_status_meta
            .log_messages
            .as_ref()
            .cloned()
            .unwrap_or_default()
    }

    fn get_total_fee(&self) -> u64 {
        self.transaction_status_meta.fee
    }

    fn get_base_fee(&self) -> u64 {
        const LAMPORTS_PER_SIGNATURE: u64 = 5000;
        let num_signatures = match &self.transaction.message {
            VersionedMessage::Legacy(msg) => msg.header.num_required_signatures as u64,
            VersionedMessage::V0(msg) => msg.header.num_required_signatures as u64,
        };
        num_signatures * LAMPORTS_PER_SIGNATURE
    }

    fn get_priority_fee(&self) -> u64 {
        self.get_total_fee().saturating_sub(self.get_base_fee())
    }

    fn get_jito_tips(&self) -> u64 {
        let account_keys = self.account_keys();
        let pre_balances = &self.transaction_status_meta.pre_balances;
        let post_balances = &self.transaction_status_meta.post_balances;

        let mut total_tips: u64 = 0;
        for (idx, account) in account_keys.iter().enumerate() {
            let account_str = account.to_string();
            if JITO_TIP_ACCOUNTS.contains(&account_str.as_str()) {
                let pre = pre_balances.get(idx).copied().unwrap_or(0);
                let post = post_balances.get(idx).copied().unwrap_or(0);
                if post > pre {
                    total_tips = total_tips.saturating_add(post - pre);
                }
            }
        }
        total_tips
    }
}
