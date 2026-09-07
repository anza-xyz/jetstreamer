use log::info;
use solana_account::{AccountSharedData, ReadableAccount};
use solana_clock::Slot;
use std::{
    sync::{
        Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};

const LOG_EVERY_N_BLOCKS: u64 = 10;

#[derive(Debug, Clone, Copy)]
struct ThroughputSample {
    at: Instant,
    transactions: u64,
    account_updates: u64,
}

#[derive(Debug, Clone, Copy)]
struct TransactionCursor {
    slot: Slot,
    next_tx_index: u64,
}

static STARTUP_ACCOUNTS: AtomicU64 = AtomicU64::new(0);
static ACCOUNT_UPDATES: AtomicU64 = AtomicU64::new(0);
static TRANSACTIONS: AtomicU64 = AtomicU64::new(0);
static TOTAL_ACCOUNT_UPDATE_DATA_BYTES: AtomicU64 = AtomicU64::new(0);
static LAST_THROUGHPUT_SAMPLE: Mutex<Option<ThroughputSample>> = Mutex::new(None);
static TRANSACTION_CURSOR: Mutex<Option<TransactionCursor>> = Mutex::new(None);

pub fn reset() {
    STARTUP_ACCOUNTS.store(0, Ordering::Relaxed);
    ACCOUNT_UPDATES.store(0, Ordering::Relaxed);
    TRANSACTIONS.store(0, Ordering::Relaxed);
    TOTAL_ACCOUNT_UPDATE_DATA_BYTES.store(0, Ordering::Relaxed);
    if let Ok(mut sample) = LAST_THROUGHPUT_SAMPLE.lock() {
        *sample = None;
    }
    if let Ok(mut cursor) = TRANSACTION_CURSOR.lock() {
        *cursor = None;
    }
}

pub fn notify_startup_account() {
    STARTUP_ACCOUNTS.fetch_add(1, Ordering::Relaxed);
}

pub fn notify_end_of_startup() {
    let startup_accounts = STARTUP_ACCOUNTS.load(Ordering::Relaxed);
    info!(
        target: "jetstreamer_node_geyser",
        "startup account updates: {}",
        startup_accounts
    );
}

pub fn notify_transaction_range(slot: Slot, start_index: usize, count: usize) {
    if count == 0 {
        return;
    }
    let start_index = start_index as u64;
    let count = count as u64;
    let end_index = start_index
        .checked_add(count)
        .expect("transaction range overflow");

    if let Ok(mut cursor_guard) = TRANSACTION_CURSOR.lock() {
        match cursor_guard.as_mut() {
            Some(cursor) if slot < cursor.slot => {
                panic!(
                    "transaction notify went backwards: slot={} previous_slot={} start_index={} previous_next_index={}",
                    slot, cursor.slot, start_index, cursor.next_tx_index
                );
            }
            Some(cursor) if slot == cursor.slot => {
                if start_index != cursor.next_tx_index {
                    panic!(
                        "transaction notify gap/overlap in slot {}: start_index={} expected_start={}",
                        slot, start_index, cursor.next_tx_index
                    );
                }
                cursor.next_tx_index = end_index;
            }
            Some(cursor) => {
                if start_index != 0 {
                    panic!(
                        "transaction notify new slot {} did not restart at index 0 (start_index={}, previous_slot={}, previous_next_index={})",
                        slot, start_index, cursor.slot, cursor.next_tx_index
                    );
                }
                *cursor = TransactionCursor {
                    slot,
                    next_tx_index: end_index,
                };
            }
            None => {
                if start_index != 0 {
                    panic!(
                        "first transaction notify for slot {} did not start at index 0 (start_index={})",
                        slot, start_index
                    );
                }
                *cursor_guard = Some(TransactionCursor {
                    slot,
                    next_tx_index: end_index,
                });
            }
        }
    }

    TRANSACTIONS.fetch_add(count, Ordering::Relaxed);
}

pub fn notify_account_update(account: &AccountSharedData) {
    ACCOUNT_UPDATES.fetch_add(1, Ordering::Relaxed);
    TOTAL_ACCOUNT_UPDATE_DATA_BYTES.fetch_add(account.data().len() as u64, Ordering::Relaxed);
}

pub fn notify_block(slot: Slot) {
    if slot == u64::MAX || !slot.is_multiple_of(LOG_EVERY_N_BLOCKS) {
        return;
    }

    let transactions = TRANSACTIONS.load(Ordering::Relaxed);
    let account_updates = ACCOUNT_UPDATES.load(Ordering::Relaxed);
    let total_data_bytes = TOTAL_ACCOUNT_UPDATE_DATA_BYTES.load(Ordering::Relaxed);

    let now = Instant::now();
    let throughput = LAST_THROUGHPUT_SAMPLE
        .lock()
        .ok()
        .and_then(|mut last_sample| {
            let rates = last_sample.as_ref().and_then(|previous| {
                let elapsed = now.duration_since(previous.at).as_secs_f64();
                if elapsed <= 0.0 {
                    return None;
                }

                Some((
                    transactions.saturating_sub(previous.transactions) as f64 / elapsed,
                    account_updates.saturating_sub(previous.account_updates) as f64 / elapsed,
                ))
            });

            *last_sample = Some(ThroughputSample {
                at: now,
                transactions,
                account_updates,
            });

            rates
        });

    let (txs_per_sec, accounts_per_sec) = match throughput {
        Some((txs, accounts)) => (format!("{txs:.2}"), format!("{accounts:.2}")),
        None => ("n/a".to_string(), "n/a".to_string()),
    };

    info!(
        target: "jetstreamer_node_geyser",
        "block slot {} total_txs={} total_account_updates={} account_data={} txs_per_sec={} accounts_per_sec={}",
        slot,
        transactions,
        account_updates,
        format_bytes(total_data_bytes),
        txs_per_sec,
        accounts_per_sec
    );
}

fn format_bytes_f64(bytes: f64) -> String {
    const UNITS: [&str; 6] = ["B", "KB", "MB", "GB", "TB", "PB"];
    if bytes < 1024.0 {
        return format!("{bytes:.0} B");
    }

    let mut value = bytes;
    let mut unit_index = 0usize;
    while value >= 1024.0 && unit_index + 1 < UNITS.len() {
        value /= 1024.0;
        unit_index += 1;
    }

    format!("{value:.2} {}", UNITS[unit_index])
}

fn format_bytes(bytes: u64) -> String {
    format_bytes_f64(bytes as f64)
}
