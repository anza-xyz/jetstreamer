use jetstreamer_horizon::account_updates::AccountUpdate;
use lencode::prelude::*;
use log::info;
use solana_account::{AccountSharedData, ReadableAccount};
use solana_address::Address;
use solana_clock::Slot;
use solana_hash::Hash;
use solana_pubkey::Pubkey;
use solana_signature::Signature;
use solana_transaction::{sanitized::SanitizedTransaction, versioned::VersionedTransaction};
use solana_transaction_status::TransactionStatusMeta;
use std::{
    cell::RefCell,
    sync::{
        Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Instant,
};

const ACCOUNT_UPDATE_ENCODE_BUFFER_SIZE: usize = 12 * 1024 * 1024;
const LOG_EVERY_N_BLOCKS: u64 = 10;

thread_local! {
    static ENCODER: RefCell<DedupeEncoder> = RefCell::new(DedupeEncoder::new());
    // Keep the large encode buffer on the heap; some notifier threads have small stacks.
    static ACCOUNT_UPDATE_ENCODE_BUFFER: RefCell<Vec<u8>> =
        RefCell::new(vec![0u8; ACCOUNT_UPDATE_ENCODE_BUFFER_SIZE]);
}

#[derive(Debug, Clone, Copy)]
struct ThroughputSample {
    at: Instant,
    transactions: u64,
    account_updates: u64,
}

static STARTUP_ACCOUNTS: AtomicU64 = AtomicU64::new(0);
static ACCOUNT_UPDATES: AtomicU64 = AtomicU64::new(0);
static TRANSACTIONS: AtomicU64 = AtomicU64::new(0);
static TOTAL_IN_MEMORY_ACCOUNT_UPDATE_SIZE: AtomicU64 = AtomicU64::new(0);
static TOTAL_ENCODED_ACCOUNT_UPDATE_SIZE: AtomicU64 = AtomicU64::new(0);
static PACKING_ENABLED: AtomicBool = AtomicBool::new(true);
static LAST_THROUGHPUT_SAMPLE: Mutex<Option<ThroughputSample>> = Mutex::new(None);

pub fn reset() {
    STARTUP_ACCOUNTS.store(0, Ordering::Relaxed);
    ACCOUNT_UPDATES.store(0, Ordering::Relaxed);
    TRANSACTIONS.store(0, Ordering::Relaxed);
    TOTAL_IN_MEMORY_ACCOUNT_UPDATE_SIZE.store(0, Ordering::Relaxed);
    TOTAL_ENCODED_ACCOUNT_UPDATE_SIZE.store(0, Ordering::Relaxed);
    if let Ok(mut sample) = LAST_THROUGHPUT_SAMPLE.lock() {
        *sample = None;
    }
    ENCODER.with(|encoder| {
        encoder.borrow_mut().clear();
    });
}

pub fn notify_startup_account() {
    STARTUP_ACCOUNTS.fetch_add(1, Ordering::Relaxed);
}

pub fn set_packing_enabled(enabled: bool) {
    PACKING_ENABLED.store(enabled, Ordering::Relaxed);
}

pub fn notify_end_of_startup() {
    let startup_accounts = STARTUP_ACCOUNTS.load(Ordering::Relaxed);
    info!(
        target: "jetstreamer_node_geyser",
        "startup account updates: {}",
        startup_accounts
    );
}

pub fn notify_transaction(
    _slot: Slot,
    _transaction_slot_index: usize,
    _signature: &Signature,
    _message_hash: &Hash,
    _is_vote: bool,
    _transaction_status_meta: &TransactionStatusMeta,
    _transaction: &VersionedTransaction,
) {
    TRANSACTIONS.fetch_add(1, Ordering::Relaxed);
}

pub fn notify_account_update(
    slot: Slot,
    account: &AccountSharedData,
    _txn: &Option<&SanitizedTransaction>,
    pubkey: &Pubkey,
    write_version: u64,
) {
    ACCOUNT_UPDATES.fetch_add(1, Ordering::Relaxed);
    let memory_size = core::mem::size_of::<AccountUpdate>() + account.data().len();
    TOTAL_IN_MEMORY_ACCOUNT_UPDATE_SIZE.fetch_add(memory_size as u64, Ordering::Relaxed);
    if !PACKING_ENABLED.load(Ordering::Relaxed) {
        return;
    }

    let account_update = AccountUpdate {
        pubkey: Address::new_from_array(pubkey.to_bytes()),
        lamports: account.lamports(),
        owner: Address::new_from_array(account.owner().to_bytes()),
        executable: account.executable(),
        rent_epoch: account.rent_epoch(),
        data: account.data().to_vec(),
        write_version,
    };

    let encoded_len = ACCOUNT_UPDATE_ENCODE_BUFFER.with(|buffer| {
        let mut buffer = buffer.borrow_mut();
        let mut cursor = Cursor::new(&mut buffer[..]);

        let encoded_len = ENCODER.with(|encoder| {
            match account_update.encode_ext(&mut cursor, Some(&mut *encoder.borrow_mut())) {
                Ok(len) => len,
                Err(Error::WriterOutOfSpace) => {
                    panic!(
                        "account update exceeded encode buffer: slot={slot} pubkey={} data_len={} buffer_len={}",
                        account_update.pubkey,
                        account_update.data.len(),
                        ACCOUNT_UPDATE_ENCODE_BUFFER_SIZE
                    )
                }
                Err(err) => panic!("failed to encode account update for slot {slot}: {err:?}"),
            }
        });

        debug_assert_eq!(encoded_len, cursor.position());
        encoded_len
    });

    TOTAL_ENCODED_ACCOUNT_UPDATE_SIZE.fetch_add(encoded_len as u64, Ordering::Relaxed);
}

pub fn notify_block(slot: Slot) {
    if slot == u64::MAX || slot % LOG_EVERY_N_BLOCKS != 0 {
        return;
    }

    let transactions = TRANSACTIONS.load(Ordering::Relaxed);
    let account_updates = ACCOUNT_UPDATES.load(Ordering::Relaxed);
    let total_memory_size = TOTAL_IN_MEMORY_ACCOUNT_UPDATE_SIZE.load(Ordering::Relaxed);
    let total_encoded_size = TOTAL_ENCODED_ACCOUNT_UPDATE_SIZE.load(Ordering::Relaxed);

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
        "block slot {} total_txs={} total_account_updates={} memory_size={} encoded_size={} txs_per_sec={} accounts_per_sec={}",
        slot,
        transactions,
        account_updates,
        format_bytes(total_memory_size),
        format_bytes(total_encoded_size),
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
