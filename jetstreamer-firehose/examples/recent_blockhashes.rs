//! Lists transaction recent-blockhash values that do not appear as an entry
//! hash in the inspected slot range. Start the range at least one blockhash
//! retention window before the transactions under investigation so ordinary
//! hashes are not misclassified; durable-nonce hashes can still be unmatched.

use {
    futures_util::FutureExt,
    jetstreamer_firehose::firehose::{self, TransactionData},
    std::{
        collections::{BTreeMap, BTreeSet},
        sync::{Arc, Mutex},
    },
};

#[derive(Clone, Copy)]
struct Observed {
    count: u64,
    first_slot: u64,
    last_slot: u64,
}

fn entry_handler(
    entry_hashes: Arc<Mutex<BTreeSet<String>>>,
) -> impl firehose::Handler<firehose::EntryData> {
    move |_thread_id, entry| {
        let entry_hashes = Arc::clone(&entry_hashes);
        async move {
            entry_hashes.lock().unwrap().insert(entry.hash.to_string());
            Ok(())
        }
        .boxed()
    }
}

fn transaction_handler(
    observed: Arc<Mutex<BTreeMap<String, Observed>>>,
) -> impl firehose::Handler<TransactionData> {
    move |_thread_id, tx| {
        let observed = Arc::clone(&observed);
        async move {
            let recent_blockhash = match &tx.transaction.message {
                solana_message::VersionedMessage::Legacy(message) => &message.recent_blockhash,
                solana_message::VersionedMessage::V0(message) => &message.recent_blockhash,
            }
            .to_string();
            let mut observed = observed.lock().unwrap();
            observed
                .entry(recent_blockhash)
                .and_modify(|item| {
                    item.count += 1;
                    item.first_slot = item.first_slot.min(tx.slot);
                    item.last_slot = item.last_slot.max(tx.slot);
                })
                .or_insert(Observed {
                    count: 1,
                    first_slot: tx.slot,
                    last_slot: tx.slot,
                });
            Ok(())
        }
        .boxed()
    }
}

#[tokio::main]
async fn main() {
    let start_slot: u64 = std::env::args()
        .nth(1)
        .expect("usage: recent_blockhashes START_SLOT END_SLOT_EXCLUSIVE")
        .parse()
        .expect("start slot must be an integer");
    let end_slot: u64 = std::env::args()
        .nth(2)
        .expect("usage: recent_blockhashes START_SLOT END_SLOT_EXCLUSIVE")
        .parse()
        .expect("end slot must be an integer");
    assert!(
        end_slot > start_slot,
        "end slot must be greater than start slot"
    );

    let observed = Arc::new(Mutex::new(BTreeMap::new()));
    let entry_hashes = Arc::new(Mutex::new(BTreeSet::new()));
    firehose::firehose(
        1,
        false,
        false,
        None,
        start_slot..end_slot,
        None::<firehose::OnBlockFn>,
        Some(transaction_handler(Arc::clone(&observed))),
        Some(entry_handler(Arc::clone(&entry_hashes))),
        None::<firehose::OnRewardFn>,
        None::<firehose::OnErrorFn>,
        None::<firehose::OnStatsTrackingFn>,
        None,
    )
    .await
    .unwrap_or_else(|(error, at)| panic!("firehose failed at slot {at}: {error}"));

    let observed = observed.lock().unwrap();
    let entry_hashes = entry_hashes.lock().unwrap();
    println!("unmatched_recent_blockhash,count,first_slot,last_slot");
    for (recent_blockhash, item) in observed.iter() {
        if entry_hashes.contains(recent_blockhash) {
            continue;
        }
        println!(
            "{},{},{},{}",
            recent_blockhash, item.count, item.first_slot, item.last_slot
        );
    }
}
