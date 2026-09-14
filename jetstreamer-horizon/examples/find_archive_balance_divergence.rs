//! Find the first transaction where replayed account state disagrees with
//! source pre/post balance metadata retained in an unfinished archive.
//!
//! This is a diagnostic comparison, not a consensus oracle. In early Old
//! Faithful data, transaction-status metadata can be associated incorrectly.

use std::{
    fs::File,
    io::{BufReader, Read, Seek, SeekFrom},
    path::Path,
    str::FromStr,
};

use jetstreamer_horizon::{
    archive::{
        BucketDecoder, BucketHeader, Consumption, MAX_FILE_HEADER_BYTES, SlotKind, SlotVisitor,
        parse_file_header,
    },
    transactions::{Transaction, VersionedMessage},
};
use lencode::Decode;
use solana_address::Address;

struct Finder {
    target: Address,
    first_mismatch: Option<String>,
    matching_transactions: u64,
}

impl SlotVisitor for Finder {
    fn on_slot_start(&mut self, _slot: u64, _kind: SlotKind) {}

    fn on_transaction(&mut self, slot: u64, tx_index: u32, tx: &Transaction) {
        if self.first_mismatch.is_some() {
            return;
        }
        let keys = match &tx.message {
            VersionedMessage::Legacy(message) => message.account_keys.as_slice(),
            VersionedMessage::V0(message) => message.account_keys.as_slice(),
        };
        let Some(account_index) = keys.iter().position(|key| *key == self.target) else {
            return;
        };
        self.matching_transactions += 1;
        let Some(source_post) = tx.post_balances.get(account_index).copied() else {
            self.first_mismatch = Some(format!(
                "slot={slot} tx={tx_index}: target account index {account_index} has no post balance"
            ));
            return;
        };
        let replayed = tx
            .iter_account_updates()
            .filter(|(update, _)| update.pubkey == self.target)
            .map(|(update, _)| update.lamports)
            .last();
        let Some(replayed) = replayed else {
            return;
        };
        if replayed != source_post {
            self.first_mismatch = Some(format!(
                "slot={slot} tx={tx_index} signature={} status={:?} source_pre={} source_post={} replayed_post={replayed}",
                tx.signatures
                    .first()
                    .map(ToString::to_string)
                    .unwrap_or_else(|| "<none>".to_string()),
                tx.status,
                tx.pre_balances
                    .get(account_index)
                    .copied()
                    .unwrap_or_default(),
                source_post,
            ));
        }
    }

    fn consumption(&self) -> Consumption {
        Consumption::all()
            .without_account_update_data()
            .without_block_account_update_arenas()
    }
}

fn usage() -> ! {
    eprintln!(
        "usage: find_archive_balance_divergence <partial.jet> <address> <start-slot> <slot-count>"
    );
    std::process::exit(2);
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 5 {
        usage();
    }
    let path = Path::new(&args[1]);
    let target = Address::from_str(&args[2]).unwrap_or_else(|_| usage());
    let start_slot = args[3].parse::<u64>().unwrap_or_else(|_| usage());
    let slot_count = args[4].parse::<u64>().unwrap_or_else(|_| usage());
    let requested_end = start_slot.saturating_add(slot_count);

    let mut source = BufReader::new(
        File::open(path).unwrap_or_else(|error| panic!("open {}: {error}", path.display())),
    );
    let file_len = source
        .seek(SeekFrom::End(0))
        .unwrap_or_else(|error| panic!("measure {}: {error}", path.display()));
    source.seek(SeekFrom::Start(0)).unwrap();
    let prefix_len = usize::try_from(file_len.min(MAX_FILE_HEADER_BYTES + 32)).unwrap();
    let mut prefix = vec![0; prefix_len];
    source.read_exact(&mut prefix).unwrap();
    let (header, mut offset) = parse_file_header(&prefix).unwrap();
    let mut decoder = BucketDecoder::for_file_header(&header).unwrap();
    decoder.materialize_account_data = false;
    decoder.materialize_block_account_update_arenas = false;
    let mut finder = Finder {
        target,
        first_mismatch: None,
        matching_transactions: 0,
    };

    while (offset as u64) < file_len && finder.first_mismatch.is_none() {
        source.seek(SeekFrom::Start(offset as u64)).unwrap();
        let bucket = BucketHeader::decode_ext(&mut source, None).unwrap_or_else(|error| {
            panic!("decode bucket at {offset} in {}: {error}", path.display())
        });
        let payload_offset = source.stream_position().unwrap();
        let bucket_end = payload_offset.checked_add(bucket.stored_len).unwrap();
        if bucket_end > file_len {
            break;
        }
        let slot_end = bucket.first_slot + u64::from(bucket.slot_count);
        if bucket.first_slot < requested_end && slot_end > start_slot {
            let raw_len = usize::try_from(bucket_end - offset as u64).unwrap();
            let mut raw = vec![0; raw_len];
            source.seek(SeekFrom::Start(offset as u64)).unwrap();
            source.read_exact(&mut raw).unwrap();
            decoder
                .decode_bucket(&raw, start_slot, slot_count, &mut finder)
                .unwrap();
        }
        if slot_end >= requested_end {
            break;
        }
        offset = usize::try_from(bucket_end).unwrap();
    }

    match finder.first_mismatch {
        Some(mismatch) => {
            println!(
                "first mismatch after {} matching transaction(s): {mismatch}",
                finder.matching_transactions
            );
            std::process::exit(1);
        }
        None => println!(
            "no replay/source post-balance mismatch in {} matching transaction(s)",
            finder.matching_transactions
        ),
    }
}
