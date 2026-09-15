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
    target: Option<Address>,
    dump_matches: bool,
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
        if self
            .target
            .is_some_and(|target| !keys.iter().any(|key| *key == target))
        {
            return;
        }

        if self.dump_matches {
            self.matching_transactions += 1;
            let replayed_updates = tx
                .iter_account_updates()
                .filter(|(update, _)| self.target.is_none_or(|target| update.pubkey == target))
                .map(|(update, data)| {
                    format!(
                        "{}:lamports={} owner={} executable={} rent_epoch={} data_len={}",
                        update.pubkey,
                        update.lamports,
                        update.owner,
                        update.executable,
                        update.rent_epoch,
                        data.len(),
                    )
                })
                .collect::<Vec<_>>();
            println!(
                "slot={slot} tx={tx_index} signature={} status={:?} fee={}\nmessage={:?}\nsource_pre_balances={:?}\nsource_post_balances={:?}\nreplayed_updates={replayed_updates:?}",
                tx.signatures
                    .first()
                    .map(ToString::to_string)
                    .unwrap_or_else(|| "<none>".to_string()),
                tx.status,
                tx.fee,
                tx.message,
                tx.pre_balances.as_slice(),
                tx.post_balances.as_slice(),
            );
            return;
        }

        let mut replayed_posts = Vec::<(Address, u64)>::new();
        for (update, _) in tx.iter_account_updates() {
            if self.target.is_some_and(|target| update.pubkey != target) {
                continue;
            }
            if let Some((_, lamports)) = replayed_posts
                .iter_mut()
                .find(|(pubkey, _)| *pubkey == update.pubkey)
            {
                *lamports = update.lamports;
            } else {
                replayed_posts.push((update.pubkey, update.lamports));
            }
        }
        if replayed_posts.is_empty() {
            if let Some(target) = self.target {
                let account_index = keys
                    .iter()
                    .position(|key| *key == target)
                    .expect("target presence was checked above");
                let source_pre = tx.pre_balances.get(account_index).copied();
                let source_post = tx.post_balances.get(account_index).copied();
                if source_pre
                    .zip(source_post)
                    .is_some_and(|(pre, post)| pre != post)
                {
                    self.first_mismatch = Some(format!(
                        "slot={slot} tx={tx_index} signature={} account={target}: source changes balance without a replay update; status={:?} fee={} source_pre={source_pre:?} source_post={source_post:?}\nmessage={:?}\nsource_pre_balances={:?}\nsource_post_balances={:?}",
                        tx.signatures
                            .first()
                            .map(ToString::to_string)
                            .unwrap_or_else(|| "<none>".to_string()),
                        tx.status,
                        tx.fee,
                        tx.message,
                        tx.pre_balances.as_slice(),
                        tx.post_balances.as_slice(),
                    ));
                }
            }
            return;
        }
        self.matching_transactions += 1;
        for &(pubkey, replayed) in &replayed_posts {
            let Some(account_index) = keys.iter().position(|key| *key == pubkey) else {
                self.first_mismatch = Some(format!(
                    "slot={slot} tx={tx_index}: replayed update {pubkey} is absent from message keys"
                ));
                return;
            };
            let Some(source_post) = tx.post_balances.get(account_index).copied() else {
                self.first_mismatch = Some(format!(
                    "slot={slot} tx={tx_index}: account {pubkey} index {account_index} has no source post balance"
                ));
                return;
            };
            if replayed != source_post {
                self.first_mismatch = Some(format!(
                    "slot={slot} tx={tx_index} signature={} account={pubkey} status={:?} fee={} source_pre={} source_post={source_post} replayed_post={replayed}\nmessage={:?}\nsource_pre_balances={:?}\nsource_post_balances={:?}\nreplayed_post_updates={replayed_posts:?}",
                    tx.signatures
                        .first()
                        .map(ToString::to_string)
                        .unwrap_or_else(|| "<none>".to_string()),
                    tx.status,
                    tx.fee,
                    tx.pre_balances
                        .get(account_index)
                        .copied()
                        .unwrap_or_default(),
                    tx.message,
                    tx.pre_balances.as_slice(),
                    tx.post_balances.as_slice(),
                ));
                return;
            }
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
        "usage: find_archive_balance_divergence <partial.jet> <address|all> <start-slot> <slot-count> [--dump]"
    );
    std::process::exit(2);
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 5 && !(args.len() == 6 && args[5] == "--dump") {
        usage();
    }
    let path = Path::new(&args[1]);
    let target = if args[2] == "all" {
        None
    } else {
        Some(Address::from_str(&args[2]).unwrap_or_else(|_| usage()))
    };
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
        dump_matches: args.len() == 6,
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
        None if finder.dump_matches => println!(
            "dumped {} matching transaction(s)",
            finder.matching_transactions
        ),
        None => println!(
            "no replay/source post-balance mismatch in {} matching transaction(s)",
            finder.matching_transactions
        ),
    }
}
