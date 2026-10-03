//! Find the first transaction-status divergence between a Horizon archive and
//! its Old Faithful source stream.
//!
//! This accepts unfinished archives: only complete bucket frames are decoded.
//! Per-slot fingerprints include every transaction index, signature, and full
//! compact status. Failed transaction details are retained for useful output,
//! while successful transactions require only two 64-bit aggregates.
//!
//! This is a diagnostic comparison, not a consensus oracle. In early Old
//! Faithful data, transaction-status metadata can be associated incorrectly.

use std::{
    collections::BTreeMap,
    fs::File,
    future::Future,
    io::{BufReader, Read, Seek, SeekFrom},
    path::Path,
    pin::Pin,
    sync::Arc,
};

use dashmap::DashMap;
use jetstreamer_firehose::{
    BlockData, SharedError, TransactionData,
    firehose::{self, Handler},
};
use jetstreamer_horizon::{
    archive::{
        BucketDecoder, BucketHeader, Consumption, MAX_FILE_HEADER_BYTES, SlotKind, SlotVisitor,
        parse_file_header,
    },
    convert::status_from_result,
    transactions::{Transaction, TransactionStatus},
};
use lencode::Decode;
use xxhash_rust::xxh64::Xxh64;

const HASH_SEED_A: u64 = 0x6a09_e667_f3bc_c908;
const HASH_SEED_B: u64 = 0xbb67_ae85_84ca_a73b;

#[derive(Clone, Debug, PartialEq, Eq)]
struct FailedTransaction {
    index: u64,
    signature: String,
    status: String,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct SlotStatus {
    transaction_count: u64,
    unavailable_source_statuses: u64,
    xor_a: u64,
    sum_b: u64,
    failures: Vec<FailedTransaction>,
}

impl SlotStatus {
    fn record(
        &mut self,
        index: u64,
        signature: &[u8; 64],
        signature_text: String,
        status: &TransactionStatus,
        source_status_available: bool,
    ) {
        self.transaction_count += 1;
        if !source_status_available {
            self.unavailable_source_statuses += 1;
        }

        let hash = |seed| {
            let mut hasher = Xxh64::new(seed);
            hasher.update(&index.to_le_bytes());
            hasher.update(signature);
            hasher.update(&status.error_code.to_le_bytes());
            hasher.update(&[status.instruction_index]);
            hasher.update(&status.custom_error_code.to_le_bytes());
            hasher.update(status.error_message.as_slice());
            hasher.digest()
        };
        self.xor_a ^= hash(HASH_SEED_A);
        self.sum_b = self.sum_b.wrapping_add(hash(HASH_SEED_B));

        if !status.is_ok() {
            self.failures.push(FailedTransaction {
                index,
                signature: signature_text,
                status: format!(
                    "error_code={:#06x} instruction_index={} custom_error_code={} message={:?}",
                    status.error_code,
                    status.instruction_index,
                    status.custom_error_code,
                    status.error_message.as_slice(),
                ),
            });
        }
    }
}

#[derive(Default)]
struct ArchiveCollector {
    slots: BTreeMap<u64, SlotStatus>,
}

impl SlotVisitor for ArchiveCollector {
    fn on_slot_start(&mut self, _slot: u64, _kind: SlotKind) {}

    fn on_transaction(&mut self, slot: u64, tx_index: u32, tx: &Transaction) {
        let Some(signature) = tx.signatures.first() else {
            panic!("archive transaction {slot}:{tx_index} has no signature");
        };
        self.slots.entry(slot).or_default().record(
            u64::from(tx_index),
            signature.as_array(),
            signature.to_string(),
            &tx.status,
            true,
        );
    }

    fn consumption(&self) -> Consumption {
        Consumption::all()
            .without_account_update_data()
            .without_block_account_update_arenas()
    }
}

fn collect_unfinished_archive(
    path: &Path,
    start_slot: u64,
    end_slot: u64,
) -> BTreeMap<u64, SlotStatus> {
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
    let (header, mut offset) = parse_file_header(&prefix)
        .unwrap_or_else(|error| panic!("decode header {}: {error}", path.display()));
    let mut decoder = BucketDecoder::for_file_header(&header)
        .unwrap_or_else(|error| panic!("configure decoder {}: {error}", path.display()));
    decoder.materialize_account_data = false;
    decoder.materialize_block_account_update_arenas = false;
    let mut collector = ArchiveCollector::default();

    while (offset as u64) < file_len {
        source.seek(SeekFrom::Start(offset as u64)).unwrap();
        let bucket = BucketHeader::decode_ext(&mut source, None).unwrap_or_else(|error| {
            panic!("decode bucket at {offset} in {}: {error}", path.display())
        });
        let payload_offset = source.stream_position().unwrap();
        let bucket_end = payload_offset
            .checked_add(bucket.stored_len)
            .expect("bucket end overflow");
        if bucket_end > file_len {
            eprintln!(
                "stopped before incomplete bucket beginning at slot {} ({} bytes missing)",
                bucket.first_slot,
                bucket_end - file_len,
            );
            break;
        }

        let bucket_slot_end = bucket.first_slot + u64::from(bucket.slot_count);
        if bucket.first_slot < end_slot && bucket_slot_end > start_slot {
            let raw_len = usize::try_from(bucket_end - offset as u64).expect("bucket too large");
            let mut raw = vec![0; raw_len];
            source.seek(SeekFrom::Start(offset as u64)).unwrap();
            source.read_exact(&mut raw).unwrap();
            decoder
                .decode_bucket(
                    &raw,
                    start_slot,
                    end_slot.saturating_sub(start_slot),
                    &mut collector,
                )
                .unwrap_or_else(|error| panic!("decode bucket {}: {error}", bucket.first_slot));
        }
        if bucket_slot_end >= end_slot {
            break;
        }
        offset = usize::try_from(bucket_end).expect("bucket offset does not fit usize");
    }
    collector.slots
}

type HandlerFuture = Pin<Box<dyn Future<Output = Result<(), SharedError>> + Send + 'static>>;

fn source_transaction_handler(
    slots: Arc<DashMap<u64, SlotStatus>>,
) -> impl Handler<TransactionData> {
    move |_thread_id, tx| {
        let slots = Arc::clone(&slots);
        Box::pin(async move {
            let status = status_from_result(&tx.transaction_status_meta.status);
            slots.entry(tx.slot).or_default().record(
                u64::try_from(tx.transaction_slot_index).expect("transaction index exceeds u64"),
                tx.signature.as_array(),
                tx.signature.to_string(),
                &status,
                tx.status_meta_available,
            );
            Ok(())
        }) as HandlerFuture
    }
}

fn source_block_handler(slots: Arc<DashMap<u64, SlotStatus>>) -> impl Handler<BlockData> {
    move |_thread_id, block| {
        let slots = Arc::clone(&slots);
        Box::pin(async move {
            // Preserve an explicit empty summary for both empty blocks and
            // skipped slots. Transaction status comparison intentionally does
            // not distinguish those two cases.
            slots.entry(block.slot()).or_default();
            Ok(())
        }) as HandlerFuture
    }
}

fn usage() -> ! {
    eprintln!(
        "usage: find_status_divergence <partial.jet> <start-slot> <end-slot-exclusive> [threads]"
    );
    std::process::exit(2);
}

#[tokio::main]
async fn main() {
    let args = std::env::args().collect::<Vec<_>>();
    if !(4..=5).contains(&args.len()) {
        usage();
    }
    let path = Path::new(&args[1]);
    let start_slot = args[2].parse::<u64>().unwrap_or_else(|_| usage());
    let end_slot = args[3].parse::<u64>().unwrap_or_else(|_| usage());
    if start_slot >= end_slot {
        usage();
    }
    let threads = args
        .get(4)
        .map(|value| value.parse::<u64>().unwrap_or_else(|_| usage()))
        .unwrap_or(16)
        .max(1);

    eprintln!("fingerprinting archive statuses...");
    let archive = collect_unfinished_archive(path, start_slot, end_slot);
    eprintln!("fetching source statuses with {threads} threads...");
    let source = Arc::new(DashMap::<u64, SlotStatus>::new());
    firehose::firehose(
        threads,
        false,
        false,
        None,
        start_slot..end_slot,
        Some(source_block_handler(Arc::clone(&source))),
        Some(source_transaction_handler(Arc::clone(&source))),
        None::<firehose::OnEntryFn>,
        None::<firehose::OnRewardFn>,
        None::<firehose::OnErrorFn>,
        None::<firehose::OnStatsTrackingFn>,
        None,
    )
    .await
    .unwrap_or_else(|(error, slot)| panic!("firehose failed at slot {slot}: {error}"));

    for slot in start_slot..end_slot {
        let archive_status = archive.get(&slot).cloned().unwrap_or_default();
        let source_status = source
            .get(&slot)
            .map(|value| value.clone())
            .unwrap_or_default();
        if archive_status != source_status {
            println!("first status divergence at slot {slot}");
            println!("archive: {archive_status:#?}");
            println!("source:  {source_status:#?}");
            std::process::exit(1);
        }
    }
    println!(
        "all transaction statuses match for {} slots ({start_slot}..{end_slot})",
        end_slot - start_slot,
    );
}
