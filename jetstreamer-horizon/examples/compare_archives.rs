//! Compare the canonical block/PoH metadata stored by two horizon archives.
//!
//! This deliberately ignores account-update payloads. It is intended for
//! diagnosing replay/source-selection differences over a small slot range.

use std::{
    collections::BTreeMap,
    fs::File,
    io::{BufReader, Read, Seek, SeekFrom},
    path::Path,
};

use jetstreamer_horizon::archive::{
    ArchiveReader, BlockNotification, BucketDecoder, BucketHeader, Consumption, EntryRecord,
    MAX_FILE_HEADER_BYTES, SlotKind, SlotVisitor, parse_file_header,
};
use lencode::Decode;
use solana_hash::Hash;

#[derive(Debug, Clone, PartialEq, Eq)]
struct SlotSummary {
    kind: SlotKind,
    parent_slot: Option<u64>,
    parent_blockhash: Option<Hash>,
    blockhash: Option<Hash>,
    executed_transactions: u64,
    entries: Vec<EntryRecord>,
}

#[derive(Default)]
struct Collector {
    current_slot: Option<u64>,
    current_kind: Option<SlotKind>,
    slots: BTreeMap<u64, SlotSummary>,
}

impl SlotVisitor for Collector {
    fn on_slot_start(&mut self, slot: u64, kind: SlotKind) {
        self.current_slot = Some(slot);
        self.current_kind = Some(kind);
    }

    fn on_block(&mut self, notification: &BlockNotification, entries: &[EntryRecord]) {
        let slot = self.current_slot.expect("slot start precedes block");
        assert_eq!(notification.slot(), slot);
        let kind = self.current_kind.expect("slot kind precedes block");
        let summary = match notification {
            BlockNotification::Skipped(_) => SlotSummary {
                kind,
                parent_slot: None,
                parent_blockhash: None,
                blockhash: None,
                executed_transactions: 0,
                entries: Vec::new(),
            },
            BlockNotification::Block(meta) => SlotSummary {
                kind,
                parent_slot: Some(meta.parent_slot),
                parent_blockhash: Some(meta.parent_blockhash),
                blockhash: Some(meta.blockhash),
                executed_transactions: meta.executed_transaction_count,
                entries: entries.to_vec(),
            },
        };
        assert!(self.slots.insert(slot, summary).is_none());
    }

    fn consumption(&self) -> Consumption {
        Consumption::all()
            .without_account_update_data()
            .without_block_account_update_arenas()
    }
}

fn collect(path: &Path, start_slot: u64, slot_count: u64) -> Collector {
    let file = File::open(path).unwrap_or_else(|error| panic!("open {}: {error}", path.display()));
    match ArchiveReader::open(BufReader::new(file)) {
        Ok(mut reader) => {
            let mut collector = Collector::default();
            reader
                .read_slots(start_slot, slot_count, &mut collector)
                .unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
            collector
        }
        Err(error) => {
            eprintln!(
                "{} has no readable footer ({error}); scanning complete bucket frames",
                path.display()
            );
            collect_unfinished(path, start_slot, slot_count)
        }
    }
}

fn collect_unfinished(path: &Path, start_slot: u64, slot_count: u64) -> Collector {
    let mut source = BufReader::new(
        File::open(path).unwrap_or_else(|error| panic!("open {}: {error}", path.display())),
    );
    let file_len = source
        .seek(SeekFrom::End(0))
        .unwrap_or_else(|error| panic!("measure {}: {error}", path.display()));
    source
        .seek(SeekFrom::Start(0))
        .unwrap_or_else(|error| panic!("seek {}: {error}", path.display()));
    let prefix_len = usize::try_from(file_len.min(MAX_FILE_HEADER_BYTES + 32)).unwrap();
    let mut prefix = vec![0; prefix_len];
    source
        .read_exact(&mut prefix)
        .unwrap_or_else(|error| panic!("read header {}: {error}", path.display()));
    let (header, mut offset) = parse_file_header(&prefix)
        .unwrap_or_else(|error| panic!("decode header {}: {error}", path.display()));
    let requested_end = start_slot.saturating_add(slot_count);
    let mut decoder = BucketDecoder::for_file_header(&header)
        .unwrap_or_else(|error| panic!("configure decoder {}: {error}", path.display()));
    decoder.materialize_account_data = false;
    decoder.materialize_block_account_update_arenas = false;
    let mut collector = Collector::default();

    while (offset as u64) < file_len {
        source
            .seek(SeekFrom::Start(offset as u64))
            .unwrap_or_else(|error| panic!("seek bucket {}: {error}", path.display()));
        let bucket = BucketHeader::decode_ext(&mut source, None).unwrap_or_else(|error| {
            panic!("decode bucket at {offset} in {}: {error}", path.display())
        });
        let payload_offset = source
            .stream_position()
            .unwrap_or_else(|error| panic!("locate bucket {}: {error}", path.display()));
        let bucket_end = payload_offset
            .checked_add(bucket.stored_len)
            .unwrap_or_else(|| panic!("bucket end overflow in {}", path.display()));
        if bucket_end > file_len {
            eprintln!(
                "stopped at incomplete bucket {} ({} bytes missing)",
                bucket.first_slot,
                bucket_end - file_len
            );
            break;
        }

        let slot_end = bucket.first_slot + u64::from(bucket.slot_count);
        if bucket.first_slot < requested_end && slot_end > start_slot {
            let raw_len = usize::try_from(bucket_end - offset as u64)
                .unwrap_or_else(|_| panic!("bucket too large in {}", path.display()));
            let mut raw = vec![0; raw_len];
            source
                .seek(SeekFrom::Start(offset as u64))
                .and_then(|_| source.read_exact(&mut raw))
                .unwrap_or_else(|error| panic!("read bucket {}: {error}", path.display()));
            decoder
                .decode_bucket(&raw, start_slot, slot_count, &mut collector)
                .unwrap_or_else(|error| panic!("decode bucket {}: {error}", path.display()));
        }
        if slot_end >= requested_end {
            break;
        }
        offset = usize::try_from(bucket_end)
            .unwrap_or_else(|_| panic!("bucket offset does not fit usize in {}", path.display()));
    }
    collector
}

fn usage() -> ! {
    eprintln!("usage: compare_archives <left.jet> <right.jet> <start-slot> <slot-count>");
    std::process::exit(2);
}

fn describe(summary: Option<&SlotSummary>) -> String {
    match summary {
        None => "absent".to_string(),
        Some(summary) => format!(
            "kind={:?} parent_slot={:?} parent_blockhash={:?} blockhash={:?} txs={} entries={}",
            summary.kind,
            summary.parent_slot,
            summary.parent_blockhash,
            summary.blockhash,
            summary.executed_transactions,
            summary.entries.len(),
        ),
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 5 {
        usage();
    }
    let start_slot = args[3].parse().unwrap_or_else(|_| usage());
    let slot_count = args[4].parse().unwrap_or_else(|_| usage());
    let left = collect(Path::new(&args[1]), start_slot, slot_count);
    let right = collect(Path::new(&args[2]), start_slot, slot_count);

    let mut differences = 0usize;
    for slot in start_slot..start_slot.saturating_add(slot_count) {
        let lhs = left.slots.get(&slot);
        let rhs = right.slots.get(&slot);
        if lhs != rhs {
            differences += 1;
            println!(
                "slot {slot} differs:\n  left:  {}\n  right: {}",
                describe(lhs),
                describe(rhs),
            );
        }
    }
    println!(
        "compared {} slot(s) from {}; {} difference(s)",
        slot_count, start_slot, differences
    );
    if differences != 0 {
        std::process::exit(1);
    }
}
