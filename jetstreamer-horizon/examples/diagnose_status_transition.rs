//! Locate abrupt transaction-status changes in a retained Horizon archive.
//!
//! This is a narrow historical-replay diagnostic. It reports slots with a
//! configurable minimum number of `InstructionError::Custom(2)` outcomes;
//! for the v1.2 vote program that code is `SlotHashMismatch`.

use std::collections::BTreeMap;
use std::fs::File;
use std::io::BufReader;

use jetstreamer_horizon::archive::{ArchiveReader, BlockNotification, EntryRecord, SlotVisitor};
use jetstreamer_horizon::transactions::Transaction;

#[derive(Default)]
struct Counts {
    transactions: u64,
    ok: u64,
    errors: u64,
    custom_two: u64,
}

#[derive(Default)]
struct StatusVisitor {
    slots: BTreeMap<u64, Counts>,
    details: bool,
}

impl SlotVisitor for StatusVisitor {
    fn on_transaction(&mut self, slot: u64, tx_index: u32, transaction: &Transaction) {
        let counts = self.slots.entry(slot).or_default();
        counts.transactions += 1;
        if transaction.status.is_ok() {
            counts.ok += 1;
        } else {
            counts.errors += 1;
            if transaction.status.custom_error_code == 2 {
                counts.custom_two += 1;
            }
            if self.details {
                println!(
                    "error slot={slot} tx={tx_index} signature={:?} error_code={} instruction_index={} custom_error_code={}",
                    transaction.signatures.first(),
                    transaction.status.error_code,
                    transaction.status.instruction_index,
                    transaction.status.custom_error_code,
                );
            }
        }
    }

    fn on_block(&mut self, notification: &BlockNotification, _entries: &[EntryRecord]) {
        self.slots.entry(notification.slot()).or_default();
    }
}

fn main() {
    let mut arguments = std::env::args().skip(1);
    let path = arguments.next().expect(
        "usage: diagnose_status_transition ARCHIVE [START_SLOT] [SLOT_COUNT] [MIN_CUSTOM_TWO] [DETAILS]",
    );
    let start = arguments
        .next()
        .map(|value| value.parse::<u64>().expect("START_SLOT must be a u64"));
    let count = arguments
        .next()
        .map(|value| value.parse::<u64>().expect("SLOT_COUNT must be a u64"))
        .unwrap_or(u64::MAX);
    let minimum = arguments
        .next()
        .map(|value| value.parse::<u64>().expect("MIN_CUSTOM_TWO must be a u64"))
        .unwrap_or(10);
    let details = arguments
        .next()
        .map(|value| {
            value
                .parse::<bool>()
                .expect("DETAILS must be true or false")
        })
        .unwrap_or(false);
    assert!(arguments.next().is_none(), "too many arguments");

    let file = File::open(&path).expect("open archive");
    let mut reader = ArchiveReader::open(BufReader::new(file)).expect("open archive reader");
    let first = start.unwrap_or(reader.header().slot_start);
    let mut visitor = StatusVisitor {
        details,
        ..StatusVisitor::default()
    };
    let visited = reader
        .read_slots(first, count, &mut visitor)
        .expect("decode archive slots");

    let mut qualifying = 0u64;
    for (slot, counts) in &visitor.slots {
        if counts.custom_two >= minimum {
            qualifying += 1;
            println!(
                "slot={slot} tx={} ok={} errors={} custom2={}",
                counts.transactions, counts.ok, counts.errors, counts.custom_two
            );
        }
    }
    eprintln!(
        "visited={visited} slots_with_transactions={} qualifying_slots={qualifying}",
        visitor
            .slots
            .values()
            .filter(|counts| counts.transactions != 0)
            .count(),
    );
}
