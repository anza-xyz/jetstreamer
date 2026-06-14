//! Read-back integrity check for a horizon archive (`.jet`) file.
//!
//! Opens the archive (validating magic, header, footer, bucket index, and
//! prime-table compatibility), then streams every slot frame through the
//! reader. Streaming forces each bucket's xxh64 checksum to be verified on
//! load and — with `verify_chain` enabled — checks the blockhash chain
//! links slot to slot. Every transaction and account-update blob is fully
//! decoded (the diff decoder reconstructs account data), so a clean run is
//! end-to-end proof the file round-trips.
//!
//! Tallies block/skipped/transaction/orphan-update/epoch counts so they can
//! be compared against the writer's `horizon archive complete: …` line.
//!
//! Usage:
//! `cargo run --release -p jetstreamer-horizon --example verify_archive -- <path> [max_slots]`

use std::io::BufReader;
use std::time::Instant;

use jetstreamer_horizon::archive::{
    ArchiveReader, BlockNotification, EntryRecord, EpochMeta, SlotVisitor,
};
use jetstreamer_horizon::transactions::Transaction;

struct Tally {
    epochs: u64,
    blocks: u64,
    skipped: u64,
    transactions: u64,
    tx_account_updates: u64,
    orphan_updates: u64,
    entries: u64,
    rewards: u64,
    /// Running totals to spot-check internal consistency.
    block_meta_tx_count: u64,
    last_slot: Option<u64>,
    /// Live-throughput tracking.
    start: Instant,
    last_report: Instant,
    last_report_txs: u64,
    slots_seen: u64,
}

impl Default for Tally {
    fn default() -> Self {
        let now = Instant::now();
        Self {
            epochs: 0,
            blocks: 0,
            skipped: 0,
            transactions: 0,
            tx_account_updates: 0,
            orphan_updates: 0,
            entries: 0,
            rewards: 0,
            block_meta_tx_count: 0,
            last_slot: None,
            start: now,
            last_report: now,
            last_report_txs: 0,
            slots_seen: 0,
        }
    }
}

impl SlotVisitor for Tally {
    fn on_epoch(&mut self, meta: &EpochMeta) {
        self.epochs += 1;
        eprintln!(
            "[verify] epoch notification: epoch={} start_slot={} slot_count={} first_block_slot={} updates={}",
            meta.epoch,
            meta.start_slot,
            meta.slot_count,
            meta.first_block_slot,
            meta.updates.len(),
        );
        self.orphan_updates += meta.updates.len() as u64;
    }

    fn on_transaction(&mut self, _slot: u64, _tx_index: u32, tx: &Transaction) {
        self.transactions += 1;
        self.tx_account_updates += tx.account_updates().len() as u64;
    }

    fn on_block(&mut self, notification: &BlockNotification, entries: &[EntryRecord]) {
        // Monotonic-slot sanity (the frame headers are authoritative).
        let slot = notification.slot();
        if let Some(last) = self.last_slot {
            assert!(
                slot > last,
                "slot frames not monotonic: {slot} after {last}"
            );
        }
        self.last_slot = Some(slot);

        match notification {
            BlockNotification::Skipped(_) => self.skipped += 1,
            BlockNotification::Block(meta) => {
                self.blocks += 1;
                self.entries += entries.len() as u64;
                self.rewards += meta.rewards.len() as u64;
                self.orphan_updates += meta.pre_updates.len() as u64;
                self.orphan_updates += meta.post_updates.len() as u64;
                self.block_meta_tx_count += meta.executed_transaction_count;
            }
        }

        self.slots_seen += 1;
        if self.last_report.elapsed().as_secs_f64() >= 3.0 {
            let now = Instant::now();
            let window = now.duration_since(self.last_report).as_secs_f64();
            let inst_tps = (self.transactions - self.last_report_txs) as f64 / window;
            let avg_tps = self.transactions as f64 / self.start.elapsed().as_secs_f64().max(0.001);
            eprintln!(
                "[verify] slot {} | {} slots, {} txs | decode tps: {:.0} (inst) / {:.0} (avg)",
                slot, self.slots_seen, self.transactions, inst_tps, avg_tps,
            );
            self.last_report = now;
            self.last_report_txs = self.transactions;
        }
    }
}

fn main() {
    let mut args = std::env::args().skip(1);
    let path = args
        .next()
        .expect("usage: verify_archive <path> [max_slots]");
    let max_slots: u64 = args
        .next()
        .map(|v| v.parse().expect("max_slots must be a number"))
        .unwrap_or(u64::MAX);
    // Optional start slot — read from a mid-epoch position to measure
    // steady-state decode throughput away from the dense boundary.
    let start_slot_arg: Option<u64> = args
        .next()
        .map(|v| v.parse().expect("start_slot must be a number"));

    let file = std::fs::File::open(&path).unwrap_or_else(|e| panic!("open {path}: {e}"));
    let file_len = file.metadata().map(|m| m.len()).unwrap_or(0);
    // Large buffer: streaming is forward and sequential, one bucket at a time.
    let source = BufReader::with_capacity(16 << 20, file);

    let start = Instant::now();
    let mut reader = ArchiveReader::open(source).unwrap_or_else(|e| panic!("open archive: {e}"));
    reader.verify_chain = true;

    let header = reader.header();
    let slot_start = header.slot_start;
    let slot_count = header.slot_count;
    let epoch = header.epoch;
    let buckets = reader.bucket_count();
    println!(
        "opened {path} ({file_len} bytes): epoch={epoch} slot_start={slot_start} slot_count={slot_count} buckets={buckets}"
    );

    // Stream the whole archive (or up to max_slots). `read_slots` starts at
    // the first stored slot >= start_slot and walks forward; one call with a
    // large cap consumes the entire file.
    let read_from = start_slot_arg.unwrap_or(slot_start);
    let to_read = slot_count.min(max_slots);
    let mut tally = Tally::default();
    let visited = reader
        .read_slots(read_from, to_read, &mut tally)
        .unwrap_or_else(|e| panic!("read_slots failed: {e}"));

    let elapsed = start.elapsed().as_secs_f64();
    let mib = file_len as f64 / (1024.0 * 1024.0);
    println!("\n=== verify_archive summary ===");
    println!(
        "read {visited} slot frames in {elapsed:.1}s ({:.0} MiB/s over file)",
        mib / elapsed.max(0.001)
    );
    println!("  epochs:              {}", tally.epochs);
    println!("  blocks:              {}", tally.blocks);
    println!("  skipped slots:       {}", tally.skipped);
    println!("  slots (blk+skipped): {}", tally.blocks + tally.skipped);
    println!("  transactions:        {}", tally.transactions);
    println!("  tx account updates:  {}", tally.tx_account_updates);
    println!("  orphan updates:      {}", tally.orphan_updates);
    println!("  entries:             {}", tally.entries);
    println!("  rewards:             {}", tally.rewards);
    println!(
        "  block-meta tx total: {} (vs decoded {})",
        tally.block_meta_tx_count, tally.transactions
    );

    // Internal consistency: the sum of each block's declared
    // executed_transaction_count must equal the transactions we decoded.
    if max_slots == u64::MAX {
        assert_eq!(
            tally.block_meta_tx_count, tally.transactions,
            "block metadata tx counts disagree with decoded transactions"
        );
        assert_eq!(
            tally.blocks + tally.skipped,
            visited,
            "block+skipped should equal slot frames visited"
        );
        println!(
            "\nRESULT: OK — archive opened, all buckets checksum-verified, chain intact, \
                  every slot/transaction/update decoded, internal tallies consistent."
        );
    } else {
        println!("\nRESULT: OK (partial read of {visited} slots).");
    }
}
