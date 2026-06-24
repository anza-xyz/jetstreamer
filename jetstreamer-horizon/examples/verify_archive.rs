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
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use jetstreamer_horizon::archive::{
    ArchiveReader, BlockNotification, EntryRecord, EpochMeta, PayloadByteStats, SlotVisitor,
};
use jetstreamer_horizon::transactions::Transaction;
use solana_hash::Hash;

struct Tally {
    epochs: u64,
    blocks: u64,
    skipped: u64,
    transactions: u64,
    tx_account_updates: u64,
    orphan_updates: u64,
    entries: u64,
    rewards: u64,
    /// Reconstructed (raw, post-diff-decode) account-data bytes across all
    /// updates — the actual account-state volume the archive represents.
    raw_account_data_bytes: u64,
    /// Running totals to spot-check internal consistency.
    block_meta_tx_count: u64,
    last_slot: Option<u64>,
    /// Blockhash of the last decoded block, for soft chain-continuity checks.
    last_blockhash: Option<Hash>,
    /// Chain discontinuities found: (block slot, expected parent, actual parent).
    chain_breaks: Vec<(u64, Hash, Hash)>,
    /// Live-throughput tracking.
    start: Instant,
    last_report: Instant,
    last_report_txs: u64,
    slots_seen: u64,
    /// Total slot frames this run will read, for ETA.
    target_slots: u64,
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
            raw_account_data_bytes: 0,
            block_meta_tx_count: 0,
            last_slot: None,
            last_blockhash: None,
            chain_breaks: Vec::new(),
            start: now,
            last_report: now,
            last_report_txs: 0,
            slots_seen: 0,
            target_slots: 0,
        }
    }
}

/// Formats an integer with thousands separators (e.g. `1234567` → `1,234,567`).
fn commas(n: u64) -> String {
    let s = n.to_string();
    let len = s.len();
    let mut out = String::with_capacity(len + len / 3);
    for (i, ch) in s.chars().enumerate() {
        if i > 0 && (len - i).is_multiple_of(3) {
            out.push(',');
        }
        out.push(ch);
    }
    out
}

/// Formats a duration in seconds as `HH:MM:SS` (or `--:--:--` when unknown).
fn fmt_hms(secs: f64) -> String {
    if !secs.is_finite() || secs < 0.0 {
        return "--:--:--".to_string();
    }
    let s = secs as u64;
    format!("{:02}:{:02}:{:02}", s / 3600, (s % 3600) / 60, s % 60)
}

/// Prints the transaction vs account-state payload-byte split + ratio.
fn report_byte_breakdown(bytes: PayloadByteStats, raw_account_data_bytes: u64) {
    let total = bytes.total().max(1);
    let pct = |b: u64| b as f64 * 100.0 / total as f64;
    println!("\n=== payload byte breakdown (uncompressed: deduped + diff-encoded, pre-zstd) ===");
    println!(
        "  transaction-field bytes: {:>16} ({:.1}%)",
        bytes.transaction_bytes,
        pct(bytes.transaction_bytes)
    );
    println!(
        "  account-update bytes:    {:>16} ({:.1}%)",
        bytes.account_update_bytes,
        pct(bytes.account_update_bytes)
    );
    println!(
        "  other (meta/entries):    {:>16} ({:.1}%)",
        bytes.other_bytes,
        pct(bytes.other_bytes)
    );
    println!("  total payload:           {:>16}", bytes.total());
    if bytes.transaction_bytes > 0 {
        println!(
            "  tx : account-state ratio (payload bytes): 1 : {:.2}",
            bytes.account_update_bytes as f64 / bytes.transaction_bytes as f64
        );
    }
    println!("  reconstructed (raw) account data: {raw_account_data_bytes} bytes");
    if bytes.account_update_bytes > 0 {
        println!(
            "  account-data dedupe+diff ratio (raw / in-archive): {:.1}x",
            raw_account_data_bytes as f64 / bytes.account_update_bytes as f64
        );
    }
}

/// Prints blockhash-chain continuity (linkage) breaks.
fn report_chain_breaks(breaks: &[(u64, Hash, Hash)], blocks: u64) {
    println!("\n=== chain continuity ===");
    if breaks.is_empty() {
        println!("  blockhash chain intact across all {} blocks", commas(blocks));
    } else {
        println!(
            "  {} chain break(s) — a block's parent_blockhash did not match the previous block:",
            commas(breaks.len() as u64)
        );
        for (slot, exp, got) in breaks.iter().take(20) {
            println!("    slot {slot}: parent={got} but previous block hash was {exp}");
        }
        if breaks.len() > 20 {
            println!("    … and {} more", breaks.len() - 20);
        }
        println!(
            "  (a break means a slot recorded as leader-skipped actually had a real block on \
             chain — its data was unavailable from old-faithful, so the archive has a genuine gap.)"
        );
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
        for (_, data) in meta.updates.iter() {
            self.raw_account_data_bytes += data.len() as u64;
        }
    }

    fn on_transaction(&mut self, _slot: u64, _tx_index: u32, tx: &Transaction) {
        self.transactions += 1;
        self.tx_account_updates += tx.account_updates().len() as u64;
        for (_, data) in tx.iter_account_updates() {
            self.raw_account_data_bytes += data.len() as u64;
        }
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
                for (_, data) in meta.pre_updates.iter().chain(meta.post_updates.iter()) {
                    self.raw_account_data_bytes += data.len() as u64;
                }
                self.block_meta_tx_count += meta.executed_transaction_count;

                // Soft blockhash-chain continuity: collect breaks instead of
                // aborting, so the whole archive is verified in one pass and
                // every discontinuity is reported (e.g. a slot recorded as
                // skipped that the chain says had a real block).
                if let Some(prev) = self.last_blockhash
                    && prev != Hash::default()
                    && meta.parent_blockhash != prev
                {
                    self.chain_breaks
                        .push((meta.slot, prev, meta.parent_blockhash));
                }
                self.last_blockhash = Some(meta.blockhash);
            }
        }

        self.slots_seen += 1;
        if self.last_report.elapsed().as_secs_f64() >= 3.0 {
            let now = Instant::now();
            let window = now.duration_since(self.last_report).as_secs_f64();
            let inst_tps = (self.transactions - self.last_report_txs) as f64 / window;
            let total_elapsed = self.start.elapsed().as_secs_f64().max(0.001);
            let avg_tps = self.transactions as f64 / total_elapsed;
            let pct = if self.target_slots > 0 {
                self.slots_seen as f64 * 100.0 / self.target_slots as f64
            } else {
                0.0
            };
            let slot_rate = self.slots_seen as f64 / total_elapsed;
            let remaining = self.target_slots.saturating_sub(self.slots_seen);
            let eta = remaining as f64 / slot_rate.max(1e-9);
            eprintln!(
                "[verify] {:.1}% | slot {} | {} slots, {} txs | decode tps: {} (inst) / {} (avg) | eta {}",
                pct,
                slot,
                commas(self.slots_seen),
                commas(self.transactions),
                commas(inst_tps.round() as u64),
                commas(avg_tps.round() as u64),
                fmt_hms(eta),
            );
            self.last_report = now;
            self.last_report_txs = self.transactions;
        }
    }
}

// ──────────────────────────── --full: PoH recompute ────────────────────────
//
// Cryptographically verifies the archive by recomputing every block's PoH hash
// from its stored `parent_blockhash`, folding `num_hashes` per entry and mixing
// in each entry's transaction signatures exactly as Solana does. Two phases:
//   1. per-block recompute (embarrassingly parallel — each block uses only its
//      own stored parent_blockhash + entries + transactions), then
//   2. a linkage pass: each block's parent_blockhash must equal the previous
//      canonical block's recomputed blockhash, anchored at the first block to a
//      seed (the previous epoch's tail blockhash, given or fetched via RPC).

use solana_message::VersionedMessage;
use solana_signature::Signature;
use solana_transaction::versioned::VersionedTransaction;

/// Recomputes a block's blockhash from `parent` by folding each entry's
/// `num_hashes` and mixing in that entry's transaction signatures, using
/// Solana's canonical [`next_hash`](solana_entry::entry::next_hash). `sigs` is
/// the block's per-transaction signature lists in slot order.
fn recompute_blockhash(parent: &Hash, entries: &[EntryRecord], sigs: &[Vec<Signature>]) -> Hash {
    let mut poh = *parent;
    let mut idx = 0usize;
    for entry in entries {
        let n = (entry.tx_count as usize).min(sigs.len().saturating_sub(idx));
        let entry_txs: Vec<VersionedTransaction> = sigs[idx..idx + n]
            .iter()
            .map(|s| VersionedTransaction {
                signatures: s.clone(),
                message: VersionedMessage::default(), // unused: only signatures are hashed
            })
            .collect();
        idx += n;
        poh = solana_entry::entry::next_hash(&poh, entry.num_hashes, &entry_txs);
    }
    poh
}

/// Recomputed identity of one block, for the linkage pass.
struct BlockHashes {
    slot: u64,
    parent_slot: u64,
    parent_blockhash: Hash,
    blockhash: Hash,
    poh_ok: bool,
}

/// Per-thread scan visitor. Always accumulates counts + per-block identities
/// (for the linkage pass); in `--full` mode it additionally buffers each
/// block's transaction signatures and recomputes its PoH hash.
#[derive(Default)]
struct ScanVisitor {
    full: bool,
    epochs: u64,
    skipped: u64,
    transactions: u64,
    tx_account_updates: u64,
    orphan_updates: u64,
    entries: u64,
    rewards: u64,
    raw_account_data_bytes: u64,
    block_meta_tx_count: u64,
    block_hashes: Vec<BlockHashes>,
    /// `--full` only: current block's per-tx signature lists, in slot order.
    cur_sigs: Vec<Vec<Signature>>,
    progress: Option<Arc<AtomicU64>>,
}

impl ScanVisitor {
    fn new(full: bool, progress: Option<Arc<AtomicU64>>) -> Self {
        Self {
            full,
            progress,
            ..Default::default()
        }
    }

    fn merge(&mut self, o: ScanVisitor) {
        self.epochs += o.epochs;
        self.skipped += o.skipped;
        self.transactions += o.transactions;
        self.tx_account_updates += o.tx_account_updates;
        self.orphan_updates += o.orphan_updates;
        self.entries += o.entries;
        self.rewards += o.rewards;
        self.raw_account_data_bytes += o.raw_account_data_bytes;
        self.block_meta_tx_count += o.block_meta_tx_count;
        self.block_hashes.extend(o.block_hashes);
    }
}

impl SlotVisitor for ScanVisitor {
    fn on_epoch(&mut self, meta: &EpochMeta) {
        self.epochs += 1;
        self.orphan_updates += meta.updates.len() as u64;
        for (_, data) in meta.updates.iter() {
            self.raw_account_data_bytes += data.len() as u64;
        }
    }

    fn on_transaction(&mut self, _slot: u64, _tx_index: u32, tx: &Transaction) {
        self.transactions += 1;
        self.tx_account_updates += tx.account_updates().len() as u64;
        for (_, data) in tx.iter_account_updates() {
            self.raw_account_data_bytes += data.len() as u64;
        }
        if self.full {
            self.cur_sigs.push(tx.signatures.as_slice().to_vec());
        }
    }

    fn on_block(&mut self, notification: &BlockNotification, entries: &[EntryRecord]) {
        match notification {
            BlockNotification::Skipped(_) => self.skipped += 1,
            BlockNotification::Block(meta) => {
                self.entries += entries.len() as u64;
                self.rewards += meta.rewards.len() as u64;
                self.orphan_updates += meta.pre_updates.len() as u64;
                self.orphan_updates += meta.post_updates.len() as u64;
                for (_, data) in meta.pre_updates.iter().chain(meta.post_updates.iter()) {
                    self.raw_account_data_bytes += data.len() as u64;
                }
                self.block_meta_tx_count += meta.executed_transaction_count;
                let poh_ok = !self.full
                    || recompute_blockhash(&meta.parent_blockhash, entries, &self.cur_sigs)
                        == meta.blockhash;
                self.block_hashes.push(BlockHashes {
                    slot: meta.slot,
                    parent_slot: meta.parent_slot,
                    parent_blockhash: meta.parent_blockhash,
                    blockhash: meta.blockhash,
                    poh_ok,
                });
            }
        }
        if self.full {
            self.cur_sigs.clear();
        }
        if let Some(p) = &self.progress {
            p.fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// Fetches the canonical blockhash of `slot` from a Solana JSON-RPC endpoint
/// (`JETSTREAMER_RPC_URL`, default mainnet-beta). Note: the public endpoint only
/// serves recent slots — for historical epochs pass the seed explicitly or set
/// `JETSTREAMER_RPC_URL` to an archival endpoint.
fn fetch_blockhash_from_rpc(slot: u64) -> Result<Hash, String> {
    let url = std::env::var("JETSTREAMER_RPC_URL")
        .unwrap_or_else(|_| "https://api.mainnet-beta.solana.com".to_string());
    let body = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getBlock",
        "params": [slot, {
            "encoding": "json",
            "transactionDetails": "none",
            "rewards": false,
            "maxSupportedTransactionVersion": 0
        }],
    });
    let client = reqwest::blocking::Client::new();
    let resp: serde_json::Value = client
        .post(&url)
        .json(&body)
        .send()
        .map_err(|e| format!("RPC request to {url} failed: {e}"))?
        .json()
        .map_err(|e| format!("RPC response parse failed: {e}"))?;
    if let Some(err) = resp.get("error") {
        return Err(format!(
            "RPC getBlock({slot}) error: {err} (the public endpoint may not retain this slot; \
             pass the seed explicitly or set JETSTREAMER_RPC_URL to an archival RPC)"
        ));
    }
    let bh = resp["result"]["blockhash"]
        .as_str()
        .ok_or_else(|| format!("RPC getBlock({slot}) returned no blockhash"))?;
    Hash::from_str(bh).map_err(|e| format!("RPC blockhash parse failed: {e}"))
}

/// Parallel verification across `threads`: each thread scans a disjoint bucket
/// range (counts, byte stats, per-block identities); in `full` mode it also
/// recomputes every block's PoH. After merging, a sequential linkage pass
/// checks the blockhash chain (anchored to `seed_arg`/RPC in full mode).
/// Returns the process exit code.
fn run_scan(path: &str, threads: usize, full: bool, seed_arg: Option<Hash>) -> i32 {
    let (slot_start, slot_count, bucket_slots, bucket_count, epoch) = {
        let file = std::fs::File::open(path).unwrap_or_else(|e| panic!("open {path}: {e}"));
        let r = ArchiveReader::open(BufReader::with_capacity(1 << 20, file))
            .unwrap_or_else(|e| panic!("open archive: {e}"));
        let h = r.header();
        (
            h.slot_start,
            h.slot_count,
            h.bucket_slots as u64,
            r.bucket_count() as u64,
            h.epoch,
        )
    };
    let threads = threads.max(1).min(bucket_count.max(1) as usize);
    let mode = if full { "full PoH verify" } else { "verify" };
    println!(
        "{mode}: epoch={epoch} slot_start={slot_start} slot_count={slot_count} \
         buckets={bucket_count} threads={threads}"
    );

    let progress = Arc::new(AtomicU64::new(0));
    let start = Instant::now();
    let done = Arc::new(AtomicU64::new(0));
    let monitor = {
        let progress = progress.clone();
        let done = done.clone();
        std::thread::spawn(move || {
            while done.load(Ordering::Relaxed) == 0 {
                std::thread::sleep(std::time::Duration::from_millis(500));
                let n = progress.load(Ordering::Relaxed);
                let el = start.elapsed().as_secs_f64().max(0.001);
                let rate = n as f64 / el;
                let eta = (slot_count.saturating_sub(n)) as f64 / rate.max(1e-9);
                eprintln!(
                    "[verify] {:.1}% | {} / {} slots | {} slots/s | eta {}",
                    n as f64 * 100.0 / slot_count.max(1) as f64,
                    commas(n),
                    commas(slot_count),
                    commas(rate.round() as u64),
                    fmt_hms(eta),
                );
            }
        })
    };

    // Parallel scan over disjoint bucket ranges. Each block is verified using
    // only its own stored data, so phase 1 needs no cross-thread coordination.
    let per_thread = bucket_count.div_ceil(threads as u64);
    let results: Vec<(ScanVisitor, PayloadByteStats)> = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..threads as u64)
            .filter_map(|t| {
                let b_start = t * per_thread;
                if b_start >= bucket_count {
                    return None;
                }
                let b_count = per_thread.min(bucket_count - b_start);
                let sub_start = slot_start + b_start * bucket_slots;
                let sub_count = b_count * bucket_slots;
                let path = path.to_string();
                let progress = progress.clone();
                Some(scope.spawn(move || {
                    let file = std::fs::File::open(&path).expect("open");
                    let mut reader = ArchiveReader::open(BufReader::with_capacity(8 << 20, file))
                        .expect("open archive");
                    let mut v = ScanVisitor::new(full, Some(progress));
                    reader
                        .read_slots(sub_start, sub_count, &mut v)
                        .unwrap_or_else(|e| panic!("read_slots failed: {e}"));
                    let bytes = reader.payload_byte_stats();
                    (v, bytes)
                }))
            })
            .collect();
        handles.into_iter().map(|h| h.join().expect("thread")).collect()
    });
    done.store(1, Ordering::Relaxed);
    let _ = monitor.join();

    // Merge counts, byte stats, and per-block identities.
    let mut scan = ScanVisitor::new(full, None);
    let mut byte_stats = PayloadByteStats::default();
    for (v, bs) in results {
        byte_stats.transaction_bytes += bs.transaction_bytes;
        byte_stats.account_update_bytes += bs.account_update_bytes;
        byte_stats.other_bytes += bs.other_bytes;
        scan.merge(v);
    }
    let mut blocks = std::mem::take(&mut scan.block_hashes);
    blocks.sort_unstable_by_key(|b| b.slot);
    let block_count = blocks.len() as u64;
    let slots = block_count + scan.skipped;

    // Seed (full mode only) anchors the first block to the previous epoch.
    let seed = if full {
        match (seed_arg, blocks.first()) {
            (Some(s), _) => Some(s),
            (None, Some(first)) => {
                eprintln!(
                    "[verify] no seed given; fetching parent blockhash for slot {} via RPC…",
                    first.parent_slot
                );
                match fetch_blockhash_from_rpc(first.parent_slot) {
                    Ok(h) => Some(h),
                    Err(e) => {
                        eprintln!("[verify] seed fetch failed: {e}");
                        None
                    }
                }
            }
            (None, None) => None,
        }
    } else {
        None
    };

    // Linkage: blocks[] holds only canonical blocks in slot order, so each
    // block's parent is the previous one. The first block is anchored to the
    // seed in full mode; in normal mode it is left unchecked (its parent is in
    // the previous epoch, which the archive doesn't contain).
    let mut linkage_breaks: Vec<(u64, Hash, Hash)> = Vec::new();
    for i in 0..blocks.len() {
        let expected = if i == 0 { seed } else { Some(blocks[i - 1].blockhash) };
        if let Some(exp) = expected
            && blocks[i].parent_blockhash != exp
        {
            linkage_breaks.push((blocks[i].slot, exp, blocks[i].parent_blockhash));
        }
    }
    let poh_failures: Vec<u64> = blocks.iter().filter(|b| !b.poh_ok).map(|b| b.slot).collect();

    let elapsed = start.elapsed().as_secs_f64();
    println!("\n=== verify summary ({elapsed:.1}s, {threads} threads) ===");
    println!("  slots (blk+skipped): {}", commas(slots));
    println!("  blocks:              {}", commas(block_count));
    println!("  skipped slots:       {}", commas(scan.skipped));
    println!("  epochs:              {}", commas(scan.epochs));
    println!("  transactions:        {}", commas(scan.transactions));
    println!("  tx account updates:  {}", commas(scan.tx_account_updates));
    println!("  orphan updates:      {}", commas(scan.orphan_updates));
    println!("  entries:             {}", commas(scan.entries));
    println!("  rewards:             {}", commas(scan.rewards));
    println!(
        "  block-meta tx total: {} (vs decoded {})",
        commas(scan.block_meta_tx_count),
        commas(scan.transactions)
    );

    report_byte_breakdown(byte_stats, scan.raw_account_data_bytes);
    report_chain_breaks(&linkage_breaks, block_count);

    if full {
        println!("\n=== full PoH verification ===");
        println!(
            "  PoH recompute: {} OK, {} mismatched",
            commas(block_count - poh_failures.len() as u64),
            commas(poh_failures.len() as u64),
        );
        for slot in poh_failures.iter().take(20) {
            println!("    PoH mismatch at slot {slot} (recomputed hash != stored blockhash)");
        }
        match seed {
            Some(s) => println!("  chain anchor (seed): {s}"),
            None => println!("  chain anchor (seed): NONE — first block's parent not verified"),
        }
    }

    let counts_ok = scan.block_meta_tx_count == scan.transactions;
    if !counts_ok {
        println!(
            "\n  WARNING: block-meta tx total ({}) != decoded transactions ({})",
            scan.block_meta_tx_count, scan.transactions
        );
    }

    let pass = linkage_breaks.is_empty() && poh_failures.is_empty() && counts_ok;
    if pass && (!full || seed.is_some()) {
        let detail = if full {
            "every block's PoH recomputes to its stored blockhash and the chain links cleanly \
             from the anchor — cryptographically verified"
        } else {
            "all buckets checksum-verified, every slot/transaction/update decoded, blockhash \
             chain intact, tallies consistent"
        };
        println!("\nRESULT: OK — {detail}.");
        0
    } else if pass {
        println!(
            "\nRESULT: OK (unanchored) — all PoH hashes and internal links verify, but no seed \
             was available to anchor the first block to the previous epoch."
        );
        0
    } else {
        println!(
            "\nRESULT: FAIL — {} linkage break(s), {} PoH mismatch(es){}.",
            linkage_breaks.len(),
            poh_failures.len(),
            if counts_ok { "" } else { ", tally mismatch" },
        );
        1
    }
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.is_empty() {
        eprintln!(
            "usage: verify_archive <path> [max_slots] [start_slot] [--full [parent_blockhash]] [--threads N]"
        );
        eprintln!(
            "  --full       cryptographically recompute every block's PoH hash and verify the"
        );
        eprintln!(
            "               blockhash chain (parallel; much slower). Optional arg is the previous"
        );
        eprintln!(
            "               epoch's tail blockhash to anchor the chain; if omitted it is fetched"
        );
        eprintln!("               via JETSTREAMER_RPC_URL (default mainnet-beta).");
        eprintln!(
            "  --threads N  parallelism for whole-file verification (default: CPU count)."
        );
        std::process::exit(2);
    }
    let path = args[0].clone();
    let mut positionals: Vec<u64> = Vec::new();
    let mut full = false;
    let mut seed_arg: Option<Hash> = None;
    let mut threads = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(8);
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--threads" => {
                i += 1;
                threads = args
                    .get(i)
                    .and_then(|v| v.parse().ok())
                    .expect("--threads N");
            }
            "--full" => {
                full = true;
                // Optional seed: the next arg, if it parses as a base58 hash.
                if let Some(next) = args.get(i + 1)
                    && !next.starts_with("--")
                    && let Ok(h) = Hash::from_str(next)
                {
                    seed_arg = Some(h);
                    i += 1;
                }
            }
            other => positionals.push(
                other
                    .parse()
                    .unwrap_or_else(|_| panic!("expected a number or flag, got `{other}`")),
            ),
        }
        i += 1;
    }
    let max_slots = positionals.first().copied().unwrap_or(u64::MAX);
    let start_slot_arg: Option<u64> = positionals.get(1).copied();

    // `--full` and whole-file normal verification both run the parallel scanner.
    // A partial slice (max_slots / start_slot, for throughput sampling) keeps the
    // single-pass path below.
    if full {
        std::process::exit(run_scan(&path, threads, true, seed_arg));
    }
    if max_slots == u64::MAX && start_slot_arg.is_none() {
        std::process::exit(run_scan(&path, threads, false, None));
    }

    let file = std::fs::File::open(&path).unwrap_or_else(|e| panic!("open {path}: {e}"));
    let file_len = file.metadata().map(|m| m.len()).unwrap_or(0);
    // Large buffer: streaming is forward and sequential, one bucket at a time.
    let source = BufReader::with_capacity(16 << 20, file);

    let start = Instant::now();
    let mut reader = ArchiveReader::open(source).unwrap_or_else(|e| panic!("open archive: {e}"));
    // The decoder's hard chain check aborts on the first mismatch; verify chain
    // continuity softly in the visitor instead, so the whole archive is read in
    // one pass and every break is reported (not just the first).
    reader.verify_chain = false;

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
    let mut tally = Tally {
        target_slots: to_read,
        ..Default::default()
    };
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

    report_byte_breakdown(reader.payload_byte_stats(), tally.raw_account_data_bytes);
    report_chain_breaks(&tally.chain_breaks, tally.blocks);

    // This path only runs for a partial slice (whole-file verification goes
    // through the parallel scanner), so counts may be incomplete by design.
    if tally.chain_breaks.is_empty() {
        println!("\nRESULT: OK (partial read of {visited} slots).");
    } else {
        println!(
            "\nRESULT: partial read of {visited} slots with {} chain break(s); see above.",
            tally.chain_breaks.len()
        );
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sig(b: u8) -> Signature {
        Signature::from([b; 64])
    }

    /// `recompute_blockhash` must split the flat per-tx signature list into
    /// entries by `tx_count` and fold them in order — identical to calling
    /// Solana's `next_hash` entry by entry.
    #[test]
    fn recompute_matches_reference_fold() {
        let parent = Hash::new_from_array([7u8; 32]);
        // tick(10) ++ tx-entry(5, 2 txs) ++ tick(8).
        let entries = vec![
            EntryRecord {
                num_hashes: 10,
                tx_count: 0,
            },
            EntryRecord {
                num_hashes: 5,
                tx_count: 2,
            },
            EntryRecord {
                num_hashes: 8,
                tx_count: 0,
            },
        ];
        // First tx has 1 signature, second has 2.
        let sigs = vec![vec![sig(1)], vec![sig(2), sig(3)]];

        // Reference fold straight through next_hash.
        let entry_txs: Vec<VersionedTransaction> = sigs
            .iter()
            .map(|s| VersionedTransaction {
                signatures: s.clone(),
                message: VersionedMessage::default(),
            })
            .collect();
        let mut poh = parent;
        poh = solana_entry::entry::next_hash(&poh, 10, &[]);
        poh = solana_entry::entry::next_hash(&poh, 5, &entry_txs);
        poh = solana_entry::entry::next_hash(&poh, 8, &[]);

        assert_eq!(recompute_blockhash(&parent, &entries, &sigs), poh);
    }

    /// A wrong stored blockhash must not equal the recompute (sanity that the
    /// check actually discriminates).
    #[test]
    fn recompute_detects_tampering() {
        let parent = Hash::new_from_array([1u8; 32]);
        let entries = vec![EntryRecord {
            num_hashes: 3,
            tx_count: 1,
        }];
        let sigs = vec![vec![sig(9)]];
        let computed = recompute_blockhash(&parent, &entries, &sigs);
        assert_ne!(computed, Hash::default());
        assert_ne!(computed, parent);
    }

    /// End-to-end smoke of the parallel scanner: build a real multi-bucket
    /// `.jet` and run normal (non-full) verification over it across threads.
    #[test]
    fn run_scan_parallel_smoke() {
        use jetstreamer_horizon::archive::{ArchiveWriter, ArchiveWriterConfig};

        let (epoch, slot_start, n) = (5u64, 1_000u64, 300u64); // 3 buckets @ 128
        let mut w = ArchiveWriter::new(
            std::io::Cursor::new(Vec::new()),
            epoch,
            slot_start,
            n,
            ArchiveWriterConfig::default(),
        )
        .unwrap();
        for i in 0..n {
            w.write_skipped_slot(slot_start + i).unwrap();
        }
        let (sink, _) = w.finish().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("smoke.jet");
        std::fs::write(&path, sink.into_inner()).unwrap();

        // All-skipped archive: no blocks, so linkage is trivially intact and the
        // parallel scan/merge/report path should run clean and return OK.
        let code = run_scan(path.to_str().unwrap(), 4, false, None);
        assert_eq!(code, 0);
    }
}
