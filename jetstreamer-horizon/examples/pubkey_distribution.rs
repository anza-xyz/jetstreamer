//! Measures the pubkey reference distribution in a horizon archive.
//!
//! Answers the two questions that size a dictionary-compression design:
//!
//! 1. **How big is a per-window dedupe checkpoint?** For each window of
//!    `--window` slots, how many *distinct* pubkeys appear, and how many of
//!    those are referenced more than once (single-use keys are cheaper left
//!    as inline literals than as dictionary entries — break-even is ~1.1
//!    references). Blob size is `distinct_multi_ref × 32` bytes.
//!
//! 2. **How large should the frozen prime table be?** The coverage curve —
//!    what fraction of all references the top *N* pubkeys account for, at
//!    each lencode varint size-class boundary (127, 255, 65_535,
//!    16_777_215). Crossed with the ID width at each size, this says
//!    directly whether growing the table past 65_535 pays for itself.
//!
//! Runs with account-update *data* materialization disabled, so it decodes
//! at metadata speed — the pubkeys and their counts are all it needs.
//!
//! Usage:
//! ```text
//! cargo run --release -p jetstreamer-horizon --example pubkey_distribution -- \
//!     <path.jet> [--window N] [--max-slots N] [--global]
//! ```
//! `--global` additionally accumulates whole-file counts to produce the
//! prime-table coverage curve. That holds every distinct pubkey in the file
//! in memory (tens of millions for a full epoch — budget a few GB).

use std::collections::HashMap;
use std::io::BufReader;

use ahash::RandomState;
use jetstreamer_horizon::archive::{
    ArchiveReader, BlockNotification, Consumption, EntryRecord, EpochMeta, SlotVisitor,
};
use jetstreamer_horizon::transactions::{Transaction, VersionedMessage};
use solana_address::Address;

/// lencode varint size classes: an ID `<= bound` costs `bytes` on the wire.
const SIZE_CLASSES: &[(u64, usize)] = &[
    (127, 1),
    (255, 2),
    (65_535, 3),
    (16_777_215, 4),
    (4_294_967_295, 5),
];

/// Per-window statistics, emitted as each window closes.
#[derive(Default)]
struct WindowStats {
    distinct: u64,
    distinct_multi_ref: u64,
    references: u64,
}

struct Collector {
    window_slots: u64,
    /// Reference counts within the current window.
    window: HashMap<Address, u32, RandomState>,
    /// First slot of the current window (`None` until the first block).
    window_start: Option<u64>,
    windows: Vec<WindowStats>,
    /// Whole-file counts, only when `--global` is set.
    global: Option<HashMap<Address, u64, RandomState>>,
    slots_seen: u64,
}

impl Collector {
    fn new(window_slots: u64, track_global: bool) -> Self {
        Self {
            window_slots,
            window: HashMap::with_hasher(RandomState::new()),
            window_start: None,
            windows: Vec::new(),
            global: track_global.then(|| HashMap::with_hasher(RandomState::new())),
            slots_seen: 0,
        }
    }

    #[inline]
    fn note(&mut self, key: Address) {
        *self.window.entry(key).or_insert(0) += 1;
        if let Some(g) = self.global.as_mut() {
            *g.entry(key).or_insert(0) += 1;
        }
    }

    /// Closes the current window and records its statistics.
    fn close_window(&mut self) {
        if self.window.is_empty() {
            return;
        }
        let mut stats = WindowStats {
            distinct: self.window.len() as u64,
            ..Default::default()
        };
        for count in self.window.values() {
            stats.references += *count as u64;
            if *count > 1 {
                stats.distinct_multi_ref += 1;
            }
        }
        self.windows.push(stats);
        self.window.clear();
    }

    /// Advances the window boundary, flushing if `slot` starts a new one.
    fn advance_to(&mut self, slot: u64) {
        match self.window_start {
            None => self.window_start = Some(slot),
            Some(start) if slot >= start + self.window_slots => {
                self.close_window();
                // Align to the window grid rather than to the observed slot,
                // so skipped slots don't drift the boundaries.
                let steps = (slot - start) / self.window_slots;
                self.window_start = Some(start + steps * self.window_slots);
            }
            Some(_) => {}
        }
    }
}

impl SlotVisitor for Collector {
    // Only metadata is needed; skipping diff reconstruction makes this run
    // at roughly 4x the speed of a materializing pass.
    fn consumption(&self) -> Consumption {
        Consumption::all().without_account_update_data()
    }

    fn on_epoch(&mut self, meta: &EpochMeta) {
        for (m, _) in meta.updates.iter() {
            self.note(m.pubkey);
            self.note(m.owner);
        }
    }

    fn on_transaction(&mut self, slot: u64, _tx_index: u32, tx: &Transaction) {
        self.advance_to(slot);

        let keys = match &tx.message {
            VersionedMessage::Legacy(m) => m.account_keys.as_slice(),
            VersionedMessage::V0(m) => m.account_keys.as_slice(),
        };
        for k in keys {
            self.note(*k);
        }
        for k in tx.loaded_writable_addresses.as_slice() {
            self.note(*k);
        }
        for k in tx.loaded_readonly_addresses.as_slice() {
            self.note(*k);
        }
        for (m, _) in tx.iter_account_updates() {
            self.note(m.pubkey);
            self.note(m.owner);
        }
    }

    fn on_block(&mut self, notification: &BlockNotification, _entries: &[EntryRecord]) {
        self.slots_seen += 1;
        let slot = notification.slot();
        self.advance_to(slot);
        if let BlockNotification::Block(meta) = notification {
            for (m, _) in meta.pre_updates.iter().chain(meta.post_updates.iter()) {
                self.note(m.pubkey);
                self.note(m.owner);
            }
        }
    }
}

fn commas(n: u64) -> String {
    let s = n.to_string();
    let mut out = String::with_capacity(s.len() + s.len() / 3);
    for (i, c) in s.chars().enumerate() {
        if i > 0 && (s.len() - i).is_multiple_of(3) {
            out.push(',');
        }
        out.push(c);
    }
    out
}

fn mean(v: &[u64]) -> f64 {
    if v.is_empty() {
        return 0.0;
    }
    v.iter().sum::<u64>() as f64 / v.len() as f64
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.is_empty() {
        eprintln!("usage: pubkey_distribution <path.jet> [--window N] [--max-slots N] [--global]");
        eprintln!("  --window N     slots per dictionary window (default 1000)");
        eprintln!("  --max-slots N  stop after N slots (default: whole file)");
        eprintln!("  --global       also build the whole-file coverage curve");
        eprintln!("                 (holds every distinct pubkey in RAM)");
        std::process::exit(2);
    }
    let path = args[0].clone();
    let mut window_slots = 1000u64;
    let mut max_slots = u64::MAX;
    let mut track_global = false;
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--window" => {
                i += 1;
                window_slots = args[i].parse().expect("--window N");
            }
            "--max-slots" => {
                i += 1;
                max_slots = args[i].parse().expect("--max-slots N");
            }
            "--global" => track_global = true,
            other => {
                eprintln!("unknown argument: {other}");
                std::process::exit(2);
            }
        }
        i += 1;
    }

    let file = std::fs::File::open(&path).expect("open archive");
    let mut reader = ArchiveReader::open(BufReader::new(file)).expect("parse archive");
    let mut collector = Collector::new(window_slots, track_global);

    let start = std::time::Instant::now();
    reader
        .read_slots(0, max_slots, &mut collector)
        .expect("read slots");
    collector.close_window();
    let elapsed = start.elapsed().as_secs_f64();

    // --- per-window results: this sizes the checkpoint blobs ---
    let distinct: Vec<u64> = collector.windows.iter().map(|w| w.distinct).collect();
    let multi: Vec<u64> = collector
        .windows
        .iter()
        .map(|w| w.distinct_multi_ref)
        .collect();
    let refs: Vec<u64> = collector.windows.iter().map(|w| w.references).collect();

    println!(
        "\n=== {} | {} slots in {elapsed:.1}s | window = {} slots | {} windows ===",
        path,
        commas(collector.slots_seen),
        commas(window_slots),
        commas(collector.windows.len() as u64),
    );

    if collector.windows.is_empty() {
        println!("no windows collected");
        return;
    }

    let mean_distinct = mean(&distinct);
    let mean_multi = mean(&multi);
    let mean_refs = mean(&refs);
    let max_multi = multi.iter().copied().max().unwrap_or(0);
    let min_multi = multi.iter().copied().min().unwrap_or(0);

    println!("\n--- per window (the checkpoint blob) ---");
    println!(
        "  pubkey references     : mean {}",
        commas(mean_refs as u64)
    );
    println!(
        "  distinct pubkeys      : mean {}",
        commas(mean_distinct as u64)
    );
    println!(
        "  distinct, 2+ refs     : mean {} (min {}, max {})",
        commas(mean_multi as u64),
        commas(min_multi),
        commas(max_multi),
    );
    println!(
        "  single-use pubkeys    : mean {}  <- cheaper as inline literals",
        commas((mean_distinct - mean_multi) as u64)
    );
    println!(
        "\n  BLOB SIZE (2+ refs x 32 B): mean {:.1} MB, max {:.1} MB",
        mean_multi * 32.0 / 1e6,
        max_multi as f64 * 32.0 / 1e6,
    );
    println!(
        "  all distinct x 32 B       : mean {:.1} MB  (upper bound, no filter)",
        mean_distinct * 32.0 / 1e6
    );
    let windows_per_epoch = 432_000 / window_slots;
    println!(
        "  blobs per epoch           : {} windows -> {:.1} GB/epoch",
        commas(windows_per_epoch),
        windows_per_epoch as f64 * mean_multi * 32.0 / 1e9,
    );

    // Byte comparison: self-contained rows vs rows + window dictionary.
    let self_contained = 33.0 * mean_refs;
    let with_dict = 32.0 * mean_multi + 4.0 * mean_refs;
    println!("\n--- pubkey bytes per window ---");
    println!(
        "  self-contained rows (33 B each) : {:.1} MB",
        self_contained / 1e6
    );
    println!(
        "  rows + window dictionary        : {:.1} MB  ({:.1}x better)",
        with_dict / 1e6,
        self_contained / with_dict,
    );

    // --- whole-file coverage curve: this sizes the frozen prime table ---
    if let Some(global) = collector.global {
        let mut counts: Vec<u64> = global.values().copied().collect();
        counts.sort_unstable_by(|a, b| b.cmp(a));
        let total: u64 = counts.iter().sum();
        let n_distinct = counts.len() as u64;

        println!("\n--- whole file (prime-table sizing) ---");
        println!("  distinct pubkeys : {}", commas(n_distinct));
        println!("  total references : {}", commas(total));
        println!(
            "\n  {:>12} | {:>8} | {:>9} | {:>12}",
            "top N", "ID bytes", "coverage", "table size"
        );
        println!("  {}", "-".repeat(50));
        let mut cumulative = 0u64;
        let mut idx = 0usize;
        for (bound, bytes) in SIZE_CLASSES {
            let n = (*bound as usize).min(counts.len());
            while idx < n {
                cumulative += counts[idx];
                idx += 1;
            }
            let pct = if total > 0 {
                cumulative as f64 * 100.0 / total as f64
            } else {
                0.0
            };
            println!(
                "  {:>12} | {:>8} | {:>8.2}% | {:>9.1} MB",
                commas(*bound),
                bytes,
                pct,
                (n as f64 * 32.0) / 1e6,
            );
            if n == counts.len() {
                break;
            }
        }

        // Weighted average ID width at each candidate table size — the
        // number that decides whether a bigger prime table pays.
        println!("\n  --- mean bytes per reference, by frozen-table size ---");
        for (bound, _) in SIZE_CLASSES.iter().take(4) {
            let n = (*bound as usize).min(counts.len());
            let mut bytes = 0f64;
            let mut covered = 0u64;
            for (rank, c) in counts.iter().take(n).enumerate() {
                let id = rank as u64 + 1;
                let w = SIZE_CLASSES
                    .iter()
                    .find(|(b, _)| id <= *b)
                    .map(|(_, w)| *w)
                    .unwrap_or(9);
                bytes += (*c as f64) * w as f64;
                covered += *c;
            }
            // Uncovered references fall back to a 4-byte scratch ID after a
            // one-time 33-byte literal per window; approximate as 4 B here
            // and report the literal overhead separately.
            let uncovered = total - covered;
            bytes += uncovered as f64 * 4.0;
            println!(
                "    table = {:>12} entries ({:>6.1} MB): {:.2} B/ref",
                commas(*bound),
                (n as f64 * 32.0) / 1e6,
                bytes / total as f64,
            );
            if n == counts.len() {
                break;
            }
        }
    } else {
        println!("\n(run with --global for the prime-table coverage curve)");
    }
}
