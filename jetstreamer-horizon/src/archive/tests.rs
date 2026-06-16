//! Roundtrip and seek tests for the archive container format.
//!
//! Synthetic slot streams exercise both compression layers: pubkey reuse
//! (dedupe) and repeated small mutations to the same accounts across
//! consecutive slots (diff).
use solana_address::Address;
use solana_hash::Hash;
use solana_signature::Signature;

use crate::account_updates::AccountUpdateView;
use crate::pubkey_prime::POPULAR_PUBKEYS;
use crate::transactions::{Transaction, VersionedMessage};

use super::*;

// --- deterministic PRNG (splitmix64) ---

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E3779B97F4A7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
        z ^ (z >> 31)
    }

    fn fill(&mut self, out: &mut [u8]) {
        for chunk in out.chunks_mut(8) {
            let v = self.next().to_le_bytes();
            chunk.copy_from_slice(&v[..chunk.len()]);
        }
    }
}

/// A small virtual ledger: persistent account blobs mutated slightly on
/// each touch, so consecutive-slot updates produce diff-friendly data.
struct Ledger {
    accounts: Vec<(Address, Vec<u8>)>,
    rng: Rng,
}

impl Ledger {
    fn new(seed: u64, n: usize) -> Self {
        let mut rng = Rng(seed);
        let accounts = (0..n)
            .map(|i| {
                let pk = if i % 3 == 0 {
                    Address::new_from_array(POPULAR_PUBKEYS[i % POPULAR_PUBKEYS.len()])
                } else {
                    let mut b = [0u8; 32];
                    rng.fill(&mut b);
                    Address::new_from_array(b)
                };
                let mut data = vec![0u8; 64 + (i % 512)];
                rng.fill(&mut data);
                (pk, data)
            })
            .collect();
        Self { accounts, rng }
    }

    /// Mutates a few bytes of account `i` and returns (pubkey, fresh copy).
    fn touch(&mut self, i: usize) -> (Address, Vec<u8>) {
        let idx = i % self.accounts.len();
        let (pk, data) = &mut self.accounts[idx];
        if !data.is_empty() {
            let off = (self.rng.next() as usize) % data.len();
            data[off] = data[off].wrapping_add(1);
        }
        (*pk, data.clone())
    }
}

/// Builds a deterministic transaction with `n_updates` account updates
/// drawn from the ledger.
fn build_tx(rng: &mut Rng, ledger: &mut Ledger, n_updates: usize) -> Box<Transaction> {
    let mut tx = Transaction::new_boxed();
    tx.fee = rng.next() % 100_000;
    tx.compute_units_consumed = Some(rng.next() % 1_400_000);

    let mut sig = [0u8; 64];
    rng.fill(&mut sig);
    tx.signatures.push(Signature::from(sig));

    if let VersionedMessage::Legacy(m) = &mut tx.message {
        m.header.num_required_signatures = 1;
        for _ in 0..4 {
            let pk_idx = (rng.next() as usize) % POPULAR_PUBKEYS.len();
            m.account_keys
                .push(Address::new_from_array(POPULAR_PUBKEYS[pk_idx]));
        }
        let mut bh = [0u8; 32];
        rng.fill(&mut bh);
        m.recent_blockhash = Hash::new_from_array(bh);
    }

    // Exercise the loaded-address sections (deterministic, dedupe-friendly).
    let lw_idx = (rng.next() as usize) % POPULAR_PUBKEYS.len();
    tx.loaded_writable_addresses
        .push(Address::new_from_array(POPULAR_PUBKEYS[lw_idx]));
    let lr_idx = (rng.next() as usize) % POPULAR_PUBKEYS.len();
    tx.loaded_readonly_addresses
        .push(Address::new_from_array(POPULAR_PUBKEYS[lr_idx]));

    for u in 0..n_updates {
        let (pk, data) = ledger.touch((rng.next() as usize).wrapping_add(u));
        tx.push_account_update(&AccountUpdateView {
            pubkey: pk,
            lamports: rng.next(),
            owner: Address::new_from_array(POPULAR_PUBKEYS[3]),
            executable: false,
            rent_epoch: u64::MAX,
            write_version: rng.next(),
            data: &data,
        })
        .unwrap();
    }
    tx
}

/// Comparable snapshot of a `BlockMeta` (the real type is ~40 MiB and not
/// `Clone`; tests compare scalar fields + flattened orphan updates).
#[derive(Debug, Clone, PartialEq, Default)]
struct MetaSnapshot {
    parent_slot: u64,
    parent_blockhash: Hash,
    blockhash: Hash,
    block_time: Option<i64>,
    block_height: Option<u64>,
    executed_transaction_count: u64,
    entry_count: u64,
    n_rewards: usize,
    num_partitions: Option<u64>,
    // (write_version, data) per orphan update, in order.
    pre: Vec<(u64, Vec<u8>)>,
    post: Vec<(u64, Vec<u8>)>,
}

impl MetaSnapshot {
    fn of(meta: &BlockMeta) -> Self {
        Self {
            parent_slot: meta.parent_slot,
            parent_blockhash: meta.parent_blockhash,
            blockhash: meta.blockhash,
            block_time: meta.block_time,
            block_height: meta.block_height,
            executed_transaction_count: meta.executed_transaction_count,
            entry_count: meta.entry_count,
            n_rewards: meta.rewards.len(),
            num_partitions: meta.num_partitions,
            pre: meta
                .pre_updates
                .iter()
                .map(|(m, d)| (m.write_version, d.to_vec()))
                .collect(),
            post: meta
                .post_updates
                .iter()
                .map(|(m, d)| (m.write_version, d.to_vec()))
                .collect(),
        }
    }
}

/// Comparable snapshot of one transaction: (fee, sig0, n_updates, concat
/// of update data, loaded writable+readonly addresses).
type TxSnapshot = (u64, Signature, usize, Vec<u8>, Vec<Address>);

/// Expected snapshot of a written slot for later comparison.
#[derive(Debug, Clone, PartialEq)]
struct ExpectedSlot {
    slot: u64,
    skipped: bool,
    meta: Option<MetaSnapshot>,
    entries: Vec<EntryRecord>,
    txs: Vec<TxSnapshot>,
}

/// Collecting visitor used to verify reads.
#[derive(Default)]
struct Collector {
    slots: Vec<ExpectedSlot>,
    // (epoch, n_updates) per on_epoch callback.
    epochs: Vec<(u64, usize)>,
}

impl SlotVisitor for Collector {
    fn on_epoch(&mut self, meta: &EpochMeta) {
        self.epochs.push((meta.epoch, meta.updates.len()));
    }

    fn on_transaction(&mut self, slot: u64, tx_index: u32, tx: &Transaction) {
        if tx_index == 0 {
            self.slots.push(ExpectedSlot {
                slot,
                skipped: false,
                meta: None,
                entries: vec![],
                txs: vec![],
            });
        }
        let last = self.slots.last_mut().unwrap();
        let mut data = Vec::new();
        for (_, d) in tx.iter_account_updates() {
            data.extend_from_slice(d);
        }
        let mut loaded: Vec<Address> = tx.loaded_writable_addresses.as_slice().to_vec();
        loaded.extend_from_slice(tx.loaded_readonly_addresses.as_slice());
        last.txs.push((
            tx.fee,
            tx.signatures[0],
            tx.account_updates().len(),
            data,
            loaded,
        ));
    }

    fn on_block(&mut self, notification: &BlockNotification, entries: &[EntryRecord]) {
        let slot = notification.slot();
        match notification {
            BlockNotification::Skipped(_) => {
                self.slots.push(ExpectedSlot {
                    slot,
                    skipped: true,
                    meta: None,
                    entries: vec![],
                    txs: vec![],
                });
            }
            BlockNotification::Block(meta) => {
                // Blocks with zero transactions never got a slot pushed by
                // on_transaction — push one now.
                if self.slots.last().map(|s| s.slot) != Some(slot) {
                    self.slots.push(ExpectedSlot {
                        slot,
                        skipped: false,
                        meta: None,
                        entries: vec![],
                        txs: vec![],
                    });
                }
                let last = self.slots.last_mut().unwrap();
                last.meta = Some(MetaSnapshot::of(meta));
                last.entries = entries.to_vec();
            }
        }
    }
}

/// Writes `n_slots` synthetic slots starting at `slot_start` and returns
/// (archive bytes, expected snapshots).
fn write_archive(
    slot_start: u64,
    n_slots: u64,
    config: ArchiveWriterConfig,
) -> (Vec<u8>, Vec<ExpectedSlot>, ArchiveStats) {
    write_archive_ckpt(slot_start, n_slots, config, None)
}

/// Like [`write_archive`], but when `checkpoint_at` is `Some(slot)` it takes a
/// writer checkpoint just before emitting that slot, drops the writer, and
/// resumes appending to the same buffer — the in-memory equivalent of a crash
/// and restart on the durable prefix.
fn write_archive_ckpt(
    slot_start: u64,
    n_slots: u64,
    config: ArchiveWriterConfig,
    checkpoint_at: Option<u64>,
) -> (Vec<u8>, Vec<ExpectedSlot>, ArchiveStats) {
    let mut rng = Rng(42);
    let mut ledger = Ledger::new(7, 64);
    let sink = std::io::Cursor::new(Vec::new());
    let config_for_resume = config.clone();
    let mut writer = ArchiveWriter::new(sink, 900, slot_start, n_slots, config).unwrap();
    let mut expected = Vec::new();
    let mut last_blockhash = Hash::default();

    for i in 0..n_slots {
        let slot = slot_start + i;
        if checkpoint_at == Some(slot) {
            // Crash-resume rehearsal: checkpoint, drop the writer, reopen the
            // durable prefix, and resume appending. The output must come out
            // identical to an uninterrupted write.
            let cp = writer.checkpoint().unwrap();
            let mut cursor = writer.into_sink();
            cursor.set_position(cp.file_offset);
            writer =
                ArchiveWriter::resume(cursor, slot_start, n_slots, config_for_resume.clone(), cp);
        }
        // Every 7th slot is leader-skipped.
        if i % 7 == 3 {
            writer.write_skipped_slot(slot).unwrap();
            expected.push(ExpectedSlot {
                slot,
                skipped: true,
                meta: None,
                entries: vec![],
                txs: vec![],
            });
            continue;
        }

        writer.begin_slot(slot).unwrap();

        // Pre-transaction orphan updates: simulate per-slot sysvar rewrites
        // (same accounts touched every slot — exercises diff compression on
        // the orphan path too).
        let mut exp_pre = Vec::new();
        for sysvar in 0..2usize {
            let (pk, data) = ledger.touch(sysvar);
            let write_version = slot * 100 + sysvar as u64;
            writer
                .write_orphan_update(&AccountUpdateView {
                    pubkey: pk,
                    lamports: 1,
                    owner: Address::new_from_array(POPULAR_PUBKEYS[2]),
                    executable: false,
                    rent_epoch: u64::MAX,
                    write_version,
                    data: &data,
                })
                .unwrap();
            exp_pre.push((write_version, data));
        }

        let n_txs = 1 + (rng.next() % 4) as usize;
        let mut exp_txs = Vec::new();
        for _ in 0..n_txs {
            let n_updates = 1 + (rng.next() % 3) as usize;
            let tx = build_tx(&mut rng, &mut ledger, n_updates);
            writer.write_transaction(&tx).unwrap();
            let mut data = Vec::new();
            for (_, d) in tx.iter_account_updates() {
                data.extend_from_slice(d);
            }
            let mut loaded: Vec<Address> = tx.loaded_writable_addresses.as_slice().to_vec();
            loaded.extend_from_slice(tx.loaded_readonly_addresses.as_slice());
            exp_txs.push((
                tx.fee,
                tx.signatures[0],
                tx.account_updates().len(),
                data,
                loaded,
            ));
        }

        // Post-transaction orphan update: simulate fee distribution to the
        // leader at freeze.
        let mut exp_post = Vec::new();
        {
            let (pk, data) = ledger.touch(3);
            let write_version = slot * 100 + 99;
            writer
                .write_orphan_update(&AccountUpdateView {
                    pubkey: pk,
                    lamports: 5_000,
                    owner: Address::new_from_array(POPULAR_PUBKEYS[2]),
                    executable: false,
                    rent_epoch: u64::MAX,
                    write_version,
                    data: &data,
                })
                .unwrap();
            exp_post.push((write_version, data));
        }

        let mut bh = [0u8; 32];
        rng.fill(&mut bh);
        let blockhash = Hash::new_from_array(bh);
        let mut meta = BlockMeta::new_boxed();
        meta.slot = slot;
        meta.parent_slot = slot.saturating_sub(1);
        meta.parent_blockhash = last_blockhash;
        meta.blockhash = blockhash;
        meta.block_time = Some(1_750_000_000 + slot as i64);
        meta.block_height = Some(slot.saturating_sub(1_000));
        meta.executed_transaction_count = n_txs as u64;
        meta.entry_count = 3;
        let entries = vec![
            EntryRecord {
                num_hashes: 12_500,
                tx_count: n_txs as u32,
            },
            EntryRecord {
                num_hashes: 12_500,
                tx_count: 0,
            },
            EntryRecord {
                num_hashes: 12_500,
                tx_count: 0,
            },
        ];
        writer.end_slot(&meta, &entries).unwrap();
        last_blockhash = blockhash;

        let mut snapshot = MetaSnapshot::of(&meta);
        snapshot.pre = exp_pre;
        snapshot.post = exp_post;
        expected.push(ExpectedSlot {
            slot,
            skipped: false,
            meta: Some(snapshot),
            entries,
            txs: exp_txs,
        });
    }

    let (sink, stats) = writer.finish().unwrap();
    (sink.into_inner(), expected, stats)
}

fn read_all(bytes: &[u8], start_slot: u64, max_slots: u64, verify: bool) -> Vec<ExpectedSlot> {
    let mut reader = ArchiveReader::open(std::io::Cursor::new(bytes)).unwrap();
    reader.verify_chain = verify;
    let mut collector = Collector::default();
    reader
        .read_slots(start_slot, max_slots, &mut collector)
        .unwrap();
    collector.slots
}

#[test]
fn roundtrip_zstd_bucket_128() {
    let (bytes, expected, stats) = write_archive(1_000, 300, ArchiveWriterConfig::default());
    assert_eq!(stats.slots, 300);
    assert_eq!(stats.buckets, 3); // 300 slots / 128 = 3 buckets (last short)

    let got = read_all(&bytes, 0, u64::MAX, true);
    assert_eq!(got.len(), expected.len());
    assert_eq!(got, expected);
}

#[test]
fn archive_checkpoint_serialization_roundtrips() {
    let config = ArchiveWriterConfig::default();
    let mut writer =
        ArchiveWriter::new(std::io::Cursor::new(Vec::new()), 900, 1_000, 300, config).unwrap();
    writer.write_skipped_slot(1_000).unwrap();
    writer.write_skipped_slot(1_001).unwrap();
    let cp = writer.checkpoint().unwrap();

    let bytes = cp.encode_to_vec().unwrap();
    let decoded = ArchiveCheckpoint::decode_from_slice(&bytes).unwrap();

    assert_eq!(decoded.file_offset, cp.file_offset);
    assert_eq!(decoded.index, cp.index);
    assert_eq!(decoded.stats, cp.stats);
    assert_eq!(decoded.last_blockhash, cp.last_blockhash);
    assert_eq!(decoded.last_slot, cp.last_slot);
    assert!(!cp.index.is_empty());
}

#[test]
fn checkpoint_resume_matches_uninterrupted() {
    let config = ArchiveWriterConfig::default();
    // Reference archive, written straight through.
    let (ref_bytes, _exp, ref_stats) = write_archive(1_000, 300, config.clone());
    let ref_slots = read_all(&ref_bytes, 0, u64::MAX, true);

    // Same synthetic data, but checkpoint + resume mid-bucket at slot 1_137.
    let (ck_bytes, _exp2, ck_stats) = write_archive_ckpt(1_000, 300, config, Some(1_137));
    let ck_slots = read_all(&ck_bytes, 0, u64::MAX, true);

    // Bucket layout differs (the checkpoint forces an early flush), but every
    // decoded slot — txs, account updates, orphans, blockhash chain — and the
    // running stats must match an uninterrupted write.
    assert_eq!(ref_slots, ck_slots);
    assert_eq!(ref_stats.slots, ck_stats.slots);
    assert_eq!(ref_stats.blocks, ck_stats.blocks);
    assert_eq!(ref_stats.transactions, ck_stats.transactions);
    assert_eq!(ref_stats.account_updates, ck_stats.account_updates);
    // The forced early flush adds at least one extra bucket boundary.
    assert!(ck_stats.buckets > ref_stats.buckets);
}

#[test]
fn roundtrip_uncompressed() {
    let config = ArchiveWriterConfig {
        compression: Compression::None,
        ..Default::default()
    };
    let (bytes, expected, _) = write_archive(1_000, 200, config);
    let got = read_all(&bytes, 0, u64::MAX, true);
    assert_eq!(got, expected);
}

#[test]
fn roundtrip_bucket_slots_1() {
    // Per-slot encoder reset: every slot independently decodable.
    let config = ArchiveWriterConfig {
        bucket_slots: 1,
        ..Default::default()
    };
    let (bytes, expected, stats) = write_archive(1_000, 50, config);
    assert_eq!(stats.buckets, 50);
    let got = read_all(&bytes, 0, u64::MAX, true);
    assert_eq!(got, expected);
}

#[test]
fn seek_to_mid_bucket_slot() {
    let (bytes, expected, _) = write_archive(1_000, 300, ArchiveWriterConfig::default());
    // Slot 1_171 sits mid-bucket (bucket 1 covers 1_128..1_256).
    let start = 1_171;
    let got = read_all(&bytes, start, u64::MAX, false);
    let expected_tail: Vec<_> = expected
        .iter()
        .filter(|s| s.slot >= start)
        .cloned()
        .collect();
    assert_eq!(got, expected_tail);
}

#[test]
fn seek_with_max_slots_window() {
    let (bytes, expected, _) = write_archive(1_000, 300, ArchiveWriterConfig::default());
    let got = read_all(&bytes, 1_050, 10, false);
    assert_eq!(got.len(), 10);
    let expected_window: Vec<_> = expected
        .iter()
        .filter(|s| s.slot >= 1_050)
        .take(10)
        .cloned()
        .collect();
    assert_eq!(got, expected_window);
}

#[test]
fn diff_compression_shrinks_repeat_updates() {
    // Same ledger accounts touched across many slots: with bucket_slots=128
    // the diff encoder should make the archive much smaller than with
    // bucket_slots=1 (where every update is a full blob).
    let bucketed = write_archive(
        0,
        256,
        ArchiveWriterConfig {
            compression: Compression::None,
            ..Default::default()
        },
    );
    let per_slot = write_archive(
        0,
        256,
        ArchiveWriterConfig {
            compression: Compression::None,
            bucket_slots: 1,
            ..Default::default()
        },
    );
    let bucketed_len = bucketed.0.len() as f64;
    let per_slot_len = per_slot.0.len() as f64;
    assert!(
        bucketed_len < per_slot_len * 0.6,
        "expected bucketed diff encoding to shrink archive: bucketed={bucketed_len} per_slot={per_slot_len}"
    );
}

#[test]
fn sequential_windows_continue_without_reload() {
    // Consuming the archive in many small forward windows must not
    // re-load the bucket on each call — the reader continues in place.
    let (bytes, expected, _) = write_archive(1_000, 300, ArchiveWriterConfig::default());
    let mut reader = ArchiveReader::open(std::io::Cursor::new(&bytes[..])).unwrap();
    let mut collector = Collector::default();

    let mut next = 1_000;
    while reader.read_slots(next, 10, &mut collector).unwrap() > 0 {
        next = collector.slots.last().unwrap().slot + 1;
    }
    assert_eq!(collector.slots, expected);
    // 300 slots / 128-slot buckets = 3 buckets; each should load exactly
    // once despite 30 read_slots calls.
    assert_eq!(reader.bucket_loads(), 3);
}

#[test]
fn forward_jump_within_bucket_streams_through() {
    // Jumping forward inside the already-loaded bucket consumes frames in
    // place (no reload); jumping backward forces a reload.
    let (bytes, expected, _) = write_archive(1_000, 128, ArchiveWriterConfig::default());
    let mut reader = ArchiveReader::open(std::io::Cursor::new(&bytes[..])).unwrap();

    let mut a = Collector::default();
    reader.read_slots(1_010, 1, &mut a).unwrap();
    assert_eq!(reader.bucket_loads(), 1);

    // Forward within the same bucket: stream through, no reload.
    let mut b = Collector::default();
    reader.read_slots(1_100, 1, &mut b).unwrap();
    assert_eq!(reader.bucket_loads(), 1);
    let expected_1100 = expected.iter().find(|s| s.slot >= 1_100).unwrap();
    assert_eq!(&b.slots[0], expected_1100);

    // Backward: must reload the bucket.
    let mut c = Collector::default();
    reader.read_slots(1_010, 1, &mut c).unwrap();
    assert_eq!(reader.bucket_loads(), 2);
    let expected_1010 = expected.iter().find(|s| s.slot >= 1_010).unwrap();
    assert_eq!(&c.slots[0], expected_1010);
}

#[test]
fn epoch_meta_roundtrips_on_boundary_block() {
    let sink = std::io::Cursor::new(Vec::new());
    let mut writer =
        ArchiveWriter::new(sink, 900, 1_000, 100, ArchiveWriterConfig::default()).unwrap();

    // Boundary block: epoch meta (with one epoch-attributed update), one
    // pre-orphan, one tx-free block.
    writer.begin_slot(1_000).unwrap();
    let mut epoch = EpochMeta::new_boxed();
    epoch.epoch = 900;
    epoch.start_slot = 1_000;
    epoch.slot_count = 100;
    epoch.first_block_slot = 1_000;
    epoch.num_reward_partitions = Some(3);
    epoch
        .updates
        .push(&AccountUpdateView {
            pubkey: Address::new_from_array([0xAB; 32]),
            lamports: 1,
            owner: Address::new_from_array([0xCD; 32]),
            executable: true,
            rent_epoch: u64::MAX,
            write_version: 1,
            data: b"feature-activation",
        })
        .unwrap();
    writer.write_epoch_meta(&epoch).unwrap();
    writer
        .write_orphan_update(&AccountUpdateView {
            pubkey: Address::new_from_array([0x11; 32]),
            lamports: 2,
            owner: Address::new_from_array([0x22; 32]),
            executable: false,
            rent_epoch: u64::MAX,
            write_version: 2,
            data: b"clock",
        })
        .unwrap();
    let mut meta = BlockMeta::new_boxed();
    meta.slot = 1_000;
    meta.blockhash = Hash::new_from_array([1u8; 32]);
    writer.end_slot(&meta, &[]).unwrap();
    let (sink, stats) = writer.finish().unwrap();
    assert_eq!(stats.epochs, 1);
    assert_eq!(stats.orphan_account_updates, 2); // 1 epoch-attributed + 1 pre

    let bytes = sink.into_inner();
    let mut reader = ArchiveReader::open(std::io::Cursor::new(&bytes[..])).unwrap();
    let mut collector = Collector::default();
    reader.read_slots(0, u64::MAX, &mut collector).unwrap();
    assert_eq!(collector.epochs, vec![(900, 1)]);
    assert_eq!(collector.slots.len(), 1);
    let slot = &collector.slots[0];
    assert!(!slot.skipped);
    let meta = slot.meta.as_ref().unwrap();
    assert_eq!(meta.pre, vec![(2u64, b"clock".to_vec())]);
    assert!(meta.post.is_empty());
}

#[test]
fn chain_verification_catches_corruption() {
    let (mut bytes, _, _) = write_archive(
        1_000,
        50,
        ArchiveWriterConfig {
            compression: Compression::None,
            ..Default::default()
        },
    );
    // Corrupt one byte somewhere inside the first bucket's payload.
    let mid = bytes.len() / 3;
    bytes[mid] ^= 0xFF;
    let mut reader = ArchiveReader::open(std::io::Cursor::new(&bytes[..])).unwrap();
    let mut collector = Collector::default();
    let result = reader.read_slots(0, u64::MAX, &mut collector);
    assert!(result.is_err(), "corrupted bucket must fail checksum");
}

#[test]
fn open_rejects_truncated_file() {
    let (bytes, _, _) = write_archive(1_000, 20, ArchiveWriterConfig::default());
    let truncated = &bytes[..bytes.len() - 10];
    assert!(ArchiveReader::open(std::io::Cursor::new(truncated)).is_err());
}

#[test]
fn writer_rejects_non_monotonic_slots() {
    let sink = std::io::Cursor::new(Vec::new());
    let mut writer =
        ArchiveWriter::new(sink, 900, 1_000, 100, ArchiveWriterConfig::default()).unwrap();
    writer.write_skipped_slot(1_005).unwrap();
    let err = writer.write_skipped_slot(1_005);
    assert!(matches!(
        err,
        Err(ArchiveFormatError::NonMonotonicSlot { .. })
    ));
}

#[test]
fn writer_rejects_out_of_range_slot() {
    let sink = std::io::Cursor::new(Vec::new());
    let mut writer =
        ArchiveWriter::new(sink, 900, 1_000, 100, ArchiveWriterConfig::default()).unwrap();
    assert!(matches!(
        writer.write_skipped_slot(2_000),
        Err(ArchiveFormatError::SlotOutOfRange { .. })
    ));
}

#[test]
fn empty_archive_roundtrips() {
    let sink = std::io::Cursor::new(Vec::new());
    let writer = ArchiveWriter::new(sink, 900, 1_000, 100, ArchiveWriterConfig::default()).unwrap();
    let (sink, stats) = writer.finish().unwrap();
    assert_eq!(stats.slots, 0);
    let bytes = sink.into_inner();
    let mut reader = ArchiveReader::open(std::io::Cursor::new(&bytes[..])).unwrap();
    let mut collector = Collector::default();
    let n = reader.read_slots(0, u64::MAX, &mut collector).unwrap();
    assert_eq!(n, 0);
}

#[test]
#[ignore = "timing measurement; run with --ignored --nocapture"]
fn timing_stateful_vs_reload_windows() {
    // Bigger corpus so timings are meaningful: 1024 slots = 8 buckets.
    let (bytes, _, _) = write_archive(0, 1024, ArchiveWriterConfig::default());

    // Pattern A (new): one reader, sequential 10-slot windows.
    let t0 = std::time::Instant::now();
    let mut reader = ArchiveReader::open(std::io::Cursor::new(&bytes[..])).unwrap();
    let mut next = 0u64;
    let mut total = 0u64;
    loop {
        let mut c = Collector::default();
        let n = reader.read_slots(next, 10, &mut c).unwrap();
        if n == 0 {
            break;
        }
        total += n;
        next = c.slots.last().unwrap().slot + 1;
    }
    let stateful = t0.elapsed();
    let loads_a = reader.bucket_loads();

    // Pattern B (old behavior): fresh reader per window — every call
    // re-loads the bucket and decodes from its start.
    let t0 = std::time::Instant::now();
    let mut next = 0u64;
    let mut total_b = 0u64;
    let mut loads_b = 0u64;
    loop {
        let mut reader = ArchiveReader::open(std::io::Cursor::new(&bytes[..])).unwrap();
        let mut c = Collector::default();
        let n = reader.read_slots(next, 10, &mut c).unwrap();
        loads_b += reader.bucket_loads();
        if n == 0 {
            break;
        }
        total_b += n;
        next = c.slots.last().unwrap().slot + 1;
    }
    let reload = t0.elapsed();

    assert_eq!(total, total_b);
    eprintln!();
    eprintln!("=== windowed read timing (1024 slots, 103 windows of 10) ===");
    eprintln!(
        "  stateful (new): {:>8.1} ms   ({} bucket loads)",
        stateful.as_secs_f64() * 1e3,
        loads_a
    );
    eprintln!(
        "  reload (old):   {:>8.1} ms   ({} bucket loads)",
        reload.as_secs_f64() * 1e3,
        loads_b
    );
    eprintln!(
        "  speedup: {:.1}x",
        reload.as_secs_f64() / stateful.as_secs_f64()
    );
}
