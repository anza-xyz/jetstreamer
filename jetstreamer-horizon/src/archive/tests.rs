//! Roundtrip and seek tests for the archive container format.
//!
//! Synthetic slot streams exercise both compression layers: pubkey reuse
//! (dedupe) and repeated small mutations to the same accounts across
//! consecutive slots (diff).
use lencode::prelude::{Decode, Encode};
use solana_address::Address;
use solana_hash::Hash;
use solana_signature::Signature;

use crate::account_updates::AccountUpdateView;
use crate::pubkey_prime::POPULAR_PUBKEYS;
use crate::transactions::{Transaction, VersionedMessage};

use super::*;

#[test]
fn writer_defaults_to_measured_archival_zstd_level() {
    assert_eq!(ArchiveWriterConfig::default().zstd_level, 9);
}

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

/// Comparable snapshot of a `BlockMeta` (`BlockMeta` is about 40 MiB and not
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
                // `on_transaction`; push one now.
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
    let mut rng = Rng(42);
    let mut ledger = Ledger::new(7, 64);
    let sink = std::io::Cursor::new(Vec::new());
    let mut writer = ArchiveWriter::new(sink, 900, slot_start, n_slots, config).unwrap();
    let mut expected = Vec::new();
    let mut last_blockhash = Hash::default();

    for i in 0..n_slots {
        let slot = slot_start + i;
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
        // (same accounts touched every slot, exercising diff compression on
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

fn archive_semantic_sha256(bytes: &[u8]) -> [u8; 32] {
    let mut reader = ArchiveReader::open(std::io::Cursor::new(bytes)).unwrap();
    let mut digest = SemanticDigest::new();
    for index in 0..reader.bucket_count() {
        reader.read_bucket(index, &mut digest).unwrap();
    }
    digest.finish().unwrap()
}

fn archive_index(bytes: &[u8]) -> Vec<BucketIndexEntry> {
    let footer = Footer::from_bytes(bytes[bytes.len() - FOOTER_LEN..].try_into().unwrap()).unwrap();
    parse_bucket_index(
        &bytes[footer.index_offset as usize..(footer.index_offset + footer.index_len) as usize],
        &footer,
    )
    .unwrap()
}

fn bucket_frames(bytes: &[u8]) -> Vec<&[u8]> {
    archive_index(bytes)
        .into_iter()
        .map(|entry| &bytes[entry.offset as usize..(entry.offset + entry.len) as usize])
        .collect()
}

fn decoded_bucket_payload(raw: &[u8]) -> (BucketHeader, Vec<u8>) {
    let mut cursor = lencode::io::Cursor::new(raw);
    let header = BucketHeader::decode_ext(&mut cursor, None).unwrap();
    let stored = &raw[cursor.position()..];
    let payload = match header.compression {
        Compression::None => stored.to_vec(),
        Compression::Zstd => {
            zstd::bulk::decompress(stored, header.uncompressed_len as usize).unwrap()
        }
        Compression::Lz4 => {
            lz4::block::decompress(stored, Some(header.uncompressed_len as i32)).unwrap()
        }
    };
    assert_eq!(payload.len() as u64, header.uncompressed_len);
    (header, payload)
}

#[test]
fn read_bucket_stops_at_the_requested_bucket() {
    let (bytes, expected, _) = write_archive(1_000, 300, ArchiveWriterConfig::default());
    let mut reader = ArchiveReader::open(std::io::Cursor::new(&bytes[..])).unwrap();
    reader.verify_chain = true;
    let mut collector = Collector::default();

    let visited = reader.read_bucket(1, &mut collector).unwrap();
    assert_eq!(visited, 128);
    assert_eq!(reader.bucket_loads(), 1);
    assert_eq!(collector.slots, expected[128..256]);

    let error = reader
        .read_bucket(reader.bucket_count(), &mut Collector::default())
        .unwrap_err();
    assert!(matches!(
        error,
        ArchiveFormatError::BucketOutOfRange { index: 3, count: 3 }
    ));
}

#[test]
fn ordered_orphan_callbacks_can_skip_grouped_arena_copies() {
    #[derive(Default)]
    struct OrderedOrphans {
        pre: Vec<(u64, u64, Vec<u8>)>,
        post: Vec<(u64, u64, Vec<u8>)>,
        grouped_updates: usize,
    }

    impl SlotVisitor for OrderedOrphans {
        fn on_pre_account_update(&mut self, slot: u64, update: &AccountUpdateView<'_>) {
            self.pre
                .push((slot, update.write_version, update.data.to_vec()));
        }

        fn on_post_account_update(&mut self, slot: u64, update: &AccountUpdateView<'_>) {
            self.post
                .push((slot, update.write_version, update.data.to_vec()));
        }

        fn on_block(&mut self, notification: &BlockNotification, _entries: &[EntryRecord]) {
            if let BlockNotification::Block(meta) = notification {
                self.grouped_updates += meta.pre_updates.len() + meta.post_updates.len();
            }
        }

        fn consumption(&self) -> Consumption {
            Consumption::all().without_block_account_update_arenas()
        }
    }

    let (bytes, expected, _) = write_archive(1_000, 20, ArchiveWriterConfig::default());
    let mut reader = ArchiveReader::open(std::io::Cursor::new(&bytes[..])).unwrap();
    let mut ordered = OrderedOrphans::default();
    reader.read_slots(0, u64::MAX, &mut ordered).unwrap();

    let expected_pre: Vec<_> = expected
        .iter()
        .flat_map(|slot| {
            slot.meta.iter().flat_map(move |meta| {
                meta.pre
                    .iter()
                    .map(move |(version, data)| (slot.slot, *version, data.clone()))
            })
        })
        .collect();
    let expected_post: Vec<_> = expected
        .iter()
        .flat_map(|slot| {
            slot.meta.iter().flat_map(move |meta| {
                meta.post
                    .iter()
                    .map(move |(version, data)| (slot.slot, *version, data.clone()))
            })
        })
        .collect();
    assert_eq!(ordered.pre, expected_pre);
    assert_eq!(ordered.post, expected_post);
    assert_eq!(ordered.grouped_updates, 0);
    assert!(ordered.pre.iter().all(|(_, _, data)| !data.is_empty()));
    assert!(ordered.post.iter().all(|(_, _, data)| !data.is_empty()));

    // The default visitor contract remains unchanged: grouped arenas are
    // still populated for ordinary consumers.
    assert_eq!(read_all(&bytes, 0, u64::MAX, false), expected);
}

#[test]
fn v1_reencode_is_semantically_lossless_and_bucket_byte_exact() {
    let config = ArchiveWriterConfig {
        format: ArchiveVersion::V1,
        diff_policy: lencode::diff::DiffPolicy::Adaptive,
        ..ArchiveWriterConfig::default()
    };
    let (source, expected, source_stats) = write_archive(1_000, 300, config.clone());
    assert_eq!(parse_file_header(&source).unwrap().0.format_version, 1);

    let sink = std::io::Cursor::new(Vec::new());
    let (sink, stats) = reencode_archive(
        std::io::Cursor::new(&source),
        sink,
        ReencodeOptions {
            writer: config,
            buckets: BucketSelection::All,
        },
    )
    .unwrap();
    let output = sink.into_inner();

    assert_eq!(read_all(&output, 0, u64::MAX, true), expected);
    assert_eq!(
        parse_file_header(&output).unwrap().0.format_version,
        FORMAT_VERSION_V1
    );
    assert_eq!(stats.source_file_bytes, source.len() as u64);
    assert_eq!(stats.source_buckets, source_stats.buckets);
    assert_eq!(stats.slots_reencoded, source_stats.slots);
    assert_eq!(stats.source_bucket_bytes, source_stats.bucket_bytes_written);
    assert_eq!(
        stats.output.bucket_bytes_written,
        source_stats.bucket_bytes_written
    );
    assert_eq!(stats.output.transactions, source_stats.transactions);
    assert_eq!(stats.output.account_updates, source_stats.account_updates);
    assert_eq!(
        stats.output.orphan_account_updates,
        source_stats.orphan_account_updates
    );

    let source_frames = bucket_frames(&source);
    let output_frames = bucket_frames(&output);
    assert_eq!(source_frames.len(), output_frames.len());
    // Compare selected frames rather than whole files: creation metadata in
    // the file header is intentionally regenerated, but a same-config v1
    // replay must reproduce every byte of every bucket.
    for index in [0, 1, source_frames.len() - 1] {
        assert_eq!(output_frames[index], source_frames[index]);
    }
}

#[test]
fn v1_to_v2_reencode_preserves_semantic_sha256() {
    let (source, _, _) = write_archive(
        1_000,
        20,
        ArchiveWriterConfig {
            format: ArchiveVersion::V1,
            diff_policy: lencode::diff::DiffPolicy::Adaptive,
            ..ArchiveWriterConfig::default()
        },
    );
    let (sink, stats) = reencode_archive(
        std::io::Cursor::new(&source),
        std::io::Cursor::new(Vec::new()),
        ReencodeOptions {
            writer: ArchiveWriterConfig::default(),
            buckets: BucketSelection::All,
        },
    )
    .unwrap();
    let output = sink.into_inner();

    assert_eq!(
        parse_file_header(&source).unwrap().0.format_version,
        FORMAT_VERSION_V1
    );
    assert_eq!(
        parse_file_header(&output).unwrap().0.format_version,
        FORMAT_VERSION_V2
    );
    assert_eq!(
        stats.source_semantic_sha256,
        archive_semantic_sha256(&source)
    );
    assert_eq!(
        stats.source_semantic_sha256,
        archive_semantic_sha256(&output)
    );
    assert_eq!(stats.source_epoch, 900);
    assert_eq!(stats.source_slot_start, 1_000);
    assert_eq!(stats.source_slot_count, 20);
    let output_header = parse_file_header(&output).unwrap().0;
    assert_eq!(output_header.epoch, stats.source_epoch);
    assert_eq!(output_header.slot_start, stats.source_slot_start);
    assert_eq!(output_header.slot_count, stats.source_slot_count);
}

#[test]
fn reencode_preserves_post_updates_on_zero_transaction_slots() {
    let mut writer =
        ArchiveWriter::new(Vec::new(), 900, 1_000, 1, ArchiveWriterConfig::default()).unwrap();
    writer.begin_slot(1_000).unwrap();
    let data = b"zero-transaction post state";
    let update = AccountUpdateView {
        pubkey: Address::new_from_array([7; 32]),
        lamports: 42,
        owner: Address::new_from_array([8; 32]),
        executable: false,
        rent_epoch: 9,
        write_version: 10,
        data,
    };
    writer.write_reencoded_post_update(&update).unwrap();

    // Once post-transaction output has begun, accepting another transaction
    // would silently move it ahead of the already-staged update on the wire.
    assert!(matches!(
        writer.write_transaction(&Transaction::new_boxed()),
        Err(ArchiveFormatError::InvalidContainerLayout(
            "transaction cannot follow a post-transaction orphan update"
        ))
    ));

    let meta = BlockMeta::new_boxed();
    writer.end_slot(&meta, &[]).unwrap();
    let (source, _) = writer.finish().unwrap();
    let source_slots = read_all(&source, 0, u64::MAX, true);
    assert_eq!(source_slots.len(), 1);
    assert!(source_slots[0].txs.is_empty());
    let source_meta = source_slots[0].meta.as_ref().unwrap();
    assert!(source_meta.pre.is_empty());
    assert_eq!(source_meta.post, [(10, data.to_vec())]);

    let (output, stats) = reencode_archive(
        std::io::Cursor::new(&source),
        Vec::new(),
        ReencodeOptions::default(),
    )
    .unwrap();
    assert_eq!(read_all(&output, 0, u64::MAX, true), source_slots);
    assert_eq!(
        stats.source_semantic_sha256,
        archive_semantic_sha256(&output)
    );
}

#[test]
fn reencode_rejects_source_parent_hash_break() {
    let mut writer = ArchiveWriter::new(
        std::io::Cursor::new(Vec::new()),
        900,
        1_000,
        2,
        ArchiveWriterConfig {
            compression: Compression::None,
            bucket_slots: 2,
            ..ArchiveWriterConfig::default()
        },
    )
    .unwrap();

    let first_hash = Hash::new_from_array([1; 32]);
    writer.begin_slot(1_000).unwrap();
    {
        let mut meta = BlockMeta::new_boxed();
        meta.slot = 1_000;
        meta.parent_slot = 999;
        meta.blockhash = first_hash;
        writer.end_slot(&meta, &[]).unwrap();
    }

    writer.begin_slot(1_001).unwrap();
    {
        let mut meta = BlockMeta::new_boxed();
        meta.slot = 1_001;
        meta.parent_slot = 1_000;
        meta.parent_blockhash = Hash::new_from_array([9; 32]);
        meta.blockhash = Hash::new_from_array([2; 32]);
        writer.end_slot(&meta, &[]).unwrap();
    }
    let (source, _) = writer.finish().unwrap();

    let error = reencode_archive(
        std::io::Cursor::new(source.into_inner()),
        std::io::Cursor::new(Vec::new()),
        ReencodeOptions::default(),
    )
    .unwrap_err();
    assert!(matches!(
        error,
        ArchiveFormatError::PohMismatch { slot: 1_001 }
    ));
}

#[test]
fn sparse_reencode_preserves_selected_bucket_frames_and_poh_anchors() {
    let config = ArchiveWriterConfig {
        format: ArchiveVersion::V1,
        diff_policy: lencode::diff::DiffPolicy::Adaptive,
        ..ArchiveWriterConfig::default()
    };
    let (source, expected, _) = write_archive(1_000, 300, config.clone());
    let source_frames = bucket_frames(&source);
    let selected_source_bytes = (source_frames[0].len() + source_frames[2].len()) as u64;

    let (sink, stats) = reencode_archive(
        std::io::Cursor::new(&source),
        std::io::Cursor::new(Vec::new()),
        ReencodeOptions {
            writer: config,
            // Deliberately unordered with a duplicate: normalization must be
            // deterministic, and bucket 2 is not adjacent to bucket 0.
            buckets: BucketSelection::Indices(vec![2, 0, 2]),
        },
    )
    .unwrap();
    let output = sink.into_inner();
    let output_frames = bucket_frames(&output);

    assert_eq!(output_frames.len(), 2);
    assert_eq!(output_frames[0], source_frames[0]);
    assert_eq!(output_frames[1], source_frames[2]);
    assert_eq!(stats.source_buckets, 2);
    assert_eq!(stats.source_bucket_bytes, selected_source_bytes);
    assert_eq!(stats.output.bucket_bytes_written, selected_source_bytes);
    assert_eq!(stats.slots_reencoded, 128 + 44);

    let expected: Vec<_> = expected
        .into_iter()
        .filter(|slot| matches!((slot.slot - 1_000) / 128, 0 | 2))
        .collect();
    // Verification proves bucket 2 retained its source PoH anchor instead
    // of incorrectly chaining to the last block in selected bucket 0.
    assert_eq!(read_all(&output, 0, u64::MAX, true), expected);
}

#[test]
fn full_reencode_can_change_outer_compression_without_changing_payload() {
    let (source, expected, _) = write_archive(1_000, 160, ArchiveWriterConfig::default());
    let (sink, _) = reencode_archive(
        std::io::Cursor::new(&source),
        std::io::Cursor::new(Vec::new()),
        ReencodeOptions {
            writer: ArchiveWriterConfig {
                compression: Compression::None,
                ..ArchiveWriterConfig::default()
            },
            buckets: BucketSelection::All,
        },
    )
    .unwrap();
    let output = sink.into_inner();

    assert_eq!(read_all(&output, 0, u64::MAX, true), expected);
    let source_frames = bucket_frames(&source);
    let output_frames = bucket_frames(&output);
    assert_eq!(source_frames.len(), output_frames.len());
    for (source, output) in source_frames.into_iter().zip(output_frames) {
        let (source_header, source_payload) = decoded_bucket_payload(source);
        let (output_header, output_payload) = decoded_bucket_payload(output);
        assert_eq!(source_header.first_slot, output_header.first_slot);
        assert_eq!(source_header.slot_count, output_header.slot_count);
        assert_eq!(source_header.poh_start_hash, output_header.poh_start_hash);
        assert_eq!(source_payload, output_payload);
    }
}

#[test]
fn full_reencode_can_change_bucket_geometry() {
    let (source, expected, _) = write_archive(1_000, 300, ArchiveWriterConfig::default());
    let (output, stats) = reencode_archive(
        std::io::Cursor::new(&source),
        Vec::new(),
        ReencodeOptions {
            writer: ArchiveWriterConfig {
                bucket_slots: 64,
                ..ArchiveWriterConfig::default()
            },
            buckets: BucketSelection::All,
        },
    )
    .unwrap();

    assert_eq!(parse_file_header(&output).unwrap().0.bucket_slots, 64);
    assert_eq!(stats.slots_reencoded, 300);
    assert_eq!(stats.output.buckets, 5);
    assert_eq!(read_all(&output, 0, u64::MAX, true), expected);
    assert_eq!(
        stats.source_semantic_sha256,
        archive_semantic_sha256(&output)
    );
}

#[test]
fn full_reencode_preserves_nonzero_initial_poh_anchor_when_rebucketing() {
    let slot_start = 1_000;
    let slot_count = 6;
    let initial_anchor = Hash::new_from_array([0xa5; 32]);
    let mut writer = ArchiveWriter::new(
        Vec::new(),
        900,
        slot_start,
        slot_count,
        ArchiveWriterConfig {
            bucket_slots: 4,
            compression: Compression::None,
            ..ArchiveWriterConfig::default()
        },
    )
    .unwrap();

    let mut last_blockhash = initial_anchor;
    for offset in 0..slot_count {
        let slot = slot_start + offset;
        if offset < 3 {
            writer.write_skipped_slot(slot).unwrap();
            if offset == 0 {
                writer
                    .preserve_current_bucket_poh_anchor(slot_start, initial_anchor)
                    .unwrap();
            }
            continue;
        }

        writer.begin_slot(slot).unwrap();
        let mut meta = BlockMeta::new_boxed();
        meta.slot = slot;
        meta.parent_slot = slot.saturating_sub(1);
        meta.parent_blockhash = last_blockhash;
        meta.blockhash = Hash::new_from_array([offset as u8 + 1; 32]);
        writer.end_slot(&meta, &[]).unwrap();
        last_blockhash = meta.blockhash;
    }
    let (source, _) = writer.finish().unwrap();
    let (source_header, _) = decoded_bucket_payload(bucket_frames(&source)[0]);
    assert_eq!(source_header.poh_start_hash, initial_anchor);
    let expected = read_all(&source, 0, u64::MAX, true);

    // Splitting exercises the case where an all-skipped destination bucket
    // flushes before the first source block reveals its parent hash. Merging
    // proves the same anchor survives in the opposite direction.
    for bucket_slots in [2, 8] {
        let (output, stats) = reencode_archive(
            std::io::Cursor::new(&source),
            Vec::new(),
            ReencodeOptions {
                writer: ArchiveWriterConfig {
                    bucket_slots,
                    compression: Compression::None,
                    ..ArchiveWriterConfig::default()
                },
                buckets: BucketSelection::All,
            },
        )
        .unwrap();

        let (output_header, _) = decoded_bucket_payload(bucket_frames(&output)[0]);
        assert_eq!(output_header.poh_start_hash, initial_anchor);
        assert_eq!(read_all(&output, 0, u64::MAX, true), expected);
        assert_eq!(
            stats.source_semantic_sha256,
            archive_semantic_sha256(&output)
        );
    }
}

#[test]
fn sparse_reencode_rejects_changed_bucket_geometry() {
    let (source, _, _) = write_archive(1_000, 300, ArchiveWriterConfig::default());
    let error = reencode_archive(
        std::io::Cursor::new(&source),
        std::io::Cursor::new(Vec::new()),
        ReencodeOptions {
            writer: ArchiveWriterConfig {
                bucket_slots: 64,
                ..ArchiveWriterConfig::default()
            },
            buckets: BucketSelection::Indices(vec![1]),
        },
    )
    .unwrap_err();
    assert!(matches!(
        error,
        ArchiveFormatError::ReencodeBucketSizeMismatch {
            source_bucket_slots: 128,
            destination_bucket_slots: 64,
        }
    ));
}

#[test]
fn all_stored_buckets_are_not_complete_when_source_has_holes() {
    let config = ArchiveWriterConfig::default();
    let (dense, _, _) = write_archive(1_000, 300, config.clone());
    let (sparse, _) = reencode_archive(
        std::io::Cursor::new(dense),
        Vec::new(),
        ReencodeOptions {
            writer: config,
            buckets: BucketSelection::Indices(vec![0, 2]),
        },
    )
    .unwrap();

    let error = reencode_archive(
        std::io::Cursor::new(sparse),
        Vec::new(),
        ReencodeOptions {
            writer: ArchiveWriterConfig {
                bucket_slots: 64,
                ..ArchiveWriterConfig::default()
            },
            buckets: BucketSelection::All,
        },
    )
    .unwrap_err();
    assert!(matches!(
        error,
        ArchiveFormatError::ReencodeBucketSizeMismatch {
            source_bucket_slots: 128,
            destination_bucket_slots: 64,
        }
    ));
}

/// Drives the source-agnostic path the horizon firehose uses: parse the
/// framing from raw byte ranges (as a network reader would fetch them),
/// then feed each bucket's bytes to one reused `BucketDecoder`. The decoded
/// result must match `ArchiveReader` exactly.
#[test]
fn bucket_decoder_matches_archive_reader() {
    let (bytes, expected, _) = write_archive(1_000, 300, ArchiveWriterConfig::default());

    let (header, _hlen) = parse_file_header(&bytes[..]).unwrap();
    let footer = Footer::from_bytes(bytes[bytes.len() - FOOTER_LEN..].try_into().unwrap()).unwrap();
    let index = parse_bucket_index(
        &bytes[footer.index_offset as usize..(footer.index_offset + footer.index_len) as usize],
        &footer,
    )
    .unwrap();

    let mut reader = ArchiveReader::open(std::io::Cursor::new(&bytes[..])).unwrap();
    assert_eq!(index.len(), reader.bucket_count());
    let mut reader_collector = Collector::default();
    reader
        .read_slots(0, u64::MAX, &mut reader_collector)
        .unwrap();

    let mut decoder = BucketDecoder::for_file_header(&header).unwrap();
    decoder.verify_chain = true;
    let mut collector = Collector::default();
    for entry in &index {
        let raw = &bytes[entry.offset as usize..(entry.offset + entry.len) as usize];
        decoder
            .decode_bucket(raw, 0, u64::MAX, &mut collector)
            .unwrap();
    }
    assert_eq!(collector.slots, expected);

    // Payload byte tally: the standalone decoder and the ArchiveReader (which
    // wraps a BucketDecoder) must agree, every category is populated for this
    // synthetic archive, and the categories sum to the total.
    let stats = decoder.byte_stats();
    assert_eq!(stats, reader.payload_byte_stats());
    assert!(stats.transaction_bytes > 0);
    assert!(stats.account_update_bytes > 0);
    assert!(stats.other_bytes > 0);
    assert_eq!(
        stats.total(),
        stats.transaction_bytes + stats.account_update_bytes + stats.other_bytes
    );

    // Mid-bucket slot maps to the right bucket (bucket 1 covers 1_128..1_256).
    assert_eq!(bucket_containing(&header, &index, 1_171), 1);
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
fn roundtrip_v2_lz4() {
    let config = ArchiveWriterConfig {
        compression: Compression::Lz4,
        ..Default::default()
    };
    let (bytes, expected, _) = write_archive(1_000, 300, config);
    let header = parse_file_header(&bytes).unwrap().0;
    assert_eq!(header.format_version, FORMAT_VERSION_V2);
    assert_eq!(header.flags, FLAG_DEDUPE_UNSIGNED_LEB128);
    assert!(bucket_frames(&bytes).iter().any(|raw| {
        let mut cursor = lencode::io::Cursor::new(*raw);
        BucketHeader::decode_ext(&mut cursor, None)
            .is_ok_and(|header| header.compression == Compression::Lz4)
    }));
    assert_eq!(read_all(&bytes, 0, u64::MAX, true), expected);
}

#[test]
fn stored_bucket_limit_covers_codec_worst_case_bounds() {
    let raw = MAX_BUCKET_UNCOMPRESSED_BYTES as usize;
    let zstd_bound = zstd::zstd_safe::compress_bound(raw) as u64;
    let lz4_bound = lz4::block::compress_bound(raw).unwrap() as u64;
    // Bucket headers are currently 59 bytes; keep a much wider framing
    // allowance so adding bounded header fields cannot invalidate the limit.
    let framing_allowance = 4 << 10;
    assert!(zstd_bound + framing_allowance <= MAX_BUCKET_STORED_BYTES);
    assert!(lz4_bound + framing_allowance <= MAX_BUCKET_STORED_BYTES);
}

#[test]
fn frame_work_limit_is_independent_of_resident_bucket_limit() {
    assert!(MAX_FRAME_CUMULATIVE_DECODE_BYTES > MAX_BUCKET_UNCOMPRESSED_BYTES as usize);
    assert_eq!(MAX_FRAME_CUMULATIVE_DECODE_BYTES, 6 << 30);
    assert_eq!(MAX_BUCKET_CUMULATIVE_DECODE_BYTES, 64 << 30);
}

#[test]
fn v1_rejects_v2_only_codecs() {
    let lz4_error = ArchiveWriter::new(
        std::io::Cursor::new(Vec::new()),
        900,
        1_000,
        10,
        ArchiveWriterConfig {
            format: ArchiveVersion::V1,
            compression: Compression::Lz4,
            diff_policy: lencode::diff::DiffPolicy::Adaptive,
            ..Default::default()
        },
    )
    .err()
    .unwrap();
    assert!(matches!(
        lz4_error,
        ArchiveFormatError::UnsupportedCompressionForVersion { .. }
    ));

    let diff_error = ArchiveWriter::new(
        std::io::Cursor::new(Vec::new()),
        900,
        1_000,
        10,
        ArchiveWriterConfig {
            format: ArchiveVersion::V1,
            compression: Compression::Zstd,
            diff_policy: lencode::diff::DiffPolicy::OuterCompressed,
            ..Default::default()
        },
    )
    .err()
    .unwrap();
    assert!(matches!(
        diff_error,
        ArchiveFormatError::UnsupportedDiffPolicyForVersion { .. }
    ));
}

#[test]
fn decoder_rejects_valid_checksum_with_trailing_payload() {
    let (bytes, _, _) = write_archive(
        1_000,
        20,
        ArchiveWriterConfig {
            compression: Compression::None,
            ..Default::default()
        },
    );
    let frame = bucket_frames(&bytes)[0];
    let mut cursor = lencode::io::Cursor::new(frame);
    let mut header = BucketHeader::decode_ext(&mut cursor, None).unwrap();
    let mut payload = frame[cursor.position()..].to_vec();
    payload.push(0xA5);
    header.uncompressed_len += 1;
    header.stored_len += 1;
    header.xxh64 = xxhash_rust::xxh64::xxh64(&payload, 0);
    let mut tampered = Vec::new();
    header.encode_ext(&mut tampered, None).unwrap();
    tampered.extend_from_slice(&payload);

    let mut decoder = BucketDecoder::for_archive_version(ArchiveVersion::V2);
    let error = decoder
        .decode_bucket(&tampered, 0, u64::MAX, &mut Collector::default())
        .unwrap_err();
    assert!(matches!(
        error,
        ArchiveFormatError::TrailingBucketBytes { bytes: 1, .. }
    ));
}

#[test]
fn v1_decoder_rejects_lz4_bucket() {
    let (bytes, _, _) = write_archive(
        1_000,
        20,
        ArchiveWriterConfig {
            compression: Compression::Lz4,
            ..Default::default()
        },
    );
    let frame = bucket_frames(&bytes)[0];
    let mut decoder = BucketDecoder::for_archive_version(ArchiveVersion::V1);
    let error = decoder.load_bucket_bytes(frame).unwrap_err();
    assert!(matches!(
        error,
        ArchiveFormatError::UnsupportedCompressionForVersion { .. }
    ));
}

#[test]
fn v1_decoder_rejects_outer_diff_with_and_without_materialization() {
    let sink = std::io::Cursor::new(Vec::new());
    let mut writer = ArchiveWriter::new(
        sink,
        900,
        1_000,
        2,
        ArchiveWriterConfig {
            compression: Compression::None,
            ..Default::default()
        },
    )
    .unwrap();
    let pubkey = Address::new_from_array(POPULAR_PUBKEYS[0]);
    let owner = Address::new_from_array(POPULAR_PUBKEYS[1]);
    let mut data = vec![0x5A; 256];
    let mut parent = Hash::default();
    for slot in 1_000..1_002 {
        writer.begin_slot(slot).unwrap();
        if slot == 1_001 {
            for byte in &mut data {
                *byte ^= 0xFF;
            }
        }
        writer
            .write_orphan_update(&AccountUpdateView {
                pubkey,
                lamports: 1,
                owner,
                executable: false,
                rent_epoch: 0,
                write_version: slot,
                data: &data,
            })
            .unwrap();
        let blockhash = Hash::new_from_array([slot as u8; 32]);
        let mut meta = BlockMeta::new_boxed();
        meta.slot = slot;
        meta.parent_slot = slot - 1;
        meta.parent_blockhash = parent;
        meta.blockhash = blockhash;
        writer.end_slot(&meta, &[]).unwrap();
        parent = blockhash;
    }
    let (sink, _) = writer.finish().unwrap();
    let bytes = sink.into_inner();
    let frame = bucket_frames(&bytes)[0];

    for materialize in [true, false] {
        let mut decoder = BucketDecoder::for_archive_version(ArchiveVersion::V1);
        decoder.materialize_account_data = materialize;
        let error = decoder
            .decode_bucket(frame, 0, u64::MAX, &mut Collector::default())
            .unwrap_err();
        assert!(matches!(error, ArchiveFormatError::Encode(_)));
    }
}

#[test]
fn decoder_rejects_zero_slot_bucket_and_index_header_mismatch() {
    let header = BucketHeader {
        first_slot: 1_000,
        slot_count: 0,
        compression: Compression::None,
        uncompressed_len: 0,
        stored_len: 0,
        xxh64: xxhash_rust::xxh64::xxh64(&[], 0),
        poh_start_hash: Hash::default(),
    };
    let mut raw = Vec::new();
    header.encode_ext(&mut raw, None).unwrap();
    let mut decoder = BucketDecoder::for_archive_version(ArchiveVersion::V2);
    assert!(matches!(
        decoder.load_bucket_bytes(&raw),
        Err(ArchiveFormatError::InvalidBucketSlotCount { slot_count: 0, .. })
    ));

    let (bytes, _, _) = write_archive(1_000, 20, ArchiveWriterConfig::default());
    let frame = bucket_frames(&bytes)[0];
    let mut decoder = BucketDecoder::for_archive_version(ArchiveVersion::V2);
    let error = decoder
        .load_indexed_bucket_bytes(
            frame,
            BucketIndexEntry {
                first_slot: 999,
                offset: 0,
                len: frame.len() as u64,
            },
        )
        .unwrap_err();
    assert!(matches!(
        error,
        ArchiveFormatError::BucketHeaderMismatch {
            indexed: 999,
            decoded: 1_000
        }
    ));
}

#[test]
fn decoder_rejects_nonconsecutive_slot_frame() {
    let mut payload = Vec::new();
    1_001u64.encode_ext(&mut payload, None).unwrap();
    payload.push(SlotKind::Skipped as u8);
    let header = BucketHeader {
        first_slot: 1_000,
        slot_count: 1,
        compression: Compression::None,
        uncompressed_len: payload.len() as u64,
        stored_len: payload.len() as u64,
        xxh64: xxhash_rust::xxh64::xxh64(&payload, 0),
        poh_start_hash: Hash::default(),
    };
    let mut raw = Vec::new();
    header.encode_ext(&mut raw, None).unwrap();
    raw.extend_from_slice(&payload);
    let mut decoder = BucketDecoder::for_archive_version(ArchiveVersion::V2);
    let error = decoder
        .decode_bucket(&raw, 0, u64::MAX, &mut Collector::default())
        .unwrap_err();
    assert!(matches!(
        error,
        ArchiveFormatError::UnexpectedBucketSlot {
            expected: 1_000,
            decoded: 1_001
        }
    ));
}

#[test]
fn container_varint_rejects_noncanonical_lengths() {
    for malformed in [
        &[0x80][..],
        &[0x89][..],
        &[0x81, 0x7F][..],
        &[0x82, 0x80, 0x00][..],
    ] {
        assert!(
            read_io_varint(&mut std::io::Cursor::new(malformed)).is_err(),
            "accepted {malformed:02x?}"
        );
    }
    assert_eq!(
        read_io_varint(&mut std::io::Cursor::new([0x81, 0x80])).unwrap(),
        128
    );
}

#[test]
fn decoder_rejects_noncanonical_epoch_presence_flag() {
    let (bytes, _, _) = write_archive(
        1_000,
        1,
        ArchiveWriterConfig {
            compression: Compression::None,
            ..Default::default()
        },
    );
    let frame = bucket_frames(&bytes)[0];
    let mut header_cursor = lencode::io::Cursor::new(frame);
    let mut header = BucketHeader::decode_ext(&mut header_cursor, None).unwrap();
    let mut payload = frame[header_cursor.position()..].to_vec();

    let mut payload_cursor = lencode::io::Cursor::new(payload.as_slice());
    assert_eq!(u64::decode_ext(&mut payload_cursor, None).unwrap(), 1_000);
    let kind_offset = payload_cursor.position();
    assert_eq!(payload[kind_offset], SlotKind::Block as u8);
    assert_eq!(payload[kind_offset + 1], 0);
    payload[kind_offset + 1] = 2;

    header.xxh64 = xxhash_rust::xxh64::xxh64(&payload, 0);
    let mut tampered = Vec::new();
    header.encode_ext(&mut tampered, None).unwrap();
    tampered.extend_from_slice(&payload);

    let mut decoder = BucketDecoder::for_archive_version(ArchiveVersion::V2);
    assert!(matches!(
        decoder.decode_bucket(&tampered, 0, u64::MAX, &mut Collector::default()),
        Err(ArchiveFormatError::InvalidContainerLayout(
            "epoch presence flag must be 0 or 1"
        ))
    ));
}

#[test]
fn decoder_rejects_checksum_valid_trailing_zstd_frame() {
    let (bytes, _, _) = write_archive(1_000, 20, ArchiveWriterConfig::default());
    let frame = bucket_frames(&bytes)[0];
    let mut cursor = lencode::io::Cursor::new(frame);
    let mut header = BucketHeader::decode_ext(&mut cursor, None).unwrap();
    assert_eq!(header.compression, Compression::Zstd);
    let mut stored = frame[cursor.position()..].to_vec();
    // Empty zstd skippable frame: magic 0x184D2A50 + little-endian size 0.
    stored.extend_from_slice(&[0x50, 0x2A, 0x4D, 0x18, 0, 0, 0, 0]);
    header.stored_len = stored.len() as u64;
    header.xxh64 = xxhash_rust::xxh64::xxh64(&stored, 0);
    let mut tampered = Vec::new();
    header.encode_ext(&mut tampered, None).unwrap();
    tampered.extend_from_slice(&stored);

    let mut decoder = BucketDecoder::for_archive_version(ArchiveVersion::V2);
    assert!(matches!(
        decoder.load_bucket_bytes(&tampered),
        Err(ArchiveFormatError::TrailingBucketBytes { bytes: 8, .. })
    ));
}

#[test]
fn decoder_rejects_checksum_valid_trailing_lz4_bytes() {
    let (bytes, _, _) = write_archive(
        1_000,
        20,
        ArchiveWriterConfig {
            compression: Compression::Lz4,
            ..Default::default()
        },
    );
    let frame = bucket_frames(&bytes)[0];
    let mut cursor = lencode::io::Cursor::new(frame);
    let mut header = BucketHeader::decode_ext(&mut cursor, None).unwrap();
    assert_eq!(header.compression, Compression::Lz4);
    let mut stored = frame[cursor.position()..].to_vec();
    stored.push(0);
    header.stored_len = stored.len() as u64;
    header.xxh64 = xxhash_rust::xxh64::xxh64(&stored, 0);
    let mut tampered = Vec::new();
    header.encode_ext(&mut tampered, None).unwrap();
    tampered.extend_from_slice(&stored);

    let mut decoder = BucketDecoder::for_archive_version(ArchiveVersion::V2);
    assert!(matches!(
        decoder.load_bucket_bytes(&tampered),
        Err(ArchiveFormatError::Io(error)) if error.kind() == std::io::ErrorKind::InvalidData
    ));
}

#[test]
fn decoder_rejects_oversized_manual_section_count_before_looping() {
    let mut payload = Vec::new();
    1_000u64.encode_ext(&mut payload, None).unwrap();
    payload.push(SlotKind::Block as u8);
    payload.push(0); // no epoch notification
    0u64.encode_ext(&mut payload, None).unwrap(); // no pre-updates
    (u64::from(u32::MAX) + 1)
        .encode_ext(&mut payload, None)
        .unwrap(); // transaction count must not truncate to zero

    let header = BucketHeader {
        first_slot: 1_000,
        slot_count: 1,
        compression: Compression::None,
        uncompressed_len: payload.len() as u64,
        stored_len: payload.len() as u64,
        xxh64: xxhash_rust::xxh64::xxh64(&payload, 0),
        poh_start_hash: Hash::default(),
    };
    let mut raw = Vec::new();
    header.encode_ext(&mut raw, None).unwrap();
    raw.extend_from_slice(&payload);

    let mut decoder = BucketDecoder::for_archive_version(ArchiveVersion::V2);
    assert!(matches!(
        decoder.decode_bucket(&raw, 0, u64::MAX, &mut Collector::default()),
        Err(ArchiveFormatError::Encode(
            lencode::io::Error::DecodeLimitExceeded
        ))
    ));
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
            diff_policy: lencode::diff::DiffPolicy::Adaptive,
            ..Default::default()
        },
    );
    let per_slot = write_archive(
        0,
        256,
        ArchiveWriterConfig {
            compression: Compression::None,
            bucket_slots: 1,
            diff_policy: lencode::diff::DiffPolicy::Adaptive,
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
    // reload the bucket on each call; the reader continues in place.
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

    // Re-encoding must replay the epoch callback before the pre-update and
    // preserve the complete bucket frame, including the epoch-owned update.
    let (reencoded, reencode_stats) = reencode_archive(
        std::io::Cursor::new(&bytes),
        std::io::Cursor::new(Vec::new()),
        ReencodeOptions::default(),
    )
    .unwrap();
    let reencoded = reencoded.into_inner();
    let mut reader = ArchiveReader::open(std::io::Cursor::new(&reencoded[..])).unwrap();
    let mut reencoded_collector = Collector::default();
    reader
        .read_slots(0, u64::MAX, &mut reencoded_collector)
        .unwrap();
    assert_eq!(reencoded_collector.epochs, collector.epochs);
    assert_eq!(reencoded_collector.slots, collector.slots);
    assert_eq!(bucket_frames(&reencoded), bucket_frames(&bytes));
    assert_eq!(reencode_stats.output.epochs, 1);
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
    writer.write_skipped_slot(1_000).unwrap();
    let err = writer.write_skipped_slot(1_000);
    assert!(matches!(
        err,
        Err(ArchiveFormatError::NonMonotonicSlot { .. })
    ));
}

#[test]
fn writer_enforces_decodable_bucket_slot_geometry() {
    let config = ArchiveWriterConfig {
        bucket_slots: 4,
        ..ArchiveWriterConfig::default()
    };

    let mut unaligned = ArchiveWriter::new(Vec::new(), 900, 1_000, 20, config.clone()).unwrap();
    assert!(matches!(
        unaligned.write_skipped_slot(1_001),
        Err(ArchiveFormatError::UnexpectedBucketSlot {
            expected: 1_000,
            decoded: 1_001,
        })
    ));

    let mut gapped = ArchiveWriter::new(Vec::new(), 900, 1_000, 20, config.clone()).unwrap();
    gapped.write_skipped_slot(1_000).unwrap();
    assert!(matches!(
        gapped.write_skipped_slot(1_002),
        Err(ArchiveFormatError::UnexpectedBucketSlot {
            expected: 1_001,
            decoded: 1_002,
        })
    ));
    // Rejection happens before writer state changes, so the missing slot can
    // still be supplied and the archive completed normally.
    gapped.write_skipped_slot(1_001).unwrap();
    let (bytes, _) = gapped.finish().unwrap();
    ArchiveReader::open(std::io::Cursor::new(bytes)).unwrap();

    // Omitting whole buckets remains valid for deterministic sparse samples.
    let mut sparse = ArchiveWriter::new(Vec::new(), 900, 1_000, 20, config).unwrap();
    sparse.write_skipped_slot(1_000).unwrap();
    sparse.write_skipped_slot(1_008).unwrap();
    let (bytes, _) = sparse.finish().unwrap();
    let mut reader = ArchiveReader::open(std::io::Cursor::new(bytes)).unwrap();
    reader.verify_chain = true;
    let mut collector = Collector::default();
    assert_eq!(reader.read_slots(0, u64::MAX, &mut collector).unwrap(), 2);
    assert_eq!(
        collector
            .slots
            .into_iter()
            .map(|slot| slot.slot)
            .collect::<Vec<_>>(),
        [1_000, 1_008]
    );
}

#[test]
fn writer_rejects_zero_bucket_slots_without_panicking() {
    let result = ArchiveWriter::new(
        Vec::new(),
        900,
        1_000,
        20,
        ArchiveWriterConfig {
            bucket_slots: 0,
            ..ArchiveWriterConfig::default()
        },
    );
    assert!(matches!(
        result,
        Err(ArchiveFormatError::InvalidContainerLayout(
            "bucket_slots must be nonzero"
        ))
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

    // Pattern B (old behavior): fresh reader per window, so every call
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

// --- account-update skip-materialization (declared consumption) ---

/// Forwarding visitor that declares it does not consume account-update
/// data, exercising the visitor-declared consumption path end to end.
struct DeclineData<V>(V);

impl<V: SlotVisitor> SlotVisitor for DeclineData<V> {
    fn on_slot_start(&mut self, slot: u64, kind: SlotKind) {
        self.0.on_slot_start(slot, kind);
    }
    fn on_epoch(&mut self, meta: &EpochMeta) {
        self.0.on_epoch(meta);
    }
    fn on_pre_account_update(&mut self, slot: u64, update: &AccountUpdateView<'_>) {
        self.0.on_pre_account_update(slot, update);
    }
    fn on_transaction(&mut self, slot: u64, tx_index: u32, tx: &Transaction) {
        self.0.on_transaction(slot, tx_index, tx);
    }
    fn on_post_account_update(&mut self, slot: u64, update: &AccountUpdateView<'_>) {
        self.0.on_post_account_update(slot, update);
    }
    fn on_block(&mut self, notification: &BlockNotification, entries: &[EntryRecord]) {
        self.0.on_block(notification, entries);
    }
    fn consumption(&self) -> Consumption {
        Consumption::all().without_account_update_data()
    }
}

/// `expected` with every update's data elided (metadata and counts kept),
/// exactly what a consumption-declining decode must produce.
fn elide_update_data(expected: &[ExpectedSlot]) -> Vec<ExpectedSlot> {
    expected
        .iter()
        .cloned()
        .map(|mut s| {
            if let Some(meta) = &mut s.meta {
                for (_, d) in meta.pre.iter_mut() {
                    d.clear();
                }
                for (_, d) in meta.post.iter_mut() {
                    d.clear();
                }
            }
            for tx in s.txs.iter_mut() {
                tx.3.clear();
            }
            s
        })
        .collect()
}

/// Skip mode must leave every non-update field of the stream byte-identical
/// (fees, sigs, loaded addresses, block meta, entries, update counts and
/// write_versions) while eliding update data across all three update
/// kinds (tx-owned, pre/post orphans) and leader-skipped slots. Byte
/// tallies are cursor-position deltas so they must agree between modes.
#[test]
fn skip_materialization_preserves_streams_and_elides_data() {
    let (bytes, expected, _) = write_archive(1_000, 300, ArchiveWriterConfig::default());
    let expected_skip = elide_update_data(&expected);
    assert_ne!(expected, expected_skip); // archive really contains update data

    let mut skip_reader = ArchiveReader::open(std::io::Cursor::new(&bytes[..])).unwrap();
    skip_reader.verify_chain = true;
    let mut skip_visitor = DeclineData(Collector::default());
    skip_reader
        .read_slots(0, u64::MAX, &mut skip_visitor)
        .unwrap();
    assert_eq!(skip_visitor.0.slots, expected_skip);

    let mut full_reader = ArchiveReader::open(std::io::Cursor::new(&bytes[..])).unwrap();
    full_reader.verify_chain = true;
    let mut full_collector = Collector::default();
    full_reader
        .read_slots(0, u64::MAX, &mut full_collector)
        .unwrap();
    assert_eq!(full_collector.slots, expected);
    assert_eq!(
        skip_reader.payload_byte_stats(),
        full_reader.payload_byte_stats()
    );
}

/// Same guarantee through the source-agnostic `BucketDecoder` path (the
/// horizon firehose driver), controlled by the decoder flag directly.
#[test]
fn skip_materialization_bucket_decoder_path() {
    let (bytes, expected, _) = write_archive(1_000, 300, ArchiveWriterConfig::default());
    let expected_skip = elide_update_data(&expected);
    let (header, _) = parse_file_header(&bytes).unwrap();

    let footer = Footer::from_bytes(bytes[bytes.len() - FOOTER_LEN..].try_into().unwrap()).unwrap();
    let index = parse_bucket_index(
        &bytes[footer.index_offset as usize..(footer.index_offset + footer.index_len) as usize],
        &footer,
    )
    .unwrap();

    let mut decoder = BucketDecoder::for_file_header(&header).unwrap();
    decoder.materialize_account_data = false;
    let mut collector = Collector::default();
    for entry in &index {
        let raw = &bytes[entry.offset as usize..(entry.offset + entry.len) as usize];
        decoder
            .decode_bucket(raw, 0, u64::MAX, &mut collector)
            .unwrap();
        assert!(!decoder.bucket_materializes_account_data());
    }
    assert_eq!(collector.slots, expected_skip);
}

/// A reader whose loaded bucket was latched under one materialization mode
/// must reload (not continue in place) when the next visitor wants the
/// other mode; otherwise a bytes-wanting visitor would resume inside a
/// bucket whose diff store was never populated.
#[test]
fn switching_consumption_mid_bucket_reloads_under_new_mode() {
    let (bytes, expected, _) = write_archive(1_000, 120, ArchiveWriterConfig::default());

    let mut reader = ArchiveReader::open(std::io::Cursor::new(&bytes[..])).unwrap();
    // First: decode part of bucket 0 with update data declined.
    let mut skip_visitor = DeclineData(Collector::default());
    reader.read_slots(1_000, 50, &mut skip_visitor).unwrap();
    assert!(!skip_visitor.0.slots.is_empty());

    // Then: continue mid-bucket with a full-consumption visitor. The reader
    // must reload the bucket under materialization and reproduce every
    // update byte exactly.
    let mut full_collector = Collector::default();
    reader
        .read_slots(1_050, u64::MAX, &mut full_collector)
        .unwrap();
    let want: Vec<ExpectedSlot> = expected
        .iter()
        .filter(|s| s.slot >= 1_050)
        .cloned()
        .collect();
    assert_eq!(full_collector.slots, want);

    // And back: a declining visitor after a materializing one also reloads
    // (stale materialized state is harmless, but the latch comparison is
    // exact, so verify the elided shape too).
    let mut skip_again = DeclineData(Collector::default());
    reader.read_slots(1_010, 5, &mut skip_again).unwrap();
    let want_skip: Vec<ExpectedSlot> = elide_update_data(&expected)
        .into_iter()
        .filter(|s| s.slot >= 1_010)
        .take(5)
        .collect();
    assert_eq!(skip_again.0.slots, want_skip);
}

/// Epoch-attributed updates survive skip mode with counts intact and data
/// elided (mirrors `epoch_meta_roundtrips_on_boundary_block`).
#[test]
fn skip_materialization_epoch_updates_counted_data_elided() {
    let sink = std::io::Cursor::new(Vec::new());
    let mut writer =
        ArchiveWriter::new(sink, 900, 1_000, 100, ArchiveWriterConfig::default()).unwrap();

    writer.begin_slot(1_000).unwrap();
    let mut epoch = EpochMeta::new_boxed();
    epoch.epoch = 900;
    epoch.start_slot = 1_000;
    epoch.slot_count = 100;
    epoch.first_block_slot = 1_000;
    epoch
        .updates
        .push(&AccountUpdateView {
            pubkey: Address::new_from_array(POPULAR_PUBKEYS[5]),
            lamports: 1,
            owner: Address::new_from_array(POPULAR_PUBKEYS[2]),
            executable: false,
            rent_epoch: u64::MAX,
            write_version: 7,
            data: b"feature-activation",
        })
        .unwrap();
    writer.write_epoch_meta(&epoch).unwrap();
    writer
        .write_orphan_update(&AccountUpdateView {
            pubkey: Address::new_from_array(POPULAR_PUBKEYS[8]),
            lamports: 1,
            owner: Address::new_from_array(POPULAR_PUBKEYS[2]),
            executable: false,
            rent_epoch: u64::MAX,
            write_version: 2,
            data: b"clock",
        })
        .unwrap();
    let mut meta = BlockMeta::new_boxed();
    meta.slot = 1_000;
    meta.blockhash = Hash::new_from_array([9; 32]);
    writer.end_slot(&meta, &[]).unwrap();
    let (sink, _) = writer.finish().unwrap();
    let bytes = sink.into_inner();

    /// Captures epoch update payload sizes + block pre-update data.
    #[derive(Default)]
    struct EpochCapture {
        epoch_updates: Vec<(u64, usize)>, // (write_version, data len)
        pre: Vec<(u64, Vec<u8>)>,
    }
    impl SlotVisitor for EpochCapture {
        fn on_epoch(&mut self, meta: &EpochMeta) {
            for (m, d) in meta.updates.iter() {
                self.epoch_updates.push((m.write_version, d.len()));
            }
        }
        fn on_block(&mut self, notification: &BlockNotification, _entries: &[EntryRecord]) {
            if let BlockNotification::Block(meta) = notification {
                for (m, d) in meta.pre_updates.iter() {
                    self.pre.push((m.write_version, d.to_vec()));
                }
            }
        }
        fn consumption(&self) -> Consumption {
            Consumption::all().without_account_update_data()
        }
    }

    let mut reader = ArchiveReader::open(std::io::Cursor::new(&bytes[..])).unwrap();
    let mut capture = EpochCapture::default();
    reader.read_slots(0, u64::MAX, &mut capture).unwrap();
    // Counts and write_versions intact; every data slice elided to empty.
    assert_eq!(capture.epoch_updates, vec![(7, 0)]);
    assert_eq!(capture.pre, vec![(2, Vec::new())]);
}

#[test]
fn resource_limits_bucket_work_budget_accumulates_across_slot_frames() {
    let mut writer = ArchiveWriter::new(
        Vec::new(),
        900,
        1_000,
        2,
        ArchiveWriterConfig {
            compression: Compression::None,
            bucket_slots: 2,
            ..Default::default()
        },
    )
    .unwrap();
    let data = vec![7u8; 1_024];
    for slot in 1_000..1_002 {
        writer.begin_slot(slot).unwrap();
        writer
            .write_orphan_update(&AccountUpdateView {
                pubkey: Address::new_from_array([1; 32]),
                lamports: 1,
                owner: Address::new_from_array([2; 32]),
                executable: false,
                rent_epoch: 0,
                write_version: slot,
                data: &data,
            })
            .unwrap();
        let mut meta = BlockMeta::new_boxed();
        meta.slot = slot;
        writer.end_slot(&meta, &[]).unwrap();
    }
    let (archive, _) = writer.finish().unwrap();
    let frame = bucket_frames(&archive)[0];

    let mut decoder = BucketDecoder::for_archive_version(ArchiveVersion::V2);
    decoder.load_bucket_bytes(frame).unwrap();
    decoder
        .decode_slot_frame(0, &mut Collector::default())
        .unwrap();
    let first_frame_work = decoder.bucket_decode_work_bytes();
    assert!(first_frame_work > 0);
    decoder.set_bucket_decode_work_limit_for_test(first_frame_work);
    assert!(matches!(
        decoder.decode_slot_frame(0, &mut Collector::default()),
        Err(ArchiveFormatError::Encode(
            lencode::io::Error::DecodeLimitExceeded
        ))
    ));
}

#[test]
fn resource_limits_skipped_data_enforces_pre_phase_limits() {
    let mut writer = ArchiveWriter::new(
        Vec::new(),
        900,
        1_000,
        1,
        ArchiveWriterConfig {
            compression: Compression::None,
            ..Default::default()
        },
    )
    .unwrap();
    writer.begin_slot(1_000).unwrap();
    for (key, byte) in [(3u8, 3u8), (4, 4)] {
        writer
            .write_orphan_update(&AccountUpdateView {
                pubkey: Address::new_from_array([key; 32]),
                lamports: 1,
                owner: Address::new_from_array([5; 32]),
                executable: false,
                rent_epoch: 0,
                write_version: u64::from(key),
                data: &[byte; 64],
            })
            .unwrap();
    }
    let mut meta = BlockMeta::new_boxed();
    meta.slot = 1_000;
    writer.end_slot(&meta, &[]).unwrap();
    let (archive, _) = writer.finish().unwrap();
    let frame = bucket_frames(&archive)[0];

    for materialize_data in [true, false] {
        for materialize_arena in [true, false] {
            let mut decoder = BucketDecoder::for_archive_version(ArchiveVersion::V2);
            decoder.materialize_account_data = materialize_data;
            decoder.materialize_block_account_update_arenas = materialize_arena;
            decoder.set_pre_update_limits_for_test(2, 100);
            assert!(matches!(
                decoder.decode_bucket(frame, 0, u64::MAX, &mut Collector::default()),
                Err(ArchiveFormatError::SectionTooLarge {
                    section: "pre-transaction account-update data",
                    ..
                })
            ));
        }
    }

    // Count rejection happens before the decoder attempts to read records,
    // and therefore cannot depend on either materialization mode.
    let mut payload = Vec::new();
    1_000u64.encode_ext(&mut payload, None).unwrap();
    payload.push(SlotKind::Block as u8);
    payload.push(0);
    3u64.encode_ext(&mut payload, None).unwrap();
    let header = BucketHeader {
        first_slot: 1_000,
        slot_count: 1,
        compression: Compression::None,
        uncompressed_len: payload.len() as u64,
        stored_len: payload.len() as u64,
        xxh64: xxhash_rust::xxh64::xxh64(&payload, 0),
        poh_start_hash: Hash::default(),
    };
    let mut raw = Vec::new();
    header.encode_ext(&mut raw, None).unwrap();
    raw.extend_from_slice(&payload);
    for materialize_data in [true, false] {
        let mut decoder = BucketDecoder::for_archive_version(ArchiveVersion::V2);
        decoder.materialize_account_data = materialize_data;
        decoder.materialize_block_account_update_arenas = false;
        decoder.set_pre_update_limits_for_test(2, usize::MAX);
        assert!(matches!(
            decoder.decode_bucket(&raw, 0, u64::MAX, &mut Collector::default()),
            Err(ArchiveFormatError::SectionTooLarge {
                section: "pre-transaction updates",
                ..
            })
        ));
    }
}

#[test]
fn resource_limits_all_update_phases_are_consumption_independent() {
    let mut writer = ArchiveWriter::new(
        Vec::new(),
        900,
        1_000,
        1,
        ArchiveWriterConfig {
            compression: Compression::None,
            ..Default::default()
        },
    )
    .unwrap();
    writer.begin_slot(1_000).unwrap();
    let data = [0xA5; 64];
    let make_update = |key: u8| AccountUpdateView {
        pubkey: Address::new_from_array([key; 32]),
        lamports: 1,
        owner: Address::new_from_array([99; 32]),
        executable: false,
        rent_epoch: 0,
        write_version: u64::from(key),
        data: &data,
    };

    let mut epoch = EpochMeta::new_boxed();
    epoch.epoch = 900;
    epoch.start_slot = 1_000;
    epoch.slot_count = 1;
    epoch.first_block_slot = 1_000;
    epoch.updates.push(&make_update(1)).unwrap();
    writer.write_epoch_meta(&epoch).unwrap();
    writer.write_orphan_update(&make_update(2)).unwrap();
    let mut tx = Transaction::new_boxed();
    tx.signatures.push(Signature::default());
    tx.push_account_update(&make_update(3)).unwrap();
    writer.write_transaction(&tx).unwrap();
    writer.write_reencoded_post_update(&make_update(4)).unwrap();
    let mut meta = BlockMeta::new_boxed();
    meta.slot = 1_000;
    writer.end_slot(&meta, &[]).unwrap();
    let (archive, _) = writer.finish().unwrap();
    let frame = bucket_frames(&archive)[0];

    let phases = [
        (0usize, "epoch account updates", "epoch account-update data"),
        (
            1,
            "transaction account updates",
            "transaction account-update data",
        ),
        (
            2,
            "pre-transaction updates",
            "pre-transaction account-update data",
        ),
        (
            3,
            "post-transaction updates",
            "post-transaction account-update data",
        ),
    ];
    for (phase, count_section, data_section) in phases {
        for (count_limit, data_limit, expected_section) in [
            (0, usize::MAX, count_section),
            (usize::MAX, 32, data_section),
        ] {
            for materialize_data in [true, false] {
                for materialize_arena in [true, false] {
                    let mut limits = [
                        (usize::MAX, usize::MAX),
                        (usize::MAX, usize::MAX),
                        (usize::MAX, usize::MAX),
                        (usize::MAX, usize::MAX),
                    ];
                    limits[phase] = (count_limit, data_limit);
                    let mut decoder = BucketDecoder::for_archive_version(ArchiveVersion::V2);
                    decoder.materialize_account_data = materialize_data;
                    decoder.materialize_block_account_update_arenas = materialize_arena;
                    decoder.set_phase_limits_for_test(limits[0], limits[1], limits[2], limits[3]);
                    assert!(matches!(
                        decoder.decode_bucket(frame, 0, u64::MAX, &mut Collector::default()),
                        Err(ArchiveFormatError::SectionTooLarge { section, .. })
                            if section == expected_section
                    ));
                }
            }
        }
    }
}

#[test]
fn resource_limits_writer_bounds_growth_and_poisoned_finish() {
    let mut writer = ArchiveWriter::new(
        Vec::new(),
        900,
        1_000,
        128,
        ArchiveWriterConfig {
            compression: Compression::None,
            ..Default::default()
        },
    )
    .unwrap();
    writer.set_bucket_limit_for_test(256);

    let mut failure = None;
    for slot in 1_000..1_128 {
        if let Err(error) = writer.begin_slot(slot) {
            failure = Some(error);
            break;
        }
        let data = [slot as u8; 96];
        let update = writer.write_orphan_update(&AccountUpdateView {
            pubkey: Address::new_from_array([slot as u8; 32]),
            lamports: 1,
            owner: Address::new_from_array([9; 32]),
            executable: false,
            rent_epoch: 0,
            write_version: slot,
            data: &data,
        });
        if let Err(error) = update {
            failure = Some(error);
            break;
        }
        let mut meta = BlockMeta::new_boxed();
        meta.slot = slot;
        if let Err(error) = writer.end_slot(&meta, &[]) {
            failure = Some(error);
            break;
        }
    }
    assert!(matches!(
        failure,
        Some(ArchiveFormatError::BucketTooLarge { .. })
    ));
    assert!(writer.retained_payload_capacity_for_test() <= 512);
    assert!(matches!(
        writer.finish(),
        Err(ArchiveFormatError::InvalidContainerLayout(
            "archive writer is unusable after an earlier encode failure"
        ))
    ));
}

#[test]
fn writer_sequence_limits_match_reader_and_reject_before_mutation() {
    #[derive(Default)]
    struct SequenceTally {
        transactions: usize,
        entries: usize,
    }

    impl SlotVisitor for SequenceTally {
        fn on_transaction(&mut self, _slot: u64, _tx_index: u32, _tx: &Transaction) {
            self.transactions += 1;
        }

        fn on_block(&mut self, _notification: &BlockNotification, entries: &[EntryRecord]) {
            self.entries += entries.len();
        }
    }

    let config = ArchiveWriterConfig {
        compression: Compression::None,
        ..Default::default()
    };

    // Transactions: accept the configured boundary, reject the next record
    // before touching codec state, and leave a reader-valid boundary archive.
    let mut writer = ArchiveWriter::new(Vec::new(), 900, 1_000, 1, config.clone()).unwrap();
    assert_eq!(
        writer.frame_sequence_limit_for_test(),
        MAX_FRAME_SEQUENCE_ELEMENTS
    );
    writer.set_frame_sequence_limit_for_test(2);
    writer.begin_slot(1_000).unwrap();
    let tx = Transaction::new_boxed();
    writer.write_transaction(&tx).unwrap();
    writer.write_transaction(&tx).unwrap();
    assert!(matches!(
        writer.write_transaction(&tx),
        Err(ArchiveFormatError::SectionTooLarge {
            section: "transactions",
            bytes: 3,
            limit: 2,
        })
    ));
    assert_eq!(writer.stats().transactions, 2);
    let mut meta = BlockMeta::new_boxed();
    meta.slot = 1_000;
    writer.end_slot(&meta, &[]).unwrap();
    let (bytes, _) = writer.finish().unwrap();

    let mut reader = ArchiveReader::open(std::io::Cursor::new(bytes)).unwrap();
    let mut tally = SequenceTally::default();
    assert_eq!(reader.read_bucket(0, &mut tally).unwrap(), 1);
    assert_eq!(tally.transactions, 2);
    assert_eq!(tally.entries, 0);

    // Entries: an over-limit end_slot must not consume the open slot. Retrying
    // with the boundary count succeeds and the reader observes exactly it.
    let mut writer = ArchiveWriter::new(Vec::new(), 900, 2_000, 1, config).unwrap();
    writer.set_frame_sequence_limit_for_test(2);
    writer.begin_slot(2_000).unwrap();
    let mut meta = BlockMeta::new_boxed();
    meta.slot = 2_000;
    assert!(matches!(
        writer.end_slot(&meta, &[EntryRecord::default(); 3]),
        Err(ArchiveFormatError::SectionTooLarge {
            section: "entry records",
            bytes: 3,
            limit: 2,
        })
    ));
    writer
        .end_slot(&meta, &[EntryRecord::default(); 2])
        .unwrap();
    let (bytes, _) = writer.finish().unwrap();

    let mut reader = ArchiveReader::open(std::io::Cursor::new(bytes)).unwrap();
    let mut tally = SequenceTally::default();
    assert_eq!(reader.read_bucket(0, &mut tally).unwrap(), 1);
    assert_eq!(tally.transactions, 0);
    assert_eq!(tally.entries, 2);
}

/// Wire-parity of the local skip against lencode's own `DiffEncoder`, with
/// mode coverage asserted: mode 0 (full), mode 1 (RLE patches), and mode 2
/// (XOR+zstd) segments must each be skipped to exactly their encoded
/// length. This is the invariant the whole feature rests on: the skip
/// path consuming byte-identical spans to `DiffDecoder::decode_blob`.
#[test]
fn skip_diff_blob_matches_encoder_output_for_all_modes() {
    use lencode::diff::DiffEncoder;

    let mut v1 = vec![0u8; 4096];
    for (i, b) in v1.iter_mut().enumerate() {
        *b = (i % 251) as u8;
    }
    // A single-byte change uses RLE patches (mode 1).
    let mut v2 = v1.clone();
    v2[100] ^= 0xFF;
    // Changing every 3rd byte (~33% of the blob, above the RLE half-blob
    // cutoff) uses XOR+zstd (mode 2), matching lencode's own mode-2
    // test.
    let mut v3 = v2.clone();
    let mut i = 0;
    while i < v3.len() {
        v3[i] = v3[i].wrapping_add(1);
        i += 3;
    }

    let mut encoder = DiffEncoder::new();
    let mut buf = Vec::new();
    let mut boundaries = Vec::new();
    for data in [&v1, &v2, &v3] {
        encoder.set_key(42);
        encoder.encode_blob(data, &mut buf).unwrap();
        boundaries.push(buf.len());
    }
    // Assert the encoder actually chose three distinct modes (first byte of
    // each segment is the varint mode flag for modes 0..=2).
    let seg_starts = [0, boundaries[0], boundaries[1]];
    assert_eq!(buf[seg_starts[0]], 0, "first write must be a full blob");
    assert_eq!(buf[seg_starts[1]], 1, "single-byte change must pick RLE");
    assert_eq!(buf[seg_starts[2]], 2, "scattered change must pick XOR+zstd");

    // The skip must land exactly on each segment boundary.
    let mut cur = lencode::io::Cursor::new(&buf[..]);
    for boundary in &boundaries {
        super::bucket::skip_diff_blob(&mut cur).unwrap();
        assert_eq!(cur.position(), *boundary);
    }

    // Truncated payloads must fail cleanly, not advance past the end.
    for boundary in &boundaries {
        let mut truncated = lencode::io::Cursor::new(&buf[..boundary - 1]);
        let mut last_ok = true;
        for _ in 0..3 {
            if super::bucket::skip_diff_blob(&mut truncated).is_err() {
                last_ok = false;
                break;
            }
        }
        assert!(!last_ok, "skip over truncated stream must error");
    }
}
