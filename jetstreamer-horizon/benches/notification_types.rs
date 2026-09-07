//! Criterion benchmarks for the notification types backing the archive
//! writer: the [`AccountUpdates`] arena, [`BlockMeta`] /
//! [`BlockNotification`], [`EpochMeta`], and the archive's orphan-update
//! sections.
//!
//! Self-contained — no network corpus. Run with
//! `cargo bench -p jetstreamer-horizon --bench notification_types`.
use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use jetstreamer_horizon::account_updates::{AccountUpdateView, AccountUpdates};
use jetstreamer_horizon::archive::{
    ArchiveReader, ArchiveWriter, ArchiveWriterConfig, EntryRecord, SlotVisitor,
};
use jetstreamer_horizon::block_metas::{BlockMeta, BlockNotification};
use jetstreamer_horizon::epochs::EpochMeta;
use jetstreamer_horizon::limits::{MAX_SLOT_PRE_UPDATE_DATA, MAX_SLOT_PRE_UPDATES};
use jetstreamer_horizon::transactions::Transaction;
use lencode::prelude::*;
use solana_address::Address;
use solana_hash::Hash;

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

fn make_view<'a>(rng: &mut Rng, data: &'a [u8]) -> AccountUpdateView<'a> {
    let mut pk = [0u8; 32];
    rng.fill(&mut pk);
    AccountUpdateView {
        pubkey: Address::new_from_array(pk),
        lamports: rng.next(),
        owner: Address::new_from_array([3u8; 32]),
        executable: false,
        rent_epoch: u64::MAX,
        write_version: rng.next(),
        data,
    }
}

// --------------------------------------------------------------------
// AccountUpdates arena
// --------------------------------------------------------------------

fn bench_arena_push(c: &mut Criterion) {
    let mut group = c.benchmark_group("arena/push");
    // (label, payload size): SPL token account, 1 KiB program state,
    // vote-state size.
    for (label, size) in [
        ("token_165B", 165usize),
        ("state_1KiB", 1024),
        ("vote_3762B", 3_762),
    ] {
        let mut rng = Rng(7);
        let mut payload = vec![0u8; size];
        rng.fill(&mut payload);

        // Push 64 updates per iteration into a pre-allocated arena, then
        // clear — models per-slot fill/reset of an orphan group.
        type Arena = AccountUpdates<128, { 1 << 20 }>;
        let mut arena: Box<Arena> = Box::default();
        let views: Vec<AccountUpdateView<'_>> =
            (0..64).map(|_| make_view(&mut rng, &payload)).collect();

        group.throughput(Throughput::Bytes(64 * size as u64));
        group.bench_function(BenchmarkId::from_parameter(label), |b| {
            b.iter(|| {
                arena.clear();
                for v in &views {
                    arena.push(v).unwrap();
                }
                black_box(arena.len());
            });
        });
    }
    group.finish();
}

fn bench_arena_iter(c: &mut Criterion) {
    let mut group = c.benchmark_group("arena/iter");
    let mut rng = Rng(8);
    let payload = vec![0xA5u8; 1024];
    type Arena = AccountUpdates<128, { 1 << 20 }>;
    let mut arena: Box<Arena> = Box::default();
    for _ in 0..128 {
        arena.push(&make_view(&mut rng, &payload)).unwrap();
    }
    group.throughput(Throughput::Bytes(128 * 1024));
    group.bench_function("128x1KiB", |b| {
        b.iter(|| {
            let mut total = 0usize;
            for (m, d) in arena.iter() {
                total += d.len() + m.write_version as usize % 2;
            }
            black_box(total);
        });
    });
    group.finish();
}

// --------------------------------------------------------------------
// BlockMeta encode / decode
// --------------------------------------------------------------------

/// Builds a BlockMeta with `n_pre` pre-updates of `pre_size` bytes each.
fn build_meta(n_pre: usize, pre_size: usize, seed: u64) -> Box<BlockMeta> {
    let mut rng = Rng(seed);
    let mut payload = vec![0u8; pre_size];
    rng.fill(&mut payload);
    let mut meta = BlockMeta::new_boxed();
    meta.slot = 388_805_000;
    meta.parent_slot = 388_804_999;
    meta.parent_blockhash = Hash::new_from_array([1u8; 32]);
    meta.blockhash = Hash::new_from_array([2u8; 32]);
    meta.block_time = Some(1_750_000_000);
    meta.block_height = Some(370_000_000);
    meta.executed_transaction_count = 1_100;
    meta.entry_count = 96;
    for _ in 0..n_pre {
        meta.pre_updates
            .push(&make_view(&mut rng, &payload))
            .unwrap();
    }
    meta.post_updates
        .push(&make_view(&mut rng, &payload[..pre_size.min(64)]))
        .unwrap();
    meta
}

fn bench_block_meta_codec(c: &mut Criterion) {
    let mut group = c.benchmark_group("block_meta");
    // typical: per-slot sysvar writes (Clock 40 B + SlotHashes ~20 KiB →
    // approximate with 2 × 10 KiB). boundary: 1 000 vote-state updates.
    for (label, n_pre, pre_size) in [
        ("typical_2x10KiB", 2usize, 10_240usize),
        ("boundary_1000xvote", 1_000, 3_762),
    ] {
        let meta = build_meta(n_pre, pre_size, 42);
        let wire_bytes = (n_pre * pre_size) as u64;
        let mut buf = vec![0u8; n_pre * pre_size + (1 << 20)];

        group.throughput(Throughput::Bytes(wire_bytes));
        group.bench_function(BenchmarkId::new("encode", label), |b| {
            b.iter(|| {
                let mut cur = lencode::io::Cursor::new(&mut buf[..]);
                let n = meta.encode_ext(&mut cur, None).unwrap();
                black_box(n);
            });
        });

        let n = {
            let mut cur = lencode::io::Cursor::new(&mut buf[..]);
            meta.encode_ext(&mut cur, None).unwrap()
        };
        let wire = buf[..n].to_vec();
        let mut scratch = BlockMeta::new_boxed();
        group.bench_function(BenchmarkId::new("decode_into", label), |b| {
            b.iter(|| {
                let mut rd = lencode::io::Cursor::new(&wire[..]);
                scratch.decode_into(&mut rd, None).unwrap();
                black_box(scratch.pre_updates.len());
            });
        });
    }
    group.finish();
}

// --------------------------------------------------------------------
// BlockNotification decode cycle (reader hot path)
// --------------------------------------------------------------------

fn bench_notification_decode_cycle(c: &mut Criterion) {
    // Pre-encode a realistic mix: skipped, small block, skipped, bigger
    // block — decoded round-robin into one scratch, exactly like the
    // archive reader's per-slot loop.
    let mut frames: Vec<Vec<u8>> = Vec::new();

    let mut buf = vec![0u8; 4 << 20];
    // skipped
    let n = {
        let mut cur = lencode::io::Cursor::new(&mut buf[..]);
        <BlockNotification as Encode>::encode_discriminant(0, &mut cur).unwrap()
            + 1_000u64.encode_ext(&mut cur, None).unwrap()
    };
    frames.push(buf[..n].to_vec());
    // small block (2 sysvar-ish updates)
    let small = build_meta(2, 1_024, 1);
    let n = {
        let mut cur = lencode::io::Cursor::new(&mut buf[..]);
        <BlockNotification as Encode>::encode_discriminant(1, &mut cur).unwrap()
            + small.encode_ext(&mut cur, None).unwrap()
    };
    frames.push(buf[..n].to_vec());
    // skipped
    let n = {
        let mut cur = lencode::io::Cursor::new(&mut buf[..]);
        <BlockNotification as Encode>::encode_discriminant(0, &mut cur).unwrap()
            + 1_002u64.encode_ext(&mut cur, None).unwrap()
    };
    frames.push(buf[..n].to_vec());
    // medium block (32 × 1 KiB updates)
    let medium = build_meta(32, 1_024, 2);
    let n = {
        let mut cur = lencode::io::Cursor::new(&mut buf[..]);
        <BlockNotification as Encode>::encode_discriminant(1, &mut cur).unwrap()
            + medium.encode_ext(&mut cur, None).unwrap()
    };
    frames.push(buf[..n].to_vec());

    let total_bytes: u64 = frames.iter().map(|f| f.len() as u64).sum();

    let mut group = c.benchmark_group("notification");
    group.throughput(Throughput::Bytes(total_bytes));

    // Production pattern (what ArchiveReader does): two scratches pinned to
    // their variants — block frames decode into the Block scratch's meta in
    // place, skipped frames touch only the tiny Skipped scratch. No variant
    // swap, no 40 MiB memset.
    {
        let mut block_scratch = BlockNotification::new_boxed();
        // Pin to Block once.
        {
            let mut rd = lencode::io::Cursor::new(&frames[1][..]);
            block_scratch.decode_into(&mut rd, None).unwrap();
        }
        let mut skipped_scratch = BlockNotification::new_boxed();
        group.bench_function("decode_cycle_pinned_scratches", |b| {
            b.iter(|| {
                for frame in &frames {
                    // Dispatch by discriminant byte (mirrors the reader's
                    // frame-kind dispatch).
                    let scratch = if frame[0] == 0 {
                        &mut skipped_scratch
                    } else {
                        &mut block_scratch
                    };
                    let mut rd = lencode::io::Cursor::new(&frame[..]);
                    scratch.decode_into(&mut rd, None).unwrap();
                    black_box(scratch.slot());
                }
            });
        });
    }

    // Anti-pattern kept as documentation: a single scratch swapping
    // variants pays a full-enum memset per swap (~40 MiB). This is why the
    // reader pins two scratches.
    {
        let mut scratch = BlockNotification::new_boxed();
        group.sample_size(10);
        group.bench_function("decode_cycle_single_scratch_swaps", |b| {
            b.iter(|| {
                for frame in &frames {
                    let mut rd = lencode::io::Cursor::new(&frame[..]);
                    scratch.decode_into(&mut rd, None).unwrap();
                    black_box(scratch.slot());
                }
            });
        });
    }
    group.finish();
}

// --------------------------------------------------------------------
// clear() and new_boxed() costs
// --------------------------------------------------------------------

fn bench_clear_and_alloc(c: &mut Criterion) {
    let mut group = c.benchmark_group("lifecycle");

    // clear() on a populated BlockMeta: must be O(used), near-free.
    let mut meta = build_meta(64, 1_024, 3);
    group.bench_function("block_meta_clear_64x1KiB", |b| {
        b.iter(|| {
            // Re-populate one entry then clear, so clear always has work.
            meta.slot = 5;
            meta.clear();
            black_box(meta.slot);
        });
    });

    let mut tx = Transaction::new_boxed();
    group.bench_function("transaction_clear", |b| {
        b.iter(|| {
            tx.fee = 5;
            tx.clear();
            black_box(tx.fee);
        });
    });

    // new_boxed: one-time per-thread cost — measured so we know what a
    // worker pays at startup (BlockMeta ≈ 40 MiB zeroed, Transaction ≈ 12 MiB).
    group.sample_size(20);
    group.bench_function("block_meta_new_boxed_40MiB", |b| {
        b.iter(|| {
            let m = BlockMeta::new_boxed();
            black_box(m.slot);
        });
    });
    group.bench_function("transaction_new_boxed_12MiB", |b| {
        b.iter(|| {
            let t = Transaction::new_boxed();
            black_box(t.fee);
        });
    });
    group.finish();
}

// --------------------------------------------------------------------
// EpochMeta roundtrip
// --------------------------------------------------------------------

fn bench_epoch_meta(c: &mut Criterion) {
    let mut rng = Rng(11);
    let payload = vec![0x42u8; 36]; // feature-account-sized
    let mut src = EpochMeta::new_boxed();
    src.epoch = 900;
    src.start_slot = 388_800_000;
    src.slot_count = 432_000;
    src.first_block_slot = 388_800_000;
    src.num_reward_partitions = Some(341);
    for _ in 0..8 {
        src.updates.push(&make_view(&mut rng, &payload)).unwrap();
    }

    let mut buf = vec![0u8; 1 << 16];
    let mut scratch = EpochMeta::new_boxed();
    let mut group = c.benchmark_group("epoch_meta");
    group.bench_function("roundtrip_8_updates", |b| {
        b.iter(|| {
            let mut cur = lencode::io::Cursor::new(&mut buf[..]);
            let n = src.encode_ext(&mut cur, None).unwrap();
            let mut rd = lencode::io::Cursor::new(&buf[..n]);
            scratch.decode_into(&mut rd, None).unwrap();
            black_box(scratch.epoch);
        });
    });
    group.finish();
}

// --------------------------------------------------------------------
// Archive end-to-end: orphan-heavy slots through writer + reader
// --------------------------------------------------------------------

struct CountingVisitor {
    slots: u64,
    orphans: u64,
}

impl SlotVisitor for CountingVisitor {
    fn on_block(&mut self, notification: &BlockNotification, _entries: &[EntryRecord]) {
        self.slots += 1;
        if let BlockNotification::Block(m) = notification {
            self.orphans += (m.pre_updates.len() + m.post_updates.len()) as u64;
        }
    }
}

fn bench_archive_orphan_slots(c: &mut Criterion) {
    // 64 slots, each with vote-state-sized orphan updates touching the
    // SAME 200 accounts every slot with small mutations — the realistic
    // shape that exercises diff compression on the orphan path.
    const SLOTS: u64 = 64;
    const ORPHANS_PER_SLOT: usize = 200;

    let mut rng = Rng(21);
    let mut accounts: Vec<(Address, Vec<u8>)> = (0..ORPHANS_PER_SLOT)
        .map(|_| {
            let mut pk = [0u8; 32];
            rng.fill(&mut pk);
            let mut data = vec![0u8; 3_762];
            rng.fill(&mut data);
            (Address::new_from_array(pk), data)
        })
        .collect();

    let write_archive = |accounts: &mut Vec<(Address, Vec<u8>)>, rng: &mut Rng| -> Vec<u8> {
        let sink = std::io::Cursor::new(Vec::new());
        let mut writer =
            ArchiveWriter::new(sink, 900, 0, SLOTS, ArchiveWriterConfig::default()).unwrap();
        let mut meta = BlockMeta::new_boxed();
        let mut last_blockhash = Hash::default();
        for slot in 0..SLOTS {
            writer.begin_slot(slot).unwrap();
            for (i, (pk, data)) in accounts.iter_mut().enumerate() {
                // Small mutation (vote-tower style).
                let off = (rng.next() as usize) % (data.len() - 8);
                let v = rng.next().to_le_bytes();
                data[off..off + 8].copy_from_slice(&v);
                writer
                    .write_orphan_update(&AccountUpdateView {
                        pubkey: *pk,
                        lamports: slot,
                        owner: Address::new_from_array([3u8; 32]),
                        executable: false,
                        rent_epoch: u64::MAX,
                        write_version: slot * 1_000 + i as u64,
                        data,
                    })
                    .unwrap();
            }
            let mut bh = [0u8; 32];
            rng.fill(&mut bh);
            let blockhash = Hash::new_from_array(bh);
            meta.clear();
            meta.slot = slot;
            meta.parent_slot = slot.saturating_sub(1);
            meta.parent_blockhash = last_blockhash;
            meta.blockhash = blockhash;
            writer
                .end_slot(
                    &meta,
                    &[EntryRecord {
                        num_hashes: 12_500,
                        tx_count: 0,
                    }],
                )
                .unwrap();
            last_blockhash = blockhash;
        }
        let (sink, _) = writer.finish().unwrap();
        sink.into_inner()
    };

    let input_bytes = SLOTS * ORPHANS_PER_SLOT as u64 * 3_762;
    let mut group = c.benchmark_group("archive/orphan_heavy");
    group.sample_size(10);
    group.throughput(Throughput::Bytes(input_bytes));

    group.bench_function("write_64slots_200votes", |b| {
        b.iter(|| {
            let bytes = write_archive(&mut accounts, &mut rng);
            black_box(bytes.len());
        });
    });

    // Pre-write once for the read bench, and report the compression ratio.
    let bytes = write_archive(&mut accounts, &mut rng);
    eprintln!(
        "\n[orphan-heavy archive] input {:.1} MiB -> file {:.2} MiB ({:.1}% of raw)\n",
        input_bytes as f64 / (1 << 20) as f64,
        bytes.len() as f64 / (1 << 20) as f64,
        bytes.len() as f64 / input_bytes as f64 * 100.0,
    );

    group.bench_function("read_64slots_200votes", |b| {
        b.iter(|| {
            let mut reader = ArchiveReader::open(std::io::Cursor::new(&bytes[..])).unwrap();
            let mut visitor = CountingVisitor {
                slots: 0,
                orphans: 0,
            };
            reader.read_slots(0, u64::MAX, &mut visitor).unwrap();
            assert_eq!(visitor.slots, SLOTS);
            black_box(visitor.orphans);
        });
    });
    group.finish();
}

// Keep the arena-capacity types honest: this is a compile-time reminder
// that the bench exercises a fraction of the real limits.
const _: () = {
    assert!(MAX_SLOT_PRE_UPDATES >= 1_000);
    assert!(MAX_SLOT_PRE_UPDATE_DATA >= 1_000 * 3_762);
};

criterion_group!(
    benches,
    bench_arena_push,
    bench_arena_iter,
    bench_block_meta_codec,
    bench_notification_decode_cycle,
    bench_clear_and_alloc,
    bench_epoch_meta,
    bench_archive_orphan_slots,
);
criterion_main!(benches);
