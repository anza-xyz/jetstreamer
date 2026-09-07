//! Criterion benchmarks for the zero-alloc `AccountUpdate` and
//! `Transaction` types against **real mainnet data**.
//!
//! # Corpus
//!
//! At process startup we fetch all transactions from mainnet epoch 900,
//! slots 5 000..6 000 (≈1 000 slots of real traffic) via the
//! `jetstreamer_firehose::firehose` streaming API. The resulting
//! `Vec<VersionedTransaction>` is memoised in a `once_cell::Lazy` so
//! every bench iterates over the same corpus with zero additional
//! network activity.
//!
//! Each bench builds a handful of `Box<Transaction>` fixtures by copying
//! real signatures + message contents (account keys, instructions,
//! blockhash) out of the corpus, then pairs them with synthetic
//! **account updates**: random pubkeys + random high-entropy data bytes
//! of varying size, seeded deterministically off the real tx's
//! signature so the same tx always produces the same updates.
//!
//! Run with
//! `cargo bench -p jetstreamer-horizon --bench encode_decode`.
//! First run pays the one-time firehose fetch (tens of seconds depending
//! on your link); subsequent benches in the same process are in-memory.

use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use futures_util::FutureExt;
use jetstreamer_firehose::epochs::epoch_to_slot_range;
use jetstreamer_firehose::firehose::{
    OnBlockFn, OnEntryFn, OnErrorFn, OnRewardFn, OnStatsTrackingFn, TransactionData, firehose,
};
use jetstreamer_horizon::account_updates::{AccountUpdate, AccountUpdateView};
use jetstreamer_horizon::dedupe::{
    new_decoder_context, new_encoder_context, reset_decoder, reset_encoder,
};
use jetstreamer_horizon::limits::{
    MAX_IX_ACCOUNTS, MAX_IX_DATA_LEN, MAX_TX_ACCOUNT_UPDATES, MAX_TX_ACCOUNTS, MAX_TX_ADDR_LOOKUPS,
    MAX_TX_INSTRUCTIONS, MAX_TX_SIGS,
};
use jetstreamer_horizon::pubkey_prime::POPULAR_PUBKEYS;
use jetstreamer_horizon::transactions::{
    CompiledInstruction, LegacyMessage, MessageAddressTableLookup, MessageHeader, Transaction,
    V0Message, VersionedMessage,
};
use lencode::io::Cursor;
use lencode::prelude::*;
use once_cell::sync::Lazy;
use solana_address::Address;
use solana_signature::Signature;
use solana_transaction::versioned::VersionedTransaction;
use std::sync::{Arc, Mutex};

// --------------------------------------------------------------------
// Corpus: real transactions from epoch 900, slots 5k..6k.
// --------------------------------------------------------------------

/// Fetches `slot_count` slots starting at `epoch_offset_slots` past the start
/// of `epoch` via the firehose API. Vote transactions are kept (they're
/// ~2/3 of real traffic). Pre-filtered to what our zero-alloc bounds allow.
fn fetch_corpus_for_epoch(
    epoch: u64,
    epoch_offset_slots: u64,
    slot_count: u64,
) -> Vec<VersionedTransaction> {
    let (epoch_start, _) = epoch_to_slot_range(epoch);
    let slot_start = epoch_start + epoch_offset_slots;
    let slot_end = slot_start + slot_count;
    eprintln!(
        "[bench] fetching real transactions: epoch {epoch}, slots {slot_start}..{slot_end} \
         ({slot_count} slots) …"
    );
    let start_time = std::time::Instant::now();

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("tokio runtime");

    let txs = runtime.block_on(async {
        let slot_range = slot_start..slot_end;
        let collected: Arc<Mutex<Vec<VersionedTransaction>>> =
            Arc::new(Mutex::new(Vec::with_capacity(slot_count as usize * 1100)));

        firehose(
            4,
            true,
            false,
            None,
            slot_range,
            None::<OnBlockFn>,
            Some({
                let collected = Arc::clone(&collected);
                move |_thread_id: usize, td: TransactionData| {
                    let collected = Arc::clone(&collected);
                    async move {
                        if !fits_our_bounds(&td.transaction) {
                            return Ok(());
                        }
                        collected.lock().unwrap().push(td.transaction);
                        Ok(())
                    }
                    .boxed()
                }
            }),
            None::<OnEntryFn>,
            None::<OnRewardFn>,
            None::<OnErrorFn>,
            None::<OnStatsTrackingFn>,
            None,
        )
        .await
        .expect("firehose fetch");

        Arc::try_unwrap(collected)
            .expect("single owner")
            .into_inner()
            .expect("mutex")
    });

    eprintln!(
        "[bench]   loaded {} txs from epoch {epoch} in {:.1}s",
        txs.len(),
        start_time.elapsed().as_secs_f64()
    );
    assert!(!txs.is_empty(), "firehose returned no transactions");
    txs
}

/// Main corpus: 1 000 slots of mainnet epoch 900 (within the pubkey-prime
/// table's collection window). This is the working corpus that drives all
/// the throughput benches.
static REAL_TXS: Lazy<Vec<VersionedTransaction>> =
    Lazy::new(|| fetch_corpus_for_epoch(900, 5_000, 1_000));

/// Smaller corpus from epoch 700 — *before* the pubkey collection
/// window. Used solely by the cross-epoch compression bench to measure
/// how the prime table's hit rate degrades on out-of-range data.
static REAL_TXS_EPOCH_700: Lazy<Vec<VersionedTransaction>> =
    Lazy::new(|| fetch_corpus_for_epoch(700, 5_000, 100));

/// Smaller corpus from epoch 960 — *after* the pubkey collection
/// cutoff. Same role as `REAL_TXS_EPOCH_700` for the future-direction.
static REAL_TXS_EPOCH_960: Lazy<Vec<VersionedTransaction>> =
    Lazy::new(|| fetch_corpus_for_epoch(960, 5_000, 100));

/// Complexity score — bigger = more complex tx (more keys, instructions, or
/// instruction data). Used to pick representative percentile samples.
fn tx_complexity_score(tx: &VersionedTransaction) -> usize {
    use solana_message::VersionedMessage as UpVm;
    let sigs = tx.signatures.len() * 64;
    match &tx.message {
        UpVm::Legacy(m) => {
            sigs + m.account_keys.len() * 32
                + m.instructions
                    .iter()
                    .map(|ix| 1 + ix.accounts.len() + ix.data.len())
                    .sum::<usize>()
        }
        UpVm::V0(m) => {
            sigs + m.account_keys.len() * 32
                + m.instructions
                    .iter()
                    .map(|ix| 1 + ix.accounts.len() + ix.data.len())
                    .sum::<usize>()
                + m.address_table_lookups
                    .iter()
                    .map(|l| 32 + l.writable_indexes.len() + l.readonly_indexes.len())
                    .sum::<usize>()
        }
    }
}

/// Index picks into [`REAL_TXS`] for each bench shape, chosen at percentiles
/// of a complexity-sorted view of the corpus.
struct CorpusPicks {
    /// 1st-percentile tx: almost always a vote (simplest shape).
    vote: usize,
    /// 50th-percentile tx: a typical user / program transaction.
    typical: usize,
    /// 95th-percentile tx: complex DeFi / multi-instruction transaction.
    complex: usize,
}

static CORPUS_PICKS: Lazy<CorpusPicks> = Lazy::new(|| {
    let corpus = &*REAL_TXS;
    // Rank every tx by complexity score. We sort indices (cheap) rather
    // than cloning transactions.
    let mut idx: Vec<usize> = (0..corpus.len()).collect();
    idx.sort_by_key(|&i| tx_complexity_score(&corpus[i]));

    let pick = |pct: usize| idx[(corpus.len() * pct) / 100];
    let picks = CorpusPicks {
        vote: pick(1),
        typical: pick(50),
        complex: pick(95),
    };

    eprintln!(
        "[bench] shape picks by complexity percentile: \
         vote={} (idx={}), typical={} (idx={}), complex={} (idx={})",
        tx_complexity_score(&corpus[picks.vote]),
        picks.vote,
        tx_complexity_score(&corpus[picks.typical]),
        picks.typical,
        tx_complexity_score(&corpus[picks.complex]),
        picks.complex,
    );
    picks
});

fn fits_our_bounds(tx: &VersionedTransaction) -> bool {
    if tx.signatures.len() > MAX_TX_SIGS {
        return false;
    }
    use solana_message::VersionedMessage as UpVm;
    match &tx.message {
        UpVm::Legacy(m) => {
            m.account_keys.len() <= MAX_TX_ACCOUNTS
                && m.instructions.len() <= MAX_TX_INSTRUCTIONS
                && m.instructions.iter().all(|ix| {
                    ix.accounts.len() <= MAX_IX_ACCOUNTS && ix.data.len() <= MAX_IX_DATA_LEN
                })
        }
        UpVm::V0(m) => {
            m.account_keys.len() <= MAX_TX_ACCOUNTS
                && m.instructions.len() <= MAX_TX_INSTRUCTIONS
                && m.address_table_lookups.len() <= MAX_TX_ADDR_LOOKUPS
                && m.instructions.iter().all(|ix| {
                    ix.accounts.len() <= MAX_IX_ACCOUNTS && ix.data.len() <= MAX_IX_DATA_LEN
                })
        }
    }
}

// --------------------------------------------------------------------
// Deterministic PRNG — splitmix64.
// --------------------------------------------------------------------

struct SplitMix64 {
    state: u64,
}

impl SplitMix64 {
    const fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E3779B97F4A7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
        z ^ (z >> 31)
    }

    fn fill(&mut self, out: &mut [u8]) {
        let mut i = 0;
        while i + 8 <= out.len() {
            let v = self.next_u64().to_le_bytes();
            out[i..i + 8].copy_from_slice(&v);
            i += 8;
        }
        if i < out.len() {
            let v = self.next_u64().to_le_bytes();
            let rem = out.len() - i;
            out[i..].copy_from_slice(&v[..rem]);
        }
    }

    /// 75 % popular pubkey, 25 % fresh random.
    fn pubkey(&mut self) -> [u8; 32] {
        if self.next_u64() & 0b11 != 0 {
            let idx = (self.next_u64() as usize) % POPULAR_PUBKEYS.len();
            POPULAR_PUBKEYS[idx]
        } else {
            let mut b = [0u8; 32];
            self.fill(&mut b);
            b
        }
    }
}

/// Seeds a PRNG off the transaction's signature so the synthetic account
/// updates for a given real tx stay stable across bench runs.
fn rng_for_tx(tx: &VersionedTransaction, salt: u64) -> SplitMix64 {
    let sig = tx
        .signatures
        .first()
        .copied()
        .unwrap_or_else(Signature::default);
    let bytes = sig.as_ref();
    let mut seed: u64 = salt;
    for chunk in bytes.chunks(8) {
        let mut buf = [0u8; 8];
        buf[..chunk.len()].copy_from_slice(chunk);
        seed ^= u64::from_le_bytes(buf);
    }
    SplitMix64::new(seed)
}

// --------------------------------------------------------------------
// Conversion: upstream VersionedTransaction → our Transaction.
// --------------------------------------------------------------------

fn copy_instruction(
    dst: &mut CompiledInstruction,
    src: &solana_message::compiled_instruction::CompiledInstruction,
) {
    dst.program_id_index = src.program_id_index;
    dst.accounts.extend_from_slice(&src.accounts);
    dst.data.extend_from_slice(&src.data);
}

fn copy_legacy(dst: &mut LegacyMessage, src: &solana_message::legacy::Message) {
    dst.header = MessageHeader {
        num_required_signatures: src.header.num_required_signatures,
        num_readonly_signed_accounts: src.header.num_readonly_signed_accounts,
        num_readonly_unsigned_accounts: src.header.num_readonly_unsigned_accounts,
    };
    for pk in &src.account_keys {
        dst.account_keys
            .push(Address::new_from_array(pk.to_bytes()));
    }
    dst.recent_blockhash = src.recent_blockhash;
    for ix in &src.instructions {
        let mut ours = CompiledInstruction::default();
        copy_instruction(&mut ours, ix);
        dst.instructions.push(ours);
    }
}

fn copy_v0(dst: &mut V0Message, src: &solana_message::v0::Message) {
    dst.header = MessageHeader {
        num_required_signatures: src.header.num_required_signatures,
        num_readonly_signed_accounts: src.header.num_readonly_signed_accounts,
        num_readonly_unsigned_accounts: src.header.num_readonly_unsigned_accounts,
    };
    for pk in &src.account_keys {
        dst.account_keys
            .push(Address::new_from_array(pk.to_bytes()));
    }
    dst.recent_blockhash = src.recent_blockhash;
    for ix in &src.instructions {
        let mut ours = CompiledInstruction::default();
        copy_instruction(&mut ours, ix);
        dst.instructions.push(ours);
    }
    for lookup in &src.address_table_lookups {
        let mut ours = MessageAddressTableLookup {
            account_key: Address::new_from_array(lookup.account_key.to_bytes()),
            ..Default::default()
        };
        ours.writable_indexes
            .extend_from_slice(&lookup.writable_indexes);
        ours.readonly_indexes
            .extend_from_slice(&lookup.readonly_indexes);
        dst.address_table_lookups.push(ours);
    }
}

/// Copies a real `VersionedTransaction`'s signatures + message into `tx`
/// in-place (no 750 KiB stack temporary).
fn populate_from_real(tx: &mut Transaction, real: &VersionedTransaction) {
    tx.signatures.clear();
    for sig in &real.signatures {
        tx.signatures.push(*sig);
    }
    use solana_message::VersionedMessage as UpVm;
    match &real.message {
        UpVm::Legacy(m) => {
            // Switch to Legacy variant in place if needed.
            if !matches!(tx.message, VersionedMessage::Legacy(_)) {
                // Zero + re-init via the same byte-level trick decode_into uses.
                unsafe {
                    core::ptr::drop_in_place(&mut tx.message as *mut VersionedMessage);
                    core::ptr::write_bytes(&mut tx.message as *mut VersionedMessage, 0, 1);
                }
            }
            match &mut tx.message {
                VersionedMessage::Legacy(dst) => {
                    dst.clear();
                    copy_legacy(dst, m);
                }
                _ => unreachable!(),
            }
        }
        UpVm::V0(m) => {
            if !matches!(tx.message, VersionedMessage::V0(_)) {
                unsafe {
                    core::ptr::drop_in_place(&mut tx.message as *mut VersionedMessage);
                    core::ptr::write_bytes(&mut tx.message as *mut VersionedMessage, 0, 1);
                    *(&mut tx.message as *mut VersionedMessage as *mut u8) = 1;
                }
            }
            match &mut tx.message {
                VersionedMessage::V0(dst) => {
                    dst.clear();
                    copy_v0(dst, m);
                }
                _ => unreachable!(),
            }
        }
    }
}

// --------------------------------------------------------------------
// Synthetic account-update population.
// --------------------------------------------------------------------

/// Populates `tx`'s account-update arena with `n_updates` synthetic entries,
/// each with a random payload size in `[min_size, max_size]` drawn from the
/// tx-seeded PRNG. Returns the actual number pushed (may be fewer than
/// requested if the arena fills up — we never panic).
fn populate_account_updates(
    tx: &mut Transaction,
    real: &VersionedTransaction,
    n_updates: usize,
    min_size: usize,
    max_size: usize,
    salt: u64,
) -> usize {
    let mut rng = rng_for_tx(real, salt);
    let mut payloads: Vec<Vec<u8>> = Vec::with_capacity(n_updates);
    for _ in 0..n_updates {
        let range = max_size.saturating_sub(min_size).max(1);
        let size = min_size + (rng.next_u64() as usize % range);
        let mut buf = vec![0u8; size];
        rng.fill(&mut buf);
        payloads.push(buf);
    }

    let mut pushed = 0;
    for payload in &payloads {
        if tx.account_updates().len() >= MAX_TX_ACCOUNT_UPDATES {
            break;
        }
        let pk = rng.pubkey();
        let view = AccountUpdateView {
            pubkey: Address::new_from_array(pk),
            lamports: rng.next_u64(),
            owner: Address::new_from_array(rng.pubkey()),
            executable: rng.next_u64() & 1 == 0,
            rent_epoch: rng.next_u64() % 1000,
            write_version: rng.next_u64(),
            data: payload,
        };
        if tx.push_account_update(&view).is_err() {
            break; // arena filled
        }
        pushed += 1;
    }
    pushed
}

// --------------------------------------------------------------------
// Representative shapes: pick real txs from different points in the corpus
// to get a range of message complexities, pair each with synthetic account
// updates of different sizes.
// --------------------------------------------------------------------

struct Shape {
    label: &'static str,
    /// Resolves the corpus index for this shape given the pre-computed
    /// percentile picks.
    pick: fn(&CorpusPicks) -> usize,
    /// Number of synthetic account updates to attach.
    n_updates: usize,
    /// Payload size range for each update (min, max) in bytes.
    data_size: (usize, usize),
}

const SHAPES: &[Shape] = &[
    // Vote transaction: 1 sig, fixed instruction, writes one vote state
    // account → one small synthetic update.
    Shape {
        label: "vote",
        pick: |p| p.vote,
        n_updates: 1,
        data_size: (120, 200),
    },
    // Typical user/program tx: medium complexity, mid-size account updates.
    Shape {
        label: "typical",
        pick: |p| p.typical,
        n_updates: 12,
        data_size: (165, 2048),
    },
    // Complex DeFi-style tx (95th percentile): larger inner state updates.
    Shape {
        label: "complex",
        pick: |p| p.complex,
        n_updates: 30,
        data_size: (512, 8192),
    },
];

fn build_transaction(shape: &Shape) -> Box<Transaction> {
    let corpus = &*REAL_TXS;
    let idx = (shape.pick)(&CORPUS_PICKS);
    let real = &corpus[idx];
    let mut tx = Transaction::new_boxed();
    populate_from_real(&mut tx, real);
    populate_account_updates(
        &mut tx,
        real,
        shape.n_updates,
        shape.data_size.0,
        shape.data_size.1,
        0xBEEF,
    );
    tx
}

// --------------------------------------------------------------------
// AccountUpdate benches — use a single synthetic update pegged to the tx
// hash, with data-size sweep.
// --------------------------------------------------------------------

const ACCOUNT_DATA_SIZES: &[usize] = &[165, 1024, 65_536, 262_144];

fn build_account_update(size: usize, salt: u64) -> Box<AccountUpdate> {
    let corpus = &*REAL_TXS;
    let real = &corpus[salt as usize % corpus.len()];
    let mut rng = rng_for_tx(real, salt);
    let mut payload = vec![0u8; size];
    rng.fill(&mut payload);
    let pk = rng.pubkey();
    let owner = rng.pubkey();
    let mut u = AccountUpdate::new_boxed();
    u.fill_from_v1(
        &agave_geyser_plugin_interface::geyser_plugin_interface::ReplicaAccountInfo {
            pubkey: &pk,
            lamports: rng.next_u64(),
            owner: &owner,
            executable: rng.next_u64() & 1 == 0,
            rent_epoch: rng.next_u64() % 1000,
            data: &payload,
            write_version: rng.next_u64(),
        },
    );
    u
}

fn bench_account_update_encode(c: &mut Criterion) {
    let _ = &*REAL_TXS; // force load before benching
    let mut group = c.benchmark_group("account_update/encode");
    for &size in ACCOUNT_DATA_SIZES {
        let update = build_account_update(size, 0xA11CE ^ size as u64);
        let mut buf = vec![0u8; size + 1024];

        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::new("no_dedupe", size), &size, |b, _| {
            b.iter(|| {
                let mut cursor = Cursor::new(&mut buf[..]);
                let n = update.encode_ext(&mut cursor, None).expect("encode");
                black_box(n);
            });
        });

        let mut ctx = new_encoder_context();
        group.bench_with_input(BenchmarkId::new("with_dedupe", size), &size, |b, _| {
            b.iter(|| {
                reset_encoder(&mut ctx);
                let mut cursor = Cursor::new(&mut buf[..]);
                let n = update
                    .encode_ext(&mut cursor, Some(&mut ctx))
                    .expect("encode");
                black_box(n);
            });
        });
    }
    group.finish();
}

fn bench_account_update_decode(c: &mut Criterion) {
    let _ = &*REAL_TXS;
    let mut group = c.benchmark_group("account_update/decode");
    for &size in ACCOUNT_DATA_SIZES {
        let src = build_account_update(size, 0xB0B0 ^ size as u64);

        let mut wire_plain = vec![0u8; size + 1024];
        let mut cur = Cursor::new(&mut wire_plain[..]);
        let plain_len = src.encode_ext(&mut cur, None).expect("encode");
        wire_plain.truncate(plain_len);

        let mut enc_ctx = new_encoder_context();
        reset_encoder(&mut enc_ctx);
        let mut wire_dedupe = vec![0u8; size + 1024];
        let mut cur = Cursor::new(&mut wire_dedupe[..]);
        let dedupe_len = src
            .encode_ext(&mut cur, Some(&mut enc_ctx))
            .expect("encode");
        wire_dedupe.truncate(dedupe_len);

        let mut dst = AccountUpdate::new_boxed();

        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::new("no_dedupe", size), &size, |b, _| {
            b.iter(|| {
                let mut cursor = Cursor::new(&wire_plain[..]);
                dst.decode_into(&mut cursor, None).expect("decode");
                black_box(&dst);
            });
        });

        let mut dec_ctx = new_decoder_context();
        group.bench_with_input(BenchmarkId::new("with_dedupe", size), &size, |b, _| {
            b.iter(|| {
                reset_decoder(&mut dec_ctx);
                let mut cursor = Cursor::new(&wire_dedupe[..]);
                dst.decode_into(&mut cursor, Some(&mut dec_ctx))
                    .expect("decode");
                black_box(&dst);
            });
        });
    }
    group.finish();
}

// --------------------------------------------------------------------
// Transaction benches — use real messages from the corpus.
// --------------------------------------------------------------------

fn bench_transaction_encode(c: &mut Criterion) {
    let _ = &*REAL_TXS;
    let mut group = c.benchmark_group("transaction/encode");
    for shape in SHAPES {
        let tx = build_transaction(shape);
        let wire_bytes = estimate_tx_size(&tx);
        let mut buf = vec![0u8; wire_bytes * 2 + 64_000];

        group.throughput(Throughput::Bytes(wire_bytes as u64));

        group.bench_with_input(
            BenchmarkId::new("no_dedupe", shape.label),
            shape.label,
            |b, _| {
                b.iter(|| {
                    let mut cursor = Cursor::new(&mut buf[..]);
                    let n = tx.encode_ext(&mut cursor, None).expect("encode");
                    black_box(n);
                });
            },
        );

        let mut ctx = new_encoder_context();
        group.bench_with_input(
            BenchmarkId::new("with_dedupe", shape.label),
            shape.label,
            |b, _| {
                b.iter(|| {
                    reset_encoder(&mut ctx);
                    let mut cursor = Cursor::new(&mut buf[..]);
                    let n = tx.encode_ext(&mut cursor, Some(&mut ctx)).expect("encode");
                    black_box(n);
                });
            },
        );
    }
    group.finish();
}

fn bench_transaction_decode(c: &mut Criterion) {
    let _ = &*REAL_TXS;
    let mut group = c.benchmark_group("transaction/decode");
    for shape in SHAPES {
        let src = build_transaction(shape);
        let wire_bytes = estimate_tx_size(&src);

        let mut wire_plain = vec![0u8; wire_bytes * 2 + 64_000];
        let mut cur = Cursor::new(&mut wire_plain[..]);
        let plain_len = src.encode_ext(&mut cur, None).expect("encode");
        wire_plain.truncate(plain_len);

        let mut enc_ctx = new_encoder_context();
        reset_encoder(&mut enc_ctx);
        let mut wire_dedupe = vec![0u8; wire_bytes * 2 + 64_000];
        let mut cur = Cursor::new(&mut wire_dedupe[..]);
        let dedupe_len = src
            .encode_ext(&mut cur, Some(&mut enc_ctx))
            .expect("encode");
        wire_dedupe.truncate(dedupe_len);

        let mut dst = Transaction::new_boxed();

        group.throughput(Throughput::Bytes(wire_bytes as u64));
        group.bench_with_input(
            BenchmarkId::new("no_dedupe", shape.label),
            shape.label,
            |b, _| {
                b.iter(|| {
                    let mut cursor = Cursor::new(&wire_plain[..]);
                    dst.decode_into(&mut cursor, None).expect("decode");
                    black_box(&dst);
                });
            },
        );

        let mut dec_ctx = new_decoder_context();
        group.bench_with_input(
            BenchmarkId::new("with_dedupe", shape.label),
            shape.label,
            |b, _| {
                b.iter(|| {
                    reset_decoder(&mut dec_ctx);
                    let mut cursor = Cursor::new(&wire_dedupe[..]);
                    dst.decode_into(&mut cursor, Some(&mut dec_ctx))
                        .expect("decode");
                    black_box(&dst);
                });
            },
        );
    }
    group.finish();
}

// --------------------------------------------------------------------
// Compression ratios — measure actual wire-bytes produced by each
// encoder configuration over the real-tx corpus.
// --------------------------------------------------------------------

struct CompressionStats {
    label: &'static str,
    total_bytes: u64,
    tx_count: u64,
}

/// Builds a `Transaction` populated only from the real signatures +
/// message (no synthetic account updates). This isolates the compression
/// effect on the parts that dedupe can actually help with — account
/// updates hold random data and wouldn't compress regardless.
fn build_message_only_tx(tx: &mut Transaction, real: &VersionedTransaction) {
    tx.clear();
    populate_from_real(tx, real);
}

/// Computes the Solana-native-wire-format size (via wincode) for every tx
/// in the sample — the "as it weighs on the network today" baseline.
fn measure_wincode_corpus(sample: &[&VersionedTransaction]) -> CompressionStats {
    let mut total_bytes: u64 = 0;
    for tx in sample {
        total_bytes += jetstreamer_firehose::transaction::wincode_serialized_size(tx)
            .expect("wincode size_of") as u64;
    }
    CompressionStats {
        label: "wincode (solana wire)",
        total_bytes,
        tx_count: sample.len() as u64,
    }
}

/// Encodes just `signatures + message` — the VersionedTransaction wire
/// equivalent — bypassing the `Transaction` wrapper's empty-Option and
/// empty-ZeroVec overhead. Apples-to-apples with wincode's
/// `VersionedTransactionSchema`.
fn encode_sig_and_message<W: lencode::prelude::Write>(
    scratch: &Transaction,
    writer: &mut W,
    mut ctx: Option<&mut lencode::context::EncoderContext>,
) -> lencode::Result<usize> {
    let mut total = 0;
    total += scratch.signatures.encode_ext(writer, ctx.as_deref_mut())?;
    total += scratch.message.encode_ext(writer, ctx)?;
    Ok(total)
}

fn encode_corpus_with_config(
    sample: &[&VersionedTransaction],
    label: &'static str,
    ctx_builder: Option<fn() -> lencode::context::EncoderContext>,
) -> CompressionStats {
    let mut scratch = Transaction::new_boxed();
    let mut buf = vec![0u8; 1 << 20]; // 1 MiB wire buffer, reused across txs
    let mut total_bytes: u64 = 0;

    let mut ctx = ctx_builder.map(|f| f());

    for real in sample {
        build_message_only_tx(&mut scratch, real);
        if let Some(ctx) = ctx.as_mut() {
            reset_encoder(ctx);
        }
        let mut cursor = Cursor::new(&mut buf[..]);
        let n = encode_sig_and_message(&scratch, &mut cursor, ctx.as_mut()).expect("encode");
        total_bytes += n as u64;
    }

    CompressionStats {
        label,
        total_bytes,
        tx_count: sample.len() as u64,
    }
}

/// Runs the full compression-size analysis once (not a timing bench) and
/// prints a ratio table to stderr. Registers a trivial Criterion bench
/// so this function integrates into `cargo bench` output normally.
fn bench_compression_ratios(c: &mut Criterion) {
    let _ = &*REAL_TXS;

    // Sample 10k transactions evenly across the corpus for a stable
    // estimate of average encoded size without running through 1M+ items.
    let corpus = &*REAL_TXS;
    let n_samples = 10_000.min(corpus.len());
    let sample: Vec<&VersionedTransaction> = (0..n_samples)
        .map(|i| &corpus[(i * corpus.len()) / n_samples])
        .collect();

    // Four configs covering the dedupe-priming progression plus the
    // Solana-wire-format baseline:
    //   0. wincode: re-encode each tx in Solana's native wire format
    //      (same encoding Old Faithful / mainnet gossip carries). This
    //      is the "what it weighs on the wire today" baseline.
    //   1. No dedupe context at all (plain lencode encode).
    //   2. Fresh, empty dedupe encoder (stream-local dedupe, no priming).
    //      Every novel pubkey pays full 32 bytes on first sight.
    //   3. Primed dedupe encoder backed by the 65 535-pubkey frozen table.
    //      Popular pubkeys collapse to 1-3 byte varint IDs immediately.
    let wincode_baseline = measure_wincode_corpus(&sample);
    let no_dedupe = encode_corpus_with_config(&sample, "lencode (no dedupe)", None);
    let fresh_dedupe = encode_corpus_with_config(
        &sample,
        "lencode (dedupe)",
        Some(lencode::context::EncoderContext::with_dedupe),
    );
    let primed_dedupe =
        encode_corpus_with_config(&sample, "lencode (+ primed)", Some(new_encoder_context));

    let configs = [&wincode_baseline, &no_dedupe, &fresh_dedupe, &primed_dedupe];
    let baseline = wincode_baseline.total_bytes as f64;
    eprintln!();
    eprintln!(
        "=== Compression vs Solana wire format over {n_samples} real mainnet transactions ==="
    );
    eprintln!(
        "  {:<22} | {:>14}  | {:>9}  | {:>6}  | {:>7}",
        "config", "total_bytes", "avg_B/tx", "ratio", "save"
    );
    eprintln!("  {}", "-".repeat(68));
    for s in configs {
        let avg = s.total_bytes as f64 / s.tx_count as f64;
        let ratio = s.total_bytes as f64 / baseline;
        let save = (1.0 - ratio) * 100.0;
        eprintln!(
            "  {:<22} | {:>14}  | {:>9.1}  | {:>6.3}  | {:>+6.1}%",
            s.label, s.total_bytes, avg, ratio, save
        );
    }
    eprintln!();

    // Register a single tiny "bench" so criterion's report includes this
    // group. We measure one encode-under-primed-dedupe iteration, which
    // also picks up the setup cost indirectly.
    let mut group = c.benchmark_group("compression/encode_sample_10k");
    let input_bytes: u64 = sample.iter().map(|tx| tx_complexity_score(tx) as u64).sum();
    group.throughput(Throughput::Bytes(input_bytes));
    group.sample_size(10);
    group.bench_function("dedupe_primed", |b| {
        b.iter(|| {
            let mut total: u64 = 0;
            let mut scratch = Transaction::new_boxed();
            let mut buf = vec![0u8; 1 << 20];
            let mut ctx = new_encoder_context();
            for real in &sample {
                build_message_only_tx(&mut scratch, real);
                reset_encoder(&mut ctx);
                let mut cursor = Cursor::new(&mut buf[..]);
                total += encode_sig_and_message(&scratch, &mut cursor, Some(&mut ctx))
                    .expect("encode") as u64;
            }
            black_box(total);
        });
    });
    group.finish();
}

// --------------------------------------------------------------------
// Cross-epoch compression — measure how well the 65 535-pubkey prime
// table generalises to data from epochs *outside* its collection
// window.
// --------------------------------------------------------------------

fn bench_cross_epoch_compression(c: &mut Criterion) {
    // Force-load all three corpora before printing — the first hit pays
    // the network fetch, subsequent benches reuse the in-memory corpus.
    let _ = &*REAL_TXS;
    let _ = &*REAL_TXS_EPOCH_700;
    let _ = &*REAL_TXS_EPOCH_960;

    eprintln!();
    eprintln!("=== Pubkey-prime effectiveness across epochs ===");
    eprintln!(
        "Prime table built from a 150-epoch window of pubkey-mention data \
         (cut off around epoch ~880-900). Comparison points:"
    );
    eprintln!("  • epoch 700  — ~180 epochs *before* the window started");
    eprintln!("  • epoch 900  — within the collection window (best case)");
    eprintln!("  • epoch 960  — ~60+ epochs *after* the cutoff");
    eprintln!();

    let n_samples_target = 10_000;
    let corpora: [(&str, &Vec<VersionedTransaction>); 3] = [
        ("epoch 700 (before window)", &*REAL_TXS_EPOCH_700),
        ("epoch 900 (within window)", &*REAL_TXS),
        ("epoch 960 (after cutoff)", &*REAL_TXS_EPOCH_960),
    ];

    for (label, corpus) in corpora {
        let n_samples = n_samples_target.min(corpus.len());
        let sample: Vec<&VersionedTransaction> = (0..n_samples)
            .map(|i| &corpus[(i * corpus.len()) / n_samples])
            .collect();

        let wincode = measure_wincode_corpus(&sample);
        let plain = encode_corpus_with_config(&sample, "lencode (no dedupe)", None);
        let primed =
            encode_corpus_with_config(&sample, "lencode (+ primed)", Some(new_encoder_context));

        let baseline = wincode.total_bytes as f64;
        eprintln!("--- {label}  ({n_samples} samples) ---");
        eprintln!(
            "  {:<22} | {:>9}  | {:>6}  | {:>7}",
            "config", "avg_B/tx", "ratio", "save"
        );
        eprintln!("  {}", "-".repeat(56));
        for stat in [&wincode, &plain, &primed] {
            let avg = stat.total_bytes as f64 / stat.tx_count as f64;
            let ratio = stat.total_bytes as f64 / baseline;
            let save = (1.0 - ratio) * 100.0;
            eprintln!(
                "  {:<22} | {:>9.1}  | {:>6.3}  | {:>+6.1}%",
                stat.label, avg, ratio, save
            );
        }
        eprintln!();
    }

    // Register a tiny Criterion bench so this group shows up in the
    // standard report. Times the primed encode of the epoch-700 sample
    // (out-of-window) for an apples-to-apples speed comparison against
    // the in-window run from `bench_compression_ratios`.
    let corpus_700 = &*REAL_TXS_EPOCH_700;
    let n_700 = n_samples_target.min(corpus_700.len());
    let sample_700: Vec<&VersionedTransaction> = (0..n_700)
        .map(|i| &corpus_700[(i * corpus_700.len()) / n_700])
        .collect();

    let mut group = c.benchmark_group("compression/cross_epoch");
    let input_bytes: u64 = sample_700
        .iter()
        .map(|tx| tx_complexity_score(tx) as u64)
        .sum();
    group.throughput(Throughput::Bytes(input_bytes));
    group.sample_size(10);
    group.bench_function("epoch_700_primed", |b| {
        b.iter(|| {
            let mut total: u64 = 0;
            let mut scratch = Transaction::new_boxed();
            let mut buf = vec![0u8; 1 << 20];
            let mut ctx = new_encoder_context();
            for real in &sample_700 {
                build_message_only_tx(&mut scratch, real);
                reset_encoder(&mut ctx);
                let mut cursor = Cursor::new(&mut buf[..]);
                total += encode_sig_and_message(&scratch, &mut cursor, Some(&mut ctx))
                    .expect("encode") as u64;
            }
            black_box(total);
        });
    });
    group.finish();
}

// --------------------------------------------------------------------
// Diff compression — simulates a realistic account-update stream (the
// same accounts updated repeatedly with small mutations) and measures
// the size savings from lencode's `DiffEncoder`.
// --------------------------------------------------------------------

/// A fake ledger of account blobs that simulates realistic mutation
/// patterns: Zipf-ish hot/cold access, sparse byte-level changes per
/// update, account sizes biased toward the SPL-token range.
struct VirtualLedger {
    /// (pubkey, current data) for each live account.
    accounts: Vec<(Address, Vec<u8>)>,
    rng: SplitMix64,
}

impl VirtualLedger {
    fn new(seed: u64, n_accounts: usize) -> Self {
        let mut rng = SplitMix64::new(seed);
        let mut accounts = Vec::with_capacity(n_accounts);
        for _ in 0..n_accounts {
            let pk = rng.pubkey();
            // Size distribution (rough Solana shape):
            //   60 % token/stake-like (100–400 B)
            //   30 % program state (400–2 KiB)
            //   10 % larger (2 KiB–10 KiB)
            let size = match rng.next_u64() % 100 {
                0..=59 => 100 + rng.next_u64() as usize % 301,
                60..=89 => 400 + rng.next_u64() as usize % 1600,
                _ => 2000 + rng.next_u64() as usize % 8000,
            };
            let mut data = vec![0u8; size];
            rng.fill(&mut data);
            accounts.push((Address::new_from_array(pk), data));
        }
        Self { accounts, rng }
    }

    /// Returns `(pubkey, snapshot_of_new_data)` for the next update,
    /// applied as a small mutation to an existing account (mostly hot).
    fn next_update(&mut self) -> (Address, Vec<u8>) {
        // 80 % of updates target the hottest 10 % of accounts; 20 % hit
        // the long tail. Approximates real Solana's access skew.
        let pool_len = self.accounts.len();
        let idx = if self.rng.next_u64() % 100 < 80 {
            (self.rng.next_u64() as usize) % pool_len.div_ceil(10).max(1)
        } else {
            (self.rng.next_u64() as usize) % pool_len
        };
        let (pk, data) = &mut self.accounts[idx];

        // Sparse mutation: flip 1–5 ranges of 1–16 bytes each at random
        // offsets. Realistic for lamport-balance / slot / counter updates.
        let n_ranges = 1 + (self.rng.next_u64() as usize % 5);
        for _ in 0..n_ranges {
            if data.is_empty() {
                break;
            }
            let start = (self.rng.next_u64() as usize) % data.len();
            let remaining = data.len() - start;
            let len = 1 + (self.rng.next_u64() as usize % 16).min(remaining - 1);
            self.rng.fill(&mut data[start..start + len]);
        }
        (*pk, data.clone())
    }
}

/// Derives a stable u64 key for the diff encoder from an `Address`.
fn diff_key(addr: &Address) -> u64 {
    let bytes = addr.to_bytes();
    let mut k: u64 = 0;
    for chunk in bytes.chunks(8) {
        let mut buf = [0u8; 8];
        buf[..chunk.len()].copy_from_slice(chunk);
        k ^= u64::from_le_bytes(buf);
    }
    k
}

fn bench_account_update_diff_compression(c: &mut Criterion) {
    const N_ACCOUNTS: usize = 1_000;
    const N_UPDATES: usize = 10_000;

    let mut ledger = VirtualLedger::new(0xDEADBEEF, N_ACCOUNTS);
    let updates: Vec<(Address, Vec<u8>)> = (0..N_UPDATES).map(|_| ledger.next_update()).collect();

    let total_input_bytes: u64 = updates.iter().map(|(_, d)| d.len() as u64).sum();

    // --- Baseline: raw length-prefixed encoding (what our ZeroVec<u8>
    //     encode_ext does: `varint(len << 1) + bytes`).
    fn varint_len(mut v: u64) -> u64 {
        if v <= 127 {
            return 1;
        }
        let mut n = 0;
        while v != 0 {
            n += 1;
            v >>= 8;
        }
        1 + n
    }
    let raw_total: u64 = updates
        .iter()
        .map(|(_, d)| varint_len((d.len() << 1) as u64) + d.len() as u64)
        .sum();

    // --- With DiffEncoder, keyed by pubkey.
    use lencode::diff::DiffEncoder;
    let mut diff_enc = DiffEncoder::with_capacity(N_ACCOUNTS);
    let mut buf = Vec::with_capacity(1 << 20);
    let mut diff_total: u64 = 0;
    for (addr, data) in &updates {
        diff_enc.set_key(diff_key(addr));
        buf.clear();
        diff_total += diff_enc.encode_blob(data, &mut buf).expect("encode") as u64;
    }

    eprintln!();
    eprintln!(
        "=== Diff compression over {N_UPDATES} updates to {N_ACCOUNTS} accounts \
         (hot/cold Zipf-ish, sparse mutations) ==="
    );
    eprintln!(
        "  {:<24} | {:>12}  | {:>10}  | {:>6}  | {:>7}",
        "config", "total_bytes", "avg_B/upd", "ratio", "save"
    );
    eprintln!("  {}", "-".repeat(72));
    for (label, bytes) in [
        ("raw (len + data)", raw_total),
        ("lencode DiffEncoder", diff_total),
    ] {
        let avg = bytes as f64 / N_UPDATES as f64;
        let ratio = bytes as f64 / raw_total as f64;
        let save = (1.0 - ratio) * 100.0;
        eprintln!(
            "  {:<24} | {:>12}  | {:>10.1}  | {:>6.3}  | {:>+6.1}%",
            label, bytes, avg, ratio, save
        );
    }
    eprintln!(
        "  input (uncompressed)     | {:>12}  | {:>10.1}  |   --   |   --",
        total_input_bytes,
        total_input_bytes as f64 / N_UPDATES as f64,
    );
    eprintln!();

    // Time the diff-encode path through the same stream — per-iter cost
    // of encoding the full update batch.
    let mut group = c.benchmark_group("compression/diff_stream");
    group.throughput(Throughput::Bytes(total_input_bytes));
    group.sample_size(10);
    group.bench_function("encode_all", |b| {
        b.iter_batched(
            || {
                (
                    DiffEncoder::with_capacity(N_ACCOUNTS),
                    Vec::with_capacity(1 << 20),
                )
            },
            |(mut enc, mut buf)| {
                let mut total = 0u64;
                for (addr, data) in &updates {
                    enc.set_key(diff_key(addr));
                    buf.clear();
                    total += enc.encode_blob(data, &mut buf).expect("encode") as u64;
                }
                black_box(total);
            },
            criterion::BatchSize::LargeInput,
        );
    });
    group.finish();
}

fn bench_transaction_roundtrip(c: &mut Criterion) {
    let _ = &*REAL_TXS;
    let mut group = c.benchmark_group("transaction/roundtrip");
    for shape in &SHAPES[..2] {
        let src = build_transaction(shape);
        let wire_bytes = estimate_tx_size(&src);
        let mut buf = vec![0u8; wire_bytes * 2 + 64_000];
        let mut dst = Transaction::new_boxed();
        let mut enc_ctx = new_encoder_context();
        let mut dec_ctx = new_decoder_context();

        group.throughput(Throughput::Bytes(wire_bytes as u64));
        group.bench_with_input(
            BenchmarkId::new("with_dedupe", shape.label),
            shape.label,
            |b, _| {
                b.iter(|| {
                    reset_encoder(&mut enc_ctx);
                    let mut cursor = Cursor::new(&mut buf[..]);
                    let wire_len = src
                        .encode_ext(&mut cursor, Some(&mut enc_ctx))
                        .expect("encode");

                    reset_decoder(&mut dec_ctx);
                    let mut read_cursor = Cursor::new(&buf[..wire_len]);
                    dst.decode_into(&mut read_cursor, Some(&mut dec_ctx))
                        .expect("decode");
                    black_box(&dst);
                });
            },
        );
    }
    group.finish();
}

/// Estimates the wire payload size (account data dominates); used as the
/// throughput scaling factor for Criterion's GiB/s output.
fn estimate_tx_size(tx: &Transaction) -> usize {
    tx.iter_account_updates()
        .map(|(_, data)| data.len())
        .sum::<usize>()
        + 1024
}

// --------------------------------------------------------------------
// Archive epoch estimator — writes simulated slots of real transactions
// + realistic account updates through the actual ArchiveWriter and
// projects full-epoch storage size and processing time.
// --------------------------------------------------------------------

/// Simulated ledger backing the archive estimate: persistent account
/// states mutated across slots so the archive's diff encoder sees
/// realistic cross-slot repetition.
struct EstimatorLedger {
    /// ~3.7 KiB vote-state blobs, one per simulated validator. Touched
    /// every slot with small scattered mutations (tower updates).
    vote_accounts: Vec<(Address, Vec<u8>)>,
    /// Mixed-size user accounts (token balances, program state), Zipf-ish
    /// hot/cold access with sparse mutations.
    user_accounts: Vec<(Address, Vec<u8>)>,
    rng: SplitMix64,
}

impl EstimatorLedger {
    fn new(seed: u64) -> Self {
        let mut rng = SplitMix64::new(seed);
        let vote_accounts = (0..1_500)
            .map(|_| {
                let mut pk = [0u8; 32];
                rng.fill(&mut pk);
                let mut data = vec![0u8; 3_762];
                rng.fill(&mut data);
                (Address::new_from_array(pk), data)
            })
            .collect();
        let user_accounts = (0..50_000)
            .map(|_| {
                let pk_arr = rng.pubkey();
                let size = match rng.next_u64() % 100 {
                    0..=59 => 165,
                    60..=89 => 400 + rng.next_u64() as usize % 1_600,
                    _ => 2_000 + rng.next_u64() as usize % 8_000,
                };
                let mut data = vec![0u8; size];
                rng.fill(&mut data);
                (Address::new_from_array(pk_arr), data)
            })
            .collect();
        Self {
            vote_accounts,
            user_accounts,
            rng,
        }
    }

    /// Touch a vote account (small scattered mutation, ~48 bytes).
    fn touch_vote(&mut self, validator: usize) -> (Address, Vec<u8>) {
        let idx = validator % self.vote_accounts.len();
        let (pk, data) = &mut self.vote_accounts[idx];
        for _ in 0..6 {
            let off = (self.rng.next_u64() as usize) % data.len().saturating_sub(8).max(1);
            let v = self.rng.next_u64().to_le_bytes();
            data[off..off + 8].copy_from_slice(&v);
        }
        (*pk, data.clone())
    }

    /// Touch a user account (Zipf-ish: 80 % of touches hit the hottest 10 %).
    fn touch_user(&mut self) -> (Address, Vec<u8>) {
        let pool = self.user_accounts.len();
        let idx = if self.rng.next_u64() % 100 < 80 {
            (self.rng.next_u64() as usize) % (pool / 10).max(1)
        } else {
            (self.rng.next_u64() as usize) % pool
        };
        let (pk, data) = &mut self.user_accounts[idx];
        let n = 1 + (self.rng.next_u64() as usize % 4);
        for _ in 0..n {
            if data.is_empty() {
                break;
            }
            let off = (self.rng.next_u64() as usize) % data.len();
            data[off] = data[off].wrapping_add(1);
        }
        (*pk, data.clone())
    }
}

/// Writes `n_slots` simulated slots (real corpus transactions + ledger
/// account updates) through the real `ArchiveWriter`. Returns
/// (stats, file_bytes, wall_seconds).
fn write_simulated_archive(
    corpus: &[VersionedTransaction],
    n_slots: u64,
    config: jetstreamer_horizon::archive::ArchiveWriterConfig,
) -> (jetstreamer_horizon::archive::ArchiveStats, u64, f64) {
    use jetstreamer_horizon::archive::{ArchiveWriter, EntryRecord};

    let vote_program = Address::new_from_array(POPULAR_PUBKEYS[0]);
    let mut ledger = EstimatorLedger::new(0xE57);
    let mut scratch = Transaction::new_boxed();
    let mut rng = SplitMix64::new(0xE58);

    let started = std::time::Instant::now();
    let sink = std::io::Cursor::new(Vec::new());
    let mut writer = ArchiveWriter::new(sink, 900, 0, n_slots, config).expect("writer");
    // Reusable boxed BlockMeta (the type is ~40 MiB with its orphan arenas).
    let mut meta_scratch = jetstreamer_horizon::block_metas::BlockMeta::new_boxed();

    let mut corpus_pos = 0usize;
    let mut last_blockhash = solana_hash::Hash::default();
    for slot in 0..n_slots {
        writer.begin_slot(slot).expect("begin_slot");

        // Real mainnet averages ~1 100 txs/slot (votes included).
        let txs_this_slot = 1_050 + (rng.next_u64() % 100) as usize;
        let mut entries = Vec::with_capacity(96);
        let mut tx_in_entry = 0u32;
        for _ in 0..txs_this_slot {
            let real = &corpus[corpus_pos % corpus.len()];
            corpus_pos += 1;

            scratch.clear();
            populate_from_real(&mut scratch, real);

            // Attach realistic account updates.
            let is_vote = match &real.message {
                solana_message::VersionedMessage::Legacy(m) => m
                    .account_keys
                    .iter()
                    .any(|k| Address::new_from_array(k.to_bytes()) == vote_program),
                solana_message::VersionedMessage::V0(m) => m
                    .account_keys
                    .iter()
                    .any(|k| Address::new_from_array(k.to_bytes()) == vote_program),
            };
            if is_vote {
                let validator = (rng.next_u64() as usize) % 1_500;
                let (pk, data) = ledger.touch_vote(validator);
                push_estimator_update(&mut scratch, &mut rng, pk, &data);
            } else {
                let n_updates = 2 + (rng.next_u64() % 5) as usize;
                for _ in 0..n_updates {
                    let (pk, data) = ledger.touch_user();
                    push_estimator_update(&mut scratch, &mut rng, pk, &data);
                }
            }

            writer.write_transaction(&scratch).expect("write_tx");
            tx_in_entry += 1;
            if tx_in_entry == 16 {
                entries.push(EntryRecord {
                    num_hashes: 800,
                    tx_count: tx_in_entry,
                });
                tx_in_entry = 0;
            }
        }
        if tx_in_entry > 0 {
            entries.push(EntryRecord {
                num_hashes: 800,
                tx_count: tx_in_entry,
            });
        }
        // 64 ticks per slot.
        for _ in 0..64 {
            entries.push(EntryRecord {
                num_hashes: 12_500,
                tx_count: 0,
            });
        }

        let mut bh = [0u8; 32];
        rng.fill(&mut bh);
        let blockhash = solana_hash::Hash::new_from_array(bh);
        meta_scratch.clear();
        meta_scratch.slot = slot;
        meta_scratch.parent_slot = slot.saturating_sub(1);
        meta_scratch.parent_blockhash = last_blockhash;
        meta_scratch.blockhash = blockhash;
        meta_scratch.block_time = Some(1_750_000_000 + slot as i64);
        meta_scratch.block_height = Some(slot);
        meta_scratch.executed_transaction_count = txs_this_slot as u64;
        meta_scratch.entry_count = entries.len() as u64;
        writer.end_slot(&meta_scratch, &entries).expect("end_slot");
        last_blockhash = blockhash;
    }

    let (sink, stats) = writer.finish().expect("finish");
    let elapsed = started.elapsed().as_secs_f64();
    (stats, sink.into_inner().len() as u64, elapsed)
}

fn push_estimator_update(tx: &mut Transaction, rng: &mut SplitMix64, pubkey: Address, data: &[u8]) {
    let _ = tx.push_account_update(&AccountUpdateView {
        pubkey,
        lamports: rng.next_u64(),
        owner: Address::new_from_array(POPULAR_PUBKEYS[3]),
        executable: false,
        rent_epoch: u64::MAX,
        write_version: rng.next_u64(),
        data,
    });
}

fn bench_archive_epoch_estimate(c: &mut Criterion) {
    use jetstreamer_horizon::archive::{ArchiveWriterConfig, Compression};

    let corpus = &*REAL_TXS;
    const SLOTS: u64 = 256;
    const EPOCH_SLOTS: f64 = 432_000.0;
    // Current measured replay rate on the reference box (slots/sec).
    const REPLAY_SLOTS_PER_SEC: f64 = 3.75;

    eprintln!();
    eprintln!(
        "=== Archive epoch estimate ({SLOTS} simulated slots, ~1.1k real txs/slot, \
         realistic account updates) ==="
    );
    eprintln!(
        "  {:<22} | {:>10}  | {:>9}  | {:>9}  | {:>8}  | {:>9}",
        "config", "bytes/slot", "GB/epoch", "slots/s", "write h", "total h"
    );
    eprintln!("  {}", "-".repeat(84));

    for (label, config) in [
        ("zstd, bucket=128", ArchiveWriterConfig::default()),
        (
            "none, bucket=128",
            ArchiveWriterConfig {
                compression: Compression::None,
                ..Default::default()
            },
        ),
        (
            "zstd, bucket=1",
            ArchiveWriterConfig {
                bucket_slots: 1,
                ..Default::default()
            },
        ),
    ] {
        let (stats, file_bytes, secs) = write_simulated_archive(corpus, SLOTS, config);
        let bytes_per_slot = file_bytes as f64 / SLOTS as f64;
        let gb_per_epoch = bytes_per_slot * EPOCH_SLOTS / 1e9;
        let write_slots_per_sec = SLOTS as f64 / secs;
        let write_hours = EPOCH_SLOTS / write_slots_per_sec / 3600.0;
        // Write happens concurrently with replay; the pipeline rate is
        // bounded by the slower stage.
        let pipeline_rate = write_slots_per_sec.min(REPLAY_SLOTS_PER_SEC);
        let total_hours = EPOCH_SLOTS / pipeline_rate / 3600.0;
        eprintln!(
            "  {:<22} | {:>10.0}  | {:>9.1}  | {:>9.1}  | {:>8.2}  | {:>9.1}",
            label, bytes_per_slot, gb_per_epoch, write_slots_per_sec, write_hours, total_hours
        );
        let _ = stats;
    }
    eprintln!(
        "  (total h = bounded by replay at {REPLAY_SLOTS_PER_SEC} slots/s; 3-day budget = 72 h)"
    );
    eprintln!();

    // Small criterion-tracked timing sample (16 slots through the writer).
    let mut group = c.benchmark_group("archive/write");
    group.sample_size(10);
    group.bench_function("16_slots_zstd_b128", |b| {
        b.iter(|| {
            let (_, file_bytes, _) =
                write_simulated_archive(corpus, 16, ArchiveWriterConfig::default());
            black_box(file_bytes);
        });
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_account_update_encode,
    bench_account_update_decode,
    bench_transaction_encode,
    bench_transaction_decode,
    bench_transaction_roundtrip,
    bench_compression_ratios,
    bench_cross_epoch_compression,
    bench_account_update_diff_compression,
    bench_archive_epoch_estimate,
);
criterion_main!(benches);
