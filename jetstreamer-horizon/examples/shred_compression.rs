//! Measures what lencode's dictionary encoding would do to shred counts and
//! Turbine wire bytes, using real mainnet data from a horizon `.jet` archive.
//!
//! A slot's shred payload is its serialized entry stream: `Vec<Entry>` where
//! `Entry { num_hashes: u64, hash: Hash, transactions: Vec<VersionedTransaction> }`,
//! bincode-encoded and chopped into data shreds. This example rebuilds that
//! stream per slot and compares three encodings:
//!
//! 1. `wire`: today's shred payload format (bincode, exactly as Turbine
//!    ships it in agave 3.x; swap to wincode, which is byte-identical, once
//!    the workspace reaches solana-transaction >= 3.1 with its `wincode`
//!    feature, currently blocked by an agave =3.0.2 pin).
//! 2. `lencode/tx`: lencode with the frozen 65,535-entry prime table, dedupe
//!    scratch reset per transaction. Models SIMD-0385-style per-transaction
//!    independent encoding (each transaction decodable alone).
//! 3. `lencode/slot`: same, but scratch accumulates across the slot. Models a
//!    leader encoding the slot stream (repeat pubkeys within the slot cost 4
//!    bytes after first sight).
//!
//! Bytes convert to shreds with the chained-merkle constants from SIMD-0504 /
//! SIMD-0317: 963 payload bytes per data shred (1051 max size - 88 header),
//! data shreds padded to 32-shred FEC sets, one coding shred per data shred,
//! 1228 bytes per packet on the wire.
//!
//! Data requirements: a `.jet` is sufficient. Entry hashes are not stored in
//! the archive (deliberately), so a 32-byte placeholder stands in. It
//! contributes identical bytes to every encoding (hashes never dedupe), so
//! the ratios are exact. `num_hashes`, tick structure, and transaction bytes
//! are all real. Old Faithful CARs would additionally provide the real entry
//! hashes for a byte-exact baseline cross-check, but change no ratio.
//!
//! Usage:
//!   cargo run --release -p jetstreamer-horizon --example shred_compression -- \
//!       <path.jet> [--max-slots N]

use std::fs::File;
use std::io::BufReader;

use jetstreamer_horizon::archive::{ArchiveReader, Consumption, SlotVisitor};
use jetstreamer_horizon::block_metas::BlockNotification;
use jetstreamer_horizon::entries::EntryRecord;
use jetstreamer_horizon::pubkey_prime::POPULAR_PUBKEYS;
use jetstreamer_horizon::transactions as htx;
use lencode::prelude::*;
use solana_address::Address;
use solana_entry::entry::Entry;
use solana_message::{
    Message as SolLegacyMessage, MessageHeader as SolMessageHeader,
    VersionedMessage as SolVersionedMessage, compiled_instruction::CompiledInstruction, v0,
};
use solana_transaction::versioned::VersionedTransaction;
use std::sync::Arc;

/// Neutral transport schema for `--dump-msgs`: real mainnet message bodies
/// (static account keys, blockhash, instructions) serialized with plain
/// bincode, consumed by lencode's `wire_speed` example for codec speed
/// comparisons (wincode vs lencode) outside this workspace's agave version
/// pins.
#[derive(serde::Serialize)]
struct DumpIx {
    program_id_index: u8,
    accounts: Vec<u8>,
    data: Vec<u8>,
}
#[derive(serde::Serialize)]
struct DumpMsg {
    account_keys: Vec<[u8; 32]>,
    recent_blockhash: [u8; 32],
    instructions: Vec<DumpIx>,
}

/// Version-neutral transaction shape used by the local lencode checkout.
///
/// This workspace still uses Solana v3 transaction containers for its
/// bincode/Turbine baseline, while lencode's full reference-type feature now
/// tracks Solana v4. Keeping the small Legacy/V0 wire shape here lets the
/// benchmark exercise current lencode (including `Address` dedupe) without
/// pulling two incompatible full SDK graphs into the Horizon crate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
struct LencodeMessageHeader {
    num_required_signatures: u8,
    num_readonly_signed_accounts: u8,
    num_readonly_unsigned_accounts: u8,
}

impl Encode for LencodeMessageHeader {
    fn encode_ext(
        &self,
        writer: &mut impl lencode::io::Write,
        ctx: Option<&mut EncoderContext>,
    ) -> lencode::Result<usize> {
        u32::from_le_bytes([
            self.num_required_signatures,
            self.num_readonly_signed_accounts,
            self.num_readonly_unsigned_accounts,
            0,
        ])
        .encode_ext(writer, ctx)
    }
}

impl Decode for LencodeMessageHeader {
    fn decode_ext(
        reader: &mut impl lencode::io::Read,
        ctx: Option<&mut DecoderContext>,
    ) -> lencode::Result<Self> {
        let bytes = u32::decode_ext(reader, ctx)?.to_le_bytes();
        Ok(Self {
            num_required_signatures: bytes[0],
            num_readonly_signed_accounts: bytes[1],
            num_readonly_unsigned_accounts: bytes[2],
        })
    }
}

#[derive(Encode, Decode, Debug, Clone, PartialEq, Eq)]
struct LencodeInstruction {
    program_id_index: u8,
    accounts: Vec<u8>,
    data: Vec<u8>,
}

#[derive(Encode, Decode, Debug, Clone, PartialEq, Eq)]
struct LencodeAddressTableLookup {
    account_key: Address,
    writable_indexes: Vec<u8>,
    readonly_indexes: Vec<u8>,
}

#[derive(Encode, Decode, Debug, Clone, PartialEq, Eq)]
struct LencodeLegacyMessage {
    header: LencodeMessageHeader,
    account_keys: Vec<Address>,
    recent_blockhash: [u8; 32],
    instructions: Vec<LencodeInstruction>,
}

#[derive(Encode, Decode, Debug, Clone, PartialEq, Eq)]
struct LencodeV0Message {
    header: LencodeMessageHeader,
    account_keys: Vec<Address>,
    recent_blockhash: [u8; 32],
    instructions: Vec<LencodeInstruction>,
    address_table_lookups: Vec<LencodeAddressTableLookup>,
}

#[derive(Encode, Decode, Debug, Clone, PartialEq, Eq)]
enum LencodeMessage {
    Legacy(LencodeLegacyMessage),
    V0(LencodeV0Message),
}

#[derive(Encode, Decode, Debug, Clone, PartialEq, Eq)]
struct LencodeTransaction {
    signatures: Vec<[u8; 64]>,
    message: LencodeMessage,
}

struct LencodeEntry {
    num_hashes: u64,
    transactions: Vec<LencodeTransaction>,
}

use lencode::context::{DecoderContext, EncoderContext};
use lencode::dedupe::{DedupeDecoder, DedupeEncodeable, DedupeEncoder};

// The example's own frozen dictionaries use the same stable `Address` type as
// Horizon. Keeping one dedupe type per context also avoids mixing independent
// ID spaces.
fn frozen_encoder() -> Arc<lencode::dedupe::FrozenEncoderState> {
    let mut primer = DedupeEncoder::new();
    for pk_bytes in POPULAR_PUBKEYS.iter() {
        let address = Address::new_from_array(*pk_bytes);
        primer.prime::<Address, <Address as DedupeEncodeable>::Hasher>(&address);
    }
    Arc::new(primer.freeze())
}

fn frozen_decoder() -> Arc<lencode::dedupe::FrozenDecoderState> {
    let mut primer = DedupeDecoder::new();
    for pk_bytes in POPULAR_PUBKEYS.iter() {
        primer.prime::<Address>(Address::new_from_array(*pk_bytes));
    }
    Arc::new(primer.freeze())
}

fn new_encoder_context(frozen: &Arc<lencode::dedupe::FrozenEncoderState>) -> EncoderContext {
    EncoderContext {
        dedupe: Some(DedupeEncoder::with_frozen(Arc::clone(frozen))),
        diff: None,
    }
}

fn reset_encoder(ctx: &mut EncoderContext) {
    if std::env::var_os("SHRED_NO_RESET").is_some() {
        return;
    }
    if let Some(enc) = ctx.dedupe.as_mut() {
        enc.clear();
    }
}

/// SIMD-0504: chained data shreds carry at most 1051 - 88 = 963 payload bytes.
const DATA_SHRED_PAYLOAD: u64 = 963;
/// SIMD-0317: FEC sets are exactly 32 data + 32 coding shreds.
const FEC_DATA_SHREDS: u64 = 32;
/// Shred packet size on the wire.
const PACKET_BYTES: u64 = 1228;

fn to_pubkey(a: impl AsRef<[u8]>) -> solana_pubkey::Pubkey {
    solana_pubkey::Pubkey::new_from_array(
        <[u8; 32]>::try_from(a.as_ref()).expect("address is 32 bytes"),
    )
}

fn to_sol_hash(h: impl AsRef<[u8]>) -> solana_hash::Hash {
    solana_hash::Hash::new_from_array(<[u8; 32]>::try_from(h.as_ref()).expect("hash is 32 bytes"))
}

fn to_instruction(ix: &htx::CompiledInstruction) -> CompiledInstruction {
    CompiledInstruction {
        program_id_index: ix.program_id_index,
        accounts: ix.accounts.as_slice().to_vec(),
        data: ix.data.as_slice().to_vec(),
    }
}

fn to_versioned(tx: &htx::Transaction) -> VersionedTransaction {
    let signatures = tx
        .signatures
        .iter()
        .map(|s| {
            solana_signature::Signature::from(
                <[u8; 64]>::try_from(s.as_ref()).expect("signature is 64 bytes"),
            )
        })
        .collect();
    let message = match &tx.message {
        htx::VersionedMessage::Legacy(m) => SolVersionedMessage::Legacy(SolLegacyMessage {
            header: SolMessageHeader {
                num_required_signatures: m.header.num_required_signatures,
                num_readonly_signed_accounts: m.header.num_readonly_signed_accounts,
                num_readonly_unsigned_accounts: m.header.num_readonly_unsigned_accounts,
            },
            account_keys: m.account_keys.iter().map(to_pubkey).collect(),
            recent_blockhash: to_sol_hash(m.recent_blockhash),
            instructions: m.instructions.iter().map(to_instruction).collect(),
        }),
        htx::VersionedMessage::V0(m) => SolVersionedMessage::V0(v0::Message {
            header: SolMessageHeader {
                num_required_signatures: m.header.num_required_signatures,
                num_readonly_signed_accounts: m.header.num_readonly_signed_accounts,
                num_readonly_unsigned_accounts: m.header.num_readonly_unsigned_accounts,
            },
            account_keys: m.account_keys.iter().map(to_pubkey).collect(),
            recent_blockhash: to_sol_hash(m.recent_blockhash),
            instructions: m.instructions.iter().map(to_instruction).collect(),
            address_table_lookups: m
                .address_table_lookups
                .iter()
                .map(|l| v0::MessageAddressTableLookup {
                    account_key: to_pubkey(l.account_key),
                    writable_indexes: l.writable_indexes.as_slice().to_vec(),
                    readonly_indexes: l.readonly_indexes.as_slice().to_vec(),
                })
                .collect(),
        }),
    };
    VersionedTransaction {
        signatures,
        message,
    }
}

fn lencode_header(header: &SolMessageHeader) -> LencodeMessageHeader {
    LencodeMessageHeader {
        num_required_signatures: header.num_required_signatures,
        num_readonly_signed_accounts: header.num_readonly_signed_accounts,
        num_readonly_unsigned_accounts: header.num_readonly_unsigned_accounts,
    }
}

fn lencode_address(pubkey: &solana_pubkey::Pubkey) -> Address {
    Address::new_from_array(pubkey.to_bytes())
}

fn lencode_instruction(ix: &CompiledInstruction) -> LencodeInstruction {
    LencodeInstruction {
        program_id_index: ix.program_id_index,
        accounts: ix.accounts.clone(),
        data: ix.data.clone(),
    }
}

fn to_lencode_transaction(tx: &VersionedTransaction) -> LencodeTransaction {
    let signatures = tx
        .signatures
        .iter()
        .map(|signature| <[u8; 64]>::try_from(signature.as_ref()).expect("signature is 64 bytes"))
        .collect();
    let message = match &tx.message {
        SolVersionedMessage::Legacy(message) => LencodeMessage::Legacy(LencodeLegacyMessage {
            header: lencode_header(&message.header),
            account_keys: message.account_keys.iter().map(lencode_address).collect(),
            recent_blockhash: message.recent_blockhash.to_bytes(),
            instructions: message
                .instructions
                .iter()
                .map(lencode_instruction)
                .collect(),
        }),
        SolVersionedMessage::V0(message) => LencodeMessage::V0(LencodeV0Message {
            header: lencode_header(&message.header),
            account_keys: message.account_keys.iter().map(lencode_address).collect(),
            recent_blockhash: message.recent_blockhash.to_bytes(),
            instructions: message
                .instructions
                .iter()
                .map(lencode_instruction)
                .collect(),
            address_table_lookups: message
                .address_table_lookups
                .iter()
                .map(|lookup| LencodeAddressTableLookup {
                    account_key: lencode_address(&lookup.account_key),
                    writable_indexes: lookup.writable_indexes.clone(),
                    readonly_indexes: lookup.readonly_indexes.clone(),
                })
                .collect(),
        }),
    };
    LencodeTransaction {
        signatures,
        message,
    }
}

/// Lencode-encodes one slot's entry stream. `reset_per_tx` selects the
/// per-transaction mode (scratch cleared before every transaction) versus the
/// slot-stream mode (scratch accumulates; cleared once per slot by the
/// caller).
fn lencode_slot(
    entries: &[LencodeEntry],
    ctx: &mut EncoderContext,
    reset_per_tx: bool,
    buf: &mut Vec<u8>,
) {
    buf.clear();
    (entries.len() as u64)
        .encode_ext(&mut *buf, Some(&mut *ctx))
        .expect("encode entry count");
    for entry in entries {
        entry
            .num_hashes
            .encode_ext(&mut *buf, Some(&mut *ctx))
            .expect("encode num_hashes");
        buf.extend_from_slice(&[0; 32]);
        (entry.transactions.len() as u64)
            .encode_ext(&mut *buf, Some(&mut *ctx))
            .expect("encode tx count");
        for tx in &entry.transactions {
            if reset_per_tx {
                reset_encoder(ctx);
            }
            tx.encode_ext(&mut *buf, Some(&mut *ctx))
                .expect("encode transaction");
        }
    }
}

/// Bytes -> (padded data shreds, wire bytes) under the modeled constants.
fn shred_model(payload_bytes: u64) -> (u64, u64) {
    let data = payload_bytes.div_ceil(DATA_SHRED_PAYLOAD).max(1);
    let padded = data.div_ceil(FEC_DATA_SHREDS) * FEC_DATA_SHREDS;
    let packets = padded * 2; // one coding shred per data shred
    (padded, packets * PACKET_BYTES)
}

#[derive(Default)]
struct ModeTotals {
    bytes: u64,
    data_shreds: u64,
    wire_bytes: u64,
}

impl ModeTotals {
    fn add_slot(&mut self, payload_bytes: u64) {
        let (shreds, wire) = shred_model(payload_bytes);
        self.bytes += payload_bytes;
        self.data_shreds += shreds;
        self.wire_bytes += wire;
    }
}

#[derive(Default)]
struct Collector {
    txs: Vec<VersionedTransaction>,
    slots: u64,
    blocks: u64,
    tx_total: u64,
    wire: ModeTotals,
    lencode_tx: ModeTotals,
    lencode_slot: ModeTotals,
    enc_buf: Vec<u8>,
    verified: bool,
    frozen_enc: Option<Arc<lencode::dedupe::FrozenEncoderState>>,
    frozen_dec: Option<Arc<lencode::dedupe::FrozenDecoderState>>,
    ctx: Option<EncoderContext>,
    dump: Option<(String, usize, Vec<DumpMsg>)>,
}

impl Collector {
    fn process_block(&mut self, entries: &[EntryRecord]) {
        let txs = std::mem::take(&mut self.txs);
        self.tx_total += txs.len() as u64;

        // Partition the slot's transactions into entries by tx_count.
        let mut stream: Vec<Entry> = Vec::with_capacity(entries.len());
        let mut cursor = 0usize;
        for record in entries {
            let take = record.tx_count as usize;
            let entry_txs = txs[cursor..cursor + take].to_vec();
            cursor += take;
            stream.push(Entry {
                num_hashes: record.num_hashes,
                // Entry hashes are not stored in the archive; a placeholder
                // contributes identical bytes to every encoding.
                hash: solana_hash::Hash::default(),
                transactions: entry_txs,
            });
        }
        assert_eq!(cursor, txs.len(), "entry tx counts must cover the slot");

        let lencode_stream = stream
            .iter()
            .map(|entry| LencodeEntry {
                num_hashes: entry.num_hashes,
                transactions: entry
                    .transactions
                    .iter()
                    .map(to_lencode_transaction)
                    .collect(),
            })
            .collect::<Vec<_>>();

        if std::env::var_os("SHRED_NO_BINCODE").is_none() {
            let baseline = bincode::serialize(&stream).expect("serialize entry stream");
            self.wire.add_slot(baseline.len() as u64);
        }

        // Per-transaction reset mode (SIMD-0385-style independence).
        let mut buf = std::mem::take(&mut self.enc_buf);
        let mut ctx = self.ctx.take().unwrap_or_else(|| {
            if std::env::var_os("SHRED_PLAIN_DEDUPE").is_some() {
                EncoderContext {
                    dedupe: Some(DedupeEncoder::new()),
                    diff: None,
                }
            } else {
                let frozen = self.frozen_enc.get_or_insert_with(frozen_encoder).clone();
                new_encoder_context(&frozen)
            }
        });
        if std::env::var_os("SHRED_NO_LENCODE_TX").is_none() {
            reset_encoder(&mut ctx);
            lencode_slot(&lencode_stream, &mut ctx, true, &mut buf);
            self.lencode_tx.add_slot(buf.len() as u64);
        }

        // Slot-stream mode (scratch accumulates across the slot).
        if std::env::var_os("SHRED_NO_LENCODE_SLOT").is_none() {
            reset_encoder(&mut ctx);
            lencode_slot(&lencode_stream, &mut ctx, false, &mut buf);
            self.lencode_slot.add_slot(buf.len() as u64);
        }
        self.ctx = Some(ctx);

        // One-time round-trip check on the first non-empty slot.
        if std::env::var_os("SHRED_NO_ROUNDTRIP").is_none()
            && std::env::var_os("SHRED_NO_LENCODE_SLOT").is_none()
            && !self.verified
            && !lencode_stream.is_empty()
            && lencode_stream
                .iter()
                .any(|entry| !entry.transactions.is_empty())
        {
            let frozen_dec = self.frozen_dec.get_or_insert_with(frozen_decoder).clone();
            verify_roundtrip(&lencode_stream, &buf, &frozen_dec);
            self.verified = true;
        }
        self.enc_buf = buf;

        self.blocks += 1;
    }
}

fn verify_roundtrip(
    stream: &[LencodeEntry],
    slot_mode_bytes: &[u8],
    frozen: &Arc<lencode::dedupe::FrozenDecoderState>,
) {
    let mut ctx = DecoderContext {
        dedupe: Some(DedupeDecoder::with_frozen(Arc::clone(frozen))),
        diff: None,
    };
    let mut cursor = std::io::Cursor::new(slot_mode_bytes);
    let count = u64::decode_ext(&mut cursor, Some(&mut ctx)).expect("decode entry count");
    assert_eq!(count as usize, stream.len());
    for entry in stream {
        let num_hashes = u64::decode_ext(&mut cursor, Some(&mut ctx)).expect("decode num_hashes");
        assert_eq!(num_hashes, entry.num_hashes);
        let mut hash = [0u8; 32];
        std::io::Read::read_exact(&mut cursor, &mut hash).expect("decode hash");
        let tx_count = u64::decode_ext(&mut cursor, Some(&mut ctx)).expect("decode tx count");
        assert_eq!(tx_count as usize, entry.transactions.len());
        for expected in &entry.transactions {
            let tx = LencodeTransaction::decode_ext(&mut cursor, Some(&mut ctx))
                .expect("decode transaction");
            assert_eq!(&tx, expected, "round-trip transaction mismatch");
        }
    }
    eprintln!("round-trip decode verified on first non-empty slot");
}

impl SlotVisitor for Collector {
    fn on_transaction(&mut self, _slot: u64, _tx_index: u32, tx: &htx::Transaction) {
        if let Some((path, count, msgs)) = self.dump.as_mut() {
            let (keys, blockhash, ixs) = match &tx.message {
                htx::VersionedMessage::Legacy(m) => (
                    m.account_keys.as_slice(),
                    m.recent_blockhash,
                    m.instructions.as_slice(),
                ),
                htx::VersionedMessage::V0(m) => (
                    m.account_keys.as_slice(),
                    m.recent_blockhash,
                    m.instructions.as_slice(),
                ),
            };
            msgs.push(DumpMsg {
                account_keys: keys
                    .iter()
                    .map(|k| <[u8; 32]>::try_from(k.as_ref()).expect("32-byte key"))
                    .collect(),
                recent_blockhash: <[u8; 32]>::try_from(blockhash.as_ref()).expect("32-byte hash"),
                instructions: ixs
                    .iter()
                    .map(|ix| DumpIx {
                        program_id_index: ix.program_id_index,
                        accounts: ix.accounts.as_slice().to_vec(),
                        data: ix.data.as_slice().to_vec(),
                    })
                    .collect(),
            });
            if msgs.len() >= *count {
                let bytes = bincode::serialize(&msgs).expect("serialize dump");
                std::fs::write(&*path, &bytes).expect("write dump");
                let mut prime = Vec::with_capacity(POPULAR_PUBKEYS.len() * 32);
                for pk in POPULAR_PUBKEYS.iter() {
                    prime.extend_from_slice(pk);
                }
                std::fs::write(format!("{path}.prime"), &prime).expect("write prime table");
                eprintln!(
                    "dumped {} messages ({} bytes) + prime table to {path}(.prime)",
                    msgs.len(),
                    bytes.len()
                );
                std::process::exit(0);
            }
            return;
        }
        self.txs.push(to_versioned(tx));
    }

    fn on_block(&mut self, notification: &BlockNotification, entries: &[EntryRecord]) {
        self.slots += 1;
        match notification {
            BlockNotification::Skipped(_) => {
                self.txs.clear();
            }
            _ if self.dump.is_some() => {}
            _ => self.process_block(entries),
        }
        if self.slots.is_multiple_of(10_000) {
            eprintln!(
                "processed {} slots ({} blocks, {} txs)",
                self.slots, self.blocks, self.tx_total
            );
        }
    }

    fn consumption(&self) -> Consumption {
        Consumption::all().without_account_update_data()
    }
}

fn print_mode(name: &str, m: &ModeTotals, baseline: &ModeTotals, blocks: u64) {
    let vs_bytes = 100.0 * (1.0 - m.bytes as f64 / baseline.bytes as f64);
    let vs_wire = 100.0 * (1.0 - m.wire_bytes as f64 / baseline.wire_bytes as f64);
    println!(
        "  {name:<14} {:>14} bytes  ({:>7.1} KiB/block)  {:>10} data shreds  {:>9.2} GiB wire  \
         [{:>5.1}% bytes, {:>5.1}% wire vs wire]",
        m.bytes,
        m.bytes as f64 / blocks.max(1) as f64 / 1024.0,
        m.data_shreds,
        m.wire_bytes as f64 / (1u64 << 30) as f64,
        vs_bytes,
        vs_wire,
    );
}

fn main() {
    // The horizon decode path moves multi-MiB scratch structures through the
    // stack (Transaction ~12 MiB, BlockNotification ~40 MiB by value in the
    // worst case); the repo's own pipelines run decode on threads with
    // enlarged stacks for the same reason.
    std::thread::Builder::new()
        .name("shred-compression".into())
        .stack_size(256 << 20)
        .spawn(run)
        .expect("spawn worker")
        .join()
        .expect("worker panicked");
}

fn run() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut path = None;
    let mut max_slots = u64::MAX;
    let mut dump: Option<(String, usize, Vec<DumpMsg>)> = None;
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--max-slots" => {
                i += 1;
                max_slots = args[i].parse().expect("--max-slots N");
            }
            "--dump-msgs" => {
                i += 1;
                let out = args[i].clone();
                i += 1;
                let n: usize = args[i].parse().expect("--dump-msgs PATH N");
                dump = Some((out, n, Vec::with_capacity(n)));
            }
            other if path.is_none() => path = Some(other.to_string()),
            other => {
                eprintln!("unknown argument: {other}");
                std::process::exit(2);
            }
        }
        i += 1;
    }
    let Some(path) = path else {
        eprintln!("usage: shred_compression <path.jet> [--max-slots N]");
        std::process::exit(2);
    };

    let file = File::open(&path).expect("open archive");
    let mut reader = ArchiveReader::open(BufReader::new(file)).expect("parse archive");
    let mut collector = Collector {
        dump,
        ..Collector::default()
    };
    reader
        .read_slots(0, max_slots, &mut collector)
        .expect("read slots");

    let c = &collector;
    println!(
        "\n=== {path} | {} slots, {} blocks, {} transactions ===",
        c.slots, c.blocks, c.tx_total
    );
    println!(
        "model: {DATA_SHRED_PAYLOAD} B payload/data shred, {FEC_DATA_SHREDS}+{FEC_DATA_SHREDS} \
         FEC sets, {PACKET_BYTES} B packets, entry hashes as 32-byte placeholders\n"
    );
    print_mode("wire (bincode)", &c.wire, &c.wire, c.blocks);
    print_mode("lencode/tx", &c.lencode_tx, &c.wire, c.blocks);
    print_mode("lencode/slot", &c.lencode_slot, &c.wire, c.blocks);
    println!(
        "\nlencode/tx   = frozen 65,535-entry table only, scratch reset per transaction \
         (SIMD-0385-style per-tx independence)"
    );
    println!("lencode/slot = scratch accumulates across the slot (leader-encoded slot stream)");
}
