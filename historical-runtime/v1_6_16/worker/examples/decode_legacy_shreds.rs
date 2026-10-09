use serde::{Deserialize, Serialize};
use solana_sdk::{hash::Hash, transaction::Transaction};
use std::{
    collections::{BTreeMap, btree_map::Entry as MapEntry},
    convert::TryInto,
    env,
    fs::File,
    io::{BufReader, BufWriter},
    path::Path,
};

const DATA_SHRED: u8 = 0b1010_0101;
const DATA_COMPLETE_SHRED: u8 = 0b0100_0000;
const LAST_SHRED_IN_SLOT: u8 = 0b1000_0000;
const SHRED_DATA_OFFSET: usize = 88;

#[derive(Deserialize)]
struct ScanState {
    status: String,
    matches: Vec<ScanMatch>,
}

#[derive(Deserialize)]
struct ScanMatch {
    column_family: Option<String>,
    key: String,
    sequence: u64,
    record_type: u8,
    value: String,
}

#[derive(Deserialize, Serialize)]
struct LedgerEntry {
    num_hashes: u64,
    hash: Hash,
    transactions: Vec<Transaction>,
}

#[derive(Serialize)]
struct Boundary {
    tick_ordinal: usize,
    entry_ordinal: usize,
    hash: String,
}

#[derive(Serialize)]
struct SlotReport {
    slot: u64,
    data_shreds: usize,
    first_shred_index: u64,
    last_shred_index: u64,
    entries: usize,
    tick_entries: usize,
    transaction_entries: usize,
    transactions: usize,
    boundaries: Vec<Boundary>,
    final_entry_hash: String,
}

struct VersionedValue {
    sequence: u64,
    record_type: u8,
    value: Vec<u8>,
}

fn decode_hex(value: &str) -> Result<Vec<u8>, String> {
    if value.len() % 2 != 0 {
        return Err(format!("odd-length hexadecimal value: {} digits", value.len()));
    }
    (0..value.len())
        .step_by(2)
        .map(|index| {
            u8::from_str_radix(&value[index..index + 2], 16)
                .map_err(|error| format!("invalid hexadecimal at digit {}: {}", index, error))
        })
        .collect()
}

fn parse_key(value: &str) -> Result<(u64, u64), String> {
    let key = decode_hex(value)?;
    if key.len() != 16 {
        return Err(format!("data-shred key is {} bytes, expected 16", key.len()));
    }
    let slot = u64::from_be_bytes(key[..8].try_into().unwrap());
    let index = u64::from_be_bytes(key[8..].try_into().unwrap());
    Ok((slot, index))
}

fn decode_slot(
    slot: u64,
    shreds: &BTreeMap<u64, Vec<u8>>,
    ticks_per_slot: usize,
) -> Result<SlotReport, String> {
    let first_shred_index = *shreds.keys().next().ok_or("slot has no data shreds")?;
    let last_shred_index = *shreds.keys().next_back().unwrap();
    let mut expected_index = first_shred_index;
    let mut block = Vec::new();
    let mut entries = Vec::new();
    for (&key_index, payload) in shreds {
        if key_index != expected_index {
            return Err(format!(
                "slot {} is missing data shred {} before {}",
                slot, expected_index, key_index
            ));
        }
        expected_index += 1;
        if payload.len() < SHRED_DATA_OFFSET {
            return Err(format!("slot {} shred {} is shorter than its header", slot, key_index));
        }
        if payload[64] != DATA_SHRED {
            return Err(format!("slot {} shred {} is not a data shred", slot, key_index));
        }
        let header_slot = u64::from_le_bytes(payload[65..73].try_into().unwrap());
        let header_index = u32::from_le_bytes(payload[73..77].try_into().unwrap()) as u64;
        if header_slot != slot || header_index != key_index {
            return Err(format!(
                "SST key ({}, {}) disagrees with shred header ({}, {})",
                slot, key_index, header_slot, header_index
            ));
        }
        let flags = payload[85];
        let size = u16::from_le_bytes(payload[86..88].try_into().unwrap()) as usize;
        if !(SHRED_DATA_OFFSET..=payload.len()).contains(&size) {
            return Err(format!(
                "slot {} shred {} has invalid data size {} for {} bytes",
                slot,
                key_index,
                size,
                payload.len()
            ));
        }
        block.extend_from_slice(&payload[SHRED_DATA_OFFSET..size]);
        if flags & (DATA_COMPLETE_SHRED | LAST_SHRED_IN_SLOT) != 0 {
            let mut decoded: Vec<LedgerEntry> = bincode::deserialize(&block).map_err(|error| {
                format!(
                    "slot {} data block ending at shred {} did not decode: {}",
                    slot, key_index, error
                )
            })?;
            entries.append(&mut decoded);
            block.clear();
        }
    }
    if !block.is_empty() {
        return Err(format!("slot {} ends with an incomplete data block", slot));
    }

    let mut tick_entries = 0usize;
    let mut transaction_entries = 0usize;
    let mut transactions = 0usize;
    let mut boundaries = Vec::new();
    for (entry_ordinal, entry) in entries.iter().enumerate() {
        if entry.transactions.is_empty() {
            tick_entries += 1;
            if tick_entries % ticks_per_slot == 0 {
                boundaries.push(Boundary {
                    tick_ordinal: tick_entries,
                    entry_ordinal,
                    hash: entry.hash.to_string(),
                });
            }
        } else {
            transaction_entries += 1;
            transactions += entry.transactions.len();
        }
        let _ = entry.num_hashes;
    }
    let final_entry_hash = entries
        .last()
        .ok_or_else(|| format!("slot {} decoded to no entries", slot))?
        .hash
        .to_string();
    Ok(SlotReport {
        slot,
        data_shreds: shreds.len(),
        first_shred_index,
        last_shred_index,
        entries: entries.len(),
        tick_entries,
        transaction_entries,
        transactions,
        boundaries,
        final_entry_hash,
    })
}

fn run(path: &Path, ticks_per_slot: usize) -> Result<Vec<SlotReport>, String> {
    if ticks_per_slot == 0 {
        return Err("ticks_per_slot must be positive".to_string());
    }
    let state: ScanState = serde_json::from_reader(BufReader::new(
        File::open(path).map_err(|error| format!("cannot open {}: {}", path.display(), error))?,
    ))
    .map_err(|error| format!("cannot parse {}: {}", path.display(), error))?;
    if state.status != "complete" {
        return Err(format!("refusing incomplete prefix scan with status {:?}", state.status));
    }

    let mut versions: BTreeMap<(u64, u64), VersionedValue> = BTreeMap::new();
    for item in state.matches {
        if item.column_family.as_deref() != Some("data_shred") {
            continue;
        }
        let key = parse_key(&item.key)?;
        let candidate = VersionedValue {
            sequence: item.sequence,
            record_type: item.record_type,
            value: decode_hex(&item.value)?,
        };
        match versions.entry(key) {
            MapEntry::Vacant(entry) => {
                entry.insert(candidate);
            }
            MapEntry::Occupied(mut entry) if candidate.sequence > entry.get().sequence => {
                entry.insert(candidate);
            }
            MapEntry::Occupied(entry) if candidate.sequence == entry.get().sequence => {
                if candidate.record_type != entry.get().record_type || candidate.value != entry.get().value {
                    return Err(format!("conflicting values at sequence {} for key {:?}", candidate.sequence, key));
                }
            }
            MapEntry::Occupied(_) => {}
        }
    }

    let mut slots: BTreeMap<u64, BTreeMap<u64, Vec<u8>>> = BTreeMap::new();
    for ((slot, index), value) in versions {
        match value.record_type {
            0 => {}
            1 => {
                slots.entry(slot).or_default().insert(index, value.value);
            }
            record_type => {
                return Err(format!(
                    "unsupported RocksDB record type {} at ({}, {})",
                    record_type, slot, index
                ));
            }
        }
    }
    slots
        .iter()
        .map(|(&slot, shreds)| decode_slot(slot, shreds, ticks_per_slot))
        .collect()
}

fn main() {
    let mut arguments = env::args_os();
    let program = arguments.next().unwrap();
    let path = arguments.next().unwrap_or_else(|| {
        eprintln!(
            "usage: {} SCAN_STATE_JSON TICKS_PER_SLOT",
            Path::new(&program).display()
        );
        std::process::exit(2);
    });
    let ticks_per_slot = arguments
        .next()
        .and_then(|value| value.into_string().ok())
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or_else(|| {
            eprintln!("TICKS_PER_SLOT must be a positive integer");
            std::process::exit(2);
        });
    if arguments.next().is_some() {
        eprintln!("unexpected extra arguments");
        std::process::exit(2);
    }
    match run(Path::new(&path), ticks_per_slot) {
        Ok(report) => {
            serde_json::to_writer_pretty(BufWriter::new(std::io::stdout()), &report).unwrap();
            println!();
        }
        Err(error) => {
            eprintln!("decode failed: {}", error);
            std::process::exit(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_shred(slot: u64, index: u32, entries: &[LedgerEntry]) -> Vec<u8> {
        let encoded = bincode::serialize(entries).unwrap();
        let mut payload = vec![0u8; SHRED_DATA_OFFSET + encoded.len()];
        payload[64] = DATA_SHRED;
        payload[65..73].copy_from_slice(&slot.to_le_bytes());
        payload[73..77].copy_from_slice(&index.to_le_bytes());
        payload[85] = DATA_COMPLETE_SHRED | LAST_SHRED_IN_SLOT;
        let size = payload.len() as u16;
        payload[86..88].copy_from_slice(&size.to_le_bytes());
        payload[SHRED_DATA_OFFSET..].copy_from_slice(&encoded);
        payload
    }

    #[test]
    fn reports_intermediate_tick_boundaries() {
        let entries = (1u8..=4)
            .map(|byte| LedgerEntry {
                num_hashes: 1,
                hash: Hash::new(&[byte; 32]),
                transactions: Vec::new(),
            })
            .collect::<Vec<_>>();
        let mut shreds = BTreeMap::new();
        shreds.insert(0, make_shred(117, 0, &entries));
        let report = decode_slot(117, &shreds, 2).unwrap();
        assert_eq!(report.tick_entries, 4);
        assert_eq!(report.boundaries.len(), 2);
        assert_eq!(report.boundaries[0].tick_ordinal, 2);
        assert_eq!(report.boundaries[0].entry_ordinal, 1);
        assert_eq!(report.boundaries[1].tick_ordinal, 4);
        assert_eq!(report.boundaries[1].entry_ordinal, 3);
    }
}
