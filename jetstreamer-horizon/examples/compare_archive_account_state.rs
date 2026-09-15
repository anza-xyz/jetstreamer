//! Compare the account state reconstructed from an unfinished replay archive
//! with a canonical snapshot fingerprint table.
//!
//! The two fingerprint inputs use the bounded `JSTRACCTFP1` diagnostic record
//! format parsed below. This tool is deliberately able to stream complete
//! buckets from an unfinished `.jet`: checkpoint failure prevents publication,
//! but the account writes preceding that failure remain useful for locating
//! historical-runtime divergence.

use std::{
    collections::{HashMap, HashSet},
    fs::File,
    io::{BufReader, Read, Seek, SeekFrom},
    path::Path,
    str::FromStr,
};

use jetstreamer_horizon::{
    account_updates::AccountUpdateView,
    archive::{
        BlockNotification, BucketDecoder, BucketHeader, Consumption, EpochMeta,
        MAX_FILE_HEADER_BYTES, SlotKind, SlotVisitor, parse_file_header,
    },
    transactions::{RewardType, Transaction},
};
use lencode::Decode;
use sha2::{Digest, Sha256};
use solana_address::Address;

const FINGERPRINT_MAGIC: &[u8; 12] = b"JSTRACCTFP1\0";
const FINGERPRINT_RECORD_LEN: u64 = 129;
const MAX_FINGERPRINTS: u64 = 10_000_000;

#[derive(Debug, Clone, PartialEq, Eq)]
struct AccountFingerprint {
    lamports: u64,
    owner: Address,
    executable: bool,
    rent_epoch: u64,
    storage_slot: u64,
    data_len: u64,
    data_sha256: [u8; 32],
}

#[derive(Debug, Clone)]
struct LastWrite {
    slot: u64,
    write_version: u64,
    phase: &'static str,
    transaction_index: Option<u32>,
}

#[derive(Debug, Clone)]
struct RewardEvent {
    slot: u64,
    lamports: i64,
    post_balance: u64,
    reward_type: Option<RewardType>,
}

struct Reconstructor {
    accounts: HashMap<Address, AccountFingerprint>,
    last_writes: HashMap<Address, LastWrite>,
    rewards: HashMap<Address, Vec<RewardEvent>>,
    watch: HashSet<Address>,
    updates: u64,
    first_slot: Option<u64>,
    last_slot: Option<u64>,
}

impl Reconstructor {
    fn record(
        &mut self,
        slot: u64,
        update: &AccountUpdateView<'_>,
        phase: &'static str,
        transaction_index: Option<u32>,
    ) {
        self.updates += 1;
        self.last_writes.insert(
            update.pubkey,
            LastWrite {
                slot,
                write_version: update.write_version,
                phase,
                transaction_index,
            },
        );
        if self.watch.contains(&update.pubkey) {
            let data_prefix = update.data[..update.data.len().min(256)]
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>();
            println!(
                "watched update pubkey={} slot={} write_version={} phase={} tx={:?} lamports={} owner={} executable={} rent_epoch={} data_len={} data_sha256={} data_prefix={}",
                update.pubkey,
                slot,
                update.write_version,
                phase,
                transaction_index,
                update.lamports,
                update.owner,
                update.executable,
                update.rent_epoch,
                update.data.len(),
                short_hash(&Sha256::digest(update.data).into()),
                data_prefix,
            );
        }
        if update.lamports == 0 {
            self.accounts.remove(&update.pubkey);
            return;
        }
        self.accounts.insert(
            update.pubkey,
            AccountFingerprint {
                lamports: update.lamports,
                owner: update.owner,
                executable: update.executable,
                rent_epoch: update.rent_epoch,
                storage_slot: slot,
                data_len: update.data.len() as u64,
                data_sha256: Sha256::digest(update.data).into(),
            },
        );
    }
}

impl SlotVisitor for Reconstructor {
    fn on_slot_start(&mut self, slot: u64, _kind: SlotKind) {
        self.first_slot.get_or_insert(slot);
        self.last_slot = Some(slot);
    }

    fn on_epoch(&mut self, meta: &EpochMeta) {
        for (update, data) in meta.updates.iter() {
            self.record(
                meta.first_block_slot,
                &AccountUpdateView {
                    pubkey: update.pubkey,
                    lamports: update.lamports,
                    owner: update.owner,
                    executable: update.executable,
                    rent_epoch: update.rent_epoch,
                    write_version: update.write_version,
                    data,
                },
                "epoch",
                None,
            );
        }
    }

    fn on_pre_account_update(&mut self, slot: u64, update: &AccountUpdateView<'_>) {
        self.record(slot, update, "pre", None);
    }

    fn on_transaction(&mut self, slot: u64, transaction_index: u32, tx: &Transaction) {
        for (update, data) in tx.iter_account_updates() {
            self.record(
                slot,
                &AccountUpdateView {
                    pubkey: update.pubkey,
                    lamports: update.lamports,
                    owner: update.owner,
                    executable: update.executable,
                    rent_epoch: update.rent_epoch,
                    write_version: update.write_version,
                    data,
                },
                "transaction",
                Some(transaction_index),
            );
        }
    }

    fn on_post_account_update(&mut self, slot: u64, update: &AccountUpdateView<'_>) {
        self.record(slot, update, "post", None);
    }

    fn on_block(
        &mut self,
        notification: &BlockNotification,
        _entries: &[jetstreamer_horizon::entries::EntryRecord],
    ) {
        let BlockNotification::Block(block) = notification else {
            return;
        };
        for reward in block.rewards.iter() {
            if self.watch.contains(&reward.pubkey) {
                println!(
                    "watched reward pubkey={} slot={} lamports={} post_balance={} type={:?}",
                    reward.pubkey,
                    block.slot,
                    reward.lamports,
                    reward.post_balance,
                    reward.reward_type,
                );
            }
            self.rewards
                .entry(reward.pubkey)
                .or_default()
                .push(RewardEvent {
                    slot: block.slot,
                    lamports: reward.lamports,
                    post_balance: reward.post_balance,
                    reward_type: reward.reward_type,
                });
        }
    }

    fn consumption(&self) -> Consumption {
        Consumption::all().without_block_account_update_arenas()
    }
}

fn read_array<const N: usize>(reader: &mut impl Read) -> Result<[u8; N], String> {
    let mut bytes = [0u8; N];
    reader
        .read_exact(&mut bytes)
        .map_err(|error| format!("read fingerprint table: {error}"))?;
    Ok(bytes)
}

fn read_u64(reader: &mut impl Read) -> Result<u64, String> {
    Ok(u64::from_le_bytes(read_array(reader)?))
}

fn read_fingerprints(path: &Path) -> Result<HashMap<Address, AccountFingerprint>, String> {
    let file = File::open(path).map_err(|error| format!("open {}: {error}", path.display()))?;
    let length = file
        .metadata()
        .map_err(|error| format!("stat {}: {error}", path.display()))?
        .len();
    let mut reader = BufReader::new(file);
    if &read_array::<12>(&mut reader)? != FINGERPRINT_MAGIC {
        return Err(format!(
            "{} has the wrong fingerprint magic",
            path.display()
        ));
    }
    let count = read_u64(&mut reader)?;
    if count > MAX_FINGERPRINTS {
        return Err(format!(
            "{} declares {count} fingerprints, limit is {MAX_FINGERPRINTS}",
            path.display()
        ));
    }
    let expected_length = 20u64
        .checked_add(
            count
                .checked_mul(FINGERPRINT_RECORD_LEN)
                .ok_or("fingerprint table length overflow")?,
        )
        .ok_or("fingerprint table length overflow")?;
    if length != expected_length {
        return Err(format!(
            "{} length is {length}, expected {expected_length}",
            path.display()
        ));
    }

    let mut accounts = HashMap::with_capacity(count as usize);
    for _ in 0..count {
        let pubkey = Address::new_from_array(read_array(&mut reader)?);
        let lamports = read_u64(&mut reader)?;
        let owner = Address::new_from_array(read_array(&mut reader)?);
        let executable = match read_array::<1>(&mut reader)?[0] {
            0 => false,
            1 => true,
            value => return Err(format!("invalid executable byte {value}")),
        };
        let rent_epoch = read_u64(&mut reader)?;
        let storage_slot = read_u64(&mut reader)?;
        let data_len = read_u64(&mut reader)?;
        let data_sha256 = read_array(&mut reader)?;
        // Historical AccountsDB scans may expose zero-lamport tombstones.
        // They are absent from the live state and from the accounts hash.
        if lamports != 0 {
            if accounts
                .insert(
                    pubkey,
                    AccountFingerprint {
                        lamports,
                        owner,
                        executable,
                        rent_epoch,
                        storage_slot,
                        data_len,
                        data_sha256,
                    },
                )
                .is_some()
            {
                return Err(format!("{} contains a duplicate pubkey", path.display()));
            }
        }
    }
    Ok(accounts)
}

fn state_equal(left: &AccountFingerprint, right: &AccountFingerprint) -> bool {
    left.lamports == right.lamports
        && left.owner == right.owner
        && left.executable == right.executable
        && left.rent_epoch == right.rent_epoch
        && left.data_len == right.data_len
        && left.data_sha256 == right.data_sha256
}

fn short_hash(hash: &[u8; 32]) -> String {
    hash[..8].iter().map(|byte| format!("{byte:02x}")).collect()
}

fn describe(value: Option<&AccountFingerprint>) -> String {
    match value {
        None => "absent".to_string(),
        Some(value) => format!(
            "lamports={} owner={} executable={} rent_epoch={} data_len={} data_sha256={} storage_slot={}",
            value.lamports,
            value.owner,
            value.executable,
            value.rent_epoch,
            value.data_len,
            short_hash(&value.data_sha256),
            value.storage_slot,
        ),
    }
}

fn describe_rewards(events: Option<&Vec<RewardEvent>>) -> String {
    events
        .map(|events| {
            events
                .iter()
                .map(|event| {
                    format!(
                        "slot={} lamports={} post_balance={} type={:?}",
                        event.slot, event.lamports, event.post_balance, event.reward_type
                    )
                })
                .collect::<Vec<_>>()
                .join("; ")
        })
        .unwrap_or_else(|| "none".to_string())
}

fn usage() -> ! {
    eprintln!(
        "usage: compare_archive_account_state <start.fp> <canonical.fp> <unfinished.jet>... [--end-slot SLOT] [--watch ADDRESS[,ADDRESS...]]"
    );
    std::process::exit(2);
}

fn apply_archive(
    archive_path: &Path,
    reconstructed: &mut Reconstructor,
    end_slot: Option<u64>,
) -> (u64, u64) {
    let mut source = BufReader::new(
        File::open(archive_path)
            .unwrap_or_else(|error| panic!("open {}: {error}", archive_path.display())),
    );
    let file_len = source.seek(SeekFrom::End(0)).unwrap();
    source.seek(SeekFrom::Start(0)).unwrap();
    let prefix_len = usize::try_from(file_len.min(MAX_FILE_HEADER_BYTES + 32)).unwrap();
    let mut prefix = vec![0; prefix_len];
    source.read_exact(&mut prefix).unwrap();
    let (header, mut offset) = parse_file_header(&prefix).unwrap();
    let mut decoder = BucketDecoder::for_file_header(&header).unwrap();
    decoder.materialize_account_data = true;
    decoder.materialize_block_account_update_arenas = false;
    let mut buckets = 0u64;
    let mut slots = 0u64;

    while (offset as u64) < file_len {
        source.seek(SeekFrom::Start(offset as u64)).unwrap();
        let bucket = match BucketHeader::decode_ext(&mut source, None) {
            Ok(bucket) => bucket,
            Err(error) => {
                eprintln!("stopped before incomplete trailing bucket header at {offset}: {error}");
                break;
            }
        };
        if end_slot.is_some_and(|end_slot| bucket.first_slot > end_slot) {
            break;
        }
        let payload_offset = source.stream_position().unwrap();
        let Some(bucket_end) = payload_offset.checked_add(bucket.stored_len) else {
            panic!("bucket at {offset} overflows its stored range");
        };
        if bucket_end > file_len {
            eprintln!(
                "stopped before incomplete trailing bucket at {offset}: end={bucket_end} file_len={file_len}"
            );
            break;
        }
        let raw_len = usize::try_from(bucket_end - offset as u64).unwrap();
        let mut raw = vec![0; raw_len];
        source.seek(SeekFrom::Start(offset as u64)).unwrap();
        source.read_exact(&mut raw).unwrap();
        let max_slots = end_slot.map_or(u64::from(bucket.slot_count), |end_slot| {
            end_slot
                .saturating_sub(bucket.first_slot)
                .saturating_add(1)
                .min(u64::from(bucket.slot_count))
        });
        slots += decoder
            .decode_bucket(&raw, 0, max_slots, reconstructed)
            .unwrap_or_else(|error| panic!("decode bucket at {offset}: {error}"));
        buckets += 1;
        offset = usize::try_from(bucket_end).unwrap();
    }

    println!(
        "decoded archive={} buckets={buckets} slots={slots}",
        archive_path.display()
    );
    (buckets, slots)
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 4 {
        usage();
    }
    let start_path = Path::new(&args[1]);
    let canonical_path = Path::new(&args[2]);
    let archive_end = args
        .iter()
        .position(|value| value.starts_with("--"))
        .unwrap_or(args.len());
    if archive_end < 4 {
        usage();
    }
    let mut end_slot = None;
    let mut watch = HashSet::new();
    let mut index = archive_end;
    while index < args.len() {
        let value = args.get(index + 1).unwrap_or_else(|| usage());
        match args[index].as_str() {
            "--end-slot" if end_slot.is_none() => {
                end_slot = Some(value.parse::<u64>().unwrap_or_else(|_| usage()));
            }
            "--watch" if watch.is_empty() => {
                watch = value
                    .split(',')
                    .map(|value| Address::from_str(value).unwrap_or_else(|_| usage()))
                    .collect();
            }
            _ => usage(),
        }
        index += 2;
    }
    let archive_paths: Vec<_> = args[3..archive_end].iter().map(Path::new).collect();
    if archive_paths.is_empty() {
        usage();
    }
    let start = read_fingerprints(start_path).unwrap_or_else(|error| panic!("{error}"));
    let canonical = read_fingerprints(canonical_path).unwrap_or_else(|error| panic!("{error}"));
    for pubkey in &watch {
        println!(
            "watched boundary states pubkey={pubkey}\n  start={}\n  canonical={}",
            describe(start.get(pubkey)),
            describe(canonical.get(pubkey)),
        );
    }
    let mut reconstructed = Reconstructor {
        accounts: start,
        last_writes: HashMap::new(),
        rewards: HashMap::new(),
        watch,
        updates: 0,
        first_slot: None,
        last_slot: None,
    };
    let (buckets, slots) = archive_paths
        .iter()
        .map(|path| apply_archive(path, &mut reconstructed, end_slot))
        .fold(
            (0, 0),
            |(left_buckets, left_slots), (right_buckets, right_slots)| {
                (left_buckets + right_buckets, left_slots + right_slots)
            },
        );

    println!(
        "decoded total archives={} buckets={buckets} slots={slots} first_slot={:?} last_slot={:?} updates={}",
        archive_paths.len(),
        reconstructed.first_slot,
        reconstructed.last_slot,
        reconstructed.updates
    );

    let mut keys = HashSet::with_capacity(reconstructed.accounts.len() + canonical.len());
    keys.extend(reconstructed.accounts.keys().copied());
    keys.extend(canonical.keys().copied());
    let mut keys: Vec<_> = keys.into_iter().collect();
    keys.sort_unstable_by_key(|pubkey| {
        (
            canonical
                .get(pubkey)
                .map(|account| account.storage_slot)
                .unwrap_or(u64::MAX),
            pubkey.to_bytes(),
        )
    });

    let mut mismatches = 0u64;
    let mut lamports = 0u64;
    let mut owner = 0u64;
    let mut executable = 0u64;
    let mut rent_epoch = 0u64;
    let mut data = 0u64;
    let mut missing = 0u64;
    let mut extra = 0u64;
    let mut same_write_slot_mismatches = 0u64;
    let decoded_first_slot = reconstructed.first_slot.unwrap_or(u64::MAX);
    let decoded_last_slot = reconstructed.last_slot.unwrap_or_default();
    for pubkey in keys {
        let replay = reconstructed.accounts.get(&pubkey);
        let expected = canonical.get(&pubkey);
        if matches!((replay, expected), (Some(left), Some(right)) if state_equal(left, right)) {
            continue;
        }
        mismatches += 1;
        match (replay, expected) {
            (Some(left), Some(right)) => {
                lamports += u64::from(left.lamports != right.lamports);
                owner += u64::from(left.owner != right.owner);
                executable += u64::from(left.executable != right.executable);
                rent_epoch += u64::from(left.rent_epoch != right.rent_epoch);
                data += u64::from(
                    left.data_len != right.data_len || left.data_sha256 != right.data_sha256,
                );
            }
            (None, Some(_)) => missing += 1,
            (Some(_), None) => extra += 1,
            (None, None) => unreachable!(),
        }
        let same_write_slot = matches!(
            (replay, expected),
            (Some(left), Some(right))
                if left.storage_slot == right.storage_slot
                    && right.storage_slot >= decoded_first_slot
                    && right.storage_slot <= decoded_last_slot
                    && reconstructed.last_writes.contains_key(&pubkey)
        );
        if same_write_slot {
            same_write_slot_mismatches += 1;
        }
        if same_write_slot && same_write_slot_mismatches <= 200 {
            let last_write = reconstructed
                .last_writes
                .get(&pubkey)
                .map(|write| {
                    format!(
                        "slot={} write_version={} phase={} tx={:?}",
                        write.slot, write.write_version, write.phase, write.transaction_index
                    )
                })
                .unwrap_or_else(|| "none".to_string());
            println!(
                "same-write-slot mismatch #{same_write_slot_mismatches} (overall #{mismatches}) pubkey={pubkey} last_write=[{last_write}] rewards=[{}]\n  replay={}\n  canonical={}",
                describe_rewards(reconstructed.rewards.get(&pubkey)),
                describe(replay),
                describe(expected),
            );
        }
    }
    println!(
        "mismatches={mismatches} same_write_slot_mismatches={same_write_slot_mismatches} missing={missing} extra={extra} lamports={lamports} owner={owner} executable={executable} rent_epoch={rent_epoch} data={data}"
    );
    if mismatches != 0 {
        std::process::exit(1);
    }
}
