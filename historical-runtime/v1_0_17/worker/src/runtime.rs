use crate::{leader_schedule, poh_backend, snapshot};
use jetstreamer_historical_protocol::{
    AccountWrite, Checkpoint, EntryProcessed, EntryRequest, Initialized, InitializedSource,
    InstructionError as WireInstructionError, TransactionError as WireTransactionError,
    TransactionOutcome, MAX_ENTRIES_PER_BATCH,
};
use rayon::{prelude::*, ThreadPool, ThreadPoolBuilder};
use solana_config_program::config_processor;
use solana_merkle_tree::MerkleTree;
use solana_rayon_threadlimit::get_thread_count;
use solana_runtime::{accounts_db::OwnedAccountWrite, bank::Bank};
use solana_sdk::{
    clock::{MAX_PROCESSING_AGE, MAX_RECENT_BLOCKHASHES},
    genesis_config::{GenesisConfig, OperatingMode},
    hash::Hash,
    instruction::InstructionError,
    pubkey::Pubkey,
    system_program,
    sysvar::{slot_hashes, slot_hashes::SlotHashes, Sysvar},
    transaction::{Transaction, TransactionError},
};
use solana_stake_program::stake_instruction;
use solana_vote_program::vote_instruction::VoteInstruction;
use std::{cmp, collections::HashMap, env, path::Path, sync::Arc};
use tempfile::TempDir;

const MAX_AGE_CORRECTION_EPOCH: u64 = 14;
// Epoch 11 starts at 4,752,000. The immediately preceding canonical snapshot
// is at 4,751,796, so admit only the 203-slot bootstrap bridge and epoch 11
// itself. Compatibility must be demonstrated by trusted checkpoints before
// the parent may route or publish with this candidate.
const MIN_SUPPORTED_SNAPSHOT_SLOT: u64 = 4_751_796;
const MIN_SUPPORTED_ENTRY_SLOT: u64 = MIN_SUPPORTED_SNAPSHOT_SLOT + 1;
const MAX_SUPPORTED_SLOT_EXCLUSIVE: u64 = 5_184_000;
const MAX_SUPPORTED_EPOCH: u64 = 11;
const POH_THREADS_ENV: &str = "JETSTREAMER_HISTORICAL_POH_THREADS";
const ABSOLUTE_MAX_POH_THREADS: usize = 256;

pub struct RuntimeState {
    bank: Arc<Bank>,
    stable_cluster: bool,
    _source: InitializedSource,
    _state_dir: TempDir,
    write_cursor: u64,
    next_entry_index: u64,
    last_entry_hash: Hash,
    tick_hash_count: u64,
    leader_schedules: HashMap<u64, Vec<Pubkey>>,
    poh_pool: ThreadPool,
    enforce_candidate_range: bool,
}

pub enum ProcessEntriesError<E> {
    Runtime(String),
    Emit(E),
}

impl RuntimeState {
    pub fn initialize(
        ledger_path: &str,
        initial_state: &jetstreamer_historical_protocol::InitialState,
        scratch_root: Option<&str>,
    ) -> Result<(Self, Initialized), String> {
        let state_dir = snapshot::private_state_dir(scratch_root)?;
        let genesis = snapshot::load_genesis(Path::new(ledger_path), &state_dir)?;
        let genesis_hash = genesis.hash().to_string();
        if genesis_hash != jetstreamer_historical_protocol::MAINNET_GENESIS_HASH {
            return Err(format!(
                "genesis hash {} does not match required mainnet hash {}",
                genesis_hash,
                jetstreamer_historical_protocol::MAINNET_GENESIS_HASH
            ));
        }
        let stable_cluster = genesis.operating_mode == OperatingMode::Stable;
        let poh_pool = build_poh_pool()?;

        let (mut bank, source) = match initial_state {
            jetstreamer_historical_protocol::InitialState::SnapshotArchive { archive_path } => {
                let archive_path_ref = Path::new(archive_path);
                validate_candidate_snapshot_slot(snapshot::archive_slot(archive_path_ref)?)?;
                let loaded = snapshot::load_archive(archive_path_ref, &state_dir)?;
                validate_candidate_snapshot_slot(loaded.bank.slot())?;
                let source = InitializedSource::SnapshotArchive {
                    archive_path: archive_path.clone(),
                    expected_accounts_hash: loaded.expected_accounts_hash.as_ref().to_vec(),
                };
                (loaded.bank, source)
            }
            jetstreamer_historical_protocol::InitialState::Genesis => {
                return Err(format!(
                    "Solana v1.0.17 epoch-11 candidate requires a snapshot at or after slot {}",
                    MIN_SUPPORTED_SNAPSHOT_SLOT
                ));
            }
        };
        restore_mainnet_runtime_hooks(&mut bank, &genesis)?;
        let write_cursor = bank.accounts().accounts_db.next_write_version();
        let last_entry_hash = bank.last_blockhash();
        let initialized = Initialized {
            genesis_hash,
            source: source.clone(),
            slot: bank.slot(),
            last_blockhash: bank.last_blockhash().as_ref().to_vec(),
            ticks_per_slot: bank.ticks_per_slot(),
            next_write_version: write_cursor,
        };
        Ok((
            Self {
                bank: Arc::new(bank),
                stable_cluster,
                _source: source,
                _state_dir: state_dir,
                write_cursor,
                next_entry_index: 0,
                last_entry_hash,
                tick_hash_count: 0,
                leader_schedules: HashMap::new(),
                poh_pool,
                enforce_candidate_range: true,
            },
            initialized,
        ))
    }

    /// Preserve the original one-entry request surface while sharing exactly
    /// the same fail-closed preparation and commit path as a batch.
    pub fn process_entry(&mut self, request: EntryRequest) -> Result<EntryProcessed, String> {
        let mut processed = self.process_entries(vec![request])?;
        Ok(processed.remove(0))
    }

    /// Validate the complete submitted batch before changing `Bank` state.
    /// Transaction decoding and independent PoH segments run in parallel;
    /// bank advances, transaction execution, results, and writes remain in
    /// canonical entry order.
    pub fn process_entries(
        &mut self,
        requests: Vec<EntryRequest>,
    ) -> Result<Vec<EntryProcessed>, String> {
        let mut processed = Vec::with_capacity(requests.len());
        match self.process_entries_with(requests, |entry| {
            processed.push(entry);
            Ok::<(), ()>(())
        }) {
            Ok(()) => Ok(processed),
            Err(ProcessEntriesError::Runtime(message)) => Err(message),
            Err(ProcessEntriesError::Emit(())) => unreachable!(),
        }
    }

    pub fn process_entries_with<E, F>(
        &mut self,
        requests: Vec<EntryRequest>,
        mut emit: F,
    ) -> Result<(), ProcessEntriesError<E>>
    where
        F: FnMut(EntryProcessed) -> Result<(), E>,
    {
        if requests.is_empty() {
            return Err(ProcessEntriesError::Runtime(
                "entry batch must contain at least one entry".to_string(),
            ));
        }
        if requests.len() > MAX_ENTRIES_PER_BATCH {
            return Err(ProcessEntriesError::Runtime(format!(
                "entry batch contains {} entries, limit is {}",
                requests.len(),
                MAX_ENTRIES_PER_BATCH
            )));
        }
        if self.enforce_candidate_range {
            for request in &requests {
                validate_candidate_entry_slot(request.slot)
                    .map_err(ProcessEntriesError::Runtime)?;
            }
        }

        let prepared_results: Vec<Result<PreparedEntry, String>> = self
            .poh_pool
            .install(|| requests.into_par_iter().map(prepare_entry).collect());
        // Inspect in wire order so a malformed batch has deterministic error
        // precedence even though all independent decoding ran concurrently.
        let mut prepared = Vec::with_capacity(prepared_results.len());
        for result in prepared_results {
            prepared.push(result.map_err(ProcessEntriesError::Runtime)?);
        }
        let validations = self
            .validate_entry_batch(&prepared)
            .map_err(ProcessEntriesError::Runtime)?;

        // Nothing above this line mutates the Bank. From here onward each
        // accepted entry is applied and reported in canonical wire order.
        for (entry, validation) in prepared.into_iter().zip(validations) {
            let processed = self
                .commit_prevalidated_entry(entry, validation)
                .map_err(ProcessEntriesError::Runtime)?;
            emit(processed).map_err(ProcessEntriesError::Emit)?;
        }
        Ok(())
    }

    fn commit_prevalidated_entry(
        &mut self,
        prepared: PreparedEntry,
        validation: EntryValidation,
    ) -> Result<EntryProcessed, String> {
        let PreparedEntry {
            request,
            transactions,
            signatures_by_writable_key,
            ..
        } = prepared;
        let mut writes = if request.slot > self.bank.slot() {
            self.advance_to(request.slot)?
        } else {
            Vec::new()
        };

        let mut outcomes = Vec::new();
        if transactions.is_empty() {
            self.bank.register_tick(&validation.hash);
            writes.extend(self.drain_writes(None)?);
        } else {
            let max_age = processing_max_age(self.stable_cluster, self.bank.epoch());
            let batch = self.bank.prepare_batch(&transactions, None);
            if let Some(error) = batch.lock_results().iter().find(|result| result.is_err()) {
                return Err(format!("entry account locking failed: {:?}", error));
            }
            let (results, _balances) = self
                .bank
                .load_execute_and_commit_transactions(&batch, max_age, false);
            drop(batch);
            if env::var_os("JETSTREAMER_HISTORICAL_TRACE").is_some() {
                for (transaction, result) in transactions.iter().zip(&results.processing_results) {
                    if matches!(
                        result.0,
                        Err(TransactionError::InstructionError(
                            _,
                            InstructionError::CustomError(2),
                        ))
                    ) {
                        let slot_hashes = self
                            .bank
                            .get_account(&slot_hashes::id())
                            .and_then(|account| SlotHashes::from_account(&account));
                        let vote =
                            transaction
                                .message
                                .instructions
                                .get(0)
                                .and_then(|instruction| {
                                    bincode::deserialize::<VoteInstruction>(&instruction.data).ok()
                                });
                        eprintln!(
                            "historical trace: bank_slot={} parent_slot={} parent_hash={:?} vote={:?} slot_hashes={:?}",
                            self.bank.slot(),
                            self.bank.parent_slot(),
                            self.bank.parent().map(|parent| parent.hash()),
                            vote,
                            slot_hashes,
                        );
                    }
                }
            }
            if let Some((index, error)) = results
                .fee_collection_results
                .iter()
                .enumerate()
                .find(|&(_index, ref result)| result.is_err())
            {
                return Err(format!(
                    "entry fee collection failed for transaction {}: {:?}",
                    index, error
                ));
            }
            for (index, transaction) in transactions.iter().enumerate() {
                outcomes.push(TransactionOutcome {
                    signature: transaction
                        .signatures
                        .get(0)
                        .map(|signature| signature.as_ref().to_vec()),
                    error: results.processing_results[index]
                        .0
                        .clone()
                        .err()
                        .map(normalize_transaction_error),
                });
            }
            let mut entry_writes = self.drain_writes(None)?;
            for write in entry_writes.iter_mut() {
                write.transaction_signature = signatures_by_writable_key
                    .get(write.pubkey.as_slice())
                    .cloned()
                    .unwrap_or(None);
            }
            writes.extend(entry_writes);
        }

        self.next_entry_index = self
            .next_entry_index
            .checked_add(1)
            .ok_or_else(|| "entry index overflowed u64".to_string())?;
        self.last_entry_hash = validation.hash;
        self.tick_hash_count = validation.next_tick_hash_count;
        Ok(EntryProcessed {
            slot: self.bank.slot(),
            entry_index: request.entry_index,
            outcomes,
            writes,
            tick_height: self.bank.tick_height(),
            slot_complete: self.bank.is_complete(),
            next_write_version: self.write_cursor,
        })
    }

    fn validate_entry_batch(
        &self,
        prepared: &[PreparedEntry],
    ) -> Result<Vec<EntryValidation>, String> {
        let mut current_slot = self.bank.slot();
        let mut next_entry_index = self.next_entry_index;
        let mut tick_height = self.bank.tick_height();
        let mut max_tick_height = self.bank.max_tick_height();
        let mut slot_complete = self.bank.is_complete();
        let mut bank_frozen = self.bank.is_frozen();
        let mut tick_hash_count = self.tick_hash_count;
        let mut previous_hash = self.last_entry_hash;
        let hashes_per_tick = self.bank.hashes_per_tick().unwrap_or(0);
        let ticks_per_slot = self.bank.ticks_per_slot();
        let mut starts = Vec::with_capacity(prepared.len());
        let mut validations = Vec::with_capacity(prepared.len());

        for (batch_index, entry) in prepared.iter().enumerate() {
            let request = &entry.request;
            if self.enforce_candidate_range {
                validate_candidate_entry_slot(request.slot)?;
            }
            if request.slot < current_slot {
                return Err(format!(
                    "entry slot {} precedes current bank slot {}",
                    request.slot, current_slot
                ));
            }

            let starts_new_slot = request.slot > current_slot;
            if starts_new_slot {
                if !slot_complete {
                    return Err(format!(
                        "cannot advance incomplete bank at slot {} (tick height {}, max {})",
                        current_slot, tick_height, max_tick_height
                    ));
                }
                current_slot = request.slot;
                next_entry_index = 0;
                max_tick_height = (current_slot + 1) * ticks_per_slot;
                slot_complete = tick_height == max_tick_height;
                bank_frozen = false;
                tick_hash_count = 0;
            }

            if request.entry_index != next_entry_index {
                return Err(format!(
                    "entry index {} for slot {} does not match expected {}",
                    request.entry_index, request.slot, next_entry_index
                ));
            }
            if slot_complete {
                return Err(format!(
                    "entry follows the completion tick for slot {}",
                    request.slot
                ));
            }
            if bank_frozen {
                return Err(format!("bank at slot {} is already frozen", request.slot));
            }
            if tick_height >= max_tick_height {
                return Err(format!(
                    "entry follows the completion tick for slot {}",
                    request.slot
                ));
            }

            let is_tick = entry.transactions.is_empty();
            if is_tick && tick_height + 1 > max_tick_height {
                return Err(format!(
                    "tick entry would exceed max tick height {} in slot {}",
                    max_tick_height, request.slot
                ));
            }

            let mut next_tick_hash_count = tick_hash_count;
            if hashes_per_tick != 0 {
                next_tick_hash_count = next_tick_hash_count
                    .checked_add(request.num_hashes)
                    .ok_or_else(|| "entry PoH hash count overflowed u64".to_string())?;
                if is_tick {
                    if next_tick_hash_count != hashes_per_tick {
                        return Err(format!(
                            "tick entry has {} accumulated PoH hashes, expected {}",
                            next_tick_hash_count, hashes_per_tick
                        ));
                    }
                    next_tick_hash_count = 0;
                } else if next_tick_hash_count >= hashes_per_tick {
                    return Err(format!(
                        "transaction entry reaches {} accumulated PoH hashes without a tick (limit {})",
                        next_tick_hash_count, hashes_per_tick
                    ));
                }
            }

            let start_hash = if batch_index == 0 && starts_new_slot {
                self.bank.last_blockhash()
            } else {
                previous_hash
            };
            starts.push(start_hash);
            validations.push(EntryValidation {
                hash: entry.hash,
                next_tick_hash_count,
            });

            previous_hash = entry.hash;
            tick_hash_count = next_tick_hash_count;
            next_entry_index = next_entry_index
                .checked_add(1)
                .ok_or_else(|| "entry index overflowed u64".to_string())?;
            if is_tick {
                tick_height += 1;
                slot_complete = tick_height == max_tick_height;
            }
        }

        let expected_hash_groups: Vec<(Hash, Option<Hash>)> = self.poh_pool.install(|| {
            prepared
                .par_chunks(2)
                .zip(starts.par_chunks(2))
                .map(|(entries, start_hashes)| match (entries, start_hashes) {
                    ([first, second], [first_start, second_start]) => {
                        let hashes = next_entry_hash_pair(
                            [first_start, second_start],
                            [first.request.num_hashes, second.request.num_hashes],
                            [&first.transactions, &second.transactions],
                        );
                        (hashes[0], Some(hashes[1]))
                    }
                    ([entry], [start_hash]) => (
                        next_entry_hash(start_hash, entry.request.num_hashes, &entry.transactions),
                        None,
                    ),
                    _ => unreachable!("matching chunks of two have equal lengths"),
                })
                .collect()
        });
        let mut expected_hashes = Vec::with_capacity(prepared.len());
        for (first, second) in expected_hash_groups {
            expected_hashes.push(first);
            if let Some(second) = second {
                expected_hashes.push(second);
            }
        }
        for ((entry, validation), expected_hash) in
            prepared.iter().zip(validations.iter()).zip(expected_hashes)
        {
            if validation.hash != expected_hash {
                return Err(format!(
                    "entry PoH hash mismatch at slot {} index {}: expected {}, got {}",
                    entry.request.slot, entry.request.entry_index, expected_hash, validation.hash
                ));
            }
        }
        Ok(validations)
    }

    pub fn freeze_checkpoint(&mut self, expected_slot: u64) -> Result<Checkpoint, String> {
        if expected_slot != self.bank.slot() {
            return Err(format!(
                "checkpoint requested for slot {}, current bank is {}",
                expected_slot,
                self.bank.slot()
            ));
        }
        if !self.bank.is_complete() {
            return Err(format!(
                "cannot checkpoint incomplete bank at slot {} (tick height {}, max {})",
                self.bank.slot(),
                self.bank.tick_height(),
                self.bank.max_tick_height()
            ));
        }
        if self.bank.hashes_per_tick().unwrap_or(0) != 0 && self.tick_hash_count != 0 {
            return Err(format!(
                "cannot checkpoint slot {} with {} uncommitted PoH hashes",
                self.bank.slot(),
                self.tick_hash_count
            ));
        }
        let writes = self.freeze_root_and_drain()?;
        let accounts_hash = self.bank.update_accounts_hash();
        Ok(Checkpoint {
            slot: self.bank.slot(),
            bank_hash: self.bank.hash().as_ref().to_vec(),
            accounts_hash: accounts_hash.as_ref().to_vec(),
            last_blockhash: self.bank.last_blockhash().as_ref().to_vec(),
            capitalization: self.bank.capitalization(),
            transaction_count: self.bank.transaction_count(),
            tick_height: self.bank.tick_height(),
            slot_complete: self.bank.is_complete(),
            writes,
            next_write_version: self.write_cursor,
        })
    }

    fn advance_to(&mut self, slot: u64) -> Result<Vec<AccountWrite>, String> {
        if !self.bank.is_complete() {
            return Err(format!(
                "cannot advance incomplete bank at slot {} (tick height {}, max {})",
                self.bank.slot(),
                self.bank.tick_height(),
                self.bank.max_tick_height()
            ));
        }
        let mut writes = self.freeze_root_and_drain()?;
        let child_poh_start = self.bank.last_blockhash();
        let leader = leader_schedule::slot_leader(&self.bank, slot, &mut self.leader_schedules)?;
        let mut child = Bank::new_from_parent(&self.bank, &leader, slot);
        restore_mainnet_native_processors(&mut child);
        self.bank = Arc::new(child);
        self.next_entry_index = 0;
        self.last_entry_hash = child_poh_start;
        self.tick_hash_count = 0;
        writes.extend(self.drain_writes(None)?);
        Ok(writes)
    }

    /// A completed bank is a canonical boundary in this linear replay. Drain
    /// every freeze-time account write before rooting it, then squash its
    /// ancestors so the next child retains only one parent Bank allocation.
    fn freeze_root_and_drain(&mut self) -> Result<Vec<AccountWrite>, String> {
        if !self.bank.is_complete() {
            return Err(format!(
                "cannot root incomplete bank at slot {} (tick height {}, max {})",
                self.bank.slot(),
                self.bank.tick_height(),
                self.bank.max_tick_height()
            ));
        }
        self.bank.freeze();
        let mut writes = self.drain_writes(None)?;
        self.bank.squash();
        // Bank::squash does not currently store accounts, but draining again
        // makes that implementation detail unable to silently lose a write.
        writes.extend(self.drain_writes(None)?);
        Ok(writes)
    }

    fn drain_writes(&mut self, signature: Option<Vec<u8>>) -> Result<Vec<AccountWrite>, String> {
        let (next, raw_writes) = self
            .bank
            .accounts()
            .accounts_db
            .ordered_account_writes_since(self.bank.slot(), self.write_cursor);
        if let Some(first) = raw_writes.first() {
            if first.write_version != self.write_cursor {
                return Err(format!(
                    "account write stream skipped version {} (first observed {})",
                    self.write_cursor, first.write_version
                ));
            }
        } else if next != self.write_cursor {
            return Err(format!(
                "account write stream advanced from {} to {} without records in slot {}",
                self.write_cursor,
                next,
                self.bank.slot()
            ));
        }
        if let Some(last) = raw_writes.last() {
            if last.write_version + 1 != next {
                return Err(format!(
                    "account write stream ended at {} but next version is {}",
                    last.write_version, next
                ));
            }
        }
        self.write_cursor = next;
        Ok(raw_writes
            .into_iter()
            .map(|write| normalize_account_write(write, signature.clone()))
            .collect())
    }

    #[cfg(test)]
    fn from_test_bank(bank: Bank, stable_cluster: bool, state_dir: TempDir) -> Self {
        let cursor = bank.accounts().accounts_db.next_write_version();
        let last_entry_hash = bank.last_blockhash();
        Self {
            bank: Arc::new(bank),
            stable_cluster,
            _source: InitializedSource::Genesis,
            _state_dir: state_dir,
            write_cursor: cursor,
            next_entry_index: 0,
            last_entry_hash,
            tick_hash_count: 0,
            leader_schedules: HashMap::new(),
            poh_pool: build_poh_pool().unwrap(),
            enforce_candidate_range: false,
        }
    }
}

fn validate_candidate_snapshot_slot(slot: u64) -> Result<(), String> {
    if slot < MIN_SUPPORTED_SNAPSHOT_SLOT || slot >= MAX_SUPPORTED_SLOT_EXCLUSIVE {
        return Err(format!(
            "Solana v1.0.17 epoch-11 candidate snapshot slot {} is outside {}..{}",
            slot, MIN_SUPPORTED_SNAPSHOT_SLOT, MAX_SUPPORTED_SLOT_EXCLUSIVE
        ));
    }
    Ok(())
}

fn validate_candidate_entry_slot(slot: u64) -> Result<(), String> {
    if slot < MIN_SUPPORTED_ENTRY_SLOT || slot >= MAX_SUPPORTED_SLOT_EXCLUSIVE {
        return Err(format!(
            "Solana v1.0.17 epoch-11 candidate entry slot {} is outside {}..{}",
            slot, MIN_SUPPORTED_ENTRY_SLOT, MAX_SUPPORTED_SLOT_EXCLUSIVE
        ));
    }
    Ok(())
}

fn build_poh_pool() -> Result<ThreadPool, String> {
    let thread_count = configured_poh_thread_count()?;
    ThreadPoolBuilder::new()
        .num_threads(thread_count)
        .thread_name(|index| format!("historical-poh-{}", index))
        .build()
        .map_err(|error| format!("failed to create historical PoH thread pool: {}", error))
}

fn configured_poh_thread_count() -> Result<usize, String> {
    let logical_cpus = cmp::max(1, num_cpus::get());
    let maximum = cmp::min(logical_cpus, ABSOLUTE_MAX_POH_THREADS);
    match env::var_os(POH_THREADS_ENV) {
        Some(value) => {
            let value = value.into_string().map_err(|_| {
                format!(
                    "{} must contain a positive decimal integer",
                    POH_THREADS_ENV
                )
            })?;
            parse_poh_thread_count(&value, maximum)
        }
        // This old workspace configures unusually large test-thread stacks.
        // A single pool thread keeps unit tests bounded; production retains
        // Solana's conservative half-of-logical-CPU default.
        None if cfg!(test) => Ok(1),
        None => Ok(cmp::max(1, cmp::min(get_thread_count(), maximum))),
    }
}

fn parse_poh_thread_count(value: &str, maximum: usize) -> Result<usize, String> {
    let configured = value.parse::<usize>().map_err(|_| {
        format!(
            "{} value {:?} is not a positive decimal integer",
            POH_THREADS_ENV, value
        )
    })?;
    if configured == 0 || configured > maximum {
        return Err(format!(
            "{} value {} is outside the supported range 1..={} (logical CPUs capped at {})",
            POH_THREADS_ENV, configured, maximum, ABSOLUTE_MAX_POH_THREADS
        ));
    }
    Ok(configured)
}

struct PreparedEntry {
    request: EntryRequest,
    hash: Hash,
    transactions: Vec<Transaction>,
    signatures_by_writable_key: HashMap<Vec<u8>, Option<Vec<u8>>>,
}

fn prepare_entry(request: EntryRequest) -> Result<PreparedEntry, String> {
    if request.hash.len() != 32 {
        return Err(format!(
            "entry hash has {} bytes, expected 32",
            request.hash.len()
        ));
    }
    let hash = Hash::new(&request.hash);
    let transactions: Vec<Transaction> = request
        .transactions
        .iter()
        .enumerate()
        .map(|(index, wire)| {
            bincode::deserialize(wire)
                .map_err(|error| format!("failed to decode transaction {}: {}", index, error))
        })
        .collect::<Result<_, _>>()?;
    for (index, transaction) in transactions.iter().enumerate() {
        validate_transaction_structure(transaction)
            .map_err(|message| format!("failed to decode transaction {}: {}", index, message))?;
    }
    let signatures_by_writable_key = writable_key_attribution(&transactions)?;
    Ok(PreparedEntry {
        request,
        hash,
        transactions,
        signatures_by_writable_key,
    })
}

fn validate_transaction_structure(transaction: &Transaction) -> Result<(), String> {
    let header = &transaction.message.header;
    let required = header.num_required_signatures as usize;
    let readonly_signed = header.num_readonly_signed_accounts as usize;
    let key_count = transaction.message.account_keys.len();
    if required > key_count {
        return Err(format!(
            "required signature count {} exceeds account-key count {}",
            required, key_count
        ));
    }
    if transaction.signatures.len() != required {
        return Err(format!(
            "signature count {} does not match required count {}",
            transaction.signatures.len(),
            required
        ));
    }
    if readonly_signed > required {
        return Err(format!(
            "readonly signed count {} exceeds signed-key count {}",
            readonly_signed, required
        ));
    }
    let unsigned = key_count - required;
    let readonly_unsigned = header.num_readonly_unsigned_accounts as usize;
    if readonly_unsigned > unsigned {
        return Err(format!(
            "readonly unsigned count {} exceeds unsigned-key count {}",
            readonly_unsigned, unsigned
        ));
    }
    for (instruction_index, instruction) in transaction.message.instructions.iter().enumerate() {
        if instruction.program_id_index as usize >= key_count {
            return Err(format!(
                "instruction {} program index {} exceeds account-key count {}",
                instruction_index, instruction.program_id_index, key_count
            ));
        }
        for account_index in &instruction.accounts {
            if *account_index as usize >= key_count {
                return Err(format!(
                    "instruction {} account index {} exceeds account-key count {}",
                    instruction_index, account_index, key_count
                ));
            }
        }
    }
    Ok(())
}

struct EntryValidation {
    hash: Hash,
    next_tick_hash_count: u64,
}

/// Exact equivalent of v1.0.17 ledger::entry::next_hash. Pulling in
/// the full ledger crate would also pull RocksDB into the isolated worker.
/// The fixed-width backend bypasses generic digest buffering and dispatches
/// to SHA-NI at runtime while retaining a portable software fallback.
fn next_entry_hash(start_hash: &Hash, num_hashes: u64, transactions: &[Transaction]) -> Hash {
    let mut start = [0u8; 32];
    start.copy_from_slice(start_hash.as_ref());
    let transaction_mixin = transaction_mixin(transactions);
    Hash::new(&poh_backend::next_hash(
        start,
        num_hashes,
        transaction_mixin,
    ))
}

fn next_entry_hash_pair(
    start_hashes: [&Hash; 2],
    num_hashes: [u64; 2],
    transactions: [&[Transaction]; 2],
) -> [Hash; 2] {
    let mut starts = [[0u8; 32]; 2];
    starts[0].copy_from_slice(start_hashes[0].as_ref());
    starts[1].copy_from_slice(start_hashes[1].as_ref());
    let hashes = poh_backend::next_hash_pair(
        starts,
        num_hashes,
        [
            transaction_mixin(transactions[0]),
            transaction_mixin(transactions[1]),
        ],
    );
    [Hash::new(&hashes[0]), Hash::new(&hashes[1])]
}

fn transaction_mixin(transactions: &[Transaction]) -> Option<poh_backend::PohHash> {
    if transactions.is_empty() {
        return None;
    }
    let transaction_hash = hash_transactions(transactions);
    let mut mixin = [0u8; 32];
    mixin.copy_from_slice(transaction_hash.as_ref());
    Some(mixin)
}

fn hash_transactions(transactions: &[Transaction]) -> Hash {
    let signatures: Vec<_> = transactions
        .iter()
        .flat_map(|transaction| transaction.signatures.iter())
        .collect();
    MerkleTree::new(&signatures)
        .get_root()
        .cloned()
        .unwrap_or_default()
}

#[derive(Clone, Copy)]
enum MainnetNativeProgram {
    Config = 0,
    Stake = 1,
    System = 2,
    Vote = 3,
}

fn mainnet_native_program(name: &str, program_id: &Pubkey) -> Option<MainnetNativeProgram> {
    if name == "solana_config_program" && *program_id == solana_config_program::id() {
        Some(MainnetNativeProgram::Config)
    } else if name == "solana_stake_program" && *program_id == solana_stake_program::id() {
        Some(MainnetNativeProgram::Stake)
    } else if name == "solana_system_program" && *program_id == system_program::id() {
        Some(MainnetNativeProgram::System)
    } else if name == "solana_vote_program" && *program_id == solana_vote_program::id() {
        Some(MainnetNativeProgram::Vote)
    } else {
        None
    }
}

fn validate_mainnet_genesis_programs(genesis: &GenesisConfig) -> Result<(), String> {
    if genesis.operating_mode != OperatingMode::Stable {
        return Err(format!(
            "mainnet historical runtime requires Stable operating mode, got {:?}",
            genesis.operating_mode
        ));
    }

    let mut seen = [false; 4];
    for (name, program_id) in &genesis.native_instruction_processors {
        let program = mainnet_native_program(name, program_id).ok_or_else(|| {
            format!(
                "unsupported mainnet native program {} ({:?})",
                program_id, name
            )
        })?;
        let index = program as usize;
        if seen[index] {
            return Err(format!(
                "duplicate mainnet native program {} ({:?})",
                program_id, name
            ));
        }
        seen[index] = true;
    }

    let expected = [
        "solana_config_program",
        "solana_stake_program",
        "solana_system_program",
        "solana_vote_program",
    ];
    let missing: Vec<_> = expected
        .iter()
        .zip(seen.iter())
        .filter_map(|(name, present)| if *present { None } else { Some(*name) })
        .collect();
    if !missing.is_empty() {
        return Err(format!(
            "mainnet genesis is missing native program(s): {}",
            missing.join(", ")
        ));
    }
    Ok(())
}

/// Snapshot serde intentionally drops the in-memory native dispatch table.
/// Restore it with the exact v1.0.17 processors linked into this worker, so
/// replay never depends on deployment-adjacent, dynamically loaded `.so`s.
fn restore_mainnet_runtime_hooks(bank: &mut Bank, genesis: &GenesisConfig) -> Result<(), String> {
    validate_mainnet_genesis_programs(genesis)?;
    restore_mainnet_native_processors(bank);

    // The upstream Stable callback has no state effects in epoch 11. Install
    // the callback explicitly and guard the candidate's upper epoch; entry
    // validation rejects the first later slot before constructing its bank.
    bank.set_entered_epoch_callback(Box::new(|bank| {
        assert!(
            bank.epoch() <= MAX_SUPPORTED_EPOCH,
            "Solana v1.0.17 candidate entered unsupported epoch {}",
            bank.epoch()
        );
    }));
    Ok(())
}

fn restore_mainnet_native_processors(bank: &mut Bank) {
    // Bank::new_from_parent intentionally starts with MessageProcessor::default,
    // so these bindings must be restored on every child, not just the bank
    // deserialized from the snapshot.
    bank.add_instruction_processor(
        solana_config_program::id(),
        config_processor::process_instruction,
    );
    bank.add_instruction_processor(
        solana_stake_program::id(),
        stake_instruction::process_instruction,
    );
    bank.add_instruction_processor(
        solana_vote_program::id(),
        solana_vote_program::vote_instruction::process_instruction,
    );
}

fn processing_max_age(stable_cluster: bool, epoch: u64) -> usize {
    if stable_cluster && epoch >= MAX_AGE_CORRECTION_EPOCH {
        MAX_PROCESSING_AGE
    } else {
        MAX_RECENT_BLOCKHASHES
    }
}

fn writable_key_attribution(
    transactions: &[Transaction],
) -> Result<HashMap<Vec<u8>, Option<Vec<u8>>>, String> {
    let mut by_key = HashMap::new();
    for transaction in transactions {
        let signature = transaction
            .signatures
            .get(0)
            .map(|signature| signature.as_ref().to_vec());
        for (index, pubkey) in transaction.message.account_keys.iter().enumerate() {
            if transaction.message.is_writable(index) {
                let key = pubkey.as_ref().to_vec();
                if by_key.insert(key.clone(), signature.clone()).is_some() {
                    return Err(format!(
                        "multiple transactions in one entry write account {}",
                        pubkey
                    ));
                }
            }
        }
    }
    Ok(by_key)
}

fn normalize_account_write(write: OwnedAccountWrite, signature: Option<Vec<u8>>) -> AccountWrite {
    AccountWrite {
        slot: write.slot,
        write_version: write.write_version,
        transaction_signature: signature,
        pubkey: write.pubkey.as_ref().to_vec(),
        lamports: write.account.lamports,
        owner: write.account.owner.as_ref().to_vec(),
        executable: write.account.executable,
        rent_epoch: write.account.rent_epoch,
        data: write.account.data,
        stored_hash: write.stored_hash.as_ref().to_vec(),
    }
}

fn normalize_transaction_error(error: TransactionError) -> WireTransactionError {
    match error {
        TransactionError::AccountInUse => WireTransactionError::AccountInUse,
        TransactionError::AccountLoadedTwice => WireTransactionError::AccountLoadedTwice,
        TransactionError::AccountNotFound => WireTransactionError::AccountNotFound,
        TransactionError::ProgramAccountNotFound => WireTransactionError::ProgramAccountNotFound,
        TransactionError::InsufficientFundsForFee => WireTransactionError::InsufficientFundsForFee,
        TransactionError::InvalidAccountForFee => WireTransactionError::InvalidAccountForFee,
        TransactionError::DuplicateSignature => WireTransactionError::DuplicateSignature,
        TransactionError::BlockhashNotFound => WireTransactionError::BlockhashNotFound,
        TransactionError::InstructionError(index, error) => {
            WireTransactionError::InstructionError {
                instruction_index: index,
                error: normalize_instruction_error(error),
            }
        }
        TransactionError::CallChainTooDeep => WireTransactionError::CallChainTooDeep,
        TransactionError::MissingSignatureForFee => WireTransactionError::MissingSignatureForFee,
        TransactionError::InvalidAccountIndex => WireTransactionError::InvalidAccountIndex,
        TransactionError::SignatureFailure => WireTransactionError::SignatureFailure,
    }
}

fn normalize_instruction_error(error: InstructionError) -> WireInstructionError {
    match error {
        InstructionError::GenericError => WireInstructionError::GenericError,
        InstructionError::InvalidArgument => WireInstructionError::InvalidArgument,
        InstructionError::InvalidInstructionData => WireInstructionError::InvalidInstructionData,
        InstructionError::InvalidAccountData => WireInstructionError::InvalidAccountData,
        InstructionError::AccountDataTooSmall => WireInstructionError::AccountDataTooSmall,
        InstructionError::InsufficientFunds => WireInstructionError::InsufficientFunds,
        InstructionError::IncorrectProgramId => WireInstructionError::IncorrectProgramId,
        InstructionError::MissingRequiredSignature => {
            WireInstructionError::MissingRequiredSignature
        }
        InstructionError::AccountAlreadyInitialized => {
            WireInstructionError::AccountAlreadyInitialized
        }
        InstructionError::UninitializedAccount => WireInstructionError::UninitializedAccount,
        InstructionError::UnbalancedInstruction => WireInstructionError::UnbalancedInstruction,
        InstructionError::ModifiedProgramId => WireInstructionError::ModifiedProgramId,
        InstructionError::ExternalAccountLamportSpend => {
            WireInstructionError::ExternalAccountLamportSpend
        }
        InstructionError::ExternalAccountDataModified => {
            WireInstructionError::ExternalAccountDataModified
        }
        InstructionError::ReadonlyLamportChange => WireInstructionError::ReadonlyLamportChange,
        InstructionError::ReadonlyDataModified => WireInstructionError::ReadonlyDataModified,
        InstructionError::DuplicateAccountIndex => WireInstructionError::DuplicateAccountIndex,
        InstructionError::ExecutableModified => WireInstructionError::ExecutableModified,
        InstructionError::RentEpochModified => WireInstructionError::RentEpochModified,
        InstructionError::NotEnoughAccountKeys => WireInstructionError::NotEnoughAccountKeys,
        InstructionError::AccountDataSizeChanged => WireInstructionError::AccountDataSizeChanged,
        InstructionError::AccountNotExecutable => WireInstructionError::AccountNotExecutable,
        InstructionError::AccountBorrowFailed => WireInstructionError::AccountBorrowFailed,
        InstructionError::AccountBorrowOutstanding => {
            WireInstructionError::AccountBorrowOutstanding
        }
        InstructionError::DuplicateAccountOutOfSync => {
            WireInstructionError::DuplicateAccountOutOfSync
        }
        InstructionError::CustomError(code) => WireInstructionError::Custom(code),
        InstructionError::InvalidError => WireInstructionError::InvalidError,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_runtime::genesis_utils::create_genesis_config_with_leader;
    use solana_sdk::{
        account::Account,
        hash::hashv,
        instruction::{AccountMeta, Instruction},
        signature::{Keypair, Signer},
        system_instruction, system_program, system_transaction, sysvar,
    };
    use solana_vote_program::vote_state::{VoteInit, VoteState};

    fn set_exact_mainnet_native_programs(genesis: &mut GenesisConfig) {
        genesis.operating_mode = OperatingMode::Stable;
        genesis.native_instruction_processors = vec![
            (
                "solana_config_program".to_string(),
                solana_config_program::id(),
            ),
            (
                "solana_stake_program".to_string(),
                solana_stake_program::id(),
            ),
            ("solana_system_program".to_string(), system_program::id()),
            ("solana_vote_program".to_string(), solana_vote_program::id()),
        ];
    }

    fn test_state(ticks_per_slot: u64, hashes_per_tick: Option<u64>) -> (RuntimeState, Keypair) {
        let leader = Pubkey::new_from_array([7; 32]);
        let mut genesis = create_genesis_config_with_leader(1_000_000, &leader, 500_000);
        genesis.genesis_config.ticks_per_slot = ticks_per_slot;
        genesis.genesis_config.poh_config.hashes_per_tick = hashes_per_tick;
        let state_dir = snapshot::private_state_dir(None).unwrap();
        let account_paths = snapshot::private_account_paths(&state_dir).unwrap();
        let bank = Bank::new_with_paths(&genesis.genesis_config, account_paths, &[]);
        (
            RuntimeState::from_test_bank(bank, false, state_dir),
            genesis.mint_keypair,
        )
    }

    #[test]
    fn poh_thread_override_is_positive_and_bounded() {
        assert_eq!(parse_poh_thread_count("1", 64).unwrap(), 1);
        assert_eq!(parse_poh_thread_count("64", 64).unwrap(), 64);
        assert!(parse_poh_thread_count("0", 64).is_err());
        assert!(parse_poh_thread_count("65", 64).is_err());
        assert!(parse_poh_thread_count("not-a-number", 64).is_err());
    }

    #[test]
    fn epoch_11_candidate_range_is_closed_at_both_ends() {
        assert!(validate_candidate_snapshot_slot(MIN_SUPPORTED_SNAPSHOT_SLOT).is_ok());
        assert!(validate_candidate_snapshot_slot(MIN_SUPPORTED_SNAPSHOT_SLOT - 1).is_err());
        assert!(validate_candidate_snapshot_slot(MAX_SUPPORTED_SLOT_EXCLUSIVE - 1).is_ok());
        assert!(validate_candidate_snapshot_slot(MAX_SUPPORTED_SLOT_EXCLUSIVE).is_err());

        assert!(validate_candidate_entry_slot(MIN_SUPPORTED_ENTRY_SLOT).is_ok());
        assert!(validate_candidate_entry_slot(MIN_SUPPORTED_ENTRY_SLOT - 1).is_err());
        assert!(validate_candidate_entry_slot(MAX_SUPPORTED_SLOT_EXCLUSIVE - 1).is_ok());
        assert!(validate_candidate_entry_slot(MAX_SUPPORTED_SLOT_EXCLUSIVE).is_err());
    }

    fn request_for(
        state: &RuntimeState,
        slot: u64,
        entry_index: u64,
        num_hashes: u64,
        transactions: &[Transaction],
    ) -> EntryRequest {
        let start_hash = if slot > state.bank.slot() {
            state.bank.last_blockhash()
        } else {
            state.last_entry_hash
        };
        let entry_hash = next_entry_hash(&start_hash, num_hashes, transactions);
        EntryRequest {
            slot,
            entry_index,
            num_hashes,
            hash: entry_hash.as_ref().to_vec(),
            transactions: transactions
                .iter()
                .map(|transaction| bincode::serialize(transaction).unwrap())
                .collect(),
        }
    }

    fn old_form_vote_initialize_transaction(
        payer: &Keypair,
        vote_account: &Keypair,
        recent_blockhash: Hash,
        lamports: u64,
    ) -> Transaction {
        let vote_init = VoteInit {
            node_pubkey: payer.pubkey(),
            authorized_voter: vote_account.pubkey(),
            authorized_withdrawer: vote_account.pubkey(),
            commission: 0,
        };
        let instructions = vec![
            system_instruction::create_account(
                &payer.pubkey(),
                &vote_account.pubkey(),
                lamports,
                VoteState::size_of() as u64,
                &solana_vote_program::id(),
            ),
            // This is the pre-v1.0.17 wire form observed on mainnet at slot
            // 521850. The node identity signs the transaction as fee payer,
            // but is deliberately absent from this instruction's account
            // list. v1.0.7 accepted that form; v1.0.17 must reject it.
            Instruction::new(
                solana_vote_program::id(),
                &VoteInstruction::InitializeAccount(vote_init),
                vec![
                    AccountMeta::new(vote_account.pubkey(), false),
                    AccountMeta::new_readonly(sysvar::rent::id(), false),
                    AccountMeta::new_readonly(sysvar::clock::id(), false),
                ],
            ),
        ];
        Transaction::new_signed_with_payer(
            instructions,
            Some(&payer.pubkey()),
            &[payer, vote_account],
            recent_blockhash,
        )
    }

    #[test]
    fn exact_mainnet_native_program_set_is_required() {
        let mut genesis = GenesisConfig::default();
        set_exact_mainnet_native_programs(&mut genesis);
        assert!(validate_mainnet_genesis_programs(&genesis).is_ok());

        genesis.native_instruction_processors.pop();
        assert!(validate_mainnet_genesis_programs(&genesis)
            .unwrap_err()
            .contains("missing native program"));

        set_exact_mainnet_native_programs(&mut genesis);
        genesis.native_instruction_processors.push((
            "unknown_native_program".to_string(),
            Pubkey::new_from_array([91; 32]),
        ));
        assert!(validate_mainnet_genesis_programs(&genesis)
            .unwrap_err()
            .contains("unsupported mainnet native program"));

        set_exact_mainnet_native_programs(&mut genesis);
        genesis
            .native_instruction_processors
            .push(genesis.native_instruction_processors[0].clone());
        assert!(validate_mainnet_genesis_programs(&genesis)
            .unwrap_err()
            .contains("duplicate mainnet native program"));

        set_exact_mainnet_native_programs(&mut genesis);
        genesis.operating_mode = OperatingMode::Development;
        assert!(validate_mainnet_genesis_programs(&genesis)
            .unwrap_err()
            .contains("requires Stable operating mode"));
    }

    #[test]
    fn v1_0_17_rejects_old_form_vote_initialization_without_node_instruction_account() {
        let leader = Pubkey::new_from_array([7; 32]);
        let mut genesis = create_genesis_config_with_leader(1_000_000, &leader, 500_000);
        set_exact_mainnet_native_programs(&mut genesis.genesis_config);
        let state_dir = snapshot::private_state_dir(None).unwrap();
        let account_paths = snapshot::private_account_paths(&state_dir).unwrap();
        let mut bank = Bank::new_with_paths(&genesis.genesis_config, account_paths, &[]);
        restore_mainnet_runtime_hooks(&mut bank, &genesis.genesis_config).unwrap();
        let mut state = RuntimeState::from_test_bank(bank, true, state_dir);
        let vote_account = Keypair::new();
        let rent_exempt_lamports = state
            .bank
            .get_minimum_balance_for_rent_exemption(VoteState::size_of())
            .max(1);
        let transaction = old_form_vote_initialize_transaction(
            &genesis.mint_keypair,
            &vote_account,
            state.bank.last_blockhash(),
            rent_exempt_lamports,
        );

        let processed = state
            .process_entry(request_for(&state, 0, 0, 1, &[transaction]))
            .unwrap();
        assert_eq!(
            processed.outcomes[0].error,
            Some(WireTransactionError::InstructionError {
                instruction_index: 1,
                error: WireInstructionError::MissingRequiredSignature,
            })
        );
        assert!(state.bank.get_account(&vote_account.pubkey()).is_none());
    }

    #[test]
    fn child_bank_restores_static_mainnet_native_dispatch() {
        let leader = Pubkey::new_from_array([7; 32]);
        let mut genesis = create_genesis_config_with_leader(1_000_000, &leader, 500_000);
        set_exact_mainnet_native_programs(&mut genesis.genesis_config);
        let state_dir = snapshot::private_state_dir(None).unwrap();
        let account_paths = snapshot::private_account_paths(&state_dir).unwrap();
        let mut bank0 = Bank::new_with_paths(&genesis.genesis_config, account_paths, &[]);
        restore_mainnet_runtime_hooks(&mut bank0, &genesis.genesis_config).unwrap();
        let tick_hash = bank0.last_blockhash();
        for _ in 0..bank0.ticks_per_slot() {
            bank0.register_tick(&tick_hash);
        }
        let mut state = RuntimeState::from_test_bank(bank0, true, state_dir);

        let cases = [
            (
                solana_config_program::id(),
                WireInstructionError::InvalidInstructionData,
            ),
            (
                solana_stake_program::id(),
                WireInstructionError::NotEnoughAccountKeys,
            ),
            (
                solana_vote_program::id(),
                WireInstructionError::NotEnoughAccountKeys,
            ),
        ];
        for (entry_index, (program_id, expected_error)) in cases.iter().enumerate() {
            let transaction = Transaction::new_signed_with_payer(
                vec![Instruction {
                    program_id: *program_id,
                    accounts: Vec::new(),
                    data: Vec::new(),
                }],
                Some(&genesis.mint_keypair.pubkey()),
                &[&genesis.mint_keypair],
                state.bank.last_blockhash(),
            );
            let entry_hash = next_entry_hash(&state.last_entry_hash, 1, &[transaction.clone()]);
            let processed = state
                .process_entry(EntryRequest {
                    slot: 1,
                    entry_index: entry_index as u64,
                    num_hashes: 1,
                    hash: entry_hash.as_ref().to_vec(),
                    transactions: vec![bincode::serialize(&transaction).unwrap()],
                })
                .unwrap();
            assert_eq!(
                processed.outcomes[0].error,
                Some(WireTransactionError::InstructionError {
                    instruction_index: 0,
                    error: expected_error.clone(),
                })
            );
        }
    }

    #[test]
    fn entry_batch_emits_attributed_ordered_writes_and_checkpoint() {
        let leader = Pubkey::new_from_array([7; 32]);
        let mut genesis = create_genesis_config_with_leader(1_000_000, &leader, 500_000);
        let second_payer = Keypair::new();
        genesis.genesis_config.accounts.insert(
            second_payer.pubkey(),
            Account::new(10_000, 0, &system_program::id()),
        );
        let state_dir = snapshot::private_state_dir(None).unwrap();
        let account_paths = snapshot::private_account_paths(&state_dir).unwrap();
        let bank0 = Bank::new_with_paths(&genesis.genesis_config, account_paths, &[]);
        let mut tick_hash = bank0.last_blockhash();
        for tick in 0..bank0.ticks_per_slot() {
            tick_hash = hashv(&[tick_hash.as_ref(), &tick.to_le_bytes()]);
            bank0.register_tick(&tick_hash);
        }
        let mut state = RuntimeState::from_test_bank(bank0, false, state_dir);
        let recipient = Keypair::new();
        let transaction = system_transaction::transfer(
            &genesis.mint_keypair,
            &recipient.pubkey(),
            123,
            state.bank.last_blockhash(),
        );
        let second_recipient = Keypair::new();
        let second_transaction = system_transaction::transfer(
            &second_payer,
            &second_recipient.pubkey(),
            321,
            state.bank.last_blockhash(),
        );
        let signature = transaction.signatures[0].as_ref().to_vec();
        let second_signature = second_transaction.signatures[0].as_ref().to_vec();
        let entry_transactions = vec![transaction.clone(), second_transaction.clone()];
        let entry_hash = next_entry_hash(&state.last_entry_hash, 1, &entry_transactions);
        let processed = state
            .process_entry(EntryRequest {
                slot: 1,
                entry_index: 0,
                num_hashes: 1,
                hash: entry_hash.as_ref().to_vec(),
                transactions: vec![
                    bincode::serialize(&transaction).unwrap(),
                    bincode::serialize(&second_transaction).unwrap(),
                ],
            })
            .unwrap();
        assert_eq!(processed.outcomes.len(), 2);
        assert_eq!(processed.outcomes[0].signature.as_ref(), Some(&signature));
        assert_eq!(
            processed.outcomes[1].signature.as_ref(),
            Some(&second_signature)
        );
        assert_eq!(processed.outcomes[0].error, None);
        assert_eq!(processed.outcomes[1].error, None);
        assert!(processed.writes.iter().any(|write| {
            write.pubkey == recipient.pubkey().as_ref()
                && write.transaction_signature.as_ref() == Some(&signature)
        }));
        assert!(processed.writes.iter().any(|write| {
            write.pubkey == second_recipient.pubkey().as_ref()
                && write.transaction_signature.as_ref() == Some(&second_signature)
        }));
        let mut last_version = None;
        for write in &processed.writes {
            if let Some(previous) = last_version {
                assert!(write.write_version > previous);
            }
            last_version = Some(write.write_version);
        }

        for index in 1..=state.bank.ticks_per_slot() {
            let entry_hash = next_entry_hash(&state.last_entry_hash, 1, &[]);
            state
                .process_entry(EntryRequest {
                    slot: 1,
                    entry_index: index,
                    num_hashes: 1,
                    hash: entry_hash.as_ref().to_vec(),
                    transactions: Vec::new(),
                })
                .unwrap();
        }
        let checkpoint = state.freeze_checkpoint(1).unwrap();
        assert!(checkpoint.slot_complete);
        assert_eq!(checkpoint.bank_hash.len(), 32);
        assert_eq!(checkpoint.accounts_hash.len(), 32);
        assert!(checkpoint
            .writes
            .iter()
            .all(|write| write.transaction_signature.is_none()));
    }

    #[test]
    fn poh_and_tick_invariants_fail_before_mutating_the_bank() {
        let (mut state, _mint_keypair) = test_state(2, Some(4));
        let initial_hash = state.last_entry_hash;

        let mut bad_hash = request_for(&state, 0, 0, 4, &[]);
        bad_hash.hash[0] ^= 1;
        assert!(state
            .process_entry(bad_hash)
            .unwrap_err()
            .contains("entry PoH hash mismatch"));
        assert_eq!(state.bank.tick_height(), 0);
        assert_eq!(state.next_entry_index, 0);
        assert_eq!(state.last_entry_hash, initial_hash);

        let wrong_tick_count = request_for(&state, 0, 0, 3, &[]);
        assert!(state
            .process_entry(wrong_tick_count)
            .unwrap_err()
            .contains("accumulated PoH hashes"));
        assert_eq!(state.bank.tick_height(), 0);

        let first_tick = request_for(&state, 0, 0, 4, &[]);
        state.process_entry(first_tick).unwrap();
        let second_tick = request_for(&state, 0, 1, 4, &[]);
        let completed = state.process_entry(second_tick).unwrap();
        assert!(completed.slot_complete);

        let trailing = request_for(&state, 0, 2, 1, &[]);
        assert!(state
            .process_entry(trailing)
            .unwrap_err()
            .contains("follows the completion tick"));
        assert_eq!(state.bank.tick_height(), state.bank.max_tick_height());

        let mut invalid_next_slot = request_for(&state, 1, 0, 4, &[]);
        invalid_next_slot.hash[0] ^= 1;
        assert!(state
            .process_entry(invalid_next_slot)
            .unwrap_err()
            .contains("entry PoH hash mismatch"));
        assert_eq!(state.bank.slot(), 0);
        assert!(!state.bank.is_frozen());

        let valid_next_slot = request_for(&state, 1, 0, 4, &[]);
        assert_eq!(state.process_entry(valid_next_slot).unwrap().slot, 1);
    }

    #[test]
    fn late_batch_validation_failure_leaves_bank_entirely_unmodified() {
        let (mut state, _mint_keypair) = test_state(2, Some(4));
        let initial_hash = state.last_entry_hash;
        let initial_write_cursor = state.write_cursor;
        let first_hash = next_entry_hash(&initial_hash, 4, &[]);
        let second_hash = next_entry_hash(&first_hash, 4, &[]);
        let first = EntryRequest {
            slot: 0,
            entry_index: 0,
            num_hashes: 4,
            hash: first_hash.as_ref().to_vec(),
            transactions: Vec::new(),
        };
        let mut bad_poh = EntryRequest {
            slot: 0,
            entry_index: 1,
            num_hashes: 4,
            hash: second_hash.as_ref().to_vec(),
            transactions: Vec::new(),
        };
        bad_poh.hash[0] ^= 1;

        assert!(state
            .process_entries(vec![first.clone(), bad_poh])
            .unwrap_err()
            .contains("entry PoH hash mismatch"));
        assert_eq!(state.bank.tick_height(), 0);
        assert_eq!(state.next_entry_index, 0);
        assert_eq!(state.last_entry_hash, initial_hash);
        assert_eq!(state.tick_hash_count, 0);
        assert_eq!(state.write_cursor, initial_write_cursor);

        let bad_order = EntryRequest {
            entry_index: 2,
            hash: second_hash.as_ref().to_vec(),
            ..first.clone()
        };
        assert!(state
            .process_entries(vec![first.clone(), bad_order])
            .unwrap_err()
            .contains("does not match expected 1"));
        assert_eq!(state.bank.tick_height(), 0);
        assert_eq!(state.next_entry_index, 0);
        assert_eq!(state.last_entry_hash, initial_hash);

        let undecodable = EntryRequest {
            entry_index: 1,
            num_hashes: 1,
            hash: vec![0; 32],
            transactions: vec![vec![0xff]],
            ..first.clone()
        };
        assert!(state
            .process_entries(vec![first, undecodable])
            .unwrap_err()
            .starts_with("failed to decode transaction"));
        assert_eq!(state.bank.tick_height(), 0);
        assert_eq!(state.next_entry_index, 0);
        assert_eq!(state.last_entry_hash, initial_hash);
    }

    #[test]
    fn malformed_legacy_transaction_structure_is_rejected_before_bank_mutation() {
        let (mut state, mint_keypair) = test_state(2, Some(4));
        let recipient = Pubkey::new_from_array([9; 32]);
        let base =
            system_transaction::transfer(&mint_keypair, &recipient, 1, state.bank.last_blockhash());
        let initial_hash = state.last_entry_hash;

        let mut bad_signatures = base.clone();
        bad_signatures.signatures.clear();
        let error = state
            .process_entry(request_for(&state, 0, 0, 1, &[bad_signatures]))
            .unwrap_err();
        assert!(error.contains("signature count"));

        let mut bad_readonly_signed = base.clone();
        bad_readonly_signed
            .message
            .header
            .num_readonly_signed_accounts = bad_readonly_signed
            .message
            .header
            .num_required_signatures
            .saturating_add(1);
        let error = state
            .process_entry(request_for(&state, 0, 0, 1, &[bad_readonly_signed]))
            .unwrap_err();
        assert!(error.contains("readonly signed count"));

        let mut bad_program_index = base.clone();
        bad_program_index.message.instructions[0].program_id_index = u8::max_value();
        let error = state
            .process_entry(request_for(&state, 0, 0, 1, &[bad_program_index]))
            .unwrap_err();
        assert!(error.contains("program index"));

        let mut bad_account_index = base;
        bad_account_index.message.instructions[0].accounts[0] = u8::max_value();
        let error = state
            .process_entry(request_for(&state, 0, 0, 1, &[bad_account_index]))
            .unwrap_err();
        assert!(error.contains("account index"));

        assert_eq!(state.bank.tick_height(), 0);
        assert_eq!(state.next_entry_index, 0);
        assert_eq!(state.last_entry_hash, initial_hash);
    }

    #[test]
    fn valid_batch_commits_and_reports_entries_in_wire_order() {
        let (mut state, _mint_keypair) = test_state(2, Some(4));
        let first_hash = next_entry_hash(&state.last_entry_hash, 4, &[]);
        let second_hash = next_entry_hash(&first_hash, 4, &[]);
        let processed = state
            .process_entries(vec![
                EntryRequest {
                    slot: 0,
                    entry_index: 0,
                    num_hashes: 4,
                    hash: first_hash.as_ref().to_vec(),
                    transactions: Vec::new(),
                },
                EntryRequest {
                    slot: 0,
                    entry_index: 1,
                    num_hashes: 4,
                    hash: second_hash.as_ref().to_vec(),
                    transactions: Vec::new(),
                },
            ])
            .unwrap();
        assert_eq!(processed.len(), 2);
        assert_eq!(processed[0].entry_index, 0);
        assert!(!processed[0].slot_complete);
        assert_eq!(processed[1].entry_index, 1);
        assert!(processed[1].slot_complete);
        assert_eq!(state.bank.tick_height(), 2);
        assert_eq!(state.next_entry_index, 2);
        assert_eq!(state.last_entry_hash, second_hash);
    }

    #[test]
    fn entry_batch_count_is_bounded_before_preparation() {
        let (mut state, _mint_keypair) = test_state(2, Some(4));
        let request = request_for(&state, 0, 0, 4, &[]);
        let error = state
            .process_entries(vec![request; MAX_ENTRIES_PER_BATCH + 1])
            .unwrap_err();
        assert!(error.contains("limit is"));
        assert_eq!(state.bank.tick_height(), 0);
        assert_eq!(state.next_entry_index, 0);
    }

    #[test]
    fn transaction_hash_count_must_leave_room_for_the_tick() {
        let (mut state, mint_keypair) = test_state(2, Some(4));
        let recipient = Pubkey::new_from_array([9; 32]);
        let transaction =
            system_transaction::transfer(&mint_keypair, &recipient, 1, state.bank.last_blockhash());

        let reaches_tick_boundary = request_for(&state, 0, 0, 4, &[transaction.clone()]);
        assert!(state
            .process_entry(reaches_tick_boundary)
            .unwrap_err()
            .contains("without a tick"));
        assert_eq!(state.next_entry_index, 0);

        let transaction_entry = request_for(&state, 0, 0, 3, &[transaction]);
        state.process_entry(transaction_entry).unwrap();
        assert_eq!(state.tick_hash_count, 3);
        let tick = request_for(&state, 0, 1, 1, &[]);
        state.process_entry(tick).unwrap();
        assert_eq!(state.tick_hash_count, 0);
        assert_eq!(state.bank.tick_height(), 1);
    }

    #[test]
    fn incomplete_checkpoint_does_not_freeze_or_poison_replay() {
        let (mut state, _mint_keypair) = test_state(2, Some(1));
        assert!(state
            .freeze_checkpoint(0)
            .unwrap_err()
            .contains("cannot checkpoint incomplete bank"));
        assert!(!state.bank.is_frozen());

        let first_tick = request_for(&state, 0, 0, 1, &[]);
        state.process_entry(first_tick).unwrap();
        let second_tick = request_for(&state, 0, 1, 1, &[]);
        state.process_entry(second_tick).unwrap();
        assert!(state.freeze_checkpoint(0).unwrap().slot_complete);
    }

    #[test]
    fn completed_slots_are_squashed_without_losing_state_or_writes() {
        let (mut state, mint_keypair) = test_state(1, Some(2));
        let preserved_key = Pubkey::new_from_array([42; 32]);
        let first_version = state.write_cursor;
        let mut expected_write_version = first_version;

        let transfer = system_transaction::transfer(
            &mint_keypair,
            &preserved_key,
            777,
            state.bank.last_blockhash(),
        );
        let transaction_entry = request_for(&state, 0, 0, 1, &[transfer]);
        let transaction_processed = state.process_entry(transaction_entry).unwrap();
        for write in transaction_processed.writes {
            assert_eq!(write.write_version, expected_write_version);
            expected_write_version += 1;
        }
        assert_eq!(
            transaction_processed.next_write_version,
            expected_write_version
        );
        let genesis_tick = request_for(&state, 0, 1, 1, &[]);
        let tick_processed = state.process_entry(genesis_tick).unwrap();
        for write in tick_processed.writes {
            assert_eq!(write.write_version, expected_write_version);
            expected_write_version += 1;
        }
        assert_eq!(tick_processed.next_write_version, expected_write_version);
        assert_eq!(state.bank.get_balance(&preserved_key), 777);

        for slot in 1..=16 {
            let tick = request_for(&state, slot, 0, 2, &[]);
            let processed = state.process_entry(tick).unwrap();
            assert!(processed.slot_complete);
            for write in processed.writes {
                assert_eq!(write.write_version, expected_write_version);
                expected_write_version += 1;
            }
            assert_eq!(processed.next_write_version, expected_write_version);
            assert_eq!(state.bank.get_balance(&preserved_key), 777);
            assert_eq!(state.bank.parents().len(), 1);
            assert!(state.bank.parent().unwrap().parents().is_empty());
        }

        let checkpoint = state.freeze_checkpoint(16).unwrap();
        for write in checkpoint.writes {
            assert_eq!(write.write_version, expected_write_version);
            expected_write_version += 1;
        }
        assert_eq!(checkpoint.next_write_version, expected_write_version);
        assert!(state.bank.parents().is_empty());
        assert_eq!(state.bank.get_balance(&preserved_key), 777);
    }

    #[test]
    fn stable_cluster_age_gate_matches_v1_blockstore_boundary() {
        assert_eq!(processing_max_age(true, 13), MAX_RECENT_BLOCKHASHES);
        assert_eq!(processing_max_age(true, 14), MAX_PROCESSING_AGE);
        assert_eq!(processing_max_age(false, 14), MAX_RECENT_BLOCKHASHES);
    }

    #[test]
    fn parent_legacy_transaction_fixture_decodes_exactly() {
        let mut wire = Vec::with_capacity(134);
        wire.push(1); // short-vec signature count
        wire.extend_from_slice(&[7; 64]);
        wire.extend_from_slice(&[1, 0, 0]); // MessageHeader
        wire.push(1); // short-vec account-key count
        wire.extend_from_slice(&[3; 32]);
        wire.extend_from_slice(&[4; 32]);
        wire.push(0); // short-vec instruction count
        assert_eq!(wire.len(), 134);

        let transaction: Transaction = bincode::deserialize(&wire).unwrap();
        assert_eq!(transaction.signatures.len(), 1);
        assert_eq!(transaction.signatures[0].as_ref().len(), 64);
        assert!(transaction.signatures[0]
            .as_ref()
            .iter()
            .all(|byte| *byte == 7));
        assert_eq!(transaction.message.header.num_required_signatures, 1);
        assert_eq!(transaction.message.header.num_readonly_signed_accounts, 0);
        assert_eq!(transaction.message.header.num_readonly_unsigned_accounts, 0);
        assert_eq!(transaction.message.account_keys.len(), 1);
        assert_eq!(transaction.message.account_keys[0].as_ref(), &[3; 32]);
        assert_eq!(transaction.message.recent_blockhash.as_ref(), &[4; 32]);
        assert!(transaction.message.instructions.is_empty());
        assert_eq!(bincode::serialize(&transaction).unwrap(), wire);
    }

    #[test]
    #[ignore]
    fn initializes_and_checkpoints_epoch_11_boundary_snapshot() {
        let archive = std::env::var("JETSTREAMER_SNAPSHOT_4751796")
            .expect("set JETSTREAMER_SNAPSHOT_4751796");
        let ledger =
            std::env::var("JETSTREAMER_MAINNET_LEDGER").expect("set JETSTREAMER_MAINNET_LEDGER");
        let (mut state, initialized) = RuntimeState::initialize(
            &ledger,
            &jetstreamer_historical_protocol::InitialState::SnapshotArchive {
                archive_path: archive,
            },
            None,
        )
        .unwrap();
        assert_eq!(initialized.slot, 4_751_796);
        let checkpoint = state.freeze_checkpoint(4_751_796).unwrap();
        println!(
            "initialized slot={} last_blockhash={} next_write_version={}",
            initialized.slot,
            Hash::new(&initialized.last_blockhash),
            initialized.next_write_version
        );
        println!(
            "checkpoint slot={} bank_hash={} accounts_hash={} last_blockhash={} capitalization={} transactions={} tick_height={} complete={}",
            checkpoint.slot,
            Hash::new(&checkpoint.bank_hash),
            Hash::new(&checkpoint.accounts_hash),
            Hash::new(&checkpoint.last_blockhash),
            checkpoint.capitalization,
            checkpoint.transaction_count,
            checkpoint.tick_height,
            checkpoint.slot_complete
        );
        assert_eq!(
            Hash::new(&checkpoint.accounts_hash).to_string(),
            "6vJ22rwAfXfr4hFUJ7AtLupR6LHWBWX114AhKJqPYejb"
        );
    }
}
