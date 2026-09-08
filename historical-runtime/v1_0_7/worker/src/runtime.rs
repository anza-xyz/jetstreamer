use crate::{leader_schedule, snapshot};
use jetstreamer_historical_protocol::{
    AccountWrite, Checkpoint, EntryProcessed, EntryRequest, Initialized, InitializedSource,
    InstructionError as WireInstructionError, SnapshotExport,
    TransactionError as WireTransactionError, TransactionOutcome,
};
use solana_config_program::config_processor;
use solana_merkle_tree::MerkleTree;
use solana_runtime::{accounts_db::OwnedAccountWrite, bank::Bank};
use solana_sdk::{
    clock::{MAX_PROCESSING_AGE, MAX_RECENT_BLOCKHASHES},
    genesis_config::{GenesisConfig, OperatingMode},
    hash::{hash, hashv, Hash},
    instruction::InstructionError,
    pubkey::Pubkey,
    system_program,
    sysvar::{slot_hashes, slot_hashes::SlotHashes, Sysvar},
    transaction::{Transaction, TransactionError},
};
use solana_stake_program::stake_instruction;
use solana_vote_program::vote_instruction::VoteInstruction;
use std::{collections::HashMap, env, path::Path, sync::Arc};
use tempfile::TempDir;

const MAX_AGE_CORRECTION_EPOCH: u64 = 14;
const MAX_SUPPORTED_SLOT_EXCLUSIVE: u64 = 4_752_000;
const MAX_SUPPORTED_EPOCH: u64 = 10;

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
    snapshot_export_seal: Option<SnapshotExportSeal>,
}

#[derive(Clone, Copy)]
struct SnapshotExportSeal {
    slot: u64,
    accounts_hash: Hash,
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

        let (mut bank, source) = match initial_state {
            jetstreamer_historical_protocol::InitialState::SnapshotArchive { archive_path } => {
                let loaded = snapshot::load_archive(Path::new(archive_path), &state_dir)?;
                let source = InitializedSource::SnapshotArchive {
                    archive_path: archive_path.clone(),
                    expected_accounts_hash: loaded.expected_accounts_hash.as_ref().to_vec(),
                };
                (loaded.bank, source)
            }
            jetstreamer_historical_protocol::InitialState::Genesis => {
                let account_paths = snapshot::private_account_paths(&state_dir)?;
                (
                    Bank::new_with_paths(&genesis, account_paths),
                    InitializedSource::Genesis,
                )
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
                snapshot_export_seal: None,
            },
            initialized,
        ))
    }

    pub fn process_entry(&mut self, request: EntryRequest) -> Result<EntryProcessed, String> {
        self.snapshot_export_seal = None;
        if request.slot >= MAX_SUPPORTED_SLOT_EXCLUSIVE {
            return Err(format!(
                "Solana v1.0.7 candidate runtime ends before slot {} (requested {})",
                MAX_SUPPORTED_SLOT_EXCLUSIVE, request.slot
            ));
        }
        if request.hash.len() != 32 {
            return Err(format!(
                "entry hash has {} bytes, expected 32",
                request.hash.len()
            ));
        }
        if request.slot < self.bank.slot() {
            return Err(format!(
                "entry slot {} precedes current bank slot {}",
                request.slot,
                self.bank.slot()
            ));
        }
        let expected_index = if request.slot == self.bank.slot() {
            self.next_entry_index
        } else {
            0
        };
        if request.entry_index != expected_index {
            return Err(format!(
                "entry index {} for slot {} does not match expected {}",
                request.entry_index, request.slot, expected_index
            ));
        }
        if request.slot == self.bank.slot() && self.bank.is_complete() {
            return Err(format!(
                "entry follows the completion tick for slot {}",
                request.slot
            ));
        }
        if request.slot == self.bank.slot() && self.bank.is_frozen() {
            return Err(format!("bank at slot {} is already frozen", request.slot));
        }
        let transactions: Vec<Transaction> = request
            .transactions
            .iter()
            .enumerate()
            .map(|(index, wire)| {
                bincode::deserialize(wire)
                    .map_err(|error| format!("failed to decode transaction {}: {}", index, error))
            })
            .collect::<Result<_, _>>()?;

        // Match the v1.0.7 BlockstoreProcessor checks before changing Bank
        // state.  In particular, an invalid first entry in a new slot must not
        // freeze/root its parent or create a child that later requests inherit.
        let validation = self.validate_entry(&request, &transactions)?;

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
            let signatures_by_writable_key = writable_key_attribution(&transactions)?;
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
                .find(|(_index, result)| result.is_err())
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

        self.next_entry_index += 1;
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

    pub fn freeze_checkpoint(&mut self, expected_slot: u64) -> Result<Checkpoint, String> {
        self.snapshot_export_seal = None;
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
        let checkpoint = Checkpoint {
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
        };
        self.snapshot_export_seal = Some(SnapshotExportSeal {
            slot: checkpoint.slot,
            accounts_hash,
        });
        Ok(checkpoint)
    }

    pub fn invalidate_snapshot_export(&mut self) {
        self.snapshot_export_seal = None;
    }

    pub fn export_snapshot(
        &mut self,
        slot: u64,
        output_directory: &str,
        expected_accounts_hash: &[u8],
    ) -> Result<SnapshotExport, String> {
        let seal = self.snapshot_export_seal.take().ok_or_else(|| {
            "snapshot export requires the immediately preceding successful checkpoint".to_string()
        })?;
        if seal.slot != slot || self.bank.slot() != slot {
            return Err(format!(
                "snapshot export slot {} does not match checkpoint slot {} and bank slot {}",
                slot,
                seal.slot,
                self.bank.slot()
            ));
        }
        if expected_accounts_hash.len() != 32 {
            return Err(format!(
                "expected accounts hash has {} bytes, expected 32",
                expected_accounts_hash.len()
            ));
        }
        let expected_accounts_hash = Hash::new(expected_accounts_hash);
        if expected_accounts_hash != seal.accounts_hash {
            return Err(format!(
                "snapshot export hash {} does not match checkpoint hash {}",
                expected_accounts_hash, seal.accounts_hash
            ));
        }
        let exported = snapshot::export_archive(
            &self.bank,
            Path::new(output_directory),
            expected_accounts_hash,
        )?;
        let archive_path = exported
            .archive_path
            .to_str()
            .ok_or_else(|| {
                format!(
                    "snapshot archive path is not UTF-8: {}",
                    exported.archive_path.display()
                )
            })?
            .to_string();
        Ok(SnapshotExport {
            slot,
            archive_path,
            accounts_hash: exported.accounts_hash.as_ref().to_vec(),
            archive_size: exported.archive_size,
            archive_sha256: exported.archive_sha256.to_vec(),
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

    fn validate_entry(
        &self,
        request: &EntryRequest,
        transactions: &[Transaction],
    ) -> Result<EntryValidation, String> {
        let starts_new_slot = request.slot > self.bank.slot();
        if starts_new_slot && !self.bank.is_complete() {
            return Err(format!(
                "cannot advance incomplete bank at slot {} (tick height {}, max {})",
                self.bank.slot(),
                self.bank.tick_height(),
                self.bank.max_tick_height()
            ));
        }

        let tick_height = self.bank.tick_height();
        let max_tick_height = if starts_new_slot {
            (request.slot + 1) * self.bank.ticks_per_slot()
        } else {
            self.bank.max_tick_height()
        };
        if tick_height >= max_tick_height {
            return Err(format!(
                "entry follows the completion tick for slot {}",
                request.slot
            ));
        }

        let is_tick = transactions.is_empty();
        if is_tick && tick_height + 1 > max_tick_height {
            return Err(format!(
                "tick entry would exceed max tick height {} in slot {}",
                max_tick_height, request.slot
            ));
        }

        let hashes_per_tick = self.bank.hashes_per_tick().unwrap_or(0);
        let mut next_tick_hash_count = if starts_new_slot {
            0
        } else {
            self.tick_hash_count
        };
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

        let start_hash = if starts_new_slot {
            self.bank.last_blockhash()
        } else {
            self.last_entry_hash
        };
        let expected_hash = next_entry_hash(&start_hash, request.num_hashes, transactions);
        let actual_hash = Hash::new(&request.hash);
        if actual_hash != expected_hash {
            return Err(format!(
                "entry PoH hash mismatch at slot {} index {}: expected {}, got {}",
                request.slot, request.entry_index, expected_hash, actual_hash
            ));
        }

        Ok(EntryValidation {
            hash: actual_hash,
            next_tick_hash_count,
        })
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
            snapshot_export_seal: None,
        }
    }
}

struct EntryValidation {
    hash: Hash,
    next_tick_hash_count: u64,
}

/// Exact scalar equivalent of v1.0.7 ledger::entry::next_hash.  Pulling in
/// the full ledger crate would also pull RocksDB into the isolated worker.
fn next_entry_hash(start_hash: &Hash, num_hashes: u64, transactions: &[Transaction]) -> Hash {
    if num_hashes == 0 && transactions.is_empty() {
        return *start_hash;
    }

    let mut poh_hash = *start_hash;
    for _ in 0..num_hashes.saturating_sub(1) {
        poh_hash = hash(poh_hash.as_ref());
    }
    if transactions.is_empty() {
        hash(poh_hash.as_ref())
    } else {
        let transaction_hash = hash_transactions(transactions);
        hashv(&[poh_hash.as_ref(), transaction_hash.as_ref()])
    }
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
/// Restore it with the exact v1.0.7 processors linked into this worker, so
/// replay never depends on dynamically loaded `.so` files from deployment.
fn restore_mainnet_runtime_hooks(bank: &mut Bank, genesis: &GenesisConfig) -> Result<(), String> {
    validate_mainnet_genesis_programs(genesis)?;
    restore_mainnet_native_processors(bank);

    // The upstream Stable callback has no state effects in epochs 0 through
    // 10. Install the callback explicitly and guard that qualified interval;
    // process_entry rejects the first later slot before constructing its bank.
    bank.set_entered_epoch_callback(Box::new(|bank| {
        assert!(
            bank.epoch() <= MAX_SUPPORTED_EPOCH,
            "Solana v1.0.7 candidate entered unsupported epoch {}",
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
    use sha2::Digest;
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
        let bank = Bank::new_with_paths(&genesis.genesis_config, account_paths);
        (
            RuntimeState::from_test_bank(bank, false, state_dir),
            genesis.mint_keypair,
        )
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
            // This is the pre-v1.0.8 wire form observed on mainnet at slot
            // 521850. The node identity signs the transaction as fee payer,
            // but is deliberately absent from this instruction's account
            // list. v1.0.7 accepted that form.
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
    fn v1_0_7_accepts_old_form_vote_initialization_without_node_instruction_account() {
        let leader = Pubkey::new_from_array([7; 32]);
        let mut genesis = create_genesis_config_with_leader(1_000_000, &leader, 500_000);
        set_exact_mainnet_native_programs(&mut genesis.genesis_config);
        let state_dir = snapshot::private_state_dir(None).unwrap();
        let account_paths = snapshot::private_account_paths(&state_dir).unwrap();
        let mut bank = Bank::new_with_paths(&genesis.genesis_config, account_paths);
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
        assert_eq!(processed.outcomes[0].error, None);
        let stored = state.bank.get_account(&vote_account.pubkey()).unwrap();
        assert_eq!(stored.owner, solana_vote_program::id());
        assert_eq!(
            VoteState::deserialize(&stored.data).unwrap().node_pubkey,
            genesis.mint_keypair.pubkey()
        );
    }

    #[test]
    fn child_bank_restores_static_mainnet_native_dispatch() {
        let leader = Pubkey::new_from_array([7; 32]);
        let mut genesis = create_genesis_config_with_leader(1_000_000, &leader, 500_000);
        set_exact_mainnet_native_programs(&mut genesis.genesis_config);
        let state_dir = snapshot::private_state_dir(None).unwrap();
        let account_paths = snapshot::private_account_paths(&state_dir).unwrap();
        let mut bank0 = Bank::new_with_paths(&genesis.genesis_config, account_paths);
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
        let bank0 = Bank::new_with_paths(&genesis.genesis_config, account_paths);
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
    fn checkpoint_export_is_one_use_no_clobber_and_restore_compatible() {
        let (mut state, mint_keypair) = test_state(1, Some(2));
        let transaction = system_transaction::transfer(
            &mint_keypair,
            &Pubkey::new_from_array([55; 32]),
            1,
            state.bank.last_blockhash(),
        );
        let signature = transaction.signatures[0];
        let transaction_entry = request_for(&state, 0, 0, 1, &[transaction]);
        state.process_entry(transaction_entry).unwrap();
        let tick = request_for(&state, 0, 1, 1, &[]);
        state.process_entry(tick).unwrap();
        let checkpoint = state.freeze_checkpoint(0).unwrap();
        let output = tempfile::tempdir().unwrap();

        let mut wrong_hash = checkpoint.accounts_hash.clone();
        wrong_hash[0] ^= 1;
        assert!(state
            .export_snapshot(0, output.path().to_str().unwrap(), &wrong_hash)
            .unwrap_err()
            .contains("does not match checkpoint hash"));
        assert!(state
            .export_snapshot(
                0,
                output.path().to_str().unwrap(),
                &checkpoint.accounts_hash,
            )
            .unwrap_err()
            .contains("immediately preceding successful checkpoint"));

        let checkpoint = state.freeze_checkpoint(0).unwrap();
        let exported = state
            .export_snapshot(
                0,
                output.path().to_str().unwrap(),
                &checkpoint.accounts_hash,
            )
            .unwrap();
        assert_eq!(exported.slot, 0);
        assert_eq!(exported.accounts_hash, checkpoint.accounts_hash);
        let archive_path = Path::new(&exported.archive_path);
        assert_eq!(
            std::fs::metadata(archive_path).unwrap().len(),
            exported.archive_size
        );
        let archive_bytes = std::fs::read(archive_path).unwrap();
        let mut archive_hasher = sha2::Sha256::new();
        archive_hasher.input(&archive_bytes);
        assert_eq!(
            exported.archive_sha256.as_slice(),
            archive_hasher.result().as_slice()
        );

        let restore_state = snapshot::private_state_dir(None).unwrap();
        let restored = snapshot::load_archive(archive_path, &restore_state).unwrap();
        assert_eq!(restored.bank.slot(), 0);
        assert_eq!(
            restored.expected_accounts_hash.as_ref(),
            checkpoint.accounts_hash.as_slice()
        );
        assert!(restored.bank.verify_snapshot_bank());
        assert_eq!(restored.bank.src.roots(), state.bank.src.roots());
        assert_eq!(restored.bank.get_signature_status(&signature), Some(Ok(())));

        if let Some(directory) = std::env::var_os("JETSTREAMER_V1_0_7_EXPORT_FIXTURE_DIR") {
            let fixture_path = Path::new(&directory).join(
                archive_path
                    .file_name()
                    .expect("exported archive has a filename"),
            );
            std::fs::copy(archive_path, &fixture_path).unwrap();
            println!("v1.0.7 export fixture: {}", fixture_path.display());
        }

        let checkpoint = state.freeze_checkpoint(0).unwrap();
        assert!(state
            .export_snapshot(
                0,
                output.path().to_str().unwrap(),
                &checkpoint.accounts_hash,
            )
            .unwrap_err()
            .contains("refusing to replace existing snapshot archive"));
        assert!(archive_path.is_file());
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
    fn initializes_and_checkpoints_mainnet_snapshot_416012() {
        let archive =
            std::env::var("JETSTREAMER_SNAPSHOT_416012").expect("set JETSTREAMER_SNAPSHOT_416012");
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
        assert_eq!(initialized.slot, 416_012);
        let checkpoint = state.freeze_checkpoint(416_012).unwrap();
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
            "BHoMQfw7kguThAkkSeR8xGjfsr9djqwzwsZ7wJ9TdfTd"
        );
    }
}
