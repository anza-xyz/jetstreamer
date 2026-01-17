use std::{
    env, fs,
    path::{Path, PathBuf},
    process::{Stdio, exit},
    sync::{Arc, Mutex, atomic::AtomicBool},
};

use agave_snapshots::{
    snapshot_archive_info::{FullSnapshotArchiveInfo, SnapshotArchiveInfoGetter},
    streaming_unarchive_snapshot,
};
use crossbeam_channel::unbounded;
use jetstreamer_firehose::{
    epochs::epoch_to_slot_range,
    firehose::{GeyserNotifiers, firehose_geyser_with_notifiers},
    index::get_index_base_url,
};
use jetstreamer_node::snapshots::{DEFAULT_BUCKET, download_snapshot_at_or_before_slot};
use log::info;
use reqwest::Client;
use solana_accounts_db::{
    accounts_db::AccountsDbConfig, accounts_update_notifier_interface::AccountsUpdateNotifier,
};
use solana_clock::Slot;
use solana_genesis_utils::{MAX_GENESIS_ARCHIVE_UNPACKED_SIZE, open_genesis_config};
use solana_geyser_plugin_manager::geyser_plugin_service::GeyserPluginService;
use solana_hash::Hash;
use solana_ledger::entry_notifier_interface::EntryNotifier;
use solana_rpc::transaction_notifier_interface::TransactionNotifier;
use solana_runtime::{
    bank::Bank, installed_scheduler_pool::BankWithScheduler, runtime_config::RuntimeConfig,
    snapshot_bank_utils, snapshot_utils,
};
use solana_signature::Signature;
use solana_transaction::versioned::VersionedTransaction;
use solana_transaction_status::TransactionStatusMeta;
use tokio::process::Command;

const PLUGIN_NAME: &str = "jetstreamer-node-geyser";
const PLUGIN_LIB_BASENAME: &str = "jetstreamer_node_geyser";
const BANK_SNAPSHOTS_DIR: &str = "snapshots";
const ACCOUNTS_HARDLINKS_DIR: &str = "accounts_hardlinks";
const GENESIS_ARCHIVE: &str = "genesis.tar.bz2";
const SNAPSHOT_VERSION_FILE: &str = "version";
const SNAPSHOT_STATUS_CACHE_FILE: &str = "status_cache";
const ACCOUNTS_SNAPSHOT_DIR: &str = "snapshot";
const ACCOUNTS_RUN_DIR: &str = "run";

struct BankReplay {
    bank: Mutex<Arc<Bank>>,
}

impl BankReplay {
    fn new(bank: Bank) -> Self {
        Self {
            bank: Mutex::new(Arc::new(bank)),
        }
    }

    fn bank_for_slot(&self, slot: Slot) -> Result<Arc<Bank>, String> {
        let mut guard = self
            .bank
            .lock()
            .map_err(|_| "bank lock poisoned".to_string())?;
        let current_slot = guard.slot();
        if slot < current_slot {
            return Err(format!(
                "slot {slot} behind current bank slot {current_slot}"
            ));
        }
        if slot > current_slot {
            guard.freeze();
            let collector_id = *guard.collector_id();
            let next_bank = Bank::new_from_parent(guard.clone(), &collector_id, slot);
            *guard = Arc::new(next_bank);
        }
        Ok(guard.clone())
    }

    fn register_tick(&self, slot: Slot, hash: Hash) -> Result<(), String> {
        let bank = self.bank_for_slot(slot)?;
        let bank_with_scheduler = BankWithScheduler::new_without_scheduler(bank);
        bank_with_scheduler.register_tick(&hash);
        Ok(())
    }
}

struct BankTransactionNotifier {
    bank_replay: Arc<BankReplay>,
    delegate: Option<Arc<dyn TransactionNotifier + Send + Sync + 'static>>,
}

impl TransactionNotifier for BankTransactionNotifier {
    fn notify_transaction(
        &self,
        slot: Slot,
        transaction_slot_index: usize,
        signature: &Signature,
        message_hash: &Hash,
        is_vote: bool,
        transaction_status_meta: &TransactionStatusMeta,
        transaction: &VersionedTransaction,
    ) {
        match self.bank_replay.bank_for_slot(slot) {
            Ok(bank) => match bank.try_process_entry_transactions(vec![transaction.clone()]) {
                Ok(mut results) => {
                    if let Some(Err(err)) = results.pop() {
                        log::debug!(
                            "transaction execution failed at slot {slot} index {transaction_slot_index}: {err}"
                        );
                    }
                }
                Err(err) => {
                    log::debug!(
                        "transaction batch execution failed at slot {slot} index {transaction_slot_index}: {err}"
                    );
                }
            },
            Err(err) => {
                log::debug!(
                    "skipping transaction execution at slot {slot} index {transaction_slot_index}: {err}"
                );
            }
        }

        if let Some(delegate) = self.delegate.as_ref() {
            delegate.notify_transaction(
                slot,
                transaction_slot_index,
                signature,
                message_hash,
                is_vote,
                transaction_status_meta,
                transaction,
            );
        }
    }
}

struct BankEntryNotifier {
    bank_replay: Arc<BankReplay>,
    delegate: Option<Arc<dyn EntryNotifier + Send + Sync + 'static>>,
}

impl EntryNotifier for BankEntryNotifier {
    fn notify_entry(
        &self,
        slot: Slot,
        index: usize,
        entry: &solana_entry::entry::EntrySummary,
        starting_transaction_index: usize,
    ) {
        if entry.num_transactions == 0 {
            if let Err(err) = self.bank_replay.register_tick(slot, entry.hash) {
                log::debug!("failed to register tick for slot {slot} entry {index}: {err}");
            }
        }

        if let Some(delegate) = self.delegate.as_ref() {
            delegate.notify_entry(slot, index, entry, starting_transaction_index);
        }
    }
}

fn epoch_to_slot(epoch: u64) -> u64 {
    epoch_to_slot_range(epoch).0
}

fn usage(program: &str) -> String {
    format!("Usage: {program} <epoch> [dest-dir]")
}

fn plugin_library_filename() -> String {
    if cfg!(target_os = "windows") {
        format!("{PLUGIN_LIB_BASENAME}.dll")
    } else if cfg!(target_os = "macos") {
        format!("lib{PLUGIN_LIB_BASENAME}.dylib")
    } else {
        format!("lib{PLUGIN_LIB_BASENAME}.so")
    }
}

fn suggested_plugin_build_command(profile_dir: &Path) -> String {
    let profile = profile_dir.file_name().and_then(|name| name.to_str());
    if matches!(profile, Some("release")) {
        "cargo build -p jetstreamer-node-geyser --release".to_string()
    } else {
        "cargo build -p jetstreamer-node-geyser".to_string()
    }
}

fn plugin_library_path() -> Result<PathBuf, String> {
    if let Ok(path) = env::var("JETSTREAMER_NODE_GEYSER_LIB") {
        return Ok(PathBuf::from(path));
    }

    let exe = env::current_exe()
        .map_err(|err| format!("failed to read current executable path: {err}"))?;
    let profile_dir = exe
        .parent()
        .ok_or_else(|| "failed to resolve executable directory".to_string())?;
    let lib_path = profile_dir.join(plugin_library_filename());
    if lib_path.exists() {
        return Ok(lib_path);
    }

    Err(format!(
        "geyser plugin library not found at {} (build with `{}`)",
        lib_path.display(),
        suggested_plugin_build_command(profile_dir),
    ))
}

fn write_geyser_config(dest_dir: &Path, libpath: &Path) -> Result<PathBuf, String> {
    let config_path = dest_dir.join("jetstreamer-node-geyser.json");
    let config = serde_json::json!({
        "libpath": libpath.display().to_string(),
        "name": PLUGIN_NAME,
    });
    let contents =
        serde_json::to_string_pretty(&config).map_err(|err| format!("config json error: {err}"))?;
    fs::write(&config_path, contents).map_err(|err| format!("failed to write config: {err}"))?;
    Ok(config_path)
}

async fn ensure_genesis_archive(ledger_dir: &Path) -> Result<(), String> {
    let genesis_bin = ledger_dir.join("genesis.bin");
    let genesis_archive = ledger_dir.join(GENESIS_ARCHIVE);
    if genesis_bin.exists() || genesis_archive.exists() {
        return Ok(());
    }

    let uri = format!("{DEFAULT_BUCKET}/{GENESIS_ARCHIVE}");
    let status = Command::new("gcloud")
        .arg("storage")
        .arg("cp")
        .arg(&uri)
        .arg(&genesis_archive)
        .env("CLOUDSDK_CORE_DISABLE_PROMPTS", "1")
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .status()
        .await
        .map_err(|err| format!("failed to run gcloud: {err}"))?;

    if !status.success() {
        return Err(format!(
            "gcloud storage cp {uri} {} failed",
            genesis_archive.display(),
        ));
    }

    Ok(())
}

fn account_run_paths_from_snapshot(bank_snapshot_dir: &Path) -> Result<Vec<PathBuf>, String> {
    let hardlinks_dir = bank_snapshot_dir.join(ACCOUNTS_HARDLINKS_DIR);
    let read_dir = fs::read_dir(&hardlinks_dir)
        .map_err(|err| format!("failed to read {}: {err}", hardlinks_dir.display()))?;

    let mut run_paths = Vec::new();
    for entry in read_dir {
        let entry = entry.map_err(|err| format!("failed to read dir entry: {err}"))?;
        let symlink_path = entry.path();
        let link_target = fs::read_link(&symlink_path)
            .map_err(|err| format!("failed to read link {}: {err}", symlink_path.display()))?;
        let target = if link_target.is_absolute() {
            link_target
        } else {
            let parent = symlink_path
                .parent()
                .ok_or_else(|| "missing symlink parent".to_string())?;
            parent.join(link_target)
        };
        let run_path = target
            .parent()
            .and_then(|parent| parent.parent())
            .ok_or_else(|| format!("invalid account snapshot path {}", target.display()))?
            .join("run");
        run_paths.push(run_path);
    }

    run_paths.sort();
    run_paths.dedup();
    if run_paths.is_empty() {
        return Err(format!(
            "no account paths found under {}",
            hardlinks_dir.display()
        ));
    }
    Ok(run_paths)
}

fn link_or_copy(src: &Path, dest: &Path) -> Result<(), String> {
    if let Err(err) = fs::hard_link(src, dest) {
        fs::copy(src, dest)
            .map_err(|copy_err| {
                format!(
                    "failed to link {} -> {}: {err}; copy failed: {copy_err}",
                    src.display(),
                    dest.display()
                )
            })
            .map(|_| ())?;
    }
    Ok(())
}

#[cfg(unix)]
fn symlink_dir(src: &Path, dest: &Path) -> Result<(), String> {
    std::os::unix::fs::symlink(src, dest)
        .map_err(|err| format!("failed to symlink {} -> {}: {err}", src.display(), dest.display()))
}

#[cfg(windows)]
fn symlink_dir(src: &Path, dest: &Path) -> Result<(), String> {
    std::os::windows::fs::symlink_dir(src, dest)
        .map_err(|err| format!("failed to symlink {} -> {}: {err}", src.display(), dest.display()))
}

fn ensure_snapshot_meta_files(ledger_dir: &Path) -> Result<bool, String> {
    let snapshots_dir = ledger_dir.join(BANK_SNAPSHOTS_DIR);
    if !snapshots_dir.is_dir() {
        return Ok(false);
    }

    let version_src = [
        ledger_dir.join(SNAPSHOT_VERSION_FILE),
        snapshots_dir.join(SNAPSHOT_VERSION_FILE),
    ]
    .into_iter()
    .find(|path| path.exists())
    .ok_or_else(|| {
        format!(
            "missing snapshot version file under {}",
            ledger_dir.display()
        )
    })?;
    let status_src = snapshots_dir.join(SNAPSHOT_STATUS_CACHE_FILE);
    if !status_src.exists() {
        return Err(format!(
            "missing snapshot status cache file {}",
            status_src.display()
        ));
    }

    let mut slot_dirs = Vec::new();
    let read_dir = fs::read_dir(&snapshots_dir)
        .map_err(|err| format!("failed to read {}: {err}", snapshots_dir.display()))?;
    for entry in read_dir {
        let entry = entry.map_err(|err| format!("failed to read dir entry: {err}"))?;
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if name.parse::<u64>().is_ok() {
            slot_dirs.push(path);
        }
    }

    if slot_dirs.is_empty() {
        return Ok(false);
    }

    let mut changed = false;
    for slot_dir in slot_dirs {
        let version_dest = slot_dir.join(SNAPSHOT_VERSION_FILE);
        if !version_dest.exists() {
            link_or_copy(&version_src, &version_dest)?;
            changed = true;
        }
        let status_dest = slot_dir.join(SNAPSHOT_STATUS_CACHE_FILE);
        if !status_dest.exists() {
            link_or_copy(&status_src, &status_dest)?;
            changed = true;
        }
    }

    Ok(changed)
}

fn ensure_accounts_hardlinks(
    ledger_dir: &Path,
    bank_snapshot: &snapshot_utils::BankSnapshotInfo,
) -> Result<bool, String> {
    let hardlinks_dir = bank_snapshot.snapshot_dir.join(ACCOUNTS_HARDLINKS_DIR);
    let mut needs_cleanup = false;
    if hardlinks_dir.is_dir() {
        let entries = fs::read_dir(&hardlinks_dir)
            .map_err(|err| format!("failed to read {}: {err}", hardlinks_dir.display()))?;
        for entry in entries {
            let entry = entry.map_err(|err| format!("failed to read dir entry: {err}"))?;
            let file_type = entry
                .file_type()
                .map_err(|err| format!("failed to read file type: {err}"))?;
            if !file_type.is_symlink() {
                needs_cleanup = true;
                break;
            }
            let link_target =
                fs::read_link(entry.path()).map_err(|err| {
                    format!("failed to read link {}: {err}", entry.path().display())
                })?;
            if link_target.is_relative() || !link_target.exists() {
                needs_cleanup = true;
                break;
            }
        }
    } else {
        fs::create_dir_all(&hardlinks_dir)
            .map_err(|err| format!("failed to create {}: {err}", hardlinks_dir.display()))?;
        needs_cleanup = true;
    }

    if needs_cleanup {
        let entries = fs::read_dir(&hardlinks_dir)
            .map_err(|err| format!("failed to read {}: {err}", hardlinks_dir.display()))?;
        for entry in entries {
            let entry = entry.map_err(|err| format!("failed to read dir entry: {err}"))?;
            let path = entry.path();
            let file_type = entry
                .file_type()
                .map_err(|err| format!("failed to read file type: {err}"))?;
            if file_type.is_dir() && !file_type.is_symlink() {
                fs::remove_dir_all(&path)
                    .map_err(|err| format!("failed to remove {}: {err}", path.display()))?;
            } else {
                fs::remove_file(&path)
                    .map_err(|err| format!("failed to remove {}: {err}", path.display()))?;
            }
        }
    }

    let slot_dir = bank_snapshot.slot.to_string();
    let mut account_paths = Vec::new();
    let mut changed = false;
    let read_dir = fs::read_dir(ledger_dir)
        .map_err(|err| format!("failed to read {}: {err}", ledger_dir.display()))?;
    for entry in read_dir {
        let entry = entry.map_err(|err| format!("failed to read dir entry: {err}"))?;
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        if path == ledger_dir.join(BANK_SNAPSHOTS_DIR) {
            continue;
        }
        let snapshot_dir = path.join(ACCOUNTS_SNAPSHOT_DIR).join(&slot_dir);
        let snapshot_has_files = snapshot_dir
            .read_dir()
            .ok()
            .and_then(|mut dir| dir.next())
            .is_some();
        if snapshot_dir.is_dir() && snapshot_has_files {
            account_paths.push((path, snapshot_dir));
            continue;
        }

        let mut has_appendvecs = false;
        let mut appendvecs = Vec::new();
        let dir_entries = fs::read_dir(&path)
            .map_err(|err| format!("failed to read {}: {err}", path.display()))?;
        for entry in dir_entries {
            let entry = entry.map_err(|err| format!("failed to read dir entry: {err}"))?;
            let file_type = entry
                .file_type()
                .map_err(|err| format!("failed to read file type: {err}"))?;
            if !file_type.is_file() {
                continue;
            }
            let Some(name) = entry.file_name().to_str().map(|s| s.to_string()) else {
                continue;
            };
            if looks_like_appendvec(&name) {
                has_appendvecs = true;
                appendvecs.push(entry.path());
            }
        }

        if has_appendvecs {
            fs::create_dir_all(&snapshot_dir).map_err(|err| {
                format!(
                    "failed to create snapshot dir {}: {err}",
                    snapshot_dir.display()
                )
            })?;
            for file_path in appendvecs {
                let Some(file_name) = file_path.file_name() else {
                    continue;
                };
                let dest_path = snapshot_dir.join(file_name);
                if dest_path.exists() {
                    continue;
                }
                link_or_copy(&file_path, &dest_path)?;
                changed = true;
            }
            let run_path = path.join(ACCOUNTS_RUN_DIR);
            fs::create_dir_all(&run_path)
                .map_err(|err| format!("failed to create {}: {err}", run_path.display()))?;
            account_paths.push((path, snapshot_dir));
        }
    }

    if account_paths.is_empty() {
        return Err(format!(
            "no account snapshot dirs found under {} for slot {}",
            ledger_dir.display(),
            slot_dir
        ));
    }

    for (idx, (account_path, snapshot_dir)) in account_paths.into_iter().enumerate() {
        let run_path = account_path.join(ACCOUNTS_RUN_DIR);
        fs::create_dir_all(&run_path)
            .map_err(|err| format!("failed to create {}: {err}", run_path.display()))?;
        let link_path = hardlinks_dir.join(format!("account_path_{idx}"));
        if link_path.exists() {
            continue;
        }
        let link_target = snapshot_dir
            .canonicalize()
            .unwrap_or_else(|_| snapshot_dir.clone());
        symlink_dir(&link_target, &link_path)?;
        changed = true;
    }

    Ok(changed || needs_cleanup)
}

fn looks_like_appendvec(name: &str) -> bool {
    let mut parts = name.split('.');
    let Some(slot) = parts.next() else {
        return false;
    };
    let Some(id) = parts.next() else {
        return false;
    };
    if parts.next().is_some() {
        return false;
    }
    !slot.is_empty()
        && !id.is_empty()
        && slot.chars().all(|c| c.is_ascii_digit())
        && id.chars().all(|c| c.is_ascii_digit())
}

fn skip_snapshot_verify() -> bool {
    match env::var("JETSTREAMER_SKIP_SNAPSHOT_VERIFY") {
        Ok(value) => {
            let value = value.trim().to_ascii_lowercase();
            !matches!(value.as_str(), "" | "0" | "false" | "no")
        }
        Err(_) => false,
    }
}

fn load_bank_from_snapshot(
    ledger_dir: &Path,
    accounts_update_notifier: Option<AccountsUpdateNotifier>,
) -> Result<Bank, String> {
    let bank_snapshots_dir = ledger_dir.join(BANK_SNAPSHOTS_DIR);
    let mut bank_snapshot = snapshot_utils::get_highest_bank_snapshot(&bank_snapshots_dir);
    if bank_snapshot.is_none() {
        if ensure_snapshot_meta_files(ledger_dir)? {
            info!(
                "repaired snapshot metadata in {}",
                bank_snapshots_dir.display()
            );
        }
        bank_snapshot = snapshot_utils::get_highest_bank_snapshot(&bank_snapshots_dir);
    }
    let bank_snapshot = bank_snapshot.ok_or_else(|| {
        format!(
            "no bank snapshots found in {}",
            bank_snapshots_dir.display()
        )
    })?;

    if ensure_accounts_hardlinks(ledger_dir, &bank_snapshot)? {
        info!(
            "repaired accounts hardlinks in {}",
            bank_snapshot.snapshot_dir.display()
        );
    }
    let account_run_paths = account_run_paths_from_snapshot(&bank_snapshot.snapshot_dir)?;
    for path in &account_run_paths {
        fs::create_dir_all(path)
            .map_err(|err| format!("failed to create {}: {err}", path.display()))?;
    }

    let genesis_config = open_genesis_config(ledger_dir, MAX_GENESIS_ARCHIVE_UNPACKED_SIZE)
        .map_err(|err| format!("failed to load genesis config: {err}"))?;
    let runtime_config = RuntimeConfig::default();
    let accounts_db_config = AccountsDbConfig::default();
    let exit = Arc::new(AtomicBool::new(false));
    let limit_load_slot_count_from_snapshot = if skip_snapshot_verify() {
        info!("snapshot verification disabled via JETSTREAMER_SKIP_SNAPSHOT_VERIFY");
        Some(usize::MAX)
    } else {
        None
    };

    snapshot_bank_utils::bank_from_snapshot_dir(
        &account_run_paths,
        &bank_snapshot,
        &genesis_config,
        &runtime_config,
        None,
        limit_load_slot_count_from_snapshot,
        false,
        accounts_db_config,
        accounts_update_notifier,
        exit,
    )
    .map_err(|err| format!("failed to build bank from snapshot: {err}"))
}

fn firehose_threads() -> u64 {
    env::var("JETSTREAMER_THREADS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(1)
}

async fn run_geyser_replay(epoch: u64, ledger_dir: &Path) -> Result<(), String> {
    let libpath = plugin_library_path()?;
    let config_path = write_geyser_config(ledger_dir, &libpath)?;
    let (confirmed_bank_sender, confirmed_bank_receiver) = unbounded();
    let config_files = [config_path];

    let service = GeyserPluginService::new(confirmed_bank_receiver, true, &config_files)
        .map_err(|err| format!("failed to load geyser plugin: {err}"))?;
    let accounts_update_notifier = service.get_accounts_update_notifier();

    ensure_genesis_archive(ledger_dir).await?;
    info!("loading bank from snapshot");
    let ledger_dir = ledger_dir.to_path_buf();
    let bank = tokio::task::spawn_blocking(move || {
        load_bank_from_snapshot(&ledger_dir, accounts_update_notifier)
    })
    .await
    .map_err(|err| format!("snapshot load task failed: {err}"))??;
    let bank_replay = Arc::new(BankReplay::new(bank));

    let (start_slot, end_inclusive) = epoch_to_slot_range(epoch);
    let slot_range = start_slot..(end_inclusive + 1);

    let rt = Arc::new(
        tokio::runtime::Runtime::new().map_err(|err| format!("failed to create runtime: {err}"))?,
    );
    let client = Client::new();
    let index_base_url =
        get_index_base_url().map_err(|err| format!("failed to resolve index base url: {err}"))?;
    let transaction_notifier = Arc::new(BankTransactionNotifier {
        bank_replay: bank_replay.clone(),
        delegate: service.get_transaction_notifier(),
    });
    let entry_notifier = Arc::new(BankEntryNotifier {
        bank_replay,
        delegate: service.get_entry_notifier(),
    });
    let notifiers = GeyserNotifiers {
        transaction_notifier: Some(transaction_notifier),
        entry_notifier: Some(entry_notifier),
        block_metadata_notifier: service.get_block_metadata_notifier(),
    };
    let mut threads = firehose_threads();
    if threads > 1 {
        info!("forcing firehose threads to 1 for bank replay");
        threads = 1;
    }

    info!("starting firehose replay with {} thread(s)", threads);
    firehose_geyser_with_notifiers(
        rt,
        slot_range,
        notifiers,
        confirmed_bank_sender,
        &index_base_url,
        &client,
        async { Ok(()) },
        threads,
    )
    .map_err(|(err, slot)| format!("firehose error at slot {slot}: {err}"))?;

    service
        .join()
        .map_err(|err| format!("geyser service join failed: {err:?}"))?;

    Ok(())
}

async fn extract_tarball(archive: &Path, dest_dir: &Path) -> Result<(), String> {
    let archive = archive.to_path_buf();
    let dest_dir = dest_dir.to_path_buf();
    tokio::task::spawn_blocking(move || {
        fn remove_path_if_exists(path: &Path) -> Result<(), String> {
            if !path.exists() {
                return Ok(());
            }
            let metadata = fs::symlink_metadata(path)
                .map_err(|err| format!("failed to read {}: {err}", path.display()))?;
            if metadata.is_dir() {
                fs::remove_dir_all(path)
                    .map_err(|err| format!("failed to remove {}: {err}", path.display()))?;
            } else {
                fs::remove_file(path)
                    .map_err(|err| format!("failed to remove {}: {err}", path.display()))?;
            }
            Ok(())
        }

        fs::create_dir_all(&dest_dir)
            .map_err(|err| format!("failed to create {}: {err}", dest_dir.display()))?;
        remove_path_if_exists(&dest_dir.join("accounts"))?;
        remove_path_if_exists(&dest_dir.join(BANK_SNAPSHOTS_DIR))?;
        remove_path_if_exists(&dest_dir.join(SNAPSHOT_VERSION_FILE))?;

        let account_path = dest_dir.join("accounts");
        fs::create_dir_all(&account_path)
            .map_err(|err| format!("failed to create {}: {err}", account_path.display()))?;

        if let Ok(archive_info) = FullSnapshotArchiveInfo::new_from_path(archive.clone()) {
            let (sender, receiver) = unbounded();
            let drain = std::thread::spawn(move || {
                for _ in receiver.iter() {}
            });
            let handle = streaming_unarchive_snapshot(
                sender,
                vec![account_path],
                dest_dir.clone(),
                archive,
                archive_info.archive_format(),
                0,
            );
            let result = handle
                .join()
                .map_err(|_| "snapshot unarchive thread panicked".to_string())?;
            result.map_err(|err| format!("snapshot unarchive failed: {err}"))?;
            let _ = drain.join();
            Ok(())
        } else {
            let output = std::process::Command::new("tar")
                .arg("-xf")
                .arg(&archive)
                .arg("-C")
                .arg(&dest_dir)
                .output()
                .map_err(|err| format!("failed to run tar: {err}"))?;
            if !output.status.success() {
                let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
                let command = format!("tar -xf {} -C {}", archive.display(), dest_dir.display());
                if stderr.is_empty() {
                    return Err(format!("{command} failed"));
                }
                return Err(format!("{command} failed: {stderr}"));
            }
            Ok(())
        }
    })
    .await
    .map_err(|err| format!("snapshot unarchive task failed: {err}"))?
}

#[tokio::main]
async fn main() {
    solana_logger::setup_with_default("info");
    let mut args = env::args();
    let program = args
        .next()
        .unwrap_or_else(|| "jetstreamer-node".to_string());
    let Some(epoch_arg) = args.next() else {
        eprintln!("{}", usage(&program));
        exit(2);
    };

    if epoch_arg == "-h" || epoch_arg == "--help" {
        println!("{}", usage(&program));
        return;
    }

    let epoch: u64 = match epoch_arg.parse() {
        Ok(epoch) => epoch,
        Err(err) => {
            eprintln!("invalid epoch '{epoch_arg}': {err}");
            eprintln!("{}", usage(&program));
            exit(2);
        }
    };

    let dest_dir = match args.next() {
        Some(path) => PathBuf::from(path),
        None => match env::current_dir() {
            Ok(path) => path,
            Err(err) => {
                eprintln!("failed to read current directory: {err}");
                exit(1);
            }
        },
    };

    let target_slot = epoch_to_slot(epoch).saturating_sub(1);
    let dest_path = match download_snapshot_at_or_before_slot(epoch, target_slot, &dest_dir).await {
        Ok(path) => path,
        Err(err) => {
            eprintln!("error: {err}");
            exit(1);
        }
    };

    println!("Downloaded snapshot to {}", dest_path.display());
    println!("Extracting snapshot into {}", dest_dir.display());
    if let Err(err) = extract_tarball(&dest_path, &dest_dir).await {
        eprintln!("error: {err}");
        exit(1);
    }
    println!("Extraction complete");

    if let Err(err) = run_geyser_replay(epoch, &dest_dir).await {
        eprintln!("error: {err}");
        exit(1);
    }
}
