use bzip2::bufread::BzDecoder;
use solana_runtime::bank::{
    self, bank_1_0::Bank1_0, deserialize_from_snapshot, Bank, BankSlotDelta,
    MAX_SNAPSHOT_DATA_FILE_SIZE,
};
use solana_sdk::{genesis_config::GenesisConfig, hash::Hash};
use std::{
    fs::{self, File},
    io::{BufReader, Read, Seek, SeekFrom},
    path::{Path, PathBuf},
    str::FromStr,
};
use tar::Archive;
use tempfile::{Builder, TempDir};

const SNAPSHOT_VERSION: &str = "1.0.0";

pub struct LoadedSnapshot {
    pub bank: Bank,
    pub expected_accounts_hash: Hash,
}

pub fn private_state_dir(scratch_root: Option<&str>) -> Result<TempDir, String> {
    let mut builder = Builder::new();
    builder.prefix("jetstreamer-historical-v1.0.24-");
    match scratch_root {
        Some(root) => {
            let root = Path::new(root);
            fs::create_dir_all(root).map_err(|error| {
                format!(
                    "failed to create scratch root {}: {}",
                    root.display(),
                    error
                )
            })?;
            builder.tempdir_in(root).map_err(|error| {
                format!(
                    "failed to create private state under {}: {}",
                    root.display(),
                    error
                )
            })
        }
        None => builder
            .tempdir()
            .map_err(|error| format!("failed to create private state: {}", error)),
    }
}

pub fn private_account_paths(state_dir: &TempDir) -> Result<Vec<PathBuf>, String> {
    let root = state_dir.path().join("accounts-state");
    (0..4)
        .map(|index| {
            let path = root.join(index.to_string());
            fs::create_dir_all(&path)
                .map_err(|error| format!("failed to create {}: {}", path.display(), error))?;
            Ok(path)
        })
        .collect()
}

pub fn load_archive(archive_path: &Path, state_dir: &TempDir) -> Result<LoadedSnapshot, String> {
    let (expected_slot, expected_accounts_hash) = parse_archive_identity(archive_path)?;
    let unpacked = state_dir.path().join("unpacked");
    fs::create_dir_all(&unpacked)
        .map_err(|error| format!("failed to create {}: {}", unpacked.display(), error))?;

    let archive_file = File::open(archive_path).map_err(|error| {
        format!(
            "failed to open snapshot {}: {}",
            archive_path.display(),
            error
        )
    })?;
    let decoder = BzDecoder::new(BufReader::new(archive_file));
    Archive::new(decoder).unpack(&unpacked).map_err(|error| {
        format!(
            "failed to unpack snapshot {}: {}",
            archive_path.display(),
            error
        )
    })?;

    let version_path = unpacked.join("version");
    let mut version = String::new();
    File::open(&version_path)
        .and_then(|mut file| file.read_to_string(&mut version))
        .map_err(|error| format!("failed to read {}: {}", version_path.display(), error))?;
    if version.trim() != SNAPSHOT_VERSION {
        return Err(format!(
            "unsupported snapshot version {:?}; expected {:?}",
            version.trim(),
            SNAPSHOT_VERSION
        ));
    }

    let snapshots_dir = unpacked.join("snapshots");
    let (snapshot_slot, bank_path) = sole_bank_snapshot(&snapshots_dir)?;
    if snapshot_slot != expected_slot {
        return Err(format!(
            "snapshot filename slot {} does not match archive contents slot {}",
            expected_slot, snapshot_slot
        ));
    }
    let account_paths = private_account_paths(&state_dir)?;
    let append_vecs_path = unpacked.join("accounts");
    let bank = deserialize_exact_file(&bank_path, |stream| {
        let old: Bank1_0 = deserialize_from_snapshot(stream.by_ref())
            .map_err(|error| format!("failed to decode Bank1_0: {}", error))?;
        let mut bank = old.convert_to_current();
        let rc = bank::BankRc::from_stream(
            &account_paths,
            bank.slot(),
            &bank.ancestors,
            &[],
            stream,
            &append_vecs_path,
        )
        .map_err(|error| format!("failed to rebuild snapshot AccountsDB: {}", error))?;
        bank.set_bank_rc(rc, bank::StatusCacheRc::default());
        Ok(bank)
    })?;

    let status_cache_path = snapshots_dir.join("status_cache");
    let slot_deltas: Vec<BankSlotDelta> = deserialize_exact_file(&status_cache_path, |stream| {
        deserialize_from_snapshot(stream)
            .map_err(|error| format!("failed to decode snapshot status cache: {}", error))
    })?;
    bank.src.append(&slot_deltas);

    if bank.slot() != expected_slot {
        return Err(format!(
            "decoded bank slot {} does not match archive slot {}",
            bank.slot(),
            expected_slot
        ));
    }
    if bank.get_accounts_hash() != expected_accounts_hash {
        return Err(format!(
            "decoded accounts hash {} does not match archive filename {}",
            bank.get_accounts_hash(),
            expected_accounts_hash
        ));
    }
    if !bank.verify_snapshot_bank() {
        return Err(format!(
            "snapshot bank verification failed at slot {}",
            bank.slot()
        ));
    }

    Ok(LoadedSnapshot {
        bank,
        expected_accounts_hash,
    })
}

/// Load an already-extracted genesis or unpack the normal js2 genesis archive
/// into worker-private storage.  The shared ledger path is never modified.
pub fn load_genesis(ledger_path: &Path, state_dir: &TempDir) -> Result<GenesisConfig, String> {
    let direct_directory = if ledger_path.is_file()
        && ledger_path.file_name().and_then(|name| name.to_str()) == Some("genesis.bin")
    {
        ledger_path.parent().unwrap_or_else(|| Path::new("."))
    } else {
        ledger_path
    };
    if direct_directory.join("genesis.bin").is_file() {
        return GenesisConfig::load(direct_directory).map_err(|error| {
            format!(
                "failed to load {}: {}",
                direct_directory.join("genesis.bin").display(),
                error
            )
        });
    }

    let archive_path = if ledger_path.is_file() {
        ledger_path.to_path_buf()
    } else {
        ledger_path.join("genesis.tar.bz2")
    };
    if archive_path.file_name().and_then(|name| name.to_str()) != Some("genesis.tar.bz2") {
        return Err(format!(
            "{} is neither genesis.bin nor genesis.tar.bz2",
            ledger_path.display()
        ));
    }
    let unpacked = state_dir.path().join("genesis-unpacked");
    fs::create_dir_all(&unpacked)
        .map_err(|error| format!("failed to create {}: {}", unpacked.display(), error))?;
    let archive = File::open(&archive_path)
        .map_err(|error| format!("failed to open {}: {}", archive_path.display(), error))?;
    let decoder = BzDecoder::new(BufReader::new(archive));
    Archive::new(decoder).unpack(&unpacked).map_err(|error| {
        format!(
            "failed to unpack genesis archive {}: {}",
            archive_path.display(),
            error
        )
    })?;
    if !unpacked.join("genesis.bin").is_file() {
        return Err(format!(
            "genesis archive {} did not contain root genesis.bin",
            archive_path.display()
        ));
    }
    GenesisConfig::load(&unpacked)
        .map_err(|error| format!("failed to load extracted genesis.bin: {}", error))
}

fn deserialize_exact_file<T, F>(path: &Path, mut decode: F) -> Result<T, String>
where
    F: FnMut(&mut BufReader<File>) -> Result<T, String>,
{
    let size = fs::metadata(path)
        .map_err(|error| format!("failed to stat {}: {}", path.display(), error))?
        .len();
    if size > MAX_SNAPSHOT_DATA_FILE_SIZE {
        return Err(format!(
            "snapshot data file {} is {} bytes (limit {})",
            path.display(),
            size,
            MAX_SNAPSHOT_DATA_FILE_SIZE
        ));
    }
    let file = File::open(path)
        .map_err(|error| format!("failed to open {}: {}", path.display(), error))?;
    let mut stream = BufReader::new(file);
    let value = decode(&mut stream)?;
    let consumed = stream
        .seek(SeekFrom::Current(0))
        .map_err(|error| format!("failed to seek {}: {}", path.display(), error))?;
    if consumed != size {
        return Err(format!(
            "snapshot data file {} has {} trailing bytes",
            path.display(),
            size - consumed
        ));
    }
    Ok(value)
}

fn sole_bank_snapshot(snapshots_dir: &Path) -> Result<(u64, PathBuf), String> {
    let mut slot_dirs = Vec::new();
    for entry in fs::read_dir(snapshots_dir)
        .map_err(|error| format!("failed to list {}: {}", snapshots_dir.display(), error))?
    {
        let entry = entry.map_err(|error| format!("failed to list snapshot entry: {}", error))?;
        if entry.file_name() == "status_cache" {
            continue;
        }
        if !entry
            .file_type()
            .map_err(|error| format!("failed to stat snapshot entry: {}", error))?
            .is_dir()
        {
            return Err(format!(
                "unexpected snapshot entry {}",
                entry.path().display()
            ));
        }
        slot_dirs.push(entry.path());
    }
    if slot_dirs.len() != 1 {
        return Err(format!(
            "expected exactly one bank snapshot directory, found {}",
            slot_dirs.len()
        ));
    }
    let slot_dir = slot_dirs.pop().unwrap();
    let slot_text = slot_dir
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| "snapshot slot directory is not UTF-8".to_string())?;
    let slot = slot_text
        .parse::<u64>()
        .map_err(|error| format!("invalid snapshot slot {:?}: {}", slot_text, error))?;
    let bank_path = slot_dir.join(slot_text);
    if !bank_path.is_file() {
        return Err(format!("missing bank snapshot {}", bank_path.display()));
    }
    let count = fs::read_dir(&slot_dir)
        .map_err(|error| format!("failed to list {}: {}", slot_dir.display(), error))?
        .count();
    if count != 1 {
        return Err(format!(
            "bank snapshot directory {} contains {} entries",
            slot_dir.display(),
            count
        ));
    }
    Ok((slot, bank_path))
}

fn parse_archive_identity(path: &Path) -> Result<(u64, Hash), String> {
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| "snapshot archive filename is not UTF-8".to_string())?;
    const PREFIX: &str = "snapshot-";
    const SUFFIX: &str = ".tar.bz2";
    if !name.starts_with(PREFIX)
        || !name.ends_with(SUFFIX)
        || name.len() <= PREFIX.len() + SUFFIX.len()
    {
        return Err(format!("invalid snapshot archive filename {:?}", name));
    }
    let body = &name[PREFIX.len()..name.len() - SUFFIX.len()];
    let mut parts = body.splitn(2, '-');
    let slot_text = parts
        .next()
        .ok_or_else(|| format!("invalid snapshot archive filename {:?}", name))?;
    let hash_text = parts
        .next()
        .ok_or_else(|| format!("invalid snapshot archive filename {:?}", name))?;
    let slot = slot_text
        .parse::<u64>()
        .map_err(|error| format!("invalid snapshot slot {:?}: {}", slot_text, error))?;
    let hash = Hash::from_str(hash_text)
        .map_err(|error| format!("invalid snapshot accounts hash {:?}: {}", hash_text, error))?;
    Ok((slot, hash))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_historical_archive_identity() {
        let (slot, hash) = parse_archive_identity(Path::new(
            "snapshot-416012-BHoMQfw7kguThAkkSeR8xGjfsr9djqwzwsZ7wJ9TdfTd.tar.bz2",
        ))
        .unwrap();
        assert_eq!(slot, 416_012);
        assert_eq!(
            hash.to_string(),
            "BHoMQfw7kguThAkkSeR8xGjfsr9djqwzwsZ7wJ9TdfTd"
        );
    }

    #[test]
    #[ignore]
    fn loads_and_verifies_mainnet_snapshot_416012() {
        let path = std::env::var("JETSTREAMER_SNAPSHOT_416012")
            .expect("set JETSTREAMER_SNAPSHOT_416012 to run this integration test");
        let state_dir = private_state_dir(None).unwrap();
        let loaded = load_archive(Path::new(&path), &state_dir).unwrap();
        assert_eq!(loaded.bank.slot(), 416_012);
        assert_eq!(
            loaded.expected_accounts_hash.to_string(),
            "BHoMQfw7kguThAkkSeR8xGjfsr9djqwzwsZ7wJ9TdfTd"
        );
    }

    #[test]
    fn loads_direct_and_archived_genesis_without_writing_source() {
        use bzip2::{write::BzEncoder, Compression};
        use solana_runtime::genesis_utils::create_genesis_config;
        use std::fs::File;
        use tar::Builder as TarBuilder;

        let direct = tempfile::tempdir().unwrap();
        let genesis = create_genesis_config(42).genesis_config;
        genesis.write(direct.path()).unwrap();
        let direct_metadata = fs::metadata(direct.path().join("genesis.bin")).unwrap();
        let state_dir = private_state_dir(None).unwrap();
        let loaded = load_genesis(direct.path(), &state_dir).unwrap();
        assert_eq!(loaded.hash(), genesis.hash());
        assert_eq!(
            fs::metadata(direct.path().join("genesis.bin"))
                .unwrap()
                .len(),
            direct_metadata.len()
        );

        let archived = tempfile::tempdir().unwrap();
        let archive_path = archived.path().join("genesis.tar.bz2");
        let archive_file = File::create(&archive_path).unwrap();
        let encoder = BzEncoder::new(archive_file, Compression::Best);
        let mut tar = TarBuilder::new(encoder);
        tar.append_path_with_name(direct.path().join("genesis.bin"), "genesis.bin")
            .unwrap();
        let encoder = tar.into_inner().unwrap();
        encoder.finish().unwrap();
        let archive_size = fs::metadata(&archive_path).unwrap().len();
        let archive_state = private_state_dir(None).unwrap();
        let loaded = load_genesis(&archive_path, &archive_state).unwrap();
        assert_eq!(loaded.hash(), genesis.hash());
        assert_eq!(fs::metadata(&archive_path).unwrap().len(), archive_size);
    }
}
