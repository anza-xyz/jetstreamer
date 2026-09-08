use bzip2::{bufread::BzDecoder, write::BzEncoder, Compression};
use sha2::{Digest, Sha256};
use solana_runtime::bank::{
    self, deserialize_from_snapshot, Bank, BankRcSerialize, BankSlotDelta,
    MAX_SNAPSHOT_DATA_FILE_SIZE,
};
use solana_sdk::{genesis_config::GenesisConfig, hash::Hash};
use std::{
    collections::BTreeSet,
    convert::TryFrom,
    fs::{self, File},
    io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    str::FromStr,
};
use tar::{Archive, Builder as TarBuilder, EntryType, Header};
use tempfile::{Builder, TempDir};

const SNAPSHOT_VERSION: &str = "1.0.0";

pub struct LoadedSnapshot {
    pub bank: Bank,
    pub expected_accounts_hash: Hash,
}

pub struct ExportedSnapshot {
    pub archive_path: PathBuf,
    pub accounts_hash: Hash,
    pub archive_size: u64,
    pub archive_sha256: [u8; 32],
}

/// Write the v1.0.7 `1.0.0` snapshot format without invoking the upstream
/// packager's shared temporary filename or archive-retention cleanup.
pub fn export_archive(
    bank: &Bank,
    output_directory: &Path,
    expected_accounts_hash: Hash,
) -> Result<ExportedSnapshot, String> {
    if !bank.is_complete() {
        return Err(format!(
            "cannot export incomplete bank at slot {}",
            bank.slot()
        ));
    }
    if !bank.is_frozen() {
        return Err(format!(
            "cannot export unfrozen bank at slot {}",
            bank.slot()
        ));
    }
    if bank.parent().is_some() {
        return Err(format!(
            "cannot export bank at slot {} before its ancestry is squashed",
            bank.slot()
        ));
    }
    if bank.get_accounts_hash() != expected_accounts_hash {
        return Err(format!(
            "bank accounts hash {} does not match expected checkpoint hash {}",
            bank.get_accounts_hash(),
            expected_accounts_hash
        ));
    }

    let output_directory = fs::canonicalize(output_directory).map_err(|error| {
        format!(
            "failed to resolve snapshot output directory {}: {}",
            output_directory.display(),
            error
        )
    })?;
    if !output_directory.is_dir() {
        return Err(format!(
            "snapshot output path is not a directory: {}",
            output_directory.display()
        ));
    }

    // Retaining these Arcs before cleaning matches the v1.0.7 packager and
    // prevents AppendVec files selected for the snapshot from being reclaimed.
    let snapshot_storages = bank.get_snapshot_storages();
    bank.clean_accounts();
    let accounts_hash = bank.update_accounts_hash();
    if accounts_hash != expected_accounts_hash {
        return Err(format!(
            "accounts hash changed after snapshot cleaning: expected {}, got {}",
            expected_accounts_hash, accounts_hash
        ));
    }
    let final_path = output_directory.join(format!(
        "snapshot-{}-{}.tar.bz2",
        bank.slot(),
        accounts_hash
    ));
    if final_path.exists() {
        return Err(format!(
            "refusing to replace existing snapshot archive {}",
            final_path.display()
        ));
    }

    let roots = bank.src.roots();
    if !roots.contains(&bank.slot()) || roots.iter().any(|slot| *slot > bank.slot()) {
        return Err(format!(
            "status cache roots do not end at frozen slot {}: {:?}",
            bank.slot(),
            roots
        ));
    }
    let slot_deltas = bank.src.slot_deltas(&roots);

    let staging = Builder::new()
        .prefix("jetstreamer-snapshot-staging-")
        .tempdir_in(&output_directory)
        .map_err(|error| {
            format!(
                "failed to create snapshot staging directory under {}: {}",
                output_directory.display(),
                error
            )
        })?;
    let snapshots_directory = staging.path().join("snapshots");
    let slot_directory = snapshots_directory.join(bank.slot().to_string());
    fs::create_dir_all(&slot_directory)
        .map_err(|error| format!("failed to create snapshot staging layout: {}", error))?;

    let bank_path = slot_directory.join(bank.slot().to_string());
    serialize_capped(&bank_path, |stream| {
        bincode::serialize_into(&mut *stream, bank)
            .map_err(|error| format!("failed to serialize snapshot Bank: {}", error))?;
        bincode::serialize_into(
            &mut *stream,
            &BankRcSerialize {
                bank_rc: &bank.rc,
                snapshot_storages: &snapshot_storages,
            },
        )
        .map_err(|error| format!("failed to serialize snapshot AccountsDB: {}", error))
    })?;

    serialize_capped(&snapshots_directory.join("status_cache"), |stream| {
        bincode::serialize_into(stream, &slot_deltas)
            .map_err(|error| format!("failed to serialize snapshot status cache: {}", error))
    })?;

    let version_path = staging.path().join("version");
    let mut version = File::create(&version_path)
        .map_err(|error| format!("failed to create {}: {}", version_path.display(), error))?;
    version
        .write_all(SNAPSHOT_VERSION.as_bytes())
        .and_then(|()| version.sync_all())
        .map_err(|error| format!("failed to write {}: {}", version_path.display(), error))?;

    let mut names = BTreeSet::new();
    let mut append_vecs = Vec::new();
    for storage in snapshot_storages.iter().flatten() {
        storage
            .flush()
            .map_err(|error| format!("failed to flush snapshot AppendVec: {}", error))?;
        let source = fs::canonicalize(storage.get_path()).map_err(|error| {
            format!(
                "failed to resolve snapshot AppendVec {}: {}",
                storage.get_path().display(),
                error
            )
        })?;
        if !source.is_file() {
            return Err(format!(
                "snapshot AppendVec is not a regular file: {}",
                source.display()
            ));
        }
        let name = source
            .file_name()
            .ok_or_else(|| format!("snapshot AppendVec has no filename: {}", source.display()))?
            .to_os_string();
        if !names.insert(name.clone()) {
            return Err(format!("duplicate snapshot AppendVec filename {:?}", name));
        }
        let (written_len, file_len) = storage.snapshot_file_layout();
        let written_len = u64::try_from(written_len)
            .map_err(|_| "snapshot AppendVec length does not fit u64".to_string())?;
        let metadata = fs::metadata(&source).map_err(|error| {
            format!(
                "failed to stat snapshot AppendVec {}: {}",
                source.display(),
                error
            )
        })?;
        if metadata.len() != file_len || written_len > file_len {
            return Err(format!(
                "snapshot AppendVec {} layout changed: written {}, runtime file {}, disk file {}",
                source.display(),
                written_len,
                file_len,
                metadata.len()
            ));
        }
        append_vecs.push((name, source, written_len, file_len));
    }

    let archive = Builder::new()
        .prefix("jetstreamer-snapshot-archive-")
        .suffix(".tar.bz2.partial")
        .tempfile_in(&output_directory)
        .map_err(|error| {
            format!(
                "failed to create temporary snapshot archive under {}: {}",
                output_directory.display(),
                error
            )
        })?;
    let archive_path = archive.path().to_path_buf();
    // Build the archive in-process so the measured worker executable binds
    // every producer implementation byte. Inheriting PATH and executing a
    // host `tar` would let an unmeasured program alter status-cache contents.
    let output = archive
        .reopen()
        .map_err(|error| format!("failed to open {}: {}", archive_path.display(), error))?;
    let writer = BufWriter::new(output);
    let encoder = BzEncoder::new(writer, Compression::Best);
    let mut tar = TarBuilder::new(encoder);
    tar.append_dir("accounts", staging.path())
        .and_then(|()| {
            for (name, source, written_len, file_len) in append_vecs {
                append_sparse_append_vec(
                    &mut tar,
                    &Path::new("accounts").join(name),
                    &source,
                    written_len,
                    file_len,
                )?;
            }
            Ok(())
        })
        .and_then(|()| tar.append_dir_all("snapshots", &snapshots_directory))
        .and_then(|()| tar.append_path_with_name(&version_path, "version"))
        .map_err(|error| format!("failed to build snapshot tar: {}", error))?;
    let encoder = tar
        .into_inner()
        .map_err(|error| format!("failed to finish snapshot tar: {}", error))?;
    let mut writer = encoder
        .finish()
        .map_err(|error| format!("failed to finish snapshot compression: {}", error))?;
    writer
        .flush()
        .map_err(|error| format!("failed to flush snapshot archive: {}", error))?;
    writer
        .get_ref()
        .sync_all()
        .map_err(|error| format!("failed to sync {}: {}", archive_path.display(), error))?;
    drop(writer);
    archive
        .as_file()
        .sync_all()
        .map_err(|error| format!("failed to sync {}: {}", archive_path.display(), error))?;
    let archive_size = archive
        .as_file()
        .metadata()
        .map_err(|error| format!("failed to stat {}: {}", archive_path.display(), error))?
        .len();
    if archive_size == 0 {
        return Err("snapshot tar produced an empty archive".to_string());
    }
    let mut persisted = archive.persist_noclobber(&final_path).map_err(|error| {
        format!(
            "failed to publish snapshot archive {} without replacing a file: {}",
            final_path.display(),
            error
        )
    })?;
    persisted
        .sync_all()
        .map_err(|error| format!("failed to sync {}: {}", final_path.display(), error))?;
    persisted
        .seek(SeekFrom::Start(0))
        .map_err(|error| format!("failed to seek {}: {}", final_path.display(), error))?;
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; 128 * 1024];
    loop {
        let read = persisted
            .read(&mut buffer)
            .map_err(|error| format!("failed to hash {}: {}", final_path.display(), error))?;
        if read == 0 {
            break;
        }
        hasher.input(&buffer[..read]);
    }
    let digest = hasher.result();
    let mut archive_sha256 = [0u8; 32];
    archive_sha256.copy_from_slice(digest.as_slice());
    let measured_size = persisted
        .metadata()
        .map_err(|error| format!("failed to stat {}: {}", final_path.display(), error))?
        .len();
    if measured_size != archive_size {
        return Err(format!(
            "snapshot archive {} changed while hashing: expected {} bytes, got {}",
            final_path.display(),
            archive_size,
            measured_size
        ));
    }
    File::open(&output_directory)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| {
            format!(
                "failed to sync snapshot output directory {}: {}",
                output_directory.display(),
                error
            )
        })?;

    Ok(ExportedSnapshot {
        archive_path: final_path,
        accounts_hash,
        archive_size,
        archive_sha256,
    })
}

/// Append an AppendVec without materializing its unwritten fixed-capacity
/// tail.  GNU sparse records describe the written prefix and a single byte at
/// the logical end of the file.  The latter preserves the original mapped file
/// length; rounding the prefix to a tar block boundary keeps multiple sparse
/// extents valid for the old tar reader used by this runtime.
fn append_sparse_append_vec<W: Write>(
    tar: &mut TarBuilder<W>,
    archive_path: &Path,
    source_path: &Path,
    written_len: u64,
    file_len: u64,
) -> io::Result<()> {
    if file_len == 0 || written_len > file_len {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "invalid AppendVec layout: written {}, file {}",
                written_len, file_len
            ),
        ));
    }
    let file = File::open(source_path)?;
    let metadata = file.metadata()?;
    if !metadata.is_file() || metadata.len() != file_len {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "AppendVec file metadata does not match the captured layout",
        ));
    }

    let prefix_len = written_len
        .checked_add(511)
        .map(|length| length / 512 * 512)
        .unwrap_or(file_len)
        .min(file_len);
    if prefix_len == file_len {
        let mut header = Header::new_gnu();
        header.set_metadata(&metadata);
        header.set_size(file_len);
        header.set_cksum();
        return tar.append_data(&mut header, archive_path, file.take(file_len));
    }

    let mut extents = Vec::with_capacity(2);
    if prefix_len != 0 {
        extents.push((0, prefix_len));
    }
    // AppendVec::new writes this byte when it creates the fixed-size mmap, so
    // reading it is both cheap and an exact representation of the source.
    extents.push((file_len - 1, 1));
    let archived_size = prefix_len + 1;

    let mut header = Header::new_gnu();
    header.set_metadata(&metadata);
    header.set_entry_type(EntryType::GNUSparse);
    header.set_size(archived_size);
    let gnu = header
        .as_gnu_mut()
        .expect("Header::new_gnu must produce a GNU header");
    write_octal(&mut gnu.realsize, file_len)?;
    gnu.isextended[0] = 0;
    for (sparse, &(offset, length)) in gnu.sparse.iter_mut().zip(&extents) {
        write_octal(&mut sparse.offset, offset)?;
        write_octal(&mut sparse.numbytes, length)?;
    }
    header.set_cksum();
    tar.append_data(
        &mut header,
        archive_path,
        FileExtentsReader::new(file, extents),
    )
}

fn write_octal(field: &mut [u8], value: u64) -> io::Result<()> {
    let encoded = format!("{:o}", value);
    if encoded.len() + 1 > field.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "value does not fit GNU tar numeric field",
        ));
    }
    for byte in field.iter_mut() {
        *byte = b'0';
    }
    let start = field.len() - encoded.len() - 1;
    field[start..start + encoded.len()].copy_from_slice(encoded.as_bytes());
    field[field.len() - 1] = 0;
    Ok(())
}

struct FileExtentsReader {
    file: File,
    extents: Vec<(u64, u64)>,
    next_extent: usize,
    remaining: u64,
}

impl FileExtentsReader {
    fn new(file: File, extents: Vec<(u64, u64)>) -> Self {
        Self {
            file,
            extents,
            next_extent: 0,
            remaining: 0,
        }
    }
}

impl Read for FileExtentsReader {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        if buffer.is_empty() {
            return Ok(0);
        }
        if self.remaining == 0 {
            let &(offset, length) = match self.extents.get(self.next_extent) {
                Some(extent) => extent,
                None => return Ok(0),
            };
            self.next_extent += 1;
            self.file.seek(SeekFrom::Start(offset))?;
            self.remaining = length;
        }
        let maximum = std::cmp::min(self.remaining, buffer.len() as u64) as usize;
        let read = self.file.read(&mut buffer[..maximum])?;
        if read == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "AppendVec changed while its snapshot was being archived",
            ));
        }
        self.remaining -= read as u64;
        Ok(read)
    }
}

fn serialize_capped<F>(path: &Path, mut serialize: F) -> Result<u64, String>
where
    F: FnMut(&mut BufWriter<File>) -> Result<(), String>,
{
    serialize_capped_with_limit(path, MAX_SNAPSHOT_DATA_FILE_SIZE, &mut serialize)
}

fn serialize_capped_with_limit<F>(
    path: &Path,
    maximum_file_size: u64,
    mut serialize: F,
) -> Result<u64, String>
where
    F: FnMut(&mut BufWriter<File>) -> Result<(), String>,
{
    let file = File::create(path)
        .map_err(|error| format!("failed to create {}: {}", path.display(), error))?;
    let mut stream = BufWriter::new(file);
    serialize(&mut stream)?;
    stream
        .flush()
        .map_err(|error| format!("failed to flush {}: {}", path.display(), error))?;
    let size = stream
        .seek(SeekFrom::Current(0))
        .map_err(|error| format!("failed to seek {}: {}", path.display(), error))?;
    if size > maximum_file_size {
        return Err(format!(
            "snapshot data file {} is {} bytes (limit {})",
            path.display(),
            size,
            maximum_file_size
        ));
    }
    stream
        .get_ref()
        .sync_all()
        .map_err(|error| format!("failed to sync {}: {}", path.display(), error))?;
    Ok(size)
}

pub fn private_state_dir(scratch_root: Option<&str>) -> Result<TempDir, String> {
    let mut builder = Builder::new();
    builder.prefix("jetstreamer-historical-v1.0.7-");
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
        // v1.0.7's 1.0.0 snapshot schema serializes Bank directly, followed
        // by BankRcSerialize. Reconstruct it exactly as that release's
        // ledger::snapshot_utils::rebuild_bank_from_snapshots did.
        let mut bank: Bank = deserialize_from_snapshot(stream.by_ref())
            .map_err(|error| format!("failed to decode v1.0.7 Bank: {}", error))?;
        bank.set_bank_rc(
            bank::BankRc::new(account_paths.clone(), 0, bank.slot()),
            bank::StatusCacheRc::default(),
        );
        bank.rc
            .accounts_from_stream(stream.by_ref(), &account_paths, &append_vecs_path)
            .map_err(|error| format!("failed to rebuild snapshot AccountsDB: {}", error))?;
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
    use std::io::Cursor;

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
    fn snapshot_data_writer_enforces_its_size_limit() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("bounded");
        let error = serialize_capped_with_limit(&path, 3, |stream| {
            stream.write_all(b"four").map_err(|error| error.to_string())
        })
        .unwrap_err();
        assert!(error.contains("is 4 bytes (limit 3)"));
    }

    #[test]
    fn sparse_append_vec_round_trips_without_archiving_the_capacity_tail() {
        let source_directory = tempfile::tempdir().unwrap();
        let source_path = source_directory.path().join("12.34");
        let mut source = File::create(&source_path).unwrap();
        source.set_len(4 * 1024 * 1024).unwrap();
        source.write_all(&vec![0x5a; 1_000]).unwrap();
        source.sync_all().unwrap();

        let mut tar = TarBuilder::new(Vec::new());
        append_sparse_append_vec(
            &mut tar,
            Path::new("accounts/12.34"),
            &source_path,
            1_000,
            4 * 1024 * 1024,
        )
        .unwrap();
        let bytes = tar.into_inner().unwrap();
        assert!(
            bytes.len() < 16 * 1024,
            "sparse tar was {} bytes",
            bytes.len()
        );

        let destination = tempfile::tempdir().unwrap();
        Archive::new(Cursor::new(bytes))
            .unpack(destination.path())
            .unwrap();
        let restored_path = destination.path().join("accounts/12.34");
        assert_eq!(fs::metadata(&restored_path).unwrap().len(), 4 * 1024 * 1024);
        let mut restored = File::open(restored_path).unwrap();
        let mut prefix = vec![0; 1_000];
        restored.read_exact(&mut prefix).unwrap();
        assert_eq!(prefix, vec![0x5a; 1_000]);
        restored.seek(SeekFrom::Start(2 * 1024 * 1024)).unwrap();
        let mut hole = [1; 1];
        restored.read_exact(&mut hole).unwrap();
        assert_eq!(hole, [0]);
        restored.seek(SeekFrom::End(-1)).unwrap();
        restored.read_exact(&mut hole).unwrap();
        assert_eq!(hole, [0]);
    }

    #[test]
    fn sparse_append_vec_rejects_inconsistent_layout() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("append-vec");
        File::create(&path).unwrap().set_len(8).unwrap();
        let mut tar = TarBuilder::new(Vec::new());
        let error =
            append_sparse_append_vec(&mut tar, Path::new("accounts/1.1"), &path, 9, 8).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
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
