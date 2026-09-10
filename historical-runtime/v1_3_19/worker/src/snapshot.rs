use bzip2::bufread::BzDecoder;
use solana_runtime::{
    bank::{Bank, BankSlotDelta, Builtins},
    serde_snapshot::{
        bank_from_stream, deserialize_from_snapshot, SerdeStyle, MAX_SNAPSHOT_DATA_FILE_SIZE,
    },
};
use solana_sdk::{genesis_config::GenesisConfig, hash::Hash};
use std::{
    collections::HashSet,
    fs::{self, File},
    io::{self, BufReader, Read, Seek, SeekFrom},
    path::{Component, Path, PathBuf},
    str::FromStr,
};
use tar::{Archive, EntryType};
use tempfile::{Builder, TempDir};

const SNAPSHOT_VERSION_1_2: &str = "1.2.0";
const MAX_HANDOFF_ARCHIVE_ENTRIES: u64 = 100_000;
const MAX_HANDOFF_ARCHIVE_MEMBER_SIZE: u64 = MAX_SNAPSHOT_DATA_FILE_SIZE;
const MAX_HANDOFF_ARCHIVE_EXTRACTED_SIZE: u64 = 8 * MAX_SNAPSHOT_DATA_FILE_SIZE;
const MAX_APPEND_VEC_FILE_SIZE: u64 = 16 * 1024 * 1024 * 1024;
const MAX_VERSION_FILE_SIZE: u64 = 64;
const TAR_BLOCK_SIZE: u64 = 512;

#[derive(Clone, Copy)]
struct SnapshotExtractionLimits {
    entries: u64,
    member_size: u64,
    extracted_size: u64,
}

const HANDOFF_EXTRACTION_LIMITS: SnapshotExtractionLimits = SnapshotExtractionLimits {
    entries: MAX_HANDOFF_ARCHIVE_ENTRIES,
    member_size: MAX_HANDOFF_ARCHIVE_MEMBER_SIZE,
    extracted_size: MAX_HANDOFF_ARCHIVE_EXTRACTED_SIZE,
};

/// Screens raw tar blocks before tar-rs sees them. tar 0.4.26's normal entry
/// iterator eagerly reads GNU long-name/PAX payloads and GNU sparse extension
/// chains before yielding an entry, so checks performed on yielded entries are
/// too late to bound those allocations and reads. This reader admits only the
/// three entry types produced by the handoff exporter and rejects multi-header
/// sparse maps before releasing their header to tar-rs.
struct HandoffTarReader<R> {
    inner: R,
    header: [u8; TAR_BLOCK_SIZE as usize],
    header_offset: usize,
    header_ready: bool,
    payload_remaining: u64,
    stream_remaining: u64,
    headers_seen: u64,
    max_headers: u64,
    max_member_size: u64,
}

impl<R: Read> HandoffTarReader<R> {
    fn new(inner: R, limits: SnapshotExtractionLimits) -> Result<Self, String> {
        // An admitted entry costs at most one header plus one partial padding
        // block beyond its logical size. Reserve another two blocks for the
        // conventional tar terminator. Sparse stored bytes are later required
        // by tar-rs to fit within their logical size.
        let framing_budget = limits
            .entries
            .checked_add(1)
            .and_then(|entries| entries.checked_mul(2 * TAR_BLOCK_SIZE))
            .ok_or_else(|| "snapshot tar framing budget overflow".to_string())?;
        let stream_remaining = limits
            .extracted_size
            .checked_add(framing_budget)
            .ok_or_else(|| "snapshot tar stream budget overflow".to_string())?;
        Ok(Self {
            inner,
            header: [0; TAR_BLOCK_SIZE as usize],
            header_offset: 0,
            header_ready: false,
            payload_remaining: 0,
            stream_remaining,
            headers_seen: 0,
            max_headers: limits.entries,
            max_member_size: limits.member_size,
        })
    }

    fn read_inner(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        if buffer.is_empty() {
            return Ok(0);
        }
        if self.stream_remaining == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "handoff tar exceeds its decompressed stream limit",
            ));
        }
        let maximum = std::cmp::min(self.stream_remaining, buffer.len() as u64) as usize;
        let read = self.inner.read(&mut buffer[..maximum])?;
        self.stream_remaining -= read as u64;
        Ok(read)
    }

    fn prepare_header(&mut self) -> io::Result<bool> {
        let mut filled = 0usize;
        while filled < self.header.len() {
            // Borrow the disjoint fields explicitly for compatibility with the
            // historical toolchain's borrow checker.
            let maximum =
                std::cmp::min(self.stream_remaining, (self.header.len() - filled) as u64) as usize;
            if maximum == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "handoff tar exceeds its decompressed stream limit",
                ));
            }
            let read = self
                .inner
                .read(&mut self.header[filled..filled + maximum])?;
            if read == 0 {
                if filled == 0 {
                    return Ok(false);
                }
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "truncated handoff tar header",
                ));
            }
            self.stream_remaining -= read as u64;
            filled += read;
        }

        self.header_offset = 0;
        self.header_ready = true;
        self.payload_remaining = 0;
        if self.header.iter().all(|byte| *byte == 0) {
            return Ok(true);
        }

        self.headers_seen = self.headers_seen.checked_add(1).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "tar entry count overflow")
        })?;
        if self.headers_seen > self.max_headers {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("snapshot tar has more than {} entries", self.max_headers),
            ));
        }

        let header = tar::Header::from_byte_slice(&self.header);
        let entry_type = header.entry_type();
        if !entry_type.is_file() && !entry_type.is_dir() && !entry_type.is_gnu_sparse() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "handoff tar contains forbidden raw entry type {:?}",
                    entry_type
                ),
            ));
        }
        if entry_type.is_gnu_sparse() {
            let gnu = header.as_gnu().ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "GNU sparse entry does not use a GNU header",
                )
            })?;
            if gnu.is_extended() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "handoff tar contains a forbidden extended GNU sparse map",
                ));
            }
        }

        let stored_size = header.entry_size()?;
        if stored_size > self.max_member_size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "handoff tar raw member is too large: stored={} limit={}",
                    stored_size, self.max_member_size
                ),
            ));
        }
        let padded_size = stored_size
            .checked_add(TAR_BLOCK_SIZE - 1)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "tar size overflow"))?
            / TAR_BLOCK_SIZE
            * TAR_BLOCK_SIZE;
        if padded_size > self.stream_remaining {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "handoff tar member exceeds its decompressed stream limit",
            ));
        }
        self.payload_remaining = padded_size;
        Ok(true)
    }
}

impl<R: Read> Read for HandoffTarReader<R> {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        if buffer.is_empty() {
            return Ok(0);
        }
        loop {
            if self.header_ready && self.header_offset < self.header.len() {
                let count = std::cmp::min(
                    buffer.len(),
                    self.header.len().saturating_sub(self.header_offset),
                );
                buffer[..count]
                    .copy_from_slice(&self.header[self.header_offset..self.header_offset + count]);
                self.header_offset += count;
                return Ok(count);
            }
            if self.header_ready {
                self.header_ready = false;
            }
            if self.payload_remaining != 0 {
                let maximum = std::cmp::min(self.payload_remaining, buffer.len() as u64) as usize;
                let read = self.read_inner(&mut buffer[..maximum])?;
                if read == 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "truncated handoff tar member",
                    ));
                }
                self.payload_remaining -= read as u64;
                return Ok(read);
            }
            if !self.prepare_header()? {
                return Ok(0);
            }
        }
    }
}

pub struct LoadedSnapshot {
    pub bank: Bank,
    pub expected_accounts_hash: Hash,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SnapshotSchema {
    V1_2,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SnapshotCompression {
    Bzip2,
    Zstd,
}

pub fn private_state_dir(scratch_root: Option<&str>) -> Result<TempDir, String> {
    let mut builder = Builder::new();
    builder.prefix("jetstreamer-historical-v1.3.19-");
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

pub fn load_archive(
    archive_path: &Path,
    state_dir: &TempDir,
    genesis: &GenesisConfig,
    additional_builtins: &Builtins,
) -> Result<LoadedSnapshot, String> {
    let (expected_slot, expected_accounts_hash, compression) =
        parse_archive_identity(archive_path)?;
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
    let archive_reader = BufReader::new(archive_file);
    let unpack_result = match compression {
        SnapshotCompression::Bzip2 => unpack_handoff_snapshot(
            BzDecoder::new(archive_reader),
            &unpacked,
            expected_slot,
            HANDOFF_EXTRACTION_LIMITS,
        ),
        SnapshotCompression::Zstd => {
            let decoder = zstd::stream::read::Decoder::new(archive_reader)
                .map_err(|error| format!("failed to open zstd stream: {}", error))?;
            unpack_handoff_snapshot(decoder, &unpacked, expected_slot, HANDOFF_EXTRACTION_LIMITS)
        }
    };
    unpack_result.map_err(|error| {
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
    let snapshot_schema = supported_snapshot_version(&version)?;

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
        // Exact v1.3.19 `bank_from_archive` schema. The upstream reconstruction
        // restores the Bank, AccountsDB, rent state, exact native processors,
        // feature activations, and the statically linked loader table in one
        // operation. Only the committed 1.2.0 schema is admitted here.
        debug_assert_eq!(snapshot_schema, SnapshotSchema::V1_2);
        let bank = bank_from_stream(
            SerdeStyle::NEWER,
            stream,
            &append_vecs_path,
            &account_paths,
            genesis,
            &[],
            None,
            Some(additional_builtins),
        )
        .map_err(|error| format!("failed to decode v1.3.19 snapshot bank: {}", error))?;
        reject_unreferenced_append_vecs(&append_vecs_path)?;
        Ok(bank)
    })?;

    let status_cache_path = snapshots_dir.join("status_cache");
    let slot_deltas: Vec<BankSlotDelta> = deserialize_exact_file(&status_cache_path, |stream| {
        deserialize_from_snapshot(stream)
            .map_err(|error| format!("failed to decode snapshot status cache: {}", error))
    })?;
    let mut previous_root = None;
    for (slot, is_root, _) in &slot_deltas {
        if !*is_root {
            return Err(format!(
                "snapshot status-cache delta at slot {} is not rooted",
                slot
            ));
        }
        if *slot > expected_slot {
            return Err(format!(
                "snapshot status-cache root {} is newer than snapshot slot {}",
                slot, expected_slot
            ));
        }
        if previous_root.map_or(false, |previous| previous >= *slot) {
            return Err(format!(
                "snapshot status-cache roots are not strictly increasing at slot {}",
                slot
            ));
        }
        previous_root = Some(*slot);
    }
    if previous_root != Some(expected_slot) {
        return Err(format!(
            "snapshot status-cache roots do not end at snapshot slot {}",
            expected_slot
        ));
    }
    bank.src.append(&slot_deltas);
    let restored_roots = bank.src.roots();
    let serialized_roots: Vec<_> = slot_deltas.iter().map(|(slot, _, _)| *slot).collect();
    if restored_roots != serialized_roots {
        return Err(format!(
            "restored status-cache roots {:?} do not match serialized roots {:?}",
            restored_roots, serialized_roots
        ));
    }

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

fn supported_snapshot_version(version: &str) -> Result<SnapshotSchema, String> {
    let version = version.trim();
    match version {
        SNAPSHOT_VERSION_1_2 => Ok(SnapshotSchema::V1_2),
        _ => Err(format!(
            "unsupported snapshot version {:?}; expected {:?}",
            version, SNAPSHOT_VERSION_1_2
        )),
    }
}

/// Extracts the narrow canonical snapshot layout into worker-private
/// storage. Every entry is admitted and charged at its logical on-disk size
/// before `unpack_in` can create it. GNU sparse files remain sparse because
/// tar's normal entry iterator parses their sparse map and `unpack_in` seeks
/// across holes instead of materializing them.
fn unpack_handoff_snapshot<R: Read>(
    source: R,
    destination: &Path,
    expected_slot: u64,
    limits: SnapshotExtractionLimits,
) -> Result<(), String> {
    let source = HandoffTarReader::new(source, limits)?;
    let mut archive = Archive::new(source);
    let entries = archive
        .entries()
        .map_err(|error| format!("failed to read snapshot tar entries: {}", error))?;
    let mut seen = HashSet::new();
    let mut entry_count = 0u64;
    let mut extracted_size = 0u64;

    for entry in entries {
        let mut entry = entry.map_err(|error| format!("invalid snapshot tar entry: {}", error))?;
        entry_count = entry_count
            .checked_add(1)
            .ok_or_else(|| "snapshot tar entry count overflow".to_string())?;
        if entry_count > limits.entries {
            return Err(format!(
                "snapshot tar has more than {} entries",
                limits.entries
            ));
        }

        let path = entry
            .path()
            .map_err(|error| format!("snapshot tar entry has an invalid path: {}", error))?
            .into_owned();
        let entry_type = entry.header().entry_type();
        let layout_limit = validate_handoff_entry_layout(&path, entry_type, expected_slot)?;
        if !seen.insert(path.clone()) {
            return Err(format!(
                "snapshot tar contains duplicate entry {}",
                path.display()
            ));
        }

        let stored_size = entry.header().entry_size().map_err(|error| {
            format!(
                "snapshot tar entry {} has an invalid stored size: {}",
                path.display(),
                error
            )
        })?;
        let logical_size = entry.header().size().map_err(|error| {
            format!(
                "snapshot tar entry {} has an invalid logical size: {}",
                path.display(),
                error
            )
        })?;
        let member_limit = std::cmp::min(limits.member_size, layout_limit);
        if stored_size > member_limit || logical_size > member_limit {
            return Err(format!(
                "snapshot tar entry {} is too large: stored={} logical={} limit={}",
                path.display(),
                stored_size,
                logical_size,
                member_limit
            ));
        }
        extracted_size = extracted_size
            .checked_add(logical_size)
            .ok_or_else(|| "snapshot tar logical extracted size overflow".to_string())?;
        if extracted_size > limits.extracted_size {
            return Err(format!(
                "snapshot tar logical extracted size {} exceeds limit {}",
                extracted_size, limits.extracted_size
            ));
        }

        let unpacked = entry.unpack_in(destination).map_err(|error| {
            format!(
                "failed to unpack snapshot tar entry {}: {}",
                path.display(),
                error
            )
        })?;
        if !unpacked {
            return Err(format!(
                "snapshot tar entry {} escaped extraction root",
                path.display()
            ));
        }
    }
    Ok(())
}

fn validate_handoff_entry_layout(
    path: &Path,
    entry_type: EntryType,
    expected_slot: u64,
) -> Result<u64, String> {
    if path.is_absolute() {
        return Err(format!(
            "snapshot tar entry path is absolute: {}",
            path.display()
        ));
    }
    let mut components = Vec::new();
    for component in path.components() {
        match component {
            Component::Normal(value) => {
                let value = value.to_str().ok_or_else(|| {
                    format!("snapshot tar entry path is not UTF-8: {}", path.display())
                })?;
                components.push(value);
            }
            Component::ParentDir => {
                return Err(format!(
                    "snapshot tar entry path contains '..': {}",
                    path.display()
                ));
            }
            Component::Prefix(_) | Component::RootDir | Component::CurDir => {
                return Err(format!(
                    "snapshot tar entry path is not a clean relative path: {}",
                    path.display()
                ));
            }
        }
    }
    if components.is_empty() {
        return Err("snapshot tar contains an empty entry path".to_string());
    }

    let slot = expected_slot.to_string();
    let is_directory = components == ["accounts"]
        || components == ["snapshots"]
        || components == ["snapshots", slot.as_str()];
    let file_limit = if components == ["version"] {
        Some(MAX_VERSION_FILE_SIZE)
    } else if components.len() == 2
        && components[0] == "accounts"
        && is_append_vec_name(components[1])
    {
        Some(MAX_APPEND_VEC_FILE_SIZE)
    } else if components == ["snapshots", "status_cache"]
        || components == ["snapshots", slot.as_str(), slot.as_str()]
    {
        Some(MAX_SNAPSHOT_DATA_FILE_SIZE)
    } else {
        None
    };

    if is_directory {
        if !entry_type.is_dir() {
            return Err(format!(
                "snapshot tar entry {} must be a directory, got {:?}",
                path.display(),
                entry_type
            ));
        }
        return Ok(0);
    }
    let file_limit = file_limit
        .ok_or_else(|| format!("snapshot tar contains unexpected path {}", path.display()))?;
    if !entry_type.is_file() && !entry_type.is_gnu_sparse() {
        return Err(format!(
            "snapshot tar entry {} has forbidden type {:?}",
            path.display(),
            entry_type
        ));
    }
    Ok(file_limit)
}

fn is_append_vec_name(name: &str) -> bool {
    let mut parts = name.split('.');
    let slot = parts.next().unwrap_or("");
    let id = parts.next().unwrap_or("");
    parts.next().is_none()
        && !slot.is_empty()
        && !id.is_empty()
        && slot.bytes().all(|byte| byte.is_ascii_digit())
        && id.bytes().all(|byte| byte.is_ascii_digit())
}

fn reject_unreferenced_append_vecs(path: &Path) -> Result<(), String> {
    let mut entries = fs::read_dir(path).map_err(|error| {
        format!(
            "failed to list snapshot accounts {}: {}",
            path.display(),
            error
        )
    })?;
    match entries.next() {
        Some(Ok(entry)) => Err(format!(
            "snapshot contains unreferenced AppendVec {}",
            entry.path().display()
        )),
        Some(Err(error)) => Err(format!(
            "failed to inspect snapshot accounts {}: {}",
            path.display(),
            error
        )),
        None => Ok(()),
    }
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

pub(crate) fn parse_archive_identity(
    path: &Path,
) -> Result<(u64, Hash, SnapshotCompression), String> {
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| "snapshot archive filename is not UTF-8".to_string())?;
    const PREFIX: &str = "snapshot-";
    let (suffix, compression) = if name.ends_with(".tar.bz2") {
        (".tar.bz2", SnapshotCompression::Bzip2)
    } else if name.ends_with(".tar.zst") {
        (".tar.zst", SnapshotCompression::Zstd)
    } else {
        return Err(format!("invalid snapshot archive filename {:?}", name));
    };
    if !name.starts_with(PREFIX) || name.len() <= PREFIX.len() + suffix.len() {
        return Err(format!("invalid snapshot archive filename {:?}", name));
    }
    let body = &name[PREFIX.len()..name.len() - suffix.len()];
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
    Ok((slot, hash, compression))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use tar::{Builder as TarBuilder, Header};

    fn test_tar(entries: &[(&str, EntryType, &[u8])]) -> Vec<u8> {
        let mut builder = TarBuilder::new(Vec::new());
        for (path, entry_type, data) in entries {
            let mut header = Header::new_gnu();
            header.set_entry_type(*entry_type);
            header.set_size(data.len() as u64);
            header.set_mode(0o644);
            header.set_cksum();
            builder
                .append_data(&mut header, path, Cursor::new(*data))
                .unwrap();
        }
        builder.into_inner().unwrap()
    }

    fn test_limits(
        entries: u64,
        member_size: u64,
        extracted_size: u64,
    ) -> SnapshotExtractionLimits {
        SnapshotExtractionLimits {
            entries,
            member_size,
            extracted_size,
        }
    }

    fn raw_header(path: &str, entry_type: EntryType, stored_size: u64) -> Header {
        let mut header = Header::new_gnu();
        header.set_path(path).unwrap();
        header.set_entry_type(entry_type);
        header.set_size(stored_size);
        header.set_mode(0o644);
        header.set_cksum();
        header
    }

    fn set_test_octal(field: &mut [u8], value: u64) {
        let encoded = format!("{:o}", value);
        assert!(encoded.len() < field.len());
        for byte in field.iter_mut() {
            *byte = b'0';
        }
        let start = field.len() - encoded.len() - 1;
        field[start..start + encoded.len()].copy_from_slice(encoded.as_bytes());
        field[field.len() - 1] = 0;
    }

    #[test]
    fn parses_historical_archive_identity() {
        let (slot, hash, compression) = parse_archive_identity(Path::new(
            "snapshot-416012-BHoMQfw7kguThAkkSeR8xGjfsr9djqwzwsZ7wJ9TdfTd.tar.bz2",
        ))
        .unwrap();
        assert_eq!(slot, 416_012);
        assert_eq!(compression, SnapshotCompression::Bzip2);
        assert_eq!(
            hash.to_string(),
            "BHoMQfw7kguThAkkSeR8xGjfsr9djqwzwsZ7wJ9TdfTd"
        );

        let (_, _, compression) = parse_archive_identity(Path::new(
            "snapshot-39743950-7zSDTwmRLETHVePvdGShVRp4227R8MtG9DAoHDmABXvp.tar.zst",
        ))
        .unwrap();
        assert_eq!(compression, SnapshotCompression::Zstd);
        assert!(parse_archive_identity(Path::new("snapshot-7-bad.tar.gz")).is_err());
    }

    #[test]
    fn accepts_only_the_upstream_v1_3_19_snapshot_schema() {
        assert_eq!(
            supported_snapshot_version("1.2.0").unwrap(),
            SnapshotSchema::V1_2
        );
        assert!(supported_snapshot_version("1.1.0\n").is_err());
        assert!(supported_snapshot_version("1.0.0").is_err());
        assert!(supported_snapshot_version("").is_err());
    }

    #[test]
    fn snapshot_decoder_rejects_oversized_string_before_reading_payload() {
        let declared_length =
            (solana_runtime::serde_snapshot::MAX_SNAPSHOT_VARIABLE_FIELD_SIZE as u64) + 1;
        let mut encoded = Vec::new();
        encoded.extend_from_slice(&declared_length.to_le_bytes());
        let mut input = Cursor::new(encoded);

        let result: bincode::Result<String> = deserialize_from_snapshot(&mut input);
        let error = result.unwrap_err();
        assert!(error.to_string().contains("size limit"), "{}", error);
        // Only the fixed-width length prefix is consumed. In particular, the
        // decoder does not reserve the attacker-declared payload first.
        assert_eq!(input.position(), 8);
    }

    #[test]
    fn snapshot_loader_rejects_unreferenced_append_vecs() {
        let accounts = tempfile::tempdir().unwrap();
        reject_unreferenced_append_vecs(accounts.path()).unwrap();

        let unreferenced = accounts.path().join("999.123");
        fs::write(&unreferenced, &b"not referenced by snapshot metadata"[..]).unwrap();
        let error = reject_unreferenced_append_vecs(accounts.path()).unwrap_err();
        assert!(error.contains("unreferenced AppendVec"), "{}", error);
        assert!(error.contains("999.123"), "{}", error);
    }

    #[test]
    fn handoff_extractor_rejects_traversal_and_absolute_paths() {
        let file = EntryType::file();
        for path in &[Path::new("../escape"), Path::new("/absolute")] {
            let error = validate_handoff_entry_layout(path, file, 7).unwrap_err();
            assert!(
                error.contains("contains '..'") || error.contains("is absolute"),
                "{}",
                error
            );
        }
    }

    #[test]
    fn handoff_extractor_rejects_unexpected_paths_and_links() {
        let unexpected =
            validate_handoff_entry_layout(Path::new("unexpected"), EntryType::file(), 7)
                .unwrap_err();
        assert!(unexpected.contains("unexpected path"));

        let link =
            validate_handoff_entry_layout(Path::new("accounts/7.1"), EntryType::symlink(), 7)
                .unwrap_err();
        assert!(link.contains("forbidden type"));
    }

    #[test]
    fn handoff_extractor_accepts_gnu_sparse_appendvecs() {
        assert_eq!(
            validate_handoff_entry_layout(Path::new("accounts/7.1"), EntryType::new(b'S'), 7,)
                .unwrap(),
            MAX_APPEND_VEC_FILE_SIZE
        );
    }

    #[test]
    fn handoff_extractor_rejects_hidden_extension_payloads_before_reading_them() {
        for entry_type in &[
            EntryType::new(b'L'),
            EntryType::new(b'K'),
            EntryType::new(b'x'),
            EntryType::new(b'g'),
        ] {
            // Only the header is present. A normal tar 0.4.26 iterator would
            // try to allocate and read this declared extension payload before
            // yielding the described member to our policy checks.
            let header = raw_header("././@Extension", *entry_type, 1 << 30);
            let destination = tempfile::tempdir().unwrap();
            let error = unpack_handoff_snapshot(
                Cursor::new(header.as_bytes().to_vec()),
                destination.path(),
                7,
                test_limits(10, 8, 8),
            )
            .unwrap_err();
            assert!(error.contains("forbidden raw entry type"), "{}", error);
            assert_eq!(fs::read_dir(destination.path()).unwrap().count(), 0);
        }
    }

    #[test]
    fn handoff_extractor_rejects_overflowing_raw_member_before_tar_sees_it() {
        let header = raw_header("version", EntryType::file(), std::u64::MAX);
        let destination = tempfile::tempdir().unwrap();
        let error = unpack_handoff_snapshot(
            Cursor::new(header.as_bytes().to_vec()),
            destination.path(),
            7,
            test_limits(10, 8, 8),
        )
        .unwrap_err();
        assert!(error.contains("raw member is too large"), "{}", error);
        assert_eq!(fs::read_dir(destination.path()).unwrap().count(), 0);
    }

    #[test]
    fn handoff_extractor_rejects_extended_sparse_map_before_reading_it() {
        let mut header = raw_header("accounts/7.1", EntryType::new(b'S'), 0);
        header.as_gnu_mut().unwrap().isextended[0] = 1;
        header.set_cksum();
        let destination = tempfile::tempdir().unwrap();
        let error = unpack_handoff_snapshot(
            Cursor::new(header.as_bytes().to_vec()),
            destination.path(),
            7,
            test_limits(10, 8, 8),
        )
        .unwrap_err();
        assert!(
            error.contains("forbidden extended GNU sparse map"),
            "{}",
            error
        );
        assert_eq!(fs::read_dir(destination.path()).unwrap().count(), 0);
    }

    #[test]
    fn handoff_extractor_round_trips_one_header_gnu_sparse_appendvec() {
        let logical_size = 4 * 1024u64;
        let mut header = raw_header("accounts/7.1", EntryType::new(b'S'), 513);
        {
            let gnu = header.as_gnu_mut().unwrap();
            set_test_octal(&mut gnu.realsize, logical_size);
            set_test_octal(&mut gnu.sparse[0].offset, 0);
            set_test_octal(&mut gnu.sparse[0].numbytes, 512);
            set_test_octal(&mut gnu.sparse[1].offset, logical_size - 1);
            set_test_octal(&mut gnu.sparse[1].numbytes, 1);
            gnu.isextended[0] = 0;
        }
        header.set_cksum();
        let mut data = vec![0x5a; 512];
        data.push(0);
        let mut builder = TarBuilder::new(Vec::new());
        builder
            .append_data(&mut header, "accounts/7.1", Cursor::new(data))
            .unwrap();
        let archive = builder.into_inner().unwrap();

        let destination = tempfile::tempdir().unwrap();
        unpack_handoff_snapshot(
            Cursor::new(archive),
            destination.path(),
            7,
            test_limits(10, 8 * 1024, 8 * 1024),
        )
        .unwrap();
        let restored_path = destination.path().join("accounts/7.1");
        assert_eq!(fs::metadata(&restored_path).unwrap().len(), logical_size);
        let mut restored = File::open(restored_path).unwrap();
        let mut prefix = vec![0; 512];
        restored.read_exact(&mut prefix).unwrap();
        assert_eq!(prefix, vec![0x5a; 512]);
        restored.seek(SeekFrom::Start(2 * 1024)).unwrap();
        let mut hole = [1];
        restored.read_exact(&mut hole).unwrap();
        assert_eq!(hole, [0]);
        restored.seek(SeekFrom::End(-1)).unwrap();
        restored.read_exact(&mut hole).unwrap();
        assert_eq!(hole, [0]);
    }

    #[test]
    fn handoff_extractor_enforces_member_and_aggregate_limits_before_writing_member() {
        let member_tar = test_tar(&[("version", EntryType::file(), b"four")]);
        let member_dest = tempfile::tempdir().unwrap();
        let member_error = unpack_handoff_snapshot(
            Cursor::new(member_tar),
            member_dest.path(),
            7,
            test_limits(10, 3, 10),
        )
        .unwrap_err();
        assert!(member_error.contains("is too large"));
        assert!(!member_dest.path().join("version").exists());

        let aggregate_tar = test_tar(&[
            ("version", EntryType::file(), b"one"),
            ("accounts/7.1", EntryType::file(), b"two"),
        ]);
        let aggregate_dest = tempfile::tempdir().unwrap();
        let aggregate_error = unpack_handoff_snapshot(
            Cursor::new(aggregate_tar),
            aggregate_dest.path(),
            7,
            test_limits(10, 10, 5),
        )
        .unwrap_err();
        assert!(aggregate_error.contains("logical extracted size"));
        assert_eq!(
            fs::read(aggregate_dest.path().join("version")).unwrap(),
            b"one"
        );
        assert!(!aggregate_dest.path().join("accounts/7.1").exists());
    }

    #[test]
    fn handoff_extractor_enforces_entry_count_before_writing_extra_member() {
        let archive = test_tar(&[
            ("version", EntryType::file(), b"1.0.0"),
            ("accounts/7.1", EntryType::file(), b"account"),
        ]);
        let destination = tempfile::tempdir().unwrap();
        let error = unpack_handoff_snapshot(
            Cursor::new(archive),
            destination.path(),
            7,
            test_limits(1, 10, 20),
        )
        .unwrap_err();
        assert!(error.contains("more than 1 entries"));
        assert!(destination.path().join("version").is_file());
        assert!(!destination.path().join("accounts/7.1").exists());
    }

    #[test]
    #[ignore]
    fn loads_and_verifies_v1_3_19_boundary_snapshot() {
        let path = std::env::var("JETSTREAMER_SNAPSHOT_39743950")
            .expect("set JETSTREAMER_SNAPSHOT_39743950 to run this integration test");
        let ledger =
            std::env::var("JETSTREAMER_MAINNET_LEDGER").expect("set JETSTREAMER_MAINNET_LEDGER");
        let state_dir = private_state_dir(None).unwrap();
        let genesis = load_genesis(Path::new(&ledger), &state_dir).unwrap();
        let builtins = crate::runtime::mainnet_additional_builtins();
        let loaded = load_archive(Path::new(&path), &state_dir, &genesis, &builtins).unwrap();
        assert_eq!(loaded.bank.slot(), 39_743_950);
        assert!(loaded.bank.verify_snapshot_bank());
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
