//! Durable JSON evidence for one single-runtime historical replay segment.
//!
//! A sidecar is bound to the exact `.jet` bytes by SHA-256 and repeats the
//! runtime/checkpoint facts needed to assemble multiple runtime segments. The
//! reader validates both the JSON and the complete Horizon source before
//! returning evidence to a caller.

use {
    jetstreamer_horizon::{
        account_updates::AccountUpdateView,
        archive::{
            ArchiveFormatError, ArchiveProvenance, ArchiveProvenanceError, ArchiveReader,
            BlockNotification, Consumption, EntryRecord, EpochMeta, FORMAT_VERSION_V2,
            RuntimeAdmission, SlotKind, SlotVisitor,
        },
        transactions::Transaction,
    },
    serde::{Deserialize, Serialize},
    sha2::{Digest, Sha256},
    solana_hash::Hash,
    std::{
        fs::{self, File},
        io::{self, BufReader, Read, Seek, SeekFrom, Write},
        ops::Range,
        path::{Path, PathBuf},
        str::FromStr,
    },
    tempfile::Builder,
    thiserror::Error,
};

pub const SEGMENT_MANIFEST_SCHEMA_VERSION: u32 = 1;
pub const SEGMENT_MANIFEST_SUFFIX: &str = ".segment.json";
const MAX_SEGMENT_MANIFEST_BYTES: u64 = 1 << 20;
const MAX_IDENTITY_TEXT_BYTES: usize = 1 << 10;

/// Evidence level under which the exact runtime span was admitted.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SegmentRuntimeAdmission {
    Verified,
    Candidate,
}

impl SegmentRuntimeAdmission {
    fn horizon(self) -> RuntimeAdmission {
        match self {
            Self::Verified => RuntimeAdmission::Verified,
            Self::Candidate => RuntimeAdmission::Candidate,
        }
    }
}

/// Exact producer identity repeated from a single-runtime V2 provenance
/// envelope. The target is sidecar-only because the older envelope did not
/// carry it; the executable digest still binds the actual worker artifact.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SegmentRuntimeIdentity {
    pub generation_profile: String,
    pub runtime_profile: String,
    pub runtime_admission: SegmentRuntimeAdmission,
    pub runtime_revision: String,
    pub runtime_toolchain: String,
    pub runtime_target: String,
    pub genesis_hash: String,
}

/// Cloneable checkpoint identity without account payloads.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SegmentCheckpointSummary {
    pub slot: u64,
    pub bank_hash: String,
    pub accounts_hash: String,
    pub last_blockhash: String,
    pub capitalization: u64,
    pub transaction_count: u64,
    pub tick_height: u64,
    pub slot_complete: bool,
    pub write_count: u64,
    pub next_write_version: u64,
}

/// Schema-versioned evidence adjacent to one complete Horizon V2 source.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct HistoricalSegmentManifest {
    pub schema_version: u32,
    pub epoch: u64,
    pub output_slot_start: u64,
    pub output_slot_count: u64,
    pub runtime: SegmentRuntimeIdentity,
    #[serde(with = "sha256_hex")]
    pub worker_executable_sha256: [u8; 32],
    #[serde(with = "sha256_hex")]
    pub archive_sha256: [u8; 32],
    /// Exact generated handoff archive loaded by this segment, when its
    /// bootstrap came from a registered cross-runtime transition.
    #[serde(default, with = "optional_sha256_hex")]
    pub bootstrap_archive_sha256: Option<[u8; 32]>,
    pub bootstrap: SegmentCheckpointSummary,
    pub terminal: SegmentCheckpointSummary,
    /// Exact validator-local write versions present in this source archive.
    pub emitted_raw_write_versions: Range<u64>,
}

impl HistoricalSegmentManifest {
    pub fn output_slot_end(&self) -> Result<u64, SegmentManifestError> {
        self.output_slot_start
            .checked_add(self.output_slot_count)
            .ok_or_else(|| invalid("output slot range overflows u64"))
    }

    /// Validates invariants that do not require opening the bound archive.
    pub fn validate(&self) -> Result<(), SegmentManifestError> {
        if self.schema_version != SEGMENT_MANIFEST_SCHEMA_VERSION {
            return Err(SegmentManifestError::UnsupportedSchemaVersion(
                self.schema_version,
            ));
        }
        if self.output_slot_count == 0 {
            return Err(invalid("output slot range must not be empty"));
        }
        if self.bootstrap_archive_sha256 == Some([0; 32]) {
            return Err(invalid(
                "bootstrap archive SHA-256 must not be all zeroes when present",
            ));
        }
        let output_end = self.output_slot_end()?;
        for (field, value) in [
            (
                "generation_profile",
                self.runtime.generation_profile.as_str(),
            ),
            ("runtime_profile", self.runtime.runtime_profile.as_str()),
            ("runtime_revision", self.runtime.runtime_revision.as_str()),
            ("runtime_toolchain", self.runtime.runtime_toolchain.as_str()),
            ("runtime_target", self.runtime.runtime_target.as_str()),
        ] {
            validate_identity_text(field, value)?;
        }
        parse_canonical_hash("runtime.genesis_hash", &self.runtime.genesis_hash)?;
        self.bootstrap.validate("bootstrap")?;
        self.terminal.validate("terminal")?;
        if self.bootstrap.slot >= self.output_slot_start {
            return Err(invalid(format!(
                "bootstrap checkpoint slot {} must precede output start {}",
                self.bootstrap.slot, self.output_slot_start
            )));
        }
        if self.terminal.slot < self.bootstrap.slot || self.terminal.slot >= output_end {
            return Err(invalid(format!(
                "terminal checkpoint slot {} is inconsistent with bootstrap slot {} and output range {}..{}",
                self.terminal.slot, self.bootstrap.slot, self.output_slot_start, output_end
            )));
        }
        if self.terminal.next_write_version < self.bootstrap.next_write_version {
            return Err(invalid(format!(
                "terminal write cursor {} precedes bootstrap cursor {}",
                self.terminal.next_write_version, self.bootstrap.next_write_version
            )));
        }

        let raw = &self.emitted_raw_write_versions;
        if raw.start > raw.end {
            return Err(invalid(format!(
                "emitted raw write-version range is inverted: {}..{}",
                raw.start, raw.end
            )));
        }
        if raw.end != self.terminal.next_write_version {
            return Err(invalid(format!(
                "terminal write cursor {} does not match emitted raw range end {}",
                self.terminal.next_write_version, raw.end
            )));
        }
        if raw.is_empty() {
            if raw.start != self.terminal.next_write_version {
                return Err(invalid(format!(
                    "empty emitted raw range must be based at terminal cursor {}",
                    self.terminal.next_write_version
                )));
            }
        } else if raw.start < self.bootstrap.next_write_version {
            return Err(invalid(format!(
                "emitted raw range starts at {} before bootstrap cursor {}",
                raw.start, self.bootstrap.next_write_version
            )));
        }
        Ok(())
    }
}

impl SegmentCheckpointSummary {
    fn validate(&self, name: &'static str) -> Result<(), SegmentManifestError> {
        if !self.slot_complete {
            return Err(invalid(format!(
                "{name} checkpoint at slot {} is incomplete",
                self.slot
            )));
        }
        parse_canonical_hash("checkpoint.bank_hash", &self.bank_hash)?;
        parse_canonical_hash("checkpoint.accounts_hash", &self.accounts_hash)?;
        parse_canonical_hash("checkpoint.last_blockhash", &self.last_blockhash)?;
        Ok(())
    }

    pub fn bank_hash_value(&self) -> Result<Hash, SegmentManifestError> {
        parse_canonical_hash("checkpoint.bank_hash", &self.bank_hash)
    }

    pub fn accounts_hash_value(&self) -> Result<Hash, SegmentManifestError> {
        parse_canonical_hash("checkpoint.accounts_hash", &self.accounts_hash)
    }

    pub fn last_blockhash_value(&self) -> Result<Hash, SegmentManifestError> {
        parse_canonical_hash("checkpoint.last_blockhash", &self.last_blockhash)
    }
}

#[derive(Debug, Error)]
pub enum SegmentManifestError {
    #[error("I/O error while {operation} {path}: {source}")]
    Io {
        operation: &'static str,
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("segment manifest is {bytes} bytes (limit {limit})")]
    ManifestTooLarge { bytes: u64, limit: u64 },
    #[error("could not encode segment manifest JSON: {0}")]
    JsonEncode(#[source] serde_json::Error),
    #[error("could not decode segment manifest JSON: {0}")]
    JsonDecode(#[source] serde_json::Error),
    #[error("unsupported segment manifest schema version {0}")]
    UnsupportedSchemaVersion(u32),
    #[error("invalid segment manifest: {0}")]
    Invalid(String),
    #[error("Horizon source archive is invalid: {0}")]
    Archive(#[source] ArchiveFormatError),
    #[error("Horizon source provenance is invalid: {0}")]
    Provenance(#[source] ArchiveProvenanceError),
    #[error("Horizon source has no runtime provenance")]
    MissingProvenance,
    #[error("Horizon source provenance V{0} is not single-runtime V2")]
    WrongProvenanceVersion(u16),
    #[error("archive SHA-256 mismatch: manifest {expected}, actual {actual}")]
    ArchiveDigestMismatch { expected: String, actual: String },
}

/// Returns `foo.jet.segment.json` for `foo.jet` without requiring a UTF-8
/// filename.
pub fn segment_manifest_path(
    archive_path: impl AsRef<Path>,
) -> Result<PathBuf, SegmentManifestError> {
    let archive_path = archive_path.as_ref();
    let file_name = archive_path.file_name().ok_or_else(|| {
        invalid(format!(
            "archive path {} has no filename",
            archive_path.display()
        ))
    })?;
    let mut sidecar_name = file_name.to_os_string();
    sidecar_name.push(SEGMENT_MANIFEST_SUFFIX);
    Ok(archive_path.with_file_name(sidecar_name))
}

/// Validates a complete source archive, binds its SHA-256, and publishes its
/// JSON sidecar with an atomic same-directory rename followed by directory
/// sync. The returned manifest contains the authoritative computed digest.
pub fn write_segment_manifest(
    archive_path: impl AsRef<Path>,
    mut manifest: HistoricalSegmentManifest,
) -> Result<(PathBuf, HistoricalSegmentManifest), SegmentManifestError> {
    let archive_path = archive_path.as_ref();
    let mut archive = open_regular_file(archive_path, "opening archive")?;
    manifest.archive_sha256 = sha256_reader(&mut archive, archive_path)?;
    manifest.validate()?;
    archive
        .seek(SeekFrom::Start(0))
        .map_err(|source| io_err("rewinding archive", archive_path, source))?;
    validate_horizon_source(archive, &manifest)?;

    let sidecar_path = segment_manifest_path(archive_path)?;
    let parent = sidecar_path.parent().unwrap_or_else(|| Path::new("."));
    let mut temp = Builder::new()
        .prefix(".jetstreamer-segment-")
        .suffix(".partial")
        .tempfile_in(parent)
        .map_err(|source| io_err("creating temporary manifest", parent, source))?;
    serde_json::to_writer_pretty(&mut temp, &manifest).map_err(SegmentManifestError::JsonEncode)?;
    temp.write_all(b"\n")
        .map_err(|source| io_err("writing temporary manifest", temp.path(), source))?;
    temp.flush()
        .map_err(|source| io_err("flushing temporary manifest", temp.path(), source))?;
    temp.as_file()
        .sync_all()
        .map_err(|source| io_err("syncing temporary manifest", temp.path(), source))?;
    let persisted = temp
        .persist(&sidecar_path)
        .map_err(|error| io_err("publishing segment manifest", &sidecar_path, error.error))?;
    persisted
        .sync_all()
        .map_err(|source| io_err("syncing segment manifest", &sidecar_path, source))?;
    sync_directory(parent)?;
    Ok((sidecar_path, manifest))
}

/// Reads the adjacent sidecar and returns it only after recomputing the
/// archive digest and fully validating the Horizon V2 source.
pub fn read_and_validate_segment_manifest(
    archive_path: impl AsRef<Path>,
) -> Result<HistoricalSegmentManifest, SegmentManifestError> {
    let archive_path = archive_path.as_ref();
    let sidecar_path = segment_manifest_path(archive_path)?;
    let bytes = read_bounded_manifest(&sidecar_path)?;
    let manifest: HistoricalSegmentManifest =
        serde_json::from_slice(&bytes).map_err(SegmentManifestError::JsonDecode)?;
    manifest.validate()?;

    let mut archive = open_regular_file(archive_path, "opening archive")?;
    let actual = sha256_reader(&mut archive, archive_path)?;
    if actual != manifest.archive_sha256 {
        return Err(SegmentManifestError::ArchiveDigestMismatch {
            expected: sha256_hex_string(&manifest.archive_sha256),
            actual: sha256_hex_string(&actual),
        });
    }
    archive
        .seek(SeekFrom::Start(0))
        .map_err(|source| io_err("rewinding archive", archive_path, source))?;
    validate_horizon_source(archive, &manifest)?;
    Ok(manifest)
}

fn validate_horizon_source(
    archive: File,
    manifest: &HistoricalSegmentManifest,
) -> Result<(), SegmentManifestError> {
    let mut reader =
        ArchiveReader::open(BufReader::new(archive)).map_err(SegmentManifestError::Archive)?;
    let header = reader.header().clone();
    if header.format_version != FORMAT_VERSION_V2 {
        return Err(invalid(format!(
            "Horizon source uses format V{}, expected V2",
            header.format_version
        )));
    }
    if header.epoch != manifest.epoch {
        return Err(invalid(format!(
            "Horizon source epoch {} does not match manifest epoch {}",
            header.epoch, manifest.epoch
        )));
    }
    if header.slot_start != manifest.output_slot_start
        || header.slot_count != manifest.output_slot_count
    {
        return Err(invalid(format!(
            "Horizon source range [{}, +{}) does not match manifest [{}, +{})",
            header.slot_start,
            header.slot_count,
            manifest.output_slot_start,
            manifest.output_slot_count
        )));
    }

    let provenance = reader
        .provenance()
        .map_err(SegmentManifestError::Provenance)?
        .ok_or(SegmentManifestError::MissingProvenance)?;
    let source = match provenance {
        ArchiveProvenance::V2(source) => source,
        other => {
            return Err(SegmentManifestError::WrongProvenanceVersion(
                other.version(),
            ));
        }
    };
    let base = &source.base;
    for (field, matches) in [
        (
            "generation_profile",
            base.generation_profile == manifest.runtime.generation_profile,
        ),
        (
            "runtime_profile",
            base.runtime_profile == manifest.runtime.runtime_profile,
        ),
        (
            "runtime_admission",
            base.runtime_admission == manifest.runtime.runtime_admission.horizon(),
        ),
        (
            "runtime_revision",
            base.runtime_revision == manifest.runtime.runtime_revision,
        ),
        (
            "runtime_toolchain",
            base.runtime_toolchain == manifest.runtime.runtime_toolchain,
        ),
        (
            "genesis_hash",
            base.genesis_hash
                == parse_canonical_hash("runtime.genesis_hash", &manifest.runtime.genesis_hash)?,
        ),
        (
            "worker_executable_sha256",
            source.worker_executable_sha256 == manifest.worker_executable_sha256,
        ),
        (
            "bootstrap_slot",
            base.bootstrap_slot == manifest.bootstrap.slot,
        ),
        (
            "bootstrap_state_hash",
            base.bootstrap_state_hash == manifest.bootstrap.accounts_hash_value()?,
        ),
    ] {
        if !matches {
            return Err(invalid(format!(
                "Horizon source provenance {field} does not match segment manifest"
            )));
        }
    }
    if manifest.bootstrap_archive_sha256.is_some()
        && base.bootstrap_state_kind
            != jetstreamer_horizon::archive::BootstrapStateKind::SnapshotArchive
    {
        return Err(invalid(
            "a bound bootstrap archive digest requires snapshot-archive provenance",
        ));
    }

    reader.verify_chain = true;
    let mut visitor = SegmentValidationVisitor::new(
        manifest.output_slot_start,
        manifest.emitted_raw_write_versions.clone(),
    );
    let mut decoded_slots = 0u64;
    for bucket in 0..reader.bucket_count() {
        let visited = reader
            .read_bucket(bucket, &mut visitor)
            .map_err(SegmentManifestError::Archive)?;
        decoded_slots = decoded_slots
            .checked_add(visited)
            .ok_or_else(|| invalid("decoded slot count overflows u64"))?;
    }
    visitor.finish()?;
    if decoded_slots != manifest.output_slot_count {
        return Err(invalid(format!(
            "Horizon source decoded {decoded_slots} slots, expected {}",
            manifest.output_slot_count
        )));
    }
    let expected_end = manifest.output_slot_end()?;
    if visitor.next_slot != expected_end {
        return Err(invalid(format!(
            "Horizon source slot coverage ended at {}, expected {expected_end}",
            visitor.next_slot
        )));
    }
    Ok(())
}

struct SegmentValidationVisitor {
    next_slot: u64,
    expected_writes: Range<u64>,
    next_write: u64,
    current_slot: Option<u64>,
    /// Wire order groups updates by owning transaction, while AccountsDB
    /// assigns write versions in physical store order.  Those orders can
    /// differ within a slot (notably when a failed transaction's fee debit is
    /// stored after later successful transactions), so validate the exact
    /// per-slot set instead of requiring callback order to be monotonic.
    slot_writes: Vec<u64>,
    error: Option<String>,
}

impl SegmentValidationVisitor {
    fn new(next_slot: u64, expected_writes: Range<u64>) -> Self {
        Self {
            next_slot,
            next_write: expected_writes.start,
            expected_writes,
            current_slot: None,
            slot_writes: Vec::new(),
            error: None,
        }
    }

    fn accept_write(&mut self, slot: u64, write_version: u64) {
        if self.error.is_some() {
            return;
        }
        if self.current_slot != Some(slot) {
            self.error = Some(format!(
                "account update callback for slot {slot} occurred while validating slot {:?}",
                self.current_slot
            ));
            return;
        }
        if write_version < self.expected_writes.start || write_version >= self.expected_writes.end {
            self.error = Some(format!(
                "account update at slot {slot} has raw write version {write_version} outside {}..{}",
                self.expected_writes.start, self.expected_writes.end
            ));
            return;
        }
        self.slot_writes.push(write_version);
    }

    fn finish_slot_writes(&mut self) {
        if self.error.is_some() || self.slot_writes.is_empty() {
            return;
        }
        let slot = self
            .current_slot
            .expect("account writes are accepted only inside a slot");
        self.slot_writes.sort_unstable();
        for index in 0..self.slot_writes.len() {
            let write_version = self.slot_writes[index];
            if write_version != self.next_write {
                self.error = Some(format!(
                    "account update at slot {slot} has raw write version {write_version}, expected {} within {}..{}",
                    self.next_write, self.expected_writes.start, self.expected_writes.end
                ));
                break;
            }
            // `write_version < expected_writes.end` was checked on receipt,
            // so incrementing cannot overflow even when the declared end is
            // `u64::MAX`.
            self.next_write = write_version + 1;
        }
        self.slot_writes.clear();
    }

    fn finish(&mut self) -> Result<(), SegmentManifestError> {
        self.finish_slot_writes();
        if let Some(error) = &self.error {
            return Err(invalid(error.clone()));
        }
        if self.next_write != self.expected_writes.end {
            return Err(invalid(format!(
                "Horizon source raw writes ended at {}, expected {}",
                self.next_write, self.expected_writes.end
            )));
        }
        Ok(())
    }
}

impl SlotVisitor for SegmentValidationVisitor {
    fn on_slot_start(&mut self, slot: u64, _kind: SlotKind) {
        self.finish_slot_writes();
        if self.error.is_some() {
            return;
        }
        if slot != self.next_slot {
            self.error = Some(format!(
                "Horizon source slot coverage expected {}, got {slot}",
                self.next_slot
            ));
            return;
        }
        self.current_slot = Some(slot);
        match self.next_slot.checked_add(1) {
            Some(next) => self.next_slot = next,
            None => self.error = Some("Horizon source slot coverage overflows u64".to_string()),
        }
    }

    fn on_epoch(&mut self, meta: &EpochMeta) {
        for (update, _) in meta.updates.iter() {
            self.accept_write(self.next_slot.saturating_sub(1), update.write_version);
        }
    }

    fn on_pre_account_update(&mut self, slot: u64, update: &AccountUpdateView<'_>) {
        self.accept_write(slot, update.write_version);
    }

    fn on_transaction(&mut self, slot: u64, _tx_index: u32, tx: &Transaction) {
        for (update, _) in tx.iter_account_updates() {
            self.accept_write(slot, update.write_version);
        }
    }

    fn on_post_account_update(&mut self, slot: u64, update: &AccountUpdateView<'_>) {
        self.accept_write(slot, update.write_version);
    }

    fn on_block(&mut self, _notification: &BlockNotification, _entries: &[EntryRecord]) {
        self.finish_slot_writes();
    }

    fn consumption(&self) -> Consumption {
        Consumption::all()
            .without_account_update_data()
            .without_block_account_update_arenas()
    }
}

fn open_regular_file(path: &Path, operation: &'static str) -> Result<File, SegmentManifestError> {
    let metadata = fs::symlink_metadata(path).map_err(|source| io_err(operation, path, source))?;
    if !metadata.file_type().is_file() {
        return Err(invalid(format!("{} is not a regular file", path.display())));
    }
    File::open(path).map_err(|source| io_err(operation, path, source))
}

fn read_bounded_manifest(path: &Path) -> Result<Vec<u8>, SegmentManifestError> {
    let mut file = open_regular_file(path, "opening segment manifest")?;
    let metadata = file
        .metadata()
        .map_err(|source| io_err("reading segment manifest metadata", path, source))?;
    if metadata.len() > MAX_SEGMENT_MANIFEST_BYTES {
        return Err(SegmentManifestError::ManifestTooLarge {
            bytes: metadata.len(),
            limit: MAX_SEGMENT_MANIFEST_BYTES,
        });
    }
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    Read::by_ref(&mut file)
        .take(MAX_SEGMENT_MANIFEST_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|source| io_err("reading segment manifest", path, source))?;
    if bytes.len() as u64 > MAX_SEGMENT_MANIFEST_BYTES {
        return Err(SegmentManifestError::ManifestTooLarge {
            bytes: bytes.len() as u64,
            limit: MAX_SEGMENT_MANIFEST_BYTES,
        });
    }
    Ok(bytes)
}

fn sha256_reader(file: &mut File, path: &Path) -> Result<[u8; 32], SegmentManifestError> {
    file.seek(SeekFrom::Start(0))
        .map_err(|source| io_err("rewinding archive for hashing", path, source))?;
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; 128 * 1024];
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|source| io_err("hashing archive", path, source))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hasher.finalize().into())
}

fn sync_directory(path: &Path) -> Result<(), SegmentManifestError> {
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(|source| io_err("syncing manifest directory", path, source))
}

fn validate_identity_text(field: &'static str, value: &str) -> Result<(), SegmentManifestError> {
    if value.is_empty() {
        return Err(invalid(format!("runtime identity field {field} is empty")));
    }
    if value.len() > MAX_IDENTITY_TEXT_BYTES {
        return Err(invalid(format!(
            "runtime identity field {field} is {} bytes (limit {MAX_IDENTITY_TEXT_BYTES})",
            value.len()
        )));
    }
    Ok(())
}

fn parse_canonical_hash(field: &'static str, value: &str) -> Result<Hash, SegmentManifestError> {
    let hash = Hash::from_str(value)
        .map_err(|error| invalid(format!("{field} is not a valid Solana hash: {error}")))?;
    if hash.to_string() != value {
        return Err(invalid(format!("{field} is not canonical base58")));
    }
    Ok(hash)
}

fn io_err(operation: &'static str, path: &Path, source: io::Error) -> SegmentManifestError {
    SegmentManifestError::Io {
        operation,
        path: path.to_path_buf(),
        source,
    }
}

fn invalid(message: impl Into<String>) -> SegmentManifestError {
    SegmentManifestError::Invalid(message.into())
}

pub fn sha256_hex_string(digest: &[u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(64);
    for byte in digest {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

mod sha256_hex {
    use serde::{Deserialize, Deserializer, Serializer, de::Error as _};

    pub fn serialize<S>(digest: &[u8; 32], serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&super::sha256_hex_string(digest))
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<[u8; 32], D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        if value.len() != 64 {
            return Err(D::Error::custom(
                "SHA-256 must contain 64 lowercase hex digits",
            ));
        }
        let mut digest = [0u8; 32];
        for (index, digest_byte) in digest.iter_mut().enumerate() {
            let pair = &value.as_bytes()[index * 2..index * 2 + 2];
            let high = nibble(pair[0]).ok_or_else(|| {
                D::Error::custom("SHA-256 must contain only lowercase hex digits")
            })?;
            let low = nibble(pair[1]).ok_or_else(|| {
                D::Error::custom("SHA-256 must contain only lowercase hex digits")
            })?;
            *digest_byte = (high << 4) | low;
        }
        Ok(digest)
    }

    fn nibble(byte: u8) -> Option<u8> {
        match byte {
            b'0'..=b'9' => Some(byte - b'0'),
            b'a'..=b'f' => Some(byte - b'a' + 10),
            _ => None,
        }
    }
}

mod optional_sha256_hex {
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(digest: &Option<[u8; 32]>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match digest {
            Some(digest) => serializer.serialize_some(&super::sha256_hex_string(digest)),
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<[u8; 32]>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = Option::<String>::deserialize(deserializer)?;
        value
            .map(|value| {
                use serde::de::Error as _;

                if value.len() != 64 {
                    return Err(D::Error::custom(
                        "SHA-256 must contain 64 lowercase hex digits",
                    ));
                }
                let mut digest = [0u8; 32];
                for (index, digest_byte) in digest.iter_mut().enumerate() {
                    let pair = &value.as_bytes()[index * 2..index * 2 + 2];
                    let nibble = |byte| match byte {
                        b'0'..=b'9' => Some(byte - b'0'),
                        b'a'..=b'f' => Some(byte - b'a' + 10),
                        _ => None,
                    };
                    let high = nibble(pair[0]).ok_or_else(|| {
                        D::Error::custom("SHA-256 must contain only lowercase hex digits")
                    })?;
                    let low = nibble(pair[1]).ok_or_else(|| {
                        D::Error::custom("SHA-256 must contain only lowercase hex digits")
                    })?;
                    *digest_byte = (high << 4) | low;
                }
                Ok(digest)
            })
            .transpose()
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        jetstreamer_horizon::archive::{
            ArchiveProvenance, ArchiveProvenanceV1, ArchiveProvenanceV2, ArchiveWriter,
            ArchiveWriterConfig, BootstrapStateKind, TransactionMetadataPolicy,
        },
        std::fs::OpenOptions,
        tempfile::TempDir,
    };

    const EPOCH: u64 = 1;
    const SLOT_START: u64 = 432_000;
    const SLOT_COUNT: u64 = 2;
    const BOOTSTRAP_SLOT: u64 = 416_012;
    const WORKER_DIGEST: [u8; 32] = [0x42; 32];

    fn validate_write_versions(
        expected: Range<u64>,
        versions: &[u64],
    ) -> Result<(), SegmentManifestError> {
        let mut visitor = SegmentValidationVisitor::new(SLOT_START, expected);
        visitor.on_slot_start(SLOT_START, SlotKind::Block);
        for &version in versions {
            visitor.accept_write(SLOT_START, version);
        }
        visitor.finish()
    }

    fn hash(byte: u8) -> Hash {
        Hash::new_from_array([byte; 32])
    }

    fn runtime_identity() -> SegmentRuntimeIdentity {
        SegmentRuntimeIdentity {
            generation_profile: "jetstreamer-node/historical-replay-v1".into(),
            runtime_profile: "solana-v1.0.24".into(),
            runtime_admission: SegmentRuntimeAdmission::Candidate,
            runtime_revision: "a93915f1bddb73480f86fc09f487315ae191897d".into(),
            runtime_toolchain: "rustc-1.43.0-x86_64-unknown-linux-gnu".into(),
            runtime_target: "x86_64-unknown-linux-gnu".into(),
            genesis_hash: hash(5).to_string(),
        }
    }

    fn provenance() -> ArchiveProvenance {
        let runtime = runtime_identity();
        ArchiveProvenanceV2 {
            base: ArchiveProvenanceV1 {
                generation_profile: runtime.generation_profile,
                runtime_profile: runtime.runtime_profile,
                runtime_admission: RuntimeAdmission::Candidate,
                runtime_revision: runtime.runtime_revision,
                runtime_toolchain: runtime.runtime_toolchain,
                genesis_hash: hash(5),
                bootstrap_state_kind: BootstrapStateKind::SnapshotArchive,
                bootstrap_slot: BOOTSTRAP_SLOT,
                bootstrap_state_hash: hash(9),
                requested_slot_start: SLOT_START,
                requested_slot_count: SLOT_COUNT,
                transaction_metadata: TransactionMetadataPolicy::observed(),
            },
            worker_executable_sha256: WORKER_DIGEST,
        }
        .into()
    }

    fn manifest() -> HistoricalSegmentManifest {
        HistoricalSegmentManifest {
            schema_version: SEGMENT_MANIFEST_SCHEMA_VERSION,
            epoch: EPOCH,
            output_slot_start: SLOT_START,
            output_slot_count: SLOT_COUNT,
            runtime: runtime_identity(),
            worker_executable_sha256: WORKER_DIGEST,
            // The writer replaces this placeholder with the measured digest.
            archive_sha256: [0; 32],
            bootstrap_archive_sha256: None,
            bootstrap: SegmentCheckpointSummary {
                slot: BOOTSTRAP_SLOT,
                bank_hash: hash(8).to_string(),
                accounts_hash: hash(9).to_string(),
                last_blockhash: hash(10).to_string(),
                capitalization: 1_000,
                transaction_count: 50,
                tick_height: 12_000,
                slot_complete: true,
                write_count: 0,
                next_write_version: 100,
            },
            terminal: SegmentCheckpointSummary {
                slot: SLOT_START + SLOT_COUNT - 1,
                bank_hash: hash(11).to_string(),
                accounts_hash: hash(12).to_string(),
                last_blockhash: hash(13).to_string(),
                capitalization: 1_100,
                transaction_count: 55,
                tick_height: 12_128,
                slot_complete: true,
                write_count: 0,
                next_write_version: 100,
            },
            emitted_raw_write_versions: 100..100,
        }
    }

    fn write_archive(directory: &TempDir) -> PathBuf {
        let mut writer = ArchiveWriter::new_with_provenance(
            Vec::new(),
            EPOCH,
            SLOT_START,
            SLOT_COUNT,
            ArchiveWriterConfig::default(),
            &provenance(),
        )
        .unwrap();
        for slot in SLOT_START..SLOT_START + SLOT_COUNT {
            writer.write_skipped_slot(slot).unwrap();
        }
        let (bytes, _) = writer.finish().unwrap();
        let path = directory.path().join("epoch-1.jet");
        fs::write(&path, bytes).unwrap();
        path
    }

    #[test]
    fn sidecar_round_trip_binds_and_fully_validates_archive() {
        let directory = TempDir::new().unwrap();
        let archive = write_archive(&directory);

        let (sidecar, written) = write_segment_manifest(&archive, manifest()).unwrap();
        assert_eq!(sidecar, directory.path().join("epoch-1.jet.segment.json"));
        assert_ne!(written.archive_sha256, [0; 32]);
        assert_eq!(
            read_and_validate_segment_manifest(&archive).unwrap(),
            written
        );

        let json = fs::read_to_string(sidecar).unwrap();
        assert!(json.contains(&format!("\"{}\"", sha256_hex_string(&WORKER_DIGEST))));
        assert!(json.ends_with('\n'));
    }

    #[test]
    fn changed_archive_is_rejected_before_it_can_supply_evidence() {
        let directory = TempDir::new().unwrap();
        let archive = write_archive(&directory);
        write_segment_manifest(&archive, manifest()).unwrap();
        OpenOptions::new()
            .append(true)
            .open(&archive)
            .unwrap()
            .write_all(b"changed")
            .unwrap();

        assert!(matches!(
            read_and_validate_segment_manifest(&archive),
            Err(SegmentManifestError::ArchiveDigestMismatch { .. })
        ));
    }

    #[test]
    fn provenance_identity_mismatch_is_rejected_without_publishing_sidecar() {
        let directory = TempDir::new().unwrap();
        let archive = write_archive(&directory);
        let mut evidence = manifest();
        evidence.runtime.runtime_revision = "different-revision".into();

        let error = write_segment_manifest(&archive, evidence).unwrap_err();
        assert!(matches!(error, SegmentManifestError::Invalid(_)));
        assert!(!segment_manifest_path(&archive).unwrap().exists());
    }

    #[test]
    fn manifest_rejects_inconsistent_terminal_write_cursor() {
        let mut evidence = manifest();
        evidence.terminal.next_write_version += 1;
        assert!(matches!(
            evidence.validate(),
            Err(SegmentManifestError::Invalid(message))
                if message.contains("does not match emitted raw range end")
        ));
    }

    #[test]
    fn segment_write_validation_accepts_a_complete_per_slot_permutation() {
        validate_write_versions(100..104, &[100, 103, 101, 102]).unwrap();
    }

    #[test]
    fn segment_write_validation_rejects_duplicates_omissions_and_out_of_range_values() {
        for (versions, expected) in [
            (&[100, 100, 102, 103][..], "expected 101"),
            (&[100, 102, 103][..], "expected 101"),
            (&[100, 101, 102][..], "ended at 103"),
            (&[100, 101, 102, 104][..], "outside 100..104"),
        ] {
            let error = validate_write_versions(100..104, versions).unwrap_err();
            assert!(error.to_string().contains(expected), "{error}");
        }
    }

    #[test]
    fn segment_write_permutations_cannot_cross_slot_boundaries() {
        let mut visitor = SegmentValidationVisitor::new(SLOT_START, 100..103);
        visitor.on_slot_start(SLOT_START, SlotKind::Block);
        visitor.accept_write(SLOT_START, 100);
        visitor.accept_write(SLOT_START, 102);
        visitor.on_slot_start(SLOT_START + 1, SlotKind::Block);
        visitor.accept_write(SLOT_START + 1, 101);
        assert!(visitor.finish().is_err());
    }

    #[test]
    fn segment_write_validation_handles_u64_end_without_range_sized_allocation() {
        let mut visitor = SegmentValidationVisitor::new(SLOT_START, 0..u64::MAX);
        // Memory is proportional to writes observed in the current slot, not
        // the attacker-controlled numeric span declared by the sidecar.
        assert_eq!(visitor.slot_writes.capacity(), 0);

        visitor = SegmentValidationVisitor::new(SLOT_START, (u64::MAX - 2)..u64::MAX);
        visitor.on_slot_start(SLOT_START, SlotKind::Block);
        visitor.accept_write(SLOT_START, u64::MAX - 1);
        visitor.accept_write(SLOT_START, u64::MAX - 2);
        visitor.finish().unwrap();
    }

    #[test]
    fn sha256_json_is_fixed_width_lowercase_hex() {
        let mut value = serde_json::to_value(manifest()).unwrap();
        assert_eq!(
            value["worker_executable_sha256"],
            sha256_hex_string(&WORKER_DIGEST)
        );
        value["worker_executable_sha256"] = serde_json::Value::String("AA".repeat(32));
        assert!(serde_json::from_value::<HistoricalSegmentManifest>(value).is_err());
    }
}
