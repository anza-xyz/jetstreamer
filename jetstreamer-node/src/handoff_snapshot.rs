//! Durable evidence for a snapshot generated at a historical-runtime handoff.
//!
//! A legacy snapshot filename commits to its slot and accounts hash, but not
//! to every byte that affects replay. In particular, the serialized status
//! cache is outside the bank/accounts-hash commitment. This sidecar binds the
//! complete archive to the runtime, worker executable, and terminal checkpoint
//! that produced it.

use {
    crate::segment_manifest::{
        SegmentCheckpointSummary, SegmentRuntimeIdentity, sha256_hex_string,
    },
    serde::{Deserialize, Serialize},
    sha2::{Digest, Sha256},
    solana_hash::Hash,
    std::{
        fs::{self, File},
        io::{self, Read, Write},
        path::{Path, PathBuf},
        str::FromStr,
    },
    tempfile::Builder,
    thiserror::Error,
};

pub const HANDOFF_SNAPSHOT_MANIFEST_SCHEMA_VERSION: u32 = 1;
pub const HANDOFF_SNAPSHOT_MANIFEST_SUFFIX: &str = ".handoff.json";
const MAX_HANDOFF_SNAPSHOT_MANIFEST_BYTES: u64 = 1 << 20;
const MAX_IDENTITY_TEXT_BYTES: usize = 1 << 10;

/// Evidence adjacent to one generated cross-runtime snapshot archive.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct HistoricalHandoffSnapshotManifest {
    pub schema_version: u32,
    /// First slot executed by the successor runtime.
    pub boundary_slot: u64,
    /// Complete predecessor slot represented by the snapshot.
    pub snapshot_slot: u64,
    /// Canonical legacy accounts hash encoded in the snapshot filename.
    pub accounts_hash: String,
    /// Canonical absolute path at which the parent published the archive.
    pub archive_path: String,
    pub archive_size: u64,
    #[serde(with = "sha256_hex")]
    pub archive_sha256: [u8; 32],
    /// Exact source-runtime identity selected by the slot registry.
    pub source_runtime: SegmentRuntimeIdentity,
    #[serde(with = "sha256_hex")]
    pub source_worker_executable_sha256: [u8; 32],
    /// Frozen source checkpoint from which the snapshot was exported.
    pub terminal: SegmentCheckpointSummary,
}

impl HistoricalHandoffSnapshotManifest {
    /// Checks every invariant represented by the sidecar itself.
    pub fn validate(&self) -> Result<(), HandoffSnapshotManifestError> {
        if self.schema_version != HANDOFF_SNAPSHOT_MANIFEST_SCHEMA_VERSION {
            return Err(HandoffSnapshotManifestError::UnsupportedSchemaVersion(
                self.schema_version,
            ));
        }
        if self.snapshot_slot.checked_add(1) != Some(self.boundary_slot) {
            return Err(invalid(format!(
                "snapshot slot {} must immediately precede boundary {}",
                self.snapshot_slot, self.boundary_slot
            )));
        }
        if self.archive_size == 0 {
            return Err(invalid("snapshot archive must not be empty"));
        }
        if self.archive_sha256 == [0; 32] {
            return Err(invalid("snapshot archive SHA-256 must not be all zeroes"));
        }
        if self.source_worker_executable_sha256 == [0; 32] {
            return Err(invalid("source worker SHA-256 must not be all zeroes"));
        }
        parse_canonical_hash("accounts_hash", &self.accounts_hash)?;
        if self.archive_path.is_empty() {
            return Err(invalid("canonical snapshot archive path is empty"));
        }
        for (field, value) in [
            (
                "generation_profile",
                self.source_runtime.generation_profile.as_str(),
            ),
            (
                "runtime_profile",
                self.source_runtime.runtime_profile.as_str(),
            ),
            (
                "runtime_revision",
                self.source_runtime.runtime_revision.as_str(),
            ),
            (
                "runtime_toolchain",
                self.source_runtime.runtime_toolchain.as_str(),
            ),
            (
                "runtime_target",
                self.source_runtime.runtime_target.as_str(),
            ),
            ("genesis_hash", self.source_runtime.genesis_hash.as_str()),
        ] {
            validate_identity_text(field, value)?;
        }
        parse_canonical_hash(
            "source_runtime.genesis_hash",
            &self.source_runtime.genesis_hash,
        )?;
        if !self.terminal.slot_complete {
            return Err(invalid("terminal checkpoint is incomplete"));
        }
        if self.terminal.slot != self.snapshot_slot {
            return Err(invalid(format!(
                "terminal checkpoint slot {} does not match snapshot slot {}",
                self.terminal.slot, self.snapshot_slot
            )));
        }
        parse_canonical_hash("terminal.bank_hash", &self.terminal.bank_hash)?;
        let terminal_accounts =
            parse_canonical_hash("terminal.accounts_hash", &self.terminal.accounts_hash)?;
        if terminal_accounts != parse_canonical_hash("accounts_hash", &self.accounts_hash)? {
            return Err(invalid(
                "terminal checkpoint accounts hash does not match snapshot identity",
            ));
        }
        parse_canonical_hash("terminal.last_blockhash", &self.terminal.last_blockhash)?;
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum HandoffSnapshotManifestError {
    #[error("I/O error while {operation} {path}: {source}")]
    Io {
        operation: &'static str,
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("handoff snapshot manifest is {bytes} bytes (limit {limit})")]
    ManifestTooLarge { bytes: u64, limit: u64 },
    #[error("could not encode handoff snapshot manifest JSON: {0}")]
    JsonEncode(#[source] serde_json::Error),
    #[error("could not decode handoff snapshot manifest JSON: {0}")]
    JsonDecode(#[source] serde_json::Error),
    #[error("unsupported handoff snapshot manifest schema version {0}")]
    UnsupportedSchemaVersion(u32),
    #[error("invalid handoff snapshot manifest: {0}")]
    Invalid(String),
    #[error("snapshot archive size mismatch: manifest {expected}, actual {actual}")]
    ArchiveSizeMismatch { expected: u64, actual: u64 },
    #[error("snapshot archive SHA-256 mismatch: manifest {expected}, actual {actual}")]
    ArchiveDigestMismatch { expected: String, actual: String },
    #[error("path no longer names the opened regular file: {0}")]
    PathIdentityChanged(PathBuf),
}

/// Returns `snapshot-....tar.bz2.handoff.json` for an archive path.
pub fn handoff_snapshot_manifest_path(
    archive_path: impl AsRef<Path>,
) -> Result<PathBuf, HandoffSnapshotManifestError> {
    let archive_path = archive_path.as_ref();
    let file_name = archive_path.file_name().ok_or_else(|| {
        invalid(format!(
            "snapshot archive path {} has no filename",
            archive_path.display()
        ))
    })?;
    let mut sidecar_name = file_name.to_os_string();
    sidecar_name.push(HANDOFF_SNAPSHOT_MANIFEST_SUFFIX);
    Ok(archive_path.with_file_name(sidecar_name))
}

/// Measures the exact archive and durably publishes its evidence sidecar.
///
/// Publication is no-clobber. Callers must quarantine any pre-existing
/// archive/sidecar pair before requesting a fresh worker export.
pub fn write_handoff_snapshot_manifest(
    archive_path: impl AsRef<Path>,
    mut manifest: HistoricalHandoffSnapshotManifest,
) -> Result<(PathBuf, HistoricalHandoffSnapshotManifest), HandoffSnapshotManifestError> {
    let archive_path = archive_path.as_ref();
    validate_archive_filename(archive_path, &manifest)?;
    let (mut archive, archive_metadata) =
        open_regular_file(archive_path, "opening snapshot archive")?;
    manifest.archive_path = canonical_path_string(archive_path)?;
    ensure_path_names_open_file(archive_path, &archive_metadata)?;
    manifest.archive_size = archive_metadata.len();
    manifest.archive_sha256 = sha256_reader(&mut archive, archive_path, manifest.archive_size)?;
    ensure_path_names_open_file(archive_path, &archive_metadata)?;
    manifest.validate()?;

    let sidecar_path = handoff_snapshot_manifest_path(archive_path)?;
    let parent = sidecar_path.parent().unwrap_or_else(|| Path::new("."));
    let mut temp = Builder::new()
        .prefix(".jetstreamer-handoff-")
        .suffix(".partial")
        .tempfile_in(parent)
        .map_err(|source| io_err("creating temporary handoff manifest", parent, source))?;
    serde_json::to_writer_pretty(&mut temp, &manifest)
        .map_err(HandoffSnapshotManifestError::JsonEncode)?;
    temp.write_all(b"\n")
        .map_err(|source| io_err("writing temporary handoff manifest", temp.path(), source))?;
    temp.flush()
        .map_err(|source| io_err("flushing temporary handoff manifest", temp.path(), source))?;
    temp.as_file()
        .sync_all()
        .map_err(|source| io_err("syncing temporary handoff manifest", temp.path(), source))?;
    let persisted = temp.persist_noclobber(&sidecar_path).map_err(|error| {
        io_err(
            "publishing handoff snapshot manifest",
            &sidecar_path,
            error.error,
        )
    })?;
    persisted
        .sync_all()
        .map_err(|source| io_err("syncing handoff snapshot manifest", &sidecar_path, source))?;
    sync_directory(parent)?;
    Ok((sidecar_path, manifest))
}

/// Reads a sidecar and returns it only after remeasuring the complete archive.
pub fn read_and_validate_handoff_snapshot_manifest(
    archive_path: impl AsRef<Path>,
) -> Result<HistoricalHandoffSnapshotManifest, HandoffSnapshotManifestError> {
    let archive_path = archive_path.as_ref();
    let sidecar_path = handoff_snapshot_manifest_path(archive_path)?;
    let bytes = read_bounded_manifest(&sidecar_path)?;
    let manifest: HistoricalHandoffSnapshotManifest =
        serde_json::from_slice(&bytes).map_err(HandoffSnapshotManifestError::JsonDecode)?;
    manifest.validate()?;
    validate_archive_filename(archive_path, &manifest)?;
    let actual_path = canonical_path_string(archive_path)?;
    if actual_path != manifest.archive_path {
        return Err(invalid(format!(
            "snapshot archive canonical path {actual_path:?} does not match manifest {:?}",
            manifest.archive_path
        )));
    }

    let (mut archive, archive_metadata) =
        open_regular_file(archive_path, "opening snapshot archive")?;
    let actual_size = archive_metadata.len();
    if actual_size != manifest.archive_size {
        return Err(HandoffSnapshotManifestError::ArchiveSizeMismatch {
            expected: manifest.archive_size,
            actual: actual_size,
        });
    }
    let actual = sha256_reader(&mut archive, archive_path, actual_size)?;
    ensure_path_names_open_file(archive_path, &archive_metadata)?;
    if actual != manifest.archive_sha256 {
        return Err(HandoffSnapshotManifestError::ArchiveDigestMismatch {
            expected: sha256_hex_string(&manifest.archive_sha256),
            actual: sha256_hex_string(&actual),
        });
    }
    Ok(manifest)
}

fn validate_archive_filename(
    path: &Path,
    manifest: &HistoricalHandoffSnapshotManifest,
) -> Result<(), HandoffSnapshotManifestError> {
    let expected = format!(
        "snapshot-{}-{}.tar.bz2",
        manifest.snapshot_slot, manifest.accounts_hash
    );
    if path.file_name().and_then(|name| name.to_str()) != Some(expected.as_str()) {
        return Err(invalid(format!(
            "snapshot archive {} does not have manifest identity {expected}",
            path.display()
        )));
    }
    Ok(())
}

fn canonical_path_string(path: &Path) -> Result<String, HandoffSnapshotManifestError> {
    let canonical = fs::canonicalize(path)
        .map_err(|source| io_err("canonicalizing snapshot archive", path, source))?;
    canonical.to_str().map(str::to_owned).ok_or_else(|| {
        invalid(format!(
            "snapshot archive path is not UTF-8: {}",
            path.display()
        ))
    })
}

fn open_regular_file(
    path: &Path,
    operation: &'static str,
) -> Result<(File, fs::Metadata), HandoffSnapshotManifestError> {
    let mut options = fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;

        options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    let file = options
        .open(path)
        .map_err(|source| io_err(operation, path, source))?;
    let metadata = file
        .metadata()
        .map_err(|source| io_err("reading opened-file metadata", path, source))?;
    if !metadata.is_file() {
        return Err(invalid(format!("{} is not a regular file", path.display())));
    }
    ensure_path_names_open_file(path, &metadata)?;
    Ok((file, metadata))
}

fn read_bounded_manifest(path: &Path) -> Result<Vec<u8>, HandoffSnapshotManifestError> {
    let (mut file, metadata) = open_regular_file(path, "opening handoff snapshot manifest")?;
    let bytes = metadata.len();
    if bytes > MAX_HANDOFF_SNAPSHOT_MANIFEST_BYTES {
        return Err(HandoffSnapshotManifestError::ManifestTooLarge {
            bytes,
            limit: MAX_HANDOFF_SNAPSHOT_MANIFEST_BYTES,
        });
    }
    let mut output = Vec::with_capacity(bytes as usize);
    Read::by_ref(&mut file)
        .take(MAX_HANDOFF_SNAPSHOT_MANIFEST_BYTES + 1)
        .read_to_end(&mut output)
        .map_err(|source| io_err("reading handoff snapshot manifest", path, source))?;
    if output.len() as u64 > MAX_HANDOFF_SNAPSHOT_MANIFEST_BYTES {
        return Err(HandoffSnapshotManifestError::ManifestTooLarge {
            bytes: output.len() as u64,
            limit: MAX_HANDOFF_SNAPSHOT_MANIFEST_BYTES,
        });
    }
    if output.len() as u64 != bytes {
        return Err(invalid(format!(
            "handoff snapshot manifest {} changed size while being read: expected {bytes}, got {}",
            path.display(),
            output.len()
        )));
    }
    ensure_path_names_open_file(path, &metadata)?;
    Ok(output)
}

fn sha256_reader(
    file: &mut File,
    path: &Path,
    expected_size: u64,
) -> Result<[u8; 32], HandoffSnapshotManifestError> {
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; 128 * 1024];
    let mut remaining = expected_size;
    while remaining != 0 {
        let maximum = std::cmp::min(remaining, buffer.len() as u64) as usize;
        let read = file
            .read(&mut buffer[..maximum])
            .map_err(|source| io_err("hashing snapshot archive", path, source))?;
        if read == 0 {
            return Err(HandoffSnapshotManifestError::ArchiveSizeMismatch {
                expected: expected_size,
                actual: expected_size - remaining,
            });
        }
        hasher.update(&buffer[..read]);
        remaining -= read as u64;
    }
    let mut extra = [0u8; 1];
    if file
        .read(&mut extra)
        .map_err(|source| io_err("hashing snapshot archive", path, source))?
        != 0
    {
        let actual = file
            .metadata()
            .map(|metadata| metadata.len())
            .unwrap_or(expected_size.saturating_add(1))
            .max(expected_size.saturating_add(1));
        return Err(HandoffSnapshotManifestError::ArchiveSizeMismatch {
            expected: expected_size,
            actual,
        });
    }
    Ok(hasher.finalize().into())
}

fn ensure_path_names_open_file(
    path: &Path,
    opened: &fs::Metadata,
) -> Result<(), HandoffSnapshotManifestError> {
    let current = fs::symlink_metadata(path)
        .map_err(|source| io_err("checking opened-file identity", path, source))?;
    if !current.file_type().is_file() || !same_file_identity(opened, &current) {
        return Err(HandoffSnapshotManifestError::PathIdentityChanged(
            path.to_path_buf(),
        ));
    }
    Ok(())
}

#[cfg(unix)]
fn same_file_identity(left: &fs::Metadata, right: &fs::Metadata) -> bool {
    use std::os::unix::fs::MetadataExt as _;

    left.dev() == right.dev() && left.ino() == right.ino()
}

#[cfg(not(unix))]
fn same_file_identity(left: &fs::Metadata, right: &fs::Metadata) -> bool {
    left.len() == right.len()
}

fn sync_directory(path: &Path) -> Result<(), HandoffSnapshotManifestError> {
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(|source| io_err("syncing handoff manifest directory", path, source))
}

fn validate_identity_text(
    field: &'static str,
    value: &str,
) -> Result<(), HandoffSnapshotManifestError> {
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

fn parse_canonical_hash(
    field: &'static str,
    value: &str,
) -> Result<Hash, HandoffSnapshotManifestError> {
    let hash = Hash::from_str(value)
        .map_err(|error| invalid(format!("{field} is not a valid Solana hash: {error}")))?;
    if hash.to_string() != value {
        return Err(invalid(format!("{field} is not canonical base58")));
    }
    Ok(hash)
}

fn io_err(operation: &'static str, path: &Path, source: io::Error) -> HandoffSnapshotManifestError {
    HandoffSnapshotManifestError::Io {
        operation,
        path: path.to_path_buf(),
        source,
    }
}

fn invalid(message: impl Into<String>) -> HandoffSnapshotManifestError {
    HandoffSnapshotManifestError::Invalid(message.into())
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

#[cfg(test)]
mod tests {
    use {
        super::*, crate::segment_manifest::SegmentRuntimeAdmission, std::fs::OpenOptions,
        tempfile::TempDir,
    };

    fn hash(byte: u8) -> String {
        Hash::new_from_array([byte; 32]).to_string()
    }

    fn manifest() -> HistoricalHandoffSnapshotManifest {
        HistoricalHandoffSnapshotManifest {
            schema_version: HANDOFF_SNAPSHOT_MANIFEST_SCHEMA_VERSION,
            boundary_slot: 620,
            snapshot_slot: 619,
            accounts_hash: hash(2),
            archive_path: String::new(),
            archive_size: 0,
            archive_sha256: [1; 32],
            source_runtime: SegmentRuntimeIdentity {
                generation_profile: "jetstreamer-node/test".into(),
                runtime_profile: "solana-v1.0.7".into(),
                runtime_admission: SegmentRuntimeAdmission::Candidate,
                runtime_revision: "revision".into(),
                runtime_toolchain: "rustc test; target=test".into(),
                runtime_target: "test".into(),
                genesis_hash: hash(3),
            },
            source_worker_executable_sha256: [4; 32],
            terminal: SegmentCheckpointSummary {
                slot: 619,
                bank_hash: hash(5),
                accounts_hash: hash(2),
                last_blockhash: hash(6),
                capitalization: 7,
                transaction_count: 8,
                tick_height: 9,
                slot_complete: true,
                write_count: 10,
                next_write_version: 11,
            },
        }
    }

    fn write_archive(directory: &TempDir, evidence: &HistoricalHandoffSnapshotManifest) -> PathBuf {
        let path = directory.path().join(format!(
            "snapshot-{}-{}.tar.bz2",
            evidence.snapshot_slot, evidence.accounts_hash
        ));
        fs::write(&path, b"complete snapshot including status cache").unwrap();
        path
    }

    #[test]
    fn sidecar_round_trip_binds_every_archive_byte() {
        let directory = TempDir::new().unwrap();
        let evidence = manifest();
        let archive = write_archive(&directory, &evidence);
        let (sidecar, written) = write_handoff_snapshot_manifest(&archive, evidence).unwrap();
        assert_eq!(
            sidecar,
            directory.path().join(format!(
                "{}.handoff.json",
                archive.file_name().unwrap().to_string_lossy()
            ))
        );
        assert_ne!(written.archive_sha256, [0; 32]);
        assert_eq!(written.archive_size, fs::metadata(&archive).unwrap().len());
        assert_eq!(
            read_and_validate_handoff_snapshot_manifest(&archive).unwrap(),
            written
        );
    }

    #[test]
    fn changed_archive_is_rejected() {
        let directory = TempDir::new().unwrap();
        let evidence = manifest();
        let archive = write_archive(&directory, &evidence);
        write_handoff_snapshot_manifest(&archive, evidence).unwrap();
        OpenOptions::new()
            .append(true)
            .open(&archive)
            .unwrap()
            .write_all(b"changed")
            .unwrap();
        assert!(matches!(
            read_and_validate_handoff_snapshot_manifest(&archive),
            Err(HandoffSnapshotManifestError::ArchiveSizeMismatch { .. })
                | Err(HandoffSnapshotManifestError::ArchiveDigestMismatch { .. })
        ));
    }

    #[test]
    fn sidecar_rejects_boundary_and_checkpoint_mismatches() {
        let mut evidence = manifest();
        evidence.boundary_slot += 1;
        assert!(matches!(
            evidence.validate(),
            Err(HandoffSnapshotManifestError::Invalid(_))
        ));
        let mut evidence = manifest();
        evidence.terminal.accounts_hash = hash(42);
        assert!(matches!(
            evidence.validate(),
            Err(HandoffSnapshotManifestError::Invalid(_))
        ));
    }

    #[test]
    fn sidecar_publication_is_no_clobber() {
        let directory = TempDir::new().unwrap();
        let evidence = manifest();
        let archive = write_archive(&directory, &evidence);
        write_handoff_snapshot_manifest(&archive, evidence.clone()).unwrap();
        assert!(matches!(
            write_handoff_snapshot_manifest(&archive, evidence),
            Err(HandoffSnapshotManifestError::Io { .. })
        ));
    }

    #[cfg(unix)]
    #[test]
    fn single_open_admission_rejects_symlinks_fifos_and_path_replacement() {
        use {
            std::ffi::CString,
            std::os::unix::{
                ffi::OsStrExt as _,
                fs::{OpenOptionsExt as _, symlink},
            },
        };

        let evidence = manifest();
        let directory = TempDir::new().unwrap();
        let archive_name = format!(
            "snapshot-{}-{}.tar.bz2",
            evidence.snapshot_slot, evidence.accounts_hash
        );
        let target = directory.path().join("target");
        fs::write(&target, b"snapshot bytes").unwrap();
        let link = directory.path().join(&archive_name);
        symlink(&target, &link).unwrap();
        assert!(matches!(
            write_handoff_snapshot_manifest(&link, evidence.clone()),
            Err(HandoffSnapshotManifestError::Io { .. })
        ));
        fs::remove_file(&link).unwrap();

        let fifo = directory.path().join(&archive_name);
        let fifo_c = CString::new(fifo.as_os_str().as_bytes()).unwrap();
        assert_eq!(unsafe { libc::mkfifo(fifo_c.as_ptr(), 0o600) }, 0);
        // Keep both FIFO ends open so the test remains nonblocking even if an
        // admission flag regresses; opened-handle metadata must reject it.
        let mut keeper_options = OpenOptions::new();
        keeper_options
            .read(true)
            .write(true)
            .custom_flags(libc::O_NONBLOCK);
        let keeper = keeper_options.open(&fifo).unwrap();
        assert!(matches!(
            write_handoff_snapshot_manifest(&fifo, evidence.clone()),
            Err(HandoffSnapshotManifestError::Invalid(_))
        ));
        drop(keeper);
        fs::remove_file(&fifo).unwrap();

        let archive = directory.path().join(&archive_name);
        fs::write(&archive, b"first").unwrap();
        let (_opened, metadata) = open_regular_file(&archive, "test opening").unwrap();
        fs::rename(&archive, directory.path().join("old")).unwrap();
        fs::write(&archive, b"other").unwrap();
        assert!(matches!(
            ensure_path_names_open_file(&archive, &metadata),
            Err(HandoffSnapshotManifestError::PathIdentityChanged(changed)) if changed == archive
        ));
    }
}
