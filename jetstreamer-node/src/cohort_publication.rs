//! Supervisor-side delivery of root-checkpoint cohort publication outcomes.
//!
//! The archive publisher deliberately leaves a durable completed-outcome
//! marker in the public destination. This module validates the returned
//! namespace evidence, writes an owner-only receipt outside that destination,
//! and only then acknowledges the exact transaction. Recovery follows the
//! same path and tells the caller to stop, making a clean invocation the only
//! boundary at which replay or archive reuse can resume.

use {
    base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD},
    jetstreamer_node::{
        archive_checksum::{
            archive_batch_publication_in_progress, archive_checksum_line, archive_checksum_path,
            archive_file_identity, open_regular_nofollow, path_matches_archive_identity,
        },
        archive_publish::{
            ArchiveBatchIdentityEvidence, ArchiveBatchPublication, ArchiveBatchRecovery,
            ArchiveBatchRollback, ArchiveNamespaceIdentity, acknowledge_archive_publication_batch,
            recover_archive_publication_batch,
        },
        segment_manifest::segment_manifest_path,
    },
    serde::Serialize,
    std::{
        ffi::{CString, OsStr},
        fs::{self, File, OpenOptions},
        io::{self, Read as _, Write as _},
        os::{
            fd::{AsRawFd as _, FromRawFd as _},
            unix::{
                ffi::OsStrExt as _,
                fs::{MetadataExt as _, OpenOptionsExt as _, PermissionsExt as _},
            },
        },
        path::{Path, PathBuf},
        time::{SystemTime, UNIX_EPOCH},
    },
};

const RECEIPT_SCHEMA: &str = "jetstreamer-root-cohort-publication-receipt-v1";
const MAX_RECEIPT_BYTES: u64 = 32 * 1024 * 1024;

#[derive(Clone, Copy)]
pub(crate) struct DestinationBinding<'a> {
    pub path: &'a Path,
    pub device: u64,
    pub inode: u64,
}

#[derive(Clone, Copy)]
pub(crate) struct ExpectedCohortArchive<'a> {
    pub epoch: u64,
    pub destination_archive: &'a Path,
    pub sha256: [u8; 32],
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum RecoveryDisposition {
    None,
    /// A marker was recovered or cleared. The caller must always stop and
    /// require a clean invocation, even when no armed transaction existed.
    Stop {
        outcome: Option<RecoveredOutcome>,
        receipt_path: Option<PathBuf>,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RecoveredOutcome {
    Committed,
    RolledBack,
}

#[derive(Debug)]
pub(crate) struct CompletedPublication {
    pub transaction_id: [u8; 32],
    pub receipt_path: PathBuf,
    pub archive_count: usize,
}

#[derive(Serialize)]
struct PublicationReceipt {
    schema: &'static str,
    transaction_id: String,
    manifest_fingerprint: String,
    destination: ReceiptDestination,
    epochs: Vec<u64>,
    outcome: ReceiptOutcome,
}

#[derive(Serialize)]
struct ReceiptDestination {
    path_base64: String,
    device: u64,
    inode: u64,
}

#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum ReceiptOutcome {
    Committed { members: Vec<CommittedMember> },
    RolledBack { members: Vec<RolledBackMember> },
}

#[derive(Serialize)]
struct CommittedMember {
    epoch: u64,
    archive_path_base64: String,
    manifest_path_base64: Option<String>,
    checksum_path_base64: String,
    archive_sha256: String,
    initial_archive: Option<ReceiptIdentity>,
    initial_manifest: Option<ReceiptIdentity>,
    initial_checksum: Option<ReceiptIdentity>,
    committed_archive: ReceiptIdentity,
    committed_manifest: Option<ReceiptIdentity>,
    committed_checksum: ReceiptIdentity,
}

#[derive(Serialize)]
struct RolledBackMember {
    epoch: u64,
    destination_archive_path_base64: String,
    staged_archive_path_base64: String,
    staged_archive_sha256: String,
    restored_archive: Option<ReceiptIdentity>,
    restored_manifest: Option<ReceiptIdentity>,
    restored_checksum: Option<ReceiptIdentity>,
    staged_archive: ReceiptIdentity,
}

#[derive(Clone, Copy, Serialize)]
struct ReceiptIdentity {
    device: u64,
    inode: u64,
    mode: u32,
    uid: u32,
    gid: u32,
    link_count: u64,
    length: u64,
    modified_seconds: i64,
    modified_nanoseconds: i64,
    changed_seconds: i64,
    changed_nanoseconds: i64,
}

impl From<ArchiveNamespaceIdentity> for ReceiptIdentity {
    fn from(identity: ArchiveNamespaceIdentity) -> Self {
        Self {
            device: identity.device,
            inode: identity.inode,
            mode: identity.mode,
            uid: identity.uid,
            gid: identity.gid,
            link_count: identity.link_count,
            length: identity.length,
            modified_seconds: identity.modified_seconds,
            modified_nanoseconds: identity.modified_nanoseconds,
            changed_seconds: identity.changed_seconds,
            changed_nanoseconds: identity.changed_nanoseconds,
        }
    }
}

pub(crate) fn recover_pending_batch(
    destination: DestinationBinding<'_>,
    receipt_directory: &Path,
) -> Result<RecoveryDisposition, String> {
    if !archive_batch_publication_in_progress(destination.path).map_err(|error| {
        format!(
            "failed to inspect archive batch state in {}: {error}",
            destination.path.display()
        )
    })? {
        return Ok(RecoveryDisposition::None);
    }
    validate_receipt_location(destination.path, receipt_directory)?;

    let recovered = recover_archive_publication_batch(destination.path).map_err(|error| {
        format!(
            "archive batch recovery is indeterminate in {}: {error}",
            destination.path.display()
        )
    })?;
    let (outcome, receipt_path, transaction_id) = match recovered {
        ArchiveBatchRecovery::None => (None, None, None),
        ArchiveBatchRecovery::Committed(publication) => {
            let receipt = committed_receipt(destination, &publication, None)?;
            let receipt_path = persist_receipt(receipt_directory, &receipt)?;
            (
                Some(RecoveredOutcome::Committed),
                Some(receipt_path),
                Some(publication.transaction_id),
            )
        }
        ArchiveBatchRecovery::RolledBack(rollback) => {
            let receipt = rolled_back_receipt(destination, &rollback)?;
            let receipt_path = persist_receipt(receipt_directory, &receipt)?;
            (
                Some(RecoveredOutcome::RolledBack),
                Some(receipt_path),
                Some(rollback.transaction_id),
            )
        }
    };

    if let Some(transaction_id) = transaction_id {
        acknowledge_archive_publication_batch(destination.path, transaction_id).map_err(
            |error| {
                format!(
                    "archive batch receipt is durable but transaction {} could not be acknowledged: {error}",
                    digest_hex(&transaction_id)
                )
            },
        )?;
    }
    if archive_batch_publication_in_progress(destination.path).map_err(|error| {
        format!(
            "failed to recheck archive batch state in {}: {error}",
            destination.path.display()
        )
    })? {
        return Err(format!(
            "archive batch state remains in {} after recovery handling",
            destination.path.display()
        ));
    }
    Ok(RecoveryDisposition::Stop {
        outcome,
        receipt_path,
    })
}

pub(crate) fn finish_committed_publication(
    destination: DestinationBinding<'_>,
    receipt_directory: &Path,
    expected_manifest_fingerprint: [u8; 32],
    expected: &[ExpectedCohortArchive<'_>],
    publication: &ArchiveBatchPublication,
) -> Result<CompletedPublication, String> {
    validate_receipt_location(destination.path, receipt_directory)?;
    let receipt = committed_receipt(destination, publication, Some(expected))?;
    if publication.manifest_fingerprint != expected_manifest_fingerprint {
        return Err(format!(
            "archive batch returned manifest fingerprint {}, expected {}",
            digest_hex(&publication.manifest_fingerprint),
            digest_hex(&expected_manifest_fingerprint)
        ));
    }
    let receipt_path = persist_receipt(receipt_directory, &receipt)?;
    acknowledge_archive_publication_batch(destination.path, publication.transaction_id).map_err(
        |error| {
            format!(
                "archive batch receipt is durable but transaction {} could not be acknowledged: {error}",
                digest_hex(&publication.transaction_id)
            )
        },
    )?;
    if archive_batch_publication_in_progress(destination.path).map_err(|error| {
        format!(
            "failed to recheck archive batch state in {}: {error}",
            destination.path.display()
        )
    })? {
        return Err(format!(
            "archive batch state remains in {} after acknowledgement",
            destination.path.display()
        ));
    }
    Ok(CompletedPublication {
        transaction_id: publication.transaction_id,
        receipt_path,
        archive_count: publication.publications.len(),
    })
}

fn committed_receipt(
    destination: DestinationBinding<'_>,
    publication: &ArchiveBatchPublication,
    expected: Option<&[ExpectedCohortArchive<'_>]>,
) -> Result<PublicationReceipt, String> {
    validate_destination_binding(destination)?;
    if publication.destination_identity.device != destination.device
        || publication.destination_identity.inode != destination.inode
    {
        return Err("archive batch result names a different destination identity".to_string());
    }
    if publication.expected_epochs.is_empty()
        || publication.expected_epochs.len() != publication.publications.len()
        || publication.publications.len() != publication.identity_evidence.len()
    {
        return Err("archive batch result has inconsistent or empty member evidence".to_string());
    }
    validate_contiguous_epoch_sequence(&publication.expected_epochs)?;
    if let Some(expected) = expected
        && expected.len() != publication.publications.len()
    {
        return Err(format!(
            "archive batch returned {} members, expected {}",
            publication.publications.len(),
            expected.len()
        ));
    }
    if let Some(expected) = expected
        && !publication
            .expected_epochs
            .iter()
            .copied()
            .eq(expected.iter().map(|archive| archive.epoch))
    {
        return Err(
            "archive batch journal membership does not match the validated cohort request"
                .to_string(),
        );
    }

    let mut epochs = Vec::with_capacity(publication.publications.len());
    let mut members = Vec::with_capacity(publication.publications.len());
    for (index, (published, identity)) in publication
        .publications
        .iter()
        .zip(&publication.identity_evidence)
        .enumerate()
    {
        if publication.expected_epochs[index] != identity.epoch {
            return Err(format!(
                "archive batch result member epoch {} does not match journal-bound expected epoch {}",
                identity.epoch, publication.expected_epochs[index]
            ));
        }
        let canonical_archive = destination
            .path
            .join(format!("epoch-{}.jet", identity.epoch));
        if published.archive_path != canonical_archive {
            return Err(format!(
                "archive batch epoch {} returned noncanonical archive path {}",
                identity.epoch,
                published.archive_path.display()
            ));
        }
        let canonical_checksum = archive_checksum_path(&canonical_archive)
            .map_err(|error| format!("failed to derive archive checksum path: {error}"))?;
        if published.checksum_path != canonical_checksum {
            return Err(format!(
                "archive batch epoch {} returned noncanonical checksum path {}",
                identity.epoch,
                published.checksum_path.display()
            ));
        }
        let canonical_manifest = segment_manifest_path(&canonical_archive)
            .map_err(|error| format!("failed to derive segment manifest path: {error}"))?;
        if published
            .manifest_path
            .as_deref()
            .is_some_and(|path| path != canonical_manifest)
            || published.manifest_path.is_some() != identity.committed_manifest.is_some()
        {
            return Err(format!(
                "archive batch epoch {} returned inconsistent manifest evidence",
                identity.epoch
            ));
        }
        if published.evidence != identity.archive_validation {
            return Err(format!(
                "archive batch epoch {} returned inconsistent validation evidence",
                identity.epoch
            ));
        }
        if let Some(expected) = expected {
            let expected = &expected[index];
            if expected.epoch != identity.epoch
                || expected.destination_archive != canonical_archive
                || expected.sha256 != identity.archive_validation.sha256
            {
                return Err(format!(
                    "archive batch member {} does not match the validated cohort request",
                    identity.epoch
                ));
            }
        }
        validate_committed_member(
            &canonical_archive,
            &canonical_manifest,
            &canonical_checksum,
            identity,
        )?;
        epochs.push(identity.epoch);
        members.push(CommittedMember {
            epoch: identity.epoch,
            archive_path_base64: path_base64(&canonical_archive),
            manifest_path_base64: published.manifest_path.as_deref().map(path_base64),
            checksum_path_base64: path_base64(&canonical_checksum),
            archive_sha256: digest_hex(&identity.archive_validation.sha256),
            initial_archive: identity.initial_archive.map(Into::into),
            initial_manifest: identity.initial_manifest.map(Into::into),
            initial_checksum: identity.initial_checksum.map(Into::into),
            committed_archive: identity.committed_archive.into(),
            committed_manifest: identity.committed_manifest.map(Into::into),
            committed_checksum: identity.committed_checksum.into(),
        });
    }

    Ok(PublicationReceipt {
        schema: RECEIPT_SCHEMA,
        transaction_id: digest_hex(&publication.transaction_id),
        manifest_fingerprint: format!("sha256:{}", digest_hex(&publication.manifest_fingerprint)),
        destination: ReceiptDestination {
            path_base64: path_base64(destination.path),
            device: destination.device,
            inode: destination.inode,
        },
        epochs,
        outcome: ReceiptOutcome::Committed { members },
    })
}

fn validate_committed_member(
    archive: &Path,
    manifest: &Path,
    checksum: &Path,
    evidence: &ArchiveBatchIdentityEvidence,
) -> Result<(), String> {
    require_namespace_identity(archive, Some(evidence.committed_archive))?;
    require_namespace_identity(manifest, evidence.committed_manifest)?;
    require_namespace_identity(checksum, Some(evidence.committed_checksum))?;
    if !path_matches_archive_identity(archive, evidence.archive_validation.identity).map_err(
        |error| {
            format!(
                "failed to rebind committed archive {}: {error}",
                archive.display()
            )
        },
    )? {
        return Err(format!(
            "committed archive changed after publication: {}",
            archive.display()
        ));
    }
    let expected_checksum = archive_checksum_line(
        &evidence.archive_validation.sha256,
        archive
            .file_name()
            .ok_or_else(|| format!("archive path has no filename: {}", archive.display()))?,
    )
    .map_err(|error| format!("failed to derive canonical checksum: {error}"))?;
    let actual_checksum = read_regular_nofollow(checksum)?;
    if actual_checksum != expected_checksum.as_bytes() {
        return Err(format!(
            "committed checksum does not match validated archive: {}",
            checksum.display()
        ));
    }
    Ok(())
}

fn rolled_back_receipt(
    destination: DestinationBinding<'_>,
    rollback: &ArchiveBatchRollback,
) -> Result<PublicationReceipt, String> {
    validate_destination_binding(destination)?;
    if rollback.destination_identity.device != destination.device
        || rollback.destination_identity.inode != destination.inode
    {
        return Err("archive batch rollback names a different destination identity".to_string());
    }
    if rollback.expected_epochs.is_empty()
        || rollback.expected_epochs.len() != rollback.identity_evidence.len()
    {
        return Err("archive batch rollback has inconsistent or empty member evidence".to_string());
    }
    validate_contiguous_epoch_sequence(&rollback.expected_epochs)?;
    let mut epochs = Vec::with_capacity(rollback.identity_evidence.len());
    let mut members = Vec::with_capacity(rollback.identity_evidence.len());
    for (expected_epoch, identity) in rollback
        .expected_epochs
        .iter()
        .zip(&rollback.identity_evidence)
    {
        if *expected_epoch != identity.epoch {
            return Err(format!(
                "archive batch rollback member epoch {} does not match journal-bound expected epoch {}",
                identity.epoch, expected_epoch
            ));
        }
        let destination_archive = destination
            .path
            .join(format!("epoch-{}.jet", identity.epoch));
        let destination_manifest = segment_manifest_path(&destination_archive)
            .map_err(|error| format!("failed to derive segment manifest path: {error}"))?;
        let destination_checksum = archive_checksum_path(&destination_archive)
            .map_err(|error| format!("failed to derive archive checksum path: {error}"))?;
        require_namespace_identity(&destination_archive, identity.restored_archive)?;
        require_namespace_identity(&destination_manifest, identity.restored_manifest)?;
        require_namespace_identity(&destination_checksum, identity.restored_checksum)?;
        require_namespace_identity(&identity.staged_archive_path, Some(identity.staged_archive))?;
        if identity.staged_archive_path.file_name()
            != Some(OsStr::new(&format!("epoch-{}.jet", identity.epoch)))
        {
            return Err(format!(
                "archive batch rollback epoch {} returned a noncanonical staged filename",
                identity.epoch
            ));
        }
        if !path_matches_archive_identity(
            &identity.staged_archive_path,
            identity.staged_archive_validation.identity,
        )
        .map_err(|error| {
            format!(
                "failed to rebind rolled-back staged archive {}: {error}",
                identity.staged_archive_path.display()
            )
        })? {
            return Err(format!(
                "rolled-back staged archive changed before receipt delivery: {}",
                identity.staged_archive_path.display()
            ));
        }
        epochs.push(identity.epoch);
        members.push(RolledBackMember {
            epoch: identity.epoch,
            destination_archive_path_base64: path_base64(&destination_archive),
            staged_archive_path_base64: path_base64(&identity.staged_archive_path),
            staged_archive_sha256: digest_hex(&identity.staged_archive_validation.sha256),
            restored_archive: identity.restored_archive.map(Into::into),
            restored_manifest: identity.restored_manifest.map(Into::into),
            restored_checksum: identity.restored_checksum.map(Into::into),
            staged_archive: identity.staged_archive.into(),
        });
    }
    Ok(PublicationReceipt {
        schema: RECEIPT_SCHEMA,
        transaction_id: digest_hex(&rollback.transaction_id),
        manifest_fingerprint: format!("sha256:{}", digest_hex(&rollback.manifest_fingerprint)),
        destination: ReceiptDestination {
            path_base64: path_base64(destination.path),
            device: destination.device,
            inode: destination.inode,
        },
        epochs,
        outcome: ReceiptOutcome::RolledBack { members },
    })
}

fn validate_destination_binding(destination: DestinationBinding<'_>) -> Result<(), String> {
    let metadata = fs::symlink_metadata(destination.path).map_err(|error| {
        format!(
            "failed to recheck cohort destination {}: {error}",
            destination.path.display()
        )
    })?;
    if !destination.path.is_absolute()
        || !metadata.file_type().is_dir()
        || metadata.dev() != destination.device
        || metadata.ino() != destination.inode
    {
        return Err(format!(
            "cohort destination changed after it was bound: {}",
            destination.path.display()
        ));
    }
    Ok(())
}

fn validate_receipt_location(destination: &Path, receipt_directory: &Path) -> Result<(), String> {
    let receipt_directory = receipt_directory.canonicalize().map_err(|error| {
        format!(
            "failed to canonicalize archive batch receipt directory {}: {error}",
            receipt_directory.display()
        )
    })?;
    if receipt_directory.starts_with(destination) {
        return Err(format!(
            "archive batch receipts must not be stored in the public destination {}",
            destination.display()
        ));
    }
    Ok(())
}

fn validate_contiguous_epoch_sequence(epochs: &[u64]) -> Result<(), String> {
    if epochs.is_empty() {
        return Err("archive batch epoch sequence is empty".to_string());
    }
    for pair in epochs.windows(2) {
        if pair[0].checked_add(1) != Some(pair[1]) {
            return Err(format!(
                "archive batch epochs are not contiguous at {} then {}",
                pair[0], pair[1]
            ));
        }
    }
    Ok(())
}

fn require_namespace_identity(
    path: &Path,
    expected: Option<ArchiveNamespaceIdentity>,
) -> Result<(), String> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => Some(metadata),
        Err(error) if error.kind() == io::ErrorKind::NotFound => None,
        Err(error) => {
            return Err(format!(
                "failed to inspect archive batch path {}: {error}",
                path.display()
            ));
        }
    };
    match (metadata, expected) {
        (None, None) => Ok(()),
        (None, Some(_)) => Err(format!(
            "archive batch path disappeared before receipt delivery: {}",
            path.display()
        )),
        (Some(_), None) => Err(format!(
            "archive batch path unexpectedly exists after rollback: {}",
            path.display()
        )),
        (Some(metadata), Some(expected)) => {
            if !metadata.file_type().is_file() || namespace_identity(&metadata) != expected {
                return Err(format!(
                    "archive batch namespace identity changed before receipt delivery: {}",
                    path.display()
                ));
            }
            Ok(())
        }
    }
}

fn namespace_identity(metadata: &fs::Metadata) -> ArchiveNamespaceIdentity {
    ArchiveNamespaceIdentity {
        device: metadata.dev(),
        inode: metadata.ino(),
        mode: metadata.mode(),
        uid: metadata.uid(),
        gid: metadata.gid(),
        link_count: metadata.nlink(),
        length: metadata.len(),
        modified_seconds: metadata.mtime(),
        modified_nanoseconds: metadata.mtime_nsec(),
        changed_seconds: metadata.ctime(),
        changed_nanoseconds: metadata.ctime_nsec(),
    }
}

fn read_regular_nofollow(path: &Path) -> Result<Vec<u8>, String> {
    let mut file = open_regular_nofollow(path)
        .map_err(|error| format!("failed to open {}: {error}", path.display()))?;
    let length = file
        .metadata()
        .map_err(|error| format!("failed to inspect {}: {error}", path.display()))?
        .len();
    if length > 1024 {
        return Err(format!(
            "archive checksum exceeds 1024 bytes: {}",
            path.display()
        ));
    }
    let identity = archive_file_identity(&file)
        .map_err(|error| format!("failed to bind {}: {error}", path.display()))?;
    let mut bytes = Vec::with_capacity(length as usize);
    std::io::Read::by_ref(&mut file)
        .take(1025)
        .read_to_end(&mut bytes)
        .map_err(|error| format!("failed to read {}: {error}", path.display()))?;
    if bytes.len() > 1024 {
        return Err(format!(
            "archive checksum grew beyond 1024 bytes: {}",
            path.display()
        ));
    }
    if archive_file_identity(&file)
        .map_err(|error| format!("failed to recheck {}: {error}", path.display()))?
        != identity
        || !path_matches_archive_identity(path, identity)
            .map_err(|error| format!("failed to recheck {}: {error}", path.display()))?
    {
        return Err(format!(
            "file changed while it was read: {}",
            path.display()
        ));
    }
    Ok(bytes)
}

fn persist_receipt(directory: &Path, receipt: &PublicationReceipt) -> Result<PathBuf, String> {
    let mut bytes = serde_json::to_vec_pretty(receipt)
        .map_err(|error| format!("failed to encode archive batch receipt: {error}"))?;
    bytes.push(b'\n');
    if bytes.len() as u64 > MAX_RECEIPT_BYTES {
        return Err("archive batch receipt exceeds its size limit".to_string());
    }
    let final_name = format!("batch-{}.json", receipt.transaction_id);
    let final_path = directory.join(&final_name);
    let directory_file = bind_private_receipt_directory(directory)?;
    let final_name = CString::new(final_name)
        .map_err(|_| "archive batch receipt filename contains NUL".to_string())?;

    if let Some(existing) = read_receipt_at(&directory_file, &final_name)? {
        if existing == bytes {
            return Ok(final_path);
        }
        return Err(format!(
            "existing archive batch receipt disagrees with recovered evidence: {}",
            final_path.display()
        ));
    }

    let temporary_name = format!(
        ".receipt-{}-{}-{}.tmp",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos(),
        &receipt.transaction_id[..16]
    );
    let temporary_name = CString::new(temporary_name)
        .map_err(|_| "archive batch receipt temporary filename contains NUL".to_string())?;
    let raw_fd = unsafe {
        // SAFETY: both the directory descriptor and NUL-terminated basename
        // remain live for the call. O_EXCL and O_NOFOLLOW prevent reuse or
        // symlink traversal of a colliding temporary name.
        libc::openat(
            directory_file.as_raw_fd(),
            temporary_name.as_ptr(),
            libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_CLOEXEC | libc::O_NOFOLLOW,
            0o600,
        )
    };
    if raw_fd < 0 {
        return Err(format!(
            "failed to create private archive batch receipt temporary file: {}",
            io::Error::last_os_error()
        ));
    }
    let mut temporary = unsafe {
        // SAFETY: openat returned a new owned descriptor on success.
        File::from_raw_fd(raw_fd)
    };
    let write_result = temporary
        .write_all(&bytes)
        .and_then(|()| temporary.sync_all())
        .map_err(|error| format!("failed to sync private archive batch receipt: {error}"));
    if let Err(error) = write_result {
        unlinkat_if_present(&directory_file, &temporary_name);
        return Err(error);
    }
    drop(temporary);

    let rename_result = unsafe {
        // SAFETY: the directory descriptor and both NUL-terminated basenames
        // remain live. RENAME_NOREPLACE preserves any pre-existing receipt.
        libc::renameat2(
            directory_file.as_raw_fd(),
            temporary_name.as_ptr(),
            directory_file.as_raw_fd(),
            final_name.as_ptr(),
            libc::RENAME_NOREPLACE,
        )
    };
    if rename_result != 0 {
        let error = io::Error::last_os_error();
        unlinkat_if_present(&directory_file, &temporary_name);
        if error.kind() == io::ErrorKind::AlreadyExists {
            let existing = read_receipt_at(&directory_file, &final_name)?.ok_or_else(|| {
                format!(
                    "archive batch receipt appeared and disappeared during delivery: {}",
                    final_path.display()
                )
            })?;
            if existing == bytes {
                return Ok(final_path);
            }
            return Err(format!(
                "existing archive batch receipt disagrees with recovered evidence: {}",
                final_path.display()
            ));
        }
        return Err(format!(
            "failed to install private archive batch receipt {}: {error}",
            final_path.display()
        ));
    }
    directory_file.sync_all().map_err(|error| {
        format!(
            "failed to sync archive batch receipt directory {}: {error}",
            directory.display()
        )
    })?;
    let installed = read_receipt_at(&directory_file, &final_name)?.ok_or_else(|| {
        format!(
            "archive batch receipt disappeared after installation: {}",
            final_path.display()
        )
    })?;
    if installed != bytes {
        return Err(format!(
            "archive batch receipt changed after installation: {}",
            final_path.display()
        ));
    }
    Ok(final_path)
}

fn bind_private_receipt_directory(path: &Path) -> Result<File, String> {
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_CLOEXEC | libc::O_DIRECTORY | libc::O_NOFOLLOW)
        .open(path)
        .map_err(|error| {
            format!(
                "failed to bind archive batch receipt directory {}: {error}",
                path.display()
            )
        })?;
    let metadata = file.metadata().map_err(|error| {
        format!(
            "failed to inspect archive batch receipt directory {}: {error}",
            path.display()
        )
    })?;
    let path_metadata = fs::symlink_metadata(path).map_err(|error| {
        format!(
            "failed to recheck archive batch receipt directory {}: {error}",
            path.display()
        )
    })?;
    if !metadata.file_type().is_dir()
        || !path_metadata.file_type().is_dir()
        || metadata.uid() != effective_user_id()
        || metadata.permissions().mode() & 0o077 != 0
        || metadata.dev() != path_metadata.dev()
        || metadata.ino() != path_metadata.ino()
    {
        return Err(format!(
            "archive batch receipt path must be an owner-only bound directory: {}",
            path.display()
        ));
    }
    Ok(file)
}

fn read_receipt_at(directory: &File, name: &CString) -> Result<Option<Vec<u8>>, String> {
    let raw_fd = unsafe {
        // SAFETY: the directory descriptor and NUL-terminated basename remain
        // live for the call. O_NOFOLLOW rejects a receipt symlink.
        libc::openat(
            directory.as_raw_fd(),
            name.as_ptr(),
            libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK,
        )
    };
    if raw_fd < 0 {
        let error = io::Error::last_os_error();
        if error.kind() == io::ErrorKind::NotFound {
            return Ok(None);
        }
        return Err(format!(
            "failed to open private archive batch receipt: {error}"
        ));
    }
    let mut file = unsafe {
        // SAFETY: openat returned a new owned descriptor on success.
        File::from_raw_fd(raw_fd)
    };
    let initial = file
        .metadata()
        .map_err(|error| format!("failed to inspect private archive batch receipt: {error}"))?;
    if !initial.file_type().is_file()
        || initial.uid() != effective_user_id()
        || initial.permissions().mode() & 0o777 != 0o600
        || initial.nlink() != 1
        || initial.len() > MAX_RECEIPT_BYTES
    {
        return Err("private archive batch receipt has unsafe identity or permissions".to_string());
    }
    let mut bytes = Vec::new();
    std::io::Read::by_ref(&mut file)
        .take(MAX_RECEIPT_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|error| format!("failed to read private archive batch receipt: {error}"))?;
    if bytes.len() as u64 > MAX_RECEIPT_BYTES {
        return Err("private archive batch receipt exceeds its size limit".to_string());
    }
    let final_metadata = file
        .metadata()
        .map_err(|error| format!("failed to recheck private archive batch receipt: {error}"))?;
    if namespace_identity(&initial) != namespace_identity(&final_metadata) {
        return Err("private archive batch receipt changed while it was read".to_string());
    }
    Ok(Some(bytes))
}

fn unlinkat_if_present(directory: &File, name: &CString) {
    unsafe {
        // SAFETY: this best-effort cleanup uses the already-bound private
        // directory and the unique temporary basename created above.
        libc::unlinkat(directory.as_raw_fd(), name.as_ptr(), 0);
    }
}

fn effective_user_id() -> u32 {
    unsafe {
        // SAFETY: geteuid has no preconditions and dereferences no pointers.
        libc::geteuid()
    }
}

fn path_base64(path: &Path) -> String {
    BASE64_STANDARD.encode(path.as_os_str().as_bytes())
}

fn digest_hex(digest: &[u8; 32]) -> String {
    jetstreamer_node::segment_manifest::sha256_hex_string(digest)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::{DirBuilderExt as _, symlink};

    fn private_directory(parent: &Path, name: &str) -> PathBuf {
        let path = parent.join(name);
        let mut builder = fs::DirBuilder::new();
        builder.mode(0o700).create(&path).unwrap();
        path
    }

    fn sample_receipt(destination: &Path, metadata: &fs::Metadata) -> PublicationReceipt {
        PublicationReceipt {
            schema: RECEIPT_SCHEMA,
            transaction_id: digest_hex(&[0x11; 32]),
            manifest_fingerprint: format!("sha256:{}", digest_hex(&[0x22; 32])),
            destination: ReceiptDestination {
                path_base64: path_base64(destination),
                device: metadata.dev(),
                inode: metadata.ino(),
            },
            epochs: vec![17, 18, 19],
            outcome: ReceiptOutcome::Committed {
                members: Vec::new(),
            },
        }
    }

    fn staged_batch_items(
        staging: &Path,
        destination: &Path,
        epochs: &[u64],
    ) -> Vec<jetstreamer_node::archive_publish::ArchiveBatchItem> {
        epochs
            .iter()
            .map(|epoch| {
                let name = format!("epoch-{epoch}.jet");
                let staged_archive = staging.join(&name);
                fs::write(&staged_archive, format!("validated epoch {epoch} archive")).unwrap();
                fs::set_permissions(&staged_archive, fs::Permissions::from_mode(0o440)).unwrap();
                let file = open_regular_nofollow(&staged_archive).unwrap();
                let evidence =
                    jetstreamer_node::archive_checksum::measure_open_archive(&file).unwrap();
                jetstreamer_node::archive_publish::ArchiveBatchItem {
                    epoch: *epoch,
                    staged_archive,
                    destination_archive: destination.join(name),
                    evidence,
                }
            })
            .collect()
    }

    fn destination_binding(path: &Path) -> DestinationBinding<'_> {
        let metadata = fs::metadata(path).unwrap();
        DestinationBinding {
            path,
            device: metadata.dev(),
            inode: metadata.ino(),
        }
    }

    #[test]
    fn recovered_receipt_epochs_must_be_nonempty_and_contiguous() {
        assert!(validate_contiguous_epoch_sequence(&[17, 18, 19]).is_ok());
        for epochs in [&[][..], &[17, 17], &[17, 16], &[17, 19]] {
            assert!(validate_contiguous_epoch_sequence(epochs).is_err());
        }
        assert!(validate_contiguous_epoch_sequence(&[u64::MAX - 1, u64::MAX]).is_ok());
        assert!(validate_contiguous_epoch_sequence(&[u64::MAX, 0]).is_err());
    }

    #[test]
    fn receipt_install_is_idempotent_but_rejects_different_evidence() {
        let fixture = tempfile::tempdir_in(".").unwrap();
        let destination = private_directory(fixture.path(), "destination");
        let receipts = private_directory(fixture.path(), "receipts");
        let metadata = fs::metadata(&destination).unwrap();
        let receipt = sample_receipt(&destination, &metadata);
        let path = persist_receipt(&receipts, &receipt).unwrap();
        assert_eq!(persist_receipt(&receipts, &receipt).unwrap(), path);
        assert_eq!(
            fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );

        let mut changed = sample_receipt(&destination, &metadata);
        changed.epochs.push(20);
        let error = persist_receipt(&receipts, &changed).unwrap_err();
        assert!(error.contains("disagrees"), "{error}");
    }

    #[test]
    fn receipt_install_rejects_symlink_targets_and_directories() {
        let fixture = tempfile::tempdir_in(".").unwrap();
        let destination = private_directory(fixture.path(), "destination");
        let receipts = private_directory(fixture.path(), "receipts");
        let metadata = fs::metadata(&destination).unwrap();
        let receipt = sample_receipt(&destination, &metadata);
        let name = format!("batch-{}.json", receipt.transaction_id);
        let target = fixture.path().join("target");
        fs::write(&target, b"not a receipt").unwrap();
        symlink(&target, receipts.join(name)).unwrap();
        let error = persist_receipt(&receipts, &receipt).unwrap_err();
        assert!(error.contains("failed to open"), "{error}");

        let redirected = fixture.path().join("redirected");
        fs::remove_file(receipts.join(format!("batch-{}.json", receipt.transaction_id))).unwrap();
        fs::create_dir(&redirected).unwrap();
        fs::remove_dir(&receipts).unwrap();
        symlink(&redirected, &receipts).unwrap();
        let error = persist_receipt(&receipts, &receipt).unwrap_err();
        assert!(error.contains("failed to bind"), "{error}");
    }

    #[test]
    fn public_destination_cannot_hold_private_receipts() {
        let fixture = tempfile::tempdir_in(".").unwrap();
        let root = fixture.path().canonicalize().unwrap();
        let destination = private_directory(&root, "destination");
        let receipts = private_directory(&destination, "receipts");
        let error = validate_receipt_location(&destination, &receipts).unwrap_err();
        assert!(error.contains("must not be stored"), "{error}");
    }

    #[test]
    fn rollback_receipt_records_restored_identity_and_validated_staging_digest() {
        let fixture = tempfile::tempdir_in(".").unwrap();
        let root = fixture.path().canonicalize().unwrap();
        let staging = private_directory(&root, "staging");
        let destination = private_directory(&root, "destination");
        let receipts = private_directory(&root, "receipts");
        let staged_path = staging.join("epoch-17.jet");
        fs::write(&staged_path, b"validated rolled-back archive").unwrap();
        fs::set_permissions(&staged_path, fs::Permissions::from_mode(0o440)).unwrap();
        let staged_file = open_regular_nofollow(&staged_path).unwrap();
        let validation =
            jetstreamer_node::archive_checksum::measure_open_archive(&staged_file).unwrap();
        let staged_identity = namespace_identity(&staged_file.metadata().unwrap());
        let binding = destination_binding(&destination);
        let rollback = ArchiveBatchRollback {
            transaction_id: [0x51; 32],
            manifest_fingerprint: [0x52; 32],
            expected_epochs: vec![17],
            destination_identity:
                jetstreamer_node::archive_publish::ArchiveBatchDestinationIdentity {
                    device: binding.device,
                    inode: binding.inode,
                },
            identity_evidence: vec![
                jetstreamer_node::archive_publish::ArchiveBatchRollbackIdentityEvidence {
                    epoch: 17,
                    staged_archive_path: staged_path,
                    restored_archive: None,
                    restored_manifest: None,
                    restored_checksum: None,
                    staged_archive: staged_identity,
                    staged_archive_validation: validation,
                },
            ],
        };
        let receipt = rolled_back_receipt(binding, &rollback).unwrap();
        let receipt_path = persist_receipt(&receipts, &receipt).unwrap();
        let value: serde_json::Value =
            serde_json::from_slice(&fs::read(receipt_path).unwrap()).unwrap();
        assert_eq!(value["outcome"]["kind"], "rolled_back");
        assert_eq!(
            value["outcome"]["members"][0]["staged_archive_sha256"],
            digest_hex(&validation.sha256)
        );
        assert!(value["outcome"]["members"][0]["restored_archive"].is_null());
    }

    #[test]
    fn crash_before_receipt_recovers_committed_batch_then_requires_clean_run() {
        let fixture = tempfile::tempdir_in(".").unwrap();
        let root = fixture.path().canonicalize().unwrap();
        let staging = private_directory(&root, "staging");
        let destination = private_directory(&root, "destination");
        let receipts = private_directory(&root, "receipts");
        let items = staged_batch_items(&staging, &destination, &[17, 18, 19]);
        let fingerprint = [0x42; 32];
        let expected_epochs = items.iter().map(|item| item.epoch).collect::<Vec<_>>();

        // Dropping this result before writing a receipt models termination in
        // the supervisor's delivery gap. The durable outcome must remain.
        drop(
            jetstreamer_node::archive_publish::publish_verified_archive_batch(
                fingerprint,
                &expected_epochs,
                &items,
            )
            .unwrap(),
        );
        assert!(archive_batch_publication_in_progress(&destination).unwrap());

        let disposition =
            recover_pending_batch(destination_binding(&destination), &receipts).unwrap();
        let RecoveryDisposition::Stop {
            outcome: Some(RecoveredOutcome::Committed),
            receipt_path: Some(receipt_path),
        } = disposition
        else {
            panic!("committed outcome was not delivered: {disposition:?}");
        };
        assert!(receipt_path.is_file());
        let receipt = fs::read_to_string(receipt_path).unwrap();
        assert!(receipt.contains("\"kind\": \"committed\""));
        assert!(receipt.contains(&format!("sha256:{}", digest_hex(&fingerprint))));
        assert!(!archive_batch_publication_in_progress(&destination).unwrap());
        assert_eq!(
            recover_pending_batch(destination_binding(&destination), &receipts).unwrap(),
            RecoveryDisposition::None
        );
    }

    #[test]
    fn successful_publication_persists_exact_receipt_before_acknowledgement() {
        let fixture = tempfile::tempdir_in(".").unwrap();
        let root = fixture.path().canonicalize().unwrap();
        let staging = private_directory(&root, "staging");
        let destination = private_directory(&root, "destination");
        let receipts = private_directory(&root, "receipts");
        let items = staged_batch_items(&staging, &destination, &[31, 32]);
        let fingerprint = [0x24; 32];
        let expected_epochs = items.iter().map(|item| item.epoch).collect::<Vec<_>>();
        let expected = items
            .iter()
            .map(|item| ExpectedCohortArchive {
                epoch: item.epoch,
                destination_archive: &item.destination_archive,
                sha256: item.evidence.sha256,
            })
            .collect::<Vec<_>>();
        let publication = jetstreamer_node::archive_publish::publish_verified_archive_batch(
            fingerprint,
            &expected_epochs,
            &items,
        )
        .unwrap();
        assert!(archive_batch_publication_in_progress(&destination).unwrap());

        let completed = finish_committed_publication(
            destination_binding(&destination),
            &receipts,
            fingerprint,
            &expected,
            &publication,
        )
        .unwrap();
        assert_eq!(completed.archive_count, 2);
        assert!(completed.receipt_path.is_file());
        assert!(!archive_batch_publication_in_progress(&destination).unwrap());
    }

    #[test]
    fn mismatched_expected_digest_leaves_outcome_closed_for_recovery() {
        let fixture = tempfile::tempdir_in(".").unwrap();
        let root = fixture.path().canonicalize().unwrap();
        let staging = private_directory(&root, "staging");
        let destination = private_directory(&root, "destination");
        let receipts = private_directory(&root, "receipts");
        let items = staged_batch_items(&staging, &destination, &[89, 90]);
        let fingerprint = [0x33; 32];
        let expected_epochs = items.iter().map(|item| item.epoch).collect::<Vec<_>>();
        let mut publication = jetstreamer_node::archive_publish::publish_verified_archive_batch(
            fingerprint,
            &expected_epochs,
            &items,
        )
        .unwrap();
        publication.expected_epochs[1] = 91;
        let expected = items
            .iter()
            .map(|item| ExpectedCohortArchive {
                epoch: item.epoch,
                destination_archive: &item.destination_archive,
                sha256: item.evidence.sha256,
            })
            .collect::<Vec<_>>();
        let error = finish_committed_publication(
            destination_binding(&destination),
            &receipts,
            fingerprint,
            &expected,
            &publication,
        )
        .unwrap_err();
        assert!(error.contains("not contiguous"), "{error}");
        assert!(archive_batch_publication_in_progress(&destination).unwrap());
        publication.expected_epochs[1] = 90;
        publication.identity_evidence[1].epoch = 91;
        let error = finish_committed_publication(
            destination_binding(&destination),
            &receipts,
            fingerprint,
            &expected,
            &publication,
        )
        .unwrap_err();
        assert!(error.contains("journal-bound expected epoch"), "{error}");
        assert!(archive_batch_publication_in_progress(&destination).unwrap());
        publication.identity_evidence[1].epoch = 90;
        let wrong = [ExpectedCohortArchive {
            epoch: 89,
            destination_archive: &items[0].destination_archive,
            sha256: [0xff; 32],
        }];
        let error = finish_committed_publication(
            destination_binding(&destination),
            &receipts,
            fingerprint,
            &wrong,
            &publication,
        )
        .unwrap_err();
        assert!(error.contains("returned 2 members, expected 1"), "{error}");
        assert!(archive_batch_publication_in_progress(&destination).unwrap());

        assert!(matches!(
            recover_pending_batch(destination_binding(&destination), &receipts).unwrap(),
            RecoveryDisposition::Stop {
                outcome: Some(RecoveredOutcome::Committed),
                ..
            }
        ));
        assert!(!archive_batch_publication_in_progress(&destination).unwrap());
    }
}
