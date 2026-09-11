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
            ArchiveBatchDestinationIdentity, ArchiveBatchIdentityEvidence, ArchiveBatchPublication,
            ArchiveBatchRecovery, ArchiveBatchRollback, ArchiveNamespaceIdentity,
            acknowledge_archive_publication_batch, recover_archive_publication_batch,
        },
        segment_manifest::segment_manifest_path,
    },
    serde::{Deserialize, Serialize},
    std::{
        ffi::{CString, OsStr, OsString},
        fs::{self, File, OpenOptions},
        io::{self, Read as _, Write as _},
        os::{
            fd::{AsRawFd as _, FromRawFd as _},
            unix::{
                ffi::{OsStrExt as _, OsStringExt as _},
                fs::{MetadataExt as _, OpenOptionsExt as _, PermissionsExt as _},
            },
        },
        path::{Path, PathBuf},
        time::{SystemTime, UNIX_EPOCH},
    },
};

const RECEIPT_SCHEMA_V1: &str = "jetstreamer-root-cohort-publication-receipt-v1";
const ROOT_CHECKPOINT_RECEIPT_SCHEMA_V2: &str = "jetstreamer-root-cohort-publication-receipt-v2";
const MAX_RECEIPT_BYTES: u64 = 32 * 1024 * 1024;
const MAX_ROOT_CHECKPOINT_CONTEXT_BYTES: usize = 4 << 20;
pub(crate) const ROOT_CHECKPOINT_GATE_KIND: &str = "sealed-root-checkpoint-cohort";
pub(crate) const ROOT_CHECKPOINT_GATE_VERSION: u32 = 1;
const ROOT_CHECKPOINT_CONTEXT_SCHEMA: &str = "jetstreamer-root-checkpoint-gate-context-v1";

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

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct PublicationReceipt {
    schema: String,
    transaction_id: String,
    manifest_fingerprint: String,
    destination: ReceiptDestination,
    epochs: Vec<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    root_checkpoint_gate: Option<RootCheckpointGateEvidence>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    publication_context_base64: Option<String>,
    outcome: ReceiptOutcome,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RootCheckpointGateEvidence {
    pub kind: String,
    pub version: u32,
    pub verified_manifest_checkpoints: Vec<VerifiedManifestCheckpoint>,
    pub members: Vec<RootCheckpointGateMember>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct VerifiedManifestCheckpoint {
    pub slot: u64,
    pub accounts_hash: [u8; 32],
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RootCheckpointGateMember {
    pub epoch: u64,
    pub bootstrap: RootCheckpointSummary,
    pub terminal: RootCheckpointSummary,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RootCheckpointSummary {
    pub slot: u64,
    pub bank_hash: [u8; 32],
    pub accounts_hash: [u8; 32],
    pub last_blockhash: [u8; 32],
    pub capitalization: u64,
    pub transaction_count: u64,
    pub tick_height: u64,
    pub slot_complete: bool,
    pub write_count: u64,
    pub next_write_version: u64,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct RootCheckpointGateContext {
    schema: String,
    manifest_fingerprint: [u8; 32],
    epochs: Vec<u64>,
    gate: RootCheckpointGateEvidence,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ReceiptDestination {
    path_base64: String,
    device: u64,
    inode: u64,
}

#[derive(Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum ReceiptOutcome {
    Committed { members: Vec<CommittedMember> },
    RolledBack { members: Vec<RolledBackMember> },
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
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

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
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

#[derive(Clone, Copy, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
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

pub(crate) struct AdmittedCommittedCohort {
    pub receipt_path: PathBuf,
    receipt_file: File,
    receipt_identity: ArchiveNamespaceIdentity,
    source_directory: File,
    source_identity: ArchiveBatchDestinationIdentity,
    pub source_path: PathBuf,
    pub transaction_id: [u8; 32],
    pub manifest_fingerprint: [u8; 32],
    pub root_checkpoint_gate: RootCheckpointGateEvidence,
    pub members: Vec<AdmittedCommittedMember>,
}

pub(crate) struct AdmittedCommittedMember {
    pub epoch: u64,
    pub archive_path: PathBuf,
    archive_file: File,
    pub archive_identity: ArchiveNamespaceIdentity,
    pub checksum_path: PathBuf,
    checksum_file: File,
    checksum_identity: ArchiveNamespaceIdentity,
    pub sha256: [u8; 32],
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

impl From<ReceiptIdentity> for ArchiveNamespaceIdentity {
    fn from(identity: ReceiptIdentity) -> Self {
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

impl AdmittedCommittedCohort {
    pub(crate) fn revalidate(&self) -> Result<(), String> {
        let receipt = self.receipt_file.metadata().map_err(|error| {
            format!(
                "failed to recheck source cohort receipt {}: {error}",
                self.receipt_path.display()
            )
        })?;
        if namespace_identity(&receipt) != self.receipt_identity
            || !path_matches_archive_identity(
                &self.receipt_path,
                archive_file_identity(&self.receipt_file).map_err(|error| {
                    format!(
                        "failed to identify source cohort receipt {}: {error}",
                        self.receipt_path.display()
                    )
                })?,
            )
            .map_err(|error| {
                format!(
                    "failed to rebind source cohort receipt {}: {error}",
                    self.receipt_path.display()
                )
            })?
        {
            return Err(format!(
                "source cohort receipt changed after admission: {}",
                self.receipt_path.display()
            ));
        }
        let source = self.source_directory.metadata().map_err(|error| {
            format!(
                "failed to recheck source cohort directory {}: {error}",
                self.source_path.display()
            )
        })?;
        let source_path = fs::symlink_metadata(&self.source_path).map_err(|error| {
            format!(
                "failed to recheck source cohort directory path {}: {error}",
                self.source_path.display()
            )
        })?;
        if !source.file_type().is_dir()
            || !source_path.file_type().is_dir()
            || source.uid() != effective_user_id()
            || source.permissions().mode() & 0o077 != 0
            || source.permissions().mode() & 0o7000 != 0
            || source.dev() != self.source_identity.device
            || source.ino() != self.source_identity.inode
            || source_path.dev() != self.source_identity.device
            || source_path.ino() != self.source_identity.inode
        {
            return Err(format!(
                "source cohort directory changed after admission: {}",
                self.source_path.display()
            ));
        }
        for member in &self.members {
            let metadata = member.archive_file.metadata().map_err(|error| {
                format!(
                    "failed to recheck source cohort archive {}: {error}",
                    member.archive_path.display()
                )
            })?;
            if namespace_identity(&metadata) != member.archive_identity
                || !path_matches_archive_identity(
                    &member.archive_path,
                    archive_file_identity(&member.archive_file).map_err(|error| {
                        format!(
                            "failed to identify source cohort archive {}: {error}",
                            member.archive_path.display()
                        )
                    })?,
                )
                .map_err(|error| {
                    format!(
                        "failed to rebind source cohort archive {}: {error}",
                        member.archive_path.display()
                    )
                })?
            {
                return Err(format!(
                    "source cohort archive changed after admission: {}",
                    member.archive_path.display()
                ));
            }
            let metadata = member.checksum_file.metadata().map_err(|error| {
                format!(
                    "failed to recheck source cohort checksum {}: {error}",
                    member.checksum_path.display()
                )
            })?;
            if namespace_identity(&metadata) != member.checksum_identity
                || !path_matches_archive_identity(
                    &member.checksum_path,
                    archive_file_identity(&member.checksum_file).map_err(|error| {
                        format!(
                            "failed to identify source cohort checksum {}: {error}",
                            member.checksum_path.display()
                        )
                    })?,
                )
                .map_err(|error| {
                    format!(
                        "failed to rebind source cohort checksum {}: {error}",
                        member.checksum_path.display()
                    )
                })?
            {
                return Err(format!(
                    "source cohort checksum changed after admission: {}",
                    member.checksum_path.display()
                ));
            }
        }
        Ok(())
    }
}

pub(crate) fn admit_committed_cohort_receipt(
    receipt_path: &Path,
    expected_manifest_fingerprint: [u8; 32],
    expected_epochs: &[u64],
    target: DestinationBinding<'_>,
) -> Result<AdmittedCommittedCohort, String> {
    validate_destination_binding(target)?;
    validate_contiguous_epoch_sequence(expected_epochs)?;
    let receipt_path = require_canonical_absolute_path(receipt_path, "source cohort receipt")?;
    let receipt_parent = receipt_path
        .parent()
        .ok_or_else(|| "source cohort receipt has no parent directory".to_string())?;
    if receipt_parent.canonicalize().map_err(|error| {
        format!(
            "failed to canonicalize source cohort receipt directory {}: {error}",
            receipt_parent.display()
        )
    })? != receipt_parent
    {
        return Err("source cohort receipt directory traverses a symlink".to_string());
    }
    let receipt_directory = bind_private_receipt_directory(receipt_parent)?;
    let receipt_name = receipt_path
        .file_name()
        .ok_or_else(|| "source cohort receipt has no filename".to_string())?;
    let receipt_name_c = CString::new(receipt_name.as_bytes())
        .map_err(|_| "source cohort receipt filename contains NUL".to_string())?;
    let (mut receipt_file, receipt_bytes) =
        read_receipt_file_at(&receipt_directory, &receipt_name_c)?.ok_or_else(|| {
            format!(
                "source cohort receipt does not exist: {}",
                receipt_path.display()
            )
        })?;
    let receipt_identity = namespace_identity(&receipt_file.metadata().map_err(|error| {
        format!(
            "failed to inspect source cohort receipt {}: {error}",
            receipt_path.display()
        )
    })?);
    let receipt: PublicationReceipt = serde_json::from_slice(&receipt_bytes).map_err(|error| {
        format!(
            "invalid source cohort receipt {}: {error}",
            receipt_path.display()
        )
    })?;
    if receipt.schema != RECEIPT_SCHEMA_V1 && receipt.schema != ROOT_CHECKPOINT_RECEIPT_SCHEMA_V2 {
        return Err(format!(
            "unsupported source cohort receipt schema {}",
            receipt.schema
        ));
    }
    let transaction_id = decode_digest(&receipt.transaction_id, false, "transaction ID")?;
    if transaction_id == [0; 32] {
        return Err("source cohort receipt has a zero transaction ID".to_string());
    }
    let expected_name = OsString::from(format!("batch-{}.json", receipt.transaction_id));
    if receipt_name != expected_name {
        return Err("source cohort receipt filename does not match its transaction ID".to_string());
    }
    let manifest_fingerprint =
        decode_digest(&receipt.manifest_fingerprint, true, "manifest fingerprint")?;
    if manifest_fingerprint != expected_manifest_fingerprint {
        return Err(
            "source cohort receipt manifest fingerprint does not match the audited request"
                .to_string(),
        );
    }
    if receipt.epochs != expected_epochs {
        return Err(format!(
            "source cohort receipt epochs {:?} do not match expected epochs {:?}",
            receipt.epochs, expected_epochs
        ));
    }
    if receipt.schema != ROOT_CHECKPOINT_RECEIPT_SCHEMA_V2 {
        return Err(
            "source receipt is an ordinary v1 batch receipt, not a sealed root-checkpoint receipt"
                .to_string(),
        );
    }
    let root_checkpoint_gate = receipt.root_checkpoint_gate.ok_or_else(|| {
        "source receipt has no sealed root-checkpoint cohort gate evidence".to_string()
    })?;
    validate_root_checkpoint_gate_structure(&root_checkpoint_gate, expected_epochs)?;
    let context_base64 = receipt.publication_context_base64.ok_or_else(|| {
        "source receipt root-checkpoint gate is not bound to a durable batch context".to_string()
    })?;
    let context_bytes = BASE64_STANDARD.decode(&context_base64).map_err(|error| {
        format!("source receipt has invalid publication context base64: {error}")
    })?;
    if context_bytes.len() > MAX_ROOT_CHECKPOINT_CONTEXT_BYTES
        || BASE64_STANDARD.encode(&context_bytes) != context_base64
    {
        return Err("source receipt has oversized or noncanonical publication context".to_string());
    }
    let bound_gate = decode_root_checkpoint_gate_context(
        &context_bytes,
        expected_manifest_fingerprint,
        expected_epochs,
    )?;
    if bound_gate != root_checkpoint_gate {
        return Err(
            "source receipt root-checkpoint evidence differs from its durable batch context"
                .to_string(),
        );
    }
    if context_bytes
        != encode_root_checkpoint_gate_context(
            expected_manifest_fingerprint,
            expected_epochs,
            &root_checkpoint_gate,
        )?
    {
        return Err(
            "source receipt root-checkpoint context is not in its canonical encoding".to_string(),
        );
    }
    let source_path = decode_path_base64(&receipt.destination.path_base64, "source destination")?;
    if source_path.canonicalize().map_err(|error| {
        format!(
            "failed to canonicalize source cohort destination {}: {error}",
            source_path.display()
        )
    })? != source_path
    {
        return Err("source cohort destination traverses a symlink".to_string());
    }
    if source_path == target.path {
        return Err("source cohort destination and public target must differ".to_string());
    }
    let source_directory = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_CLOEXEC | libc::O_DIRECTORY | libc::O_NOFOLLOW)
        .open(&source_path)
        .map_err(|error| {
            format!(
                "failed to bind source cohort destination {}: {error}",
                source_path.display()
            )
        })?;
    let source_metadata = source_directory.metadata().map_err(|error| {
        format!(
            "failed to inspect source cohort destination {}: {error}",
            source_path.display()
        )
    })?;
    let source_path_metadata = fs::symlink_metadata(&source_path).map_err(|error| {
        format!(
            "failed to recheck source cohort destination {}: {error}",
            source_path.display()
        )
    })?;
    if !source_metadata.file_type().is_dir()
        || !source_path_metadata.file_type().is_dir()
        || source_metadata.uid() != effective_user_id()
        || source_metadata.permissions().mode() & 0o077 != 0
        || source_metadata.permissions().mode() & 0o7000 != 0
        || source_metadata.dev() != receipt.destination.device
        || source_metadata.ino() != receipt.destination.inode
        || source_path_metadata.dev() != receipt.destination.device
        || source_path_metadata.ino() != receipt.destination.inode
    {
        return Err(format!(
            "source cohort receipt does not bind an owner-only live destination: {}",
            source_path.display()
        ));
    }
    let expected_receipt_directory_name = OsString::from(format!(
        "destination-{:016x}-{:016x}",
        receipt.destination.device, receipt.destination.inode
    ));
    if receipt_parent.file_name() != Some(expected_receipt_directory_name.as_os_str())
        || receipt_parent
            .parent()
            .and_then(Path::file_name)
            .is_none_or(|name| name != "archive-batches")
    {
        return Err(
            "source cohort receipt is outside its identity-bound archive-batches directory"
                .to_string(),
        );
    }
    let receipt_batches_directory = receipt_parent
        .parent()
        .expect("identity-bound receipt directory has an archive-batches parent");
    let _receipt_batches = bind_private_receipt_directory(receipt_batches_directory)?;
    let target_metadata = fs::symlink_metadata(target.path).map_err(|error| {
        format!(
            "failed to inspect public target {}: {error}",
            target.path.display()
        )
    })?;
    if source_metadata.dev() != target_metadata.dev() {
        return Err(
            "source cohort destination and public target are on different filesystems".to_string(),
        );
    }
    if archive_batch_publication_in_progress(&source_path).map_err(|error| {
        format!(
            "failed to inspect source cohort batch state {}: {error}",
            source_path.display()
        )
    })? {
        return Err(
            "source cohort destination has active or unacknowledged batch state".to_string(),
        );
    }

    let ReceiptOutcome::Committed { members } = receipt.outcome else {
        return Err("source cohort receipt does not record a committed publication".to_string());
    };
    if members.len() != expected_epochs.len() {
        return Err("source cohort receipt has inconsistent member count".to_string());
    }
    let mut admitted_members = Vec::with_capacity(members.len());
    for (&expected_epoch, member) in expected_epochs.iter().zip(members) {
        if member.epoch != expected_epoch {
            return Err(format!(
                "source cohort receipt member epoch {} does not match expected epoch {expected_epoch}",
                member.epoch
            ));
        }
        if member.manifest_path_base64.is_some() || member.committed_manifest.is_some() {
            return Err(format!(
                "source root-cohort receipt unexpectedly contains a segment manifest for epoch {expected_epoch}"
            ));
        }
        if member.initial_archive.is_some()
            || member.initial_manifest.is_some()
            || member.initial_checksum.is_some()
        {
            return Err(format!(
                "source root-cohort receipt replaced an existing namespace for epoch {expected_epoch}"
            ));
        }
        let archive_path = decode_path_base64(&member.archive_path_base64, "source archive")?;
        let expected_archive = source_path.join(format!("epoch-{expected_epoch}.jet"));
        if archive_path != expected_archive {
            return Err(format!(
                "source cohort receipt contains a noncanonical archive path for epoch {expected_epoch}"
            ));
        }
        let checksum_path = decode_path_base64(&member.checksum_path_base64, "source checksum")?;
        let expected_checksum = archive_checksum_path(&expected_archive)
            .map_err(|error| format!("failed to derive source checksum path: {error}"))?;
        if checksum_path != expected_checksum {
            return Err(format!(
                "source cohort receipt contains a noncanonical checksum path for epoch {expected_epoch}"
            ));
        }
        let receipt_archive_identity: ArchiveNamespaceIdentity = member.committed_archive.into();
        let receipt_checksum_identity: ArchiveNamespaceIdentity = member.committed_checksum.into();
        validate_import_identity(receipt_archive_identity, target_metadata.gid(), "archive")?;
        validate_import_identity(receipt_checksum_identity, target_metadata.gid(), "checksum")?;
        // A failed import can move these exact inodes to the public
        // destination and back. Rename changes ctime, so bind the observed
        // post-rollback identities after requiring every stable field from
        // the sealed receipt. Full archive hashing and exact observed-identity
        // revalidation still run before publication.
        let archive_identity =
            require_namespace_identity_across_rename(&archive_path, receipt_archive_identity)?;
        require_namespace_identity(&checksum_path, Some(receipt_checksum_identity))?;
        let checksum_identity = receipt_checksum_identity;
        let sha256 = decode_digest(&member.archive_sha256, false, "archive SHA-256")?;
        let checksum = read_regular_nofollow(&checksum_path)?;
        let expected_checksum_bytes = archive_checksum_line(
            &sha256,
            archive_path
                .file_name()
                .ok_or_else(|| "source archive has no filename".to_string())?,
        )
        .map_err(|error| format!("failed to derive source checksum: {error}"))?;
        if checksum != expected_checksum_bytes.as_bytes() {
            return Err(format!(
                "source cohort checksum does not match epoch {expected_epoch} receipt digest"
            ));
        }
        let archive_file = open_regular_nofollow(&archive_path).map_err(|error| {
            format!(
                "failed to bind source cohort archive {}: {error}",
                archive_path.display()
            )
        })?;
        let checksum_file = open_regular_nofollow(&checksum_path).map_err(|error| {
            format!(
                "failed to bind source cohort checksum {}: {error}",
                checksum_path.display()
            )
        })?;
        admitted_members.push(AdmittedCommittedMember {
            epoch: expected_epoch,
            archive_path,
            archive_file,
            archive_identity,
            checksum_path,
            checksum_file,
            checksum_identity,
            sha256,
        });
    }
    // Keep the parsed receipt descriptor at offset zero for later diagnostics.
    use std::io::Seek as _;
    receipt_file
        .rewind()
        .map_err(|error| format!("failed to rewind source cohort receipt: {error}"))?;
    let admitted = AdmittedCommittedCohort {
        receipt_path,
        receipt_file,
        receipt_identity,
        source_directory,
        source_identity: ArchiveBatchDestinationIdentity {
            device: receipt.destination.device,
            inode: receipt.destination.inode,
        },
        source_path,
        transaction_id,
        manifest_fingerprint,
        root_checkpoint_gate,
        members: admitted_members,
    };
    admitted.revalidate()?;
    Ok(admitted)
}

fn validate_root_checkpoint_gate_structure(
    gate: &RootCheckpointGateEvidence,
    expected_epochs: &[u64],
) -> Result<(), String> {
    if gate.kind != ROOT_CHECKPOINT_GATE_KIND || gate.version != ROOT_CHECKPOINT_GATE_VERSION {
        return Err(format!(
            "source receipt has unsupported root-checkpoint gate {} version {}",
            gate.kind, gate.version
        ));
    }
    if gate.members.len() != expected_epochs.len()
        || gate
            .members
            .iter()
            .zip(expected_epochs)
            .any(|(member, expected)| member.epoch != *expected)
    {
        return Err(
            "source receipt root-checkpoint gate membership does not match its cohort".to_string(),
        );
    }
    if gate.verified_manifest_checkpoints.is_empty()
        || gate
            .verified_manifest_checkpoints
            .windows(2)
            .any(|pair| pair[0].slot >= pair[1].slot)
    {
        return Err(
            "source receipt root-checkpoint gate has no strictly ordered manifest checkpoints"
                .to_string(),
        );
    }
    for member in &gate.members {
        if !member.bootstrap.slot_complete
            || !member.terminal.slot_complete
            || member.terminal.slot < member.bootstrap.slot
        {
            return Err(format!(
                "source receipt root-checkpoint gate has incomplete or reversed checkpoint evidence for epoch {}",
                member.epoch
            ));
        }
    }
    Ok(())
}

pub(crate) fn encode_root_checkpoint_gate_context(
    manifest_fingerprint: [u8; 32],
    epochs: &[u64],
    gate: &RootCheckpointGateEvidence,
) -> Result<Vec<u8>, String> {
    validate_contiguous_epoch_sequence(epochs)?;
    validate_root_checkpoint_gate_structure(gate, epochs)?;
    let encoded = serde_json::to_vec(&RootCheckpointGateContext {
        schema: ROOT_CHECKPOINT_CONTEXT_SCHEMA.to_owned(),
        manifest_fingerprint,
        epochs: epochs.to_vec(),
        gate: gate.clone(),
    })
    .map_err(|error| format!("failed to encode root-checkpoint gate context: {error}"))?;
    if encoded.len() > MAX_ROOT_CHECKPOINT_CONTEXT_BYTES {
        return Err("root-checkpoint gate context exceeds its size limit".to_string());
    }
    Ok(encoded)
}

fn root_checkpoint_gate_from_publication(
    publication: &ArchiveBatchPublication,
) -> Result<Option<RootCheckpointGateEvidence>, String> {
    let Some(context) = publication.publication_context.as_deref() else {
        return Ok(None);
    };
    decode_root_checkpoint_gate_context(
        context,
        publication.manifest_fingerprint,
        &publication.expected_epochs,
    )
    .map(Some)
}

fn decode_root_checkpoint_gate_context(
    context: &[u8],
    expected_manifest_fingerprint: [u8; 32],
    expected_epochs: &[u64],
) -> Result<RootCheckpointGateEvidence, String> {
    if context.len() > MAX_ROOT_CHECKPOINT_CONTEXT_BYTES {
        return Err("durable root-checkpoint gate context exceeds its size limit".to_string());
    }
    let context: RootCheckpointGateContext = serde_json::from_slice(context)
        .map_err(|error| format!("invalid durable root-checkpoint gate context: {error}"))?;
    if context.schema != ROOT_CHECKPOINT_CONTEXT_SCHEMA
        || context.manifest_fingerprint != expected_manifest_fingerprint
        || context.epochs != expected_epochs
    {
        return Err(
            "durable root-checkpoint gate context does not bind its archive batch".to_string(),
        );
    }
    validate_root_checkpoint_gate_structure(&context.gate, expected_epochs)?;
    Ok(context.gate)
}

fn validate_import_identity(
    identity: ArchiveNamespaceIdentity,
    target_gid: u32,
    label: &str,
) -> Result<(), String> {
    if identity.mode & libc::S_IFMT != libc::S_IFREG
        || identity.uid != effective_user_id()
        || identity.gid != target_gid
        || identity.link_count != 1
        || identity.mode & 0o777 != 0o440
        || identity.mode & 0o7000 != 0
    {
        return Err(format!(
            "source cohort receipt contains an unsafe {label} identity"
        ));
    }
    Ok(())
}

fn require_canonical_absolute_path(path: &Path, label: &str) -> Result<PathBuf, String> {
    if !path.is_absolute() {
        return Err(format!("{label} path must be absolute: {}", path.display()));
    }
    let mut rebuilt = PathBuf::from("/");
    for component in path.components().skip(1) {
        let std::path::Component::Normal(name) = component else {
            return Err(format!("{label} path is not canonical: {}", path.display()));
        };
        rebuilt.push(name);
    }
    if rebuilt.as_os_str().as_bytes() != path.as_os_str().as_bytes() {
        return Err(format!("{label} path is not canonical: {}", path.display()));
    }
    Ok(rebuilt)
}

fn decode_path_base64(value: &str, label: &str) -> Result<PathBuf, String> {
    let bytes = BASE64_STANDARD
        .decode(value)
        .map_err(|error| format!("source cohort receipt has invalid {label} base64: {error}"))?;
    if bytes.is_empty() || bytes.contains(&0) {
        return Err(format!("source cohort receipt has invalid {label} path"));
    }
    let path = PathBuf::from(OsString::from_vec(bytes));
    require_canonical_absolute_path(&path, label)
}

fn decode_digest(value: &str, prefixed: bool, label: &str) -> Result<[u8; 32], String> {
    let hex = if prefixed {
        value
            .strip_prefix("sha256:")
            .ok_or_else(|| format!("source cohort receipt {label} lacks sha256 prefix"))?
    } else {
        value
    };
    if hex.len() != 64
        || !hex
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(format!(
            "source cohort receipt {label} must contain 64 lowercase hexadecimal digits"
        ));
    }
    let mut digest = [0u8; 32];
    for (index, output) in digest.iter_mut().enumerate() {
        *output = u8::from_str_radix(&hex[index * 2..index * 2 + 2], 16)
            .map_err(|error| format!("invalid source cohort receipt {label}: {error}"))?;
    }
    Ok(digest)
}

pub(crate) fn recover_pending_batch(
    destination: DestinationBinding<'_>,
    receipt_directory: &Path,
) -> Result<RecoveryDisposition, String> {
    validate_destination_binding(destination)?;
    validate_receipt_location(destination.path, receipt_directory)?;

    let recovered = recover_archive_publication_batch(
        destination.path,
        ArchiveBatchDestinationIdentity {
            device: destination.device,
            inode: destination.inode,
        },
    )
    .map_err(|error| {
        format!(
            "archive batch recovery is indeterminate in {}: {error}",
            destination.path.display()
        )
    })?;
    let (outcome, receipt_path, transaction_id) = match recovered {
        ArchiveBatchRecovery::None => return Ok(RecoveryDisposition::None),
        ArchiveBatchRecovery::UnarmedMarkerCleared => (None, None, None),
        ArchiveBatchRecovery::Committed(publication) => {
            let receipt = committed_receipt(destination, &publication, None, None)?;
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

#[allow(dead_code)]
pub(crate) fn finish_committed_publication(
    destination: DestinationBinding<'_>,
    receipt_directory: &Path,
    expected_manifest_fingerprint: [u8; 32],
    expected: &[ExpectedCohortArchive<'_>],
    publication: &ArchiveBatchPublication,
) -> Result<CompletedPublication, String> {
    if publication.publication_context.is_some() {
        return Err(
            "ordinary archive batch receipt delivery refuses root-checkpoint publication context"
                .to_string(),
        );
    }
    validate_receipt_location(destination.path, receipt_directory)?;
    let receipt = committed_receipt(destination, publication, Some(expected), None)?;
    finish_committed_receipt(
        destination,
        receipt_directory,
        expected_manifest_fingerprint,
        publication,
        receipt,
    )
}

pub(crate) fn finish_root_checkpoint_publication(
    destination: DestinationBinding<'_>,
    receipt_directory: &Path,
    expected_manifest_fingerprint: [u8; 32],
    expected: &[ExpectedCohortArchive<'_>],
    publication: &ArchiveBatchPublication,
    gate: &RootCheckpointGateEvidence,
) -> Result<CompletedPublication, String> {
    validate_receipt_location(destination.path, receipt_directory)?;
    validate_root_checkpoint_gate_structure(gate, &publication.expected_epochs)?;
    let bound_gate = root_checkpoint_gate_from_publication(publication)?
        .ok_or_else(|| "root-checkpoint publication has no durable gate context".to_string())?;
    if &bound_gate != gate {
        return Err(
            "root-checkpoint publication durable gate context changed before receipt delivery"
                .to_string(),
        );
    }
    let receipt = committed_receipt(destination, publication, Some(expected), Some(gate))?;
    finish_committed_receipt(
        destination,
        receipt_directory,
        expected_manifest_fingerprint,
        publication,
        receipt,
    )
}

fn finish_committed_receipt(
    destination: DestinationBinding<'_>,
    receipt_directory: &Path,
    expected_manifest_fingerprint: [u8; 32],
    publication: &ArchiveBatchPublication,
    receipt: PublicationReceipt,
) -> Result<CompletedPublication, String> {
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
    root_checkpoint_gate: Option<&RootCheckpointGateEvidence>,
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

    let bound_root_checkpoint_gate = root_checkpoint_gate_from_publication(publication)?;
    if root_checkpoint_gate
        .is_some_and(|expected| Some(expected) != bound_root_checkpoint_gate.as_ref())
    {
        return Err(
            "archive batch result does not bind the required root-checkpoint gate evidence"
                .to_string(),
        );
    }
    Ok(PublicationReceipt {
        schema: if bound_root_checkpoint_gate.is_some() {
            ROOT_CHECKPOINT_RECEIPT_SCHEMA_V2.to_owned()
        } else {
            RECEIPT_SCHEMA_V1.to_owned()
        },
        transaction_id: digest_hex(&publication.transaction_id),
        manifest_fingerprint: format!("sha256:{}", digest_hex(&publication.manifest_fingerprint)),
        destination: ReceiptDestination {
            path_base64: path_base64(destination.path),
            device: destination.device,
            inode: destination.inode,
        },
        epochs,
        root_checkpoint_gate: bound_root_checkpoint_gate,
        publication_context_base64: publication
            .publication_context
            .as_deref()
            .map(|context| BASE64_STANDARD.encode(context)),
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
        schema: RECEIPT_SCHEMA_V1.to_owned(),
        transaction_id: digest_hex(&rollback.transaction_id),
        manifest_fingerprint: format!("sha256:{}", digest_hex(&rollback.manifest_fingerprint)),
        destination: ReceiptDestination {
            path_base64: path_base64(destination.path),
            device: destination.device,
            inode: destination.inode,
        },
        epochs,
        root_checkpoint_gate: None,
        publication_context_base64: None,
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

fn require_namespace_identity_across_rename(
    path: &Path,
    expected: ArchiveNamespaceIdentity,
) -> Result<ArchiveNamespaceIdentity, String> {
    let metadata = fs::symlink_metadata(path).map_err(|error| {
        format!(
            "failed to inspect source cohort path {}: {error}",
            path.display()
        )
    })?;
    let observed = namespace_identity(&metadata);
    if !metadata.file_type().is_file()
        || observed.device != expected.device
        || observed.inode != expected.inode
        || observed.mode != expected.mode
        || observed.uid != expected.uid
        || observed.gid != expected.gid
        || observed.link_count != expected.link_count
        || observed.length != expected.length
        || observed.modified_seconds != expected.modified_seconds
        || observed.modified_nanoseconds != expected.modified_nanoseconds
    {
        return Err(format!(
            "source cohort identity changed beyond a rename: {}",
            path.display()
        ));
    }
    Ok(observed)
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
    persist_receipt_with_directory_sync(directory, receipt, File::sync_all)
}

fn persist_receipt_with_directory_sync(
    directory: &Path,
    receipt: &PublicationReceipt,
    mut sync_directory: impl FnMut(&File) -> io::Result<()>,
) -> Result<PathBuf, String> {
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
            sync_and_revalidate_receipt(
                directory,
                &directory_file,
                &final_name,
                &bytes,
                &mut sync_directory,
            )?;
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
            sync_and_revalidate_receipt(
                directory,
                &directory_file,
                &final_name,
                &bytes,
                &mut sync_directory,
            )?;
            return Ok(final_path);
        }
        return Err(format!(
            "failed to install private archive batch receipt {}: {error}",
            final_path.display()
        ));
    }
    sync_and_revalidate_receipt(
        directory,
        &directory_file,
        &final_name,
        &bytes,
        &mut sync_directory,
    )?;
    Ok(final_path)
}

fn sync_and_revalidate_receipt(
    directory_path: &Path,
    directory: &File,
    name: &CString,
    expected: &[u8],
    sync_directory: &mut impl FnMut(&File) -> io::Result<()>,
) -> Result<(), String> {
    sync_directory(directory).map_err(|error| {
        format!(
            "failed to sync archive batch receipt directory {}: {error}",
            directory_path.display()
        )
    })?;
    let installed = read_receipt_at(directory, name)?.ok_or_else(|| {
        format!(
            "archive batch receipt disappeared after installation: {}",
            directory_path
                .join(OsStr::from_bytes(name.as_bytes()))
                .display()
        )
    })?;
    if installed != expected {
        return Err(format!(
            "existing archive batch receipt disagrees with recovered evidence: {}",
            directory_path
                .join(OsStr::from_bytes(name.as_bytes()))
                .display()
        ));
    }
    Ok(())
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
        || metadata.permissions().mode() & 0o7000 != 0
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
    Ok(read_receipt_file_at(directory, name)?.map(|(_, bytes)| bytes))
}

fn read_receipt_file_at(
    directory: &File,
    name: &CString,
) -> Result<Option<(File, Vec<u8>)>, String> {
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
        || initial.permissions().mode() & 0o7000 != 0
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
    Ok(Some((file, bytes)))
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
    use std::time::{Duration, Instant};

    fn retry_after_transient_fork_lock<T>(
        mut operation: impl FnMut() -> Result<T, String>,
    ) -> Result<T, String> {
        // The bin tests run worker-spawn tests in parallel. A fork that lands
        // while this test's publisher owns the destination flock inherits the
        // open file description until exec closes its O_CLOEXEC descriptor.
        // Retry only that contention result; receipt delivery is idempotent and
        // transaction-ID-bound, while every other failure remains immediate.
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            match operation() {
                Err(error)
                    if error.contains("failed to acquire archive destination writer lock:")
                        && error.contains("Resource temporarily unavailable")
                        && Instant::now() < deadline =>
                {
                    std::thread::sleep(Duration::from_millis(1));
                }
                result => return result,
            }
        }
    }

    fn recover_after_transient_fork_lock(
        destination: DestinationBinding<'_>,
        receipt_directory: &Path,
    ) -> Result<RecoveryDisposition, String> {
        retry_after_transient_fork_lock(|| recover_pending_batch(destination, receipt_directory))
    }

    fn private_directory(parent: &Path, name: &str) -> PathBuf {
        let path = parent.join(name);
        let mut builder = fs::DirBuilder::new();
        builder.mode(0o700).create(&path).unwrap();
        path
    }

    fn sample_receipt(destination: &Path, metadata: &fs::Metadata) -> PublicationReceipt {
        PublicationReceipt {
            schema: RECEIPT_SCHEMA_V1.to_owned(),
            transaction_id: digest_hex(&[0x11; 32]),
            manifest_fingerprint: format!("sha256:{}", digest_hex(&[0x22; 32])),
            destination: ReceiptDestination {
                path_base64: path_base64(destination),
                device: metadata.dev(),
                inode: metadata.ino(),
            },
            epochs: vec![17, 18, 19],
            root_checkpoint_gate: None,
            publication_context_base64: None,
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

    fn identity_bound_receipt_directory(root: &Path, destination: &Path) -> PathBuf {
        let batches = private_directory(root, "archive-batches");
        let metadata = fs::metadata(destination).unwrap();
        private_directory(
            &batches,
            &format!(
                "destination-{:016x}-{:016x}",
                metadata.dev(),
                metadata.ino()
            ),
        )
    }

    fn sample_root_gate(epochs: &[u64]) -> RootCheckpointGateEvidence {
        let summary = |slot, byte| RootCheckpointSummary {
            slot,
            bank_hash: [byte; 32],
            accounts_hash: [byte.wrapping_add(1); 32],
            last_blockhash: [byte.wrapping_add(2); 32],
            capitalization: 1,
            transaction_count: 2,
            tick_height: 3,
            slot_complete: true,
            write_count: 4,
            next_write_version: 5,
        };
        RootCheckpointGateEvidence {
            kind: ROOT_CHECKPOINT_GATE_KIND.to_owned(),
            version: ROOT_CHECKPOINT_GATE_VERSION,
            verified_manifest_checkpoints: vec![VerifiedManifestCheckpoint {
                slot: 99,
                accounts_hash: [0x77; 32],
            }],
            members: epochs
                .iter()
                .enumerate()
                .map(|(index, epoch)| RootCheckpointGateMember {
                    epoch: *epoch,
                    bootstrap: summary(index as u64, index as u8),
                    terminal: summary(index as u64 + 1, index as u8 + 10),
                })
                .collect(),
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
    fn identical_existing_receipt_is_synced_then_revalidated() {
        let fixture = tempfile::tempdir_in(".").unwrap();
        let destination = private_directory(fixture.path(), "destination");
        let receipts = private_directory(fixture.path(), "receipts");
        let metadata = fs::metadata(&destination).unwrap();
        let receipt = sample_receipt(&destination, &metadata);
        let path = persist_receipt(&receipts, &receipt).unwrap();

        let error = persist_receipt_with_directory_sync(&receipts, &receipt, |_| {
            Err(io::Error::other("injected directory sync failure"))
        })
        .unwrap_err();
        assert!(error.contains("injected directory sync failure"), "{error}");

        let expected = fs::read(&path).unwrap();
        let changed_path = path.clone();
        let error = persist_receipt_with_directory_sync(&receipts, &receipt, |directory| {
            directory.sync_all()?;
            fs::write(&changed_path, b"changed after directory sync")
        })
        .unwrap_err();
        assert!(error.contains("disagrees"), "{error}");
        assert_ne!(fs::read(path).unwrap(), expected);
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
    fn root_receipt_admission_rejects_zero_transaction_id() {
        let fixture = tempfile::tempdir_in(".").unwrap();
        let root = fixture.path().canonicalize().unwrap();
        let source = private_directory(&root, "source");
        let target = private_directory(&root, "target");
        let receipts = identity_bound_receipt_directory(&root, &source);
        let metadata = fs::metadata(&source).unwrap();
        let epochs = [17, 18, 19];
        let fingerprint = [0x22; 32];
        let gate = sample_root_gate(&epochs);
        let context = encode_root_checkpoint_gate_context(fingerprint, &epochs, &gate).unwrap();
        let mut receipt = sample_receipt(&source, &metadata);
        receipt.schema = ROOT_CHECKPOINT_RECEIPT_SCHEMA_V2.to_owned();
        receipt.transaction_id = digest_hex(&[0; 32]);
        receipt.root_checkpoint_gate = Some(gate);
        receipt.publication_context_base64 = Some(BASE64_STANDARD.encode(context));
        let receipt_path = persist_receipt(&receipts, &receipt).unwrap();

        let error = match admit_committed_cohort_receipt(
            &receipt_path,
            fingerprint,
            &epochs,
            destination_binding(&target),
        ) {
            Ok(_) => panic!("zero transaction ID was admitted"),
            Err(error) => error,
        };
        assert!(error.contains("zero transaction ID"), "{error}");
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
            recover_after_transient_fork_lock(destination_binding(&destination), &receipts)
                .unwrap();
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
            recover_after_transient_fork_lock(destination_binding(&destination), &receipts)
                .unwrap(),
            RecoveryDisposition::None
        );
    }

    #[test]
    fn clearing_an_unarmed_marker_requires_a_clean_run() {
        let fixture = tempfile::tempdir_in(".").unwrap();
        let root = fixture.path().canonicalize().unwrap();
        let destination = private_directory(&root, "destination");
        let receipts = private_directory(&root, "receipts");
        let marker = destination
            .join(jetstreamer_node::archive_checksum::ARCHIVE_BATCH_TRANSACTION_DIRECTORY);
        let mut builder = fs::DirBuilder::new();
        builder.mode(0o700).create(&marker).unwrap();

        assert_eq!(
            recover_after_transient_fork_lock(destination_binding(&destination), &receipts)
                .unwrap(),
            RecoveryDisposition::Stop {
                outcome: None,
                receipt_path: None,
            }
        );
        assert!(!marker.exists());
        assert_eq!(
            recover_after_transient_fork_lock(destination_binding(&destination), &receipts)
                .unwrap(),
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

        let completed = retry_after_transient_fork_lock(|| {
            finish_committed_publication(
                destination_binding(&destination),
                &receipts,
                fingerprint,
                &expected,
                &publication,
            )
        })
        .unwrap();
        assert_eq!(completed.archive_count, 2);
        assert!(completed.receipt_path.is_file());
        assert!(!archive_batch_publication_in_progress(&destination).unwrap());
    }

    #[test]
    fn committed_receipt_admission_binds_members_and_detects_replacement() {
        let fixture = tempfile::tempdir_in(".").unwrap();
        let root = fixture.path().canonicalize().unwrap();
        let staging = private_directory(&root, "staging");
        let source = private_directory(&root, "source");
        let target = private_directory(&root, "target");
        let receipts = identity_bound_receipt_directory(&root, &source);
        let items = staged_batch_items(&staging, &source, &[31, 32]);
        let fingerprint = [0x24; 32];
        let expected_epochs = vec![31, 32];
        let gate = sample_root_gate(&expected_epochs);
        let gate_context =
            encode_root_checkpoint_gate_context(fingerprint, &expected_epochs, &gate).unwrap();
        let expected = items
            .iter()
            .map(|item| ExpectedCohortArchive {
                epoch: item.epoch,
                destination_archive: &item.destination_archive,
                sha256: item.evidence.sha256,
            })
            .collect::<Vec<_>>();
        let publication =
            jetstreamer_node::archive_publish::publish_verified_archive_batch_with_context(
                fingerprint,
                &expected_epochs,
                &items,
                &gate_context,
            )
            .unwrap();
        let receipt = retry_after_transient_fork_lock(|| {
            finish_root_checkpoint_publication(
                destination_binding(&source),
                &receipts,
                fingerprint,
                &expected,
                &publication,
                &gate,
            )
        })
        .unwrap();

        let admitted = admit_committed_cohort_receipt(
            &receipt.receipt_path,
            fingerprint,
            &expected_epochs,
            destination_binding(&target),
        )
        .unwrap();
        assert_eq!(admitted.transaction_id, publication.transaction_id);
        assert_eq!(admitted.manifest_fingerprint, fingerprint);
        assert_eq!(
            admitted
                .members
                .iter()
                .map(|member| member.epoch)
                .collect::<Vec<_>>(),
            expected_epochs
        );
        admitted.revalidate().unwrap();

        fs::remove_file(&admitted.members[0].checksum_path).unwrap();
        fs::write(&admitted.members[0].checksum_path, b"replacement").unwrap();
        fs::set_permissions(
            &admitted.members[0].checksum_path,
            fs::Permissions::from_mode(0o440),
        )
        .unwrap();
        let error = admitted.revalidate().unwrap_err();
        assert!(
            error.contains("checksum changed after admission"),
            "{error}"
        );
    }

    #[test]
    fn committed_receipt_admission_rebinds_members_after_rollback_renames() {
        let fixture = tempfile::tempdir_in(".").unwrap();
        let root = fixture.path().canonicalize().unwrap();
        let staging = private_directory(&root, "staging");
        let source = private_directory(&root, "source");
        let target = private_directory(&root, "target");
        let receipts = identity_bound_receipt_directory(&root, &source);
        let items = staged_batch_items(&staging, &source, &[31]);
        let fingerprint = [0x24; 32];
        let gate = sample_root_gate(&[31]);
        let gate_context = encode_root_checkpoint_gate_context(fingerprint, &[31], &gate).unwrap();
        let expected = [ExpectedCohortArchive {
            epoch: 31,
            destination_archive: &items[0].destination_archive,
            sha256: items[0].evidence.sha256,
        }];
        let publication =
            jetstreamer_node::archive_publish::publish_verified_archive_batch_with_context(
                fingerprint,
                &[31],
                &items,
                &gate_context,
            )
            .unwrap();
        let receipt = retry_after_transient_fork_lock(|| {
            finish_root_checkpoint_publication(
                destination_binding(&source),
                &receipts,
                fingerprint,
                &expected,
                &publication,
                &gate,
            )
        })
        .unwrap();

        let archive = &items[0].destination_archive;
        let checksum = archive_checksum_path(archive).unwrap();
        let receipt_json: PublicationReceipt =
            serde_json::from_slice(&fs::read(&receipt.receipt_path).unwrap()).unwrap();
        let ReceiptOutcome::Committed { members } = receipt_json.outcome else {
            panic!("source receipt was not committed");
        };
        let recorded_archive: ArchiveNamespaceIdentity = members[0].committed_archive.into();
        let recorded_checksum: ArchiveNamespaceIdentity = members[0].committed_checksum.into();

        let parked_archive = staging.join("rollback-archive");
        fs::rename(archive, &parked_archive).unwrap();
        fs::rename(&parked_archive, archive).unwrap();

        let observed_archive = namespace_identity(&fs::metadata(archive).unwrap());
        let observed_checksum = namespace_identity(&fs::metadata(&checksum).unwrap());
        assert_ne!(
            (
                recorded_archive.changed_seconds,
                recorded_archive.changed_nanoseconds,
            ),
            (
                observed_archive.changed_seconds,
                observed_archive.changed_nanoseconds,
            )
        );
        assert_eq!(recorded_checksum, observed_checksum);

        let admitted = admit_committed_cohort_receipt(
            &receipt.receipt_path,
            fingerprint,
            &[31],
            destination_binding(&target),
        )
        .unwrap();
        assert_eq!(admitted.members[0].archive_identity, observed_archive);
        admitted.revalidate().unwrap();
        drop(admitted);

        fs::set_permissions(archive, fs::Permissions::from_mode(0o600)).unwrap();
        let error = match admit_committed_cohort_receipt(
            &receipt.receipt_path,
            fingerprint,
            &[31],
            destination_binding(&target),
        ) {
            Ok(_) => panic!("source archive mode change was admitted as a rename"),
            Err(error) => error,
        };
        assert!(error.contains("changed beyond a rename"), "{error}");
        fs::set_permissions(archive, fs::Permissions::from_mode(0o440)).unwrap();

        let parked_checksum = staging.join("rollback-checksum");
        fs::rename(&checksum, &parked_checksum).unwrap();
        fs::rename(&parked_checksum, &checksum).unwrap();
        let error = match admit_committed_cohort_receipt(
            &receipt.receipt_path,
            fingerprint,
            &[31],
            destination_binding(&target),
        ) {
            Ok(_) => panic!("source checksum ctime drift was admitted as a rollback rename"),
            Err(error) => error,
        };
        assert!(error.contains("identity changed"), "{error}");
    }

    #[test]
    fn committed_receipt_admission_rejects_wrong_fingerprint() {
        let fixture = tempfile::tempdir_in(".").unwrap();
        let root = fixture.path().canonicalize().unwrap();
        let staging = private_directory(&root, "staging");
        let source = private_directory(&root, "source");
        let target = private_directory(&root, "target");
        let receipts = identity_bound_receipt_directory(&root, &source);
        let items = staged_batch_items(&staging, &source, &[31]);
        let fingerprint = [0x24; 32];
        let gate = sample_root_gate(&[31]);
        let gate_context = encode_root_checkpoint_gate_context(fingerprint, &[31], &gate).unwrap();
        let expected = [ExpectedCohortArchive {
            epoch: 31,
            destination_archive: &items[0].destination_archive,
            sha256: items[0].evidence.sha256,
        }];
        let publication =
            jetstreamer_node::archive_publish::publish_verified_archive_batch_with_context(
                fingerprint,
                &[31],
                &items,
                &gate_context,
            )
            .unwrap();
        let receipt = retry_after_transient_fork_lock(|| {
            finish_root_checkpoint_publication(
                destination_binding(&source),
                &receipts,
                fingerprint,
                &expected,
                &publication,
                &gate,
            )
        })
        .unwrap();

        let error = match admit_committed_cohort_receipt(
            &receipt.receipt_path,
            [0x25; 32],
            &[31],
            destination_binding(&target),
        ) {
            Ok(_) => panic!("wrong manifest fingerprint was admitted"),
            Err(error) => error,
        };
        assert!(error.contains("fingerprint does not match"), "{error}");
    }

    #[test]
    fn ordinary_v1_receipt_is_compatible_but_not_root_import_admissible() {
        let fixture = tempfile::tempdir_in(".").unwrap();
        let root = fixture.path().canonicalize().unwrap();
        let staging = private_directory(&root, "staging");
        let source = private_directory(&root, "source");
        let target = private_directory(&root, "target");
        let receipts = identity_bound_receipt_directory(&root, &source);
        let items = staged_batch_items(&staging, &source, &[31]);
        let fingerprint = [0x24; 32];
        let expected = [ExpectedCohortArchive {
            epoch: 31,
            destination_archive: &items[0].destination_archive,
            sha256: items[0].evidence.sha256,
        }];
        let publication = jetstreamer_node::archive_publish::publish_verified_archive_batch(
            fingerprint,
            &[31],
            &items,
        )
        .unwrap();
        let receipt = retry_after_transient_fork_lock(|| {
            finish_committed_publication(
                destination_binding(&source),
                &receipts,
                fingerprint,
                &expected,
                &publication,
            )
        })
        .unwrap();

        let error = match admit_committed_cohort_receipt(
            &receipt.receipt_path,
            fingerprint,
            &[31],
            destination_binding(&target),
        ) {
            Ok(_) => panic!("ordinary v1 batch receipt was admitted as a root cohort"),
            Err(error) => error,
        };
        assert!(error.contains("ordinary v1 batch receipt"), "{error}");
    }

    #[test]
    fn committed_batch_recovery_preserves_durable_root_gate_context() {
        let fixture = tempfile::tempdir_in(".").unwrap();
        let root = fixture.path().canonicalize().unwrap();
        let staging = private_directory(&root, "staging");
        let source = private_directory(&root, "source");
        let target = private_directory(&root, "target");
        let receipts = identity_bound_receipt_directory(&root, &source);
        let items = staged_batch_items(&staging, &source, &[31]);
        let fingerprint = [0x24; 32];
        let gate = sample_root_gate(&[31]);
        let context = encode_root_checkpoint_gate_context(fingerprint, &[31], &gate).unwrap();
        let expected = [ExpectedCohortArchive {
            epoch: 31,
            destination_archive: &items[0].destination_archive,
            sha256: items[0].evidence.sha256,
        }];
        let publication =
            jetstreamer_node::archive_publish::publish_verified_archive_batch_with_context(
                fingerprint,
                &[31],
                &items,
                &context,
            )
            .unwrap();
        let error = finish_committed_publication(
            destination_binding(&source),
            &receipts,
            fingerprint,
            &expected,
            &publication,
        )
        .unwrap_err();
        assert!(error.contains("ordinary archive batch"), "{error}");
        assert!(archive_batch_publication_in_progress(&source).unwrap());
        drop(publication);

        let disposition =
            recover_after_transient_fork_lock(destination_binding(&source), &receipts).unwrap();
        let RecoveryDisposition::Stop {
            outcome: Some(RecoveredOutcome::Committed),
            receipt_path: Some(receipt_path),
        } = disposition
        else {
            panic!("committed root batch was not recovered");
        };
        assert!(!archive_batch_publication_in_progress(&source).unwrap());
        let receipt_json: serde_json::Value =
            serde_json::from_slice(&fs::read(&receipt_path).unwrap()).unwrap();
        assert_eq!(receipt_json["schema"], ROOT_CHECKPOINT_RECEIPT_SCHEMA_V2);
        assert!(receipt_json["publication_context_base64"].is_string());
        let admitted = admit_committed_cohort_receipt(
            &receipt_path,
            fingerprint,
            &[31],
            destination_binding(&target),
        )
        .unwrap();
        assert_eq!(admitted.root_checkpoint_gate, gate);
    }

    #[test]
    fn root_finish_rejects_receipt_directory_inside_destination() {
        let fixture = tempfile::tempdir_in(".").unwrap();
        let root = fixture.path().canonicalize().unwrap();
        let staging = private_directory(&root, "staging");
        let destination = private_directory(&root, "destination");
        let invalid_receipts = private_directory(&destination, "receipts");
        let items = staged_batch_items(&staging, &destination, &[31]);
        let fingerprint = [0x24; 32];
        let gate = sample_root_gate(&[31]);
        let context = encode_root_checkpoint_gate_context(fingerprint, &[31], &gate).unwrap();
        let expected = [ExpectedCohortArchive {
            epoch: 31,
            destination_archive: &items[0].destination_archive,
            sha256: items[0].evidence.sha256,
        }];
        let publication =
            jetstreamer_node::archive_publish::publish_verified_archive_batch_with_context(
                fingerprint,
                &[31],
                &items,
                &context,
            )
            .unwrap();

        let error = finish_root_checkpoint_publication(
            destination_binding(&destination),
            &invalid_receipts,
            fingerprint,
            &expected,
            &publication,
            &gate,
        )
        .unwrap_err();
        assert!(error.contains("must not be stored"), "{error}");
        assert!(archive_batch_publication_in_progress(&destination).unwrap());
    }

    #[test]
    fn malformed_committed_context_remains_unacknowledged() {
        let fixture = tempfile::tempdir_in(".").unwrap();
        let root = fixture.path().canonicalize().unwrap();
        let staging = private_directory(&root, "staging");
        let source = private_directory(&root, "source");
        let receipts = identity_bound_receipt_directory(&root, &source);
        let items = staged_batch_items(&staging, &source, &[31]);
        drop(
            jetstreamer_node::archive_publish::publish_verified_archive_batch_with_context(
                [0x24; 32],
                &[31],
                &items,
                b"not a root gate context",
            )
            .unwrap(),
        );

        let error =
            recover_after_transient_fork_lock(destination_binding(&source), &receipts).unwrap_err();
        assert!(error.contains("invalid durable root-checkpoint"), "{error}");
        assert!(archive_batch_publication_in_progress(&source).unwrap());
        assert!(fs::read_dir(&receipts).unwrap().next().is_none());
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
            recover_after_transient_fork_lock(destination_binding(&destination), &receipts)
                .unwrap(),
            RecoveryDisposition::Stop {
                outcome: Some(RecoveredOutcome::Committed),
                ..
            }
        ));
        assert!(!archive_batch_publication_in_progress(&destination).unwrap());
    }
}
