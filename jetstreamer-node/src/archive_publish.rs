//! Transactional publication of a fully validated Horizon archive.
//!
//! A canonical checksum sidecar is the commit marker. Publication atomically
//! replaces any previous checksum with an invalid, publisher-owned sentinel,
//! installs the optional segment manifest and archive, checks their exact
//! inodes again, and exchanges the canonical checksum into place last.
//! Existing files are retained in a private recovery directory.
//!
//! A single-archive mutation journal is process-local, so an interrupted
//! single publication requires manual reconciliation. Ordered cohort
//! publication adds a durable, identity-bound journal and a commit decision.
//! Startup recovery rolls the whole cohort back before that decision, or
//! finishes every checksum after it.

use {
    crate::{
        archive_checksum::{
            ARCHIVE_BATCH_OUTCOME_DIRECTORY, ARCHIVE_BATCH_TRANSACTION_DIRECTORY,
            ARCHIVE_PUBLICATION_SENTINEL, PersistedValidatedArchiveFile, ValidatedArchiveFile,
            archive_batch_publication_in_progress, archive_checksum_line, archive_checksum_path,
            archive_file_identity, path_matches_archive_identity, rebind_validated_after_rename,
        },
        segment_manifest::{
            HistoricalSegmentManifest, read_and_validate_segment_manifest, segment_manifest_path,
        },
    },
    serde::{Deserialize, Serialize},
    sha2::{Digest, Sha256},
    std::{
        collections::HashSet,
        ffi::{CStr, CString, OsStr},
        fmt,
        fs::{self, File, OpenOptions},
        io::{self, Read, Write},
        mem::MaybeUninit,
        os::{
            fd::{AsRawFd as _, FromRawFd as _, IntoRawFd as _, RawFd},
            unix::{
                ffi::{OsStrExt as _, OsStringExt as _},
                fs::{MetadataExt as _, OpenOptionsExt as _, PermissionsExt as _},
            },
        },
        path::{Path, PathBuf},
        time::{SystemTime, UNIX_EPOCH},
    },
};

const MAX_SEGMENT_MANIFEST_BYTES: u64 = 1 << 20;
const FINAL_FILE_MODE: u32 = 0o440;
const CAPABILITY_PROBE_A: &str = "rename-probe-a";
const CAPABILITY_PROBE_B: &str = "rename-probe-b";
const CAPABILITY_PROBE_MOVED: &str = "rename-probe-moved";
const ARCHIVE_BATCH_JOURNAL: &str = "journal.json";
const ARCHIVE_BATCH_JOURNAL_NEXT: &str = "journal.next.json";
const ARCHIVE_BATCH_ARMED: &str = "armed";
const ARCHIVE_BATCH_ARMED_PREFIX: &[u8] = b"jetstreamer archive batch armed v1\0";
const ARCHIVE_BATCH_JOURNAL_VERSION: u32 = 1;
const MAX_ARCHIVE_BATCH_ITEMS: usize = 4096;
const MAX_ARCHIVE_BATCH_JOURNAL_BYTES: u64 = 8 << 20;

#[derive(Debug)]
pub struct ArchivePublication {
    pub archive_path: PathBuf,
    pub manifest_path: Option<PathBuf>,
    pub checksum_path: PathBuf,
    pub recovery_directory: Option<PathBuf>,
    pub evidence: ValidatedArchiveFile,
}

/// One fully validated member of an ordered archive publication cohort.
///
/// Every destination in a batch must share one directory, and the entries must
/// be ordered by strictly increasing epoch. The staged and destination
/// basenames must be the same.
#[derive(Clone, Debug)]
pub struct ArchiveBatchItem {
    pub epoch: u64,
    pub staged_archive: PathBuf,
    pub destination_archive: PathBuf,
    pub evidence: ValidatedArchiveFile,
}

#[derive(Debug)]
pub struct ArchiveBatchPublication {
    pub transaction_id: [u8; 32],
    pub manifest_fingerprint: [u8; 32],
    pub destination_identity: ArchiveBatchDestinationIdentity,
    /// Publications in the same strictly increasing epoch order as the input.
    pub publications: Vec<ArchivePublication>,
    /// Exact initial and committed namespace identities for every member.
    pub identity_evidence: Vec<ArchiveBatchIdentityEvidence>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ArchiveBatchDestinationIdentity {
    pub device: u64,
    pub inode: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ArchiveNamespaceIdentity {
    pub device: u64,
    pub inode: u64,
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
    pub link_count: u64,
    pub length: u64,
    pub modified_seconds: i64,
    pub modified_nanoseconds: i64,
    pub changed_seconds: i64,
    pub changed_nanoseconds: i64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ArchiveBatchIdentityEvidence {
    pub epoch: u64,
    pub initial_archive: Option<ArchiveNamespaceIdentity>,
    pub initial_manifest: Option<ArchiveNamespaceIdentity>,
    pub initial_checksum: Option<ArchiveNamespaceIdentity>,
    pub committed_archive: ArchiveNamespaceIdentity,
    pub committed_manifest: Option<ArchiveNamespaceIdentity>,
    pub committed_checksum: ArchiveNamespaceIdentity,
    pub archive_validation: ValidatedArchiveFile,
}

#[derive(Debug)]
pub struct ArchiveBatchRollback {
    pub transaction_id: [u8; 32],
    pub manifest_fingerprint: [u8; 32],
    pub destination_identity: ArchiveBatchDestinationIdentity,
    pub identity_evidence: Vec<ArchiveBatchRollbackIdentityEvidence>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ArchiveBatchRollbackIdentityEvidence {
    pub epoch: u64,
    pub staged_archive_path: PathBuf,
    pub restored_archive: Option<ArchiveNamespaceIdentity>,
    pub restored_manifest: Option<ArchiveNamespaceIdentity>,
    pub restored_checksum: Option<ArchiveNamespaceIdentity>,
    pub staged_archive: ArchiveNamespaceIdentity,
    pub staged_archive_validation: ValidatedArchiveFile,
}

#[derive(Debug)]
pub enum ArchiveBatchRecovery {
    None,
    RolledBack(ArchiveBatchRollback),
    Committed(ArchiveBatchPublication),
}

#[derive(Debug)]
pub struct ArchivePublicationError {
    message: String,
    committed: bool,
    recovery_directory: Option<PathBuf>,
}

impl ArchivePublicationError {
    pub fn committed(&self) -> bool {
        self.committed
    }

    pub fn recovery_directory(&self) -> Option<&Path> {
        self.recovery_directory.as_deref()
    }
}

impl fmt::Display for ArchivePublicationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for ArchivePublicationError {}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct FileIdentity {
    dev: u64,
    ino: u64,
    mode: u32,
    uid: u32,
    gid: u32,
    nlink: u64,
    len: u64,
    mtime: i64,
    mtime_nsec: i64,
    ctime: i64,
    ctime_nsec: i64,
}

impl FileIdentity {
    fn from_metadata(metadata: &fs::Metadata) -> Self {
        Self {
            dev: metadata.dev(),
            ino: metadata.ino(),
            mode: metadata.mode(),
            uid: metadata.uid(),
            gid: metadata.gid(),
            nlink: metadata.nlink(),
            len: metadata.len(),
            mtime: metadata.mtime(),
            mtime_nsec: metadata.mtime_nsec(),
            ctime: metadata.ctime(),
            ctime_nsec: metadata.ctime_nsec(),
        }
    }

    fn from_stat(stat: &libc::stat) -> Self {
        Self {
            dev: stat.st_dev,
            ino: stat.st_ino,
            mode: stat.st_mode,
            uid: stat.st_uid,
            gid: stat.st_gid,
            nlink: stat.st_nlink,
            len: stat.st_size as u64,
            mtime: stat.st_mtime,
            mtime_nsec: stat.st_mtime_nsec,
            ctime: stat.st_ctime,
            ctime_nsec: stat.st_ctime_nsec,
        }
    }

    /// A rename updates ctime while preserving the inode, contents, ownership,
    /// link count, permissions, and mtime.
    fn same_across_rename(self, other: Self) -> bool {
        self.dev == other.dev
            && self.ino == other.ino
            && self.mode == other.mode
            && self.uid == other.uid
            && self.gid == other.gid
            && self.nlink == other.nlink
            && self.len == other.len
            && self.mtime == other.mtime
            && self.mtime_nsec == other.mtime_nsec
    }

    fn same_directory_binding(self, other: Self) -> bool {
        self.dev == other.dev
            && self.ino == other.ino
            && self.mode == other.mode
            && self.uid == other.uid
            && self.gid == other.gid
    }
}

impl From<FileIdentity> for ArchiveNamespaceIdentity {
    fn from(identity: FileIdentity) -> Self {
        Self {
            device: identity.dev,
            inode: identity.ino,
            mode: identity.mode,
            uid: identity.uid,
            gid: identity.gid,
            link_count: identity.nlink,
            length: identity.len,
            modified_seconds: identity.mtime,
            modified_nanoseconds: identity.mtime_nsec,
            changed_seconds: identity.ctime,
            changed_nanoseconds: identity.ctime_nsec,
        }
    }
}

struct BoundDirectory {
    path: PathBuf,
    file: File,
    identity: FileIdentity,
    policy: DirectoryPolicy,
}

impl BoundDirectory {
    fn bind(path: &Path, policy: DirectoryPolicy) -> Result<Self, String> {
        let path_metadata = fs::symlink_metadata(path)
            .map_err(|error| format!("failed to inspect directory {}: {error}", path.display()))?;
        if !path_metadata.file_type().is_dir() {
            return Err(format!("path is not a real directory: {}", path.display()));
        }
        let file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_CLOEXEC | libc::O_DIRECTORY | libc::O_NOFOLLOW)
            .open(path)
            .map_err(|error| {
                format!(
                    "failed to open directory without following links {}: {error}",
                    path.display()
                )
            })?;
        let descriptor_metadata = file.metadata().map_err(|error| {
            format!(
                "failed to inspect opened directory {}: {error}",
                path.display()
            )
        })?;
        let path_identity = FileIdentity::from_metadata(&path_metadata);
        let identity = FileIdentity::from_metadata(&descriptor_metadata);
        if path_identity != identity {
            return Err(format!(
                "directory path changed while it was opened: {}",
                path.display()
            ));
        }
        validate_directory_policy(identity, policy, path)?;
        Ok(Self {
            path: path.to_path_buf(),
            file,
            identity,
            policy,
        })
    }

    fn bind_absolute_nofollow(path: &Path, policy: DirectoryPolicy) -> Result<Self, String> {
        require_canonical_absolute_path(path, "directory")?;
        let mut file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_CLOEXEC | libc::O_DIRECTORY | libc::O_NOFOLLOW)
            .open("/")
            .map_err(|error| format!("failed to open filesystem root: {error}"))?;
        for component in path.components().skip(1) {
            let std::path::Component::Normal(name) = component else {
                return Err(format!(
                    "directory path has a non-canonical component: {}",
                    path.display()
                ));
            };
            file = openat_directory(file.as_raw_fd(), name).map_err(|error| {
                format!(
                    "failed to open directory component {:?} without following links in {}: {error}",
                    name,
                    path.display()
                )
            })?;
        }
        let descriptor_metadata = file.metadata().map_err(|error| {
            format!(
                "failed to inspect opened directory {}: {error}",
                path.display()
            )
        })?;
        let path_metadata = fs::symlink_metadata(path)
            .map_err(|error| format!("failed to inspect directory {}: {error}", path.display()))?;
        let identity = FileIdentity::from_metadata(&descriptor_metadata);
        if !path_metadata.file_type().is_dir()
            || FileIdentity::from_metadata(&path_metadata) != identity
        {
            return Err(format!(
                "directory path changed while its components were bound: {}",
                path.display()
            ));
        }
        validate_directory_policy(identity, policy, path)?;
        Ok(Self {
            path: path.to_path_buf(),
            file,
            identity,
            policy,
        })
    }

    fn bind_child(
        parent: &BoundDirectory,
        name: &OsStr,
        path: PathBuf,
        policy: DirectoryPolicy,
    ) -> Result<Self, String> {
        let file = openat_directory(parent.file.as_raw_fd(), name).map_err(|error| {
            format!(
                "failed to bind child directory without following links {}: {error}",
                path.display()
            )
        })?;
        let descriptor_metadata = file.metadata().map_err(|error| {
            format!(
                "failed to inspect child directory {}: {error}",
                path.display()
            )
        })?;
        let identity = FileIdentity::from_metadata(&descriptor_metadata);
        let entry_identity = fstatat_identity(parent.file.as_raw_fd(), name)
            .map_err(|error| format!("failed to inspect child directory entry: {error}"))?
            .ok_or_else(|| format!("child directory disappeared: {}", path.display()))?;
        if !descriptor_metadata.file_type().is_dir()
            || !entry_identity.same_directory_binding(identity)
        {
            return Err(format!(
                "child directory changed while it was bound: {}",
                path.display()
            ));
        }
        validate_directory_policy(identity, policy, &path)?;
        Ok(Self {
            path,
            file,
            identity,
            policy,
        })
    }

    fn recheck_path(&self) -> Result<(), String> {
        let metadata = fs::symlink_metadata(&self.path).map_err(|error| {
            format!(
                "failed to recheck directory path {}: {error}",
                self.path.display()
            )
        })?;
        let current = FileIdentity::from_metadata(&metadata);
        if !metadata.file_type().is_dir()
            || current.dev != self.identity.dev
            || current.ino != self.identity.ino
            || current.mode != self.identity.mode
            || current.uid != self.identity.uid
            || current.gid != self.identity.gid
        {
            return Err(format!(
                "directory path changed during publication: {}",
                self.path.display()
            ));
        }
        Ok(())
    }

    fn sync(&self) -> Result<(), String> {
        #[cfg(test)]
        {
            SYNC_ATTEMPTS.with(|attempts| attempts.set(attempts.get().saturating_add(1)));
            if let Some(errno) = SYNC_FAULT.with(|fault| fault.take()) {
                return Err(format!(
                    "failed to sync directory {}: {}",
                    self.path.display(),
                    io::Error::from_raw_os_error(errno)
                ));
            }
        }
        self.file
            .sync_all()
            .map_err(|error| format!("failed to sync directory {}: {error}", self.path.display()))
    }
}

fn validate_directory_policy(
    identity: FileIdentity,
    policy: DirectoryPolicy,
    path: &Path,
) -> Result<(), String> {
    let euid = effective_user_id();
    match policy {
        DirectoryPolicy::PrivateSource => {
            if identity.uid != euid || identity.mode & 0o077 != 0 {
                return Err(format!(
                    "staging directory must be owned by this user and owner-only: {}",
                    path.display()
                ));
            }
        }
        DirectoryPolicy::SharedDestination => {
            let private_destination = identity.uid == euid
                && identity.mode & 0o700 == 0o700
                && identity.mode & 0o022 == 0;
            let shared_horizon_destination = identity.uid == euid
                && identity.mode & 0o777 == 0o770
                && identity.mode & libc::S_ISGID != 0
                && identity.mode & libc::S_ISVTX != 0;
            if !private_destination && !shared_horizon_destination {
                return Err(format!(
                    "destination directory must be user-owned and non-writable by group/other, or use shared Horizon mode 3770: {}",
                    path.display()
                ));
            }
        }
        DirectoryPolicy::PrivateRecovery => {
            if identity.uid != euid || identity.mode & 0o077 != 0 {
                return Err(format!(
                    "recovery directory must be owned by this user and owner-only: {}",
                    path.display()
                ));
            }
        }
    }
    Ok(())
}

#[derive(Clone, Copy)]
enum DirectoryPolicy {
    PrivateSource,
    SharedDestination,
    PrivateRecovery,
}

struct BoundFile {
    file: File,
    identity: FileIdentity,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ComponentKind {
    Archive,
    Manifest,
    Checksum,
}

impl ComponentKind {
    fn label(self) -> &'static str {
        match self {
            Self::Archive => "archive",
            Self::Manifest => "segment manifest",
            Self::Checksum => "checksum",
        }
    }

    fn backup_name(self) -> &'static OsStr {
        match self {
            Self::Archive => OsStr::new("previous-archive.jet"),
            Self::Manifest => OsStr::new("previous-archive.jet.segment.json"),
            Self::Checksum => OsStr::new("previous-archive.jet.sha256"),
        }
    }

    fn staging_name(self) -> &'static OsStr {
        match self {
            Self::Archive => OsStr::new("new-archive.jet"),
            Self::Manifest => OsStr::new("new-archive.jet.segment.json"),
            Self::Checksum => OsStr::new("new-archive.jet.sha256"),
        }
    }
}

struct StagedComponent {
    kind: ComponentKind,
    directory_index: DirectoryIndex,
    name: std::ffi::OsString,
    file: File,
    identity: FileIdentity,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DirectoryIndex {
    Source,
    Recovery,
    Destination,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
struct InitialDestination {
    archive: Option<FileIdentity>,
    manifest: Option<FileIdentity>,
    checksum: Option<FileIdentity>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
enum ArchiveBatchDecision {
    RollBack,
    Commit,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ArchiveBatchJournalItem {
    epoch: u64,
    source_path: Vec<u8>,
    source_identity: FileIdentity,
    recovery_name: Vec<u8>,
    recovery_identity: FileIdentity,
    archive_name: Vec<u8>,
    archive_identity: FileIdentity,
    manifest_staged_identity: Option<FileIdentity>,
    checksum_staged_identity: FileIdentity,
    checksum_sentinel_identity: FileIdentity,
    destination_name: Vec<u8>,
    destination_manifest_name: Vec<u8>,
    destination_checksum_name: Vec<u8>,
    initial: InitialDestination,
    evidence: PersistedValidatedArchiveFile,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ArchiveBatchJournal {
    version: u32,
    transaction_id: [u8; 32],
    transaction_nonce: [u8; 32],
    manifest_fingerprint: [u8; 32],
    decision: ArchiveBatchDecision,
    destination_path: Vec<u8>,
    destination_identity: FileIdentity,
    items: Vec<ArchiveBatchJournalItem>,
}

#[derive(Clone)]
struct NamespaceEntry {
    directory: DirectoryIndex,
    name: std::ffi::OsString,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MutationRole {
    Data,
    ChecksumReservation,
    ChecksumCommit,
}

#[derive(Clone)]
enum Mutation {
    Rename {
        role: MutationRole,
        source: NamespaceEntry,
        destination: NamespaceEntry,
        identity: FileIdentity,
    },
    Exchange {
        role: MutationRole,
        left: NamespaceEntry,
        right: NamespaceEntry,
        left_identity: FileIdentity,
        right_identity: FileIdentity,
    },
}

impl Mutation {
    fn role(&self) -> MutationRole {
        match self {
            Self::Rename { role, .. } | Self::Exchange { role, .. } => *role,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PublishPhase {
    BeforeChecksumInvalidation,
    AfterChecksumInvalidationMutation,
    AfterChecksumInvalidation,
    BeforeManifestMutation,
    AfterManifestInstallMutation,
    AfterManifestBackupMutation,
    AfterManifestMutation,
    BeforeArchiveInstall,
    AfterArchiveInstallMutation,
    AfterArchiveBackupMutation,
    AfterArchiveInstall,
    BeforeChecksumCommit,
    AfterChecksumCommitMutation,
    BeforeRollbackMutationSync,
    BeforeRollbackDataSync,
    AfterRollbackDataSync,
    AfterChecksumRestoreMutation,
    AfterChecksumRollbackSync,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ArchiveBatchPhase {
    JournalPrepared,
    Item { index: usize, phase: PublishPhase },
    CommitDecisionDurable,
    BeforeTransactionMarkerRemoval,
    AfterTransactionMarkerRetirement,
}

/// Verifies the directory policies and filesystem operations required by
/// [`publish_verified_archive`] before expensive archive generation begins.
///
/// The staging directory must be owner-only. The destination must satisfy the
/// same ownership and mode policy as publication. The probe creates only
/// private, identity-bound temporary entries and removes them before success.
pub fn preflight_archive_publication(
    staging_directory: &Path,
    destination_directory: &Path,
) -> Result<(), ArchivePublicationError> {
    let source = BoundDirectory::bind(staging_directory, DirectoryPolicy::PrivateSource)
        .map_err(preflight_error)?;
    let destination =
        BoundDirectory::bind(destination_directory, DirectoryPolicy::SharedDestination)
            .map_err(preflight_error)?;
    if archive_batch_publication_in_progress(destination_directory).map_err(|error| {
        preflight_error(format!("failed to inspect archive batch marker: {error}"))
    })? {
        return Err(preflight_error(format!(
            "destination {} has an in-progress archive batch transaction",
            destination_directory.display()
        )));
    }
    require_compatible_directories(&source, &destination).map_err(preflight_error)?;
    let (recovery_name, recovery) = create_recovery_directory(&destination)
        .map_err(|error| preflight_error_with_recovery(error.message, error.recovery_directory))?;
    if let Err(message) = probe_rename_capabilities(&source, &recovery, destination.identity.gid) {
        return Err(cleanup_preflight_error(
            message,
            &destination,
            &recovery_name,
            &recovery,
            &[],
        ));
    }
    remove_empty_recovery_directory(&destination, &recovery_name, &recovery)
        .map_err(|message| preflight_error_with_recovery(message, Some(recovery.path.clone())))
}

/// Publishes a validated staging archive to a shared Horizon directory.
///
/// The staging archive must already have its final group and mode (`0440`).
/// The staging parent must be owner-only. A private destination must be owned
/// by the caller and non-writable by group/other. A shared Horizon destination
/// must be owned by the caller and use mode `3770`.
pub fn publish_verified_archive(
    staged_archive: &Path,
    destination_archive: &Path,
    evidence: ValidatedArchiveFile,
) -> Result<ArchivePublication, ArchivePublicationError> {
    publish_verified_archive_with_hook(staged_archive, destination_archive, evidence, |_| Ok(()))
}

/// Publishes an ordered cohort as one recoverable transaction.
///
/// Before any archive or manifest changes, every member is validated and every
/// checksum name is durably replaced by the publication sentinel. The durable
/// batch marker remains present until every new archive and canonical checksum
/// is installed. It is then durably retired to a completed-outcome record.
/// Persist a private receipt from the returned evidence, fsync it, and call
/// [`acknowledge_archive_publication_batch`] with the transaction ID. Until
/// acknowledgement, recovery returns the same outcome and no new batch may
/// start. Call [`recover_archive_publication_batch`] during startup before
/// constructing a new cohort for the same destination directory.
///
/// This is marked, recoverable visibility rather than literal multi-name POSIX
/// atomicity. Batch-aware readers must refuse the destination while
/// [`archive_batch_publication_in_progress`] reports true.
pub fn publish_verified_archive_batch(
    manifest_fingerprint: [u8; 32],
    items: &[ArchiveBatchItem],
) -> Result<ArchiveBatchPublication, ArchivePublicationError> {
    publish_verified_archive_batch_with_hook(manifest_fingerprint, items, |_| Ok(()))
}

fn publish_verified_archive_batch_with_hook<F>(
    manifest_fingerprint: [u8; 32],
    items: &[ArchiveBatchItem],
    mut hook: F,
) -> Result<ArchiveBatchPublication, ArchivePublicationError>
where
    F: FnMut(ArchiveBatchPhase) -> Result<(), String>,
{
    if manifest_fingerprint == [0; 32] {
        return Err(preflight_error(
            "archive batch manifest fingerprint must not be zero".to_string(),
        ));
    }
    let destination_path = validate_archive_batch_request(items)?;
    if archive_batch_marker_exists(&destination_path).map_err(preflight_error)? {
        return Err(preflight_error(format!(
            "an interrupted archive batch transaction exists in {}; call recover_archive_publication_batch before starting another cohort",
            destination_path.display()
        )));
    }

    let mut transactions = Vec::with_capacity(items.len());
    for item in items {
        match prepare_publication_transaction(
            &item.staged_archive,
            &item.destination_archive,
            item.evidence,
        ) {
            Ok(transaction) => transactions.push(transaction),
            Err(error) => {
                let cleanup = cleanup_prepared_batch_transactions(&mut transactions);
                return Err(append_batch_cleanup_error(error, cleanup));
            }
        }
    }
    if let Err(message) = require_one_batch_destination(&transactions) {
        let cleanup = cleanup_prepared_batch_transactions(&mut transactions);
        return Err(append_batch_cleanup_error(
            preflight_error(message),
            cleanup,
        ));
    }

    let transaction_nonce =
        archive_batch_transaction_nonce(&transactions, items, manifest_fingerprint);
    let mut journal = ArchiveBatchJournal {
        version: ARCHIVE_BATCH_JOURNAL_VERSION,
        transaction_id: [0; 32],
        transaction_nonce,
        manifest_fingerprint,
        decision: ArchiveBatchDecision::RollBack,
        destination_path: path_bytes(&destination_path),
        destination_identity: transactions[0].destination.identity,
        items: transactions
            .iter()
            .zip(items)
            .map(|(transaction, item)| transaction.batch_journal_item(item.epoch))
            .collect(),
    };
    journal.transaction_id = match archive_batch_journal_transaction_id(&journal) {
        Ok(transaction_id) => transaction_id,
        Err(message) => {
            let cleanup = cleanup_prepared_batch_transactions(&mut transactions);
            return Err(append_batch_cleanup_error(
                preflight_error(message),
                cleanup,
            ));
        }
    };
    let transaction_id = journal.transaction_id;
    let marker = match create_archive_batch_marker(&transactions[0].destination, &journal) {
        Ok(marker) => marker,
        Err(error) => {
            let cleanup = cleanup_prepared_batch_transactions(&mut transactions);
            return Err(append_batch_cleanup_error(error, cleanup));
        }
    };

    let operation = (|| -> Result<(), String> {
        hook(ArchiveBatchPhase::JournalPrepared)?;
        for (index, transaction) in transactions.iter_mut().enumerate() {
            let mut item_hook = |phase| hook(ArchiveBatchPhase::Item { index, phase });
            item_hook(PublishPhase::BeforeChecksumInvalidation)?;
            transaction.reserve_checksum_name(&mut item_hook)?;
            transaction.sync_mutation_directories()?;
            item_hook(PublishPhase::AfterChecksumInvalidation)?;
        }
        for (index, transaction) in transactions.iter_mut().enumerate() {
            let mut item_hook = |phase| hook(ArchiveBatchPhase::Item { index, phase });
            transaction.run_data_precommit(&mut item_hook)?;
        }
        for transaction in &mut transactions {
            transaction.verify_commit_ready()?;
            transaction.sync_mutation_directories()?;
        }

        journal.decision = ArchiveBatchDecision::Commit;
        replace_archive_batch_journal(&marker, &journal)?;
        hook(ArchiveBatchPhase::CommitDecisionDurable)?;

        for (index, transaction) in transactions.iter_mut().enumerate() {
            let mut item_hook = |phase| hook(ArchiveBatchPhase::Item { index, phase });
            transaction.commit_checksum_for_batch(&mut item_hook)?;
        }
        Ok(())
    })();

    if let Err(cause) = operation {
        drop(transactions);
        drop(marker);
        return resolve_failed_archive_batch(&destination_path, cause);
    }

    if let Err(cause) = cleanup_committed_batch_sentinels(&transactions)
        .and_then(|()| hook(ArchiveBatchPhase::BeforeTransactionMarkerRemoval))
        .and_then(|()| {
            for transaction in &mut transactions {
                transaction.verify_finalized_batch_namespace()?;
                transaction.sync_mutation_directories()?;
            }
            Ok(())
        })
    {
        drop(transactions);
        drop(marker);
        return resolve_failed_archive_batch(&destination_path, cause);
    }
    let publications = match transactions
        .iter_mut()
        .map(PublicationTransaction::batch_publication)
        .collect::<Result<Vec<_>, _>>()
    {
        Ok(publications) => publications,
        Err(cause) => {
            drop(transactions);
            drop(marker);
            return resolve_failed_archive_batch(&destination_path, cause);
        }
    };
    let identity_evidence = match transactions
        .iter()
        .zip(items)
        .map(|(transaction, item)| transaction.batch_identity_evidence(item.epoch))
        .collect::<Result<Vec<_>, _>>()
    {
        Ok(evidence) => evidence,
        Err(cause) => {
            drop(transactions);
            drop(marker);
            return resolve_failed_archive_batch(&destination_path, cause);
        }
    };
    if let Err(cause) = transactions
        .iter_mut()
        .try_for_each(PublicationTransaction::verify_finalized_batch_namespace)
    {
        drop(transactions);
        drop(marker);
        return resolve_failed_archive_batch(&destination_path, cause);
    }
    retire_archive_batch_marker(&transactions[0].destination, &marker)
        .map_err(|message| batch_indeterminate_error(&destination_path, message, true))?;
    if let Err(cause) = hook(ArchiveBatchPhase::AfterTransactionMarkerRetirement) {
        drop(transactions);
        drop(marker);
        return resolve_failed_archive_batch(&destination_path, cause);
    }
    Ok(ArchiveBatchPublication {
        transaction_id,
        manifest_fingerprint,
        destination_identity: ArchiveBatchDestinationIdentity {
            device: journal.destination_identity.dev,
            inode: journal.destination_identity.ino,
        },
        publications,
        identity_evidence,
    })
}

/// Recovers the one durable batch transaction marker in a destination.
///
/// A rollback decision restores every original archive, manifest, checksum,
/// and staged archive. A commit decision finishes every canonical checksum.
/// A completed outcome is returned repeatedly until it is acknowledged after
/// the caller's private receipt is durable.
/// The caller must ensure no live publisher is using this destination; an
/// advisory lock rejects recovery while the publishing process is alive.
pub fn recover_archive_publication_batch(
    destination_directory: &Path,
) -> Result<ArchiveBatchRecovery, ArchivePublicationError> {
    let destination = BoundDirectory::bind_absolute_nofollow(
        destination_directory,
        DirectoryPolicy::SharedDestination,
    )
    .map_err(preflight_error)?;
    let marker = bind_archive_batch_marker(&destination).map_err(preflight_error)?;
    let outcome = bind_archive_batch_outcome(&destination).map_err(preflight_error)?;
    if marker.is_some() && outcome.is_some() {
        return Err(preflight_error(
            "destination contains both an active archive batch and a completed outcome".to_string(),
        ));
    }
    let Some(marker) = marker.or(outcome) else {
        return Ok(ArchiveBatchRecovery::None);
    };
    let completed_outcome =
        marker.path.file_name() == Some(OsStr::new(ARCHIVE_BATCH_OUTCOME_DIRECTORY));
    lock_archive_batch_marker(&marker)
        .map_err(|message| preflight_error_with_recovery(message, Some(marker.path.clone())))?;
    let journal = read_archive_batch_journal(&destination, &marker)?;
    if completed_outcome {
        let journal = journal.ok_or_else(|| {
            preflight_error_with_recovery(
                "completed archive batch outcome is not armed".to_string(),
                Some(marker.path.clone()),
            )
        })?;
        return observe_finalized_archive_batch(&destination, &journal).map_err(|message| {
            batch_indeterminate_error(
                destination_directory,
                message,
                journal.decision == ArchiveBatchDecision::Commit,
            )
        });
    }
    let Some(journal) = journal else {
        remove_archive_batch_marker_directory(
            &destination,
            &marker,
            OsStr::new(ARCHIVE_BATCH_TRANSACTION_DIRECTORY),
            Some(false),
        )
        .map_err(|message| preflight_error_with_recovery(message, Some(marker.path.clone())))?;
        return Ok(ArchiveBatchRecovery::None);
    };
    recover_bound_archive_batch(&destination, &marker, &journal)
}

/// Removes the durable completed-outcome record after the caller has written
/// its own durable receipt. The transaction ID prevents acknowledging a
/// different batch. This operation must be ordered after receipt fsync.
pub fn acknowledge_archive_publication_batch(
    destination_directory: &Path,
    transaction_id: [u8; 32],
) -> Result<(), ArchivePublicationError> {
    let destination = BoundDirectory::bind_absolute_nofollow(
        destination_directory,
        DirectoryPolicy::SharedDestination,
    )
    .map_err(preflight_error)?;
    if bind_archive_batch_marker(&destination)
        .map_err(preflight_error)?
        .is_some()
    {
        return Err(preflight_error(
            "cannot acknowledge a completed outcome while an active archive batch exists"
                .to_string(),
        ));
    }
    let outcome = bind_archive_batch_outcome(&destination)
        .map_err(preflight_error)?
        .ok_or_else(|| preflight_error("archive batch outcome does not exist".to_string()))?;
    lock_archive_batch_marker(&outcome)
        .map_err(|message| preflight_error_with_recovery(message, Some(outcome.path.clone())))?;
    let journal = read_archive_batch_journal(&destination, &outcome)?.ok_or_else(|| {
        preflight_error_with_recovery(
            "completed archive batch outcome is not armed".to_string(),
            Some(outcome.path.clone()),
        )
    })?;
    if journal.transaction_id != transaction_id {
        return Err(preflight_error_with_recovery(
            "archive batch outcome transaction ID does not match acknowledgement".to_string(),
            Some(outcome.path.clone()),
        ));
    }
    let recovered =
        bind_recovered_archive_batch_items(&destination, &journal).map_err(|message| {
            batch_indeterminate_error(
                destination_directory,
                message,
                journal.decision == ArchiveBatchDecision::Commit,
            )
        })?;
    observe_finalized_archive_batch(&destination, &journal).map_err(|message| {
        batch_indeterminate_error(
            destination_directory,
            message,
            journal.decision == ArchiveBatchDecision::Commit,
        )
    })?;
    remove_archive_batch_marker_directory(
        &destination,
        &outcome,
        OsStr::new(ARCHIVE_BATCH_OUTCOME_DIRECTORY),
        Some(true),
    )
    .map_err(|message| {
        batch_indeterminate_error(
            destination_directory,
            message,
            journal.decision == ArchiveBatchDecision::Commit,
        )
    })?;
    cleanup_acknowledged_archive_batch(&destination, journal.decision, &recovered);
    Ok(())
}

fn publish_verified_archive_with_hook<F>(
    staged_archive: &Path,
    destination_archive: &Path,
    evidence: ValidatedArchiveFile,
    mut hook: F,
) -> Result<ArchivePublication, ArchivePublicationError>
where
    F: FnMut(PublishPhase) -> Result<(), String>,
{
    publish_impl(staged_archive, destination_archive, evidence, &mut hook)
}

fn publish_impl(
    staged_archive: &Path,
    destination_archive: &Path,
    evidence: ValidatedArchiveFile,
    hook: &mut dyn FnMut(PublishPhase) -> Result<(), String>,
) -> Result<ArchivePublication, ArchivePublicationError> {
    let mut transaction =
        prepare_publication_transaction(staged_archive, destination_archive, evidence)?;
    if let Err(message) = transaction.run_precommit(hook) {
        return Err(transaction.rollback_error(message, hook));
    }
    transaction.commit(hook)
}

fn prepare_publication_transaction(
    staged_archive: &Path,
    destination_archive: &Path,
    evidence: ValidatedArchiveFile,
) -> Result<PublicationTransaction, ArchivePublicationError> {
    let source_parent_path = staged_archive.parent().unwrap_or_else(|| Path::new("."));
    let destination_parent_path = destination_archive
        .parent()
        .unwrap_or_else(|| Path::new("."));
    let source = BoundDirectory::bind(source_parent_path, DirectoryPolicy::PrivateSource)
        .map_err(preflight_error)?;
    let destination =
        BoundDirectory::bind(destination_parent_path, DirectoryPolicy::SharedDestination)
            .map_err(preflight_error)?;
    if archive_batch_publication_in_progress(destination_parent_path).map_err(|error| {
        preflight_error(format!("failed to inspect archive batch marker: {error}"))
    })? {
        return Err(preflight_error(format!(
            "destination {} has an in-progress archive batch transaction",
            destination_parent_path.display()
        )));
    }
    require_compatible_directories(&source, &destination).map_err(preflight_error)?;

    let archive_name = safe_file_name(staged_archive).map_err(preflight_error)?;
    let destination_name = safe_file_name(destination_archive).map_err(preflight_error)?;
    if archive_name != destination_name {
        return Err(preflight_error(format!(
            "staging and destination archive filenames differ: {:?} and {:?}",
            archive_name, destination_name
        )));
    }

    let archive = bind_required_regular(&source, &archive_name, ComponentKind::Archive)
        .map_err(preflight_error)?;
    let archive_evidence = archive_file_identity(&archive.file).map_err(|error| {
        preflight_error(format!(
            "failed to identify staged archive {}: {error}",
            staged_archive.display()
        ))
    })?;
    if archive_evidence != evidence.identity
        || !path_matches_archive_identity(staged_archive, evidence.identity).map_err(|error| {
            preflight_error(format!(
                "failed to bind validation evidence to {}: {error}",
                staged_archive.display()
            ))
        })?
    {
        return Err(preflight_error(format!(
            "staged archive changed after validation: {}",
            staged_archive.display()
        )));
    }
    require_staged_archive_permissions(&archive, destination.identity.gid)
        .map_err(preflight_error)?;
    archive.file.sync_all().map_err(|error| {
        preflight_error(format!(
            "failed to sync staged archive {}: {error}",
            staged_archive.display()
        ))
    })?;

    let source_manifest_path = segment_manifest_path(staged_archive)
        .map_err(|error| preflight_error(error.to_string()))?;
    let source_manifest_name = safe_file_name(&source_manifest_path).map_err(preflight_error)?;
    let source_manifest =
        bind_optional_regular(&source, &source_manifest_name, ComponentKind::Manifest)
            .map_err(preflight_error)?;
    if let Some(bound) = source_manifest.as_ref() {
        bound.file.sync_all().map_err(|error| {
            preflight_error(format!(
                "failed to sync staged segment manifest {}: {error}",
                source_manifest_path.display()
            ))
        })?;
    }
    let manifest_bytes = match source_manifest.as_ref() {
        Some(bound) => Some(
            validate_bound_manifest(
                staged_archive,
                &archive,
                evidence,
                &source,
                &source_manifest_path,
                &source_manifest_name,
                bound,
            )
            .map_err(preflight_error)?,
        ),
        None => None,
    };

    let source_checksum_path = archive_checksum_path(staged_archive)
        .map_err(|error| preflight_error(error.to_string()))?;
    let source_checksum_name = safe_file_name(&source_checksum_path).map_err(preflight_error)?;
    let expected_checksum = archive_checksum_line(&evidence.sha256, &destination_name)
        .map_err(|error| preflight_error(error.to_string()))?;
    if let Some(bound) =
        bind_optional_regular(&source, &source_checksum_name, ComponentKind::Checksum)
            .map_err(preflight_error)?
    {
        bound.file.sync_all().map_err(|error| {
            preflight_error(format!(
                "failed to sync staged checksum {}: {error}",
                source_checksum_path.display()
            ))
        })?;
        let bytes = read_bound_file(&bound.file, expected_checksum.len() as u64)
            .map_err(preflight_error)?;
        if bytes != expected_checksum.as_bytes() {
            return Err(preflight_error(format!(
                "staged checksum is not the canonical checksum for {}",
                staged_archive.display()
            )));
        }
        recheck_bound_file(&source, &source_checksum_name, bound.identity)
            .map_err(preflight_error)?;
    }

    let destination_manifest_path = segment_manifest_path(destination_archive)
        .map_err(|error| preflight_error(error.to_string()))?;
    let destination_checksum_path = archive_checksum_path(destination_archive)
        .map_err(|error| preflight_error(error.to_string()))?;
    let destination_manifest_name =
        safe_file_name(&destination_manifest_path).map_err(preflight_error)?;
    let destination_checksum_name =
        safe_file_name(&destination_checksum_path).map_err(preflight_error)?;

    let initial_archive =
        bind_optional_regular(&destination, &destination_name, ComponentKind::Archive)
            .map_err(preflight_error)?;
    let initial_manifest = bind_optional_regular(
        &destination,
        &destination_manifest_name,
        ComponentKind::Manifest,
    )
    .map_err(preflight_error)?;
    let initial_checksum = bind_optional_regular(
        &destination,
        &destination_checksum_name,
        ComponentKind::Checksum,
    )
    .map_err(preflight_error)?;
    if let Some(bound) = initial_checksum.as_ref()
        && bound.identity.len == ARCHIVE_PUBLICATION_SENTINEL.len() as u64
        && read_bound_file(&bound.file, ARCHIVE_PUBLICATION_SENTINEL.len() as u64)
            .map_err(preflight_error)?
            == ARCHIVE_PUBLICATION_SENTINEL
    {
        return Err(preflight_error(format!(
            "destination checksum {} is an interrupted Jetstreamer publication sentinel; manual transaction recovery is required before another publication",
            destination_checksum_path.display()
        )));
    }
    let initial = InitialDestination {
        archive: initial_archive.as_ref().map(|bound| bound.identity),
        manifest: initial_manifest.as_ref().map(|bound| bound.identity),
        checksum: initial_checksum.as_ref().map(|bound| bound.identity),
    };
    for (kind, bound) in [
        (ComponentKind::Archive, initial_archive.as_ref()),
        (ComponentKind::Manifest, initial_manifest.as_ref()),
        (ComponentKind::Checksum, initial_checksum.as_ref()),
    ] {
        if let Some(bound) = bound {
            bound.file.sync_all().map_err(|error| {
                preflight_error(format!(
                    "failed to sync pre-existing destination {}: {error}",
                    kind.label()
                ))
            })?;
        }
    }
    let initial_handles = [initial_archive, initial_manifest, initial_checksum]
        .into_iter()
        .flatten()
        .map(|bound| bound.file)
        .collect();

    source.recheck_path().map_err(preflight_error)?;
    destination.recheck_path().map_err(preflight_error)?;
    let (recovery_name, recovery) = create_recovery_directory(&destination)
        .map_err(|error| preflight_error_with_recovery(error.message, error.recovery_directory))?;
    if let Err(message) = probe_rename_capabilities(&source, &recovery, destination.identity.gid) {
        return Err(cleanup_preflight_error(
            message,
            &destination,
            &recovery_name,
            &recovery,
            &[],
        ));
    }
    let manifest_staged = match manifest_bytes {
        Some(bytes) => Some(
            create_recovery_file(
                &recovery,
                ComponentKind::Manifest,
                ComponentKind::Manifest.staging_name(),
                &bytes,
                destination.identity.gid,
            )
            .map_err(|message| {
                cleanup_preflight_error(message, &destination, &recovery_name, &recovery, &[])
            })?,
        ),
        None => None,
    };
    let manifest_cleanup = manifest_staged
        .as_ref()
        .map(|staged| (staged.name.clone(), staged.identity))
        .into_iter()
        .collect::<Vec<_>>();
    let checksum_staged = create_recovery_file(
        &recovery,
        ComponentKind::Checksum,
        ComponentKind::Checksum.staging_name(),
        expected_checksum.as_bytes(),
        destination.identity.gid,
    )
    .map_err(|message| {
        cleanup_preflight_error(
            message,
            &destination,
            &recovery_name,
            &recovery,
            &manifest_cleanup,
        )
    })?;
    let mut staged_cleanup = manifest_cleanup;
    staged_cleanup.push((checksum_staged.name.clone(), checksum_staged.identity));
    let checksum_sentinel = create_recovery_file(
        &recovery,
        ComponentKind::Checksum,
        ComponentKind::Checksum.backup_name(),
        ARCHIVE_PUBLICATION_SENTINEL,
        destination.identity.gid,
    )
    .map_err(|message| {
        cleanup_preflight_error(
            message,
            &destination,
            &recovery_name,
            &recovery,
            &staged_cleanup,
        )
    })?;
    if let Err(message) = recovery.sync().and_then(|()| destination.sync()) {
        staged_cleanup.push((checksum_sentinel.name.clone(), checksum_sentinel.identity));
        return Err(cleanup_preflight_error(
            message,
            &destination,
            &recovery_name,
            &recovery,
            &staged_cleanup,
        ));
    }

    let archive_staged = StagedComponent {
        kind: ComponentKind::Archive,
        directory_index: DirectoryIndex::Source,
        name: archive_name,
        file: archive.file,
        identity: archive.identity,
    };
    let manifest_staging_identity = manifest_staged.as_ref().map(|staged| staged.identity);
    Ok(PublicationTransaction {
        source,
        destination,
        recovery,
        recovery_name,
        initial,
        // The fixed transaction performs at most reservation + two manifest
        // operations + two archive operations + checksum commit. Preallocation
        // leaves no allocation window after a successful namespace syscall.
        mutations: Vec::with_capacity(6),
        _initial_handles: initial_handles,
        archive_staged,
        manifest_staged,
        checksum_staged,
        checksum_sentinel,
        destination_name,
        destination_manifest_name,
        destination_checksum_name,
        evidence,
        published_evidence: None,
        installed_archive_identity: None,
        installed_manifest_identity: None,
        manifest_staging_identity,
    })
}

struct PublicationTransaction {
    source: BoundDirectory,
    destination: BoundDirectory,
    recovery: BoundDirectory,
    recovery_name: std::ffi::OsString,
    initial: InitialDestination,
    mutations: Vec<Mutation>,
    // Keep descriptors for every pre-existing target open until commit or
    // rollback. Backups are then provably the same inodes admitted at preflight.
    _initial_handles: Vec<File>,
    archive_staged: StagedComponent,
    manifest_staged: Option<StagedComponent>,
    checksum_staged: StagedComponent,
    checksum_sentinel: StagedComponent,
    destination_name: std::ffi::OsString,
    destination_manifest_name: std::ffi::OsString,
    destination_checksum_name: std::ffi::OsString,
    evidence: ValidatedArchiveFile,
    published_evidence: Option<ValidatedArchiveFile>,
    installed_archive_identity: Option<FileIdentity>,
    installed_manifest_identity: Option<FileIdentity>,
    manifest_staging_identity: Option<FileIdentity>,
}

impl PublicationTransaction {
    fn directory(&self, index: DirectoryIndex) -> &BoundDirectory {
        match index {
            DirectoryIndex::Source => &self.source,
            DirectoryIndex::Recovery => &self.recovery,
            DirectoryIndex::Destination => &self.destination,
        }
    }

    fn run_precommit(
        &mut self,
        hook: &mut dyn FnMut(PublishPhase) -> Result<(), String>,
    ) -> Result<(), String> {
        hook(PublishPhase::BeforeChecksumInvalidation)?;
        self.reserve_checksum_name(hook)?;
        self.sync_mutation_directories()?;
        hook(PublishPhase::AfterChecksumInvalidation)?;

        self.run_data_precommit(hook)
    }

    fn run_data_precommit(
        &mut self,
        hook: &mut dyn FnMut(PublishPhase) -> Result<(), String>,
    ) -> Result<(), String> {
        hook(PublishPhase::BeforeManifestMutation)?;
        if let Some((kind, directory, name, identity)) =
            self.manifest_staged.as_ref().map(|staged| {
                (
                    staged.kind,
                    staged.directory_index,
                    staged.name.clone(),
                    staged.identity,
                )
            })
        {
            self.installed_manifest_identity = Some(self.install_component(
                kind,
                directory,
                name,
                identity,
                self.destination_manifest_name.clone(),
                self.initial.manifest,
                PublishPhase::AfterManifestInstallMutation,
                PublishPhase::AfterManifestBackupMutation,
                hook,
            )?);
        } else if let Some(identity) = self.initial.manifest {
            self.remove_destination_to_backup(
                ComponentKind::Manifest,
                self.destination_manifest_name.clone(),
                identity,
                PublishPhase::AfterManifestBackupMutation,
                hook,
            )?;
        } else {
            require_expected_target(&self.destination, &self.destination_manifest_name, None)?;
        }
        self.sync_mutation_directories()?;
        hook(PublishPhase::AfterManifestMutation)?;

        hook(PublishPhase::BeforeArchiveInstall)?;
        self.installed_archive_identity = Some(self.install_component(
            self.archive_staged.kind,
            self.archive_staged.directory_index,
            self.archive_staged.name.clone(),
            self.archive_staged.identity,
            self.destination_name.clone(),
            self.initial.archive,
            PublishPhase::AfterArchiveInstallMutation,
            PublishPhase::AfterArchiveBackupMutation,
            hook,
        )?);
        self.sync_mutation_directories()?;
        hook(PublishPhase::AfterArchiveInstall)?;

        self.refresh_published_evidence()?;
        hook(PublishPhase::BeforeChecksumCommit)?;
        Ok(())
    }

    fn reserve_checksum_name(
        &mut self,
        hook: &mut dyn FnMut(PublishPhase) -> Result<(), String>,
    ) -> Result<(), String> {
        self.verify_staged(&self.checksum_sentinel)?;
        require_expected_target(
            &self.destination,
            &self.destination_checksum_name,
            self.initial.checksum,
        )?;
        let recovery_entry = NamespaceEntry {
            directory: DirectoryIndex::Recovery,
            name: self.checksum_sentinel.name.clone(),
        };
        let destination_entry = NamespaceEntry {
            directory: DirectoryIndex::Destination,
            name: self.destination_checksum_name.clone(),
        };
        let mutation = match self.initial.checksum {
            Some(old_identity) => Mutation::Exchange {
                role: MutationRole::ChecksumReservation,
                left: recovery_entry,
                right: destination_entry,
                left_identity: self.checksum_sentinel.identity,
                right_identity: old_identity,
            },
            None => Mutation::Rename {
                role: MutationRole::ChecksumReservation,
                source: recovery_entry,
                destination: destination_entry,
                identity: self.checksum_sentinel.identity,
            },
        };
        self.perform_journaled_mutation(mutation)
            .map_err(|error| format!("failed to reserve checksum commit-marker name: {error}"))?;
        self.finish_last_mutation(PublishPhase::AfterChecksumInvalidationMutation, hook)?;
        self.checksum_sentinel.identity = rebound_target_identity(
            &self.destination,
            &self.destination_checksum_name,
            self.checksum_sentinel.identity,
        )?;
        match self.initial.checksum {
            Some(identity) => {
                rebound_target_identity(
                    &self.recovery,
                    ComponentKind::Checksum.backup_name(),
                    identity,
                )?;
            }
            None => require_expected_target(
                &self.recovery,
                ComponentKind::Checksum.backup_name(),
                None,
            )?,
        }
        Ok(())
    }

    fn commit(
        mut self,
        hook: &mut dyn FnMut(PublishPhase) -> Result<(), String>,
    ) -> Result<ArchivePublication, ArchivePublicationError> {
        let commit_mutation = Mutation::Exchange {
            role: MutationRole::ChecksumCommit,
            left: NamespaceEntry {
                directory: DirectoryIndex::Recovery,
                name: self.checksum_staged.name.clone(),
            },
            right: NamespaceEntry {
                directory: DirectoryIndex::Destination,
                name: self.destination_checksum_name.clone(),
            },
            left_identity: self.checksum_staged.identity,
            right_identity: self.checksum_sentinel.identity,
        };

        // Build the journal record before the final verification. The exchange
        // and non-allocating push are the only namespace steps before the
        // checksum durability barrier runs.
        if let Err(message) = self.verify_commit_ready() {
            return Err(self.rollback_error(message, hook));
        }
        if let Err(error) = self.perform_journaled_mutation(commit_mutation) {
            return Err(self.rollback_error(
                format!("failed to exchange checksum commit marker: {error}"),
                hook,
            ));
        }
        if let Err(message) =
            self.finish_last_mutation(PublishPhase::AfterChecksumCommitMutation, hook)
        {
            return self.fail_after_checksum_commit(
                format!("failed to durably install checksum commit marker: {message}"),
                hook,
            );
        }

        let post_commit = (|| {
            self.checksum_staged.identity = rebound_target_identity(
                &self.destination,
                &self.destination_checksum_name,
                self.checksum_staged.identity,
            )?;
            self.checksum_sentinel.identity = rebound_target_identity(
                &self.recovery,
                ComponentKind::Checksum.staging_name(),
                self.checksum_sentinel.identity,
            )?;
            sync_all(&[&self.recovery, &self.destination])?;
            self.verify_committed_namespace()
        })();
        if let Err(message) = post_commit {
            return self.fail_after_checksum_commit(message, hook);
        }

        if let Err(message) = remove_corresponding_file(
            &self.recovery,
            ComponentKind::Checksum.staging_name(),
            self.checksum_sentinel.identity,
        )
        .and_then(|()| self.recovery.sync())
        {
            return Err(self.committed_failure(format!(
                "committed archive, but failed to clean the invalid checksum sentinel: {message}"
            )));
        }

        let evidence = self
            .published_evidence
            .expect("commit readiness bound archive evidence");
        let retains_backups = self.initial.archive.is_some()
            || self.initial.manifest.is_some()
            || self.initial.checksum.is_some();
        let recovery_directory = if retains_backups {
            if let Err(message) =
                require_recovery_binding(&self.destination, &self.recovery_name, &self.recovery)
            {
                return Err(self.committed_failure(format!(
                    "committed archive, but the retained recovery directory changed: {message}"
                )));
            }
            Some(self.recovery.path.clone())
        } else {
            match remove_empty_recovery_directory(
                &self.destination,
                &self.recovery_name,
                &self.recovery,
            ) {
                Ok(()) => None,
                Err(message) => {
                    return Err(self.committed_failure(format!(
                        "committed archive, but failed to remove empty recovery directory: {message}"
                    )));
                }
            }
        };

        Ok(ArchivePublication {
            archive_path: self.destination.path.join(&self.destination_name),
            manifest_path: self
                .installed_manifest_identity
                .map(|_| self.destination.path.join(&self.destination_manifest_name)),
            checksum_path: self.destination.path.join(&self.destination_checksum_name),
            recovery_directory,
            evidence,
        })
    }

    fn commit_checksum_for_batch(
        &mut self,
        hook: &mut dyn FnMut(PublishPhase) -> Result<(), String>,
    ) -> Result<(), String> {
        let commit_mutation = Mutation::Exchange {
            role: MutationRole::ChecksumCommit,
            left: NamespaceEntry {
                directory: DirectoryIndex::Recovery,
                name: self.checksum_staged.name.clone(),
            },
            right: NamespaceEntry {
                directory: DirectoryIndex::Destination,
                name: self.destination_checksum_name.clone(),
            },
            left_identity: self.checksum_staged.identity,
            right_identity: self.checksum_sentinel.identity,
        };
        self.verify_commit_ready()?;
        self.perform_journaled_mutation(commit_mutation)
            .map_err(|error| format!("failed to exchange checksum commit marker: {error}"))?;
        self.finish_last_mutation(PublishPhase::AfterChecksumCommitMutation, hook)?;
        self.checksum_staged.identity = rebound_target_identity(
            &self.destination,
            &self.destination_checksum_name,
            self.checksum_staged.identity,
        )?;
        self.checksum_sentinel.identity = rebound_target_identity(
            &self.recovery,
            ComponentKind::Checksum.staging_name(),
            self.checksum_sentinel.identity,
        )?;
        sync_all(&[&self.recovery, &self.destination])?;
        self.verify_committed_namespace()
    }

    fn batch_journal_item(&self, epoch: u64) -> ArchiveBatchJournalItem {
        ArchiveBatchJournalItem {
            epoch,
            source_path: path_bytes(&self.source.path),
            source_identity: self.source.identity,
            recovery_name: self.recovery_name.as_bytes().to_vec(),
            recovery_identity: self.recovery.identity,
            archive_name: self.archive_staged.name.as_bytes().to_vec(),
            archive_identity: self.archive_staged.identity,
            manifest_staged_identity: self.manifest_staging_identity,
            checksum_staged_identity: self.checksum_staged.identity,
            checksum_sentinel_identity: self.checksum_sentinel.identity,
            destination_name: self.destination_name.as_bytes().to_vec(),
            destination_manifest_name: self.destination_manifest_name.as_bytes().to_vec(),
            destination_checksum_name: self.destination_checksum_name.as_bytes().to_vec(),
            initial: self.initial,
            evidence: self.evidence.into(),
        }
    }

    fn batch_publication(&mut self) -> Result<ArchivePublication, String> {
        self.verify_finalized_batch_namespace()?;
        let evidence = self
            .published_evidence
            .ok_or_else(|| "committed batch archive has no bound evidence".to_string())?;
        let retains_backups = self.initial.archive.is_some()
            || self.initial.manifest.is_some()
            || self.initial.checksum.is_some();
        Ok(ArchivePublication {
            archive_path: self.destination.path.join(&self.destination_name),
            manifest_path: self
                .installed_manifest_identity
                .map(|_| self.destination.path.join(&self.destination_manifest_name)),
            checksum_path: self.destination.path.join(&self.destination_checksum_name),
            recovery_directory: retains_backups.then(|| self.recovery.path.clone()),
            evidence,
        })
    }

    fn batch_identity_evidence(&self, epoch: u64) -> Result<ArchiveBatchIdentityEvidence, String> {
        let committed_archive = target_identity(&self.destination, &self.destination_name)?
            .ok_or_else(|| "committed batch archive disappeared".to_string())?;
        let committed_manifest =
            target_identity(&self.destination, &self.destination_manifest_name)?;
        let committed_checksum =
            target_identity(&self.destination, &self.destination_checksum_name)?
                .ok_or_else(|| "committed batch checksum disappeared".to_string())?;
        if !committed_archive.same_across_rename(
            self.installed_archive_identity
                .ok_or_else(|| "committed batch archive has no installed identity".to_string())?,
        ) || !option_identities_correspond(committed_manifest, self.installed_manifest_identity)
            || !committed_checksum.same_across_rename(self.checksum_staged.identity)
        {
            return Err("committed batch identity evidence changed after verification".into());
        }
        Ok(ArchiveBatchIdentityEvidence {
            epoch,
            initial_archive: self.initial.archive.map(Into::into),
            initial_manifest: self.initial.manifest.map(Into::into),
            initial_checksum: self.initial.checksum.map(Into::into),
            committed_archive: committed_archive.into(),
            committed_manifest: committed_manifest.map(Into::into),
            committed_checksum: committed_checksum.into(),
            archive_validation: self
                .published_evidence
                .ok_or_else(|| "committed batch archive has no validation evidence".to_string())?,
        })
    }

    fn fail_after_checksum_commit(
        mut self,
        cause: String,
        hook: &mut dyn FnMut(PublishPhase) -> Result<(), String>,
    ) -> Result<ArchivePublication, ArchivePublicationError> {
        let Some(commit_mutation) = self
            .mutations
            .last()
            .filter(|mutation| mutation.role() == MutationRole::ChecksumCommit)
            .cloned()
        else {
            return Err(self.committed_failure(format!(
                "post-commit failure has no checksum exchange journal: {cause}"
            )));
        };
        if let Err(error) = self.reverse_mutation_namespace(&commit_mutation) {
            return Err(self.committed_failure(format!(
                "post-commit verification failed: {cause}; failed to replace the canonical checksum with the invalid sentinel: {error}"
            )));
        }
        self.mutations.pop();

        let mut invalidation_failures = Vec::new();
        if let Err(error) = self.sync_namespace_mutation(&commit_mutation) {
            invalidation_failures.push(error);
        }
        if let Err(error) = self.verify_reversed_mutation(&commit_mutation) {
            invalidation_failures.push(error);
        }
        if !invalidation_failures.is_empty() {
            return Err(self.committed_failure(format!(
                "post-commit verification failed: {cause}; checksum invalidation could not be proven durable: {}",
                invalidation_failures.join("; ")
            )));
        }
        Err(self.rollback_error(
            format!("post-commit verification failed after the checksum was durably invalidated: {cause}"),
            hook,
        ))
    }

    fn committed_failure(&self, message: String) -> ArchivePublicationError {
        ArchivePublicationError {
            message: format!(
                "{message}; publication state is indeterminate and recovery data remains in {}",
                self.recovery.path.display()
            ),
            committed: true,
            recovery_directory: Some(self.recovery.path.clone()),
        }
    }

    fn verify_staged(&self, staged: &StagedComponent) -> Result<(), String> {
        require_expected_target(
            self.directory(staged.directory_index),
            &staged.name,
            Some(staged.identity),
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn install_component(
        &mut self,
        kind: ComponentKind,
        source_directory: DirectoryIndex,
        source_name: std::ffi::OsString,
        staged_identity: FileIdentity,
        destination_name: std::ffi::OsString,
        expected_target: Option<FileIdentity>,
        install_phase: PublishPhase,
        backup_phase: PublishPhase,
        hook: &mut dyn FnMut(PublishPhase) -> Result<(), String>,
    ) -> Result<FileIdentity, String> {
        require_expected_target(
            self.directory(source_directory),
            &source_name,
            Some(staged_identity),
        )?;
        require_expected_target(&self.destination, &destination_name, expected_target)?;
        match expected_target {
            None => {
                let mutation = Mutation::Rename {
                    role: MutationRole::Data,
                    source: NamespaceEntry {
                        directory: source_directory,
                        name: source_name.clone(),
                    },
                    destination: NamespaceEntry {
                        directory: DirectoryIndex::Destination,
                        name: destination_name.clone(),
                    },
                    identity: staged_identity,
                };
                self.perform_journaled_mutation(mutation).map_err(|error| {
                    format!(
                        "failed to install {} without replacing a concurrent target: {error}",
                        kind.label()
                    )
                })?;
                self.finish_last_mutation(install_phase, hook)?;
                let new_identity =
                    rebound_target_identity(&self.destination, &destination_name, staged_identity)?;
                require_expected_target(self.directory(source_directory), &source_name, None)?;
                Ok(new_identity)
            }
            Some(old_identity) => {
                let exchange = Mutation::Exchange {
                    role: MutationRole::Data,
                    left: NamespaceEntry {
                        directory: source_directory,
                        name: source_name.clone(),
                    },
                    right: NamespaceEntry {
                        directory: DirectoryIndex::Destination,
                        name: destination_name.clone(),
                    },
                    left_identity: staged_identity,
                    right_identity: old_identity,
                };
                self.perform_journaled_mutation(exchange)
                    .map_err(|error| format!("failed to exchange {}: {error}", kind.label()))?;
                self.finish_last_mutation(install_phase, hook)?;
                let new_identity =
                    rebound_target_identity(&self.destination, &destination_name, staged_identity)?;
                let exchanged_old_identity = rebound_target_identity(
                    self.directory(source_directory),
                    &source_name,
                    old_identity,
                )?;

                let backup = Mutation::Rename {
                    role: MutationRole::Data,
                    source: NamespaceEntry {
                        directory: source_directory,
                        name: source_name.clone(),
                    },
                    destination: NamespaceEntry {
                        directory: DirectoryIndex::Recovery,
                        name: kind.backup_name().to_os_string(),
                    },
                    identity: exchanged_old_identity,
                };
                self.perform_journaled_mutation(backup).map_err(|error| {
                    format!("failed to retain replaced {}: {error}", kind.label())
                })?;
                self.finish_last_mutation(backup_phase, hook)?;
                rebound_target_identity(
                    &self.recovery,
                    kind.backup_name(),
                    exchanged_old_identity,
                )?;
                require_expected_target(self.directory(source_directory), &source_name, None)?;
                Ok(new_identity)
            }
        }
    }

    fn remove_destination_to_backup(
        &mut self,
        kind: ComponentKind,
        destination_name: std::ffi::OsString,
        expected_identity: FileIdentity,
        phase: PublishPhase,
        hook: &mut dyn FnMut(PublishPhase) -> Result<(), String>,
    ) -> Result<(), String> {
        require_expected_target(
            &self.destination,
            &destination_name,
            Some(expected_identity),
        )?;
        let mutation = Mutation::Rename {
            role: MutationRole::Data,
            source: NamespaceEntry {
                directory: DirectoryIndex::Destination,
                name: destination_name.clone(),
            },
            destination: NamespaceEntry {
                directory: DirectoryIndex::Recovery,
                name: kind.backup_name().to_os_string(),
            },
            identity: expected_identity,
        };
        self.perform_journaled_mutation(mutation)
            .map_err(|error| format!("failed to retain previous {}: {error}", kind.label()))?;
        self.finish_last_mutation(phase, hook)?;
        rebound_target_identity(&self.recovery, kind.backup_name(), expected_identity)?;
        require_expected_target(&self.destination, &destination_name, None)
    }

    fn perform_journaled_mutation(&mut self, mutation: Mutation) -> io::Result<()> {
        match &mutation {
            Mutation::Rename {
                source,
                destination,
                ..
            } => rename_noreplace(
                self.directory(source.directory),
                &source.name,
                self.directory(destination.directory),
                &destination.name,
            )?,
            Mutation::Exchange { left, right, .. } => rename_exchange(
                self.directory(left.directory),
                &left.name,
                self.directory(right.directory),
                &right.name,
            )?,
        }
        self.mutations.push(mutation);
        Ok(())
    }

    fn finish_last_mutation(
        &self,
        phase: PublishPhase,
        hook: &mut dyn FnMut(PublishPhase) -> Result<(), String>,
    ) -> Result<(), String> {
        let mutation = self
            .mutations
            .last()
            .ok_or_else(|| "namespace mutation was not journaled".to_string())?;
        let mut failures = Vec::new();
        // The test hook runs before the barrier so it can inject an fsync
        // fault. Even a rejected hook cannot skip durability for a namespace
        // mutation that has already completed.
        if let Err(error) = hook(phase) {
            failures.push(error);
        }
        if let Err(error) = self.sync_namespace_mutation(mutation) {
            failures.push(error);
        }
        if failures.is_empty() {
            Ok(())
        } else {
            Err(failures.join("; "))
        }
    }

    fn sync_namespace_mutation(&self, mutation: &Mutation) -> Result<(), String> {
        let (left, right) = match mutation {
            Mutation::Rename {
                source,
                destination,
                ..
            } => (source.directory, destination.directory),
            Mutation::Exchange { left, right, .. } => (left.directory, right.directory),
        };
        if left == right {
            self.directory(left).sync()
        } else {
            sync_all(&[self.directory(left), self.directory(right)])
        }
    }

    fn refresh_published_evidence(&mut self) -> Result<(), String> {
        let archive_identity = self
            .installed_archive_identity
            .ok_or_else(|| "archive was not installed".to_string())?;
        require_expected_target(
            &self.destination,
            &self.destination_name,
            Some(archive_identity),
        )?;
        let rebound = rebind_validated_after_rename(&self.archive_staged.file, self.evidence)
            .map_err(|error| {
                format!("published archive changed across controlled rename: {error}")
            })?;
        self.published_evidence = Some(rebound);
        Ok(())
    }

    fn verify_commit_ready(&mut self) -> Result<(), String> {
        self.source.recheck_path()?;
        self.destination.recheck_path()?;
        require_recovery_binding(&self.destination, &self.recovery_name, &self.recovery)?;
        self.refresh_published_evidence()?;
        require_expected_target(
            &self.destination,
            &self.destination_manifest_name,
            self.installed_manifest_identity,
        )?;
        require_expected_target(
            &self.destination,
            &self.destination_checksum_name,
            Some(self.checksum_sentinel.identity),
        )?;
        require_expected_target(
            &self.recovery,
            ComponentKind::Checksum.staging_name(),
            Some(self.checksum_staged.identity),
        )
    }

    fn verify_committed_namespace(&mut self) -> Result<(), String> {
        self.source.recheck_path()?;
        self.destination.recheck_path()?;
        require_recovery_binding(&self.destination, &self.recovery_name, &self.recovery)?;
        self.refresh_published_evidence()?;
        require_expected_target(
            &self.destination,
            &self.destination_manifest_name,
            self.installed_manifest_identity,
        )?;
        require_expected_target(
            &self.destination,
            &self.destination_checksum_name,
            Some(self.checksum_staged.identity),
        )?;
        require_expected_target(
            &self.recovery,
            ComponentKind::Checksum.staging_name(),
            Some(self.checksum_sentinel.identity),
        )
    }

    fn verify_finalized_batch_namespace(&mut self) -> Result<(), String> {
        self.source.recheck_path()?;
        self.destination.recheck_path()?;
        require_recovery_binding(&self.destination, &self.recovery_name, &self.recovery)?;
        self.refresh_published_evidence()?;
        require_expected_target(
            &self.destination,
            &self.destination_manifest_name,
            self.installed_manifest_identity,
        )?;
        require_expected_target(
            &self.destination,
            &self.destination_checksum_name,
            Some(self.checksum_staged.identity),
        )?;
        require_expected_target(&self.recovery, ComponentKind::Checksum.staging_name(), None)?;
        let evidence = self
            .published_evidence
            .ok_or_else(|| "finalized batch archive has no bound evidence".to_string())?;
        let expected_checksum = archive_checksum_line(&evidence.sha256, &self.destination_name)
            .map_err(|error| error.to_string())?;
        let checksum = bind_required_regular(
            &self.destination,
            &self.destination_checksum_name,
            ComponentKind::Checksum,
        )?;
        if read_bound_file(&checksum.file, expected_checksum.len() as u64)?
            != expected_checksum.as_bytes()
        {
            return Err("finalized batch checksum is not canonical".to_string());
        }
        Ok(())
    }

    fn sync_mutation_directories(&self) -> Result<(), String> {
        sync_all(&[&self.source, &self.recovery, &self.destination])
    }

    fn rollback_error(
        mut self,
        cause: String,
        hook: &mut dyn FnMut(PublishPhase) -> Result<(), String>,
    ) -> ArchivePublicationError {
        let rollback = self.rollback(hook);
        match rollback {
            Ok(()) => ArchivePublicationError {
                message: format!(
                    "archive publication failed before checksum commit and was rolled back: {cause}"
                ),
                committed: false,
                recovery_directory: None,
            },
            Err(rollback_error) => ArchivePublicationError {
                message: format!(
                    "archive publication failed with no durable canonical checksum: {cause}; rollback could not restore the exact initial namespace: {rollback_error}; recovery data remains in {}",
                    self.recovery.path.display()
                ),
                committed: false,
                recovery_directory: Some(self.recovery.path.clone()),
            },
        }
    }

    fn rollback(
        &mut self,
        hook: &mut dyn FnMut(PublishPhase) -> Result<(), String>,
    ) -> Result<(), String> {
        if self
            .mutations
            .iter()
            .any(|mutation| mutation.role() == MutationRole::ChecksumCommit)
        {
            return Err(
                "canonical checksum is still installed; refusing data rollback".to_string(),
            );
        }

        let data_mutations = self
            .mutations
            .iter()
            .rev()
            .filter(|mutation| mutation.role() == MutationRole::Data)
            .cloned()
            .collect::<Vec<_>>();
        let mut failures = Vec::new();
        for mutation in &data_mutations {
            match self.reverse_mutation_namespace(mutation) {
                Ok(()) => {
                    let failure_count = failures.len();
                    if let Err(error) = hook(PublishPhase::BeforeRollbackMutationSync) {
                        failures.push(format!(
                            "rollback mutation durability barrier was rejected: {error}"
                        ));
                    }
                    if let Err(error) = self.verify_reversed_mutation(mutation) {
                        failures.push(error);
                    }
                    if let Err(error) = self.sync_namespace_mutation(mutation) {
                        failures.push(error);
                    }
                    if failures.len() != failure_count {
                        break;
                    }
                }
                Err(error) => {
                    failures.push(error);
                    break;
                }
            }
        }

        if let Err(error) = hook(PublishPhase::BeforeRollbackDataSync) {
            failures.push(format!(
                "rollback data durability barrier was rejected: {error}"
            ));
        }
        if let Err(error) = self.sync_mutation_directories() {
            failures.push(error);
        }
        if let Err(error) = self.verify_initial_data_namespace() {
            failures.push(error);
        }
        if failures.is_empty()
            && let Err(error) = hook(PublishPhase::AfterRollbackDataSync)
        {
            failures.push(error);
        }
        if !failures.is_empty() {
            return Err(failures.join("; "));
        }

        if let Some(reservation) = self
            .mutations
            .iter()
            .find(|mutation| mutation.role() == MutationRole::ChecksumReservation)
            .cloned()
        {
            self.reverse_mutation_namespace(&reservation)?;
            if let Err(error) = hook(PublishPhase::AfterChecksumRestoreMutation) {
                failures.push(error);
            }
            if let Err(error) = self.verify_reversed_mutation(&reservation) {
                failures.push(error);
            }
            if let Err(error) = self.sync_namespace_mutation(&reservation) {
                failures.push(error);
            }
            if let Err(error) = hook(PublishPhase::AfterChecksumRollbackSync) {
                failures.push(error);
            }
        } else if let Err(error) = target_corresponds(
            &self.destination,
            &self.destination_checksum_name,
            self.initial.checksum,
        )
        .and_then(|matches| {
            matches
                .then_some(())
                .ok_or_else(|| "checksum changed before rollback".to_string())
        }) {
            failures.push(error);
        }
        if !failures.is_empty() {
            return Err(failures.join("; "));
        }

        self.verify_initial_namespace()?;
        let mut cleanup_failures = Vec::new();
        if let Some(identity) = self.manifest_staging_identity
            && let Err(error) = remove_corresponding_file(
                &self.recovery,
                ComponentKind::Manifest.staging_name(),
                identity,
            )
        {
            cleanup_failures.push(error);
        }
        for (name, identity) in [
            (
                ComponentKind::Checksum.staging_name(),
                self.checksum_staged.identity,
            ),
            (
                ComponentKind::Checksum.backup_name(),
                self.checksum_sentinel.identity,
            ),
        ] {
            if let Err(error) = remove_corresponding_file(&self.recovery, name, identity) {
                cleanup_failures.push(error);
            }
        }
        collect_sync_failures(&[&self.recovery, &self.destination], &mut cleanup_failures);
        if cleanup_failures.is_empty()
            && let Err(error) = remove_empty_recovery_directory(
                &self.destination,
                &self.recovery_name,
                &self.recovery,
            )
        {
            cleanup_failures.push(error);
        }
        if cleanup_failures.is_empty() {
            Ok(())
        } else {
            Err(cleanup_failures.join("; "))
        }
    }

    fn verify_initial_data_namespace(&self) -> Result<(), String> {
        require_corresponding_target(
            &self.destination,
            &self.destination_name,
            self.initial.archive,
            "archive",
        )?;
        require_corresponding_target(
            &self.destination,
            &self.destination_manifest_name,
            self.initial.manifest,
            "segment manifest",
        )?;
        require_corresponding_target(
            &self.source,
            &self.archive_staged.name,
            Some(self.archive_staged.identity),
            "staged archive",
        )
    }

    fn verify_initial_namespace(&self) -> Result<(), String> {
        self.verify_initial_data_namespace()?;
        require_corresponding_target(
            &self.destination,
            &self.destination_checksum_name,
            self.initial.checksum,
            "checksum",
        )
    }

    fn reverse_mutation_namespace(&self, mutation: &Mutation) -> Result<(), String> {
        match mutation {
            Mutation::Rename {
                source,
                destination,
                identity,
                ..
            } => {
                require_corresponding_target(
                    self.directory(source.directory),
                    &source.name,
                    None,
                    "rollback rename source",
                )?;
                require_corresponding_target(
                    self.directory(destination.directory),
                    &destination.name,
                    Some(*identity),
                    "rollback rename destination",
                )?;
                rename_noreplace(
                    self.directory(destination.directory),
                    &destination.name,
                    self.directory(source.directory),
                    &source.name,
                )
                .map_err(|error| format!("failed to reverse namespace rename: {error}"))
            }
            Mutation::Exchange {
                left,
                right,
                left_identity,
                right_identity,
                ..
            } => {
                require_corresponding_target(
                    self.directory(left.directory),
                    &left.name,
                    Some(*right_identity),
                    "rollback exchange left",
                )?;
                require_corresponding_target(
                    self.directory(right.directory),
                    &right.name,
                    Some(*left_identity),
                    "rollback exchange right",
                )?;
                rename_exchange(
                    self.directory(left.directory),
                    &left.name,
                    self.directory(right.directory),
                    &right.name,
                )
                .map_err(|error| format!("failed to reverse namespace exchange: {error}"))
            }
        }
    }

    fn verify_reversed_mutation(&self, mutation: &Mutation) -> Result<(), String> {
        match mutation {
            Mutation::Rename {
                source,
                destination,
                identity,
                ..
            } => {
                require_corresponding_target(
                    self.directory(source.directory),
                    &source.name,
                    Some(*identity),
                    "reversed rename source",
                )?;
                require_corresponding_target(
                    self.directory(destination.directory),
                    &destination.name,
                    None,
                    "reversed rename destination",
                )
            }
            Mutation::Exchange {
                left,
                right,
                left_identity,
                right_identity,
                ..
            } => {
                require_corresponding_target(
                    self.directory(left.directory),
                    &left.name,
                    Some(*left_identity),
                    "reversed exchange left",
                )?;
                require_corresponding_target(
                    self.directory(right.directory),
                    &right.name,
                    Some(*right_identity),
                    "reversed exchange right",
                )
            }
        }
    }
}

fn path_bytes(path: &Path) -> Vec<u8> {
    path.as_os_str().as_bytes().to_vec()
}

fn require_canonical_absolute_path(path: &Path, label: &str) -> Result<(), String> {
    if !path.is_absolute() {
        return Err(format!("{label} path must be absolute: {}", path.display()));
    }
    let mut rebuilt = PathBuf::from("/");
    for component in path.components().skip(1) {
        let std::path::Component::Normal(name) = component else {
            return Err(format!(
                "{label} path has a non-canonical component: {}",
                path.display()
            ));
        };
        rebuilt.push(name);
    }
    if rebuilt.as_os_str().as_bytes() != path.as_os_str().as_bytes() {
        return Err(format!(
            "{label} path is not in canonical lexical form: {}",
            path.display()
        ));
    }
    Ok(())
}

fn archive_batch_transaction_nonce(
    transactions: &[PublicationTransaction],
    items: &[ArchiveBatchItem],
    manifest_fingerprint: [u8; 32],
) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(b"jetstreamer archive batch transaction v1\0");
    digest.update(manifest_fingerprint);
    digest.update(std::process::id().to_le_bytes());
    digest.update(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
            .to_le_bytes(),
    );
    for (transaction, item) in transactions.iter().zip(items) {
        digest.update(item.epoch.to_le_bytes());
        digest.update(item.evidence.sha256);
        digest.update(transaction.source.identity.dev.to_le_bytes());
        digest.update(transaction.source.identity.ino.to_le_bytes());
        digest.update(transaction.recovery.identity.dev.to_le_bytes());
        digest.update(transaction.recovery.identity.ino.to_le_bytes());
        digest.update(transaction.archive_staged.identity.ino.to_le_bytes());
        digest.update(path_bytes(&item.staged_archive));
        digest.update([0]);
        digest.update(path_bytes(&item.destination_archive));
        digest.update([0]);
    }
    digest.finalize().into()
}

fn path_from_bytes(bytes: &[u8], label: &str) -> Result<PathBuf, String> {
    if bytes.is_empty() || bytes.contains(&0) {
        return Err(format!("batch journal contains an invalid {label} path"));
    }
    let path = PathBuf::from(std::ffi::OsString::from_vec(bytes.to_vec()));
    require_canonical_absolute_path(&path, &format!("batch journal {label}"))?;
    Ok(path)
}

fn name_from_bytes(bytes: &[u8], label: &str) -> Result<std::ffi::OsString, String> {
    let name = std::ffi::OsString::from_vec(bytes.to_vec());
    let path = Path::new(&name);
    if bytes.is_empty()
        || bytes == b"."
        || bytes == b".."
        || bytes.contains(&b'/')
        || bytes.contains(&0)
        || path.file_name() != Some(name.as_os_str())
    {
        return Err(format!("batch journal contains an unsafe {label} filename"));
    }
    Ok(name)
}

fn validate_archive_batch_request(
    items: &[ArchiveBatchItem],
) -> Result<PathBuf, ArchivePublicationError> {
    if items.is_empty() {
        return Err(preflight_error(
            "archive publication batch must not be empty".to_string(),
        ));
    }
    if items.len() > MAX_ARCHIVE_BATCH_ITEMS {
        return Err(preflight_error(format!(
            "archive publication batch contains {} entries; maximum is {MAX_ARCHIVE_BATCH_ITEMS}",
            items.len()
        )));
    }
    let destination_path = items[0]
        .destination_archive
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .to_path_buf();
    require_canonical_absolute_path(&destination_path, "archive batch destination directory")
        .map_err(preflight_error)?;

    let mut epochs = HashSet::with_capacity(items.len());
    let mut destination_names = HashSet::with_capacity(items.len().saturating_mul(3));
    let mut staged_paths = HashSet::with_capacity(items.len());
    let mut previous_epoch = None;
    for item in items {
        require_canonical_absolute_path(&item.staged_archive, "staged archive")
            .map_err(preflight_error)?;
        require_canonical_absolute_path(&item.destination_archive, "destination archive")
            .map_err(preflight_error)?;
        if !epochs.insert(item.epoch) {
            return Err(preflight_error(format!(
                "archive batch contains duplicate epoch {}",
                item.epoch
            )));
        }
        if previous_epoch.is_some_and(|previous| item.epoch <= previous) {
            return Err(preflight_error(format!(
                "archive batch epochs must be strictly increasing; epoch {} is out of order",
                item.epoch
            )));
        }
        previous_epoch = Some(item.epoch);
        if item.destination_archive.parent() != Some(destination_path.as_path()) {
            return Err(preflight_error(format!(
                "archive batch destinations must share exactly one directory: {}",
                item.destination_archive.display()
            )));
        }
        let staged_name = safe_file_name(&item.staged_archive).map_err(preflight_error)?;
        let destination_name =
            safe_file_name(&item.destination_archive).map_err(preflight_error)?;
        if staged_name != destination_name {
            return Err(preflight_error(format!(
                "staging and destination archive filenames differ for epoch {}",
                item.epoch
            )));
        }
        if !staged_paths.insert(item.staged_archive.clone()) {
            return Err(preflight_error(format!(
                "archive batch contains duplicate staged path {}",
                item.staged_archive.display()
            )));
        }
        let manifest_name = safe_file_name(
            &segment_manifest_path(&item.destination_archive)
                .map_err(|error| preflight_error(error.to_string()))?,
        )
        .map_err(preflight_error)?;
        let checksum_name = safe_file_name(
            &archive_checksum_path(&item.destination_archive)
                .map_err(|error| preflight_error(error.to_string()))?,
        )
        .map_err(preflight_error)?;
        for name in [destination_name, manifest_name, checksum_name] {
            if !destination_names.insert(name.clone()) {
                return Err(preflight_error(format!(
                    "archive batch destination namespaces overlap at {:?}",
                    name
                )));
            }
        }
        let expected_name = std::ffi::OsString::from(format!("epoch-{}.jet", item.epoch));
        if staged_name != expected_name {
            return Err(preflight_error(format!(
                "archive batch destination for epoch {} must be named {:?}",
                item.epoch, expected_name
            )));
        }
    }
    Ok(destination_path)
}

fn require_one_batch_destination(transactions: &[PublicationTransaction]) -> Result<(), String> {
    let first = transactions
        .first()
        .ok_or_else(|| "archive batch unexpectedly contains no transactions".to_string())?;
    let expected = first.destination.identity;
    let rebound_destination = BoundDirectory::bind_absolute_nofollow(
        &first.destination.path,
        DirectoryPolicy::SharedDestination,
    )?;
    if !rebound_destination
        .identity
        .same_directory_binding(expected)
    {
        return Err("archive batch destination path changed during preflight".to_string());
    }
    for transaction in &transactions[1..] {
        if !transaction
            .destination
            .identity
            .same_directory_binding(expected)
        {
            return Err("archive batch destination directory changed during preflight".to_string());
        }
    }
    let mut source_namespaces = HashSet::with_capacity(transactions.len());
    let mut archive_inodes = HashSet::with_capacity(transactions.len());
    for transaction in transactions {
        let rebound_source = BoundDirectory::bind_absolute_nofollow(
            &transaction.source.path,
            DirectoryPolicy::PrivateSource,
        )?;
        if !rebound_source
            .identity
            .same_directory_binding(transaction.source.identity)
        {
            return Err(format!(
                "archive batch source path changed during preflight: {}",
                transaction.source.path.display()
            ));
        }
        if !source_namespaces.insert((
            transaction.source.identity.dev,
            transaction.source.identity.ino,
            transaction.archive_staged.name.as_bytes().to_vec(),
        )) || !archive_inodes.insert((
            transaction.archive_staged.identity.dev,
            transaction.archive_staged.identity.ino,
        )) {
            return Err("archive batch contains an aliased staged archive".to_string());
        }
    }
    Ok(())
}

fn cleanup_prepared_batch_transactions(transactions: &mut [PublicationTransaction]) -> Vec<String> {
    let mut failures = Vec::new();
    for transaction in transactions.iter_mut().rev() {
        let mut hook = |_| Ok(());
        if let Err(error) = transaction.rollback(&mut hook) {
            failures.push(format!(
                "failed to clean prepared recovery directory {}: {error}",
                transaction.recovery.path.display()
            ));
        }
    }
    failures
}

fn append_batch_cleanup_error(
    mut error: ArchivePublicationError,
    cleanup: Vec<String>,
) -> ArchivePublicationError {
    if !cleanup.is_empty() {
        error.message.push_str(&format!(
            "; prepared batch cleanup failed: {}",
            cleanup.join("; ")
        ));
        error.recovery_directory = None;
    }
    error
}

fn batch_indeterminate_error(
    destination: &Path,
    message: String,
    committed: bool,
) -> ArchivePublicationError {
    ArchivePublicationError {
        message: format!(
            "archive batch transaction requires restart recovery in {}: {message}",
            destination.display()
        ),
        committed,
        recovery_directory: Some(destination.join(ARCHIVE_BATCH_TRANSACTION_DIRECTORY)),
    }
}

fn archive_batch_marker_exists(destination_path: &Path) -> Result<bool, String> {
    let destination = BoundDirectory::bind_absolute_nofollow(
        destination_path,
        DirectoryPolicy::SharedDestination,
    )?;
    Ok(bind_archive_batch_marker(&destination)?.is_some()
        || bind_archive_batch_outcome(&destination)?.is_some())
}

fn bind_archive_batch_marker(
    destination: &BoundDirectory,
) -> Result<Option<BoundDirectory>, String> {
    bind_named_archive_batch_marker(destination, OsStr::new(ARCHIVE_BATCH_TRANSACTION_DIRECTORY))
}

fn bind_archive_batch_outcome(
    destination: &BoundDirectory,
) -> Result<Option<BoundDirectory>, String> {
    bind_named_archive_batch_marker(destination, OsStr::new(ARCHIVE_BATCH_OUTCOME_DIRECTORY))
}

fn bind_named_archive_batch_marker(
    destination: &BoundDirectory,
    name: &OsStr,
) -> Result<Option<BoundDirectory>, String> {
    let Some(identity) = fstatat_identity(destination.file.as_raw_fd(), name)
        .map_err(|error| format!("failed to inspect archive batch marker: {error}"))?
    else {
        return Ok(None);
    };
    if identity.mode & libc::S_IFMT != libc::S_IFDIR {
        return Err(format!(
            "archive batch marker is not a real directory: {}",
            destination.path.join(name).display()
        ));
    }
    BoundDirectory::bind_child(
        destination,
        name,
        destination.path.join(name),
        DirectoryPolicy::PrivateRecovery,
    )
    .map(Some)
}

fn lock_archive_batch_marker(marker: &BoundDirectory) -> Result<(), String> {
    // SAFETY: marker.file owns a live directory descriptor. flock does not
    // dereference memory and the nonblocking lock is released with the fd.
    if unsafe { libc::flock(marker.file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } == 0 {
        Ok(())
    } else {
        Err(format!(
            "archive batch transaction {} is owned by a live process: {}",
            marker.path.display(),
            io::Error::last_os_error()
        ))
    }
}

fn create_archive_batch_marker(
    destination: &BoundDirectory,
    journal: &ArchiveBatchJournal,
) -> Result<BoundDirectory, ArchivePublicationError> {
    let name = OsStr::new(ARCHIVE_BATCH_TRANSACTION_DIRECTORY);
    let name_c = os_str_cstring(name).map_err(|error| preflight_error(error.to_string()))?;
    // SAFETY: destination is a live directory and name_c is a NUL-terminated
    // basename. O_EXCL semantics come from mkdirat returning EEXIST.
    if unsafe {
        libc::mkdirat(
            destination.file.as_raw_fd(),
            name_c.as_ptr(),
            0o700 as libc::mode_t,
        )
    } != 0
    {
        let error = io::Error::last_os_error();
        return Err(preflight_error(
            if error.kind() == io::ErrorKind::AlreadyExists {
                format!(
                    "archive batch marker already exists at {}; recover it before starting another batch",
                    destination.path.join(name).display()
                )
            } else {
                format!("failed to create archive batch marker: {error}")
            },
        ));
    }
    let marker = BoundDirectory::bind_child(
        destination,
        name,
        destination.path.join(name),
        DirectoryPolicy::PrivateRecovery,
    )
    .map_err(|message| preflight_error_with_recovery(message, Some(destination.path.join(name))))?;
    lock_archive_batch_marker(&marker)
        .map_err(|message| preflight_error_with_recovery(message, Some(marker.path.clone())))?;
    let outcome = match bind_archive_batch_outcome(destination) {
        Ok(outcome) => outcome,
        Err(message) => {
            let cleanup = remove_archive_batch_marker_directory(
                destination,
                &marker,
                OsStr::new(ARCHIVE_BATCH_TRANSACTION_DIRECTORY),
                Some(false),
            );
            return Err(match cleanup {
                Ok(()) => preflight_error(message),
                Err(cleanup) => preflight_error_with_recovery(
                    format!(
                        "{message}; failed to remove unused active marker after outcome inspection failure: {cleanup}"
                    ),
                    Some(marker.path.clone()),
                ),
            });
        }
    };
    if outcome.is_some() {
        let cleanup = remove_archive_batch_marker_directory(
            destination,
            &marker,
            OsStr::new(ARCHIVE_BATCH_TRANSACTION_DIRECTORY),
            Some(false),
        );
        let message =
            "an unacknowledged archive batch outcome appeared during publication preflight";
        return Err(match cleanup {
            Ok(()) => preflight_error(message.to_string()),
            Err(cleanup) => preflight_error_with_recovery(
                format!("{message}; failed to remove unused active marker: {cleanup}"),
                Some(marker.path.clone()),
            ),
        });
    }

    let create = (|| -> Result<(), String> {
        write_initial_archive_batch_journal(&marker, destination.identity.gid, journal)?;
        create_bound_file(
            &marker,
            ComponentKind::Checksum,
            OsStr::new(ARCHIVE_BATCH_ARMED),
            &archive_batch_armed_contents(journal.transaction_id),
            Some(destination.identity.gid),
            0o400,
        )?;
        sync_all(&[&marker, destination])
    })();
    if let Err(message) = create {
        let cleanup = remove_archive_batch_marker_directory(
            destination,
            &marker,
            OsStr::new(ARCHIVE_BATCH_TRANSACTION_DIRECTORY),
            None,
        );
        let recovery_directory = cleanup.as_ref().err().map(|_| marker.path.clone());
        let message = match cleanup {
            Ok(()) => message,
            Err(cleanup) => {
                format!("{message}; failed to remove incomplete archive batch marker: {cleanup}")
            }
        };
        return Err(preflight_error_with_recovery(message, recovery_directory));
    }
    Ok(marker)
}

fn serialize_archive_batch_journal(journal: &ArchiveBatchJournal) -> Result<Vec<u8>, String> {
    let bytes = serde_json::to_vec_pretty(journal)
        .map_err(|error| format!("failed to serialize archive batch journal: {error}"))?;
    if bytes.len() as u64 > MAX_ARCHIVE_BATCH_JOURNAL_BYTES {
        return Err(format!(
            "archive batch journal is {} bytes; maximum is {MAX_ARCHIVE_BATCH_JOURNAL_BYTES}",
            bytes.len()
        ));
    }
    Ok(bytes)
}

fn archive_batch_journal_transaction_id(journal: &ArchiveBatchJournal) -> Result<[u8; 32], String> {
    let mut committed = journal.clone();
    committed.transaction_id = [0; 32];
    committed.decision = ArchiveBatchDecision::RollBack;
    let bytes = serde_json::to_vec(&committed)
        .map_err(|error| format!("failed to commit archive batch journal identity: {error}"))?;
    let mut digest = Sha256::new();
    digest.update(b"jetstreamer archive batch journal identity v1\0");
    digest.update(bytes);
    Ok(digest.finalize().into())
}

fn archive_batch_armed_contents(transaction_id: [u8; 32]) -> Vec<u8> {
    let mut contents = Vec::with_capacity(ARCHIVE_BATCH_ARMED_PREFIX.len() + transaction_id.len());
    contents.extend_from_slice(ARCHIVE_BATCH_ARMED_PREFIX);
    contents.extend_from_slice(&transaction_id);
    contents
}

fn write_initial_archive_batch_journal(
    marker: &BoundDirectory,
    destination_gid: u32,
    journal: &ArchiveBatchJournal,
) -> Result<(), String> {
    let bytes = serialize_archive_batch_journal(journal)?;
    create_bound_file(
        marker,
        ComponentKind::Checksum,
        OsStr::new(ARCHIVE_BATCH_JOURNAL),
        &bytes,
        Some(destination_gid),
        0o400,
    )?;
    marker.sync()
}

fn replace_archive_batch_journal(
    marker: &BoundDirectory,
    journal: &ArchiveBatchJournal,
) -> Result<(), String> {
    let bytes = serialize_archive_batch_journal(journal)?;
    let next = create_bound_file(
        marker,
        ComponentKind::Checksum,
        OsStr::new(ARCHIVE_BATCH_JOURNAL_NEXT),
        &bytes,
        Some(marker.identity.gid),
        0o400,
    )?;
    marker.sync()?;
    rename_exchange(
        marker,
        OsStr::new(ARCHIVE_BATCH_JOURNAL_NEXT),
        marker,
        OsStr::new(ARCHIVE_BATCH_JOURNAL),
    )
    .map_err(|error| format!("failed to install archive batch commit decision: {error}"))?;
    marker.sync()?;
    let rebound =
        rebound_target_identity(marker, OsStr::new(ARCHIVE_BATCH_JOURNAL), next.identity)?;
    require_corresponding_target(
        marker,
        OsStr::new(ARCHIVE_BATCH_JOURNAL),
        Some(rebound),
        "archive batch commit journal",
    )?;
    let previous = target_identity(marker, OsStr::new(ARCHIVE_BATCH_JOURNAL_NEXT))?
        .ok_or_else(|| "previous archive batch journal disappeared after exchange".to_string())?;
    unlink_corresponding_entry(marker, OsStr::new(ARCHIVE_BATCH_JOURNAL_NEXT), previous)?;
    marker.sync()
}

fn read_archive_batch_journal(
    destination: &BoundDirectory,
    marker: &BoundDirectory,
) -> Result<Option<ArchiveBatchJournal>, ArchivePublicationError> {
    let armed = bind_optional_regular(
        marker,
        OsStr::new(ARCHIVE_BATCH_ARMED),
        ComponentKind::Checksum,
    )
    .map_err(|message| preflight_error_with_recovery(message, Some(marker.path.clone())))?;
    let Some(armed) = armed else {
        return Ok(None);
    };
    validate_archive_batch_marker_entries(marker, Some(true))
        .map_err(|message| preflight_error_with_recovery(message, Some(marker.path.clone())))?;
    let armed_bytes = read_bound_file(&armed.file, (ARCHIVE_BATCH_ARMED_PREFIX.len() + 32) as u64)
        .map_err(|message| preflight_error_with_recovery(message, Some(marker.path.clone())))?;
    let journal_file = bind_required_regular(
        marker,
        OsStr::new(ARCHIVE_BATCH_JOURNAL),
        ComponentKind::Checksum,
    )
    .map_err(|message| preflight_error_with_recovery(message, Some(marker.path.clone())))?;
    let bytes = read_bound_file(&journal_file.file, MAX_ARCHIVE_BATCH_JOURNAL_BYTES)
        .map_err(|message| preflight_error_with_recovery(message, Some(marker.path.clone())))?;
    let journal: ArchiveBatchJournal = serde_json::from_slice(&bytes).map_err(|error| {
        preflight_error_with_recovery(
            format!("failed to parse armed archive batch journal: {error}"),
            Some(marker.path.clone()),
        )
    })?;
    validate_archive_batch_journal(destination, &journal)
        .map_err(|message| preflight_error_with_recovery(message, Some(marker.path.clone())))?;
    if armed_bytes != archive_batch_armed_contents(journal.transaction_id) {
        return Err(preflight_error_with_recovery(
            "archive batch armed marker does not bind the journal transaction ID".to_string(),
            Some(marker.path.clone()),
        ));
    }
    Ok(Some(journal))
}

fn validate_archive_batch_journal(
    destination: &BoundDirectory,
    journal: &ArchiveBatchJournal,
) -> Result<(), String> {
    if journal.version != ARCHIVE_BATCH_JOURNAL_VERSION {
        return Err(format!(
            "unsupported archive batch journal version {}",
            journal.version
        ));
    }
    if journal.transaction_id == [0; 32] {
        return Err("archive batch journal has an invalid zero transaction ID".into());
    }
    if journal.transaction_nonce == [0; 32] {
        return Err("archive batch journal has an invalid zero transaction nonce".into());
    }
    if journal.manifest_fingerprint == [0; 32] {
        return Err("archive batch journal has an invalid zero manifest fingerprint".into());
    }
    if journal.items.is_empty() || journal.items.len() > MAX_ARCHIVE_BATCH_ITEMS {
        return Err(format!(
            "archive batch journal item count {} is outside 1..={MAX_ARCHIVE_BATCH_ITEMS}",
            journal.items.len()
        ));
    }
    let recorded_destination = path_from_bytes(&journal.destination_path, "destination")?;
    if recorded_destination != destination.path
        || !journal
            .destination_identity
            .same_directory_binding(destination.identity)
    {
        return Err("archive batch journal is bound to a different destination directory".into());
    }

    let mut epochs = HashSet::with_capacity(journal.items.len());
    let mut destination_names = HashSet::with_capacity(journal.items.len().saturating_mul(3));
    let mut recovery_names = HashSet::with_capacity(journal.items.len());
    let mut source_archives = HashSet::with_capacity(journal.items.len());
    let mut source_namespaces = HashSet::with_capacity(journal.items.len());
    let mut archive_inodes = HashSet::with_capacity(journal.items.len());
    let mut previous_epoch = None;
    for item in &journal.items {
        if !epochs.insert(item.epoch) {
            return Err(format!(
                "archive batch journal contains duplicate epoch {}",
                item.epoch
            ));
        }
        if previous_epoch.is_some_and(|previous| item.epoch <= previous) {
            return Err(format!(
                "archive batch journal epochs are not strictly increasing at {}",
                item.epoch
            ));
        }
        previous_epoch = Some(item.epoch);
        let source_path = path_from_bytes(&item.source_path, "source")?;
        let recovery_name = name_from_bytes(&item.recovery_name, "recovery")?;
        if !recovery_name
            .as_bytes()
            .starts_with(b".jetstreamer-recovery-")
            || !recovery_names.insert(recovery_name.clone())
        {
            return Err("archive batch journal has an invalid or duplicate recovery name".into());
        }
        let archive_name = name_from_bytes(&item.archive_name, "archive")?;
        let destination_name = name_from_bytes(&item.destination_name, "destination archive")?;
        let destination_manifest_name =
            name_from_bytes(&item.destination_manifest_name, "destination manifest")?;
        let destination_checksum_name =
            name_from_bytes(&item.destination_checksum_name, "destination checksum")?;
        let expected_name = std::ffi::OsString::from(format!("epoch-{}.jet", item.epoch));
        if archive_name != destination_name || destination_name != expected_name {
            return Err(format!(
                "archive batch journal filename does not bind epoch {}",
                item.epoch
            ));
        }
        let destination_archive = destination.path.join(&destination_name);
        if safe_file_name(
            &segment_manifest_path(&destination_archive).map_err(|error| error.to_string())?,
        )? != destination_manifest_name
            || safe_file_name(
                &archive_checksum_path(&destination_archive).map_err(|error| error.to_string())?,
            )? != destination_checksum_name
        {
            return Err(format!(
                "archive batch journal sidecar names do not match epoch {}",
                item.epoch
            ));
        }
        for name in [
            destination_name,
            destination_manifest_name,
            destination_checksum_name,
        ] {
            if !destination_names.insert(name) {
                return Err("archive batch journal destination namespaces overlap".into());
            }
        }
        if !source_archives.insert((source_path, archive_name.clone()))
            || !source_namespaces.insert((
                item.source_identity.dev,
                item.source_identity.ino,
                archive_name.as_bytes().to_vec(),
            ))
            || !archive_inodes.insert((item.archive_identity.dev, item.archive_identity.ino))
        {
            return Err("archive batch journal contains a duplicate staged archive".into());
        }
        validate_journal_regular_identity(
            item.archive_identity,
            destination.identity.gid,
            "staged archive",
        )?;
        if !item.evidence.matches_identity(
            item.archive_identity.dev,
            item.archive_identity.ino,
            item.archive_identity.len,
            item.archive_identity.mtime,
            item.archive_identity.mtime_nsec,
            item.archive_identity.ctime,
            item.archive_identity.ctime_nsec,
        ) {
            return Err(format!(
                "archive batch journal validation evidence does not bind epoch {} staged archive",
                item.epoch
            ));
        }
        if item.archive_identity.mode & 0o777 != FINAL_FILE_MODE {
            return Err(format!(
                "archive batch journal staged archive for epoch {} is not mode {FINAL_FILE_MODE:04o}",
                item.epoch
            ));
        }
        for (identity, label) in [
            (item.manifest_staged_identity, "staged manifest"),
            (Some(item.checksum_staged_identity), "staged checksum"),
            (Some(item.checksum_sentinel_identity), "checksum sentinel"),
            (item.initial.archive, "initial archive"),
            (item.initial.manifest, "initial manifest"),
            (item.initial.checksum, "initial checksum"),
        ] {
            if let Some(identity) = identity {
                validate_journal_regular_identity(identity, destination.identity.gid, label)?;
            }
        }
    }
    if archive_batch_journal_transaction_id(journal)? != journal.transaction_id {
        return Err("archive batch journal does not match its transaction ID commitment".into());
    }
    Ok(())
}

fn validate_journal_regular_identity(
    identity: FileIdentity,
    destination_gid: u32,
    label: &str,
) -> Result<(), String> {
    if identity.mode & libc::S_IFMT != libc::S_IFREG
        || identity.uid != effective_user_id()
        || identity.gid != destination_gid
        || identity.nlink != 1
        || identity.mode & 0o022 != 0
        || identity.mode & 0o6000 != 0
    {
        return Err(format!(
            "archive batch journal contains an unsafe {label} identity"
        ));
    }
    Ok(())
}

struct DirectoryStream(*mut libc::DIR);

impl Drop for DirectoryStream {
    fn drop(&mut self) {
        // SAFETY: the pointer was returned by fdopendir and is owned here.
        unsafe {
            libc::closedir(self.0);
        }
    }
}

fn bound_directory_entry_names(directory: &BoundDirectory) -> Result<HashSet<Vec<u8>>, String> {
    let cloned =
        openat_directory(directory.file.as_raw_fd(), OsStr::new(".")).map_err(|error| {
            format!(
                "failed to open an enumeration descriptor for {}: {error}",
                directory.path.display()
            )
        })?;
    let cloned_fd = cloned.into_raw_fd();
    // SAFETY: fdopendir takes ownership of the valid cloned directory fd on
    // success. On failure it leaves ownership with the caller.
    let stream = unsafe { libc::fdopendir(cloned_fd) };
    if stream.is_null() {
        // SAFETY: fdopendir failed and did not take ownership of cloned_fd.
        unsafe {
            libc::close(cloned_fd);
        }
        return Err(format!(
            "failed to enumerate directory {}: {}",
            directory.path.display(),
            io::Error::last_os_error()
        ));
    }
    let stream = DirectoryStream(stream);
    let mut names = HashSet::new();
    loop {
        // SAFETY: the stream is live, exclusively owned here, and readdir's
        // returned entry remains valid until the next call.
        unsafe {
            *libc::__errno_location() = 0;
        }
        // SAFETY: stream.0 is a valid DIR pointer owned by stream.
        let entry = unsafe { libc::readdir(stream.0) };
        if entry.is_null() {
            let error = io::Error::last_os_error();
            if error.raw_os_error() == Some(0) {
                break;
            }
            return Err(format!(
                "failed while enumerating directory {}: {error}",
                directory.path.display()
            ));
        }
        // SAFETY: d_name is NUL-terminated for a successful readdir result.
        let name = unsafe { CStr::from_ptr((*entry).d_name.as_ptr()) }.to_bytes();
        if name != b"." && name != b".." {
            names.insert(name.to_vec());
        }
    }
    Ok(names)
}

fn validate_archive_batch_marker_entries(
    marker: &BoundDirectory,
    armed_required: Option<bool>,
) -> Result<(), String> {
    let names = bound_directory_entry_names(marker)?;
    let known = [
        ARCHIVE_BATCH_ARMED.as_bytes(),
        ARCHIVE_BATCH_JOURNAL.as_bytes(),
        ARCHIVE_BATCH_JOURNAL_NEXT.as_bytes(),
    ];
    if let Some(unknown) = names.iter().find(|name| !known.contains(&name.as_slice())) {
        return Err(format!(
            "archive batch marker contains unexpected entry {:?}; refusing namespace mutation",
            OsStr::from_bytes(unknown)
        ));
    }
    match armed_required {
        Some(true)
            if !names.contains(ARCHIVE_BATCH_ARMED.as_bytes())
                || !names.contains(ARCHIVE_BATCH_JOURNAL.as_bytes()) =>
        {
            return Err("armed archive batch marker lacks its required files".to_string());
        }
        Some(false)
            if names.contains(ARCHIVE_BATCH_ARMED.as_bytes())
                || names.contains(ARCHIVE_BATCH_JOURNAL_NEXT.as_bytes()) =>
        {
            return Err("unarmed archive batch marker contains an armed-only file".to_string());
        }
        _ => {}
    }
    let mut identities = Vec::with_capacity(names.len());
    for name in &names {
        let bound =
            bind_required_regular(marker, OsStr::from_bytes(name), ComponentKind::Checksum)?;
        identities.push((name, bound.identity));
    }
    // Close the check/use gap for accidental or concurrent namespace changes.
    let rechecked = bound_directory_entry_names(marker)?;
    if rechecked != names {
        return Err(
            "archive batch marker entries changed while they were being validated".to_string(),
        );
    }
    for (name, identity) in identities {
        require_corresponding_target(
            marker,
            OsStr::from_bytes(name),
            Some(identity),
            "archive batch marker entry",
        )?;
    }
    Ok(())
}

fn retire_archive_batch_marker(
    destination: &BoundDirectory,
    marker: &BoundDirectory,
) -> Result<(), String> {
    validate_archive_batch_marker_entries(marker, Some(true))?;
    require_recovery_binding(
        destination,
        OsStr::new(ARCHIVE_BATCH_TRANSACTION_DIRECTORY),
        marker,
    )?;
    if target_identity(destination, OsStr::new(ARCHIVE_BATCH_OUTCOME_DIRECTORY))?.is_some() {
        return Err(format!(
            "archive batch outcome already exists at {}",
            destination
                .path
                .join(ARCHIVE_BATCH_OUTCOME_DIRECTORY)
                .display()
        ));
    }
    rename_noreplace(
        destination,
        OsStr::new(ARCHIVE_BATCH_TRANSACTION_DIRECTORY),
        destination,
        OsStr::new(ARCHIVE_BATCH_OUTCOME_DIRECTORY),
    )
    .map_err(|error| format!("failed to retire archive batch marker: {error}"))?;
    destination.sync()
}

fn remove_archive_batch_marker_directory(
    destination: &BoundDirectory,
    marker: &BoundDirectory,
    marker_name: &OsStr,
    armed_required: Option<bool>,
) -> Result<(), String> {
    validate_archive_batch_marker_entries(marker, armed_required)?;
    require_recovery_binding(destination, marker_name, marker)?;
    let cleanup_name = (0..100u32)
        .map(|sequence| {
            std::ffi::OsString::from(format!(
                ".jetstreamer-archive-batch-finished-{}-{}-{sequence}",
                std::process::id(),
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_nanos()
            ))
        })
        .find(|name| target_identity(destination, name).is_ok_and(|entry| entry.is_none()))
        .ok_or_else(|| "could not allocate archive batch cleanup name".to_string())?;
    rename_noreplace(destination, marker_name, destination, &cleanup_name)
        .map_err(|error| format!("failed to retire archive batch marker: {error}"))?;
    destination.sync()?;

    let mut failures = Vec::new();
    for name in [
        ARCHIVE_BATCH_ARMED,
        ARCHIVE_BATCH_JOURNAL_NEXT,
        ARCHIVE_BATCH_JOURNAL,
    ] {
        match bind_optional_regular(marker, OsStr::new(name), ComponentKind::Checksum) {
            Ok(Some(bound)) => {
                if let Err(error) =
                    unlink_corresponding_entry(marker, OsStr::new(name), bound.identity)
                {
                    failures.push(error);
                }
            }
            Ok(None) => {}
            Err(error) => failures.push(error),
        }
    }
    collect_sync_failures(&[marker], &mut failures);
    if !failures.is_empty() {
        return Err(failures.join("; "));
    }
    remove_empty_recovery_directory(destination, &cleanup_name, marker)
}

fn cleanup_committed_batch_sentinels(
    transactions: &[PublicationTransaction],
) -> Result<(), String> {
    let mut failures = Vec::new();
    for transaction in transactions {
        match target_identity(
            &transaction.recovery,
            ComponentKind::Checksum.staging_name(),
        ) {
            Ok(Some(identity))
                if identity.same_across_rename(transaction.checksum_sentinel.identity) =>
            {
                if let Err(error) = remove_corresponding_file(
                    &transaction.recovery,
                    ComponentKind::Checksum.staging_name(),
                    identity,
                ) {
                    failures.push(error);
                }
            }
            Ok(Some(_)) => failures.push(format!(
                "refusing to remove changed batch checksum sentinel in {}",
                transaction.recovery.path.display()
            )),
            Ok(None) => {}
            Err(error) => failures.push(error),
        }
        collect_sync_failures(&[&transaction.recovery], &mut failures);
    }
    if failures.is_empty() {
        Ok(())
    } else {
        Err(failures.join("; "))
    }
}

fn resolve_failed_archive_batch(
    destination_path: &Path,
    cause: String,
) -> Result<ArchiveBatchPublication, ArchivePublicationError> {
    match recover_archive_publication_batch(destination_path) {
        Ok(ArchiveBatchRecovery::RolledBack(_)) => Err(ArchivePublicationError {
            message: format!(
                "archive batch publication failed before its durable commit decision and was rolled back: {cause}"
            ),
            committed: false,
            recovery_directory: None,
        }),
        Ok(ArchiveBatchRecovery::Committed(publication)) => Ok(publication),
        Ok(ArchiveBatchRecovery::None) => Err(batch_indeterminate_error(
            destination_path,
            format!("{cause}; transaction marker disappeared before recovery"),
            false,
        )),
        Err(recovery) => Err(batch_indeterminate_error(
            destination_path,
            format!("{cause}; automatic recovery failed: {recovery}"),
            recovery.committed(),
        )),
    }
}

struct RecoveredArchiveBatchItem<'a> {
    record: &'a ArchiveBatchJournalItem,
    source: BoundDirectory,
    recovery: BoundDirectory,
    recovery_name: std::ffi::OsString,
    archive_name: std::ffi::OsString,
    destination_name: std::ffi::OsString,
    destination_manifest_name: std::ffi::OsString,
    destination_checksum_name: std::ffi::OsString,
}

fn bind_recovered_archive_batch_items<'a>(
    destination: &BoundDirectory,
    journal: &'a ArchiveBatchJournal,
) -> Result<Vec<RecoveredArchiveBatchItem<'a>>, String> {
    let mut recovered = Vec::with_capacity(journal.items.len());
    for record in &journal.items {
        let source_path = path_from_bytes(&record.source_path, "source")?;
        let source =
            BoundDirectory::bind_absolute_nofollow(&source_path, DirectoryPolicy::PrivateSource)?;
        if !source
            .identity
            .same_directory_binding(record.source_identity)
        {
            return Err(format!(
                "archive batch source directory changed for epoch {}: {}",
                record.epoch,
                source_path.display()
            ));
        }
        require_compatible_directories(&source, destination)?;
        let recovery_name = name_from_bytes(&record.recovery_name, "recovery")?;
        let recovery = BoundDirectory::bind_child(
            destination,
            &recovery_name,
            destination.path.join(&recovery_name),
            DirectoryPolicy::PrivateRecovery,
        )?;
        if !recovery
            .identity
            .same_directory_binding(record.recovery_identity)
        {
            return Err(format!(
                "archive batch recovery directory changed for epoch {}",
                record.epoch
            ));
        }
        recovered.push(RecoveredArchiveBatchItem {
            record,
            source,
            recovery,
            recovery_name,
            archive_name: name_from_bytes(&record.archive_name, "archive")?,
            destination_name: name_from_bytes(&record.destination_name, "destination archive")?,
            destination_manifest_name: name_from_bytes(
                &record.destination_manifest_name,
                "destination manifest",
            )?,
            destination_checksum_name: name_from_bytes(
                &record.destination_checksum_name,
                "destination checksum",
            )?,
        });
    }
    Ok(recovered)
}

fn observe_finalized_archive_batch(
    destination: &BoundDirectory,
    journal: &ArchiveBatchJournal,
) -> Result<ArchiveBatchRecovery, String> {
    let recovered = bind_recovered_archive_batch_items(destination, journal)?;
    match journal.decision {
        ArchiveBatchDecision::RollBack => {
            for item in &recovered {
                validate_batch_rollback_item(destination, item)?;
                verify_rolled_back_batch_item(destination, item)?;
            }
            let identity_evidence = recovered
                .iter()
                .map(|item| recovered_batch_rollback_evidence(destination, item))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(ArchiveBatchRecovery::RolledBack(ArchiveBatchRollback {
                transaction_id: journal.transaction_id,
                manifest_fingerprint: journal.manifest_fingerprint,
                destination_identity: ArchiveBatchDestinationIdentity {
                    device: journal.destination_identity.dev,
                    inode: journal.destination_identity.ino,
                },
                identity_evidence,
            }))
        }
        ArchiveBatchDecision::Commit => {
            for item in &recovered {
                validate_batch_commit_item(destination, item)?;
            }
            let publications = recovered
                .iter()
                .map(|item| recovered_batch_publication(destination, item))
                .collect::<Result<Vec<_>, _>>()?;
            let identity_evidence = recovered
                .iter()
                .zip(&publications)
                .map(|(item, publication)| {
                    recovered_batch_identity_evidence(destination, item, publication.evidence)
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(ArchiveBatchRecovery::Committed(ArchiveBatchPublication {
                transaction_id: journal.transaction_id,
                manifest_fingerprint: journal.manifest_fingerprint,
                destination_identity: ArchiveBatchDestinationIdentity {
                    device: journal.destination_identity.dev,
                    inode: journal.destination_identity.ino,
                },
                publications,
                identity_evidence,
            }))
        }
    }
}

fn recover_bound_archive_batch(
    destination: &BoundDirectory,
    marker: &BoundDirectory,
    journal: &ArchiveBatchJournal,
) -> Result<ArchiveBatchRecovery, ArchivePublicationError> {
    let committed = journal.decision == ArchiveBatchDecision::Commit;
    let operation = (|| -> Result<ArchiveBatchRecovery, String> {
        let recovered = bind_recovered_archive_batch_items(destination, journal)?;
        match journal.decision {
            ArchiveBatchDecision::RollBack => {
                for item in &recovered {
                    validate_batch_rollback_item(destination, item)?;
                }
                for item in recovered.iter().rev() {
                    rollback_batch_data_item(destination, item)?;
                }
                for item in recovered.iter().rev() {
                    rollback_batch_checksum_item(destination, item)?;
                }
                for item in &recovered {
                    verify_rolled_back_batch_item(destination, item)?;
                }
                let outcome = observe_finalized_archive_batch(destination, journal)?;
                retire_archive_batch_marker(destination, marker)?;
                Ok(outcome)
            }
            ArchiveBatchDecision::Commit => {
                for item in &recovered {
                    validate_batch_commit_item(destination, item)?;
                }
                for item in &recovered {
                    commit_batch_checksum_item(destination, item)?;
                }
                for item in &recovered {
                    cleanup_recovered_batch_sentinel(item)?;
                }
                let outcome = observe_finalized_archive_batch(destination, journal)?;
                retire_archive_batch_marker(destination, marker)?;
                Ok(outcome)
            }
        }
    })();
    operation.map_err(|message| batch_indeterminate_error(&destination.path, message, committed))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BatchComponentState {
    Initial,
    Exchanged,
    Installed,
}

fn corresponding(observed: Option<FileIdentity>, expected: FileIdentity) -> bool {
    observed.is_some_and(|observed| observed.same_across_rename(expected))
}

fn option_identities_correspond(
    observed: Option<FileIdentity>,
    expected: Option<FileIdentity>,
) -> bool {
    match (observed, expected) {
        (None, None) => true,
        (Some(observed), Some(expected)) => observed.same_across_rename(expected),
        _ => false,
    }
}

#[allow(clippy::too_many_arguments)]
fn batch_component_state(
    source: &BoundDirectory,
    source_name: &OsStr,
    new_identity: Option<FileIdentity>,
    destination: &BoundDirectory,
    destination_name: &OsStr,
    old_identity: Option<FileIdentity>,
    recovery: &BoundDirectory,
    backup_name: &OsStr,
    label: &str,
) -> Result<BatchComponentState, String> {
    let source_observed = target_identity(source, source_name)?;
    let destination_observed = target_identity(destination, destination_name)?;
    let backup_observed = target_identity(recovery, backup_name)?;
    let state = match (new_identity, old_identity) {
        (Some(new), Some(old))
            if corresponding(source_observed, new)
                && corresponding(destination_observed, old)
                && backup_observed.is_none() =>
        {
            BatchComponentState::Initial
        }
        (Some(new), Some(old))
            if corresponding(source_observed, old)
                && corresponding(destination_observed, new)
                && backup_observed.is_none() =>
        {
            BatchComponentState::Exchanged
        }
        (Some(new), Some(old))
            if source_observed.is_none()
                && corresponding(destination_observed, new)
                && corresponding(backup_observed, old) =>
        {
            BatchComponentState::Installed
        }
        (Some(new), None)
            if corresponding(source_observed, new)
                && destination_observed.is_none()
                && backup_observed.is_none() =>
        {
            BatchComponentState::Initial
        }
        (Some(new), None)
            if source_observed.is_none()
                && corresponding(destination_observed, new)
                && backup_observed.is_none() =>
        {
            BatchComponentState::Installed
        }
        (None, Some(old))
            if source_observed.is_none()
                && corresponding(destination_observed, old)
                && backup_observed.is_none() =>
        {
            BatchComponentState::Initial
        }
        (None, Some(old))
            if source_observed.is_none()
                && destination_observed.is_none()
                && corresponding(backup_observed, old) =>
        {
            BatchComponentState::Installed
        }
        (None, None)
            if source_observed.is_none()
                && destination_observed.is_none()
                && backup_observed.is_none() =>
        {
            BatchComponentState::Initial
        }
        _ => {
            return Err(format!(
                "archive batch {label} namespace is not a recoverable transaction state"
            ));
        }
    };
    Ok(state)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BatchChecksumState {
    Initial,
    Reserved,
    Committed,
    CommittedAndCleaned,
}

fn batch_checksum_state(
    destination: &BoundDirectory,
    item: &RecoveredArchiveBatchItem<'_>,
) -> Result<BatchChecksumState, String> {
    let destination_observed = target_identity(destination, &item.destination_checksum_name)?;
    let backup_observed = target_identity(&item.recovery, ComponentKind::Checksum.backup_name())?;
    let staging_observed = target_identity(&item.recovery, ComponentKind::Checksum.staging_name())?;
    let old = item.record.initial.checksum;
    let canonical = item.record.checksum_staged_identity;
    let sentinel = item.record.checksum_sentinel_identity;
    match old {
        Some(old)
            if corresponding(destination_observed, old)
                && corresponding(backup_observed, sentinel)
                && corresponding(staging_observed, canonical) =>
        {
            Ok(BatchChecksumState::Initial)
        }
        None if destination_observed.is_none()
            && corresponding(backup_observed, sentinel)
            && corresponding(staging_observed, canonical) =>
        {
            Ok(BatchChecksumState::Initial)
        }
        Some(old)
            if corresponding(destination_observed, sentinel)
                && corresponding(backup_observed, old)
                && corresponding(staging_observed, canonical) =>
        {
            Ok(BatchChecksumState::Reserved)
        }
        None if corresponding(destination_observed, sentinel)
            && backup_observed.is_none()
            && corresponding(staging_observed, canonical) =>
        {
            Ok(BatchChecksumState::Reserved)
        }
        Some(old)
            if corresponding(destination_observed, canonical)
                && corresponding(backup_observed, old)
                && corresponding(staging_observed, sentinel) =>
        {
            Ok(BatchChecksumState::Committed)
        }
        None if corresponding(destination_observed, canonical)
            && backup_observed.is_none()
            && corresponding(staging_observed, sentinel) =>
        {
            Ok(BatchChecksumState::Committed)
        }
        Some(old)
            if corresponding(destination_observed, canonical)
                && corresponding(backup_observed, old)
                && staging_observed.is_none() =>
        {
            Ok(BatchChecksumState::CommittedAndCleaned)
        }
        None if corresponding(destination_observed, canonical)
            && backup_observed.is_none()
            && staging_observed.is_none() =>
        {
            Ok(BatchChecksumState::CommittedAndCleaned)
        }
        _ => Err(format!(
            "archive batch checksum namespace for epoch {} is not recoverable",
            item.record.epoch
        )),
    }
}

fn validate_batch_rollback_item(
    destination: &BoundDirectory,
    item: &RecoveredArchiveBatchItem<'_>,
) -> Result<(), String> {
    batch_component_state(
        &item.source,
        &item.archive_name,
        Some(item.record.archive_identity),
        destination,
        &item.destination_name,
        item.record.initial.archive,
        &item.recovery,
        ComponentKind::Archive.backup_name(),
        "archive",
    )?;
    batch_component_state(
        &item.recovery,
        ComponentKind::Manifest.staging_name(),
        item.record.manifest_staged_identity,
        destination,
        &item.destination_manifest_name,
        item.record.initial.manifest,
        &item.recovery,
        ComponentKind::Manifest.backup_name(),
        "manifest",
    )?;
    match batch_checksum_state(destination, item)? {
        BatchChecksumState::Initial | BatchChecksumState::Reserved => Ok(()),
        BatchChecksumState::Committed | BatchChecksumState::CommittedAndCleaned => Err(format!(
            "archive batch rollback journal found a committed checksum for epoch {}",
            item.record.epoch
        )),
    }
}

fn validate_batch_commit_item(
    destination: &BoundDirectory,
    item: &RecoveredArchiveBatchItem<'_>,
) -> Result<(), String> {
    if batch_component_state(
        &item.source,
        &item.archive_name,
        Some(item.record.archive_identity),
        destination,
        &item.destination_name,
        item.record.initial.archive,
        &item.recovery,
        ComponentKind::Archive.backup_name(),
        "archive",
    )? != BatchComponentState::Installed
    {
        return Err(format!(
            "archive batch commit journal found epoch {} archive uninstalled",
            item.record.epoch
        ));
    }
    let manifest_state = batch_component_state(
        &item.recovery,
        ComponentKind::Manifest.staging_name(),
        item.record.manifest_staged_identity,
        destination,
        &item.destination_manifest_name,
        item.record.initial.manifest,
        &item.recovery,
        ComponentKind::Manifest.backup_name(),
        "manifest",
    )?;
    let expected_manifest_state = if item.record.manifest_staged_identity.is_some()
        || item.record.initial.manifest.is_some()
    {
        BatchComponentState::Installed
    } else {
        BatchComponentState::Initial
    };
    if manifest_state != expected_manifest_state {
        return Err(format!(
            "archive batch commit journal found epoch {} manifest uninstalled",
            item.record.epoch
        ));
    }
    match batch_checksum_state(destination, item)? {
        BatchChecksumState::Reserved
        | BatchChecksumState::Committed
        | BatchChecksumState::CommittedAndCleaned => Ok(()),
        BatchChecksumState::Initial => Err(format!(
            "archive batch commit journal found epoch {} checksum unreserved",
            item.record.epoch
        )),
    }
}

#[allow(clippy::too_many_arguments)]
fn rollback_batch_component(
    source: &BoundDirectory,
    source_name: &OsStr,
    new_identity: Option<FileIdentity>,
    destination: &BoundDirectory,
    destination_name: &OsStr,
    old_identity: Option<FileIdentity>,
    recovery: &BoundDirectory,
    backup_name: &OsStr,
    label: &str,
) -> Result<(), String> {
    for _ in 0..3 {
        match batch_component_state(
            source,
            source_name,
            new_identity,
            destination,
            destination_name,
            old_identity,
            recovery,
            backup_name,
            label,
        )? {
            BatchComponentState::Initial => {
                return sync_all(&[source, recovery, destination]);
            }
            BatchComponentState::Exchanged => {
                sync_all(&[source, recovery, destination])?;
                rename_exchange(source, source_name, destination, destination_name).map_err(
                    |error| format!("failed to roll back exchanged batch {label}: {error}"),
                )?;
                sync_all(&[source, destination])?;
            }
            BatchComponentState::Installed => {
                sync_all(&[source, recovery, destination])?;
                match (new_identity, old_identity) {
                    (Some(_), Some(_)) => {
                        rename_noreplace(recovery, backup_name, source, source_name).map_err(
                            |error| format!("failed to restore staged old batch {label}: {error}"),
                        )?;
                        sync_all(&[recovery, source])?;
                    }
                    (Some(_), None) => {
                        rename_noreplace(destination, destination_name, source, source_name)
                            .map_err(|error| {
                                format!("failed to return new batch {label} to staging: {error}")
                            })?;
                        sync_all(&[destination, source])?;
                    }
                    (None, Some(_)) => {
                        rename_noreplace(recovery, backup_name, destination, destination_name)
                            .map_err(|error| {
                                format!("failed to restore removed batch {label}: {error}")
                            })?;
                        sync_all(&[recovery, destination])?;
                    }
                    (None, None) => {
                        return Err(format!(
                            "empty batch {label} unexpectedly reached installed state"
                        ));
                    }
                }
            }
        }
    }
    Err(format!("batch {label} rollback did not converge"))
}

fn rollback_batch_data_item(
    destination: &BoundDirectory,
    item: &RecoveredArchiveBatchItem<'_>,
) -> Result<(), String> {
    rollback_batch_component(
        &item.source,
        &item.archive_name,
        Some(item.record.archive_identity),
        destination,
        &item.destination_name,
        item.record.initial.archive,
        &item.recovery,
        ComponentKind::Archive.backup_name(),
        "archive",
    )?;
    rollback_batch_component(
        &item.recovery,
        ComponentKind::Manifest.staging_name(),
        item.record.manifest_staged_identity,
        destination,
        &item.destination_manifest_name,
        item.record.initial.manifest,
        &item.recovery,
        ComponentKind::Manifest.backup_name(),
        "manifest",
    )
}

fn rollback_batch_checksum_item(
    destination: &BoundDirectory,
    item: &RecoveredArchiveBatchItem<'_>,
) -> Result<(), String> {
    match batch_checksum_state(destination, item)? {
        BatchChecksumState::Initial => sync_all(&[&item.recovery, destination]),
        BatchChecksumState::Reserved => {
            sync_all(&[&item.recovery, destination])?;
            if item.record.initial.checksum.is_some() {
                rename_exchange(
                    &item.recovery,
                    ComponentKind::Checksum.backup_name(),
                    destination,
                    &item.destination_checksum_name,
                )
                .map_err(|error| format!("failed to restore batch checksum: {error}"))?;
            } else {
                rename_noreplace(
                    destination,
                    &item.destination_checksum_name,
                    &item.recovery,
                    ComponentKind::Checksum.backup_name(),
                )
                .map_err(|error| format!("failed to remove batch checksum sentinel: {error}"))?;
            }
            sync_all(&[&item.recovery, destination])?;
            if batch_checksum_state(destination, item)? == BatchChecksumState::Initial {
                Ok(())
            } else {
                Err("batch checksum rollback did not restore its initial state".into())
            }
        }
        BatchChecksumState::Committed | BatchChecksumState::CommittedAndCleaned => {
            Err("refusing to roll back batch data while a canonical checksum is installed".into())
        }
    }
}

fn verify_rolled_back_batch_item(
    destination: &BoundDirectory,
    item: &RecoveredArchiveBatchItem<'_>,
) -> Result<(), String> {
    if batch_component_state(
        &item.source,
        &item.archive_name,
        Some(item.record.archive_identity),
        destination,
        &item.destination_name,
        item.record.initial.archive,
        &item.recovery,
        ComponentKind::Archive.backup_name(),
        "archive",
    )? != BatchComponentState::Initial
        || batch_component_state(
            &item.recovery,
            ComponentKind::Manifest.staging_name(),
            item.record.manifest_staged_identity,
            destination,
            &item.destination_manifest_name,
            item.record.initial.manifest,
            &item.recovery,
            ComponentKind::Manifest.backup_name(),
            "manifest",
        )? != BatchComponentState::Initial
        || batch_checksum_state(destination, item)? != BatchChecksumState::Initial
    {
        return Err(format!(
            "archive batch rollback did not restore the exact epoch {} namespace",
            item.record.epoch
        ));
    }
    let archive = bind_required_regular(&item.source, &item.archive_name, ComponentKind::Archive)?;
    rebind_validated_after_rename(&archive.file, item.record.evidence.into()).map_err(|error| {
        format!(
            "rolled-back staged archive evidence changed for epoch {}: {error}",
            item.record.epoch
        )
    })?;
    sync_all(&[&item.source, &item.recovery, destination])
}

fn remove_optional_corresponding_file(
    directory: &BoundDirectory,
    name: &OsStr,
    identity: FileIdentity,
    label: &str,
) -> Result<(), String> {
    match target_identity(directory, name)? {
        Some(observed) if observed.same_across_rename(identity) => {
            remove_corresponding_file(directory, name, observed)
        }
        Some(_) => Err(format!(
            "refusing to remove changed {label}: {}",
            directory.path.join(name).display()
        )),
        None => Ok(()),
    }
}

fn cleanup_rolled_back_batch_item(item: &RecoveredArchiveBatchItem<'_>) -> Result<(), String> {
    if let Some(identity) = item.record.manifest_staged_identity {
        remove_optional_corresponding_file(
            &item.recovery,
            ComponentKind::Manifest.staging_name(),
            identity,
            "rolled-back staged manifest",
        )?;
    }
    remove_optional_corresponding_file(
        &item.recovery,
        ComponentKind::Checksum.staging_name(),
        item.record.checksum_staged_identity,
        "rolled-back staged checksum",
    )?;
    remove_optional_corresponding_file(
        &item.recovery,
        ComponentKind::Checksum.backup_name(),
        item.record.checksum_sentinel_identity,
        "rolled-back checksum sentinel",
    )?;
    item.recovery.sync()
}

fn commit_batch_checksum_item(
    destination: &BoundDirectory,
    item: &RecoveredArchiveBatchItem<'_>,
) -> Result<(), String> {
    match batch_checksum_state(destination, item)? {
        BatchChecksumState::Reserved => {
            sync_all(&[&item.recovery, destination])?;
            rename_exchange(
                &item.recovery,
                ComponentKind::Checksum.staging_name(),
                destination,
                &item.destination_checksum_name,
            )
            .map_err(|error| format!("failed to commit batch checksum: {error}"))?;
            sync_all(&[&item.recovery, destination])?;
        }
        BatchChecksumState::Committed | BatchChecksumState::CommittedAndCleaned => {
            sync_all(&[&item.recovery, destination])?;
        }
        BatchChecksumState::Initial => {
            return Err(format!(
                "cannot commit unreserved batch checksum for epoch {}",
                item.record.epoch
            ));
        }
    }
    match batch_checksum_state(destination, item)? {
        BatchChecksumState::Committed | BatchChecksumState::CommittedAndCleaned => Ok(()),
        BatchChecksumState::Initial | BatchChecksumState::Reserved => Err(format!(
            "batch checksum commit did not converge for epoch {}",
            item.record.epoch
        )),
    }
}

fn cleanup_recovered_batch_sentinel(item: &RecoveredArchiveBatchItem<'_>) -> Result<(), String> {
    remove_optional_corresponding_file(
        &item.recovery,
        ComponentKind::Checksum.staging_name(),
        item.record.checksum_sentinel_identity,
        "committed batch checksum sentinel",
    )?;
    item.recovery.sync()
}

fn cleanup_acknowledged_archive_batch(
    destination: &BoundDirectory,
    decision: ArchiveBatchDecision,
    recovered: &[RecoveredArchiveBatchItem<'_>],
) {
    for item in recovered {
        let removable = match decision {
            ArchiveBatchDecision::RollBack => cleanup_rolled_back_batch_item(item).is_ok(),
            ArchiveBatchDecision::Commit => {
                let retains_backups = item.record.initial.archive.is_some()
                    || item.record.initial.manifest.is_some()
                    || item.record.initial.checksum.is_some();
                !retains_backups && cleanup_recovered_batch_sentinel(item).is_ok()
            }
        };
        if removable {
            let _ =
                remove_empty_recovery_directory(destination, &item.recovery_name, &item.recovery);
        }
    }
}

fn recovered_batch_publication(
    destination: &BoundDirectory,
    item: &RecoveredArchiveBatchItem<'_>,
) -> Result<ArchivePublication, String> {
    validate_batch_commit_item(destination, item)?;
    let archive =
        bind_required_regular(destination, &item.destination_name, ComponentKind::Archive)?;
    let evidence = rebind_validated_after_rename(&archive.file, item.record.evidence.into())
        .map_err(|error| {
            format!(
                "committed batch archive evidence changed for epoch {}: {error}",
                item.record.epoch
            )
        })?;
    let expected_checksum = archive_checksum_line(&evidence.sha256, &item.destination_name)
        .map_err(|error| error.to_string())?;
    let checksum = bind_required_regular(
        destination,
        &item.destination_checksum_name,
        ComponentKind::Checksum,
    )?;
    if read_bound_file(&checksum.file, expected_checksum.len() as u64)?
        != expected_checksum.as_bytes()
    {
        return Err(format!(
            "committed batch checksum contents are not canonical for epoch {}",
            item.record.epoch
        ));
    }
    Ok(ArchivePublication {
        archive_path: destination.path.join(&item.destination_name),
        manifest_path: item
            .record
            .manifest_staged_identity
            .map(|_| destination.path.join(&item.destination_manifest_name)),
        checksum_path: destination.path.join(&item.destination_checksum_name),
        recovery_directory: (item.record.initial.archive.is_some()
            || item.record.initial.manifest.is_some()
            || item.record.initial.checksum.is_some())
        .then(|| item.recovery.path.clone()),
        evidence,
    })
}

fn recovered_batch_identity_evidence(
    destination: &BoundDirectory,
    item: &RecoveredArchiveBatchItem<'_>,
    archive_validation: ValidatedArchiveFile,
) -> Result<ArchiveBatchIdentityEvidence, String> {
    let committed_archive = target_identity(destination, &item.destination_name)?
        .ok_or_else(|| "recovered batch archive disappeared".to_string())?;
    let committed_manifest = target_identity(destination, &item.destination_manifest_name)?;
    let committed_checksum = target_identity(destination, &item.destination_checksum_name)?
        .ok_or_else(|| "recovered batch checksum disappeared".to_string())?;
    if !committed_archive.same_across_rename(item.record.archive_identity)
        || !option_identities_correspond(committed_manifest, item.record.manifest_staged_identity)
        || !committed_checksum.same_across_rename(item.record.checksum_staged_identity)
    {
        return Err(format!(
            "recovered batch identities changed after verification for epoch {}",
            item.record.epoch
        ));
    }
    Ok(ArchiveBatchIdentityEvidence {
        epoch: item.record.epoch,
        initial_archive: item.record.initial.archive.map(Into::into),
        initial_manifest: item.record.initial.manifest.map(Into::into),
        initial_checksum: item.record.initial.checksum.map(Into::into),
        committed_archive: committed_archive.into(),
        committed_manifest: committed_manifest.map(Into::into),
        committed_checksum: committed_checksum.into(),
        archive_validation,
    })
}

fn recovered_batch_rollback_evidence(
    destination: &BoundDirectory,
    item: &RecoveredArchiveBatchItem<'_>,
) -> Result<ArchiveBatchRollbackIdentityEvidence, String> {
    let staged = bind_required_regular(&item.source, &item.archive_name, ComponentKind::Archive)?;
    let staged_archive_validation =
        rebind_validated_after_rename(&staged.file, item.record.evidence.into()).map_err(
            |error| {
                format!(
                    "rolled-back batch archive evidence changed for epoch {}: {error}",
                    item.record.epoch
                )
            },
        )?;
    let staged_archive = FileIdentity::from_metadata(
        &staged
            .file
            .metadata()
            .map_err(|error| format!("failed to identify rolled-back archive: {error}"))?,
    );
    let restored_archive = target_identity(destination, &item.destination_name)?;
    let restored_manifest = target_identity(destination, &item.destination_manifest_name)?;
    let restored_checksum = target_identity(destination, &item.destination_checksum_name)?;
    if !staged_archive.same_across_rename(item.record.archive_identity)
        || !option_identities_correspond(restored_archive, item.record.initial.archive)
        || !option_identities_correspond(restored_manifest, item.record.initial.manifest)
        || !option_identities_correspond(restored_checksum, item.record.initial.checksum)
    {
        return Err(format!(
            "rolled-back batch identities changed after verification for epoch {}",
            item.record.epoch
        ));
    }
    Ok(ArchiveBatchRollbackIdentityEvidence {
        epoch: item.record.epoch,
        staged_archive_path: item.source.path.join(&item.archive_name),
        restored_archive: restored_archive.map(Into::into),
        restored_manifest: restored_manifest.map(Into::into),
        restored_checksum: restored_checksum.map(Into::into),
        staged_archive: staged_archive.into(),
        staged_archive_validation,
    })
}

fn preflight_error(message: String) -> ArchivePublicationError {
    ArchivePublicationError {
        message: format!("archive publication preflight failed: {message}"),
        committed: false,
        recovery_directory: None,
    }
}

fn preflight_error_with_recovery(
    message: String,
    recovery_directory: Option<PathBuf>,
) -> ArchivePublicationError {
    ArchivePublicationError {
        message: format!("archive publication preflight failed: {message}"),
        committed: false,
        recovery_directory,
    }
}

fn cleanup_preflight_error(
    message: String,
    destination: &BoundDirectory,
    recovery_name: &OsStr,
    recovery: &BoundDirectory,
    staged_files: &[(std::ffi::OsString, FileIdentity)],
) -> ArchivePublicationError {
    match cleanup_unused_recovery_directory(destination, recovery_name, recovery, staged_files) {
        Ok(()) => preflight_error(message),
        Err(cleanup) => preflight_error_with_recovery(
            format!("{message}; failed to remove unused recovery directory: {cleanup}"),
            Some(recovery.path.clone()),
        ),
    }
}

fn effective_user_id() -> u32 {
    // SAFETY: geteuid has no preconditions.
    unsafe { libc::geteuid() }
}

fn require_compatible_directories(
    source: &BoundDirectory,
    destination: &BoundDirectory,
) -> Result<(), String> {
    if source.identity.dev != destination.identity.dev {
        return Err(format!(
            "staging {} and destination {} are on different filesystems",
            source.path.display(),
            destination.path.display()
        ));
    }
    if source.identity.ino == destination.identity.ino {
        return Err("staging and destination directories must differ".to_string());
    }
    Ok(())
}

fn safe_file_name(path: &Path) -> Result<std::ffi::OsString, String> {
    let name = path
        .file_name()
        .ok_or_else(|| format!("path has no filename: {}", path.display()))?;
    let bytes = name.as_bytes();
    if bytes.is_empty() || bytes == b"." || bytes == b".." || bytes.contains(&b'/') {
        return Err(format!("unsafe filename in path: {}", path.display()));
    }
    CString::new(bytes).map_err(|_| format!("filename contains NUL: {}", path.display()))?;
    Ok(name.to_os_string())
}

fn bind_required_regular(
    directory: &BoundDirectory,
    name: &OsStr,
    kind: ComponentKind,
) -> Result<BoundFile, String> {
    bind_optional_regular(directory, name, kind)?
        .ok_or_else(|| format!("missing staged {}: {:?}", kind.label(), name))
}

fn bind_optional_regular(
    directory: &BoundDirectory,
    name: &OsStr,
    kind: ComponentKind,
) -> Result<Option<BoundFile>, String> {
    let full_path = directory.path.join(name);
    let entry_identity = match fstatat_identity(directory.file.as_raw_fd(), name) {
        Ok(Some(identity)) => identity,
        Ok(None) => match openat_readonly(directory.file.as_raw_fd(), name) {
            Err(open_error) if open_error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Ok(_) => {
                return Err(format!(
                    "{} appeared while absence was checked: {}",
                    kind.label(),
                    full_path.display()
                ));
            }
            Err(open_error) => {
                return Err(format!(
                    "failed to confirm absent {} {}: {open_error}",
                    kind.label(),
                    full_path.display()
                ));
            }
        },
        Err(error) => {
            return Err(format!(
                "failed to inspect {} {}: {error}",
                kind.label(),
                full_path.display()
            ));
        }
    };
    if entry_identity.mode & libc::S_IFMT != libc::S_IFREG {
        return Err(format!(
            "{} path is not a regular file: {}",
            kind.label(),
            full_path.display()
        ));
    }
    let file = openat_readonly(directory.file.as_raw_fd(), name).map_err(|error| {
        format!(
            "failed to open {} without following links {}: {error}",
            kind.label(),
            full_path.display()
        )
    })?;
    let descriptor_metadata = file.metadata().map_err(|error| {
        format!(
            "failed to inspect opened {} {}: {error}",
            kind.label(),
            full_path.display()
        )
    })?;
    let identity = FileIdentity::from_metadata(&descriptor_metadata);
    if entry_identity != identity {
        return Err(format!(
            "{} changed while it was opened: {}",
            kind.label(),
            full_path.display()
        ));
    }
    let expected_gid = match directory.policy {
        DirectoryPolicy::PrivateSource => None,
        DirectoryPolicy::SharedDestination | DirectoryPolicy::PrivateRecovery => {
            Some(directory.identity.gid)
        }
    };
    require_safe_regular(identity, expected_gid, kind, &full_path)?;
    Ok(Some(BoundFile { file, identity }))
}

fn require_safe_regular(
    identity: FileIdentity,
    expected_gid: Option<u32>,
    kind: ComponentKind,
    path: &Path,
) -> Result<(), String> {
    if identity.uid != effective_user_id()
        || expected_gid.is_some_and(|gid| identity.gid != gid)
        || identity.nlink != 1
        || identity.mode & 0o022 != 0
        || identity.mode & 0o6000 != 0
    {
        return Err(format!(
            "{} must be caller-owned, destination-group-owned, singly linked, non-writable by group/other, and free of set-ID bits: {}",
            kind.label(),
            path.display()
        ));
    }
    Ok(())
}

fn require_staged_archive_permissions(
    archive: &BoundFile,
    destination_gid: u32,
) -> Result<(), String> {
    if archive.identity.gid != destination_gid || archive.identity.mode & 0o777 != FINAL_FILE_MODE {
        return Err(format!(
            "staged archive must already use destination group {} and mode {:04o}",
            destination_gid, FINAL_FILE_MODE
        ));
    }
    Ok(())
}

fn validate_bound_manifest(
    archive_path: &Path,
    archive: &BoundFile,
    evidence: ValidatedArchiveFile,
    source: &BoundDirectory,
    manifest_path: &Path,
    manifest_name: &OsStr,
    manifest: &BoundFile,
) -> Result<Vec<u8>, String> {
    if manifest.identity.len > MAX_SEGMENT_MANIFEST_BYTES {
        return Err(format!(
            "segment manifest exceeds {} bytes: {}",
            MAX_SEGMENT_MANIFEST_BYTES,
            manifest_path.display()
        ));
    }
    let bytes = read_bound_file(&manifest.file, MAX_SEGMENT_MANIFEST_BYTES)?;
    let parsed: HistoricalSegmentManifest = serde_json::from_slice(&bytes).map_err(|error| {
        format!(
            "failed to parse bound segment manifest {}: {error}",
            manifest_path.display()
        )
    })?;
    parsed.validate().map_err(|error| {
        format!(
            "invalid segment manifest {}: {error}",
            manifest_path.display()
        )
    })?;
    if parsed.archive_sha256 != evidence.sha256 {
        return Err(format!(
            "segment manifest digest does not match validated archive {}",
            archive_path.display()
        ));
    }
    let validated = read_and_validate_segment_manifest(archive_path).map_err(|error| {
        format!(
            "segment manifest did not pass durable archive validation {}: {error}",
            manifest_path.display()
        )
    })?;
    if validated != parsed {
        return Err(format!(
            "segment manifest changed across validation: {}",
            manifest_path.display()
        ));
    }
    if archive_file_identity(&archive.file)
        .map_err(|error| format!("failed to recheck bound archive: {error}"))?
        != evidence.identity
        || !path_matches_archive_identity(archive_path, evidence.identity)
            .map_err(|error| format!("failed to recheck archive path: {error}"))?
    {
        return Err("archive or segment manifest changed during sidecar validation".to_string());
    }
    recheck_bound_file(source, manifest_name, manifest.identity)?;
    Ok(bytes)
}

fn read_bound_file(file: &File, max_len: u64) -> Result<Vec<u8>, String> {
    let metadata = file
        .metadata()
        .map_err(|error| format!("failed to inspect bound file: {error}"))?;
    if metadata.len() > max_len {
        return Err(format!("bound file exceeds {max_len} bytes"));
    }
    let capacity = usize::try_from(metadata.len())
        .map_err(|_| "bound file length does not fit usize".to_string())?;
    let mut clone = file
        .try_clone()
        .map_err(|error| format!("failed to clone bound file: {error}"))?;
    let mut bytes = Vec::with_capacity(capacity);
    clone
        .read_to_end(&mut bytes)
        .map_err(|error| format!("failed to read bound file: {error}"))?;
    if bytes.len() != capacity {
        return Err("bound file length changed while it was read".to_string());
    }
    Ok(bytes)
}

struct RecoveryDirectoryError {
    message: String,
    recovery_directory: Option<PathBuf>,
}

fn create_recovery_directory(
    destination: &BoundDirectory,
) -> Result<(std::ffi::OsString, BoundDirectory), RecoveryDirectoryError> {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    for sequence in 0..100u32 {
        let name = std::ffi::OsString::from(format!(
            ".jetstreamer-recovery-{}-{timestamp}-{sequence}",
            std::process::id()
        ));
        let name_c = os_str_cstring(&name).map_err(|error| RecoveryDirectoryError {
            message: error.to_string(),
            recovery_directory: None,
        })?;
        // SAFETY: the directory descriptor and NUL-terminated basename are
        // valid. The destination directory is held open for the transaction.
        let result = unsafe {
            libc::mkdirat(
                destination.file.as_raw_fd(),
                name_c.as_ptr(),
                0o700 as libc::mode_t,
            )
        };
        if result != 0 {
            let error = io::Error::last_os_error();
            if error.kind() == io::ErrorKind::AlreadyExists {
                continue;
            }
            return Err(RecoveryDirectoryError {
                message: format!("failed to create recovery directory: {error}"),
                recovery_directory: None,
            });
        }
        let path = destination.path.join(&name);
        let recovery = BoundDirectory::bind_child(
            destination,
            &name,
            path.clone(),
            DirectoryPolicy::PrivateRecovery,
        )
        .map_err(|message| RecoveryDirectoryError {
            message,
            recovery_directory: Some(path.clone()),
        })?;
        if recovery.identity.dev != destination.identity.dev {
            return Err(RecoveryDirectoryError {
                message: "recovery directory is on a different filesystem".to_string(),
                recovery_directory: Some(path),
            });
        }
        if let Err(message) = destination.sync() {
            let cleanup = remove_empty_recovery_directory(destination, &name, &recovery);
            let recovery_directory = cleanup.as_ref().err().map(|_| recovery.path.clone());
            return Err(RecoveryDirectoryError {
                message: match &cleanup {
                    Ok(()) => message,
                    Err(cleanup) => format!(
                        "{message}; failed to remove recovery directory after sync failure: {cleanup}"
                    ),
                },
                recovery_directory,
            });
        }
        return Ok((name, recovery));
    }
    Err(RecoveryDirectoryError {
        message: "could not allocate a unique recovery directory".to_string(),
        recovery_directory: None,
    })
}

fn create_recovery_file(
    recovery: &BoundDirectory,
    kind: ComponentKind,
    name: &OsStr,
    contents: &[u8],
    destination_gid: u32,
) -> Result<StagedComponent, String> {
    let name = name.to_os_string();
    let bound = create_bound_file(
        recovery,
        kind,
        &name,
        contents,
        Some(destination_gid),
        FINAL_FILE_MODE,
    )?;
    Ok(StagedComponent {
        kind,
        directory_index: DirectoryIndex::Recovery,
        name,
        file: bound.file,
        identity: bound.identity,
    })
}

fn create_bound_file(
    directory: &BoundDirectory,
    kind: ComponentKind,
    name: &OsStr,
    contents: &[u8],
    expected_gid: Option<u32>,
    final_mode: u32,
) -> Result<BoundFile, String> {
    let path = directory.path.join(name);
    let mut file = openat_create(directory.file.as_raw_fd(), name, 0o400)
        .map_err(|error| format!("failed to create staged {}: {error}", kind.label()))?;
    let operation = (|| {
        file.write_all(contents)
            .and_then(|()| file.flush())
            .map_err(|error| format!("failed to write staged {}: {error}", kind.label()))?;
        if let Some(gid) = expected_gid {
            assign_destination_group(&file, gid).map_err(|error| {
                format!(
                    "failed to assign destination group to staged {}: {error}",
                    kind.label()
                )
            })?;
        }
        file.set_permissions(fs::Permissions::from_mode(final_mode))
            .and_then(|()| file.sync_all())
            .map_err(|error| format!("failed to sync staged {}: {error}", kind.label()))?;
        let identity = FileIdentity::from_metadata(
            &file
                .metadata()
                .map_err(|error| format!("failed to inspect staged {}: {error}", kind.label()))?,
        );
        require_safe_regular(identity, expected_gid, kind, &path)?;
        require_expected_target(directory, name, Some(identity))?;
        Ok(identity)
    })();
    match operation {
        Ok(identity) => Ok(BoundFile { file, identity }),
        Err(message) => {
            let mut cleanup_failures = Vec::new();
            if let Err(error) = file
                .metadata()
                .map(|metadata| FileIdentity::from_metadata(&metadata))
                .map_err(|error| format!("failed to identify partial staged file: {error}"))
                .and_then(|identity| unlink_corresponding_entry(directory, name, identity))
            {
                cleanup_failures.push(error);
            }
            collect_sync_failures(&[directory], &mut cleanup_failures);
            if cleanup_failures.is_empty() {
                Err(message)
            } else {
                Err(format!(
                    "{message}; failed to remove partial staged file {}: {}",
                    path.display(),
                    cleanup_failures.join("; ")
                ))
            }
        }
    }
}

fn assign_destination_group(file: &File, gid: u32) -> io::Result<()> {
    #[cfg(test)]
    if let Some(errno) = FCHOWN_FAULT.with(|fault| fault.take()) {
        return Err(io::Error::from_raw_os_error(errno));
    }
    // SAFETY: file is live; -1 preserves uid and gid is supplied by a bound
    // destination directory.
    if unsafe { libc::fchown(file.as_raw_fd(), !0 as libc::uid_t, gid as libc::gid_t) } == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

fn probe_rename_capabilities(
    source: &BoundDirectory,
    recovery: &BoundDirectory,
    destination_gid: u32,
) -> Result<(), String> {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let source_name = std::ffi::OsString::from(format!(
        ".jetstreamer-{}-{}-{timestamp}",
        CAPABILITY_PROBE_A,
        std::process::id()
    ));
    let recovery_name = std::ffi::OsString::from(format!(
        ".jetstreamer-{}-{}-{timestamp}",
        CAPABILITY_PROBE_B,
        std::process::id()
    ));
    let moved_name = std::ffi::OsString::from(format!(
        ".jetstreamer-{}-{}-{timestamp}",
        CAPABILITY_PROBE_MOVED,
        std::process::id()
    ));
    let source_file = create_bound_file(
        source,
        ComponentKind::Checksum,
        &source_name,
        b"source rename probe\n",
        Some(destination_gid),
        FINAL_FILE_MODE,
    )?;
    let recovery_file = match create_bound_file(
        recovery,
        ComponentKind::Checksum,
        &recovery_name,
        b"destination rename probe\n",
        Some(destination_gid),
        FINAL_FILE_MODE,
    ) {
        Ok(file) => file,
        Err(error) => {
            let mut cleanup_failures = Vec::new();
            if let Err(cleanup) =
                unlink_corresponding_entry(source, &source_name, source_file.identity)
            {
                cleanup_failures.push(cleanup);
            }
            collect_sync_failures(&[source], &mut cleanup_failures);
            return if cleanup_failures.is_empty() {
                Err(error)
            } else {
                Err(format!(
                    "{error}; probe cleanup failed: {}",
                    cleanup_failures.join("; ")
                ))
            };
        }
    };
    let probe = (|| {
        sync_all(&[source, recovery])?;
        rename_noreplace(source, &source_name, recovery, &moved_name)
            .map_err(|error| format!("RENAME_NOREPLACE is unavailable: {error}"))?;
        require_corresponding_target(
            recovery,
            &moved_name,
            Some(source_file.identity),
            "moved rename probe",
        )?;
        rename_noreplace(recovery, &moved_name, source, &source_name)
            .map_err(|error| format!("cross-directory RENAME_NOREPLACE failed: {error}"))?;
        require_corresponding_target(
            source,
            &source_name,
            Some(source_file.identity),
            "returned rename probe",
        )?;
        rename_exchange(source, &source_name, recovery, &recovery_name)
            .map_err(|error| format!("cross-directory RENAME_EXCHANGE is unavailable: {error}"))?;
        require_corresponding_target(
            source,
            &source_name,
            Some(recovery_file.identity),
            "exchanged source probe",
        )?;
        require_corresponding_target(
            recovery,
            &recovery_name,
            Some(source_file.identity),
            "exchanged recovery probe",
        )?;
        rename_exchange(source, &source_name, recovery, &recovery_name)
            .map_err(|error| format!("failed to restore RENAME_EXCHANGE probe: {error}"))?;
        sync_all(&[source, recovery])
    })();

    let mut cleanup_failures = Vec::new();
    cleanup_probe_identity(
        source,
        &[&source_name],
        &[source_file.identity, recovery_file.identity],
        &mut cleanup_failures,
    );
    cleanup_probe_identity(
        recovery,
        &[&recovery_name, &moved_name],
        &[source_file.identity, recovery_file.identity],
        &mut cleanup_failures,
    );
    collect_sync_failures(&[source, recovery], &mut cleanup_failures);
    match (probe, cleanup_failures.is_empty()) {
        (Ok(()), true) => Ok(()),
        (Ok(()), false) => Err(format!(
            "rename capability probe cleanup failed: {}",
            cleanup_failures.join("; ")
        )),
        (Err(error), true) => Err(error),
        (Err(error), false) => Err(format!(
            "{error}; rename capability probe cleanup failed: {}",
            cleanup_failures.join("; ")
        )),
    }
}

fn cleanup_probe_identity(
    directory: &BoundDirectory,
    names: &[&OsStr],
    identities: &[FileIdentity],
    failures: &mut Vec<String>,
) {
    for name in names {
        match fstatat_identity(directory.file.as_raw_fd(), name) {
            Ok(None) => {}
            Ok(Some(observed)) => {
                if identities
                    .iter()
                    .any(|identity| observed.same_across_rename(*identity))
                {
                    if let Err(error) = unlink_corresponding_entry(directory, name, observed) {
                        failures.push(error);
                    }
                } else {
                    failures.push(format!(
                        "refusing to remove changed rename probe {}",
                        directory.path.join(name).display()
                    ));
                }
            }
            Err(error) => failures.push(format!(
                "failed to inspect rename probe {}: {error}",
                directory.path.join(name).display()
            )),
        }
    }
}

fn require_expected_target(
    directory: &BoundDirectory,
    name: &OsStr,
    expected: Option<FileIdentity>,
) -> Result<(), String> {
    if target_identity(directory, name)? == expected {
        Ok(())
    } else {
        Err(format!(
            "target changed concurrently: {}",
            directory.path.join(name).display()
        ))
    }
}

fn target_corresponds(
    directory: &BoundDirectory,
    name: &OsStr,
    expected: Option<FileIdentity>,
) -> Result<bool, String> {
    match (target_identity(directory, name)?, expected) {
        (None, None) => Ok(true),
        (Some(observed), Some(expected)) => Ok(observed.same_across_rename(expected)),
        _ => Ok(false),
    }
}

fn require_corresponding_target(
    directory: &BoundDirectory,
    name: &OsStr,
    expected: Option<FileIdentity>,
    label: &str,
) -> Result<(), String> {
    if target_corresponds(directory, name, expected)? {
        Ok(())
    } else {
        Err(format!(
            "{label} does not correspond to the expected inode: {}",
            directory.path.join(name).display()
        ))
    }
}

fn sync_all(directories: &[&BoundDirectory]) -> Result<(), String> {
    let mut failures = Vec::new();
    collect_sync_failures(directories, &mut failures);
    if failures.is_empty() {
        Ok(())
    } else {
        Err(failures.join("; "))
    }
}

fn collect_sync_failures(directories: &[&BoundDirectory], failures: &mut Vec<String>) {
    for directory in directories {
        if let Err(error) = directory.sync() {
            failures.push(error);
        }
    }
}

fn require_recovery_binding(
    destination: &BoundDirectory,
    recovery_name: &OsStr,
    recovery: &BoundDirectory,
) -> Result<(), String> {
    let descriptor_identity =
        FileIdentity::from_metadata(&recovery.file.metadata().map_err(|error| {
            format!(
                "failed to inspect bound recovery directory {}: {error}",
                recovery.path.display()
            )
        })?);
    let entry_identity = fstatat_identity(destination.file.as_raw_fd(), recovery_name)
        .map_err(|error| format!("failed to inspect recovery directory entry: {error}"))?
        .ok_or_else(|| {
            format!(
                "recovery directory entry disappeared: {}",
                recovery.path.display()
            )
        })?;
    if descriptor_identity.same_directory_binding(recovery.identity)
        && entry_identity.same_directory_binding(recovery.identity)
        && entry_identity.mode & libc::S_IFMT == libc::S_IFDIR
    {
        Ok(())
    } else {
        Err(format!(
            "recovery directory entry no longer names the bound inode: {}",
            recovery.path.display()
        ))
    }
}

fn rebound_target_identity(
    directory: &BoundDirectory,
    name: &OsStr,
    previous: FileIdentity,
) -> Result<FileIdentity, String> {
    let observed = target_identity(directory, name)?.ok_or_else(|| {
        format!(
            "target disappeared after rename: {}",
            directory.path.join(name).display()
        )
    })?;
    if !observed.same_across_rename(previous) {
        return Err(format!(
            "target inode or content metadata changed across rename: {}",
            directory.path.join(name).display()
        ));
    }
    Ok(observed)
}

fn target_identity(
    directory: &BoundDirectory,
    name: &OsStr,
) -> Result<Option<FileIdentity>, String> {
    let observed = bind_optional_regular(directory, name, ComponentKind::Archive)?;
    Ok(observed.map(|bound| bound.identity))
}

fn recheck_bound_file(
    directory: &BoundDirectory,
    name: &OsStr,
    expected: FileIdentity,
) -> Result<(), String> {
    require_expected_target(directory, name, Some(expected))
}

fn fstatat_identity(directory_fd: RawFd, name: &OsStr) -> io::Result<Option<FileIdentity>> {
    let name = os_str_cstring(name)?;
    let mut stat = MaybeUninit::<libc::stat>::uninit();
    // SAFETY: directory_fd is live, name is NUL terminated, and stat points to
    // writable storage. AT_SYMLINK_NOFOLLOW inspects the directory entry.
    let result = unsafe {
        libc::fstatat(
            directory_fd,
            name.as_ptr(),
            stat.as_mut_ptr(),
            libc::AT_SYMLINK_NOFOLLOW,
        )
    };
    if result == 0 {
        // SAFETY: successful fstatat initialized stat.
        let stat = unsafe { stat.assume_init() };
        Ok(Some(FileIdentity::from_stat(&stat)))
    } else {
        let error = io::Error::last_os_error();
        if error.kind() == io::ErrorKind::NotFound {
            Ok(None)
        } else {
            Err(error)
        }
    }
}

fn openat_directory(directory_fd: RawFd, name: &OsStr) -> io::Result<File> {
    let name = os_str_cstring(name)?;
    // SAFETY: directory_fd is live and name is a NUL-terminated basename. The
    // returned descriptor is uniquely owned.
    let fd = unsafe {
        libc::openat(
            directory_fd,
            name.as_ptr(),
            libc::O_RDONLY | libc::O_CLOEXEC | libc::O_DIRECTORY | libc::O_NOFOLLOW,
        )
    };
    if fd < 0 {
        Err(io::Error::last_os_error())
    } else {
        // SAFETY: openat returned a fresh owned descriptor.
        Ok(unsafe { File::from_raw_fd(fd) })
    }
}

fn openat_create(directory_fd: RawFd, name: &OsStr, mode: u32) -> io::Result<File> {
    let name = os_str_cstring(name)?;
    // SAFETY: directory_fd is live and name is a NUL-terminated basename. The
    // returned descriptor is uniquely owned.
    let fd = unsafe {
        libc::openat(
            directory_fd,
            name.as_ptr(),
            libc::O_RDWR | libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_CREAT | libc::O_EXCL,
            mode as libc::mode_t,
        )
    };
    if fd < 0 {
        Err(io::Error::last_os_error())
    } else {
        // SAFETY: openat returned a fresh owned descriptor.
        Ok(unsafe { File::from_raw_fd(fd) })
    }
}

fn openat_readonly(directory_fd: RawFd, name: &OsStr) -> io::Result<File> {
    let name = os_str_cstring(name)?;
    // SAFETY: directory_fd is a live directory descriptor and name is a
    // NUL-terminated basename. The returned descriptor is uniquely owned.
    let fd = unsafe {
        libc::openat(
            directory_fd,
            name.as_ptr(),
            libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK,
        )
    };
    if fd < 0 {
        Err(io::Error::last_os_error())
    } else {
        // SAFETY: openat returned a fresh owned descriptor.
        Ok(unsafe { File::from_raw_fd(fd) })
    }
}

fn os_str_cstring(name: &OsStr) -> io::Result<CString> {
    if name.as_bytes().contains(&b'/') {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "operation requires a basename",
        ));
    }
    CString::new(name.as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "filename contains a NUL byte"))
}

fn rename_noreplace(
    source_directory: &BoundDirectory,
    source_name: &OsStr,
    destination_directory: &BoundDirectory,
    destination_name: &OsStr,
) -> io::Result<()> {
    renameat2(
        source_directory.file.as_raw_fd(),
        source_name,
        destination_directory.file.as_raw_fd(),
        destination_name,
        libc::RENAME_NOREPLACE,
    )
}

fn rename_exchange(
    source_directory: &BoundDirectory,
    source_name: &OsStr,
    destination_directory: &BoundDirectory,
    destination_name: &OsStr,
) -> io::Result<()> {
    renameat2(
        source_directory.file.as_raw_fd(),
        source_name,
        destination_directory.file.as_raw_fd(),
        destination_name,
        libc::RENAME_EXCHANGE,
    )
}

fn renameat2(
    source_directory_fd: RawFd,
    source_name: &OsStr,
    destination_directory_fd: RawFd,
    destination_name: &OsStr,
    flags: u32,
) -> io::Result<()> {
    #[cfg(test)]
    if let Some(error) = injected_renameat2_error(flags) {
        return Err(error);
    }
    let source_name = os_str_cstring(source_name)?;
    let destination_name = os_str_cstring(destination_name)?;
    // SAFETY: both descriptors are live directories and both paths are
    // NUL-terminated basenames. renameat2 does not dereference user pointers
    // after the syscall returns.
    let result = unsafe {
        libc::renameat2(
            source_directory_fd,
            source_name.as_ptr(),
            destination_directory_fd,
            destination_name.as_ptr(),
            flags,
        )
    };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(test)]
thread_local! {
    static RENAMEAT2_FAULT: std::cell::Cell<Option<(u32, i32)>> = const {
        std::cell::Cell::new(None)
    };
    static SYNC_FAULT: std::cell::Cell<Option<i32>> = const {
        std::cell::Cell::new(None)
    };
    static SYNC_ATTEMPTS: std::cell::Cell<usize> = const {
        std::cell::Cell::new(0)
    };
    static FCHOWN_FAULT: std::cell::Cell<Option<i32>> = const {
        std::cell::Cell::new(None)
    };
}

#[cfg(test)]
fn injected_renameat2_error(flags: u32) -> Option<io::Error> {
    RENAMEAT2_FAULT.with(|fault| {
        fault
            .get()
            .filter(|(fault_flags, _)| *fault_flags == flags)
            .map(|(_, errno)| io::Error::from_raw_os_error(errno))
    })
}

fn remove_corresponding_file(
    directory: &BoundDirectory,
    name: &OsStr,
    expected: FileIdentity,
) -> Result<(), String> {
    require_corresponding_target(
        directory,
        name,
        Some(expected),
        "transaction cleanup target",
    )?;
    unlink_corresponding_entry(directory, name, expected)
}

fn unlink_corresponding_entry(
    directory: &BoundDirectory,
    name: &OsStr,
    expected: FileIdentity,
) -> Result<(), String> {
    let observed = fstatat_identity(directory.file.as_raw_fd(), name)
        .map_err(|error| format!("failed to inspect transaction file: {error}"))?
        .ok_or_else(|| {
            format!(
                "transaction file disappeared before cleanup: {}",
                directory.path.join(name).display()
            )
        })?;
    if observed.mode & libc::S_IFMT != libc::S_IFREG || !observed.same_across_rename(expected) {
        return Err(format!(
            "refusing to remove changed transaction file: {}",
            directory.path.join(name).display()
        ));
    }
    let name_c = os_str_cstring(name).map_err(|error| error.to_string())?;
    // SAFETY: the directory and basename are valid. Identity was checked
    // immediately above and the directory is private to this transaction.
    if unsafe { libc::unlinkat(directory.file.as_raw_fd(), name_c.as_ptr(), 0) } != 0 {
        return Err(format!(
            "failed to remove staged transaction file: {}",
            io::Error::last_os_error()
        ));
    }
    Ok(())
}

fn remove_empty_recovery_directory(
    destination: &BoundDirectory,
    recovery_name: &OsStr,
    recovery: &BoundDirectory,
) -> Result<(), String> {
    recovery.sync()?;
    require_recovery_binding(destination, recovery_name, recovery)?;
    let name = os_str_cstring(recovery_name).map_err(|error| error.to_string())?;
    // SAFETY: destination is a live directory descriptor and name is the
    // transaction directory's checked basename.
    if unsafe {
        libc::unlinkat(
            destination.file.as_raw_fd(),
            name.as_ptr(),
            libc::AT_REMOVEDIR,
        )
    } != 0
    {
        return Err(format!(
            "failed to remove empty recovery directory {}: {}",
            recovery.path.display(),
            io::Error::last_os_error()
        ));
    }
    destination.sync()
}

fn cleanup_unused_recovery_directory(
    destination: &BoundDirectory,
    recovery_name: &OsStr,
    recovery: &BoundDirectory,
    staged_files: &[(std::ffi::OsString, FileIdentity)],
) -> Result<(), String> {
    let mut failures = Vec::new();
    for (name, identity) in staged_files {
        if let Err(error) = remove_corresponding_file(recovery, name, *identity) {
            failures.push(error);
        }
    }
    collect_sync_failures(&[recovery, destination], &mut failures);
    if failures.is_empty() {
        remove_empty_recovery_directory(destination, recovery_name, recovery)
    } else {
        Err(failures.join("; "))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const BATCH_MANIFEST_FINGERPRINT: [u8; 32] = [0x5a; 32];

    use {
        crate::segment_manifest::{
            HistoricalSegmentManifest, SEGMENT_MANIFEST_SCHEMA_VERSION, SegmentCheckpointSummary,
            SegmentRuntimeAdmission, SegmentRuntimeIdentity, write_segment_manifest,
        },
        jetstreamer_horizon::archive::{
            ArchiveProvenanceV1, ArchiveProvenanceV2, ArchiveWriter, ArchiveWriterConfig,
            BootstrapStateKind, RuntimeAdmission, TransactionMetadataPolicy,
        },
        solana_hash::Hash,
    };

    struct RenameFaultGuard;

    impl RenameFaultGuard {
        fn install(flags: u32, errno: i32) -> Self {
            RENAMEAT2_FAULT.with(|fault| {
                assert!(fault.replace(Some((flags, errno))).is_none());
            });
            Self
        }
    }

    impl Drop for RenameFaultGuard {
        fn drop(&mut self) {
            RENAMEAT2_FAULT.with(|fault| fault.set(None));
        }
    }

    struct ChownFaultGuard;

    impl ChownFaultGuard {
        fn install(errno: i32) -> Self {
            FCHOWN_FAULT.with(|fault| {
                assert!(fault.replace(Some(errno)).is_none());
            });
            Self
        }
    }

    impl Drop for ChownFaultGuard {
        fn drop(&mut self) {
            FCHOWN_FAULT.with(|fault| fault.set(None));
        }
    }

    fn recovery_directories(parent: &Path) -> Vec<PathBuf> {
        fs::read_dir(parent)
            .unwrap()
            .map(|entry| entry.unwrap())
            .filter(|entry| {
                entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with(".jetstreamer-recovery-")
            })
            .map(|entry| entry.path())
            .collect()
    }

    fn fixture() -> (tempfile::TempDir, PathBuf, PathBuf, ValidatedArchiveFile) {
        let root = tempfile::TempDir::new().unwrap();
        let source = root.path().join("source");
        let destination = root.path().join("destination");
        fs::create_dir(&source).unwrap();
        fs::create_dir(&destination).unwrap();
        fs::set_permissions(&source, fs::Permissions::from_mode(0o700)).unwrap();
        fs::set_permissions(&destination, fs::Permissions::from_mode(0o3770)).unwrap();
        let archive = source.join("epoch-7.jet");
        fs::write(&archive, b"new archive").unwrap();
        let destination_gid = fs::metadata(&destination).unwrap().gid();
        let archive_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&archive)
            .unwrap();
        // SAFETY: the test owns the regular file descriptor.
        assert_eq!(
            unsafe { libc::fchown(archive_file.as_raw_fd(), !0 as libc::uid_t, destination_gid,) },
            0
        );
        archive_file
            .set_permissions(fs::Permissions::from_mode(FINAL_FILE_MODE))
            .unwrap();
        archive_file.sync_all().unwrap();
        let evidence = crate::archive_checksum::measure_open_archive(&archive_file).unwrap();
        (root, archive, destination.join("epoch-7.jet"), evidence)
    }

    fn write_existing_set(destination: &Path) -> [u64; 3] {
        let manifest = segment_manifest_path(destination).unwrap();
        let checksum = archive_checksum_path(destination).unwrap();
        fs::write(destination, b"old archive").unwrap();
        fs::write(&manifest, b"old manifest").unwrap();
        fs::write(&checksum, b"old checksum").unwrap();
        for path in [destination, manifest.as_path(), checksum.as_path()] {
            fs::set_permissions(path, fs::Permissions::from_mode(0o440)).unwrap();
        }
        [
            fs::metadata(destination).unwrap().ino(),
            fs::metadata(&manifest).unwrap().ino(),
            fs::metadata(&checksum).unwrap().ino(),
        ]
    }

    fn fixture_with_manifest() -> (tempfile::TempDir, PathBuf, PathBuf, ValidatedArchiveFile) {
        const SLOT_START: u64 = 3_024_000;
        const SLOT_COUNT: u64 = 2;
        const BOOTSTRAP_SLOT: u64 = SLOT_START - 1;
        const WORKER_DIGEST: [u8; 32] = [0x42; 32];

        let root = tempfile::TempDir::new().unwrap();
        let source = root.path().join("source");
        let destination = root.path().join("destination");
        fs::create_dir(&source).unwrap();
        fs::create_dir(&destination).unwrap();
        fs::set_permissions(&source, fs::Permissions::from_mode(0o700)).unwrap();
        fs::set_permissions(&destination, fs::Permissions::from_mode(0o3770)).unwrap();

        let hash = |byte| Hash::new_from_array([byte; 32]);
        let runtime = SegmentRuntimeIdentity {
            generation_profile: "test-generation".into(),
            runtime_profile: "test-runtime".into(),
            runtime_admission: SegmentRuntimeAdmission::Verified,
            runtime_revision: "test-revision".into(),
            runtime_toolchain: "test-toolchain".into(),
            runtime_target: "x86_64-unknown-linux-gnu".into(),
            genesis_hash: hash(1).to_string(),
        };
        let provenance = ArchiveProvenanceV2 {
            base: ArchiveProvenanceV1 {
                generation_profile: runtime.generation_profile.clone(),
                runtime_profile: runtime.runtime_profile.clone(),
                runtime_admission: RuntimeAdmission::Verified,
                runtime_revision: runtime.runtime_revision.clone(),
                runtime_toolchain: runtime.runtime_toolchain.clone(),
                genesis_hash: hash(1),
                bootstrap_state_kind: BootstrapStateKind::SnapshotArchive,
                bootstrap_slot: BOOTSTRAP_SLOT,
                bootstrap_state_hash: hash(2),
                requested_slot_start: SLOT_START,
                requested_slot_count: SLOT_COUNT,
                transaction_metadata: TransactionMetadataPolicy::observed(),
            },
            worker_executable_sha256: WORKER_DIGEST,
        };
        let mut writer = ArchiveWriter::new_with_provenance(
            Vec::new(),
            7,
            SLOT_START,
            SLOT_COUNT,
            ArchiveWriterConfig::default(),
            &provenance.into(),
        )
        .unwrap();
        for slot in SLOT_START..SLOT_START + SLOT_COUNT {
            writer.write_skipped_slot(slot).unwrap();
        }
        let (bytes, _) = writer.finish().unwrap();
        let archive = source.join("epoch-7.jet");
        fs::write(&archive, bytes).unwrap();
        let manifest = HistoricalSegmentManifest {
            schema_version: SEGMENT_MANIFEST_SCHEMA_VERSION,
            epoch: 7,
            output_slot_start: SLOT_START,
            output_slot_count: SLOT_COUNT,
            runtime,
            worker_executable_sha256: WORKER_DIGEST,
            archive_sha256: [0; 32],
            bootstrap_archive_sha256: None,
            bootstrap: SegmentCheckpointSummary {
                slot: BOOTSTRAP_SLOT,
                bank_hash: hash(3).to_string(),
                accounts_hash: hash(2).to_string(),
                last_blockhash: hash(4).to_string(),
                capitalization: 1,
                transaction_count: 2,
                tick_height: 3,
                slot_complete: true,
                write_count: 0,
                next_write_version: 10,
            },
            terminal: SegmentCheckpointSummary {
                slot: SLOT_START + SLOT_COUNT - 1,
                bank_hash: hash(5).to_string(),
                accounts_hash: hash(6).to_string(),
                last_blockhash: hash(7).to_string(),
                capitalization: 1,
                transaction_count: 2,
                tick_height: 3,
                slot_complete: true,
                write_count: 0,
                next_write_version: 10,
            },
            emitted_raw_write_versions: 10..10,
        };
        write_segment_manifest(&archive, manifest).unwrap();

        let destination_gid = fs::metadata(&destination).unwrap().gid();
        let archive_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&archive)
            .unwrap();
        // SAFETY: the test owns the regular file descriptor.
        assert_eq!(
            unsafe { libc::fchown(archive_file.as_raw_fd(), !0, destination_gid) },
            0
        );
        archive_file
            .set_permissions(fs::Permissions::from_mode(FINAL_FILE_MODE))
            .unwrap();
        archive_file.sync_all().unwrap();
        let evidence = crate::archive_checksum::measure_open_archive(&archive_file).unwrap();
        (root, archive, destination.join("epoch-7.jet"), evidence)
    }

    fn corrected_batch_fixture(
        end_epoch: u64,
        with_manifests: bool,
    ) -> (tempfile::TempDir, Vec<ArchiveBatchItem>) {
        assert!((7..=10).contains(&end_epoch));
        let (root, first_archive, first_destination, first_evidence) = fixture_with_manifest();
        let source = first_archive.parent().unwrap().to_path_buf();
        let destination = first_destination.parent().unwrap().to_path_buf();
        let first_manifest = segment_manifest_path(&first_archive).unwrap();
        if !with_manifests {
            fs::remove_file(&first_manifest).unwrap();
        }
        let mut items = Vec::new();
        for epoch in 7..=end_epoch {
            let staged_archive = source.join(format!("epoch-{epoch}.jet"));
            let destination_archive = destination.join(format!("epoch-{epoch}.jet"));
            let evidence = if epoch == 7 {
                first_evidence
            } else {
                fs::copy(&first_archive, &staged_archive).unwrap();
                if with_manifests {
                    fs::copy(
                        segment_manifest_path(&first_archive).unwrap(),
                        segment_manifest_path(&staged_archive).unwrap(),
                    )
                    .unwrap();
                }
                fs::set_permissions(&staged_archive, fs::Permissions::from_mode(FINAL_FILE_MODE))
                    .unwrap();
                let file = OpenOptions::new().read(true).open(&staged_archive).unwrap();
                file.sync_all().unwrap();
                crate::archive_checksum::measure_open_archive(&file).unwrap()
            };
            write_existing_set(&destination_archive);
            items.push(ArchiveBatchItem {
                epoch,
                staged_archive,
                destination_archive,
                evidence,
            });
        }
        (root, items)
    }

    fn assert_batch_rolled_back(items: &[ArchiveBatchItem]) {
        for item in items {
            let staged =
                crate::archive_checksum::open_regular_nofollow(&item.staged_archive).unwrap();
            assert_eq!(
                crate::archive_checksum::measure_open_archive(&staged)
                    .unwrap()
                    .sha256,
                item.evidence.sha256
            );
            assert_eq!(fs::read(&item.destination_archive).unwrap(), b"old archive");
            assert_eq!(
                fs::read(segment_manifest_path(&item.destination_archive).unwrap()).unwrap(),
                b"old manifest"
            );
            assert_eq!(
                fs::read(archive_checksum_path(&item.destination_archive).unwrap()).unwrap(),
                b"old checksum"
            );
        }
    }

    fn assert_batch_committed(items: &[ArchiveBatchItem]) {
        for item in items {
            assert!(!item.staged_archive.exists());
            let published =
                crate::archive_checksum::open_regular_nofollow(&item.destination_archive).unwrap();
            assert_eq!(
                crate::archive_checksum::measure_open_archive(&published)
                    .unwrap()
                    .sha256,
                item.evidence.sha256
            );
            let expected = archive_checksum_line(
                &item.evidence.sha256,
                item.destination_archive.file_name().unwrap(),
            )
            .unwrap();
            assert_eq!(
                fs::read(archive_checksum_path(&item.destination_archive).unwrap()).unwrap(),
                expected.as_bytes()
            );
        }
    }

    #[test]
    fn corrected_epochs_7_through_10_publish_as_one_batch() {
        let (_root, items) = corrected_batch_fixture(10, false);
        let destination = items[0].destination_archive.parent().unwrap();

        let publication =
            publish_verified_archive_batch(BATCH_MANIFEST_FINGERPRINT, &items).unwrap();

        assert_eq!(publication.publications.len(), 4);
        assert_ne!(publication.transaction_id, [0; 32]);
        assert_eq!(publication.manifest_fingerprint, BATCH_MANIFEST_FINGERPRINT);
        assert_eq!(publication.identity_evidence.len(), 4);
        for (epoch, evidence) in (7..=10).zip(&publication.identity_evidence) {
            assert_eq!(evidence.epoch, epoch);
            assert!(evidence.initial_archive.is_some());
            assert!(evidence.initial_manifest.is_some());
            assert!(evidence.initial_checksum.is_some());
            assert_eq!(
                evidence.archive_validation.sha256,
                items[(epoch - 7) as usize].evidence.sha256
            );
            assert_ne!(
                evidence.initial_archive.unwrap().inode,
                evidence.committed_archive.inode
            );
            assert_ne!(
                evidence.initial_checksum.unwrap().inode,
                evidence.committed_checksum.inode
            );
        }
        assert_batch_committed(&items);
        for item in &items {
            assert!(
                !segment_manifest_path(&item.destination_archive)
                    .unwrap()
                    .exists()
            );
        }
        assert!(
            !destination
                .join(ARCHIVE_BATCH_TRANSACTION_DIRECTORY)
                .exists()
        );
        assert!(destination.join(ARCHIVE_BATCH_OUTCOME_DIRECTORY).is_dir());
        assert!(archive_batch_publication_in_progress(destination).unwrap());
        let ArchiveBatchRecovery::Committed(recovered) =
            recover_archive_publication_batch(destination).unwrap()
        else {
            panic!("completed batch outcome was not recoverable");
        };
        assert_eq!(recovered.transaction_id, publication.transaction_id);
        assert_eq!(recovered.manifest_fingerprint, BATCH_MANIFEST_FINGERPRINT);
        acknowledge_archive_publication_batch(destination, publication.transaction_id).unwrap();
        assert!(!destination.join(ARCHIVE_BATCH_OUTCOME_DIRECTORY).exists());
        assert!(!archive_batch_publication_in_progress(destination).unwrap());
    }

    #[test]
    fn batch_rejects_duplicate_epochs_and_destination_targets() {
        let (_root, items) = corrected_batch_fixture(8, false);
        let duplicate_epoch = vec![items[0].clone(), items[0].clone()];
        let error = publish_verified_archive_batch(BATCH_MANIFEST_FINGERPRINT, &duplicate_epoch)
            .unwrap_err();
        assert!(error.to_string().contains("duplicate epoch"), "{error}");

        let second_source = items[1]
            .staged_archive
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .join("second-source");
        fs::create_dir(&second_source).unwrap();
        fs::set_permissions(&second_source, fs::Permissions::from_mode(0o700)).unwrap();
        let duplicate_staged = second_source.join("epoch-7.jet");
        fs::copy(&items[1].staged_archive, &duplicate_staged).unwrap();
        fs::set_permissions(
            &duplicate_staged,
            fs::Permissions::from_mode(FINAL_FILE_MODE),
        )
        .unwrap();
        let duplicate_file = OpenOptions::new()
            .read(true)
            .open(&duplicate_staged)
            .unwrap();
        duplicate_file.sync_all().unwrap();
        let duplicate_target = vec![
            items[0].clone(),
            ArchiveBatchItem {
                epoch: 8,
                staged_archive: duplicate_staged,
                destination_archive: items[0].destination_archive.clone(),
                evidence: crate::archive_checksum::measure_open_archive(&duplicate_file).unwrap(),
            },
        ];
        let error = publish_verified_archive_batch(BATCH_MANIFEST_FINGERPRINT, &duplicate_target)
            .unwrap_err();
        assert!(
            error.to_string().contains("destination namespaces overlap"),
            "{error}"
        );
    }

    #[test]
    fn batch_rejects_noncanonical_input_paths_before_preflight() {
        let (_root, mut items) = corrected_batch_fixture(8, false);
        let source = items[0].staged_archive.parent().unwrap();
        items[0].staged_archive = source.join("subdirectory").join("..").join("epoch-7.jet");

        let error = publish_verified_archive_batch(BATCH_MANIFEST_FINGERPRINT, &items).unwrap_err();

        assert!(error.to_string().contains("non-canonical"), "{error}");
        assert_batch_rolled_back(&items[1..]);
    }

    #[test]
    fn batch_recovery_rejects_an_intermediate_source_symlink() {
        let (root, mut items) = corrected_batch_fixture(8, false);
        let original_source = items[0].staged_archive.parent().unwrap().to_path_buf();
        let container = root.path().join("container");
        fs::create_dir(&container).unwrap();
        fs::set_permissions(&container, fs::Permissions::from_mode(0o700)).unwrap();
        let moved_source = container.join("source");
        fs::rename(&original_source, &moved_source).unwrap();
        for item in &mut items {
            item.staged_archive = moved_source.join(item.staged_archive.file_name().unwrap());
        }
        let destination = items[0].destination_archive.parent().unwrap().to_path_buf();
        let crashed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = publish_verified_archive_batch_with_hook(
                BATCH_MANIFEST_FINGERPRINT,
                &items,
                |phase| {
                    if phase == ArchiveBatchPhase::JournalPrepared {
                        panic!("simulated crash before source ancestor replacement");
                    }
                    Ok(())
                },
            );
        }));
        assert!(crashed.is_err());
        let real_container = root.path().join("container-real");
        fs::rename(&container, &real_container).unwrap();
        std::os::unix::fs::symlink(&real_container, &container).unwrap();

        let error = recover_archive_publication_batch(&destination).unwrap_err();

        assert!(
            error.to_string().contains("without following links"),
            "{error}"
        );
        for item in &items {
            assert_eq!(fs::read(&item.destination_archive).unwrap(), b"old archive");
        }
        assert!(
            destination
                .join(ARCHIVE_BATCH_TRANSACTION_DIRECTORY)
                .is_dir()
        );
    }

    #[test]
    fn batch_recovery_rejects_noncanonical_journal_paths() {
        let (_root, items) = corrected_batch_fixture(8, false);
        let destination = items[0].destination_archive.parent().unwrap().to_path_buf();
        let crashed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = publish_verified_archive_batch_with_hook(
                BATCH_MANIFEST_FINGERPRINT,
                &items,
                |phase| {
                    if phase == ArchiveBatchPhase::JournalPrepared {
                        panic!("simulated crash before journal tampering");
                    }
                    Ok(())
                },
            );
        }));
        assert!(crashed.is_err());
        let journal_path = destination
            .join(ARCHIVE_BATCH_TRANSACTION_DIRECTORY)
            .join(ARCHIVE_BATCH_JOURNAL);
        let mut journal: ArchiveBatchJournal =
            serde_json::from_slice(&fs::read(&journal_path).unwrap()).unwrap();
        let source = path_from_bytes(&journal.items[0].source_path, "source").unwrap();
        journal.items[0].source_path = path_bytes(
            &source
                .parent()
                .unwrap()
                .join("not-used")
                .join("..")
                .join(source.file_name().unwrap()),
        );
        fs::set_permissions(&journal_path, fs::Permissions::from_mode(0o600)).unwrap();
        fs::write(&journal_path, serde_json::to_vec_pretty(&journal).unwrap()).unwrap();
        fs::set_permissions(&journal_path, fs::Permissions::from_mode(0o400)).unwrap();

        let error = recover_archive_publication_batch(&destination).unwrap_err();

        assert!(error.to_string().contains("non-canonical"), "{error}");
        assert!(
            destination
                .join(ARCHIVE_BATCH_TRANSACTION_DIRECTORY)
                .is_dir()
        );
    }

    #[test]
    fn batch_recovery_rejects_journal_changes_before_namespace_mutation() {
        let (_root, items) = corrected_batch_fixture(8, false);
        let destination = items[0].destination_archive.parent().unwrap().to_path_buf();
        let crashed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = publish_verified_archive_batch_with_hook(
                BATCH_MANIFEST_FINGERPRINT,
                &items,
                |phase| {
                    if phase == ArchiveBatchPhase::JournalPrepared {
                        panic!("simulated crash before journal tampering");
                    }
                    Ok(())
                },
            );
        }));
        assert!(crashed.is_err());
        let journal_path = destination
            .join(ARCHIVE_BATCH_TRANSACTION_DIRECTORY)
            .join(ARCHIVE_BATCH_JOURNAL);
        let mut journal: ArchiveBatchJournal =
            serde_json::from_slice(&fs::read(&journal_path).unwrap()).unwrap();
        journal.manifest_fingerprint = [0x77; 32];
        fs::set_permissions(&journal_path, fs::Permissions::from_mode(0o600)).unwrap();
        fs::write(&journal_path, serde_json::to_vec_pretty(&journal).unwrap()).unwrap();
        fs::set_permissions(&journal_path, fs::Permissions::from_mode(0o400)).unwrap();

        let error = recover_archive_publication_batch(&destination).unwrap_err();

        assert!(
            error.to_string().contains("transaction ID commitment"),
            "{error}"
        );
        assert_batch_rolled_back(&items);
        assert!(
            destination
                .join(ARCHIVE_BATCH_TRANSACTION_DIRECTORY)
                .is_dir()
        );
    }

    #[test]
    fn unarmed_batch_marker_with_unknown_entry_is_not_renamed() {
        let (_root, items) = corrected_batch_fixture(8, false);
        let destination = items[0].destination_archive.parent().unwrap().to_path_buf();
        let marker = destination.join(ARCHIVE_BATCH_TRANSACTION_DIRECTORY);
        fs::create_dir(&marker).unwrap();
        fs::set_permissions(&marker, fs::Permissions::from_mode(0o700)).unwrap();
        fs::write(marker.join("unexpected"), b"do not discard").unwrap();

        let error = recover_archive_publication_batch(&destination).unwrap_err();

        assert!(error.to_string().contains("unexpected entry"), "{error}");
        assert!(marker.is_dir());
        assert_eq!(
            fs::read(marker.join("unexpected")).unwrap(),
            b"do not discard"
        );
        assert!(!destination.join(ARCHIVE_BATCH_OUTCOME_DIRECTORY).exists());
    }

    #[test]
    fn armed_batch_marker_with_unknown_entry_is_not_retired() {
        let (_root, items) = corrected_batch_fixture(8, false);
        let destination = items[0].destination_archive.parent().unwrap().to_path_buf();

        let error =
            publish_verified_archive_batch_with_hook(BATCH_MANIFEST_FINGERPRINT, &items, |phase| {
                if phase == ArchiveBatchPhase::BeforeTransactionMarkerRemoval {
                    fs::write(
                        destination
                            .join(ARCHIVE_BATCH_TRANSACTION_DIRECTORY)
                            .join("unexpected"),
                        b"do not discard",
                    )
                    .unwrap();
                }
                Ok(())
            })
            .unwrap_err();

        assert!(error.committed());
        assert!(error.to_string().contains("unexpected entry"), "{error}");
        assert!(
            destination
                .join(ARCHIVE_BATCH_TRANSACTION_DIRECTORY)
                .is_dir()
        );
        assert!(!destination.join(ARCHIVE_BATCH_OUTCOME_DIRECTORY).exists());
        let recovery = recover_archive_publication_batch(&destination).unwrap_err();
        assert!(
            recovery.to_string().contains("unexpected entry"),
            "{recovery}"
        );
    }

    #[test]
    fn final_batch_recheck_prevents_stale_outcome_evidence() {
        let (_root, items) = corrected_batch_fixture(8, false);
        let destination = items[0].destination_archive.parent().unwrap().to_path_buf();
        let displaced = destination.join("displaced-epoch-7.jet");

        let error =
            publish_verified_archive_batch_with_hook(BATCH_MANIFEST_FINGERPRINT, &items, |phase| {
                if phase == ArchiveBatchPhase::BeforeTransactionMarkerRemoval {
                    fs::rename(&items[0].destination_archive, &displaced).unwrap();
                    fs::write(&items[0].destination_archive, b"concurrent replacement").unwrap();
                    fs::set_permissions(
                        &items[0].destination_archive,
                        fs::Permissions::from_mode(FINAL_FILE_MODE),
                    )
                    .unwrap();
                }
                Ok(())
            })
            .unwrap_err();

        assert!(error.committed());
        assert!(
            error.to_string().contains("automatic recovery failed"),
            "{error}"
        );
        assert!(
            destination
                .join(ARCHIVE_BATCH_TRANSACTION_DIRECTORY)
                .is_dir()
        );
        assert!(!destination.join(ARCHIVE_BATCH_OUTCOME_DIRECTORY).exists());
        assert!(displaced.is_file());
    }

    #[test]
    fn batch_outcome_requires_matching_acknowledgement() {
        let (_root, items) = corrected_batch_fixture(8, false);
        let destination = items[0].destination_archive.parent().unwrap().to_path_buf();
        let publication =
            publish_verified_archive_batch(BATCH_MANIFEST_FINGERPRINT, &items).unwrap();

        let error = acknowledge_archive_publication_batch(&destination, [0x55; 32]).unwrap_err();

        assert!(error.to_string().contains("transaction ID"), "{error}");
        assert!(destination.join(ARCHIVE_BATCH_OUTCOME_DIRECTORY).is_dir());
        let ArchiveBatchRecovery::Committed(recovered) =
            recover_archive_publication_batch(&destination).unwrap()
        else {
            panic!("pending outcome was lost after a mismatched acknowledgement");
        };
        assert_eq!(recovered.transaction_id, publication.transaction_id);
        acknowledge_archive_publication_batch(&destination, publication.transaction_id).unwrap();
    }

    #[test]
    fn every_batch_mutation_boundary_recovers_the_whole_cohort() {
        let mut phases = vec![ArchiveBatchPhase::JournalPrepared];
        for index in 0..2 {
            for phase in [
                PublishPhase::AfterChecksumInvalidationMutation,
                PublishPhase::AfterManifestInstallMutation,
                PublishPhase::AfterManifestBackupMutation,
                PublishPhase::AfterArchiveInstallMutation,
                PublishPhase::AfterArchiveBackupMutation,
            ] {
                phases.push(ArchiveBatchPhase::Item { index, phase });
            }
        }
        phases.push(ArchiveBatchPhase::CommitDecisionDurable);
        for index in 0..2 {
            phases.push(ArchiveBatchPhase::Item {
                index,
                phase: PublishPhase::AfterChecksumCommitMutation,
            });
        }
        phases.push(ArchiveBatchPhase::BeforeTransactionMarkerRemoval);
        phases.push(ArchiveBatchPhase::AfterTransactionMarkerRetirement);

        for crash_phase in phases {
            let (_root, items) = corrected_batch_fixture(8, true);
            let destination = items[0].destination_archive.parent().unwrap().to_path_buf();
            let crashed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let _ = publish_verified_archive_batch_with_hook(
                    BATCH_MANIFEST_FINGERPRINT,
                    &items,
                    |phase| {
                        if phase == crash_phase {
                            panic!("simulated batch publisher crash at {crash_phase:?}");
                        }
                        Ok(())
                    },
                );
            }));
            assert!(crashed.is_err(), "phase was not reached: {crash_phase:?}");
            if crash_phase == ArchiveBatchPhase::AfterTransactionMarkerRetirement {
                assert!(destination.join(ARCHIVE_BATCH_OUTCOME_DIRECTORY).is_dir());
            } else {
                assert!(
                    destination
                        .join(ARCHIVE_BATCH_TRANSACTION_DIRECTORY)
                        .is_dir()
                );
            }

            let recovery = recover_archive_publication_batch(&destination).unwrap();

            let committed = matches!(
                crash_phase,
                ArchiveBatchPhase::CommitDecisionDurable
                    | ArchiveBatchPhase::Item {
                        phase: PublishPhase::AfterChecksumCommitMutation,
                        ..
                    }
                    | ArchiveBatchPhase::BeforeTransactionMarkerRemoval
                    | ArchiveBatchPhase::AfterTransactionMarkerRetirement
            );
            let transaction_id = if committed {
                let ArchiveBatchRecovery::Committed(publication) = recovery else {
                    panic!("commit decision did not finish the cohort at {crash_phase:?}");
                };
                assert_eq!(publication.publications.len(), items.len());
                assert_batch_committed(&items);
                for item in &items {
                    assert_ne!(
                        fs::read(segment_manifest_path(&item.destination_archive).unwrap())
                            .unwrap(),
                        b"old manifest"
                    );
                }
                publication.transaction_id
            } else {
                let ArchiveBatchRecovery::RolledBack(rollback) = recovery else {
                    panic!("rollback decision did not restore the cohort at {crash_phase:?}");
                };
                assert_eq!(
                    rollback
                        .identity_evidence
                        .iter()
                        .map(|evidence| evidence.epoch)
                        .collect::<Vec<_>>(),
                    vec![7, 8]
                );
                assert_eq!(rollback.manifest_fingerprint, BATCH_MANIFEST_FINGERPRINT);
                assert_ne!(rollback.transaction_id, [0; 32]);
                for (evidence, item) in rollback.identity_evidence.iter().zip(&items) {
                    assert_eq!(evidence.staged_archive_path, item.staged_archive);
                    assert_eq!(
                        evidence.staged_archive_validation.sha256,
                        item.evidence.sha256
                    );
                }
                assert_batch_rolled_back(&items);
                rollback.transaction_id
            };
            assert!(
                !destination
                    .join(ARCHIVE_BATCH_TRANSACTION_DIRECTORY)
                    .exists()
            );
            assert!(destination.join(ARCHIVE_BATCH_OUTCOME_DIRECTORY).is_dir());
            let repeated = recover_archive_publication_batch(&destination).unwrap();
            match repeated {
                ArchiveBatchRecovery::Committed(publication) if committed => {
                    assert_eq!(publication.transaction_id, transaction_id);
                }
                ArchiveBatchRecovery::RolledBack(rollback) if !committed => {
                    assert_eq!(rollback.transaction_id, transaction_id);
                }
                _ => panic!("completed outcome changed on repeated recovery"),
            }
            acknowledge_archive_publication_batch(&destination, transaction_id).unwrap();
            assert!(!destination.join(ARCHIVE_BATCH_OUTCOME_DIRECTORY).exists());
        }
    }

    #[test]
    fn batch_marks_every_member_before_data_and_removes_marker_only_after_checksums() {
        let (_root, items) = corrected_batch_fixture(8, true);
        let destination = items[0].destination_archive.parent().unwrap().to_path_buf();
        let mut saw_data_gate = false;
        let mut saw_partial_checksum_commit = false;

        let publication =
            publish_verified_archive_batch_with_hook(BATCH_MANIFEST_FINGERPRINT, &items, |phase| {
                if phase
                    == (ArchiveBatchPhase::Item {
                        index: 0,
                        phase: PublishPhase::BeforeManifestMutation,
                    })
                {
                    saw_data_gate = true;
                    assert!(
                        destination
                            .join(ARCHIVE_BATCH_TRANSACTION_DIRECTORY)
                            .is_dir()
                    );
                    for item in &items {
                        assert_eq!(
                            fs::read(archive_checksum_path(&item.destination_archive).unwrap())
                                .unwrap(),
                            ARCHIVE_PUBLICATION_SENTINEL
                        );
                        assert_eq!(fs::read(&item.destination_archive).unwrap(), b"old archive");
                    }
                }
                if phase
                    == (ArchiveBatchPhase::Item {
                        index: 0,
                        phase: PublishPhase::AfterChecksumCommitMutation,
                    })
                {
                    saw_partial_checksum_commit = true;
                    assert!(
                        destination
                            .join(ARCHIVE_BATCH_TRANSACTION_DIRECTORY)
                            .is_dir()
                    );
                    assert_ne!(
                        fs::read(archive_checksum_path(&items[0].destination_archive).unwrap())
                            .unwrap(),
                        ARCHIVE_PUBLICATION_SENTINEL
                    );
                    assert_eq!(
                        fs::read(archive_checksum_path(&items[1].destination_archive).unwrap())
                            .unwrap(),
                        ARCHIVE_PUBLICATION_SENTINEL
                    );
                    for item in &items {
                        let published = crate::archive_checksum::open_regular_nofollow(
                            &item.destination_archive,
                        )
                        .unwrap();
                        assert_eq!(
                            crate::archive_checksum::measure_open_archive(&published)
                                .unwrap()
                                .sha256,
                            item.evidence.sha256
                        );
                    }
                }
                Ok(())
            })
            .unwrap();

        assert!(saw_data_gate);
        assert!(saw_partial_checksum_commit);
        assert_eq!(
            publication
                .identity_evidence
                .iter()
                .map(|evidence| evidence.epoch)
                .collect::<Vec<_>>(),
            vec![7, 8]
        );
        assert!(
            !destination
                .join(ARCHIVE_BATCH_TRANSACTION_DIRECTORY)
                .exists()
        );
        assert!(destination.join(ARCHIVE_BATCH_OUTCOME_DIRECTORY).is_dir());
        acknowledge_archive_publication_batch(&destination, publication.transaction_id).unwrap();
        assert!(!destination.join(ARCHIVE_BATCH_OUTCOME_DIRECTORY).exists());
    }

    #[test]
    fn batch_recovery_handles_initially_absent_destinations() {
        for (crash_phase, expect_commit) in [
            (
                ArchiveBatchPhase::Item {
                    index: 1,
                    phase: PublishPhase::AfterArchiveInstallMutation,
                },
                false,
            ),
            (ArchiveBatchPhase::CommitDecisionDurable, true),
        ] {
            let (_root, items) = corrected_batch_fixture(8, false);
            let destination = items[0].destination_archive.parent().unwrap().to_path_buf();
            for item in &items {
                fs::remove_file(&item.destination_archive).unwrap();
                fs::remove_file(segment_manifest_path(&item.destination_archive).unwrap()).unwrap();
                fs::remove_file(archive_checksum_path(&item.destination_archive).unwrap()).unwrap();
            }
            let crashed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let _ = publish_verified_archive_batch_with_hook(
                    BATCH_MANIFEST_FINGERPRINT,
                    &items,
                    |phase| {
                        if phase == crash_phase {
                            panic!("simulated absent-destination batch crash");
                        }
                        Ok(())
                    },
                );
            }));
            assert!(crashed.is_err());

            let recovery = recover_archive_publication_batch(&destination).unwrap();

            let transaction_id = if expect_commit {
                let ArchiveBatchRecovery::Committed(publication) = recovery else {
                    panic!("commit recovery did not commit");
                };
                assert_batch_committed(&items);
                publication.transaction_id
            } else {
                let ArchiveBatchRecovery::RolledBack(rollback) = recovery else {
                    panic!("rollback recovery did not roll back");
                };
                for item in &items {
                    assert!(item.staged_archive.is_file());
                    assert!(!item.destination_archive.exists());
                    assert!(
                        !archive_checksum_path(&item.destination_archive)
                            .unwrap()
                            .exists()
                    );
                }
                rollback.transaction_id
            };
            assert!(
                !destination
                    .join(ARCHIVE_BATCH_TRANSACTION_DIRECTORY)
                    .exists()
            );
            assert!(destination.join(ARCHIVE_BATCH_OUTCOME_DIRECTORY).is_dir());
            acknowledge_archive_publication_batch(&destination, transaction_id).unwrap();
            assert!(recovery_directories(&destination).is_empty());
            assert!(!destination.join(ARCHIVE_BATCH_OUTCOME_DIRECTORY).exists());
        }
    }

    #[test]
    fn batch_mutation_error_still_runs_its_fsync_barrier_before_rollback() {
        let (_root, items) = corrected_batch_fixture(8, true);

        let error =
            publish_verified_archive_batch_with_hook(BATCH_MANIFEST_FINGERPRINT, &items, |phase| {
                if phase
                    == (ArchiveBatchPhase::Item {
                        index: 1,
                        phase: PublishPhase::AfterArchiveInstallMutation,
                    })
                {
                    SYNC_ATTEMPTS.with(|attempts| attempts.set(0));
                    SYNC_FAULT.with(|fault| fault.set(Some(libc::EIO)));
                    return Err("injected batch post-mutation failure".to_string());
                }
                Ok(())
            })
            .unwrap_err();

        assert!(!error.committed());
        assert!(SYNC_ATTEMPTS.with(std::cell::Cell::get) > 0);
        assert_batch_rolled_back(&items);
    }

    #[test]
    fn checksum_is_the_last_commit_marker() {
        let (_root, archive, destination, evidence) = fixture();
        let mut observed = false;
        let mut sentinel_inode = None;
        let result =
            publish_verified_archive_with_hook(&archive, &destination, evidence, |phase| {
                if matches!(
                    phase,
                    PublishPhase::AfterChecksumInvalidationMutation
                        | PublishPhase::AfterChecksumInvalidation
                        | PublishPhase::BeforeManifestMutation
                        | PublishPhase::AfterManifestMutation
                        | PublishPhase::BeforeArchiveInstall
                        | PublishPhase::AfterArchiveInstall
                        | PublishPhase::BeforeChecksumCommit
                ) {
                    let checksum = archive_checksum_path(&destination).unwrap();
                    assert_eq!(fs::read(&checksum).unwrap(), ARCHIVE_PUBLICATION_SENTINEL);
                    let inode = fs::metadata(&checksum).unwrap().ino();
                    assert_eq!(*sentinel_inode.get_or_insert(inode), inode);
                    let create = OpenOptions::new()
                        .write(true)
                        .create_new(true)
                        .open(checksum);
                    assert_eq!(
                        create.unwrap_err().kind(),
                        io::ErrorKind::AlreadyExists,
                        "the invalid sentinel must reserve the checksum basename"
                    );
                }
                if phase == PublishPhase::BeforeChecksumCommit {
                    observed = true;
                    assert_eq!(fs::read(&destination).unwrap(), b"new archive");
                }
                Ok(())
            })
            .unwrap();
        assert!(observed);
        assert_eq!(fs::read(&result.archive_path).unwrap(), b"new archive");
        assert_eq!(
            fs::read_to_string(&result.checksum_path).unwrap(),
            archive_checksum_line(&evidence.sha256, destination.file_name().unwrap()).unwrap()
        );
        let destination_gid = fs::metadata(destination.parent().unwrap()).unwrap().gid();
        for path in [&result.archive_path, &result.checksum_path] {
            let metadata = fs::metadata(path).unwrap();
            assert_eq!(metadata.uid(), effective_user_id());
            assert_eq!(metadata.gid(), destination_gid);
            assert_eq!(metadata.permissions().mode() & 0o777, FINAL_FILE_MODE);
        }
        assert!(result.recovery_directory.is_none());
    }

    #[test]
    fn valid_segment_manifest_is_copied_and_committed_with_archive() {
        let (_root, archive, destination, evidence) = fixture_with_manifest();
        let source_manifest = segment_manifest_path(&archive).unwrap();
        let expected_manifest = fs::read(&source_manifest).unwrap();

        let result = publish_verified_archive(&archive, &destination, evidence).unwrap();

        let published_manifest = result.manifest_path.unwrap();
        assert_eq!(fs::read(&published_manifest).unwrap(), expected_manifest);
        assert_eq!(
            fs::metadata(&published_manifest)
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            FINAL_FILE_MODE
        );
        assert!(source_manifest.exists());
    }

    #[test]
    fn restart_refuses_to_overwrite_sentinel_after_new_manifest_install() {
        let (_root, archive, destination, evidence) = fixture_with_manifest();
        let expected_new_manifest = fs::read(segment_manifest_path(&archive).unwrap()).unwrap();
        write_existing_set(&destination);
        let old_archive = crate::archive_checksum::open_regular_nofollow(&destination).unwrap();
        let old_evidence = crate::archive_checksum::measure_open_archive(&old_archive).unwrap();

        let crashed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = publish_verified_archive_with_hook(&archive, &destination, evidence, |phase| {
                if phase == PublishPhase::AfterManifestMutation {
                    panic!("simulated process crash after manifest commit");
                }
                Ok(())
            });
        }));
        assert!(crashed.is_err());
        assert_eq!(fs::read(&destination).unwrap(), b"old archive");
        assert_eq!(
            fs::read(segment_manifest_path(&destination).unwrap()).unwrap(),
            expected_new_manifest
        );
        let checksum = archive_checksum_path(&destination).unwrap();
        let sentinel_inode = fs::metadata(&checksum).unwrap().ino();
        assert_eq!(fs::read(&checksum).unwrap(), ARCHIVE_PUBLICATION_SENTINEL);

        let error = crate::archive_checksum::ensure_archive_checksum_for_validated(
            &destination,
            old_evidence,
        )
        .unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("manual transaction recovery"));
        assert_eq!(fs::metadata(&checksum).unwrap().ino(), sentinel_inode);
        assert_eq!(fs::read(&checksum).unwrap(), ARCHIVE_PUBLICATION_SENTINEL);
        let retry = publish_verified_archive(&archive, &destination, evidence).unwrap_err();
        assert!(!retry.committed());
        assert!(retry.to_string().contains("manual transaction recovery"));
        assert_eq!(fs::metadata(&checksum).unwrap().ino(), sentinel_inode);
        assert_eq!(fs::read(checksum).unwrap(), ARCHIVE_PUBLICATION_SENTINEL);
    }

    #[test]
    fn restart_refuses_to_overwrite_sentinel_after_manifest_removal() {
        let (_root, archive, destination, evidence) = fixture();
        write_existing_set(&destination);
        let old_archive = crate::archive_checksum::open_regular_nofollow(&destination).unwrap();
        let old_evidence = crate::archive_checksum::measure_open_archive(&old_archive).unwrap();

        let crashed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = publish_verified_archive_with_hook(&archive, &destination, evidence, |phase| {
                if phase == PublishPhase::AfterManifestMutation {
                    panic!("simulated process crash after manifest removal");
                }
                Ok(())
            });
        }));
        assert!(crashed.is_err());
        assert_eq!(fs::read(&destination).unwrap(), b"old archive");
        assert!(!segment_manifest_path(&destination).unwrap().exists());
        let checksum = archive_checksum_path(&destination).unwrap();
        let sentinel_inode = fs::metadata(&checksum).unwrap().ino();
        assert_eq!(fs::read(&checksum).unwrap(), ARCHIVE_PUBLICATION_SENTINEL);

        let error = crate::archive_checksum::ensure_archive_checksum_for_validated(
            &destination,
            old_evidence,
        )
        .unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("manual transaction recovery"));
        assert_eq!(fs::metadata(&checksum).unwrap().ino(), sentinel_inode);
        assert_eq!(fs::read(&checksum).unwrap(), ARCHIVE_PUBLICATION_SENTINEL);
        let retry = publish_verified_archive(&archive, &destination, evidence).unwrap_err();
        assert!(!retry.committed());
        assert!(retry.to_string().contains("manual transaction recovery"));
        assert_eq!(fs::metadata(&checksum).unwrap().ino(), sentinel_inode);
        assert_eq!(fs::read(checksum).unwrap(), ARCHIVE_PUBLICATION_SENTINEL);
    }

    #[test]
    fn every_precommit_phase_rolls_back_exact_existing_inodes() {
        for failed_phase in [
            PublishPhase::BeforeChecksumInvalidation,
            PublishPhase::AfterChecksumInvalidationMutation,
            PublishPhase::AfterChecksumInvalidation,
            PublishPhase::BeforeManifestMutation,
            PublishPhase::AfterManifestBackupMutation,
            PublishPhase::AfterManifestMutation,
            PublishPhase::BeforeArchiveInstall,
            PublishPhase::AfterArchiveInstallMutation,
            PublishPhase::AfterArchiveBackupMutation,
            PublishPhase::AfterArchiveInstall,
            PublishPhase::BeforeChecksumCommit,
            PublishPhase::AfterChecksumCommitMutation,
        ] {
            let (_root, archive, destination, evidence) = fixture();
            let old_inodes = write_existing_set(&destination);
            let error =
                publish_verified_archive_with_hook(&archive, &destination, evidence, |phase| {
                    if phase == failed_phase {
                        Err(format!("injected failure at {phase:?}"))
                    } else {
                        Ok(())
                    }
                })
                .unwrap_err();
            assert!(!error.committed(), "{failed_phase:?}: {error}");
            assert!(
                error.recovery_directory().is_none(),
                "{failed_phase:?}: {error}"
            );
            assert_eq!(fs::read(&destination).unwrap(), b"old archive");
            assert_eq!(
                fs::read(segment_manifest_path(&destination).unwrap()).unwrap(),
                b"old manifest"
            );
            assert_eq!(
                fs::read(archive_checksum_path(&destination).unwrap()).unwrap(),
                b"old checksum"
            );
            assert_eq!(
                [
                    fs::metadata(&destination).unwrap().ino(),
                    fs::metadata(segment_manifest_path(&destination).unwrap())
                        .unwrap()
                        .ino(),
                    fs::metadata(archive_checksum_path(&destination).unwrap())
                        .unwrap()
                        .ino(),
                ],
                old_inodes,
                "{failed_phase:?}"
            );
            assert_eq!(fs::read(&archive).unwrap(), b"new archive");
        }
    }

    #[test]
    fn symlink_destination_is_rejected_without_touching_victim() {
        use std::os::unix::fs::symlink;

        let (root, archive, destination, evidence) = fixture();
        let victim = root.path().join("victim");
        fs::write(&victim, b"victim").unwrap();
        symlink(&victim, &destination).unwrap();
        let error = publish_verified_archive(&archive, &destination, evidence).unwrap_err();
        assert!(!error.committed());
        assert_eq!(fs::read(victim).unwrap(), b"victim");
        assert!(archive.exists());
    }

    #[test]
    fn symlink_staged_manifest_is_rejected_without_reading_through_it() {
        use std::os::unix::fs::symlink;

        let (root, archive, destination, evidence) = fixture_with_manifest();
        let manifest = segment_manifest_path(&archive).unwrap();
        let victim = root.path().join("manifest-victim");
        fs::write(&victim, b"not a manifest").unwrap();
        fs::remove_file(&manifest).unwrap();
        symlink(&victim, &manifest).unwrap();

        let error = publish_verified_archive(&archive, &destination, evidence).unwrap_err();

        assert!(!error.committed());
        assert_eq!(fs::read(victim).unwrap(), b"not a manifest");
        assert!(archive.exists());
        assert!(!destination.exists());
    }

    #[test]
    fn replacement_race_is_detected_without_publishing_checksum() {
        let (root, archive, destination, evidence) = fixture();
        write_existing_set(&destination);
        let displaced = root.path().join("displaced-old-archive");
        let error = publish_verified_archive_with_hook(&archive, &destination, evidence, |phase| {
            if phase == PublishPhase::BeforeArchiveInstall {
                fs::rename(&destination, &displaced).unwrap();
                fs::write(&destination, b"concurrent replacement").unwrap();
                fs::set_permissions(&destination, fs::Permissions::from_mode(0o440)).unwrap();
            }
            Ok(())
        })
        .unwrap_err();
        assert!(!error.committed());
        assert_eq!(fs::read(&destination).unwrap(), b"concurrent replacement");
        assert_eq!(
            fs::read(archive_checksum_path(&destination).unwrap()).unwrap(),
            ARCHIVE_PUBLICATION_SENTINEL
        );
        assert_eq!(fs::read(&displaced).unwrap(), b"old archive");
        assert!(archive.exists());
        assert!(error.recovery_directory().is_some());
    }

    #[test]
    fn successful_replacement_retains_exact_old_files() {
        let (_root, archive, destination, evidence) = fixture();
        let old_inodes = write_existing_set(&destination);
        let result = publish_verified_archive(&archive, &destination, evidence).unwrap();
        let recovery = result.recovery_directory.unwrap();
        assert_eq!(
            fs::metadata(recovery.join(ComponentKind::Archive.backup_name()))
                .unwrap()
                .ino(),
            old_inodes[0]
        );
        assert_eq!(
            fs::metadata(recovery.join(ComponentKind::Manifest.backup_name()))
                .unwrap()
                .ino(),
            old_inodes[1]
        );
        assert_eq!(
            fs::metadata(recovery.join(ComponentKind::Checksum.backup_name()))
                .unwrap()
                .ino(),
            old_inodes[2]
        );
    }

    #[test]
    fn stale_destination_manifest_is_removed_when_source_has_none() {
        let (_root, archive, destination, evidence) = fixture();
        write_existing_set(&destination);
        let result = publish_verified_archive(&archive, &destination, evidence).unwrap();
        assert!(result.manifest_path.is_none());
        assert!(!segment_manifest_path(&destination).unwrap().exists());
        assert!(
            result
                .recovery_directory
                .unwrap()
                .join(ComponentKind::Manifest.backup_name())
                .exists()
        );
    }

    #[test]
    fn unsafe_existing_mode_is_rejected_before_mutation() {
        let (_root, archive, destination, evidence) = fixture();
        fs::write(&destination, b"group writable").unwrap();
        fs::set_permissions(&destination, fs::Permissions::from_mode(0o660)).unwrap();
        let error = publish_verified_archive(&archive, &destination, evidence).unwrap_err();
        assert!(!error.committed());
        assert_eq!(fs::read(destination).unwrap(), b"group writable");
        assert!(archive.exists());
    }

    #[test]
    fn provisional_manifest_journal_rolls_back_each_namespace_mutation() {
        for failed_phase in [
            PublishPhase::AfterManifestInstallMutation,
            PublishPhase::AfterManifestBackupMutation,
        ] {
            let (_root, archive, destination, evidence) = fixture_with_manifest();
            let source_manifest = segment_manifest_path(&archive).unwrap();
            let old_inodes = write_existing_set(&destination);
            let error =
                publish_verified_archive_with_hook(&archive, &destination, evidence, |phase| {
                    if phase == failed_phase {
                        Err(format!("injected failure at {phase:?}"))
                    } else {
                        Ok(())
                    }
                })
                .unwrap_err();

            assert!(!error.committed(), "{failed_phase:?}: {error}");
            assert!(error.recovery_directory().is_none(), "{failed_phase:?}");
            assert_eq!(
                [
                    fs::metadata(&destination).unwrap().ino(),
                    fs::metadata(segment_manifest_path(&destination).unwrap())
                        .unwrap()
                        .ino(),
                    fs::metadata(archive_checksum_path(&destination).unwrap())
                        .unwrap()
                        .ino(),
                ],
                old_inodes,
                "{failed_phase:?}"
            );
            assert!(archive.exists());
            assert!(source_manifest.exists());
        }
    }

    #[test]
    fn provisional_new_install_journal_restores_absence() {
        for failed_phase in [
            PublishPhase::AfterChecksumInvalidationMutation,
            PublishPhase::AfterArchiveInstallMutation,
            PublishPhase::AfterChecksumCommitMutation,
        ] {
            let (_root, archive, destination, evidence) = fixture();
            let error =
                publish_verified_archive_with_hook(&archive, &destination, evidence, |phase| {
                    if phase == failed_phase {
                        Err(format!("injected failure at {phase:?}"))
                    } else {
                        Ok(())
                    }
                })
                .unwrap_err();

            assert!(!error.committed(), "{failed_phase:?}: {error}");
            assert!(error.recovery_directory().is_none(), "{failed_phase:?}");
            assert!(!destination.exists(), "{failed_phase:?}");
            assert!(
                !archive_checksum_path(&destination).unwrap().exists(),
                "{failed_phase:?}"
            );
            assert!(archive.exists(), "{failed_phase:?}");
        }
    }

    #[test]
    fn final_precommit_race_cannot_publish_a_canonical_checksum() {
        let (root, archive, destination, evidence) = fixture();
        let displaced = root.path().join("displaced-validated-archive");
        let error = publish_verified_archive_with_hook(&archive, &destination, evidence, |phase| {
            if phase == PublishPhase::BeforeChecksumCommit {
                fs::rename(&destination, &displaced).unwrap();
                fs::write(&destination, b"same uid replacement").unwrap();
                fs::set_permissions(&destination, fs::Permissions::from_mode(FINAL_FILE_MODE))
                    .unwrap();
            }
            Ok(())
        })
        .unwrap_err();

        assert!(!error.committed(), "{error}");
        assert!(error.recovery_directory().is_some());
        assert_eq!(fs::read(&destination).unwrap(), b"same uid replacement");
        assert_eq!(fs::read(&displaced).unwrap(), b"new archive");
        assert_eq!(
            fs::read(archive_checksum_path(&destination).unwrap()).unwrap(),
            ARCHIVE_PUBLICATION_SENTINEL
        );
    }

    #[test]
    fn postcommit_race_is_atomically_invalidated_before_rollback() {
        let (root, archive, destination, evidence) = fixture();
        let displaced = root.path().join("displaced-after-checksum-exchange");
        let error = publish_verified_archive_with_hook(&archive, &destination, evidence, |phase| {
            if phase == PublishPhase::AfterChecksumCommitMutation {
                fs::rename(&destination, &displaced).unwrap();
                fs::write(&destination, b"postcommit same uid replacement").unwrap();
                fs::set_permissions(&destination, fs::Permissions::from_mode(FINAL_FILE_MODE))
                    .unwrap();
            }
            Ok(())
        })
        .unwrap_err();

        assert!(!error.committed(), "{error}");
        assert!(error.recovery_directory().is_some());
        assert_eq!(
            fs::read(archive_checksum_path(&destination).unwrap()).unwrap(),
            ARCHIVE_PUBLICATION_SENTINEL
        );
        assert_eq!(fs::read(&displaced).unwrap(), b"new archive");
    }

    #[test]
    fn dependent_forward_mutation_sync_failures_roll_back_exactly() {
        for failed_barrier in [
            PublishPhase::AfterArchiveInstallMutation,
            PublishPhase::AfterArchiveBackupMutation,
        ] {
            let (_root, archive, destination, evidence) = fixture();
            let old_inodes = write_existing_set(&destination);
            let mut saw_backup_mutation = false;
            let error =
                publish_verified_archive_with_hook(&archive, &destination, evidence, |phase| {
                    if phase == PublishPhase::AfterArchiveBackupMutation {
                        saw_backup_mutation = true;
                    }
                    if phase == failed_barrier {
                        SYNC_FAULT.with(|fault| fault.set(Some(libc::EIO)));
                    }
                    Ok(())
                })
                .unwrap_err();

            assert_eq!(
                saw_backup_mutation,
                failed_barrier == PublishPhase::AfterArchiveBackupMutation,
                "the backup rename must not follow a failed exchange barrier"
            );
            assert!(!error.committed(), "{failed_barrier:?}: {error}");
            assert!(
                error.recovery_directory().is_none(),
                "{failed_barrier:?}: {error}"
            );
            assert_eq!(fs::read(&destination).unwrap(), b"old archive");
            assert_eq!(fs::metadata(&destination).unwrap().ino(), old_inodes[0]);
            assert_eq!(fs::read(&archive).unwrap(), b"new archive");
            assert_eq!(
                fs::metadata(archive_checksum_path(&destination).unwrap())
                    .unwrap()
                    .ino(),
                old_inodes[2]
            );
        }
    }

    #[test]
    fn failed_rollback_mutation_sync_stops_before_dependent_exchange() {
        let (_root, archive, destination, evidence) = fixture();
        write_existing_set(&destination);
        let mut injected = false;
        let error =
            publish_verified_archive_with_hook(
                &archive,
                &destination,
                evidence,
                |phase| match phase {
                    PublishPhase::AfterArchiveInstall => Err("start rollback".to_string()),
                    PublishPhase::BeforeRollbackMutationSync if !injected => {
                        injected = true;
                        SYNC_FAULT.with(|fault| fault.set(Some(libc::EIO)));
                        Ok(())
                    }
                    _ => Ok(()),
                },
            )
            .unwrap_err();

        assert!(injected);
        assert!(!error.committed());
        assert!(error.recovery_directory().is_some());
        assert_eq!(fs::read(&destination).unwrap(), b"new archive");
        assert_eq!(fs::read(&archive).unwrap(), b"old archive");
        assert_eq!(
            fs::read(archive_checksum_path(&destination).unwrap()).unwrap(),
            ARCHIVE_PUBLICATION_SENTINEL
        );
    }

    #[test]
    fn failed_data_durability_barrier_leaves_checksum_invalid() {
        let (_root, archive, destination, evidence) = fixture();
        let old_inodes = write_existing_set(&destination);
        let error =
            publish_verified_archive_with_hook(
                &archive,
                &destination,
                evidence,
                |phase| match phase {
                    PublishPhase::AfterArchiveInstall => Err("start rollback".to_string()),
                    PublishPhase::BeforeRollbackDataSync => {
                        SYNC_ATTEMPTS.with(|attempts| attempts.set(0));
                        SYNC_FAULT.with(|fault| fault.set(Some(libc::EIO)));
                        Ok(())
                    }
                    _ => Ok(()),
                },
            )
            .unwrap_err();

        assert!(!error.committed());
        let recovery = error.recovery_directory().unwrap();
        assert_eq!(fs::read(&destination).unwrap(), b"old archive");
        assert_eq!(fs::metadata(&destination).unwrap().ino(), old_inodes[0]);
        assert_eq!(
            fs::read(archive_checksum_path(&destination).unwrap()).unwrap(),
            ARCHIVE_PUBLICATION_SENTINEL
        );
        assert_eq!(
            fs::metadata(recovery.join(ComponentKind::Checksum.backup_name()))
                .unwrap()
                .ino(),
            old_inodes[2]
        );
        assert_eq!(
            SYNC_ATTEMPTS.with(std::cell::Cell::get),
            3,
            "all data directories must be attempted even after one sync fails"
        );
    }

    #[test]
    fn checksum_restore_attempts_both_directory_syncs_after_an_error() {
        let (_root, archive, destination, evidence) = fixture();
        let old_inodes = write_existing_set(&destination);
        let mut observed_attempts = None;
        let error =
            publish_verified_archive_with_hook(
                &archive,
                &destination,
                evidence,
                |phase| match phase {
                    PublishPhase::AfterArchiveInstall => Err("start rollback".to_string()),
                    PublishPhase::AfterChecksumRestoreMutation => {
                        SYNC_ATTEMPTS.with(|attempts| attempts.set(0));
                        SYNC_FAULT.with(|fault| fault.set(Some(libc::EIO)));
                        Ok(())
                    }
                    PublishPhase::AfterChecksumRollbackSync => {
                        observed_attempts = Some(SYNC_ATTEMPTS.with(std::cell::Cell::get));
                        Ok(())
                    }
                    _ => Ok(()),
                },
            )
            .unwrap_err();

        assert_eq!(observed_attempts, Some(2));
        assert!(!error.committed());
        assert!(error.recovery_directory().is_some());
        assert_eq!(
            fs::metadata(archive_checksum_path(&destination).unwrap())
                .unwrap()
                .ino(),
            old_inodes[2]
        );
    }

    #[test]
    fn checksum_restore_is_synced_before_cleanup_failure() {
        let (_root, archive, destination, evidence) = fixture();
        let old_inodes = write_existing_set(&destination);
        let parent = destination.parent().unwrap().to_path_buf();
        let mut saw_checksum_sync = false;
        let error =
            publish_verified_archive_with_hook(
                &archive,
                &destination,
                evidence,
                |phase| match phase {
                    PublishPhase::AfterArchiveInstall => Err("start rollback".to_string()),
                    PublishPhase::AfterChecksumRollbackSync => {
                        saw_checksum_sync = true;
                        let recovery = recovery_directories(&parent);
                        assert_eq!(recovery.len(), 1);
                        fs::write(recovery[0].join("unexpected-entry"), b"retain").unwrap();
                        Ok(())
                    }
                    _ => Ok(()),
                },
            )
            .unwrap_err();

        assert!(saw_checksum_sync);
        assert!(!error.committed());
        assert!(error.recovery_directory().is_some());
        assert_eq!(
            [
                fs::metadata(&destination).unwrap().ino(),
                fs::metadata(segment_manifest_path(&destination).unwrap())
                    .unwrap()
                    .ino(),
                fs::metadata(archive_checksum_path(&destination).unwrap())
                    .unwrap()
                    .ino(),
            ],
            old_inodes
        );
    }

    #[test]
    fn cleanup_refuses_a_swapped_recovery_entry() {
        let (root, archive, destination, evidence) = fixture();
        write_existing_set(&destination);
        let parent = destination.parent().unwrap().to_path_buf();
        let moved_recovery = root.path().join("moved-bound-recovery");
        let mut lookalike = None;
        let error =
            publish_verified_archive_with_hook(
                &archive,
                &destination,
                evidence,
                |phase| match phase {
                    PublishPhase::AfterArchiveInstall => Err("start rollback".to_string()),
                    PublishPhase::AfterChecksumRollbackSync => {
                        let recovery = recovery_directories(&parent);
                        assert_eq!(recovery.len(), 1);
                        fs::rename(&recovery[0], &moved_recovery).unwrap();
                        fs::create_dir(&recovery[0]).unwrap();
                        fs::set_permissions(&recovery[0], fs::Permissions::from_mode(0o700))
                            .unwrap();
                        fs::write(recovery[0].join("victim"), b"must survive").unwrap();
                        lookalike = Some(recovery[0].clone());
                        Ok(())
                    }
                    _ => Ok(()),
                },
            )
            .unwrap_err();

        assert!(!error.committed());
        assert!(error.to_string().contains("bound inode"), "{error}");
        let lookalike = lookalike.unwrap();
        assert_eq!(fs::read(lookalike.join("victim")).unwrap(), b"must survive");
        assert!(moved_recovery.exists());
    }

    #[test]
    fn rollback_barriers_run_in_checksum_safe_order() {
        let (_root, archive, destination, evidence) = fixture();
        write_existing_set(&destination);
        let mut trace = Vec::new();
        publish_verified_archive_with_hook(&archive, &destination, evidence, |phase| match phase {
            PublishPhase::AfterArchiveInstall => Err("start rollback".to_string()),
            PublishPhase::BeforeRollbackDataSync
            | PublishPhase::AfterRollbackDataSync
            | PublishPhase::AfterChecksumRestoreMutation
            | PublishPhase::AfterChecksumRollbackSync => {
                trace.push(phase);
                Ok(())
            }
            _ => Ok(()),
        })
        .unwrap_err();
        assert_eq!(
            trace,
            [
                PublishPhase::BeforeRollbackDataSync,
                PublishPhase::AfterRollbackDataSync,
                PublishPhase::AfterChecksumRestoreMutation,
                PublishPhase::AfterChecksumRollbackSync,
            ]
        );
    }

    #[test]
    fn publication_preflight_enforces_destination_modes() {
        for (mode, accepted) in [
            (0o700, true),
            (0o750, true),
            (0o755, true),
            (0o770, false),
            (0o2770, false),
            (0o3770, true),
        ] {
            let root = tempfile::TempDir::new().unwrap();
            let source = root.path().join("source");
            let destination = root.path().join("destination");
            fs::create_dir(&source).unwrap();
            fs::create_dir(&destination).unwrap();
            fs::set_permissions(&source, fs::Permissions::from_mode(0o700)).unwrap();
            fs::set_permissions(&destination, fs::Permissions::from_mode(mode)).unwrap();

            let result = preflight_archive_publication(&source, &destination);
            assert_eq!(result.is_ok(), accepted, "mode {mode:04o}: {result:?}");
            assert!(
                recovery_directories(&destination).is_empty(),
                "mode {mode:04o}"
            );
        }
    }

    #[test]
    fn publication_preflight_reports_renameat2_capability_failures() {
        for (flags, errno) in [
            (libc::RENAME_NOREPLACE, libc::ENOSYS),
            (libc::RENAME_EXCHANGE, libc::EOPNOTSUPP),
            (libc::RENAME_NOREPLACE, libc::EXDEV),
        ] {
            let root = tempfile::TempDir::new().unwrap();
            let source = root.path().join("source");
            let destination = root.path().join("destination");
            fs::create_dir(&source).unwrap();
            fs::create_dir(&destination).unwrap();
            fs::set_permissions(&source, fs::Permissions::from_mode(0o700)).unwrap();
            fs::set_permissions(&destination, fs::Permissions::from_mode(0o3770)).unwrap();

            let fault = RenameFaultGuard::install(flags, errno);
            let error = preflight_archive_publication(&source, &destination).unwrap_err();
            drop(fault);

            assert!(!error.committed());
            assert!(error.recovery_directory().is_none(), "{error}");
            assert!(
                error.to_string().contains("RENAME_"),
                "errno {errno}: {error}"
            );
            assert!(recovery_directories(&destination).is_empty());
            assert!(
                fs::read_dir(&source).unwrap().next().is_none(),
                "source probe was not cleaned for errno {errno}"
            );
        }
    }

    #[test]
    fn publication_preflight_rejects_missing_destination_group_capability() {
        let root = tempfile::TempDir::new().unwrap();
        let source = root.path().join("source");
        let destination = root.path().join("destination");
        fs::create_dir(&source).unwrap();
        fs::create_dir(&destination).unwrap();
        fs::set_permissions(&source, fs::Permissions::from_mode(0o700)).unwrap();
        fs::set_permissions(&destination, fs::Permissions::from_mode(0o3770)).unwrap();

        let fault = ChownFaultGuard::install(libc::EPERM);
        let error = preflight_archive_publication(&source, &destination).unwrap_err();
        drop(fault);

        assert!(!error.committed());
        assert!(error.recovery_directory().is_none(), "{error}");
        assert!(
            error.to_string().contains("assign destination group"),
            "{error}"
        );
        assert!(recovery_directories(&destination).is_empty());
        assert!(fs::read_dir(&source).unwrap().next().is_none());
    }
}
