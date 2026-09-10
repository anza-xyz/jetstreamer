//! Transactional publication of a fully validated Horizon archive.
//!
//! A canonical checksum sidecar is the commit marker. Publication atomically
//! replaces any previous checksum with an invalid, publisher-owned sentinel,
//! installs the optional segment manifest and archive, checks their exact
//! inodes again, and exchanges the canonical checksum into place last.
//! Existing files are retained in a private recovery directory.
//!
//! The mutation journal is process-local. A crash can therefore leave an
//! invalid checksum sentinel and an identity-bound recovery directory. Those
//! leftovers require manual reconciliation; startup never guesses at or
//! recursively removes an interrupted transaction.

use {
    crate::{
        archive_checksum::{
            ValidatedArchiveFile, archive_checksum_line, archive_checksum_path,
            archive_file_identity, path_matches_archive_identity, rebind_validated_after_rename,
        },
        segment_manifest::{
            HistoricalSegmentManifest, read_and_validate_segment_manifest, segment_manifest_path,
        },
    },
    std::{
        ffi::{CString, OsStr},
        fmt,
        fs::{self, File, OpenOptions},
        io::{self, Read, Write},
        mem::MaybeUninit,
        os::{
            fd::{AsRawFd as _, FromRawFd as _, RawFd},
            unix::{
                ffi::OsStrExt as _,
                fs::{MetadataExt as _, OpenOptionsExt as _, PermissionsExt as _},
            },
        },
        path::{Path, PathBuf},
        time::{SystemTime, UNIX_EPOCH},
    },
};

const MAX_SEGMENT_MANIFEST_BYTES: u64 = 1 << 20;
const FINAL_FILE_MODE: u32 = 0o440;
const INVALID_CHECKSUM_CONTENTS: &[u8] = b"jetstreamer publication in progress\n";
const CAPABILITY_PROBE_A: &str = "rename-probe-a";
const CAPABILITY_PROBE_B: &str = "rename-probe-b";
const CAPABILITY_PROBE_MOVED: &str = "rename-probe-moved";

#[derive(Debug)]
pub struct ArchivePublication {
    pub archive_path: PathBuf,
    pub manifest_path: Option<PathBuf>,
    pub checksum_path: PathBuf,
    pub recovery_directory: Option<PathBuf>,
    pub evidence: ValidatedArchiveFile,
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
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

#[derive(Clone, Copy)]
struct InitialDestination {
    archive: Option<FileIdentity>,
    manifest: Option<FileIdentity>,
    checksum: Option<FileIdentity>,
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
    let source_parent_path = staged_archive.parent().unwrap_or_else(|| Path::new("."));
    let destination_parent_path = destination_archive
        .parent()
        .unwrap_or_else(|| Path::new("."));
    let source = BoundDirectory::bind(source_parent_path, DirectoryPolicy::PrivateSource)
        .map_err(preflight_error)?;
    let destination =
        BoundDirectory::bind(destination_parent_path, DirectoryPolicy::SharedDestination)
            .map_err(preflight_error)?;
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
        INVALID_CHECKSUM_CONTENTS,
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
    let mut transaction = PublicationTransaction {
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
    };

    if let Err(message) = transaction.run_precommit(hook) {
        return Err(transaction.rollback_error(message, hook));
    }
    transaction.commit(hook)
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
                    assert_eq!(fs::read(&checksum).unwrap(), INVALID_CHECKSUM_CONTENTS);
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
            INVALID_CHECKSUM_CONTENTS
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
            INVALID_CHECKSUM_CONTENTS
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
            INVALID_CHECKSUM_CONTENTS
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
            INVALID_CHECKSUM_CONTENTS
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
            INVALID_CHECKSUM_CONTENTS
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
