//! Transactional publication of a fully validated Horizon archive.
//!
//! A checksum sidecar is the commit marker. Publication removes any previous
//! checksum first, installs the optional segment manifest and archive, checks
//! their exact inodes again, and installs the new checksum last. Existing
//! files are retained in a private recovery directory.

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
        Ok(Self {
            path: path.to_path_buf(),
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
        self.file
            .sync_all()
            .map_err(|error| format!("failed to sync directory {}: {error}", self.path.display()))
    }
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
}

#[derive(Clone, Copy)]
struct InitialDestination {
    archive: Option<FileIdentity>,
    manifest: Option<FileIdentity>,
    checksum: Option<FileIdentity>,
}

enum Mutation {
    Removed {
        kind: ComponentKind,
        destination_name: std::ffi::OsString,
        old_identity: FileIdentity,
    },
    InstalledNew {
        kind: ComponentKind,
        source_directory: DirectoryIndex,
        source_name: std::ffi::OsString,
        destination_name: std::ffi::OsString,
        new_identity: FileIdentity,
    },
    InstalledReplacing {
        kind: ComponentKind,
        source_directory: DirectoryIndex,
        source_name: std::ffi::OsString,
        destination_name: std::ffi::OsString,
        new_identity: FileIdentity,
        old_identity: FileIdentity,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PublishPhase {
    BeforeChecksumInvalidation,
    AfterChecksumInvalidation,
    BeforeManifestMutation,
    AfterManifestMutation,
    BeforeArchiveInstall,
    AfterArchiveInstall,
    BeforeChecksumCommit,
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
    if source.identity.dev != destination.identity.dev {
        return Err(preflight_error(format!(
            "staging {} and destination {} are on different filesystems",
            source.path.display(),
            destination.path.display()
        )));
    }
    if source.identity.dev == destination.identity.dev
        && source.identity.ino == destination.identity.ino
    {
        return Err(preflight_error(
            "staging and destination directories must differ".to_string(),
        ));
    }

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
                &source_manifest_path,
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
    let (recovery_name, recovery) =
        create_recovery_directory(&destination, &destination_name).map_err(preflight_error)?;
    let manifest_staged = match manifest_bytes {
        Some(bytes) => Some(
            create_recovery_staging_file(
                &recovery,
                ComponentKind::Manifest,
                &bytes,
                destination.identity.gid,
            )
            .map_err(|message| cleanup_preflight_error(message, &destination, &recovery_name))?,
        ),
        None => None,
    };
    let checksum_staged = create_recovery_staging_file(
        &recovery,
        ComponentKind::Checksum,
        expected_checksum.as_bytes(),
        destination.identity.gid,
    )
    .map_err(|message| cleanup_preflight_error(message, &destination, &recovery_name))?;
    if let Err(message) = recovery.sync().and_then(|()| destination.sync()) {
        return Err(cleanup_preflight_error(
            message,
            &destination,
            &recovery_name,
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
    let checksum_staging_identity = checksum_staged.identity;
    let mut transaction = PublicationTransaction {
        source,
        destination,
        recovery,
        recovery_name,
        initial,
        mutations: Vec::new(),
        _initial_handles: initial_handles,
        archive_staged,
        manifest_staged,
        checksum_staged,
        destination_name,
        destination_manifest_name,
        destination_checksum_name,
        evidence,
        published_evidence: None,
        manifest_staging_identity,
        checksum_staging_identity,
    };

    if let Err(message) = transaction.run_precommit(hook) {
        return Err(transaction.rollback_error(message));
    }
    transaction.commit()
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
    destination_name: std::ffi::OsString,
    destination_manifest_name: std::ffi::OsString,
    destination_checksum_name: std::ffi::OsString,
    evidence: ValidatedArchiveFile,
    published_evidence: Option<ValidatedArchiveFile>,
    manifest_staging_identity: Option<FileIdentity>,
    checksum_staging_identity: FileIdentity,
}

impl PublicationTransaction {
    fn directory(&self, index: DirectoryIndex) -> &BoundDirectory {
        match index {
            DirectoryIndex::Source => &self.source,
            DirectoryIndex::Recovery => &self.recovery,
        }
    }

    fn run_precommit(
        &mut self,
        hook: &mut dyn FnMut(PublishPhase) -> Result<(), String>,
    ) -> Result<(), String> {
        hook(PublishPhase::BeforeChecksumInvalidation)?;
        if let Some(identity) = self.initial.checksum {
            self.remove_destination_to_backup(
                ComponentKind::Checksum,
                self.destination_checksum_name.clone(),
                identity,
            )?;
        } else {
            require_expected_target(&self.destination, &self.destination_checksum_name, None)?;
        }
        self.sync_mutation_directories()?;
        hook(PublishPhase::AfterChecksumInvalidation)?;

        hook(PublishPhase::BeforeManifestMutation)?;
        if let Some(staged) = self.manifest_staged.take() {
            let destination_name = self.destination_manifest_name.clone();
            let expected = self.initial.manifest;
            self.install_component(staged, destination_name, expected)?;
        } else if let Some(identity) = self.initial.manifest {
            self.remove_destination_to_backup(
                ComponentKind::Manifest,
                self.destination_manifest_name.clone(),
                identity,
            )?;
        } else {
            require_expected_target(&self.destination, &self.destination_manifest_name, None)?;
        }
        self.sync_mutation_directories()?;
        hook(PublishPhase::AfterManifestMutation)?;

        hook(PublishPhase::BeforeArchiveInstall)?;
        let archive = take_staged_component(&mut self.archive_staged)?;
        self.install_component(archive, self.destination_name.clone(), self.initial.archive)?;
        self.sync_mutation_directories()?;
        hook(PublishPhase::AfterArchiveInstall)?;

        let archive_identity = self.installed_identity(ComponentKind::Archive)?;
        require_expected_target(
            &self.destination,
            &self.destination_name,
            Some(archive_identity),
        )?;
        let rebound = rebind_validated_after_rename(&self.archive_staged.file, self.evidence)
            .map_err(|error| {
                format!("published archive changed across controlled rename: {error}")
            })?;
        if !path_matches_archive_identity(
            &self.destination.path.join(&self.destination_name),
            rebound.identity,
        )
        .map_err(|error| format!("failed to bind published archive path: {error}"))?
        {
            return Err("published archive path does not name the validated inode".to_string());
        }
        self.published_evidence = Some(rebound);
        match self.installed_identity_optional(ComponentKind::Manifest) {
            Some(identity) => require_expected_target(
                &self.destination,
                &self.destination_manifest_name,
                Some(identity),
            )?,
            None => {
                require_expected_target(&self.destination, &self.destination_manifest_name, None)?
            }
        }
        require_expected_target(&self.destination, &self.destination_checksum_name, None)?;
        self.source.recheck_path()?;
        self.destination.recheck_path()?;
        self.recovery.recheck_path()?;
        hook(PublishPhase::BeforeChecksumCommit)?;
        Ok(())
    }

    fn commit(mut self) -> Result<ArchivePublication, ArchivePublicationError> {
        let checksum = match take_staged_component(&mut self.checksum_staged) {
            Ok(checksum) => checksum,
            Err(message) => return Err(self.rollback_error(message)),
        };
        if let Err(message) = self.verify_staged(&checksum) {
            return Err(self.rollback_error(message));
        }
        if let Err(message) =
            require_expected_target(&self.destination, &self.destination_checksum_name, None)
        {
            return Err(self.rollback_error(message));
        }
        let checksum_directory = self.directory(checksum.directory_index);
        if let Err(error) = rename_noreplace(
            checksum_directory,
            &checksum.name,
            &self.destination,
            &self.destination_checksum_name,
        ) {
            return Err(
                self.rollback_error(format!("failed to install checksum commit marker: {error}"))
            );
        }

        // A successful rename is the commit point. Any failure below reports
        // a committed transaction and leaves the checksum in place.
        let committed_checksum_identity = match rebound_target_identity(
            &self.destination,
            &self.destination_checksum_name,
            checksum.identity,
        ) {
            Ok(identity) => identity,
            Err(message) => {
                return Err(ArchivePublicationError {
                    message: format!(
                        "archive checksum rename committed, but its inode changed immediately afterward: {message}; recovery data remains in {}",
                        self.recovery.path.display()
                    ),
                    committed: true,
                    recovery_directory: Some(self.recovery.path.clone()),
                });
            }
        };
        let post_commit = (|| {
            require_expected_target(
                &self.destination,
                &self.destination_checksum_name,
                Some(committed_checksum_identity),
            )?;
            self.recovery.sync()?;
            self.destination.sync()?;
            let archive_identity = self.installed_identity(ComponentKind::Archive)?;
            require_expected_target(
                &self.destination,
                &self.destination_name,
                Some(archive_identity),
            )?;
            match self.installed_identity_optional(ComponentKind::Manifest) {
                Some(identity) => require_expected_target(
                    &self.destination,
                    &self.destination_manifest_name,
                    Some(identity),
                )?,
                None => require_expected_target(
                    &self.destination,
                    &self.destination_manifest_name,
                    None,
                )?,
            }
            Ok::<(), String>(())
        })();
        if let Err(message) = post_commit {
            return Err(ArchivePublicationError {
                message: format!(
                    "archive checksum was committed, but post-commit verification or sync failed: {message}; recovery data remains in {}",
                    self.recovery.path.display()
                ),
                committed: true,
                recovery_directory: Some(self.recovery.path.clone()),
            });
        }

        let evidence = self
            .published_evidence
            .expect("precommit bound archive evidence");

        let recovery_directory = if self.mutations.iter().any(|mutation| {
            matches!(
                mutation,
                Mutation::Removed { .. } | Mutation::InstalledReplacing { .. }
            )
        }) {
            Some(self.recovery.path.clone())
        } else {
            match remove_empty_recovery_directory(
                &self.destination,
                &self.recovery_name,
                &self.recovery,
            ) {
                Ok(()) => None,
                Err(message) => {
                    return Err(ArchivePublicationError {
                        message: format!(
                            "archive checksum was committed, but empty transaction cleanup failed: {message}"
                        ),
                        committed: true,
                        recovery_directory: Some(self.recovery.path.clone()),
                    });
                }
            }
        };

        Ok(ArchivePublication {
            archive_path: self.destination.path.join(&self.destination_name),
            manifest_path: self
                .installed_identity_optional(ComponentKind::Manifest)
                .map(|_| self.destination.path.join(&self.destination_manifest_name)),
            checksum_path: self.destination.path.join(&self.destination_checksum_name),
            recovery_directory,
            evidence,
        })
    }

    fn verify_staged(&self, staged: &StagedComponent) -> Result<(), String> {
        let directory = self.directory(staged.directory_index);
        require_expected_target(directory, &staged.name, Some(staged.identity))
    }

    fn install_component(
        &mut self,
        staged: StagedComponent,
        destination_name: std::ffi::OsString,
        expected_target: Option<FileIdentity>,
    ) -> Result<(), String> {
        self.verify_staged(&staged)?;
        require_expected_target(&self.destination, &destination_name, expected_target)?;
        let source_directory = self.directory(staged.directory_index);
        match expected_target {
            None => {
                rename_noreplace(
                    source_directory,
                    &staged.name,
                    &self.destination,
                    &destination_name,
                )
                .map_err(|error| {
                    format!(
                        "failed to install {} without replacing a concurrent target: {error}",
                        staged.kind.label()
                    )
                })?;
                let new_identity =
                    rebound_target_identity(&self.destination, &destination_name, staged.identity)?;
                require_expected_target(source_directory, &staged.name, None)?;
                self.mutations.push(Mutation::InstalledNew {
                    kind: staged.kind,
                    source_directory: staged.directory_index,
                    source_name: staged.name,
                    destination_name,
                    new_identity,
                });
            }
            Some(old_identity) => {
                rename_exchange(
                    source_directory,
                    &staged.name,
                    &self.destination,
                    &destination_name,
                )
                .map_err(|error| format!("failed to exchange {}: {error}", staged.kind.label()))?;
                let new_identity =
                    rebound_target_identity(&self.destination, &destination_name, staged.identity);
                let exchanged_old_identity =
                    rebound_target_identity(source_directory, &staged.name, old_identity);
                if new_identity.is_err() || exchanged_old_identity.is_err() {
                    if new_identity.is_ok() {
                        rename_exchange(
                            &self.destination,
                            &destination_name,
                            source_directory,
                            &staged.name,
                        )
                        .map_err(|error| {
                            format!(
                                "{} target changed during exchange and atomic restoration failed: {error}",
                                staged.kind.label()
                            )
                        })?;
                    }
                    return Err(format!(
                        "{} target changed concurrently during atomic exchange",
                        staged.kind.label()
                    ));
                }
                let new_identity = new_identity.expect("checked above");
                let exchanged_old_identity = exchanged_old_identity.expect("checked above");
                let backup_name = staged.kind.backup_name();
                if let Err(error) =
                    rename_noreplace(source_directory, &staged.name, &self.recovery, backup_name)
                {
                    rename_exchange(
                        &self.destination,
                        &destination_name,
                        source_directory,
                        &staged.name,
                    )
                    .map_err(|restore_error| {
                        format!(
                            "failed to retain replaced {}: {error}; atomic restoration also failed: {restore_error}",
                            staged.kind.label()
                        )
                    })?;
                    return Err(format!(
                        "failed to retain replaced {}: {error}",
                        staged.kind.label()
                    ));
                }
                let old_identity =
                    rebound_target_identity(&self.recovery, backup_name, exchanged_old_identity)?;
                self.mutations.push(Mutation::InstalledReplacing {
                    kind: staged.kind,
                    source_directory: staged.directory_index,
                    source_name: staged.name,
                    destination_name,
                    new_identity,
                    old_identity,
                });
            }
        }
        Ok(())
    }

    fn remove_destination_to_backup(
        &mut self,
        kind: ComponentKind,
        destination_name: std::ffi::OsString,
        expected_identity: FileIdentity,
    ) -> Result<(), String> {
        require_expected_target(
            &self.destination,
            &destination_name,
            Some(expected_identity),
        )?;
        let backup_name = kind.backup_name();
        rename_noreplace(
            &self.destination,
            &destination_name,
            &self.recovery,
            backup_name,
        )
        .map_err(|error| format!("failed to retain previous {}: {error}", kind.label()))?;
        let backup_identity =
            rebound_target_identity(&self.recovery, backup_name, expected_identity);
        if backup_identity.is_err() {
            if target_matches(&self.destination, &destination_name, None)? {
                rename_noreplace(
                    &self.recovery,
                    backup_name,
                    &self.destination,
                    &destination_name,
                )
                .map_err(|error| {
                    format!(
                        "{} changed during backup and restoration failed: {error}",
                        kind.label()
                    )
                })?;
            }
            return Err(format!(
                "{} changed concurrently while it was moved to recovery",
                kind.label()
            ));
        }
        self.mutations.push(Mutation::Removed {
            kind,
            destination_name,
            old_identity: backup_identity.expect("checked above"),
        });
        Ok(())
    }

    fn sync_mutation_directories(&self) -> Result<(), String> {
        self.source.sync()?;
        self.recovery.sync()?;
        self.destination.sync()
    }

    fn installed_identity(&self, kind: ComponentKind) -> Result<FileIdentity, String> {
        self.installed_identity_optional(kind)
            .ok_or_else(|| format!("{} was not installed", kind.label()))
    }

    fn installed_identity_optional(&self, kind: ComponentKind) -> Option<FileIdentity> {
        self.mutations
            .iter()
            .rev()
            .find_map(|mutation| match mutation {
                Mutation::InstalledNew {
                    kind: mutation_kind,
                    new_identity,
                    ..
                }
                | Mutation::InstalledReplacing {
                    kind: mutation_kind,
                    new_identity,
                    ..
                } if *mutation_kind == kind => Some(*new_identity),
                _ => None,
            })
    }

    fn rollback_error(mut self, cause: String) -> ArchivePublicationError {
        let rollback = self.rollback();
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
                    "archive publication failed before checksum commit: {cause}; rollback could not restore the exact initial namespace: {rollback_error}; recovery data remains in {}",
                    self.recovery.path.display()
                ),
                committed: false,
                recovery_directory: Some(self.recovery.path.clone()),
            },
        }
    }

    fn rollback(&mut self) -> Result<(), String> {
        let mut failures = Vec::new();
        let mut checksum_mutation = None;
        while let Some(mutation) = self.mutations.pop() {
            if matches!(
                mutation,
                Mutation::Removed {
                    kind: ComponentKind::Checksum,
                    ..
                }
            ) {
                checksum_mutation = Some(mutation);
                continue;
            }
            if let Err(error) = self.rollback_mutation(&mutation) {
                failures.push(error);
            }
        }

        let data_matches_initial = failures.is_empty()
            && target_corresponds(
                &self.destination,
                &self.destination_name,
                self.initial.archive,
            )
            .unwrap_or(false)
            && target_corresponds(
                &self.destination,
                &self.destination_manifest_name,
                self.initial.manifest,
            )
            .unwrap_or(false);
        if let Some(mutation) = checksum_mutation {
            if data_matches_initial {
                if let Err(error) = self.rollback_mutation(&mutation) {
                    failures.push(error);
                }
            } else {
                failures.push(
                    "previous checksum remains in recovery because destination data changed concurrently"
                        .to_string(),
                );
            }
        } else {
            match target_corresponds(
                &self.destination,
                &self.destination_checksum_name,
                self.initial.checksum,
            ) {
                Ok(true) => {}
                Ok(false) => failures
                    .push("checksum target changed concurrently during rollback".to_string()),
                Err(error) => failures.push(error),
            }
        }

        if failures.is_empty() {
            if let Some(identity) = self.manifest_staging_identity
                && let Err(error) = remove_corresponding_file(
                    &self.recovery,
                    ComponentKind::Manifest.staging_name(),
                    identity,
                )
            {
                failures.push(error);
            }
            if let Err(error) = remove_corresponding_file(
                &self.recovery,
                ComponentKind::Checksum.staging_name(),
                self.checksum_staging_identity,
            ) {
                failures.push(error);
            }
        }
        if failures.is_empty() {
            if let Err(error) = self.sync_mutation_directories() {
                failures.push(error);
            } else if let Err(error) = remove_empty_recovery_directory(
                &self.destination,
                &self.recovery_name,
                &self.recovery,
            ) {
                failures.push(error);
            }
        }
        if failures.is_empty() {
            Ok(())
        } else {
            Err(failures.join("; "))
        }
    }

    fn rollback_mutation(&self, mutation: &Mutation) -> Result<(), String> {
        match mutation {
            Mutation::Removed {
                kind,
                destination_name,
                old_identity,
            } => {
                require_expected_target(&self.destination, destination_name, None)?;
                require_expected_target(&self.recovery, kind.backup_name(), Some(*old_identity))?;
                rename_noreplace(
                    &self.recovery,
                    kind.backup_name(),
                    &self.destination,
                    destination_name,
                )
                .map_err(|error| format!("failed to restore previous {}: {error}", kind.label()))?;
                rebound_target_identity(&self.destination, destination_name, *old_identity)
                    .map(|_| ())
            }
            Mutation::InstalledNew {
                kind,
                source_directory,
                source_name,
                destination_name,
                new_identity,
            } => {
                let source = self.directory(*source_directory);
                require_expected_target(source, source_name, None)?;
                require_expected_target(&self.destination, destination_name, Some(*new_identity))?;
                rename_noreplace(&self.destination, destination_name, source, source_name)
                    .map_err(|error| {
                        format!("failed to return staged {}: {error}", kind.label())
                    })?;
                rebound_target_identity(source, source_name, *new_identity).map(|_| ())
            }
            Mutation::InstalledReplacing {
                kind,
                source_directory,
                source_name,
                destination_name,
                new_identity,
                old_identity,
            } => {
                let source = self.directory(*source_directory);
                require_expected_target(source, source_name, None)?;
                require_expected_target(&self.destination, destination_name, Some(*new_identity))?;
                require_expected_target(&self.recovery, kind.backup_name(), Some(*old_identity))?;
                rename_exchange(
                    &self.destination,
                    destination_name,
                    &self.recovery,
                    kind.backup_name(),
                )
                .map_err(|error| format!("failed to restore previous {}: {error}", kind.label()))?;
                rename_noreplace(&self.recovery, kind.backup_name(), source, source_name).map_err(
                    |error| format!("failed to return staged {}: {error}", kind.label()),
                )?;
                rebound_target_identity(&self.destination, destination_name, *old_identity)?;
                rebound_target_identity(source, source_name, *new_identity).map(|_| ())
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

fn cleanup_preflight_error(
    message: String,
    destination: &BoundDirectory,
    recovery_name: &OsStr,
) -> ArchivePublicationError {
    let cleanup = remove_recovery_directory_tree(destination, recovery_name);
    preflight_error(match cleanup {
        Ok(()) => message,
        Err(cleanup) => format!("{message}; failed to remove unused recovery directory: {cleanup}"),
    })
}

fn effective_user_id() -> u32 {
    // SAFETY: geteuid has no preconditions.
    unsafe { libc::geteuid() }
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
    directory.recheck_path()?;
    let full_path = directory.path.join(name);
    let path_metadata = match fs::symlink_metadata(&full_path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            match openat_readonly(directory.file.as_raw_fd(), name) {
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
            }
        }
        Err(error) => {
            return Err(format!(
                "failed to inspect {} {}: {error}",
                kind.label(),
                full_path.display()
            ));
        }
    };
    if !path_metadata.file_type().is_file() {
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
    let path_identity = FileIdentity::from_metadata(&path_metadata);
    let identity = FileIdentity::from_metadata(&descriptor_metadata);
    if path_identity != identity {
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
    manifest_path: &Path,
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
        || !path_matches_generic_identity(manifest_path, manifest.identity)?
    {
        return Err("archive or segment manifest changed during sidecar validation".to_string());
    }
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

fn path_matches_generic_identity(path: &Path, expected: FileIdentity) -> Result<bool, String> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(error) => {
            return Err(format!("failed to inspect {}: {error}", path.display()));
        }
    };
    Ok(metadata.file_type().is_file() && FileIdentity::from_metadata(&metadata) == expected)
}

fn create_recovery_directory(
    destination: &BoundDirectory,
    _archive_name: &OsStr,
) -> Result<(std::ffi::OsString, BoundDirectory), String> {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    for sequence in 0..100u32 {
        let name = std::ffi::OsString::from(format!(
            ".jetstreamer-recovery-{}-{timestamp}-{sequence}",
            std::process::id()
        ));
        let name_c = os_str_cstring(&name).map_err(|error| error.to_string())?;
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
            return Err(format!("failed to create recovery directory: {error}"));
        }
        let path = destination.path.join(&name);
        let recovery = BoundDirectory::bind(&path, DirectoryPolicy::PrivateRecovery)?;
        if recovery.identity.dev != destination.identity.dev {
            return Err("recovery directory is on a different filesystem".to_string());
        }
        destination.sync()?;
        return Ok((name, recovery));
    }
    Err("could not allocate a unique recovery directory".to_string())
}

fn create_recovery_staging_file(
    recovery: &BoundDirectory,
    kind: ComponentKind,
    contents: &[u8],
    destination_gid: u32,
) -> Result<StagedComponent, String> {
    let name = kind.staging_name().to_os_string();
    let path = recovery.path.join(&name);
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .mode(0o400)
        .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW)
        .open(&path)
        .map_err(|error| format!("failed to create staged {}: {error}", kind.label()))?;
    file.write_all(contents)
        .and_then(|()| file.flush())
        .map_err(|error| format!("failed to write staged {}: {error}", kind.label()))?;
    // SAFETY: `file` is a live regular-file descriptor. -1 preserves uid.
    if unsafe {
        libc::fchown(
            file.as_raw_fd(),
            !0 as libc::uid_t,
            destination_gid as libc::gid_t,
        )
    } != 0
    {
        return Err(format!(
            "failed to assign destination group to staged {}: {}",
            kind.label(),
            io::Error::last_os_error()
        ));
    }
    file.set_permissions(fs::Permissions::from_mode(FINAL_FILE_MODE))
        .and_then(|()| file.sync_all())
        .map_err(|error| format!("failed to sync staged {}: {error}", kind.label()))?;
    let identity = FileIdentity::from_metadata(
        &file
            .metadata()
            .map_err(|error| format!("failed to inspect staged {}: {error}", kind.label()))?,
    );
    require_safe_regular(identity, Some(destination_gid), kind, &path)?;
    Ok(StagedComponent {
        kind,
        directory_index: DirectoryIndex::Recovery,
        name,
        file,
        identity,
    })
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

fn target_matches(
    directory: &BoundDirectory,
    name: &OsStr,
    expected: Option<FileIdentity>,
) -> Result<bool, String> {
    Ok(target_identity(directory, name)? == expected)
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
    directory.recheck_path()?;
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

fn remove_corresponding_file(
    directory: &BoundDirectory,
    name: &OsStr,
    expected: FileIdentity,
) -> Result<(), String> {
    if !target_corresponds(directory, name, Some(expected))? {
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
    recovery.recheck_path()?;
    recovery.sync()?;
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

fn remove_recovery_directory_tree(
    destination: &BoundDirectory,
    recovery_name: &OsStr,
) -> Result<(), String> {
    let path = destination.path.join(recovery_name);
    let metadata = fs::symlink_metadata(&path)
        .map_err(|error| format!("failed to inspect recovery directory: {error}"))?;
    if !metadata.file_type().is_dir()
        || metadata.uid() != effective_user_id()
        || metadata.mode() & 0o077 != 0
    {
        return Err(format!(
            "refusing to remove unsafe recovery path {}",
            path.display()
        ));
    }
    fs::remove_dir_all(&path)
        .map_err(|error| format!("failed to remove recovery directory: {error}"))?;
    destination.sync()
}

fn take_staged_component(component: &mut StagedComponent) -> Result<StagedComponent, String> {
    let placeholder = StagedComponent {
        kind: component.kind,
        directory_index: component.directory_index,
        name: std::ffi::OsString::new(),
        file: component
            .file
            .try_clone()
            .map_err(|error| format!("failed to retain staged descriptor: {error}"))?,
        identity: component.identity,
    };
    Ok(std::mem::replace(component, placeholder))
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
        let result =
            publish_verified_archive_with_hook(&archive, &destination, evidence, |phase| {
                if phase == PublishPhase::BeforeChecksumCommit {
                    observed = true;
                    assert_eq!(fs::read(&destination).unwrap(), b"new archive");
                    assert!(!archive_checksum_path(&destination).unwrap().exists());
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
            PublishPhase::AfterChecksumInvalidation,
            PublishPhase::BeforeManifestMutation,
            PublishPhase::AfterManifestMutation,
            PublishPhase::BeforeArchiveInstall,
            PublishPhase::AfterArchiveInstall,
            PublishPhase::BeforeChecksumCommit,
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
        assert!(!archive_checksum_path(&destination).unwrap().exists());
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
}
