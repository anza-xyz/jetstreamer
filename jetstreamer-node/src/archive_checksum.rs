use std::{
    ffi::OsStr,
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    os::{
        fd::AsRawFd as _,
        unix::fs::{FileExt as _, MetadataExt as _, OpenOptionsExt as _, PermissionsExt as _},
    },
    path::{Path, PathBuf},
    sync::atomic::{AtomicBool, Ordering},
};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tempfile::Builder;

pub const ARCHIVE_CHECKSUM_SUFFIX: &str = ".sha256";
pub const ARCHIVE_BATCH_TRANSACTION_DIRECTORY: &str = ".jetstreamer-archive-batch";
pub const ARCHIVE_BATCH_OUTCOME_DIRECTORY: &str = ".jetstreamer-archive-batch-outcome";
/// Exact contents installed at the canonical checksum path while an archive
/// publication transaction is incomplete. Checksum repair must never replace
/// this marker because the adjacent archive and manifest may be from different
/// sides of an interrupted transaction.
pub const ARCHIVE_PUBLICATION_SENTINEL: &[u8] = b"jetstreamer publication in progress\n";

/// Reports whether an active batch transaction or an unacknowledged completed
/// batch outcome exists in a destination. Any non-directory entry at either
/// reserved name is rejected rather than treated as absence.
pub fn archive_batch_publication_in_progress(
    destination_directory: impl AsRef<Path>,
) -> io::Result<bool> {
    for name in [
        ARCHIVE_BATCH_TRANSACTION_DIRECTORY,
        ARCHIVE_BATCH_OUTCOME_DIRECTORY,
    ] {
        let marker = destination_directory.as_ref().join(name);
        match fs::symlink_metadata(&marker) {
            Ok(metadata) if metadata.file_type().is_dir() => return Ok(true),
            Ok(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "archive batch transaction marker is not a real directory: {}",
                        marker.display()
                    ),
                ));
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
    }
    Ok(false)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ArchiveFileIdentity {
    dev: u64,
    ino: u64,
    len: u64,
    modified_seconds: i64,
    modified_nanoseconds: i64,
    changed_seconds: i64,
    changed_nanoseconds: i64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ValidatedArchiveFile {
    pub identity: ArchiveFileIdentity,
    pub sha256: [u8; 32],
}

/// Journal-only representation. Keeping serde off `ValidatedArchiveFile`
/// preserves its role as an in-process validation capability: downstream
/// callers cannot deserialize forged evidence and pass it to publication.
#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PersistedValidatedArchiveFile {
    dev: u64,
    ino: u64,
    len: u64,
    modified_seconds: i64,
    modified_nanoseconds: i64,
    changed_seconds: i64,
    changed_nanoseconds: i64,
    sha256: [u8; 32],
}

impl PersistedValidatedArchiveFile {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn matches_identity(
        self,
        dev: u64,
        ino: u64,
        len: u64,
        modified_seconds: i64,
        modified_nanoseconds: i64,
        changed_seconds: i64,
        changed_nanoseconds: i64,
    ) -> bool {
        self.dev == dev
            && self.ino == ino
            && self.len == len
            && self.modified_seconds == modified_seconds
            && self.modified_nanoseconds == modified_nanoseconds
            && self.changed_seconds == changed_seconds
            && self.changed_nanoseconds == changed_nanoseconds
    }
}

impl From<ValidatedArchiveFile> for PersistedValidatedArchiveFile {
    fn from(validated: ValidatedArchiveFile) -> Self {
        Self {
            dev: validated.identity.dev,
            ino: validated.identity.ino,
            len: validated.identity.len,
            modified_seconds: validated.identity.modified_seconds,
            modified_nanoseconds: validated.identity.modified_nanoseconds,
            changed_seconds: validated.identity.changed_seconds,
            changed_nanoseconds: validated.identity.changed_nanoseconds,
            sha256: validated.sha256,
        }
    }
}

impl From<PersistedValidatedArchiveFile> for ValidatedArchiveFile {
    fn from(persisted: PersistedValidatedArchiveFile) -> Self {
        Self {
            identity: ArchiveFileIdentity {
                dev: persisted.dev,
                ino: persisted.ino,
                len: persisted.len,
                modified_seconds: persisted.modified_seconds,
                modified_nanoseconds: persisted.modified_nanoseconds,
                changed_seconds: persisted.changed_seconds,
                changed_nanoseconds: persisted.changed_nanoseconds,
            },
            sha256: persisted.sha256,
        }
    }
}

pub fn archive_checksum_path(archive_path: impl AsRef<Path>) -> io::Result<PathBuf> {
    let archive_path = archive_path.as_ref();
    let file_name = archive_path.file_name().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("archive path {} has no filename", archive_path.display()),
        )
    })?;
    let mut checksum_name = file_name.to_os_string();
    checksum_name.push(ARCHIVE_CHECKSUM_SUFFIX);
    Ok(archive_path.with_file_name(checksum_name))
}

/// Returns whether the adjacent checksum is the exact marker left by an
/// interrupted archive publication. A true result requires operator-directed
/// recovery; callers must not repair the checksum or start another publication
/// transaction over that namespace.
pub fn archive_checksum_is_publication_sentinel(
    archive_path: impl AsRef<Path>,
) -> io::Result<bool> {
    let checksum_path = archive_checksum_path(archive_path)?;
    let metadata = match fs::symlink_metadata(&checksum_path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error),
    };
    if !metadata.file_type().is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "archive checksum path is not a regular file: {}",
                checksum_path.display()
            ),
        ));
    }
    if metadata.len() != ARCHIVE_PUBLICATION_SENTINEL.len() as u64 {
        return Ok(false);
    }
    let expected_identity = identity_from_metadata(&metadata);
    let mut file = open_regular_nofollow(&checksum_path)?;
    if archive_file_identity(&file)? != expected_identity {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "archive checksum changed while checking the publication sentinel: {}",
                checksum_path.display()
            ),
        ));
    }
    let mut bytes = vec![0u8; ARCHIVE_PUBLICATION_SENTINEL.len()];
    file.read_exact(&mut bytes)?;
    let mut trailing = [0u8; 1];
    if file.read(&mut trailing)? != 0 || archive_file_identity(&file)? != expected_identity {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "archive checksum changed while checking the publication sentinel: {}",
                checksum_path.display()
            ),
        ));
    }
    Ok(bytes == ARCHIVE_PUBLICATION_SENTINEL)
}

fn identity_from_metadata(metadata: &fs::Metadata) -> ArchiveFileIdentity {
    ArchiveFileIdentity {
        dev: metadata.dev(),
        ino: metadata.ino(),
        len: metadata.len(),
        modified_seconds: metadata.mtime(),
        modified_nanoseconds: metadata.mtime_nsec(),
        changed_seconds: metadata.ctime(),
        changed_nanoseconds: metadata.ctime_nsec(),
    }
}

/// Opens an untrusted archive path without following a final symlink. The
/// nonblocking flag ensures a group-created FIFO cannot stall range startup
/// before the regular-file check runs.
pub fn open_regular_nofollow(path: &Path) -> io::Result<File> {
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)?;
    if !file.metadata()?.file_type().is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("path is not a regular file: {}", path.display()),
        ));
    }
    Ok(file)
}

pub fn archive_file_identity(file: &File) -> io::Result<ArchiveFileIdentity> {
    let metadata = file.metadata()?;
    if !metadata.file_type().is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "archive descriptor is not a regular file",
        ));
    }
    Ok(identity_from_metadata(&metadata))
}

/// Refreshes only the ctime portion of validation evidence after a controlled
/// same-filesystem rename. The caller must keep this exact descriptor open
/// across the rename; inode, size, and mtime are still required to match.
pub fn rebind_validated_after_rename(
    file: &File,
    validated: ValidatedArchiveFile,
) -> io::Result<ValidatedArchiveFile> {
    let current = archive_file_identity(file)?;
    if current.dev != validated.identity.dev
        || current.ino != validated.identity.ino
        || current.len != validated.identity.len
        || current.modified_seconds != validated.identity.modified_seconds
        || current.modified_nanoseconds != validated.identity.modified_nanoseconds
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "archive inode or content metadata changed across rename",
        ));
    }
    Ok(ValidatedArchiveFile {
        identity: current,
        sha256: validated.sha256,
    })
}

pub fn path_matches_archive_identity(
    path: &Path,
    expected: ArchiveFileIdentity,
) -> io::Result<bool> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error),
    };
    Ok(metadata.file_type().is_file() && identity_from_metadata(&metadata) == expected)
}

/// Measures the exact already-open inode that an authoritative archive
/// validator decoded. Callers can carry this evidence through publication
/// without reopening a group-writable pathname and accidentally blessing a
/// replacement file.
pub fn measure_open_archive(file: &File) -> io::Result<ValidatedArchiveFile> {
    measure_open_archive_impl(file, None)
}

pub fn measure_open_archive_cancellable(
    file: &File,
    cancelled: &AtomicBool,
) -> io::Result<ValidatedArchiveFile> {
    measure_open_archive_impl(file, Some(cancelled))
}

fn measure_open_archive_impl(
    file: &File,
    cancelled: Option<&AtomicBool>,
) -> io::Result<ValidatedArchiveFile> {
    let identity = archive_file_identity(file)?;
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; 128 * 1024];
    let mut offset = 0u64;
    while offset < identity.len {
        if cancelled.is_some_and(|cancelled| cancelled.load(Ordering::Relaxed)) {
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "archive measurement cancelled",
            ));
        }
        let remaining = usize::try_from((identity.len - offset).min(buffer.len() as u64))
            .expect("bounded by the fixed hash buffer");
        let read = file.read_at(&mut buffer[..remaining], offset)?;
        if read == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "archive was truncated while it was measured",
            ));
        }
        hasher.update(&buffer[..read]);
        offset = offset
            .checked_add(read as u64)
            .ok_or_else(|| io::Error::other("archive hash offset overflow"))?;
    }
    let measured = ValidatedArchiveFile {
        identity,
        sha256: hasher.finalize().into(),
    };
    if archive_file_identity(file)? != identity {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "archive inode changed while it was measured",
        ));
    }
    Ok(measured)
}

/// Makes a fully written staging inode group-readable but not writable and
/// assigns the destination directory's group before validation. A later rename
/// preserves both properties, so publication cannot accidentally expose a
/// group-writable archive or a file unreadable by the Horizon/FTP group.
pub fn prepare_archive_permissions(file: &File, destination_parent: &Path) -> io::Result<()> {
    let parent = fs::metadata(destination_parent)?;
    if !parent.file_type().is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "archive destination parent is not a directory: {}",
                destination_parent.display()
            ),
        ));
    }
    // SAFETY: `file` owns a live descriptor; -1 preserves the uid, and the gid
    // comes from a successfully statted destination directory.
    if unsafe {
        libc::fchown(
            file.as_raw_fd(),
            !0 as libc::uid_t,
            parent.gid() as libc::gid_t,
        )
    } != 0
    {
        return Err(io::Error::last_os_error());
    }
    file.set_permissions(fs::Permissions::from_mode(0o440))
}

pub fn archive_checksum_line(digest: &[u8; 32], file_name: &OsStr) -> io::Result<String> {
    let file_name = file_name.to_str().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "archive checksum filename is not UTF-8",
        )
    })?;
    Ok(format!(
        "{}  {file_name}\n",
        crate::segment_manifest::sha256_hex_string(digest)
    ))
}

/// Computes the archive digest and creates or repairs its standard adjacent
/// checksum using a synced same-directory temporary file and atomic rename.
/// Callers must complete authoritative archive validation before invoking it;
/// this sidecar is never an archive-validation substitute.
pub fn ensure_archive_checksum_for_validated(
    archive_path: impl AsRef<Path>,
    validated: ValidatedArchiveFile,
) -> io::Result<PathBuf> {
    let archive_path = archive_path.as_ref();
    let parent = archive_path.parent().unwrap_or_else(|| Path::new("."));
    if archive_batch_publication_in_progress(parent)? {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "destination {} has an in-progress archive batch transaction; refusing checksum publication until batch recovery completes",
                parent.display()
            ),
        ));
    }
    if !path_matches_archive_identity(archive_path, validated.identity)? {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "archive path changed after validation: {}",
                archive_path.display()
            ),
        ));
    }
    if archive_checksum_is_publication_sentinel(archive_path)? {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "archive checksum {} is an interrupted Jetstreamer publication sentinel; refusing automatic repair because manual transaction recovery is required",
                archive_checksum_path(archive_path)?.display()
            ),
        ));
    }
    let line = archive_checksum_line(
        &validated.sha256,
        archive_path.file_name().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("archive path {} has no filename", archive_path.display()),
            )
        })?,
    )?;
    let checksum_path = archive_checksum_path(archive_path)?;
    match fs::symlink_metadata(&checksum_path) {
        Ok(metadata) if !metadata.file_type().is_file() => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "archive checksum path is not a regular file: {}",
                    checksum_path.display()
                ),
            ));
        }
        Ok(metadata) => {
            if metadata.len() == line.len() as u64 {
                let mut existing = open_regular_nofollow(&checksum_path)?;
                let existing_len = usize::try_from(metadata.len()).map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        "archive checksum length does not fit usize",
                    )
                })?;
                let mut bytes = vec![0u8; existing_len];
                existing.read_exact(&mut bytes)?;
                let mut trailing = [0u8; 1];
                if existing.read(&mut trailing)? != 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "archive checksum grew while it was read: {}",
                            checksum_path.display()
                        ),
                    ));
                }
                if bytes == line.as_bytes() {
                    if !path_matches_archive_identity(archive_path, validated.identity)? {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!(
                                "archive path changed while validating checksum: {}",
                                archive_path.display()
                            ),
                        ));
                    }
                    return Ok(checksum_path);
                }
            }
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }

    let parent = checksum_path.parent().unwrap_or_else(|| Path::new("."));
    let mut temporary = Builder::new()
        .prefix(".jetstreamer-archive-checksum-")
        .suffix(".partial")
        .tempfile_in(parent)?;
    temporary.write_all(line.as_bytes())?;
    temporary.flush()?;
    temporary
        .as_file()
        .set_permissions(fs::Permissions::from_mode(0o640))?;
    temporary.as_file().sync_all()?;
    if !path_matches_archive_identity(archive_path, validated.identity)? {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "archive path changed before checksum publication: {}",
                archive_path.display()
            ),
        ));
    }
    if archive_checksum_is_publication_sentinel(archive_path)? {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "archive checksum {} became an interrupted Jetstreamer publication sentinel before repair; manual transaction recovery is required",
                checksum_path.display()
            ),
        ));
    }
    if archive_batch_publication_in_progress(parent)? {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "destination {} acquired an in-progress archive batch transaction before checksum publication",
                parent.display()
            ),
        ));
    }
    let published = temporary
        .persist(&checksum_path)
        .map_err(|error| error.error)?;
    published.sync_all()?;
    File::open(parent)?.sync_all()?;
    if !path_matches_archive_identity(archive_path, validated.identity)? {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "archive path changed during checksum publication: {}",
                archive_path.display()
            ),
        ));
    }
    Ok(checksum_path)
}

/// Convenience wrapper for already-authoritatively-validated paths. It is
/// intentionally nofollow and binds the digest to one inode, but callers that
/// already hold validation evidence should use
/// [`ensure_archive_checksum_for_validated`] to close the validation/hash gap.
pub fn ensure_archive_checksum(archive_path: impl AsRef<Path>) -> io::Result<PathBuf> {
    let archive_path = archive_path.as_ref();
    let file = open_regular_nofollow(archive_path)?;
    let validated = measure_open_archive(&file)?;
    ensure_archive_checksum_for_validated(archive_path, validated)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checksum_line_is_lowercase_coreutils_format() {
        let line = archive_checksum_line(&[0xab; 32], OsStr::new("epoch-7.jet")).unwrap();
        assert_eq!(line, format!("{}  epoch-7.jet\n", "ab".repeat(32)));
    }

    #[test]
    fn checksum_is_atomically_created_and_repaired_adjacent_to_archive() {
        let directory = tempfile::TempDir::new().unwrap();
        let archive = directory.path().join("epoch-7.jet");
        fs::write(&archive, b"verified archive bytes").unwrap();

        let checksum = ensure_archive_checksum(&archive).unwrap();
        assert_eq!(checksum, directory.path().join("epoch-7.jet.sha256"));
        let expected = format!(
            "{}  epoch-7.jet\n",
            crate::segment_manifest::sha256_hex_string(
                &measure_open_archive(&open_regular_nofollow(&archive).unwrap())
                    .unwrap()
                    .sha256
            )
        );
        assert_eq!(fs::read_to_string(&checksum).unwrap(), expected);

        fs::write(&checksum, "stale\n").unwrap();
        ensure_archive_checksum(&archive).unwrap();
        assert_eq!(fs::read_to_string(&checksum).unwrap(), expected);
        assert!(fs::read_dir(directory.path()).unwrap().all(|entry| {
            !entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .contains(".partial")
        }));
        assert_eq!(
            fs::metadata(&checksum).unwrap().permissions().mode() & 0o777,
            0o640
        );
    }

    #[test]
    fn publication_sentinel_requires_manual_recovery() {
        let directory = tempfile::TempDir::new().unwrap();
        let archive = directory.path().join("epoch-7.jet");
        let checksum = archive_checksum_path(&archive).unwrap();
        fs::write(&archive, b"verified archive bytes").unwrap();
        fs::write(&checksum, ARCHIVE_PUBLICATION_SENTINEL).unwrap();
        let sentinel_identity =
            archive_file_identity(&open_regular_nofollow(&checksum).unwrap()).unwrap();
        let archive_file = open_regular_nofollow(&archive).unwrap();
        let evidence = measure_open_archive(&archive_file).unwrap();

        assert!(archive_checksum_is_publication_sentinel(&archive).unwrap());
        let error = ensure_archive_checksum_for_validated(&archive, evidence).unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("manual transaction recovery"));
        assert_eq!(
            archive_file_identity(&open_regular_nofollow(&checksum).unwrap()).unwrap(),
            sentinel_identity
        );
        assert_eq!(fs::read(checksum).unwrap(), ARCHIVE_PUBLICATION_SENTINEL);
    }

    #[test]
    fn destination_batch_marker_blocks_checksum_repair() {
        let directory = tempfile::TempDir::new().unwrap();
        let archive = directory.path().join("epoch-7.jet");
        fs::write(&archive, b"verified archive bytes").unwrap();
        fs::create_dir(directory.path().join(ARCHIVE_BATCH_TRANSACTION_DIRECTORY)).unwrap();
        let archive_file = open_regular_nofollow(&archive).unwrap();
        let evidence = measure_open_archive(&archive_file).unwrap();

        let error = ensure_archive_checksum_for_validated(&archive, evidence).unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("batch transaction"));
        assert!(!archive_checksum_path(archive).unwrap().exists());
    }

    #[test]
    fn oversized_existing_checksum_is_replaced_without_unbounded_read() {
        let directory = tempfile::TempDir::new().unwrap();
        let archive = directory.path().join("epoch-8.jet");
        let checksum = directory.path().join("epoch-8.jet.sha256");
        fs::write(&archive, b"verified archive bytes").unwrap();
        let oversized = File::create(&checksum).unwrap();
        oversized.set_len(64 * 1024 * 1024).unwrap();

        ensure_archive_checksum(&archive).unwrap();

        let contents = fs::read_to_string(checksum).unwrap();
        assert_eq!(contents.len(), 64 + 2 + "epoch-8.jet".len() + 1);
    }

    #[test]
    fn publication_permissions_are_read_only_and_follow_destination_group() {
        let directory = tempfile::TempDir::new().unwrap();
        let archive = directory.path().join("epoch-9.jet");
        fs::write(&archive, b"verified archive bytes").unwrap();
        let file = open_regular_nofollow(&archive).unwrap();

        prepare_archive_permissions(&file, directory.path()).unwrap();

        let archive_metadata = file.metadata().unwrap();
        let directory_metadata = fs::metadata(directory.path()).unwrap();
        assert_eq!(archive_metadata.gid(), directory_metadata.gid());
        assert_eq!(archive_metadata.permissions().mode() & 0o777, 0o440);
    }

    #[test]
    fn validated_inode_can_be_rebound_across_a_controlled_rename() {
        let directory = tempfile::TempDir::new().unwrap();
        let staged = directory.path().join("staged.jet");
        let published = directory.path().join("published.jet");
        fs::write(&staged, b"verified archive bytes").unwrap();
        let file = open_regular_nofollow(&staged).unwrap();
        let evidence = measure_open_archive(&file).unwrap();

        fs::rename(&staged, &published).unwrap();
        let rebound = rebind_validated_after_rename(&file, evidence).unwrap();

        assert!(path_matches_archive_identity(&published, rebound.identity).unwrap());
        assert_eq!(rebound.sha256, evidence.sha256);
    }

    #[test]
    fn untrusted_archive_open_rejects_symlinks_and_fifos_without_blocking() {
        use std::{
            ffi::CString,
            os::unix::{ffi::OsStrExt as _, fs::symlink},
            time::{Duration, Instant},
        };

        let directory = tempfile::TempDir::new().unwrap();
        let archive = directory.path().join("epoch-10.jet");
        let symlink_path = directory.path().join("epoch-symlink.jet");
        fs::write(&archive, b"verified archive bytes").unwrap();
        symlink(&archive, &symlink_path).unwrap();
        assert!(open_regular_nofollow(&symlink_path).is_err());

        let fifo_path = directory.path().join("epoch-fifo.jet");
        let fifo_path_c = CString::new(fifo_path.as_os_str().as_bytes()).unwrap();
        // SAFETY: `fifo_path_c` is a valid, NUL-terminated pathname and the
        // temporary directory is exclusively owned by this test.
        assert_eq!(unsafe { libc::mkfifo(fifo_path_c.as_ptr(), 0o600) }, 0);
        let started = Instant::now();
        let error = open_regular_nofollow(&fifo_path).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "opening an untrusted FIFO blocked despite O_NONBLOCK"
        );
    }

    #[test]
    fn checksum_publication_refuses_symlink_and_fifo_sidecars() {
        use std::{
            ffi::CString,
            os::unix::{ffi::OsStrExt as _, fs::symlink},
            time::{Duration, Instant},
        };

        let directory = tempfile::TempDir::new().unwrap();
        let archive = directory.path().join("epoch-11.jet");
        let checksum = archive_checksum_path(&archive).unwrap();
        let victim = directory.path().join("unrelated-file");
        fs::write(&archive, b"verified archive bytes").unwrap();
        fs::write(&victim, b"must remain unchanged").unwrap();
        let evidence = measure_open_archive(&open_regular_nofollow(&archive).unwrap()).unwrap();

        symlink(&victim, &checksum).unwrap();
        let error = ensure_archive_checksum_for_validated(&archive, evidence).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(fs::read(&victim).unwrap(), b"must remain unchanged");
        fs::remove_file(&checksum).unwrap();

        let checksum_c = CString::new(checksum.as_os_str().as_bytes()).unwrap();
        // SAFETY: `checksum_c` is a valid, NUL-terminated path inside an
        // exclusively owned temporary directory.
        assert_eq!(unsafe { libc::mkfifo(checksum_c.as_ptr(), 0o600) }, 0);
        let started = Instant::now();
        let error = ensure_archive_checksum_for_validated(&archive, evidence).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "checking an attacker-created checksum FIFO blocked"
        );
    }
}
