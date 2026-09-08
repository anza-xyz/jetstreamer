use std::{
    path::{Path, PathBuf},
    process::Stdio,
};

use jetstreamer_firehose::epochs::{epoch_to_slot_range, slot_to_epoch};
use tokio::process::Command;

pub const DEFAULT_BUCKET: &str = "gs://mainnet-beta-ledger-us-ny5";
const ALL_SNAPSHOT_ARCHIVE_EXTENSIONS: &[&str] = &[".tar.zst", ".tar.lz4", ".tar.bz2"];

/// Metadata for a snapshot stored in the ledger bucket.
#[derive(Debug, Clone)]
pub struct SnapshotInfo {
    /// Epoch number requested by the caller.
    pub epoch: u64,
    /// Slot directory selected inside the bucket.
    pub slot_dir: u64,
    /// Full GCS URI to the snapshot tarball.
    pub snapshot_uri: String,
}

/// Errors surfaced while resolving or downloading snapshots.
#[derive(Debug, thiserror::Error)]
pub enum SnapshotError {
    #[error("failed to run gcloud: {0}")]
    Spawn(#[from] std::io::Error),
    #[error("gcloud command failed: {command}: {stderr}")]
    CommandFailed { command: String, stderr: String },
    #[error("failed to parse {context}: {value}")]
    Parse {
        context: &'static str,
        value: String,
    },
    #[error("failed to create destination directory {path}: {source}")]
    CreateDir {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("no snapshot directory found for epoch {epoch} in slot range {start}-{end}")]
    SnapshotDirNotFound { epoch: u64, start: u64, end: u64 },
    #[error("no snapshot directory found at or before slot {slot} for epoch {epoch}")]
    SnapshotDirNotFoundAtOrBeforeSlot { epoch: u64, slot: u64 },
    #[error("no snapshot directory reported epoch {epoch}; candidates: {candidates:?}")]
    SnapshotDirEpochMismatch { epoch: u64, candidates: Vec<u64> },
    #[error("no snapshot object found in {slot_dir}")]
    SnapshotObjectNotFound { slot_dir: String },
    #[error("multiple snapshot objects found in {slot_dir}: {objects:?}")]
    MultipleSnapshotObjects {
        slot_dir: String,
        objects: Vec<String>,
    },
    #[error("snapshot uri missing filename: {uri}")]
    SnapshotFilenameMissing { uri: String },
    #[error("invalid exact snapshot identity for slot {slot}: {name}")]
    InvalidExactSnapshotIdentity { slot: u64, name: String },
    #[error("snapshot destination is not a non-empty regular file: {path}")]
    InvalidDestination { path: PathBuf },
    #[error("failed to persist downloaded snapshot to {path}: {source}")]
    Persist {
        path: PathBuf,
        source: std::io::Error,
    },
}

/// Resolve the GCS URI for the snapshot tarball corresponding to an epoch.
pub async fn resolve_epoch_snapshot(epoch: u64) -> Result<SnapshotInfo, SnapshotError> {
    let (start, end) = epoch_snapshot_search_window(epoch);
    let mut candidates = list_bucket_slots(DEFAULT_BUCKET)
        .await?
        .into_iter()
        .filter(|slot| *slot >= start && *slot <= end)
        .collect::<Vec<_>>();

    if candidates.is_empty() {
        return Err(SnapshotError::SnapshotDirNotFound { epoch, start, end });
    }

    candidates.sort_unstable();
    let mut matches = Vec::new();
    for slot in candidates.iter().copied() {
        let marker_epoch = read_epoch_marker(DEFAULT_BUCKET, slot).await?;
        if marker_epoch == Some(epoch) {
            matches.push(slot);
        }
    }

    if matches.is_empty() {
        return Err(SnapshotError::SnapshotDirEpochMismatch { epoch, candidates });
    }

    let slot_dir = *matches.iter().max().unwrap();
    resolve_snapshot_for_slot(DEFAULT_BUCKET, epoch, slot_dir).await
}

/// List all snapshot tarballs that report the requested epoch.
pub async fn list_epoch_snapshots(epoch: u64) -> Result<Vec<SnapshotInfo>, SnapshotError> {
    let (start, end) = epoch_snapshot_search_window(epoch);
    let mut candidates = list_bucket_slots(DEFAULT_BUCKET)
        .await?
        .into_iter()
        .filter(|slot| *slot >= start && *slot <= end)
        .collect::<Vec<_>>();

    if candidates.is_empty() {
        return Err(SnapshotError::SnapshotDirNotFound { epoch, start, end });
    }

    candidates.sort_unstable();
    let mut matches = Vec::new();
    for slot in candidates.iter().copied() {
        let marker_epoch = read_epoch_marker(DEFAULT_BUCKET, slot).await?;
        if marker_epoch == Some(epoch) {
            matches.push(slot);
        }
    }

    if matches.is_empty() {
        return Err(SnapshotError::SnapshotDirEpochMismatch { epoch, candidates });
    }

    let mut snapshots = Vec::new();
    for slot in matches {
        snapshots.push(resolve_snapshot_for_slot(DEFAULT_BUCKET, epoch, slot).await?);
    }
    snapshots.sort_by_key(|info| info.slot_dir);
    Ok(snapshots)
}

/// List every snapshot archive whose directory is inside the inclusive slot
/// range. This is the compatibility-safe discovery path for historical
/// replay: early snapshot directories predate `epoch` marker objects, while
/// their directory and archive filenames still identify the slot exactly.
///
/// Directories containing only RocksDB exports are skipped. More than one
/// snapshot archive in a directory remains an error because there is no safe
/// way to choose between competing state roots.
pub async fn list_snapshots_in_slot_range(
    start_slot: u64,
    end_slot_inclusive: u64,
) -> Result<Vec<SnapshotInfo>, SnapshotError> {
    list_snapshots_in_slot_range_matching(
        start_slot,
        end_slot_inclusive,
        ALL_SNAPSHOT_ARCHIVE_EXTENSIONS,
    )
    .await
}

/// Lists snapshot archives accepted by a slot-selected runtime descriptor.
/// Filtering precedes ambiguity checks, so co-located formats for unrelated
/// runtimes cannot influence which state is selected.
pub async fn list_snapshots_in_slot_range_matching(
    start_slot: u64,
    end_slot_inclusive: u64,
    archive_extensions: &[&str],
) -> Result<Vec<SnapshotInfo>, SnapshotError> {
    if start_slot > end_slot_inclusive {
        return Ok(Vec::new());
    }

    let mut slots = list_bucket_slots(DEFAULT_BUCKET)
        .await?
        .into_iter()
        .filter(|slot| *slot >= start_slot && *slot <= end_slot_inclusive)
        .collect::<Vec<_>>();
    slots.sort_unstable();

    let mut snapshots = Vec::new();
    for slot in slots {
        let objects = list_snapshot_objects(DEFAULT_BUCKET, slot, archive_extensions).await?;
        match objects.as_slice() {
            [] => {}
            [snapshot_uri] => snapshots.push(SnapshotInfo {
                epoch: slot_to_epoch(slot),
                slot_dir: slot,
                snapshot_uri: snapshot_uri.clone(),
            }),
            _ => {
                return Err(SnapshotError::MultipleSnapshotObjects {
                    slot_dir: format!("{DEFAULT_BUCKET}/{slot}"),
                    objects,
                });
            }
        }
    }
    Ok(snapshots)
}

/// Resolve the latest snapshot at or before the provided slot.
pub async fn resolve_snapshot_at_or_before_slot(
    epoch: u64,
    target_slot: u64,
) -> Result<SnapshotInfo, SnapshotError> {
    resolve_snapshot_at_or_before_slot_matching(epoch, target_slot, ALL_SNAPSHOT_ARCHIVE_EXTENSIONS)
        .await
}

/// Resolves the newest snapshot whose archive format is accepted by the
/// slot-selected runtime descriptor.
pub async fn resolve_snapshot_at_or_before_slot_matching(
    epoch: u64,
    target_slot: u64,
    archive_extensions: &[&str],
) -> Result<SnapshotInfo, SnapshotError> {
    // A numeric bucket directory may contain only a RocksDB export. Walk the
    // candidates newest-first and select the first actual snapshot archive,
    // rather than treating the newest numeric directory as a snapshot.
    let mut slots = list_bucket_slots(DEFAULT_BUCKET)
        .await?
        .into_iter()
        .filter(|slot| *slot <= target_slot)
        .collect::<Vec<_>>();
    slots.sort_unstable_by(|left, right| right.cmp(left));

    for slot_dir in slots {
        let objects = list_snapshot_objects(DEFAULT_BUCKET, slot_dir, archive_extensions).await?;
        match objects.as_slice() {
            [] => continue,
            [snapshot_uri] => {
                return Ok(SnapshotInfo {
                    epoch,
                    slot_dir,
                    snapshot_uri: snapshot_uri.clone(),
                });
            }
            _ => {
                return Err(SnapshotError::MultipleSnapshotObjects {
                    slot_dir: format!("{DEFAULT_BUCKET}/{slot_dir}"),
                    objects,
                });
            }
        }
    }

    Err(SnapshotError::SnapshotDirNotFoundAtOrBeforeSlot {
        epoch,
        slot: target_slot,
    })
}

/// Download the snapshot tarball for an epoch into a destination directory.
pub async fn download_epoch_snapshot(
    epoch: u64,
    dest_dir: impl AsRef<Path>,
) -> Result<PathBuf, SnapshotError> {
    let dest_dir = dest_dir.as_ref();
    tokio::fs::create_dir_all(dest_dir)
        .await
        .map_err(|source| SnapshotError::CreateDir {
            path: dest_dir.to_path_buf(),
            source,
        })?;

    let info = resolve_epoch_snapshot(epoch).await?;
    download_snapshot_to_dir(&info, dest_dir).await
}

/// Download the latest snapshot at or before the provided slot into a destination directory.
pub async fn download_snapshot_at_or_before_slot(
    epoch: u64,
    target_slot: u64,
    dest_dir: impl AsRef<Path>,
) -> Result<PathBuf, SnapshotError> {
    download_snapshot_at_or_before_slot_matching(
        epoch,
        target_slot,
        dest_dir,
        ALL_SNAPSHOT_ARCHIVE_EXTENSIONS,
    )
    .await
}

/// Downloads the newest snapshot accepted by the selected runtime.
pub async fn download_snapshot_at_or_before_slot_matching(
    epoch: u64,
    target_slot: u64,
    dest_dir: impl AsRef<Path>,
    archive_extensions: &[&str],
) -> Result<PathBuf, SnapshotError> {
    let dest_dir = dest_dir.as_ref();
    tokio::fs::create_dir_all(dest_dir)
        .await
        .map_err(|source| SnapshotError::CreateDir {
            path: dest_dir.to_path_buf(),
            source,
        })?;

    let info =
        resolve_snapshot_at_or_before_slot_matching(epoch, target_slot, archive_extensions).await?;
    download_snapshot_to_dir(&info, dest_dir).await
}

/// Downloads one registry-committed snapshot object without performing a
/// bucket listing or trusting a discovered filename.
///
/// The object is first written to a unique temporary file in `dest_dir`,
/// synced, and then published without replacing an existing path. A caller
/// must still restore and verify the snapshot's state commitment; this helper
/// only makes local publication crash-safe and binds the requested object name
/// to `slot`.
pub async fn download_exact_snapshot(
    slot: u64,
    archive_name: &str,
    dest_dir: impl AsRef<Path>,
) -> Result<PathBuf, SnapshotError> {
    let expected_prefix = format!("snapshot-{slot}-");
    if !archive_name.starts_with(&expected_prefix)
        || archive_name.contains('/')
        || archive_name.contains('\\')
        || !ALL_SNAPSHOT_ARCHIVE_EXTENSIONS
            .iter()
            .any(|extension| archive_name.ends_with(extension))
    {
        return Err(SnapshotError::InvalidExactSnapshotIdentity {
            slot,
            name: archive_name.to_owned(),
        });
    }

    let dest_dir = dest_dir.as_ref();
    tokio::fs::create_dir_all(dest_dir)
        .await
        .map_err(|source| SnapshotError::CreateDir {
            path: dest_dir.to_path_buf(),
            source,
        })?;
    let destination = dest_dir.join(archive_name);
    if destination.exists() {
        let metadata = std::fs::metadata(&destination).map_err(SnapshotError::Spawn)?;
        if metadata.is_file() && metadata.len() > 0 {
            return Ok(destination);
        }
        return Err(SnapshotError::InvalidDestination { path: destination });
    }

    let temporary = tempfile::Builder::new()
        .prefix(&format!(".{archive_name}."))
        .suffix(".download")
        .tempfile_in(dest_dir)
        .map_err(SnapshotError::Spawn)?;
    let temporary_arg = temporary.path().to_string_lossy().into_owned();
    let uri = format!("{DEFAULT_BUCKET}/{slot}/{archive_name}");
    gcloud_status(&["storage", "cp", &uri, &temporary_arg]).await?;

    let metadata = temporary
        .as_file()
        .metadata()
        .map_err(SnapshotError::Spawn)?;
    if !metadata.is_file() || metadata.len() == 0 {
        return Err(SnapshotError::InvalidDestination {
            path: temporary.path().to_path_buf(),
        });
    }
    temporary
        .as_file()
        .sync_all()
        .map_err(SnapshotError::Spawn)?;
    let published = match temporary.persist_noclobber(&destination) {
        Ok(_) => destination,
        Err(error) if error.error.kind() == std::io::ErrorKind::AlreadyExists => {
            let metadata = std::fs::metadata(&destination).map_err(SnapshotError::Spawn)?;
            if metadata.is_file() && metadata.len() > 0 {
                destination
            } else {
                return Err(SnapshotError::InvalidDestination { path: destination });
            }
        }
        Err(error) => {
            return Err(SnapshotError::Persist {
                path: destination,
                source: error.error,
            });
        }
    };
    std::fs::File::open(dest_dir)
        .and_then(|directory| directory.sync_all())
        .map_err(SnapshotError::Spawn)?;
    Ok(published)
}

fn snapshot_filename(uri: &str) -> Result<&str, SnapshotError> {
    uri.rsplit('/')
        .next()
        .filter(|name| !name.is_empty())
        .ok_or_else(|| SnapshotError::SnapshotFilenameMissing {
            uri: uri.to_string(),
        })
}

fn epoch_snapshot_search_window(epoch: u64) -> (u64, u64) {
    let (_, epoch_end) = epoch_to_slot_range(epoch);
    let prev_epoch = epoch.saturating_sub(1);
    let (prev_start, _) = epoch_to_slot_range(prev_epoch);
    (prev_start, epoch_end)
}

async fn resolve_snapshot_for_slot(
    bucket: &str,
    epoch: u64,
    slot_dir: u64,
) -> Result<SnapshotInfo, SnapshotError> {
    let snapshot_objects =
        list_snapshot_objects(bucket, slot_dir, ALL_SNAPSHOT_ARCHIVE_EXTENSIONS).await?;
    let snapshot_uri = match snapshot_objects.len() {
        0 => {
            return Err(SnapshotError::SnapshotObjectNotFound {
                slot_dir: format!("{bucket}/{slot_dir}"),
            });
        }
        1 => snapshot_objects[0].clone(),
        _ => {
            return Err(SnapshotError::MultipleSnapshotObjects {
                slot_dir: format!("{bucket}/{slot_dir}"),
                objects: snapshot_objects,
            });
        }
    };

    Ok(SnapshotInfo {
        epoch,
        slot_dir,
        snapshot_uri,
    })
}

async fn download_snapshot_to_dir(
    info: &SnapshotInfo,
    dest_dir: &Path,
) -> Result<PathBuf, SnapshotError> {
    let filename = snapshot_filename(&info.snapshot_uri)?;
    let dest_path = dest_dir.join(filename);
    let dest_arg = dest_path.to_string_lossy().to_string();

    gcloud_status(&["storage", "cp", &info.snapshot_uri, &dest_arg]).await?;
    Ok(dest_path)
}

async fn list_bucket_slots(bucket: &str) -> Result<Vec<u64>, SnapshotError> {
    let stdout = gcloud_stdout(&["storage", "ls", bucket]).await?;
    let mut slots = Vec::new();

    for line in stdout.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let Some(rest) = line.strip_prefix(bucket) else {
            continue;
        };
        let rest = rest.trim_start_matches('/').trim_end_matches('/');
        if rest.is_empty() || !rest.chars().all(|ch| ch.is_ascii_digit()) {
            continue;
        }
        let slot: u64 = rest.parse().map_err(|_| SnapshotError::Parse {
            context: "slot directory",
            value: rest.to_string(),
        })?;
        slots.push(slot);
    }

    Ok(slots)
}

fn epoch_marker_uri(bucket: &str, slot: u64, listing: &str) -> Option<String> {
    let base = format!("{bucket}/{slot}");
    for name in ["epoch", "epoch.txt"] {
        let uri = format!("{base}/{name}");
        if listing.lines().map(str::trim).any(|line| line == uri) {
            return Some(uri);
        }
    }
    None
}

async fn read_epoch_marker(bucket: &str, slot: u64) -> Result<Option<u64>, SnapshotError> {
    let base = format!("{bucket}/{slot}");
    let listing = gcloud_stdout(&["storage", "ls", &format!("{base}/")]).await?;
    let Some(uri) = epoch_marker_uri(bucket, slot, &listing) else {
        return Ok(None);
    };

    let output = gcloud_output(&["storage", "cat", &uri]).await?;
    if !output.success {
        return Err(SnapshotError::CommandFailed {
            command: format!("gcloud storage cat {uri}"),
            stderr: output.stderr.trim().to_string(),
        });
    }

    parse_epoch_marker(&uri, &output.stdout).map(Some)
}

fn parse_epoch_marker(path: &str, stdout: &str) -> Result<u64, SnapshotError> {
    let value = stdout
        .lines()
        .map(str::trim)
        .find(|line| !line.is_empty())
        .ok_or_else(|| SnapshotError::Parse {
            context: "epoch marker",
            value: format!("{path}: empty output"),
        })?;
    value.parse().map_err(|_| SnapshotError::Parse {
        context: "epoch marker",
        value: format!("{path}: {value}"),
    })
}

async fn list_snapshot_objects(
    bucket: &str,
    slot: u64,
    archive_extensions: &[&str],
) -> Result<Vec<String>, SnapshotError> {
    let prefix = format!("{bucket}/{slot}");
    let stdout = gcloud_stdout(&["storage", "ls", &format!("{prefix}/")]).await?;
    let mut objects = Vec::new();

    for line in stdout.lines() {
        let line = line.trim();
        if line.is_empty() || line.ends_with('/') {
            continue;
        }
        let Some(name) = line.rsplit('/').next() else {
            continue;
        };
        if snapshot_name_matches(name, archive_extensions) {
            objects.push(line.to_string());
        }
    }

    Ok(objects)
}

fn snapshot_name_matches(name: &str, archive_extensions: &[&str]) -> bool {
    name.starts_with("snapshot-")
        && archive_extensions
            .iter()
            .any(|extension| name.ends_with(extension))
}

struct CommandOutput {
    stdout: String,
    stderr: String,
    success: bool,
}

async fn gcloud_output(args: &[&str]) -> Result<CommandOutput, SnapshotError> {
    let output = Command::new("gcloud")
        .arg("--quiet")
        .args(args)
        .env("CLOUDSDK_CORE_DISABLE_PROMPTS", "1")
        .output()
        .await?;

    Ok(CommandOutput {
        stdout: String::from_utf8_lossy(&output.stdout).to_string(),
        stderr: String::from_utf8_lossy(&output.stderr).to_string(),
        success: output.status.success(),
    })
}

async fn gcloud_stdout(args: &[&str]) -> Result<String, SnapshotError> {
    let output = gcloud_output(args).await?;
    if !output.success {
        return Err(SnapshotError::CommandFailed {
            command: format_command(args),
            stderr: output.stderr.trim().to_string(),
        });
    }
    Ok(output.stdout)
}

async fn gcloud_status(args: &[&str]) -> Result<(), SnapshotError> {
    let status = Command::new("gcloud")
        .args(args)
        .env("CLOUDSDK_CORE_DISABLE_PROMPTS", "1")
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .status()
        .await?;

    if !status.success() {
        return Err(SnapshotError::CommandFailed {
            command: format_command(args),
            stderr: "see output above".to_string(),
        });
    }

    Ok(())
}

fn format_command(args: &[&str]) -> String {
    let mut command = String::from("gcloud");
    for arg in args {
        command.push(' ');
        command.push_str(arg);
    }
    command
}

#[cfg(test)]
mod tests {
    use super::*;

    const BUCKET: &str = "gs://example-bucket";

    #[test]
    fn missing_epoch_marker_is_skipped() {
        let listing = "gs://example-bucket/416012/rocksdb.tar.bz2\n\
                       gs://example-bucket/416012/snapshot-416012-hash.tar.bz2\n";

        assert_eq!(epoch_marker_uri(BUCKET, 416_012, listing), None);
    }

    #[test]
    fn canonical_epoch_marker_is_preferred() {
        let listing = "gs://example-bucket/416012/epoch.txt\n\
                       gs://example-bucket/416012/epoch\n";

        assert_eq!(
            epoch_marker_uri(BUCKET, 416_012, listing).as_deref(),
            Some("gs://example-bucket/416012/epoch")
        );
    }

    #[test]
    fn malformed_epoch_marker_remains_an_error() {
        let err =
            parse_epoch_marker("gs://example-bucket/416012/epoch", "not-an-epoch\n").unwrap_err();

        assert!(matches!(
            err,
            SnapshotError::Parse {
                context: "epoch marker",
                ..
            }
        ));
    }

    #[test]
    fn runtime_archive_filter_is_applied_before_selection() {
        let legacy = &[".tar.bz2"];
        assert!(snapshot_name_matches(
            "snapshot-416012-hash.tar.bz2",
            legacy
        ));
        assert!(!snapshot_name_matches(
            "snapshot-416012-hash.tar.zst",
            legacy
        ));
        assert!(!snapshot_name_matches("rocksdb.tar.bz2", legacy));
    }

    #[tokio::test]
    async fn exact_snapshot_download_rejects_unbound_or_unsafe_names_before_io() {
        let directory = tempfile::tempdir().unwrap();
        for name in [
            "snapshot-619847-hash.tar.bz2",
            "snapshot-619848-hash.zip",
            "../snapshot-619848-hash.tar.bz2",
            "snapshot-619848-dir/file.tar.bz2",
        ] {
            assert!(matches!(
                download_exact_snapshot(619_848, name, directory.path()).await,
                Err(SnapshotError::InvalidExactSnapshotIdentity { .. })
            ));
        }
        assert!(
            std::fs::read_dir(directory.path())
                .unwrap()
                .next()
                .is_none()
        );
    }
}
