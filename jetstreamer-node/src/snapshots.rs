use std::{
    path::{Path, PathBuf},
    process::Stdio,
};

use jetstreamer_firehose::epochs::epoch_to_slot_range;
use tokio::process::Command;

pub const DEFAULT_BUCKET: &str = "gs://mainnet-beta-ledger-us-ny5";

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
}

/// Resolve the GCS URI for the snapshot tarball corresponding to an epoch.
pub async fn resolve_epoch_snapshot(epoch: u64) -> Result<SnapshotInfo, SnapshotError> {
    let (start, end) = epoch_to_slot_range(epoch);
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
        if marker_epoch == epoch {
            matches.push(slot);
        }
    }

    let slot_dir = match matches.len() {
        0 => return Err(SnapshotError::SnapshotDirEpochMismatch { epoch, candidates }),
        1 => matches[0],
        _ => *matches.iter().max().unwrap(),
    };

    resolve_snapshot_for_slot(DEFAULT_BUCKET, epoch, slot_dir).await
}

/// Resolve the latest snapshot at or before the provided slot.
pub async fn resolve_snapshot_at_or_before_slot(
    epoch: u64,
    target_slot: u64,
) -> Result<SnapshotInfo, SnapshotError> {
    let slot_dir = list_bucket_slots(DEFAULT_BUCKET)
        .await?
        .into_iter()
        .filter(|slot| *slot <= target_slot)
        .max()
        .ok_or(SnapshotError::SnapshotDirNotFoundAtOrBeforeSlot {
            epoch,
            slot: target_slot,
        })?;

    resolve_snapshot_for_slot(DEFAULT_BUCKET, epoch, slot_dir).await
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
    let dest_dir = dest_dir.as_ref();
    tokio::fs::create_dir_all(dest_dir)
        .await
        .map_err(|source| SnapshotError::CreateDir {
            path: dest_dir.to_path_buf(),
            source,
        })?;

    let info = resolve_snapshot_at_or_before_slot(epoch, target_slot).await?;
    download_snapshot_to_dir(&info, dest_dir).await
}

fn snapshot_filename(uri: &str) -> Result<&str, SnapshotError> {
    uri.rsplit('/')
        .next()
        .filter(|name| !name.is_empty())
        .ok_or_else(|| SnapshotError::SnapshotFilenameMissing {
            uri: uri.to_string(),
        })
}

async fn resolve_snapshot_for_slot(
    bucket: &str,
    epoch: u64,
    slot_dir: u64,
) -> Result<SnapshotInfo, SnapshotError> {
    let snapshot_objects = list_snapshot_objects(bucket, slot_dir).await?;
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

async fn read_epoch_marker(bucket: &str, slot: u64) -> Result<u64, SnapshotError> {
    let base = format!("{bucket}/{slot}");
    let candidates = ["epoch", "epoch.txt"];
    let mut last_error = None;

    for name in candidates {
        let uri = format!("{base}/{name}");
        let output = gcloud_output(&["storage", "cat", &uri]).await?;
        if output.success {
            return parse_epoch_marker(&uri, &output.stdout);
        }
        last_error = Some((uri, output.stderr));
    }

    if let Some((uri, stderr)) = last_error {
        return Err(SnapshotError::CommandFailed {
            command: format!("gcloud storage cat {uri}"),
            stderr: stderr.trim().to_string(),
        });
    }

    Err(SnapshotError::Parse {
        context: "epoch marker",
        value: String::new(),
    })
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

async fn list_snapshot_objects(bucket: &str, slot: u64) -> Result<Vec<String>, SnapshotError> {
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
        if name.starts_with("snapshot-")
            && (name.ends_with(".tar.zst") || name.ends_with(".tar.bz2"))
        {
            objects.push(line.to_string());
        }
    }

    Ok(objects)
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
