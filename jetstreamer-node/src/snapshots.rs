use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    process::Stdio,
};

use jetstreamer_firehose::epochs::{epoch_to_slot_range, slot_to_epoch};
use tokio::process::Command;

pub const DEFAULT_BUCKET: &str = "gs://mainnet-beta-ledger-us-ny5";
const ALL_SNAPSHOT_ARCHIVE_EXTENSIONS: &[&str] = &[".tar.zst", ".tar.lz4", ".tar.bz2"];
static DEFAULT_SNAPSHOT_INVENTORY: tokio::sync::OnceCell<SnapshotInventory> =
    tokio::sync::OnceCell::const_new();

/// Metadata for a snapshot stored in the ledger bucket.
#[derive(Debug, Clone)]
pub struct SnapshotInfo {
    /// Epoch number requested by the caller.
    pub epoch: u64,
    /// Snapshot slot committed by the archive filename.
    pub slot_dir: u64,
    /// Full GCS URI to the snapshot tarball.
    pub snapshot_uri: String,
}

/// Validated snapshot objects returned by one bounded bucket listing.
///
/// Root snapshots remain separate because they are consensus checkpoints.
/// Hourly snapshots are additional bootstrap choices, not checkpoint
/// expectations in their own right.
#[derive(Debug, Default)]
struct SnapshotInventory {
    root_objects_by_slot: BTreeMap<u64, Vec<String>>,
    hourly_objects_by_slot: BTreeMap<u64, Vec<String>>,
}

impl SnapshotInventory {
    fn root_slots(&self) -> impl Iterator<Item = u64> + '_ {
        self.root_objects_by_slot.keys().copied()
    }

    fn root_objects_for_slot(&self, slot: u64, archive_extensions: &[&str]) -> Vec<String> {
        matching_snapshot_objects(&self.root_objects_by_slot, slot, archive_extensions)
    }

    fn bootstrap_slots(&self) -> impl Iterator<Item = u64> {
        self.root_objects_by_slot
            .keys()
            .chain(self.hourly_objects_by_slot.keys())
            .copied()
            .collect::<BTreeSet<_>>()
            .into_iter()
    }

    fn bootstrap_objects_for_slot(&self, slot: u64, archive_extensions: &[&str]) -> Vec<String> {
        let mut objects =
            matching_snapshot_objects(&self.root_objects_by_slot, slot, archive_extensions);
        objects.extend(matching_snapshot_objects(
            &self.hourly_objects_by_slot,
            slot,
            archive_extensions,
        ));
        objects.sort_unstable();
        objects.dedup();
        objects
    }
}

fn matching_snapshot_objects(
    objects_by_slot: &BTreeMap<u64, Vec<String>>,
    slot: u64,
    archive_extensions: &[&str],
) -> Vec<String> {
    objects_by_slot
        .get(&slot)
        .into_iter()
        .flatten()
        .filter(|uri| {
            uri.rsplit('/')
                .next()
                .is_some_and(|name| snapshot_name_matches(name, archive_extensions))
        })
        .cloned()
        .collect()
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
    let mut candidates = snapshot_slots_for_bucket(DEFAULT_BUCKET)
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
    let mut candidates = snapshot_slots_for_bucket(DEFAULT_BUCKET)
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

    let mut slots = snapshot_slots_for_bucket(DEFAULT_BUCKET)
        .await?
        .into_iter()
        .filter(|slot| *slot >= start_slot && *slot <= end_slot_inclusive)
        .collect::<Vec<_>>();
    slots.sort_unstable();

    let mut snapshots = Vec::new();
    for slot in slots {
        let objects = snapshot_objects_for_bucket(DEFAULT_BUCKET, slot, archive_extensions).await?;
        if let Some(snapshot_uri) = unique_snapshot_object(DEFAULT_BUCKET, slot, objects)? {
            snapshots.push(SnapshotInfo {
                epoch: slot_to_epoch(slot),
                slot_dir: slot,
                snapshot_uri,
            });
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
    let mut slots = bootstrap_snapshot_slots_for_bucket(DEFAULT_BUCKET)
        .await?
        .into_iter()
        .filter(|slot| *slot <= target_slot)
        .collect::<Vec<_>>();
    slots.sort_unstable_by(|left, right| right.cmp(left));

    for slot_dir in slots {
        let objects =
            bootstrap_snapshot_objects_for_bucket(DEFAULT_BUCKET, slot_dir, archive_extensions)
                .await?;
        if let Some(snapshot_uri) = unique_snapshot_object(DEFAULT_BUCKET, slot_dir, objects)? {
            return Ok(SnapshotInfo {
                epoch,
                slot_dir,
                snapshot_uri,
            });
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
/// The object is first written beneath a unique private directory in
/// `dest_dir`, synced, and then published without replacing an existing path. A caller
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
    let uri = format!("{DEFAULT_BUCKET}/{slot}/{archive_name}");
    download_snapshot_uri_to_dir(&uri, archive_name, dest_dir).await
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
        snapshot_objects_for_bucket(bucket, slot_dir, ALL_SNAPSHOT_ARCHIVE_EXTENSIONS).await?;
    let snapshot_uri =
        unique_snapshot_object(bucket, slot_dir, snapshot_objects)?.ok_or_else(|| {
            SnapshotError::SnapshotObjectNotFound {
                slot_dir: format!("{bucket}/{slot_dir}"),
            }
        })?;

    Ok(SnapshotInfo {
        epoch,
        slot_dir,
        snapshot_uri,
    })
}

fn unique_snapshot_object(
    bucket: &str,
    slot: u64,
    objects: Vec<String>,
) -> Result<Option<String>, SnapshotError> {
    match objects.len() {
        0 => Ok(None),
        1 => Ok(objects.into_iter().next()),
        _ => Err(SnapshotError::MultipleSnapshotObjects {
            slot_dir: format!("{}/{slot}", bucket.trim_end_matches('/')),
            objects,
        }),
    }
}

async fn download_snapshot_to_dir(
    info: &SnapshotInfo,
    dest_dir: &Path,
) -> Result<PathBuf, SnapshotError> {
    let filename = snapshot_filename(&info.snapshot_uri)?;
    download_snapshot_uri_to_dir(&info.snapshot_uri, filename, dest_dir).await
}

/// Downloads beneath a unique private directory in `dest_dir` and publishes
/// it atomically.
///
/// The temporary-directory suffix deliberately cannot match a supported
/// snapshot archive extension. Snapshot discovery therefore cannot mistake
/// an interrupted download for a complete archive. No-clobber hard-link
/// publication also ensures a concurrently created destination is never
/// overwritten.
async fn download_snapshot_uri_to_dir(
    uri: &str,
    filename: &str,
    dest_dir: &Path,
) -> Result<PathBuf, SnapshotError> {
    let destination = dest_dir.join(filename);
    if valid_existing_snapshot_destination(&destination)? {
        return Ok(destination);
    }

    let temporary = new_snapshot_download(filename, dest_dir)?;
    let temporary_arg = temporary.path.to_string_lossy().into_owned();

    gcloud_status(&["storage", "cp", uri, &temporary_arg]).await?;
    publish_downloaded_snapshot(&temporary.path, &destination, dest_dir)
}

struct SnapshotDownload {
    _directory: tempfile::TempDir,
    path: PathBuf,
}

/// Reserves an owner-only directory but deliberately leaves the payload path
/// absent. Current `gcloud storage cp` publishes downloads by renaming over
/// its destination; passing an already-open `NamedTempFile` would leave our
/// descriptor pointing at the empty, replaced inode.
fn new_snapshot_download(
    filename: &str,
    dest_dir: &Path,
) -> Result<SnapshotDownload, SnapshotError> {
    let directory = tempfile::Builder::new()
        .prefix(&format!(".{filename}."))
        .suffix(".download")
        .tempdir_in(dest_dir)
        .map_err(SnapshotError::Spawn)?;
    let path = directory.path().join("payload");
    Ok(SnapshotDownload {
        _directory: directory,
        path,
    })
}

/// Returns whether `destination` is an already complete snapshot file.
///
/// `symlink_metadata` is intentional: following a symlink here would let an
/// attacker redirect either reuse or publication outside `dest_dir`.
fn valid_existing_snapshot_destination(destination: &Path) -> Result<bool, SnapshotError> {
    let metadata = match std::fs::symlink_metadata(destination) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(SnapshotError::Spawn(error)),
    };
    if metadata.file_type().is_file() && metadata.len() > 0 {
        return Ok(true);
    }
    Err(SnapshotError::InvalidDestination {
        path: destination.to_path_buf(),
    })
}

fn publish_downloaded_snapshot(
    temporary: &Path,
    destination: &Path,
    dest_dir: &Path,
) -> Result<PathBuf, SnapshotError> {
    if !valid_existing_snapshot_destination(temporary)? {
        return Err(SnapshotError::InvalidDestination {
            path: temporary.to_path_buf(),
        });
    }
    std::fs::File::open(temporary)
        .and_then(|file| file.sync_all())
        .map_err(SnapshotError::Spawn)?;

    // The temporary directory lives inside `dest_dir`, so a hard link is an
    // atomic, same-filesystem no-clobber publication. Dropping the temporary
    // directory removes its name while the published link retains the bytes.
    let published = match std::fs::hard_link(temporary, destination) {
        Ok(()) => destination.to_path_buf(),
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            if valid_existing_snapshot_destination(destination)? {
                destination.to_path_buf()
            } else {
                return Err(SnapshotError::InvalidDestination {
                    path: destination.to_path_buf(),
                });
            }
        }
        Err(error) => {
            return Err(SnapshotError::Persist {
                path: destination.to_path_buf(),
                source: error,
            });
        }
    };

    std::fs::File::open(dest_dir)
        .and_then(|directory| directory.sync_all())
        .map_err(SnapshotError::Spawn)?;
    Ok(published)
}

fn is_default_bucket(bucket: &str) -> bool {
    bucket.trim_end_matches('/') == DEFAULT_BUCKET
}

fn bulk_snapshot_patterns(bucket: &str) -> [String; 2] {
    let bucket = bucket.trim_end_matches('/');
    [
        format!("{bucket}/*/snapshot-*"),
        format!("{bucket}/*/hourly/snapshot-*"),
    ]
}

async fn default_snapshot_inventory() -> Result<&'static SnapshotInventory, SnapshotError> {
    DEFAULT_SNAPSHOT_INVENTORY
        .get_or_try_init(|| async {
            let [root_pattern, hourly_pattern] = bulk_snapshot_patterns(DEFAULT_BUCKET);
            let listing = gcloud_stdout(&["storage", "ls", &root_pattern, &hourly_pattern]).await?;
            parse_snapshot_inventory(DEFAULT_BUCKET, &listing)
        })
        .await
}

async fn snapshot_slots_for_bucket(bucket: &str) -> Result<Vec<u64>, SnapshotError> {
    if is_default_bucket(bucket) {
        return Ok(default_snapshot_inventory().await?.root_slots().collect());
    }
    list_bucket_slots(bucket).await
}

async fn snapshot_objects_for_bucket(
    bucket: &str,
    slot: u64,
    archive_extensions: &[&str],
) -> Result<Vec<String>, SnapshotError> {
    if is_default_bucket(bucket) {
        return Ok(default_snapshot_inventory()
            .await?
            .root_objects_for_slot(slot, archive_extensions));
    }
    list_snapshot_objects(bucket, slot, archive_extensions).await
}

async fn bootstrap_snapshot_slots_for_bucket(bucket: &str) -> Result<Vec<u64>, SnapshotError> {
    if is_default_bucket(bucket) {
        return Ok(default_snapshot_inventory()
            .await?
            .bootstrap_slots()
            .collect());
    }
    list_bucket_slots(bucket).await
}

async fn bootstrap_snapshot_objects_for_bucket(
    bucket: &str,
    slot: u64,
    archive_extensions: &[&str],
) -> Result<Vec<String>, SnapshotError> {
    if is_default_bucket(bucket) {
        return Ok(default_snapshot_inventory()
            .await?
            .bootstrap_objects_for_slot(slot, archive_extensions));
    }
    list_snapshot_objects(bucket, slot, archive_extensions).await
}

fn parse_snapshot_inventory(
    bucket: &str,
    listing: &str,
) -> Result<SnapshotInventory, SnapshotError> {
    let mut inventory = SnapshotInventory::default();
    for line in listing.lines() {
        let uri = line.trim();
        if uri.is_empty() {
            continue;
        }
        let object = parse_snapshot_object_uri(bucket, uri)?;
        // The wildcard also sees future or unrelated snapshot archive
        // formats. Keep the existing extension-filtering behavior.
        if !snapshot_name_matches(object.name, ALL_SNAPSHOT_ARCHIVE_EXTENSIONS) {
            continue;
        }
        let snapshot_slot = snapshot_slot_from_name(object.name, ALL_SNAPSHOT_ARCHIVE_EXTENSIONS)
            .ok_or_else(|| invalid_snapshot_object_uri(uri))?;
        let objects_by_slot = match object.location {
            SnapshotObjectLocation::Root if object.anchor_slot == snapshot_slot => {
                &mut inventory.root_objects_by_slot
            }
            SnapshotObjectLocation::Hourly if object.anchor_slot <= snapshot_slot => {
                &mut inventory.hourly_objects_by_slot
            }
            SnapshotObjectLocation::Root | SnapshotObjectLocation::Hourly => {
                return Err(invalid_snapshot_object_uri(uri));
            }
        };
        objects_by_slot
            .entry(snapshot_slot)
            .or_default()
            .push(uri.to_owned());
    }
    for objects_by_slot in [
        &mut inventory.root_objects_by_slot,
        &mut inventory.hourly_objects_by_slot,
    ] {
        for objects in objects_by_slot.values_mut() {
            objects.sort_unstable();
            objects.dedup();
        }
    }
    Ok(inventory)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SnapshotObjectLocation {
    Root,
    Hourly,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ParsedSnapshotObject<'a> {
    anchor_slot: u64,
    location: SnapshotObjectLocation,
    name: &'a str,
}

fn parse_snapshot_object_uri<'a>(
    bucket: &str,
    uri: &'a str,
) -> Result<ParsedSnapshotObject<'a>, SnapshotError> {
    let prefix = format!("{}/", bucket.trim_end_matches('/'));
    let rest = uri
        .strip_prefix(&prefix)
        .ok_or_else(|| invalid_snapshot_object_uri(uri))?;
    let mut components = rest.split('/');
    let anchor_text = components
        .next()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| invalid_snapshot_object_uri(uri))?;
    let second = components
        .next()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| invalid_snapshot_object_uri(uri))?;
    let third = components.next();
    let fourth = components.next();
    let (location, name) = match (second, third, fourth) {
        (name, None, None) => (SnapshotObjectLocation::Root, name),
        ("hourly", Some(name), None) if !name.is_empty() => (SnapshotObjectLocation::Hourly, name),
        _ => return Err(invalid_snapshot_object_uri(uri)),
    };
    if name.contains('\\') {
        return Err(invalid_snapshot_object_uri(uri));
    }
    let anchor_slot =
        parse_canonical_slot(anchor_text).ok_or_else(|| invalid_snapshot_object_uri(uri))?;
    Ok(ParsedSnapshotObject {
        anchor_slot,
        location,
        name,
    })
}

fn snapshot_name_is_bound_to_slot(name: &str, slot: u64, archive_extensions: &[&str]) -> bool {
    snapshot_slot_from_name(name, archive_extensions) == Some(slot)
}

fn snapshot_slot_from_name(name: &str, archive_extensions: &[&str]) -> Option<u64> {
    let rest = name.strip_prefix("snapshot-")?;
    let (slot_text, identity_and_extension) = rest.split_once('-')?;
    let slot = parse_canonical_slot(slot_text)?;
    archive_extensions.iter().find_map(|extension| {
        (!extension.is_empty()
            && identity_and_extension
                .strip_suffix(extension)
                .is_some_and(|identity| !identity.is_empty()))
        .then_some(slot)
    })
}

fn parse_canonical_slot(value: &str) -> Option<u64> {
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    let slot = value.parse::<u64>().ok()?;
    (slot.to_string() == value).then_some(slot)
}

fn invalid_snapshot_object_uri(uri: &str) -> SnapshotError {
    SnapshotError::Parse {
        context: "snapshot object URI",
        value: uri.to_owned(),
    }
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
        let uri = line.trim();
        if uri.is_empty() || uri.ends_with('/') {
            continue;
        }
        let Some(name) = uri.rsplit('/').next() else {
            continue;
        };
        if !snapshot_name_matches(name, archive_extensions) {
            continue;
        }
        let object = parse_snapshot_object_uri(bucket, uri)?;
        if object.location != SnapshotObjectLocation::Root
            || object.anchor_slot != slot
            || object.name != name
            || !snapshot_name_is_bound_to_slot(name, slot, archive_extensions)
        {
            return Err(invalid_snapshot_object_uri(uri));
        }
        objects.push(uri.to_owned());
    }

    objects.sort_unstable();
    objects.dedup();

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
    fn bulk_snapshot_listing_separates_root_checkpoints_from_hourly_bootstraps() {
        let listing = "gs://example-bucket/619848/snapshot-619848-zeta.tar.bz2\n\
                       gs://example-bucket/416012/snapshot-416012-alpha.tar.zst\n\
                       gs://example-bucket/3455940/hourly/snapshot-3464856-hour-a.tar.bz2\n\
                       gs://example-bucket/0/hourly/snapshot-3464857-hour-b.tar.bz2\n\
                       gs://example-bucket/3464858/hourly/snapshot-3464858-hour-c.tar.zst\n\
                       gs://example-bucket/619848/snapshot-619848-zeta.tar.bz2\n\
                       gs://example-bucket/830484/snapshot-830484-future.tar.gz\n";

        let inventory = parse_snapshot_inventory(BUCKET, listing).unwrap();
        assert_eq!(
            inventory.root_slots().collect::<Vec<_>>(),
            vec![416_012, 619_848]
        );
        assert_eq!(
            inventory.root_objects_for_slot(416_012, ALL_SNAPSHOT_ARCHIVE_EXTENSIONS),
            vec!["gs://example-bucket/416012/snapshot-416012-alpha.tar.zst"]
        );
        assert_eq!(
            inventory.root_objects_for_slot(619_848, &[".tar.bz2"]),
            vec!["gs://example-bucket/619848/snapshot-619848-zeta.tar.bz2"]
        );
        assert!(
            inventory
                .root_objects_for_slot(416_012, &[".tar.bz2"])
                .is_empty()
        );
        assert_eq!(
            inventory.bootstrap_slots().collect::<Vec<_>>(),
            vec![416_012, 619_848, 3_464_856, 3_464_857, 3_464_858]
        );
        assert_eq!(
            inventory.bootstrap_objects_for_slot(3_464_856, &[".tar.bz2"]),
            vec!["gs://example-bucket/3455940/hourly/snapshot-3464856-hour-a.tar.bz2"]
        );
        assert!(inventory.root_slots().all(|slot| slot != 830_484));
    }

    #[test]
    fn bulk_snapshot_listing_rejects_malformed_or_unbound_uris() {
        for uri in [
            "gs://other-bucket/416012/snapshot-416012-hash.tar.bz2",
            "gs://example-bucket/not-a-slot/snapshot-416012-hash.tar.bz2",
            "gs://example-bucket/0416012/snapshot-416012-hash.tar.bz2",
            "gs://example-bucket/416012/nested/snapshot-416012-hash.tar.bz2",
            "gs://example-bucket/416012/snapshot-619848-hash.tar.bz2",
            "gs://example-bucket/416012/snapshot-0416012-hash.tar.bz2",
            "gs://example-bucket/416012/snapshot-416012-.tar.bz2",
            "gs://example-bucket/416012/snapshot-416012-hash\\.tar.bz2",
            "gs://example-bucket/416013/hourly/snapshot-416012-hash.tar.bz2",
            "gs://example-bucket/0416012/hourly/snapshot-416012-hash.tar.bz2",
            "gs://example-bucket/416012/hourly/snapshot-0416012-hash.tar.bz2",
            "gs://example-bucket/416012/hourly/extra/snapshot-416013-hash.tar.bz2",
            "gs://example-bucket/416012/hourly//snapshot-416013-hash.tar.bz2",
            "gs://example-bucket/416012/../snapshot-416013-hash.tar.bz2",
            "gs://example-bucket/416012/hourly/../snapshot-416013-hash.tar.bz2",
            "gs://example-bucket/18446744073709551616/hourly/snapshot-18446744073709551616-hash.tar.bz2",
        ] {
            let error = parse_snapshot_inventory(BUCKET, uri).unwrap_err();
            assert!(matches!(
                error,
                SnapshotError::Parse {
                    context: "snapshot object URI",
                    ..
                }
            ));
        }
    }

    #[test]
    fn default_bucket_uses_one_bounded_root_and_hourly_listing() {
        assert!(is_default_bucket(DEFAULT_BUCKET));
        assert!(is_default_bucket(&format!("{DEFAULT_BUCKET}/")));
        assert!(!is_default_bucket(BUCKET));
        assert_eq!(
            bulk_snapshot_patterns(DEFAULT_BUCKET),
            [
                "gs://mainnet-beta-ledger-us-ny5/*/snapshot-*",
                "gs://mainnet-beta-ledger-us-ny5/*/hourly/snapshot-*",
            ]
        );
    }

    #[test]
    fn inventory_filtering_precedes_ambiguity_checks() {
        let listing = "gs://example-bucket/416012/snapshot-416012-alpha.tar.bz2\n\
                       gs://example-bucket/416012/snapshot-416012-beta.tar.zst\n\
                       gs://example-bucket/400000/hourly/snapshot-416012-gamma.tar.bz2\n";
        let inventory = parse_snapshot_inventory(BUCKET, listing).unwrap();

        let legacy = inventory.root_objects_for_slot(416_012, &[".tar.bz2"]);
        assert_eq!(
            unique_snapshot_object(BUCKET, 416_012, legacy).unwrap(),
            Some("gs://example-bucket/416012/snapshot-416012-alpha.tar.bz2".to_owned())
        );

        let all = inventory.root_objects_for_slot(416_012, ALL_SNAPSHOT_ARCHIVE_EXTENSIONS);
        assert!(matches!(
            unique_snapshot_object(BUCKET, 416_012, all),
            Err(SnapshotError::MultipleSnapshotObjects { .. })
        ));

        let bootstrap = inventory.bootstrap_objects_for_slot(416_012, &[".tar.bz2"]);
        assert!(matches!(
            unique_snapshot_object(BUCKET, 416_012, bootstrap),
            Err(SnapshotError::MultipleSnapshotObjects { .. })
        ));
    }

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

    #[test]
    fn snapshot_download_directories_cannot_be_discovered_as_archives() {
        let directory = tempfile::tempdir().unwrap();
        let archive_name = "snapshot-830484-hash.tar.bz2";
        let temporary = new_snapshot_download(archive_name, directory.path()).unwrap();
        let temporary_directory = temporary._directory.path().to_path_buf();
        let temporary_name = temporary_directory.file_name().unwrap().to_str().unwrap();

        assert!(temporary_name.starts_with(&format!(".{archive_name}.")));
        assert!(temporary_name.ends_with(".download"));
        assert!(!snapshot_name_matches(
            temporary_name,
            ALL_SNAPSHOT_ARCHIVE_EXTENSIONS
        ));
        assert!(!temporary.path.exists());

        drop(temporary);
        assert!(!temporary_directory.exists());
    }

    #[test]
    fn downloaded_snapshot_is_synced_and_published_without_clobbering() {
        let directory = tempfile::tempdir().unwrap();
        let archive_name = "snapshot-830484-hash.tar.bz2";
        let destination = directory.path().join(archive_name);
        let temporary = new_snapshot_download(archive_name, directory.path()).unwrap();
        std::fs::write(&temporary.path, b"complete snapshot").unwrap();

        let published =
            publish_downloaded_snapshot(&temporary.path, &destination, directory.path()).unwrap();
        drop(temporary);

        assert_eq!(published, destination);
        assert_eq!(std::fs::read(&published).unwrap(), b"complete snapshot");
        assert!(valid_existing_snapshot_destination(&published).unwrap());
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 1);
    }

    #[test]
    fn empty_download_is_rejected_without_publishing() {
        let directory = tempfile::tempdir().unwrap();
        let archive_name = "snapshot-830484-hash.tar.bz2";
        let destination = directory.path().join(archive_name);
        let temporary = new_snapshot_download(archive_name, directory.path()).unwrap();
        std::fs::File::create(&temporary.path).unwrap();

        assert!(matches!(
            publish_downloaded_snapshot(&temporary.path, &destination, directory.path()),
            Err(SnapshotError::InvalidDestination { .. })
        ));
        drop(temporary);
        assert!(!destination.exists());
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
    }

    #[test]
    fn empty_existing_destination_is_rejected() {
        let directory = tempfile::tempdir().unwrap();
        let destination = directory.path().join("snapshot-830484-hash.tar.bz2");
        std::fs::File::create(&destination).unwrap();

        assert!(matches!(
            valid_existing_snapshot_destination(&destination),
            Err(SnapshotError::InvalidDestination { .. })
        ));
    }

    #[cfg(unix)]
    #[test]
    fn symlink_destination_is_rejected_without_touching_its_target() {
        use std::os::unix::fs::symlink;

        let directory = tempfile::tempdir().unwrap();
        let target = directory.path().join("outside-target");
        std::fs::write(&target, b"sentinel").unwrap();
        let destination = directory.path().join("snapshot-830484-hash.tar.bz2");
        symlink(&target, &destination).unwrap();

        assert!(matches!(
            valid_existing_snapshot_destination(&destination),
            Err(SnapshotError::InvalidDestination { .. })
        ));
        assert_eq!(std::fs::read(target).unwrap(), b"sentinel");
    }

    #[cfg(unix)]
    #[test]
    fn race_created_symlink_cannot_redirect_publication() {
        use std::os::unix::fs::symlink;

        let directory = tempfile::tempdir().unwrap();
        let archive_name = "snapshot-830484-hash.tar.bz2";
        let destination = directory.path().join(archive_name);
        let temporary = new_snapshot_download(archive_name, directory.path()).unwrap();
        std::fs::write(&temporary.path, b"complete snapshot").unwrap();

        let target = directory.path().join("outside-target");
        std::fs::write(&target, b"sentinel").unwrap();
        symlink(&target, &destination).unwrap();

        assert!(matches!(
            publish_downloaded_snapshot(&temporary.path, &destination, directory.path()),
            Err(SnapshotError::InvalidDestination { .. })
        ));
        assert_eq!(std::fs::read(target).unwrap(), b"sentinel");
    }

    #[cfg(unix)]
    #[test]
    fn fifo_destination_is_rejected_without_opening_it() {
        use std::{ffi::CString, os::unix::ffi::OsStrExt};

        let directory = tempfile::tempdir().unwrap();
        let destination = directory.path().join("snapshot-830484-hash.tar.bz2");
        let destination_c = CString::new(destination.as_os_str().as_bytes()).unwrap();
        // SAFETY: `destination_c` is a valid NUL-terminated pathname and the
        // mode contains only ordinary permission bits.
        assert_eq!(unsafe { libc::mkfifo(destination_c.as_ptr(), 0o600) }, 0);

        assert!(matches!(
            valid_existing_snapshot_destination(&destination),
            Err(SnapshotError::InvalidDestination { .. })
        ));
    }
}
