//! Safely re-encodes a Horizon archive into the current `.jet` format.
//!
//! The source is opened read-only. Output is streamed to a uniquely named
//! partial file in the destination directory, synced, reopened, and decoded
//! before a same-directory, no-replace hard link publishes the final path. A
//! failed conversion or verification deliberately leaves the partial file in
//! place. The destination parent must be an operator-owned directory: another
//! process with permission to unlink its entries can replace any published
//! path after verification, regardless of the publication primitive.
//! The CLI holds a shared advisory lock on the source for the whole conversion;
//! cooperating writers must acquire an exclusive lock before modifying it.
//! A metadata fingerprint is rechecked immediately before publication as
//! defense in depth. Writers that ignore advisory locks can still race, so the
//! source must otherwise remain immutable during conversion.
//!
//! Usage:
//! ```text
//! cargo run --release -p jetstreamer-horizon --example reencode_archive -- \
//!     <source.jet> <destination.jet> \
//!     [--buckets 1,7,42] [--format v1|v2] \
//!     [--compression none|zstd|lz4] \
//!     [--diff adaptive|rle|outer|disabled] [--zstd-level N] [--bucket-slots N]
//! ```

use std::ffi::OsString;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use jetstreamer_horizon::archive::{
    ArchiveReader, ArchiveVersion, BlockNotification, BucketSelection, ChainMismatchPolicy,
    Compression, Consumption, EpochMeta, ReencodeOptions, ReencodeStats, SemanticDigest, SlotKind,
    SlotVisitor, reencode_archive,
};
use jetstreamer_horizon::transactions::Transaction;
use lencode::diff::DiffPolicy;

const IO_BUFFER_BYTES: usize = 16 << 20;
const WORKER_STACK_BYTES: usize = 256 << 20;

#[derive(Debug)]
struct Args {
    source: PathBuf,
    destination: PathBuf,
    buckets: Option<Vec<usize>>,
    format: ArchiveVersion,
    compression: CompressionName,
    diff_policy: DiffPolicy,
    zstd_level: i32,
    bucket_slots: Option<u16>,
}

#[derive(Debug, Clone, Copy)]
enum CompressionName {
    None,
    Zstd,
    Lz4,
}

impl CompressionName {
    fn parse(value: &str) -> Result<Self, String> {
        match value {
            "none" => Ok(Self::None),
            "zstd" => Ok(Self::Zstd),
            "lz4" => Ok(Self::Lz4),
            _ => Err(format!(
                "unsupported compression `{value}`; expected none, zstd, or lz4"
            )),
        }
    }

    fn archive(self) -> Compression {
        match self {
            Self::None => Compression::None,
            Self::Zstd => Compression::Zstd,
            Self::Lz4 => Compression::Lz4,
        }
    }
}

#[derive(Debug)]
struct Completed {
    source: PathBuf,
    destination: PathBuf,
    stats: ReencodeStats,
    output_file_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SourceFingerprint {
    len: u64,
    #[cfg(unix)]
    device: u64,
    #[cfg(unix)]
    inode: u64,
    #[cfg(unix)]
    modified_seconds: i64,
    #[cfg(unix)]
    modified_nanoseconds: i64,
    #[cfg(unix)]
    changed_seconds: i64,
    #[cfg(unix)]
    changed_nanoseconds: i64,
    #[cfg(not(unix))]
    modified: SystemTime,
}

fn source_fingerprint(metadata: &std::fs::Metadata) -> Result<SourceFingerprint, String> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        Ok(SourceFingerprint {
            len: metadata.len(),
            device: metadata.dev(),
            inode: metadata.ino(),
            modified_seconds: metadata.mtime(),
            modified_nanoseconds: metadata.mtime_nsec(),
            changed_seconds: metadata.ctime(),
            changed_nanoseconds: metadata.ctime_nsec(),
        })
    }

    #[cfg(not(unix))]
    {
        Ok(SourceFingerprint {
            len: metadata.len(),
            modified: metadata
                .modified()
                .map_err(|error| format!("read source modification time: {error}"))?,
        })
    }
}

fn ensure_source_unchanged(file: &File, expected: &SourceFingerprint) -> Result<(), String> {
    let current = file
        .metadata()
        .map_err(|error| format!("reinspect open source before publication: {error}"))?;
    let current = source_fingerprint(&current)?;
    if &current != expected {
        return Err(format!(
            "source changed during conversion (before {expected:?}, after {current:?}); refusing publication"
        ));
    }
    Ok(())
}

#[derive(Default)]
struct VerificationTally {
    slots: u64,
    blocks: u64,
    transactions: u64,
    account_updates: u64,
    orphan_account_updates: u64,
    epochs: u64,
    digest: SemanticDigest,
}

impl SlotVisitor for VerificationTally {
    fn on_slot_start(&mut self, slot: u64, kind: SlotKind) {
        self.digest.on_slot_start(slot, kind);
        self.slots += 1;
        if kind == SlotKind::Block {
            self.blocks += 1;
        }
    }

    fn on_epoch(&mut self, meta: &EpochMeta) {
        self.digest.on_epoch(meta);
        self.epochs += 1;
        self.orphan_account_updates += meta.updates.len() as u64;
    }

    fn on_pre_account_update(
        &mut self,
        slot: u64,
        update: &jetstreamer_horizon::account_updates::AccountUpdateView<'_>,
    ) {
        self.digest.on_pre_account_update(slot, update);
        self.orphan_account_updates += 1;
    }

    fn on_transaction(&mut self, slot: u64, tx_index: u32, tx: &Transaction) {
        self.digest.on_transaction(slot, tx_index, tx);
        self.transactions += 1;
        self.account_updates += tx.account_updates().len() as u64;
    }

    fn on_post_account_update(
        &mut self,
        slot: u64,
        update: &jetstreamer_horizon::account_updates::AccountUpdateView<'_>,
    ) {
        self.digest.on_post_account_update(slot, update);
        self.orphan_account_updates += 1;
    }

    fn on_block(
        &mut self,
        notification: &BlockNotification,
        entries: &[jetstreamer_horizon::archive::EntryRecord],
    ) {
        self.digest.on_block(notification, entries);
    }

    fn consumption(&self) -> Consumption {
        Consumption::all().without_block_account_update_arenas()
    }
}

fn usage() -> &'static str {
    "usage: reencode_archive <source.jet> <destination.jet> \
     [--buckets 1,7,42] [--format v1|v2] \
     [--compression none|zstd|lz4] \
     [--diff adaptive|rle|outer|disabled] [--zstd-level N] [--bucket-slots N]"
}

fn take_value(args: &[String], index: &mut usize, flag: &str) -> Result<String, String> {
    *index += 1;
    args.get(*index)
        .cloned()
        .ok_or_else(|| format!("{flag} requires a value"))
}

fn parse_buckets(value: &str) -> Result<Vec<usize>, String> {
    if value.trim().is_empty() {
        return Err("--buckets requires at least one bucket index".to_string());
    }
    let mut indexes = value
        .split(',')
        .map(|part| {
            let part = part.trim();
            if part.is_empty() {
                return Err(format!("invalid empty bucket index in `{value}`"));
            }
            part.parse::<usize>()
                .map_err(|_| format!("invalid bucket index `{part}`"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    indexes.sort_unstable();
    indexes.dedup();
    Ok(indexes)
}

fn parse_args(args: Vec<String>) -> Result<Option<Args>, String> {
    let mut positionals = Vec::new();
    let mut buckets = None;
    let mut format = ArchiveVersion::V2;
    let mut compression = CompressionName::Zstd;
    let mut diff_policy = DiffPolicy::OuterCompressed;
    // The corpus benchmark found level 9 to be the useful archival knee:
    // materially smaller than level 6, without level 12's disproportionate
    // conversion cost. This matches the archive writer default.
    let mut zstd_level = 9;
    let mut bucket_slots = None;
    let mut seen_compression = false;
    let mut seen_format = false;
    let mut seen_diff = false;
    let mut seen_zstd_level = false;
    let mut seen_bucket_slots = false;

    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "-h" | "--help" => return Ok(None),
            "--buckets" => {
                if buckets.is_some() {
                    return Err("--buckets may be specified only once".to_string());
                }
                let value = take_value(&args, &mut index, "--buckets")?;
                buckets = Some(parse_buckets(&value)?);
            }
            "--compression" => {
                if seen_compression {
                    return Err("--compression may be specified only once".to_string());
                }
                let value = take_value(&args, &mut index, "--compression")?;
                compression = CompressionName::parse(&value)?;
                seen_compression = true;
            }
            "--format" => {
                if seen_format {
                    return Err("--format may be specified only once".to_string());
                }
                let value = take_value(&args, &mut index, "--format")?;
                format = match value.as_str() {
                    "v1" => ArchiveVersion::V1,
                    "v2" => ArchiveVersion::V2,
                    _ => return Err(format!("invalid format `{value}`; expected v1 or v2")),
                };
                seen_format = true;
            }
            "--diff" => {
                if seen_diff {
                    return Err("--diff may be specified only once".to_string());
                }
                let value = take_value(&args, &mut index, "--diff")?;
                diff_policy = match value.as_str() {
                    "adaptive" => DiffPolicy::Adaptive,
                    "rle" => DiffPolicy::RleOnly,
                    "outer" => DiffPolicy::OuterCompressed,
                    "disabled" => DiffPolicy::Disabled,
                    _ => {
                        return Err(format!(
                            "invalid diff policy `{value}`; expected adaptive, rle, outer, or disabled"
                        ));
                    }
                };
                seen_diff = true;
            }
            "--zstd-level" => {
                if seen_zstd_level {
                    return Err("--zstd-level may be specified only once".to_string());
                }
                let value = take_value(&args, &mut index, "--zstd-level")?;
                zstd_level = value
                    .parse::<i32>()
                    .map_err(|_| format!("invalid zstd level `{value}`"))?;
                seen_zstd_level = true;
            }
            "--bucket-slots" => {
                if seen_bucket_slots {
                    return Err("--bucket-slots may be specified only once".to_string());
                }
                let value = take_value(&args, &mut index, "--bucket-slots")?;
                let parsed = value
                    .parse::<u16>()
                    .map_err(|_| format!("invalid bucket slot count `{value}`"))?;
                if parsed == 0 {
                    return Err("--bucket-slots must be at least 1".to_string());
                }
                bucket_slots = Some(parsed);
                seen_bucket_slots = true;
            }
            value if value.starts_with('-') => {
                return Err(format!("unknown option `{value}`"));
            }
            value => positionals.push(PathBuf::from(value)),
        }
        index += 1;
    }

    if positionals.len() != 2 {
        return Err(format!(
            "expected source and destination paths\n{}",
            usage()
        ));
    }

    if !seen_diff && format == ArchiveVersion::V1 {
        diff_policy = DiffPolicy::Adaptive;
    }
    if seen_zstd_level && !matches!(compression, CompressionName::Zstd) {
        return Err("--zstd-level requires --compression zstd".to_string());
    }

    Ok(Some(Args {
        source: positionals.remove(0),
        destination: positionals.remove(0),
        buckets,
        format,
        compression,
        diff_policy,
        zstd_level,
        bucket_slots,
    }))
}

fn ensure_absent(path: &Path) -> Result<(), String> {
    match std::fs::symlink_metadata(path) {
        Ok(_) => Err(format!(
            "refusing to replace existing destination {}",
            path.display()
        )),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(format!("inspect destination {}: {error}", path.display())),
    }
}

fn normalized_destination(path: &Path) -> Result<PathBuf, String> {
    let name = path
        .file_name()
        .ok_or_else(|| format!("destination must name a file: {}", path.display()))?;
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let parent = parent.canonicalize().map_err(|error| {
        format!(
            "resolve destination directory {}: {error}",
            parent.display()
        )
    })?;
    if !parent.is_dir() {
        return Err(format!(
            "destination parent is not a directory: {}",
            parent.display()
        ));
    }
    Ok(parent.join(name))
}

fn create_partial(destination: &Path) -> Result<(PathBuf, File), String> {
    let parent = destination
        .parent()
        .expect("normalized destination has a parent");
    let final_name = destination
        .file_name()
        .expect("normalized destination has a filename");
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();

    for attempt in 0..1024u16 {
        let mut name = OsString::from(".");
        name.push(final_name);
        name.push(format!(
            ".partial.{}.{}.{}",
            std::process::id(),
            nonce,
            attempt
        ));
        let path = parent.join(name);
        match OpenOptions::new().write(true).create_new(true).open(&path) {
            Ok(file) => return Ok((path, file)),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => {
                return Err(format!("create partial output {}: {error}", path.display()));
            }
        }
    }
    Err(format!(
        "could not allocate a unique partial file beside {}",
        destination.display()
    ))
}

fn validate_selection(indexes: &[usize], bucket_count: usize) -> Result<(), String> {
    if let Some(index) = indexes.iter().copied().find(|&index| index >= bucket_count) {
        return Err(format!(
            "bucket index {index} is outside source bucket count {bucket_count}"
        ));
    }
    Ok(())
}

fn verify_output(
    path: &Path,
    expected: &ReencodeStats,
    expected_version: ArchiveVersion,
    expected_bucket_slots: u16,
) -> Result<File, String> {
    let file = File::open(path)
        .map_err(|error| format!("reopen partial output {}: {error}", path.display()))?;
    let verified_file = file
        .try_clone()
        .map_err(|error| format!("retain verified output {}: {error}", path.display()))?;
    let source = BufReader::with_capacity(IO_BUFFER_BYTES, file);
    let mut reader = ArchiveReader::open(source)
        .map_err(|error| format!("open partial output as an archive: {error}"))?;
    reader.verify_chain = true;
    reader.chain_mismatch_policy = ChainMismatchPolicy::AllowZeroParentResume;
    let header_checks = [
        (
            "format version",
            u64::from(reader.header().format_version),
            u64::from(expected_version.as_u16()),
        ),
        (
            "format flags",
            reader.header().flags,
            expected_version.required_flags(),
        ),
        (
            "bucket slots",
            u64::from(reader.header().bucket_slots),
            u64::from(expected_bucket_slots),
        ),
        ("epoch", reader.header().epoch, expected.source_epoch),
        (
            "slot start",
            reader.header().slot_start,
            expected.source_slot_start,
        ),
        (
            "slot count",
            reader.header().slot_count,
            expected.source_slot_count,
        ),
    ];
    if let Some((label, decoded, source)) = header_checks
        .into_iter()
        .find(|(_, decoded, source)| decoded != source)
    {
        return Err(format!(
            "verification header {label} mismatch: decoded {decoded}, source {source}"
        ));
    }
    if reader.bucket_count() as u64 != expected.output.buckets {
        return Err(format!(
            "verification bucket count mismatch: decoded {}, wrote {}",
            reader.bucket_count(),
            expected.output.buckets
        ));
    }

    let mut tally = VerificationTally::default();
    let mut decoded_slots = 0u64;
    for index in 0..reader.bucket_count() {
        decoded_slots += reader
            .read_bucket(index, &mut tally)
            .map_err(|error| format!("verify output bucket {index}: {error}"))?;
    }
    let decoded_payload_bytes = reader.payload_byte_stats().total();
    let zero_parent_resume_artifacts = reader.zero_parent_resume_artifacts();

    let checks = [
        ("slots", decoded_slots, expected.output.slots),
        ("slot callbacks", tally.slots, expected.output.slots),
        (
            "uncompressed payload bytes",
            decoded_payload_bytes,
            expected.output.uncompressed_payload_bytes,
        ),
        ("blocks", tally.blocks, expected.output.blocks),
        (
            "transactions",
            tally.transactions,
            expected.output.transactions,
        ),
        (
            "transaction account updates",
            tally.account_updates,
            expected.output.account_updates,
        ),
        (
            "orphan account updates",
            tally.orphan_account_updates,
            expected.output.orphan_account_updates,
        ),
        ("epoch notifications", tally.epochs, expected.output.epochs),
    ];
    if let Some((label, decoded, written)) = checks
        .into_iter()
        .find(|(_, decoded, written)| decoded != written)
    {
        return Err(format!(
            "verification {label} mismatch: decoded {decoded}, wrote {written}"
        ));
    }
    if zero_parent_resume_artifacts != expected.source_zero_parent_resume_artifacts {
        return Err(format!(
            "verification zero-parent resume artifact mismatch: decoded {zero_parent_resume_artifacts}, source had {}",
            expected.source_zero_parent_resume_artifacts
        ));
    }
    let output_semantic_sha256 = tally
        .digest
        .finish()
        .map_err(|error| format!("finalize output semantic SHA-256: {error}"))?;
    if output_semantic_sha256 != expected.source_semantic_sha256 {
        return Err(
            "verification semantic SHA-256 mismatch: decoded output differs from source"
                .to_string(),
        );
    }
    Ok(verified_file)
}

fn ensure_path_matches_open_file(path: &Path, file: &File, stage: &str) -> Result<(), String> {
    let open_metadata = file
        .metadata()
        .map_err(|error| format!("inspect verified output handle {stage}: {error}"))?;
    let path_metadata = std::fs::symlink_metadata(path)
        .map_err(|error| format!("inspect {} {stage}: {error}", path.display()))?;
    if !path_metadata.file_type().is_file() {
        return Err(format!(
            "{} {stage} is no longer a regular file",
            path.display()
        ));
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if open_metadata.dev() != path_metadata.dev() || open_metadata.ino() != path_metadata.ino()
        {
            return Err(format!(
                "{} {stage} no longer names the verified file",
                path.display()
            ));
        }
    }

    #[cfg(not(unix))]
    if open_metadata.len() != path_metadata.len() {
        return Err(format!(
            "{} {stage} no longer matches the verified file length",
            path.display()
        ));
    }

    Ok(())
}

fn set_partial(shared: &Mutex<Option<PathBuf>>, value: Option<PathBuf>) {
    *shared
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = value;
}

fn sync_directory(path: &Path) -> Result<(), String> {
    let directory = File::open(path)
        .map_err(|error| format!("open destination directory {}: {error}", path.display()))?;
    directory
        .sync_all()
        .map_err(|error| format!("sync destination directory {}: {error}", path.display()))
}

fn run(
    args: Vec<String>,
    partial_state: &Mutex<Option<PathBuf>>,
) -> Result<Option<Completed>, String> {
    let Some(args) = parse_args(args)? else {
        println!("{}", usage());
        return Ok(None);
    };

    let source = args
        .source
        .canonicalize()
        .map_err(|error| format!("resolve source {}: {error}", args.source.display()))?;
    let source_file = OpenOptions::new()
        .read(true)
        .open(&source)
        .map_err(|error| format!("open source read-only {}: {error}", source.display()))?;
    let source_metadata = source_file
        .metadata()
        .map_err(|error| format!("inspect open source {}: {error}", source.display()))?;
    if !source_metadata.is_file() {
        return Err(format!(
            "source is not a regular file: {}",
            source.display()
        ));
    }
    source_file.try_lock_shared().map_err(|error| {
        format!(
            "acquire shared source lock without waiting {}: {error}",
            source.display()
        )
    })?;
    // Capture the fingerprint only after the cooperative lock is held. The
    // retained clone below keeps that lock alive through output publication.
    let source_metadata = source_file
        .metadata()
        .map_err(|error| format!("inspect locked source {}: {error}", source.display()))?;
    let source_fingerprint = source_fingerprint(&source_metadata)?;
    let source_guard = source_file.try_clone().map_err(|error| {
        format!(
            "retain source identity handle {}: {error}",
            source.display()
        )
    })?;

    ensure_absent(&args.destination)?;
    let destination = normalized_destination(&args.destination)?;
    ensure_absent(&destination)?;
    if source == destination {
        return Err("source and destination resolve to the same path".to_string());
    }

    let probe_file = source_file
        .try_clone()
        .map_err(|error| format!("duplicate source handle {}: {error}", source.display()))?;
    let probe = ArchiveReader::open(BufReader::with_capacity(IO_BUFFER_BYTES, probe_file))
        .map_err(|error| format!("open source archive: {error}"))?;
    let source_bucket_count = probe.bucket_count();
    let source_bucket_slots = probe.header().bucket_slots;
    if let Some(indexes) = args.buckets.as_deref() {
        validate_selection(indexes, source_bucket_count)?;
    }
    drop(probe);

    let selection = args
        .buckets
        .map_or(BucketSelection::All, BucketSelection::Indices);
    let selected_count = match &selection {
        BucketSelection::All => source_bucket_count,
        BucketSelection::Indices(indexes) => indexes.len(),
    };
    let selection_is_complete = selected_count == source_bucket_count;
    let destination_bucket_slots = args.bucket_slots.unwrap_or(source_bucket_slots);
    if !selection_is_complete && destination_bucket_slots != source_bucket_slots {
        return Err(format!(
            "partial bucket re-encoding requires --bucket-slots {source_bucket_slots}; got {destination_bucket_slots}"
        ));
    }
    if args.format == ArchiveVersion::V1 && matches!(args.compression, CompressionName::Lz4) {
        return Err("format v1 does not support LZ4 bucket compression".to_string());
    }
    if args.format == ArchiveVersion::V1 && args.diff_policy == DiffPolicy::OuterCompressed {
        return Err("format v1 does not support the outer-compressed diff mode".to_string());
    }

    let (partial, output_file) = create_partial(&destination)?;
    set_partial(partial_state, Some(partial.clone()));
    eprintln!("source:      {}", source.display());
    eprintln!("partial:     {}", partial.display());
    eprintln!("destination: {}", destination.display());
    eprintln!("selection:   {selected_count} of {source_bucket_count} source buckets");
    eprintln!(
        "encoding:    {:?}, {:?}, {:?}, zstd level {}",
        args.format, args.compression, args.diff_policy, args.zstd_level
    );

    let source_reader = BufReader::with_capacity(IO_BUFFER_BYTES, source_file);
    let output_writer = BufWriter::with_capacity(IO_BUFFER_BYTES, output_file);
    let options = ReencodeOptions {
        writer: jetstreamer_horizon::archive::ArchiveWriterConfig {
            format: args.format,
            bucket_slots: destination_bucket_slots,
            compression: args.compression.archive(),
            diff_policy: args.diff_policy,
            zstd_level: args.zstd_level,
        },
        buckets: selection,
    };

    let (mut output_writer, stats) = reencode_archive(source_reader, output_writer, options)
        .map_err(|error| format!("re-encode failed: {error}"))?;
    output_writer
        .flush()
        .map_err(|error| format!("flush partial output: {error}"))?;
    output_writer
        .get_ref()
        .sync_all()
        .map_err(|error| format!("sync partial output: {error}"))?;
    drop(output_writer);

    let output_file_bytes = partial
        .metadata()
        .map_err(|error| format!("inspect partial output: {error}"))?
        .len();
    if output_file_bytes != stats.output.bytes_written {
        return Err(format!(
            "output length mismatch after sync: file has {output_file_bytes} bytes, writer reported {}",
            stats.output.bytes_written
        ));
    }

    eprintln!(
        "verifying {} output buckets by checksum, decompression, and full decode",
        stats.output.buckets
    );
    let verified_output = verify_output(&partial, &stats, args.format, destination_bucket_slots)?;
    ensure_path_matches_open_file(&partial, &verified_output, "before publication")?;
    ensure_source_unchanged(&source_guard, &source_fingerprint)?;

    // `hard_link` is an atomic no-replace publication: unlike Unix rename,
    // it cannot overwrite a destination created during the conversion. The
    // verified partial and final name temporarily refer to the same inode.
    ensure_absent(&destination)?;
    std::fs::hard_link(&partial, &destination).map_err(|error| {
        format!(
            "publish {} as {}: {error}",
            partial.display(),
            destination.display()
        )
    })?;
    if let Err(identity_error) =
        ensure_path_matches_open_file(&destination, &verified_output, "after publication")
    {
        return match std::fs::remove_file(&destination) {
            Ok(()) => Err(format!(
                "{identity_error}; removed the unverified destination path"
            )),
            Err(cleanup_error) => Err(format!(
                "{identity_error}; removing the unverified destination {} also failed: {cleanup_error}",
                destination.display()
            )),
        };
    }
    let destination_parent = destination
        .parent()
        .expect("normalized destination has a parent");
    sync_directory(destination_parent).map_err(|error| {
        format!(
            "destination {} was linked but its directory was not synced: {error}; verified partial remains at {}",
            destination.display(),
            partial.display()
        )
    })?;

    std::fs::remove_file(&partial).map_err(|error| {
        format!(
            "destination {} was published and synced, but removing its partial hard link {} failed: {error}",
            destination.display(),
            partial.display()
        )
    })?;
    set_partial(partial_state, None);
    if let Err(error) = sync_directory(destination_parent) {
        // The final link was already synced before cleanup. Failure to sync
        // the removal is not a data-integrity failure, but it is worth making
        // visible to an operator.
        eprintln!("warning: {error} after partial cleanup");
    }

    Ok(Some(Completed {
        source,
        destination,
        stats,
        output_file_bytes,
    }))
}

fn print_completed(completed: &Completed) {
    let stats = completed.stats;
    let bucket_change = if stats.source_bucket_bytes == 0 {
        0.0
    } else {
        100.0 * (stats.output.bucket_bytes_written as f64 / stats.source_bucket_bytes as f64 - 1.0)
    };
    println!("re-encode complete and verified");
    println!("  source:              {}", completed.source.display());
    println!("  destination:         {}", completed.destination.display());
    println!("  source file bytes:    {}", stats.source_file_bytes);
    println!("  selected buckets:     {}", stats.source_buckets);
    println!("  slots re-encoded:     {}", stats.slots_reencoded);
    println!(
        "  zero-parent resumes:   {} (preserved unchanged)",
        stats.source_zero_parent_resume_artifacts
    );
    println!("  source bucket bytes:  {}", stats.source_bucket_bytes);
    println!(
        "  output bucket bytes:  {} ({bucket_change:+.3}%)",
        stats.output.bucket_bytes_written
    );
    println!("  output file bytes:    {}", completed.output_file_bytes);
}

fn main() {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    let partial_state = Arc::new(Mutex::new(None));
    let worker_state = Arc::clone(&partial_state);
    let worker = match std::thread::Builder::new()
        .name("horizon-reencode".to_string())
        .stack_size(WORKER_STACK_BYTES)
        .spawn(move || run(args, &worker_state))
    {
        Ok(worker) => worker,
        Err(error) => {
            eprintln!("failed to spawn re-encode worker: {error}");
            std::process::exit(1);
        }
    };

    match worker.join() {
        Ok(Ok(Some(completed))) => print_completed(&completed),
        Ok(Ok(None)) => {}
        Ok(Err(error)) => {
            eprintln!("reencode_archive: {error}");
            if let Some(path) = partial_state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .as_ref()
            {
                eprintln!("partial output retained at {}", path.display());
            }
            std::process::exit(1);
        }
        Err(_) => {
            eprintln!("reencode_archive: worker panicked");
            if let Some(path) = partial_state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .as_ref()
            {
                eprintln!("partial output retained at {}", path.display());
            }
            std::process::exit(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(extra: &[&str]) -> Result<Args, String> {
        let mut args = vec!["source.jet".to_string(), "destination.jet".to_string()];
        args.extend(extra.iter().map(|arg| (*arg).to_string()));
        parse_args(args)?.ok_or_else(|| "unexpected help result".to_string())
    }

    #[test]
    fn migration_defaults_target_the_measured_archival_configuration() {
        let args = parse(&[]).unwrap();
        assert_eq!(args.format, ArchiveVersion::V2);
        assert!(matches!(args.compression, CompressionName::Zstd));
        assert_eq!(args.diff_policy, DiffPolicy::OuterCompressed);
        assert_eq!(args.zstd_level, 9);
    }

    #[test]
    fn v1_uses_its_compatible_default_diff_policy() {
        let args = parse(&["--format", "v1"]).unwrap();
        assert_eq!(args.diff_policy, DiffPolicy::Adaptive);
    }

    #[test]
    fn zstd_level_is_not_silently_ignored_for_other_codecs() {
        let error = parse(&["--compression", "lz4", "--zstd-level", "9"]).unwrap_err();
        assert_eq!(error, "--zstd-level requires --compression zstd");
    }

    #[test]
    fn source_shared_lock_blocks_a_cooperating_writer() {
        let mut source = tempfile::NamedTempFile::new().unwrap();
        std::io::Write::write_all(&mut source, b"before").unwrap();
        source.as_file().sync_all().unwrap();
        source.as_file().lock_shared().unwrap();

        let writer = OpenOptions::new()
            .read(true)
            .write(true)
            .open(source.path())
            .unwrap();
        assert!(matches!(
            writer.try_lock(),
            Err(std::fs::TryLockError::WouldBlock)
        ));
    }

    #[test]
    fn source_fingerprint_detects_observable_changes() {
        let mut source = tempfile::NamedTempFile::new().unwrap();
        std::io::Write::write_all(&mut source, b"before").unwrap();
        source.as_file().sync_all().unwrap();
        let fingerprint = source_fingerprint(&source.as_file().metadata().unwrap()).unwrap();
        ensure_source_unchanged(source.as_file(), &fingerprint).unwrap();

        source.as_file().set_len(3).unwrap();
        source.as_file().sync_all().unwrap();
        assert!(ensure_source_unchanged(source.as_file(), &fingerprint).is_err());
    }
}
