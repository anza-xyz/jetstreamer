//! Deterministically estimates v1 -> candidate Horizon archive size changes.
//!
//! The source corpus is opened read-only. Each sampled bucket is decoded and
//! re-encoded into a byte-counting sink, so this program never materializes a
//! candidate archive and never writes into the source directory.
//!
//! Sampling is paired: every candidate sees exactly the same buckets. Within
//! each epoch, buckets are divided into equal-population strata by their
//! existing stored frame size, then ranked by a seeded xxh64. Corpus totals
//! use the exact existing stored size plus the stratified estimate of each
//! candidate's paired byte delta. Approximate 95% confidence intervals use a
//! finite-population correction and require at least two samples in every
//! non-census stratum.
//!
//! Usage:
//! ```text
//! cargo run --release -p jetstreamer-horizon --example estimate_reencode -- \
//!     [ARCHIVE_DIR] [--epochs 940,951] [--strata 5] \
//!     [--samples-per-stratum 2] [--seed 0x485a4e3253414d50] \
//!     [--candidates v2-zstd:9+outer,v2-lz4+adaptive]
//! ```

use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io::{self, BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use jetstreamer_horizon::archive::{
    ArchiveReader, ArchiveVersion, ArchiveWriterConfig, BucketHeader, BucketIndexEntry,
    BucketSelection, Compression, FileHeader, ReencodeOptions, parse_file_header, reencode_archive,
};
use lencode::diff::DiffPolicy;
use lencode::prelude::*;
use xxhash_rust::xxh64::xxh64;

const DEFAULT_STRATA: usize = 5;
const DEFAULT_SAMPLES_PER_STRATUM: usize = 2;
const DEFAULT_SEED: u64 = 0x485a_4e32_5341_4d50;
const WORKER_STACK_BYTES: usize = 256 << 20;
const REENCODE_IO_BUFFER_BYTES: usize = 16 << 20;
const INDEX_IO_BUFFER_BYTES: usize = 1 << 20;
const CAPTURE_PREFIX_BYTES: usize = 4 << 10;
const BUCKET_HEADER_PREFIX_BYTES: usize = 4 << 10;

#[derive(Debug)]
struct Args {
    archive_dir: PathBuf,
    epochs: Option<BTreeSet<u64>>,
    strata: usize,
    samples_per_stratum: usize,
    seed: u64,
    candidates: Vec<Candidate>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Candidate {
    label: String,
    format: ArchiveVersion,
    compression: Compression,
    diff_policy: DiffPolicy,
    zstd_level: i32,
}

#[derive(Debug)]
struct ArchiveInventory {
    path: PathBuf,
    epoch: u64,
    file_bytes: u64,
    header: FileHeader,
    index: Vec<BucketIndexEntry>,
}

#[derive(Debug, Clone, Copy)]
struct Sample {
    archive_index: usize,
    bucket_index: usize,
    stratum: usize,
    stratum_population: usize,
    hash_rank: u64,
}

#[derive(Debug)]
struct SourceBucket {
    header: BucketHeader,
    header_bytes: u64,
    frame_bytes: u64,
}

#[derive(Debug, Clone)]
struct CandidateMeasurement {
    actual_compression: Compression,
    raw_bytes: u64,
    stored_payload_bytes: u64,
    frame_bytes: u64,
    elapsed: Duration,
}

#[derive(Debug, Clone)]
enum CandidateOutcome {
    Success(CandidateMeasurement),
    Failed { elapsed: Duration, error: String },
}

impl CandidateOutcome {
    fn elapsed(&self) -> Duration {
        match self {
            Self::Success(measurement) => measurement.elapsed,
            Self::Failed { elapsed, .. } => *elapsed,
        }
    }
}

#[derive(Debug)]
struct SampleResult {
    sample: Sample,
    source: SourceBucket,
    outcomes: Vec<CandidateOutcome>,
}

#[derive(Debug, Clone, Copy)]
struct Estimate {
    total: f64,
    ci95_half_width: Option<f64>,
}

#[derive(Debug)]
struct CountingWriter {
    bytes: u64,
    prefix: Vec<u8>,
}

impl Default for CountingWriter {
    fn default() -> Self {
        Self {
            bytes: 0,
            prefix: Vec::with_capacity(CAPTURE_PREFIX_BYTES),
        }
    }
}

impl Write for CountingWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.bytes = self
            .bytes
            .checked_add(buf.len() as u64)
            .ok_or_else(|| io::Error::other("counting sink byte count overflow"))?;
        let remaining = CAPTURE_PREFIX_BYTES.saturating_sub(self.prefix.len());
        self.prefix
            .extend_from_slice(&buf[..buf.len().min(remaining)]);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn usage() -> &'static str {
    "usage: estimate_reencode [ARCHIVE_DIR] [--epochs 940,951] [--strata N] \
     [--samples-per-stratum N] [--seed N|0xHEX] \
     [--candidates v2-zstd:9+outer,v2-lz4+adaptive]\n\
     candidate syntax: (v1|v2)-(none|zstd[:LEVEL]|lz4) \
     [+adaptive|+rle|+outer|+disabled]; policy defaults are v1=adaptive, v2=outer"
}

fn default_archive_dir() -> PathBuf {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."))
        .join("horizon")
}

fn take_value(args: &[String], index: &mut usize, flag: &str) -> Result<String, String> {
    *index += 1;
    args.get(*index)
        .cloned()
        .ok_or_else(|| format!("{flag} requires a value"))
}

fn parse_nonzero_usize(value: &str, flag: &str) -> Result<usize, String> {
    let parsed = value
        .parse::<usize>()
        .map_err(|_| format!("invalid {flag} value `{value}`"))?;
    if parsed == 0 {
        return Err(format!("{flag} must be at least 1"));
    }
    Ok(parsed)
}

fn parse_seed(value: &str) -> Result<u64, String> {
    if let Some(hex) = value
        .strip_prefix("0x")
        .or_else(|| value.strip_prefix("0X"))
    {
        u64::from_str_radix(hex, 16).map_err(|_| format!("invalid hexadecimal seed `{value}`"))
    } else {
        value
            .parse::<u64>()
            .map_err(|_| format!("invalid seed `{value}`"))
    }
}

fn parse_epochs(value: &str) -> Result<BTreeSet<u64>, String> {
    if value.trim().is_empty() {
        return Err("--epochs requires at least one epoch".to_string());
    }
    value
        .split(',')
        .map(|part| {
            let part = part.trim();
            if part.is_empty() {
                return Err(format!("invalid empty epoch in `{value}`"));
            }
            part.parse::<u64>()
                .map_err(|_| format!("invalid epoch `{part}`"))
        })
        .collect()
}

fn parse_candidate(value: &str) -> Result<Candidate, String> {
    let value = value.trim();
    let (codec_and_level, policy_name) = match value.split_once('+') {
        Some((candidate, policy)) => {
            if policy.is_empty() || policy.contains('+') {
                return Err(format!("invalid candidate `{value}`"));
            }
            (candidate, Some(policy))
        }
        None => (value, None),
    };
    let (codec, level) = match codec_and_level.split_once(':') {
        Some((codec, level)) => {
            if level.is_empty() || level.contains(':') {
                return Err(format!("invalid candidate `{value}`"));
            }
            (codec, Some(level))
        }
        None => (codec_and_level, None),
    };
    let (version, compression_name) = codec
        .split_once('-')
        .ok_or_else(|| format!("invalid candidate `{value}`"))?;
    let format = match version {
        "v1" => ArchiveVersion::V1,
        "v2" => ArchiveVersion::V2,
        _ => return Err(format!("invalid candidate version in `{value}`")),
    };
    let diff_policy = match policy_name {
        Some("adaptive") => DiffPolicy::Adaptive,
        Some("rle") => DiffPolicy::RleOnly,
        Some("outer") => DiffPolicy::OuterCompressed,
        Some("disabled") => DiffPolicy::Disabled,
        Some(_) => return Err(format!("invalid diff policy in candidate `{value}`")),
        None if format == ArchiveVersion::V1 => DiffPolicy::Adaptive,
        None => DiffPolicy::OuterCompressed,
    };
    if format == ArchiveVersion::V1 && diff_policy == DiffPolicy::OuterCompressed {
        return Err("v1 candidates do not support the outer diff policy".to_string());
    }
    let (compression, zstd_level) = match compression_name {
        "none" => {
            if level.is_some() {
                return Err(format!("candidate `{value}` gives a level to `none`"));
            }
            (Compression::None, 3)
        }
        "zstd" => {
            let level = match level {
                Some(level) => level
                    .parse::<i32>()
                    .map_err(|_| format!("invalid zstd level in `{value}`"))?,
                None => 3,
            };
            (Compression::Zstd, level)
        }
        "lz4" => {
            if level.is_some() {
                return Err(format!("candidate `{value}` gives a level to `lz4`"));
            }
            if format == ArchiveVersion::V1 {
                return Err("v1-lz4 is not a supported archive format".to_string());
            }
            (Compression::Lz4, 3)
        }
        _ => return Err(format!("invalid candidate compression in `{value}`")),
    };
    let version_name = match format {
        ArchiveVersion::V1 => "v1",
        ArchiveVersion::V2 => "v2",
    };
    let codec_label = match compression {
        Compression::None => format!("{version_name}-none"),
        Compression::Zstd => format!("{version_name}-zstd:{zstd_level}"),
        Compression::Lz4 => format!("{version_name}-lz4"),
    };
    let label = format!("{codec_label}+{}", diff_policy_name(diff_policy));
    Ok(Candidate {
        label,
        format,
        compression,
        diff_policy,
        zstd_level,
    })
}

fn parse_candidates(value: &str) -> Result<Vec<Candidate>, String> {
    if value.trim().is_empty() {
        return Err("--candidates requires at least one candidate".to_string());
    }
    let candidates = value
        .split(',')
        .map(parse_candidate)
        .collect::<Result<Vec<_>, _>>()?;
    if candidates.len() > 32 {
        return Err("at most 32 candidates may be compared at once".to_string());
    }
    let mut labels = BTreeSet::new();
    for candidate in &candidates {
        if !labels.insert(candidate.label.clone()) {
            return Err(format!("duplicate candidate `{}`", candidate.label));
        }
    }
    Ok(candidates)
}

fn parse_args(args: Vec<String>) -> Result<Option<Args>, String> {
    let mut archive_dir = None;
    let mut epochs = None;
    let mut strata = DEFAULT_STRATA;
    let mut samples_per_stratum = DEFAULT_SAMPLES_PER_STRATUM;
    let mut seed = DEFAULT_SEED;
    let mut candidates = parse_candidates("v2-zstd:9+outer,v2-lz4+adaptive")?;
    let mut seen_strata = false;
    let mut seen_samples = false;
    let mut seen_seed = false;
    let mut seen_candidates = false;

    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "-h" | "--help" => return Ok(None),
            "--epochs" => {
                if epochs.is_some() {
                    return Err("--epochs may be specified only once".to_string());
                }
                let value = take_value(&args, &mut index, "--epochs")?;
                epochs = Some(parse_epochs(&value)?);
            }
            "--strata" => {
                if seen_strata {
                    return Err("--strata may be specified only once".to_string());
                }
                let value = take_value(&args, &mut index, "--strata")?;
                strata = parse_nonzero_usize(&value, "--strata")?;
                seen_strata = true;
            }
            "--samples-per-stratum" => {
                if seen_samples {
                    return Err("--samples-per-stratum may be specified only once".to_string());
                }
                let value = take_value(&args, &mut index, "--samples-per-stratum")?;
                samples_per_stratum = parse_nonzero_usize(&value, "--samples-per-stratum")?;
                seen_samples = true;
            }
            "--seed" => {
                if seen_seed {
                    return Err("--seed may be specified only once".to_string());
                }
                let value = take_value(&args, &mut index, "--seed")?;
                seed = parse_seed(&value)?;
                seen_seed = true;
            }
            "--candidates" => {
                if seen_candidates {
                    return Err("--candidates may be specified only once".to_string());
                }
                let value = take_value(&args, &mut index, "--candidates")?;
                candidates = parse_candidates(&value)?;
                seen_candidates = true;
            }
            value if value.starts_with('-') => {
                return Err(format!("unknown option `{value}`"));
            }
            value => {
                if archive_dir.is_some() {
                    return Err("at most one ARCHIVE_DIR may be supplied".to_string());
                }
                archive_dir = Some(PathBuf::from(value));
            }
        }
        index += 1;
    }

    Ok(Some(Args {
        archive_dir: archive_dir.unwrap_or_else(default_archive_dir),
        epochs,
        strata,
        samples_per_stratum,
        seed,
        candidates,
    }))
}

fn epoch_from_path(path: &Path) -> Option<u64> {
    let name = path.file_name()?.to_str()?;
    name.strip_prefix("epoch-")?
        .strip_suffix(".jet")?
        .parse()
        .ok()
}

fn checked_add(total: &mut u64, value: u64, what: &str) -> Result<(), String> {
    *total = total
        .checked_add(value)
        .ok_or_else(|| format!("{what} byte count overflow"))?;
    Ok(())
}

fn inventory(args: &Args) -> Result<Vec<ArchiveInventory>, String> {
    let mut paths = Vec::new();
    let entries = std::fs::read_dir(&args.archive_dir).map_err(|error| {
        format!(
            "cannot read archive directory {}: {error}",
            args.archive_dir.display()
        )
    })?;
    for entry in entries {
        let entry = entry.map_err(|error| {
            format!(
                "cannot inspect archive directory {}: {error}",
                args.archive_dir.display()
            )
        })?;
        let file_type = entry
            .file_type()
            .map_err(|error| format!("cannot inspect {}: {error}", entry.path().display()))?;
        if !file_type.is_file() {
            continue;
        }
        let path = entry.path();
        let Some(epoch) = epoch_from_path(&path) else {
            continue;
        };
        if args
            .epochs
            .as_ref()
            .is_some_and(|requested| !requested.contains(&epoch))
        {
            continue;
        }
        paths.push((epoch, path));
    }
    paths.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));
    if paths.is_empty() {
        return Err(format!(
            "no selected epoch-*.jet files found in {}",
            args.archive_dir.display()
        ));
    }
    for pair in paths.windows(2) {
        if pair[0].0 == pair[1].0 {
            return Err(format!(
                "more than one archive claims epoch {}: {} and {}",
                pair[0].0,
                pair[0].1.display(),
                pair[1].1.display()
            ));
        }
    }
    if let Some(requested) = &args.epochs {
        let found = paths
            .iter()
            .map(|(epoch, _)| *epoch)
            .collect::<BTreeSet<_>>();
        let missing = requested.difference(&found).copied().collect::<Vec<_>>();
        if !missing.is_empty() {
            return Err(format!(
                "requested epochs are missing from {}: {missing:?}",
                args.archive_dir.display()
            ));
        }
    }

    let mut archives = Vec::with_capacity(paths.len());
    for (epoch, path) in paths {
        let file = File::open(&path)
            .map_err(|error| format!("cannot open {} read-only: {error}", path.display()))?;
        let file_bytes = file
            .metadata()
            .map_err(|error| format!("cannot stat {}: {error}", path.display()))?
            .len();
        let reader = ArchiveReader::open(BufReader::with_capacity(INDEX_IO_BUFFER_BYTES, file))
            .map_err(|error| format!("cannot index {}: {error}", path.display()))?;
        let header = reader.header().clone();
        if header.epoch != epoch {
            return Err(format!(
                "{} names epoch {epoch}, but its header names epoch {}",
                path.display(),
                header.epoch
            ));
        }
        if header.format_version != ArchiveVersion::V1.as_u16() {
            return Err(format!(
                "{} is format v{}, not the v1 source format this estimator expects",
                path.display(),
                header.format_version
            ));
        }
        let index = reader.bucket_index().to_vec();
        if index.is_empty() {
            return Err(format!("{} contains no buckets", path.display()));
        }
        if args.strata > index.len() {
            return Err(format!(
                "--strata {} exceeds the {} buckets in {}",
                args.strata,
                index.len(),
                path.display()
            ));
        }
        archives.push(ArchiveInventory {
            path,
            epoch,
            file_bytes,
            header,
            index,
        });
    }
    Ok(archives)
}

fn sample_hash(seed: u64, epoch: u64, bucket_index: usize) -> u64 {
    let mut key = [0u8; 16];
    key[..8].copy_from_slice(&epoch.to_le_bytes());
    key[8..].copy_from_slice(&(bucket_index as u64).to_le_bytes());
    xxh64(&key, seed)
}

fn select_samples(args: &Args, archives: &[ArchiveInventory]) -> Result<Vec<Sample>, String> {
    let mut selected = Vec::with_capacity(
        archives
            .len()
            .saturating_mul(args.strata)
            .saturating_mul(args.samples_per_stratum),
    );
    for (archive_index, archive) in archives.iter().enumerate() {
        let mut by_size = (0..archive.index.len()).collect::<Vec<_>>();
        by_size
            .sort_unstable_by_key(|&bucket_index| (archive.index[bucket_index].len, bucket_index));
        for stratum in 0..args.strata {
            let start = stratum * by_size.len() / args.strata;
            let end = (stratum + 1) * by_size.len() / args.strata;
            let population = end - start;
            if args.samples_per_stratum > population {
                return Err(format!(
                    "--samples-per-stratum {} exceeds stratum {stratum}'s population {population} in epoch {}",
                    args.samples_per_stratum, archive.epoch
                ));
            }
            let mut ranked = by_size[start..end]
                .iter()
                .copied()
                .map(|bucket_index| {
                    (
                        sample_hash(args.seed, archive.epoch, bucket_index),
                        bucket_index,
                    )
                })
                .collect::<Vec<_>>();
            ranked.sort_unstable();
            selected.extend(ranked.into_iter().take(args.samples_per_stratum).map(
                |(hash_rank, bucket_index)| Sample {
                    archive_index,
                    bucket_index,
                    stratum,
                    stratum_population: population,
                    hash_rank,
                },
            ));
        }
    }
    selected.sort_unstable_by_key(|sample| {
        (
            archives[sample.archive_index].epoch,
            sample.bucket_index,
            sample.stratum,
        )
    });
    Ok(selected)
}

fn read_source_bucket(
    archive: &ArchiveInventory,
    bucket_index: usize,
) -> Result<SourceBucket, String> {
    let entry = archive.index[bucket_index];
    let mut file = File::open(&archive.path).map_err(|error| {
        format!(
            "cannot open {} read-only for bucket header: {error}",
            archive.path.display()
        )
    })?;
    file.seek(SeekFrom::Start(entry.offset)).map_err(|error| {
        format!(
            "cannot seek {} to bucket {bucket_index}: {error}",
            archive.path.display()
        )
    })?;
    let prefix_len = entry.len.min(BUCKET_HEADER_PREFIX_BYTES as u64) as usize;
    let mut prefix = vec![0u8; prefix_len];
    file.read_exact(&mut prefix).map_err(|error| {
        format!(
            "cannot read bucket {bucket_index} header from {}: {error}",
            archive.path.display()
        )
    })?;
    let mut cursor = lencode::io::Cursor::new(&prefix[..]);
    let header = BucketHeader::decode_ext(&mut cursor, None).map_err(|error| {
        format!(
            "cannot decode bucket {bucket_index} header from {}: {error}",
            archive.path.display()
        )
    })?;
    let header_bytes = cursor.position() as u64;
    if header.first_slot != entry.first_slot {
        return Err(format!(
            "bucket {bucket_index} first-slot mismatch in {}: index {}, header {}",
            archive.path.display(),
            entry.first_slot,
            header.first_slot
        ));
    }
    if header_bytes.checked_add(header.stored_len) != Some(entry.len) {
        return Err(format!(
            "bucket {bucket_index} length mismatch in {}: header {header_bytes} + payload {} != frame {}",
            archive.path.display(),
            header.stored_len,
            entry.len
        ));
    }
    Ok(SourceBucket {
        header,
        header_bytes,
        frame_bytes: entry.len,
    })
}

fn captured_output_bucket_header(sink: &CountingWriter) -> Result<(BucketHeader, u64), String> {
    let (_, bucket_offset) = parse_file_header(&sink.prefix)
        .map_err(|error| format!("cannot parse captured candidate file header: {error}"))?;
    let raw = sink
        .prefix
        .get(bucket_offset..)
        .ok_or_else(|| "captured candidate prefix ends before its bucket".to_string())?;
    let mut cursor = lencode::io::Cursor::new(raw);
    let header = BucketHeader::decode_ext(&mut cursor, None)
        .map_err(|error| format!("cannot parse captured candidate bucket header: {error}"))?;
    Ok((header, cursor.position() as u64))
}

fn run_candidate(
    archive: &ArchiveInventory,
    sample: Sample,
    source: &SourceBucket,
    candidate: &Candidate,
) -> Result<CandidateMeasurement, String> {
    let started = Instant::now();
    let file = File::open(&archive.path)
        .map_err(|error| format!("cannot open {} read-only: {error}", archive.path.display()))?;
    let reader = BufReader::with_capacity(REENCODE_IO_BUFFER_BYTES, file);
    let options = ReencodeOptions {
        writer: ArchiveWriterConfig {
            format: candidate.format,
            bucket_slots: archive.header.bucket_slots,
            compression: candidate.compression,
            diff_policy: candidate.diff_policy,
            zstd_level: candidate.zstd_level,
        },
        buckets: BucketSelection::Indices(vec![sample.bucket_index]),
    };
    let (sink, stats) =
        reencode_archive(reader, CountingWriter::default(), options).map_err(|error| {
            format!(
                "{} bucket {} candidate {} failed: {error}",
                archive.path.display(),
                sample.bucket_index,
                candidate.label
            )
        })?;
    let elapsed = started.elapsed();

    if stats.source_file_bytes != archive.file_bytes
        || stats.source_bucket_bytes != source.frame_bytes
        || stats.source_buckets != 1
        || stats.output.buckets != 1
        || stats.output.slots != source.header.slot_count as u64
        || stats.slots_reencoded != source.header.slot_count as u64
        || stats.output.bytes_written != sink.bytes
    {
        return Err(format!(
            "candidate accounting mismatch for epoch {} bucket {} candidate {}: {stats:?}, sink_bytes={}",
            archive.epoch, sample.bucket_index, candidate.label, sink.bytes
        ));
    }

    let (output_header, output_header_bytes) = captured_output_bucket_header(&sink)?;
    if output_header.first_slot != source.header.first_slot
        || output_header.slot_count != source.header.slot_count
        || output_header.uncompressed_len != stats.output.uncompressed_payload_bytes
        || output_header_bytes.checked_add(output_header.stored_len)
            != Some(stats.output.bucket_bytes_written)
    {
        return Err(format!(
            "candidate frame mismatch for epoch {} bucket {} candidate {}",
            archive.epoch, sample.bucket_index, candidate.label
        ));
    }

    Ok(CandidateMeasurement {
        actual_compression: output_header.compression,
        raw_bytes: output_header.uncompressed_len,
        stored_payload_bytes: output_header.stored_len,
        frame_bytes: stats.output.bucket_bytes_written,
        elapsed,
    })
}

fn collect_results(
    args: &Args,
    archives: &[ArchiveInventory],
    samples: &[Sample],
) -> Result<Vec<SampleResult>, String> {
    let mut results = Vec::with_capacity(samples.len());
    for (sample_ordinal, &sample) in samples.iter().enumerate() {
        let archive = &archives[sample.archive_index];
        let source = read_source_bucket(archive, sample.bucket_index)?;
        let mut outcomes = vec![None; args.candidates.len()];
        for order in 0..args.candidates.len() {
            let candidate_index = (sample_ordinal + order) % args.candidates.len();
            let candidate = &args.candidates[candidate_index];
            eprintln!(
                "sample {}/{}: epoch {} bucket {} (stratum {}, {} source frame bytes), candidate {}",
                sample_ordinal + 1,
                samples.len(),
                archive.epoch,
                sample.bucket_index,
                sample.stratum,
                source.frame_bytes,
                candidate.label
            );
            let started = Instant::now();
            outcomes[candidate_index] =
                Some(match run_candidate(archive, sample, &source, candidate) {
                    Ok(measurement) => CandidateOutcome::Success(measurement),
                    Err(error) => {
                        eprintln!(
                            "candidate failure retained: epoch {} bucket {} candidate {}: {error}",
                            archive.epoch, sample.bucket_index, candidate.label
                        );
                        CandidateOutcome::Failed {
                            elapsed: started.elapsed(),
                            error,
                        }
                    }
                });
        }
        results.push(SampleResult {
            sample,
            source,
            outcomes: outcomes
                .into_iter()
                .map(|outcome| outcome.expect("every candidate ran"))
                .collect(),
        });
    }
    Ok(results)
}

fn stratified_total(
    results: &[SampleResult],
    value: impl Fn(&SampleResult) -> f64,
) -> Result<Estimate, String> {
    let mut groups: BTreeMap<(usize, usize), (usize, Vec<f64>)> = BTreeMap::new();
    for result in results {
        let key = (result.sample.archive_index, result.sample.stratum);
        let group = groups
            .entry(key)
            .or_insert_with(|| (result.sample.stratum_population, Vec::new()));
        if group.0 != result.sample.stratum_population {
            return Err(format!("inconsistent population for sample group {key:?}"));
        }
        group.1.push(value(result));
    }

    let mut total = 0.0;
    let mut variance = 0.0;
    let mut ci_available = true;
    for ((archive_index, stratum), (population, values)) in groups {
        let sampled = values.len();
        if sampled == 0 || sampled > population {
            return Err(format!(
                "invalid sample count {sampled}/{population} for archive {archive_index} stratum {stratum}"
            ));
        }
        let mean = values.iter().sum::<f64>() / sampled as f64;
        total += population as f64 * mean;
        if sampled == population {
            continue;
        }
        if sampled < 2 {
            ci_available = false;
            continue;
        }
        let sample_variance = values
            .iter()
            .map(|sample| {
                let deviation = sample - mean;
                deviation * deviation
            })
            .sum::<f64>()
            / (sampled - 1) as f64;
        let finite_population_correction = 1.0 - sampled as f64 / population as f64;
        variance += (population as f64).powi(2) * finite_population_correction * sample_variance
            / sampled as f64;
    }
    Ok(Estimate {
        total,
        ci95_half_width: ci_available.then(|| 1.96 * variance.sqrt()),
    })
}

fn compression_name(compression: Compression) -> &'static str {
    match compression {
        Compression::None => "none",
        Compression::Zstd => "zstd",
        Compression::Lz4 => "lz4",
    }
}

fn diff_policy_name(policy: DiffPolicy) -> &'static str {
    match policy {
        DiffPolicy::Adaptive => "adaptive",
        DiffPolicy::RleOnly => "rle",
        DiffPolicy::OuterCompressed => "outer",
        DiffPolicy::Disabled => "disabled",
    }
}

fn display_estimate_bound(value: Option<f64>) -> String {
    value
        .map(|value| format!("{value:.0}"))
        .unwrap_or_else(|| "NA".to_string())
}

fn percentile_us(measurements: &[CandidateMeasurement], percentile: f64) -> u128 {
    let mut values = measurements
        .iter()
        .map(|measurement| measurement.elapsed.as_micros())
        .collect::<Vec<_>>();
    values.sort_unstable();
    let rank = ((percentile * values.len() as f64).ceil() as usize)
        .saturating_sub(1)
        .min(values.len() - 1);
    values[rank]
}

fn print_results(
    args: &Args,
    archives: &[ArchiveInventory],
    results: &[SampleResult],
) -> Result<(), String> {
    let mut exact_source_file_bytes = 0u64;
    let mut exact_source_bucket_bytes = 0u64;
    let mut source_buckets = 0u64;
    for archive in archives {
        checked_add(
            &mut exact_source_file_bytes,
            archive.file_bytes,
            "source file",
        )?;
        checked_add(
            &mut source_buckets,
            archive.index.len() as u64,
            "source bucket count",
        )?;
        for entry in &archive.index {
            checked_add(&mut exact_source_bucket_bytes, entry.len, "source bucket")?;
        }
    }
    let source_container_overhead = exact_source_file_bytes
        .checked_sub(exact_source_bucket_bytes)
        .ok_or_else(|| "source bucket bytes exceed source file bytes".to_string())?;

    println!("# horizon deterministic paired re-encode estimate");
    println!("# source_directory={}", args.archive_dir.display());
    println!(
        "# epochs={} files={} buckets={} exact_file_bytes={} exact_bucket_frame_bytes={} exact_container_overhead_bytes={}",
        archives
            .iter()
            .map(|archive| archive.epoch.to_string())
            .collect::<Vec<_>>()
            .join(","),
        archives.len(),
        source_buckets,
        exact_source_file_bytes,
        exact_source_bucket_bytes,
        source_container_overhead
    );
    println!(
        "# sampling=stratified_by_epoch_and_existing_frame_size strata_per_epoch={} samples_per_stratum={} sampled_buckets={} seed=0x{:016x}",
        args.strata,
        args.samples_per_stratum,
        results.len(),
        args.seed
    );
    println!(
        "# candidates={} (same buckets; candidate order rotates by sample to balance cache/order effects)",
        args.candidates
            .iter()
            .map(|candidate| candidate.label.as_str())
            .collect::<Vec<_>>()
            .join(",")
    );
    println!(
        "# timing_scope=end-to-end random-access bucket reencode, including source reopen/index parse/decompress/decode and destination encode/compress; it is not a full-archive ETA"
    );
    println!(
        "# projection=exact old bucket frames + stratified paired candidate-minus-old frame deltas + unchanged old container overhead"
    );
    println!(
        "# ci95=normal approximation with within-stratum sample variance and finite-population correction; NA when any non-census stratum has fewer than 2 samples"
    );
    println!(
        "epoch\tbucket_index\tstratum\tstratum_population\tsample_hash\tfirst_slot\tslots\tsource_codec\tsource_raw_bytes\tsource_header_bytes\tsource_stored_payload_bytes\tsource_frame_bytes\tcandidate\tstatus\tactual_codec\tcandidate_raw_bytes\tcandidate_stored_payload_bytes\tcandidate_frame_bytes\tdelta_frame_bytes\tdelta_percent\telapsed_us\terror"
    );
    for result in results {
        let archive = &archives[result.sample.archive_index];
        for (candidate, outcome) in args.candidates.iter().zip(&result.outcomes) {
            let mut columns = vec![
                archive.epoch.to_string(),
                result.sample.bucket_index.to_string(),
                result.sample.stratum.to_string(),
                result.sample.stratum_population.to_string(),
                format!("0x{:016x}", result.sample.hash_rank),
                result.source.header.first_slot.to_string(),
                result.source.header.slot_count.to_string(),
                compression_name(result.source.header.compression).to_string(),
                result.source.header.uncompressed_len.to_string(),
                result.source.header_bytes.to_string(),
                result.source.header.stored_len.to_string(),
                result.source.frame_bytes.to_string(),
                candidate.label.clone(),
            ];
            match outcome {
                CandidateOutcome::Success(measurement) => {
                    let delta = measurement.frame_bytes as i128 - result.source.frame_bytes as i128;
                    let delta_percent = 100.0 * delta as f64 / result.source.frame_bytes as f64;
                    columns.extend([
                        "ok".to_string(),
                        compression_name(measurement.actual_compression).to_string(),
                        measurement.raw_bytes.to_string(),
                        measurement.stored_payload_bytes.to_string(),
                        measurement.frame_bytes.to_string(),
                        delta.to_string(),
                        format!("{delta_percent:.6}"),
                        measurement.elapsed.as_micros().to_string(),
                        String::new(),
                    ]);
                }
                CandidateOutcome::Failed { elapsed, error } => {
                    columns.extend([
                        "failed".to_string(),
                        "NA".to_string(),
                        "NA".to_string(),
                        "NA".to_string(),
                        "NA".to_string(),
                        "NA".to_string(),
                        "NA".to_string(),
                        elapsed.as_micros().to_string(),
                        error.replace(['\t', '\r', '\n'], " "),
                    ]);
                }
            }
            println!("{}", columns.join("\t"));
        }
    }

    let source_raw = stratified_total(results, |result| {
        result.source.header.uncompressed_len as f64
    })?;
    println!("# summary");
    println!(
        "# projected_source_raw_bytes={:.0} ci95_low={} ci95_high={}",
        source_raw.total,
        display_estimate_bound(
            source_raw
                .ci95_half_width
                .map(|half| source_raw.total - half)
        ),
        display_estimate_bound(
            source_raw
                .ci95_half_width
                .map(|half| source_raw.total + half)
        )
    );
    println!(
        "candidate\tsampled_buckets\tsuccessful_buckets\tfailed_buckets\tsample_success_raw_bytes\tsample_success_frame_bytes\ttotal_attempt_elapsed_us\ttiming_success_p50_us\ttiming_success_p95_us\tsuccess_raw_mib_per_second\tcompression_fallback_buckets\tprojected_candidate_raw_bytes\traw_ci95_low\traw_ci95_high\tprojected_candidate_file_bytes\tfile_ci95_low\tfile_ci95_high\tprojected_savings_bytes\tsavings_ci95_low\tsavings_ci95_high\tprojected_savings_percent\tprojection_status"
    );
    for (candidate_index, candidate) in args.candidates.iter().enumerate() {
        let measurements = results
            .iter()
            .filter_map(|result| match &result.outcomes[candidate_index] {
                CandidateOutcome::Success(measurement) => Some(measurement.clone()),
                CandidateOutcome::Failed { .. } => None,
            })
            .collect::<Vec<_>>();
        let failed_buckets = results.len() - measurements.len();
        let sample_raw = measurements
            .iter()
            .map(|measurement| measurement.raw_bytes)
            .sum::<u64>();
        let sample_frames = measurements
            .iter()
            .map(|measurement| measurement.frame_bytes)
            .sum::<u64>();
        let total_attempt_elapsed = results
            .iter()
            .map(|result| result.outcomes[candidate_index].elapsed())
            .sum::<Duration>();
        let successful_elapsed = measurements
            .iter()
            .map(|measurement| measurement.elapsed)
            .sum::<Duration>();
        let throughput = sample_raw as f64
            / (1024.0 * 1024.0)
            / successful_elapsed.as_secs_f64().max(f64::MIN_POSITIVE);
        let fallback_buckets = measurements
            .iter()
            .filter(|measurement| measurement.actual_compression != candidate.compression)
            .count();

        if failed_buckets != 0 {
            println!(
                "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{:.3}\t{}\tNA\tNA\tNA\tNA\tNA\tNA\tNA\tNA\tNA\tNA\tfailed_candidate_rows",
                candidate.label,
                results.len(),
                measurements.len(),
                failed_buckets,
                sample_raw,
                sample_frames,
                total_attempt_elapsed.as_micros(),
                if measurements.is_empty() {
                    "NA".to_string()
                } else {
                    percentile_us(&measurements, 0.50).to_string()
                },
                if measurements.is_empty() {
                    "NA".to_string()
                } else {
                    percentile_us(&measurements, 0.95).to_string()
                },
                throughput,
                fallback_buckets,
            );
            continue;
        }

        let raw = stratified_total(results, |result| {
            let CandidateOutcome::Success(measurement) = &result.outcomes[candidate_index] else {
                unreachable!("failed candidates were handled before projection")
            };
            measurement.raw_bytes as f64
        })?;
        let paired_delta = stratified_total(results, |result| {
            let CandidateOutcome::Success(measurement) = &result.outcomes[candidate_index] else {
                unreachable!("failed candidates were handled before projection")
            };
            measurement.frame_bytes as f64 - result.source.frame_bytes as f64
        })?;
        let projected_file_bytes = exact_source_file_bytes as f64 + paired_delta.total;
        let projected_savings = -paired_delta.total;
        let savings_percent = 100.0 * projected_savings / exact_source_file_bytes as f64;
        let file_low = paired_delta
            .ci95_half_width
            .map(|half| projected_file_bytes - half);
        let file_high = paired_delta
            .ci95_half_width
            .map(|half| projected_file_bytes + half);
        let savings_low = paired_delta
            .ci95_half_width
            .map(|half| projected_savings - half);
        let savings_high = paired_delta
            .ci95_half_width
            .map(|half| projected_savings + half);

        println!(
            "{}\t{}\t{}\t0\t{}\t{}\t{}\t{}\t{}\t{:.3}\t{}\t{:.0}\t{}\t{}\t{:.0}\t{}\t{}\t{:.0}\t{}\t{}\t{:.6}\tok",
            candidate.label,
            results.len(),
            measurements.len(),
            sample_raw,
            sample_frames,
            total_attempt_elapsed.as_micros(),
            percentile_us(&measurements, 0.50),
            percentile_us(&measurements, 0.95),
            throughput,
            fallback_buckets,
            raw.total,
            display_estimate_bound(raw.ci95_half_width.map(|half| raw.total - half)),
            display_estimate_bound(raw.ci95_half_width.map(|half| raw.total + half)),
            projected_file_bytes,
            display_estimate_bound(file_low),
            display_estimate_bound(file_high),
            projected_savings,
            display_estimate_bound(savings_low),
            display_estimate_bound(savings_high),
            savings_percent
        );
    }
    Ok(())
}

fn run(args: Args) -> Result<(), String> {
    eprintln!(
        "inventorying {} read-only (no archive payload scan and no output files)",
        args.archive_dir.display()
    );
    let archives = inventory(&args)?;
    let samples = select_samples(&args, &archives)?;
    eprintln!(
        "selected {} buckets across {} epoch archives; running {} paired candidates into a counting sink",
        samples.len(),
        archives.len(),
        args.candidates.len()
    );
    let results = collect_results(&args, &archives, &samples)?;
    print_results(&args, &archives, &results)?;
    let failures = results
        .iter()
        .flat_map(|result| &result.outcomes)
        .filter(|outcome| matches!(outcome, CandidateOutcome::Failed { .. }))
        .count();
    if failures != 0 {
        return Err(format!(
            "{failures} candidate bucket run(s) failed; failed rows were retained and projections for affected candidates were suppressed"
        ));
    }
    Ok(())
}

fn main() {
    let args = match parse_args(std::env::args().skip(1).collect()) {
        Ok(Some(args)) => args,
        Ok(None) => {
            println!("{}", usage());
            return;
        }
        Err(error) => {
            eprintln!("error: {error}\n{}", usage());
            std::process::exit(2);
        }
    };

    let worker = match std::thread::Builder::new()
        .name("horizon-reencode-estimator".to_string())
        .stack_size(WORKER_STACK_BYTES)
        .spawn(move || run(args))
    {
        Ok(worker) => worker,
        Err(error) => {
            eprintln!("error: cannot start estimator worker: {error}");
            std::process::exit(1);
        }
    };
    match worker.join() {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            eprintln!("error: {error}");
            std::process::exit(1);
        }
        Err(_) => {
            eprintln!("error: estimator worker panicked");
            std::process::exit(1);
        }
    }
}
