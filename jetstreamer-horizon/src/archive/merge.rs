//! Verified assembly of contiguous single-runtime archives.
//!
//! Sources are decoded and re-encoded in event order. This is intentionally
//! not a bucket concatenation operation: runtime-local account write versions
//! are checked and rebased, and PoH continuity is checked across every bucket
//! and source boundary before a V3 provenance envelope is attached.

use std::io::{Read, Seek, SeekFrom, Write};

use sha2::{Digest, Sha256};
use solana_hash::Hash;

use crate::{account_updates::AccountUpdateView, transactions::Transaction};

use super::{
    ARCHIVE_PROVENANCE_VERSION_V1, ARCHIVE_PROVENANCE_VERSION_V3, ArchiveFormatError,
    ArchiveProvenance, ArchiveProvenanceError, ArchiveProvenanceV2, ArchiveProvenanceV3,
    ArchiveReader, ArchiveStats, ArchiveWriter, ArchiveWriterConfig, BlockNotification,
    BucketHeader, ChainMismatchPolicy, Consumption, EntryRecord, EpochMeta, FORMAT_VERSION_V2,
    RuntimeSegmentProvenance, SlotKind, SlotVisitor, WriteVersionNormalization,
};

/// Counters from one verified multi-segment assembly.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MultiSegmentMergeStats {
    /// Number of source archives consumed.
    pub source_archives: u64,
    /// Number of decoded source slots written to the destination.
    pub slots_merged: u64,
    /// Number of account-update write versions checked and rebased.
    pub account_updates_rebased: u64,
    /// Destination writer counters.
    pub output: ArchiveStats,
}

/// One already-open runtime-segment archive plus the authenticated SHA-256 and
/// terminal last blockhash recorded by its durable runtime evidence.
///
/// The merger re-measures this exact handle immediately before decoding it.
/// Binding the digest to the handle (instead of validating a path and opening
/// it later) prevents a pathname replacement from swapping in different
/// archive bytes between evidence validation and assembly. The terminal hash
/// binds the decoded source chain to the runtime checkpoint even for the final
/// segment, which has no outgoing V3 handoff.
#[derive(Debug)]
pub struct RuntimeSegmentSource<R> {
    reader: R,
    expected_sha256: [u8; 32],
    expected_terminal_last_blockhash: Hash,
}

impl<R> RuntimeSegmentSource<R> {
    /// Binds an opened source to its archive digest and independently observed
    /// terminal runtime checkpoint.
    pub fn new(
        reader: R,
        expected_sha256: [u8; 32],
        expected_terminal_last_blockhash: Hash,
    ) -> Self {
        Self {
            reader,
            expected_sha256,
            expected_terminal_last_blockhash,
        }
    }
}

/// Failure from [`merge_runtime_segments`].
#[derive(Debug, thiserror::Error)]
pub enum MultiSegmentMergeError {
    #[error("invalid destination V3 provenance: {0}")]
    InvalidDestinationProvenance(#[source] ArchiveProvenanceError),
    #[error("received {actual} source archives for {expected} runtime segments")]
    SourceCountMismatch { expected: usize, actual: usize },
    #[error("source archive {index} could not be decoded: {error}")]
    SourceArchive {
        index: usize,
        #[source]
        error: ArchiveFormatError,
    },
    #[error("source archive {index} could not be hashed: {error}")]
    SourceDigestIo {
        index: usize,
        #[source]
        error: std::io::Error,
    },
    #[error(
        "source archive {index} SHA-256 does not match its authenticated evidence (expected {expected:?}, actual {actual:?})"
    )]
    SourceDigestMismatch {
        index: usize,
        expected: [u8; 32],
        actual: [u8; 32],
    },
    #[error("source archive {index} provenance could not be decoded: {error}")]
    SourceProvenance {
        index: usize,
        #[source]
        error: ArchiveProvenanceError,
    },
    #[error("source archive {index} uses Horizon format V{actual}; V2 is required")]
    SourceFormatVersion { index: usize, actual: u16 },
    #[error("source archive {index} has no provenance; single-runtime V2 is required")]
    MissingSourceProvenance { index: usize },
    #[error("source archive {index} has provenance V{actual}; single-runtime V2 is required")]
    SourceProvenanceVersion { index: usize, actual: u16 },
    #[error(
        "source archive {index} range [{actual_start}, +{actual_count}) does not match segment [{expected_start}, +{expected_count})"
    )]
    SourceRangeMismatch {
        index: usize,
        expected_start: u64,
        expected_count: u64,
        actual_start: u64,
        actual_count: u64,
    },
    #[error("source archive {index} belongs to epoch {actual}, expected epoch {expected}")]
    SourceEpochMismatch {
        index: usize,
        expected: u64,
        actual: u64,
    },
    #[error("source archive {index} epoch metadata at slot {slot} has an invalid {field} field")]
    SourceEpochMetaMismatch {
        index: usize,
        slot: u64,
        field: &'static str,
    },
    #[error("source archive {index} first block at slot {slot} has no epoch metadata")]
    MissingSourceEpochMeta { index: usize, slot: u64 },
    #[error("source archive {index} does not completely cover its declared slot range")]
    IncompleteSource { index: usize },
    #[error("source archive {index} provenance field {field} does not match V3 segment {index}")]
    SourceIdentityMismatch { index: usize, field: &'static str },
    #[error("source archive {index} genesis hash does not match destination V3 provenance")]
    SourceGenesisMismatch { index: usize },
    #[error(
        "source archive {index} transaction-metadata policy does not match destination V3 provenance"
    )]
    SourceTransactionMetadataMismatch { index: usize },
    #[error("source archive {index} bootstrap field {field} does not match V3 provenance")]
    SourceBootstrapMismatch { index: usize, field: &'static str },
    #[error(
        "source archive {index} bucket beginning at slot {slot} has PoH anchor {actual}, expected {expected}"
    )]
    PohAnchorMismatch {
        index: usize,
        slot: u64,
        expected: Hash,
        actual: Hash,
    },
    #[error(
        "source archive {index} block at slot {slot} has parent blockhash {actual}, expected {expected}"
    )]
    PohParentMismatch {
        index: usize,
        slot: u64,
        expected: Hash,
        actual: Hash,
    },
    #[error(
        "source archive {index} terminal evidence has blockhash {actual}, but V3 handoff records {expected}"
    )]
    SourceTerminalEvidenceMismatch {
        index: usize,
        expected: Hash,
        actual: Hash,
    },
    #[error(
        "source archive {index} ends with blockhash {actual}, but runtime evidence requires {expected}"
    )]
    TerminalBlockhashMismatch {
        index: usize,
        expected: Hash,
        actual: Hash,
    },
    #[error(
        "source archive {index} account update at slot {slot} has raw write version {actual} outside declared range [{start}, {end})"
    )]
    WriteVersionOutOfRange {
        index: usize,
        slot: u64,
        start: u64,
        end: u64,
        actual: u64,
    },
    #[error(
        "source archive {index} account update at slot {slot} has raw write version {actual}, expected contiguous version {expected}"
    )]
    NonContiguousWriteVersion {
        index: usize,
        slot: u64,
        expected: u64,
        actual: u64,
    },
    #[error(
        "source archive {index} raw write versions ended at {actual}, but the segment declares {expected}"
    )]
    IncompleteWriteVersionRange {
        index: usize,
        expected: u64,
        actual: u64,
    },
    #[error(
        "source archive {index} raw write version {raw} cannot be mapped into its declared archive range"
    )]
    WriteVersionMappingOverflow { index: usize, raw: u64 },
    #[error(
        "source archive {index} decoded {actual} slots, expected exactly {expected} from its header"
    )]
    DecodedSlotCountMismatch {
        index: usize,
        expected: u64,
        actual: u64,
    },
    #[error("destination archive could not be written: {0}")]
    Destination(#[source] ArchiveFormatError),
}

/// Decodes and assembles complete single-runtime V2 archives into one V3
/// archive.
///
/// `sources` must have exactly one archive per V3 runtime segment, in segment
/// order. Each source must bind the exact open archive handle to authenticated
/// digest and terminal-checkpoint evidence. For a non-final segment, its
/// terminal evidence must also equal the predecessor side of the corresponding
/// V3 handoff. The sink is returned only after every source and the completed
/// destination have passed validation. As with [`super::reencode_archive`], a
/// caller writing a file should use a distinct temporary path because an error
/// can leave an unpublished partial sink behind.
pub fn merge_runtime_segments<R, W>(
    sources: Vec<RuntimeSegmentSource<R>>,
    sink: W,
    writer_config: ArchiveWriterConfig,
    provenance: &ArchiveProvenanceV3,
) -> Result<(W, MultiSegmentMergeStats), MultiSegmentMergeError>
where
    R: Read + Seek,
    W: Write,
{
    provenance
        .validate()
        .map_err(MultiSegmentMergeError::InvalidDestinationProvenance)?;
    if sources.len() != provenance.runtime_segments.len() {
        return Err(MultiSegmentMergeError::SourceCountMismatch {
            expected: provenance.runtime_segments.len(),
            actual: sources.len(),
        });
    }

    let destination = ArchiveProvenance::V3(provenance.clone());
    let requested_end = provenance
        .requested_slot_start
        .checked_add(provenance.requested_slot_count)
        .expect("validated V3 requested range cannot overflow");
    let mut sources = sources.into_iter().enumerate();
    let (first_index, first_source) = sources
        .next()
        .expect("validated V3 contains at least two runtime segments");
    let (first_source, first_terminal_hash) = verify_source_digest(first_source, first_index)?;
    validate_source_terminal_evidence(first_index, first_terminal_hash, provenance)?;
    let mut first_reader = prepare_source(
        first_source,
        first_index,
        &provenance.runtime_segments[first_index],
        provenance,
        None,
    )?;
    let epoch = first_reader.header().epoch;
    let mut writer = ArchiveWriter::new_with_provenance(
        sink,
        epoch,
        provenance.requested_slot_start,
        requested_end - provenance.requested_slot_start,
        writer_config,
        &destination,
    )
    .map_err(MultiSegmentMergeError::Destination)?;

    let mut chain = None;
    let mut destination_epoch_written = false;
    let mut slots_merged = 0u64;
    let mut account_updates_rebased = 0u64;
    let (slots, updates) = merge_one_source(
        &mut first_reader,
        first_index,
        &provenance.runtime_segments[first_index],
        first_terminal_hash,
        &mut writer,
        &mut chain,
        epoch,
        provenance.requested_slot_start,
        provenance.requested_slot_count,
        &mut destination_epoch_written,
    )?;
    slots_merged += slots;
    account_updates_rebased += updates;

    for (index, source) in sources {
        let (source, terminal_hash) = verify_source_digest(source, index)?;
        validate_source_terminal_evidence(index, terminal_hash, provenance)?;
        let mut reader = prepare_source(
            source,
            index,
            &provenance.runtime_segments[index],
            provenance,
            Some(epoch),
        )?;
        let (slots, updates) = merge_one_source(
            &mut reader,
            index,
            &provenance.runtime_segments[index],
            terminal_hash,
            &mut writer,
            &mut chain,
            epoch,
            provenance.requested_slot_start,
            provenance.requested_slot_count,
            &mut destination_epoch_written,
        )?;
        slots_merged = slots_merged.checked_add(slots).ok_or(
            MultiSegmentMergeError::DecodedSlotCountMismatch {
                index,
                expected: provenance.requested_slot_count,
                actual: u64::MAX,
            },
        )?;
        account_updates_rebased = account_updates_rebased.checked_add(updates).ok_or(
            MultiSegmentMergeError::WriteVersionMappingOverflow {
                index,
                raw: u64::MAX,
            },
        )?;
    }

    if slots_merged != provenance.requested_slot_count {
        return Err(MultiSegmentMergeError::DecodedSlotCountMismatch {
            index: provenance.runtime_segments.len() - 1,
            expected: provenance.requested_slot_count,
            actual: slots_merged,
        });
    }
    let (sink, output) = writer
        .finish()
        .map_err(MultiSegmentMergeError::Destination)?;
    Ok((
        sink,
        MultiSegmentMergeStats {
            source_archives: provenance.runtime_segments.len() as u64,
            slots_merged,
            account_updates_rebased,
            output,
        },
    ))
}

fn verify_source_digest<R: Read + Seek>(
    mut source: RuntimeSegmentSource<R>,
    index: usize,
) -> Result<(R, Hash), MultiSegmentMergeError> {
    source
        .reader
        .seek(SeekFrom::Start(0))
        .map_err(|error| MultiSegmentMergeError::SourceDigestIo { index, error })?;
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let bytes = source
            .reader
            .read(&mut buffer)
            .map_err(|error| MultiSegmentMergeError::SourceDigestIo { index, error })?;
        if bytes == 0 {
            break;
        }
        hasher.update(&buffer[..bytes]);
    }
    let actual: [u8; 32] = hasher.finalize().into();
    if actual != source.expected_sha256 {
        return Err(MultiSegmentMergeError::SourceDigestMismatch {
            index,
            expected: source.expected_sha256,
            actual,
        });
    }
    source
        .reader
        .seek(SeekFrom::Start(0))
        .map_err(|error| MultiSegmentMergeError::SourceDigestIo { index, error })?;
    Ok((source.reader, source.expected_terminal_last_blockhash))
}

fn validate_source_terminal_evidence(
    index: usize,
    terminal_hash: Hash,
    destination: &ArchiveProvenanceV3,
) -> Result<(), MultiSegmentMergeError> {
    if let Some(handoff) = destination.handoffs.get(index) {
        let expected = handoff.predecessor.last_blockhash;
        if terminal_hash != expected {
            return Err(MultiSegmentMergeError::SourceTerminalEvidenceMismatch {
                index,
                expected,
                actual: terminal_hash,
            });
        }
    }
    Ok(())
}

fn prepare_source<R: Read + Seek>(
    source: R,
    index: usize,
    segment: &RuntimeSegmentProvenance,
    destination: &ArchiveProvenanceV3,
    expected_epoch: Option<u64>,
) -> Result<ArchiveReader<R>, MultiSegmentMergeError> {
    let mut reader = ArchiveReader::open(source)
        .map_err(|error| MultiSegmentMergeError::SourceArchive { index, error })?;
    let header = reader.header().clone();
    if header.format_version != FORMAT_VERSION_V2 {
        return Err(MultiSegmentMergeError::SourceFormatVersion {
            index,
            actual: header.format_version,
        });
    }
    if header.slot_start != segment.slot_start || header.slot_count != segment.slot_count {
        return Err(MultiSegmentMergeError::SourceRangeMismatch {
            index,
            expected_start: segment.slot_start,
            expected_count: segment.slot_count,
            actual_start: header.slot_start,
            actual_count: header.slot_count,
        });
    }
    if let Some(expected) = expected_epoch
        && header.epoch != expected
    {
        return Err(MultiSegmentMergeError::SourceEpochMismatch {
            index,
            expected,
            actual: header.epoch,
        });
    }
    if !reader
        .has_complete_slot_coverage()
        .map_err(|error| MultiSegmentMergeError::SourceArchive { index, error })?
    {
        return Err(MultiSegmentMergeError::IncompleteSource { index });
    }

    let source_provenance = reader
        .provenance()
        .map_err(|error| MultiSegmentMergeError::SourceProvenance { index, error })?
        .ok_or(MultiSegmentMergeError::MissingSourceProvenance { index })?;
    let source_provenance = match source_provenance {
        ArchiveProvenance::V2(value) => value,
        ArchiveProvenance::V1(_) => {
            return Err(MultiSegmentMergeError::SourceProvenanceVersion {
                index,
                actual: ARCHIVE_PROVENANCE_VERSION_V1,
            });
        }
        ArchiveProvenance::V3(_) => {
            return Err(MultiSegmentMergeError::SourceProvenanceVersion {
                index,
                actual: ARCHIVE_PROVENANCE_VERSION_V3,
            });
        }
    };
    validate_source_provenance(index, &source_provenance, segment, destination)?;

    reader.verify_chain = true;
    reader.chain_mismatch_policy = ChainMismatchPolicy::Reject;
    Ok(reader)
}

fn validate_source_provenance(
    index: usize,
    source: &ArchiveProvenanceV2,
    segment: &RuntimeSegmentProvenance,
    destination: &ArchiveProvenanceV3,
) -> Result<(), MultiSegmentMergeError> {
    let base = &source.base;
    for (field, matches) in [
        (
            "generation_profile",
            base.generation_profile == segment.generation_profile,
        ),
        (
            "runtime_profile",
            base.runtime_profile == segment.runtime_profile,
        ),
        (
            "runtime_admission",
            base.runtime_admission == segment.runtime_admission,
        ),
        (
            "runtime_revision",
            base.runtime_revision == segment.runtime_revision,
        ),
        (
            "runtime_toolchain",
            base.runtime_toolchain == segment.runtime_toolchain,
        ),
        (
            "worker_executable_sha256",
            Some(source.worker_executable_sha256) == segment.worker_executable_sha256,
        ),
    ] {
        if !matches {
            return Err(MultiSegmentMergeError::SourceIdentityMismatch { index, field });
        }
    }
    if base.genesis_hash != destination.genesis_hash {
        return Err(MultiSegmentMergeError::SourceGenesisMismatch { index });
    }
    if base.transaction_metadata != destination.transaction_metadata {
        return Err(MultiSegmentMergeError::SourceTransactionMetadataMismatch { index });
    }

    let (expected_kind, expected_slot, expected_hash) = if index == 0 {
        (
            destination.bootstrap_state_kind,
            destination.bootstrap_state.slot,
            destination.bootstrap_state.hash,
        )
    } else {
        let handoff = &destination.handoffs[index - 1];
        (
            handoff.successor_bootstrap_kind,
            handoff.successor.slot,
            handoff.successor.accounts_hash,
        )
    };
    for (field, matches) in [
        ("kind", base.bootstrap_state_kind == expected_kind),
        ("slot", base.bootstrap_slot == expected_slot),
        ("state_hash", base.bootstrap_state_hash == expected_hash),
    ] {
        if !matches {
            return Err(MultiSegmentMergeError::SourceBootstrapMismatch { index, field });
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)] // one explicit accumulator per cross-source invariant
fn merge_one_source<R, W>(
    reader: &mut ArchiveReader<R>,
    index: usize,
    segment: &RuntimeSegmentProvenance,
    expected_terminal_hash: Hash,
    writer: &mut ArchiveWriter<W>,
    chain: &mut Option<Hash>,
    expected_epoch: u64,
    destination_slot_start: u64,
    destination_slot_count: u64,
    destination_epoch_written: &mut bool,
) -> Result<(u64, u64), MultiSegmentMergeError>
where
    R: Read + Seek,
    W: Write,
{
    let mut visitor = MergeVisitor::new(
        index,
        segment,
        expected_epoch,
        destination_slot_start,
        destination_slot_count,
        destination_epoch_written,
        writer,
        chain,
    );
    let mut slots = 0u64;
    for bucket in 0..reader.bucket_count() {
        let visited = reader
            .read_bucket_with_header(bucket, &mut visitor, |header, visitor| {
                visitor.on_bucket_header(header);
                Ok(())
            })
            .map_err(|error| MultiSegmentMergeError::SourceArchive { index, error })?;
        if let Some(error) = visitor.error.take() {
            return Err(error);
        }
        slots =
            slots
                .checked_add(visited)
                .ok_or(MultiSegmentMergeError::DecodedSlotCountMismatch {
                    index,
                    expected: segment.slot_count,
                    actual: u64::MAX,
                })?;
    }
    if slots != segment.slot_count {
        return Err(MultiSegmentMergeError::DecodedSlotCountMismatch {
            index,
            expected: segment.slot_count,
            actual: slots,
        });
    }
    visitor
        .finish(expected_terminal_hash)
        .map(|updates| (slots, updates))
}

struct MergeVisitor<'a, W: Write> {
    source_index: usize,
    normalization: WriteVersionNormalization,
    next_raw_write_version: u64,
    /// Source wire order groups writes by semantic owner. AccountsDB's raw
    /// write versions can therefore be a permutation within one slot (for
    /// example, failed-transaction fee debits are physically stored after
    /// later successful transactions). Keep only the current slot's versions
    /// and validate their exact sorted run at the slot boundary.
    slot_write_versions: Vec<u64>,
    account_updates: u64,
    current_slot: u64,
    current_is_block: bool,
    first_block_slot: Option<u64>,
    source_epoch_seen: bool,
    source_slot_start: u64,
    source_slot_count: u64,
    expected_epoch: u64,
    destination_slot_start: u64,
    destination_slot_count: u64,
    destination_epoch_written: &'a mut bool,
    writer: &'a mut ArchiveWriter<W>,
    chain: &'a mut Option<Hash>,
    error: Option<MultiSegmentMergeError>,
}

impl<'a, W: Write> MergeVisitor<'a, W> {
    #[allow(clippy::too_many_arguments)] // mirrors the independently verified merge invariants
    fn new(
        source_index: usize,
        segment: &RuntimeSegmentProvenance,
        expected_epoch: u64,
        destination_slot_start: u64,
        destination_slot_count: u64,
        destination_epoch_written: &'a mut bool,
        writer: &'a mut ArchiveWriter<W>,
        chain: &'a mut Option<Hash>,
    ) -> Self {
        let normalization = segment.write_versions;
        Self {
            source_index,
            normalization,
            next_raw_write_version: normalization.worker_start,
            slot_write_versions: Vec::new(),
            account_updates: 0,
            current_slot: 0,
            current_is_block: false,
            first_block_slot: None,
            source_epoch_seen: false,
            source_slot_start: segment.slot_start,
            source_slot_count: segment.slot_count,
            expected_epoch,
            destination_slot_start,
            destination_slot_count,
            destination_epoch_written,
            writer,
            chain,
            error: None,
        }
    }

    fn epoch_meta_mismatch(&mut self, field: &'static str) {
        if self.error.is_none() {
            self.error = Some(MultiSegmentMergeError::SourceEpochMetaMismatch {
                index: self.source_index,
                slot: self.current_slot,
                field,
            });
        }
    }

    fn record_destination(&mut self, result: Result<(), ArchiveFormatError>) {
        if self.error.is_none()
            && let Err(error) = result
        {
            self.error = Some(MultiSegmentMergeError::Destination(error));
        }
    }

    fn on_bucket_header(&mut self, header: &BucketHeader) {
        if self.error.is_some() {
            return;
        }
        match *self.chain {
            Some(expected) if header.poh_start_hash != expected => {
                self.error = Some(MultiSegmentMergeError::PohAnchorMismatch {
                    index: self.source_index,
                    slot: header.first_slot,
                    expected,
                    actual: header.poh_start_hash,
                });
            }
            Some(_) => {}
            None => {
                let result = self
                    .writer
                    .preserve_initial_poh_anchor(header.first_slot, header.poh_start_hash);
                self.record_destination(result);
                if self.error.is_none() {
                    *self.chain = Some(header.poh_start_hash);
                }
            }
        }
    }

    fn accept_write_version(&mut self, raw: u64, slot: u64) -> Option<u64> {
        if self.error.is_some() {
            return None;
        }
        if raw < self.normalization.worker_start || raw >= self.normalization.worker_end_exclusive {
            self.error = Some(MultiSegmentMergeError::WriteVersionOutOfRange {
                index: self.source_index,
                slot,
                start: self.normalization.worker_start,
                end: self.normalization.worker_end_exclusive,
                actual: raw,
            });
            return None;
        }
        let Some(mapped) = self.normalization.normalize(raw) else {
            self.error = Some(MultiSegmentMergeError::WriteVersionMappingOverflow {
                index: self.source_index,
                raw,
            });
            return None;
        };
        self.slot_write_versions.push(raw);
        self.account_updates += 1;
        Some(mapped)
    }

    fn finish_slot_write_versions(&mut self) {
        if self.error.is_some() || self.slot_write_versions.is_empty() {
            return;
        }
        self.slot_write_versions.sort_unstable();
        for index in 0..self.slot_write_versions.len() {
            let raw = self.slot_write_versions[index];
            if raw != self.next_raw_write_version {
                self.error = Some(MultiSegmentMergeError::NonContiguousWriteVersion {
                    index: self.source_index,
                    slot: self.current_slot,
                    expected: self.next_raw_write_version,
                    actual: raw,
                });
                break;
            }
            // `raw < worker_end_exclusive` was checked on receipt, so this
            // cannot overflow even when the declared end is `u64::MAX`.
            self.next_raw_write_version = raw + 1;
        }
        self.slot_write_versions.clear();
    }

    fn finish(mut self, expected_terminal_hash: Hash) -> Result<u64, MultiSegmentMergeError> {
        self.finish_slot_write_versions();
        if let Some(error) = self.error {
            return Err(error);
        }
        if self.next_raw_write_version != self.normalization.worker_end_exclusive {
            return Err(MultiSegmentMergeError::IncompleteWriteVersionRange {
                index: self.source_index,
                expected: self.normalization.worker_end_exclusive,
                actual: self.next_raw_write_version,
            });
        }
        let actual = self.chain.unwrap_or_default();
        if actual != expected_terminal_hash {
            return Err(MultiSegmentMergeError::TerminalBlockhashMismatch {
                index: self.source_index,
                expected: expected_terminal_hash,
                actual,
            });
        }
        Ok(self.account_updates)
    }
}

impl<W: Write> SlotVisitor for MergeVisitor<'_, W> {
    fn on_slot_start(&mut self, slot: u64, kind: SlotKind) {
        self.finish_slot_write_versions();
        if self.error.is_some() {
            return;
        }
        self.current_slot = slot;
        self.current_is_block = kind == SlotKind::Block;
        if self.current_is_block && self.first_block_slot.is_none() {
            self.first_block_slot = Some(slot);
        }
        let result = match kind {
            SlotKind::Skipped => self.writer.write_skipped_slot(slot),
            SlotKind::Block => self.writer.begin_slot(slot),
        };
        self.record_destination(result);
    }

    fn on_epoch(&mut self, meta: &EpochMeta) {
        if self.error.is_some() {
            return;
        }
        if !self.current_is_block {
            self.epoch_meta_mismatch("placement");
            return;
        }
        if self.source_epoch_seen {
            self.epoch_meta_mismatch("duplicate");
            return;
        }
        if self.first_block_slot != Some(self.current_slot) {
            self.epoch_meta_mismatch("placement");
            return;
        }
        for (field, matches) in [
            ("epoch", meta.epoch == self.expected_epoch),
            ("start_slot", meta.start_slot == self.source_slot_start),
            ("slot_count", meta.slot_count == self.source_slot_count),
            (
                "first_block_slot",
                meta.first_block_slot == self.current_slot,
            ),
        ] {
            if !matches {
                self.epoch_meta_mismatch(field);
                return;
            }
        }
        for (update, _) in meta.updates.iter() {
            if self
                .accept_write_version(update.write_version, self.current_slot)
                .is_none()
            {
                return;
            }
        }
        let normalization = self.normalization;
        if *self.destination_epoch_written {
            // Every focused recorder emits segment-local epoch metadata. Its
            // nested writes still happened before that segment's first
            // transaction, so retain their exact stream position as ordinary
            // pre-transaction updates while suppressing the duplicate header.
            for (update, data) in meta.updates.iter() {
                let mapped = AccountUpdateView {
                    pubkey: update.pubkey,
                    lamports: update.lamports,
                    owner: update.owner,
                    executable: update.executable,
                    rent_epoch: update.rent_epoch,
                    write_version: normalization
                        .normalize(update.write_version)
                        .expect("epoch write version was validated before re-encoding"),
                    data,
                };
                let result = self.writer.write_reencoded_pre_update(&mapped);
                self.record_destination(result);
                if self.error.is_some() {
                    return;
                }
            }
        } else {
            let result = self
                .writer
                .write_normalized_epoch_meta_with_write_version_map(
                    meta,
                    self.expected_epoch,
                    self.destination_slot_start,
                    self.destination_slot_count,
                    self.current_slot,
                    |raw| {
                        normalization
                            .normalize(raw)
                            .expect("epoch write version was validated before re-encoding")
                    },
                );
            self.record_destination(result);
            if self.error.is_none() {
                *self.destination_epoch_written = true;
            }
        }
        self.source_epoch_seen = true;
    }

    fn on_pre_account_update(&mut self, slot: u64, update: &AccountUpdateView<'_>) {
        let Some(write_version) = self.accept_write_version(update.write_version, slot) else {
            return;
        };
        let mapped = AccountUpdateView {
            write_version,
            ..*update
        };
        let result = self.writer.write_reencoded_pre_update(&mapped);
        self.record_destination(result);
    }

    fn on_transaction(&mut self, slot: u64, _tx_index: u32, tx: &Transaction) {
        if self.error.is_some() {
            return;
        }
        for (update, _) in tx.iter_account_updates() {
            if self
                .accept_write_version(update.write_version, slot)
                .is_none()
            {
                return;
            }
        }
        let normalization = self.normalization;
        let result = self
            .writer
            .write_transaction_with_write_version_map(tx, |raw| {
                normalization
                    .normalize(raw)
                    .expect("transaction write version was validated before re-encoding")
            });
        self.record_destination(result);
    }

    fn on_post_account_update(&mut self, slot: u64, update: &AccountUpdateView<'_>) {
        let Some(write_version) = self.accept_write_version(update.write_version, slot) else {
            return;
        };
        let mapped = AccountUpdateView {
            write_version,
            ..*update
        };
        let result = self.writer.write_reencoded_post_update(&mapped);
        self.record_destination(result);
    }

    fn on_block(&mut self, notification: &BlockNotification, entries: &[EntryRecord]) {
        self.finish_slot_write_versions();
        if self.error.is_some() {
            return;
        }
        let BlockNotification::Block(meta) = notification else {
            return;
        };
        if self.first_block_slot == Some(meta.slot) && !self.source_epoch_seen {
            self.error = Some(MultiSegmentMergeError::MissingSourceEpochMeta {
                index: self.source_index,
                slot: meta.slot,
            });
            return;
        }
        let Some(expected) = *self.chain else {
            self.error = Some(MultiSegmentMergeError::PohParentMismatch {
                index: self.source_index,
                slot: meta.slot,
                expected: Hash::default(),
                actual: meta.parent_blockhash,
            });
            return;
        };
        if meta.parent_blockhash != expected {
            self.error = Some(MultiSegmentMergeError::PohParentMismatch {
                index: self.source_index,
                slot: meta.slot,
                expected,
                actual: meta.parent_blockhash,
            });
            return;
        }
        *self.chain = Some(meta.blockhash);
        let result = self.writer.end_reencoded_slot(meta, entries);
        self.record_destination(result);
    }

    fn consumption(&self) -> Consumption {
        Consumption::all().without_block_account_update_arenas()
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use solana_address::Address;

    use crate::{
        account_updates::AccountUpdateView,
        archive::{
            AccountsHashKind, ArchiveProvenanceV1, BootstrapStateKind, Compression,
            RuntimeAdmission, RuntimeHandoffProvenance, RuntimeStateCheckpoint, StateCommitment,
            StateCommitmentKind, TransactionMetadataPolicy,
        },
        block_metas::BlockMeta,
    };

    use super::*;

    const EPOCH: u64 = 7;
    const FIRST_SLOT: u64 = 100;

    fn hash(byte: u8) -> Hash {
        Hash::new_from_array([byte; 32])
    }

    fn segment(
        slot_start: u64,
        generation_profile: &str,
        runtime_profile: &str,
        digest: [u8; 32],
        write_versions: WriteVersionNormalization,
    ) -> RuntimeSegmentProvenance {
        RuntimeSegmentProvenance {
            slot_start,
            slot_count: 1,
            generation_profile: generation_profile.into(),
            runtime_profile: runtime_profile.into(),
            runtime_admission: RuntimeAdmission::Candidate,
            runtime_revision: format!("{runtime_profile}-revision"),
            runtime_toolchain: format!("{runtime_profile}-toolchain"),
            worker_executable_sha256: Some(digest),
            write_versions,
        }
    }

    fn fixture_provenance() -> ArchiveProvenanceV3 {
        let predecessor = RuntimeStateCheckpoint {
            slot: FIRST_SLOT,
            bank_hash: hash(0x31),
            accounts_hash_kind: AccountsHashKind::LegacyAccountsHash,
            accounts_hash: hash(0x32),
            last_blockhash: hash(0x20),
            capitalization: 1_000_000,
            transaction_count: 42,
            tick_height: 64,
            slot_complete: true,
            next_write_version: 14,
        };
        ArchiveProvenanceV3 {
            assembly_profile: "test/verified-runtime-merge-v1".into(),
            genesis_hash: hash(0x90),
            bootstrap_state_kind: BootstrapStateKind::SnapshotArchive,
            bootstrap_state: StateCommitment {
                slot: FIRST_SLOT - 1,
                kind: StateCommitmentKind::LegacyAccountsHash,
                hash: hash(0x91),
            },
            requested_slot_start: FIRST_SLOT,
            requested_slot_count: 2,
            transaction_metadata: TransactionMetadataPolicy::observed(),
            runtime_segments: vec![
                segment(
                    FIRST_SLOT,
                    "test/source-a",
                    "solana-v1.0.7",
                    [0xa1; 32],
                    WriteVersionNormalization {
                        worker_start: 10,
                        worker_end_exclusive: 14,
                        archive_start: 1_000,
                    },
                ),
                segment(
                    FIRST_SLOT + 1,
                    "test/source-b",
                    "solana-v1.0.8",
                    [0xb2; 32],
                    WriteVersionNormalization {
                        worker_start: 200,
                        worker_end_exclusive: 201,
                        archive_start: 1_004,
                    },
                ),
            ],
            handoffs: vec![RuntimeHandoffProvenance {
                boundary_slot: FIRST_SLOT + 1,
                predecessor,
                successor: RuntimeStateCheckpoint {
                    // Snapshot-local raw cursor intentionally differs.
                    next_write_version: 200,
                    ..predecessor
                },
                successor_bootstrap_kind: BootstrapStateKind::SnapshotArchive,
                successor_bootstrap_archive_sha256: [0xc3; 32],
                successor_bootstrap_write_count: 0,
            }],
        }
    }

    fn source_provenance(destination: &ArchiveProvenanceV3, index: usize) -> ArchiveProvenanceV2 {
        let segment = &destination.runtime_segments[index];
        let (bootstrap_state_kind, bootstrap_slot, bootstrap_state_hash) = if index == 0 {
            (
                destination.bootstrap_state_kind,
                destination.bootstrap_state.slot,
                destination.bootstrap_state.hash,
            )
        } else {
            let handoff = &destination.handoffs[index - 1];
            (
                handoff.successor_bootstrap_kind,
                handoff.successor.slot,
                handoff.successor.accounts_hash,
            )
        };
        ArchiveProvenanceV2 {
            base: ArchiveProvenanceV1 {
                generation_profile: segment.generation_profile.clone(),
                runtime_profile: segment.runtime_profile.clone(),
                runtime_admission: segment.runtime_admission,
                runtime_revision: segment.runtime_revision.clone(),
                runtime_toolchain: segment.runtime_toolchain.clone(),
                genesis_hash: destination.genesis_hash,
                bootstrap_state_kind,
                bootstrap_slot,
                bootstrap_state_hash,
                requested_slot_start: segment.slot_start,
                requested_slot_count: segment.slot_count,
                transaction_metadata: destination.transaction_metadata,
            },
            worker_executable_sha256: segment.worker_executable_sha256.unwrap(),
        }
    }

    fn update(raw: u64, tag: u8) -> AccountUpdateView<'static> {
        AccountUpdateView {
            pubkey: Address::new_from_array([tag; 32]),
            lamports: u64::from(tag),
            owner: Address::new_from_array([0xee; 32]),
            executable: false,
            rent_epoch: u64::MAX,
            write_version: raw,
            data: b"state",
        }
    }

    fn write_source(
        destination: &ArchiveProvenanceV3,
        index: usize,
        raw_write_versions: &[u64],
        poh_anchor: Hash,
        blockhash: Hash,
        provenance: Option<ArchiveProvenance>,
    ) -> Vec<u8> {
        write_source_with_parent(
            destination,
            index,
            raw_write_versions,
            poh_anchor,
            poh_anchor,
            blockhash,
            provenance,
        )
    }

    fn write_source_with_parent(
        destination: &ArchiveProvenanceV3,
        index: usize,
        raw_write_versions: &[u64],
        poh_anchor: Hash,
        parent_blockhash: Hash,
        blockhash: Hash,
        provenance: Option<ArchiveProvenance>,
    ) -> Vec<u8> {
        write_source_with_epoch_fields(
            destination,
            index,
            raw_write_versions,
            poh_anchor,
            parent_blockhash,
            blockhash,
            provenance,
            Some((
                EPOCH,
                destination.runtime_segments[index].slot_start,
                destination.runtime_segments[index].slot_count,
                destination.runtime_segments[index].slot_start,
            )),
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn write_source_with_epoch_fields(
        destination: &ArchiveProvenanceV3,
        index: usize,
        raw_write_versions: &[u64],
        poh_anchor: Hash,
        parent_blockhash: Hash,
        blockhash: Hash,
        provenance: Option<ArchiveProvenance>,
        epoch_fields: Option<(u64, u64, u64, u64)>,
    ) -> Vec<u8> {
        let segment = &destination.runtime_segments[index];
        let config = ArchiveWriterConfig {
            bucket_slots: 1,
            compression: Compression::None,
            ..ArchiveWriterConfig::default()
        };
        let mut writer = match provenance {
            Some(provenance) => ArchiveWriter::new_with_provenance(
                Vec::new(),
                EPOCH,
                segment.slot_start,
                segment.slot_count,
                config,
                &provenance,
            )
            .unwrap(),
            None => ArchiveWriter::new(
                Vec::new(),
                EPOCH,
                segment.slot_start,
                segment.slot_count,
                config,
            )
            .unwrap(),
        };
        writer
            .preserve_initial_poh_anchor(segment.slot_start, poh_anchor)
            .unwrap();
        writer.begin_slot(segment.slot_start).unwrap();

        let mut first_non_epoch_update = 0;
        if let Some((epoch_number, start_slot, slot_count, first_block_slot)) = epoch_fields {
            let mut epoch = EpochMeta::new_boxed();
            epoch.epoch = epoch_number;
            epoch.start_slot = start_slot;
            epoch.slot_count = slot_count;
            epoch.first_block_slot = first_block_slot;
            if let Some(&raw) = raw_write_versions.first() {
                epoch.updates.push(&update(raw, 1)).unwrap();
                first_non_epoch_update = 1;
            }
            writer.write_epoch_meta(&epoch).unwrap();
        }

        if index == 0 {
            if let Some(&raw) = raw_write_versions.get(first_non_epoch_update) {
                writer.write_orphan_update(&update(raw, 2)).unwrap();
            }
            if let Some(&raw) = raw_write_versions.get(first_non_epoch_update + 1) {
                let mut transaction = Transaction::new_boxed();
                transaction.push_account_update(&update(raw, 3)).unwrap();
                writer.write_transaction(&transaction).unwrap();
            }
            for (offset, &raw) in raw_write_versions
                .iter()
                .enumerate()
                .skip(first_non_epoch_update + 2)
            {
                writer
                    .write_orphan_update(&update(raw, 4 + offset as u8))
                    .unwrap();
            }
        } else {
            for (offset, &raw) in raw_write_versions
                .iter()
                .enumerate()
                .skip(first_non_epoch_update)
            {
                writer
                    .write_orphan_update(&update(raw, 20 + offset as u8))
                    .unwrap();
            }
        }

        let mut meta = BlockMeta::new_boxed();
        meta.slot = segment.slot_start;
        meta.parent_slot = segment.slot_start - 1;
        meta.parent_blockhash = parent_blockhash;
        meta.blockhash = blockhash;
        writer.end_slot(&meta, &[]).unwrap();
        writer.finish().unwrap().0
    }

    fn fixture_sources(destination: &ArchiveProvenanceV3) -> Vec<Vec<u8>> {
        vec![
            write_source(
                destination,
                0,
                &[10, 11, 12, 13],
                hash(0x10),
                hash(0x20),
                Some(source_provenance(destination, 0).into()),
            ),
            write_source(
                destination,
                1,
                &[200],
                hash(0x20),
                hash(0x30),
                Some(source_provenance(destination, 1).into()),
            ),
        ]
    }

    fn fixture_terminal_hash(destination: &ArchiveProvenanceV3, index: usize) -> Hash {
        destination
            .handoffs
            .get(index)
            .map_or_else(|| hash(0x30), |handoff| handoff.predecessor.last_blockhash)
    }

    fn bind_sources(
        sources: Vec<Vec<u8>>,
        provenance: &ArchiveProvenanceV3,
    ) -> Vec<RuntimeSegmentSource<Cursor<Vec<u8>>>> {
        sources
            .into_iter()
            .enumerate()
            .map(|(index, bytes)| {
                let digest: [u8; 32] = Sha256::digest(&bytes).into();
                RuntimeSegmentSource::new(
                    Cursor::new(bytes),
                    digest,
                    fixture_terminal_hash(provenance, index),
                )
            })
            .collect()
    }

    fn write_skipped_source(
        destination: &ArchiveProvenanceV3,
        index: usize,
        poh_anchor: Hash,
    ) -> Vec<u8> {
        let segment = &destination.runtime_segments[index];
        let mut writer = ArchiveWriter::new_with_provenance(
            Vec::new(),
            EPOCH,
            segment.slot_start,
            segment.slot_count,
            ArchiveWriterConfig {
                bucket_slots: 1,
                compression: Compression::None,
                ..ArchiveWriterConfig::default()
            },
            &ArchiveProvenance::V2(source_provenance(destination, index)),
        )
        .unwrap();
        writer
            .preserve_initial_poh_anchor(segment.slot_start, poh_anchor)
            .unwrap();
        writer.write_skipped_slot(segment.slot_start).unwrap();
        writer.finish().unwrap().0
    }

    fn merge(
        sources: Vec<Vec<u8>>,
        provenance: &ArchiveProvenanceV3,
    ) -> Result<(Vec<u8>, MultiSegmentMergeStats), MultiSegmentMergeError> {
        let sources = bind_sources(sources, provenance);
        merge_runtime_segments(
            sources,
            Vec::new(),
            ArchiveWriterConfig {
                bucket_slots: 1,
                compression: Compression::None,
                ..ArchiveWriterConfig::default()
            },
            provenance,
        )
    }

    #[derive(Default)]
    struct WriteVersionCollector {
        versions: Vec<u64>,
        epochs: Vec<(u64, u64, u64, u64)>,
    }

    impl SlotVisitor for WriteVersionCollector {
        fn on_epoch(&mut self, meta: &EpochMeta) {
            self.epochs.push((
                meta.epoch,
                meta.start_slot,
                meta.slot_count,
                meta.first_block_slot,
            ));
            self.versions
                .extend(meta.updates.iter().map(|(update, _)| update.write_version));
        }

        fn on_pre_account_update(&mut self, _slot: u64, update: &AccountUpdateView<'_>) {
            self.versions.push(update.write_version);
        }

        fn on_transaction(&mut self, _slot: u64, _tx_index: u32, tx: &Transaction) {
            self.versions.extend(
                tx.iter_account_updates()
                    .map(|(update, _)| update.write_version),
            );
        }

        fn on_post_account_update(&mut self, _slot: u64, update: &AccountUpdateView<'_>) {
            self.versions.push(update.write_version);
        }
    }

    #[test]
    fn verified_merge_rebases_every_update_phase_and_records_v3() {
        let provenance = fixture_provenance();
        let (output, stats) = merge(fixture_sources(&provenance), &provenance).unwrap();
        assert_eq!(stats.source_archives, 2);
        assert_eq!(stats.slots_merged, 2);
        assert_eq!(stats.account_updates_rebased, 5);

        let mut reader = ArchiveReader::open(Cursor::new(output)).unwrap();
        assert_eq!(reader.provenance().unwrap(), Some(provenance.into()));
        reader.verify_chain = true;
        reader.chain_mismatch_policy = ChainMismatchPolicy::Reject;
        let mut collector = WriteVersionCollector::default();
        for bucket in 0..reader.bucket_count() {
            reader.read_bucket(bucket, &mut collector).unwrap();
        }
        assert_eq!(collector.epochs, vec![(EPOCH, FIRST_SLOT, 2, FIRST_SLOT)]);
        assert_eq!(collector.versions, vec![1_000, 1_001, 1_002, 1_003, 1_004]);
    }

    #[test]
    fn merge_normalizes_segment_epoch_meta_and_rejects_invalid_local_semantics() {
        let provenance = fixture_provenance();

        for (epoch_fields, field) in [
            ((EPOCH + 1, FIRST_SLOT, 1, FIRST_SLOT), "epoch"),
            ((EPOCH, FIRST_SLOT + 1, 1, FIRST_SLOT), "start_slot"),
            ((EPOCH, FIRST_SLOT, 2, FIRST_SLOT), "slot_count"),
            ((EPOCH, FIRST_SLOT, 1, FIRST_SLOT + 1), "first_block_slot"),
        ] {
            let mut sources = fixture_sources(&provenance);
            sources[0] = write_source_with_epoch_fields(
                &provenance,
                0,
                &[10, 11, 12, 13],
                hash(0x10),
                hash(0x10),
                hash(0x20),
                Some(source_provenance(&provenance, 0).into()),
                Some(epoch_fields),
            );
            assert!(matches!(
                merge(sources, &provenance),
                Err(MultiSegmentMergeError::SourceEpochMetaMismatch {
                    index: 0,
                    field: actual,
                    ..
                }) if actual == field
            ));
        }

        let mut sources = fixture_sources(&provenance);
        sources[0] = write_source_with_epoch_fields(
            &provenance,
            0,
            &[10, 11, 12, 13],
            hash(0x10),
            hash(0x10),
            hash(0x20),
            Some(source_provenance(&provenance, 0).into()),
            None,
        );
        assert!(matches!(
            merge(sources, &provenance),
            Err(MultiSegmentMergeError::MissingSourceEpochMeta {
                index: 0,
                slot: FIRST_SLOT
            })
        ));
    }

    #[test]
    fn merge_accepts_per_slot_write_version_permutations_and_preserves_wire_order() {
        let provenance = fixture_provenance();
        let mut sources = fixture_sources(&provenance);
        sources[0] = write_source(
            &provenance,
            0,
            &[10, 13, 11, 12],
            hash(0x10),
            hash(0x20),
            Some(source_provenance(&provenance, 0).into()),
        );

        let (output, stats) = merge(sources, &provenance).unwrap();
        assert_eq!(stats.account_updates_rebased, 5);
        let mut reader = ArchiveReader::open(Cursor::new(output)).unwrap();
        let mut collector = WriteVersionCollector::default();
        for bucket in 0..reader.bucket_count() {
            reader.read_bucket(bucket, &mut collector).unwrap();
        }
        assert_eq!(collector.versions, vec![1_000, 1_003, 1_001, 1_002, 1_004]);
    }

    #[test]
    fn merge_allows_leading_all_skipped_segment_and_anchors_first_real_block() {
        let mut provenance = fixture_provenance();
        provenance.runtime_segments[0]
            .write_versions
            .worker_end_exclusive = 10;
        provenance.runtime_segments[1].write_versions.archive_start = 1_000;
        provenance.handoffs[0].predecessor.last_blockhash = hash(0x10);
        provenance.handoffs[0].predecessor.next_write_version = 10;
        provenance.handoffs[0].successor.last_blockhash = hash(0x10);

        let sources = vec![
            write_skipped_source(&provenance, 0, hash(0x10)),
            write_source(
                &provenance,
                1,
                &[200],
                hash(0x10),
                hash(0x30),
                Some(source_provenance(&provenance, 1).into()),
            ),
        ];
        let (output, stats) = merge(sources, &provenance).unwrap();
        assert_eq!(stats.account_updates_rebased, 1);

        let mut reader = ArchiveReader::open(Cursor::new(output)).unwrap();
        reader.verify_chain = true;
        let mut collector = WriteVersionCollector::default();
        for bucket in 0..reader.bucket_count() {
            reader.read_bucket(bucket, &mut collector).unwrap();
        }
        assert_eq!(
            collector.epochs,
            vec![(EPOCH, FIRST_SLOT, 2, FIRST_SLOT + 1)]
        );
        assert_eq!(collector.versions, vec![1_000]);
    }

    #[test]
    fn merge_rejects_identity_executable_range_and_cross_segment_chain_mutations() {
        let provenance = fixture_provenance();
        let sources = fixture_sources(&provenance);

        let mut wrong_identity = provenance.clone();
        wrong_identity.runtime_segments[1].runtime_revision = "tampered-revision".into();
        assert!(matches!(
            merge(sources.clone(), &wrong_identity),
            Err(MultiSegmentMergeError::SourceIdentityMismatch {
                index: 1,
                field: "runtime_revision"
            })
        ));

        let mut wrong_executable = provenance.clone();
        wrong_executable.runtime_segments[1].worker_executable_sha256 = Some([0xff; 32]);
        assert!(matches!(
            merge(sources.clone(), &wrong_executable),
            Err(MultiSegmentMergeError::SourceIdentityMismatch {
                index: 1,
                field: "worker_executable_sha256"
            })
        ));

        assert!(matches!(
            merge(vec![sources[1].clone(), sources[0].clone()], &provenance),
            Err(MultiSegmentMergeError::SourceRangeMismatch { index: 0, .. })
        ));

        let mut wrong_chain = sources;
        wrong_chain[1] = write_source(
            &provenance,
            1,
            &[200],
            hash(0x99),
            hash(0x30),
            Some(source_provenance(&provenance, 1).into()),
        );
        assert!(matches!(
            merge(wrong_chain, &provenance),
            Err(MultiSegmentMergeError::PohAnchorMismatch { index: 1, .. })
        ));

        let mut broken_internal_chain = fixture_sources(&provenance);
        broken_internal_chain[0] = write_source_with_parent(
            &provenance,
            0,
            &[10, 11, 12, 13],
            hash(0x10),
            hash(0x77),
            hash(0x20),
            Some(source_provenance(&provenance, 0).into()),
        );
        assert!(matches!(
            merge(broken_internal_chain, &provenance),
            Err(MultiSegmentMergeError::SourceArchive {
                index: 0,
                error: ArchiveFormatError::PohMismatch { slot: FIRST_SLOT }
            })
        ));
    }

    #[test]
    fn merge_binds_each_open_source_handle_to_its_authenticated_digest() {
        let provenance = fixture_provenance();
        let sources = fixture_sources(&provenance);
        let mut bound_sources = bind_sources(sources, &provenance);
        bound_sources[1].expected_sha256 = [0xff; 32];

        assert!(matches!(
            merge_runtime_segments(
                bound_sources,
                Vec::new(),
                ArchiveWriterConfig {
                    bucket_slots: 1,
                    compression: Compression::None,
                    ..ArchiveWriterConfig::default()
                },
                &provenance,
            ),
            Err(MultiSegmentMergeError::SourceDigestMismatch { index: 1, .. })
        ));
    }

    #[test]
    fn merge_binds_every_source_to_authenticated_terminal_blockhash_evidence() {
        let provenance = fixture_provenance();

        let mut wrong_handoff_evidence = bind_sources(fixture_sources(&provenance), &provenance);
        wrong_handoff_evidence[0].expected_terminal_last_blockhash = hash(0x99);
        assert!(matches!(
            merge_runtime_segments(
                wrong_handoff_evidence,
                Vec::new(),
                ArchiveWriterConfig {
                    bucket_slots: 1,
                    compression: Compression::None,
                    ..ArchiveWriterConfig::default()
                },
                &provenance,
            ),
            Err(MultiSegmentMergeError::SourceTerminalEvidenceMismatch {
                index: 0,
                expected,
                actual,
            }) if expected == hash(0x20) && actual == hash(0x99)
        ));

        let mut wrong_final_evidence = bind_sources(fixture_sources(&provenance), &provenance);
        wrong_final_evidence[1].expected_terminal_last_blockhash = hash(0x99);
        assert!(matches!(
            merge_runtime_segments(
                wrong_final_evidence,
                Vec::new(),
                ArchiveWriterConfig {
                    bucket_slots: 1,
                    compression: Compression::None,
                    ..ArchiveWriterConfig::default()
                },
                &provenance,
            ),
            Err(MultiSegmentMergeError::TerminalBlockhashMismatch {
                index: 1,
                expected,
                actual,
            }) if expected == hash(0x99) && actual == hash(0x30)
        ));
    }

    #[test]
    fn merge_rejects_write_version_gaps_duplicates_bounds_and_missing_tail() {
        let provenance = fixture_provenance();
        for (raw, expected) in [
            (vec![10, 12, 13], "gap"),
            (vec![10, 10, 12, 13], "duplicate"),
        ] {
            let mut sources = fixture_sources(&provenance);
            sources[0] = write_source(
                &provenance,
                0,
                &raw,
                hash(0x10),
                hash(0x20),
                Some(source_provenance(&provenance, 0).into()),
            );
            assert!(
                matches!(
                    merge(sources, &provenance),
                    Err(MultiSegmentMergeError::NonContiguousWriteVersion { index: 0, .. })
                ),
                "{expected} must fail"
            );
        }

        let mut sources = fixture_sources(&provenance);
        sources[0] = write_source(
            &provenance,
            0,
            &[10, 11, 12, 14],
            hash(0x10),
            hash(0x20),
            Some(source_provenance(&provenance, 0).into()),
        );
        assert!(matches!(
            merge(sources, &provenance),
            Err(MultiSegmentMergeError::WriteVersionOutOfRange {
                index: 0,
                actual: 14,
                ..
            })
        ));

        let mut sources = fixture_sources(&provenance);
        sources[0] = write_source(
            &provenance,
            0,
            &[10, 11, 12],
            hash(0x10),
            hash(0x20),
            Some(source_provenance(&provenance, 0).into()),
        );
        assert!(matches!(
            merge(sources, &provenance),
            Err(MultiSegmentMergeError::IncompleteWriteVersionRange {
                index: 0,
                expected: 14,
                actual: 13
            })
        ));
    }

    #[test]
    fn merge_requires_complete_sources_with_single_runtime_v2_provenance() {
        let provenance = fixture_provenance();

        let mut sources = fixture_sources(&provenance);
        sources[0] = write_source(
            &provenance,
            0,
            &[10, 11, 12, 13],
            hash(0x10),
            hash(0x20),
            None,
        );
        assert!(matches!(
            merge(sources, &provenance),
            Err(MultiSegmentMergeError::MissingSourceProvenance { index: 0 })
        ));

        let mut sources = fixture_sources(&provenance);
        let legacy: ArchiveProvenance = source_provenance(&provenance, 0).base.into();
        sources[0] = write_source(
            &provenance,
            0,
            &[10, 11, 12, 13],
            hash(0x10),
            hash(0x20),
            Some(legacy),
        );
        assert!(matches!(
            merge(sources, &provenance),
            Err(MultiSegmentMergeError::SourceProvenanceVersion {
                index: 0,
                actual: ARCHIVE_PROVENANCE_VERSION_V1
            })
        ));

        let config = ArchiveWriterConfig {
            bucket_slots: 1,
            compression: Compression::None,
            ..ArchiveWriterConfig::default()
        };
        let source_provenance: ArchiveProvenance = source_provenance(&provenance, 0).into();
        let incomplete = ArchiveWriter::new_with_provenance(
            Vec::new(),
            EPOCH,
            FIRST_SLOT,
            1,
            config,
            &source_provenance,
        )
        .unwrap()
        .finish()
        .unwrap()
        .0;
        let mut sources = fixture_sources(&provenance);
        sources[0] = incomplete;
        assert!(matches!(
            merge(sources, &provenance),
            Err(MultiSegmentMergeError::IncompleteSource { index: 0 })
        ));
    }
}
