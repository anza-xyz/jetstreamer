//! Lossless archive re-encoding from any readable Horizon version into the
//! writer's current format.
//!
//! The source is decoded in its original event order and streamed directly
//! into a fresh [`ArchiveWriter`]. No slot-sized transaction buffer is needed:
//! ordered visitor hooks expose epoch metadata, pre-transaction orphan
//! updates, transactions, and post-transaction orphan updates in exactly the
//! order the destination encoder must observe them.

use std::io::{Read, Seek, SeekFrom, Write};

use lencode::Encode;
use sha2::{Digest, Sha256};
use solana_hash::Hash;
use solana_message::VersionedMessage;
use solana_signature::Signature;
use solana_transaction::versioned::VersionedTransaction;

use crate::account_updates::AccountUpdateView;
use crate::transactions::Transaction;

use super::{
    ARCHIVE_PROVENANCE_MAGIC, ArchiveFormatError, ArchiveReader, ArchiveStats, ArchiveWriter,
    ArchiveWriterConfig, BlockNotification, ChainMismatchPolicy, Consumption, EntryRecord,
    EpochMeta, SlotKind, SlotVisitor,
};

/// Which independently decodable source buckets to re-encode.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum BucketSelection {
    /// Re-encode every bucket in the source archive.
    #[default]
    All,
    /// Re-encode these zero-based bucket indexes. Input is normalized to a
    /// sorted, duplicate-free list before decoding.
    Indices(Vec<usize>),
}

/// Configuration for [`reencode_archive`].
#[derive(Debug, Clone, Default)]
pub struct ReencodeOptions {
    /// Destination writer settings.
    pub writer: ArchiveWriterConfig,
    /// Full conversion or a deterministic bucket subset.
    pub buckets: BucketSelection,
}

/// Canonical predecessor used to repair one historical archive-boundary
/// placeholder.
///
/// Old writers could start an otherwise complete archive with both the first
/// bucket PoH anchor and first block `parent_blockhash` set to zero. Repair is
/// permitted only for that first non-genesis block, after its stored blockhash
/// has been recomputed from this predecessor and the archive's own entries and
/// transaction signatures.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InitialParentRepair {
    pub parent_slot: u64,
    pub parent_blockhash: Hash,
}

/// Source/destination accounting from one re-encoding pass.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReencodeStats {
    /// Source archive epoch identity retained by the destination header.
    pub source_epoch: u64,
    /// First slot in the source archive's declared range.
    pub source_slot_start: u64,
    /// Number of slots in the source archive's declared range.
    pub source_slot_count: u64,
    /// Complete source file size, even when only a subset was selected.
    pub source_file_bytes: u64,
    /// Sum of source bucket-header + stored-payload bytes selected.
    pub source_bucket_bytes: u64,
    /// Number of source buckets selected.
    pub source_buckets: u64,
    /// Slot frames decoded and written.
    pub slots_reencoded: u64,
    /// Historical writer-resume artifacts in the selected source buckets:
    /// parent-blockhash mismatches accepted only because the stored parent was
    /// zero. Re-encoding preserves those zero values unchanged.
    pub source_zero_parent_resume_artifacts: u64,
    /// Zero-parent resume artifacts expected after the transformation. This
    /// equals the source count normally and is zero after the sole initial
    /// placeholder is repaired.
    pub output_zero_parent_resume_artifacts: u64,
    /// SHA-256 of the selected source events in decoded semantic order.
    pub source_semantic_sha256: [u8; 32],
    /// SHA-256 of decoded destination events. This equals the source digest
    /// for ordinary re-encoding and differs only by the proven parent hash for
    /// an initial-parent repair.
    pub output_semantic_sha256: [u8; 32],
    /// Destination writer counters.
    pub output: ArchiveStats,
}

const SEMANTIC_DIGEST_DOMAIN: &[u8] = b"jetstreamer-horizon-semantic-sha256-v1\0";
const SEMANTIC_EVENT_DOMAIN: &[u8] = b"jetstreamer-horizon-semantic-event-v1\0";
const EVENT_SLOT_SKIPPED: u8 = 1;
const EVENT_SLOT_BLOCK: u8 = 2;
const EVENT_EPOCH: u8 = 3;
const EVENT_PRE_UPDATE: u8 = 4;
const EVENT_TRANSACTION: u8 = 5;
const EVENT_POST_UPDATE: u8 = 6;
const EVENT_SKIPPED_END: u8 = 7;
const EVENT_BLOCK_END: u8 = 8;

struct SemanticHashWriter<'a>(&'a mut Sha256);

impl lencode::io::Write for SemanticHashWriter<'_> {
    #[inline]
    fn write(&mut self, bytes: &[u8]) -> lencode::Result<usize> {
        self.0.update(bytes);
        Ok(bytes.len())
    }

    #[inline]
    fn flush(&mut self) -> lencode::Result<()> {
        Ok(())
    }
}

/// Streaming, version-independent digest of decoded Horizon events.
///
/// Each callback is hashed as a separately domain-tagged event, then its
/// fixed-size event digest is appended to the running digest. This gives the
/// stream explicit boundaries without buffering a transaction or slot.
pub struct SemanticDigest {
    hasher: Sha256,
    event_count: u64,
    error: Option<lencode::io::Error>,
}

impl Default for SemanticDigest {
    fn default() -> Self {
        let mut hasher = Sha256::new();
        hasher.update(SEMANTIC_DIGEST_DOMAIN);
        Self {
            hasher,
            event_count: 0,
            error: None,
        }
    }
}

impl SemanticDigest {
    /// Creates an empty semantic event digest.
    pub fn new() -> Self {
        Self::default()
    }

    fn record_event(
        &mut self,
        tag: u8,
        encode: impl FnOnce(&mut SemanticHashWriter<'_>) -> lencode::Result<()>,
    ) {
        if self.error.is_some() {
            return;
        }

        let mut event = Sha256::new();
        event.update(SEMANTIC_EVENT_DOMAIN);
        event.update([tag]);
        let result = {
            let mut writer = SemanticHashWriter(&mut event);
            encode(&mut writer)
        };
        match result {
            Ok(()) => {
                self.hasher.update([tag]);
                self.hasher.update(event.finalize());
                self.event_count += 1;
            }
            Err(error) => self.error = Some(error),
        }
    }

    fn record_update(&mut self, tag: u8, slot: u64, update: &AccountUpdateView<'_>) {
        self.record_event(tag, |writer| {
            slot.encode_ext(writer, None)?;
            update.pubkey.encode_ext(writer, None)?;
            update.lamports.encode_ext(writer, None)?;
            update.owner.encode_ext(writer, None)?;
            update.executable.encode_ext(writer, None)?;
            update.rent_epoch.encode_ext(writer, None)?;
            update.write_version.encode_ext(writer, None)?;
            (update.data.len() as u64).encode_ext(writer, None)?;
            let written = lencode::io::Write::write(writer, update.data)?;
            if written != update.data.len() {
                return Err(lencode::io::Error::WriterOutOfSpace);
            }
            Ok(())
        });
    }

    /// Finalizes the digest, returning a stored encoding error if one of the
    /// semantic values could not be encoded.
    pub fn finish(mut self) -> lencode::Result<[u8; 32]> {
        if let Some(error) = self.error {
            return Err(error);
        }
        self.hasher.update([0]);
        self.hasher.update(self.event_count.to_le_bytes());
        Ok(self.hasher.finalize().into())
    }

    fn record_block(
        &mut self,
        notification: &BlockNotification,
        entries: &[EntryRecord],
        parent_blockhash_override: Option<Hash>,
    ) {
        match notification {
            BlockNotification::Skipped(skipped) => {
                self.record_event(EVENT_SKIPPED_END, |writer| {
                    skipped.slot.encode_ext(writer, None)?;
                    (entries.len() as u64).encode_ext(writer, None)?;
                    for entry in entries {
                        entry.encode_ext(writer, None)?;
                    }
                    Ok(())
                });
            }
            BlockNotification::Block(meta) => {
                self.record_event(EVENT_BLOCK_END, |writer| {
                    meta.slot.encode_ext(writer, None)?;
                    meta.parent_slot.encode_ext(writer, None)?;
                    parent_blockhash_override
                        .unwrap_or(meta.parent_blockhash)
                        .encode_ext(writer, None)?;
                    meta.blockhash.encode_ext(writer, None)?;
                    meta.block_time.encode_ext(writer, None)?;
                    meta.block_height.encode_ext(writer, None)?;
                    meta.executed_transaction_count.encode_ext(writer, None)?;
                    meta.entry_count.encode_ext(writer, None)?;
                    meta.rewards.encode_ext(writer, None)?;
                    meta.num_partitions.encode_ext(writer, None)?;
                    (entries.len() as u64).encode_ext(writer, None)?;
                    for entry in entries {
                        entry.encode_ext(writer, None)?;
                    }
                    Ok(())
                });
            }
        }
    }
}

impl SlotVisitor for SemanticDigest {
    fn on_slot_start(&mut self, slot: u64, kind: SlotKind) {
        let tag = match kind {
            SlotKind::Skipped => EVENT_SLOT_SKIPPED,
            SlotKind::Block => EVENT_SLOT_BLOCK,
        };
        self.record_event(tag, |writer| {
            slot.encode_ext(writer, None)?;
            Ok(())
        });
    }

    fn on_epoch(&mut self, meta: &EpochMeta) {
        self.record_event(EVENT_EPOCH, |writer| {
            meta.encode_ext(writer, None)?;
            Ok(())
        });
    }

    fn on_pre_account_update(&mut self, slot: u64, update: &AccountUpdateView<'_>) {
        self.record_update(EVENT_PRE_UPDATE, slot, update);
    }

    fn on_transaction(&mut self, slot: u64, tx_index: u32, tx: &Transaction) {
        self.record_event(EVENT_TRANSACTION, |writer| {
            slot.encode_ext(writer, None)?;
            tx_index.encode_ext(writer, None)?;
            tx.encode_ext(writer, None)?;
            Ok(())
        });
    }

    fn on_post_account_update(&mut self, slot: u64, update: &AccountUpdateView<'_>) {
        self.record_update(EVENT_POST_UPDATE, slot, update);
    }

    fn on_block(&mut self, notification: &BlockNotification, entries: &[EntryRecord]) {
        self.record_block(notification, entries, None);
    }

    fn consumption(&self) -> Consumption {
        Consumption::all().without_block_account_update_arenas()
    }
}

/// Decodes a supported Horizon archive and writes the selected buckets using
/// the explicitly selected writer version and codec settings.
///
/// `source` is only read and sought. Callers are responsible for opening the
/// destination safely; the companion CLI uses a distinct, newly-created
/// partial file and publishes it only after successful validation.
/// A complete semantic re-encode preserves a recognized, valid provenance
/// envelope byte-for-byte. Bucket subsets omit it because its declared slot
/// range would overstate the subset actually written.
pub fn reencode_archive<R, W>(
    source: R,
    sink: W,
    options: ReencodeOptions,
) -> Result<(W, ReencodeStats), ArchiveFormatError>
where
    R: Read + Seek,
    W: Write,
{
    reencode_archive_inner(source, sink, options, None)
}

/// Re-encodes a complete archive while repairing its single initial
/// zero-parent placeholder from a canonical predecessor.
///
/// The transformation fails closed unless the source is dense, the first
/// non-genesis block has exactly the supplied parent slot and a zero stored
/// parent hash, that block's PoH recomputes to its stored blockhash from the
/// supplied parent hash, and no later block has a zero parent hash.
pub fn reencode_archive_with_initial_parent_repair<R, W>(
    source: R,
    sink: W,
    options: ReencodeOptions,
    repair: InitialParentRepair,
) -> Result<(W, ReencodeStats), ArchiveFormatError>
where
    R: Read + Seek,
    W: Write,
{
    reencode_archive_inner(source, sink, options, Some(repair))
}

fn reencode_archive_inner<R, W>(
    mut source: R,
    sink: W,
    options: ReencodeOptions,
    repair: Option<InitialParentRepair>,
) -> Result<(W, ReencodeStats), ArchiveFormatError>
where
    R: Read + Seek,
    W: Write,
{
    let source_file_bytes = source.seek(SeekFrom::End(0))?;
    let mut reader = ArchiveReader::open(source)?;
    reader.verify_chain = true;
    reader.chain_mismatch_policy = ChainMismatchPolicy::AllowZeroParentResume;
    let header = reader.header().clone();
    let destination_bucket_slots = options.writer.bucket_slots;

    let mut selected = match options.buckets {
        BucketSelection::All => (0..reader.bucket_count()).collect::<Vec<_>>(),
        BucketSelection::Indices(indices) => indices,
    };
    selected.sort_unstable();
    selected.dedup();
    for &index in &selected {
        if index >= reader.bucket_count() {
            return Err(ArchiveFormatError::BucketOutOfRange {
                index,
                count: reader.bucket_count(),
            });
        }
    }
    let expected_source_buckets = header.slot_count.div_ceil(u64::from(header.bucket_slots));
    let source_layout_is_dense = u64::try_from(reader.bucket_count())
        .is_ok_and(|count| count == expected_source_buckets)
        && reader
            .bucket_index()
            .iter()
            .enumerate()
            .all(|(index, entry)| {
                u64::try_from(index)
                    .ok()
                    .and_then(|index| index.checked_mul(u64::from(header.bucket_slots)))
                    .and_then(|offset| header.slot_start.checked_add(offset))
                    == Some(entry.first_slot)
            });
    let selection_is_complete = source_layout_is_dense
        && selected.len() == reader.bucket_count()
        && selected.iter().copied().eq(0..reader.bucket_count());
    if let Some(repair) = repair {
        if !selection_is_complete || !reader.has_complete_slot_coverage()? {
            return Err(ArchiveFormatError::InitialParentRepairRequiresCompleteArchive);
        }
        if header.slot_start == 0 {
            return Err(ArchiveFormatError::InitialParentRepairAtGenesis);
        }
        if repair.parent_blockhash == Hash::default() {
            return Err(ArchiveFormatError::InitialParentRepairZeroPredecessor);
        }
        if repair.parent_slot >= header.slot_start {
            return Err(
                ArchiveFormatError::InitialParentRepairInvalidPredecessorSlot {
                    parent_slot: repair.parent_slot,
                    slot_start: header.slot_start,
                },
            );
        }
    }
    if !selection_is_complete
        && !selected.is_empty()
        && destination_bucket_slots != header.bucket_slots
    {
        return Err(ArchiveFormatError::ReencodeBucketSizeMismatch {
            source_bucket_slots: header.bucket_slots,
            destination_bucket_slots,
        });
    }

    let source_bucket_bytes = selected
        .iter()
        .map(|&index| reader.bucket_index()[index].len)
        .sum();

    let preserved_provenance = if selection_is_complete
        && header.meta.reserved.starts_with(&ARCHIVE_PROVENANCE_MAGIC)
        && reader.has_complete_slot_coverage()?
    {
        // Parse before copying so a malformed or unsupported envelope cannot
        // be laundered into a newly verified archive. The destination retains
        // the same declared range, so the reader's range check also proves the
        // copied claim remains applicable.
        reader
            .provenance()?
            .ok_or(ArchiveFormatError::InvalidContainerLayout(
                "archive provenance magic decoded as absent provenance",
            ))?;
        header.meta.reserved.clone()
    } else {
        Vec::new()
    };

    let mut writer = ArchiveWriter::new_with_reserved(
        sink,
        header.epoch,
        header.slot_start,
        header.slot_count,
        options.writer,
        preserved_provenance,
    )?;
    if let Some(repair) = repair {
        writer.preserve_initial_poh_anchor(header.slot_start, repair.parent_blockhash)?;
    }
    let mut slots_reencoded = 0u64;

    let (source_semantic_sha256, output_semantic_sha256) = {
        let mut visitor = ReencodeVisitor::new(&mut writer, repair);
        for index in selected.iter().copied() {
            if selection_is_complete
                && destination_bucket_slots != header.bucket_slots
                && index == 0
            {
                slots_reencoded += reader.read_bucket_with_header(
                    index,
                    &mut visitor,
                    |source_header, visitor| {
                        let anchor =
                            repaired_initial_anchor(index, source_header.poh_start_hash, repair)?;
                        visitor
                            .writer
                            .preserve_initial_poh_anchor(source_header.first_slot, anchor)
                    },
                )?;
            } else {
                slots_reencoded += reader.read_bucket(index, &mut visitor)?;
            }
            if let Some(error) = visitor.error.take() {
                return Err(error);
            }
            if destination_bucket_slots == header.bucket_slots {
                let source_header = reader.current_bucket_header().ok_or(
                    ArchiveFormatError::InvalidContainerLayout(
                        "reader did not retain the decoded bucket header",
                    ),
                )?;
                let anchor = repaired_initial_anchor(index, source_header.poh_start_hash, repair)?;
                visitor
                    .writer
                    .preserve_current_bucket_poh_anchor(source_header.first_slot, anchor)?;
            }
        }
        if repair.is_some() && !visitor.repair_applied {
            return Err(ArchiveFormatError::InitialParentRepairMissingBlock);
        }
        (
            visitor.source_digest.finish()?,
            visitor.output_digest.finish()?,
        )
    };

    let (sink, output) = writer.finish()?;
    let source_zero_parent_resume_artifacts = reader.zero_parent_resume_artifacts();
    let output_zero_parent_resume_artifacts = if repair.is_some() {
        if source_zero_parent_resume_artifacts != 1 {
            return Err(ArchiveFormatError::InitialParentRepairArtifactCount {
                actual: source_zero_parent_resume_artifacts,
            });
        }
        0
    } else {
        source_zero_parent_resume_artifacts
    };
    Ok((
        sink,
        ReencodeStats {
            source_epoch: header.epoch,
            source_slot_start: header.slot_start,
            source_slot_count: header.slot_count,
            source_file_bytes,
            source_bucket_bytes,
            source_buckets: selected.len() as u64,
            slots_reencoded,
            source_zero_parent_resume_artifacts,
            output_zero_parent_resume_artifacts,
            source_semantic_sha256,
            output_semantic_sha256,
            output,
        },
    ))
}

fn repaired_initial_anchor(
    bucket_index: usize,
    source_anchor: Hash,
    repair: Option<InitialParentRepair>,
) -> Result<Hash, ArchiveFormatError> {
    let Some(repair) = repair.filter(|_| bucket_index == 0) else {
        return Ok(source_anchor);
    };
    if source_anchor != Hash::default() {
        return Err(ArchiveFormatError::InitialParentRepairNonzeroAnchor {
            actual: source_anchor,
        });
    }
    Ok(repair.parent_blockhash)
}

fn recompute_blockhash(
    parent: Hash,
    entries: &[EntryRecord],
    signatures: &[Vec<Signature>],
) -> Option<Hash> {
    let mut poh = parent;
    let mut index = 0usize;
    for entry in entries {
        let count = entry.tx_count as usize;
        let end = index.checked_add(count)?;
        let transactions: Vec<VersionedTransaction> = signatures
            .get(index..end)?
            .iter()
            .map(|signatures| VersionedTransaction {
                signatures: signatures.clone(),
                message: VersionedMessage::default(),
            })
            .collect();
        index = end;
        #[allow(deprecated)]
        {
            poh = solana_entry::entry::next_hash(&poh, entry.num_hashes, &transactions);
        }
    }
    (index == signatures.len()).then_some(poh)
}

struct ReencodeVisitor<'a, W: Write> {
    writer: &'a mut ArchiveWriter<W>,
    source_digest: SemanticDigest,
    output_digest: SemanticDigest,
    repair: Option<InitialParentRepair>,
    repair_applied: bool,
    first_block_signatures: Vec<Vec<Signature>>,
    error: Option<ArchiveFormatError>,
}

impl<'a, W: Write> ReencodeVisitor<'a, W> {
    fn new(writer: &'a mut ArchiveWriter<W>, repair: Option<InitialParentRepair>) -> Self {
        Self {
            writer,
            source_digest: SemanticDigest::new(),
            output_digest: SemanticDigest::new(),
            repair,
            repair_applied: false,
            first_block_signatures: Vec::new(),
            error: None,
        }
    }

    #[inline]
    fn record(&mut self, result: Result<(), ArchiveFormatError>) {
        if self.error.is_none()
            && let Err(error) = result
        {
            self.error = Some(error);
        }
    }
}

impl<W: Write> SlotVisitor for ReencodeVisitor<'_, W> {
    fn on_slot_start(&mut self, slot: u64, kind: SlotKind) {
        if self.error.is_some() {
            return;
        }
        self.source_digest.on_slot_start(slot, kind);
        self.output_digest.on_slot_start(slot, kind);
        let result = match kind {
            SlotKind::Skipped => self.writer.write_skipped_slot(slot),
            SlotKind::Block => self.writer.begin_slot(slot),
        };
        self.record(result);
    }

    fn on_epoch(&mut self, meta: &EpochMeta) {
        if self.error.is_some() {
            return;
        }
        self.source_digest.on_epoch(meta);
        self.output_digest.on_epoch(meta);
        let result = self.writer.write_epoch_meta(meta);
        self.record(result);
    }

    fn on_pre_account_update(&mut self, slot: u64, update: &AccountUpdateView<'_>) {
        if self.error.is_some() {
            return;
        }
        self.source_digest.on_pre_account_update(slot, update);
        self.output_digest.on_pre_account_update(slot, update);
        let result = self.writer.write_reencoded_pre_update(update);
        self.record(result);
    }

    fn on_transaction(&mut self, slot: u64, tx_index: u32, tx: &Transaction) {
        if self.error.is_some() {
            return;
        }
        self.source_digest.on_transaction(slot, tx_index, tx);
        self.output_digest.on_transaction(slot, tx_index, tx);
        if self.repair.is_some() && !self.repair_applied {
            self.first_block_signatures
                .push(tx.signatures.as_slice().to_vec());
        }
        let result = self.writer.write_transaction(tx);
        self.record(result);
    }

    fn on_post_account_update(&mut self, slot: u64, update: &AccountUpdateView<'_>) {
        if self.error.is_some() {
            return;
        }
        self.source_digest.on_post_account_update(slot, update);
        self.output_digest.on_post_account_update(slot, update);
        let result = self.writer.write_reencoded_post_update(update);
        self.record(result);
    }

    fn on_block(&mut self, notification: &BlockNotification, entries: &[EntryRecord]) {
        if self.error.is_some() {
            return;
        }
        self.source_digest.record_block(notification, entries, None);
        if let BlockNotification::Block(meta) = notification {
            let parent_override = if let Some(repair) = self.repair {
                if self.repair_applied {
                    if meta.parent_blockhash == Hash::default() {
                        self.error = Some(
                            ArchiveFormatError::InitialParentRepairAdditionalZeroParent {
                                slot: meta.slot,
                            },
                        );
                        return;
                    }
                    None
                } else {
                    if meta.parent_slot != repair.parent_slot {
                        self.error = Some(ArchiveFormatError::InitialParentRepairParentSlot {
                            slot: meta.slot,
                            expected: repair.parent_slot,
                            actual: meta.parent_slot,
                        });
                        return;
                    }
                    if meta.parent_blockhash != Hash::default() {
                        self.error = Some(ArchiveFormatError::InitialParentRepairNonzeroParent {
                            slot: meta.slot,
                            actual: meta.parent_blockhash,
                        });
                        return;
                    }
                    let recomputed = recompute_blockhash(
                        repair.parent_blockhash,
                        entries,
                        &self.first_block_signatures,
                    );
                    if recomputed != Some(meta.blockhash) {
                        self.error = Some(ArchiveFormatError::InitialParentRepairPohMismatch {
                            slot: meta.slot,
                            recomputed,
                            stored: meta.blockhash,
                        });
                        return;
                    }
                    self.repair_applied = true;
                    self.first_block_signatures.clear();
                    Some(repair.parent_blockhash)
                }
            } else {
                None
            };
            self.output_digest
                .record_block(notification, entries, parent_override);
            let result = match parent_override {
                Some(parent) => self
                    .writer
                    .end_reencoded_slot_with_parent_blockhash(meta, entries, parent),
                None => self.writer.end_reencoded_slot(meta, entries),
            };
            self.record(result);
        } else {
            self.output_digest.record_block(notification, entries, None);
        }
    }

    fn consumption(&self) -> Consumption {
        Consumption::all().without_block_account_update_arenas()
    }
}

#[cfg(test)]
mod tests {
    use solana_address::Address;

    use crate::block_metas::BlockMeta;

    use super::*;

    fn one_update_digest(lamports: u64, data: &[u8]) -> [u8; 32] {
        let update = AccountUpdateView {
            pubkey: Address::new_from_array([1; 32]),
            lamports,
            owner: Address::new_from_array([2; 32]),
            executable: false,
            rent_epoch: 9,
            write_version: 11,
            data,
        };
        let mut digest = SemanticDigest::new();
        digest.on_slot_start(42, SlotKind::Block);
        digest.on_pre_account_update(42, &update);
        digest.finish().unwrap()
    }

    fn archive_semantic_sha256(bytes: &[u8]) -> [u8; 32] {
        let mut reader = ArchiveReader::open(std::io::Cursor::new(bytes)).unwrap();
        let mut digest = SemanticDigest::new();
        reader.read_slots(0, u64::MAX, &mut digest).unwrap();
        digest.finish().unwrap()
    }

    #[test]
    fn semantic_digest_detects_same_count_field_and_data_mutations() {
        let original = one_update_digest(7, b"account state");
        assert_ne!(original, one_update_digest(8, b"account state"));
        assert_ne!(original, one_update_digest(7, b"account STATE"));
    }

    #[derive(Default)]
    struct ParentCollector(Vec<(u64, Hash)>);

    impl SlotVisitor for ParentCollector {
        fn on_block(&mut self, notification: &BlockNotification, _entries: &[EntryRecord]) {
            if let BlockNotification::Block(meta) = notification {
                self.0.push((meta.parent_slot, meta.parent_blockhash));
            }
        }

        fn consumption(&self) -> Consumption {
            Consumption::all()
                .without_account_update_data()
                .without_block_account_update_arenas()
        }
    }

    #[allow(deprecated)]
    fn tick_hash(parent: Hash) -> Hash {
        solana_entry::entry::next_hash(&parent, 1, &[])
    }

    fn zero_parent_source(second_parent_is_zero: bool) -> (Vec<u8>, InitialParentRepair) {
        let repair = InitialParentRepair {
            parent_slot: 999,
            parent_blockhash: Hash::new_from_array([0xa5; 32]),
        };
        let mut writer = ArchiveWriter::new(
            Vec::new(),
            1,
            1_000,
            2,
            ArchiveWriterConfig {
                compression: super::super::Compression::None,
                bucket_slots: 2,
                ..ArchiveWriterConfig::default()
            },
        )
        .unwrap();
        let entries = [EntryRecord {
            num_hashes: 1,
            tx_count: 0,
        }];
        let first_hash = tick_hash(repair.parent_blockhash);
        writer.begin_slot(1_000).unwrap();
        let mut first = BlockMeta::new_boxed();
        first.slot = 1_000;
        first.parent_slot = repair.parent_slot;
        first.parent_blockhash = Hash::default();
        first.blockhash = first_hash;
        first.entry_count = 1;
        writer.end_slot(&first, &entries).unwrap();

        writer.begin_slot(1_001).unwrap();
        let mut second = BlockMeta::new_boxed();
        second.slot = 1_001;
        second.parent_slot = 1_000;
        second.parent_blockhash = if second_parent_is_zero {
            Hash::default()
        } else {
            first_hash
        };
        second.blockhash = tick_hash(first_hash);
        second.entry_count = 1;
        writer.end_slot(&second, &entries).unwrap();
        (writer.finish().unwrap().0, repair)
    }

    #[test]
    fn initial_parent_repair_is_poh_proven_and_strictly_readable() {
        let (source, repair) = zero_parent_source(false);
        let (output, stats) = reencode_archive_with_initial_parent_repair(
            std::io::Cursor::new(source),
            Vec::new(),
            ReencodeOptions {
                writer: ArchiveWriterConfig {
                    compression: super::super::Compression::None,
                    bucket_slots: 2,
                    ..ArchiveWriterConfig::default()
                },
                buckets: BucketSelection::All,
            },
            repair,
        )
        .unwrap();
        assert_eq!(stats.source_zero_parent_resume_artifacts, 1);
        assert_eq!(stats.output_zero_parent_resume_artifacts, 0);
        assert_ne!(stats.source_semantic_sha256, stats.output_semantic_sha256);
        assert_eq!(
            stats.output_semantic_sha256,
            archive_semantic_sha256(&output)
        );

        let mut reader = ArchiveReader::open(std::io::Cursor::new(output)).unwrap();
        reader.verify_chain = true;
        let mut initial_anchor = None;
        let mut parents = ParentCollector::default();
        assert_eq!(
            reader
                .read_bucket_with_header(0, &mut parents, |header, _| {
                    initial_anchor = Some(header.poh_start_hash);
                    Ok(())
                })
                .unwrap(),
            2
        );
        assert_eq!(initial_anchor, Some(repair.parent_blockhash));
        assert_eq!(
            parents.0,
            vec![
                (repair.parent_slot, repair.parent_blockhash),
                (1_000, tick_hash(repair.parent_blockhash))
            ]
        );
    }

    #[test]
    fn initial_parent_repair_rejects_wrong_anchor_and_later_zero_parent() {
        let (source, repair) = zero_parent_source(false);
        let error = reencode_archive_with_initial_parent_repair(
            std::io::Cursor::new(source),
            Vec::new(),
            ReencodeOptions::default(),
            InitialParentRepair {
                parent_blockhash: Hash::new_from_array([0xb6; 32]),
                ..repair
            },
        )
        .unwrap_err();
        assert!(matches!(
            error,
            ArchiveFormatError::InitialParentRepairPohMismatch { slot: 1_000, .. }
        ));

        let (source, repair) = zero_parent_source(true);
        let error = reencode_archive_with_initial_parent_repair(
            std::io::Cursor::new(source),
            Vec::new(),
            ReencodeOptions::default(),
            repair,
        )
        .unwrap_err();
        assert!(matches!(
            error,
            ArchiveFormatError::InitialParentRepairAdditionalZeroParent { slot: 1_001 }
        ));
    }
}
