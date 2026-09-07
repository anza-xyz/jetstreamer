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

use crate::account_updates::AccountUpdateView;
use crate::transactions::Transaction;

use super::{
    ArchiveFormatError, ArchiveReader, ArchiveStats, ArchiveWriter, ArchiveWriterConfig,
    BlockNotification, ChainMismatchPolicy, Consumption, EntryRecord, EpochMeta, SlotKind,
    SlotVisitor,
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
    /// SHA-256 of the selected source events in decoded semantic order.
    pub source_semantic_sha256: [u8; 32],
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
                    meta.parent_blockhash.encode_ext(writer, None)?;
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
pub fn reencode_archive<R, W>(
    mut source: R,
    sink: W,
    options: ReencodeOptions,
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

    let mut writer = ArchiveWriter::new(
        sink,
        header.epoch,
        header.slot_start,
        header.slot_count,
        options.writer,
    )?;
    let mut slots_reencoded = 0u64;

    let source_semantic_sha256 = {
        let mut visitor = ReencodeVisitor::new(&mut writer);
        for index in selected.iter().copied() {
            if selection_is_complete
                && destination_bucket_slots != header.bucket_slots
                && index == 0
            {
                slots_reencoded += reader.read_bucket_with_header(
                    index,
                    &mut visitor,
                    |source_header, visitor| {
                        visitor.writer.preserve_initial_poh_anchor(
                            source_header.first_slot,
                            source_header.poh_start_hash,
                        )
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
                visitor.writer.preserve_current_bucket_poh_anchor(
                    source_header.first_slot,
                    source_header.poh_start_hash,
                )?;
            }
        }
        visitor.digest.finish()?
    };

    let (sink, output) = writer.finish()?;
    let source_zero_parent_resume_artifacts = reader.zero_parent_resume_artifacts();
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
            source_semantic_sha256,
            output,
        },
    ))
}

struct ReencodeVisitor<'a, W: Write> {
    writer: &'a mut ArchiveWriter<W>,
    digest: SemanticDigest,
    error: Option<ArchiveFormatError>,
}

impl<'a, W: Write> ReencodeVisitor<'a, W> {
    fn new(writer: &'a mut ArchiveWriter<W>) -> Self {
        Self {
            writer,
            digest: SemanticDigest::new(),
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
        self.digest.on_slot_start(slot, kind);
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
        self.digest.on_epoch(meta);
        let result = self.writer.write_epoch_meta(meta);
        self.record(result);
    }

    fn on_pre_account_update(&mut self, slot: u64, update: &AccountUpdateView<'_>) {
        if self.error.is_some() {
            return;
        }
        self.digest.on_pre_account_update(slot, update);
        let result = self.writer.write_reencoded_pre_update(update);
        self.record(result);
    }

    fn on_transaction(&mut self, slot: u64, tx_index: u32, tx: &Transaction) {
        if self.error.is_some() {
            return;
        }
        self.digest.on_transaction(slot, tx_index, tx);
        let result = self.writer.write_transaction(tx);
        self.record(result);
    }

    fn on_post_account_update(&mut self, slot: u64, update: &AccountUpdateView<'_>) {
        if self.error.is_some() {
            return;
        }
        self.digest.on_post_account_update(slot, update);
        let result = self.writer.write_reencoded_post_update(update);
        self.record(result);
    }

    fn on_block(&mut self, notification: &BlockNotification, entries: &[EntryRecord]) {
        if self.error.is_some() {
            return;
        }
        self.digest.on_block(notification, entries);
        if let BlockNotification::Block(meta) = notification {
            let result = self.writer.end_reencoded_slot(meta, entries);
            self.record(result);
        }
    }

    fn consumption(&self) -> Consumption {
        Consumption::all().without_block_account_update_arenas()
    }
}

#[cfg(test)]
mod tests {
    use solana_address::Address;

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

    #[test]
    fn semantic_digest_detects_same_count_field_and_data_mutations() {
        let original = one_update_digest(7, b"account state");
        assert_ne!(original, one_update_digest(8, b"account state"));
        assert_ne!(original, one_update_digest(7, b"account STATE"));
    }
}
