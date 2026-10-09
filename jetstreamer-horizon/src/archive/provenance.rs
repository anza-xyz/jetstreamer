//! Optional, versioned provenance carried in [`ArchiveMeta::reserved`].
//!
//! Provenance has its own magic and version so it can evolve independently of
//! the Horizon container and transaction wire formats. Readers do not parse it
//! while opening an archive. Callers that use provenance as an admission check
//! must explicitly request it and handle every error.

use lencode::prelude::*;
use solana_hash::Hash;
use std::ops::Range;

use super::ArchiveMeta;

/// Magic prefix for a structured archive-provenance envelope.
pub const ARCHIVE_PROVENANCE_MAGIC: [u8; 8] = *b"JSPROV\0\0";

/// First version of the provenance payload.
pub const ARCHIVE_PROVENANCE_VERSION_V1: u16 = 1;

/// Provenance payload that binds a historical runtime identity to the exact
/// worker executable measured by the parent process.
pub const ARCHIVE_PROVENANCE_VERSION_V2: u16 = 2;

/// Provenance payload for an archive assembled from more than one selected
/// runtime segment.
pub const ARCHIVE_PROVENANCE_VERSION_V3: u16 = 3;

/// Upper bound for a structured provenance envelope.
pub const MAX_ARCHIVE_PROVENANCE_BYTES: usize = 16 << 10;

/// Upper bound for each human-readable provenance identifier.
pub const MAX_ARCHIVE_PROVENANCE_TEXT_BYTES: usize = 1 << 10;

/// Maximum number of runtime segments represented by one provenance payload.
///
/// The outer envelope limit also bounds allocation, but this smaller semantic
/// limit prevents a malformed payload from turning validation into unbounded
/// pairwise work as future invariants are added.
pub const MAX_RUNTIME_PROVENANCE_SEGMENTS: usize = 64;

const PROVENANCE_PREFIX_BYTES: usize = ARCHIVE_PROVENANCE_MAGIC.len() + size_of::<u16>();

/// Origin of the transaction status stored in an archive.
#[derive(Encode, Decode, Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum TransactionMetadataSemantics {
    /// Status and the rest of the transaction metadata were observed in the
    /// source data.
    Observed = 0,
    /// Replay supplied the status. Other metadata fields retain their source
    /// values, including absent/default values when the source frame was
    /// missing.
    RuntimeReconstructedStatusOnly = 1,
    /// Replay supplied both status and fee, or verified identical values from
    /// an exact canonical recovery. Remaining metadata fields retain available
    /// observations; unavailable fields remain absent/default.
    RuntimeReconstructedStatusAndFee = 2,
}

/// Source of the runtime state from which this output range was replayed.
#[derive(Encode, Decode, Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum BootstrapStateKind {
    /// State was loaded directly from a snapshot archive.
    SnapshotArchive = 0,
    /// State was carried forward by the runtime from a preceding generated
    /// range in the same replay chain.
    CarriedBank = 1,
    /// State was constructed directly from the chain genesis configuration.
    Genesis = 2,
}

/// Evidence level attached to the selected runtime range when the archive was
/// generated.
#[derive(Encode, Decode, Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum RuntimeAdmission {
    /// The runtime passed all configured canonical checkpoints for this range.
    Verified = 0,
    /// The runtime is still being qualified and required an explicit opt-in.
    Candidate = 1,
}

/// Hash algorithm/meaning used for an accounts-state checkpoint.
#[derive(Encode, Decode, Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum AccountsHashKind {
    /// The historical full accounts hash used by early Solana runtimes.
    LegacyAccountsHash = 0,
    /// The accounts lattice hash used by current Agave snapshots.
    AccountsLtHash = 1,
}

/// Meaning of the hash identifying an initial bootstrap state.
#[derive(Encode, Decode, Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum StateCommitmentKind {
    /// A historical full accounts hash.
    LegacyAccountsHash = 0,
    /// An accounts lattice hash.
    AccountsLtHash = 1,
    /// A bank hash.
    BankHash = 2,
}

/// A one-boundary transaction-metadata policy.
///
/// This represents the current historical source transition without storing
/// per-transaction provenance. The semantics for any transaction are derived
/// from its slot and `boundary_slot`.
#[derive(Encode, Decode, Debug, Clone, Copy, PartialEq, Eq)]
pub struct TransactionMetadataPolicy {
    /// Slots below this value use [`Self::before_boundary`]. Slots at or above
    /// it use [`Self::at_or_after_boundary`].
    pub boundary_slot: u64,
    /// Semantics for slots below `boundary_slot`.
    pub before_boundary: TransactionMetadataSemantics,
    /// Semantics for slots at or above `boundary_slot`.
    pub at_or_after_boundary: TransactionMetadataSemantics,
}

impl TransactionMetadataPolicy {
    /// A policy for sources that carry observed metadata at every slot.
    pub const fn observed() -> Self {
        Self {
            boundary_slot: 0,
            before_boundary: TransactionMetadataSemantics::Observed,
            at_or_after_boundary: TransactionMetadataSemantics::Observed,
        }
    }

    /// A policy for a historical source whose missing pre-boundary status is
    /// reconstructed by the selected runtime.
    pub const fn runtime_reconstructed_before(observed_from_slot: u64) -> Self {
        Self {
            boundary_slot: observed_from_slot,
            before_boundary: TransactionMetadataSemantics::RuntimeReconstructedStatusOnly,
            at_or_after_boundary: TransactionMetadataSemantics::Observed,
        }
    }

    /// A policy that uses runtime-reconstructed status at every slot.
    pub const fn runtime_reconstructed() -> Self {
        Self {
            boundary_slot: 0,
            before_boundary: TransactionMetadataSemantics::RuntimeReconstructedStatusOnly,
            at_or_after_boundary: TransactionMetadataSemantics::RuntimeReconstructedStatusOnly,
        }
    }

    /// A policy that reconstructs status before `fee_from_slot`, then
    /// reconstructs both status and fee at and after that slot.
    pub const fn runtime_reconstructed_with_fee_from(fee_from_slot: u64) -> Self {
        Self {
            boundary_slot: fee_from_slot,
            before_boundary: TransactionMetadataSemantics::RuntimeReconstructedStatusOnly,
            at_or_after_boundary: TransactionMetadataSemantics::RuntimeReconstructedStatusAndFee,
        }
    }

    /// A policy that uses runtime-reconstructed status and fee at every slot.
    pub const fn runtime_reconstructed_status_and_fee() -> Self {
        Self {
            boundary_slot: 0,
            before_boundary: TransactionMetadataSemantics::RuntimeReconstructedStatusAndFee,
            at_or_after_boundary: TransactionMetadataSemantics::RuntimeReconstructedStatusAndFee,
        }
    }

    /// Returns the metadata semantics for `slot`.
    pub const fn semantics_for_slot(self, slot: u64) -> TransactionMetadataSemantics {
        if slot < self.boundary_slot {
            self.before_boundary
        } else {
            self.at_or_after_boundary
        }
    }
}

impl Default for TransactionMetadataPolicy {
    fn default() -> Self {
        Self::observed()
    }
}

/// Provenance schema stored by envelope version 1.
#[derive(Encode, Decode, Debug, Clone, PartialEq, Eq)]
pub struct ArchiveProvenanceV1 {
    /// Stable identifier for the archive-generation pipeline and its policy.
    pub generation_profile: String,
    /// Stable identifier selected from the slot-to-runtime registry.
    pub runtime_profile: String,
    /// Qualification state of that runtime for the requested slot range.
    pub runtime_admission: RuntimeAdmission,
    /// Exact runtime source revision, normally a Git commit.
    pub runtime_revision: String,
    /// Exact compiler/toolchain identifier used for the runtime worker.
    pub runtime_toolchain: String,
    /// Genesis identity enforced by the selected runtime.
    pub genesis_hash: Hash,
    /// How the initial runtime state was obtained.
    pub bootstrap_state_kind: BootstrapStateKind,
    /// Slot of the initial runtime state.
    pub bootstrap_slot: u64,
    /// Bank/state hash identifying the initial runtime state.
    pub bootstrap_state_hash: Hash,
    /// Requested output slot range start, inclusive.
    pub requested_slot_start: u64,
    /// Number of slots in the requested output range.
    pub requested_slot_count: u64,
    /// Slot-derived provenance for transaction status metadata.
    pub transaction_metadata: TransactionMetadataPolicy,
}

/// Provenance schema stored by envelope version 2.
///
/// The v1 payload is embedded unchanged so deployed v1 envelopes remain
/// byte-decodable. The digest is measured by the parent from the canonical
/// worker executable immediately before spawning it; it is not supplied by
/// the worker handshake.
#[derive(Encode, Decode, Debug, Clone, PartialEq, Eq)]
pub struct ArchiveProvenanceV2 {
    /// All generation, runtime, bootstrap, and source-policy fields from v1.
    pub base: ArchiveProvenanceV1,
    /// SHA-256 of the historical worker executable used for this replay.
    pub worker_executable_sha256: [u8; 32],
}

/// A hash commitment to the state from which replay begins.
#[derive(Encode, Decode, Debug, Clone, Copy, PartialEq, Eq)]
pub struct StateCommitment {
    /// Slot represented by this state.
    pub slot: u64,
    /// Meaning of [`Self::hash`].
    pub kind: StateCommitmentKind,
    /// State hash supplied by the bootstrap source.
    pub hash: Hash,
}

/// Checked mapping from validator-local worker write versions into the one
/// contiguous write-version namespace stored by an assembled archive.
///
/// For `worker_start <= raw < worker_end_exclusive`, the archive value is
/// `archive_start + (raw - worker_start)`. Recording both ends makes the
/// segment's declared output extent auditable and lets adjacent normalized
/// ranges be checked without comparing validator-local cursors.
#[derive(Encode, Decode, Debug, Clone, Copy, PartialEq, Eq)]
pub struct WriteVersionNormalization {
    /// First validator-local write version emitted into this segment.
    pub worker_start: u64,
    /// Validator-local cursor immediately after this segment's final write.
    pub worker_end_exclusive: u64,
    /// Archive/global write version corresponding to [`Self::worker_start`].
    pub archive_start: u64,
}

impl WriteVersionNormalization {
    /// Exclusive end of this segment's normalized archive write range.
    pub fn archive_end_exclusive(self) -> Option<u64> {
        let count = self.worker_end_exclusive.checked_sub(self.worker_start)?;
        self.archive_start.checked_add(count)
    }

    /// Maps a validator-local write version into the archive namespace.
    ///
    /// Values outside this segment's half-open worker range, and arithmetic
    /// overflow, return `None`.
    pub fn normalize(self, raw: u64) -> Option<u64> {
        if raw >= self.worker_end_exclusive {
            return None;
        }
        let offset = raw.checked_sub(self.worker_start)?;
        self.archive_start.checked_add(offset)
    }
}

/// Runtime and producer identity for one non-empty contiguous output segment.
#[derive(Encode, Decode, Debug, Clone, PartialEq, Eq)]
pub struct RuntimeSegmentProvenance {
    /// First output slot produced by this runtime, inclusive.
    pub slot_start: u64,
    /// Number of output slots produced by this runtime.
    pub slot_count: u64,
    /// Stable identifier for the segment-generation pipeline and its policy.
    pub generation_profile: String,
    /// Stable identifier selected from the slot-to-runtime registry.
    pub runtime_profile: String,
    /// Qualification state of this runtime for the segment.
    pub runtime_admission: RuntimeAdmission,
    /// Exact runtime source revision, normally a Git commit.
    pub runtime_revision: String,
    /// Exact compiler/toolchain identifier used for this runtime.
    pub runtime_toolchain: String,
    /// SHA-256 of an out-of-process runtime executable. In-process runtimes
    /// have no separate worker and record `None`.
    pub worker_executable_sha256: Option<[u8; 32]>,
    /// Mapping from this runtime's local write cursor to the archive's one
    /// contiguous write-version namespace.
    pub write_versions: WriteVersionNormalization,
}

impl RuntimeSegmentProvenance {
    fn slot_end(&self, index: usize) -> Result<u64, ArchiveProvenanceError> {
        self.slot_start
            .checked_add(self.slot_count)
            .ok_or(ArchiveProvenanceError::RuntimeSegmentRangeOverflow { index })
    }

    fn validate(&self, index: usize) -> Result<u64, ArchiveProvenanceError> {
        if self.slot_count == 0 {
            return Err(ArchiveProvenanceError::EmptyRuntimeSegment { index });
        }
        validate_text(
            "runtime_segments[].generation_profile",
            &self.generation_profile,
        )?;
        validate_text("runtime_segments[].runtime_profile", &self.runtime_profile)?;
        validate_text(
            "runtime_segments[].runtime_revision",
            &self.runtime_revision,
        )?;
        validate_text(
            "runtime_segments[].runtime_toolchain",
            &self.runtime_toolchain,
        )?;
        if self.write_versions.worker_end_exclusive < self.write_versions.worker_start {
            return Err(ArchiveProvenanceError::InvertedWorkerWriteVersionRange {
                index,
                start: self.write_versions.worker_start,
                end: self.write_versions.worker_end_exclusive,
            });
        }
        if self.write_versions.archive_end_exclusive().is_none() {
            return Err(ArchiveProvenanceError::NormalizedWriteVersionRangeOverflow { index });
        }
        self.slot_end(index)
    }

    fn has_same_producer_identity(&self, other: &Self) -> bool {
        self.generation_profile == other.generation_profile
            && self.runtime_profile == other.runtime_profile
            && self.runtime_admission == other.runtime_admission
            && self.runtime_revision == other.runtime_revision
            && self.runtime_toolchain == other.runtime_toolchain
            && self.worker_executable_sha256 == other.worker_executable_sha256
    }
}

/// State observed independently on one side of a runtime handoff.
///
/// `next_write_version` is deliberately not consensus state. Snapshot-local
/// cursors can differ across validators and runtimes; it is retained only to
/// bind each side to its segment's [`WriteVersionNormalization`].
#[derive(Encode, Decode, Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuntimeStateCheckpoint {
    /// Frozen, complete slot represented by this checkpoint.
    pub slot: u64,
    /// Bank hash at `slot`.
    pub bank_hash: Hash,
    /// Kind of accounts hash recorded by [`Self::accounts_hash`].
    pub accounts_hash_kind: AccountsHashKind,
    /// Accounts-state hash at `slot`.
    pub accounts_hash: Hash,
    /// Last blockhash at `slot`.
    pub last_blockhash: Hash,
    /// Total capitalization at `slot`.
    pub capitalization: u64,
    /// Cumulative processed transaction count at `slot`.
    pub transaction_count: u64,
    /// Runtime tick height at `slot`.
    pub tick_height: u64,
    /// Whether the runtime reported the slot as complete.
    pub slot_complete: bool,
    /// Validator-local write cursor at this checkpoint.
    pub next_write_version: u64,
}

impl RuntimeStateCheckpoint {
    fn consensus_state_eq(&self, other: &Self) -> bool {
        self.slot == other.slot
            && self.bank_hash == other.bank_hash
            && self.accounts_hash_kind == other.accounts_hash_kind
            && self.accounts_hash == other.accounts_hash
            && self.last_blockhash == other.last_blockhash
            && self.capitalization == other.capitalization
            && self.transaction_count == other.transaction_count
            && self.tick_height == other.tick_height
            && self.slot_complete == other.slot_complete
    }
}

/// Independently observed transition between adjacent runtime segments.
#[derive(Encode, Decode, Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuntimeHandoffProvenance {
    /// First slot produced by the successor segment.
    pub boundary_slot: u64,
    /// Frozen terminal checkpoint emitted by the predecessor.
    pub predecessor: RuntimeStateCheckpoint,
    /// Bootstrap checkpoint emitted by the successor before it processes the
    /// boundary slot.
    pub successor: RuntimeStateCheckpoint,
    /// How the successor obtained its bootstrap state.
    pub successor_bootstrap_kind: BootstrapStateKind,
    /// SHA-256 of the exact snapshot archive loaded by the successor.
    ///
    /// Checkpoint hashes alone do not bind every byte in a snapshot archive
    /// (notably historical status-cache data). Recording the artifact digest
    /// proves which complete bootstrap object crossed the runtime boundary.
    pub successor_bootstrap_archive_sha256: [u8; 32],
    /// Account writes emitted while the successor constructed/loaded the
    /// bootstrap checkpoint. Canonical handoffs require this to be zero.
    pub successor_bootstrap_write_count: u64,
}

/// Provenance schema stored by envelope version 3.
///
/// V3 represents a single requested archive range assembled from two or more
/// runtime-selected segments. V1 and V2 remain the canonical representation
/// for a range produced by one runtime.
#[derive(Encode, Decode, Debug, Clone, PartialEq, Eq)]
pub struct ArchiveProvenanceV3 {
    /// Stable identifier for the multi-segment assembly implementation and
    /// validation policy.
    pub assembly_profile: String,
    /// Genesis identity shared by every segment.
    pub genesis_hash: Hash,
    /// How the first segment obtained its initial state.
    pub bootstrap_state_kind: BootstrapStateKind,
    /// State from which the first segment began replay.
    pub bootstrap_state: StateCommitment,
    /// Requested output slot range start, inclusive.
    pub requested_slot_start: u64,
    /// Number of slots in the requested output range.
    pub requested_slot_count: u64,
    /// Slot-derived provenance for transaction status metadata.
    pub transaction_metadata: TransactionMetadataPolicy,
    /// Ordered runtime segments covering the requested range exactly.
    pub runtime_segments: Vec<RuntimeSegmentProvenance>,
    /// Independently checked transitions between each adjacent segment pair.
    pub handoffs: Vec<RuntimeHandoffProvenance>,
}

impl ArchiveProvenanceV1 {
    /// Returns the exclusive end of the requested output range.
    pub fn requested_slot_end(&self) -> Result<u64, ArchiveProvenanceError> {
        self.requested_slot_start
            .checked_add(self.requested_slot_count)
            .ok_or(ArchiveProvenanceError::SlotRangeOverflow)
    }

    fn validate(&self) -> Result<(), ArchiveProvenanceError> {
        validate_text("generation_profile", &self.generation_profile)?;
        validate_text("runtime_profile", &self.runtime_profile)?;
        validate_text("runtime_revision", &self.runtime_revision)?;
        validate_text("runtime_toolchain", &self.runtime_toolchain)?;
        self.requested_slot_end()?;
        Ok(())
    }
}

impl ArchiveProvenanceV3 {
    /// Returns the exclusive end of the requested output range.
    pub fn requested_slot_end(&self) -> Result<u64, ArchiveProvenanceError> {
        self.requested_slot_start
            .checked_add(self.requested_slot_count)
            .ok_or(ArchiveProvenanceError::SlotRangeOverflow)
    }

    /// Checks all structural invariants that can be established from the V3
    /// envelope itself.
    ///
    /// This deliberately does not establish that checkpoint hashes are
    /// canonical chain state or that the named executables produced the
    /// segments. Producers must establish those facts before constructing the
    /// envelope; consumers still need an authenticated archive source.
    pub fn validate(&self) -> Result<(), ArchiveProvenanceError> {
        validate_text("assembly_profile", &self.assembly_profile)?;
        let requested_end = self.requested_slot_end()?;

        let segment_count = self.runtime_segments.len();
        if !(2..=MAX_RUNTIME_PROVENANCE_SEGMENTS).contains(&segment_count) {
            return Err(ArchiveProvenanceError::InvalidRuntimeSegmentCount {
                actual: segment_count,
                min: 2,
                max: MAX_RUNTIME_PROVENANCE_SEGMENTS,
            });
        }

        let expected_handoffs = segment_count - 1;
        if self.handoffs.len() != expected_handoffs {
            return Err(ArchiveProvenanceError::HandoffCountMismatch {
                expected: expected_handoffs,
                actual: self.handoffs.len(),
            });
        }

        let mut expected_slot_start = self.requested_slot_start;
        let mut expected_archive_write_start = None;
        for (index, segment) in self.runtime_segments.iter().enumerate() {
            let segment_end = segment.validate(index)?;
            if segment.slot_start != expected_slot_start {
                return Err(ArchiveProvenanceError::RuntimeSegmentCoverageMismatch {
                    index,
                    expected_start: expected_slot_start,
                    actual_start: segment.slot_start,
                });
            }
            if index > 0 && self.runtime_segments[index - 1].has_same_producer_identity(segment) {
                return Err(ArchiveProvenanceError::RedundantAdjacentRuntimeSegments {
                    first: index - 1,
                    second: index,
                });
            }
            if let Some(expected_start) = expected_archive_write_start
                && segment.write_versions.archive_start != expected_start
            {
                return Err(
                    ArchiveProvenanceError::NormalizedWriteVersionCoverageMismatch {
                        index,
                        expected_start,
                        actual_start: segment.write_versions.archive_start,
                    },
                );
            }
            expected_archive_write_start = segment.write_versions.archive_end_exclusive();
            expected_slot_start = segment_end;
        }
        if expected_slot_start != requested_end {
            return Err(ArchiveProvenanceError::RuntimeSegmentEndMismatch {
                expected_end: requested_end,
                actual_end: expected_slot_start,
            });
        }

        match self.bootstrap_state_kind {
            BootstrapStateKind::Genesis => {
                if self.requested_slot_start != 0 || self.bootstrap_state.slot != 0 {
                    return Err(ArchiveProvenanceError::InvalidGenesisBootstrap {
                        requested_start: self.requested_slot_start,
                        state_slot: self.bootstrap_state.slot,
                    });
                }
            }
            BootstrapStateKind::SnapshotArchive | BootstrapStateKind::CarriedBank => {
                if self.bootstrap_state.slot >= self.requested_slot_start {
                    return Err(ArchiveProvenanceError::BootstrapStateNotBeforeRange {
                        requested_start: self.requested_slot_start,
                        state_slot: self.bootstrap_state.slot,
                    });
                }
            }
        }

        for (index, handoff) in self.handoffs.iter().enumerate() {
            let predecessor_segment = &self.runtime_segments[index];
            let successor_segment = &self.runtime_segments[index + 1];
            let expected_boundary = successor_segment.slot_start;
            if handoff.boundary_slot != expected_boundary {
                return Err(ArchiveProvenanceError::HandoffBoundaryMismatch {
                    index,
                    expected: expected_boundary,
                    actual: handoff.boundary_slot,
                });
            }
            let expected_checkpoint_slot = expected_boundary - 1;
            for (side, checkpoint) in [
                ("predecessor", &handoff.predecessor),
                ("successor", &handoff.successor),
            ] {
                if checkpoint.slot != expected_checkpoint_slot {
                    return Err(ArchiveProvenanceError::HandoffCheckpointSlotMismatch {
                        index,
                        side,
                        expected: expected_checkpoint_slot,
                        actual: checkpoint.slot,
                    });
                }
                if !checkpoint.slot_complete {
                    return Err(ArchiveProvenanceError::IncompleteHandoffCheckpoint {
                        index,
                        side,
                    });
                }
            }
            if !handoff.predecessor.consensus_state_eq(&handoff.successor) {
                return Err(ArchiveProvenanceError::HandoffStateMismatch { index });
            }
            if handoff.successor_bootstrap_kind == BootstrapStateKind::Genesis {
                return Err(ArchiveProvenanceError::InvalidSuccessorBootstrapKind { index });
            }
            if handoff.successor_bootstrap_archive_sha256 == [0; 32] {
                return Err(
                    ArchiveProvenanceError::MissingSuccessorBootstrapArchiveDigest { index },
                );
            }
            if handoff.successor_bootstrap_write_count != 0 {
                return Err(ArchiveProvenanceError::SuccessorBootstrapEmittedWrites {
                    index,
                    actual: handoff.successor_bootstrap_write_count,
                });
            }
            if handoff.predecessor.next_write_version
                != predecessor_segment.write_versions.worker_end_exclusive
            {
                return Err(ArchiveProvenanceError::HandoffWriteCursorMismatch {
                    index,
                    side: "predecessor",
                    expected: predecessor_segment.write_versions.worker_end_exclusive,
                    actual: handoff.predecessor.next_write_version,
                });
            }
            if handoff.successor.next_write_version != successor_segment.write_versions.worker_start
            {
                return Err(ArchiveProvenanceError::HandoffWriteCursorMismatch {
                    index,
                    side: "successor",
                    expected: successor_segment.write_versions.worker_start,
                    actual: handoff.successor.next_write_version,
                });
            }
        }

        Ok(())
    }
}

/// Structured provenance versions understood by this crate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ArchiveProvenance {
    /// Provenance envelope version 1.
    V1(ArchiveProvenanceV1),
    /// Provenance envelope version 2, including a parent-measured worker
    /// executable digest.
    V2(ArchiveProvenanceV2),
    /// Provenance envelope version 3, covering two or more runtime segments.
    V3(ArchiveProvenanceV3),
}

impl ArchiveProvenance {
    /// The independent provenance-envelope version.
    pub const fn version(&self) -> u16 {
        match self {
            Self::V1(_) => ARCHIVE_PROVENANCE_VERSION_V1,
            Self::V2(_) => ARCHIVE_PROVENANCE_VERSION_V2,
            Self::V3(_) => ARCHIVE_PROVENANCE_VERSION_V3,
        }
    }

    /// Returns the common v1 fields when this archive has one runtime.
    ///
    /// Multi-runtime V3 provenance cannot be projected into V1 without losing
    /// material identity and handoff evidence, so it returns `None`.
    pub const fn single_runtime_v1(&self) -> Option<&ArchiveProvenanceV1> {
        match self {
            Self::V1(value) => Some(value),
            Self::V2(value) => Some(&value.base),
            Self::V3(_) => None,
        }
    }

    /// SHA-256 of the out-of-process historical worker for a single-runtime
    /// archive.
    ///
    /// The outer `Option` is `None` for multi-runtime V3. For a single-runtime
    /// archive, the inner value is absent only for V1, which predates
    /// executable binding.
    pub const fn single_runtime_worker_executable_sha256(&self) -> Option<Option<[u8; 32]>> {
        match self {
            Self::V1(_) => Some(None),
            Self::V2(value) => Some(Some(value.worker_executable_sha256)),
            Self::V3(_) => None,
        }
    }

    /// Requested half-open slot range represented by this provenance.
    pub fn requested_range(&self) -> Result<Range<u64>, ArchiveProvenanceError> {
        let (start, count) = match self {
            Self::V1(value) => (value.requested_slot_start, value.requested_slot_count),
            Self::V2(value) => (
                value.base.requested_slot_start,
                value.base.requested_slot_count,
            ),
            Self::V3(value) => (value.requested_slot_start, value.requested_slot_count),
        };
        let end = start
            .checked_add(count)
            .ok_or(ArchiveProvenanceError::SlotRangeOverflow)?;
        Ok(start..end)
    }

    /// Checks the version-specific structure of this envelope.
    pub fn validate(&self) -> Result<(), ArchiveProvenanceError> {
        match self {
            Self::V1(value) => value.validate(),
            Self::V2(value) => value.base.validate(),
            Self::V3(value) => value.validate(),
        }
    }

    /// Checks that this provenance describes the enclosing archive's exact
    /// declared slot range.
    pub fn validate_for_archive(
        &self,
        archive_start: u64,
        archive_count: u64,
    ) -> Result<(), ArchiveProvenanceError> {
        self.validate()?;
        let range = self.requested_range()?;
        let provenance_count = range.end - range.start;
        if range.start != archive_start || provenance_count != archive_count {
            return Err(ArchiveProvenanceError::RequestedRangeMismatch {
                provenance_start: range.start,
                provenance_count,
                archive_start,
                archive_count,
            });
        }
        Ok(())
    }
}

impl From<ArchiveProvenanceV1> for ArchiveProvenance {
    fn from(value: ArchiveProvenanceV1) -> Self {
        Self::V1(value)
    }
}

impl From<ArchiveProvenanceV2> for ArchiveProvenance {
    fn from(value: ArchiveProvenanceV2) -> Self {
        Self::V2(value)
    }
}

impl From<ArchiveProvenanceV3> for ArchiveProvenance {
    fn from(value: ArchiveProvenanceV3) -> Self {
        Self::V3(value)
    }
}

/// Errors returned only when structured provenance is explicitly encoded or
/// parsed.
#[derive(Debug, thiserror::Error)]
pub enum ArchiveProvenanceError {
    #[error("non-empty archive provenance has an unknown magic prefix")]
    BadMagic,
    #[error("archive provenance envelope is truncated")]
    Truncated,
    #[error("unsupported archive provenance version {0}")]
    UnsupportedVersion(u16),
    #[error("archive provenance is {bytes} bytes (limit {limit})")]
    TooLarge { bytes: usize, limit: usize },
    #[error("archive provenance field {0} must not be empty")]
    EmptyField(&'static str),
    #[error("archive provenance field {field} is {bytes} bytes (limit {limit})")]
    FieldTooLarge {
        field: &'static str,
        bytes: usize,
        limit: usize,
    },
    #[error("archive provenance requested slot range overflows u64")]
    SlotRangeOverflow,
    #[error("archive provenance has {actual} runtime segments; expected {min}..={max} for V3")]
    InvalidRuntimeSegmentCount {
        actual: usize,
        min: usize,
        max: usize,
    },
    #[error("archive provenance runtime segment {index} is empty")]
    EmptyRuntimeSegment { index: usize },
    #[error("archive provenance runtime segment {index} slot range overflows u64")]
    RuntimeSegmentRangeOverflow { index: usize },
    #[error(
        "archive provenance runtime segment {index} starts at {actual_start}, expected {expected_start}"
    )]
    RuntimeSegmentCoverageMismatch {
        index: usize,
        expected_start: u64,
        actual_start: u64,
    },
    #[error(
        "archive provenance runtime segments end at {actual_end}, expected requested end {expected_end}"
    )]
    RuntimeSegmentEndMismatch { expected_end: u64, actual_end: u64 },
    #[error(
        "archive provenance runtime segments {first} and {second} have the same producer identity and must be coalesced"
    )]
    RedundantAdjacentRuntimeSegments { first: usize, second: usize },
    #[error(
        "archive provenance runtime segment {index} has inverted worker write range [{start}, {end})"
    )]
    InvertedWorkerWriteVersionRange { index: usize, start: u64, end: u64 },
    #[error(
        "archive provenance runtime segment {index} normalized write-version range overflows u64"
    )]
    NormalizedWriteVersionRangeOverflow { index: usize },
    #[error(
        "archive provenance runtime segment {index} normalized writes start at {actual_start}, expected {expected_start}"
    )]
    NormalizedWriteVersionCoverageMismatch {
        index: usize,
        expected_start: u64,
        actual_start: u64,
    },
    #[error("archive provenance has {actual} handoffs, expected {expected}")]
    HandoffCountMismatch { expected: usize, actual: usize },
    #[error("archive provenance handoff {index} boundary is {actual}, expected {expected}")]
    HandoffBoundaryMismatch {
        index: usize,
        expected: u64,
        actual: u64,
    },
    #[error(
        "archive provenance handoff {index} {side} checkpoint is for slot {actual}, expected {expected}"
    )]
    HandoffCheckpointSlotMismatch {
        index: usize,
        side: &'static str,
        expected: u64,
        actual: u64,
    },
    #[error("archive provenance handoff {index} {side} checkpoint is incomplete")]
    IncompleteHandoffCheckpoint { index: usize, side: &'static str },
    #[error(
        "archive provenance handoff {index} predecessor and successor consensus checkpoints differ"
    )]
    HandoffStateMismatch { index: usize },
    #[error("archive provenance handoff {index} cannot bootstrap its successor from genesis")]
    InvalidSuccessorBootstrapKind { index: usize },
    #[error("archive provenance handoff {index} has no successor bootstrap archive SHA-256")]
    MissingSuccessorBootstrapArchiveDigest { index: usize },
    #[error(
        "archive provenance handoff {index} successor bootstrap emitted {actual} account writes; expected zero"
    )]
    SuccessorBootstrapEmittedWrites { index: usize, actual: u64 },
    #[error(
        "archive provenance handoff {index} {side} next write version is {actual}, expected {expected}"
    )]
    HandoffWriteCursorMismatch {
        index: usize,
        side: &'static str,
        expected: u64,
        actual: u64,
    },
    #[error(
        "archive provenance genesis bootstrap has requested start {requested_start} and state slot {state_slot}; both must be zero"
    )]
    InvalidGenesisBootstrap {
        requested_start: u64,
        state_slot: u64,
    },
    #[error(
        "archive provenance bootstrap state slot {state_slot} is not before requested start {requested_start}"
    )]
    BootstrapStateNotBeforeRange {
        requested_start: u64,
        state_slot: u64,
    },
    #[error(
        "archive provenance requested range [{provenance_start}, +{provenance_count}) does not match archive range [{archive_start}, +{archive_count})"
    )]
    RequestedRangeMismatch {
        provenance_start: u64,
        provenance_count: u64,
        archive_start: u64,
        archive_count: u64,
    },
    #[error("could not encode archive provenance: {0}")]
    Encode(lencode::io::Error),
    #[error("malformed archive provenance payload: {0}")]
    Decode(lencode::io::Error),
}

fn validate_text(field: &'static str, value: &str) -> Result<(), ArchiveProvenanceError> {
    if value.is_empty() {
        return Err(ArchiveProvenanceError::EmptyField(field));
    }
    if value.len() > MAX_ARCHIVE_PROVENANCE_TEXT_BYTES {
        return Err(ArchiveProvenanceError::FieldTooLarge {
            field,
            bytes: value.len(),
            limit: MAX_ARCHIVE_PROVENANCE_TEXT_BYTES,
        });
    }
    Ok(())
}

/// Encodes structured provenance for storage in [`ArchiveMeta::reserved`].
pub fn encode_archive_provenance(
    provenance: &ArchiveProvenance,
) -> Result<Vec<u8>, ArchiveProvenanceError> {
    provenance.validate()?;

    let mut encoded = Vec::with_capacity(256);
    encoded.extend_from_slice(&ARCHIVE_PROVENANCE_MAGIC);
    encoded.extend_from_slice(&provenance.version().to_le_bytes());
    match provenance {
        ArchiveProvenance::V1(value) => value
            .encode_ext(&mut encoded, None)
            .map_err(ArchiveProvenanceError::Encode)?,
        ArchiveProvenance::V2(value) => value
            .encode_ext(&mut encoded, None)
            .map_err(ArchiveProvenanceError::Encode)?,
        ArchiveProvenance::V3(value) => value
            .encode_ext(&mut encoded, None)
            .map_err(ArchiveProvenanceError::Encode)?,
    };
    if encoded.len() > MAX_ARCHIVE_PROVENANCE_BYTES {
        return Err(ArchiveProvenanceError::TooLarge {
            bytes: encoded.len(),
            limit: MAX_ARCHIVE_PROVENANCE_BYTES,
        });
    }
    Ok(encoded)
}

/// Decodes structured provenance from [`ArchiveMeta::reserved`].
///
/// Empty bytes are the backward-compatible representation of an archive that
/// predates provenance. Every non-empty value must be a complete, known
/// envelope; callers that opt into this parser therefore fail closed.
pub fn decode_archive_provenance(
    reserved: &[u8],
) -> Result<Option<ArchiveProvenance>, ArchiveProvenanceError> {
    if reserved.is_empty() {
        return Ok(None);
    }
    if reserved.len() > MAX_ARCHIVE_PROVENANCE_BYTES {
        return Err(ArchiveProvenanceError::TooLarge {
            bytes: reserved.len(),
            limit: MAX_ARCHIVE_PROVENANCE_BYTES,
        });
    }
    if reserved.len() < ARCHIVE_PROVENANCE_MAGIC.len() {
        return Err(ArchiveProvenanceError::Truncated);
    }
    if reserved[..ARCHIVE_PROVENANCE_MAGIC.len()] != ARCHIVE_PROVENANCE_MAGIC {
        return Err(ArchiveProvenanceError::BadMagic);
    }
    if reserved.len() < PROVENANCE_PREFIX_BYTES {
        return Err(ArchiveProvenanceError::Truncated);
    }

    let version = u16::from_le_bytes([
        reserved[ARCHIVE_PROVENANCE_MAGIC.len()],
        reserved[ARCHIVE_PROVENANCE_MAGIC.len() + 1],
    ]);
    let payload = &reserved[PROVENANCE_PREFIX_BYTES..];
    let provenance = match version {
        ARCHIVE_PROVENANCE_VERSION_V1 => {
            let value: ArchiveProvenanceV1 = decode_exact_with_limits(
                payload,
                None,
                DecodeLimits::new(
                    payload.len(),
                    MAX_ARCHIVE_PROVENANCE_TEXT_BYTES,
                    MAX_ARCHIVE_PROVENANCE_BYTES,
                ),
            )
            .map_err(ArchiveProvenanceError::Decode)?;
            value.validate()?;
            ArchiveProvenance::V1(value)
        }
        ARCHIVE_PROVENANCE_VERSION_V2 => {
            let value: ArchiveProvenanceV2 = decode_exact_with_limits(
                payload,
                None,
                DecodeLimits::new(
                    payload.len(),
                    MAX_ARCHIVE_PROVENANCE_TEXT_BYTES,
                    MAX_ARCHIVE_PROVENANCE_BYTES,
                ),
            )
            .map_err(ArchiveProvenanceError::Decode)?;
            value.base.validate()?;
            ArchiveProvenance::V2(value)
        }
        ARCHIVE_PROVENANCE_VERSION_V3 => {
            let value: ArchiveProvenanceV3 = decode_exact_with_limits(
                payload,
                None,
                DecodeLimits::new(
                    payload.len(),
                    MAX_ARCHIVE_PROVENANCE_TEXT_BYTES,
                    MAX_ARCHIVE_PROVENANCE_BYTES,
                ),
            )
            .map_err(ArchiveProvenanceError::Decode)?;
            value.validate()?;
            ArchiveProvenance::V3(value)
        }
        other => return Err(ArchiveProvenanceError::UnsupportedVersion(other)),
    };
    Ok(Some(provenance))
}

impl ArchiveMeta {
    /// Explicitly parses this header's optional structured provenance.
    pub fn provenance(&self) -> Result<Option<ArchiveProvenance>, ArchiveProvenanceError> {
        decode_archive_provenance(&self.reserved)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::{Digest, Sha256};

    fn sample_v1_value() -> ArchiveProvenanceV1 {
        ArchiveProvenanceV1 {
            generation_profile: "jetstreamer-node/historical-replay-v1".into(),
            runtime_profile: "solana-v1.0.24".into(),
            runtime_admission: RuntimeAdmission::Candidate,
            runtime_revision: "a93915f1bddb73480f86fc09f487315ae191897d".into(),
            runtime_toolchain: "rustc-1.43.0-x86_64-unknown-linux-gnu".into(),
            genesis_hash: Hash::new_from_array([5; 32]),
            bootstrap_state_kind: BootstrapStateKind::SnapshotArchive,
            bootstrap_slot: 416_012,
            bootstrap_state_hash: Hash::new_from_array([7; 32]),
            requested_slot_start: 432_000,
            requested_slot_count: 432_000,
            transaction_metadata: TransactionMetadataPolicy::runtime_reconstructed_before(
                157 * 432_000,
            ),
        }
    }

    fn sample() -> ArchiveProvenance {
        sample_v1_value().into()
    }

    fn sample_v2() -> ArchiveProvenance {
        ArchiveProvenanceV2 {
            base: sample_v1_value(),
            worker_executable_sha256: [0xa5; 32],
        }
        .into()
    }

    fn sample_checkpoint(next_write_version: u64) -> RuntimeStateCheckpoint {
        RuntimeStateCheckpoint {
            slot: 618_196,
            bank_hash: Hash::new_from_array([0x11; 32]),
            accounts_hash_kind: AccountsHashKind::LegacyAccountsHash,
            accounts_hash: Hash::new_from_array([0x22; 32]),
            last_blockhash: Hash::new_from_array([0x33; 32]),
            capitalization: 12_345_678,
            transaction_count: 987_654,
            tick_height: 42_000_000,
            slot_complete: true,
            next_write_version,
        }
    }

    fn sample_v3_value() -> ArchiveProvenanceV3 {
        ArchiveProvenanceV3 {
            assembly_profile: "jetstreamer-node/runtime-segment-assembly-v1".into(),
            genesis_hash: Hash::new_from_array([5; 32]),
            bootstrap_state_kind: BootstrapStateKind::SnapshotArchive,
            bootstrap_state: StateCommitment {
                slot: 416_012,
                kind: StateCommitmentKind::LegacyAccountsHash,
                hash: Hash::new_from_array([7; 32]),
            },
            requested_slot_start: 432_000,
            requested_slot_count: 432_000,
            transaction_metadata: TransactionMetadataPolicy::runtime_reconstructed_before(
                157 * 432_000,
            ),
            runtime_segments: vec![
                RuntimeSegmentProvenance {
                    slot_start: 432_000,
                    slot_count: 186_197,
                    generation_profile: "jetstreamer-node/historical-replay-v1".into(),
                    runtime_profile: "solana-v1.0.24".into(),
                    runtime_admission: RuntimeAdmission::Candidate,
                    runtime_revision: "a93915f1bddb73480f86fc09f487315ae191897d".into(),
                    runtime_toolchain: "rustc-1.43.0-x86_64-unknown-linux-gnu".into(),
                    worker_executable_sha256: Some([0xa5; 32]),
                    write_versions: WriteVersionNormalization {
                        worker_start: 100,
                        worker_end_exclusive: 150,
                        archive_start: 10_000,
                    },
                },
                RuntimeSegmentProvenance {
                    slot_start: 618_197,
                    slot_count: 245_803,
                    generation_profile: "jetstreamer-node/current-replay-v1".into(),
                    runtime_profile: "agave-v3".into(),
                    runtime_admission: RuntimeAdmission::Verified,
                    runtime_revision: "agave-v3-revision".into(),
                    runtime_toolchain: "rustc-1.89.0-x86_64-unknown-linux-gnu".into(),
                    worker_executable_sha256: None,
                    write_versions: WriteVersionNormalization {
                        // Deliberately unrelated to the predecessor's raw
                        // cursor: snapshot-local cursors are not consensus.
                        worker_start: 9_000,
                        worker_end_exclusive: 9_075,
                        archive_start: 10_050,
                    },
                },
            ],
            handoffs: vec![RuntimeHandoffProvenance {
                boundary_slot: 618_197,
                predecessor: sample_checkpoint(150),
                successor: sample_checkpoint(9_000),
                successor_bootstrap_kind: BootstrapStateKind::SnapshotArchive,
                successor_bootstrap_archive_sha256: [0xb7; 32],
                successor_bootstrap_write_count: 0,
            }],
        }
    }

    fn sample_v3() -> ArchiveProvenance {
        sample_v3_value().into()
    }

    #[test]
    fn provenance_roundtrip_and_slot_semantics() {
        let expected = sample();
        let encoded = encode_archive_provenance(&expected).unwrap();
        assert_eq!(
            decode_archive_provenance(&encoded).unwrap(),
            Some(expected.clone())
        );
        let expected_v1 = expected.single_runtime_v1().unwrap();
        assert_eq!(expected_v1.requested_slot_end().unwrap(), 864_000);
        assert_eq!(
            expected_v1.transaction_metadata.semantics_for_slot(1),
            TransactionMetadataSemantics::RuntimeReconstructedStatusOnly
        );
        assert_eq!(
            expected_v1
                .transaction_metadata
                .semantics_for_slot(157 * 432_000),
            TransactionMetadataSemantics::Observed
        );

        let mut carried = expected_v1.clone();
        carried.bootstrap_state_kind = BootstrapStateKind::CarriedBank;
        carried.bootstrap_slot = 431_999;
        carried.bootstrap_state_hash = Hash::new_from_array([8; 32]);
        let carried = ArchiveProvenance::V1(carried);
        let encoded = encode_archive_provenance(&carried).unwrap();
        assert_eq!(decode_archive_provenance(&encoded).unwrap(), Some(carried));
    }

    #[test]
    fn worker_bound_v2_roundtrips_without_changing_v1_decoding() {
        let legacy = sample();
        let legacy_encoded = encode_archive_provenance(&legacy).unwrap();
        assert_eq!(legacy_encoded.len(), 227);
        assert_eq!(
            format!("{:x}", Sha256::digest(&legacy_encoded)),
            "fc2fa2933338acf7059c29f291eeaf491312fbf883da7aace299b51b01f831d3"
        );
        assert_eq!(
            &legacy_encoded[ARCHIVE_PROVENANCE_MAGIC.len()..PROVENANCE_PREFIX_BYTES],
            &ARCHIVE_PROVENANCE_VERSION_V1.to_le_bytes()
        );
        assert_eq!(
            decode_archive_provenance(&legacy_encoded).unwrap(),
            Some(legacy)
        );

        let worker_bound = sample_v2();
        let encoded = encode_archive_provenance(&worker_bound).unwrap();
        assert_eq!(encoded.len(), 259);
        assert_eq!(
            format!("{:x}", Sha256::digest(&encoded)),
            "fb40db93ba1b46530052ae2832a5c6e0c5f29b557a16ca5603d8516a739e7f64"
        );
        assert_eq!(
            &encoded[ARCHIVE_PROVENANCE_MAGIC.len()..PROVENANCE_PREFIX_BYTES],
            &ARCHIVE_PROVENANCE_VERSION_V2.to_le_bytes()
        );
        assert_eq!(
            worker_bound.single_runtime_worker_executable_sha256(),
            Some(Some([0xa5; 32]))
        );
        assert_eq!(
            decode_archive_provenance(&encoded).unwrap(),
            Some(worker_bound)
        );
    }

    #[test]
    fn all_runtime_reconstructed_policy_has_no_observed_side() {
        let policy = TransactionMetadataPolicy::runtime_reconstructed();
        assert_eq!(
            policy.semantics_for_slot(0),
            TransactionMetadataSemantics::RuntimeReconstructedStatusOnly
        );
        assert_eq!(
            policy.semantics_for_slot(u64::MAX),
            TransactionMetadataSemantics::RuntimeReconstructedStatusOnly
        );
    }

    #[test]
    fn reconstructed_fee_policy_changes_at_its_declared_slot() {
        let policy = TransactionMetadataPolicy::runtime_reconstructed_with_fee_from(42);
        assert_eq!(
            policy.semantics_for_slot(41),
            TransactionMetadataSemantics::RuntimeReconstructedStatusOnly
        );
        assert_eq!(
            policy.semantics_for_slot(42),
            TransactionMetadataSemantics::RuntimeReconstructedStatusAndFee
        );

        let all = TransactionMetadataPolicy::runtime_reconstructed_status_and_fee();
        assert_eq!(
            all.semantics_for_slot(0),
            TransactionMetadataSemantics::RuntimeReconstructedStatusAndFee
        );
        assert_eq!(
            all.semantics_for_slot(u64::MAX),
            TransactionMetadataSemantics::RuntimeReconstructedStatusAndFee
        );
    }

    #[test]
    fn multi_runtime_v3_roundtrips_and_keeps_raw_write_cursors_local() {
        let expected = sample_v3();
        expected.validate().unwrap();
        assert_eq!(expected.version(), ARCHIVE_PROVENANCE_VERSION_V3);
        assert_eq!(expected.requested_range().unwrap(), 432_000..864_000);
        assert_eq!(expected.single_runtime_v1(), None);
        assert_eq!(expected.single_runtime_worker_executable_sha256(), None);

        let ArchiveProvenance::V3(value) = &expected else {
            unreachable!();
        };
        let predecessor = value.runtime_segments[0].write_versions;
        let successor = value.runtime_segments[1].write_versions;
        assert_eq!(predecessor.normalize(99), None);
        assert_eq!(predecessor.normalize(100), Some(10_000));
        assert_eq!(predecessor.normalize(149), Some(10_049));
        assert_eq!(predecessor.normalize(150), None);
        assert_eq!(predecessor.archive_end_exclusive(), Some(10_050));
        assert_eq!(successor.normalize(9_000), Some(10_050));
        assert_ne!(
            value.handoffs[0].predecessor.next_write_version,
            value.handoffs[0].successor.next_write_version,
            "validator-local write cursors are intentionally not a handoff equality"
        );

        let encoded = encode_archive_provenance(&expected).unwrap();
        assert_eq!(
            &encoded[ARCHIVE_PROVENANCE_MAGIC.len()..PROVENANCE_PREFIX_BYTES],
            &ARCHIVE_PROVENANCE_VERSION_V3.to_le_bytes()
        );
        assert_eq!(decode_archive_provenance(&encoded).unwrap(), Some(expected));
    }

    #[test]
    fn v3_rejects_noncanonical_segment_layouts_and_write_maps() {
        let mut value = sample_v3_value();
        value.runtime_segments.clear();
        value.handoffs.clear();
        assert!(matches!(
            value.validate(),
            Err(ArchiveProvenanceError::InvalidRuntimeSegmentCount { actual: 0, .. })
        ));

        let mut value = sample_v3_value();
        value.runtime_segments.truncate(1);
        value.handoffs.clear();
        assert!(matches!(
            value.validate(),
            Err(ArchiveProvenanceError::InvalidRuntimeSegmentCount { actual: 1, .. })
        ));

        let mut value = sample_v3_value();
        value.runtime_segments =
            vec![value.runtime_segments[0].clone(); MAX_RUNTIME_PROVENANCE_SEGMENTS + 1];
        assert!(matches!(
            value.validate(),
            Err(ArchiveProvenanceError::InvalidRuntimeSegmentCount { actual, .. })
                if actual == MAX_RUNTIME_PROVENANCE_SEGMENTS + 1
        ));

        let mut value = sample_v3_value();
        value.runtime_segments[0].slot_count = 0;
        assert!(matches!(
            value.validate(),
            Err(ArchiveProvenanceError::EmptyRuntimeSegment { index: 0 })
        ));

        for actual_start in [618_196, 618_198] {
            let mut value = sample_v3_value();
            value.runtime_segments[1].slot_start = actual_start;
            assert!(matches!(
                value.validate(),
                Err(ArchiveProvenanceError::RuntimeSegmentCoverageMismatch { index: 1, .. })
            ));
        }

        let mut value = sample_v3_value();
        value.runtime_segments[1].slot_count -= 1;
        assert!(matches!(
            value.validate(),
            Err(ArchiveProvenanceError::RuntimeSegmentEndMismatch { .. })
        ));

        let mut value = sample_v3_value();
        value.requested_slot_start = 0;
        value.requested_slot_count = 100;
        value.runtime_segments[0].slot_start = 0;
        value.runtime_segments[0].slot_count = 10;
        value.runtime_segments[1].slot_start = 10;
        value.runtime_segments[1].slot_count = u64::MAX;
        assert!(matches!(
            value.validate(),
            Err(ArchiveProvenanceError::RuntimeSegmentRangeOverflow { index: 1 })
        ));

        let mut value = sample_v3_value();
        value.runtime_segments[0].generation_profile.clear();
        assert!(matches!(
            value.validate(),
            Err(ArchiveProvenanceError::EmptyField(
                "runtime_segments[].generation_profile"
            ))
        ));

        let mut value = sample_v3_value();
        let first = value.runtime_segments[0].clone();
        let second = &mut value.runtime_segments[1];
        second.generation_profile = first.generation_profile;
        second.runtime_profile = first.runtime_profile;
        second.runtime_admission = first.runtime_admission;
        second.runtime_revision = first.runtime_revision;
        second.runtime_toolchain = first.runtime_toolchain;
        second.worker_executable_sha256 = first.worker_executable_sha256;
        assert!(matches!(
            value.validate(),
            Err(ArchiveProvenanceError::RedundantAdjacentRuntimeSegments {
                first: 0,
                second: 1
            })
        ));

        let mut value = sample_v3_value();
        value.runtime_segments[0]
            .write_versions
            .worker_end_exclusive = 99;
        assert!(matches!(
            value.validate(),
            Err(ArchiveProvenanceError::InvertedWorkerWriteVersionRange { index: 0, .. })
        ));

        let mut value = sample_v3_value();
        value.runtime_segments[0].write_versions.archive_start = u64::MAX;
        assert!(matches!(
            value.validate(),
            Err(ArchiveProvenanceError::NormalizedWriteVersionRangeOverflow { index: 0 })
        ));

        let mut value = sample_v3_value();
        value.runtime_segments[1].write_versions.archive_start += 1;
        assert!(matches!(
            value.validate(),
            Err(ArchiveProvenanceError::NormalizedWriteVersionCoverageMismatch { index: 1, .. })
        ));
    }

    #[test]
    fn v3_rejects_unproven_or_inconsistent_handoffs() {
        let mut value = sample_v3_value();
        value.handoffs.clear();
        assert!(matches!(
            value.validate(),
            Err(ArchiveProvenanceError::HandoffCountMismatch {
                expected: 1,
                actual: 0
            })
        ));

        let mut value = sample_v3_value();
        value.handoffs[0].boundary_slot += 1;
        assert!(matches!(
            value.validate(),
            Err(ArchiveProvenanceError::HandoffBoundaryMismatch { index: 0, .. })
        ));

        for side in ["predecessor", "successor"] {
            let mut value = sample_v3_value();
            if side == "predecessor" {
                value.handoffs[0].predecessor.slot -= 1;
            } else {
                value.handoffs[0].successor.slot -= 1;
            }
            assert!(matches!(
                value.validate(),
                Err(ArchiveProvenanceError::HandoffCheckpointSlotMismatch {
                    index: 0,
                    side: actual_side,
                    ..
                }) if actual_side == side
            ));
        }

        let mut value = sample_v3_value();
        value.handoffs[0].successor.slot_complete = false;
        assert!(matches!(
            value.validate(),
            Err(ArchiveProvenanceError::IncompleteHandoffCheckpoint {
                index: 0,
                side: "successor"
            })
        ));

        let mut mismatches = Vec::new();
        let mut value = sample_v3_value();
        value.handoffs[0].successor.bank_hash = Hash::new_from_array([0x44; 32]);
        mismatches.push(value);
        let mut value = sample_v3_value();
        value.handoffs[0].successor.accounts_hash_kind = AccountsHashKind::AccountsLtHash;
        mismatches.push(value);
        let mut value = sample_v3_value();
        value.handoffs[0].successor.accounts_hash = Hash::new_from_array([0x44; 32]);
        mismatches.push(value);
        let mut value = sample_v3_value();
        value.handoffs[0].successor.last_blockhash = Hash::new_from_array([0x44; 32]);
        mismatches.push(value);
        let mut value = sample_v3_value();
        value.handoffs[0].successor.capitalization += 1;
        mismatches.push(value);
        let mut value = sample_v3_value();
        value.handoffs[0].successor.transaction_count += 1;
        mismatches.push(value);
        let mut value = sample_v3_value();
        value.handoffs[0].successor.tick_height += 1;
        mismatches.push(value);
        for value in mismatches {
            assert!(matches!(
                value.validate(),
                Err(ArchiveProvenanceError::HandoffStateMismatch { index: 0 })
            ));
        }

        let mut value = sample_v3_value();
        value.handoffs[0].successor_bootstrap_kind = BootstrapStateKind::Genesis;
        assert!(matches!(
            value.validate(),
            Err(ArchiveProvenanceError::InvalidSuccessorBootstrapKind { index: 0 })
        ));

        let mut value = sample_v3_value();
        value.handoffs[0].successor_bootstrap_archive_sha256 = [0; 32];
        assert!(matches!(
            value.validate(),
            Err(ArchiveProvenanceError::MissingSuccessorBootstrapArchiveDigest { index: 0 })
        ));

        let mut value = sample_v3_value();
        value.handoffs[0].successor_bootstrap_write_count = 1;
        assert!(matches!(
            value.validate(),
            Err(ArchiveProvenanceError::SuccessorBootstrapEmittedWrites {
                index: 0,
                actual: 1
            })
        ));

        let mut value = sample_v3_value();
        value.handoffs[0].predecessor.next_write_version -= 1;
        assert!(matches!(
            value.validate(),
            Err(ArchiveProvenanceError::HandoffWriteCursorMismatch {
                index: 0,
                side: "predecessor",
                ..
            })
        ));

        let mut value = sample_v3_value();
        value.handoffs[0].successor.next_write_version -= 1;
        assert!(matches!(
            value.validate(),
            Err(ArchiveProvenanceError::HandoffWriteCursorMismatch {
                index: 0,
                side: "successor",
                ..
            })
        ));
    }

    #[test]
    fn v3_bootstrap_must_precede_range_unless_it_is_genesis() {
        let mut value = sample_v3_value();
        value.bootstrap_state.slot = value.requested_slot_start;
        assert!(matches!(
            value.validate(),
            Err(ArchiveProvenanceError::BootstrapStateNotBeforeRange { .. })
        ));

        let mut value = sample_v3_value();
        value.bootstrap_state_kind = BootstrapStateKind::Genesis;
        assert!(matches!(
            value.validate(),
            Err(ArchiveProvenanceError::InvalidGenesisBootstrap { .. })
        ));

        let mut value = sample_v3_value();
        value.requested_slot_start = 0;
        value.requested_slot_count = 2;
        value.bootstrap_state_kind = BootstrapStateKind::Genesis;
        value.bootstrap_state.slot = 0;
        value.runtime_segments[0].slot_start = 0;
        value.runtime_segments[0].slot_count = 1;
        value.runtime_segments[1].slot_start = 1;
        value.runtime_segments[1].slot_count = 1;
        value.handoffs[0].boundary_slot = 1;
        value.handoffs[0].predecessor.slot = 0;
        value.handoffs[0].successor.slot = 0;
        value.validate().unwrap();
    }

    #[test]
    fn requested_range_helpers_cover_every_version_and_fail_on_overflow() {
        assert_eq!(sample().requested_range().unwrap(), 432_000..864_000);
        assert_eq!(sample_v2().requested_range().unwrap(), 432_000..864_000);
        assert_eq!(sample_v3().requested_range().unwrap(), 432_000..864_000);
        assert!(sample_v3().validate_for_archive(432_000, 432_000).is_ok());
        assert!(matches!(
            sample_v3().validate_for_archive(432_001, 431_999),
            Err(ArchiveProvenanceError::RequestedRangeMismatch { .. })
        ));

        let mut value = sample_v1_value();
        value.requested_slot_start = u64::MAX;
        value.requested_slot_count = 1;
        assert!(matches!(
            ArchiveProvenance::V1(value).requested_range(),
            Err(ArchiveProvenanceError::SlotRangeOverflow)
        ));
    }

    #[test]
    fn empty_reserved_is_legacy_archive_without_provenance() {
        assert_eq!(decode_archive_provenance(&[]).unwrap(), None);
    }

    #[test]
    fn malformed_and_unknown_envelopes_fail_closed() {
        assert!(matches!(
            decode_archive_provenance(b"reserved"),
            Err(ArchiveProvenanceError::BadMagic)
        ));

        let mut truncated = ARCHIVE_PROVENANCE_MAGIC.to_vec();
        truncated.push(1);
        assert!(matches!(
            decode_archive_provenance(&truncated),
            Err(ArchiveProvenanceError::Truncated)
        ));

        let mut unknown = ARCHIVE_PROVENANCE_MAGIC.to_vec();
        unknown.extend_from_slice(&999u16.to_le_bytes());
        assert!(matches!(
            decode_archive_provenance(&unknown),
            Err(ArchiveProvenanceError::UnsupportedVersion(999))
        ));

        let mut trailing = encode_archive_provenance(&sample()).unwrap();
        trailing.push(0);
        assert!(matches!(
            decode_archive_provenance(&trailing),
            Err(ArchiveProvenanceError::Decode(
                lencode::io::Error::TrailingData
            ))
        ));
    }
}
