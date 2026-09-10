//! Slot-driven compatibility planning for historical replay.
//!
//! Archive decoding, transaction execution, and Horizon output are separate
//! compatibility dimensions. Their boundaries need not coincide, so this
//! module plans them independently and returns combined half-open slot
//! segments. A snapshot is state, not an execution-version signal; it is
//! deliberately absent from the selection API.

use {
    core::fmt,
    jetstreamer_firehose::firehose::{
        OLD_FAITHFUL_PROTOBUF_META_START_SLOT, OldFaithfulMetaEncoding, old_faithful_meta_encoding,
    },
    solana_clock::Slot,
    solana_hash::Hash,
    std::{ops::Range, str::FromStr},
};

/// End of epoch 7, the last range both executable and bootstrap-compatible
/// with the current v1.0.8 worker. Epoch 8's boundary snapshot uses the newer
/// 1.1.0 bank schema and must remain fail-closed until a matching historical
/// worker passes differential replay.
pub const SOLANA_V1_0_8_CANDIDATE_END_SLOT_EXCLUSIVE: Slot = 3_456_000;

/// Epoch 8 is the first output range whose best available predecessor
/// snapshot uses the 1.1 bank schema. The exact v1.0.13 worker has restored
/// that snapshot and is admitted here only as a checkpoint-verified
/// differential candidate.
pub const SOLANA_V1_0_13_CANDIDATE_START_SLOT: Slot = SOLANA_V1_0_8_CANDIDATE_END_SLOT_EXCLUSIVE;
pub const SOLANA_V1_0_13_CANDIDATE_END_SLOT_EXCLUSIVE: Slot = 3_888_000;

/// Epochs 9 through 11 use the exact v1.0.14 runtime. Each epoch is restored
/// from its own canonical predecessor-epoch snapshot and verified
/// independently.
pub const SOLANA_V1_0_14_CANDIDATE_START_SLOT: Slot = SOLANA_V1_0_13_CANDIDATE_END_SLOT_EXCLUSIVE;
pub const SOLANA_V1_0_14_CANDIDATE_END_SLOT_EXCLUSIVE: Slot = 5_184_000;

/// Canonical predecessor snapshot used to start the v1.0.23 epoch-12 worker.
/// Its successor through the end of epoch 11 is warmup input; Horizon output
/// starts at the epoch-12 boundary. Production epoch-12 bootstrap selection
/// must bind both this slot and its canonical legacy accounts hash.
pub const SOLANA_V1_0_23_INITIAL_SNAPSHOT_SLOT: Slot = 5_183_736;
pub const SOLANA_V1_0_23_INITIAL_SNAPSHOT_ACCOUNTS_HASH: &str =
    "BUqwiSm2GgH9ByKrBDF6epXHYK9RRh3vyZDKtUqtMXfR";
pub const SOLANA_V1_0_23_INITIAL_REPLAY_SLOT: Slot = SOLANA_V1_0_23_INITIAL_SNAPSHOT_SLOT + 1;

/// Epoch-aligned diagnostic envelopes for the terminal patch of each later
/// Solana minor line. These are candidate-only: an epoch is publishable only
/// after every canonical snapshot checkpoint in its span matches. A mismatch
/// fails closed and is evidence to split that envelope around an earlier exact
/// patch worker; release-marker timestamps are not treated as activation slots.
pub const SOLANA_V1_0_23_CANDIDATE_START_SLOT: Slot = SOLANA_V1_0_14_CANDIDATE_END_SLOT_EXCLUSIVE;
pub const SOLANA_V1_0_23_CANDIDATE_END_SLOT_EXCLUSIVE: Slot = 12_960_000;
pub const SOLANA_V1_1_23_CANDIDATE_START_SLOT: Slot = SOLANA_V1_0_23_CANDIDATE_END_SLOT_EXCLUSIVE;
pub const SOLANA_V1_1_23_CANDIDATE_END_SLOT_EXCLUSIVE: Slot = 26_352_000;
pub const SOLANA_V1_2_32_CANDIDATE_START_SLOT: Slot = SOLANA_V1_1_23_CANDIDATE_END_SLOT_EXCLUSIVE;
pub const SOLANA_V1_2_32_CANDIDATE_END_SLOT_EXCLUSIVE: Slot = 39_744_000;
pub const SOLANA_V1_3_19_CANDIDATE_START_SLOT: Slot = SOLANA_V1_2_32_CANDIDATE_END_SLOT_EXCLUSIVE;
pub const SOLANA_V1_3_19_CANDIDATE_END_SLOT_EXCLUSIVE: Slot = 43_632_000;

/// Start of epoch 940, the earliest Agave 3 range currently backed by locally
/// generated Horizon archives and canonical snapshot checks in this checkout.
pub const AGAVE_V3_VERIFIED_START_SLOT: Slot = 406_080_000;

/// First slot at which the Old Faithful archive is known to carry transaction
/// status metadata for every transaction. The last observed missing record is
/// in slot 4,258,771; slots 4,258,772 through 4,258,775 are absent.
pub const OLD_FAITHFUL_STATUS_REQUIRED_START_SLOT: Slot = 4_258_776;

/// End of the historical range for which source transaction-to-status
/// association is treated as untrusted. The old status writer paired
/// original-order transactions with results produced in randomized execution
/// order. The affected producer cutover is not encoded in the archive, and a
/// mismatch is observed at the first slot of the v1.1 candidate envelope, so
/// replay stays fail-safe through the currently qualified epoch 0-100 scope.
pub const OLD_FAITHFUL_UNTRUSTED_STATUS_ASSOCIATION_END_SLOT_EXCLUSIVE: Slot =
    SOLANA_V1_3_19_CANDIDATE_END_SLOT_EXCLUSIVE;

/// Mainnet genesis identity every historical worker must verify before it
/// accepts replay input.
pub const MAINNET_GENESIS_HASH: &str = "5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d";

/// Exact upstream revisions behind the historical runtime candidates.
pub const SOLANA_V1_0_7_REVISION: &str = "57abc370fa39e42e8fb84145a30395ddcf891692";
pub const SOLANA_V1_0_8_REVISION: &str = "2a617f2d07f714918891f2b479d1cb1c324f0365";
pub const SOLANA_V1_0_13_REVISION: &str = "fdeda769d05fea4a3f861e787d47d995feee15d7";
pub const SOLANA_V1_0_14_REVISION: &str = "8631be42ac29a062b5e26a85fc2f4c94af042afd";
pub const SOLANA_V1_0_17_REVISION: &str = "cfc7b22c4c9094d09fc969247bfe60a154027d84";
pub const SOLANA_V1_0_18_REVISION: &str = "f26f18d29d650d06f5c5b7a4eb625622a999ea66";
pub const SOLANA_V1_0_23_REVISION: &str = "825c0e2b6e39ae67431ed0a8282260ad3914c87a";
pub const SOLANA_V1_0_24_REVISION: &str = "a93915f1bddb73480f86fc09f487315ae191897d";
pub const SOLANA_V1_1_23_REVISION: &str = "263fc25992ebae85e7ba2f176e9a066449489c3e";
pub const SOLANA_V1_2_32_REVISION: &str = "8c989da68342918f1717c60aa60fdfab7d1e676e";
pub const SOLANA_V1_3_19_REVISION: &str = "15a49d75086f95573ad319b22e4843639bdf2169";

/// Latest mainnet slot currently known to exercise the pre-v1.0.8
/// three-account vote initialization successfully. This is behavioral
/// evidence for v1.0.7, not a guessed release-transition boundary.
pub const SOLANA_V1_0_7_OLD_VOTE_SEMANTICS_OBSERVED_THROUGH_SLOT: Slot = 618_196;

/// Canonical snapshot chosen for the first cross-runtime handoff. It follows
/// the last observed transaction requiring v1.0.7 and precedes the first
/// observed transaction requiring v1.0.8.
pub const SOLANA_V1_0_8_HANDOFF_SNAPSHOT_SLOT: Slot = 619_848;

/// Accounts hash committed by the canonical snapshot at the first historical
/// runtime handoff. Callers must compare against this registry value rather
/// than trusting an arbitrary archive filename.
pub const SOLANA_V1_0_8_HANDOFF_SNAPSHOT_ACCOUNTS_HASH: &str =
    "Fc4tnsTue85dYTMHfTUm3RJjv5nr8wB1HP9hPcY93G7p";

/// First slot routed to v1.0.8. This is a behaviorally safe replay boundary,
/// not a claim that validator deployment activated at this exact slot. The
/// first observed transaction that distinguishes v1.0.8 is slot 630,648.
pub const SOLANA_V1_0_8_ROUTING_START_SLOT: Slot = SOLANA_V1_0_8_HANDOFF_SNAPSHOT_SLOT + 1;
pub const SOLANA_V1_0_8_REQUIRED_SEMANTICS_OBSERVED_AT_SLOT: Slot = 630_648;

/// Exact source revision of the enclosing build. Unlike an independent
/// historical worker, the Agave runtime includes this workspace's patches.
pub const AGAVE_V3_REVISION: &str = env!("JETSTREAMER_BUILD_REVISION");

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RuntimeBackend {
    /// Pinned upstream Solana runtime, isolated in its own process and built
    /// with its historical compiler because old AppendVec layout was not yet
    /// stable across Rust versions.
    SolanaV1_0_7,
    /// First pinned Solana 1.0 runtime known to enforce node-signature checks
    /// for vote-account initialization. Its first routed slot follows a
    /// canonical snapshot checkpoint rather than a guessed deployment date.
    SolanaV1_0_8,
    /// Exact v1.0.13 runtime and 1.1 snapshot loader, initially bounded to
    /// epoch 8 while canonical checkpoint replay is qualified.
    SolanaV1_0_13,
    /// Exact v1.0.14 runtime bounded to epochs 9 through 11.
    SolanaV1_0_14,
    /// Exact v1.0.17 runtime retained as an unassigned comparison candidate.
    SolanaV1_0_17,
    /// Exact v1.0.18 candidate retained without a routed slot era until
    /// differential replay identifies a justified whole-epoch envelope.
    SolanaV1_0_18,
    /// Exact terminal v1.0 patch used only in the checkpoint-gated diagnostic
    /// envelope for epochs 12 through 29.
    SolanaV1_0_23,
    /// Later Solana 1.0 candidate retained separately until differential
    /// replay proves the exact slot at which its consensus behavior applies.
    SolanaV1_0_24,
    /// Exact terminal v1.1 patch used only in the checkpoint-gated diagnostic
    /// envelope for epochs 30 through 60.
    SolanaV1_1_23,
    /// Exact terminal v1.2 patch used only in the checkpoint-gated diagnostic
    /// envelope for epochs 61 through 91.
    SolanaV1_2_32,
    /// Exact terminal v1.3 patch used only in the checkpoint-gated diagnostic
    /// envelope for epochs 92 through 100.
    SolanaV1_3_19,
    /// In-process Agave 3 runtime used by the existing replay path.
    AgaveV3,
}

/// Stable build identity recorded for output provenance.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RuntimeIdentity {
    pub name: &'static str,
    pub revision: &'static str,
    pub rust_toolchain: &'static str,
    /// Exact compilation target of the runtime artifact or enclosing binary.
    pub target: Option<&'static str>,
    pub genesis_hash: &'static str,
}

/// Code path capable of restoring the runtime's bootstrap state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BootstrapStateLoader {
    /// Restore into the current process with the Agave snapshot loader.
    AgaveSnapshotArchive,
    /// Delegate restoration to the isolated historical runtime worker.
    HistoricalWorkerSnapshotArchive,
}

/// Meaning of the hash embedded in a snapshot archive filename.
///
/// This is an execution-runtime property, not a compression-format property:
/// historical runtimes can store their legacy accounts hash in either a bzip2
/// or zstd archive name.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SnapshotHashKind {
    LegacyAccountsHash,
    AccountsLtHash,
}

/// Persisted state formats accepted by one execution runtime.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BootstrapState {
    pub loader: BootstrapStateLoader,
    /// Complete archive suffixes, including the leading dot.
    pub archive_extensions: &'static [&'static str],
    /// Hash scheme committed by the archive filename.
    pub snapshot_hash_kind: SnapshotHashKind,
    /// Whether a bank produced by this same runtime may be carried directly
    /// across an epoch boundary without reloading an archive.
    pub permits_in_memory_handoff: bool,
}

impl BootstrapState {
    /// Tests only the archive container selected by the runtime descriptor;
    /// filename parsing and snapshot-hash validation remain separate checks.
    pub fn accepts_archive_name(self, name: &str) -> bool {
        self.archive_extensions
            .iter()
            .any(|extension| name.ends_with(extension))
    }
}

/// How to locate an out-of-process execution worker.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WorkerExecutable {
    /// Stable executable identity (normally its file name).
    pub identity: &'static str,
    /// Existing operator override honored before the default path.
    pub environment_override: &'static str,
    /// Path resolved relative to `jetstreamer-node`'s manifest directory.
    pub default_manifest_relative_path: &'static str,
}

/// All immutable properties of one execution backend. Slot eras reference
/// these registry-owned values rather than repeating format or build details.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RuntimeDescriptor {
    pub backend: RuntimeBackend,
    pub identity: RuntimeIdentity,
    pub bootstrap: BootstrapState,
    pub worker: Option<WorkerExecutable>,
}

impl RuntimeDescriptor {
    /// Whether this exact runtime can preserve live state across an epoch
    /// boundary. Agave carries its in-process bank. Historical runtimes keep
    /// the same isolated worker and protocol session alive.
    pub fn permits_live_epoch_handoff(&self) -> bool {
        self.bootstrap.permits_in_memory_handoff
            || (self.bootstrap.loader == BootstrapStateLoader::HistoricalWorkerSnapshotArchive
                && self.worker.is_some())
    }

    pub fn accepts_bootstrap_archive_name(self, name: &str) -> bool {
        self.bootstrap.accepts_archive_name(name)
    }

    fn validate(self) -> Result<(), String> {
        if self.identity.name.is_empty()
            || self.identity.revision.is_empty()
            || self.identity.rust_toolchain.is_empty()
            || self.identity.genesis_hash.is_empty()
        {
            return Err(format!(
                "runtime descriptor {:?} has incomplete build identity",
                self.backend
            ));
        }
        if self.bootstrap.archive_extensions.is_empty() {
            return Err(format!(
                "runtime descriptor {} accepts no bootstrap archive format",
                self.identity.name
            ));
        }
        for (index, extension) in self.bootstrap.archive_extensions.iter().enumerate() {
            if !extension.starts_with('.') || extension.len() == 1 {
                return Err(format!(
                    "runtime descriptor {} has invalid bootstrap archive extension {extension:?}",
                    self.identity.name
                ));
            }
            if self.bootstrap.archive_extensions[..index].contains(extension) {
                return Err(format!(
                    "runtime descriptor {} repeats bootstrap archive extension {extension}",
                    self.identity.name
                ));
            }
        }

        let expected_loader = match self.backend {
            RuntimeBackend::SolanaV1_0_7
            | RuntimeBackend::SolanaV1_0_8
            | RuntimeBackend::SolanaV1_0_13
            | RuntimeBackend::SolanaV1_0_14
            | RuntimeBackend::SolanaV1_0_17
            | RuntimeBackend::SolanaV1_0_18
            | RuntimeBackend::SolanaV1_0_23
            | RuntimeBackend::SolanaV1_0_24
            | RuntimeBackend::SolanaV1_1_23
            | RuntimeBackend::SolanaV1_2_32
            | RuntimeBackend::SolanaV1_3_19 => {
                BootstrapStateLoader::HistoricalWorkerSnapshotArchive
            }
            RuntimeBackend::AgaveV3 => BootstrapStateLoader::AgaveSnapshotArchive,
        };
        if self.bootstrap.loader != expected_loader {
            return Err(format!(
                "runtime descriptor {} selects loader {:?}, expected {:?} for backend {:?}",
                self.identity.name, self.bootstrap.loader, expected_loader, self.backend
            ));
        }

        let expected_snapshot_hash_kind = match self.bootstrap.loader {
            BootstrapStateLoader::HistoricalWorkerSnapshotArchive => {
                SnapshotHashKind::LegacyAccountsHash
            }
            BootstrapStateLoader::AgaveSnapshotArchive => SnapshotHashKind::AccountsLtHash,
        };
        if self.bootstrap.snapshot_hash_kind != expected_snapshot_hash_kind {
            return Err(format!(
                "runtime descriptor {} uses snapshot hash kind {:?}, expected {:?} for loader {:?}",
                self.identity.name,
                self.bootstrap.snapshot_hash_kind,
                expected_snapshot_hash_kind,
                self.bootstrap.loader,
            ));
        }

        match (self.bootstrap.loader, self.worker) {
            (BootstrapStateLoader::AgaveSnapshotArchive, None) => {}
            (BootstrapStateLoader::HistoricalWorkerSnapshotArchive, Some(worker)) => {
                if self.bootstrap.permits_in_memory_handoff {
                    return Err(format!(
                        "historical worker runtime {} cannot accept an in-process bank handoff",
                        self.identity.name
                    ));
                }
                if self.identity.target.is_none()
                    || worker.identity.is_empty()
                    || worker.environment_override.is_empty()
                    || worker.default_manifest_relative_path.is_empty()
                {
                    return Err(format!(
                        "historical worker runtime {} has incomplete executable identity",
                        self.identity.name
                    ));
                }
            }
            (BootstrapStateLoader::AgaveSnapshotArchive, Some(_)) => {
                return Err(format!(
                    "in-process runtime {} unexpectedly declares a worker",
                    self.identity.name
                ));
            }
            (BootstrapStateLoader::HistoricalWorkerSnapshotArchive, None) => {
                return Err(format!(
                    "historical runtime {} has no worker executable",
                    self.identity.name
                ));
            }
        }
        Ok(())
    }
}

/// Registry-owned identity of a canonical snapshot used to cross an
/// execution-runtime boundary.
///
/// The slot and accounts hash are independent commitments. `archive_name`
/// constructs the only filename accepted for this identity; callers must not
/// derive the expected hash by parsing a user-supplied path.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CanonicalSnapshotIdentity {
    pub slot: Slot,
    pub accounts_hash_base58: &'static str,
    pub archive_extension: &'static str,
}

impl CanonicalSnapshotIdentity {
    /// Parses the committed accounts hash after registry validation.
    pub fn accounts_hash(self) -> Result<Hash, String> {
        Hash::from_str(self.accounts_hash_base58).map_err(|error| {
            format!(
                "canonical snapshot at slot {} has invalid accounts hash {:?}: {error}",
                self.slot, self.accounts_hash_base58
            )
        })
    }

    /// Canonical archive name derived from registry commitments, never from
    /// an untrusted input filename.
    pub fn archive_name(self) -> String {
        format!(
            "snapshot-{}-{}{}",
            self.slot, self.accounts_hash_base58, self.archive_extension
        )
    }

    fn validate(self) -> Result<(), String> {
        let accounts_hash = self.accounts_hash()?;
        if accounts_hash.to_string() != self.accounts_hash_base58 {
            return Err(format!(
                "canonical snapshot at slot {} has non-canonical accounts hash {:?}",
                self.slot, self.accounts_hash_base58
            ));
        }
        if !self.archive_extension.starts_with('.') || self.archive_extension.len() == 1 {
            return Err(format!(
                "canonical snapshot at slot {} has invalid archive extension {:?}",
                self.slot, self.archive_extension
            ));
        }
        Ok(())
    }
}

/// A canonical-snapshot transition between two out-of-process execution
/// runtimes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RuntimeHandoff {
    /// First slot executed by `destination`.
    pub boundary_slot: Slot,
    pub source: &'static RuntimeDescriptor,
    pub destination: &'static RuntimeDescriptor,
    /// Complete state at `boundary_slot - 1` used to bootstrap `destination`.
    pub snapshot: CanonicalSnapshotIdentity,
}

pub static SOLANA_V1_0_7_RUNTIME: RuntimeDescriptor = RuntimeDescriptor {
    backend: RuntimeBackend::SolanaV1_0_7,
    identity: RuntimeIdentity {
        name: "solana-v1.0.7",
        revision: SOLANA_V1_0_7_REVISION,
        rust_toolchain: "rustc 1.42.0 (b8cedc004 2020-03-09)",
        target: Some("x86_64-unknown-linux-gnu"),
        genesis_hash: MAINNET_GENESIS_HASH,
    },
    bootstrap: BootstrapState {
        loader: BootstrapStateLoader::HistoricalWorkerSnapshotArchive,
        archive_extensions: &[".tar.bz2"],
        snapshot_hash_kind: SnapshotHashKind::LegacyAccountsHash,
        permits_in_memory_handoff: false,
    },
    worker: Some(WorkerExecutable {
        identity: "jetstreamer-historical-worker-v1-0-7",
        environment_override: "JETSTREAMER_HISTORICAL_WORKER_V1_0_7",
        default_manifest_relative_path: "../historical-runtime/v1_0_7/target/release/jetstreamer-historical-worker-v1-0-7",
    }),
};

pub static SOLANA_V1_0_8_RUNTIME: RuntimeDescriptor = RuntimeDescriptor {
    backend: RuntimeBackend::SolanaV1_0_8,
    identity: RuntimeIdentity {
        name: "solana-v1.0.8",
        revision: SOLANA_V1_0_8_REVISION,
        rust_toolchain: "rustc 1.42.0 (b8cedc004 2020-03-09)",
        target: Some("x86_64-unknown-linux-gnu"),
        genesis_hash: MAINNET_GENESIS_HASH,
    },
    bootstrap: BootstrapState {
        loader: BootstrapStateLoader::HistoricalWorkerSnapshotArchive,
        archive_extensions: &[".tar.bz2"],
        snapshot_hash_kind: SnapshotHashKind::LegacyAccountsHash,
        permits_in_memory_handoff: false,
    },
    worker: Some(WorkerExecutable {
        identity: "jetstreamer-historical-worker-v1-0-8",
        environment_override: "JETSTREAMER_HISTORICAL_WORKER_V1_0_8",
        default_manifest_relative_path: "../historical-runtime/v1_0_8/target/release/jetstreamer-historical-worker-v1-0-8",
    }),
};

pub static SOLANA_V1_0_13_RUNTIME: RuntimeDescriptor = RuntimeDescriptor {
    backend: RuntimeBackend::SolanaV1_0_13,
    identity: RuntimeIdentity {
        name: "solana-v1.0.13",
        revision: SOLANA_V1_0_13_REVISION,
        rust_toolchain: "rustc 1.42.0 (b8cedc004 2020-03-09)",
        target: Some("x86_64-unknown-linux-gnu"),
        genesis_hash: MAINNET_GENESIS_HASH,
    },
    bootstrap: BootstrapState {
        loader: BootstrapStateLoader::HistoricalWorkerSnapshotArchive,
        archive_extensions: &[".tar.bz2"],
        snapshot_hash_kind: SnapshotHashKind::LegacyAccountsHash,
        permits_in_memory_handoff: false,
    },
    worker: Some(WorkerExecutable {
        identity: "jetstreamer-historical-worker-v1-0-13",
        environment_override: "JETSTREAMER_HISTORICAL_WORKER_V1_0_13",
        default_manifest_relative_path: "../historical-runtime/v1_0_13/target/release/jetstreamer-historical-worker-v1-0-13",
    }),
};

pub static SOLANA_V1_0_14_RUNTIME: RuntimeDescriptor = RuntimeDescriptor {
    backend: RuntimeBackend::SolanaV1_0_14,
    identity: RuntimeIdentity {
        name: "solana-v1.0.14",
        revision: SOLANA_V1_0_14_REVISION,
        rust_toolchain: "rustc 1.42.0 (b8cedc004 2020-03-09)",
        target: Some("x86_64-unknown-linux-gnu"),
        genesis_hash: MAINNET_GENESIS_HASH,
    },
    bootstrap: BootstrapState {
        loader: BootstrapStateLoader::HistoricalWorkerSnapshotArchive,
        archive_extensions: &[".tar.bz2"],
        snapshot_hash_kind: SnapshotHashKind::LegacyAccountsHash,
        permits_in_memory_handoff: false,
    },
    worker: Some(WorkerExecutable {
        identity: "jetstreamer-historical-worker-v1-0-14",
        environment_override: "JETSTREAMER_HISTORICAL_WORKER_V1_0_14",
        default_manifest_relative_path: "../historical-runtime/v1_0_14/target/release/jetstreamer-historical-worker-v1-0-14",
    }),
};

pub static SOLANA_V1_0_17_RUNTIME: RuntimeDescriptor = RuntimeDescriptor {
    backend: RuntimeBackend::SolanaV1_0_17,
    identity: RuntimeIdentity {
        name: "solana-v1.0.17",
        revision: SOLANA_V1_0_17_REVISION,
        rust_toolchain: "rustc 1.42.0 (b8cedc004 2020-03-09)",
        target: Some("x86_64-unknown-linux-gnu"),
        genesis_hash: MAINNET_GENESIS_HASH,
    },
    bootstrap: BootstrapState {
        loader: BootstrapStateLoader::HistoricalWorkerSnapshotArchive,
        archive_extensions: &[".tar.bz2"],
        snapshot_hash_kind: SnapshotHashKind::LegacyAccountsHash,
        permits_in_memory_handoff: false,
    },
    worker: Some(WorkerExecutable {
        identity: "jetstreamer-historical-worker-v1-0-17",
        environment_override: "JETSTREAMER_HISTORICAL_WORKER_V1_0_17",
        default_manifest_relative_path: "../historical-runtime/v1_0_17/target/release/jetstreamer-historical-worker-v1-0-17",
    }),
};

pub static SOLANA_V1_0_18_RUNTIME: RuntimeDescriptor = RuntimeDescriptor {
    backend: RuntimeBackend::SolanaV1_0_18,
    identity: RuntimeIdentity {
        name: "solana-v1.0.18",
        revision: SOLANA_V1_0_18_REVISION,
        rust_toolchain: "rustc 1.42.0 (b8cedc004 2020-03-09)",
        target: Some("x86_64-unknown-linux-gnu"),
        genesis_hash: MAINNET_GENESIS_HASH,
    },
    bootstrap: BootstrapState {
        loader: BootstrapStateLoader::HistoricalWorkerSnapshotArchive,
        archive_extensions: &[".tar.bz2"],
        snapshot_hash_kind: SnapshotHashKind::LegacyAccountsHash,
        permits_in_memory_handoff: false,
    },
    worker: Some(WorkerExecutable {
        identity: "jetstreamer-historical-worker-v1-0-18",
        environment_override: "JETSTREAMER_HISTORICAL_WORKER_V1_0_18",
        default_manifest_relative_path: "../historical-runtime/v1_0_18/target/release/jetstreamer-historical-worker-v1-0-18",
    }),
};

pub static SOLANA_V1_0_23_RUNTIME: RuntimeDescriptor = RuntimeDescriptor {
    backend: RuntimeBackend::SolanaV1_0_23,
    identity: RuntimeIdentity {
        name: "solana-v1.0.23",
        revision: SOLANA_V1_0_23_REVISION,
        rust_toolchain: "rustc 1.43.0 (4fb7144ed 2020-04-20)",
        target: Some("x86_64-unknown-linux-gnu"),
        genesis_hash: MAINNET_GENESIS_HASH,
    },
    bootstrap: BootstrapState {
        loader: BootstrapStateLoader::HistoricalWorkerSnapshotArchive,
        archive_extensions: &[".tar.bz2"],
        snapshot_hash_kind: SnapshotHashKind::LegacyAccountsHash,
        permits_in_memory_handoff: false,
    },
    worker: Some(WorkerExecutable {
        identity: "jetstreamer-historical-worker-v1-0-23",
        environment_override: "JETSTREAMER_HISTORICAL_WORKER_V1_0_23",
        default_manifest_relative_path: "../historical-runtime/v1_0_23/target/release/jetstreamer-historical-worker-v1-0-23",
    }),
};

pub static SOLANA_V1_0_24_RUNTIME: RuntimeDescriptor = RuntimeDescriptor {
    backend: RuntimeBackend::SolanaV1_0_24,
    identity: RuntimeIdentity {
        name: "solana-v1.0.24",
        revision: SOLANA_V1_0_24_REVISION,
        rust_toolchain: "rustc 1.43.0 (4fb7144ed 2020-04-20)",
        target: Some("x86_64-unknown-linux-gnu"),
        genesis_hash: MAINNET_GENESIS_HASH,
    },
    bootstrap: BootstrapState {
        loader: BootstrapStateLoader::HistoricalWorkerSnapshotArchive,
        archive_extensions: &[".tar.bz2"],
        snapshot_hash_kind: SnapshotHashKind::LegacyAccountsHash,
        permits_in_memory_handoff: false,
    },
    worker: Some(WorkerExecutable {
        identity: "jetstreamer-historical-worker-v1-0-24",
        environment_override: "JETSTREAMER_HISTORICAL_WORKER",
        default_manifest_relative_path: "../historical-runtime/v1_0_24/target/release/jetstreamer-historical-worker-v1-0-24",
    }),
};

pub static SOLANA_V1_1_23_RUNTIME: RuntimeDescriptor = RuntimeDescriptor {
    backend: RuntimeBackend::SolanaV1_1_23,
    identity: RuntimeIdentity {
        name: "solana-v1.1.23",
        revision: SOLANA_V1_1_23_REVISION,
        rust_toolchain: "rustc 1.43.0 (4fb7144ed 2020-04-20)",
        target: Some("x86_64-unknown-linux-gnu"),
        genesis_hash: MAINNET_GENESIS_HASH,
    },
    bootstrap: BootstrapState {
        loader: BootstrapStateLoader::HistoricalWorkerSnapshotArchive,
        archive_extensions: &[".tar.bz2"],
        snapshot_hash_kind: SnapshotHashKind::LegacyAccountsHash,
        permits_in_memory_handoff: false,
    },
    worker: Some(WorkerExecutable {
        identity: "jetstreamer-historical-worker-v1-1-23",
        environment_override: "JETSTREAMER_HISTORICAL_WORKER_V1_1_23",
        default_manifest_relative_path: "../historical-runtime/v1_1_23/target/release/jetstreamer-historical-worker-v1-1-23",
    }),
};

pub static SOLANA_V1_2_32_RUNTIME: RuntimeDescriptor = RuntimeDescriptor {
    backend: RuntimeBackend::SolanaV1_2_32,
    identity: RuntimeIdentity {
        name: "solana-v1.2.32",
        revision: SOLANA_V1_2_32_REVISION,
        rust_toolchain: "rustc 1.43.0 (4fb7144ed 2020-04-20)",
        target: Some("x86_64-unknown-linux-gnu"),
        genesis_hash: MAINNET_GENESIS_HASH,
    },
    bootstrap: BootstrapState {
        loader: BootstrapStateLoader::HistoricalWorkerSnapshotArchive,
        archive_extensions: &[".tar.bz2", ".tar.zst"],
        snapshot_hash_kind: SnapshotHashKind::LegacyAccountsHash,
        permits_in_memory_handoff: false,
    },
    worker: Some(WorkerExecutable {
        identity: "jetstreamer-historical-worker-v1-2-32",
        environment_override: "JETSTREAMER_HISTORICAL_WORKER_V1_2_32",
        default_manifest_relative_path: "../historical-runtime/v1_2_32/target/release/jetstreamer-historical-worker-v1-2-32",
    }),
};

pub static SOLANA_V1_3_19_RUNTIME: RuntimeDescriptor = RuntimeDescriptor {
    backend: RuntimeBackend::SolanaV1_3_19,
    identity: RuntimeIdentity {
        name: "solana-v1.3.19",
        revision: SOLANA_V1_3_19_REVISION,
        rust_toolchain: "rustc 1.45.1 (c367798cf 2020-07-26)",
        target: Some("x86_64-unknown-linux-gnu"),
        genesis_hash: MAINNET_GENESIS_HASH,
    },
    bootstrap: BootstrapState {
        loader: BootstrapStateLoader::HistoricalWorkerSnapshotArchive,
        archive_extensions: &[".tar.bz2", ".tar.zst"],
        snapshot_hash_kind: SnapshotHashKind::LegacyAccountsHash,
        permits_in_memory_handoff: false,
    },
    worker: Some(WorkerExecutable {
        identity: "jetstreamer-historical-worker-v1-3-19",
        environment_override: "JETSTREAMER_HISTORICAL_WORKER_V1_3_19",
        default_manifest_relative_path: "../historical-runtime/v1_3_19/target/release/jetstreamer-historical-worker-v1-3-19",
    }),
};

pub static AGAVE_V3_RUNTIME: RuntimeDescriptor = RuntimeDescriptor {
    backend: RuntimeBackend::AgaveV3,
    identity: RuntimeIdentity {
        name: "agave-v3",
        revision: AGAVE_V3_REVISION,
        rust_toolchain: env!("JETSTREAMER_BUILD_RUSTC"),
        target: Some(env!("JETSTREAMER_BUILD_TARGET")),
        genesis_hash: MAINNET_GENESIS_HASH,
    },
    bootstrap: BootstrapState {
        loader: BootstrapStateLoader::AgaveSnapshotArchive,
        archive_extensions: &[".tar.zst", ".tar.lz4"],
        snapshot_hash_kind: SnapshotHashKind::AccountsLtHash,
        permits_in_memory_handoff: true,
    },
    worker: None,
};

/// First registered state-transfer point in the historical runtime registry.
pub static SOLANA_V1_0_7_TO_V1_0_8_HANDOFF: RuntimeHandoff = RuntimeHandoff {
    boundary_slot: SOLANA_V1_0_8_ROUTING_START_SLOT,
    source: &SOLANA_V1_0_7_RUNTIME,
    destination: &SOLANA_V1_0_8_RUNTIME,
    snapshot: CanonicalSnapshotIdentity {
        slot: SOLANA_V1_0_8_HANDOFF_SNAPSHOT_SLOT,
        accounts_hash_base58: SOLANA_V1_0_8_HANDOFF_SNAPSHOT_ACCOUNTS_HASH,
        archive_extension: ".tar.bz2",
    },
};

/// Canonical snapshot transitions keyed by their destination slot.
pub static RUNTIME_HANDOFFS: &[&RuntimeHandoff] = &[&SOLANA_V1_0_7_TO_V1_0_8_HANDOFF];

/// Runtime changes at these epoch boundaries are deliberately serviced by a
/// fresh, independently verified predecessor-epoch snapshot in each isolated
/// child. They are not canonical state handoffs and therefore cannot be used
/// to assemble a replay that crosses the boundary inside one output archive.
pub static SNAPSHOT_ISOLATED_RUNTIME_BOUNDARIES: &[Slot] = &[
    SOLANA_V1_0_13_CANDIDATE_START_SLOT,
    SOLANA_V1_0_14_CANDIDATE_START_SLOT,
    SOLANA_V1_0_23_CANDIDATE_START_SLOT,
    SOLANA_V1_1_23_CANDIDATE_START_SLOT,
    SOLANA_V1_2_32_CANDIDATE_START_SLOT,
    SOLANA_V1_3_19_CANDIDATE_START_SLOT,
];

/// Single source of truth for executable and bootstrap-state identities.
/// A candidate may be registered before it claims a slot era; this is how a
/// differential worker remains available without inventing a release-date
/// boundary. Every selectable era must still reference a registered profile.
pub static RUNTIME_DESCRIPTORS: &[&RuntimeDescriptor] = &[
    &SOLANA_V1_0_7_RUNTIME,
    &SOLANA_V1_0_8_RUNTIME,
    &SOLANA_V1_0_13_RUNTIME,
    &SOLANA_V1_0_14_RUNTIME,
    &SOLANA_V1_0_17_RUNTIME,
    &SOLANA_V1_0_18_RUNTIME,
    &SOLANA_V1_0_23_RUNTIME,
    &SOLANA_V1_0_24_RUNTIME,
    &SOLANA_V1_1_23_RUNTIME,
    &SOLANA_V1_2_32_RUNTIME,
    &SOLANA_V1_3_19_RUNTIME,
    &AGAVE_V3_RUNTIME,
];

/// One executable/runtime profile selected for a complete replay span.
///
/// Evidence can divide one profile into multiple registry eras without
/// requiring a state handoff. A handoff is required only when `backend`
/// changes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RuntimeSelection {
    /// Stable executable and consensus-behavior identity.
    pub backend: RuntimeBackend,
    /// Registry-owned runtime, build, bootstrap, and worker identity.
    pub descriptor: &'static RuntimeDescriptor,
    /// Weakest evidence level covering the selected slot span.
    pub admission: AdmissionLevel,
}

/// Immutable build identity for an out-of-process historical executor.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HistoricalArtifact {
    pub upstream_revision: &'static str,
    pub rust_toolchain: &'static str,
    pub target: &'static str,
    pub genesis_hash: &'static str,
}

impl RuntimeBackend {
    /// Resolves through the descriptor registry instead of duplicating build
    /// properties in a match statement.
    pub fn descriptor(self) -> Result<&'static RuntimeDescriptor, String> {
        validate_runtime_registry()?;
        let mut matches = RUNTIME_DESCRIPTORS
            .iter()
            .copied()
            .filter(|descriptor| descriptor.backend == self);
        let descriptor = matches
            .next()
            .ok_or_else(|| format!("runtime backend {self:?} has no registered descriptor"))?;
        if matches.next().is_some() {
            return Err(format!(
                "runtime backend {self:?} has multiple registered descriptors"
            ));
        }
        descriptor.validate()?;
        Ok(descriptor)
    }

    pub fn name(self) -> Result<&'static str, String> {
        Ok(self.descriptor()?.identity.name)
    }

    /// Compatibility projection for existing callers. New code should use the
    /// complete descriptor exposed by `RuntimeSelection`.
    pub fn historical_artifact(self) -> Option<HistoricalArtifact> {
        let descriptor = self.descriptor().ok()?;
        descriptor.worker?;
        Some(HistoricalArtifact {
            upstream_revision: descriptor.identity.revision,
            rust_toolchain: descriptor.identity.rust_toolchain,
            target: descriptor.identity.target?,
            genesis_hash: descriptor.identity.genesis_hash,
        })
    }
}

impl fmt::Display for RuntimeBackend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name().map_err(|_| fmt::Error)?)
    }
}

/// Evidence level attached to an execution-range claim.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AdmissionLevel {
    /// Passed the configured canonical transaction and state checkpoints.
    Verified,
    /// Useful for differential replay, but not yet safe to publish as
    /// canonical output without an explicit operator opt-in.
    Candidate,
}

/// How replay handles a transaction whose archive record has no status frame.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MissingTransactionStatus {
    /// Absence is corruption for this range and aborts replay.
    Reject,
    /// The selected exact runtime supplies the status; other metadata fields
    /// remain unavailable and must not be treated as source observations.
    Reconstruct,
}

/// How replay validates and associates source transaction statuses.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransactionStatusValidation {
    /// No complete source status set exists, so the selected runtime supplies
    /// each transaction's status.
    RuntimeOnly,
    /// The source status writer permuted execution results within each entry.
    /// Replay checks the entry-wide multiset, then uses runtime results for the
    /// per-transaction association.
    RuntimeWithSourceEntryMultiset,
    /// Source statuses are associated with their transactions and must match
    /// replay individually.
    SourceExact,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EraBackend {
    Available(&'static RuntimeDescriptor),
    /// An explicit hole is safer than guessing consensus semantics.
    Unsupported,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RuntimeEra {
    pub name: &'static str,
    pub start_slot: Slot,
    pub end_slot_exclusive: Option<Slot>,
    pub backend: EraBackend,
    pub admission: AdmissionLevel,
}

impl RuntimeEra {
    const fn contains(&self, slot: Slot) -> bool {
        slot >= self.start_slot
            && match self.end_slot_exclusive {
                Some(end) => slot < end,
                None => true,
            }
    }

    fn display_range(&self) -> String {
        match self.end_slot_exclusive {
            Some(end) => format!("{}..{end}", self.start_slot),
            None => format!("{}..", self.start_slot),
        }
    }
}

/// Ordered, contiguous execution registry. Release dates are intentionally not
/// used as boundaries: historical intervals graduate from the unsupported gap
/// only after differential replay locates their actual switch slot and golden
/// checkpoints prove the pinned worker on both sides.
pub static RUNTIME_ERAS: &[RuntimeEra] = &[
    RuntimeEra {
        name: "solana-v1.0.7-through-first-handoff-candidate",
        start_slot: 0,
        end_slot_exclusive: Some(SOLANA_V1_0_8_ROUTING_START_SLOT),
        backend: EraBackend::Available(&SOLANA_V1_0_7_RUNTIME),
        admission: AdmissionLevel::Candidate,
    },
    RuntimeEra {
        name: "solana-v1.0.8-epochs-1-7-candidate-search-envelope",
        start_slot: SOLANA_V1_0_8_ROUTING_START_SLOT,
        end_slot_exclusive: Some(SOLANA_V1_0_8_CANDIDATE_END_SLOT_EXCLUSIVE),
        backend: EraBackend::Available(&SOLANA_V1_0_8_RUNTIME),
        admission: AdmissionLevel::Candidate,
    },
    RuntimeEra {
        name: "solana-v1.0.13-epoch-8-candidate",
        start_slot: SOLANA_V1_0_13_CANDIDATE_START_SLOT,
        end_slot_exclusive: Some(SOLANA_V1_0_13_CANDIDATE_END_SLOT_EXCLUSIVE),
        backend: EraBackend::Available(&SOLANA_V1_0_13_RUNTIME),
        admission: AdmissionLevel::Candidate,
    },
    RuntimeEra {
        name: "solana-v1.0.14-epochs-9-11-candidate",
        start_slot: SOLANA_V1_0_14_CANDIDATE_START_SLOT,
        end_slot_exclusive: Some(SOLANA_V1_0_14_CANDIDATE_END_SLOT_EXCLUSIVE),
        backend: EraBackend::Available(&SOLANA_V1_0_14_RUNTIME),
        admission: AdmissionLevel::Candidate,
    },
    RuntimeEra {
        name: "solana-v1.0.23-epochs-12-29-differential-candidate",
        start_slot: SOLANA_V1_0_23_CANDIDATE_START_SLOT,
        end_slot_exclusive: Some(SOLANA_V1_0_23_CANDIDATE_END_SLOT_EXCLUSIVE),
        backend: EraBackend::Available(&SOLANA_V1_0_23_RUNTIME),
        admission: AdmissionLevel::Candidate,
    },
    RuntimeEra {
        name: "solana-v1.1.23-epochs-30-60-differential-candidate",
        start_slot: SOLANA_V1_1_23_CANDIDATE_START_SLOT,
        end_slot_exclusive: Some(SOLANA_V1_1_23_CANDIDATE_END_SLOT_EXCLUSIVE),
        backend: EraBackend::Available(&SOLANA_V1_1_23_RUNTIME),
        admission: AdmissionLevel::Candidate,
    },
    RuntimeEra {
        name: "solana-v1.2.32-epochs-61-91-differential-candidate",
        start_slot: SOLANA_V1_2_32_CANDIDATE_START_SLOT,
        end_slot_exclusive: Some(SOLANA_V1_2_32_CANDIDATE_END_SLOT_EXCLUSIVE),
        backend: EraBackend::Available(&SOLANA_V1_2_32_RUNTIME),
        admission: AdmissionLevel::Candidate,
    },
    RuntimeEra {
        name: "solana-v1.3.19-epochs-92-100-differential-candidate",
        start_slot: SOLANA_V1_3_19_CANDIDATE_START_SLOT,
        end_slot_exclusive: Some(SOLANA_V1_3_19_CANDIDATE_END_SLOT_EXCLUSIVE),
        backend: EraBackend::Available(&SOLANA_V1_3_19_RUNTIME),
        admission: AdmissionLevel::Candidate,
    },
    RuntimeEra {
        name: "historical-runtime-gap",
        start_slot: SOLANA_V1_3_19_CANDIDATE_END_SLOT_EXCLUSIVE,
        end_slot_exclusive: Some(AGAVE_V3_VERIFIED_START_SLOT),
        backend: EraBackend::Unsupported,
        admission: AdmissionLevel::Candidate,
    },
    RuntimeEra {
        name: "agave-v3",
        start_slot: AGAVE_V3_VERIFIED_START_SLOT,
        end_slot_exclusive: None,
        backend: EraBackend::Available(&AGAVE_V3_RUNTIME),
        admission: AdmissionLevel::Verified,
    },
];

/// Validates the complete registry before a plan can select a runtime. This
/// turns omissions, duplicate identities, malformed archive formats, and
/// accidental holes or overlaps into startup errors.
pub fn validate_runtime_registry() -> Result<(), String> {
    if SOLANA_V1_0_7_OLD_VOTE_SEMANTICS_OBSERVED_THROUGH_SLOT >= SOLANA_V1_0_8_ROUTING_START_SLOT {
        return Err(format!(
            "v1.0.7 behavioral evidence slot {} falls outside its candidate range 0..{}",
            SOLANA_V1_0_7_OLD_VOTE_SEMANTICS_OBSERVED_THROUGH_SLOT,
            SOLANA_V1_0_8_ROUTING_START_SLOT,
        ));
    }
    if SOLANA_V1_0_8_REQUIRED_SEMANTICS_OBSERVED_AT_SLOT < SOLANA_V1_0_8_ROUTING_START_SLOT
        || SOLANA_V1_0_8_REQUIRED_SEMANTICS_OBSERVED_AT_SLOT
            >= SOLANA_V1_0_8_CANDIDATE_END_SLOT_EXCLUSIVE
    {
        return Err(format!(
            "v1.0.8 behavioral evidence slot {} falls outside its candidate range {}..{}",
            SOLANA_V1_0_8_REQUIRED_SEMANTICS_OBSERVED_AT_SLOT,
            SOLANA_V1_0_8_ROUTING_START_SLOT,
            SOLANA_V1_0_8_CANDIDATE_END_SLOT_EXCLUSIVE,
        ));
    }
    if RUNTIME_DESCRIPTORS.is_empty() {
        return Err("runtime descriptor registry is empty".to_string());
    }
    for (index, descriptor) in RUNTIME_DESCRIPTORS.iter().copied().enumerate() {
        descriptor.validate()?;
        for previous in RUNTIME_DESCRIPTORS[..index].iter().copied() {
            if previous.backend == descriptor.backend {
                return Err(format!(
                    "runtime backend {:?} has multiple registered descriptors",
                    descriptor.backend
                ));
            }
            if previous.identity.name == descriptor.identity.name {
                return Err(format!(
                    "runtime identity {} is registered more than once",
                    descriptor.identity.name
                ));
            }
        }
    }

    let Some(first_era) = RUNTIME_ERAS.first() else {
        return Err("runtime era registry is empty".to_string());
    };
    if first_era.start_slot != 0 {
        return Err(format!(
            "runtime era registry starts at slot {} instead of slot 0",
            first_era.start_slot
        ));
    }
    for (index, era) in RUNTIME_ERAS.iter().enumerate() {
        if era.name.is_empty() {
            return Err(format!("runtime era at index {index} has no name"));
        }
        if let Some(end) = era.end_slot_exclusive
            && end <= era.start_slot
        {
            return Err(format!(
                "runtime era {} has invalid range {}..{end}",
                era.name, era.start_slot
            ));
        }
        if let Some(next) = RUNTIME_ERAS.get(index + 1) {
            if era.end_slot_exclusive != Some(next.start_slot) {
                return Err(format!(
                    "runtime eras {} and {} have a gap or overlap",
                    era.name, next.name
                ));
            }
        } else if era.end_slot_exclusive.is_some() {
            return Err(format!(
                "final runtime era {} does not cover the open-ended tail",
                era.name
            ));
        }

        if let EraBackend::Available(descriptor) = era.backend
            && !RUNTIME_DESCRIPTORS
                .iter()
                .any(|registered| std::ptr::eq(*registered, descriptor))
        {
            return Err(format!(
                "runtime era {} references an unregistered descriptor",
                era.name
            ));
        }
    }

    validate_runtime_handoffs(RUNTIME_DESCRIPTORS, RUNTIME_ERAS, RUNTIME_HANDOFFS)?;

    Ok(())
}

fn validate_runtime_handoffs(
    descriptors: &[&RuntimeDescriptor],
    eras: &[RuntimeEra],
    handoffs: &[&RuntimeHandoff],
) -> Result<(), String> {
    for (index, boundary) in SNAPSHOT_ISOLATED_RUNTIME_BOUNDARIES
        .iter()
        .copied()
        .enumerate()
    {
        if SNAPSHOT_ISOLATED_RUNTIME_BOUNDARIES[..index].contains(&boundary) {
            return Err(format!(
                "snapshot-isolated runtime boundary {boundary} is registered more than once"
            ));
        }
        let matches_runtime_change = eras.windows(2).any(|pair| {
            pair[0].end_slot_exclusive == Some(boundary)
                && pair[1].start_slot == boundary
                && matches!(
                    (pair[0].backend, pair[1].backend),
                    (EraBackend::Available(source), EraBackend::Available(destination))
                        if !std::ptr::eq(source, destination)
                )
        });
        if !matches_runtime_change {
            return Err(format!(
                "snapshot-isolated boundary {boundary} does not match an adjacent runtime change"
            ));
        }
        if handoffs
            .iter()
            .any(|handoff| handoff.boundary_slot == boundary)
        {
            return Err(format!(
                "runtime boundary {boundary} cannot be both a canonical handoff and snapshot-isolated restart"
            ));
        }
    }

    for (index, handoff) in handoffs.iter().copied().enumerate() {
        handoff.snapshot.validate()?;
        if handoff.snapshot.slot.checked_add(1) != Some(handoff.boundary_slot) {
            return Err(format!(
                "runtime handoff at slot {} must use a snapshot of completed slot {} (got {})",
                handoff.boundary_slot,
                handoff.boundary_slot.saturating_sub(1),
                handoff.snapshot.slot,
            ));
        }
        if !descriptors
            .iter()
            .any(|descriptor| std::ptr::eq(*descriptor, handoff.source))
            || !descriptors
                .iter()
                .any(|descriptor| std::ptr::eq(*descriptor, handoff.destination))
        {
            return Err(format!(
                "runtime handoff at slot {} references an unregistered descriptor",
                handoff.boundary_slot
            ));
        }
        if std::ptr::eq(handoff.source, handoff.destination) {
            return Err(format!(
                "runtime handoff at slot {} does not change runtime",
                handoff.boundary_slot
            ));
        }
        let archive_name = handoff.snapshot.archive_name();
        if !handoff
            .destination
            .accepts_bootstrap_archive_name(&archive_name)
        {
            return Err(format!(
                "runtime handoff at slot {} uses snapshot {} unsupported by destination {}",
                handoff.boundary_slot, archive_name, handoff.destination.identity.name
            ));
        }
        if handoffs[..index]
            .iter()
            .any(|previous| previous.boundary_slot == handoff.boundary_slot)
        {
            return Err(format!(
                "runtime handoff boundary {} is registered more than once",
                handoff.boundary_slot
            ));
        }

        let matches_adjacent_eras = eras.windows(2).any(|pair| {
            pair[0].end_slot_exclusive == Some(handoff.boundary_slot)
                && pair[1].start_slot == handoff.boundary_slot
                && matches!(
                    (pair[0].backend, pair[1].backend),
                    (EraBackend::Available(source), EraBackend::Available(destination))
                        if std::ptr::eq(source, handoff.source)
                            && std::ptr::eq(destination, handoff.destination)
                )
        });
        if !matches_adjacent_eras {
            return Err(format!(
                "runtime handoff at slot {} does not match adjacent source and destination eras",
                handoff.boundary_slot
            ));
        }
    }

    for pair in eras.windows(2) {
        let (EraBackend::Available(source), EraBackend::Available(destination)) =
            (pair[0].backend, pair[1].backend)
        else {
            continue;
        };
        if source.backend == destination.backend {
            continue;
        }
        let boundary = pair[1].start_slot;
        let Some(handoff) = handoffs
            .iter()
            .copied()
            .find(|handoff| handoff.boundary_slot == boundary)
        else {
            if SNAPSHOT_ISOLATED_RUNTIME_BOUNDARIES.contains(&boundary) {
                continue;
            }
            return Err(format!(
                "runtime boundary at slot {boundary} changes {} to {} without a canonical snapshot handoff",
                source.identity.name, destination.identity.name
            ));
        };
        if !std::ptr::eq(handoff.source, source) || !std::ptr::eq(handoff.destination, destination)
        {
            return Err(format!(
                "runtime handoff at slot {boundary} does not match the registered execution eras"
            ));
        }
    }

    Ok(())
}

/// Output adapter selected independently of the execution runtime.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OutputSchema {
    HorizonV2,
}

/// One maximal interval over which all three compatibility dimensions remain
/// constant.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplaySegment {
    pub slots: Range<Slot>,
    pub input_metadata: OldFaithfulMetaEncoding,
    pub missing_transaction_status: MissingTransactionStatus,
    pub transaction_status_validation: TransactionStatusValidation,
    pub execution: &'static RuntimeEra,
    pub output: OutputSchema,
}

/// One exact execution-runtime span for a requested output range.
///
/// `handoff` identifies the canonical completed state required before entering
/// this runtime era. It is present even when the requested range begins at the
/// era boundary, allowing a caller to bootstrap that single span safely.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RuntimeSpan {
    pub slots: Range<Slot>,
    pub execution: &'static RuntimeEra,
    pub handoff: Option<&'static RuntimeHandoff>,
}

fn runtime_at(slot: Slot) -> Result<&'static RuntimeEra, String> {
    RUNTIME_ERAS
        .iter()
        .find(|era| era.contains(slot))
        .ok_or_else(|| format!("no runtime era contains slot {slot}"))
}

fn input_end_exclusive(slot: Slot) -> Option<Slot> {
    [
        OLD_FAITHFUL_STATUS_REQUIRED_START_SLOT,
        OLD_FAITHFUL_UNTRUSTED_STATUS_ASSOCIATION_END_SLOT_EXCLUSIVE,
        OLD_FAITHFUL_PROTOBUF_META_START_SLOT,
    ]
    .into_iter()
    .filter(|boundary| slot < *boundary)
    .min()
}

/// Selects the missing-status policy independently from execution semantics.
#[inline]
pub const fn missing_transaction_status_at(slot: Slot) -> MissingTransactionStatus {
    if slot < OLD_FAITHFUL_STATUS_REQUIRED_START_SLOT {
        MissingTransactionStatus::Reconstruct
    } else {
        MissingTransactionStatus::Reject
    }
}

/// Selects transaction-status validation independently from status presence
/// and execution-runtime routing.
#[inline]
pub const fn transaction_status_validation_at(slot: Slot) -> TransactionStatusValidation {
    if slot < OLD_FAITHFUL_STATUS_REQUIRED_START_SLOT {
        TransactionStatusValidation::RuntimeOnly
    } else if slot < OLD_FAITHFUL_UNTRUSTED_STATUS_ASSOCIATION_END_SLOT_EXCLUSIVE {
        TransactionStatusValidation::RuntimeWithSourceEntryMultiset
    } else {
        TransactionStatusValidation::SourceExact
    }
}

/// Builds exact ordered execution spans, split only at registered runtime-era
/// boundaries. The returned spans cover the requested half-open range once,
/// without gaps or overlaps.
pub fn plan_runtime_spans(
    range: Range<Slot>,
    allow_candidate_runtime: bool,
) -> Result<Vec<RuntimeSpan>, String> {
    if range.start >= range.end {
        return Err(format!(
            "runtime planning requires a non-empty half-open slot range (got {}..{})",
            range.start, range.end
        ));
    }
    validate_runtime_registry()?;

    let mut cursor = range.start;
    let mut spans = Vec::new();
    while cursor < range.end {
        let execution = runtime_at(cursor)?;
        if execution.backend == EraBackend::Unsupported {
            return Err(format!(
                "slot {} falls in unsupported runtime era {} ({}); refusing to guess consensus semantics",
                cursor,
                execution.name,
                execution.display_range(),
            ));
        }
        if execution.admission == AdmissionLevel::Candidate && !allow_candidate_runtime {
            return Err(format!(
                "runtime era {} ({}) is candidate-only; set JETSTREAMER_ALLOW_CANDIDATE_RUNTIME=1 for differential replay",
                execution.name,
                execution.display_range(),
            ));
        }

        let span_end = range
            .end
            .min(execution.end_slot_exclusive.unwrap_or(range.end));
        if span_end <= cursor {
            return Err(format!(
                "runtime planner made no progress at slot {cursor} in era {}",
                execution.name
            ));
        }
        let handoff = RUNTIME_HANDOFFS.iter().copied().find(|handoff| {
            handoff.boundary_slot == execution.start_slot
                && matches!(
                    execution.backend,
                    EraBackend::Available(descriptor)
                        if std::ptr::eq(descriptor, handoff.destination)
                )
        });
        spans.push(RuntimeSpan {
            slots: cursor..span_end,
            execution,
            handoff,
        });
        cursor = span_end;
    }

    let mut expected_start = range.start;
    for span in &spans {
        if span.slots.start != expected_start || span.slots.end <= span.slots.start {
            return Err(format!(
                "runtime planner produced a gap, overlap, or empty span at {}..{}",
                span.slots.start, span.slots.end
            ));
        }
        expected_start = span.slots.end;
    }
    if expected_start != range.end {
        return Err(format!(
            "runtime planner stopped at slot {expected_start} before requested end {}",
            range.end
        ));
    }
    Ok(spans)
}

/// Builds the slot-driven replay plan, splitting automatically at every known
/// input or execution boundary. Unsupported execution holes fail closed.
pub fn plan_replay(
    range: Range<Slot>,
    allow_candidate_runtime: bool,
) -> Result<Vec<ReplaySegment>, String> {
    if range.start >= range.end {
        return Err(format!(
            "replay planning requires a non-empty half-open slot range (got {}..{})",
            range.start, range.end
        ));
    }
    validate_runtime_registry()?;

    let mut cursor = range.start;
    let mut segments = Vec::new();
    while cursor < range.end {
        let execution = runtime_at(cursor)?;
        if execution.backend == EraBackend::Unsupported {
            return Err(format!(
                "slot {} falls in unsupported runtime era {} ({}); refusing to guess consensus semantics",
                cursor,
                execution.name,
                execution.display_range(),
            ));
        }
        if execution.admission == AdmissionLevel::Candidate && !allow_candidate_runtime {
            return Err(format!(
                "runtime era {} ({}) is candidate-only; set JETSTREAMER_ALLOW_CANDIDATE_RUNTIME=1 for differential replay",
                execution.name,
                execution.display_range(),
            ));
        }

        let execution_end = execution.end_slot_exclusive.unwrap_or(range.end);
        let input_end = input_end_exclusive(cursor).unwrap_or(range.end);
        let segment_end = range.end.min(execution_end).min(input_end);
        debug_assert!(segment_end > cursor);
        segments.push(ReplaySegment {
            slots: cursor..segment_end,
            input_metadata: old_faithful_meta_encoding(cursor),
            missing_transaction_status: missing_transaction_status_at(cursor),
            transaction_status_validation: transaction_status_validation_at(cursor),
            execution,
            output: OutputSchema::HorizonV2,
        });
        cursor = segment_end;
    }
    Ok(segments)
}

/// Selects one execution runtime for a live bank span. Input-only boundaries
/// may be crossed transparently, but changing execution semantics requires a
/// verified state handoff and therefore returns an error here.
pub fn select_runtime(
    range: Range<Slot>,
    allow_candidate_runtime: bool,
) -> Result<RuntimeSelection, String> {
    let plans = plan_replay(range.clone(), allow_candidate_runtime)?;
    let first_era = plans
        .first()
        .expect("non-empty replay range produces a segment")
        .execution;
    let EraBackend::Available(descriptor) = first_era.backend else {
        unreachable!("unsupported eras fail while planning")
    };
    if plans.iter().any(|plan| match plan.execution.backend {
        EraBackend::Available(candidate) => candidate.backend != descriptor.backend,
        EraBackend::Unsupported => true,
    }) {
        return Err(format!(
            "slot range {}..{} crosses an execution-runtime boundary; split it at a verified state handoff",
            range.start, range.end
        ));
    }
    let admission = if plans
        .iter()
        .any(|plan| plan.execution.admission == AdmissionLevel::Candidate)
    {
        AdmissionLevel::Candidate
    } else {
        AdmissionLevel::Verified
    };
    Ok(RuntimeSelection {
        backend: descriptor.backend,
        descriptor,
        admission,
    })
}

/// Selects the runtime that owns an output range while validating an earlier
/// snapshot warm-up range. A worker may consume pre-output slots from the
/// preceding runtime era only at an explicitly registered snapshot-isolated
/// epoch boundary. This is not a state handoff: the destination worker must
/// independently restore and verify the canonical predecessor snapshot, and
/// no output archive may cross the boundary.
pub fn select_runtime_with_snapshot_warmup(
    replay_start: Slot,
    output_range: Range<Slot>,
    allow_candidate_runtime: bool,
) -> Result<RuntimeSelection, String> {
    if replay_start > output_range.start {
        return Err(format!(
            "snapshot warm-up starts at {replay_start}, after output starts at {}",
            output_range.start
        ));
    }
    let output = select_runtime(output_range.clone(), allow_candidate_runtime)?;
    if replay_start == output_range.start {
        return Ok(output);
    }

    let complete_range = replay_start..output_range.end;
    match select_runtime(complete_range.clone(), allow_candidate_runtime) {
        Ok(selection) => return Ok(selection),
        Err(crossing_error) => {
            let boundary = output_range.start;
            if !SNAPSHOT_ISOLATED_RUNTIME_BOUNDARIES.contains(&boundary) {
                return Err(crossing_error);
            }
        }
    }

    let spans = plan_runtime_spans(complete_range, allow_candidate_runtime)?;
    if spans.len() != 2
        || spans[0].slots.end != output_range.start
        || spans[1].slots.start != output_range.start
        || spans[1].slots.end != output_range.end
        || !matches!(
            spans[1].execution.backend,
            EraBackend::Available(descriptor) if std::ptr::eq(descriptor, output.descriptor)
        )
    {
        return Err(format!(
            "snapshot warm-up {}..{} does not form one registered predecessor span followed by output runtime {} at isolated boundary {}",
            replay_start, output_range.start, output.backend, output_range.start,
        ));
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_registry_is_complete_and_well_formed() {
        validate_runtime_registry().unwrap();
        assert_eq!(RUNTIME_DESCRIPTORS.len(), 12);
        for descriptor in RUNTIME_DESCRIPTORS.iter().copied() {
            assert!(std::ptr::eq(
                descriptor,
                descriptor.backend.descriptor().unwrap()
            ));
        }
    }

    #[test]
    fn descriptor_owns_bootstrap_and_worker_compatibility() {
        let early = RuntimeBackend::SolanaV1_0_7.descriptor().unwrap();
        assert_eq!(
            early.bootstrap.loader,
            BootstrapStateLoader::HistoricalWorkerSnapshotArchive
        );
        assert!(early.accepts_bootstrap_archive_name(
            "snapshot-416012-11111111111111111111111111111111.tar.bz2"
        ));
        assert!(!early.bootstrap.permits_in_memory_handoff);
        let early_worker = early.worker.unwrap();
        assert_eq!(
            early_worker.identity,
            "jetstreamer-historical-worker-v1-0-7"
        );
        assert_eq!(
            early_worker.environment_override,
            "JETSTREAMER_HISTORICAL_WORKER_V1_0_7"
        );
        assert_eq!(
            early_worker.default_manifest_relative_path,
            "../historical-runtime/v1_0_7/target/release/jetstreamer-historical-worker-v1-0-7"
        );

        let vote_signature_boundary = RuntimeBackend::SolanaV1_0_8.descriptor().unwrap();
        assert_eq!(
            vote_signature_boundary.identity.revision,
            SOLANA_V1_0_8_REVISION
        );
        assert_eq!(
            vote_signature_boundary.bootstrap.loader,
            BootstrapStateLoader::HistoricalWorkerSnapshotArchive
        );
        let boundary_worker = vote_signature_boundary.worker.unwrap();
        assert_eq!(
            boundary_worker.identity,
            "jetstreamer-historical-worker-v1-0-8"
        );
        assert_eq!(
            boundary_worker.environment_override,
            "JETSTREAMER_HISTORICAL_WORKER_V1_0_8"
        );
        assert_eq!(
            boundary_worker.default_manifest_relative_path,
            "../historical-runtime/v1_0_8/target/release/jetstreamer-historical-worker-v1-0-8"
        );

        let schema_1_1_candidate = RuntimeBackend::SolanaV1_0_13.descriptor().unwrap();
        assert_eq!(
            schema_1_1_candidate.identity.revision,
            SOLANA_V1_0_13_REVISION
        );
        let schema_1_1_worker = schema_1_1_candidate.worker.unwrap();
        assert_eq!(
            schema_1_1_worker.environment_override,
            "JETSTREAMER_HISTORICAL_WORKER_V1_0_13"
        );
        assert_eq!(
            schema_1_1_worker.default_manifest_relative_path,
            "../historical-runtime/v1_0_13/target/release/jetstreamer-historical-worker-v1-0-13"
        );

        for (backend, revision, environment_override, relative_path) in [
            (
                RuntimeBackend::SolanaV1_0_14,
                SOLANA_V1_0_14_REVISION,
                "JETSTREAMER_HISTORICAL_WORKER_V1_0_14",
                "../historical-runtime/v1_0_14/target/release/jetstreamer-historical-worker-v1-0-14",
            ),
            (
                RuntimeBackend::SolanaV1_0_17,
                SOLANA_V1_0_17_REVISION,
                "JETSTREAMER_HISTORICAL_WORKER_V1_0_17",
                "../historical-runtime/v1_0_17/target/release/jetstreamer-historical-worker-v1-0-17",
            ),
            (
                RuntimeBackend::SolanaV1_0_18,
                SOLANA_V1_0_18_REVISION,
                "JETSTREAMER_HISTORICAL_WORKER_V1_0_18",
                "../historical-runtime/v1_0_18/target/release/jetstreamer-historical-worker-v1-0-18",
            ),
            (
                RuntimeBackend::SolanaV1_0_23,
                SOLANA_V1_0_23_REVISION,
                "JETSTREAMER_HISTORICAL_WORKER_V1_0_23",
                "../historical-runtime/v1_0_23/target/release/jetstreamer-historical-worker-v1-0-23",
            ),
            (
                RuntimeBackend::SolanaV1_1_23,
                SOLANA_V1_1_23_REVISION,
                "JETSTREAMER_HISTORICAL_WORKER_V1_1_23",
                "../historical-runtime/v1_1_23/target/release/jetstreamer-historical-worker-v1-1-23",
            ),
            (
                RuntimeBackend::SolanaV1_2_32,
                SOLANA_V1_2_32_REVISION,
                "JETSTREAMER_HISTORICAL_WORKER_V1_2_32",
                "../historical-runtime/v1_2_32/target/release/jetstreamer-historical-worker-v1-2-32",
            ),
            (
                RuntimeBackend::SolanaV1_3_19,
                SOLANA_V1_3_19_REVISION,
                "JETSTREAMER_HISTORICAL_WORKER_V1_3_19",
                "../historical-runtime/v1_3_19/target/release/jetstreamer-historical-worker-v1-3-19",
            ),
        ] {
            let descriptor = backend.descriptor().unwrap();
            assert_eq!(descriptor.identity.revision, revision);
            let worker = descriptor.worker.unwrap();
            assert_eq!(worker.environment_override, environment_override);
            assert_eq!(worker.default_manifest_relative_path, relative_path);
        }

        let historical = RuntimeBackend::SolanaV1_0_24.descriptor().unwrap();
        assert_eq!(
            historical.bootstrap.loader,
            BootstrapStateLoader::HistoricalWorkerSnapshotArchive
        );
        assert!(historical.accepts_bootstrap_archive_name(
            "snapshot-416012-11111111111111111111111111111111.tar.bz2"
        ));
        assert!(!historical.accepts_bootstrap_archive_name(
            "snapshot-416012-11111111111111111111111111111111.tar.zst"
        ));
        assert!(!historical.bootstrap.permits_in_memory_handoff);
        assert!(historical.permits_live_epoch_handoff());
        let worker = historical.worker.unwrap();
        assert_eq!(worker.identity, "jetstreamer-historical-worker-v1-0-24");
        assert_eq!(worker.environment_override, "JETSTREAMER_HISTORICAL_WORKER");
        assert_eq!(
            worker.default_manifest_relative_path,
            "../historical-runtime/v1_0_24/target/release/jetstreamer-historical-worker-v1-0-24"
        );

        let agave = RuntimeBackend::AgaveV3.descriptor().unwrap();
        assert_eq!(
            agave.bootstrap.loader,
            BootstrapStateLoader::AgaveSnapshotArchive
        );
        assert!(agave.accepts_bootstrap_archive_name(
            "snapshot-406080000-11111111111111111111111111111111.tar.zst"
        ));
        assert!(agave.accepts_bootstrap_archive_name(
            "snapshot-406080000-11111111111111111111111111111111.tar.lz4"
        ));
        assert!(!agave.accepts_bootstrap_archive_name(
            "snapshot-406080000-11111111111111111111111111111111.tar.bz2"
        ));
        assert!(agave.bootstrap.permits_in_memory_handoff);
        assert!(agave.permits_live_epoch_handoff());
        assert!(agave.worker.is_none());
    }

    #[test]
    fn snapshot_filename_hash_and_container_support_are_runtime_scoped() {
        let legacy_bzip2_only = [
            RuntimeBackend::SolanaV1_0_7,
            RuntimeBackend::SolanaV1_0_8,
            RuntimeBackend::SolanaV1_0_13,
            RuntimeBackend::SolanaV1_0_14,
            RuntimeBackend::SolanaV1_0_17,
            RuntimeBackend::SolanaV1_0_18,
            RuntimeBackend::SolanaV1_0_23,
            RuntimeBackend::SolanaV1_0_24,
            RuntimeBackend::SolanaV1_1_23,
        ];
        for backend in legacy_bzip2_only {
            let bootstrap = backend.descriptor().unwrap().bootstrap;
            assert_eq!(
                bootstrap.snapshot_hash_kind,
                SnapshotHashKind::LegacyAccountsHash
            );
            assert_eq!(bootstrap.archive_extensions, &[".tar.bz2"]);
        }

        for backend in [RuntimeBackend::SolanaV1_2_32, RuntimeBackend::SolanaV1_3_19] {
            let descriptor = backend.descriptor().unwrap();
            assert_eq!(
                descriptor.bootstrap.snapshot_hash_kind,
                SnapshotHashKind::LegacyAccountsHash
            );
            assert_eq!(
                descriptor.bootstrap.archive_extensions,
                &[".tar.bz2", ".tar.zst"]
            );
            assert!(descriptor.accepts_bootstrap_archive_name("snapshot-1-hash.tar.bz2"));
            assert!(descriptor.accepts_bootstrap_archive_name("snapshot-1-hash.tar.zst"));
            assert!(!descriptor.accepts_bootstrap_archive_name("snapshot-1-hash.tar.lz4"));
        }

        assert_eq!(
            AGAVE_V3_RUNTIME.bootstrap.snapshot_hash_kind,
            SnapshotHashKind::AccountsLtHash
        );

        let mut historical_with_current_hash = SOLANA_V1_2_32_RUNTIME;
        historical_with_current_hash.bootstrap.snapshot_hash_kind =
            SnapshotHashKind::AccountsLtHash;
        assert!(
            historical_with_current_hash
                .validate()
                .unwrap_err()
                .contains("expected LegacyAccountsHash")
        );

        let mut agave_with_legacy_hash = AGAVE_V3_RUNTIME;
        agave_with_legacy_hash.bootstrap.snapshot_hash_kind = SnapshotHashKind::LegacyAccountsHash;
        assert!(
            agave_with_legacy_hash
                .validate()
                .unwrap_err()
                .contains("expected AccountsLtHash")
        );
    }

    #[test]
    fn unassigned_candidates_remain_registered_without_claiming_an_era() {
        for backend in [
            RuntimeBackend::SolanaV1_0_17,
            RuntimeBackend::SolanaV1_0_18,
            RuntimeBackend::SolanaV1_0_24,
        ] {
            assert!(backend.descriptor().is_ok());
            assert!(!RUNTIME_ERAS.iter().any(|era| {
                matches!(
                    era.backend,
                    EraBackend::Available(descriptor) if descriptor.backend == backend
                )
            }));
        }
    }

    #[test]
    fn early_candidate_requires_explicit_admission() {
        const {
            assert!(
                SOLANA_V1_0_7_OLD_VOTE_SEMANTICS_OBSERVED_THROUGH_SLOT
                    < SOLANA_V1_0_8_ROUTING_START_SLOT
            );
        }
        let error =
            select_runtime(432_000..SOLANA_V1_0_8_CANDIDATE_END_SLOT_EXCLUSIVE, false).unwrap_err();
        assert!(error.contains("candidate-only"));

        let selection = select_runtime(432_000..SOLANA_V1_0_8_ROUTING_START_SLOT, true).unwrap();
        assert_eq!(selection.backend, RuntimeBackend::SolanaV1_0_7);
        assert!(std::ptr::eq(selection.descriptor, &SOLANA_V1_0_7_RUNTIME));

        let selection = select_runtime(SOLANA_V1_0_8_ROUTING_START_SLOT..864_000, true).unwrap();
        assert_eq!(selection.backend, RuntimeBackend::SolanaV1_0_8);
        assert!(std::ptr::eq(selection.descriptor, &SOLANA_V1_0_8_RUNTIME));

        let error = select_runtime(432_000..864_000, true).unwrap_err();
        assert!(error.contains("crosses an execution-runtime boundary"));
    }

    #[test]
    fn epoch_7_is_bounded_candidate_v1_0_8_work() {
        let epoch_7 = 3_024_000..SOLANA_V1_0_8_CANDIDATE_END_SLOT_EXCLUSIVE;

        let error = select_runtime(epoch_7.clone(), false).unwrap_err();
        assert!(error.contains("candidate-only"));

        let selection = select_runtime(epoch_7, true).unwrap();
        assert_eq!(selection.backend, RuntimeBackend::SolanaV1_0_8);
        assert!(std::ptr::eq(selection.descriptor, &SOLANA_V1_0_8_RUNTIME));

        let last_slot = SOLANA_V1_0_8_CANDIDATE_END_SLOT_EXCLUSIVE - 1;
        assert!(select_runtime(last_slot..last_slot + 1, true).is_ok());
        let successor = select_runtime(last_slot + 1..last_slot + 2, true).unwrap();
        assert_eq!(successor.backend, RuntimeBackend::SolanaV1_0_13);
    }

    #[test]
    fn epoch_8_uses_the_exact_v1_0_13_candidate() {
        let range =
            SOLANA_V1_0_13_CANDIDATE_START_SLOT..SOLANA_V1_0_13_CANDIDATE_END_SLOT_EXCLUSIVE;
        let selection = select_runtime(range, true).unwrap();
        assert_eq!(selection.backend, RuntimeBackend::SolanaV1_0_13);
        assert!(std::ptr::eq(selection.descriptor, &SOLANA_V1_0_13_RUNTIME));

        let spans = plan_runtime_spans(
            SOLANA_V1_0_13_CANDIDATE_START_SLOT..SOLANA_V1_0_13_CANDIDATE_START_SLOT + 1,
            true,
        )
        .unwrap();
        assert!(spans[0].handoff.is_none());
        assert!(
            SNAPSHOT_ISOLATED_RUNTIME_BOUNDARIES.contains(&SOLANA_V1_0_13_CANDIDATE_START_SLOT)
        );
    }

    #[test]
    fn epochs_9_through_11_are_one_v1_0_14_candidate_era() {
        let range =
            SOLANA_V1_0_14_CANDIDATE_START_SLOT..SOLANA_V1_0_14_CANDIDATE_END_SLOT_EXCLUSIVE;
        let spans = plan_runtime_spans(range.clone(), true).unwrap();
        assert_eq!(spans.len(), 1);
        assert_eq!(spans[0].slots, range.clone());
        assert!(spans[0].handoff.is_none());

        let selection = select_runtime(range, true).unwrap();
        assert_eq!(selection.backend, RuntimeBackend::SolanaV1_0_14);
        assert!(std::ptr::eq(selection.descriptor, &SOLANA_V1_0_14_RUNTIME));

        assert!(
            SNAPSHOT_ISOLATED_RUNTIME_BOUNDARIES.contains(&SOLANA_V1_0_14_CANDIDATE_START_SLOT)
        );
        assert!(!SNAPSHOT_ISOLATED_RUNTIME_BOUNDARIES.contains(&4_752_000));
        assert!(!RUNTIME_ERAS.iter().any(|era| era.start_slot == 4_752_000));
        assert!(
            !RUNTIME_HANDOFFS
                .iter()
                .any(|handoff| handoff.boundary_slot == 4_752_000)
        );
    }

    #[test]
    fn epoch_12_uses_the_v1_0_23_snapshot_isolated_warmup() {
        const EPOCH_12_END_SLOT_EXCLUSIVE: Slot = 5_616_000;
        assert_eq!(SOLANA_V1_0_23_CANDIDATE_START_SLOT, 5_184_000);
        assert_eq!(SOLANA_V1_0_23_INITIAL_REPLAY_SLOT, 5_183_737);
        assert_eq!(
            SOLANA_V1_0_23_CANDIDATE_START_SLOT - SOLANA_V1_0_23_INITIAL_REPLAY_SLOT,
            263
        );
        assert!(
            SNAPSHOT_ISOLATED_RUNTIME_BOUNDARIES.contains(&SOLANA_V1_0_23_CANDIDATE_START_SLOT)
        );
        assert!(
            !RUNTIME_HANDOFFS
                .iter()
                .any(|handoff| handoff.boundary_slot == SOLANA_V1_0_23_CANDIDATE_START_SLOT)
        );

        let selection = select_runtime_with_snapshot_warmup(
            SOLANA_V1_0_23_INITIAL_REPLAY_SLOT,
            SOLANA_V1_0_23_CANDIDATE_START_SLOT..EPOCH_12_END_SLOT_EXCLUSIVE,
            true,
        )
        .unwrap();
        assert_eq!(selection.backend, RuntimeBackend::SolanaV1_0_23);
        assert!(std::ptr::eq(selection.descriptor, &SOLANA_V1_0_23_RUNTIME));

        let spans = plan_runtime_spans(
            SOLANA_V1_0_23_INITIAL_REPLAY_SLOT..EPOCH_12_END_SLOT_EXCLUSIVE,
            true,
        )
        .unwrap();
        assert_eq!(spans.len(), 2);
        assert_eq!(
            spans[0].slots,
            SOLANA_V1_0_23_INITIAL_REPLAY_SLOT..SOLANA_V1_0_23_CANDIDATE_START_SLOT
        );
        assert!(matches!(
            spans[0].execution.backend,
            EraBackend::Available(descriptor)
                if std::ptr::eq(descriptor, &SOLANA_V1_0_14_RUNTIME)
        ));
        assert_eq!(
            spans[1].slots,
            SOLANA_V1_0_23_CANDIDATE_START_SLOT..EPOCH_12_END_SLOT_EXCLUSIVE
        );
        assert!(matches!(
            spans[1].execution.backend,
            EraBackend::Available(descriptor)
                if std::ptr::eq(descriptor, &SOLANA_V1_0_23_RUNTIME)
        ));
        assert!(spans.iter().all(|span| span.handoff.is_none()));
    }

    #[test]
    fn epochs_12_through_100_route_to_checkpoint_gated_terminal_patch_candidates() {
        for (range, backend, descriptor, boundary) in [
            (
                SOLANA_V1_0_23_CANDIDATE_START_SLOT..SOLANA_V1_0_23_CANDIDATE_END_SLOT_EXCLUSIVE,
                RuntimeBackend::SolanaV1_0_23,
                &SOLANA_V1_0_23_RUNTIME,
                SOLANA_V1_0_23_CANDIDATE_START_SLOT,
            ),
            (
                SOLANA_V1_1_23_CANDIDATE_START_SLOT..SOLANA_V1_1_23_CANDIDATE_END_SLOT_EXCLUSIVE,
                RuntimeBackend::SolanaV1_1_23,
                &SOLANA_V1_1_23_RUNTIME,
                SOLANA_V1_1_23_CANDIDATE_START_SLOT,
            ),
            (
                SOLANA_V1_2_32_CANDIDATE_START_SLOT..SOLANA_V1_2_32_CANDIDATE_END_SLOT_EXCLUSIVE,
                RuntimeBackend::SolanaV1_2_32,
                &SOLANA_V1_2_32_RUNTIME,
                SOLANA_V1_2_32_CANDIDATE_START_SLOT,
            ),
            (
                SOLANA_V1_3_19_CANDIDATE_START_SLOT..SOLANA_V1_3_19_CANDIDATE_END_SLOT_EXCLUSIVE,
                RuntimeBackend::SolanaV1_3_19,
                &SOLANA_V1_3_19_RUNTIME,
                SOLANA_V1_3_19_CANDIDATE_START_SLOT,
            ),
        ] {
            let error = select_runtime(range.clone(), false).unwrap_err();
            assert!(error.contains("candidate-only"));
            let selection = select_runtime(range, true).unwrap();
            assert_eq!(selection.backend, backend);
            assert!(std::ptr::eq(selection.descriptor, descriptor));
            assert!(SNAPSHOT_ISOLATED_RUNTIME_BOUNDARIES.contains(&boundary));
        }
    }

    #[test]
    fn snapshot_warmup_may_cross_only_at_the_registered_output_boundary() {
        let epoch_8 = select_runtime_with_snapshot_warmup(
            3_455_941,
            SOLANA_V1_0_13_CANDIDATE_START_SLOT..SOLANA_V1_0_13_CANDIDATE_END_SLOT_EXCLUSIVE,
            true,
        )
        .unwrap();
        assert_eq!(epoch_8.backend, RuntimeBackend::SolanaV1_0_13);

        let error = select_runtime_with_snapshot_warmup(
            3_455_941,
            SOLANA_V1_0_13_CANDIDATE_START_SLOT + 1..SOLANA_V1_0_13_CANDIDATE_END_SLOT_EXCLUSIVE,
            true,
        )
        .unwrap_err();
        assert!(error.contains("crosses an execution-runtime boundary"));

        let error = select_runtime_with_snapshot_warmup(
            SOLANA_V1_0_13_CANDIDATE_START_SLOT + 1,
            SOLANA_V1_0_13_CANDIDATE_START_SLOT..SOLANA_V1_0_13_CANDIDATE_END_SLOT_EXCLUSIVE,
            true,
        )
        .unwrap_err();
        assert!(error.contains("after output starts"));
    }

    #[test]
    fn first_historical_handoff_is_split_at_snapshot_successor() {
        let segments = plan_replay(
            SOLANA_V1_0_8_HANDOFF_SNAPSHOT_SLOT..SOLANA_V1_0_8_ROUTING_START_SLOT + 1,
            true,
        )
        .unwrap();
        assert_eq!(segments.len(), 2);
        assert_eq!(
            segments[0].slots,
            SOLANA_V1_0_8_HANDOFF_SNAPSHOT_SLOT..SOLANA_V1_0_8_ROUTING_START_SLOT
        );
        assert_eq!(
            segments[0].execution.backend,
            EraBackend::Available(&SOLANA_V1_0_7_RUNTIME)
        );
        assert_eq!(
            segments[1].slots,
            SOLANA_V1_0_8_ROUTING_START_SLOT..SOLANA_V1_0_8_ROUTING_START_SLOT + 1
        );
        assert_eq!(
            segments[1].execution.backend,
            EraBackend::Available(&SOLANA_V1_0_8_RUNTIME)
        );
    }

    #[test]
    fn epoch_one_runtime_spans_are_exact_and_carry_the_committed_handoff() {
        let requested = 432_000..864_000;
        let spans = plan_runtime_spans(requested.clone(), true).unwrap();
        assert_eq!(spans.len(), 2);
        assert_eq!(spans[0].slots, 432_000..SOLANA_V1_0_8_ROUTING_START_SLOT);
        assert_eq!(
            spans[0].execution.backend,
            EraBackend::Available(&SOLANA_V1_0_7_RUNTIME)
        );
        assert!(spans[0].handoff.is_none());

        assert_eq!(spans[1].slots, SOLANA_V1_0_8_ROUTING_START_SLOT..864_000);
        assert_eq!(
            spans[1].execution.backend,
            EraBackend::Available(&SOLANA_V1_0_8_RUNTIME)
        );
        let handoff = spans[1].handoff.unwrap();
        assert!(std::ptr::eq(handoff, &SOLANA_V1_0_7_TO_V1_0_8_HANDOFF));
        assert_eq!(handoff.boundary_slot, 619_849);
        assert_eq!(handoff.snapshot.slot, 619_848);
        assert_eq!(
            handoff.snapshot.accounts_hash_base58,
            "Fc4tnsTue85dYTMHfTUm3RJjv5nr8wB1HP9hPcY93G7p"
        );
        assert_eq!(
            handoff.snapshot.archive_name(),
            "snapshot-619848-Fc4tnsTue85dYTMHfTUm3RJjv5nr8wB1HP9hPcY93G7p.tar.bz2"
        );
        handoff.snapshot.accounts_hash().unwrap();

        let mut cursor = requested.start;
        for span in &spans {
            assert_eq!(span.slots.start, cursor);
            assert!(span.slots.end > span.slots.start);
            cursor = span.slots.end;
        }
        assert_eq!(cursor, requested.end);
    }

    #[test]
    fn destination_only_plan_still_exposes_its_canonical_bootstrap() {
        let spans = plan_runtime_spans(
            SOLANA_V1_0_8_ROUTING_START_SLOT..SOLANA_V1_0_8_ROUTING_START_SLOT + 1,
            true,
        )
        .unwrap();
        assert_eq!(spans.len(), 1);
        assert!(std::ptr::eq(
            spans[0].handoff.unwrap(),
            &SOLANA_V1_0_7_TO_V1_0_8_HANDOFF
        ));
    }

    #[test]
    fn runtime_change_without_handoff_metadata_is_rejected() {
        let error = validate_runtime_handoffs(RUNTIME_DESCRIPTORS, RUNTIME_ERAS, &[]).unwrap_err();
        assert!(error.contains("without a canonical snapshot handoff"));
    }

    #[test]
    fn input_codec_boundary_is_not_an_execution_boundary() {
        assert_eq!(
            old_faithful_meta_encoding(OLD_FAITHFUL_PROTOBUF_META_START_SLOT - 1),
            OldFaithfulMetaEncoding::BincodeWithProtobufFallback
        );
        assert_eq!(
            old_faithful_meta_encoding(OLD_FAITHFUL_PROTOBUF_META_START_SLOT),
            OldFaithfulMetaEncoding::Protobuf
        );
    }

    #[test]
    fn modern_epoch_selects_verified_agave() {
        let selection = select_runtime(412_000_000..412_432_000, false).unwrap();
        assert_eq!(selection.backend, RuntimeBackend::AgaveV3);
        assert!(std::ptr::eq(selection.descriptor, &AGAVE_V3_RUNTIME));
        assert_eq!(selection.admission, AdmissionLevel::Verified);
    }

    #[test]
    fn unknown_middle_history_fails_closed() {
        let start = SOLANA_V1_3_19_CANDIDATE_END_SLOT_EXCLUSIVE;
        let error = select_runtime(start..start + 1, true).unwrap_err();
        assert!(error.contains("unsupported runtime era"));
    }

    #[test]
    fn crossing_into_an_unknown_era_fails_closed() {
        let boundary = SOLANA_V1_3_19_CANDIDATE_END_SLOT_EXCLUSIVE;
        let error = plan_replay(boundary - 1..boundary + 1, true).unwrap_err();
        assert!(error.contains("unsupported runtime era"));
    }

    #[test]
    fn era_table_is_contiguous_and_ordered() {
        assert_eq!(RUNTIME_ERAS.first().unwrap().start_slot, 0);
        for pair in RUNTIME_ERAS.windows(2) {
            assert_eq!(pair[0].end_slot_exclusive, Some(pair[1].start_slot));
        }
        assert_eq!(RUNTIME_ERAS.last().unwrap().end_slot_exclusive, None);
    }

    #[test]
    fn missing_status_policy_changes_at_its_input_boundary() {
        let start = OLD_FAITHFUL_STATUS_REQUIRED_START_SLOT - 1;
        let boundary = OLD_FAITHFUL_STATUS_REQUIRED_START_SLOT;
        assert_eq!(
            missing_transaction_status_at(start),
            MissingTransactionStatus::Reconstruct
        );
        assert_eq!(input_end_exclusive(start), Some(boundary));
        assert_eq!(
            missing_transaction_status_at(boundary),
            MissingTransactionStatus::Reject
        );
    }

    #[test]
    fn transaction_status_validation_tracks_source_writer_eras() {
        assert_eq!(
            transaction_status_validation_at(OLD_FAITHFUL_STATUS_REQUIRED_START_SLOT - 1),
            TransactionStatusValidation::RuntimeOnly
        );
        assert_eq!(
            transaction_status_validation_at(OLD_FAITHFUL_STATUS_REQUIRED_START_SLOT),
            TransactionStatusValidation::RuntimeWithSourceEntryMultiset
        );
        assert_eq!(
            transaction_status_validation_at(
                OLD_FAITHFUL_UNTRUSTED_STATUS_ASSOCIATION_END_SLOT_EXCLUSIVE - 1
            ),
            TransactionStatusValidation::RuntimeWithSourceEntryMultiset
        );
        assert_eq!(
            transaction_status_validation_at(
                OLD_FAITHFUL_UNTRUSTED_STATUS_ASSOCIATION_END_SLOT_EXCLUSIVE
            ),
            TransactionStatusValidation::SourceExact
        );
    }
}
