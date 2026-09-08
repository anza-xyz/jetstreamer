//! Parent-side client for isolated historical Solana runtime workers.
//!
//! Historical runtimes are separate executables because their persisted data
//! formats and consensus behavior depend on the original compiler, target,
//! dependency graph, and Solana source revision.  This module deliberately
//! treats the child as untrusted IPC: every frame is bounded by the protocol,
//! request IDs are strictly ordered, worker identity is checked before any
//! state is loaded, and all protocol byte vectors are validated before they are
//! exposed as fixed-size hashes, keys, or signatures.

use std::{
    env, fs,
    io::{self, BufReader, BufWriter, Read, Write},
    path::{Path, PathBuf},
    process::{Child, ChildStdin, ChildStdout, Command, ExitStatus, Stdio},
    sync::mpsc::{self, RecvTimeoutError},
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use jetstreamer_historical_protocol as protocol;
use protocol::{
    BackendQualification, InitialState, InitializedSource, Request, RequestBody, Response,
    ResponseBody,
};
use sha2::{Digest, Sha256};
#[cfg(test)]
use solana_hash::Hash;
use solana_transaction::{
    InstructionError as CurrentInstructionError, TransactionError as CurrentTransactionError,
    versioned::VersionedTransaction,
};
use tempfile::{Builder as TempDirBuilder, TempDir};

pub const HASH_BYTES: usize = 32;
pub const SIGNATURE_BYTES: usize = 64;

/// Historical workers are small, statically linked compatibility binaries.
/// Keep the admission ceiling comfortably above current artifacts while
/// preventing a replaced path from driving an unbounded private copy.
const MAX_HISTORICAL_WORKER_EXECUTABLE_BYTES: u64 = 256 * 1024 * 1024;

const SOLANA_V1_0_24_BACKEND_ID: &str = "solana-v1.0.24";
const SOLANA_V1_0_24_TAG: &str = "v1.0.24";
const SOLANA_V1_0_24_COMMIT: &str = "a93915f1bddb73480f86fc09f487315ae191897d";
const SOLANA_V1_0_24_RUST_TOOLCHAIN: &str = "rustc 1.43.0 (4fb7144ed 2020-04-20)";
const SOLANA_V1_0_24_TARGET: &str = "x86_64-unknown-linux-gnu";
const SOLANA_V1_0_7_BACKEND_ID: &str = "solana-v1.0.7";
const SOLANA_V1_0_7_TAG: &str = "v1.0.7";
const SOLANA_V1_0_7_COMMIT: &str = "57abc370fa39e42e8fb84145a30395ddcf891692";
const SOLANA_V1_0_7_RUST_TOOLCHAIN: &str = "rustc 1.42.0 (b8cedc004 2020-03-09)";
const SOLANA_V1_0_7_TARGET: &str = "x86_64-unknown-linux-gnu";
const SOLANA_V1_0_8_BACKEND_ID: &str = "solana-v1.0.8";
const SOLANA_V1_0_8_TAG: &str = "v1.0.8";
const SOLANA_V1_0_8_COMMIT: &str = "2a617f2d07f714918891f2b479d1cb1c324f0365";
const SOLANA_V1_0_8_RUST_TOOLCHAIN: &str = "rustc 1.42.0 (b8cedc004 2020-03-09)";
const SOLANA_V1_0_8_TARGET: &str = "x86_64-unknown-linux-gnu";

const DEFAULT_CONTROL_TIMEOUT: Duration = Duration::from_secs(60);
const DEFAULT_INITIALIZE_TIMEOUT: Duration = Duration::from_secs(6 * 60 * 60);
const DEFAULT_ENTRY_TIMEOUT: Duration = Duration::from_secs(60 * 60);
const DEFAULT_CHECKPOINT_TIMEOUT: Duration = Duration::from_secs(24 * 60 * 60);
const DEFAULT_REAP_TIMEOUT: Duration = Duration::from_secs(10);
const CHILD_WAIT_POLL_INTERVAL: Duration = Duration::from_millis(10);

const CONTROL_TIMEOUT_ENV: &str = "JETSTREAMER_HISTORICAL_CONTROL_TIMEOUT_SECS";
const INITIALIZE_TIMEOUT_ENV: &str = "JETSTREAMER_HISTORICAL_INITIALIZE_TIMEOUT_SECS";
const ENTRY_TIMEOUT_ENV: &str = "JETSTREAMER_HISTORICAL_ENTRY_TIMEOUT_SECS";
const CHECKPOINT_TIMEOUT_ENV: &str = "JETSTREAMER_HISTORICAL_CHECKPOINT_TIMEOUT_SECS";
const REAP_TIMEOUT_ENV: &str = "JETSTREAMER_HISTORICAL_REAP_TIMEOUT_SECS";
const REPLAY_ENTRY_TIMEOUT_ENV: &str = "JETSTREAMER_ENTRY_EXEC_TIMEOUT_SECS";

/// The complete identity expected from a historical worker executable.
///
/// Slot-range selection belongs in the compatibility registry.  That registry
/// selects one of these profiles; a profile itself never claims which slots it
/// is valid for.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WorkerProfile {
    pub backend_id: &'static str,
    pub solana_tag: &'static str,
    pub solana_commit: &'static str,
    pub rust_toolchain: &'static str,
    pub target: &'static str,
    pub required_genesis_hash: &'static str,
}

pub const SOLANA_V1_0_7_CANDIDATE: WorkerProfile = WorkerProfile {
    backend_id: SOLANA_V1_0_7_BACKEND_ID,
    solana_tag: SOLANA_V1_0_7_TAG,
    solana_commit: SOLANA_V1_0_7_COMMIT,
    rust_toolchain: SOLANA_V1_0_7_RUST_TOOLCHAIN,
    target: SOLANA_V1_0_7_TARGET,
    required_genesis_hash: protocol::MAINNET_GENESIS_HASH,
};

pub const SOLANA_V1_0_8_CANDIDATE: WorkerProfile = WorkerProfile {
    backend_id: SOLANA_V1_0_8_BACKEND_ID,
    solana_tag: SOLANA_V1_0_8_TAG,
    solana_commit: SOLANA_V1_0_8_COMMIT,
    rust_toolchain: SOLANA_V1_0_8_RUST_TOOLCHAIN,
    target: SOLANA_V1_0_8_TARGET,
    required_genesis_hash: protocol::MAINNET_GENESIS_HASH,
};

pub const SOLANA_V1_0_24_CANDIDATE: WorkerProfile = WorkerProfile {
    backend_id: SOLANA_V1_0_24_BACKEND_ID,
    solana_tag: SOLANA_V1_0_24_TAG,
    solana_commit: SOLANA_V1_0_24_COMMIT,
    rust_toolchain: SOLANA_V1_0_24_RUST_TOOLCHAIN,
    target: SOLANA_V1_0_24_TARGET,
    required_genesis_hash: protocol::MAINNET_GENESIS_HASH,
};

/// Exact snapshot identity supplied by the caller's archive-name parser.
///
/// Requiring these values keeps initialization fail-closed: the parent checks
/// that the worker actually loaded the snapshot it selected, rather than
/// trusting metadata returned by the worker.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotInitialization {
    pub ledger_path: PathBuf,
    pub archive_path: PathBuf,
    pub expected_slot: u64,
    pub expected_accounts_hash: [u8; HASH_BYTES],
    /// When present, copy the archive into parent-owned private storage while
    /// hashing it, and initialize the worker from only that bound copy.
    pub expected_archive_sha256: Option<[u8; HASH_BYTES]>,
    /// Exact byte length paired with `expected_archive_sha256`. Keeping the
    /// copy bounded prevents a concurrently growing input from turning
    /// admission into an unbounded read.
    pub expected_archive_size: Option<u64>,
    /// An existing directory in which the parent creates a private temporary
    /// directory.  The worker creates its own state directory below that.
    pub scratch_parent: Option<PathBuf>,
}

/// Spawn configuration separated from runtime identity so the same verified
/// profile can be deployed at different filesystem locations.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkerSpawn {
    pub executable: PathBuf,
    pub initialization: SnapshotInitialization,
}

#[derive(Debug, thiserror::Error)]
pub enum HistoricalRuntimeError {
    #[error("historical worker executable is not an absolute path: {0}")]
    ExecutableNotAbsolute(PathBuf),
    #[error("historical worker executable is not a regular file: {0}")]
    ExecutableNotFile(PathBuf),
    #[error("historical worker executable is empty: {0}")]
    ExecutableEmpty(PathBuf),
    #[error("historical worker executable {path} is {bytes} bytes (limit {limit})")]
    ExecutableTooLarge {
        path: PathBuf,
        bytes: u64,
        limit: u64,
    },
    #[error(
        "historical worker executable {path} changed size while being read: expected {expected}, got at least {actual}"
    )]
    ExecutableSizeChanged {
        path: PathBuf,
        expected: u64,
        actual: u64,
    },
    #[error("ledger path is not a directory: {0}")]
    LedgerNotDirectory(PathBuf),
    #[error("historical snapshot archive is not a regular file: {0}")]
    SnapshotNotFile(PathBuf),
    #[error("historical snapshot archive is not a legacy .tar.bz2 archive: {0}")]
    NotLegacySnapshot(PathBuf),
    #[error("historical snapshot archive SHA-256 mismatch: expected {expected}, got {actual}")]
    SnapshotArchiveDigestMismatch { expected: String, actual: String },
    #[error("historical snapshot archive size mismatch: expected {expected}, got {actual}")]
    SnapshotArchiveSizeMismatch { expected: u64, actual: u64 },
    #[error("historical snapshot archive binding must provide both size and SHA-256")]
    IncompleteSnapshotArchiveBinding,
    #[error("historical worker scratch parent is not a directory: {0}")]
    ScratchParentNotDirectory(PathBuf),
    #[error("historical snapshot output path is not a directory: {0}")]
    SnapshotOutputNotDirectory(PathBuf),
    #[error("path is not valid UTF-8: {0}")]
    NonUtf8Path(PathBuf),
    #[error("failed to access {path}: {source}")]
    PathIo {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("path no longer names the opened regular file: {0}")]
    PathIdentityChanged(PathBuf),
    #[error("failed to create private historical worker directory below {path}: {source}")]
    CreatePrivateWorkDir {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("failed to spawn historical worker {path}: {source}")]
    Spawn {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("failed to start historical worker IPC thread: {0}")]
    SpawnIpcThread(#[source] io::Error),
    #[error("historical worker did not provide a {stream} pipe")]
    MissingPipe { stream: &'static str },
    #[error("historical worker I/O failed: {0}")]
    Io(#[from] io::Error),
    #[error(
        "historical worker {operation} request {request_id} timed out after {timeout:?}; worker was terminated"
    )]
    RequestTimeout {
        operation: &'static str,
        request_id: u64,
        timeout: Duration,
    },
    #[error("historical worker IPC thread stopped during {operation} request {request_id}")]
    IpcStopped {
        operation: &'static str,
        request_id: u64,
    },
    #[error(
        "historical worker acknowledged shutdown but did not exit within {timeout:?}; worker was terminated"
    )]
    ShutdownExitTimeout { timeout: Duration },
    #[error("{variable} must be a positive integer number of seconds, got {value:?}")]
    InvalidTimeout {
        variable: &'static str,
        value: String,
    },
    #[error("historical worker closed stdout while request {request_id} was outstanding")]
    UnexpectedEof { request_id: u64 },
    #[error("historical worker request ID overflow")]
    RequestIdOverflow,
    #[error(
        "historical worker response ID {actual} is not newer than previous response ID {previous}"
    )]
    NonMonotonicResponseId { previous: u64, actual: u64 },
    #[error("historical worker response ID mismatch: requested {expected}, received {actual}")]
    ResponseIdMismatch { expected: u64, actual: u64 },
    #[error("historical worker returned {actual} while {expected} was expected")]
    UnexpectedResponse {
        expected: &'static str,
        actual: &'static str,
    },
    #[error(
        "historical worker identity mismatch for {field}: expected {expected:?}, got {actual:?}"
    )]
    IdentityMismatch {
        field: &'static str,
        expected: String,
        actual: String,
    },
    #[error("historical worker is not a candidate backend: {0:?}")]
    QualificationMismatch(BackendQualification),
    #[error("historical worker rejected the request ({code:?}): {message}")]
    Worker {
        code: protocol::WorkerErrorCode,
        message: String,
    },
    #[error("worker initialized with genesis {actual}, expected {expected}")]
    InitializedGenesisMismatch { expected: String, actual: String },
    #[error("worker initialized at slot {actual}, expected snapshot slot {expected}")]
    InitializedSlotMismatch { expected: u64, actual: u64 },
    #[error("worker initialized from the wrong source: {0}")]
    InitializedSourceMismatch(String),
    #[error("worker reported snapshot path {actual:?}, expected {expected:?}")]
    InitializedArchiveMismatch { expected: String, actual: String },
    #[error("{field} must contain exactly {expected} bytes, got {actual}")]
    InvalidArrayLength {
        field: &'static str,
        expected: usize,
        actual: usize,
    },
    #[error(
        "entry response identifies ({actual_slot}, {actual_index}), expected ({expected_slot}, {expected_index})"
    )]
    EntryIdentityMismatch {
        expected_slot: u64,
        expected_index: u64,
        actual_slot: u64,
        actual_index: u64,
    },
    #[error("entry response contains {actual} transaction outcomes, expected {expected}")]
    OutcomeCountMismatch { expected: usize, actual: usize },
    #[error("account write is for slot {actual}, expected {expected}")]
    AccountWriteSlotMismatch { expected: u64, actual: u64 },
    #[error(
        "entry account write is for slot {actual}; advancing from {previous} to {requested} permits only those two slots"
    )]
    EntryAccountWriteSlotMismatch {
        previous: u64,
        requested: u64,
        actual: u64,
    },
    #[error(
        "account write slot order regressed from {previous} to {actual} within one worker response"
    )]
    AccountWriteSlotOrder { previous: u64, actual: u64 },
    #[error("entry slot {requested} precedes the client's current worker slot {current}")]
    RequestedSlotRegression { current: u64, requested: u64 },
    #[error("checkpoint is for slot {actual}, expected {expected}")]
    CheckpointSlotMismatch { expected: u64, actual: u64 },
    #[error("historical worker returned an incomplete checkpoint for slot {slot}")]
    IncompleteCheckpoint { slot: u64 },
    #[error("snapshot export requires the immediately preceding successful checkpoint")]
    SnapshotExportWithoutCheckpoint,
    #[error(
        "snapshot export request for slot {requested} does not match the preceding checkpoint slot {checkpoint}"
    )]
    SnapshotExportCheckpointSlotMismatch { checkpoint: u64, requested: u64 },
    #[error("snapshot export request accounts hash does not match the preceding checkpoint")]
    SnapshotExportCheckpointHashMismatch,
    #[error("snapshot export response is for slot {actual}, expected {expected}")]
    SnapshotExportSlotMismatch { expected: u64, actual: u64 },
    #[error("snapshot export response accounts hash does not match the request")]
    SnapshotExportHashMismatch,
    #[error("snapshot export response path {actual:?} does not match {expected}")]
    SnapshotExportPathMismatch { expected: PathBuf, actual: String },
    #[error("snapshot export path is not a regular file: {0}")]
    SnapshotExportNotFile(PathBuf),
    #[error("snapshot export response reports {reported} bytes, file has {actual} bytes")]
    SnapshotExportSizeMismatch { reported: u64, actual: u64 },
    #[error("snapshot export SHA-256 mismatch: worker {reported}, parent {actual}")]
    SnapshotExportDigestMismatch { reported: String, actual: String },
    #[error("snapshot export archive is empty: {0}")]
    SnapshotExportEmpty(PathBuf),
    #[error(
        "account write {index} has write version {actual}, expected the contiguous version {expected}"
    )]
    WriteVersionGap {
        index: usize,
        expected: u64,
        actual: u64,
    },
    #[error("account write version overflow at {0}")]
    WriteVersionOverflow(u64),
    #[error("worker reported next write version {actual}, expected {expected} from its writes")]
    WriteVersionCursorMismatch { expected: u64, actual: u64 },
    #[error("historical worker has already shut down")]
    Closed,
    #[error("historical worker exited unsuccessfully: {0}")]
    UnsuccessfulExit(ExitStatus),
    #[error("only legacy transactions can be sent to a Solana v1 runtime")]
    VersionedTransactionUnsupported,
    #[error("failed to serialize legacy transaction: {0}")]
    TransactionSerialization(#[source] bincode::Error),
    #[error("encoded transaction {index} is too large for one IPC frame: {bytes} bytes")]
    EncodedTransactionTooLarge { index: usize, bytes: usize },
    #[error(
        "current transaction status has no equivalent in the historical v1 status vocabulary: {0:?}"
    )]
    UnsupportedTransactionError(CurrentTransactionError),
    #[error(
        "current instruction status has no equivalent in the historical v1 status vocabulary: {0:?}"
    )]
    UnsupportedInstructionError(CurrentInstructionError),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HistoricalInitialized {
    pub genesis_hash: String,
    pub slot: u64,
    pub last_blockhash: [u8; HASH_BYTES],
    pub ticks_per_slot: u64,
    pub next_write_version: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HistoricalTransactionOutcome {
    pub signature: Option<[u8; SIGNATURE_BYTES]>,
    pub error: Option<protocol::TransactionError>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HistoricalAccountWrite {
    pub slot: u64,
    pub write_version: u64,
    pub transaction_signature: Option<[u8; SIGNATURE_BYTES]>,
    pub pubkey: [u8; HASH_BYTES],
    pub lamports: u64,
    pub owner: [u8; HASH_BYTES],
    pub executable: bool,
    pub rent_epoch: u64,
    pub data: Vec<u8>,
    pub stored_hash: [u8; HASH_BYTES],
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HistoricalEntryProcessed {
    pub slot: u64,
    pub entry_index: u64,
    pub outcomes: Vec<HistoricalTransactionOutcome>,
    pub writes: Vec<HistoricalAccountWrite>,
    pub tick_height: u64,
    pub slot_complete: bool,
    pub next_write_version: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HistoricalCheckpoint {
    pub slot: u64,
    pub bank_hash: [u8; HASH_BYTES],
    pub accounts_hash: [u8; HASH_BYTES],
    pub last_blockhash: [u8; HASH_BYTES],
    pub capitalization: u64,
    pub transaction_count: u64,
    pub tick_height: u64,
    pub slot_complete: bool,
    pub writes: Vec<HistoricalAccountWrite>,
    pub next_write_version: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HistoricalSnapshotExport {
    pub slot: u64,
    pub archive_path: PathBuf,
    pub accounts_hash: [u8; HASH_BYTES],
    pub archive_size: u64,
    /// Worker-attested SHA-256 independently remeasured by the parent.
    pub archive_sha256: [u8; HASH_BYTES],
}

/// Wall-clock limits for one lockstep worker request.
///
/// Each value can be overridden with the corresponding
/// `JETSTREAMER_HISTORICAL_*_TIMEOUT_SECS` variable. Entry processing also
/// inherits `JETSTREAMER_ENTRY_EXEC_TIMEOUT_SECS` when its historical-specific
/// override is absent, keeping the process boundary aligned with the replay
/// watchdog. Checkpoint hashing has a deliberately independent, much larger
/// budget because it is expected to make no entry-level progress while it
/// scans the accounts state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct HistoricalRuntimeTimeouts {
    control: Duration,
    initialize: Duration,
    entry: Duration,
    checkpoint: Duration,
    reap: Duration,
}

impl HistoricalRuntimeTimeouts {
    fn from_env() -> Result<Self, HistoricalRuntimeError> {
        Ok(Self {
            control: timeout_from_env(CONTROL_TIMEOUT_ENV, None, DEFAULT_CONTROL_TIMEOUT)?,
            initialize: timeout_from_env(INITIALIZE_TIMEOUT_ENV, None, DEFAULT_INITIALIZE_TIMEOUT)?,
            entry: timeout_from_env(
                ENTRY_TIMEOUT_ENV,
                Some(REPLAY_ENTRY_TIMEOUT_ENV),
                DEFAULT_ENTRY_TIMEOUT,
            )?,
            checkpoint: timeout_from_env(CHECKPOINT_TIMEOUT_ENV, None, DEFAULT_CHECKPOINT_TIMEOUT)?,
            reap: timeout_from_env(REAP_TIMEOUT_ENV, None, DEFAULT_REAP_TIMEOUT)?,
        })
    }

    fn request(&self, body: &RequestBody) -> (&'static str, Duration) {
        match body {
            RequestBody::Hello { .. } => ("hello", self.control),
            RequestBody::Initialize { .. } => ("initialize", self.initialize),
            RequestBody::ProcessEntry(_) => ("process-entry", self.entry),
            RequestBody::FreezeCheckpoint { .. } => ("freeze-checkpoint", self.checkpoint),
            RequestBody::ExportSnapshot { .. } => ("export-snapshot", self.checkpoint),
            RequestBody::Ping => ("ping", self.control),
            RequestBody::Shutdown => ("shutdown", self.control),
        }
    }
}

impl Default for HistoricalRuntimeTimeouts {
    fn default() -> Self {
        Self {
            control: DEFAULT_CONTROL_TIMEOUT,
            initialize: DEFAULT_INITIALIZE_TIMEOUT,
            entry: DEFAULT_ENTRY_TIMEOUT,
            checkpoint: DEFAULT_CHECKPOINT_TIMEOUT,
            reap: DEFAULT_REAP_TIMEOUT,
        }
    }
}

struct IpcCall {
    request: Request,
    response_tx: mpsc::SyncSender<io::Result<Option<Response>>>,
}

struct IpcTransport {
    call_tx: mpsc::Sender<IpcCall>,
    io_thread: Option<JoinHandle<()>>,
}

enum IpcExchangeError {
    Io(io::Error),
    Timeout,
    Stopped,
}

impl IpcTransport {
    fn spawn(stdin: ChildStdin, stdout: ChildStdout) -> io::Result<Self> {
        let (call_tx, call_rx) = mpsc::channel::<IpcCall>();
        let io_thread = thread::Builder::new()
            .name("historical-runtime-ipc".to_owned())
            .spawn(move || {
                let mut stdin = BufWriter::new(stdin);
                let mut stdout = BufReader::new(stdout);
                while let Ok(call) = call_rx.recv() {
                    let response = protocol::write_frame(&mut stdin, &call.request)
                        .and_then(|()| protocol::read_frame(&mut stdout));
                    let terminal = response.as_ref().map_or(true, Option::is_none);
                    if call.response_tx.send(response).is_err() || terminal {
                        break;
                    }
                }
            })?;
        Ok(Self {
            call_tx,
            io_thread: Some(io_thread),
        })
    }

    fn exchange(
        &self,
        request: Request,
        timeout: Duration,
    ) -> Result<Option<Response>, IpcExchangeError> {
        let (response_tx, response_rx) = mpsc::sync_channel(1);
        self.call_tx
            .send(IpcCall {
                request,
                response_tx,
            })
            .map_err(|_| IpcExchangeError::Stopped)?;
        match response_rx.recv_timeout(timeout) {
            Ok(Ok(response)) => Ok(response),
            Ok(Err(error)) => Err(IpcExchangeError::Io(error)),
            Err(RecvTimeoutError::Timeout) => Err(IpcExchangeError::Timeout),
            Err(RecvTimeoutError::Disconnected) => Err(IpcExchangeError::Stopped),
        }
    }
}

impl Drop for IpcTransport {
    fn drop(&mut self) {
        // Dropping the final request sender lets an idle I/O thread exit. A
        // thread blocked in pipe I/O is released when the supervised child is
        // killed; detaching here keeps client teardown bounded even if the OS
        // has not delivered that wakeup yet.
        if self.io_thread.as_ref().is_some_and(JoinHandle::is_finished) {
            let _ = self.io_thread.take().expect("checked above").join();
        }
    }
}

/// Sequential client for one isolated historical runtime process.
///
/// The protocol is deliberately lockstep.  At most one request is outstanding,
/// which makes request/response ordering auditable and prevents an unexpected
/// response from being associated with a later entry.
pub struct HistoricalRuntimeClient {
    executable_sha256: [u8; HASH_BYTES],
    child: Option<Child>,
    transport: Option<IpcTransport>,
    private_work_dir: Option<TempDir>,
    timeouts: HistoricalRuntimeTimeouts,
    next_request_id: u64,
    last_response_id: Option<u64>,
    current_slot: u64,
    next_write_version: u64,
    snapshot_export_seal: Option<(u64, [u8; HASH_BYTES])>,
    initialized: HistoricalInitialized,
    closed: bool,
}

impl HistoricalRuntimeClient {
    /// Spawn, identify, and initialize a worker from an exact legacy snapshot.
    pub fn spawn(
        profile: WorkerProfile,
        spawn: WorkerSpawn,
    ) -> Result<Self, HistoricalRuntimeError> {
        Self::spawn_with_timeouts(profile, spawn, HistoricalRuntimeTimeouts::from_env()?)
    }

    fn spawn_with_timeouts(
        profile: WorkerProfile,
        spawn: WorkerSpawn,
        timeouts: HistoricalRuntimeTimeouts,
    ) -> Result<Self, HistoricalRuntimeError> {
        let executable = canonical_regular_file(&spawn.executable, true)?;
        let initialization = &spawn.initialization;
        let ledger_path = canonical_directory(&initialization.ledger_path, "ledger")?;
        let source_archive_path = canonical_regular_file(&initialization.archive_path, false)?;
        if !source_archive_path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.ends_with(".tar.bz2"))
        {
            return Err(HistoricalRuntimeError::NotLegacySnapshot(
                source_archive_path,
            ));
        }

        let private_work_dir = make_private_work_dir(initialization.scratch_parent.as_deref())?;
        // A generated runtime-handoff archive contains replay-relevant bytes
        // (notably status-cache entries) that are not covered by its filename's
        // accounts hash. Copy from one open handle while hashing, then give the
        // worker only the private, digest-bound copy. This closes the path
        // replacement window between parent admission and worker loading.
        let bound_executable = bind_worker_executable(&executable, private_work_dir.path())?;
        let archive_path = match (
            initialization.expected_archive_size,
            initialization.expected_archive_sha256,
        ) {
            (Some(expected_size), Some(expected_sha256)) => bind_snapshot_archive(
                &source_archive_path,
                private_work_dir.path(),
                expected_size,
                expected_sha256,
            )?,
            (None, None) => source_archive_path,
            _ => return Err(HistoricalRuntimeError::IncompleteSnapshotArchiveBinding),
        };
        let ledger_path_string = path_string(&ledger_path)?;
        let archive_path_string = path_string(&archive_path)?;
        let scratch_path_string = path_string(private_work_dir.path())?;

        // The private copy and its digest were produced from the same bounded
        // byte stream. Spawn only that copy so an atomic deployment update to
        // the configured pathname cannot separate provenance from execution.
        let executable_sha256 = bound_executable.sha256;
        let mut child = Command::new(&bound_executable.path)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .map_err(|source| HistoricalRuntimeError::Spawn {
                path: bound_executable.path.clone(),
                source,
            })?;
        let stdin = match child.stdin.take() {
            Some(stdin) => stdin,
            None => {
                kill_and_reap(child, Some(private_work_dir), timeouts.reap);
                return Err(HistoricalRuntimeError::MissingPipe { stream: "stdin" });
            }
        };
        let stdout = match child.stdout.take() {
            Some(stdout) => stdout,
            None => {
                drop(stdin);
                kill_and_reap(child, Some(private_work_dir), timeouts.reap);
                return Err(HistoricalRuntimeError::MissingPipe { stream: "stdout" });
            }
        };
        let transport = match IpcTransport::spawn(stdin, stdout) {
            Ok(transport) => transport,
            Err(error) => {
                kill_and_reap(child, Some(private_work_dir), timeouts.reap);
                return Err(HistoricalRuntimeError::SpawnIpcThread(error));
            }
        };

        // Install a placeholder until Initialize succeeds.  Any failure below
        // drops the client, which kills and reaps the partially started child.
        let mut client = Self {
            executable_sha256,
            child: Some(child),
            transport: Some(transport),
            private_work_dir: Some(private_work_dir),
            timeouts,
            next_request_id: 1,
            last_response_id: None,
            current_slot: 0,
            next_write_version: 0,
            snapshot_export_seal: None,
            initialized: HistoricalInitialized {
                genesis_hash: String::new(),
                slot: 0,
                last_blockhash: [0; HASH_BYTES],
                ticks_per_slot: 0,
                next_write_version: 0,
            },
            closed: false,
        };

        let body = client.exchange(RequestBody::Hello {
            protocol_version: protocol::PROTOCOL_VERSION,
        })?;
        let handshake = match body {
            ResponseBody::Handshake(handshake) => handshake,
            other => {
                return client.abort_after_response(HistoricalRuntimeError::UnexpectedResponse {
                    expected: "Handshake",
                    actual: response_kind(&other),
                });
            }
        };
        if let Err(error) = verify_handshake(profile, &handshake) {
            return client.abort_after_response(error);
        }

        let body = client.exchange(RequestBody::Initialize {
            ledger_path: ledger_path_string,
            initial_state: InitialState::SnapshotArchive {
                archive_path: archive_path_string.clone(),
            },
            scratch_root: Some(scratch_path_string),
        })?;
        let initialized = match body {
            ResponseBody::Initialized(initialized) => initialized,
            other => {
                return client.abort_after_response(HistoricalRuntimeError::UnexpectedResponse {
                    expected: "Initialized",
                    actual: response_kind(&other),
                });
            }
        };
        let initialized = validate_initialized(
            profile,
            initialized,
            &archive_path_string,
            initialization.expected_slot,
            initialization.expected_accounts_hash,
        );
        client.initialized = match initialized {
            Ok(initialized) => initialized,
            Err(error) => return client.abort_after_response(error),
        };
        client.current_slot = client.initialized.slot;
        client.next_write_version = client.initialized.next_write_version;
        Ok(client)
    }

    /// Parent-measured SHA-256 of the canonical executable passed to
    /// [`Command::spawn`].
    pub fn executable_sha256(&self) -> [u8; HASH_BYTES] {
        self.executable_sha256
    }

    pub fn initialized(&self) -> &HistoricalInitialized {
        &self.initialized
    }

    /// Process an entry whose transaction payloads are already canonical
    /// bincode encodings of legacy Solana `Transaction` values.
    ///
    /// This neutral API is the integration boundary used when the caller owns
    /// transaction conversion.  `process_versioned_entry` is the checked
    /// convenience adapter for current Agave transaction objects.
    pub fn process_entry(
        &mut self,
        slot: u64,
        entry_index: u64,
        num_hashes: u64,
        hash: [u8; HASH_BYTES],
        transactions: Vec<Vec<u8>>,
    ) -> Result<HistoricalEntryProcessed, HistoricalRuntimeError> {
        if slot < self.current_slot {
            return Err(HistoricalRuntimeError::RequestedSlotRegression {
                current: self.current_slot,
                requested: slot,
            });
        }
        let previous_slot = self.current_slot;
        validate_encoded_transactions(&transactions)?;
        let transaction_count = transactions.len();
        let body = self.exchange(RequestBody::ProcessEntry(protocol::EntryRequest {
            slot,
            entry_index,
            num_hashes,
            hash: hash.to_vec(),
            transactions,
        }))?;
        let processed = match body {
            ResponseBody::EntryProcessed(processed) => processed,
            other => {
                return self.abort_after_response(HistoricalRuntimeError::UnexpectedResponse {
                    expected: "EntryProcessed",
                    actual: response_kind(&other),
                });
            }
        };
        let processed = self.validate_processed_entry(
            previous_slot,
            slot,
            entry_index,
            transaction_count,
            processed,
        );
        let processed = match processed {
            Ok(processed) => processed,
            Err(error) => return self.abort_after_response(error),
        };
        self.current_slot = slot;
        Ok(processed)
    }

    pub fn freeze_checkpoint(
        &mut self,
        slot: u64,
    ) -> Result<HistoricalCheckpoint, HistoricalRuntimeError> {
        let body = self.exchange(RequestBody::FreezeCheckpoint { slot })?;
        let checkpoint = match body {
            ResponseBody::Checkpoint(checkpoint) => checkpoint,
            other => {
                return self.abort_after_response(HistoricalRuntimeError::UnexpectedResponse {
                    expected: "Checkpoint",
                    actual: response_kind(&other),
                });
            }
        };
        if checkpoint.slot != slot {
            return self.abort_after_response(HistoricalRuntimeError::CheckpointSlotMismatch {
                expected: slot,
                actual: checkpoint.slot,
            });
        }
        if !checkpoint.slot_complete {
            return self
                .abort_after_response(HistoricalRuntimeError::IncompleteCheckpoint { slot });
        }
        let validated = (|| {
            let writes = checkpoint
                .writes
                .into_iter()
                .map(|write| validate_account_write(write, slot))
                .collect::<Result<Vec<_>, _>>()?;
            let validated = HistoricalCheckpoint {
                slot,
                bank_hash: array_32("checkpoint.bank_hash", checkpoint.bank_hash)?,
                accounts_hash: array_32("checkpoint.accounts_hash", checkpoint.accounts_hash)?,
                last_blockhash: array_32("checkpoint.last_blockhash", checkpoint.last_blockhash)?,
                capitalization: checkpoint.capitalization,
                transaction_count: checkpoint.transaction_count,
                tick_height: checkpoint.tick_height,
                slot_complete: checkpoint.slot_complete,
                writes,
                next_write_version: checkpoint.next_write_version,
            };
            self.validate_write_stream(&validated.writes, validated.next_write_version)?;
            Ok(validated)
        })();
        let validated = match validated {
            Ok(validated) => validated,
            Err(error) => return self.abort_after_response(error),
        };
        self.snapshot_export_seal = Some((slot, validated.accounts_hash));
        Ok(validated)
    }

    /// Publish a canonical v1.0.7 snapshot from the immediately preceding
    /// successful checkpoint. The one-use gate is also enforced by the worker.
    pub fn export_snapshot(
        &mut self,
        slot: u64,
        output_directory: &Path,
        expected_accounts_hash: [u8; HASH_BYTES],
    ) -> Result<HistoricalSnapshotExport, HistoricalRuntimeError> {
        let output_directory = canonical_directory(output_directory, "snapshot-output")?;
        let output_directory_string = path_string(&output_directory)?;
        let (checkpoint_slot, checkpoint_accounts_hash) = self
            .snapshot_export_seal
            .take()
            .ok_or(HistoricalRuntimeError::SnapshotExportWithoutCheckpoint)?;
        if checkpoint_slot != slot {
            return Err(
                HistoricalRuntimeError::SnapshotExportCheckpointSlotMismatch {
                    checkpoint: checkpoint_slot,
                    requested: slot,
                },
            );
        }
        if checkpoint_accounts_hash != expected_accounts_hash {
            return Err(HistoricalRuntimeError::SnapshotExportCheckpointHashMismatch);
        }

        let body = self.exchange(RequestBody::ExportSnapshot {
            slot,
            output_directory: output_directory_string,
            expected_accounts_hash: expected_accounts_hash.to_vec(),
        })?;
        let exported = match body {
            ResponseBody::SnapshotExported(exported) => exported,
            other => {
                return self.abort_after_response(HistoricalRuntimeError::UnexpectedResponse {
                    expected: "SnapshotExported",
                    actual: response_kind(&other),
                });
            }
        };
        match validate_snapshot_export(&output_directory, slot, expected_accounts_hash, exported) {
            Ok(exported) => Ok(exported),
            Err(error) => self.abort_after_response(error),
        }
    }

    #[cfg(test)]
    fn ping(&mut self) -> Result<(), HistoricalRuntimeError> {
        match self.exchange(RequestBody::Ping)? {
            ResponseBody::Pong => Ok(()),
            other => self.abort_after_response(HistoricalRuntimeError::UnexpectedResponse {
                expected: "Pong",
                actual: response_kind(&other),
            }),
        }
    }

    /// Ask the worker to shut down, close its pipes, and reap it.
    pub fn shutdown(&mut self) -> Result<(), HistoricalRuntimeError> {
        if self.closed {
            return Ok(());
        }
        match self.exchange(RequestBody::Shutdown)? {
            ResponseBody::ShuttingDown => {}
            other => {
                return self.abort_after_response(HistoricalRuntimeError::UnexpectedResponse {
                    expected: "ShuttingDown",
                    actual: response_kind(&other),
                });
            }
        }
        // Disconnect the idle I/O thread before waiting. The worker already
        // acknowledged Shutdown, so no further protocol traffic is valid.
        self.transport.take();
        let mut child = self.child.take().ok_or(HistoricalRuntimeError::Closed)?;
        let status = match wait_for_child(&mut child, self.timeouts.reap) {
            Ok(Some(status)) => status,
            Ok(None) => {
                kill_and_reap(child, self.private_work_dir.take(), self.timeouts.reap);
                self.closed = true;
                return Err(HistoricalRuntimeError::ShutdownExitTimeout {
                    timeout: self.timeouts.reap,
                });
            }
            Err(error) => {
                kill_and_reap(child, self.private_work_dir.take(), self.timeouts.reap);
                self.closed = true;
                return Err(HistoricalRuntimeError::Io(error));
            }
        };
        self.closed = true;
        self.private_work_dir.take();
        if status.success() {
            Ok(())
        } else {
            Err(HistoricalRuntimeError::UnsuccessfulExit(status))
        }
    }

    fn exchange(&mut self, body: RequestBody) -> Result<ResponseBody, HistoricalRuntimeError> {
        if self.closed {
            return Err(HistoricalRuntimeError::Closed);
        }
        if !matches!(&body, RequestBody::ExportSnapshot { .. }) {
            self.snapshot_export_seal = None;
        }
        let request_id = self.next_request_id;
        self.next_request_id = request_id
            .checked_add(1)
            .ok_or(HistoricalRuntimeError::RequestIdOverflow)?;
        let (operation, timeout) = self.timeouts.request(&body);
        let result = self
            .transport
            .as_ref()
            .ok_or(HistoricalRuntimeError::Closed)?
            .exchange(
                Request {
                    id: request_id,
                    body,
                },
                timeout,
            );
        let response = match result {
            Ok(Some(response)) => response,
            Ok(None) => {
                self.abort_worker();
                return Err(HistoricalRuntimeError::UnexpectedEof { request_id });
            }
            Err(IpcExchangeError::Io(error)) => {
                self.abort_worker();
                return Err(HistoricalRuntimeError::Io(error));
            }
            Err(IpcExchangeError::Timeout) => {
                self.abort_worker();
                return Err(HistoricalRuntimeError::RequestTimeout {
                    operation,
                    request_id,
                    timeout,
                });
            }
            Err(IpcExchangeError::Stopped) => {
                self.abort_worker();
                return Err(HistoricalRuntimeError::IpcStopped {
                    operation,
                    request_id,
                });
            }
        };
        let response_id =
            validate_response_id(self.last_response_id, request_id, response.request_id);
        self.last_response_id = Some(match response_id {
            Ok(response_id) => response_id,
            Err(error) => return self.abort_after_response(error),
        });
        match response.body {
            // A worker may discover an error only after advancing, freezing,
            // or otherwise mutating its private runtime state. The neutral
            // protocol cannot prove that an Error response was pre-mutation,
            // so every worker error invalidates this session.
            ResponseBody::Error(error) => {
                self.abort_after_response(HistoricalRuntimeError::Worker {
                    code: error.code,
                    message: error.message,
                })
            }
            body => Ok(body),
        }
    }

    fn abort_worker(&mut self) {
        if self.closed {
            return;
        }
        self.closed = true;
        self.transport.take();
        if let Some(child) = self.child.take() {
            kill_and_reap(child, self.private_work_dir.take(), self.timeouts.reap);
        } else {
            self.private_work_dir.take();
        }
    }

    /// A response-validation failure leaves the child's state unknowable. It
    /// must never be allowed to answer another request on the same session.
    fn abort_after_response<T>(
        &mut self,
        error: HistoricalRuntimeError,
    ) -> Result<T, HistoricalRuntimeError> {
        self.abort_worker();
        Err(error)
    }

    fn validate_processed_entry(
        &mut self,
        previous_slot: u64,
        expected_slot: u64,
        expected_index: u64,
        transaction_count: usize,
        processed: protocol::EntryProcessed,
    ) -> Result<HistoricalEntryProcessed, HistoricalRuntimeError> {
        if processed.slot != expected_slot || processed.entry_index != expected_index {
            return Err(HistoricalRuntimeError::EntryIdentityMismatch {
                expected_slot,
                expected_index,
                actual_slot: processed.slot,
                actual_index: processed.entry_index,
            });
        }
        if processed.outcomes.len() != transaction_count {
            return Err(HistoricalRuntimeError::OutcomeCountMismatch {
                expected: transaction_count,
                actual: processed.outcomes.len(),
            });
        }
        let outcomes = processed
            .outcomes
            .into_iter()
            .map(|outcome| {
                Ok(HistoricalTransactionOutcome {
                    signature: outcome
                        .signature
                        .map(|bytes| array_64("transaction_outcome.signature", bytes))
                        .transpose()?,
                    error: outcome.error,
                })
            })
            .collect::<Result<Vec<_>, HistoricalRuntimeError>>()?;
        let writes = validate_entry_writes(processed.writes, previous_slot, expected_slot)?;
        let validated = HistoricalEntryProcessed {
            slot: processed.slot,
            entry_index: processed.entry_index,
            outcomes,
            writes,
            tick_height: processed.tick_height,
            slot_complete: processed.slot_complete,
            next_write_version: processed.next_write_version,
        };
        self.validate_write_stream(&validated.writes, validated.next_write_version)?;
        Ok(validated)
    }

    fn validate_write_stream(
        &mut self,
        writes: &[HistoricalAccountWrite],
        reported_next: u64,
    ) -> Result<(), HistoricalRuntimeError> {
        self.next_write_version = validate_write_versions(
            self.next_write_version,
            writes.iter().map(|write| write.write_version),
            reported_next,
        )?;
        Ok(())
    }
}

impl Drop for HistoricalRuntimeClient {
    fn drop(&mut self) {
        // Never perform protocol I/O from Drop: the other end may be wedged.
        // Pipe I/O runs on a disposable helper thread, while abort_worker
        // kills the process and gives reaping a bounded synchronous window.
        // In the pathological case where the OS does not report the exit in
        // that window, a detached reaper retains both Child and TempDir.
        self.abort_worker();
    }
}

/// Encode a current legacy transaction as the non-versioned `Transaction`
/// representation expected by Solana v1.0.24.
///
/// It is important that this does not serialize `VersionedTransaction`
/// directly: doing so would couple the old worker to the current enum's serde
/// representation.  The explicit conversion removes that enum from the wire.
pub fn encode_legacy_transaction(
    transaction: &VersionedTransaction,
) -> Result<Vec<u8>, HistoricalRuntimeError> {
    let legacy = transaction
        .clone()
        .into_legacy_transaction()
        .ok_or(HistoricalRuntimeError::VersionedTransactionUnsupported)?;
    let encoded =
        bincode::serialize(&legacy).map_err(HistoricalRuntimeError::TransactionSerialization)?;
    if encoded.len() > protocol::MAX_FRAME_BYTES {
        return Err(HistoricalRuntimeError::EncodedTransactionTooLarge {
            index: 0,
            bytes: encoded.len(),
        });
    }
    Ok(encoded)
}

/// Normalize a current transaction status into exactly the v1.0.24 status
/// vocabulary.  Newer-only statuses fail closed instead of being conflated.
pub fn normalize_transaction_error(
    error: &CurrentTransactionError,
) -> Result<protocol::TransactionError, HistoricalRuntimeError> {
    use CurrentTransactionError as Current;
    use protocol::TransactionError as Old;

    Ok(match error {
        Current::AccountInUse => Old::AccountInUse,
        Current::AccountLoadedTwice => Old::AccountLoadedTwice,
        Current::AccountNotFound => Old::AccountNotFound,
        Current::ProgramAccountNotFound => Old::ProgramAccountNotFound,
        Current::InsufficientFundsForFee => Old::InsufficientFundsForFee,
        Current::InvalidAccountForFee => Old::InvalidAccountForFee,
        Current::AlreadyProcessed => Old::DuplicateSignature,
        Current::BlockhashNotFound => Old::BlockhashNotFound,
        Current::InstructionError(instruction_index, instruction_error) => Old::InstructionError {
            instruction_index: *instruction_index,
            error: normalize_instruction_error(instruction_error)?,
        },
        Current::CallChainTooDeep => Old::CallChainTooDeep,
        Current::MissingSignatureForFee => Old::MissingSignatureForFee,
        Current::InvalidAccountIndex => Old::InvalidAccountIndex,
        Current::SignatureFailure => Old::SignatureFailure,
        Current::InvalidProgramForExecution => Old::InvalidProgramForExecution,
        Current::SanitizeFailure => Old::SanitizeFailure,
        Current::ClusterMaintenance
        | Current::AccountBorrowOutstanding
        | Current::WouldExceedMaxBlockCostLimit
        | Current::UnsupportedVersion
        | Current::InvalidWritableAccount
        | Current::WouldExceedMaxAccountCostLimit
        | Current::WouldExceedAccountDataBlockLimit
        | Current::TooManyAccountLocks
        | Current::AddressLookupTableNotFound
        | Current::InvalidAddressLookupTableOwner
        | Current::InvalidAddressLookupTableData
        | Current::InvalidAddressLookupTableIndex
        | Current::InvalidRentPayingAccount
        | Current::WouldExceedMaxVoteCostLimit
        | Current::WouldExceedAccountDataTotalLimit
        | Current::DuplicateInstruction(_)
        | Current::InsufficientFundsForRent { .. }
        | Current::MaxLoadedAccountsDataSizeExceeded
        | Current::InvalidLoadedAccountsDataSizeLimit
        | Current::ResanitizationNeeded
        | Current::ProgramExecutionTemporarilyRestricted { .. }
        | Current::UnbalancedTransaction
        | Current::ProgramCacheHitMaxLimit
        | Current::CommitCancelled => {
            return Err(HistoricalRuntimeError::UnsupportedTransactionError(
                error.clone(),
            ));
        }
    })
}

/// Normalize a current instruction status into exactly the v1.0.24 status
/// vocabulary.  `Custom` is preserved as the old custom-error value.
#[allow(deprecated)]
pub fn normalize_instruction_error(
    error: &CurrentInstructionError,
) -> Result<protocol::InstructionError, HistoricalRuntimeError> {
    use CurrentInstructionError as Current;
    use protocol::InstructionError as Old;

    Ok(match error {
        Current::GenericError => Old::GenericError,
        Current::InvalidArgument => Old::InvalidArgument,
        Current::InvalidInstructionData => Old::InvalidInstructionData,
        Current::InvalidAccountData => Old::InvalidAccountData,
        Current::AccountDataTooSmall => Old::AccountDataTooSmall,
        Current::InsufficientFunds => Old::InsufficientFunds,
        Current::IncorrectProgramId => Old::IncorrectProgramId,
        Current::MissingRequiredSignature => Old::MissingRequiredSignature,
        Current::AccountAlreadyInitialized => Old::AccountAlreadyInitialized,
        Current::UninitializedAccount => Old::UninitializedAccount,
        Current::UnbalancedInstruction => Old::UnbalancedInstruction,
        Current::ModifiedProgramId => Old::ModifiedProgramId,
        Current::ExternalAccountLamportSpend => Old::ExternalAccountLamportSpend,
        Current::ExternalAccountDataModified => Old::ExternalAccountDataModified,
        Current::ReadonlyLamportChange => Old::ReadonlyLamportChange,
        Current::ReadonlyDataModified => Old::ReadonlyDataModified,
        Current::DuplicateAccountIndex => Old::DuplicateAccountIndex,
        Current::ExecutableModified => Old::ExecutableModified,
        Current::RentEpochModified => Old::RentEpochModified,
        Current::NotEnoughAccountKeys => Old::NotEnoughAccountKeys,
        Current::AccountDataSizeChanged => Old::AccountDataSizeChanged,
        Current::AccountNotExecutable => Old::AccountNotExecutable,
        Current::AccountBorrowFailed => Old::AccountBorrowFailed,
        Current::AccountBorrowOutstanding => Old::AccountBorrowOutstanding,
        Current::DuplicateAccountOutOfSync => Old::DuplicateAccountOutOfSync,
        Current::Custom(code) => Old::Custom(*code),
        Current::InvalidError => Old::InvalidError,
        Current::ExecutableDataModified
        | Current::ExecutableLamportChange
        | Current::ExecutableAccountNotRentExempt
        | Current::UnsupportedProgramId
        | Current::CallDepth
        | Current::MissingAccount
        | Current::ReentrancyNotAllowed
        | Current::MaxSeedLengthExceeded
        | Current::InvalidSeeds
        | Current::InvalidRealloc
        | Current::ComputationalBudgetExceeded
        | Current::PrivilegeEscalation
        | Current::ProgramEnvironmentSetupFailure
        | Current::ProgramFailedToComplete
        | Current::ProgramFailedToCompile
        | Current::Immutable
        | Current::IncorrectAuthority
        | Current::BorshIoError
        | Current::AccountNotRentExempt
        | Current::InvalidAccountOwner
        | Current::ArithmeticOverflow
        | Current::UnsupportedSysvar
        | Current::IllegalOwner
        | Current::MaxAccountsDataAllocationsExceeded
        | Current::MaxAccountsExceeded
        | Current::MaxInstructionTraceLengthExceeded
        | Current::BuiltinProgramsMustConsumeComputeUnits => {
            return Err(HistoricalRuntimeError::UnsupportedInstructionError(
                error.clone(),
            ));
        }
    })
}

/// Convert an exact v1.0.24 transaction status back into the current status
/// type used by Horizon metadata. The historical vocabulary is a strict
/// subset, so this direction is infallible.
pub fn denormalize_transaction_error(
    error: &protocol::TransactionError,
) -> CurrentTransactionError {
    use CurrentTransactionError as Current;
    use protocol::TransactionError as Old;

    match error {
        Old::AccountInUse => Current::AccountInUse,
        Old::AccountLoadedTwice => Current::AccountLoadedTwice,
        Old::AccountNotFound => Current::AccountNotFound,
        Old::ProgramAccountNotFound => Current::ProgramAccountNotFound,
        Old::InsufficientFundsForFee => Current::InsufficientFundsForFee,
        Old::InvalidAccountForFee => Current::InvalidAccountForFee,
        Old::DuplicateSignature => Current::AlreadyProcessed,
        Old::BlockhashNotFound => Current::BlockhashNotFound,
        Old::InstructionError {
            instruction_index,
            error,
        } => Current::InstructionError(*instruction_index, denormalize_instruction_error(error)),
        Old::CallChainTooDeep => Current::CallChainTooDeep,
        Old::MissingSignatureForFee => Current::MissingSignatureForFee,
        Old::InvalidAccountIndex => Current::InvalidAccountIndex,
        Old::SignatureFailure => Current::SignatureFailure,
        Old::InvalidProgramForExecution => Current::InvalidProgramForExecution,
        Old::SanitizeFailure => Current::SanitizeFailure,
    }
}

/// Convert an exact v1.0.24 instruction status into the current superset.
#[allow(deprecated)]
pub fn denormalize_instruction_error(
    error: &protocol::InstructionError,
) -> CurrentInstructionError {
    use CurrentInstructionError as Current;
    use protocol::InstructionError as Old;

    match error {
        Old::GenericError => Current::GenericError,
        Old::InvalidArgument => Current::InvalidArgument,
        Old::InvalidInstructionData => Current::InvalidInstructionData,
        Old::InvalidAccountData => Current::InvalidAccountData,
        Old::AccountDataTooSmall => Current::AccountDataTooSmall,
        Old::InsufficientFunds => Current::InsufficientFunds,
        Old::IncorrectProgramId => Current::IncorrectProgramId,
        Old::MissingRequiredSignature => Current::MissingRequiredSignature,
        Old::AccountAlreadyInitialized => Current::AccountAlreadyInitialized,
        Old::UninitializedAccount => Current::UninitializedAccount,
        Old::UnbalancedInstruction => Current::UnbalancedInstruction,
        Old::ModifiedProgramId => Current::ModifiedProgramId,
        Old::ExternalAccountLamportSpend => Current::ExternalAccountLamportSpend,
        Old::ExternalAccountDataModified => Current::ExternalAccountDataModified,
        Old::ReadonlyLamportChange => Current::ReadonlyLamportChange,
        Old::ReadonlyDataModified => Current::ReadonlyDataModified,
        Old::DuplicateAccountIndex => Current::DuplicateAccountIndex,
        Old::ExecutableModified => Current::ExecutableModified,
        Old::RentEpochModified => Current::RentEpochModified,
        Old::NotEnoughAccountKeys => Current::NotEnoughAccountKeys,
        Old::AccountDataSizeChanged => Current::AccountDataSizeChanged,
        Old::AccountNotExecutable => Current::AccountNotExecutable,
        Old::AccountBorrowFailed => Current::AccountBorrowFailed,
        Old::AccountBorrowOutstanding => Current::AccountBorrowOutstanding,
        Old::DuplicateAccountOutOfSync => Current::DuplicateAccountOutOfSync,
        Old::Custom(code) => Current::Custom(*code),
        Old::InvalidError => Current::InvalidError,
    }
}

fn canonical_regular_file(
    path: &Path,
    executable: bool,
) -> Result<PathBuf, HistoricalRuntimeError> {
    if executable && !path.is_absolute() {
        return Err(HistoricalRuntimeError::ExecutableNotAbsolute(
            path.to_path_buf(),
        ));
    }
    let canonical = fs::canonicalize(path).map_err(|source| HistoricalRuntimeError::PathIo {
        path: path.to_path_buf(),
        source,
    })?;
    let _ = open_regular_file_nofollow(&canonical, executable)?;
    Ok(canonical)
}

#[cfg(test)]
fn sha256_file(path: &Path) -> Result<[u8; HASH_BYTES], HistoricalRuntimeError> {
    let (mut file, metadata) = open_regular_file_nofollow(path, false)?;
    let hash = sha256_open_file_exact(&mut file, path, metadata.len())?;
    ensure_path_names_open_file(path, &metadata)?;
    Ok(hash)
}

fn sha256_open_file_exact(
    file: &mut fs::File,
    path: &Path,
    expected_size: u64,
) -> Result<[u8; HASH_BYTES], HistoricalRuntimeError> {
    let mut hash = Sha256::new();
    let mut buffer = [0u8; 64 * 1024];
    let mut remaining = expected_size;
    while remaining != 0 {
        let maximum = std::cmp::min(remaining, buffer.len() as u64) as usize;
        let read =
            file.read(&mut buffer[..maximum])
                .map_err(|source| HistoricalRuntimeError::PathIo {
                    path: path.to_path_buf(),
                    source,
                })?;
        if read == 0 {
            return Err(HistoricalRuntimeError::SnapshotArchiveSizeMismatch {
                expected: expected_size,
                actual: expected_size - remaining,
            });
        }
        hash.update(&buffer[..read]);
        remaining -= read as u64;
    }
    let mut extra = [0u8; 1];
    if file
        .read(&mut extra)
        .map_err(|source| HistoricalRuntimeError::PathIo {
            path: path.to_path_buf(),
            source,
        })?
        != 0
    {
        let actual = file
            .metadata()
            .map(|metadata| metadata.len())
            .unwrap_or(expected_size.saturating_add(1))
            .max(expected_size.saturating_add(1));
        return Err(HistoricalRuntimeError::SnapshotArchiveSizeMismatch {
            expected: expected_size,
            actual,
        });
    }
    Ok(hash.finalize().into())
}

fn open_readonly_nofollow(path: &Path) -> io::Result<fs::File> {
    let mut options = fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;

        options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    options.open(path)
}

fn open_regular_file_nofollow(
    path: &Path,
    executable: bool,
) -> Result<(fs::File, fs::Metadata), HistoricalRuntimeError> {
    let file = open_readonly_nofollow(path).map_err(|source| HistoricalRuntimeError::PathIo {
        path: path.to_path_buf(),
        source,
    })?;
    let metadata = file
        .metadata()
        .map_err(|source| HistoricalRuntimeError::PathIo {
            path: path.to_path_buf(),
            source,
        })?;
    if !metadata.is_file() {
        return if executable {
            Err(HistoricalRuntimeError::ExecutableNotFile(
                path.to_path_buf(),
            ))
        } else {
            Err(HistoricalRuntimeError::SnapshotNotFile(path.to_path_buf()))
        };
    }
    ensure_path_names_open_file(path, &metadata)?;
    Ok((file, metadata))
}

fn ensure_path_names_open_file(
    path: &Path,
    opened: &fs::Metadata,
) -> Result<(), HistoricalRuntimeError> {
    let current = fs::symlink_metadata(path).map_err(|source| HistoricalRuntimeError::PathIo {
        path: path.to_path_buf(),
        source,
    })?;
    if !current.file_type().is_file() || !same_file_identity(opened, &current) {
        return Err(HistoricalRuntimeError::PathIdentityChanged(
            path.to_path_buf(),
        ));
    }
    Ok(())
}

#[cfg(unix)]
fn same_file_identity(left: &fs::Metadata, right: &fs::Metadata) -> bool {
    use std::os::unix::fs::MetadataExt as _;

    left.dev() == right.dev() && left.ino() == right.ino()
}

#[cfg(not(unix))]
fn same_file_identity(left: &fs::Metadata, right: &fs::Metadata) -> bool {
    left.len() == right.len()
}

struct BoundWorkerExecutable {
    path: PathBuf,
    sha256: [u8; HASH_BYTES],
}

/// Open one worker inode and stream exactly its admitted size into the hash and
/// optional private copy. Hashing and copying the same chunks makes the result
/// self-consistent even if the configured pathname is atomically replaced.
fn stream_worker_executable(
    path: &Path,
    mut destination: Option<&mut fs::File>,
) -> Result<([u8; HASH_BYTES], fs::Permissions), HistoricalRuntimeError> {
    let (mut source, metadata) = open_regular_file_nofollow(path, true)?;
    let expected_size = metadata.len();
    if expected_size == 0 {
        return Err(HistoricalRuntimeError::ExecutableEmpty(path.to_path_buf()));
    }
    if expected_size > MAX_HISTORICAL_WORKER_EXECUTABLE_BYTES {
        return Err(HistoricalRuntimeError::ExecutableTooLarge {
            path: path.to_path_buf(),
            bytes: expected_size,
            limit: MAX_HISTORICAL_WORKER_EXECUTABLE_BYTES,
        });
    }

    let mut hasher = Sha256::new();
    let mut buffer = [0u8; 128 * 1024];
    let mut remaining = expected_size;
    while remaining != 0 {
        let maximum = std::cmp::min(remaining, buffer.len() as u64) as usize;
        let read = source.read(&mut buffer[..maximum]).map_err(|source| {
            HistoricalRuntimeError::PathIo {
                path: path.to_path_buf(),
                source,
            }
        })?;
        if read == 0 {
            return Err(HistoricalRuntimeError::ExecutableSizeChanged {
                path: path.to_path_buf(),
                expected: expected_size,
                actual: expected_size - remaining,
            });
        }
        hasher.update(&buffer[..read]);
        if let Some(output) = destination.as_deref_mut() {
            output
                .write_all(&buffer[..read])
                .map_err(|source| HistoricalRuntimeError::PathIo {
                    path: path.to_path_buf(),
                    source,
                })?;
        }
        remaining -= read as u64;
    }
    let mut extra = [0u8; 1];
    if source
        .read(&mut extra)
        .map_err(|source| HistoricalRuntimeError::PathIo {
            path: path.to_path_buf(),
            source,
        })?
        != 0
    {
        let actual = source
            .metadata()
            .map(|metadata| metadata.len())
            .unwrap_or(expected_size.saturating_add(1))
            .max(expected_size.saturating_add(1));
        return Err(HistoricalRuntimeError::ExecutableSizeChanged {
            path: path.to_path_buf(),
            expected: expected_size,
            actual,
        });
    }

    ensure_path_names_open_file(path, &metadata)?;
    Ok((hasher.finalize().into(), metadata.permissions()))
}

fn bind_worker_executable(
    source_path: &Path,
    private_work_dir: &Path,
) -> Result<BoundWorkerExecutable, HistoricalRuntimeError> {
    let bound_directory = private_work_dir.join("bound-worker");
    fs::create_dir(&bound_directory).map_err(|source| HistoricalRuntimeError::PathIo {
        path: bound_directory.clone(),
        source,
    })?;
    let bound_path = bound_directory.join("historical-worker");
    let mut bound_file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&bound_path)
        .map_err(|source| HistoricalRuntimeError::PathIo {
            path: bound_path.clone(),
            source,
        })?;
    let (sha256, source_permissions) =
        stream_worker_executable(source_path, Some(&mut bound_file))?;
    bound_file
        .flush()
        .and_then(|()| bound_file.set_permissions(source_permissions))
        .and_then(|()| bound_file.sync_all())
        .map_err(|source| HistoricalRuntimeError::PathIo {
            path: bound_path.clone(),
            source,
        })?;
    fs::File::open(&bound_directory)
        .and_then(|directory| directory.sync_all())
        .map_err(|source| HistoricalRuntimeError::PathIo {
            path: bound_directory,
            source,
        })?;
    Ok(BoundWorkerExecutable {
        path: bound_path,
        sha256,
    })
}

fn bind_snapshot_archive(
    source_path: &Path,
    private_work_dir: &Path,
    expected_size: u64,
    expected_sha256: [u8; HASH_BYTES],
) -> Result<PathBuf, HistoricalRuntimeError> {
    let filename = source_path
        .file_name()
        .ok_or_else(|| HistoricalRuntimeError::SnapshotNotFile(source_path.to_path_buf()))?;
    let bound_directory = private_work_dir.join("bound-bootstrap");
    fs::create_dir(&bound_directory).map_err(|source| HistoricalRuntimeError::PathIo {
        path: bound_directory.clone(),
        source,
    })?;
    let bound_path = bound_directory.join(filename);
    let (mut source_file, source_metadata) = open_regular_file_nofollow(source_path, false)?;
    if expected_size == 0 || source_metadata.len() != expected_size {
        return Err(HistoricalRuntimeError::SnapshotArchiveSizeMismatch {
            expected: expected_size,
            actual: source_metadata.len(),
        });
    }
    let mut bound_file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&bound_path)
        .map_err(|source| HistoricalRuntimeError::PathIo {
            path: bound_path.clone(),
            source,
        })?;
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; 128 * 1024];
    let mut remaining = expected_size;
    while remaining != 0 {
        let maximum = std::cmp::min(remaining, buffer.len() as u64) as usize;
        let read = source_file.read(&mut buffer[..maximum]).map_err(|source| {
            HistoricalRuntimeError::PathIo {
                path: source_path.to_path_buf(),
                source,
            }
        })?;
        if read == 0 {
            return Err(HistoricalRuntimeError::SnapshotArchiveSizeMismatch {
                expected: expected_size,
                actual: expected_size - remaining,
            });
        }
        hasher.update(&buffer[..read]);
        bound_file
            .write_all(&buffer[..read])
            .map_err(|source| HistoricalRuntimeError::PathIo {
                path: bound_path.clone(),
                source,
            })?;
        remaining -= read as u64;
    }
    let mut extra = [0u8; 1];
    if source_file
        .read(&mut extra)
        .map_err(|source| HistoricalRuntimeError::PathIo {
            path: source_path.to_path_buf(),
            source,
        })?
        != 0
    {
        let actual = source_file
            .metadata()
            .map(|metadata| metadata.len())
            .unwrap_or(expected_size.saturating_add(1))
            .max(expected_size.saturating_add(1));
        return Err(HistoricalRuntimeError::SnapshotArchiveSizeMismatch {
            expected: expected_size,
            actual,
        });
    }
    bound_file
        .flush()
        .and_then(|()| bound_file.sync_all())
        .map_err(|source| HistoricalRuntimeError::PathIo {
            path: bound_path.clone(),
            source,
        })?;
    let actual_sha256: [u8; HASH_BYTES] = hasher.finalize().into();
    if actual_sha256 != expected_sha256 {
        return Err(HistoricalRuntimeError::SnapshotArchiveDigestMismatch {
            expected: digest_hex(&expected_sha256),
            actual: digest_hex(&actual_sha256),
        });
    }
    ensure_path_names_open_file(source_path, &source_metadata)?;
    fs::File::open(&bound_directory)
        .and_then(|directory| directory.sync_all())
        .map_err(|source| HistoricalRuntimeError::PathIo {
            path: bound_directory,
            source,
        })?;
    Ok(bound_path)
}

fn digest_hex(digest: &[u8; HASH_BYTES]) -> String {
    use std::fmt::Write as _;

    let mut output = String::with_capacity(HASH_BYTES * 2);
    for byte in digest {
        write!(&mut output, "{byte:02x}").expect("writing to a String cannot fail");
    }
    output
}

fn validate_snapshot_export(
    output_directory: &Path,
    expected_slot: u64,
    expected_accounts_hash: [u8; HASH_BYTES],
    exported: protocol::SnapshotExport,
) -> Result<HistoricalSnapshotExport, HistoricalRuntimeError> {
    if exported.slot != expected_slot {
        return Err(HistoricalRuntimeError::SnapshotExportSlotMismatch {
            expected: expected_slot,
            actual: exported.slot,
        });
    }
    let accounts_hash = array_32("snapshot_export.accounts_hash", exported.accounts_hash)?;
    if accounts_hash != expected_accounts_hash {
        return Err(HistoricalRuntimeError::SnapshotExportHashMismatch);
    }
    let reported_sha256 = array_32("snapshot_export.archive_sha256", exported.archive_sha256)?;
    let expected_path = output_directory.join(format!(
        "snapshot-{}-{}.tar.bz2",
        expected_slot,
        bs58::encode(expected_accounts_hash).into_string()
    ));
    if exported.archive_path != path_string(&expected_path)? {
        return Err(HistoricalRuntimeError::SnapshotExportPathMismatch {
            expected: expected_path,
            actual: exported.archive_path,
        });
    }
    let mut archive = open_readonly_nofollow(&expected_path).map_err(|source| {
        HistoricalRuntimeError::PathIo {
            path: expected_path.clone(),
            source,
        }
    })?;
    let metadata = archive
        .metadata()
        .map_err(|source| HistoricalRuntimeError::PathIo {
            path: expected_path.clone(),
            source,
        })?;
    if !metadata.is_file() {
        return Err(HistoricalRuntimeError::SnapshotExportNotFile(expected_path));
    }
    ensure_path_names_open_file(&expected_path, &metadata)?;
    let actual_size = metadata.len();
    if exported.archive_size != actual_size {
        return Err(HistoricalRuntimeError::SnapshotExportSizeMismatch {
            reported: exported.archive_size,
            actual: actual_size,
        });
    }
    if actual_size == 0 {
        return Err(HistoricalRuntimeError::SnapshotExportEmpty(expected_path));
    }
    let archive_sha256 = sha256_open_file_exact(&mut archive, &expected_path, actual_size)?;
    ensure_path_names_open_file(&expected_path, &metadata)?;
    if archive_sha256 != reported_sha256 {
        return Err(HistoricalRuntimeError::SnapshotExportDigestMismatch {
            reported: digest_hex(&reported_sha256),
            actual: digest_hex(&archive_sha256),
        });
    }
    Ok(HistoricalSnapshotExport {
        slot: expected_slot,
        archive_path: expected_path,
        accounts_hash,
        archive_size: actual_size,
        archive_sha256,
    })
}

/// Measure one bounded, open worker file. This is used when deciding whether
/// existing evidence was produced by the worker configured for the current
/// run; actual worker startup additionally executes a private copy made from
/// the same measured byte stream.
pub(crate) fn measure_executable_sha256(
    path: &Path,
) -> Result<[u8; HASH_BYTES], HistoricalRuntimeError> {
    let canonical = canonical_regular_file(path, true)?;
    stream_worker_executable(&canonical, None).map(|(sha256, _)| sha256)
}

fn canonical_directory(path: &Path, kind: &'static str) -> Result<PathBuf, HistoricalRuntimeError> {
    let canonical = fs::canonicalize(path).map_err(|source| HistoricalRuntimeError::PathIo {
        path: path.to_path_buf(),
        source,
    })?;
    if canonical.is_dir() {
        Ok(canonical)
    } else if kind == "ledger" {
        Err(HistoricalRuntimeError::LedgerNotDirectory(canonical))
    } else if kind == "snapshot-output" {
        Err(HistoricalRuntimeError::SnapshotOutputNotDirectory(
            canonical,
        ))
    } else {
        Err(HistoricalRuntimeError::ScratchParentNotDirectory(canonical))
    }
}

fn make_private_work_dir(parent: Option<&Path>) -> Result<TempDir, HistoricalRuntimeError> {
    let mut builder = TempDirBuilder::new();
    builder.prefix("jetstreamer-historical-parent-");
    match parent {
        Some(parent) => {
            let parent = canonical_directory(parent, "scratch")?;
            builder.tempdir_in(&parent).map_err(|source| {
                HistoricalRuntimeError::CreatePrivateWorkDir {
                    path: parent,
                    source,
                }
            })
        }
        None => builder
            .tempdir()
            .map_err(|source| HistoricalRuntimeError::CreatePrivateWorkDir {
                path: std::env::temp_dir(),
                source,
            }),
    }
}

fn path_string(path: &Path) -> Result<String, HistoricalRuntimeError> {
    path.to_str()
        .map(str::to_owned)
        .ok_or_else(|| HistoricalRuntimeError::NonUtf8Path(path.to_path_buf()))
}

fn verify_handshake(
    profile: WorkerProfile,
    handshake: &protocol::Handshake,
) -> Result<(), HistoricalRuntimeError> {
    verify_identity_field(
        "protocol_version",
        protocol::PROTOCOL_VERSION.to_string(),
        handshake.protocol_version.to_string(),
    )?;
    if handshake.qualification != BackendQualification::Candidate {
        return Err(HistoricalRuntimeError::QualificationMismatch(
            handshake.qualification.clone(),
        ));
    }
    verify_identity_field("backend_id", profile.backend_id, &handshake.backend_id)?;
    verify_identity_field("solana_tag", profile.solana_tag, &handshake.solana_tag)?;
    verify_identity_field(
        "solana_commit",
        profile.solana_commit,
        &handshake.solana_commit,
    )?;
    verify_identity_field(
        "rust_toolchain",
        profile.rust_toolchain,
        &handshake.rust_toolchain,
    )?;
    verify_identity_field("target", profile.target, &handshake.target)?;
    verify_identity_field(
        "required_genesis_hash",
        profile.required_genesis_hash,
        &handshake.required_genesis_hash,
    )
}

fn verify_identity_field(
    field: &'static str,
    expected: impl ToString,
    actual: impl ToString,
) -> Result<(), HistoricalRuntimeError> {
    let expected = expected.to_string();
    let actual = actual.to_string();
    if expected == actual {
        Ok(())
    } else {
        Err(HistoricalRuntimeError::IdentityMismatch {
            field,
            expected,
            actual,
        })
    }
}

fn validate_initialized(
    profile: WorkerProfile,
    initialized: protocol::Initialized,
    expected_archive_path: &str,
    expected_slot: u64,
    expected_accounts_hash: [u8; HASH_BYTES],
) -> Result<HistoricalInitialized, HistoricalRuntimeError> {
    if initialized.genesis_hash != profile.required_genesis_hash {
        return Err(HistoricalRuntimeError::InitializedGenesisMismatch {
            expected: profile.required_genesis_hash.to_owned(),
            actual: initialized.genesis_hash,
        });
    }
    if initialized.slot != expected_slot {
        return Err(HistoricalRuntimeError::InitializedSlotMismatch {
            expected: expected_slot,
            actual: initialized.slot,
        });
    }
    match initialized.source {
        InitializedSource::SnapshotArchive {
            archive_path,
            expected_accounts_hash: actual_accounts_hash,
        } => {
            if archive_path != expected_archive_path {
                return Err(HistoricalRuntimeError::InitializedArchiveMismatch {
                    expected: expected_archive_path.to_owned(),
                    actual: archive_path,
                });
            }
            let actual_accounts_hash = array_32(
                "initialized.source.expected_accounts_hash",
                actual_accounts_hash,
            )?;
            if actual_accounts_hash != expected_accounts_hash {
                return Err(HistoricalRuntimeError::InitializedSourceMismatch(format!(
                    "snapshot accounts hash mismatch: expected {}, got {}",
                    bs58::encode(expected_accounts_hash).into_string(),
                    bs58::encode(actual_accounts_hash).into_string()
                )));
            }
        }
        InitializedSource::Genesis => {
            return Err(HistoricalRuntimeError::InitializedSourceMismatch(
                "worker reported Genesis after SnapshotArchive initialization".to_owned(),
            ));
        }
    }
    Ok(HistoricalInitialized {
        genesis_hash: initialized.genesis_hash,
        slot: initialized.slot,
        last_blockhash: array_32("initialized.last_blockhash", initialized.last_blockhash)?,
        ticks_per_slot: initialized.ticks_per_slot,
        next_write_version: initialized.next_write_version,
    })
}

fn validate_encoded_transactions(transactions: &[Vec<u8>]) -> Result<(), HistoricalRuntimeError> {
    for (index, transaction) in transactions.iter().enumerate() {
        if transaction.len() > protocol::MAX_FRAME_BYTES {
            return Err(HistoricalRuntimeError::EncodedTransactionTooLarge {
                index,
                bytes: transaction.len(),
            });
        }
    }
    Ok(())
}

fn validate_account_write(
    write: protocol::AccountWrite,
    expected_slot: u64,
) -> Result<HistoricalAccountWrite, HistoricalRuntimeError> {
    if write.slot != expected_slot {
        return Err(HistoricalRuntimeError::AccountWriteSlotMismatch {
            expected: expected_slot,
            actual: write.slot,
        });
    }
    Ok(HistoricalAccountWrite {
        slot: write.slot,
        write_version: write.write_version,
        transaction_signature: write
            .transaction_signature
            .map(|bytes| array_64("account_write.transaction_signature", bytes))
            .transpose()?,
        pubkey: array_32("account_write.pubkey", write.pubkey)?,
        lamports: write.lamports,
        owner: array_32("account_write.owner", write.owner)?,
        executable: write.executable,
        rent_epoch: write.rent_epoch,
        data: write.data,
        stored_hash: array_32("account_write.stored_hash", write.stored_hash)?,
    })
}

fn validate_entry_writes(
    writes: Vec<protocol::AccountWrite>,
    previous_slot: u64,
    requested_slot: u64,
) -> Result<Vec<HistoricalAccountWrite>, HistoricalRuntimeError> {
    let advanced = requested_slot > previous_slot;
    let mut last_slot = previous_slot;
    let mut validated = Vec::with_capacity(writes.len());
    for write in writes {
        let actual = write.slot;
        let allowed = actual == requested_slot || (advanced && actual == previous_slot);
        if !allowed {
            return Err(HistoricalRuntimeError::EntryAccountWriteSlotMismatch {
                previous: previous_slot,
                requested: requested_slot,
                actual,
            });
        }
        if actual < last_slot {
            return Err(HistoricalRuntimeError::AccountWriteSlotOrder {
                previous: last_slot,
                actual,
            });
        }
        last_slot = actual;
        validated.push(validate_account_write(write, actual)?);
    }
    Ok(validated)
}

fn validate_write_versions(
    previous_next: u64,
    versions: impl IntoIterator<Item = u64>,
    reported_next: u64,
) -> Result<u64, HistoricalRuntimeError> {
    let mut expected = previous_next;
    for (index, actual) in versions.into_iter().enumerate() {
        if actual != expected {
            return Err(HistoricalRuntimeError::WriteVersionGap {
                index,
                expected,
                actual,
            });
        }
        expected = expected
            .checked_add(1)
            .ok_or(HistoricalRuntimeError::WriteVersionOverflow(expected))?;
    }
    if reported_next != expected {
        return Err(HistoricalRuntimeError::WriteVersionCursorMismatch {
            expected,
            actual: reported_next,
        });
    }
    Ok(reported_next)
}

fn timeout_from_env(
    variable: &'static str,
    fallback_variable: Option<&'static str>,
    default: Duration,
) -> Result<Duration, HistoricalRuntimeError> {
    let selected = env::var_os(variable)
        .map(|value| (variable, value))
        .or_else(|| {
            fallback_variable
                .and_then(|fallback| env::var_os(fallback).map(|value| (fallback, value)))
        });
    let Some((selected_variable, value)) = selected else {
        return Ok(default);
    };
    let value = value.to_string_lossy().into_owned();
    let seconds = value
        .trim()
        .parse::<u64>()
        .ok()
        .filter(|seconds| *seconds > 0)
        .ok_or_else(|| HistoricalRuntimeError::InvalidTimeout {
            variable: selected_variable,
            value: value.clone(),
        })?;
    Ok(Duration::from_secs(seconds))
}

fn wait_for_child(child: &mut Child, timeout: Duration) -> io::Result<Option<ExitStatus>> {
    let started = Instant::now();
    loop {
        if let Some(status) = child.try_wait()? {
            return Ok(Some(status));
        }
        let elapsed = started.elapsed();
        if elapsed >= timeout {
            return Ok(None);
        }
        thread::sleep(CHILD_WAIT_POLL_INTERVAL.min(timeout.saturating_sub(elapsed)));
    }
}

/// Terminate a worker without allowing client teardown to wait forever.
///
/// A normally scheduled process is reaped synchronously. If the kernel does
/// not make it waitable within the configured window, ownership of both the
/// `Child` and its private directory moves to a detached reaper. Rust threads
/// do not keep process exit alive, so even that pathological wait cannot wedge
/// the parent process.
fn kill_and_reap(mut child: Child, private_work_dir: Option<TempDir>, timeout: Duration) -> bool {
    let _ = child.kill();
    if matches!(wait_for_child(&mut child, timeout), Ok(Some(_))) {
        return true;
    }
    thread::spawn(move || {
        let _private_work_dir = private_work_dir;
        let _ = child.wait();
    });
    false
}

fn validate_response_id(
    previous: Option<u64>,
    expected: u64,
    actual: u64,
) -> Result<u64, HistoricalRuntimeError> {
    if let Some(previous) = previous
        && actual <= previous
    {
        return Err(HistoricalRuntimeError::NonMonotonicResponseId { previous, actual });
    }
    if actual != expected {
        return Err(HistoricalRuntimeError::ResponseIdMismatch { expected, actual });
    }
    Ok(actual)
}

fn array_32(
    field: &'static str,
    bytes: Vec<u8>,
) -> Result<[u8; HASH_BYTES], HistoricalRuntimeError> {
    exact_array(field, bytes)
}

fn array_64(
    field: &'static str,
    bytes: Vec<u8>,
) -> Result<[u8; SIGNATURE_BYTES], HistoricalRuntimeError> {
    exact_array(field, bytes)
}

fn exact_array<const N: usize>(
    field: &'static str,
    bytes: Vec<u8>,
) -> Result<[u8; N], HistoricalRuntimeError> {
    let actual = bytes.len();
    bytes
        .try_into()
        .map_err(|_| HistoricalRuntimeError::InvalidArrayLength {
            field,
            expected: N,
            actual,
        })
}

fn response_kind(body: &ResponseBody) -> &'static str {
    match body {
        ResponseBody::Handshake(_) => "Handshake",
        ResponseBody::Initialized(_) => "Initialized",
        ResponseBody::EntryProcessed(_) => "EntryProcessed",
        ResponseBody::Checkpoint(_) => "Checkpoint",
        ResponseBody::Pong => "Pong",
        ResponseBody::ShuttingDown => "ShuttingDown",
        ResponseBody::Error(_) => "Error",
        ResponseBody::SnapshotExported(_) => "SnapshotExported",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_transaction::{Address, Signature, Transaction};

    type HandshakeMutation = (&'static str, fn(&mut protocol::Handshake));

    #[test]
    fn bound_handoff_snapshot_is_an_exact_private_copy() {
        let source_directory = TempDir::new().unwrap();
        let private_directory = TempDir::new().unwrap();
        let source = source_directory.path().join("snapshot-1-hash.tar.bz2");
        fs::write(&source, b"bank, accounts, and status cache").unwrap();
        let expected = sha256_file(&source).unwrap();

        let bound = bind_snapshot_archive(
            &source,
            private_directory.path(),
            fs::metadata(&source).unwrap().len(),
            expected,
        )
        .unwrap();
        assert_ne!(bound, source);
        assert_eq!(bound.file_name(), source.file_name());
        assert_eq!(fs::read(bound).unwrap(), fs::read(source).unwrap());
    }

    #[test]
    fn bound_handoff_snapshot_rejects_an_unexpected_digest() {
        let source_directory = TempDir::new().unwrap();
        let private_directory = TempDir::new().unwrap();
        let source = source_directory.path().join("snapshot-1-hash.tar.bz2");
        fs::write(&source, b"unexpected bytes").unwrap();

        assert!(matches!(
            bind_snapshot_archive(
                &source,
                private_directory.path(),
                fs::metadata(&source).unwrap().len(),
                [0x55; HASH_BYTES]
            ),
            Err(HistoricalRuntimeError::SnapshotArchiveDigestMismatch { .. })
        ));
    }

    #[test]
    fn bound_handoff_snapshot_rejects_an_unexpected_size_before_copying() {
        let source_directory = TempDir::new().unwrap();
        let private_directory = TempDir::new().unwrap();
        let source = source_directory.path().join("snapshot-1-hash.tar.bz2");
        fs::write(&source, b"unexpected bytes").unwrap();

        assert!(matches!(
            bind_snapshot_archive(
                &source,
                private_directory.path(),
                1,
                sha256_file(&source).unwrap()
            ),
            Err(HistoricalRuntimeError::SnapshotArchiveSizeMismatch {
                expected: 1,
                actual: 16
            })
        ));
    }

    #[test]
    fn request_timeouts_separate_entry_execution_from_checkpoint_hashing() {
        let timeouts = HistoricalRuntimeTimeouts::default();
        assert_eq!(
            timeouts.request(&RequestBody::Ping),
            ("ping", DEFAULT_CONTROL_TIMEOUT)
        );
        assert_eq!(
            timeouts.request(&RequestBody::ProcessEntry(protocol::EntryRequest {
                slot: 1,
                entry_index: 0,
                num_hashes: 0,
                hash: vec![0; HASH_BYTES],
                transactions: Vec::new(),
            })),
            ("process-entry", DEFAULT_ENTRY_TIMEOUT)
        );
        assert_eq!(
            timeouts.request(&RequestBody::FreezeCheckpoint { slot: 1 }),
            ("freeze-checkpoint", DEFAULT_CHECKPOINT_TIMEOUT)
        );
        assert_eq!(
            timeouts.request(&RequestBody::ExportSnapshot {
                slot: 1,
                output_directory: "/tmp".to_owned(),
                expected_accounts_hash: vec![0; HASH_BYTES],
            }),
            ("export-snapshot", DEFAULT_CHECKPOINT_TIMEOUT)
        );
        assert!(timeouts.checkpoint > timeouts.initialize);
        assert!(timeouts.initialize > timeouts.entry);
    }

    #[cfg(unix)]
    fn sleeping_child() -> Child {
        Command::new("sleep")
            .arg("30")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .unwrap()
    }

    #[cfg(unix)]
    fn scripted_response_client(response_bytes: &[u8]) -> (HistoricalRuntimeClient, u32) {
        let private_work_dir = TempDir::new().unwrap();
        let response_path = private_work_dir.path().join("response.bin");
        fs::write(&response_path, response_bytes).unwrap();
        let mut child = Command::new("sh")
            .arg("-c")
            .arg("cat \"$1\"; exec sleep 30")
            .arg("historical-runtime-test-worker")
            .arg(&response_path)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let child_id = child.id();
        let stdin = child.stdin.take().unwrap();
        let stdout = child.stdout.take().unwrap();
        let transport = IpcTransport::spawn(stdin, stdout).unwrap();
        let client = HistoricalRuntimeClient {
            executable_sha256: [0; HASH_BYTES],
            child: Some(child),
            transport: Some(transport),
            private_work_dir: Some(private_work_dir),
            timeouts: HistoricalRuntimeTimeouts {
                control: Duration::from_secs(2),
                checkpoint: Duration::from_secs(2),
                reap: Duration::from_secs(2),
                ..HistoricalRuntimeTimeouts::default()
            },
            next_request_id: 1,
            last_response_id: None,
            current_slot: 0,
            next_write_version: 0,
            snapshot_export_seal: None,
            initialized: HistoricalInitialized {
                genesis_hash: String::new(),
                slot: 0,
                last_blockhash: [0; HASH_BYTES],
                ticks_per_slot: 0,
                next_write_version: 0,
            },
            closed: false,
        };
        (client, child_id)
    }

    #[cfg(unix)]
    fn client_with_response(response: &Response) -> (HistoricalRuntimeClient, u32) {
        let mut response_bytes = Vec::new();
        protocol::write_frame(&mut response_bytes, response).unwrap();
        scripted_response_client(&response_bytes)
    }

    #[cfg(unix)]
    fn process_is_running(process_id: u32) -> bool {
        Command::new("sh")
            .arg("-c")
            .arg("kill -0 \"$1\" 2>/dev/null")
            .arg("historical-runtime-process-check")
            .arg(process_id.to_string())
            .status()
            .unwrap()
            .success()
    }

    #[cfg(unix)]
    fn assert_client_poisoned_and_worker_reaped(
        client: &mut HistoricalRuntimeClient,
        child_id: u32,
    ) {
        assert!(client.closed);
        assert!(client.child.is_none());
        assert!(client.transport.is_none());
        assert!(matches!(client.ping(), Err(HistoricalRuntimeError::Closed)));
        assert!(!process_is_running(child_id));
    }

    #[cfg(unix)]
    #[test]
    fn invalid_response_id_poisons_client_and_reaps_worker() {
        let (mut client, child_id) = client_with_response(&Response {
            request_id: 2,
            body: ResponseBody::Pong,
        });
        assert!(process_is_running(child_id));

        assert!(matches!(
            client.ping(),
            Err(HistoricalRuntimeError::ResponseIdMismatch {
                expected: 1,
                actual: 2,
            })
        ));
        assert_client_poisoned_and_worker_reaped(&mut client, child_id);
    }

    #[cfg(unix)]
    #[test]
    fn unexpected_response_body_poisons_client_and_reaps_worker() {
        let (mut client, child_id) = client_with_response(&Response {
            request_id: 1,
            body: ResponseBody::ShuttingDown,
        });
        assert!(process_is_running(child_id));

        assert!(matches!(
            client.ping(),
            Err(HistoricalRuntimeError::UnexpectedResponse {
                expected: "Pong",
                actual: "ShuttingDown",
            })
        ));
        assert_client_poisoned_and_worker_reaped(&mut client, child_id);
    }

    #[cfg(unix)]
    #[test]
    fn invalid_response_field_poisons_client_and_reaps_worker() {
        let (mut client, child_id) = client_with_response(&Response {
            request_id: 1,
            body: ResponseBody::Checkpoint(protocol::Checkpoint {
                slot: 8,
                bank_hash: vec![0; HASH_BYTES],
                accounts_hash: vec![0; HASH_BYTES],
                last_blockhash: vec![0; HASH_BYTES],
                capitalization: 0,
                transaction_count: 0,
                tick_height: 0,
                slot_complete: true,
                writes: Vec::new(),
                next_write_version: 0,
            }),
        });
        assert!(process_is_running(child_id));

        assert!(matches!(
            client.freeze_checkpoint(7),
            Err(HistoricalRuntimeError::CheckpointSlotMismatch {
                expected: 7,
                actual: 8,
            })
        ));
        assert_client_poisoned_and_worker_reaped(&mut client, child_id);
    }

    #[cfg(unix)]
    #[test]
    fn malformed_response_frame_poisons_client_and_reaps_worker() {
        let (mut client, child_id) = scripted_response_client(&[1, 0, 0, 0, 0xff]);
        assert!(process_is_running(child_id));

        assert!(matches!(client.ping(), Err(HistoricalRuntimeError::Io(_))));
        assert_client_poisoned_and_worker_reaped(&mut client, child_id);
    }

    #[cfg(unix)]
    #[test]
    fn worker_error_poisons_client_and_reaps_worker() {
        let (mut client, child_id) = client_with_response(&Response {
            request_id: 1,
            body: ResponseBody::Error(protocol::WorkerError {
                code: protocol::WorkerErrorCode::InvalidRequest,
                message: "failure after possible mutation".to_owned(),
            }),
        });
        assert!(process_is_running(child_id));

        assert!(matches!(
            client.ping(),
            Err(HistoricalRuntimeError::Worker {
                code: protocol::WorkerErrorCode::InvalidRequest,
                ..
            })
        ));
        assert_client_poisoned_and_worker_reaped(&mut client, child_id);
    }

    #[cfg(unix)]
    #[test]
    fn timed_out_ipc_terminates_and_reaps_worker() {
        let mut child = sleeping_child();
        let stdin = child.stdin.take().unwrap();
        let stdout = child.stdout.take().unwrap();
        let transport = IpcTransport::spawn(stdin, stdout).unwrap();
        let mut client = HistoricalRuntimeClient {
            executable_sha256: [0; HASH_BYTES],
            child: Some(child),
            transport: Some(transport),
            private_work_dir: None,
            timeouts: HistoricalRuntimeTimeouts {
                control: Duration::from_millis(50),
                reap: Duration::from_secs(2),
                ..HistoricalRuntimeTimeouts::default()
            },
            next_request_id: 1,
            last_response_id: None,
            current_slot: 0,
            next_write_version: 0,
            snapshot_export_seal: None,
            initialized: HistoricalInitialized {
                genesis_hash: String::new(),
                slot: 0,
                last_blockhash: [0; HASH_BYTES],
                ticks_per_slot: 0,
                next_write_version: 0,
            },
            closed: false,
        };

        let started = Instant::now();
        let error = client.ping().unwrap_err();
        assert!(matches!(
            error,
            HistoricalRuntimeError::RequestTimeout {
                operation: "ping",
                request_id: 1,
                ..
            }
        ));
        assert!(started.elapsed() < Duration::from_secs(2));
        assert!(client.closed);
        assert!(client.child.is_none());
        assert!(client.transport.is_none());

        // Exercise the same bounded kill/wait helper directly so the test also
        // proves the normal SIGKILL path is reaped synchronously.
        assert!(kill_and_reap(
            sleeping_child(),
            None,
            Duration::from_secs(2)
        ));
    }

    #[test]
    fn executable_digest_is_content_bound_and_detects_replacement() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("worker");
        fs::write(&path, b"abc").unwrap();
        let original = measure_executable_sha256(&path).unwrap();
        assert_eq!(
            original,
            [
                0xba, 0x78, 0x16, 0xbf, 0x8f, 0x01, 0xcf, 0xea, 0x41, 0x41, 0x40, 0xde, 0x5d, 0xae,
                0x22, 0x23, 0xb0, 0x03, 0x61, 0xa3, 0x96, 0x17, 0x7a, 0x9c, 0xb4, 0x10, 0xff, 0x61,
                0xf2, 0x00, 0x15, 0xad,
            ]
        );

        fs::write(&path, b"abd").unwrap();
        assert_ne!(measure_executable_sha256(&path).unwrap(), original);
    }

    #[cfg(unix)]
    #[test]
    fn bound_worker_executes_the_bytes_that_were_measured_after_source_replacement() {
        use std::os::unix::fs::PermissionsExt as _;

        let source_directory = TempDir::new().unwrap();
        let private_directory = TempDir::new().unwrap();
        let source = source_directory.path().join("worker");
        fs::write(&source, b"#!/bin/sh\nprintf old").unwrap();
        fs::set_permissions(&source, fs::Permissions::from_mode(0o700)).unwrap();

        let bound = bind_worker_executable(&source, private_directory.path()).unwrap();
        assert_eq!(bound.sha256, sha256_file(&bound.path).unwrap());
        assert_eq!(
            fs::metadata(&bound.path).unwrap().permissions().mode() & 0o777,
            0o700
        );

        let replaced = source_directory.path().join("worker.replaced");
        fs::rename(&source, &replaced).unwrap();
        fs::write(&source, b"#!/bin/sh\nprintf new").unwrap();
        fs::set_permissions(&source, fs::Permissions::from_mode(0o700)).unwrap();

        let output = Command::new(&bound.path).output().unwrap();
        assert!(output.status.success());
        assert_eq!(output.stdout, b"old");
        assert_ne!(measure_executable_sha256(&source).unwrap(), bound.sha256);
    }

    #[test]
    fn executable_measurement_rejects_files_above_the_hard_limit() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("worker");
        let file = fs::File::create(&path).unwrap();
        file.set_len(MAX_HISTORICAL_WORKER_EXECUTABLE_BYTES + 1)
            .unwrap();

        assert!(matches!(
            measure_executable_sha256(&path),
            Err(HistoricalRuntimeError::ExecutableTooLarge {
                bytes,
                limit: MAX_HISTORICAL_WORKER_EXECUTABLE_BYTES,
                ..
            }) if bytes == MAX_HISTORICAL_WORKER_EXECUTABLE_BYTES + 1
        ));
    }

    #[cfg(unix)]
    #[test]
    fn single_open_admission_rejects_symlinks_fifos_and_path_replacement() {
        use {
            std::ffi::CString,
            std::os::unix::{ffi::OsStrExt as _, fs::symlink},
        };

        let directory = TempDir::new().unwrap();
        let target = directory.path().join("target");
        fs::write(&target, b"worker bytes").unwrap();
        let link = directory.path().join("link");
        symlink(&target, &link).unwrap();
        assert!(matches!(
            open_regular_file_nofollow(&link, true),
            Err(HistoricalRuntimeError::PathIo { .. })
        ));

        let fifo = directory.path().join("fifo");
        let fifo_c = CString::new(fifo.as_os_str().as_bytes()).unwrap();
        assert_eq!(unsafe { libc::mkfifo(fifo_c.as_ptr(), 0o600) }, 0);
        // Keeping the FIFO open read/write makes this test nonblocking even if
        // the admission flags regress; fstat must still reject the file type.
        let mut keeper_options = fs::OpenOptions::new();
        keeper_options.read(true).write(true);
        use std::os::unix::fs::OpenOptionsExt as _;
        keeper_options.custom_flags(libc::O_NONBLOCK);
        let _keeper = keeper_options.open(&fifo).unwrap();
        assert!(matches!(
            open_regular_file_nofollow(&fifo, false),
            Err(HistoricalRuntimeError::SnapshotNotFile(_))
        ));

        let path = directory.path().join("swapped");
        fs::write(&path, b"first").unwrap();
        let (_opened, metadata) = open_regular_file_nofollow(&path, false).unwrap();
        fs::rename(&path, directory.path().join("old")).unwrap();
        fs::write(&path, b"other").unwrap();
        assert!(matches!(
            ensure_path_names_open_file(&path, &metadata),
            Err(HistoricalRuntimeError::PathIdentityChanged(changed)) if changed == path
        ));
    }

    #[test]
    fn exported_snapshot_path_size_hash_and_digest_are_verified() {
        let directory = tempfile::tempdir().unwrap();
        let accounts_hash = [7; HASH_BYTES];
        let path = directory.path().join(format!(
            "snapshot-42-{}.tar.bz2",
            bs58::encode(accounts_hash).into_string()
        ));
        fs::write(&path, b"snapshot fixture").unwrap();
        let archive_sha256 = sha256_file(&path).unwrap();
        let wire = protocol::SnapshotExport {
            slot: 42,
            archive_path: path.to_str().unwrap().to_owned(),
            accounts_hash: accounts_hash.to_vec(),
            archive_size: 16,
            archive_sha256: archive_sha256.to_vec(),
        };
        let validated =
            validate_snapshot_export(directory.path(), 42, accounts_hash, wire).unwrap();
        assert_eq!(validated.archive_path, path);
        assert_eq!(validated.archive_size, 16);
        assert_eq!(validated.archive_sha256, sha256_file(&path).unwrap());

        let wrong_size = protocol::SnapshotExport {
            slot: 42,
            archive_path: path.to_str().unwrap().to_owned(),
            accounts_hash: accounts_hash.to_vec(),
            archive_size: 15,
            archive_sha256: archive_sha256.to_vec(),
        };
        assert!(matches!(
            validate_snapshot_export(directory.path(), 42, accounts_hash, wrong_size),
            Err(HistoricalRuntimeError::SnapshotExportSizeMismatch {
                reported: 15,
                actual: 16
            })
        ));

        let wrong_path = protocol::SnapshotExport {
            slot: 42,
            archive_path: directory
                .path()
                .join("other")
                .to_string_lossy()
                .into_owned(),
            accounts_hash: accounts_hash.to_vec(),
            archive_size: 16,
            archive_sha256: archive_sha256.to_vec(),
        };
        assert!(matches!(
            validate_snapshot_export(directory.path(), 42, accounts_hash, wrong_path),
            Err(HistoricalRuntimeError::SnapshotExportPathMismatch { .. })
        ));

        let wrong_digest = protocol::SnapshotExport {
            slot: 42,
            archive_path: path.to_str().unwrap().to_owned(),
            accounts_hash: accounts_hash.to_vec(),
            archive_size: 16,
            archive_sha256: vec![0x55; HASH_BYTES],
        };
        assert!(matches!(
            validate_snapshot_export(directory.path(), 42, accounts_hash, wrong_digest),
            Err(HistoricalRuntimeError::SnapshotExportDigestMismatch { .. })
        ));

        let short_digest = protocol::SnapshotExport {
            slot: 42,
            archive_path: path.to_str().unwrap().to_owned(),
            accounts_hash: accounts_hash.to_vec(),
            archive_size: 16,
            archive_sha256: vec![0x55; HASH_BYTES - 1],
        };
        assert!(matches!(
            validate_snapshot_export(directory.path(), 42, accounts_hash, short_digest),
            Err(HistoricalRuntimeError::InvalidArrayLength {
                field: "snapshot_export.archive_sha256",
                expected: HASH_BYTES,
                actual,
            }) if actual == HASH_BYTES - 1
        ));
    }

    fn valid_handshake_for(profile: WorkerProfile) -> protocol::Handshake {
        protocol::Handshake {
            protocol_version: protocol::PROTOCOL_VERSION,
            backend_id: profile.backend_id.to_owned(),
            qualification: BackendQualification::Candidate,
            solana_tag: profile.solana_tag.to_owned(),
            solana_commit: profile.solana_commit.to_owned(),
            rust_toolchain: profile.rust_toolchain.to_owned(),
            target: profile.target.to_owned(),
            required_genesis_hash: profile.required_genesis_hash.to_owned(),
        }
    }

    fn valid_handshake() -> protocol::Handshake {
        valid_handshake_for(SOLANA_V1_0_24_CANDIDATE)
    }

    #[test]
    fn exact_worker_identity_is_accepted() {
        verify_handshake(
            SOLANA_V1_0_7_CANDIDATE,
            &valid_handshake_for(SOLANA_V1_0_7_CANDIDATE),
        )
        .unwrap();
        verify_handshake(
            SOLANA_V1_0_8_CANDIDATE,
            &valid_handshake_for(SOLANA_V1_0_8_CANDIDATE),
        )
        .unwrap();
        verify_handshake(SOLANA_V1_0_24_CANDIDATE, &valid_handshake()).unwrap();
    }

    #[test]
    fn worker_profiles_are_not_interchangeable() {
        let error = verify_handshake(SOLANA_V1_0_7_CANDIDATE, &valid_handshake()).unwrap_err();
        assert!(matches!(
            error,
            HistoricalRuntimeError::IdentityMismatch {
                field: "backend_id",
                ..
            }
        ));
        let error = verify_handshake(
            SOLANA_V1_0_8_CANDIDATE,
            &valid_handshake_for(SOLANA_V1_0_7_CANDIDATE),
        )
        .unwrap_err();
        assert!(matches!(
            error,
            HistoricalRuntimeError::IdentityMismatch {
                field: "backend_id",
                ..
            }
        ));
    }

    #[test]
    fn any_worker_identity_mismatch_is_rejected() {
        let mutations: &[HandshakeMutation] = &[
            ("protocol_version", |value| value.protocol_version += 1),
            ("backend_id", |value| value.backend_id.push_str("-other")),
            ("solana_tag", |value| value.solana_tag.push_str("-other")),
            ("solana_commit", |value| value.solana_commit.push('0')),
            ("rust_toolchain", |value| {
                value.rust_toolchain.push_str("-other")
            }),
            ("target", |value| value.target.push_str("-other")),
            ("required_genesis_hash", |value| {
                value.required_genesis_hash.push('1')
            }),
        ];
        for (expected_field, mutate) in mutations {
            let mut handshake = valid_handshake();
            mutate(&mut handshake);
            let error = verify_handshake(SOLANA_V1_0_24_CANDIDATE, &handshake).unwrap_err();
            assert!(
                matches!(error, HistoricalRuntimeError::IdentityMismatch { field, .. } if field == *expected_field),
                "unexpected error for {expected_field}: {error:?}"
            );
        }
    }

    #[test]
    fn validates_hash_and_signature_lengths_strictly() {
        assert_eq!(
            array_32("hash", vec![7; HASH_BYTES]).unwrap(),
            [7; HASH_BYTES]
        );
        assert_eq!(
            array_64("signature", vec![9; SIGNATURE_BYTES]).unwrap(),
            [9; SIGNATURE_BYTES]
        );
        for length in [0, HASH_BYTES - 1, HASH_BYTES + 1, SIGNATURE_BYTES] {
            assert!(matches!(
                array_32("hash", vec![0; length]),
                Err(HistoricalRuntimeError::InvalidArrayLength {
                    expected: HASH_BYTES,
                    actual,
                    ..
                }) if actual == length
            ));
        }
        for length in [0, SIGNATURE_BYTES - 1, SIGNATURE_BYTES + 1, HASH_BYTES] {
            assert!(matches!(
                array_64("signature", vec![0; length]),
                Err(HistoricalRuntimeError::InvalidArrayLength {
                    expected: SIGNATURE_BYTES,
                    actual,
                    ..
                }) if actual == length
            ));
        }
    }

    #[test]
    fn response_ids_are_exact_and_strictly_monotonic() {
        assert_eq!(validate_response_id(None, 1, 1).unwrap(), 1);
        assert_eq!(validate_response_id(Some(1), 2, 2).unwrap(), 2);
        assert!(matches!(
            validate_response_id(Some(2), 3, 2),
            Err(HistoricalRuntimeError::NonMonotonicResponseId {
                previous: 2,
                actual: 2,
            })
        ));
        assert!(matches!(
            validate_response_id(Some(2), 3, 4),
            Err(HistoricalRuntimeError::ResponseIdMismatch {
                expected: 3,
                actual: 4,
            })
        ));
    }

    fn wire_write(slot: u64, write_version: u64) -> protocol::AccountWrite {
        protocol::AccountWrite {
            slot,
            write_version,
            transaction_signature: None,
            pubkey: vec![1; HASH_BYTES],
            lamports: 1,
            owner: vec![2; HASH_BYTES],
            executable: false,
            rent_epoch: 0,
            data: Vec::new(),
            stored_hash: vec![3; HASH_BYTES],
        }
    }

    #[test]
    fn advancing_entry_accepts_only_ordered_old_and_new_slot_writes() {
        let writes = validate_entry_writes(
            vec![wire_write(10, 4), wire_write(10, 5), wire_write(12, 6)],
            10,
            12,
        )
        .unwrap();
        assert_eq!(
            writes.iter().map(|write| write.slot).collect::<Vec<_>>(),
            vec![10, 10, 12]
        );

        assert!(matches!(
            validate_entry_writes(vec![wire_write(11, 4)], 10, 12),
            Err(HistoricalRuntimeError::EntryAccountWriteSlotMismatch {
                previous: 10,
                requested: 12,
                actual: 11,
            })
        ));
        assert!(matches!(
            validate_entry_writes(vec![wire_write(12, 4), wire_write(10, 5)], 10, 12),
            Err(HistoricalRuntimeError::AccountWriteSlotOrder {
                previous: 12,
                actual: 10,
            })
        ));
        assert!(matches!(
            validate_entry_writes(vec![wire_write(9, 4)], 10, 10),
            Err(HistoricalRuntimeError::EntryAccountWriteSlotMismatch {
                previous: 10,
                requested: 10,
                actual: 9,
            })
        ));
    }

    #[test]
    fn write_versions_must_be_complete_contiguous_and_match_cursor() {
        assert_eq!(validate_write_versions(40, [40, 41, 42], 43).unwrap(), 43);
        assert_eq!(
            validate_write_versions(40, std::iter::empty(), 40).unwrap(),
            40
        );
        assert!(matches!(
            validate_write_versions(40, [40, 42], 43),
            Err(HistoricalRuntimeError::WriteVersionGap {
                index: 1,
                expected: 41,
                actual: 42,
            })
        ));
        assert!(matches!(
            validate_write_versions(40, [40, 41], 44),
            Err(HistoricalRuntimeError::WriteVersionCursorMismatch {
                expected: 42,
                actual: 44,
            })
        ));
        assert!(matches!(
            validate_write_versions(40, std::iter::empty(), 41),
            Err(HistoricalRuntimeError::WriteVersionCursorMismatch {
                expected: 40,
                actual: 41,
            })
        ));
    }

    #[test]
    fn maps_every_v1_transaction_status_and_rejects_newer_statuses() {
        use CurrentTransactionError as Current;
        use protocol::TransactionError as Old;

        let cases = vec![
            (Current::AccountInUse, Old::AccountInUse),
            (Current::AccountLoadedTwice, Old::AccountLoadedTwice),
            (Current::AccountNotFound, Old::AccountNotFound),
            (Current::ProgramAccountNotFound, Old::ProgramAccountNotFound),
            (
                Current::InsufficientFundsForFee,
                Old::InsufficientFundsForFee,
            ),
            (Current::InvalidAccountForFee, Old::InvalidAccountForFee),
            (Current::AlreadyProcessed, Old::DuplicateSignature),
            (Current::BlockhashNotFound, Old::BlockhashNotFound),
            (
                Current::InstructionError(3, CurrentInstructionError::Custom(42)),
                Old::InstructionError {
                    instruction_index: 3,
                    error: protocol::InstructionError::Custom(42),
                },
            ),
            (Current::CallChainTooDeep, Old::CallChainTooDeep),
            (Current::MissingSignatureForFee, Old::MissingSignatureForFee),
            (Current::InvalidAccountIndex, Old::InvalidAccountIndex),
            (Current::SignatureFailure, Old::SignatureFailure),
            (
                Current::InvalidProgramForExecution,
                Old::InvalidProgramForExecution,
            ),
            (Current::SanitizeFailure, Old::SanitizeFailure),
        ];
        for (current, expected) in cases {
            assert_eq!(normalize_transaction_error(&current).unwrap(), expected);
            assert_eq!(denormalize_transaction_error(&expected), current);
        }
        assert!(matches!(
            normalize_transaction_error(&Current::UnsupportedVersion),
            Err(HistoricalRuntimeError::UnsupportedTransactionError(
                Current::UnsupportedVersion
            ))
        ));
    }

    #[test]
    #[allow(deprecated)]
    fn maps_every_v1_instruction_status_and_rejects_newer_statuses() {
        use CurrentInstructionError as Current;
        use protocol::InstructionError as Old;

        let cases = vec![
            (Current::GenericError, Old::GenericError),
            (Current::InvalidArgument, Old::InvalidArgument),
            (Current::InvalidInstructionData, Old::InvalidInstructionData),
            (Current::InvalidAccountData, Old::InvalidAccountData),
            (Current::AccountDataTooSmall, Old::AccountDataTooSmall),
            (Current::InsufficientFunds, Old::InsufficientFunds),
            (Current::IncorrectProgramId, Old::IncorrectProgramId),
            (
                Current::MissingRequiredSignature,
                Old::MissingRequiredSignature,
            ),
            (
                Current::AccountAlreadyInitialized,
                Old::AccountAlreadyInitialized,
            ),
            (Current::UninitializedAccount, Old::UninitializedAccount),
            (Current::UnbalancedInstruction, Old::UnbalancedInstruction),
            (Current::ModifiedProgramId, Old::ModifiedProgramId),
            (
                Current::ExternalAccountLamportSpend,
                Old::ExternalAccountLamportSpend,
            ),
            (
                Current::ExternalAccountDataModified,
                Old::ExternalAccountDataModified,
            ),
            (Current::ReadonlyLamportChange, Old::ReadonlyLamportChange),
            (Current::ReadonlyDataModified, Old::ReadonlyDataModified),
            (Current::DuplicateAccountIndex, Old::DuplicateAccountIndex),
            (Current::ExecutableModified, Old::ExecutableModified),
            (Current::RentEpochModified, Old::RentEpochModified),
            (Current::NotEnoughAccountKeys, Old::NotEnoughAccountKeys),
            (Current::AccountDataSizeChanged, Old::AccountDataSizeChanged),
            (Current::AccountNotExecutable, Old::AccountNotExecutable),
            (Current::AccountBorrowFailed, Old::AccountBorrowFailed),
            (
                Current::AccountBorrowOutstanding,
                Old::AccountBorrowOutstanding,
            ),
            (
                Current::DuplicateAccountOutOfSync,
                Old::DuplicateAccountOutOfSync,
            ),
            (Current::Custom(0xdecafbad), Old::Custom(0xdecafbad)),
            (Current::InvalidError, Old::InvalidError),
        ];
        for (current, expected) in cases {
            assert_eq!(normalize_instruction_error(&current).unwrap(), expected);
            assert_eq!(denormalize_instruction_error(&expected), current);
        }
        assert!(matches!(
            normalize_instruction_error(&Current::ComputationalBudgetExceeded),
            Err(HistoricalRuntimeError::UnsupportedInstructionError(
                Current::ComputationalBudgetExceeded
            ))
        ));
    }

    #[test]
    fn current_legacy_transaction_has_the_v1_transaction_wire_fixture() {
        let legacy = Transaction {
            signatures: vec![Signature::from([7; SIGNATURE_BYTES])],
            message: solana_message::legacy::Message {
                header: solana_message::MessageHeader {
                    num_required_signatures: 1,
                    ..Default::default()
                },
                account_keys: vec![Address::new_from_array([3; HASH_BYTES])],
                recent_blockhash: Hash::new_from_array([4; HASH_BYTES]),
                instructions: Vec::new(),
            },
        };
        let versioned = VersionedTransaction::from(legacy);

        let actual = encode_legacy_transaction(&versioned).unwrap();
        let mut fixture = Vec::with_capacity(134);
        fixture.push(1); // short-vec signature count
        fixture.extend_from_slice(&[7; SIGNATURE_BYTES]);
        fixture.extend_from_slice(&[1, 0, 0]); // MessageHeader
        fixture.push(1); // short-vec account-key count
        fixture.extend_from_slice(&[3; HASH_BYTES]);
        fixture.extend_from_slice(&[4; HASH_BYTES]);
        fixture.push(0); // short-vec instruction count
        assert_eq!(actual, fixture);
    }
}
