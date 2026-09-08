// Version-neutral IPC for isolated historical runtime workers.
//
// Keep this source free of Solana dependencies. Both a current Agave parent
// and an old compiler/runtime worker compile exactly these types.

use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::io::{self, Cursor, ErrorKind, Read, Write};

pub const PROTOCOL_VERSION: u16 = 3;
pub const MAX_FRAME_BYTES: usize = 64 * 1024 * 1024;

pub const MAINNET_GENESIS_HASH: &str = "5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d";

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Request {
    pub id: u64,
    pub body: RequestBody,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum RequestBody {
    Hello {
        protocol_version: u16,
    },
    Initialize {
        ledger_path: String,
        initial_state: InitialState,
        /// Optional existing directory under which the worker creates and
        /// owns a unique temporary state directory.
        scratch_root: Option<String>,
    },
    ProcessEntry(EntryRequest),
    FreezeCheckpoint {
        slot: u64,
    },
    Ping,
    Shutdown,
    ExportSnapshot {
        slot: u64,
        output_directory: String,
        /// Exactly 32 bytes. The worker must recompute and match this hash
        /// before publishing the archive.
        expected_accounts_hash: Vec<u8>,
    },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum InitialState {
    SnapshotArchive { archive_path: String },
    Genesis,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct EntryRequest {
    pub slot: u64,
    pub entry_index: u64,
    pub num_hashes: u64,
    /// Exactly 32 bytes.
    pub hash: Vec<u8>,
    /// Canonical bincode encodings of legacy Solana `Transaction` values.
    pub transactions: Vec<Vec<u8>>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Response {
    pub request_id: u64,
    pub body: ResponseBody,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum ResponseBody {
    Handshake(Handshake),
    Initialized(Initialized),
    EntryProcessed(EntryProcessed),
    Checkpoint(Checkpoint),
    Pong,
    ShuttingDown,
    Error(WorkerError),
    SnapshotExported(SnapshotExport),
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Handshake {
    pub protocol_version: u16,
    pub backend_id: String,
    pub qualification: BackendQualification,
    pub solana_tag: String,
    pub solana_commit: String,
    pub rust_toolchain: String,
    pub target: String,
    pub required_genesis_hash: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum BackendQualification {
    /// Source/toolchain are pinned, but no slot interval is asserted correct.
    Candidate,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Initialized {
    pub genesis_hash: String,
    pub source: InitializedSource,
    pub slot: u64,
    pub last_blockhash: Vec<u8>,
    pub ticks_per_slot: u64,
    pub next_write_version: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum InitializedSource {
    SnapshotArchive {
        archive_path: String,
        expected_accounts_hash: Vec<u8>,
    },
    Genesis,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct EntryProcessed {
    pub slot: u64,
    pub entry_index: u64,
    pub outcomes: Vec<TransactionOutcome>,
    pub writes: Vec<AccountWrite>,
    pub tick_height: u64,
    pub slot_complete: bool,
    pub next_write_version: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TransactionOutcome {
    pub signature: Option<Vec<u8>>,
    pub error: Option<TransactionError>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct AccountWrite {
    pub slot: u64,
    pub write_version: u64,
    pub transaction_signature: Option<Vec<u8>>,
    pub pubkey: Vec<u8>,
    pub lamports: u64,
    pub owner: Vec<u8>,
    pub executable: bool,
    pub rent_epoch: u64,
    pub data: Vec<u8>,
    pub stored_hash: Vec<u8>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Checkpoint {
    pub slot: u64,
    pub bank_hash: Vec<u8>,
    pub accounts_hash: Vec<u8>,
    pub last_blockhash: Vec<u8>,
    pub capitalization: u64,
    pub transaction_count: u64,
    pub tick_height: u64,
    pub slot_complete: bool,
    pub writes: Vec<AccountWrite>,
    pub next_write_version: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SnapshotExport {
    pub slot: u64,
    pub archive_path: String,
    /// Exactly 32 bytes.
    pub accounts_hash: Vec<u8>,
    pub archive_size: u64,
    /// SHA-256 measured by the worker through the persisted archive handle.
    /// Exactly 32 bytes.
    pub archive_sha256: Vec<u8>,
}

/// The normalized Solana v1 transaction-error superset through v1.0.24.
/// Older workers emit only the variants present in their exact release.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum TransactionError {
    AccountInUse,
    AccountLoadedTwice,
    AccountNotFound,
    ProgramAccountNotFound,
    InsufficientFundsForFee,
    InvalidAccountForFee,
    DuplicateSignature,
    BlockhashNotFound,
    InstructionError {
        instruction_index: u8,
        error: InstructionError,
    },
    CallChainTooDeep,
    MissingSignatureForFee,
    InvalidAccountIndex,
    SignatureFailure,
    InvalidProgramForExecution,
    SanitizeFailure,
}

/// The normalized Solana v1 instruction-error superset through v1.0.24.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum InstructionError {
    GenericError,
    InvalidArgument,
    InvalidInstructionData,
    InvalidAccountData,
    AccountDataTooSmall,
    InsufficientFunds,
    IncorrectProgramId,
    MissingRequiredSignature,
    AccountAlreadyInitialized,
    UninitializedAccount,
    UnbalancedInstruction,
    ModifiedProgramId,
    ExternalAccountLamportSpend,
    ExternalAccountDataModified,
    ReadonlyLamportChange,
    ReadonlyDataModified,
    DuplicateAccountIndex,
    ExecutableModified,
    RentEpochModified,
    NotEnoughAccountKeys,
    AccountDataSizeChanged,
    AccountNotExecutable,
    AccountBorrowFailed,
    AccountBorrowOutstanding,
    DuplicateAccountOutOfSync,
    Custom(u32),
    InvalidError,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct WorkerError {
    pub code: WorkerErrorCode,
    pub message: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum WorkerErrorCode {
    ProtocolVersionMismatch,
    NotInitialized,
    AlreadyInitialized,
    InvalidGenesis,
    InvalidRequest,
    NonMonotonicSlot,
    InvalidEntryOrder,
    MissingLeaderSchedule,
    TransactionDecode,
    Runtime,
    Internal,
    UnsupportedOperation,
}

pub fn write_frame<T: Serialize, W: Write>(writer: &mut W, value: &T) -> io::Result<()> {
    // The free functions use bincode's legacy fixed-integer configuration in
    // both 1.2.1 and 1.3.3.
    let payload =
        bincode::serialize(value).map_err(|error| io::Error::new(ErrorKind::InvalidData, error))?;
    if payload.len() > MAX_FRAME_BYTES {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            "historical-runtime frame exceeds maximum size",
        ));
    }
    let length = payload.len() as u32;
    writer.write_all(&length.to_le_bytes())?;
    writer.write_all(&payload)?;
    writer.flush()
}

pub fn read_frame<T: DeserializeOwned, R: Read>(reader: &mut R) -> io::Result<Option<T>> {
    let mut prefix = [0u8; 4];
    let mut read = 0;
    while read < prefix.len() {
        match reader.read(&mut prefix[read..]) {
            Ok(0) if read == 0 => return Ok(None),
            Ok(0) => {
                return Err(io::Error::new(
                    ErrorKind::UnexpectedEof,
                    "truncated historical-runtime frame prefix",
                ))
            }
            Ok(count) => read += count,
            Err(ref error) if error.kind() == ErrorKind::Interrupted => continue,
            Err(error) => return Err(error),
        }
    }

    let length = u32::from_le_bytes(prefix) as usize;
    if length > MAX_FRAME_BYTES {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            "historical-runtime frame exceeds maximum size",
        ));
    }
    let mut payload = vec![0u8; length];
    reader.read_exact(&mut payload)?;
    let mut cursor = Cursor::new(payload.as_slice());
    let value = bincode::deserialize_from(&mut cursor)
        .map_err(|error| io::Error::new(ErrorKind::InvalidData, error))?;
    if cursor.position() != length as u64 {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            "historical-runtime frame has trailing bytes",
        ));
    }
    Ok(Some(value))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frame_round_trip_and_clean_eof() {
        let request = Request {
            id: 7,
            body: RequestBody::Ping,
        };
        let mut bytes = Vec::new();
        write_frame(&mut bytes, &request).unwrap();
        let mut input = bytes.as_slice();
        assert_eq!(read_frame(&mut input).unwrap(), Some(request));
        assert_eq!(read_frame::<Request, _>(&mut input).unwrap(), None);
    }

    /// This exact fixture is emitted under both protocol feature builds.  It
    /// pins the shared subset of bincode 1.2.1 and 1.3.3 used by the IPC.
    #[test]
    fn ping_frame_has_cross_version_golden_encoding() {
        let request = Request {
            id: 7,
            body: RequestBody::Ping,
        };
        let mut bytes = Vec::new();
        write_frame(&mut bytes, &request).unwrap();
        assert_eq!(
            bytes,
            vec![
                12, 0, 0, 0, // frame length
                7, 0, 0, 0, 0, 0, 0, 0, // request id
                4, 0, 0, 0, // RequestBody::Ping
            ]
        );
    }

    #[test]
    fn snapshot_export_request_round_trips() {
        let request = Request {
            id: 9,
            body: RequestBody::ExportSnapshot {
                slot: 619_848,
                output_directory: "/tmp/snapshots".to_string(),
                expected_accounts_hash: vec![7; 32],
            },
        };
        let mut bytes = Vec::new();
        write_frame(&mut bytes, &request).unwrap();
        let mut input = bytes.as_slice();
        assert_eq!(read_frame(&mut input).unwrap(), Some(request));
    }

    #[test]
    fn snapshot_export_response_round_trips_with_worker_digest() {
        let response = Response {
            request_id: 10,
            body: ResponseBody::SnapshotExported(SnapshotExport {
                slot: 619_848,
                archive_path: "/tmp/snapshots/snapshot.tar.bz2".to_string(),
                accounts_hash: vec![7; 32],
                archive_size: 123,
                archive_sha256: vec![8; 32],
            }),
        };
        let mut bytes = Vec::new();
        write_frame(&mut bytes, &response).unwrap();
        let mut input = bytes.as_slice();
        assert_eq!(read_frame(&mut input).unwrap(), Some(response));
    }

    #[test]
    fn rejects_oversized_frame_before_allocating_payload() {
        let mut bytes = ((MAX_FRAME_BYTES as u32) + 1).to_le_bytes().to_vec();
        bytes.extend_from_slice(&[0; 8]);
        let error = read_frame::<Request, _>(&mut bytes.as_slice()).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidData);
    }

    #[test]
    fn rejects_trailing_bytes() {
        let mut bytes = bincode::serialize(&Request {
            id: 1,
            body: RequestBody::Ping,
        })
        .unwrap();
        bytes.push(0xff);
        let mut framed = (bytes.len() as u32).to_le_bytes().to_vec();
        framed.extend_from_slice(&bytes);
        assert!(read_frame::<Request, _>(&mut framed.as_slice()).is_err());
    }
}
