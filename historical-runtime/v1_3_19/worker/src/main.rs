mod leader_schedule;
mod poh_backend;
mod runtime;
mod snapshot;

use jetstreamer_historical_protocol::{
    read_frame, write_frame, AccountWrite, BackendQualification, EntryProcessed,
    EntryProcessedChunk, EntryProcessedChunkBody, EntryRequest, Handshake, Request, RequestBody,
    Response, ResponseBody, TransactionOutcome, WorkerError, WorkerErrorCode, MAINNET_GENESIS_HASH,
    MAX_FRAME_BYTES, PROTOCOL_VERSION,
};
use runtime::{ProcessEntriesError, RuntimeState};
use solana_sdk::system_instruction::MAX_PERMITTED_DATA_LENGTH;
use std::{io, mem};

const BACKEND_ID: &str = "solana-v1.3.19";
const SOLANA_TAG: &str = "v1.3.19";
const SOLANA_COMMIT: &str = "15a49d75086f95573ad319b22e4843639bdf2169";
const ENTRY_CHUNK_TARGET_BYTES: u64 = (MAX_FRAME_BYTES / 2) as u64;
const ENTRY_CHUNK_OVERHEAD_BYTES: u64 = 256;

struct Worker {
    hello_complete: bool,
    state: Option<RuntimeState>,
}

impl Worker {
    fn new() -> Self {
        Self {
            hello_complete: false,
            state: None,
        }
    }

    fn handle(&mut self, request: Request) -> (Response, bool) {
        let request_id = request.id;
        let fatal_on_error = matches!(
            &request.body,
            RequestBody::ProcessEntry(_) | RequestBody::ProcessEntries(_)
        );
        let result = self.handle_body(request.body);
        match result {
            Ok((body, shutdown)) => (Response { request_id, body }, shutdown),
            Err(error) => (
                Response {
                    request_id,
                    body: ResponseBody::Error(error),
                },
                fatal_on_error,
            ),
        }
    }

    fn handle_body(&mut self, body: RequestBody) -> Result<(ResponseBody, bool), WorkerError> {
        match body {
            RequestBody::Hello { protocol_version } => {
                if protocol_version != PROTOCOL_VERSION {
                    return Err(worker_error(
                        WorkerErrorCode::ProtocolVersionMismatch,
                        format!(
                            "parent protocol version {} does not match worker version {}",
                            protocol_version, PROTOCOL_VERSION
                        ),
                    ));
                }
                self.hello_complete = true;
                Ok((ResponseBody::Handshake(handshake()), false))
            }
            RequestBody::Initialize {
                ledger_path,
                initial_state,
                scratch_root,
            } => {
                self.require_hello()?;
                if self.state.is_some() {
                    return Err(worker_error(
                        WorkerErrorCode::AlreadyInitialized,
                        "worker is already initialized".to_string(),
                    ));
                }
                let (state, initialized) =
                    RuntimeState::initialize(&ledger_path, &initial_state, scratch_root.as_deref())
                        .map_err(|message| {
                            worker_error(WorkerErrorCode::InvalidGenesis, message)
                        })?;
                self.state = Some(state);
                Ok((ResponseBody::Initialized(initialized), false))
            }
            RequestBody::ProcessEntry(entry) => {
                self.require_hello()?;
                let state = self.state.as_mut().ok_or_else(|| {
                    worker_error(
                        WorkerErrorCode::NotInitialized,
                        "initialize the worker before replay".to_string(),
                    )
                })?;
                let processed = state.process_entry(entry).map_err(classify_runtime_error)?;
                Ok((ResponseBody::EntryProcessed(processed), false))
            }
            RequestBody::ProcessEntries(entries) => {
                let _ = entries;
                Err(worker_error(
                    WorkerErrorCode::Internal,
                    "ProcessEntries must use the streaming response path".to_string(),
                ))
            }
            RequestBody::FreezeCheckpoint { slot } => {
                self.require_hello()?;
                let state = self.state.as_mut().ok_or_else(|| {
                    worker_error(
                        WorkerErrorCode::NotInitialized,
                        "initialize the worker before checkpointing".to_string(),
                    )
                })?;
                let checkpoint = state
                    .freeze_checkpoint(slot)
                    .map_err(|message| worker_error(WorkerErrorCode::Runtime, message))?;
                Ok((ResponseBody::Checkpoint(checkpoint), false))
            }
            RequestBody::ExportSnapshot { .. } => {
                self.require_hello()?;
                Err(worker_error(
                    WorkerErrorCode::UnsupportedOperation,
                    "snapshot export is supported only by the Solana v1.0.7 worker".to_string(),
                ))
            }
            RequestBody::Ping => Ok((ResponseBody::Pong, false)),
            RequestBody::Shutdown => Ok((ResponseBody::ShuttingDown, true)),
        }
    }

    fn require_hello(&self) -> Result<(), WorkerError> {
        if self.hello_complete {
            Ok(())
        } else {
            Err(worker_error(
                WorkerErrorCode::ProtocolVersionMismatch,
                "Hello must be the first stateful request".to_string(),
            ))
        }
    }

    fn process_entry_batch<W: io::Write>(
        &mut self,
        request_id: u64,
        entries: Vec<EntryRequest>,
        output: &mut W,
    ) -> io::Result<bool> {
        if let Err(error) = self.require_hello() {
            write_error(output, request_id, error)?;
            return Ok(false);
        }
        let state = match self.state.as_mut() {
            Some(state) => state,
            None => {
                write_error(
                    output,
                    request_id,
                    worker_error(
                        WorkerErrorCode::NotInitialized,
                        "initialize the worker before replay".to_string(),
                    ),
                )?;
                return Ok(false);
            }
        };
        let result = state.process_entries_with(entries, |processed| {
            write_processed_entry(output, request_id, processed)
        });
        match result {
            Ok(()) => Ok(false),
            Err(ProcessEntriesError::Runtime(message)) => {
                write_error(output, request_id, classify_runtime_error(message))?;
                // A commit-path error may follow earlier bank mutations and
                // response chunks. Never accept another request afterward.
                Ok(true)
            }
            // A framing or pipe error after mutation terminates main without
            // attempting to continue on an unknowable session.
            Err(ProcessEntriesError::Emit(error)) => Err(error),
        }
    }
}

fn handshake() -> Handshake {
    Handshake {
        protocol_version: PROTOCOL_VERSION,
        backend_id: BACKEND_ID.to_string(),
        qualification: BackendQualification::Candidate,
        solana_tag: SOLANA_TAG.to_string(),
        solana_commit: SOLANA_COMMIT.to_string(),
        rust_toolchain: env!("HISTORICAL_RUSTC_VERSION").to_string(),
        target: env!("HISTORICAL_BUILD_TARGET").to_string(),
        required_genesis_hash: MAINNET_GENESIS_HASH.to_string(),
    }
}

fn classify_runtime_error(message: String) -> WorkerError {
    let code = if message.starts_with("entry batch") {
        WorkerErrorCode::InvalidRequest
    } else if message.starts_with("entry slot") {
        WorkerErrorCode::NonMonotonicSlot
    } else if message.starts_with("entry index") {
        WorkerErrorCode::InvalidEntryOrder
    } else if message.contains("leader schedule") || message.contains("epoch vote accounts") {
        WorkerErrorCode::MissingLeaderSchedule
    } else if message.starts_with("failed to decode transaction") {
        WorkerErrorCode::TransactionDecode
    } else {
        WorkerErrorCode::Runtime
    };
    worker_error(code, message)
}

fn worker_error(code: WorkerErrorCode, message: String) -> WorkerError {
    WorkerError { code, message }
}

fn main() -> io::Result<()> {
    let stdin = io::stdin();
    let stdout = io::stdout();
    let mut input = stdin.lock();
    let mut output = stdout.lock();
    let mut worker = Worker::new();
    while let Some(request) = read_frame::<Request, _>(&mut input)? {
        let request_id = request.id;
        let shutdown = match request.body {
            RequestBody::ProcessEntries(entries) => {
                worker.process_entry_batch(request_id, entries, &mut output)?
            }
            body => {
                let (response, shutdown) = worker.handle(Request {
                    id: request_id,
                    body,
                });
                write_worker_response(&mut output, response)?;
                shutdown
            }
        };
        if shutdown {
            break;
        }
    }
    Ok(())
}

fn write_worker_response<W: io::Write>(output: &mut W, response: Response) -> io::Result<()> {
    let request_id = response.request_id;
    match response.body {
        ResponseBody::EntryProcessed(entry) => write_processed_entry(output, request_id, entry),
        body => write_frame(output, &Response { request_id, body }),
    }
}

fn write_error<W: io::Write>(
    output: &mut W,
    request_id: u64,
    error: WorkerError,
) -> io::Result<()> {
    write_frame(
        output,
        &Response {
            request_id,
            body: ResponseBody::Error(error),
        },
    )
}

fn write_processed_entry<W: io::Write>(
    output: &mut W,
    request_id: u64,
    entry: EntryProcessed,
) -> io::Result<()> {
    let EntryProcessed {
        slot,
        entry_index,
        outcomes,
        writes,
        tick_height,
        slot_complete,
        next_write_version,
    } = entry;
    let mut sequence = 0u32;
    write_outcome_chunks(
        output,
        request_id,
        slot,
        entry_index,
        &mut sequence,
        outcomes,
    )?;
    write_account_chunks(output, request_id, slot, entry_index, &mut sequence, writes)?;
    write_entry_chunk(
        output,
        request_id,
        EntryProcessedChunk {
            slot,
            entry_index,
            sequence,
            body: EntryProcessedChunkBody::End {
                tick_height,
                slot_complete,
                next_write_version,
            },
        },
    )
}

fn write_outcome_chunks<W: io::Write>(
    output: &mut W,
    request_id: u64,
    slot: u64,
    entry_index: u64,
    sequence: &mut u32,
    outcomes: Vec<TransactionOutcome>,
) -> io::Result<()> {
    let mut chunk = Vec::new();
    let mut bytes = ENTRY_CHUNK_OVERHEAD_BYTES;
    for outcome in outcomes {
        let item_bytes = serialized_size(&outcome)?;
        if !chunk.is_empty() && bytes.saturating_add(item_bytes) > ENTRY_CHUNK_TARGET_BYTES {
            flush_outcomes(output, request_id, slot, entry_index, sequence, &mut chunk)?;
            bytes = ENTRY_CHUNK_OVERHEAD_BYTES;
        }
        bytes = bytes.saturating_add(item_bytes);
        chunk.push(outcome);
    }
    if !chunk.is_empty() {
        flush_outcomes(output, request_id, slot, entry_index, sequence, &mut chunk)?;
    }
    Ok(())
}

fn flush_outcomes<W: io::Write>(
    output: &mut W,
    request_id: u64,
    slot: u64,
    entry_index: u64,
    sequence: &mut u32,
    chunk: &mut Vec<TransactionOutcome>,
) -> io::Result<()> {
    write_entry_chunk(
        output,
        request_id,
        EntryProcessedChunk {
            slot,
            entry_index,
            sequence: *sequence,
            body: EntryProcessedChunkBody::Outcomes(mem::replace(chunk, Vec::new())),
        },
    )?;
    *sequence = sequence
        .checked_add(1)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "entry chunk overflow"))?;
    Ok(())
}

fn write_account_chunks<W: io::Write>(
    output: &mut W,
    request_id: u64,
    slot: u64,
    entry_index: u64,
    sequence: &mut u32,
    writes: Vec<AccountWrite>,
) -> io::Result<()> {
    let mut chunk = Vec::new();
    let mut bytes = ENTRY_CHUNK_OVERHEAD_BYTES;
    for write in writes {
        if write.data.len() as u64 > MAX_PERMITTED_DATA_LENGTH {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "historical account write exceeds v1.3.19's maximum account-data length",
            ));
        }
        let item_bytes = serialized_size(&write)?;
        if item_bytes.saturating_add(ENTRY_CHUNK_OVERHEAD_BYTES) > ENTRY_CHUNK_TARGET_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "one historical account write exceeds the entry chunk limit",
            ));
        }
        if !chunk.is_empty() && bytes.saturating_add(item_bytes) > ENTRY_CHUNK_TARGET_BYTES {
            flush_writes(output, request_id, slot, entry_index, sequence, &mut chunk)?;
            bytes = ENTRY_CHUNK_OVERHEAD_BYTES;
        }
        bytes = bytes.saturating_add(item_bytes);
        chunk.push(write);
    }
    if !chunk.is_empty() {
        flush_writes(output, request_id, slot, entry_index, sequence, &mut chunk)?;
    }
    Ok(())
}

fn flush_writes<W: io::Write>(
    output: &mut W,
    request_id: u64,
    slot: u64,
    entry_index: u64,
    sequence: &mut u32,
    chunk: &mut Vec<AccountWrite>,
) -> io::Result<()> {
    write_entry_chunk(
        output,
        request_id,
        EntryProcessedChunk {
            slot,
            entry_index,
            sequence: *sequence,
            body: EntryProcessedChunkBody::Writes(mem::replace(chunk, Vec::new())),
        },
    )?;
    *sequence = sequence
        .checked_add(1)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "entry chunk overflow"))?;
    Ok(())
}

fn write_entry_chunk<W: io::Write>(
    output: &mut W,
    request_id: u64,
    chunk: EntryProcessedChunk,
) -> io::Result<()> {
    write_frame(
        output,
        &Response {
            request_id,
            body: ResponseBody::EntryProcessedChunk(chunk),
        },
    )
}

fn serialized_size<T: serde::Serialize>(value: &T) -> io::Result<u64> {
    bincode::serialized_size(value)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::convert::TryInto;

    fn processed_entry(entry_index: u64) -> jetstreamer_historical_protocol::EntryProcessed {
        jetstreamer_historical_protocol::EntryProcessed {
            slot: 3,
            entry_index,
            outcomes: Vec::new(),
            writes: Vec::new(),
            tick_height: entry_index + 1,
            slot_complete: false,
            next_write_version: 0,
        }
    }

    fn account_write(write_version: u64, data_len: usize) -> AccountWrite {
        AccountWrite {
            slot: 3,
            write_version,
            transaction_signature: None,
            pubkey: vec![write_version as u8; 32],
            lamports: 1,
            owner: vec![2; 32],
            executable: false,
            rent_epoch: 0,
            data: vec![write_version as u8; data_len],
            stored_hash: vec![3; 32],
        }
    }

    #[test]
    fn large_entry_writes_are_losslessly_split_below_the_frame_cap() {
        let mut entry = processed_entry(0);
        entry.writes = (0..7)
            .map(|version| account_write(version, MAX_PERMITTED_DATA_LENGTH as usize))
            .collect();
        entry.next_write_version = 7;
        let mut bytes = Vec::new();
        write_processed_entry(&mut bytes, 9, entry).unwrap();

        let mut input = bytes.as_slice();
        let mut versions = Vec::new();
        let mut sequences = Vec::new();
        let mut saw_end = false;
        while let Some(response) = read_frame::<Response, _>(&mut input).unwrap() {
            assert_eq!(response.request_id, 9);
            match response.body {
                ResponseBody::EntryProcessedChunk(chunk) => {
                    sequences.push(chunk.sequence);
                    match chunk.body {
                        EntryProcessedChunkBody::Writes(writes) => {
                            assert!(!saw_end);
                            for write in writes {
                                assert_eq!(write.data.len(), MAX_PERMITTED_DATA_LENGTH as usize);
                                versions.push(write.write_version);
                            }
                        }
                        EntryProcessedChunkBody::End {
                            next_write_version, ..
                        } => {
                            assert!(!saw_end);
                            assert_eq!(next_write_version, 7);
                            saw_end = true;
                        }
                        other => panic!("unexpected entry chunk: {:?}", other),
                    }
                }
                other => panic!("unexpected streamed response: {:?}", other),
            }
        }
        assert_eq!(versions, (0..7).collect::<Vec<_>>());
        assert_eq!(sequences, (0..sequences.len() as u32).collect::<Vec<_>>());
        assert!(saw_end);

        let mut framed = bytes.as_slice();
        while !framed.is_empty() {
            let length = u32::from_le_bytes(framed[..4].try_into().unwrap()) as usize;
            assert!(length <= MAX_FRAME_BYTES);
            framed = &framed[4 + length..];
        }
    }

    #[test]
    fn account_write_above_protocol_limit_is_rejected_before_output() {
        let mut entry = processed_entry(0);
        entry.writes = vec![account_write(0, MAX_PERMITTED_DATA_LENGTH as usize + 1)];
        entry.next_write_version = 1;
        let mut bytes = Vec::new();
        let error = write_processed_entry(&mut bytes, 9, entry).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("maximum account-data length"));
        assert!(bytes.is_empty());
    }

    #[test]
    fn handshake_is_explicitly_unqualified_candidate() {
        let handshake = handshake();
        assert_eq!(handshake.protocol_version, PROTOCOL_VERSION);
        assert_eq!(handshake.backend_id, "solana-v1.3.19");
        assert_eq!(
            handshake.solana_commit,
            "15a49d75086f95573ad319b22e4843639bdf2169"
        );
        assert_eq!(
            handshake.rust_toolchain,
            "rustc 1.45.1 (c367798cf 2020-07-26)"
        );
        assert_eq!(handshake.target, "x86_64-unknown-linux-gnu");
        assert_eq!(handshake.qualification, BackendQualification::Candidate);
    }

    #[test]
    fn stateful_requests_require_matching_hello() {
        let mut worker = Worker::new();
        let (response, _) = worker.handle(Request {
            id: 9,
            body: RequestBody::FreezeCheckpoint { slot: 0 },
        });
        match response.body {
            ResponseBody::Error(error) => {
                assert_eq!(error.code, WorkerErrorCode::ProtocolVersionMismatch)
            }
            other => panic!("unexpected response: {:?}", other),
        }
    }

    #[test]
    fn snapshot_export_fails_closed_as_unsupported() {
        let mut worker = Worker::new();
        worker.handle(Request {
            id: 1,
            body: RequestBody::Hello {
                protocol_version: PROTOCOL_VERSION,
            },
        });
        let (response, shutdown) = worker.handle(Request {
            id: 2,
            body: RequestBody::ExportSnapshot {
                slot: 619_848,
                output_directory: "/tmp".to_string(),
                expected_accounts_hash: vec![0; 32],
            },
        });
        assert!(!shutdown);
        match response.body {
            ResponseBody::Error(error) => {
                assert_eq!(error.code, WorkerErrorCode::UnsupportedOperation)
            }
            other => panic!("unexpected response: {:?}", other),
        }
    }
}
