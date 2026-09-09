mod leader_schedule;
mod runtime;
mod snapshot;

use jetstreamer_historical_protocol::{
    read_frame, write_frame, BackendQualification, Handshake, Request, RequestBody, Response,
    ResponseBody, WorkerError, WorkerErrorCode, MAINNET_GENESIS_HASH, PROTOCOL_VERSION,
};
use runtime::RuntimeState;
use std::io;

const BACKEND_ID: &str = "solana-v1.0.24";
const SOLANA_TAG: &str = "v1.0.24";
const SOLANA_COMMIT: &str = "a93915f1bddb73480f86fc09f487315ae191897d";

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
        let result = self.handle_body(request.body);
        match result {
            Ok((body, shutdown)) => (Response { request_id, body }, shutdown),
            Err(error) => (
                Response {
                    request_id,
                    body: ResponseBody::Error(error),
                },
                false,
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
            RequestBody::ProcessEntries(_) => {
                self.require_hello()?;
                Err(worker_error(
                    WorkerErrorCode::UnsupportedOperation,
                    "entry batches are supported only by the Solana v1.0.7 worker".to_string(),
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
    let code = if message.starts_with("entry slot") {
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
        let (response, shutdown) = worker.handle(request);
        write_frame(&mut output, &response)?;
        if shutdown {
            break;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn handshake_is_explicitly_unqualified_candidate() {
        let handshake = handshake();
        assert_eq!(handshake.protocol_version, PROTOCOL_VERSION);
        assert_eq!(handshake.backend_id, "solana-v1.0.24");
        assert_eq!(
            handshake.solana_commit,
            "a93915f1bddb73480f86fc09f487315ae191897d"
        );
        assert_eq!(
            handshake.rust_toolchain,
            "rustc 1.43.0 (4fb7144ed 2020-04-20)"
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
