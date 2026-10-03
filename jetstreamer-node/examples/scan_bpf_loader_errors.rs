//! Locate the historical BPF-loader error-format transition in Old Faithful.
//!
//! This scans source transaction statuses without executing them. The old
//! loader reported a VM failure as custom error `0x0b9f0002`; the replacement
//! reports `ProgramFailedToComplete`. Results are grouped by epoch and retain
//! the first and last transaction carrying each status.

use {
    jetstreamer_firehose::{
        SharedError, TransactionData,
        firehose::{self, Handler},
    },
    serde::Serialize,
    solana_transaction::{InstructionError, TransactionError},
    std::{
        collections::BTreeMap,
        future::Future,
        pin::Pin,
        sync::{Arc, Mutex},
    },
};

const LEGACY_BPF_VM_ERROR: u32 = 0x0b9f_0002;
const SLOTS_PER_EPOCH: u64 = 432_000;

#[derive(Clone, Debug, Serialize)]
struct Evidence {
    slot: u64,
    transaction_index: usize,
    signature: String,
}

#[derive(Clone, Debug, Default, Serialize)]
struct StatusEvidence {
    count: u64,
    first: Option<Evidence>,
    last: Option<Evidence>,
}

impl StatusEvidence {
    fn record(&mut self, evidence: Evidence) {
        self.count += 1;
        if self.first.is_none() {
            self.first = Some(evidence.clone());
        }
        self.last = Some(evidence);
    }
}

#[derive(Clone, Debug, Default, Serialize)]
struct EpochEvidence {
    legacy_custom: StatusEvidence,
    program_failed_to_complete: StatusEvidence,
}

type HandlerFuture = Pin<Box<dyn Future<Output = Result<(), SharedError>> + Send + 'static>>;

fn transaction_handler(
    epochs: Arc<Mutex<BTreeMap<u64, EpochEvidence>>>,
) -> impl Handler<TransactionData> {
    move |_thread_id, transaction| {
        let epochs = Arc::clone(&epochs);
        Box::pin(async move {
            if !transaction.status_meta_available {
                return Ok(());
            }
            let status = &transaction.transaction_status_meta.status;
            let kind = match status {
                Err(TransactionError::InstructionError(
                    _,
                    InstructionError::Custom(LEGACY_BPF_VM_ERROR),
                )) => 0,
                Err(TransactionError::InstructionError(
                    _,
                    InstructionError::ProgramFailedToComplete,
                )) => 1,
                _ => return Ok(()),
            };
            let evidence = Evidence {
                slot: transaction.slot,
                transaction_index: transaction.transaction_slot_index,
                signature: transaction.signature.to_string(),
            };
            let mut epochs = epochs.lock().expect("evidence mutex poisoned");
            let epoch = epochs
                .entry(transaction.slot / SLOTS_PER_EPOCH)
                .or_default();
            if kind == 0 {
                epoch.legacy_custom.record(evidence);
            } else {
                epoch.program_failed_to_complete.record(evidence);
            }
            Ok(())
        }) as HandlerFuture
    }
}

fn usage() -> ! {
    eprintln!("usage: scan_bpf_loader_errors <start-slot> <end-slot-exclusive> [firehose-threads]");
    std::process::exit(2);
}

#[tokio::main]
async fn main() {
    let arguments = std::env::args().skip(1).collect::<Vec<_>>();
    if !(2..=3).contains(&arguments.len()) {
        usage();
    }
    let start = arguments[0].parse::<u64>().unwrap_or_else(|_| usage());
    let end = arguments[1].parse::<u64>().unwrap_or_else(|_| usage());
    let threads = arguments
        .get(2)
        .map(|value| value.parse::<u64>().unwrap_or_else(|_| usage()))
        .unwrap_or(16)
        .max(1);
    if start >= end {
        usage();
    }

    let evidence = Arc::new(Mutex::new(BTreeMap::new()));
    firehose::firehose(
        threads,
        false,
        false,
        None,
        start..end,
        None::<firehose::OnBlockFn>,
        Some(transaction_handler(Arc::clone(&evidence))),
        None::<firehose::OnEntryFn>,
        None::<firehose::OnRewardFn>,
        None::<firehose::OnErrorFn>,
        None::<firehose::OnStatsTrackingFn>,
        None,
    )
    .await
    .unwrap_or_else(|(error, slot)| panic!("firehose failed at slot {slot}: {error}"));

    let evidence = evidence.lock().expect("evidence mutex poisoned");
    println!(
        "{}",
        serde_json::to_string_pretty(&*evidence).expect("serialize evidence")
    );
}
