//! Audits an Old Faithful slot range for transactions with no status frame.

use {
    futures_util::FutureExt,
    jetstreamer_firehose::{
        epochs::epoch_to_slot_range,
        firehose::{
            OnBlockFn, OnEntryFn, OnErrorFn, OnRewardFn, OnStatsTrackingFn, TransactionData,
            firehose,
        },
    },
    serde::Serialize,
    std::{
        collections::BTreeSet,
        env, fs,
        ops::Range,
        path::{Path, PathBuf},
        process,
        sync::{
            Arc, Mutex,
            atomic::{AtomicU64, Ordering},
        },
        time::Instant,
    },
};

#[derive(Clone, Debug, Ord, PartialOrd, Eq, PartialEq, Serialize)]
struct MissingStatus {
    slot: u64,
    transaction_slot_index: usize,
    signature: String,
}

#[derive(Debug, Serialize)]
struct AuditReport {
    slot_start: u64,
    slot_end_exclusive: u64,
    threads: u64,
    transaction_notifications: u64,
    elapsed_seconds: f64,
    missing_statuses: Vec<MissingStatus>,
}

fn usage(program: &str) -> String {
    format!(
        "Usage: {program} <epoch|start:end-inclusive> [threads] [output.json]\n\
         Scans transaction metadata only; it does not execute transactions or produce archives."
    )
}

fn parse_range(argument: &str) -> Result<Range<u64>, String> {
    if let Some((start, end_inclusive)) = argument.split_once(':') {
        let start = start
            .parse::<u64>()
            .map_err(|error| format!("invalid start slot '{start}': {error}"))?;
        let end_inclusive = end_inclusive
            .parse::<u64>()
            .map_err(|error| format!("invalid end slot '{end_inclusive}': {error}"))?;
        if end_inclusive < start {
            return Err(format!(
                "end slot {end_inclusive} is before start slot {start}"
            ));
        }
        let end = end_inclusive
            .checked_add(1)
            .ok_or_else(|| "inclusive end slot cannot be u64::MAX".to_string())?;
        Ok(start..end)
    } else {
        let epoch = argument
            .parse::<u64>()
            .map_err(|error| format!("invalid epoch '{argument}': {error}"))?;
        let (start, end_inclusive) = epoch_to_slot_range(epoch);
        Ok(start..end_inclusive.saturating_add(1))
    }
}

fn write_report(path: &Path, bytes: &[u8]) -> Result<(), String> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let file_name = path
        .file_name()
        .ok_or_else(|| format!("output path has no file name: {}", path.display()))?;
    let temporary = parent.join(format!(
        ".{}.tmp-{}",
        file_name.to_string_lossy(),
        process::id()
    ));
    fs::write(&temporary, bytes)
        .map_err(|error| format!("failed to write {}: {error}", temporary.display()))?;
    fs::File::open(&temporary)
        .and_then(|file| file.sync_all())
        .map_err(|error| format!("failed to sync {}: {error}", temporary.display()))?;
    fs::rename(&temporary, path).map_err(|error| {
        format!(
            "failed to rename {} to {}: {error}",
            temporary.display(),
            path.display()
        )
    })?;
    fs::File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| format!("failed to sync {}: {error}", parent.display()))
}

fn main() {
    solana_logger::setup_with_default("info");
    let mut arguments = env::args();
    let program = arguments
        .next()
        .unwrap_or_else(|| "audit-missing-status".to_string());
    let Some(range_argument) = arguments.next() else {
        eprintln!("{}", usage(&program));
        process::exit(2);
    };
    let slot_range = match parse_range(&range_argument) {
        Ok(range) => range,
        Err(error) => {
            eprintln!("error: {error}\n{}", usage(&program));
            process::exit(2);
        }
    };
    let threads = match arguments.next() {
        Some(raw) => match raw.parse::<u64>() {
            Ok(threads) if threads > 0 => threads,
            _ => {
                eprintln!(
                    "error: threads must be a positive integer\n{}",
                    usage(&program)
                );
                process::exit(2);
            }
        },
        None => 16,
    };
    let output_path = arguments.next().map(PathBuf::from);
    if arguments.next().is_some() {
        eprintln!("error: too many arguments\n{}", usage(&program));
        process::exit(2);
    }

    let missing = Arc::new(Mutex::new(BTreeSet::new()));
    let transaction_notifications = Arc::new(AtomicU64::new(0));
    let started = Instant::now();
    let runtime = tokio::runtime::Runtime::new().expect("create Tokio runtime");
    let result = runtime.block_on(firehose(
        threads,
        false,
        false,
        None,
        slot_range.clone(),
        None::<OnBlockFn>,
        Some({
            let missing = Arc::clone(&missing);
            let transaction_notifications = Arc::clone(&transaction_notifications);
            move |_thread_id: usize, transaction: TransactionData| {
                let missing = Arc::clone(&missing);
                let transaction_notifications = Arc::clone(&transaction_notifications);
                async move {
                    transaction_notifications.fetch_add(1, Ordering::Relaxed);
                    if !transaction.status_meta_available {
                        missing
                            .lock()
                            .expect("missing-status lock poisoned")
                            .insert(MissingStatus {
                                slot: transaction.slot,
                                transaction_slot_index: transaction.transaction_slot_index,
                                signature: transaction.signature.to_string(),
                            });
                    }
                    Ok(())
                }
                .boxed()
            }
        }),
        None::<OnEntryFn>,
        None::<OnRewardFn>,
        None::<OnErrorFn>,
        None::<OnStatsTrackingFn>,
        None,
    ));
    if let Err((error, slot)) = result {
        eprintln!("error: audit failed at slot {slot}: {error}");
        process::exit(1);
    }

    let report = AuditReport {
        slot_start: slot_range.start,
        slot_end_exclusive: slot_range.end,
        threads,
        transaction_notifications: transaction_notifications.load(Ordering::Relaxed),
        elapsed_seconds: started.elapsed().as_secs_f64(),
        missing_statuses: missing
            .lock()
            .expect("missing-status lock poisoned")
            .iter()
            .cloned()
            .collect(),
    };
    let mut bytes = serde_json::to_vec_pretty(&report).expect("serialize audit report");
    bytes.push(b'\n');
    match output_path {
        Some(path) => {
            if let Err(error) = write_report(&path, &bytes) {
                eprintln!("error: {error}");
                process::exit(1);
            }
            println!("wrote {}", path.display());
        }
        None => print!("{}", String::from_utf8_lossy(&bytes)),
    }
}
