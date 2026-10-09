//! Derive narrowly scoped recovery evidence from a private Horizon archive.
//!
//! This command reads only independently decodable edge buckets. It never
//! mutates the archive, creates a canonical checksum sidecar, or talks to R2.
//! A recovered segment manifest still has to pass the complete archive reread
//! performed by `jetstreamer-segment-recover`.

use {
    jetstreamer_horizon::{
        account_updates::AccountUpdateView,
        archive::{
            ArchiveReader, BlockNotification, Consumption, EntryRecord, EpochMeta, SlotKind,
            SlotVisitor,
        },
        transactions::Transaction,
    },
    jetstreamer_node::archive_checksum::{
        archive_file_identity, open_regular_nofollow, path_matches_archive_identity,
    },
    serde::Serialize,
    solana_hash::Hash,
    std::{
        collections::BTreeMap,
        env,
        fs::{self, File, OpenOptions},
        io::{BufReader, Write},
        os::unix::fs::OpenOptionsExt as _,
        path::{Path, PathBuf},
        process,
    },
};

const SCHEMA: &str = "horizon-private-archive-edge-evidence-v1";

const DECODER_STACK_BYTES: usize = 64 * 1024 * 1024;

#[derive(Clone, Debug, Eq, PartialEq)]
struct Arguments {
    archive: PathBuf,
    output: PathBuf,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct TerminalSlot {
    slot: u64,
    kind: SlotKind,
    blockhash: Option<Hash>,
    write_count: u64,
}

#[derive(Default)]
struct EdgeVisitor {
    current_slot: Option<u64>,
    current_kind: Option<SlotKind>,
    current_write_count: u64,
    first_write: Option<u64>,
    last_write: Option<u64>,
    terminal: Option<TerminalSlot>,
    error: Option<String>,
}

impl EdgeVisitor {
    fn accept_write(&mut self, slot: u64, write_version: u64) {
        if self.error.is_some() {
            return;
        }
        if self.current_slot != Some(slot) {
            self.error = Some(format!(
                "account update for slot {slot} arrived while decoding slot {:?}",
                self.current_slot
            ));
            return;
        }
        self.current_write_count = match self.current_write_count.checked_add(1) {
            Some(count) => count,
            None => {
                self.error = Some(format!("account-update count overflows at slot {slot}"));
                return;
            }
        };
        self.first_write = Some(
            self.first_write
                .map_or(write_version, |current| current.min(write_version)),
        );
        self.last_write = Some(
            self.last_write
                .map_or(write_version, |current| current.max(write_version)),
        );
    }

    fn finish(self) -> Result<Self, String> {
        if let Some(error) = &self.error {
            return Err(error.clone());
        }
        Ok(self)
    }
}

impl SlotVisitor for EdgeVisitor {
    fn on_slot_start(&mut self, slot: u64, kind: SlotKind) {
        if self.current_slot.is_some() && self.terminal.is_none() {
            self.error = Some("slot ended without a block notification".to_string());
            return;
        }
        self.current_slot = Some(slot);
        self.current_kind = Some(kind);
        self.current_write_count = 0;
        self.terminal = None;
    }

    fn on_epoch(&mut self, meta: &EpochMeta) {
        let slot = self.current_slot.unwrap_or(0);
        for (update, _) in meta.updates.iter() {
            self.accept_write(slot, update.write_version);
        }
    }

    fn on_pre_account_update(&mut self, slot: u64, update: &AccountUpdateView<'_>) {
        self.accept_write(slot, update.write_version);
    }

    fn on_transaction(&mut self, slot: u64, _tx_index: u32, transaction: &Transaction) {
        for (update, _) in transaction.iter_account_updates() {
            self.accept_write(slot, update.write_version);
        }
    }

    fn on_post_account_update(&mut self, slot: u64, update: &AccountUpdateView<'_>) {
        self.accept_write(slot, update.write_version);
    }

    fn on_block(&mut self, notification: &BlockNotification, _entries: &[EntryRecord]) {
        let slot = notification.slot();
        if self.current_slot != Some(slot) {
            self.error = Some(format!(
                "block notification for slot {slot} arrived while decoding slot {:?}",
                self.current_slot
            ));
            return;
        }
        let kind = match self.current_kind {
            Some(kind) => kind,
            None => {
                self.error = Some(format!("slot {slot} has no slot-kind callback"));
                return;
            }
        };
        let blockhash = match notification {
            BlockNotification::Block(meta) => Some(meta.blockhash),
            BlockNotification::Skipped(_) => None,
        };
        if (kind == SlotKind::Block) != blockhash.is_some() {
            self.error = Some(format!(
                "slot {slot} kind does not match its block notification"
            ));
            return;
        }
        self.terminal = Some(TerminalSlot {
            slot,
            kind,
            blockhash,
            write_count: self.current_write_count,
        });
    }

    fn consumption(&self) -> Consumption {
        Consumption::all()
            .without_account_update_data()
            .without_block_account_update_arenas()
    }
}

#[derive(Debug, Serialize)]
struct EpochEvidence {
    archive: PathBuf,
    archive_bytes: u64,
    output_start_slot: u64,
    observed_first_write_version: u64,
    terminal_slot: u64,
    terminal_kind: &'static str,
    observed_terminal_next_write_version: u64,
    derived_terminal_checkpoint_write_count: u64,
    observed_terminal_blockhash: String,
}

#[derive(Debug, Serialize)]
struct Evidence {
    schema: &'static str,
    evidence_scope: &'static str,
    #[serde(flatten)]
    epochs: BTreeMap<String, EpochEvidence>,
    archive_mutations: bool,
    sidecar_created: bool,
    r2_mutations: bool,
}

fn usage(program: &str) -> String {
    format!("usage: {program} <archive.jet> --output <private-evidence.json>")
}

fn parse_arguments(arguments: impl IntoIterator<Item = String>) -> Result<Arguments, String> {
    let mut archive = None;
    let mut output = None;
    let mut arguments = arguments.into_iter();
    while let Some(argument) = arguments.next() {
        match argument.as_str() {
            "--output" if output.is_none() => {
                output = Some(
                    arguments
                        .next()
                        .ok_or_else(|| "--output requires a path".to_string())?
                        .into(),
                );
            }
            value if value.starts_with('-') => {
                return Err(format!("unknown option: {value}"));
            }
            _ if archive.is_none() => archive = Some(argument.into()),
            _ => return Err("only one archive path is accepted".to_string()),
        }
    }
    Ok(Arguments {
        archive: archive.ok_or_else(|| "missing archive path".to_string())?,
        output: output.ok_or_else(|| "missing --output path".to_string())?,
    })
}

fn read_bucket(path: &Path, index: usize) -> Result<EdgeVisitor, String> {
    let file = open_regular_nofollow(path)
        .map_err(|error| format!("failed to open {}: {error}", path.display()))?;
    let mut reader = ArchiveReader::open(BufReader::with_capacity(16 << 20, file))
        .map_err(|error| format!("failed to open Horizon archive: {error}"))?;
    reader.verify_chain = true;
    let mut visitor = EdgeVisitor::default();
    reader
        .read_bucket(index, &mut visitor)
        .map_err(|error| format!("failed to decode bucket {index}: {error}"))?;
    visitor.finish()
}

fn collect(arguments: &Arguments) -> Result<Evidence, String> {
    let archive = fs::canonicalize(&arguments.archive).map_err(|error| {
        format!(
            "failed to resolve archive {}: {error}",
            arguments.archive.display()
        )
    })?;
    if archive != arguments.archive {
        return Err(format!(
            "archive path must already be absolute and canonical: {}",
            archive.display()
        ));
    }
    let file = open_regular_nofollow(&archive)
        .map_err(|error| format!("failed to open {}: {error}", archive.display()))?;
    let identity = archive_file_identity(&file)
        .map_err(|error| format!("failed to identify {}: {error}", archive.display()))?;
    let archive_bytes = file
        .metadata()
        .map_err(|error| format!("failed to inspect {}: {error}", archive.display()))?
        .len();
    let reader = ArchiveReader::open(BufReader::with_capacity(16 << 20, file))
        .map_err(|error| format!("failed to open Horizon archive: {error}"))?;
    let header = reader.header().clone();
    let bucket_count = reader.bucket_count();
    if bucket_count == 0 {
        return Err("archive has no buckets".to_string());
    }
    let expected_terminal = header
        .slot_start
        .checked_add(header.slot_count)
        .and_then(|end| end.checked_sub(1))
        .ok_or_else(|| "archive slot range is empty or overflows".to_string())?;
    drop(reader);

    let mut first_write = None;
    for index in 0..bucket_count {
        let visitor = read_bucket(&archive, index)?;
        if visitor.first_write.is_some() {
            first_write = visitor.first_write;
            break;
        }
    }
    let first_write =
        first_write.ok_or_else(|| "archive contains no account writes".to_string())?;

    let terminal_visitor = read_bucket(&archive, bucket_count - 1)?;
    let terminal = terminal_visitor
        .terminal
        .ok_or_else(|| "final bucket contains no terminal slot".to_string())?;
    if terminal.slot != expected_terminal {
        return Err(format!(
            "archive terminal slot is {}, expected {expected_terminal}",
            terminal.slot
        ));
    }
    if terminal.kind != SlotKind::Block {
        return Err(format!(
            "archive terminal slot {} is skipped, not a block checkpoint",
            terminal.slot
        ));
    }
    let blockhash = terminal
        .blockhash
        .ok_or_else(|| "terminal block has no blockhash".to_string())?;

    let mut last_write = terminal_visitor.last_write;
    if last_write.is_none() {
        for index in (0..bucket_count - 1).rev() {
            let visitor = read_bucket(&archive, index)?;
            if visitor.last_write.is_some() {
                last_write = visitor.last_write;
                break;
            }
        }
    }
    let terminal_next_write = last_write
        .ok_or_else(|| "archive contains no terminal write-version evidence".to_string())?
        .checked_add(1)
        .ok_or_else(|| "terminal write version overflows u64".to_string())?;
    if first_write >= terminal_next_write {
        return Err("archive write-version edge range is empty or inverted".to_string());
    }
    if !path_matches_archive_identity(&archive, identity)
        .map_err(|error| format!("failed to recheck {}: {error}", archive.display()))?
    {
        return Err("archive changed while collecting edge evidence".to_string());
    }

    let mut epochs = BTreeMap::new();
    epochs.insert(
        format!("epoch{}", header.epoch),
        EpochEvidence {
            archive,
            archive_bytes,
            output_start_slot: header.slot_start,
            observed_first_write_version: first_write,
            terminal_slot: terminal.slot,
            terminal_kind: "block",
            observed_terminal_next_write_version: terminal_next_write,
            derived_terminal_checkpoint_write_count: terminal.write_count,
            observed_terminal_blockhash: blockhash.to_string(),
        },
    );
    Ok(Evidence {
        schema: SCHEMA,
        evidence_scope: "independently-decoded-edge-buckets; complete-reread-required",
        epochs,
        archive_mutations: false,
        sidecar_created: false,
        r2_mutations: false,
    })
}

fn write_evidence(path: &Path, evidence: &Evidence) -> Result<(), String> {
    if !path.is_absolute() {
        return Err("evidence output path must be absolute".to_string());
    }
    let parent = path
        .parent()
        .ok_or_else(|| "evidence output has no parent directory".to_string())?;
    let canonical_parent = fs::canonicalize(parent).map_err(|error| {
        format!(
            "failed to resolve evidence directory {}: {error}",
            parent.display()
        )
    })?;
    if canonical_parent != parent {
        return Err(format!(
            "evidence directory must already be canonical: {}",
            canonical_parent.display()
        ));
    }
    let mut bytes = serde_json::to_vec_pretty(evidence)
        .map_err(|error| format!("failed to serialize evidence: {error}"))?;
    bytes.push(b'\n');
    let mut output = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
        .map_err(|error| format!("failed to create {}: {error}", path.display()))?;
    output
        .write_all(&bytes)
        .and_then(|()| output.sync_all())
        .map_err(|error| format!("failed to persist {}: {error}", path.display()))?;
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| format!("failed to sync {}: {error}", parent.display()))?;
    Ok(())
}

fn main() {
    let mut raw = env::args();
    let program = raw
        .next()
        .unwrap_or_else(|| "jetstreamer-archive-edge-evidence".to_string());
    let arguments = parse_arguments(raw).unwrap_or_else(|error| {
        eprintln!("error: {error}\n{}", usage(&program));
        process::exit(2);
    });
    let worker_arguments = arguments.clone();
    let evidence = std::thread::Builder::new()
        .name("archive-edge-evidence".to_string())
        .stack_size(DECODER_STACK_BYTES)
        .spawn(move || collect(&worker_arguments))
        .unwrap_or_else(|error| {
            eprintln!("error: failed to start archive decoder: {error}");
            process::exit(1);
        })
        .join()
        .unwrap_or_else(|_| {
            eprintln!("error: archive decoder panicked");
            process::exit(1);
        })
        .unwrap_or_else(|error| {
            eprintln!("error: {error}");
            process::exit(1);
        });
    write_evidence(&arguments.output, &evidence).unwrap_or_else(|error| {
        eprintln!("error: {error}");
        process::exit(1);
    });
    println!("wrote {}", arguments.output.display());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn arguments_require_one_archive_and_output() {
        assert_eq!(
            parse_arguments([
                "/private/epoch-151.jet".to_string(),
                "--output".to_string(),
                "/private/edge.json".to_string(),
            ])
            .unwrap(),
            Arguments {
                archive: "/private/epoch-151.jet".into(),
                output: "/private/edge.json".into(),
            }
        );
        assert!(parse_arguments(["/private/epoch-151.jet".to_string()]).is_err());
        assert!(
            parse_arguments([
                "/private/one.jet".to_string(),
                "/private/two.jet".to_string(),
                "--output".to_string(),
                "/private/edge.json".to_string(),
            ])
            .is_err()
        );
    }

    #[test]
    fn write_edges_track_minimum_maximum_and_count() {
        let mut visitor = EdgeVisitor {
            current_slot: Some(10),
            current_kind: Some(SlotKind::Block),
            ..EdgeVisitor::default()
        };
        visitor.accept_write(10, 12);
        visitor.accept_write(10, 10);
        visitor.accept_write(10, 11);
        assert_eq!(visitor.first_write, Some(10));
        assert_eq!(visitor.last_write, Some(12));
        assert_eq!(visitor.current_write_count, 3);
        assert!(visitor.finish().is_ok());
    }

    #[test]
    fn write_for_the_wrong_slot_fails_closed() {
        let mut visitor = EdgeVisitor {
            current_slot: Some(10),
            current_kind: Some(SlotKind::Block),
            ..EdgeVisitor::default()
        };
        visitor.accept_write(11, 10);
        assert!(visitor.finish().is_err());
    }
}
