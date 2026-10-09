//! Recover a missing private historical segment manifest from root-reviewed evidence.
//!
//! This command is deliberately narrow: it never creates a canonical checksum
//! sidecar, never replaces an existing segment manifest, and requires an exact
//! SHA-256-bound recovery plan plus every evidence file named by that plan.

use {
    jetstreamer_node::{
        archive_checksum::archive_checksum_path,
        segment_manifest::{
            HistoricalSegmentManifest, read_and_validate_segment_manifest, segment_manifest_path,
            sha256_hex_string, write_segment_manifest,
        },
    },
    serde::{Deserialize, Serialize},
    sha2::{Digest, Sha256},
    std::{
        env, fs,
        fs::{File, OpenOptions},
        io::{Read, Write},
        os::unix::fs::{MetadataExt as _, OpenOptionsExt as _, PermissionsExt as _},
        path::{Path, PathBuf},
        process,
    },
};

const PLAN_SCHEMA: &str = "horizon-private-segment-manifest-recovery-v1";
const RECEIPT_SCHEMA: &str = "horizon-private-segment-manifest-recovery-receipt-v1";

#[derive(Debug, Eq, PartialEq)]
struct Arguments {
    plan: PathBuf,
    expected_plan_sha256: String,
    receipt: PathBuf,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct EvidenceBinding {
    path: PathBuf,
    sha256: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileIdentity {
    bytes: u64,
    uid: u32,
    gid: u32,
    mode: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RecoveryPlan {
    schema: String,
    private_root: PathBuf,
    archive: PathBuf,
    archive_identity: FileIdentity,
    failure_evidence: EvidenceBinding,
    archive_edge_evidence: EvidenceBinding,
    journal_evidence: EvidenceBinding,
    full_verification_evidence: EvidenceBinding,
    manifest: HistoricalSegmentManifest,
    canonical_checksum_sidecar_absent: bool,
    existing_segment_manifest_absent: bool,
    diagnostic_only: bool,
    publication_authorized: bool,
    r2_mutations: bool,
}

fn read_json(path: &Path, label: &str) -> Result<serde_json::Value, String> {
    let bytes = fs::read(path)
        .map_err(|error| format!("failed to read {label} {}: {error}", path.display()))?;
    serde_json::from_slice(&bytes)
        .map_err(|error| format!("invalid {label} JSON {}: {error}", path.display()))
}

fn json_path<'a>(
    value: &'a serde_json::Value,
    pointer: &str,
    label: &str,
) -> Result<&'a serde_json::Value, String> {
    value
        .pointer(pointer)
        .ok_or_else(|| format!("{label} is missing JSON field {pointer}"))
}

fn json_str<'a>(
    value: &'a serde_json::Value,
    pointer: &str,
    label: &str,
) -> Result<&'a str, String> {
    json_path(value, pointer, label)?
        .as_str()
        .ok_or_else(|| format!("{label} JSON field {pointer} is not a string"))
}

fn json_u64(value: &serde_json::Value, pointer: &str, label: &str) -> Result<u64, String> {
    json_path(value, pointer, label)?
        .as_u64()
        .ok_or_else(|| format!("{label} JSON field {pointer} is not a u64"))
}

fn json_bool(value: &serde_json::Value, pointer: &str, label: &str) -> Result<bool, String> {
    json_path(value, pointer, label)?
        .as_bool()
        .ok_or_else(|| format!("{label} JSON field {pointer} is not a boolean"))
}

fn epoch_edge_evidence<'a>(
    value: &'a serde_json::Value,
    epoch: u64,
    label: &str,
) -> Result<&'a serde_json::Value, String> {
    json_path(value, &format!("/epoch{epoch}"), label)
}

fn validate_failure_evidence(value: &serde_json::Value, plan: &RecoveryPlan) -> Result<(), String> {
    let label = "failure evidence";
    let manifest = &plan.manifest;
    let expected_archive = plan.archive.to_string_lossy();
    if json_str(value, "/schema", label)? != "horizon-focused-replay-postarchive-failure-v1"
        || json_u64(value, "/epoch", label)? != manifest.epoch
        || !json_bool(value, "/diagnostic_only", label)?
        || json_str(value, "/archive_completion/archive", label)? != expected_archive
        || json_u64(value, "/archive_completion/bytes", label)? != plan.archive_identity.bytes
        || json_u64(value, "/terminal_checkpoint/slot", label)? != manifest.terminal.slot
        || json_str(value, "/terminal_checkpoint/bank_hash", label)? != manifest.terminal.bank_hash
        || json_str(value, "/terminal_checkpoint/accounts_hash", label)?
            != manifest.terminal.accounts_hash
        || json_u64(value, "/terminal_checkpoint/capitalization", label)?
            != manifest.terminal.capitalization
        || json_u64(value, "/terminal_checkpoint/transactions", label)?
            != manifest.terminal.transaction_count
        || json_u64(value, "/terminal_checkpoint/tick_height", label)?
            != manifest.terminal.tick_height
        || !json_bool(value, "/terminal_checkpoint/complete", label)?
        || json_str(value, "/deployed_binaries/worker_sha256", label)?
            != sha256_hex_string(&manifest.worker_executable_sha256)
        || !json_bool(value, "/evidence/segment_manifest_absent", label)?
        || !json_bool(value, "/evidence/canonical_checksum_sidecar_absent", label)?
        || json_bool(value, "/evidence/validator_started", label)?
        || json_bool(value, "/evidence/retirement_started", label)?
        || json_bool(value, "/r2_mutations", label)?
        || !json_str(value, "/failure/message", label)?
            .contains("acknowledged shutdown but did not exit")
    {
        return Err("failure evidence does not bind the recovery manifest".to_string());
    }
    Ok(())
}

fn validate_edge_evidence(value: &serde_json::Value, plan: &RecoveryPlan) -> Result<(), String> {
    let label = "archive edge evidence";
    let manifest = &plan.manifest;
    let expected_archive = plan.archive.to_string_lossy();
    let epoch = epoch_edge_evidence(value, manifest.epoch, label)?;
    if json_str(value, "/schema", label)? != "horizon-private-archive-edge-evidence-v1"
        || json_u64(epoch, "/archive_bytes", label)? != plan.archive_identity.bytes
        || json_str(epoch, "/archive", label)? != expected_archive
        || json_u64(epoch, "/output_start_slot", label)? != manifest.output_slot_start
        || json_u64(epoch, "/observed_first_write_version", label)?
            != manifest.emitted_raw_write_versions.start
        || json_u64(epoch, "/terminal_slot", label)? != manifest.terminal.slot
        || json_str(epoch, "/terminal_kind", label)? != "block"
        || json_u64(epoch, "/observed_terminal_next_write_version", label)?
            != manifest.emitted_raw_write_versions.end
        || json_u64(epoch, "/derived_terminal_checkpoint_write_count", label)?
            != manifest.terminal.write_count
        || json_str(epoch, "/observed_terminal_blockhash", label)?
            != manifest.terminal.last_blockhash
        || json_bool(value, "/archive_mutations", label)?
        || json_bool(value, "/sidecar_created", label)?
        || json_bool(value, "/r2_mutations", label)?
    {
        return Err("archive edge evidence does not bind the recovery manifest".to_string());
    }
    Ok(())
}

fn validate_journal_evidence(value: &serde_json::Value, plan: &RecoveryPlan) -> Result<(), String> {
    let label = "journal evidence";
    let manifest = &plan.manifest;
    if json_str(value, "/schema", label)? != "horizon-historical-journal-evidence-v1"
        || json_u64(value, "/epoch", label)? != manifest.epoch
        || json_u64(value, "/bootstrap/slot", label)? != manifest.bootstrap.slot
        || json_str(value, "/bootstrap/bank_hash", label)? != manifest.bootstrap.bank_hash
        || json_str(value, "/bootstrap/accounts_hash", label)? != manifest.bootstrap.accounts_hash
        || json_str(value, "/bootstrap/last_blockhash", label)? != manifest.bootstrap.last_blockhash
        || json_u64(value, "/bootstrap/capitalization", label)? != manifest.bootstrap.capitalization
        || json_u64(value, "/bootstrap/transactions", label)?
            != manifest.bootstrap.transaction_count
        || json_u64(value, "/bootstrap/tick_height", label)? != manifest.bootstrap.tick_height
        || !json_bool(value, "/bootstrap/complete", label)?
        || json_u64(value, "/bootstrap/write_count", label)? != manifest.bootstrap.write_count
        || json_u64(value, "/bootstrap/next_write_version", label)?
            != manifest.bootstrap.next_write_version
        || json_u64(value, "/terminal/slot", label)? != manifest.terminal.slot
        || json_str(value, "/terminal/bank_hash", label)? != manifest.terminal.bank_hash
        || json_str(value, "/terminal/accounts_hash", label)? != manifest.terminal.accounts_hash
        || json_u64(value, "/terminal/capitalization", label)? != manifest.terminal.capitalization
        || json_u64(value, "/terminal/transactions", label)? != manifest.terminal.transaction_count
        || json_u64(value, "/terminal/tick_height", label)? != manifest.terminal.tick_height
        || !json_bool(value, "/terminal/complete", label)?
        || !json_bool(value, "/archive_complete", label)?
        || json_bool(value, "/r2_mutations", label)?
    {
        return Err("journal evidence does not bind the recovery manifest".to_string());
    }
    canonical_sha256(
        json_str(value, "/whole_journal_sha256", label)?,
        "whole-journal-sha256",
    )?;
    Ok(())
}

fn validate_full_verification_evidence(
    value: &serde_json::Value,
    plan: &RecoveryPlan,
    private_root: &Path,
) -> Result<(), String> {
    let label = "full verification evidence";
    let manifest = &plan.manifest;
    let expected_archive = plan.archive.to_string_lossy();
    let verifier_sha256 = json_str(value, "/verifier/sha256", label)?;
    canonical_sha256(verifier_sha256, "full-verifier-sha256")?;
    let verifier_path = PathBuf::from(json_str(value, "/verifier/path", label)?);
    require_root_executable(&verifier_path, verifier_sha256, "full verifier")?;
    let log_path = PathBuf::from(json_str(value, "/log/path", label)?);
    if log_path == private_root || !log_path.starts_with(private_root) {
        return Err(format!(
            "full verification log is outside private root {}: {}",
            private_root.display(),
            log_path.display()
        ));
    }
    let log_sha256 = json_str(value, "/log/sha256", label)?;
    canonical_sha256(log_sha256, "full-verification-log-sha256")?;
    require_root_evidence(&log_path, log_sha256, "full verification log")?;
    let result_line = json_str(value, "/result/line", label)?;
    let invocation_id = json_str(value, "/unit/invocation_id", label)?;
    if invocation_id.len() != 32
        || !invocation_id
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err("full verification evidence has an invalid invocation ID".to_string());
    }
    let threads = json_u64(value, "/command/threads", label)?;
    if json_str(value, "/schema", label)? != "horizon-private-full-verification-v1"
        || json_str(value, "/status", label)? != "passed"
        || json_u64(value, "/epoch", label)? != manifest.epoch
        || json_str(value, "/archive/path", label)? != expected_archive
        || json_u64(value, "/archive/bytes", label)? != plan.archive_identity.bytes
        || json_u64(value, "/archive/uid", label)? != u64::from(plan.archive_identity.uid)
        || json_u64(value, "/archive/gid", label)? != u64::from(plan.archive_identity.gid)
        || json_str(value, "/archive/mode", label)? != plan.archive_identity.mode
        || json_str(value, "/unit/result", label)? != "success"
        || json_u64(value, "/unit/exec_main_status", label)? != 0
        || json_u64(value, "/unit/restarts", label)? != 0
        || !json_bool(value, "/command/full", label)?
        || !json_bool(value, "/command/internal_full", label)?
        || threads == 0
        || threads > 64
        || !result_line.starts_with("RESULT: OK (internal only):")
        || json_bool(value, "/archive_mutations", label)?
        || json_bool(value, "/sidecar_created", label)?
        || json_bool(value, "/r2_mutations", label)?
    {
        return Err("full verification evidence does not bind the recovery archive".to_string());
    }
    Ok(())
}

#[derive(Debug, Serialize)]
struct RecoveryReceipt {
    schema: &'static str,
    status: &'static str,
    plan: PathBuf,
    plan_sha256: String,
    archive: PathBuf,
    archive_bytes: u64,
    archive_uid: u32,
    archive_gid: u32,
    archive_mode: String,
    archive_sha256: String,
    manifest: PathBuf,
    manifest_bytes: u64,
    manifest_uid: u32,
    manifest_gid: u32,
    manifest_mode: String,
    epoch: u64,
    output_slot_start: u64,
    output_slot_count: u64,
    bootstrap_slot: u64,
    terminal_slot: u64,
    canonical_checksum_sidecar_absent: bool,
    diagnostic_only: bool,
    publication_authorized: bool,
    r2_mutations: bool,
}

fn usage(program: &str) -> String {
    format!("Usage: {program} --plan=PATH --expected-plan-sha256=HEX --receipt=PATH")
}

fn canonical_sha256(value: &str, option: &str) -> Result<String, String> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(format!(
            "--{option} must be exactly 64 lowercase hexadecimal characters"
        ));
    }
    Ok(value.to_owned())
}

fn take_once<T>(slot: &mut Option<T>, value: T, name: &str) -> Result<(), String> {
    if slot.replace(value).is_some() {
        return Err(format!("duplicate --{name} option"));
    }
    Ok(())
}

fn parse_arguments<I>(arguments: I) -> Result<Arguments, String>
where
    I: IntoIterator<Item = String>,
{
    let mut plan = None;
    let mut expected_plan_sha256 = None;
    let mut receipt = None;
    for argument in arguments {
        if let Some(value) = argument.strip_prefix("--plan=") {
            take_once(&mut plan, PathBuf::from(value), "plan")?;
        } else if let Some(value) = argument.strip_prefix("--expected-plan-sha256=") {
            take_once(
                &mut expected_plan_sha256,
                canonical_sha256(value, "expected-plan-sha256")?,
                "expected-plan-sha256",
            )?;
        } else if let Some(value) = argument.strip_prefix("--receipt=") {
            take_once(&mut receipt, PathBuf::from(value), "receipt")?;
        } else {
            return Err(format!("unknown argument {argument:?}"));
        }
    }
    let arguments = Arguments {
        plan: plan.ok_or_else(|| "missing --plan".to_string())?,
        expected_plan_sha256: expected_plan_sha256
            .ok_or_else(|| "missing --expected-plan-sha256".to_string())?,
        receipt: receipt.ok_or_else(|| "missing --receipt".to_string())?,
    };
    if !arguments.plan.is_absolute() || !arguments.receipt.is_absolute() {
        return Err("plan and receipt paths must be absolute".to_string());
    }
    if arguments.plan == arguments.receipt {
        return Err("plan and receipt paths must differ".to_string());
    }
    Ok(arguments)
}

fn sha256_file(path: &Path) -> Result<String, String> {
    let mut file = File::open(path)
        .map_err(|error| format!("failed to open {} for hashing: {error}", path.display()))?;
    let mut digest = Sha256::new();
    let mut buffer = [0u8; 128 * 1024];
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|error| format!("failed to hash {}: {error}", path.display()))?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    Ok(format!("{:x}", digest.finalize()))
}

fn require_root_evidence(path: &Path, expected_sha256: &str, label: &str) -> Result<(), String> {
    if !path.is_absolute() {
        return Err(format!("{label} path must be absolute: {}", path.display()));
    }
    let metadata = fs::symlink_metadata(path)
        .map_err(|error| format!("failed to inspect {label} {}: {error}", path.display()))?;
    if !metadata.file_type().is_file()
        || metadata.uid() != 0
        || metadata.nlink() != 1
        || metadata.permissions().mode() & 0o077 != 0
        || metadata.permissions().mode() & 0o6000 != 0
    {
        return Err(format!(
            "{label} must be a singly linked root-owned owner-only regular file: {}",
            path.display()
        ));
    }
    let actual = sha256_file(path)?;
    if actual != expected_sha256 {
        return Err(format!(
            "{label} SHA-256 mismatch: expected {expected_sha256}, got {actual}"
        ));
    }
    Ok(())
}

fn require_root_executable(path: &Path, expected_sha256: &str, label: &str) -> Result<(), String> {
    if !path.is_absolute() {
        return Err(format!("{label} path must be absolute: {}", path.display()));
    }
    let canonical = fs::canonicalize(path)
        .map_err(|error| format!("failed to resolve {label} {}: {error}", path.display()))?;
    if canonical != path {
        return Err(format!(
            "{label} path must already be canonical: {}",
            path.display()
        ));
    }
    let metadata = fs::symlink_metadata(path)
        .map_err(|error| format!("failed to inspect {label} {}: {error}", path.display()))?;
    if !metadata.file_type().is_file()
        || metadata.uid() != 0
        || metadata.nlink() != 1
        || metadata.permissions().mode() & 0o022 != 0
        || metadata.permissions().mode() & 0o6000 != 0
        || metadata.permissions().mode() & 0o100 == 0
    {
        return Err(format!(
            "{label} must be a singly linked, root-owned, non-writable executable: {}",
            path.display()
        ));
    }
    let actual = sha256_file(path)?;
    if actual != expected_sha256 {
        return Err(format!(
            "{label} SHA-256 mismatch: expected {expected_sha256}, got {actual}"
        ));
    }
    Ok(())
}

fn require_absent(path: &Path, label: &str) -> Result<(), String> {
    match fs::symlink_metadata(path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(format!(
            "failed to inspect {label} {}: {error}",
            path.display()
        )),
        Ok(_) => Err(format!("{label} already exists: {}", path.display())),
    }
}

fn file_identity(path: &Path, label: &str) -> Result<(fs::Metadata, String), String> {
    let metadata = fs::symlink_metadata(path)
        .map_err(|error| format!("failed to inspect {label} {}: {error}", path.display()))?;
    if !metadata.file_type().is_file()
        || metadata.nlink() != 1
        || metadata.permissions().mode() & 0o077 != 0
        || metadata.permissions().mode() & 0o6000 != 0
    {
        return Err(format!(
            "{label} must be a singly linked owner-only regular file: {}",
            path.display()
        ));
    }
    let mode = format!("{:04o}", metadata.permissions().mode() & 0o7777);
    Ok((metadata, mode))
}

fn sync_directory(path: &Path) -> Result<(), String> {
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| format!("failed to sync directory {}: {error}", path.display()))
}

fn write_receipt(path: &Path, receipt: &RecoveryReceipt) -> Result<(), String> {
    if !path.is_absolute() {
        return Err("receipt path must be absolute".to_string());
    }
    let parent = path
        .parent()
        .ok_or_else(|| "receipt has no parent directory".to_string())?;
    let parent_metadata = fs::symlink_metadata(parent).map_err(|error| {
        format!(
            "failed to inspect receipt parent {}: {error}",
            parent.display()
        )
    })?;
    if !parent_metadata.file_type().is_dir() || parent_metadata.permissions().mode() & 0o077 != 0 {
        return Err(format!(
            "receipt parent must be an owner-only real directory: {}",
            parent.display()
        ));
    }
    let mut output = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW)
        .open(path)
        .map_err(|error| format!("failed to create receipt {}: {error}", path.display()))?;
    serde_json::to_writer_pretty(&mut output, receipt)
        .map_err(|error| format!("failed to encode recovery receipt: {error}"))?;
    output
        .write_all(b"\n")
        .and_then(|()| output.flush())
        .and_then(|()| output.sync_all())
        .map_err(|error| format!("failed to persist receipt {}: {error}", path.display()))?;
    sync_directory(parent)
}

fn recover(arguments: &Arguments) -> Result<RecoveryReceipt, String> {
    require_root_evidence(
        &arguments.plan,
        &arguments.expected_plan_sha256,
        "recovery plan",
    )?;
    require_absent(&arguments.receipt, "recovery receipt")?;
    let plan_bytes = fs::read(&arguments.plan)
        .map_err(|error| format!("failed to read recovery plan: {error}"))?;
    let plan: RecoveryPlan = serde_json::from_slice(&plan_bytes)
        .map_err(|error| format!("invalid recovery plan JSON: {error}"))?;
    if plan.schema != PLAN_SCHEMA
        || !plan.canonical_checksum_sidecar_absent
        || !plan.existing_segment_manifest_absent
        || !plan.diagnostic_only
        || plan.publication_authorized
        || plan.r2_mutations
    {
        return Err("recovery plan does not preserve private fail-closed semantics".to_string());
    }
    let private_root = fs::canonicalize(&plan.private_root).map_err(|error| {
        format!(
            "failed to resolve private root {}: {error}",
            plan.private_root.display()
        )
    })?;
    if private_root != plan.private_root {
        return Err(format!(
            "private root path is not exact and canonical: {}",
            plan.private_root.display()
        ));
    }
    let private_metadata = fs::symlink_metadata(&private_root)
        .map_err(|error| format!("failed to inspect private root: {error}"))?;
    if !private_metadata.file_type().is_dir()
        || private_metadata.permissions().mode() & 0o022 != 0
        || private_metadata.permissions().mode() & 0o6000 != 0
    {
        return Err("private root must be a real directory not writable by group/other".into());
    }
    for (path, label) in [
        (&arguments.plan, "recovery plan"),
        (&arguments.receipt, "recovery receipt"),
        (&plan.archive, "diagnostic archive"),
        (&plan.failure_evidence.path, "failure evidence"),
        (&plan.archive_edge_evidence.path, "archive edge evidence"),
        (&plan.journal_evidence.path, "journal evidence"),
        (
            &plan.full_verification_evidence.path,
            "full verification evidence",
        ),
    ] {
        if path == &private_root || !path.starts_with(&private_root) {
            return Err(format!(
                "{label} is outside private root {}: {}",
                private_root.display(),
                path.display()
            ));
        }
    }
    for (binding, label) in [
        (&plan.failure_evidence, "failure evidence"),
        (&plan.archive_edge_evidence, "archive edge evidence"),
        (&plan.journal_evidence, "journal evidence"),
        (
            &plan.full_verification_evidence,
            "full verification evidence",
        ),
    ] {
        canonical_sha256(&binding.sha256, label)?;
        require_root_evidence(&binding.path, &binding.sha256, label)?;
    }
    let failure = read_json(&plan.failure_evidence.path, "failure evidence")?;
    validate_failure_evidence(&failure, &plan)?;
    let edge = read_json(&plan.archive_edge_evidence.path, "archive edge evidence")?;
    validate_edge_evidence(&edge, &plan)?;
    let journal = read_json(&plan.journal_evidence.path, "journal evidence")?;
    validate_journal_evidence(&journal, &plan)?;
    let full_verification = read_json(
        &plan.full_verification_evidence.path,
        "full verification evidence",
    )?;
    validate_full_verification_evidence(&full_verification, &plan, &private_root)?;
    let archive = fs::canonicalize(&plan.archive).map_err(|error| {
        format!(
            "failed to resolve archive {}: {error}",
            plan.archive.display()
        )
    })?;
    if archive != plan.archive {
        return Err(format!(
            "archive path is not exact and canonical: {}",
            plan.archive.display()
        ));
    }
    let (archive_before, archive_mode) = file_identity(&archive, "diagnostic archive")?;
    if archive_before.len() != plan.archive_identity.bytes
        || archive_before.uid() != plan.archive_identity.uid
        || archive_before.gid() != plan.archive_identity.gid
        || archive_mode != plan.archive_identity.mode
    {
        return Err("diagnostic archive identity does not match recovery plan".to_string());
    }
    let segment_path = segment_manifest_path(&archive)
        .map_err(|error| format!("failed to derive segment path: {error}"))?;
    let checksum_path = archive_checksum_path(&archive)
        .map_err(|error| format!("failed to derive checksum path: {error}"))?;
    require_absent(&segment_path, "segment manifest")?;
    require_absent(&checksum_path, "canonical checksum sidecar")?;
    if plan.manifest.archive_sha256 != [0; 32] {
        return Err(
            "recovery plan manifest archive_sha256 must be the all-zero placeholder".into(),
        );
    }
    plan.manifest
        .validate()
        .map_err(|error| format!("recovery manifest invariants failed: {error}"))?;
    let expected_count = plan
        .manifest
        .terminal
        .slot
        .checked_sub(plan.manifest.output_slot_start)
        .and_then(|difference| difference.checked_add(1))
        .ok_or_else(|| "recovery manifest output range is invalid".to_string())?;
    if expected_count != plan.manifest.output_slot_count {
        return Err("recovery manifest terminal slot does not close its output range".to_string());
    }

    File::open(&archive)
        .and_then(|file| file.sync_all())
        .map_err(|error| format!("failed to sync diagnostic archive: {error}"))?;
    let (published_path, published) = write_segment_manifest(&archive, plan.manifest)
        .map_err(|error| format!("failed to validate and recover segment manifest: {error}"))?;
    if published_path != segment_path {
        return Err("segment recovery published an unexpected path".to_string());
    }
    let reread = read_and_validate_segment_manifest(&archive)
        .map_err(|error| format!("recovered segment manifest reread failed: {error}"))?;
    if reread != published {
        return Err("recovered manifest changed across durable reread".to_string());
    }
    require_absent(&checksum_path, "canonical checksum sidecar")?;
    let (archive_after, archive_mode_after) = file_identity(&archive, "diagnostic archive")?;
    if archive_before.dev() != archive_after.dev()
        || archive_before.ino() != archive_after.ino()
        || archive_before.len() != archive_after.len()
        || archive_before.mtime() != archive_after.mtime()
        || archive_before.mtime_nsec() != archive_after.mtime_nsec()
        || archive_before.ctime() != archive_after.ctime()
        || archive_before.ctime_nsec() != archive_after.ctime_nsec()
        || archive_mode != archive_mode_after
    {
        return Err("diagnostic archive changed during recovery".to_string());
    }
    let (manifest_metadata, manifest_mode) = file_identity(&segment_path, "segment manifest")?;
    Ok(RecoveryReceipt {
        schema: RECEIPT_SCHEMA,
        status: "recovered-and-independently-reread",
        plan: arguments.plan.clone(),
        plan_sha256: arguments.expected_plan_sha256.clone(),
        archive: archive.clone(),
        archive_bytes: archive_after.len(),
        archive_uid: archive_after.uid(),
        archive_gid: archive_after.gid(),
        archive_mode,
        archive_sha256: sha256_hex_string(&published.archive_sha256),
        manifest: segment_path,
        manifest_bytes: manifest_metadata.len(),
        manifest_uid: manifest_metadata.uid(),
        manifest_gid: manifest_metadata.gid(),
        manifest_mode,
        epoch: published.epoch,
        output_slot_start: published.output_slot_start,
        output_slot_count: published.output_slot_count,
        bootstrap_slot: published.bootstrap.slot,
        terminal_slot: published.terminal.slot,
        canonical_checksum_sidecar_absent: true,
        diagnostic_only: true,
        publication_authorized: false,
        r2_mutations: false,
    })
}

fn main() {
    let mut raw = env::args();
    let program = raw
        .next()
        .unwrap_or_else(|| "jetstreamer-segment-recover".to_string());
    let arguments = parse_arguments(raw).unwrap_or_else(|error| {
        eprintln!("error: {error}\n{}", usage(&program));
        process::exit(2);
    });
    match recover(&arguments) {
        Ok(receipt) => {
            if let Err(error) = write_receipt(&arguments.receipt, &receipt) {
                eprintln!("error: {error}");
                process::exit(1);
            }
            println!(
                "{}",
                serde_json::to_string_pretty(&receipt).expect("receipt is serializable")
            );
        }
        Err(error) => {
            eprintln!("error: {error}");
            process::exit(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_arguments() -> Vec<String> {
        vec![
            "--plan=/private/recovery.json".to_string(),
            format!("--expected-plan-sha256={}", "a".repeat(64)),
            "--receipt=/private/recovery-receipt.json".to_string(),
        ]
    }

    #[test]
    fn exact_arguments_are_required() {
        let parsed = parse_arguments(valid_arguments()).unwrap();
        assert_eq!(parsed.plan, Path::new("/private/recovery.json"));
        assert_eq!(parsed.receipt, Path::new("/private/recovery-receipt.json"));
        let mut duplicate = valid_arguments();
        duplicate.push("--plan=/different.json".to_string());
        assert!(
            parse_arguments(duplicate)
                .unwrap_err()
                .contains("duplicate --plan")
        );
        let mut relative = valid_arguments();
        relative[0] = "--plan=relative.json".to_string();
        assert!(
            parse_arguments(relative)
                .unwrap_err()
                .contains("must be absolute")
        );
    }

    #[test]
    fn plan_digest_is_strict_lowercase_hex() {
        assert!(canonical_sha256(&"a".repeat(64), "digest").is_ok());
        assert!(canonical_sha256(&"A".repeat(64), "digest").is_err());
        assert!(canonical_sha256(&"a".repeat(63), "digest").is_err());
    }

    #[test]
    fn archive_edge_evidence_is_selected_by_manifest_epoch() {
        let evidence = serde_json::json!({
            "epoch151": {"archive_bytes": 151},
            "epoch204": {"archive_bytes": 204},
        });
        let selected = epoch_edge_evidence(&evidence, 151, "archive edge evidence").unwrap();
        assert_eq!(
            json_u64(selected, "/archive_bytes", "archive edge evidence").unwrap(),
            151
        );
        assert!(epoch_edge_evidence(&evidence, 152, "archive edge evidence").is_err());
    }
}
