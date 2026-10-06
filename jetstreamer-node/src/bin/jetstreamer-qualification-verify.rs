//! Read-only, fail-closed validation of a focused qualification artifact.

use {
    jetstreamer_node::{
        archive_checksum::archive_checksum_path,
        segment_manifest::{
            SegmentRuntimeAdmission, read_and_validate_segment_manifest, segment_manifest_path,
            sha256_hex_string,
        },
    },
    serde_json::json,
    std::{
        env, fs,
        os::unix::fs::{MetadataExt as _, PermissionsExt as _},
        path::{Path, PathBuf},
        process,
    },
};

#[derive(Debug, Eq, PartialEq)]
struct Arguments {
    archive: PathBuf,
    private_root: PathBuf,
    expected_epoch: u64,
    expected_output_start_slot: u64,
    expected_bootstrap_slot: u64,
    expected_terminal_slot: u64,
    expected_runtime_profile: String,
    expected_worker_sha256: String,
}

fn usage(program: &str) -> String {
    format!(
        r#"Usage: {program} ARCHIVE \
  --private-root=PATH \
  --expected-epoch=N \
  --expected-output-start-slot=SLOT \
  --expected-bootstrap-slot=SLOT \
  --expected-terminal-slot=SLOT \
  --expected-runtime-profile=NAME \
  --expected-worker-sha256=HEX"#
    )
}

fn take_once<T>(slot: &mut Option<T>, value: T, name: &str) -> Result<(), String> {
    if slot.replace(value).is_some() {
        return Err(format!("duplicate --{name} option"));
    }
    Ok(())
}

fn parse_u64(value: &str, name: &str) -> Result<u64, String> {
    value
        .parse()
        .map_err(|error| format!("invalid --{name} value {value:?}: {error}"))
}

fn validate_sha256(value: &str) -> Result<String, String> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(
            "--expected-worker-sha256 must be exactly 64 lowercase hexadecimal characters"
                .to_string(),
        );
    }
    Ok(value.to_owned())
}

fn parse_arguments<I>(arguments: I) -> Result<Arguments, String>
where
    I: IntoIterator<Item = String>,
{
    let mut archive = None;
    let mut private_root = None;
    let mut expected_epoch = None;
    let mut expected_output_start_slot = None;
    let mut expected_bootstrap_slot = None;
    let mut expected_terminal_slot = None;
    let mut expected_runtime_profile = None;
    let mut expected_worker_sha256 = None;

    for argument in arguments {
        if let Some(value) = argument.strip_prefix("--private-root=") {
            if value.is_empty() {
                return Err("--private-root must not be empty".to_string());
            }
            take_once(&mut private_root, PathBuf::from(value), "private-root")?;
        } else if let Some(value) = argument.strip_prefix("--expected-epoch=") {
            let value = parse_u64(value, "expected-epoch")?;
            take_once(&mut expected_epoch, value, "expected-epoch")?;
        } else if let Some(value) = argument.strip_prefix("--expected-output-start-slot=") {
            let value = parse_u64(value, "expected-output-start-slot")?;
            take_once(
                &mut expected_output_start_slot,
                value,
                "expected-output-start-slot",
            )?;
        } else if let Some(value) = argument.strip_prefix("--expected-bootstrap-slot=") {
            let value = parse_u64(value, "expected-bootstrap-slot")?;
            take_once(
                &mut expected_bootstrap_slot,
                value,
                "expected-bootstrap-slot",
            )?;
        } else if let Some(value) = argument.strip_prefix("--expected-terminal-slot=") {
            let value = parse_u64(value, "expected-terminal-slot")?;
            take_once(&mut expected_terminal_slot, value, "expected-terminal-slot")?;
        } else if let Some(value) = argument.strip_prefix("--expected-runtime-profile=") {
            if value.is_empty() {
                return Err("--expected-runtime-profile must not be empty".to_string());
            }
            take_once(
                &mut expected_runtime_profile,
                value.to_owned(),
                "expected-runtime-profile",
            )?;
        } else if let Some(value) = argument.strip_prefix("--expected-worker-sha256=") {
            let value = validate_sha256(value)?;
            take_once(&mut expected_worker_sha256, value, "expected-worker-sha256")?;
        } else if argument.starts_with('-') {
            return Err(format!("unknown option {argument:?}"));
        } else {
            take_once(&mut archive, PathBuf::from(argument), "archive")?;
        }
    }

    Ok(Arguments {
        archive: archive.ok_or_else(|| "missing ARCHIVE".to_string())?,
        private_root: private_root.ok_or_else(|| "missing --private-root".to_string())?,
        expected_epoch: expected_epoch.ok_or_else(|| "missing --expected-epoch".to_string())?,
        expected_output_start_slot: expected_output_start_slot
            .ok_or_else(|| "missing --expected-output-start-slot".to_string())?,
        expected_bootstrap_slot: expected_bootstrap_slot
            .ok_or_else(|| "missing --expected-bootstrap-slot".to_string())?,
        expected_terminal_slot: expected_terminal_slot
            .ok_or_else(|| "missing --expected-terminal-slot".to_string())?,
        expected_runtime_profile: expected_runtime_profile
            .ok_or_else(|| "missing --expected-runtime-profile".to_string())?,
        expected_worker_sha256: expected_worker_sha256
            .ok_or_else(|| "missing --expected-worker-sha256".to_string())?,
    })
}

fn require_private_regular(path: &Path, label: &str) -> Result<fs::Metadata, String> {
    let metadata = fs::symlink_metadata(path)
        .map_err(|error| format!("failed to inspect {label} {}: {error}", path.display()))?;
    if !metadata.file_type().is_file()
        || metadata.nlink() != 1
        || metadata.permissions().mode() & 0o077 != 0
        || metadata.permissions().mode() & 0o6000 != 0
    {
        return Err(format!(
            "{label} must be a singly linked owner-only regular file without set-ID bits: {}",
            path.display()
        ));
    }
    Ok(metadata)
}

fn require_private_root(path: &Path) -> Result<(), String> {
    let metadata = fs::symlink_metadata(path)
        .map_err(|error| format!("failed to inspect private root {}: {error}", path.display()))?;
    if !metadata.file_type().is_dir()
        || metadata.permissions().mode() & 0o022 != 0
        || metadata.permissions().mode() & 0o6000 != 0
    {
        return Err(format!(
            "private root must be a real directory, non-writable by group/other, and free of set-ID bits: {}",
            path.display()
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
        Ok(_) => Err(format!(
            "focused qualification {label} must be absent: {}",
            path.display()
        )),
    }
}

fn verify(arguments: &Arguments) -> Result<serde_json::Value, String> {
    require_private_root(&arguments.private_root)?;
    let private_root = fs::canonicalize(&arguments.private_root).map_err(|error| {
        format!(
            "failed to resolve private root {}: {error}",
            arguments.private_root.display()
        )
    })?;
    let archive = fs::canonicalize(&arguments.archive).map_err(|error| {
        format!(
            "failed to resolve archive {}: {error}",
            arguments.archive.display()
        )
    })?;
    if arguments.private_root != private_root {
        return Err(format!(
            "private root must be supplied as its exact canonical path: {}",
            private_root.display()
        ));
    }
    if arguments.archive != archive {
        return Err(format!(
            "archive must be supplied as its exact canonical path: {}",
            archive.display()
        ));
    }
    if !archive.starts_with(&private_root) || archive == private_root {
        return Err(format!(
            "archive is outside the required private root {}: {}",
            private_root.display(),
            archive.display()
        ));
    }

    let expected_name = format!(
        "epoch-{}-through-{}.jet",
        arguments.expected_epoch, arguments.expected_terminal_slot
    );
    if archive.file_name().and_then(|name| name.to_str()) != Some(expected_name.as_str()) {
        return Err(format!(
            "qualification archive filename must be {expected_name}: {}",
            archive.display()
        ));
    }

    let manifest_path = segment_manifest_path(&archive)
        .map_err(|error| format!("failed to resolve segment manifest path: {error}"))?;
    let checksum_path = archive_checksum_path(&archive)
        .map_err(|error| format!("failed to resolve checksum path: {error}"))?;
    let archive_before = require_private_regular(&archive, "qualification archive")?;
    let manifest_before = require_private_regular(&manifest_path, "qualification manifest")?;
    require_absent(&checksum_path, "canonical checksum sidecar")?;

    let manifest = read_and_validate_segment_manifest(&archive)
        .map_err(|error| format!("durable segment validation failed: {error}"))?;
    let expected_count = arguments
        .expected_terminal_slot
        .checked_sub(arguments.expected_output_start_slot)
        .and_then(|difference| difference.checked_add(1))
        .ok_or_else(|| "expected output slot range is invalid".to_string())?;
    let worker_sha256 = sha256_hex_string(&manifest.worker_executable_sha256);
    if manifest.epoch != arguments.expected_epoch
        || manifest.output_slot_start != arguments.expected_output_start_slot
        || manifest.output_slot_count != expected_count
        || manifest.bootstrap.slot != arguments.expected_bootstrap_slot
        || manifest.terminal.slot != arguments.expected_terminal_slot
        || manifest.runtime.runtime_profile != arguments.expected_runtime_profile
        || manifest.runtime.runtime_admission != SegmentRuntimeAdmission::Candidate
        || worker_sha256 != arguments.expected_worker_sha256
    {
        return Err(
            "validated segment manifest does not match the exact qualification expectations"
                .to_string(),
        );
    }

    let archive_after = require_private_regular(&archive, "qualification archive")?;
    let manifest_after = require_private_regular(&manifest_path, "qualification manifest")?;
    require_absent(&checksum_path, "canonical checksum sidecar")?;
    for (label, before, after) in [
        ("qualification archive", &archive_before, &archive_after),
        ("qualification manifest", &manifest_before, &manifest_after),
    ] {
        if before.dev() != after.dev()
            || before.ino() != after.ino()
            || before.len() != after.len()
            || before.mtime() != after.mtime()
            || before.mtime_nsec() != after.mtime_nsec()
            || before.ctime() != after.ctime()
            || before.ctime_nsec() != after.ctime_nsec()
        {
            return Err(format!("{label} changed during validation"));
        }
    }

    Ok(json!({
        "schema": "jetstreamer-focused-qualification-artifact-validation-v1",
        "validation": "pass",
        "archive": archive,
        "archive_bytes": archive_after.len(),
        "archive_mode": format!("{:04o}", archive_after.permissions().mode() & 0o7777),
        "archive_uid": archive_after.uid(),
        "archive_gid": archive_after.gid(),
        "archive_sha256": sha256_hex_string(&manifest.archive_sha256),
        "manifest": manifest_path,
        "manifest_bytes": manifest_after.len(),
        "manifest_mode": format!("{:04o}", manifest_after.permissions().mode() & 0o7777),
        "manifest_uid": manifest_after.uid(),
        "manifest_gid": manifest_after.gid(),
        "canonical_checksum_sidecar": checksum_path,
        "canonical_checksum_sidecar_absent": true,
        "epoch": manifest.epoch,
        "output_slot_start": manifest.output_slot_start,
        "output_slot_count": manifest.output_slot_count,
        "bootstrap_slot": manifest.bootstrap.slot,
        "terminal_slot": manifest.terminal.slot,
        "runtime_profile": manifest.runtime.runtime_profile,
        "runtime_revision": manifest.runtime.runtime_revision,
        "runtime_admission": "candidate",
        "worker_executable_sha256": worker_sha256,
    }))
}

fn main() {
    let mut arguments = env::args();
    let program = arguments
        .next()
        .unwrap_or_else(|| "jetstreamer-qualification-verify".to_string());
    let remaining: Vec<_> = arguments.collect();
    if remaining.iter().any(|argument| argument == "--help") {
        println!("{}", usage(&program));
        return;
    }
    let arguments = parse_arguments(remaining).unwrap_or_else(|error| {
        eprintln!("error: {error}\n{}", usage(&program));
        process::exit(2);
    });
    match verify(&arguments) {
        Ok(receipt) => println!(
            "{}",
            serde_json::to_string_pretty(&receipt).expect("validation receipt is serializable")
        ),
        Err(error) => {
            eprintln!("error: {error}");
            process::exit(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        jetstreamer_horizon::archive::{
            ArchiveProvenanceV1, ArchiveProvenanceV2, ArchiveWriter, ArchiveWriterConfig,
            BootstrapStateKind, RuntimeAdmission, TransactionMetadataPolicy,
        },
        jetstreamer_node::segment_manifest::{
            HistoricalSegmentManifest, SEGMENT_MANIFEST_SCHEMA_VERSION, SegmentCheckpointSummary,
            SegmentRuntimeIdentity, write_segment_manifest,
        },
        solana_hash::Hash,
        tempfile::TempDir,
    };

    const TEST_EPOCH: u64 = 1;
    const TEST_OUTPUT_START: u64 = 432_000;
    const TEST_BOOTSTRAP: u64 = 431_999;
    const TEST_TERMINAL: u64 = 432_001;
    const TEST_WORKER_DIGEST: [u8; 32] = [0x42; 32];

    fn valid_arguments() -> Vec<String> {
        vec![
            "/private/epoch-202-through-87695515.jet".to_string(),
            "--private-root=/private".to_string(),
            "--expected-epoch=202".to_string(),
            "--expected-output-start-slot=87264000".to_string(),
            "--expected-bootstrap-slot=87263434".to_string(),
            "--expected-terminal-slot=87695515".to_string(),
            "--expected-runtime-profile=solana-v1.6.16".to_string(),
            format!("--expected-worker-sha256={}", "a".repeat(64)),
        ]
    }

    #[test]
    fn exact_arguments_are_required() {
        let parsed = parse_arguments(valid_arguments()).unwrap();
        assert_eq!(parsed.expected_epoch, 202);
        assert_eq!(parsed.expected_terminal_slot, 87_695_515);

        let mut missing = valid_arguments();
        missing.retain(|argument| !argument.starts_with("--expected-bootstrap-slot="));
        assert!(
            parse_arguments(missing)
                .unwrap_err()
                .contains("missing --expected-bootstrap-slot")
        );

        let mut duplicate = valid_arguments();
        duplicate.push("--expected-epoch=203".to_string());
        assert!(
            parse_arguments(duplicate)
                .unwrap_err()
                .contains("duplicate --expected-epoch")
        );
    }

    #[test]
    fn worker_digest_must_be_canonical_lowercase_sha256() {
        assert!(validate_sha256(&"a".repeat(64)).is_ok());
        assert!(validate_sha256(&"A".repeat(64)).is_err());
        assert!(validate_sha256(&"a".repeat(63)).is_err());
        assert!(validate_sha256(&format!("{}g", "a".repeat(63))).is_err());
    }

    fn hash(byte: u8) -> Hash {
        Hash::new_from_array([byte; 32])
    }

    fn runtime_identity() -> SegmentRuntimeIdentity {
        SegmentRuntimeIdentity {
            generation_profile: "jetstreamer-node/historical-replay-v1".to_string(),
            runtime_profile: "solana-v1.6.16".to_string(),
            runtime_admission: SegmentRuntimeAdmission::Candidate,
            runtime_revision: "86c26f843276581509c3434acc2efbf4202c44e0".to_string(),
            runtime_toolchain: "rustc 1.51.0 (2fd73fabe 2021-03-23)".to_string(),
            runtime_target: "x86_64-unknown-linux-gnu".to_string(),
            genesis_hash: hash(5).to_string(),
        }
    }

    fn write_qualification_fixture(directory: &TempDir) -> PathBuf {
        let runtime = runtime_identity();
        let provenance = ArchiveProvenanceV2 {
            base: ArchiveProvenanceV1 {
                generation_profile: runtime.generation_profile.clone(),
                runtime_profile: runtime.runtime_profile.clone(),
                runtime_admission: RuntimeAdmission::Candidate,
                runtime_revision: runtime.runtime_revision.clone(),
                runtime_toolchain: runtime.runtime_toolchain.clone(),
                genesis_hash: hash(5),
                bootstrap_state_kind: BootstrapStateKind::SnapshotArchive,
                bootstrap_slot: TEST_BOOTSTRAP,
                bootstrap_state_hash: hash(9),
                requested_slot_start: TEST_OUTPUT_START,
                requested_slot_count: TEST_TERMINAL - TEST_OUTPUT_START + 1,
                transaction_metadata: TransactionMetadataPolicy::observed(),
            },
            worker_executable_sha256: TEST_WORKER_DIGEST,
        };
        let mut writer = ArchiveWriter::new_with_provenance(
            Vec::new(),
            TEST_EPOCH,
            TEST_OUTPUT_START,
            TEST_TERMINAL - TEST_OUTPUT_START + 1,
            ArchiveWriterConfig::default(),
            &provenance.into(),
        )
        .unwrap();
        for slot in TEST_OUTPUT_START..=TEST_TERMINAL {
            writer.write_skipped_slot(slot).unwrap();
        }
        let (bytes, _) = writer.finish().unwrap();
        let archive = directory
            .path()
            .join(format!("epoch-{TEST_EPOCH}-through-{TEST_TERMINAL}.jet"));
        fs::write(&archive, bytes).unwrap();
        fs::set_permissions(&archive, fs::Permissions::from_mode(0o600)).unwrap();

        let checkpoint = |slot, byte| SegmentCheckpointSummary {
            slot,
            bank_hash: hash(byte).to_string(),
            accounts_hash: hash(byte + 1).to_string(),
            last_blockhash: hash(byte + 2).to_string(),
            capitalization: 1_000,
            transaction_count: 50,
            tick_height: 12_000,
            slot_complete: true,
            write_count: 0,
            next_write_version: 100,
        };
        let manifest = HistoricalSegmentManifest {
            schema_version: SEGMENT_MANIFEST_SCHEMA_VERSION,
            epoch: TEST_EPOCH,
            output_slot_start: TEST_OUTPUT_START,
            output_slot_count: TEST_TERMINAL - TEST_OUTPUT_START + 1,
            runtime,
            worker_executable_sha256: TEST_WORKER_DIGEST,
            archive_sha256: [0; 32],
            bootstrap_archive_sha256: None,
            bootstrap: checkpoint(TEST_BOOTSTRAP, 8),
            terminal: checkpoint(TEST_TERMINAL, 11),
            emitted_raw_write_versions: 100..100,
        };
        let (manifest_path, _) = write_segment_manifest(&archive, manifest).unwrap();
        fs::set_permissions(manifest_path, fs::Permissions::from_mode(0o600)).unwrap();
        archive
    }

    #[test]
    fn artifact_validation_is_deep_private_and_sidecar_free() {
        let directory = TempDir::new().unwrap();
        fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o700)).unwrap();
        let archive = write_qualification_fixture(&directory);
        let arguments = Arguments {
            archive: archive.clone(),
            private_root: directory.path().to_path_buf(),
            expected_epoch: TEST_EPOCH,
            expected_output_start_slot: TEST_OUTPUT_START,
            expected_bootstrap_slot: TEST_BOOTSTRAP,
            expected_terminal_slot: TEST_TERMINAL,
            expected_runtime_profile: "solana-v1.6.16".to_string(),
            expected_worker_sha256: sha256_hex_string(&TEST_WORKER_DIGEST),
        };
        let receipt = verify(&arguments).unwrap();
        assert_eq!(receipt["validation"], "pass");
        assert_eq!(receipt["canonical_checksum_sidecar_absent"], true);

        let checksum = archive_checksum_path(&archive).unwrap();
        fs::write(&checksum, b"publication marker must be rejected\n").unwrap();
        fs::set_permissions(checksum, fs::Permissions::from_mode(0o600)).unwrap();
        assert!(
            verify(&arguments)
                .unwrap_err()
                .contains("canonical checksum sidecar must be absent")
        );
    }
}
