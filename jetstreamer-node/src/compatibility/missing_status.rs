//! Exact, independently audited exceptions for missing Old Faithful metadata.
//!
//! The ordinary post-cutover rule is fail-closed. This registry admits only a
//! byte-for-byte `(slot, transaction index, signature)` match and carries the
//! canonical execution result used to verify historical replay. Where the
//! finalized block RPC still exposes the complete metadata frame, its fee and
//! balance vectors are retained as well.

use {
    super::{
        OLD_FAITHFUL_STATUS_REQUIRED_START_SLOT,
        OLD_FAITHFUL_UNTRUSTED_STATUS_ASSOCIATION_END_SLOT_EXCLUSIVE,
    },
    serde::Deserialize,
    sha2::{Digest as _, Sha256},
    solana_clock::Slot,
    solana_signature::Signature,
    solana_transaction::{InstructionError, TransactionError},
    solana_transaction_status::TransactionStatusMeta,
    std::{str::FromStr, sync::LazyLock},
};

const REGISTRY_SCHEMA: &str = "jetstreamer-audited-missing-transaction-runtime-table-v1";
const REGISTRY_BYTES: &[u8] = include_bytes!("audited-missing-transaction-runtime-table-v1.json");
const REGISTRY_SHA256: &str = "182ed1f572cb0be788943ca9d8a3881a78ee377aaf5169c2ca8015ad8f5515b8";
const REGISTRY_RECORD_COUNT: usize = 1_084;
const REGISTRY_SLOT_COUNT: usize = 15;
const REGISTRY_SUCCESS_COUNT: usize = 1_048;
const REGISTRY_FAILURE_COUNT: usize = 36;
const REGISTRY_CANONICAL_METADATA_COUNT: usize = 463;

const EXPECTED_SOURCES: [ExpectedSource; 5] = [
    ExpectedSource {
        role: "epochs18_19_missing_status_report",
        path: "jetstreamer-node/tests/missing-transaction-status-epochs18-19.json",
        byte_length: 6_194,
        sha256: "ed79368a214e62f99af2ed60cdab8f10d7cfd75483485ac7ccc9657341de492d",
    },
    ExpectedSource {
        role: "epoch8120052_signature_status_rpc",
        path: "jetstreamer-node/tests/fixtures/mainnet-signature-statuses-8120052.json",
        byte_length: 10_527,
        sha256: "046d9dc2b9047ec486b7db484d78f0fe25b71462df1f26856798052687b22caa",
    },
    ExpectedSource {
        role: "epochs24_100_missing_status_report",
        path: "jetstreamer-node/tests/missing-transaction-status-epochs24-100.json",
        byte_length: 192_494,
        sha256: "27c5ab1725d9224c51577272ae8f4131f6e848ca0ca325985558d603ce2367bb",
    },
    ExpectedSource {
        role: "epochs24_100_signature_status_rpc",
        path: "jetstreamer-node/tests/fixtures/mainnet-signature-statuses-epochs24-100.json",
        byte_length: 229_984,
        sha256: "0f48ac64670c3e314b885c63512cb24cadf9cdd2966ecc19be48f12defeaeede",
    },
    ExpectedSource {
        role: "epochs24_100_canonical_block_metadata_rpc",
        path: "jetstreamer-node/tests/fixtures/mainnet-block-metadata-epochs24-100.json",
        byte_length: 293_537,
        sha256: "d1c269a310be173a1337ce6a2b31d683c580325d22dd5f4e3fe6d9777acee741",
    },
];

struct ExpectedSource {
    role: &'static str,
    path: &'static str,
    byte_length: u64,
    sha256: &'static str,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct AuditedMissingTransactionStatus {
    pub(crate) slot: Slot,
    pub(crate) transaction_slot_index: u32,
    pub(crate) signature: Signature,
    pub(crate) expected_status: Result<(), TransactionError>,
    pub(crate) canonical_metadata: Option<TransactionStatusMeta>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum MissingTransactionStatusEvidence {
    /// The archive predates status frames; the selected historical runtime is
    /// the only available source for the transaction result.
    PreCutoverRuntime,
    /// An exact post-cutover source hole with independently captured status.
    Audited(Box<AuditedMissingTransactionStatus>),
}

#[derive(Clone, Debug)]
struct AuditedRecord {
    slot: Slot,
    transaction_slot_index: u32,
    signature: Signature,
    expected_status: Result<(), TransactionError>,
    canonical_metadata: Option<TransactionStatusMeta>,
}

impl AuditedRecord {
    const fn key(&self) -> (Slot, u32) {
        (self.slot, self.transaction_slot_index)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RegistryWire {
    schema: String,
    sources: Vec<SourceWire>,
    summary: SummaryWire,
    records: Vec<RecordWire>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SourceWire {
    role: String,
    path: String,
    byte_length: u64,
    sha256: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SummaryWire {
    record_count: usize,
    slot_count: usize,
    status_counts: StatusCountsWire,
    canonical_metadata_present: usize,
    canonical_metadata_absent: usize,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StatusCountsWire {
    success: usize,
    instruction_error_0_custom_0: usize,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RecordWire {
    slot: Slot,
    transaction_slot_index: u32,
    signature: String,
    status: ExpectedStatusWire,
    canonical_metadata: Option<CanonicalMetadataWire>,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
enum ExpectedStatusWire {
    #[serde(rename = "success")]
    Success,
    #[serde(rename = "instruction_error_0_custom_0")]
    InstructionError0Custom0,
}

impl ExpectedStatusWire {
    fn into_status(self) -> Result<(), TransactionError> {
        match self {
            Self::Success => Ok(()),
            Self::InstructionError0Custom0 => Err(TransactionError::InstructionError(
                0,
                InstructionError::Custom(0),
            )),
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CanonicalMetadataWire {
    fee: u64,
    pre_balances: Vec<u64>,
    post_balances: Vec<u64>,
}

static AUDITED_RECORDS: LazyLock<Result<Box<[AuditedRecord]>, String>> =
    LazyLock::new(|| parse_registry(REGISTRY_BYTES));

fn parse_registry(bytes: &[u8]) -> Result<Box<[AuditedRecord]>, String> {
    let digest = format!("{:x}", Sha256::digest(bytes));
    if digest != REGISTRY_SHA256 {
        return Err(format!(
            "audited missing-status registry digest mismatch: expected {REGISTRY_SHA256}, got {digest}"
        ));
    }
    parse_registry_contents(bytes)
}

fn parse_registry_contents(bytes: &[u8]) -> Result<Box<[AuditedRecord]>, String> {
    let wire: RegistryWire = serde_json::from_slice(bytes)
        .map_err(|error| format!("invalid audited missing-status registry JSON: {error}"))?;
    if wire.schema != REGISTRY_SCHEMA {
        return Err(format!(
            "unsupported audited missing-status registry schema {:?}",
            wire.schema
        ));
    }
    if wire.sources.len() != EXPECTED_SOURCES.len() {
        return Err(format!(
            "audited missing-status registry has {} sources, expected {}",
            wire.sources.len(),
            EXPECTED_SOURCES.len()
        ));
    }
    for (actual, expected) in wire.sources.iter().zip(EXPECTED_SOURCES) {
        if actual.role != expected.role
            || actual.path != expected.path
            || actual.byte_length != expected.byte_length
            || actual.sha256 != expected.sha256
        {
            return Err(format!(
                "audited missing-status registry source {:?} does not match its compiled identity",
                actual.role
            ));
        }
    }
    let summary = &wire.summary;
    if summary.record_count != REGISTRY_RECORD_COUNT
        || summary.slot_count != REGISTRY_SLOT_COUNT
        || summary.status_counts.success != REGISTRY_SUCCESS_COUNT
        || summary.status_counts.instruction_error_0_custom_0 != REGISTRY_FAILURE_COUNT
        || summary.canonical_metadata_present != REGISTRY_CANONICAL_METADATA_COUNT
        || summary.canonical_metadata_absent
            != REGISTRY_RECORD_COUNT - REGISTRY_CANONICAL_METADATA_COUNT
        || wire.records.len() != REGISTRY_RECORD_COUNT
    {
        return Err(
            "audited missing-status registry summary does not match the compiled corpus"
                .to_string(),
        );
    }

    let mut records = Vec::with_capacity(wire.records.len());
    let mut previous_key = None;
    let mut previous_slot = None;
    let mut slot_count = 0usize;
    let mut success_count = 0usize;
    let mut failure_count = 0usize;
    let mut canonical_metadata_count = 0usize;
    for record in wire.records {
        if record.slot < OLD_FAITHFUL_STATUS_REQUIRED_START_SLOT
            || record.slot >= OLD_FAITHFUL_UNTRUSTED_STATUS_ASSOCIATION_END_SLOT_EXCLUSIVE
        {
            return Err(format!(
                "audited missing-status record at slot {} is outside the qualified source era",
                record.slot
            ));
        }
        let key = (record.slot, record.transaction_slot_index);
        if previous_key.is_some_and(|previous| previous >= key) {
            return Err(format!(
                "audited missing-status records are not strictly ordered at slot {} index {}",
                record.slot, record.transaction_slot_index
            ));
        }
        previous_key = Some(key);
        if previous_slot != Some(record.slot) {
            previous_slot = Some(record.slot);
            slot_count = slot_count.saturating_add(1);
        }
        let signature = Signature::from_str(&record.signature).map_err(|error| {
            format!(
                "invalid audited signature at slot {} index {}: {error}",
                record.slot, record.transaction_slot_index
            )
        })?;
        let expected_status = record.status.into_status();
        if expected_status.is_ok() {
            success_count = success_count.saturating_add(1);
        } else {
            failure_count = failure_count.saturating_add(1);
        }
        let canonical_metadata = record
            .canonical_metadata
            .map(|metadata| {
                if metadata.pre_balances.len() != metadata.post_balances.len() {
                    return Err(format!(
                        "canonical metadata balance length mismatch at slot {} index {}",
                        record.slot, record.transaction_slot_index
                    ));
                }
                Ok(TransactionStatusMeta {
                    status: expected_status.clone(),
                    fee: metadata.fee,
                    pre_balances: metadata.pre_balances,
                    post_balances: metadata.post_balances,
                    ..TransactionStatusMeta::default()
                })
            })
            .transpose()?;
        canonical_metadata_count =
            canonical_metadata_count.saturating_add(usize::from(canonical_metadata.is_some()));
        records.push(AuditedRecord {
            slot: record.slot,
            transaction_slot_index: record.transaction_slot_index,
            signature,
            expected_status,
            canonical_metadata,
        });
    }
    if slot_count != REGISTRY_SLOT_COUNT
        || success_count != REGISTRY_SUCCESS_COUNT
        || failure_count != REGISTRY_FAILURE_COUNT
        || canonical_metadata_count != REGISTRY_CANONICAL_METADATA_COUNT
    {
        return Err(
            "audited missing-status registry records disagree with their summary".to_string(),
        );
    }
    Ok(records.into_boxed_slice())
}

fn audited_records() -> Result<&'static [AuditedRecord], String> {
    AUDITED_RECORDS
        .as_ref()
        .map(Box::as_ref)
        .map_err(Clone::clone)
}

pub(crate) fn validate_audited_missing_status_registry() -> Result<(), String> {
    audited_records().map(|_| ())
}

/// Resolves one source-missing transaction without weakening the slot-wide
/// post-cutover rejection policy.
///
/// Callers invoke this only for an actually missing frame. Startup planning
/// validates the embedded table once; ordinary observed transactions never
/// search it.
pub(crate) fn resolve_missing_transaction_status(
    slot: Slot,
    transaction_slot_index: usize,
    signature: &Signature,
) -> Result<Option<MissingTransactionStatusEvidence>, String> {
    if slot < OLD_FAITHFUL_STATUS_REQUIRED_START_SLOT {
        return Ok(Some(MissingTransactionStatusEvidence::PreCutoverRuntime));
    }
    let Ok(transaction_slot_index) = u32::try_from(transaction_slot_index) else {
        return Ok(None);
    };
    let records = audited_records()?;
    let key = (slot, transaction_slot_index);
    let Ok(ordinal) = records.binary_search_by_key(&key, AuditedRecord::key) else {
        return Ok(None);
    };
    let record = &records[ordinal];
    if record.signature != *signature {
        return Ok(None);
    }
    Ok(Some(MissingTransactionStatusEvidence::Audited(Box::new(
        AuditedMissingTransactionStatus {
            slot: record.slot,
            transaction_slot_index: record.transaction_slot_index,
            signature: record.signature,
            expected_status: record.expected_status.clone(),
            canonical_metadata: record.canonical_metadata.clone(),
        },
    ))))
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::Sha256;
    use std::collections::{BTreeMap, BTreeSet};

    fn rpc_status(value: &serde_json::Value) -> ExpectedStatusWire {
        if value["err"].is_null() && value["status"] == serde_json::json!({"Ok": null}) {
            ExpectedStatusWire::Success
        } else {
            let expected_error = serde_json::json!({
                "InstructionError": [0, {"Custom": 0}]
            });
            assert_eq!(value["err"], expected_error);
            assert_eq!(value["status"]["Err"], expected_error);
            ExpectedStatusWire::InstructionError0Custom0
        }
    }

    #[test]
    fn registry_is_valid_and_source_bound() {
        validate_audited_missing_status_registry().unwrap();
        assert_eq!(
            format!("{:x}", Sha256::digest(REGISTRY_BYTES)),
            REGISTRY_SHA256
        );

        let source_bytes: [&[u8]; 5] = [
            include_bytes!("../../tests/missing-transaction-status-epochs18-19.json"),
            include_bytes!("../../tests/fixtures/mainnet-signature-statuses-8120052.json"),
            include_bytes!("../../tests/missing-transaction-status-epochs24-100.json"),
            include_bytes!("../../tests/fixtures/mainnet-signature-statuses-epochs24-100.json"),
            include_bytes!("../../tests/fixtures/mainnet-block-metadata-epochs24-100.json"),
        ];
        for (bytes, expected) in source_bytes.into_iter().zip(EXPECTED_SOURCES) {
            assert_eq!(bytes.len() as u64, expected.byte_length);
            assert_eq!(format!("{:x}", Sha256::digest(bytes)), expected.sha256);
        }
    }

    #[test]
    fn normalized_records_match_every_raw_audit_and_rpc_record() {
        let registry: RegistryWire = serde_json::from_slice(REGISTRY_BYTES).unwrap();
        let early_report: serde_json::Value = serde_json::from_slice(include_bytes!(
            "../../tests/missing-transaction-status-epochs18-19.json"
        ))
        .unwrap();
        let early_rpc: serde_json::Value = serde_json::from_slice(include_bytes!(
            "../../tests/fixtures/mainnet-signature-statuses-8120052.json"
        ))
        .unwrap();
        let early_missing = early_report["missing_statuses"].as_array().unwrap();
        let early_requested = early_rpc["request"]["params"][0].as_array().unwrap();
        let early_statuses = early_rpc["response"]["result"]["value"].as_array().unwrap();
        assert_eq!(early_missing.len(), 33);
        assert_eq!(early_requested.len(), early_missing.len());
        assert_eq!(early_statuses.len(), early_missing.len());
        for (ordinal, ((missing, requested), status)) in early_missing
            .iter()
            .zip(early_requested)
            .zip(early_statuses)
            .enumerate()
        {
            let normalized = &registry.records[ordinal];
            assert_eq!(normalized.slot, missing["slot"].as_u64().unwrap());
            assert_eq!(
                u64::from(normalized.transaction_slot_index),
                missing["transaction_slot_index"].as_u64().unwrap()
            );
            assert_eq!(normalized.signature, missing["signature"].as_str().unwrap());
            assert_eq!(requested.as_str(), Some(normalized.signature.as_str()));
            assert_eq!(status["slot"].as_u64(), Some(normalized.slot));
            assert_eq!(status["confirmationStatus"].as_str(), Some("finalized"));
            assert_eq!(rpc_status(status), normalized.status);
            assert!(normalized.canonical_metadata.is_none());
        }

        let report: serde_json::Value = serde_json::from_slice(include_bytes!(
            "../../tests/missing-transaction-status-epochs24-100.json"
        ))
        .unwrap();
        let status_capture: serde_json::Value = serde_json::from_slice(include_bytes!(
            "../../tests/fixtures/mainnet-signature-statuses-epochs24-100.json"
        ))
        .unwrap();
        let missing = report["missing_statuses"].as_array().unwrap();
        let mut statuses = Vec::with_capacity(missing.len());
        for batch in status_capture["batches"].as_array().unwrap() {
            let start = batch["first_record_index"].as_u64().unwrap() as usize;
            let count = batch["record_count"].as_u64().unwrap() as usize;
            assert_eq!(start, statuses.len());
            let end = start + count;
            let requested = missing[start..end]
                .iter()
                .map(|record| record["signature"].as_str().unwrap())
                .collect::<Vec<_>>();
            let requested_bytes = serde_json::to_vec(&requested).unwrap();
            assert_eq!(
                format!("{:x}", Sha256::digest(requested_bytes)),
                batch["requested_signatures_sha256"].as_str().unwrap()
            );
            let values = batch["values"].as_array().unwrap();
            assert_eq!(values.len(), count);
            statuses.extend(values);
        }
        assert_eq!(statuses.len(), 1_051);
        assert_eq!(missing.len(), statuses.len());
        for (offset, (missing, status)) in missing.iter().zip(statuses).enumerate() {
            let normalized = &registry.records[33 + offset];
            assert_eq!(normalized.slot, missing["slot"].as_u64().unwrap());
            assert_eq!(
                u64::from(normalized.transaction_slot_index),
                missing["transaction_slot_index"].as_u64().unwrap()
            );
            assert_eq!(normalized.signature, missing["signature"].as_str().unwrap());
            assert_eq!(status["slot"].as_u64(), Some(normalized.slot));
            assert_eq!(status["confirmationStatus"].as_str(), Some("finalized"));
            assert_eq!(rpc_status(status), normalized.status);
        }

        let normalized_by_key = registry
            .records
            .iter()
            .map(|record| ((record.slot, record.transaction_slot_index), record))
            .collect::<BTreeMap<_, _>>();
        let block_capture: serde_json::Value = serde_json::from_slice(include_bytes!(
            "../../tests/fixtures/mainnet-block-metadata-epochs24-100.json"
        ))
        .unwrap();
        let mut block_record_count = 0usize;
        let mut block_keys = BTreeSet::new();
        let mut canonical_metadata_count = 0usize;
        for slot_capture in block_capture["per_slot"].as_array().unwrap() {
            let slot = slot_capture["slot"].as_u64().unwrap();
            for raw in slot_capture["records"].as_array().unwrap() {
                block_record_count += 1;
                let index = u32::try_from(raw["transaction_slot_index"].as_u64().unwrap()).unwrap();
                assert!(block_keys.insert((slot, index)));
                let normalized = normalized_by_key[&(slot, index)];
                assert_eq!(normalized.signature, raw["signature"].as_str().unwrap());
                let Some(meta) = raw["meta"].as_object() else {
                    assert!(raw["meta"].is_null());
                    assert!(normalized.canonical_metadata.is_none());
                    continue;
                };
                canonical_metadata_count += 1;
                let fields = meta.keys().map(String::as_str).collect::<BTreeSet<_>>();
                assert_eq!(
                    fields,
                    BTreeSet::from([
                        "err",
                        "fee",
                        "innerInstructions",
                        "loadedAddresses",
                        "logMessages",
                        "postBalances",
                        "postTokenBalances",
                        "preBalances",
                        "preTokenBalances",
                        "rewards",
                        "status",
                    ])
                );
                assert_eq!(
                    rpc_status(&serde_json::Value::Object(meta.clone())),
                    normalized.status
                );
                for field in [
                    "innerInstructions",
                    "logMessages",
                    "preTokenBalances",
                    "postTokenBalances",
                    "rewards",
                ] {
                    assert!(meta[field].is_null());
                }
                assert_eq!(meta["loadedAddresses"]["writable"], serde_json::json!([]));
                assert_eq!(meta["loadedAddresses"]["readonly"], serde_json::json!([]));
                let canonical = normalized.canonical_metadata.as_ref().unwrap();
                assert_eq!(meta["fee"].as_u64(), Some(canonical.fee));
                assert_eq!(
                    serde_json::from_value::<Vec<u64>>(meta["preBalances"].clone()).unwrap(),
                    canonical.pre_balances
                );
                assert_eq!(
                    serde_json::from_value::<Vec<u64>>(meta["postBalances"].clone()).unwrap(),
                    canonical.post_balances
                );
            }
        }
        assert_eq!(block_record_count, 1_051);
        assert_eq!(
            block_keys,
            registry.records[33..]
                .iter()
                .map(|record| (record.slot, record.transaction_slot_index))
                .collect()
        );
        assert_eq!(canonical_metadata_count, REGISTRY_CANONICAL_METADATA_COUNT);
    }

    #[test]
    fn entry_shape_evidence_proves_the_single_mixed_entry() {
        const SHAPES: &[u8] =
            include_bytes!("../../tests/fixtures/old-faithful-entry-shapes-epochs24-100.json");
        assert_eq!(SHAPES.len(), 338_170);
        assert_eq!(
            format!("{:x}", Sha256::digest(SHAPES)),
            "dfbae1dd22882152bbd6d9a09d97c72dd216d6cd9a688f1796a3cc6bb3de5606"
        );
        let shapes: serde_json::Value = serde_json::from_slice(SHAPES).unwrap();
        let generator =
            include_bytes!("../../tests/fixtures/old-faithful-entry-shapes-generator.rs");
        assert_eq!(
            generator.len() as u64,
            shapes["shape_extractor"]["source_length"].as_u64().unwrap()
        );
        assert_eq!(
            format!("{:x}", Sha256::digest(generator)),
            shapes["shape_extractor"]["source_sha256"].as_str().unwrap()
        );
        assert_eq!(
            shapes["shape_extractor"]["source_path"].as_str(),
            Some("jetstreamer-node/tests/fixtures/old-faithful-entry-shapes-generator.rs")
        );
        assert_eq!(
            shapes["summary"]["missing_status_count"].as_u64(),
            Some(1_051)
        );
        assert_eq!(
            shapes["summary"]["affected_entry_count"].as_u64(),
            Some(977)
        );
        assert_eq!(
            shapes["summary"]["wholly_missing_entry_count"].as_u64(),
            Some(976)
        );
        assert_eq!(shapes["summary"]["mixed_entry_count"].as_u64(), Some(1));
        let mixed = shapes["slots"]
            .as_array()
            .unwrap()
            .iter()
            .flat_map(|slot| slot["affected_entries"].as_array().unwrap())
            .filter(|entry| entry["classification"].as_str() == Some("mixed"))
            .collect::<Vec<_>>();
        assert_eq!(mixed.len(), 1);
        assert_eq!(mixed[0]["entry_index"].as_u64(), Some(35));
        assert_eq!(mixed[0]["transaction_start_index"].as_u64(), Some(7));
        assert_eq!(mixed[0]["transaction_end_exclusive"].as_u64(), Some(9));
        assert_eq!(mixed[0]["missing_indexes"], serde_json::json!([8]));
    }

    #[test]
    fn transaction_rpc_sample_confirms_null_block_metadata_is_not_recoverable_there() {
        const SAMPLE: &[u8] = include_bytes!(
            "../../tests/fixtures/mainnet-get-transaction-null-metadata-sample-epochs24-100.json"
        );
        assert_eq!(SAMPLE.len(), 14_714);
        assert_eq!(
            format!("{:x}", Sha256::digest(SAMPLE)),
            "cb20aaa84bb961bef9a31bba654340030fc0c8509274a8bde56048b50ea936bf"
        );
        let capture: serde_json::Value = serde_json::from_slice(SAMPLE).unwrap();
        assert_eq!(capture["summary"]["sample_count"].as_u64(), Some(10));
        assert_eq!(capture["summary"]["nonnull_meta"].as_u64(), Some(0));
        assert_eq!(capture["selection"]["null_bearing_slots"].as_u64(), Some(7));
        let mut slots = BTreeSet::new();
        for sample in capture["samples"].as_array().unwrap() {
            let expected = &sample["expected"];
            let result = &sample["response"]["result"];
            let signature = expected["signature"].as_str().unwrap();
            assert_eq!(sample["request"]["params"][0].as_str(), Some(signature));
            assert_eq!(result["slot"], expected["slot"]);
            assert_eq!(
                result["transactionIndex"],
                expected["transaction_slot_index"]
            );
            assert_eq!(
                result["transaction"]["signatures"][0].as_str(),
                Some(signature)
            );
            assert!(result["meta"].is_null());
            assert_eq!(sample["meta_nonnull"].as_bool(), Some(false));
            slots.insert(expected["slot"].as_u64().unwrap());
        }
        assert_eq!(slots.len(), 7);
    }

    #[test]
    fn malformed_or_reidentified_registry_data_fails_closed() {
        let mutation_error = |mutate: fn(&mut serde_json::Value)| {
            let mut value: serde_json::Value = serde_json::from_slice(REGISTRY_BYTES).unwrap();
            mutate(&mut value);
            parse_registry_contents(&serde_json::to_vec(&value).unwrap()).unwrap_err()
        };

        let mut digest_mutation = REGISTRY_BYTES.to_vec();
        digest_mutation[0] ^= 1;
        let error = parse_registry(&digest_mutation).unwrap_err();
        assert!(error.contains("registry digest mismatch"), "{error}");

        let error = mutation_error(|value| {
            value["records"][1]["slot"] = value["records"][0]["slot"].clone();
            value["records"][1]["transaction_slot_index"] =
                value["records"][0]["transaction_slot_index"].clone();
        });
        assert!(error.contains("not strictly ordered"), "{error}");

        let error = mutation_error(|value| {
            value["records"][0]["status"] = serde_json::json!("future_status");
        });
        assert!(error.contains("invalid audited missing-status registry JSON"));

        let error = mutation_error(|value| {
            value["records"][33]["canonical_metadata"]["post_balances"] = serde_json::json!([]);
        });
        assert!(error.contains("balance length mismatch"), "{error}");

        let error = mutation_error(|value| {
            value["sources"][0]["sha256"] = serde_json::json!("00");
        });
        assert!(error.contains("does not match its compiled identity"));

        let error = mutation_error(|value| {
            value["records"][0]["unexpected"] = serde_json::json!(true);
        });
        assert!(error.contains("unknown field"), "{error}");
    }

    #[test]
    fn exact_resolution_preserves_status_and_available_metadata() {
        let original_signature: Signature =
            "5iDNYejCujaTwp2m64YJstEKJPQP5xBVmh73u3eXejLp8c2fmyJNmyZss8RKoBhMYYeiQkadosN3W644Ro8h1cD2"
                .parse()
                .unwrap();
        assert_eq!(
            resolve_missing_transaction_status(8_120_052, 79, &original_signature).unwrap(),
            Some(MissingTransactionStatusEvidence::Audited(Box::new(
                AuditedMissingTransactionStatus {
                    slot: 8_120_052,
                    transaction_slot_index: 79,
                    signature: original_signature,
                    expected_status: Ok(()),
                    canonical_metadata: None,
                }
            )))
        );

        let failed_signature: Signature =
            "BEGWJ7cztpfGAieC9mQQuuKWNee615fHVatKfssvQGdUjpJJHFtXSscoc1Kotucu4BmkEBmd9bmBq9FjmxmpKTb"
                .parse()
                .unwrap();
        let evidence = resolve_missing_transaction_status(13_334_463, 13, &failed_signature)
            .unwrap()
            .unwrap();
        let MissingTransactionStatusEvidence::Audited(evidence) = evidence else {
            panic!("post-cutover record must be audited");
        };
        assert_eq!(
            evidence.expected_status,
            Err(TransactionError::InstructionError(
                0,
                InstructionError::Custom(0)
            ))
        );
        let metadata = evidence.canonical_metadata.unwrap();
        assert_eq!(metadata.status, evidence.expected_status);
        assert_eq!(metadata.fee, 5_000);
        assert_eq!(metadata.pre_balances.len(), 5);
        assert_eq!(metadata.pre_balances.len(), metadata.post_balances.len());
        assert!(metadata.inner_instructions.is_none());
        assert!(metadata.loaded_addresses.is_empty());
    }

    #[test]
    fn unknown_or_reidentified_post_cutover_holes_remain_rejected() {
        let signature: Signature =
            "5iDNYejCujaTwp2m64YJstEKJPQP5xBVmh73u3eXejLp8c2fmyJNmyZss8RKoBhMYYeiQkadosN3W644Ro8h1cD2"
                .parse()
                .unwrap();
        assert!(
            resolve_missing_transaction_status(8_120_052, 80, &signature)
                .unwrap()
                .is_none()
        );
        assert!(
            resolve_missing_transaction_status(8_120_051, 79, &signature)
                .unwrap()
                .is_none()
        );
        assert!(
            resolve_missing_transaction_status(8_120_052, 79, &Signature::default())
                .unwrap()
                .is_none()
        );
        assert_eq!(
            resolve_missing_transaction_status(4_258_775, usize::MAX, &Signature::default())
                .unwrap(),
            Some(MissingTransactionStatusEvidence::PreCutoverRuntime)
        );
    }
}
