//! Exact recovery for a bounded Old Faithful source gap using finalized block
//! RPC. Unlike the earlier runtime-authoritative audit table, every record in
//! this registry carries canonical metadata and remains source-exact.

use {
    serde::Deserialize,
    sha2::{Digest as _, Sha256},
    solana_address::Address,
    solana_clock::Slot,
    solana_message::{compiled_instruction::CompiledInstruction, v0::LoadedAddresses},
    solana_signature::Signature,
    solana_transaction::TransactionError,
    solana_transaction_status::{
        InnerInstruction, InnerInstructions, TransactionStatusMeta, TransactionTokenBalance,
        UiInnerInstructions, UiInstruction, UiLoadedAddresses, UiTransactionStatusMeta,
        UiTransactionTokenBalance, option_serializer::OptionSerializer,
    },
    std::{
        collections::{BTreeMap, BTreeSet},
        io::Cursor,
        str::FromStr,
        sync::LazyLock,
    },
};

const AUDIT_COMPRESSED: &[u8] =
    include_bytes!("../../tests/fixtures/old-faithful-missing-status-epoch-208.json.zst");
const AUDIT_COMPRESSED_SHA256: &str =
    "2dbcf22aef65514e51a1aa7365e362690ee6f4e26d8c44cfbf26233c5ae24dff";
const AUDIT_COMPRESSED_LENGTH: usize = 5_037_200;
const AUDIT_JSON_SHA256: &str = "c7f41e57f3f24628069297cb4e40c84442cab158dd0d3d4acd6eb3413782b931";
const AUDIT_JSON_LENGTH: usize = 13_554_746;

const BLOCKS_COMPRESSED: &[u8] =
    include_bytes!("../../tests/fixtures/mainnet-block-metadata-epoch-208.tar.zst");
const BLOCKS_COMPRESSED_SHA256: &str =
    "476400bae03413d79e90547602c646b5da7238efa929334eb8686e65e5decbdb";
const BLOCKS_COMPRESSED_LENGTH: usize = 7_541_326;

const AUDIT_SLOT_START: Slot = 89_856_000;
const AUDIT_SLOT_END_EXCLUSIVE: Slot = 90_288_000;
const AUDIT_TRANSACTION_NOTIFICATIONS: u64 = 344_586_521;
const RECORD_COUNT: usize = 73_688;
const SLOT_COUNT: usize = 88;
const SUCCESS_COUNT: usize = 65_179;
const FAILURE_COUNT: usize = 8_509;
const FIRST_MISSING_SLOT: Slot = 89_856_001;
const LAST_MISSING_SLOT: Slot = 89_856_106;

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct CanonicalMissingTransactionStatus {
    pub(crate) slot: Slot,
    pub(crate) transaction_slot_index: u32,
    pub(crate) signature: Signature,
    pub(crate) metadata: TransactionStatusMeta,
}

#[derive(Clone, Debug)]
struct CanonicalRecord {
    slot: Slot,
    transaction_slot_index: u32,
    signature: Signature,
    metadata: TransactionStatusMeta,
}

impl CanonicalRecord {
    const fn key(&self) -> (Slot, u32) {
        (self.slot, self.transaction_slot_index)
    }
}

#[derive(Deserialize)]
struct AuditWire {
    slot_start: Slot,
    slot_end_exclusive: Slot,
    transaction_notifications: u64,
    threads: usize,
    missing_statuses: Vec<AuditRecordWire>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AuditRecordWire {
    slot: Slot,
    transaction_slot_index: u32,
    signature: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CaptureWire {
    schema: String,
    request: RpcRequestWire,
    response: RpcResponseWire,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RpcRequestWire {
    jsonrpc: String,
    id: u64,
    method: String,
    params: serde_json::Value,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RpcResponseWire {
    jsonrpc: String,
    id: u64,
    result: RpcBlockWire,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RpcBlockWire {
    transactions: Vec<RpcTransactionWithMetaWire>,
}

#[derive(Deserialize)]
struct RpcTransactionWithMetaWire {
    transaction: RpcTransactionWire,
    meta: UiTransactionStatusMeta,
}

#[derive(Deserialize)]
struct RpcTransactionWire {
    signatures: Vec<String>,
}

static CANONICAL_RECORDS: LazyLock<Result<Box<[CanonicalRecord]>, String>> =
    LazyLock::new(parse_registry);

fn verify_embedded_source(
    bytes: &[u8],
    expected_length: usize,
    expected_sha256: &str,
) -> Result<(), String> {
    if bytes.len() != expected_length {
        return Err(format!(
            "canonical missing-status source length mismatch: expected {expected_length}, got {}",
            bytes.len()
        ));
    }
    let actual = format!("{:x}", Sha256::digest(bytes));
    if actual != expected_sha256 {
        return Err(format!(
            "canonical missing-status source digest mismatch: expected {expected_sha256}, got {actual}"
        ));
    }
    Ok(())
}

fn parse_registry() -> Result<Box<[CanonicalRecord]>, String> {
    verify_embedded_source(
        AUDIT_COMPRESSED,
        AUDIT_COMPRESSED_LENGTH,
        AUDIT_COMPRESSED_SHA256,
    )?;
    verify_embedded_source(
        BLOCKS_COMPRESSED,
        BLOCKS_COMPRESSED_LENGTH,
        BLOCKS_COMPRESSED_SHA256,
    )?;

    let audit_json = zstd::stream::decode_all(Cursor::new(AUDIT_COMPRESSED))
        .map_err(|error| format!("failed to decompress epoch-208 missing-status audit: {error}"))?;
    verify_embedded_source(&audit_json, AUDIT_JSON_LENGTH, AUDIT_JSON_SHA256)?;
    let audit: AuditWire = serde_json::from_slice(&audit_json)
        .map_err(|error| format!("invalid epoch-208 missing-status audit: {error}"))?;
    if audit.slot_start != AUDIT_SLOT_START
        || audit.slot_end_exclusive != AUDIT_SLOT_END_EXCLUSIVE
        || audit.transaction_notifications != AUDIT_TRANSACTION_NOTIFICATIONS
        || audit.threads != 16
        || audit.missing_statuses.len() != RECORD_COUNT
    {
        return Err("epoch-208 missing-status audit summary mismatch".to_string());
    }

    let mut missing = BTreeMap::new();
    let mut previous_key = None;
    for record in audit.missing_statuses {
        let key = (record.slot, record.transaction_slot_index);
        if previous_key.is_some_and(|previous| previous >= key) {
            return Err(format!(
                "epoch-208 missing-status audit is not strictly ordered at slot {} index {}",
                record.slot, record.transaction_slot_index
            ));
        }
        previous_key = Some(key);
        let signature = Signature::from_str(&record.signature).map_err(|error| {
            format!(
                "invalid epoch-208 audited signature at slot {} index {}: {error}",
                record.slot, record.transaction_slot_index
            )
        })?;
        if missing.insert(key, signature).is_some() {
            return Err(format!(
                "duplicate epoch-208 audit identity at slot {} index {}",
                record.slot, record.transaction_slot_index
            ));
        }
    }

    let decoder = zstd::stream::read::Decoder::new(Cursor::new(BLOCKS_COMPRESSED))
        .map_err(|error| format!("failed to open epoch-208 block corpus: {error}"))?;
    let mut archive = tar::Archive::new(decoder);
    let entries = archive
        .entries()
        .map_err(|error| format!("failed to read epoch-208 block corpus: {error}"))?;
    let mut records = Vec::with_capacity(RECORD_COUNT);
    let mut slots = BTreeSet::new();
    let mut success_count = 0usize;
    let mut failure_count = 0usize;
    for entry in entries {
        let mut entry =
            entry.map_err(|error| format!("invalid epoch-208 block-corpus entry: {error}"))?;
        if !entry.header().entry_type().is_file() {
            continue;
        }
        let path = entry
            .path()
            .map_err(|error| format!("invalid epoch-208 block-corpus path: {error}"))?;
        let file_name = path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| "epoch-208 block-corpus entry has no UTF-8 file name".to_string())?;
        let slot = file_name
            .strip_suffix(".json")
            .ok_or_else(|| format!("unexpected epoch-208 block-corpus entry {file_name:?}"))?
            .parse::<Slot>()
            .map_err(|error| {
                format!("invalid slot-bearing block-corpus entry {file_name:?}: {error}")
            })?;
        let capture: CaptureWire = serde_json::from_reader(&mut entry)
            .map_err(|error| format!("invalid finalized block capture for slot {slot}: {error}"))?;
        validate_capture_identity(slot, &capture)?;
        if !slots.insert(slot) {
            return Err(format!("duplicate finalized block capture for slot {slot}"));
        }
        for (index, transaction) in capture.response.result.transactions.into_iter().enumerate() {
            let index = u32::try_from(index)
                .map_err(|_| format!("too many finalized transactions at slot {slot}"))?;
            let key = (slot, index);
            let expected_signature = missing.remove(&key).ok_or_else(|| {
                format!("finalized block contains unaudited identity at slot {slot} index {index}")
            })?;
            if transaction.transaction.signatures.is_empty() {
                return Err(format!(
                    "finalized block transaction at slot {slot} index {index} has no identity signature"
                ));
            }
            let signature =
                Signature::from_str(&transaction.transaction.signatures[0]).map_err(|error| {
                    format!("invalid finalized signature at slot {slot} index {index}: {error}")
                })?;
            if signature != expected_signature {
                return Err(format!(
                    "finalized block signature mismatch at slot {slot} index {index}"
                ));
            }
            let metadata = convert_metadata(transaction.meta, slot, index)?;
            if metadata.status.is_ok() {
                success_count = success_count.saturating_add(1);
            } else {
                failure_count = failure_count.saturating_add(1);
            }
            records.push(CanonicalRecord {
                slot,
                transaction_slot_index: index,
                signature,
                metadata,
            });
        }
    }

    if !missing.is_empty()
        || records.len() != RECORD_COUNT
        || slots.len() != SLOT_COUNT
        || slots.first().copied() != Some(FIRST_MISSING_SLOT)
        || slots.last().copied() != Some(LAST_MISSING_SLOT)
        || success_count != SUCCESS_COUNT
        || failure_count != FAILURE_COUNT
    {
        return Err(format!(
            "epoch-208 canonical registry summary mismatch: records={} slots={} success={} failure={} unresolved={}",
            records.len(),
            slots.len(),
            success_count,
            failure_count,
            missing.len()
        ));
    }
    if records
        .windows(2)
        .any(|pair| pair[0].key() >= pair[1].key())
    {
        return Err("epoch-208 canonical registry is not strictly ordered".to_string());
    }
    Ok(records.into_boxed_slice())
}

fn validate_capture_identity(slot: Slot, capture: &CaptureWire) -> Result<(), String> {
    let expected_params = serde_json::json!([
        slot,
        {
            "commitment": "finalized",
            "encoding": "json",
            "transactionDetails": "full",
            "rewards": false,
            "maxSupportedTransactionVersion": 0
        }
    ]);
    if capture.schema != "jetstreamer-finalized-block-rpc-capture-v1"
        || capture.request.jsonrpc != "2.0"
        || capture.request.id != 1
        || capture.request.method != "getBlock"
        || capture.request.params != expected_params
        || capture.response.jsonrpc != "2.0"
        || capture.response.id != 1
        || capture.response.result.transactions.is_empty()
    {
        return Err(format!(
            "finalized block capture identity mismatch for slot {slot}"
        ));
    }
    Ok(())
}

fn option_value<T>(value: OptionSerializer<T>) -> Option<T> {
    match value {
        OptionSerializer::Some(value) => Some(value),
        OptionSerializer::None | OptionSerializer::Skip => None,
    }
}

fn convert_inner_instructions(
    groups: Vec<UiInnerInstructions>,
    slot: Slot,
    index: u32,
) -> Result<Vec<InnerInstructions>, String> {
    groups
        .into_iter()
        .map(|group| {
            let instructions = group
                .instructions
                .into_iter()
                .map(|instruction| {
                    let UiInstruction::Compiled(instruction) = instruction else {
                        return Err(format!(
                            "parsed inner instruction in raw finalized block at slot {slot} index {index}"
                        ));
                    };
                    let data = bs58::decode(&instruction.data).into_vec().map_err(|error| {
                        format!(
                            "invalid inner-instruction data at slot {slot} index {index}: {error}"
                        )
                    })?;
                    Ok(InnerInstruction {
                        instruction: CompiledInstruction {
                            program_id_index: instruction.program_id_index,
                            accounts: instruction.accounts,
                            data,
                        },
                        stack_height: instruction.stack_height,
                    })
                })
                .collect::<Result<Vec<_>, String>>()?;
            Ok(InnerInstructions {
                index: group.index,
                instructions,
            })
        })
        .collect()
}

fn convert_token_balances(
    balances: Vec<UiTransactionTokenBalance>,
) -> Vec<TransactionTokenBalance> {
    balances
        .into_iter()
        .map(|balance| TransactionTokenBalance {
            account_index: balance.account_index,
            mint: balance.mint,
            ui_token_amount: balance.ui_token_amount,
            owner: option_value(balance.owner).unwrap_or_default(),
            program_id: option_value(balance.program_id).unwrap_or_default(),
        })
        .collect()
}

fn convert_loaded_addresses(
    loaded: UiLoadedAddresses,
    slot: Slot,
    index: u32,
) -> Result<LoadedAddresses, String> {
    let parse = |address: String| {
        Address::from_str(&address).map_err(|error| {
            format!("invalid loaded address at slot {slot} index {index}: {error}")
        })
    };
    Ok(LoadedAddresses {
        writable: loaded
            .writable
            .into_iter()
            .map(|address| parse(address))
            .collect::<Result<Vec<_>, _>>()?,
        readonly: loaded
            .readonly
            .into_iter()
            .map(parse)
            .collect::<Result<Vec<_>, _>>()?,
    })
}

fn convert_metadata(
    meta: UiTransactionStatusMeta,
    slot: Slot,
    index: u32,
) -> Result<TransactionStatusMeta, String> {
    let status = meta.status.map_err(TransactionError::from);
    let err = meta.err.map(TransactionError::from);
    if status.clone().err() != err {
        return Err(format!(
            "finalized metadata status/err mismatch at slot {slot} index {index}"
        ));
    }
    if option_value(meta.return_data).is_some()
        || option_value(meta.compute_units_consumed).is_some()
        || option_value(meta.cost_units).is_some()
    {
        return Err(format!(
            "unexpected post-era finalized metadata field at slot {slot} index {index}"
        ));
    }
    Ok(TransactionStatusMeta {
        status,
        fee: meta.fee,
        pre_balances: meta.pre_balances,
        post_balances: meta.post_balances,
        inner_instructions: option_value(meta.inner_instructions)
            .map(|groups| convert_inner_instructions(groups, slot, index))
            .transpose()?,
        log_messages: option_value(meta.log_messages),
        pre_token_balances: option_value(meta.pre_token_balances).map(convert_token_balances),
        post_token_balances: option_value(meta.post_token_balances).map(convert_token_balances),
        rewards: option_value(meta.rewards),
        loaded_addresses: option_value(meta.loaded_addresses)
            .map(|loaded| convert_loaded_addresses(loaded, slot, index))
            .transpose()?
            .unwrap_or_default(),
        return_data: None,
        compute_units_consumed: None,
        cost_units: None,
    })
}

fn canonical_records() -> Result<&'static [CanonicalRecord], String> {
    CANONICAL_RECORDS
        .as_ref()
        .map(Box::as_ref)
        .map_err(Clone::clone)
}

pub(crate) fn validate_canonical_missing_status_registry() -> Result<(), String> {
    canonical_records().map(|_| ())
}

pub(crate) fn resolve_canonical_missing_transaction_status(
    slot: Slot,
    transaction_slot_index: usize,
    signature: &Signature,
) -> Result<Option<CanonicalMissingTransactionStatus>, String> {
    if !(FIRST_MISSING_SLOT..=LAST_MISSING_SLOT).contains(&slot) {
        return Ok(None);
    }
    let Ok(transaction_slot_index) = u32::try_from(transaction_slot_index) else {
        return Ok(None);
    };
    let records = canonical_records()?;
    let key = (slot, transaction_slot_index);
    let Ok(ordinal) = records.binary_search_by_key(&key, CanonicalRecord::key) else {
        return Ok(None);
    };
    let record = &records[ordinal];
    if record.signature != *signature {
        return Ok(None);
    }
    Ok(Some(CanonicalMissingTransactionStatus {
        slot: record.slot,
        transaction_slot_index: record.transaction_slot_index,
        signature: record.signature,
        metadata: record.metadata.clone(),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_is_source_bound_complete_and_exact() {
        validate_canonical_missing_status_registry().unwrap();
        let records = canonical_records().unwrap();
        assert_eq!(records.len(), RECORD_COUNT);
        assert_eq!(records.first().unwrap().slot, FIRST_MISSING_SLOT);
        assert_eq!(records.last().unwrap().slot, LAST_MISSING_SLOT);
    }

    #[test]
    fn exact_identity_resolves_and_nearby_or_reidentified_holes_do_not() {
        let records = canonical_records().unwrap();
        let record = &records[records.len() / 2];
        let resolved = resolve_canonical_missing_transaction_status(
            record.slot,
            record.transaction_slot_index as usize,
            &record.signature,
        )
        .unwrap()
        .unwrap();
        assert_eq!(resolved.slot, record.slot);
        assert_eq!(
            resolved.transaction_slot_index,
            record.transaction_slot_index
        );
        assert_eq!(resolved.signature, record.signature);
        assert_eq!(resolved.metadata, record.metadata);
        assert!(
            resolve_canonical_missing_transaction_status(
                record.slot,
                record.transaction_slot_index as usize,
                &Signature::default(),
            )
            .unwrap()
            .is_none()
        );
        assert!(
            resolve_canonical_missing_transaction_status(
                LAST_MISSING_SLOT + 1,
                0,
                &Signature::default(),
            )
            .unwrap()
            .is_none()
        );
    }
}
