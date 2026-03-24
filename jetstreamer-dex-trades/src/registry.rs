//! DEX decoder registry — dispatches transactions to the correct decoder.

use crate::instruction_iter::{Instruction, InstructionIteratorExt};
use crate::transaction_ext::TransactionExt;
use crate::types::SwapRecord;
use chrono::NaiveDate;
use jetstreamer_firehose::firehose::TransactionData;
use std::collections::HashMap;

const SOL_DECIMALS: i32 = 9;

/// Transaction-level common fields computed once per transaction.
#[derive(Clone)]
pub struct TxContext {
    pub block_slot: u64,
    pub block_time: i64,
    pub block_date: NaiveDate,
    pub tx_id: String,
    pub tx_index: u32,
    pub signer: String,
    pub txn_fee: f64,
    pub priority_fee: f64,
    pub jito_tips: f64,
    pub compute_units_consumed: u64,
}

impl TxContext {
    /// Build context from a transaction.
    ///
    /// `block_time` is passed in separately because the public jetstreamer
    /// `TransactionData` does not carry it (it lives on `BlockData`).
    pub fn from_tx(tx: &TransactionData, block_time: i64) -> Self {
        let block_date = chrono::DateTime::from_timestamp(block_time, 0)
            .map(|dt| dt.date_naive())
            .unwrap_or_else(|| NaiveDate::from_ymd_opt(1970, 1, 1).unwrap());

        let account_keys = tx.account_keys();
        let signer = account_keys
            .first()
            .map(|a| a.to_string())
            .unwrap_or_default();

        Self {
            block_slot: tx.slot,
            block_time,
            block_date,
            tx_id: tx.signature.to_string(),
            tx_index: tx.transaction_slot_index as u32,
            signer,
            txn_fee: tx.get_total_fee() as f64 / 10f64.powi(SOL_DECIMALS),
            priority_fee: tx.get_priority_fee() as f64 / 10f64.powi(SOL_DECIMALS),
            jito_tips: tx.get_jito_tips() as f64 / 10f64.powi(SOL_DECIMALS),
            compute_units_consumed: tx
                .transaction_status_meta
                .compute_units_consumed
                .unwrap_or(0),
        }
    }

    pub fn apply_to(&self, record: &mut SwapRecord) {
        record.block_slot = self.block_slot;
        record.block_time = self.block_time;
        record.block_date = self.block_date;
        record.tx_id = self.tx_id.clone();
        record.tx_index = self.tx_index;
        record.signer = self.signer.clone();
        record.txn_fee = self.txn_fee;
        record.priority_fee = self.priority_fee;
        record.jito_tips = self.jito_tips;
        record.compute_units_consumed = self.compute_units_consumed;
    }
}

/// Trait that all DEX decoders implement.
pub trait DexDecoder: Send + Sync {
    fn name(&self) -> &'static str;
    fn program_ids(&self) -> &[&'static str];

    /// Decode a single instruction. Returns `Some(SwapRecord)` with
    /// swap-specific fields filled; the registry will apply common fields.
    fn decode_instruction(
        &self,
        tx: &TransactionData,
        ix: &Instruction,
        outer_program: &str,
    ) -> Option<SwapRecord>;

    /// Decode instruction(s) — override for multi-hop swaps that produce
    /// multiple records from one instruction.
    fn decode_instructions(
        &self,
        tx: &TransactionData,
        ix: &Instruction,
        outer_program: &str,
    ) -> Vec<SwapRecord> {
        self.decode_instruction(tx, ix, outer_program)
            .into_iter()
            .collect()
    }

    fn on_transaction_start(&self, _tx: &TransactionData) {}
    fn on_transaction_end(&self, _tx_id: &str) {}
}

/// Registry of all DEX decoders. Maps program IDs to decoder instances and
/// drives the decoding loop.
pub struct DexRegistry {
    decoders: Vec<Box<dyn DexDecoder>>,
    program_map: HashMap<String, usize>,
}

impl Default for DexRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl DexRegistry {
    pub fn new() -> Self {
        let mut registry = Self {
            decoders: Vec::new(),
            program_map: HashMap::new(),
        };

        crate::decoders::register_all(&mut registry);

        log::info!(
            "registered {} DEX decoders covering {} program IDs",
            registry.decoders.len(),
            registry.program_map.len()
        );

        registry
    }

    /// Register a single decoder.
    pub fn register(&mut self, decoder: Box<dyn DexDecoder>) {
        let index = self.decoders.len();
        for program_id in decoder.program_ids() {
            self.program_map.insert(program_id.to_string(), index);
        }
        log::info!(
            "  {} ({})",
            decoder.name(),
            decoder.program_ids().join(", ")
        );
        self.decoders.push(decoder);
    }

    /// Decode all swaps from a transaction.
    ///
    /// `block_time` must be supplied because the public `TransactionData`
    /// struct does not include it.
    pub fn decode_transaction(&self, tx: &TransactionData, block_time: i64) -> Vec<SwapRecord> {
        if tx.is_vote_transaction() || tx.is_failed() {
            return Vec::new();
        }

        let mut swaps = Vec::new();
        let tx_id = tx.signature.to_string();
        let mut active_decoders = Vec::new();
        let mut outer_programs: HashMap<usize, String> = HashMap::new();

        for ix in tx.all_instructions() {
            let program_id = ix.program_id().to_string();

            if let Instruction::TopLevel { index, .. } = &ix {
                outer_programs.insert(*index, program_id.clone());
            }

            let decoder_idx = match self.program_map.get(&program_id) {
                Some(&idx) => idx,
                None => continue,
            };

            let decoder = &self.decoders[decoder_idx];

            if !active_decoders.contains(&decoder_idx) {
                decoder.on_transaction_start(tx);
                active_decoders.push(decoder_idx);
            }

            let outer_program = match &ix {
                Instruction::TopLevel { .. } => program_id.clone(),
                Instruction::Inner { parent_index, .. } => outer_programs
                    .get(parent_index)
                    .cloned()
                    .unwrap_or_default(),
            };

            let decoded = decoder.decode_instructions(tx, &ix, &outer_program);
            swaps.extend(decoded);
        }

        for idx in active_decoders {
            self.decoders[idx].on_transaction_end(&tx_id);
        }

        if !swaps.is_empty() {
            let tx_context = TxContext::from_tx(tx, block_time);
            for swap in &mut swaps {
                tx_context.apply_to(swap);
            }
        }

        swaps
    }

    pub fn decoder_count(&self) -> usize {
        self.decoders.len()
    }

    pub fn supported_program_ids(&self) -> Vec<&str> {
        self.decoders
            .iter()
            .flat_map(|d| d.program_ids().iter().copied())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_registry_registers_all_decoders() {
        let registry = DexRegistry::new();
        assert!(
            registry.decoder_count() >= 30,
            "expected >= 30 decoders, got {}",
            registry.decoder_count()
        );
    }

    #[test]
    fn test_registry_covers_expected_programs() {
        let registry = DexRegistry::new();
        let program_ids = registry.supported_program_ids();

        let expected = [
            "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P", // Pump Fun
            "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA", // Pump AMM
            "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8", // Raydium AMM V4
            "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C", // Raydium CPMM
            "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK", // Raydium CLMM
            "LanMV9sAd7wArD4vJFi2qDdfnVhFxYSUg6eADduJ3uj", // Raydium LaunchLab
            "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc", // Orca Whirlpool
            "9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP", // Orca V2
            "cpamdpZCGKUy5JxQXB4dcpGPiikHawvSWAd6mEn1sGG", // Meteora DAMM V2
            "dbcij3LWUppWqq96dh6gJWwBifmcGfLSB5D4DuSMaqN", // Meteora DBC
            "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo", // Meteora DLMM
            "Eo7WjKq67rjJQSZxS6z3YkapzY3eMj6Xy8X5EQVn5UaB", // Meteora Pools
            "MNFSTqtC93rEfYHB6hF82sKdZpUDFWkViLByLd1k1Ms", // Manifest
            "AQU1FRd7papthgdrwPTTq5JacJh8YtwEXaBfKU3bTz45", // Aquifer
            "obriQD1zbpyLz95G5n7nJe6a4DPjpFwa5XYPoNm113y", // Obric V2
            "swapNyd8XiQwJ6ianp9snpu4brUqFxadzvHebnAXjJZ", // Stabble Stable
            "swapFpHZwjELNnjvThjajtiVmkz3yPQEHjLtka2fwHW", // Stabble Weighted
            "EewxydAPCCVuNEyrVN68PuSYdQ7wKn27V9Gjeoi8dy3S", // Lifinity V1
            "2wT8Yq49kHgDzXuPxZSaeLaH1qbmGXtEyPy64bL7aD3c", // Lifinity V2
            "FUTARELBfJfQ8RDGhg1wdhddq1odMAJUePHFuBYfUxKq", // Futarchy AMM
            "HpNfyc2Saw7RKkQd8nEL4khUcuPhQ7WwY1B2qjx8jxFq", // PancakeSwap CLMM
            "REALQqNEomY6cQGZJUGwywTBD2UmDT32rZcNnfxQ5N2", // Byreal CLMM
            "SoLFiHG9TfgtdUXUjWAxi3LtvYuFyDLVhBWxdMZxyCe", // SolFi V1
            "SV2EYYJyRz2YhfXwXnhNAevDEui5Q6yrfyo13WtupPF", // SolFi V2
            "ALPHAQmeA7bjrVuccPsYPiCvsi428SNwte66Srvs4pHA", // AlphaQ
        ];

        for program_id in &expected {
            assert!(
                program_ids.contains(program_id),
                "missing program ID: {}",
                program_id
            );
        }
    }

    #[test]
    fn test_no_duplicate_program_ids() {
        let registry = DexRegistry::new();
        let program_ids = registry.supported_program_ids();
        let mut seen = std::collections::HashSet::new();
        for id in &program_ids {
            assert!(seen.insert(id), "duplicate program ID registered: {}", id);
        }
    }
}
