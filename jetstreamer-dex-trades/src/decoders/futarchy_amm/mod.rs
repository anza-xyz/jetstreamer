pub mod idl;

use crate::instruction_iter::Instruction;
use crate::token_transfers::get_token_account_info;
use crate::types::SwapRecord;
use crate::registry::DexDecoder;
use jetstreamer_firehose::firehose::TransactionData;
use solana_address::Address;
use std::collections::HashMap;
use std::sync::RwLock;

const PROGRAM_ID: &str = "FUTARELBfJfQ8RDGhg1wdhddq1odMAJUePHFuBYfUxKq";

/// Cached context for an instruction within a transaction.
#[derive(Debug, Clone)]
struct InstructionContext {
    pool: String,
    instruction_type: String,
}

pub struct FutarchyAmmDecoder {
    context_cache: RwLock<HashMap<String, Vec<InstructionContext>>>,
}

impl FutarchyAmmDecoder {
    pub fn new() -> Self {
        Self {
            context_cache: RwLock::new(HashMap::new()),
        }
    }
}

impl DexDecoder for FutarchyAmmDecoder {
    fn name(&self) -> &'static str {
        "Futarchy AMM"
    }

    fn program_ids(&self) -> &[&'static str] {
        &[PROGRAM_ID]
    }

    fn on_transaction_start(&self, tx: &TransactionData) {
        let tx_id = tx.signature.to_string();
        let mut contexts = Vec::new();

        for ix in crate::instruction_iter::InstructionIteratorExt::all_instructions(tx) {
            if ix.program_id().to_string() != PROGRAM_ID {
                continue;
            }

            let data = ix.data();
            let accounts = ix.accounts();

            if ix.has_discriminator(&idl::SPOT_SWAP_DISC) {
                let pool = accounts.first().map(|a| a.to_string()).unwrap_or_default();
                contexts.push(InstructionContext {
                    pool,
                    instruction_type: "spotSwap".to_string(),
                });
            } else if ix.has_discriminator(&idl::CONDITIONAL_SWAP_DISC) {
                let pool = accounts.first().map(|a| a.to_string()).unwrap_or_default();
                contexts.push(InstructionContext {
                    pool,
                    instruction_type: "conditionalSwap".to_string(),
                });
            }

            let _ = data;
        }

        if !contexts.is_empty() {
            if let Ok(mut cache) = self.context_cache.write() {
                cache.insert(tx_id, contexts);
            }
        }
    }

    fn on_transaction_end(&self, tx_id: &str) {
        if let Ok(mut cache) = self.context_cache.write() {
            cache.remove(tx_id);
        }
    }

    fn decode_instruction(
        &self,
        tx: &TransactionData,
        ix: &Instruction,
        outer_program: &str,
    ) -> Option<SwapRecord> {
        let data = ix.data();

        let event = idl::events::decode_event(data)?;

        match event {
            idl::events::FutarchyEvent::SpotSwap(e) => {
                self.decode_spot_swap(tx, ix, outer_program, &e)
            }
            idl::events::FutarchyEvent::ConditionalSwap(e) => {
                self.decode_conditional_swap(tx, ix, outer_program, &e)
            }
        }
    }
}

impl FutarchyAmmDecoder {
    fn get_instruction_context(&self, tx_id: &str, ix_type: &str) -> Option<InstructionContext> {
        let cache = self.context_cache.read().ok()?;
        let contexts = cache.get(tx_id)?;
        contexts
            .iter()
            .find(|ctx| ctx.instruction_type == ix_type)
            .cloned()
    }

    fn decode_spot_swap(
        &self,
        tx: &TransactionData,
        ix: &Instruction,
        outer_program: &str,
        event: &idl::SpotSwapEvent,
    ) -> Option<SwapRecord> {
        let tx_id = tx.signature.to_string();
        let dao = Address::new_from_array(event.dao);
        let user = Address::new_from_array(event.user);
        let amm = &event.post_amm_state;

        let pool = self
            .get_instruction_context(&tx_id, "spotSwap")
            .map(|ctx| ctx.pool)
            .unwrap_or_else(|| dao.to_string());

        let base_mint = amm.base_mint.to_string();
        let quote_mint = amm.quote_mint.to_string();
        let base_vault = amm.amm_base_vault.to_string();
        let quote_vault = amm.amm_quote_vault.to_string();

        let base_info = get_token_account_info(tx, &base_vault);
        let quote_info = get_token_account_info(tx, &quote_vault);

        let base_decimals = base_info.as_ref().map(|i| i.decimals).unwrap_or(6);
        let quote_decimals = quote_info.as_ref().map(|i| i.decimals).unwrap_or(6);

        let input_scaled = event.input_amount as f64 / 10f64.powi(
            if event.swap_type == idl::SwapType::Buy { quote_decimals } else { base_decimals } as i32
        );
        let output_scaled = event.output_amount as f64 / 10f64.powi(
            if event.swap_type == idl::SwapType::Buy { base_decimals } else { quote_decimals } as i32
        );

        let mut record = SwapRecord::default();
        record.instruction_index = ix.instruction_index();
        record.inner_instruction_index = ix.inner_instruction_index();
        record.is_inner_instruction = ix.is_inner();
        record.instruction_type = format!("spotSwap{}", event.swap_type.label());
        record.outer_program = outer_program.to_string();
        record.inner_program = PROGRAM_ID.to_string();
        record.pool_address = pool;
        record.signer = user.to_string();

        match event.swap_type {
            idl::SwapType::Buy => {
                record.token_bought_mint = base_mint;
                record.token_bought_vault = base_vault;
                record.token_bought_amount = output_scaled;
                record.token_bought_decimals = base_decimals;
                record.token_sold_mint = quote_mint;
                record.token_sold_vault = quote_vault;
                record.token_sold_amount = input_scaled;
                record.token_sold_decimals = quote_decimals;
            }
            idl::SwapType::Sell => {
                record.token_bought_mint = quote_mint;
                record.token_bought_vault = quote_vault;
                record.token_bought_amount = output_scaled;
                record.token_bought_decimals = quote_decimals;
                record.token_sold_mint = base_mint;
                record.token_sold_vault = base_vault;
                record.token_sold_amount = input_scaled;
                record.token_sold_decimals = base_decimals;
            }
        }

        let sold_vault_info = get_token_account_info(tx, &record.token_sold_vault);
        let bought_vault_info = get_token_account_info(tx, &record.token_bought_vault);
        record.token_sold_vault_reserve =
            sold_vault_info.map(|i| i.post_balance_scaled()).unwrap_or(0.0);
        record.token_bought_vault_reserve =
            bought_vault_info.map(|i| i.post_balance_scaled()).unwrap_or(0.0);

        Some(record)
    }

    fn decode_conditional_swap(
        &self,
        tx: &TransactionData,
        ix: &Instruction,
        outer_program: &str,
        event: &idl::ConditionalSwapEvent,
    ) -> Option<SwapRecord> {
        let tx_id = tx.signature.to_string();
        let dao = Address::new_from_array(event.dao);
        let user = Address::new_from_array(event.user);

        let pool = self
            .get_instruction_context(&tx_id, "conditionalSwap")
            .map(|ctx| ctx.pool)
            .unwrap_or_else(|| dao.to_string());

        // Use first pool in the array for mint/vault info
        let amm = &event.post_amm_state[0];

        let base_mint = amm.base_mint.to_string();
        let quote_mint = amm.quote_mint.to_string();
        let base_vault = amm.amm_base_vault.to_string();
        let quote_vault = amm.amm_quote_vault.to_string();

        let base_info = get_token_account_info(tx, &base_vault);
        let quote_info = get_token_account_info(tx, &quote_vault);

        let base_decimals = base_info.as_ref().map(|i| i.decimals).unwrap_or(6);
        let quote_decimals = quote_info.as_ref().map(|i| i.decimals).unwrap_or(6);

        let input_scaled = event.input_amount as f64 / 10f64.powi(
            if event.swap_type == idl::SwapType::Buy { quote_decimals } else { base_decimals } as i32
        );
        let output_scaled = event.output_amount as f64 / 10f64.powi(
            if event.swap_type == idl::SwapType::Buy { base_decimals } else { quote_decimals } as i32
        );

        let mut record = SwapRecord::default();
        record.instruction_index = ix.instruction_index();
        record.inner_instruction_index = ix.inner_instruction_index();
        record.is_inner_instruction = ix.is_inner();
        record.instruction_type = format!("conditionalSwap{}", event.swap_type.label());
        record.outer_program = outer_program.to_string();
        record.inner_program = PROGRAM_ID.to_string();
        record.pool_address = pool;
        record.signer = user.to_string();

        match event.swap_type {
            idl::SwapType::Buy => {
                record.token_bought_mint = base_mint;
                record.token_bought_vault = base_vault;
                record.token_bought_amount = output_scaled;
                record.token_bought_decimals = base_decimals;
                record.token_sold_mint = quote_mint;
                record.token_sold_vault = quote_vault;
                record.token_sold_amount = input_scaled;
                record.token_sold_decimals = quote_decimals;
            }
            idl::SwapType::Sell => {
                record.token_bought_mint = quote_mint;
                record.token_bought_vault = quote_vault;
                record.token_bought_amount = output_scaled;
                record.token_bought_decimals = quote_decimals;
                record.token_sold_mint = base_mint;
                record.token_sold_vault = base_vault;
                record.token_sold_amount = input_scaled;
                record.token_sold_decimals = base_decimals;
            }
        }

        let sold_vault_info = get_token_account_info(tx, &record.token_sold_vault);
        let bought_vault_info = get_token_account_info(tx, &record.token_bought_vault);
        record.token_sold_vault_reserve =
            sold_vault_info.map(|i| i.post_balance_scaled()).unwrap_or(0.0);
        record.token_bought_vault_reserve =
            bought_vault_info.map(|i| i.post_balance_scaled()).unwrap_or(0.0);

        Some(record)
    }
}
