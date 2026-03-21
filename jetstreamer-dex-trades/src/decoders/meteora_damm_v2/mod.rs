pub mod idl;

use crate::instruction_iter::Instruction;
use crate::log_parser::LogParser;
use crate::registry::DexDecoder;
use crate::token_transfers::{get_swap_amounts, get_token_account_info};
use crate::transaction_ext::TransactionExt;
use crate::types::SwapRecord;
use jetstreamer_firehose::firehose::TransactionData;
use solana_address::Address;
use std::collections::{HashMap, VecDeque};
use std::sync::RwLock;

const PROGRAM_ID: &str = "cpamdpZCGKUy5JxQXB4dcpGPiikHawvSWAd6mEn1sGG";

const POOL_INDEX: usize = 0;
const VAULT_A_INDEX: usize = 3;
const VAULT_B_INDEX: usize = 4;

struct EventCache {
    events: VecDeque<idl::SwapEvent>,
    truncated: bool,
}

pub struct MeteoraDammV2Decoder {
    cache: RwLock<HashMap<String, EventCache>>,
}

impl MeteoraDammV2Decoder {
    pub fn new() -> Self {
        Self {
            cache: RwLock::new(HashMap::new()),
        }
    }

    fn pop_event(&self, tx_id: &str) -> Option<idl::SwapEvent> {
        let mut cache = self.cache.write().unwrap();
        cache.get_mut(tx_id).and_then(|c| c.events.pop_front())
    }

    fn decode_from_event(
        &self,
        tx: &TransactionData,
        ix: &Instruction,
        outer_program: &str,
        event: &idl::SwapEvent,
    ) -> Option<SwapRecord> {
        let pool = Address::new_from_array(event.pool_id);
        let accounts = ix.accounts();
        let vault_a = accounts.get(VAULT_A_INDEX)?;
        let vault_b = accounts.get(VAULT_B_INDEX)?;

        let info_a = get_token_account_info(tx, &vault_a.to_string())?;
        let info_b = get_token_account_info(tx, &vault_b.to_string())?;

        let mut record = SwapRecord::default();
        record.instruction_index = ix.instruction_index();
        record.inner_instruction_index = ix.inner_instruction_index();
        record.is_inner_instruction = ix.is_inner();
        record.instruction_type = "swap".to_string();
        record.outer_program = outer_program.to_string();
        record.inner_program = if ix.is_inner() {
            PROGRAM_ID.to_string()
        } else {
            String::new()
        };
        record.pool_address = pool.to_string();
        record.sqrt_price = event.sqrt_price.to_string();
        record.is_base_to_quote = Some(event.trade_direction);

        if event.trade_direction {
            // A → B: selling A, buying B
            record.token_sold_mint = info_a.mint.clone();
            record.token_sold_vault = vault_a.to_string();
            record.token_sold_amount =
                event.amount_in as f64 / 10f64.powi(info_a.decimals as i32);
            record.token_sold_decimals = info_a.decimals;
            record.token_sold_vault_reserve = info_a.post_balance_scaled();

            record.token_bought_mint = info_b.mint.clone();
            record.token_bought_vault = vault_b.to_string();
            record.token_bought_amount =
                event.amount_out as f64 / 10f64.powi(info_b.decimals as i32);
            record.token_bought_decimals = info_b.decimals;
            record.token_bought_vault_reserve = info_b.post_balance_scaled();
        } else {
            // B → A: selling B, buying A
            record.token_sold_mint = info_b.mint.clone();
            record.token_sold_vault = vault_b.to_string();
            record.token_sold_amount =
                event.amount_in as f64 / 10f64.powi(info_b.decimals as i32);
            record.token_sold_decimals = info_b.decimals;
            record.token_sold_vault_reserve = info_b.post_balance_scaled();

            record.token_bought_mint = info_a.mint.clone();
            record.token_bought_vault = vault_a.to_string();
            record.token_bought_amount =
                event.amount_out as f64 / 10f64.powi(info_a.decimals as i32);
            record.token_bought_decimals = info_a.decimals;
            record.token_bought_vault_reserve = info_a.post_balance_scaled();
        }

        Some(record)
    }

    fn decode_from_transfers(
        &self,
        tx: &TransactionData,
        ix: &Instruction,
        outer_program: &str,
    ) -> Option<SwapRecord> {
        let accounts = ix.accounts();
        let pool = accounts.get(POOL_INDEX)?;
        let vault_a = accounts.get(VAULT_A_INDEX)?;
        let vault_b = accounts.get(VAULT_B_INDEX)?;

        let swap = get_swap_amounts(
            tx,
            ix.instruction_index() as usize,
            ix.inner_instruction_index() as usize,
            &vault_a.to_string(),
            &vault_b.to_string(),
            None,
            false,
        )?;

        let bought_reserve = get_token_account_info(tx, &swap.bought.vault)
            .map(|i| i.post_balance_scaled())
            .unwrap_or(0.0);
        let sold_reserve = get_token_account_info(tx, &swap.sold.vault)
            .map(|i| i.post_balance_scaled())
            .unwrap_or(0.0);

        let mut record = SwapRecord::default();
        record.instruction_index = ix.instruction_index();
        record.inner_instruction_index = ix.inner_instruction_index();
        record.is_inner_instruction = ix.is_inner();
        record.instruction_type = "swap".to_string();
        record.outer_program = outer_program.to_string();
        record.inner_program = if ix.is_inner() {
            PROGRAM_ID.to_string()
        } else {
            String::new()
        };
        record.pool_address = pool.to_string();

        record.token_sold_mint = swap.sold.mint.clone();
        record.token_sold_vault = swap.sold.vault.clone();
        record.token_sold_amount = swap.sold.amount_scaled();
        record.token_sold_decimals = swap.sold.decimals;
        record.token_sold_vault_reserve = sold_reserve;

        record.token_bought_mint = swap.bought.mint.clone();
        record.token_bought_vault = swap.bought.vault.clone();
        record.token_bought_amount = swap.bought.amount_scaled();
        record.token_bought_decimals = swap.bought.decimals;
        record.token_bought_vault_reserve = bought_reserve;

        Some(record)
    }
}

impl DexDecoder for MeteoraDammV2Decoder {
    fn name(&self) -> &'static str {
        "Meteora DAMM V2"
    }

    fn program_ids(&self) -> &[&'static str] {
        &[PROGRAM_ID]
    }

    fn on_transaction_start(&self, tx: &TransactionData) {
        let logs = tx.get_logs();
        let truncated = LogParser::is_truncated(&logs);

        let data_by_pos = LogParser::extract_program_data_by_position(&logs, PROGRAM_ID);
        let mut sorted: Vec<(String, String)> = Vec::new();
        for (pos, entries) in &data_by_pos {
            for entry in entries {
                sorted.push((pos.clone(), entry.clone()));
            }
        }
        sorted.sort_by(|a, b| parse_position(&a.0).cmp(&parse_position(&b.0)));

        let mut events = VecDeque::new();
        for (_pos, data) in sorted {
            if let Some(event) = idl::decode_swap_event_from_log(&data) {
                events.push_back(event);
            }
        }

        let tx_id = tx.signature.to_string();
        let mut cache = self.cache.write().unwrap();
        cache.insert(tx_id, EventCache { events, truncated });
    }

    fn decode_instruction(
        &self,
        tx: &TransactionData,
        ix: &Instruction,
        outer_program: &str,
    ) -> Option<SwapRecord> {
        if !ix.has_discriminator(&idl::SWAP_DISC) {
            return None;
        }

        let tx_id = tx.signature.to_string();

        if let Some(event) = self.pop_event(&tx_id) {
            if let Some(record) = self.decode_from_event(tx, ix, outer_program, &event) {
                return Some(record);
            }
        }

        self.decode_from_transfers(tx, ix, outer_program)
    }

    fn on_transaction_end(&self, tx_id: &str) {
        let mut cache = self.cache.write().unwrap();
        cache.remove(tx_id);
    }
}

fn parse_position(pos: &str) -> (usize, usize) {
    let mut parts = pos.split('.');
    let outer = parts.next().and_then(|s| s.parse().ok()).unwrap_or(0);
    let inner = parts.next().and_then(|s| s.parse().ok()).unwrap_or(0);
    (outer, inner)
}
