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

const PROGRAM_ID: &str = "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK";

const POOL_ACCOUNT_IDX: usize = 2;
const INPUT_VAULT_IDX: usize = 5;
const OUTPUT_VAULT_IDX: usize = 6;
const MIN_ACCOUNTS: usize = 7;

struct EventCache {
    events: VecDeque<idl::SwapEvent>,
}

pub struct RaydiumClmmDecoder {
    cache: RwLock<HashMap<String, EventCache>>,
}

impl RaydiumClmmDecoder {
    pub fn new() -> Self {
        Self {
            cache: RwLock::new(HashMap::new()),
        }
    }
}

impl DexDecoder for RaydiumClmmDecoder {
    fn name(&self) -> &'static str {
        "Raydium CLMM"
    }

    fn program_ids(&self) -> &[&'static str] {
        &[PROGRAM_ID]
    }

    fn on_transaction_start(&self, tx: &TransactionData) {
        let logs = tx.get_logs();
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
            if let Some(event) = idl::events::decode_swap_event_from_log(&data) {
                events.push_back(event);
            }
        }

        let tx_id = tx.signature.to_string();
        let mut cache = self.cache.write().unwrap();
        cache.insert(tx_id, EventCache { events });
    }

    fn on_transaction_end(&self, tx_id: &str) {
        let mut cache = self.cache.write().unwrap();
        cache.remove(tx_id);
    }

    fn decode_instruction(
        &self,
        tx: &TransactionData,
        ix: &Instruction,
        outer_program: &str,
    ) -> Option<SwapRecord> {
        let data = ix.data();
        if data.len() < 8 {
            return None;
        }

        let disc = &data[..8];
        let instruction_type = if disc == idl::SWAP_DISC {
            "Swap"
        } else if disc == idl::SWAP_V2_DISC {
            "SwapV2"
        } else {
            return None;
        };

        let accounts = ix.accounts();
        if accounts.len() < MIN_ACCOUNTS {
            return None;
        }

        let tx_id = tx.signature.to_string();
        let mut cache = self.cache.write().unwrap();
        let event = cache
            .get_mut(&tx_id)
            .and_then(|e| e.events.pop_front());
        drop(cache);

        if let Some(event) = event {
            self.decode_from_event(tx, ix, outer_program, instruction_type, &event)
        } else {
            self.decode_from_transfers(tx, ix, outer_program, instruction_type, accounts)
        }
    }
}

impl RaydiumClmmDecoder {
    fn decode_from_event(
        &self,
        tx: &TransactionData,
        ix: &Instruction,
        outer_program: &str,
        instruction_type: &str,
        event: &idl::SwapEvent,
    ) -> Option<SwapRecord> {
        let pool = event.pool_state_str();
        let vault_0 = event.token_account_0_str();
        let vault_1 = event.token_account_1_str();

        let info_0 = get_token_account_info(tx, &vault_0)?;
        let info_1 = get_token_account_info(tx, &vault_1)?;

        let mut record = SwapRecord::default();
        record.instruction_index = ix.instruction_index();
        record.inner_instruction_index = ix.inner_instruction_index();
        record.is_inner_instruction = ix.is_inner();
        record.instruction_type = instruction_type.to_string();
        record.outer_program = outer_program.to_string();
        record.inner_program = PROGRAM_ID.to_string();
        record.pool_address = pool;

        record.sqrt_price = event.sqrt_price_x64.to_string();
        record.is_base_to_quote = Some(event.zero_for_one);

        if event.zero_for_one {
            // Selling token_0 → buying token_1
            record.token_sold_mint = info_0.mint.clone();
            record.token_sold_vault = vault_0;
            record.token_sold_amount =
                event.amount_0 as f64 / 10f64.powi(info_0.decimals as i32);
            record.token_sold_decimals = info_0.decimals;

            record.token_bought_mint = info_1.mint.clone();
            record.token_bought_vault = vault_1;
            record.token_bought_amount =
                event.amount_1 as f64 / 10f64.powi(info_1.decimals as i32);
            record.token_bought_decimals = info_1.decimals;
        } else {
            // Selling token_1 → buying token_0
            record.token_sold_mint = info_1.mint.clone();
            record.token_sold_vault = vault_1;
            record.token_sold_amount =
                event.amount_1 as f64 / 10f64.powi(info_1.decimals as i32);
            record.token_sold_decimals = info_1.decimals;

            record.token_bought_mint = info_0.mint.clone();
            record.token_bought_vault = vault_0;
            record.token_bought_amount =
                event.amount_0 as f64 / 10f64.powi(info_0.decimals as i32);
            record.token_bought_decimals = info_0.decimals;
        }

        Some(record)
    }

    fn decode_from_transfers(
        &self,
        tx: &TransactionData,
        ix: &Instruction,
        outer_program: &str,
        instruction_type: &str,
        accounts: &[Address],
    ) -> Option<SwapRecord> {
        let pool = accounts[POOL_ACCOUNT_IDX].to_string();
        let vault_a = accounts[INPUT_VAULT_IDX].to_string();
        let vault_b = accounts[OUTPUT_VAULT_IDX].to_string();

        let outer_idx = ix.instruction_index() as usize;
        let inner_idx = ix.inner_instruction_index() as usize;

        let swap = get_swap_amounts(
            tx,
            outer_idx,
            inner_idx,
            &vault_a,
            &vault_b,
            None,
            false,
        )?;

        let mut record = SwapRecord::default();
        record.instruction_index = ix.instruction_index();
        record.inner_instruction_index = ix.inner_instruction_index();
        record.is_inner_instruction = ix.is_inner();
        record.instruction_type = instruction_type.to_string();
        record.outer_program = outer_program.to_string();
        record.inner_program = PROGRAM_ID.to_string();
        record.pool_address = pool;

        record.token_sold_mint = swap.sold.mint.clone();
        record.token_sold_vault = swap.sold.vault.clone();
        record.token_sold_amount = swap.sold.amount_scaled();
        record.token_sold_decimals = swap.sold.decimals;

        record.token_bought_mint = swap.bought.mint.clone();
        record.token_bought_vault = swap.bought.vault.clone();
        record.token_bought_amount = swap.bought.amount_scaled();
        record.token_bought_decimals = swap.bought.decimals;

        Some(record)
    }
}

fn parse_position(pos: &str) -> (usize, usize) {
    let mut parts = pos.split('.');
    let outer = parts.next().and_then(|s| s.parse().ok()).unwrap_or(0);
    let inner = parts.next().and_then(|s| s.parse().ok()).unwrap_or(0);
    (outer, inner)
}
