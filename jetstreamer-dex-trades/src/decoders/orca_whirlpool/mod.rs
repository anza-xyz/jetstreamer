pub mod idl;

use crate::instruction_iter::Instruction;
use crate::log_parser::LogParser;
use crate::registry::DexDecoder;
use crate::token_transfers::{get_swap_amounts, get_token_account_info};
use crate::transaction_ext::TransactionExt;
use crate::types::SwapRecord;
use jetstreamer_firehose::firehose::TransactionData;
use std::collections::{HashMap, VecDeque};
use std::sync::RwLock;

const PROGRAM_ID: &str = "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc";

// swap: pool at accounts[2], vault_a=4, vault_b=6
const SWAP_POOL_IDX: usize = 2;
const SWAP_VAULT_A_IDX: usize = 4;
const SWAP_VAULT_B_IDX: usize = 6;

// swapV2: pool at accounts[4], vault_a=8, vault_b=10
const SWAP_V2_POOL_IDX: usize = 4;
const SWAP_V2_VAULT_A_IDX: usize = 8;
const SWAP_V2_VAULT_B_IDX: usize = 10;

// twoHopSwap: pool_one=2, vault_one_a=5, vault_one_b=7, pool_two=3, vault_two_a=9, vault_two_b=11
const TWO_HOP_POOL_ONE_IDX: usize = 2;
const TWO_HOP_V1A_IDX: usize = 5;
const TWO_HOP_V1B_IDX: usize = 7;
const TWO_HOP_POOL_TWO_IDX: usize = 3;
const TWO_HOP_V2A_IDX: usize = 9;
const TWO_HOP_V2B_IDX: usize = 11;

// twoHopSwapV2: pool_one=0, pool_two=1
const TWO_HOP_V2_POOL_ONE_IDX: usize = 0;
const TWO_HOP_V2_V1A_IDX: usize = 9;
const TWO_HOP_V2_V1B_IDX: usize = 10;
const TWO_HOP_V2_POOL_TWO_IDX: usize = 1;
const TWO_HOP_V2_V2A_IDX: usize = 11;
const TWO_HOP_V2_V2B_IDX: usize = 12;

struct EventCache {
    events: VecDeque<idl::SwapEvent>,
    truncated: bool,
}

pub struct OrcaWhirlpoolDecoder {
    cache: RwLock<HashMap<String, EventCache>>,
}

impl OrcaWhirlpoolDecoder {
    pub fn new() -> Self {
        Self {
            cache: RwLock::new(HashMap::new()),
        }
    }

    fn pop_event(&self, tx_id: &str) -> Option<idl::SwapEvent> {
        let mut cache = self.cache.write().unwrap();
        cache.get_mut(tx_id).and_then(|c| c.events.pop_front())
    }

    fn is_truncated(&self, tx_id: &str) -> bool {
        let cache = self.cache.read().unwrap();
        cache.get(tx_id).map(|c| c.truncated).unwrap_or(false)
    }

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
        record.inner_program = if ix.is_inner() {
            PROGRAM_ID.to_string()
        } else {
            String::new()
        };
        record.pool_address = pool;
        record.sqrt_price = event.sqrt_price_x64.to_string();
        record.is_base_to_quote = Some(event.zero_for_one);

        if event.zero_for_one {
            record.token_sold_mint = info_0.mint.clone();
            record.token_sold_vault = vault_0;
            record.token_sold_amount =
                event.amount_0 as f64 / 10f64.powi(info_0.decimals as i32);
            record.token_sold_decimals = info_0.decimals;
            record.token_sold_vault_reserve = info_0.post_balance_scaled();

            record.token_bought_mint = info_1.mint.clone();
            record.token_bought_vault = vault_1;
            record.token_bought_amount =
                event.amount_1 as f64 / 10f64.powi(info_1.decimals as i32);
            record.token_bought_decimals = info_1.decimals;
            record.token_bought_vault_reserve = info_1.post_balance_scaled();
        } else {
            record.token_sold_mint = info_1.mint.clone();
            record.token_sold_vault = vault_1;
            record.token_sold_amount =
                event.amount_1 as f64 / 10f64.powi(info_1.decimals as i32);
            record.token_sold_decimals = info_1.decimals;
            record.token_sold_vault_reserve = info_1.post_balance_scaled();

            record.token_bought_mint = info_0.mint.clone();
            record.token_bought_vault = vault_0;
            record.token_bought_amount =
                event.amount_0 as f64 / 10f64.powi(info_0.decimals as i32);
            record.token_bought_decimals = info_0.decimals;
            record.token_bought_vault_reserve = info_0.post_balance_scaled();
        }

        Some(record)
    }

    fn decode_from_transfers(
        &self,
        tx: &TransactionData,
        ix: &Instruction,
        outer_program: &str,
        instruction_type: &str,
        pool_idx: usize,
        vault_a_idx: usize,
        vault_b_idx: usize,
    ) -> Option<SwapRecord> {
        let accounts = ix.accounts();
        let pool = accounts.get(pool_idx)?;
        let vault_a = accounts.get(vault_a_idx)?;
        let vault_b = accounts.get(vault_b_idx)?;

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
        record.instruction_type = instruction_type.to_string();
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

impl DexDecoder for OrcaWhirlpoolDecoder {
    fn name(&self) -> &'static str {
        "Orca Whirlpool"
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
            if let Some(event) = idl::events::decode_swap_event_from_log(&data) {
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
        let data = ix.data();
        if data.len() < 8 {
            return None;
        }

        let (instruction_type, pool_idx, vault_a_idx, vault_b_idx) = match &data[..8] {
            d if d == idl::SWAP_DISC => ("swap", SWAP_POOL_IDX, SWAP_VAULT_A_IDX, SWAP_VAULT_B_IDX),
            d if d == idl::SWAP_V2_DISC => (
                "swapV2",
                SWAP_V2_POOL_IDX,
                SWAP_V2_VAULT_A_IDX,
                SWAP_V2_VAULT_B_IDX,
            ),
            _ => return None,
        };

        let tx_id = tx.signature.to_string();

        if let Some(event) = self.pop_event(&tx_id) {
            if let Some(record) =
                self.decode_from_event(tx, ix, outer_program, instruction_type, &event)
            {
                return Some(record);
            }
        }

        self.decode_from_transfers(
            tx,
            ix,
            outer_program,
            instruction_type,
            pool_idx,
            vault_a_idx,
            vault_b_idx,
        )
    }

    fn decode_instructions(
        &self,
        tx: &TransactionData,
        ix: &Instruction,
        outer_program: &str,
    ) -> Vec<SwapRecord> {
        let data = ix.data();
        if data.len() < 8 {
            return Vec::new();
        }

        let hops: &[(
            &str,
            usize,
            usize,
            usize,
            usize,
            usize,
            usize,
        )] = match &data[..8] {
            d if d == idl::TWO_HOP_SWAP_DISC => &[(
                "twoHopSwap",
                TWO_HOP_POOL_ONE_IDX,
                TWO_HOP_V1A_IDX,
                TWO_HOP_V1B_IDX,
                TWO_HOP_POOL_TWO_IDX,
                TWO_HOP_V2A_IDX,
                TWO_HOP_V2B_IDX,
            )],
            d if d == idl::TWO_HOP_SWAP_V2_DISC => &[(
                "twoHopSwapV2",
                TWO_HOP_V2_POOL_ONE_IDX,
                TWO_HOP_V2_V1A_IDX,
                TWO_HOP_V2_V1B_IDX,
                TWO_HOP_V2_POOL_TWO_IDX,
                TWO_HOP_V2_V2A_IDX,
                TWO_HOP_V2_V2B_IDX,
            )],
            _ => {
                return self
                    .decode_instruction(tx, ix, outer_program)
                    .into_iter()
                    .collect();
            }
        };

        let (instruction_type, p1, v1a, v1b, p2, v2a, v2b) = hops[0];
        let tx_id = tx.signature.to_string();
        let mut records = Vec::new();

        for &(pool_idx, va_idx, vb_idx) in &[(p1, v1a, v1b), (p2, v2a, v2b)] {
            if let Some(event) = self.pop_event(&tx_id) {
                if let Some(record) =
                    self.decode_from_event(tx, ix, outer_program, instruction_type, &event)
                {
                    records.push(record);
                    continue;
                }
            }

            if let Some(record) = self.decode_from_transfers(
                tx,
                ix,
                outer_program,
                instruction_type,
                pool_idx,
                va_idx,
                vb_idx,
            ) {
                records.push(record);
            }
        }

        records
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
