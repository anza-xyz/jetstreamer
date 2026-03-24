use crate::instruction_iter::Instruction;
use crate::registry::DexDecoder;
use crate::token_transfers::{get_swap_amounts, get_token_account_info};
use crate::types::SwapRecord;
use jetstreamer_firehose::firehose::TransactionData;

const PROGRAM_ID: &str = "goonERTdGsjnkZqWuVjs73BZ3Pb9qoCUdBUL17BnS5j";
const SWAP_DISC: [u8; 8] = [248, 198, 158, 145, 225, 117, 135, 200];

pub struct GoonfiDecoder;

impl GoonfiDecoder {
    pub fn new() -> Self {
        Self
    }
}

impl Default for GoonfiDecoder {
    fn default() -> Self {
        Self::new()
    }
}

impl DexDecoder for GoonfiDecoder {
    fn name(&self) -> &'static str {
        "GoonFi"
    }

    fn program_ids(&self) -> &[&'static str] {
        &[PROGRAM_ID]
    }

    fn decode_instruction(
        &self,
        tx: &TransactionData,
        ix: &Instruction,
        outer_program: &str,
    ) -> Option<SwapRecord> {
        let data = ix.data();
        if data.len() < 8 || data[..8] != SWAP_DISC {
            return None;
        }

        let accounts: Vec<String> = ix.accounts().iter().map(|a| a.to_string()).collect();
        let pool = accounts.first().cloned().unwrap_or_default();
        let vault_a = accounts.get(3).cloned().unwrap_or_default();
        let vault_b = accounts.get(4).cloned().unwrap_or_default();

        let outer_idx = ix.instruction_index() as usize;
        let inner_idx = ix.inner_instruction_index() as usize;

        let swap = get_swap_amounts(tx, outer_idx, inner_idx, &vault_a, &vault_b, None, false)?;

        let vault_a_info = get_token_account_info(tx, &vault_a);
        let vault_b_info = get_token_account_info(tx, &vault_b);

        let mut record = SwapRecord::default();
        record.instruction_index = ix.instruction_index();
        record.inner_instruction_index = ix.inner_instruction_index();
        record.is_inner_instruction = ix.is_inner();
        record.outer_program = outer_program.to_string();
        record.inner_program = PROGRAM_ID.to_string();
        record.pool_address = pool;
        record.instruction_type = "Swap".to_string();

        record.token_sold_mint = swap.sold.mint.clone();
        record.token_sold_vault = swap.sold.vault.clone();
        record.token_sold_amount = swap.sold.amount_scaled();
        record.token_sold_decimals = swap.sold.decimals;
        record.token_bought_mint = swap.bought.mint.clone();
        record.token_bought_vault = swap.bought.vault.clone();
        record.token_bought_amount = swap.bought.amount_scaled();
        record.token_bought_decimals = swap.bought.decimals;

        record.token_sold_vault_reserve = if swap.sold.vault == vault_a {
            vault_a_info
                .as_ref()
                .map(|i| i.post_balance_scaled())
                .unwrap_or(0.0)
        } else {
            vault_b_info
                .as_ref()
                .map(|i| i.post_balance_scaled())
                .unwrap_or(0.0)
        };
        record.token_bought_vault_reserve = if swap.bought.vault == vault_a {
            vault_a_info
                .as_ref()
                .map(|i| i.post_balance_scaled())
                .unwrap_or(0.0)
        } else {
            vault_b_info
                .as_ref()
                .map(|i| i.post_balance_scaled())
                .unwrap_or(0.0)
        };

        Some(record)
    }
}
