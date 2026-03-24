use crate::instruction_iter::Instruction;
use crate::registry::DexDecoder;
use crate::token_transfers::{get_swap_amounts, get_token_account_info};
use crate::types::SwapRecord;
use jetstreamer_firehose::firehose::TransactionData;

const PROGRAM_ID: &str = "9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP";
const SWAP_DISC: u8 = 1;

pub struct OrcaV2Decoder;

impl DexDecoder for OrcaV2Decoder {
    fn name(&self) -> &'static str {
        "Orca V2"
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
        if data.is_empty() || data[0] != SWAP_DISC {
            return None;
        }

        let accounts = ix.accounts();
        let pool = accounts.first()?.to_string();
        let vault_a = accounts.get(4)?.to_string();
        let vault_b = accounts.get(5)?.to_string();

        let outer_idx = ix.instruction_index() as usize;
        let inner_idx = ix.inner_instruction_index() as usize;
        let amounts = get_swap_amounts(tx, outer_idx, inner_idx, &vault_a, &vault_b, None, false)?;

        let mut record = SwapRecord::default();
        record.instruction_index = ix.instruction_index();
        record.inner_instruction_index = ix.inner_instruction_index();
        record.is_inner_instruction = ix.is_inner();
        record.instruction_type = "Swap".to_string();
        record.outer_program = outer_program.to_string();
        record.inner_program = PROGRAM_ID.to_string();
        record.pool_address = pool;

        record.token_sold_mint = amounts.sold.mint.clone();
        record.token_sold_vault = amounts.sold.vault.clone();
        record.token_sold_amount = amounts.sold.amount_scaled();
        record.token_sold_decimals = amounts.sold.decimals;

        record.token_bought_mint = amounts.bought.mint.clone();
        record.token_bought_vault = amounts.bought.vault.clone();
        record.token_bought_amount = amounts.bought.amount_scaled();
        record.token_bought_decimals = amounts.bought.decimals;

        if let Some(info) = get_token_account_info(tx, &amounts.sold.vault) {
            record.token_sold_vault_reserve = info.post_balance_scaled();
        }
        if let Some(info) = get_token_account_info(tx, &amounts.bought.vault) {
            record.token_bought_vault_reserve = info.post_balance_scaled();
        }

        Some(record)
    }
}
