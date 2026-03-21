use crate::instruction_iter::Instruction;
use crate::token_transfers::{get_swap_amounts, get_token_account_info};
use crate::types::SwapRecord;
use crate::registry::DexDecoder;
use jetstreamer_firehose::firehose::TransactionData;

const PROGRAM_ID: &str = "2wT8Yq49kHgDzXuPxZSaeLaH1qbmGXtEyPy64bL7aD3c";

const SWAP_DISC: [u8; 8] = [248, 198, 158, 145, 225, 117, 135, 200];

pub struct LifinityV2Decoder;

impl DexDecoder for LifinityV2Decoder {
    fn name(&self) -> &'static str {
        "Lifinity V2"
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
        let accounts = ix.accounts();

        if !ix.has_discriminator(&SWAP_DISC) {
            return None;
        }

        if accounts.len() < 7 {
            return None;
        }

        let pool = accounts[1].to_string();
        let vault_a = accounts[5].to_string();
        let vault_b = accounts[6].to_string();

        let outer_idx = ix.instruction_index() as usize;
        let inner_idx = ix.inner_instruction_index() as usize;

        let amounts = get_swap_amounts(
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

        let sold_vault_info = get_token_account_info(tx, &record.token_sold_vault);
        let bought_vault_info = get_token_account_info(tx, &record.token_bought_vault);
        record.token_sold_vault_reserve =
            sold_vault_info.map(|i| i.post_balance_scaled()).unwrap_or(0.0);
        record.token_bought_vault_reserve =
            bought_vault_info.map(|i| i.post_balance_scaled()).unwrap_or(0.0);

        Some(record)
    }
}
