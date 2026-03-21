use crate::instruction_iter::Instruction;
use crate::token_transfers::{get_swap_amounts, get_token_account_info};
use crate::types::SwapRecord;
use crate::registry::DexDecoder;
use jetstreamer_firehose::firehose::TransactionData;

const PROGRAM_ID: &str = "swapNyd8XiQwJ6ianp9snpu4brUqFxadzvHebnAXjJZ";

const SWAP_DISC: [u8; 8] = [248, 198, 158, 145, 225, 117, 135, 200];
const SWAP_V2_DISC: [u8; 8] = [43, 4, 237, 11, 26, 201, 30, 98];

pub struct StabbleStableDecoder;

impl DexDecoder for StabbleStableDecoder {
    fn name(&self) -> &'static str {
        "Stabble Stable"
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

        let (instruction_type, pool, vault_a, vault_b, fee_account) =
            if ix.has_discriminator(&SWAP_DISC) {
                if accounts.len() < 7 {
                    return None;
                }
                (
                    "Swap",
                    accounts[6].to_string(),
                    accounts[3].to_string(),
                    accounts[4].to_string(),
                    accounts[5].to_string(),
                )
            } else if ix.has_discriminator(&SWAP_V2_DISC) {
                if accounts.len() < 9 {
                    return None;
                }
                (
                    "SwapV2",
                    accounts[8].to_string(),
                    accounts[5].to_string(),
                    accounts[6].to_string(),
                    accounts[7].to_string(),
                )
            } else {
                return None;
            };

        let outer_idx = ix.instruction_index() as usize;
        let inner_idx = ix.inner_instruction_index() as usize;

        let exclude = [fee_account.as_str()];
        let amounts = get_swap_amounts(
            tx,
            outer_idx,
            inner_idx,
            &vault_a,
            &vault_b,
            Some(&exclude),
            true,
        )?;

        let mut record = SwapRecord::default();
        record.instruction_index = ix.instruction_index();
        record.inner_instruction_index = ix.inner_instruction_index();
        record.is_inner_instruction = ix.is_inner();
        record.instruction_type = instruction_type.to_string();
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
