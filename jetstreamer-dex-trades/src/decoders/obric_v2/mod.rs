use crate::instruction_iter::Instruction;
use crate::token_transfers::{get_swap_amounts_partial, get_token_account_info};
use crate::types::SwapRecord;
use crate::registry::DexDecoder;
use jetstreamer_firehose::firehose::TransactionData;

const PROGRAM_ID: &str = "obriQD1zbpyLz95G5n7nJe6a4DPjpFwa5XYPoNm113y";

const SWAP_DISC: [u8; 8] = [248, 198, 158, 145, 225, 117, 135, 200];
const SWAP2_DISC: [u8; 8] = [65, 75, 63, 76, 235, 91, 91, 136];

pub struct ObricV2Decoder;

impl DexDecoder for ObricV2Decoder {
    fn name(&self) -> &'static str {
        "Obric V2"
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
        let accounts = ix.accounts();

        let instruction_type = if ix.has_discriminator(&SWAP_DISC) {
            "Swap"
        } else if ix.has_discriminator(&SWAP2_DISC) {
            "Swap2"
        } else {
            return None;
        };

        if accounts.len() < 5 {
            return None;
        }

        let pool = accounts[0].to_string();
        let base_vault = accounts[3].to_string();
        let quote_vault = accounts[4].to_string();

        let base_info = get_token_account_info(tx, &base_vault)?;
        let quote_info = get_token_account_info(tx, &quote_vault)?;

        let outer_idx = ix.instruction_index() as usize;
        let inner_idx = ix.inner_instruction_index() as usize;

        let partial = get_swap_amounts_partial(
            tx,
            outer_idx,
            inner_idx,
            &base_vault,
            &quote_vault,
            &base_info,
            &quote_info,
        )?;

        let sold = partial.sold.unwrap_or_else(|| {
            crate::token_transfers::TransferInfo {
                mint: base_info.mint.clone(),
                vault: base_vault.clone(),
                amount: 0,
                decimals: base_info.decimals,
            }
        });

        let bought = partial.bought.unwrap_or_else(|| {
            crate::token_transfers::TransferInfo {
                mint: quote_info.mint.clone(),
                vault: quote_vault.clone(),
                amount: 0,
                decimals: quote_info.decimals,
            }
        });

        let _ = data;

        let mut record = SwapRecord::default();
        record.instruction_index = ix.instruction_index();
        record.inner_instruction_index = ix.inner_instruction_index();
        record.is_inner_instruction = ix.is_inner();
        record.instruction_type = instruction_type.to_string();
        record.outer_program = outer_program.to_string();
        record.inner_program = PROGRAM_ID.to_string();
        record.pool_address = pool;

        record.token_sold_mint = sold.mint.clone();
        record.token_sold_vault = sold.vault.clone();
        record.token_sold_amount = sold.amount_scaled();
        record.token_sold_decimals = sold.decimals;

        record.token_bought_mint = bought.mint.clone();
        record.token_bought_vault = bought.vault.clone();
        record.token_bought_amount = bought.amount_scaled();
        record.token_bought_decimals = bought.decimals;

        let sold_vault_info = get_token_account_info(tx, &record.token_sold_vault);
        let bought_vault_info = get_token_account_info(tx, &record.token_bought_vault);
        record.token_sold_vault_reserve =
            sold_vault_info.map(|i| i.post_balance_scaled()).unwrap_or(0.0);
        record.token_bought_vault_reserve =
            bought_vault_info.map(|i| i.post_balance_scaled()).unwrap_or(0.0);

        Some(record)
    }
}
