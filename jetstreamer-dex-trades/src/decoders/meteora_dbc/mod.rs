mod idl;

use crate::instruction_iter::Instruction;
use crate::log_parser::LogParser;
use crate::registry::DexDecoder;
use crate::token_transfers::{get_swap_amounts, get_token_account_info};
use crate::transaction_ext::TransactionExt;
use crate::types::SwapRecord;
use jetstreamer_firehose::firehose::TransactionData;
use solana_address::Address;

const PROGRAM_ID: &str = "dbcij3LWUppWqq96dh6gJWwBifmcGfLSB5D4DuSMaqN";

pub struct MeteoraDbcDecoder;

impl MeteoraDbcDecoder {
    pub fn new() -> Self {
        Self
    }
}

impl Default for MeteoraDbcDecoder {
    fn default() -> Self {
        Self::new()
    }
}

impl DexDecoder for MeteoraDbcDecoder {
    fn name(&self) -> &'static str {
        "Meteora DBC"
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
        let accounts: Vec<String> = ix.accounts().iter().map(|a| a.to_string()).collect();

        if let Some((event, is_buy)) = idl::events::decode_swap_event(data) {
            return self.decode_from_event(tx, ix, outer_program, &accounts, &event, is_buy);
        }

        let is_buy = data.len() >= 8 && data[..8] == idl::BUY_DISC;
        let is_sell = data.len() >= 8 && data[..8] == idl::SELL_DISC;
        if !is_buy && !is_sell {
            return None;
        }

        let logs = tx.get_logs();
        if !LogParser::is_truncated(&logs) {
            let position = format!(
                "{}.{}",
                ix.instruction_index(),
                ix.inner_instruction_index()
            );
            let data_events = LogParser::extract_program_data_by_position(&logs, PROGRAM_ID);
            if let Some(events) = data_events.get(&position) {
                for event_data in events {
                    if let Ok(decoded) = base64::Engine::decode(
                        &base64::engine::general_purpose::STANDARD,
                        event_data,
                    ) && let Some((event, _)) = idl::events::decode_swap_event(&decoded)
                    {
                        return self.decode_from_event(
                            tx,
                            ix,
                            outer_program,
                            &accounts,
                            &event,
                            is_buy,
                        );
                    }
                }
            }
        }

        self.decode_from_transfers(tx, ix, outer_program, &accounts, is_buy)
    }
}

impl MeteoraDbcDecoder {
    fn decode_from_event(
        &self,
        tx: &TransactionData,
        ix: &Instruction,
        outer_program: &str,
        accounts: &[String],
        event: &idl::SwapEvent,
        is_buy: bool,
    ) -> Option<SwapRecord> {
        let pool = Address::new_from_array(event.pool).to_string();
        let token_a_mint = Address::new_from_array(event.token_a_mint).to_string();
        let token_b_mint = Address::new_from_array(event.token_b_mint).to_string();

        let base_vault = accounts.get(4).cloned().unwrap_or_default();
        let quote_vault = accounts.get(5).cloned().unwrap_or_default();

        let base_info = get_token_account_info(tx, &base_vault);
        let quote_info = get_token_account_info(tx, &quote_vault);

        let base_decimals = base_info.as_ref().map(|i| i.decimals).unwrap_or(6);
        let quote_decimals = quote_info.as_ref().map(|i| i.decimals).unwrap_or(6);

        let token_a_amount = event.token_a_amount as f64 / 10f64.powi(base_decimals as i32);
        let token_b_amount = event.token_b_amount as f64 / 10f64.powi(quote_decimals as i32);

        let base_reserve = base_info
            .as_ref()
            .map(|i| i.post_balance_scaled())
            .unwrap_or(0.0);
        let quote_reserve = quote_info
            .as_ref()
            .map(|i| i.post_balance_scaled())
            .unwrap_or(0.0);

        let mut record = SwapRecord::default();
        record.instruction_index = ix.instruction_index();
        record.inner_instruction_index = ix.inner_instruction_index();
        record.is_inner_instruction = ix.is_inner();
        record.outer_program = outer_program.to_string();
        record.inner_program = PROGRAM_ID.to_string();
        record.pool_address = pool;

        if is_buy {
            record.instruction_type = "Buy".to_string();
            record.token_bought_mint = token_a_mint;
            record.token_bought_vault = base_vault;
            record.token_bought_amount = token_a_amount;
            record.token_bought_decimals = base_decimals;
            record.token_bought_vault_reserve = base_reserve;
            record.token_sold_mint = token_b_mint;
            record.token_sold_vault = quote_vault;
            record.token_sold_amount = token_b_amount;
            record.token_sold_decimals = quote_decimals;
            record.token_sold_vault_reserve = quote_reserve;
        } else {
            record.instruction_type = "Sell".to_string();
            record.token_bought_mint = token_b_mint;
            record.token_bought_vault = quote_vault;
            record.token_bought_amount = token_b_amount;
            record.token_bought_decimals = quote_decimals;
            record.token_bought_vault_reserve = quote_reserve;
            record.token_sold_mint = token_a_mint;
            record.token_sold_vault = base_vault;
            record.token_sold_amount = token_a_amount;
            record.token_sold_decimals = base_decimals;
            record.token_sold_vault_reserve = base_reserve;
        }

        Some(record)
    }

    fn decode_from_transfers(
        &self,
        tx: &TransactionData,
        ix: &Instruction,
        outer_program: &str,
        accounts: &[String],
        is_buy: bool,
    ) -> Option<SwapRecord> {
        let pool_address = accounts.first().cloned().unwrap_or_default();
        let base_vault = accounts.get(4).cloned().unwrap_or_default();
        let quote_vault = accounts.get(5).cloned().unwrap_or_default();

        let outer_idx = ix.instruction_index() as usize;
        let inner_idx = ix.inner_instruction_index() as usize;
        let swap = get_swap_amounts(
            tx,
            outer_idx,
            inner_idx,
            &base_vault,
            &quote_vault,
            None,
            false,
        )?;

        let base_reserve = get_token_account_info(tx, &base_vault)
            .map(|i| i.post_balance_scaled())
            .unwrap_or(0.0);
        let quote_reserve = get_token_account_info(tx, &quote_vault)
            .map(|i| i.post_balance_scaled())
            .unwrap_or(0.0);

        let mut record = SwapRecord::default();
        record.instruction_index = ix.instruction_index();
        record.inner_instruction_index = ix.inner_instruction_index();
        record.is_inner_instruction = ix.is_inner();
        record.instruction_type = if is_buy { "Buy" } else { "Sell" }.to_string();
        record.outer_program = outer_program.to_string();
        record.inner_program = PROGRAM_ID.to_string();
        record.pool_address = pool_address;

        record.token_bought_mint = swap.bought.mint.clone();
        record.token_bought_vault = swap.bought.vault.clone();
        record.token_bought_amount = swap.bought.amount_scaled();
        record.token_bought_decimals = swap.bought.decimals;
        record.token_bought_vault_reserve = if swap.bought.vault == base_vault {
            base_reserve
        } else {
            quote_reserve
        };

        record.token_sold_mint = swap.sold.mint.clone();
        record.token_sold_vault = swap.sold.vault.clone();
        record.token_sold_amount = swap.sold.amount_scaled();
        record.token_sold_decimals = swap.sold.decimals;
        record.token_sold_vault_reserve = if swap.sold.vault == base_vault {
            base_reserve
        } else {
            quote_reserve
        };

        Some(record)
    }
}
