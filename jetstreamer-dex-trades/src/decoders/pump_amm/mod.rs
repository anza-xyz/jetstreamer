mod idl;

use crate::instruction_iter::Instruction;
use crate::registry::DexDecoder;
use crate::token_transfers::get_token_account_info;
use crate::types::SwapRecord;
use jetstreamer_firehose::firehose::TransactionData;
use solana_address::Address;

const PROGRAM_ID: &str = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA";
const SOL_MINT: &str = "So11111111111111111111111111111111111111112";
const SOL_DECIMALS: u8 = 9;

pub struct PumpAmmDecoder;

impl DexDecoder for PumpAmmDecoder {
    fn name(&self) -> &'static str {
        "Pump AMM"
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
        let trade_event = idl::events::decode_trade_event(data)?;

        match trade_event {
            idl::events::TradeEvent::Buy(event) => self.decode_buy(tx, ix, outer_program, &event),
            idl::events::TradeEvent::Sell(event) => self.decode_sell(tx, ix, outer_program, &event),
        }
    }
}

impl PumpAmmDecoder {
    fn decode_buy(
        &self,
        tx: &TransactionData,
        ix: &Instruction,
        outer_program: &str,
        event: &idl::BuyEvent,
    ) -> Option<SwapRecord> {
        let pool = Address::new_from_array(event.pool);
        let user = Address::new_from_array(event.user);
        let user_base_account = Address::new_from_array(event.user_base_token_account);
        let user_quote_account = Address::new_from_array(event.user_quote_token_account);

        let base_info = get_token_account_info(tx, &user_base_account.to_string());
        let quote_info = get_token_account_info(tx, &user_quote_account.to_string());

        let base_decimals = base_info.as_ref().map(|i| i.decimals).unwrap_or(6);
        let quote_decimals = quote_info
            .as_ref()
            .map(|i| i.decimals)
            .unwrap_or(SOL_DECIMALS);
        let base_mint = base_info
            .as_ref()
            .map(|i| i.mint.clone())
            .unwrap_or_default();
        let quote_mint = quote_info
            .as_ref()
            .map(|i| i.mint.clone())
            .unwrap_or_else(|| SOL_MINT.to_string());

        let base_amount = event.base_amount_out as f64 / 10f64.powi(base_decimals as i32);
        let quote_amount = event.quote_amount_in as f64 / 10f64.powi(quote_decimals as i32);

        let pool_base_reserve =
            event.pool_base_token_reserves as f64 / 10f64.powi(base_decimals as i32);
        let pool_quote_reserve =
            event.pool_quote_token_reserves as f64 / 10f64.powi(quote_decimals as i32);

        let mut record = SwapRecord::default();
        record.instruction_index = ix.instruction_index();
        record.inner_instruction_index = ix.inner_instruction_index();
        record.is_inner_instruction = ix.is_inner();
        record.instruction_type = "Buy".to_string();
        record.outer_program = outer_program.to_string();
        record.inner_program = PROGRAM_ID.to_string();
        record.pool_address = pool.to_string();
        record.signer = user.to_string();

        record.token_bought_mint = base_mint;
        record.token_bought_vault = user_base_account.to_string();
        record.token_bought_amount = base_amount;
        record.token_bought_decimals = base_decimals;
        record.token_bought_vault_reserve = pool_base_reserve;

        record.token_sold_mint = quote_mint;
        record.token_sold_vault = user_quote_account.to_string();
        record.token_sold_amount = quote_amount;
        record.token_sold_decimals = quote_decimals;
        record.token_sold_vault_reserve = pool_quote_reserve;

        Some(record)
    }

    fn decode_sell(
        &self,
        tx: &TransactionData,
        ix: &Instruction,
        outer_program: &str,
        event: &idl::SellEvent,
    ) -> Option<SwapRecord> {
        let pool = Address::new_from_array(event.pool);
        let user = Address::new_from_array(event.user);
        let user_base_account = Address::new_from_array(event.user_base_token_account);
        let user_quote_account = Address::new_from_array(event.user_quote_token_account);

        let base_info = get_token_account_info(tx, &user_base_account.to_string());
        let quote_info = get_token_account_info(tx, &user_quote_account.to_string());

        let base_decimals = base_info.as_ref().map(|i| i.decimals).unwrap_or(6);
        let quote_decimals = quote_info
            .as_ref()
            .map(|i| i.decimals)
            .unwrap_or(SOL_DECIMALS);
        let base_mint = base_info
            .as_ref()
            .map(|i| i.mint.clone())
            .unwrap_or_default();
        let quote_mint = quote_info
            .as_ref()
            .map(|i| i.mint.clone())
            .unwrap_or_else(|| SOL_MINT.to_string());

        let base_amount = event.base_amount_in as f64 / 10f64.powi(base_decimals as i32);
        let quote_amount = event.quote_amount_out as f64 / 10f64.powi(quote_decimals as i32);

        let pool_base_reserve =
            event.pool_base_token_reserves as f64 / 10f64.powi(base_decimals as i32);
        let pool_quote_reserve =
            event.pool_quote_token_reserves as f64 / 10f64.powi(quote_decimals as i32);

        let mut record = SwapRecord::default();
        record.instruction_index = ix.instruction_index();
        record.inner_instruction_index = ix.inner_instruction_index();
        record.is_inner_instruction = ix.is_inner();
        record.instruction_type = "Sell".to_string();
        record.outer_program = outer_program.to_string();
        record.inner_program = PROGRAM_ID.to_string();
        record.pool_address = pool.to_string();
        record.signer = user.to_string();

        record.token_bought_mint = quote_mint;
        record.token_bought_vault = user_quote_account.to_string();
        record.token_bought_amount = quote_amount;
        record.token_bought_decimals = quote_decimals;
        record.token_bought_vault_reserve = pool_quote_reserve;

        record.token_sold_mint = base_mint;
        record.token_sold_vault = user_base_account.to_string();
        record.token_sold_amount = base_amount;
        record.token_sold_decimals = base_decimals;
        record.token_sold_vault_reserve = pool_base_reserve;

        Some(record)
    }
}
