mod idl;

use crate::instruction_iter::Instruction;
use crate::registry::DexDecoder;
use crate::types::SwapRecord;
use jetstreamer_firehose::firehose::TransactionData;
use solana_address::Address;

const PROGRAM_ID: &str = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P";
const SOL_MINT: &str = "So11111111111111111111111111111111111111112";
const SOL_DECIMALS: u8 = 9;
const TOKEN_DECIMALS: u8 = 6;

pub struct PumpFunDecoder;

impl DexDecoder for PumpFunDecoder {
    fn name(&self) -> &'static str {
        "Pump Fun"
    }

    fn program_ids(&self) -> &[&'static str] {
        &[PROGRAM_ID]
    }

    fn decode_instruction(
        &self,
        _tx: &TransactionData,
        ix: &Instruction,
        outer_program: &str,
    ) -> Option<SwapRecord> {
        let data = ix.data();
        let accounts = ix.accounts();

        let (event, is_buy) = idl::events::decode_trade_event(data)?;

        let mint = Address::new_from_array(event.mint);
        let mint_str = mint.to_string();
        let user = Address::new_from_array(event.user);

        let pool_address = if accounts.len() > 3 {
            accounts[3].to_string()
        } else {
            String::new()
        };

        let signer = if accounts.len() > 6 {
            accounts[6].to_string()
        } else {
            user.to_string()
        };

        let sol_amount_scaled = event.sol_amount as f64 / 10f64.powi(SOL_DECIMALS as i32);
        let token_amount_scaled = event.token_amount as f64 / 10f64.powi(TOKEN_DECIMALS as i32);

        let mut record = SwapRecord::default();

        record.instruction_index = ix.instruction_index();
        record.inner_instruction_index = ix.inner_instruction_index();
        record.is_inner_instruction = ix.is_inner();
        record.outer_program = outer_program.to_string();
        record.inner_program = PROGRAM_ID.to_string();
        record.pool_address = pool_address;
        record.signer = signer;

        if is_buy {
            record.instruction_type = "Buy".to_string();
            record.token_bought_mint = mint_str;
            record.token_bought_amount = token_amount_scaled;
            record.token_bought_decimals = TOKEN_DECIMALS;
            record.token_sold_mint = SOL_MINT.to_string();
            record.token_sold_amount = sol_amount_scaled;
            record.token_sold_decimals = SOL_DECIMALS;
        } else {
            record.instruction_type = "Sell".to_string();
            record.token_bought_mint = SOL_MINT.to_string();
            record.token_bought_amount = sol_amount_scaled;
            record.token_bought_decimals = SOL_DECIMALS;
            record.token_sold_mint = mint_str;
            record.token_sold_amount = token_amount_scaled;
            record.token_sold_decimals = TOKEN_DECIMALS;
        }

        Some(record)
    }
}
