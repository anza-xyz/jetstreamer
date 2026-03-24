pub mod idl;

use crate::instruction_iter::Instruction;
use crate::log_parser::LogParser;
use crate::registry::DexDecoder;
use crate::token_transfers::{get_swap_amounts, get_token_account_info};
use crate::transaction_ext::TransactionExt;
use crate::types::SwapRecord;
use borsh::BorshDeserialize;
use jetstreamer_firehose::firehose::TransactionData;
use solana_address::Address;

const PROGRAM_ID: &str = "HpNfyc2Saw7RKkQd8nEL4khUcuPhQ7WwY1B2qjx8jxFq";

const SWAP_DISC: [u8; 8] = [248, 198, 158, 145, 225, 117, 135, 200];
const SWAP_V2_DISC: [u8; 8] = [43, 4, 237, 11, 26, 201, 30, 98];

/// Anchor self-CPI event wrapper discriminator.
const EVENT_WRAPPER_DISC: [u8; 8] = [228, 69, 165, 46, 81, 203, 154, 29];

pub struct PancakeSwapClmmDecoder;

impl DexDecoder for PancakeSwapClmmDecoder {
    fn name(&self) -> &'static str {
        "PancakeSwap CLMM"
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

        let instruction_type = if ix.has_discriminator(&SWAP_DISC) {
            "Swap"
        } else if ix.has_discriminator(&SWAP_V2_DISC) {
            "SwapV2"
        } else {
            return try_decode_event(tx, ix, outer_program);
        };

        if accounts.len() < 2 {
            return None;
        }

        let pool = accounts[1].to_string();

        if let Some(record) = try_decode_from_logs(tx, ix, outer_program, &pool, instruction_type) {
            return Some(record);
        }

        decode_from_transfers(tx, ix, outer_program, &pool, instruction_type)
    }
}

fn try_decode_event(
    tx: &TransactionData,
    ix: &Instruction,
    outer_program: &str,
) -> Option<SwapRecord> {
    let data = ix.data();
    if data.len() < 16 {
        return None;
    }
    if data[..8] != EVENT_WRAPPER_DISC {
        return None;
    }

    let event = idl::SwapEvent::try_from_slice(&data[8..]).ok()?;
    build_record_from_event(tx, ix, outer_program, &event)
}

fn try_decode_from_logs(
    tx: &TransactionData,
    ix: &Instruction,
    outer_program: &str,
    _pool: &str,
    instruction_type: &str,
) -> Option<SwapRecord> {
    let logs = tx.get_logs();
    let data_events = LogParser::extract_program_data_by_position(&logs, PROGRAM_ID);

    let position = if ix.is_inner() {
        format!(
            "{}.{}",
            ix.instruction_index(),
            ix.inner_instruction_index()
        )
    } else {
        ix.instruction_index().to_string()
    };

    let event_data_list = data_events.get(&position)?;

    for encoded in event_data_list {
        if let Ok(raw) = base64::Engine::decode(&base64::engine::general_purpose::STANDARD, encoded)
        {
            if raw.len() < 8 {
                continue;
            }
            if let Ok(event) = idl::SwapEvent::try_from_slice(&raw[8..]) {
                let pool_addr = Address::new_from_array(event.pool_state);
                let mut record = build_record_from_event(tx, ix, outer_program, &event)?;
                record.pool_address = pool_addr.to_string();
                record.instruction_type = instruction_type.to_string();
                return Some(record);
            }
        }
    }

    None
}

fn decode_from_transfers(
    tx: &TransactionData,
    ix: &Instruction,
    outer_program: &str,
    pool: &str,
    instruction_type: &str,
) -> Option<SwapRecord> {
    let accounts = ix.accounts();
    if accounts.len() < 10 {
        return None;
    }

    let vault_0 = accounts[4].to_string();
    let vault_1 = accounts[5].to_string();

    let outer_idx = ix.instruction_index() as usize;
    let inner_idx = ix.inner_instruction_index() as usize;

    let amounts = get_swap_amounts(tx, outer_idx, inner_idx, &vault_0, &vault_1, None, false)?;

    let mut record = SwapRecord::default();
    record.instruction_index = ix.instruction_index();
    record.inner_instruction_index = ix.inner_instruction_index();
    record.is_inner_instruction = ix.is_inner();
    record.instruction_type = instruction_type.to_string();
    record.outer_program = outer_program.to_string();
    record.inner_program = PROGRAM_ID.to_string();
    record.pool_address = pool.to_string();

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
    record.token_sold_vault_reserve = sold_vault_info
        .map(|i| i.post_balance_scaled())
        .unwrap_or(0.0);
    record.token_bought_vault_reserve = bought_vault_info
        .map(|i| i.post_balance_scaled())
        .unwrap_or(0.0);

    Some(record)
}

fn build_record_from_event(
    tx: &TransactionData,
    ix: &Instruction,
    outer_program: &str,
    event: &idl::SwapEvent,
) -> Option<SwapRecord> {
    let pool = Address::new_from_array(event.pool_state);
    let vault_0 = Address::new_from_array(event.token_account_0);
    let vault_1 = Address::new_from_array(event.token_account_1);

    let info_0 = get_token_account_info(tx, &vault_0.to_string());
    let info_1 = get_token_account_info(tx, &vault_1.to_string());

    let decimals_0 = info_0.as_ref().map(|i| i.decimals).unwrap_or(6);
    let decimals_1 = info_1.as_ref().map(|i| i.decimals).unwrap_or(6);
    let mint_0 = info_0.as_ref().map(|i| i.mint.clone()).unwrap_or_default();
    let mint_1 = info_1.as_ref().map(|i| i.mint.clone()).unwrap_or_default();

    let amount_0 = event.amount_0 as f64 / 10f64.powi(decimals_0 as i32);
    let amount_1 = event.amount_1 as f64 / 10f64.powi(decimals_1 as i32);

    let mut record = SwapRecord::default();
    record.instruction_index = ix.instruction_index();
    record.inner_instruction_index = ix.inner_instruction_index();
    record.is_inner_instruction = ix.is_inner();
    record.instruction_type = "Swap".to_string();
    record.outer_program = outer_program.to_string();
    record.inner_program = PROGRAM_ID.to_string();
    record.pool_address = pool.to_string();

    record.sqrt_price = event.sqrt_price_x64.to_string();
    record.is_base_to_quote = Some(event.zero_for_one);

    if event.zero_for_one {
        record.token_sold_mint = mint_0;
        record.token_sold_vault = vault_0.to_string();
        record.token_sold_amount = amount_0;
        record.token_sold_decimals = decimals_0;
        record.token_bought_mint = mint_1;
        record.token_bought_vault = vault_1.to_string();
        record.token_bought_amount = amount_1;
        record.token_bought_decimals = decimals_1;
    } else {
        record.token_sold_mint = mint_1;
        record.token_sold_vault = vault_1.to_string();
        record.token_sold_amount = amount_1;
        record.token_sold_decimals = decimals_1;
        record.token_bought_mint = mint_0;
        record.token_bought_vault = vault_0.to_string();
        record.token_bought_amount = amount_0;
        record.token_bought_decimals = decimals_0;
    }

    let sold_vault_info = get_token_account_info(tx, &record.token_sold_vault);
    let bought_vault_info = get_token_account_info(tx, &record.token_bought_vault);
    record.token_sold_vault_reserve = sold_vault_info
        .map(|i| i.post_balance_scaled())
        .unwrap_or(0.0);
    record.token_bought_vault_reserve = bought_vault_info
        .map(|i| i.post_balance_scaled())
        .unwrap_or(0.0);

    Some(record)
}
