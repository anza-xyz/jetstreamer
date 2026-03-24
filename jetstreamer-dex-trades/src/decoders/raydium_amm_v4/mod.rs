use crate::instruction_iter::Instruction;
use crate::log_parser::LogParser;
use crate::registry::DexDecoder;
use crate::token_transfers::{get_swap_amounts, get_token_account_info};
use crate::transaction_ext::TransactionExt;
use crate::types::SwapRecord;
use base64::{Engine, engine::general_purpose::STANDARD};
use byteorder::{LittleEndian, ReadBytesExt};
use jetstreamer_firehose::firehose::TransactionData;
use std::collections::HashMap;
use std::io::Cursor;
use std::sync::RwLock;

const PROGRAM_ID: &str = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8";

const SWAP_BASE_IN_DISC: u8 = 9;
const SWAP_BASE_OUT_DISC: u8 = 11;

/// Decoded `ray_log:` swap event from Raydium AMM V4 program logs.
#[allow(dead_code)]
#[derive(Debug, Clone)]
struct SwapInstructionLog {
    log_type: u8,
    amount_in: u64,
    minimum_out: u64,
    direction: u64,
    user_source: u64,
    pool_coin: u64,
    pool_pc: u64,
    out_amount: u64,
}

impl SwapInstructionLog {
    /// Layout: log_type(1) + amount_in(8) + minimum_out(8) + direction(8)
    ///       + user_source(8) + pool_coin(8) + pool_pc(8) + out_amount(8) = 57 bytes
    fn deserialize(data: &[u8]) -> Option<Self> {
        if data.len() < 57 {
            return None;
        }
        let mut cursor = Cursor::new(data);
        let log_type = cursor.read_u8().ok()?;
        let amount_in = cursor.read_u64::<LittleEndian>().ok()?;
        let minimum_out = cursor.read_u64::<LittleEndian>().ok()?;
        let direction = cursor.read_u64::<LittleEndian>().ok()?;
        let user_source = cursor.read_u64::<LittleEndian>().ok()?;
        let pool_coin = cursor.read_u64::<LittleEndian>().ok()?;
        let pool_pc = cursor.read_u64::<LittleEndian>().ok()?;
        let out_amount = cursor.read_u64::<LittleEndian>().ok()?;

        Some(Self {
            log_type,
            amount_in,
            minimum_out,
            direction,
            user_source,
            pool_coin,
            pool_pc,
            out_amount,
        })
    }
}

pub struct RaydiumAmmV4Decoder {
    /// tx_id → (position → SwapInstructionLog)
    event_cache: RwLock<HashMap<String, HashMap<String, SwapInstructionLog>>>,
}

impl Default for RaydiumAmmV4Decoder {
    fn default() -> Self {
        Self::new()
    }
}

impl RaydiumAmmV4Decoder {
    pub fn new() -> Self {
        Self {
            event_cache: RwLock::new(HashMap::new()),
        }
    }

    fn get_position(ix: &Instruction) -> String {
        match ix {
            Instruction::TopLevel { index, .. } => index.to_string(),
            Instruction::Inner {
                parent_index,
                index,
                ..
            } => format!("{}.{}", parent_index, index),
        }
    }
}

impl DexDecoder for RaydiumAmmV4Decoder {
    fn name(&self) -> &'static str {
        "Raydium AMM V4"
    }

    fn program_ids(&self) -> &[&'static str] {
        &[PROGRAM_ID]
    }

    fn on_transaction_start(&self, tx: &TransactionData) {
        let logs = tx.get_logs();
        let log_map = LogParser::extract_program_logs_by_position(&logs, PROGRAM_ID);

        let mut events: HashMap<String, SwapInstructionLog> = HashMap::new();

        for (position, log_messages) in &log_map {
            for msg in log_messages {
                if let Some(ray_data) = msg.strip_prefix("ray_log: ")
                    && let Ok(bytes) = STANDARD.decode(ray_data)
                    && let Some(log) = SwapInstructionLog::deserialize(&bytes)
                {
                    events.insert(position.clone(), log);
                }
            }
        }

        if !events.is_empty() {
            let tx_id = tx.signature.to_string();
            let mut cache = self.event_cache.write().unwrap();
            cache.insert(tx_id, events);
        }
    }

    fn on_transaction_end(&self, tx_id: &str) {
        let mut cache = self.event_cache.write().unwrap();
        cache.remove(tx_id);
    }

    fn decode_instruction(
        &self,
        tx: &TransactionData,
        ix: &Instruction,
        outer_program: &str,
    ) -> Option<SwapRecord> {
        let data = ix.data();
        if data.is_empty() {
            return None;
        }

        let instruction_type = match data[0] {
            SWAP_BASE_IN_DISC => "SwapBaseIn",
            SWAP_BASE_OUT_DISC => "SwapBaseOut",
            _ => return None,
        };

        let accounts = ix.accounts();
        if accounts.len() < 6 {
            return None;
        }

        let pool = accounts[1].to_string();
        let vault_a = accounts[4].to_string();
        let vault_b = accounts[5].to_string();

        let position = Self::get_position(ix);
        let tx_id = tx.signature.to_string();

        let cache = self.event_cache.read().unwrap();
        let event = cache.get(&tx_id).and_then(|m| m.get(&position)).cloned();
        drop(cache);

        if let Some(log) = event {
            self.decode_from_log(
                tx,
                ix,
                outer_program,
                instruction_type,
                &pool,
                &vault_a,
                &vault_b,
                &log,
            )
        } else {
            self.decode_from_transfers(
                tx,
                ix,
                outer_program,
                instruction_type,
                &pool,
                &vault_a,
                &vault_b,
            )
        }
    }
}

impl RaydiumAmmV4Decoder {
    fn decode_from_log(
        &self,
        tx: &TransactionData,
        ix: &Instruction,
        outer_program: &str,
        instruction_type: &str,
        pool: &str,
        vault_a: &str,
        vault_b: &str,
        log: &SwapInstructionLog,
    ) -> Option<SwapRecord> {
        let vault_a_info = get_token_account_info(tx, vault_a)?;
        let vault_b_info = get_token_account_info(tx, vault_b)?;

        let mut record = SwapRecord::default();
        record.instruction_index = ix.instruction_index();
        record.inner_instruction_index = ix.inner_instruction_index();
        record.is_inner_instruction = ix.is_inner();
        record.instruction_type = instruction_type.to_string();
        record.outer_program = outer_program.to_string();
        record.inner_program = PROGRAM_ID.to_string();
        record.pool_address = pool.to_string();

        match log.direction {
            1 => {
                // User sells coin (token_a via vault_a), buys pc (token_b via vault_b)
                let sold_dec = vault_a_info.decimals;
                let bought_dec = vault_b_info.decimals;

                record.token_sold_mint = vault_a_info.mint.clone();
                record.token_sold_vault = vault_a.to_string();
                record.token_sold_amount = log.amount_in as f64 / 10f64.powi(sold_dec as i32);
                record.token_sold_decimals = sold_dec;
                record.token_sold_vault_reserve =
                    log.pool_coin as f64 / 10f64.powi(sold_dec as i32);

                record.token_bought_mint = vault_b_info.mint.clone();
                record.token_bought_vault = vault_b.to_string();
                record.token_bought_amount = log.out_amount as f64 / 10f64.powi(bought_dec as i32);
                record.token_bought_decimals = bought_dec;
                record.token_bought_vault_reserve =
                    log.pool_pc as f64 / 10f64.powi(bought_dec as i32);
            }
            _ => {
                // direction == 2: User sells pc (token_b via vault_b), buys coin (token_a via vault_a)
                let sold_dec = vault_b_info.decimals;
                let bought_dec = vault_a_info.decimals;

                record.token_sold_mint = vault_b_info.mint.clone();
                record.token_sold_vault = vault_b.to_string();
                record.token_sold_amount = log.amount_in as f64 / 10f64.powi(sold_dec as i32);
                record.token_sold_decimals = sold_dec;
                record.token_sold_vault_reserve = log.pool_pc as f64 / 10f64.powi(sold_dec as i32);

                record.token_bought_mint = vault_a_info.mint.clone();
                record.token_bought_vault = vault_a.to_string();
                record.token_bought_amount = log.out_amount as f64 / 10f64.powi(bought_dec as i32);
                record.token_bought_decimals = bought_dec;
                record.token_bought_vault_reserve =
                    log.pool_coin as f64 / 10f64.powi(bought_dec as i32);
            }
        }

        Some(record)
    }

    fn decode_from_transfers(
        &self,
        tx: &TransactionData,
        ix: &Instruction,
        outer_program: &str,
        instruction_type: &str,
        pool: &str,
        vault_a: &str,
        vault_b: &str,
    ) -> Option<SwapRecord> {
        let outer_idx = ix.instruction_index() as usize;
        let inner_idx = ix.inner_instruction_index() as usize;

        let swap = get_swap_amounts(tx, outer_idx, inner_idx, vault_a, vault_b, None, false)?;

        let mut record = SwapRecord::default();
        record.instruction_index = ix.instruction_index();
        record.inner_instruction_index = ix.inner_instruction_index();
        record.is_inner_instruction = ix.is_inner();
        record.instruction_type = instruction_type.to_string();
        record.outer_program = outer_program.to_string();
        record.inner_program = PROGRAM_ID.to_string();
        record.pool_address = pool.to_string();

        record.token_sold_mint = swap.sold.mint.clone();
        record.token_sold_vault = swap.sold.vault.clone();
        record.token_sold_amount = swap.sold.amount_scaled();
        record.token_sold_decimals = swap.sold.decimals;

        record.token_bought_mint = swap.bought.mint.clone();
        record.token_bought_vault = swap.bought.vault.clone();
        record.token_bought_amount = swap.bought.amount_scaled();
        record.token_bought_decimals = swap.bought.decimals;

        Some(record)
    }
}
