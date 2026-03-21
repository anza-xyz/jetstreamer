//! Unified [`SwapRecord`] type for all DEX decoders.
//!
//! ## Token Convention
//! - `token_bought` = token user RECEIVED (output of swap)
//! - `token_sold`   = token user SENT (input to swap)

use chrono::NaiveDate;

/// Programs that don't have a pool address concept.
const PROGRAMS_WITHOUT_POOL: &[&str] = &[
    "AQU1FRd7papthgdrwPTTq5JacJh8YtwEXaBfKU3bTz45", // Aquifer
];

/// Unified swap record for all DEX protocols.
#[derive(Debug, Clone)]
pub struct SwapRecord {
    // Block identity
    pub block_slot: u64,
    pub block_time: i64,
    pub block_date: NaiveDate,

    // Transaction identity
    pub tx_id: String,
    pub tx_index: u32,

    // Instruction identity
    pub instruction_index: u32,
    pub inner_instruction_index: u32,
    pub is_inner_instruction: bool,
    pub instruction_type: String,

    // Program identity
    pub outer_program: String,
    pub inner_program: String,

    // Pool & signer
    pub pool_address: String,
    pub signer: String,

    // Token bought (user received)
    pub token_bought_mint: String,
    pub token_bought_vault: String,
    pub token_bought_amount: f64,
    pub token_bought_decimals: u8,
    pub token_bought_vault_reserve: f64,

    // Token sold (user sent)
    pub token_sold_mint: String,
    pub token_sold_vault: String,
    pub token_sold_amount: f64,
    pub token_sold_decimals: u8,
    pub token_sold_vault_reserve: f64,

    // Fees (SOL)
    pub txn_fee: f64,
    pub priority_fee: f64,
    pub jito_tips: f64,

    // CLMM pool state
    pub sqrt_price: String,
    pub is_base_to_quote: Option<bool>,

    // Compute
    pub compute_units_consumed: u64,
}

impl Default for SwapRecord {
    fn default() -> Self {
        Self {
            block_slot: 0,
            block_time: 0,
            block_date: NaiveDate::from_ymd_opt(1970, 1, 1).unwrap(),
            tx_id: String::new(),
            tx_index: 0,
            instruction_index: 0,
            inner_instruction_index: 0,
            is_inner_instruction: false,
            instruction_type: String::new(),
            outer_program: String::new(),
            inner_program: String::new(),
            pool_address: String::new(),
            signer: String::new(),
            token_bought_mint: String::new(),
            token_bought_vault: String::new(),
            token_bought_amount: 0.0,
            token_bought_decimals: 0,
            token_bought_vault_reserve: 0.0,
            token_sold_mint: String::new(),
            token_sold_vault: String::new(),
            token_sold_amount: 0.0,
            token_sold_decimals: 0,
            token_sold_vault_reserve: 0.0,
            txn_fee: 0.0,
            priority_fee: 0.0,
            jito_tips: 0.0,
            sqrt_price: String::new(),
            is_base_to_quote: None,
            compute_units_consumed: 0,
        }
    }
}

impl SwapRecord {
    fn is_poolless_program(&self) -> bool {
        PROGRAMS_WITHOUT_POOL.contains(&self.outer_program.as_str())
            || PROGRAMS_WITHOUT_POOL.contains(&self.inner_program.as_str())
    }

    /// Validate that all mandatory fields are populated.
    pub fn validate(&self) -> Result<(), String> {
        let mandatory_fields = [
            ("tx_id", &self.tx_id),
            ("signer", &self.signer),
            ("pool_address", &self.pool_address),
            ("token_bought_mint", &self.token_bought_mint),
            ("token_bought_vault", &self.token_bought_vault),
            ("token_sold_mint", &self.token_sold_mint),
            ("token_sold_vault", &self.token_sold_vault),
            ("outer_program", &self.outer_program),
            ("instruction_type", &self.instruction_type),
        ];

        let is_poolless = self.is_poolless_program();

        for (field_name, value) in mandatory_fields {
            if field_name == "pool_address" && is_poolless {
                continue;
            }
            if value.is_empty() {
                return Err(format!(
                    "empty mandatory field '{}' in SwapRecord (tx_id={}, outer_program={})",
                    field_name,
                    if self.tx_id.is_empty() {
                        "EMPTY"
                    } else {
                        &self.tx_id
                    },
                    if self.outer_program.is_empty() {
                        "EMPTY"
                    } else {
                        &self.outer_program
                    },
                ));
            }
        }
        Ok(())
    }

    /// Convert to protobuf representation.
    pub fn to_proto(&self) -> crate::proto::SwapRecord {
        crate::proto::SwapRecord {
            block_slot: self.block_slot,
            block_time: self.block_time,
            block_date: self.block_date.format("%Y-%m-%d").to_string(),
            tx_id: self.tx_id.clone(),
            tx_index: self.tx_index,
            instruction_index: self.instruction_index,
            inner_instruction_index: self.inner_instruction_index,
            is_inner_instruction: self.is_inner_instruction,
            instruction_type: self.instruction_type.clone(),
            outer_program: self.outer_program.clone(),
            inner_program: self.inner_program.clone(),
            pool_address: self.pool_address.clone(),
            signer: self.signer.clone(),
            token_bought_mint: self.token_bought_mint.clone(),
            token_bought_vault: self.token_bought_vault.clone(),
            token_bought_amount: self.token_bought_amount,
            token_bought_decimals: self.token_bought_decimals as u32,
            token_bought_vault_reserve: self.token_bought_vault_reserve,
            token_sold_mint: self.token_sold_mint.clone(),
            token_sold_vault: self.token_sold_vault.clone(),
            token_sold_amount: self.token_sold_amount,
            token_sold_vault_reserve: self.token_sold_vault_reserve,
            token_sold_decimals: self.token_sold_decimals as u32,
            txn_fee: self.txn_fee,
            priority_fee: self.priority_fee,
            jito_tips: self.jito_tips,
            sqrt_price: self.sqrt_price.clone(),
            has_direction: self.is_base_to_quote.is_some(),
            is_base_to_quote: self.is_base_to_quote.unwrap_or(false),
            compute_units_consumed: self.compute_units_consumed,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message;

    fn sample_swap_record() -> SwapRecord {
        SwapRecord {
            block_slot: 280_000_000,
            block_time: 1711000000,
            block_date: NaiveDate::from_ymd_opt(2024, 3, 21).unwrap(),
            tx_id: "5VERv8NMvzbJMEkV8xnrLkEaWRtSz9Cos".to_string(),
            tx_index: 42,
            instruction_index: 1,
            inner_instruction_index: 2,
            is_inner_instruction: true,
            instruction_type: "swap".to_string(),
            outer_program: "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8".to_string(),
            inner_program: "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA".to_string(),
            pool_address: "58oQChx4yWmvKdwLLZzBi4ChoCc2fqCUWBkwMihLYQo2".to_string(),
            signer: "FmKAfMMnxRMaqG7yWrLMyLLEcRPQFqBPHH9Pmosigner".to_string(),
            token_bought_mint: "So11111111111111111111111111111111111111112".to_string(),
            token_bought_vault: "HWy1jotHpo6UqeQCR5m3bCA9PUbNK6a6".to_string(),
            token_bought_amount: 1.5,
            token_bought_decimals: 9,
            token_bought_vault_reserve: 100.0,
            token_sold_mint: "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v".to_string(),
            token_sold_vault: "ErcxwkPgL2Sc8qFNmY7Go".to_string(),
            token_sold_amount: 250.0,
            token_sold_decimals: 6,
            token_sold_vault_reserve: 5000.0,
            txn_fee: 0.000005,
            priority_fee: 0.001,
            jito_tips: 0.0,
            sqrt_price: String::new(),
            is_base_to_quote: None,
            compute_units_consumed: 200_000,
        }
    }

    #[test]
    fn test_swap_record_default() {
        let record = SwapRecord::default();
        assert_eq!(record.block_slot, 0);
        assert_eq!(record.tx_id, "");
        assert_eq!(
            record.block_date,
            NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()
        );
    }

    #[test]
    fn test_swap_record_validate_success() {
        let record = sample_swap_record();
        assert!(record.validate().is_ok());
    }

    #[test]
    fn test_swap_record_validate_missing_tx_id() {
        let mut record = sample_swap_record();
        record.tx_id = String::new();
        assert!(record.validate().is_err());
    }

    #[test]
    fn test_swap_record_validate_missing_pool_is_ok_for_poolless() {
        let mut record = sample_swap_record();
        record.outer_program =
            "AQU1FRd7papthgdrwPTTq5JacJh8YtwEXaBfKU3bTz45".to_string();
        record.pool_address = String::new();
        assert!(record.validate().is_ok(), "poolless programs should pass without pool_address");
    }

    #[test]
    fn test_swap_record_to_proto_roundtrip() {
        let record = sample_swap_record();
        let proto = record.to_proto();

        assert_eq!(proto.block_slot, record.block_slot);
        assert_eq!(proto.block_time, record.block_time);
        assert_eq!(proto.block_date, "2024-03-21");
        assert_eq!(proto.tx_id, record.tx_id);
        assert_eq!(proto.tx_index, record.tx_index);
        assert_eq!(proto.instruction_index, record.instruction_index);
        assert_eq!(proto.inner_instruction_index, record.inner_instruction_index);
        assert_eq!(proto.is_inner_instruction, record.is_inner_instruction);
        assert_eq!(proto.instruction_type, record.instruction_type);
        assert_eq!(proto.outer_program, record.outer_program);
        assert_eq!(proto.inner_program, record.inner_program);
        assert_eq!(proto.pool_address, record.pool_address);
        assert_eq!(proto.signer, record.signer);
        assert_eq!(proto.token_bought_mint, record.token_bought_mint);
        assert_eq!(proto.token_bought_amount, record.token_bought_amount);
        assert_eq!(proto.token_sold_mint, record.token_sold_mint);
        assert_eq!(proto.token_sold_amount, record.token_sold_amount);
        assert_eq!(proto.txn_fee, record.txn_fee);
        assert_eq!(proto.priority_fee, record.priority_fee);
        assert_eq!(proto.jito_tips, record.jito_tips);
        assert!(!proto.has_direction);
        assert!(!proto.is_base_to_quote);
        assert_eq!(proto.compute_units_consumed, record.compute_units_consumed);

        let bytes = proto.encode_to_vec();
        assert!(!bytes.is_empty());

        let decoded = crate::proto::SwapRecord::decode(bytes.as_slice()).unwrap();
        assert_eq!(decoded.tx_id, record.tx_id);
        assert_eq!(decoded.block_slot, record.block_slot);
        assert_eq!(decoded.token_bought_amount, record.token_bought_amount);
        assert_eq!(decoded.token_sold_amount, record.token_sold_amount);
    }

    #[test]
    fn test_proto_batch_roundtrip() {
        let record = sample_swap_record();
        let batch = crate::proto::DexTradesBatch {
            slot: 280_000_000,
            block_time: 1711000000,
            records: vec![record.to_proto()],
        };

        let bytes = batch.encode_to_vec();
        let decoded = crate::proto::DexTradesBatch::decode(bytes.as_slice()).unwrap();
        assert_eq!(decoded.slot, 280_000_000);
        assert_eq!(decoded.records.len(), 1);
        assert_eq!(decoded.records[0].tx_id, "5VERv8NMvzbJMEkV8xnrLkEaWRtSz9Cos");
    }

    #[test]
    fn test_clmm_direction_fields() {
        let mut record = sample_swap_record();
        record.sqrt_price = "79228162514264337593543950336".to_string();
        record.is_base_to_quote = Some(true);

        let proto = record.to_proto();
        assert!(proto.has_direction);
        assert!(proto.is_base_to_quote);
        assert_eq!(proto.sqrt_price, "79228162514264337593543950336");
    }
}
