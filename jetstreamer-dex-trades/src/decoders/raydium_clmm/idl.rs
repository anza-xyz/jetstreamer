use borsh::BorshDeserialize;
use solana_address::Address;

pub const SWAP_DISC: [u8; 8] = [248, 198, 158, 145, 225, 117, 135, 200];
pub const SWAP_V2_DISC: [u8; 8] = [43, 4, 237, 11, 26, 201, 30, 98];

/// Anchor self-CPI event wrapper discriminator.
pub const EVENT_WRAPPER_DISC: [u8; 8] = [228, 69, 165, 46, 81, 203, 154, 29];

/// SwapEvent discriminator emitted via `Program data:` logs.
pub const SWAP_EVENT_DISC: [u8; 8] = [64, 198, 205, 232, 38, 8, 113, 226];

#[derive(Debug, Clone, BorshDeserialize)]
pub struct SwapEvent {
    pub pool_state: [u8; 32],
    pub sender: [u8; 32],
    pub token_account_0: [u8; 32],
    pub token_account_1: [u8; 32],
    pub amount_0: u64,
    pub transfer_fee_0: u64,
    pub amount_1: u64,
    pub transfer_fee_1: u64,
    pub zero_for_one: bool,
    pub sqrt_price_x64: u128,
    pub liquidity: u128,
    pub tick: i32,
}

impl SwapEvent {
    pub fn pool_state_str(&self) -> String {
        Address::new_from_array(self.pool_state).to_string()
    }

    pub fn token_account_0_str(&self) -> String {
        Address::new_from_array(self.token_account_0).to_string()
    }

    pub fn token_account_1_str(&self) -> String {
        Address::new_from_array(self.token_account_1).to_string()
    }
}

pub mod events {
    use super::*;
    use base64::{Engine, engine::general_purpose::STANDARD};

    /// Decode a SwapEvent from a base64-encoded `Program data:` log entry.
    pub fn decode_swap_event_from_log(data: &str) -> Option<SwapEvent> {
        let bytes = STANDARD.decode(data).ok()?;
        if bytes.len() < 8 || bytes[..8] != SWAP_EVENT_DISC {
            return None;
        }
        SwapEvent::try_from_slice(&bytes[8..]).ok()
    }
}
