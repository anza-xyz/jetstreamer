use borsh::BorshDeserialize;

pub const BUY_EXACT_IN_DISC: [u8; 8] = [250, 234, 13, 123, 213, 156, 92, 253];
pub const SELL_EXACT_IN_DISC: [u8; 8] = [149, 39, 222, 155, 211, 124, 152, 26];

/// Anchor self-CPI event wrapper discriminator.
pub const EVENT_WRAPPER_DISC: [u8; 8] = [228, 69, 165, 46, 81, 203, 154, 29];

/// SwapEvent discriminator emitted via `Program data:` logs.
pub const SWAP_EVENT_DISC: [u8; 8] = [64, 198, 205, 232, 38, 8, 113, 226];

#[derive(Debug, Clone, BorshDeserialize)]
pub struct SwapEvent {
    pub pool_state: [u8; 32],
    pub input_vault_before: u64,
    pub input_vault_after: u64,
    pub output_vault_before: u64,
    pub output_vault_after: u64,
    pub input_amount: u64,
    pub input_transfer_fee: u64,
    pub output_amount: u64,
    pub output_transfer_fee: u64,
    pub base_input: bool,
    pub base_reserve: u64,
    pub quote_reserve: u64,
    pub quote_protocol_fee: u64,
    pub platform_fee: u64,
    pub share_fee: u64,
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
