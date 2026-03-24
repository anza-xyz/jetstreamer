use borsh::BorshDeserialize;

pub const SWAP_BASE_INPUT_DISC: [u8; 8] = [143, 190, 90, 218, 196, 30, 51, 174];
pub const SWAP_BASE_OUTPUT_DISC: [u8; 8] = [55, 217, 98, 86, 163, 74, 180, 173];

/// Anchor self-CPI event wrapper discriminator (sha256("anchor:event")[..8]).
pub const EVENT_WRAPPER_DISC: [u8; 8] = [228, 69, 165, 46, 81, 203, 154, 29];

/// SwapEvent discriminator emitted via `Program data:` logs.
pub const SWAP_EVENT_DISC: [u8; 8] = [64, 198, 205, 232, 38, 8, 113, 226];

#[derive(Debug, Clone, BorshDeserialize)]
pub struct SwapEvent {
    pub pool_id: [u8; 32],
    pub input_vault_before: u64,
    pub input_vault_after: u64,
    pub output_vault_before: u64,
    pub output_vault_after: u64,
    pub input_amount: u64,
    pub output_amount: u64,
    pub input_transfer_fee: u64,
    pub output_transfer_fee: u64,
    pub base_input: bool,
}

pub mod events {
    use super::*;
    use base64::{Engine, engine::general_purpose::STANDARD};

    /// Decode a SwapEvent from a base64-encoded `Program data:` log entry.
    /// The decoded bytes start with the SwapEvent discriminator (8 bytes)
    /// followed by the borsh-serialized event payload.
    pub fn decode_swap_event_from_log(data: &str) -> Option<SwapEvent> {
        let bytes = STANDARD.decode(data).ok()?;
        if bytes.len() < 8 || bytes[..8] != SWAP_EVENT_DISC {
            return None;
        }
        SwapEvent::try_from_slice(&bytes[8..]).ok()
    }
}
