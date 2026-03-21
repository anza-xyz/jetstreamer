use borsh::BorshDeserialize;
use base64::{engine::general_purpose::STANDARD, Engine};

pub const SWAP_DISC: [u8; 8] = [248, 198, 158, 145, 225, 117, 135, 200];

pub const SWAP_EVENT_DISC: [u8; 8] = [64, 198, 205, 232, 38, 8, 113, 226];

#[derive(Debug, Clone, BorshDeserialize)]
pub struct SwapEvent {
    pub pool_id: [u8; 32],
    pub trade_direction: bool,
    pub amount_in: u64,
    pub amount_out: u64,
    pub sqrt_price: u128,
    pub current_tick: i32,
    pub protocol_fee_amount: u64,
    pub host_fee: u64,
    pub padding: u64,
}

pub fn decode_swap_event_from_log(data: &str) -> Option<SwapEvent> {
    let bytes = STANDARD.decode(data).ok()?;
    if bytes.len() < 8 || bytes[..8] != SWAP_EVENT_DISC {
        return None;
    }
    SwapEvent::try_from_slice(&bytes[8..]).ok()
}
