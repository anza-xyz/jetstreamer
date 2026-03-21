use borsh::BorshDeserialize;
use base64::{engine::general_purpose::STANDARD, Engine};

pub const SWAP_DISC: [u8; 8] = [248, 198, 158, 145, 225, 117, 135, 200];

pub const SWAP_EVENT_DISC: [u8; 8] = [231, 195, 65, 79, 107, 30, 205, 218];

#[derive(Debug, Clone, BorshDeserialize)]
pub struct SwapEvent {
    pub lb_pair: [u8; 32],
    pub from: [u8; 32],
    pub start_bin_id: i32,
    pub end_bin_id: i32,
    pub amount_in: u64,
    pub amount_out: u64,
    pub swap_for_y: bool,
    pub fee: u64,
    pub protocol_fee: u64,
    pub host_fee: u64,
}

pub fn decode_swap_event_from_log(data: &str) -> Option<SwapEvent> {
    let bytes = STANDARD.decode(data).ok()?;
    if bytes.len() < 8 || bytes[..8] != SWAP_EVENT_DISC {
        return None;
    }
    SwapEvent::try_from_slice(&bytes[8..]).ok()
}
