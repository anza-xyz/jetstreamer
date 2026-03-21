use byteorder::{LittleEndian, ReadBytesExt};
use std::io::Cursor;

/// Swap instruction discriminator.
pub const SWAP_DISC: [u8; 8] = [248, 198, 158, 145, 225, 117, 135, 200];

/// Swap event discriminator (Anchor self-CPI event inner disc).
pub const SWAP_EVENT_DISC: [u8; 8] = [81, 108, 227, 190, 205, 208, 10, 196];

/// Anchor self-CPI event wrapper discriminator.
pub const EVENT_WRAPPER_DISC: [u8; 8] = [228, 69, 165, 46, 81, 203, 154, 29];

/// Meteora Pools swap event — contains amounts and fees but NO direction info.
#[derive(Debug, Clone)]
pub struct SwapEvent {
    pub in_amount: u64,
    pub out_amount: u64,
    pub trade_fee: u64,
    pub protocol_fee: u64,
    pub host_fee: u64,
}

impl SwapEvent {
    /// Layout: in_amount(8) + out_amount(8) + trade_fee(8) + protocol_fee(8) + host_fee(8) = 40 bytes
    pub fn deserialize(data: &[u8]) -> Option<Self> {
        if data.len() < 40 {
            return None;
        }

        let mut cursor = Cursor::new(data);

        let in_amount = cursor.read_u64::<LittleEndian>().ok()?;
        let out_amount = cursor.read_u64::<LittleEndian>().ok()?;
        let trade_fee = cursor.read_u64::<LittleEndian>().ok()?;
        let protocol_fee = cursor.read_u64::<LittleEndian>().ok()?;
        let host_fee = cursor.read_u64::<LittleEndian>().ok()?;

        Some(Self {
            in_amount,
            out_amount,
            trade_fee,
            protocol_fee,
            host_fee,
        })
    }
}

pub mod events {
    use super::*;

    /// Decode a swap event from Anchor self-CPI instruction data.
    /// Data layout: event_wrapper_disc(8) + swap_event_disc(8) + payload.
    pub fn decode_swap_event(data: &[u8]) -> Option<SwapEvent> {
        if data.len() < 16 {
            return None;
        }
        if data[..8] != EVENT_WRAPPER_DISC {
            return None;
        }

        let inner_disc = &data[8..16];
        if inner_disc != SWAP_EVENT_DISC {
            return None;
        }

        SwapEvent::deserialize(&data[16..])
    }
}
