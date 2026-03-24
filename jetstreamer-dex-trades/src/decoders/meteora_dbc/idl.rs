use byteorder::{LittleEndian, ReadBytesExt};
use std::io::Cursor;

const PUBKEY_LEN: usize = 32;

/// Anchor self-CPI event wrapper discriminator (sha256("anchor:event")[..8]).
pub const EVENT_WRAPPER_DISC: [u8; 8] = [228, 69, 165, 46, 81, 203, 154, 29];

/// Buy instruction discriminator.
pub const BUY_DISC: [u8; 8] = [102, 6, 61, 18, 1, 218, 235, 234];
/// Sell instruction discriminator.
pub const SELL_DISC: [u8; 8] = [51, 230, 133, 164, 1, 127, 131, 173];

/// Buy event discriminator (inner, after event wrapper).
pub const BUY_EVENT_DISC: [u8; 8] = [103, 244, 82, 31, 44, 245, 119, 119];
/// Sell event discriminator (inner, after event wrapper).
pub const SELL_EVENT_DISC: [u8; 8] = [62, 47, 55, 210, 224, 105, 121, 62];

/// Swap event emitted by Meteora DBC via Anchor self-CPI.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct SwapEvent {
    pub pool: [u8; 32],
    pub token_a_amount: u64,
    pub token_b_amount: u64,
    pub a_to_b: bool,
    pub token_a_mint: [u8; 32],
    pub token_b_mint: [u8; 32],
}

impl SwapEvent {
    /// Layout: pool(32) + token_a_amount(8) + token_b_amount(8) + a_to_b(1) +
    ///         token_a_mint(32) + token_b_mint(32) = 113 bytes
    pub fn deserialize(data: &[u8]) -> Option<Self> {
        if data.len() < 113 {
            return None;
        }

        let mut cursor = Cursor::new(data);

        let mut pool = [0u8; PUBKEY_LEN];
        std::io::Read::read_exact(&mut cursor, &mut pool).ok()?;

        let token_a_amount = cursor.read_u64::<LittleEndian>().ok()?;
        let token_b_amount = cursor.read_u64::<LittleEndian>().ok()?;
        let a_to_b = cursor.read_u8().ok()? != 0;

        let mut token_a_mint = [0u8; PUBKEY_LEN];
        std::io::Read::read_exact(&mut cursor, &mut token_a_mint).ok()?;

        let mut token_b_mint = [0u8; PUBKEY_LEN];
        std::io::Read::read_exact(&mut cursor, &mut token_b_mint).ok()?;

        Some(Self {
            pool,
            token_a_amount,
            token_b_amount,
            a_to_b,
            token_a_mint,
            token_b_mint,
        })
    }
}

pub mod events {
    use super::*;

    /// Decode a swap event from Anchor self-CPI instruction data.
    /// Data layout: event_wrapper_disc(8) + inner_event_disc(8) + payload.
    pub fn decode_swap_event(data: &[u8]) -> Option<(SwapEvent, bool)> {
        if data.len() < 16 {
            return None;
        }
        if data[..8] != EVENT_WRAPPER_DISC {
            return None;
        }

        let inner_disc = &data[8..16];
        let is_buy = if inner_disc == BUY_EVENT_DISC {
            true
        } else if inner_disc == SELL_EVENT_DISC {
            false
        } else {
            return None;
        };

        let event = SwapEvent::deserialize(&data[16..])?;
        Some((event, is_buy))
    }
}
