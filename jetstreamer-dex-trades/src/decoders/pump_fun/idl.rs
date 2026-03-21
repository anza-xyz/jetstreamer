use byteorder::{LittleEndian, ReadBytesExt};
use std::io::Cursor;

const PUBKEY_LEN: usize = 32;

#[derive(Debug, Clone)]
pub struct TradeEvent {
    pub mint: [u8; 32],
    pub sol_amount: u64,
    pub token_amount: u64,
    pub is_buy: bool,
    pub user: [u8; 32],
    pub timestamp: i64,
    pub virtual_sol_reserves: u64,
    pub virtual_token_reserves: u64,
    pub real_sol_reserves: u64,
    pub real_token_reserves: u64,
}

impl TradeEvent {
    /// Deserialize from raw bytes (after stripping discriminator).
    /// Layout: mint(32) + sol_amount(8) + token_amount(8) + is_buy(1)
    ///       + user(32) + timestamp(8) + virtual_sol(8) + virtual_token(8)
    ///       + real_sol(8) + real_token(8) = 121 bytes
    pub fn deserialize(data: &[u8]) -> Option<Self> {
        if data.len() < 121 {
            return None;
        }

        let mut cursor = Cursor::new(data);

        let mut mint = [0u8; PUBKEY_LEN];
        std::io::Read::read_exact(&mut cursor, &mut mint).ok()?;

        let sol_amount = cursor.read_u64::<LittleEndian>().ok()?;
        let token_amount = cursor.read_u64::<LittleEndian>().ok()?;
        let is_buy = cursor.read_u8().ok()? != 0;

        let mut user = [0u8; PUBKEY_LEN];
        std::io::Read::read_exact(&mut cursor, &mut user).ok()?;

        let timestamp = cursor.read_i64::<LittleEndian>().ok()?;
        let virtual_sol_reserves = cursor.read_u64::<LittleEndian>().ok()?;
        let virtual_token_reserves = cursor.read_u64::<LittleEndian>().ok()?;
        let real_sol_reserves = cursor.read_u64::<LittleEndian>().ok()?;
        let real_token_reserves = cursor.read_u64::<LittleEndian>().ok()?;

        Some(Self {
            mint,
            sol_amount,
            token_amount,
            is_buy,
            user,
            timestamp,
            virtual_sol_reserves,
            virtual_token_reserves,
            real_sol_reserves,
            real_token_reserves,
        })
    }
}

pub mod events {
    use super::TradeEvent;

    /// Anchor self-CPI event wrapper discriminator (first 8 bytes of sha256("anchor:event"))
    pub const EVENT_WRAPPER_DISC: [u8; 8] = [228, 69, 165, 46, 81, 203, 154, 29];
    /// Buy event discriminator
    pub const BUY_EVENT_DISC: [u8; 8] = [189, 23, 183, 97, 220, 167, 55, 174];
    /// Sell event discriminator
    pub const SELL_EVENT_DISC: [u8; 8] = [33, 108, 13, 221, 235, 37, 153, 28];

    /// Decode a trade event from the inner CPI instruction data.
    /// The data starts with the event wrapper discriminator (8 bytes), then
    /// the inner event discriminator (8 bytes), then the event payload.
    pub fn decode_trade_event(data: &[u8]) -> Option<(TradeEvent, bool)> {
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

        let event = TradeEvent::deserialize(&data[16..])?;
        Some((event, is_buy))
    }
}
