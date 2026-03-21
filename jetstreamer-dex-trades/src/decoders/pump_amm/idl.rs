use byteorder::{LittleEndian, ReadBytesExt};
use std::io::{Cursor, Read};

const PUBKEY_LEN: usize = 32;

fn read_pubkey(cursor: &mut Cursor<&[u8]>) -> Option<[u8; 32]> {
    let mut buf = [0u8; PUBKEY_LEN];
    cursor.read_exact(&mut buf).ok()?;
    Some(buf)
}

fn read_option_pubkey(cursor: &mut Cursor<&[u8]>) -> Option<Option<[u8; 32]>> {
    let tag = cursor.read_u8().ok()?;
    if tag == 0 {
        Some(None)
    } else {
        let pk = read_pubkey(cursor)?;
        Some(Some(pk))
    }
}

fn read_option_u64(cursor: &mut Cursor<&[u8]>) -> Option<Option<u64>> {
    let tag = cursor.read_u8().ok()?;
    if tag == 0 {
        Some(None)
    } else {
        let val = cursor.read_u64::<LittleEndian>().ok()?;
        Some(Some(val))
    }
}

/// Pump AMM BuyEvent — variable size due to optional coin_creator fields (V0 vs V1).
#[derive(Debug, Clone)]
pub struct BuyEvent {
    pub timestamp: i64,
    pub base_amount_out: u64,
    pub max_quote_amount_in: u64,
    pub user_base_token_reserves: u64,
    pub user_quote_token_reserves: u64,
    pub pool_base_token_reserves: u64,
    pub pool_quote_token_reserves: u64,
    pub quote_amount_in: u64,
    pub lp_fee_basis_points: u64,
    pub lp_fee: u64,
    pub protocol_fee_basis_points: u64,
    pub protocol_fee: u64,
    pub quote_amount_in_with_lp_fee: u64,
    pub user_quote_amount_in_with_fees: u64,
    pub pool: [u8; 32],
    pub user: [u8; 32],
    pub user_base_token_account: [u8; 32],
    pub user_quote_token_account: [u8; 32],
    pub protocol_fee_recipient: [u8; 32],
    pub protocol_fee_recipient_token_account: [u8; 32],
    // V1 optional fields
    pub coin_creator: Option<[u8; 32]>,
    pub coin_creator_fee_basis_points: Option<u64>,
    pub coin_creator_fee: Option<u64>,
    pub coin_creator_token_account: Option<[u8; 32]>,
}

impl BuyEvent {
    pub fn deserialize(data: &[u8]) -> Option<Self> {
        let mut cursor = Cursor::new(data);

        let timestamp = cursor.read_i64::<LittleEndian>().ok()?;
        let base_amount_out = cursor.read_u64::<LittleEndian>().ok()?;
        let max_quote_amount_in = cursor.read_u64::<LittleEndian>().ok()?;
        let user_base_token_reserves = cursor.read_u64::<LittleEndian>().ok()?;
        let user_quote_token_reserves = cursor.read_u64::<LittleEndian>().ok()?;
        let pool_base_token_reserves = cursor.read_u64::<LittleEndian>().ok()?;
        let pool_quote_token_reserves = cursor.read_u64::<LittleEndian>().ok()?;
        let quote_amount_in = cursor.read_u64::<LittleEndian>().ok()?;
        let lp_fee_basis_points = cursor.read_u64::<LittleEndian>().ok()?;
        let lp_fee = cursor.read_u64::<LittleEndian>().ok()?;
        let protocol_fee_basis_points = cursor.read_u64::<LittleEndian>().ok()?;
        let protocol_fee = cursor.read_u64::<LittleEndian>().ok()?;
        let quote_amount_in_with_lp_fee = cursor.read_u64::<LittleEndian>().ok()?;
        let user_quote_amount_in_with_fees = cursor.read_u64::<LittleEndian>().ok()?;
        let pool = read_pubkey(&mut cursor)?;
        let user = read_pubkey(&mut cursor)?;
        let user_base_token_account = read_pubkey(&mut cursor)?;
        let user_quote_token_account = read_pubkey(&mut cursor)?;
        let protocol_fee_recipient = read_pubkey(&mut cursor)?;
        let protocol_fee_recipient_token_account = read_pubkey(&mut cursor)?;

        let coin_creator = read_option_pubkey(&mut cursor).unwrap_or(None);
        let coin_creator_fee_basis_points = read_option_u64(&mut cursor).unwrap_or(None);
        let coin_creator_fee = read_option_u64(&mut cursor).unwrap_or(None);
        let coin_creator_token_account = read_option_pubkey(&mut cursor).unwrap_or(None);

        Some(Self {
            timestamp,
            base_amount_out,
            max_quote_amount_in,
            user_base_token_reserves,
            user_quote_token_reserves,
            pool_base_token_reserves,
            pool_quote_token_reserves,
            quote_amount_in,
            lp_fee_basis_points,
            lp_fee,
            protocol_fee_basis_points,
            protocol_fee,
            quote_amount_in_with_lp_fee,
            user_quote_amount_in_with_fees,
            pool,
            user,
            user_base_token_account,
            user_quote_token_account,
            protocol_fee_recipient,
            protocol_fee_recipient_token_account,
            coin_creator,
            coin_creator_fee_basis_points,
            coin_creator_fee,
            coin_creator_token_account,
        })
    }
}

/// Pump AMM SellEvent — variable size due to optional coin_creator fields.
#[derive(Debug, Clone)]
pub struct SellEvent {
    pub timestamp: i64,
    pub base_amount_in: u64,
    pub min_quote_amount_out: u64,
    pub user_base_token_reserves: u64,
    pub user_quote_token_reserves: u64,
    pub pool_base_token_reserves: u64,
    pub pool_quote_token_reserves: u64,
    pub quote_amount_out: u64,
    pub lp_fee_basis_points: u64,
    pub lp_fee: u64,
    pub protocol_fee_basis_points: u64,
    pub protocol_fee: u64,
    pub quote_amount_out_without_lp_fee: u64,
    pub user_quote_amount_out_without_fees: u64,
    pub pool: [u8; 32],
    pub user: [u8; 32],
    pub user_base_token_account: [u8; 32],
    pub user_quote_token_account: [u8; 32],
    pub protocol_fee_recipient: [u8; 32],
    pub protocol_fee_recipient_token_account: [u8; 32],
    // V1 optional fields
    pub coin_creator: Option<[u8; 32]>,
    pub coin_creator_fee_basis_points: Option<u64>,
    pub coin_creator_fee: Option<u64>,
    pub coin_creator_token_account: Option<[u8; 32]>,
}

impl SellEvent {
    pub fn deserialize(data: &[u8]) -> Option<Self> {
        let mut cursor = Cursor::new(data);

        let timestamp = cursor.read_i64::<LittleEndian>().ok()?;
        let base_amount_in = cursor.read_u64::<LittleEndian>().ok()?;
        let min_quote_amount_out = cursor.read_u64::<LittleEndian>().ok()?;
        let user_base_token_reserves = cursor.read_u64::<LittleEndian>().ok()?;
        let user_quote_token_reserves = cursor.read_u64::<LittleEndian>().ok()?;
        let pool_base_token_reserves = cursor.read_u64::<LittleEndian>().ok()?;
        let pool_quote_token_reserves = cursor.read_u64::<LittleEndian>().ok()?;
        let quote_amount_out = cursor.read_u64::<LittleEndian>().ok()?;
        let lp_fee_basis_points = cursor.read_u64::<LittleEndian>().ok()?;
        let lp_fee = cursor.read_u64::<LittleEndian>().ok()?;
        let protocol_fee_basis_points = cursor.read_u64::<LittleEndian>().ok()?;
        let protocol_fee = cursor.read_u64::<LittleEndian>().ok()?;
        let quote_amount_out_without_lp_fee = cursor.read_u64::<LittleEndian>().ok()?;
        let user_quote_amount_out_without_fees = cursor.read_u64::<LittleEndian>().ok()?;
        let pool = read_pubkey(&mut cursor)?;
        let user = read_pubkey(&mut cursor)?;
        let user_base_token_account = read_pubkey(&mut cursor)?;
        let user_quote_token_account = read_pubkey(&mut cursor)?;
        let protocol_fee_recipient = read_pubkey(&mut cursor)?;
        let protocol_fee_recipient_token_account = read_pubkey(&mut cursor)?;

        let coin_creator = read_option_pubkey(&mut cursor).unwrap_or(None);
        let coin_creator_fee_basis_points = read_option_u64(&mut cursor).unwrap_or(None);
        let coin_creator_fee = read_option_u64(&mut cursor).unwrap_or(None);
        let coin_creator_token_account = read_option_pubkey(&mut cursor).unwrap_or(None);

        Some(Self {
            timestamp,
            base_amount_in,
            min_quote_amount_out,
            user_base_token_reserves,
            user_quote_token_reserves,
            pool_base_token_reserves,
            pool_quote_token_reserves,
            quote_amount_out,
            lp_fee_basis_points,
            lp_fee,
            protocol_fee_basis_points,
            protocol_fee,
            quote_amount_out_without_lp_fee,
            user_quote_amount_out_without_fees,
            pool,
            user,
            user_base_token_account,
            user_quote_token_account,
            protocol_fee_recipient,
            protocol_fee_recipient_token_account,
            coin_creator,
            coin_creator_fee_basis_points,
            coin_creator_fee,
            coin_creator_token_account,
        })
    }
}

pub mod events {
    use super::{BuyEvent, SellEvent};

    pub const EVENT_WRAPPER_DISC: [u8; 8] = [228, 69, 165, 46, 81, 203, 154, 29];
    pub const BUY_EVENT_DISC: [u8; 8] = [103, 244, 82, 31, 44, 245, 119, 119];
    pub const SELL_EVENT_DISC: [u8; 8] = [62, 47, 55, 10, 165, 3, 220, 42];

    pub enum TradeEvent {
        Buy(BuyEvent),
        Sell(SellEvent),
    }

    /// Decode a trade event from the self-CPI instruction data.
    /// Layout: event_wrapper_disc(8) + inner_disc(8) + payload
    pub fn decode_trade_event(data: &[u8]) -> Option<TradeEvent> {
        if data.len() < 16 {
            return None;
        }
        if data[..8] != EVENT_WRAPPER_DISC {
            return None;
        }

        let inner_disc = &data[8..16];
        let payload = &data[16..];

        if inner_disc == BUY_EVENT_DISC {
            let event = BuyEvent::deserialize(payload)?;
            Some(TradeEvent::Buy(event))
        } else if inner_disc == SELL_EVENT_DISC {
            let event = SellEvent::deserialize(payload)?;
            Some(TradeEvent::Sell(event))
        } else {
            None
        }
    }
}
