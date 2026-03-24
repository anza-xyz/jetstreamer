use byteorder::{LittleEndian, ReadBytesExt};
use solana_address::Address;
use std::io::{Cursor, Read};

const PUBKEY_LEN: usize = 32;

pub const EVENT_WRAPPER_DISC: [u8; 8] = [228, 69, 165, 46, 81, 203, 154, 29];
pub const SPOT_SWAP_EVENT_DISC: [u8; 8] = [29, 253, 121, 255, 82, 60, 148, 195];
pub const CONDITIONAL_SWAP_EVENT_DISC: [u8; 8] = [2, 166, 200, 160, 94, 212, 68, 45];

pub const SPOT_SWAP_DISC: [u8; 8] = [167, 97, 12, 231, 237, 78, 166, 251];
pub const CONDITIONAL_SWAP_DISC: [u8; 8] = [194, 136, 220, 89, 242, 169, 130, 157];

fn read_pubkey(cursor: &mut Cursor<&[u8]>) -> Option<[u8; 32]> {
    let mut buf = [0u8; PUBKEY_LEN];
    cursor.read_exact(&mut buf).ok()?;
    Some(buf)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SwapType {
    Buy = 0,
    Sell = 1,
}

impl SwapType {
    pub fn from_u8(val: u8) -> Option<Self> {
        match val {
            0 => Some(Self::Buy),
            1 => Some(Self::Sell),
            _ => None,
        }
    }

    pub fn label(&self) -> &'static str {
        match self {
            Self::Buy => "Buy",
            Self::Sell => "Sell",
        }
    }
}

/// On-chain FutarchyAmm state — only the fields we need for decoding.
#[derive(Debug, Clone)]
pub struct FutarchyAmm {
    pub base_mint: Address,
    pub quote_mint: Address,
    pub amm_base_vault: Address,
    pub amm_quote_vault: Address,
}

/// Pool state variants embedded in events.
/// Spot: 1 pool (132 bytes), Futarchy: 3 pools (396 bytes).
#[derive(Debug, Clone)]
pub enum PoolState {
    Spot(FutarchyAmm),
    Futarchy([FutarchyAmm; 3]),
}

impl FutarchyAmm {
    /// Deserialize a single pool from 132 bytes:
    /// state_variant(u8) + total_liquidity(u128=16) + padding(3) +
    /// base_mint(32) + quote_mint(32) + amm_base_vault(32) + amm_quote_vault(32) = ~148
    /// Simplified layout: skip state_variant(1) + total_liquidity(16) = 17 prefix,
    /// then base_mint(32) + quote_mint(32) + amm_base_vault(32) + amm_quote_vault(32) = 128
    pub fn deserialize(data: &[u8]) -> Option<Self> {
        if data.len() < 132 {
            return None;
        }
        let mut cursor = Cursor::new(data);

        // state_variant: u8
        let _state_variant = cursor.read_u8().ok()?;
        // total_liquidity: u128
        let _total_liquidity = cursor.read_u128::<LittleEndian>().ok()?;
        // padding: 3 bytes
        let mut _pad = [0u8; 3];
        cursor.read_exact(&mut _pad).ok()?;

        let base_mint = Address::new_from_array(read_pubkey(&mut cursor)?);
        let quote_mint = Address::new_from_array(read_pubkey(&mut cursor)?);
        let amm_base_vault = Address::new_from_array(read_pubkey(&mut cursor)?);
        let amm_quote_vault = Address::new_from_array(read_pubkey(&mut cursor)?);

        Some(Self {
            base_mint,
            quote_mint,
            amm_base_vault,
            amm_quote_vault,
        })
    }
}

/// SpotSwapEvent emitted by Futarchy AMM via Anchor self-CPI.
#[derive(Debug, Clone)]
pub struct SpotSwapEvent {
    pub dao: [u8; 32],
    pub user: [u8; 32],
    pub swap_type: SwapType,
    pub input_amount: u64,
    pub output_amount: u64,
    pub post_amm_state: FutarchyAmm,
}

impl SpotSwapEvent {
    /// Layout: dao(32) + user(32) + swap_type(1) + input_amount(8) + output_amount(8) +
    /// post_amm_state(132) = 213 bytes
    pub fn deserialize(data: &[u8]) -> Option<Self> {
        if data.len() < 213 {
            return None;
        }
        let mut cursor = Cursor::new(data);

        let dao = read_pubkey(&mut cursor)?;
        let user = read_pubkey(&mut cursor)?;
        let swap_type_byte = cursor.read_u8().ok()?;
        let swap_type = SwapType::from_u8(swap_type_byte)?;
        let input_amount = cursor.read_u64::<LittleEndian>().ok()?;
        let output_amount = cursor.read_u64::<LittleEndian>().ok()?;

        let pos = cursor.position() as usize;
        let post_amm_state = FutarchyAmm::deserialize(&data[pos..])?;

        Some(Self {
            dao,
            user,
            swap_type,
            input_amount,
            output_amount,
            post_amm_state,
        })
    }
}

/// ConditionalSwapEvent emitted by Futarchy AMM via Anchor self-CPI.
#[derive(Debug, Clone)]
pub struct ConditionalSwapEvent {
    pub dao: [u8; 32],
    pub user: [u8; 32],
    pub swap_type: SwapType,
    pub input_amount: u64,
    pub output_amount: u64,
    pub post_amm_state: [FutarchyAmm; 3],
}

impl ConditionalSwapEvent {
    /// Layout: dao(32) + user(32) + swap_type(1) + input_amount(8) + output_amount(8) +
    /// post_amm_state(3 * 132 = 396) = 477 bytes
    pub fn deserialize(data: &[u8]) -> Option<Self> {
        if data.len() < 477 {
            return None;
        }
        let mut cursor = Cursor::new(data);

        let dao = read_pubkey(&mut cursor)?;
        let user = read_pubkey(&mut cursor)?;
        let swap_type_byte = cursor.read_u8().ok()?;
        let swap_type = SwapType::from_u8(swap_type_byte)?;
        let input_amount = cursor.read_u64::<LittleEndian>().ok()?;
        let output_amount = cursor.read_u64::<LittleEndian>().ok()?;

        let pos = cursor.position() as usize;
        let pool0 = FutarchyAmm::deserialize(&data[pos..])?;
        let pool1 = FutarchyAmm::deserialize(&data[pos + 132..])?;
        let pool2 = FutarchyAmm::deserialize(&data[pos + 264..])?;

        Some(Self {
            dao,
            user,
            swap_type,
            input_amount,
            output_amount,
            post_amm_state: [pool0, pool1, pool2],
        })
    }
}

pub mod events {
    use super::*;

    pub enum FutarchyEvent {
        SpotSwap(SpotSwapEvent),
        ConditionalSwap(ConditionalSwapEvent),
    }

    /// Decode a Futarchy event from Anchor self-CPI instruction data.
    /// Layout: event_wrapper_disc(8) + inner_event_disc(8) + payload.
    pub fn decode_event(data: &[u8]) -> Option<FutarchyEvent> {
        if data.len() < 16 {
            return None;
        }
        if data[..8] != EVENT_WRAPPER_DISC {
            return None;
        }

        let inner_disc = &data[8..16];
        let payload = &data[16..];

        if inner_disc == SPOT_SWAP_EVENT_DISC {
            let event = SpotSwapEvent::deserialize(payload)?;
            Some(FutarchyEvent::SpotSwap(event))
        } else if inner_disc == CONDITIONAL_SWAP_EVENT_DISC {
            let event = ConditionalSwapEvent::deserialize(payload)?;
            Some(FutarchyEvent::ConditionalSwap(event))
        } else {
            None
        }
    }
}
