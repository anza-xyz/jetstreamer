use borsh::BorshDeserialize;

/// SwapEvent emitted by CLMM programs (PancakeSwap, Byreal, Raydium CLMM).
/// Borsh-serialized after the 8-byte Anchor event discriminator.
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
