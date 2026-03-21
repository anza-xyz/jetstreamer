//! Individual DEX decoder implementations.

pub mod alphaq;
pub mod aquifer;
pub mod bisonfi;
pub mod byreal_clmm;
pub mod futarchy_amm;
pub mod goonfi;
pub mod humidifi;
pub mod lifinity_v1;
pub mod lifinity_v2;
pub mod manifest;
pub mod meteora_damm_v2;
pub mod meteora_dbc;
pub mod meteora_dlmm;
pub mod meteora_pools;
pub mod obric_v2;
pub mod orca_v2;
pub mod orca_whirlpool;
pub mod pancakeswap_clmm;
pub mod pump_amm;
pub mod pump_fun;
pub mod raydium_amm_v4;
pub mod raydium_clmm;
pub mod raydium_cpmm;
pub mod raydium_launchlab;
pub mod solfi_v1;
pub mod solfi_v2;
pub mod stabble_stable;
pub mod stabble_weighted;
pub mod tessera;
pub mod zerofi;

use crate::registry::DexRegistry;

/// Register all built-in DEX decoders with the given registry.
pub fn register_all(registry: &mut DexRegistry) {
    registry.register(Box::new(pump_fun::PumpFunDecoder));
    registry.register(Box::new(pump_amm::PumpAmmDecoder));
    registry.register(Box::new(raydium_amm_v4::RaydiumAmmV4Decoder::new()));
    registry.register(Box::new(raydium_cpmm::RaydiumCpmmDecoder::new()));
    registry.register(Box::new(raydium_clmm::RaydiumClmmDecoder::new()));
    registry.register(Box::new(raydium_launchlab::RaydiumLaunchLabDecoder::new()));
    registry.register(Box::new(orca_whirlpool::OrcaWhirlpoolDecoder::new()));
    registry.register(Box::new(orca_v2::OrcaV2Decoder));
    registry.register(Box::new(meteora_damm_v2::MeteoraDammV2Decoder::new()));
    registry.register(Box::new(meteora_dbc::MeteoraDbcDecoder::new()));
    registry.register(Box::new(meteora_dlmm::MeteoraDlmmDecoder::new()));
    registry.register(Box::new(meteora_pools::MeteoraPoolsDecoder::new()));
    registry.register(Box::new(manifest::ManifestDecoder));
    registry.register(Box::new(aquifer::AquiferDecoder));
    registry.register(Box::new(alphaq::AlphaQDecoder));
    registry.register(Box::new(bisonfi::BisonFiDecoder));
    registry.register(Box::new(zerofi::ZerofiDecoder));
    registry.register(Box::new(goonfi::GoonfiDecoder));
    registry.register(Box::new(humidifi::HumidifiDecoder));
    registry.register(Box::new(tessera::TesseraDecoder));
    registry.register(Box::new(obric_v2::ObricV2Decoder));
    registry.register(Box::new(solfi_v1::SolfiV1Decoder));
    registry.register(Box::new(solfi_v2::SolfiV2Decoder));
    registry.register(Box::new(stabble_stable::StabbleStableDecoder));
    registry.register(Box::new(stabble_weighted::StabbleWeightedDecoder));
    registry.register(Box::new(lifinity_v1::LifinityV1Decoder));
    registry.register(Box::new(lifinity_v2::LifinityV2Decoder));
    registry.register(Box::new(futarchy_amm::FutarchyAmmDecoder::new()));
    registry.register(Box::new(pancakeswap_clmm::PancakeSwapClmmDecoder));
    registry.register(Box::new(byreal_clmm::ByrealClmmDecoder));
}
