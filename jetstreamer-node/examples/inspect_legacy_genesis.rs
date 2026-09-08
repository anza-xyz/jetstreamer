use {
    solana_genesis_utils::{MAX_GENESIS_ARCHIVE_UNPACKED_SIZE, open_genesis_config},
    std::path::Path,
};

fn main() {
    let ledger = std::env::args().nth(1).unwrap_or_else(|| ".".to_owned());
    let genesis =
        open_genesis_config(Path::new(&ledger), MAX_GENESIS_ARCHIVE_UNPACKED_SIZE).unwrap();

    println!("genesis hash: {}", genesis.hash());
    println!("accounts: {}", genesis.accounts.len());
    println!("reward pools: {}", genesis.rewards_pools.len());
    println!(
        "fee governor: {:?}; rent: {:?}",
        genesis.fee_rate_governor, genesis.rent
    );
    for (name, program_id) in &genesis.native_instruction_processors {
        println!("native: {program_id} {name:?}");
    }
}
