# jetstreamer-dex-trades

Solana DEX trades decoder plugin for [Jetstreamer](https://github.com/anza-xyz/jetstreamer). Decodes swap transactions from 30 DEX programs into a unified protobuf `SwapRecord`.

Contributed by [Top Ledger](https://topledger.xyz/) ([@ledger_top](https://x.com/ledger_top)) — Solana blockchain analytics and data infrastructure.

## Supported DEXes

Covers all major Solana DEXes plus proprietary AMMs (SolFi, AlphaQ, BisonFi, ZeroFi, GoonFi, HumidiFi, Tessera, and others).

| DEX | Program ID |
|-----|-----------|
| Pump Fun | `6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P` |
| Pump AMM | `pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA` |
| Raydium AMM V4 | `675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8` |
| Raydium CPMM | `CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C` |
| Raydium CLMM | `CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK` |
| Raydium LaunchLab | `LanMV9sAd7wArD4vJFi2qDdfnVhFxYSUg6eADduJ3uj` |
| Orca Whirlpool | `whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc` |
| Orca V2 | `9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP` |
| Meteora DAMM V2 | `cpamdpZCGKUy5JxQXB4dcpGPiikHawvSWAd6mEn1sGG` |
| Meteora DBC | `dbcij3LWUppWqq96dh6gJWwBifmcGfLSB5D4DuSMaqN` |
| Meteora DLMM | `LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo` |
| Meteora Pools | `Eo7WjKq67rjJQSZxS6z3YkapzY3eMj6Xy8X5EQVn5UaB` |
| Manifest | `MNFSTqtC93rEfYHB6hF82sKdZpUDFWkViLByLd1k1Ms` |
| Aquifer | `AQU1FRd7papthgdrwPTTq5JacJh8YtwEXaBfKU3bTz45` |
| Lifinity V1 | `EewxydAPCCVuNEyrVN68PuSYdQ7wKn27V9Gjeoi8dy3S` |
| Lifinity V2 | `2wT8Yq49kHgDzXuPxZSaeLaH1qbmGXtEyPy64bL7aD3c` |
| PancakeSwap CLMM | `HpNfyc2Saw7RKkQd8nEL4khUcuPhQ7WwY1B2qjx8jxFq` |
| Byreal CLMM | `REALQqNEomY6cQGZJUGwywTBD2UmDT32rZcNnfxQ5N2` |
| Futarchy AMM | `FUTARELBfJfQ8RDGhg1wdhddq1odMAJUePHFuBYfUxKq` |
| Obric V2 | `obriQD1zbpyLz95G5n7nJe6a4DPjpFwa5XYPoNm113y` |
| SolFi V1 | `SoLFiHG9TfgtdUXUjWAxi3LtvYuFyDLVhBWxdMZxyCe` |
| SolFi V2 | `SV2EYYJyRz2YhfXwXnhNAevDEui5Q6yrfyo13WtupPF` |
| AlphaQ | `ALPHAQmeA7bjrVuccPsYPiCvsi428SNwte66Srvs4pHA` |
| BisonFi | `BiSoNHVpsVZW2F7rx2eQ59yQwKxzU5NvBcmKshCSUypi` |
| ZeroFi | `ZERor4xhbUycZ6gb9ntrhqscUcZmAbQDjEAtCf4hbZY` |
| GoonFi | `goonERTdGsjnkZqWuVjs73BZ3Pb9qoCUdBUL17BnS5j` |
| HumidiFi | `9H6tua7jkLhdm3w8BvgpTn5LZNU7g4ZynDmCiNN3q6Rp` |
| Tessera | `TessVdML9pBGgG9yGks7o4HewRaXVAMuoVj4x83GLQH` |
| Stabble Stable | `swapNyd8XiQwJ6ianp9snpu4brUqFxadzvHebnAXjJZ` |
| Stabble Weighted | `swapFpHZwjELNnjvThjajtiVmkz3yPQEHjLtka2fwHW` |

## Output Schema

Primary output is protobuf. Each slot produces a `DexTradesBatch` containing `SwapRecord` messages:

| Field | Type | Description |
|-------|------|-------------|
| `block_slot` | uint64 | Slot number |
| `block_time` | int64 | Unix timestamp (seconds) |
| `block_date` | string | `YYYY-MM-DD` |
| `tx_id` | string | Base58 transaction signature |
| `tx_index` | uint32 | Transaction index within block |
| `instruction_index` | uint32 | Outer instruction index (1-based) |
| `inner_instruction_index` | uint32 | Inner index (1-based, 0 if outer) |
| `is_inner_instruction` | bool | Whether this is a CPI call |
| `instruction_type` | string | e.g. `Buy`, `Sell`, `Swap`, `SwapBaseIn` |
| `outer_program` | string | Top-level program (e.g. Jupiter aggregator) |
| `inner_program` | string | Actual DEX program that executed the swap |
| `pool_address` | string | AMM/CLMM pool address |
| `signer` | string | Transaction fee payer |
| `token_bought_mint` | string | Mint of token received |
| `token_bought_vault` | string | Pool vault for bought token |
| `token_bought_amount` | double | Amount received (decimal-adjusted) |
| `token_bought_decimals` | uint32 | Decimals for bought token |
| `token_bought_vault_reserve` | double | Pool reserve after swap |
| `token_sold_mint` | string | Mint of token sent |
| `token_sold_vault` | string | Pool vault for sold token |
| `token_sold_amount` | double | Amount sent (decimal-adjusted) |
| `token_sold_decimals` | uint32 | Decimals for sold token |
| `token_sold_vault_reserve` | double | Pool reserve after swap |
| `txn_fee` | double | Total transaction fee (SOL) |
| `priority_fee` | double | Priority fee portion (SOL) |
| `jito_tips` | double | Jito tip amount (SOL) |
| `sqrt_price` | string | CLMM sqrt price (u128 string, empty for AMMs) |
| `has_direction` | bool | Whether `is_base_to_quote` is meaningful |
| `is_base_to_quote` | bool | Trade direction for CLMM pools |
| `compute_units_consumed` | uint64 | CU used by the transaction |

Token convention: `token_bought` = what the user received, `token_sold` = what the user sent.

## Usage

### As a built-in plugin

```bash
JETSTREAMER_CLICKHOUSE_MODE=off JETSTREAMER_THREADS=4 \
  cargo run --release -- --with-plugin dex-trades 280000000:280001000
```

### With decoded output dump

Set `DEX_TRADES_DUMP=1` to print every decoded swap to stdout:

```bash
DEX_TRADES_DUMP=1 JETSTREAMER_CLICKHOUSE_MODE=off JETSTREAMER_THREADS=1 \
  cargo run --release -- --with-plugin dex-trades 280000000:280000010
```

### As a library

```rust
use jetstreamer_dex_trades::{DexTradesPlugin, DexRegistry};

// Plugin mode — attach to JetstreamerRunner
JetstreamerRunner::new()
    .with_plugin(Box::new(DexTradesPlugin::new()))
    .parse_cli_args().unwrap()
    .run().unwrap();

// Library mode — decode transactions directly
let registry = DexRegistry::new();
let swaps: Vec<SwapRecord> = registry.decode_transaction(&tx_data, block_time);
```

### With batch callback

```rust
let plugin = DexTradesPlugin::with_batch_callback(|slot, protobuf_bytes| {
    // Write to file, send over network, etc.
    std::fs::write(format!("slot_{slot}.pb"), &protobuf_bytes).unwrap();
});
```

## Decoding Strategies

Decoders use three approaches depending on the DEX:

1. **Event-based** — Parse Anchor self-CPI events from `Program data:` logs (Orca Whirlpool, Raydium CLMM/CPMM, Meteora DLMM/DAMM V2, etc.)
2. **Log-based** — Parse custom log prefixes like `ray_log:` (Raydium AMM V4)
3. **Transfer-based** — Infer swaps from SPL Token / Token-2022 transfers in inner instructions (Orca V2, Lifinity, Stabble, smaller DEXes)
