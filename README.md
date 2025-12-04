# Jetstreamer

[![Crates.io](https://img.shields.io/crates/v/jetstreamer.svg)](https://crates.io/crates/jetstreamer)
[![Docs.rs](https://docs.rs/jetstreamer/badge.svg)](https://docs.rs/jetstreamer)
[![CI](https://github.com/anza-xyz/jetstreamer/actions/workflows/rust.yaml/badge.svg)](https://github.com/anza-xyz/jetstreamer/actions/workflows/rust.yaml)

## Overview

Jetstreamer is a high-throughput Solana backfilling and research toolkit designed to stream
historical chain data live over the network from Project Yellowstone's [Old
Faithful](https://old-faithful.net/) archive, which is a comprehensive open source archive of
all Solana blocks and transactions from genesis to the current tip of the chain. Given the
right hardware and network connection, Jetstreamer can stream data at over 2.7M TPS to a local
Jetstreamer plugin or geyser plugin. Higher speeds are possible with better hardware (in our
case 64 core CPU, 30 Gbps+ network for the 2.7M TPS record).

Jetstreamer exposes three companion crates:

- `jetstreamer` – the primary facade that wires firehose ingestion into your plugins through
  `JetstreamerRunner`.
- `jetstreamer-firehose` – async helpers for downloading, compacting, and replaying Old
  Faithful CAR archives at scale.
- `jetstreamer-plugin` – a trait-based framework for building structured observers with
  ClickHouse-friendly batching and runtime metrics.
- `jetstreamer-utils` - utils used by the Jetstreamer ecosystem.

Every crate ships with rich module-level documentation and runnable examples. Visit
[docs.rs/jetstreamer](https://docs.rs/jetstreamer) to explore the API surface in detail.

All 3 sub-crates are provided as re-exports within the main `jetstreamer` crate via the
following re-exports:
- `jetstreamer::firehose`
- `jetstreamer::plugin`
- `jetstreamer::utils`

## Limitations

While Jetstreamer is able to play back all blocks, transactions, epochs, and rewards in the
history of Solana mainnet, it is limited by what is in Old Faithful. Old Faithful does not
contain account updates, so Jetstreamer at the moment also does not have them, though we plan to eventually have a separate project that provides this, stay
tuned!

It is worth noting that the way Old Faithful and thus Jetstreamer stores transactions, they are
stored in their "already-executed" state as they originally appeared to Geyser when they were
first executed. Thus while Jetstreamer can replay ledger data, it is not executing transactions
directly, and when we say 2.7M TPS, we mean "2.7M transactions processed by a Jetstreamer or
Geyser plugin locally, streamed over the internet from the Old Faithful archive."

## Quick Start

To get an idea of what Jetstreamer is capable of, you can try out the demo CLI that runs
Jetstreamer Runner with the Program Tracking plugin enabled. Pass `--with-plugin
instruction-tracking` (or repeat the flag to run both built-ins) to change the default set:

### Jetstreamer Runner CLI

```bash
# Replay all transactions in epoch 800, using the default number of multiplexing threads based on your system
cargo run --release -- 800

# The same as above, but tuning network capacity for 10 Gbps, resulting in a higher number of multiplexing threads
JETSTREAMER_NETWORK_CAPACITY_MB=10000 cargo run --release -- 800

# Do the same but for slots 358560000 through 367631999, which is epoch 830-850 (slot ranges can be cross-epoch!)
# and using 8 threads explicitly instead of using automatic thread count
JETSTREAMER_THREADS=8 cargo run --release -- 358560000:367631999

# Replay epoch 800 with the instruction tracking plugin instead of the default
cargo run --release -- 800 --with-plugin instruction-tracking
```

If `JETSTREAMER_THREADS` is omitted, Jetstreamer auto-sizes the worker pool using the same
hardware-aware heuristic exposed by
`jetstreamer_firehose::system::optimal_firehose_thread_count`.

The built-in program and instruction tracking plugins now record vote and non-vote activity
separately. `program_invocations` includes an `is_vote` flag per row, while `slot_instructions`
stores separate vote/non-vote instruction and transaction counts.

The CLI accepts either `<start>:<end>` slot ranges or a single epoch on the command line. See
[`JetstreamerRunner::parse_cli_args`](https://docs.rs/jetstreamer/latest/jetstreamer/fn.parse_cli_args.html)
for the precise rules.

### ClickHouse Integration

Jetstreamer Runner has a built-in ClickHouse integration (by default a clickhouse server is
spawned running out of the `bin` directory in the repo)

To manage the ClickHouse integration with ease, the following bundled Cargo aliases are
provided when within the `jetstreamer` workspace:

```bash
cargo clickhouse-server
cargo clickhouse-client
```

`cargo clickhouse-server` launches the same ClickHouse binary that Jetstreamer Runner spawns in
`bin/`, while `cargo clickhouse-client` connects to the local instance so you can inspect
tables populated by the runner or plugin runner.

While Jetstreamer is running, you can use `cargo clickhouse-client` to connect directly to the
ClickHouse instance that Jetstreamer has spawned. If you want to access data after a run has
finished, you can run `cargo clickhouse-server` to bring up that server again using the data
that is currently in the `bin` directory. It is also possible to copy a `bin` directory from
one system to another as a way of migrating data.

### Writing Jetstreamer Plugins

Jetstreamer Plugins are plugins that can be run by the Jetstreamer Runner.

Implement the `Plugin` trait to observe epoch/block/transaction/reward/entry events. The
example below mirrors the crate-level documentation and demonstrates how to react to both
transactions and blocks.

Note that Jetstreamer's firehose and underlying interface emits `BlockData::PossibleLeaderSkipped`
events whenever it observes a slot gap. These represent either leader-skipped slots or blocks
that have not arrived yet; when the real block eventually shows up, `BlockData::Block` will be
emitted for it just like normal geyser streams.

Also note that because Jetstreamer spawns parallel threads that process different subranges of
the overall slot range at the same time, while each thread sees a purely sequential view of
transactions, downstream services such as databases that consume this data will see writes in a
fairly arbitrary order, so you should design your database tables and shared data structures
accordingly.

```rust
use std::sync::Arc;

use clickhouse::Client;
use jetstreamer::{
    JetstreamerRunner,
    firehose::firehose::{BlockData, TransactionData},
    firehose::epochs,
    plugin::{Plugin, PluginFuture},
};

struct LoggingPlugin;

impl Plugin for LoggingPlugin {
    fn name(&self) -> &'static str {
        "logging"
    }

    fn on_transaction<'a>(
        &'a self,
        _thread_id: usize,
        _db: Option<Arc<Client>>,
        tx: &'a TransactionData,
    ) -> PluginFuture<'a> {
        Box::pin(async move {
            println!("tx {} landed in slot {}", tx.signature, tx.slot);
            Ok(())
        })
    }

    fn on_block<'a>(
        &'a self,
        _thread_id: usize,
        _db: Option<Arc<Client>>,
        block: &'a BlockData,
    ) -> PluginFuture<'a> {
        Box::pin(async move {
            if block.was_skipped() {
                println!("slot {} was skipped", block.slot());
            } else {
                println!("processed block at slot {}", block.slot());
            }
            Ok(())
        })
    }
}

let (start_slot, end_inclusive) = epochs::epoch_to_slot_range(800);

JetstreamerRunner::new()
    .with_plugin(Box::new(LoggingPlugin))
    .with_threads(4)
    .with_slot_range_bounds(start_slot, end_inclusive + 1)
    .with_clickhouse_dsn("https://clickhouse.example.com")
    .run()
    .expect("runner completed");
```

If you prefer to configure Jetstreamer via the command line, keep using
`JetstreamerRunner::parse_cli_args` to hydrate the runner from process arguments and
environment variables.

When `JETSTREAMER_CLICKHOUSE_MODE` is `auto` (the default), Jetstreamer inspects the DSN to
decide whether to launch the bundled ClickHouse helper or connect to an external cluster.

### Alternate Archive Mirrors

Jetstreamer defaults to the public Old Faithful mirror (`https://files.old-faithful.net`), but
the firehose can also stream CARs and compact indexes directly from authenticated
S3-compatible storage. Configure the backend via the following environment variables:

- `JETSTREAMER_ARCHIVE_BACKEND` (default `http`): set to `s3` to force the S3 client.
- `JETSTREAMER_HTTP_BASE_URL`: base URL or `s3://bucket/prefix` for CAR files.
- `JETSTREAMER_COMPACT_INDEX_BASE_URL`: optional override for compact indexes (also accepts `s3://` URIs).
- `JETSTREAMER_ARCHIVE_BASE`: single knob that applies to both cars and indexes when the more specific variables are unset.
- `JETSTREAMER_S3_BUCKET`, `JETSTREAMER_S3_PREFIX`, `JETSTREAMER_S3_INDEX_PREFIX`: bucket/prefix overrides when not encoded in the `s3://` URL.
- `JETSTREAMER_S3_REGION` and `JETSTREAMER_S3_ENDPOINT`: region plus optional custom endpoint (e.g. `https://s3.eu-central-003.backblazeb2.com`).
- `JETSTREAMER_S3_ACCESS_KEY`, `JETSTREAMER_S3_SECRET_KEY`, `JETSTREAMER_S3_SESSION_TOKEN`: credentials used for signing requests (falls back to AWS standard env vars).

S3 support is compiled behind the `s3-backend` Cargo feature. Enable it when running or
depending on `jetstreamer` if you plan to consume `s3://` archives:

```bash
cargo run --features s3-backend -- 800
```

#### Batching ClickHouse Writes

ClickHouse (and anything you do in your callbacks) applies backpressure that will slow down
Jetstreamer if not kept in check.

When implementing a Jetstreamer plugin, prefer buffering records locally and flushing them in
periodic batches rather than writing on every hook invocation. The runner's built-in stats
pulses are emitted every 100 slots by default (`jetstreamer-plugin/src/lib.rs`), which strikes
a balance between timely metrics and avoiding tight write loops. The bundled Program Tracking
plugin follows this model: each worker thread accumulates its desired `ProgramEvent` rows in a
`Vec` and performs a single batch insert once 1,000 slots have elapsed
(`jetstreamer-plugin/src/plugins/program_tracking.rs`). Structuring custom plugins with a
similar cadence keeps ClickHouse responsive during high throughput replays.

### Firehose

For direct access to the stream of transactions/blocks/rewards etc, you can use the `firehose`
interface, which allows you to specify a number of async function callbacks that will receive
transaction/block/reward/etc data on multiple threads in parallel.

## Epoch Feature Availability

Old Faithful ledger snapshots vary in what metadata is available, because Solana as a
blockchain has evolved significantly over time. Use the table below to decide which epochs fit
your needs. In particular, note that early versions of the chain are no longer compatible with
modern geyser but _do_ work with the current `firehose` interface and `JetstreamerRunner`.
Furthermore, CU tracking was not always available historically so it is not available once you
go back far enough.

| Epoch | Slot        | Comment |
|-------|-------------|--------------------------------------------------|
| 0-156 | 0-?         | Incompatible with modern Geyser plugins |
| 157+  | ?           | Compatible with modern Geyser plugins |
| 0-449 | 0-194184610 | CU tracking not available (reported as 0)        |
| 450+  | 194184611+  | CU tracking available                            |

Epochs at or above 157 are compatible with the current Geyser plugin interface, while compute
unit accounting first appears at epoch 450. Plan replay windows accordingly.

## Installation and Setup

### Nix (Recommended)

For the most reliable setup, use Nix:

```bash
nix-shell
cargo build --release
```

### Non-Nix Setup

Jetstreamer requires **Clang 16** (not 17) due to RocksDB dependencies. Install dependencies and set environment variables:

#### Linux (Ubuntu/Debian)

```bash
# Install Clang 16
wget -qO- https://apt.llvm.org/llvm.sh | sudo bash -s -- 16
sudo apt update && sudo apt install -y gcc-13 g++-13 zlib1g-dev libssl-dev libtool

# Set as default
sudo update-alternatives --install /usr/bin/clang clang /usr/bin/clang-16 100
sudo update-alternatives --install /usr/bin/clang++ clang++ /usr/bin/clang++-16 100

# Environment variables (add to ~/.bashrc)
export CC=clang
export CXX=clang++
export LIBCLANG_PATH=/usr/lib/llvm16/lib/libclang.so
```

#### Linux (Arch)

```bash
sudo pacman -S clang16 llvm16 zlib openssl libtool
yay -S gcc13  # or use system gcc

# Environment variables (add to ~/.bashrc)
export CC=clang-16
export CXX=clang++-16
export LIBCLANG_PATH=/usr/lib/llvm16/lib/libclang.so
export LD_LIBRARY_PATH=/usr/lib/llvm16/lib:$LD_LIBRARY_PATH
```

#### macOS

```bash
brew install llvm@16 zlib openssl libtool

# Environment variables (add to ~/.zshrc)
export CC=/opt/homebrew/opt/llvm@16/bin/clang
export CXX=/opt/homebrew/opt/llvm@16/bin/clang++
export LIBCLANG_PATH=/opt/homebrew/opt/llvm@16/lib/libclang.dylib
export LDFLAGS="-L/opt/homebrew/opt/llvm@16/lib"
export CPPFLAGS="-I/opt/homebrew/opt/llvm@16/include"
```

**Troubleshooting**: If you get RocksDB compilation errors, ensure you're using Clang 16 (not 17) and `LIBCLANG_PATH` is correctly set.

## Developing Locally

- Format and lint: `cargo fmt --all` and `cargo clippy --workspace`.
- Run tests: `cargo test --workspace`.
- Regenerate docs: `cargo doc --workspace --open`.

## Community

Questions, issues, and contributions are welcome! Open a discussion or pull request on
[GitHub](https://github.com/anza-xyz/jetstreamer) and join the effort to build faster Solana
analytics pipelines.

## License

Licensed under either of

* Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
* MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.
