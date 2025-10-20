# Jetstreamer

[![Crates.io](https://img.shields.io/crates/v/jetstreamer.svg)](https://crates.io/crates/jetstreamer)
[![Docs.rs](https://docs.rs/jetstreamer/badge.svg)](https://docs.rs/jetstreamer)
[![CI](https://github.com/anza-xyz/jetstreamer/actions/workflows/rust.yaml/badge.svg)](https://github.com/anza-xyz/jetstreamer/actions/workflows/rust.yaml)

## Overview

Jetstreamer is a high-throughput Solana backfilling and research toolkit designed to stream
historical chain data live over the network from Project Yellowstone's [Old
Faithful](https://old-faithful.net/) archive which is a comprehensive open source archive of
all Solana blocks and transactions from genesis to the current tip of the chain. Jetstreamer
can stream data at over 2.7M TPS with the right hardware and network connection. 

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
contain account updates, so Jetstreamer at the moment also does not have account updates,
though we plan to eventually have a separate project that provides this, stay tuned!

## Quick Start

To get an idea of what Jetstreamer is capable of, you can try out the demo CLI that runs
Jetstreamer Runner with the Program Tracking plugin enabled:

### Jetstreamer Runner CLI

```bash
# Replay all transactions in epoch 800, using 8 HTTP multiplexing cores.
JETSTREAMER_THREADS=8 cargo run --release -- 800

# Do the same but for slots 358560000 through 367631999, which is epoch 830-850 (slot ranges can be cross-epoch!)
JETSTREAMER_THREADS=8 cargo run --release -- 358560000:367631999
```

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

Note that Jetstreamer's firehose and underlying interface emits events for leader-skipped
blocks, unlike traditional geyser.

```rust
use std::sync::Arc;

use clickhouse::Client;
use jetstreamer::{
    JetstreamerRunner,
    firehose::firehose::{BlockData, TransactionData},
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

unsafe {
    std::env::set_var("JETSTREAMER_THREADS", "4");
    std::env::set_var("JETSTREAMER_CLICKHOUSE_MODE", "remote");
}

JetstreamerRunner::new()
    .with_plugin(Box::new(LoggingPlugin))
    .parse_cli_args()
    .expect("CLI arguments and environment variables parsed")
    .run()
    .expect("runner completed");
```

When `JETSTREAMER_CLICKHOUSE_MODE` is `auto` (the default), Jetstreamer inspects the DSN to
decide whether to launch the bundled ClickHouse helper or connect to an external cluster.

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

## Configuration

The following configuration ENV vars are available across the Jetstreamer ecosystem:

### JetstreamerRunner Config

| Variable | Default | Effect |
|----------|---------|--------|
| `JETSTREAMER_CLICKHOUSE_DSN` | `http://localhost:8123` | HTTP(S) DSN passed to the embedded plugin runner for ClickHouse writes. Override to target a remote ClickHouse deployment. |
| `JETSTREAMER_CLICKHOUSE_MODE` | `auto` | Controls ClickHouse integration. `auto` enables output and spawns the helper only for local DSNs, `remote` enables output without spawning the helper, `local` always requests the helper, and `off` disables ClickHouse entirely. |
| `JETSTREAMER_THREADS` | `1` | Number of firehose ingestion threads. Increase based on CPU headroom and downstream sink capacity. |

Helper spawning only occurs when both the mode allows it (`auto`/`local`) **and** the DSN
points to `localhost` or `127.0.0.1`.

### Firehose Config (also used by JetstreamerRunner)

| Variable | Default | Effect |
|----------|---------|--------|
| `JETSTREAMER_COMPACT_INDEX_BASE_URL` | `https://files.old-faithful.net` | Base URL for compact CAR index artifacts. Point this at your own mirror to reduce load on the public archive. |
| `JETSTREAMER_NETWORK` | `mainnet` | Network suffix appended to cache namespaces and index filenames (e.g., `testnet`). |

Changing the network automatically segregates cache entries, allowing you to toggle between
clusters without purging state.

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

## Developing Locally

- Format and lint: `cargo fmt --all` and `cargo clippy --workspace`.
- Run tests: `cargo test --workspace`.
- Regenerate docs: `cargo doc --workspace --open`.

## Community

Questions, issues, and contributions are welcome! Open a discussion or pull request on
[GitHub](https://github.com/anza-xyz/jetstreamer) and join the effort to build faster Solana
analytics pipelines.
