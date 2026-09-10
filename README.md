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

Jetstreamer is split across four crates:

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
contain account updates, so Jetstreamer at the moment also does not have them. Transaction logs
are available in `transaction_status_meta.log_messages` for all epochs.

It is worth noting that the way Old Faithful and thus Jetstreamer stores transactions, they are
stored in their "already-executed" state as they originally appeared to Geyser when they were
first executed. Thus while Jetstreamer can replay ledger data, it is not executing transactions
directly, and when we say 2.7M TPS, we mean "2.7M transactions processed by a Jetstreamer or
Geyser plugin locally, streamed over the internet from the Old Faithful archive."

## Quick Start

To get an idea of what Jetstreamer is capable of, you can try out the demo CLI that runs
Jetstreamer Runner with the Program Tracking plugin enabled. The built-in plugins are
`program-tracking`, `instruction-tracking`, and `pubkey-stats`; pass `--with-plugin <name>` to
select one (repeat the flag to run multiple at once), or `--no-plugins` to disable the default.
Run `--list-plugins` to print the names of every bundled plugin and exit.

### Jetstreamer Runner CLI

```bash
# Replay all transactions in epoch 800, using the default number of multiplexing threads based on your system
cargo run --release -- 800

# The same as above, but tuning network capacity for 10 Gbps, resulting in a higher number of multiplexing threads
JETSTREAMER_NETWORK_CAPACITY_MB=10000 cargo run --release -- 800

# Do the same but for slots 358560000 through 367631999, which is epoch 830-850 (slot ranges can be cross-epoch!)
# and using 8 threads explicitly instead of using automatic thread count
JETSTREAMER_THREADS=8 cargo run --release -- 358560000:367631999

# Replay epochs 900 through 950 inclusive using epoch-range syntax
cargo run --release -- 900-950

# Replay with the live terminal dashboard (TPS graph, per-thread health, system stats)
cargo run --release -- 950 --tui

# Replay epoch 800 with the instruction tracking plugin instead of the default
cargo run --release -- 800 --with-plugin instruction-tracking

# Point the runner at an external ClickHouse instance (overrides JETSTREAMER_CLICKHOUSE_DSN)
cargo run --release -- 800 --clickhouse-dsn http://clickhouse.example.com:8123
```

If `JETSTREAMER_THREADS` is omitted, Jetstreamer auto-sizes the worker pool using the same
hardware-aware heuristic exposed by
`jetstreamer_firehose::system::optimal_firehose_thread_count`.

For sequential replay mode, enable `--sequential` (or `JETSTREAMER_SEQUENTIAL=1`). In this mode
Jetstreamer uses a single firehose worker and reuses `JETSTREAMER_THREADS` as ripget parallel
download concurrency:

```bash
# Sequential mode with CLI flag
JETSTREAMER_THREADS=4 cargo run --release -- 800 --sequential

# Sequential mode with explicit ripget window override (either the env var or the
# --buffer-window CLI flag works)
JETSTREAMER_SEQUENTIAL=1 cargo run --release -- 800 --buffer-window 4GiB
```

`JETSTREAMER_BUFFER_WINDOW` (or `--buffer-window`) defaults to `min(4 GiB, 15% of available
RAM)` when unset.

To backfill from newest to oldest, add `--reverse` (or `JETSTREAMER_REVERSE=1`). Reverse mode
implies sequential mode and processes epochs in the slot range from highest to lowest. Within
each epoch, slots still come out in ascending order because Old Faithful CAR archives are
forward-only streams.

```bash
# Backfill epochs 800..=802 starting with 802, then 801, then 800
cargo run --release -- 345600000:1296000000 --reverse
```

The built-in `program-tracking` and `instruction-tracking` plugins record vote and non-vote
activity separately: `program_invocations` includes an `is_vote` flag per row, while
`slot_instructions` stores separate vote/non-vote instruction and transaction counts. The
`pubkey-stats` plugin aggregates per-slot account-key mention counts into a `pubkey_mentions`
table (with a companion `pubkeys` lookup table populated via materialised view).

The CLI accepts a single epoch (`950`), an inclusive `<start>-<end>` epoch range (`900-950`),
or an inclusive `<start>:<end>` slot range on the command line. See
[`JetstreamerRunner::parse_cli_args`](https://docs.rs/jetstreamer/latest/jetstreamer/fn.parse_cli_args.html)
for the precise rules.

### TUI dashboard

Add `--tui` to render a live terminal dashboard instead of plain log output:

- **TPS graph** with clickable time ranges (`5m`–`12h` trailing windows, or `all` for the
  entire run), a 5s rolling rate, and `0 / current / peak` axis labels.
- **Progress bar** with slots, ETA, and elapsed time.
- **Thread grid**: one dot per firehose thread, colored by data freshness (green → red as a
  thread approaches the stall timeout; cyan ✓ = finished its work).
- **System chart**: CPU, memory, and NIC download rate on one axis.
- **Stats panel**: live and average TPS, per-thread TPS spread, block/transaction totals,
  connection recycles, timeouts, work steals, ClickHouse write retries, and wire vs payload
  data rates.
- **Scrollable log pane** with a scrollbar and a click-to-resume `live` button.

Pane dividers are mouse-draggable. Press `q`, `Esc`, or `Ctrl-C` for the same graceful
shutdown as SIGINT; the final log lines are replayed to the terminal on exit.

### Throughput management

The threaded firehose actively manages its connection fleet to cope with CDN throttling:
threads launch through a health gate (pausing the ramp while any thread is stalled),
failed threads restart with exponential backoff, persistently-slow connections are recycled
(♻️) for fresh ones, and threads that finish their slot range steal work (🥷) — via a
message handshake in which the least-progressed thread hands over half of its remaining
slots — so every connection stays busy to the end of the run. Tune with
`JETSTREAMER_SPAWN_PENDING`, `JETSTREAMER_SPAWN_GRACE_SECS`, and `JETSTREAMER_RECYCLE_PCT`
(see the crate docs for details).

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

#### Write durability

ClickHouse writes are never silently dropped. Inserts use `async_insert` with
`wait_for_async_insert=1`, so an acknowledgment means the data was durably flushed; any
failure is retried with exponential backoff (0.5s doubling to a 15s cap) for up to 10
minutes, visible live as the `db retries` stat in the TUI. In-flight write tasks are tracked
and drained at shutdown, so finishing a run (or Ctrl-C) never cancels a batch mid-delivery.
If a write is still failing after the full horizon, the run aborts loudly and prints the
exact command to resume from the lowest unprocessed slot. Retries give at-least-once delivery: every table is a
`ReplacingMergeTree` keyed on its logical identity, so a replayed batch deduplicates on
merge — consumers should query with `FINAL` (or tolerate transient duplicates) when reading
while ingestion is active.

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
    firehose::{BlockData, TransactionData},
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
- `JETSTREAMER_COMPACT_INDEX_BASE_URL`: optional override for slot indexes (also accepts `s3://` URIs). Jetstreamer resolves slot offsets from the per-epoch `epoch-{N}-slot-ranges.raw` files (~5 MB each) and falls back to the deprecated legacy compactindex pair when a mirror does not serve them (`JETSTREAMER_FORCE_LEGACY_INDEX=1` forces the legacy path).
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
pulses and per-plugin flush cadence are both driven by `db_update_interval_slots` (100 slots
by default, defined in `jetstreamer-plugin/src/lib.rs`), which strikes a balance between
timely metrics and avoiding tight write loops. The bundled plugins follow this model: they
accumulate per-slot events in a shared `DashMap` (see
`jetstreamer-plugin/src/plugins/program_tracking.rs`) and the runner batch-inserts the drained
rows on the shared flush cadence. Structuring custom plugins with a similar cadence keeps
ClickHouse responsive during high throughput replays.

### Firehose

For direct access to the stream of transactions/blocks/rewards etc, you can use the `firehose`
interface, which allows you to specify a number of async function callbacks that will receive
transaction/block/reward/etc data on multiple threads in parallel.

## Epoch Feature Availability

Old Faithful changed its transaction-status metadata encoding at epoch 157. The firehose
selects the decoder from the slot and supports both encodings. Compute-unit metadata starts at
slot 194,184,611, partway through epoch 449.

| Epoch/range | Slot range        | Comment |
|-------------|-------------------|-----------------------------------------------|
| 0-156       | 0-67,823,999      | Bincode transaction metadata (auto-decoded)   |
| 157+        | 67,824,000+       | Protobuf transaction metadata                 |
| through 449 | 0-194,184,610     | CU tracking unavailable (reported as `0`)     |
| from 449    | 194,184,611+      | CU tracking available                         |

The epoch-157 cutoff is an archive-input boundary, not a consensus-runtime boundary.
Reconstructing historical account updates requires execution rules that match the requested
slot range.

### Historical replay compatibility

`jetstreamer-node` selects execution semantics from the complete half-open slot range before it
loads a snapshot or starts a worker. Snapshot extensions choose only the state loader; they do not
select a runtime. The current registry is deliberately conservative:

| Slots | Runtime | Admission |
|---|---|---|
| `0..619,849` | pinned Solana v1.0.7 worker | candidate through the first proven-safe handoff |
| `619,849..3,456,000` | pinned Solana v1.0.8 worker | candidate through epoch 7 |
| `3,456,000..3,888,000` | pinned Solana v1.0.13 worker | candidate for epoch 8 |
| `3,888,000..5,184,000` | pinned Solana v1.0.14 worker | candidate for epochs 9-11 |
| `5,184,000..12,960,000` | pinned Solana v1.0.23 worker | diagnostic candidate for epochs 12-29 |
| `12,960,000..26,352,000` | pinned Solana v1.1.23 worker | diagnostic candidate for epochs 30-60 |
| `26,352,000..39,744,000` | pinned Solana v1.2.32 worker | diagnostic candidate for epochs 61-91 |
| `39,744,000..43,632,000` | pinned Solana v1.3.19 worker | diagnostic candidate for epochs 92-100 |
| `43,632,000..406,080,000` | none | unsupported; replay fails closed |
| `406,080,000..` | in-process Agave v3 | verified |

Candidate mode requires the exact runtime identity, an explicit
`JETSTREAMER_ALLOW_CANDIDATE_RUNTIME=1`, snapshot verification, and at least one canonical
checkpoint after the bootstrap slot. Unknown opt-in values are rejected. Current behavioral
evidence proves the old v1.0.7 vote-initialization semantics through slot 618,196 and first requires
the v1.0.8 semantics at slot 630,648. The canonical snapshot at slot 619,848 is therefore the
behaviorally safe handoff: v1.0.7 processes through that snapshot and v1.0.8 starts at slot 619,849.
This routing point is not a claim about the exact deployment slot. Every candidate envelope remains
explicitly non-canonical until its checkpoint replay completes. Additional exact patch workers stay
registered but unassigned until differential evidence requires a narrower runtime boundary. This
includes the v1.0.17 worker, which remains available for comparison without claiming a slot range.

Epoch 12 starts v1.0.23 from the canonical snapshot at slot 5,183,736 with legacy accounts hash
`BUqwiSm2GgH9ByKrBDF6epXHYK9RRh3vyZDKtUqtMXfR`. Production discovery binds both values, so a
later local epoch-11 snapshot cannot shorten the registered warmup. The worker warms slots 5,183,737
through 5,183,999 and begins Horizon output at slot 5,184,000. This boundary uses an independently
verified snapshot restart. The runtime handoff registry has no entry at 5,184,000.

Transaction metadata is an independent compatibility dimension. Old Faithful has no status frame
before slot `4,258,776`; the pinned historical runtime reconstructs transaction status there, while
the remaining metadata stays explicitly unavailable. At and after that slot, a missing status frame
is an error. The early source writer also stored an entry's randomized execution results beside its
original-order transactions. For the epoch 0-100 compatibility scope, replay verifies that the
source and runtime statuses have the same entry-wide multiset, then uses the runtime result to
restore each transaction's status. Because the same defect could select another transaction's
durable-nonce fee calculator, protocol-v5 workers also return the runtime-associated fee. Replay
uses that fee wherever complete source metadata is available. This policy changes during epoch 9
without changing the execution runtime.

Generated Horizon archives record the selected runtime identity and admission level, genesis,
bootstrap state, output slot range, and transaction-metadata policy in a versioned provenance
envelope. Range resume skips a completed archive only when that provenance matches the current
slot-derived plan.

When an epoch crosses a registered runtime boundary, `jetstreamer-node` splits it automatically
into bounded child replays. Each child writes a complete Horizon V2 segment plus a durable JSON
evidence sidecar bound to both the archive and worker executable by SHA-256. At the v1.0.7 →
v1.0.8 boundary, the predecessor exports the registry-committed slot-619,848 snapshot only after
its frozen checkpoint matches the canonical accounts hash. A second durable sidecar binds every
snapshot byte—including status-cache state not covered by the accounts hash—to that checkpoint
and the measured predecessor worker. The successor copies exactly the sidecar-declared byte length
into private storage while verifying that digest, restores only the bound copy, and must reproduce
the full checkpoint state
without bootstrap writes. The final Horizon V3 records the exact handoff archive digest, streams
and re-encodes the segments, checks PoH continuity and every segment's terminal blockhash, rebases
runtime-local write versions into one contiguous namespace, fully decodes the result, and then
publishes it atomically. Interrupted segment files are retained for validated resume, and an older
final output is moved to a recoverable backup only after the new archive has passed verification.

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
