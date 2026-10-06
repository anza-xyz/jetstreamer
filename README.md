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

### Horizon plugin pipeline

`horizon_pipeline` runs the zero-copy Horizon plugin interface over local `.jet` files or an
HTTP base URL. Its verification plugin writes no database rows. It consumes every delivered
block, transaction, entry, account update, and account-data byte, then checks complete slot
coverage, per-worker ordering, transaction and entry counts, and epoch metadata before allowing
the epoch to finish.

```bash
cargo run --release --bin horizon_pipeline -- \
  0:100 /path/to/horizon --threads 16 --verify-only
```

This checks plugin compatibility and stream delivery. Archive acceptance also requires every
`.sha256` sidecar and a strict ordered-chain scan. Large ranges can recompute PoH in parallel by
pairing one non-full chain scan with one internal full scan per archive:

```bash
verify_archive --chain /path/to/horizon/epoch-{0..100}.jet --threads 8
verify_archive /path/to/horizon/epoch-42.jet \
  --full --internal-full --threads 12
```

The internal scan checks every within-archive link and recomputes every block's PoH. It is not an
acceptance result by itself because it cannot prove the incoming boundary or trailing skipped slots
without an adjacent archive. The successful chain scan supplies those proofs across every archive
boundary.

`scripts/verify_horizon_range.sh` starts the independent full scan for each archive as soon as its
SHA-256 sidecar appears. After the complete range is present, it verifies the range manifest, runs
the ordered chain scan, and rehashes every archive before recording acceptance. Receipts include
the archive, verifier, and script hashes, so a restart only repeats stale or unfinished work.

Large ranges need not remain on local disk simultaneously. After both adjacent archives have exact
full-verification receipts, `archive_boundaries --verify-pair` decodes the left archive's terminal
bucket and the right archive's leading bucket(s). It requires contiguous epoch and slot ranges, the
right initial PoH anchor to equal the left terminal blockhash, and the right first block's parent
slot and hash to name that same terminal block. `scripts/verify_horizon_boundaries_progressive.sh`
composes that edge proof with the two full receipts and writes an fsynced receipt bound to both
archive SHA-256 values and the exact boundary verifier. This avoids redundantly decoding tens of
gigabytes for every overlapping pair while retaining the same proof: the full receipts cover all
within-archive structure and PoH, and the edge receipt covers the only cross-archive link. The two
outer boundaries must still be proved by the neighboring epochs or explicit canonical anchors.
The full-verifier argument is an explicit comma-separated SHA-256 allowlist so a boundary spanning
a verifier upgrade can compose two independently approved full receipts without weakening either
side of the proof.

`scripts/verify_horizon_plugin_range.sh` is the corresponding progressive launcher for the
consumer/API gate. It waits for every archive and well-formed sidecar in an inclusive epoch range,
then runs a pinned `horizon_pipeline --verify-only` binary over the complete range. Run both scripts
for final acceptance: the archive verifier proves integrity, boundaries, and PoH, while the plugin
verifier proves that the current streaming interface can consume every record and account-data byte.

`scripts/verify_horizon_plugin_progressive.sh` runs that consumer/API gate per epoch as soon as a
complete archive pair appears. Its fsynced receipt binds the archive SHA-256, the exact
`horizon_pipeline` binary, and the verification script. R2 work remains concurrent with replay, but
upload is not eligible until this receipt, the full receipt, and both adjacent-boundary receipts
exist for the exact archive digest. Local retirement uses the same gates. This preserves the
ordered-chain proof when a range is larger than available local disk; an epoch remains local until
its predecessor and successor boundaries have both been checked against the exact archive digest.
For a long-lived range, `scripts/watch_horizon_plugin_progressive.sh` checks the sidecar and exact
receipt first, then dispatches that sealed verifier only for a newly available or stale epoch. It
therefore does not repeatedly hash already verified archives while waiting for later epochs; the
dispatched verifier still hashes before and after the plugin scan, and R2 independently hashes the
local source before upload.

### Horizon R2 delivery

`jetstreamer-r2` delivers accepted Horizon archives to an append-only Cloudflare R2 bucket. The
`HORIZON_S3_ENDPOINT` value is an HTTPS R2 endpoint whose path is the bucket name; credentials come
from `HORIZON_ACCESS_KEY_ID` and `HORIZON_SECRET_ACCESS_KEY`. Credential values are never written to
receipts or logs.

R2's S3 `UploadPart` currently rejects `x-amz-checksum-sha256` even though the R2 compatibility
matrix advertises composite SHA-256. The uploader therefore sends R2-validated `Content-MD5` for
every part, reconstructs and checks the final multipart ETag, and reads the completed archive back
through R2 while recomputing its whole-file SHA-256. Only after those checks and local source
revalidation does it upload and read back the canonical whole-file `.sha256` sidecar. The sidecar
is therefore the remote completion marker; an archive key without its sidecar is staged and must
not be consumed as complete. An orphan sidecar with no archive fails closed. If R2 exposes a native
composite SHA-256 for an object, the uploader validates and uses it. A private, fsynced receipt is
the prerequisite for optional local retirement. The binary intentionally has no completed-object
delete operation and refuses to replace an existing remote object that does not match local
evidence by default.
`--overwrite-existing` is an explicit recovery mode: it replaces both remote objects, performs a
fresh full-object SHA-256 readback, and atomically replaces the private receipt. Use it only with
specific authorization to replace the affected keys.

`jetstreamer-r2 restore` reconstructs a local archive pair from R2 for audits that need retired
neighbors. It requires an explicit epoch range and the private R2 receipt directory, conditionally
downloads the exact recorded ETag, recomputes the whole-file SHA-256 and multipart ETag, validates
the canonical sidecar, and publishes with no-clobber semantics. Long downloads and remote
SHA-256 readbacks resume with conditional ranged GETs after transient stalls or early EOFs:

```bash
jetstreamer-r2 restore /absolute/scratch/directory \
  --epochs 101-107 \
  --receipt-directory /absolute/private/r2-receipts
```

An existing destination is accepted only when it already matches the receipt. Partial restores
can be resumed safely; unrelated or mismatching files are never overwritten.

`scripts/sync_horizon_r2_progressive.py` permits R2 publication after the full and current-plugin
receipts agree with the archive. Local retirement remains stricter and additionally requires both
adjacent-boundary receipts. This allows disjoint producers to exchange a verified boundary neighbor
through R2 without creating a circular upload dependency.

`scripts/audit_horizon_receipts.py` is the local-file-independent completion gate. It requires the
full, current-plugin, R2, and adjacent-boundary receipts to agree on every archive SHA-256, and
requires the approved full, plugin, and boundary-verifier binary/script SHA-256 values explicitly. Add
`--require-outer-boundaries` for a strict range publication audit that also proves the incoming
predecessor boundary and the trailing successor boundary.

```bash
cargo build --release -p jetstreamer-r2

# Sync every complete local pair. Omit --delete-local while an active historical
# controller still uses this directory as its completion ledger.
target/release/jetstreamer-r2 sync /path/to/horizon \
  --receipt-directory /path/to/private/r2-receipts \
  --legacy-part-size-mib 5

# Restrict work to an inclusive range and retire proven local pairs.
target/release/jetstreamer-r2 sync /path/to/horizon \
  --epochs 0-100 \
  --receipt-directory /path/to/private/r2-receipts \
  --legacy-part-size-mib 5 \
  --legacy-etag-only \
  --delete-local
```

`--legacy-etag-only` is only for a pre-existing multipart object whose provenance is already
trusted. It still rehashes the local archive, reconstructs the legacy multipart ETag, and reads the
remote sidecar, but skips downloading the whole object. Newly uploaded objects always require native
R2 SHA-256 evidence or a successful whole-object SHA-256 readback before a receipt can authorize
local deletion.

The checked-in `horizon-r2` Codex skill inventories R2 first, selects missing work, uses the
historical compatibility pipeline, and invokes this binary after replay and plugin verification.
With no requested range it starts at the lowest supported missing epoch; an explicit range bounds
generation, verification, delivery, and cleanup.

For an actively generated range, `scripts/sync_horizon_r2_progressive.py` watches for complete
archive/sidecar pairs and invokes `jetstreamer-r2` serially. It checks whether an existing private
receipt still describes the local archive before skipping it. Local retirement additionally
requires receipts from the exact approved full verifier/script, plugin binary/script, and boundary
verifier/script plus both adjacent archive boundaries. The watcher never deletes remote data.

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
| 0-155; early 156 | 0-67,681,335  | Historical bincode schemas with guarded protobuf fallback |
| 156         | 67,681,336-67,823,999 | v1.5.13 bincode with guarded protobuf fallback |
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

| Slots | Runtime | Boundary and range-specific handling | Admission |
|---|---|---|---|
| `0..619,849` | pinned Solana v1.0.7 worker | starts from genesis; preserves the old vote-initialization checks observed through slot 618,196 | candidate through the first proven-safe handoff |
| `619,849..3,456,000` | pinned Solana v1.0.8 worker | canonical state handoff from snapshot slot 619,848; the newer vote checks are first required by observed execution at slot 630,648 | candidate through epoch 7 |
| `3,456,000..3,888,000` | pinned Solana v1.0.13 worker | independently verified snapshot restart; epoch 8's predecessor snapshot uses the newer bank schema | candidate for epoch 8 |
| `3,888,000..5,184,000` | pinned Solana v1.0.14 worker | independently verified snapshot restart | candidate for epochs 9-11 |
| `5,184,000..12,960,000` | pinned Solana v1.0.23 worker | anchored at canonical snapshot slot 5,183,736, warms slots 5,183,737-5,183,999, then starts output at 5,184,000 | diagnostic candidate for epochs 12-29 |
| `12,960,000..13,392,000` | pinned Solana v1.1.15 worker | independently verified snapshot restart; restores and strictly validates the mainnet hard-fork marker at slot 13,334,463 | diagnostic candidate for epoch 30 |
| `13,392,000..26,352,000` | pinned Solana v1.1.23 worker | independently verified snapshot restart; retains the upstream epoch-34 BPF-loader and epoch-40 system-program transitions | diagnostic candidate for epochs 31-60 |
| `26,352,000..29,327,576` | pinned Solana v1.2.32 worker | independently verified snapshot restart; source-lineage replay through slot 29,327,575 binds accounts hash `A286WmNJJ1r5F8G2cnBqykiDGbXgo7aphzJVJqX5ZwbR` | qualified first epoch-67 handoff |
| `29,327,576..29,371,188` | pinned Solana v1.2.24 pre-CPI worker | starts at the first recorded transaction whose outcome requires CPI to remain disabled; source-lineage replay through slot 29,371,187 binds accounts hash `6ubQSWsXQ8dEtxTkZwgpB8vEVj4nAsQcSGmu9usxVSGR` | qualified second epoch-67 handoff |
| `29,371,188..29,808,000` | pinned Solana v1.2.32 mainnet transition worker | generated state handoff after the last observed old-semantics transaction; reconstructs the CPI and vote-timestamp activation state | qualified by the terminal epoch-68 checkpoint |
| `29,808,000..39,744,000` | pinned Solana v1.2.32 worker | independently verified snapshot restart | diagnostic candidate for epochs 69-91 |
| `39,744,000..43,632,000` | pinned Solana v1.3.19 worker | independently verified snapshot restart; bounded extractor admits up to 131,072 members for the audited 104,267–106,520-member epoch-98 through epoch-100 snapshots | diagnostic candidate for epochs 92-100 |
| `43,632,000..55,728,000` | pinned Solana v1.3.23 worker | independently verified snapshot restart; restores and strictly validates mainnet's second hard-fork marker at slot 53,180,900; mainnet still accepted a 4,008-byte stake initialization at slot 55,686,407; the extractor remains byte-bounded while admitting the later snapshot's 131,072+ members; every cohort must match all canonical post-bootstrap roots | unqualified diagnostic candidate for epochs 101-128 |
| `55,728,000..56,592,000` | pinned Solana v1.4.17 worker | owns epoch 129's v1.4 feature boundary; reproduces the canonical successful vote at slot 55,728,002 where terminal v1.4.25 returns `SlotHashMismatch`, and the source-recorded BPF-loader custom error at slot 56,298,256 where v1.4.25 returns `ProgramFailedToComplete`; every canonical post-bootstrap root must still match before publication | source-status-selected, checkpoint-gated candidate for epochs 129-130 |
| `56,592,000..57,888,000` | pinned Solana v1.4.19 worker | complete source-status scans found 7, 1, and 12 legacy loader custom errors in epochs 131, 132, and 133 respectively, with no `ProgramFailedToComplete` status; v1.4.19 is the final upstream patch before that error contract changed; focused replay matched the canonical epoch-131 checkpoint at slot 56,705,196 | source-status-selected, bounded checkpoint-qualified candidate for epochs 131-133 |
| `57,888,000..63,936,000` | pinned Solana v1.4.25 worker | complete source-status scans across epochs 134-147 found no legacy loader custom errors; per-epoch `ProgramFailedToComplete` counts were `134:0, 135:0, 136:1, 137:0, 138:0, 139:1, 140:1314, 141:0, 142:0, 143:0, 144:2, 145:204, 146:3, 147:2`, confirming the later error contract throughout this range; independently verified snapshot restart carries that vocabulary through the shared stream protocol, and every cohort must still match all canonical post-bootstrap roots | unqualified diagnostic candidate for epochs 134-147 |
| `63,936,000..64,800,000` | pinned Solana v1.5.5 worker | exact v1.5.5 reproduces the trusted slot-63,948,761 accounts hash; normalizes v1.5 status variants for current plugins; every production cohort must still match all canonical post-bootstrap roots | bounded checkpoint-qualified candidate for epochs 148-149 |
| `64,800,000..66,528,000` | pinned Solana v1.5.6 worker | exact v1.5.6 reproduces epoch 150's first canonical vote and the trusted slot-64,807,725 accounts hash; normalizes v1.5 status variants for current plugins | bounded checkpoint-qualified candidate for epochs 150-153 |
| `66,528,000..66,960,000` | pinned Solana v1.5.8 worker | anchored at canonical snapshot slot 66,527,778, warms slots 66,527,779-66,527,999, reproduces the source-successful transaction at slot 66,528,004 that v1.5.6 rejects, and matches the canonical terminal accounts hash at slot 66,958,784; terminal v1.5.19 already diverges during warmup at slot 66,527,779 | source-status-selected, checkpoint-qualified candidate for epoch 154 |
| `66,960,000..68,140,177` | pinned Solana v1.5.6 worker | independently verified snapshot restart after the epoch-154 v1.5.8 envelope; source-exact replay binds the frozen slot-68,140,176 handoff to accounts hash `3TNSv7MXDB4GxhyRyHJcuBNeXaumYC8W8vu5WrwCXhhZ` | checkpoint-gated candidate for epochs 155 through the epoch-157 prefix |
| `68,140,177..68,256,000` | pinned Solana v1.5.8 worker | starts from the hash-bound v1.5.6 handoff immediately before the first transaction whose canonical `ProgramFailedToComplete` result differs from v1.5.6's `ComputationalBudgetExceeded`, then matches the canonical terminal accounts hash at slot 68,255,828 | source-status-selected, checkpoint-qualified candidate for the epoch-157 suffix |
| `68,256,000..75,168,000` | pinned Solana v1.5.6 worker | independently verified snapshot restart at the epoch-158 boundary; later cohorts remain independently checkpoint-gated | checkpoint-gated candidate for epochs 158-173 |
| `75,168,000..86,832,000` | pinned Solana v1.6.15 worker | independently verified snapshot restart; reproduces the v1.6 loader set, write-lock demotion, and expanded status vocabulary for current plugins; every cohort must match all canonical post-bootstrap roots | unqualified diagnostic candidate for epochs 174-200 |
| `86,832,000..87,264,000` | pinned Solana v1.6.16 worker | independently restarted epoch-201 canary; upstream execution source is byte-identical to v1.6.15, while the exact tag identity and canonical terminal root remain independently gated | unqualified diagnostic candidate for epoch 201 |
| `87,264,000..92,448,000` | pinned Solana v1.6.16 worker | focused-qualification-only search envelope; requires the explicit snapshot, checkpoint file, private output, `--verify`, and candidate opt-in; ordinary replay still fails closed. Preserved first-boundary attempts for epochs 209-213 reached their canonical bootstrap roots, then stopped at the former block-reward bound as their first observed failure (`146,527..=180,866` observed versus the current tested `262,144` limit), so those epochs remain diagnostic until fresh terminal checkpoints pass | diagnostic-only epochs 202-213 |
| `92,448,000..93,312,000` | pinned Solana v1.6.17 worker | independently restarted from canonical snapshot slot 92,447,542; that destination worker owns only the exact 457-slot pre-output warmup; exact upstream tag identity and terminal root at slot 93,311,535 remain checkpoint-gated | unqualified diagnostic candidate for epochs 214-215 |
| `93,312,000..100,656,000` | pinned Solana v1.6.20 worker | independently restarted from canonical snapshot slot 93,311,535; that destination worker owns only the exact 464-slot pre-output warmup and every cohort remains canonical-root-gated | unqualified diagnostic candidate for epochs 216-232 |
| `100,656,000..114,912,000` | pinned Solana v1.7.15 worker | independently restarted from canonical snapshot slot 100,655,540; owns the exact 459-slot pre-output warmup and remains canonical-root-gated | unqualified diagnostic candidate for epochs 233-265 |
| `114,912,000..130,464,000` | pinned Solana v1.8.11 worker | independently restarted from canonical snapshot slot 114,910,768; owns the exact 1,231-slot pre-output warmup; epochs 266-300 are the current publication range and epoch 301 is retained as a verification tail | unqualified diagnostic candidate for epochs 266-301 |
| `130,464,000..406,080,000` | none | unsupported; replay fails closed | unsupported |
| `406,080,000..` | in-process Agave v3 | independently verified modern snapshot bootstrap | verified |

Verified epochs 0-100 use 12 execution envelopes backed by 11 historical worker variants. The
11 runtime boundaries consist of one hash-bound canonical state handoff, two source-lineage-verified
epoch-67 handoffs, and eight independently verified snapshot restarts. The
v1.2.32 worker is used on both sides of the two specialized epoch-67 ranges. The v1.3.23,
v1.4.17, v1.4.19, v1.4.25, v1.5.5, v1.5.6, v1.5.8, v1.6.15, and v1.6.16
candidates add snapshot-isolated envelopes plus one hash-bound epoch-157
handoff. The v1.6.17 envelope is
independently bootstrapped across an unsupported gap rather than treated as an
adjacent runtime handoff. The
v1.4.19, v1.5.5, and v1.5.6 envelopes have each passed their first bounded post-boundary checkpoint;
every complete production cohort still requires all canonical roots before publication. The terminal
v1.5.19 worker remains registered only as an unassigned comparison candidate. The v1.6.16
worker advances one epoch at a time and is currently limited to the independently
checkpoint-gated epoch-201 canary for ordinary replay. A separate focused-qualification-only
planner may exercise the documented v1.6.16 diagnostic envelope through epoch 213, but it cannot
be used by normal epoch/range replay, archive reuse, or publication; every such diagnostic must
still reproduce its explicit terminal checkpoint before its evidence can advance the ordinary
registry. The independently restarted v1.6.17 worker is
limited to epochs 214-215; the preceding gap remains unsupported.
The independently restarted v1.6.20 worker is bounded to epochs 216-232.
Exact v1.7.13 remains an unassigned fallback comparison candidate. The primary
v1.7.15 envelope covers epochs 233-265, and v1.8.11 covers epochs 266-301 so
epoch 301 can serve as a verification tail beyond the current epoch-300
publication boundary.

Before a completed focused artifact is used as registry evidence, independently revalidate it with
`cargo run --release -p jetstreamer-node --bin jetstreamer-qualification-verify -- ...`. The
validator requires the exact epoch, output start, bootstrap and terminal slots, runtime profile,
worker SHA-256, and private root. It fully re-reads the Horizon source and bound segment manifest,
rejects non-private or multiply linked files, and fails if a canonical `.sha256` publication
sidecar exists.

Eleven execution interventions are explicitly recorded in addition to the ordinary
epoch-aligned pinned-worker snapshot restarts:

1. The behaviorally safe v1.0.7 to v1.0.8 state handoff at slot 619,849.
2. The fixed epoch-12 bootstrap at slot 5,183,736 and warmup through slot 5,183,999.
3. Reconstruction and validation of the epoch-30 hard-fork marker at slot 13,334,463.
4. The v1.2.32 to pre-CPI v1.2.24 state handoff at slot 29,327,576.
5. The return to v1.2.32 at slot 29,371,188 with CPI and vote-timestamp state reconstructed.
6. The v1.3.23 envelope extends through epoch 128 because mainnet still accepted 4,008-byte stake
   initializations through slot 55,725,865, while none were found after the epoch-129 feature boundary
   at slot 55,728,000; publication still requires every terminal root.
7. The v1.3.23 worker reconstructs mainnet's externally supplied hard-fork marker at slot
   53,180,900 when starting from its predecessor snapshot, and requires both persisted mainnet
   markers in later snapshots. The marker transforms the otherwise matching pre-extension bank hash
   `EZzqCDxdzWF4sak54hfhz9CMExLjgoh9qtKbMn8TdNLA` into the canonical vote witness
   `Fi4p8z3AkfsuGXZzQ4TD28N8QDNSWC7ccqAqTs2GPdPu`.
8. Epochs 129-130 use exact v1.4.17 because the first canonical epoch-129 vote at slot 55,728,002
   succeeds under v1.4.17 while terminal v1.4.25 rolls it back with `SlotHashMismatch`, and slot
   56,298,256 records BPF-loader custom error `0x0b9f0002` while v1.4.25 returns
   `ProgramFailedToComplete`; both epochs remain checkpoint-gated.
9. Epochs 131 through 133 use exact v1.4.19 because complete source-status scans found 7, 1, and
   12 legacy loader custom errors respectively and no `ProgramFailedToComplete` records. Exact
   v1.4.20 and later changed that mapping. Complete scans of epochs 134 through 147 found no
   legacy custom errors and found per-epoch `ProgramFailedToComplete` counts of
   `134:0, 135:0, 136:1, 137:0, 138:0, 139:1, 140:1314, 141:0, 142:0, 143:0, 144:2,
   145:204, 146:3, 147:2`, so v1.4.25 resumes from the independent epoch-134
   predecessor snapshot after qualification.
10. Epoch 154 uses exact v1.5.8 from the hash-bound slot-66,527,778 snapshot because v1.5.6 rejects
   a source-successful transaction at slot 66,528,004 while terminal v1.5.19 already disagrees at
   slot 66,527,779. It reproduces the canonical slot-66,958,784 terminal accounts hash
   `AVQPbbxxeLMaSyjVrcBC2fZpemNuGZhUF1CsQPBP16Ns`; v1.5.6 resumes at epoch 155, and every
   production replay remains independently checkpoint-gated.
11. Epoch 157 switches from v1.5.6 to v1.5.8 at slot 68,140,177. The preceding frozen v1.5.6 bank
    has accounts hash `3TNSv7MXDB4GxhyRyHJcuBNeXaumYC8W8vu5WrwCXhhZ`; v1.5.8 reproduces the
    source-recorded `ProgramFailedToComplete` result where v1.5.6 returns
    `ComputationalBudgetExceeded`. Epoch 158 restarts v1.5.6 from its independently verified
    predecessor snapshot. The v1.5.8 suffix reproduces the canonical slot-68,255,828 terminal
    accounts hash `AVreSVPd46H4WiihQ2pFUcRExPRQwL1MxhzQHVGky8bH`; every production replay remains
    independently checkpoint-gated.

Input repair is planned independently of execution and adds five more historical interventions:

| Slots or records | Input handling | Evidence gate |
|---|---|---|
| `0..4,258,776` | reconstruct status and fee from the selected runtime because the archive has no status frames | canonical account-state checkpoints |
| `4,258,776..43,632,000` | use runtime-associated status and fee because the early writer could pair source statuses with the wrong transactions | exact worker identity plus canonical account-state checkpoints |
| 1,084 exact records across 15 post-cutover slots | admit a missing status only for a checked-in `(slot, transaction index, signature)` match | hashed audit registry plus captured finalized RPC evidence |
| `67,681,336..67,824,000` | exclude pre-v1.5.13 bincode candidates while retaining guarded protobuf fallback; this resolves bytes that are valid but unequal under both v1.5.12 and v1.5.13 | CID-verified contiguous boundary audit: protobuf at the last present predecessor slot 67,681,331, absent slots 67,681,332-335, and five v1.5.13-only records at the first present successor slot 67,681,336; the exact dual-valid slot-67,711,948 fixture is checked in |
| 73,688 exact records across 88 present slots in `89,856,001..89,856,107` | replace the empty source frame with full finalized-block metadata only after slot, transaction index, signature order, and transaction signature all match | hash-bound full-epoch source audit plus 88 canonicalized finalized `getBlock` captures |

Using this narrower definition, which excludes ordinary version pinning, snapshot-format support,
PoH optimization, and AccountsDB maintenance, epochs 0-100 currently require eight distinct
slot- or record-specific compatibility interventions.

Snapshot selection and current-plugin streaming add three non-execution compatibility rules for the
later range. All are general invariants rather than slot-special-case branches:

| First observed at | General handling | Safety boundary |
|---|---|---|
| bootstrap slot `61,328,765` for epoch 142 | coalesce an hourly object and canonical root object only when slot, accounts hash, extension, byte length, CRC32C, and MD5 all match; prefer the root object | any digest or identity disagreement remains an ambiguous-preflight failure |
| misplaced root alias `122724642/snapshot-116819852-*` | ignore the alias only when the exact canonical root path exists and filename identity, byte length, CRC32C, and MD5 all match; prefer the canonical path | a missing canonical path, absent MD5, or any identity/content disagreement remains a fatal inventory error |
| late-anchor hourly objects, first observed around snapshots `110627202` and `116838296` | quarantine every object whose hourly anchor is later than its snapshot slot and record it in the preflight report; when a valid-anchor digest-identical copy exists, retain that copy | hourly objects are transport-only and never checkpoint trust anchors; if quarantine leaves no valid bootstrap, cohort planning fails closed |
| epoch-boundary slots `75,168,000`, `86,832,000`, `88,560,000`, `88,992,000`, `89,424,000`, and `90,288,004` | permit up to 262,144 pre-transaction runtime-direct account writes, 64 MiB of their data, and 262,144 reward records in one archive block (observed: 131,073 writes, more than 32 MiB of write data, and 155,661 rewards) | all three limits remain finite and the independent per-account, bucket, and decode-work limits remain enforced; wire encoding is unchanged |
| rent-collection slots `76,611,288`, `76,920,172`, `77,374,128`, `77,882,688`, `78,204,496`, and `80,017,516` | permit up to 8,192 post-transaction runtime-direct account writes and 16 MiB of their data in one archive block (observed: at least 4,097 writes at each earlier slot and 9,141,825 data bytes at slot 80,017,516) | both limits remain finite and the independent per-account, bucket, and cumulative decode-work limits remain enforced; the capacity is not encoded, so wire encoding is unchanged |

Archive-container repair is tracked separately from execution compatibility. Early independently
generated epoch 1-6 files can carry the old writer's zero placeholder as both their first bucket
PoH anchor and first block `parent_blockhash`, even though the preceding archive contains the
canonical parent. The re-encoding pipeline may replace only that initial placeholder: it requires
the preceding canonical slot and blockhash, recomputes the first block's PoH to its already stored
blockhash, rejects any other zero-parent block, writes a new file, and strictly decodes it before
publication. This does not alter transaction, account-update, entry, or runtime semantics and is
not counted as another execution intervention.

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

Epoch 67 requires more than one intra-epoch runtime handoff. Direct comparisons with Old Faithful
show that v1.2.32 still matches recorded execution at slots 29,188,719 and 29,189,576. The first
known source transaction requiring CPI to remain disabled is at slot 29,327,576, where v1.2.32
succeeds but Old Faithful records BPF-loader error `0x0b9f0002`. The hourly GCS snapshots previously
used to place the first handoff are not independent proof: their persisted slot-hash state disagrees
with successful votes in the Old Faithful stream. The candidate route therefore keeps v1.2.32
through slot 29,327,575, uses v1.2.24 through slot 29,371,187, and uses the v1.2.32 transition
runtime afterward. Corrected source-lineage replay binds the first handoff at slot 29,327,575
to accounts hash `A286WmNJJ1r5F8G2cnBqykiDGbXgo7aphzJVJqX5ZwbR` and the second at slot 29,371,187
to `6ubQSWsXQ8dEtxTkZwgpB8vEVj4nAsQcSGmu9usxVSGR`. The successor then matches the complete
terminal checkpoint at slot 29,807,999: bank hash
`439dySBi6LuxMisYQJbt6iPGgu8oSZe3uvvALBRMPXsD` and accounts hash
`5drX1gEHUyDxfnXEtotcfhZDUrwazrVMoH2A2icSsN3k`. Transactional cohort publication and both
checksum commits remain mandatory; neither superseded handoff hash is treated as proof.

Epoch 12 starts v1.0.23 from the canonical snapshot at slot 5,183,736 with legacy accounts hash
`BUqwiSm2GgH9ByKrBDF6epXHYK9RRh3vyZDKtUqtMXfR`. Production discovery binds both values, so a
later local epoch-11 snapshot cannot shorten the registered warmup. The worker warms slots 5,183,737
through 5,183,999 and begins Horizon output at slot 5,184,000. This boundary uses an independently
verified snapshot restart. The runtime handoff registry has no entry at 5,184,000.

Transaction metadata is an independent compatibility dimension. Old Faithful has no status frame
before slot `4,258,776`; the pinned historical runtime reconstructs transaction status there, while
the remaining metadata stays explicitly unavailable. At and after that slot, a missing status frame
is rejected unless it matches a checked-in audited `(slot, transaction index, signature)` anomaly.
The early audit table binds 1,084 known holes across 15 slots to finalized RPC status evidence. For 463
of them, finalized block RPC data also preserves the original balance vectors. For the other 621,
complete metadata was absent from the captured RPC evidence and the other fields remain absent or
at their defaults. The early source writer also stored execution results beside the wrong
transactions. Observed singleton entries and complete slots prove that the corruption is not bounded
to one entry or slot, so a source-status multiset is not valid execution evidence. For the epoch 0-100
compatibility scope, the pinned runtime is authoritative for each transaction's status and fee.
Audited holes only authorize the exact source omission and preserve any available ancillary metadata.
Later canonical account-state checkpoints remain the admission gate for that reconstructed execution.
Because the same writer defect could select another transaction's durable-nonce fee calculator,
protocol-v5 workers also return the runtime-associated fee. Replay uses that fee for source-present
records and audited missing-frame exceptions in the affected writer era. This policy changes during
epoch 9 without changing the execution runtime. Separately, the epoch-208 archive has a bounded
prefix gap covering every transaction in 88 present slots from `89,856,001` through `89,856,106`.
A full-epoch audit found exactly 73,688 empty frames, and finalized block captures reproduce the
same ordered signatures with non-null metadata for every record. Those records remain source-exact:
replay admits only the checked-in `(slot, transaction index, signature)` identity, carries the full
canonical metadata into Horizon, and still requires the replayed status to match it individually.

Generated Horizon archives record the selected runtime identity and admission level, genesis,
bootstrap state, output slot range, and transaction-metadata policy in a versioned provenance
envelope. Range resume skips a completed archive only when that provenance matches the current
slot-derived plan.

One content-addressed rule is narrower than normal range resume. The epoch-11 archive produced at
revision `0a8ec77094ddf2b21ff22e6f4a55fef836f8f2c6` may proceed from private crash recovery to
the secure publisher, or be reused after publication, only when its complete provenance matches
the audited v1.0.14 run. `JETSTREAMER_HISTORICAL_WORKER_V1_0_14` must resolve to
`/home/sol/.jetstreamer-private/deploy-epoch11-full-v1014-20260910-v3/jetstreamer-historical-worker-v1-0-14`,
and that executable must hash to the recorded SHA-256. The archive must then pass its full decode,
semantic and PoH-chain checks, exact 2,765,674,556-byte length, and exact audited SHA-256. The old
producer profile is not part of the general compatibility allowlist, so no other artifact from that
profile gains admission.

Root snapshots are the only trust anchors for historical verification. The read-only inventory
preflight groups an epoch without a root checkpoint with the first later epoch that has one, as
long as the runtime descriptor does not change. Hourly snapshots may shorten bootstrap work for
an independent single-epoch cohort, but they never satisfy a checkpoint or anchor a root-gap
cohort. The preflight manifest records the root object's generation, CRC32C, size, slot, and
accounts hash. It also records the full cohort range and terminal root checkpoints.

Epochs 17 through 19 are one such cohort. They start from root slot 7,343,776 and reach root
checkpoints in epoch 19. First save the complete preflight report in an owner-controlled file and
record its printed `manifest_fingerprint` through the review channel. The fingerprint supplied to
the replay must be the independently reviewed value, not a value copied from a newly downloaded
report. Run the cohort with:

```bash
JETSTREAMER_ALLOW_CANDIDATE_RUNTIME=1 \
  cargo run --release -p jetstreamer-node --bin jetstreamer-node -- \
  17-19 /path/to/output --verify --root-checkpoint-cohort \
  --cohort-manifest=/secure/path/preflight-1-100.json \
  --cohort-manifest-fingerprint=sha256:0f972577503068f9a3d74c2a427f20363da0e5fa3b727f3eb5bcba220cf7e976
```

This mode keeps one historical worker and one root-only verifier alive across every epoch
boundary. It downloads the manifest's immutable GCS generation, verifies the recorded size and
CRC32C, and holds the measured inode and SHA-256 digest through historical worker startup. The
worker makes its own digest-checked private copy before decoding. The bootstrap normally comes
from a root object. A singleton cohort may instead use the manifest's exact hourly transport
object; its anchor, snapshot slot, path, and generation remain fingerprint-bound. Multi-epoch
cohorts still require a root bootstrap, and hourly objects are never admitted as checkpoint
expectations. Checkpoint expectations come
only from the same fingerprinted manifest; replay does not replace them with a later bucket
listing. Each epoch archive is written below the owner-only private run directory. The process
checks the exact checkpoint handoff, the next archive's initial PoH and parent anchors,
cross-bucket continuity, provenance, full archive decode, and archive digest while publication is
closed. Each terminal checkpoint must match the archive's final present block, so trailing skipped
slots do not create a false epoch-boundary requirement. A mismatch, missing final-epoch root,
infrastructure failure, or interruption leaves every generated archive and its recognizable run
state private and creates no checksum. The final root and every archive must pass before the
transactional publisher can expose any cohort member. Publication capability is tested once before
replay begins, then the ordered archive set is committed through one durable batch journal. While
that journal or its completed outcome exists, archive reuse, checksum repair, and another batch all
fail closed. After commit, Jetstreamer verifies the exact final inode set and checksums, writes and
fsyncs an owner-only receipt below the destination's private evidence scope, and acknowledges the
exact transaction ID before removing the private run. The receipt records the reviewed manifest
fingerprint, canonical destination identity, ordered epochs, prior namespace identities, new archive
digests, and final identities.

On startup, a top-level producer recovers or observes any pending batch before inspecting an
archive. It writes the same durable private receipt for a committed result, or a rollback receipt
containing restored identities and the still-private validated archive digest. It then acknowledges
that outcome and exits unconditionally. A clean subsequent invocation is required to resume. If
the journal, destination, staged archive, or receipt evidence has changed, recovery makes no further
mutation and the destination remains closed for operator review.

Use `scripts/preflight_gcs_snapshots.py` before scheduling early epochs. Its schema-v2
`verification_cohorts` array supplies the ranges accepted by this mode and fails if a root gap
would cross a runtime boundary, select a non-historical runtime, or extend beyond the requested
range.

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

### Adaptive historical sweeps

`scripts/adaptive_root_cohort_sweep.py` runs reviewed verification cohorts in separate,
pre-provisioned lanes. It starts in planning mode. Execution is intended for a detached,
root-owned service using a root-owned, non-writable deployment tree and owner-only controller
state under root-controlled ancestry. The controller derives every work boundary from the
fingerprinted preflight manifest and will not split a verification cohort.

Concurrency begins at the configured floor and increases gradually when CPU, memory, and disk
headroom allow it. The optional `--memory-admission-gib` keeps the measured non-reclaimable
per-worker reservation separate from the larger `MemoryMax` safety ceiling; it defaults to that
ceiling unless an operator supplies measured evidence. Disk admission reserves a fixed safety
margin and conservatively budgets the full configured private-storage allowance for every live
producer before starting another one;
it never terminates live work merely because available space falls below that estimate. By
default, every producer runs as the unprivileged `sol:horizon` identity in a resource-bounded
systemd unit. Hosts with a different dedicated identity must pass `--producer-user` and
`--archive-group`; the controller resolves that account's home directory and applies it to the
service environment, home execution barrier, sensitive-path isolation, ownership checks, live-unit
adoption policy, sealed configuration digest, and printed plan. A plan may
configure up to 32 lanes, but admission is still capped by the number of provisioned lanes and by
the live CPU, memory-admission, and disk calculations. The final global claim check and launch
are serialized through the bound public-directory lock, so controllers for different runtime eras
cannot consume the same capacity slot concurrently. A queue's local lane count limits only how many
jobs that queue can add; it does not become an accidental ceiling on the global producer count.
Producer services deliberately use a minimal PATH. If `gcloud` is installed outside
`/usr/bin`—for example by Homebrew—pass its absolute path with `--gcloud-bin`. The controller
resolves and validates that executable, binds its SHA-256 into the sealed configuration, adds only
its parent directory to the producer PATH, and requires the same environment when adopting a live
unit.
Producer namespace filtering uses a reload-stable cgroup-only allow-list; user namespaces and every
other namespace type remain denied,
and the empty capability set prevents the unprivileged worker from using the nominal cgroup option.
The controller will adopt existing work only when the process identity, arguments, cgroup, lane,
runtime, manifest, and complete sandbox configuration match the sealed plan. Overlapping epoch or
lane claims stop scheduling.

Completed lanes are imported one at a time through the same Rust recovery and transactional
publication path used by manual cohort runs. Before launch, the root controller binds the source
receipt, its gate context, and independently checked archive digests into durable state. A retained
systemd unit supplies durable importer exit status across controller restarts. After a successful
exit, the controller requires the public receipt to reproduce that evidence, rehashes every public
archive, and records a root-owned completion attestation before unloading the unit. Directory
inodes are bound into the sealed configuration and rechecked while the controller runs. A committed
import consumes the source checksum sidecars so the lane can be reused. Publication journals,
partial namespaces, changed receipts, and failed attempts remain in place for recovery or operator
review; the controller does not delete them.

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
