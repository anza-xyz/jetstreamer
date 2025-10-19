# Jetstreamer

## Historical Info

| Epoch | Slot        | Comment |
|-------|-------------|--------------------------------------------------|
| 0-156 | 0-?         | Incompatible with modern Geyser plugins |
| 157+  | ?           | Compatible with modern Geyser plugins |
| 0-449 | 0-194184610 | CU tracking not available (reported as 0)        |
| 450+  | 194184611+  | CU tracking available                            |

## Configuration

### Environment variables

| Variable | Default | Effect |
|----------|---------|--------|
| `JETSTREAMER_CLICKHOUSE_DSN` | `http://localhost:8123` | HTTP(S) DSN passed to the embedded plugin runner for ClickHouse writes. Set this to point at a remote ClickHouse deployment if you are not running the bundled instance locally. |
| `JETSTREAMER_CLICKHOUSE_MODE` | `auto` | Governs ClickHouse integration. `auto` enables output and only spawns the helper for local DSNs, `remote` enables output without spawning the helper, `local` always requests the helper, and `off` disables ClickHouse entirely. |
| `JETSTREAMER_THREADS` | `1` | Number of firehose ingestion threads to spawn. Increase this carefully based on available CPU and downstream capacity. |

Notes:

- The helper only starts when the mode allows it (`auto` or `local`) *and* the DSN points to `localhost` or `127.0.0.1`. Modes `remote` and `off` skip the helper regardless of DSN.
- Legacy variables `JETSTREAMER_NO_CLICKHOUSE` and `JETSTREAMER_SPAWN_CLICKHOUSE` are still understood for backward compatibility but `JETSTREAMER_CLICKHOUSE_MODE` should be preferred.
