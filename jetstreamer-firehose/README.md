# jetstreamer-firehose

A utility that allows replaying Solana blocks (even all the way back to genesis!) over a geyser
plugin or the Jetstreamer plugin runner.

Based on the demo provided by the Old Faithful project in
https://github.com/rpcpool/yellowstone-faithful/tree/main/geyser-plugin-runner

## Configuration

### Environment variables

| Variable | Default | Effect |
|----------|---------|--------|
| `JETSTREAMER_COMPACT_INDEX_BASE_URL` | `https://files.old-faithful.net` | Sets the base URL used for downloading compact index CAR artifacts. Override this when mirroring Old Faithful data to your own storage. |
| `JETSTREAMER_NETWORK` | `mainnet` | Network identifier appended to the index filenames. Use this to point the replay engine at alternative network snapshots (e.g. `testnet`). |

Notes:

- `JETSTREAMER_COMPACT_INDEX_BASE_URL` accepts a full HTTP(S) URL and is resolved relative to per-epoch paths (for example `https://domain/450/...`).
- Changing `JETSTREAMER_NETWORK` also alters the in-memory cache namespace, so you can switch networks without cross-contaminating cached offsets.
