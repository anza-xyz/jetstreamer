# jetstreamer-firehose

A utility that allows replaying Solana blocks (even all the way back to genesis!) over a geyser
plugin or the Jetstreamer plugin runner.

Based on the demo provided by the Old Faithful project in
https://github.com/rpcpool/yellowstone-faithful/tree/main/geyser-plugin-runner

## Configuration

### Environment variables

- `JETSTREAMER_COMPACT_INDEX_BASE_URL` (default `https://files.old-faithful.net`): base URL for
  downloading compact index CAR artifacts. Override this when mirroring Old Faithful data to
  your own storage.
- `JETSTREAMER_NETWORK` (default `mainnet`): network identifier appended to index filenames so
  you can point the replay engine at other clusters (for example `testnet`).
- `JETSTREAMER_NETWORK_CAPACITY_MB` (default `1000`): assumed network throughput in megabytes
  per second when sizing the firehose thread pool. Increase or decrease to match your host's
  effective bandwidth.

Notes:

- `JETSTREAMER_COMPACT_INDEX_BASE_URL` accepts a full HTTP(S) URL and is resolved relative to
  per-epoch paths (for example `https://domain/450/...`).
- Changing `JETSTREAMER_NETWORK` also alters the in-memory cache namespace, so you can switch
  networks without cross-contaminating cached offsets.
