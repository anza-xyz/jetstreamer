# jetstreamer-firehose

A utility that allows replaying Solana blocks (even all the way back to genesis!) over a geyser
plugin or the Jetstreamer plugin runner.

Based on the demo provided by the Old Faithful project in
https://github.com/rpcpool/yellowstone-faithful/tree/main/geyser-plugin-runner

## Configuration

### Environment variables

- `JETSTREAMER_ARCHIVE_BACKEND` (default `http`): set to `s3` to force the new S3 transport
  even when the base URLs still point at `https://`.
- `JETSTREAMER_HTTP_BASE_URL` (default `https://files.old-faithful.net`): base URL or
  `s3://bucket/prefix` for CAR data.
- `JETSTREAMER_COMPACT_INDEX_BASE_URL` (default `https://files.old-faithful.net`): override for
  compact index artifacts; also accepts `s3://` URIs when mirroring Old Faithful into private
  storage.
- `JETSTREAMER_ARCHIVE_BASE`: shortcut that applies to both CARs and indexes when the more
  specific knobs are unset.
- `JETSTREAMER_S3_BUCKET`, `JETSTREAMER_S3_PREFIX`, `JETSTREAMER_S3_INDEX_PREFIX`,
  `JETSTREAMER_S3_REGION`, `JETSTREAMER_S3_ENDPOINT`, `JETSTREAMER_S3_ACCESS_KEY`,
  `JETSTREAMER_S3_SECRET_KEY`, `JETSTREAMER_S3_SESSION_TOKEN`: credentials and addressing
  details used when the S3 backend is active.
- `JETSTREAMER_NETWORK` (default `mainnet`): network identifier appended to index filenames so
  you can point the replay engine at other clusters (for example `testnet`).
- `JETSTREAMER_NETWORK_CAPACITY_MB` (default `1000`): assumed network throughput in megabytes
  per second when sizing the firehose thread pool. Increase or decrease to match your host's
  effective bandwidth.

Notes:

- `JETSTREAMER_HTTP_BASE_URL` and `JETSTREAMER_COMPACT_INDEX_BASE_URL` accept both full HTTP(S)
  URLs and `s3://bucket/...` URIs; the latter automatically activates the S3 transport layer.
- Changing `JETSTREAMER_NETWORK` also alters the in-memory cache namespace, so you can switch
  networks without cross-contaminating cached offsets.
