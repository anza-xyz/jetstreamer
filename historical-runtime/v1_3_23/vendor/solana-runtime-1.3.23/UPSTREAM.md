# Vendored source record

- Repository: `https://github.com/solana-labs/solana`
- Tag/version: `v1.3.23`
- Commit: `ab235b8160f1c76e5066eee52d62d976d12f42f1`
- Imported subtree: `runtime/`
- Upstream `runtime/` Git tree: `8b3d979efe5c5c9cd8a8d16f1231974b185a62c8`
- License blob: `285adee28490f5a79e92b3e860e1af2f9b865f07`
- Required compiler/target: `rustc 1.45.1 (c367798cf 2020-07-26)`,
  `x86_64-unknown-linux-gnu`

The compiler pin is the `stable_version` in upstream `ci/rust-version.sh` at
the recorded commit (file SHA-256
`48ba62e77a630ec1baf96be44b46363d973ea9a9d8fa3b38ccb1985cbdbd860a`).
Upstream's `runtime/build.rs` symlink is materialized with the byte-identical
contents of its `sdk/build.rs` target (SHA-256
`974c03023a0a15b75323d49df690875c9d1682cafd4754d001e9d7a49996ef0e`).

To re-check the source identity in an upstream checkout:

```text
git rev-parse v1.3.23^{} v1.3.23^{}:runtime v1.3.23^{}:LICENSE
```

Jetstreamer changes are intentionally narrow: monorepo-relative dependencies
are pinned to the exact commit; snapshot decoding has aggregate and per-field
allocation limits and exposes the exact reconstruction entrypoint; AccountsDB
exposes an owned, write-version-ordered read view; and zero/one-storage scans
avoid waking a Rayon pool that cannot add parallelism. These changes do not
alter transaction execution, account hashing, or snapshot serialization.

`LICENSE-APACHE` is copied from the recorded upstream commit.
