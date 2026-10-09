# Vendored source record

- Repository: `https://github.com/solana-labs/solana`
- Tag/version: `v1.5.8`
- Commit: `460c643f8e549d22c09a9cddc3b0f5be9c7b2204`
- Imported subtree: `runtime/`
- Upstream `runtime/` Git tree: `55a91febf62f6331e76d754a7b07d8e2f9d15b67`
- License blob: `285adee28490f5a79e92b3e860e1af2f9b865f07`
- Required compiler/target: `rustc 1.49.0 (e1884a8e3 2020-12-29)`,
  `x86_64-unknown-linux-gnu`

The compiler pin is the `stable_version` in upstream `ci/rust-version.sh` at
the recorded commit. Upstream's `runtime/build.rs` symlink is materialized
with the byte-identical contents of its `frozen-abi/build.rs` target (SHA-256
`974c03023a0a15b75323d49df690875c9d1682cafd4754d001e9d7a49996ef0e`).

To re-check the source identity in an upstream checkout:

```text
git rev-parse v1.5.8^{} v1.5.8^{}:runtime v1.5.8^{}:LICENSE
```

Jetstreamer changes are limited to exact Git dependency pins, bounded snapshot
decoding, an owned write-version-ordered account view, and the zero/one-storage
scan fast path. The isolated worker disables the account cache so every write
retains the historical global write-version ordering. These changes do not
alter transaction execution, account hashing, or snapshot serialization.

`LICENSE-APACHE` is copied from the recorded upstream commit.
