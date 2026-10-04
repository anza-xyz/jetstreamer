# Vendored source record

- Repository: `https://github.com/solana-labs/solana`
- Tag/version: `v1.7.15`
- Commit: `4892eb4e1ad278d5249b6cda8983f88effb3e98b`
- Imported subtree: `runtime/`
- Upstream `runtime/` Git tree: `9370e13aa8b093fffa908ad3a1388fe2e85c2100`
- License blob: `285adee28490f5a79e92b3e860e1af2f9b865f07`
- Required compiler/target: `rustc 1.52.1 (9bc8c42bb 2021-05-09)`,
  `x86_64-unknown-linux-gnu`

The compiler pin is the `stable_version` in upstream `ci/rust-version.sh` at
the recorded commit (file SHA-256
`f4ddc50717c22c8cb65eae66905daaa7fd8c1cfc3d9df7df31f2c080b0038874`).
Upstream's `runtime/build.rs` symlink is materialized with the byte-identical
contents of its `frozen-abi/build.rs` target (SHA-256
`974c03023a0a15b75323d49df690875c9d1682cafd4754d001e9d7a49996ef0e`).

To re-check the source identity in an upstream checkout:

```text
git rev-parse v1.7.15^{} v1.7.15^{}:runtime v1.7.15^{}:LICENSE
```

Jetstreamer changes are limited to exact Git dependency pins, bounded snapshot
decoding, strict consumption of the unpacked AppendVec map, an owned
write-version-ordered account view, and the zero/one-storage scan fast path.
The isolated worker disables the account cache so every physical replay write
retains its historical global write-version ordering. These changes do not
alter transaction execution, account hashing, or snapshot serialization.

`LICENSE-APACHE` is copied from the recorded upstream commit.
