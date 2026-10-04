# Vendored source record

- Repository: `https://github.com/solana-labs/solana`
- Tag/version: `v1.6.20`
- Commit: `77bdb45d4af61fc687161ec06cc0178ff79726d5`
- Imported subtree: `runtime/`
- Upstream `runtime/` Git tree: `3b48d6a62b264765b82b9c077cba1db0f454de9f`
- License blob: `285adee28490f5a79e92b3e860e1af2f9b865f07`
- Required compiler/target: `rustc 1.51.0 (2fd73fabe 2021-03-23)`,
  `x86_64-unknown-linux-gnu`

The compiler pin is the `stable_version` in upstream `ci/rust-version.sh` at
the recorded commit (file SHA-256
`e33be867cf2d65d72176331246a22f17e802c857fa5b682a5c65e406abc7b396`).
Upstream's `runtime/build.rs` symlink is materialized with the byte-identical
contents of its `frozen-abi/build.rs` target (SHA-256
`974c03023a0a15b75323d49df690875c9d1682cafd4754d001e9d7a49996ef0e`).

To re-check the source identity in an upstream checkout:

```text
git rev-parse v1.6.20^{} v1.6.20^{}:runtime v1.6.20^{}:LICENSE
```

Jetstreamer changes are limited to exact Git dependency pins, bounded snapshot
decoding, strict consumption of the unpacked AppendVec map, an owned
write-version-ordered account view, and the zero/one-storage scan fast path.
The isolated worker disables the account cache so every physical replay write
retains its historical global write-version ordering. These changes do not
alter transaction execution, account hashing, or snapshot serialization.

`LICENSE-APACHE` is copied from the recorded upstream commit.
