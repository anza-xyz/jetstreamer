# Vendored source record

- Repository: `https://github.com/solana-labs/solana`
- Tag/version: `v1.4.25`
- Commit: `893cc7647248a3536fb6e6d0b5e51c71446b862d`
- Imported subtree: `runtime/`
- Upstream `runtime/` Git tree: `89432cfeda4222bdb14b2c352a01115e4f1b440e`
- License blob: `285adee28490f5a79e92b3e860e1af2f9b865f07`
- Required compiler/target: `rustc 1.46.0 (04488afe3 2020-08-24)`,
  `x86_64-unknown-linux-gnu`

The compiler pin is the `stable_version` in upstream `ci/rust-version.sh` at
the recorded commit (file SHA-256
`a57fe12eac3cf372ed19bc662875aaeec4d0a8b04e3e3ddf7a85c7e252e6652e`).
Upstream's `runtime/build.rs` symlink is materialized with the byte-identical
contents of its `frozen-abi/build.rs` target (SHA-256
`974c03023a0a15b75323d49df690875c9d1682cafd4754d001e9d7a49996ef0e`).

To re-check the source identity in an upstream checkout:

```text
git rev-parse v1.4.25^{} v1.4.25^{}:runtime v1.4.25^{}:LICENSE
```

Jetstreamer changes are limited to exact Git dependency pins, bounded snapshot
decoding, an owned write-version-ordered account view, and the zero/one-storage
scan fast path. These changes do not alter transaction execution, account
hashing, or snapshot serialization.

`LICENSE-APACHE` is copied from the recorded upstream commit.
