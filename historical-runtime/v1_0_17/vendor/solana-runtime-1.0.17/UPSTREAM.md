# Vendored source record

- Repository: `https://github.com/solana-labs/solana`
- Tag/version: `v1.0.17`
- Commit: `cfc7b22c4c9094d09fc969247bfe60a154027d84`
- Imported subtree: `runtime/`
- Upstream `runtime/` Git tree: `fdcedcf66fb3fe264db6029b887f4e62f413f8c1`
- License blob: `a7d55d2d016bf6c7d0a991eb0472dc9e88c06ae2`
- Required compiler/target: `rustc 1.42.0 (b8cedc004 2020-03-09)`,
  `x86_64-unknown-linux-gnu`

The compiler pin comes from upstream `ci/rust-version.sh` at the recorded
commit (file SHA-256
`d23401945c116b80819c6b667f1c3e4a225f9245671a1ca7195a2ab94653b4de`).
The target is the Linux release target selected by upstream
`ci/publish-tarball.sh` (file SHA-256
`c66bc094d62d41b76865674bcdec10367ffbee41f07008dd7f4f67ea88d2aa03`).

To re-check the source identity in an upstream checkout:

```text
git rev-parse v1.0.17^{} v1.0.17^{}:runtime v1.0.17^{}:LICENSE
```

Jetstreamer changes are intentionally narrow:

- monorepo-relative Solana dependencies in `Cargo.toml` point to the exact
  commit above;
- three test-genesis macro calls in `src/genesis_utils.rs` are expanded to
  their identical name/id tuples so the split dependency graph compiles;
- snapshot bincode decoding rejects any single attacker-declared variable
  field larger than 16 MiB before allocating it;
- `src/accounts_db.rs` provides read-only, owned, write-version-ordered access
  to physical account writes for the IPC adapter; and
- zero- and one-storage `scan_account_storage` calls run directly instead of
  entering the old Rayon pool, whose workers cannot add parallelism there.

The decoder accepts the same valid bincode representation while adding a
per-field allocation bound. The write adapter observes AppendVec contents and
the existing atomic write counter. The scan fast path applies the same closure
to the same storage in the same order as the parallel iterator's single-item
path. None of these changes alter transaction execution, storage, account
indexing, hashing, or serialization.

`LICENSE-APACHE` is the upstream repository license copied with the subtree.
