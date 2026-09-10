# Vendored source record

- Repository: `https://github.com/solana-labs/solana`
- Tag/version: `v1.3.19`
- Commit: `15a49d75086f95573ad319b22e4843639bdf2169`
- Imported subtree: `runtime/`
- Upstream `runtime/` Git tree: `87ed6430590918d051f0e836362e8d02a153c578`
- License blob: `285adee28490f5a79e92b3e860e1af2f9b865f07`
- Required compiler/target: `rustc 1.45.1 (c367798cf 2020-07-26)`,
  `x86_64-unknown-linux-gnu`

The compiler pin is the `stable_version` in upstream `ci/rust-version.sh` at
the recorded commit (file SHA-256
`48ba62e77a630ec1baf96be44b46363d973ea9a9d8fa3b38ccb1985cbdbd860a`).
The target is the Linux release target selected by upstream
`ci/publish-tarball.sh` (file SHA-256
`c8ef4a268f8544e19cebaeba418c5fccd470b234636a08ce3c7c831b722d78b6`).

To re-check the source identity in an upstream checkout:

```text
git rev-parse v1.3.19^{} v1.3.19^{}:runtime v1.3.19^{}:LICENSE
```

Jetstreamer changes to the imported subtree are intentionally narrow:

- upstream's `runtime/build.rs` symlink is materialized with the byte-identical
  contents of its `sdk/build.rs` target (SHA-256
  `974c03023a0a15b75323d49df690875c9d1682cafd4754d001e9d7a49996ef0e`)
  because the isolated vendor does not include the sibling `sdk/` directory;
- monorepo-relative Solana dependencies in `Cargo.toml` point to the exact
  commit above;
- snapshot bincode decoding rejects any single attacker-declared variable
  field larger than 16 MiB before allocating it, and the exact decoder and
  reconstruction entrypoint are exposed to the isolated IPC adapter;
- `src/accounts_db.rs` provides read-only, owned, write-version-ordered access
  to physical account writes for the IPC adapter; and
- zero- and one-storage `scan_account_storage` calls run directly instead of
  entering the old Rayon pool, whose workers cannot add parallelism there.

The bounded decoder preserves the exact v1.3.19 fixed-integer/trailing-byte
snapshot representation. The write adapter observes AppendVec contents and
the existing atomic write counter. The scan fast path applies the same closure
to the same storage in the same order as the parallel iterator's single-item
path. None of these changes alter transaction execution, storage, account
indexing, hashing, or serialization.

`LICENSE-APACHE` is the upstream repository license copied from the recorded
commit.
