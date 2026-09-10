# Vendored source record

- Repository: `https://github.com/solana-labs/solana`
- Tag/version: `v1.2.32`
- Commit: `8c989da68342918f1717c60aa60fdfab7d1e676e`
- Imported subtree: `runtime/`
- Upstream `runtime/` Git tree: `e9f477ec87fe9328cebf0a55eaa4d7eed48131c2`
- License blob: `a7d55d2d016bf6c7d0a991eb0472dc9e88c06ae2`
- Required compiler/target: `rustc 1.43.0 (4fb7144ed 2020-04-20)`,
  `x86_64-unknown-linux-gnu`

The compiler pin comes from upstream `ci/rust-version.sh` at the recorded
commit (file SHA-256
`2593fff8e9c5ee4a4cbe9d2fd285575a6b173d0c598717919d16ce956f54522a`).
The target is the Linux release target selected by upstream
`ci/publish-tarball.sh` (file SHA-256
`f99228e25d40e6ae813e6eb47e837a6af2a7f6c9cdaa0bbfa704784137a298e0`).

To re-check the source identity in an upstream checkout:

```text
git rev-parse v1.2.32^{} v1.2.32^{}:runtime v1.2.32^{}:LICENSE
```

Jetstreamer changes are intentionally narrow:

- monorepo-relative Solana dependencies in `Cargo.toml` point to the exact
  commit above;
- snapshot bincode decoding rejects any single attacker-declared variable
  field larger than 16 MiB before allocating it;
- `src/accounts_db.rs` provides read-only, owned, write-version-ordered access
  to physical account writes for the IPC adapter;
- zero- and one-storage `scan_account_storage` calls run directly instead of
  entering the old Rayon pool, whose workers cannot add parallelism there; and
- `Bank::add_builtin_loader` restores the serde-skipped, exact static BPF
  loader entrypoint without consulting a deployment-adjacent shared object;
  and
- read-only `Bank::cross_program_support` exposes the serde-skipped CPI gate
  so the worker can regression-test exact epoch-63 restoration semantics.

The decoder accepts the same valid bincode representation while adding a
per-field allocation bound. The write adapter observes AppendVec contents and
the existing atomic write counter. The scan fast path applies the same closure
to the same storage in the same order as the parallel iterator's single-item
path. None of these changes alter transaction execution, storage, account
indexing, hashing, or serialization.

`LICENSE-APACHE` is the upstream repository license copied with the subtree.
