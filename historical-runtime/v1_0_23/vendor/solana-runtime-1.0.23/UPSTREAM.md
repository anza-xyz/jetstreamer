# Vendored source record

- Repository: `https://github.com/solana-labs/solana`
- Tag/version: `v1.0.23`
- Commit: `825c0e2b6e39ae67431ed0a8282260ad3914c87a`
- Imported subtree: `runtime/`
- Upstream `runtime/` Git tree: `6b8b60c440287ddbd44f59a42c29d2ddce22c082`
- License blob: `a7d55d2d016bf6c7d0a991eb0472dc9e88c06ae2`
- Required compiler/target: `rustc 1.43.0 (4fb7144ed 2020-04-20)`,
  `x86_64-unknown-linux-gnu`

The compiler pin comes from upstream `ci/rust-version.sh` at the recorded
commit (file SHA-256
`2593fff8e9c5ee4a4cbe9d2fd285575a6b173d0c598717919d16ce956f54522a`).
The target is the Linux release target selected by upstream
`ci/publish-tarball.sh` (file SHA-256
`610a349def16124e374f665d6bdadce7ca626cd0a82304b8f8591b99887986fa`).

To re-check the source identity in an upstream checkout:

```text
git rev-parse v1.0.23^{} v1.0.23^{}:runtime v1.0.23^{}:LICENSE
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
