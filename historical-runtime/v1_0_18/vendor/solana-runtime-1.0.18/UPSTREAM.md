# Vendored source record

- Repository: `https://github.com/solana-labs/solana`
- Tag/version: `v1.0.18`
- Commit: `f26f18d29d650d06f5c5b7a4eb625622a999ea66`
- Imported subtree: `runtime/`
- Imported subtree tree: `b5e2bdbaf7e2bcb1859f427318b99b52d592e2da`
- Required compiler/target: `rustc 1.42.0 (b8cedc004 2020-03-09)`,
  `x86_64-unknown-linux-gnu`

The compiler pin comes from upstream `ci/rust-version.sh` at the recorded
commit. The target is the Linux release target selected by upstream
`ci/publish-tarball.sh` at that commit.

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
