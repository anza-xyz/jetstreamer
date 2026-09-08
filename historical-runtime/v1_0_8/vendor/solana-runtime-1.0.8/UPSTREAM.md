# Vendored source record

- Repository: `https://github.com/solana-labs/solana`
- Tag: `v1.0.8`
- Commit: `2a617f2d07f714918891f2b479d1cb1c324f0365`
- Imported subtree: `runtime/`
- Required compiler/target: `rustc 1.42.0 (b8cedc004 2020-03-09)`,
  `x86_64-unknown-linux-gnu`

The compiler pin comes from upstream `ci/rust-version.sh` at the recorded
commit. The target is the Linux release target selected by upstream
`ci/publish-tarball.sh` at that commit.

Jetstreamer changes are intentionally narrow:

- monorepo-relative Solana dependencies in `Cargo.toml` point to the exact
  commit above;
- three test-genesis macro calls in `src/genesis_utils.rs` are expanded to
  their identical name/id tuples so the split dependency graph compiles; and
- `src/accounts_db.rs` provides read-only, owned, write-version-ordered access
  to physical account writes for the IPC adapter.

The last change observes AppendVec contents and the existing atomic write
counter. It does not participate in transaction execution, storage, account
indexing, or hashing.

`LICENSE-APACHE` is the upstream repository license copied with the subtree.
