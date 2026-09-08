# Vendored source record

- Repository: `https://github.com/solana-labs/solana`
- Tag: `v1.0.24`
- Commit: `a93915f1bddb73480f86fc09f487315ae191897d`
- Imported subtree: `runtime/`
- Required compiler/target: `rustc 1.43.0 (4fb7144ed 2020-04-20)`,
  `x86_64-unknown-linux-gnu`

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
