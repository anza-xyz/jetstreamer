# Vendored source record

- Repository: `https://github.com/solana-labs/solana`
- Tag: `v1.0.7`
- Commit: `57abc370fa39e42e8fb84145a30395ddcf891692`
- Imported subtree: `runtime/`
- Required compiler/target: `rustc 1.42.0 (b8cedc004 2020-03-09)`,
  `x86_64-unknown-linux-gnu`

Jetstreamer changes are intentionally narrow:

- monorepo-relative Solana dependencies in `Cargo.toml` point to the exact
  commit above;
- three test-genesis macro calls in `src/genesis_utils.rs` are expanded to
  their identical name/id tuples so the split dependency graph compiles; and
- `src/accounts_db.rs` provides read-only, owned, write-version-ordered access
  to physical account writes for the IPC adapter; and
- `AccountStorageEntry::snapshot_file_layout` exposes the existing AppendVec
  written length and mapped file length to the snapshot packager; and
- zero- and one-storage `scan_account_storage` calls run directly instead of
  entering the old Rayon pool, whose workers cannot add parallelism there.

The last two changes only observe AppendVec metadata/contents and the existing
atomic write counter. They do not participate in transaction execution,
storage, account indexing, or hashing. The layout accessor lets the measured
v1.0.7 worker reproduce GNU sparse archive semantics without invoking an
unmeasured host `tar` executable. The scan fast path applies the same closure
to the same storage in the same order as the parallel iterator's single-item
path; the multi-storage path is unchanged.

`LICENSE-APACHE` is the upstream repository license copied with the subtree.
