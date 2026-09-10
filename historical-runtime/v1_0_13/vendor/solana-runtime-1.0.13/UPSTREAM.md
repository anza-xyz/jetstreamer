# Vendored source record

- Repository: `https://github.com/solana-labs/solana`
- Tag: `v1.0.13`
- Commit: `fdeda769d05fea4a3f861e787d47d995feee15d7`
- Imported subtree: `runtime/`
- Upstream `runtime/` Git tree: `03bf93af15cbb9c0f8dd85252bb4d18e7d728270`
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
git rev-parse v1.0.13^{} v1.0.13^{}:runtime v1.0.13^{}:LICENSE
```

Jetstreamer changes are intentionally narrow:

- monorepo-relative Solana dependencies in `Cargo.toml` point to the exact
  commit above;
- three test-genesis macro calls in `src/genesis_utils.rs` are expanded to
  their identical name/id tuples so the split dependency graph compiles;
- `src/accounts_db.rs` provides read-only, owned, write-version-ordered access
  to physical account writes for the IPC adapter;
- zero- and one-storage `scan_account_storage` calls run directly instead of
  entering the old Rayon pool, whose workers cannot add parallelism there; and
- snapshot bincode decoding rejects any individual byte or string allocation
  above 16 MiB in addition to upstream's aggregate 32 GiB file limit.

The write adapter observes AppendVec contents and the existing atomic write
counter. The scan fast path applies the same closure to the same storage in the
same order as the parallel iterator's single-item path. Neither changes
transaction execution, storage, account indexing, or hashing. The bincode
reader only narrows acceptance of malicious or corrupt snapshot inputs.

`LICENSE-APACHE` is the upstream repository license copied with the subtree.
