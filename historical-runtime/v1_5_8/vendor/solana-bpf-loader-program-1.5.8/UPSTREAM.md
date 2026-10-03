# Vendored source record

- Repository: `https://github.com/solana-labs/solana`
- Tag/version: `v1.5.8`
- Commit: `460c643f8e549d22c09a9cddc3b0f5be9c7b2204`
- Imported subtree: `programs/bpf_loader/`
- Upstream Git tree: `018a30f1f679cd98a9860b8b060c8ea1ce502ada`
- License blob: `285adee28490f5a79e92b3e860e1af2f9b865f07`
- Required compiler/target: `rustc 1.49.0 (e1884a8e3 2020-12-29)`,
  `x86_64-unknown-linux-gnu`

Only the monorepo-relative runtime and SDK dependencies are redirected to the
sibling exact vendored runtime and exact upstream Git revision. Program source
is unchanged. Static linking supplies the exact v1.5.8 BPF loader entrypoints
during snapshot reconstruction.

`LICENSE-APACHE` is copied from the recorded upstream commit.
