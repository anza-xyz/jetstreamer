# Vendored source record

- Repository: `https://github.com/solana-labs/solana`
- Tag/version: `v1.7.13`
- Commit: `257ddbeee1e8e7db2daa54e86f8eeedf76ace8f1`
- Imported subtree: `programs/bpf_loader/`
- Upstream Git tree: `b83c5c8c91610938ff9f778d536b6cdfcab60280`
- License blob: `285adee28490f5a79e92b3e860e1af2f9b865f07`
- Required compiler/target: `rustc 1.52.1 (9bc8c42bb 2021-05-09)`,
  `x86_64-unknown-linux-gnu`

Only the monorepo-relative runtime and SDK dependencies are redirected to the
sibling exact vendored runtime and exact upstream Git revision. Program source
is unchanged. Static linking supplies the exact v1.7.13 deprecated, standard,
and upgradeable BPF loader entrypoints during snapshot reconstruction.

`LICENSE-APACHE` is copied from the recorded upstream commit.
