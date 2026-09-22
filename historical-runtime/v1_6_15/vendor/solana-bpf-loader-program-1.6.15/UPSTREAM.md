# Vendored source record

- Repository: `https://github.com/solana-labs/solana`
- Tag/version: `v1.6.15`
- Commit: `5c2dab8055e8162386fcac313b6547f223fd386c`
- Imported subtree: `programs/bpf_loader/`
- Upstream Git tree: `f695a67f8743b5442b077ec19fd21ffd8110f9b6`
- License blob: `285adee28490f5a79e92b3e860e1af2f9b865f07`
- Required compiler/target: `rustc 1.51.0 (2fd73fabe 2021-03-23)`,
  `x86_64-unknown-linux-gnu`

Only the monorepo-relative runtime and SDK dependencies are redirected to the
sibling exact vendored runtime and exact upstream Git revision. Program source
is unchanged. Static linking supplies the exact v1.6.15 deprecated, standard,
and upgradeable BPF loader entrypoints during snapshot reconstruction.

`LICENSE-APACHE` is copied from the recorded upstream commit.
