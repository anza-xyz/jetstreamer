# Vendored source record

- Repository: `https://github.com/solana-labs/solana`
- Tag/version: `v1.3.23`
- Commit: `ab235b8160f1c76e5066eee52d62d976d12f42f1`
- Imported subtree: `programs/bpf_loader/`
- Upstream Git tree: `f8d2a6bab1b186a70630a5953ac735e8eef06072`
- License blob: `285adee28490f5a79e92b3e860e1af2f9b865f07`
- Required compiler/target: `rustc 1.45.1 (c367798cf 2020-07-26)`,
  `x86_64-unknown-linux-gnu`

Only the monorepo-relative runtime and SDK dependencies are redirected to the
sibling exact vendored runtime and exact upstream Git revision. Program source
is unchanged. Static linking supplies the exact v1.3.23 deprecated and
feature-gated BPF loader entrypoints during snapshot reconstruction.

`LICENSE-APACHE` is copied from the recorded upstream commit.
