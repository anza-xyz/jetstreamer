# Vendored source record

- Repository: `https://github.com/solana-labs/solana`
- Tag/version: `v1.3.19`
- Commit: `15a49d75086f95573ad319b22e4843639bdf2169`
- Imported subtree: `programs/bpf_loader/`
- Upstream Git tree: `4777d819e27311d303c72eda227e5478affc8cf1`
- License blob: `285adee28490f5a79e92b3e860e1af2f9b865f07`
- Required compiler/target: `rustc 1.45.1 (c367798cf 2020-07-26)`,
  `x86_64-unknown-linux-gnu`

Only the monorepo-relative runtime and SDK dependency paths in `Cargo.toml`
are redirected to the sibling exact vendored runtime and exact upstream Git
revision. Program source is unchanged. Static linking lets the worker supply
the exact v1.3.19 deprecated and feature-gated BPF loader entrypoints to
snapshot reconstruction without loading a mutable external shared object.

`LICENSE-APACHE` is the upstream repository license copied from the recorded
commit.
