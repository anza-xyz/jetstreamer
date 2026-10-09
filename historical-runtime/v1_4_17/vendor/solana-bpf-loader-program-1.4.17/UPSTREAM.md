# Vendored source record

- Repository: `https://github.com/solana-labs/solana`
- Tag/version: `v1.4.17`
- Commit: `599b22baf31c90a80c75b720cd06b4840f5b29fe`
- Imported subtree: `programs/bpf_loader/`
- Upstream Git tree: `b511c7fd8d5c47d473b7a4933fd0fb690adc0272`
- License blob: `285adee28490f5a79e92b3e860e1af2f9b865f07`
- Required compiler/target: `rustc 1.46.0 (04488afe3 2020-08-24)`,
  `x86_64-unknown-linux-gnu`

Only the monorepo-relative runtime and SDK dependencies are redirected to the
sibling exact vendored runtime and exact upstream Git revision. Program source
is unchanged. Static linking supplies the exact v1.4.17 BPF loader entrypoints
during snapshot reconstruction.

`LICENSE-APACHE` is copied from the recorded upstream commit.
