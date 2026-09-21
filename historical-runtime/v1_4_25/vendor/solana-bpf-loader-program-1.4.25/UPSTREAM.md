# Vendored source record

- Repository: `https://github.com/solana-labs/solana`
- Tag/version: `v1.4.25`
- Commit: `893cc7647248a3536fb6e6d0b5e51c71446b862d`
- Imported subtree: `programs/bpf_loader/`
- Upstream Git tree: `8833f9eb7597399ff6f7a41ad04a4a58400d66c6`
- License blob: `285adee28490f5a79e92b3e860e1af2f9b865f07`
- Required compiler/target: `rustc 1.46.0 (04488afe3 2020-08-24)`,
  `x86_64-unknown-linux-gnu`

Only the monorepo-relative runtime and SDK dependencies are redirected to the
sibling exact vendored runtime and exact upstream Git revision. Program source
is unchanged. Static linking supplies the exact v1.4.25 BPF loader entrypoints
during snapshot reconstruction.

`LICENSE-APACHE` is copied from the recorded upstream commit.
