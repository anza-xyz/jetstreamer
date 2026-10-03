# Vendored source record

- Repository: `https://github.com/solana-labs/solana`
- Tag/version: `v1.4.19`
- Commit: `9466ad3c1f11fb90df6015d6c910f0e89747a553`
- Imported subtree: `programs/bpf_loader/`
- Upstream Git tree: `451540fd75a143b5d8730eb12893d9235b932b68`
- License blob: `285adee28490f5a79e92b3e860e1af2f9b865f07`
- Required compiler/target: `rustc 1.46.0 (04488afe3 2020-08-24)`,
  `x86_64-unknown-linux-gnu`

Only the monorepo-relative runtime and SDK dependencies are redirected to the
sibling exact vendored runtime and exact upstream Git revision. Program source
is unchanged. Static linking supplies the exact v1.4.19 BPF loader entrypoints
during snapshot reconstruction.

`LICENSE-APACHE` is copied from the recorded upstream commit.
