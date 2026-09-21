# Vendored source record

- Repository: `https://github.com/solana-labs/solana`
- Tag/version: `v1.5.19`
- Commit: `936ff7424e1306b0df07dabcd6863bf7896d2cb5`
- Imported subtree: `programs/bpf_loader/`
- Upstream Git tree: `e37f9429b681f5213119a84abc7dc205e432f35c`
- License blob: `285adee28490f5a79e92b3e860e1af2f9b865f07`
- Required compiler/target: `rustc 1.49.0 (e1884a8e3 2020-12-29)`,
  `x86_64-unknown-linux-gnu`

Only the monorepo-relative runtime and SDK dependencies are redirected to the
sibling exact vendored runtime and exact upstream Git revision. Program source
is unchanged. Static linking supplies the exact v1.5.19 BPF loader entrypoints
during snapshot reconstruction.

`LICENSE-APACHE` is copied from the recorded upstream commit.
