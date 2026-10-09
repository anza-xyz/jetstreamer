# Vendored source record

- Repository: `https://github.com/solana-labs/solana`
- Tag/version: `v1.5.5`
- Commit: `10e12d14e105bc2a5cd9c216ffe943a28d2aabf1`
- Imported subtree: `programs/bpf_loader/`
- Upstream Git tree: `b130fcc68c0f3343a1a410ef7cf61450ffe7ac3a`
- License blob: `285adee28490f5a79e92b3e860e1af2f9b865f07`
- Required compiler/target: `rustc 1.49.0 (e1884a8e3 2020-12-29)`,
  `x86_64-unknown-linux-gnu`

Only the monorepo-relative runtime and SDK dependencies are redirected to the
sibling exact vendored runtime and exact upstream Git revision. Program source
is unchanged. Static linking supplies the exact v1.5.5 BPF loader entrypoints
during snapshot reconstruction.

`LICENSE-APACHE` is copied from the recorded upstream commit.
