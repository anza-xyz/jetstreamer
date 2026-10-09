# Vendored source record

- Repository: `https://github.com/solana-labs/solana`
- Tag/version: `v1.7.15`
- Commit: `4892eb4e1ad278d5249b6cda8983f88effb3e98b`
- Imported subtree: `programs/bpf_loader/`
- Upstream Git tree: `530dbb500c5580b774be3fe33c30e53ace46c4f9`
- License blob: `285adee28490f5a79e92b3e860e1af2f9b865f07`
- Required compiler/target: `rustc 1.52.1 (9bc8c42bb 2021-05-09)`,
  `x86_64-unknown-linux-gnu`

Only the monorepo-relative runtime and SDK dependencies are redirected to the
sibling exact vendored runtime and exact upstream Git revision. Program source
is unchanged. Static linking supplies the exact v1.7.15 deprecated, standard,
and upgradeable BPF loader entrypoints during snapshot reconstruction.

`LICENSE-APACHE` is copied from the recorded upstream commit.
