# Vendored source record

- Repository: `https://github.com/solana-labs/solana`
- Tag/version: `v1.8.11`
- Commit: `423a4d65461e36fefb371a2f164c20c4e7ed5afa`
- Imported subtree: `programs/bpf_loader/`
- Upstream Git tree: `d67e20ec64d1e394d98a7047ac490bd14a211d9c`
- License blob: `285adee28490f5a79e92b3e860e1af2f9b865f07`
- Required compiler/target: `rustc 1.52.1 (9bc8c42bb 2021-05-09)`,
  `x86_64-unknown-linux-gnu`

Only the monorepo-relative runtime and SDK dependencies are redirected to the
sibling exact vendored runtime and exact upstream Git revision. Program source
is unchanged. Static linking supplies the exact v1.8.11 deprecated, standard,
and upgradeable BPF loader entrypoints during snapshot reconstruction.

`LICENSE-APACHE` is copied from the recorded upstream commit.
