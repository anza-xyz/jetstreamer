# Vendored source record

- Repository: `https://github.com/solana-labs/solana`
- Tag/version: `v1.6.17`
- Commit: `8f3ce5fc578da93405ccedf22150b9fae2bd83c7`
- Imported subtree: `programs/bpf_loader/`
- Upstream Git tree: `df63d92af7eb6f402aad95e521a07b8ba7407f2b`
- License blob: `285adee28490f5a79e92b3e860e1af2f9b865f07`
- Required compiler/target: `rustc 1.51.0 (2fd73fabe 2021-03-23)`,
  `x86_64-unknown-linux-gnu`

Only the monorepo-relative runtime and SDK dependencies are redirected to the
sibling exact vendored runtime and exact upstream Git revision. Program source
is unchanged. Static linking supplies the exact v1.6.17 deprecated, standard,
and upgradeable BPF loader entrypoints during snapshot reconstruction.

`LICENSE-APACHE` is copied from the recorded upstream commit.
