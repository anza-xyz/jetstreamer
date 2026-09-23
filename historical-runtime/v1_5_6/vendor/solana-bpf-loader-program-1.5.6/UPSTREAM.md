# Vendored source record

- Repository: `https://github.com/solana-labs/solana`
- Tag/version: `v1.5.6`
- Commit: `01e4d0a1e9917701d1a148e1043b0ccf545c27f1`
- Imported subtree: `programs/bpf_loader/`
- Upstream Git tree: `61cafea94d8376948e0bb8112eb9a8d6b9b34846`
- License blob: `285adee28490f5a79e92b3e860e1af2f9b865f07`
- Required compiler/target: `rustc 1.49.0 (e1884a8e3 2020-12-29)`,
  `x86_64-unknown-linux-gnu`

Only the monorepo-relative runtime and SDK dependencies are redirected to the
sibling exact vendored runtime and exact upstream Git revision. Program source
is unchanged. Static linking supplies the exact v1.5.6 BPF loader entrypoints
during snapshot reconstruction.

`LICENSE-APACHE` is copied from the recorded upstream commit.
