# Vendored source record

- Repository: `https://github.com/solana-labs/solana`
- Tag/version: `v1.2.24`
- Commit: `14bc62398944d6270698506d468cbeddc8513ccd`
- Imported subtree: `programs/bpf_loader/`
- Upstream `programs/bpf_loader/` Git tree: `eba082914a6cbd98dfe1bfc7a599deda7cc30b55`
- License blob: `a7d55d2d016bf6c7d0a991eb0472dc9e88c06ae2`
- Required compiler/target: `rustc 1.43.0 (4fb7144ed 2020-04-20)`,
  `x86_64-unknown-linux-gnu`

The compiler pin comes from upstream `ci/rust-version.sh` at the recorded
commit (file SHA-256
`2593fff8e9c5ee4a4cbe9d2fd285575a6b173d0c598717919d16ce956f54522a`).
The target is the Linux release target selected by upstream
`ci/publish-tarball.sh` (file SHA-256
`f99228e25d40e6ae813e6eb47e837a6af2a7f6c9cdaa0bbfa704784137a298e0`).

To re-check the source identity in an upstream checkout:

```text
git rev-parse v1.2.24^{} v1.2.24^{}:programs/bpf_loader v1.2.24^{}:LICENSE
```

Only the monorepo-relative runtime and SDK dependency paths in `Cargo.toml`
are redirected to the sibling exact vendored runtime and exact upstream Git
revision. Program source is unchanged. Static linking lets the worker restore
the snapshot-serde-skipped BPF loader function pointer without loading a
mutable external shared object.

`LICENSE-APACHE` is the upstream repository license copied with the subtree.
