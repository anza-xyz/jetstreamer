# Vendored source record

- Repository: `https://github.com/solana-labs/solana`
- Tag/version: `v1.2.32`
- Commit: `8c989da68342918f1717c60aa60fdfab7d1e676e`
- Imported subtree: `programs/vote/`
- Upstream `programs/vote/` Git tree: `807a207c520da1bcc5e29fbcd60b697e3ac179d8`
- License blob: `a7d55d2d016bf6c7d0a991eb0472dc9e88c06ae2`

To re-check the source identity in an upstream checkout:

```text
git rev-parse v1.2.32^{} v1.2.32^{}:programs/vote v1.2.32^{}:LICENSE
```

Jetstreamer redirects monorepo-relative dependencies to the exact upstream
Git revision and adds one bounded mainnet compatibility gate: equal vote
timestamps on a later slot retain the preceding strict rule through the last
canonical transaction observed with the old CPI behavior at slot `29,371,187`.
The v1.2.26 vote and CPI hotfix behavior applies from the shared candidate
restart boundary at slot `29,371,188`; the enclosing replay must match an
independent terminal accounts hash before this worker is admissible.

`LICENSE-APACHE` is the upstream repository license copied with the subtree.
