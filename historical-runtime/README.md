# Historical runtime workers

These sibling workspaces isolate consensus-era Solana runtimes from the
current Jetstreamer process. A worker exchanges only the versioned,
length-bounded types in its `protocol` crate; Solana SDK types never cross the
process boundary. Tiny facade crates compile the one source file in
`protocol-schema`: each historical workspace pins bincode 1.2.1/serde
1.0.105, while `protocol-current` uses bincode 1.3.3/current serde for Agave.
A parent can
alias `jetstreamer-historical-protocol-current` to
`jetstreamer-historical-protocol` in its dependency table to retain the same
Rust imports.  Golden frame tests pin the facades' common fixed-integer wire
encoding.

Every sibling worker is a separate **candidate backend**, not a claim that a
release is correct for a range merely because of its release date. Runtime
selection is slot-driven, candidate use requires an explicit opt-in, and a
generated epoch is publishable only after all canonical post-bootstrap
snapshot checkpoints in that epoch match. The wider terminal-patch envelopes
for epochs 12 through 100 are diagnostic: the first mismatch must split an
envelope around an earlier exact worker rather than weakening validation.

Differential replay found an old-form vote
initialization accepted by mainnet at slot 521850: v1.0.7 accepts that
transaction, while v1.0.8 and v1.0.24 enforce the later node-signature rule.
The same old semantics are observed through slot 618196. A scan of every vote
account initialization and node-account update in slots 618197 through 630648
found no earlier transaction that distinguishes the two runtimes. The
three-account initialization at slot 630648 is the first observed
discriminating transaction: mainnet rolled it back, then initialized the same
account with the four-account form at slot 631224.

The canonical snapshot at slot 619848 provides a behaviorally safe handoff.
The compatibility policy routes slots through 619848 to v1.0.7 and starts
v1.0.8 at slot 619849. This routing boundary is chosen at a snapshot, before
the first observed discriminating transaction, so it does not identify the
validator deployment slot. The deployment transition remains known only to
have occurred after slot 618196 and no later than slot 630648. Both runtimes
have equivalent behavior for the vote-account transactions observed between
the snapshot and that upper bound, and the pinned source diff confines the
relevant state-processing change to those vote checks. v1.0.24 remains
unassigned until replay evidence supports another handoff. Candidate routing
still requires explicit opt-in and trusted checkpoint validation.

The sibling virtual workspaces have independent old-format lockfiles. Workers
through v1.0.18 are pinned to `1.42.0-x86_64-unknown-linux-gnu`; v1.0.23,
v1.0.24, v1.1.23, and v1.2.32 use `1.43.0-x86_64-unknown-linux-gnu`; v1.3.19
uses `1.45.1-x86_64-unknown-linux-gnu`. They cannot share dependency
resolution because the exact upstream graphs require incompatible
pre-release cryptography packages. The old `AppendVec` persisted native Rust
layout, so compiling a runtime with another compiler can interpret historical
storage incorrectly. Each workspace selects the historical compiler, and
each worker's build script enforces the exact compiler commit and target.

Build and test any worker from its isolated workspace (v1.0.24 shown):

```sh
cd v1_0_24
cargo test --locked
cargo build --release --locked
```

All workers accept bounded entry batches so transaction
decoding and independently anchored PoH segments can be prepared in parallel.
All batch validation finishes before bank mutation, while bank advancement,
transaction execution, write collection, and response emission remain in
canonical wire order. The fixed-width PoH backend is differentially tested
against each version-pinned Solana SDK implementation: runtime-dispatched
SHA-NI is guarded by feature detection, every unsafe load/store operates on
fixed-size owned arrays, and randomized tests cover optimized, paired, and
forced-portable paths.

The broad v1.1.23 envelope retains mainnet's epoch-34 BPF-loader activation
and the runtime's epoch-40 system-program transition. The v1.2.32 envelope
retains the Stable-cluster CPI transition at epoch 63. Static loader bindings
reproduce the exact linked processors without relying on mutable,
deployment-adjacent shared libraries.

The executable handshake reports candidate status, protocol version, Solana
tag and commit, Rust toolchain, target, and required mainnet genesis hash. The
parent also hashes the executable itself and records that SHA-256 in historical
archive provenance; resume rejects a different worker binary.

The version-neutral protocol also has a narrowly scoped snapshot-export
transition. Only the v1.0.7 worker implements it, and only immediately after a
successful complete frozen checkpoint. The request supplies a registry-owned
slot and accounts hash; both the parent and worker compare them with the sealed
checkpoint before the worker writes a no-clobber `.tar.bz2` archive through a
same-directory temporary file. The v1.0.7 packager writes GNU sparse AppendVec
members in-process, retaining their mapped length without compressing
fixed-capacity zero tails or executing an unmeasured host `tar`. The parent
then checks the result's path, size, accounts hash, and SHA-256, publishes a
no-clobber evidence sidecar, and gives the successor only an exact-size,
digest-verified private copy. v1.0.8 admits only the expected snapshot topology
under hard entry/member/aggregate extraction limits; v1.0.8 and v1.0.24 reject
the export operation. This is used to bootstrap the next pinned runtime at a
verified boundary, not as a general snapshot RPC.

## Vendored runtime provenance

Each vendored runtime directory contains the upstream `runtime/` crate and an
`UPSTREAM.md` source/patch record. The registry currently contains:

| Workspace | Exact upstream commit | Intended evidence envelope |
| --- | --- | --- |
| `v1_0_7` | `57abc370fa39e42e8fb84145a30395ddcf891692` | slot 0 through the verified slot-619848 handoff |
| `v1_0_8` | `2a617f2d07f714918891f2b479d1cb1c324f0365` | slot 619849 through epoch 7 |
| `v1_0_13` | `fdeda769d05fea4a3f861e787d47d995feee15d7` | epoch 8 |
| `v1_0_14` | `8631be42ac29a062b5e26a85fc2f4c94af042afd` | epochs 9–10 |
| `v1_0_17` | `cfc7b22c4c9094d09fc969247bfe60a154027d84` | epoch 11 |
| `v1_0_18` | `f26f18d29d650d06f5c5b7a4eb625622a999ea66` | unassigned exact fallback for late epoch 12 through epoch 15 |
| `v1_0_23` | `825c0e2b6e39ae67431ed0a8282260ad3914c87a` | checkpoint-gated diagnostic envelope, epochs 12–29 |
| `v1_0_24` | `a93915f1bddb73480f86fc09f487315ae191897d` | registered but unassigned differential candidate |
| `v1_1_23` | `263fc25992ebae85e7ba2f176e9a066449489c3e` | checkpoint-gated diagnostic envelope, epochs 30–60 |
| `v1_2_32` | `8c989da68342918f1717c60aa60fdfab7d1e676e` | checkpoint-gated diagnostic envelope, epochs 61–91 |
| `v1_3_19` | `15a49d75086f95573ad319b22e4843639bdf2169` | checkpoint-gated diagnostic envelope, epochs 92–100 |

The source has only these integration changes:

1. Its internal Solana path dependencies are exact Git dependencies at that
   commit, allowing this small crate to build without vendoring the whole
   monorepo.
2. Three test-only genesis helper macro invocations are expanded to the same
   `(name, id)` values because exported macros cannot be resolved through the
   split Git dependency graph on current Cargo.
3. `AccountsDB` exposes a read-only snapshot of owned account writes ordered by
   their persisted `write_version`, plus the next write version.  This is the
   worker's only runtime instrumentation and does not alter account storage,
   hashing, transaction execution, or serialization.
4. The v1.0.7 and v1.0.8 `AccountsDB` scanners keep zero- and one-AppendVec
   scans on the caller thread, avoiding an old Rayon-pool wakeup when no scan
   parallelism exists. The multi-storage path is unchanged.
