# Solana v1.2.24 mainnet epoch-67 pre-CPI worker

This isolated worker executes the pre-transition portion of mainnet epoch 67
with the exact Solana v1.2.24 runtime, BPF loader, stake program, vote program,
and SDK at commit `14bc62398944d6270698506d468cbeddc8513ccd`. It accepts
snapshots in slots `29,186,735..29,371,188` and entries in
`29,186,736..29,371,188`; all ranges are half-open.

The canonical snapshot at slot `29,186,735` records the state from which this
runtime is proven. Cross-program invocation is omitted from historical
snapshots, so the worker restores it as disabled throughout its bounded
lifetime. A root-start proof rejects using v1.2.24 from the epoch boundary: it
diverges at checkpoint `29,195,008`. The first
known transaction that distinguishes this execution from a v1.2.32 replay is
signature
`DJjBpur97ZQVp5EVPUAmWzYkbahzZfr2Sc2zrnDdftFeRWFT5SfWQJHSJBX77KY69FToPYYFNqVY2PjGUQzuYV4`
at slot `29,188,719`: mainnet and exact v1.2.24 return BPF-loader error
`0x0b9f0002`, while an early v1.2.32 replay succeeds and changes canonical
state. Exact v1.2.24 with CPI disabled matches the independent checkpoint at
slot `29,195,008`, including its accounts hash, bank hash, capitalization,
transaction count, and tick height.

The worker can export the frozen bank at slot `29,371,187` only immediately
after a successful checkpoint. Export is one-use and no-clobber, verifies the
registry-owned accounts hash
`JBDL7UvkWrgMdcW9PuFyoXmSUjxpiB5gWJGWpG6RTwC8`, writes the v1.2 `1.2.0`
snapshot schema entirely in process, preserves sparse AppendVec capacities,
hashes the completed archive, and exposes no general snapshot-writing RPC. The
parent binds that archive to the exact source segment and worker executable
before allowing the v1.2.32 transition worker to consume it.

This remains a candidate runtime: production publication requires all
canonical post-bootstrap checkpoints and the successor runtime's independent
terminal checkpoint to match.
