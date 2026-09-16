# Solana v1.2.24 mainnet epoch-67 pre-CPI worker

This isolated worker executes the pre-transition portion of mainnet epoch 67
with the exact Solana v1.2.24 runtime, BPF loader, stake program, vote program,
and SDK at commit `14bc62398944d6270698506d468cbeddc8513ccd`. It accepts
snapshots in slots `29,327,575..29,371,188` and entries in
`29,327,576..29,371,188`; all ranges are half-open.

The handoff snapshot at slot `29,327,575`, with accounts hash
`A286WmNJJ1r5F8G2cnBqykiDGbXgo7aphzJVJqX5ZwbR`, is generated from the Old
Faithful source lineage by the preceding v1.2.32 worker. Cross-program invocation is
omitted from historical snapshots, so this worker restores it as disabled
throughout its bounded lifetime. The first known transaction that distinguishes
this execution from a v1.2.32 replay is
signature
`v1sn5twz95Xc4Qa7WRVvE6QvqxKSHhvzi4RbCs73saETU6KhVam9wtY66R87dg8gHGUfCWRhXY8fybXnSBCKL9H`
at slot `29,327,576`: Old Faithful and exact v1.2.24 return BPF-loader error
`0x0b9f0002`, while v1.2.32 succeeds. Earlier hourly GCS snapshots have
slot-hash state that disagrees with successful Old Faithful votes and are not
used as source-lineage proof.

The worker can export the frozen bank at slot `29,371,187` only immediately
after a successful checkpoint. The corrected source-lineage replay binds that
checkpoint to accounts hash
`6ubQSWsXQ8dEtxTkZwgpB8vEVj4nAsQcSGmu9usxVSGR`. Export is one-use and
no-clobber, verifies the registry-owned accounts hash, writes the v1.2 `1.2.0`
snapshot schema entirely in process, preserves sparse AppendVec capacities,
hashes the completed archive, and exposes no general snapshot-writing RPC. The
parent binds that archive to the exact source segment and worker executable
before allowing the v1.2.32 transition worker to consume it.

This remains a candidate runtime. Both source-lineage handoffs are bound;
production publication requires the successor runtime's terminal checkpoint
to match.
