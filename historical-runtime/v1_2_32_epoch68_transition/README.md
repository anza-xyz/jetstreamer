# Solana v1.2.32 mainnet epoch-68 transition worker

This isolated worker executes mainnet slots
`29,371,188..29,807,999`. It accepts only a completed snapshot in
`29,371,187..29,807,999`, with candidate routing bound to the source-lineage
slot-`29,371,187` snapshot exported by the exact v1.2.24 predecessor. Its
accounts hash is
`6ubQSWsXQ8dEtxTkZwgpB8vEVj4nAsQcSGmu9usxVSGR`. Its
runtime, BPF loader, stake program, vote program, and SDK are pinned to
upstream Solana v1.2.32 at
`8c989da68342918f1717c60aa60fdfab7d1e676e`.

Two pieces of consensus state are not serialized in these historical
snapshots and therefore cannot be recovered merely by decoding the account
state:

- whether cross-program support, including the
  `sol_create_program_address` BPF syscall, is enabled; and
- whether vote timestamps may repeat on a later slot.

The canonical epoch-67 transaction stream proves the CPI gate was closed
under the predecessor runtime:
transactions using `sol_create_program_address` fail with the BPF loader's
missing-syscall error and charge only fees. Replaying the same transactions
with v1.2.32's in-memory default drains their payer accounts and diverges from
the next canonical snapshot. The separate v1.2.24 worker processes and
checkpoints all old-semantics slots. This worker starts from that checkpoint
and reconstructs both activation states as open from slot `29,371,188`, the
first behaviorally admissible child slot after the last transaction proving
the old behavior.

One reproducible witness is transaction
`v1sn5twz95Xc4Qa7WRVvE6QvqxKSHhvzi4RbCs73saETU6KhVam9wtY66R87dg8gHGUfCWRhXY8fybXnSBCKL9H`
at slot `29,327,576`, source transaction index 180. Old Faithful records
instruction 5 as a call to program
`7WgdLYq1HiVgd1e6T8n1rugBeHBZK3nzm19m6H2eBqFg`, a 30,000-lamport fee, and
`Err(InstructionError(5, Custom(194969602)))`. Payer
`7EEAZWuzNZTUSCdwrw1JS4eX9D8uTTL91owsk4GC3zNB` moves from 42,598,601,840 to
42,598,571,840 lamports, exactly the fee. Upstream v1.2.32 defines
`0x0b9f0002` as `VirtualMachineFailedToRunProgram` and registers
`sol_create_program_address` only while cross-program support is enabled.

The worker has a distinct handshake identity, is bounded to the remainder of
epochs 67 and 68, and remains a checkpoint-gated candidate. The complete
three-span cohort must replay from the immutable epoch-67 root, bind both
handoff snapshots across all three workers, and match the independent terminal
legacy accounts hash before either archive can be published.
