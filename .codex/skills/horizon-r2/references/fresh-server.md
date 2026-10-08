# Fresh Server and Unsupported Ranges

Use this procedure when a Horizon server has no prior controller state, or when the requested
epochs touch a slot interval marked unsupported by the checked-in runtime registry.

## Establish the host

1. Work from the newest reviewed commit on the requested repository branch. Record `git rev-parse
   HEAD`, retain unknown local changes, and never substitute an uncommitted binary from another
   host. Confirm passwordless `sudo`/systemd operation and enough CPU, RAM, and disk for the
   controller's configured reserves.
2. Confirm `HORIZON_S3_ENDPOINT`, `HORIZON_ACCESS_KEY_ID`, and
   `HORIZON_SECRET_ACCESS_KEY` are present without printing their values. Confirm an active gcloud
   account and read access to the snapshot buckets used by `scripts/preflight_gcs_snapshots.py`.
   The historical snapshot bucket is requester-pays: every detached restore must bind the intended
   account and billing project explicitly, not merely inherit an interactive default. Prove the
   exact generation, size, CRC32C, MD5, bucket/name identity, and object ID with those same flags
   before transferring bytes, and retain that metadata in the restore receipt. Successful token
   refresh alone is insufficient because an otherwise authenticated download can still fail with
   `UserProjectMissing`.
3. Keep public output in `${HORIZON_DIR:-$HOME/horizon}` and private runs, manifests, receipts,
   restore scratch, and controller state outside that directory. The public directory may contain
   only canonical `epoch-N.jet` and `epoch-N.jet.sha256` pairs. Before launching a service as an
   unprivileged replay user, create each private run-root parent under that user, set it to mode
   `0700`, and verify it is neither group- nor world-writable. Do this before the first launch so a
   pre-replay permissions failure does not consume a bounded restart attempt. The adaptive
   controller defaults to `sol:horizon`; on a host with a different dedicated identity, pass
   `--producer-user USER --archive-group GROUP` and verify the printed plan resolves the intended
   UID, GID, and home directory. Do not create placeholder accounts merely to satisfy the defaults.
   If `gcloud` is installed outside `/usr/bin` (including Homebrew), pass its absolute executable
   path with `--gcloud-bin` and verify the printed producer path. Do not rely on an interactive
   shell startup file inside a detached systemd producer. A fresh isolated diagnostic-output
   directory may still request `genesis.tar.bz2` even when its snapshot is already cached. Before
   launch, preseed a retained same-cluster genesis only when its digest matches previously verified
   run evidence, then recheck the destination digest and ownership; otherwise prove that gcloud
   authentication works noninteractively for the service identity.
   Also validate the pinned restore helper against live metadata from the installed gcloud build.
   Homebrew's current standardized JSON names hashes `crc32c_hash` and `md5_hash`, represents size
   as an integer, and binds generation through `storage_url`; older raw-API output uses `crc32c`,
   `md5Hash`, string size, and `id`. The helper must accept either complete shape, reject conflicting
   aliases, and bind the exact versioned object identity before downloading bytes.
4. Build from that recorded commit and pin the hashes of the producer, full verifier, current
   plugin pipeline, boundary verifier, and their launcher scripts before creating a production
   service. Deploy immutable binaries and manifests into root-owned, non-writable paths; do not run
   a long replay from `target/` in a mutable worktree. Initialize each recurring monitor's
   last-observed state from the replay's authoritative launch time before starting its timer, then
   run the monitor once manually to prove it can write evidence. A monitor setup failure does not
   justify restarting a healthy producer. A later `systemctl daemon-reload` can move a pending
   monotonic `OnActiveSec` deadline; after every reload, verify each active monitor's next deadline
   and immediately run only an overdue oneshot monitor so `OnUnitActiveSec` re-anchors the cadence.
   Give every automatic retry a distinct diagnostic output
   and scratch path, or use a proven launcher that preserves the failed attempt before the next
   `.jet` is opened; otherwise use a non-restarting unit and relaunch manually after evidence
   capture. Never let `Restart=on-failure` truncate the only partial archive from the failed run.
5. Before the first replay, record each unit's exact scratch path and keep the diagnostic evidence
   needed after failure outside that tree. Failed-replay scratch cleanup is authorized by default:
   once failure is confirmed, stop the service and retry path, preserve its logs, replay state,
   checkpoints, manifests, receipts, any partial archive needed for diagnosis, and any snapshot
   still needed for diagnosis, lineage, restart, or a staged epoch, and
   then delete the exact unreferenced scratch tree immediately. Verify no live process or staged
   relaunch refers to it first. Retain a controlled stop only when it can genuinely resume in place;
   after a clean bootstrap relaunch into a new isolated generation with no resume cursor, reclaim
   older unreferenced generations promptly as well.
   An ordered historical-worker `ShuttingDown` response is the protocol commit point. On very large
   mmap-backed account stores, SIGKILL may remain pending in an uninterruptible kernel operation past
   a second reap window. Deploy a parent that hands this delayed cleanup to its detached reaper and
   continues sealed manifest publication; the scratch retirement reference scan remains the deletion
   gate. If an older parent reports this condition as a post-archive failure, quarantine every staged
   restart first, preserve the terminal checkpoint, archive-complete log, diagnostic archive, and
   snapshot, then delete only the unreferenced scratch. A sealed archive without its producer segment
   manifest remains private until a reviewed recovery tool reconstructs the evidence and performs a
   complete independent reread.
   Before deployment, confirm checkpoint logs include last blockhash, checkpoint write count, and
   next write-version cursor at both bootstrap and terminal checkpoints. These are part of the
   durable segment evidence and must not exist only in parent memory until manifest publication.
6. Treat disk cleanup as part of admission control. Proactively remove reproducible build caches,
   download caches, obsolete snapshots, and obsolete scratch generations once their exact
   canonical paths have been checked against live processes and staged units. Keep the immutable
   manifest or receipt that
   identifies any removed generation-pinned download, record before/after free space, and preserve
   every required input, resumable state tree, diagnostic artifact, checkpoint, receipt, public
   archive, and R2 object.
7. Include mmap capacity in the host preflight. Record `vm.max_map_count`, live historical-worker
   map counts, and the snapshot/account-store baseline. Legacy AppendVec creation may silently exit
   status 1 at the VMA ceiling because its error log is not necessarily initialized. Establish and
   persist a host-approved limit with enough full-run growth headroom before launching multiple
   workers; do not infer this safety from spare CPU, RAM, or disk.

Inventory canonical archive/sidecar pairs in R2 before scheduling. The sidecar is the remote
completion marker, but a private receipt from another host is not portable completion evidence.
For an explicit range, do not generate epochs already represented by a valid canonical R2 pair.
Do not infer completeness from an archive-only object, a sidecar-only object, or a local filename.

## Prove runtime support first

Read the `Historical replay compatibility` table in `README.md` and the `RUNTIME_ERAS` registry in
`jetstreamer-node/src/compatibility.rs`. Derive the complete half-open slot range for every
requested epoch and require the registry to plan all of it. Then run the snapshot preflight in
planning mode for the exact epoch range. An `unsupported runtime era` or `outside ... supported`
error is a required stop, not a setup problem to bypass.

The registry may contain bounded candidate eras separated by explicit unsupported gaps. A request
for epochs 201–300 must plan the complete range and begin compatibility qualification at its lowest
unsupported slot; support for a later bounded envelope does not authorize skipping an earlier gap.
Do not extend the preceding worker by adjacency, route historical slots through modern Agave, or
publish diagnostic output. Once a reviewed commit has genuinely qualified a bounded range, the
checked-in registry is the authority.

A checked-in focused-qualification-only route may exercise a separately documented, bounded
diagnostic envelope while the ordinary registry continues to expose an unsupported gap. Such a
route must be unreachable from normal epoch/range replay, archive reuse, and publication; require
one epoch, an explicit snapshot, canonical checkpoint file, private output, `--verify`, and
candidate opt-in; and leave the diagnostic archive without a checksum sidecar. Its success is
evidence for a later reviewed registry change, not permission to publish by itself.

## Qualify the next unsupported era

1. Start with the smallest useful canary at the first unsupported slot. Use release history and
   snapshot creator metadata only to shortlist exact Solana candidates; neither is execution
   evidence.
2. Add a pinned historical worker and snapshot loader only after inspecting the exact upstream
   revision. Keep all behavior changes explicit and local to that worker. Register a bounded
   candidate era rather than claiming the entire requested range.
3. Differentially check transaction outcomes/status vocabulary around the proposed boundary and
   replay from a digest-bound canonical predecessor snapshot through every available canonical
   root checkpoint. After a focused replay succeeds, independently run the repository's
   `jetstreamer-qualification-verify` binary with the exact sealed epoch, output range, bootstrap,
   terminal, runtime-profile, worker-digest, and private-root expectations. Treat the diagnostic
   artifact as registry evidence only if that full re-read passes and the canonical `.sha256`
   sidecar remains absent. Derive the validator's output start from the durable segment manifest
   and confirm its slot-count equation; never assume it is `bootstrap + 1`, because predecessor
   snapshot warmup can begin before the epoch boundary where output starts. A candidate must
   reproduce those checkpoints before it can generate a publishable archive.
   For the v1.6.16 epoch-208 qualification, make canonical slot `89,856,107` a required conflict
   gate. Transactions 7 and 8 both succeed and consecutively write payer account
   `FJwFtQFEyKEA4M6ZTrosTRPJphEpDA9ckUeMq9pRJdd4`: signature
   `2jFfi2JubVwgEZd11pQZkX3kBHr9M4CjH5jbeULzJ4JijrfNwS8amU2ipd1Y3BZEfgWYQ143eVseceSn46TkPL1y`
   leaves `848104700000` lamports, then signature
   `3oKU6ZkBjX9SP6njWLoiSChvP87y73Y26GBuj6TL8X62nhZ1FABjDfYT3AZc4eS54fppWgFzyPCrSGHKFxDbuyov`
   leaves `848104695000`. Pass these as the four atomic `--expected-conflict-*` options to
   `jetstreamer-qualification-verify`. Require its digest-bound JSON evidence to show both
   successful, correctly attributed writes and increasing write versions; a standalone RPC record
   or unit test is not qualification evidence.
   If this qualification later fails in the reconstructed confirmed-block interval, compare the
   same input under the conflict-wave worker and the last qualified true-singleton worker before
   changing batching. An identical slot, signature, expected status, and actual status disproves
   wave construction as the cause of that failure. For a vote-program `SlotHashMismatch`, capture
   the voted slot/hash, current parent Bank slot/hash, and the first matching `SlotHashes` sysvar
   entry in one durable receipt. When the canonical vote hash differs from the reconstructed
   parent's Bank/`SlotHashes` hash, investigate reconstructed slot completion and child-Bank sysvar
   continuity; do not promote a singleton guard merely because the affected recovery groups are
   synthetic entry boundaries.
   Before declaring the underlying PoH or sysvar source unavailable, audit the official regional
   GCS ledger replicas (`mainnet-beta-ledger-us-ny5`, `mainnet-beta-ledger-europe-fr2`, and
   `mainnet-beta-ledger-asia-sg1`) by reading their generation-bound `bounds.txt` objects. Bind each
   object's exact generation or versioned `storage_url`, size, CRC32C, MD5, rooted range, and full
   data range in a durable receipt. Do this before downloading a hundreds-of-gigabytes RocksDB
   archive: bounds that end before and restart after the target prove that replica cannot contain
   the missing shreds. Inspect a nearby canonical snapshot only when its slot is still within the
   pinned runtime's relevant sysvar-retention window; a later snapshot cannot recover an already
   expired RecentBlockhashes entry. If every regional ledger excludes the boundary and no retained
   snapshot can carry it, record source insufficiency and require another independently verifiable
   raw-shred or exact historical-sysvar source. Never guess the missing hash, force a Bank hash, or
   weaken terminal/plugin checks to bridge the gap.
   For reconstructed confirmed-block slots, count the PoH block boundaries crossed from the parent
   tick height through completion. A source that supplies only the slot's final blockhash is
   insufficient when that count exceeds one: require an independently verifiable ordered hash for
   every crossed boundary. Never repeat the final hash across multiple boundaries; doing so mutates
   RecentBlockhashes with the wrong intermediate value and can change the frozen Bank/SlotHashes
   hash. Keep a synthetic skipped-slot regression that compares distinct canonical boundary hashes
   with repeated-final reconstruction and proves that both RecentBlockhashes and the Bank hash
   diverge.
   Before launch, also inspect the exact immutable worker's internal snapshot and entry bounds and
   require them to admit the requested terminal slot. The focused planner's search envelope may be
   wider than the current worker guard. If so, defer the lane until the worker bound is deliberately
   extended in reviewed source, range tests and the relevant suites pass, the change is committed
   and pushed to every required SSH remote, and a newly deployed immutable worker digest is bound
   by the launch manifest. Planner acceptance must never be used to bypass a narrower worker guard.
4. Locate later runtime boundaries by evidence, not release dates. Extend the supported interval
   in bounded steps; add another descriptor or isolated restart whenever checkpoint or status
   evidence requires it. Update the compatibility table and focused registry tests with each
   qualified boundary.
5. Run the relevant Rust tests and the complete `scripts/tests` suite. Commit and push the registry,
   worker, documentation, and tests before sealing a production deployment so every server uses
   the same reviewed semantics.

Diagnostic `.jet` files remain private and receive no checksum sidecar. Only after the registry and
snapshot preflight accept a bounded range may the adaptive controller run it with a fingerprinted
manifest and normal root-checkpoint gates.

The user-assigned end epoch is also the publication boundary. If its final sealed verification
cohort extends beyond that epoch to obtain a terminal root, replay and independently validate the
whole cohort in a private lane, then use the repository's bounded staged-cohort recovery option to
publish only the contiguous in-range prefix. Leave every verification-tail `.jet` private and
sidecar-free; never place it in the public Horizon directory or include it in the R2 upload range.

## Coordinate a second server

- Give each server a disjoint explicit range and bind every controller to that range. For the
  Foundation host assigned 201–300, leave 101–200 to the existing host.
- Publication requires full archive verification and the current plugin. Local retirement also
  requires both adjacent boundary receipts. This ordering is deliberate: publish a verified edge
  archive so the neighboring host can restore it into explicit private scratch, close the
  cross-host boundary, and only then retire either local edge. Never restore over the public
  namespace.
- Run replay, verification, and uploads concurrently as resources allow. Upload the archive first
  and the canonical SHA-256 sidecar last. Only the host that holds the exact local gates and durable
  R2 receipt may retire its local pair.
- Resume from remote inventory and local root-owned controller state after interruption. Never
  delete from R2, copy another host's private completion state, or overwrite a mismatching object
  without the user's explicit authorization.
