---
name: horizon-r2
description: Generate, verify, upload, and safely retire Horizon epoch archives using Cloudflare R2 as the durable inventory. Use when filling missing Horizon epochs, syncing verified .jet archives to R2, or resuming a specified epoch range.
---

# Horizon R2

Treat R2 as durable storage. Never call `DeleteObject` or `DeleteObjects`. Aborting an incomplete multipart upload is allowed. Preserve an existing object by default. Overwrite an existing `.jet` only when positive verification evidence identifies that remote archive as erroneous and the user explicitly authorizes its replacement; a mismatch alone does not establish which copy is wrong.

Use the repository's `jetstreamer-r2` binary for upload and remote proof. Do not reimplement its multipart protocol in shell or Python. R2's S3 `UploadPart` currently rejects its advertised SHA-256 header, so the binary uses R2-enforced `Content-MD5`, checks the multipart ETag independently, and reads the completed remote object back while computing its whole-file SHA-256. It must fully upload, complete, read back, and revalidate the `.jet` before it uploads the canonical `.sha256` sidecar. It then reads the sidecar back and fsyncs a private receipt before optional local retirement. Treat presence of the matching sidecar as the remote readiness commit marker; an archive without one is staged, and a sidecar without an archive or whose digest does not match the archive is an error. If R2 begins returning native composite SHA-256 evidence, the binary validates and prefers it.

For a long-running range, use `scripts/sync_horizon_r2_progressive.py` to discover newly completed local pairs and invoke the Rust uploader serially. Do not run two progressive uploaders over overlapping ranges. The Python process never implements remote integrity checks or unlinks files itself. Upload requires digest-matching full-verification and current-plugin receipts. Optional local retirement additionally requires both adjacent-boundary receipts. This separation lets disjoint servers publish complete archives so a retired neighbor can be restored from R2 to close a cross-host boundary without circularly weakening retirement. The orchestrator requires the exact approved binary and verifier-script SHA-256 values, accepts explicit comma-separated binary- and script-hash allowlists during a pinned verifier transition, honors every `--defer-epochs` range for retirement, and then delegates remote verification plus optional local retirement to the Rust uploader.

## Resolve the work

- On a fresh host, or whenever any requested epoch is outside the checked-in runtime registry,
  read [references/fresh-server.md](references/fresh-server.md) before downloading snapshots or
  launching replay. An explicit range assigns work; it does not authorize guessing an execution
  runtime or weakening a compatibility boundary.
- Treat invocation without an epoch argument as authorization to start the next missing supported
  work, not as an inventory-only request. If the lowest missing requested epoch is unsupported,
  begin the compatibility-qualification path in the fresh-server reference instead of silently
  skipping it. Accept either one epoch or an inclusive `START-END` range when the user supplies a
  bound. Do not ask for a range merely because it was omitted.
- Locate the Jetstreamer repository and read its current historical compatibility table and active controller state before starting generation.
- Plan missing work as ordered contiguous cohorts before falling back to singleton epochs. Adjacent
  epochs that share an immutable runtime and checkpoint chain should run sequentially in one
  persistent generation, carrying the same live runtime state and AccountsDb scratch forward across
  epoch boundaries. Do not implement such a cohort as adjacent independent units, and do not budget,
  restore, reinitialize, or clean its scratch as if each member were an independent replay. Apply
  the detailed cohort boundaries and publication gates in **Produce and verify**. When generating a
  fresh preflight manifest for compatible missing work, explicitly pass
  `--target-cohort-epochs=3` or `--target-cohort-epochs=4`; the preflight command's default of one
  epoch preserves legacy behavior and does not implement this preference. Confirm the resulting
  fingerprinted manifest actually contains the intended multi-epoch cohorts before sealing a
  controller.
- Use `HORIZON_DIR` when set; otherwise use `$HOME/horizon`. Keep only `epoch-N.jet` and `epoch-N.jet.sha256` in that public directory.
- Credentials are `HORIZON_S3_ENDPOINT`, `HORIZON_ACCESS_KEY_ID`, and `HORIZON_SECRET_ACCESS_KEY`. Check only that they exist; never print their values. The endpoint path names the bucket.
- With an explicit user range, restrict ordinary generation, verification, upload, and cleanup to
  that range; the only permitted out-of-range work is the isolated verification tail below.
- Treat the explicit range end as the publication boundary even when a sealed verification cohort
  must replay farther to reach its terminal root. Keep every out-of-range verification-tail archive
  in private storage, give it no canonical sidecar, and exclude it from the public Horizon directory
  and R2. Deeply validate the complete sealed cohort before transactionally importing only the
  requested contiguous prefix through the repository's bounded recovery option. Seal this intent in
  the source plan: run `preflight_gcs_snapshots.py` through the first compatible later root with
  `--last-epoch` set to that verification endpoint and `--publish-through-epoch` set to the user's
  requested range end. Require the manifest's tail epochs to be marked
  `verification-tail-private`; never rely on an operator remembering an unrecorded boundary.
- Without a range, inventory canonical R2 pairs first. Upload any complete local pairs missing from R2, then begin with the lowest missing epoch supported by the repository's compatibility manifests. Continue in bounded cohorts as resources permit.
- An R2 epoch is complete only when both `epoch-N.jet` and `epoch-N.jet.sha256` exist and agree with verified local evidence. A checksum-only or archive-only epoch is incomplete.
- Use the repository binary's mutation-free inventory mode for an authoritative explicit-range
  census: `jetstreamer-r2 inventory "$HORIZON_DIR" --epochs START-END`. It issues only archive HEAD
  and sidecar GET requests and reports every epoch in JSON. Treat `ready-pair` as remote presence
  evidence only; it does not replace the private R2 receipt or the original full/plugin gates.
  Investigate every `sidecar-only`, `invalid-sidecar`, or `metadata-mismatch` anomaly immediately;
  never repair or overwrite it merely because inventory detected it.

Remote object pairs are the shared inventory between servers; private receipts and controller
state are host-local evidence and must not be copied to make another host believe work completed.
Give concurrently operating hosts disjoint explicit epoch ranges. Existing R2 objects are never a
lease: if ranges overlap accidentally, stop the duplicate producer rather than racing publication.

## Produce and verify

- Use the repository's sealed historical replay/controller path and automatic slot-range runtime selection. Do not invent a compatibility override.
- Run generation and long-lived verification/upload watchers in persistent systemd units so loss of the interactive session cannot kill them. Use `Restart=on-failure` with a bounded retry delay only when every invocation gets isolated diagnostic output and scratch, or a proven launcher preserves the failed invocation's artifacts before opening the next output. A replay normally creates or truncates its `.jet` at startup, so never let an automatic retry reuse the same diagnostic archive path. Use a fail-closed non-restarting unit and relaunch manually after evidence capture when attempt isolation is unavailable. Do not use `Restart=always`, because successful completion must remain terminal. Respect unrelated jobs and configured RAM/disk reserves.
- A controller with a long polling interval must wake that wait when SIGTERM/SIGINT requests
  shutdown; setting a flag while leaving a restartable sleep in place can consume the entire systemd
  stop timeout. Before sealing a new controller generation, verify a no-worker instance reaches
  clean terminal success promptly when stopped.
- When a guard pauses adaptive controllers around a serialized public import, authenticate and fsync
  the exact live systemd controller commands plus the importer's invocation ID before stopping the
  first controller. After a guard restart, recover that state, finish stopping authenticated live
  controllers (including late arrivals) before waiting on the same zero-restart importer, and keep
  the state until every controller has been restored or deliberately left paused. A collected
  importer unit is terminal only when no other public importer is live. Never replace a guard while
  it has stopped-controller commands only in memory and its bound importer is still active.
- Before starting a recurring status timer, initialize its last-observed state to the replay's
  authoritative launch time and validate the monitor once manually. A timer that exists without
  this state is not monitoring the replay; diagnose monitor failures without restarting a healthy
  producer. After any `systemctl daemon-reload` while monotonic monitor timers are active, re-observe
  every timer's next deadline: systemd can move a pending `OnActiveSec` deadline relative to the
  reload. If that delays an already-due check, run only the oneshot monitor immediately and confirm
  that `OnUnitActiveSec` has re-anchored the next interval; never restart the producer to repair its
  monitor schedule.
- A hardened adaptive lane may use an isolated `CLOUDSDK_CONFIG` under its private lane root.
  Refreshing the interactive user's default GCloud configuration does not refresh those copies.
  Before retrying a producer after authentication changes, update the isolated configuration with
  a consistent, permission-preserving copy of the refreshed credential databases, then perform a
  read-only lookup of one exact generation-bound snapshot through the lane's effective
  `CLOUDSDK_CONFIG`, account, and project. Never print an access token or infer success from a
  lookup made through the interactive default configuration. Refresh idle lanes before they are
  admitted so a controller does not consume an attempt on stale credentials. Never replace or
  mutate the isolated configuration of a live producer: first authenticate its exact systemd
  invocation and lane claim, and update only lanes with no live process or staged launch. Stage a
  coherent permission-preserving configuration copy beside the idle lane, validate it with the
  exact generation-bound lookup, and install it atomically instead of copying credential databases
  piecemeal into the effective directory.
- When launching into a fresh isolated diagnostic-output directory, ensure it contains the verified
  same-cluster genesis expected by the loader. A cached snapshot does not imply the genesis is
  present. Preseed only from digest-bound retained evidence and verify the copied digest and
  ownership; otherwise validate noninteractive gcloud authentication for the service identity.
- Before sealing a generation-pinned GCS restore helper, exercise its metadata parser against the
  host's live `gcloud storage objects describe --format=json` output. Current Homebrew gcloud uses
  normalized `crc32c_hash`, `md5_hash`, integer `size`, and `storage_url` fields, while older
  releases exposed raw-API `crc32c`, `md5Hash`, string `size`, and `id` fields. Accept both shapes,
  fail on conflicting aliases, and require either the exact raw object ID or exact versioned
  `storage_url`; authentication success without this complete identity/hash proof is insufficient.
  GCS composite objects legitimately omit MD5. Represent them only with the current manifest
  schema's explicit `md5_hash: null`, and still require the exact generation, canonical versioned
  URI, nonzero size, and canonical CRC32C. Recompute CRC32C after download and retain the local
  SHA-256/inode binding. Never accept a missing MD5 field, fake a digest, or relax a legacy schema
  that requires MD5.
  Before transferring an absent snapshot, require actual free bytes to cover the complete expected
  object size above the protected filesystem floor. Recheck the protected floor after hashing the
  temporary download and before no-clobber publication. If either gate fails, discard only the
  temporary download and leave the destination and receipt absent.
- If live GCS inventory is temporarily unavailable, regroup only from a previously sealed v2, v3,
  or v4 preflight report whose independently recorded fingerprint is supplied explicitly. Use
  `scripts/preflight_gcs_snapshots.py --source-manifest-report=PATH
  --source-manifest-fingerprint=sha256:...`; never use the report's adjacent embedded fingerprint
  as the independent value. The conversion must strictly validate every selected generation,
  canonical versioned URI, size, CRC32C, schema-specific MD5 representation, cohort/epoch binding,
  and runtime route before emitting a fresh v4 manifest. This is planning evidence only: snapshot
  restore must still re-observe the exact GCS generation and hashes before replay.
- Failed-replay cleanup is authorized by default for this workflow. After confirming the failure and capturing the evidence needed to diagnose or reproduce it, stop the service and retry path, verify that no live process or staged relaunch references each exact target, and delete that failed run's scratch immediately instead of allowing failures to accumulate. Preserve diagnostic output and partial archives only until their useful evidence is sealed: for a terminal, non-resumable, superseded generation that cannot satisfy a full/plugin/publication gate, fsync a root-owned receipt with the service result, last slot/progress, exact size and archive hash when available, and copy any segment manifest outside the deletion tree; then retire the unreferenced local output promptly while retaining logs, manifests, receipts, hashes, and inputs needed for lineage or reproduction. Never apply this rule to a controlled stop that can genuinely resume in place or to any locally complete archive awaiting verification/upload. If a relaunch demonstrably starts from the bootstrap in a new isolated runtime generation with no resume cursor, verify the new worker's exact generation and reclaim older unreferenced generations promptly; preserving them does not make that replay resumable.
- Diagnose an archive `SectionTooLarge` against both the record-count and data-arena limits for the
  named phase. The error's historical `bytes` field is also used for a count overflow, so a value
  such as `8193 (limit 8192)` can mean the 8,193rd update rather than an 8,193-byte account. Capture
  the exact slot, phase, observed value, immutable producer identities, and partial archive before
  cleanup. Never drop, truncate, or misattribute an account update to make replay continue. When the
  observed canonical shape exceeds a practical inline capacity that is not serialized on the wire,
  raise it conservatively while retaining independent byte/decode-work bounds, add an exact
  observed-shape plus one-past-the-new-bound writer/reader regression, and retry only in a fresh
  isolated archive and scratch generation.
- Treat an ordered historical-worker `ShuttingDown` acknowledgement as the worker protocol commit
  point. A legacy worker can remain uninterruptible for longer than both graceful and forced reap
  windows while the kernel tears down a multi-terabyte mmap-backed AccountsDb; delayed reaping after
  that acknowledgement is cleanup latency, not replay divergence. Pin a parent build that transfers
  the child, private runtime directory, and guardian to a detached reaper without failing the sealed
  producer. Scratch retirement must still scan commands, maps, cwd/root/exe links, and descriptors
  and refuse deletion while any process retains the tree. If an older parent instead exits nonzero
  after the terminal checkpoint and `horizon archive complete`, quarantine its restart path, preserve
  the archive and exact journal evidence, and reclaim only the unreferenced scratch. Do not invent a
  missing segment manifest or publish the artifact: require an independently reviewed recovery path
  and a complete durable archive reread before treating it as qualification evidence.
  Ensure every immutable parent logs the complete bootstrap and terminal checkpoint summaries,
  including last blockhash, drained-write count, and next write-version cursor. Those fields are
  required to reconstruct auditable producer evidence after a post-archive parent failure; terminal
  bank and accounts hashes alone are insufficient.
  When recovering v1.6.16 terminal evidence from the archive, do not equate all terminal-slot
  account updates with the checkpoint's drained-write count. Bind the archive to single-runtime V2
  `solana-v1.6.16` provenance, count pre-, transaction-, and post-phase writes separately, and
  derive the checkpoint count as post-phase writes minus the final freeze-root drain write. Require
  that runtime-specific derivation to match the journal's drained-write count and next write-version
  cursor; fail closed for other provenance or an empty post phase until its runtime semantics are
  independently qualified.
- Do not raise replay concurrency from low CPU or RAM utilization alone. Snapshot extraction and
  historical account state can consume hundreds of GiB per worker; require the controller's
  configured per-worker disk admission budget and measured filesystem growth to leave the reserve
  intact before launching another cohort. If a manual canary set would violate that gate, stop the
  newest units and preserve their private run directories for later resumption.
- A controller that observes producers owned by other controllers must not silently charge every
  external producer the same remaining-growth budget as a new local worker when stronger sealed
  evidence establishes heterogeneous claims. Use invocation-bound external growth claims derived
  from exact live trees and stable recent windows; keep the complete local worker budget for the
  proposed launch and the global filesystem reserve. Bind those claims into the controller
  configuration and admission manifest. An absent unit, invocation mismatch, unbound producer, or
  sampling race must fall back to the conservative full per-worker budget. Re-run admission rather
  than editing claims underneath a live sealed controller.
- Consecutive epochs using the same immutable runtime should normally be replayed as a bounded
  contiguous cohort so later epochs carry the live runtime state and AccountsDb scratch instead of
  restoring another bootstrap. A contiguous cohort is one ordered replay generation, not a set of independent
  epoch jobs: select the complete range before launch, use one persistent producer/worker and one
  scratch path, process every member in ascending order, emit a separate archive for each epoch,
  and keep the same scratch/AccountsDb state live across member boundaries. Sharing a pathname is
  not sufficient: do not stop and relaunch the worker, restore another snapshot, reopen the next
  epoch as a fresh replay, or rebuild AccountsDb at a member boundary. Never clean, reinitialize,
  or independently restore that scratch between cohort members. Budget bootstrap and scratch once
  for the whole cohort, plus measured incremental growth, rather than charging every epoch as a
  separate full restore. When several still-missing adjacent epochs share the same immutable
  runtime and checkpoint chain, normally
  target three to four epochs per continuous run before admitting more single-epoch workers, and
  use a longer bounded run when measured scratch reuse and restart risk justify it. Shorten or split
  the run whenever runtime, root, resource-admission, or live-claim boundaries require it. Prefer
  this when duplicated bootstrap/scratch is the admission bottleneck.
  Never use a continuous root-cohort plan to cross or erase a focused-qualification-only
  compatibility gap. A diagnostic route that requires `--qualification-end-slot` remains
  independently checkpoint-gated and may explicitly forbid carried runtime state; qualify and
  promote every required checkpoint through its reviewed focused path before the ordinary runtime
  registry or production preflight may coalesce those epochs. A retained manifest produced by an
  older or broader temporary registry is not authority to bypass the current checked-in boundary.
  When the sealed manifest's exact generation-pinned bootstrap is already retained locally, prefer
  reusing that cache to downloading a duplicate only through a reviewed cohort input that preserves
  the manifest's trust boundary. Require an absolute path with the exact canonical snapshot filename,
  open it as a regular file without following symlinks, verify the manifest size, CRC32C, and MD5,
  bind its SHA-256 and file identity, and revalidate the open file plus path immediately before worker
  initialization. Preserve the original restore/download receipt. Never substitute loose directory
  discovery, a copied filename, or an unbound cache entry for this check; if the deployed parent lacks
  such an input, keep the cohort staged until authenticated generation-pinned download works or a new
  immutable parent implementing the gate is qualified and deployed.
  The checked-in adaptive controller exposes this input as repeated
  `--cohort-bootstrap=EPOCH_OR_RANGE=/ABSOLUTE/PATH`. Require the range to name one exact managed
  cohort and the archive to be a singly linked, root-owned, non-writable direct member of the sealed
  deployment with an exact `SHA256SUMS` entry. Review the planning report's `cohort_bootstraps`
  mapping before execute mode; the controller binds the path and digest into its configuration and
  exact adoption arguments, while the parent performs the manifest filename, slot, size, CRC32C,
  MD5, SHA-256, and file-identity checks. Do not pass this option to a controller or parent that
  predates the reviewed cache gate. Do not infer parent compatibility from the controller source:
  an immutable deployment can accidentally combine a newer controller with an older
  `jetstreamer-node`. Before admission, capability-probe the exact sealed node with the intended
  root-cohort, manifest, and cached-bootstrap arguments plus a deliberately mismatched manifest
  fingerprint. Require it to reach the fingerprint gate, rather than reject the cached-bootstrap
  argument combination, and bind the probe receipt and node SHA-256 into the admission manifest.
  Keep independent root-verifiable cohorts parallel when disk admission is healthy and fleet wall
  time is the priority: historical execution is often mostly serial within one worker, and an
  unnecessarily long cohort increases the restart blast radius. Never merge across a runtime or
  compatibility boundary, exceed the immutable worker's proven terminal bound, omit an
  intermediate root check, overlap a live epoch claim, or publish any member before the complete
  sealed cohort reaches its final root and passes the normal per-archive gates. Treat a requested
  cohort length as a soft maximum: when its preferred endpoint lacks a root, finish at the latest
  earlier root in that window; extend to the first later same-runtime root only when the whole
  target window has no usable root. This bounds restart and publication blast radius without
  inventing a checkpoint.
- When a live replay's measured growth could cross the filesystem reserve before its next safe
  milestone, use an actual-free-space guard rather than relying only on projections. Bind the guard
  to the exact service invocation ID and zero-restart state, fsync a root-owned stop intent before
  stopping that one producer, and make an intent-only interruption resumable even if free space
  later recovers. Record completion only after the bound process is inactive. A reserve trip does
  not authorize deleting its scratch, restarting it automatically, modifying remote objects, or
  stopping any unbound invocation.
  After operator review, a reserve-stopped run whose exact pinned node demonstrably restarts from
  its retained sealed bootstrap (rather than a scratch cursor) may retire only that run's scratch
  with `scripts/retire_reserve_stopped_replay_scratch.py`. Run its `--check-only` gate first; bind
  the exact guard intent/completion, controller and producer invocation IDs, cursor-free cohort
  state, and repeated deletion path. Preserve the run directory, retained input, partial archives,
  state, journals, and receipts. Do not use this path for a replay that can resume its live state
  from scratch or before the bound controller and producer are terminal.
  For a producer owned by a live adaptive controller, use
  `scripts/guard_adaptive_replay_reserve.py` and bind both invocation IDs. It must stop and confirm
  the controller first, then resample and stop only the selected producer; otherwise the controller
  can race the reserve action by scheduling replacement work. Leave the controller stopped after a
  trip until an operator has reviewed the durable receipts and chosen a recovery or fresh cohort.
  Use the single-service guard only for a genuinely standalone producer.
  Bind the adaptive guard to a measured VMA ceiling as well as the filesystem floor. A live worker
  above the ceiling, multiple identifiable workers, or unreadable maps must use the same durable
  controller-before-producer stop path. Permit zero workers without a VMA trip because an authenticated
  producer can remain active during terminal archive drainage; service identity, restart, and disk
  checks still apply. Prove the installed sandbox can read the exact worker's maps on its first fire.
  When admitting a new producer beside older live producers, set that producer's stop floor to the
  global reserve plus the conservative remaining-growth budgets still owed to the older producers,
  unless one proven aggregate guard serializes the complete fleet's response. Giving every lane an
  independent guard at only the global reserve lets several lanes consume the same headroom and can
  overshoot the reserve before their next samples. Record the exact budget formula and live claims
  in the launch receipt; reduce the protected amount only from newer measured evidence, never from
  idle CPU or current scratch size alone.
  Do not leave an automatic admission trigger armed when the resulting controller can launch a
  producer before its invocation-bound reserve guard can be created and proved. If controller or
  producer invocation IDs are not knowable until launch, install the reviewed admission and
  controller generation dormant, launch it under observation, bind the guard immediately to the
  exact zero-restart invocations, and require the first sandboxed guard fire to pass before leaving
  the replay unattended. When replacing an admission generation, explicitly clear or disable every
  older timer, dependency, and `OnSuccess=` trigger that could still launch stale receipt,
  capacity, binary, or guard assumptions. Preserve a durable receipt showing that old and new
  launchers were inactive during the handoff; never infer that installing a replacement unit
  neutralized the previous trigger.
  Do not treat an unsandboxed manual guard invocation or an `active (waiting)` timer as proof that
  the installed guard works. Before considering a producer protected, observe the timer's first
  real service invocation complete with exit status zero under the deployed sandbox, and preserve
  its sampled invocation ID, restart count, available bytes, floor, and no-trip result. If the
  sandboxed invocation cannot traverse or write its private receipt path, stop that broken timer,
  preserve its failure journal, correct the least-privilege capability or path policy, and repeat
  this first-fire proof without restarting the producer. Pre-create every directory named by the
  guard's `ReadWritePaths=` and verify its sealed owner and mode before launch; systemd resolves
  those paths while constructing the mount namespace, so a missing receipt directory fails with
  status 226 before the guard can sample or create it.
- Before using scratch growth for admission or cross-host comparisons, resolve the exact live
  `--replay-scratch` argument from the service/process rather than measuring a lane, qualification,
  or retry parent. Record physical bytes, apparent bytes, file count, and a bounded-depth directory
  breakdown for that exact generation. When growth is nonlinear, compare account-store file-size
  distributions and disk/VMA growth per million transactions before attributing it to host capacity
  or launching another worker.
- For pinned legacy Solana workers, record the visible CPU count and the effective
  `SOLANA_RAYON_THREADS` value. When unset, the old thread-limit crate defaults to half of visible
  CPUs, and AccountsDb also uses that value as its minimum store fan-out; a high-core host can
  therefore create many extra 4-MiB AppendVecs per busy slot even when replay uses little CPU.
  Confirm this with a per-slot store-count histogram rather than assuming transaction batching is
  the sole cause. A historical snapshot restored with AccountsDb caching disabled sends every
  commit directly through store selection, so singleton transaction commits and the minimum-store
  fan-out can amplify each other. Treat the unpacked snapshot's files, physical bytes, and VMAs as
  a baseline: compare settled incremental windows at equal transaction/update work, and never
  extrapolate a full epoch from one early absolute scratch measurement. Before the next immutable
  generation, use a bounded same-snapshot A/B to select an explicit value appropriate to the
  worker's actual CPU quota, comparing throughput, physical bytes, file count, and VMAs; measure
  commit-wave changes as a separate A/B. Decoupling eager per-slot store fan-out from the rayon
  pool is also a distinct storage-layout experiment: keep it diagnostic until it wins that bounded
  comparison and then passes the normal canonical checkpoint and plugin qualification.
  A conflict-wave implementation must also preserve the pinned runtime's invalid-transaction lock
  behavior: sanitize failures and duplicate account keys do not acquire locks, and repeated writable
  keys within one transaction are not a cross-transaction attribution conflict. Differentially test
  write/write and write/read barriers, independent batching, invalid duplicate-key handling, and the
  known canonical conflicting-write slot before qualification.
  Derive writable-key attribution with the same feature gate used by that pinned Bank's
  `prepare_batch` account-lock path. This API is version-specific: the qualified v1.6 workers expose
  `demote_sysvar_write_locks`, while v1.7 and v1.8 expose `demote_program_write_locks`. Do not copy
  the older boolean or approximate writable keys across a runtime boundary; inspect the vendored
  Bank and Message implementations, pass the exact active demotion value to `is_writable`, and add a
  compile/runtime regression for the selected candidate.
  Run each real boundary-snapshot integration test from that historical runtime's workspace root,
  not from the repository root with only `--manifest-path`: rustup selects the pinned legacy
  toolchain from the current directory, and every worker build script must reject a modern compiler.
  Give the libtest process `RUST_MIN_STACK=134217728` for these old full-snapshot load/verify gates;
  the default test-thread stack can overflow inside legacy snapshot restoration even when the same
  code is valid in the production main thread. Keep this as a test-only environment setting and
  still treat any hash, bank verification, or checkpoint mismatch as a real qualification failure.
  Pin the chosen environment in the launch manifest and repeat the normal root/plugin qualification;
  never change it underneath a live replay.
  When a regression lives inside an excluded vendored runtime crate, do not assume the worker suite
  executes that crate's own `#[cfg(test)]` tests. Old Cargo can also walk upward into the modern root
  manifest, while a standalone copied crate can resolve newly published dependencies that its pinned
  compiler cannot parse. Test it in a uniquely named disposable copy of the complete historical
  workspace: add only the exact vendor crate as a temporary workspace member, retain the historical
  `Cargo.lock` and toolchain, resolve offline, and remove an upstream dev-dependency only from that
  disposable manifest when it is unused and absent from the pinned lock. Never alter production
  workspace membership to make a vendor test run. Preserve failure output until a corrected retry is
  proven, then delete the disposable tree promptly; the normal worker suite and real boundary-snapshot
  gates remain independently required.
- For a bounded performance cohort, capture cgroup CPU, memory peak/events, major-fault, pressure,
  and I/O counters inside the persistent runner before and after its child; terminal service
  cgroups may disappear before an external collector can read them. Keep target-slot detection
  independent of optional throughput-counter parsing so a log-format change cannot let a canary
  run past its bound. If the runner copies an immutable worker into scratch, make the safety guard
  recognize the actual bound executable path as well as the source basename. Require exactly one
  identifiable worker with readable maps in every active or activating lane and trip closed on
  zero, multiple, or unreadable workers. The sole zero-worker exception is bounded post-target
  drainage: before signaling the child, the pinned runner must fsync a no-clobber intent binding
  the exact systemd invocation, target and observed slots, child PID, captured scratch metrics,
  and absence of an external signal. The guard may accept that exact intent for a short sealed
  timeout while the wrapper writes its final receipt; it must reject an absent, stale, malformed,
  wrong-owner, or wrong-invocation intent and must never extend the exception to multiple workers
  or a pre-target stop. Test the guard against the copied path and this handoff race in a nested
  fake cgroup before sealing it.
  A pinned legacy worker may acknowledge the runner's post-target SIGINT and then exit 1 while its
  ready-entry channel closes. Normalize that exact outcome only after the runner has independently
  observed the bound, retain the actual child return code and stop signal in its durable receipt,
  and keep pre-target exit 1 fatal. Bind any accepted controlled-stop codes in the result manifest;
  collectors and scratch retirement must reject codes outside that sealed allowlist.
  Historical runtime state may live in a temporary directory that the parent removes during
  shutdown. Freeze the bounded child process group and capture the exact scratch/accounts-state
  statistics before sending the controlled stop; then resume and stop it. Do not design a terminal
  collector that assumes those temporary AppendVecs will still exist after the parent exits.
  A collector or selector that re-observes diagnostic archives below the producer home must not
  use `ProtectHome=yes`: that makes a valid archive appear absent and can turn a successful cohort
  into a false selection failure. Use `ProtectHome=read-only` plus an explicit read-only binding for
  the exact private cohort root, and make only the root-owned receipt directory writable. Preserve
  the failed sandbox invocation as evidence, correct it in a distinctly named oneshot unit, and
  require that unit plus the selection receipt to succeed before scratch retirement.
- When a follow-up performance cohort must preserve its predecessor's systemd unit identity as
  durable result or cleanup evidence, give the follow-up producer template a distinct constrained
  namespace instead of replacing or prematurely removing the predecessor template. Bind that
  namespace transitively in the launch receipt, result manifest, collector, retirement plan, and
  scratch retirer; reject undeclared or mismatched namespaces. A different template name does not
  relax lane-path, invocation-ID, terminal-success, or live-reference checks.
- After a bounded performance cohort has a root-owned, fsynced result receipt containing every
  lane's exact scratch measurements, retire its scratch promptly without discarding the evidence
  needed to choose or qualify a winner. For a multi-lane cohort, bind every exact scratch path in
  one root-owned plan, require all producer units to remain at clean terminal success, and scan
  process command lines, maps, cwd/root/exe links, and file descriptors for live references. Fsync
  one intent covering the complete set before the first deletion, make an intent-only interruption
  safely resumable, and fsync completion only after every bound tree is absent. Preserve partial
  diagnostic archives, segment manifests, canary/result/guard receipts, configurations, genesis,
  journals, and immutable manifests outside the scratch trees. This cleanup never authorizes
  publication, a canonical sidecar, or an R2 mutation.
- When a bounded comparison has a precommitted JSON policy, use the repository's
  `scripts/select_historical_performance_candidate.py` rather than hand-calculating the decision.
  Seal the selector first, require the exact root-owned terminal result receipt bound by that
  policy, and preserve its no-clobber selection receipt. A selection receipt chooses an environment
  only; it must keep qualification launch and publication unauthorized until their separate fresh
  admission and integrity gates pass.
- Treat a revised admission or launch manifest as a transitive binding change. Before activating
  that revision, re-seal every downstream result collector, timer, cleanup manifest, and retirement
  unit that names or hashes the superseded manifest. Validate the final launch receipt against the
  downstream binding; do not edit an old sealed manifest underneath a live cohort to make it match.
  A persistent `PathExists=` watcher remains true after its trigger file appears. If its service
  later skips on a negative receipt condition, systemd can retrigger it in a tight loop. Make the
  successful launch path stop or disable that watcher, or otherwise give it a proven one-shot
  lifecycle; verify it is inactive after the launch receipt is durable. A path unit normally has
  `SubState=waiting` before its event but reports `SubState=running` while its triggered service is
  active, so an invocation-bound launcher may accept either authenticated active state; do not
  mistake `running` for a foreign watcher. Bind its invocation ID, put a conservative
  `StartLimitIntervalSec`/`StartLimitBurst` on the triggered service so a failed persistent event
  cannot spin, and prove the actual triggered lifecycle before arming production. After an
  authenticated stop, an installed path normally remains `loaded/inactive/dead`, while a collected
  transient path may already be `not-found/inactive/dead`; accept either terminal shape rather than
  waiting forever for a collected unit to reappear.
- Preflight the host's VMA ceiling for mmap-backed historical account stores as part of admission.
  Compare `vm.max_map_count` with live worker map counts and the snapshot/store-file baseline, and
  leave credible growth headroom for the full replay. Some legacy Solana AppendVec code logs an
  mmap failure through an uninitialized logger and then calls `exit(1)`, which can otherwise look
  like a silent worker EOF. When the limit is inadequate and host policy permits, raise it to a
  documented persistent value before launch and record the effective value; this is independent
  of free RAM, CPU, and disk checks.
- Reclaim local disk proactively when admission is constrained, but only from positively
  reproducible caches, obsolete scratch generations, and terminal partial outputs whose sealed
  evidence proves they cannot pass the full-epoch gates. Resolve each deletion target to an exact
  canonical path, prove no live process or staged unit references it, preserve the manifest or
  receipt needed to reproduce it, and record the before/after evidence. Build caches, obsolete
  snapshots, fully generation-pinned snapshot caches, and proven superseded partial outputs may be
  removed after those checks; required inputs, active scratch, resumable state, unsealed diagnostic
  evidence, checkpoints, receipts, complete archives awaiting gates/publication, and all R2 objects
  must remain untouched. Record both the target's measured physical bytes and filesystem free space,
  but do not equate their delta while live jobs allocate or files share extents.
  Treat command-line and mmap references as exact paths or descendants, not raw string prefixes:
  a live `scratch-store8` sibling is not a reference to an empty completed `scratch` tree. Use the
  repository retirement scanners' path-boundary checks, and still fail closed on any genuine
  command, map, cwd/root/exe, or descriptor reference beneath the exact target.
- Retire a generation-bound download cache only after its root-owned verification receipt still
  binds the exact immutable cache identity, the consuming scan has durably recorded `complete`
  against that same identity, and every explicitly bound producer/verifier/consumer invocation is
  inactive with clean terminal success. Scan `/proc` commands, maps, cwd/root/exe links, and file
  descriptors immediately before deletion. Fsync a no-clobber deletion intent first, unlink only
  the one cache file, fsync its parent directory, preserve scan results and any retained matches,
  then fsync a completion receipt. An intent-only interruption must be safely resumable and must
  reject a replacement file at the same path. Any permission-denied `/proc` command, map, link, or
  descriptor inspection is a failed gate, not evidence that the process has no reference. This is
  local scratch reclamation only and never
  authorizes an R2 mutation. A garbage-collected transient download unit may be proven by the exact
  clean terminal state and invocation embedded in its root-owned cache receipt; still require live
  terminal checks for explicitly bound verifier/consumer units that remain loaded.
- For adaptive ranges, start the controller with `--r2-receipt-directory "$HOME/.jetstreamer-private/r2-receipts" --r2-bucket BUCKET`. The controller accepts receipts from that exact bucket only for bytes already bound by its root-owned local completion attestation; R2 can replace local storage, but can never establish initial completion.
- Start the current Horizon verification plugin as soon as each local archive is available. R2 work may run concurrently with replay of other epochs. An archive is eligible for upload only after its full and current-plugin receipts bind the same archive SHA-256. It is not eligible for local retirement until both adjacent-boundary receipts bind that digest as well. Preserve the canonical lowercase coreutils sidecar format: `<64 hex>  epoch-N.jet\n`.
- When a verifier starts on the first archive of a long continuous cohort, size its oneshot
  `TimeoutStartSec` for the remaining replay plus final validation, not just one archive scan. While
  waiting for later sidecars, dispatch the plugin verifier through
  `scripts/watch_horizon_plugin_progressive.sh` (or an equivalently sealed receipt-aware watcher):
  a matching exact receipt should skip the already-verified archive without another whole-file
  hash on every poll, while each newly dispatched verifier must still hash before and after its
  plugin scan. Preserve the receipt-producing verifier's script hash in the upload gate; the outer
  watcher does not replace that evidence identity.
- Start the progressive R2 uploader alongside those verifiers when the first complete archive pair
  appears, so it can publish each epoch immediately after that epoch's exact full and plugin
  receipts arrive. The uploader must remain receipt-gated per epoch and must not use local deletion
  merely because it is long-lived. Do not order the uploader `After=` the cohort-long verifier
  services or require all cohort sidecars as unit conditions: either mistake serializes publication
  behind the last epoch. Systemd dependency directives such as `After=` and `Requires=` cannot be
  removed by an empty drop-in assignment, so replace the complete unit when removing legacy
  dependencies. Authenticate the triggering producer invocation, launch verifiers and uploader as
  one recorded transition, and stop the persistent path watcher after a successful launch.
- Report single-job latency and fleet completion cadence separately. Historical slot density varies,
  so compare transaction and account-update throughput as well as slots per second. For live memory,
  CPU, disk, or VMA experiments, use incremental pre/post windows after a settling interval rather
  than cumulative rates; preserve invocation identity and do not restart a healthy replay merely to
  obtain a cleaner sample.
- Time replay terminal, archive close/fsync, each full validation, plugin verification, boundary
  verification, upload, remote readback, and sidecar publication as distinct phases. Parallelize or
  overlap independent work when safe, but do not collapse a producer scan and an independent durable
  reread into one trust event or remove a verification gate merely to improve end-to-end latency.
- Before launching an independent focused-qualification validator, read the durable segment
  manifest and bind every expectation to it. In particular, pass its `output_slot_start`; do not
  substitute `bootstrap_slot + 1`. A predecessor snapshot may warm up before the target epoch, so
  replay can begin at `bootstrap_slot + 1` while recorded output begins at the epoch boundary.
  Independently check that `terminal_slot - output_slot_start + 1` equals the manifest's
  `output_slot_count`, and launch a distinct non-restarting attempt if an expectation was wrong.
- Before spending a full archive scan on the current per-epoch plugin, also compare the segment
  manifest's output start, terminal slot, and count with that epoch's complete canonical slot
  range. A focused checkpoint artifact that starts at the epoch boundary but stops even one slot
  before the epoch end can pass its own sealed partial-range validator while the current plugin
  must reject its epoch notification. Preserve that artifact as diagnostic evidence, but do not
  launch the production plugin gate, create a plugin receipt, canonicalize it, or publish it. Run
  the plugin against such an artifact only when the explicit purpose is to exercise and record the
  expected rejection path; mark that run diagnostic-only and give it no publication authority.
- Before scheduling or launching a focused qualification, separately prove that the exact immutable
  worker's own snapshot and entry bounds admit the requested terminal slot. A parent focused planner
  may intentionally describe a wider search envelope than the currently deployed worker accepts;
  planner routing alone is not launch authority. If the terminal lies beyond the worker's internal
  bound, defer that lane until a reviewed source change extends the bound, range tests and the
  relevant suites pass, the change is committed and pushed to every required SSH remote, and a new
  immutable worker digest is bound in the launch manifest. Never silently substitute a nearby epoch
  or weaken the worker guard.
- Treat a source-obscured confirmed block that omits PoH tick entries as a compatibility gap whenever
  its finalized parent relationship crosses one or more skipped slots. Repeating the visible block's
  final blockhash across multiple tick/block boundaries changes RecentBlockhashes, SlotHashes, and the
  frozen Bank hash even when the visible final blockhash and every transaction are canonical. Require
  trusted intermediate PoH boundary hashes or an independently qualified exact-state recovery; do not
  invent, interpolate, or repeat them. Finalized `getBlocks`/`getBlock` evidence and a successful
  canonical vote whose tip hash disagrees with the reconstructed SlotHashes value can prove this
  divergence, but RPC block metadata does not recover the missing intermediate PoH hash and therefore
  cannot by itself authorize a runtime-route promotion or publication.
- Before limiting recovery to the first observed hash mismatch, census the complete source-obscured
  slot interval with finalized `getBlocks` evidence and identify every parent jump. A single interval
  can contain many disjoint skipped-slot runs. For each run, the first visible post-gap block can carry
  `skipped_slots + 1` tick boundaries, so recover and validate original data shreds for every such
  post-gap block, not only the skipped slot or the first failing block. Seal the raw block-list response,
  the derived gap list, and the expected total hidden/boundary counts before starting the bounded ledger
  scan. Independently bind finalized `getBlock` parent slots and final blockhashes for all targets; they
  validate the recovered final boundary but still do not supply the hidden intermediate hashes.
- For an exact-state recovery of such a gap, search immutable, generation-pinned ledger backups rooted
  before the boundary for the blockstore `data_shred`, `code_shred`, and `meta` column families. When
  the backup is too large for the current admission envelope, stream its compressed tar one SST at a
  time with explicit per-SST, total-retention, memory, and runtime bounds; retain only matching SSTs
  and durably checkpoint progress. Scan every SST before choosing a value: an LSM backup can contain
  multiple internal versions and tombstones for the same big-endian `(slot, shred_index)` key, so a
  partial scan or first match is not live-state evidence. Select the highest internal sequence only
  after the complete generation-bound scan, honor deletions, and decode shred payloads with the exact
  pinned runtime's header and bincode layout. Require the recovered entry/tick chain to reproduce the
  visible canonical final blockhash and the missing intermediate boundary before changing a runtime
  route; the ordinary checkpoint, plugin, boundary, and publication gates still apply independently.
  Treat the resulting boundary set as a sealed runtime input, not as ambient diagnostic state. Bind an
  absolute regular-file path and exact lowercase SHA-256 in the immutable qualification launch; open
  without following symlinks, reject group/other-writable or changing files, constrain schema and slot
  scope, and cap its size before parsing. At each parent jump, require exactly `slot - parent_slot`
  boundaries at the runtime's canonical tick ordinals and require the final recovered hash to equal the
  independently preserved visible blockhash before mutating the Bank. Record the recovery digest in the
  launch evidence, and keep the new worker/runtime route unqualified until the normal exact checkpoint
  and plugin gates pass.
  If the sealed collector output is beneath a root-private evidence tree that the unprivileged worker
  cannot traverse, do not relax that tree's permissions or grant the worker a broad DAC capability.
  Revalidate the non-sensitive boundary bytes and collector receipt, then publish them no-clobber into
  a separately fsynced, root-owned immutable bundle with a readable file mode and non-writable directory;
  bind the worker to that deployed path and digest while leaving the original private evidence untouched.

## Deliver

Build once with `cargo build --release -p jetstreamer-r2`. Store receipts outside the public archive directory, normally at `$HOME/.jetstreamer-private/r2-receipts`.

For all complete local pairs:

```sh
target/release/jetstreamer-r2 sync "$HORIZON_DIR" \
  --receipt-directory "$HOME/.jetstreamer-private/r2-receipts" \
  --legacy-part-size-mib 5
```

Add `--epochs START-END` for an explicit range. Add `--delete-local` only after checking the matching plugin receipt and that no active controller or verifier still requires those local paths. Existing deployed controllers may use the public directory as their completion ledger; defer retirement for their managed range until that controller finishes or is deliberately upgraded to understand R2 receipts.

After an adaptive controller has been deliberately deployed with `--r2-receipt-directory`, its completed cohorts no longer need a matching `--defer-epochs` entry: the progressive uploader may retire them after all normal publication gates pass. Never remove the defer for a controller that lacks this flag.

For progressive retirement, pass both receipt directories and explicitly defer every active
controller range:

```sh
scripts/sync_horizon_r2_progressive.py \
  target/release/jetstreamer-r2 "$HORIZON_DIR" \
  "$HOME/.jetstreamer-private/r2-receipts" START END \
  --delete-local \
  --local-mutation-lock "$HOME/.jetstreamer-private/r2-receipts/local-mutation.lock" \
  --full-receipt-directory "$HOME/.jetstreamer-private/final-audits/receipts" \
  --full-verifier-sha256 FULL_VERIFIER_SHA256[,TRANSITION_SHA256] \
  --full-verifier-script-sha256 FULL_SCRIPT_SHA256 \
  --plugin-receipt-directory "$HOME/.jetstreamer-private/plugin-audits/receipts" \
  --plugin-pipeline-sha256 PIPELINE_SHA256[,TRANSITION_SHA256] \
  --plugin-verifier-script-sha256 SCRIPT_SHA256 \
  --boundary-receipt-directory "$HOME/.jetstreamer-private/boundary-audits/receipts" \
  --boundary-verifier-sha256 BOUNDARY_SHA256[,TRANSITION_SHA256] \
  --boundary-verifier-script-sha256 BOUNDARY_SCRIPT_SHA256 \
  --defer-epochs ACTIVE_START-ACTIVE_END
```

Before arming any generated upload service, audit the effective wrapper without executing it using
`scripts/audit_horizon_upload_wrapper.py`. Bind every configured `--*-sha256` option to the exact
immutable binary or script it names, require the binding set to cover every hash option exactly
once, and bind every configured full/plugin/boundary `--*-receipt-directory` to the exact verifier
state directory that produces it. Require each configured receipt directory to be that state
directory's `receipts/` child; pointing the uploader at the state-directory parent makes valid
verifier receipts invisible and can leave publication waiting forever even when every hash is
correct. Preserve the JSON result with the deployment evidence. Re-derive these hashes from the
deployed files even when a wrapper and deployment manifest already agree with each other: copied or
mistyped evidence can be internally consistent yet reject the verifier's correct receipt only after
a long replay finishes. Repeat this audit after any wrapper, verifier, plugin, or immutable package
transition and confirm the effective systemd `ExecStart` points at the audited wrapper.

The binary must fail closed on an existing remote mismatch by default. If the user explicitly authorizes replacement, `--overwrite-existing` replaces both the archive and sidecar, performs a fresh whole-object SHA-256 readback, and atomically replaces the private receipt. A native R2 checksum, when present, must report type `COMPOSITE` and match local part-SHA-256 evidence. Objects without native R2 checksums require a matching reconstructed ETag, canonical sidecar, and, unless explicitly trusted as legacy, a successful whole-object SHA-256 readback.

If a trusted legacy upload has a canonical sidecar but its archive object is missing, use
`--repair-orphaned-archive` only after the user authorizes repair. The command requires the remote
sidecar to match the verified local sidecar byte-for-byte, creates the archive with append-only
preconditions, performs a whole-object SHA-256 readback, and leaves the existing sidecar untouched.
It never admits a mismatching sidecar and is mutually exclusive with `--overwrite-existing`.

Use `jetstreamer-r2 restore` when an ordered-chain or boundary audit needs an archive that has already been retired locally. Restore only into an explicit scratch directory, require `--epochs` and the private `--receipt-directory`, and let the Rust command enforce the recorded length, ETag, whole-file SHA-256, canonical sidecar, resumable conditional ranged GET, and no-clobber publication. Never download directly over a public Horizon archive.

Use `jetstreamer-r2 retire-local` only when local retirement has already been authorized by the
normal boundary gates or by an explicit user-approved backup/emergency policy. It makes no R2
mutation: it requires a durable receipt with whole-object SHA-256 evidence, rehashes the local
archive and ETag, re-observes the remote archive and sidecar, then removes the local sidecar before
the archive. It requires an explicit `--epochs START-END`; never infer a destructive selection from
directory discovery. The command deliberately does not decide policy, so never treat its integrity
checks as a substitute for the ordinary full/plugin/boundary gates unless the user explicitly
authorized that narrower local-retirement exception.

For an explicit user-approved filesystem-floor exception, use
`scripts/retire_horizon_at_reserve.py` as the policy wrapper and give it the exact same private
`--local-mutation-lock` as the progressive uploader. The wrapper must recheck actual free space
after acquiring the lock, validate that the public directory contains only complete canonical
pairs inside the authorized range, call `retire-local` for one explicit epoch at a time, and stop
as soon as the floor is recovered. A newly arriving pair must not broaden the selected set. Treat
no eligible pair below the floor, a partial pair, an out-of-range entry, or failure to recover the
floor after exhausting pairs as a hard failure. Before enabling its timer, prove one sandboxed
above-floor invocation performs no archive hashing, no local removal, and no R2 mutation.

When a restore or audit runs in a systemd sandbox with `ProtectHome=read-only` and the scratch
directory itself named in `ReadWritePaths=`, clean the directory's contents but keep the empty
scratch directory. Removing that directory requires write access to its parent and fails with
`EROFS`; under `Restart=on-failure` this can otherwise trigger an unnecessary restore loop. Create
the mount-point directory before starting the sandbox, validate its exact canonical path before
cleanup, and constrain deletion to that directory with `find "$scratch" -xdev -mindepth 1 -delete`.

Do not use `--legacy-etag-only` unless the user has explicitly established that a pre-existing object is trusted. Newly uploaded archives always require native R2 SHA-256 evidence or whole-object SHA-256 readback.

## Operate safely

- Never use `sync -f PATH` or `sync --file-system PATH` to make a receipt durable on a filesystem
  containing live replay scratch. GNU `sync -f` calls `syncfs(2)` and flushes the entire filesystem;
  continuously dirtied mmap-backed AccountsDb pages can keep it blocked indefinitely and impose
  avoidable writeback pressure on every producer. Durable receipt writers must fsync the receipt's
  own file descriptor and then its containing directory descriptor, preferably inside the Rust or
  Python writer that performs the atomic rename. Do not substitute a filesystem-wide flush.
- Run multi-check shell observations with fail-fast semantics, normally `set -euo pipefail`, and
  capture each asserted condition explicitly in the durable observation. Never let a later
  successful status or presence probe mask an earlier failed absence, ownership, hash, or unit-state
  assertion through the shell's final-command exit status. When correcting a mistaken observation,
  preserve the original immutable receipt, seal a separate correction that binds its exact digest,
  state the authoritative evidence and gate impact, and fix the human-readable summary rather than
  rewriting history.
- Private qualification, admission, guard, and publication receipt directories may deliberately be
  root-only. An unprivileged `test -e`, `stat`, or file read can therefore look like absence even
  when the receipt exists. Before declaring a required receipt missing, inspect the parent access
  controls and repeat the existence, metadata, digest, and JSON checks through the intended
  privileged reader. Treat an actual unreadable or invalid receipt as a failed gate; never weaken
  its ownership or mode merely to make an unprivileged probe pass.
  A root systemd service with an empty capability bounding set still cannot traverse a user-owned
  mode-0700 ancestor: UID 0 needs `CAP_DAC_OVERRIDE` for that traversal. When such a sandbox must
  read root-owned sealed evidence beneath the private root, retain only that capability (including
  the effective or ambient set as required by the unit), keep the target mount read-only except for
  exact receipt directories, and prove the read path in an equivalently sandboxed transient unit.
  Do not relax private-directory permissions to work around the sandbox.
  A root launcher that validates private evidence and then drops to an unprivileged scanner needs
  CAP_SETGID and CAP_SETUID effective at the drop point in addition to any traversal capability.
  `CapabilityBoundingSet=` limits what is possible but does not by itself prove those capabilities
  are effective; bind the same minimal set through `AmbientCapabilities=` when required and run an
  end-to-end sandbox proof that observes the unprivileged output owner before arming production.
  For a long `Type=oneshot` verifier use `TimeoutStartSec=` as its execution bound;
  `RuntimeMaxSec=` is ignored for oneshot units.
- Use SSH URLs for Git network operations. Select a currently loaded keychain agent socket for the
  active login session by verifying it with `ssh-add -l`; never hard-code an ephemeral socket path
  from an earlier session. Check the inherited environment for required service credentials before
  sourcing interactive shell startup files in a detached command. A startup file may attach to a
  stale empty agent and return nonzero when it cannot prompt for the key passphrase even though a
  different current-login socket is healthy. After any shell initialization, rediscover all
  candidate sockets and select only one whose `ssh-add -l` succeeds and lists an identity; never
  treat the socket exported by the startup file itself as proof that Git authentication is ready.
- Never delete from R2, including during cleanup or retries.
- Never use `--overwrite-existing` without both positive evidence that the existing remote `.jet` is erroneous and explicit user authorization for its replacement.
- Upload the sidecar only after the complete `.jet` has passed remote readback and local source revalidation; the matching sidecar is the readiness marker.
- Never remove a local archive before its durable receipt exists and both remote objects have been re-observed.
- Refuse to begin upload without exact full and current-plugin receipts. Refuse local retirement without both adjacent-boundary receipts as well.
- Do not treat a sidecar alone as proof that the archive was uploaded.
- Resume idempotently from R2 inventory and private receipts after interruption.
- Leave at least 30 minutes between routine status checks for each running job or
  cohort. Diagnose an observed failure immediately instead of waiting for the next
  interval.
