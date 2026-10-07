---
name: horizon-r2
description: Generate, verify, upload, and safely retire Horizon epoch archives using Cloudflare R2 as the durable inventory. Use when filling missing Horizon epochs, syncing verified .jet archives to R2, or resuming a specified epoch range.
---

# Horizon R2

Treat R2 as durable storage. Never call `DeleteObject` or `DeleteObjects`. Aborting an incomplete multipart upload is allowed. Preserve an existing object by default. Overwrite an existing `.jet` only when positive verification evidence identifies that remote archive as erroneous and the user explicitly authorizes its replacement; a mismatch alone does not establish which copy is wrong.

Use the repository's `jetstreamer-r2` binary for upload and remote proof. Do not reimplement its multipart protocol in shell or Python. R2's S3 `UploadPart` currently rejects its advertised SHA-256 header, so the binary uses R2-enforced `Content-MD5`, checks the multipart ETag independently, and reads the completed remote object back while computing its whole-file SHA-256. It must fully upload, complete, read back, and revalidate the `.jet` before it uploads the canonical `.sha256` sidecar. It then reads the sidecar back and fsyncs a private receipt before optional local retirement. Treat presence of the matching sidecar as the remote readiness commit marker; an archive without one is staged, and a sidecar without an archive or whose digest does not match the archive is an error. If R2 begins returning native composite SHA-256 evidence, the binary validates and prefers it.

For a long-running range, use `scripts/sync_horizon_r2_progressive.py` to discover newly completed local pairs and invoke the Rust uploader serially. Do not run two progressive uploaders over overlapping ranges. The Python process never implements remote integrity checks or unlinks files itself. Upload requires digest-matching full-verification and current-plugin receipts. Optional local retirement additionally requires both adjacent-boundary receipts. This separation lets disjoint servers publish complete archives so a retired neighbor can be restored from R2 to close a cross-host boundary without circularly weakening retirement. The orchestrator requires the exact approved binary and verifier-script SHA-256 values, accepts explicit comma-separated binary-hash allowlists during a pinned verifier transition, honors every `--defer-epochs` range for retirement, and then delegates remote verification plus optional local retirement to the Rust uploader.

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
- Use `HORIZON_DIR` when set; otherwise use `$HOME/horizon`. Keep only `epoch-N.jet` and `epoch-N.jet.sha256` in that public directory.
- Credentials are `HORIZON_S3_ENDPOINT`, `HORIZON_ACCESS_KEY_ID`, and `HORIZON_SECRET_ACCESS_KEY`. Check only that they exist; never print their values. The endpoint path names the bucket.
- With an explicit user range, restrict ordinary generation, verification, upload, and cleanup to
  that range; the only permitted out-of-range work is the isolated verification tail below.
- Treat the explicit range end as the publication boundary even when a sealed verification cohort
  must replay farther to reach its terminal root. Keep every out-of-range verification-tail archive
  in private storage, give it no canonical sidecar, and exclude it from the public Horizon directory
  and R2. Deeply validate the complete sealed cohort before transactionally importing only the
  requested contiguous prefix through the repository's bounded recovery option.
- Without a range, inventory canonical R2 pairs first. Upload any complete local pairs missing from R2, then begin with the lowest missing epoch supported by the repository's compatibility manifests. Continue in bounded cohorts as resources permit.
- An R2 epoch is complete only when both `epoch-N.jet` and `epoch-N.jet.sha256` exist and agree with verified local evidence. A checksum-only or archive-only epoch is incomplete.

Remote object pairs are the shared inventory between servers; private receipts and controller
state are host-local evidence and must not be copied to make another host believe work completed.
Give concurrently operating hosts disjoint explicit epoch ranges. Existing R2 objects are never a
lease: if ranges overlap accidentally, stop the duplicate producer rather than racing publication.

## Produce and verify

- Use the repository's sealed historical replay/controller path and automatic slot-range runtime selection. Do not invent a compatibility override.
- Run generation and long-lived verification/upload watchers in persistent systemd units so loss of the interactive session cannot kill them. Use `Restart=on-failure` with a bounded retry delay only when every invocation gets isolated diagnostic output and scratch, or a proven launcher preserves the failed invocation's artifacts before opening the next output. A replay normally creates or truncates its `.jet` at startup, so never let an automatic retry reuse the same diagnostic archive path. Use a fail-closed non-restarting unit and relaunch manually after evidence capture when attempt isolation is unavailable. Do not use `Restart=always`, because successful completion must remain terminal. Respect unrelated jobs and configured RAM/disk reserves.
- Before starting a recurring status timer, initialize its last-observed state to the replay's
  authoritative launch time and validate the monitor once manually. A timer that exists without
  this state is not monitoring the replay; diagnose monitor failures without restarting a healthy
  producer. After any `systemctl daemon-reload` while monotonic monitor timers are active, re-observe
  every timer's next deadline: systemd can move a pending `OnActiveSec` deadline relative to the
  reload. If that delays an already-due check, run only the oneshot monitor immediately and confirm
  that `OnUnitActiveSec` has re-anchored the next interval; never restart the producer to repair its
  monitor schedule.
- When launching into a fresh isolated diagnostic-output directory, ensure it contains the verified
  same-cluster genesis expected by the loader. A cached snapshot does not imply the genesis is
  present. Preseed only from digest-bound retained evidence and verify the copied digest and
  ownership; otherwise validate noninteractive gcloud authentication for the service identity.
- Failed-replay scratch cleanup is authorized by default for this workflow. After confirming the failure and capturing the evidence needed to diagnose or reproduce it, stop the service and retry path, verify that no live process or staged relaunch references the exact scratch path, and delete that failed run's scratch immediately instead of allowing failures to accumulate. Preserve diagnostic output, partial archives, replay state, logs, checkpoints, manifests, receipts, and any snapshot still needed for diagnosis, lineage, restart, or a staged epoch outside the scratch tree. Never apply this cleanup rule to a controlled stop that can genuinely resume in place. If a relaunch demonstrably starts from the bootstrap in a new isolated runtime generation with no resume cursor, verify the new worker's exact generation and reclaim older unreferenced generations promptly; preserving them does not make that replay resumable.
- Do not raise replay concurrency from low CPU or RAM utilization alone. Snapshot extraction and
  historical account state can consume hundreds of GiB per worker; require the controller's
  configured per-worker disk admission budget and measured filesystem growth to leave the reserve
  intact before launching another cohort. If a manual canary set would violate that gate, stop the
  newest units and preserve their private run directories for later resumption.
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
  fan-out can amplify each other. Before the next immutable generation, use a bounded same-snapshot
  A/B to select an explicit value appropriate to the worker's actual CPU quota, comparing
  throughput, physical bytes, file count, and VMAs; measure commit-wave changes as a separate A/B.
  Pin the chosen environment in the launch manifest and repeat the normal root/plugin qualification;
  never change it underneath a live replay.
- Preflight the host's VMA ceiling for mmap-backed historical account stores as part of admission.
  Compare `vm.max_map_count` with live worker map counts and the snapshot/store-file baseline, and
  leave credible growth headroom for the full replay. Some legacy Solana AppendVec code logs an
  mmap failure through an uninitialized logger and then calls `exit(1)`, which can otherwise look
  like a silent worker EOF. When the limit is inadequate and host policy permits, raise it to a
  documented persistent value before launch and record the effective value; this is independent
  of free RAM, CPU, and disk checks.
- Reclaim local disk proactively when admission is constrained, but only from positively
  reproducible caches and obsolete scratch generations. Resolve each deletion target to an exact
  canonical path, prove no live process or staged unit references it, preserve the manifest or
  receipt needed to reproduce it, and record the before/after evidence. Build caches, obsolete
  snapshots, and fully generation-pinned snapshot caches may be removed after those checks;
  required inputs, active scratch, resumable state, diagnostic evidence, checkpoints, receipts,
  public archives, and all R2 objects must remain untouched.
- For adaptive ranges, start the controller with `--r2-receipt-directory "$HOME/.jetstreamer-private/r2-receipts" --r2-bucket BUCKET`. The controller accepts receipts from that exact bucket only for bytes already bound by its root-owned local completion attestation; R2 can replace local storage, but can never establish initial completion.
- Start the current Horizon verification plugin as soon as each local archive is available. R2 work may run concurrently with replay of other epochs. An archive is eligible for upload only after its full and current-plugin receipts bind the same archive SHA-256. It is not eligible for local retirement until both adjacent-boundary receipts bind that digest as well. Preserve the canonical lowercase coreutils sidecar format: `<64 hex>  epoch-N.jet\n`.
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

The binary must fail closed on an existing remote mismatch by default. If the user explicitly authorizes replacement, `--overwrite-existing` replaces both the archive and sidecar, performs a fresh whole-object SHA-256 readback, and atomically replaces the private receipt. A native R2 checksum, when present, must report type `COMPOSITE` and match local part-SHA-256 evidence. Objects without native R2 checksums require a matching reconstructed ETag, canonical sidecar, and, unless explicitly trusted as legacy, a successful whole-object SHA-256 readback.

If a trusted legacy upload has a canonical sidecar but its archive object is missing, use
`--repair-orphaned-archive` only after the user authorizes repair. The command requires the remote
sidecar to match the verified local sidecar byte-for-byte, creates the archive with append-only
preconditions, performs a whole-object SHA-256 readback, and leaves the existing sidecar untouched.
It never admits a mismatching sidecar and is mutually exclusive with `--overwrite-existing`.

Use `jetstreamer-r2 restore` when an ordered-chain or boundary audit needs an archive that has already been retired locally. Restore only into an explicit scratch directory, require `--epochs` and the private `--receipt-directory`, and let the Rust command enforce the recorded length, ETag, whole-file SHA-256, canonical sidecar, resumable conditional ranged GET, and no-clobber publication. Never download directly over a public Horizon archive.

When a restore or audit runs in a systemd sandbox with `ProtectHome=read-only` and the scratch
directory itself named in `ReadWritePaths=`, clean the directory's contents but keep the empty
scratch directory. Removing that directory requires write access to its parent and fails with
`EROFS`; under `Restart=on-failure` this can otherwise trigger an unnecessary restore loop. Create
the mount-point directory before starting the sandbox, validate its exact canonical path before
cleanup, and constrain deletion to that directory with `find "$scratch" -xdev -mindepth 1 -delete`.

Do not use `--legacy-etag-only` unless the user has explicitly established that a pre-existing object is trusted. Newly uploaded archives always require native R2 SHA-256 evidence or whole-object SHA-256 readback.

## Operate safely

- Use SSH URLs for Git network operations. Select a currently loaded keychain agent socket for the
  active login session by verifying it with `ssh-add -l`; never hard-code an ephemeral socket path
  from an earlier session.
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
