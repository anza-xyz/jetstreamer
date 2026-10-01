---
name: horizon-r2
description: Generate, verify, upload, and safely retire Horizon epoch archives using Cloudflare R2 as the durable inventory. Use when filling missing Horizon epochs, syncing verified .jet archives to R2, or resuming a specified epoch range.
---

# Horizon R2

Treat R2 as durable storage. Never call `DeleteObject` or `DeleteObjects`. Aborting an incomplete multipart upload is allowed. Preserve an existing object by default; use the uploader's explicit overwrite mode only after the user authorizes replacement of mismatching data.

Use the repository's `jetstreamer-r2` binary for upload and remote proof. Do not reimplement its multipart protocol in shell or Python. R2's S3 `UploadPart` currently rejects its advertised SHA-256 header, so the binary uses R2-enforced `Content-MD5`, checks the multipart ETag independently, and reads the completed remote object back while computing its whole-file SHA-256. It publishes and reads back the canonical `.sha256` sidecar only after the archive passes those checks and local source revalidation, then fsyncs a private receipt before optional local retirement. Treat the sidecar as the remote completion marker; an archive without one is staged, and a sidecar without an archive is an error. If R2 begins returning native composite SHA-256 evidence, the binary validates and prefers it.

For a long-running range, use `scripts/sync_horizon_r2_progressive.py` to discover newly completed local pairs and invoke the Rust uploader serially. Do not run two progressive uploaders over overlapping ranges. The Python process never implements remote integrity checks or unlinks files itself. Upload and optional local retirement both require digest-matching full-verification, current-plugin, and both adjacent-boundary receipts. It requires the exact approved binary and verifier-script SHA-256 values, accepts explicit comma-separated binary-hash allowlists during a pinned verifier transition, honors every `--defer-epochs` range for retirement, and then delegates remote verification plus optional local retirement to the Rust uploader.

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
- With an explicit user range, restrict generation, verification, upload, and cleanup to that range.
- Without a range, inventory canonical R2 pairs first. Upload any complete local pairs missing from R2, then begin with the lowest missing epoch supported by the repository's compatibility manifests. Continue in bounded cohorts as resources permit.
- An R2 epoch is complete only when both `epoch-N.jet` and `epoch-N.jet.sha256` exist and agree with verified local evidence. A checksum-only or archive-only epoch is incomplete.

Remote object pairs are the shared inventory between servers; private receipts and controller
state are host-local evidence and must not be copied to make another host believe work completed.
Give concurrently operating hosts disjoint explicit epoch ranges. Existing R2 objects are never a
lease: if ranges overlap accidentally, stop the duplicate producer rather than racing publication.

## Produce and verify

- Use the repository's sealed historical replay/controller path and automatic slot-range runtime selection. Do not invent a compatibility override.
- Run generation in a persistent systemd unit so loss of the interactive session cannot kill it. Respect unrelated jobs and configured RAM/disk reserves.
- For adaptive ranges, start the controller with `--r2-receipt-directory "$HOME/.jetstreamer-private/r2-receipts" --r2-bucket BUCKET`. The controller accepts receipts from that exact bucket only for bytes already bound by its root-owned local completion attestation; R2 can replace local storage, but can never establish initial completion.
- Start the current Horizon verification plugin as soon as each local archive is available. R2 work may run concurrently with replay of other epochs, but an archive is not eligible for upload or local retirement until its full, current-plugin, and both adjacent-boundary receipts bind the same archive SHA-256. Preserve the canonical lowercase coreutils sidecar format: `<64 hex>  epoch-N.jet\n`.

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

Use `jetstreamer-r2 restore` when an ordered-chain or boundary audit needs an archive that has already been retired locally. Restore only into an explicit scratch directory, require `--epochs` and the private `--receipt-directory`, and let the Rust command enforce the recorded length, ETag, whole-file SHA-256, canonical sidecar, resumable conditional ranged GET, and no-clobber publication. Never download directly over a public Horizon archive.

Do not use `--legacy-etag-only` unless the user has explicitly established that a pre-existing object is trusted. Newly uploaded archives always require native R2 SHA-256 evidence or whole-object SHA-256 readback.

## Operate safely

- Never delete from R2, including during cleanup or retries.
- Never use `--overwrite-existing` without explicit user authorization for the affected range.
- Never remove a local archive before its durable receipt exists and both remote objects have been re-observed.
- Refuse to begin upload without exact full, current-plugin, and adjacent-boundary receipts.
- Do not treat a sidecar alone as proof that the archive was uploaded.
- Resume idempotently from R2 inventory and private receipts after interruption.
- Monitor long jobs at 20-30 minute intervals unless a failure needs immediate work.
