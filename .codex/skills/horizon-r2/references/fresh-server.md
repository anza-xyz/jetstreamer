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
   shell startup file inside a detached systemd producer.
4. Build from that recorded commit and pin the hashes of the producer, full verifier, current
   plugin pipeline, boundary verifier, and their launcher scripts before creating a production
   service. Deploy immutable binaries and manifests into root-owned, non-writable paths; do not run
   a long replay from `target/` in a mutable worktree.

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
   root checkpoint. A candidate must reproduce those checkpoints before it can generate a
   publishable archive.
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
