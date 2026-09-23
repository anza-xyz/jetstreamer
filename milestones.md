# Milestones

Major project results, newest first. Each entry records the UTC date and the
code revision that produced the result.

## 2026-09-23: Solana v1.3.23 qualified across the epoch-126 divergence

- Commit: `099f5ef2633d9c911cdab8afde8b4a42e5517638`.
- Replayed slots 54,676,313 through 54,684,686 with the exact upstream
  v1.3.23 runtime, including the stake initialization at slot 54,681,962 that
  v1.4.25 rejects.
- Matched canonical accounts hash
  `DvbSn4RPv615yNpsFCuYSajL3zGy5oK57XDxUwKi36gv` and bank hash
  `BpiynHZekZybrfotnE8i5Ua6EL65uQz97rFL18NCjwZj` at slot 54,684,686.
- A source scan found 42 successful 4,008-byte stake initializations through
  slot 55,725,865 and none after the epoch-129 feature boundary at slot
  55,728,000. Epochs 101-128 now route to v1.3.23; v1.4.25 starts at epoch 129.

## 2026-09-23: Archive acceptance made 15.8x faster

- Commit: `72a52e8dd65bc46bd155c610d623d2729f0190c3`.
- On the verified 380.1 MiB epoch-0 archive, full decoding took 2.834 seconds
  after the change, compared with 44.816 seconds through the previous semantic
  hashing path. This is a 15.812x speedup in archive acceptance.
- Removed a SHA-256 pass over the reconstructed semantic stream whose result
  was discarded because there was no trusted digest to compare against.
  Validation still reconstructs every account-data byte, checks stored bucket
  integrity and the PoH chain, validates framing and provenance, then SHA-256
  binds the exact archive inode used for publication.
- The optimized node passed all 361 `jetstreamer-node` tests and strict Clippy
  checks before deployment to the epoch 101-200 production queue.

## 2026-09-23: Exact Solana v1.5.5 qualified for epochs 148-149

- Commit: `6e4be7a889ebc62e42ba3b372c6b5c3d2a637350`.
- Replayed 13,102 slots from the canonical slot-63,935,659 snapshot through
  trusted checkpoint slot 63,948,761 using the exact upstream v1.5.5 runtime.
- Matched canonical accounts hash
  `64X5VqKPzxqsTu5gTd9aZRi7nE7PtHZGqkmmg65ZcVEz` and bank hash
  `DUo7Vw9hPD4C2yjpyPmJNuZCs2cKdbYtrZThzkjnBtAF`, with capitalization
  `488586234469265886`, transaction count `10817258255`, and tick height
  `4092720768`.
- Added automatic slot-range routing for epochs 148-149 while retaining
  v1.5.19 only as an unassigned comparison candidate.

## 2026-09-21: Mainnet epochs 0-100 fully verified

- Commits:
  - `4439b9d611fb476236fd69e6d3150a587d4042f4` for the full archive verifier
    and audit runner.
  - `8f3e136f001963546eb7eebb498848793ffa01fa` for the Horizon verification
    plugin.
- Generated 101 contiguous Horizon archives for mainnet epochs 0 through 100,
  totaling 1.028 TiB, with a SHA-256 sidecar for every archive.
- Verified every sidecar, every archive's full PoH and semantic contents, and
  the ordered blockhash chain across all archive boundaries. The audit repeated
  every SHA-256 check after verification and exited successfully.
- Archive manifest SHA-256:
  `5188ce9912f81ef083d17c2b8ddf2a5928fa6d220fe44ab8a29019d89db42182`.
- Ran the current Horizon plugin over all 43,632,000 slots using 16 threads. It
  decoded and validated all 101 archives in 4 hours, 45 minutes, 58 seconds,
  averaging about 2,543 slots per second, and exited successfully.
