# Milestones

Major project results, newest first. Each entry records the UTC date and the
code revision that produced the result.

## 2026-09-25: Mainnet epoch 114 published and independently verified

- Commits: `36c8f0ed79a419f7018bc484675527107d707184` introduced the
  checkpoint-gated Solana v1.3.23 replay candidate, and
  `cb3fb22cb77c4163350301e3922879eb00509d44` added the later historical
  snapshot and update limits used by this replay.
- Replayed epoch 114 with the exact upstream runtime and matched bank hash
  `JB2jrQYrBfFUwDR9FaxyvYsBoHBsfgMyWumK3A1Z7Wrp` and accounts hash
  `9jKVXw9ro9fNUtXAU4Ls5jahimobsnFf7Agjn7GN3e3T` at slot 49,679,999.
- Published the 50,173,524,797-byte archive atomically with SHA-256
  `61c2f1836e03aea028d7c2cb3fa5951e2b26ca5f044ce292eed88da114c37d50`.
  Publication transaction
  `28a012564a6b30ab5308e48aff969384eedf737ac38facbdffc6721854af6448`
  records the same digest.
- The current independent verifier completed the full PoH and semantic audit
  in 33 minutes, 43 seconds. Its receipt binds the archive digest, verifier
  binary, and audit script.

## 2026-09-25: Mainnet epoch 174 published and independently verified

- Commits: `ff3697ade2f83162a66ab3a4a8cda185ea207600` introduced the
  checkpoint-gated Solana v1.6.15 replay candidate, and
  `cb3fb22cb77c4163350301e3922879eb00509d44` added the later historical
  snapshot and update limits used by this replay.
- Replayed epoch 174 with the exact upstream runtime and matched bank hash
  `HbVBuVWiuEPmBN1Qmi5CH2hEF2RYvFPYgpzAQPyaZczR` and accounts hash
  `ASsdDvcHnPkpSsxXsKdfKGG4q6NrKEjPeqPF3fHvQcXC` at slot 75,599,999.
- Published the 120,277,370,220-byte archive atomically with SHA-256
  `d95d69a3d5c13c573ad7fd96fad1cdbd820abc20191d1b1190318891818d36a0`.
  Publication transaction
  `458670bc5dfaa2e1a9c20619c8cf77d88d0e2202d97b2e9dac79b177eb2796b5`
  records the same digest.
- The current independent verifier completed the full PoH and semantic audit
  in 42 minutes, 38 seconds. Its receipt binds the archive digest, verifier
  binary, and audit script.

## 2026-09-25: Mainnet epoch 176 published and independently verified

- Commit: `f8cd608b2d4546921773b8f22c01d287acef1275`.
- Replayed epoch 176 with the exact upstream runtime and matched bank hash
  `4yVPT8FTqd5r3oE9UWXZMmvpkqQHrsDJ2sBDZRA8ZUx2` and accounts hash
  `Aub7D7zwqSdKd5Zhy2DvvUT1wqeuW77pDRnRwuwCCkhb` at slot 76,463,996.
- Published the 130,778,395,846-byte archive atomically with SHA-256
  `27dcc6656117b19a64fb570617a3220fa17c5e6421b8f0ea248639cbe5b91728`.
  Publication transaction
  `05e6e5124b24e6d3a5601c045cbe0e91740d94fbfb31131d36fc38e15cbc28da`
  records the same digest.
- The current independent verifier completed the full PoH and semantic audit
  in 42 minutes, 36 seconds. Its receipt binds the archive digest, verifier
  binary, and audit script.

## 2026-09-25: Mainnet epoch 175 published and independently verified

- Commit: `f8cd608b2d4546921773b8f22c01d287acef1275`; the historical
  update-limit fix was introduced in
  `cb3fb22cb77c4163350301e3922879eb00509d44`.
- Replayed epoch 175 with the exact upstream runtime and matched bank hash
  `Bfm9YGVCT13hcoQmVeqWokhKfB7aiMMpY7627yDQnAxy` and accounts hash
  `EMLejzoioxxv2NTmiVPg2ZNyv6LvBLmrR4U9H8FSa2NY` at slot 76,031,999.
- Published the 121,545,913,295-byte archive atomically with SHA-256
  `2a9c9228be8651c8ecc87c3f8ca50356c3c685d02fe0c7526b6c9556d002ae80`.
  Publication transaction
  `cddf71073c2621746d03f680b3a73b8c07492d29f561510f7f9f00992946af43`
  records the same digest.
- The first audit exposed a stale 16,384-record verifier cap at a valid
  20,811-update epoch-boundary section. The current reader keeps a finite
  65,536-record ceiling plus an independent 32 MiB data limit. Its archive
  tests passed 67/67, and the rebuilt verifier completed the full PoH and
  semantic audit in 43 minutes, 59 seconds.

## 2026-09-24: Mainnet epochs 101-102 published and independently verified

- Commit: `36c8f0ed79a419f7018bc484675527107d707184`.
- Replayed both epochs with the exact upstream Solana v1.3.23 runtime. Epoch
  101 matched bank hash `4bgJCXM1ZLuRaMHrhaPtRMYFeS6UsS13em4gY4jKREvi`
  and accounts hash `Ehb5xhJs9wcugxp4NNU1xssH8mV15ujDtRh3jzwVaoxt` at
  slot 44,063,999. Epoch 102 matched bank hash
  `4Pbbf4aU72nDkY2epd3E2pDPctaqmtBTTrAmc5voBvdV` and accounts hash
  `7EBFFxPXq2WxTBTRfdR4kVwxJ9tG53GH9R9vqBeX1sKL` at slot 44,495,999.
- Published 84,123,732,275 bytes atomically. Epoch 101 has SHA-256
  `244ade25b1c149e054b20786ff18f3920984f9d9d846d36714889caa02e983db`;
  epoch 102 has SHA-256
  `f9bd81c906951a6418f5929cdb996e92cf1b8262bd91f9eb7d777fc9095c4ba7`.
  The controller's durable publication attestation records both digests.
- Separate full-PoH verifiers checked both archives in parallel. Epoch 101
  completed in 29 minutes, 49 seconds, and epoch 102 completed in 29 minutes,
  35 seconds. Both receipts record the published SHA-256 digests.

## 2026-09-24: Mainnet epoch 121 published and independently verified

- Commit: `51eb8353b8f3fed60324f9d4101112df72d27708`.
- Replayed epoch 121 with the exact upstream Solana v1.3.23 runtime and matched
  canonical bank hash `GAhkbqM5kR4HLNYBFi77YxaokkGisZyMGHcqKvbS2zTw` and accounts hash
  `CC3783AY4R84L3QuGrd2Sra5m8GBXsCDyE4XSaH7AjeG` at slot 52,703,999.
- Published the 50,272,206,002-byte archive atomically with SHA-256
  `9d118075932b64e494754625612f3ddece1186193ec94d3f35f840bf6c033b20`.
  The controller's durable publication attestation records the same digest.
- A separate verifier checked the complete archive, including its PoH chain
  and semantic contents, in 35 minutes, 41 seconds. Its receipt records the
  published SHA-256 digest.

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
