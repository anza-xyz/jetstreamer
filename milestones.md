# Milestones

Major project results, newest first. Each entry records the UTC date and the
code revision that produced the result.

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
