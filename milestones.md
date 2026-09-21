# Milestones

Major project results, newest first. Each entry records the UTC date and the
code revision that produced the result.

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
