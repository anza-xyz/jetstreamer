# Milestones

Major project results, newest first. Each entry records the UTC date and the
code revision that produced the result.

## 2026-10-04: Mainnet epoch 131 published and verified in R2

- Commits `343238c3e1cc15469df08a81fc2badf895fc125b`,
  `a22146d9d5602461e92f44dadf2c42dc4ad3d3af`,
  `29af7f5dc53e6745c207ad7705ddfec68d751599`, and
  `4941f317be133c7b1f888d293dfeaafe79b70a55` added and qualified the Solana
  v1.4.19 runtime, selected the canonical predecessor snapshot, and recorded
  the terminal checkpoint used by this replay.
- Replay covered all 432,000 slots and matched bank hash
  `2Fa11iFuTN3FAMZzrvf4SnPFs7GSeJuYX74yBWq355GW` and accounts hash
  `9Y55jp7GoaLWgpjt5GuFfXtPo2NHbwVuTNAJCEzCS8n8` at the last rooted slot,
  57,023,995. The sealed source transaction was
  `b82d09bcee7ca9dcf978d8ab14fef05bdae6f8b79b4842a7a038e56e3307fd13`;
  the public import transaction was
  `85a919bc85db66b31bc895422ac09c16f7823652c0f9096739f49082b3143183`.
- The 90,639,807,772-byte archive has SHA-256
  `05fd174fca4eeb966817b82cec473cd9b303be9485bf1e8a83d3b7dff0cd14b2`.
  Independent full PoH verification passed, and the current plugin consumed
  130,468,649 transactions, 417,401,988 transaction updates, and 5,076,722
  orphan updates with stream SHA-256
  `6f8acdbd763ca0573f35c6d4732aeffee0a0ffafcacc23e3aa23d8b0cf43ec0a`.
- R2 validated the multipart ETag
  `8fe43b1c01479493118315cd2e5dec32-1351` and a complete remote readback
  reproduced the archive SHA-256 before the canonical sidecar and durable
  receipt were accepted.

## 2026-10-01: Mainnet epoch-123 restart marker restored and canonically qualified

- Commit `ad889245396c96e0f682a888f0b6b8b85d75e2f1` restores the historical
  hard-fork marker at slot 53,180,900 in the exact Solana v1.3.23 runtime.
  Snapshot markers are validated strictly: the pre-existing slot-13,334,463
  marker and the restart marker must each occur exactly once, and unknown
  markers are rejected.
- Before the marker, replay produced bank hash
  `EZzqCDxdzWF4sak54hfhz9CMExLjgoh9qtKbMn8TdNLA`; applying the canonical
  one-count extension produces
  `Fi4p8z3AkfsuGXZzQ4TD28N8QDNSWC7ccqAqTs2GPdPu`, matching the first
  post-restart vote stream exactly.
- The production v1.3.23 worker replayed slots 53,180,854 through 53,199,885
  and matched terminal bank hash
  `EQXtC91MHAc8e4fpdtxJEmkweu2rztuQDrDqa9fUe1b2` and accounts hash
  `Ah3EL9YhkP5djcuJyRgzTSvQ4dQU8pNNz6Z1ypWSZQnA`. The complete replay and
  post-write archive validation finished in 48 minutes, 59 seconds.
- The verified segment has SHA-256
  `1a63218cebb80079a0978e6c57aebb9f39ae707a71c7eedda1e16a7528e43a9c`.
  Its evidence manifest binds production worker SHA-256
  `20c8d1031ae89889f53210aae5fcfdf5f4b08154012f6ff1494d80aeacb30966`
  and both canonical checkpoints.

## 2026-10-01: Receipt-composed boundary verification reduced a multi-hour audit to seconds

- Commit `49fb9996e5053ee17733b2dffcd309c72852e2cc` replaced overlapping full
  archive rescans with an exact cross-archive edge proof composed with each
  archive's existing full-verification receipt. Follow-up commit
  `ae48d49d4ab2de64590f4b1d5415d2c79bcd68cb` pins the approved full verifier
  and script identities throughout the composition and retirement gates.
- On the live 45.3 GiB epoch-108 and 49.3 GiB epoch-109 pair, the old verifier
  had not completed after 2 hours, 40 minutes. The new production workflow
  verified and fsynced the same boundary in 17 seconds end to end.
- The verifier requires contiguous epoch and slot ranges, the successor's
  initial PoH anchor to equal the predecessor's terminal blockhash, and the
  successor's first block to name that exact parent slot and hash. R2 local
  retirement now pins the approved full, plugin, and boundary binary/script
  SHA-256 values.

## 2026-09-30: Measured-memory admission raised historical replay concurrency to 14

- Commit `c70b757032a2d28d7694bb23c55e51e33bf48ceb` separated the measured
  non-reclaimable admission reservation from each worker's larger hard cgroup
  memory ceiling.
- Live cgroup measurements across ten producers showed 2.4–12.5 GiB of
  anonymous memory per worker; most of their 22–68 GiB `MemoryCurrent` was
  reclaimable file cache. The live-shaped admission benchmark increased from
  10 to 14 producers while retaining 64/68 GiB `MemoryHigh`/`MemoryMax` limits
  and the 1 TiB disk reserve.
- The checksum-pinned controller immediately admitted epoch 131 and cohorts
  182, 183–184, and 185. All four downloaded and bound their canonical GCS
  predecessor snapshots and started isolated historical workers successfully.
- All 98 scheduler and preflight tests passed before deployment.

## 2026-09-30: Current Horizon plugin consumed the complete epoch-101 archive

- Commit `343238c3e1cc15469df08a81fc2badf895fc125b` added the progressive,
  receipt-bound current-plugin gate used for this run.
- The plugin consumed all 432,000 slots, including 411,371 produced blocks,
  109,333,914 entries, 84,903,526 transactions, 218,072,888 transaction
  updates, and 2,615,394 orphan updates in 27 minutes, 53 seconds.
- It processed 13,485,130,897,497 account-data bytes and produced deterministic
  stream SHA-256
  `ee5e9dd0f3212c820a5f67de75cfc0074c060e37bbbf5f81c245516485925b80`.
- The fsynced gate receipt binds archive SHA-256
  `244ade25b1c149e054b20786ff18f3920984f9d9d846d36714889caa02e983db`
  to `horizon_pipeline` SHA-256
  `f2d6cda1e7a1f2c3ad670ef84d13cb2461ba2e7699d3d38dde216c633d9e7f1a`
  and verification-script SHA-256
  `966fba1a0a08715db2e328532662450ec432f5dfe42dc3af076db56a14c6e1cf`.

## 2026-09-30: First Horizon archive delivered to append-only R2

- Commit: `d7243fbbd8f1b4234256a57c9a1003cb0fcdc3a8` added the fail-closed
  `jetstreamer-r2` delivery tool and portable `horizon-r2` Codex skill.
- Uploaded the 37,448,762,331-byte epoch-107 archive in 559 parts. R2 checked
  each part's `Content-MD5`, and the reconstructed multipart ETag matched
  `364ccc22ecb71b9421a11d6483fa273e-559`.
- Read the completed archive back through R2 and reproduced SHA-256
  `3f832480dc694be004ee27a79054d266c753496ac78c77b82fe8c795ad402f1d`.
  The canonical checksum sidecar also matched byte-for-byte.
- Fsynced the private delivery receipt only after both remote objects were
  re-observed. The canary left the local pair intact and left no incomplete
  multipart upload.

## 2026-09-29: Mainnet epoch 180 published and independently verified

- Commits: `ed00144fbf303723304090655b84230af61e7a12` supplied the
  bounded historical post-freeze limits used by this replay, and
  `8ba812357ea250d09c050f2433a77a8f97056dca` parallelized the deep
  source-archive validation used before publication.
- Replayed all 432,000 slots with the Solana v1.6.15 runtime and matched bank
  hash `94CiUQQaYix4PCQyGXgeJPoB1tr7FTup4QgcNS8kgUCP` and accounts hash
  `bTSU9kkyQuFrederdQr2x8GdNieckiFmLeJMB3WWpYE` at slot 78,191,999.
- Published the 124,669,828,961-byte archive atomically with SHA-256
  `b11bd7739afa2117c213053ee715cc834a9e924b6f42683f19e94ad38f09e908`.
  Publication transaction
  `c64fc4c074594eff08d58325fef6d4e447cd10d7a36ba8c7ccef05d75b695b14`
  records the same digest.
- The current independent verifier checked every slot frame, the complete PoH
  chain, and semantic contents in 42 minutes, 36 seconds. Its receipt binds
  the archive digest to verifier SHA-256
  `51bb4a90dcac0a2475b46b2820470c3a53a0a541720a233dac288934cc864336`
  and audit-script SHA-256
  `a64fed21248715b2214292ea961a82afc5367a34b337d35afdbbc48d62c04bed`.

## 2026-09-29: Mainnet epoch 118 published and independently verified

- Commits: `36c8f0ed79a419f7018bc484675527107d707184` introduced the
  checkpoint-gated Solana v1.3.23 replay candidate, and
  `cb3fb22cb77c4163350301e3922879eb00509d44` supplied the historical
  archive limits used by this replay.
- Replayed all 432,000 slots and matched bank hash
  `6UzELcv2UP2oPjrkGdCXQHwLcidAc19Teg8zeGA1aUgp` and accounts hash
  `5zVyDX9nRih9b8X354ir1WqKtoF2rj298FS5ggJ5dBP4` at slot 51,407,999.
- Published the 49,749,923,223-byte archive atomically with SHA-256
  `326425e0bbc84f26e532f3f1e7eee08b65069bab95e46991dcbd5333b749e025`.
  Publication transaction
  `9aec5f49c42d13bc3c3f2f34735e90f876e48b3d6595d98bf1ceacd8aa7f274c`
  records the same digest.
- The current independent verifier checked every slot frame, the complete PoH
  chain, and semantic contents in 34 minutes, 30 seconds. Its receipt binds
  the archive digest to verifier SHA-256
  `51bb4a90dcac0a2475b46b2820470c3a53a0a541720a233dac288934cc864336`
  and audit-script SHA-256
  `a64fed21248715b2214292ea961a82afc5367a34b337d35afdbbc48d62c04bed`.

## 2026-09-29: Mainnet epoch 179 published and independently verified

- Commit `ed00144fbf303723304090655b84230af61e7a12` supplied the bounded
  historical post-freeze limits used by this replay.
- Replayed all 432,000 slots with the Solana v1.6.15 runtime and matched bank
  hash `7ZXMmf2hoeyomGMdg1biW3gBaUDzK3d14iS57xDqb29e` and accounts hash
  `AJ8L2N7vLw6f8Y7XjwSV4mPGtRvYKm37VmFjt6mbfefB` at slot 77,759,999.
- Published the 120,899,969,361-byte archive atomically with SHA-256
  `7f47354d7a33c78ee431b46be318fc9182f6910d8d783c8dbc3e3564b3c12004`.
  Publication transaction
  `e12cfed6b71e57d580106a2cc46b927d5a8335f1b4b180608417a0d034132706`
  records the same digest.
- The current independent verifier checked all slot frames, 364,362 produced
  blocks, the complete PoH chain, and semantic contents in 42 minutes, 6
  seconds. Its receipt binds the archive digest to verifier SHA-256
  `51bb4a90dcac0a2475b46b2820470c3a53a0a541720a233dac288934cc864336`
  and audit-script SHA-256
  `a64fed21248715b2214292ea961a82afc5367a34b337d35afdbbc48d62c04bed`.

## 2026-09-28: Mainnet epoch 128 published and independently verified

- Commits: `36c8f0ed79a419f7018bc484675527107d707184` introduced the
  checkpoint-gated Solana v1.3.23 replay candidate, and
  `cb3fb22cb77c4163350301e3922879eb00509d44` supplied the historical
  archive limits used by this replay.
- Replayed all 432,000 slots and matched bank hash
  `5rdypn2Y3kE2sophSWe2dahodmJq8Y5AJfR4z6dFuezV` and accounts hash
  `55d2eydGE1xSXenReGbujAnc8pdBMfrEehkxym6YkBu6` at slot 55,727,996.
- Published the 85,389,391,753-byte archive atomically with SHA-256
  `48cd9f9ab392e051dd47db4868cf4617d374889caef18683e831b93e2a34b98c`.
  Publication transaction
  `2ce8c791357eb0f6e76f58e24adbddcf490f1f356c11b37fa50e84dba603929b`
  records the same digest.
- The current independent verifier checked all slot frames, 318,843 produced
  blocks, the complete PoH chain, and semantic contents in 34 minutes, 28
  seconds. Its receipt binds the archive digest to verifier SHA-256
  `51bb4a90dcac0a2475b46b2820470c3a53a0a541720a233dac288934cc864336`
  and audit-script SHA-256
  `a64fed21248715b2214292ea961a82afc5367a34b337d35afdbbc48d62c04bed`.

## 2026-09-28: Mainnet epoch 127 published and independently verified

- Commits: `36c8f0ed79a419f7018bc484675527107d707184` introduced the
  checkpoint-gated Solana v1.3.23 replay candidate, and
  `cb3fb22cb77c4163350301e3922879eb00509d44` supplied the historical
  archive limits used by this replay.
- Replayed all 432,000 slots and matched bank hash
  `4V8Nd7BB7wt7Lh2HzzXAoR6j1vcU2mgTWjNDyDx1KNE3` and accounts hash
  `6Cg7xdrVJUztYHD6tj4DNjbmXJurCrK6ij6uzezWqg5P` at slot 55,295,999.
- Published the 82,554,677,007-byte archive atomically with SHA-256
  `976090f0a8078d3b56d7c287b58ca160d18d4f07996af2e93c07612d6e36af1d`.
  Publication transaction
  `8964418fc3221a58e2b2365657613ad0278a394c8cc48499b78af51cb224962e`
  records the same digest.
- A transient controller lock collision left the first import uncommitted and
  preserved its complete source receipt. The retained importer published the
  same verified archive after all controller lock holders were paused. Commit
  `a5099f043cd06b5467efafc20a9508e1438d2fb4` adds a bounded,
  identity-checked wait before future import mutations.
- The current independent verifier checked all slot frames, 334,264 produced
  blocks, the complete PoH chain, and semantic contents in 34 minutes, 13
  seconds. Its receipt binds the archive digest to verifier SHA-256
  `51bb4a90dcac0a2475b46b2820470c3a53a0a541720a233dac288934cc864336`
  and audit-script SHA-256
  `a64fed21248715b2214292ea961a82afc5367a34b337d35afdbbc48d62c04bed`.

## 2026-09-27: All 22 published epochs reverified with the current limits

- Commit: `ed00144fbf303723304090655b84230af61e7a12` raised the bounded
  historical post-freeze record limit from 4,096 to 8,192 while retaining an
  independent 8 MiB account-data limit per slot.
- The current verifier independently checked epochs 101-117, 121, 126, and
  174-176. The set contains 1,279,648,807,329 archive bytes. Each check
  recomputed the complete stored PoH chain and decoded every semantic record.
- All 22 checks passed between 12:22 and 18:14 UTC. The receipts bind each
  archive digest to verifier SHA-256
  `51bb4a90dcac0a2475b46b2820470c3a53a0a541720a233dac288934cc864336`
  and audit-script SHA-256
  `a64fed21248715b2214292ea961a82afc5367a34b337d35afdbbc48d62c04bed`.

## 2026-09-26: Mainnet epochs 115-116 published and independently verified

- Commits: `36c8f0ed79a419f7018bc484675527107d707184` introduced the
  checkpoint-gated Solana v1.3.23 replay candidate, and
  `cb3fb22cb77c4163350301e3922879eb00509d44` supplied the historical
  archive limits used by these replays.
- Epoch 115 matched bank hash
  `CftETQCv4Tx5Wz35H9j6sHPDwuRF2nZQmFYnTZBncKM5` and accounts hash
  `Asp6SdvTboWmGQb9MoaTrCU3AEVL9sxDpay6MgV76UAV` at slot 50,111,999.
  Epoch 116 matched bank hash
  `5hV7MBKNjzcSbH2DJDgPViG51C9Gy5KhhPWXqXzFRERw` and accounts hash
  `5LdE9mpyeLv18JFHbhK3SfLmVuHu46U94F3vVHFKa31t` at slot 50,543,999.
- Published 97,566,250,920 bytes atomically. Epoch 115 has SHA-256
  `e46560111400be904ea230d5c8693cb13342db75c6d4d6017d90555806bfe1e6`;
  epoch 116 has SHA-256
  `d9d3ff5607a03e07deac3ce8b9f5a0d557329f3d1bacf75bc8321f79893f731c`.
  Publication transaction
  `c5a75a671faa89c4a0839ff213f2da14b36e19dced5def11ca5ace12da345ba1`
  records both digests.
- Separate current verifiers checked both complete archives in parallel.
  Epoch 115 completed in 34 minutes, 54 seconds and epoch 116 in 34 minutes,
  22 seconds; together they verified all semantic contents and 784,978 stored
  PoH links. Each receipt binds the archive digest, verifier binary, and audit
  script.

## 2026-09-26: Mainnet epochs 112-113 published and independently verified

- Commits: `36c8f0ed79a419f7018bc484675527107d707184` introduced the
  checkpoint-gated Solana v1.3.23 replay candidate, and
  `cb3fb22cb77c4163350301e3922879eb00509d44` supplied the historical
  archive limits used by these replays.
- Epoch 112 matched bank hash
  `G39oL9mCAkGJHk7iXY5EgNTAXUmc6S369EDE5ddohdGs` and accounts hash
  `J3KCREsSmdbKJAxnoDRAgzbu5rX9DTfM9obpVM4ivvDJ` at slot 48,815,999.
  Epoch 113 matched bank hash
  `2KULnv3TPzRmc9TxmdaPdHjWgeZF2hyVgf3kb5b9LWCJ` and accounts hash
  `9ad7H18SEBdiwheuNNd3VgEyRr7bqXhWy5fApwHX4kDN` at slot 49,247,999.
- Published 98,038,111,784 bytes atomically. Epoch 112 has SHA-256
  `eceef44aea5fa6916acf9cfdc60917782d203f5934ae5b89030754ee965c7f32`;
  epoch 113 has SHA-256
  `f610256365f7c610e6f8225e12667e4203cc6cb641e265240c798fd947c2462a`.
  Publication transaction
  `5329f25a4c704c1c6f2e808d81d97a375bacd66c8821b5acce14c83de0ade08f`
  records both digests.
- Separate current verifiers checked both complete archives in parallel.
  Epoch 112 completed in 34 minutes, 58 seconds and epoch 113 in 34 minutes,
  57 seconds; together they verified all semantic contents and 815,155 stored
  PoH links. Each receipt binds the archive digest, verifier binary, and audit
  script.

## 2026-09-26: Bounded historical rent-burst support proven on mainnet

- Commit: `ed00144fbf303723304090655b84230af61e7a12`.
- Five independent Solana v1.6.15 replays using the previous 4,096-record
  post-freeze limit stopped on the 4,097th update: epoch 177 at slot
  76,611,288, epoch 178 at slot 76,920,172, epoch 179 at slot 77,374,128,
  epoch 180 at slot 77,882,688, and epoch 181 at slot 78,204,496.
- The corrected runtime retains finite, independent safeguards: at most 8,192
  post-freeze records and 8 MiB of post-freeze account data per slot.
- Production retries with the corrected binary have passed all five known
  failure points without relaxing any other archive or decode limit.
  Epoch 178 reached slot 76,944,153, epoch 179 reached 77,380,085, and epoch
  181 reached 78,204,930. At 21:35 UTC on September 27, epoch 180 reached slot
  77,884,656, which is 1,968 slots beyond its previous failure point. At 23:18
  UTC, epoch 177 reached slot 76,611,642, 354 slots beyond the last known
  failure point. All five corrected replays remained active and healthy.

## 2026-09-26: Mainnet epoch 126 published and independently verified

- Commits: `099f5ef2633d9c911cdab8afde8b4a42e5517638` qualified the exact
  Solana v1.3.23 runtime across the epoch-126 divergence, and
  `cb3fb22cb77c4163350301e3922879eb00509d44` supplied the historical
  archive limits used by the replay and verifier.
- Replayed epoch 126 and matched bank hash
  `G1zw8VTkFEbbiwqNUN9tkYa6gNGhZ4LvDVJqkfznrxar` and accounts hash
  `NiLtZQDK1L5o7Gi788wqYZpDLQg4gqmR5WmBDLJo8H7` at slot 54,863,999.
- Published the 84,663,787,927-byte archive atomically with SHA-256
  `560458b9847cf1c7c5c30291a09b188625c2ed95bb4a3cda989ac01bd9bb1cdd`.
  Publication transaction
  `011997a6ee9750b7201a7a5a367f26dcb2eeb15c36e62ba23ccf22732974b7f6`
  records the same digest.
- The current independent verifier checked all 432,000 slot frames, 318,205
  produced blocks, the complete PoH chain, and semantic contents in 33
  minutes, 55 seconds. Its receipt binds the archive digest, verifier binary,
  and audit script.

## 2026-09-26: Mainnet epochs 108-109 published and independently verified

- Commits: `36c8f0ed79a419f7018bc484675527107d707184` introduced the
  checkpoint-gated Solana v1.3.23 replay candidate, and
  `cb3fb22cb77c4163350301e3922879eb00509d44` supplied the historical
  archive limits used by these replays.
- Epoch 108 matched bank hash
  `8S29XN11ZG9z2s3zTh7s8rBAs3Dhwhm5jinLFYTxFxuR` and accounts hash
  `DE4Bh4Kf2bc5mUzrTZS1FfU86deYficyG976nDsoYGCc` at slot 47,087,999.
  Epoch 109 matched bank hash
  `GnQEAKLCg44cwhz3DLAqQ5ukcEt2zrJmgr2MLr5rVaLF` and accounts hash
  `3kFWbD8t8tjQTEJDux47CiQzTRPH6afLY6UresiG2D1K` at slot 47,519,999.
- Published 94,567,115,462 bytes atomically. Epoch 108 has SHA-256
  `4bb38edb70ac14ab62f2a7ae14a2f221963b65ca6d0f13dcf793546df3796a5b`;
  epoch 109 has SHA-256
  `5fd6fbb84fc4202fd98f5f73aba8c8102c5d9df8f0f427ad880153f4fa704189`.
  Publication transaction
  `72a1efad5505c1003b31150e0e1a0557b15c017daf8edbaf93140c5e0b398d4c`
  records both digests.
- Separate current verifiers checked both complete archives in parallel.
  Epoch 108 completed in 31 minutes, 39 seconds and epoch 109 in 32 minutes,
  59 seconds; together they verified 797,334 produced blocks and both full
  PoH chains. Each receipt binds its archive digest, verifier binary, and
  audit script.

## 2026-09-25: Mainnet epoch 117 published and independently verified

- Commits: `36c8f0ed79a419f7018bc484675527107d707184` introduced the
  checkpoint-gated Solana v1.3.23 replay candidate, and
  `cb3fb22cb77c4163350301e3922879eb00509d44` supplied the historical
  archive limits used by this replay.
- Replayed epoch 117 and matched bank hash
  `5i6LPtbD8cE55SNtmvc7XtWPKjk5tgP68b9oNQmLBq6z` and accounts hash
  `HsC3ANL3pzeYFx9suVNS86THC4t9kvnPRGsGBcajMiQx` at slot 50,975,999.
- Published the 50,255,468,131-byte archive atomically with SHA-256
  `b0bf0fbdb73fd32ce261ed33c2aeb09d5785d601123a747cbd7dbf2bde74d933`.
  Publication transaction
  `526f20e439a323ac89b1bdf2c5d515e6674fab8eb2ffbaba3351d06c78df66f9`
  records the same digest.
- The current independent verifier checked all 432,000 slot frames, 369,692
  produced blocks, the complete PoH chain, and semantic contents in 32
  minutes, 52 seconds. Its receipt binds the archive digest, verifier binary,
  and audit script.

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
