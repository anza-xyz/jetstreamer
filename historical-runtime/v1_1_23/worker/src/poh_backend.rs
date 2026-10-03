//! Fast, exact v1.1.23 Proof-of-History hashing.
//!
//! Ordinary PoH steps hash exactly 32 bytes. A transaction entry's final
//! step hashes exactly 64 bytes: `poh_hash || transaction_merkle_root`.
//! The portable path supplies already padded blocks to `sha2` 0.9.9, avoiding
//! generic digest buffering. On x86-64 with SHA-NI, the hot path additionally
//! keeps dependent digest words in SIMD registers between steps. When two
//! independent entry segments are available, their rounds are interleaved to
//! hide the instruction dependency latency. Other targets and the explicit
//! test-only soft feature retain the portable implementation.
//!
//! Solana v1.1.23 retained v1.0.7's `entry::next_hash` wire semantics, so this
//! backend is intentionally the same implementation used by the v1.0.7
//! worker. Its tests differentially check the optimized, paired, and portable
//! paths against `solana_sdk::hash::{hash, hashv}` over randomized inputs.

use rayon::prelude::*;
use sha2_fast::{
    compress256,
    digest::generic_array::{typenum::U64, GenericArray},
};
use std::slice;

pub type PohHash = [u8; 32];
type Sha256Block = GenericArray<u8, U64>;

const SHA256_IV: [u32; 8] = [
    0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19,
];

#[cfg(all(target_arch = "x86_64", not(feature = "poh-force-soft")))]
const SHA256_K: [u32; 64] = [
    0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
    0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
    0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
    0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
    0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
    0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
    0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
    0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
];

#[derive(Clone, Copy, Debug)]
pub struct PohSegment {
    pub num_hashes: u64,
    pub transaction_mixin: Option<PohHash>,
    pub expected: PohHash,
}

/// SHA-256 specialized for the 32-byte input of an ordinary PoH step.
#[allow(dead_code)]
#[inline]
pub fn hash32(input: &PohHash) -> PohHash {
    hash32_chain(*input, 1)
}

#[inline]
fn padded_hash32_block() -> Sha256Block {
    let mut block = GenericArray::default();
    block[32] = 0x80;
    // 32 bytes == 256 bits, encoded as a big-endian u64 at block[56..64].
    block[62] = 1;
    block
}

#[inline]
fn hash32_with_block(input: &PohHash, block: &mut Sha256Block) -> PohHash {
    block[..32].copy_from_slice(input);
    let mut state = SHA256_IV;
    compress256(&mut state, slice::from_ref(&block));
    state_to_bytes(&state)
}

/// SHA-256 specialized for `poh_hash || transaction_merkle_root`.
#[inline]
pub fn hash64(left: &PohHash, right: &PohHash) -> PohHash {
    let mut blocks = [GenericArray::default(), GenericArray::default()];
    blocks[0][..32].copy_from_slice(left);
    blocks[0][32..].copy_from_slice(right);
    blocks[1][0] = 0x80;
    // 64 bytes == 512 bits, encoded as a big-endian u64 at block[56..64].
    blocks[1][62] = 2;

    let mut state = SHA256_IV;
    compress256(&mut state, &blocks);
    state_to_bytes(&state)
}

#[inline]
fn state_to_bytes(state: &[u32; 8]) -> PohHash {
    let mut output = [0u8; 32];
    for (chunk, word) in output.chunks_exact_mut(4).zip(state.iter()) {
        chunk.copy_from_slice(&word.to_be_bytes());
    }
    output
}

/// Exact equivalent of v1.1.23 `ledger::entry::next_hash` after the caller has
/// computed the transaction-signature Merkle root.
#[inline(never)]
pub fn next_hash(current: PohHash, num_hashes: u64, transaction_mixin: Option<PohHash>) -> PohHash {
    let current = hash32_chain(current, ordinary_hash_count(num_hashes, transaction_mixin));
    finish_hash(current, transaction_mixin)
}

/// Compute two independently anchored PoH segments together. This is exactly
/// two calls to [`next_hash`]; it does not join their hash chains. On SHA-NI
/// hosts their independent rounds are interleaved on one physical core.
#[inline(never)]
pub fn next_hash_pair(
    starts: [PohHash; 2],
    num_hashes: [u64; 2],
    transaction_mixins: [Option<PohHash>; 2],
) -> [PohHash; 2] {
    let ordinary = [
        ordinary_hash_count(num_hashes[0], transaction_mixins[0]),
        ordinary_hash_count(num_hashes[1], transaction_mixins[1]),
    ];

    #[cfg(all(target_arch = "x86_64", not(feature = "poh-force-soft")))]
    let currents = if sha_ni::available() {
        let common = ordinary[0].min(ordinary[1]);
        // SAFETY: `available` checked every target feature enabled on these
        // functions. Inputs and outputs are fixed-size owned arrays; the
        // implementation performs no length-derived pointer arithmetic.
        unsafe {
            let mut currents = sha_ni::chain_pair(starts, common);
            let first = currents[0];
            let second = currents[1];
            currents[0] = sha_ni::chain(first, ordinary[0] - common);
            currents[1] = sha_ni::chain(second, ordinary[1] - common);
            currents
        }
    } else {
        [
            hash32_chain_sha2(starts[0], ordinary[0]),
            hash32_chain_sha2(starts[1], ordinary[1]),
        ]
    };

    #[cfg(any(not(target_arch = "x86_64"), feature = "poh-force-soft"))]
    let currents = [
        hash32_chain_sha2(starts[0], ordinary[0]),
        hash32_chain_sha2(starts[1], ordinary[1]),
    ];

    [
        finish_hash(currents[0], transaction_mixins[0]),
        finish_hash(currents[1], transaction_mixins[1]),
    ]
}

#[inline]
fn ordinary_hash_count(num_hashes: u64, transaction_mixin: Option<PohHash>) -> u64 {
    if transaction_mixin.is_some() {
        num_hashes.saturating_sub(1)
    } else {
        num_hashes
    }
}

#[inline]
fn finish_hash(current: PohHash, transaction_mixin: Option<PohHash>) -> PohHash {
    match transaction_mixin {
        None => current,
        Some(mixin) => hash64(&current, &mixin),
    }
}

#[inline]
#[cfg(all(target_arch = "x86_64", not(feature = "poh-force-soft")))]
fn hash32_chain(current: PohHash, count: u64) -> PohHash {
    if sha_ni::available() {
        // SAFETY: every enabled target feature was checked above. The input
        // is a fixed-size owned array and `chain` never reads past it.
        return unsafe { sha_ni::chain(current, count) };
    }

    hash32_chain_sha2(current, count)
}

#[inline]
#[cfg(any(not(target_arch = "x86_64"), feature = "poh-force-soft"))]
fn hash32_chain(current: PohHash, count: u64) -> PohHash {
    hash32_chain_sha2(current, count)
}

/// Fixed-block `sha2` path retained as the portable fallback and benchmark
/// baseline for the runtime-dispatched implementation.
#[allow(dead_code)]
#[inline(never)]
pub fn next_hash_sha2(
    current: PohHash,
    num_hashes: u64,
    transaction_mixin: Option<PohHash>,
) -> PohHash {
    let current = hash32_chain_sha2(current, ordinary_hash_count(num_hashes, transaction_mixin));
    finish_hash(current, transaction_mixin)
}

#[inline]
fn hash32_chain_sha2(mut current: PohHash, count: u64) -> PohHash {
    // Reuse the invariant padding half of the one-block message throughout a
    // PoH chain. This removes 32 bytes of zeroing and two padding stores from
    // every hash after the first.
    let mut block = padded_hash32_block();
    for _ in 0..count {
        current = hash32_with_block(&current, &mut block);
    }
    current
}

#[cfg(all(target_arch = "x86_64", not(feature = "poh-force-soft")))]
mod sha_ni {
    use super::{PohHash as Hash, SHA256_IV as IV, SHA256_K as K};
    use std::arch::x86_64::*;

    /// Check every extension used by the specialized functions before
    /// entering their target-feature boundary.
    #[inline]
    pub fn available() -> bool {
        is_x86_feature_detected!("sha")
            && is_x86_feature_detected!("sse2")
            && is_x86_feature_detected!("ssse3")
            && is_x86_feature_detected!("sse4.1")
    }

    #[inline(always)]
    unsafe fn schedule(v0: __m128i, v1: __m128i, v2: __m128i, v3: __m128i) -> __m128i {
        let t1 = _mm_sha256msg1_epu32(v0, v1);
        let t2 = _mm_alignr_epi8(v3, v2, 4);
        let t3 = _mm_add_epi32(t1, t2);
        _mm_sha256msg2_epu32(t3, v3)
    }

    macro_rules! rounds4 {
        ($abef:ident, $cdgh:ident, $rest:expr, $i:expr) => {{
            let i = $i * 4;
            let kv = _mm_set_epi32(
                K[i + 3] as i32,
                K[i + 2] as i32,
                K[i + 1] as i32,
                K[i] as i32,
            );
            let t1 = _mm_add_epi32($rest, kv);
            $cdgh = _mm_sha256rnds2_epu32($cdgh, $abef, t1);
            let t2 = _mm_shuffle_epi32(t1, 0x0e);
            $abef = _mm_sha256rnds2_epu32($abef, $cdgh, t2);
        }};
    }

    macro_rules! schedule_rounds4 {
        ($abef:ident, $cdgh:ident, $w0:expr, $w1:expr, $w2:expr, $w3:expr, $w4:expr, $i:expr) => {{
            $w4 = schedule($w0, $w1, $w2, $w3);
            rounds4!($abef, $cdgh, $w4, $i);
        }};
    }

    #[target_feature(enable = "sha,sse2,ssse3,sse4.1")]
    unsafe fn chain_inner(input: Hash, count: u64) -> Hash {
        let mask = _mm_set_epi64x(
            0x0c0d_0e0f_0809_0a0bu64 as i64,
            0x0405_0607_0001_0203u64 as i64,
        );
        // SAFETY: `input` is exactly 32 bytes, so these are the only two
        // unaligned 16-byte loads and both remain within the owned array.
        let input_ptr = input.as_ptr() as *const __m128i;
        let mut digest0 = _mm_shuffle_epi8(_mm_loadu_si128(input_ptr), mask);
        let mut digest1 = _mm_shuffle_epi8(_mm_loadu_si128(input_ptr.add(1)), mask);

        // SAFETY: `IV` contains exactly eight u32 words (32 bytes), so the two
        // unaligned loads cover it without crossing its bounds.
        let iv_ptr = IV.as_ptr() as *const __m128i;
        let dcba = _mm_loadu_si128(iv_ptr);
        let efgh = _mm_loadu_si128(iv_ptr.add(1));
        let cdab = _mm_shuffle_epi32(dcba, 0xb1);
        let efgh = _mm_shuffle_epi32(efgh, 0x1b);
        let initial_abef = _mm_alignr_epi8(cdab, efgh, 8);
        let initial_cdgh = _mm_blend_epi16(efgh, cdab, 0xf0);

        let padding0 = _mm_set_epi32(0, 0, 0, 0x8000_0000u32 as i32);
        let padding1 = _mm_set_epi32(0x0000_0100, 0, 0, 0);

        for _ in 0..count {
            let mut abef = initial_abef;
            let mut cdgh = initial_cdgh;
            let mut w0 = digest0;
            let mut w1 = digest1;
            let mut w2 = padding0;
            let mut w3 = padding1;
            let mut w4;

            rounds4!(abef, cdgh, w0, 0);
            rounds4!(abef, cdgh, w1, 1);
            rounds4!(abef, cdgh, w2, 2);
            rounds4!(abef, cdgh, w3, 3);
            schedule_rounds4!(abef, cdgh, w0, w1, w2, w3, w4, 4);
            schedule_rounds4!(abef, cdgh, w1, w2, w3, w4, w0, 5);
            schedule_rounds4!(abef, cdgh, w2, w3, w4, w0, w1, 6);
            schedule_rounds4!(abef, cdgh, w3, w4, w0, w1, w2, 7);
            schedule_rounds4!(abef, cdgh, w4, w0, w1, w2, w3, 8);
            schedule_rounds4!(abef, cdgh, w0, w1, w2, w3, w4, 9);
            schedule_rounds4!(abef, cdgh, w1, w2, w3, w4, w0, 10);
            schedule_rounds4!(abef, cdgh, w2, w3, w4, w0, w1, 11);
            schedule_rounds4!(abef, cdgh, w3, w4, w0, w1, w2, 12);
            schedule_rounds4!(abef, cdgh, w4, w0, w1, w2, w3, 13);
            schedule_rounds4!(abef, cdgh, w0, w1, w2, w3, w4, 14);
            schedule_rounds4!(abef, cdgh, w1, w2, w3, w4, w0, 15);

            abef = _mm_add_epi32(abef, initial_abef);
            cdgh = _mm_add_epi32(cdgh, initial_cdgh);
            let feba = _mm_shuffle_epi32(abef, 0x1b);
            let dchg = _mm_shuffle_epi32(cdgh, 0xb1);
            digest0 = _mm_blend_epi16(feba, dchg, 0xf0);
            digest1 = _mm_alignr_epi8(dchg, feba, 8);
        }

        let mut output = [0u8; 32];
        // SAFETY: `output` is exactly 32 bytes; the two unaligned stores cover
        // precisely its first and second 16-byte halves.
        let output_ptr = output.as_mut_ptr() as *mut __m128i;
        _mm_storeu_si128(output_ptr, _mm_shuffle_epi8(digest0, mask));
        _mm_storeu_si128(output_ptr.add(1), _mm_shuffle_epi8(digest1, mask));
        output
    }

    /// Caller must first ensure that [`available`] returned true.
    #[inline(never)]
    pub unsafe fn chain(input: Hash, count: u64) -> Hash {
        chain_inner(input, count)
    }

    #[target_feature(enable = "sha,sse2,ssse3,sse4.1")]
    unsafe fn chain_pair_inner(inputs: [Hash; 2], count: u64) -> [Hash; 2] {
        let mask = _mm_set_epi64x(
            0x0c0d_0e0f_0809_0a0bu64 as i64,
            0x0405_0607_0001_0203u64 as i64,
        );
        // SAFETY: each input is an owned 32-byte array. The two loads for
        // each input cover only its first and second 16-byte halves.
        let input0 = inputs[0].as_ptr() as *const __m128i;
        let input1 = inputs[1].as_ptr() as *const __m128i;
        let mut a0 = _mm_shuffle_epi8(_mm_loadu_si128(input0), mask);
        let mut a1 = _mm_shuffle_epi8(_mm_loadu_si128(input0.add(1)), mask);
        let mut b0 = _mm_shuffle_epi8(_mm_loadu_si128(input1), mask);
        let mut b1 = _mm_shuffle_epi8(_mm_loadu_si128(input1.add(1)), mask);

        // SAFETY: `IV` is exactly 32 bytes, split into two 16-byte loads.
        let iv_ptr = IV.as_ptr() as *const __m128i;
        let dcba = _mm_loadu_si128(iv_ptr);
        let efgh = _mm_loadu_si128(iv_ptr.add(1));
        let cdab = _mm_shuffle_epi32(dcba, 0xb1);
        let efgh = _mm_shuffle_epi32(efgh, 0x1b);
        let initial_abef = _mm_alignr_epi8(cdab, efgh, 8);
        let initial_cdgh = _mm_blend_epi16(efgh, cdab, 0xf0);
        let padding0 = _mm_set_epi32(0, 0, 0, 0x8000_0000u32 as i32);
        let padding1 = _mm_set_epi32(0x0000_0100, 0, 0, 0);

        for _ in 0..count {
            let mut a_abef = initial_abef;
            let mut a_cdgh = initial_cdgh;
            let mut b_abef = initial_abef;
            let mut b_cdgh = initial_cdgh;
            let mut aw0 = a0;
            let mut aw1 = a1;
            let mut aw2 = padding0;
            let mut aw3 = padding1;
            let mut aw4;
            let mut bw0 = b0;
            let mut bw1 = b1;
            let mut bw2 = padding0;
            let mut bw3 = padding1;
            let mut bw4;

            rounds4!(a_abef, a_cdgh, aw0, 0);
            rounds4!(b_abef, b_cdgh, bw0, 0);
            rounds4!(a_abef, a_cdgh, aw1, 1);
            rounds4!(b_abef, b_cdgh, bw1, 1);
            rounds4!(a_abef, a_cdgh, aw2, 2);
            rounds4!(b_abef, b_cdgh, bw2, 2);
            rounds4!(a_abef, a_cdgh, aw3, 3);
            rounds4!(b_abef, b_cdgh, bw3, 3);
            schedule_rounds4!(a_abef, a_cdgh, aw0, aw1, aw2, aw3, aw4, 4);
            schedule_rounds4!(b_abef, b_cdgh, bw0, bw1, bw2, bw3, bw4, 4);
            schedule_rounds4!(a_abef, a_cdgh, aw1, aw2, aw3, aw4, aw0, 5);
            schedule_rounds4!(b_abef, b_cdgh, bw1, bw2, bw3, bw4, bw0, 5);
            schedule_rounds4!(a_abef, a_cdgh, aw2, aw3, aw4, aw0, aw1, 6);
            schedule_rounds4!(b_abef, b_cdgh, bw2, bw3, bw4, bw0, bw1, 6);
            schedule_rounds4!(a_abef, a_cdgh, aw3, aw4, aw0, aw1, aw2, 7);
            schedule_rounds4!(b_abef, b_cdgh, bw3, bw4, bw0, bw1, bw2, 7);
            schedule_rounds4!(a_abef, a_cdgh, aw4, aw0, aw1, aw2, aw3, 8);
            schedule_rounds4!(b_abef, b_cdgh, bw4, bw0, bw1, bw2, bw3, 8);
            schedule_rounds4!(a_abef, a_cdgh, aw0, aw1, aw2, aw3, aw4, 9);
            schedule_rounds4!(b_abef, b_cdgh, bw0, bw1, bw2, bw3, bw4, 9);
            schedule_rounds4!(a_abef, a_cdgh, aw1, aw2, aw3, aw4, aw0, 10);
            schedule_rounds4!(b_abef, b_cdgh, bw1, bw2, bw3, bw4, bw0, 10);
            schedule_rounds4!(a_abef, a_cdgh, aw2, aw3, aw4, aw0, aw1, 11);
            schedule_rounds4!(b_abef, b_cdgh, bw2, bw3, bw4, bw0, bw1, 11);
            schedule_rounds4!(a_abef, a_cdgh, aw3, aw4, aw0, aw1, aw2, 12);
            schedule_rounds4!(b_abef, b_cdgh, bw3, bw4, bw0, bw1, bw2, 12);
            schedule_rounds4!(a_abef, a_cdgh, aw4, aw0, aw1, aw2, aw3, 13);
            schedule_rounds4!(b_abef, b_cdgh, bw4, bw0, bw1, bw2, bw3, 13);
            schedule_rounds4!(a_abef, a_cdgh, aw0, aw1, aw2, aw3, aw4, 14);
            schedule_rounds4!(b_abef, b_cdgh, bw0, bw1, bw2, bw3, bw4, 14);
            schedule_rounds4!(a_abef, a_cdgh, aw1, aw2, aw3, aw4, aw0, 15);
            schedule_rounds4!(b_abef, b_cdgh, bw1, bw2, bw3, bw4, bw0, 15);

            a_abef = _mm_add_epi32(a_abef, initial_abef);
            a_cdgh = _mm_add_epi32(a_cdgh, initial_cdgh);
            let feba = _mm_shuffle_epi32(a_abef, 0x1b);
            let dchg = _mm_shuffle_epi32(a_cdgh, 0xb1);
            a0 = _mm_blend_epi16(feba, dchg, 0xf0);
            a1 = _mm_alignr_epi8(dchg, feba, 8);

            b_abef = _mm_add_epi32(b_abef, initial_abef);
            b_cdgh = _mm_add_epi32(b_cdgh, initial_cdgh);
            let feba = _mm_shuffle_epi32(b_abef, 0x1b);
            let dchg = _mm_shuffle_epi32(b_cdgh, 0xb1);
            b0 = _mm_blend_epi16(feba, dchg, 0xf0);
            b1 = _mm_alignr_epi8(dchg, feba, 8);
        }

        let mut outputs = [[0u8; 32]; 2];
        for (output, words) in outputs.iter_mut().zip(&[[a0, a1], [b0, b1]]) {
            // SAFETY: each output is exactly 32 bytes and receives precisely
            // two unaligned 16-byte stores.
            let output_ptr = output.as_mut_ptr() as *mut __m128i;
            _mm_storeu_si128(output_ptr, _mm_shuffle_epi8(words[0], mask));
            _mm_storeu_si128(output_ptr.add(1), _mm_shuffle_epi8(words[1], mask));
        }
        outputs
    }

    /// Caller must first ensure that [`available`] returned true.
    #[inline(never)]
    pub unsafe fn chain_pair(inputs: [Hash; 2], count: u64) -> [Hash; 2] {
        chain_pair_inner(inputs, count)
    }
}

#[allow(dead_code)]
#[inline(never)]
pub fn verify_segments(anchor: PohHash, segments: &[PohSegment]) -> bool {
    let mut start = anchor;
    for segment in segments {
        let computed = next_hash(start, segment.num_hashes, segment.transaction_mixin);
        if computed != segment.expected {
            return false;
        }
        start = segment.expected;
    }
    true
}

/// Verify every independently anchored entry segment in parallel. The first
/// segment is anchored to trusted Bank state; each later segment starts at the
/// preceding claimed entry hash. Therefore all links and all PoH work must be
/// valid before this returns true.
#[allow(dead_code)]
#[inline(never)]
pub fn verify_segments_parallel(anchor: PohHash, segments: &[PohSegment]) -> bool {
    segments
        .par_chunks(2)
        .enumerate()
        .all(|(chunk_index, chunk)| {
            let index = chunk_index * 2;
            let first_start = if index == 0 {
                anchor
            } else {
                segments[index - 1].expected
            };
            match chunk {
                [first, second] => {
                    let computed = next_hash_pair(
                        [first_start, first.expected],
                        [first.num_hashes, second.num_hashes],
                        [first.transaction_mixin, second.transaction_mixin],
                    );
                    computed[0] == first.expected && computed[1] == second.expected
                }
                [first] => {
                    next_hash(first_start, first.num_hashes, first.transaction_mixin)
                        == first.expected
                }
                _ => unreachable!("chunks of two contain one or two segments"),
            }
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::{Rng, RngCore, SeedableRng};
    use rand_chacha::ChaChaRng;
    use solana_sdk::hash::{hash, hashv, Hash};

    fn rng() -> ChaChaRng {
        ChaChaRng::from_seed([0x73; 32])
    }

    fn sdk_hash32(input: &PohHash) -> PohHash {
        to_bytes(hash(input))
    }

    fn sdk_hash64(left: &PohHash, right: &PohHash) -> PohHash {
        to_bytes(hashv(&[left, right]))
    }

    fn to_bytes(hash: Hash) -> PohHash {
        let mut output = [0u8; 32];
        output.copy_from_slice(hash.as_ref());
        output
    }

    fn sdk_next_hash(
        mut current: PohHash,
        num_hashes: u64,
        transaction_mixin: Option<PohHash>,
    ) -> PohHash {
        if num_hashes == 0 && transaction_mixin.is_none() {
            return current;
        }
        for _ in 0..num_hashes.saturating_sub(1) {
            current = sdk_hash32(&current);
        }
        match transaction_mixin {
            None => sdk_hash32(&current),
            Some(mixin) => sdk_hash64(&current, &mixin),
        }
    }

    #[test]
    fn fixed_width_hashes_match_solana_sdk_randomized() {
        let mut rng = rng();
        for _ in 0..10_000 {
            let mut left = [0u8; 32];
            let mut right = [0u8; 32];
            rng.fill_bytes(&mut left);
            rng.fill_bytes(&mut right);
            assert_eq!(sdk_hash32(&left), hash32(&left));
            assert_eq!(sdk_hash64(&left, &right), hash64(&left, &right));
        }
    }

    #[test]
    fn every_next_hash_branch_matches_solana_sdk_randomized() {
        let mut rng = rng();
        for case in 0..2_000 {
            let mut start = [0u8; 32];
            let mut mixin = [0u8; 32];
            rng.fill_bytes(&mut start);
            rng.fill_bytes(&mut mixin);
            let count = match case % 8 {
                0 => 0,
                1 => 1,
                2 => 2,
                3 => 3,
                4 => 63,
                5 => 64,
                6 => 12_499,
                _ => rng.gen_range(0, 2_000),
            };
            let transaction_mixin = if rng.gen() { Some(mixin) } else { None };
            assert_eq!(
                sdk_next_hash(start, count, transaction_mixin),
                next_hash(start, count, transaction_mixin)
            );
            assert_eq!(
                next_hash_sha2(start, count, transaction_mixin),
                next_hash(start, count, transaction_mixin)
            );
        }
    }

    #[test]
    fn paired_hashes_match_two_sdk_chains_with_unequal_work() {
        let mut rng = rng();
        let counts = [0, 1, 2, 3, 17, 63, 511, 12_499, 12_500];
        for case in 0..2_000 {
            let mut starts = [[0u8; 32]; 2];
            let mut mixins = [[0u8; 32]; 2];
            rng.fill_bytes(&mut starts[0]);
            rng.fill_bytes(&mut starts[1]);
            rng.fill_bytes(&mut mixins[0]);
            rng.fill_bytes(&mut mixins[1]);
            let num_hashes = [
                counts[case % counts.len()],
                counts[(case * 5 + 3) % counts.len()],
            ];
            let transaction_mixins = [
                if case % 2 == 0 { Some(mixins[0]) } else { None },
                if case % 3 == 0 { Some(mixins[1]) } else { None },
            ];
            assert_eq!(
                [
                    sdk_next_hash(starts[0], num_hashes[0], transaction_mixins[0]),
                    sdk_next_hash(starts[1], num_hashes[1], transaction_mixins[1]),
                ],
                next_hash_pair(starts, num_hashes, transaction_mixins),
                "case {}",
                case,
            );
        }
    }

    fn mixed_segments(count: usize) -> (PohHash, Vec<PohSegment>) {
        let mut rng = rng();
        let mut anchor = [0u8; 32];
        rng.fill_bytes(&mut anchor);
        let mut start = anchor;
        let mut segments = Vec::with_capacity(count);
        for index in 0..count {
            let mut mixin = [0u8; 32];
            rng.fill_bytes(&mut mixin);
            let transaction_mixin = if index % 3 == 0 { Some(mixin) } else { None };
            let num_hashes = match index % 7 {
                0 => 0,
                1 => 1,
                2 => 2,
                3 => 17,
                4 => 63,
                5 => 511,
                _ => 12_500,
            };
            let expected = sdk_next_hash(start, num_hashes, transaction_mixin);
            segments.push(PohSegment {
                num_hashes,
                transaction_mixin,
                expected,
            });
            start = expected;
        }
        (anchor, segments)
    }

    #[test]
    fn sequential_and_parallel_verifiers_match_and_reject_corruption() {
        let (anchor, segments) = mixed_segments(257);
        assert!(verify_segments(anchor, &segments));
        assert!(verify_segments_parallel(anchor, &segments));
        for index in [0, segments.len() / 2, segments.len() - 1].iter().copied() {
            let mut corrupted = segments.clone();
            corrupted[index].expected[(index + 11) % 32] ^= 0x80;
            assert!(!verify_segments(anchor, &corrupted));
            assert!(!verify_segments_parallel(anchor, &corrupted));

            let mut corrupted = segments.clone();
            // 0 and 1 are digest-equivalent for transaction entries in this
            // historical algorithm, so add two to guarantee different work.
            corrupted[index].num_hashes = corrupted[index].num_hashes.saturating_add(2);
            assert!(!verify_segments(anchor, &corrupted));
            assert!(!verify_segments_parallel(anchor, &corrupted));
        }
    }

    #[test]
    fn transaction_entries_with_zero_and_one_hash_are_digest_equivalent() {
        let start = [0x31; 32];
        let mixin = [0x92; 32];
        assert_eq!(
            next_hash(start, 0, Some(mixin)),
            next_hash(start, 1, Some(mixin))
        );
    }
}
