#[path = "../src/poh_backend.rs"]
mod poh_backend;

use poh_backend::{next_hash, verify_segments, verify_segments_parallel, PohHash, PohSegment};
use rayon::ThreadPoolBuilder;
use sha2_fast::{Digest, Sha256};
use solana_sdk::hash::{hash, hashv};
use std::{env, mem, ptr, time::Instant};

const HASHES_PER_TICK: u64 = 12_500;
const TICKS_PER_SLOT: usize = 64;

type NextHashFn = fn(PohHash, u64, Option<PohHash>) -> PohHash;

fn sdk_hash32(input: &PohHash) -> PohHash {
    let digest = hash(input);
    let mut output = [0u8; 32];
    output.copy_from_slice(digest.as_ref());
    output
}

fn sdk_hash64(left: &PohHash, right: &PohHash) -> PohHash {
    let digest = hashv(&[left, right]);
    let mut output = [0u8; 32];
    output.copy_from_slice(digest.as_ref());
    output
}

#[inline(never)]
fn next_hash_legacy(
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

#[inline(never)]
fn next_hash_digest(
    mut current: PohHash,
    num_hashes: u64,
    transaction_mixin: Option<PohHash>,
) -> PohHash {
    if num_hashes == 0 && transaction_mixin.is_none() {
        return current;
    }
    for _ in 0..num_hashes.saturating_sub(1) {
        let digest = Sha256::digest(&current);
        current.copy_from_slice(digest.as_slice());
    }
    match transaction_mixin {
        None => {
            let digest = Sha256::digest(&current);
            current.copy_from_slice(digest.as_slice());
            current
        }
        Some(mixin) => {
            let mut hasher = Sha256::new();
            hasher.update(&current);
            hasher.update(&mixin);
            let digest = hasher.finalize();
            current.copy_from_slice(digest.as_slice());
            current
        }
    }
}

fn verify_with(anchor: PohHash, segments: &[PohSegment], next: NextHashFn) -> bool {
    let mut start = anchor;
    for segment in segments {
        if next(start, segment.num_hashes, segment.transaction_mixin) != segment.expected {
            return false;
        }
        start = segment.expected;
    }
    true
}

fn build_segments(slots: usize) -> (PohHash, Vec<PohSegment>) {
    let mut anchor = [0u8; 32];
    for (index, byte) in anchor.iter_mut().enumerate() {
        *byte = (index as u8).wrapping_mul(17).wrapping_add(31);
    }
    let mut start = anchor;
    let mut segments = Vec::with_capacity(slots * TICKS_PER_SLOT);
    for _ in 0..slots * TICKS_PER_SLOT {
        let expected = next_hash(start, HASHES_PER_TICK, None);
        segments.push(PohSegment {
            num_hashes: HASHES_PER_TICK,
            transaction_mixin: None,
            expected,
        });
        start = expected;
    }
    (anchor, segments)
}

#[inline(never)]
fn black_box<T>(value: T) -> T {
    unsafe {
        let result = ptr::read_volatile(&value);
        mem::forget(value);
        result
    }
}

fn measure(label: &str, slots: usize, samples: usize, mut verify: impl FnMut() -> bool) {
    for _ in 0..2 {
        assert!(black_box(verify()));
    }
    let mut elapsed = Vec::with_capacity(samples);
    for sample in 0..samples {
        let started = Instant::now();
        assert!(black_box(verify()));
        let duration = started.elapsed();
        let nanos =
            u128::from(duration.as_secs()) * 1_000_000_000 + u128::from(duration.subsec_nanos());
        elapsed.push(nanos);
        println!(
            "SAMPLE label={} index={} elapsed_ns={}",
            label, sample, nanos
        );
    }
    elapsed.sort_unstable();
    let median_ns = elapsed[elapsed.len() / 2];
    let hashes = slots as f64 * TICKS_PER_SLOT as f64 * HASHES_PER_TICK as f64;
    println!(
        "RESULT label={} slots={} samples={} median_ns={} min_ns={} max_ns={} per_slot_ms={:.6} mhps={:.6}",
        label,
        slots,
        samples,
        median_ns,
        elapsed[0],
        elapsed[elapsed.len() - 1],
        median_ns as f64 / slots as f64 / 1_000_000.0,
        hashes * 1_000.0 / median_ns as f64,
    );
}

fn parse_usize(index: usize, default: usize) -> usize {
    env::args()
        .nth(index)
        .and_then(|argument| argument.parse().ok())
        .unwrap_or(default)
}

fn main() {
    let mode = env::args().nth(1).unwrap_or_else(|| "fixed".to_string());
    let slots = parse_usize(2, 4);
    let samples = parse_usize(3, 9);
    let threads = parse_usize(4, 1);
    assert!(slots > 0 && samples > 0 && threads > 0);

    let (anchor, segments) = build_segments(slots);
    let proof = &segments[..TICKS_PER_SLOT.min(segments.len())];
    assert!(verify_with(anchor, proof, next_hash_legacy));
    assert!(verify_with(anchor, proof, next_hash_digest));
    assert!(verify_segments(anchor, proof));

    match mode.as_str() {
        "legacy" => measure("solana-sdk-sha2-0.8.1", slots, samples, || {
            verify_with(anchor, &segments, next_hash_legacy)
        }),
        "digest" => measure("sha2-0.9.9-digest", slots, samples, || {
            verify_with(anchor, &segments, next_hash_digest)
        }),
        "fixed" => measure("sha2-0.9.9-fixed-compress", slots, samples, || {
            verify_segments(anchor, &segments)
        }),
        "fixed-parallel" => {
            let pool = ThreadPoolBuilder::new()
                .num_threads(threads)
                .build()
                .unwrap();
            measure(
                &format!("sha2-0.9.9-fixed-parallel-{}t", threads),
                slots,
                samples,
                || pool.install(|| verify_segments_parallel(anchor, &segments)),
            );
        }
        other => panic!("unknown benchmark mode {}", other),
    }
}
