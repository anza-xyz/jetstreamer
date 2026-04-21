//! Pre-computed list of the most frequently referenced Solana pubkeys.
//!
//! Derived from a 150-epoch scan of all transaction account keys via the
//! `PubkeyStatsPlugin`. The list is ordered by descending mention count, so
//! index 0 is the single most common pubkey across the sampled epochs.
//!
//! Exactly 65 535 entries are included — the largest count that fits entirely
//! within 3-byte lencode varint IDs when loaded into a `DedupeEncoder`,
//! saving 29-31 bytes per hit vs. emitting a full 32-byte address.
//!
//! To regenerate: re-run the pubkey stats plugin, dump the top 65 535 rows
//! ordered by `sum(num_mentions)` to CSV, and convert to raw 32-byte records.

/// Number of pubkeys in the prime list.
pub const POPULAR_PUBKEY_COUNT: usize = 65_535;

const PUBKEY_PRIME_BYTES: &[u8; POPULAR_PUBKEY_COUNT * 32] = include_bytes!("pubkey_prime.bin");

/// The top [`POPULAR_PUBKEY_COUNT`] most-referenced Solana pubkeys, as raw
/// 32-byte arrays, ordered by descending mention frequency.
pub const POPULAR_PUBKEYS: &[[u8; 32]; POPULAR_PUBKEY_COUNT] = unsafe {
    // SAFETY: `pubkey_prime.bin` is exactly 65 535 * 32 bytes, and `[[u8; 32]; N]`
    // has the same layout as `[u8; N * 32]`.
    &*(PUBKEY_PRIME_BYTES.as_ptr() as *const [[u8; 32]; POPULAR_PUBKEY_COUNT])
};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn has_expected_count() {
        assert_eq!(POPULAR_PUBKEYS.len(), 65_535);
    }

    #[test]
    fn first_entries_are_top_programs() {
        // Vote program: Vote111111111111111111111111111111111111111
        assert_eq!(
            POPULAR_PUBKEYS[0],
            [
                0x07, 0x61, 0x48, 0x1d, 0x35, 0x74, 0x74, 0xbb, 0x7c, 0x4d, 0x76, 0x24, 0xeb, 0xd3,
                0xbd, 0xb3, 0xd8, 0x35, 0x5e, 0x73, 0xd1, 0x10, 0x43, 0xfc, 0x0d, 0xa3, 0x53, 0x80,
                0x00, 0x00, 0x00, 0x00,
            ]
        );
        // System program: 11111111111111111111111111111111 (all zeros)
        assert_eq!(POPULAR_PUBKEYS[2], [0u8; 32]);
    }

    #[test]
    fn all_entries_unique() {
        use std::collections::HashSet;
        let set: HashSet<&[u8; 32]> = POPULAR_PUBKEYS.iter().collect();
        assert_eq!(set.len(), POPULAR_PUBKEYS.len());
    }
}
