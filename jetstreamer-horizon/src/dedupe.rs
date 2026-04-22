//! Pre-primed dedupe encoder/decoder backed by [`POPULAR_PUBKEYS`].
//!
//! At program startup we build a [`FrozenEncoderState`] / [`FrozenDecoderState`]
//! populated with the 65 535 most-referenced Solana pubkeys (see
//! [`crate::pubkey_prime`]). Those frozen states are wrapped in [`Arc`] and
//! shared across every worker encoder/decoder via [`new_encoder_context`] /
//! [`new_decoder_context`].
//!
//! When encoding an [`Address`] that appears in the popular list, the wire
//! output is a 1-3 byte varint ID instead of a full 32-byte pubkey — a ~10-16×
//! saving on the dominant case in Solana account/transaction streams.
//!
//! # Per-slot reset
//!
//! Call [`reset_encoder`] / [`reset_decoder`] between slot boundaries. This
//! clears only the small per-worker scratch layer (novel pubkeys seen in the
//! current slot); the shared 65 535-entry primed table stays intact at zero
//! cost. Slots are independently decodable — random-access seeks are safe.
//!
//! # Example
//!
//! ```ignore
//! use jetstreamer_horizon::dedupe::{new_encoder_context, reset_encoder};
//! use lencode::prelude::*;
//!
//! let mut ctx = new_encoder_context();
//! for slot in stream {
//!     reset_encoder(&mut ctx);
//!     for update in slot.account_updates {
//!         update.encode_ext(&mut writer, Some(&mut ctx)).unwrap();
//!     }
//! }
//! ```
use std::sync::Arc;

use lencode::context::{DecoderContext, EncoderContext};
use lencode::dedupe::{DedupeDecoder, DedupeEncoder, FrozenDecoderState, FrozenEncoderState};
use once_cell::sync::Lazy;
use solana_address::Address;

use crate::pubkey_prime::POPULAR_PUBKEYS;

/// Lazily-initialised frozen encoder state populated from [`POPULAR_PUBKEYS`].
///
/// First access triggers a one-time priming pass over the 65 535-entry const
/// slice (~10-20 ms on modern hardware). Subsequent accesses are free — the
/// wrapped [`Arc`] is cloned cheaply for each worker encoder.
pub static FROZEN_PUBKEY_ENCODER: Lazy<Arc<FrozenEncoderState>> = Lazy::new(|| {
    let mut primer = DedupeEncoder::new();
    for pk_bytes in POPULAR_PUBKEYS.iter() {
        let addr = Address::from(*pk_bytes);
        primer.prime::<Address, <Address as lencode::dedupe::DedupeEncodeable>::Hasher>(&addr);
    }
    Arc::new(primer.freeze())
});

/// Lazily-initialised frozen decoder state populated from [`POPULAR_PUBKEYS`].
///
/// Mirrors [`FROZEN_PUBKEY_ENCODER`] — values are primed in the exact same
/// order so wire IDs align on both sides.
pub static FROZEN_PUBKEY_DECODER: Lazy<Arc<FrozenDecoderState>> = Lazy::new(|| {
    let mut primer = DedupeDecoder::new();
    for pk_bytes in POPULAR_PUBKEYS.iter() {
        primer.prime::<Address>(Address::from(*pk_bytes));
    }
    Arc::new(primer.freeze())
});

/// Creates a fresh [`EncoderContext`] with a [`DedupeEncoder`] backed by the
/// shared [`FROZEN_PUBKEY_ENCODER`] frozen state.
///
/// The returned context is cheap to create (one [`Arc::clone`] + a small
/// scratch allocation). Use one per worker thread.
#[inline]
pub fn new_encoder_context() -> EncoderContext {
    EncoderContext {
        dedupe: Some(DedupeEncoder::with_frozen(Arc::clone(&FROZEN_PUBKEY_ENCODER))),
        diff: None,
    }
}

/// Creates a fresh [`DecoderContext`] with a [`DedupeDecoder`] backed by the
/// shared [`FROZEN_PUBKEY_DECODER`] frozen state.
#[inline]
pub fn new_decoder_context() -> DecoderContext {
    DecoderContext {
        dedupe: Some(DedupeDecoder::with_frozen(Arc::clone(&FROZEN_PUBKEY_DECODER))),
        diff: None,
    }
}

/// Clears the scratch layer of the encoder inside `ctx`, preserving the frozen
/// primed table. Call this at each slot boundary so per-slot novel pubkeys
/// don't accumulate across slots (which would break random-access decoding).
#[inline]
pub fn reset_encoder(ctx: &mut EncoderContext) {
    if let Some(enc) = ctx.dedupe.as_mut() {
        enc.clear();
    }
}

/// Clears the scratch layer of the decoder inside `ctx`, preserving the frozen
/// primed table. Companion to [`reset_encoder`].
#[inline]
pub fn reset_decoder(ctx: &mut DecoderContext) {
    if let Some(dec) = ctx.dedupe.as_mut() {
        dec.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use lencode::io::Cursor;
    use lencode::prelude::*;

    #[test]
    fn frozen_states_have_expected_count() {
        assert_eq!(FROZEN_PUBKEY_ENCODER.len(), POPULAR_PUBKEYS.len());
        assert_eq!(FROZEN_PUBKEY_DECODER.len(), POPULAR_PUBKEYS.len());
    }

    #[test]
    fn popular_pubkey_encodes_to_small_varint() {
        // Encoding any pubkey in the top 127 should collapse to a single byte.
        let mut ctx = new_encoder_context();
        let popular = Address::from(POPULAR_PUBKEYS[0]);
        let mut buf = Vec::new();
        let n = popular
            .encode_ext(&mut buf, Some(&mut ctx))
            .expect("encode popular");
        assert_eq!(n, 1, "top-ranked pubkey should encode to 1 byte");
        assert_eq!(buf, vec![1]);
    }

    #[test]
    fn popular_pubkey_encodes_small_in_each_varint_zone() {
        // Lencode varint sizes:
        //   ID 1..=127  → 1 byte
        //   ID 128..=255 → 2 bytes
        //   ID 256..=65535 → 3 bytes
        // Verify each zone produces the expected wire length.
        let cases = [
            (0usize, 1usize),    // ID 1 → 1 byte
            (126, 1),            // ID 127 → 1 byte
            (127, 2),            // ID 128 → 2 bytes
            (254, 2),            // ID 255 → 2 bytes
            (255, 3),            // ID 256 → 3 bytes
            (65_534, 3),         // ID 65535 → 3 bytes
        ];
        for (index, expected_bytes) in cases {
            let mut ctx = new_encoder_context();
            let popular = Address::from(POPULAR_PUBKEYS[index]);
            let mut buf = Vec::new();
            let n = popular
                .encode_ext(&mut buf, Some(&mut ctx))
                .expect("encode popular");
            assert_eq!(
                n, expected_bytes,
                "index {index} should encode to {expected_bytes} bytes, got {n}"
            );
        }
    }

    #[test]
    fn novel_pubkey_encodes_full_32_bytes() {
        // A pubkey NOT in the popular list encodes as ID 0 + 32 raw bytes.
        let mut ctx = new_encoder_context();
        let novel = Address::from([0xAAu8; 32]);
        let mut buf = Vec::new();
        let n = novel
            .encode_ext(&mut buf, Some(&mut ctx))
            .expect("encode novel");
        // 1 byte for ID=0 marker + 32 bytes payload
        assert_eq!(n, 33);
        assert_eq!(buf[0], 0);
    }

    #[test]
    fn roundtrip_mixed_popular_and_novel() {
        let mut enc = new_encoder_context();
        let mut dec = new_decoder_context();

        let addrs: Vec<Address> = vec![
            Address::from(POPULAR_PUBKEYS[0]),
            Address::from([0xDEu8; 32]), // novel
            Address::from(POPULAR_PUBKEYS[5000]),
            Address::from(POPULAR_PUBKEYS[0]), // repeat popular
            Address::from([0xDEu8; 32]),       // repeat novel (should be scratch hit)
        ];

        let mut buf = Vec::new();
        for a in &addrs {
            a.encode_ext(&mut buf, Some(&mut enc)).unwrap();
        }

        let mut cursor = Cursor::new(&buf[..]);
        for expected in &addrs {
            let decoded = Address::decode_ext(&mut cursor, Some(&mut dec)).unwrap();
            assert_eq!(decoded, *expected);
        }
    }

    #[test]
    fn reset_clears_scratch_but_preserves_frozen() {
        let mut enc = new_encoder_context();
        let mut dec = new_decoder_context();

        let novel = Address::from([0xBBu8; 32]);
        let popular = Address::from(POPULAR_PUBKEYS[0]);

        let mut buf = Vec::new();
        novel.encode_ext(&mut buf, Some(&mut enc)).unwrap();
        popular.encode_ext(&mut buf, Some(&mut enc)).unwrap();

        let mut cursor = Cursor::new(&buf[..]);
        assert_eq!(
            Address::decode_ext(&mut cursor, Some(&mut dec)).unwrap(),
            novel
        );
        assert_eq!(
            Address::decode_ext(&mut cursor, Some(&mut dec)).unwrap(),
            popular
        );

        // Reset between slots — novel should be re-encoded from scratch, but
        // popular should still hit the frozen layer.
        reset_encoder(&mut enc);
        reset_decoder(&mut dec);

        let mut buf2 = Vec::new();
        novel.encode_ext(&mut buf2, Some(&mut enc)).unwrap();
        popular.encode_ext(&mut buf2, Some(&mut enc)).unwrap();

        // Novel gets the same scratch ID it had before (first novel after frozen).
        assert_eq!(buf, buf2, "reset should produce identical output for repeat slot");
    }
}
