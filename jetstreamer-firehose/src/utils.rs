use {
    crate::SharedError,
    base64::engine::{Engine, general_purpose::STANDARD},
    std::{
        io::{self, Read},
        vec::Vec,
    },
};

const MAX_VARINT_LEN_64: usize = 10;

/// Reads an unsigned LEB128-encoded integer from the provided reader.
pub fn read_uvarint<R: Read>(reader: &mut R) -> io::Result<u64> {
    let mut x = 0u64;
    let mut s = 0u32;
    let mut buffer = [0u8; 1];
    for i in 0..MAX_VARINT_LEN_64 {
        reader.read_exact(&mut buffer)?;
        let b = buffer[0];
        if b < 0x80 {
            if i == MAX_VARINT_LEN_64 - 1 && b > 1 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "uvarint overflow",
                ));
            }
            return Ok(x | ((b as u64) << s));
        }
        x |= ((b & 0x7f) as u64) << s;
        s += 7;

        if s > 63 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "uvarint too long",
            ));
        }
    }
    Err(io::Error::new(
        io::ErrorKind::InvalidData,
        "uvarint overflow",
    ))
}

/// Owner type for 32-byte hashes that renders them as lowercase hex.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct Hash(#[doc = "Underlying bytes comprising the hash."] pub Vec<u8>);

// debug converts the hash to hex
impl std::fmt::Debug for Hash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut hex = String::new();
        for byte in &self.0 {
            hex.push_str(&format!("{:02x}", byte));
        }
        write!(f, "{}", hex)
    }
}

// implement stringer for hash
impl std::fmt::Display for Hash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut hex = String::new();
        for byte in &self.0 {
            hex.push_str(&format!("{:02x}", byte));
        }
        write!(f, "{}", hex)
    }
}

// implement serde serialization for hash
impl serde::Serialize for Hash {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        let mut hex = String::new();
        for byte in &self.0 {
            hex.push_str(&format!("{:02x}", byte));
        }
        serializer.serialize_str(&hex)
    }
}

// implement serde deserialization for hash
impl<'de> serde::Deserialize<'de> for Hash {
    fn deserialize<D>(deserializer: D) -> Result<Hash, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        let hex = String::deserialize(deserializer)?;
        let mut bytes = vec![];
        for i in 0..hex.len() / 2 {
            bytes.push(u8::from_str_radix(&hex[2 * i..2 * i + 2], 16).unwrap());
        }
        Ok(Hash(bytes))
    }
}

impl Hash {
    /// Returns the hash bytes as a `Vec<u8>`.
    pub fn to_vec(&self) -> Vec<u8> {
        self.0.clone()
    }

    /// Constructs a [`struct@Hash`] from owned bytes.
    pub const fn from_vec(data: Vec<u8>) -> Hash {
        Hash(data)
    }

    /// Returns the hash as a 32-byte array.
    ///
    /// # Panics
    ///
    /// Panics if the underlying byte slice is shorter than 32 bytes.
    pub fn to_bytes(&self) -> [u8; 32] {
        let mut bytes = [0u8; 32];
        bytes[..32].copy_from_slice(&self.0[..32]);
        bytes
    }
}

/// Growable binary buffer with base64 formatting helpers.
#[derive(Default, Clone, PartialEq, Eq, Hash)]
pub struct Buffer(#[doc = "Owned bytes stored in the buffer."] Vec<u8>);

impl Buffer {
    /// Creates an empty buffer.
    pub const fn new() -> Buffer {
        Buffer(vec![])
    }

    /// Appends `data` to the buffer.
    pub fn write(&mut self, data: Vec<u8>) {
        self.0.extend(data);
    }

    /// Removes and returns `len` bytes from the front of the buffer.
    ///
    /// # Panics
    ///
    /// Panics if `len` exceeds the available bytes.
    pub fn read(&mut self, len: usize) -> Vec<u8> {
        let mut data = vec![];
        for _ in 0..len {
            data.push(self.0.remove(0));
        }
        data
    }

    /// Returns the buffer length in bytes.
    pub const fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns `true` if the buffer is empty.
    pub const fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the buffer contents as a borrowed slice.
    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }

    /// Returns the buffer contents as a `Vec<u8>`.
    pub fn to_vec(&self) -> Vec<u8> {
        self.0.clone()
    }

    /// Creates a buffer from owned bytes.
    pub const fn from_vec(data: Vec<u8>) -> Buffer {
        Buffer(data)
    }
}

impl std::fmt::Debug for Buffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Buffer").field("data", &self.0).finish()
    }
}

// base64
impl std::fmt::Display for Buffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        STANDARD.encode(&self.0).fmt(f)
    }
}

impl serde::Serialize for Buffer {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        STANDARD.encode(&self.0).serialize(serializer)
    }
}

impl<'de> serde::Deserialize<'de> for Buffer {
    fn deserialize<D>(deserializer: D) -> Result<Buffer, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        let base64 = String::deserialize(deserializer)?;
        Ok(Buffer(STANDARD.decode(base64).unwrap()))
    }
}

/// Maximum Old Faithful CAR section size permitted while parsing (32 MiB).
pub const MAX_ALLOWED_SECTION_SIZE: usize = 32 << 20; // 32MiB

/// Maximum size of a reassembled Old Faithful frame (128 MiB).
///
/// The largest epoch-start reward frame found across epochs 0 through 954 is
/// 63,448,722 compressed bytes, so this retains more than 2x headroom.
pub const MAX_ALLOWED_REASSEMBLED_FRAME_SIZE: usize = 128 << 20; // 128MiB

/// Maximum decompressed size of an Old Faithful frame (128 MiB).
///
/// That same CID-verified reward frame expands to 87,557,613 bytes.
pub const MAX_ALLOWED_DECOMPRESSED_FRAME_SIZE: usize = 128 << 20; // 128MiB

/// Maximum compressed or decompressed transaction-metadata frame (1 MiB).
/// Transaction metadata is handled once per transaction and is much smaller
/// than a slot-wide rewards frame; sampled historical metadata peaks below
/// one KiB.
pub const MAX_TRANSACTION_METADATA_FRAME_SIZE: usize = 1 << 20; // 1MiB

const _: () = {
    assert!(MAX_ALLOWED_REASSEMBLED_FRAME_SIZE >= 63_448_722);
    assert!(MAX_ALLOWED_DECOMPRESSED_FRAME_SIZE >= 87_557_613);
};

/// Decompresses a Zstandard byte stream to at most
/// [`MAX_ALLOWED_DECOMPRESSED_FRAME_SIZE`] bytes.
pub fn decompress_zstd(data: &[u8]) -> Result<Vec<u8>, SharedError> {
    decompress_zstd_with_limit(data, MAX_ALLOWED_DECOMPRESSED_FRAME_SIZE)
}

pub(crate) fn decompress_zstd_with_limit(
    data: &[u8],
    max_output_size: usize,
) -> Result<Vec<u8>, SharedError> {
    let mut decoder = zstd::Decoder::new(data)?;
    decoder.window_log_max(zstd_window_log_for_limit(max_output_size))?;
    let read_limit = u64::try_from(max_output_size)
        .unwrap_or(u64::MAX)
        .saturating_add(1);
    let mut decompressed = Vec::new();
    decoder
        .by_ref()
        .take(read_limit)
        .read_to_end(&mut decompressed)?;
    if decompressed.len() > max_output_size {
        return Err(Box::new(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("decompressed frame exceeds {max_output_size} bytes"),
        )));
    }
    Ok(decompressed)
}

fn zstd_window_log_for_limit(max_output_size: usize) -> u32 {
    // The streaming encoder uses a window larger than tiny test payloads, so
    // keep a 1 MiB floor while still matching the metadata allocation limit.
    const ZSTD_WINDOW_LOG_MIN: u32 = 20;
    const ZSTD_WINDOW_LOG_MAX: u32 = 27;

    let required_log = usize::BITS - max_output_size.saturating_sub(1).leading_zeros();
    required_log.clamp(ZSTD_WINDOW_LOG_MIN, ZSTD_WINDOW_LOG_MAX)
}

#[cfg(test)]
mod decompression_tests {
    use super::*;
    use std::io::Write as _;

    #[test]
    fn bounded_zstd_decompression_accepts_the_exact_limit() {
        let compressed = zstd::encode_all([1, 2, 3].as_slice(), 1).unwrap();
        assert_eq!(
            decompress_zstd_with_limit(&compressed, 3).unwrap(),
            [1, 2, 3]
        );
    }

    #[test]
    fn bounded_zstd_decompression_rejects_output_past_the_limit() {
        let compressed = zstd::encode_all([1, 2, 3].as_slice(), 1).unwrap();
        let error = decompress_zstd_with_limit(&compressed, 2).unwrap_err();
        assert!(error.to_string().contains("exceeds 2 bytes"));
    }

    #[test]
    fn bounded_zstd_decompression_rejects_large_decoder_window() {
        let mut encoder = zstd::stream::write::Encoder::new(Vec::new(), 1).unwrap();
        encoder.window_log(27).unwrap();
        encoder.include_contentsize(false).unwrap();
        encoder.write_all(&[1]).unwrap();
        let compressed = encoder.finish().unwrap();

        assert_eq!(zstd::decode_all(compressed.as_slice()).unwrap(), [1]);
        assert!(decompress_zstd_with_limit(&compressed, 1 << 20).is_err());
    }

    #[test]
    fn zstd_decoder_window_tracks_output_limit() {
        assert_eq!(zstd_window_log_for_limit(0), 20);
        assert_eq!(zstd_window_log_for_limit(1 << 10), 20);
        assert_eq!(zstd_window_log_for_limit(1 << 20), 20);
        assert_eq!(zstd_window_log_for_limit((1 << 20) + 1), 21);
        assert_eq!(zstd_window_log_for_limit(usize::MAX), 27);
    }
}
