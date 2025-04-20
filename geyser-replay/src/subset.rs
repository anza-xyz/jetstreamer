//! Rust mirror of the “Subset” tuple in the Old‑Faithful DAG.

use cid::Cid;
use serde_cbor::Value;
use std::error::Error;
use tokio::io::{AsyncRead, AsyncReadExt};

/// DAG discriminator (`Kind::Subset == 3`).
pub const SUBSET_KIND: u64 = 3;

/// Native Rust representation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Subset {
    pub kind: u64,
    pub first: u64,
    pub last: u64,
    pub blocks: Vec<Cid>,
}

impl Subset {
    // ──────────────────────────────────────────────────────────────────────────
    // Public API
    // ──────────────────────────────────────────────────────────────────────────
    /// Read one Subset CBOR tuple from any `AsyncRead` stream.
    pub async fn try_read<R>(reader: &mut R) -> Result<Self, Box<dyn Error>>
    where
        R: AsyncRead + Unpin,
    {
        let mut buf = Vec::<u8>::with_capacity(128);

        loop {
            // read one more byte
            let mut byte = [0u8; 1];
            reader.read_exact(&mut byte).await?;
            buf.push(byte[0]);

            match serde_cbor::from_slice::<Value>(&buf) {
                Ok(val) => return Self::from_cbor_value(val), // ✅ got it
                Err(e) if e.is_eof() => continue,             // need more data
                Err(e) => return Err(e.into()),               // real CBOR error
            }
        }
    }

    // ──────────────────────────────────────────────────────────────────────────
    // Conversion helper (unchanged logic, no Cursor needed)
    // ──────────────────────────────────────────────────────────────────────────
    pub fn from_cbor_value(v: Value) -> Result<Self, Box<dyn Error>> {
        let arr = match v {
            Value::Array(a) => a,
            _ => return Err("subset: expected CBOR array".into()),
        };
        if arr.len() != 4 {
            return Err(format!("subset: expected 4‑tuple, got {}", arr.len()).into());
        }

        // helper: pull integer out of Value
        let int = |v: &Value| -> Result<u64, Box<dyn Error>> {
            match v {
                Value::Integer(i) => Ok(*i as u64),
                _ => Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "subset: expected integer",
                )
                .into()),
            }
        };

        let kind = int(&arr[0])?;
        let first = int(&arr[1])?;
        let last = int(&arr[2])?;

        // 4th element: array of byte‑strings that encode raw CIDs
        let blocks = match &arr[3] {
            Value::Array(vec) => vec
                .iter()
                .map(|item| match item {
                    Value::Bytes(b) => Cid::read_bytes(&b[..])
                        .map_err(|e| format!("subset: invalid CID in blocks: {e}")),
                    _ => Err("subset: blocks element is not bytes".into()),
                })
                .collect::<Result<Vec<_>, _>>()?,
            _ => return Err("subset: blocks is not an array".into()),
        };

        Ok(Self {
            kind,
            first,
            last,
            blocks,
        })
    }
}

#[tokio::test]
async fn read_subset_from_stream() {
    use crate::epochs_async::fetch_epoch_stream;
    let client = reqwest::Client::new();
    let mut stream = fetch_epoch_stream(670, &client).await;

    let subset = crate::subset::Subset::try_read(&mut stream).await.unwrap();
    assert_eq!(subset.kind, SUBSET_KIND);
    println!(
        "subset {}..{} with {} blocks",
        subset.first,
        subset.last,
        subset.blocks.len()
    );
}
