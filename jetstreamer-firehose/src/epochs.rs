use reqwest::Client;
use rseek::Seekable;
use serde::Deserialize;
use std::fmt;
use tokio::io::{AsyncRead, AsyncSeek, BufReader};

use crate::node_reader::Len;

/// Default base URL used to fetch compact epoch CAR archives hosted by Old Faithful.
pub const BASE_URL: &str = "https://files.old-faithful.net";

#[inline(always)]
/// Returns the inclusive slot range covered by a Solana epoch.
///
/// The tuple contains the first slot and the final slot of the epoch.
pub const fn epoch_to_slot_range(epoch: u64) -> (u64, u64) {
    let first = epoch * 432000;
    (first, first + 431999)
}

#[inline(always)]
/// Converts a slot back into the epoch that contains it.
pub const fn slot_to_epoch(slot: u64) -> u64 {
    slot / 432000
}

/* ────────────────────────────────────────────────────────────────────────── */
/*  Blanket Len impl so BufReader<T> keeps the .len() we rely on            */
/* ────────────────────────────────────────────────────────────────────────── */
impl<T: Len + AsyncRead> Len for BufReader<T> {
    #[inline]
    fn len(&self) -> u64 {
        self.get_ref().len()
    }
}

/// Checks [`BASE_URL`] to determine whether the Old Faithful CAR archive for an epoch exists.
pub async fn epoch_exists(epoch: u64, client: &Client) -> bool {
    let url = format!("{}/{}/epoch-{}.car", BASE_URL, epoch, epoch);
    let response = client.head(&url).send().await;
    match response {
        Ok(res) => res.status().is_success(),
        Err(_) => false,
    }
}

/// Fetches an epoch’s CAR file from Old Faithful as a buffered, seekable async stream.
///
/// The returned reader implements [`Len`] and can be consumed sequentially or
/// randomly via [`AsyncSeek`].
pub async fn fetch_epoch_stream(epoch: u64, client: &Client) -> impl AsyncRead + AsyncSeek + Len {
    let client = client.clone();
    let seekable =
        Seekable::new(move || client.get(format!("{}/{}/epoch-{}.car", BASE_URL, epoch, epoch)))
            .await;

    BufReader::with_capacity(8 * 1024 * 1024, seekable)
}

/// Errors that can occur when calling [`get_slot_timestamp`].
#[derive(Debug)]
pub enum SlotTimestampError {
    /// Network request failed while contacting the RPC endpoint.
    Transport(reqwest::Error),
    /// JSON payload could not be decoded.
    Decode(serde_json::Error),
    /// RPC returned an error object instead of a result.
    Rpc(Option<serde_json::Value>),
    /// The RPC response did not include a block time.
    NoBlockTime,
}
impl fmt::Display for SlotTimestampError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SlotTimestampError::Transport(e) => write!(f, "RPC transport error: {e}"),
            SlotTimestampError::Decode(e) => write!(f, "RPC decode error: {e}"),
            SlotTimestampError::Rpc(e) => write!(f, "RPC error: {:?}", e),
            SlotTimestampError::NoBlockTime => write!(f, "No blockTime found in getBlock result"),
        }
    }
}
impl std::error::Error for SlotTimestampError {}

/// Get the true Unix timestamp (seconds since epoch, UTC) for a Solana slot.
/// Uses the validator RPC getBlock method, returns Ok(timestamp) or Err(reason).
pub async fn get_slot_timestamp(
    slot: u64,
    rpc_url: &str,
    client: &Client,
) -> Result<u64, SlotTimestampError> {
    #[derive(Deserialize)]
    struct BlockResult {
        #[serde(rename = "blockTime")]
        block_time: Option<u64>,
    }
    #[derive(Deserialize)]
    struct RpcResponse {
        result: Option<BlockResult>,
        error: Option<serde_json::Value>,
    }

    let req = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getBlock",
        "params": [slot, { "maxSupportedTransactionVersion": 0 }],
    });

    let resp = client
        .post(rpc_url)
        .json(&req)
        .send()
        .await
        .map_err(SlotTimestampError::Transport)?;

    let text = resp.text().await.map_err(SlotTimestampError::Transport)?;
    let resp_val: RpcResponse = serde_json::from_str(&text).map_err(SlotTimestampError::Decode)?;

    if resp_val.error.is_some() {
        return Err(SlotTimestampError::Rpc(resp_val.error));
    }
    resp_val
        .result
        .and_then(|r| r.block_time)
        .ok_or(SlotTimestampError::NoBlockTime)
}

/* ── Tests ──────────────────────────────────────────────────────────────── */
#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncSeekExt};

    #[tokio::test]
    async fn test_fetch_epoch_stream() {
        let client = reqwest::Client::new();
        let mut stream = fetch_epoch_stream(670, &client).await;

        /* first 1 KiB */
        let mut buf = vec![0u8; 1024];
        stream.read_exact(&mut buf).await.unwrap();
        assert_eq!(buf[0], 58);

        /* last 1 KiB */
        stream.seek(std::io::SeekFrom::End(-1024)).await.unwrap();
        stream.read_exact(&mut buf).await.unwrap();
        assert_eq!(buf[1], 1);
    }

    #[tokio::test]
    async fn test_get_slot_timestamp() {
        // well-known public Solana RPC, slot 246446651 occurred in Apr 2024
        let client = reqwest::Client::new();
        let rpc_url = "https://api.mainnet-beta.solana.com";
        let slot = 246446651u64;
        let ts = get_slot_timestamp(slot, rpc_url, &client)
            .await
            .expect("should get a timestamp for valid slot");
        // Unix timestamp should be after 2023, plausibility check (> 1672531200 = Jan 1, 2023)
        assert!(ts > 1672531200, "timestamp was {}", ts);
    }
}

#[tokio::test]
async fn test_epoch_exists() {
    let client = reqwest::Client::new();
    assert!(epoch_exists(670, &client).await);
    assert!(!epoch_exists(999999, &client).await);
}

#[test]
fn test_epoch_to_slot() {
    assert_eq!(epoch_to_slot_range(0), (0, 431999));
    assert_eq!(epoch_to_slot_range(770), (332640000, 333071999));
}

#[test]
fn test_slot_to_epoch() {
    assert_eq!(slot_to_epoch(0), 0);
    assert_eq!(slot_to_epoch(431999), 0);
    assert_eq!(slot_to_epoch(432000), 1);
    assert_eq!(slot_to_epoch(332640000), 770);
    assert_eq!(slot_to_epoch(333071999), 770);
}

#[test]
fn test_epoch_to_slot_range() {
    assert_eq!(epoch_to_slot_range(0), (0, 431999));
    assert_eq!(epoch_to_slot_range(1), (432000, 863999));
    assert_eq!(epoch_to_slot_range(2), (864000, 1295999));
    assert_eq!(epoch_to_slot_range(3), (1296000, 1727999));
}
