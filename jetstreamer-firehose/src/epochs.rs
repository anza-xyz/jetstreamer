use reqwest::Client;
use rseek::Seekable;
use serde::Deserialize;
use std::{fmt, io, pin::Pin};
use tokio::io::{AsyncRead, AsyncSeek, BufReader, ReadBuf, SeekFrom};

use crate::archive;
use crate::node_reader::Len;

#[cfg(feature = "s3-backend")]
use {
    crate::archive::S3Location,
    aws_sdk_s3::Client as S3Client,
    std::{future::Future, sync::Arc},
};

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

trait EpochReader: AsyncRead + AsyncSeek + Send + Unpin {}
impl<T> EpochReader for T where T: AsyncRead + AsyncSeek + Send + Unpin {}
type DynEpochReader = dyn EpochReader;

/// Seekable reader returned by [`fetch_epoch_stream`] regardless of backend.
pub struct EpochStream {
    inner: Pin<Box<DynEpochReader>>,
    total_len: u64,
}

impl EpochStream {
    fn new<R>(reader: R) -> Self
    where
        R: AsyncRead + AsyncSeek + Len + Send + Unpin + 'static,
    {
        let len = reader.len();
        Self {
            inner: Box::pin(reader),
            total_len: len,
        }
    }
}

impl AsyncRead for EpochStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        let this = unsafe { self.get_unchecked_mut() };
        this.inner.as_mut().poll_read(cx, buf)
    }
}

impl AsyncSeek for EpochStream {
    fn start_seek(self: Pin<&mut Self>, position: SeekFrom) -> io::Result<()> {
        let this = unsafe { self.get_unchecked_mut() };
        this.inner.as_mut().start_seek(position)
    }

    fn poll_complete(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<u64>> {
        let this = unsafe { self.get_unchecked_mut() };
        this.inner.as_mut().poll_complete(cx)
    }
}

impl Len for EpochStream {
    fn len(&self) -> u64 {
        self.total_len
    }
}

impl Unpin for EpochStream {}

/* ────────────────────────────────────────────────────────────────────────── */
/*  Blanket Len impl so BufReader<T> keeps the .len() we rely on            */
/* ────────────────────────────────────────────────────────────────────────── */
impl<T: Len + AsyncRead> Len for BufReader<T> {
    #[inline]
    fn len(&self) -> u64 {
        self.get_ref().len()
    }
}

/// Checks the configured archive backend to determine whether an epoch CAR exists.
pub async fn epoch_exists(epoch: u64, client: &Client) -> bool {
    let location = archive::car_location();
    let path = format!("{epoch}/epoch-{epoch}.car");

    if location.is_http() {
        let url = location
            .url()
            .join(&path)
            .unwrap_or_else(|err| panic!("invalid CAR URL for epoch {epoch}: {err}"));
        let response = client.head(url).send().await;
        return response
            .map(|res| res.status().is_success())
            .unwrap_or(false);
    }

    #[cfg(feature = "s3-backend")]
    if let Some(cfg) = location.as_s3() {
        let key = cfg.key_for(&path);
        return cfg
            .client
            .head_object()
            .bucket(cfg.bucket.as_ref())
            .key(key)
            .send()
            .await
            .is_ok();
    }

    panic!(
        "unsupported archive backend for CAR location {}",
        location.url()
    );
}

/// Fetches an epoch’s CAR file from the configured archive backend as a buffered, seekable stream.
pub async fn fetch_epoch_stream(epoch: u64, client: &Client) -> EpochStream {
    let location = archive::car_location();
    let path = format!("{epoch}/epoch-{epoch}.car");

    if location.is_http() {
        let url = location
            .url()
            .join(&path)
            .unwrap_or_else(|err| panic!("invalid CAR URL for epoch {epoch}: {err}"));
        let request_url = url.to_string();
        let http_client = client.clone();
        let seekable = Seekable::new(move || http_client.get(request_url.clone())).await;
        let reader = BufReader::with_capacity(8 * 1024 * 1024, seekable);
        return EpochStream::new(reader);
    }

    #[cfg(feature = "s3-backend")]
    if let Some(cfg) = location.as_s3() {
        let s3_reader = S3SeekableReader::new(cfg, path)
            .await
            .unwrap_or_else(|err| panic!("failed to open epoch {epoch} CAR via S3: {err}"));
        let reader = BufReader::with_capacity(8 * 1024 * 1024, s3_reader);
        return EpochStream::new(reader);
    }

    panic!(
        "unsupported archive backend for CAR location {}",
        location.url()
    );
}

#[cfg(feature = "s3-backend")]
type ReaderFuture =
    Pin<Box<dyn Future<Output = io::Result<Pin<Box<dyn AsyncRead + Send>>>> + Send>>;

#[cfg(feature = "s3-backend")]
struct S3SeekableReader {
    client: Arc<S3Client>,
    bucket: Arc<str>,
    key: String,
    len: u64,
    position: u64,
    reader: Option<Pin<Box<dyn AsyncRead + Send>>>,
    init_fetch: Option<ReaderFuture>,
}

#[cfg(feature = "s3-backend")]
impl S3SeekableReader {
    async fn new(location: Arc<S3Location>, path: String) -> io::Result<Self> {
        let key = location.key_for(&path);
        let head = location
            .client
            .head_object()
            .bucket(location.bucket.as_ref())
            .key(&key)
            .send()
            .await
            .map_err(|err| io::Error::other(err.to_string()))?;
        let len = head
            .content_length()
            .ok_or_else(|| io::Error::other("S3 object missing length"))?
            as u64;

        let mut reader = Self {
            client: Arc::clone(&location.client),
            bucket: Arc::clone(&location.bucket),
            key,
            len,
            position: 0,
            reader: None,
            init_fetch: None,
        };
        reader.schedule_fetch(0);
        Ok(reader)
    }

    fn schedule_fetch(&mut self, start: u64) {
        let client = Arc::clone(&self.client);
        let bucket = Arc::clone(&self.bucket);
        let key = self.key.clone();
        let len = self.len;
        self.reader = None;
        self.init_fetch = Some(Box::pin(async move {
            let end = len.saturating_sub(1);
            let range = if start >= len {
                format!("bytes={end}-{end}")
            } else {
                format!("bytes={start}-{end}")
            };
            let resp = client
                .get_object()
                .bucket(bucket.as_ref())
                .key(&key)
                .range(range)
                .send()
                .await
                .map_err(|err| io::Error::other(err.to_string()))?;
            let reader = tokio::io::BufReader::new(resp.body.into_async_read());
            Ok(Box::pin(reader) as Pin<Box<dyn AsyncRead + Send>>)
        }));
    }
}

#[cfg(feature = "s3-backend")]
impl AsyncRead for S3SeekableReader {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        let this = unsafe { self.get_unchecked_mut() };

        if this.position >= this.len {
            return std::task::Poll::Ready(Ok(()));
        }

        loop {
            if let Some(reader) = this.reader.as_mut() {
                let before = buf.filled().len();
                match reader.as_mut().poll_read(cx, buf) {
                    std::task::Poll::Ready(Ok(())) => {
                        let after = buf.filled().len();
                        let delta = after.saturating_sub(before);
                        this.position = this.position.saturating_add(delta as u64);
                        return std::task::Poll::Ready(Ok(()));
                    }
                    other => return other,
                }
            }

            if let Some(fut) = this.init_fetch.as_mut() {
                match fut.as_mut().poll(cx) {
                    std::task::Poll::Ready(Ok(reader)) => {
                        this.reader = Some(reader);
                        this.init_fetch = None;
                        continue;
                    }
                    std::task::Poll::Ready(Err(err)) => {
                        this.init_fetch = None;
                        return std::task::Poll::Ready(Err(err));
                    }
                    std::task::Poll::Pending => return std::task::Poll::Pending,
                }
            } else {
                this.schedule_fetch(this.position);
            }
        }
    }
}

#[cfg(feature = "s3-backend")]
impl AsyncSeek for S3SeekableReader {
    fn start_seek(self: Pin<&mut Self>, position: SeekFrom) -> io::Result<()> {
        let this = unsafe { self.get_unchecked_mut() };
        let target = match position {
            SeekFrom::Start(offset) => offset,
            SeekFrom::Current(delta) => {
                let tmp = this.position as i64 + delta;
                if tmp < 0 {
                    return Err(io::Error::new(io::ErrorKind::InvalidInput, "negative seek"));
                }
                tmp as u64
            }
            SeekFrom::End(delta) => {
                let tmp = this.len as i64 + delta;
                if tmp < 0 {
                    return Err(io::Error::new(io::ErrorKind::InvalidInput, "negative seek"));
                }
                tmp as u64
            }
        };
        this.position = target.min(this.len);
        this.reader = None;
        this.init_fetch = None;
        this.schedule_fetch(this.position);
        Ok(())
    }

    fn poll_complete(
        self: Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<u64>> {
        let this = unsafe { self.get_unchecked_mut() };
        std::task::Poll::Ready(Ok(this.position))
    }
}

#[cfg(feature = "s3-backend")]
impl Len for S3SeekableReader {
    fn len(&self) -> u64 {
        self.len
    }
}

#[cfg(feature = "s3-backend")]
impl Unpin for S3SeekableReader {}

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
