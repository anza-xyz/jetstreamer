//! Helpers for resolving and loading compact index artifacts for the firehose replay engine.
//!
//! ## Environment variables
//!
//! - `JETSTREAMER_COMPACT_INDEX_BASE_URL`: Preferred base URL for compact index files.
//! - `JETSTREAMER_OFFSET_BASE_URL`: Legacy alias used when the primary variable is unset.
//! - `JETSTREAMER_NETWORK`: Network suffix appended to on-disk cache keys and remote filenames.
//!
//! ### Example
//!
//! ```no_run
//! use jetstreamer_firehose::index::{get_index_base_url, SlotOffsetIndex};
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! unsafe {
//!     std::env::set_var("JETSTREAMER_NETWORK", "testnet");
//!     std::env::set_var(
//!         "JETSTREAMER_COMPACT_INDEX_BASE_URL",
//!         "https://mirror.example.com/indexes",
//!     );
//! }
//!
//! let base = get_index_base_url()?;
//! let index = SlotOffsetIndex::new(base)?;
//! # let _ = index.base_url();
//! # Ok(())
//! # }
//! ```
use crate::epochs::{BASE_URL, epoch_to_slot_range, slot_to_epoch};
use cid::{Cid, multibase::Base};
use dashmap::{DashMap, mapref::entry::Entry};
use log::{info, warn};
use once_cell::sync::Lazy;
use reqwest::{Client, StatusCode, Url, header::RANGE};
use serde_cbor::Value;
use std::{collections::HashMap, future::Future, ops::Range, sync::Arc, time::Duration};
use thiserror::Error;
use tokio::{sync::OnceCell, time::sleep};
use xxhash_rust::xxh64::xxh64;

const COMPACT_INDEX_MAGIC: &[u8; 8] = b"compiszd";
const BUCKET_HEADER_SIZE: usize = 16;
const HASH_PREFIX_SIZE: usize = 32;
const SLOT_TO_CID_KIND: &[u8] = b"slot-to-cid";
const CID_TO_OFFSET_KIND: &[u8] = b"cid-to-offset-and-size";
const METADATA_KEY_KIND: &[u8] = b"index_kind";
const METADATA_KEY_EPOCH: &[u8] = b"epoch";
const HTTP_PREFETCH_BYTES: u64 = 4 * 1024; // initial bytes to fetch for headers
const FETCH_RANGE_MAX_RETRIES: usize = 10;
const FETCH_RANGE_BASE_DELAY_MS: u64 = 2000;

/// Errors returned while accessing the compact slot offset index.
#[derive(Debug, Error)]
pub enum SlotOffsetIndexError {
    /// Environment provided an invalid base URL.
    #[error("invalid index base URL: {0}")]
    InvalidBaseUrl(String),
    /// Constructed index URL is invalid.
    #[error("invalid index URL: {0}")]
    InvalidIndexUrl(String),
    /// Epoch index file was not found at the expected location.
    #[error("epoch index file not found: {0}")]
    EpochIndexFileNotFound(Url),
    /// Slot was not present in the index.
    #[error("slot {0} not found in index {1}")]
    SlotNotFound(u64, Url),
    /// Request failed while fetching the index.
    #[error("network error while fetching {0}: {1}")]
    NetworkError(Url, #[source] reqwest::Error),
    /// Unexpected HTTP status returned while fetching index data.
    #[error("unexpected HTTP status {1} when fetching {0}")]
    HttpStatusError(Url, StatusCode),
    /// Index payload was malformed.
    #[error("invalid index format at {0}: {1}")]
    IndexFormatError(Url, String),
    /// CAR header could not be read or decoded.
    #[error("failed to read CAR header {0}: {1}")]
    CarHeaderError(Url, String),
}

/// Lazily constructed global [`SlotOffsetIndex`] that honors environment configuration.
pub static SLOT_OFFSET_INDEX: Lazy<SlotOffsetIndex> = Lazy::new(|| {
    let base_url =
        get_index_base_url().expect("JETSTREAMER_COMPACT_INDEX_BASE_URL must be a valid URL");
    SlotOffsetIndex::new(base_url).expect("failed to initialize slot offset index")
});

static SLOT_OFFSET_RESULT_CACHE: Lazy<DashMap<u64, u64>> = Lazy::new(DashMap::new);
static EPOCH_CACHE: Lazy<DashMap<EpochCacheKey, Arc<EpochEntry>>> = Lazy::new(DashMap::new);

#[derive(Clone, Hash, PartialEq, Eq)]
struct EpochCacheKey {
    namespace: Arc<str>,
    epoch: u64,
}

impl EpochCacheKey {
    fn new(namespace: &Arc<str>, epoch: u64) -> Self {
        Self {
            namespace: Arc::clone(namespace),
            epoch,
        }
    }
}

/// Looks up the byte offset of a slot within Old Faithful firehose CAR archives.
pub async fn slot_to_offset(slot: u64) -> Result<u64, SlotOffsetIndexError> {
    if let Some(offset) = SLOT_OFFSET_RESULT_CACHE.get(&slot) {
        return Ok(*offset);
    }

    let offset = SLOT_OFFSET_INDEX.get_offset(slot).await?;
    SLOT_OFFSET_RESULT_CACHE.insert(slot, offset);
    Ok(offset)
}

/// Client that resolves slot offsets using compact CAR index files from Old Faithful.
pub struct SlotOffsetIndex {
    client: Client,
    base_url: Url,
    network: String,
    cache_namespace: Arc<str>,
}

struct EpochEntry {
    once: OnceCell<Arc<EpochIndex>>,
}

struct EpochIndex {
    slot_range: Range<u64>,
    slot_index: RemoteCompactIndex,
    cid_index: RemoteCompactIndex,
    slot_cache: DashMap<u64, (u64, u32)>,
}

impl EpochEntry {
    fn new() -> Self {
        Self {
            once: OnceCell::new(),
        }
    }

    async fn get_or_load<F, Fut>(&self, loader: F) -> Result<Arc<EpochIndex>, SlotOffsetIndexError>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<Arc<EpochIndex>, SlotOffsetIndexError>>,
    {
        self.once
            .get_or_try_init(|| async { loader().await })
            .await
            .map(Arc::clone)
    }
}

impl EpochIndex {
    async fn offset_for_slot(&self, slot: u64) -> Result<u64, SlotOffsetIndexError> {
        if !self.slot_range.contains(&slot) {
            return Err(SlotOffsetIndexError::IndexFormatError(
                self.slot_index.url().clone(),
                format!("slot {slot} not in epoch slot range"),
            ));
        }

        if let Some(entry) = self.slot_cache.get(&slot) {
            return Ok(entry.value().0);
        }

        let slot_key = slot.to_le_bytes();
        let slot_value = match self.slot_index.lookup(&slot_key).await? {
            Some(bytes) => bytes,
            None => {
                let index_url = self.slot_index.url().clone();
                return Err(SlotOffsetIndexError::SlotNotFound(slot, index_url));
            }
        };

        let (offset, size) = match self.cid_index.lookup(&slot_value).await? {
            Some(bytes) => decode_offset_and_size(&bytes, self.cid_index.url()),
            None => {
                let index_url = self.cid_index.url().clone();
                return Err(SlotOffsetIndexError::SlotNotFound(slot, index_url));
            }
        }?;

        self.slot_cache.insert(slot, (offset, size));
        Ok(offset)
    }
}

impl SlotOffsetIndex {
    /// Constructs a new [`SlotOffsetIndex`] rooted at the provided base URL.
    ///
    /// The [`SlotOffsetIndex`] uses the `JETSTREAMER_NETWORK` variable to scope cache keys and
    /// remote filenames. When unset, the network defaults to `mainnet`.
    pub fn new(base_url: Url) -> Result<Self, SlotOffsetIndexError> {
        let network =
            std::env::var("JETSTREAMER_NETWORK").unwrap_or_else(|_| "mainnet".to_string());
        let cache_namespace =
            Arc::<str>::from(format!("{}|{}", base_url.as_str(), network.as_str()));
        Ok(Self {
            client: Client::new(),
            base_url,
            network,
            cache_namespace,
        })
    }

    /// Returns the base URL that remote index files are fetched from.
    pub fn base_url(&self) -> &Url {
        &self.base_url
    }

    async fn get_epoch(&self, epoch: u64) -> Result<Arc<EpochIndex>, SlotOffsetIndexError> {
        let cache_key = EpochCacheKey::new(&self.cache_namespace, epoch);
        if let Some(entry_ref) = EPOCH_CACHE.get(&cache_key) {
            let entry_arc = Arc::clone(entry_ref.value());
            drop(entry_ref);
            return entry_arc
                .get_or_load(|| async { self.load_epoch(epoch).await })
                .await;
        }

        let new_entry = Arc::new(EpochEntry::new());
        let entry_arc = match EPOCH_CACHE.entry(cache_key) {
            Entry::Occupied(occupied) => Arc::clone(occupied.get()),
            Entry::Vacant(vacant) => {
                vacant.insert(Arc::clone(&new_entry));
                new_entry
            }
        };

        entry_arc
            .get_or_load(|| async { self.load_epoch(epoch).await })
            .await
    }

    async fn load_epoch(&self, epoch: u64) -> Result<Arc<EpochIndex>, SlotOffsetIndexError> {
        let (slot_index, cid_index) = self.load_epoch_indexes(epoch).await?;
        let (slot_start, slot_end_inclusive) = epoch_to_slot_range(epoch);
        Ok(Arc::new(EpochIndex {
            slot_range: slot_start..(slot_end_inclusive + 1),
            slot_index,
            cid_index,
            slot_cache: DashMap::new(),
        }))
    }

    async fn load_epoch_indexes(
        &self,
        epoch: u64,
    ) -> Result<(RemoteCompactIndex, RemoteCompactIndex), SlotOffsetIndexError> {
        let (root_cid, car_url) = fetch_epoch_root(&self.client, &self.base_url, epoch).await?;
        let root_base32 = root_cid
            .to_string_of_base(Base::Base32Lower)
            .map_err(|err| {
                SlotOffsetIndexError::CarHeaderError(car_url.clone(), err.to_string())
            })?;

        let slot_index_path = format!(
            "{0}/epoch-{0}-{1}-{2}-slot-to-cid.index",
            epoch, root_base32, self.network
        );
        let cid_index_path = format!(
            "{0}/epoch-{0}-{1}-{2}-cid-to-offset-and-size.index",
            epoch, root_base32, self.network
        );

        let slot_index_url = self.base_url.join(&slot_index_path).map_err(|err| {
            SlotOffsetIndexError::InvalidIndexUrl(format!("{slot_index_path} ({err})"))
        })?;
        let cid_index_url = self.base_url.join(&cid_index_path).map_err(|err| {
            SlotOffsetIndexError::InvalidIndexUrl(format!("{cid_index_path} ({err})"))
        })?;

        let slot_index = RemoteCompactIndex::open(
            self.client.clone(),
            slot_index_url.clone(),
            SLOT_TO_CID_KIND,
            None,
        )
        .await?;

        let cid_index = RemoteCompactIndex::open(
            self.client.clone(),
            cid_index_url.clone(),
            CID_TO_OFFSET_KIND,
            Some(RemoteCompactIndex::OFFSET_AND_SIZE_VALUE_SIZE),
        )
        .await?;

        if let Some(meta_epoch) = slot_index.metadata_epoch()
            && meta_epoch != epoch
        {
            warn!("Slot index epoch metadata mismatch: expected {epoch}, got {meta_epoch}");
        }
        if let Some(meta_epoch) = cid_index.metadata_epoch()
            && meta_epoch != epoch
        {
            warn!("CID index epoch metadata mismatch: expected {epoch}, got {meta_epoch}");
        }

        Ok((slot_index, cid_index))
    }

    /// Resolves the byte offset of `slot` within its Old Faithful CAR archive.
    pub async fn get_offset(&self, slot: u64) -> Result<u64, SlotOffsetIndexError> {
        let epoch = slot_to_epoch(slot);
        let epoch_index = self.get_epoch(epoch).await?;
        epoch_index.offset_for_slot(slot).await
    }
}

fn decode_offset_and_size(value: &[u8], url: &Url) -> Result<(u64, u32), SlotOffsetIndexError> {
    if value.len() != RemoteCompactIndex::OFFSET_AND_SIZE_VALUE_SIZE as usize {
        return Err(SlotOffsetIndexError::IndexFormatError(
            url.clone(),
            format!(
                "offset-and-size entry expected {} bytes, got {}",
                RemoteCompactIndex::OFFSET_AND_SIZE_VALUE_SIZE,
                value.len()
            ),
        ));
    }
    let mut offset_bytes = [0u8; 8];
    offset_bytes[..6].copy_from_slice(&value[..6]);
    let offset = u64::from_le_bytes(offset_bytes);

    let mut size_bytes = [0u8; 4];
    size_bytes[..3].copy_from_slice(&value[6..9]);
    let size = u32::from_le_bytes(size_bytes);
    Ok((offset, size))
}

struct RemoteCompactIndex {
    client: Client,
    url: Url,
    header: CompactIndexHeader,
    bucket_entries: DashMap<u32, Arc<BucketEntry>>,
}

impl RemoteCompactIndex {
    const OFFSET_AND_SIZE_VALUE_SIZE: u64 = 9;

    async fn open(
        client: Client,
        url: Url,
        expected_kind: &[u8],
        expected_value_size: Option<u64>,
    ) -> Result<Self, SlotOffsetIndexError> {
        let kind_label = std::str::from_utf8(expected_kind).unwrap_or("index");
        let epoch_hint = url
            .path_segments()
            .and_then(|mut segments| segments.next_back())
            .and_then(|name| name.split('-').nth(1))
            .and_then(|value| value.parse::<u64>().ok());
        if let Some(epoch) = epoch_hint {
            info!("Fetching {kind_label} compact index for epoch {epoch}");
        } else {
            info!("Fetching {kind_label} compact index");
        }

        let header =
            fetch_and_parse_header(&client, &url, expected_kind, expected_value_size).await?;
        Ok(Self {
            client,
            url,
            header,
            bucket_entries: DashMap::new(),
        })
    }

    async fn lookup(&self, key: &[u8]) -> Result<Option<Vec<u8>>, SlotOffsetIndexError> {
        let bucket_index = self.bucket_hash(key);
        let bucket = self.get_bucket(bucket_index).await?;
        bucket.lookup(key)
    }

    fn metadata_epoch(&self) -> Option<u64> {
        self.header.metadata_epoch()
    }

    fn url(&self) -> &Url {
        &self.url
    }

    fn bucket_hash(&self, key: &[u8]) -> u32 {
        let h = xxh64(key, 0);
        let n = self.header.num_buckets as u64;
        let mut u = h % n;
        if ((h - u) / n) < u {
            u = hash_uint64(u);
        }
        (u % n) as u32
    }

    async fn get_bucket(&self, index: u32) -> Result<Arc<BucketData>, SlotOffsetIndexError> {
        let entry = match self.bucket_entries.entry(index) {
            Entry::Occupied(occupied) => Arc::clone(occupied.get()),
            Entry::Vacant(vacant) => {
                let new_entry = Arc::new(BucketEntry::new());
                vacant.insert(Arc::clone(&new_entry));
                new_entry
            }
        };
        entry
            .get_or_load(|| async { self.load_bucket(index).await })
            .await
    }

    async fn load_bucket(&self, index: u32) -> Result<Arc<BucketData>, SlotOffsetIndexError> {
        let bucket_header_offset =
            self.header.header_size + (index as u64) * BUCKET_HEADER_SIZE as u64;
        let header_bytes = fetch_range(
            &self.client,
            &self.url,
            bucket_header_offset,
            bucket_header_offset + BUCKET_HEADER_SIZE as u64 - 1,
            true,
        )
        .await?;
        if header_bytes.len() != BUCKET_HEADER_SIZE {
            return Err(SlotOffsetIndexError::IndexFormatError(
                self.url.clone(),
                format!(
                    "expected {BUCKET_HEADER_SIZE} bucket header bytes, got {}",
                    header_bytes.len()
                ),
            ));
        }
        let bucket_header = BucketHeader::from_bytes(header_bytes.try_into().unwrap());
        let stride = bucket_header.hash_len as usize + self.header.value_size as usize;
        let data_len = stride * bucket_header.num_entries as usize;
        let data_start = bucket_header.file_offset;
        let data_end = data_start + data_len as u64 - 1;
        let data = fetch_range(&self.client, &self.url, data_start, data_end, true).await?;
        if data.len() != data_len {
            return Err(SlotOffsetIndexError::IndexFormatError(
                self.url.clone(),
                format!(
                    "expected {} bytes of bucket data, got {}",
                    data_len,
                    data.len()
                ),
            ));
        }

        Ok(Arc::new(BucketData::new(
            bucket_header,
            data,
            stride,
            self.header.value_size as usize,
        )))
    }
}

struct CompactIndexHeader {
    value_size: u64,
    num_buckets: u32,
    header_size: u64,
    metadata: HashMap<Vec<u8>, Vec<u8>>,
}

impl CompactIndexHeader {
    fn metadata_epoch(&self) -> Option<u64> {
        self.metadata
            .get(METADATA_KEY_EPOCH)
            .and_then(|bytes| bytes.get(..8))
            .map(|slice| {
                let mut buf = [0u8; 8];
                buf.copy_from_slice(slice);
                u64::from_le_bytes(buf)
            })
    }
}

struct BucketData {
    header: BucketHeader,
    data: Vec<u8>,
    stride: usize,
    value_width: usize,
}

impl BucketData {
    fn new(header: BucketHeader, data: Vec<u8>, stride: usize, value_width: usize) -> Self {
        Self {
            header,
            data,
            stride,
            value_width,
        }
    }

    fn lookup(&self, key: &[u8]) -> Result<Option<Vec<u8>>, SlotOffsetIndexError> {
        let target_hash = truncated_entry_hash(self.header.hash_domain, key, self.header.hash_len);
        let max = self.header.num_entries as usize;
        let hash_len = self.header.hash_len as usize;

        let mut index = 0usize;
        while index < max {
            let offset = index * self.stride;
            let hash = read_hash(&self.data[offset..offset + hash_len]);
            if hash == target_hash {
                let value_start = offset + hash_len;
                let value_end = value_start + self.value_width;
                return Ok(Some(self.data[value_start..value_end].to_vec()));
            }
            index = (index << 1) | 1;
            if hash < target_hash {
                index += 1;
            }
        }
        Ok(None)
    }
}

struct BucketEntry {
    once: OnceCell<Arc<BucketData>>,
}

impl BucketEntry {
    fn new() -> Self {
        Self {
            once: OnceCell::new(),
        }
    }

    async fn get_or_load<F, Fut>(&self, loader: F) -> Result<Arc<BucketData>, SlotOffsetIndexError>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<Arc<BucketData>, SlotOffsetIndexError>>,
    {
        self.once
            .get_or_try_init(|| async { loader().await })
            .await
            .map(Arc::clone)
    }
}

#[derive(Clone, Copy)]
struct BucketHeader {
    hash_domain: u32,
    num_entries: u32,
    hash_len: u8,
    file_offset: u64,
}

impl BucketHeader {
    fn from_bytes(bytes: [u8; BUCKET_HEADER_SIZE]) -> Self {
        let hash_domain = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
        let num_entries = u32::from_le_bytes(bytes[4..8].try_into().unwrap());
        let hash_len = bytes[8];
        let mut offset_bytes = [0u8; 8];
        offset_bytes[..6].copy_from_slice(&bytes[10..16]);
        let file_offset = u64::from_le_bytes(offset_bytes);
        Self {
            hash_domain,
            num_entries,
            hash_len,
            file_offset,
        }
    }
}

fn read_hash(bytes: &[u8]) -> u64 {
    let mut buf = 0u64;
    for (i, b) in bytes.iter().enumerate() {
        buf |= (*b as u64) << (8 * i);
    }
    buf
}

fn truncated_entry_hash(hash_domain: u32, key: &[u8], hash_len: u8) -> u64 {
    let raw = entry_hash64(hash_domain, key);
    if hash_len >= 8 {
        raw
    } else {
        let bits = (hash_len as usize) * 8;
        let mask = if bits == 64 {
            u64::MAX
        } else {
            (1u64 << bits) - 1
        };
        raw & mask
    }
}

fn entry_hash64(prefix: u32, key: &[u8]) -> u64 {
    let mut block = [0u8; HASH_PREFIX_SIZE];
    block[..4].copy_from_slice(&prefix.to_le_bytes());
    let mut data = Vec::with_capacity(HASH_PREFIX_SIZE + key.len());
    data.extend_from_slice(&block);
    data.extend_from_slice(key);
    xxh64(&data, 0)
}

fn hash_uint64(mut x: u64) -> u64 {
    x ^= x >> 33;
    x = x.wrapping_mul(0xff51afd7ed558ccd);
    x ^= x >> 33;
    x = x.wrapping_mul(0xc4ceb9fe1a85ec53);
    x ^= x >> 33;
    x
}

async fn fetch_and_parse_header(
    client: &Client,
    url: &Url,
    expected_kind: &[u8],
    expected_value_size: Option<u64>,
) -> Result<CompactIndexHeader, SlotOffsetIndexError> {
    let mut bytes = fetch_range(client, url, 0, HTTP_PREFETCH_BYTES - 1, false).await?;
    if bytes.len() < 12 {
        return Err(SlotOffsetIndexError::IndexFormatError(
            url.clone(),
            "index header shorter than 12 bytes".into(),
        ));
    }
    if bytes[..8] != COMPACT_INDEX_MAGIC[..] {
        return Err(SlotOffsetIndexError::IndexFormatError(
            url.clone(),
            "invalid compactindex magic".into(),
        ));
    }
    let header_len = u32::from_le_bytes(bytes[8..12].try_into().unwrap()) as usize;
    let total_header_size = 8 + 4 + header_len;
    if bytes.len() < total_header_size {
        bytes = fetch_range(client, url, 0, total_header_size as u64 - 1, false).await?;
        if bytes.len() < total_header_size {
            return Err(SlotOffsetIndexError::IndexFormatError(
                url.clone(),
                format!(
                    "incomplete index header: expected {total_header_size} bytes, got {}",
                    bytes.len()
                ),
            ));
        }
    }

    let value_size = u64::from_le_bytes(bytes[12..20].try_into().unwrap());
    let num_buckets = u32::from_le_bytes(bytes[20..24].try_into().unwrap());
    let version = bytes[24];
    if version != 1 {
        return Err(SlotOffsetIndexError::IndexFormatError(
            url.clone(),
            format!("unsupported compactindex version {version}"),
        ));
    }
    let metadata_slice = &bytes[25..total_header_size];
    let metadata = parse_metadata(metadata_slice).map_err(|msg| {
        SlotOffsetIndexError::IndexFormatError(url.clone(), format!("invalid metadata: {msg}"))
    })?;

    if let Some(expected) = expected_value_size
        && value_size != expected
    {
        return Err(SlotOffsetIndexError::IndexFormatError(
            url.clone(),
            format!("unexpected value size: expected {expected}, got {value_size}"),
        ));
    }
    if let Some(kind) = metadata.get(METADATA_KEY_KIND)
        && kind.as_slice() != expected_kind
    {
        return Err(SlotOffsetIndexError::IndexFormatError(
            url.clone(),
            format!(
                "wrong index kind: expected {:?}, got {:?}",
                expected_kind, kind
            ),
        ));
    }

    Ok(CompactIndexHeader {
        value_size,
        num_buckets,
        header_size: total_header_size as u64,
        metadata,
    })
}

async fn fetch_epoch_root(
    client: &Client,
    base_url: &Url,
    epoch: u64,
) -> Result<(Cid, Url), SlotOffsetIndexError> {
    let car_path = format!("{epoch}/epoch-{epoch}.car");
    let car_url = base_url
        .join(&car_path)
        .map_err(|err| SlotOffsetIndexError::InvalidIndexUrl(format!("{car_path} ({err})")))?;

    let mut bytes = fetch_range(client, &car_url, 0, HTTP_PREFETCH_BYTES - 1, false).await?;
    let (header_len, prefix) = decode_varint(&bytes)
        .map_err(|msg| SlotOffsetIndexError::CarHeaderError(car_url.clone(), msg))?;
    let total_needed = prefix + header_len as usize;
    if bytes.len() < total_needed {
        bytes = fetch_range(client, &car_url, 0, total_needed as u64 - 1, false).await?;
        if bytes.len() < total_needed {
            return Err(SlotOffsetIndexError::CarHeaderError(
                car_url.clone(),
                format!(
                    "incomplete CAR header: expected {total_needed} bytes, got {}",
                    bytes.len()
                ),
            ));
        }
    }
    let header_bytes = &bytes[prefix..total_needed];
    let value: Value = serde_cbor::from_slice(header_bytes).map_err(|err| {
        SlotOffsetIndexError::CarHeaderError(
            car_url.clone(),
            format!("failed to decode CBOR: {err}"),
        )
    })?;
    let root_cid = extract_root_cid(&value)
        .map_err(|msg| SlotOffsetIndexError::CarHeaderError(car_url.clone(), msg.to_string()))?;
    Ok((root_cid, car_url))
}

fn decode_varint(bytes: &[u8]) -> Result<(u64, usize), String> {
    let mut value = 0u64;
    let mut shift = 0u32;
    for (idx, b) in bytes.iter().enumerate() {
        let byte = *b as u64;
        if byte < 0x80 {
            value |= byte << shift;
            return Ok((value, idx + 1));
        }
        value |= (byte & 0x7f) << shift;
        shift += 7;
        if shift > 63 {
            return Err("varint overflow".into());
        }
    }
    Err("buffer ended before varint terminated".into())
}

fn extract_root_cid(value: &Value) -> Result<Cid, String> {
    let map_entries = match value {
        Value::Map(entries) => entries,
        _ => return Err("CAR header is not a map".into()),
    };
    let roots_value = map_entries
        .iter()
        .find(|(k, _)| matches!(k, Value::Text(s) if s == "roots"))
        .map(|(_, v)| v)
        .ok_or_else(|| "CAR header missing 'roots'".to_string())?;
    let roots = match roots_value {
        Value::Array(items) => items,
        _ => return Err("CAR header 'roots' not an array".into()),
    };
    let first = roots
        .first()
        .ok_or_else(|| "CAR header 'roots' array empty".to_string())?;
    match first {
        Value::Tag(42, boxed) => match boxed.as_ref() {
            Value::Bytes(bytes) => decode_cid_bytes(bytes),
            _ => Err("CID tag did not contain bytes".into()),
        },
        Value::Bytes(bytes) => decode_cid_bytes(bytes),
        _ => Err("unexpected CID encoding in CAR header".into()),
    }
}

fn decode_cid_bytes(bytes: &[u8]) -> Result<Cid, String> {
    if bytes.is_empty() {
        return Err("CID bytes were empty".into());
    }
    // CID-in-CBOR is prefixed with a multibase identity (0x00) byte.
    let mut candidates: Vec<&[u8]> = Vec::with_capacity(2);
    if bytes[0] == 0 && bytes.len() > 1 {
        candidates.push(&bytes[1..]);
    }
    candidates.push(bytes);
    let mut last_err = None;
    for slice in candidates {
        match Cid::try_from(slice.to_vec()) {
            Ok(cid) => return Ok(cid),
            Err(err) => last_err = Some(err),
        }
    }
    Err(last_err
        .map(|err| format!("invalid CID: {err}"))
        .unwrap_or_else(|| "invalid CID bytes".into()))
}

async fn fetch_range(
    client: &Client,
    url: &Url,
    start: u64,
    end: u64,
    exact: bool,
) -> Result<Vec<u8>, SlotOffsetIndexError> {
    if end < start {
        return Ok(Vec::new());
    }
    let range_header = format!("bytes={start}-{end}");
    let mut attempt = 0usize;
    loop {
        let response = client
            .get(url.clone())
            .header(RANGE, range_header.clone())
            .send()
            .await
            .map_err(|err| SlotOffsetIndexError::NetworkError(url.clone(), err))?;

        if response.status() == StatusCode::NOT_FOUND {
            return Err(SlotOffsetIndexError::EpochIndexFileNotFound(url.clone()));
        }

        if response.status() == StatusCode::TOO_MANY_REQUESTS
            || response.status() == StatusCode::SERVICE_UNAVAILABLE
        {
            if attempt < FETCH_RANGE_MAX_RETRIES {
                let delay_ms = FETCH_RANGE_BASE_DELAY_MS.saturating_mul(1u64 << attempt.min(10));
                warn!(
                    "HTTP {} fetching {} (range {}-{}); retrying in {} ms (attempt {}/{})",
                    response.status(),
                    url,
                    start,
                    end,
                    delay_ms,
                    attempt + 1,
                    FETCH_RANGE_MAX_RETRIES
                );
                sleep(Duration::from_millis(delay_ms)).await;
                attempt += 1;
                continue;
            }
            return Err(SlotOffsetIndexError::HttpStatusError(
                url.clone(),
                response.status(),
            ));
        }

        if !(response.status().is_success() || response.status() == StatusCode::PARTIAL_CONTENT) {
            return Err(SlotOffsetIndexError::HttpStatusError(
                url.clone(),
                response.status(),
            ));
        }

        let bytes = response
            .bytes()
            .await
            .map_err(|err| SlotOffsetIndexError::NetworkError(url.clone(), err))?;
        let expected = (end - start + 1) as usize;
        if exact && bytes.len() != expected {
            return Err(SlotOffsetIndexError::IndexFormatError(
                url.clone(),
                format!("expected {expected} bytes, got {}", bytes.len()),
            ));
        }
        return Ok(bytes.to_vec());
    }
}

fn parse_metadata(data: &[u8]) -> Result<HashMap<Vec<u8>, Vec<u8>>, String> {
    if data.is_empty() {
        return Ok(HashMap::new());
    }
    let mut map = HashMap::new();
    let mut offset = 0;
    let num_pairs = data[offset] as usize;
    offset += 1;
    for _ in 0..num_pairs {
        if offset >= data.len() {
            return Err("unexpected end while reading metadata key length".into());
        }
        let key_len = data[offset] as usize;
        offset += 1;
        if offset + key_len > data.len() {
            return Err("metadata key length out of bounds".into());
        }
        let key = data[offset..offset + key_len].to_vec();
        offset += key_len;
        if offset >= data.len() {
            return Err("unexpected end while reading metadata value length".into());
        }
        let value_len = data[offset] as usize;
        offset += 1;
        if offset + value_len > data.len() {
            return Err("metadata value length out of bounds".into());
        }
        let value = data[offset..offset + value_len].to_vec();
        offset += value_len;
        map.insert(key, value);
    }
    Ok(map)
}

/// Resolves the base URL used when constructing the global [`SLOT_OFFSET_INDEX`].
///
/// The resolution order is:
/// 1. `JETSTREAMER_COMPACT_INDEX_BASE_URL`
/// 2. `JETSTREAMER_OFFSET_BASE_URL` (legacy)
/// 3. The built-in [`BASE_URL`], pointing at Old Faithful's public mirror.
///
/// # Examples
///
/// ```no_run
/// # use jetstreamer_firehose::index::get_index_base_url;
/// unsafe {
///     std::env::set_var("JETSTREAMER_COMPACT_INDEX_BASE_URL", "https://mirror.example.com/indexes");
/// }
///
/// let base = get_index_base_url().expect("valid URL");
/// assert_eq!(base.as_str(), "https://mirror.example.com/indexes");
/// ```
pub fn get_index_base_url() -> Result<Url, SlotOffsetIndexError> {
    let base = std::env::var("JETSTREAMER_COMPACT_INDEX_BASE_URL")
        .or_else(|_| std::env::var("JETSTREAMER_OFFSET_BASE_URL"))
        .unwrap_or_else(|_| BASE_URL.to_string());
    Url::parse(&base).map_err(|err| SlotOffsetIndexError::InvalidBaseUrl(format!("{base} ({err})")))
}
