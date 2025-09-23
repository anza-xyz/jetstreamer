use crate::epochs::{epoch_to_slot_range, fetch_epoch_stream, slot_to_epoch};
use crate::network::{current_epoch, latest_old_faithful_epoch};
use crate::node::Node;
use crate::node_reader::NodeReader;
use futures_util::StreamExt;
use rayon::prelude::*;
use reqwest::{Client, StatusCode, Url};
use std::collections::{HashMap, HashSet};
use std::fmt::Display;
use std::io::SeekFrom;
use std::path::Path;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

#[derive(Debug)]
pub struct SlotOffsetIndex {
    base_url: Url,
    client: Client,
    data: HashMap<u64, u64>,
    loaded_epochs: HashSet<u64>,
}

#[derive(Debug)]
pub enum SlotOffsetIndexError {
    InvalidBaseUrl(String),
    InvalidIndexUrl(String),
    EpochIndexFileNotFound(Url),
    SlotNotFound(u64, Url),
    NetworkError(Url, reqwest::Error),
    HttpStatusError(Url, StatusCode),
    IndexFormatError(Url, String),
}

impl Display for SlotOffsetIndexError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SlotOffsetIndexError::InvalidBaseUrl(url) => {
                write!(f, "invalid index base URL: {}", url)
            }
            SlotOffsetIndexError::InvalidIndexUrl(url) => {
                write!(f, "invalid index URL: {}", url)
            }
            SlotOffsetIndexError::EpochIndexFileNotFound(url) => {
                write!(f, "epoch index file not found: {}", url)
            }
            SlotOffsetIndexError::SlotNotFound(slot, url) => {
                write!(f, "slot {} not found in index: {}", slot, url)
            }
            SlotOffsetIndexError::NetworkError(url, err) => {
                write!(f, "failed to fetch index {}: {}", url, err)
            }
            SlotOffsetIndexError::HttpStatusError(url, status) => {
                write!(
                    f,
                    "unexpected HTTP status {} while fetching index {}",
                    status, url
                )
            }
            SlotOffsetIndexError::IndexFormatError(url, message) => {
                write!(f, "invalid index data at {}: {}", url, message)
            }
        }
    }
}

impl std::error::Error for SlotOffsetIndexError {}

impl SlotOffsetIndex {
    pub fn new(mut base_url: Url) -> Result<Self, SlotOffsetIndexError> {
        if !base_url.path().ends_with('/') {
            let mut path = base_url.path().to_string();
            if !path.ends_with('/') {
                path.push('/');
            }
            base_url.set_path(&path);
        }
        Ok(Self {
            base_url,
            client: Client::new(),
            data: HashMap::new(),
            loaded_epochs: HashSet::new(),
        })
    }

    fn epoch_url(&self, epoch: u64) -> Result<Url, SlotOffsetIndexError> {
        let candidate = format!("epoch-{}.idx", epoch);
        self.base_url.join(&candidate).map_err(|err| {
            SlotOffsetIndexError::InvalidIndexUrl(format!("{}{} ({err})", self.base_url, candidate))
        })
    }

    async fn load_epoch(&mut self, epoch: u64) -> Result<(), SlotOffsetIndexError> {
        if self.loaded_epochs.contains(&epoch) {
            return Ok(());
        }

        let url = self.epoch_url(epoch)?;
        log::info!("Loading slot offset index: {}", url);

        let response = self
            .client
            .get(url.clone())
            .send()
            .await
            .map_err(|err| SlotOffsetIndexError::NetworkError(url.clone(), err))?;

        if response.status() == StatusCode::NOT_FOUND {
            return Err(SlotOffsetIndexError::EpochIndexFileNotFound(url));
        }

        if !response.status().is_success() {
            return Err(SlotOffsetIndexError::HttpStatusError(
                url,
                response.status(),
            ));
        }

        let mut stream = response.bytes_stream();
        let mut buffer: Vec<u8> = Vec::new();
        let mut entries = 0u64;

        while let Some(chunk_result) = stream.next().await {
            let chunk =
                chunk_result.map_err(|err| SlotOffsetIndexError::NetworkError(url.clone(), err))?;
            buffer.extend_from_slice(&chunk);

            let mut processed = 0usize;
            while buffer.len() - processed >= 24 {
                let slot_bytes = &buffer[processed..processed + 8];
                let offset_bytes = &buffer[processed + 8..processed + 16];

                let mut slot = [0u8; 8];
                slot.copy_from_slice(slot_bytes);
                let mut offset = [0u8; 8];
                offset.copy_from_slice(offset_bytes);

                let slot = u64::from_le_bytes(slot);
                let offset = u64::from_le_bytes(offset);
                self.data.insert(slot, offset);
                entries += 1;
                processed += 24;
            }

            if processed > 0 {
                buffer.drain(0..processed);
            }
        }

        if !buffer.is_empty() {
            return Err(SlotOffsetIndexError::IndexFormatError(
                url,
                format!(
                    "index length {} is not a multiple of 24 bytes",
                    buffer.len()
                ),
            ));
        }

        self.loaded_epochs.insert(epoch);
        log::info!(
            "Loaded {} slot offset index entries from epoch {}.",
            entries,
            epoch
        );
        Ok(())
    }

    pub async fn get_offset(&mut self, slot: u64) -> Result<u64, SlotOffsetIndexError> {
        if let Some(offset) = self.data.get(&slot) {
            return Ok(*offset);
        }
        let epoch = slot_to_epoch(slot);
        self.load_epoch(epoch).await?;
        if let Some(offset) = self.data.get(&slot) {
            return Ok(*offset);
        }
        let url = self.epoch_url(epoch)?;
        Err(SlotOffsetIndexError::SlotNotFound(slot, url))
    }
}

/// Build a block-only index: `[u64 slot] [u64 offset] [u64 size]`.
///
/// Uses `next_parsed()` for maximum throughput (no second parse pass). Build a `[slot | offset
/// | size]` index quickly. Requires that `self.reader` is already wrapped in a `BufReader`.
/// Build a block-only index `[u64 slot] [u64 offset] [u64 size]`. Re-uses a single buffer to
/// avoid per-block allocations.
pub async fn build_index<P>(
    client: &reqwest::Client,
    epoch: u64,
    idx_path: P,
    start_offset: Option<u64>,
) -> Result<(), Box<dyn std::error::Error>>
where
    P: AsRef<std::path::Path>,
{
    let stream = fetch_epoch_stream(epoch, client).await;
    let mut node_reader = NodeReader::new(stream);

    /* ── 1. make sure the CAR header has been consumed ───────────────────── */
    if node_reader.header.is_empty() {
        node_reader.read_raw_header().await?;
    }

    /* ── 2. output file ──────────────────────────────────────────────────── */
    let mut out = File::options()
        .create(true)
        .append(true)
        .open(idx_path)
        .await?;

    /* ── 3. helper: read varint and return its byte-length ───────────────── */
    async fn read_uvarint_len<R: AsyncReadExt + Unpin>(r: &mut R) -> std::io::Result<(u64, u64)> {
        let mut x = 0u64;
        let mut s = 0u32;
        let mut buf = [0u8; 1];
        let mut n = 0u64;
        loop {
            r.read_exact(&mut buf).await?;
            n += 1;
            let b = buf[0];
            if b < 0x80 {
                return Ok((x | ((b as u64) << s), n));
            }
            x |= ((b & 0x7f) as u64) << s;
            s += 7;
            if s > 63 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "uvarint overflow",
                ));
            }
        }
    }

    /* ── 4. streaming loop ──────────────────────────────────────────────── */
    let mut buf: Vec<u8> = Vec::with_capacity(64 * 1024); // reusable scratch
    let mut offset = node_reader.reader.stream_position().await?; // current file pos
    if let Some(start_offset) = start_offset {
        offset = start_offset;
        // seek to start_offset if it was specified
        log::info!(
            "(Epoch = {}) Seeking to start offset: {}",
            epoch,
            start_offset
        );
        node_reader.reader.seek(SeekFrom::Start(offset)).await?;
    }
    let mut blocks = 0u64;

    loop {
        let start_off = offset;

        /* size of the section + how long the varint was */
        let (section_size, varint_len) = match read_uvarint_len(&mut node_reader.reader).await {
            Ok(v) => v,
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e.into()),
        };

        /* ensure buffer big enough, then read */
        if buf.len() < section_size as usize {
            buf.resize(section_size as usize, 0);
        }
        node_reader
            .reader
            .read_exact(&mut buf[..section_size as usize])
            .await?;

        offset += varint_len + section_size; // next loop starts here

        /* parse exactly once */
        let bytes_vec = buf[..section_size as usize].to_vec(); // <-- make Vec<u8>
        let mut cur = std::io::Cursor::new(bytes_vec); // Cursor<Vec<u8>>
        let raw = crate::node_reader::RawNode::from_cursor(&mut cur).await?;

        if let Node::Block(b) = raw.parse()? {
            blocks += 1;

            /* slot | offset | size  */
            out.write_all(&b.slot.to_le_bytes()).await?;
            out.write_all(&start_off.to_le_bytes()).await?;
            out.write_all(&(varint_len + section_size).to_le_bytes())
                .await?;

            if b.slot % 100 == 0 {
                log::info!(
                    "build_index: Epoch={} Block slot={} @ {} ({} B) - {} indexed",
                    epoch,
                    b.slot,
                    start_off,
                    section_size + varint_len,
                    blocks
                );
            }
        }
    }

    out.flush().await?;
    log::info!(
        "build_index for epoch={}: DONE – {} Block records",
        epoch,
        blocks
    );
    Ok(())
}

pub async fn get_latest_index_line(
    idx_path: impl AsRef<Path>,
) -> Result<(u64, u64, u64), Box<dyn std::error::Error>> {
    let idx_path = idx_path.as_ref();
    if !idx_path.exists() {
        return Err(format!("Index file does not exist: {:?}", idx_path).into());
    }
    let mut file = File::open(idx_path).await?;
    let mut buf = vec![0u8; 24];
    file.seek(SeekFrom::End(-24)).await?;
    file.read_exact(&mut buf).await?;

    let slot = u64::from_le_bytes(buf[0..8].try_into().unwrap());
    let offset = u64::from_le_bytes(buf[8..16].try_into().unwrap());
    let size = u64::from_le_bytes(buf[16..24].try_into().unwrap());
    Ok((slot, offset, size))
}

pub async fn build_missing_indexes(
    idx_dir: impl AsRef<Path>,
) -> Result<(), Box<dyn std::error::Error>> {
    rayon::ThreadPoolBuilder::new()
        .num_threads(48)
        .build_global()
        .unwrap();
    let idx_dir = idx_dir.as_ref();
    if !idx_dir.exists() {
        log::info!("Creating index directory: {:?}", idx_dir);
        std::fs::create_dir_all(idx_dir)?;
    } else {
        log::info!("Index directory already exists: {:?}", idx_dir);
    }

    let client = Client::new();

    let current_epoch = current_epoch(&client).await?;
    log::info!("Current Mainnet epoch: {}", current_epoch);

    let (of1_last_epoch, of1_last_epoch_first_slot, of1_last_epoch_last_slot) =
        latest_old_faithful_epoch(&client, Some(current_epoch)).await?;
    log::info!(
        "Latest Old Faithful epoch: {} (slots {}-{})",
        of1_last_epoch,
        of1_last_epoch_first_slot,
        of1_last_epoch_last_slot
    );

    (0..of1_last_epoch).par_bridge().for_each(|epoch| {
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let client = Client::new();
            let idx_path = idx_dir.join(format!("epoch-{}.idx", epoch));
            let (start_slot, end_slot) = epoch_to_slot_range(epoch);
            if idx_path.exists() {
                let Ok((last_slot, last_offset, last_size)) =
                    get_latest_index_line(&idx_path).await
                else {
                    log::error!("Failed to get last index line for epoch {}", epoch);
                    log::info!("Building index for epoch {} from scratch", epoch);
                    build_index(&client, epoch, &idx_path, None).await.unwrap();
                    log::info!("Finished building index for epoch {}", epoch);
                    return;
                };

                let offset = last_offset + last_size;
                if last_slot < end_slot {
                    log::info!(
                        "Building index for epoch {} from offset: {} (slots {}-{})",
                        epoch,
                        offset,
                        last_slot,
                        end_slot
                    );
                    if let Err(e) = build_index(&client, epoch, &idx_path, Some(offset)).await {
                        log::error!("Failed to build index for epoch {}: {}", epoch, e);
                        return;
                    }
                } else {
                    log::info!(
                        "Full index already exists for epoch {} (slots {}-{})",
                        epoch,
                        start_slot,
                        end_slot
                    );
                }
            } else {
                log::info!("Building index for epoch {} from scratch", epoch);
                build_index(&client, epoch, &idx_path, None).await.unwrap();
            }
            log::info!("Finished building index for epoch {}", epoch);
        });
    });
    Ok(())
}

pub fn get_index_base_url() -> Result<Url, SlotOffsetIndexError> {
    let base = std::env::var("JETSTREAMER_OFFSET_BASE_URL")
        .unwrap_or_else(|_| "https://storage.googleapis.com/jetstreamer/index/".to_string());
    let mut url = Url::parse(&base)
        .map_err(|err| SlotOffsetIndexError::InvalidBaseUrl(format!("{} ({err})", base)))?;
    if !url.path().ends_with('/') {
        let mut path = url.path().to_string();
        if !path.ends_with('/') {
            path.push('/');
        }
        url.set_path(&path);
    }
    Ok(url)
}

#[tokio::test]
async fn test_slot_offset_index() {
    let base_url = get_index_base_url().unwrap();
    let mut index = SlotOffsetIndex::new(base_url).unwrap();
    assert_eq!(index.get_offset(123456).await.unwrap(), 1218137096);
    assert_eq!(index.get_offset(123456789).await.unwrap(), 384461630701);
    let start = index.get_offset(334368000).await.unwrap();
    assert_eq!(start, 69224); // will cross epoch boundary at i = 1
    for i in 1..1000 {
        let slot = 334367999 + i;
        match slot {
            334368004 | 334368005 | 334368008 | 334368009 | 334368010 | 334368011 | 334368080
            | 334368081 | 334368082 | 334368083 | 334368108 | 334368109 | 334368112 | 334368113
            | 334368114 | 334368115 | 334368128 | 334368129 | 334368130 | 334368131 | 334368180
            | 334368181 | 334368182 | 334368183 | 334368236 | 334368237 | 334368238 | 334368239
            | 334368248 | 334368249 | 334368250 | 334368251 | 334368276 | 334368277 | 334368278
            | 334368279 | 334368500 | 334368501 | 334368502 | 334368503 | 334368644 | 334368645
            | 334368646 | 334368647 => {
                index.get_offset(slot).await.unwrap_err();
            }
            _ => assert!(index.get_offset(334367999 + i).await.unwrap() >= start),
        }
    }
}
