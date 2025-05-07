use crate::epochs::{epoch_exists, epoch_to_slot_range, fetch_epoch_stream, slot_to_epoch};
use crate::node::Node;
use crate::node_reader::NodeReader;
use rayon::prelude::*;
use reqwest::Client;
use std::collections::HashMap;
use std::io::SeekFrom;
use std::path::{Path, PathBuf};
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

#[derive(Clone, PartialEq, Eq, Default)]
pub struct SlotOffsetIndex {
    index_dir: PathBuf,
    data: HashMap<u64, u64>,
}

impl SlotOffsetIndex {
    pub fn new(index_dir: impl AsRef<Path>) -> Result<Self, Box<dyn std::error::Error>> {
        if !index_dir.as_ref().exists() {
            return Err(format!("index directory does not exist: {:?}", index_dir.as_ref()).into());
        }
        Ok(Self {
            index_dir: index_dir.as_ref().to_path_buf(),
            data: HashMap::new(),
        })
    }

    pub async fn load_index_file(
        &mut self,
        idx_path: impl AsRef<Path>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let idx_path = idx_path.as_ref();
        if !idx_path.exists() {
            return Err(format!("index file does not exist: {}", idx_path.display()).into());
        }
        let file_name = idx_path.file_name().unwrap().to_str().unwrap();
        log::info!("Loading slot offset index file: {}...", file_name);
        let file = File::open(&idx_path).await?;
        let mut file = tokio::io::BufReader::new(file);
        let mut buf = vec![0u8; 24];
        let mut i = 0;
        while file.read_exact(&mut buf).await.is_ok() {
            let slot = u64::from_le_bytes(buf[0..8].try_into().unwrap());
            let offset = u64::from_le_bytes(buf[8..16].try_into().unwrap());
            self.data.insert(slot, offset);
            i += 1;
        }
        log::info!("Loaded {} slot offset index entries from {}.", i, file_name);
        Ok(())
    }

    pub async fn get_offset(&mut self, slot: u64) -> Result<u64, Box<dyn std::error::Error>> {
        if let Some(offset) = self.data.get(&slot) {
            return Ok(*offset);
        }
        let epoch = slot_to_epoch(slot);
        let idx_path = self.index_dir.join(format!("epoch-{}.idx", epoch));
        if !idx_path.exists() {
            return Err(format!("slot index file does not exist: {:?}", idx_path).into());
        }
        self.load_index_file(&idx_path).await?;
        if let Some(offset) = self.data.get(&slot) {
            return Ok(*offset);
        }
        Err(format!(
            "Slot {} not found in index {}, possible that leader missed this slot",
            slot,
            idx_path.display()
        )
        .into())
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

/// Queries the current epoch from mainnet
pub async fn current_epoch(client: &Client) -> Result<u64, Box<dyn std::error::Error>> {
    let url = "https://api.mainnet-beta.solana.com";
    let request_body = r#"{"jsonrpc":"2.0","id":1,"method":"getEpochInfo","params":[]}"#;
    let response = client
        .post(url)
        .header("Content-Type", "application/json")
        .body(request_body)
        .send()
        .await?;
    let text = response.text().await?;
    let epoch_info: serde_json::Value = serde_json::from_str(&text).unwrap();
    let epoch = epoch_info["result"]["epoch"].as_u64().unwrap();
    Ok(epoch)
}

pub async fn latest_old_faithful_epoch(
    client: &Client,
    epoch: Option<u64>,
) -> Result<(u64, u64, u64), Box<dyn std::error::Error>> {
    let mut epoch = if let Some(epoch) = epoch {
        epoch
    } else {
        current_epoch(client).await?
    };
    loop {
        if epoch_exists(epoch, client).await {
            let (start_slot, end_slot) = epoch_to_slot_range(epoch);
            return Ok((epoch, start_slot, end_slot));
        }
        epoch -= 1;
    }
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
        .num_threads(32)
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

    (0..of1_last_epoch)
        .into_iter()
        .par_bridge()
        .for_each(|epoch| {
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
                        build_index(&client, epoch, &idx_path, Some(offset))
                            .await
                            .unwrap();
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

#[tokio::test]
async fn test_slot_offset_index() {
    let cache_dir = std::env::var("SOLIRA_OFFSET_CACHE_DIR").unwrap_or_else(|_| {
        if PathBuf::from("./geyser_replay").exists() {
            "./geyser-replay/src/index".to_string()
        } else {
            "./src/index".to_string()
        }
    });
    let mut index = SlotOffsetIndex::new(cache_dir).unwrap();
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
