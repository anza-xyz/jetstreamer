use crate::node::{parse_any_from_cbordata, Node, NodeWithCid, NodesWithCids};
use cid::Cid;
use demo_rust_ipld_car::subset::Subset;
use demo_rust_ipld_car::utils;
use multihash::Multihash;
use reqwest::RequestBuilder;
use rseek::Seekable;
use std::collections::HashMap;
use std::io::SeekFrom;
use std::vec::Vec;
use std::{
    error::Error,
    io::{self},
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt};

const MAX_VARINT_LEN_64: usize = 10;

pub async fn read_uvarint<R: AsyncRead + Unpin>(reader: &mut R) -> io::Result<u64> {
    let mut x = 0u64;
    let mut s = 0u32;
    let mut buffer = [0u8; 1];

    for i in 0..MAX_VARINT_LEN_64 {
        reader.read_exact(&mut buffer).await?;
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

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct RawNode {
    pub cid: Cid,
    pub data: Vec<u8>,
}

// Debug trait for RawNode
impl core::fmt::Debug for RawNode {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("RawNode")
            .field("cid", &self.cid)
            .field("data", &self.data)
            .finish()
    }
}

impl RawNode {
    pub fn new(cid: Cid, data: Vec<u8>) -> RawNode {
        RawNode { cid, data }
    }

    pub fn parse(&self) -> Result<Node, Box<dyn Error>> {
        let parsed = parse_any_from_cbordata(self.data.clone());
        if parsed.is_err() {
            println!("Error: {:?}", parsed.err().unwrap());
            Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Unknown type".to_owned(),
            )))
        } else {
            let node = parsed.unwrap();
            Ok(node)
        }
    }

    pub async fn from_cursor(cursor: &mut io::Cursor<Vec<u8>>) -> Result<RawNode, Box<dyn Error>> {
        let cid_version = read_uvarint(cursor).await?;
        // println!("CID version: {}", cid_version);

        let multicodec = read_uvarint(cursor).await?;
        // println!("Multicodec: {}", multicodec);

        // Multihash hash function code.
        let hash_function = read_uvarint(cursor).await?;
        // println!("Hash function: {}", hash_function);

        // Multihash digest length.
        let digest_length = read_uvarint(cursor).await?;
        // println!("Digest length: {}", digest_length);

        if digest_length > 64 {
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Digest length too long".to_owned(),
            )));
        }

        // reac actual digest
        let mut digest = vec![0u8; digest_length as usize];
        cursor.read_exact(&mut digest).await?;

        // the rest is the data
        let mut data = vec![];
        cursor.read_to_end(&mut data).await?;

        // println!("Data: {:?}", data);

        let ha = multihash::Multihash::wrap(hash_function, digest.as_slice())?;

        match cid_version {
            0 => {
                let cid = Cid::new_v0(ha)?;
                let raw_node = RawNode::new(cid, data);
                Ok(raw_node)
            }
            1 => {
                let cid = Cid::new_v1(multicodec, ha);
                let raw_node = RawNode::new(cid, data);
                Ok(raw_node)
            }
            _ => Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Unknown CID version".to_owned(),
            ))),
        }
    }
}

pub trait Len {
    fn len(&self) -> u64;
}

impl<F> Len for Seekable<F>
where
    F: Fn() -> RequestBuilder + Send + Sync + 'static,
{
    fn len(&self) -> u64 {
        self.file_size().unwrap_or(0)
    }
}

pub struct AsyncNodeReader<R: AsyncRead + AsyncSeek + Len> {
    reader: R,
    header: Vec<u8>,
    // // Map of CIDs to their offsets and lengths
    cid_offset_index: HashMap<Cid, (u64, u64)>,
    item_index: u64,
}

impl<R: AsyncRead + Unpin + AsyncSeek + Len> AsyncNodeReader<R> {
    pub fn new(reader: R) -> AsyncNodeReader<R> {
        let node_reader = AsyncNodeReader {
            reader,
            header: vec![],
            cid_offset_index: HashMap::new(),
            item_index: 0,
        };
        node_reader
    }

    /// Lazily build CID → (offset, size) map by parsing the embedded multihash
    /// index at the tail of the CAR file.
    pub async fn cid_offset_index(
        &mut self,
    ) -> Result<&std::collections::HashMap<cid::Cid, (u64, u64)>, Box<dyn std::error::Error>> {
        use cid::Cid;
        use multihash::Multihash;
        use tokio::io::{AsyncReadExt, AsyncSeekExt, SeekFrom};

        /* return cache if already built */
        if !self.cid_offset_index.is_empty() {
            log::info!(
                "cid_offset_index: cache hit ({} entries)",
                self.cid_offset_index.len()
            );
            return Ok(&self.cid_offset_index);
        }

        /* ────────────────────────────────────────────────────────────────────
        1.  Read first 4 bytes to see whether we have a CAR-v2 wrapper
        ──────────────────────────────────────────────────────────────────── */
        let mut magic4 = [0u8; 4];
        self.reader.seek(SeekFrom::Start(0)).await?;
        self.reader.read_exact(&mut magic4).await?;

        let (index_offset, index_size) = if &magic4 == b"\x0a\x8a\x4d\x54" {
            /* ── Fast path : CAR v2 ───────────────────────────────────────── */
            let mut prag = [0u8; 40];
            self.reader.seek(SeekFrom::Start(0)).await?;
            self.reader.read_exact(&mut prag).await?;
            let index_off = u64::from_le_bytes(prag[24..32].try_into().unwrap());
            let index_sz = u64::from_le_bytes(prag[32..40].try_into().unwrap());
            log::info!(
                "cid_offset_index: CAR-v2 index @ {} ({} B)",
                index_off,
                index_sz
            );
            (index_off, index_sz)
        } else {
            /* ── Fallback : scan last ≤128 MiB for a valid index marker ──── */
            const WINDOW: u64 = 128 * 1024 * 1024;
            let file_len = self.reader.len();
            let start = file_len.saturating_sub(WINDOW);
            let tail_len = file_len - start;
            log::info!(
                "cid_offset_index: scanning tail {} B for index marker",
                tail_len
            );

            self.reader.seek(SeekFrom::Start(start)).await?;
            let mut tail = vec![0u8; tail_len as usize];
            self.reader.read_exact(&mut tail).await?;

            /* tiny uvarint helper */
            fn uvar(slice: &[u8]) -> Option<(u64, usize)> {
                let mut x = 0u64;
                let mut s = 0u32;
                for (i, &b) in slice.iter().enumerate().take(10) {
                    if b < 0x80 {
                        return Some((x | ((b as u64) << s), i + 1));
                    }
                    x |= ((b & 0x7f) as u64) << s;
                    s += 7;
                }
                None
            }

            let mut marker_pos = None;
            for pos in (0..tail.len()).rev() {
                let (codec, len) = match uvar(&tail[pos..]) {
                    Some(v) => v,
                    None => continue,
                };
                if codec != 0x0400 && codec != 0x0401 {
                    continue;
                }

                /* Skip header fields to peek at width */
                let mut off = pos + len;
                if codec == 0x0401 {
                    if let Some((_, l)) = uvar(&tail[off..]) {
                        off += l;
                    } else {
                        continue;
                    }
                    if let Some((_, l)) = uvar(&tail[off..]) {
                        off += l;
                    } else {
                        continue;
                    }
                }
                if off + 4 > tail.len() {
                    continue;
                }
                let width = u32::from_le_bytes(tail[off..off + 4].try_into().unwrap()) as usize;
                let min = if codec == 0x0400 { 8 } else { 16 };
                let max = min + 64;
                if width < min || width > max {
                    continue;
                } // unrealistic

                marker_pos = Some(pos);
                log::info!("cid_offset_index: marker 0x{:x} at tail +{}", codec, pos);
                break;
            }
            let rel = marker_pos.ok_or("cid_offset_index: index marker not found")?;
            let index_off = start + rel as u64;
            let index_sz = file_len - index_off;
            log::info!("cid_offset_index: index @ {} ({} B)", index_off, index_sz);
            (index_off, index_sz)
        };

        /* ────────────────────────────────────────────────────────────────────
        2.  Read the index blob
        ──────────────────────────────────────────────────────────────────── */
        let mut buf = vec![0u8; index_size as usize];
        self.reader.seek(SeekFrom::Start(index_offset)).await?;
        self.reader.read_exact(&mut buf).await?;
        let mut cur = std::io::Cursor::new(&buf[..]);

        /* uvarint helper reused */
        fn uvar(mut s: &[u8]) -> Option<(u64, usize)> {
            let mut x = 0u64;
            let mut sh = 0u32;
            for i in 0..10 {
                let b = *s.get(0)?;
                s = &s[1..];
                if b < 0x80 {
                    return Some((x | ((b as u64) << sh), i + 1));
                }
                x |= ((b & 0x7f) as u64) << sh;
                sh += 7;
            }
            None
        }

        /* 3. codec header ---------------------------------------------------- */
        let (codec, len_codec) = uvar(cur.get_ref()).unwrap();
        cur.set_position(len_codec as u64);
        if codec == 0x0401 {
            let (_, l1) = uvar(&cur.get_ref()[cur.position() as usize..]).unwrap();
            cur.set_position(cur.position() + l1 as u64);
            let (_, l2) = uvar(&cur.get_ref()[cur.position() as usize..]).unwrap();
            cur.set_position(cur.position() + l2 as u64);
        }
        let min_fixed = if codec == 0x0400 { 8 } else { 16 };
        let has_size = codec == 0x0401;

        /* 4. buckets --------------------------------------------------------- */
        let mut bucket = 0u64;
        while (cur.position() as usize) < buf.len() {
            bucket += 1;
            let mut w4 = [0u8; 4];
            if cur.read_exact(&mut w4).await.is_err() {
                break;
            }
            let width = u32::from_le_bytes(w4) as usize;
            let mut c8 = [0u8; 8];
            if cur.read_exact(&mut c8).await.is_err() {
                break;
            }
            let count = u64::from_le_bytes(c8);

            let digest_len = match width.checked_sub(min_fixed) {
                Some(d) if d > 0 && d <= 64 => d,
                _ => {
                    log::info!("bucket {bucket}: bad width {width}, stop");
                    break;
                }
            };
            log::info!("bucket {bucket}: count={count}, digest_len={digest_len}");

            for _ in 0..count {
                let mut dig = vec![0u8; digest_len];
                if cur.read_exact(&mut dig).await.is_err() {
                    break;
                }
                let mut offb = [0u8; 8];
                if cur.read_exact(&mut offb).await.is_err() {
                    break;
                }
                let offset = u64::from_le_bytes(offb);

                let size = if has_size {
                    let mut szb = [0u8; 8];
                    if cur.read_exact(&mut szb).await.is_err() {
                        break;
                    }
                    u64::from_le_bytes(szb)
                } else {
                    0
                };

                let cid = Cid::new_v1(0x55, Multihash::from_bytes(&dig)?);
                self.cid_offset_index.insert(cid, (offset, size));
            }
        }

        log::info!(
            "cid_offset_index: parsed {} CIDs",
            self.cid_offset_index.len()
        );
        Ok(&self.cid_offset_index)
    }

    pub async fn read_raw_header(&mut self) -> Result<Vec<u8>, Box<dyn Error>> {
        if !self.header.is_empty() {
            return Ok(self.header.clone());
        };
        let header_length = read_uvarint(&mut self.reader).await?;
        if header_length > 1024 {
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Header length too long".to_owned(),
            )));
        }
        let mut header = vec![0u8; header_length as usize];
        self.reader.read_exact(&mut header).await?;

        self.header.clone_from(&header);

        let clone = header.clone();
        Ok(clone.as_slice().to_owned())
    }

    pub async fn skip_next(&mut self) -> Result<(), Box<dyn Error>> {
        if self.header.is_empty() {
            self.read_raw_header().await?;
        };

        // Read and decode the uvarint prefix (length of CID + data)
        let section_size = read_uvarint(&mut self.reader).await?;

        if section_size > utils::MAX_ALLOWED_SECTION_SIZE as u64 {
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Section size too long".to_owned(),
            )));
        }

        // skip item
        self.reader
            .seek(SeekFrom::Current(section_size as i64))
            .await?;

        Ok(())
    }

    #[allow(clippy::should_implement_trait)]
    pub async fn next(&mut self) -> Result<RawNode, Box<dyn Error>> {
        if self.header.is_empty() {
            self.read_raw_header().await?;
        };

        // println!("Item index: {}", item_index);
        self.item_index += 1;

        // Read and decode the uvarint prefix (length of CID + data)
        let section_size = read_uvarint(&mut self.reader).await?;
        // println!("Section size: {}", section_size);

        if section_size > utils::MAX_ALLOWED_SECTION_SIZE as u64 {
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Section size too long".to_owned(),
            )));
        }

        // read whole item
        let mut item = vec![0u8; section_size as usize];
        self.reader.read_exact(&mut item).await?;

        // dump item bytes as numbers
        // println!("Item bytes: {:?}", item);

        // now create a cursor over the item
        let mut cursor = io::Cursor::new(item);

        RawNode::from_cursor(&mut cursor).await
    }

    pub async fn next_parsed(&mut self) -> Result<NodeWithCid, Box<dyn Error>> {
        let raw_node = self.next().await?;
        let cid = raw_node.cid;
        Ok(NodeWithCid::new(cid, raw_node.parse()?))
    }

    pub async fn read_until_block(&mut self) -> Result<NodesWithCids, Box<dyn Error>> {
        let mut nodes = NodesWithCids::new();
        loop {
            let node = self.next_parsed().await?;
            if node.get_node().is_block() {
                nodes.push(node);
                break;
            }
            nodes.push(node);
        }
        Ok(nodes)
    }

    pub fn get_item_index(&self) -> u64 {
        self.item_index
    }

    /// Return all Subset nodes referenced by the Epoch in this CAR.
    pub async fn get_subsets(
        &mut self,
    ) -> Result<Vec<demo_rust_ipld_car::subset::Subset>, Box<dyn std::error::Error>>
    where
        R: Unpin,
    {
        use demo_rust_ipld_car::{epoch, subset};
        use serde_cbor::Value;

        // ── 1. Read / cache CAR header and decode it as CBOR map ────────────
        if self.header.is_empty() {
            self.read_raw_header().await?;
        };
        let header_cbor: Value = serde_cbor::from_slice(&self.header)?;

        // header_cbor must be a CBOR map
        let header_map = if let Value::Map(m) = header_cbor {
            m
        } else {
            return Err("CAR header is not a CBOR map".into());
        };

        // Extract "roots" key (array of links)
        let roots_val = header_map
            .iter()
            .find_map(|(k, v)| match k {
                Value::Text(s) if s == "roots" => Some(v),
                _ => None,
            })
            .ok_or("CAR header missing \"roots\" key")?;

        let roots_arr = if let Value::Array(arr) = roots_val {
            arr
        } else {
            return Err("\"roots\" is not an array".into());
        };

        if roots_arr.is_empty() {
            return Err("CAR header \"roots\" array is empty".into());
        }

        log::info!("roots: {:#?}", roots_arr);

        // Helpers ------------------------------------------------------------
        fn cid_from_link(val: &Value) -> Result<cid::Cid, Box<dyn std::error::Error>> {
            if let Value::Bytes(b) = val {
                if b.first() == Some(&0) {
                    return Ok(cid::Cid::try_from(b[1..].to_vec())?);
                }
            }
            Err("invalid DAG‑CBOR link encoding".into())
        }

        // TODO

        todo!()
    }

    pub async fn og(&mut self) -> Result<(), Box<dyn Error>> {
        const WINDOW_SIZE: usize = 100 * 1024 * 1024; // 50 MB
        let mut window = vec![0u8; WINDOW_SIZE];

        loop {
            let start_pos = self.reader.stream_position().await?;
            log::debug!("Scanning window from offset {}", start_pos);

            log::debug!("Reading {} bytes", WINDOW_SIZE);
            let bytes_read = self.reader.read_exact(&mut window).await?;
            log::debug!("Read {} bytes", bytes_read);
            if bytes_read == 0 {
                return Err("End of file reached before finding valid block".into());
            }

            for offset in 0..bytes_read {
                //log::debug!("Checking offset {}+{}", start_pos, offset);
                let slice = &window[offset..bytes_read];
                let mut cursor = io::Cursor::new(slice);

                let maybe_block = async {
                    //let _ = read_uvarint(&mut cursor).await.ok()?; // skip CID version
                    let section_size = read_uvarint(&mut cursor).await.ok()?;
                    if section_size > utils::MAX_ALLOWED_SECTION_SIZE as u64 {
                        return None;
                    }

                    let mut section = vec![0u8; section_size as usize];
                    cursor.read_exact(&mut section).await.ok()?;

                    let mut node_cursor = io::Cursor::new(section);
                    let raw_node = RawNode::from_cursor(&mut node_cursor).await.ok()?;
                    log::debug!("D. raw_node: {:?}", raw_node.cid);
                    let parsed = raw_node.parse();
                    let Ok(parsed) = parsed else {
                        let parsed = parsed.unwrap_err();
                        log::warn!("Error parsing node: {:?}", parsed);
                        return None;
                    };
                    log::debug!("E. parsed: {:?}", parsed.is_block());

                    if parsed.is_block() {
                        Some(parsed)
                    } else {
                        None
                    }
                }
                .await;

                if let Some(block) = maybe_block {
                    log::debug!("Found valid block at offset {}", start_pos + offset as u64);
                    log::debug!("slot num: {}", block.get_block().unwrap().slot);
                    self.reader
                        .seek(SeekFrom::Start(start_pos + offset as u64))
                        .await?;
                    return Ok(());
                }
            }

            if bytes_read < WINDOW_SIZE {
                return Err("Reached EOF without finding valid block".into());
            }

            let backtrack = WINDOW_SIZE.saturating_sub(1);
            log::debug!(
                "No valid block found in window, sliding forward by {} bytes",
                backtrack
            );

            self.reader
                .seek(SeekFrom::Current(-(backtrack as i64)))
                .await?;
        }
    }

    pub async fn scan_window_in_chunks(buffer: &[u8]) -> Result<Option<usize>, Box<dyn Error>> {
        use std::sync::Arc;
        use tokio::task::JoinSet;

        const NUM_CORES: usize = 16;
        let chunk_size = buffer.len() / NUM_CORES;
        let buffer: Arc<[u8]> = Arc::from(buffer); // Makes it an Arc<[u8]> for safety

        let mut tasks = JoinSet::new();

        // TODO: do a sliding window with 1 byte offset for each thread instead of regions
        for i in 0..NUM_CORES {
            let buffer = Arc::clone(&buffer);
            let start = i * chunk_size;
            let end = buffer.len(); // overlap into the next chunk

            tasks.spawn(async move {
                let local_buf = &buffer[start..end];
                for offset in 0..local_buf.len() {
                    let slice = &local_buf[offset..];
                    let mut cursor = std::io::Cursor::new(slice);

                    let maybe_block = async {
                        let section_size = read_uvarint(&mut cursor).await.ok()?;
                        if section_size > utils::MAX_ALLOWED_SECTION_SIZE as u64 {
                            return None;
                        }

                        let mut section = vec![0u8; section_size as usize];
                        cursor.read_exact(&mut section).await.ok()?;

                        let mut node_cursor = std::io::Cursor::new(section);
                        let raw_node = RawNode::from_cursor(&mut node_cursor).await.ok()?;
                        let parsed = raw_node.parse().ok()?;

                        log::debug!("Parsed node in thread {}: {}", i, raw_node.cid);
                        if parsed.is_block() {
                            Some(offset)
                        } else {
                            None
                        }
                    }
                    .await;

                    if let Some(local_offset) = maybe_block {
                        return Some(start + local_offset);
                    }

                    tokio::task::yield_now().await;
                }

                None
            });
        }

        while let Some(res) = tasks.join_next().await {
            if let Ok(Some(offset)) = res {
                return Ok(Some(offset));
            }
        }

        Ok(None)
    }

    /// Scans forward from the current position until a valid block is found.
    /// Leaves the reader positioned at the start of the valid block.
    pub async fn skip_until_valid_block(&mut self) -> Result<(), Box<dyn Error>> {
        // if self.header.is_empty() {
        //     log::debug!("Reading header before scanning for block");
        //     self.read_raw_header().await?;
        // }

        const WINDOW_SIZE: usize = 100 * 1024 * 1024; // 50 MB
        let mut window = vec![0u8; WINDOW_SIZE];

        loop {
            let start_pos = self.reader.stream_position().await?;
            log::debug!("Scanning window from offset {}", start_pos);

            log::debug!("Reading {} bytes", WINDOW_SIZE);
            let bytes_read = self.reader.read_exact(&mut window).await?;
            log::debug!("Read {} bytes", bytes_read);
            if bytes_read == 0 {
                return Err("End of file reached before finding valid block".into());
            }

            Self::scan_window_in_chunks(&window).await.map_err(|e| {
                log::error!("Error scanning window: {}", e);
                e
            })?;

            if bytes_read < WINDOW_SIZE {
                return Err("Reached EOF without finding valid block".into());
            }

            let backtrack = WINDOW_SIZE.saturating_sub(1);
            log::debug!(
                "No valid block found in window, sliding forward by {} bytes",
                backtrack
            );

            self.reader
                .seek(SeekFrom::Current(-(backtrack as i64)))
                .await?;
        }
    }

    pub async fn seek_to_slot(&mut self, target_slot: u64) -> Result<bool, Box<dyn Error>> {
        log::debug!("Seeking to slot {}", target_slot);
        log::debug!("Current position: {}", self.reader.stream_position().await?);

        if self.header.is_empty() {
            log::debug!("Reading header");
            self.read_raw_header().await?;
        }

        let file_len = self.reader.len() as u64;
        log::debug!("File length: {}", file_len);

        let mut low = file_len / 4;
        let mut high = file_len;

        log::debug!("Initial low: {}, high: {}", low, high);

        while low < high {
            let mid = (low + high) / 2;
            log::debug!("Seeking to midpoint: {}", mid);
            self.reader.seek(SeekFrom::Start(mid)).await?;

            let mut found_valid = false;
            for _ in 0..3 {
                let pos = self.reader.stream_position().await?;
                log::debug!("Trying block at pos {}", pos);

                match self.read_until_block().await {
                    Ok(nodes) => match nodes.get_block() {
                        Ok(block) => {
                            let slot = block.slot;
                            log::debug!("Found block with slot {}", slot);

                            if slot == target_slot {
                                self.reader.seek(SeekFrom::Start(pos)).await?;
                                return Ok(true);
                            } else if slot < target_slot {
                                low = pos + 1;
                            } else {
                                high = mid;
                            }

                            found_valid = true;
                            break;
                        }
                        Err(e) => {
                            log::debug!("Expected block, got error: {}", e);
                        }
                    },
                    Err(e) => {
                        log::debug!("Error decoding block: {}", e);
                    }
                }

                self.reader.seek(SeekFrom::Current(64)).await?;
            }

            if !found_valid {
                log::debug!("Failed to find a valid block near {}", mid);
                break;
            }
        }

        Ok(false)
    }
}

fn cid_from_cbor_link(val: &serde_cbor::Value) -> Result<cid::Cid, Box<dyn std::error::Error>> {
    if let serde_cbor::Value::Bytes(b) = val {
        if b.first() == Some(&0) {
            return Ok(cid::Cid::try_from(b[1..].to_vec())?);
        }
    }
    Err("invalid DAG‑CBOR link encoding".into())
}

#[tokio::test]
async fn test_async_node_reader() {
    use crate::epochs_async::fetch_epoch_stream;
    let client = reqwest::Client::new();
    let stream = fetch_epoch_stream(670, &client).await;
    let mut reader = AsyncNodeReader::new(stream);
    let nodes = reader.read_until_block().await.unwrap();
    assert_eq!(nodes.len(), 117);
}

#[tokio::test]
async fn test_skip_until_valid_block() {
    solana_logger::setup_with_default("debug");
    use crate::epochs_async::fetch_epoch_stream;
    let client = reqwest::Client::new();
    let stream = fetch_epoch_stream(670, &client).await;
    let mut reader = AsyncNodeReader::new(stream);
    //reader.read_raw_header().await.unwrap();
    // reader
    //     .reader
    //     .seek(SeekFrom::Start(reader.reader.len() / 2))
    //     .await
    //     .unwrap();
    reader.skip_until_valid_block().await.unwrap();
    //let _nodes = reader.read_until_block().await.unwrap();
}

#[tokio::test]
async fn test_seek_to_slot() {
    solana_logger::setup_with_default("debug");
    log::debug!("Starting test_seek_to_slot");
    use crate::epochs_async::fetch_epoch_stream;
    let client = reqwest::Client::new();
    let stream = fetch_epoch_stream(670, &client).await;
    let mut reader = AsyncNodeReader::new(stream);
    assert_eq!(reader.seek_to_slot(100).await.unwrap(), false);
    //let nodes = reader.read_until_block().await.unwrap();
    //assert_eq!(nodes.len(), 117);
}

#[tokio::test]
async fn read_first_subset() {
    solana_logger::setup_with_default("debug");
    use crate::{epochs_async::fetch_epoch_stream, node_reader::AsyncNodeReader};

    let client = reqwest::Client::new();
    let stream = fetch_epoch_stream(670, &client).await;
    let mut rdr = AsyncNodeReader::new(stream);

    rdr.cid_offset_index().await.unwrap();
    log::info!("CID offset index: {:#?}", rdr.cid_offset_index);
}
