use crate::node::{parse_any_from_cbordata, Node, NodeWithCid, NodesWithCids};
use cid::Cid;
use demo_rust_ipld_car::utils;
use reqwest::RequestBuilder;
use rseek::Seekable;
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
    item_index: u64,
}

impl<R: AsyncRead + Unpin + AsyncSeek + Len> AsyncNodeReader<R> {
    pub fn new(reader: R) -> AsyncNodeReader<R> {
        let node_reader = AsyncNodeReader {
            reader,
            header: vec![],
            item_index: 0,
        };
        node_reader
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
