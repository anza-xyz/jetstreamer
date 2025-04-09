use cid::Cid;
use demo_rust_ipld_car::node::{parse_any_from_cbordata, Node, NodeWithCid, NodesWithCids};
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

    pub async fn find_block_with_slot(
        &mut self,
        target_slot: u64,
    ) -> Result<Option<RawNode>, Box<dyn Error>> {
        if self.header.is_empty() {
            self.read_raw_header().await?;
        }

        let file_len = self.reader.len() as u64;
        let mut low = 0;
        let mut high = file_len;

        while low < high {
            let mid = (low + high) / 2;

            // Seek to the middle of the file (could land inside a block)
            self.reader.seek(SeekFrom::Start(mid)).await?;

            // Skip any partial data by trying to read and discard a node
            // If it fails, shift forward a little and try again
            let mut found_valid = false;
            for _ in 0..3 {
                let pos = self.reader.stream_position().await?;
                if let Ok(_) = read_uvarint(&mut self.reader).await {
                    if let Ok(section_size) = read_uvarint(&mut self.reader).await {
                        if section_size > utils::MAX_ALLOWED_SECTION_SIZE as u64 {
                            break;
                        }

                        let mut buf = vec![0u8; section_size as usize];
                        if self.reader.read_exact(&mut buf).await.is_ok() {
                            let mut cursor = io::Cursor::new(buf);
                            if let Ok(node) = RawNode::from_cursor(&mut cursor).await {
                                if let Ok(parsed) = node.parse() {
                                    if let Some(block) = parsed.get_block() {
                                        let slot = block.slot;
                                        if slot == target_slot {
                                            return Ok(Some(node));
                                        } else if slot < target_slot {
                                            low = pos + section_size; // start after this block
                                        } else {
                                            high = mid; // try earlier
                                        }
                                        found_valid = true;
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }

                // seek forward a bit and try again
                self.reader.seek(SeekFrom::Current(64)).await?;
            }

            if !found_valid {
                // Could not decode a valid block even after 3 attempts; give up
                break;
            }
        }

        Ok(None)
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
