use crate::epochs::slot_to_epoch;
use crate::firehose::FirehoseError;
use crate::index::{SlotOffsetIndexError, slot_to_offset};
use crate::node::{Node, NodeWithCid, NodesWithCids, parse_any_from_cbordata};
use crate::utils;
use cid::Cid;
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

/// Reads an unsigned LEB128-encoded integer from the provided async reader.
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

/// Raw DAG-CBOR node paired with its [`Cid`].
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct RawNode {
    /// Content identifier for the node.
    pub cid: Cid,
    /// Raw CBOR-encoded bytes for the node.
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
    /// Creates a new [`RawNode`] from a CID and CBOR payload.
    pub fn new(cid: Cid, data: Vec<u8>) -> RawNode {
        RawNode { cid, data }
    }

    /// Parses the CBOR payload into a typed [`Node`].
    pub fn parse(&self) -> Result<Node, Box<dyn Error>> {
        match parse_any_from_cbordata(self.data.clone()) {
            Ok(node) => Ok(node),
            Err(err) => {
                println!("Error: {:?}", err);
                Err(Box::new(std::io::Error::other("Unknown type".to_owned())))
            }
        }
    }

    /// Reads a [`RawNode`] from a CAR section cursor.
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
            return Err(Box::new(std::io::Error::other(format!(
                "Digest length too long, position={}",
                cursor.position()
            ))));
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
            _ => Err(Box::new(std::io::Error::other(
                "Unknown CID version".to_owned(),
            ))),
        }
    }
}

/// Trait for readers that can report their total length.
pub trait Len {
    /// Returns the total number of bytes available.
    fn len(&self) -> u64;
    /// Returns `true` when the length is zero.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl<F> Len for Seekable<F>
where
    F: Fn() -> RequestBuilder + Send + Sync + 'static,
{
    fn len(&self) -> u64 {
        self.file_size.unwrap_or(0)
    }
}

/// Incremental reader that produces typed nodes from an Old Faithful CAR stream.
pub struct NodeReader<R: AsyncRead + AsyncSeek + Len> {
    /// Underlying stream yielding Old Faithful CAR bytes.
    pub reader: R,
    /// Cached Old Faithful CAR header data.
    pub header: Vec<u8>,
    /// Number of Old Faithful items that have been read so far.
    pub item_index: u64,
}

impl<R: AsyncRead + Unpin + AsyncSeek + Len> NodeReader<R> {
    /// Wraps an async reader and primes it for Old Faithful CAR decoding.
    pub fn new(reader: R) -> NodeReader<R> {
        NodeReader {
            reader,
            header: vec![],
            item_index: 0,
        }
    }

    /// Returns the raw Old Faithful CAR header, fetching and caching it on first use.
    pub async fn read_raw_header(&mut self) -> Result<Vec<u8>, Box<dyn Error>> {
        if !self.header.is_empty() {
            return Ok(self.header.clone());
        };
        let header_length = read_uvarint(&mut self.reader).await?;
        if header_length > 1024 {
            return Err(Box::new(std::io::Error::other(
                "Header length too long".to_owned(),
            )));
        }
        let mut header = vec![0u8; header_length as usize];
        self.reader.read_exact(&mut header).await?;

        self.header.clone_from(&header);

        let clone = header.clone();
        Ok(clone.as_slice().to_owned())
    }

    /// Seeks the underlying reader to the Old Faithful CAR section that begins at `slot`.
    pub async fn seek_to_slot(&mut self, slot: u64) -> Result<(), FirehoseError> {
        self.seek_to_slot_inner(slot).await
    }

    async fn seek_to_slot_inner(&mut self, slot: u64) -> Result<(), FirehoseError> {
        if self.header.is_empty() {
            self.read_raw_header()
                .await
                .map_err(FirehoseError::SeekToSlotError)?;
        };

        let epoch = slot_to_epoch(slot);

        let res = slot_to_offset(slot).await;
        if let Err(SlotOffsetIndexError::SlotNotFound(..)) = res {
            log::warn!("Slot {} not found in index, seeking to next slot", slot);
            // Box the recursive call to avoid infinitely sized future
            return Box::pin(self.seek_to_slot_inner(slot + 1)).await;
        }
        let offset = res?;
        log::info!(
            "Seeking to slot {} in epoch {} @ offset {}",
            slot,
            epoch,
            offset
        );
        self.reader
            .seek(SeekFrom::Start(offset))
            .await
            .map_err(|e| FirehoseError::SeekToSlotError(Box::new(e)))?;

        Ok(())
    }

    #[allow(clippy::should_implement_trait)]
    /// Reads the next raw node from the Old Faithful stream without parsing it.
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
            return Err(Box::new(std::io::Error::other(
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

    /// Reads and parses the next node, returning it paired with its [`Cid`].
    pub async fn next_parsed(&mut self) -> Result<NodeWithCid, Box<dyn Error>> {
        let raw_node = self.next().await?;
        let cid = raw_node.cid;
        Ok(NodeWithCid::new(cid, raw_node.parse()?))
    }

    /// Continues reading nodes until the next block is encountered.
    pub async fn read_until_block(&mut self) -> Result<NodesWithCids, Box<dyn Error>> {
        let mut nodes = NodesWithCids::new();
        loop {
            let node = match self.next_parsed().await {
                Ok(node) => node,
                Err(e)
                    if e.downcast_ref::<io::Error>()
                        .is_some_and(|io_err| io_err.kind() == io::ErrorKind::UnexpectedEof) =>
                {
                    break;
                }
                Err(e) => return Err(e),
            };
            if node.get_node().is_block() {
                nodes.push(node);
                break;
            }
            nodes.push(node);
        }
        Ok(nodes)
    }

    /// Returns the number of Old Faithful CAR items that have been yielded so far.
    pub fn get_item_index(&self) -> u64 {
        self.item_index
    }
}

/// Extracts a CID from a DAG-CBOR link value.
pub fn cid_from_cbor_link(val: &serde_cbor::Value) -> Result<cid::Cid, Box<dyn std::error::Error>> {
    if let serde_cbor::Value::Bytes(b) = val
        && b.first() == Some(&0)
    {
        return Ok(cid::Cid::try_from(b[1..].to_vec())?);
    }
    Err("invalid DAG‑CBOR link encoding".into())
}

#[tokio::test]
async fn test_async_node_reader() {
    use crate::epochs::fetch_epoch_stream;
    let client = reqwest::Client::new();
    let stream = fetch_epoch_stream(670, &client).await;
    let mut reader = NodeReader::new(stream);
    let nodes = reader.read_until_block().await.unwrap();
    assert_eq!(nodes.len(), 117);
}
