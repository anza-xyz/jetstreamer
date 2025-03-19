use std::{
    error::Error,
    io::{self, Read},
};

use cid::Cid;
use demo_rust_ipld_car::{
    node::{parse_any_from_cbordata, Node, NodeWithCid, NodesWithCids},
    utils,
};

pub struct RawNode {
    cid: Cid,
    data: Vec<u8>,
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

    pub fn from_cursor(cursor: &mut io::Cursor<Vec<u8>>) -> Result<RawNode, Box<dyn Error>> {
        let cid_version = utils::read_uvarint(cursor)?;
        // println!("CID version: {}", cid_version);

        let multicodec = utils::read_uvarint(cursor)?;
        // println!("Multicodec: {}", multicodec);

        // Multihash hash function code.
        let hash_function = utils::read_uvarint(cursor)?;
        // println!("Hash function: {}", hash_function);

        // Multihash digest length.
        let digest_length = utils::read_uvarint(cursor)?;
        // println!("Digest length: {}", digest_length);

        if digest_length > 64 {
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Digest length too long".to_owned(),
            )));
        }

        // reac actual digest
        let mut digest = vec![0u8; digest_length as usize];
        cursor.read_exact(&mut digest)?;

        // the rest is the data
        let mut data = vec![];
        cursor.read_to_end(&mut data)?;

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

pub struct NodeReader<R: Read> {
    reader: R,
    header: Vec<u8>,
    item_index: u64,
}

impl<R: Read> NodeReader<R> {
    pub fn new(reader: R) -> Result<NodeReader<R>, Box<dyn Error>> {
        let node_reader = NodeReader {
            reader,
            header: vec![],
            item_index: 0,
        };
        Ok(node_reader)
    }

    pub fn read_raw_header(&mut self) -> Result<Vec<u8>, Box<dyn Error>> {
        if !self.header.is_empty() {
            return Ok(self.header.clone());
        };
        let header_length = utils::read_uvarint(&mut self.reader)?;
        if header_length > 1024 {
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Header length too long".to_owned(),
            )));
        }
        let mut header = vec![0u8; header_length as usize];
        self.reader.read_exact(&mut header)?;

        self.header.clone_from(&header);

        let clone = header.clone();
        Ok(clone.as_slice().to_owned())
    }

    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Result<RawNode, Box<dyn Error>> {
        if self.header.is_empty() {
            self.read_raw_header()?;
        };

        // println!("Item index: {}", item_index);
        self.item_index += 1;

        // Read and decode the uvarint prefix (length of CID + data)
        let section_size = utils::read_uvarint(&mut self.reader)?;
        // println!("Section size: {}", section_size);

        if section_size > utils::MAX_ALLOWED_SECTION_SIZE as u64 {
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Section size too long".to_owned(),
            )));
        }

        // read whole item
        let mut item = vec![0u8; section_size as usize];
        self.reader.read_exact(&mut item)?;

        // dump item bytes as numbers
        // println!("Item bytes: {:?}", item);

        // now create a cursor over the item
        let mut cursor = io::Cursor::new(item);

        RawNode::from_cursor(&mut cursor)
    }

    pub fn next_parsed(&mut self) -> Result<NodeWithCid, Box<dyn Error>> {
        let raw_node = self.next()?;
        let cid = raw_node.cid;
        Ok(NodeWithCid::new(cid, raw_node.parse()?))
    }

    pub fn read_until_block(&mut self) -> Result<NodesWithCids, Box<dyn Error>> {
        let mut nodes = NodesWithCids::new();
        loop {
            let node = self.next_parsed()?;
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
}
