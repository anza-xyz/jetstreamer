use {
    crate::{block, dataframe, entry, epoch, rewards, subset, transaction, utils},
    cid::Cid,
    core::hash::Hasher,
    crc::{CRC_64_GO_ISO, Crc},
    fnv::FnvHasher,
    std::{
        error::Error,
        fmt,
        io::{self, Read},
        vec::Vec,
    },
};

/// Pairing of a decoded [`Node`] with its [`Cid`].
pub struct NodeWithCid {
    cid: Cid,
    node: Node,
}

impl NodeWithCid {
    /// Creates a new `(CID, node)` pair.
    pub fn new(cid: Cid, node: Node) -> NodeWithCid {
        NodeWithCid { cid, node }
    }

    /// Returns the CID associated with the node.
    pub fn get_cid(&self) -> &Cid {
        &self.cid
    }

    /// Returns the decoded node.
    pub fn get_node(&self) -> &Node {
        &self.node
    }
}

/// Convenience collection that retains the CID for every stored node.
#[derive(Default)]
pub struct NodesWithCids(
    #[doc = "Ordered collection of nodes paired with their content identifiers."]
    pub  Vec<NodeWithCid>,
);

impl NodesWithCids {
    /// Creates an empty [`NodesWithCids`].
    pub fn new() -> NodesWithCids {
        NodesWithCids(vec![])
    }

    /// Appends a node to the collection.
    pub fn push(&mut self, node_with_cid: NodeWithCid) {
        self.0.push(node_with_cid);
    }

    /// Returns the number of stored nodes.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns `true` if no nodes are stored.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the node at `index`.
    pub fn get(&self, index: usize) -> &NodeWithCid {
        &self.0[index]
    }

    /// Looks up a node by CID.
    pub fn get_by_cid(&self, cid: &Cid) -> Option<&NodeWithCid> {
        self.0
            .iter()
            .find(|&node_with_cid| node_with_cid.get_cid() == cid)
    }

    /// Reassembles a potentially multi-part dataframe using the nodes in the collection.
    pub fn reassemble_dataframes(
        &self,
        first_dataframe: dataframe::DataFrame,
    ) -> Result<Vec<u8>, Box<dyn Error>> {
        let mut data = first_dataframe.data.to_vec();
        let mut next_arr = first_dataframe.next;
        while next_arr.is_some() {
            for next_cid in next_arr.clone().unwrap() {
                let next_node = self.get_by_cid(&next_cid);
                if next_node.is_none() {
                    return Err(Box::new(std::io::Error::other(std::format!(
                        "Missing CID: {:?}",
                        next_cid
                    ))));
                }
                let next_node_un = next_node.unwrap();

                if !next_node_un.get_node().is_dataframe() {
                    return Err(Box::new(std::io::Error::other(std::format!(
                        "Expected DataFrame, got {:?}",
                        next_node_un.get_node()
                    ))));
                }

                let next_dataframe = next_node_un.get_node().get_dataframe().unwrap();
                data.extend(next_dataframe.data.to_vec());
                next_arr.clone_from(&next_dataframe.next);
            }
        }

        if first_dataframe.hash.is_some() {
            let wanted_hash = first_dataframe.hash.unwrap();
            verify_hash(data.clone(), wanted_hash)?;
        }
        Ok(data)
    }

    /// Iterates over every node and invokes `f`.
    pub fn each<F>(&self, mut f: F) -> Result<(), Box<dyn Error>>
    where
        F: FnMut(&NodeWithCid) -> Result<(), Box<dyn Error>>,
    {
        for node_with_cid in &self.0 {
            f(node_with_cid)?;
        }
        Ok(())
    }

    /// Returns the CIDs for all stored nodes.
    pub fn get_cids(&self) -> Vec<Cid> {
        let mut cids = vec![];
        for node_with_cid in &self.0 {
            cids.push(*node_with_cid.get_cid());
        }
        cids
    }

    /// Returns a reference to the final [`block::Block`] in the collection.
    pub fn get_block(&self) -> Result<&block::Block, Box<dyn Error>> {
        // the last node should be a block
        let last_node = self.0.last();
        if last_node.is_none() {
            return Err(Box::new(std::io::Error::other("No nodes".to_owned())));
        }
        let last_node_un = last_node.unwrap();
        if !last_node_un.get_node().is_block() {
            return Err(Box::new(std::io::Error::other(std::format!(
                "Expected Block, got {:?}",
                last_node_un.get_node()
            ))));
        }
        let block = last_node_un.get_node().get_block().unwrap();
        Ok(block)
    }
}

/// Validates the provided data against the expected CRC64 (or legacy FNV) hash.
pub fn verify_hash(data: Vec<u8>, hash: u64) -> Result<(), Box<dyn Error>> {
    let crc64 = checksum_crc64(&data);
    if crc64 != hash {
        // Maybe it's the legacy checksum function?
        let fnv = checksum_fnv(&data);
        if fnv != hash {
            return Err(Box::new(std::io::Error::other(std::format!(
                "data hash mismatch: wanted {:?}, got crc64={:?}, fnv={:?}",
                hash,
                crc64,
                fnv
            ))));
        }
    }
    Ok(())
}

fn checksum_crc64(data: &[u8]) -> u64 {
    let crc = Crc::<u64>::new(&CRC_64_GO_ISO);
    let mut digest = crc.digest();
    digest.update(data);
    digest.finalize()
}

fn checksum_fnv(data: &[u8]) -> u64 {
    let mut hasher = FnvHasher::default();
    hasher.write(data);
    hasher.finish()
}

/// Unified representation of all decoded firehose node types.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum Node {
    /// Raw transaction node.
    Transaction(transaction::Transaction),
    /// Ledger entry node.
    Entry(entry::Entry),
    /// Block node containing ledger metadata and entries.
    Block(block::Block),
    /// Subset node linking to a contiguous block range.
    Subset(subset::Subset),
    /// Epoch node referencing subset descriptors.
    Epoch(epoch::Epoch),
    /// Rewards node containing per-account payouts.
    Rewards(rewards::Rewards),
    /// Data frame node wrapping arbitrary binary payloads.
    DataFrame(dataframe::DataFrame),
}

impl Node {
    /// Returns `true` if this node is a [`transaction::Transaction`].
    pub fn is_transaction(&self) -> bool {
        matches!(self, Node::Transaction(_))
    }

    /// Returns `true` if this node is an [`entry::Entry`].
    pub fn is_entry(&self) -> bool {
        matches!(self, Node::Entry(_))
    }

    /// Returns `true` if this node is a [`block::Block`].
    pub fn is_block(&self) -> bool {
        matches!(self, Node::Block(_))
    }

    /// Returns `true` if this node is a [`subset::Subset`].
    pub fn is_subset(&self) -> bool {
        matches!(self, Node::Subset(_))
    }

    /// Returns `true` if this node is an [`epoch::Epoch`].
    pub fn is_epoch(&self) -> bool {
        matches!(self, Node::Epoch(_))
    }

    /// Returns `true` if this node is a [`rewards::Rewards`].
    pub fn is_rewards(&self) -> bool {
        matches!(self, Node::Rewards(_))
    }

    /// Returns `true` if this node is a [`dataframe::DataFrame`].
    pub fn is_dataframe(&self) -> bool {
        matches!(self, Node::DataFrame(_))
    }

    /// Returns the transaction if this node is [`Node::Transaction`].
    pub fn get_transaction(&self) -> Option<&transaction::Transaction> {
        match self {
            Node::Transaction(transaction) => Some(transaction),
            _ => None,
        }
    }

    /// Returns the entry if this node is [`Node::Entry`].
    pub fn get_entry(&self) -> Option<&entry::Entry> {
        match self {
            Node::Entry(entry) => Some(entry),
            _ => None,
        }
    }

    /// Returns the block if this node is [`Node::Block`].
    pub fn get_block(&self) -> Option<&block::Block> {
        match self {
            Node::Block(block) => Some(block),
            _ => None,
        }
    }

    /// Returns the subset if this node is [`Node::Subset`].
    pub fn get_subset(&self) -> Option<&subset::Subset> {
        match self {
            Node::Subset(subset) => Some(subset),
            _ => None,
        }
    }

    /// Returns the epoch if this node is [`Node::Epoch`].
    pub fn get_epoch(&self) -> Option<&epoch::Epoch> {
        match self {
            Node::Epoch(epoch) => Some(epoch),
            _ => None,
        }
    }

    /// Returns the rewards data if this node is [`Node::Rewards`].
    pub fn get_rewards(&self) -> Option<&rewards::Rewards> {
        match self {
            Node::Rewards(rewards) => Some(rewards),
            _ => None,
        }
    }

    /// Returns the dataframe if this node is [`Node::DataFrame`].
    pub fn get_dataframe(&self) -> Option<&dataframe::DataFrame> {
        match self {
            Node::DataFrame(dataframe) => Some(dataframe),
            _ => None,
        }
    }
}

// parse_any_from_cbordata parses any CBOR data into either a Epoch, Subset, Block, Rewards, Entry, or Transaction
/// Parses the raw CBOR payload into the appropriate [`Node`] variant.
pub fn parse_any_from_cbordata(data: Vec<u8>) -> Result<Node, Box<dyn Error>> {
    let decoded_data: serde_cbor::Value = serde_cbor::from_slice(&data)?;
    // Process the decoded data
    // println!("Data: {:?}", decoded_data);
    let cloned_data = decoded_data.clone();

    // decoded_data is an serde_cbor.Array; print the kind, which is the first element of the array
    if let serde_cbor::Value::Array(array) = decoded_data {
        // println!("Kind: {:?}", array[0]);
        if let Some(serde_cbor::Value::Integer(kind)) = array.first() {
            // println!(
            //     "Kind: {:?}",
            //     Kind::from_u64(kind as u64).unwrap().to_string()
            // );

            // based on the kind, we can decode the rest of the data
            let Some(kind) = Kind::from_u64(*kind as u64) else {
                return Err(Box::new(std::io::Error::other(std::format!(
                    "Invalid kind: {:?}",
                    kind
                ))));
            };
            match kind {
                Kind::Transaction => {
                    let transaction = transaction::Transaction::from_cbor(cloned_data)?;
                    return Ok(Node::Transaction(transaction));
                }
                Kind::Entry => {
                    let entry = entry::Entry::from_cbor(cloned_data)?;
                    return Ok(Node::Entry(entry));
                }
                Kind::Block => {
                    let block = block::Block::from_cbor(cloned_data)?;
                    return Ok(Node::Block(block));
                }
                Kind::Subset => {
                    let subset = subset::Subset::from_cbor(cloned_data)?;
                    return Ok(Node::Subset(subset));
                }
                Kind::Epoch => {
                    let epoch = epoch::Epoch::from_cbor(cloned_data)?;
                    return Ok(Node::Epoch(epoch));
                }
                Kind::Rewards => {
                    let rewards = rewards::Rewards::from_cbor(cloned_data)?;
                    return Ok(Node::Rewards(rewards));
                }
                Kind::DataFrame => {
                    let dataframe = dataframe::DataFrame::from_cbor(cloned_data)?;
                    return Ok(Node::DataFrame(dataframe));
                } // unknown => {
                  //     return Err(Box::new(std::io::Error::new(
                  //         std::io::ErrorKind::Other,
                  //         std::format!("Unknown type: {:?}", unknown),
                  //     )))
                  // }
            }
        }
    }

    Err(Box::new(std::io::Error::other("Unknown type".to_owned())))
}

/// Numeric discriminant used in the CBOR encoding of [`Node`] variants.
pub enum Kind {
    /// Transaction node discriminant.
    Transaction,
    /// Entry node discriminant.
    Entry,
    /// Block node discriminant.
    Block,
    /// Subset node discriminant.
    Subset,
    /// Epoch node discriminant.
    Epoch,
    /// Rewards node discriminant.
    Rewards,
    /// Data frame node discriminant.
    DataFrame,
}

impl fmt::Debug for Kind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Kind")
            .field("kind", &self.to_string())
            .finish()
    }
}

impl fmt::Display for Kind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let kind = match self {
            Kind::Transaction => "Transaction",
            Kind::Entry => "Entry",
            Kind::Block => "Block",
            Kind::Subset => "Subset",
            Kind::Epoch => "Epoch",
            Kind::Rewards => "Rewards",
            Kind::DataFrame => "DataFrame",
        };
        write!(f, "{}", kind)
    }
}

impl Kind {
    /// Converts a numeric discriminant into a [`Kind`].
    pub fn from_u64(kind: u64) -> Option<Kind> {
        match kind {
            0 => Some(Kind::Transaction),
            1 => Some(Kind::Entry),
            2 => Some(Kind::Block),
            3 => Some(Kind::Subset),
            4 => Some(Kind::Epoch),
            5 => Some(Kind::Rewards),
            6 => Some(Kind::DataFrame),
            _ => None,
        }
    }

    /// Returns the numeric discriminant for this [`Kind`].
    pub fn to_u64(&self) -> u64 {
        match self {
            Kind::Transaction => 0,
            Kind::Entry => 1,
            Kind::Block => 2,
            Kind::Subset => 3,
            Kind::Epoch => 4,
            Kind::Rewards => 5,
            Kind::DataFrame => 6,
        }
    }
}

/// Raw node extracted from an Old Faithful CAR segment.
pub struct RawNode {
    cid: Cid,
    data: Vec<u8>,
}

// Debug trait for RawNode
impl fmt::Debug for RawNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RawNode")
            .field("cid", &self.cid)
            .field("data", &self.data)
            .finish()
    }
}

impl RawNode {
    /// Creates a [`RawNode`] from the provided CID and raw bytes read from Old Faithful.
    pub fn new(cid: Cid, data: Vec<u8>) -> RawNode {
        RawNode { cid, data }
    }

    /// Parses the node into a typed [`Node`].
    pub fn parse(&self) -> Result<Node, Box<dyn Error>> {
        let parsed = parse_any_from_cbordata(self.data.clone());
        match parsed {
            Ok(node) => Ok(node),
            Err(_) => Err(Box::new(std::io::Error::other("Unknown type".to_owned()))),
        }
    }

    /// Decodes a [`RawNode`] from an Old Faithful CAR cursor.
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
            return Err(Box::new(std::io::Error::other(
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
            _ => Err(Box::new(std::io::Error::other(
                "Unknown CID version".to_owned(),
            ))),
        }
    }
}

/// Old Faithful CAR reader that produces [`RawNode`] values from a synchronous source.
pub struct NodeReader<R: Read> {
    /// Underlying reader yielding Old Faithful CAR bytes.
    reader: R,
    /// Cached Old Faithful CAR header.
    header: Vec<u8>,
    /// Number of Old Faithful items that have been read.
    item_index: u64,
}

impl<R: Read> NodeReader<R> {
    /// Creates a new [`NodeReader`] around a blocking reader.
    pub fn new(reader: R) -> Result<NodeReader<R>, Box<dyn Error>> {
        let node_reader = NodeReader {
            reader,
            header: vec![],
            item_index: 0,
        };
        Ok(node_reader)
    }

    /// Returns the raw Old Faithful CAR header, caching it for subsequent calls.
    pub fn read_raw_header(&mut self) -> Result<Vec<u8>, Box<dyn Error>> {
        if !self.header.is_empty() {
            return Ok(self.header.clone());
        };
        let header_length = utils::read_uvarint(&mut self.reader)?;
        if header_length > 1024 {
            return Err(Box::new(std::io::Error::other(
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
    /// Reads the next [`RawNode`] without parsing it from Old Faithful data.
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
            return Err(Box::new(std::io::Error::other(
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

    /// Reads and parses the next node, returning it with its [`Cid`].
    pub fn next_parsed(&mut self) -> Result<NodeWithCid, Box<dyn Error>> {
        let raw_node = self.next()?;
        let cid = raw_node.cid;
        Ok(NodeWithCid::new(cid, raw_node.parse()?))
    }

    /// Iterates Old Faithful nodes until a block is encountered, returning the collected list.
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

    /// Returns the number of CAR items read so far.
    pub fn get_item_index(&self) -> u64 {
        self.item_index
    }
}
