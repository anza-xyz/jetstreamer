use {geyser_replay::firehose::firehose, reqwest::Client, std::env::args};

#[tokio::main(worker_threads = 32)]
async fn main() {
    let client = Client::new();
    let slot_range = args().nth(1).expect("no slot range given");
    let (slot_a, slot_b) = slot_range
        .split_once(':')
        .expect("failed to parse slot range, expected format: <start>:<end>");
    let slot_a: u64 = slot_a.parse().expect("failed to parse first slot");
    let slot_b: u64 = slot_b.parse().expect("failed to parse second slot");
    let slot_range = slot_a..(slot_b + 1);
    let geyser_config_files = &[std::path::PathBuf::from(args().nth(2).unwrap())];
    println!("Geyser config files: {:?}", geyser_config_files);
    firehose(slot_range, Some(geyser_config_files), &client)
        .await
        .unwrap();
}

pub struct MessageAddressLoaderFromTxMeta {
    pub tx_meta: solana_transaction_status::TransactionStatusMeta,
}

impl MessageAddressLoaderFromTxMeta {
    pub fn new(tx_meta: solana_transaction_status::TransactionStatusMeta) -> Self {
        MessageAddressLoaderFromTxMeta { tx_meta }
    }
}

impl solana_sdk::message::AddressLoader for MessageAddressLoaderFromTxMeta {
    fn load_addresses(
        self,
        _lookups: &[solana_sdk::message::v0::MessageAddressTableLookup],
    ) -> Result<solana_sdk::message::v0::LoadedAddresses, solana_sdk::message::AddressLoaderError>
    {
        Ok(self.tx_meta.loaded_addresses.clone())
    }
}

// implement clone for MessageAddressLoaderFromTxMeta
impl Clone for MessageAddressLoaderFromTxMeta {
    fn clone(&self) -> Self {
        MessageAddressLoaderFromTxMeta {
            tx_meta: self.tx_meta.clone(),
        }
    }
}
