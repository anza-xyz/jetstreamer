use std::{fmt::Display, ops::Range};

use reqwest::Client;
use thiserror::Error;

use crate::slot_cache::{fetch_epoch_stream_async, SlotCache};

#[derive(Debug, Error)]
pub enum GeyserReplayError {
    Reqwest(reqwest::Error),
}

impl Display for GeyserReplayError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GeyserReplayError::Reqwest(e) => write!(f, "Reqwest error: {}", e),
        }
    }
}

impl From<reqwest::Error> for GeyserReplayError {
    fn from(e: reqwest::Error) -> Self {
        GeyserReplayError::Reqwest(e)
    }
}

pub async fn process_slot_range(
    slot_range: Range<u64>,
    epoch_range: Range<u64>,
    client: Client,
) -> Result<(), GeyserReplayError> {
    for (epoch_num, stream) in epoch_range
        .map(|epoch| fetch_epoch_stream_async(epoch, &client))
        .enumerate()
    {
        tracing::info!("Processing epoch {}", epoch_num);
        let stream = stream.await?;
    }
    Ok(())
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

#[test]
fn test_process_slot_range() {
    let client = reqwest::Client::new();
    let slot_range = 700..705;
    let epoch_range = 700..705;
    let result = process_slot_range(slot_range, epoch_range, client);
    assert!(result.is_ok());
}
