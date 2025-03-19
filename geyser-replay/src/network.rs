use std::{
    collections::HashSet, fmt::Display, ops::Range, path::{Path, PathBuf}
};

use crossbeam_channel::unbounded;
use demo_rust_ipld_car::utils;
use reqwest::Client;
use solana_geyser_plugin_manager::geyser_plugin_service::GeyserPluginServiceError;
use thiserror::Error;

use crate::{node_reader::AsyncNodeReader, slot_cache::fetch_epoch_stream_async};

#[derive(Debug, Error)]
pub enum GeyserReplayError {
    Reqwest(reqwest::Error),
    ReadHeader(Box<dyn std::error::Error>),
    GeyserPluginService(GeyserPluginServiceError),
    FailedToGetTransactionNotifier,
    ReadUntilBlockError(Box<dyn std::error::Error>),
    GetBlockError(Box<dyn std::error::Error>),
    NodeSeekError(Box<dyn std::error::Error>),
}

impl Display for GeyserReplayError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GeyserReplayError::Reqwest(e) => write!(f, "Reqwest error: {}", e),
            GeyserReplayError::ReadHeader(error) => {
                write!(f, "Error reading header: {}", error)
            }
            GeyserReplayError::GeyserPluginService(geyser_plugin_service_error) => write!(
                f,
                "Error initializing geyser plugin service: {}",
                geyser_plugin_service_error
            ),
            GeyserReplayError::FailedToGetTransactionNotifier => write!(
                f,
                "Failed to get transaction notifier from GeyserPluginService"
            ),
            GeyserReplayError::ReadUntilBlockError(error) => {
                write!(f, "Error reading until block: {}", error)
            }
            GeyserReplayError::GetBlockError(error) => write!(f, "Error getting block: {}", error),
            GeyserReplayError::NodeSeekError(error) => {
                write!(
                    f,
                    "Error seeking to or reading data from next node: {}",
                    error
                )
            }
        }
    }
}

impl From<reqwest::Error> for GeyserReplayError {
    fn from(e: reqwest::Error) -> Self {
        GeyserReplayError::Reqwest(e)
    }
}

impl From<GeyserPluginServiceError> for GeyserReplayError {
    fn from(e: GeyserPluginServiceError) -> Self {
        GeyserReplayError::GeyserPluginService(e)
    }
}

pub async fn process_slot_range(
    slot_range: Range<u64>,
    epoch_range: Range<u64>,
    geyser_config_files: &[PathBuf],
    client: Client,
) -> Result<(), GeyserReplayError> {
    for (epoch_num, stream) in epoch_range
        .map(|epoch| fetch_epoch_stream_async(epoch, &client))
        .enumerate()
    {
        tracing::info!("Processing epoch {}", epoch_num);
        let stream = stream.await?;
        let mut reader = AsyncNodeReader::new(stream);

        let header = reader
            .read_raw_header()
            .await
            .map_err(|e| GeyserReplayError::ReadHeader(e))?;
        tracing::debug!("read epoch {} header: {:?}", epoch_num, header);

        tracing::debug!("Geyser config files: {:?}", geyser_config_files);

        let (confirmed_bank_sender, confirmed_bank_receiver) = unbounded();

        let service =
            solana_geyser_plugin_manager::geyser_plugin_service::GeyserPluginService::new(
                confirmed_bank_receiver,
                geyser_config_files,
            )?;
        tracing::debug!("Geyser plugin service initialized.");

        let transaction_notifier = service
            .get_transaction_notifier()
            .ok_or(GeyserReplayError::FailedToGetTransactionNotifier)?;

        let entry_notifier_maybe = service.get_entry_notifier();
        if entry_notifier_maybe.is_some() {
            tracing::debug!("Entry notifications enabled")
        } else {
            tracing::debug!("None of the plugins have enabled entry notifications")
        }

        let block_meta_notifier_maybe = service.get_block_metadata_notifier();

        let mut todo_previous_blockhash = solana_sdk::hash::Hash::default();
        let mut todo_latest_entry_blockhash = solana_sdk::hash::Hash::default();

        let mut item_index = 0;
        loop {
            let nodes = reader
                .read_until_block()
                .await
                .map_err(|e| GeyserReplayError::ReadUntilBlockError(e))?;
            let block = nodes
                .get_block()
                .map_err(|e| GeyserReplayError::GetBlockError(e))?;
            tracing::debug!(
                "read block {} of epoch {} with slot {}",
                item_index,
                epoch_num,
                block.slot
            );
            if !slot_range.contains(&block.slot) {
                tracing::debug!("skipping slot {}", block.slot);
                continue;
            }
            let mut entry_index: usize = 0;
            let mut this_block_executed_transaction_count: u64 = 0;
            let mut this_block_entry_count: u64 = 0;
            let mut this_block_rewards: solana_storage_proto::convert::generated::Rewards =
                solana_storage_proto::convert::generated::Rewards::default();

            nodes
                .each(|node_with_cid| -> Result<(), Box<dyn std::error::Error>> {
                    item_index += 1;
                    let node = node_with_cid.get_node();

                    use demo_rust_ipld_car::node::Node::*;
                    match node {
                        Transaction(tx) => {
                            let parsed = tx.as_parsed()?;
                            let reassembled_metadata =
                                nodes.reassemble_dataframes(tx.metadata.clone())?;

                            let decompressed = utils::decompress_zstd(reassembled_metadata.clone())?;

                            let metadata: solana_storage_proto::convert::generated::TransactionStatusMeta =
                                prost_011::Message::decode(decompressed.as_slice()).map_err(|err| {
                                    Box::new(std::io::Error::new(
                                        std::io::ErrorKind::Other,
                                        std::format!("Error decoding metadata: {:?}", err),
                                    ))
                                })?;

							let as_native_metadata: solana_transaction_status::TransactionStatusMeta =
								metadata.try_into()?;
							
							let dummy_address_loader = MessageAddressLoaderFromTxMeta::new(as_native_metadata.clone());

							let sanitized_tx = match  parsed.version() {
								solana_sdk::transaction::TransactionVersion::Number(_)=> {
									let message_hash = parsed.verify_and_hash_message()?;
									let versioned_sanitized_tx= solana_sdk::transaction::SanitizedVersionedTransaction::try_from(parsed)?;
									solana_sdk::transaction::SanitizedTransaction::try_new(
										versioned_sanitized_tx,
										message_hash,
										false,
										dummy_address_loader,
										&HashSet::default(),
									)
								},
								solana_sdk::transaction::TransactionVersion::Legacy(_legacy)=> {
									solana_sdk::transaction::SanitizedTransaction::try_from_legacy_transaction(
										parsed.into_legacy_transaction().unwrap(),
										&HashSet::default(),
									)
								},
							}?;

							transaction_notifier
							.notify_transaction(
								block.slot,
								tx.index.unwrap() as usize,
								sanitized_tx.signature(),
								&as_native_metadata,
								&sanitized_tx,
							);
                        }
                        Entry(entry) => todo!(),
                        Block(block) => todo!(),
                        Subset(subset) => todo!(),
                        Epoch(epoch) => todo!(),
                        Rewards(rewards) => todo!(),
                        DataFrame(data_frame) => todo!(),
                    }
                    Ok(())
                })
                .map_err(|e| GeyserReplayError::NodeSeekError(e))?;
        }
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

#[tokio::test]
async fn test_process_slot_range() {
    // let client = reqwest::Client::new();
    // let slot_range = 700..705;
    // let epoch_range = 700..705;
    // let result = process_slot_range(slot_range, epoch_range, client).await;
    // assert!(result.is_ok());
}
