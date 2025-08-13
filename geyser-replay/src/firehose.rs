use crossbeam_channel::{Receiver, Sender, unbounded};
use demo_rust_ipld_car::utils;
use rayon::prelude::*;
use reqwest::Client;
use solana_geyser_plugin_manager::{
    block_metadata_notifier_interface::BlockMetadataNotifier,
    geyser_plugin_service::GeyserPluginServiceError,
};
use solana_ledger::entry_notifier_interface::EntryNotifier;
use solana_rpc::{
    optimistically_confirmed_bank_tracker::SlotNotification,
    transaction_notifier_interface::TransactionNotifier,
};
use solana_runtime::bank::KeyedRewardsAndNumPartitions;
use solana_sdk::{reward_info::RewardInfo, reward_type::RewardType};
use std::{
    collections::HashSet,
    fmt::Display,
    future::Future,
    ops::Range,
    path::{Path, PathBuf},
    sync::Arc,
};
use thiserror::Error;

use crate::{
    epochs::{epoch_to_slot_range, fetch_epoch_stream, slot_to_epoch},
    index::{SlotOffsetIndex, SlotOffsetIndexError},
    node_reader::NodeReader,
};

#[derive(Debug, Error)]
pub enum GeyserReplayError {
    Reqwest(reqwest::Error),
    ReadHeader(Box<dyn std::error::Error>),
    GeyserPluginService(GeyserPluginServiceError),
    FailedToGetTransactionNotifier,
    ReadUntilBlockError(Box<dyn std::error::Error>),
    GetBlockError(Box<dyn std::error::Error>),
    NodeDecodingError(usize, Box<dyn std::error::Error>),
    SlotOffsetIndexError(SlotOffsetIndexError),
    SeekToSlotError(Box<dyn std::error::Error>),
    OnLoadError(Box<dyn std::error::Error>),
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
            GeyserReplayError::NodeDecodingError(item_index, error) => {
                write!(
                    f,
                    "Error seeking, reading data from, or decoding data for data node {}: {}",
                    item_index, error
                )
            }
            GeyserReplayError::SlotOffsetIndexError(slot_offset_index_error) => write!(
                f,
                "Error getting info from slot offset index: {}",
                slot_offset_index_error
            ),
            GeyserReplayError::SeekToSlotError(error) => {
                write!(f, "Error seeking to slot: {}", error)
            }
            GeyserReplayError::OnLoadError(error) => write!(f, "Error on load: {}", error),
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

impl From<SlotOffsetIndexError> for GeyserReplayError {
    fn from(e: SlotOffsetIndexError) -> Self {
        GeyserReplayError::SlotOffsetIndexError(e)
    }
}

pub async fn firehose(
    slot_range: Range<u64>,
    geyser_config_files: Option<&[PathBuf]>,
    slot_offset_index_path: impl AsRef<Path>,
    client: &Client,
    on_load: impl Future<Output = Result<(), Box<dyn std::error::Error + Send + 'static>>>
    + Send
    + 'static,
    threads: u8,
) -> Result<Receiver<SlotNotification>, (GeyserReplayError, u64)> {
    if threads == 0 {
        return Err((
            GeyserReplayError::OnLoadError("Number of threads must be greater than 0".into()),
            slot_range.start,
        ));
    }
    let _ = rayon::ThreadPoolBuilder::new()
        .num_threads(threads as usize)
        .build_global();
    log::info!("starting firehose...");
    let (confirmed_bank_sender, confirmed_bank_receiver) = unbounded();
    let mut entry_notifier_maybe = None;
    let mut block_meta_notifier_maybe = None;
    let mut transaction_notifier_maybe = None;
    if let Some(geyser_config_files) = geyser_config_files {
        log::debug!("geyser config files: {:?}", geyser_config_files);

        let service =
            solana_geyser_plugin_manager::geyser_plugin_service::GeyserPluginService::new(
                confirmed_bank_receiver.clone(),
                true,
                geyser_config_files,
            )
            .map_err(|e| (e.into(), slot_range.start))?;

        transaction_notifier_maybe = Some(
            service
                .get_transaction_notifier()
                .ok_or(GeyserReplayError::FailedToGetTransactionNotifier)
                .map_err(|e| (e, slot_range.start))?,
        );

        entry_notifier_maybe = service.get_entry_notifier();
        block_meta_notifier_maybe = service.get_block_metadata_notifier();

        log::debug!("geyser plugin service initialized.");
    }

    if entry_notifier_maybe.is_some() {
        log::debug!("entry notifications enabled")
    } else {
        log::debug!("none of the plugins have enabled entry notifications")
    }
    log::info!("running on_load...");
    tokio::task::spawn(on_load);

    let slot_offset_index_path = Arc::new(slot_offset_index_path.as_ref().to_owned());
    let slot_range = Arc::new(slot_range);
    let transaction_notifier_maybe = Arc::new(transaction_notifier_maybe);
    let entry_notifier_maybe = Arc::new(entry_notifier_maybe);
    let block_meta_notifier_maybe = Arc::new(block_meta_notifier_maybe);
    let confirmed_bank_sender = Arc::new(confirmed_bank_sender);

    // divide slot_range into n subranges
    let subranges = generate_subranges(&slot_range, threads);
    if threads > 1 {
        log::info!("⚡ thread sub-ranges: {:?}", subranges);
    }

    subranges
        .into_iter()
        .enumerate()
        .par_bridge()
        .for_each(|(i, slot_range)| {
            let transaction_notifier_maybe = (*transaction_notifier_maybe).clone();
            let entry_notifier_maybe = (*entry_notifier_maybe).clone();
            let block_meta_notifier_maybe = (*block_meta_notifier_maybe).clone();
            let confirmed_bank_sender = (*confirmed_bank_sender).clone();

            let slot_offset_index_path = slot_offset_index_path.as_ref().to_owned();
            tokio::runtime::Runtime::new().unwrap().block_on(async {
                firehose_thread(
                    slot_range,
                    slot_offset_index_path,
                    transaction_notifier_maybe,
                    entry_notifier_maybe,
                    block_meta_notifier_maybe,
                    confirmed_bank_sender,
                    client,
                    if threads > 1 { Some(i) } else { None },
                )
                .await
                .unwrap();
            });
        });
    log::info!("🚒 firehose finished successfully.");
    Ok(confirmed_bank_receiver)
}

async fn firehose_thread(
    slot_range: Range<u64>,
    slot_offset_index_path: impl AsRef<Path>,
    transaction_notifier_maybe: Option<Arc<dyn TransactionNotifier + Send + Sync + 'static>>,
    entry_notifier_maybe: Option<Arc<dyn EntryNotifier + Send + Sync + 'static>>,
    block_meta_notifier_maybe: Option<Arc<dyn BlockMetadataNotifier + Send + Sync + 'static>>,
    confirmed_bank_sender: Sender<SlotNotification>,
    client: &Client,
    thread_index: Option<usize>,
) -> Result<(), (GeyserReplayError, u64)> {
    let start_time = std::time::Instant::now();
    let log_target = if let Some(thread_index) = thread_index {
        format!("{}::{}", module_path!(), thread_index)
    } else {
        module_path!().to_string()
    };
    let mut skip_until_index = None;
    // let mut triggered = false;
    loop {
        if let Err((err, slot)) = async {
            let epoch_range = slot_to_epoch(slot_range.start)..=slot_to_epoch(slot_range.end);
            log::info!(
                target: &log_target,
                "slot range: {} (epoch {}) ... {} (epoch {})",
                slot_range.start,
                slot_to_epoch(slot_range.start),
                slot_range.end,
                slot_to_epoch(slot_range.end)
            );

            let mut slot_offset_index = SlotOffsetIndex::new(slot_offset_index_path.as_ref())
                            .map_err(|e| (GeyserReplayError::SlotOffsetIndexError(e), slot_range.start))?;

            log::info!(target: &log_target, "🚒 starting firehose...");

            // for each epoch
            let mut current_slot: Option<u64> = None;
            'epoch_loop: for (epoch_num, stream) in
                epoch_range.map(|epoch| (epoch, fetch_epoch_stream(epoch, client)))
            {
                log::info!(target: &log_target, "entering epoch {}", epoch_num);
                let stream = stream.await;
                let mut reader = NodeReader::new(stream);

                let header = reader
                    .read_raw_header()
                    .await
                    .map_err(GeyserReplayError::ReadHeader)
                    .map_err(|e| (e, current_slot.unwrap_or(slot_range.start)))?;
                log::debug!(target: &log_target, "read epoch {} header: {:?}", epoch_num, header);

                let mut todo_previous_blockhash = solana_sdk::hash::Hash::default();
                let mut todo_latest_entry_blockhash = solana_sdk::hash::Hash::default();

                if slot_range.start > epoch_to_slot_range(epoch_num).0 {
                    reader
                        .seek_to_slot(slot_range.start, &mut slot_offset_index)
                        .await
                        .map_err(|e| (e, current_slot.unwrap_or(slot_range.start)))?;
                }

                // for each item in each block
                let mut item_index = 0;
                let mut displayed_skip_message = false;
                loop {
                    let mut nodes = reader
                        .read_until_block()
                        .await
                        .map_err(GeyserReplayError::ReadUntilBlockError)
                        .map_err(|e| (e, current_slot.unwrap_or(slot_range.start)))?;
                    // ignore epoch and subset nodes at end of car file
                    loop {
                        if nodes.0.is_empty() {
                            break;
                        }
                        if let Some(node) = nodes.0.last() {
                            if node.get_node().is_epoch() {
                                log::debug!("skipping epoch node for epoch {}", epoch_num);
                                nodes.0.pop();
                            } else if node.get_node().is_subset() {
                                nodes.0.pop();
                            } else if node.get_node().is_block() {
                                break;
                            }
                        }
                    }
                    if nodes.0.is_empty() {
                        log::info!("reached end of epoch {}", epoch_num);
                        break;
                    }
                    let block = nodes
                        .get_block()
                        .map_err(GeyserReplayError::GetBlockError)
                        .map_err(|e| (e, current_slot.unwrap_or(slot_range.start)))?;
                    log::debug!(
                        "read {} items from epoch {}, now at slot {}",
                        item_index,
                        epoch_num,
                        block.slot
                    );
                    let slot = block.slot;
                    if slot >= slot_range.end {
                        log::info!("reached end of slot range at slot {}", slot);
                        break 'epoch_loop;
                    }
                    if slot < slot_range.start {
                        log::warn!(
                            "encountered slot {} before start of range {}, skipping",
                            slot,
                            slot_range.start
                        );
                        continue;
                    }
                    current_slot = Some(slot);
                    let mut entry_index: usize = 0;
                    let mut this_block_executed_transaction_count: u64 = 0;
                    let mut this_block_entry_count: u64 = 0;
                    let mut this_block_rewards: solana_storage_proto::convert::generated::Rewards =
                        solana_storage_proto::convert::generated::Rewards::default();

                    nodes
                .each(|node_with_cid| -> Result<(), Box<dyn std::error::Error>> {
                    item_index += 1;
                    // if item_index == 100000 && !triggered {
                    //     log::info!("simulating error");
                    //     triggered = true;
                    //     return Err(Box::new(GeyserReplayError::NodeDecodingError(item_index, 
                    //         Box::new(std::io::Error::new(
                    //             std::io::ErrorKind::Other,
                    //             "simulated error",
                    //         )),
                    //     )));
                    // }
                    if let Some(skip) = skip_until_index {
                        if item_index < skip {
                            if !displayed_skip_message {
                                log::info!("skipping until index {} (at {})", skip, item_index);
                                displayed_skip_message = true;
                            }
                            return Ok(());
                        } else {
                            log::info!("reached target index {}, resuming...", skip);
                            skip_until_index = None;
                        }
                    }
                    let node = node_with_cid.get_node();

                    use crate::node::Node::*;
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

							if let Some(transaction_notifier) = transaction_notifier_maybe.as_ref() {
								transaction_notifier
								.notify_transaction(
									block.slot,
									tx.index.unwrap() as usize,
									sanitized_tx.signature(),
									&as_native_metadata,
									&sanitized_tx,
								);
							}
                        }
                        Entry(entry) => {
							todo_latest_entry_blockhash = solana_sdk::hash::Hash::from(entry.hash.to_bytes());
							this_block_executed_transaction_count += entry.transactions.len() as u64;
							this_block_entry_count += 1;
							if entry_notifier_maybe.is_none() {
								return Ok(());
							}
							let entry_notifier = entry_notifier_maybe.as_ref().unwrap();
							let entry_summary = solana_entry::entry::EntrySummary {
								num_hashes: entry.num_hashes,
								hash: solana_sdk::hash::Hash::from(entry.hash.to_bytes()),
								num_transactions: entry.transactions.len() as u64,
							};

							let starting_transaction_index = 0; // TODO:: implement this
							entry_notifier
								.notify_entry(block.slot, entry_index  ,&entry_summary, starting_transaction_index);
							entry_index += 1;
						},
                        Block(block) => {
							let notification = SlotNotification::Root((block.slot, block.meta.parent_slot));
							confirmed_bank_sender.send(notification).unwrap();

							{
								if block_meta_notifier_maybe.is_none() {
									return Ok(());
								}
								let mut keyed_rewards = Vec::with_capacity(this_block_rewards.rewards.len());
								{
									// convert this_block_rewards to rewards
									for this_block_reward in this_block_rewards.rewards.iter() {
										let reward: RewardInfo = RewardInfo{
											reward_type: match this_block_reward.reward_type  - 1 { // -1 because of protobuf
												0 => RewardType::Fee,
												1 => RewardType::Rent,
												2 => RewardType::Staking,
												3 => RewardType::Voting,
												typ => panic!("___ not supported reward type {}", typ),
											},
											lamports: this_block_reward.lamports,
											post_balance: this_block_reward.post_balance,
											// commission is Option<u8> , but this_block_reward.commission is string
											commission: match this_block_reward.commission.parse::<u8>() {
												Ok(commission) => Some(commission),
												Err(_err) => None,
											},
										};
										keyed_rewards.push((this_block_reward.pubkey.parse()?, reward));
									}
								}
								// if keyed_rewards.read().unwrap().len() > 0 {
								//   panic!("___ Rewards: {:?}", keyed_rewards.read().unwrap());
								// }
								let block_meta_notifier = block_meta_notifier_maybe.as_ref().unwrap();
								block_meta_notifier
									.notify_block_metadata(
										block.meta.parent_slot,
										todo_previous_blockhash.to_string().as_str(),
										block.slot,
										todo_latest_entry_blockhash.to_string().as_str(),
										&KeyedRewardsAndNumPartitions {
											keyed_rewards,
											num_partitions: None
										},
										Some(block.meta.blocktime as i64) ,
										block.meta.block_height,
										this_block_executed_transaction_count,
										this_block_entry_count,
									);
							}
							todo_previous_blockhash = todo_latest_entry_blockhash;
						},
                        Subset(_subset) => (),
                        Epoch(_epoch) => (),
                        Rewards(rewards) => {
							if !rewards.is_complete() {
								let reassembled = nodes.reassemble_dataframes(rewards.data.clone())?;

								let decompressed = utils::decompress_zstd(reassembled)?;

								this_block_rewards = prost_011::Message::decode(decompressed.as_slice()).map_err(|err| {
									Box::new(std::io::Error::new(
										std::io::ErrorKind::Other,
										std::format!("Error decoding rewards: {:?}", err),
									))
								})?;
							}
						},
                        DataFrame(_data_frame) => (),
                    }
                    Ok(())
                })
                .map_err(|e| GeyserReplayError::NodeDecodingError(item_index, e)).map_err(|e| (e, current_slot.unwrap_or(slot_range.start)))?;
                    if block.slot == slot_range.end - 1 {
                        let finish_time = std::time::Instant::now();
                        let elapsed = finish_time.duration_since(start_time);
                        log::info!("processed slot {}", block.slot);
                        log::info!(
                            "processed {} slots across {} epochs in {} seconds.",
                            slot_range.end - slot_range.start,
                            slot_to_epoch(slot_range.end) + 1 - slot_to_epoch(slot_range.start),
                            elapsed.as_secs_f32()
                        );
                        log::info!("a 🚒 firehose thread finished completed its work.");
                        break 'epoch_loop;
                    }
                }
            }
            Ok(())
        }.await {
            log::error!(
                "🔥🔥🔥 firehose encountered an error at slot {} in epoch {}:",
                slot,
                slot_to_epoch(slot)
            );
            log::error!("{}", err);
            let item_index = match err {
                GeyserReplayError::NodeDecodingError(item_index, _) => item_index,
                _ => 0,
            };
            log::warn!(
                "restarting from slot {} at index {}",
                slot,
                item_index,
            );
            skip_until_index = Some(item_index);
        } else {
            break;
        }
    }
    Ok(())
}

pub fn generate_subranges(slot_range: &Range<u64>, threads: u8) -> Vec<Range<u64>> {
    let threads = threads as u64;
    let total = slot_range.end - slot_range.start;

    (0..threads)
        .map(|i| {
            let start = slot_range.start + total * i / threads;
            let end = if i == threads - 1 {
                slot_range.end
            } else {
                slot_range.start + total * (i + 1) / threads
            };
            start..end
        })
        .collect::<Vec<_>>()
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
