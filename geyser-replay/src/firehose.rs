use crossbeam_channel::{Receiver, Sender, unbounded};
use reqwest::{Client, Url};
use solana_geyser_plugin_manager::{
    block_metadata_notifier_interface::BlockMetadataNotifier,
    geyser_plugin_service::GeyserPluginServiceError,
};
use solana_ledger::entry_notifier_interface::EntryNotifier;
use solana_reward_info::RewardInfo;
use solana_rpc::{
    optimistically_confirmed_bank_tracker::SlotNotification,
    transaction_notifier_interface::TransactionNotifier,
};
use solana_runtime::bank::{KeyedRewardsAndNumPartitions, RewardType};
use solana_sdk::{hash::Hash, transaction::VersionedTransaction};
use solana_vote_program::id as vote_program_id;
use std::{
    fmt::Display,
    future::Future,
    ops::Range,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    },
};
use thiserror::Error;
use tokio::time::timeout;

use crate::{
    epochs::{epoch_to_slot_range, fetch_epoch_stream, slot_to_epoch},
    index::{SlotOffsetIndex, SlotOffsetIndexError, get_index_base_url},
    node_reader::NodeReader,
    utils,
};

// Timeout applied to each asynchronous firehose operation (fetching epoch stream, reading header,
// seeking, reading next block). Adjust here to tune stall detection/restart aggressiveness.
const OP_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

#[derive(Debug, Error)]
pub enum FirehoseError {
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
    OperationTimeout(&'static str),
    TransactionHandlerError(Box<dyn std::error::Error>),
    BlockHandlerError(Box<dyn std::error::Error>),
}

impl Display for FirehoseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FirehoseError::Reqwest(e) => write!(f, "Reqwest error: {}", e),
            FirehoseError::ReadHeader(error) => {
                write!(f, "Error reading header: {}", error)
            }
            FirehoseError::GeyserPluginService(geyser_plugin_service_error) => write!(
                f,
                "Error initializing geyser plugin service: {}",
                geyser_plugin_service_error
            ),
            FirehoseError::FailedToGetTransactionNotifier => write!(
                f,
                "Failed to get transaction notifier from GeyserPluginService"
            ),
            FirehoseError::ReadUntilBlockError(error) => {
                write!(f, "Error reading until block: {}", error)
            }
            FirehoseError::GetBlockError(error) => write!(f, "Error getting block: {}", error),
            FirehoseError::NodeDecodingError(item_index, error) => {
                write!(
                    f,
                    "Error seeking, reading data from, or decoding data for data node {}: {}",
                    item_index, error
                )
            }
            FirehoseError::SlotOffsetIndexError(slot_offset_index_error) => write!(
                f,
                "Error getting info from slot offset index: {}",
                slot_offset_index_error
            ),
            FirehoseError::SeekToSlotError(error) => {
                write!(f, "Error seeking to slot: {}", error)
            }
            FirehoseError::OnLoadError(error) => write!(f, "Error on load: {}", error),
            FirehoseError::OperationTimeout(op) => {
                write!(f, "Timeout while waiting for operation: {}", op)
            }
            FirehoseError::TransactionHandlerError(error) => {
                write!(f, "Transaction handler error: {}", error)
            }
            FirehoseError::BlockHandlerError(error) => {
                write!(f, "Block handler error: {}", error)
            }
        }
    }
}

impl From<reqwest::Error> for FirehoseError {
    fn from(e: reqwest::Error) -> Self {
        FirehoseError::Reqwest(e)
    }
}

impl From<GeyserPluginServiceError> for FirehoseError {
    fn from(e: GeyserPluginServiceError) -> Self {
        FirehoseError::GeyserPluginService(e)
    }
}

impl From<SlotOffsetIndexError> for FirehoseError {
    fn from(e: SlotOffsetIndexError) -> Self {
        FirehoseError::SlotOffsetIndexError(e)
    }
}

#[derive(Debug, Clone)]
pub struct TransactionData {
    pub slot: u64,
    pub transaction_slot_index: usize,
    pub signature: solana_sdk::signature::Signature,
    pub message_hash: Hash,
    pub is_vote: bool,
    pub transaction_status_meta: solana_transaction_status::TransactionStatusMeta,
    pub transaction: VersionedTransaction,
}

#[derive(Debug)]
pub enum BlockData {
    Block {
        parent_slot: u64,
        parent_blockhash: Hash,
        slot: u64,
        blockhash: Hash,
        rewards: KeyedRewardsAndNumPartitions,
        block_time: Option<i64>,
        block_height: Option<u64>,
        executed_transaction_count: u64,
        entry_count: u64,
    },
    LeaderSkipped {
        slot: u64,
    },
}

impl BlockData {
    #[inline(always)]
    pub const fn slot(&self) -> u64 {
        match self {
            BlockData::Block { slot, .. } => *slot,
            BlockData::LeaderSkipped { slot } => *slot,
        }
    }
}

#[inline]
pub async fn firehose<OnBlock, OnTransaction>(
    threads: u64,
    slot_range: Range<u64>,
    on_block: OnBlock,
    on_tx: OnTransaction,
) -> Result<(), (FirehoseError, u64)>
where
    OnBlock: Fn(usize, BlockData) -> Result<(), Box<dyn std::error::Error + Send + 'static>>
        + Send
        + Sync
        + Clone
        + 'static,
    OnTransaction: Fn(usize, TransactionData) -> Result<(), Box<dyn std::error::Error + Send + 'static>>
        + Send
        + Sync
        + Clone
        + 'static,
{
    if threads == 0 {
        return Err((
            FirehoseError::OnLoadError("Number of threads must be greater than 0".into()),
            slot_range.start,
        ));
    }
    let client = Client::new();
    let index_base_url = get_index_base_url()
        .map_err(|e| (FirehoseError::SlotOffsetIndexError(e), slot_range.start))?;
    log::info!("starting firehose...");
    log::info!("index base url: {}", index_base_url);

    let index_base_url = Arc::new(index_base_url);
    let slot_range = Arc::new(slot_range);

    // divide slot_range into n subranges
    let subranges = generate_subranges(&slot_range, threads);
    if threads > 1 {
        log::debug!("⚡ thread sub-ranges: {:?}", subranges);
    }

    let mut handles = Vec::new();
    // Shared per-thread error counters
    let error_counts: Arc<Vec<AtomicU32>> =
        Arc::new((0..subranges.len()).map(|_| AtomicU32::new(0)).collect());

    for (thread_index, mut slot_range) in subranges.into_iter().enumerate() {
        let index_base_url = index_base_url.clone();
        let error_counts = error_counts.clone();
        let client = client.clone();
        let on_block = on_block.clone();
        let on_tx = on_tx.clone();

        let handle = tokio::spawn(async move {
            let start_time = std::time::Instant::now();
            let log_target = format!("{}::T{:03}", module_path!(), thread_index);
            let mut skip_until_index = None;
            // let mut triggered = false;
            while let Err((err, slot)) = async {
                    let epoch_range = slot_to_epoch(slot_range.start)..=slot_to_epoch(slot_range.end - 1);
                    log::info!(
                        target: &log_target,
                        "slot range: {} (epoch {}) ... {} (epoch {})",
                        slot_range.start,
                        slot_to_epoch(slot_range.start),
                        slot_range.end,
                        slot_to_epoch(slot_range.end)
                    );

                    let mut slot_offset_index = SlotOffsetIndex::new((*index_base_url).clone())
                                    .map_err(|e| (FirehoseError::SlotOffsetIndexError(e), slot_range.start))?;

                    log::info!(target: &log_target, "🚒 starting firehose...");

                    // for each epoch
                    let mut current_slot: Option<u64> = None;
                    let mut previous_slot: Option<u64>;
                    for epoch_num in epoch_range.clone() {
                        log::info!(target: &log_target, "entering epoch {}", epoch_num);
                        let stream = match timeout(OP_TIMEOUT, fetch_epoch_stream(epoch_num, &client)).await {
                            Ok(stream) => stream,
                            Err(_) => {
                                return Err((FirehoseError::OperationTimeout("fetch_epoch_stream"), current_slot.unwrap_or(slot_range.start)));
                            }
                        };
                        let mut reader = NodeReader::new(stream);

                        let header_fut = reader.read_raw_header();
                        let header = match timeout(OP_TIMEOUT, header_fut).await {
                            Ok(res) => res
                                .map_err(FirehoseError::ReadHeader)
                                .map_err(|e| (e, current_slot.unwrap_or(slot_range.start)))?,
                            Err(_) => {
                                return Err((FirehoseError::OperationTimeout("read_raw_header"), current_slot.unwrap_or(slot_range.start)));
                            }
                        };
                        log::debug!(target: &log_target, "read epoch {} header: {:?}", epoch_num, header);

                        let mut todo_previous_blockhash = Hash::default();
                        let mut todo_latest_entry_blockhash = Hash::default();

                        if slot_range.start > epoch_to_slot_range(epoch_num).0 {
                            let seek_fut = reader.seek_to_slot(slot_range.start, &mut slot_offset_index);
                            match timeout(OP_TIMEOUT, seek_fut).await {
                                Ok(res) => res.map_err(|e| (e, current_slot.unwrap_or(slot_range.start)))?,
                                Err(_) => {
                                    return Err((FirehoseError::OperationTimeout("seek_to_slot"), current_slot.unwrap_or(slot_range.start)));
                                }
                            }
                        }

                        // for each item in each block
                        let mut item_index = 0;
                        let mut displayed_skip_message = false;
                        loop {
                            let read_fut = reader.read_until_block();
                            let nodes = match timeout(OP_TIMEOUT, read_fut).await {
                                Ok(result) => result
                                    .map_err(FirehoseError::ReadUntilBlockError)
                                    .map_err(|e| (e, current_slot.unwrap_or(slot_range.start)))?,
                                Err(_) => {
                                    log::warn!(target: &log_target, "timeout reading next block, retrying (will restart)...");
                                    return Err((FirehoseError::OperationTimeout("read_until_block"), current_slot.unwrap_or(slot_range.start)));
                                }
                            };
                            if let Some(last_node) = nodes.0.last()
                                && !last_node.get_node().is_block() {
                                    log::info!(target: &log_target, "reached end of epoch {}", epoch_num);
                                    break;
                                }
                            let block = nodes
                                .get_block()
                                .map_err(FirehoseError::GetBlockError)
                                .map_err(|e| (e, current_slot.unwrap_or(slot_range.start)))?;
                            log::debug!(
                                target: &log_target,
                                "read {} items from epoch {}, now at slot {}",
                                item_index,
                                epoch_num,
                                block.slot
                            );
                            let slot = block.slot;
                            if slot >= slot_range.end {
                                log::info!(target: &log_target, "reached end of slot range at slot {}", slot);
                                // Return early to terminate the firehose thread cleanly.
                                // We use >= because slot_range is half-open [start, end), so any slot
                                // equal to end is out-of-range and must not be processed.
                                return Ok(());
                            }
                            debug_assert!(slot < slot_range.end, "processing out-of-range slot {} (end {})", slot, slot_range.end);
                            if slot < slot_range.start {
                                log::warn!(
                                    target: &log_target,
                                    "encountered slot {} before start of range {}, skipping",
                                    slot,
                                    slot_range.start
                                );
                                continue;
                            }
                            previous_slot = current_slot;
                            current_slot = Some(slot);
                            let mut this_block_executed_transaction_count: u64 = 0;
                            let mut this_block_entry_count: u64 = 0;
                            let mut this_block_rewards: solana_storage_proto::convert::generated::Rewards =
                                solana_storage_proto::convert::generated::Rewards::default();

                            nodes
                        .each(|node_with_cid| -> Result<(), Box<dyn std::error::Error>> {
                            item_index += 1;
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
                                    let versioned_tx = tx.as_parsed()?;
                                    let reassembled_metadata =
                                        nodes.reassemble_dataframes(tx.metadata.clone())?;

                                    let decompressed = utils::decompress_zstd(reassembled_metadata.clone())?;

                                        let metadata: solana_storage_proto::convert::generated::TransactionStatusMeta =
                                            prost_011::Message::decode(decompressed.as_slice()).map_err(|err| {
                                                Box::new(std::io::Error::other(
                                                    std::format!("Error decoding metadata: {:?}", err),
                                                ))
                                            })?;

                                    let as_native_metadata: solana_transaction_status::TransactionStatusMeta =
                                        metadata.try_into()?;

                                    let message_hash = {
                                        #[cfg(feature = "verify-transaction-signatures")]
                                        {
                                            versioned_tx.verify_and_hash_message()?
                                        }
                                        #[cfg(not(feature = "verify-transaction-signatures"))]
                                        {
                                            // Signature verification is optional because it is extremely expensive at replay scale.
                                            versioned_tx.message.hash()
                                        }
                                    };
                                    let signature = versioned_tx
                                        .signatures
                                        .first()
                                        .ok_or_else(|| {
                                            Box::new(std::io::Error::new(
                                                std::io::ErrorKind::InvalidData,
                                                "transaction missing signature",
                                            )) as Box<dyn std::error::Error>
                                        })?;
                                    let is_vote = is_simple_vote_transaction(&versioned_tx);

                                    on_tx(
                                        thread_index,
                                        TransactionData {
                                            slot: block.slot,
                                            transaction_slot_index: tx.index.unwrap() as usize,
                                            signature: *signature,
                                            message_hash,
                                            is_vote,
                                            transaction_status_meta: as_native_metadata.clone(),
                                            transaction: versioned_tx.clone(),
                                        },
                                    ).map_err(|e| FirehoseError::TransactionHandlerError(e))?;
                                }
                                Entry(entry) => {
                                    todo_latest_entry_blockhash = Hash::from(entry.hash.to_bytes());
                                    let entry_transaction_count = entry.transactions.len() as u64;
                                    this_block_executed_transaction_count = this_block_executed_transaction_count
                                        .checked_add(entry_transaction_count)
                                        .ok_or_else(|| {
                                            Box::new(std::io::Error::other(
                                                "transaction count overflow",
                                            )) as Box<dyn std::error::Error>
                                        })?;
                                    this_block_entry_count += 1;
                                },
                                Block(block) => {
                                    let mut keyed_rewards = Vec::with_capacity(this_block_rewards.rewards.len());
                                    {
                                        // convert this_block_rewards to rewards
                                        for this_block_reward in this_block_rewards.rewards.iter() {
                                            let reward: RewardInfo = RewardInfo {
                                                reward_type: match this_block_reward.reward_type - 1 {
                                                    // -1 because of protobuf
                                                    0 => RewardType::Fee,
                                                    1 => RewardType::Rent,
                                                    2 => RewardType::Staking,
                                                    3 => RewardType::Voting,
                                                    typ => panic!("___ not supported reward type {}", typ),
                                                },
                                                lamports: this_block_reward.lamports,
                                                post_balance: this_block_reward.post_balance,
                                                // commission is Option<u8>, but this_block_reward.commission is string
                                                commission: this_block_reward.commission.parse::<u8>().ok(),
                                            };
                                            keyed_rewards.push((this_block_reward.pubkey.parse()?, reward));
                                        }
                                    }
                                    if let Some(previous_slot) = previous_slot {
                                        for skipped_slot in (previous_slot + 1)..slot {
                                            log::debug!(
                                                target: &log_target,
                                                "leader skipped slot {} (previous slot {}, current slot {})",
                                                skipped_slot,
                                                previous_slot,
                                                slot,
                                            );
                                            on_block(
                                                thread_index,
                                                BlockData::LeaderSkipped { slot: skipped_slot },
                                            )
                                            .map_err(|e| FirehoseError::BlockHandlerError(e))?;
                                        }
                                    }
                                    on_block(
                                        thread_index,
                                        BlockData::Block {
                                            parent_slot: block.meta.parent_slot,
                                            parent_blockhash: todo_previous_blockhash,
                                            slot: block.slot,
                                            blockhash: todo_latest_entry_blockhash,
                                            rewards: KeyedRewardsAndNumPartitions {
                                                keyed_rewards,
                                                num_partitions: None,
                                            },
                                            block_time: Some(block.meta.blocktime as i64),
                                            block_height: block.meta.block_height,
                                            executed_transaction_count: this_block_executed_transaction_count,
                                            entry_count: this_block_entry_count,
                                        },
                                    )
                                    .map_err(|e| FirehoseError::BlockHandlerError(e))?;
                                    todo_previous_blockhash = todo_latest_entry_blockhash;
                                    std::thread::yield_now();
                                },
                                Subset(_subset) => (),
                                Epoch(_epoch) => (),
                                Rewards(rewards) => {
                                    if !rewards.is_complete() {
                                        let reassembled = nodes.reassemble_dataframes(rewards.data.clone())?;

                                        let decompressed = utils::decompress_zstd(reassembled)?;

                                        this_block_rewards = prost_011::Message::decode(decompressed.as_slice()).map_err(|err| {
                                            Box::new(std::io::Error::other(
                                                std::format!("Error decoding rewards: {:?}", err),
                                            ))
                                        })?;
                                    }
                                },
                                DataFrame(_data_frame) => (),
                            }
                            Ok(())
                        })
                        .map_err(|e| FirehoseError::NodeDecodingError(item_index, e)).map_err(|e| (e, current_slot.unwrap_or(slot_range.start)))?;
                            if block.slot == slot_range.end - 1 {
                                let finish_time = std::time::Instant::now();
                                let elapsed = finish_time.duration_since(start_time);
                                log::info!(target: &log_target, "processed slot {}", block.slot);
                                log::info!(
                                    target: &log_target,
                                    "processed {} slots across {} epochs in {} seconds.",
                                    slot_range.end - slot_range.start,
                                    slot_to_epoch(slot_range.end) + 1 - slot_to_epoch(slot_range.start),
                                    elapsed.as_secs_f32()
                                );
                                log::info!(target: &log_target, "a 🚒 firehose thread finished completed its work.");
                                // On completion, report threads with non-zero error counts for visibility.
                                let summary: String = error_counts
                                    .iter()
                                    .enumerate()
                                    .filter_map(|(i, c)| {
                                        let v = c.load(Ordering::Relaxed);
                                        if v > 0 { Some(format!("{:03}({})", i, v)) } else { None }
                                    })
                                    .collect::<Vec<_>>()
                                    .join(", ");
                                if !summary.is_empty() {
                                    log::debug!(target: &log_target, "threads with errors: {}", summary);
                                }
                                return Ok(());
                            }
                        }
                    }
                    Ok(())
            }
            .await
            {
                    log::error!(
                        target: &log_target,
                        "🔥🔥🔥 firehose encountered an error at slot {} in epoch {}:",
                        slot,
                        slot_to_epoch(slot)
                    );
                    log::error!(target: &log_target, "{}", err);
                    let item_index = match err {
                        FirehoseError::NodeDecodingError(item_index, _) => item_index,
                        _ => 0,
                    };
                    // Increment this thread's error counter
                    error_counts[thread_index].fetch_add(1, Ordering::Relaxed);
                    log::warn!(
                        target: &log_target,
                        "restarting from slot {} at index {}",
                        slot,
                        item_index,
                    );
                    // Update slot range to resume from the failed slot, not the original start
                    slot_range.start = slot;
                    skip_until_index = Some(item_index);
            }
        });
        handles.push(handle);
    }

    // Wait for all threads to complete
    for handle in handles {
        handle.await.unwrap();
    }
    log::info!("🚒 firehose finished successfully.");
    Ok(())
}

pub fn firehose_geyser(
    rt: Arc<tokio::runtime::Runtime>,
    slot_range: Range<u64>,
    geyser_config_files: Option<&[PathBuf]>,
    index_base_url: &Url,
    client: &Client,
    on_load: impl Future<Output = Result<(), Box<dyn std::error::Error + Send + 'static>>>
    + Send
    + 'static,
    threads: u64,
) -> Result<Receiver<SlotNotification>, (FirehoseError, u64)> {
    if threads == 0 {
        return Err((
            FirehoseError::OnLoadError("Number of threads must be greater than 0".into()),
            slot_range.start,
        ));
    }
    log::info!("starting firehose...");
    log::info!("index base url: {}", index_base_url);
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
                .ok_or(FirehoseError::FailedToGetTransactionNotifier)
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
    rt.spawn(on_load);

    let index_base_url = Arc::new(index_base_url.clone());
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

    let mut handles = Vec::new();
    // Shared per-thread error counters
    let error_counts: Arc<Vec<AtomicU32>> =
        Arc::new((0..subranges.len()).map(|_| AtomicU32::new(0)).collect());

    for (i, slot_range) in subranges.into_iter().enumerate() {
        let transaction_notifier_maybe = (*transaction_notifier_maybe).clone();
        let entry_notifier_maybe = (*entry_notifier_maybe).clone();
        let block_meta_notifier_maybe = (*block_meta_notifier_maybe).clone();
        let confirmed_bank_sender = (*confirmed_bank_sender).clone();
        let index_base_url = index_base_url.clone();
        let client = client.clone();
        let error_counts = error_counts.clone();

        let rt_clone = rt.clone();

        let handle = std::thread::spawn(move || {
            rt_clone.block_on(async {
                firehose_geyser_thread(
                    slot_range,
                    (*index_base_url).clone(),
                    transaction_notifier_maybe,
                    entry_notifier_maybe,
                    block_meta_notifier_maybe,
                    confirmed_bank_sender,
                    &client,
                    if threads > 1 { Some(i) } else { None },
                    error_counts,
                )
                .await
                .unwrap();
            });
        });
        handles.push(handle);
    }

    // Wait for all threads to complete
    for handle in handles {
        handle.join().unwrap();
    }
    log::info!("🚒 firehose finished successfully.");
    if let Some(block_meta_notifier) = block_meta_notifier_maybe.as_ref() {
        block_meta_notifier.notify_block_metadata(
            u64::MAX,
            "unload",
            u64::MAX,
            "unload",
            &KeyedRewardsAndNumPartitions {
                keyed_rewards: vec![],
                num_partitions: None,
            },
            None,
            None,
            0,
            0,
        );
    }
    Ok(confirmed_bank_receiver)
}

#[allow(clippy::too_many_arguments)]
async fn firehose_geyser_thread(
    mut slot_range: Range<u64>,
    index_base_url: Url,
    transaction_notifier_maybe: Option<Arc<dyn TransactionNotifier + Send + Sync + 'static>>,
    entry_notifier_maybe: Option<Arc<dyn EntryNotifier + Send + Sync + 'static>>,
    block_meta_notifier_maybe: Option<Arc<dyn BlockMetadataNotifier + Send + Sync + 'static>>,
    confirmed_bank_sender: Sender<SlotNotification>,
    client: &Client,
    thread_index: Option<usize>,
    error_counts: Arc<Vec<AtomicU32>>,
) -> Result<(), (FirehoseError, u64)> {
    let start_time = std::time::Instant::now();
    let log_target = if let Some(thread_index) = thread_index {
        format!("{}::T{:03}", module_path!(), thread_index)
    } else {
        module_path!().to_string()
    };
    let mut skip_until_index = None;
    // let mut triggered = false;
    while let Err((err, slot)) = async {
            let epoch_range = slot_to_epoch(slot_range.start)..=slot_to_epoch(slot_range.end - 1);
            log::info!(
                target: &log_target,
                "slot range: {} (epoch {}) ... {} (epoch {})",
                slot_range.start,
                slot_to_epoch(slot_range.start),
                slot_range.end,
                slot_to_epoch(slot_range.end)
            );

            let mut slot_offset_index = SlotOffsetIndex::new(index_base_url.clone())
                .map_err(|e| (FirehoseError::SlotOffsetIndexError(e), slot_range.start))?;

            log::info!(target: &log_target, "🚒 starting firehose...");

            // for each epoch
            let mut current_slot: Option<u64> = None;
            let mut previous_slot: Option<u64> = None;
            for epoch_num in epoch_range.clone() {
                log::info!(target: &log_target, "entering epoch {}", epoch_num);
                let stream = match timeout(OP_TIMEOUT, fetch_epoch_stream(epoch_num, client)).await {
                    Ok(stream) => stream,
                    Err(_) => {
                        return Err((FirehoseError::OperationTimeout("fetch_epoch_stream"), current_slot.unwrap_or(slot_range.start)));
                    }
                };
                let mut reader = NodeReader::new(stream);

                let header_fut = reader.read_raw_header();
                let header = match timeout(OP_TIMEOUT, header_fut).await {
                    Ok(res) => res
                        .map_err(FirehoseError::ReadHeader)
                        .map_err(|e| (e, current_slot.unwrap_or(slot_range.start)))?,
                    Err(_) => {
                        return Err((FirehoseError::OperationTimeout("read_raw_header"), current_slot.unwrap_or(slot_range.start)));
                    }
                };
                log::debug!(target: &log_target, "read epoch {} header: {:?}", epoch_num, header);

                let mut todo_previous_blockhash = Hash::default();
                let mut todo_latest_entry_blockhash = Hash::default();

                if slot_range.start > epoch_to_slot_range(epoch_num).0 {
                    let seek_fut = reader.seek_to_slot(slot_range.start, &mut slot_offset_index);
                    match timeout(OP_TIMEOUT, seek_fut).await {
                        Ok(res) => res.map_err(|e| (e, current_slot.unwrap_or(slot_range.start)))?,
                        Err(_) => {
                            return Err((FirehoseError::OperationTimeout("seek_to_slot"), current_slot.unwrap_or(slot_range.start)));
                        }
                    }
                }

                // for each item in each block
                let mut item_index = 0;
                let mut displayed_skip_message = false;
                loop {
                    let read_fut = reader.read_until_block();
                    let nodes = match timeout(OP_TIMEOUT, read_fut).await {
                        Ok(result) => result
                            .map_err(FirehoseError::ReadUntilBlockError)
                            .map_err(|e| (e, current_slot.unwrap_or(slot_range.start)))?,
                        Err(_) => {
                            log::warn!(target: &log_target, "timeout reading next block, retrying (will restart)...");
                            return Err((FirehoseError::OperationTimeout("read_until_block"), current_slot.unwrap_or(slot_range.start)));
                        }
                    };
                    // ignore epoch and subset nodes at end of car file
                    // loop {
                    //     if nodes.0.is_empty() {
                    //         break;
                    //     }
                    //     if let Some(node) = nodes.0.last() {
                    //         if node.get_node().is_epoch() {
                    //             log::debug!(target: &log_target, "skipping epoch node for epoch {}", epoch_num);
                    //             nodes.0.pop();
                    //         } else if node.get_node().is_subset() {
                    //             nodes.0.pop();
                    //         } else if node.get_node().is_block() {
                    //             break;
                    //         }
                    //     }
                    // }
                    // if nodes.0.is_empty() {
                    //     log::info!(target: &log_target, "reached end of epoch {}", epoch_num);
                    //     break;
                    // }
                    if let Some(last_node) = nodes.0.last()
                        && !last_node.get_node().is_block() {
                            log::info!(target: &log_target, "reached end of epoch {}", epoch_num);
                            break;
                        }
                    let block = nodes
                        .get_block()
                        .map_err(FirehoseError::GetBlockError)
                        .map_err(|e| (e, current_slot.unwrap_or(slot_range.start)))?;
                    log::debug!(
                        target: &log_target,
                        "read {} items from epoch {}, now at slot {}",
                        item_index,
                        epoch_num,
                        block.slot
                    );
                    let slot = block.slot;
                    if slot >= slot_range.end {
                        log::info!(target: &log_target, "reached end of slot range at slot {}", slot);
                        // Return early to terminate the firehose thread cleanly.
                        // We use >= because slot_range is half-open [start, end), so any slot
                        // equal to end is out-of-range and must not be processed.
                        return Ok(());
                    }
                    debug_assert!(slot < slot_range.end, "processing out-of-range slot {} (end {})", slot, slot_range.end);
                    if slot < slot_range.start {
                        log::warn!(
                            target: &log_target,
                            "encountered slot {} before start of range {}, skipping",
                            slot,
                            slot_range.start
                        );
                        continue;
                    }
                    if let Some(previous_slot) = previous_slot
                        && slot != previous_slot + 1 {
                            // log::warn!(target: &log_target, "non-consecutive slots: {} followed by {}", previous_slot, slot);
                        }
                    previous_slot = current_slot;
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
						let versioned_tx = tx.as_parsed()?;
						let reassembled_metadata =
							nodes.reassemble_dataframes(tx.metadata.clone())?;

						let decompressed = utils::decompress_zstd(reassembled_metadata.clone())?;

                            let metadata: solana_storage_proto::convert::generated::TransactionStatusMeta =
                                prost_011::Message::decode(decompressed.as_slice()).map_err(|err| {
                                    Box::new(std::io::Error::other(
                                        std::format!("Error decoding metadata: {:?}", err),
                                    ))
                                })?;

						let as_native_metadata: solana_transaction_status::TransactionStatusMeta =
							metadata.try_into()?;

						let message_hash = {
							#[cfg(feature = "verify-transaction-signatures")]
							{
								versioned_tx.verify_and_hash_message()?
							}
							#[cfg(not(feature = "verify-transaction-signatures"))]
							{
								// Signature verification is optional because it is extremely expensive at replay scale.
								versioned_tx.message.hash()
							}
						};
						let signature = versioned_tx
							.signatures
							.first()
							.ok_or_else(|| {
								Box::new(std::io::Error::new(
									std::io::ErrorKind::InvalidData,
									"transaction missing signature",
								)) as Box<dyn std::error::Error>
							})?;
						let is_vote = is_simple_vote_transaction(&versioned_tx);

						if let Some(transaction_notifier) = transaction_notifier_maybe.as_ref() {
							transaction_notifier.notify_transaction(
								block.slot,
								tx.index.unwrap() as usize,
								signature,
								&message_hash,
								is_vote,
								&as_native_metadata,
								&versioned_tx,
							);
						}
                        }
                        Entry(entry) => {
							todo_latest_entry_blockhash = Hash::from(entry.hash.to_bytes());
							let entry_transaction_count = entry.transactions.len() as u64;
							let starting_transaction_index = usize::try_from(
								this_block_executed_transaction_count,
							)
							.map_err(|_| {
								Box::new(std::io::Error::other(
									"transaction index exceeds usize range",
								)) as Box<dyn std::error::Error>
							})?;
							this_block_executed_transaction_count = this_block_executed_transaction_count
								.checked_add(entry_transaction_count)
								.ok_or_else(|| {
									Box::new(std::io::Error::other(
										"transaction count overflow",
									)) as Box<dyn std::error::Error>
								})?;
							this_block_entry_count += 1;
							if entry_notifier_maybe.is_none() {
								return Ok(());
							}
							let entry_notifier = entry_notifier_maybe.as_ref().unwrap();
							let entry_summary = solana_entry::entry::EntrySummary {
								num_hashes: entry.num_hashes,
								hash: Hash::from(entry.hash.to_bytes()),
								num_transactions: entry_transaction_count,
							};
							entry_notifier
								.notify_entry(block.slot, entry_index, &entry_summary, starting_transaction_index);
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
											commission: this_block_reward.commission.parse::<u8>().ok(),
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
                            std::thread::yield_now();
						},
                        Subset(_subset) => (),
                        Epoch(_epoch) => (),
                        Rewards(rewards) => {
							if !rewards.is_complete() {
								let reassembled = nodes.reassemble_dataframes(rewards.data.clone())?;

								let decompressed = utils::decompress_zstd(reassembled)?;

								this_block_rewards = prost_011::Message::decode(decompressed.as_slice()).map_err(|err| {
									Box::new(std::io::Error::other(
										std::format!("Error decoding rewards: {:?}", err),
									))
								})?;
							}
						},
                        DataFrame(_data_frame) => (),
                    }
                    Ok(())
                })
                .map_err(|e| FirehoseError::NodeDecodingError(item_index, e)).map_err(|e| (e, current_slot.unwrap_or(slot_range.start)))?;
                    if block.slot == slot_range.end - 1 {
                        let finish_time = std::time::Instant::now();
                        let elapsed = finish_time.duration_since(start_time);
                        log::info!(target: &log_target, "processed slot {}", block.slot);
                        log::info!(
                            target: &log_target,
                            "processed {} slots across {} epochs in {} seconds.",
                            slot_range.end - slot_range.start,
                            slot_to_epoch(slot_range.end) + 1 - slot_to_epoch(slot_range.start),
                            elapsed.as_secs_f32()
                        );
                        log::info!(target: &log_target, "a 🚒 firehose thread finished completed its work.");
                        // On completion, report threads with non-zero error counts for visibility.
                        let summary: String = error_counts
                            .iter()
                            .enumerate()
                            .filter_map(|(i, c)| {
                                let v = c.load(Ordering::Relaxed);
                                if v > 0 { Some(format!("{:03}({})", i, v)) } else { None }
                            })
                            .collect::<Vec<_>>()
                            .join(", ");
                        if !summary.is_empty() {
                            log::debug!(target: &log_target, "threads with errors: {}", summary);
                        }
                        return Ok(());
                    }
                }
            }
            Ok(())
    }
    .await
    {
            log::error!(
                target: &log_target,
                "🔥🔥🔥 firehose encountered an error at slot {} in epoch {}:",
                slot,
                slot_to_epoch(slot)
            );
            log::error!(target: &log_target, "{}", err);
            let item_index = match err {
                FirehoseError::NodeDecodingError(item_index, _) => item_index,
                _ => 0,
            };
            // Increment this thread's error counter
            let idx = thread_index.unwrap_or(0);
            error_counts[idx].fetch_add(1, Ordering::Relaxed);
            log::warn!(
                target: &log_target,
                "restarting from slot {} at index {}",
                slot,
                item_index,
            );
            // Update slot range to resume from the failed slot, not the original start
            slot_range.start = slot;
            skip_until_index = Some(item_index);
    }
    Ok(())
}

#[inline]
fn is_simple_vote_transaction(versioned_tx: &VersionedTransaction) -> bool {
    if !(1..=2).contains(&versioned_tx.signatures.len()) {
        return false;
    }

    if !matches!(
        versioned_tx.version(),
        solana_sdk::transaction::TransactionVersion::Legacy(_)
    ) {
        return false;
    }

    let instructions = versioned_tx.message.instructions();
    if instructions.len() != 1 {
        return false;
    }

    let program_index = instructions[0].program_id_index as usize;
    versioned_tx
        .message
        .static_account_keys()
        .get(program_index)
        .map(|program_id| program_id == &vote_program_id())
        .unwrap_or(false)
}

#[inline]
pub fn generate_subranges(slot_range: &Range<u64>, threads: u64) -> Vec<Range<u64>> {
    let total = slot_range.end - slot_range.start;
    let slots_per_thread = total / threads;
    let remainder = total % threads;

    let ranges: Vec<Range<u64>> = (0..threads)
        .map(|i| {
            // Distribute remainder slots to the first `remainder` threads
            let extra_slot = if i < remainder { 1 } else { 0 };
            let start = slot_range.start + i * slots_per_thread + i.min(remainder);
            let end = start + slots_per_thread + extra_slot;
            start..end
        })
        .collect();

    // Verify that ranges cover all slots exactly
    let total_covered: u64 = ranges.iter().map(|r| r.end - r.start).sum();
    assert_eq!(
        total_covered, total,
        "Range generation failed: {} threads should cover {} slots but only cover {}",
        threads, total, total_covered
    );

    // Verify no gaps between ranges
    for i in 1..ranges.len() {
        assert_eq!(
            ranges[i - 1].end,
            ranges[i].start,
            "Gap found between thread {} (ends at {}) and thread {} (starts at {})",
            i - 1,
            ranges[i - 1].end,
            i,
            ranges[i].start
        );
    }

    log::info!(
        "Generated {} thread ranges covering {} slots total",
        threads,
        total_covered
    );
    ranges
}

#[tokio::test(flavor = "multi_thread")]
async fn test_firehose_epoch_800() {
    use std::sync::atomic::{AtomicU64, Ordering};
    solana_logger::setup_with_default("info");
    const THREADS: usize = 228;
    static PREV_BLOCK: [AtomicU64; THREADS] = [const { AtomicU64::new(0) }; THREADS];
    firehose(
        THREADS.try_into().unwrap(),
        345600000..(345600000 + 10000),
        |thread_id, block: BlockData| {
            let prev =
                PREV_BLOCK[thread_id % PREV_BLOCK.len()].swap(block.slot(), Ordering::Relaxed);
            log::info!("got block {} on thread {}", block.slot(), thread_id,);
            if prev > 0 {
                assert_eq!(prev + 1, block.slot());
            }
            Ok(())
        },
        |_thread_id, _tx: TransactionData| Ok(()),
    )
    .await
    .unwrap();
}
