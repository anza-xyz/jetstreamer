use crossbeam_channel::{Receiver, Sender, unbounded};
#[cfg(test)]
use futures_util::FutureExt;
use futures_util::future::BoxFuture;
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
use solana_sdk::{hash::Hash, pubkey::Pubkey, transaction::VersionedTransaction};
use solana_vote_program::id as vote_program_id;
use std::{
    fmt::Display,
    future::Future,
    io,
    ops::Range,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering},
    },
};
use thiserror::Error;
use tokio::{
    sync::broadcast::{self, error::TryRecvError},
    time::timeout,
};

use crate::{
    epochs::{epoch_to_slot_range, fetch_epoch_stream, slot_to_epoch},
    index::{SLOT_OFFSET_INDEX, SlotOffsetIndexError},
    node_reader::NodeReader,
    utils,
};

// Timeout applied to each asynchronous firehose operation (fetching epoch stream, reading header,
// seeking, reading next block). Adjust here to tune stall detection/restart aggressiveness.
const OP_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
const LOG_MODULE: &str = "jetstreamer::firehose";

fn poll_shutdown(
    flag: &Arc<std::sync::atomic::AtomicBool>,
    receiver: &mut Option<broadcast::Receiver<()>>,
) -> bool {
    if let Some(rx) = receiver {
        match rx.try_recv() {
            Ok(_) | Err(TryRecvError::Lagged(_)) => {
                flag.store(true, Ordering::SeqCst);
            }
            Err(TryRecvError::Closed) => {
                flag.store(true, Ordering::SeqCst);
            }
            Err(TryRecvError::Empty) => {}
        }
    }
    flag.load(Ordering::SeqCst)
}

fn is_shutdown_error(err: &FirehoseError) -> bool {
    fn is_interrupted(inner: &(dyn std::error::Error + 'static)) -> bool {
        inner
            .downcast_ref::<io::Error>()
            .map(|io_err| io_err.kind() == io::ErrorKind::Interrupted)
            .unwrap_or(false)
    }

    match err {
        FirehoseError::BlockHandlerError(inner)
        | FirehoseError::TransactionHandlerError(inner)
        | FirehoseError::EntryHandlerError(inner)
        | FirehoseError::RewardHandlerError(inner)
        | FirehoseError::OnStatsHandlerError(inner) => is_interrupted(inner.as_ref()),
        _ => false,
    }
}

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
    OnStatsHandlerError(Box<dyn std::error::Error>),
    OperationTimeout(&'static str),
    TransactionHandlerError(Box<dyn std::error::Error>),
    EntryHandlerError(Box<dyn std::error::Error>),
    RewardHandlerError(Box<dyn std::error::Error>),
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
            FirehoseError::OnStatsHandlerError(error) => {
                write!(f, "Stats handler error: {}", error)
            }
            FirehoseError::OperationTimeout(op) => {
                write!(f, "Timeout while waiting for operation: {}", op)
            }
            FirehoseError::TransactionHandlerError(error) => {
                write!(f, "Transaction handler error: {}", error)
            }
            FirehoseError::EntryHandlerError(error) => {
                write!(f, "Entry handler error: {}", error)
            }
            FirehoseError::RewardHandlerError(error) => {
                write!(f, "Reward handler error: {}", error)
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

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct ThreadStats {
    pub thread_id: usize,
    pub start_time: std::time::Instant,
    pub finish_time: Option<std::time::Instant>,
    pub slot_range: Range<u64>,
    pub current_slot: u64,
    pub slots_processed: u64,
    pub blocks_processed: u64,
    pub leader_skipped_slots: u64,
    pub transactions_processed: u64,
    pub entries_processed: u64,
    pub rewards_processed: u64,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct Stats {
    pub thread_stats: ThreadStats,
    pub start_time: std::time::Instant,
    pub finish_time: Option<std::time::Instant>,
    pub slot_range: Range<u64>,
    pub slots_processed: u64,
    pub blocks_processed: u64,
    pub leader_skipped_slots: u64,
    pub transactions_processed: u64,
    pub entries_processed: u64,
    pub rewards_processed: u64,
    pub transactions_since_last_pulse: u64,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct StatsTracking<OnStats: Handler<Stats>> {
    pub on_stats: OnStats,
    pub tracking_interval_slots: u64,
}

#[inline(always)]
async fn maybe_emit_stats<OnStats: Handler<Stats>>(
    stats_tracking: Option<&StatsTracking<OnStats>>,
    thread_index: usize,
    thread_stats: &ThreadStats,
    overall_slots_processed: &AtomicU64,
    overall_blocks_processed: &AtomicU64,
    overall_transactions_processed: &AtomicU64,
    overall_entries_processed: &AtomicU64,
    transactions_since_stats: &AtomicU64,
) -> Result<(), (FirehoseError, u64)> {
    if let Some(stats_tracker) = stats_tracking {
        let total_slots = overall_slots_processed.load(Ordering::Relaxed);
        let total_blocks = overall_blocks_processed.load(Ordering::Relaxed);
        let total_transactions = overall_transactions_processed.load(Ordering::Relaxed);
        let total_entries = overall_entries_processed.load(Ordering::Relaxed);
        let processed_transactions = transactions_since_stats.swap(0, Ordering::Relaxed);

        let stats = Stats {
            thread_stats: thread_stats.clone(),
            start_time: thread_stats.start_time,
            finish_time: thread_stats.finish_time,
            slot_range: thread_stats.slot_range.clone(),
            slots_processed: total_slots,
            blocks_processed: total_blocks,
            leader_skipped_slots: total_slots.saturating_sub(total_blocks),
            transactions_processed: total_transactions,
            entries_processed: total_entries,
            rewards_processed: thread_stats.rewards_processed,
            transactions_since_last_pulse: processed_transactions,
        };

        (stats_tracker.on_stats)(thread_index, stats)
            .await
            .map_err(|e| {
                (
                    FirehoseError::OnStatsHandlerError(e),
                    thread_stats.current_slot,
                )
            })?;
    }
    Ok(())
}

#[inline(always)]
fn fetch_add_if(tracking_enabled: bool, atomic: &AtomicU64, value: u64) {
    if tracking_enabled {
        atomic.fetch_add(value, Ordering::Relaxed);
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

#[derive(Debug, Clone)]
pub struct EntryData {
    pub slot: u64,
    pub entry_index: usize,
    pub transaction_indexes: Range<usize>,
    pub num_hashes: u64,
    pub hash: Hash,
}

#[derive(Debug, Clone)]
pub struct RewardsData {
    pub slot: u64,
    pub rewards: Vec<(Pubkey, RewardInfo)>,
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

    #[inline(always)]
    pub const fn was_skipped(&self) -> bool {
        matches!(self, BlockData::LeaderSkipped { .. })
    }
}

type HandlerResult = Result<(), Box<dyn std::error::Error + Send + 'static>>;
type HandlerFuture = BoxFuture<'static, HandlerResult>;

pub trait Handler<Data>: Fn(usize, Data) -> HandlerFuture + Send + Sync + Clone + 'static {}

impl<Data, F> Handler<Data> for F where
    F: Fn(usize, Data) -> HandlerFuture + Send + Sync + Clone + 'static
{
}

pub type HandlerFn<Data> = fn(usize, Data) -> HandlerFuture;
pub type OnBlockFn = HandlerFn<BlockData>;
pub type OnTxFn = HandlerFn<TransactionData>;
pub type OnEntryFn = HandlerFn<EntryData>;
pub type OnRewardFn = HandlerFn<RewardsData>;
pub type StatsTracker = StatsTracking<HandlerFn<Stats>>;

#[inline]
pub async fn firehose<OnBlock, OnTransaction, OnEntry, OnRewards, OnStats>(
    threads: u64,
    slot_range: Range<u64>,
    on_block: Option<OnBlock>,
    on_tx: Option<OnTransaction>,
    on_entry: Option<OnEntry>,
    on_rewards: Option<OnRewards>,
    stats_tracking: Option<StatsTracking<OnStats>>,
    shutdown_signal: Option<broadcast::Receiver<()>>,
) -> Result<(), (FirehoseError, u64)>
where
    OnBlock: Handler<BlockData>,
    OnTransaction: Handler<TransactionData>,
    OnEntry: Handler<EntryData>,
    OnRewards: Handler<RewardsData>,
    OnStats: Handler<Stats>,
{
    if threads == 0 {
        return Err((
            FirehoseError::OnLoadError("Number of threads must be greater than 0".into()),
            slot_range.start,
        ));
    }
    let client = Client::new();
    log::info!(target: LOG_MODULE, "starting firehose...");
    log::info!(target: LOG_MODULE, "index base url: {}", SLOT_OFFSET_INDEX.base_url());

    let slot_range = Arc::new(slot_range);

    // divide slot_range into n subranges
    let subranges = generate_subranges(&slot_range, threads);
    if threads > 1 {
        log::debug!(target: LOG_MODULE, "⚡ thread sub-ranges: {:?}", subranges);
    }

    let firehose_start = std::time::Instant::now();
    let shutdown_flag = Arc::new(AtomicBool::new(false));
    if let Some(ref rx) = shutdown_signal {
        let mut rx = rx.resubscribe();
        let flag = shutdown_flag.clone();
        tokio::spawn(async move {
            if rx.recv().await.is_ok() {
                log::info!(target: LOG_MODULE, "shutdown signal received; notifying firehose threads");
                flag.store(true, Ordering::SeqCst);
            }
        });
    }
    let mut handles = Vec::new();
    // Shared per-thread error counters
    let error_counts: Arc<Vec<AtomicU32>> =
        Arc::new((0..subranges.len()).map(|_| AtomicU32::new(0)).collect());

    let overall_slots_processed: Arc<AtomicU64> = Arc::new(AtomicU64::new(0));
    let overall_blocks_processed: Arc<AtomicU64> = Arc::new(AtomicU64::new(0));
    let overall_transactions_processed: Arc<AtomicU64> = Arc::new(AtomicU64::new(0));
    let overall_entries_processed: Arc<AtomicU64> = Arc::new(AtomicU64::new(0));

    for (thread_index, mut slot_range) in subranges.into_iter().enumerate() {
        let error_counts = error_counts.clone();
        let client = client.clone();
        let on_block = on_block.clone();
        let on_tx = on_tx.clone();
        let on_entry = on_entry.clone();
        let on_reward = on_rewards.clone();
        let overall_slots_processed = overall_slots_processed.clone();
        let overall_blocks_processed = overall_blocks_processed.clone();
        let overall_transactions_processed = overall_transactions_processed.clone();
        let overall_entries_processed = overall_entries_processed.clone();
        let stats_tracking = stats_tracking.clone();
        let transactions_since_stats = Arc::new(AtomicU64::new(0));
        let transactions_since_stats_cloned = transactions_since_stats.clone();
        let shutdown_flag = shutdown_flag.clone();
        let thread_shutdown_rx = shutdown_signal.as_ref().map(|rx| rx.resubscribe());

        let handle = tokio::spawn(async move {
            let transactions_since_stats = transactions_since_stats_cloned;
            let mut shutdown_rx = thread_shutdown_rx;
            let start_time = std::time::Instant::now();
            let log_target = format!("{}::T{:03}", LOG_MODULE, thread_index);
            let mut skip_until_index = None;
            let block_enabled = on_block.is_some();
            let tx_enabled = on_tx.is_some();
            let entry_enabled = on_entry.is_some();
            let reward_enabled = on_reward.is_some();
            let tracking_enabled = stats_tracking.is_some();

            // let mut triggered = false;
            while let Err((err, slot)) = async {
                if poll_shutdown(&shutdown_flag, &mut shutdown_rx) {
                    log::info!(
                        target: &log_target,
                        "shutdown requested; terminating firehose thread {}",
                        thread_index
                    );
                    return Ok(());
                }
                let epoch_range = slot_to_epoch(slot_range.start)..=slot_to_epoch(slot_range.end - 1);
                log::info!(
                    target: &log_target,
                    "slot range: {} (epoch {}) ... {} (epoch {})",
                    slot_range.start,
                    slot_to_epoch(slot_range.start),
                    slot_range.end,
                    slot_to_epoch(slot_range.end)
                );

                log::info!(target: &log_target, "🚒 starting firehose...");

                // for each epoch
                let mut current_slot: Option<u64> = None;
                let mut previous_slot: Option<u64> = Some(slot_range.start.saturating_sub(1));
                for epoch_num in epoch_range.clone() {
                    if poll_shutdown(&shutdown_flag, &mut shutdown_rx) {
                        log::info!(
                            target: &log_target,
                            "shutdown requested; terminating firehose thread {}",
                            thread_index
                        );
                        return Ok(());
                    }
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

                    let mut previous_blockhash = Hash::default();
                    let mut latest_entry_blockhash = Hash::default();

                    let mut thread_stats = if tracking_enabled {
                        Some(ThreadStats {
                            thread_id: thread_index,
                            start_time,
                            finish_time: None,
                            slot_range: slot_range.clone(),
                            current_slot: slot_range.start,
                            slots_processed: 0,
                            blocks_processed: 0,
                            leader_skipped_slots: 0,
                            transactions_processed: 0,
                            entries_processed: 0,
                            rewards_processed: 0,
                        })
                    } else {
                        None
                    };

                    if slot_range.start > epoch_to_slot_range(epoch_num).0 {
                        let seek_fut = reader.seek_to_slot(slot_range.start);
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
                        if poll_shutdown(&shutdown_flag, &mut shutdown_rx) {
                            log::info!(
                                target: &log_target,
                                "shutdown requested; terminating firehose thread {}",
                                thread_index
                            );
                            return Ok(());
                        }
                        let read_fut = reader.read_until_block();
                        let nodes = match timeout(OP_TIMEOUT, read_fut).await {
                            Ok(result) => result
                                .map_err(FirehoseError::ReadUntilBlockError)
                                .map_err(|e| (e, current_slot.unwrap_or(slot_range.start)))?,
                            Err(_) => {
                                log::warn!(target: &log_target, "timeout reading next block, retrying (will restart)...");
                                return Err((FirehoseError::OperationTimeout("read_until_block"), current_slot.map(|s| s + 1).unwrap_or(slot_range.start)));
                            }
                        };
                        if nodes.is_empty() {
                            log::info!(
                                target: &log_target,
                                "reached end of epoch {}",
                                epoch_num
                            );
                            break;
                        }
                        if let Some(last_node) = nodes.0.last()
                            && !last_node.get_node().is_block()
                        {
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
                        let mut slot = block.slot;
                        if slot >= slot_range.end {
                            log::info!(target: &log_target, "reached end of slot range at slot {}", slot);
                            // Return early to terminate the firehose thread cleanly.
                            // We use >= because slot_range is half-open [start, end), so any slot
                            // equal to end is out-of-range and must not be processed.

                            // still need to emit skipped slots up to end-1
                            slot = slot_range.end;
                            if let (Some(on_block_cb), Some(previous_slot)) =
                                (on_block.as_ref(), previous_slot)
                            {
                                for skipped_slot in (previous_slot + 2)..slot {
                                    log::debug!(
                                        target: &log_target,
                                        "leader skipped slot {} (previous slot {}, current slot {})",
                                        skipped_slot,
                                        previous_slot,
                                        slot,
                                    );
                                    if block_enabled {
                                        on_block_cb(
                                            thread_index,
                                            BlockData::LeaderSkipped { slot: skipped_slot },
                                        )
                                        .await
                                        .map_err(|e| FirehoseError::BlockHandlerError(e))
                                        .map_err(|e| (e, current_slot.unwrap_or(slot_range.start)))?;
                                    }
                                    if let Some(ref mut stats) = thread_stats {
                                        stats.leader_skipped_slots += 1;
                                        stats.slots_processed += 1;
                                        stats.current_slot = skipped_slot;
                                    }
                                    fetch_add_if(tracking_enabled, &overall_slots_processed, 1);
                                }
                            }
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
                        if current_slot.is_some() {
                            previous_slot = current_slot;
                        }
                        current_slot = Some(slot);
                        let mut entry_index: usize = 0;
                        let mut this_block_executed_transaction_count: u64 = 0;
                        let mut this_block_entry_count: u64 = 0;
                        let mut this_block_rewards: Vec<(Pubkey, RewardInfo)> = Vec::new();

                        for node_with_cid in &nodes.0 {
                            item_index += 1;
                            if let Some(skip) = skip_until_index {
                                if item_index < skip {
                                    if !displayed_skip_message {
                                        log::info!(
                                            target: &log_target,
                                            "skipping until index {} (at {})",
                                            skip,
                                            item_index
                                        );
                                        displayed_skip_message = true;
                                    }
                                    continue;
                                } else {
                                    log::info!(
                                        target: &log_target,
                                        "reached target index {}, resuming...",
                                        skip
                                    );
                                    skip_until_index = None;
                                }
                            }
                            let node = node_with_cid.get_node();

                            if let Some(ref mut stats) = thread_stats {
                                stats.current_slot = slot;
                            }

                            let error_slot = current_slot.unwrap_or(slot_range.start);

                            use crate::node::Node::*;
                            match node {
                                Transaction(tx) => {
                                    if tx_enabled
                                        && let Some(on_tx_cb) = on_tx.as_ref()
                                    {
                                        let error_slot = current_slot.unwrap_or(slot_range.start);
                                        let versioned_tx = tx.as_parsed().map_err(|err| {
                                            (
                                                FirehoseError::NodeDecodingError(item_index, err),
                                                error_slot,
                                            )
                                        })?;
                                        let reassembled_metadata = nodes
                                            .reassemble_dataframes(tx.metadata.clone())
                                            .map_err(|err| {
                                                (
                                                    FirehoseError::NodeDecodingError(item_index, err),
                                                    error_slot,
                                                )
                                            })?;

                                        let decompressed =
                                            utils::decompress_zstd(reassembled_metadata.clone())
                                                .map_err(|err| {
                                                    (
                                                        FirehoseError::NodeDecodingError(
                                                            item_index,
                                                            err,
                                                        ),
                                                        error_slot,
                                                    )
                                                })?;

                                        let metadata: solana_storage_proto::convert::generated::TransactionStatusMeta =
                                            prost_011::Message::decode(decompressed.as_slice())
                                                .map_err(|err| {
                                                    (
                                                        FirehoseError::NodeDecodingError(
                                                            item_index,
                                                            Box::new(err),
                                                        ),
                                                        error_slot,
                                                    )
                                                })?;

                                        let as_native_metadata: solana_transaction_status::TransactionStatusMeta =
                                            metadata.try_into().map_err(|err| {
                                                (
                                                    FirehoseError::NodeDecodingError(
                                                        item_index,
                                                        Box::new(err),
                                                    ),
                                                    error_slot,
                                                )
                                            })?;

                                        let message_hash = {
                                            #[cfg(feature = "verify-transaction-signatures")]
                                            {
                                                versioned_tx.verify_and_hash_message().map_err(|err| {
                                                    (
                                                        FirehoseError::TransactionHandlerError(Box::new(err)),
                                                        error_slot,
                                                    )
                                                })?
                                            }
                                            #[cfg(not(feature = "verify-transaction-signatures"))]
                                            {
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
                                            })
                                            .map_err(|err| {
                                                (
                                                    FirehoseError::NodeDecodingError(
                                                        item_index,
                                                        err,
                                                    ),
                                                    error_slot,
                                                )
                                            })?;
                                        let is_vote = is_simple_vote_transaction(&versioned_tx);

                                        on_tx_cb(
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
                                        )
                                        .await
                                        .map_err(|e| {
                                            (
                                                FirehoseError::TransactionHandlerError(e),
                                                error_slot,
                                            )
                                        })?;
                                    }
                                    fetch_add_if(
                                        tracking_enabled,
                                        &overall_transactions_processed,
                                        1,
                                    );
                                    if let Some(ref mut stats) = thread_stats {
                                        stats.transactions_processed += 1;
                                    }
                                    transactions_since_stats.fetch_add(1, Ordering::Relaxed);
                                }
                                Entry(entry) => {
                                    let entry_hash = Hash::from(entry.hash.to_bytes());
                                    let entry_transaction_count = entry.transactions.len();
                                    let entry_transaction_count_u64 = entry_transaction_count as u64;
                                    let starting_transaction_index_u64 =
                                        this_block_executed_transaction_count;
                                    latest_entry_blockhash = entry_hash;
                                    this_block_executed_transaction_count += entry_transaction_count_u64;
                                    this_block_entry_count += 1;

                                    if entry_enabled && let Some(on_entry_cb) = on_entry.as_ref() {
                                        let starting_transaction_index = usize::try_from(
                                            starting_transaction_index_u64,
                                        )
                                        .map_err(|err| {
                                            (
                                                FirehoseError::EntryHandlerError(Box::new(err)),
                                                error_slot,
                                            )
                                        })?;
                                        let transaction_indexes_end =
                                            starting_transaction_index + entry_transaction_count;
                                        on_entry_cb(
                                            thread_index,
                                            EntryData {
                                                slot: block.slot,
                                                entry_index,
                                                transaction_indexes: starting_transaction_index
                                                    ..transaction_indexes_end,
                                                num_hashes: entry.num_hashes,
                                                hash: entry_hash,
                                            },
                                        )
                                        .await
                                        .map_err(|e| {
                                            (
                                                FirehoseError::EntryHandlerError(e),
                                                error_slot,
                                            )
                                        })?;
                                    }
                                    entry_index += 1;
                                    fetch_add_if(
                                        tracking_enabled,
                                        &overall_entries_processed,
                                        1,
                                    );
                                    if let Some(ref mut stats) = thread_stats {
                                        stats.entries_processed += 1;
                                    }
                                }
                                Block(block) => {
                                    if block_enabled {
                                        if let Some(on_block_cb) = on_block.as_ref() {
                                            if let Some(previous_slot) = previous_slot {
                                                for skipped_slot in (previous_slot + 1)..slot {
                                                    log::debug!(
                                                        target: &log_target,
                                                        "leader skipped slot {} (previous slot {}, current slot {})",
                                                        skipped_slot,
                                                        previous_slot,
                                                        slot,
                                                    );
                                                    on_block_cb(
                                                        thread_index,
                                                        BlockData::LeaderSkipped {
                                                            slot: skipped_slot,
                                                        },
                                                    )
                                                    .await
                                                    .map_err(|e| {
                                                        (
                                                            FirehoseError::BlockHandlerError(e),
                                                            error_slot,
                                                        )
                                                    })?;
                                                    fetch_add_if(
                                                        tracking_enabled,
                                                        &overall_slots_processed,
                                                        1,
                                                    );
                                                    if let Some(ref mut stats) = thread_stats {
                                                        stats.leader_skipped_slots += 1;
                                                        stats.slots_processed += 1;
                                                        stats.current_slot = skipped_slot;
                                                    }
                                                }
                                            }
                                            let keyed_rewards = std::mem::take(&mut this_block_rewards);
                                            on_block_cb(
                                                thread_index,
                                                BlockData::Block {
                                                    parent_slot: block.meta.parent_slot,
                                                    parent_blockhash: previous_blockhash,
                                                    slot: block.slot,
                                                    blockhash: latest_entry_blockhash,
                                                    rewards: KeyedRewardsAndNumPartitions {
                                                        keyed_rewards,
                                                        num_partitions: None,
                                                    },
                                                    block_time: Some(block.meta.blocktime as i64),
                                                    block_height: block.meta.block_height,
                                                    executed_transaction_count:
                                                        this_block_executed_transaction_count,
                                                    entry_count: this_block_entry_count,
                                                },
                                            )
                                            .await
                                            .map_err(|e| {
                                                (
                                                    FirehoseError::BlockHandlerError(e),
                                                    error_slot,
                                                )
                                            })?;
                                        }
                                    } else {
                                        this_block_rewards.clear();
                                    }
                                    previous_blockhash = latest_entry_blockhash;
                                    if let (Some(stats_tracking_tmp), Some(thread_stats)) = (&stats_tracking, &mut thread_stats) {
                                        overall_slots_processed.fetch_add(1, Ordering::Relaxed);
                                        overall_blocks_processed.fetch_add(1, Ordering::Relaxed);
                                        thread_stats.blocks_processed += 1;
                                        thread_stats.slots_processed += 1;
                                        thread_stats.current_slot = slot;
                                        if slot % stats_tracking_tmp.tracking_interval_slots == 0 {
                                            maybe_emit_stats(
                                                stats_tracking.as_ref(),
                                                thread_index,
                                                &thread_stats,
                                                &overall_slots_processed,
                                                &overall_blocks_processed,
                                                &overall_transactions_processed,
                                                &overall_entries_processed,
                                                &transactions_since_stats,
                                            )
                                            .await?;
                                        }
                                    }
                                }
                                Subset(_subset) => (),
                                Epoch(_epoch) => (),
                                Rewards(rewards) => {
                                    if reward_enabled || block_enabled {
                                        let reassembled = nodes
                                            .reassemble_dataframes(rewards.data.clone())
                                            .map_err(|err| {
                                                (
                                                    FirehoseError::NodeDecodingError(item_index, err),
                                                    current_slot.unwrap_or(slot_range.start),
                                                )
                                            })?;
                                        if reassembled.is_empty() {
                                            this_block_rewards.clear();
                                            if reward_enabled
                                                && let Some(on_reward_cb) = on_reward.as_ref()
                                            {
                                                on_reward_cb(
                                                    thread_index,
                                                    RewardsData {
                                                        slot: block.slot,
                                                        rewards: Vec::new(),
                                                    },
                                                )
                                                .await
                                                .map_err(|e| {
                                                    (
                                                        FirehoseError::RewardHandlerError(e),
                                                        error_slot,
                                                    )
                                                })?;
                                            }
                                            continue;
                                        }

                                        let decompressed = utils::decompress_zstd(reassembled)
                                            .map_err(|err| {
                                                (
                                                    FirehoseError::NodeDecodingError(
                                                        item_index,
                                                        err,
                                                    ),
                                                    error_slot,
                                                )
                                            })?;

                                        let decoded =
                                            prost_011::Message::decode(decompressed.as_slice())
                                                .map_err(|err| {
                                                    (
                                                        FirehoseError::NodeDecodingError(
                                                            item_index,
                                                            Box::new(err),
                                                        ),
                                                        error_slot,
                                                    )
                                                })?;
                                        let keyed_rewards = convert_proto_rewards(&decoded)
                                            .map_err(|err| {
                                                (
                                                    FirehoseError::NodeDecodingError(item_index, err),
                                                    error_slot,
                                                )
                                            })?;
                                        if reward_enabled
                                            && let Some(on_reward_cb) = on_reward.as_ref()
                                        {
                                            on_reward_cb(
                                                thread_index,
                                                RewardsData {
                                                    slot: block.slot,
                                                    rewards: keyed_rewards.clone(),
                                                },
                                            )
                                            .await
                                            .map_err(|e| {
                                                (
                                                    FirehoseError::RewardHandlerError(e),
                                                    error_slot,
                                                )
                                            })?;
                                        }
                                        this_block_rewards = keyed_rewards;
                                        if let Some(ref mut stats) = thread_stats {
                                            stats.rewards_processed +=
                                                this_block_rewards.len() as u64;
                                        }
                                    }
                                }
                                DataFrame(_data_frame) => (),
                            }
                        }
                        if block.slot == slot_range.end - 1 {
                            let finish_time = std::time::Instant::now();
                            let elapsed = finish_time.duration_since(start_time);
                            log::info!(target: &log_target, "processed slot {}", block.slot);
                            let elapsed_pretty = human_readable_duration(elapsed);
                            log::info!(
                                target: &log_target,
                                "processed {} slots across {} epochs in {}.",
                                slot_range.end - slot_range.start,
                                slot_to_epoch(slot_range.end) + 1 - slot_to_epoch(slot_range.start),
                                elapsed_pretty
                            );
                            log::info!(target: &log_target, "a 🚒 firehose thread completed its work.");
                            // On completion, report threads with non-zero error counts for visibility.
                            let summary: String = error_counts
                                .iter()
                                .enumerate()
                                .filter_map(|(i, c)| {
                                    let v = c.load(Ordering::Relaxed);
                                    if v > 0 {
                                        Some(format!("{:03}({})", i, v))
                                    } else {
                                        None
                                    }
                                })
                                .collect::<Vec<_>>()
                                .join(", ");
                            if !summary.is_empty() {
                                log::debug!(target: &log_target, "threads with errors: {}", summary);
                            }
                            return Ok(());
                        }
                    }
                    if let Some(ref mut stats) = thread_stats {
                        stats.finish_time = Some(std::time::Instant::now());
                        maybe_emit_stats(
                            stats_tracking.as_ref(),
                            thread_index,
                            stats,
                            &overall_slots_processed,
                            &overall_blocks_processed,
                            &overall_transactions_processed,
                            &overall_entries_processed,
                            &transactions_since_stats,
                        )
                        .await?;
                    }
                    log::info!(target: &log_target, "thread {} has finished its work", thread_index);
                    }
                    Ok(())
            }
            .await
            {
                if is_shutdown_error(&err) {
                    log::info!(
                        target: &log_target,
                        "shutdown requested; terminating firehose thread {}",
                        thread_index
                    );
                    break;
                }
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
    if stats_tracking.is_some() {
        let elapsed = firehose_start.elapsed();
        let elapsed_secs = elapsed.as_secs_f64();
        let total_slots = overall_slots_processed.load(Ordering::Relaxed);
        let total_blocks = overall_blocks_processed.load(Ordering::Relaxed);
        let total_transactions = overall_transactions_processed.load(Ordering::Relaxed);
        let total_leader_skipped = total_slots.saturating_sub(total_blocks);
        let total_errors: u64 = error_counts
            .iter()
            .map(|counter| counter.load(Ordering::Relaxed) as u64)
            .sum();
        let overall_tps = if elapsed_secs > 0.0 {
            total_transactions as f64 / elapsed_secs
        } else {
            0.0
        };
        log::info!(
            target: LOG_MODULE,
            "firehose summary: elapsed={:.2}s, slots={}, blocks={}, leader_skipped={}, transactions={}, overall_tps={:.2}, total_errors={}",
            elapsed_secs,
            total_slots,
            total_blocks,
            total_leader_skipped,
            total_transactions,
            overall_tps,
            total_errors
        );
    }
    if shutdown_flag.load(Ordering::SeqCst) {
        log::info!(target: LOG_MODULE, "firehose shutdown complete; all threads exited cleanly.");
    } else {
        log::info!(target: LOG_MODULE, "🚒 firehose finished successfully.");
    }
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
    log::info!(target: LOG_MODULE, "starting firehose...");
    log::info!(target: LOG_MODULE, "index base url: {}", index_base_url);
    let (confirmed_bank_sender, confirmed_bank_receiver) = unbounded();
    let mut entry_notifier_maybe = None;
    let mut block_meta_notifier_maybe = None;
    let mut transaction_notifier_maybe = None;
    if let Some(geyser_config_files) = geyser_config_files {
        log::debug!(target: LOG_MODULE, "geyser config files: {:?}", geyser_config_files);

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

        log::debug!(target: LOG_MODULE, "geyser plugin service initialized.");
    }

    if entry_notifier_maybe.is_some() {
        log::debug!(target: LOG_MODULE, "entry notifications enabled")
    } else {
        log::debug!(target: LOG_MODULE, "none of the plugins have enabled entry notifications")
    }
    log::info!(target: LOG_MODULE, "running on_load...");
    rt.spawn(on_load);

    let slot_range = Arc::new(slot_range);
    let transaction_notifier_maybe = Arc::new(transaction_notifier_maybe);
    let entry_notifier_maybe = Arc::new(entry_notifier_maybe);
    let block_meta_notifier_maybe = Arc::new(block_meta_notifier_maybe);
    let confirmed_bank_sender = Arc::new(confirmed_bank_sender);

    // divide slot_range into n subranges
    let subranges = generate_subranges(&slot_range, threads);
    if threads > 1 {
        log::info!(target: LOG_MODULE, "⚡ thread sub-ranges: {:?}", subranges);
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
        let client = client.clone();
        let error_counts = error_counts.clone();

        let rt_clone = rt.clone();

        let handle = std::thread::spawn(move || {
            rt_clone.block_on(async {
                firehose_geyser_thread(
                    slot_range,
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
    log::info!(target: LOG_MODULE, "🚒 firehose finished successfully.");
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
        format!("{}::T{:03}", LOG_MODULE, thread_index)
    } else {
        LOG_MODULE.to_string()
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
                    let seek_fut = reader.seek_to_slot(slot_range.start);
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
                    if nodes.is_empty() {
                        log::info!(
                            target: &log_target,
                            "reached end of epoch {}",
                            epoch_num
                        );
                        break;
                    }
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
                    let mut this_block_rewards: Vec<(Pubkey, RewardInfo)> = Vec::new();

                    nodes.each(|node_with_cid| -> Result<(), Box<dyn std::error::Error>> {
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
                                    log::info!(
                                        target: &log_target,
                                        "skipping until index {} (at {})",
                                        skip,
                                        item_index
                                    );
                                    displayed_skip_message = true;
                                }
                                return Ok(());
                            } else {
                                log::info!(
                                    target: &log_target,
                                    "reached target index {}, resuming...",
                                    skip
                                );
                                skip_until_index = None;
                            }
                        }
                        let node = node_with_cid.get_node();

                        use crate::node::Node::*;
                        match node {
                            Transaction(tx) => {
                                let versioned_tx = tx.as_parsed()?;
                                let reassembled_metadata = nodes.reassemble_dataframes(tx.metadata.clone())?;

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
                                let entry_hash = Hash::from(entry.hash.to_bytes());
                                let entry_transaction_count = entry.transactions.len();
                                let entry_transaction_count_u64 = entry_transaction_count as u64;
                                let starting_transaction_index =
                                    usize::try_from(this_block_executed_transaction_count).map_err(|_| {
                                        Box::new(std::io::Error::other(
                                            "transaction index exceeds usize range",
                                        )) as Box<dyn std::error::Error>
                                    })?;
                                todo_latest_entry_blockhash = entry_hash;
                                this_block_executed_transaction_count += entry_transaction_count_u64;
                                this_block_entry_count += 1;
                                if entry_notifier_maybe.is_none() {
                                    return Ok(());
                                }
                                let entry_notifier = entry_notifier_maybe.as_ref().unwrap();
                                let entry_summary = solana_entry::entry::EntrySummary {
                                    num_hashes: entry.num_hashes,
                                    hash: Hash::from(entry.hash.to_bytes()),
                                    num_transactions: entry_transaction_count_u64,
                                };
                                entry_notifier.notify_entry(
                                    block.slot,
                                    entry_index,
                                    &entry_summary,
                                    starting_transaction_index,
                                );
                                entry_index += 1;
                            }
                            Block(block) => {
                                let notification = SlotNotification::Root((block.slot, block.meta.parent_slot));
                                confirmed_bank_sender.send(notification).unwrap();

                                if block_meta_notifier_maybe.is_none() {
                                    return Ok(());
                                }
                                let keyed_rewards = std::mem::take(&mut this_block_rewards);
                                let block_meta_notifier = block_meta_notifier_maybe.as_ref().unwrap();
                                block_meta_notifier.notify_block_metadata(
                                    block.meta.parent_slot,
                                    todo_previous_blockhash.to_string().as_str(),
                                    block.slot,
                                    todo_latest_entry_blockhash.to_string().as_str(),
                                    &KeyedRewardsAndNumPartitions {
                                        keyed_rewards,
                                        num_partitions: None,
                                    },
                                    Some(block.meta.blocktime as i64),
                                    block.meta.block_height,
                                    this_block_executed_transaction_count,
                                    this_block_entry_count,
                                );
                                todo_previous_blockhash = todo_latest_entry_blockhash;
                                std::thread::yield_now();
                            }
                            Subset(_subset) => (),
                            Epoch(_epoch) => (),
                            Rewards(rewards) => {
                                if !rewards.is_complete() {
                                    let reassembled = nodes.reassemble_dataframes(rewards.data.clone())?;
                                    let decompressed = utils::decompress_zstd(reassembled)?;
                                    let decoded = prost_011::Message::decode(decompressed.as_slice()).map_err(|err| {
                                        Box::new(std::io::Error::other(
                                            std::format!("Error decoding rewards: {:?}", err),
                                        ))
                                    })?;
                                    this_block_rewards = convert_proto_rewards(&decoded)?;
                                }
                            }
                            DataFrame(_data_frame) => (),
                        }
                        Ok(())
                    })
                .map_err(|e| FirehoseError::NodeDecodingError(item_index, e)).map_err(|e| (e, current_slot.unwrap_or(slot_range.start)))?;
                    if block.slot == slot_range.end - 1 {
                        let finish_time = std::time::Instant::now();
                        let elapsed = finish_time.duration_since(start_time);
                        log::info!(target: &log_target, "processed slot {}", block.slot);
                        let elapsed_pretty = human_readable_duration(elapsed);
                        log::info!(
                            target: &log_target,
                            "processed {} slots across {} epochs in {}.",
                            slot_range.end - slot_range.start,
                            slot_to_epoch(slot_range.end) + 1 - slot_to_epoch(slot_range.start),
                            elapsed_pretty
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
        if is_shutdown_error(&err) {
            log::info!(
                target: &log_target,
                "shutdown requested; terminating firehose thread {:?}",
                thread_index
            );
            return Ok(());
        }
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

#[inline(always)]
fn convert_proto_rewards(
    proto_rewards: &solana_storage_proto::convert::generated::Rewards,
) -> Result<Vec<(Pubkey, RewardInfo)>, Box<dyn std::error::Error>> {
    let mut keyed_rewards = Vec::with_capacity(proto_rewards.rewards.len());
    for proto_reward in proto_rewards.rewards.iter() {
        let reward = RewardInfo {
            reward_type: match proto_reward.reward_type - 1 {
                0 => RewardType::Fee,
                1 => RewardType::Rent,
                2 => RewardType::Staking,
                3 => RewardType::Voting,
                typ => {
                    return Err(Box::new(std::io::Error::other(format!(
                        "unsupported reward type {}",
                        typ
                    ))));
                }
            },
            lamports: proto_reward.lamports,
            post_balance: proto_reward.post_balance,
            commission: proto_reward.commission.parse::<u8>().ok(),
        };
        let pubkey = proto_reward
            .pubkey
            .parse::<Pubkey>()
            .map_err(|err| Box::new(err) as Box<dyn std::error::Error>)?;
        keyed_rewards.push((pubkey, reward));
    }
    Ok(keyed_rewards)
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
        target: LOG_MODULE,
        "Generated {} thread ranges covering {} slots total",
        threads,
        total_covered
    );
    ranges
}

fn human_readable_duration(duration: std::time::Duration) -> String {
    if duration.is_zero() {
        return "0s".into();
    }
    let total_secs = duration.as_secs();
    if total_secs < 60 {
        let secs_f = duration.as_secs_f64();
        if total_secs == 0 {
            format!("{:.2}s", secs_f)
        } else if duration.subsec_millis() == 0 {
            format!("{}s", total_secs)
        } else {
            format!("{:.2}s", secs_f)
        }
    } else {
        let mut secs = total_secs;
        let days = secs / 86_400;
        secs %= 86_400;
        let hours = secs / 3_600;
        secs %= 3_600;
        let minutes = secs / 60;
        secs %= 60;
        if days > 0 {
            if hours > 0 {
                format!("{days}d{hours}h")
            } else {
                format!("{days}d")
            }
        } else if hours > 0 {
            if minutes > 0 {
                format!("{hours}h{minutes}m")
            } else {
                format!("{hours}h")
            }
        } else if minutes > 0 {
            if secs > 0 {
                format!("{minutes}m{secs}s")
            } else {
                format!("{minutes}m")
            }
        } else {
            format!("{secs}s")
        }
    }
}

#[cfg(test)]
fn log_stats_handler(thread_id: usize, stats: Stats) -> HandlerFuture {
    Box::pin(async move {
        let elapsed = stats.start_time.elapsed();
        let elapsed_secs = elapsed.as_secs_f64();
        let tps = if elapsed_secs > 0.0 {
            stats.transactions_processed as f64 / elapsed_secs
        } else {
            0.0
        };
        log::info!(
            target: LOG_MODULE,
            "thread {thread_id} stats: current_slot={}, slots_processed={}, blocks_processed={}, txs={}, entries={}, rewards={}, elapsed_s={:.2}, tps={:.2}",
            stats.thread_stats.current_slot,
            stats.slots_processed,
            stats.blocks_processed,
            stats.transactions_processed,
            stats.entries_processed,
            stats.rewards_processed,
            elapsed_secs,
            tps
        );
        Ok(())
    })
}

#[tokio::test(flavor = "multi_thread")]
async fn test_firehose_epoch_800() {
    use std::sync::atomic::{AtomicU64, Ordering};
    solana_logger::setup_with_default("info");
    const THREADS: usize = 4;
    const NUM_SLOTS_TO_COVER: u64 = 50;
    static PREV_BLOCK: [AtomicU64; THREADS] = [const { AtomicU64::new(0) }; THREADS];
    static NUM_SKIPPED_BLOCKS: AtomicU64 = AtomicU64::new(0);
    static NUM_BLOCKS: AtomicU64 = AtomicU64::new(0);
    let stats_tracking = StatsTracking {
        on_stats: log_stats_handler,
        tracking_interval_slots: 10,
    };

    firehose(
        THREADS.try_into().unwrap(),
        (345600000 - NUM_SLOTS_TO_COVER / 2)..(345600000 + NUM_SLOTS_TO_COVER / 2),
        Some(|thread_id: usize, block: BlockData| {
            async move {
                let prev =
                    PREV_BLOCK[thread_id % PREV_BLOCK.len()].swap(block.slot(), Ordering::Relaxed);
                if block.was_skipped() {
                    log::info!(
                        target: LOG_MODULE,
                        "leader skipped block {} on thread {}",
                        block.slot(),
                        thread_id,
                    );
                } else {
                    /*log::info!(
                        target: LOG_MODULE,
                        "got block {} on thread {}",
                        block.slot(),
                        thread_id,
                    );*/
                }

                if prev > 0 {
                    assert_eq!(prev + 1, block.slot());
                }
                if block.was_skipped() {
                    NUM_SKIPPED_BLOCKS.fetch_add(1, Ordering::Relaxed);
                } else {
                    NUM_BLOCKS.fetch_add(1, Ordering::Relaxed);
                }
                Ok(())
            }
            .boxed()
        }),
        None::<OnTxFn>,
        None::<OnEntryFn>,
        None::<OnRewardFn>,
        Some(stats_tracking),
        None,
    )
    .await
    .unwrap();
    assert_eq!(
        NUM_BLOCKS.load(Ordering::Relaxed) + NUM_SKIPPED_BLOCKS.load(Ordering::Relaxed),
        NUM_SLOTS_TO_COVER
    );
    assert!(NUM_BLOCKS.load(Ordering::Relaxed) > 0);
}
