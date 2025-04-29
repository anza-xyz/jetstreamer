use crossbeam_channel::{unbounded, Receiver};
use demo_rust_ipld_car::utils;
use rayon::prelude::*;
use reqwest::Client;
use solana_geyser_plugin_manager::geyser_plugin_service::GeyserPluginServiceError;
use solana_rpc::optimistically_confirmed_bank_tracker::SlotNotification;
use solana_runtime::bank::KeyedRewardsAndNumPartitions;
use solana_sdk::{reward_info::RewardInfo, reward_type::RewardType};
use std::io::SeekFrom;
use std::path::Path;
use std::{collections::HashSet, fmt::Display, ops::Range, path::PathBuf};
use thiserror::Error;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

use crate::slot_cache::fetch_epoch_slot_range;
use crate::{epochs_async::fetch_epoch_stream, node_reader::AsyncNodeReader};

#[derive(Debug, Error)]
pub enum GeyserReplayError {
    Reqwest(reqwest::Error),
    ReadHeader(Box<dyn std::error::Error>),
    GeyserPluginService(GeyserPluginServiceError),
    FailedToGetTransactionNotifier,
    ReadUntilBlockError(Box<dyn std::error::Error>),
    GetBlockError(Box<dyn std::error::Error>),
    NodeDecodingError(usize, Box<dyn std::error::Error>),
    SkipBlockError(Box<dyn std::error::Error>),
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
            GeyserReplayError::SkipBlockError(error) => {
                write!(f, "Error skipping block: {}", error)
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

pub async fn firehose(
    slot_range: Range<u64>,
    epoch_range: Range<u64>,
    geyser_config_files: Option<&[PathBuf]>,
    client: Client,
) -> Result<Receiver<SlotNotification>, GeyserReplayError> {
    log::info!("starting firehose...");
    let (confirmed_bank_sender, confirmed_bank_receiver) = unbounded();
    let mut entry_notifier_maybe = None;
    let mut block_meta_notifier_maybe = None;
    let mut transaction_notifier_maybe = None;
    if let Some(geyser_config_files) = geyser_config_files {
        log::debug!("Geyser config files: {:?}", geyser_config_files);

        let service =
            solana_geyser_plugin_manager::geyser_plugin_service::GeyserPluginService::new(
                confirmed_bank_receiver.clone(),
                geyser_config_files,
            )?;

        transaction_notifier_maybe = Some(
            service
                .get_transaction_notifier()
                .ok_or(GeyserReplayError::FailedToGetTransactionNotifier)?,
        );

        entry_notifier_maybe = service.get_entry_notifier();
        block_meta_notifier_maybe = service.get_block_metadata_notifier();

        log::debug!("Geyser plugin service initialized.");
    }

    if entry_notifier_maybe.is_some() {
        log::debug!("Entry notifications enabled")
    } else {
        log::debug!("None of the plugins have enabled entry notifications")
    }

    // for each epoch
    for (epoch_num, stream) in epoch_range
        .map(|epoch| fetch_epoch_stream(epoch, &client))
        .enumerate()
    {
        log::info!("Processing epoch {}", epoch_num);
        let stream = stream.await;
        let mut reader = AsyncNodeReader::new(stream);

        let header = reader
            .read_raw_header()
            .await
            .map_err(GeyserReplayError::ReadHeader)?;
        log::debug!("read epoch {} header: {:?}", epoch_num, header);

        let mut todo_previous_blockhash = solana_sdk::hash::Hash::default();
        let mut todo_latest_entry_blockhash = solana_sdk::hash::Hash::default();

        // for each item in each block
        let mut item_index = 0;
        let mut current_slot: Option<u64> = None;
        loop {
            if let Some(current) = &mut current_slot {
                if *current + 1 < slot_range.start {
                    let target = slot_range.start - 1;
                    let diff = slot_range.start - *current;
                    log::debug!("skipping slots {}-{}", current, target);
                    for _ in 0..diff {
                        log::debug!("skipping slot {}", current);
                        reader
                            .skip_next()
                            .await
                            .map_err(GeyserReplayError::SkipBlockError)?;
                        *current += 1;
                    }
                    continue;
                }
            }
            let nodes = reader
                .read_until_block()
                .await
                .map_err(GeyserReplayError::ReadUntilBlockError)?;
            let block = nodes
                .get_block()
                .map_err(GeyserReplayError::GetBlockError)?;
            log::debug!(
                "read block {} of epoch {} with slot {}",
                item_index,
                epoch_num,
                block.slot
            );
            current_slot = Some(block.slot);
            if block.slot > slot_range.end {
                log::debug!("slot range exceeded {}", block.slot);
                break;
            }
            if !slot_range.contains(&block.slot) {
                log::debug!("skipping slot {}", block.slot);
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
											reward_type: match this_block_reward.reward_type {
												0 => RewardType::Fee,
												1 => RewardType::Rent,
												2 => RewardType::Staking,
												3 => RewardType::Voting,
												_ => panic!("___ not supported reward type"),
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
                .map_err(|e| GeyserReplayError::NodeDecodingError(item_index, e))?;
        }
    }
    Ok(confirmed_bank_receiver)
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

/// Build a block-only index: `[u64 slot] [u64 offset] [u64 size]`.
///
/// Uses `next_parsed()` for maximum throughput (no second parse pass). Build a `[slot | offset
/// | size]` index quickly. Requires that `self.reader` is already wrapped in a `BufReader`.
/// Build a block-only index `[u64 slot] [u64 offset] [u64 size]`. Re-uses a single buffer to
/// avoid per-block allocations.
pub async fn build_index<P>(
    client: &reqwest::Client,
    epoch: u64,
    idx_path: P,
    start_offset: Option<u64>,
) -> Result<(), Box<dyn std::error::Error>>
where
    P: AsRef<std::path::Path>,
{
    let stream = fetch_epoch_stream(epoch, client).await;
    let mut node_reader = AsyncNodeReader::new(stream);

    /* ── 1. make sure the CAR header has been consumed ───────────────────── */
    if node_reader.header.is_empty() {
        node_reader.read_raw_header().await?;
    }

    /* ── 2. output file ──────────────────────────────────────────────────── */
    let mut out = File::options()
        .create(true)
        .append(true)
        .open(idx_path)
        .await?;

    /* ── 3. helper: read varint and return its byte-length ───────────────── */
    async fn read_uvarint_len<R: AsyncReadExt + Unpin>(r: &mut R) -> std::io::Result<(u64, u64)> {
        let mut x = 0u64;
        let mut s = 0u32;
        let mut buf = [0u8; 1];
        let mut n = 0u64;
        loop {
            r.read_exact(&mut buf).await?;
            n += 1;
            let b = buf[0];
            if b < 0x80 {
                return Ok((x | ((b as u64) << s), n));
            }
            x |= ((b & 0x7f) as u64) << s;
            s += 7;
            if s > 63 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "uvarint overflow",
                ));
            }
        }
    }

    /* ── 4. streaming loop ──────────────────────────────────────────────── */
    let mut buf: Vec<u8> = Vec::with_capacity(64 * 1024); // reusable scratch
    let mut offset = node_reader.reader.stream_position().await?; // current file pos
    if let Some(start_offset) = start_offset {
        offset = start_offset;
        // seek to start_offset if it was specified
        log::info!(
            "(Epoch = {}) Seeking to start offset: {}",
            epoch,
            start_offset
        );
        node_reader.reader.seek(SeekFrom::Start(offset)).await?;
    }
    let mut blocks = 0u64;

    loop {
        let start_off = offset;

        /* size of the section + how long the varint was */
        let (section_size, varint_len) = match read_uvarint_len(&mut node_reader.reader).await {
            Ok(v) => v,
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e.into()),
        };

        /* ensure buffer big enough, then read */
        if buf.len() < section_size as usize {
            buf.resize(section_size as usize, 0);
        }
        node_reader
            .reader
            .read_exact(&mut buf[..section_size as usize])
            .await?;

        offset += varint_len + section_size; // next loop starts here

        /* parse exactly once */
        let bytes_vec = buf[..section_size as usize].to_vec(); // <-- make Vec<u8>
        let mut cur = std::io::Cursor::new(bytes_vec); // Cursor<Vec<u8>>
        let raw = crate::node_reader::RawNode::from_cursor(&mut cur).await?;

        if let crate::node::Node::Block(b) = raw.parse()? {
            blocks += 1;

            /* slot | offset | size  */
            out.write_all(&b.slot.to_le_bytes()).await?;
            out.write_all(&start_off.to_le_bytes()).await?;
            out.write_all(&(varint_len + section_size).to_le_bytes())
                .await?;

            if b.slot % 100 == 0 {
                log::info!(
                    "build_index: Epoch={} Block slot={} @ {} ({} B) - {} indexed",
                    epoch,
                    b.slot,
                    start_off,
                    section_size + varint_len,
                    blocks
                );
            }
        }
    }

    out.flush().await?;
    log::info!(
        "build_index for epoch={}: DONE – {} Block records",
        epoch,
        blocks
    );
    Ok(())
}

/// Queries the current epoch from mainnet
pub async fn current_epoch(client: &Client) -> Result<u64, Box<dyn std::error::Error>> {
    let url = "https://api.mainnet-beta.solana.com";
    let request_body = r#"{"jsonrpc":"2.0","id":1,"method":"getEpochInfo","params":[]}"#;
    let response = client
        .post(url)
        .header("Content-Type", "application/json")
        .body(request_body)
        .send()
        .await?;
    let text = response.text().await?;
    let epoch_info: serde_json::Value = serde_json::from_str(&text).unwrap();
    let epoch = epoch_info["result"]["epoch"].as_u64().unwrap();
    Ok(epoch)
}

pub async fn latest_old_faithful_epoch(
    client: &Client,
    epoch: Option<u64>,
) -> Result<(u64, u64, u64), Box<dyn std::error::Error>> {
    let mut epoch = if let Some(epoch) = epoch {
        epoch
    } else {
        current_epoch(client).await?
    };
    loop {
        if let Some(res) = fetch_epoch_slot_range(epoch, client).await {
            return Ok(res);
        }
        epoch -= 1;
    }
}

pub async fn get_latest_index_line(
    idx_path: impl AsRef<Path>,
) -> Result<(u64, u64, u64), Box<dyn std::error::Error>> {
    let idx_path = idx_path.as_ref();
    if !idx_path.exists() {
        return Err(format!("Index file does not exist: {:?}", idx_path).into());
    }
    let mut file = File::open(idx_path).await?;
    let mut buf = vec![0u8; 24];
    file.seek(SeekFrom::End(-24)).await?;
    file.read_exact(&mut buf).await?;

    let slot = u64::from_le_bytes(buf[0..8].try_into().unwrap());
    let offset = u64::from_le_bytes(buf[8..16].try_into().unwrap());
    let size = u64::from_le_bytes(buf[16..24].try_into().unwrap());
    Ok((slot, offset, size))
}

pub async fn build_missing_indexes(
    idx_dir: impl AsRef<Path>,
) -> Result<(), Box<dyn std::error::Error>> {
    rayon::ThreadPoolBuilder::new()
        .num_threads(32)
        .build_global()
        .unwrap();
    let idx_dir = idx_dir.as_ref();
    if !idx_dir.exists() {
        log::info!("Creating index directory: {:?}", idx_dir);
        std::fs::create_dir_all(idx_dir)?;
    } else {
        log::info!("Index directory already exists: {:?}", idx_dir);
    }

    let client = Client::new();

    let current_epoch = current_epoch(&client).await?;
    log::info!("Current Mainnet epoch: {}", current_epoch);

    let (of1_last_epoch, of1_last_epoch_first_slot, of1_last_epoch_last_slot) =
        latest_old_faithful_epoch(&client, Some(current_epoch)).await?;
    log::info!(
        "Latest Old Faithful epoch: {} (slots {}-{})",
        of1_last_epoch,
        of1_last_epoch_first_slot,
        of1_last_epoch_last_slot
    );

    (0..of1_last_epoch)
        .into_iter()
        .par_bridge()
        .for_each(|epoch| {
            tokio::runtime::Runtime::new().unwrap().block_on(async {
                let client = Client::new();
                let idx_path = idx_dir.join(format!("epoch-{}.idx", epoch));
                let (epoch, start_slot, end_slot) =
                    fetch_epoch_slot_range(epoch, &client).await.unwrap();
                if idx_path.exists() {
                    let Ok((last_slot, last_offset, last_size)) =
                        get_latest_index_line(&idx_path).await
                    else {
                        log::error!("Failed to get last index line for epoch {}", epoch);
                        log::info!("Building index for epoch {} from scratch", epoch);
                        build_index(&client, epoch, &idx_path, None).await.unwrap();
                        log::info!("Finished building index for epoch {}", epoch);
                        return;
                    };

                    let offset = last_offset + last_size;
                    if last_slot < end_slot {
                        log::info!(
                            "Building index for epoch {} from offset: {} (slots {}-{})",
                            epoch,
                            offset,
                            last_slot,
                            end_slot
                        );
                        build_index(&client, epoch, &idx_path, Some(offset))
                            .await
                            .unwrap();
                    } else {
                        log::info!(
                            "Full index already exists for epoch {} (slots {}-{})",
                            epoch,
                            start_slot,
                            end_slot
                        );
                    }
                } else {
                    log::info!("Building index for epoch {} from scratch", epoch);
                    build_index(&client, epoch, &idx_path, None).await.unwrap();
                }
                log::info!("Finished building index for epoch {}", epoch);
            });
        });
    Ok(())
}

#[tokio::test(worker_threads = 32, flavor = "multi_thread")]
async fn test_firehose() {
    solana_logger::setup_with_default("debug");
    let client = reqwest::Client::new();
    let slot_range = (302400000 + 1000)..(302400000 + 1000 + 10);
    let epoch_range = 700..701;
    firehose(slot_range, epoch_range, None, client)
        .await
        .unwrap();
}

#[tokio::test(worker_threads = 64, flavor = "multi_thread")]
async fn test_build_missing_indexes() {
    solana_logger::setup_with_default("info");
    let idx_dir = PathBuf::from("./src/index");
    build_missing_indexes(&idx_dir).await.unwrap();
}

// #[tokio::test]
// async fn test_build_index() {
//     solana_logger::setup_with_default("info,geyser_replay::node_reader=debug");

//     let client = reqwest::Client::new();
//     let _ = std::fs::remove_file("./../bin/index.idx");
//     build_index(&client, 670, "./../bin/index.idx")
//         .await
//         .unwrap();
// }
