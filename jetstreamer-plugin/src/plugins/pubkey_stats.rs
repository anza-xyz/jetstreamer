use std::sync::Arc;

use clickhouse::{Client, Row};
use dashmap::DashMap;
use futures_util::FutureExt;
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use solana_address::Address;
use solana_message::VersionedMessage;

use crate::{Plugin, PluginFuture};
use jetstreamer_firehose::firehose::{BlockData, TransactionData};

/// Per-slot accumulator: maps each pubkey to its mention count within that slot.
static PENDING_BY_SLOT: Lazy<
    DashMap<u64, DashMap<Address, u32, ahash::RandomState>, ahash::RandomState>,
> = Lazy::new(|| DashMap::with_hasher(ahash::RandomState::new()));

#[derive(Row, Deserialize, Serialize, Copy, Clone, Debug)]
struct PubkeyMention {
    slot: u32,
    timestamp: u32,
    pubkey: Address,
    num_mentions: u32,
}

#[derive(Debug, Clone)]
/// Tracks per-slot pubkey mention counts and writes them to ClickHouse.
///
/// For every transaction, all account keys referenced in the message (both static and loaded)
/// are counted. A ClickHouse `pubkey_mentions` table stores the aggregated count per
/// `(slot, pubkey)` pair using `ReplacingMergeTree` for safe parallel ingestion.
///
/// A companion `pubkeys` table assigns a unique auto-incremented id to each pubkey, maintained
/// via a materialised view so lookups by id are efficient.
pub struct PubkeyStatsPlugin;

impl PubkeyStatsPlugin {
    /// Creates a new instance.
    pub const fn new() -> Self {
        Self
    }

    fn take_slot_events(slot: u64, block_time: Option<i64>) -> Vec<PubkeyMention> {
        let timestamp = clamp_block_time(block_time);
        if let Some((_, pubkey_counts)) = PENDING_BY_SLOT.remove(&slot) {
            return pubkey_counts
                .into_iter()
                .map(|(pubkey, num_mentions)| PubkeyMention {
                    slot: slot.min(u32::MAX as u64) as u32,
                    timestamp,
                    pubkey,
                    num_mentions,
                })
                .collect();
        }
        Vec::new()
    }

    fn drain_all_pending(block_time: Option<i64>) -> Vec<PubkeyMention> {
        let timestamp = clamp_block_time(block_time);
        let slots: Vec<u64> = PENDING_BY_SLOT.iter().map(|entry| *entry.key()).collect();
        let mut rows = Vec::new();
        for slot in slots {
            if let Some((_, pubkey_counts)) = PENDING_BY_SLOT.remove(&slot) {
                rows.extend(pubkey_counts.into_iter().map(|(pubkey, num_mentions)| {
                    PubkeyMention {
                        slot: slot.min(u32::MAX as u64) as u32,
                        timestamp,
                        pubkey,
                        num_mentions,
                    }
                }));
            }
        }
        rows
    }
}

impl Default for PubkeyStatsPlugin {
    fn default() -> Self {
        Self::new()
    }
}

impl Plugin for PubkeyStatsPlugin {
    #[inline(always)]
    fn name(&self) -> &'static str {
        "Pubkey Stats"
    }

    #[inline(always)]
    fn on_transaction<'a>(
        &'a self,
        _thread_id: usize,
        _db: Option<Arc<Client>>,
        transaction: &'a TransactionData,
    ) -> PluginFuture<'a> {
        async move {
            let account_keys = match &transaction.transaction.message {
                VersionedMessage::Legacy(msg) => &msg.account_keys,
                VersionedMessage::V0(msg) => &msg.account_keys,
            };
            if account_keys.is_empty() {
                return Ok(());
            }

            let slot = transaction.slot;
            let slot_entry = PENDING_BY_SLOT
                .entry(slot)
                .or_insert_with(|| DashMap::with_hasher(ahash::RandomState::new()));
            for pubkey in account_keys {
                *slot_entry.entry(*pubkey).or_insert(0) += 1;
            }

            Ok(())
        }
        .boxed()
    }

    #[inline(always)]
    fn on_block(
        &self,
        _thread_id: usize,
        db: Option<Arc<Client>>,
        block: &BlockData,
    ) -> PluginFuture<'_> {
        let slot = block.slot();
        let block_time = block.block_time();
        let was_skipped = block.was_skipped();
        async move {
            if was_skipped {
                return Ok(());
            }

            let rows = Self::take_slot_events(slot, block_time);

            if let Some(db_client) = db
                && !rows.is_empty()
            {
                tokio::spawn(async move {
                    if let Err(err) = write_pubkey_mentions(db_client, rows).await {
                        log::error!("failed to write pubkey mentions: {}", err);
                    }
                });
            }

            Ok(())
        }
        .boxed()
    }

    #[inline(always)]
    fn on_load(&self, db: Option<Arc<Client>>) -> PluginFuture<'_> {
        async move {
            log::info!("Pubkey Stats Plugin loaded.");
            if let Some(db) = db {
                log::info!("Creating pubkey_mentions table if it does not exist...");
                db.query(
                    r#"
                    CREATE TABLE IF NOT EXISTS pubkey_mentions (
                        slot          UInt32,
                        timestamp     DateTime('UTC'),
                        pubkey        FixedString(32),
                        num_mentions  UInt32
                    )
                    ENGINE = ReplacingMergeTree(timestamp)
                    ORDER BY (slot, pubkey)
                    "#,
                )
                .execute()
                .await?;

                log::info!("Creating pubkeys table if it does not exist...");
                db.query(
                    r#"
                    CREATE TABLE IF NOT EXISTS pubkeys (
                        pubkey  FixedString(32),
                        id      UInt64
                    )
                    ENGINE = ReplacingMergeTree()
                    ORDER BY pubkey
                    "#,
                )
                .execute()
                .await?;

                log::info!("Creating pubkeys materialised view if it does not exist...");
                db.query(
                    r#"
                    CREATE MATERIALIZED VIEW IF NOT EXISTS pubkeys_mv TO pubkeys AS
                    SELECT
                        pubkey,
                        sipHash64(pubkey) AS id
                    FROM pubkey_mentions
                    GROUP BY pubkey
                    "#,
                )
                .execute()
                .await?;

                log::info!("done.");
            } else {
                log::warn!(
                    "Pubkey Stats Plugin running without ClickHouse; data will not be persisted."
                );
            }
            Ok(())
        }
        .boxed()
    }

    #[inline(always)]
    fn on_exit(&self, db: Option<Arc<Client>>) -> PluginFuture<'_> {
        async move {
            if let Some(db_client) = db {
                let rows = Self::drain_all_pending(None);
                if !rows.is_empty() {
                    write_pubkey_mentions(Arc::clone(&db_client), rows)
                        .await
                        .map_err(|err| -> Box<dyn std::error::Error + Send + Sync> {
                            Box::new(err)
                        })?;
                }
                backfill_pubkey_timestamps(db_client)
                    .await
                    .map_err(|err| -> Box<dyn std::error::Error + Send + Sync> { Box::new(err) })?;
            }
            Ok(())
        }
        .boxed()
    }
}

async fn write_pubkey_mentions(
    db: Arc<Client>,
    rows: Vec<PubkeyMention>,
) -> Result<(), clickhouse::error::Error> {
    if rows.is_empty() {
        return Ok(());
    }
    let mut insert = db.insert::<PubkeyMention>("pubkey_mentions").await?;
    for row in rows {
        insert.write(&row).await?;
    }
    insert.end().await?;
    Ok(())
}

fn clamp_block_time(block_time: Option<i64>) -> u32 {
    let Some(raw_ts) = block_time else {
        return 0;
    };
    if raw_ts < 0 {
        0
    } else if raw_ts > u32::MAX as i64 {
        u32::MAX
    } else {
        raw_ts as u32
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Plugin;
    use jetstreamer_firehose::firehose::{BlockData, TransactionData};
    use serial_test::serial;
    use solana_hash::Hash;
    use solana_message::VersionedMessage;
    use solana_message::legacy::Message as LegacyMessage;
    use solana_runtime::bank::KeyedRewardsAndNumPartitions;
    use solana_transaction::versioned::VersionedTransaction;
    use solana_transaction_status::TransactionStatusMeta;

    fn make_tx(slot: u64, account_keys: Vec<Address>) -> TransactionData {
        let message = LegacyMessage {
            account_keys,
            ..LegacyMessage::default()
        };
        TransactionData {
            slot,
            transaction_slot_index: 0,
            signature: Default::default(),
            message_hash: Hash::default(),
            is_vote: false,
            transaction_status_meta: TransactionStatusMeta {
                status: Ok(()),
                fee: 0,
                pre_balances: vec![],
                post_balances: vec![],
                inner_instructions: None,
                log_messages: None,
                pre_token_balances: None,
                post_token_balances: None,
                rewards: None,
                loaded_addresses: Default::default(),
                return_data: None,
                compute_units_consumed: Some(0),
                cost_units: None,
            },
            transaction: VersionedTransaction {
                signatures: vec![],
                message: VersionedMessage::Legacy(message),
            },
        }
    }

    fn make_block(slot: u64, block_time: Option<i64>) -> BlockData {
        BlockData::Block {
            slot,
            parent_slot: slot.saturating_sub(1),
            blockhash: Hash::default(),
            parent_blockhash: Hash::default(),
            rewards: KeyedRewardsAndNumPartitions {
                keyed_rewards: vec![],
                num_partitions: None,
            },
            block_time,
            block_height: Some(slot),
            executed_transaction_count: 0,
            entry_count: 0,
        }
    }

    fn clear_pending() {
        PENDING_BY_SLOT.clear();
    }

    #[test]
    fn clamp_block_time_none_returns_zero() {
        assert_eq!(clamp_block_time(None), 0);
    }

    #[test]
    fn clamp_block_time_negative_returns_zero() {
        assert_eq!(clamp_block_time(Some(-100)), 0);
    }

    #[test]
    fn clamp_block_time_overflow_returns_max() {
        assert_eq!(clamp_block_time(Some(u32::MAX as i64 + 1)), u32::MAX);
    }

    #[test]
    fn clamp_block_time_normal() {
        assert_eq!(clamp_block_time(Some(1_700_000_000)), 1_700_000_000);
    }

    #[serial]
    #[tokio::test]
    async fn single_transaction_counts_all_account_keys() {
        clear_pending();
        let plugin = PubkeyStatsPlugin::new();
        let key_a = Address::from([1u8; 32]);
        let key_b = Address::from([2u8; 32]);
        let key_c = Address::from([3u8; 32]);
        let tx = make_tx(100, vec![key_a, key_b, key_c]);

        plugin.on_transaction(0, None, &tx).await.unwrap();

        let events = PubkeyStatsPlugin::take_slot_events(100, Some(1_700_000_000));
        assert_eq!(events.len(), 3);
        for event in &events {
            assert_eq!(event.num_mentions, 1);
            assert_eq!(event.slot, 100);
            assert_eq!(event.timestamp, 1_700_000_000);
        }
    }

    #[serial]
    #[tokio::test]
    async fn duplicate_keys_in_single_transaction_accumulate() {
        clear_pending();
        let plugin = PubkeyStatsPlugin::new();
        let key_a = Address::from([1u8; 32]);
        let tx = make_tx(200, vec![key_a, key_a, key_a]);

        plugin.on_transaction(0, None, &tx).await.unwrap();

        let events = PubkeyStatsPlugin::take_slot_events(200, None);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].num_mentions, 3);
        assert_eq!(events[0].pubkey, key_a);
    }

    #[serial]
    #[tokio::test]
    async fn multiple_transactions_same_slot_accumulate() {
        clear_pending();
        let plugin = PubkeyStatsPlugin::new();
        let key_a = Address::from([10u8; 32]);
        let key_b = Address::from([20u8; 32]);

        let tx1 = make_tx(300, vec![key_a, key_b]);
        let tx2 = make_tx(300, vec![key_a]);

        plugin.on_transaction(0, None, &tx1).await.unwrap();
        plugin.on_transaction(0, None, &tx2).await.unwrap();

        let events = PubkeyStatsPlugin::take_slot_events(300, None);
        assert_eq!(events.len(), 2);
        let a_event = events.iter().find(|e| e.pubkey == key_a).unwrap();
        let b_event = events.iter().find(|e| e.pubkey == key_b).unwrap();
        assert_eq!(a_event.num_mentions, 2);
        assert_eq!(b_event.num_mentions, 1);
    }

    #[serial]
    #[tokio::test]
    async fn different_slots_are_independent() {
        clear_pending();
        let plugin = PubkeyStatsPlugin::new();
        let key = Address::from([42u8; 32]);

        let tx1 = make_tx(400, vec![key]);
        let tx2 = make_tx(401, vec![key]);

        plugin.on_transaction(0, None, &tx1).await.unwrap();
        plugin.on_transaction(0, None, &tx2).await.unwrap();

        let events_400 = PubkeyStatsPlugin::take_slot_events(400, None);
        let events_401 = PubkeyStatsPlugin::take_slot_events(401, None);
        assert_eq!(events_400.len(), 1);
        assert_eq!(events_401.len(), 1);
        assert_eq!(events_400[0].num_mentions, 1);
        assert_eq!(events_401[0].num_mentions, 1);
    }

    #[serial]
    #[tokio::test]
    async fn take_slot_events_drains_slot() {
        clear_pending();
        let plugin = PubkeyStatsPlugin::new();
        let tx = make_tx(500, vec![Address::from([1u8; 32])]);
        plugin.on_transaction(0, None, &tx).await.unwrap();

        let first = PubkeyStatsPlugin::take_slot_events(500, None);
        let second = PubkeyStatsPlugin::take_slot_events(500, None);
        assert_eq!(first.len(), 1);
        assert!(second.is_empty());
    }

    #[serial]
    #[tokio::test]
    async fn drain_all_pending_collects_all_slots() {
        clear_pending();
        let plugin = PubkeyStatsPlugin::new();

        let tx1 = make_tx(600, vec![Address::from([1u8; 32])]);
        let tx2 = make_tx(601, vec![Address::from([2u8; 32])]);
        let tx3 = make_tx(602, vec![Address::from([3u8; 32])]);

        plugin.on_transaction(0, None, &tx1).await.unwrap();
        plugin.on_transaction(0, None, &tx2).await.unwrap();
        plugin.on_transaction(0, None, &tx3).await.unwrap();

        let events = PubkeyStatsPlugin::drain_all_pending(Some(1_000));
        assert_eq!(events.len(), 3);
        assert!(PENDING_BY_SLOT.is_empty());
    }

    #[serial]
    #[tokio::test]
    async fn empty_account_keys_produces_no_events() {
        clear_pending();
        let plugin = PubkeyStatsPlugin::new();
        let tx = make_tx(700, vec![]);
        plugin.on_transaction(0, None, &tx).await.unwrap();
        assert!(PENDING_BY_SLOT.is_empty());
    }

    #[serial]
    #[tokio::test]
    async fn on_block_drains_pending_slot() {
        clear_pending();
        let plugin = PubkeyStatsPlugin::new();
        let tx = make_tx(800, vec![Address::from([1u8; 32])]);
        plugin.on_transaction(0, None, &tx).await.unwrap();
        assert!(!PENDING_BY_SLOT.is_empty());

        let block = make_block(800, Some(1_700_000_000));
        plugin.on_block(0, None, &block).await.unwrap();

        // on_block without db just drains, doesn't write
        assert!(!PENDING_BY_SLOT.contains_key(&800));
    }

    #[serial]
    #[tokio::test]
    async fn skipped_block_does_not_drain() {
        clear_pending();
        let plugin = PubkeyStatsPlugin::new();
        let tx = make_tx(900, vec![Address::from([1u8; 32])]);
        plugin.on_transaction(0, None, &tx).await.unwrap();

        let skipped = BlockData::PossibleLeaderSkipped { slot: 900 };
        plugin.on_block(0, None, &skipped).await.unwrap();

        assert!(PENDING_BY_SLOT.contains_key(&900));
        clear_pending();
    }

    #[test]
    fn plugin_name() {
        assert_eq!(PubkeyStatsPlugin::new().name(), "Pubkey Stats");
    }

    #[serial]
    #[test]
    fn slot_clamped_to_u32_max() {
        let slot = u64::from(u32::MAX) + 100;
        PENDING_BY_SLOT.clear();
        let inner = DashMap::with_hasher(ahash::RandomState::new());
        inner.insert(Address::from([1u8; 32]), 5);
        PENDING_BY_SLOT.insert(slot, inner);

        let events = PubkeyStatsPlugin::take_slot_events(slot, None);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].slot, u32::MAX);
    }
}

async fn backfill_pubkey_timestamps(db: Arc<Client>) -> Result<(), clickhouse::error::Error> {
    db.query(
        r#"
        INSERT INTO pubkey_mentions
        SELECT pm.slot,
               ss.block_time,
               pm.pubkey,
               pm.num_mentions
        FROM pubkey_mentions AS pm
        ANY INNER JOIN jetstreamer_slot_status AS ss USING (slot)
        WHERE pm.timestamp = toDateTime(0)
          AND ss.block_time > toDateTime(0)
        "#,
    )
    .execute()
    .await?;

    Ok(())
}
