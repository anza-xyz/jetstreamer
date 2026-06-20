//! Horizon-native port of [`PubkeyStatsPlugin`](super::pubkey_stats::PubkeyStatsPlugin).
//!
//! Counts, per slot, how many times each account key is referenced across the
//! slot's transaction messages, and writes the `(slot, pubkey, num_mentions)`
//! rows to the same `pubkey_mentions` table — so it can be compared directly
//! against the Old-Faithful plugin for parity. Each reader thread owns its own
//! per-slot accumulator (no shared map, no locks).
use std::collections::HashMap;
use std::sync::Arc;

use ahash::RandomState;
use clickhouse::{Client, Row};
use futures_util::FutureExt;
use serde::{Deserialize, Serialize};
use solana_address::Address;

use jetstreamer_horizon::archive::BlockNotification;
use jetstreamer_horizon::transactions::{Transaction, VersionedMessage};

use crate::PluginFuture;
use crate::horizon::{HorizonPlugin, Output, PluginWorker, clamp_block_time, clamp_slot};

#[derive(Row, Deserialize, Serialize, Copy, Clone, Debug)]
struct PubkeyMention {
    slot: u32,
    timestamp: u32,
    pubkey: Address,
    num_mentions: u32,
}

/// Per-thread worker: accumulates pubkey mention counts per slot, finalizing a
/// slot's rows when its block notification arrives (which carries `block_time`).
#[derive(Default)]
struct PubkeyStatsWorker {
    pending: HashMap<u64, HashMap<Address, u32, RandomState>, RandomState>,
    rows: Vec<PubkeyMention>,
}

impl PluginWorker for PubkeyStatsWorker {
    fn on_transaction(&mut self, slot: u64, _tx_index: u32, tx: &Transaction) {
        let keys = match &tx.message {
            VersionedMessage::Legacy(m) => m.account_keys.as_slice(),
            VersionedMessage::V0(m) => m.account_keys.as_slice(),
        };
        if keys.is_empty() {
            return;
        }
        let counts = self
            .pending
            .entry(slot)
            .or_insert_with(|| HashMap::with_hasher(RandomState::new()));
        for key in keys {
            *counts.entry(*key).or_insert(0) += 1;
        }
    }

    fn on_block(&mut self, notification: &BlockNotification, _entries: &[jetstreamer_horizon::archive::EntryRecord]) {
        if let BlockNotification::Block(meta) = notification
            && let Some(counts) = self.pending.remove(&meta.slot)
        {
            let slot = clamp_slot(meta.slot);
            let timestamp = clamp_block_time(meta.block_time);
            self.rows
                .extend(counts.into_iter().map(|(pubkey, num_mentions)| PubkeyMention {
                    slot,
                    timestamp,
                    pubkey,
                    num_mentions,
                }));
        }
    }

    fn flush(&mut self, out: &Output) {
        if self.rows.is_empty() {
            return;
        }
        let rows = std::mem::take(&mut self.rows);
        let db = out.db();
        out.submit(async move {
            let mut insert = db.insert::<PubkeyMention>("pubkey_mentions").await?;
            for row in &rows {
                insert.write(row).await?;
            }
            insert.end().await
        });
    }
}

/// Horizon-native pubkey mention tracker. Writes the same `pubkey_mentions`
/// schema as the Old-Faithful [`PubkeyStatsPlugin`](super::pubkey_stats::PubkeyStatsPlugin).
#[derive(Debug, Clone, Default)]
pub struct PubkeyStatsHorizonPlugin;

impl PubkeyStatsHorizonPlugin {
    /// Creates a new instance.
    pub const fn new() -> Self {
        Self
    }
}

impl HorizonPlugin for PubkeyStatsHorizonPlugin {
    fn name(&self) -> &'static str {
        "Pubkey Stats (horizon)"
    }

    fn spawn_worker(&self, _thread_id: usize) -> Box<dyn PluginWorker> {
        Box::<PubkeyStatsWorker>::default()
    }

    fn on_start(&self, db: Arc<Client>, _epoch: u64) -> PluginFuture<'_> {
        async move {
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
            db.query(
                r#"
                CREATE MATERIALIZED VIEW IF NOT EXISTS pubkeys_mv TO pubkeys AS
                SELECT pubkey, sipHash64(pubkey) AS id
                FROM pubkey_mentions
                GROUP BY pubkey
                "#,
            )
            .execute()
            .await?;
            Ok(())
        }
        .boxed()
    }

    fn on_finish(&self, db: Arc<Client>, _epoch: u64) -> PluginFuture<'_> {
        async move {
            db.query(
                r#"
                INSERT INTO pubkey_mentions
                SELECT pm.slot, ss.block_time, pm.pubkey, pm.num_mentions
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
        .boxed()
    }
}
