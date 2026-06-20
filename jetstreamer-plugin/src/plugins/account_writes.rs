//! Per-slot account-write statistics — analytics the Old-Faithful path cannot
//! produce, because CAR archives carry no account updates. Horizon `.jet`
//! archives carry the full write stream, reached here through the records that
//! bundle it: transaction-owned writes via [`Transaction::iter_account_updates`]
//! and runtime-direct ("orphan") writes via the block's pre/post arenas.
//!
//! For each slot it records the number of account writes, how many distinct
//! accounts were written, and the total bytes of account data written.
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use ahash::RandomState;
use clickhouse::{Client, Row};
use futures_util::FutureExt;
use serde::{Deserialize, Serialize};
use solana_address::Address;

use jetstreamer_horizon::archive::{BlockNotification, EntryRecord};
use jetstreamer_horizon::transactions::Transaction;

use crate::PluginFuture;
use crate::horizon::{HorizonPlugin, Output, PluginWorker, clamp_block_time, clamp_slot};

#[derive(Row, Deserialize, Serialize, Copy, Clone, Debug)]
struct AccountWriteStats {
    slot: u32,
    timestamp: u32,
    writes: u64,
    distinct_accounts: u32,
    data_bytes: u64,
}

#[derive(Default)]
struct SlotAcc {
    writes: u64,
    data_bytes: u64,
    distinct: HashSet<Address, RandomState>,
}

fn tally(acc: &mut SlotAcc, pubkey: Address, len: usize) {
    acc.writes += 1;
    acc.data_bytes += len as u64;
    acc.distinct.insert(pubkey);
}

/// Per-thread worker: tallies the slot's account writes (transaction-owned in
/// `on_transaction`, orphan in `on_block`) and emits one row per block.
#[derive(Default)]
struct AccountWritesWorker {
    pending: HashMap<u64, SlotAcc, RandomState>,
    rows: Vec<AccountWriteStats>,
}

impl PluginWorker for AccountWritesWorker {
    fn on_transaction(&mut self, slot: u64, _tx_index: u32, tx: &Transaction) {
        let acc = self.pending.entry(slot).or_default();
        for (meta, data) in tx.iter_account_updates() {
            tally(acc, meta.pubkey, data.len());
        }
    }

    fn on_block(&mut self, notification: &BlockNotification, _entries: &[EntryRecord]) {
        let BlockNotification::Block(meta) = notification else {
            return; // leader-skipped: no block, no writes
        };
        {
            let acc = self.pending.entry(meta.slot).or_default();
            for (m, d) in meta.pre_updates.iter() {
                tally(acc, m.pubkey, d.len());
            }
            for (m, d) in meta.post_updates.iter() {
                tally(acc, m.pubkey, d.len());
            }
        }
        let acc = self.pending.remove(&meta.slot).unwrap_or_default();
        self.rows.push(AccountWriteStats {
            slot: clamp_slot(meta.slot),
            timestamp: clamp_block_time(meta.block_time),
            writes: acc.writes,
            distinct_accounts: acc.distinct.len() as u32,
            data_bytes: acc.data_bytes,
        });
    }

    fn flush(&mut self, out: &Output) {
        if self.rows.is_empty() {
            return;
        }
        let rows = std::mem::take(&mut self.rows);
        let db = out.db();
        out.submit(async move {
            let mut insert = db.insert::<AccountWriteStats>("account_write_stats").await?;
            for row in &rows {
                insert.write(row).await?;
            }
            insert.end().await
        });
    }
}

/// Per-slot account-write statistics, sourced from horizon's account-update
/// stream (unavailable to the Old-Faithful CAR path).
#[derive(Debug, Clone, Default)]
pub struct AccountWritesPlugin;

impl AccountWritesPlugin {
    /// Creates a new instance.
    pub const fn new() -> Self {
        Self
    }
}

impl HorizonPlugin for AccountWritesPlugin {
    fn name(&self) -> &'static str {
        "Account Writes (horizon)"
    }

    fn spawn_worker(&self, _thread_id: usize) -> Box<dyn PluginWorker> {
        Box::<AccountWritesWorker>::default()
    }

    fn on_start(&self, db: Arc<Client>, _epoch: u64) -> PluginFuture<'_> {
        async move {
            db.query(
                r#"
                CREATE TABLE IF NOT EXISTS account_write_stats (
                    slot               UInt32,
                    timestamp          DateTime('UTC'),
                    writes             UInt64,
                    distinct_accounts  UInt32,
                    data_bytes         UInt64
                )
                ENGINE = ReplacingMergeTree(timestamp)
                ORDER BY slot
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
                INSERT INTO account_write_stats
                SELECT aw.slot, ss.block_time, aw.writes, aw.distinct_accounts, aw.data_bytes
                FROM account_write_stats AS aw
                ANY INNER JOIN jetstreamer_slot_status AS ss USING (slot)
                WHERE aw.timestamp = toDateTime(0)
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
