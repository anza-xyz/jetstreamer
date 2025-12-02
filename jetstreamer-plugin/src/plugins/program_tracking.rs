use std::{collections::HashMap, sync::Arc};

use clickhouse::{Client, Row};
use dashmap::DashMap;
use futures_util::FutureExt;
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use solana_address::Address;
use solana_message::VersionedMessage;

use crate::{Plugin, PluginFuture};
use jetstreamer_firehose::firehose::{BlockData, TransactionData};

type SlotProgramKey = (Address, bool);
type SlotProgramEvents = HashMap<SlotProgramKey, ProgramEvent>;

static PENDING_BY_SLOT: Lazy<DashMap<u64, SlotProgramEvents>> = Lazy::new(DashMap::new);

#[derive(Row, Deserialize, Serialize, Copy, Clone, Debug, PartialEq, Eq, Hash)]
struct ProgramEvent {
    pub slot: u32,
    // Stored as ClickHouse DateTime('UTC') -> UInt32 seconds; we clamp Solana i64.
    pub timestamp: u32,
    pub program_id: Address,
    pub is_vote: bool,
    pub count: u32,
    pub error_count: u32,
    pub min_cus: u32,
    pub max_cus: u32,
    pub total_cus: u32,
}

#[derive(Debug, Clone)]
/// Tracks per-program invocation counts (including vote transactions) and writes them to ClickHouse.
pub struct ProgramTrackingPlugin;

impl ProgramTrackingPlugin {
    /// Creates a new instance that records both vote and non-vote transactions.
    pub const fn new() -> Self {
        Self
    }

    fn take_slot_events(slot: u64, block_time: Option<i64>) -> Vec<ProgramEvent> {
        let timestamp = clamp_block_time(block_time);
        if let Some((_, events_by_program)) = PENDING_BY_SLOT.remove(&slot) {
            return events_by_program
                .into_values()
                .map(|mut event| {
                    event.timestamp = timestamp;
                    event
                })
                .collect();
        }
        Vec::new()
    }

    fn drain_all_pending(block_time: Option<i64>) -> Vec<ProgramEvent> {
        let timestamp = clamp_block_time(block_time);
        let slots: Vec<u64> = PENDING_BY_SLOT.iter().map(|entry| *entry.key()).collect();
        let mut rows = Vec::new();
        for slot in slots {
            if let Some((_, events_by_program)) = PENDING_BY_SLOT.remove(&slot) {
                rows.extend(events_by_program.into_values().map(|mut event| {
                    event.timestamp = timestamp;
                    event
                }));
            }
        }
        rows
    }
}

impl Default for ProgramTrackingPlugin {
    fn default() -> Self {
        Self::new()
    }
}

impl Plugin for ProgramTrackingPlugin {
    #[inline(always)]
    fn name(&self) -> &'static str {
        "Program Tracking"
    }

    #[inline(always)]
    fn on_transaction<'a>(
        &'a self,
        _thread_id: usize,
        _db: Option<Arc<Client>>,
        transaction: &'a TransactionData,
    ) -> PluginFuture<'a> {
        async move {
            let message = &transaction.transaction.message;
            let (account_keys, instructions) = match message {
                VersionedMessage::Legacy(msg) => (&msg.account_keys, &msg.instructions),
                VersionedMessage::V0(msg) => (&msg.account_keys, &msg.instructions),
            };
            if instructions.is_empty() {
                return Ok(());
            }
            let program_ids = instructions
                .iter()
                .filter_map(|ix| account_keys.get(ix.program_id_index as usize))
                .cloned()
                .collect::<Vec<_>>();
            if program_ids.is_empty() {
                return Ok(());
            }
            let total_cu = transaction
                .transaction_status_meta
                .compute_units_consumed
                .unwrap_or(0) as u32;
            let program_count = program_ids.len() as u32;
            let errored = transaction.transaction_status_meta.status.is_err();
            let slot = transaction.slot;
            let is_vote = transaction.is_vote;

            let mut slot_entry = PENDING_BY_SLOT.entry(slot).or_default();
            for program_id in program_ids.iter() {
                let this_program_cu = if program_count == 0 {
                    0
                } else {
                    total_cu / program_count
                };
                let event =
                    slot_entry
                        .entry((*program_id, is_vote))
                        .or_insert_with(|| ProgramEvent {
                            slot: slot.min(u32::MAX as u64) as u32,
                            timestamp: 0,
                            program_id: *program_id,
                            is_vote,
                            count: 0,
                            error_count: 0,
                            min_cus: u32::MAX,
                            max_cus: 0,
                            total_cus: 0,
                        });
                event.min_cus = event.min_cus.min(this_program_cu);
                event.max_cus = event.max_cus.max(this_program_cu);
                event.total_cus = event.total_cus.saturating_add(this_program_cu);
                event.count = event.count.saturating_add(1);
                if errored {
                    event.error_count = event.error_count.saturating_add(1);
                }
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

            if let Some(db_client) = db.as_ref()
                && !rows.is_empty()
            {
                write_program_events(Arc::clone(db_client), rows)
                    .await
                    .map_err(|err| -> Box<dyn std::error::Error + Send + Sync> { Box::new(err) })?;
            }

            Ok(())
        }
        .boxed()
    }

    #[inline(always)]
    fn on_load(&self, db: Option<Arc<Client>>) -> PluginFuture<'_> {
        async move {
            log::info!("Program Tracking Plugin loaded.");
            if let Some(db) = db {
                log::info!("Creating program_invocations table if it does not exist...");
                db.query(
                    r#"
                    CREATE TABLE IF NOT EXISTS program_invocations (
                        slot        UInt32,
                        timestamp   DateTime('UTC'),
                        program_id  FixedString(32),
                        is_vote     UInt8,
                        count       UInt32,
                        error_count UInt32,
                        min_cus     UInt32,
                        max_cus     UInt32,
                        total_cus   UInt32
                    )
                    ENGINE = ReplacingMergeTree(timestamp)
                    ORDER BY (slot, program_id, is_vote)
                    "#,
                )
                .execute()
                .await?;
                log::info!("done.");
            } else {
                log::warn!("Program Tracking Plugin running without ClickHouse; data will not be persisted.");
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
                    write_program_events(Arc::clone(&db_client), rows)
                        .await
                        .map_err(|err| -> Box<dyn std::error::Error + Send + Sync> {
                            Box::new(err)
                        })?;
                }
                backfill_program_timestamps(db_client)
                    .await
                    .map_err(|err| -> Box<dyn std::error::Error + Send + Sync> { Box::new(err) })?;
            }
            Ok(())
        }
        .boxed()
    }
}

async fn write_program_events(
    db: Arc<Client>,
    rows: Vec<ProgramEvent>,
) -> Result<(), clickhouse::error::Error> {
    if rows.is_empty() {
        return Ok(());
    }
    let mut insert = db.insert::<ProgramEvent>("program_invocations").await?;
    for row in rows {
        insert.write(&row).await?;
    }
    insert.end().await?;
    Ok(())
}

fn clamp_block_time(block_time: Option<i64>) -> u32 {
    let raw_ts = block_time.unwrap_or(0);
    if raw_ts < 0 {
        0
    } else if raw_ts > u32::MAX as i64 {
        u32::MAX
    } else {
        raw_ts as u32
    }
}

async fn backfill_program_timestamps(db: Arc<Client>) -> Result<(), clickhouse::error::Error> {
    db.query(
        r#"
        INSERT INTO program_invocations
        SELECT pi.slot,
               ss.block_time,
               pi.program_id,
               pi.is_vote,
               pi.count,
               pi.error_count,
               pi.min_cus,
               pi.max_cus,
               pi.total_cus
        FROM program_invocations AS pi
        ANY INNER JOIN jetstreamer_slot_status AS ss USING (slot)
        WHERE pi.timestamp = toDateTime(0)
          AND ss.block_time > toDateTime(0)
        "#,
    )
    .execute()
    .await?;

    Ok(())
}
