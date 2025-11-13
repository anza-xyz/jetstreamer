use std::{collections::HashMap, sync::Arc};

use clickhouse::{Client, Row};
use dashmap::DashMap;
use futures_util::FutureExt;
use log::error;
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use solana_message::VersionedMessage;

use crate::{Plugin, PluginFuture};
use jetstreamer_firehose::firehose::{BlockData, TransactionData};

const DB_WRITE_INTERVAL_SLOTS: u64 = 1000;

#[derive(Default)]
struct ThreadLocalData {
    slot_stats: HashMap<u64, SlotInstructionStats>,
    pending_rows: Vec<SlotInstructionEvent>,
    slots_since_flush: u64,
}

static THREAD_DATA: Lazy<DashMap<usize, ThreadLocalData>> = Lazy::new(DashMap::new);

#[derive(Copy, Clone, Default)]
struct SlotInstructionStats {
    instruction_count: u64,
    transaction_count: u32,
    block_time: Option<i64>,
}

#[derive(Row, Deserialize, Serialize, Copy, Clone, Debug)]
struct SlotInstructionEvent {
    slot: u32,
    // Stored as ClickHouse DateTime('UTC') -> UInt32 seconds; clamp Solana's i64 timestamp.
    timestamp: u32,
    instruction_count: u64,
    transaction_count: u32,
}

#[derive(Debug, Default, Clone)]
/// Tracks total instructions executed per slot and batches writes to ClickHouse.
pub struct InstructionTrackingPlugin;

impl InstructionTrackingPlugin {
    fn with_thread_data<F, R>(thread_id: usize, f: F) -> R
    where
        F: FnOnce(&mut ThreadLocalData) -> R,
    {
        let mut guard = THREAD_DATA
            .entry(thread_id)
            .or_insert_with(ThreadLocalData::default);
        f(&mut *guard)
    }

    fn drain_all_rows(block_time: Option<i64>) -> Vec<SlotInstructionEvent> {
        let mut rows = Vec::new();
        for mut entry in THREAD_DATA.iter_mut() {
            rows.extend(Self::flush_data(&mut *entry, block_time));
        }
        rows
    }

    fn flush_data(
        data: &mut ThreadLocalData,
        block_time: Option<i64>,
    ) -> Vec<SlotInstructionEvent> {
        let mut rows = std::mem::take(&mut data.pending_rows);
        rows.extend(
            data.slot_stats
                .drain()
                .map(|(slot, stats)| event_from_slot(slot, block_time.or(stats.block_time), stats)),
        );
        rows
    }
}

impl Plugin for InstructionTrackingPlugin {
    #[inline(always)]
    fn name(&self) -> &'static str {
        "Instruction Tracking"
    }

    #[inline(always)]
    fn on_transaction<'a>(
        &'a self,
        thread_id: usize,
        _db: Option<Arc<Client>>,
        transaction: &'a TransactionData,
    ) -> PluginFuture<'a> {
        async move {
            let instruction_count = total_instruction_count(transaction);

            Self::with_thread_data(thread_id, |data| {
                let entry = data.slot_stats.entry(transaction.slot).or_default();
                entry.instruction_count = entry.instruction_count.saturating_add(instruction_count);
                entry.transaction_count = entry.transaction_count.saturating_add(1);
            });

            Ok(())
        }
        .boxed()
    }

    #[inline(always)]
    fn on_block(
        &self,
        thread_id: usize,
        db: Option<Arc<Client>>,
        block: &BlockData,
    ) -> PluginFuture<'_> {
        let slot_info = match block {
            BlockData::Block {
                slot, block_time, ..
            } => Some((*slot, *block_time)),
            BlockData::LeaderSkipped { .. } => None,
        };

        async move {
            let Some((slot, block_time)) = slot_info else {
                return Ok(());
            };

            let flush_rows = Self::with_thread_data(thread_id, |data| {
                let mut stats = data.slot_stats.remove(&slot).unwrap_or_default();
                if stats.block_time.is_none() {
                    stats.block_time = block_time;
                }
                let timestamp_source = stats.block_time.or(block_time);
                data.pending_rows
                    .push(event_from_slot(slot, timestamp_source, stats));
                data.slots_since_flush = data.slots_since_flush.saturating_add(1);
                if data.slots_since_flush >= DB_WRITE_INTERVAL_SLOTS {
                    data.slots_since_flush = 0;
                    if data.pending_rows.is_empty() {
                        None
                    } else {
                        Some(data.pending_rows.drain(..).collect::<Vec<_>>())
                    }
                } else {
                    None
                }
            });

            if let (Some(db_client), Some(rows)) = (db.clone(), flush_rows) {
                tokio::spawn(async move {
                    if let Err(err) = write_instruction_events(db_client, rows).await {
                        error!("failed to flush instruction rows: {}", err);
                    }
                });
            }

            if let Some(db_client) = db {
                let slot_to_update = slot;
                tokio::spawn(async move {
                    if let Err(err) =
                        backfill_instruction_timestamp(db_client, slot_to_update).await
                    {
                        error!("failed to backfill instruction timestamp: {}", err);
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
            log::info!("Instruction Tracking Plugin loaded.");
            if let Some(db) = db {
                log::info!("Ensuring slot_instructions table exists...");
                db.query(
                    r#"
                    CREATE TABLE IF NOT EXISTS slot_instructions (
                        slot               UInt32,
                        timestamp          DateTime('UTC'),
                        instruction_count  UInt64,
                        transaction_count  UInt32
                    )
                    ENGINE = ReplacingMergeTree(slot)
                    ORDER BY slot
                    "#,
                )
                .execute()
                .await?;
                log::info!("done.");
            } else {
                log::warn!(
                    "Instruction Tracking Plugin running without ClickHouse; data will not be persisted."
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
                let rows = Self::drain_all_rows(None);
                if !rows.is_empty()
                    && let Err(err) = write_instruction_events(db_client, rows).await
                {
                    error!("failed to flush instruction rows on exit: {}", err);
                }
            }
            Ok(())
        }
        .boxed()
    }
}

async fn write_instruction_events(
    db: Arc<Client>,
    rows: Vec<SlotInstructionEvent>,
) -> Result<(), clickhouse::error::Error> {
    if rows.is_empty() {
        return Ok(());
    }
    let mut insert = db
        .insert::<SlotInstructionEvent>("slot_instructions")
        .await?;
    for row in rows {
        insert.write(&row).await?;
    }
    insert.end().await?;
    Ok(())
}

fn event_from_slot(
    slot: u64,
    block_time: Option<i64>,
    stats: SlotInstructionStats,
) -> SlotInstructionEvent {
    let timestamp = clamp_block_time(block_time);
    SlotInstructionEvent {
        slot: slot.min(u32::MAX as u64) as u32,
        timestamp,
        instruction_count: stats.instruction_count,
        transaction_count: stats.transaction_count,
    }
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

fn total_instruction_count(transaction: &TransactionData) -> u64 {
    let message_instructions = match &transaction.transaction.message {
        VersionedMessage::Legacy(msg) => msg.instructions.len() as u64,
        VersionedMessage::V0(msg) => msg.instructions.len() as u64,
    };
    let inner_instruction_count = transaction
        .transaction_status_meta
        .inner_instructions
        .as_ref()
        .map(|sets| {
            sets.iter()
                .map(|set| set.instructions.len() as u64)
                .sum::<u64>()
        })
        .unwrap_or(0);
    message_instructions.saturating_add(inner_instruction_count)
}

async fn backfill_instruction_timestamp(
    db: Arc<Client>,
    slot: u64,
) -> Result<(), clickhouse::error::Error> {
    let Some(block_time) = fetch_slot_block_time(db.as_ref(), slot).await? else {
        return Ok(());
    };

    db.query(
        r#"
        INSERT INTO slot_instructions
        SELECT slot,
               toDateTime(?),
               instruction_count,
               transaction_count
        FROM slot_instructions
        WHERE slot = ? AND timestamp = toDateTime(0)
        "#,
    )
    .bind(block_time)
    .bind(slot)
    .execute()
    .await
}

#[derive(Row, Deserialize)]
struct SlotBlockTime {
    block_time: u32,
}

async fn fetch_slot_block_time(
    db: &Client,
    slot: u64,
) -> Result<Option<u32>, clickhouse::error::Error> {
    let result = db
        .query(
            r#"
            SELECT toUInt32(block_time) AS block_time
            FROM jetstreamer_slot_status
            WHERE slot = ? AND block_time > toDateTime(0)
            ORDER BY block_time DESC
            LIMIT 1
            "#,
        )
        .bind(slot)
        .fetch_optional::<SlotBlockTime>()
        .await?;

    Ok(result.map(|row| row.block_time))
}
