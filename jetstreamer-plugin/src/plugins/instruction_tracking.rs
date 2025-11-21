use std::{collections::HashMap, sync::Arc};

use clickhouse::{Client, Row};
use dashmap::DashMap;
use futures_util::FutureExt;
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use solana_message::VersionedMessage;

use crate::{Plugin, PluginFuture};
use jetstreamer_firehose::firehose::{BlockData, FirehoseErrorContext, TransactionData};

const DB_WRITE_INTERVAL_SLOTS: u64 = 1;
const OPTIMIZE_INTERVAL_SLOTS: u64 = 100;

#[derive(Default)]
struct ThreadData {
    slot_stats: HashMap<u64, SlotInstructionStats>,
    pending_rows: Vec<SlotInstructionEvent>,
    slots_since_flush: u64,
    slots_since_optimize: u64,
}

static THREAD_DATA: Lazy<DashMap<usize, ThreadData>> = Lazy::new(DashMap::new);

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
        F: FnOnce(&mut ThreadData) -> R,
    {
        let mut guard = THREAD_DATA.entry(thread_id).or_default();
        f(&mut guard)
    }

    fn drain_all_rows(block_time: Option<i64>) -> Vec<SlotInstructionEvent> {
        let mut rows = Vec::new();
        for mut entry in THREAD_DATA.iter_mut() {
            rows.extend(Self::flush_data(&mut entry, block_time));
        }
        rows
    }

    fn drain_thread_rows(thread_id: usize, block_time: Option<i64>) -> Vec<SlotInstructionEvent> {
        Self::with_thread_data(thread_id, |data| {
            let rows = Self::flush_data(data, block_time);
            data.slots_since_flush = 0;
            data.slots_since_optimize = 0;
            rows
        })
    }

    fn flush_data(data: &mut ThreadData, block_time: Option<i64>) -> Vec<SlotInstructionEvent> {
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
        let slot = block.slot();
        let block_time = block.block_time();
        let was_skipped = block.was_skipped();

        async move {
            if was_skipped {
                return Ok(());
            }

            let (flush_rows, should_optimize) = Self::with_thread_data(thread_id, |data| {
                let mut do_optimize = false;
                let mut flush_rows: Option<Vec<SlotInstructionEvent>> = None;
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
                    if !data.pending_rows.is_empty() {
                        flush_rows = Some(data.pending_rows.drain(..).collect::<Vec<_>>());
                    }
                }
                data.slots_since_optimize = data.slots_since_optimize.saturating_add(1);
                if data.slots_since_optimize >= OPTIMIZE_INTERVAL_SLOTS {
                    data.slots_since_optimize = 0;
                    do_optimize = true;
                }
                (flush_rows, do_optimize)
            });

            if let (Some(db_client), Some(rows)) = (db.as_ref(), flush_rows) {
                if let Err(err) = write_instruction_events(Arc::clone(db_client), &rows).await {
                    Self::with_thread_data(thread_id, |data| {
                        data.pending_rows.extend(rows);
                    });
                    return Err(Box::new(err) as Box<dyn std::error::Error + Send + Sync>);
                }
            }

            if let (Some(db_client), true) = (db.as_ref(), should_optimize) {
                if let Err(err) = optimize_slot_instructions(Arc::clone(db_client)).await {
                    log::warn!(
                        "failed to optimize slot_instructions after slot {}: {}",
                        slot,
                        err
                    );
                }
            }

            Ok(())
        }
        .boxed()
    }

    #[inline(always)]
    fn on_error<'a>(
        &'a self,
        thread_id: usize,
        db: Option<Arc<Client>>,
        _error: &'a FirehoseErrorContext,
    ) -> PluginFuture<'a> {
        async move {
            let rows = Self::drain_thread_rows(thread_id, None);
            if let Some(db_client) = db {
                if !rows.is_empty() {
                    if let Err(err) = write_instruction_events(Arc::clone(&db_client), &rows).await
                    {
                        Self::with_thread_data(thread_id, |data| {
                            data.pending_rows.extend(rows);
                        });
                        return Err(Box::new(err) as Box<dyn std::error::Error + Send + Sync>);
                    }
                }
            } else if !rows.is_empty() {
                Self::with_thread_data(thread_id, |data| data.pending_rows.extend(rows));
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
                    ENGINE = ReplacingMergeTree(timestamp)
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
            if let Some(db_client) = db.clone() {
                let rows = Self::drain_all_rows(None);
                if !rows.is_empty() {
                    write_instruction_events(Arc::clone(&db_client), &rows)
                        .await
                        .map_err(|err| -> Box<dyn std::error::Error + Send + Sync> {
                            Box::new(err)
                        })?;
                }
                backfill_instruction_timestamps(db_client)
                    .await
                    .map_err(|err| -> Box<dyn std::error::Error + Send + Sync> { Box::new(err) })?;
            }
            if let Some(db_client) = db {
                if let Err(err) = optimize_slot_instructions(db_client).await {
                    log::warn!("failed to optimize slot_instructions at exit: {}", err);
                }
            }
            Ok(())
        }
        .boxed()
    }
}

async fn write_instruction_events(
    db: Arc<Client>,
    rows: &[SlotInstructionEvent],
) -> Result<(), clickhouse::error::Error> {
    if rows.is_empty() {
        return Ok(());
    }
    let mut insert = db
        .insert::<SlotInstructionEvent>("slot_instructions")
        .await?;
    for row in rows {
        insert.write(row).await?;
    }
    insert.end().await?;
    Ok(())
}

async fn optimize_slot_instructions(db: Arc<Client>) -> Result<(), clickhouse::error::Error> {
    db.query("OPTIMIZE TABLE slot_instructions FINAL")
        .execute()
        .await?;
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

async fn backfill_instruction_timestamps(db: Arc<Client>) -> Result<(), clickhouse::error::Error> {
    db.query(
        r#"
        INSERT INTO slot_instructions
        SELECT si.slot,
               ss.block_time,
               si.instruction_count,
               si.transaction_count
        FROM slot_instructions AS si
        ANY INNER JOIN jetstreamer_slot_status AS ss USING (slot)
        WHERE si.timestamp = toDateTime(0)
          AND ss.block_time > toDateTime(0)
        "#,
    )
    .execute()
    .await?;

    Ok(())
}
