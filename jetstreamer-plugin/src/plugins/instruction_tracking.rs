use std::sync::Arc;

use clickhouse::{Client, Row};
use dashmap::DashMap;
use futures_util::FutureExt;
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use solana_message::VersionedMessage;

use crate::{Plugin, PluginFuture};
use jetstreamer_firehose::firehose::{BlockData, TransactionData};

static PENDING_BY_SLOT: Lazy<DashMap<u64, SlotInstructionEvent>> = Lazy::new(DashMap::new);

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
    fn take_slot_event(slot: u64, block_time: Option<i64>) -> Option<SlotInstructionEvent> {
        let timestamp = clamp_block_time(block_time);
        PENDING_BY_SLOT.remove(&slot).map(|(_, mut event)| {
            event.timestamp = timestamp;
            event
        })
    }

    fn drain_all_pending(block_time: Option<i64>) -> Vec<SlotInstructionEvent> {
        let timestamp = clamp_block_time(block_time);
        let slots: Vec<u64> = PENDING_BY_SLOT.iter().map(|entry| *entry.key()).collect();
        let mut rows = Vec::new();
        for slot in slots {
            if let Some((_, mut event)) = PENDING_BY_SLOT.remove(&slot) {
                event.timestamp = timestamp;
                rows.push(event);
            }
        }
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
        _thread_id: usize,
        _db: Option<Arc<Client>>,
        transaction: &'a TransactionData,
    ) -> PluginFuture<'a> {
        async move {
            let instruction_count = total_instruction_count(transaction);

            let slot = transaction.slot;
            let mut entry = PENDING_BY_SLOT
                .entry(slot)
                .or_insert_with(|| SlotInstructionEvent {
                    slot: slot.min(u32::MAX as u64) as u32,
                    timestamp: 0,
                    instruction_count: 0,
                    transaction_count: 0,
                });
            entry.instruction_count = entry.instruction_count.saturating_add(instruction_count);
            entry.transaction_count = entry.transaction_count.saturating_add(1);

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

            let rows = Self::take_slot_event(slot, block_time)
                .into_iter()
                .collect::<Vec<_>>();

            if let Some(db_client) = db.as_ref() {
                if !rows.is_empty() {
                    write_instruction_events(Arc::clone(db_client), rows)
                        .await
                        .map_err(|err| -> Box<dyn std::error::Error + Send + Sync> {
                            Box::new(err)
                        })?;
                }
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
            if let Some(db_client) = db {
                let rows = Self::drain_all_pending(None);
                if !rows.is_empty() {
                    write_instruction_events(Arc::clone(&db_client), rows)
                        .await
                        .map_err(|err| -> Box<dyn std::error::Error + Send + Sync> {
                            Box::new(err)
                        })?;
                }
                backfill_instruction_timestamps(db_client)
                    .await
                    .map_err(|err| -> Box<dyn std::error::Error + Send + Sync> { Box::new(err) })?;
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
