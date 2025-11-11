use std::{cell::RefCell, collections::HashMap, sync::Arc};

use clickhouse::{Client, Row};
use futures_util::FutureExt;
use log::error;
use serde::{Deserialize, Serialize};
use solana_message::VersionedMessage;

use crate::{Plugin, PluginFuture};
use jetstreamer_firehose::firehose::{BlockData, FirehoseErrorContext, TransactionData};

const DB_WRITE_INTERVAL_SLOTS: u64 = 1000;

#[derive(Default)]
struct ThreadLocalData {
    slot_stats: HashMap<u64, SlotInstructionStats>,
    pending_rows: Vec<SlotInstructionEvent>,
    slots_since_flush: u64,
}

thread_local! {
    static DATA: RefCell<ThreadLocalData> = RefCell::new(ThreadLocalData::default());
}

#[derive(Copy, Clone, Default)]
struct SlotInstructionStats {
    instruction_count: u64,
    transaction_count: u32,
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
    fn drain_thread_rows(block_time: Option<i64>) -> Vec<SlotInstructionEvent> {
        DATA.with(|data| {
            let mut data = data.borrow_mut();
            let mut rows = std::mem::take(&mut data.pending_rows);
            rows.extend(
                data.slot_stats
                    .drain()
                    .map(|(slot, stats)| event_from_slot(slot, block_time, stats)),
            );
            rows
        })
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

            DATA.with(|data| {
                let mut data = data.borrow_mut();
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
        _thread_id: usize,
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

            let flush_rows = DATA.with(|data| {
                let mut data = data.borrow_mut();
                if let Some(stats) = data.slot_stats.remove(&slot) {
                    data.pending_rows
                        .push(event_from_slot(slot, block_time, stats));
                }
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

            if let (Some(db_client), Some(rows)) = (db, flush_rows) {
                tokio::spawn(async move {
                    if let Err(err) = write_instruction_events(db_client, rows).await {
                        error!("failed to flush instruction rows: {}", err);
                    }
                });
            }

            Ok(())
        }
        .boxed()
    }

    #[inline(always)]
    fn on_load(&self, db: Option<Arc<Client>>) -> PluginFuture<'_> {
        DATA.with(|_| {});
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
                let rows = Self::drain_thread_rows(None);
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

    #[inline(always)]
    fn on_error<'a>(
        &'a self,
        _thread_id: usize,
        db: Option<Arc<Client>>,
        _error: &'a FirehoseErrorContext,
    ) -> PluginFuture<'a> {
        async move {
            if let Some(db_client) = db {
                let rows = Self::drain_thread_rows(None);
                if !rows.is_empty()
                    && let Err(err) = write_instruction_events(db_client, rows).await
                {
                    error!("failed to flush instruction rows after error: {}", err);
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
