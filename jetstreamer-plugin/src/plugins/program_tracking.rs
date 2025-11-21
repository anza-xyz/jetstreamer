use std::{collections::HashMap, sync::Arc};

use clickhouse::{Client, Row};
use dashmap::DashMap;
use futures_util::FutureExt;
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use solana_address::Address;
use solana_message::VersionedMessage;

use crate::{Plugin, PluginFuture};
use jetstreamer_firehose::firehose::{BlockData, FirehoseErrorContext, TransactionData};

const DB_WRITE_INTERVAL_SLOTS: u64 = 1;

#[derive(Default)]
struct ThreadData {
    slot_stats: HashMap<u64, HashMap<Address, ProgramStats>>,
    pending_rows: Vec<ProgramEvent>,
    slots_since_flush: u64,
}

static THREAD_DATA: Lazy<DashMap<usize, ThreadData>> = Lazy::new(DashMap::new);

#[derive(Row, Deserialize, Serialize, Copy, Clone, Debug, PartialEq, Eq, Hash)]
struct ProgramEvent {
    pub slot: u32,
    // Stored as ClickHouse DateTime('UTC') -> UInt32 seconds; we clamp Solana i64.
    pub timestamp: u32,
    pub program_id: Address,
    pub count: u32,
    pub error_count: u32,
    pub min_cus: u32,
    pub max_cus: u32,
    pub total_cus: u32,
}

#[derive(Copy, Clone, PartialEq, Eq, Hash, Debug)]
struct ProgramStats {
    pub count: u32,
    pub error_count: u32,
    pub min_cus: u32,
    pub max_cus: u32,
    pub total_cus: u32,
}

#[derive(Debug, Default, Clone)]
/// Tracks per-program invocation counts and writes them to ClickHouse.
pub struct ProgramTrackingPlugin;

impl ProgramTrackingPlugin {
    fn with_thread_data<F, R>(thread_id: usize, f: F) -> R
    where
        F: FnOnce(&mut ThreadData) -> R,
    {
        let mut guard = THREAD_DATA.entry(thread_id).or_default();
        f(&mut guard)
    }

    fn drain_all_rows(block_time: Option<i64>) -> Vec<ProgramEvent> {
        let mut rows = Vec::new();
        for mut entry in THREAD_DATA.iter_mut() {
            rows.extend(Self::flush_data(&mut entry, block_time));
        }
        rows
    }

    fn drain_thread_rows(thread_id: usize, block_time: Option<i64>) -> Vec<ProgramEvent> {
        Self::with_thread_data(thread_id, |data| {
            let rows = Self::flush_data(data, block_time);
            data.slots_since_flush = 0;
            rows
        })
    }

    fn flush_data(data: &mut ThreadData, block_time: Option<i64>) -> Vec<ProgramEvent> {
        let mut rows = std::mem::take(&mut data.pending_rows);
        for (slot, stats) in data.slot_stats.drain() {
            rows.extend(events_from_slot(slot, block_time, &stats));
        }
        rows
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
        thread_id: usize,
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

            Self::with_thread_data(thread_id, |data| {
                let slot_data = data.slot_stats.entry(transaction.slot).or_default();

                for program_id in program_ids.iter() {
                    let this_program_cu = if program_count == 0 {
                        0
                    } else {
                        total_cu / program_count
                    };
                    let stats = slot_data.entry(*program_id).or_insert(ProgramStats {
                        min_cus: u32::MAX,
                        max_cus: 0,
                        total_cus: 0,
                        count: 0,
                        error_count: 0,
                    });
                    stats.min_cus = stats.min_cus.min(this_program_cu);
                    stats.max_cus = stats.max_cus.max(this_program_cu);
                    stats.total_cus += this_program_cu;
                    stats.count += 1;
                    if transaction.transaction_status_meta.status.is_err() {
                        stats.error_count += 1;
                    }
                }
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

            let flush_rows = Self::with_thread_data(thread_id, |data| {
                if let Some(slot_data) = data.slot_stats.remove(&slot) {
                    let slot_rows = events_from_slot(slot, block_time, &slot_data);
                    data.pending_rows.extend(slot_rows);
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

            if let (Some(db_client), Some(rows)) = (db.as_ref(), flush_rows) {
                if let Err(err) = write_program_events(Arc::clone(db_client), &rows).await {
                    Self::with_thread_data(thread_id, |data| {
                        data.pending_rows.extend(rows);
                    });
                    return Err(Box::new(err) as Box<dyn std::error::Error + Send + Sync>);
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
                    if let Err(err) = write_program_events(Arc::clone(&db_client), &rows).await {
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
            log::info!("Program Tracking Plugin loaded.");
            if let Some(db) = db {
                log::info!("Creating program_invocations table if it does not exist...");
                db.query(
                    r#"
                    CREATE TABLE IF NOT EXISTS program_invocations (
                        slot        UInt32,
                        timestamp   DateTime('UTC'),
                        program_id  FixedString(32),
                        count       UInt32,
                        error_count UInt32,
                        min_cus     UInt32,
                        max_cus     UInt32,
                        total_cus   UInt32
                    )
                    ENGINE = ReplacingMergeTree(timestamp)
                    ORDER BY (slot, program_id)
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
                let rows = Self::drain_all_rows(None);
                if !rows.is_empty() {
                    write_program_events(Arc::clone(&db_client), &rows)
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
    rows: &[ProgramEvent],
) -> Result<(), clickhouse::error::Error> {
    if rows.is_empty() {
        return Ok(());
    }
    let mut insert = db.insert::<ProgramEvent>("program_invocations").await?;
    for row in rows {
        insert.write(row).await?;
    }
    insert.end().await?;
    Ok(())
}

fn events_from_slot(
    slot: u64,
    block_time: Option<i64>,
    slot_data: &HashMap<Address, ProgramStats>,
) -> Vec<ProgramEvent> {
    let timestamp = clamp_block_time(block_time);

    slot_data
        .iter()
        .map(|(program_id, stats)| ProgramEvent {
            slot: slot.min(u32::MAX as u64) as u32,
            program_id: *program_id,
            count: stats.count,
            error_count: stats.error_count,
            min_cus: stats.min_cus,
            max_cus: stats.max_cus,
            total_cus: stats.total_cus,
            timestamp,
        })
        .collect()
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
