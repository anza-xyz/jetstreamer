use std::{cell::RefCell, collections::HashMap, sync::Arc};

use clickhouse::{Client, Row};
use futures_util::FutureExt;
use log::error;
use serde::{Deserialize, Serialize};
use solana_sdk::{message::VersionedMessage, pubkey::Pubkey};

use crate::{Plugin, PluginFuture};
use jetstreamer_firehose::firehose::{BlockData, TransactionData};

const DB_WRITE_INTERVAL_SLOTS: u64 = 1000;

#[derive(Default)]
struct ThreadLocalData {
    slot_stats: HashMap<u64, HashMap<Pubkey, ProgramStats>>,
    pending_rows: Vec<ProgramEvent>,
    slots_since_flush: u64,
}

thread_local! {
    static DATA: RefCell<ThreadLocalData> = RefCell::new(ThreadLocalData::default());
}

#[derive(Row, Deserialize, Serialize, Copy, Clone, Debug, PartialEq, Eq, Hash)]
struct ProgramEvent {
    pub slot: u32,
    // Stored as ClickHouse DateTime('UTC') -> UInt32 seconds; we clamp Solana i64.
    pub timestamp: u32,
    pub program_id: Pubkey,
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
pub struct ProgramTrackingPlugin;

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

            DATA.with(|data| {
                let mut data = data.borrow_mut();
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

            if let (Some(db_client), Some(rows)) = (db, flush_rows) {
                tokio::spawn(async move {
                    if let Err(err) = write_program_events(db_client, rows).await {
                        error!("failed to flush program rows: {}", err);
                    }
                });
            }

            Ok(())
        }
        .boxed()
    }

    #[inline(always)]
    fn on_load(&self, db: Option<Arc<Client>>) -> PluginFuture<'_> {
        // Remove invalid `get_or_init` call in `on_load`
        DATA.with(|_| {});
        // SLOT_TIMESTAMPS is a Lazy global, nothing to initialize
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
                    ENGINE = ReplacingMergeTree(slot)
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
                let rows = DATA.with(|data| {
                    let mut data = data.borrow_mut();
                    let mut rows = std::mem::take(&mut data.pending_rows);
                    for (slot, stats) in data.slot_stats.drain() {
                        rows.extend(events_from_slot(slot, None, &stats));
                    }
                    rows
                });
                if !rows.is_empty() {
                    if let Err(err) = write_program_events(db_client, rows).await {
                        error!("failed to flush program rows on exit: {}", err);
                    }
                }
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
    let mut insert = db.insert("program_invocations")?;
    for row in rows {
        insert.write(&row).await?;
    }
    insert.end().await?;
    Ok(())
}

fn events_from_slot(
    slot: u64,
    block_time: Option<i64>,
    slot_data: &HashMap<Pubkey, ProgramStats>,
) -> Vec<ProgramEvent> {
    let raw_ts = block_time.unwrap_or(0);
    let timestamp: u32 = if raw_ts < 0 {
        0
    } else if raw_ts > u32::MAX as i64 {
        u32::MAX
    } else {
        raw_ts as u32
    };

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
