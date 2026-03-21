//! Jetstreamer [`Plugin`] implementation that decodes DEX swaps into protobuf.

use std::sync::Arc;

use dashmap::DashMap;
use futures_util::FutureExt;
use jetstreamer_firehose::firehose::{BlockData, TransactionData};
use jetstreamer_plugin::{Plugin, PluginFuture};
use once_cell::sync::Lazy;
use prost::Message;

use crate::proto::DexTradesBatch;
use crate::registry::DexRegistry;
use crate::types::SwapRecord;

/// Pending swap records keyed by slot, waiting for block_time from `on_block`.
static PENDING_BY_SLOT: Lazy<DashMap<u64, Vec<SwapRecord>>> = Lazy::new(DashMap::new);

/// Global registry shared across threads.
static REGISTRY: Lazy<DexRegistry> = Lazy::new(DexRegistry::new);

/// When `DEX_TRADES_DUMP=1` is set, each decoded swap is printed to stdout.
static DUMP_ENABLED: Lazy<bool> = Lazy::new(|| {
    std::env::var("DEX_TRADES_DUMP")
        .map(|v| matches!(v.as_str(), "1" | "true" | "yes"))
        .unwrap_or(false)
});

/// DEX trades decoder plugin for Jetstreamer.
///
/// Decodes swap transactions from 28+ Solana DEX programs and emits
/// protobuf-encoded [`DexTradesBatch`] messages.
///
/// # Lifecycle
///
/// 1. `on_load` — logs startup info.
/// 2. `on_transaction` — decodes swaps (without block_time) and buffers them.
/// 3. `on_block` — applies block_time, encodes protobuf, and flushes.
/// 4. `on_exit` — drains any remaining buffered records.
pub struct DexTradesPlugin {
    /// Optional callback invoked with each encoded protobuf batch.
    /// Consumers can use this to write to files, send over the network, etc.
    on_batch: Option<Arc<dyn Fn(u64, Vec<u8>) + Send + Sync>>,
}

impl DexTradesPlugin {
    /// Create a new plugin that logs decoded batches (no persistence).
    pub fn new() -> Self {
        Self { on_batch: None }
    }

    /// Create a plugin with a callback that receives `(slot, protobuf_bytes)`.
    pub fn with_batch_callback(
        callback: impl Fn(u64, Vec<u8>) + Send + Sync + 'static,
    ) -> Self {
        Self {
            on_batch: Some(Arc::new(callback)),
        }
    }

    fn flush_slot(slot: u64, block_time: i64, on_batch: &Option<Arc<dyn Fn(u64, Vec<u8>) + Send + Sync>>) {
        if let Some((_, records)) = PENDING_BY_SLOT.remove(&slot) {
            if records.is_empty() {
                return;
            }

            let block_date = chrono::DateTime::from_timestamp(block_time, 0)
                .map(|dt| dt.date_naive())
                .unwrap_or_else(|| chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap());

            if *DUMP_ENABLED {
                for (i, r) in records.iter().enumerate() {
                    println!(
                        "─── slot={} date={} swap {}/{} ───\n\
                         tx_id:       {}\n\
                         tx_index:    {}  ix: {}.{}  inner: {}\n\
                         ix_type:     {}\n\
                         outer_prog:  {}\n\
                         inner_prog:  {}\n\
                         pool:        {}\n\
                         signer:      {}\n\
                         bought:      {} (decimals={})  amount={:.10}\n\
                         bought_vault:{} reserve={:.6}\n\
                         sold:        {} (decimals={})  amount={:.10}\n\
                         sold_vault:  {} reserve={:.6}\n\
                         txn_fee:     {:.9}  priority_fee: {:.9}  jito_tips: {:.9}\n\
                         sqrt_price:  {}  direction: {}\n\
                         CU consumed: {}",
                        slot, block_date, i + 1, records.len(),
                        r.tx_id,
                        r.tx_index, r.instruction_index, r.inner_instruction_index, r.is_inner_instruction,
                        r.instruction_type,
                        r.outer_program,
                        r.inner_program,
                        r.pool_address,
                        r.signer,
                        r.token_bought_mint, r.token_bought_decimals, r.token_bought_amount,
                        r.token_bought_vault, r.token_bought_vault_reserve,
                        r.token_sold_mint, r.token_sold_decimals, r.token_sold_amount,
                        r.token_sold_vault, r.token_sold_vault_reserve,
                        r.txn_fee, r.priority_fee, r.jito_tips,
                        if r.sqrt_price.is_empty() { "-" } else { &r.sqrt_price },
                        match r.is_base_to_quote { Some(true) => "base→quote", Some(false) => "quote→base", None => "-" },
                        r.compute_units_consumed,
                    );
                }
            }

            let proto_records: Vec<crate::proto::SwapRecord> = records
                .iter()
                .map(|r| {
                    let mut pr = r.to_proto();
                    pr.block_time = block_time;
                    pr.block_date = block_date.format("%Y-%m-%d").to_string();
                    pr
                })
                .collect();

            let count = proto_records.len();
            let batch = DexTradesBatch {
                slot,
                block_time,
                records: proto_records,
            };

            let bytes = batch.encode_to_vec();

            log::info!(
                "dex-trades: slot {} — {} swaps, {} bytes protobuf",
                slot,
                count,
                bytes.len()
            );

            if let Some(cb) = on_batch {
                cb(slot, bytes);
            }
        }
    }
}

impl Default for DexTradesPlugin {
    fn default() -> Self {
        Self::new()
    }
}

impl Plugin for DexTradesPlugin {
    fn name(&self) -> &'static str {
        "DEX Trades"
    }

    fn on_load(&self, _db: Option<Arc<clickhouse::Client>>) -> PluginFuture<'_> {
        async move {
            log::info!(
                "DEX Trades Plugin loaded — {} decoders, {} program IDs",
                REGISTRY.decoder_count(),
                REGISTRY.supported_program_ids().len()
            );
            Ok(())
        }
        .boxed()
    }

    fn on_transaction<'a>(
        &'a self,
        _thread_id: usize,
        _db: Option<Arc<clickhouse::Client>>,
        transaction: &'a TransactionData,
    ) -> PluginFuture<'a> {
        async move {
            // Decode with block_time=0; the real value is applied in on_block.
            let swaps = REGISTRY.decode_transaction(transaction, 0);
            if !swaps.is_empty() {
                PENDING_BY_SLOT
                    .entry(transaction.slot)
                    .or_default()
                    .extend(swaps);
            }
            Ok(())
        }
        .boxed()
    }

    fn on_block<'a>(
        &'a self,
        _thread_id: usize,
        _db: Option<Arc<clickhouse::Client>>,
        block: &'a BlockData,
    ) -> PluginFuture<'a> {
        let slot = block.slot();
        let block_time = block.block_time().unwrap_or(0);
        let on_batch = self.on_batch.clone();
        async move {
            Self::flush_slot(slot, block_time, &on_batch);
            Ok(())
        }
        .boxed()
    }

    fn on_exit(&self, _db: Option<Arc<clickhouse::Client>>) -> PluginFuture<'_> {
        let on_batch = self.on_batch.clone();
        async move {
            let slots: Vec<u64> = PENDING_BY_SLOT.iter().map(|e| *e.key()).collect();
            for slot in slots {
                Self::flush_slot(slot, 0, &on_batch);
            }
            log::info!("DEX Trades Plugin exiting.");
            Ok(())
        }
        .boxed()
    }
}
