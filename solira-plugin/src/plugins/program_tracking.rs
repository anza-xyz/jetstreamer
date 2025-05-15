use std::collections::HashMap;

use clickhouse::{Client, Row};
use futures_util::FutureExt;
use serde::{Deserialize, Serialize};
use solana_sdk::{instruction::CompiledInstruction, message::VersionedMessage, pubkey::Pubkey};

use crate::{
    Plugin, PluginFuture,
    bridge::{Block, Transaction},
};

#[derive(Row, Deserialize, Serialize, Copy, Clone, Debug, PartialEq, Eq, Hash)]
struct ProgramEvent {
    pub slot: u16,
    pub program_id: Pubkey,
    pub count: u32,
    pub min_cus: u32, // need to still use u32 because a single transaction can be up to 1.4M CUs
    pub max_cus: u32,
    pub total_cus: u32, // 32 bits is enough for total CU within a block since max is 48M CUs
}

#[derive(Copy, Clone, PartialEq, Eq, Hash, Debug)]
struct ProgramStats {
    pub count: u32,
    pub min_cus: u32,
    pub max_cus: u32,
    pub total_cus: u32,
}

#[derive(Debug, Default)]
pub struct ProgramTrackingPlugin {
    slot_data: HashMap<Pubkey, ProgramStats>,
}

impl Plugin for ProgramTrackingPlugin {
    fn name(&self) -> &'static str {
        "Program Tracking"
    }

    fn on_transaction<'a>(
        &'a mut self,
        _db: Client,
        transaction: Transaction,
        _tx_index: u32,
    ) -> PluginFuture<'a> {
        async move {
            let (account_keys, instructions) = match transaction.tx.message {
                VersionedMessage::Legacy(msg) => (msg.account_keys, msg.instructions),
                VersionedMessage::V0(msg) => (msg.account_keys, msg.instructions),
            };
            let program_ids = instructions
                .iter()
                .map(|ix: &CompiledInstruction| account_keys[ix.program_id_index as usize])
                .collect::<Vec<_>>();
            let total_cu = transaction.cu.unwrap_or(0) as u32;
            for program_id in program_ids.iter() {
                let this_program_cu = total_cu / program_ids.len() as u32;
                let stats = self.slot_data.entry(*program_id).or_insert(ProgramStats {
                    min_cus: u32::MAX,
                    max_cus: 0,
                    total_cus: 0,
                    count: 0,
                });
                stats.min_cus = stats.min_cus.min(this_program_cu);
                stats.max_cus = stats.max_cus.max(this_program_cu);
                stats.total_cus += this_program_cu;
                stats.count += 1;
            }
            Ok(())
        }
        .boxed()
    }

    fn on_block<'a>(&'a mut self, db: Client, _block: Block) -> PluginFuture<'a> {
        async move {
            let mut insert = db.insert("program_invocations")?;
            for (program_id, stats) in self.slot_data.iter() {
                let row = ProgramEvent {
                    slot: _block.slot as u16,
                    program_id: *program_id,
                    count: stats.count,
                    min_cus: stats.min_cus,
                    max_cus: stats.max_cus,
                    total_cus: stats.total_cus,
                };
                // log::info!("{:?}", row);
                insert.write(&row).await.unwrap();
            }
            let num = self.slot_data.len();
            self.slot_data.clear();
            insert.end().await.unwrap();
            log::info!("Inserted {} program invocations", num);
            Ok(())
        }
        .boxed()
    }

    fn on_load<'a>(&'a mut self, db: Client) -> PluginFuture<'a> {
        async move {
            log::info!("Program Tracking Plugin loaded.");
            log::info!("Creating program_invocations table if it does not exist...");
            db.query(
                r#"
                CREATE TABLE IF NOT EXISTS program_invocations (
                    slot        UInt16,
                    program_id  FixedString(32),
                    count       UInt32,
                    min_cus     UInt32,
                    max_cus     UInt32,
                    total_cus   UInt32,
                ) 
                ENGINE = MergeTree()
                ORDER BY (slot, count, total_cus)
                "#,
            )
            .execute()
            .await?;
            log::info!("done.");
            Ok(())
        }
        .boxed()
    }

    fn on_exit<'a>(&'a mut self, _db: Client) -> PluginFuture<'a> {
        async move {
            log::info!("Program Tracking Plugin unloading...");
            Ok(())
        }
        .boxed()
    }
}
