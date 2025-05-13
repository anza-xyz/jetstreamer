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
    pub slot: u64,
    pub tx_index: u32,
    pub program_id: Pubkey,
    pub count: u32,
}

pub struct ProgramTrackingPlugin {}

impl Plugin for ProgramTrackingPlugin {
    fn name(&self) -> &'static str {
        "Program Tracking"
    }

    fn on_transaction<'a>(
        &mut self,
        db: Client,
        transaction: Transaction,
        tx_index: u32,
    ) -> PluginFuture<'a> {
        async move {
            let mut insert = db.insert("program_invocations")?;
            let (account_keys, instructions) = match transaction.tx.message {
                VersionedMessage::Legacy(msg) => (msg.account_keys, msg.instructions),
                VersionedMessage::V0(msg) => (msg.account_keys, msg.instructions),
            };
            let program_ids = instructions
                .iter()
                .map(|ix: &CompiledInstruction| account_keys[ix.program_id_index as usize])
                .collect::<Vec<_>>();
            let mut counts = HashMap::new();
            for program_id in program_ids.into_iter() {
                *counts.entry(program_id).or_insert(0) += 1;
            }
            for (program_id, count) in counts {
                let row = ProgramEvent {
                    slot: transaction.slot,
                    program_id,
                    tx_index,
                    count,
                };
                insert.write(&row).await.unwrap();
            }
            insert.end().await.unwrap();
            Ok(())
        }
        .boxed()
    }

    fn on_block<'a>(&mut self, _db: Client, _block: Block) -> PluginFuture<'a> {
        async move { Ok(()) }.boxed()
    }

    fn on_load<'a>(&mut self, db: Client) -> PluginFuture<'a> {
        async move {
            log::info!("Program Tracking Plugin loaded.");
            log::info!("Creating program_invocations table if it does not exist...");
            db.query(
                r#"
                CREATE TABLE IF NOT EXISTS program_invocations (
                    slot        UInt64,
                    program_id  FixedString(32),
                    tx_index    UInt32,
                    count       UInt32,
                ) 
                ENGINE = MergeTree()
                ORDER BY (slot, count, program_id, tx_index)
                "#,
            )
            .execute()
            .await?;
            log::info!("done.");
            Ok(())
        }
        .boxed()
    }

    fn on_exit<'a>(&mut self, _db: Client) -> PluginFuture<'a> {
        async move {
            log::info!("Program Tracking Plugin unloading...");
            Ok(())
        }
        .boxed()
    }
}
