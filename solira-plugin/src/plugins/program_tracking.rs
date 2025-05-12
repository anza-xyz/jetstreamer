use clickhouse::{Client, Row};
use futures_util::FutureExt;
use serde::{Deserialize, Serialize};
use solana_sdk::pubkey::Pubkey;

use crate::{
    Plugin, PluginFuture,
    bridge::{Block, Transaction},
};

#[derive(Row, Deserialize, Serialize)]
struct ProgramEvent {
    pub slot: u64,
    pub program_id: Pubkey,
}

pub struct ProgramTrackingPlugin {}

impl Plugin for ProgramTrackingPlugin {
    fn name(&self) -> &'static str {
        "Program Tracking"
    }

    fn on_transaction<'a>(&mut self, db: &Client, transaction: Transaction) -> PluginFuture<'a> {
        async move {
            // db.insert("program_events")?;
            let program_ids = match transaction.tx.message {
                solana_sdk::message::VersionedMessage::Legacy(message) => message
                    .program_ids()
                    .into_iter()
                    .cloned()
                    .collect::<Vec<_>>(),
                solana_sdk::message::VersionedMessage::V0(_message) => todo!(),
            };
            for program_id in program_ids {
                let row = ProgramEvent {
                    slot: transaction.slot,
                    program_id,
                };
                //db.insert("program_events", row)?;
            }
            Ok(())
        }
        .boxed()
    }

    fn on_block<'a>(&mut self, db: &Client, block: Block) -> PluginFuture<'a> {
        todo!()
    }
}
