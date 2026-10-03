use {
    futures_util::FutureExt,
    jetstreamer_firehose::firehose::{self, BlockData, EntryData, TransactionData},
};

fn block_handler() -> impl firehose::Handler<BlockData> {
    move |_thread_id, block| {
        async move {
            println!(
                "BLOCK slot={} skipped={} block_time={:?}",
                block.slot(),
                block.was_skipped(),
                block.block_time(),
            );
            Ok(())
        }
        .boxed()
    }
}

fn transaction_handler() -> impl firehose::Handler<TransactionData> {
    move |_thread_id, tx| {
        async move {
            let account_keys = match &tx.transaction.message {
                solana_message::VersionedMessage::Legacy(message) => &message.account_keys,
                solana_message::VersionedMessage::V0(message) => &message.account_keys,
            };
            let instructions = match &tx.transaction.message {
                solana_message::VersionedMessage::Legacy(message) => &message.instructions,
                solana_message::VersionedMessage::V0(message) => &message.instructions,
            };
            let programs = instructions
                .iter()
                .map(|instruction| {
                    account_keys
                        .get(usize::from(instruction.program_id_index))
                        .map(ToString::to_string)
                        .unwrap_or_else(|| "<invalid>".to_owned())
                })
                .collect::<Vec<_>>();
            println!(
                "TX slot={} index={} sig={} status={:?} fee={} available={} programs={:?} keys={:?} instructions={:?}",
                tx.slot,
                tx.transaction_slot_index,
                tx.signature,
                tx.transaction_status_meta.status,
                tx.transaction_status_meta.fee,
                tx.status_meta_available,
                programs,
                account_keys,
                instructions,
            );
            Ok(())
        }
        .boxed()
    }
}

fn entry_handler() -> impl firehose::Handler<EntryData> {
    move |_thread_id, entry| {
        async move {
            println!(
                "ENTRY slot={} index={} transactions={:?}",
                entry.slot, entry.entry_index, entry.transaction_indexes
            );
            Ok(())
        }
        .boxed()
    }
}

#[tokio::main]
async fn main() {
    let slot: u64 = std::env::args()
        .nth(1)
        .expect("usage: inspect_slot SLOT")
        .parse()
        .expect("slot must be an integer");
    firehose::firehose(
        1,
        false,
        false,
        None,
        slot..slot + 1,
        Some(block_handler()),
        Some(transaction_handler()),
        Some(entry_handler()),
        None::<firehose::OnRewardFn>,
        None::<firehose::OnErrorFn>,
        None::<firehose::OnStatsTrackingFn>,
        None,
    )
    .await
    .unwrap_or_else(|(error, at)| panic!("firehose failed at slot {at}: {error}"));
}
