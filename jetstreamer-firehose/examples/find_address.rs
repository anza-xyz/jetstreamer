use {
    futures_util::FutureExt,
    jetstreamer_firehose::firehose::{self, TransactionData},
    solana_address::Address,
    std::str::FromStr,
};

fn transaction_handler(address: Address) -> impl firehose::Handler<TransactionData> {
    move |_thread_id, tx| {
        async move {
            let (account_keys, instructions) = match &tx.transaction.message {
                solana_message::VersionedMessage::Legacy(message) => {
                    (&message.account_keys, &message.instructions)
                }
                solana_message::VersionedMessage::V0(message) => {
                    (&message.account_keys, &message.instructions)
                }
            };
            if account_keys.contains(&address) {
                let programs = instructions
                    .iter()
                    .map(|instruction| {
                        account_keys
                            .get(usize::from(instruction.program_id_index))
                            .copied()
                    })
                    .collect::<Vec<_>>();
                let instruction_shapes = instructions
                    .iter()
                    .map(|instruction| {
                        (
                            instruction.program_id_index,
                            instruction.accounts.clone(),
                            instruction.data.len(),
                            instruction.data.get(..instruction.data.len().min(12)).unwrap().to_vec(),
                        )
                    })
                    .collect::<Vec<_>>();
                println!(
                    "MATCH slot={} index={} sig={} status={:?} fee={} available={} pre_balances={:?} post_balances={:?} programs={:?} keys={:?} instructions={:?}",
                    tx.slot,
                    tx.transaction_slot_index,
                    tx.signature,
                    tx.transaction_status_meta.status,
                    tx.transaction_status_meta.fee,
                    tx.status_meta_available,
                    tx.transaction_status_meta.pre_balances,
                    tx.transaction_status_meta.post_balances,
                    programs,
                    account_keys,
                    instruction_shapes,
                );
            }
            Ok(())
        }
        .boxed()
    }
}

#[tokio::main]
async fn main() {
    let range = std::env::args()
        .nth(1)
        .expect("usage: find_address START:END ADDRESS [THREADS]");
    let (start, end) = range.split_once(':').expect("range must be START:END");
    let start = start.parse::<u64>().expect("start must be an integer");
    let end = end.parse::<u64>().expect("end must be an integer");
    let address = Address::from_str(&std::env::args().nth(2).expect("missing address"))
        .expect("invalid address");
    let threads = std::env::args()
        .nth(3)
        .map(|value| value.parse::<u64>().expect("threads must be an integer"))
        .unwrap_or(16);
    firehose::firehose(
        threads,
        false,
        false,
        None,
        start..end + 1,
        None::<firehose::OnBlockFn>,
        Some(transaction_handler(address)),
        None::<firehose::OnEntryFn>,
        None::<firehose::OnRewardFn>,
        None::<firehose::OnErrorFn>,
        None::<firehose::OnStatsTrackingFn>,
        None,
    )
    .await
    .unwrap_or_else(|(error, at)| panic!("firehose failed at slot {at}: {error}"));
}
