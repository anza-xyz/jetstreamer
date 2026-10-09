//! Find source transactions that create and initialize oversized stake accounts.
//!
//! Solana v1.3 accepted stake accounts larger than 200 bytes. Later runtimes
//! reject them during `StakeInstruction::Initialize`. The recorded outcome of
//! these transactions gives a narrow compatibility signal for locating that
//! runtime transition in historical mainnet data.
//!
//! Usage:
//!   cargo run --release -p jetstreamer-node \
//!     --example find_oversized_stake_initializations -- START END [THREADS]
//!
//! `END` is exclusive. Each match is emitted as one tab-separated line so
//! results from adjacent scans can be concatenated and sorted by slot.

use std::{
    future::Future,
    io::{self, Write},
    pin::Pin,
    str::FromStr,
};

use jetstreamer_firehose::{
    SharedError, TransactionData,
    firehose::{self, Handler},
};
use solana_address::Address;

const STAKE_STATE_BYTES: u64 = 200;
const SYSTEM_CREATE_ACCOUNT: u32 = 0;
const SYSTEM_CREATE_ACCOUNT_WITH_SEED: u32 = 3;
const STAKE_INITIALIZE: u32 = 0;

type HandlerFuture = Pin<Box<dyn Future<Output = Result<(), SharedError>> + Send + 'static>>;

#[derive(Clone, Copy, Debug)]
struct CreatedStakeAccount {
    account: Address,
    space: u64,
}

fn read_u32(data: &[u8], offset: usize) -> Option<u32> {
    Some(u32::from_le_bytes(
        data.get(offset..offset + 4)?.try_into().ok()?,
    ))
}

fn read_u64(data: &[u8], offset: usize) -> Option<u64> {
    Some(u64::from_le_bytes(
        data.get(offset..offset + 8)?.try_into().ok()?,
    ))
}

fn address(data: &[u8], offset: usize) -> Option<Address> {
    Some(Address::new_from_array(
        data.get(offset..offset + 32)?.try_into().ok()?,
    ))
}

fn created_stake_account(
    keys: &[Address],
    instruction: &solana_message::compiled_instruction::CompiledInstruction,
    system_program: &Address,
    stake_program: &Address,
) -> Option<CreatedStakeAccount> {
    if keys.get(usize::from(instruction.program_id_index))? != system_program {
        return None;
    }
    let target_index = usize::from(*instruction.accounts.get(1)?);
    let target = *keys.get(target_index)?;

    let (space, owner) = match read_u32(&instruction.data, 0)? {
        SYSTEM_CREATE_ACCOUNT => (
            read_u64(&instruction.data, 12)?,
            address(&instruction.data, 20)?,
        ),
        SYSTEM_CREATE_ACCOUNT_WITH_SEED => {
            let seed_len = usize::try_from(read_u64(&instruction.data, 36)?).ok()?;
            let fields = 44usize.checked_add(seed_len)?;
            (
                read_u64(&instruction.data, fields + 8)?,
                address(&instruction.data, fields + 16)?,
            )
        }
        _ => return None,
    };
    (owner == *stake_program && space > STAKE_STATE_BYTES).then_some(CreatedStakeAccount {
        account: target,
        space,
    })
}

fn transaction_handler(
    system_program: Address,
    stake_program: Address,
) -> impl Handler<TransactionData> {
    move |_thread_id, transaction| {
        Box::pin(async move {
            let keys = transaction.transaction.message.static_account_keys();
            let instructions = transaction.transaction.message.instructions();

            for (create_index, instruction) in instructions.iter().enumerate() {
                let Some(created) =
                    created_stake_account(keys, instruction, &system_program, &stake_program)
                else {
                    continue;
                };

                for (initialize_index, candidate) in instructions.iter().enumerate() {
                    let initializes_created_account = keys
                        .get(usize::from(candidate.program_id_index))
                        .is_some_and(|program| program == &stake_program)
                        && read_u32(&candidate.data, 0) == Some(STAKE_INITIALIZE)
                        && candidate
                            .accounts
                            .first()
                            .and_then(|index| keys.get(usize::from(*index)))
                            == Some(&created.account);
                    if !initializes_created_account {
                        continue;
                    }

                    println!(
                        "MATCH\tslot={}\ttx_index={}\tsignature={}\tspace={}\tcreate_ix={}\tinitialize_ix={}\tstatus_available={}\tstatus={:?}",
                        transaction.slot,
                        transaction.transaction_slot_index,
                        transaction.signature,
                        created.space,
                        create_index,
                        initialize_index,
                        transaction.status_meta_available,
                        transaction.transaction_status_meta.status,
                    );
                    io::stdout().flush().expect("flush match");
                }
            }
            Ok(())
        }) as HandlerFuture
    }
}

fn usage() -> ! {
    eprintln!(
        "usage: find_oversized_stake_initializations START END [THREADS]\n\
         END is exclusive"
    );
    std::process::exit(2);
}

#[tokio::main]
async fn main() {
    let args = std::env::args().collect::<Vec<_>>();
    if !(3..=4).contains(&args.len()) {
        usage();
    }
    let start = args[1].parse::<u64>().unwrap_or_else(|_| usage());
    let end = args[2].parse::<u64>().unwrap_or_else(|_| usage());
    let threads = args
        .get(3)
        .map(|value| value.parse::<u64>().unwrap_or_else(|_| usage()))
        .unwrap_or(16)
        .max(1);
    if start >= end {
        usage();
    }

    let system_program = Address::from_str("11111111111111111111111111111111").unwrap();
    let stake_program = Address::from_str("Stake11111111111111111111111111111111111111").unwrap();

    eprintln!("scanning source slots {start}..{end} with {threads} firehose threads");
    firehose::firehose(
        threads,
        false,
        false,
        None,
        start..end,
        None::<firehose::OnBlockFn>,
        Some(transaction_handler(system_program, stake_program)),
        None::<firehose::OnEntryFn>,
        None::<firehose::OnRewardFn>,
        None::<firehose::OnErrorFn>,
        None::<firehose::OnStatsTrackingFn>,
        None,
    )
    .await
    .unwrap_or_else(|(error, slot)| panic!("firehose failed at slot {slot}: {error}"));
    eprintln!("scan complete for source slots {start}..{end}");
}
