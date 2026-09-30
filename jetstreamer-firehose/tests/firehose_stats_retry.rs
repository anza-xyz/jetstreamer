use futures_util::FutureExt;
use jetstreamer_firehose::firehose::{
    BlockData, FirehoseErrorContext, OnEntryFn, OnRewardFn, Stats, StatsTracking, TransactionData,
    firehose,
};
use serial_test::serial;
use std::{
    collections::BTreeMap,
    ops::Range,
    sync::{Arc, Mutex},
    time::Duration,
};

#[derive(Clone, Copy)]
enum FailurePoint {
    Slot(u64),
    EpochFlush(u64),
}

impl FailurePoint {
    fn matches(self, stats: &Stats) -> bool {
        match self {
            Self::Slot(slot) => stats.thread_stats.current_slot == slot,
            Self::EpochFlush(epoch) => {
                stats.finish_time.is_some() && stats.thread_stats.current_slot / 432_000 == epoch
            }
        }
    }
}

#[derive(Default)]
struct Observed {
    pending: BTreeMap<u64, usize>,
    transactions: BTreeMap<(u64, usize), usize>,
    blocks: BTreeMap<u64, usize>,
    stats_attempts: Vec<Stats>,
    latest_stats: Option<Stats>,
    stats_errors: usize,
}

async fn assert_stats_retry(range: Range<u64>, reverse: bool, failure: FailurePoint) {
    let observed = Arc::new(Mutex::new(Observed::default()));
    let run = firehose(
        1,
        false,
        reverse,
        Some(8 * 1024 * 1024),
        range.clone(),
        Some({
            let observed = observed.clone();
            move |_: usize, block: BlockData| {
                let observed = observed.clone();
                async move {
                    let mut observed = observed.lock().unwrap();
                    observed.pending.remove(&block.slot());
                    *observed.blocks.entry(block.slot()).or_default() += 1;
                    Ok(())
                }
                .boxed()
            }
        }),
        Some({
            let observed = observed.clone();
            move |_: usize, tx: TransactionData| {
                let observed = observed.clone();
                async move {
                    let mut observed = observed.lock().unwrap();
                    *observed.pending.entry(tx.slot).or_default() += 1;
                    *observed
                        .transactions
                        .entry((tx.slot, tx.transaction_slot_index))
                        .or_default() += 1;
                    Ok(())
                }
                .boxed()
            }
        }),
        None::<OnEntryFn>,
        None::<OnRewardFn>,
        Some({
            let observed = observed.clone();
            move |_: usize, error: FirehoseErrorContext| {
                let observed = observed.clone();
                async move {
                    if error
                        .error_message
                        .contains("synthetic stats callback failure")
                    {
                        observed.lock().unwrap().stats_errors += 1;
                    }
                    Ok(())
                }
                .boxed()
            }
        }),
        Some(StatsTracking {
            tracking_interval_slots: match failure {
                FailurePoint::Slot(_) => 1,
                FailurePoint::EpochFlush(_) => u64::MAX,
            },
            on_stats: {
                let observed = observed.clone();
                move |_: usize, stats: Stats| {
                    let observed = observed.clone();
                    async move {
                        let mut observed = observed.lock().unwrap();
                        if failure.matches(&stats) {
                            observed.stats_attempts.push(stats.clone());
                            if observed.stats_attempts.len() <= 2 {
                                return Err("synthetic stats callback failure".into());
                            }
                        }
                        observed.latest_stats = Some(stats);
                        Ok(())
                    }
                    .boxed()
                }
            },
        }),
        None,
    );

    tokio::time::timeout(Duration::from_secs(180), run)
        .await
        .expect("stats retry run timed out")
        .expect("firehose failed");

    let observed = observed.lock().unwrap();
    assert_eq!(
        observed.stats_errors, 2,
        "both failures must reach on_error"
    );
    assert_eq!(
        observed.stats_attempts.len(),
        3,
        "the failed pulse must succeed"
    );
    assert!(!observed.transactions.is_empty());
    assert!(
        observed.pending.is_empty(),
        "transactions have no closing block callback: {:?}",
        observed.pending
    );
    for (tx, count) in &observed.transactions {
        assert_eq!(
            *count, 1,
            "transaction {tx:?} must not be replayed for stats"
        );
    }
    for slot in range {
        assert_eq!(observed.blocks.get(&slot), Some(&1), "block {slot}");
    }

    let first = &observed.stats_attempts[0];
    for stats in &observed.stats_attempts[1..] {
        assert_eq!(stats.slots_processed, first.slots_processed);
        assert_eq!(stats.blocks_processed, first.blocks_processed);
        assert_eq!(stats.transactions_processed, first.transactions_processed);
        assert_eq!(stats.entries_processed, first.entries_processed);
        assert_eq!(stats.slots_since_last_pulse, first.slots_since_last_pulse);
        assert_eq!(stats.blocks_since_last_pulse, first.blocks_since_last_pulse);
        assert_eq!(
            stats.transactions_since_last_pulse,
            first.transactions_since_last_pulse
        );
    }
    let latest = observed.latest_stats.as_ref().unwrap();
    assert_eq!(latest.slots_processed, observed.blocks.len() as u64);
    assert_eq!(
        latest.transactions_processed,
        observed.transactions.len() as u64
    );
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn stats_retry_does_not_replay_completed_slot() {
    assert_stats_retry(
        345_600_000..345_600_004,
        false,
        FailurePoint::Slot(345_600_001),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn stats_retry_finishes_before_range_completion() {
    assert_stats_retry(
        345_600_000..345_600_002,
        false,
        FailurePoint::Slot(345_600_001),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn stats_retry_preserves_skipped_slot_counts() {
    // Slots 378864396..400 are absent from this archive.
    assert_stats_retry(
        378_864_395..378_864_402,
        false,
        FailurePoint::Slot(378_864_400),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn stats_retry_finishes_after_final_reverse_epoch() {
    assert_stats_retry(
        345_599_998..345_600_002,
        true,
        FailurePoint::EpochFlush(799),
    )
    .await;
}
