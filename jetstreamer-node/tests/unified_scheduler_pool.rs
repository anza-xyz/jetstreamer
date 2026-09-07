//! Exercises the exact scheduler lifecycle `JETSTREAMER_SCHEDULER=unified`
//! uses: a block-verification scheduler checked out of the pool per slot via
//! `BankWithScheduler::new_for_verification_replay`, consumed by
//! `wait_for_completed_scheduler`, and returned to the pool for the next slot.

use std::sync::Arc;

use solana_runtime::{
    bank::Bank, bank_forks::BankForks, genesis_utils::create_genesis_config,
    installed_scheduler_pool::BankWithScheduler, prioritization_fee_cache::PrioritizationFeeCache,
};
use solana_unified_scheduler_pool::DefaultSchedulerPool;

#[test]
fn per_slot_checkout_wait_and_recycle() {
    let genesis = create_genesis_config(1_000_000_000);
    let bank = Bank::new_for_tests(&genesis.genesis_config);
    let bank_forks = BankForks::new_rw_arc(bank);

    let pool = DefaultSchedulerPool::new_dyn(
        Some(4),
        None,
        None,
        None,
        Arc::new(PrioritizationFeeCache::default()),
    );

    // Slot 1: check out, run an empty session, wait for completion.
    let parent = bank_forks.read().unwrap().working_bank();
    let child = Bank::new_from_parent(parent, &solana_pubkey::Pubkey::new_unique(), 1);
    let child = bank_forks.write().unwrap().insert(child);
    let bank1 = child.clone_without_scheduler();
    let bank_ws = BankWithScheduler::new_for_verification_replay(bank1.clone(), &pool);
    assert!(bank_ws.has_installed_scheduler());
    let (result, _timings) = bank_ws
        .wait_for_completed_scheduler()
        .expect("scheduler session must produce a result");
    result.expect("empty session must succeed");
    assert!(!bank_ws.has_installed_scheduler());

    // Slot 2: a second checkout works after the first returned to the pool.
    bank1.freeze();
    let child2 = Bank::new_from_parent(bank1, &solana_pubkey::Pubkey::new_unique(), 2);
    let child2 = bank_forks.write().unwrap().insert(child2);
    let bank2 = child2.clone_without_scheduler();
    let bank_ws2 = BankWithScheduler::new_for_verification_replay(bank2, &pool);
    assert!(bank_ws2.has_installed_scheduler());
    let (result2, _timings) = bank_ws2
        .wait_for_completed_scheduler()
        .expect("second scheduler session must produce a result");
    result2.expect("second empty session must succeed");
}
