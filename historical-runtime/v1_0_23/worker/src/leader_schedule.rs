//! Exact v1.0.23 stake-to-leader algorithm, copied in small form from
//! `ledger/src/{leader_schedule,leader_schedule_utils,staking_utils}.rs`.

use rand::distributions::{Distribution, WeightedIndex};
use rand_chacha::{rand_core::SeedableRng, ChaChaRng};
use solana_runtime::bank::Bank;
use solana_sdk::{
    clock::{Epoch, Slot, NUM_CONSECUTIVE_LEADER_SLOTS},
    pubkey::Pubkey,
};
use solana_vote_program::vote_state::VoteState;
use std::collections::HashMap;

pub fn schedule_for_epoch(bank: &Bank, epoch: Epoch) -> Result<Vec<Pubkey>, String> {
    let vote_accounts = bank
        .epoch_vote_accounts(epoch)
        .ok_or_else(|| format!("bank has no epoch vote accounts for epoch {}", epoch))?;
    let mut node_stakes = HashMap::<Pubkey, u64>::new();
    for (_vote_pubkey, (stake, account)) in vote_accounts.iter() {
        if let Ok(vote_state) = VoteState::deserialize(&account.data) {
            node_stakes
                .entry(vote_state.node_pubkey)
                .and_modify(|node_stake| *node_stake += *stake)
                .or_insert(*stake);
        }
    }

    let mut stakes: Vec<(Pubkey, u64)> = node_stakes.into_iter().collect();
    // This ordering is consensus-sensitive and matches v1.0.23 exactly.
    stakes.sort_unstable_by(|(left_key, left_stake), (right_key, right_stake)| {
        if right_stake == left_stake {
            right_key.cmp(left_key)
        } else {
            right_stake.cmp(left_stake)
        }
    });
    stakes.dedup();
    sample_schedule(
        &stakes,
        epoch_seed(epoch),
        bank.get_slots_in_epoch(epoch),
        NUM_CONSECUTIVE_LEADER_SLOTS,
    )
}

pub fn slot_leader(
    bank: &Bank,
    slot: Slot,
    schedules: &mut HashMap<Epoch, Vec<Pubkey>>,
) -> Result<Pubkey, String> {
    let (epoch, slot_index) = bank.get_epoch_and_slot_index(slot);
    if !schedules.contains_key(&epoch) {
        schedules.insert(epoch, schedule_for_epoch(bank, epoch)?);
    }
    let schedule = schedules
        .get(&epoch)
        .ok_or_else(|| format!("leader schedule cache lost epoch {}", epoch))?;
    schedule
        .get(slot_index as usize)
        .cloned()
        .ok_or_else(|| format!("slot index {} is outside epoch {}", slot_index, epoch))
}

fn epoch_seed(epoch: Epoch) -> [u8; 32] {
    let mut seed = [0u8; 32];
    seed[0..8].copy_from_slice(&epoch.to_le_bytes());
    seed
}

fn sample_schedule(
    ids_and_stakes: &[(Pubkey, u64)],
    seed: [u8; 32],
    len: u64,
    repeat: u64,
) -> Result<Vec<Pubkey>, String> {
    if ids_and_stakes.is_empty() {
        return Err("leader schedule contains no staked nodes".to_string());
    }
    let (ids, stakes): (Vec<_>, Vec<_>) = ids_and_stakes.iter().cloned().unzip();
    let weighted_index = WeightedIndex::new(stakes)
        .map_err(|error| format!("invalid leader schedule weights: {}", error))?;
    let rng = &mut ChaChaRng::from_seed(seed);
    let mut current_node = Pubkey::default();
    Ok((0..len)
        .map(|index| {
            if index % repeat == 0 {
                current_node = ids[weighted_index.sample(rng)];
            }
            current_node
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn upstream_weighted_schedule_vector_is_stable() {
        let alice = Pubkey::new_from_array([1; 32]);
        let bob = Pubkey::new_from_array([2; 32]);
        let schedule = sample_schedule(&[(alice, 2), (bob, 1)], [0; 32], 8, 1).unwrap();
        assert_eq!(
            schedule,
            vec![alice, alice, alice, bob, alice, alice, alice, alice]
        );
        let repeated = sample_schedule(&[(alice, 2), (bob, 1)], [0; 32], 8, 2).unwrap();
        assert_eq!(
            repeated,
            vec![alice, alice, alice, alice, alice, alice, bob, bob]
        );
    }

    #[test]
    fn empty_schedule_fails_closed() {
        assert!(sample_schedule(&[], [0; 32], 8, 4).is_err());
    }
}
