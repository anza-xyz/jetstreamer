//! Bridge between Geyser and the plugin interface.

use std::str::FromStr;

use agave_geyser_plugin_interface::geyser_plugin_interface::{
    ReplicaBlockInfoVersions, ReplicaTransactionInfoVersions,
};
use serde::{Deserialize, Serialize};
use solana_sdk::{hash::Hash, signature::Signature, transaction::VersionedTransaction};

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Transaction {
    pub slot: u64,
    pub signature: Signature,
    pub is_vote: bool,
    pub tx: VersionedTransaction,
    pub cu: Option<u64>,
}

impl Transaction {
    pub fn from_replica(slot: u64, info: ReplicaTransactionInfoVersions<'_>) -> Self {
        match info {
            ReplicaTransactionInfoVersions::V0_0_1(v) => Self {
                slot,
                signature: *v.signature,
                is_vote: {
                    // temporary fix until https://github.com/anza-xyz/agave/pull/6230
                    let msg = v.transaction.message();
                    let instructions = msg.instructions();
                    let account_keys = msg.account_keys();
                    if account_keys.len() > 0 && instructions.len() > 0 {
                        account_keys[instructions[0].program_id_index as usize]
                            == solana_vote_program::id()
                    } else {
                        false
                    }
                },
                tx: v.transaction.to_versioned_transaction(),
                cu: v.transaction_status_meta.compute_units_consumed,
            },
            ReplicaTransactionInfoVersions::V0_0_2(v) => Self {
                slot,
                signature: *v.signature,
                is_vote: {
                    // temporary fix until https://github.com/anza-xyz/agave/pull/6230
                    let msg = v.transaction.message();
                    let instructions = msg.instructions();
                    let account_keys = msg.account_keys();
                    if account_keys.len() > 0 && instructions.len() > 0 {
                        account_keys[instructions[0].program_id_index as usize]
                            == solana_vote_program::id()
                    } else {
                        false
                    }
                },
                tx: v.transaction.to_versioned_transaction(),
                cu: v.transaction_status_meta.compute_units_consumed,
            },
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Block {
    pub slot: u64,
    pub parent_slot: u64,
    pub block_hash: Hash,
    pub block_height: Option<u64>,
}

impl Block {
    pub fn from_replica(info: ReplicaBlockInfoVersions) -> Self {
        match info {
            ReplicaBlockInfoVersions::V0_0_1(b) => Self {
                slot: b.slot,
                parent_slot: b.slot.saturating_sub(1), // might be wrong
                block_hash: Hash::from_str(b.blockhash).unwrap(),
                block_height: b.block_height,
            },
            ReplicaBlockInfoVersions::V0_0_2(b) => Self {
                slot: b.slot,
                parent_slot: b.parent_slot,
                block_hash: Hash::from_str(b.blockhash).unwrap(),
                block_height: b.block_height,
            },
            ReplicaBlockInfoVersions::V0_0_3(b) => Self {
                slot: b.slot,
                parent_slot: b.parent_slot,
                block_hash: Hash::from_str(b.blockhash).unwrap(),
                block_height: b.block_height,
            },
            ReplicaBlockInfoVersions::V0_0_4(b) => Self {
                slot: b.slot,
                parent_slot: b.parent_slot,
                block_hash: Hash::from_str(b.blockhash).unwrap(),
                block_height: b.block_height,
            },
        }
    }
}
