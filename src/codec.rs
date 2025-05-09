//! Turn a `ReplicaTransactionInfoVersions` into an owned, Serde-friendly blob.
//! ```
//! Everything here works equally with serde_json / rmp-serde / postcard.

use agave_geyser_plugin_interface::geyser_plugin_interface::ReplicaTransactionInfoVersions;
use serde::{Deserialize, Serialize};
use solana_sdk::{signature::Signature, transaction::VersionedTransaction};

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Transaction {
    pub slot: u64,
    pub signature: Signature,
    pub is_vote: bool,
    pub tx: VersionedTransaction, // already derives Serde (with `serde` feature)
    pub cu: Option<u64>,
}

impl Transaction {
    pub fn from_replica(
        slot: u64, // slot is supplied by Geyser callback
        info: ReplicaTransactionInfoVersions<'_>,
    ) -> Self {
        match info {
            ReplicaTransactionInfoVersions::V0_0_1(v) => Self {
                slot,
                signature: *v.signature,
                is_vote: v.is_vote,
                tx: v
                    .transaction // SanitizedTransaction<'_>
                    .clone() // -> owned SanitizedTransaction
                    .to_versioned_transaction(), // -> VersionedTransaction
                cu: v.transaction_status_meta.compute_units_consumed,
            },
            ReplicaTransactionInfoVersions::V0_0_2(v) => Self {
                slot,
                signature: *v.signature,
                is_vote: v.is_vote,
                tx: v.transaction.clone().to_versioned_transaction(),
                cu: v.transaction_status_meta.compute_units_consumed,
            },
        }
    }
}
