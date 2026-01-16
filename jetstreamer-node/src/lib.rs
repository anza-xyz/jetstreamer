//! Helpers for working with Solana ledger snapshots stored in GCS.

pub mod snapshots;

pub use snapshots::{
    SnapshotError, SnapshotInfo, download_epoch_snapshot, download_snapshot_at_or_before_slot,
    resolve_epoch_snapshot, resolve_snapshot_at_or_before_slot,
};
