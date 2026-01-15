//! Helpers for working with Solana ledger snapshots stored in GCS.

pub mod snapshots;

pub use snapshots::{SnapshotError, SnapshotInfo, download_epoch_snapshot, resolve_epoch_snapshot};
