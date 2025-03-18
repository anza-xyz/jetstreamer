use std::{fmt::Display, ops::Range};

use reqwest::Client;
use thiserror::Error;

use crate::slot_cache::SlotCache;

#[derive(Debug, Error)]
pub enum GeyserReplayError {
    Reqwest(reqwest::Error),
}

impl Display for GeyserReplayError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GeyserReplayError::Reqwest(e) => write!(f, "Reqwest error: {}", e),
        }
    }
}

pub async fn process_slot_range(
    slot_range: Range<u64>,
    client: Client,
    cache: SlotCache,
) -> Result<(), GeyserReplayError> {
    todo!()
}
