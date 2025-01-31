use backoff::{future::retry, Error as BackoffError, ExponentialBackoff};
use reqwest::{Client, StatusCode};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinSet;

const BASE_URL: &str = "https://files.old-faithful.net";
const RANGE_BYTES: usize = 64; // Allow room for longer slot numbers

/// Builds an epoch index mapping slots to epoch numbers.
pub async fn build_epochs_index() -> Result<HashMap<u64, u64>, reqwest::Error> {
    let client = Arc::new(Client::new());
    let mut tasks = JoinSet::new();
    let mut epoch = 0;

    loop {
        let client_clone = client.clone();
        let epoch_number = epoch;

        tasks.spawn(async move { scan_epoch(client_clone, epoch_number).await });

        epoch += 1;

        // Check if we hit a 404, stopping the loop
        if fetch_slot(&client, epoch_number, 0).await.is_err() {
            break;
        }
    }

    let mut final_index = HashMap::new();
    while let Some(res) = tasks.join_next().await {
        if let Ok(Some(epoch_index)) = res {
            final_index.extend(epoch_index);
        }
    }

    Ok(final_index)
}

/// Attempts to fetch the first and last slot of an epoch.
async fn scan_epoch(client: Arc<Client>, epoch: u64) -> Option<HashMap<u64, u64>> {
    let url = format!("{}/{}/{}.slots.txt", BASE_URL, epoch, epoch);

    let first_slot = fetch_slot(&client, epoch, 0).await.ok()?;
    let last_slot = fetch_slot(&client, epoch, RANGE_BYTES as u64).await.ok()?;

    let mut index = HashMap::new();
    index.insert(first_slot, epoch);
    index.insert(last_slot, epoch);

    Some(index)
}

use backoff::{future::retry, Error as BackoffError, ExponentialBackoff};
use reqwest::{Client, StatusCode};
use std::time::Duration;

pub enum FetchSlotError {
    NotFound(reqwest::Error),
    OtherReqwest(reqwest::Error),
    EmptyResponse,
    InvalidSlotNumber,
    BackoffError,
}

impl From<BackoffError<reqwest::Error>> for FetchSlotError {
    fn from(e: BackoffError<reqwest::Error>) -> Self {
        match e {
            BackoffError::Permanent(e) => FetchSlotError::NotFound(e),
            BackoffError::Transient { err, .. } => FetchSlotError::OtherReqwest(err),
        }
    }
}

impl From<reqwest::Error> for FetchSlotError {
    fn from(e: reqwest::Error) -> Self {
        match e.status() {
            Some(StatusCode::NOT_FOUND) => FetchSlotError::NotFound(e),
            _ => FetchSlotError::OtherReqwest(e),
        }
    }
}

/// Fetches a slot number from a file using an HTTP range request.
async fn fetch_slot(client: &Client, epoch: u64, range_start: u64) -> Result<u64, FetchSlotError> {
    let url = format!(
        "https://files.old-faithful.net/{}/{}.slots.txt",
        epoch, epoch
    );
    let backoff = ExponentialBackoff {
        initial_interval: Duration::from_millis(500),
        max_interval: Duration::from_secs(10),
        max_elapsed_time: Some(Duration::from_secs(60)),
        ..ExponentialBackoff::default()
    };
    let response = retry(backoff, || async {
        client
            .get(&url)
            .header(
                "Range",
                format!("bytes={}-{}", range_start, range_start + 64),
            )
            .send()
    });
    todo!()
}
