use backoff::{future::retry, ExponentialBackoff};
use reqwest::Client;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::task;
use tokio::task::JoinHandle;

const BASE_URL: &str = "https://files.old-faithful.net";
const RANGE_PADDING: usize = 64;

async fn fetch_slot_range(epoch: u64, client: Arc<Client>) -> Option<(u64, u64, u64)> {
    let url = format!("{}/{}/{}.slots.txt", BASE_URL, epoch, epoch);
    let backoff = ExponentialBackoff::default();

    let result = retry(backoff, || async {
        let head_range = format!("bytes=0-{}", RANGE_PADDING);
        let tail_range = format!("bytes=-{}", RANGE_PADDING);

        let first_slot = fetch_slot_with_range(&url, &head_range, client.clone())
            .await
            .ok_or("Failed to fetch first slot")?;
        let last_slot = fetch_slot_with_range(&url, &tail_range, client.clone())
            .await
            .ok_or("Failed to fetch last slot")?;

        Ok((epoch, first_slot, last_slot))
    })
    .await;

    result.ok()
}

async fn fetch_slot_with_range(url: &str, range: &str, client: Arc<Client>) -> Option<u64> {
    let response = client.get(url).header("Range", range).send().await.ok()?;

    if response.status() == reqwest::StatusCode::NOT_FOUND {
        return None;
    }

    let text = response.text().await.ok()?;
    text.lines().next()?.trim().parse::<u64>().ok()
}

pub async fn build_epochs_index() -> anyhow::Result<HashMap<u64, u64>> {
    let client = Arc::new(Client::new());
    let (tx, mut rx) = mpsc::channel(100);

    let mut handles: Vec<JoinHandle<()>> = Vec::new();

    for epoch in 0u64.. {
        let client = client.clone();
        let tx = tx.clone();

        let handle = task::spawn(async move {
            if let Some((epoch, start_slot, end_slot)) = fetch_slot_range(epoch, client).await {
                let _ = tx.send((epoch, start_slot, end_slot)).await;
            } else {
                drop(tx);
            }
        });

        handles.push(handle);
    }

    drop(tx);
    for handle in handles {
        let _ = handle.await;
    }

    let mut index = HashMap::new();

    while let Some((epoch, start_slot, end_slot)) = rx.recv().await {
        for slot in start_slot..=end_slot {
            index.insert(slot, epoch);
        }
    }

    Ok(index)
}
