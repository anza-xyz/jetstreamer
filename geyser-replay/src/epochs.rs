use backoff::{future::retry, ExponentialBackoff};
use reqwest::Client;
use std::collections::HashMap;
use tokio::sync::mpsc;
use tokio::task;
use tokio::task::JoinHandle;

const BASE_URL: &str = "https://files.old-faithful.net";
const RANGE_PADDING: usize = 11; // current slot sizes are around 9 bytes

async fn fetch_slot_range(epoch: u64, client: &Client) -> Option<(u64, u64, u64)> {
    let url = format!("{}/{}/{}.slots.txt", BASE_URL, epoch, epoch);
    let backoff = ExponentialBackoff::default();

    let result = retry(backoff, || async {
        let head_range = format!("bytes=0-{}", RANGE_PADDING);
        let tail_range = format!("bytes=-{}", RANGE_PADDING);

        let first_slot = fetch_slot_with_range(true, &url, &head_range, &client)
            .await
            .ok_or("Failed to fetch first slot")?;
        let last_slot = fetch_slot_with_range(false, &url, &tail_range, &client)
            .await
            .ok_or("Failed to fetch last slot")?;

        Ok((epoch, first_slot, last_slot))
    })
    .await;

    result.ok()
}

async fn fetch_slot_with_range(
    first: bool,
    url: &str,
    range: &str,
    client: &Client,
) -> Option<u64> {
    println!("Fetching: {} with range: {}", url, range);
    let response = client.get(url).header("Range", range).send().await.ok()?;

    if response.status() == reqwest::StatusCode::NOT_FOUND {
        return None;
    }
    let text = response.text().await.ok()?;
    if first {
        text.trim().lines().next()?.trim().parse::<u64>().ok()
    } else {
        text.trim().lines().last()?.trim().parse::<u64>().ok()
    }
}

pub async fn build_epochs_index() -> anyhow::Result<HashMap<u64, u64>> {
    let client = Client::new();
    let (tx, mut rx) = mpsc::unbounded_channel();
    let (stop_tx, mut stop_rx) = mpsc::channel(1);

    let mut handles: Vec<JoinHandle<()>> = Vec::new();

    let index_fut = tokio::spawn(async move {
        let mut index = HashMap::new();

        while let Some((epoch, start_slot, end_slot)) = rx.recv().await {
            println!(
                "Epoch: {}, start_slot: {}, end_slot: {}",
                epoch, start_slot, end_slot
            );
            for slot in start_slot..=end_slot {
                index.insert(slot, epoch);
            }
        }
        index
    });

    for epoch in 0u64.. {
        if stop_rx.try_recv().is_ok() {
            break;
        }

        let tx = tx.clone();
        let stop_tx = stop_tx.clone();
        let client = client.clone();

        let handle = task::spawn(async move {
            if let Some((epoch, start_slot, end_slot)) = fetch_slot_range(epoch, &client).await {
                let _ = tx.send((epoch, start_slot, end_slot));
            } else {
                println!("No more epochs to fetch, stopping...");
                std::process::exit(1);
                let _ = stop_tx.send(()).await;
            }
        });

        if epoch % 16 == 0 {
            handle.await?;
            std::thread::sleep(std::time::Duration::from_millis(1));
        } else {
            handles.push(handle);
        }
    }

    drop(tx);
    for handle in handles {
        handle.await?;
    }

    let index = index_fut.await?;

    let min_slot = index.keys().min().unwrap();
    let max_slot = index.keys().max().unwrap();
    let min_epoch = index.get(min_slot).unwrap();
    let max_epoch = index.get(max_slot).unwrap();
    println!(
        "Epochs index built: {} slots from slot {} to slot {} (epochs {} to {})",
        index.len(),
        min_slot,
        max_slot,
        min_epoch,
        max_epoch,
    );

    Ok(index)
}
