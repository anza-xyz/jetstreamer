use std::time::Duration;

use rangemap::RangeMap;
use reqwest::Client;
use tokio::sync::mpsc;
use tokio::task;
use tokio::task::JoinHandle;

pub const BASE_URL: &str = "https://files.old-faithful.net";
const RANGE_PADDING: usize = 11; // current slot sizes are around 9 bytes

pub type SlotCache = RangeMap<u64, u64>;

pub async fn fetch_epoch_slot_range(epoch: u64, client: &Client) -> Option<(u64, u64, u64)> {
    let url = format!("{}/{}/{}.slots.txt", BASE_URL, epoch, epoch);
    let head_range = format!("bytes=0-{}", RANGE_PADDING);
    let tail_range = format!("bytes=-{}", RANGE_PADDING);

    let Some(first_slot) =
        fetch_epoch_slot_txt_with_http_range(true, &url, &head_range, client).await
    else {
        return None;
    };
    let Some(last_slot) =
        fetch_epoch_slot_txt_with_http_range(false, &url, &tail_range, client).await
    else {
        return None;
    };

    Some((epoch, first_slot, last_slot))
}

/// Fetches the slots.txt file from the Old Faithful network and extracts the first or last
/// slot based on the `first` parameter.
async fn fetch_epoch_slot_txt_with_http_range(
    first: bool,
    url: &str,
    range: &str,
    client: &Client,
) -> Option<u64> {
    let response = client.get(url).header("Range", range).send().await.ok()?;

    if response.status() == reqwest::StatusCode::NOT_FOUND {
        return None;
    }

    if response.status() != reqwest::StatusCode::PARTIAL_CONTENT
        && response.status() != reqwest::StatusCode::OK
    {
        std::thread::sleep(Duration::from_millis(100));
    }
    let Ok(text) = response.text().await else {
        log::warn!("Failed to read response text");
        return None;
    };
    if first {
        let Some(first_line) = text.trim().lines().next() else {
            log::warn!("Failed to read first line");
            return None;
        };
        let Ok(parsed) = first_line.trim().parse::<u64>() else {
            log::warn!("Failed to parse first line");
            return None;
        };
        Some(parsed)
    } else {
        let Some(last_line) = text.trim().lines().last() else {
            log::warn!("Failed to read last line");
            return None;
        };
        let Ok(parsed) = last_line.trim().parse::<u64>() else {
            log::warn!("Failed to parse last line");
            return None;
        };
        Some(parsed)
    }
}

/// Builds an index of epochs to slot ranges based on the Old Faithful network.
pub async fn build_epochs_index(client: &Client) -> anyhow::Result<RangeMap<u64, u64>> {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let (stop_tx, mut stop_rx) = mpsc::unbounded_channel();

    let mut handles: Vec<JoinHandle<()>> = Vec::new();

    let index_fut = tokio::spawn(async move {
        let mut index = RangeMap::new();

        while let Some((epoch, start_slot, end_slot)) = rx.recv().await {
            index.insert(start_slot..(end_slot + 1), epoch);
        }
        index
    });

    let mut first_bad_epoch = u64::MAX;
    for epoch in 0u64.. {
        if let Ok(bad_epoch) = stop_rx.try_recv() {
            if bad_epoch < first_bad_epoch {
                first_bad_epoch = bad_epoch;
            }
        }

        if epoch >= first_bad_epoch {
            break;
        }

        let tx = tx.clone();
        let stop_tx = stop_tx.clone();
        let client = client.clone();

        let handle = task::spawn(async move {
            if let Some((epoch, start_slot, end_slot)) =
                fetch_epoch_slot_range(epoch, &client).await
            {
                let _ = tx.send((epoch, start_slot, end_slot));
            } else {
                let _ = stop_tx.send(epoch);
            }
        });
        handles.push(handle);
        std::thread::sleep(std::time::Duration::from_millis(1));
    }

    stop_rx.close();

    drop(tx);
    for handle in handles {
        handle.await?;
    }

    let index = index_fut.await?;

    let min_slot = &index.first_range_value().unwrap().0.start;
    let max_slot = &index.last_range_value().unwrap().0.end - 1;
    let min_epoch = index.get(min_slot).unwrap();
    let max_epoch = index.get(&max_slot).unwrap();
    log::info!(
        "Epochs index built from slot {} to slot {} (epochs {} to {})",
        min_slot,
        max_slot,
        min_epoch,
        max_epoch,
    );

    Ok(index)
}

#[tokio::test(worker_threads = 32, flavor = "multi_thread")]
async fn test_build_epoch_index() {
    let client = Client::new();
    log::info!("building epochs index");
    let start = std::time::Instant::now();
    let cache = build_epochs_index(&client).await.unwrap();
    log::info!("built epochs index in {:?}", start.elapsed());
    assert!(cache.len() > 710);
}
