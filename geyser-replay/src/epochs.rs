use futures_util::TryStreamExt;
use rangemap::RangeMap;
use reqwest::Client;
use tokio::io::{AsyncRead, BufReader};
use tokio::sync::mpsc;
use tokio::task;
use tokio::task::JoinHandle;

const BASE_URL: &str = "https://files.old-faithful.net";
const RANGE_PADDING: usize = 11; // current slot sizes are around 9 bytes

async fn fetch_epoch_slot_range(epoch: u64, client: &Client) -> Option<(u64, u64, u64)> {
    let url = format!("{}/{}/{}.slots.txt", BASE_URL, epoch, epoch);
    let head_range = format!("bytes=0-{}", RANGE_PADDING);
    let tail_range = format!("bytes=-{}", RANGE_PADDING);

    let Some(first_slot) =
        fetch_epoch_slot_txt_with_http_range(true, &url, &head_range, &client).await
    else {
        return None;
    };
    let Some(last_slot) =
        fetch_epoch_slot_txt_with_http_range(false, &url, &tail_range, &client).await
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

    if response.status() != reqwest::StatusCode::PARTIAL_CONTENT
        && response.status() != reqwest::StatusCode::OK
    {
        return None;
    }
    let text = response.text().await.ok()?;
    if first {
        text.trim().lines().next()?.trim().parse::<u64>().ok()
    } else {
        text.trim().lines().last()?.trim().parse::<u64>().ok()
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

    for epoch in 0u64.. {
        if stop_rx.try_recv().is_ok() {
            stop_rx.close();
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

    drop(tx);
    for handle in handles {
        handle.await?;
    }

    let index = index_fut.await?;

    let min_slot = &index.first_range_value().unwrap().0.start;
    let max_slot = &index.last_range_value().unwrap().0.end - 1;
    let min_epoch = index.get(min_slot).unwrap();
    let max_epoch = index.get(&max_slot).unwrap();
    println!(
        "Epochs index built from slot {} to slot {} (epochs {} to {})",
        min_slot, max_slot, min_epoch, max_epoch,
    );

    Ok(index)
}

/// Fetches a network stream pointing to the specified epoch CAR file in Old Faithful.
pub async fn fetch_epoch_stream(
    epoch: u64,
    client: &Client,
) -> reqwest::Result<impl AsyncRead + Unpin> {
    let url = format!("{}/{}/epoch-{}.car", BASE_URL, epoch, epoch);
    let response = client.get(&url).send().await?;
    let stream = response.bytes_stream();

    Ok(BufReader::new(tokio_util::io::StreamReader::new(
        stream.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e)),
    )))
}

#[tokio::test(worker_threads = 32, flavor = "multi_thread")]
async fn test_build_epoch_index() {
    let client = Client::new();
    println!("building epochs index");
    let start = std::time::Instant::now();
    let cache = build_epochs_index(&client).await.unwrap();
    println!("built epochs index in {:?}", start.elapsed());
    assert!(cache.len() > 710);
}
