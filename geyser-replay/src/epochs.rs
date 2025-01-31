use rangemap::RangeMap;
use reqwest::Client;
use tokio::sync::mpsc;
use tokio::task;
use tokio::task::JoinHandle;

const BASE_URL: &str = "https://files.old-faithful.net";
const RANGE_PADDING: usize = 11; // current slot sizes are around 9 bytes

async fn fetch_slot_range(epoch: u64, client: &Client) -> Option<(u64, u64, u64)> {
    let url = format!("{}/{}/{}.slots.txt", BASE_URL, epoch, epoch);
    let head_range = format!("bytes=0-{}", RANGE_PADDING);
    let tail_range = format!("bytes=-{}", RANGE_PADDING);

    let Some(first_slot) = fetch_slot_with_range(true, &url, &head_range, &client).await else {
        return None;
    };
    let Some(last_slot) = fetch_slot_with_range(false, &url, &tail_range, &client).await else {
        return None;
    };

    Some((epoch, first_slot, last_slot))
}

async fn fetch_slot_with_range(
    first: bool,
    url: &str,
    range: &str,
    client: &Client,
) -> Option<u64> {
    //println!("Fetching: {} with range: {}", url, range);
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

pub async fn build_epochs_index() -> anyhow::Result<RangeMap<u64, u64>> {
    let client = Client::new();
    let (tx, mut rx) = mpsc::unbounded_channel();
    let (stop_tx, mut stop_rx) = mpsc::unbounded_channel();

    let mut handles: Vec<JoinHandle<()>> = Vec::new();

    let index_fut = tokio::spawn(async move {
        let mut index = RangeMap::new();

        while let Some((epoch, start_slot, end_slot)) = rx.recv().await {
            // println!(
            //     "Epoch: {}, start_slot: {}, end_slot: {}",
            //     epoch, start_slot, end_slot
            // );
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
            if let Some((epoch, start_slot, end_slot)) = fetch_slot_range(epoch, &client).await {
                let _ = tx.send((epoch, start_slot, end_slot));
            } else {
                //println!("Epoch: {} not found", epoch);
                let _ = stop_tx.send(epoch);
            }
        });

        // if epoch % 100 == 0 {
        //     handle.await?;
        // } else {
        handles.push(handle);
        //}
        std::thread::sleep(std::time::Duration::from_millis(2));
        //std::thread::yield_now();
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
