use reqwest::Client;
use rseek::Seekable;
use tokio::io::{AsyncRead, AsyncSeek};

/// Fetches a network stream pointing to the specified epoch CAR file in Old Faithful.
pub async fn fetch_epoch_stream(epoch: u64, client: &Client) -> impl AsyncRead + AsyncSeek {
    let client = client.clone();
    let stream = Seekable::new(move || {
        client.get(&format!(
            "{}/{}/epoch-{}.car",
            crate::slot_cache::BASE_URL,
            epoch,
            epoch
        ))
    })
    .await;
    stream
}

#[cfg(test)]
use tokio::io::AsyncReadExt;

#[tokio::test]
async fn test_fetch_epoch_stream() {
    use tokio::io::AsyncSeekExt;
    let client = reqwest::Client::new();
    let mut stream = fetch_epoch_stream(670, &client).await;

    // Read first 1024 bytes of the stream
    let mut buf = vec![0u8; 1024];
    stream.read_exact(&mut buf).await.unwrap();
    assert_eq!(buf[0], 58);

    // read last 1024 bytes of the stream
    stream.seek(std::io::SeekFrom::End(-1024)).await.unwrap();
    stream.read_exact(&mut buf).await.unwrap();
    assert_eq!(buf[1], 1);
}
