use futures_util::TryStreamExt;
use reqwest::Client;
use tokio::io::AsyncRead;

/// Fetches a network stream pointing to the specified epoch CAR file in Old Faithful.
pub async fn fetch_epoch_stream(epoch: u64, client: &Client) -> reqwest::Result<impl AsyncRead> {
    let url = format!(
        "{}/{}/epoch-{}.car",
        crate::slot_cache::BASE_URL,
        epoch,
        epoch
    );
    let response = client.get(&url).send().await?;
    let byte_stream = response.bytes_stream();
    let reader = tokio_util::io::StreamReader::new(
        byte_stream.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e)),
    );

    Ok(reader)
}

#[cfg(test)]
use tokio::io::AsyncReadExt;

#[tokio::test]
async fn test_fetch_epoch_stream() {
    let client = reqwest::Client::new();
    let mut stream = fetch_epoch_stream(670, &client).await.unwrap();

    // Read first 1024 bytes of the stream
    let mut buf = vec![0u8; 1024];
    stream.read_exact(&mut buf).await.unwrap();

    assert_eq!(buf[0], 58);
}
