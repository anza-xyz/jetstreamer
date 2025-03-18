use reqwest::blocking::Client;
use std::io::{BufReader, Read};

/// Fetches a network stream pointing to the specified epoch CAR file in Old Faithful.
pub fn fetch_epoch_stream(epoch: u64, client: &Client) -> reqwest::Result<impl Read> {
    let url = format!(
        "{}/{}/epoch-{}.car",
        crate::slot_cache::BASE_URL,
        epoch,
        epoch
    );
    let response = client.get(&url).send()?;
    let reader = BufReader::new(response);

    Ok(reader)
}

#[test]
fn test_fetch_epoch_stream() {
    let client = reqwest::blocking::Client::new();
    let mut stream = fetch_epoch_stream(670, &client).unwrap();

    // Read first 1024 bytes of the stream
    let mut buf = vec![0u8; 1024];
    stream.read_exact(&mut buf).unwrap();

    assert_eq!(buf[0], 58);
}
