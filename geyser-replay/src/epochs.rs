use reqwest::Client;
use rseek::Seekable;
use tokio::io::{AsyncRead, AsyncSeek, BufReader};

use crate::node_reader::Len;

pub const BASE_URL: &str = "https://files.old-faithful.net";

#[inline(always)]
pub const fn epoch_to_slot_range(epoch: u64) -> (u64, u64) {
    let first = epoch * 432000;
    (first, first + 431999)
}

#[inline(always)]
pub const fn slot_to_epoch(slot: u64) -> u64 {
    slot / 432000
}

/* ────────────────────────────────────────────────────────────────────────── */
/*  Blanket Len impl so BufReader<T> keeps the .len() we rely on            */
/* ────────────────────────────────────────────────────────────────────────── */
impl<T: Len + AsyncRead> Len for BufReader<T> {
    #[inline]
    fn len(&self) -> u64 {
        self.get_ref().len()
    }
}

pub async fn epoch_exists(epoch: u64, client: &Client) -> bool {
    let url = format!("{}/{}/epoch-{}.car", BASE_URL, epoch, epoch);
    let response = client.head(&url).send().await;
    match response {
        Ok(res) => res.status().is_success(),
        Err(_) => false,
    }
}

/// Fetch an epoch’s CAR file as a **buffered, seek-able** async stream.
pub async fn fetch_epoch_stream(epoch: u64, client: &Client) -> impl AsyncRead + AsyncSeek + Len {
    let client = client.clone();
    let seekable =
        Seekable::new(move || client.get(format!("{}/{}/epoch-{}.car", BASE_URL, epoch, epoch)))
            .await;

    BufReader::with_capacity(8 * 1024 * 1024, seekable)
}

/* ── Tests ──────────────────────────────────────────────────────────────── */
#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncSeekExt};

    #[tokio::test]
    async fn test_fetch_epoch_stream() {
        let client = reqwest::Client::new();
        let mut stream = fetch_epoch_stream(670, &client).await;

        /* first 1 KiB */
        let mut buf = vec![0u8; 1024];
        stream.read_exact(&mut buf).await.unwrap();
        assert_eq!(buf[0], 58);

        /* last 1 KiB */
        stream.seek(std::io::SeekFrom::End(-1024)).await.unwrap();
        stream.read_exact(&mut buf).await.unwrap();
        assert_eq!(buf[1], 1);
    }
}

#[tokio::test]
async fn test_epoch_exists() {
    let client = reqwest::Client::new();
    assert!(epoch_exists(670, &client).await);
    assert!(!epoch_exists(999999, &client).await);
}

#[test]
fn test_epoch_to_slot() {
    assert_eq!(epoch_to_slot_range(0), (0, 431999));
    assert_eq!(epoch_to_slot_range(770), (332640000, 333071999));
}

#[test]
fn test_slot_to_epoch() {
    assert_eq!(slot_to_epoch(0), 0);
    assert_eq!(slot_to_epoch(431999), 0);
    assert_eq!(slot_to_epoch(432000), 1);
    assert_eq!(slot_to_epoch(332640000), 770);
    assert_eq!(slot_to_epoch(333071999), 770);
}
