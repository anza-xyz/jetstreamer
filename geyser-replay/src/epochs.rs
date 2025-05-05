use reqwest::Client;
use rseek::Seekable;
use tokio::io::{AsyncRead, AsyncSeek, BufReader};

use crate::node_reader::Len;

/* ────────────────────────────────────────────────────────────────────────── */
/*  Blanket Len impl so BufReader<T> keeps the .len() we rely on            */
/* ────────────────────────────────────────────────────────────────────────── */
impl<T: Len + AsyncRead> Len for BufReader<T> {
    #[inline]
    fn len(&self) -> u64 {
        self.get_ref().len()
    }
}

/// Fetch an epoch’s CAR file as a **buffered, seek-able** async stream.
pub async fn fetch_epoch_stream(epoch: u64, client: &Client) -> impl AsyncRead + AsyncSeek + Len {
    let client = client.clone();
    let seekable = Seekable::new(move || {
        client.get(format!(
            "{}/{}/epoch-{}.car",
            crate::slot_cache::BASE_URL,
            epoch,
            epoch
        ))
    })
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
