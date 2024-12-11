//! Provides a client for streaming resources such as blocks and transactions directly from the
//! Old Faithful archive without having to maintain a local file cache.

#![deny(missing_docs)]

use std::ops::{Bound, RangeBounds};

use reqwest::{
    header::{HeaderValue, RANGE},
    Response, Url,
};
use tokio_stream::{Stream, StreamExt};
use url::ParseError;

use crate::core::block::Block;

/// Determines the buffer size to use when streaming data over the network.
const BUFFER_SIZE: u64 = 64 * 1024; // 64KB

/// An error that occurred while interacting with the Old Faithful archive
#[derive(Debug)]
pub enum ClientError {
    /// An error occurred while communicating with the network
    Network(reqwest::Error),
}

/// A client for streaming resources such as blocks and transactions directly from the Old
/// Faithful archive without having to maintain a local file cache
pub struct CarClient {
    base_url: Url,
    client: reqwest::Client,
}

impl CarClient {
    /// Creates a new client with the default base URL. This will `files.old-faithful.net` directly.
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::new(),
            base_url: Url::parse("https://files.old-faithful.net")
                .expect("default base URL is statically known to be valid"),
        }
    }

    /// Allows creating a new client with a custom base URL.
    pub fn new_with_url(url: &str) -> Result<Self, ParseError> {
        Ok(Self {
            client: reqwest::Client::new(),
            base_url: Url::parse(url)?,
        })
    }

    /// Internal helper function used to stream the specified range of bytes from the specified
    /// path, evaluated against the base path.
    async fn get_byte_range(
        &self,
        path: &str,
        range: impl RangeBounds<u64>,
    ) -> Result<Response, reqwest::Error> {
        // Construct the full URL by joining the base URL and the path
        let url = self
            .base_url
            .join(path)
            .expect("internal paths should always be valid");

        // Build the Range header value
        let start = match range.start_bound() {
            Bound::Included(&start) => start,
            Bound::Excluded(&start) => start + 1,
            Bound::Unbounded => 0,
        };

        let range_header_value = match range.end_bound() {
            Bound::Included(&end) => format!("bytes={}-{}", start, end),
            Bound::Excluded(&end) => format!("bytes={}-{}", start, end - 1),
            Bound::Unbounded => format!("bytes={}-", start),
        };

        // Send the GET request with the Range header
        let response = self
            .client
            .get(url)
            .header(RANGE, HeaderValue::from_str(&range_header_value).unwrap())
            .send()
            .await?;

        Ok(response)
    }

    /// Gets the block at the specified slot number from OF1, returning an error if the block
    /// has not been indexed yet or if a network error occurred
    pub async fn get_block(&self, slot: u64) -> Result<Block, reqwest::Error> {
        todo!()
    }

    /// Returns an async stream of blocks within the specified slot range
    pub async fn get_blocks(
        &self,
        slot_range: impl RangeBounds<u64>,
    ) -> impl Stream<Item = Result<Block, reqwest::Error>> + '_ {
        // Determine the start and end of the range
        let start = match slot_range.start_bound() {
            std::ops::Bound::Included(&start) => start,
            std::ops::Bound::Excluded(&start) => start + 1,
            std::ops::Bound::Unbounded => 0, // Default start
        };
        let end = match slot_range.end_bound() {
            std::ops::Bound::Included(&end) => end + 1,
            std::ops::Bound::Excluded(&end) => end,
            std::ops::Bound::Unbounded => u64::MAX, // Default end
        };

        // Create an async stream of blocks
        tokio_stream::iter(start..end).then(move |slot| self.get_block(slot))
    }
}
