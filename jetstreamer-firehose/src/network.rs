use reqwest::Client;

use crate::{
    SharedError,
    epochs::{epoch_exists, epoch_to_slot_range},
};

/// Creates a new HTTP client with the Jetstreamer user agent header.
///
/// The user agent is set to `jetstreamer/v{version}` where version comes from
/// the crate version. If the `JETSTREAMER_VERSION` environment variable is set
/// at build time (e.g., via CI/CD), it will be used instead, allowing git
/// commit information to be included (e.g., `jetstreamer/v0.3.0+24d5efe-dirty`).
pub fn create_http_client() -> Client {
    let version = option_env!("JETSTREAMER_VERSION").unwrap_or(env!("CARGO_PKG_VERSION"));
    let user_agent = format!("jetstreamer/v{}", version);
    Client::builder()
        .user_agent(user_agent)
        .build()
        .expect("failed to create HTTP client")
}

/// Queries the current epoch from mainnet using the Solana RPC API.
pub async fn current_epoch(client: &Client) -> Result<u64, SharedError> {
    let url = "https://api.mainnet-beta.solana.com";
    let request_body = r#"{"jsonrpc":"2.0","id":1,"method":"getEpochInfo","params":[]}"#;
    let response = client
        .post(url)
        .header("Content-Type", "application/json")
        .body(request_body)
        .send()
        .await?;
    let text = response.text().await?;
    let epoch_info: serde_json::Value = serde_json::from_str(&text).unwrap();
    let epoch = epoch_info["result"]["epoch"].as_u64().unwrap();
    Ok(epoch)
}

/// Finds the most recent epoch with a compact archive hosted on Old Faithful.
///
/// If `epoch` is `None`, the search starts from [`current_epoch`]. The returned
/// tuple is `(epoch, first_slot, last_slot)`.
pub async fn latest_old_faithful_epoch(
    client: &Client,
    epoch: Option<u64>,
) -> Result<(u64, u64, u64), SharedError> {
    let mut epoch = if let Some(epoch) = epoch {
        epoch
    } else {
        current_epoch(client).await?
    };
    loop {
        if epoch_exists(epoch, client).await {
            let (start_slot, end_slot) = epoch_to_slot_range(epoch);
            return Ok((epoch, start_slot, end_slot));
        }
        epoch -= 1;
    }
}
