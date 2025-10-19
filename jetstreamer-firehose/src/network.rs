use reqwest::Client;

use crate::epochs::{epoch_exists, epoch_to_slot_range};

/// Queries the current epoch from mainnet using the Solana RPC API.
pub async fn current_epoch(client: &Client) -> Result<u64, Box<dyn std::error::Error>> {
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
) -> Result<(u64, u64, u64), Box<dyn std::error::Error>> {
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
