use once_cell::sync::OnceCell;
use serde::Serialize;
use std::path::PathBuf;
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;

static EXPORT_FORMAT: OnceCell<String> = OnceCell::new();

/// Sets the export format for JSONL export.
/// This should be called once during initialization.
pub fn set_export_format(format: String) {
    let _ = EXPORT_FORMAT.set(format);
}

/// Gets the current export format, if set.
pub fn get_export_format() -> Option<&'static str> {
    EXPORT_FORMAT.get().map(|s| s.as_str())
}

/// Writes rows to a JSONL file locally.
/// Files are written to `tables/<table_name>/<timestamp>.jsonl`.
pub async fn write_to_jsonl<T: Serialize>(
    table_name: &str,
    rows: Vec<T>,
) -> Result<(), Box<dyn std::error::Error>> {
    if rows.is_empty() {
        return Ok(());
    }

    // Create output directory if it doesn't exist
    let output_dir = PathBuf::from("tables").join(table_name);
    tokio::fs::create_dir_all(&output_dir).await?;

    // Generate filename with timestamp
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let filename = format!("{}.jsonl", timestamp);
    let filepath = output_dir.join(&filename);

    // Open file in write mode (create new file each time)
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&filepath)
        .await?;

    // Write each event as a JSON line
    for event in rows {
        let json_line = serde_json::to_string(&event)?;
        file.write_all(json_line.as_bytes()).await?;
        file.write_all(b"\n").await?;
    }

    file.flush().await?;
    Ok(())
}
