use std::process::{exit, Stdio};
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    process::Command,
    sync::oneshot,
};
use tracing::{error, info};

pub async fn spawn_click_house() -> Result<impl std::future::Future<Output = Result<(), ()>>, String>
{
    tracing::info!("Spawning local ClickHouse server...");

    // Build the command using `tokio::process::Command` for async support
    let mut clickhouse_command = Command::new("clickhouse")
        .arg("server")
        .current_dir("./bin")
        .stdout(Stdio::piped()) // Redirect stdout to capture logs
        .stderr(Stdio::inherit()) // Inherit stderr
        .spawn()
        .map_err(|err| {
            tracing::error!("Failed to start the ClickHouse process: {}", err);
            err.to_string()
        })?;

    // Get the stdout stream
    let stdout = clickhouse_command
        .stdout
        .take()
        .expect("Failed to capture stdout");

    // Wrap stdout in an async BufReader to read line by line
    let mut reader = BufReader::new(stdout).lines();

    // Create a oneshot channel to signal when we see "Ready to accept connections"
    let (tx, rx) = oneshot::channel();

    // Spawn a task to monitor the ClickHouse output for the "Ready to accept connections" message
    tokio::spawn(async move {
        while let Ok(Some(line)) = reader.next_line().await {
            tracing::info!("ClickHouse output: {}", line);

            // Check for the "Ready to accept connections" message
            if line.contains("Ready to accept connections") {
                tracing::info!("ClickHouse is ready to accept connections.");
                let _ = tx.send(()); // Signal that ClickHouse is ready
                break;
            }
        }
    });

    // Wait for the readiness signal
    rx.await
        .map_err(|_| "Failed to receive ready signal".to_string())?;

    tracing::info!("ClickHouse process is fully ready.");

    // Return a future that waits for the ClickHouse process to complete
    Ok(async move {
        let status = clickhouse_command.wait().await;

        match status {
            Ok(status) => {
                tracing::info!("ClickHouse exited with status: {}", status);
                Ok(())
            }
            Err(err) => {
                tracing::error!("Failed to wait on the ClickHouse process: {}", err);
                Err(())
            }
        }
    })
}
