use std::process::Stdio;

use tokio::{
    io::{AsyncBufReadExt, BufReader},
    process::Command,
    sync::mpsc,
};

pub async fn spawn_click_house() -> Result<
    (
        mpsc::Receiver<()>,
        impl std::future::Future<Output = Result<(), ()>>,
    ),
    String,
> {
    tracing::info!("Spawning local ClickHouse server...");

    // Create a channel to signal when ClickHouse is ready
    let (ready_tx, ready_rx) = mpsc::channel(1);

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

    // Spawn a task to monitor the ClickHouse output for the "Ready to accept connections" message
    tokio::spawn(async move {
        while let Ok(Some(line)) = reader.next_line().await {
            tracing::info!("ClickHouse output: {}", line);

            // Check for the exact "Ready for connections." message
            if line.trim().contains("Ready for connections.") {
                tracing::info!("ClickHouse is ready to accept connections.");

                // Send the readiness signal through the channel
                if let Err(err) = ready_tx.send(()).await {
                    tracing::error!("Failed to send readiness signal: {}", err);
                }
                break;
            }
        }

        tracing::warn!("ClickHouse stdout stream ended without readiness signal.");
    });

    tracing::info!("Waiting for ClickHouse process to be ready.");

    // Return the receiver side of the channel and the future for the ClickHouse process
    Ok((ready_rx, async move {
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
    }))
}
