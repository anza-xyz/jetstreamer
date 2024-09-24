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
    log::info!("Spawning local ClickHouse server...");

    // Create a channel to signal when ClickHouse is ready
    let (ready_tx, ready_rx) = mpsc::channel(1);

    let mut clickhouse_command = Command::new("./clickhouse")
        .arg("server")
        .current_dir("bin")
        .stdout(Stdio::piped()) // Redirect stdout to capture logs
        .stderr(Stdio::piped()) // Also capture stderr
        .spawn()
        .map_err(|err| {
            log::error!("Failed to start the ClickHouse process: {}", err);
            err.to_string()
        })?;

    // Capture stdout and stderr
    let stdout = clickhouse_command
        .stdout
        .take()
        .expect("Failed to capture stdout");
    let stderr = clickhouse_command
        .stderr
        .take()
        .expect("Failed to capture stderr");

    // Create a combined reader for stdout and stderr
    let mut stdout_reader = BufReader::new(stdout).lines();
    let mut stderr_reader = BufReader::new(stderr).lines();

    // Spawn a task to monitor both stdout and stderr for the "Ready for connections." message
    tokio::spawn(async move {
        let mut ready_signal_sent = false;

        loop {
            tokio::select! {
                line = stdout_reader.next_line() => {
                    if let Ok(Some(line)) = line {
                        println!("{}", line);
                    }
                }
                line = stderr_reader.next_line() => {
                    if let Ok(Some(line)) = line {
                        eprintln!("{}", line);

                        // Check for "Ready for connections" message, ignoring extra formatting or invisible chars
                        if !ready_signal_sent && line.contains("Ready for connections") {
                            log::info!("ClickHouse is ready to accept connections.");

                            // Send the readiness signal through the channel
                            if let Err(err) = ready_tx.send(()).await {
                                log::error!("Failed to send readiness signal: {}", err);
                            }
                            ready_signal_sent = true;
                        }
                    }
                }
            }
        }
    });

    log::info!("Waiting for ClickHouse process to be ready.");

    // Return the receiver side of the channel and the future for the ClickHouse process
    Ok((ready_rx, async move {
        let status = clickhouse_command.wait().await;

        match status {
            Ok(status) => {
                log::info!("ClickHouse exited with status: {}", status);
                Ok(())
            }
            Err(err) => {
                log::error!("Failed to wait on the ClickHouse process: {}", err);
                Err(())
            }
        }
    }))
}
