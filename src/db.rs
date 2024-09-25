use std::process::Stdio;
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    process::Command,
    sync::mpsc,
};

use crate::SoliraError;

fn process_log_line(line: impl AsRef<str>) {
    let line = line.as_ref();
    if line.len() > 41 {
        match &line[41..] {
            ln if ln.starts_with("<Information>") => {
                log::info!("{}", &ln[14..])
            }
            ln if ln.starts_with("<Trace>") => log::trace!("{}", &ln[8..]),
            ln if ln.starts_with("<Error>") => log::error!("{}", &ln[8..]),
            ln if ln.starts_with("<Debug>") => log::debug!("{}", &ln[8..]),
            ln if ln.starts_with("<Warning>") => log::warn!("{}", &ln[10..]),
            _ => log::info!("{}", line),
        }
    } else {
        log::info!("{}", line);
    }
}

pub async fn spawn_click_house() -> Result<
    (
        mpsc::Receiver<()>,
        impl std::future::Future<Output = Result<(), ()>>,
    ),
    SoliraError,
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
            SoliraError::ClickHouseError(format!("Failed to start the ClickHouse process: {}", err))
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
                        process_log_line(line);
                    }
                }
                line = stderr_reader.next_line() => {
                    if let Ok(Some(line)) = line {
                        process_log_line(&line);

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
