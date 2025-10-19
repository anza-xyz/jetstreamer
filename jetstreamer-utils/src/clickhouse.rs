use std::{future::Future, os::unix::fs::PermissionsExt, path::Path, pin::Pin, process::Stdio};

use log;
use tempfile::NamedTempFile;
use tokio::{
    fs::File,
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    process::Command,
    sync::{OnceCell, mpsc},
};

fn process_log_line(line: impl AsRef<str>) {
    let line = line.as_ref();
    let prefix_len = "2025.05.07 20:25:31.905655 [ 3286299 ] {} ".len();
    if line.len() > prefix_len {
        match &line[prefix_len..] {
            ln if ln.starts_with("<Information>") => {
                let msg = &ln[14..];
                let msg_trimmed = msg.trim();
                // Suppress noisy ClickHouse client version banner lines
                if msg_trimmed.starts_with("(version ") {
                    return;
                }
                if !msg_trimmed.is_empty() {
                    log::info!("{}", msg)
                }
            }
            ln if ln.starts_with("<Trace>") => log::trace!("{}", &ln[8..]),
            ln if ln.starts_with("<Error>") => log::error!("{}", &ln[8..]),
            ln if ln.starts_with("<Debug>") => log::debug!("{}", &ln[8..]),
            ln if ln.starts_with("<Warning>") => log::warn!("{}", &ln[10..]),
            _ => log::debug!("{}", line),
        }
    } else if !line.trim().is_empty() {
        let t = line.trim();
        // Suppress bare version banner lines that sometimes arrive without the standard prefix
        if t.starts_with("(version ") {
            return;
        }
        log::info!("{}", line);
    }
}

static CLICKHOUSE_PROCESS: OnceCell<u32> = OnceCell::const_new();

include!(concat!(env!("OUT_DIR"), "/embed_clickhouse.rs")); // raw bytes for clickhouse binary

/// Errors that can occur when managing the embedded ClickHouse process.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClickhouseError {
    /// ClickHouse process terminated with an error message.
    Process(String),
    /// Server failed to perform its required initialization steps.
    InitializationFailed,
}

impl std::fmt::Display for ClickhouseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ClickhouseError::Process(msg) => write!(f, "ClickHouse error: {}", msg),
            ClickhouseError::InitializationFailed => {
                write!(f, "ClickHouse initialization failed")
            }
        }
    }
}

impl std::error::Error for ClickhouseError {}

/// Future type returned when supervising the ClickHouse process.
pub type ClickhouseProcessFuture = Pin<Box<dyn Future<Output = Result<(), ()>> + Send>>;
/// Tuple containing the readiness channel and process future returned by [`start`].
pub type ClickhouseStartResult = (mpsc::Receiver<()>, ClickhouseProcessFuture);

/// Launches the bundled ClickHouse client binary and forwards STDIO.
pub async fn start_client() -> Result<(), Box<dyn std::error::Error>> {
    let clickhouse_path = NamedTempFile::with_suffix("-clickhouse")
        .unwrap()
        .into_temp_path()
        .keep()
        .unwrap();
    log::info!("Writing ClickHouse binary to: {:?}", clickhouse_path);
    File::create(&clickhouse_path)
        .await
        .unwrap()
        .write_all(CLICKHOUSE_BINARY)
        .await
        .unwrap();
    // executable permission for Unix
    #[cfg(unix)]
    std::fs::set_permissions(&clickhouse_path, std::fs::Permissions::from_mode(0o755)).unwrap();
    log::info!("ClickHouse binary written and permissions set.");

    let bin_dir = Path::new("./bin");
    std::fs::create_dir_all(bin_dir).unwrap();

    std::thread::sleep(std::time::Duration::from_secs(1));

    // let clickhouse take over the current process
    Command::new(clickhouse_path)
        .arg("client")
        .arg("--host=localhost")
        .current_dir(bin_dir)
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("Failed to start ClickHouse client process")
        .wait()
        .await?;

    Ok(())
}

/// Spawns the embedded ClickHouse server and returns a readiness channel plus process task.
pub async fn start() -> Result<ClickhouseStartResult, ClickhouseError> {
    log::info!("Spawning local ClickHouse server...");

    // write clickhouse binary to a temp file
    let clickhouse_path = NamedTempFile::with_suffix("-clickhouse")
        .unwrap()
        .into_temp_path()
        .keep()
        .unwrap();
    log::info!("Writing ClickHouse binary to: {:?}", clickhouse_path);
    File::create(&clickhouse_path)
        .await
        .unwrap()
        .write_all(CLICKHOUSE_BINARY)
        .await
        .unwrap();
    // executable permission for Unix
    #[cfg(unix)]
    std::fs::set_permissions(&clickhouse_path, std::fs::Permissions::from_mode(0o755)).unwrap();
    log::info!("ClickHouse binary written and permissions set.");

    // Create a channel to signal when ClickHouse is ready
    let (ready_tx, ready_rx) = mpsc::channel(1);

    let bin_dir = Path::new("./bin");
    std::fs::create_dir_all(bin_dir).unwrap();
    std::thread::sleep(std::time::Duration::from_secs(1));
    let mut clickhouse_command = unsafe {
        Command::new(clickhouse_path)
            .arg("server")
            //.arg("--async_insert_queue_flush_on_shutdown=1")
            .stdout(Stdio::piped()) // Redirect stdout to capture logs
            .stderr(Stdio::piped()) // Also capture stderr
            .current_dir(bin_dir)
            .pre_exec(|| {
                // safety: setsid() can't fail if we're child of a real process
                libc::setsid();
                Ok(())
            })
            .spawn()
            .map_err(|err| {
                ClickhouseError::Process(format!("Failed to start the ClickHouse process: {}", err))
            })?
    };

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
        let mut other_pid: Option<u32> = None;
        loop {
            tokio::select! {
                line = stdout_reader.next_line() => {
                    if let Ok(Some(line)) = line {
                        process_log_line(line);
                    }
                }
                line = stderr_reader.next_line() => {
                    if let Ok(Some(line)) = line {
                        if line.ends_with("Updating DNS cache") || line.ends_with("Updated DNS cache") {
                            // Ignore DNS cache update messages
                            continue;
                        }
                        process_log_line(&line);

                        // Check for "Ready for connections" message, ignoring extra formatting or invisible chars
                        if !ready_signal_sent && line.contains("Ready for connections") {
                            log::info!("ClickHouse is ready to accept connections.");

                            // Send the readiness signal through the channel
                            if let Err(err) = ready_tx.send(()).await {
                                log::error!("Failed to send readiness signal: {}", err);
                            }
                            ready_signal_sent = true;
                        } else if line.contains("DB::Server::run() @") {
                            log::warn!("ClickHouse server is already running, gracefully shutting down and restarting.");
                            let Some(other_pid) = other_pid else {
                                panic!("Failed to find the PID of the running ClickHouse server.");
                            };
                            if let Err(err) = Command::new("kill")
                                .arg("-s")
                                .arg("SIGTERM")
                                .arg(other_pid.to_string())
                                .status()
                                .await
                            {
                                log::error!("Failed to send SIGTERM to ClickHouse process: {}", err);
                            }
                            log::warn!("ClickHouse process with PID {} killed.", other_pid);
                            log::warn!("Please re-launch.");
                            std::process::exit(0);
                        } else if line.contains("PID: ")
                            && let Some(pid_str) = line.split_whitespace().nth(1)
                                && let Ok(pid) = pid_str.parse::<u32>() {
                                    other_pid = Some(pid);
                                }
                    }
                }
            }
        }
    });

    log::info!("Waiting for ClickHouse process to be ready.");

    // Return the receiver side of the channel and the future for the ClickHouse process
    Ok((
        ready_rx,
        Box::pin(async move {
            CLICKHOUSE_PROCESS
                .set(clickhouse_command.id().unwrap())
                .unwrap();
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
        }),
    ))
}

/// Stops the ClickHouse process asynchronously, if one is running.
pub async fn stop() {
    if let Some(&pid) = CLICKHOUSE_PROCESS.get() {
        log::info!("Stopping ClickHouse process with PID: {}", pid);

        let status = Command::new("kill").arg(pid.to_string()).status();

        match status.await {
            Ok(exit_status) if exit_status.success() => {
                log::info!("ClickHouse process with PID {} stopped gracefully.", pid);
            }
            Ok(exit_status) => {
                log::warn!(
                    "pkill executed, but ClickHouse process might not have stopped. Exit status: {}",
                    exit_status
                );
            }
            Err(err) => {
                log::error!("Failed to execute pkill for PID {}: {}", pid, err);
            }
        }
    } else {
        log::warn!("ClickHouse process PID not found in CLICKHOUSE_PROCESS.");
    }
}

/// Synchronously stops the ClickHouse process by blocking on [`stop`].
pub fn stop_sync() {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(stop());
}
