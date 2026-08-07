use std::{
    future::Future,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    pin::Pin,
    process::Stdio,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU32, Ordering},
    },
    time::Duration,
};

use log;
use tempfile::NamedTempFile;
use tokio::{
    fs::File,
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    process::Command,
    sync::{mpsc, oneshot},
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

/// PID of the currently-supervised ClickHouse server (0 = none). An atomic
/// rather than a set-once cell because the supervisor can respawn the server
/// after evicting a stale instance from a previous run.
static CLICKHOUSE_PROCESS: AtomicU32 = AtomicU32::new(0);

/// Attempts to start the server before giving up (each retry evicts a stale
/// instance and waits for it to exit first).
const START_ATTEMPTS: u32 = 5;

/// Finds an orphaned embedded ClickHouse server left behind by a previous run (they are
/// `setsid`-detached, so they outlive their parent), identified by the temp binary naming
/// convention (`<random>-clickhouse server`), excluding our own child process.
async fn find_orphaned_clickhouse(own_pid: Option<u32>) -> Option<u32> {
    let output = Command::new("pgrep")
        .args(["-f", "--", "-clickhouse server"])
        .output()
        .await
        .ok()?;
    String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter_map(|line| line.trim().parse::<u32>().ok())
        .find(|pid| Some(*pid) != own_pid)
}

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
///
/// If a stale server from a previous run still holds the data-directory lock,
/// it is SIGTERMed and ours is respawned automatically once it exits (bounded
/// by [`START_ATTEMPTS`]) — no manual re-launch. The supervision runs in its
/// own task because callers await readiness before polling the returned
/// future.
pub async fn start() -> Result<ClickhouseStartResult, ClickhouseError> {
    log::info!("Spawning local ClickHouse server...");

    // write clickhouse binary to a temp file
    let clickhouse_path = NamedTempFile::with_suffix("-clickhouse")
        .unwrap()
        .into_temp_path()
        .keep()
        .unwrap();
    log::info!("Writing ClickHouse binary to: {:?}", clickhouse_path);
    // Synchronous write: the fd must be fully closed before the supervisor
    // exec()s the binary, or the spawn races an async close and hits ETXTBSY
    // (the old code papered over this with a 1s sleep).
    std::fs::write(&clickhouse_path, CLICKHOUSE_BINARY).unwrap();
    // executable permission for Unix
    #[cfg(unix)]
    std::fs::set_permissions(&clickhouse_path, std::fs::Permissions::from_mode(0o755)).unwrap();
    log::info!("ClickHouse binary written and permissions set.");

    let bin_dir = PathBuf::from("./bin");
    std::fs::create_dir_all(&bin_dir).unwrap();

    let (ready_tx, ready_rx) = mpsc::channel(1);
    let (done_tx, done_rx) = oneshot::channel::<Result<(), ()>>();
    tokio::spawn(async move {
        let result = supervise_server(clickhouse_path, bin_dir, ready_tx).await;
        let _ = done_tx.send(result);
    });

    log::info!("Waiting for ClickHouse process to be ready.");
    Ok((
        ready_rx,
        Box::pin(async move { done_rx.await.unwrap_or(Err(())) }),
    ))
}

/// Runs the server, evicting a stale instance and respawning if necessary.
/// Resolves when the (final) server process exits.
async fn supervise_server(
    binary: PathBuf,
    bin_dir: PathBuf,
    ready_tx: mpsc::Sender<()>,
) -> Result<(), ()> {
    for attempt in 1..=START_ATTEMPTS {
        if attempt > 1 {
            log::info!("Re-launching ClickHouse server (attempt {attempt}/{START_ATTEMPTS})...");
        }
        let mut child = match spawn_server(&binary, &bin_dir) {
            Ok(child) => child,
            Err(err) => {
                log::error!("{err}");
                return Err(());
            }
        };
        let stdout = child.stdout.take().expect("Failed to capture stdout");
        let stderr = child.stderr.take().expect("Failed to capture stderr");
        // PID of a conflicting stale server, set by the log pump when the
        // fresh child reports one already running (it prints the running
        // instance's status and exits).
        let conflict = Arc::new(Mutex::new(None::<u32>));
        let pump = tokio::spawn(pump_server_logs(
            stdout,
            stderr,
            ready_tx.clone(),
            conflict.clone(),
            child.id(),
        ));

        CLICKHOUSE_PROCESS.store(child.id().unwrap_or(0), Ordering::SeqCst);
        let status = child.wait().await;
        let _ = pump.await; // ends at EOF, which follows child exit
        CLICKHOUSE_PROCESS.store(0, Ordering::SeqCst);

        let stale_pid = *conflict.lock().unwrap();
        if let Some(pid) = stale_pid {
            // The pump already SIGTERMed it; wait for it to release the lock,
            // then respawn ours.
            wait_for_pid_exit(pid, Duration::from_secs(60)).await;
            continue;
        }
        return match status {
            Ok(status) => {
                log::info!("ClickHouse exited with status: {}", status);
                Ok(())
            }
            Err(err) => {
                log::error!("Failed to wait on the ClickHouse process: {}", err);
                Err(())
            }
        };
    }
    log::error!(
        "ClickHouse data directory still locked by another instance after {START_ATTEMPTS} \
         attempts; giving up."
    );
    Err(())
}

/// Spawns one ClickHouse server child with piped logs in its own session.
fn spawn_server(binary: &Path, bin_dir: &Path) -> Result<tokio::process::Child, ClickhouseError> {
    unsafe {
        Command::new(binary)
            .arg("server")
            // NOTE: leaving ClickHouse at its default `trace` log level. Lower levels
            // (`information`, `warning`) suppress the "Ready for connections" banner that the
            // readiness scanner looks for, so the firehose hangs at startup. The
            // AsyncLogMessageQueue overflow warnings under high-throughput async inserts are
            // noise from this trace verbosity but are not a correctness issue.
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .current_dir(bin_dir)
            .pre_exec(|| {
                // safety: setsid() can't fail if we're child of a real process
                libc::setsid();
                Ok(())
            })
            .spawn()
            .map_err(|err| {
                ClickhouseError::Process(format!("Failed to start the ClickHouse process: {}", err))
            })
    }
}

/// Forwards the server's stdout/stderr to the log, signals readiness, and —
/// when the fresh child reports a conflicting server already running —
/// SIGTERMs the stale instance and records its PID in `conflict` so the
/// supervisor respawns. Ends when both streams reach EOF (child exit).
async fn pump_server_logs(
    stdout: tokio::process::ChildStdout,
    stderr: tokio::process::ChildStderr,
    ready_tx: mpsc::Sender<()>,
    conflict: Arc<Mutex<Option<u32>>>,
    own_pid: Option<u32>,
) {
    let mut stdout_reader = BufReader::new(stdout).lines();
    let mut stderr_reader = BufReader::new(stderr).lines();
    let mut ready_signal_sent = false;
    let mut other_pid: Option<u32> = None;
    let (mut stdout_done, mut stderr_done) = (false, false);
    while !(stdout_done && stderr_done) {
        tokio::select! {
            line = stdout_reader.next_line(), if !stdout_done => {
                match line {
                    Ok(Some(line)) => process_log_line(line),
                    _ => stdout_done = true,
                }
            }
            line = stderr_reader.next_line(), if !stderr_done => {
                let line = match line {
                    Ok(Some(line)) => line,
                    _ => {
                        stderr_done = true;
                        continue;
                    }
                };
                if line.ends_with("Updating DNS cache") || line.ends_with("Updated DNS cache") {
                    // Ignore DNS cache update messages
                    continue;
                }
                process_log_line(&line);

                // Check for "Ready for connections" message, ignoring extra formatting or invisible chars
                if !ready_signal_sent && line.contains("Ready for connections") {
                    log::info!("ClickHouse is ready to accept connections.");
                    if let Err(err) = ready_tx.send(()).await {
                        log::error!("Failed to send readiness signal: {}", err);
                    }
                    ready_signal_sent = true;
                } else if line.contains("DB::Server::run() @") {
                    // A fresh child that finds the data dir locked prints the
                    // running instance's status (whose "PID:" line we captured
                    // below) and exits on its own.
                    match other_pid {
                        Some(pid) => {
                            log::warn!(
                                "ClickHouse server already running (PID {pid}); sending SIGTERM \
                                 and re-launching automatically."
                            );
                            if let Err(err) = Command::new("kill")
                                .arg("-s")
                                .arg("SIGTERM")
                                .arg(pid.to_string())
                                .status()
                                .await
                            {
                                log::error!("Failed to send SIGTERM to ClickHouse process: {err}");
                            }
                            *conflict.lock().unwrap() = Some(pid);
                        }
                        None => {
                            // No "PID:" line was scraped — happens when the data
                            // dir was wiped while an orphaned server (setsid-
                            // detached from a previous run) still holds the port.
                            // Fall back to process discovery by the embedded
                            // binary's temp naming convention.
                            match find_orphaned_clickhouse(own_pid).await {
                                Some(pid) => {
                                    log::warn!(
                                        "ClickHouse server already running (PID {pid} via process \
                                         discovery); sending SIGTERM and re-launching automatically."
                                    );
                                    if let Err(err) = Command::new("kill")
                                        .arg("-s")
                                        .arg("SIGTERM")
                                        .arg(pid.to_string())
                                        .status()
                                        .await
                                    {
                                        log::error!(
                                            "Failed to send SIGTERM to ClickHouse process: {err}"
                                        );
                                    }
                                    *conflict.lock().unwrap() = Some(pid);
                                }
                                None => {
                                    log::warn!(
                                        "ClickHouse server already running but its PID could not \
                                         be determined; waiting and re-launching anyway."
                                    );
                                    *conflict.lock().unwrap() = Some(0);
                                }
                            }
                        }
                    }
                } else if line.contains("PID: ")
                    && let Some(pid_str) = line.split_whitespace().nth(1)
                    && let Ok(pid) = pid_str.parse::<u32>()
                {
                    other_pid = Some(pid);
                }
            }
        }
    }
}

/// Waits (up to `timeout`) for `pid` to exit, polling with signal 0. A PID of
/// 0 (unknown) just waits a grace period.
async fn wait_for_pid_exit(pid: u32, timeout: Duration) {
    if pid == 0 {
        tokio::time::sleep(Duration::from_secs(3)).await;
        return;
    }
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        // safety: kill with signal 0 only checks for process existence
        if unsafe { libc::kill(pid as i32, 0) } != 0 {
            log::info!("Stale ClickHouse process {pid} has exited.");
            return;
        }
        if tokio::time::Instant::now() >= deadline {
            log::warn!(
                "Stale ClickHouse process {pid} still alive after {}s; retrying spawn anyway.",
                timeout.as_secs()
            );
            return;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// Stops the ClickHouse process asynchronously, if one is running.
pub async fn stop() {
    let pid = CLICKHOUSE_PROCESS.load(Ordering::SeqCst);
    if pid != 0 {
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
