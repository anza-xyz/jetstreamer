use std::process::{exit, Command, Stdio};

pub async fn spawn_click_house() {
    tracing::info!("spawning local ClickHouse server...");
    // Build the command to run the ClickHouse binary
    let clickhouse_command = Command::new("clickhouse")
        .arg("server")
        .current_dir("./bin")
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn();

    // Check if the process started successfully
    match clickhouse_command {
        Ok(mut child) => {
            tracing::info!("ClickHouse started successfully with PID: {}", child.id());

            // Wait for the process to exit
            match child.wait() {
                Ok(status) => {
                    tracing::info!("ClickHouse exited with status: {}", status);
                }
                Err(err) => {
                    tracing::error!("Failed to wait on the ClickHouse process: {}", err);
                    exit(1)
                }
            }
        }
        Err(err) => {
            tracing::error!("Failed to start the ClickHouse process: {}", err);
            exit(1)
        }
    }
}
