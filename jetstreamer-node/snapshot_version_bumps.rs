#!/usr/bin/env rust-script

use std::error::Error;
use std::process::Command;

fn read_version(bucket: &str, slot: u64, filename: &str) -> Option<String> {
    let version_path = format!("{}/{}/{}", bucket, slot, filename);
    let output = Command::new("gcloud")
        .env("CLOUDSDK_CORE_DISABLE_PROMPTS", "1")
        .args(["storage", "cat", &version_path])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let version = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if version.is_empty() {
        None
    } else {
        Some(version)
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let bucket = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "gs://mainnet-beta-ledger-us-ny5".to_string());
    let bucket = bucket.trim_end_matches('/').to_string();

    let output = Command::new("gcloud")
        .env("CLOUDSDK_CORE_DISABLE_PROMPTS", "1")
        .args(["storage", "ls", &bucket])
        .output()?;
    if !output.status.success() {
        return Err(format!(
            "gcloud storage ls failed: {}",
            String::from_utf8_lossy(&output.stderr)
        )
        .into());
    }

    let mut slots: Vec<u64> = String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter_map(|line| {
            let line = line.trim().trim_end_matches('/');
            let name = line.rsplit('/').next()?;
            if name.chars().all(|c| c.is_ascii_digit()) {
                name.parse::<u64>().ok()
            } else {
                None
            }
        })
        .collect();

    slots.sort_unstable();

    let mut last_version: Option<String> = None;
    for slot in slots {
        let version = read_version(&bucket, slot, "version")
            .or_else(|| read_version(&bucket, slot, "version.txt"));
        let Some(version) = version else {
            continue;
        };

        if last_version.as_deref() != Some(&version) {
            println!("{slot}\t{version}");
            last_version = Some(version);
        }
    }

    Ok(())
}
