#!/usr/bin/env rust-script

use std::error::Error;
use std::process::Command;

#[derive(Clone, Debug)]
struct VersionInfo {
    version: String,
    major: u64,
    minor: u64,
}

fn read_version(bucket: &str, slot: u64, filename: &str) -> Option<VersionInfo> {
    let version_path = format!("{}/{}/{}", bucket, slot, filename);
    let output = Command::new("gcloud")
        .env("CLOUDSDK_CORE_DISABLE_PROMPTS", "1")
        .args(["storage", "cat", &version_path])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let raw = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if raw.is_empty() {
        return None;
    }
    if let Some((version, major, minor)) = extract_version(&raw) {
        return Some(VersionInfo {
            version,
            major,
            minor,
        });
    }
    eprintln!("warning: failed to parse version from '{raw}' at slot {slot}");
    None
}

fn extract_version(raw: &str) -> Option<(String, u64, u64)> {
    for token in raw.split_whitespace() {
        let token = token
            .trim_matches(|c: char| !c.is_ascii_alphanumeric() && c != '.')
            .trim_start_matches('v');
        if token.contains('.') && token.chars().all(|c| c.is_ascii_digit() || c == '.') {
            let mut parts = token.split('.');
            let major_str = parts.next().unwrap_or(token);
            let minor_str = parts.next().unwrap_or("0");
            if let (Ok(major), Ok(minor)) = (major_str.parse::<u64>(), minor_str.parse::<u64>())
            {
                return Some((token.to_string(), major, minor));
            }
        }
    }
    None
}

fn main() -> Result<(), Box<dyn Error>> {
    let mut bucket = "gs://mainnet-beta-ledger-us-ny5".to_string();
    let mut breaking_only = false;
    for arg in std::env::args().skip(1) {
        match arg.as_str() {
            "--breaking" | "-b" => breaking_only = true,
            "--help" | "-h" => {
                println!(
                    "Usage: snapshot_version_bumps.rs [bucket] [--breaking]\n\n\
                     --breaking  Only print major/minor version boundaries"
                );
                return Ok(());
            }
            _ if !arg.starts_with('-') => bucket = arg,
            _ => {
                return Err(format!("unknown argument: {arg}").into());
            }
        }
    }
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

    let mut last_version: Option<VersionInfo> = None;
    let mut last_boundary: Option<(u64, u64)> = None;
    for slot in slots {
        let version = read_version(&bucket, slot, "version")
            .or_else(|| read_version(&bucket, slot, "version.txt"));
        let Some(version) = version else {
            continue;
        };

        let boundary = (version.major, version.minor);
        let should_print = if breaking_only {
            last_boundary.map_or(true, |last| last != boundary)
        } else {
            last_version
                .as_ref()
                .map_or(true, |last| last.version != version.version)
        };

        if should_print {
            println!("{slot}\t{}", &version.version);
        }

        last_boundary = Some(boundary);
        last_version = Some(version);
    }

    Ok(())
}
