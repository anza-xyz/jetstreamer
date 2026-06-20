#!/usr/bin/env rust-script

use std::{
    collections::{BTreeMap, VecDeque},
    error::Error,
    io::{self, Write},
    process::Command,
    sync::{
        Arc, Mutex, mpsc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

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

fn format_duration(duration: Duration) -> String {
    let total_secs = duration.as_secs();
    let hours = total_secs / 3600;
    let minutes = (total_secs % 3600) / 60;
    let seconds = total_secs % 60;
    format!("{hours:02}:{minutes:02}:{seconds:02}")
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

    let workers = thread::available_parallelism()
        .map(|count| count.get())
        .unwrap_or(4)
        .min(32)
        .max(1);
    let workers = workers.min(slots.len().max(1));
    let queue = Arc::new(Mutex::new(VecDeque::from(slots)));
    let (tx, rx) = mpsc::channel::<(u64, VersionInfo)>();
    let done = Arc::new(AtomicUsize::new(0));
    let progress_done = Arc::new(AtomicBool::new(false));
    let total = queue.lock().unwrap().len();
    let progress_handle = if total > 0 {
        let done = Arc::clone(&done);
        let progress_done = Arc::clone(&progress_done);
        Some(thread::spawn(move || {
            let stderr = io::stderr();
            let mut handle = stderr.lock();
            let start = Instant::now();
            while !progress_done.load(Ordering::Relaxed) {
                let count = done.load(Ordering::Relaxed);
                let percent = (count as f64) * 100.0 / (total as f64);
                let eta = if count == 0 {
                    "unknown".to_string()
                } else {
                    let elapsed = start.elapsed().as_secs_f64();
                    if elapsed <= 0.0 {
                        "unknown".to_string()
                    } else {
                        let rate = (count as f64) / elapsed;
                        if rate <= 0.0 {
                            "unknown".to_string()
                        } else {
                            let remaining = total.saturating_sub(count);
                            let eta_secs = ((remaining as f64) / rate).ceil() as u64;
                            format_duration(Duration::from_secs(eta_secs))
                        }
                    }
                };
                let _ = write!(
                    handle,
                    "\rprogress: {count}/{total} ({percent:.2}%) eta={eta}"
                );
                let _ = handle.flush();
                thread::sleep(Duration::from_secs(1));
            }
            let _ = write!(handle, "\r{:<60}\r", "");
            let _ = handle.flush();
        }))
    } else {
        None
    };
    let mut handles = Vec::with_capacity(workers);

    for _ in 0..workers {
        let bucket = bucket.clone();
        let queue = Arc::clone(&queue);
        let tx = tx.clone();
        let done = Arc::clone(&done);
        handles.push(thread::spawn(move || loop {
            let slot = {
                let mut queue = queue.lock().unwrap();
                queue.pop_front()
            };
            let Some(slot) = slot else {
                break;
            };
            let version = read_version(&bucket, slot, "version")
                .or_else(|| read_version(&bucket, slot, "version.txt"));
            if let Some(version) = version {
                let _ = tx.send((slot, version));
            }
            done.fetch_add(1, Ordering::Relaxed);
        }));
    }

    drop(tx);
    for handle in handles {
        let _ = handle.join();
    }
    progress_done.store(true, Ordering::Relaxed);
    if let Some(handle) = progress_handle {
        let _ = handle.join();
    }

    let mut results = BTreeMap::new();
    for (slot, version) in rx {
        results.insert(slot, version);
    }

    let mut last_version: Option<VersionInfo> = None;
    let mut last_boundary: Option<(u64, u64)> = None;
    for (slot, version) in results {
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
