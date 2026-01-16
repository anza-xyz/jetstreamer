use std::{
    env,
    path::{Path, PathBuf},
    process::exit,
};

use jetstreamer_firehose::epochs::epoch_to_slot_range;
use jetstreamer_node::snapshots::download_snapshot_at_or_before_slot;
use tokio::process::Command;

fn epoch_to_slot(epoch: u64) -> u64 {
    epoch_to_slot_range(epoch).0
}

fn usage(program: &str) -> String {
    format!("Usage: {program} <epoch> [dest-dir]")
}

async fn extract_tarball(archive: &Path, dest_dir: &Path) -> Result<(), String> {
    let output = Command::new("tar")
        .arg("-xf")
        .arg(archive)
        .arg("-C")
        .arg(dest_dir)
        .output()
        .await
        .map_err(|err| format!("failed to run tar: {err}"))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        let command = format!("tar -xf {} -C {}", archive.display(), dest_dir.display());
        if stderr.is_empty() {
            return Err(format!("{command} failed"));
        }
        return Err(format!("{command} failed: {stderr}"));
    }

    Ok(())
}

#[tokio::main]
async fn main() {
    let mut args = env::args();
    let program = args
        .next()
        .unwrap_or_else(|| "jetstreamer-node".to_string());
    let Some(epoch_arg) = args.next() else {
        eprintln!("{}", usage(&program));
        exit(2);
    };

    if epoch_arg == "-h" || epoch_arg == "--help" {
        println!("{}", usage(&program));
        return;
    }

    let epoch: u64 = match epoch_arg.parse() {
        Ok(epoch) => epoch,
        Err(err) => {
            eprintln!("invalid epoch '{epoch_arg}': {err}");
            eprintln!("{}", usage(&program));
            exit(2);
        }
    };

    let dest_dir = match args.next() {
        Some(path) => PathBuf::from(path),
        None => match env::current_dir() {
            Ok(path) => path,
            Err(err) => {
                eprintln!("failed to read current directory: {err}");
                exit(1);
            }
        },
    };

    let target_slot = epoch_to_slot(epoch).saturating_sub(1);
    let dest_path = match download_snapshot_at_or_before_slot(epoch, target_slot, &dest_dir).await {
        Ok(path) => path,
        Err(err) => {
            eprintln!("error: {err}");
            exit(1);
        }
    };

    println!("Downloaded snapshot to {}", dest_path.display());
    println!("Extracting snapshot into {}", dest_dir.display());
    if let Err(err) = extract_tarball(&dest_path, &dest_dir).await {
        eprintln!("error: {err}");
        exit(1);
    }
    println!("Extraction complete");
}
