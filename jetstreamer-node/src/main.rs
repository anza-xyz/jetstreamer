use std::{env, path::PathBuf, process::exit};

use jetstreamer_firehose::epochs::epoch_to_slot_range;
use jetstreamer_node::snapshots::download_snapshot_at_or_before_slot;

fn epoch_to_slot(epoch: u64) -> u64 {
    epoch_to_slot_range(epoch).0
}

fn usage(program: &str) -> String {
    format!("Usage: {program} <epoch> [dest-dir]")
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
}
