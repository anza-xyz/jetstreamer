//! Quickly inspects the independently decodable boundaries of Horizon archives.
//!
//! Only the leading buckets through the first block and trailing buckets back
//! through the last block are decoded. This is useful for auditing cross-file
//! parent links without paying the cost of a complete archive scan.

use std::io::BufReader;
use std::path::Path;

use jetstreamer_horizon::archive::{ArchiveReader, BlockNotification, Consumption, SlotVisitor};
use solana_hash::Hash;

#[derive(Clone, Copy)]
struct BlockEdge {
    slot: u64,
    parent_slot: u64,
    parent_blockhash: Hash,
    blockhash: Hash,
}

#[derive(Default)]
struct EdgeVisitor {
    first: Option<BlockEdge>,
    last: Option<BlockEdge>,
}

impl SlotVisitor for EdgeVisitor {
    fn on_block(
        &mut self,
        notification: &BlockNotification,
        _entries: &[jetstreamer_horizon::archive::EntryRecord],
    ) {
        if let BlockNotification::Block(meta) = notification {
            let edge = BlockEdge {
                slot: meta.slot,
                parent_slot: meta.parent_slot,
                parent_blockhash: meta.parent_blockhash,
                blockhash: meta.blockhash,
            };
            self.first.get_or_insert(edge);
            self.last = Some(edge);
        }
    }

    fn consumption(&self) -> Consumption {
        Consumption::all()
            .without_account_update_data()
            .without_block_account_update_arenas()
    }
}

fn inspect(path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let file = std::fs::File::open(path)?;
    let mut reader = ArchiveReader::open(BufReader::with_capacity(16 << 20, file))?;
    let header = reader.header().clone();
    let bucket_count = reader.bucket_count();
    if bucket_count == 0 {
        return Err("archive has no buckets".into());
    }

    let provenance = reader.provenance()?;
    let (runtime_profile, generation_profile) = provenance
        .as_ref()
        .and_then(|value| value.single_runtime_v1())
        .map_or(("-", "-"), |value| {
            (
                value.runtime_profile.as_str(),
                value.generation_profile.as_str(),
            )
        });

    let mut initial_anchor = None;
    let mut first = None;
    for bucket in 0..bucket_count {
        let mut visitor = EdgeVisitor::default();
        reader.read_bucket_with_header(bucket, &mut visitor, |bucket_header, _| {
            if bucket == 0 {
                initial_anchor = Some(bucket_header.poh_start_hash);
            }
            Ok(())
        })?;
        if visitor.first.is_some() {
            first = visitor.first;
            break;
        }
    }

    let mut last = None;
    for bucket in (0..bucket_count).rev() {
        let mut visitor = EdgeVisitor::default();
        reader.read_bucket(bucket, &mut visitor)?;
        if visitor.last.is_some() {
            last = visitor.last;
            break;
        }
    }

    let first = first.ok_or("archive contains no blocks")?;
    let last = last.ok_or("archive contains no blocks")?;
    let initial_anchor = initial_anchor.ok_or("archive has no initial PoH anchor")?;
    println!(
        "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
        path.display(),
        header.epoch,
        header.slot_start,
        header.slot_count,
        initial_anchor,
        first.slot,
        first.parent_slot,
        first.parent_blockhash,
        last.slot,
        last.blockhash,
        runtime_profile,
        generation_profile,
    );
    Ok(())
}

fn main() {
    let paths = std::env::args_os().skip(1).collect::<Vec<_>>();
    if paths.is_empty() {
        eprintln!(
            "usage: archive_boundaries <archive.jet>...\n\
             columns: path epoch slot_start slot_count initial_anchor first_slot \
             first_parent_slot first_parent_hash last_slot last_hash runtime generation_profile"
        );
        std::process::exit(2);
    }
    let mut failed = false;
    for path in paths {
        let path = Path::new(&path);
        if let Err(error) = inspect(path) {
            eprintln!("{}: {error}", path.display());
            failed = true;
        }
    }
    if failed {
        std::process::exit(1);
    }
}
