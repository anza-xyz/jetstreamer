//! Quickly inspects the independently decodable boundaries of Horizon archives.
//!
//! Only the leading buckets through the first block and trailing buckets back
//! through the last block are decoded. This is useful for auditing cross-file
//! parent links without paying the cost of a complete archive scan.

use std::io::BufReader;
use std::path::Path;

use jetstreamer_horizon::archive::{ArchiveReader, BlockNotification, Consumption, SlotVisitor};
use solana_hash::Hash;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct BlockEdge {
    slot: u64,
    parent_slot: u64,
    parent_blockhash: Hash,
    blockhash: Hash,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ArchiveEdges {
    epoch: u64,
    slot_start: u64,
    slot_count: u64,
    initial_anchor: Hash,
    first: BlockEdge,
    last: BlockEdge,
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

fn archive_edges(path: &Path) -> Result<ArchiveEdges, Box<dyn std::error::Error>> {
    let file = std::fs::File::open(path)?;
    let mut reader = ArchiveReader::open(BufReader::with_capacity(16 << 20, file))?;
    let header = reader.header().clone();
    let bucket_count = reader.bucket_count();
    if bucket_count == 0 {
        return Err("archive has no buckets".into());
    }

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
    Ok(ArchiveEdges {
        epoch: header.epoch,
        slot_start: header.slot_start,
        slot_count: header.slot_count,
        initial_anchor,
        first,
        last,
    })
}

fn inspect(path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let file = std::fs::File::open(path)?;
    let reader = ArchiveReader::open(BufReader::with_capacity(16 << 20, file))?;
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
    let edges = archive_edges(path)?;
    println!(
        "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
        path.display(),
        edges.epoch,
        edges.slot_start,
        edges.slot_count,
        edges.initial_anchor,
        edges.first.slot,
        edges.first.parent_slot,
        edges.first.parent_blockhash,
        edges.last.slot,
        edges.last.blockhash,
        runtime_profile,
        generation_profile,
    );
    Ok(())
}

fn verify_pair_edges(left: ArchiveEdges, right: ArchiveEdges) -> Result<(), String> {
    let expected_epoch = left
        .epoch
        .checked_add(1)
        .ok_or_else(|| "left epoch overflows u64".to_owned())?;
    if right.epoch != expected_epoch {
        return Err(format!(
            "right epoch is {}, expected {expected_epoch}",
            right.epoch
        ));
    }

    let expected_start = left
        .slot_start
        .checked_add(left.slot_count)
        .ok_or_else(|| "left slot range overflows u64".to_owned())?;
    if right.slot_start != expected_start {
        return Err(format!(
            "right archive starts at slot {}, expected {expected_start}",
            right.slot_start
        ));
    }
    right
        .slot_start
        .checked_add(right.slot_count)
        .ok_or_else(|| "right slot range overflows u64".to_owned())?;

    if right.initial_anchor != left.last.blockhash {
        return Err(format!(
            "right initial PoH anchor {} does not match left terminal blockhash {}",
            right.initial_anchor, left.last.blockhash
        ));
    }
    if right.first.parent_slot != left.last.slot {
        return Err(format!(
            "right first block parent slot is {}, expected left terminal slot {}",
            right.first.parent_slot, left.last.slot
        ));
    }
    if right.first.parent_blockhash != left.last.blockhash {
        return Err(format!(
            "right first block parent hash {} does not match left terminal blockhash {}",
            right.first.parent_blockhash, left.last.blockhash
        ));
    }
    Ok(())
}

fn verify_pair(left_path: &Path, right_path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let left = archive_edges(left_path)?;
    let right = archive_edges(right_path)?;
    verify_pair_edges(left, right)?;
    println!(
        "BOUNDARY RESULT: OK: epoch {} terminal slot {} ({}) anchors epoch {} at slot {}; first block slot {} names the same canonical parent.",
        left.epoch,
        left.last.slot,
        left.last.blockhash,
        right.epoch,
        right.slot_start,
        right.first.slot,
    );
    Ok(())
}

fn main() {
    let mut paths = std::env::args_os().skip(1).collect::<Vec<_>>();
    if paths.is_empty() {
        eprintln!(
            "usage: archive_boundaries <archive.jet>...\n\
             or: archive_boundaries --verify-pair <left.jet> <right.jet>\n\
             columns: path epoch slot_start slot_count initial_anchor first_slot \
             first_parent_slot first_parent_hash last_slot last_hash runtime generation_profile"
        );
        std::process::exit(2);
    }
    if paths.first().is_some_and(|arg| arg == "--verify-pair") {
        paths.remove(0);
        if paths.len() != 2 {
            eprintln!("--verify-pair requires exactly two archive paths");
            std::process::exit(2);
        }
        if let Err(error) = verify_pair(Path::new(&paths[0]), Path::new(&paths[1])) {
            eprintln!("BOUNDARY RESULT: FAIL: {error}");
            std::process::exit(1);
        }
        return;
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

#[cfg(test)]
mod tests {
    use super::*;
    use jetstreamer_horizon::archive::{ArchiveWriter, ArchiveWriterConfig, BlockMeta};

    fn write_archive(
        path: &Path,
        epoch: u64,
        slot_start: u64,
        initial_anchor: Hash,
        frames: &[Option<(u64, Hash, Hash)>],
    ) {
        let file = std::fs::File::create(path).unwrap();
        let mut writer = ArchiveWriter::new(
            file,
            epoch,
            slot_start,
            frames.len() as u64,
            ArchiveWriterConfig::default(),
        )
        .unwrap();
        writer
            .preserve_initial_poh_anchor(slot_start, initial_anchor)
            .unwrap();
        for (offset, frame) in frames.iter().enumerate() {
            let slot = slot_start + offset as u64;
            match frame {
                None => writer.write_skipped_slot(slot).unwrap(),
                Some((parent_slot, parent_blockhash, blockhash)) => {
                    writer.begin_slot(slot).unwrap();
                    let mut meta = BlockMeta::new_boxed();
                    meta.slot = slot;
                    meta.parent_slot = *parent_slot;
                    meta.parent_blockhash = *parent_blockhash;
                    meta.blockhash = *blockhash;
                    writer.end_slot(&meta, &[]).unwrap();
                }
            }
        }
        writer.finish().unwrap();
    }

    #[test]
    fn accepts_exact_boundary_across_skipped_slots() {
        let directory = tempfile::tempdir().unwrap();
        let left_path = directory.path().join("epoch-7.jet");
        let right_path = directory.path().join("epoch-8.jet");
        let before_left = Hash::new_from_array([1; 32]);
        let terminal = Hash::new_from_array([2; 32]);
        let child = Hash::new_from_array([3; 32]);
        write_archive(
            &left_path,
            7,
            100,
            before_left,
            &[Some((99, before_left, terminal)), None, None],
        );
        write_archive(
            &right_path,
            8,
            103,
            terminal,
            &[None, Some((100, terminal, child))],
        );

        verify_pair(&left_path, &right_path).unwrap();
    }

    #[test]
    fn rejects_wrong_epoch_range_anchor_parent_slot_and_parent_hash() {
        let edge =
            |epoch, slot_start, initial_anchor, parent_slot, parent_blockhash| ArchiveEdges {
                epoch,
                slot_start,
                slot_count: 10,
                initial_anchor,
                first: BlockEdge {
                    slot: slot_start,
                    parent_slot,
                    parent_blockhash,
                    blockhash: Hash::new_from_array([4; 32]),
                },
                last: BlockEdge {
                    slot: slot_start + 9,
                    parent_slot: slot_start + 8,
                    parent_blockhash: Hash::new_from_array([4; 32]),
                    blockhash: Hash::new_from_array([5; 32]),
                },
            };
        let terminal = Hash::new_from_array([5; 32]);
        let left = edge(7, 100, Hash::new_unique(), 99, Hash::new_unique());
        let valid = edge(8, 110, terminal, 109, terminal);
        assert!(verify_pair_edges(left, valid).is_ok());

        assert!(verify_pair_edges(left, edge(9, 110, terminal, 109, terminal)).is_err());
        assert!(verify_pair_edges(left, edge(8, 111, terminal, 109, terminal)).is_err());
        assert!(verify_pair_edges(left, edge(8, 110, Hash::new_unique(), 109, terminal)).is_err());
        assert!(verify_pair_edges(left, edge(8, 110, terminal, 108, terminal)).is_err());
        assert!(verify_pair_edges(left, edge(8, 110, terminal, 109, Hash::new_unique())).is_err());
    }
}
