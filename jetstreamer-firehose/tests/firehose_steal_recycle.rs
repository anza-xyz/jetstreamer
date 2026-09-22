use base64::{Engine, engine::general_purpose::STANDARD};
use cid::Cid;
use futures_util::FutureExt;
use jetstreamer_firehose::firehose::{
    BlockData, OnEntryFn, OnErrorFn, OnRewardFn, OnStatsTrackingFn, TransactionData, firehose,
    thread_activity,
};
use serde_cbor::Value::{self, Array, Bytes, Integer, Map, Null, Text};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeSet,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    process::Command,
    sync::Notify,
    time::{sleep, timeout},
};

const START: u64 = 432_000;
const SPLIT: u64 = START + 100;
const END: u64 = START + 200;

// The signed transaction already used in transaction.rs's decoding fixture (slot 16848004).
// Reusing it once per synthetic slot exercises signature verification with all features,
// without replaying hundreds of thousands of transactions from the public archive.
const TRANSACTION: &str = "AYbTMUdKwOfLPFey+AwyctaBtizbmzA4GiKpHwj+4ZrfKJu+xyl67fjZA6Nn1P8bg57V3OnuZVmUWyx8eSIdEwgBAAMFBRm4eNZlQLMYzIafIkHEG3bCnw0fIZY+Zqt/itnGLqcFGbhso5XTeMn5AgdGOiWLQlHMPlUD7ru2OG1kkuQjSgan1RcZLwqvxvJl4/t3zHragsUp0L47E24tAFUgAAAABqfVFxjHdMkoVmOYaR1etoteuKObS21cc1VbIQAAAAAHYUgdNXR0u3xNdiTr072z2DVec9EQQ/wNo1OAAAAAALY8zyGeltaQlaJeQ5wMCwZM8BOX2PZ5LVgiytnw6PELAQQEAQIDAD0CAAAAAgAAAAAAAAB9FAEBAAAAAH4UAQEAAAAA8qsHs5MMwvaTJoc++kGCUvyGn9od2r8SeheTKCk1uFgA";

fn varint(mut value: usize, output: &mut Vec<u8>) {
    while value >= 128 {
        output.push((value as u8 & 0x7f) | 0x80);
        value >>= 7;
    }
    output.push(value as u8);
}

fn append_node(car: &mut Vec<u8>, node: Value) -> Cid {
    let bytes = serde_cbor::to_vec(&node).unwrap();
    let digest = Sha256::digest(&bytes);
    let cid = Cid::new_v1(0x71, multihash::Multihash::wrap(0x12, &digest).unwrap());
    let cid_bytes = cid.to_bytes();
    varint(cid_bytes.len() + bytes.len(), car);
    car.extend(cid_bytes);
    car.extend(bytes);
    cid
}

fn link(cid: Cid) -> Value {
    let mut bytes = vec![0];
    bytes.extend(cid.to_bytes());
    Bytes(bytes)
}

fn frame(bytes: Vec<u8>) -> Value {
    Array(vec![Integer(6), Null, Null, Null, Bytes(bytes)])
}

fn archive() -> (Vec<u8>, Vec<u8>) {
    let mut nodes = Vec::new();
    let mut records = Vec::new();
    let transaction = STANDARD.decode(TRANSACTION).unwrap();
    let parsed: solana_transaction::versioned::VersionedTransaction =
        wincode::deserialize(&transaction).unwrap();
    parsed
        .verify_and_hash_message()
        .expect("valid fixture signature");

    // Include one extra block so the final worker can read past its half-open range.
    let mut root = None;
    for slot in START..=END {
        let offset = nodes.len();
        let tx_cid = append_node(
            &mut nodes,
            Array(vec![
                Integer(0),
                frame(transaction.clone()),
                frame(Vec::new()),
                Integer(slot.into()),
                Integer(0),
            ]),
        );
        let entry_cid = append_node(
            &mut nodes,
            Array(vec![
                Integer(1),
                Integer(1),
                Bytes(vec![0; 32]),
                Array(vec![link(tx_cid)]),
            ]),
        );
        root = Some(append_node(
            &mut nodes,
            Array(vec![
                Integer(2),
                Integer(slot.into()),
                Array(vec![]),
                Array(vec![link(entry_cid)]),
                Array(vec![
                    Integer((slot - 1).into()),
                    Integer(0),
                    Integer(slot.into()),
                ]),
            ]),
        ));
        records.push((offset, nodes.len() - offset));
    }

    let header = serde_cbor::to_vec(&Map([
        (Text("roots".into()), Array(vec![link(root.unwrap())])),
        (Text("version".into()), Integer(1)),
    ]
    .into()))
    .unwrap();
    let mut car = Vec::new();
    varint(header.len(), &mut car);
    car.extend(header);
    let header_len = car.len();
    car.extend(nodes);

    let mut index = vec![0; 432_000 * 12];
    for (slot, (offset, length)) in records.into_iter().enumerate() {
        index[slot * 12..slot * 12 + 8]
            .copy_from_slice(&((offset + header_len) as u64).to_le_bytes());
        index[slot * 12 + 8..slot * 12 + 12].copy_from_slice(&(length as u32).to_le_bytes());
    }
    (car, index)
}

async fn respond(mut stream: TcpStream, car: Arc<Vec<u8>>, index: Arc<Vec<u8>>) {
    let mut request = Vec::new();
    while !request.ends_with(b"\r\n\r\n") {
        request.push(stream.read_u8().await.unwrap());
        assert!(request.len() < 16_384, "oversized fixture request");
    }
    let request = String::from_utf8(request).unwrap();
    let mut parts = request.lines().next().unwrap().split_whitespace();
    let method = parts.next().unwrap();
    let body = match parts.next().unwrap() {
        "/1/epoch-1.car" => car,
        "/1/epoch-1-slot-ranges.raw" => index,
        path => panic!("unexpected fixture request: {path}"),
    };
    let range = request.lines().find_map(|line| {
        let (name, value) = line.split_once(':')?;
        name.eq_ignore_ascii_case("range").then_some(value.trim())
    });
    let (status, start, end, content_range) = if let Some(range) = range {
        let (start, end) = range
            .strip_prefix("bytes=")
            .unwrap()
            .split_once('-')
            .unwrap();
        let start: usize = start.parse().unwrap();
        let end = end
            .parse::<usize>()
            .unwrap_or(body.len() - 1)
            .min(body.len() - 1);
        (
            "206 Partial Content",
            start,
            end + 1,
            format!("Content-Range: bytes {start}-{end}/{}\r\n", body.len()),
        )
    } else {
        ("200 OK", 0, body.len(), String::new())
    };
    let headers = format!(
        "HTTP/1.1 {status}\r\nContent-Length: {}\r\nAccept-Ranges: bytes\r\n{content_range}Connection: close\r\n\r\n",
        end - start
    );
    // Readers intentionally close responses when seeking or recycling a connection.
    if stream.write_all(headers.as_bytes()).await.is_ok() && method != "HEAD" {
        let _ = stream.write_all(&body[start..end]).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stolen_lower_range_keeps_callbacks_after_recycle() {
    const CHILD: &str = "JETSTREAMER_STEAL_RECYCLE_TEST_CHILD";
    if std::env::var_os(CHILD).is_some() {
        assert_callbacks_after_recycle().await;
        return;
    }

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}/", listener.local_addr().unwrap());
    let (car, index) = archive();
    let (car, index) = (Arc::new(car), Arc::new(index));
    let server = tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            tokio::spawn(respond(stream, car.clone(), index.clone()));
        }
    });

    // Configure a fresh process before its archive globals or runtime threads start.
    // This also isolates the firehose's global worker counters from other tests.
    let status = timeout(
        Duration::from_secs(45),
        Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "stolen_lower_range_keeps_callbacks_after_recycle",
                "--nocapture",
            ])
            .env(CHILD, "1")
            .env("JETSTREAMER_ARCHIVE_BACKEND", "http")
            .env("JETSTREAMER_HTTP_BASE_URL", &base)
            .env("JETSTREAMER_COMPACT_INDEX_BASE_URL", &base)
            .env("JETSTREAMER_FORCE_LEGACY_INDEX", "0")
            .env("JETSTREAMER_SPAWN_GRACE_SECS", "0")
            .env("JETSTREAMER_RECYCLE_PCT", "0")
            .kill_on_drop(true)
            .status(),
    )
    .await
    .expect("fixture child timed out")
    .expect("start fixture child");
    server.abort();
    assert!(
        status.success(),
        "steal/recycle regression failed: {status}"
    );
}

async fn assert_callbacks_after_recycle() {
    let release_victim = Arc::new(Notify::new());
    let victim_paused = Arc::new(AtomicBool::new(false));
    let transaction_slots = Arc::new(Mutex::new(BTreeSet::new()));
    let block_slots = Arc::new(Mutex::new(BTreeSet::new()));
    let first_stolen_slot = Arc::new(AtomicU64::new(0));

    let run = firehose(
        2,
        false,
        false,
        None,
        START..END,
        Some({
            let release_victim = release_victim.clone();
            let block_slots = block_slots.clone();
            let first_stolen_slot = first_stolen_slot.clone();
            move |thread_id: usize, block: BlockData| {
                let release_victim = release_victim.clone();
                let victim_paused = victim_paused.clone();
                let block_slots = block_slots.clone();
                let first_stolen_slot = first_stolen_slot.clone();
                async move {
                    if thread_id == 0 {
                        if !victim_paused.swap(true, Ordering::SeqCst) {
                            release_victim.notified().await;
                        }
                        sleep(Duration::from_millis(20)).await;
                    } else if block.slot() == END - 1 {
                        release_victim.notify_one();
                    } else if block.slot() < SPLIT {
                        block_slots.lock().unwrap().insert(block.slot());
                        if first_stolen_slot
                            .compare_exchange(0, block.slot(), Ordering::SeqCst, Ordering::SeqCst)
                            .is_ok()
                        {
                            thread_activity::request_recycle(thread_id);
                        }
                    }
                    Ok(())
                }
                .boxed()
            }
        }),
        Some({
            let transaction_slots = transaction_slots.clone();
            move |thread_id: usize, transaction: TransactionData| {
                let transaction_slots = transaction_slots.clone();
                async move {
                    if thread_id == 1 && transaction.slot < SPLIT {
                        transaction_slots.lock().unwrap().insert(transaction.slot);
                    }
                    Ok(())
                }
                .boxed()
            }
        }),
        None::<OnEntryFn>,
        None::<OnRewardFn>,
        None::<OnErrorFn>,
        None::<OnStatsTrackingFn>,
        None,
    );
    timeout(Duration::from_secs(30), run)
        .await
        .expect("local steal/recycle run timed out")
        .expect("firehose failed");

    assert!(thread_activity::steal_count() > 0);
    assert_eq!(thread_activity::recycle_count(), 1);
    let first = first_stolen_slot.load(Ordering::SeqCst);
    assert!(
        first > 0,
        "worker 1 must emit a stolen block before recycling"
    );
    let transactions = transaction_slots.lock().unwrap();
    let blocks = block_slots.lock().unwrap();
    assert!(
        transactions.iter().any(|slot| *slot > first),
        "transactions must continue after recycling"
    );
    assert!(
        blocks.iter().any(|slot| *slot > first),
        "blocks must continue after recycling"
    );
    assert_eq!(
        *transactions, *blocks,
        "every stolen transaction slot needs a block callback"
    );
}
