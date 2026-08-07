//! Horizon-native firehose: streams a `.jet` archive (the horizon
//! container) instead of an Old Faithful CAR, driving the zero-copy
//! [`SlotVisitor`] callbacks.
//!
//! This is the network-seekable counterpart to [`firehose`](crate::firehose).
//! Bytes are fetched **async** over the same backends the rest of the crate
//! uses — `rseek::Seekable` for HTTP range requests, `tokio::fs` for local
//! files — while slot-frame **decode** stays sync and zero-copy in
//! [`jetstreamer_horizon`]'s [`BucketDecoder`]. The two are bridged per
//! worker by a small bounded channel: an async fetch task pulls the worker's
//! bucket range over its own connection and hands raw bytes to a blocking
//! decode task. The channel's depth gives one-bucket prefetch, so network
//! latency overlaps CPU decode.
//!
//! `.jet` buckets are independent zstd frames, so a slot lives in exactly
//! one bucket and each bucket range is owned by exactly one worker — there
//! is no cross-worker coordination.
use std::io;
use std::ops::Range;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use reqwest::{Client, Url};
use rseek::Seekable;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt, BufReader, ReadBuf};

use jetstreamer_horizon::archive::{
    ArchiveFormatError, BlockNotification, BucketDecoder, BucketIndexEntry, Consumption,
    EntryRecord, EpochMeta, FOOTER_LEN, Footer, SlotVisitor, bucket_containing, parse_bucket_index,
    parse_file_header,
};
use jetstreamer_horizon::transactions::Transaction;

use crate::epochs::EpochStream;
use crate::node_reader::Len;

/// Bytes fetched from the file front to cover the header section. The
/// `FileHeader` is small; a few KiB is always enough.
const HEADER_PREFIX: usize = 16 * 1024;
/// Buckets a fetch task may run ahead of its decoder (prefetch depth).
const PREFETCH_DEPTH: usize = 2;

/// Where `.jet` files live. Each epoch resolves to `epoch-<N>.jet`.
#[derive(Clone)]
pub enum JetSource {
    /// One epoch's `.jet` bytes held entirely in memory (e.g. preloaded by a
    /// benchmark to take disk I/O out of the measurement). Serves only its
    /// own epoch; workers share the buffer via cheap `Arc` clones.
    ///
    /// `Arc<Vec<u8>>` rather than `Arc<[u8]>` deliberately: converting a
    /// `Vec` to `Arc<[u8]>` copies the whole buffer (the slice must live
    /// inline with the Arc header), which for a ~400 GB epoch means two
    /// full-size allocations alive at once — an instant OOM. Wrapping the
    /// Vec is copy-free.
    Memory {
        /// The epoch these bytes are the archive for.
        epoch: u64,
        /// The complete `.jet` file contents.
        bytes: std::sync::Arc<Vec<u8>>,
    },
    /// A local directory holding `epoch-<N>.jet`, read with `tokio::fs`.
    LocalDir(PathBuf),
    /// A base URL under which `epoch-<N>.jet` is served, seeked with
    /// `rseek` HTTP range requests. Must end with `/` to join correctly.
    HttpBase(Url),
}

impl JetSource {
    /// A local directory of `epoch-<N>.jet` files.
    pub fn local(dir: impl Into<PathBuf>) -> Self {
        Self::LocalDir(dir.into())
    }

    /// A base URL serving `epoch-<N>.jet`. A trailing `/` is appended if
    /// missing so the per-epoch path joins as a sub-path, not a sibling.
    pub fn http(base_url: &str) -> Result<Self, HorizonFirehoseError> {
        let normalized = if base_url.ends_with('/') {
            base_url.to_string()
        } else {
            format!("{base_url}/")
        };
        let url = Url::parse(&normalized).map_err(|e| HorizonFirehoseError::Url(e.to_string()))?;
        Ok(Self::HttpBase(url))
    }

    /// One epoch's complete `.jet` contents held in memory. Serves only
    /// `epoch`; requesting any other epoch errors. Takes the `Vec` by value
    /// and never copies it — callers hand over buffers the size of a whole
    /// epoch file.
    pub fn in_memory(epoch: u64, bytes: Vec<u8>) -> Self {
        Self::Memory {
            epoch,
            bytes: std::sync::Arc::new(bytes),
        }
    }
}

impl std::fmt::Debug for JetSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Memory { epoch, bytes } => f
                .debug_struct("Memory")
                .field("epoch", epoch)
                .field("len", &bytes.len())
                .finish(),
            Self::LocalDir(dir) => f.debug_tuple("LocalDir").field(dir).finish(),
            Self::HttpBase(url) => f.debug_tuple("HttpBase").field(url).finish(),
        }
    }
}

/// Errors from the horizon firehose.
#[derive(Debug)]
pub enum HorizonFirehoseError {
    /// Network or filesystem I/O error fetching `.jet` bytes.
    Io(io::Error),
    /// The fetched bytes did not decode as a valid horizon archive.
    Archive(ArchiveFormatError),
    /// The `.jet` URL could not be built from the base + epoch.
    Url(String),
    /// The file is shorter than a footer — not a horizon archive.
    Truncated,
    /// A worker task panicked or was cancelled.
    Join(String),
}

impl std::fmt::Display for HorizonFirehoseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "io error: {e}"),
            Self::Archive(e) => write!(f, "archive decode error: {e}"),
            Self::Url(e) => write!(f, "invalid .jet url: {e}"),
            Self::Truncated => write!(f, "file shorter than a horizon footer"),
            Self::Join(e) => write!(f, "worker task failed: {e}"),
        }
    }
}

impl std::error::Error for HorizonFirehoseError {}

impl From<io::Error> for HorizonFirehoseError {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}

impl From<ArchiveFormatError> for HorizonFirehoseError {
    fn from(e: ArchiveFormatError) -> Self {
        Self::Archive(e)
    }
}

/// Streams the `.jet` for `epoch` (restricted to `slot_range`) across
/// `threads` workers, driving a fresh [`SlotVisitor`] per worker built by
/// `make_visitor`. Returns the per-worker visitors in worker order, for the
/// caller to finalize (flush). Worker `t` owns a contiguous bucket range, so
/// every slot is visited exactly once across the workers.
pub async fn firehose_horizon<F, V>(
    threads: usize,
    src: JetSource,
    epoch: u64,
    slot_range: Range<u64>,
    make_visitor: F,
) -> Result<Vec<V>, HorizonFirehoseError>
where
    F: Fn(usize) -> V,
    V: SlotVisitor + Send + 'static,
{
    let threads = threads.max(1);
    let client = Client::new();

    // --- read framing once (header at the front, footer + index at the tail) ---
    let mut probe = open_jet_stream(&src, epoch, &client).await?;
    let total_len = probe.len();
    if total_len < FOOTER_LEN as u64 {
        return Err(HorizonFirehoseError::Truncated);
    }
    let prefix_len = (HEADER_PREFIX as u64).min(total_len) as usize;
    let prefix = read_range(&mut probe, 0, prefix_len).await?;
    let (header, _) = parse_file_header(&prefix)?;
    let footer_bytes = read_range(&mut probe, total_len - FOOTER_LEN as u64, FOOTER_LEN).await?;
    let footer_arr: [u8; FOOTER_LEN] = footer_bytes
        .as_slice()
        .try_into()
        .map_err(|_| HorizonFirehoseError::Truncated)?;
    let footer = Footer::from_bytes(&footer_arr)?;
    let index_bytes =
        read_range(&mut probe, footer.index_offset, footer.index_len as usize).await?;
    let index: Arc<[BucketIndexEntry]> = parse_bucket_index(&index_bytes, &footer)?.into();
    drop(probe);

    if index.is_empty() || slot_range.is_empty() {
        return Ok((0..threads).map(&make_visitor).collect());
    }

    // --- buckets covering slot_range, partitioned across workers ---
    let start_bucket = bucket_containing(&header, &index, slot_range.start);
    let end_bucket = bucket_containing(&header, &index, slot_range.end - 1);
    let chunks = partition_buckets(start_bucket, end_bucket, threads);

    // --- one (async fetcher + blocking decoder) pair per worker ---
    let mut fetchers = Vec::with_capacity(threads);
    let mut decoders = Vec::with_capacity(threads);
    for (tid, chunk) in chunks.into_iter().enumerate() {
        let (tx, rx) = tokio::sync::mpsc::channel::<RawBucket>(PREFETCH_DEPTH);
        // Drained fetch buffers travel back to the fetcher for reuse, so the
        // stream path allocates a bounded handful of buffers per worker for
        // the whole run instead of one ~bucket-sized Vec per bucket. Sized so
        // a return can never block: at most PREFETCH_DEPTH buffers sit in
        // `tx` plus one in each task.
        let (recycle_tx, mut recycle_rx) =
            tokio::sync::mpsc::channel::<Vec<u8>>(PREFETCH_DEPTH + 2);

        let src2 = src.clone();
        let client2 = client.clone();
        let index2 = index.clone();
        fetchers.push(tokio::spawn(async move {
            // In-memory source: hand the decoder shared slices of the one
            // preloaded buffer — zero per-bucket buffers, zero copies.
            if let JetSource::Memory { bytes, .. } = &src2 {
                for b in chunk {
                    let e = index2[b];
                    let bucket_start = e.offset as usize;
                    let bucket_end = bucket_start + e.len as usize;
                    if bucket_end > bytes.len() {
                        return Err(HorizonFirehoseError::Truncated);
                    }
                    let shared = RawBucket::Shared {
                        bytes: bytes.clone(),
                        range: bucket_start..bucket_end,
                    };
                    if tx.send(shared).await.is_err() {
                        break; // decoder finished early or errored
                    }
                }
                return Ok(());
            }

            let mut s = open_jet_stream(&src2, epoch, &client2).await?;
            for b in chunk {
                let e = index2[b];
                let mut buf = recycle_rx.try_recv().unwrap_or_default();
                read_range_into(&mut s, e.offset, e.len as usize, &mut buf).await?;
                if tx.send(RawBucket::Owned(buf)).await.is_err() {
                    break; // decoder finished early (reached end slot) or errored
                }
            }
            Ok::<(), HorizonFirehoseError>(())
        }));

        let mut visitor = make_visitor(tid);
        let start = slot_range.start;
        let end = slot_range.end;
        decoders.push(tokio::task::spawn_blocking(move || {
            let mut rx = rx;
            let mut decoder = BucketDecoder::new();
            decoder.verify_chain = false;
            // Honor the visitor's declared consumption: unconsumed
            // account-update data is skipped, not reconstructed.
            decoder.materialize_account_data = visitor.consumption().account_update_data;
            'outer: while let Some(raw) = rx.blocking_recv() {
                decoder.load_bucket_bytes(raw.as_slice())?;
                // The decoder copied/decompressed what it needs; return the
                // buffer to the fetcher (capacity ensures this never blocks;
                // a closed channel just means the fetcher already finished).
                if let RawBucket::Owned(buf) = raw {
                    let _ = recycle_tx.try_send(buf);
                }
                let mut bounded = Bounded {
                    inner: &mut visitor,
                    end,
                };
                while decoder.slots_remaining() > 0 {
                    decoder.decode_slot_frame(start, &mut bounded)?;
                    if decoder.last_decoded_slot().is_some_and(|s| s + 1 >= end) {
                        break 'outer; // past the requested range; stop this worker
                    }
                }
            }
            Ok::<V, HorizonFirehoseError>(visitor)
        }));
    }

    // --- join: drain fetchers, collect visitors, surface the first error ---
    let mut first_err: Option<HorizonFirehoseError> = None;
    for f in fetchers {
        match f.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                first_err.get_or_insert(e);
            }
            Err(j) => {
                first_err.get_or_insert(HorizonFirehoseError::Join(j.to_string()));
            }
        }
    }
    let mut visitors = Vec::with_capacity(threads);
    for d in decoders {
        match d.await {
            Ok(Ok(v)) => visitors.push(v),
            Ok(Err(e)) => {
                first_err.get_or_insert(e);
            }
            Err(j) => {
                first_err.get_or_insert(HorizonFirehoseError::Join(j.to_string()));
            }
        }
    }
    if let Some(e) = first_err {
        return Err(e);
    }
    Ok(visitors)
}

/// Visitor wrapper that drops slots at or beyond `end` (the exclusive upper
/// bound of the requested range), so a worker's final bucket does not emit
/// slots past the range.
struct Bounded<'a, V: SlotVisitor> {
    inner: &'a mut V,
    end: u64,
}

impl<V: SlotVisitor> SlotVisitor for Bounded<'_, V> {
    fn on_epoch(&mut self, meta: &EpochMeta) {
        self.inner.on_epoch(meta);
    }
    fn on_transaction(&mut self, slot: u64, tx_index: u32, tx: &Transaction) {
        if slot < self.end {
            self.inner.on_transaction(slot, tx_index, tx);
        }
    }
    fn on_block(&mut self, notification: &BlockNotification, entries: &[EntryRecord]) {
        if notification.slot() < self.end {
            self.inner.on_block(notification, entries);
        }
    }
    fn consumption(&self) -> Consumption {
        self.inner.consumption()
    }
}

/// Splits the inclusive bucket range `[start, end_inclusive]` into `threads`
/// contiguous chunks (trailing chunks are empty when there are fewer buckets
/// than workers).
fn partition_buckets(start: usize, end_inclusive: usize, threads: usize) -> Vec<Range<usize>> {
    let total = end_inclusive + 1 - start;
    let per = total / threads;
    let rem = total % threads;
    let mut out = Vec::with_capacity(threads);
    let mut s = start;
    for i in 0..threads {
        let n = per + usize::from(i < rem);
        out.push(s..s + n);
        s += n;
    }
    out
}

/// Opens an async, seekable stream over `epoch`'s `.jet` from `src`.
async fn open_jet_stream(
    src: &JetSource,
    epoch: u64,
    client: &Client,
) -> Result<EpochStream, HorizonFirehoseError> {
    match src {
        JetSource::Memory { epoch: held, bytes } => {
            if *held != epoch {
                return Err(HorizonFirehoseError::Url(format!(
                    "in-memory source holds epoch {held}, not {epoch}"
                )));
            }
            Ok(EpochStream::new(MemJetReader {
                bytes: bytes.clone(),
                pos: 0,
            }))
        }
        JetSource::LocalDir(dir) => {
            let path = dir.join(format!("epoch-{epoch}.jet"));
            let file = tokio::fs::File::open(&path).await?;
            let len = file.metadata().await?.len();
            Ok(EpochStream::new(LocalJetReader { file, len }))
        }
        JetSource::HttpBase(base) => {
            let url = base
                .join(&format!("epoch-{epoch}.jet"))
                .map_err(|e| HorizonFirehoseError::Url(e.to_string()))?;
            let req_url = url.to_string();
            let c = client.clone();
            let seekable = Seekable::new(move || c.get(req_url.clone())).await;
            Ok(EpochStream::new(BufReader::with_capacity(
                1 << 20,
                seekable,
            )))
        }
    }
}

/// One bucket's raw bytes on their way from a fetcher to its decoder:
/// either a fetched buffer the decoder recycles back afterwards, or — for
/// the in-memory source — a shared slice of the single preloaded file
/// buffer (no per-bucket buffer, no copy).
enum RawBucket {
    Owned(Vec<u8>),
    Shared {
        bytes: Arc<Vec<u8>>,
        range: Range<usize>,
    },
}

impl RawBucket {
    fn as_slice(&self) -> &[u8] {
        match self {
            Self::Owned(buf) => buf,
            Self::Shared { bytes, range } => &bytes[range.clone()],
        }
    }
}

/// Seeks to `offset` and reads exactly `len` bytes — one bucket frame (or a
/// framing range) per call, mapping to a single HTTP range request.
async fn read_range(stream: &mut EpochStream, offset: u64, len: usize) -> io::Result<Vec<u8>> {
    stream.seek(io::SeekFrom::Start(offset)).await?;
    let mut buf = vec![0u8; len];
    stream.read_exact(&mut buf).await?;
    Ok(buf)
}

/// [`read_range`] into a caller-provided (recycled) buffer: no allocation
/// once the buffer has grown to bucket size.
async fn read_range_into(
    stream: &mut EpochStream,
    offset: u64,
    len: usize,
    buf: &mut Vec<u8>,
) -> io::Result<()> {
    stream.seek(io::SeekFrom::Start(offset)).await?;
    buf.clear();
    buf.resize(len, 0);
    stream.read_exact(buf).await?;
    Ok(())
}

/// A local `.jet` file as an `AsyncRead + AsyncSeek + Len` reader, so it
/// shares the [`EpochStream`] code path with the network backends.
struct LocalJetReader {
    file: tokio::fs::File,
    len: u64,
}

impl AsyncRead for LocalJetReader {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().file).poll_read(cx, buf)
    }
}

impl AsyncSeek for LocalJetReader {
    fn start_seek(self: Pin<&mut Self>, position: io::SeekFrom) -> io::Result<()> {
        Pin::new(&mut self.get_mut().file).start_seek(position)
    }
    fn poll_complete(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<u64>> {
        Pin::new(&mut self.get_mut().file).poll_complete(cx)
    }
}

impl Len for LocalJetReader {
    fn len(&self) -> u64 {
        self.len
    }
}

/// A fully in-memory `.jet` as an `AsyncRead + AsyncSeek + Len` reader —
/// every read is a memcpy from the shared buffer, always `Ready`.
struct MemJetReader {
    bytes: std::sync::Arc<Vec<u8>>,
    pos: u64,
}

impl AsyncRead for MemJetReader {
    fn poll_read(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let start = (this.pos as usize).min(this.bytes.len());
        let n = buf.remaining().min(this.bytes.len() - start);
        buf.put_slice(&this.bytes[start..start + n]);
        this.pos += n as u64;
        Poll::Ready(Ok(()))
    }
}

impl AsyncSeek for MemJetReader {
    fn start_seek(self: Pin<&mut Self>, position: io::SeekFrom) -> io::Result<()> {
        let this = self.get_mut();
        let new = match position {
            io::SeekFrom::Start(off) => off as i128,
            io::SeekFrom::End(off) => this.bytes.len() as i128 + off as i128,
            io::SeekFrom::Current(off) => this.pos as i128 + off as i128,
        };
        if new < 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "seek before start",
            ));
        }
        this.pos = new as u64;
        Ok(())
    }
    fn poll_complete(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<u64>> {
        Poll::Ready(Ok(self.get_mut().pos))
    }
}

impl Len for MemJetReader {
    fn len(&self) -> u64 {
        self.bytes.len() as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use jetstreamer_horizon::archive::{ArchiveWriter, ArchiveWriterConfig};

    /// Counts skipped/block slots seen, to verify the driver visited them all.
    #[derive(Default)]
    struct SlotCounter {
        skipped: u64,
        blocks: u64,
    }

    impl SlotVisitor for SlotCounter {
        fn on_block(&mut self, notification: &BlockNotification, _entries: &[EntryRecord]) {
            match notification {
                BlockNotification::Skipped(_) => self.skipped += 1,
                BlockNotification::Block(_) => self.blocks += 1,
            }
        }
    }

    /// End-to-end over the local backend: build a tiny `.jet`, run it through
    /// the full async driver (framing → bucket partition → prefetch fetch →
    /// blocking decode → join), and confirm every slot is visited exactly once.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn local_jet_drives_every_slot_once() {
        let epoch = 7u64;
        let slot_start = 1_000u64;
        let n = 300u64; // 3 buckets at the default 128 slots/bucket

        // A skipped-only archive is a fully valid `.jet` and needs no
        // transaction construction.
        let mut writer = ArchiveWriter::new(
            std::io::Cursor::new(Vec::new()),
            epoch,
            slot_start,
            n,
            ArchiveWriterConfig::default(),
        )
        .unwrap();
        for i in 0..n {
            writer.write_skipped_slot(slot_start + i).unwrap();
        }
        let (sink, _stats) = writer.finish().unwrap();

        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join(format!("epoch-{epoch}.jet")),
            sink.into_inner(),
        )
        .unwrap();

        // 4 workers over 3 buckets exercises a trailing empty worker too.
        let visitors = firehose_horizon(
            4,
            JetSource::local(dir.path()),
            epoch,
            slot_start..slot_start + n,
            |_tid| SlotCounter::default(),
        )
        .await
        .unwrap();

        assert_eq!(visitors.len(), 4);
        let skipped: u64 = visitors.iter().map(|v| v.skipped).sum();
        let blocks: u64 = visitors.iter().map(|v| v.blocks).sum();
        assert_eq!(skipped, n, "every skipped slot visited exactly once");
        assert_eq!(blocks, 0);
    }

    /// A sub-range that starts and ends mid-bucket must visit only the
    /// requested slots (lower bound via the decoder, upper bound via `Bounded`).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn local_jet_respects_sub_range() {
        let epoch = 8u64;
        let slot_start = 0u64;
        let n = 300u64;

        let mut writer = ArchiveWriter::new(
            std::io::Cursor::new(Vec::new()),
            epoch,
            slot_start,
            n,
            ArchiveWriterConfig::default(),
        )
        .unwrap();
        for i in 0..n {
            writer.write_skipped_slot(slot_start + i).unwrap();
        }
        let (sink, _stats) = writer.finish().unwrap();

        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join(format!("epoch-{epoch}.jet")),
            sink.into_inner(),
        )
        .unwrap();

        // 50..200 spans bucket boundaries on both ends (buckets are 128 wide).
        let visitors = firehose_horizon(3, JetSource::local(dir.path()), epoch, 50..200, |_tid| {
            SlotCounter::default()
        })
        .await
        .unwrap();

        let skipped: u64 = visitors.iter().map(|v| v.skipped).sum();
        assert_eq!(skipped, 150, "only slots [50, 200) visited");
    }

    /// A visitor that declares it does not consume account-update data must
    /// get elided (empty) data slices with metadata and counts intact — the
    /// declaration travels visitor → `Bounded` → decoder through the full
    /// async driver.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn local_jet_elides_update_data_when_declined() {
        use jetstreamer_horizon::account_updates::AccountUpdateView;
        use jetstreamer_horizon::block_metas::BlockMeta;
        use solana_address::Address;
        use solana_hash::Hash;

        let epoch = 9u64;
        let slot_start = 0u64;
        let n = 10u64;

        let mut writer = ArchiveWriter::new(
            std::io::Cursor::new(Vec::new()),
            epoch,
            slot_start,
            n,
            ArchiveWriterConfig::default(),
        )
        .unwrap();
        let owner = Address::new_from_array([7u8; 32]);
        for i in 0..n {
            let slot = slot_start + i;
            writer.begin_slot(slot).unwrap();
            writer
                .write_orphan_update(&AccountUpdateView {
                    pubkey: Address::new_from_array([11u8; 32]),
                    lamports: i,
                    owner,
                    executable: false,
                    rent_epoch: u64::MAX,
                    write_version: i,
                    data: &[0xAB; 64],
                })
                .unwrap();
            let mut meta = BlockMeta::new_boxed();
            meta.slot = slot;
            meta.parent_slot = slot.saturating_sub(1);
            meta.blockhash = Hash::new_from_array([i as u8 + 1; 32]);
            writer.end_slot(&meta, &[]).unwrap();
        }
        let (sink, _stats) = writer.finish().unwrap();

        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join(format!("epoch-{epoch}.jet")),
            sink.into_inner(),
        )
        .unwrap();

        /// Tallies orphan updates and their data bytes; declines data.
        #[derive(Default)]
        struct DecliningTally {
            updates: u64,
            data_bytes: u64,
        }
        impl SlotVisitor for DecliningTally {
            fn on_block(&mut self, notification: &BlockNotification, _entries: &[EntryRecord]) {
                if let BlockNotification::Block(meta) = notification {
                    for (_, d) in meta.pre_updates.iter().chain(meta.post_updates.iter()) {
                        self.updates += 1;
                        self.data_bytes += d.len() as u64;
                    }
                }
            }
            fn consumption(&self) -> Consumption {
                Consumption::all().without_account_update_data()
            }
        }

        let visitors = firehose_horizon(
            2,
            JetSource::local(dir.path()),
            epoch,
            slot_start..slot_start + n,
            |_tid| DecliningTally::default(),
        )
        .await
        .unwrap();

        let updates: u64 = visitors.iter().map(|v| v.updates).sum();
        let data_bytes: u64 = visitors.iter().map(|v| v.data_bytes).sum();
        assert_eq!(updates, n, "every update's metadata still arrives");
        assert_eq!(data_bytes, 0, "declined update data decodes empty");
    }

    /// The in-memory source must drive the exact same decode as the local
    /// backend (and reject a mismatched epoch).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn memory_source_matches_local() {
        let epoch = 11u64;
        let slot_start = 0u64;
        let n = 300u64;

        let mut writer = ArchiveWriter::new(
            std::io::Cursor::new(Vec::new()),
            epoch,
            slot_start,
            n,
            ArchiveWriterConfig::default(),
        )
        .unwrap();
        for i in 0..n {
            writer.write_skipped_slot(slot_start + i).unwrap();
        }
        let (sink, _stats) = writer.finish().unwrap();
        let bytes = sink.into_inner();

        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join(format!("epoch-{epoch}.jet")), &bytes).unwrap();

        let mem_src = JetSource::in_memory(epoch, bytes);
        for src in [JetSource::local(dir.path()), mem_src.clone()] {
            let visitors = firehose_horizon(3, src, epoch, slot_start..slot_start + n, |_tid| {
                SlotCounter::default()
            })
            .await
            .unwrap();
            let skipped: u64 = visitors.iter().map(|v| v.skipped).sum();
            assert_eq!(skipped, n, "every slot visited exactly once");
        }

        // Wrong epoch → error, not silent garbage.
        let err = firehose_horizon(2, mem_src, epoch + 1, 0..1, |_tid| SlotCounter::default())
            .await
            .err();
        assert!(err.is_some(), "mismatched epoch must be rejected");
    }
}
