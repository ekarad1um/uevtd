//! Bounded append-only log: segment index, group-commit writer, tailing
//! readers, time-range queries and channel cursors (ARCHITECTURE.md §5).
//!
//! Concurrency model: one writer (a dedicated thread) mutates files and the
//! index; readers hold their own file handles and only read below the
//! committed length published in the index. Eviction removes whole sealed
//! segments under the index lock.

mod segment;

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde::Serialize;
use tokio::sync::{mpsc, watch};
use tracing::{debug, error, info, warn};

pub use segment::{Corrupt, Record};
use segment::{HEADER_LEN, Header, Next, Peek, SegmentCursor};

const CURSOR_DIR: &str = "cursors";
const MAX_BATCH: usize = 1024;
const MAX_BATCH_BYTES: usize = 1 << 20;

/// A message as handed to the writer: receipt time and verbatim payload.
#[derive(Clone, Debug)]
pub struct Message {
    pub ts: i64,
    pub payload: Vec<u8>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Limits {
    pub max_bytes: u64,
    pub segment_bytes: u64,
    /// Zero means sync after every batch.
    pub sync_interval: Duration,
}

#[derive(Default)]
pub struct Metrics {
    pub batches: AtomicU64,
    pub records: AtomicU64,
    pub bytes: AtomicU64,
    pub write_errors: AtomicU64,
    pub evictions: AtomicU64,
    pub corrupt_segments: AtomicU64,
    pub clock_steps: AtomicU64,
}

#[derive(Clone, Copy, Debug, Serialize)]
pub struct Status {
    pub segments: usize,
    pub bytes: u64,
    pub first_seq: u64,
    pub head_seq: u64,
    pub head_ts: i64,
}

struct Segment {
    base_ts: i64,
    /// Committed length including the header; readers never read past it.
    len: u64,
}

struct Index {
    /// Keyed by first sequence number; the last entry is the active segment.
    segments: BTreeMap<u64, Segment>,
    /// Last assigned sequence number; 0 when the log is empty.
    head_seq: u64,
    head_ts: i64,
    bytes: u64,
}

pub struct Storage {
    dir: PathBuf,
    index: Mutex<Index>,
    max_bytes: AtomicU64,
    segment_bytes: AtomicU64,
    sync_interval_us: AtomicU64,
    head: watch::Sender<u64>,
    metrics: Metrics,
}

impl Storage {
    /// Opens or creates the log, validating the active segment and truncating
    /// a torn tail.
    pub fn open(dir: &Path, limits: Limits) -> io::Result<Storage> {
        fs::create_dir_all(dir)?;
        fs::create_dir_all(dir.join(CURSOR_DIR))?;
        let mut names: Vec<u64> = fs::read_dir(dir)?
            .filter_map(|e| e.ok())
            .filter_map(|e| e.file_name().to_str().and_then(segment::parse_file_name))
            .collect();
        names.sort_unstable();

        let mut index = Index {
            segments: BTreeMap::new(),
            head_seq: 0,
            head_ts: 0,
            bytes: 0,
        };
        // Trailing files that hold no record are removed, so the previous
        // segment becomes the active one again.
        while let Some(&first_seq) = names.last() {
            let path = dir.join(segment::file_name(first_seq));
            let file = File::open(&path)?;
            let file_len = file.metadata()?.len();
            let junk = file_len < HEADER_LEN as u64
                || (read_header(&file, first_seq).is_err()
                    && segment::scan(file, file_len, first_seq)?.count == 0);
            if !junk {
                break;
            }
            warn!(
                segment = first_seq,
                file_len, "removing trailing segment without records"
            );
            fs::remove_file(&path)?;
            names.pop();
        }
        let last = names.last().copied();
        for first_seq in names {
            let path = dir.join(segment::file_name(first_seq));
            let file = File::open(&path)?;
            let file_len = file.metadata()?.len();
            if file_len < HEADER_LEN as u64 {
                warn!(
                    segment = first_seq,
                    file_len, "removing segment too short to hold a header"
                );
                fs::remove_file(&path)?;
                continue;
            }
            // Without a trustworthy base_ts the segment must never be chosen as
            // the start of a time-range scan; i64::MAX makes queries start earlier.
            let base_ts = match read_header(&file, first_seq) {
                Ok(h) => h.base_ts,
                Err(e) => {
                    warn!(segment = first_seq, error = %e, "unreadable segment header; records will be verified on read");
                    i64::MAX
                }
            };
            let len = if Some(first_seq) == last {
                let scan = segment::scan(file, file_len, first_seq)?;
                if let Some(err) = scan.error {
                    warn!(segment = first_seq, valid_len = scan.valid_len, file_len, error = %err,
                        "truncating active segment after last valid record");
                    OpenOptions::new()
                        .write(true)
                        .open(&path)?
                        .set_len(scan.valid_len)?;
                }
                match scan.last {
                    Some((seq, ts)) => {
                        index.head_seq = seq;
                        index.head_ts = ts;
                    }
                    None => {
                        index.head_seq = first_seq - 1;
                        index.head_ts = base_ts;
                    }
                }
                scan.valid_len
            } else {
                file_len
            };
            index.bytes += len;
            index.segments.insert(first_seq, Segment { base_ts, len });
        }
        info!(dir = %dir.display(), segments = index.segments.len(), bytes = index.bytes, head_seq = index.head_seq, "log opened");

        let (head, _) = watch::channel(index.head_seq);
        let storage = Storage {
            dir: dir.to_path_buf(),
            index: Mutex::new(index),
            max_bytes: AtomicU64::new(limits.max_bytes),
            segment_bytes: AtomicU64::new(limits.segment_bytes),
            sync_interval_us: AtomicU64::new(limits.sync_interval.as_micros() as u64),
            head,
            metrics: Metrics::default(),
        };
        storage.evict(&mut storage.index.lock().unwrap());
        Ok(storage)
    }

    /// Applies new limits; a lower size limit evicts immediately.
    pub fn set_limits(&self, limits: Limits) {
        self.max_bytes.store(limits.max_bytes, Relaxed);
        self.segment_bytes.store(limits.segment_bytes, Relaxed);
        self.sync_interval_us
            .store(limits.sync_interval.as_micros() as u64, Relaxed);
        self.evict(&mut self.index.lock().unwrap());
    }

    pub fn sync_interval(&self) -> Duration {
        Duration::from_micros(self.sync_interval_us.load(Relaxed))
    }

    pub fn head_seq(&self) -> u64 {
        *self.head.borrow()
    }

    /// Receives every new head sequence number; readers wait on it.
    pub fn subscribe(&self) -> watch::Receiver<u64> {
        self.head.subscribe()
    }

    pub fn metrics(&self) -> &Metrics {
        &self.metrics
    }

    pub fn status(&self) -> Status {
        let index = self.index.lock().unwrap();
        Status {
            segments: index.segments.len(),
            bytes: index.bytes,
            first_seq: index
                .segments
                .keys()
                .next()
                .copied()
                .unwrap_or(index.head_seq + 1),
            head_seq: index.head_seq,
            head_ts: index.head_ts,
        }
    }

    pub fn reader(self: &Arc<Self>, next_seq: u64) -> Reader {
        Reader::new(Arc::clone(self), next_seq)
    }

    /// Records with `from <= ts < to` and `seq > after`, in sequence order,
    /// at most `limit` of them.
    pub fn query(
        self: &Arc<Self>,
        from: Option<i64>,
        to: Option<i64>,
        after: u64,
        limit: usize,
    ) -> io::Result<Vec<Record>> {
        let start = {
            let index = self.index.lock().unwrap();
            let by_time = from.and_then(|from| {
                index
                    .segments
                    .iter()
                    .rev()
                    .find(|(_, s)| s.base_ts < from)
                    .map(|(k, _)| *k)
            });
            after.saturating_add(1).max(by_time.unwrap_or(1))
        };
        let mut reader = self.reader(start);
        let mut out = Vec::new();
        'read: while out.len() < limit {
            let batch = reader.read(256, 1 << 20)?;
            if batch.records.is_empty() {
                break;
            }
            for record in batch.records {
                if to.is_some_and(|to| record.ts >= to) {
                    break 'read;
                }
                if from.is_some_and(|from| record.ts < from) {
                    continue;
                }
                out.push(record);
                if out.len() >= limit {
                    break 'read;
                }
            }
        }
        Ok(out)
    }

    pub fn load_cursor(&self, name: &str) -> io::Result<Option<u64>> {
        match fs::read_to_string(self.cursor_path(name)) {
            Ok(text) => text.trim().parse().map(Some).map_err(|e| {
                io::Error::new(io::ErrorKind::InvalidData, format!("cursor {name}: {e}"))
            }),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e),
        }
    }

    pub fn save_cursor(&self, name: &str, next_seq: u64) -> io::Result<()> {
        let dir = self.dir.join(CURSOR_DIR);
        let tmp = dir.join(format!(".{name}.tmp"));
        let mut file = File::create(&tmp)?;
        writeln!(file, "{next_seq}")?;
        file.sync_all()?;
        fs::rename(&tmp, self.cursor_path(name))?;
        sync_dir(&dir);
        Ok(())
    }

    pub fn remove_cursor(&self, name: &str) -> io::Result<()> {
        match fs::remove_file(self.cursor_path(name)) {
            Err(e) if e.kind() != io::ErrorKind::NotFound => Err(e),
            _ => Ok(()),
        }
    }

    fn cursor_path(&self, name: &str) -> PathBuf {
        self.dir.join(CURSOR_DIR).join(name)
    }

    fn segment_path(&self, first_seq: u64) -> PathBuf {
        self.dir.join(segment::file_name(first_seq))
    }

    fn evict(&self, index: &mut Index) {
        let max = self.max_bytes.load(Relaxed);
        while index.bytes > max && index.segments.len() > 1 {
            let (&first_seq, segment) = index.segments.iter().next().expect("non-empty");
            let len = segment.len;
            match fs::remove_file(self.segment_path(first_seq)) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => {
                    error!(segment = first_seq, error = %e, "eviction failed");
                    break;
                }
            }
            index.segments.remove(&first_seq);
            index.bytes -= len;
            self.metrics.evictions.fetch_add(1, Relaxed);
            debug!(
                segment = first_seq,
                bytes = len,
                remaining = index.bytes,
                "evicted segment"
            );
        }
    }
}

fn read_header(file: &File, first_seq: u64) -> io::Result<Header> {
    let mut bytes = [0u8; HEADER_LEN];
    std::os::unix::fs::FileExt::read_exact_at(file, &mut bytes, 0)?;
    let header = Header::decode(&bytes).map_err(io::Error::other)?;
    if header.first_seq != first_seq {
        return Err(io::Error::other("header does not match file name"));
    }
    Ok(header)
}

fn sync_dir(dir: &Path) {
    if let Err(e) = File::open(dir).and_then(|d| d.sync_all()) {
        debug!(dir = %dir.display(), error = %e, "directory sync failed");
    }
}

struct Active {
    first_seq: u64,
    file: File,
    len: u64,
}

/// The single writer. Owns the active segment file; publishes commits to the
/// index and the head watch.
pub struct Writer {
    storage: Arc<Storage>,
    active: Option<Active>,
    buf: Vec<u8>,
    unsynced: bool,
    last_sync: Instant,
    clock_warned: Option<Instant>,
    /// Set after a failure that makes the active file untrustworthy.
    force_new: bool,
}

impl Writer {
    pub fn new(storage: Arc<Storage>) -> Writer {
        Writer {
            storage,
            active: None,
            buf: Vec::with_capacity(MAX_BATCH_BYTES),
            unsynced: false,
            last_sync: Instant::now(),
            clock_warned: None,
            force_new: false,
        }
    }

    pub fn unsynced(&self) -> bool {
        self.unsynced
    }

    pub fn since_sync(&self) -> Duration {
        self.last_sync.elapsed()
    }

    /// Appends a batch as one write. With `sync`, the data is durable before
    /// it becomes visible to readers; on any failure the file is rolled back
    /// to its committed length and nothing of the batch is visible.
    pub fn append(&mut self, batch: &[Message], sync: bool) -> io::Result<()> {
        if batch.is_empty() {
            return Ok(());
        }
        let segment_bytes = self.storage.segment_bytes.load(Relaxed);
        if self.active.as_ref().is_some_and(|a| a.len >= segment_bytes) {
            if let Err(e) = self.sync() {
                warn!(error = %e, "sync before rotation failed; continuing in a new segment");
            }
            self.active = None;
        }
        if self.active.is_none() {
            self.open_active()?;
        }

        let (mut seq, mut ts) = {
            let index = self.storage.index.lock().unwrap();
            (index.head_seq, index.head_ts)
        };
        self.buf.clear();
        for message in batch {
            seq += 1;
            if message.ts < ts {
                self.note_clock_step(ts - message.ts);
            } else {
                ts = message.ts;
            }
            segment::encode_frame(&mut self.buf, seq, ts, &message.payload);
        }

        let active = self.active.as_mut().expect("active segment");
        let written = active.file.write_all(&self.buf).and_then(|_| {
            self.unsynced = true;
            if sync {
                sync_file(&active.file, &mut self.unsynced, &mut self.last_sync)
            } else {
                Ok(())
            }
        });
        if let Err(e) = written {
            self.storage.metrics.write_errors.fetch_add(1, Relaxed);
            self.rollback();
            return Err(e);
        }
        active.len += self.buf.len() as u64;

        {
            let mut index = self.storage.index.lock().unwrap();
            if let Some(segment) = index.segments.get_mut(&active.first_seq) {
                segment.len = active.len;
            }
            index.bytes += self.buf.len() as u64;
            index.head_seq = seq;
            index.head_ts = ts;
            self.storage.evict(&mut index);
        }
        self.storage.head.send_replace(seq);
        let metrics = &self.storage.metrics;
        metrics.batches.fetch_add(1, Relaxed);
        metrics.records.fetch_add(batch.len() as u64, Relaxed);
        metrics.bytes.fetch_add(self.buf.len() as u64, Relaxed);
        Ok(())
    }

    /// Flushes unsynced data. A failure seals the segment because the kernel
    /// may have discarded the dirty pages.
    pub fn sync(&mut self) -> io::Result<()> {
        if !self.unsynced {
            return Ok(());
        }
        let Some(active) = self.active.as_ref() else {
            self.unsynced = false;
            return Ok(());
        };
        if let Err(e) = sync_file(&active.file, &mut self.unsynced, &mut self.last_sync) {
            self.storage.metrics.write_errors.fetch_add(1, Relaxed);
            error!(error = %e, "sync failed; sealing segment");
            self.active = None;
            self.unsynced = false;
            self.force_new = true;
            return Err(e);
        }
        Ok(())
    }

    fn open_active(&mut self) -> io::Result<()> {
        let (head_seq, head_ts, last) = {
            let index = self.storage.index.lock().unwrap();
            (
                index.head_seq,
                index.head_ts,
                index.segments.iter().next_back().map(|(k, s)| (*k, s.len)),
            )
        };
        if let Some((first_seq, len)) = last {
            if len < self.storage.segment_bytes.load(Relaxed) && !self.force_new {
                let file = OpenOptions::new()
                    .append(true)
                    .open(self.storage.segment_path(first_seq))?;
                let actual = file.metadata()?.len();
                if actual >= len {
                    file.set_len(len)?;
                    self.active = Some(Active {
                        first_seq,
                        file,
                        len,
                    });
                    return Ok(());
                }
                warn!(
                    segment = first_seq,
                    committed = len,
                    actual,
                    "segment shorter than its committed length; starting a new one"
                );
            }
        }
        self.force_new = false;
        self.create_segment(head_seq + 1, head_ts)
    }

    fn create_segment(&mut self, first_seq: u64, base_ts: i64) -> io::Result<()> {
        let path = self.storage.segment_path(first_seq);
        {
            // A segment that already starts at this sequence number holds no
            // records (otherwise the head would be past it); replace it.
            let mut index = self.storage.index.lock().unwrap();
            if let Some(stale) = index.segments.get(&first_seq) {
                let stale_len = stale.len;
                match fs::remove_file(&path) {
                    Ok(()) => {}
                    Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                    Err(e) => return Err(e),
                }
                index.segments.remove(&first_seq);
                index.bytes -= stale_len;
            }
        }
        let mut file = OpenOptions::new()
            .append(true)
            .create_new(true)
            .open(&path)?;
        if let Err(e) = file.write_all(&Header { first_seq, base_ts }.encode()) {
            let _ = fs::remove_file(&path);
            return Err(e);
        }
        sync_dir(&self.storage.dir);
        let len = HEADER_LEN as u64;
        {
            let mut index = self.storage.index.lock().unwrap();
            index.segments.insert(first_seq, Segment { base_ts, len });
            index.bytes += len;
        }
        self.active = Some(Active {
            first_seq,
            file,
            len,
        });
        self.unsynced = true;
        info!(segment = first_seq, "created segment");
        Ok(())
    }

    fn rollback(&mut self) {
        let Some(active) = self.active.as_mut() else {
            return;
        };
        if let Err(e) = active.file.set_len(active.len) {
            error!(error = %e, "rollback failed; sealing segment");
            self.active = None;
            self.force_new = true;
        }
    }

    fn note_clock_step(&mut self, step_us: i64) {
        self.storage.metrics.clock_steps.fetch_add(1, Relaxed);
        if step_us > 1_000_000
            && self
                .clock_warned
                .is_none_or(|t| t.elapsed() > Duration::from_secs(60))
        {
            warn!(
                step_us,
                "clock stepped backwards; clamping timestamps to keep them monotonic"
            );
            self.clock_warned = Some(Instant::now());
        }
    }
}

fn sync_file(file: &File, unsynced: &mut bool, last_sync: &mut Instant) -> io::Result<()> {
    file.sync_data()?;
    *unsynced = false;
    *last_sync = Instant::now();
    Ok(())
}

/// Writer thread body: group commit with the sync policy from the storage limits.
pub fn run_writer(
    storage: Arc<Storage>,
    mut rx: mpsc::Receiver<Message>,
    rt: tokio::runtime::Handle,
) {
    let mut writer = Writer::new(storage.clone());
    let mut batch: Vec<Message> = Vec::with_capacity(MAX_BATCH);
    loop {
        let interval = storage.sync_interval();
        let first = if writer.unsynced() && !interval.is_zero() {
            let wait = interval.saturating_sub(writer.since_sync());
            match rt.block_on(tokio::time::timeout(wait, rx.recv())) {
                Ok(Some(m)) => m,
                Ok(None) => break,
                Err(_) => {
                    if let Err(e) = writer.sync() {
                        error!(error = %e, "periodic sync failed");
                    }
                    continue;
                }
            }
        } else {
            match rx.blocking_recv() {
                Some(m) => m,
                None => break,
            }
        };
        let mut bytes = first.payload.len();
        batch.push(first);
        while batch.len() < MAX_BATCH && bytes < MAX_BATCH_BYTES {
            match rx.try_recv() {
                Ok(m) => {
                    bytes += m.payload.len();
                    batch.push(m);
                }
                Err(_) => break,
            }
        }
        let sync = interval.is_zero() || writer.since_sync() >= interval;
        if let Err(e) = writer.append(&batch, sync) {
            error!(error = %e, records = batch.len(), "write failed; batch dropped");
        }
        batch.clear();
    }
    if let Err(e) = writer.sync() {
        error!(error = %e, "final sync failed");
    }
    info!("log writer stopped");
}

#[derive(Default, Debug)]
pub struct Batch {
    pub records: Vec<Record>,
    /// Records that no longer exist (evicted or corrupt) between the previous
    /// position and the first record returned.
    pub skipped: u64,
}

struct View {
    first_seq: u64,
    bound: u64,
    next_first_seq: Option<u64>,
    head_seq: u64,
}

struct Open {
    first_seq: u64,
    cursor: SegmentCursor,
}

/// A tailing cursor over the log. Cheap to advance; re-positioning to an
/// arbitrary sequence number scans from the start of its segment once.
pub struct Reader {
    storage: Arc<Storage>,
    next_seq: u64,
    current: Option<Open>,
}

impl Reader {
    pub fn new(storage: Arc<Storage>, next_seq: u64) -> Reader {
        Reader {
            storage,
            next_seq: next_seq.max(1),
            current: None,
        }
    }

    pub fn next_seq(&self) -> u64 {
        self.next_seq
    }

    /// Reads up to `max_records` / `max_bytes` of records in sequence order;
    /// an empty batch means the reader has caught up with the head.
    pub fn read(&mut self, max_records: usize, max_bytes: usize) -> io::Result<Batch> {
        let mut batch = Batch::default();
        let mut bytes = 0usize;
        let mut reopen_attempts = 0;
        while batch.records.len() < max_records && bytes < max_bytes {
            let view = match self.locate(&mut batch) {
                Ok(Some(view)) => view,
                Ok(None) => break,
                Err(e) if e.kind() == io::ErrorKind::NotFound && reopen_attempts < 3 => {
                    reopen_attempts += 1;
                    self.current = None;
                    continue;
                }
                Err(e) => return Err(e),
            };
            let open = self.current.as_mut().expect("positioned");
            match open.cursor.next(view.bound)? {
                Next::Record(record) if record.seq == self.next_seq => {
                    bytes += record.payload.len();
                    self.next_seq += 1;
                    batch.records.push(record);
                }
                Next::Record(_) => {
                    self.skip_segment(&view, &mut batch, Corrupt("sequence mismatch"))
                }
                Next::End => match view.next_first_seq {
                    Some(next) => {
                        batch.skipped += next.saturating_sub(self.next_seq);
                        self.next_seq = self.next_seq.max(next);
                        self.current = None;
                    }
                    None => break,
                },
                Next::Corrupt(c) => self.skip_segment(&view, &mut batch, c),
            }
        }
        Ok(batch)
    }

    fn skip_segment(&mut self, view: &View, batch: &mut Batch, cause: Corrupt) {
        let offset = self.current.as_ref().map(|o| o.cursor.pos()).unwrap_or(0);
        error!(segment = view.first_seq, offset, error = %cause, "corrupt record; skipping the rest of the segment");
        self.storage.metrics.corrupt_segments.fetch_add(1, Relaxed);
        let resume = view.next_first_seq.unwrap_or(view.head_seq + 1);
        batch.skipped += resume.saturating_sub(self.next_seq);
        self.next_seq = self.next_seq.max(resume);
        self.current = None;
    }

    /// Positions the cursor at `next_seq`; `None` when caught up.
    fn locate(&mut self, batch: &mut Batch) -> io::Result<Option<View>> {
        let view = {
            let index = self.storage.index.lock().unwrap();
            if self.next_seq > index.head_seq {
                return Ok(None);
            }
            let (first_seq, segment) = match index.segments.range(..=self.next_seq).next_back() {
                Some((k, s)) => (*k, s),
                None => {
                    let (k, s) = index
                        .segments
                        .iter()
                        .next()
                        .expect("a non-empty log has a segment");
                    batch.skipped += *k - self.next_seq;
                    self.next_seq = *k;
                    (*k, s)
                }
            };
            View {
                first_seq,
                bound: segment.len,
                next_first_seq: index
                    .segments
                    .range(first_seq + 1..)
                    .next()
                    .map(|(k, _)| *k),
                head_seq: index.head_seq,
            }
        };
        if self.current.as_ref().map(|o| o.first_seq) != Some(view.first_seq) {
            let file = File::open(self.storage.segment_path(view.first_seq))?;
            let mut cursor = SegmentCursor::new(file, HEADER_LEN as u64);
            loop {
                match cursor.peek(view.bound)? {
                    Peek::Frame { seq, len } if seq < self.next_seq => cursor.skip(len),
                    _ => break,
                }
            }
            self.current = Some(Open {
                first_seq: view.first_seq,
                cursor,
            });
        }
        Ok(Some(view))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limits(max_bytes: u64, segment_bytes: u64) -> Limits {
        Limits {
            max_bytes,
            segment_bytes,
            sync_interval: Duration::ZERO,
        }
    }

    fn msg(ts: i64, payload: &str) -> Message {
        Message {
            ts,
            payload: payload.as_bytes().to_vec(),
        }
    }

    fn open(dir: &Path, l: Limits) -> (Arc<Storage>, Writer) {
        let storage = Arc::new(Storage::open(dir, l).unwrap());
        let writer = Writer::new(storage.clone());
        (storage, writer)
    }

    #[test]
    fn append_then_read_in_order() {
        let dir = tempfile::tempdir().unwrap();
        let (storage, mut writer) = open(dir.path(), limits(1 << 20, 64 << 10));
        writer.append(&[msg(10, "a"), msg(11, "b")], true).unwrap();
        writer.append(&[msg(12, "c")], true).unwrap();
        assert_eq!(storage.head_seq(), 3);

        let mut reader = storage.reader(1);
        let batch = reader.read(10, 1 << 20).unwrap();
        let got: Vec<_> = batch
            .records
            .iter()
            .map(|r| (r.seq, r.ts, r.payload.clone()))
            .collect();
        assert_eq!(
            got,
            vec![
                (1, 10, b"a".to_vec()),
                (2, 11, b"b".to_vec()),
                (3, 12, b"c".to_vec())
            ]
        );
        assert!(reader.read(10, 1 << 20).unwrap().records.is_empty());

        writer.append(&[msg(13, "d")], true).unwrap();
        let batch = reader.read(10, 1 << 20).unwrap();
        assert_eq!(batch.records.len(), 1);
        assert_eq!(batch.records[0].seq, 4);

        let mut late = storage.reader(3);
        assert_eq!(late.read(1, 1 << 20).unwrap().records[0].payload, b"c");
    }

    #[test]
    fn timestamps_are_clamped_monotonic() {
        let dir = tempfile::tempdir().unwrap();
        let (storage, mut writer) = open(dir.path(), limits(1 << 20, 64 << 10));
        writer
            .append(&[msg(100, "a"), msg(50, "b"), msg(120, "c")], true)
            .unwrap();
        let records = storage.reader(1).read(10, 1 << 20).unwrap().records;
        assert_eq!(
            records.iter().map(|r| r.ts).collect::<Vec<_>>(),
            vec![100, 100, 120]
        );
        assert_eq!(storage.metrics().clock_steps.load(Relaxed), 1);
    }

    #[test]
    fn rotation_and_eviction_keep_whole_segments() {
        let dir = tempfile::tempdir().unwrap();
        let l = limits(128 << 10, 64 << 10);
        let (storage, mut writer) = open(dir.path(), l);
        let payload = "x".repeat(1000);
        for i in 0..400 {
            writer
                .append(
                    &[Message {
                        ts: i,
                        payload: payload.clone().into_bytes(),
                    }],
                    i % 7 == 0,
                )
                .unwrap();
        }
        let status = storage.status();
        assert!(
            status.bytes <= l.max_bytes,
            "bytes {} over limit",
            status.bytes
        );
        assert!(status.segments >= 2);
        assert!(status.first_seq > 1);
        assert!(storage.metrics().evictions.load(Relaxed) > 0);
        assert_eq!(status.head_seq, 400);

        // A reader positioned before the eviction horizon jumps forward and reports the gap.
        let mut reader = storage.reader(1);
        let batch = reader.read(5, 1 << 20).unwrap();
        assert_eq!(batch.skipped, status.first_seq - 1);
        assert_eq!(batch.records[0].seq, status.first_seq);

        // Segment files on disk match the index.
        let on_disk = fs::read_dir(dir.path())
            .unwrap()
            .filter(|e| {
                e.as_ref()
                    .unwrap()
                    .path()
                    .extension()
                    .is_some_and(|x| x == "seg")
            })
            .count();
        assert_eq!(on_disk, status.segments);
    }

    #[test]
    fn recovery_truncates_torn_tail_and_continues_sequence() {
        let dir = tempfile::tempdir().unwrap();
        let l = limits(1 << 20, 64 << 10);
        {
            let (_storage, mut writer) = open(dir.path(), l);
            writer.append(&[msg(1, "a"), msg(2, "b")], true).unwrap();
        }
        let path = dir.path().join(segment::file_name(1));
        let mut bytes = fs::read(&path).unwrap();
        let intact = bytes.len();
        segment::encode_frame(&mut bytes, 3, 3, b"torn");
        bytes.truncate(bytes.len() - 2);
        fs::write(&path, &bytes).unwrap();

        let (storage, mut writer) = open(dir.path(), l);
        assert_eq!(fs::metadata(&path).unwrap().len(), intact as u64);
        assert_eq!(storage.head_seq(), 2);
        writer.append(&[msg(5, "c")], true).unwrap();
        let records = storage.reader(1).read(10, 1 << 20).unwrap().records;
        assert_eq!(
            records.iter().map(|r| r.seq).collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        assert_eq!(records[2].payload, b"c");

        // Reopen again: nothing to repair, head continues.
        let (storage, _) = open(dir.path(), l);
        assert_eq!(storage.status().head_seq, 3);
    }

    #[test]
    fn corrupt_sealed_segment_is_skipped() {
        let dir = tempfile::tempdir().unwrap();
        let l = limits(1 << 20, 64 << 10);
        let (storage, mut writer) = open(dir.path(), l);
        let payload = "y".repeat(10_000);
        for i in 0..20 {
            writer
                .append(
                    &[Message {
                        ts: i,
                        payload: payload.clone().into_bytes(),
                    }],
                    true,
                )
                .unwrap();
        }
        let status = storage.status();
        assert!(status.segments >= 3);
        // Flip a byte in the middle of the first (sealed) segment.
        let first = storage
            .index
            .lock()
            .unwrap()
            .segments
            .keys()
            .next()
            .copied()
            .unwrap();
        let path = storage.segment_path(first);
        let mut bytes = fs::read(&path).unwrap();
        let mid = bytes.len() / 2;
        bytes[mid] ^= 0x55;
        fs::write(&path, &bytes).unwrap();

        let mut reader = storage.reader(1);
        let mut seqs = Vec::new();
        let mut skipped = 0;
        loop {
            let batch = reader.read(100, 1 << 30).unwrap();
            skipped += batch.skipped;
            if batch.records.is_empty() {
                break;
            }
            seqs.extend(batch.records.iter().map(|r| r.seq));
        }
        assert!(skipped > 0);
        assert_eq!(*seqs.last().unwrap(), 20);
        assert!(seqs.windows(2).all(|w| w[1] > w[0]));
        assert_eq!(storage.metrics().corrupt_segments.load(Relaxed), 1);
    }

    #[test]
    fn query_by_time_and_sequence() {
        let dir = tempfile::tempdir().unwrap();
        let (storage, mut writer) = open(dir.path(), limits(1 << 20, 64 << 10));
        let payload = "z".repeat(5000);
        for i in 0..100 {
            writer
                .append(
                    &[Message {
                        ts: 1000 + i * 10,
                        payload: payload.clone().into_bytes(),
                    }],
                    false,
                )
                .unwrap();
        }
        assert!(storage.status().segments > 1);
        let got = storage.query(Some(1200), Some(1250), 0, 100).unwrap();
        assert_eq!(
            got.iter().map(|r| r.ts).collect::<Vec<_>>(),
            vec![1200, 1210, 1220, 1230, 1240]
        );
        let page = storage.query(Some(1200), None, got[1].seq, 2).unwrap();
        assert_eq!(
            page.iter().map(|r| r.ts).collect::<Vec<_>>(),
            vec![1220, 1230]
        );
        assert!(storage.query(Some(5000), None, 0, 10).unwrap().is_empty());
        assert_eq!(storage.query(None, Some(1010), 0, 10).unwrap().len(), 1);
        assert_eq!(storage.query(None, None, 98, 10).unwrap().len(), 2);
    }

    #[test]
    fn open_removes_trailing_segments_without_records() {
        let dir = tempfile::tempdir().unwrap();
        let l = limits(1 << 20, 64 << 10);
        {
            let (_storage, mut writer) = open(dir.path(), l);
            writer.append(&[msg(1, "a"), msg(2, "b")], true).unwrap();
        }
        // A crash between creating a segment and its first write leaves an
        // empty or header-less file; neither holds a record.
        fs::write(dir.path().join(segment::file_name(3)), b"").unwrap();
        fs::write(dir.path().join(segment::file_name(4)), [0u8; HEADER_LEN]).unwrap();
        let (storage, mut writer) = open(dir.path(), l);
        assert_eq!(storage.status().head_seq, 2);
        assert_eq!(storage.status().segments, 1);
        assert!(!dir.path().join(segment::file_name(3)).exists());
        assert!(!dir.path().join(segment::file_name(4)).exists());
        writer.append(&[msg(3, "c")], true).unwrap();
        let records = storage.reader(1).read(10, 1 << 20).unwrap().records;
        assert_eq!(
            records.iter().map(|r| r.seq).collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
    }

    #[test]
    fn unreadable_header_never_hides_earlier_records_from_queries() {
        let dir = tempfile::tempdir().unwrap();
        let l = limits(1 << 20, 64 << 10);
        let payload = "q".repeat(8000);
        let firsts: Vec<u64> = {
            let (storage, mut writer) = open(dir.path(), l);
            for i in 0..30 {
                writer
                    .append(
                        &[Message {
                            ts: 1000 + i * 10,
                            payload: payload.clone().into_bytes(),
                        }],
                        false,
                    )
                    .unwrap();
            }
            storage
                .index
                .lock()
                .unwrap()
                .segments
                .keys()
                .copied()
                .collect()
        };
        assert!(firsts.len() >= 3);
        let second = firsts[1];
        let path = dir.path().join(segment::file_name(second));
        let mut bytes = fs::read(&path).unwrap();
        bytes[0] ^= 0xff;
        fs::write(&path, &bytes).unwrap();

        let (storage, _) = open(dir.path(), l);
        // Sequence k has timestamp 1000 + (k - 1) * 10; start two records before the damaged segment.
        let from = 1000 + (second as i64 - 3) * 10;
        let got = storage.query(Some(from), None, 0, 1000).unwrap();
        assert_eq!(got.first().map(|r| r.ts), Some(from));
        assert_eq!(got.len() as u64, 33 - second);
        assert!(got.windows(2).all(|w| w[1].seq == w[0].seq + 1));
    }

    #[test]
    fn cursors_persist() {
        let dir = tempfile::tempdir().unwrap();
        let (storage, _) = open(dir.path(), limits(1 << 20, 64 << 10));
        assert_eq!(storage.load_cursor("a").unwrap(), None);
        storage.save_cursor("a", 17).unwrap();
        storage.save_cursor("a", 18).unwrap();
        assert_eq!(storage.load_cursor("a").unwrap(), Some(18));
        storage.remove_cursor("a").unwrap();
        storage.remove_cursor("a").unwrap();
        assert_eq!(storage.load_cursor("a").unwrap(), None);
    }

    #[test]
    fn lowering_limit_evicts_immediately() {
        let dir = tempfile::tempdir().unwrap();
        let (storage, mut writer) = open(dir.path(), limits(1 << 20, 64 << 10));
        let payload = "w".repeat(10_000);
        for i in 0..30 {
            writer
                .append(
                    &[Message {
                        ts: i,
                        payload: payload.clone().into_bytes(),
                    }],
                    false,
                )
                .unwrap();
        }
        let before = storage.status();
        assert!(before.segments > 2);
        storage.set_limits(limits(128 << 10, 64 << 10));
        let after = storage.status();
        assert!(after.segments < before.segments);
        assert!(after.bytes <= 128 << 10);
    }
}
