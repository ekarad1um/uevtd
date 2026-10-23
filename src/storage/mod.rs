//! Bounded append-only log: segment index, group-commit writer, tailing
//! readers, time-range queries and channel cursors (ARCHITECTURE.md §5).
//!
//! Concurrency model: one writer (a dedicated thread) mutates files and the
//! index; readers hold their own file handles and only read below the
//! committed length published in the index, which covers synced data only.
//! Eviction removes whole sealed segments, their files outside the index
//! lock and then their entries under it, and counts what channels lose by
//! it under the same lock. No file operation happens under the index lock
//! once the log is open.

mod queue;
mod segment;

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, DirBuilder, File, OpenOptions};
use std::io::{self, Read, Write};
use std::os::fd::AsRawFd;
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde::Serialize;
use tokio::sync::watch;
use tracing::{debug, error, info, warn};

use crate::metrics::ErrorSlot;

pub use queue::{Depth, QueueReceiver, QueueSender, queue};
pub use segment::{Corrupt, Record};
use segment::{FRAME_HEADER_LEN, HEADER_LEN, Header, Next, Peek, Rest, SegmentCursor, Stopped};

/// Bytes a segment needs besides the payload of its first record: the
/// segment header and one frame header.
pub const SEGMENT_OVERHEAD: u64 = (HEADER_LEN + FRAME_HEADER_LEN) as u64;

const CURSOR_DIR: &str = "cursors";
const LOCK_FILE: &str = "lock";
/// A cursor above this was not written by the daemon, which would need
/// centuries to assign as many sequence numbers; receivers in JavaScript tell
/// numbers apart up to here.
const MAX_CURSOR: u64 = 1 << 53;
const MAX_BATCH: usize = 1024;
/// Encoded bytes per write batch, frame headers included; a single larger
/// message is written alone.
const MAX_BATCH_BYTES: usize = 1 << 20;
/// Read size for queries, which filter what they read.
const QUERY_READ_RECORDS: usize = 256;
const QUERY_READ_BYTES: usize = 256 << 10;
/// Stored messages are readable by the owner and the group only.
const FILE_MODE: u32 = 0o640;
const DIR_MODE: u32 = 0o750;
/// A channel that stays behind loses records with every eviction; its
/// warning repeats at most this often, with the running total.
const LOSS_WARN_INTERVAL: Duration = Duration::from_secs(60);
/// A write or sync that takes longer makes the log unhealthy.
pub const STALLED: Duration = Duration::from_secs(30);
/// How long a reader retries a position whose data cannot be read back, as
/// from a worn sector, before it skips the rest of the segment (§5.7).
const UNREADABLE_RETRY: Duration = Duration::from_secs(600);

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
    /// Batches written, committed or not.
    pub batches: AtomicU64,
    pub records: AtomicU64,
    pub bytes: AtomicU64,
    /// Failed writes, syncs and segment creations.
    pub write_errors: AtomicU64,
    /// Records lost to a failed write or sync.
    pub dropped: AtomicU64,
    pub syncs: AtomicU64,
    pub sync_us: AtomicU64,
    pub evictions: AtomicU64,
    pub corrupt_segments: AtomicU64,
    pub clock_steps: AtomicU64,
    /// The last write failure, until a batch is committed again.
    pub last_error: ErrorSlot,
    /// Why eviction cannot bring the log within `max_bytes`, while it cannot.
    pub evict_error: ErrorSlot,
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
    /// Counted in `corrupt_segments` already.
    damaged: bool,
}

struct Index {
    /// Keyed by first sequence number; the last entry is the active segment.
    segments: BTreeMap<u64, Segment>,
    /// Last assigned sequence number; 0 when the log is empty.
    head_seq: u64,
    head_ts: i64,
    bytes: u64,
    /// Channel positions kept for loss accounting, by [`Tail`] id.
    tails: BTreeMap<u64, TailState>,
    next_tail: u64,
    /// Segments below this are being evicted: their files are removed
    /// outside the lock, and their entries go after. A reader that finds one
    /// of their files gone has found it evicted.
    evicting_below: u64,
}

impl Index {
    /// The oldest retained sequence number; the head's successor when no
    /// record is retained.
    fn first_seq(&self) -> u64 {
        self.segments
            .keys()
            .next()
            .copied()
            .unwrap_or(self.head_seq + 1)
    }
}

pub struct Storage {
    dir: PathBuf,
    /// Holds the log's lock while the log is open.
    _lock: File,
    index: Mutex<Index>,
    /// Held by the eviction in progress, so that there is one at a time:
    /// when its failure was last logged, while eviction fails.
    eviction: Mutex<Option<Instant>>,
    max_bytes: AtomicU64,
    segment_bytes: AtomicU64,
    sync_interval_us: AtomicU64,
    head: watch::Sender<u64>,
    metrics: Metrics,
    opened: Instant,
    /// Milliseconds after `opened`, plus one, at which the writer began its
    /// current batch or sync; zero while it waits.
    busy_since: AtomicU64,
}

impl Storage {
    /// Opens or creates the log (§5.6), after taking its lock. The active
    /// segment is validated and synced before its records become visible: a
    /// torn tail is cut off, and after damage of any other kind, a read error
    /// included, the log continues in a new segment past every sequence
    /// number the damaged part could hold. Numbering also continues past the
    /// highest channel cursor.
    pub fn open(dir: &Path, limits: Limits) -> io::Result<Storage> {
        let mut builder = DirBuilder::new();
        builder.recursive(true).mode(DIR_MODE);
        builder.create(dir)?;
        let lock = lock(dir)?;
        builder.create(dir.join(CURSOR_DIR))?;
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
            tails: BTreeMap::new(),
            next_tail: 0,
            evicting_below: 0,
        };
        // Trailing files that hold no record are removed, so the previous
        // segment becomes the active one again. One that cannot be read
        // stays, since what it holds is unknown, and so does one with an
        // intact record anywhere in it, whatever its header.
        while let Some(&first_seq) = names.last() {
            let path = dir.join(segment::file_name(first_seq));
            let file = File::open(&path)?;
            let file_len = file.metadata()?.len();
            let junk = file_len < HEADER_LEN as u64
                || (read_header(&file, first_seq).is_err()
                    && holds_nothing(file, file_len, first_seq));
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
        let active = names.pop();
        let mut damaged = 0;
        // Sealed segments are trusted structurally, and only their headers
        // are read now; every record is verified when it is read. A segment
        // that cannot be opened is left to the readers, which retry it and
        // report the error (§5.7).
        for first_seq in names {
            let path = dir.join(segment::file_name(first_seq));
            let (file_len, unknown) = match fs::metadata(&path) {
                Ok(meta) => (meta.len(), false),
                Err(e) if e.kind() == io::ErrorKind::NotFound => {
                    warn!(
                        segment = first_seq,
                        "segment removed while the log was opened"
                    );
                    continue;
                }
                // Kept as a full segment of the configured size, so that
                // readers retry it, as one that cannot be opened, and
                // eviction removes it in its turn.
                Err(e) => {
                    error!(segment = first_seq, error = %e, "cannot stat segment; keeping it as a full segment");
                    damaged += 1;
                    (limits.segment_bytes, true)
                }
            };
            if file_len < HEADER_LEN as u64 {
                warn!(
                    segment = first_seq,
                    file_len, "removing segment too short to hold a header"
                );
                if let Err(e) = fs::remove_file(&path) {
                    warn!(segment = first_seq, error = %e, "could not remove segment");
                }
                continue;
            }
            // Without a trustworthy base_ts the segment must never be chosen as
            // the start of a time-range scan; i64::MAX makes queries start earlier.
            let base_ts = match File::open(&path).and_then(|file| read_header(&file, first_seq)) {
                Ok(h) => h.base_ts,
                Err(e) => {
                    warn!(segment = first_seq, error = %e, "unreadable segment header; records will be verified on read");
                    i64::MAX
                }
            };
            index.bytes += file_len;
            index.segments.insert(
                first_seq,
                Segment {
                    base_ts,
                    len: file_len,
                    damaged: unknown,
                },
            );
        }
        if let Some(first_seq) = active {
            damaged += recover_active(dir, first_seq, &mut index)?;
        }
        // A cursor past the head means that records a channel delivered are
        // gone, to damage or to a disk that lost what it had synced. Their
        // receivers have seen those sequence numbers, and one that drops
        // duplicates would drop the next records given them.
        let cursor = highest_cursor(&dir.join(CURSOR_DIR));
        if cursor > index.head_seq + 1 {
            error!(
                cursor,
                head_seq = index.head_seq,
                "a channel's cursor is past the head of the log, so records it delivered are gone; numbering continues at the cursor"
            );
            let known_ts = index.head_ts;
            start_at(dir, &mut index, cursor, known_ts)?;
        }
        info!(dir = %dir.display(), segments = index.segments.len(), bytes = index.bytes, head_seq = index.head_seq, "log opened");

        let (head, _) = watch::channel(index.head_seq);
        let storage = Storage {
            dir: dir.to_path_buf(),
            _lock: lock,
            index: Mutex::new(index),
            eviction: Mutex::new(None),
            max_bytes: AtomicU64::new(limits.max_bytes),
            segment_bytes: AtomicU64::new(limits.segment_bytes),
            sync_interval_us: AtomicU64::new(micros(limits.sync_interval)),
            head,
            metrics: Metrics::default(),
            opened: Instant::now(),
            busy_since: AtomicU64::new(0),
        };
        storage.metrics.corrupt_segments.fetch_add(damaged, Relaxed);
        storage.evict();
        Ok(storage)
    }

    /// Applies new limits; a lower size limit evicts immediately.
    pub fn set_limits(&self, limits: Limits) {
        self.max_bytes.store(limits.max_bytes, Relaxed);
        self.segment_bytes.store(limits.segment_bytes, Relaxed);
        self.sync_interval_us
            .store(micros(limits.sync_interval), Relaxed);
        self.evict();
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
            first_seq: index.first_seq(),
            head_seq: index.head_seq,
            head_ts: index.head_ts,
        }
    }

    /// Whether the log takes writes: not after a failed write or sync until a
    /// batch is committed again, nor while one batch or sync has taken longer
    /// than [`STALLED`]. It never waits for the writer, so it answers even
    /// then.
    pub fn health(&self) -> Result<(), String> {
        let since = self.busy_since.load(Relaxed);
        if since > 0 {
            let busy = (self.opened.elapsed().as_millis() as u64 + 1).saturating_sub(since);
            if busy > STALLED.as_millis() as u64 {
                return Err(format!("log writer busy for {} s", busy / 1000));
            }
        }
        if let Some(e) = self.metrics.last_error.get() {
            return Err(format!("log write failed: {}", e.message));
        }
        match self.metrics.evict_error.get() {
            Some(e) => Err(format!("log over its size limit: {}", e.message)),
            None => Ok(()),
        }
    }

    /// Marks the writer busy until the guard is dropped.
    fn busy(&self) -> Busy<'_> {
        let now = self.opened.elapsed().as_millis() as u64 + 1;
        self.busy_since.store(now, Relaxed);
        Busy(self)
    }

    /// The receipt time of record `seq`, if it is committed and retained.
    /// Unlike a [`Reader`], it counts and logs nothing. It reads the record's
    /// segment from the start, or from `mark` if a lookup of an earlier
    /// record in the same segment left it there, and leaves `mark` at `seq`.
    pub fn timestamp(&self, seq: u64, mark: &mut Mark) -> io::Result<Option<i64>> {
        let (segment, bound) = {
            let index = self.index.lock().unwrap();
            if seq == 0 || seq > index.head_seq {
                return Ok(None);
            }
            match index.segments.range(..=seq).next_back() {
                Some((&first_seq, s)) => (first_seq, s.len),
                None => return Ok(None),
            }
        };
        let file = match File::open(self.segment_path(segment)) {
            Ok(file) => file,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e),
        };
        let start = if mark.segment == segment && mark.seq <= seq {
            mark.pos
        } else {
            HEADER_LEN as u64
        };
        let mut cursor = SegmentCursor::new(file, start);
        loop {
            match cursor.peek(bound)? {
                Peek::Frame { seq: s, len } if s < seq => cursor.skip(len),
                Peek::Frame { seq: s, .. } if s == seq => break,
                _ => return Ok(None),
            }
        }
        *mark = Mark {
            segment,
            pos: cursor.pos(),
            seq,
        };
        Ok(match cursor.next(bound)? {
            Next::Record(record) => Some(record.ts),
            Next::End | Next::Corrupt(_) => None,
        })
    }

    pub fn reader(self: &Arc<Self>, next_seq: u64) -> Reader {
        Reader::new(Arc::clone(self), next_seq)
    }

    /// Keeps a channel's position at `next_seq` for loss accounting, with
    /// losses counted in `loss` and logged under `label`; see [`Tail`].
    /// Records already gone below the position are counted at once.
    pub fn tail(self: &Arc<Self>, label: &str, next_seq: u64, loss: Arc<Loss>) -> Tail {
        let mut index = self.index.lock().unwrap();
        let mut state = TailState {
            next: next_seq.max(1),
            reading: false,
            label: label.to_string(),
            loss,
            warned: None,
        };
        state.claim(index.first_seq());
        let id = index.next_tail;
        index.next_tail += 1;
        index.tails.insert(id, state);
        Tail {
            storage: Arc::clone(self),
            id,
        }
    }

    /// Records with `from <= ts < to` and `seq > after`, in sequence order:
    /// at most `limit` of them, and at most `max_bytes` of payload unless the
    /// first record alone is larger.
    pub fn query(
        self: &Arc<Self>,
        from: Option<i64>,
        to: Option<i64>,
        after: u64,
        limit: usize,
        max_bytes: usize,
    ) -> io::Result<Page> {
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
        let mut page = Page::default();
        let mut bytes = 0usize;
        'read: loop {
            let batch = match reader.read(QUERY_READ_RECORDS, QUERY_READ_BYTES) {
                Ok(batch) => batch,
                // The page ends in front of a read error; the next one starts
                // at the failing position, which reports it.
                Err(_) if !page.records.is_empty() => {
                    page.more = true;
                    break;
                }
                Err(e) => return Err(e),
            };
            if batch.records.is_empty() && batch.skipped == 0 {
                break;
            }
            for record in batch.records {
                if to.is_some_and(|to| record.ts >= to) {
                    break 'read;
                }
                if from.is_some_and(|from| record.ts < from) {
                    continue;
                }
                // Timestamps never decrease, so the first record left out
                // tells whether the page is the last one.
                let len = record.payload.len();
                if page.records.len() >= limit
                    || (!page.records.is_empty() && bytes.saturating_add(len) > max_bytes)
                {
                    page.more = true;
                    break 'read;
                }
                bytes += len;
                page.records.push(record);
            }
        }
        Ok(page)
    }

    /// The cursor saved for channel `name`; `None` without a file. A file
    /// that holds no cursor fails with `InvalidData`, a kind that no error of
    /// the system has.
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
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(FILE_MODE)
            .open(&tmp)?;
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

    /// Removes every cursor file whose name is not in `keep`: cursors of
    /// channels removed from the configuration while the daemon was stopped,
    /// and temporary files left by an interrupted save.
    pub fn prune_cursors(&self, keep: &BTreeSet<String>) -> io::Result<()> {
        for entry in fs::read_dir(self.dir.join(CURSOR_DIR))? {
            let entry = entry?;
            let name = entry.file_name();
            if name.to_str().is_some_and(|n| keep.contains(n)) {
                continue;
            }
            match fs::remove_file(entry.path()) {
                Ok(()) => info!(file = %name.to_string_lossy(), "removed stale cursor file"),
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => {
                    warn!(file = %name.to_string_lossy(), error = %e, "could not remove cursor file")
                }
            }
        }
        Ok(())
    }

    fn cursor_path(&self, name: &str) -> PathBuf {
        self.dir.join(CURSOR_DIR).join(name)
    }

    fn segment_path(&self, first_seq: u64) -> PathBuf {
        self.dir.join(segment::file_name(first_seq))
    }

    /// Removes the oldest segments while the log is over its size limit, and
    /// counts the records that channels had not read as lost. The oldest
    /// records go first, so a segment that cannot be removed stops eviction:
    /// the log then grows past its limit, and its health fails, until the
    /// segment can be removed.
    fn evict(&self) {
        self.evict_with(|path| fs::remove_file(path));
    }

    /// [`Storage::evict`], with `remove` removing each file. The files go
    /// without the index lock, which status and readers take on the
    /// runtime's threads, since removing them can take long on a slow disk
    /// or never end on a stalled one.
    fn evict_with(&self, mut remove: impl FnMut(&Path) -> io::Result<()>) {
        let mut warned = self.eviction.lock().unwrap();
        let max = self.max_bytes.load(Relaxed);
        let victims: Vec<(u64, u64)> = {
            let mut index = self.index.lock().unwrap();
            let mut bytes = index.bytes;
            let mut left = index.segments.len();
            let mut victims = Vec::new();
            for (&first_seq, segment) in &index.segments {
                if bytes <= max || left <= 1 {
                    break;
                }
                bytes -= segment.len;
                left -= 1;
                victims.push((first_seq, segment.len));
            }
            if let Some(&(last, _)) = victims.last() {
                index.evicting_below = last + 1;
            }
            victims
        };
        let mut removed = 0;
        let mut failed = None;
        for &(first_seq, _) in &victims {
            match remove(&self.segment_path(first_seq)) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => {
                    failed = Some((first_seq, e));
                    break;
                }
            }
            removed += 1;
        }

        let mut index = self.index.lock().unwrap();
        index.evicting_below = 0;
        for &(first_seq, len) in &victims[..removed] {
            if index.segments.remove(&first_seq).is_none() {
                continue;
            }
            index.bytes -= len;
            self.metrics.evictions.fetch_add(1, Relaxed);
            debug!(
                segment = first_seq,
                bytes = len,
                remaining = index.bytes,
                "evicted segment"
            );
        }
        match failed {
            Some((segment, e)) => {
                // Eviction runs at every commit; the error is logged at most
                // once a minute.
                if warned.is_none_or(|t| t.elapsed() >= LOSS_WARN_INTERVAL) {
                    error!(segment, error = %e, bytes = index.bytes, max_bytes = max,
                        "eviction failed; the log grows past max_bytes until the segment can be removed");
                    *warned = Some(Instant::now());
                }
                self.metrics
                    .evict_error
                    .set(format!("removing segment {segment}: {e}"));
            }
            None => {
                if warned.take().is_some() {
                    info!(bytes = index.bytes, max_bytes = max, "eviction works again");
                }
                self.metrics.evict_error.clear();
            }
        }
        if removed > 0 {
            let first = index.first_seq();
            for tail in index.tails.values_mut().filter(|t| !t.reading) {
                tail.claim(first);
            }
        }
    }
}

struct Busy<'a>(&'a Storage);

impl Drop for Busy<'_> {
    fn drop(&mut self) {
        self.0.busy_since.store(0, Relaxed);
    }
}

/// Where [`Storage::timestamp`] found a record, so that a later lookup in
/// the same segment can read on from there.
#[derive(Clone, Copy, Debug, Default)]
pub struct Mark {
    segment: u64,
    pos: u64,
    seq: u64,
}

/// What a channel lost before delivery: the records that left the log
/// before the channel read them.
#[derive(Default)]
pub struct Loss {
    records: AtomicU64,
    /// Records below this have been counted. A channel is restarted on every
    /// change to its configuration, and the new tail starts at the cursor,
    /// behind what the one before it counted.
    until: AtomicU64,
}

impl Loss {
    pub fn records(&self) -> u64 {
        self.records.load(Relaxed)
    }
}

/// A channel's position in the log, kept for loss accounting: records that
/// leave the log before the channel has read them are counted in its
/// [`Loss`] and logged as soon as that happens, whether or not the channel is
/// reading, and only once (ARCHITECTURE.md §5.9). Readers made from it move
/// the position as they read; dropping it ends the accounting.
pub struct Tail {
    storage: Arc<Storage>,
    id: u64,
}

impl Tail {
    /// A reader at `next_seq` that moves this tail. A tail has one reader at
    /// a time; a new one replaces a reader that was lost.
    pub fn reader(&self, next_seq: u64) -> Reader {
        let mut index = self.storage.index.lock().unwrap();
        let first = index.first_seq();
        if let Some(tail) = index.tails.get_mut(&self.id) {
            // A reader lost in the middle of a read never ended it.
            tail.reading = false;
            tail.next = tail.next.max(next_seq);
            tail.claim(first);
        }
        drop(index);
        Reader {
            tail: Some(self.id),
            ..Reader::new(Arc::clone(&self.storage), next_seq)
        }
    }
}

impl Drop for Tail {
    fn drop(&mut self) {
        self.storage.index.lock().unwrap().tails.remove(&self.id);
    }
}

/// The accounting state of a [`Tail`], kept in the index.
struct TailState {
    /// Every record below this is in the channel's hands or counted as lost.
    next: u64,
    /// A read is running and counts what it skips when it ends; eviction
    /// leaves the tail alone until then.
    reading: bool,
    label: String,
    loss: Arc<Loss>,
    warned: Option<Instant>,
}

impl TailState {
    /// Counts the records below `first` that the channel has not read.
    fn claim(&mut self, first: u64) {
        if self.next < first {
            self.lose(self.next, first);
            self.next = first;
        }
    }

    /// Counts records `from..to` as lost, apart from those counted before.
    /// Every count happens under the index lock.
    fn lose(&mut self, from: u64, to: u64) {
        let from = from.max(self.loss.until.load(Relaxed));
        if from >= to {
            return;
        }
        self.loss.until.store(to, Relaxed);
        let records = to - from;
        let total = self.loss.records.fetch_add(records, Relaxed) + records;
        let (from_seq, to_seq) = (from, to - 1);
        if self
            .warned
            .is_none_or(|t| t.elapsed() >= LOSS_WARN_INTERVAL)
        {
            warn!(channel = %self.label, from_seq, to_seq, records, total, "records lost before delivery");
            self.warned = Some(Instant::now());
        } else {
            debug!(channel = %self.label, from_seq, to_seq, records, total, "records lost before delivery");
        }
    }
}

/// Recovers the active segment into the index; returns 1 if it was damaged.
///
/// Its valid prefix is synced before it becomes visible: after a crash of the
/// daemon, not of the system, records written but not synced are still in the
/// page cache, and a power loss could take them back after their delivery.
///
/// Only a torn tail is cut off: an incomplete last record, as a crash in the
/// middle of a write leaves it, or zeros, which storage reads where a write
/// never arrived. Any other end may be damage to committed records, which may
/// have been delivered: a read error, an invalid record that is there in full,
/// or intact records after an invalid one. The file is then kept as it is, as
/// a sealed segment that readers read up to the damage, retrying a read error
/// as anywhere else, and the log continues in a new segment past every
/// sequence number the rest of the file could hold, so that none is used
/// again. Creating that segment is the only change, and it is durable, so a
/// crash at any point is recovered the same way again.
fn recover_active(dir: &Path, first_seq: u64, index: &mut Index) -> io::Result<u64> {
    let path = dir.join(segment::file_name(first_seq));
    let file = File::open(&path)?;
    let file_len = file.metadata()?.len();
    let header_ts = match read_header(&file, first_seq) {
        Ok(h) => Some(h.base_ts),
        Err(e) => {
            warn!(segment = first_seq, error = %e, "unreadable segment header; records will be verified on read");
            None
        }
    };
    // Without a trustworthy base_ts the segment must never be chosen as the
    // start of a time-range scan; i64::MAX makes queries start earlier.
    let base_ts = header_ts.unwrap_or(i64::MAX);
    let scan = segment::scan(file.try_clone()?, file_len, first_seq);
    // With no record, the newest receipt time is the header's, or now: new
    // records must not be stamped below one before them.
    let (head_seq, head_ts) = scan.last.unwrap_or_else(|| {
        let ts = header_ts.unwrap_or_else(crate::now_us);
        (first_seq.saturating_sub(1), ts)
    });
    (index.head_seq, index.head_ts) = (head_seq, head_ts);
    let mut keep = |len, damaged| {
        index.bytes += len;
        index.segments.insert(
            first_seq,
            Segment {
                base_ts,
                len,
                damaged,
            },
        );
    };
    match ending(&file, &scan, file_len, head_seq, head_ts) {
        Ending::Clean => {
            keep(scan.valid_len, false);
            file.sync_data()?;
            Ok(0)
        }
        Ending::Torn(error) => {
            warn!(segment = first_seq, valid_len = scan.valid_len, file_len, %error,
                "cutting off the torn tail of the active segment");
            keep(scan.valid_len, false);
            // Readers stop at the valid length anyway, and the writer cuts
            // the file back again before it appends.
            if let Err(e) = cut_back(&path, scan.valid_len) {
                warn!(segment = first_seq, error = %e, "could not cut off the torn tail");
            }
            file.sync_data()?;
            Ok(0)
        }
        Ending::Damaged {
            error,
            next_seq,
            known_ts,
        } => {
            error!(segment = first_seq, offset = scan.valid_len, file_len, %error, next_seq,
                records_at_most = next_seq - head_seq - 1,
                "active segment damaged; keeping it as it is and continuing past every record it can hold");
            keep(file_len, true);
            if let Err(e) = file.sync_data() {
                warn!(segment = first_seq, error = %e, "could not sync the damaged segment");
            }
            start_at(dir, index, next_seq, known_ts)?;
            Ok(1)
        }
    }
}

/// How the active segment ends, after its valid prefix.
enum Ending {
    /// With its last valid record.
    Clean,
    /// With a torn tail.
    Torn(String),
    /// With damage that may have hit committed records; the log continues at
    /// `next_seq`, past every record the rest of the file can hold.
    Damaged {
        error: String,
        next_seq: u64,
        /// The newest receipt time known.
        known_ts: i64,
    },
}

/// Tells a torn tail from damage, from what follows the valid prefix that
/// ends with record `head_seq`; see [`recover_active`].
fn ending(file: &File, scan: &segment::Scan, file_len: u64, head_seq: u64, head_ts: i64) -> Ending {
    // Every record takes at least a frame header.
    let past = |from: u64, seq: u64| {
        let room = (file_len - from) / FRAME_HEADER_LEN as u64;
        seq.saturating_add(1).saturating_add(room)
    };
    let damaged = |error| Ending::Damaged {
        error,
        next_seq: past(scan.valid_len, head_seq),
        known_ts: head_ts,
    };
    // Where the invalid record ends by its length; none if the file ends
    // inside it.
    let (error, end) = match &scan.stopped {
        None => return Ending::Clean,
        Some(Stopped::Unreadable(e)) => return damaged(format!("read failed: {e}")),
        Some(Stopped::Torn(c)) => (c.to_string(), None),
        Some(Stopped::Invalid { error, end }) => (error.clone(), Some(*end)),
    };
    match segment::search(file, scan.valid_len, file_len, head_seq) {
        Ok(Rest {
            last: Some(found), ..
        }) => Ending::Damaged {
            error: format!("{error}, and intact records up to {} after it", found.seq),
            next_seq: past(found.end, found.seq),
            known_ts: head_ts.max(found.ts),
        },
        Ok(rest) if end.is_none_or(|end| torn_into_zeros(scan.valid_len, end, rest.zeros_from)) => {
            Ending::Torn(error)
        }
        Ok(_) => damaged(error),
        Err(e) => damaged(format!("{error}; reading on failed: {e}")),
    }
}

/// Whether a record at `start..end` whose bytes from `zeros_from` on, and
/// all bytes after it, are zeros was torn: storage reads zeros where a write
/// never arrived. Such zeros start where the record does, or at a block
/// boundary inside it, since storage writes whole blocks of at least 512
/// bytes; zeros only at the end of a record are more likely its payload.
fn torn_into_zeros(start: u64, end: u64, zeros_from: u64) -> bool {
    zeros_from == start || zeros_from.next_multiple_of(512) < end
}

/// Whether a segment file holds no record at all, intact or not, from its
/// header on; false if part of it cannot be read.
fn holds_nothing(file: File, file_len: u64, first_seq: u64) -> bool {
    let Ok(copy) = file.try_clone() else {
        return false;
    };
    let scan = segment::scan(copy, file_len, first_seq);
    match scan.stopped {
        _ if scan.count > 0 => false,
        None => true,
        Some(Stopped::Unreadable(_)) => false,
        Some(_) => {
            let rest = segment::search(
                &file,
                HEADER_LEN as u64,
                file_len,
                first_seq.saturating_sub(1),
            );
            rest.is_ok_and(|rest| rest.last.is_none())
        }
    }
}

/// Continues the log at `first_seq` with a segment that holds no record yet,
/// created durably, so that no sequence number below it is ever given out
/// again. `known_ts` is the newest receipt time known before it.
fn start_at(dir: &Path, index: &mut Index, first_seq: u64, known_ts: i64) -> io::Result<()> {
    let base_ts = known_ts.max(crate::now_us());
    create_empty(dir, first_seq, base_ts)?;
    index.bytes += HEADER_LEN as u64;
    index.segments.insert(
        first_seq,
        Segment {
            base_ts,
            len: HEADER_LEN as u64,
            damaged: false,
        },
    );
    index.head_seq = first_seq - 1;
    index.head_ts = base_ts;
    Ok(())
}

/// The highest position in a cursor file, a temporary one that an interrupted
/// save left included; 0 without one.
fn highest_cursor(dir: &Path) -> u64 {
    let Ok(entries) = fs::read_dir(dir) else {
        return 0;
    };
    let mut highest = 0;
    for entry in entries.filter_map(|e| e.ok()) {
        // A cursor is at most 20 digits and a newline.
        let mut text = String::new();
        let read = File::open(entry.path()).and_then(|f| f.take(32).read_to_string(&mut text));
        let Some(seq) = read.ok().and_then(|_| text.trim().parse::<u64>().ok()) else {
            continue;
        };
        if seq > MAX_CURSOR {
            warn!(file = %entry.file_name().to_string_lossy(), cursor = seq, "ignoring a cursor no daemon could have written");
            continue;
        }
        highest = highest.max(seq);
    }
    highest
}

/// Takes the log's lock, held for as long as the returned file is open, so
/// that a second instance on the same log stops before it changes anything.
/// The lock goes with the process, so a crash leaves nothing to clean up. The
/// file holds the process id, for the message the second instance gives.
fn lock(dir: &Path) -> io::Result<File> {
    let path = dir.join(LOCK_FILE);
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(FILE_MODE)
        .open(&path)?;
    let locked = loop {
        // SAFETY: flock only takes the descriptor, which `file` keeps open.
        if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } == 0 {
            break Ok(());
        }
        let e = io::Error::last_os_error();
        if e.kind() != io::ErrorKind::Interrupted {
            break Err(e);
        }
    };
    match locked {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
            let pid = fs::read_to_string(&path)
                .ok()
                .and_then(|s| s.trim().parse::<u32>().ok());
            let by = pid.map_or_else(String::new, |pid| format!(", pid {pid}"));
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                format!("locked by another process{by}; is another uevtd running on this log?"),
            ));
        }
        // A file system that cannot lock; a second instance then goes
        // unnoticed, as it did before the lock.
        Err(e) => {
            warn!(path = %path.display(), error = %e, "cannot lock the log; a second instance on it would go unnoticed");
            return Ok(file);
        }
    }
    // Only for the message above, so a full disk does not stop the startup.
    if let Err(e) = file
        .set_len(0)
        .and_then(|()| writeln!(file, "{}", std::process::id()))
    {
        warn!(path = %path.display(), error = %e, "could not write the process id into the lock file");
    }
    Ok(file)
}

fn cut_back(path: &Path, len: u64) -> io::Result<()> {
    OpenOptions::new().write(true).open(path)?.set_len(len)
}

/// Creates a segment without records, durably: file, header and directory
/// entry.
fn create_empty(dir: &Path, first_seq: u64, base_ts: i64) -> io::Result<()> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(FILE_MODE)
        .open(dir.join(segment::file_name(first_seq)))?;
    file.write_all(&Header { first_seq, base_ts }.encode())?;
    file.sync_all()?;
    File::open(dir)?.sync_all()
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
        warn!(dir = %dir.display(), error = %e, "directory sync failed");
    }
}

fn micros(duration: Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}

struct Active {
    first_seq: u64,
    file: File,
    /// Bytes written, including those not yet synced.
    len: u64,
    /// Bytes synced and published to readers.
    committed: u64,
}

/// The single writer. Owns the active segment file. A batch becomes visible
/// to readers only once it is synced, so a crash can never take back a record
/// that a channel delivered or a query returned, nor reuse its sequence number.
pub struct Writer {
    storage: Arc<Storage>,
    active: Option<Active>,
    /// Last written sequence number and timestamp; the index holds the committed ones.
    head_seq: u64,
    head_ts: i64,
    buf: Vec<u8>,
    unsynced: bool,
    last_sync: Instant,
    clock_warned: Option<Instant>,
    /// Set after a failure that makes the active file untrustworthy.
    force_new: bool,
}

impl Writer {
    pub fn new(storage: Arc<Storage>) -> Writer {
        let (head_seq, head_ts) = {
            let index = storage.index.lock().unwrap();
            (index.head_seq, index.head_ts)
        };
        Writer {
            storage,
            active: None,
            head_seq,
            head_ts,
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

    /// Appends a batch as one write. With `sync` it is committed before this
    /// returns; otherwise it stays invisible until the next [`Writer::sync`].
    /// On a write failure the file is rolled back and the batch is dropped.
    ///
    /// A batch that would carry the active segment past `segment_bytes`
    /// starts a new segment, so a segment exceeds that size only when it
    /// holds a single batch that is larger on its own.
    pub fn append(&mut self, batch: &[Message], sync: bool) -> io::Result<()> {
        if batch.is_empty() {
            return Ok(());
        }
        let need: u64 = batch.iter().map(|m| frame_len(m) as u64).sum();
        let segment_bytes = self.storage.segment_bytes.load(Relaxed);
        if self
            .active
            .as_ref()
            .is_some_and(|a| !fits(a.len, need, segment_bytes))
        {
            // A failed sync has already sealed the segment.
            let _ = self.sync();
            self.active = None;
        }
        if self.active.is_none() {
            if let Err(e) = self.open_active(need) {
                self.failed(format!("opening the active segment: {e}"), batch.len());
                return Err(e);
            }
        }

        let (mut seq, mut ts) = (self.head_seq, self.head_ts);
        let mut now = None;
        self.buf.clear();
        for message in batch {
            seq += 1;
            if message.ts < ts {
                // Clamped, and counted as a step only while the clock is
                // behind the log's last timestamp: one set forward again
                // before the writer took the message has not stepped back.
                let now = *now.get_or_insert_with(crate::now_us);
                if now < ts {
                    self.note_clock_step(ts - now);
                }
            } else {
                ts = message.ts;
            }
            segment::encode_frame(&mut self.buf, seq, ts, &message.payload);
        }

        let active = self.active.as_mut().expect("active segment");
        if let Err(e) = active.file.write_all(&self.buf) {
            self.failed(format!("write: {e}"), batch.len());
            self.rollback();
            return Err(e);
        }
        active.len += self.buf.len() as u64;
        self.head_seq = seq;
        self.head_ts = ts;
        self.unsynced = true;
        self.storage.metrics.batches.fetch_add(1, Relaxed);
        if sync { self.sync() } else { Ok(()) }
    }

    /// Syncs the active file and publishes everything written to it. On
    /// failure the unsynced tail is discarded, because the kernel may have
    /// dropped its pages, and the segment is sealed.
    pub fn sync(&mut self) -> io::Result<()> {
        if !self.unsynced {
            return Ok(());
        }
        let Some(active) = self.active.as_mut() else {
            self.unsynced = false;
            return Ok(());
        };
        let started = Instant::now();
        let synced = active.file.sync_data();
        let metrics = &self.storage.metrics;
        metrics.syncs.fetch_add(1, Relaxed);
        metrics
            .sync_us
            .fetch_add(micros(started.elapsed()), Relaxed);
        if let Err(e) = synced {
            error!(error = %e, "sync failed; sealing segment");
            if let Err(e) = active.file.set_len(active.committed) {
                error!(error = %e, "could not truncate the unsynced tail");
            }
            self.failed(format!("sync: {e}"), 0);
            self.seal();
            return Err(e);
        }
        self.unsynced = false;
        self.last_sync = Instant::now();
        let added = active.len - active.committed;
        if added == 0 {
            return Ok(());
        }
        active.committed = active.len;
        metrics.last_error.clear();
        {
            let mut index = self.storage.index.lock().unwrap();
            if let Some(segment) = index.segments.get_mut(&active.first_seq) {
                segment.len = active.len;
            }
            index.bytes += added;
            let records = self.head_seq - index.head_seq;
            index.head_seq = self.head_seq;
            index.head_ts = self.head_ts;
            self.storage.metrics.records.fetch_add(records, Relaxed);
        }
        self.storage.metrics.bytes.fetch_add(added, Relaxed);
        self.storage.head.send_replace(self.head_seq);
        self.storage.evict();
        Ok(())
    }

    /// Reopens the last segment if `need` more bytes fit into it, or creates
    /// the next one.
    fn open_active(&mut self, need: u64) -> io::Result<()> {
        let last = {
            let index = self.storage.index.lock().unwrap();
            debug_assert_eq!(self.head_seq, index.head_seq, "no segment, nothing pending");
            index.segments.iter().next_back().map(|(k, s)| (*k, s.len))
        };
        if let Some((first_seq, len)) = last {
            if fits(len, need, self.storage.segment_bytes.load(Relaxed)) && !self.force_new {
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
                        committed: len,
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
        self.create_segment(self.head_seq + 1, self.head_ts)?;
        // Only once it exists: until then, no batch may go into the segment
        // that the failure sealed.
        self.force_new = false;
        Ok(())
    }

    fn create_segment(&mut self, first_seq: u64, base_ts: i64) -> io::Result<()> {
        let path = self.storage.segment_path(first_seq);
        // A file that already starts at this sequence number holds no record
        // (otherwise the head would be past it), so it is replaced. Its index
        // entry goes only once the file has: while the file is there, the
        // next batch must not go into the segment before it, since at the
        // next start the file would hide that batch and give its sequence
        // numbers out again.
        match fs::remove_file(&path) {
            Ok(()) => warn!(segment = first_seq, "replacing an empty segment file"),
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
        {
            let mut index = self.storage.index.lock().unwrap();
            if let Some(stale) = index.segments.remove(&first_seq) {
                index.bytes -= stale.len;
            }
        }
        let mut file = OpenOptions::new()
            .append(true)
            .create_new(true)
            .mode(FILE_MODE)
            .open(&path)?;
        if let Err(e) = file.write_all(&Header { first_seq, base_ts }.encode()) {
            let _ = fs::remove_file(&path);
            return Err(e);
        }
        sync_dir(&self.storage.dir);
        let len = HEADER_LEN as u64;
        {
            let mut index = self.storage.index.lock().unwrap();
            index.segments.insert(
                first_seq,
                Segment {
                    base_ts,
                    len,
                    damaged: false,
                },
            );
            index.bytes += len;
        }
        self.active = Some(Active {
            first_seq,
            file,
            len,
            committed: len,
        });
        self.unsynced = true;
        info!(segment = first_seq, "created segment");
        Ok(())
    }

    /// Counts a failure that loses `records` of the batch at hand, and keeps
    /// it as the log's error until a batch is committed again.
    fn failed(&self, error: String, records: usize) {
        let metrics = &self.storage.metrics;
        metrics.write_errors.fetch_add(1, Relaxed);
        metrics.dropped.fetch_add(records as u64, Relaxed);
        metrics.last_error.set(error);
    }

    fn rollback(&mut self) {
        let Some(active) = self.active.as_mut() else {
            return;
        };
        if let Err(e) = active.file.set_len(active.len) {
            error!(error = %e, "rollback failed; sealing segment");
            self.seal();
        }
    }

    /// Abandons the active segment together with the records written to it
    /// since the last sync; the next batch starts a new segment.
    fn seal(&mut self) {
        let discarded = {
            let index = self.storage.index.lock().unwrap();
            let discarded = self.head_seq - index.head_seq;
            self.head_seq = index.head_seq;
            self.head_ts = index.head_ts;
            discarded
        };
        if discarded > 0 {
            error!(records = discarded, "unsynced records discarded");
            self.storage.metrics.dropped.fetch_add(discarded, Relaxed);
        }
        self.active = None;
        self.unsynced = false;
        self.force_new = true;
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

/// Whether `need` more bytes fit into a segment that is `len` bytes long. A
/// segment without records takes any batch, so that a record larger than
/// `segment_bytes` still has a place.
fn fits(len: u64, need: u64, segment_bytes: u64) -> bool {
    len <= HEADER_LEN as u64 || len.saturating_add(need) <= segment_bytes
}

fn frame_len(message: &Message) -> usize {
    FRAME_HEADER_LEN + message.payload.len()
}

/// Writer thread body: group commit with the sync policy from the storage limits.
pub fn run_writer(storage: Arc<Storage>, mut rx: QueueReceiver, rt: tokio::runtime::Handle) {
    let mut writer = Writer::new(storage.clone());
    let mut batch: Vec<Message> = Vec::with_capacity(MAX_BATCH);
    // A message that did not fit into the previous batch starts the next one.
    let mut carry: Option<Message> = None;
    loop {
        let first = if let Some(message) = carry.take() {
            message
        } else if writer.unsynced() {
            // The interval is re-read at least once a second while data is
            // pending, so a change made meanwhile (to zero in particular)
            // takes effect without waiting for the next message.
            let wait = storage.sync_interval().saturating_sub(writer.since_sync());
            if wait.is_zero() {
                let _busy = storage.busy();
                if let Err(e) = writer.sync() {
                    error!(error = %e, "periodic sync failed");
                }
                continue;
            }
            // The timer must be created inside the runtime context that
            // `block_on` enters, not before it.
            match rt.block_on(async {
                tokio::time::timeout(wait.min(Duration::from_secs(1)), rx.recv()).await
            }) {
                Ok(Some(m)) => m,
                Ok(None) => break,
                Err(_) => continue,
            }
        } else {
            match rx.blocking_recv() {
                Some(m) => m,
                None => break,
            }
        };
        // A batch is one write into one segment, so it never holds more than
        // an empty segment does.
        let segment_room = usize::try_from(storage.segment_bytes.load(Relaxed))
            .unwrap_or(usize::MAX)
            .saturating_sub(HEADER_LEN);
        let cap = MAX_BATCH_BYTES.min(segment_room);
        let mut bytes = frame_len(&first);
        batch.push(first);
        while batch.len() < MAX_BATCH {
            let Some(message) = rx.try_recv() else {
                break;
            };
            let len = frame_len(&message);
            if bytes + len > cap {
                carry = Some(message);
                break;
            }
            bytes += len;
            batch.push(message);
        }
        let interval = storage.sync_interval();
        let sync = interval.is_zero() || writer.since_sync() >= interval;
        let busy = storage.busy();
        if let Err(e) = writer.append(&batch, sync) {
            error!(error = %e, records = batch.len(), "write failed; batch dropped");
        }
        drop(busy);
        rx.release(&batch);
        batch.clear();
    }
    let _busy = storage.busy();
    if let Err(e) = writer.sync() {
        error!(error = %e, "final sync failed");
    }
    info!("log writer stopped");
}

/// One page of a time-range query.
#[derive(Default, Debug)]
pub struct Page {
    pub records: Vec<Record>,
    /// More matching records follow; the page ended at its record or byte limit.
    pub more: bool,
}

/// What a read returns: consecutive records, or a gap. A batch never holds
/// both, so a gap always ends at the reader's position.
#[derive(Default, Debug)]
pub struct Batch {
    pub records: Vec<Record>,
    /// With no records: how many records before the reader's position no
    /// longer exist (evicted or unreadable). Zero as well means caught up.
    pub skipped: u64,
}

enum Step {
    Record(Record),
    /// Moved past this many missing records.
    Skipped(u64),
    /// Caught up, or missing records follow that this read may not skip.
    Stop,
}

enum Located {
    At(View),
    CaughtUp,
    /// The position was evicted; the oldest segment starts here.
    Evicted(u64),
}

struct View {
    first_seq: u64,
    bound: u64,
    next_first_seq: Option<u64>,
    head_seq: u64,
    /// Eviction is removing the segment.
    evicting: bool,
}

struct Open {
    first_seq: u64,
    cursor: SegmentCursor,
}

/// Why a reader skips the rest of a segment.
enum Damage<'a> {
    Corrupt(Corrupt),
    /// Reads failed with this error for [`UNREADABLE_RETRY`].
    Unreadable(&'a io::Error),
}

/// A tailing cursor over the log. Cheap to advance; re-positioning to an
/// arbitrary sequence number scans from the start of its segment once.
pub struct Reader {
    storage: Arc<Storage>,
    next_seq: u64,
    current: Option<Open>,
    /// The [`Tail`] this reader moves, if it reads for a channel.
    tail: Option<u64>,
    /// The position whose data could not be read back, and since when. A
    /// reader only moves forward, so a read that works moves past it.
    unreadable: Option<(u64, Instant)>,
    /// How long such a position is retried; [`UNREADABLE_RETRY`].
    retry_for: Duration,
}

impl Reader {
    pub fn new(storage: Arc<Storage>, next_seq: u64) -> Reader {
        Reader {
            storage,
            next_seq: next_seq.max(1),
            current: None,
            tail: None,
            unreadable: None,
            retry_for: UNREADABLE_RETRY,
        }
    }

    pub fn next_seq(&self) -> u64 {
        self.next_seq
    }

    /// Reads up to `max_records` / `max_bytes` of consecutive records. Where
    /// records are missing (evicted or unreadable), the batch holds no
    /// records and says how many were passed over: a gap always comes alone.
    /// An empty batch with nothing skipped means the reader has caught up.
    /// An I/O error is returned only if nothing was read before it;
    /// otherwise the records are returned and the error surfaces on the next
    /// call, which starts at the failing position with nothing to lose. An
    /// error that says the data cannot be read back, at the same position
    /// for [`UNREADABLE_RETRY`], skips the rest of the segment instead, as
    /// corruption does.
    pub fn read(&mut self, max_records: usize, max_bytes: usize) -> io::Result<Batch> {
        let start = self.next_seq;
        if self.begin() {
            return Ok(Batch {
                records: Vec::new(),
                skipped: self.next_seq - start,
            });
        }
        let batch = self.fill(max_records, max_bytes);
        self.settle(batch)
    }

    /// Ends a read that [`Reader::begin`] started, with what it found.
    fn settle(&mut self, batch: io::Result<Batch>) -> io::Result<Batch> {
        let batch = match batch {
            Err(e) if self.gives_up(&e) => Ok(self.skip_unreadable(&e)),
            batch => batch,
        };
        self.end(batch.as_ref().map_or(0, |b| b.skipped));
        batch
    }

    /// Whether a read that failed at `next_seq` with `e` has failed for
    /// long enough to skip the rest of the segment. Only an error that says
    /// the data cannot be read back counts. Others, such as a missing
    /// permission or no descriptor left, can be fixed, and then cost nothing.
    fn gives_up(&mut self, e: &io::Error) -> bool {
        if !unreadable(e) {
            return false;
        }
        match self.unreadable {
            Some((seq, since)) if seq == self.next_seq => since.elapsed() >= self.retry_for,
            _ => {
                self.unreadable = Some((self.next_seq, Instant::now()));
                false
            }
        }
    }

    /// Skips the rest of the segment that holds `next_seq`, which reads could
    /// not get past with `e` for [`UNREADABLE_RETRY`].
    fn skip_unreadable(&mut self, e: &io::Error) -> Batch {
        let step = match self.find() {
            Located::At(view) => self.skip_segment(&view, Damage::Unreadable(e), true),
            Located::Evicted(first) => self.skip_to(first, true),
            Located::CaughtUp => Step::Stop,
        };
        match step {
            Step::Skipped(missing) => Batch {
                records: Vec::new(),
                skipped: missing,
            },
            Step::Record(_) | Step::Stop => Batch::default(),
        }
    }

    fn fill(&mut self, max_records: usize, max_bytes: usize) -> io::Result<Batch> {
        let mut batch = Batch::default();
        let mut bytes = 0usize;
        let mut reopen_attempts = 0;
        while batch.records.len() < max_records && bytes < max_bytes {
            match self.next(batch.records.is_empty()) {
                Ok(Step::Record(record)) => {
                    bytes += record.payload.len();
                    batch.records.push(record);
                }
                Ok(Step::Skipped(missing)) => {
                    batch.skipped = missing;
                    break;
                }
                Ok(Step::Stop) => break,
                Err(e) if e.kind() == io::ErrorKind::NotFound && reopen_attempts < 3 => {
                    reopen_attempts += 1;
                    self.current = None;
                }
                Err(_) if !batch.records.is_empty() => break,
                Err(e) => return Err(e),
            }
        }
        Ok(batch)
    }

    /// The record at `next_seq`. Where records are missing, moves past them
    /// if `may_skip`, and stops in front of them otherwise.
    fn next(&mut self, may_skip: bool) -> io::Result<Step> {
        loop {
            let view = match self.locate()? {
                Located::At(view) => view,
                Located::CaughtUp => return Ok(Step::Stop),
                Located::Evicted(first) => return Ok(self.skip_to(first, may_skip)),
            };
            let open = self.current.as_mut().expect("positioned");
            match open.cursor.next(view.bound)? {
                Next::Record(record) if record.seq == self.next_seq => {
                    self.next_seq += 1;
                    return Ok(Step::Record(record));
                }
                Next::Record(_) => {
                    let cause = Damage::Corrupt(Corrupt::Invalid("sequence mismatch"));
                    return Ok(self.skip_segment(&view, cause, may_skip));
                }
                Next::End => match view.next_first_seq {
                    Some(next) if next > self.next_seq => return Ok(self.skip_to(next, may_skip)),
                    Some(_) => self.current = None,
                    None => return Ok(Step::Stop),
                },
                Next::Corrupt(c) => {
                    return Ok(self.skip_segment(&view, Damage::Corrupt(c), may_skip));
                }
            }
        }
    }

    /// Moves past the missing records below `seq`, if this read may.
    fn skip_to(&mut self, seq: u64, may_skip: bool) -> Step {
        if !may_skip {
            return Step::Stop;
        }
        debug_assert!(seq > self.next_seq, "a gap ends past the reader");
        let missing = seq.saturating_sub(self.next_seq);
        self.next_seq = self.next_seq.max(seq);
        self.current = None;
        Step::Skipped(missing)
    }

    fn skip_segment(&mut self, view: &View, damage: Damage<'_>, may_skip: bool) -> Step {
        if !may_skip {
            // The next read reopens the segment and finds the record again.
            self.current = None;
            return Step::Stop;
        }
        let offset = self.current.as_ref().map(|o| o.cursor.pos()).unwrap_or(0);
        // Counted and logged once per segment, not by every reader that
        // passes it.
        let first = {
            let mut index = self.storage.index.lock().unwrap();
            let segment = index.segments.get_mut(&view.first_seq);
            segment.is_some_and(|s| !std::mem::replace(&mut s.damaged, true))
        };
        let (segment, seq) = (view.first_seq, self.next_seq);
        match damage {
            Damage::Corrupt(cause) if first => {
                error!(segment, offset, error = %cause, "corrupt record; skipping the rest of the segment");
            }
            Damage::Corrupt(cause) => {
                debug!(segment, offset, error = %cause, "corrupt record; skipping the rest of the segment");
            }
            Damage::Unreadable(e) if first => {
                error!(segment, seq, error = %e, retried_s = self.retry_for.as_secs(),
                    "segment unreadable; skipping the rest of it");
            }
            Damage::Unreadable(e) => {
                debug!(segment, seq, error = %e, "segment unreadable; skipping the rest of it");
            }
        }
        if first {
            self.storage.metrics.corrupt_segments.fetch_add(1, Relaxed);
        }
        let Some(next) = view.next_first_seq else {
            // The active segment. The records written after the head will
            // start at its committed length, where a reader that had read up
            // to the head would stand. Waiting there, the reader never reads
            // the damaged part again, which positioning in the segment would,
            // and then skip what was written meanwhile.
            let step = self.skip_to(view.head_seq + 1, true);
            if let Ok(file) = File::open(self.storage.segment_path(segment)) {
                self.current = Some(Open {
                    first_seq: segment,
                    cursor: SegmentCursor::new(file, view.bound),
                });
            }
            return step;
        };
        self.skip_to(next, true)
    }

    /// Starts a read for a tail. Returns true, with no read to follow, if the
    /// tail is ahead of the reader, past records already counted as lost
    /// while the reader was idle: the reader catches up to it. Otherwise the
    /// read counts what it skips, in [`Reader::end`].
    fn begin(&mut self) -> bool {
        let Some(id) = self.tail else {
            return false;
        };
        let mut index = self.storage.index.lock().unwrap();
        let Some(tail) = index.tails.get_mut(&id) else {
            return false;
        };
        if tail.next > self.next_seq {
            self.next_seq = tail.next;
            self.current = None;
            return true;
        }
        tail.reading = true;
        false
    }

    /// Ends a read for a tail: counts the `skipped` records it passed over,
    /// moves the tail to the reader, and counts what an eviction removed
    /// beyond it meanwhile.
    fn end(&mut self, skipped: u64) {
        let Some(id) = self.tail else {
            return;
        };
        let mut index = self.storage.index.lock().unwrap();
        let first = index.first_seq();
        let Some(tail) = index.tails.get_mut(&id) else {
            return;
        };
        if skipped > 0 {
            tail.lose(self.next_seq - skipped, self.next_seq);
        }
        tail.next = self.next_seq;
        tail.reading = false;
        tail.claim(first);
    }

    /// Where `next_seq` is in the index.
    fn find(&self) -> Located {
        let index = self.storage.index.lock().unwrap();
        if self.next_seq > index.head_seq {
            return Located::CaughtUp;
        }
        let Some((&first_seq, segment)) = index.segments.range(..=self.next_seq).next_back() else {
            // Evicted; or the index is momentarily empty while the writer
            // replaces a segment file.
            return match index.segments.keys().next() {
                Some(&first) => Located::Evicted(first),
                None => Located::CaughtUp,
            };
        };
        Located::At(View {
            first_seq,
            bound: segment.len,
            next_first_seq: index
                .segments
                .range(first_seq + 1..)
                .next()
                .map(|(k, _)| *k),
            head_seq: index.head_seq,
            evicting: first_seq < index.evicting_below,
        })
    }

    /// Positions the cursor at `next_seq`.
    fn locate(&mut self) -> io::Result<Located> {
        let view = match self.find() {
            Located::At(view) => view,
            other => return Ok(other),
        };
        if self.current.as_ref().map(|o| o.first_seq) != Some(view.first_seq) {
            let file = match File::open(self.storage.segment_path(view.first_seq)) {
                Ok(file) => file,
                // Removed by eviction, which takes the entry once all its
                // files are gone.
                Err(e) if e.kind() == io::ErrorKind::NotFound && view.evicting => {
                    let next = view.next_first_seq.unwrap_or(view.head_seq + 1);
                    return Ok(Located::Evicted(next));
                }
                Err(e) => return Err(e),
            };
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
        Ok(Located::At(view))
    }

    /// Retries data that cannot be read back for `retry_for` instead.
    #[cfg(test)]
    pub(crate) fn retry_unreadable_for(&mut self, retry_for: Duration) {
        self.retry_for = retry_for;
    }
}

/// Whether `e` says that the data cannot be read back: an I/O error of the
/// device, as from a worn sector, or a file system that found its own
/// structures damaged.
fn unreadable(e: &io::Error) -> bool {
    match e.raw_os_error() {
        Some(libc::EIO | libc::EBADMSG) => true,
        #[cfg(target_os = "linux")]
        Some(libc::EUCLEAN) => true,
        _ => false,
    }
}

#[cfg(test)]
mod model_tests;

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
        // Messages out of order are clamped too, but only a clock behind the
        // log's last timestamp counts as stepped back.
        assert_eq!(storage.metrics().clock_steps.load(Relaxed), 0);
        let ahead = crate::now_us() + 60_000_000;
        writer
            .append(&[msg(ahead, "d"), msg(crate::now_us(), "e")], true)
            .unwrap();
        let records = storage.reader(4).read(10, 1 << 20).unwrap().records;
        assert_eq!(
            records.iter().map(|r| r.ts).collect::<Vec<_>>(),
            vec![ahead, ahead]
        );
        assert_eq!(storage.metrics().clock_steps.load(Relaxed), 1);
    }

    #[test]
    fn a_failed_write_is_counted_and_reported_until_a_batch_is_committed() {
        let dir = tempfile::tempdir().unwrap();
        let (storage, mut writer) = open(dir.path(), limits(1 << 20, 64 << 10));
        assert_eq!(storage.health(), Ok(()));
        // A directory where the first segment goes: creating it fails.
        let blocker = dir.path().join(segment::file_name(1));
        fs::create_dir(&blocker).unwrap();
        assert!(writer.append(&[msg(1, "a"), msg(2, "b")], true).is_err());
        let metrics = storage.metrics();
        let counts = || {
            (
                metrics.write_errors.load(Relaxed),
                metrics.dropped.load(Relaxed),
                metrics.syncs.load(Relaxed),
            )
        };
        assert_eq!(counts(), (1, 2, 0));
        let health = storage.health().unwrap_err();
        assert!(
            health.starts_with("log write failed: opening the active segment: "),
            "{health}"
        );

        fs::remove_dir(&blocker).unwrap();
        writer.append(&[msg(3, "c")], true).unwrap();
        assert_eq!(storage.health(), Ok(()));
        assert_eq!(counts(), (1, 2, 1));
        assert_eq!(storage.head_seq(), 1, "the lost batch took no numbers");
    }

    #[test]
    fn a_batch_or_sync_that_takes_too_long_makes_the_log_unhealthy() {
        let dir = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(dir.path(), limits(1 << 20, 64 << 10)).unwrap();
        storage.opened = Instant::now() - Duration::from_secs(60);
        let busy = storage.busy();
        assert_eq!(storage.health(), Ok(()));
        drop(busy);
        // As if the writer had begun when the log was opened.
        storage.busy_since.store(1, Relaxed);
        assert_eq!(storage.health(), Err("log writer busy for 60 s".into()));
        storage.busy_since.store(0, Relaxed);
        assert_eq!(storage.health(), Ok(()));
    }

    #[test]
    fn timestamps_are_found_by_sequence_number() {
        let dir = tempfile::tempdir().unwrap();
        let (storage, mut writer) = open(dir.path(), limits(128 << 10, 64 << 10));
        let payload = "x".repeat(1000);
        for ts in 1..=150 {
            writer.append(&[msg(ts * 10, &payload)], true).unwrap();
        }
        // 63 records a segment; the first segment is evicted.
        let first = storage.status().first_seq;
        assert_eq!(first, 64);
        let mut mark = Mark::default();
        // A lookup after an earlier one in the same segment reads on from
        // its mark; any other reads from the start of the segment.
        for seq in [first, first + 3, first + 1, 140, 150, first + 2] {
            let ts = storage.timestamp(seq, &mut mark).unwrap();
            assert_eq!(ts, Some(seq as i64 * 10), "{seq}");
        }
        assert_eq!(storage.timestamp(first - 1, &mut mark).unwrap(), None);
        assert_eq!(storage.timestamp(151, &mut mark).unwrap(), None);
        assert_eq!(storage.timestamp(0, &mut mark).unwrap(), None);
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
        writer.sync().unwrap();
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

        // A reader positioned before the eviction horizon reports the gap
        // on its own, then continues at the oldest record.
        let mut reader = storage.reader(1);
        let gap = reader.read(5, 1 << 20).unwrap();
        assert_eq!((gap.records.len(), gap.skipped), (0, status.first_seq - 1));
        let batch = reader.read(5, 1 << 20).unwrap();
        assert_eq!(batch.skipped, 0);
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
        assert_eq!(storage.metrics().corrupt_segments.load(Relaxed), 0);
        writer.append(&[msg(5, "c")], true).unwrap();
        let records = storage.reader(1).read(10, 1 << 20).unwrap().records;
        assert_eq!(
            records.iter().map(|r| r.seq).collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        assert_eq!(records[2].payload, b"c");
        drop((storage, writer));

        // Reopen again: nothing to repair, head continues.
        let (storage, _) = open(dir.path(), l);
        assert_eq!(storage.status().head_seq, 3);
    }

    /// A segment file holding `records`, each `(seq, ts, payload)`.
    fn segment_file(first_seq: u64, base_ts: i64, records: &[(u64, i64, &str)]) -> Vec<u8> {
        let mut bytes = Header { first_seq, base_ts }.encode().to_vec();
        for (seq, ts, payload) in records {
            segment::encode_frame(&mut bytes, *seq, *ts, payload.as_bytes());
        }
        bytes
    }

    fn seqs(batch: &Batch) -> Vec<u64> {
        batch.records.iter().map(|r| r.seq).collect()
    }

    /// Reads from `reader` until it has caught up: the sequence numbers
    /// read, and how many it passed over.
    fn read_to_end(reader: &mut Reader) -> (Vec<u64>, u64) {
        let (mut got, mut skipped) = (Vec::new(), 0);
        loop {
            let batch = reader.read(100, 1 << 30).unwrap();
            if batch.records.is_empty() && batch.skipped == 0 {
                return (got, skipped);
            }
            skipped += batch.skipped;
            got.extend(seqs(&batch));
        }
    }

    #[test]
    fn an_unreadable_active_segment_is_kept_and_numbering_continues_past_it() {
        let dir = tempfile::tempdir().unwrap();
        let l = limits(1 << 20, 64 << 10);
        {
            let (_storage, mut writer) = open(dir.path(), l);
            writer
                .append(&[msg(1, "a"), msg(2, "b"), msg(3, "c")], true)
                .unwrap();
        }
        // A directory in place of the next segment: reading it fails as a
        // bad sector would. Its entries give it a size on every file system.
        let unreadable = dir.path().join(segment::file_name(4));
        fs::create_dir(&unreadable).unwrap();
        for name in ["x", "y"] {
            fs::write(unreadable.join(name), b"").unwrap();
        }
        let size = fs::metadata(&unreadable).unwrap().len();
        let next = 4 + (size - HEADER_LEN as u64) / FRAME_HEADER_LEN as u64;
        assert!(next > 5, "{size}");

        // The records before the failure stay. The new segment starts past
        // every record the unreadable part could hold, and readers retry
        // that part, as a read error anywhere else.
        {
            let (storage, _) = open(dir.path(), l);
            assert_eq!(storage.head_seq(), next - 1);
            assert_eq!(storage.metrics().corrupt_segments.load(Relaxed), 1);
            let mut reader = storage.reader(1);
            assert_eq!(seqs(&reader.read(10, 1 << 20).unwrap()), [1, 2, 3]);
            assert!(reader.read(10, 1 << 20).is_err());
        }
        // Creating that segment was the only change, and it is durable, so a
        // restart finds the same.
        let (storage, mut writer) = open(dir.path(), l);
        assert_eq!(storage.head_seq(), next - 1);
        writer.append(&[msg(5, "d")], true).unwrap();
        // Once the damaged part reads again, its records are delivered, and
        // the numbers it did not use are passed over.
        fs::remove_dir_all(&unreadable).unwrap();
        let readable = segment_file(4, 3, &[(4, 4, "x"), (5, 5, "y")]);
        fs::write(&unreadable, readable).unwrap();
        let (got, skipped) = read_to_end(&mut storage.reader(1));
        assert_eq!((got, skipped), (vec![1, 2, 3, 4, 5, next], next - 6));
    }

    #[test]
    fn records_left_past_the_next_segment_are_never_read() {
        let dir = tempfile::tempdir().unwrap();
        // Segment 1 still holds a record 3 that was never committed, as a
        // failed sync with a failed rollback leaves it; segment 3 holds the
        // record committed in its place. A reader locates every record in
        // the last segment starting at or before it, so it never sees the
        // leftover.
        let left_over = segment_file(1, 0, &[(1, 1, "a"), (2, 2, "b"), (3, 3, "left over")]);
        fs::write(dir.path().join(segment::file_name(1)), left_over).unwrap();
        let next = segment_file(3, 2, &[(3, 4, "c"), (4, 5, "d")]);
        fs::write(dir.path().join(segment::file_name(3)), next).unwrap();

        let (storage, _) = open(dir.path(), limits(1 << 20, 64 << 10));
        let payloads = |records: Vec<Record>| {
            records
                .into_iter()
                .map(|r| (r.seq, String::from_utf8(r.payload).unwrap()))
                .collect::<Vec<_>>()
        };
        let expected = [(1, "a"), (2, "b"), (3, "c"), (4, "d")].map(|(s, p)| (s, p.to_string()));
        let batch = storage.reader(1).read(10, 1 << 20).unwrap();
        assert_eq!(payloads(batch.records), expected);
        let page = storage.query(Some(0), None, 0, 10, 1 << 20).unwrap();
        assert_eq!(payloads(page.records), expected);
    }

    #[test]
    fn a_sealed_segment_that_cannot_be_opened_does_not_block_startup() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let sealed = dir.path().join(segment::file_name(1));
        fs::write(&sealed, segment_file(1, 0, &[(1, 1, "a"), (2, 2, "b")])).unwrap();
        let active = segment_file(3, 2, &[(3, 3, "c")]);
        fs::write(dir.path().join(segment::file_name(3)), active).unwrap();
        fs::set_permissions(&sealed, fs::Permissions::from_mode(0o000)).unwrap();

        let (storage, _) = open(dir.path(), limits(1 << 20, 64 << 10));
        assert_eq!(storage.head_seq(), 3);
        let mut reader = storage.reader(1);
        // Reads fail and are retried until the file is readable again, with
        // nothing lost meanwhile (unless the tests run as root, which reads
        // it anyway).
        if File::open(&sealed).is_err() {
            let err = reader.read(10, 1 << 20).unwrap_err();
            assert_eq!(err.kind(), io::ErrorKind::PermissionDenied);
        }
        fs::set_permissions(&sealed, fs::Permissions::from_mode(0o640)).unwrap();
        assert_eq!(seqs(&reader.read(10, 1 << 20).unwrap()), [1, 2, 3]);
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
            if batch.records.is_empty() && batch.skipped == 0 {
                break;
            }
            skipped += batch.skipped;
            seqs.extend(batch.records.iter().map(|r| r.seq));
        }
        assert!(skipped > 0);
        assert_eq!(seqs.len() as u64 + skipped, 20);
        assert_eq!(*seqs.last().unwrap(), 20);
        assert!(seqs.windows(2).all(|w| w[1] > w[0]));
        assert_eq!(storage.metrics().corrupt_segments.load(Relaxed), 1);
        // Counted once, however many readers and queries pass it.
        assert_eq!(read_to_end(&mut storage.reader(1)).1, skipped);
        storage.query(None, None, 0, 100, 1 << 30).unwrap();
        assert_eq!(storage.metrics().corrupt_segments.load(Relaxed), 1);
    }

    fn write_records(writer: &mut Writer, records: usize) {
        for _ in 0..records {
            let message = Message {
                ts: 0,
                payload: vec![b'f'; 1000],
            };
            writer.append(&[message], true).unwrap();
        }
    }

    #[test]
    fn loss_is_counted_once_and_when_it_happens() {
        let dir = tempfile::tempdir().unwrap();
        let (storage, mut writer) = open(dir.path(), limits(128 << 10, 64 << 10));
        let lost = Arc::new(Loss::default());
        let tail = storage.tail("c", 1, lost.clone());
        let mut reader = tail.reader(1);

        // Nothing was read, so the eviction counts at once.
        write_records(&mut writer, 200);
        let first = storage.status().first_seq;
        assert!(first > 1);
        assert_eq!(lost.records(), first - 1);
        // The read reports the gap alone without counting it again, then
        // continues at the oldest record.
        let gap = reader.read(1000, 1 << 30).unwrap();
        assert_eq!((gap.records.len(), gap.skipped), (0, first - 1));
        let batch = reader.read(10, 1 << 30).unwrap();
        assert_eq!((batch.records[0].seq, batch.skipped), (first, 0));
        assert_eq!(lost.records(), first - 1);

        // Records the reader holds are not lost when evicted; the ones it
        // has not reached are.
        write_records(&mut writer, 200);
        let second = storage.status().first_seq;
        assert_eq!(lost.records(), second - 1 - 10);

        // A tail kept from the start of a lagging position counts at once.
        let late = Arc::new(Loss::default());
        let _late = storage.tail("late", 1, late.clone());
        assert_eq!(late.records(), second - 1);

        // A dropped tail no longer counts, and its reader still reads.
        drop(tail);
        write_records(&mut writer, 200);
        assert_eq!(lost.records(), second - 1 - 10);
        assert!(reader.read(10, 1 << 30).unwrap().skipped > 0);

        // A channel restarted at a cursor that still waits for its notice
        // does not count the same records again, only those evicted while
        // it had no tail.
        let third = storage.status().first_seq;
        let again = storage.tail("c", 1, lost.clone());
        assert_eq!(lost.records(), third - 1 - 10);
        let gap = again.reader(1).read(1000, 1 << 30).unwrap();
        assert_eq!(gap.skipped, third - 1);
    }

    #[test]
    fn an_eviction_during_a_read_is_counted_when_the_read_ends() {
        let dir = tempfile::tempdir().unwrap();
        let (storage, mut writer) = open(dir.path(), limits(128 << 10, 64 << 10));
        let lost = Arc::new(Loss::default());
        let tail = storage.tail("c", 1, lost.clone());
        let mut reader = tail.reader(1);
        write_records(&mut writer, 10);
        assert_eq!(reader.read(10, 1 << 30).unwrap().records.len(), 10);

        // Records the read may already be returning are not counted under it.
        assert!(!reader.begin());
        write_records(&mut writer, 200);
        assert_eq!(lost.records(), 0);
        reader.end(0);
        let first = storage.status().first_seq;
        assert_eq!(lost.records(), first - 11);
        let gap = reader.read(1000, 1 << 30).unwrap();
        assert_eq!((gap.records.len(), gap.skipped), (0, first - 11));
        assert_eq!(lost.records(), first - 11);
    }

    #[test]
    fn a_gap_found_by_a_read_is_counted_by_it() {
        let dir = tempfile::tempdir().unwrap();
        let (storage, mut writer) = open(dir.path(), limits(1 << 20, 64 << 10));
        for i in 0..20 {
            let message = Message {
                ts: i,
                payload: vec![b'g'; 10_000],
            };
            writer.append(&[message], true).unwrap();
        }
        let second = *storage
            .index
            .lock()
            .unwrap()
            .segments
            .keys()
            .nth(1)
            .unwrap();
        let path = storage.segment_path(1);
        let mut bytes = fs::read(&path).unwrap();
        let at = HEADER_LEN + 2 * (FRAME_HEADER_LEN + 10_000) + FRAME_HEADER_LEN;
        bytes[at] ^= 0x55;
        fs::write(&path, &bytes).unwrap();

        let lost = Arc::new(Loss::default());
        let tail = storage.tail("c", 1, lost.clone());
        let mut reader = tail.reader(1);
        // Records before the damage, then the gap alone, then the rest.
        let before = reader.read(1000, 1 << 30).unwrap();
        assert_eq!(
            before.records.iter().map(|r| r.seq).collect::<Vec<_>>(),
            vec![1, 2]
        );
        assert_eq!(lost.records(), 0);
        let gap = reader.read(1000, 1 << 30).unwrap();
        assert_eq!((gap.records.len(), gap.skipped), (0, second - 3));
        assert_eq!(lost.records(), second - 3);
        let after = reader.read(1000, 1 << 30).unwrap();
        assert_eq!(after.records[0].seq, second);
        assert_eq!(lost.records(), second - 3);
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
        writer.sync().unwrap();
        assert!(storage.status().segments > 1);
        let all = usize::MAX;
        let got = storage.query(Some(1200), Some(1250), 0, 100, all).unwrap();
        assert_eq!(
            got.records.iter().map(|r| r.ts).collect::<Vec<_>>(),
            vec![1200, 1210, 1220, 1230, 1240]
        );
        assert!(!got.more);
        let page = storage
            .query(Some(1200), None, got.records[1].seq, 2, all)
            .unwrap();
        assert_eq!(
            page.records.iter().map(|r| r.ts).collect::<Vec<_>>(),
            vec![1220, 1230]
        );
        assert!(page.more);
        assert!(
            storage
                .query(Some(5000), None, 0, 10, all)
                .unwrap()
                .records
                .is_empty()
        );
        let first = storage.query(None, Some(1010), 0, 10, all).unwrap();
        assert_eq!((first.records.len(), first.more), (1, false));
        let last = storage.query(None, None, 98, 2, all).unwrap();
        assert_eq!((last.records.len(), last.more), (2, false));
    }

    #[test]
    fn query_pages_are_cut_by_payload_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let (storage, mut writer) = open(dir.path(), limits(1 << 20, 64 << 10));
        for i in 1..=10 {
            writer
                .append(
                    &[Message {
                        ts: i,
                        payload: vec![b'p'; 1000],
                    }],
                    true,
                )
                .unwrap();
        }
        let page = storage.query(None, None, 0, 100, 3500).unwrap();
        assert_eq!(page.records.len(), 3);
        assert!(page.more);
        let next = storage
            .query(None, None, page.records[2].seq, 100, 3500)
            .unwrap();
        assert_eq!(next.records[0].seq, 4);
        // The first record is returned even when it alone exceeds the budget.
        let one = storage.query(None, None, 0, 100, 10).unwrap();
        assert_eq!((one.records.len(), one.more), (1, true));
        let rest = storage.query(None, None, 7, 100, 1 << 20).unwrap();
        assert_eq!((rest.records.len(), rest.more), (3, false));
    }

    #[test]
    fn segments_never_grow_past_segment_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let segment_bytes = 64 << 10;
        let (storage, mut writer) = open(dir.path(), limits(64 << 20, segment_bytes));
        let mut ts = 0;
        let mut written = 0u64;
        for records in [1usize, 3, 7, 20, 2, 50, 1, 11] {
            for size in [100usize, 1000, 5000, 30_000] {
                let batch: Vec<Message> = (0..records)
                    .map(|_| {
                        ts += 1;
                        Message {
                            ts,
                            payload: vec![b'x'; size],
                        }
                    })
                    .collect();
                // The writer thread never forms a batch larger than an empty segment.
                let need: usize = batch.iter().map(frame_len).sum();
                if need > segment_bytes as usize - HEADER_LEN {
                    ts -= records as i64;
                    continue;
                }
                writer.append(&batch, records % 2 == 0).unwrap();
                written += records as u64;
            }
        }
        writer.sync().unwrap();
        assert_eq!(storage.head_seq(), written);
        assert!(storage.status().segments >= 5);
        for entry in fs::read_dir(dir.path()).unwrap() {
            let path = entry.unwrap().path();
            if path.extension().is_some_and(|x| x == "seg") {
                let len = fs::metadata(&path).unwrap().len();
                assert!(len <= segment_bytes, "{} is {len} bytes", path.display());
            }
        }
        let mut reader = storage.reader(1);
        let mut next = 1;
        loop {
            let batch = reader.read(1000, 1 << 30).unwrap();
            assert_eq!(batch.skipped, 0);
            if batch.records.is_empty() {
                break;
            }
            for r in batch.records {
                assert_eq!(r.seq, next);
                next += 1;
            }
        }
        assert_eq!(next, written + 1);
    }

    #[test]
    fn a_record_larger_than_a_segment_is_written_alone() {
        let dir = tempfile::tempdir().unwrap();
        let segment_bytes = 64 << 10;
        let (storage, mut writer) = open(dir.path(), limits(64 << 20, segment_bytes));
        writer.append(&[msg(1, "a")], true).unwrap();
        writer
            .append(
                &[Message {
                    ts: 2,
                    payload: vec![b'b'; 100 << 10],
                }],
                true,
            )
            .unwrap();
        writer.append(&[msg(3, "c")], true).unwrap();
        let firsts: Vec<u64> = storage
            .index
            .lock()
            .unwrap()
            .segments
            .keys()
            .copied()
            .collect();
        assert_eq!(firsts, vec![1, 2, 3]);
        let records = storage.reader(1).read(10, 1 << 30).unwrap().records;
        assert_eq!(
            records.iter().map(|r| r.payload.len()).collect::<Vec<_>>(),
            vec![1, 100 << 10, 1]
        );
    }

    #[test]
    fn writer_thread_batches_fit_in_a_segment() {
        let dir = tempfile::tempdir().unwrap();
        let segment_bytes = 64 << 10;
        let storage = Arc::new(Storage::open(dir.path(), limits(64 << 20, segment_bytes)).unwrap());
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (tx, rx) = queue(4096, 8 << 20);
        // Queue everything first, so the writer forms the largest batches it can.
        for i in 1..=500 {
            rt.block_on(tx.send(Message {
                ts: i,
                payload: vec![b'w'; 1000],
            }))
            .unwrap();
        }
        drop(tx);
        let handle = rt.handle().clone();
        let thread = {
            let storage = storage.clone();
            std::thread::spawn(move || run_writer(storage, rx, handle))
        };
        thread.join().unwrap();
        assert_eq!(storage.head_seq(), 500);
        assert!(storage.metrics().batches.load(Relaxed) < 50);
        for entry in fs::read_dir(dir.path()).unwrap() {
            let path = entry.unwrap().path();
            if path.extension().is_some_and(|x| x == "seg") {
                let len = fs::metadata(&path).unwrap().len();
                assert!(len <= segment_bytes, "{} is {len} bytes", path.display());
            }
        }
        let page = storage.query(None, None, 0, 1000, usize::MAX).unwrap();
        assert_eq!(page.records.len(), 500);
        assert!(page.records.windows(2).all(|w| w[1].seq == w[0].seq + 1));
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
            writer.sync().unwrap();
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
        let got = storage
            .query(Some(from), None, 0, 1000, usize::MAX)
            .unwrap()
            .records;
        assert_eq!(got.first().map(|r| r.ts), Some(from));
        assert_eq!(got.len() as u64, 33 - second);
        assert!(got.windows(2).all(|w| w[1].seq == w[0].seq + 1));
    }

    #[test]
    fn unsynced_batches_become_visible_only_on_sync() {
        let dir = tempfile::tempdir().unwrap();
        let (storage, mut writer) = open(dir.path(), limits(1 << 20, 64 << 10));
        writer.append(&[msg(1, "a")], false).unwrap();
        writer.append(&[msg(2, "b")], false).unwrap();
        assert_eq!(storage.head_seq(), 0);
        assert!(
            storage
                .reader(1)
                .read(10, 1 << 20)
                .unwrap()
                .records
                .is_empty()
        );
        assert_eq!(storage.status().bytes, HEADER_LEN as u64);
        writer.sync().unwrap();
        assert_eq!(storage.head_seq(), 2);
        assert_eq!(
            storage.reader(1).read(10, 1 << 20).unwrap().records.len(),
            2
        );

        // Sequence numbers continue from the written head, not the committed one.
        writer.append(&[msg(3, "c")], false).unwrap();
        writer.append(&[msg(4, "d")], true).unwrap();
        let records = storage.reader(3).read(10, 1 << 20).unwrap().records;
        assert_eq!(
            records.iter().map(|r| (r.seq, r.ts)).collect::<Vec<_>>(),
            vec![(3, 3), (4, 4)]
        );
        assert_eq!(storage.metrics().records.load(Relaxed), 4);
    }

    #[test]
    fn writer_thread_commits_on_the_timer_and_on_exit() {
        let dir = tempfile::tempdir().unwrap();
        let l = Limits {
            max_bytes: 1 << 20,
            segment_bytes: 64 << 10,
            sync_interval: Duration::from_millis(50),
        };
        let storage = Arc::new(Storage::open(dir.path(), l).unwrap());
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (tx, rx) = queue(16, 1 << 20);
        let thread = {
            let (storage, handle) = (storage.clone(), rt.handle().clone());
            std::thread::spawn(move || run_writer(storage, rx, handle))
        };
        for i in 1..=3 {
            rt.block_on(tx.send(msg(i, "t"))).unwrap();
        }
        let deadline = Instant::now() + Duration::from_secs(5);
        while storage.head_seq() < 3 {
            assert!(Instant::now() < deadline, "timer did not commit");
            std::thread::sleep(Duration::from_millis(5));
        }
        rt.block_on(tx.send(msg(4, "u"))).unwrap();
        drop(tx);
        thread.join().unwrap();
        assert_eq!(storage.head_seq(), 4);
        assert_eq!(storage.metrics().records.load(Relaxed), 4);
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
    fn writer_thread_syncs_pending_data_once_the_interval_is_zero() {
        let dir = tempfile::tempdir().unwrap();
        let l = Limits {
            max_bytes: 1 << 20,
            segment_bytes: 64 << 10,
            sync_interval: Duration::from_secs(60),
        };
        let storage = Arc::new(Storage::open(dir.path(), l).unwrap());
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (tx, rx) = queue(16, 1 << 20);
        let thread = {
            let (storage, handle) = (storage.clone(), rt.handle().clone());
            std::thread::spawn(move || run_writer(storage, rx, handle))
        };
        // Written but not synced: the interval has not elapsed since the writer started.
        rt.block_on(tx.send(msg(1, "a"))).unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        while storage.metrics().batches.load(Relaxed) < 1 {
            assert!(Instant::now() < deadline, "batch not written");
            std::thread::sleep(Duration::from_millis(5));
        }
        assert_eq!(storage.head_seq(), 0);

        storage.set_limits(Limits {
            sync_interval: Duration::ZERO,
            ..l
        });
        let deadline = Instant::now() + Duration::from_secs(5);
        while storage.head_seq() < 1 {
            assert!(
                Instant::now() < deadline,
                "pending data not committed after the interval was set to zero"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
        drop(tx);
        thread.join().unwrap();
    }

    #[test]
    fn read_error_keeps_the_records_read_before_it() {
        let dir = tempfile::tempdir().unwrap();
        let (storage, mut writer) = open(dir.path(), limits(1 << 20, 64 << 10));
        let payload = "r".repeat(1000);
        for i in 0..200 {
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
        writer.sync().unwrap();
        let firsts: Vec<u64> = storage
            .index
            .lock()
            .unwrap()
            .segments
            .keys()
            .copied()
            .collect();
        assert!(firsts.len() >= 3);
        // A directory in place of the second segment: opening it works,
        // reading it fails with EISDIR, whoever runs the test.
        let second = storage.segment_path(firsts[1]);
        let saved = fs::read(&second).unwrap();
        fs::remove_file(&second).unwrap();
        fs::create_dir(&second).unwrap();

        let mut reader = storage.reader(1);
        let first = reader.read(1000, 1 << 30).unwrap();
        assert_eq!(first.records.len() as u64, firsts[1] - 1);
        assert_eq!(first.skipped, 0);
        let err = reader.read(1000, 1 << 30).unwrap_err();
        assert_ne!(err.kind(), io::ErrorKind::NotFound);
        assert_eq!(reader.next_seq(), firsts[1]);

        fs::remove_dir(&second).unwrap();
        fs::write(&second, &saved).unwrap();
        let mut seqs: Vec<u64> = first.records.iter().map(|r| r.seq).collect();
        loop {
            let batch = reader.read(1000, 1 << 30).unwrap();
            assert_eq!(batch.skipped, 0);
            if batch.records.is_empty() {
                break;
            }
            seqs.extend(batch.records.iter().map(|r| r.seq));
        }
        assert_eq!(seqs, (1..=200).collect::<Vec<_>>());
    }

    #[test]
    fn files_are_not_world_accessible() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let (storage, mut writer) = open(dir.path(), limits(1 << 20, 64 << 10));
        writer.append(&[msg(1, "a")], true).unwrap();
        storage.save_cursor("c", 2).unwrap();
        for path in [
            storage.segment_path(1),
            storage.cursor_path("c"),
            dir.path().join(CURSOR_DIR),
        ] {
            let mode = fs::metadata(&path).unwrap().permissions().mode();
            assert_eq!(mode & 0o027, 0, "{} has mode {mode:o}", path.display());
        }
    }

    #[test]
    fn prune_removes_unconfigured_and_temporary_cursor_files() {
        let dir = tempfile::tempdir().unwrap();
        let (storage, _) = open(dir.path(), limits(1 << 20, 64 << 10));
        storage.save_cursor("keep", 1).unwrap();
        storage.save_cursor("gone", 1).unwrap();
        let tmp = dir.path().join(CURSOR_DIR).join(".keep.tmp");
        fs::write(&tmp, b"7\n").unwrap();
        storage
            .prune_cursors(&BTreeSet::from(["keep".to_string()]))
            .unwrap();
        assert_eq!(storage.load_cursor("keep").unwrap(), Some(1));
        assert_eq!(storage.load_cursor("gone").unwrap(), None);
        assert!(!tmp.exists());
    }

    #[test]
    fn sync_interval_conversion_saturates() {
        let dir = tempfile::tempdir().unwrap();
        let l = Limits {
            max_bytes: 1 << 20,
            segment_bytes: 64 << 10,
            sync_interval: Duration::MAX,
        };
        let (storage, _) = open(dir.path(), l);
        assert_eq!(storage.sync_interval(), Duration::from_micros(u64::MAX));
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
        writer.sync().unwrap();
        let before = storage.status();
        assert!(before.segments > 2);
        storage.set_limits(limits(128 << 10, 64 << 10));
        let after = storage.status();
        assert!(after.segments < before.segments);
        assert!(after.bytes <= 128 << 10);
    }

    #[test]
    fn a_read_that_fails_once_costs_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let (storage, mut writer) = open(dir.path(), limits(64 << 20, 1 << 20));
        let payload = "t".repeat(490);
        for i in 1..=3000 {
            writer.append(&[msg(i, &payload)], i % 50 == 0).unwrap();
        }
        writer.sync().unwrap();
        let lost = Arc::new(Loss::default());
        let tail = storage.tail("c", 1, lost.clone());
        let mut reader = tail.reader(1);
        let mut got = seqs(&reader.read(32, 1 << 20).unwrap());
        // The device fails a read in the middle of the segment, then works
        // again. The records read before it are returned first.
        let bad = File::open(dir.path()).unwrap();
        let good = reader.current.as_mut().unwrap().cursor.replace_file(bad);
        if let Ok(batch) = reader.read(1000, 1 << 20) {
            got.extend(seqs(&batch));
            assert!(reader.read(1000, 1 << 20).is_err());
        }
        reader.current.as_mut().unwrap().cursor.replace_file(good);
        let (rest, skipped) = read_to_end(&mut reader);
        got.extend(rest);
        assert_eq!((got, skipped), ((1..=3000).collect(), 0));
        assert_eq!(lost.records(), 0);
        assert_eq!(storage.metrics().corrupt_segments.load(Relaxed), 0);
    }

    fn segment_starts(storage: &Storage) -> Vec<u64> {
        storage
            .index
            .lock()
            .unwrap()
            .segments
            .keys()
            .copied()
            .collect()
    }

    #[test]
    fn data_that_cannot_be_read_back_is_retried_then_skipped() {
        let dir = tempfile::tempdir().unwrap();
        let (storage, mut writer) = open(dir.path(), limits(1 << 20, 64 << 10));
        write_records(&mut writer, 150);
        let firsts = segment_starts(&storage);
        assert!(firsts.len() >= 3, "{firsts:?}");
        let lost = Arc::new(Loss::default());
        let tail = storage.tail("c", 1, lost.clone());
        let mut reader = tail.reader(1);
        reader.retry_unreadable_for(Duration::from_millis(200));
        let middle = (firsts[1] + firsts[2]) / 2;
        reader.read((middle - 1) as usize, 1 << 30).unwrap();
        assert_eq!(reader.next_seq(), middle);
        // A read at the reader's position fails with `e`.
        let fail = |reader: &mut Reader, e: i32| {
            assert!(!reader.begin());
            reader.settle(Err(io::Error::from_raw_os_error(e)))
        };
        let wait = || std::thread::sleep(Duration::from_millis(250));

        // A read that works again ends the retries.
        assert!(fail(&mut reader, libc::EIO).is_err());
        wait();
        assert_eq!(seqs(&reader.read(1, 1 << 30).unwrap()), [middle]);
        assert!(fail(&mut reader, libc::EIO).is_err());
        // Other errors can be fixed and are retried however long they last.
        wait();
        assert!(fail(&mut reader, libc::EACCES).is_err());
        assert!(fail(&mut reader, libc::EMFILE).is_err());
        assert_eq!(lost.records(), 0);
        // Data that does not read back for that long is skipped with the
        // rest of its segment, as corruption is.
        let batch = fail(&mut reader, libc::EIO).unwrap();
        assert_eq!(
            (batch.records.len(), batch.skipped),
            (0, firsts[2] - middle - 1)
        );
        assert_eq!(reader.next_seq(), firsts[2]);
        assert_eq!(lost.records(), firsts[2] - middle - 1);
        assert_eq!(storage.metrics().corrupt_segments.load(Relaxed), 1);
        let (rest, skipped) = read_to_end(&mut reader);
        assert_eq!((rest, skipped), ((firsts[2]..=150).collect(), 0));
    }

    /// On Linux a read of `/proc/self/mem` at a low offset fails with EIO,
    /// as a worn sector does.
    #[cfg(target_os = "linux")]
    #[test]
    fn a_segment_that_does_not_read_back_is_skipped_once_retried_long_enough() {
        let dir = tempfile::tempdir().unwrap();
        let (storage, mut writer) = open(dir.path(), limits(1 << 20, 64 << 10));
        write_records(&mut writer, 150);
        let firsts = segment_starts(&storage);
        let second = storage.segment_path(firsts[1]);
        fs::remove_file(&second).unwrap();
        std::os::unix::fs::symlink("/proc/self/mem", &second).unwrap();
        let lost = Arc::new(Loss::default());
        let tail = storage.tail("c", 1, lost.clone());
        let mut reader = tail.reader(1);
        let retry_for = Duration::from_millis(300);
        reader.retry_unreadable_for(retry_for);
        let first = reader.read(1000, 1 << 30).unwrap();
        assert_eq!(first.records.len() as u64, firsts[1] - 1);

        let failing = Instant::now();
        let batch = loop {
            match reader.read(1000, 1 << 30) {
                Ok(batch) => break batch,
                Err(e) => assert_eq!(e.raw_os_error(), Some(libc::EIO), "{e}"),
            }
            assert!(failing.elapsed() < Duration::from_secs(10), "never skipped");
            std::thread::sleep(Duration::from_millis(50));
        };
        assert!(failing.elapsed() >= retry_for);
        assert_eq!(
            (batch.records.len(), batch.skipped),
            (0, firsts[2] - firsts[1])
        );
        assert_eq!(lost.records(), firsts[2] - firsts[1]);
        assert_eq!(storage.metrics().corrupt_segments.load(Relaxed), 1);
        let (rest, skipped) = read_to_end(&mut reader);
        assert_eq!((rest, skipped), ((firsts[2]..=150).collect(), 0));
    }

    #[test]
    fn records_written_after_damage_in_the_active_segment_are_read() {
        let dir = tempfile::tempdir().unwrap();
        let (storage, mut writer) = open(dir.path(), limits(1 << 20, 64 << 10));
        for ts in 1..=10 {
            writer.append(&[msg(ts, "abc")], true).unwrap();
        }
        // A damaged length in record 5: a reader stops there, and so does
        // one that positions itself past it.
        let path = storage.segment_path(1);
        let mut bytes = fs::read(&path).unwrap();
        let (seq, start, _) = frames(&bytes)[4];
        assert_eq!(seq, 5);
        bytes[start + 4..start + 8].copy_from_slice(&u32::MAX.to_le_bytes());
        fs::write(&path, &bytes).unwrap();
        let lost = Arc::new(Loss::default());
        let tail = storage.tail("c", 1, lost.clone());
        let mut reader = tail.reader(1);
        assert_eq!(read_to_end(&mut reader), (vec![1, 2, 3, 4], 6));

        // What is written after the damage, in the same segment, is read.
        for ts in 11..=15 {
            writer.append(&[msg(ts, "def")], true).unwrap();
        }
        assert_eq!(storage.status().segments, 1);
        assert_eq!(read_to_end(&mut reader), ((11..=15).collect(), 0));
        assert_eq!(lost.records(), 6);
        assert_eq!(storage.metrics().corrupt_segments.load(Relaxed), 1);
    }

    #[test]
    fn a_query_returns_the_records_before_a_read_error() {
        let dir = tempfile::tempdir().unwrap();
        let (storage, mut writer) = open(dir.path(), limits(1 << 20, 64 << 10));
        for i in 0..200 {
            let message = Message {
                ts: i,
                payload: vec![b'q'; 1000],
            };
            writer.append(&[message], false).unwrap();
        }
        writer.sync().unwrap();
        let second = *storage
            .index
            .lock()
            .unwrap()
            .segments
            .keys()
            .nth(1)
            .unwrap();
        // A directory in place of the second segment fails every read.
        let path = storage.segment_path(second);
        fs::remove_file(&path).unwrap();
        fs::create_dir(&path).unwrap();
        let page = storage.query(None, None, 0, 1000, 1 << 30).unwrap();
        assert_eq!((page.records.len() as u64, page.more), (second - 1, true));
        // The next page starts at the failing position, which reports it.
        assert!(
            storage
                .query(None, None, second - 1, 1000, 1 << 30)
                .is_err()
        );
    }

    #[test]
    fn a_log_is_open_in_one_place_at_a_time() {
        let dir = tempfile::tempdir().unwrap();
        let l = limits(1 << 20, 64 << 10);
        let (storage, mut writer) = open(dir.path(), l);
        writer.append(&[msg(1, "a")], true).unwrap();
        // A batch being written: a second open must not cut it off.
        let path = storage.segment_path(1);
        let mut active = OpenOptions::new().append(true).open(&path).unwrap();
        active.write_all(&[1, 2, 3]).unwrap();
        let len = fs::metadata(&path).unwrap().len();
        let err = match Storage::open(dir.path(), l) {
            Ok(_) => panic!("opened twice"),
            Err(e) => e,
        };
        assert_eq!(err.kind(), io::ErrorKind::WouldBlock);
        let pid = format!("pid {}", std::process::id());
        assert!(err.to_string().contains(&pid), "{err}");
        assert_eq!(fs::metadata(&path).unwrap().len(), len);
        drop((storage, writer));
        let (storage, _) = open(dir.path(), l);
        assert_eq!(storage.head_seq(), 1);
    }

    /// (seq, start, end) of every frame in segment file bytes.
    pub(super) fn frames(bytes: &[u8]) -> Vec<(u64, usize, usize)> {
        let mut out = Vec::new();
        let mut pos = HEADER_LEN;
        while pos + FRAME_HEADER_LEN <= bytes.len() {
            let len = u32::from_le_bytes(bytes[pos + 4..pos + 8].try_into().unwrap()) as usize;
            let seq = u64::from_le_bytes(bytes[pos + 8..pos + 16].try_into().unwrap());
            let end = pos + FRAME_HEADER_LEN + len;
            if end > bytes.len() {
                break;
            }
            out.push((seq, pos, end));
            pos = end;
        }
        out
    }

    #[test]
    fn damage_to_committed_records_never_gives_their_numbers_out_again() {
        // Each case damages the active segment, which holds records 23 to 25,
        // and gives the sequence number that the next message must get, and
        // whether the damage counts as such.
        type Damage = fn(&mut Vec<u8>, &[(u64, usize, usize)]);
        let cases: [(&str, Damage, u64, bool); 7] = [
            (
                "header and first frame zeroed",
                |b, _| b[..48].fill(0),
                26,
                true,
            ),
            ("record 23 flipped", |b, f| b[f[0].2 - 1] ^= 1, 26, true),
            ("last record flipped", |b, f| b[f[2].2 - 1] ^= 1, 28, true),
            (
                "length of record 24 flipped",
                |b, f| b[f[1].1 + 7] ^= 0x40,
                26,
                true,
            ),
            (
                "last record torn",
                |b, f| b.truncate(f[2].2 - 10),
                25,
                false,
            ),
            ("last record zeros", |b, f| b[f[2].1..].fill(0), 25, false),
            (
                "end of the last record zeros",
                |b, f| b[f[2].2 - 30..].fill(0),
                28,
                true,
            ),
        ];
        let payload = "p".repeat(60);
        for (case, damage, next, counted) in cases {
            let dir = tempfile::tempdir().unwrap();
            let l = limits(1 << 20, 1000);
            let path = {
                let (storage, mut writer) = open(dir.path(), l);
                for i in 1..=25 {
                    writer.append(&[msg(i, &payload)], true).unwrap();
                }
                storage.segment_path(23)
            };
            let mut bytes = fs::read(&path).unwrap();
            let held = frames(&bytes);
            assert_eq!(held.iter().map(|f| f.0).collect::<Vec<_>>(), [23, 24, 25]);
            damage(&mut bytes, &held);
            fs::write(&path, &bytes).unwrap();
            for start in 0..2 {
                let (storage, mut writer) = open(dir.path(), l);
                assert_eq!(storage.head_seq(), next - 1, "{case}, start {start}");
                let corrupt = storage.metrics().corrupt_segments.load(Relaxed);
                assert_eq!(corrupt, u64::from(counted && start == 0), "{case}");
                assert!(path.exists(), "{case}");
                if start == 1 {
                    writer.append(&[msg(99, "new")], true).unwrap();
                    let page = storage.query(None, None, next - 1, 10, 1 << 20).unwrap();
                    assert_eq!(seqs_of(&page.records), [next], "{case}");
                }
            }
        }
    }

    fn seqs_of(records: &[Record]) -> Vec<u64> {
        records.iter().map(|r| r.seq).collect()
    }

    #[test]
    fn a_torn_tail_that_cannot_be_cut_off_does_not_block_startup() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let l = limits(1 << 20, 64 << 10);
        {
            let (_storage, mut writer) = open(dir.path(), l);
            writer.append(&[msg(1, "a"), msg(2, "b")], true).unwrap();
        }
        let path = dir.path().join(segment::file_name(1));
        let mut bytes = fs::read(&path).unwrap();
        bytes.extend_from_slice(&[7; 10]);
        fs::write(&path, &bytes).unwrap();
        // Read-only, so that cutting it fails (unless the tests run as root,
        // which cuts it anyway).
        fs::set_permissions(&path, fs::Permissions::from_mode(0o440)).unwrap();
        let (storage, _) = open(dir.path(), l);
        fs::set_permissions(&path, fs::Permissions::from_mode(0o640)).unwrap();
        assert_eq!(storage.head_seq(), 2);
        assert_eq!(seqs(&storage.reader(1).read(10, 1 << 20).unwrap()), [1, 2]);
    }

    #[test]
    fn zeros_from_a_block_boundary_inside_the_last_record_are_a_torn_tail() {
        let dir = tempfile::tempdir().unwrap();
        let l = limits(1 << 20, 64 << 10);
        let path = {
            let (storage, mut writer) = open(dir.path(), l);
            for i in 1..=3 {
                writer.append(&[msg(i, &"z".repeat(1000))], true).unwrap();
            }
            storage.segment_path(1)
        };
        // Record 3 takes bytes 2072 to 3096; the block from 2560 never
        // arrived.
        let mut bytes = fs::read(&path).unwrap();
        assert_eq!(bytes.len(), 3096);
        bytes[2560..].fill(0);
        fs::write(&path, &bytes).unwrap();
        let (storage, _) = open(dir.path(), l);
        assert_eq!(storage.head_seq(), 2);
        assert_eq!(storage.metrics().corrupt_segments.load(Relaxed), 0);
        assert_eq!(fs::metadata(&path).unwrap().len(), 2072);
    }

    #[test]
    fn numbering_continues_past_the_highest_cursor() {
        let dir = tempfile::tempdir().unwrap();
        let l = limits(1 << 20, 64 << 10);
        {
            let (storage, mut writer) = open(dir.path(), l);
            writer.append(&[msg(1, "a"), msg(2, "b")], true).unwrap();
            storage.save_cursor("behind", 2).unwrap();
            // A channel delivered up to 39, and those records are gone.
            storage.save_cursor("ahead", 40).unwrap();
            // Left by an interrupted save, and one no daemon could write.
            let cursors = dir.path().join(CURSOR_DIR);
            fs::write(cursors.join(".behind.tmp"), "30\n").unwrap();
            fs::write(cursors.join("forged"), format!("{}\n", u64::MAX)).unwrap();
        }
        let (storage, mut writer) = open(dir.path(), l);
        assert_eq!(storage.head_seq(), 39);
        writer.append(&[msg(3, "c")], true).unwrap();
        // A reader behind passes over the numbers whose records are gone.
        assert_eq!(read_to_end(&mut storage.reader(2)), (vec![2, 40], 37));
        drop((storage, writer));
        let (storage, _) = open(dir.path(), l);
        assert_eq!(storage.head_seq(), 40);
    }

    #[test]
    fn a_failed_eviction_makes_the_log_unhealthy_until_it_succeeds() {
        let dir = tempfile::tempdir().unwrap();
        let (storage, mut writer) = open(dir.path(), limits(4000, 1000));
        let payload = "e".repeat(60);
        for i in 1..=20 {
            writer.append(&[msg(i, &payload)], true).unwrap();
        }
        // A directory in place of the oldest segment, which removing a file
        // cannot remove.
        let oldest = storage.segment_path(storage.status().first_seq);
        fs::remove_file(&oldest).unwrap();
        fs::create_dir(&oldest).unwrap();
        fs::write(oldest.join("x"), b"").unwrap();
        for i in 21..=200 {
            writer.append(&[msg(i, &payload)], true).unwrap();
        }
        assert!(storage.status().bytes > 8000);
        let health = storage.health().unwrap_err();
        let prefix = format!("log over its size limit: removing segment {}: ", 1);
        assert!(health.starts_with(&prefix), "{health}");
        // Once the segment can go, eviction catches up.
        fs::remove_dir_all(&oldest).unwrap();
        writer.append(&[msg(201, &payload)], true).unwrap();
        assert_eq!(storage.health(), Ok(()));
        assert!(storage.status().bytes <= 4000);
    }

    #[test]
    fn eviction_removes_files_without_the_index_and_readers_find_them_evicted() {
        let dir = tempfile::tempdir().unwrap();
        let (storage, mut writer) = open(dir.path(), limits(1 << 20, 64 << 10));
        write_records(&mut writer, 300);
        let before = storage.status();
        assert!(before.segments >= 4, "{before:?}");
        let loss = Arc::new(Loss::default());
        let tail = storage.tail("t", 1, loss.clone());
        let mut reader = tail.reader(1);
        storage.max_bytes.store(128 << 10, Relaxed);
        let (mut removed, mut locked, mut read) = (0, false, None);
        storage.evict_with(|path| {
            // Status, readers and lookups go on while files are removed.
            locked |= storage.index.try_lock().is_err();
            fs::remove_file(path)?;
            removed += 1;
            if removed == 1 && !locked {
                read = Some(reader.read(10, 1 << 20));
            }
            Ok(())
        });
        assert!(!locked, "the index was locked while a file was removed");
        // A reader that finds a file gone before its entry finds the records
        // evicted, and no error.
        let batch = read.expect("a read").unwrap();
        assert!(batch.records.is_empty() && batch.skipped > 0, "{batch:?}");
        let after = storage.status();
        assert!(after.bytes <= 128 << 10, "{after:?}");
        assert_eq!(after.segments, before.segments - removed);
        // Counted once: by the reader for its segment, by eviction for the
        // rest.
        assert_eq!(loss.records(), after.first_seq - 1);
        assert_eq!(storage.health(), Ok(()));
    }

    #[test]
    fn a_segment_that_cannot_be_replaced_takes_no_batch_before_it() {
        let dir = tempfile::tempdir().unwrap();
        let l = limits(1 << 20, 64 << 10);
        let (storage, mut writer) = open(dir.path(), l);
        writer.append(&[msg(1, "a"), msg(2, "b")], true).unwrap();
        // As a failed sync of the first batch of segment 3 leaves it: in the
        // index without a committed record, and sealed. A directory stands in
        // for a file that cannot be removed.
        {
            let mut index = storage.index.lock().unwrap();
            let len = HEADER_LEN as u64;
            index.segments.insert(
                3,
                Segment {
                    base_ts: 2,
                    len,
                    damaged: false,
                },
            );
            index.bytes += len;
        }
        let stale = storage.segment_path(3);
        fs::create_dir(&stale).unwrap();
        fs::write(stale.join("x"), b"").unwrap();
        writer.active = None;
        writer.force_new = true;
        // No batch goes into segment 1, where the file would hide it.
        assert!(writer.append(&[msg(3, "c")], true).is_err());
        assert!(writer.append(&[msg(4, "d")], true).is_err());
        assert_eq!(storage.head_seq(), 2);
        fs::remove_dir_all(&stale).unwrap();
        writer.append(&[msg(5, "e")], true).unwrap();
        drop((storage, writer));
        let (storage, _) = open(dir.path(), l);
        let records = storage.reader(1).read(10, 1 << 20).unwrap().records;
        let got: Vec<_> = records
            .iter()
            .map(|r| (r.seq, r.payload.as_slice()))
            .collect();
        assert_eq!(got, [(1, &b"a"[..]), (2, b"b"), (3, b"e")]);
    }

    #[test]
    fn a_segment_that_cannot_be_stat_ed_is_kept_and_evicted_in_its_turn() {
        let dir = tempfile::tempdir().unwrap();
        // A link to itself: stat fails, as for a damaged inode.
        let broken = dir.path().join(segment::file_name(1));
        std::os::unix::fs::symlink(segment::file_name(1), &broken).unwrap();
        let active = segment_file(3, 2, &[(3, 3, "c")]);
        fs::write(dir.path().join(segment::file_name(3)), active).unwrap();
        let (storage, mut writer) = open(dir.path(), limits(200 << 10, 64 << 10));
        let status = storage.status();
        assert_eq!((status.segments, status.first_seq), (2, 1));
        assert_eq!(storage.metrics().corrupt_segments.load(Relaxed), 1);
        // Readers report the error, as for one that cannot be opened.
        assert!(storage.reader(1).read(10, 1 << 20).is_err());
        // It counts as a full segment, and is the first to go.
        write_records(&mut writer, 160);
        assert!(storage.status().first_seq > 1);
        assert!(fs::symlink_metadata(&broken).is_err());
    }
}
