//! Outbound: a supervisor that keeps one task per enabled channel, each an
//! independent reader of the log with a durable cursor (ARCHITECTURE.md §6).

pub mod http;
pub mod mqtt;

use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::time::{Duration, Instant};

use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, error, info, warn};

use crate::config::{Channel, Config};
use crate::metrics::ErrorSlot;
use crate::storage::{Batch, Reader, Record, Storage};

const SAVE_INTERVAL: Duration = Duration::from_secs(1);
const MAX_BATCH_BYTES: usize = 4 << 20;

pub struct ChannelMetrics {
    pub kind: &'static str,
    pub enabled: AtomicBool,
    pub running: AtomicBool,
    pub connected: AtomicBool,
    /// Next sequence number to deliver.
    pub cursor: AtomicU64,
    pub delivered: AtomicU64,
    pub rejected: AtomicU64,
    pub retries: AtomicU64,
    pub lost: AtomicU64,
    pub last_error: ErrorSlot,
}

impl ChannelMetrics {
    fn new(kind: &'static str) -> Self {
        Self {
            kind,
            enabled: AtomicBool::new(false),
            running: AtomicBool::new(false),
            connected: AtomicBool::new(false),
            cursor: AtomicU64::new(0),
            delivered: AtomicU64::new(0),
            rejected: AtomicU64::new(0),
            retries: AtomicU64::new(0),
            lost: AtomicU64::new(0),
            last_error: ErrorSlot::default(),
        }
    }
}

/// Metrics of every configured channel, shared with the API.
pub type Registry = Mutex<BTreeMap<String, Arc<ChannelMetrics>>>;

struct Running {
    config: Channel,
    token: CancellationToken,
    task: JoinHandle<()>,
    metrics: Arc<ChannelMetrics>,
}

pub async fn run(
    mut cfg: watch::Receiver<Arc<Config>>,
    storage: Arc<Storage>,
    registry: Arc<Registry>,
    shutdown: CancellationToken,
) {
    let mut running: BTreeMap<String, Running> = BTreeMap::new();
    let configured: BTreeSet<String> = cfg.borrow().outbound.channels.keys().cloned().collect();
    prune_cursors(&storage, configured).await;
    loop {
        let desired = cfg.borrow_and_update().outbound.channels.clone();
        reconcile(&desired, &mut running, &storage, &registry).await;
        tokio::select! {
            changed = cfg.changed() => if changed.is_err() { break },
            _ = shutdown.cancelled() => break,
        }
    }
    for (name, channel) in running {
        stop(&name, channel).await;
    }
    info!("outbound stopped");
}

async fn reconcile(
    desired: &BTreeMap<String, Channel>,
    running: &mut BTreeMap<String, Running>,
    storage: &Arc<Storage>,
    registry: &Registry,
) {
    let stale: Vec<String> = running
        .iter()
        .filter(|(name, r)| r.task.is_finished() || desired.get(*name) != Some(&r.config))
        .map(|(name, _)| name.clone())
        .collect();
    for name in stale {
        let channel = running.remove(&name).expect("present");
        if channel.task.is_finished() {
            error!(channel = %name, "channel task ended on its own");
        }
        stop(&name, channel).await;
    }

    {
        let mut registry = registry.lock().unwrap();
        let removed: Vec<String> = registry
            .keys()
            .filter(|k| !desired.contains_key(*k))
            .cloned()
            .collect();
        for name in removed {
            registry.remove(&name);
            if let Err(e) = storage.remove_cursor(&name) {
                warn!(channel = %name, error = %e, "could not remove cursor");
            }
            info!(channel = %name, "channel removed");
        }
    }

    for (name, channel) in desired {
        let metrics = {
            let mut registry = registry.lock().unwrap();
            let entry = registry
                .entry(name.clone())
                .or_insert_with(|| Arc::new(ChannelMetrics::new(channel.kind())));
            if entry.kind != channel.kind() {
                *entry = Arc::new(ChannelMetrics::new(channel.kind()));
            }
            entry.clone()
        };
        metrics.enabled.store(channel.enabled(), Relaxed);
        if !channel.enabled() || running.contains_key(name) {
            continue;
        }
        let start = initial_cursor(storage, name).await;
        let feed = Feed::new(storage.clone(), start, metrics.clone());
        let cursor = CursorStore::new(storage.clone(), name.clone(), start, metrics.clone());
        let token = CancellationToken::new();
        let span = tracing::info_span!("channel", name = %name);
        let task = match channel.clone() {
            Channel::Http(c) => tokio::spawn(
                http::run(c, feed, cursor, metrics.clone(), token.clone()).instrument(span),
            ),
            Channel::Mqtt(c) => tokio::spawn(
                mqtt::run(c, feed, cursor, metrics.clone(), token.clone()).instrument(span),
            ),
        };
        metrics.running.store(true, Relaxed);
        info!(channel = %name, kind = channel.kind(), start_seq = start, "channel started");
        running.insert(
            name.clone(),
            Running {
                config: channel.clone(),
                token,
                task,
                metrics,
            },
        );
    }
}

async fn stop(name: &str, channel: Running) {
    channel.token.cancel();
    if let Err(e) = channel.task.await {
        error!(channel = name, error = %e, "channel task failed");
    }
    channel.metrics.running.store(false, Relaxed);
    channel.metrics.connected.store(false, Relaxed);
    info!(channel = name, "channel stopped");
}

/// Cursor files of channels that are no longer configured are removed at
/// startup, as they are when a channel is removed while running.
async fn prune_cursors(storage: &Arc<Storage>, keep: BTreeSet<String>) {
    let storage = storage.clone();
    match tokio::task::spawn_blocking(move || storage.prune_cursors(&keep)).await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => warn!(error = %e, "could not prune cursor files"),
        Err(e) => error!(error = %e, "cursor prune task failed"),
    }
}

/// The persisted cursor, or the current head for a channel without one. A
/// fresh start is persisted immediately so the backlog that accumulates before
/// the first delivery survives a restart. A cursor past the head (the log was
/// repaired by hand) is reset to the head, so new records are not skipped. An
/// unreadable cursor restarts at the oldest retained record: the position is
/// unknown, and at-least-once means replaying rather than skipping.
async fn initial_cursor(storage: &Arc<Storage>, name: &str) -> u64 {
    let fallback = storage.head_seq() + 1;
    let (storage, name) = (storage.clone(), name.to_string());
    let task = tokio::task::spawn_blocking(move || {
        let status = storage.status();
        let head = status.head_seq;
        let start = match storage.load_cursor(&name) {
            Ok(Some(seq)) if seq <= head + 1 => return seq,
            Ok(Some(seq)) => {
                warn!(channel = %name, cursor = seq, head, "cursor is past the head; starting at head");
                head + 1
            }
            Ok(None) => head + 1,
            Err(e) => {
                warn!(channel = %name, error = %e, start_seq = status.first_seq, "unreadable cursor; replaying from the oldest retained record");
                status.first_seq
            }
        };
        if let Err(e) = storage.save_cursor(&name, start) {
            warn!(channel = %name, error = %e, "could not persist initial cursor");
        }
        start
    });
    task.await.unwrap_or_else(|e| {
        error!(error = %e, "cursor task failed");
        fallback
    })
}

/// Exponential backoff: `base * 2^attempt`, capped.
pub fn backoff(attempt: u32, base: Duration, max: Duration) -> Duration {
    base.saturating_mul(1u32 << attempt.min(16)).min(max)
}

/// Cancel-safe batch reader for a channel. Blocking reads run on the
/// blocking pool with the reader handed in and out, so dropping a pending
/// `next` never loses it.
pub struct Feed {
    storage: Arc<Storage>,
    head: watch::Receiver<u64>,
    reader: Option<Reader>,
    next_seq: u64,
    pending: Option<JoinHandle<(Reader, io::Result<Batch>)>>,
    metrics: Arc<ChannelMetrics>,
    /// The last read failed and `last_error` holds its message.
    read_failed: bool,
}

impl Feed {
    fn new(storage: Arc<Storage>, start: u64, metrics: Arc<ChannelMetrics>) -> Self {
        let head = storage.subscribe();
        let reader = storage.reader(start);
        Feed {
            storage,
            head,
            reader: Some(reader),
            next_seq: start,
            pending: None,
            metrics,
            read_failed: false,
        }
    }

    /// Next non-empty batch of records in sequence order, at most `max`.
    pub async fn next(&mut self, max: usize) -> Vec<Record> {
        loop {
            if let Some(task) = self.pending.as_mut() {
                let outcome = task.await;
                self.pending = None;
                match outcome {
                    Ok((reader, result)) => {
                        self.next_seq = reader.next_seq();
                        self.reader = Some(reader);
                        match result {
                            Ok(batch) => {
                                if self.read_failed {
                                    self.read_failed = false;
                                    self.metrics
                                        .last_error
                                        .clear_if(|m| m.starts_with("log read: "));
                                }
                                if batch.skipped > 0 {
                                    self.metrics.lost.fetch_add(batch.skipped, Relaxed);
                                    warn!(
                                        skipped = batch.skipped,
                                        "records were evicted before delivery"
                                    );
                                }
                                if !batch.records.is_empty() {
                                    return batch.records;
                                }
                            }
                            Err(e) => {
                                warn!(error = %e, "log read failed; retrying");
                                self.metrics.last_error.set(format!("log read: {e}"));
                                self.read_failed = true;
                                tokio::time::sleep(Duration::from_secs(1)).await;
                            }
                        }
                    }
                    Err(e) => {
                        error!(error = %e, "log reader task failed; reopening");
                        self.reader = Some(self.storage.reader(self.next_seq));
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
                continue;
            }
            let next = self.next_seq;
            let _ = self.head.wait_for(|head| *head >= next).await;
            let mut reader = self
                .reader
                .take()
                .expect("reader is present when no read is pending");
            let max = max.max(1);
            self.pending = Some(tokio::task::spawn_blocking(move || {
                let result = reader.read(max, MAX_BATCH_BYTES);
                (reader, result)
            }));
        }
    }
}

/// A channel's cursor with debounced, atomic persistence.
pub struct CursorStore {
    storage: Arc<Storage>,
    name: String,
    saved: u64,
    current: u64,
    last_save: Instant,
    metrics: Arc<ChannelMetrics>,
}

impl CursorStore {
    fn new(storage: Arc<Storage>, name: String, start: u64, metrics: Arc<ChannelMetrics>) -> Self {
        metrics.cursor.store(start, Relaxed);
        Self {
            storage,
            name,
            saved: start,
            current: start,
            last_save: Instant::now(),
            metrics,
        }
    }

    pub fn current(&self) -> u64 {
        self.current
    }

    pub fn advance(&mut self, next_seq: u64) {
        if next_seq > self.current {
            self.current = next_seq;
            self.metrics.cursor.store(next_seq, Relaxed);
        }
    }

    pub fn dirty(&self) -> bool {
        self.current != self.saved
    }

    /// Persists the cursor if it moved; unforced calls are rate-limited.
    pub async fn flush(&mut self, force: bool) {
        if !self.dirty() || (!force && self.last_save.elapsed() < SAVE_INTERVAL) {
            return;
        }
        let (storage, name, seq) = (self.storage.clone(), self.name.clone(), self.current);
        match tokio::task::spawn_blocking(move || storage.save_cursor(&name, seq)).await {
            Ok(Ok(())) => {
                self.saved = seq;
                self.last_save = Instant::now();
            }
            Ok(Err(e)) => {
                warn!(error = %e, "could not persist cursor");
                self.metrics.last_error.set(format!("cursor: {e}"));
            }
            Err(e) => error!(error = %e, "cursor task failed"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::Limits;

    #[tokio::test]
    async fn initial_cursor_is_persisted_and_never_past_the_head() {
        let dir = tempfile::tempdir().unwrap();
        let limits = Limits {
            max_bytes: 1 << 20,
            segment_bytes: 64 << 10,
            sync_interval: Duration::ZERO,
        };
        let storage = Arc::new(Storage::open(dir.path(), limits).unwrap());
        assert_eq!(initial_cursor(&storage, "fresh").await, 1);
        assert_eq!(storage.load_cursor("fresh").unwrap(), Some(1));
        storage.save_cursor("ahead", 50).unwrap();
        assert_eq!(initial_cursor(&storage, "ahead").await, 1);
        assert_eq!(storage.load_cursor("ahead").unwrap(), Some(1));
    }

    #[tokio::test]
    async fn unreadable_cursor_replays_from_the_oldest_retained_record() {
        let dir = tempfile::tempdir().unwrap();
        let limits = Limits {
            max_bytes: 1 << 20,
            segment_bytes: 64 << 10,
            sync_interval: Duration::ZERO,
        };
        let storage = Arc::new(Storage::open(dir.path(), limits).unwrap());
        let mut writer = crate::storage::Writer::new(storage.clone());
        for ts in 1..=5 {
            writer
                .append(
                    &[crate::storage::Message {
                        ts,
                        payload: b"x".to_vec(),
                    }],
                    true,
                )
                .unwrap();
        }
        std::fs::write(dir.path().join("cursors").join("bad"), b"garbage\n").unwrap();
        assert_eq!(initial_cursor(&storage, "bad").await, 1);
        assert_eq!(storage.load_cursor("bad").unwrap(), Some(1));
    }

    #[test]
    fn backoff_doubles_and_caps() {
        let base = Duration::from_millis(100);
        let max = Duration::from_secs(30);
        assert_eq!(backoff(0, base, max), base);
        assert_eq!(backoff(3, base, max), Duration::from_millis(800));
        assert_eq!(backoff(20, base, max), max);
    }
}
