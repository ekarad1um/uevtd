//! Outbound: a supervisor that keeps one task per enabled channel, each an
//! independent reader of the log with a durable cursor.

pub mod mqtt;
pub mod notice;
pub mod websocket;

use std::any::Any;
use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::time::{Duration, Instant};

use tokio::sync::watch;
use tokio::task::{JoinError, JoinHandle};
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, debug, error, info, warn};

use crate::config::{self, Channel, Config};
use crate::metrics::ErrorSlot;
use crate::storage::{Batch, Loss, Reader, Record, Storage, Tail};

const SAVE_INTERVAL: Duration = Duration::from_secs(1);
/// Payload bytes a channel holds for delivery beyond one record: the
/// messages awaiting acknowledgement.
pub const DELIVERY_BYTES: usize = 1 << 20;
const RECONNECT_BASE: Duration = Duration::from_secs(1);
const RECONNECT_MAX: Duration = Duration::from_secs(60);

pub struct ChannelMetrics {
    pub kind: &'static str,
    pub enabled: AtomicBool,
    pub running: AtomicBool,
    pub connected: AtomicBool,
    /// Next sequence number to deliver; 0 for a channel that has not run
    /// and has no cursor file.
    pub cursor: AtomicU64,
    pub delivered: AtomicU64,
    /// Microseconds from receipt to acknowledgement, summed over `delivered`.
    pub delivery_us: AtomicU64,
    /// Records passed undelivered, as refused or too large; see
    /// [`ChannelMetrics::rejected_passed`].
    pub rejected: AtomicU64,
    /// Rejections below this sequence number are counted.
    rejected_until: AtomicU64,
    pub retries: AtomicU64,
    /// Starts after the channel's task ended on its own.
    pub restarts: AtomicU64,
    /// Records that left the log before the channel read them, counted by
    /// the log when that happens.
    pub lost: Arc<Loss>,
    /// Drop notices the target acknowledged.
    pub notices: AtomicU64,
    pub last_error: ErrorSlot,
    /// While the channel shares its client id with another, why. The two
    /// take turns on the broker, so `last_error` shows it while a session is
    /// up, rather than nothing.
    conflict: Mutex<Option<String>>,
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
            delivery_us: AtomicU64::new(0),
            rejected: AtomicU64::new(0),
            rejected_until: AtomicU64::new(0),
            retries: AtomicU64::new(0),
            restarts: AtomicU64::new(0),
            lost: Arc::default(),
            notices: AtomicU64::new(0),
            last_error: ErrorSlot::default(),
            conflict: Mutex::default(),
        }
    }

    /// Clears `last_error` once a session is up, unless the channel shares
    /// its client id, which it then shows instead.
    pub fn session_up(&self) {
        match self.conflict.lock().unwrap().as_ref() {
            Some(conflict) => self.last_error.set(conflict),
            None => self.last_error.clear(),
        }
    }

    /// Records whether the channel shares its client id; true when that is
    /// new.
    fn set_conflict(&self, conflict: Option<&String>) -> bool {
        let mut slot = self.conflict.lock().unwrap();
        if slot.as_ref() == conflict {
            return false;
        }
        let old = std::mem::replace(&mut *slot, conflict.cloned());
        match conflict {
            Some(conflict) => {
                self.last_error.set(conflict);
                true
            }
            None => {
                if let Some(old) = old {
                    self.last_error.clear_if(|message| message == old);
                }
                false
            }
        }
    }

    /// Counts a rejection once the cursor has passed record `seq`, and only
    /// once. Until then a restarted channel sends the record again, and it
    /// can be refused again, or accepted. The cursor passes records in order,
    /// so what was counted is what lies below the highest one.
    pub fn rejected_passed(&self, seq: u64) {
        if self.rejected_until.fetch_max(seq + 1, Relaxed) <= seq {
            self.rejected.fetch_add(1, Relaxed);
        }
    }

    /// Counts the delivery, completed at `now`, of a record received at `ts`.
    pub fn delivered_at(&self, ts: i64, now: i64) {
        self.delivered.fetch_add(1, Relaxed);
        let waited = u64::try_from(now.saturating_sub(ts)).unwrap_or(0);
        self.delivery_us.fetch_add(waited, Relaxed);
    }
}

/// Metrics of every configured channel, shared with the API.
pub type Registry = Mutex<BTreeMap<String, Arc<ChannelMetrics>>>;

struct Running {
    config: Channel,
    token: CancellationToken,
    task: JoinHandle<()>,
    metrics: Arc<ChannelMetrics>,
    /// The highest cursor before this start. Only progress past it ends the
    /// backoff: a restart begins at the saved cursor, which may lag behind,
    /// and sending those records again is no progress.
    best: u64,
    /// Times the task ended on its own, without progress, before this start.
    failures: u32,
}

/// A channel waiting to start again: its task ended on its own, or its
/// cursor could not be read.
struct Waiting {
    config: Channel,
    failures: u32,
    best: u64,
    until: tokio::time::Instant,
    /// A task ended, so the next start counts in `restarts`.
    ended: bool,
}

/// Starts the task of a channel.
type Spawn =
    fn(&str, Channel, Feed, CursorStore, Arc<ChannelMetrics>, CancellationToken) -> JoinHandle<()>;

pub async fn run(
    cfg: watch::Receiver<Arc<Config>>,
    storage: Arc<Storage>,
    registry: Arc<Registry>,
    shutdown: CancellationToken,
) {
    supervise(cfg, storage, registry, shutdown, spawn_channel).await;
}

fn spawn_channel(
    name: &str,
    channel: Channel,
    feed: Feed,
    cursor: CursorStore,
    metrics: Arc<ChannelMetrics>,
    token: CancellationToken,
) -> JoinHandle<()> {
    let span = tracing::info_span!("channel", name = %name);
    match channel {
        Channel::WebSocket(c) => {
            tokio::spawn(websocket::run(c, feed, cursor, metrics, token).instrument(span))
        }
        Channel::Mqtt(c) => tokio::spawn(
            mqtt::run(c, name.to_string(), feed, cursor, metrics, token).instrument(span),
        ),
    }
}

/// Keeps one task per enabled channel, started and stopped as the
/// configuration changes. A task that ends on its own, which only a panic
/// should cause, is started again after a backoff.
async fn supervise(
    mut cfg: watch::Receiver<Arc<Config>>,
    storage: Arc<Storage>,
    registry: Arc<Registry>,
    shutdown: CancellationToken,
    spawn: Spawn,
) {
    let mut running: BTreeMap<String, Running> = BTreeMap::new();
    let mut waiting: BTreeMap<String, Waiting> = BTreeMap::new();
    let configured: BTreeSet<String> = cfg.borrow().outbound.channels.keys().cloned().collect();
    prune_cursors(&storage, configured).await;
    loop {
        let desired = cfg.borrow_and_update().outbound.channels.clone();
        reconcile(
            &desired,
            &mut running,
            &mut waiting,
            &storage,
            &registry,
            spawn,
        )
        .await;
        let restart = waiting.values().map(|w| w.until).min();
        tokio::select! {
            changed = cfg.changed() => if changed.is_err() { break },
            (name, outcome) = exited(&mut running) => {
                let channel = running.remove(&name).expect("present");
                waiting.insert(name.clone(), ended(&name, channel, outcome));
            }
            () = tokio::time::sleep_until(restart.unwrap_or_else(tokio::time::Instant::now)),
                if restart.is_some() => {}
            _ = shutdown.cancelled() => break,
        }
    }
    for (name, channel) in running {
        stop(&name, channel).await;
    }
    info!("outbound stopped");
}

/// The first channel whose task has ended, and how it ended; pending while
/// none has.
async fn exited(running: &mut BTreeMap<String, Running>) -> (String, Result<(), JoinError>) {
    if running.is_empty() {
        return std::future::pending().await;
    }
    let tasks = running.values_mut().map(|r| &mut r.task);
    let (outcome, index, _) = futures_util::future::select_all(tasks).await;
    let name = running.keys().nth(index).expect("present").clone();
    (name, outcome)
}

/// Reports a task that ended on its own, and schedules the next start: 1 s
/// later, doubling to 60 s while the channel ends again without progress, as
/// reconnects do. A record that makes the channel panic is retried once a
/// minute, not in a loop.
fn ended(name: &str, channel: Running, outcome: Result<(), JoinError>) -> Waiting {
    let how = match outcome {
        Ok(()) => "ended on its own".to_string(),
        Err(e) if e.is_panic() => format!("panicked: {}", panic_message(e.into_panic())),
        Err(e) => format!("failed: {e}"),
    };
    let metrics = &channel.metrics;
    let cursor = metrics.cursor.load(Relaxed);
    let failures = if cursor > channel.best {
        1
    } else {
        channel.failures + 1
    };
    let delay = backoff(failures - 1, RECONNECT_BASE, RECONNECT_MAX);
    error!(
        channel = name,
        delay_s = delay.as_secs(),
        "channel task {how}; restarting"
    );
    metrics.running.store(false, Relaxed);
    metrics.connected.store(false, Relaxed);
    metrics.last_error.set(format!("channel task {how}"));
    Waiting {
        config: channel.config,
        failures,
        best: channel.best.max(cursor),
        until: tokio::time::Instant::now() + delay,
        ended: true,
    }
}

fn panic_message(payload: Box<dyn Any + Send>) -> String {
    match payload.downcast::<String>() {
        Ok(message) => *message,
        Err(payload) => payload
            .downcast_ref::<&str>()
            .map_or("of unknown type", |m| m)
            .to_string(),
    }
}

async fn reconcile(
    desired: &BTreeMap<String, Channel>,
    running: &mut BTreeMap<String, Running>,
    waiting: &mut BTreeMap<String, Waiting>,
    storage: &Arc<Storage>,
    registry: &Registry,
    spawn: Spawn,
) {
    let stale: Vec<String> = running
        .iter()
        .filter(|(name, r)| desired.get(*name) != Some(&r.config))
        .map(|(name, _)| name.clone())
        .collect();
    for name in stale {
        let channel = running.remove(&name).expect("present");
        stop(&name, channel).await;
    }
    // A new definition ends the wait, and the channel starts below at once;
    // so do disabling and removing it, which leave it stopped.
    waiting.retain(|name, w| desired.get(name) == Some(&w.config));

    let removed: Vec<String> = {
        let mut registry = registry.lock().unwrap();
        let removed: Vec<String> = registry
            .keys()
            .filter(|k| !desired.contains_key(*k))
            .cloned()
            .collect();
        for name in &removed {
            registry.remove(name);
        }
        removed
    };
    // Off the runtime's threads, and without the registry, which status
    // and metrics take there.
    for name in removed {
        let (storage, file) = (storage.clone(), name.clone());
        match tokio::task::spawn_blocking(move || storage.remove_cursor(&file)).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => warn!(channel = %name, error = %e, "could not remove cursor"),
            Err(e) => error!(channel = %name, error = %e, "cursor removal task failed"),
        }
        info!(channel = %name, "channel removed");
    }

    // Validation refuses a change that makes two channels share a client id,
    // but one loaded at startup may, or one saved before the host was renamed.
    let conflicts = config::client_id_conflicts(desired);
    let now = tokio::time::Instant::now();
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
        if metrics.set_conflict(conflicts.get(name)) {
            error!(channel = %name, error = %conflicts[name], "client id shared with another channel");
        }
        metrics.enabled.store(channel.enabled(), Relaxed);
        if !channel.enabled() {
            // Shows where a channel disabled since startup will resume.
            if metrics.cursor.load(Relaxed) == 0 {
                if let Some(seq) = saved_cursor(storage, name).await {
                    metrics.cursor.store(seq, Relaxed);
                }
            }
            continue;
        }
        if running.contains_key(name) {
            continue;
        }
        let (failures, best, ended) = match waiting.get(name) {
            Some(w) if w.until > now => continue,
            Some(w) => (w.failures, w.best, w.ended),
            None => (0, 0, false),
        };
        waiting.remove(name);
        let start_seq = match initial_cursor(storage, name).await {
            Ok(seq) => seq,
            // The file is kept and read again after a backoff, as a task
            // that ended is started again.
            Err(e) => {
                let failures = failures + 1;
                let delay = backoff(failures - 1, RECONNECT_BASE, RECONNECT_MAX);
                error!(channel = %name, error = %e, delay_s = delay.as_secs(),
                    "cannot read the channel's cursor; starting it later");
                metrics.last_error.set(format!("cursor: {e}"));
                let until = tokio::time::Instant::now() + delay;
                let config = channel.clone();
                waiting.insert(
                    name.clone(),
                    Waiting {
                        config,
                        failures,
                        best,
                        until,
                        ended,
                    },
                );
                continue;
            }
        };
        let feed = Feed::new(storage, name, start_seq, metrics.clone());
        let cursor = CursorStore::new(storage.clone(), name.clone(), start_seq, metrics.clone());
        let token = CancellationToken::new();
        let task = spawn(
            name,
            channel.clone(),
            feed,
            cursor,
            metrics.clone(),
            token.clone(),
        );
        metrics.running.store(true, Relaxed);
        if ended {
            metrics.restarts.fetch_add(1, Relaxed);
        }
        info!(channel = %name, kind = channel.kind(), start_seq, "channel started");
        running.insert(
            name.clone(),
            Running {
                config: channel.clone(),
                token,
                task,
                metrics,
                best: best.max(start_seq),
                failures,
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

/// The persisted cursor of a channel that does not run, if it has one.
async fn saved_cursor(storage: &Arc<Storage>, name: &str) -> Option<u64> {
    let (storage, name) = (storage.clone(), name.to_string());
    tokio::task::spawn_blocking(move || storage.load_cursor(&name))
        .await
        .ok()?
        .ok()?
}

/// The persisted cursor, or the current head for a channel without one. A
/// fresh start is persisted immediately so the backlog that accumulates before
/// the first delivery survives a restart. A cursor past the head (the log was
/// repaired by hand) is reset to the head, so new records are not skipped. A
/// cursor file that holds no cursor restarts at the oldest retained record:
/// the position is unknown, and at-least-once means replaying rather than
/// skipping. A file that cannot be read is an error, and stays as it is.
async fn initial_cursor(storage: &Arc<Storage>, name: &str) -> io::Result<u64> {
    let (storage, name) = (storage.clone(), name.to_string());
    let task = tokio::task::spawn_blocking(move || {
        let status = storage.status();
        let head = status.head_seq;
        let start = match storage.load_cursor(&name) {
            Ok(Some(seq)) if seq <= head + 1 => return Ok(seq),
            Ok(Some(seq)) => {
                warn!(channel = %name, cursor = seq, head, "cursor is past the head; starting at head");
                head + 1
            }
            Ok(None) => head + 1,
            Err(e) if e.kind() == io::ErrorKind::InvalidData => {
                warn!(channel = %name, error = %e, start_seq = status.first_seq, "unparsable cursor; replaying from the oldest retained record");
                status.first_seq
            }
            Err(e) => return Err(e),
        };
        if let Err(e) = storage.save_cursor(&name, start) {
            warn!(channel = %name, error = %e, "could not persist initial cursor");
        }
        Ok(start)
    });
    task.await.unwrap_or_else(|e| Err(io::Error::other(e)))
}

/// Exponential backoff: `base * 2^attempt`, capped.
pub fn backoff(attempt: u32, base: Duration, max: Duration) -> Duration {
    base.saturating_mul(1u32 << attempt.min(16)).min(max)
}

/// Waits before a channel reconnects: 1 s after the first failure, doubling
/// to 60 s. `attempt` counts the failures since the cursor last advanced, so
/// a target that accepts every connection but takes nothing still backs
/// off. False if the channel was stopped meanwhile.
pub async fn pause(attempt: u32, token: &CancellationToken) -> bool {
    let delay = backoff(attempt, RECONNECT_BASE, RECONNECT_MAX);
    debug!(delay_s = delay.as_secs(), "reconnecting after backoff");
    tokio::select! {
        _ = tokio::time::sleep(delay) => true,
        _ = token.cancelled() => false,
    }
}

/// What a channel delivers next.
#[derive(Debug)]
pub enum Delivery {
    /// Consecutive records in sequence order.
    Records(Vec<Record>),
    /// Records `from..=to` left the log before the channel read them; they
    /// are counted in `lost` already. A channel that can sends a notice in
    /// their place.
    Lost { from: u64, to: u64 },
}

/// Cancel-safe reader of the log for a channel, which keeps the channel's
/// position for loss accounting. Blocking reads run on the blocking pool with
/// the reader handed in and out, so dropping a pending `next` never loses it.
pub struct Feed {
    tail: Tail,
    head: watch::Receiver<u64>,
    reader: Option<Reader>,
    next_seq: u64,
    pending: Option<JoinHandle<(Reader, io::Result<Batch>)>>,
    metrics: Arc<ChannelMetrics>,
    /// The error of the last read, if it failed; `last_error` holds it too.
    read_error: Option<String>,
}

impl Feed {
    fn new(storage: &Arc<Storage>, name: &str, start: u64, metrics: Arc<ChannelMetrics>) -> Self {
        let tail = storage.tail(name, start, metrics.lost.clone());
        Feed {
            reader: Some(tail.reader(start)),
            tail,
            head: storage.subscribe(),
            next_seq: start,
            pending: None,
            metrics,
            read_error: None,
        }
    }

    /// The next delivery: consecutive records, at most `max_records` and at
    /// most `max_bytes` of payload plus one record; or a range of records
    /// that left the log before the channel read them.
    pub async fn next(&mut self, max_records: usize, max_bytes: usize) -> Delivery {
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
                                if self.read_error.take().is_some() {
                                    self.metrics
                                        .last_error
                                        .clear_if(|m| m.starts_with("log read: "));
                                }
                                if !batch.records.is_empty() {
                                    return Delivery::Records(batch.records);
                                }
                                if batch.skipped > 0 {
                                    return Delivery::Lost {
                                        from: self.next_seq - batch.skipped,
                                        to: self.next_seq - 1,
                                    };
                                }
                            }
                            Err(e) => {
                                // Logged when it starts or changes, not at
                                // every retry: data that cannot be read back
                                // is retried for ten minutes.
                                let message = format!("log read: {e}");
                                if self.read_error.as_ref() != Some(&message) {
                                    warn!(error = %e, "log read failed; retrying every second");
                                } else {
                                    debug!(error = %e, "log read failed again");
                                }
                                self.metrics.last_error.set(&message);
                                self.read_error = Some(message);
                                tokio::time::sleep(Duration::from_secs(1)).await;
                            }
                        }
                    }
                    Err(e) => {
                        error!(error = %e, "log reader task failed; reopening");
                        self.reader = Some(self.tail.reader(self.next_seq));
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
            let (max_records, max_bytes) = (max_records.max(1), max_bytes.max(1));
            self.pending = Some(tokio::task::spawn_blocking(move || {
                let result = reader.read(max_records, max_bytes);
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

    /// Moves the cursor forward to `next_seq`; true if it moved.
    pub fn advance(&mut self, next_seq: u64) -> bool {
        if next_seq <= self.current {
            return false;
        }
        self.current = next_seq;
        self.metrics.cursor.store(next_seq, Relaxed);
        true
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
        assert_eq!(initial_cursor(&storage, "fresh").await.unwrap(), 1);
        assert_eq!(storage.load_cursor("fresh").unwrap(), Some(1));
        storage.save_cursor("ahead", 50).unwrap();
        assert_eq!(initial_cursor(&storage, "ahead").await.unwrap(), 1);
        assert_eq!(storage.load_cursor("ahead").unwrap(), Some(1));
    }

    #[tokio::test]
    async fn an_unparsable_cursor_replays_from_the_oldest_retained_record() {
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
        let cursors = dir.path().join("cursors");
        std::fs::write(cursors.join("bad"), b"garbage\n").unwrap();
        assert_eq!(initial_cursor(&storage, "bad").await.unwrap(), 1);
        assert_eq!(storage.load_cursor("bad").unwrap(), Some(1));
        std::fs::write(cursors.join("binary"), b"\xff\xfe\n").unwrap();
        assert_eq!(initial_cursor(&storage, "binary").await.unwrap(), 1);
    }

    /// Where `starts_at_its_cursor` started the channel; 0 before.
    static STARTED_AT: AtomicU64 = AtomicU64::new(0);

    fn starts_at_its_cursor(
        _: &str,
        _: Channel,
        feed: Feed,
        cursor: CursorStore,
        _: Arc<ChannelMetrics>,
        token: CancellationToken,
    ) -> JoinHandle<()> {
        STARTED_AT.store(cursor.current(), Relaxed);
        tokio::spawn(async move {
            let _feed = feed;
            token.cancelled().await;
        })
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_cursor_that_cannot_be_read_is_kept_and_read_again() {
        let dir = tempfile::tempdir().unwrap();
        let limits = Limits {
            max_bytes: 1 << 20,
            segment_bytes: 64 << 10,
            sync_interval: Duration::ZERO,
        };
        let storage = Arc::new(Storage::open(dir.path(), limits).unwrap());
        let mut writer = crate::storage::Writer::new(storage.clone());
        for ts in 1..=5 {
            let message = crate::storage::Message {
                ts,
                payload: b"x".to_vec(),
            };
            writer.append(&[message], true).unwrap();
        }
        // A link to itself in place of the cursor file: reading it fails, as
        // an I/O error would, whoever runs the test.
        let file = dir.path().join("cursors").join("w");
        std::os::unix::fs::symlink("w", &file).unwrap();
        let mut config = Config::default();
        let channel = Channel::WebSocket(crate::config::WebSocket::default());
        config.outbound.channels.insert("w".into(), channel);
        let (_cfg, rx) = watch::channel(Arc::new(config));
        let registry = Arc::new(Registry::default());
        let shutdown = CancellationToken::new();
        let supervisor = tokio::spawn(supervise(
            rx,
            storage,
            registry.clone(),
            shutdown.clone(),
            starts_at_its_cursor,
        ));
        async fn until(what: &str, mut done: impl FnMut() -> bool) {
            let deadline = Instant::now() + Duration::from_secs(10);
            while !done() {
                assert!(Instant::now() < deadline, "{what}");
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
        until("registered", || registry.lock().unwrap().contains_key("w")).await;
        let metrics = registry.lock().unwrap()["w"].clone();
        let error = || metrics.last_error.get().map(|e| e.message);

        // The channel does not start, rather than replay the log, and the
        // file is left alone.
        until("error", || {
            error().is_some_and(|m| m.starts_with("cursor: "))
        })
        .await;
        assert_eq!(STARTED_AT.load(Relaxed), 0);
        assert!(!metrics.running.load(Relaxed));
        assert!(file.symlink_metadata().unwrap().is_symlink());
        // Once the file reads, the channel starts where it says.
        let fixed = dir.path().join("fixed");
        std::fs::write(&fixed, b"4\n").unwrap();
        std::fs::rename(&fixed, &file).unwrap();
        until("started", || STARTED_AT.load(Relaxed) == 4).await;
        until("running", || metrics.running.load(Relaxed)).await;
        assert_eq!(metrics.restarts.load(Relaxed), 0);
        shutdown.cancel();
        supervisor.await.unwrap();
    }

    #[tokio::test]
    async fn feed_delivers_a_loss_in_place_of_the_records_it_covers() {
        let dir = tempfile::tempdir().unwrap();
        let limits = Limits {
            max_bytes: 128 << 10,
            segment_bytes: 64 << 10,
            sync_interval: Duration::ZERO,
        };
        let storage = Arc::new(Storage::open(dir.path(), limits).unwrap());
        let metrics = Arc::new(ChannelMetrics::new("mqtt"));
        let mut feed = Feed::new(&storage, "c", 1, metrics.clone());
        let mut writer = crate::storage::Writer::new(storage.clone());
        let mut write = |records: i64| {
            for ts in 0..records {
                let message = crate::storage::Message {
                    ts,
                    payload: vec![b'f'; 1000],
                };
                writer.append(&[message], true).unwrap();
            }
        };
        write(300);
        let first = storage.status().first_seq;
        assert!(first > 1);
        // Counted when evicted, before the channel reads.
        assert_eq!(metrics.lost.records(), first - 1);
        let Delivery::Lost { from, to } = feed.next(10, 1 << 20).await else {
            panic!("the loss comes first");
        };
        assert_eq!((from, to), (1, first - 1));
        let Delivery::Records(records) = feed.next(10, 1 << 20).await else {
            panic!("then the records after it");
        };
        assert_eq!(records[0].seq, first);
        assert_eq!(metrics.lost.records(), first - 1);

        // Restarted before its notice was acknowledged, the channel resumes
        // at the same cursor: the notice is due again, the count is not.
        drop(feed);
        let mut feed = Feed::new(&storage, "c", 1, metrics.clone());
        assert_eq!(metrics.lost.records(), first - 1);
        let Delivery::Lost { from, to } = feed.next(10, 1 << 20).await else {
            panic!("the loss again");
        };
        assert_eq!((from, to), (1, first - 1));

        // A stopped channel's position is not accounted any more.
        drop(feed);
        write(300);
        assert_eq!(metrics.lost.records(), first - 1);
    }

    #[test]
    fn delivery_time_is_summed_and_never_negative() {
        let metrics = ChannelMetrics::new("mqtt");
        metrics.delivered_at(1_000, 3_500);
        // A clock that stepped back since the record arrived.
        metrics.delivered_at(5_000, 4_000);
        let delivered = metrics.delivered.load(Relaxed);
        assert_eq!((delivered, metrics.delivery_us.load(Relaxed)), (2, 2_500));
    }

    #[tokio::test]
    async fn a_disabled_channel_shows_where_it_will_resume() {
        let dir = tempfile::tempdir().unwrap();
        let limits = Limits {
            max_bytes: 1 << 20,
            segment_bytes: 64 << 10,
            sync_interval: Duration::ZERO,
        };
        let storage = Arc::new(Storage::open(dir.path(), limits).unwrap());
        storage.save_cursor("saved", 7).unwrap();
        let mut config = Config::default();
        for name in ["saved", "new"] {
            let channel = crate::config::WebSocket {
                enabled: false,
                ..Default::default()
            };
            let channel = Channel::WebSocket(channel);
            config.outbound.channels.insert(name.into(), channel);
        }
        let (_cfg, rx) = watch::channel(Arc::new(config));
        let registry = Arc::new(Registry::default());
        let shutdown = CancellationToken::new();
        let never: Spawn = |_, _, _, _, _, _| unreachable!("a disabled channel is never started");
        let supervisor = tokio::spawn(supervise(
            rx,
            storage,
            registry.clone(),
            shutdown.clone(),
            never,
        ));
        let cursors = || {
            let registry = registry.lock().unwrap();
            let cursor = |name: &str| registry.get(name).map(|m| m.cursor.load(Relaxed));
            (cursor("saved"), cursor("new"))
        };
        let deadline = Instant::now() + Duration::from_secs(10);
        while cursors() != (Some(7), Some(0)) {
            assert!(Instant::now() < deadline, "{:?}", cursors());
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        shutdown.cancel();
        supervisor.await.unwrap();
    }

    #[test]
    fn backoff_doubles_and_caps() {
        let base = Duration::from_millis(100);
        let max = Duration::from_secs(30);
        assert_eq!(backoff(0, base, max), base);
        assert_eq!(backoff(3, base, max), Duration::from_millis(800));
        assert_eq!(backoff(20, base, max), max);
    }

    #[tokio::test]
    async fn an_ended_task_waits_longer_until_the_channel_makes_progress() {
        let metrics = Arc::new(ChannelMetrics::new("websocket"));
        let channel = |best, failures| Running {
            config: Channel::WebSocket(Default::default()),
            token: CancellationToken::new(),
            task: tokio::spawn(async {}),
            metrics: metrics.clone(),
            best,
            failures,
        };
        let wait = |w: &Waiting| w.until - tokio::time::Instant::now();
        metrics.cursor.store(5, Relaxed);
        metrics.running.store(true, Relaxed);

        // Each end without progress past the highest cursor so far doubles
        // the wait.
        let w = ended("w", channel(5, 0), Ok(()));
        assert_eq!((w.failures, w.best), (1, 5));
        assert!(wait(&w) > Duration::from_millis(900), "{:?}", wait(&w));
        assert!(!metrics.running.load(Relaxed));
        let message = || metrics.last_error.get().unwrap().message;
        assert_eq!(message(), "channel task ended on its own");
        let w = ended("w", channel(5, 3), Ok(()));
        assert_eq!(w.failures, 4);
        assert!(wait(&w) > Duration::from_millis(7900), "{:?}", wait(&w));
        // Progress resets it and raises the mark.
        let w = ended("w", channel(4, 3), Ok(()));
        assert_eq!((w.failures, w.best), (1, 5));

        let panicked = tokio::spawn(async { panic!("boom") }).await.unwrap_err();
        ended("w", channel(5, 0), Err(panicked));
        assert_eq!(message(), "channel task panicked: boom");
        let n = 7;
        let panicked = tokio::spawn(async move { panic!("boom {n}") })
            .await
            .unwrap_err();
        ended("w", channel(5, 0), Err(panicked));
        assert_eq!(message(), "channel task panicked: boom 7");
    }

    /// Starts in `a_channel_whose_task_ends_is_started_again`.
    static STARTS: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);

    /// A channel task that delivers three records without saving its cursor
    /// and panics, on its first two starts; then it runs until stopped.
    fn panics_twice(
        _: &str,
        _: Channel,
        feed: Feed,
        mut cursor: CursorStore,
        _: Arc<ChannelMetrics>,
        token: CancellationToken,
    ) -> JoinHandle<()> {
        let start = STARTS.fetch_add(1, Relaxed);
        tokio::spawn(async move {
            let _feed = feed;
            if start < 2 {
                cursor.advance(cursor.current() + 3);
                panic!("start {start}");
            }
            token.cancelled().await;
        })
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_channel_whose_task_ends_is_started_again() {
        async fn until(what: &str, mut done: impl FnMut() -> bool) -> Instant {
            let deadline = Instant::now() + Duration::from_secs(10);
            while !done() {
                assert!(Instant::now() < deadline, "{what}");
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Instant::now()
        }
        let dir = tempfile::tempdir().unwrap();
        let limits = Limits {
            max_bytes: 1 << 20,
            segment_bytes: 64 << 10,
            sync_interval: Duration::ZERO,
        };
        let storage = Arc::new(Storage::open(dir.path(), limits).unwrap());
        let websocket = |url: &str| {
            let mut config = Config::default();
            let channel = crate::config::WebSocket {
                url: url.into(),
                ..Default::default()
            };
            let channel = Channel::WebSocket(channel);
            config.outbound.channels.insert("w".into(), channel);
            Arc::new(config)
        };
        let (cfg, rx) = watch::channel(websocket("ws://h/a"));
        let registry = Arc::new(Registry::default());
        let shutdown = CancellationToken::new();
        let supervisor = tokio::spawn(supervise(
            rx,
            storage,
            registry.clone(),
            shutdown.clone(),
            panics_twice,
        ));
        until("registered", || registry.lock().unwrap().contains_key("w")).await;
        let metrics = registry.lock().unwrap()["w"].clone();
        let error = || metrics.last_error.get().map(|e| e.message);

        // The end is reported at once, and the channel starts again after
        // the backoff.
        let first = until("first end", || error().is_some()).await;
        assert_eq!(error().unwrap(), "channel task panicked: start 0");
        assert!(!metrics.running.load(Relaxed));
        let second = until("second start", || metrics.restarts.load(Relaxed) == 1).await;
        assert!(
            second - first >= Duration::from_millis(900),
            "{:?}",
            second - first
        );
        assert_eq!(STARTS.load(Relaxed), 2);

        // It starts at the saved cursor, sends the same three records again,
        // which is no progress, and ends again: now it waits 2 s. A new
        // definition starts it at once.
        until("second end", || error().unwrap().ends_with("start 1")).await;
        tokio::time::sleep(Duration::from_millis(1300)).await;
        assert_eq!(STARTS.load(Relaxed), 2, "started again within 2 s");
        let changed = Instant::now();
        cfg.send_replace(websocket("ws://h/b"));
        let third = until("third start", || STARTS.load(Relaxed) == 3).await;
        assert!(
            third - changed < Duration::from_millis(900),
            "{:?}",
            third - changed
        );
        until("running", || metrics.running.load(Relaxed)).await;
        assert_eq!(metrics.restarts.load(Relaxed), 1);

        shutdown.cancel();
        supervisor.await.unwrap();
        assert!(!metrics.running.load(Relaxed));
    }
}
