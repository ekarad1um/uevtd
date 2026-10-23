//! HTTP API over a Unix domain socket: configuration, time-range queries,
//! status, metrics and health (ARCHITECTURE.md §8).

use std::collections::BTreeMap;
use std::fmt::{self, Display};
use std::io;
use std::pin::Pin;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::{Arc, Mutex};
use std::task::{Context as TaskContext, Poll};
use std::time::{Duration, Instant};

use anyhow::Context;
use axum::body::Bytes;
use axum::extract::rejection::BytesRejection;
use axum::extract::{DefaultBodyLimit, Query, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use hyper::server::conn::http1;
use hyper_util::rt::{TokioIo, TokioTimer};
use hyper_util::service::TowerToHyperService;
use serde::Serialize;
use serde_json::{Value, json};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, watch};
use tokio::task::{JoinError, JoinHandle, JoinSet};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::config::{self, Config};
use crate::metrics::{ErrorSlot, Exposition};
use crate::outbound::{ChannelMetrics, Registry};
use crate::storage::{self, Mark, Storage};
use crate::view::{self, RecordView};
use crate::{inbound, uds};

const DEFAULT_LIMIT: usize = 100;
const MAX_LIMIT: usize = 10_000;
/// Payload bytes per page of `/v1/messages`, beyond its first record.
const PAGE_BYTES: usize = 1 << 20;
/// Largest request body; a configuration document is a few KiB.
const BODY_BYTES: usize = 256 << 10;
/// Queries read and serialize their page one at a time.
pub const CONCURRENT_QUERIES: usize = 1;
/// How often [`Times`] is brought up to date.
const TIMES_INTERVAL: Duration = Duration::from_secs(1);
/// How the API serves its connections (§8).
const SERVE: Serve = Serve {
    connections: 16,
    idle: Duration::from_secs(10),
    stall: Duration::from_secs(60),
};
/// The warning that the connections reached their limit repeats at most this
/// often.
const LIMIT_WARN_INTERVAL: Duration = Duration::from_secs(60);

pub struct AppState {
    pub config: Arc<config::Store>,
    pub boot: Arc<Config>,
    pub storage: Arc<Storage>,
    pub queue: Arc<storage::Depth>,
    pub inbound: Arc<inbound::Metrics>,
    pub channels: Arc<Registry>,
    pub started: Instant,
    pub last_error: ErrorSlot,
    /// Holds [`CONCURRENT_QUERIES`] permits. A query's permit stays with its
    /// read, and then with its response until that is written.
    pub queries: Arc<Semaphore>,
    pub times: Mutex<Times>,
}

/// Receipt times that status and metrics take from the log: of the oldest
/// retained record, and of the oldest record waiting for each channel. They
/// are looked up in the background, so that no request waits for the disk.
#[derive(Clone, Default)]
pub struct Times {
    first: Lookup,
    waiting: BTreeMap<String, Lookup>,
}

/// The receipt time of record `seq`, and where its lookup left off.
#[derive(Clone, Copy, Default)]
struct Lookup {
    seq: u64,
    ts: Option<i64>,
    mark: Mark,
}

impl Lookup {
    /// Looks up `seq`, unless this holds it already.
    fn update(&mut self, storage: &Storage, seq: u64) {
        if self.seq == seq && self.ts.is_some() {
            return;
        }
        self.seq = seq;
        self.ts = storage.timestamp(seq, &mut self.mark).unwrap_or_else(|e| {
            debug!(seq, error = %e, "receipt time not found");
            None
        });
    }
}

type Shared = Arc<AppState>;

pub async fn run(
    mut cfg: watch::Receiver<Arc<Config>>,
    state: Shared,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    let app = router(state.clone());
    let mut current = cfg.borrow_and_update().api.clone();
    let listener = uds::bind(&current.path, current.mode.0)
        .await
        .with_context(|| format!("binding api socket {}", current.path.display()))?;
    info!(path = %current.path.display(), "api listening");
    let mut server = Server::spawn(listener, app.clone(), SERVE);
    let times = tokio::spawn(keep_times(state.clone()));

    loop {
        tokio::select! {
            changed = cfg.changed() => {
                if changed.is_err() {
                    break;
                }
                let next = cfg.borrow_and_update().api.clone();
                if next.path != current.path {
                    match uds::bind(&next.path, next.mode.0).await {
                        Ok(fresh) => {
                            let old = std::mem::replace(&mut server, Server::spawn(fresh, app.clone(), SERVE));
                            old.stop().await;
                            uds::remove(&current.path);
                            info!(path = %next.path.display(), "api rebound");
                            current = next.clone();
                        }
                        Err(e) => {
                            error!(path = %next.path.display(), error = %e, "rebind failed; keeping current socket");
                            state.last_error.set(format!("bind {}: {e}", next.path.display()));
                        }
                    }
                } else if next.mode != current.mode {
                    match uds::set_mode(&next.path, next.mode.0) {
                        Ok(()) => current = next.clone(),
                        Err(e) => {
                            error!(error = %e, "could not change socket mode");
                            state.last_error.set(format!("chmod {}: {e}", next.path.display()));
                        }
                    }
                }
                // What failed stays to be retried by the next change; once
                // what is in effect is what is configured, nothing has.
                if current == next {
                    state.last_error.clear();
                }
            }
            _ = shutdown.cancelled() => break,
        }
    }
    times.abort();
    server.stop().await;
    uds::remove(&current.path);
    info!("api stopped");
    Ok(())
}

/// Keeps [`Times`] up to date.
async fn keep_times(state: Shared) {
    let mut tick = tokio::time::interval(TIMES_INTERVAL);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tick.tick().await;
        let status = state.storage.status();
        let cursors: BTreeMap<String, u64> = state
            .channels
            .lock()
            .unwrap()
            .iter()
            .map(|(name, m)| (name.clone(), m.cursor.load(Relaxed)))
            .collect();
        let mut times = state.times.lock().unwrap().clone();
        let storage = state.storage.clone();
        let looked_up = tokio::task::spawn_blocking(move || {
            times.refresh(&storage, &status, &cursors);
            times
        });
        match looked_up.await {
            Ok(times) => *state.times.lock().unwrap() = times,
            Err(e) => error!(error = %e, "receipt time lookup failed"),
        }
    }
}

/// Bounds on the API's connections, which would otherwise use up the file
/// descriptors that the log and inbound's connection need.
#[derive(Clone, Copy)]
struct Serve {
    /// Open at once; more wait to be accepted, holding no descriptor.
    connections: usize,
    /// For a request's head, so that an idle connection is closed after it.
    idle: Duration,
    /// For a read or write that waits, with no byte passing either way.
    stall: Duration,
}

struct Server {
    token: CancellationToken,
    task: JoinHandle<()>,
}

impl Server {
    fn spawn(listener: UnixListener, app: Router, limits: Serve) -> Server {
        let token = CancellationToken::new();
        let task = tokio::spawn(serve(listener, app, limits, token.clone()));
        Server { token, task }
    }

    /// Stops accepting, lets the requests in progress finish for up to 5 s,
    /// and closes every connection.
    async fn stop(self) {
        self.token.cancel();
        let mut task = self.task;
        match tokio::time::timeout(Duration::from_secs(5), &mut task).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => error!(error = %e, "api server task failed"),
            Err(_) => {
                warn!("api server did not stop in time; aborting");
                task.abort();
            }
        }
    }
}

/// Accepts connections and serves each on a task of its own, at most
/// `limits.connections` at once, until `token` is cancelled; then waits for
/// them to finish.
async fn serve(listener: UnixListener, app: Router, limits: Serve, token: CancellationToken) {
    let slots = Arc::new(Semaphore::new(limits.connections));
    let mut connections = JoinSet::new();
    let mut warned: Option<Instant> = None;
    'serve: loop {
        // A slot first: a connection past the limit waits in the listen
        // queue, where it holds no descriptor of the daemon's.
        let slot = match slots.clone().try_acquire_owned() {
            Ok(slot) => slot,
            Err(_) => {
                if warned.is_none_or(|t| t.elapsed() >= LIMIT_WARN_INTERVAL) {
                    warn!(
                        max = limits.connections,
                        "api connection limit reached; new connections wait"
                    );
                    warned = Some(Instant::now());
                }
                tokio::select! {
                    slot = slots.clone().acquire_owned() => slot.expect("never closed"),
                    Some(done) = connections.join_next() => {
                        ended(done);
                        continue;
                    }
                    _ = token.cancelled() => break,
                }
            }
        };
        tokio::select! {
            accepted = listener.accept() => match accepted {
                Ok((stream, _)) => {
                    connections.spawn(connection(stream, app.clone(), limits, token.clone(), slot));
                }
                // The client's own error; another, such as running out of
                // descriptors, may last, and is waited out.
                Err(e) if matches!(
                    e.kind(),
                    io::ErrorKind::ConnectionAborted | io::ErrorKind::ConnectionReset
                ) => debug!(error = %e, "api accept failed"),
                Err(e) => {
                    warn!(error = %e, "api accept failed");
                    tokio::select! {
                        _ = tokio::time::sleep(Duration::from_secs(1)) => {}
                        _ = token.cancelled() => break 'serve,
                    }
                }
            },
            Some(done) = connections.join_next() => ended(done),
            _ = token.cancelled() => break,
        }
    }
    drop(listener);
    while let Some(done) = connections.join_next().await {
        ended(done);
    }
}

fn ended(connection: Result<(), JoinError>) {
    if let Err(e) = connection {
        error!(error = %e, "api connection task failed");
    }
}

/// Serves one connection. It is closed once it has waited `limits.idle` for
/// a request, or `limits.stall` for a read or write; or, after the request
/// in progress, once `token` is cancelled.
async fn connection(
    stream: UnixStream,
    app: Router,
    limits: Serve,
    token: CancellationToken,
    _slot: OwnedSemaphorePermit,
) {
    let io = TokioIo::new(Watchdog::new(stream, limits.stall));
    let mut builder = http1::Builder::new();
    builder
        .timer(TokioTimer::new())
        // Runs from the end of each response too, while the connection idles.
        .header_read_timeout(limits.idle)
        // Bytes of a response are queued as they are, not copied: the query
        // slot they hold is released only once they are written.
        .writev(true);
    let served = builder.serve_connection(io, TowerToHyperService::new(app));
    let mut served = std::pin::pin!(served);
    let ended = tokio::select! {
        ended = served.as_mut() => ended,
        _ = token.cancelled() => {
            served.as_mut().graceful_shutdown();
            served.await
        }
    };
    if let Err(e) = ended {
        debug!(error = %e, "api connection closed");
    }
}

/// A connection's stream whose reads and writes fail once one of them has
/// waited `stall` with no byte passing either way. A client that stops
/// reading its response, or sending its request, then loses the connection,
/// and the query slot a response holds.
struct Watchdog<S> {
    stream: S,
    stall: Duration,
    timer: Pin<Box<tokio::time::Sleep>>,
    /// The timer runs: something waits, and nothing has passed since.
    armed: bool,
}

impl<S> Watchdog<S> {
    fn new(stream: S, stall: Duration) -> Self {
        Self {
            stream,
            stall,
            timer: Box::pin(tokio::time::sleep(stall)),
            armed: false,
        }
    }

    /// For an operation that waits: whether it has waited too long. The
    /// waker is registered to find out.
    fn expired(&mut self, cx: &mut TaskContext<'_>) -> bool {
        if !self.armed {
            self.armed = true;
            let deadline = tokio::time::Instant::now() + self.stall;
            self.timer.as_mut().reset(deadline);
        }
        self.timer.as_mut().poll(cx).is_ready()
    }

    fn passed(&mut self, bytes: usize) {
        if bytes > 0 {
            self.armed = false;
        }
    }

    /// What an operation that waits returns.
    fn wait<T>(&mut self, cx: &mut TaskContext<'_>) -> Poll<io::Result<T>> {
        if self.expired(cx) {
            Poll::Ready(Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!("no progress for {:?}", self.stall),
            )))
        } else {
            Poll::Pending
        }
    }

    fn wrote(
        &mut self,
        cx: &mut TaskContext<'_>,
        written: Poll<io::Result<usize>>,
    ) -> Poll<io::Result<usize>> {
        match written {
            Poll::Ready(Ok(n)) => {
                self.passed(n);
                Poll::Ready(Ok(n))
            }
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Pending => self.wait(cx),
        }
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for Watchdog<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let before = buf.filled().len();
        match Pin::new(&mut this.stream).poll_read(cx, buf) {
            Poll::Ready(read) => {
                this.passed(buf.filled().len() - before);
                Poll::Ready(read)
            }
            Poll::Pending => this.wait(cx),
        }
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for Watchdog<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let written = Pin::new(&mut this.stream).poll_write(cx, buf);
        this.wrote(cx, written)
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let written = Pin::new(&mut this.stream).poll_write_vectored(cx, bufs);
        this.wrote(cx, written)
    }

    fn is_write_vectored(&self) -> bool {
        self.stream.is_write_vectored()
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        match Pin::new(&mut this.stream).poll_flush(cx) {
            Poll::Pending => this.wait(cx),
            flushed => flushed,
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().stream).poll_shutdown(cx)
    }
}

fn router(state: Shared) -> Router {
    Router::new()
        .route(
            "/v1/config",
            get(get_config).put(put_config).patch(patch_config),
        )
        .route("/v1/messages", get(get_messages))
        .route("/v1/status", get(get_status))
        .route("/v1/metrics", get(get_metrics))
        .route("/v1/health", get(get_health))
        .fallback(|| async { ApiError(StatusCode::NOT_FOUND, "not found".into()) })
        .method_not_allowed_fallback(|| async {
            ApiError(StatusCode::METHOD_NOT_ALLOWED, "method not allowed".into())
        })
        .layer(DefaultBodyLimit::max(BODY_BYTES))
        .with_state(state)
}

struct ApiError(StatusCode, String);

impl ApiError {
    fn bad(message: impl ToString) -> Self {
        ApiError(StatusCode::BAD_REQUEST, message.to_string())
    }

    fn internal(message: impl ToString) -> Self {
        ApiError(StatusCode::INTERNAL_SERVER_ERROR, message.to_string())
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.0, Json(json!({ "error": self.1 }))).into_response()
    }
}

impl From<config::Error> for ApiError {
    fn from(e: config::Error) -> Self {
        match e {
            config::Error::Io(_) => ApiError::internal(e),
            _ => ApiError::bad(e),
        }
    }
}

/// A body over the limit, for one.
impl From<BytesRejection> for ApiError {
    fn from(e: BytesRejection) -> Self {
        ApiError(e.status(), e.body_text())
    }
}

async fn get_config(State(state): State<Shared>) -> Json<Config> {
    Json(state.config.current().as_ref().clone())
}

async fn put_config(
    State(state): State<Shared>,
    body: Result<Bytes, BytesRejection>,
) -> Result<Json<Config>, ApiError> {
    let config = config::read(&body?)?;
    apply(move || state.config.update(config)).await
}

async fn patch_config(
    State(state): State<Shared>,
    body: Result<Bytes, BytesRejection>,
) -> Result<Json<Config>, ApiError> {
    let patch: Value = serde_json::from_slice(&body?).map_err(ApiError::bad)?;
    apply(move || state.config.patch(&patch)).await
}

/// Runs a store operation on the blocking pool (the save fsyncs).
async fn apply(
    operation: impl FnOnce() -> Result<Arc<Config>, config::Error> + Send + 'static,
) -> Result<Json<Config>, ApiError> {
    let applied = tokio::task::spawn_blocking(operation)
        .await
        .map_err(ApiError::internal)??;
    Ok(Json(applied.as_ref().clone()))
}

fn param<T: std::str::FromStr>(
    query: &BTreeMap<String, String>,
    name: &str,
) -> Result<Option<T>, ApiError> {
    query
        .get(name)
        .map(|v| {
            v.parse()
                .map_err(|_| ApiError::bad(format!("invalid {name:?}: {v:?}")))
        })
        .transpose()
}

async fn get_messages(
    State(state): State<Shared>,
    Query(query): Query<BTreeMap<String, String>>,
) -> Result<Response, ApiError> {
    let from: Option<i64> = param(&query, "from")?;
    let to: Option<i64> = param(&query, "to")?;
    let after: u64 = param(&query, "after")?.unwrap_or(0);
    let limit: usize = param(&query, "limit")?.unwrap_or(DEFAULT_LIMIT);
    if limit == 0 || limit > MAX_LIMIT {
        return Err(ApiError::bad(format!(
            "limit must be between 1 and {MAX_LIMIT}"
        )));
    }
    if let (Some(from), Some(to)) = (from, to) {
        if to <= from {
            return Err(ApiError::bad("to must be greater than from"));
        }
    }
    // Owned, so that it goes with the read if the client leaves mid-query,
    // and then with the response until its bytes are written.
    let slot = state
        .queries
        .clone()
        .acquire_owned()
        .await
        .map_err(ApiError::internal)?;
    let storage = state.storage.clone();
    let json = tokio::task::spawn_blocking(move || -> io::Result<Bytes> {
        let page = storage.query(from, to, after, limit, PAGE_BYTES)?;
        let next = page
            .more
            .then(|| page.records.last().map(|r| r.seq))
            .flatten();
        let body = MessagesBody {
            messages: page.records.iter().map(view::record).collect(),
            next,
        };
        let json = serde_json::to_vec(&body).map_err(io::Error::other)?;
        Ok(Bytes::from_owner(JsonPage { json, _slot: slot }))
    })
    .await
    .map_err(ApiError::internal)?
    .map_err(ApiError::internal)?;
    Ok(([(header::CONTENT_TYPE, "application/json")], json).into_response())
}

/// A page as JSON, holding the query slot until its bytes are freed, which
/// hyper does once it has written them.
struct JsonPage {
    json: Vec<u8>,
    _slot: OwnedSemaphorePermit,
}

impl AsRef<[u8]> for JsonPage {
    fn as_ref(&self) -> &[u8] {
        &self.json
    }
}

#[derive(Serialize)]
struct MessagesBody<'a> {
    messages: Vec<RecordView<'a>>,
    next: Option<u64>,
}

async fn get_status(State(state): State<Shared>) -> Json<Value> {
    let config = state.config.current();
    let storage = state.storage.status();
    let sm = state.storage.metrics();
    let inbound = &state.inbound;
    let times = state.times.lock().unwrap().clone();
    let channels: serde_json::Map<String, Value> = state
        .channels
        .lock()
        .unwrap()
        .iter()
        .map(|(name, m)| {
            let cursor = m.cursor.load(Relaxed);
            let lag = lag(&storage, cursor);
            (
                name.clone(),
                json!({
                    "kind": m.kind,
                    "enabled": m.enabled.load(Relaxed),
                    "running": m.running.load(Relaxed),
                    "connected": m.connected.load(Relaxed),
                    "cursor": (cursor > 0).then_some(cursor),
                    "lag": lag,
                    "waiting_ts": times.waiting_ts(name, lag),
                    "delivered": m.delivered.load(Relaxed),
                    "delivery_us": m.delivery_us.load(Relaxed),
                    "rejected": m.rejected.load(Relaxed),
                    "retries": m.retries.load(Relaxed),
                    "restarts": m.restarts.load(Relaxed),
                    "lost": m.lost.records(),
                    "notices": m.notices.load(Relaxed),
                    "last_error": m.last_error.get(),
                }),
            )
        })
        .collect();
    Json(json!({
        "version": env!("CARGO_PKG_VERSION"),
        "uptime_s": state.started.elapsed().as_secs(),
        "restart_required": config.restart_required(&state.boot),
        "api": { "last_error": state.last_error.get() },
        "inbound": {
            "connected": inbound.connected.load(Relaxed),
            "connections": inbound.connections.load(Relaxed),
            "messages": inbound.messages.load(Relaxed),
            "bytes": inbound.bytes.load(Relaxed),
            "truncated": inbound.truncated.load(Relaxed),
            "oversized": inbound.oversized.load(Relaxed),
            "last_error": inbound.last_error.get(),
        },
        "storage": {
            "segments": storage.segments,
            "bytes": storage.bytes,
            "first_seq": storage.first_seq,
            "first_ts": times.first_ts(&storage),
            "head_seq": storage.head_seq,
            "head_ts": storage.head_ts,
            "queued": state.queue.messages.load(Relaxed),
            "queued_bytes": state.queue.bytes.load(Relaxed),
            "batches": sm.batches.load(Relaxed),
            "records": sm.records.load(Relaxed),
            "bytes_written": sm.bytes.load(Relaxed),
            "syncs": sm.syncs.load(Relaxed),
            "sync_us": sm.sync_us.load(Relaxed),
            "write_errors": sm.write_errors.load(Relaxed),
            "dropped": sm.dropped.load(Relaxed),
            "evictions": sm.evictions.load(Relaxed),
            "corrupt_segments": sm.corrupt_segments.load(Relaxed),
            "clock_steps": sm.clock_steps.load(Relaxed),
            "last_error": sm.last_error.get().or_else(|| sm.evict_error.get()),
        },
        "channels": channels,
    }))
}

impl Times {
    /// Looks up the times for the log at `status` and the channels at
    /// `cursors`. A lookup reads on from where the one before it left off,
    /// and a channel keeps its lookup while nothing waits for it, so
    /// following a channel costs about as much as reading what it delivered.
    fn refresh(
        &mut self,
        storage: &Storage,
        status: &storage::Status,
        cursors: &BTreeMap<String, u64>,
    ) {
        if status.first_seq <= status.head_seq {
            self.first.update(storage, status.first_seq);
        }
        self.waiting.retain(|name, _| cursors.contains_key(name));
        for (name, &cursor) in cursors {
            let lookup = self.waiting.entry(name.clone()).or_default();
            if lag(status, cursor) > 0 {
                lookup.update(storage, cursor.max(status.first_seq));
            }
        }
    }

    /// The oldest retained record's receipt time, while the log holds one.
    fn first_ts(&self, storage: &storage::Status) -> Option<i64> {
        self.first
            .ts
            .filter(|_| storage.first_seq <= storage.head_seq)
    }

    /// The receipt time of the oldest record waiting for a channel, while
    /// one waits.
    fn waiting_ts(&self, channel: &str, lag: u64) -> Option<i64> {
        self.waiting.get(channel)?.ts.filter(|_| lag > 0)
    }
}

/// Records still in the log that a channel has not delivered; those that
/// left the log undelivered are counted in `lost` instead. Zero for a channel
/// without a position yet.
fn lag(storage: &storage::Status, cursor: u64) -> u64 {
    if cursor == 0 {
        return 0;
    }
    (storage.head_seq + 1).saturating_sub(cursor.max(storage.first_seq))
}

/// The log takes writes.
async fn get_health(State(state): State<Shared>) -> Result<&'static str, ApiError> {
    match state.storage.health() {
        Ok(()) => Ok("ok\n"),
        Err(reason) => Err(ApiError(StatusCode::SERVICE_UNAVAILABLE, reason)),
    }
}

/// A sample: a count, or a time in microseconds, exported in seconds.
enum Reading {
    Count(u64),
    Micros(i64),
}

impl Display for Reading {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Reading::Count(n) => write!(f, "{n}"),
            Reading::Micros(us) => {
                let sign = if us < 0 { "-" } else { "" };
                let us = us.unsigned_abs();
                write!(f, "{sign}{}.{:06}", us / 1_000_000, us % 1_000_000)
            }
        }
    }
}

fn micros(us: u64) -> Reading {
    Reading::Micros(i64::try_from(us).unwrap_or(i64::MAX))
}

/// What a channel family reads from a channel.
struct Row<'a> {
    metrics: &'a ChannelMetrics,
    cursor: u64,
    lag: u64,
    waiting_ts: Option<i64>,
}

type Sample<'a> = &'a dyn Fn(&Row) -> Option<Reading>;

async fn get_metrics(State(state): State<Shared>) -> Response {
    use Reading::Count;
    let mut e = Exposition::default();
    let inbound = &state.inbound;
    e.family(
        "uevtd_inbound_connected",
        "gauge",
        "A connection to the producer is up",
    )
    .value(u8::from(inbound.connected.load(Relaxed)));
    e.family(
        "uevtd_inbound_connections_total",
        "counter",
        "Connections made to the producer",
    )
    .value(inbound.connections.load(Relaxed));
    e.family(
        "uevtd_inbound_messages_total",
        "counter",
        "Complete messages received",
    )
    .value(inbound.messages.load(Relaxed));
    e.family(
        "uevtd_inbound_bytes_total",
        "counter",
        "Payload bytes received",
    )
    .value(inbound.bytes.load(Relaxed));
    e.family(
        "uevtd_inbound_truncated_total",
        "counter",
        "Messages discarded because the connection closed mid-line",
    )
    .value(inbound.truncated.load(Relaxed));
    e.family(
        "uevtd_inbound_oversized_total",
        "counter",
        "Messages discarded for exceeding the size limit",
    )
    .value(inbound.oversized.load(Relaxed));

    let storage = state.storage.status();
    let sm = state.storage.metrics();
    let times = state.times.lock().unwrap().clone();
    e.family("uevtd_storage_segments", "gauge", "Segment files")
        .value(storage.segments);
    e.family("uevtd_storage_bytes", "gauge", "Bytes on disk")
        .value(storage.bytes);
    e.family(
        "uevtd_storage_first_seq",
        "gauge",
        "Oldest retained sequence number",
    )
    .value(storage.first_seq);
    let mut first_ts = e.family(
        "uevtd_storage_first_ts_seconds",
        "gauge",
        "Receipt time of the oldest retained message",
    );
    if let Some(ts) = times.first_ts(&storage) {
        first_ts.value(Reading::Micros(ts));
    }
    e.family("uevtd_storage_head_seq", "gauge", "Newest sequence number")
        .value(storage.head_seq);
    let mut head_ts = e.family(
        "uevtd_storage_head_ts_seconds",
        "gauge",
        "Receipt time of the newest message",
    );
    if storage.head_seq > 0 {
        head_ts.value(Reading::Micros(storage.head_ts));
    }
    e.family(
        "uevtd_storage_queued_messages",
        "gauge",
        "Messages waiting for the writer",
    )
    .value(state.queue.messages.load(Relaxed));
    e.family(
        "uevtd_storage_queued_bytes",
        "gauge",
        "Payload bytes waiting for the writer",
    )
    .value(state.queue.bytes.load(Relaxed));
    e.family(
        "uevtd_storage_batches_total",
        "counter",
        "Write batches, committed or not",
    )
    .value(sm.batches.load(Relaxed));
    e.family(
        "uevtd_storage_records_total",
        "counter",
        "Committed records",
    )
    .value(sm.records.load(Relaxed));
    e.family(
        "uevtd_storage_bytes_written_total",
        "counter",
        "Bytes of records committed",
    )
    .value(sm.bytes.load(Relaxed));
    e.family(
        "uevtd_storage_syncs_total",
        "counter",
        "Syncs of the active segment",
    )
    .value(sm.syncs.load(Relaxed));
    e.family(
        "uevtd_storage_sync_seconds_total",
        "counter",
        "Time spent in syncs",
    )
    .value(micros(sm.sync_us.load(Relaxed)));
    e.family(
        "uevtd_storage_write_errors_total",
        "counter",
        "Failed writes, syncs and segment creations",
    )
    .value(sm.write_errors.load(Relaxed));
    e.family(
        "uevtd_storage_dropped_total",
        "counter",
        "Messages lost to a failed write or sync",
    )
    .value(sm.dropped.load(Relaxed));
    e.family(
        "uevtd_storage_evictions_total",
        "counter",
        "Evicted segments",
    )
    .value(sm.evictions.load(Relaxed));
    e.family(
        "uevtd_storage_corrupt_segments_total",
        "counter",
        "Segments whose remainder was skipped as corrupt or unreadable",
    )
    .value(sm.corrupt_segments.load(Relaxed));
    e.family(
        "uevtd_storage_clock_steps_total",
        "counter",
        "Timestamps clamped because the clock stepped back",
    )
    .value(sm.clock_steps.load(Relaxed));

    let channels = state.channels.lock().unwrap();
    let rows: Vec<(&str, Row<'_>)> = channels
        .iter()
        .map(|(name, metrics)| {
            let cursor = metrics.cursor.load(Relaxed);
            let lag = lag(&storage, cursor);
            let row = Row {
                metrics,
                cursor,
                lag,
                waiting_ts: times.waiting_ts(name, lag),
            };
            (name.as_str(), row)
        })
        .collect();
    let families: [(&str, &str, &str, Sample<'_>); 12] = [
        (
            "uevtd_channel_enabled",
            "gauge",
            "Channel enabled in configuration",
            &|r| Some(Count(r.metrics.enabled.load(Relaxed).into())),
        ),
        (
            "uevtd_channel_connected",
            "gauge",
            "A session with the target is up",
            &|r| Some(Count(r.metrics.connected.load(Relaxed).into())),
        ),
        (
            "uevtd_channel_cursor",
            "gauge",
            "Next sequence number to deliver",
            &|r| (r.cursor > 0).then_some(Count(r.cursor)),
        ),
        (
            "uevtd_channel_lag",
            "gauge",
            "Messages in the log waiting for delivery",
            &|r| Some(Count(r.lag)),
        ),
        (
            "uevtd_channel_waiting_ts_seconds",
            "gauge",
            "Receipt time of the oldest message waiting for delivery",
            &|r| r.waiting_ts.map(Reading::Micros),
        ),
        (
            "uevtd_channel_delivered_total",
            "counter",
            "Acknowledged deliveries",
            &|r| Some(Count(r.metrics.delivered.load(Relaxed))),
        ),
        (
            "uevtd_channel_delivery_seconds_total",
            "counter",
            "Time from receipt to acknowledgement, summed over deliveries",
            &|r| Some(micros(r.metrics.delivery_us.load(Relaxed))),
        ),
        (
            "uevtd_channel_rejected_total",
            "counter",
            "Messages the channel or its target refused",
            &|r| Some(Count(r.metrics.rejected.load(Relaxed))),
        ),
        (
            "uevtd_channel_retries_total",
            "counter",
            "Reconnections after a failed connection or a lost session",
            &|r| Some(Count(r.metrics.retries.load(Relaxed))),
        ),
        (
            "uevtd_channel_restarts_total",
            "counter",
            "Starts after the channel's task ended on its own",
            &|r| Some(Count(r.metrics.restarts.load(Relaxed))),
        ),
        (
            "uevtd_channel_lost_total",
            "counter",
            "Messages that left the log before delivery",
            &|r| Some(Count(r.metrics.lost.records())),
        ),
        (
            "uevtd_channel_notices_total",
            "counter",
            "Drop notices acknowledged",
            &|r| Some(Count(r.metrics.notices.load(Relaxed))),
        ),
    ];
    for (name, kind, help, value) in families {
        let mut family = e.family(name, kind, help);
        for (channel, row) in &rows {
            if let Some(value) = value(row) {
                family.sample(&[("channel", channel), ("kind", row.metrics.kind)], value);
            }
        }
    }
    drop(rows);
    drop(channels);
    (
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        e.finish(),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::{Path, PathBuf};

    use axum::routing::put;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::*;
    use crate::storage::{Limits, Message, Writer};
    use crate::testing::Pipe;

    /// A state whose log holds `records` messages of 1 000 bytes, received
    /// at 0, 1, 2 and so on, in segments of 64 KiB.
    fn state(dir: &Path, records: i64) -> Shared {
        let limits = Limits {
            max_bytes: 64 << 20,
            segment_bytes: 64 << 10,
            sync_interval: Duration::ZERO,
        };
        let storage = Arc::new(Storage::open(&dir.join("log"), limits).unwrap());
        let mut writer = Writer::new(storage.clone());
        for ts in 0..records {
            let message = Message {
                ts,
                payload: vec![b'x'; 1000],
            };
            writer.append(&[message], false).unwrap();
        }
        writer.sync().unwrap();
        let (queue, _) = storage::queue(16, 1 << 20);
        Arc::new(AppState {
            config: Arc::new(config::Store::new(
                dir.join("config.json"),
                Config::default(),
            )),
            boot: Arc::new(Config::default()),
            storage,
            queue: queue.depth(),
            inbound: Arc::default(),
            channels: Arc::default(),
            started: Instant::now(),
            last_error: ErrorSlot::default(),
            queries: Arc::new(Semaphore::new(CONCURRENT_QUERIES)),
            times: Mutex::default(),
        })
    }

    async fn until(mut done: impl FnMut() -> bool) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while !done() {
            assert!(Instant::now() < deadline, "not in time");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    #[test]
    fn a_channel_keeps_its_lookup_while_nothing_waits() {
        let dir = tempfile::tempdir().unwrap();
        let state = state(dir.path(), 100);
        let (storage, status) = (&state.storage, state.storage.status());
        let mut times = Times::default();
        let cursors = |cursor: u64| BTreeMap::from([("t".to_string(), cursor)]);
        times.refresh(storage, &status, &cursors(50));
        assert_eq!(times.waiting_ts("t", lag(&status, 50)), Some(49));
        let mark = format!("{:?}", times.waiting["t"].mark);
        // Caught up, it has no time to show, and the lookup stays where it
        // was, for the next one to read on from.
        let caught_up = status.head_seq + 1;
        times.refresh(storage, &status, &cursors(caught_up));
        assert_eq!(times.waiting_ts("t", lag(&status, caught_up)), None);
        assert_eq!(format!("{:?}", times.waiting["t"].mark), mark);
        // Behind again, it reads on from there.
        times.refresh(storage, &status, &cursors(60));
        assert_eq!(times.waiting_ts("t", lag(&status, 60)), Some(59));
        // A channel that is gone takes its lookup with it.
        times.refresh(storage, &status, &BTreeMap::new());
        assert!(times.waiting.is_empty());
    }

    #[tokio::test]
    async fn a_query_holds_its_slot_until_its_response_is_written() {
        let dir = tempfile::tempdir().unwrap();
        let state = state(dir.path(), 100);
        let query = Query(BTreeMap::from([("limit".to_string(), "1000".to_string())]));
        let Ok(response) = get_messages(State(state.clone()), query).await else {
            panic!("query failed");
        };
        // The response's bytes hold the slot, not the handler that made them.
        assert_eq!(state.queries.available_permits(), 0);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        assert!(body.len() > 100_000, "{}", body.len());
        drop(body);
        assert_eq!(state.queries.available_permits(), 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn an_abandoned_query_keeps_its_slot_until_its_read_ends() {
        let dir = tempfile::tempdir().unwrap();
        let state = state(dir.path(), 100);
        let first = dir.path().join("log/00000000000000000001.seg");
        fs::rename(&first, dir.path().join("first.seg")).unwrap();
        let pipe = Pipe::new(&first);
        let query = tokio::spawn(get_messages(State(state.clone()), Query(BTreeMap::new())));
        until(|| state.queries.available_permits() == 0).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        // The client goes away, as hyper then drops the handler, and the
        // read goes on with the slot.
        query.abort();
        assert!(matches!(query.await, Err(e) if e.is_cancelled()));
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(state.queries.available_permits(), 0);
        // Once the read ends, the slot is free.
        drop(pipe);
        until(|| state.queries.available_permits() == 1).await;
    }

    /// A server with `limits` in `dir`, which answers `/` with `ok`, sends
    /// 4 MiB for `/big` and echoes what is put to `/echo`.
    fn server(dir: &Path, limits: Serve) -> (Server, PathBuf) {
        let path = dir.join("api.sock");
        let listener = UnixListener::bind(&path).unwrap();
        let app = Router::new()
            .route("/", get(|| async { "ok" }))
            .route("/big", get(|| async { vec![b'b'; 4 << 20] }))
            .route("/echo", put(|body: Bytes| async move { body }));
        (Server::spawn(listener, app, limits), path)
    }

    async fn get_root(stream: &mut UnixStream) {
        stream
            .write_all(b"GET / HTTP/1.1\r\nhost: uevtd\r\n\r\n")
            .await
            .unwrap();
    }

    /// Reads the response to `get_root`; false if the connection ends first.
    async fn answered(stream: &mut UnixStream) -> bool {
        let mut got = Vec::new();
        let mut buf = [0u8; 1024];
        while !got.ends_with(b"\r\n\r\nok") {
            match stream.read(&mut buf).await {
                Ok(0) | Err(_) => return false,
                Ok(n) => got.extend_from_slice(&buf[..n]),
            }
        }
        true
    }

    /// Reads until the connection ends; how many bytes came.
    async fn until_closed(stream: &mut UnixStream) -> usize {
        let mut got = 0;
        let mut buf = vec![0u8; 64 << 10];
        loop {
            match stream.read(&mut buf).await {
                Ok(0) | Err(_) => return got,
                Ok(n) => got += n,
            }
        }
    }

    const LONG: Duration = Duration::from_secs(60);

    #[tokio::test]
    async fn connections_past_the_limit_wait_until_one_closes() {
        let dir = tempfile::tempdir().unwrap();
        let limits = Serve {
            connections: 2,
            idle: LONG,
            stall: LONG,
        };
        let (server, path) = server(dir.path(), limits);
        let mut open = Vec::new();
        for _ in 0..2 {
            let mut stream = UnixStream::connect(&path).await.unwrap();
            get_root(&mut stream).await;
            assert!(answered(&mut stream).await);
            open.push(stream);
        }
        let mut third = UnixStream::connect(&path).await.unwrap();
        get_root(&mut third).await;
        let waited = tokio::time::timeout(Duration::from_millis(300), answered(&mut third)).await;
        assert!(waited.is_err(), "answered past the limit");
        drop(open.pop());
        let answer = tokio::time::timeout(Duration::from_secs(5), answered(&mut third)).await;
        assert!(answer.unwrap());
        server.stop().await;
    }

    #[tokio::test]
    async fn idle_connections_are_closed() {
        let dir = tempfile::tempdir().unwrap();
        let limits = Serve {
            connections: 2,
            idle: Duration::from_millis(200),
            stall: LONG,
        };
        let (server, path) = server(dir.path(), limits);
        // One that never sends a request, and one after its response.
        let mut silent = UnixStream::connect(&path).await.unwrap();
        let mut served = UnixStream::connect(&path).await.unwrap();
        get_root(&mut served).await;
        assert!(answered(&mut served).await);
        for stream in [&mut silent, &mut served] {
            let closed = tokio::time::timeout(Duration::from_secs(5), until_closed(stream)).await;
            assert!(closed.is_ok(), "still open");
        }
        server.stop().await;
    }

    #[tokio::test]
    async fn a_client_that_stops_sending_or_reading_loses_its_connection() {
        let dir = tempfile::tempdir().unwrap();
        let limits = Serve {
            connections: 1,
            idle: LONG,
            stall: Duration::from_secs(1),
        };
        let (server, path) = server(dir.path(), limits);
        // A body that stops coming, and a response that is never read: each
        // holds the one connection until it stalls for a second.
        let requests: [&[u8]; 2] = [
            b"PUT /echo HTTP/1.1\r\nhost: uevtd\r\ncontent-length: 100\r\n\r\nabc",
            b"GET /big HTTP/1.1\r\nhost: uevtd\r\n\r\n",
        ];
        for request in requests {
            let mut stuck = UnixStream::connect(&path).await.unwrap();
            stuck.write_all(request).await.unwrap();
            let mut next = UnixStream::connect(&path).await.unwrap();
            get_root(&mut next).await;
            let waited =
                tokio::time::timeout(Duration::from_millis(200), answered(&mut next)).await;
            assert!(
                waited.is_err(),
                "answered while the other connection held the slot"
            );
            let answer = tokio::time::timeout(Duration::from_secs(5), answered(&mut next)).await;
            assert!(answer.unwrap());
            assert!(until_closed(&mut stuck).await < 4 << 20);
        }
        server.stop().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn what_failed_to_apply_is_retried_until_the_configuration_is_in_effect() {
        async fn until(what: &str, mut done: impl FnMut() -> bool) {
            let deadline = Instant::now() + Duration::from_secs(10);
            while !done() {
                assert!(Instant::now() < deadline, "{what}");
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let state = state(dir.path(), 0);
        let path = dir.path().join("api.sock");
        let mut config = Config::default();
        config.api.path = path.clone();
        let (cfg, rx) = watch::channel(Arc::new(config.clone()));
        let shutdown = CancellationToken::new();
        let task = tokio::spawn(run(rx, state.clone(), shutdown.clone()));
        until("listening", || path.exists()).await;
        let error = || state.last_error.get();
        let message = || error().map(|e| e.message).unwrap_or_default();

        // Without its file the socket's mode cannot change. The old mode is
        // in effect, so the same configuration tries again, and going back
        // to that mode leaves nothing to apply.
        fs::remove_file(&path).unwrap();
        config.api.mode = config::Mode(0o600);
        cfg.send_replace(Arc::new(config.clone()));
        until("chmod failed", || message().starts_with("chmod")).await;
        let failed = error().unwrap().ts;
        tokio::time::sleep(Duration::from_millis(2)).await;
        cfg.send_replace(Arc::new(config.clone()));
        until("chmod tried again", || {
            error().is_some_and(|e| e.ts > failed)
        })
        .await;
        config.api.mode = config::Mode(0o660);
        cfg.send_replace(Arc::new(config.clone()));
        until("chmod error cleared", || error().is_none()).await;

        // Likewise for a path that cannot be bound, under a file.
        let file = dir.path().join("file");
        fs::write(&file, b"").unwrap();
        config.api.path = file.join("api.sock");
        cfg.send_replace(Arc::new(config.clone()));
        until("bind failed", || message().starts_with("bind")).await;
        config.api.path = path.clone();
        cfg.send_replace(Arc::new(config.clone()));
        until("bind error cleared", || error().is_none()).await;

        shutdown.cancel();
        task.await.unwrap().unwrap();
    }
}
