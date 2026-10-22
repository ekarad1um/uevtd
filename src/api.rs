//! HTTP API over a Unix domain socket: configuration, time-range queries,
//! status, metrics and health (ARCHITECTURE.md §8).

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::Ordering::Relaxed;
use std::time::{Duration, Instant};

use anyhow::Context;
use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, Query, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use serde::Serialize;
use serde_json::{Value, json};
use tokio::net::UnixListener;
use tokio::sync::{Semaphore, watch};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::config::{self, Config};
use crate::metrics::{ErrorSlot, Exposition};
use crate::outbound::Registry;
use crate::storage::{self, Storage};
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

pub struct AppState {
    pub config: Arc<config::Store>,
    pub boot: Arc<Config>,
    pub storage: Arc<Storage>,
    pub inbound: Arc<inbound::Metrics>,
    pub channels: Arc<Registry>,
    pub started: Instant,
    pub last_error: ErrorSlot,
    /// Holds [`CONCURRENT_QUERIES`] permits.
    pub queries: Semaphore,
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
    let mut server = Server::spawn(listener, app.clone());

    loop {
        tokio::select! {
            changed = cfg.changed() => {
                if changed.is_err() {
                    break;
                }
                let next = cfg.borrow_and_update().api.clone();
                if next == current {
                    continue;
                }
                if next.path != current.path {
                    match uds::bind(&next.path, next.mode.0).await {
                        Ok(fresh) => {
                            let old = std::mem::replace(&mut server, Server::spawn(fresh, app.clone()));
                            old.stop().await;
                            uds::remove(&current.path);
                            state.last_error.clear();
                            info!(path = %next.path.display(), "api rebound");
                            current = next;
                        }
                        Err(e) => {
                            error!(path = %next.path.display(), error = %e, "rebind failed; keeping current socket");
                            state.last_error.set(format!("bind {}: {e}", next.path.display()));
                        }
                    }
                } else {
                    if let Err(e) = uds::set_mode(&next.path, next.mode.0) {
                        error!(error = %e, "could not change socket mode");
                        state.last_error.set(format!("chmod {}: {e}", next.path.display()));
                    }
                    current = next;
                }
            }
            _ = shutdown.cancelled() => break,
        }
    }
    server.stop().await;
    uds::remove(&current.path);
    info!("api stopped");
    Ok(())
}

struct Server {
    token: CancellationToken,
    task: JoinHandle<std::io::Result<()>>,
}

impl Server {
    fn spawn(listener: UnixListener, app: Router) -> Server {
        let token = CancellationToken::new();
        let signal = token.clone().cancelled_owned();
        let task = tokio::spawn(
            axum::serve(listener, app)
                .with_graceful_shutdown(signal)
                .into_future(),
        );
        Server { token, task }
    }

    async fn stop(self) {
        self.token.cancel();
        let mut task = self.task;
        match tokio::time::timeout(Duration::from_secs(5), &mut task).await {
            Ok(Ok(Ok(()))) => {}
            Ok(Ok(Err(e))) => warn!(error = %e, "api server error"),
            Ok(Err(e)) => error!(error = %e, "api server task failed"),
            Err(_) => {
                warn!("api server did not stop in time; aborting");
                task.abort();
            }
        }
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
        .route("/v1/health", get(|| async { "ok\n" }))
        .fallback(|| async { ApiError(StatusCode::NOT_FOUND, "not found".into()) })
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

async fn get_config(State(state): State<Shared>) -> Json<Config> {
    Json(state.config.current().as_ref().clone())
}

async fn put_config(State(state): State<Shared>, body: Bytes) -> Result<Json<Config>, ApiError> {
    let config: Config = serde_json::from_slice(&body).map_err(ApiError::bad)?;
    apply(move || state.config.update(config)).await
}

async fn patch_config(State(state): State<Shared>, body: Bytes) -> Result<Json<Config>, ApiError> {
    let patch: Value = serde_json::from_slice(&body).map_err(ApiError::bad)?;
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
    let _slot = state.queries.acquire().await.map_err(ApiError::internal)?;
    let storage = state.storage.clone();
    let page =
        tokio::task::spawn_blocking(move || storage.query(from, to, after, limit, PAGE_BYTES))
            .await
            .map_err(ApiError::internal)?
            .map_err(ApiError::internal)?;
    let next = page
        .more
        .then(|| page.records.last().map(|r| r.seq))
        .flatten();
    let body = MessagesBody {
        messages: page.records.iter().map(view::record).collect(),
        next,
    };
    let json = serde_json::to_vec(&body).map_err(ApiError::internal)?;
    Ok(([(header::CONTENT_TYPE, "application/json")], json).into_response())
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
    let channels: serde_json::Map<String, Value> = state
        .channels
        .lock()
        .unwrap()
        .iter()
        .map(|(name, m)| {
            let cursor = m.cursor.load(Relaxed);
            (
                name.clone(),
                json!({
                    "kind": m.kind,
                    "enabled": m.enabled.load(Relaxed),
                    "running": m.running.load(Relaxed),
                    "connected": m.connected.load(Relaxed),
                    "cursor": cursor,
                    "lag": lag(&storage, cursor),
                    "delivered": m.delivered.load(Relaxed),
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
            "connections": inbound.connections.load(Relaxed),
            "active": inbound.active.load(Relaxed),
            "rejected": inbound.rejected.load(Relaxed),
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
            "head_seq": storage.head_seq,
            "head_ts": storage.head_ts,
            "batches": sm.batches.load(Relaxed),
            "records": sm.records.load(Relaxed),
            "bytes_written": sm.bytes.load(Relaxed),
            "write_errors": sm.write_errors.load(Relaxed),
            "evictions": sm.evictions.load(Relaxed),
            "corrupt_segments": sm.corrupt_segments.load(Relaxed),
            "clock_steps": sm.clock_steps.load(Relaxed),
        },
        "channels": channels,
    }))
}

/// Records still in the log that a channel has not delivered; those that
/// left the log undelivered are counted in `lost` instead.
fn lag(storage: &storage::Status, cursor: u64) -> u64 {
    (storage.head_seq + 1).saturating_sub(cursor.max(storage.first_seq))
}

type Sample<'a> = &'a dyn Fn(&crate::outbound::ChannelMetrics) -> u64;

async fn get_metrics(State(state): State<Shared>) -> Response {
    let mut e = Exposition::default();
    let inbound = &state.inbound;
    e.family(
        "uevtd_inbound_connections_total",
        "counter",
        "Accepted connections",
    )
    .value(inbound.connections.load(Relaxed));
    e.family(
        "uevtd_inbound_connections_active",
        "gauge",
        "Open connections",
    )
    .value(inbound.active.load(Relaxed));
    e.family(
        "uevtd_inbound_connections_rejected_total",
        "counter",
        "Connections closed because of the limit",
    )
    .value(inbound.rejected.load(Relaxed));
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
        "Messages discarded because the peer closed mid-line",
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
    e.family("uevtd_storage_head_seq", "gauge", "Newest sequence number")
        .value(storage.head_seq);
    e.family(
        "uevtd_storage_batches_total",
        "counter",
        "Committed write batches",
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
        "Bytes written",
    )
    .value(sm.bytes.load(Relaxed));
    e.family(
        "uevtd_storage_write_errors_total",
        "counter",
        "Failed batches",
    )
    .value(sm.write_errors.load(Relaxed));
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
    let families: [(&str, &str, &str, Sample<'_>); 10] = [
        (
            "uevtd_channel_enabled",
            "gauge",
            "Channel enabled in configuration",
            &|m| m.enabled.load(Relaxed) as u64,
        ),
        (
            "uevtd_channel_connected",
            "gauge",
            "A session with the target is up",
            &|m| m.connected.load(Relaxed) as u64,
        ),
        (
            "uevtd_channel_cursor",
            "gauge",
            "Next sequence number to deliver",
            &|m| m.cursor.load(Relaxed),
        ),
        (
            "uevtd_channel_delivered_total",
            "counter",
            "Acknowledged deliveries",
            &|m| m.delivered.load(Relaxed),
        ),
        (
            "uevtd_channel_rejected_total",
            "counter",
            "Messages the channel or its target refused",
            &|m| m.rejected.load(Relaxed),
        ),
        (
            "uevtd_channel_retries_total",
            "counter",
            "Reconnections after a failed connection or a lost session",
            &|m| m.retries.load(Relaxed),
        ),
        (
            "uevtd_channel_restarts_total",
            "counter",
            "Starts after the channel's task ended on its own",
            &|m| m.restarts.load(Relaxed),
        ),
        (
            "uevtd_channel_lost_total",
            "counter",
            "Messages that left the log before delivery",
            &|m| m.lost.records(),
        ),
        (
            "uevtd_channel_notices_total",
            "counter",
            "Drop notices acknowledged",
            &|m| m.notices.load(Relaxed),
        ),
        (
            "uevtd_channel_lag",
            "gauge",
            "Messages in the log waiting for delivery",
            &|m| lag(&storage, m.cursor.load(Relaxed)),
        ),
    ];
    for (name, kind, help, value) in families {
        let mut family = e.family(name, kind, help);
        for (channel, m) in channels.iter() {
            family.sample(&[("channel", channel), ("kind", m.kind)], value(m));
        }
    }
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
