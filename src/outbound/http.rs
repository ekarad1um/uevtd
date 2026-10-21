//! HTTP channel: one request per message, strictly in order, with retries for
//! transient failures and skipping of permanent rejections (ARCHITECTURE.md §6.2).

use std::sync::Arc;
use std::sync::atomic::Ordering::Relaxed;
use std::time::Duration;

use reqwest::header::{CONTENT_TYPE, HeaderMap, HeaderName, HeaderValue};
use reqwest::{Client, Method, StatusCode, Url};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use super::{ChannelMetrics, CursorStore, DELIVERY_BYTES, Delivery, Feed, backoff};
use crate::config;
use crate::storage::Record;

const BATCH: usize = 64;
const FLUSH_INTERVAL: Duration = Duration::from_secs(1);
const BACKOFF_BASE: Duration = Duration::from_millis(100);
const BACKOFF_MAX: Duration = Duration::from_secs(30);

struct Template {
    client: Client,
    method: Method,
    url: Url,
    headers: HeaderMap,
}

pub async fn run(
    cfg: config::Http,
    mut feed: Feed,
    mut cursor: CursorStore,
    metrics: Arc<ChannelMetrics>,
    token: CancellationToken,
) {
    let template = loop {
        match Template::new(&cfg) {
            Ok(template) => break template,
            Err(e) => {
                warn!(error = %e, "cannot build http client; retrying");
                metrics.last_error.set(&e);
                tokio::select! {
                    _ = tokio::time::sleep(BACKOFF_MAX) => {}
                    _ = token.cancelled() => return,
                }
            }
        }
    };
    info!(url = %config::redacted_url(&cfg.url), method = %template.method, "forwarding over http");
    let mut flush = tokio::time::interval(FLUSH_INTERVAL);
    flush.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        let delivery = tokio::select! {
            delivery = feed.next(BATCH, DELIVERY_BYTES) => delivery,
            _ = flush.tick() => {
                cursor.flush(false).await;
                continue;
            }
            _ = token.cancelled() => break,
        };
        match delivery {
            // Counted and logged by the log; this channel sends no notice.
            Delivery::Lost { to, .. } => cursor.advance(to + 1),
            Delivery::Records(records) => {
                for record in records {
                    if token.is_cancelled() || !deliver(&template, &record, &metrics, &token).await
                    {
                        cursor.flush(true).await;
                        return;
                    }
                    cursor.advance(record.seq + 1);
                }
            }
        }
        cursor.flush(false).await;
    }
    cursor.flush(true).await;
}

impl Template {
    fn new(cfg: &config::Http) -> Result<Self, String> {
        let client = Client::builder()
            .timeout(Duration::from_millis(cfg.timeout_ms))
            .redirect(reqwest::redirect::Policy::none())
            .user_agent(concat!("uevtd/", env!("CARGO_PKG_VERSION")))
            .build()
            .map_err(|e| format!("http client: {e}"))?;
        let method = Method::from_bytes(cfg.method.to_ascii_uppercase().as_bytes())
            .map_err(|e| format!("method: {e}"))?;
        let url = Url::parse(&cfg.url).map_err(|e| format!("url: {e}"))?;
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        for (name, value) in &cfg.headers {
            let name = HeaderName::from_bytes(name.as_bytes())
                .map_err(|e| format!("header {name:?}: {e}"))?;
            let value =
                HeaderValue::from_str(value).map_err(|e| format!("header {name:?}: {e}"))?;
            headers.insert(name, value);
        }
        Ok(Self {
            client,
            method,
            url,
            headers,
        })
    }
}

/// Delivers one record; false only if cancelled while waiting to retry.
async fn deliver(
    t: &Template,
    record: &Record,
    metrics: &ChannelMetrics,
    token: &CancellationToken,
) -> bool {
    let mut attempt = 0u32;
    loop {
        let request = t
            .client
            .request(t.method.clone(), t.url.clone())
            .headers(t.headers.clone())
            .body(record.payload.clone());
        let sent = tokio::select! {
            sent = request.send() => sent,
            _ = token.cancelled() => return false,
        };
        let failure = match sent {
            Ok(mut response) => {
                let status = response.status();
                while let Ok(Some(_)) = response.chunk().await {}
                if status.is_success() {
                    if attempt > 0 {
                        info!(
                            seq = record.seq,
                            attempts = attempt + 1,
                            "delivered after retries"
                        );
                    }
                    metrics.delivered.fetch_add(1, Relaxed);
                    metrics.connected.store(true, Relaxed);
                    return true;
                }
                if !transient(status) {
                    warn!(seq = record.seq, %status, "rejected by endpoint; skipping");
                    metrics.rejected.fetch_add(1, Relaxed);
                    metrics
                        .last_error
                        .set(format!("seq {}: status {status}", record.seq));
                    return true;
                }
                format!("status {status}")
            }
            Err(e) => e.to_string(),
        };
        metrics.connected.store(false, Relaxed);
        metrics.retries.fetch_add(1, Relaxed);
        metrics.last_error.set(&failure);
        let delay = backoff(attempt, BACKOFF_BASE, BACKOFF_MAX);
        attempt += 1;
        warn!(seq = record.seq, attempt, error = %failure, retry_in_ms = delay.as_millis() as u64, "delivery failed");
        tokio::select! {
            _ = tokio::time::sleep(delay) => {}
            _ = token.cancelled() => return false,
        }
    }
}

fn transient(status: StatusCode) -> bool {
    status.is_server_error()
        || status == StatusCode::REQUEST_TIMEOUT
        || status == StatusCode::TOO_MANY_REQUESTS
}
