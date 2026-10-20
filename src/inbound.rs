//! Inbound: newline-delimited messages over a Unix domain socket
//! (ARCHITECTURE.md §4). Framing is a sans-IO state machine so its edge cases
//! are unit-testable.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::Duration;

use anyhow::Context;
use tokio::io::AsyncReadExt;
use tokio::net::UnixStream;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, watch};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, debug, error, info, warn};

use crate::config::{self, Config};
use crate::metrics::ErrorSlot;
use crate::storage::Message;
use crate::{now_us, uds};

#[derive(Default)]
pub struct Metrics {
    pub connections: AtomicU64,
    pub active: AtomicU64,
    pub rejected: AtomicU64,
    pub messages: AtomicU64,
    pub bytes: AtomicU64,
    pub truncated: AtomicU64,
    pub oversized: AtomicU64,
    pub last_error: ErrorSlot,
}

pub async fn run(
    mut cfg: watch::Receiver<Arc<Config>>,
    sink: mpsc::Sender<Message>,
    metrics: Arc<Metrics>,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    let mut current = cfg.borrow_and_update().inbound.clone();
    let mut listener = uds::bind(&current.path, current.mode.0)
        .with_context(|| format!("binding inbound socket {}", current.path.display()))?;
    info!(path = %current.path.display(), "inbound listening");
    let mut limiter = Arc::new(Semaphore::new(current.max_connections));
    let mut conns = JoinSet::new();
    let mut next_id = 0u64;

    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let stream = match accepted {
                    Ok((stream, _)) => stream,
                    Err(e) => {
                        warn!(error = %e, "accept failed");
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        continue;
                    }
                };
                metrics.connections.fetch_add(1, Relaxed);
                let Ok(permit) = limiter.clone().try_acquire_owned() else {
                    metrics.rejected.fetch_add(1, Relaxed);
                    warn!(max = current.max_connections, "connection limit reached; closing new connection");
                    continue;
                };
                next_id += 1;
                metrics.active.fetch_add(1, Relaxed);
                let span = tracing::info_span!("conn", id = next_id);
                conns.spawn(
                    serve(stream, current.max_message_bytes, sink.clone(), metrics.clone(), shutdown.clone(), permit)
                        .instrument(span),
                );
            }
            Some(finished) = conns.join_next() => {
                metrics.active.fetch_sub(1, Relaxed);
                if let Err(e) = finished {
                    error!(error = %e, "connection task failed");
                }
            }
            changed = cfg.changed() => {
                if changed.is_err() {
                    break;
                }
                let next = cfg.borrow_and_update().inbound.clone();
                if next == current {
                    continue;
                }
                if next.max_connections != current.max_connections {
                    limiter = Arc::new(Semaphore::new(next.max_connections));
                }
                current = rebind(&mut listener, current, next, &metrics);
            }
            _ = shutdown.cancelled() => break,
        }
    }

    drop(listener);
    uds::remove(&current.path);
    while conns.join_next().await.is_some() {}
    metrics.active.store(0, Relaxed);
    info!("inbound stopped");
    Ok(())
}

/// Applies a socket path or mode change; on failure keeps the old socket and
/// records the error, so producers are never left without a listener.
fn rebind(
    listener: &mut tokio::net::UnixListener,
    current: config::Inbound,
    next: config::Inbound,
    metrics: &Metrics,
) -> config::Inbound {
    if next.path != current.path {
        match uds::bind(&next.path, next.mode.0) {
            Ok(fresh) => {
                *listener = fresh;
                uds::remove(&current.path);
                metrics.last_error.clear();
                info!(path = %next.path.display(), "inbound rebound");
                next
            }
            Err(e) => {
                error!(path = %next.path.display(), error = %e, "rebind failed; keeping current socket");
                metrics
                    .last_error
                    .set(format!("bind {}: {e}", next.path.display()));
                config::Inbound {
                    path: current.path,
                    mode: current.mode,
                    ..next
                }
            }
        }
    } else {
        if next.mode != current.mode {
            if let Err(e) = uds::set_mode(&next.path, next.mode.0) {
                error!(error = %e, "could not change socket mode");
                metrics
                    .last_error
                    .set(format!("chmod {}: {e}", next.path.display()));
            }
        }
        next
    }
}

async fn serve(
    mut stream: UnixStream,
    max_line: usize,
    sink: mpsc::Sender<Message>,
    metrics: Arc<Metrics>,
    shutdown: CancellationToken,
    _permit: OwnedSemaphorePermit,
) {
    debug!("connected");
    let mut framer = Framer::new(max_line);
    let mut buf = vec![0u8; 16 * 1024];
    loop {
        let n = tokio::select! {
            read = stream.read(&mut buf) => match read {
                Ok(0) => break,
                Ok(n) => n,
                Err(e) => {
                    debug!(error = %e, "read failed");
                    break;
                }
            },
            _ = shutdown.cancelled() => {
                debug!("closing for shutdown");
                return;
            }
        };
        framer.push(&buf[..n]);
        while let Some(frame) = framer.next_frame() {
            match frame {
                Frame::Line(payload) => {
                    metrics.messages.fetch_add(1, Relaxed);
                    metrics.bytes.fetch_add(payload.len() as u64, Relaxed);
                    if sink
                        .send(Message {
                            ts: now_us(),
                            payload,
                        })
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
                Frame::Oversized(bytes) => {
                    metrics.oversized.fetch_add(1, Relaxed);
                    warn!(bytes, max = max_line, "discarding oversized message");
                }
            }
        }
    }
    let pending = framer.pending();
    if pending > 0 {
        metrics.truncated.fetch_add(1, Relaxed);
        warn!(
            bytes = pending,
            "peer closed mid-message; discarding truncated message"
        );
    }
    debug!("disconnected");
}

pub enum Frame {
    /// A complete line without its terminator.
    Line(Vec<u8>),
    /// A line longer than the limit was dropped; its length in bytes on the wire, newline excluded.
    Oversized(usize),
}

/// Splits a byte stream into newline-terminated messages with a length limit.
pub struct Framer {
    max: usize,
    buf: Vec<u8>,
    start: usize,
    /// Bytes after `start` already known to contain no newline.
    scanned: usize,
    discarding: bool,
    discarded: usize,
}

impl Framer {
    pub fn new(max: usize) -> Self {
        Self {
            max,
            buf: Vec::new(),
            start: 0,
            scanned: 0,
            discarding: false,
            discarded: 0,
        }
    }

    pub fn push(&mut self, data: &[u8]) {
        if self.start > 0 && self.start >= self.buf.len() / 2 {
            self.buf.drain(..self.start);
            self.start = 0;
        }
        self.buf.extend_from_slice(data);
    }

    pub fn next_frame(&mut self) -> Option<Frame> {
        loop {
            let pending = &self.buf[self.start..];
            let newline = pending[self.scanned..]
                .iter()
                .position(|&b| b == b'\n')
                .map(|i| i + self.scanned);
            if self.discarding {
                return match newline {
                    Some(i) => {
                        self.discarded += i;
                        self.consume(i + 1);
                        self.discarding = false;
                        Some(Frame::Oversized(std::mem::take(&mut self.discarded)))
                    }
                    None => {
                        self.discarded += pending.len();
                        self.clear();
                        None
                    }
                };
            }
            match newline {
                Some(i) => {
                    let line = pending[..i].strip_suffix(b"\r").unwrap_or(&pending[..i]);
                    let frame = if line.is_empty() {
                        None
                    } else if line.len() > self.max {
                        Some(Frame::Oversized(i))
                    } else {
                        Some(Frame::Line(line.to_vec()))
                    };
                    self.consume(i + 1);
                    if frame.is_some() {
                        return frame;
                    }
                }
                None => {
                    // One extra byte is allowed for a `\r` that is stripped once the `\n` arrives.
                    if pending.len() > self.max + 1 {
                        self.discarded = pending.len();
                        self.discarding = true;
                        self.clear();
                    } else {
                        self.scanned = pending.len();
                    }
                    return None;
                }
            }
        }
    }

    /// Bytes of an unterminated message currently held or being discarded.
    pub fn pending(&self) -> usize {
        self.discarded + (self.buf.len() - self.start)
    }

    fn consume(&mut self, n: usize) {
        self.start += n;
        self.scanned = 0;
    }

    fn clear(&mut self) {
        self.buf.clear();
        self.start = 0;
        self.scanned = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn collect(framer: &mut Framer) -> Vec<Result<String, usize>> {
        let mut out = Vec::new();
        while let Some(f) = framer.next_frame() {
            out.push(match f {
                Frame::Line(l) => Ok(String::from_utf8(l).unwrap()),
                Frame::Oversized(n) => Err(n),
            });
        }
        out
    }

    #[test]
    fn splits_lines_across_pushes() {
        let mut f = Framer::new(100);
        f.push(b"{\"a\":1}\n{\"b\"");
        assert_eq!(collect(&mut f), vec![Ok("{\"a\":1}".into())]);
        assert_eq!(f.pending(), 4);
        f.push(b":2}\r\n\n\r\nlast");
        assert_eq!(collect(&mut f), vec![Ok("{\"b\":2}".into())]);
        assert_eq!(f.pending(), 4);
    }

    #[test]
    fn oversized_lines_are_discarded_until_newline() {
        let mut f = Framer::new(5);
        f.push(b"123456");
        assert!(collect(&mut f).is_empty());
        assert_eq!(f.pending(), 6);
        f.push(b"789\nok\n");
        assert_eq!(collect(&mut f), vec![Err(9), Ok("ok".into())]);
        f.push(b"12345\n123456\n");
        assert_eq!(collect(&mut f), vec![Ok("12345".into()), Err(6)]);
        assert_eq!(f.pending(), 0);
    }

    #[test]
    fn crlf_at_the_limit_is_accepted_regardless_of_chunking() {
        let mut f = Framer::new(5);
        f.push(b"12345\r");
        assert!(collect(&mut f).is_empty());
        f.push(b"\n");
        assert_eq!(collect(&mut f), vec![Ok("12345".into())]);
        f.push(b"123456\r");
        assert!(collect(&mut f).is_empty());
        f.push(b"\nx\n");
        assert_eq!(collect(&mut f), vec![Err(7), Ok("x".into())]);
    }

    #[test]
    fn compaction_keeps_unconsumed_tail() {
        let mut f = Framer::new(1000);
        for i in 0..100 {
            f.push(format!("line{i}\n").as_bytes());
            assert_eq!(collect(&mut f), vec![Ok(format!("line{i}"))]);
        }
        f.push(b"partial");
        f.push(b"-end\n");
        assert_eq!(collect(&mut f), vec![Ok("partial-end".into())]);
        assert!(f.buf.len() < 64);
    }
}
