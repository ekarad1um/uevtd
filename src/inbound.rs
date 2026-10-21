//! Inbound: newline-delimited messages over a Unix domain socket
//! (ARCHITECTURE.md §4). Framing is a sans-IO state machine so its edge cases
//! are unit-testable.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::Duration;

use anyhow::Context;
use tokio::io::AsyncReadExt;
use tokio::net::UnixStream;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, watch};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, debug, error, info, warn};

use crate::config::{self, Config};
use crate::metrics::ErrorSlot;
use crate::storage::{Message, QueueSender};
use crate::{now_us, uds};

/// Bytes read from a connection at a time.
const READ_BUF: usize = 8 << 10;

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
    sink: QueueSender,
    metrics: Arc<Metrics>,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    let mut current = cfg.borrow_and_update().inbound.clone();
    let mut listener = uds::bind(&current.path, current.mode.0)
        .await
        .with_context(|| format!("binding inbound socket {}", current.path.display()))?;
    info!(path = %current.path.display(), "inbound listening");
    let mut limiter = Limiter::new(current.max_connections);
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
                limiter.reclaim();
                let Some(permit) = limiter.try_acquire() else {
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
                limiter.reclaim();
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
                    limiter.resize(next.max_connections);
                }
                current = rebind(&mut listener, current, next, &metrics).await;
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

/// The connection limit as a semaphore that is resized in place, so
/// connections accepted under the old limit keep counting: raising it adds
/// permits at once; lowering it forgets permits as connections close.
struct Limiter {
    semaphore: Arc<Semaphore>,
    max: usize,
    /// Permits still to be forgotten after the limit was lowered.
    deficit: usize,
}

impl Limiter {
    fn new(max: usize) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(max)),
            max,
            deficit: 0,
        }
    }

    fn try_acquire(&self) -> Option<OwnedSemaphorePermit> {
        self.semaphore.clone().try_acquire_owned().ok()
    }

    fn resize(&mut self, max: usize) {
        if max >= self.max {
            let grow = max - self.max;
            let cancelled = self.deficit.min(grow);
            self.deficit -= cancelled;
            self.semaphore.add_permits(grow - cancelled);
        } else {
            self.deficit += self.max - max;
        }
        self.max = max;
        self.reclaim();
    }

    /// Forgets permits owed by an earlier decrease; called whenever a permit
    /// may have been released.
    fn reclaim(&mut self) {
        if self.deficit > 0 {
            self.deficit -= self.semaphore.forget_permits(self.deficit);
        }
    }
}

/// Applies a socket path or mode change; on failure keeps the old socket and
/// records the error, so producers are never left without a listener.
async fn rebind(
    listener: &mut tokio::net::UnixListener,
    current: config::Inbound,
    next: config::Inbound,
    metrics: &Metrics,
) -> config::Inbound {
    if next.path != current.path {
        match uds::bind(&next.path, next.mode.0).await {
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
    sink: QueueSender,
    metrics: Arc<Metrics>,
    shutdown: CancellationToken,
    _permit: OwnedSemaphorePermit,
) {
    debug!("connected");
    let mut framer = Framer::new(max_line);
    let mut buf = vec![0u8; READ_BUF];
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
                debug!(pending = framer.pending(), "closing for shutdown");
                return;
            }
        };
        framer.push(&buf[..n]);
        while let Some(frame) = framer.next_frame() {
            match frame {
                Frame::Line(len) => {
                    let ts = now_us();
                    metrics.messages.fetch_add(1, Relaxed);
                    metrics.bytes.fetch_add(len as u64, Relaxed);
                    // The message is copied out of the framer only once the
                    // queue has room for it, so a connection never holds a
                    // message twice while it waits.
                    let Ok(permit) = sink.reserve(len).await else {
                        return;
                    };
                    permit.send(Message {
                        ts,
                        payload: framer.take_line(),
                    });
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
    /// A complete line of this many bytes, terminator excluded, is next;
    /// [`Framer::take_line`] returns it.
    Line(usize),
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
    /// A line at `start` that was announced but not taken yet: its length,
    /// and the bytes it occupies with its terminator.
    ready: Option<(usize, usize)>,
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
            ready: None,
        }
    }

    /// Appends received bytes. Consumed bytes are reclaimed before the buffer
    /// grows, and it never grows beyond one message plus this chunk, which is
    /// the most it has to hold once the previous chunk has been framed.
    pub fn push(&mut self, data: &[u8]) {
        let needed = self.buf.len() + data.len();
        if self.start > 0 && (self.start >= self.buf.len() / 2 || needed > self.buf.capacity()) {
            self.buf.drain(..self.start);
            self.start = 0;
        }
        let needed = self.buf.len() + data.len();
        if needed > self.buf.capacity() {
            let most = (self.max + 1).saturating_add(data.len()).max(needed);
            let target = (self.buf.capacity() * 2).clamp(needed, most);
            self.buf.reserve_exact(target - self.buf.len());
        }
        self.buf.extend_from_slice(data);
    }

    /// The next frame. A line is announced by its length and stays in the
    /// buffer, announced again by every call, until [`Framer::take_line`].
    pub fn next_frame(&mut self) -> Option<Frame> {
        if let Some((len, _)) = self.ready {
            return Some(Frame::Line(len));
        }
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
                    let len = pending[..i].strip_suffix(b"\r").map_or(i, <[u8]>::len);
                    if len == 0 {
                        self.consume(i + 1);
                    } else if len > self.max {
                        self.consume(i + 1);
                        return Some(Frame::Oversized(i));
                    } else {
                        self.ready = Some((len, i + 1));
                        return Some(Frame::Line(len));
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

    /// Copies out the line announced by [`Framer::next_frame`] and consumes it.
    pub fn take_line(&mut self) -> Vec<u8> {
        let (len, consumed) = self.ready.take().expect("a line was announced");
        let line = self.buf[self.start..self.start + len].to_vec();
        self.consume(consumed);
        line
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
                Frame::Line(_) => Ok(String::from_utf8(framer.take_line()).unwrap()),
                Frame::Oversized(n) => Err(n),
            });
        }
        out
    }

    #[test]
    fn an_announced_line_stays_until_taken() {
        let mut f = Framer::new(100);
        f.push(b"ab\r\ncd\n");
        assert!(matches!(f.next_frame(), Some(Frame::Line(2))));
        assert!(matches!(f.next_frame(), Some(Frame::Line(2))));
        // More data may arrive while a line waits.
        f.push(b"ef\n");
        assert_eq!(f.take_line(), b"ab");
        assert_eq!(collect(&mut f), vec![Ok("cd".into()), Ok("ef".into())]);
        assert_eq!(f.pending(), 0);
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
    fn limiter_resizes_without_losing_track_of_open_connections() {
        let mut limiter = Limiter::new(3);
        let held: Vec<_> = (0..3).map(|_| limiter.try_acquire().unwrap()).collect();
        assert!(limiter.try_acquire().is_none());

        // Lowering to 1 with 3 open: nothing is admitted until two close.
        limiter.resize(1);
        let mut held = held.into_iter();
        for _ in 0..2 {
            drop(held.next());
            limiter.reclaim();
            assert!(limiter.try_acquire().is_none());
        }
        drop(held.next());
        limiter.reclaim();
        let one = limiter.try_acquire().unwrap();
        assert!(limiter.try_acquire().is_none());

        // Raising again while permits are still owed cancels the debt first.
        limiter.resize(3);
        let two = limiter.try_acquire().unwrap();
        let three = limiter.try_acquire().unwrap();
        assert!(limiter.try_acquire().is_none());
        drop((one, two, three));
        limiter.reclaim();
        assert_eq!(limiter.semaphore.available_permits(), 3);
    }

    #[test]
    fn buffer_holds_at_most_one_message_and_one_read() {
        let max = 1000;
        let chunk = 64;
        let mut stream = Vec::new();
        for len in [1, 10, 500, 999, 1000, 1001, 3000, 7, 1000] {
            stream.extend(std::iter::repeat_n(b'x', len));
            stream.push(b'\n');
        }
        let mut f = Framer::new(max);
        let mut frames = 0;
        for piece in stream.chunks(chunk) {
            f.push(piece);
            frames += collect(&mut f).len();
            assert!(
                f.buf.capacity() <= max + 1 + chunk,
                "capacity {}",
                f.buf.capacity()
            );
        }
        assert_eq!(frames, 9);
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
