//! Inbound: newline-delimited messages read from the producer's Unix domain
//! socket, which inbound connects to. Framing is a sans-IO state machine so its
//! edge cases are unit-testable.

use std::fs;
use std::io;
use std::os::unix::fs::FileTypeExt;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::time::Duration;

use tokio::io::AsyncReadExt;
use tokio::net::UnixStream;
use tokio::sync::watch;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, debug, info, warn};

use crate::config::{self, Config};
use crate::metrics::ErrorSlot;
use crate::now_us;
use crate::storage::{Message, QueueSender};

/// Bytes read from the connection at a time.
const READ_BUF: usize = 8 << 10;
/// The wait before connecting again after a failure: 100 ms, doubling to
/// 1 s. A connection to a local socket costs little to try, and the most is
/// how long a producer that is back may wait to be read.
const RETRY_BASE: Duration = Duration::from_millis(100);
const RETRY_MAX: Duration = Duration::from_secs(1);
/// A connection the producer's side has not completed by then fails. Linux
/// and macOS complete or refuse one at once; this is only a bound.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(1);
/// How a connection that ended before anything arrived on it failed.
const CLOSED_EARLY: &str = "the producer closed the connection before sending anything";

#[derive(Default)]
pub struct Metrics {
    pub connected: AtomicBool,
    /// Connections made to the producer.
    pub connections: AtomicU64,
    pub messages: AtomicU64,
    pub bytes: AtomicU64,
    pub truncated: AtomicU64,
    pub oversized: AtomicU64,
    pub last_error: ErrorSlot,
}

/// Reads the producer's socket at `inbound.path` until `shutdown`, over one
/// connection at a time, made again whenever it ends. Nothing at the path is
/// ever created, changed or removed, and nothing is written to the socket.
pub async fn run(
    mut cfg: watch::Receiver<Arc<Config>>,
    sink: QueueSender,
    metrics: Arc<Metrics>,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    let mut current = cfg.borrow_and_update().inbound.clone();
    info!(path = %current.path.display(), "inbound connecting to the producer");
    let mut retry = Retry::default();
    let mut id = 0u64;
    loop {
        if let Some(after) = retry.after {
            tokio::select! {
                _ = tokio::time::sleep_until(after) => {}
                changed = cfg.changed() => {
                    if changed.is_err() {
                        break;
                    }
                    let next = cfg.borrow_and_update().inbound.clone();
                    // Another producer is tried at once.
                    if next.path != current.path {
                        retry.reset();
                    }
                    current = next;
                    continue;
                }
                _ = shutdown.cancelled() => break,
            }
        }
        let connected = tokio::select! {
            connected = connect(&current.path) => connected,
            _ = shutdown.cancelled() => break,
        };
        let stream = match connected {
            Ok(stream) => stream,
            Err(e) => {
                retry.failed(format!("connect {}: {e}", current.path.display()), &metrics);
                continue;
            }
        };
        id += 1;
        metrics.connections.fetch_add(1, Relaxed);
        metrics.last_error.clear();
        metrics.connected.store(true, Relaxed);
        let span = tracing::info_span!("conn", id);
        // While connections end before anything arrives, the failure they
        // end in is what is logged of each.
        if retry.closing_early() {
            debug!(parent: &span, path = %current.path.display(), "connected to the producer");
        } else {
            info!(parent: &span, path = %current.path.display(), "connected to the producer");
        }
        let end = serve(stream, &mut cfg, &mut current, &sink, &metrics, &shutdown)
            .instrument(span)
            .await;
        metrics.connected.store(false, Relaxed);
        let read_error = |e: io::Error| format!("read {}: {e}", current.path.display());
        match end {
            End::Closed {
                received: true,
                error: None,
            } => {
                info!("the producer closed the connection");
                retry.reset();
            }
            End::Closed {
                received: true,
                error: Some(e),
            } => {
                warn!(error = %e, "reading from the producer failed");
                metrics.last_error.set(read_error(e));
                retry.reset();
            }
            End::Closed {
                received: false,
                error,
            } => {
                let error = error.map_or_else(|| CLOSED_EARLY.into(), read_error);
                retry.failed(error, &metrics);
            }
            End::Moved => retry.reset(),
            End::Stopped => break,
        }
    }
    metrics.connected.store(false, Relaxed);
    info!("inbound stopped");
    Ok(())
}

/// The connections that failed since data last arrived, which the wait
/// before the next one grows with, and the failure logged last, so that one
/// that repeats is logged at `debug`.
#[derive(Default)]
struct Retry {
    failures: u32,
    /// When the next connection may be made, after a failure. A deadline,
    /// so that configurations published meanwhile do not put it off.
    after: Option<Instant>,
    logged: Option<String>,
}

impl Retry {
    fn failed(&mut self, error: String, metrics: &Metrics) {
        self.failures = self.failures.saturating_add(1);
        let doublings = (self.failures - 1).min(16);
        let delay = RETRY_BASE.saturating_mul(1 << doublings).min(RETRY_MAX);
        self.after = Some(Instant::now() + delay);
        if self.logged.as_ref() == Some(&error) {
            debug!(%error, "cannot read from the producer; retrying");
        } else {
            warn!(%error, "cannot read from the producer; retrying");
        }
        metrics.last_error.set(&error);
        self.logged = Some(error);
    }

    /// Data arrived, or another producer is to be read: the next connection
    /// is made at once, and its failure is logged.
    fn reset(&mut self) {
        *self = Self::default();
    }

    fn closing_early(&self) -> bool {
        self.logged.as_deref() == Some(CLOSED_EARLY)
    }
}

/// Connects to the producer's socket at `path`.
async fn connect(path: &Path) -> io::Result<UnixStream> {
    let error = match tokio::time::timeout(CONNECT_TIMEOUT, UnixStream::connect(path)).await {
        Ok(Ok(stream)) => return Ok(stream),
        Ok(Err(e)) => e,
        Err(_) => io::Error::new(
            io::ErrorKind::TimedOut,
            format!("not accepted within {CONNECT_TIMEOUT:?}"),
        ),
    };
    // Linux refuses a connection to a file that is not a socket as it does
    // one to a socket that nothing listens on; this tells them apart.
    match fs::metadata(path) {
        Ok(meta) if !meta.file_type().is_socket() => {
            Err(io::Error::new(io::ErrorKind::InvalidInput, "not a socket"))
        }
        _ => Err(error),
    }
}

/// How a connection ended.
enum End {
    /// The producer closed it, or reading from it failed with `error`;
    /// `received` tells whether anything arrived on it.
    Closed {
        received: bool,
        error: Option<io::Error>,
    },
    /// `inbound.path` changed.
    Moved,
    /// The daemon is stopping.
    Stopped,
}

/// Reads one connection, applying the configuration as it changes.
async fn serve(
    mut stream: UnixStream,
    cfg: &mut watch::Receiver<Arc<Config>>,
    current: &mut config::Inbound,
    sink: &QueueSender,
    metrics: &Metrics,
    shutdown: &CancellationToken,
) -> End {
    let mut framer = Framer::new(current.max_message_bytes);
    let mut buf = vec![0u8; READ_BUF];
    let mut received = false;
    let end = loop {
        let n = tokio::select! {
            read = stream.read(&mut buf) => match read {
                Ok(0) => break End::Closed { received, error: None },
                Ok(n) => n,
                Err(e) => break End::Closed { received, error: Some(e) },
            },
            changed = cfg.changed() => {
                if changed.is_err() {
                    return End::Stopped;
                }
                let next = cfg.borrow_and_update().inbound.clone();
                // A new limit applies at once, to the message in progress
                // too (`Framer::set_max`).
                if next.max_message_bytes != current.max_message_bytes {
                    framer.set_max(next.max_message_bytes);
                }
                let moved = next.path != current.path;
                *current = next;
                if moved {
                    info!(path = %current.path.display(), "inbound path changed; closing the connection");
                    break End::Moved;
                }
                continue;
            }
            _ = shutdown.cancelled() => {
                debug!(pending = framer.pending(), "closing for shutdown");
                return End::Stopped;
            }
        };
        received = true;
        framer.push(&buf[..n]);
        while let Some(frame) = framer.next_frame() {
            match frame {
                Frame::Line(len) => {
                    let ts = now_us();
                    metrics.messages.fetch_add(1, Relaxed);
                    metrics.bytes.fetch_add(len as u64, Relaxed);
                    // The message is copied out of the framer only once the
                    // queue has room for it, so the connection never holds
                    // a message twice while it waits. Nothing is read
                    // meanwhile, so the producer's writes wait too.
                    let Ok(permit) = sink.reserve(len).await else {
                        // The writer is gone, which stops the daemon. Ending
                        // first would make inbound the reported cause.
                        shutdown.cancelled().await;
                        return End::Stopped;
                    };
                    permit.send(Message {
                        ts,
                        payload: framer.take_line(),
                    });
                }
                Frame::Oversized(bytes) => {
                    metrics.oversized.fetch_add(1, Relaxed);
                    warn!(
                        bytes,
                        max = current.max_message_bytes,
                        "discarding oversized message"
                    );
                }
            }
        }
    };
    let pending = framer.pending();
    if pending > 0 {
        metrics.truncated.fetch_add(1, Relaxed);
        warn!(
            bytes = pending,
            "connection closed mid-message; discarding truncated message"
        );
    }
    end
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

    /// Holds what follows to the limit `max`, higher or lower, the line in
    /// progress included: one that is longer already is discarded, and one
    /// that is being discarded stays so. The buffer gives back what it kept
    /// for a higher limit.
    pub fn set_max(&mut self, max: usize) {
        self.max = max;
        let pending = self.buf.len() - self.start;
        // Only a line known to have no end yet; one that ends in what is
        // still to be scanned is measured when it is.
        if self.ready.is_none() && !self.discarding && self.scanned == pending && pending > max + 1
        {
            self.discarded = pending;
            self.discarding = true;
            self.clear();
        }
        self.buf.drain(..self.start);
        self.start = 0;
        self.buf.shrink_to(max + 1);
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
    use std::time::Instant;

    use tokio::io::AsyncWriteExt;
    use tokio::net::UnixListener;

    use super::*;
    use crate::testing::{Rng, seeds};

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

    async fn until(what: &str, mut done: impl FnMut() -> bool) {
        let deadline = Instant::now() + Duration::from_secs(10);
        while !done() {
            assert!(Instant::now() < deadline, "{what}");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Inbound reading the producer's socket at a path, what it queues, and
    /// its metrics.
    struct Reader {
        cfg: watch::Sender<Arc<Config>>,
        config: Config,
        queue: crate::storage::QueueReceiver,
        metrics: Arc<Metrics>,
        shutdown: CancellationToken,
        task: tokio::task::JoinHandle<anyhow::Result<()>>,
    }

    impl Reader {
        fn start(path: &Path) -> Reader {
            let mut config = Config::default();
            config.inbound.path = path.to_path_buf();
            let (cfg, rx) = watch::channel(Arc::new(config.clone()));
            let (sink, queue) = crate::storage::queue(16, 1 << 20);
            let metrics = Arc::new(Metrics::default());
            let shutdown = CancellationToken::new();
            let task = tokio::spawn(run(rx, sink, metrics.clone(), shutdown.clone()));
            Reader {
                cfg,
                config,
                queue,
                metrics,
                shutdown,
                task,
            }
        }

        async fn next(&mut self) -> Vec<u8> {
            let message = tokio::time::timeout(Duration::from_secs(10), self.queue.recv());
            message.await.expect("a message").unwrap().payload
        }

        fn error(&self) -> String {
            let error = self.metrics.last_error.get();
            error.map(|e| e.message).unwrap_or_default()
        }

        fn connected(&self) -> bool {
            self.metrics.connected.load(Relaxed)
        }

        fn change(&mut self, change: impl FnOnce(&mut config::Inbound)) {
            change(&mut self.config.inbound);
            self.cfg.send_replace(Arc::new(self.config.clone()));
        }

        async fn stop(self) {
            self.shutdown.cancel();
            let stopped = tokio::time::timeout(Duration::from_secs(5), self.task);
            stopped.await.expect("inbound stops").unwrap().unwrap();
        }
    }

    /// The next connection inbound made to `producer`.
    async fn accept(producer: &UnixListener) -> UnixStream {
        let accepted = tokio::time::timeout(Duration::from_secs(10), producer.accept());
        accepted.await.expect("inbound connects").unwrap().0
    }

    #[tokio::test]
    async fn reads_the_producer_once_it_listens_and_again_after_it_restarts() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("in.sock");
        let mut reader = Reader::start(&path);
        until("no producer", || reader.error().starts_with("connect")).await;
        assert!(!reader.connected());

        let producer = UnixListener::bind(&path).unwrap();
        let mut conn = accept(&producer).await;
        until("connected", || reader.connected()).await;
        assert_eq!(reader.error(), "");
        conn.write_all(b"one\ntwo\npart").await.unwrap();
        assert_eq!(reader.next().await, b"one");
        assert_eq!(reader.next().await, b"two");

        // The producer stops in the middle of a message, which is discarded.
        drop((conn, producer));
        std::fs::remove_file(&path).unwrap();
        until("truncated", || reader.metrics.truncated.load(Relaxed) == 1).await;
        until("producer gone", || reader.error().starts_with("connect")).await;
        assert!(!reader.connected());

        // And starts again.
        let producer = UnixListener::bind(&path).unwrap();
        let mut conn = accept(&producer).await;
        conn.write_all(b"three\n").await.unwrap();
        assert_eq!(reader.next().await, b"three");
        let metrics = &reader.metrics;
        let counts = (
            metrics.connections.load(Relaxed),
            metrics.messages.load(Relaxed),
        );
        assert_eq!(counts, (2, 3));

        reader.stop().await;
        // Inbound closed its end and left the producer's socket alone.
        assert_eq!(conn.read(&mut [0; 1]).await.unwrap(), 0);
        let meta = std::fs::symlink_metadata(&path).unwrap();
        assert!(meta.file_type().is_socket());
    }

    #[tokio::test]
    async fn a_producer_that_closes_at_once_is_tried_ever_less_often() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("in.sock");
        let producer = UnixListener::bind(&path).unwrap();
        let mut reader = Reader::start(&path);
        let mut times = Vec::new();
        while times.len() < 8 {
            drop(accept(&producer).await);
            times.push(Instant::now());
        }
        // 100 ms after the first, doubling to a second.
        let gaps: Vec<_> = times.windows(2).map(|w| w[1] - w[0]).collect();
        let ms: Vec<_> = gaps.iter().map(Duration::as_millis).collect();
        assert!(ms[0] >= 90 && ms[..4].is_sorted(), "{ms:?}");
        assert!(ms[4..].iter().all(|ms| (900..1300).contains(ms)), "{ms:?}");
        until("closed early", || reader.error() == CLOSED_EARLY).await;

        // Once a connection brings data, the next one is made at once...
        let mut conn = accept(&producer).await;
        conn.write_all(b"x\n").await.unwrap();
        assert_eq!(reader.next().await, b"x");
        drop(conn);
        let closed = Instant::now();
        let mut conn = accept(&producer).await;
        let waited = closed.elapsed();
        assert!(waited < Duration::from_millis(500), "{waited:?}");
        // ...and the waits start over: a producer that restarts then is read
        // again within the first of them, not a second later.
        conn.write_all(b"y\n").await.unwrap();
        assert_eq!(reader.next().await, b"y");
        drop((conn, producer));
        std::fs::remove_file(&path).unwrap();
        tokio::time::sleep(Duration::from_millis(250)).await;
        let producer = UnixListener::bind(&path).unwrap();
        let back = Instant::now();
        let _conn = accept(&producer).await;
        let waited = back.elapsed();
        assert!(waited < Duration::from_millis(500), "{waited:?}");
        reader.stop().await;
    }

    #[tokio::test]
    async fn configurations_published_meanwhile_do_not_put_off_a_connection() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("in.sock");
        let mut reader = Reader::start(&path);
        // Without the producer until the wait is at its most, a second.
        tokio::time::sleep(Duration::from_millis(1600)).await;
        let producer = UnixListener::bind(&path).unwrap();
        let listening = Instant::now();
        let accepted = producer.accept();
        tokio::pin!(accepted);
        loop {
            tokio::select! {
                accepted = &mut accepted => {
                    accepted.unwrap();
                    break;
                }
                // The same configuration again, as a change to another
                // section publishes it.
                _ = tokio::time::sleep(Duration::from_millis(50)) => reader.change(|_| {}),
            };
            assert!(
                listening.elapsed() < Duration::from_secs(3),
                "never connected"
            );
        }
        let waited = listening.elapsed();
        assert!(waited < Duration::from_millis(1500), "{waited:?}");
        reader.stop().await;
    }

    #[tokio::test]
    async fn a_new_path_is_read_from_its_producer_at_once() {
        let dir = tempfile::tempdir().unwrap();
        let (a, b) = (dir.path().join("a.sock"), dir.path().join("b.sock"));
        let (first, second) = (
            UnixListener::bind(&a).unwrap(),
            UnixListener::bind(&b).unwrap(),
        );
        let mut reader = Reader::start(&a);
        let mut old = accept(&first).await;
        old.write_all(b"a\npart").await.unwrap();
        assert_eq!(reader.next().await, b"a");

        reader.change(|inbound| inbound.path = b.clone());
        // The old connection is closed, with its partial message discarded.
        let mut byte = [0; 1];
        let closed = tokio::time::timeout(Duration::from_secs(5), old.read(&mut byte));
        assert_eq!(closed.await.expect("closed").unwrap(), 0);
        let mut new = accept(&second).await;
        new.write_all(b"b\n").await.unwrap();
        assert_eq!(reader.next().await, b"b");
        assert_eq!(reader.metrics.truncated.load(Relaxed), 1);
        assert_eq!(reader.metrics.connections.load(Relaxed), 2);
        reader.stop().await;
        assert!(a.exists() && b.exists());
    }

    #[tokio::test]
    async fn a_new_path_is_tried_at_once_while_the_old_one_fails() {
        let dir = tempfile::tempdir().unwrap();
        let (a, b) = (dir.path().join("a.sock"), dir.path().join("b.sock"));
        let producer = UnixListener::bind(&b).unwrap();
        let mut reader = Reader::start(&a);
        // Nothing at `a` until the wait is at its most, a second; then the
        // path changes just after a failure.
        tokio::time::sleep(Duration::from_millis(1600)).await;
        let failed = reader.metrics.last_error.get().unwrap().ts;
        until("failed again", || {
            let error = reader.metrics.last_error.get();
            error.is_some_and(|e| e.ts > failed)
        })
        .await;
        reader.change(|inbound| inbound.path = b.clone());
        let changed = Instant::now();
        let _conn = accept(&producer).await;
        let waited = changed.elapsed();
        assert!(waited < Duration::from_millis(300), "{waited:?}");
        reader.stop().await;
    }

    #[tokio::test]
    async fn waits_to_be_stopped_once_the_writer_is_gone() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("in.sock");
        let producer = UnixListener::bind(&path).unwrap();
        let mut reader = Reader::start(&path);
        let mut conn = accept(&producer).await;
        // The writer goes, as when it panics, and takes the queue with it.
        let (_, other) = crate::storage::queue(1, 1);
        drop(std::mem::replace(&mut reader.queue, other));
        conn.write_all(b"x\n").await.unwrap();
        until("read", || reader.metrics.messages.load(Relaxed) == 1).await;
        // Inbound does not end by itself, so that the daemon stops for the
        // writer and names it as the cause (daemon.rs).
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(!reader.task.is_finished());
        reader.stop().await;
        assert_eq!(conn.read(&mut [0; 1]).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn a_file_that_is_not_a_socket_is_named_so_and_left_alone() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("in.sock");
        std::fs::write(&path, b"x").unwrap();
        let reader = Reader::start(&path);
        until("refused", || reader.error().ends_with(": not a socket")).await;
        reader.stop().await;
        assert_eq!(std::fs::read(&path).unwrap(), b"x");
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
    fn a_new_limit_applies_to_the_line_in_progress() {
        let mut f = Framer::new(100);
        f.push(b"0123456789");
        assert!(collect(&mut f).is_empty());
        // Longer than a lower limit already: discarded, and so is the rest
        // of it, and the buffer shrinks to the new limit.
        f.set_max(5);
        assert!(f.buf.capacity() <= 6, "capacity {}", f.buf.capacity());
        assert_eq!(f.pending(), 10);
        // A higher limit does not bring back what was discarded.
        f.set_max(100);
        f.push(b"abc\nok\n");
        assert_eq!(collect(&mut f), vec![Err(13), Ok("ok".into())]);
        // Within a lower limit: kept, and measured against it when it ends.
        f.push(b"1234");
        assert!(collect(&mut f).is_empty());
        f.set_max(4);
        f.push(b"\n12345\n");
        assert_eq!(collect(&mut f), vec![Ok("1234".into()), Err(5)]);
        // A higher limit holds the line in progress to it.
        f.push(b"123");
        assert!(collect(&mut f).is_empty());
        f.set_max(8);
        f.push(b"45678\n123456789\n");
        assert_eq!(collect(&mut f), vec![Ok("12345678".into()), Err(9)]);
        assert_eq!(f.pending(), 0);
    }

    #[derive(Debug, PartialEq)]
    enum Out {
        Line(Vec<u8>),
        Over(usize),
    }

    /// What the framer must make of `stream`, pushed in pieces that end at
    /// `ends`, when its limit becomes `max` after the piece that ends at
    /// `at` for each `(at, max)` in `limits`: complete lines with one
    /// trailing `\r` stripped, empty ones ignored, and those over the limit
    /// reported by their length on the wire; and the bytes of the
    /// unterminated tail. A line is over the limit if it is longer when its
    /// newline arrives, or if what had arrived of it at the end of a piece
    /// before then was longer than the limit and a byte for a `\r`, the
    /// limit as that piece was framed or as it became after.
    fn reference(stream: &[u8], ends: &[usize], limits: &[(usize, usize)]) -> (Vec<Out>, usize) {
        // The limit that the byte at `i` is framed under.
        let limit = |i: usize| limits.iter().rev().find(|(at, _)| *at <= i).unwrap().1;
        let mut out = Vec::new();
        let mut start = 0;
        for (i, _) in stream.iter().enumerate().filter(|(_, b)| **b == b'\n') {
            let raw = &stream[start..i];
            let line = raw.strip_suffix(b"\r").unwrap_or(raw);
            let cut = ends
                .iter()
                .filter(|&&end| start < end && end <= i)
                .any(|&end| end - start > limit(end - 1).min(limit(end)) + 1);
            if cut || line.len() > limit(i) {
                out.push(Out::Over(raw.len()));
            } else if !line.is_empty() {
                out.push(Out::Line(line.to_vec()));
            }
            start = i + 1;
        }
        (out, stream.len() - start)
    }

    #[test]
    fn the_framer_matches_a_reference_at_every_chunking() {
        for seed in 0..seeds(20_000) {
            let mut rng = Rng::new(seed);
            let first = rng.pick(&[1usize, 2, 3, 4, 5, 8, 31, 64]);
            let mut stream = Vec::new();
            for _ in 0..rng.below(16) {
                let len = match rng.below(9) {
                    0 => 0,
                    1 => first.saturating_sub(1),
                    2 => first,
                    3 => first + 1,
                    4 => first + 2,
                    5 => 2 * first + rng.below(3) as usize,
                    6 => rng.below(3 * first as u64 + 3) as usize,
                    7 => 1,
                    _ => first + rng.below(2) as usize,
                };
                for _ in 0..len {
                    let byte = if rng.chance(15) {
                        b'\r'
                    } else {
                        b'a' + rng.below(26) as u8
                    };
                    stream.push(byte);
                }
                if rng.chance(40) {
                    stream.extend_from_slice(b"\r\n");
                } else {
                    stream.push(b'\n');
                }
            }
            if rng.chance(50) {
                stream.extend(std::iter::repeat_n(
                    b'x',
                    rng.below(3 * first as u64 + 3) as usize,
                ));
                if rng.chance(30) {
                    stream.push(b'\r');
                }
            }
            let mut f = Framer::new(first);
            let mut limits = vec![(0, first)];
            let mut ends = Vec::new();
            let mut got = Vec::new();
            let (mut i, mut biggest) = (0, 0);
            while i < stream.len() {
                let n = (1 + rng.below(3 * first as u64 + 4) as usize).min(stream.len() - i);
                biggest = biggest.max(n);
                f.push(&stream[i..i + n]);
                i += n;
                ends.push(i);
                while let Some(frame) = f.next_frame() {
                    match frame {
                        Frame::Line(len) => {
                            // Announced again until taken.
                            assert!(matches!(f.next_frame(), Some(Frame::Line(l)) if l == len));
                            let line = f.take_line();
                            assert_eq!(line.len(), len);
                            got.push(Out::Line(line));
                        }
                        Frame::Oversized(n) => got.push(Out::Over(n)),
                    }
                }
                if rng.chance(15) {
                    // Higher or lower.
                    let max = 1 + rng.below(3 * first as u64 + 3) as usize;
                    f.set_max(max);
                    limits.push((i, max));
                }
                let (_, max) = *limits.last().unwrap();
                assert!(
                    f.buf.capacity() <= max + 1 + biggest,
                    "seed {seed}: capacity {} for max {max} and chunks up to {biggest}",
                    f.buf.capacity()
                );
            }
            let (expected, tail) = reference(&stream, &ends, &limits);
            let text = String::from_utf8_lossy(&stream);
            assert_eq!(
                got, expected,
                "seed {seed} limits {limits:?} stream {text:?}"
            );
            assert_eq!(f.pending(), tail, "seed {seed} limits {limits:?}");
        }
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
