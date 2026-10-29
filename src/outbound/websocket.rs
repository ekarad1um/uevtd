//! WebSocket channel: records stream as text frames to a receiver that
//! acknowledges them by sequence number. The cursor follows the answered
//! prefix, and unanswered frames are sent again after a reconnect.

use std::collections::VecDeque;
use std::io;
use std::net::IpAddr;
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, OwnedFd};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};
use std::task::{Context, Poll, ready};
use std::time::Duration;

use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_rustls::TlsConnector;
use tokio_rustls::client::TlsStream;
use tokio_rustls::rustls::pki_types::ServerName;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::handshake::client::Request;
use tokio_tungstenite::tungstenite::http::{HeaderName, HeaderValue};
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::protocol::{CloseFrame, WebSocketConfig};
use tokio_tungstenite::tungstenite::{Error as WsError, Message, Utf8Bytes};
use tokio_util::either::Either;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};
use url::{Host, Url};

use super::notice::{self, Notice};
use super::{ChannelMetrics, CursorStore, DELIVERY_BYTES, Delivery, Feed, pause};
use crate::storage::Record;
use crate::{config, tls, view};

const FLUSH_INTERVAL: Duration = Duration::from_secs(1);
/// The receiver sends only acknowledgements; a larger message ends the
/// session.
const MAX_INCOMING_BYTES: usize = 64 << 10;
/// How long a stopping channel waits for its close frame to be written.
const CLOSE_WAIT: Duration = Duration::from_secs(1);
/// The longest excerpt of the receiver's text kept in logs and errors.
const EXCERPT_BYTES: usize = 200;

type Socket = WebSocketStream<Either<Counted, TlsStream<Counted>>>;

pub async fn run(
    cfg: config::WebSocket,
    feed: Feed,
    cursor: CursorStore,
    metrics: Arc<ChannelMetrics>,
    token: CancellationToken,
) {
    info!(url = %config::redacted_url(&cfg.url), "forwarding over websocket");
    let mut flush = tokio::time::interval(FLUSH_INTERVAL);
    flush.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut channel = Channel {
        window: Window::new(cursor.current()),
        cfg,
        feed,
        cursor,
        metrics,
        flush,
        attempt: 0,
    };
    loop {
        let connected = tokio::select! {
            connected = connect(&channel.cfg) => connected,
            _ = token.cancelled() => break,
        };
        let failure = match connected {
            Ok((socket, progress)) => match channel.session(socket, &progress, &token).await {
                Ok(()) => break,
                Err(e) => e,
            },
            Err(e) => e,
        };
        warn!(error = %failure, outstanding = channel.window.outstanding(), "no session");
        channel.metrics.last_error.set(&failure);
        channel.metrics.retries.fetch_add(1, Relaxed);
        if !pause(channel.attempt, &token).await {
            break;
        }
        channel.attempt += 1;
    }
    channel.cursor.flush(true).await;
}

struct Channel {
    cfg: config::WebSocket,
    feed: Feed,
    cursor: CursorStore,
    metrics: Arc<ChannelMetrics>,
    window: Window,
    flush: tokio::time::Interval,
    /// Sessions that ended since the cursor last advanced.
    attempt: u32,
}

impl Channel {
    /// Serves one connection until it fails, or until the channel is stopped,
    /// which returns `Ok`.
    async fn session(
        &mut self,
        socket: Socket,
        progress: &Progress,
        token: &CancellationToken,
    ) -> Result<(), String> {
        let (sink, mut incoming) = socket.split();
        // Frames wait here to be written: the unanswered ones sent again,
        // then new ones, never more than two windows, since a receiver
        // acknowledges only frames it has received in some session.
        let (tx, rx) = mpsc::channel(2 * usize::from(self.cfg.inflight) + 2);
        let mut writer = tokio::spawn(write(sink, rx));
        self.metrics.connected.store(true, Relaxed);
        self.metrics.last_error.clear();
        info!(outstanding = self.window.outstanding(), "connected");
        let result = self
            .serve(&mut incoming, &tx, &mut writer, progress, token)
            .await;
        self.metrics.connected.store(false, Relaxed);
        writer.abort();
        result
    }

    async fn serve(
        &mut self,
        incoming: &mut SplitStream<Socket>,
        tx: &mpsc::Sender<Outgoing>,
        writer: &mut JoinHandle<Result<(), WsError>>,
        progress: &Progress,
        token: &CancellationToken,
    ) -> Result<(), String> {
        for body in self.window.unanswered() {
            queue(tx, Outgoing::Frame(body))?;
        }
        let limit = usize::from(self.cfg.inflight);
        let keep_alive = Duration::from_secs(self.cfg.keep_alive_s);
        let timeout = Duration::from_millis(self.cfg.timeout_ms);
        let mut ping = tokio::time::interval_at(Instant::now() + keep_alive, keep_alive);
        ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // While a ping waits for an answer: when the session must have shown
        // progress, and what the receiver's TCP had acknowledged before.
        let mut unanswered: Option<(Instant, Option<u64>)> = None;
        loop {
            // Bounded by count and by payload bytes, like the MQTT window.
            let room = limit.saturating_sub(self.window.outstanding());
            let bytes = DELIVERY_BYTES.saturating_sub(self.window.bytes());
            let due = unanswered.map(|(due, _)| due);
            tokio::select! {
                message = incoming.next() => {
                    // Anything from the receiver answers a ping. On a slow
                    // link the pong comes only after the frames sent before
                    // the ping, while acknowledgements come all along.
                    unanswered = None;
                    match message {
                        Some(Ok(Message::Text(text))) => self.answer(&text),
                        Some(Ok(Message::Close(frame))) => return Err(closed(frame)),
                        // Binary frames are ignored; pings are answered by
                        // the library.
                        Some(Ok(_)) => {}
                        Some(Err(e)) => return Err(e.to_string()),
                        None => return Err("connection closed".into()),
                    }
                }
                delivery = self.feed.next(room, bytes), if room > 0 && bytes > 0 => {
                    self.submit(delivery, tx)?;
                }
                _ = ping.tick(), if unanswered.is_none() => {
                    queue(tx, Outgoing::Ping)?;
                    unanswered = Some((Instant::now() + timeout, progress.acked()));
                }
                _ = tokio::time::sleep_until(due.unwrap_or_else(Instant::now)), if due.is_some() => {
                    // Data that the receiver's TCP acknowledged shows that the
                    // link moves, if slowly: a frame can take longer to send
                    // than the timeout, and its answer comes after it.
                    let acked = progress.acked();
                    match unanswered {
                        Some((_, Some(before))) if acked.is_some_and(|now| now > before) => {
                            unanswered = Some((Instant::now() + timeout, acked));
                        }
                        _ => {
                            return Err(format!(
                                "no answer to a ping within {} ms, and nothing sent got through meanwhile",
                                self.cfg.timeout_ms
                            ));
                        }
                    }
                }
                _ = self.flush.tick() => self.cursor.flush(false).await,
                written = &mut *writer => return Err(match written {
                    Ok(Ok(())) => "connection closed".into(),
                    Ok(Err(e)) => e.to_string(),
                    Err(e) => format!("writer: {e}"),
                }),
                _ = token.cancelled() => {
                    if tx.try_send(Outgoing::Close).is_ok() {
                        let _ = tokio::time::timeout(CLOSE_WAIT, &mut *writer).await;
                    }
                    return Ok(());
                }
            }
        }
    }

    /// Queues what the feed delivered, one frame per record and per loss: a
    /// record, or a notice in place of a record too large to send or of
    /// records lost before delivery. Every frame enters the window even when
    /// queueing fails, since the feed has moved past it: the next session
    /// sends it.
    fn submit(&mut self, delivery: Delivery, tx: &mpsc::Sender<Outgoing>) -> Result<(), String> {
        let mut queued = Ok(());
        match delivery {
            Delivery::Records(records) => {
                for record in records {
                    let body = if frame_len(&record) > self.cfg.max_frame_bytes {
                        warn!(
                            seq = record.seq,
                            bytes = record.payload.len(),
                            max = self.cfg.max_frame_bytes,
                            "frame too large; sending a notice instead"
                        );
                        self.metrics
                            .last_error
                            .set(format!("seq {}: frame too large", record.seq));
                        let notice = Notice::Rejected {
                            seq: record.seq,
                            ts: record.ts,
                            reason: notice::TOO_LARGE,
                        };
                        self.window
                            .push_rejected(record.seq, notice.frame(record.seq))
                    } else {
                        self.window.push(record)
                    };
                    queued = queued.and_then(|()| queue(tx, Outgoing::Frame(body)));
                }
            }
            Delivery::Lost { from, to } => {
                info!(
                    from_seq = from,
                    to_seq = to,
                    "sending a notice for records lost before delivery"
                );
                let body = self
                    .window
                    .push_notice(to, Notice::lost(from, to).frame(to));
                queued = queue(tx, Outgoing::Frame(body));
            }
        }
        queued
    }

    /// Handles a text message from the receiver: an acknowledgement, a
    /// refusal, or anything else, which is ignored.
    fn answer(&mut self, text: &str) {
        match serde_json::from_str::<Reply>(text) {
            Ok(Reply::Ack { ack }) => {
                let now = crate::now_us();
                for answered in self.window.ack(ack) {
                    match answered {
                        Answered::Record { ts } => self.metrics.delivered_at(ts, now),
                        Answered::Notice => {
                            self.metrics.notices.fetch_add(1, Relaxed);
                        }
                    }
                }
            }
            Ok(Reply::Nack { nack, reason }) => {
                let reason = excerpt(&reason);
                match self.window.nack(nack) {
                    Some(Answered::Record { .. }) => {
                        warn!(seq = nack, reason, "refused by the receiver");
                        self.metrics
                            .last_error
                            .set(format!("seq {nack}: refused: {reason}"));
                    }
                    Some(Answered::Notice) => {
                        warn!(seq = nack, reason, "notice refused by the receiver");
                    }
                    None => debug!(seq = nack, "refusal of no unanswered frame"),
                }
            }
            Err(_) => debug!(text = excerpt(text), "ignoring a message from the receiver"),
        }
        for seq in self.window.take_rejections() {
            self.metrics.rejected_passed(seq);
        }
        // Progress, not a successful handshake, ends the backoff: a receiver
        // that accepts every connection and then drops it still waits longer
        // each time.
        if self.cursor.advance(self.window.cursor()) {
            self.attempt = 0;
        }
    }
}

/// A message from the receiver.
#[derive(Deserialize)]
#[serde(untagged)]
enum Reply {
    /// Every frame up to `ack` is processed.
    Ack { ack: u64 },
    /// Frame `nack` will never be accepted.
    Nack {
        nack: u64,
        #[serde(default)]
        reason: String,
    },
}

/// What the writer sends.
enum Outgoing {
    Frame(Body),
    Ping,
    Close,
}

fn queue(tx: &mpsc::Sender<Outgoing>, item: Outgoing) -> Result<(), String> {
    tx.try_send(item).map_err(|e| match e {
        TrySendError::Full(_) => "the receiver acknowledged frames it was never sent".into(),
        TrySendError::Closed(_) => "connection closed".into(),
    })
}

/// Owns the sending half, so a frame is never abandoned half-written when
/// the channel's loop moves on. Writes what is queued, then flushes; ends
/// when the queue closes or after a close frame.
async fn write(
    mut sink: SplitSink<Socket, Message>,
    mut queued: mpsc::Receiver<Outgoing>,
) -> Result<(), WsError> {
    while let Some(first) = queued.recv().await {
        let mut next = Some(first);
        while let Some(item) = next {
            let message = match item {
                Outgoing::Frame(Body::Record(record)) => Message::text(
                    serde_json::to_string(&view::record(&record)).expect("a record serializes"),
                ),
                Outgoing::Frame(Body::Notice(text)) => Message::Text(text),
                Outgoing::Ping => Message::Ping(Default::default()),
                Outgoing::Close => {
                    let away = CloseFrame {
                        code: CloseCode::Away,
                        reason: Utf8Bytes::from_static(""),
                    };
                    return sink.send(Message::Close(Some(away))).await;
                }
            };
            sink.feed(message).await?;
            next = queued.try_recv().ok();
        }
        sink.flush().await?;
    }
    Ok(())
}

/// Opens a session: TCP, TLS for `wss`, then the handshake, all within
/// `timeout_ms`.
async fn connect(cfg: &config::WebSocket) -> Result<(Socket, Progress), String> {
    let url = Url::parse(&cfg.url).map_err(|e| format!("url: {e}"))?;
    let request = handshake_request(cfg)?;
    let tls = if url.scheme() == "wss" {
        let ca_file = cfg.ca_file.clone();
        let config = tokio::task::spawn_blocking(move || tls::client_config(ca_file.as_deref()))
            .await
            .map_err(|e| format!("tls: {e}"))??;
        Some(TlsConnector::from(config))
    } else {
        None
    };
    let open = async {
        let host = url.host().ok_or("url has no host")?;
        let port = url.port_or_known_default().ok_or("url has no port")?;
        let tcp = match host {
            Host::Domain(name) => TcpStream::connect((name, port)).await,
            Host::Ipv4(ip) => TcpStream::connect((ip, port)).await,
            Host::Ipv6(ip) => TcpStream::connect((ip, port)).await,
        }
        .map_err(|e| format!("connect: {e}"))?;
        tcp.set_nodelay(true).map_err(|e| format!("connect: {e}"))?;
        let written = Arc::new(AtomicU64::new(0));
        let progress = Progress {
            written: written.clone(),
            socket: tcp
                .as_fd()
                .try_clone_to_owned()
                .map_err(|e| format!("connect: {e}"))?,
        };
        let tcp = Counted {
            stream: tcp,
            written,
        };
        let stream = match tls {
            Some(connector) => {
                let name = match host {
                    Host::Domain(name) => {
                        ServerName::try_from(name.to_string()).map_err(|e| format!("tls: {e}"))?
                    }
                    Host::Ipv4(ip) => ServerName::IpAddress(IpAddr::V4(ip).into()),
                    Host::Ipv6(ip) => ServerName::IpAddress(IpAddr::V6(ip).into()),
                };
                let tls = connector
                    .connect(name, tcp)
                    .await
                    .map_err(|e| format!("tls: {e}"))?;
                Either::Right(tls)
            }
            None => Either::Left(tcp),
        };
        let config = WebSocketConfig::default()
            .read_buffer_size(16 << 10)
            .max_message_size(Some(MAX_INCOMING_BYTES))
            .max_frame_size(Some(MAX_INCOMING_BYTES));
        let (socket, _) =
            tokio_tungstenite::client_async_with_config(request, stream, Some(config))
                .await
                .map_err(|e| format!("handshake: {e}"))?;
        Ok::<_, String>((socket, progress))
    };
    tokio::time::timeout(Duration::from_millis(cfg.timeout_ms), open)
        .await
        .map_err(|_| format!("no session within {} ms", cfg.timeout_ms))?
}

fn handshake_request(cfg: &config::WebSocket) -> Result<Request, String> {
    let mut request = cfg
        .url
        .as_str()
        .into_client_request()
        .map_err(|e| format!("url: {e}"))?;
    for (name, value) in &cfg.headers {
        let header =
            HeaderName::from_bytes(name.as_bytes()).map_err(|e| format!("header {name:?}: {e}"))?;
        let value = HeaderValue::from_str(value).map_err(|e| format!("header {name:?}: {e}"))?;
        request.headers_mut().insert(header, value);
    }
    Ok(request)
}

/// The TCP stream under a session. It counts the bytes the socket takes, so
/// that [`Progress`] can tell how many of them the receiver's TCP has
/// acknowledged.
struct Counted {
    stream: TcpStream,
    written: Arc<AtomicU64>,
}

impl AsyncRead for Counted {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_read(cx, buf)
    }
}

impl AsyncWrite for Counted {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let n = ready!(Pin::new(&mut self.stream).poll_write(cx, buf))?;
        self.written.fetch_add(n as u64, Release);
        Poll::Ready(Ok(n))
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let n = ready!(Pin::new(&mut self.stream).poll_write_vectored(cx, bufs))?;
        self.written.fetch_add(n as u64, Release);
        Poll::Ready(Ok(n))
    }

    fn is_write_vectored(&self) -> bool {
        self.stream.is_write_vectored()
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_shutdown(cx)
    }
}

/// How much of what a session sent the receiver's TCP has acknowledged. It
/// shows that a slow link still moves while no answer can come yet.
struct Progress {
    /// What [`Counted`] counts.
    written: Arc<AtomicU64>,
    /// The session's socket, for asking the system.
    socket: OwnedFd,
}

impl Progress {
    /// Bytes the receiver's TCP has acknowledged; `None` where the system does
    /// not tell. The bytes written are counted before the system is asked,
    /// so that a write in between can hide progress but never make it up.
    fn acked(&self) -> Option<u64> {
        let written = self.written.load(Acquire);
        Some(written.saturating_sub(unacked(self.socket.as_fd())?))
    }
}

/// Bytes that `socket` holds until its peer acknowledges them, sent or not.
#[cfg(target_os = "linux")]
fn unacked(socket: BorrowedFd<'_>) -> Option<u64> {
    let mut queued: libc::c_int = 0;
    // SAFETY: the descriptor is open, and the pointer is valid for the call.
    if unsafe { libc::ioctl(socket.as_raw_fd(), libc::TIOCOUTQ, &raw mut queued) } != 0 {
        return None;
    }
    u64::try_from(queued).ok()
}

/// Bytes that `socket` holds until its peer acknowledges them, sent or not.
#[cfg(target_os = "macos")]
fn unacked(socket: BorrowedFd<'_>) -> Option<u64> {
    let mut queued: libc::c_int = 0;
    let mut len = size_of::<libc::c_int>() as libc::socklen_t;
    // SAFETY: the descriptor is open, and the pointers are valid for the
    // call, `len` holding the size of what the other points to.
    let failed = unsafe {
        libc::getsockopt(
            socket.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_NWRITE,
            (&raw mut queued).cast(),
            &mut len,
        )
    } != 0;
    if failed {
        return None;
    }
    u64::try_from(queued).ok()
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn unacked(_: BorrowedFd<'_>) -> Option<u64> {
    None
}

fn closed(frame: Option<CloseFrame>) -> String {
    match frame {
        Some(f) if !f.reason.is_empty() => format!(
            "closed by the receiver: {} {}",
            u16::from(f.code),
            excerpt(&f.reason)
        ),
        Some(f) => format!("closed by the receiver: {}", u16::from(f.code)),
        None => "closed by the receiver".into(),
    }
}

fn excerpt(text: &str) -> &str {
    if text.len() <= EXCERPT_BYTES {
        return text;
    }
    let mut end = EXCERPT_BYTES;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    &text[..end]
}

/// The length of a record's frame, without building it.
fn frame_len(record: &Record) -> usize {
    struct Count(usize);
    impl io::Write for Count {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.0 += buf.len();
            Ok(buf.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    let mut count = Count(0);
    serde_json::to_writer(&mut count, &view::record(record)).expect("a record serializes");
    count.0
}

/// What a frame carries. Cheap to clone, so that the window and the writer
/// share it; a record is encoded only when it is written.
#[derive(Clone)]
enum Body {
    Record(Arc<Record>),
    Notice(Utf8Bytes),
}

impl Body {
    fn len(&self) -> usize {
        match self {
            Body::Record(record) => record.payload.len(),
            Body::Notice(text) => text.len(),
        }
    }
}

struct Entry {
    /// The last sequence number the frame stands for.
    seq: u64,
    /// `None` once the receiver answered.
    body: Option<Body>,
    /// The record is passed undelivered: the receiver refused it, or a
    /// notice is sent in its place, as too large.
    rejected: bool,
}

/// What an answered frame carried.
#[derive(Debug, PartialEq, Eq)]
enum Answered {
    Record { ts: i64 },
    Notice,
}

/// The frames sent and not yet answered, in sequence order. The cursor is
/// the end of the longest answered prefix.
struct Window {
    entries: VecDeque<Entry>,
    cursor: u64,
    /// Payload bytes held for unanswered frames.
    bytes: usize,
    /// The rejected records the cursor passed, until taken.
    rejections: Vec<u64>,
}

impl Window {
    fn new(cursor: u64) -> Self {
        Self {
            entries: VecDeque::new(),
            cursor,
            bytes: 0,
            rejections: Vec::new(),
        }
    }

    fn cursor(&self) -> u64 {
        self.cursor
    }

    fn outstanding(&self) -> usize {
        self.entries.len()
    }

    fn bytes(&self) -> usize {
        self.bytes
    }

    /// Adds a record's frame; returns it for sending.
    fn push(&mut self, record: Record) -> Body {
        let seq = record.seq;
        self.enqueue(seq, Body::Record(Arc::new(record)), false)
    }

    /// Adds a notice's frame, standing for the records up to `seq`.
    fn push_notice(&mut self, seq: u64, frame: String) -> Body {
        self.enqueue(seq, Body::Notice(frame.into()), false)
    }

    /// Adds the frame of a notice sent in place of record `seq`.
    fn push_rejected(&mut self, seq: u64, frame: String) -> Body {
        self.enqueue(seq, Body::Notice(frame.into()), true)
    }

    fn enqueue(&mut self, seq: u64, body: Body, rejected: bool) -> Body {
        self.bytes += body.len();
        self.entries.push_back(Entry {
            seq,
            body: Some(body.clone()),
            rejected,
        });
        body
    }

    /// The receiver processed every frame up to `seq`; returns the frames
    /// that answered, in order.
    fn ack(&mut self, seq: u64) -> Vec<Answered> {
        let answered = self
            .entries
            .iter_mut()
            .take_while(|e| e.seq <= seq)
            .filter_map(|entry| answer(entry, &mut self.bytes))
            .collect();
        self.advance();
        answered
    }

    /// The receiver will never accept the frame for `seq`.
    fn nack(&mut self, seq: u64) -> Option<Answered> {
        let entry = self.entries.iter_mut().find(|e| e.seq == seq)?;
        let answered = answer(entry, &mut self.bytes);
        if matches!(answered, Some(Answered::Record { .. })) {
            entry.rejected = true;
        }
        self.advance();
        answered
    }

    /// The frames to send again after a reconnect, in order.
    fn unanswered(&self) -> impl Iterator<Item = Body> + '_ {
        self.entries.iter().filter_map(|e| e.body.clone())
    }

    /// The rejected records that the cursor passed since the last call, in
    /// order.
    fn take_rejections(&mut self) -> Vec<u64> {
        std::mem::take(&mut self.rejections)
    }

    fn advance(&mut self) {
        while let Some(front) = self.entries.front() {
            if front.body.is_some() {
                break;
            }
            if front.rejected {
                self.rejections.push(front.seq);
            }
            self.cursor = front.seq + 1;
            self.entries.pop_front();
        }
    }
}

/// Marks an entry answered and releases its body.
fn answer(entry: &mut Entry, bytes: &mut usize) -> Option<Answered> {
    let body = entry.body.take()?;
    *bytes -= body.len();
    Some(match body {
        Body::Record(record) => Answered::Record { ts: record.ts },
        Body::Notice(_) => Answered::Notice,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{Message as Stored, Storage, Writer};
    use crate::testing::{Rng, seeds};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpSocket};

    /// Received at `seq` microseconds.
    fn rec(seq: u64, payload: &[u8]) -> Record {
        Record {
            seq,
            ts: seq as i64,
            payload: payload.to_vec(),
        }
    }

    fn seqs(window: &Window) -> Vec<u64> {
        window
            .unanswered()
            .map(|body| match body {
                Body::Record(record) => record.seq,
                Body::Notice(text) => {
                    serde_json::from_str::<serde_json::Value>(&text).unwrap()["seq"]
                        .as_u64()
                        .unwrap()
                }
            })
            .collect()
    }

    #[test]
    fn acknowledgements_are_cumulative_and_refusals_single() {
        let mut w = Window::new(1);
        let notice = Notice::lost(1, 3).frame(3);
        let notice_len = notice.len();
        w.push_notice(3, notice);
        w.push(rec(4, b"[4]"));
        w.push(rec(5, b"[5]"));
        w.push(rec(6, b"[6]"));
        assert_eq!((w.outstanding(), w.bytes()), (4, notice_len + 9));

        // A refusal answers one frame; the cursor waits for the prefix, and
        // the refused record is a rejection once the cursor passes it.
        assert_eq!(w.nack(5), Some(Answered::Record { ts: 5 }));
        assert_eq!(w.nack(5), None);
        assert_eq!(w.cursor(), 1);
        assert_eq!(seqs(&w), vec![3, 4, 6]);
        assert_eq!(w.take_rejections(), Vec::<u64>::new());
        // An acknowledgement answers everything up to it.
        assert_eq!(w.ack(4), vec![Answered::Notice, Answered::Record { ts: 4 }]);
        assert_eq!((w.cursor(), w.outstanding()), (6, 1));
        assert_eq!(w.take_rejections(), vec![5]);
        assert_eq!(w.ack(4), vec![]);
        // One past what was sent answers what was sent, and no more.
        assert_eq!(w.ack(u64::MAX), vec![Answered::Record { ts: 6 }]);
        assert_eq!((w.cursor(), w.outstanding(), w.bytes()), (7, 0, 0));
        assert_eq!(w.nack(9), None);
    }

    fn log(dir: &std::path::Path) -> Arc<Storage> {
        let limits = crate::storage::Limits {
            max_bytes: 1 << 20,
            segment_bytes: 64 << 10,
            sync_interval: Duration::ZERO,
        };
        Arc::new(Storage::open(dir, limits).unwrap())
    }

    /// A channel at the start of `storage`, with `metrics` kept across a
    /// restart.
    fn channel_on(storage: &Arc<Storage>, metrics: &Arc<ChannelMetrics>) -> Channel {
        Channel {
            cfg: config::WebSocket::default(),
            feed: Feed::new(storage, "w", 1, metrics.clone()),
            cursor: CursorStore::new(storage.clone(), "w".into(), 1, metrics.clone()),
            metrics: metrics.clone(),
            window: Window::new(1),
            flush: tokio::time::interval(FLUSH_INTERVAL),
            attempt: 0,
        }
    }

    fn channel(dir: &std::path::Path) -> Channel {
        channel_on(&log(dir), &Arc::new(ChannelMetrics::new("websocket")))
    }

    #[tokio::test]
    async fn a_rejection_is_counted_once_the_cursor_passes_it_and_once() {
        let dir = tempfile::tempdir().unwrap();
        let storage = log(dir.path());
        let metrics = Arc::new(ChannelMetrics::new("websocket"));
        let (tx, _rx) = mpsc::channel(16);
        let large = format!("\"{}\"", "x".repeat(300));
        let records = || vec![rec(1, b"[1]"), rec(2, large.as_bytes()), rec(3, b"[3]")];
        let rejected = || metrics.rejected.load(Relaxed);

        // Record 2 is too large for a frame, and the receiver refuses 3,
        // while 1 waits for an answer: neither is passed yet.
        let mut first = channel_on(&storage, &metrics);
        first.cfg.max_frame_bytes = notice::MAX_NOTICE_BYTES;
        first.submit(Delivery::Records(records()), &tx).unwrap();
        first.answer(r#"{"nack": 3}"#);
        assert_eq!(rejected(), 0);
        assert!(metrics.last_error.get().unwrap().message.contains("seq 3"));
        // Restarted, as by a change of its configuration, the channel sends
        // all three again, and the receiver refuses 3 again.
        drop(first);
        let mut again = channel_on(&storage, &metrics);
        again.cfg.max_frame_bytes = notice::MAX_NOTICE_BYTES;
        again.submit(Delivery::Records(records()), &tx).unwrap();
        again.answer(r#"{"nack": 3}"#);
        again.answer(r#"{"ack": 2}"#);
        assert_eq!((again.cursor.current(), rejected()), (4, 2));
        // A channel that panicked starts at the cursor it saved, behind what
        // it passed: what it passes again is not counted again.
        let mut late = channel_on(&storage, &metrics);
        late.submit(Delivery::Records(records()), &tx).unwrap();
        late.answer(r#"{"nack": 2}"#);
        late.answer(r#"{"ack": 3}"#);
        assert_eq!((late.cursor.current(), rejected()), (4, 2));
    }

    #[tokio::test]
    async fn frames_that_cannot_be_queued_stay_in_the_window() {
        let dir = tempfile::tempdir().unwrap();
        let mut channel = channel(dir.path());
        let (tx, rx) = mpsc::channel(4);
        drop(rx);
        let records = (1..=3).map(|seq| rec(seq, b"[0]")).collect();
        assert!(channel.submit(Delivery::Records(records), &tx).is_err());
        assert_eq!(seqs(&channel.window), vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn only_progress_ends_the_backoff() {
        let dir = tempfile::tempdir().unwrap();
        let mut channel = channel(dir.path());
        channel.window.push(rec(1, b"[1]"));
        channel.window.push(rec(2, b"[2]"));
        channel.attempt = 3;
        channel.answer(r#"{"nack": 2}"#);
        assert_eq!((channel.attempt, channel.cursor.current()), (3, 1));
        channel.answer(r#"{"ack": 1}"#);
        assert_eq!((channel.attempt, channel.cursor.current()), (0, 3));
        assert_eq!(channel.metrics.delivered.load(Relaxed), 1);
        assert_eq!(channel.metrics.rejected.load(Relaxed), 1);
    }

    #[test]
    fn a_reply_is_an_ack_a_nack_or_ignored() {
        let parse = |text: &str| match serde_json::from_str::<Reply>(text) {
            Ok(Reply::Ack { ack }) => format!("ack {ack}"),
            Ok(Reply::Nack { nack, reason }) => format!("nack {nack} {reason}"),
            Err(_) => "ignored".into(),
        };
        assert_eq!(parse(r#"{"ack": 7}"#), "ack 7");
        assert_eq!(parse(r#"{"ack": 7, "lag": 1}"#), "ack 7");
        assert_eq!(parse(r#"{"nack": 8, "reason": "schema"}"#), "nack 8 schema");
        assert_eq!(parse(r#"{"nack": 8}"#), "nack 8 ");
        assert_eq!(parse(r#"{"ack": -1}"#), "ignored");
        assert_eq!(parse(r#"{"hello": 1}"#), "ignored");
        assert_eq!(parse("ok"), "ignored");
    }

    #[test]
    fn frame_len_is_the_length_of_the_frame() {
        for payload in [&b"{\"a\":1}"[..], b"not json", b"\x00\x01", b"a\xffb"] {
            let record = rec(42, payload);
            let frame = serde_json::to_string(&view::record(&record)).unwrap();
            assert_eq!(frame_len(&record), frame.len(), "{frame}");
        }
    }

    #[test]
    fn excerpts_end_on_a_character() {
        let long = "é".repeat(EXCERPT_BYTES);
        assert_eq!(excerpt(&long).len(), EXCERPT_BYTES);
        assert_eq!(excerpt(&format!("x{long}")).len(), EXCERPT_BYTES - 1);
        assert_eq!(excerpt("short"), "short");
    }

    /// A receiver that acknowledges each frame `delay` after it has read it,
    /// and reads nothing meanwhile. It reports each connection as `None` and
    /// each frame by its `seq`.
    async fn receiver(delay: Duration) -> (String, mpsc::UnboundedReceiver<Option<u64>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}/", listener.local_addr().unwrap());
        let (tx, rx) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let tx = tx.clone();
                tokio::spawn(async move {
                    let Ok(mut socket) = tokio_tungstenite::accept_async(stream).await else {
                        return;
                    };
                    let _ = tx.send(None);
                    while let Some(Ok(message)) = socket.next().await {
                        let Message::Text(text) = message else {
                            continue;
                        };
                        let frame: serde_json::Value = serde_json::from_str(&text).unwrap();
                        let seq = frame["seq"].as_u64().unwrap();
                        let _ = tx.send(Some(seq));
                        tokio::time::sleep(delay).await;
                        let ack = Message::text(format!(r#"{{"ack": {seq}}}"#));
                        if socket.send(ack).await.is_err() {
                            return;
                        }
                    }
                });
            }
        });
        (url, rx)
    }

    /// What the throttle's socket is to buffer: little enough to pass on
    /// within the timeouts of the tests, and on Linux, which doubles what it
    /// is asked for and keeps to that, enough for the reads that make the
    /// rate. macOS holds about 130 kB for a small size, and far more for 64
    /// KiB while the reader keeps up.
    #[cfg(target_os = "linux")]
    const RECEIVE_BUFFER: libc::c_int = 64 << 10;
    #[cfg(not(target_os = "linux"))]
    const RECEIVE_BUFFER: libc::c_int = 4 << 10;

    /// Forwards connections to `url`, passing `rate` bytes a second towards
    /// it, through a socket that buffers little: the sender's TCP sees what
    /// it sent acknowledged about as fast as it passes, as on a slow link.
    async fn throttled(url: &str, rate: usize) -> String {
        let target = Url::parse(url).unwrap().socket_addrs(|| None).unwrap()[0];
        let socket = TcpSocket::new_v4().unwrap();
        socket.set_recv_buffer_size(RECEIVE_BUFFER as u32).unwrap();
        socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let listener = socket.listen(16).unwrap();
        let throttled = format!("ws://{}/", listener.local_addr().unwrap());
        tokio::spawn(async move {
            while let Ok((inbound, _)) = listener.accept().await {
                small_receive_buffer(&inbound);
                let outbound = TcpStream::connect(target).await.unwrap();
                let (mut from_sender, mut to_sender) = inbound.into_split();
                let (mut from_receiver, mut to_receiver) = outbound.into_split();
                tokio::spawn(async move {
                    let _ = tokio::io::copy(&mut from_receiver, &mut to_sender).await;
                });
                tokio::spawn(async move {
                    let mut chunk = vec![0; rate / 20];
                    loop {
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        match from_sender.read(&mut chunk).await {
                            Ok(0) | Err(_) => return,
                            Ok(n) => {
                                if to_receiver.write_all(&chunk[..n]).await.is_err() {
                                    return;
                                }
                            }
                        }
                    }
                });
            }
        });
        throttled
    }

    /// Sets the receive buffer of an accepted socket as well: macOS does not
    /// take it from the listening one, and grows it while the reader keeps up.
    fn small_receive_buffer(socket: &TcpStream) {
        let size = RECEIVE_BUFFER;
        let len = size_of::<libc::c_int>() as libc::socklen_t;
        let (fd, opt) = (socket.as_raw_fd(), libc::SO_RCVBUF);
        // SAFETY: the descriptor is open, and the pointer and length are
        // those of `size`.
        let set =
            unsafe { libc::setsockopt(fd, libc::SOL_SOCKET, opt, (&raw const size).cast(), len) };
        assert_eq!(set, 0, "{}", io::Error::last_os_error());
    }

    /// Runs a channel with `cfg` over a log that holds `payloads`.
    fn start(
        dir: &std::path::Path,
        cfg: config::WebSocket,
        payloads: &[Vec<u8>],
    ) -> (Arc<ChannelMetrics>, CancellationToken, JoinHandle<()>) {
        let storage = log(dir);
        let mut writer = Writer::new(storage.clone());
        for (ts, payload) in payloads.iter().enumerate() {
            let message = Stored {
                ts: ts as i64,
                payload: payload.clone(),
            };
            writer.append(&[message], true).unwrap();
        }
        let metrics = Arc::new(ChannelMetrics::new("websocket"));
        let feed = Feed::new(&storage, "w", 1, metrics.clone());
        let cursor = CursorStore::new(storage, "w".into(), 1, metrics.clone());
        let token = CancellationToken::new();
        let task = tokio::spawn(run(cfg, feed, cursor, metrics.clone(), token.clone()));
        (metrics, token, task)
    }

    /// Takes what the receiver reports until it has seen `frames` frames:
    /// their `seq`s, and the connections before and among them.
    async fn frames(
        seen: &mut mpsc::UnboundedReceiver<Option<u64>>,
        frames: usize,
    ) -> (Vec<u64>, usize) {
        let (mut seqs, mut connections) = (Vec::new(), 0);
        while seqs.len() < frames {
            let next = tokio::time::timeout(Duration::from_secs(15), seen.recv());
            match next.await.expect("the receiver sees the frames").unwrap() {
                None => connections += 1,
                Some(seq) => seqs.push(seq),
            }
        }
        (seqs, connections)
    }

    async fn delivered(metrics: &ChannelMetrics, cursor: u64) {
        let deadline = Instant::now() + Duration::from_secs(10);
        while metrics.cursor.load(Relaxed) < cursor {
            let now = metrics.cursor.load(Relaxed);
            assert!(Instant::now() < deadline, "cursor {now}");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn acknowledgements_answer_a_ping_that_waits_behind_frames() {
        // Each frame takes the receiver 20 ms, so a ping waits behind the
        // frames sent before it for longer than the timeout.
        let (url, mut seen) = receiver(Duration::from_millis(20)).await;
        let dir = tempfile::tempdir().unwrap();
        let cfg = config::WebSocket {
            url,
            keep_alive_s: 1,
            timeout_ms: 300,
            ..config::WebSocket::default()
        };
        let (metrics, token, task) = start(dir.path(), cfg, &vec![b"[0]".to_vec(); 150]);
        // The session that acknowledges all along is kept, and every frame
        // is sent once.
        assert_eq!(frames(&mut seen, 150).await, ((1..=150).collect(), 1));
        delivered(&metrics, 151).await;
        assert_eq!(metrics.retries.load(Relaxed), 0);
        token.cancel();
        task.await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_frame_that_takes_longer_to_send_than_the_timeout_gets_through() {
        // 1.6 MB at 400 kB a second takes 4 s, and no answer can come before
        // the frame is there. What the throttle's socket buffers, about
        // 130 kB, it acknowledges before it passes it on, so the timeout
        // allows for that.
        let (url, mut seen) = receiver(Duration::ZERO).await;
        let dir = tempfile::tempdir().unwrap();
        let cfg = config::WebSocket {
            url: throttled(&url, 400_000).await,
            keep_alive_s: 1,
            timeout_ms: 1000,
            max_frame_bytes: 4 << 20,
            ..config::WebSocket::default()
        };
        let payload = format!("\"{}\"", "x".repeat(1_600_000)).into_bytes();
        let (metrics, token, task) = start(dir.path(), cfg, &[payload]);
        assert_eq!(frames(&mut seen, 1).await, (vec![1], 1));
        delivered(&metrics, 2).await;
        assert_eq!(metrics.retries.load(Relaxed), 0);
        token.cancel();
        task.await.unwrap();
    }

    /// One frame in the model of the window: the last sequence number it
    /// stands for, its payload's length, whether it is a notice, whether it
    /// is answered, and whether its record is rejected.
    struct Modelled {
        seq: u64,
        len: usize,
        notice: bool,
        answered: bool,
        rejected: bool,
    }

    impl Modelled {
        fn answer(&mut self) -> Answered {
            self.answered = true;
            if self.notice {
                Answered::Notice
            } else {
                Answered::Record {
                    ts: self.seq as i64,
                }
            }
        }
    }

    #[test]
    fn the_window_matches_a_model() {
        for seed in 0..seeds(20_000) {
            let mut rng = Rng::new(seed);
            let start = rng.below(5) + 1;
            let mut w = Window::new(start);
            let mut model: Vec<Modelled> = Vec::new();
            let (mut cursor, mut next) = (start, start);
            let mut passed = Vec::new();
            for step in 0..rng.below(60) {
                let near = |rng: &mut Rng| cursor.saturating_sub(2) + rng.below(next - cursor + 4);
                match rng.below(11) {
                    0..=3 => {
                        let len = rng.below(5) as usize;
                        w.push(rec(next, &vec![b'x'; len]));
                        model.push(Modelled {
                            seq: next,
                            len,
                            notice: false,
                            answered: false,
                            rejected: false,
                        });
                        next += 1;
                    }
                    4 | 5 => {
                        let rejected = rng.chance(50);
                        let to = if rejected { next } else { next + rng.below(4) };
                        let frame = if rejected {
                            let notice = Notice::Rejected {
                                seq: to,
                                ts: to as i64,
                                reason: notice::TOO_LARGE,
                            };
                            let frame = notice.frame(to);
                            w.push_rejected(to, frame.clone());
                            frame
                        } else {
                            let frame = Notice::lost(next, to).frame(to);
                            w.push_notice(to, frame.clone());
                            frame
                        };
                        model.push(Modelled {
                            seq: to,
                            len: frame.len(),
                            notice: true,
                            answered: false,
                            rejected,
                        });
                        next = to + 1;
                    }
                    6..=8 => {
                        let n = if rng.chance(10) {
                            u64::MAX
                        } else {
                            near(&mut rng)
                        };
                        let want: Vec<Answered> = model
                            .iter_mut()
                            .filter(|m| !m.answered && m.seq <= n)
                            .map(Modelled::answer)
                            .collect();
                        assert_eq!(w.ack(n), want, "seed {seed} step {step}: ack {n}");
                    }
                    _ => {
                        let n = near(&mut rng);
                        let want = model
                            .iter_mut()
                            .find(|m| m.seq == n && !m.answered)
                            .map(|m| {
                                m.rejected |= !m.notice;
                                m.answer()
                            });
                        assert_eq!(w.nack(n), want, "seed {seed} step {step}: nack {n}");
                    }
                }
                while model.first().is_some_and(|m| m.answered) {
                    let m = model.remove(0);
                    if m.rejected {
                        passed.push(m.seq);
                    }
                    cursor = m.seq + 1;
                }
                let at = format!("seed {seed} step {step}");
                assert_eq!(w.cursor(), cursor, "{at}");
                assert_eq!(w.outstanding(), model.len(), "{at}");
                assert_eq!(w.take_rejections(), std::mem::take(&mut passed), "{at}");
                let open = model.iter().filter(|m| !m.answered);
                assert_eq!(
                    w.bytes(),
                    open.clone().map(|m| m.len).sum::<usize>(),
                    "{at}"
                );
                let lens: Vec<usize> = w.unanswered().map(|b| b.len()).collect();
                assert_eq!(lens, open.map(|m| m.len).collect::<Vec<_>>(), "{at}");
            }
        }
    }
}
