//! WebSocket channel: records stream as text frames to a receiver that
//! acknowledges them by sequence number. The cursor follows the answered
//! prefix, and unanswered frames are sent again after a reconnect
//! (ARCHITECTURE.md §6.2).

use std::collections::VecDeque;
use std::io;
use std::net::IpAddr;
use std::sync::Arc;
use std::sync::atomic::Ordering::Relaxed;
use std::time::Duration;

use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
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

type Socket = WebSocketStream<Either<TcpStream, TlsStream<TcpStream>>>;

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
            Ok(socket) => match channel.session(socket, &token).await {
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
    async fn session(&mut self, socket: Socket, token: &CancellationToken) -> Result<(), String> {
        let (sink, mut incoming) = socket.split();
        // Frames wait here to be written: the unanswered ones sent again,
        // then new ones, never more than two windows, since a receiver
        // acknowledges only frames it has received in some session.
        let (tx, rx) = mpsc::channel(2 * usize::from(self.cfg.inflight) + 2);
        let mut writer = tokio::spawn(write(sink, rx));
        self.metrics.connected.store(true, Relaxed);
        self.metrics.last_error.clear();
        info!(outstanding = self.window.outstanding(), "connected");
        let result = self.serve(&mut incoming, &tx, &mut writer, token).await;
        self.metrics.connected.store(false, Relaxed);
        writer.abort();
        result
    }

    async fn serve(
        &mut self,
        incoming: &mut SplitStream<Socket>,
        tx: &mpsc::Sender<Outgoing>,
        writer: &mut JoinHandle<Result<(), WsError>>,
        token: &CancellationToken,
    ) -> Result<(), String> {
        for body in self.window.unanswered() {
            queue(tx, Outgoing::Frame(body))?;
        }
        let limit = usize::from(self.cfg.inflight);
        let keep_alive = Duration::from_secs(self.cfg.keep_alive_s);
        let mut ping = tokio::time::interval_at(Instant::now() + keep_alive, keep_alive);
        ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut pong_due: Option<Instant> = None;
        loop {
            // Bounded by count and by payload bytes, like the MQTT window.
            let room = limit.saturating_sub(self.window.outstanding());
            let bytes = DELIVERY_BYTES.saturating_sub(self.window.bytes());
            tokio::select! {
                message = incoming.next() => match message {
                    Some(Ok(Message::Text(text))) => self.answer(&text),
                    Some(Ok(Message::Pong(_))) => pong_due = None,
                    Some(Ok(Message::Close(frame))) => return Err(closed(frame)),
                    // Binary frames are ignored; pings are answered by the library.
                    Some(Ok(_)) => {}
                    Some(Err(e)) => return Err(e.to_string()),
                    None => return Err("connection closed".into()),
                },
                delivery = self.feed.next(room, bytes), if room > 0 && bytes > 0 => {
                    self.submit(delivery, tx)?;
                }
                _ = ping.tick(), if pong_due.is_none() => {
                    queue(tx, Outgoing::Ping)?;
                    pong_due = Some(Instant::now() + Duration::from_millis(self.cfg.timeout_ms));
                }
                _ = tokio::time::sleep_until(pong_due.unwrap_or_else(Instant::now)), if pong_due.is_some() => {
                    return Err(format!("no pong within {} ms", self.cfg.timeout_ms));
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
                        self.metrics.rejected.fetch_add(1, Relaxed);
                        self.metrics
                            .last_error
                            .set(format!("seq {}: frame too large", record.seq));
                        let notice = Notice::Rejected {
                            seq: record.seq,
                            ts: record.ts,
                            reason: notice::TOO_LARGE,
                        };
                        self.window
                            .push_notice(record.seq, notice.frame(record.seq))
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
                let (records, notices) = self.window.ack(ack);
                self.metrics.delivered.fetch_add(records, Relaxed);
                self.metrics.notices.fetch_add(notices, Relaxed);
            }
            Ok(Reply::Nack { nack, reason }) => {
                let reason = excerpt(&reason);
                match self.window.nack(nack) {
                    Some(Answered::Record) => {
                        warn!(seq = nack, reason, "refused by the receiver");
                        self.metrics.rejected.fetch_add(1, Relaxed);
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
async fn connect(cfg: &config::WebSocket) -> Result<Socket, String> {
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
        Ok::<_, String>(socket)
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
}

/// What an answered frame carried.
#[derive(Debug, PartialEq, Eq)]
enum Answered {
    Record,
    Notice,
}

/// The frames sent and not yet answered, in sequence order. The cursor is
/// the end of the longest answered prefix.
struct Window {
    entries: VecDeque<Entry>,
    cursor: u64,
    /// Payload bytes held for unanswered frames.
    bytes: usize,
}

impl Window {
    fn new(cursor: u64) -> Self {
        Self {
            entries: VecDeque::new(),
            cursor,
            bytes: 0,
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
        self.enqueue(seq, Body::Record(Arc::new(record)))
    }

    /// Adds a notice's frame, standing for the records up to `seq`.
    fn push_notice(&mut self, seq: u64, frame: String) -> Body {
        self.enqueue(seq, Body::Notice(frame.into()))
    }

    fn enqueue(&mut self, seq: u64, body: Body) -> Body {
        self.bytes += body.len();
        self.entries.push_back(Entry {
            seq,
            body: Some(body.clone()),
        });
        body
    }

    /// The receiver processed every frame up to `seq`; returns how many
    /// records and notices that answered.
    fn ack(&mut self, seq: u64) -> (u64, u64) {
        let (mut records, mut notices) = (0, 0);
        for entry in self.entries.iter_mut().take_while(|e| e.seq <= seq) {
            match answer(entry, &mut self.bytes) {
                Some(Answered::Record) => records += 1,
                Some(Answered::Notice) => notices += 1,
                None => {}
            }
        }
        self.advance();
        (records, notices)
    }

    /// The receiver will never accept the frame for `seq`.
    fn nack(&mut self, seq: u64) -> Option<Answered> {
        let entry = self.entries.iter_mut().find(|e| e.seq == seq)?;
        let answered = answer(entry, &mut self.bytes);
        self.advance();
        answered
    }

    /// The frames to send again after a reconnect, in order.
    fn unanswered(&self) -> impl Iterator<Item = Body> + '_ {
        self.entries.iter().filter_map(|e| e.body.clone())
    }

    fn advance(&mut self) {
        while let Some(front) = self.entries.front() {
            if front.body.is_some() {
                break;
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
        Body::Record(_) => Answered::Record,
        Body::Notice(_) => Answered::Notice,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(seq: u64, payload: &[u8]) -> Record {
        Record {
            seq,
            ts: 0,
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

        // A refusal answers one frame; the cursor waits for the prefix.
        assert_eq!(w.nack(5), Some(Answered::Record));
        assert_eq!(w.nack(5), None);
        assert_eq!(w.cursor(), 1);
        assert_eq!(seqs(&w), vec![3, 4, 6]);
        // An acknowledgement answers everything up to it.
        assert_eq!(w.ack(4), (1, 1));
        assert_eq!((w.cursor(), w.outstanding()), (6, 1));
        assert_eq!(w.ack(4), (0, 0));
        // One past what was sent answers what was sent, and no more.
        assert_eq!(w.ack(u64::MAX), (1, 0));
        assert_eq!((w.cursor(), w.outstanding(), w.bytes()), (7, 0, 0));
        assert_eq!(w.nack(9), None);
    }

    fn channel(dir: &std::path::Path) -> Channel {
        let limits = crate::storage::Limits {
            max_bytes: 1 << 20,
            segment_bytes: 64 << 10,
            sync_interval: Duration::ZERO,
        };
        let storage = Arc::new(crate::storage::Storage::open(dir, limits).unwrap());
        let metrics = Arc::new(ChannelMetrics::new("websocket"));
        Channel {
            cfg: config::WebSocket::default(),
            feed: Feed::new(&storage, "w", 1, metrics.clone()),
            cursor: CursorStore::new(storage, "w".into(), 1, metrics.clone()),
            metrics,
            window: Window::new(1),
            flush: tokio::time::interval(FLUSH_INTERVAL),
            attempt: 0,
        }
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
}
