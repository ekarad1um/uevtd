//! MQTT channel: pipelined publishes with packet-id tracking so the cursor
//! only advances on acknowledgement, daemon-managed reconnection that
//! republishes unacknowledged messages in order, and drop notices in place of
//! records that are skipped.

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::Ordering::Relaxed;
use std::time::Duration;

use rumqttc::{
    AsyncClient, ConnectionError, Event, MqttOptions, NetworkOptions, Outgoing, Packet, QoS,
    TlsConfiguration, Transport,
};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use super::notice::{self, Notice};
use super::{ChannelMetrics, CursorStore, DELIVERY_BYTES, Delivery, Feed, pause};
use crate::storage::Record;
use crate::{config, tls};

const FLUSH_INTERVAL: Duration = Duration::from_secs(1);

pub async fn run(
    cfg: config::Mqtt,
    name: String,
    mut feed: Feed,
    mut cursor: CursorStore,
    metrics: Arc<ChannelMetrics>,
    token: CancellationToken,
) {
    let qos = match cfg.qos {
        0 => QoS::AtMostOnce,
        1 => QoS::AtLeastOnce,
        _ => QoS::ExactlyOnce,
    };
    let limit = inflight(&cfg);
    let mut tracker = Tracker::new(cursor.current());
    // Sessions that ended since the cursor last advanced.
    let mut attempt = 0u32;
    let mut flush = tokio::time::interval(FLUSH_INTERVAL);
    flush.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let client_id = cfg.client_id(&name);
    info!(
        host = %cfg.host, port = cfg.port, topic = %cfg.topic, qos = cfg.qos, client_id = %client_id,
        inflight = limit, "forwarding over mqtt"
    );

    'run: loop {
        let failure = 'session: {
            // Reading a CA file can block, on a hung network mount for one.
            let connected = tokio::select! {
                connected = connect(&cfg, &client_id, limit) => connected,
                _ = token.cancelled() => break 'run,
            };
            let (client, mut events) = match connected {
                Ok(session) => session,
                Err(e) => break 'session e,
            };
            for entry in tracker.reset() {
                publish(&client, &cfg, qos, entry);
            }
            let mut connected = false;
            loop {
                tokio::select! {
                    event = events.recv() => {
                        // Progress, not a CONNACK, ends the backoff: a broker
                        // that accepts the connection and then drops it on the
                        // first publish still waits longer each time.
                        let completed = match event {
                            Some(Ok(Event::Incoming(Packet::ConnAck(_)))) => {
                                connected = true;
                                metrics.session_up();
                                metrics.connected.store(true, Relaxed);
                                info!(outstanding = tracker.outstanding(), "connected");
                                None
                            }
                            Some(Ok(Event::Outgoing(Outgoing::Publish(pkid)))) => tracker.sent(pkid),
                            Some(Ok(Event::Incoming(Packet::PubAck(ack)))) => tracker.acked(ack.pkid),
                            Some(Ok(Event::Incoming(Packet::PubComp(comp)))) => tracker.acked(comp.pkid),
                            Some(Ok(_)) => None,
                            Some(Err(e)) => break 'session e.to_string(),
                            None => break 'session "event loop ended".to_string(),
                        };
                        tally(&metrics, completed);
                        for seq in tracker.take_rejections() {
                            metrics.rejected_passed(seq);
                        }
                        if cursor.advance(tracker.cursor()) {
                            attempt = 0;
                        }
                    }
                    // Bounded by count and by payload bytes; the library holds a
                    // copy of every unacknowledged publish as well.
                    delivery = feed.next(limit - tracker.outstanding(), DELIVERY_BYTES.saturating_sub(tracker.bytes())),
                        if connected && tracker.outstanding() < limit && tracker.bytes() < DELIVERY_BYTES => {
                        submit(delivery, &client, &cfg, qos, &mut tracker, &metrics);
                    }
                    _ = flush.tick() => cursor.flush(false).await,
                    _ = token.cancelled() => break 'run,
                }
            }
        };
        metrics.connected.store(false, Relaxed);
        warn!(error = %failure, outstanding = tracker.outstanding(), "no session");
        metrics.last_error.set(&failure);
        metrics.retries.fetch_add(1, Relaxed);
        if !pause(attempt, &token).await {
            break;
        }
        attempt += 1;
    }
    metrics.connected.store(false, Relaxed);
    cursor.flush(true).await;
}

type Events = mpsc::Receiver<Result<Event, ConnectionError>>;

/// How many publishes may await acknowledgement: `inflight`, or fewer for a
/// long topic. The library keeps a copy of the topic with each, and those
/// copies may hold at most [`DELIVERY_BYTES`].
fn inflight(cfg: &config::Mqtt) -> usize {
    let topics = DELIVERY_BYTES / cfg.topic.len().max(1);
    usize::from(cfg.inflight).min(topics).max(1)
}

/// Builds a client and drives its event loop on a task that forwards events;
/// the task ends on the first connection error or when the receiver is dropped.
async fn connect(
    cfg: &config::Mqtt,
    client_id: &str,
    limit: usize,
) -> Result<(AsyncClient, Events), String> {
    let mut options = MqttOptions::new(client_id, cfg.host.clone(), cfg.port);
    options
        .set_keep_alive(Duration::from_secs(cfg.keep_alive_s.max(1)))
        .set_clean_session(true)
        .set_inflight((limit * 2 + 1).min(u16::MAX as usize) as u16)
        // Our own payload check is the only size limit that may fire: the
        // library's would end the connection and the record would be
        // republished forever. A publish packet adds at most 9 bytes to the
        // topic and payload.
        .set_max_packet_size(
            64 * 1024,
            cfg.max_packet_bytes.saturating_add(cfg.topic.len() + 16),
        );
    if let Some(user) = &cfg.username {
        options.set_credentials(user.clone(), cfg.password.clone().unwrap_or_default());
    }
    if cfg.tls {
        let ca_file = cfg.ca_file.clone();
        let tls = tokio::task::spawn_blocking(move || tls::client_config(ca_file.as_deref()))
            .await
            .map_err(|e| format!("tls: {e}"))??;
        options.set_transport(Transport::tls_with_config(TlsConfiguration::Rustls(tls)));
    }
    let (client, mut eventloop) = AsyncClient::new(options, limit);
    // The library's default is 5 s, for the whole connection and for each
    // flush, which a slow or distant broker does not meet.
    let mut network = NetworkOptions::new();
    network.set_connection_timeout(cfg.timeout_ms.div_ceil(1000));
    network.set_tcp_nodelay(true);
    eventloop.set_network_options(network);
    let (tx, rx) = mpsc::channel(limit.max(16));
    tokio::spawn(async move {
        loop {
            tokio::select! {
                event = eventloop.poll() => {
                    let failed = event.is_err();
                    if tx.send(event).await.is_err() || failed {
                        break;
                    }
                }
                _ = tx.closed() => break,
            }
        }
    });
    Ok((client, rx))
}

/// Publishes what the feed delivered, one publish per record and per loss:
/// a record, or a notice in place of a record too large to publish or of
/// records lost before delivery.
fn submit(
    delivery: Delivery,
    client: &AsyncClient,
    cfg: &config::Mqtt,
    qos: QoS,
    tracker: &mut Tracker,
    metrics: &ChannelMetrics,
) {
    match delivery {
        Delivery::Records(records) => {
            for record in records {
                let entry = if record.payload.len() > cfg.max_packet_bytes {
                    warn!(
                        seq = record.seq,
                        bytes = record.payload.len(),
                        max = cfg.max_packet_bytes,
                        "payload too large; publishing a notice instead"
                    );
                    metrics
                        .last_error
                        .set(format!("seq {}: payload too large", record.seq));
                    let notice = Notice::Rejected {
                        seq: record.seq,
                        ts: record.ts,
                        reason: notice::TOO_LARGE,
                    };
                    tracker.push_rejected(record.seq, notice.encode())
                } else {
                    tracker.push(record)
                };
                publish(client, cfg, qos, entry);
            }
        }
        Delivery::Lost { from, to } => {
            info!(
                from_seq = from,
                to_seq = to,
                "publishing a notice for records lost before delivery"
            );
            let entry = tracker.push_notice(to, Notice::lost(from, to).encode());
            publish(client, cfg, qos, entry);
        }
    }
}

fn publish(client: &AsyncClient, cfg: &config::Mqtt, qos: QoS, entry: &Entry) {
    // Capacity is guaranteed by the in-flight bound; a failure means the event
    // loop is gone, which the next event report handles. A notice is never
    // retained, or it would replace the topic's last message for subscribers.
    let retain = cfg.retain && !entry.notice;
    if let Err(e) = client.try_publish(&cfg.topic, qos, retain, entry.payload.clone()) {
        debug!(seq = entry.seq, error = %e, "publish not queued");
    }
}

/// Counts a completed publish.
fn tally(metrics: &ChannelMetrics, completed: Option<Completed>) {
    match completed {
        Some(Completed::Record { ts }) => metrics.delivered_at(ts, crate::now_us()),
        Some(Completed::Notice) => {
            metrics.notices.fetch_add(1, Relaxed);
        }
        None => {}
    }
}

/// One publish: a record, or a notice in place of the records up to `seq`
/// that the channel will not deliver.
struct Entry {
    /// The last sequence number the publish accounts for.
    seq: u64,
    /// The record's receipt time; 0 for a notice.
    ts: i64,
    payload: Vec<u8>,
    notice: bool,
    /// A notice in place of record `seq`, too large to publish.
    rejected: bool,
    pkid: Option<u16>,
    done: bool,
}

/// What a completed publish carried.
#[derive(Debug, PartialEq, Eq)]
enum Completed {
    Record { ts: i64 },
    Notice,
}

/// Maps the library's packet ids to log sequence numbers and derives the
/// cursor as the end of the longest completed prefix. A payload is kept
/// until its publish completes, for republishing after a reconnect.
struct Tracker {
    entries: VecDeque<Entry>,
    cursor: u64,
    /// Payload bytes kept for entries that have not completed.
    bytes: usize,
    /// The rejected records the cursor passed, until taken.
    rejections: Vec<u64>,
}

impl Tracker {
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

    /// Payload bytes kept for republishing.
    fn bytes(&self) -> usize {
        self.bytes
    }

    /// Adds the publish of a record; returns it for publishing.
    fn push(&mut self, record: Record) -> &Entry {
        self.enqueue(record.seq, record.ts, record.payload, false, false)
    }

    /// Adds the publish of a notice that stands for the records up to `seq`.
    fn push_notice(&mut self, seq: u64, payload: Vec<u8>) -> &Entry {
        self.enqueue(seq, 0, payload, true, false)
    }

    /// Adds the publish of a notice in place of record `seq`.
    fn push_rejected(&mut self, seq: u64, payload: Vec<u8>) -> &Entry {
        self.enqueue(seq, 0, payload, true, true)
    }

    fn enqueue(
        &mut self,
        seq: u64,
        ts: i64,
        payload: Vec<u8>,
        notice: bool,
        rejected: bool,
    ) -> &Entry {
        self.bytes += payload.len();
        self.entries.push_back(Entry {
            seq,
            ts,
            payload,
            notice,
            rejected,
            pkid: None,
            done: false,
        });
        self.entries.back().expect("just pushed")
    }

    /// The rejected records that the cursor passed since the last call, in
    /// order.
    fn take_rejections(&mut self) -> Vec<u64> {
        std::mem::take(&mut self.rejections)
    }

    /// A publish was written with `pkid`. Binds it to the oldest unsent entry;
    /// packet id 0 (QoS 0) completes that entry.
    fn sent(&mut self, pkid: u16) -> Option<Completed> {
        if pkid != 0 && self.entries.iter().any(|e| !e.done && e.pkid == Some(pkid)) {
            return None; // retransmission of an entry we already track
        }
        let entry = self
            .entries
            .iter_mut()
            .find(|e| !e.done && e.pkid.is_none())?;
        if pkid != 0 {
            entry.pkid = Some(pkid);
            return None;
        }
        let (released, completed) = complete(entry);
        self.bytes -= released;
        self.advance();
        Some(completed)
    }

    /// The broker acknowledged `pkid`.
    fn acked(&mut self, pkid: u16) -> Option<Completed> {
        let entry = self
            .entries
            .iter_mut()
            .find(|e| !e.done && e.pkid == Some(pkid))?;
        let (released, completed) = complete(entry);
        self.bytes -= released;
        self.advance();
        Some(completed)
    }

    /// Forgets packet ids after a reconnect; yields what must be republished, in order.
    fn reset(&mut self) -> impl Iterator<Item = &Entry> {
        self.entries.iter_mut().filter(|e| !e.done).map(|e| {
            e.pkid = None;
            &*e
        })
    }

    fn advance(&mut self) {
        while let Some(front) = self.entries.front() {
            if !front.done {
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

/// Marks an entry completed and drops its payload, which is not needed any
/// more even while the entry waits for the prefix before it; returns the
/// bytes released.
fn complete(entry: &mut Entry) -> (usize, Completed) {
    entry.done = true;
    let released = std::mem::take(&mut entry.payload).len();
    let completed = if entry.notice {
        Completed::Notice
    } else {
        Completed::Record { ts: entry.ts }
    };
    (released, completed)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::net::SocketAddr;
    use std::path::Path;
    use std::time::Instant;

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};
    use tokio::task::JoinHandle;

    use super::*;
    use crate::storage::{Limits, Storage};
    use crate::testing::{Pipe, Rng, seeds};

    fn rec(seq: u64) -> Record {
        Record {
            seq,
            ts: 0,
            payload: vec![seq as u8],
        }
    }

    const RECORD: Option<Completed> = Some(Completed::Record { ts: 0 });
    const NOTICE: Option<Completed> = Some(Completed::Notice);

    #[test]
    fn qos1_in_order() {
        let mut t = Tracker::new(10);
        t.push(rec(10));
        t.push(rec(11));
        assert_eq!(t.sent(1), None);
        assert_eq!(t.sent(2), None);
        assert_eq!(t.acked(1), RECORD);
        assert_eq!(t.cursor(), 11);
        assert_eq!(t.acked(2), RECORD);
        assert_eq!(t.cursor(), 12);
        assert_eq!(t.outstanding(), 0);
    }

    #[test]
    fn a_completed_record_carries_its_receipt_time() {
        let mut t = Tracker::new(1);
        for (seq, ts) in [(1, 42), (2, 43)] {
            t.push(Record {
                seq,
                ts,
                payload: vec![1],
            });
        }
        t.push_notice(3, b"rejected 3".to_vec());
        assert_eq!(t.sent(0), Some(Completed::Record { ts: 42 }));
        t.sent(5);
        t.sent(6);
        assert_eq!(t.acked(5), Some(Completed::Record { ts: 43 }));
        assert_eq!(t.acked(6), NOTICE);
    }

    #[test]
    fn out_of_order_acks_keep_prefix_semantics() {
        let mut t = Tracker::new(1);
        for s in 1..=3 {
            t.push(rec(s));
        }
        t.sent(1);
        t.sent(2);
        t.sent(3);
        assert_eq!(t.acked(3), RECORD);
        assert_eq!(t.cursor(), 1);
        assert_eq!(t.acked(1), RECORD);
        assert_eq!(t.cursor(), 2);
        assert_eq!(t.acked(2), RECORD);
        assert_eq!(t.cursor(), 4);
        assert_eq!(t.acked(2), None);
    }

    #[test]
    fn qos0_completes_on_send_and_retransmits_are_ignored() {
        let mut t = Tracker::new(5);
        t.push(rec(5));
        t.push(rec(6));
        assert_eq!(t.sent(0), RECORD);
        assert_eq!(t.cursor(), 6);
        assert_eq!(t.sent(7), None);
        assert_eq!(t.sent(7), None);
        assert_eq!(t.outstanding(), 1);
        assert_eq!(t.acked(7), RECORD);
        assert_eq!(t.cursor(), 7);
    }

    #[test]
    fn payloads_are_released_once_their_publish_completes() {
        let sized = |seq: u64, len: usize| Record {
            seq,
            ts: 0,
            payload: vec![0; len],
        };
        let mut t = Tracker::new(1);
        t.push(sized(1, 100));
        t.push(sized(2, 50));
        t.push_notice(3, vec![0; 5]);
        assert_eq!(t.bytes(), 155);
        t.sent(1);
        t.sent(2);
        // An acknowledgement out of order releases the payload at once,
        // although the entry stays until the one before it completes.
        t.acked(2);
        assert_eq!((t.bytes(), t.outstanding(), t.cursor()), (105, 3, 1));
        let again: Vec<u64> = t.reset().map(|e| e.seq).collect();
        assert_eq!(again, vec![1, 3]);
        t.sent(7);
        t.sent(8);
        t.acked(7);
        t.acked(8);
        assert_eq!((t.bytes(), t.outstanding(), t.cursor()), (0, 0, 4));
        // QoS 0 completes when written.
        t.push(sized(4, 10));
        t.sent(0);
        assert_eq!((t.bytes(), t.cursor()), (0, 5));
    }

    #[test]
    fn reset_republishes_unacked_in_order_and_skips_done() {
        let mut t = Tracker::new(1);
        for s in 1..=4 {
            t.push(rec(s));
        }
        t.sent(1);
        t.sent(2);
        t.sent(3);
        t.acked(2);
        t.push_notice(6, b"lost 5-6".to_vec());
        let again: Vec<u64> = t.reset().map(|e| e.seq).collect();
        assert_eq!(again, vec![1, 3, 4, 6]);
        for pkid in 9..=12 {
            t.sent(pkid);
        }
        assert_eq!(t.acked(9), RECORD);
        assert_eq!(t.cursor(), 3);
        t.acked(10);
        t.acked(11);
        assert_eq!(t.cursor(), 5);
        assert_eq!(t.acked(12), NOTICE);
        assert_eq!(t.cursor(), 7);
        assert_eq!(t.outstanding(), 0);
    }

    #[test]
    fn a_notice_takes_the_place_of_the_records_it_covers() {
        let mut t = Tracker::new(1);
        t.push_notice(4, b"lost 1-4".to_vec());
        t.push(rec(5));
        t.push_notice(6, b"rejected 6".to_vec());
        let notices: Vec<bool> = t.reset().map(|e| e.notice).collect();
        assert_eq!(notices, vec![true, false, true]);
        t.sent(1);
        t.sent(2);
        t.sent(3);
        // The cursor passes the lost records only once their notice is
        // acknowledged.
        assert_eq!(t.acked(2), RECORD);
        assert_eq!(t.cursor(), 1);
        assert_eq!(t.acked(1), NOTICE);
        assert_eq!(t.cursor(), 6);
        assert_eq!(t.acked(3), NOTICE);
        assert_eq!((t.cursor(), t.outstanding(), t.bytes()), (7, 0, 0));
    }

    #[test]
    fn a_rejection_is_counted_once_the_cursor_passes_it_and_once() {
        let metrics = ChannelMetrics::new("mqtt");
        let cfg = config::Mqtt {
            topic: "t".into(),
            max_packet_bytes: notice::MAX_NOTICE_BYTES,
            ..config::Mqtt::default()
        };
        let (client, _eventloop) = AsyncClient::new(MqttOptions::new("c", "127.0.0.1", 1), 16);
        let records = || {
            let large = Record {
                seq: 2,
                ts: 0,
                payload: vec![b'x'; 300],
            };
            Delivery::Records(vec![rec(1), large, rec(3)])
        };
        let qos = QoS::AtLeastOnce;

        // Record 2 is too large to publish; the channel restarts before the
        // broker acknowledges its notice, and publishes it again.
        let mut first = Tracker::new(1);
        submit(records(), &client, &cfg, qos, &mut first, &metrics);
        assert_eq!(metrics.rejected.load(Relaxed), 0);
        let mut again = Tracker::new(1);
        submit(records(), &client, &cfg, qos, &mut again, &metrics);
        for pkid in 1..=3 {
            again.sent(pkid);
        }
        for pkid in 1..=3 {
            again.acked(pkid);
            for seq in again.take_rejections() {
                metrics.rejected_passed(seq);
            }
        }
        assert_eq!((again.cursor(), metrics.rejected.load(Relaxed)), (4, 1));
    }

    #[test]
    fn a_long_topic_lowers_the_publishes_in_flight() {
        let cfg = |topic: usize, inflight| config::Mqtt {
            topic: "t".repeat(topic),
            inflight,
            ..config::Mqtt::default()
        };
        // Their copies of the topic hold at most a MiB.
        assert_eq!(inflight(&cfg(12, 32)), 32);
        assert_eq!(inflight(&cfg(1024, 1024)), 1024);
        assert_eq!(inflight(&cfg(1025, 1024)), 1023);
        assert_eq!(inflight(&cfg(65_535, 32)), 16);
        assert_eq!(inflight(&cfg(65_535, 1)), 1);
    }

    /// Runs a channel with `cfg` over an empty log.
    fn start(
        dir: &Path,
        cfg: config::Mqtt,
    ) -> (Arc<ChannelMetrics>, CancellationToken, JoinHandle<()>) {
        let limits = Limits {
            max_bytes: 1 << 20,
            segment_bytes: 64 << 10,
            sync_interval: Duration::ZERO,
        };
        let storage = Arc::new(Storage::open(dir, limits).unwrap());
        let metrics = Arc::new(ChannelMetrics::new("mqtt"));
        let feed = Feed::new(&storage, "m", 1, metrics.clone());
        let cursor = CursorStore::new(storage, "m".into(), 1, metrics.clone());
        let token = CancellationToken::new();
        let channel = run(
            cfg,
            "m".into(),
            feed,
            cursor,
            metrics.clone(),
            token.clone(),
        );
        (metrics, token, tokio::spawn(channel))
    }

    async fn until(what: &str, mut done: impl FnMut() -> bool) {
        let deadline = Instant::now() + Duration::from_secs(10);
        while !done() {
            assert!(Instant::now() < deadline, "{what}");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Reads one MQTT packet: its type, and its body.
    async fn packet(stream: &mut TcpStream) -> std::io::Result<(u8, Vec<u8>)> {
        let header = stream.read_u8().await?;
        let mut len = 0;
        for shift in [0, 7, 14, 21] {
            let byte = stream.read_u8().await?;
            len |= usize::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                break;
            }
        }
        let mut body = vec![0; len];
        stream.read_exact(&mut body).await?;
        Ok((header >> 4, body))
    }

    /// A broker that answers CONNECT `delay` after it arrives, PINGREQ at
    /// once, and nothing else.
    async fn slow_broker(delay: Duration) -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                tokio::spawn(async move {
                    while let Ok((kind, _)) = packet(&mut stream).await {
                        let answer: &[u8] = match kind {
                            1 => {
                                tokio::time::sleep(delay).await;
                                &[0x20, 2, 0, 0]
                            }
                            12 => &[0xd0, 0],
                            _ => continue,
                        };
                        if stream.write_all(answer).await.is_err() {
                            return;
                        }
                    }
                });
            }
        });
        addr
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn connecting_may_take_as_long_as_the_configured_timeout() {
        let addr = slow_broker(Duration::from_millis(1500)).await;
        let cfg = |timeout_ms| config::Mqtt {
            host: addr.ip().to_string(),
            port: addr.port(),
            topic: "t".into(),
            client_id: Some("c".into()),
            timeout_ms,
            ..config::Mqtt::default()
        };
        // A second is not enough for this broker, as the library's own five
        // seconds would be...
        let dir = tempfile::tempdir().unwrap();
        let (metrics, token, task) = start(dir.path(), cfg(1000));
        let error = || metrics.last_error.get().map(|e| e.message);
        until("a timeout", || {
            error().is_some_and(|m| m.contains("Network timeout"))
        })
        .await;
        assert!(!metrics.connected.load(Relaxed));
        token.cancel();
        task.await.unwrap();
        // ... and 1001 ms are rounded up to two seconds, which are.
        let dir = tempfile::tempdir().unwrap();
        let (metrics, token, task) = start(dir.path(), cfg(1001));
        until("connected", || metrics.connected.load(Relaxed)).await;
        assert_eq!(metrics.retries.load(Relaxed), 0);
        token.cancel();
        task.await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_channel_stops_while_it_waits_for_its_ca_file() {
        let dir = tempfile::tempdir().unwrap();
        // A pipe that nothing writes: reading it waits, as on a hung network
        // mount.
        let ca_file = dir.path().join("ca.pem");
        let _pipe = Pipe::new(&ca_file);
        let cfg = config::Mqtt {
            host: "127.0.0.1".into(),
            tls: true,
            ca_file: Some(ca_file),
            topic: "t".into(),
            client_id: Some("c".into()),
            ..config::Mqtt::default()
        };
        let (_metrics, token, task) = start(&dir.path().join("log"), cfg);
        tokio::time::sleep(Duration::from_millis(200)).await;
        token.cancel();
        let stopped = tokio::time::timeout(Duration::from_secs(2), task).await;
        stopped.expect("the channel stops").unwrap();
    }

    /// rumqttc 0.25.1's packet ids: 1 to `max` in turn (`next_pkid` in its
    /// state.rs). A QoS 2 id is free again at PUBREC, before its PUBCOMP.
    struct PacketIds {
        last: u16,
        max: u16,
    }

    impl PacketIds {
        fn next(&mut self) -> u16 {
            let next = self.last + 1;
            self.last = if next == self.max { 0 } else { next };
            next
        }
    }

    /// A publish in the model: the last sequence number it stands for,
    /// whether it is a notice, whether its record is rejected, and whether
    /// the broker completed it.
    struct Published {
        seq: u64,
        notice: bool,
        rejected: bool,
        done: bool,
    }

    impl Published {
        fn completed(&self) -> Option<Completed> {
            Some(if self.notice {
                Completed::Notice
            } else {
                Completed::Record {
                    ts: self.seq as i64,
                }
            })
        }
    }

    #[test]
    fn the_tracker_follows_the_broker_through_reconnects() {
        for seed in 0..seeds(20_000) {
            let mut rng = Rng::new(seed);
            let limit = rng.pick(&[1usize, 2, 3, 4, 8]);
            let qos = rng.below(3);
            let start = 1 + rng.below(3);
            let mut t = Tracker::new(start);
            let mut pushed: Vec<Published> = Vec::new();
            // How many of them the cursor passed.
            let mut passed = 0;
            let mut requests: VecDeque<usize> = VecDeque::new();
            let ids = || PacketIds {
                last: 0,
                max: (2 * limit + 1) as u16,
            };
            let mut lib = ids();
            // Awaiting PUBACK or PUBREC, and awaiting PUBCOMP.
            let mut publish: BTreeMap<u16, usize> = BTreeMap::new();
            let mut release: BTreeMap<u16, usize> = BTreeMap::new();
            let mut next = start;
            for step in 0..rng.below(300) {
                let at = format!("seed {seed} step {step}");
                match rng.below(10) {
                    0..=2 if t.outstanding() < limit => {
                        for _ in 0..1 + rng.below((limit - t.outstanding()) as u64) {
                            requests.push_back(pushed.len());
                            let (seq, notice, rejected) = match rng.below(10) {
                                0 => {
                                    let to = next + rng.below(3);
                                    t.push_notice(to, vec![0; 3]);
                                    (to, true, false)
                                }
                                1 => {
                                    t.push_rejected(next, vec![0; 4]);
                                    (next, true, true)
                                }
                                _ => {
                                    let payload = vec![0; 2];
                                    let ts = next as i64;
                                    t.push(Record {
                                        seq: next,
                                        ts,
                                        payload,
                                    });
                                    (next, false, false)
                                }
                            };
                            pushed.push(Published {
                                seq,
                                notice,
                                rejected,
                                done: false,
                            });
                            next = seq + 1;
                        }
                    }
                    3 | 4 => {
                        // The event loop takes the next request and writes it.
                        let Some(i) = requests.pop_front() else {
                            continue;
                        };
                        if qos == 0 {
                            pushed[i].done = true;
                            assert_eq!(t.sent(0), pushed[i].completed(), "{at}: QoS 0");
                        } else {
                            let pkid = lib.next();
                            assert!(!publish.contains_key(&pkid), "{at}: id {pkid} collides");
                            publish.insert(pkid, i);
                            assert_eq!(t.sent(pkid), None, "{at}");
                        }
                    }
                    5..=7 => {
                        // The broker answers one outstanding publish.
                        let any = |ids: &BTreeMap<u16, usize>, rng: &mut Rng| {
                            *ids.keys()
                                .nth(rng.below(ids.len() as u64) as usize)
                                .unwrap()
                        };
                        if qos == 2 && !release.is_empty() && rng.chance(50) {
                            let pkid = any(&release, &mut rng);
                            let i = release.remove(&pkid).unwrap();
                            pushed[i].done = true;
                            assert_eq!(t.acked(pkid), pushed[i].completed(), "{at}: PUBCOMP");
                        } else if !publish.is_empty() {
                            let pkid = any(&publish, &mut rng);
                            let i = publish.remove(&pkid).unwrap();
                            if qos == 2 {
                                // PUBREC: the library frees the id now.
                                release.insert(pkid, i);
                            } else {
                                pushed[i].done = true;
                                assert_eq!(t.acked(pkid), pushed[i].completed(), "{at}: PUBACK");
                            }
                        }
                    }
                    8 if rng.chance(30) => {
                        // A new session: a fresh client publishes again what
                        // is not done, in order.
                        let open: VecDeque<usize> =
                            (0..pushed.len()).filter(|&i| !pushed[i].done).collect();
                        let again: Vec<u64> = t.reset().map(|e| e.seq).collect();
                        let want: Vec<u64> = open.iter().map(|&i| pushed[i].seq).collect();
                        assert_eq!(again, want, "{at}: published again");
                        requests = open;
                        lib = ids();
                        publish.clear();
                        release.clear();
                    }
                    _ => {}
                }
                // The cursor is the end of the longest completed prefix, and
                // the rejections it passes are reported once each, in order.
                let mut rejections = Vec::new();
                while pushed.get(passed).is_some_and(|p| p.done) {
                    if pushed[passed].rejected {
                        rejections.push(pushed[passed].seq);
                    }
                    passed += 1;
                }
                let cursor = passed.checked_sub(1).map_or(start, |i| pushed[i].seq + 1);
                assert_eq!(t.cursor(), cursor, "{at}");
                assert_eq!(t.take_rejections(), rejections, "{at}");
                assert_eq!(t.outstanding(), pushed.len() - passed, "{at}");
                assert!(t.outstanding() <= limit, "{at}");
            }
        }
    }
}
