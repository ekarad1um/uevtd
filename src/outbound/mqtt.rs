//! MQTT channel: pipelined publishes with packet-id tracking so the cursor
//! only advances on acknowledgement, daemon-managed reconnection that
//! republishes unacknowledged messages in order, and drop notices in place of
//! records that are skipped (ARCHITECTURE.md §6.3).

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::Ordering::Relaxed;
use std::time::Duration;

use rumqttc::tokio_rustls::rustls::ClientConfig;
use rumqttc::{
    AsyncClient, ConnectionError, Event, MqttOptions, Outgoing, Packet, QoS, TlsConfiguration,
    Transport,
};
use rustls_platform_verifier::ConfigVerifierExt;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use super::notice::{self, Notice};
use super::{ChannelMetrics, CursorStore, DELIVERY_BYTES, Delivery, Feed, backoff};
use crate::config;
use crate::storage::Record;

const FLUSH_INTERVAL: Duration = Duration::from_secs(1);
const BACKOFF_BASE: Duration = Duration::from_secs(1);
const BACKOFF_MAX: Duration = Duration::from_secs(60);

pub async fn run(
    cfg: config::Mqtt,
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
    let limit = cfg.inflight.max(1) as usize;
    let mut tracker = Tracker::new(cursor.current());
    let mut attempt = 0u32;
    let mut flush = tokio::time::interval(FLUSH_INTERVAL);
    flush.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    info!(host = %cfg.host, port = cfg.port, topic = %cfg.topic, qos = cfg.qos, "forwarding over mqtt");

    'session: loop {
        let (client, mut events) = match connect(&cfg, limit) {
            Ok(session) => session,
            Err(e) => {
                warn!(error = %e, "cannot create mqtt client");
                metrics.last_error.set(&e);
                if !pause(attempt, &token).await {
                    break;
                }
                attempt += 1;
                continue;
            }
        };
        for entry in tracker.reset() {
            publish(&client, &cfg, qos, entry);
        }
        let mut connected = false;
        loop {
            tokio::select! {
                event = events.recv() => match event {
                    Some(Ok(Event::Incoming(Packet::ConnAck(_)))) => {
                        connected = true;
                        attempt = 0;
                        metrics.connected.store(true, Relaxed);
                        metrics.last_error.clear();
                        info!(outstanding = tracker.outstanding(), "connected");
                    }
                    Some(Ok(Event::Outgoing(Outgoing::Publish(pkid)))) => {
                        tally(&metrics, tracker.sent(pkid));
                        cursor.advance(tracker.cursor());
                    }
                    Some(Ok(Event::Incoming(Packet::PubAck(ack)))) => {
                        tally(&metrics, tracker.acked(ack.pkid));
                        cursor.advance(tracker.cursor());
                    }
                    Some(Ok(Event::Incoming(Packet::PubComp(comp)))) => {
                        tally(&metrics, tracker.acked(comp.pkid));
                        cursor.advance(tracker.cursor());
                    }
                    Some(Ok(_)) => {}
                    Some(Err(e)) => {
                        metrics.connected.store(false, Relaxed);
                        metrics.last_error.set(&e);
                        warn!(error = %e, outstanding = tracker.outstanding(), "connection lost");
                        if !pause(attempt, &token).await {
                            break 'session;
                        }
                        attempt += 1;
                        continue 'session;
                    }
                    None => {
                        metrics.connected.store(false, Relaxed);
                        warn!("event loop ended");
                        if !pause(attempt, &token).await {
                            break 'session;
                        }
                        attempt += 1;
                        continue 'session;
                    }
                },
                // Bounded by count and by payload bytes; the library holds a
                // copy of every unacknowledged publish as well.
                delivery = feed.next(limit - tracker.outstanding(), DELIVERY_BYTES.saturating_sub(tracker.bytes())),
                    if connected && tracker.outstanding() < limit && tracker.bytes() < DELIVERY_BYTES => {
                    submit(delivery, &client, &cfg, qos, &mut tracker, &metrics);
                }
                _ = flush.tick() => cursor.flush(false).await,
                _ = token.cancelled() => break 'session,
            }
        }
    }
    cursor.flush(true).await;
}

/// Sleeps for the reconnect backoff; false if cancelled meanwhile.
async fn pause(attempt: u32, token: &CancellationToken) -> bool {
    let delay = backoff(attempt, BACKOFF_BASE, BACKOFF_MAX);
    debug!(delay_s = delay.as_secs(), "reconnecting after backoff");
    tokio::select! {
        _ = tokio::time::sleep(delay) => true,
        _ = token.cancelled() => false,
    }
}

type Events = mpsc::Receiver<Result<Event, ConnectionError>>;

/// Builds a client and drives its event loop on a task that forwards events;
/// the task ends on the first connection error or when the receiver is dropped.
fn connect(cfg: &config::Mqtt, limit: usize) -> Result<(AsyncClient, Events), String> {
    let mut options = MqttOptions::new(cfg.client_id.clone(), cfg.host.clone(), cfg.port);
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
        let tls = ClientConfig::with_platform_verifier().map_err(|e| format!("tls: {e}"))?;
        options.set_transport(Transport::tls_with_config(TlsConfiguration::Rustls(
            Arc::new(tls),
        )));
    }
    let (client, mut eventloop) = AsyncClient::new(options, limit);
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
                    metrics.rejected.fetch_add(1, Relaxed);
                    metrics
                        .last_error
                        .set(format!("seq {}: payload too large", record.seq));
                    let notice = Notice::Rejected {
                        seq: record.seq,
                        ts: record.ts,
                        reason: notice::TOO_LARGE,
                    };
                    tracker.push_notice(record.seq, notice.encode())
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
    let counter = match completed {
        Some(Completed::Record) => &metrics.delivered,
        Some(Completed::Notice) => &metrics.notices,
        None => return,
    };
    counter.fetch_add(1, Relaxed);
}

/// One publish: a record, or a notice in place of the records up to `seq`
/// that the channel will not deliver.
struct Entry {
    /// The last sequence number the publish accounts for.
    seq: u64,
    payload: Vec<u8>,
    notice: bool,
    pkid: Option<u16>,
    done: bool,
}

/// What a completed publish carried.
#[derive(Debug, PartialEq, Eq)]
enum Completed {
    Record,
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
}

impl Tracker {
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

    /// Payload bytes kept for republishing.
    fn bytes(&self) -> usize {
        self.bytes
    }

    /// Adds the publish of a record; returns it for publishing.
    fn push(&mut self, record: Record) -> &Entry {
        self.enqueue(record.seq, record.payload, false)
    }

    /// Adds the publish of a notice that stands for the records up to `seq`.
    fn push_notice(&mut self, seq: u64, payload: Vec<u8>) -> &Entry {
        self.enqueue(seq, payload, true)
    }

    fn enqueue(&mut self, seq: u64, payload: Vec<u8>, notice: bool) -> &Entry {
        self.bytes += payload.len();
        self.entries.push_back(Entry {
            seq,
            payload,
            notice,
            pkid: None,
            done: false,
        });
        self.entries.back().expect("just pushed")
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
        Completed::Record
    };
    (released, completed)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(seq: u64) -> Record {
        Record {
            seq,
            ts: 0,
            payload: vec![seq as u8],
        }
    }

    const RECORD: Option<Completed> = Some(Completed::Record);
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
}
