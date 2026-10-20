//! MQTT channel: pipelined publishes with packet-id tracking so the cursor
//! only advances on acknowledgement, and daemon-managed reconnection that
//! republishes unacknowledged messages in order (ARCHITECTURE.md §6.3).

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

use super::{ChannelMetrics, CursorStore, Feed, backoff};
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
        for record in tracker.reset() {
            publish(&client, &cfg, qos, &record);
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
                        metrics.delivered.fetch_add(tracker.sent(pkid) as u64, Relaxed);
                        cursor.advance(tracker.cursor());
                    }
                    Some(Ok(Event::Incoming(Packet::PubAck(ack)))) => {
                        metrics.delivered.fetch_add(tracker.acked(ack.pkid) as u64, Relaxed);
                        cursor.advance(tracker.cursor());
                    }
                    Some(Ok(Event::Incoming(Packet::PubComp(comp)))) => {
                        metrics.delivered.fetch_add(tracker.acked(comp.pkid) as u64, Relaxed);
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
                records = feed.next(limit - tracker.outstanding()), if connected && tracker.outstanding() < limit => {
                    for record in records {
                        if record.payload.len() > cfg.max_packet_bytes {
                            warn!(seq = record.seq, bytes = record.payload.len(), max = cfg.max_packet_bytes, "payload too large; skipping");
                            metrics.rejected.fetch_add(1, Relaxed);
                            metrics.last_error.set(format!("seq {}: payload too large", record.seq));
                            tracker.push_done(record.seq);
                        } else {
                            publish(&client, &cfg, qos, &record);
                            tracker.push(record);
                        }
                    }
                    cursor.advance(tracker.cursor());
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

fn publish(client: &AsyncClient, cfg: &config::Mqtt, qos: QoS, record: &Record) {
    // Capacity is guaranteed by the in-flight bound; a failure means the event
    // loop is gone, which the next event report handles.
    if let Err(e) = client.try_publish(&cfg.topic, qos, cfg.retain, record.payload.clone()) {
        debug!(seq = record.seq, error = %e, "publish not queued");
    }
}

struct Entry {
    record: Record,
    pkid: Option<u16>,
    done: bool,
}

/// Maps the library's packet ids to log sequence numbers and derives the
/// cursor as the end of the longest completed prefix.
pub struct Tracker {
    entries: VecDeque<Entry>,
    cursor: u64,
}

impl Tracker {
    pub fn new(cursor: u64) -> Self {
        Self {
            entries: VecDeque::new(),
            cursor,
        }
    }

    pub fn cursor(&self) -> u64 {
        self.cursor
    }

    pub fn outstanding(&self) -> usize {
        self.entries.len()
    }

    pub fn push(&mut self, record: Record) {
        self.entries.push_back(Entry {
            record,
            pkid: None,
            done: false,
        });
    }

    /// Records a sequence number as handled without publishing it.
    pub fn push_done(&mut self, seq: u64) {
        self.entries.push_back(Entry {
            record: Record {
                seq,
                ts: 0,
                payload: Vec::new(),
            },
            pkid: None,
            done: true,
        });
        self.advance();
    }

    /// A publish was written with `pkid`. Binds it to the oldest unsent entry;
    /// packet id 0 (QoS 0) completes that entry. Returns completed entries.
    pub fn sent(&mut self, pkid: u16) -> usize {
        if pkid != 0 && self.entries.iter().any(|e| !e.done && e.pkid == Some(pkid)) {
            return 0; // retransmission of an entry we already track
        }
        let Some(entry) = self
            .entries
            .iter_mut()
            .find(|e| !e.done && e.pkid.is_none())
        else {
            return 0;
        };
        if pkid == 0 {
            entry.done = true;
            self.advance();
            1
        } else {
            entry.pkid = Some(pkid);
            0
        }
    }

    /// The broker acknowledged `pkid`. Returns completed entries.
    pub fn acked(&mut self, pkid: u16) -> usize {
        let Some(entry) = self
            .entries
            .iter_mut()
            .find(|e| !e.done && e.pkid == Some(pkid))
        else {
            return 0;
        };
        entry.done = true;
        self.advance();
        1
    }

    /// Forgets packet ids after a reconnect; returns what must be republished, in order.
    pub fn reset(&mut self) -> Vec<Record> {
        let mut again = Vec::new();
        for entry in &mut self.entries {
            if !entry.done {
                entry.pkid = None;
                again.push(entry.record.clone());
            }
        }
        again
    }

    fn advance(&mut self) {
        while let Some(front) = self.entries.front() {
            if !front.done {
                break;
            }
            self.cursor = front.record.seq + 1;
            self.entries.pop_front();
        }
    }
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

    #[test]
    fn qos1_in_order() {
        let mut t = Tracker::new(10);
        t.push(rec(10));
        t.push(rec(11));
        assert_eq!(t.sent(1), 0);
        assert_eq!(t.sent(2), 0);
        assert_eq!(t.acked(1), 1);
        assert_eq!(t.cursor(), 11);
        assert_eq!(t.acked(2), 1);
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
        assert_eq!(t.acked(3), 1);
        assert_eq!(t.cursor(), 1);
        assert_eq!(t.acked(1), 1);
        assert_eq!(t.cursor(), 2);
        assert_eq!(t.acked(2), 1);
        assert_eq!(t.cursor(), 4);
        assert_eq!(t.acked(2), 0);
    }

    #[test]
    fn qos0_completes_on_send_and_retransmits_are_ignored() {
        let mut t = Tracker::new(5);
        t.push(rec(5));
        t.push(rec(6));
        assert_eq!(t.sent(0), 1);
        assert_eq!(t.cursor(), 6);
        assert_eq!(t.sent(7), 0);
        assert_eq!(t.sent(7), 0);
        assert_eq!(t.outstanding(), 1);
        assert_eq!(t.acked(7), 1);
        assert_eq!(t.cursor(), 7);
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
        t.push_done(5);
        let again: Vec<u64> = t.reset().iter().map(|r| r.seq).collect();
        assert_eq!(again, vec![1, 3, 4]);
        t.sent(9);
        t.sent(10);
        t.sent(11);
        assert_eq!(t.acked(9), 1);
        assert_eq!(t.cursor(), 3);
        t.acked(10);
        t.acked(11);
        assert_eq!(t.cursor(), 6);
        assert_eq!(t.outstanding(), 0);
    }
}
