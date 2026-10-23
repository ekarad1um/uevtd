//! End-to-end: a daemon with channels pointed at a scripted WebSocket
//! receiver and a minimal MQTT broker, fed by a producer that listens on the
//! socket it reads, and driven through its HTTP API.

use std::collections::BTreeMap;
use std::io;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering::Relaxed};
use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use reqwest::StatusCode;
use serde_json::{Value, json};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, UnixListener, UnixStream};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_rustls::TlsAcceptor;
use tokio_rustls::rustls::ServerConfig;
use tokio_rustls::rustls::pki_types::pem::PemObject;
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};
use tokio_tungstenite::tungstenite::http::HeaderMap;
use tokio_util::sync::CancellationToken;

use uevtd::config::{self, Channel, Config, Mqtt, WebSocket};
use uevtd::daemon::{self, Options};

struct Env {
    _dir: tempfile::TempDir,
    config_path: PathBuf,
    inbound: PathBuf,
    api: PathBuf,
    /// The producer, listening on `inbound` for the daemon to connect.
    producer: Option<UnixListener>,
}

impl Env {
    fn new() -> Env {
        let dir = tempfile::Builder::new().prefix("uevtd").tempdir().unwrap();
        let path = dir.path().to_path_buf();
        let inbound = path.join("in.sock");
        Env {
            _dir: dir,
            config_path: path.join("config.json"),
            producer: Some(UnixListener::bind(&inbound).unwrap()),
            inbound,
            api: path.join("api.sock"),
        }
    }

    /// The producer stops, and removes its socket.
    fn stop_producer(&mut self) {
        self.producer = None;
        std::fs::remove_file(&self.inbound).unwrap();
    }

    fn start_producer(&mut self) {
        self.producer = Some(UnixListener::bind(&self.inbound).unwrap());
    }

    /// A configuration without channels.
    fn base(&self) -> Config {
        let mut c = Config::default();
        c.inbound.path = self.inbound.clone();
        c.inbound.max_message_bytes = 1024;
        c.api.path = self.api.clone();
        c.storage.path = self.config_path.parent().unwrap().join("log");
        c.storage.max_bytes = 2 << 20;
        c.storage.segment_bytes = 64 << 10;
        c
    }

    /// With one WebSocket channel `t` that sends the header `X-Test: 1`.
    fn config(&self, url: &str) -> Config {
        let mut c = self.base();
        c.outbound.channels.insert("t".into(), websocket(url, "1"));
        c
    }

    async fn start(&self, config: Config) -> Daemon {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .with_test_writer()
            .try_init();
        config::save(&self.config_path, &config).unwrap();
        let token = CancellationToken::new();
        let task = tokio::spawn(daemon::run(
            Options {
                config_path: self.config_path.clone(),
                config,
                log: None,
            },
            token.clone(),
        ));
        let client = reqwest::Client::builder()
            .unix_socket(self.api.clone())
            .build()
            .unwrap();
        let daemon = Daemon {
            token,
            task,
            client,
        };
        wait_for(Duration::from_secs(10), || async {
            daemon
                .client
                .get("http://uevtd/v1/health")
                .send()
                .await
                .is_ok_and(|r| r.status().is_success())
        })
        .await;
        daemon
    }

    /// The next connection the daemon made to the producer.
    async fn accept(&self) -> UnixStream {
        let producer = self.producer.as_ref().expect("the producer listens");
        let accepted = tokio::time::timeout(Duration::from_secs(10), producer.accept());
        accepted.await.expect("the daemon connects").unwrap().0
    }

    /// Writes `lines` on a connection of their own, which the producer then
    /// closes; the daemon connects again at once.
    async fn send(&self, lines: &[&[u8]]) {
        use io::ErrorKind::{BrokenPipe, ConnectionReset};
        let data = lines.concat();
        loop {
            let mut stream = self.accept().await;
            // A connection the daemon closed while it waited to be accepted,
            // as one made just before it stopped, fails the first write.
            let written = match stream.write(&data).await {
                Err(e) if matches!(e.kind(), BrokenPipe | ConnectionReset) => continue,
                written => written.unwrap(),
            };
            stream.write_all(&data[written..]).await.unwrap();
            stream.shutdown().await.unwrap();
            return;
        }
    }
}

/// Pings every second, so that every test also exercises them.
fn websocket(url: &str, test_header: &str) -> Channel {
    Channel::WebSocket(WebSocket {
        url: url.into(),
        headers: BTreeMap::from([("X-Test".to_string(), test_header.to_string())]),
        keep_alive_s: 1,
        ..WebSocket::default()
    })
}

struct Daemon {
    token: CancellationToken,
    task: JoinHandle<anyhow::Result<()>>,
    client: reqwest::Client,
}

impl Daemon {
    async fn get(&self, path: &str) -> (StatusCode, Value) {
        let response = self
            .client
            .get(format!("http://uevtd{path}"))
            .send()
            .await
            .unwrap();
        let status = response.status();
        let body = response.bytes().await.unwrap();
        (status, serde_json::from_slice(&body).unwrap_or(Value::Null))
    }

    async fn channel(&self, name: &str) -> Value {
        self.get("/v1/status").await.1["channels"][name].take()
    }

    async fn patch(&self, body: Value) -> (StatusCode, Value) {
        let response = self
            .client
            .patch("http://uevtd/v1/config")
            .body(body.to_string())
            .send()
            .await
            .unwrap();
        let status = response.status();
        let body = response.bytes().await.unwrap();
        (status, serde_json::from_slice(&body).unwrap())
    }

    async fn stop(self) {
        self.token.cancel();
        tokio::time::timeout(Duration::from_secs(10), self.task)
            .await
            .expect("daemon stops")
            .unwrap()
            .unwrap();
    }
}

async fn wait_for<F, Fut>(timeout: Duration, mut condition: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let deadline = Instant::now() + timeout;
    while !condition().await {
        assert!(Instant::now() < deadline, "condition not met in time");
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn listen() -> TcpListener {
    TcpListener::bind("127.0.0.1:0").await.unwrap()
}

/// An address that nothing listens on yet.
async fn unused_address() -> SocketAddr {
    listen().await.local_addr().unwrap()
}

fn data(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/data")
        .join(name)
}

/// TLS with the test certificate for `localhost` and `127.0.0.1`, signed by
/// the test CA in `tests/data/ca.pem`.
fn tls_acceptor() -> TlsAcceptor {
    let certs = CertificateDer::pem_file_iter(data("server.pem"))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let key = PrivateKeyDer::from_pem_file(data("server.key")).unwrap();
    let config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .unwrap();
    TlsAcceptor::from(Arc::new(config))
}

// How the receiver answers each frame.
/// Acknowledges it.
const ACK: u8 = 0;
/// Refuses every record and acknowledges notices.
const NACK: u8 = 1;
/// Answers nothing.
const SILENT: u8 = 2;
/// Drops the connection on the next frame without answering, then acknowledges.
const DROP_ONCE: u8 = 3;
/// Drops the connection on every frame.
const DROP: u8 = 4;
/// Stops reading after the next frame, without closing; pings go unanswered.
const STALL: u8 = 5;

enum Event {
    /// A handshake, with its request headers.
    Connected(HeaderMap, Instant),
    Frame(Value),
}

/// A WebSocket receiver that reports every connection and frame, in order,
/// and answers frames as `mode` says.
struct Receiver {
    url: String,
    events: mpsc::UnboundedReceiver<Event>,
    mode: Arc<AtomicU8>,
}

impl Receiver {
    fn start(listener: TcpListener, tls: Option<TlsAcceptor>) -> Receiver {
        let scheme = if tls.is_some() { "wss" } else { "ws" };
        let url = format!("{scheme}://{}/ingest", listener.local_addr().unwrap());
        let (tx, events) = mpsc::unbounded_channel();
        let mode = Arc::new(AtomicU8::new(ACK));
        let shared = mode.clone();
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let (tx, mode, tls) = (tx.clone(), shared.clone(), tls.clone());
                tokio::spawn(async move {
                    match tls {
                        Some(tls) => {
                            if let Ok(stream) = tls.accept(stream).await {
                                receive(stream, tx, mode).await;
                            }
                        }
                        None => receive(stream, tx, mode).await,
                    }
                });
            }
        });
        Receiver { url, events, mode }
    }

    fn set(&self, mode: u8) {
        self.mode.store(mode, Relaxed);
    }

    async fn event(&mut self) -> Event {
        tokio::time::timeout(Duration::from_secs(30), self.events.recv())
            .await
            .expect("the receiver sees something")
            .unwrap()
    }

    async fn connected(&mut self) -> HeaderMap {
        match self.event().await {
            Event::Connected(headers, _) => headers,
            Event::Frame(frame) => panic!("a frame where a connection was expected: {frame}"),
        }
    }

    async fn frame(&mut self) -> Value {
        match self.event().await {
            Event::Frame(frame) => frame,
            Event::Connected(..) => panic!("a connection where a frame was expected"),
        }
    }
}

async fn receive<S>(stream: S, events: mpsc::UnboundedSender<Event>, mode: Arc<AtomicU8>)
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut headers = HeaderMap::new();
    // The signature, and its large error, are tungstenite's.
    #[allow(clippy::result_large_err)]
    let callback = |request: &Request, response: Response| {
        headers = request.headers().clone();
        Ok(response)
    };
    let Ok(mut socket) = tokio_tungstenite::accept_hdr_async(stream, callback).await else {
        return;
    };
    let _ = events.send(Event::Connected(headers, Instant::now()));
    while let Some(Ok(message)) = socket.next().await {
        let Message::Text(text) = message else {
            continue;
        };
        let frame: Value = serde_json::from_str(&text).unwrap();
        let seq = frame["seq"].as_u64().unwrap();
        let notice = frame.get("notice").is_some();
        // Taken before the frame is reported, so that a test reacting to the
        // frame decides the next one, never this one.
        let now = mode
            .compare_exchange(DROP_ONCE, ACK, Relaxed, Relaxed)
            .unwrap_or_else(|current| current);
        let _ = events.send(Event::Frame(frame));
        let answer = match now {
            ACK => json!({"ack": seq}),
            NACK if notice => json!({"ack": seq}),
            NACK => json!({"nack": seq, "reason": "refused by test"}),
            SILENT => continue,
            STALL => return std::future::pending().await,
            _ => return,
        };
        if socket
            .send(Message::text(answer.to_string()))
            .await
            .is_err()
        {
            return;
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn forwards_queries_reconfigures_and_resumes() {
    let mut receiver = Receiver::start(listen().await, None);
    let env = Env::new();
    let daemon = env.start(env.config(&receiver.url)).await;
    assert_eq!(receiver.connected().await["x-test"], "1");

    // Three messages, one oversized line, one truncated tail.
    let big = format!("{{\"big\":\"{}\"}}\n", "x".repeat(2000));
    env.send(&[
        b"{\"n\":1}\n",
        b"{\"n\":2}\r\n",
        big.as_bytes(),
        b"not json\n",
        b"{\"n\":3",
    ])
    .await;
    let frames = vec![
        receiver.frame().await,
        receiver.frame().await,
        receiver.frame().await,
    ];
    assert_eq!(frames[0]["seq"], 1);
    assert_eq!(frames[0]["data"], json!({"n": 1}));
    assert_eq!(frames[1]["data"], json!({"n": 2}));
    assert_eq!(frames[2]["seq"], 3);
    assert_eq!(frames[2]["text"], "not json");
    // A frame and a query return the same record the same way.
    let (_, all) = daemon.get("/v1/messages").await;
    assert_eq!(all["messages"], Value::Array(frames));

    let (status, messages) = daemon.get("/v1/messages?limit=2").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(messages["messages"][0]["seq"], 1);
    assert_eq!(messages["messages"][0]["data"]["n"], 1);
    assert_eq!(messages["messages"][1]["data"]["n"], 2);
    assert_eq!(messages["next"], 2);
    let (_, page) = daemon.get("/v1/messages?after=2").await;
    assert_eq!(page["messages"][0]["seq"], 3);
    assert_eq!(page["messages"][0]["text"], "not json");
    assert_eq!(page["messages"][0].get("data"), None);
    assert_eq!(page["next"], Value::Null);
    let ts = messages["messages"][1]["ts"].as_i64().unwrap();
    let (_, ranged) = daemon
        .get(&format!("/v1/messages?from={ts}&to={}", ts + 1))
        .await;
    assert_eq!(ranged["messages"].as_array().unwrap().len(), 1);
    let (status, err) = daemon.get("/v1/messages?limit=0").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(err["error"].is_string());

    wait_for(Duration::from_secs(5), || async {
        daemon.channel("t").await["lag"] == 0
    })
    .await;
    let (_, status) = daemon.get("/v1/status").await;
    assert_eq!(status["inbound"]["messages"], 3);
    assert_eq!(status["inbound"]["oversized"], 1);
    assert_eq!(status["inbound"]["truncated"], 1);
    assert_eq!(status["storage"]["head_seq"], 3);
    let channel = &status["channels"]["t"];
    assert_eq!(channel["kind"], "websocket");
    assert_eq!(channel["connected"], true);
    assert_eq!(channel["delivered"], 3);
    assert_eq!(channel["cursor"], 4);
    // Pinged every second all along, and answered.
    assert_eq!(channel["retries"], 0);
    assert_eq!(status["restart_required"], json!([]));

    // Reconfigure the channel: it reconnects with the new header and keeps
    // its position.
    let (status, applied) = daemon
        .patch(json!({
            "outbound": {"channels": {"t": {"headers": {"X-Test": "2"}}}},
            "storage": {"path": "/elsewhere"},
        }))
        .await;
    assert_eq!(status, StatusCode::OK, "{applied}");
    assert_eq!(
        applied["outbound"]["channels"]["t"]["headers"]["X-Test"],
        "2"
    );
    assert_eq!(
        config::load(&env.config_path).unwrap().storage.path,
        PathBuf::from("/elsewhere")
    );
    assert_eq!(
        daemon.get("/v1/status").await.1["restart_required"],
        json!(["storage.path"])
    );
    let (status, err) = daemon.patch(json!({"storage": {"max_bytes": 1}})).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(err["error"].as_str().unwrap().contains("max_bytes"));
    assert_eq!(receiver.connected().await["x-test"], "2");

    env.send(&[b"{\"n\":4}\n"]).await;
    let frame = receiver.frame().await;
    assert_eq!(
        (&frame["seq"], &frame["data"]),
        (&json!(4), &json!({"n": 4}))
    );
    // The receiver sees the frame before the daemon sees its acknowledgement.
    wait_for(Duration::from_secs(5), || async {
        daemon.channel("t").await["delivered"] == 4
    })
    .await;

    let metrics = daemon
        .client
        .get("http://uevtd/v1/metrics")
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(
        metrics.contains("uevtd_channel_delivered_total{channel=\"t\",kind=\"websocket\"} 4"),
        "{metrics}"
    );
    assert!(metrics.contains("uevtd_inbound_truncated_total 1"));

    // Restore the storage path before restarting on the same directory.
    let (status, _) = daemon
        .patch(json!({"storage": {"path": env.base().storage.path}}))
        .await;
    assert_eq!(status, StatusCode::OK);
    daemon.stop().await;
    assert!(!env.api.exists());
    // The producer's socket is left alone.
    assert!(env.inbound.exists());

    // Restart: sequence numbers continue and nothing is sent again.
    let daemon = env.start(config::load(&env.config_path).unwrap()).await;
    assert_eq!(receiver.connected().await["x-test"], "2");
    env.send(&[b"{\"n\":5}\n"]).await;
    let frame = receiver.frame().await;
    assert_eq!((&frame["seq"], &frame["data"]["n"]), (&json!(5), &json!(5)));
    assert!(
        tokio::time::timeout(Duration::from_millis(300), receiver.events.recv())
            .await
            .is_err()
    );
    wait_for(Duration::from_secs(5), || async {
        daemon.channel("t").await["cursor"] == 6
    })
    .await;
    assert_eq!(daemon.get("/v1/status").await.1["storage"]["head_seq"], 5);
    daemon.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn status_shows_what_waits_and_errors_are_json() {
    let mut receiver = Receiver::start(listen().await, None);
    receiver.set(SILENT);
    let env = Env::new();
    let mut config = env.config(&receiver.url);
    let mut off = websocket(&receiver.url, "off");
    if let Channel::WebSocket(c) = &mut off {
        c.enabled = false;
    }
    config.outbound.channels.insert("off".into(), off);
    let daemon = env.start(config).await;
    receiver.connected().await;

    // Two frames go unanswered: they wait, and so does the oldest of them.
    env.send(&[b"{\"n\":1}\n", b"{\"n\":2}\n"]).await;
    receiver.frame().await;
    receiver.frame().await;
    let (_, page) = daemon.get("/v1/messages").await;
    let first_ts = page["messages"][0]["ts"].as_i64().unwrap();
    wait_for(Duration::from_secs(5), || async {
        daemon.channel("t").await["waiting_ts"] == first_ts
    })
    .await;
    let (_, status) = daemon.get("/v1/status").await;
    let storage = &status["storage"];
    assert_eq!(storage["first_ts"], first_ts);
    assert_eq!(
        (&storage["queued"], &storage["queued_bytes"]),
        (&json!(0), &json!(0))
    );
    assert_eq!(
        (&storage["dropped"], &storage["last_error"]),
        (&json!(0), &Value::Null)
    );
    assert!(storage["syncs"].as_u64().unwrap() >= 1, "{storage}");
    let channel = &status["channels"]["t"];
    assert_eq!(
        (&channel["lag"], &channel["delivered"]),
        (&json!(2), &json!(0))
    );
    // A channel disabled since it was added has no position yet.
    let off = &status["channels"]["off"];
    assert_eq!((&off["cursor"], &off["lag"]), (&Value::Null, &json!(0)));
    let metrics = daemon
        .client
        .get("http://uevtd/v1/metrics")
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    let seconds = format!("{}.{:06}", first_ts / 1_000_000, first_ts % 1_000_000);
    for sample in [
        format!("uevtd_storage_first_ts_seconds {seconds}"),
        format!("uevtd_channel_waiting_ts_seconds{{channel=\"t\",kind=\"websocket\"}} {seconds}"),
        "uevtd_storage_queued_messages 0".to_string(),
        "uevtd_channel_lag{channel=\"off\",kind=\"websocket\"} 0".to_string(),
    ] {
        assert!(metrics.contains(&sample), "{sample} in {metrics}");
    }
    assert!(
        !metrics.contains("uevtd_channel_cursor{channel=\"off\""),
        "{metrics}"
    );

    // One acknowledgement answers all three; nothing waits any more, and
    // each delivery took at least the time it waited.
    receiver.set(ACK);
    env.send(&[b"{\"n\":3}\n"]).await;
    receiver.frame().await;
    wait_for(Duration::from_secs(5), || async {
        daemon.channel("t").await["delivered"] == 3
    })
    .await;
    let channel = daemon.channel("t").await;
    assert_eq!(
        (&channel["lag"], &channel["waiting_ts"]),
        (&json!(0), &Value::Null)
    );
    assert!(channel["delivery_us"].as_u64().unwrap() > 0, "{channel}");

    // Errors are JSON, whatever their cause.
    let (status, body) = daemon.get("/v1/health").await;
    assert_eq!((status, body), (StatusCode::OK, Value::Null), "plain ok");
    let response = daemon
        .client
        .delete("http://uevtd/v1/config")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
    let allow = response.headers()["allow"].to_str().unwrap().to_string();
    assert!(allow.contains("PATCH"), "{allow}");
    let body: Value = serde_json::from_slice(&response.bytes().await.unwrap()).unwrap();
    assert_eq!(body["error"], "method not allowed");
    let response = daemon
        .client
        .put("http://uevtd/v1/config")
        .body(vec![b' '; 300 << 10])
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    let body: Value = serde_json::from_slice(&response.bytes().await.unwrap()).unwrap();
    assert!(body["error"].as_str().unwrap().contains("limit"), "{body}");
    let (status, body) = daemon.get("/v1/nothing").await;
    assert_eq!(
        (status, &body["error"]),
        (StatusCode::NOT_FOUND, &json!("not found"))
    );
    daemon.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn resends_after_a_lost_session_and_counts_refusals() {
    let mut receiver = Receiver::start(listen().await, None);
    let env = Env::new();
    let daemon = env.start(env.config(&receiver.url)).await;
    receiver.connected().await;

    // The receiver drops the connection without answering: the frame is
    // sent again in the next session.
    receiver.set(DROP_ONCE);
    env.send(&[b"{\"n\":1}\n"]).await;
    assert_eq!(receiver.frame().await["seq"], 1);
    receiver.connected().await;
    assert_eq!(receiver.frame().await["seq"], 1);
    wait_for(Duration::from_secs(5), || async {
        daemon.channel("t").await["delivered"] == 1
    })
    .await;
    let channel = daemon.channel("t").await;
    assert_eq!(channel["cursor"], 2);
    assert_eq!(channel["retries"], 1);

    // Refused records are counted and passed, and stay in the log.
    receiver.set(NACK);
    env.send(&[b"{\"n\":2}\n", b"{\"n\":3}\n"]).await;
    assert_eq!(receiver.frame().await["seq"], 2);
    assert_eq!(receiver.frame().await["seq"], 3);
    wait_for(Duration::from_secs(5), || async {
        daemon.channel("t").await["cursor"] == 4
    })
    .await;
    let channel = daemon.channel("t").await;
    assert_eq!(channel["rejected"], 2);
    assert_eq!(channel["delivered"], 1);
    let error = channel["last_error"]["message"].as_str().unwrap();
    assert!(
        error.contains("seq 3") && error.contains("refused by test"),
        "{error}"
    );
    let (_, page) = daemon.get("/v1/messages?after=1").await;
    assert_eq!(page["messages"].as_array().unwrap().len(), 2);
    daemon.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn backlog_survives_channel_restart_before_first_delivery() {
    let mut receiver = Receiver::start(listen().await, None);
    receiver.set(SILENT);
    let env = Env::new();
    let daemon = env.start(env.config(&receiver.url)).await;
    receiver.connected().await;
    env.send(&[b"{\"n\":1}\n", b"{\"n\":2}\n", b"{\"n\":3}\n"])
        .await;
    for seq in 1..=3 {
        assert_eq!(receiver.frame().await["seq"], seq);
    }

    // Reconfigure the channel and restart the daemon while nothing has been
    // acknowledged.
    let (status, _) = daemon
        .patch(json!({"outbound": {"channels": {"t": {"headers": {"X-Test": "2"}}}}}))
        .await;
    assert_eq!(status, StatusCode::OK);
    daemon.stop().await;
    receiver.set(ACK);
    let mut config = config::load(&env.config_path).unwrap();
    let Some(Channel::WebSocket(c)) = config.outbound.channels.get_mut("t") else {
        panic!("websocket channel");
    };
    c.headers.insert("X-Test".into(), "3".into());
    let daemon = env.start(config).await;

    // The session after the reconfiguration may have sent some frames again;
    // the session after the restart starts over at the first.
    while !matches!(receiver.event().await, Event::Connected(h, _) if h["x-test"] == "3") {}
    for n in 1..=3 {
        assert_eq!(receiver.frame().await["data"]["n"], n);
    }
    wait_for(Duration::from_secs(5), || async {
        daemon.channel("t").await["cursor"] == 4
    })
    .await;
    let channel = daemon.channel("t").await;
    assert_eq!(channel["delivered"], 3);
    assert_eq!(channel["lost"], 0);
    daemon.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn positive_sync_interval_commits_on_the_timer_and_on_stop() {
    let mut receiver = Receiver::start(listen().await, None);
    let env = Env::new();
    let mut config = env.config(&receiver.url);
    config.storage.sync_interval_ms = 50;
    let daemon = env.start(config).await;
    env.send(&[b"{\"n\":1}\n", b"{\"n\":2}\n", b"{\"n\":3}\n"])
        .await;
    let mut seen = Vec::new();
    while seen.len() < 3 {
        if let Event::Frame(frame) = receiver.event().await {
            seen.push(frame["data"]["n"].as_u64().unwrap());
        }
    }
    assert_eq!(seen, vec![1, 2, 3]);
    // Once frame 3's acknowledgement is in, the restart sends only what
    // follows it.
    wait_for(Duration::from_secs(5), || async {
        daemon.channel("t").await["cursor"] == 4
    })
    .await;

    // Stop right after sending: the final sync commits what the timer has not.
    env.send(&[b"{\"n\":4}\n", b"{\"n\":5}\n"]).await;
    wait_for(Duration::from_secs(5), || async {
        daemon.get("/v1/status").await.1["inbound"]["messages"] == 5
    })
    .await;
    daemon.stop().await;
    let daemon = env.start(config::load(&env.config_path).unwrap()).await;
    assert_eq!(daemon.get("/v1/status").await.1["storage"]["head_seq"], 5);
    let mut seen = Vec::new();
    while seen.last() != Some(&5) {
        if let Event::Frame(frame) = receiver.event().await {
            let n = frame["data"]["n"].as_u64().unwrap();
            if seen.last() != Some(&n) {
                seen.push(n);
            }
        }
    }
    assert_eq!(seen, vec![4, 5]);
    daemon.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_new_message_limit_applies_to_the_open_connection() {
    let env = Env::new();
    let daemon = env.start(env.base()).await;
    let inbound = || async { daemon.get("/v1/status").await.1["inbound"].take() };
    let mut open = env.accept().await;
    open.write_all(b"{\"n\":1}\n").await.unwrap();
    wait_for(Duration::from_secs(5), || async {
        inbound().await["messages"] == 1
    })
    .await;

    // A message of about 100 bytes, between the limits, on the connection
    // made under 1024: refused once the lower limit has reached it, and
    // taken again once the higher one has.
    let line = format!("{{\"pad\":\"{}\"}}\n", "p".repeat(100));
    for (max, counter) in [(64, "oversized"), (1024, "messages")] {
        let before = inbound().await[counter].as_u64().unwrap();
        let (status, _) = daemon
            .patch(json!({"inbound": {"max_message_bytes": max}}))
            .await;
        assert_eq!(status, StatusCode::OK);
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            open.write_all(line.as_bytes()).await.unwrap();
            tokio::time::sleep(Duration::from_millis(50)).await;
            if inbound().await[counter].as_u64().unwrap() > before {
                break;
            }
            assert!(Instant::now() < deadline, "the limit {max} never applied");
        }
    }
    assert_eq!(inbound().await["connections"], 1);
    daemon.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn reads_the_producer_once_it_listens_and_after_it_restarts() {
    let mut env = Env::new();
    env.stop_producer();
    let daemon = env.start(env.base()).await;
    let inbound = || async { daemon.get("/v1/status").await.1["inbound"].take() };
    let head = || async { daemon.get("/v1/status").await.1["storage"]["head_seq"].take() };

    // The daemon runs without its producer, and waits for it.
    wait_for(Duration::from_secs(5), || async {
        let error = inbound().await["last_error"]["message"].take();
        error.as_str().is_some_and(|e| e.starts_with("connect"))
    })
    .await;
    assert_eq!(inbound().await["connected"], false);

    env.start_producer();
    env.send(&[b"{\"n\":1}\n"]).await;
    wait_for(Duration::from_secs(5), || async { head().await == 1 }).await;
    // `send` closed its connection, and the daemon made another at once.
    wait_for(Duration::from_secs(5), || async {
        let inbound = inbound().await;
        inbound["connected"] == true && inbound["connections"] == 2
    })
    .await;
    assert_eq!(inbound().await["last_error"], Value::Null);
    let metrics = daemon
        .client
        .get("http://uevtd/v1/metrics")
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(metrics.contains("uevtd_inbound_connected 1"), "{metrics}");
    assert!(
        metrics.contains("uevtd_inbound_connections_total 2"),
        "{metrics}"
    );

    // The producer restarts.
    env.stop_producer();
    wait_for(Duration::from_secs(5), || async {
        inbound().await["connected"] == false
    })
    .await;
    env.start_producer();
    env.send(&[b"{\"n\":2}\n"]).await;
    wait_for(Duration::from_secs(5), || async { head().await == 2 }).await;
    daemon.stop().await;
    assert!(env.inbound.exists());
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_patches_compose() {
    let receiver = Receiver::start(listen().await, None);
    let env = Env::new();
    let daemon = env.start(env.config(&receiver.url)).await;
    for i in 1..=10u64 {
        let a = daemon.patch(json!({"storage": {"sync_interval_ms": i}}));
        let b = daemon.patch(json!({"inbound": {"max_message_bytes": 100 + i}}));
        let ((status_a, _), (status_b, _)) = tokio::join!(a, b);
        assert_eq!((status_a, status_b), (StatusCode::OK, StatusCode::OK));
    }
    let (_, config) = daemon.get("/v1/config").await;
    assert_eq!(config["storage"]["sync_interval_ms"], 10);
    assert_eq!(config["inbound"]["max_message_bytes"], 110);
    assert_eq!(
        config::load(&env.config_path)
            .unwrap()
            .inbound
            .max_message_bytes,
        110
    );
    daemon.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_document_not_shaped_like_a_configuration_changes_nothing() {
    let receiver = Receiver::start(listen().await, None);
    let env = Env::new();
    let daemon = env.start(env.config(&receiver.url)).await;
    let cursor = env.base().storage.path.join("cursors/t");
    wait_for(Duration::from_secs(10), || async { cursor.exists() }).await;
    // An empty JSON Patch, a section as an array, and an array for the
    // whole: serde would read each as a configuration without channels.
    for (method, body) in [
        (reqwest::Method::PATCH, "[]"),
        (reqwest::Method::PATCH, r#"{"outbound": []}"#),
        (reqwest::Method::PUT, "[]"),
    ] {
        let response = daemon
            .client
            .request(method.clone(), "http://uevtd/v1/config")
            .body(body)
            .send()
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            StatusCode::BAD_REQUEST,
            "{method} {body}"
        );
    }
    let (_, config) = daemon.get("/v1/config").await;
    assert!(config["outbound"]["channels"]["t"].is_object(), "{config}");
    assert!(cursor.exists());
    daemon.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn refuses_to_steal_a_live_socket() {
    let receiver = Receiver::start(listen().await, None);
    let env = Env::new();
    let daemon = env.start(env.config(&receiver.url)).await;
    let other = Env::new();
    let mut config = other.config(&receiver.url);
    config.api.path = env.api.clone();
    config::save(&other.config_path, &config).unwrap();
    let result = daemon::run(
        Options {
            config_path: other.config_path.clone(),
            config,
            log: None,
        },
        CancellationToken::new(),
    )
    .await;
    // The bind failure is the reported cause, not a consequence of it.
    let message = format!("{:#}", result.unwrap_err());
    assert!(
        message.starts_with("api") && message.contains("in use"),
        "{message}"
    );
    assert_eq!(daemon.get("/v1/health").await.0, StatusCode::OK);
    daemon.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_second_daemon_on_the_same_log_stops_before_it_changes_anything() {
    let receiver = Receiver::start(listen().await, None);
    let env = Env::new();
    let daemon = env.start(env.config(&receiver.url)).await;
    let log = env.base().storage.path;
    let cursor = log.join("cursors").join("t");
    wait_for(Duration::from_secs(10), || async { cursor.exists() }).await;
    // Other sockets and no channels: it would remove the cursor of `t`, and
    // run beside the first.
    let other = Env::new();
    let mut config = other.base();
    config.storage.path = log.clone();
    config::save(&other.config_path, &config).unwrap();
    let options = Options {
        config_path: other.config_path.clone(),
        config,
        log: None,
    };
    let run = daemon::run(options, CancellationToken::new());
    let result = tokio::time::timeout(Duration::from_secs(5), run)
        .await
        .expect("the second daemon stops");
    let message = format!("{:#}", result.unwrap_err());
    let pid = format!("pid {}", std::process::id());
    assert!(
        message.contains("locked by another process") && message.contains(&pid),
        "{message}"
    );
    assert!(cursor.exists());
    assert!(!other.api.exists());
    // Nor did it read from its producer.
    let producer = other.producer.as_ref().unwrap();
    let accepted = tokio::time::timeout(Duration::from_millis(100), producer.accept());
    assert!(accepted.await.is_err());
    daemon.stop().await;
}

/// 2 000 messages of about 420 bytes overflow a 256 KiB log; then one more
/// of about 610 bytes.
async fn overflow_then_one_large(env: &Env, daemon: &Daemon) {
    let pad = "p".repeat(400);
    let lines: Vec<String> = (1..=2000)
        .map(|n| format!("{{\"n\":{n},\"pad\":\"{pad}\"}}\n"))
        .collect();
    env.send(&lines.iter().map(|l| l.as_bytes()).collect::<Vec<_>>())
        .await;
    wait_for(Duration::from_secs(10), || async {
        daemon.get("/v1/status").await.1["storage"]["head_seq"] == 2000
    })
    .await;
    let large = format!("{{\"large\":\"{}\"}}\n", "l".repeat(600));
    env.send(&[large.as_bytes()]).await;
    wait_for(Duration::from_secs(10), || async {
        daemon.get("/v1/status").await.1["storage"]["head_seq"] == 2001
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn websocket_sends_notices_in_place_of_lost_and_oversized_records() {
    let addr = unused_address().await;
    let env = Env::new();
    let mut config = env.base();
    config.storage.max_bytes = 256 << 10;
    config.outbound.channels.insert(
        "w".into(),
        Channel::WebSocket(WebSocket {
            url: format!("ws://{addr}/ingest"),
            max_frame_bytes: 512,
            ..WebSocket::default()
        }),
    );
    let daemon = env.start(config).await;

    // The log overflows while the receiver is down. The records evicted are
    // counted as lost when they are evicted.
    overflow_then_one_large(&env, &daemon).await;
    let (_, status) = daemon.get("/v1/status").await;
    let first = status["storage"]["first_seq"].as_u64().unwrap();
    assert!(first > 1);
    let channel = &status["channels"]["w"];
    assert_eq!(channel["connected"], false);
    assert_eq!(channel["cursor"], 1);
    assert_eq!(channel["lost"], first - 1);

    // Once the receiver is up: a notice in place of the evicted records, the
    // records still in the log, then a notice in place of the large one. A
    // notice carries the sequence number to acknowledge, as a record does.
    let mut receiver = Receiver::start(TcpListener::bind(addr).await.unwrap(), None);
    receiver.connected().await;
    assert_eq!(
        receiver.frame().await,
        json!({"seq": first - 1, "notice": {"event": "lost", "from_seq": 1, "to_seq": first - 1, "count": first - 1}})
    );
    for n in first..=2000 {
        let frame = receiver.frame().await;
        assert_eq!((&frame["seq"], &frame["data"]["n"]), (&json!(n), &json!(n)));
    }
    let rejected = receiver.frame().await;
    assert_eq!(rejected["seq"], 2001);
    assert_eq!(rejected["notice"]["event"], "rejected");
    assert_eq!(rejected["notice"]["seq"], 2001);
    assert_eq!(rejected["notice"]["reason"], "payload too large");

    wait_for(Duration::from_secs(10), || async {
        daemon.channel("w").await["cursor"] == 2002
    })
    .await;
    let channel = daemon.channel("w").await;
    assert_eq!(channel["delivered"], 2001 - first);
    assert_eq!(channel["notices"], 2);
    assert_eq!(channel["rejected"], 1);
    assert_eq!(channel["lost"], first - 1);
    assert_eq!(channel["lag"], 0);
    daemon.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn websocket_reconnects_back_off_while_the_receiver_takes_nothing() {
    let mut receiver = Receiver::start(listen().await, None);
    receiver.set(DROP);
    let env = Env::new();
    let daemon = env.start(env.config(&receiver.url)).await;
    let Event::Connected(_, mut last) = receiver.event().await else {
        panic!("a connection first");
    };
    env.send(&[b"{\"n\":1}\n"]).await;
    // Every session ends at the first frame, so the cursor never advances and
    // each reconnect waits twice as long as the one before: 1 s, then 2 s.
    let mut gaps = Vec::new();
    while gaps.len() < 2 {
        if let Event::Connected(_, at) = receiver.event().await {
            gaps.push(at - last);
            last = at;
        }
    }
    assert!(gaps[0] >= Duration::from_millis(900), "{gaps:?}");
    assert!(gaps[1] >= Duration::from_millis(1900), "{gaps:?}");
    let channel = daemon.channel("t").await;
    assert_eq!(channel["delivered"], 0);
    assert!(channel["retries"].as_u64().unwrap() >= 2);
    daemon.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_receiver_that_stops_answering_pings_loses_its_session() {
    let mut receiver = Receiver::start(listen().await, None);
    receiver.set(STALL);
    let env = Env::new();
    let mut config = env.config(&receiver.url);
    let Some(Channel::WebSocket(c)) = config.outbound.channels.get_mut("t") else {
        panic!("websocket channel");
    };
    c.timeout_ms = 500;
    let daemon = env.start(config).await;
    receiver.connected().await;
    env.send(&[b"{\"n\":1}\n"]).await;
    assert_eq!(receiver.frame().await["seq"], 1);
    // The stalled receiver answers neither the frame nor the next ping; the
    // channel gives up on the session and sends the frame again in a new one.
    receiver.set(ACK);
    receiver.connected().await;
    assert_eq!(receiver.frame().await["seq"], 1);
    wait_for(Duration::from_secs(5), || async {
        daemon.channel("t").await["delivered"] == 1
    })
    .await;
    assert!(daemon.channel("t").await["retries"].as_u64().unwrap() >= 1);
    daemon.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn wss_trusts_the_ca_file_instead_of_the_platform_store() {
    let mut receiver = Receiver::start(listen().await, Some(tls_acceptor()));
    let env = Env::new();
    let mut config = env.base();
    let channel = |ca_file: Option<PathBuf>| {
        Channel::WebSocket(WebSocket {
            url: receiver.url.clone(),
            ca_file,
            ..WebSocket::default()
        })
    };
    config
        .outbound
        .channels
        .insert("trusted".into(), channel(Some(data("ca.pem"))));
    config
        .outbound
        .channels
        .insert("platform".into(), channel(None));
    let daemon = env.start(config).await;

    // Only the channel that trusts the test CA gets through.
    receiver.connected().await;
    env.send(&[b"{\"n\":1}\n"]).await;
    assert_eq!(receiver.frame().await["data"]["n"], 1);
    wait_for(Duration::from_secs(5), || async {
        daemon.channel("trusted").await["delivered"] == 1
    })
    .await;
    wait_for(Duration::from_secs(5), || async {
        daemon.channel("platform").await["last_error"]["message"]
            .as_str()
            .is_some_and(|m| m.starts_with("tls"))
    })
    .await;
    let platform = daemon.channel("platform").await;
    assert_eq!(platform["connected"], false);
    assert_eq!(platform["delivered"], 0);
    daemon.stop().await;
}

/// A PUBLISH as the broker received it.
struct Publish {
    topic: String,
    retain: bool,
    payload: Vec<u8>,
}

enum BrokerEvent {
    /// When, and the client id.
    Connected(Instant, String),
    Published(Publish),
}

/// A minimal MQTT 3.1.1 broker: accepts every client, acknowledges QoS 1
/// publishes or drops the connection on each, and reports connections and
/// publishes in the order they arrive.
fn broker(
    listener: TcpListener,
    tls: Option<TlsAcceptor>,
    drop_publishes: bool,
) -> mpsc::UnboundedReceiver<BrokerEvent> {
    let (tx, rx) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let (tx, tls) = (tx.clone(), tls.clone());
            tokio::spawn(async move {
                match tls {
                    Some(tls) => {
                        if let Ok(stream) = tls.accept(stream).await {
                            let _ = session(stream, tx, drop_publishes).await;
                        }
                    }
                    None => {
                        let _ = session(stream, tx, drop_publishes).await;
                    }
                }
            });
        }
    });
    rx
}

async fn session<S>(
    mut stream: S,
    tx: mpsc::UnboundedSender<BrokerEvent>,
    drop_publishes: bool,
) -> std::io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    loop {
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
        match header >> 4 {
            // CONNECT: accepted. The client id follows the protocol name,
            // level, flags and keep-alive.
            1 => {
                stream.write_all(&[0x20, 2, 0, 0]).await?;
                let len = usize::from(u16::from_be_bytes([body[10], body[11]]));
                let client_id = String::from_utf8(body[12..12 + len].to_vec()).unwrap();
                let _ = tx.send(BrokerEvent::Connected(Instant::now(), client_id));
            }
            // PUBLISH: topic, a packet id for QoS 1, then the payload.
            3 if drop_publishes => return Ok(()),
            3 => {
                let topic_end = 2 + usize::from(u16::from_be_bytes([body[0], body[1]]));
                let topic = String::from_utf8(body[2..topic_end].to_vec()).unwrap();
                let mut payload_start = topic_end;
                if (header >> 1) & 3 == 1 {
                    let pkid = [body[topic_end], body[topic_end + 1]];
                    stream.write_all(&[0x40, 2, pkid[0], pkid[1]]).await?;
                    payload_start += 2;
                }
                let _ = tx.send(BrokerEvent::Published(Publish {
                    topic,
                    retain: header & 1 == 1,
                    payload: body[payload_start..].to_vec(),
                }));
            }
            // PINGREQ
            12 => stream.write_all(&[0xd0, 0]).await?,
            // DISCONNECT
            14 => return Ok(()),
            _ => {}
        }
    }
}

async fn broker_event(events: &mut mpsc::UnboundedReceiver<BrokerEvent>) -> BrokerEvent {
    tokio::time::timeout(Duration::from_secs(30), events.recv())
        .await
        .expect("the broker sees something")
        .unwrap()
}

async fn next_publish(events: &mut mpsc::UnboundedReceiver<BrokerEvent>) -> Publish {
    loop {
        if let BrokerEvent::Published(publish) = broker_event(events).await {
            return publish;
        }
    }
}

async fn next_connection(events: &mut mpsc::UnboundedReceiver<BrokerEvent>) -> Instant {
    loop {
        if let BrokerEvent::Connected(at, _) = broker_event(events).await {
            return at;
        }
    }
}

async fn next_client_id(events: &mut mpsc::UnboundedReceiver<BrokerEvent>) -> String {
    loop {
        if let BrokerEvent::Connected(_, client_id) = broker_event(events).await {
            return client_id;
        }
    }
}

fn mqtt(addr: SocketAddr, topic: &str) -> Mqtt {
    Mqtt {
        host: addr.ip().to_string(),
        port: addr.port(),
        topic: topic.into(),
        ..Mqtt::default()
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn mqtt_publishes_notices_in_place_of_lost_and_rejected_records() {
    let addr = unused_address().await;
    let env = Env::new();
    let mut config = env.base();
    config.storage.max_bytes = 256 << 10;
    config.outbound.channels.insert(
        "m".into(),
        Channel::Mqtt(Mqtt {
            retain: true,
            max_packet_bytes: 512,
            ..mqtt(addr, "dev/events")
        }),
    );
    let daemon = env.start(config).await;

    // The log overflows while the broker is down. The records evicted are
    // counted as lost when they are evicted, not when the channel reads.
    overflow_then_one_large(&env, &daemon).await;
    let (_, status) = daemon.get("/v1/status").await;
    let first = status["storage"]["first_seq"].as_u64().unwrap();
    assert!(first > 1);
    let channel = &status["channels"]["m"];
    assert_eq!(channel["connected"], false);
    assert_eq!(channel["cursor"], 1);
    assert_eq!(channel["lost"], first - 1);
    assert_eq!(channel["lag"], 2002 - first);

    // Once the broker is up: a notice in place of the evicted records, the
    // records still in the log, then a notice in place of the large one.
    // Notices are never retained.
    let mut events = broker(TcpListener::bind(addr).await.unwrap(), None, false);
    let lost = next_publish(&mut events).await;
    assert_eq!((lost.topic.as_str(), lost.retain), ("dev/events", false));
    assert_eq!(
        serde_json::from_slice::<Value>(&lost.payload).unwrap(),
        json!({"notice": {"event": "lost", "from_seq": 1, "to_seq": first - 1, "count": first - 1}})
    );
    for n in first..=2000 {
        let record = next_publish(&mut events).await;
        assert!(record.retain);
        assert_eq!(
            serde_json::from_slice::<Value>(&record.payload).unwrap()["n"],
            n
        );
    }
    let rejected = next_publish(&mut events).await;
    assert!(!rejected.retain);
    let notice = serde_json::from_slice::<Value>(&rejected.payload).unwrap()["notice"].take();
    assert_eq!(notice["event"], "rejected");
    assert_eq!(notice["seq"], 2001);
    assert_eq!(notice["reason"], "payload too large");

    wait_for(Duration::from_secs(10), || async {
        daemon.channel("m").await["cursor"] == 2002
    })
    .await;
    let channel = daemon.channel("m").await;
    assert_eq!(channel["delivered"], 2001 - first);
    assert_eq!(channel["notices"], 2);
    assert_eq!(channel["rejected"], 1);
    assert_eq!(channel["lost"], first - 1);
    assert_eq!(channel["lag"], 0);

    // The rejected record stays in the log, under the time the notice gives.
    let (_, page) = daemon.get("/v1/messages?after=2000").await;
    assert_eq!(page["messages"][0]["seq"], 2001);
    assert_eq!(page["messages"][0]["ts"], notice["ts"]);
    assert!(page["messages"][0]["data"]["large"].is_string());
    daemon.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn mqtt_reconnects_back_off_while_the_broker_takes_nothing() {
    let listener = listen().await;
    let addr = listener.local_addr().unwrap();
    let mut events = broker(listener, None, true);
    let env = Env::new();
    let mut config = env.base();
    config
        .outbound
        .channels
        .insert("m".into(), Channel::Mqtt(mqtt(addr, "dev/events")));
    let daemon = env.start(config).await;
    let mut last = next_connection(&mut events).await;
    env.send(&[b"{\"n\":1}\n"]).await;
    // The broker accepts each connection and drops it at the first publish:
    // a CONNACK is no progress, so the waits grow, 1 s, then 2 s.
    let mut gaps = Vec::new();
    while gaps.len() < 2 {
        let at = next_connection(&mut events).await;
        gaps.push(at - last);
        last = at;
    }
    assert!(gaps[0] >= Duration::from_millis(900), "{gaps:?}");
    assert!(gaps[1] >= Duration::from_millis(1900), "{gaps:?}");
    let channel = daemon.channel("m").await;
    assert_eq!(channel["delivered"], 0);
    assert!(channel["retries"].as_u64().unwrap() >= 2);
    daemon.stop().await;
}

/// The host name, as `uname` reports it.
fn host_name() -> String {
    let out = std::process::Command::new("uname")
        .arg("-n")
        .output()
        .unwrap();
    String::from_utf8(out.stdout)
        .unwrap()
        .trim_end()
        .to_string()
}

#[tokio::test(flavor = "multi_thread")]
async fn mqtt_channels_connect_with_the_host_name_or_their_own_id() {
    let host_name = host_name();
    assert!(
        !host_name.is_empty() && host_name != "(none)",
        "{host_name:?}"
    );
    let listener = listen().await;
    let addr = listener.local_addr().unwrap();
    let mut events = broker(listener, None, false);
    let env = Env::new();
    let mut config = env.base();
    config
        .outbound
        .channels
        .insert("a".into(), Channel::Mqtt(mqtt(addr, "dev/a")));
    let daemon = env.start(config).await;
    // Without a client_id, a channel connects with the host name.
    assert_eq!(next_client_id(&mut events).await, host_name);

    // A second channel to the broker would connect with it as well, and
    // validation refuses that, whether the id is left out or given.
    let b = |client_id: Value| {
        json!({"outbound": {"channels": {"b": {
            "type": "mqtt",
            "host": addr.ip().to_string(),
            "port": addr.port(),
            "topic": "dev/b",
            "client_id": client_id,
        }}}})
    };
    for id in [Value::Null, json!(host_name)] {
        let (status, body) = daemon.patch(b(id)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        let error = body["error"].as_str().unwrap();
        assert!(error.contains("channels a and b"), "{error}");
    }

    // A configured id is used as given, the old default too, and a channel
    // restarted by a configuration change keeps its id.
    let patches = [
        b(json!("uevtd")),
        json!({"outbound": {"channels": {"a": {"topic": "dev/a2"}}}}),
    ];
    let mut ids = Vec::new();
    for patch in patches {
        assert_eq!(daemon.patch(patch).await.0, StatusCode::OK);
        ids.push(next_client_id(&mut events).await);
    }
    assert_eq!(ids, ["uevtd", host_name.as_str()]);
    daemon.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_client_id_shared_at_startup_is_reported_not_refused() {
    let host_name = host_name();
    let listener = listen().await;
    let addr = listener.local_addr().unwrap();
    let mut events = broker(listener, None, false);
    let env = Env::new();
    // As a file saved while the host had no name: now both channels connect
    // with the name it has.
    let mut config = env.base();
    for (name, topic) in [("a", "dev/a"), ("b", "dev/b")] {
        config
            .outbound
            .channels
            .insert(name.into(), Channel::Mqtt(mqtt(addr, topic)));
    }
    config::save(&env.config_path, &config).unwrap();
    let daemon = env.start(config::load(&env.config_path).unwrap()).await;
    for _ in 0..2 {
        assert_eq!(next_client_id(&mut events).await, host_name);
    }
    let conflict = |name: &'static str| {
        let daemon = &daemon;
        async move {
            let channel = daemon.channel(name).await;
            let message = channel["last_error"]["message"].as_str().unwrap_or("");
            message.contains("channels a and b both connect")
        }
    };
    // Shown while a session is up, which would otherwise clear it.
    for name in ["a", "b"] {
        wait_for(Duration::from_secs(5), || async {
            daemon.channel(name).await["connected"] == true && conflict(name).await
        })
        .await;
    }

    // A change elsewhere is taken; a third channel that would share the id
    // is not.
    let (status, body) = daemon
        .patch(json!({"inbound": {"max_message_bytes": 2048}}))
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, body) = daemon
        .patch(json!({"outbound": {"channels": {"c": {
            "type": "mqtt",
            "host": addr.ip().to_string(),
            "port": addr.port(),
            "topic": "dev/c",
        }}}}))
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    let error = body["error"].as_str().unwrap();
    assert!(error.contains("channels a and c"), "{error}");

    // An id of its own for one of them resolves it, for both.
    let (status, body) = daemon
        .patch(json!({"outbound": {"channels": {"b": {"client_id": "b"}}}}))
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(next_client_id(&mut events).await, "b");
    for name in ["a", "b"] {
        wait_for(Duration::from_secs(5), || async { !conflict(name).await }).await;
    }
    daemon.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn mqtt_over_tls_trusts_the_ca_file() {
    let listener = listen().await;
    let addr = listener.local_addr().unwrap();
    let mut events = broker(listener, Some(tls_acceptor()), false);
    let env = Env::new();
    let mut config = env.base();
    config.outbound.channels.insert(
        "m".into(),
        Channel::Mqtt(Mqtt {
            tls: true,
            ca_file: Some(data("ca.pem")),
            ..mqtt(addr, "dev/tls")
        }),
    );
    let daemon = env.start(config).await;
    env.send(&[b"{\"n\":1}\n"]).await;
    let publish = next_publish(&mut events).await;
    assert_eq!(
        (publish.topic.as_str(), &publish.payload[..]),
        ("dev/tls", &b"{\"n\":1}"[..])
    );
    wait_for(Duration::from_secs(5), || async {
        daemon.channel("m").await["delivered"] == 1
    })
    .await;
    daemon.stop().await;
}
