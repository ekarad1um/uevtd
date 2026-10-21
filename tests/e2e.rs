//! End-to-end: a daemon with an HTTP channel pointed at a local server,
//! driven through its inbound socket and HTTP API.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU16, Ordering::Relaxed};
use std::time::{Duration, Instant};

use axum::Router;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, Method, StatusCode};
use axum::routing::post;
use serde_json::Value;
use tokio::io::AsyncWriteExt;
use tokio::net::UnixStream;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use uevtd::config::{self, Channel, Config, Http};
use uevtd::daemon::{self, Options};

struct Received {
    method: Method,
    headers: HeaderMap,
    body: Vec<u8>,
}

struct Target {
    url: String,
    requests: mpsc::UnboundedReceiver<Received>,
    status: Arc<AtomicU16>,
}

async fn target() -> Target {
    let (tx, requests) = mpsc::unbounded_channel();
    let status = Arc::new(AtomicU16::new(200));
    let state = (tx, status.clone());
    let app = Router::new()
        .route("/ingest", post(ingest))
        .with_state(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();
    tokio::spawn(axum::serve(listener, app).into_future());
    Target {
        url: format!("http://{addr}/ingest"),
        requests,
        status,
    }
}

async fn ingest(
    State((tx, status)): State<(mpsc::UnboundedSender<Received>, Arc<AtomicU16>)>,
    method: Method,
    headers: HeaderMap,
    body: Bytes,
) -> StatusCode {
    let _ = tx.send(Received {
        method,
        headers,
        body: body.to_vec(),
    });
    StatusCode::from_u16(status.load(Relaxed)).unwrap()
}

struct Env {
    _dir: tempfile::TempDir,
    config_path: PathBuf,
    inbound: PathBuf,
    api: PathBuf,
}

impl Env {
    fn new() -> Env {
        let dir = tempfile::Builder::new().prefix("uevtd").tempdir().unwrap();
        let path = dir.path().to_path_buf();
        Env {
            _dir: dir,
            config_path: path.join("config.json"),
            inbound: path.join("in.sock"),
            api: path.join("api.sock"),
        }
    }

    fn config(&self, url: &str) -> Config {
        let mut c = Config::default();
        c.inbound.path = self.inbound.clone();
        c.inbound.max_message_bytes = 1024;
        c.api.path = self.api.clone();
        c.storage.path = self.config_path.parent().unwrap().join("log");
        c.storage.max_bytes = 2 << 20;
        c.storage.segment_bytes = 64 << 10;
        c.outbound.channels.insert("t".into(), http(url, "1"));
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

    async fn send(&self, lines: &[&[u8]]) {
        let mut stream = UnixStream::connect(&self.inbound).await.unwrap();
        for line in lines {
            stream.write_all(line).await.unwrap();
        }
        stream.shutdown().await.unwrap();
    }
}

fn http(url: &str, test_header: &str) -> Channel {
    Channel::Http(Http {
        url: url.into(),
        headers: BTreeMap::from([("X-Test".to_string(), test_header.to_string())]),
        timeout_ms: 2000,
        ..Http::default()
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

async fn next_request(target: &mut Target) -> Received {
    tokio::time::timeout(Duration::from_secs(10), target.requests.recv())
        .await
        .expect("request arrives")
        .unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn forwards_queries_reconfigures_and_resumes() {
    let mut target = target().await;
    let env = Env::new();
    let daemon = env.start(env.config(&target.url)).await;

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

    for expected in ["{\"n\":1}", "{\"n\":2}", "not json"] {
        let r = next_request(&mut target).await;
        assert_eq!(r.method, Method::POST);
        assert_eq!(r.headers.get("content-type").unwrap(), "application/json");
        assert_eq!(r.headers.get("x-test").unwrap(), "1");
        assert_eq!(r.body, expected.as_bytes());
    }

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
        daemon.get("/v1/status").await.1["channels"]["t"]["lag"] == 0
    })
    .await;
    let (_, status) = daemon.get("/v1/status").await;
    assert_eq!(status["inbound"]["messages"], 3);
    assert_eq!(status["inbound"]["oversized"], 1);
    assert_eq!(status["inbound"]["truncated"], 1);
    assert_eq!(status["storage"]["head_seq"], 3);
    assert_eq!(status["channels"]["t"]["delivered"], 3);
    assert_eq!(status["channels"]["t"]["cursor"], 4);
    assert_eq!(status["restart_required"], serde_json::json!([]));

    // Reconfigure the channel: it restarts and keeps its position.
    let (status, applied) = daemon
        .patch(serde_json::json!({
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
        serde_json::json!(["storage.path"])
    );
    let (status, err) = daemon
        .patch(serde_json::json!({"storage": {"max_bytes": 1}}))
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(err["error"].as_str().unwrap().contains("max_bytes"));

    env.send(&[b"{\"n\":4}\n"]).await;
    let r = next_request(&mut target).await;
    assert_eq!(r.headers.get("x-test").unwrap(), "2");
    assert_eq!(r.body, b"{\"n\":4}");

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
        metrics.contains("uevtd_channel_delivered_total{channel=\"t\",kind=\"http\"} 4"),
        "{metrics}"
    );
    assert!(metrics.contains("uevtd_inbound_truncated_total 1"));

    // Restore the storage path before restarting on the same directory.
    let (status, _) = daemon
        .patch(serde_json::json!({"storage": {"path": env.config(&target.url).storage.path}}))
        .await;
    assert_eq!(status, StatusCode::OK);
    daemon.stop().await;
    assert!(!env.api.exists());
    assert!(!env.inbound.exists());

    // Restart: sequence numbers continue and nothing is redelivered.
    let daemon = env.start(config::load(&env.config_path).unwrap()).await;
    env.send(&[b"{\"n\":5}\n"]).await;
    let r = next_request(&mut target).await;
    assert_eq!(r.body, b"{\"n\":5}");
    assert!(
        tokio::time::timeout(Duration::from_millis(300), target.requests.recv())
            .await
            .is_err()
    );
    let (_, status) = daemon.get("/v1/status").await;
    assert_eq!(status["storage"]["head_seq"], 5);
    assert_eq!(status["channels"]["t"]["cursor"], 6);
    daemon.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn retries_transient_failures_and_skips_rejections() {
    let mut target = target().await;
    let env = Env::new();
    let daemon = env.start(env.config(&target.url)).await;

    target.status.store(503, Relaxed);
    env.send(&[b"{\"n\":1}\n"]).await;
    let first = next_request(&mut target).await;
    assert_eq!(first.body, b"{\"n\":1}");
    let retry = next_request(&mut target).await;
    assert_eq!(retry.body, b"{\"n\":1}");
    target.status.store(200, Relaxed);
    wait_for(Duration::from_secs(10), || async {
        daemon.get("/v1/status").await.1["channels"]["t"]["delivered"] == 1
    })
    .await;
    while target.requests.try_recv().is_ok() {}

    target.status.store(400, Relaxed);
    env.send(&[b"{\"n\":2}\n", b"{\"n\":3}\n"]).await;
    let _ = next_request(&mut target).await;
    let _ = next_request(&mut target).await;
    wait_for(Duration::from_secs(5), || async {
        daemon.get("/v1/status").await.1["channels"]["t"]["cursor"] == 4
    })
    .await;
    let (_, status) = daemon.get("/v1/status").await;
    assert_eq!(status["channels"]["t"]["rejected"], 2);
    assert!(status["channels"]["t"]["retries"].as_u64().unwrap() >= 1);
    assert!(
        status["channels"]["t"]["last_error"]["message"]
            .as_str()
            .unwrap()
            .contains("400")
    );
    daemon.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn backlog_survives_channel_restart_before_first_delivery() {
    let mut target = target().await;
    let env = Env::new();
    target.status.store(503, Relaxed);
    let daemon = env.start(env.config(&target.url)).await;
    env.send(&[b"{\"n\":1}\n", b"{\"n\":2}\n", b"{\"n\":3}\n"])
        .await;
    let first = next_request(&mut target).await;
    assert_eq!(first.body, b"{\"n\":1}");

    // Reconfigure the channel and restart the daemon while nothing has been delivered.
    let (status, _) = daemon
        .patch(serde_json::json!({"outbound": {"channels": {"t": {"headers": {"X-Test": "2"}}}}}))
        .await;
    assert_eq!(status, StatusCode::OK);
    daemon.stop().await;
    let daemon = env.start(config::load(&env.config_path).unwrap()).await;
    target.status.store(200, Relaxed);

    let mut seen = Vec::new();
    while seen.last() != Some(&3) {
        let body: Value = serde_json::from_slice(&next_request(&mut target).await.body).unwrap();
        let n = body["n"].as_u64().unwrap();
        if seen.last() != Some(&n) {
            seen.push(n);
        }
    }
    assert_eq!(seen, vec![1, 2, 3]);
    wait_for(Duration::from_secs(5), || async {
        daemon.get("/v1/status").await.1["channels"]["t"]["cursor"] == 4
    })
    .await;
    let (_, status) = daemon.get("/v1/status").await;
    assert_eq!(status["channels"]["t"]["delivered"], 3);
    assert_eq!(status["channels"]["t"]["lost"], 0);
    daemon.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn positive_sync_interval_commits_on_the_timer_and_on_stop() {
    let mut target = target().await;
    let env = Env::new();
    let mut config = env.config(&target.url);
    config.storage.sync_interval_ms = 50;
    let daemon = env.start(config).await;
    env.send(&[b"{\"n\":1}\n", b"{\"n\":2}\n", b"{\"n\":3}\n"])
        .await;
    for n in 1..=3 {
        let r = next_request(&mut target).await;
        assert_eq!(r.body, format!("{{\"n\":{n}}}").as_bytes());
    }

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
        let body: Value = serde_json::from_slice(&next_request(&mut target).await.body).unwrap();
        let n = body["n"].as_u64().unwrap();
        if seen.last() != Some(&n) {
            seen.push(n);
        }
    }
    assert_eq!(seen, vec![4, 5]);
    daemon.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_patches_compose() {
    let target = target().await;
    let env = Env::new();
    let daemon = env.start(env.config(&target.url)).await;
    for i in 1..=10u64 {
        let a = daemon.patch(serde_json::json!({"storage": {"sync_interval_ms": i}}));
        let b = daemon.patch(serde_json::json!({"inbound": {"max_connections": 100 + i}}));
        let ((status_a, _), (status_b, _)) = tokio::join!(a, b);
        assert_eq!((status_a, status_b), (StatusCode::OK, StatusCode::OK));
    }
    let (_, config) = daemon.get("/v1/config").await;
    assert_eq!(config["storage"]["sync_interval_ms"], 10);
    assert_eq!(config["inbound"]["max_connections"], 110);
    assert_eq!(
        config::load(&env.config_path)
            .unwrap()
            .inbound
            .max_connections,
        110
    );
    daemon.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn refuses_to_steal_a_live_socket() {
    let target = target().await;
    let env = Env::new();
    let daemon = env.start(env.config(&target.url)).await;
    let other = Env::new();
    let mut config = other.config(&target.url);
    config.inbound.path = env.inbound.clone();
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
    assert!(result.unwrap_err().to_string().contains("inbound"));
    daemon.stop().await;
}
