//! Configuration: schema with defaults, validation, atomic persistence, and
//! live distribution to components through a `watch` channel.

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::{Map, Value};
use tokio::sync::watch;
use tokio_tungstenite::tungstenite::http::{HeaderName, HeaderValue};
use tracing::info;

use crate::outbound::notice::MAX_NOTICE_BYTES;
use crate::storage::SEGMENT_OVERHEAD;

#[derive(Clone, Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub inbound: Inbound,
    pub storage: Storage,
    pub outbound: Outbound,
    pub api: Api,
    pub log: Log,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Inbound {
    pub path: PathBuf,
    pub mode: Mode,
    pub max_message_bytes: usize,
    pub max_connections: usize,
}

impl Default for Inbound {
    fn default() -> Self {
        Self {
            path: "/run/uevtd/in.sock".into(),
            mode: Mode(0o660),
            max_message_bytes: 64 << 10,
            max_connections: 64,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Storage {
    pub path: PathBuf,
    pub max_bytes: u64,
    pub segment_bytes: u64,
    /// 0 syncs after every batch; otherwise at most once per interval.
    pub sync_interval_ms: u64,
}

impl Default for Storage {
    fn default() -> Self {
        Self {
            path: "/var/lib/uevtd".into(),
            max_bytes: 256 << 20,
            segment_bytes: 8 << 20,
            sync_interval_ms: 0,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Outbound {
    pub channels: BTreeMap<String, Channel>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum Channel {
    WebSocket(WebSocket),
    Mqtt(Mqtt),
}

impl Channel {
    pub fn enabled(&self) -> bool {
        match self {
            Channel::WebSocket(c) => c.enabled,
            Channel::Mqtt(c) => c.enabled,
        }
    }

    pub fn kind(&self) -> &'static str {
        match self {
            Channel::WebSocket(_) => "websocket",
            Channel::Mqtt(_) => "mqtt",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct WebSocket {
    pub enabled: bool,
    /// `ws://` or `wss://`.
    pub url: String,
    /// Sent with the handshake.
    pub headers: BTreeMap<String, String>,
    /// PEM certificates trusted instead of the platform's store.
    pub ca_file: Option<PathBuf>,
    /// For the handshake, and for a pong after each ping.
    pub timeout_ms: u64,
    /// Ping interval.
    pub keep_alive_s: u64,
    /// Maximum unacknowledged frames.
    pub inflight: u16,
    /// Records whose frame would be larger are replaced by a drop notice.
    pub max_frame_bytes: usize,
}

impl Default for WebSocket {
    fn default() -> Self {
        Self {
            enabled: true,
            url: String::new(),
            headers: BTreeMap::new(),
            ca_file: None,
            timeout_ms: 10_000,
            keep_alive_s: 30,
            inflight: 256,
            max_frame_bytes: 1 << 20,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Mqtt {
    pub enabled: bool,
    pub host: String,
    pub port: u16,
    pub tls: bool,
    /// PEM certificates trusted instead of the platform's store.
    pub ca_file: Option<PathBuf>,
    pub topic: String,
    pub client_id: String,
    pub username: Option<String>,
    pub password: Option<String>,
    pub qos: u8,
    pub retain: bool,
    pub keep_alive_s: u64,
    /// Maximum unacknowledged publishes in flight.
    pub inflight: u16,
    /// Payloads larger than this are replaced by a drop notice.
    pub max_packet_bytes: usize,
}

impl Default for Mqtt {
    fn default() -> Self {
        Self {
            enabled: true,
            host: String::new(),
            port: 1883,
            tls: false,
            ca_file: None,
            topic: String::new(),
            client_id: "uevtd".into(),
            username: None,
            password: None,
            qos: 1,
            retain: false,
            keep_alive_s: 30,
            inflight: 32,
            max_packet_bytes: 1 << 20,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Api {
    pub path: PathBuf,
    pub mode: Mode,
}

impl Default for Api {
    fn default() -> Self {
        Self {
            path: "/run/uevtd/api.sock".into(),
            mode: Mode(0o660),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Log {
    /// `tracing` filter directives, e.g. `info` or `info,uevtd::storage=debug`.
    pub filter: String,
    pub format: LogFormat,
}

impl Default for Log {
    fn default() -> Self {
        Self {
            filter: "info".into(),
            format: LogFormat::Text,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LogFormat {
    Text,
    Json,
}

/// Unix permission bits, written as an octal string such as `"0660"`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Mode(pub u32);

impl Serialize for Mode {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&format!("{:04o}", self.0))
    }
}

impl<'de> Deserialize<'de> for Mode {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let text = String::deserialize(d)?;
        u32::from_str_radix(&text, 8)
            .ok()
            .filter(|m| *m <= 0o777)
            .map(Mode)
            .ok_or_else(|| {
                serde::de::Error::custom(format!(
                    "invalid mode {text:?}; expected octal such as \"0660\""
                ))
            })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{0}")]
    Invalid(String),
    #[error("invalid JSON: {0}")]
    Parse(#[from] serde_json::Error),
    #[error("{0}")]
    Io(#[from] io::Error),
}

fn ensure(ok: bool, message: impl FnOnce() -> String) -> Result<(), Error> {
    if ok {
        Ok(())
    } else {
        Err(Error::Invalid(message()))
    }
}

impl Config {
    pub fn validate(&self) -> Result<(), Error> {
        let inbound = &self.inbound;
        ensure(!inbound.path.as_os_str().is_empty(), || {
            "inbound.path must not be empty".into()
        })?;
        ensure(!self.api.path.as_os_str().is_empty(), || {
            "api.path must not be empty".into()
        })?;
        ensure(inbound.path != self.api.path, || {
            "inbound.path and api.path must differ".into()
        })?;
        // Every buffer that holds messages is sized from these two, so their
        // bounds are what keeps memory bounded (ARCHITECTURE.md §11.1).
        ensure((1..=256 << 10).contains(&inbound.max_message_bytes), || {
            "inbound.max_message_bytes must be between 1 and 262144".into()
        })?;
        ensure((1..=256).contains(&inbound.max_connections), || {
            "inbound.max_connections must be between 1 and 256".into()
        })?;
        ensure(
            inbound.max_connections * inbound.max_message_bytes <= 16 << 20,
            || {
                "inbound.max_connections times inbound.max_message_bytes must be at most 16777216, \
                 since each connection may hold one partial message"
                    .into()
            },
        )?;

        let storage = &self.storage;
        ensure(!storage.path.as_os_str().is_empty(), || {
            "storage.path must not be empty".into()
        })?;
        ensure(
            (64 << 10..=256 << 20).contains(&storage.segment_bytes),
            || "storage.segment_bytes must be between 65536 and 268435456".into(),
        )?;
        ensure(storage.max_bytes <= 1 << 40, || {
            "storage.max_bytes must be at most 1099511627776".into()
        })?;
        ensure(
            storage
                .segment_bytes
                .checked_mul(2)
                .is_some_and(|min| storage.max_bytes >= min),
            || "storage.max_bytes must be at least twice storage.segment_bytes".into(),
        )?;
        ensure(storage.sync_interval_ms <= 60_000, || {
            "storage.sync_interval_ms must be at most 60000".into()
        })?;
        ensure(
            storage.segment_bytes >= inbound.max_message_bytes as u64 + SEGMENT_OVERHEAD,
            || {
                format!(
                    "storage.segment_bytes must be at least inbound.max_message_bytes + \
                     {SEGMENT_OVERHEAD}, so that every message fits in a segment"
                )
            },
        )?;

        ensure(self.outbound.channels.len() <= 8, || {
            "outbound.channels must not hold more than 8 channels".into()
        })?;
        for (name, channel) in &self.outbound.channels {
            validate_name(name)?;
            match channel {
                Channel::WebSocket(c) => validate_websocket(name, c)?,
                Channel::Mqtt(c) => validate_mqtt(name, c)?,
            }
        }

        tracing_subscriber::EnvFilter::try_new(&self.log.filter)
            .map_err(|e| Error::Invalid(format!("log.filter: {e}")))?;
        Ok(())
    }

    /// Copy with secrets replaced, for logging.
    pub fn redacted(&self) -> Config {
        let mut copy = self.clone();
        for channel in copy.outbound.channels.values_mut() {
            match channel {
                Channel::WebSocket(c) => {
                    c.url = redacted_url(&c.url);
                    for (name, value) in c.headers.iter_mut() {
                        if secret_name(name) {
                            *value = "***".into();
                        }
                    }
                }
                Channel::Mqtt(c) => {
                    if c.password.is_some() {
                        c.password = Some("***".into());
                    }
                }
            }
        }
        copy
    }

    /// Fields that differ from the configuration the process started with and
    /// only take effect after a restart.
    pub fn restart_required(&self, boot: &Config) -> Vec<&'static str> {
        let mut fields = Vec::new();
        if self.storage.path != boot.storage.path {
            fields.push("storage.path");
        }
        if self.log.format != boot.log.format {
            fields.push("log.format");
        }
        fields
    }

    fn changed_sections(&self, other: &Config) -> Vec<&'static str> {
        let mut sections = Vec::new();
        if self.inbound != other.inbound {
            sections.push("inbound");
        }
        if self.storage != other.storage {
            sections.push("storage");
        }
        if self.outbound != other.outbound {
            sections.push("outbound");
        }
        if self.api != other.api {
            sections.push("api");
        }
        if self.log != other.log {
            sections.push("log");
        }
        sections
    }
}

/// Header and query parameter names that usually carry a secret.
fn secret_name(name: &str) -> bool {
    let lower = name.to_ascii_lowercase();
    [
        "authorization",
        "token",
        "key",
        "secret",
        "cookie",
        "password",
    ]
    .iter()
    .any(|s| lower.contains(s))
}

/// The URL with user information and secret-looking query values replaced,
/// for logging.
pub fn redacted_url(url: &str) -> String {
    let Ok(mut parsed) = url::Url::parse(url) else {
        return url.to_string();
    };
    let mut changed = false;
    if !parsed.username().is_empty() || parsed.password().is_some() {
        let _ = parsed.set_username("***");
        let _ = parsed.set_password(None);
        changed = true;
    }
    let pairs: Vec<(String, String)> = parsed
        .query_pairs()
        .map(|(k, v)| (k.into_owned(), v.into_owned()))
        .collect();
    if pairs.iter().any(|(k, _)| secret_name(k)) {
        let mut query = parsed.query_pairs_mut();
        query.clear();
        for (k, v) in &pairs {
            query.append_pair(k, if secret_name(k) { "***" } else { v });
        }
        drop(query);
        changed = true;
    }
    if changed {
        parsed.to_string()
    } else {
        url.to_string()
    }
}

fn validate_name(name: &str) -> Result<(), Error> {
    let mut chars = name.chars();
    let ok = name.len() <= 64
        && chars.next().is_some_and(|c| c.is_ascii_alphanumeric())
        && chars.all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | '-'));
    ensure(ok, || {
        format!("channel name {name:?} must match [A-Za-z0-9][A-Za-z0-9_.-]{{0,63}}")
    })
}

/// Request headers that the WebSocket handshake sets itself.
const HANDSHAKE_HEADERS: [&str; 5] = [
    "host",
    "connection",
    "upgrade",
    "sec-websocket-key",
    "sec-websocket-version",
];

fn validate_websocket(name: &str, c: &WebSocket) -> Result<(), Error> {
    let url =
        url::Url::parse(&c.url).map_err(|e| Error::Invalid(format!("channel {name}: url: {e}")))?;
    ensure(matches!(url.scheme(), "ws" | "wss"), || {
        format!("channel {name}: url must use ws or wss")
    })?;
    ensure(
        url.username().is_empty() && url.password().is_none(),
        || format!("channel {name}: url must not hold credentials; send them in headers"),
    )?;
    ensure(url.fragment().is_none(), || {
        format!("channel {name}: url must not have a fragment")
    })?;
    for (k, v) in &c.headers {
        let header = HeaderName::from_bytes(k.as_bytes())
            .map_err(|e| Error::Invalid(format!("channel {name}: header {k:?}: {e}")))?;
        HeaderValue::from_str(v)
            .map_err(|e| Error::Invalid(format!("channel {name}: header {k:?}: {e}")))?;
        ensure(!HANDSHAKE_HEADERS.contains(&header.as_str()), || {
            format!("channel {name}: header {k:?} is set by the handshake")
        })?;
    }
    ensure(c.ca_file.is_none() || url.scheme() == "wss", || {
        format!("channel {name}: ca_file needs a wss url")
    })?;
    ensure(c.timeout_ms >= 1, || {
        format!("channel {name}: timeout_ms must be at least 1")
    })?;
    ensure((1..=65_535).contains(&c.keep_alive_s), || {
        format!("channel {name}: keep_alive_s must be between 1 and 65535")
    })?;
    ensure((1..=1024).contains(&c.inflight), || {
        format!("channel {name}: inflight must be between 1 and 1024")
    })?;
    ensure(c.max_frame_bytes >= MAX_NOTICE_BYTES, || {
        format!(
            "channel {name}: max_frame_bytes must be at least {MAX_NOTICE_BYTES}, \
             so that a drop notice fits"
        )
    })
}

fn validate_mqtt(name: &str, c: &Mqtt) -> Result<(), Error> {
    ensure(!c.host.is_empty(), || {
        format!("channel {name}: host must not be empty")
    })?;
    ensure(c.port != 0, || {
        format!("channel {name}: port must not be 0")
    })?;
    ensure(c.ca_file.is_none() || c.tls, || {
        format!("channel {name}: ca_file needs tls")
    })?;
    ensure(
        !c.topic.is_empty() && rumqttc::valid_topic(&c.topic),
        || format!("channel {name}: topic must be a non-empty publish topic without wildcards"),
    )?;
    ensure(!c.client_id.is_empty(), || {
        format!("channel {name}: client_id must not be empty")
    })?;
    ensure(c.qos <= 2, || {
        format!("channel {name}: qos must be 0, 1 or 2")
    })?;
    ensure((1..=65_535).contains(&c.keep_alive_s), || {
        format!("channel {name}: keep_alive_s must be between 1 and 65535")
    })?;
    ensure((1..=1024).contains(&c.inflight), || {
        format!("channel {name}: inflight must be between 1 and 1024")
    })?;
    ensure(c.max_packet_bytes >= MAX_NOTICE_BYTES, || {
        format!(
            "channel {name}: max_packet_bytes must be at least {MAX_NOTICE_BYTES}, \
             so that a drop notice fits"
        )
    })
}

/// Reads and validates the configuration file.
pub fn load(path: &Path) -> Result<Config, Error> {
    parse(&fs::read_to_string(path)?)
}

/// Like [`load`], but a missing file yields the defaults, which are written
/// to the path so the effective configuration is visible on disk. Any other
/// failure to read the file is an error, never a reason to overwrite it.
pub fn load_or_init(path: &Path) -> Result<Config, Error> {
    match fs::read_to_string(path) {
        Ok(text) => parse(&text),
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            let config = Config::default();
            config.validate()?;
            save(path, &config)?;
            info!(path = %path.display(), "configuration file not found; wrote defaults");
            Ok(config)
        }
        Err(e) => Err(e.into()),
    }
}

fn parse(text: &str) -> Result<Config, Error> {
    let config: Config = serde_json::from_str(text)?;
    config.validate()?;
    Ok(config)
}

/// Writes the configuration atomically: temp file (mode 0600), fsync, rename,
/// directory fsync.
pub fn save(path: &Path, config: &Config) -> io::Result<()> {
    let dir = match path.parent() {
        Some(p) if !p.as_os_str().is_empty() => p.to_path_buf(),
        _ => PathBuf::from("."),
    };
    fs::create_dir_all(&dir)?;
    let name = path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("config.json");
    let tmp = dir.join(format!(".{name}.tmp"));
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(&tmp)?;
    serde_json::to_writer_pretty(&mut file, config).map_err(io::Error::other)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    fs::rename(&tmp, path)?;
    File::open(&dir)?.sync_all()?;
    Ok(())
}

/// RFC 7386 JSON merge patch.
pub fn merge_patch(target: &mut Value, patch: &Value) {
    match patch {
        Value::Object(patch) => {
            if !target.is_object() {
                *target = Value::Object(Map::new());
            }
            let target = target.as_object_mut().expect("object");
            for (key, value) in patch {
                if value.is_null() {
                    target.remove(key);
                } else {
                    merge_patch(target.entry(key.clone()).or_insert(Value::Null), value);
                }
            }
        }
        _ => *target = patch.clone(),
    }
}

/// The live configuration: the current value, its file, and a channel that
/// components watch for changes.
pub struct Store {
    path: PathBuf,
    tx: watch::Sender<Arc<Config>>,
    write: Mutex<()>,
}

impl Store {
    pub fn new(path: PathBuf, config: Config) -> Self {
        let (tx, _) = watch::channel(Arc::new(config));
        Self {
            path,
            tx,
            write: Mutex::new(()),
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn current(&self) -> Arc<Config> {
        self.tx.borrow().clone()
    }

    pub fn subscribe(&self) -> watch::Receiver<Arc<Config>> {
        self.tx.subscribe()
    }

    /// Validates, persists and publishes a new configuration.
    pub fn update(&self, config: Config) -> Result<Arc<Config>, Error> {
        let _guard = self.write.lock().unwrap();
        self.commit(config)
    }

    /// Applies an RFC 7386 merge patch to the current configuration and
    /// commits the result. Read, merge and write happen under one lock, so
    /// concurrent patches compose instead of overwriting each other.
    pub fn patch(&self, patch: &Value) -> Result<Arc<Config>, Error> {
        let _guard = self.write.lock().unwrap();
        let mut document = serde_json::to_value(self.current().as_ref())?;
        merge_patch(&mut document, patch);
        self.commit(serde_json::from_value(document)?)
    }

    /// Re-reads the file and publishes it without writing it back.
    pub fn reload(&self) -> Result<Arc<Config>, Error> {
        let _guard = self.write.lock().unwrap();
        let config = load(&self.path)?;
        Ok(self.publish(config))
    }

    fn commit(&self, config: Config) -> Result<Arc<Config>, Error> {
        config.validate()?;
        if *self.current() != config {
            save(&self.path, &config)?;
        }
        Ok(self.publish(config))
    }

    /// Publishes even an unchanged configuration: a component whose last
    /// apply failed (a rebind, say) retries when it sees the value again.
    fn publish(&self, config: Config) -> Arc<Config> {
        let changed = self.current().changed_sections(&config);
        let config = Arc::new(config);
        self.tx.send_replace(config.clone());
        if changed.is_empty() {
            info!("configuration re-applied");
        } else {
            info!(sections = ?changed, "configuration updated");
        }
        config
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn websocket(url: &str) -> Channel {
        Channel::WebSocket(WebSocket {
            url: url.into(),
            ..WebSocket::default()
        })
    }

    #[test]
    fn defaults_are_valid_and_round_trip() {
        let config = Config::default();
        config.validate().unwrap();
        let text = serde_json::to_string_pretty(&config).unwrap();
        assert!(text.contains("\"mode\": \"0660\""));
        assert_eq!(serde_json::from_str::<Config>(&text).unwrap(), config);
    }

    #[test]
    fn example_configuration_is_valid() {
        let config: Config =
            serde_json::from_str(include_str!("../contrib/config.example.json")).unwrap();
        config.validate().unwrap();
    }

    #[test]
    fn memory_bounds_are_validated() {
        let check = |f: &dyn Fn(&mut Config)| {
            let mut config = Config::default();
            f(&mut config);
            config.validate()
        };
        assert!(check(&|c| c.inbound.max_message_bytes = 256 << 10).is_ok());
        assert!(check(&|c| c.inbound.max_message_bytes = (256 << 10) + 1).is_err());
        assert!(check(&|c| c.inbound.max_connections = 256).is_ok());
        assert!(check(&|c| c.inbound.max_connections = 257).is_err());
        // Each limit alone is fine; together they exceed the 16 MiB for partial messages.
        assert!(
            check(&|c| {
                c.inbound.max_connections = 65;
                c.inbound.max_message_bytes = 256 << 10;
            })
            .is_err()
        );
        // The configuration files written by earlier versions held 64 connections of 1 MiB.
        assert!(check(&|c| c.inbound.max_message_bytes = 1 << 20).is_err());

        // A message must fit into a segment together with the headers.
        let fits = |segment_bytes: u64, max_message_bytes: usize| {
            check(&|c| {
                c.storage.segment_bytes = segment_bytes;
                c.inbound.max_message_bytes = max_message_bytes;
            })
        };
        assert!(fits(64 << 10, (64 << 10) - 48).is_ok());
        assert!(fits(64 << 10, (64 << 10) - 47).is_err());

        let channels = |n: usize| {
            check(&|c| {
                for i in 0..n {
                    c.outbound
                        .channels
                        .insert(format!("c{i}"), websocket("ws://h/x"));
                }
            })
        };
        assert!(channels(8).is_ok());
        assert!(channels(9).is_err());

        let inflight = |n: u16| {
            check(&|c| {
                c.outbound.channels.insert(
                    "m".into(),
                    Channel::Mqtt(Mqtt {
                        host: "h".into(),
                        topic: "t".into(),
                        inflight: n,
                        ..Mqtt::default()
                    }),
                );
            })
        };
        assert!(inflight(1024).is_ok());
        assert!(inflight(1025).is_err());

        // A channel's size limit must admit its drop notices.
        let packet = |max_packet_bytes: usize| {
            check(&|c| {
                c.outbound.channels.insert(
                    "m".into(),
                    Channel::Mqtt(Mqtt {
                        host: "h".into(),
                        topic: "t".into(),
                        max_packet_bytes,
                        ..Mqtt::default()
                    }),
                );
            })
        };
        assert!(packet(256).is_ok());
        assert!(packet(255).is_err());
    }

    #[test]
    fn channels_are_tagged_and_strict() {
        let text = r#"{"outbound":{"channels":{"a":{"type":"websocket","url":"ws://h/x"},
            "b":{"type":"mqtt","host":"h","topic":"t"}}}}"#;
        let config: Config = serde_json::from_str(text).unwrap();
        config.validate().unwrap();
        assert_eq!(config.outbound.channels.len(), 2);
        assert!(
            serde_json::from_str::<Config>(
                r#"{"outbound":{"channels":{"a":{"type":"websocket","url":"ws://h","bogus":1}}}}"#
            )
            .is_err()
        );
        assert!(serde_json::from_str::<Config>(r#"{"inbound":{"mode":"999"}}"#).is_err());
    }

    #[test]
    fn validation_rejects_bad_values() {
        let mut config = Config::default();
        config.storage.max_bytes = config.storage.segment_bytes;
        assert!(config.validate().is_err());

        // Would overflow `2 * segment_bytes`.
        let mut config = Config::default();
        config.storage.segment_bytes = 1 << 63;
        config.storage.max_bytes = 1;
        assert!(config.validate().is_err());

        let mut config = Config::default();
        config.storage.sync_interval_ms = 60_001;
        assert!(config.validate().is_err());

        let mut config = Config::default();
        config
            .outbound
            .channels
            .insert("bad name".into(), websocket("ws://h/"));
        assert!(config.validate().is_err());

        let mut config = Config::default();
        config
            .outbound
            .channels
            .insert("a".into(), websocket("ftp://h/"));
        assert!(config.validate().is_err());

        let mut config = Config::default();
        config.outbound.channels.insert(
            "m".into(),
            Channel::Mqtt(Mqtt {
                host: "h".into(),
                topic: "a/#".into(),
                ..Mqtt::default()
            }),
        );
        assert!(config.validate().is_err());

        let mut config = Config::default();
        config.log.filter = "uevtd=nolevel".into();
        assert!(config.validate().is_err());
    }

    #[test]
    fn websocket_channels_are_validated() {
        let check = |c: WebSocket| {
            let mut config = Config::default();
            config
                .outbound
                .channels
                .insert("w".into(), Channel::WebSocket(c));
            config.validate()
        };
        let base = |url: &str| WebSocket {
            url: url.into(),
            ..WebSocket::default()
        };
        assert!(check(base("ws://h:8080/x?device=1")).is_ok());
        assert!(check(base("wss://[::1]/x")).is_ok());
        assert!(check(base("https://h/x")).is_err());
        assert!(check(base("ws://user:pw@h/x")).is_err());
        assert!(check(base("ws://h/x#part")).is_err());
        let headers = |name: &str| WebSocket {
            headers: BTreeMap::from([(name.to_string(), "v".to_string())]),
            ..base("ws://h/x")
        };
        assert!(check(headers("Authorization")).is_ok());
        assert!(check(headers("Sec-WebSocket-Key")).is_err());
        assert!(check(headers("bad header")).is_err());
        let ca_file = |url: &str| WebSocket {
            ca_file: Some("/etc/ca.pem".into()),
            ..base(url)
        };
        assert!(check(ca_file("wss://h/x")).is_ok());
        assert!(check(ca_file("ws://h/x")).is_err());
        let sized = |inflight, max_frame_bytes| WebSocket {
            inflight,
            max_frame_bytes,
            ..base("ws://h/x")
        };
        assert!(check(sized(1024, MAX_NOTICE_BYTES)).is_ok());
        assert!(check(sized(1025, 1 << 20)).is_err());
        assert!(check(sized(0, 1 << 20)).is_err());
        assert!(check(sized(1, MAX_NOTICE_BYTES - 1)).is_err());

        let mut config = Config::default();
        config.outbound.channels.insert(
            "m".into(),
            Channel::Mqtt(Mqtt {
                host: "h".into(),
                topic: "t".into(),
                ca_file: Some("/etc/ca.pem".into()),
                ..Mqtt::default()
            }),
        );
        assert!(config.validate().is_err(), "ca_file without tls");

        // The HTTP channel is gone; a file that still has one says so.
        let err = serde_json::from_str::<Config>(
            r#"{"outbound":{"channels":{"a":{"type":"http","url":"http://h/x"}}}}"#,
        )
        .unwrap_err();
        assert!(err.to_string().contains("unknown variant `http`"), "{err}");
    }

    #[test]
    fn redaction_hides_secrets() {
        let mut config = Config::default();
        config.outbound.channels.insert(
            "a".into(),
            Channel::WebSocket(WebSocket {
                url: "wss://h/x?token=abc".into(),
                headers: BTreeMap::from([
                    ("Authorization".to_string(), "Bearer t".to_string()),
                    ("X-Id".to_string(), "1".to_string()),
                ]),
                ..WebSocket::default()
            }),
        );
        let redacted = config.redacted();
        let Channel::WebSocket(c) = &redacted.outbound.channels["a"] else {
            panic!("websocket channel");
        };
        assert_eq!(c.url, "wss://h/x?token=***");
        assert_eq!(c.headers["Authorization"], "***");
        assert_eq!(c.headers["X-Id"], "1");
        assert_eq!(redacted_url("wss://h/x"), "wss://h/x");
        assert_eq!(redacted_url("ws://h/x?a=1&b=2"), "ws://h/x?a=1&b=2");
        assert_eq!(
            redacted_url("wss://u:p@h/x?device=42&token=abc"),
            "wss://***@h/x?device=42&token=***"
        );
    }

    #[test]
    fn load_or_init_keeps_an_unparsable_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.json");
        fs::write(&path, "{").unwrap();
        assert!(matches!(load_or_init(&path), Err(Error::Parse(_))));
        assert_eq!(fs::read_to_string(&path).unwrap(), "{");
    }

    #[test]
    fn concurrent_patches_compose() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.json");
        let store = Store::new(path.clone(), Config::default());
        let rounds = 20u64;
        std::thread::scope(|s| {
            s.spawn(|| {
                for i in 1..=rounds {
                    store
                        .patch(&serde_json::json!({"storage": {"sync_interval_ms": i}}))
                        .unwrap();
                }
            });
            s.spawn(|| {
                for i in 1..=rounds {
                    store
                        .patch(&serde_json::json!({"inbound": {"max_connections": 100 + i}}))
                        .unwrap();
                }
            });
        });
        let current = store.current();
        assert_eq!(current.storage.sync_interval_ms, rounds);
        assert_eq!(current.inbound.max_connections as u64, 100 + rounds);
        assert_eq!(load(&path).unwrap(), *current);
    }

    #[test]
    fn unchanged_configuration_is_republished() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.json");
        let config = load_or_init(&path).unwrap();
        let store = Store::new(path, config.clone());
        let mut rx = store.subscribe();
        rx.borrow_and_update();
        store.update(config).unwrap();
        assert!(rx.has_changed().unwrap());
        rx.borrow_and_update();
        store.reload().unwrap();
        assert!(rx.has_changed().unwrap());
    }

    #[test]
    fn merge_patch_follows_rfc7386() {
        let mut doc = serde_json::json!({"a": {"b": 1, "c": 2}, "d": [1]});
        merge_patch(
            &mut doc,
            &serde_json::json!({"a": {"b": null, "e": 3}, "d": 4}),
        );
        assert_eq!(doc, serde_json::json!({"a": {"c": 2, "e": 3}, "d": 4}));
    }

    #[test]
    fn store_persists_and_publishes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.json");
        let store = Store::new(path.clone(), load_or_init(&path).unwrap());
        let mut rx = store.subscribe();
        let mut next = Config::default();
        next.outbound
            .channels
            .insert("a".into(), websocket("ws://h/x"));
        store.update(next.clone()).unwrap();
        assert!(rx.has_changed().unwrap());
        assert_eq!(**rx.borrow_and_update(), next);
        assert_eq!(load(&path).unwrap(), next);
        assert_eq!(store.redacted_for_test(), next.redacted());
    }

    impl Store {
        fn redacted_for_test(&self) -> Config {
            self.current().redacted()
        }
    }
}
