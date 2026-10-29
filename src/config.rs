//! Configuration: schema with defaults, validation, atomic persistence, and
//! live distribution to components through a `watch` channel.

use std::collections::BTreeMap;
use std::collections::btree_map::Entry;
use std::fs::{self, File, OpenOptions};
use std::hash::{BuildHasher, RandomState};
use std::io::{self, Write};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::SystemTime;

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::{Map, Value};
use tokio::sync::watch;
use tokio_tungstenite::tungstenite::http::{HeaderName, HeaderValue};
use tracing::{info, warn};

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
    /// The producer's socket, which it listens on and inbound connects to.
    pub path: PathBuf,
    pub max_message_bytes: usize,
}

impl Default for Inbound {
    fn default() -> Self {
        Self {
            path: "/run/uevtd/in.sock".into(),
            max_message_bytes: 64 << 10,
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
    /// The host name when `None`; see [`Mqtt::client_id`].
    pub client_id: Option<String>,
    pub username: Option<String>,
    pub password: Option<String>,
    pub qos: u8,
    pub retain: bool,
    /// For connecting, and for writing out each request; rounded up to whole
    /// seconds.
    pub timeout_ms: u64,
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
            client_id: None,
            username: None,
            password: None,
            qos: 1,
            retain: false,
            timeout_ms: 10_000,
            keep_alive_s: 30,
            inflight: 32,
            max_packet_bytes: 1 << 20,
        }
    }
}

impl Mqtt {
    /// The client id the channel `name` connects with: the configured one
    /// as given, or else the host name.
    pub fn client_id(&self, name: &str) -> String {
        self.client_id_on(host_name().as_deref(), name)
    }

    /// The client id on a host named `host_name`. A host without a name gets
    /// `uevtd` and 16 hex digits, distinct for every channel and process, and
    /// within the 23 letters and digits that every MQTT 3.1.1 broker accepts.
    fn client_id_on(&self, host_name: Option<&str>, name: &str) -> String {
        // Keys from the OS's random source, drawn once per process. The start
        // time goes in as well, in case that source is weak early in boot.
        static KEYS: LazyLock<(RandomState, SystemTime)> =
            LazyLock::new(|| (RandomState::new(), SystemTime::now()));
        match (&self.client_id, host_name) {
            (Some(id), _) => id.clone(),
            (None, Some(host_name)) => host_name.to_string(),
            (None, None) => {
                let (keys, started) = &*KEYS;
                format!("uevtd{:016x}", keys.hash_one((name, started)))
            }
        }
    }
}

/// The host's name, unless it has none: an empty one, or Linux's `(none)`,
/// which the kernel reports when nothing has set it.
fn host_name() -> Option<String> {
    let mut buf = [0u8; 256];
    // SAFETY: the buffer is valid for writes of its whole length.
    if unsafe { libc::gethostname(buf.as_mut_ptr().cast(), buf.len()) } != 0 {
        return None;
    }
    let len = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
    let name = String::from_utf8_lossy(&buf[..len]).into_owned();
    (!name.is_empty() && name != "(none)").then_some(name)
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
    /// Checks every field. Client ids that two channels share, and sockets
    /// that take each other's path, are checked against the configuration
    /// in effect instead ([`Store`]).
    pub fn validate(&self) -> Result<(), Error> {
        let inbound = &self.inbound;
        socket_path("inbound.path", &inbound.path)?;
        socket_path("api.path", &self.api.path)?;
        ensure(inbound.path != self.api.path, || {
            "inbound.path and api.path must differ".into()
        })?;
        // Every buffer that holds messages is sized from this one, so its
        // bound is what keeps memory bounded.
        ensure((1..=256 << 10).contains(&inbound.max_message_bytes), || {
            "inbound.max_message_bytes must be between 1 and 262144".into()
        })?;

        let storage = &self.storage;
        ensure(!storage.path.as_os_str().is_empty(), || {
            "storage.path must not be empty".into()
        })?;
        ensure(!storage.path.as_os_str().as_bytes().contains(&0), || {
            "storage.path must not contain U+0000".into()
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

    /// Refuses what this configuration cannot change in `current`, the one in
    /// effect:
    /// - A socket moved onto the path of the other one. Inbound and the API
    ///   apply a change each on its own, so inbound could connect to the
    ///   API's socket, and the API's bind could replace the producer's
    ///   socket while the producer is down, as it replaces a stale one.
    /// - A client id that two channels would share, unless they share it in
    ///   `current` already. The host name is read now, so a host renamed
    ///   since `current` was applied can make channels share one; a change
    ///   that leaves them alone need not resolve that, and outbound reports
    ///   it ([`client_id_conflicts`]).
    fn check_change(&self, current: &Config) -> Result<(), Error> {
        ensure(self.inbound.path != current.api.path, || {
            "inbound.path is the API's socket in effect; move that one first, in a change of its own"
                .into()
        })?;
        ensure(self.api.path != current.inbound.path, || {
            "api.path is the producer's socket in effect; move inbound.path first, in a change of its own"
                .into()
        })?;
        check_client_ids(
            &self.outbound.channels,
            &current.outbound.channels,
            host_name().as_deref(),
        )
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

/// Bytes a socket address holds for its path, the terminating NUL included:
/// 108 on Linux, 104 on macOS.
const SOCKET_PATH_ROOM: usize =
    std::mem::size_of::<libc::sockaddr_un>() - std::mem::offset_of!(libc::sockaddr_un, sun_path);

/// A path that a socket address holds, so that neither binding the API's
/// socket nor connecting to the producer's can fail on the path itself.
fn socket_path(field: &str, path: &Path) -> Result<(), Error> {
    let bytes = path.as_os_str().as_bytes();
    ensure(!bytes.is_empty(), || format!("{field} must not be empty"))?;
    ensure(!bytes.contains(&0), || {
        format!("{field} must not contain U+0000")
    })?;
    ensure(bytes.len() < SOCKET_PATH_ROOM, || {
        format!(
            "{field} must be at most {} bytes, as a socket address holds",
            SOCKET_PATH_ROOM - 1
        )
    })
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

/// The host, without case, and the client id that an enabled MQTT channel
/// connects with, configured or the default for `host_name`.
fn connection(name: &str, channel: &Channel, host_name: Option<&str>) -> Option<(String, String)> {
    match channel {
        Channel::Mqtt(c) if c.enabled => {
            Some((c.host.to_ascii_lowercase(), c.client_id_on(host_name, name)))
        }
        _ => None,
    }
}

/// Two channels that connect to one host with the same client id. A broker
/// disconnects a client when another connects with its id, on any of its
/// ports, so the two take turns.
struct Conflict {
    channels: (String, String),
    message: String,
}

/// Every channel that connects like one before it, paired with that one.
fn conflicts(channels: &BTreeMap<String, Channel>, host_name: Option<&str>) -> Vec<Conflict> {
    let mut ids = BTreeMap::new();
    let mut found = Vec::new();
    for (name, channel) in channels {
        let (Channel::Mqtt(c), Some(key)) = (channel, connection(name, channel, host_name)) else {
            continue;
        };
        let default = c.client_id.is_none();
        match ids.entry(key) {
            Entry::Vacant(entry) => {
                entry.insert((name, default));
            }
            Entry::Occupied(entry) => {
                let &(other, other_default) = entry.get();
                let which = if default || other_default {
                    ", the host name used without a client_id"
                } else {
                    ""
                };
                found.push(Conflict {
                    channels: (other.clone(), name.clone()),
                    message: format!(
                        "channels {other} and {name} both connect to {} as client_id {:?}{which}; \
                         a broker disconnects one when the other connects",
                        c.host,
                        entry.key().1
                    ),
                });
            }
        }
    }
    found
}

/// Refuses two channels that would share a client id, unless they share it
/// in `current` already.
fn check_client_ids(
    channels: &BTreeMap<String, Channel>,
    current: &BTreeMap<String, Channel>,
    host_name: Option<&str>,
) -> Result<(), Error> {
    let before = |name: &str| connection(name, current.get(name)?, host_name);
    for conflict in conflicts(channels, host_name) {
        let (a, b) = &conflict.channels;
        if before(a).is_none_or(|key| before(b) != Some(key)) {
            return Err(Error::Invalid(conflict.message));
        }
    }
    Ok(())
}

/// For every channel that shares its client id with another, the message
/// that names the two. Validation refuses a change that makes two channels
/// share one, but a configuration loaded at startup may, as can one saved
/// before the host was renamed.
pub fn client_id_conflicts(channels: &BTreeMap<String, Channel>) -> BTreeMap<String, String> {
    let mut by_channel = BTreeMap::new();
    for Conflict { channels, message } in conflicts(channels, host_name().as_deref()) {
        by_channel
            .entry(channels.0)
            .or_insert_with(|| message.clone());
        by_channel.entry(channels.1).or_insert(message);
    }
    by_channel
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
    ensure(c.client_id.as_ref().is_none_or(|id| !id.is_empty()), || {
        format!("channel {name}: client_id must not be empty; leave it out for the host name")
    })?;
    // MQTT strings carry a 16-bit length, which the library truncates
    // silently, and a broker closes the connection on U+0000 in one; the
    // password is binary. Either would end every session, and in the topic
    // every publish.
    for (field, value) in [
        ("topic", Some(&c.topic)),
        ("client_id", c.client_id.as_ref()),
        ("username", c.username.as_ref()),
    ] {
        ensure(
            value.is_none_or(|v| v.len() <= 65_535 && !v.contains('\0')),
            || format!("channel {name}: {field} must be at most 65535 bytes, without U+0000"),
        )?;
    }
    ensure(
        c.password.as_ref().is_none_or(|p| p.len() <= 65_535),
        || format!("channel {name}: password must be at most 65535 bytes"),
    )?;
    ensure(c.qos <= 2, || {
        format!("channel {name}: qos must be 0, 1 or 2")
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

/// Like [`load`], but when nothing at all is at the path, the defaults are
/// used and written there, so the effective configuration is visible on
/// disk. Any other failure to read the file is an error, never a reason to
/// overwrite it. That includes a link to a file that does not exist: its
/// target may only be missing for now, as on a partition not mounted yet.
pub fn load_or_init(path: &Path) -> Result<Config, Error> {
    match fs::read_to_string(path) {
        Ok(text) => parse(&text),
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            if let Ok(target) = fs::read_link(path) {
                return Err(Error::Io(io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("a link to {}, which does not exist", target.display()),
                )));
            }
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
    let config = read(text.as_bytes())?;
    config.validate()?;
    Ok(config)
}

/// Reads a configuration document, which must have the shape of one
/// (`check_shape`). It is not validated.
pub fn read(text: &[u8]) -> Result<Config, Error> {
    check_shape(&serde_json::from_slice(text)?)?;
    Ok(serde_json::from_slice(text)?)
}

/// Refuses a document that serde would read in another shape than its own:
/// it reads a struct from an array as well, element by element and with
/// defaults for the rest, which `deny_unknown_fields` does not cover. `[]`
/// would read as the defaults, `{"outbound": []}` as no channels, and a
/// channel from an array of its fields.
fn check_shape(document: &Value) -> Result<(), Error> {
    let object = |value: Option<&Value>, what: &str| {
        ensure(value.is_none_or(Value::is_object), || {
            format!("{what} must be a JSON object")
        })
    };
    object(Some(document), "the configuration")?;
    for section in ["inbound", "storage", "outbound", "api", "log"] {
        object(document.get(section), section)?;
    }
    let channels = document.get("outbound").and_then(|o| o.get("channels"));
    object(channels, "outbound.channels")?;
    for (name, channel) in channels.and_then(Value::as_object).into_iter().flatten() {
        object(Some(channel), &format!("channel {name}"))?;
    }
    Ok(())
}

/// Writes the configuration atomically: temp file (mode 0600), fsync, rename,
/// directory fsync. A link at the path is followed, so that the file it
/// points to is replaced, not the link.
pub fn save(path: &Path, config: &Config) -> io::Result<()> {
    save_with(path, config, |dir| File::open(dir)?.sync_all())
}

fn save_with(
    path: &Path,
    config: &Config,
    sync_dir: impl FnOnce(&Path) -> io::Result<()>,
) -> io::Result<()> {
    let path = match fs::symlink_metadata(path) {
        Ok(meta) if meta.file_type().is_symlink() => fs::canonicalize(path).map_err(|e| {
            io::Error::new(
                e.kind(),
                format!("following the link {}: {e}", path.display()),
            )
        })?,
        _ => path.to_path_buf(),
    };
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
    fs::rename(&tmp, &path)?;
    // The new file is in place, and the next start reads it. Failing now
    // would report the configuration as not applied while the file holds it.
    if let Err(e) = sync_dir(&dir) {
        warn!(dir = %dir.display(), error = %e, "could not sync the configuration directory");
    }
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
        // Any other patch replaces the whole document (RFC 7386), and is
        // likelier a JSON Patch (RFC 6902), where `[]` changes nothing.
        ensure(patch.is_object(), || {
            "a patch must be a JSON object, an RFC 7386 merge patch".into()
        })?;
        let _guard = self.write.lock().unwrap();
        let mut document = serde_json::to_value(self.current().as_ref())?;
        merge_patch(&mut document, patch);
        check_shape(&document)?;
        self.commit(serde_json::from_value(document)?)
    }

    /// Re-reads the file and publishes it without writing it back.
    pub fn reload(&self) -> Result<Arc<Config>, Error> {
        let _guard = self.write.lock().unwrap();
        let config = load(&self.path)?;
        config.check_change(&self.current())?;
        Ok(self.publish(config))
    }

    fn commit(&self, config: Config) -> Result<Arc<Config>, Error> {
        config.validate()?;
        config.check_change(&self.current())?;
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
        assert!(check(&|c| c.inbound.max_message_bytes = 0).is_err());
        // The configuration files written by earlier versions held 1 MiB.
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
        assert!(serde_json::from_str::<Config>(r#"{"api":{"mode":"999"}}"#).is_err());
        // Inbound connects to the producer's socket: it sets no mode, and
        // holds one connection.
        for removed in [r#"{"mode":"0660"}"#, r#"{"max_connections":64}"#] {
            let text = format!(r#"{{"inbound":{removed}}}"#);
            let err = serde_json::from_str::<Config>(&text).unwrap_err();
            assert!(err.to_string().contains("unknown field"), "{err}");
        }
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
    fn mqtt_channels_are_validated() {
        let check = |channels: &[(&str, &Mqtt)]| {
            let mut config = Config::default();
            for (name, c) in channels {
                let c = Channel::Mqtt((*c).clone());
                config.outbound.channels.insert(name.to_string(), c);
            }
            config.validate()
        };
        let base = |host: &str, port, client_id: Option<&str>| Mqtt {
            host: host.into(),
            port,
            topic: "t".into(),
            client_id: client_id.map(Into::into),
            ..Mqtt::default()
        };

        // Two channels may not connect to one host with the same client id,
        // whatever the port or the case of the host name, unless one of them
        // is disabled. Without a client_id a channel uses the host name, so
        // two such channels collide; on a host without a name each gets an
        // id of its own.
        let ids = |channels: &[(&str, &Mqtt)], host_name| {
            check_client_ids(&mqtt_channels(channels), &BTreeMap::new(), host_name)
        };
        let default = base("h", 1883, None);
        let x = base("h", 1883, Some("x"));
        let dev = Some("dev42");
        assert!(ids(&[("a", &x), ("b", &default)], dev).is_ok());
        assert!(ids(&[("a", &x), ("b", &base("g", 1883, Some("x")))], dev).is_ok());
        let err = ids(&[("a", &x), ("b", &base("H", 8883, Some("x")))], dev).unwrap_err();
        assert!(err.to_string().contains("channels a and b"), "{err}");
        let disabled = Mqtt {
            enabled: false,
            ..x.clone()
        };
        assert!(ids(&[("a", &x), ("b", &disabled)], dev).is_ok());
        let err = ids(&[("a", &default), ("b", &default)], dev).unwrap_err();
        assert!(
            err.to_string().contains("\"dev42\", the host name"),
            "{err}"
        );
        let named = base("h", 1883, Some("dev42"));
        assert!(ids(&[("a", &named), ("b", &default)], dev).is_err());
        assert!(ids(&[("a", &default), ("b", &default)], None).is_ok());
        assert!(check(&[("a", &base("h", 1883, Some("")))]).is_err());

        // MQTT strings have 16-bit lengths and no U+0000, but the password
        // is binary.
        let field = |f: &dyn Fn(&mut Mqtt)| {
            let mut c = default.clone();
            f(&mut c);
            check(&[("a", &c)])
        };
        let long = "x".repeat(65_536);
        assert!(field(&|c| c.topic = long[1..].into()).is_ok());
        assert!(field(&|c| c.topic = long.clone()).is_err());
        assert!(field(&|c| c.topic = "a\0b".into()).is_err());
        assert!(field(&|c| c.client_id = Some(long.clone())).is_err());
        assert!(field(&|c| c.client_id = Some("a\0".into())).is_err());
        assert!(field(&|c| c.username = Some("a\0".into())).is_err());
        assert!(field(&|c| c.password = Some("a\0".into())).is_ok());
        assert!(field(&|c| c.password = Some(long.clone())).is_err());
        assert!(field(&|c| c.timeout_ms = 1).is_ok());
        assert!(field(&|c| c.timeout_ms = 0).is_err());
    }

    fn mqtt_channels(channels: &[(&str, &Mqtt)]) -> BTreeMap<String, Channel> {
        channels
            .iter()
            .map(|(name, c)| (name.to_string(), Channel::Mqtt((*c).clone())))
            .collect()
    }

    #[test]
    fn a_client_id_shared_already_is_not_refused_again() {
        let base = |client_id: Option<&str>| Mqtt {
            host: "h".into(),
            topic: "t".into(),
            client_id: client_id.map(Into::into),
            ..Mqtt::default()
        };
        let (default, x) = (base(None), base(Some("x")));
        let check = |new: &[(&str, &Mqtt)], current: &[(&str, &Mqtt)]| {
            check_client_ids(&mqtt_channels(new), &mqtt_channels(current), Some("dev42"))
        };
        // Two channels that share an id now, as after the host was renamed,
        // may stay as they are while anything else changes, and the change
        // that resolves it is taken too.
        let shared: &[(&str, &Mqtt)] = &[("a", &default), ("b", &default)];
        assert!(check(shared, shared).is_ok());
        assert!(check(&[("a", &default), ("b", &x)], shared).is_ok());
        // A channel that would share it as well is refused, also beside a
        // pair that shares it already, and so is a pair that did not before.
        let three = [("a", &default), ("b", &default), ("c", &default)];
        let err = check(&three, shared).unwrap_err();
        assert!(err.to_string().contains("channels a and c"), "{err}");
        assert!(check(shared, &[("a", &default), ("b", &x)]).is_err());
        assert!(check(shared, &[("a", &default)]).is_err());
        // Taking one out of three that share it leaves two that did.
        assert!(check(&three[1..], &three).is_ok());

        // Validation alone no longer depends on the host name, so a file
        // saved before the host was renamed still loads.
        let mut config = Config::default();
        config.outbound.channels = mqtt_channels(shared);
        config.validate().unwrap();
        let conflicts = conflicts(&config.outbound.channels, Some("dev42"));
        assert_eq!(conflicts.len(), 1);
        assert_eq!(conflicts[0].channels, ("a".into(), "b".into()));
    }

    #[test]
    fn documents_must_have_the_shape_of_a_configuration() {
        for text in [
            "[]",
            r#"{"outbound": []}"#,
            r#"{"inbound": ["/run/x.sock"]}"#,
            r#"{"outbound": {"channels": []}}"#,
            r#"{"outbound": {"channels": {"a": ["websocket", true, "ws://h/x"]}}}"#,
        ] {
            assert!(matches!(parse(text), Err(Error::Invalid(_))), "{text}");
            assert!(read(text.as_bytes()).is_err(), "{text}");
        }
        let err = parse("[]").unwrap_err();
        assert_eq!(err.to_string(), "the configuration must be a JSON object");
        // A syntax error still says where it is.
        let err = parse("{\n\"inbound\": {\"max_message_bytes\": x}}").unwrap_err();
        assert!(err.to_string().contains("line 2"), "{err}");

        // A patch must be an object, and so must every section of what it
        // makes; neither removes the channels.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.json");
        let mut config = Config::default();
        config
            .outbound
            .channels
            .insert("a".into(), websocket("ws://h/x"));
        save(&path, &config).unwrap();
        let store = Store::new(path.clone(), config.clone());
        for patch in [
            serde_json::json!([]),
            serde_json::json!("x"),
            serde_json::json!({"outbound": []}),
            serde_json::json!({"outbound": {"channels": {"a": []}}}),
        ] {
            assert!(
                matches!(store.patch(&patch), Err(Error::Invalid(_))),
                "{patch}"
            );
        }
        // An empty JSON Patch is told what PATCH takes.
        let err = store.patch(&serde_json::json!([])).unwrap_err();
        assert!(err.to_string().contains("RFC 7386"), "{err}");
        assert_eq!(*store.current(), config);
        assert_eq!(load(&path).unwrap(), config);
    }

    #[test]
    fn socket_paths_must_fit_a_socket_address() {
        let check = |f: &dyn Fn(&mut Config)| {
            let mut config = Config::default();
            f(&mut config);
            config.validate()
        };
        let path = |len: usize| PathBuf::from(format!("/{}", "s".repeat(len - 1)));
        let most = SOCKET_PATH_ROOM - 1;
        assert!(check(&|c| c.inbound.path = path(most)).is_ok());
        let err = check(&|c| c.inbound.path = path(most + 1)).unwrap_err();
        assert!(
            err.to_string().starts_with("inbound.path must be at most"),
            "{err}"
        );
        assert!(check(&|c| c.api.path = path(most + 1)).is_err());
        assert!(check(&|c| c.api.path = "/run/a\0b".into()).is_err());
        assert!(check(&|c| c.inbound.path = "/run/a\0b".into()).is_err());
        assert!(check(&|c| c.storage.path = "/var/a\0b".into()).is_err());
        // Linux and macOS hold 107 and 103 bytes.
        assert!([107, 103].contains(&most), "{most}");
    }

    #[test]
    fn a_link_to_a_missing_file_is_neither_read_as_defaults_nor_replaced() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("data/config.json");
        let link = dir.path().join("config.json");
        std::os::unix::fs::symlink(&target, &link).unwrap();
        let err = load_or_init(&link).unwrap_err();
        assert!(err.to_string().contains("which does not exist"), "{err}");
        assert!(matches!(&err, Error::Io(e) if e.kind() == io::ErrorKind::NotFound));
        assert_eq!(fs::read_link(&link).unwrap(), target);
        assert!(save(&link, &Config::default()).is_err());
        assert_eq!(fs::read_link(&link).unwrap(), target);
        assert!(!target.exists());

        // Once the target is there, the file it names is read and saved,
        // and the link stays.
        fs::create_dir(target.parent().unwrap()).unwrap();
        save(&target, &Config::default()).unwrap();
        let store = Store::new(link.clone(), load_or_init(&link).unwrap());
        store
            .patch(&serde_json::json!({"storage": {"sync_interval_ms": 7}}))
            .unwrap();
        assert_eq!(fs::read_link(&link).unwrap(), target);
        assert_eq!(load(&target).unwrap().storage.sync_interval_ms, 7);
    }

    #[test]
    fn a_save_whose_directory_sync_fails_after_the_rename_still_succeeds() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.json");
        let mut config = Config::default();
        config.storage.sync_interval_ms = 9;
        save_with(&path, &config, |_| Err(io::Error::other("injected"))).unwrap();
        assert_eq!(load(&path).unwrap(), config);
    }

    #[test]
    fn client_ids_default_to_the_host_name() {
        let default = Mqtt::default();
        assert_eq!(default.client_id_on(Some("dev42"), "a"), "dev42");
        // A configured id is used as given, the old default too.
        let saved = Mqtt {
            client_id: Some("uevtd".into()),
            ..Mqtt::default()
        };
        assert_eq!(saved.client_id_on(Some("dev42"), "a"), "uevtd");
        // On a host without a name: 21 letters and digits, distinct for
        // every channel, and the same while the process runs.
        let a = default.client_id_on(None, "a");
        assert_eq!(a.len(), 21, "{a}");
        assert!(a.starts_with("uevtd"), "{a}");
        assert!(a[5..].bytes().all(|b| b.is_ascii_hexdigit()), "{a}");
        assert_eq!(default.client_id_on(None, "a"), a);
        assert_ne!(default.client_id_on(None, "b"), a);
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
                        .patch(&serde_json::json!({"inbound": {"max_message_bytes": 100 + i}}))
                        .unwrap();
                }
            });
        });
        let current = store.current();
        assert_eq!(current.storage.sync_interval_ms, rounds);
        assert_eq!(current.inbound.max_message_bytes as u64, 100 + rounds);
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

    #[test]
    fn a_socket_cannot_move_onto_the_other_in_one_change() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::new(dir.path().join("config.json"), Config::default());
        let defaults = Config::default();
        let (inbound, api) = (defaults.inbound.path.as_path(), defaults.api.path.as_path());
        let with = |inbound: &Path, api: &Path| {
            let mut config = Config::default();
            config.inbound.path = inbound.to_path_buf();
            config.api.path = api.to_path_buf();
            config
        };
        // A swap, or a move onto the other's path, which is in use until
        // that one has moved.
        let other = Path::new("/run/uevtd/other.sock");
        for (a, b, refused) in [
            (api, inbound, "inbound.path"),
            (api, other, "inbound.path"),
            (other, inbound, "api.path"),
        ] {
            let err = store.update(with(a, b)).unwrap_err();
            assert!(err.to_string().starts_with(refused), "{err}");
        }
        // In two changes it is fine.
        store.update(with(inbound, other)).unwrap();
        store.update(with(api, other)).unwrap();
    }

    impl Store {
        fn redacted_for_test(&self) -> Config {
            self.current().redacted()
        }
    }
}
