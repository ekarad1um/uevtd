# uevtd

A message forwarding daemon. Producers write newline-delimited messages
(typically JSON) to a Unix domain socket; `uevtd` appends them to a bounded,
crash-safe log on disk and forwards them, in order and at least once, to the
configured WebSocket receivers and MQTT topics. A second Unix socket serves an
HTTP API for configuration, time-range queries over the log, status and metrics.

See [ARCHITECTURE.md](ARCHITECTURE.md) for the design and its trade-offs.

## Build and run

```sh
cargo build --release
./target/release/uevtd --config /etc/uevtd/config.json
```

If the configuration file does not exist it is created with defaults. The
daemon stops cleanly on `SIGTERM`/`SIGINT` (a second signal exits at once) and
re-reads its configuration file on `SIGHUP`. A full example configuration is in
[contrib/config.example.json](contrib/config.example.json).

Two things are left to the system that runs it:

- A process supervisor that starts it again when it exits with a non-zero
  status. The daemon exits with 1 when it cannot start, for example because a
  socket path is in use or the log cannot be opened, and when its log writer
  thread panics; a channel that panics is restarted inside the daemon. A clean
  stop exits with 0, a second signal with 130 and a usage error with 2.
- Write access for the daemon's user to the directory of the configuration
  file, where the API saves it through a temporary file and a missing file is
  created, as well as to `storage.path` and the directories of both sockets.

Logs go to stderr; the filter (`log.filter`) uses `tracing` directives such as
`info` or `info,uevtd::storage=debug` and can be changed at runtime.

## Sending messages

One message per line, terminated by `\n`:

```sh
printf '{"event":"boot","uptime":3}\n' | socat - UNIX-CONNECT:/run/uevtd/in.sock
```

Rules: a trailing `\r` is stripped, empty lines are ignored, a line longer than
`inbound.max_message_bytes` is discarded and logged, and a final line without
`\n` when the peer closes is discarded and logged as truncated. Payloads are
stored and forwarded verbatim; they are not parsed. JSON must therefore be sent
compact, one message to a line: pretty-printed JSON would be stored and
forwarded as one message per line.

The guarantees start once a message is committed to the log, and producers get
no acknowledgement. Until then a message can be lost: by a crash while it waits
for the writer or for its sync, by a stop before the daemon has read it from the
socket, or by a failed write, which is counted in `dropped`.

## Configuration

JSON, every field optional, unknown fields rejected. The defaults:

```json
{
  "inbound": { "path": "/run/uevtd/in.sock", "mode": "0660", "max_message_bytes": 65536, "max_connections": 64 },
  "storage": { "path": "/var/lib/uevtd", "max_bytes": 268435456, "segment_bytes": 8388608, "sync_interval_ms": 0 },
  "outbound": { "channels": {} },
  "api": { "path": "/run/uevtd/api.sock", "mode": "0660" },
  "log": { "filter": "info", "format": "text" }
}
```

Channels are a map from name to definition. The name identifies the channel's
delivery cursor, so renaming a channel starts it afresh at the current head.

```json
{
  "outbound": {
    "channels": {
      "ingest": {
        "type": "websocket",
        "enabled": true,
        "url": "wss://collector.example/ingest",
        "headers": { "Authorization": "Bearer ..." },
        "ca_file": null,
        "timeout_ms": 10000,
        "keep_alive_s": 30,
        "inflight": 256,
        "max_frame_bytes": 1048576
      },
      "broker": {
        "type": "mqtt",
        "enabled": true,
        "host": "broker.example",
        "port": 8883,
        "tls": true,
        "ca_file": "/etc/uevtd/broker-ca.pem",
        "topic": "devices/42/events",
        "client_id": null,
        "username": "device",
        "password": "secret",
        "qos": 1,
        "retain": false,
        "keep_alive_s": 30,
        "inflight": 32,
        "max_packet_bytes": 1048576
      }
    }
  }
}
```

Notes:

- Memory is bounded by sizes and counts, not by traffic: a backlog waits on disk.
  `inbound.max_message_bytes` is at most 262144 and `max_connections` at most
  256, and their product at most 16 MiB, because each connection may hold one
  message. There are at most 8 channels, and a channel's `inflight` is at most 1024.
  Within these limits the daemon stays below about 60 MiB resident; see
  ARCHITECTURE.md §11.1 for the budget and measurements.
- `storage.max_bytes` bounds the log; the oldest whole segment is evicted when
  it is exceeded, so retained data stays within one `segment_bytes` of the limit.
  `segment_bytes` is between 64 KiB and 256 MiB and at least
  `inbound.max_message_bytes + 48`, `max_bytes` at most 1 TiB and at least
  `2 * segment_bytes`, and `sync_interval_ms` at most 60000.
- `storage.sync_interval_ms = 0` fsyncs after every write batch (group commit
  during bursts); a positive value syncs at most once per interval. Records are
  forwarded and queryable only once synced, so a positive interval adds up to
  that much latency and a crash can lose at most the unsynced tail, never a
  record that was already seen.
- At startup the newest segment is checked and synced before its records are
  forwarded. A read error in it, such as a bad sector, costs the rest of that
  segment, counted as lost with a notice in its place, but not the startup; an
  older segment that cannot be read is retried by the channels, with the error
  in `last_error`.
- WebSocket: one long-lived connection (`ws://` or `wss://`); `headers` go with
  the handshake. Every message is a text frame in the shape `/v1/messages`
  returns, `{"seq": 7, "ts": 1760928000123456, "data": {...}}`, or `"text"`
  instead of `"data"` for a payload that is not JSON. The receiver answers
  `{"ack": n}` once it has processed every frame up to `seq` `n`, or
  `{"nack": n, "reason": "..."}` for a frame it will never accept, which is
  counted as rejected and skipped. At most `inflight` frames and 1 MiB of payload
  wait for an answer, so a receiver slows the stream by answering later. The
  daemon pings every `keep_alive_s` and gives up on a session without a pong
  within `timeout_ms`, the same limit as for connecting. Unanswered frames are
  sent again after a reconnect, so receivers deduplicate by `seq`. A message
  whose frame would exceed `max_frame_bytes` (at least 256) is replaced by a
  notice.
- MQTT: MQTT 3.1.1, `clean_session`, `inflight` publishes pipelined; the cursor
  advances on `PUBACK`/`PUBCOMP` (QoS 1/2) or when written (QoS 0). On a lost
  connection the daemon reconnects and republishes unacknowledged messages in
  order, so duplicates are possible. Payloads above `max_packet_bytes` (at least
  256) are rejected: a notice is published in their place. Keep it below the
  broker's packet limit, since MQTT 3.1.1 cannot tell an oversized packet from a
  lost connection.
- MQTT `client_id`: without one, a channel connects as `uevtd` and 16 hex
  digits, logged when the channel starts. The id stays the same until the
  daemon restarts and differs for every channel and device, so they never take
  over each other's session on a broker. Set it when the broker expects a fixed
  id; two enabled channels to the same host cannot share one.
- Reconnects of both channels back off from 1 s to 60 s, and the wait shortens
  again only once the target acknowledges something: a target that accepts
  connections but takes nothing is retried less and less often. `retries`
  counts the reconnects.
- A channel whose task panics shows `running: false` and the panic in
  `last_error`, and starts again from its cursor after 1 s, then 2 s, up to
  60 s while it makes no progress; `restarts` counts it. Other channels carry
  on.
- TLS (`wss://`, or MQTT `tls`) trusts the platform's certificate store, or with
  `ca_file` only the certificates in that PEM file. The file is read for every
  connection; a missing file shows in `last_error`.
- Drop notices: a channel sends a notice in place of the messages it will not
  deliver, so the receiver learns of the gap. MQTT publishes it on the topic:

  ```json
  {"notice": {"event": "lost", "from_seq": 1, "to_seq": 3075, "count": 3075}}
  {"notice": {"event": "rejected", "seq": 4242, "ts": 1760928000123456, "reason": "payload too large"}}
  ```

  WebSocket sends it as a frame with the `seq` to acknowledge, the last one it
  stands for: `{"seq": 3075, "notice": {...}}`. `lost` stands for messages
  evicted from the log before the channel delivered them. `rejected` names one
  too large to send; it stays in the log, and `/v1/messages` returns it by `seq`
  or `ts`. Notices are delivered at least once and in order: the cursor passes
  the messages a notice stands for once it is acknowledged. MQTT never retains
  them, and producers should not send messages with a top-level `notice` key.
- Messages evicted before a channel delivered them are counted in its `lost`
  and logged when they are evicted, also while the target is down. `lag`
  counts only messages still in the log.
- A new channel starts at the current head; disabling a channel keeps its
  cursor; removing it deletes the cursor.
- All fields apply live except `storage.path` and `log.format`, which are
  reported in `/v1/status` under `restart_required`.

## HTTP API

Served over `api.path`; use `curl --unix-socket`.

| Method | Path           | Description |
|--------|----------------|-------------|
| GET    | `/v1/config`   | Current configuration (secrets included). |
| PUT    | `/v1/config`   | Replace the configuration. Validated, persisted, applied. Returns the applied document. |
| PATCH  | `/v1/config`   | [RFC 7386](https://www.rfc-editor.org/rfc/rfc7386) merge patch of the configuration. |
| GET    | `/v1/messages` | Time-ordered range query, see below. |
| GET    | `/v1/status`   | JSON status: uptime, `restart_required`, inbound and storage counters; per channel the cursor, `lag`, the counts `delivered`, `rejected`, `retries`, `lost` and `notices`, and the last error. See below. |
| GET    | `/v1/metrics`  | The same values in Prometheus text format. |
| GET    | `/v1/health`   | `200 ok` while the log takes writes; `503` with the reason after a failed write or sync until a batch is committed again, and while one write or sync takes more than 30 s. |

Errors are `{"error": "<message>"}` with status 400, 404, 405, 413, 500 or 503.
Request bodies are limited to 256 KiB.

### Status and metrics

`/v1/status` and `/v1/metrics` report the same values, times in microseconds
since the Unix epoch in the status and in seconds in the metrics. Besides the
counters they show:

- `storage.first_ts` and `head_ts`: when the oldest and the newest message in
  the log were received, the range a query can reach.
- `storage.queued` and `queued_bytes`: messages waiting for the writer.
  Producers wait once 8 192 messages or 8 MiB do.
- `storage.syncs` and `sync_us`: syncs and the time spent in them. `sync_us /
  syncs` is the mean sync time, and `records / syncs` the number of messages a
  sync commits.
- `storage.dropped` and `storage.last_error`: messages lost to a failed write or
  sync, and the last failure, kept until a batch is committed again.
- `storage.clock_steps`: messages received while the clock was behind the
  newest message in the log, whose time was raised to it. Messages from two
  connections arriving slightly out of order are raised too, but not counted.
- Per channel, `lag` and `waiting_ts`: messages in the log waiting for
  delivery, and when the oldest of them was received; `delivery_us /
  delivered` is the mean time from receipt to acknowledgement. A channel that
  has been disabled since it was added has no `cursor` yet, and `lag` 0.

### `GET /v1/messages`

| Parameter | Meaning |
|-----------|---------|
| `from`    | Inclusive lower bound on the receipt timestamp, microseconds since the Unix epoch. |
| `to`      | Exclusive upper bound, same unit. |
| `after`   | Return only sequence numbers greater than this (pagination). |
| `limit`   | 1 to 10000, default 100. A page also stops at 1 MiB of payload. |

Records are returned in sequence order:

```json
{
  "messages": [
    { "seq": 1041, "ts": 1760928000123456, "data": { "event": "boot", "uptime": 3 } },
    { "seq": 1042, "ts": 1760928000223456, "text": "not json" }
  ],
  "next": 1042
}
```

`data` is the payload embedded verbatim when it is valid JSON; any other payload
is returned as a string in `text` instead. `next` is the page's last sequence
number when more records follow and `null` on the last page; pass it as `after`
for the next page.

Examples:

```sh
curl --unix-socket /run/uevtd/api.sock http://uevtd/v1/status
curl --unix-socket /run/uevtd/api.sock -X PATCH http://uevtd/v1/config \
     -d '{"outbound":{"channels":{"ingest":{"enabled":false}}}}'
curl --unix-socket /run/uevtd/api.sock 'http://uevtd/v1/messages?from=1760928000000000&limit=10'
```

## Storage layout

```
/var/lib/uevtd/
  00000000000000000001.seg    append-only segments named by first sequence number
  00000000000000016384.seg
  cursors/ingest              next sequence number each channel will deliver
```

Every record carries a CRC-32; a torn write at the end of the active segment
is truncated on start, and corruption elsewhere is skipped and counted. Files
are created with mode 0640 and directories with 0750. Files
can be inspected offline: a segment is a 24-byte header followed by
`crc32 | len | seq | ts | payload` frames (little-endian; see
`src/storage/segment.rs`).

## Development

```sh
cargo test            # unit tests and end-to-end tests (no external services)
cargo clippy --all-targets
```

The end-to-end tests start the daemon in-process with temporary sockets, a
scripted WebSocket receiver and a minimal MQTT broker, both also over TLS with
the test certificates in `tests/data`. A real broker such as `mosquitto` works
too: configure a channel and watch `/v1/status`.
