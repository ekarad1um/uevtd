# uevtd

A message forwarding daemon. Producers write newline-delimited messages
(typically JSON) to a Unix domain socket; `uevtd` appends them to a bounded,
crash-safe log on disk and forwards them, in order and at least once, to the
configured HTTP endpoints and MQTT topics. A second Unix socket serves an HTTP
API for configuration, time-range queries over the log, status and metrics.

See [ARCHITECTURE.md](ARCHITECTURE.md) for the design and its trade-offs.

## Build and run

```sh
cargo build --release
./target/release/uevtd --config /etc/uevtd/config.json
```

If the configuration file does not exist it is created with defaults. The
daemon stops cleanly on `SIGTERM`/`SIGINT` (a second signal exits at once) and
re-reads its configuration file on `SIGHUP`. A systemd unit is in [contrib/uevtd.service](contrib/uevtd.service)
and a full example configuration in [contrib/config.example.json](contrib/config.example.json).

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
stored and forwarded verbatim; they are not parsed.

## Configuration

JSON, every field optional, unknown fields rejected. The defaults:

```json
{
  "inbound": { "path": "/run/uevtd/in.sock", "mode": "0660", "max_message_bytes": 1048576, "max_connections": 64 },
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
        "type": "http",
        "enabled": true,
        "url": "https://collector.example/ingest",
        "method": "POST",
        "headers": { "Authorization": "Bearer ..." },
        "timeout_ms": 10000
      },
      "broker": {
        "type": "mqtt",
        "enabled": true,
        "host": "broker.example",
        "port": 8883,
        "tls": true,
        "topic": "devices/42/events",
        "client_id": "uevtd-42",
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

- `storage.max_bytes` bounds the log; the oldest whole segment is evicted when
  it is exceeded, so retained data stays within one `segment_bytes` of the limit.
  `segment_bytes` is between 64 KiB and 256 MiB, `max_bytes` at most 1 TiB and at
  least `2 * segment_bytes`, and `sync_interval_ms` at most 60000.
- `storage.sync_interval_ms = 0` fsyncs after every write batch (group commit
  during bursts); a positive value syncs at most once per interval. Records are
  forwarded and queryable only once synced, so a positive interval adds up to
  that much latency and a crash can lose at most the unsynced tail, never a
  record that was already seen.
- HTTP: every message is one request with the payload as the body and
  `Content-Type: application/json` unless a header overrides it. 2xx is
  delivered; 408, 429, 5xx, timeouts and connection errors are retried with
  backoff (100 ms to 30 s) while the channel waits; any other status is logged,
  counted as rejected and skipped.
- MQTT: MQTT 3.1.1, `clean_session`, `inflight` publishes pipelined; the cursor
  advances on `PUBACK`/`PUBCOMP` (QoS 1/2) or when written (QoS 0). On a lost
  connection the daemon reconnects with backoff (1 s to 60 s) and republishes
  unacknowledged messages in order, so duplicates are possible. Payloads above
  `max_packet_bytes` are rejected and skipped; keep it below the broker's packet
  limit, since MQTT 3.1.1 cannot tell an oversized packet from a lost connection.
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
| GET    | `/v1/status`   | JSON status: uptime, `restart_required`, inbound and storage counters, per-channel cursor, lag and last error. |
| GET    | `/v1/metrics`  | The same counters in Prometheus text format. |
| GET    | `/v1/health`   | `200 ok`. |

Errors are `{"error": "<message>"}` with status 400, 404 or 500.

### `GET /v1/messages`

| Parameter | Meaning |
|-----------|---------|
| `from`    | Inclusive lower bound on the receipt timestamp, microseconds since the Unix epoch. |
| `to`      | Exclusive upper bound, same unit. |
| `after`   | Return only sequence numbers greater than this (pagination). |
| `limit`   | 1 to 10000, default 100. |

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
is returned as a string in `text` instead. `next` is the last sequence number
when the page is full and `null` otherwise; pass it as `after` for the next page.

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

The end-to-end tests start the daemon in-process with temporary sockets and a
local HTTP target. MQTT delivery can be exercised against any broker, e.g.
`mosquitto`, by configuring a channel and watching `/v1/status`.
