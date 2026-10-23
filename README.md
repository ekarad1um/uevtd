# uevtd

A message forwarding daemon. `uevtd` connects to the Unix domain socket that a
producer listens on and reads newline-delimited messages (typically JSON) from
it; it appends them to a bounded, crash-safe log on disk and forwards them, in
order and at least once, to the configured WebSocket receivers and MQTT topics.
A second Unix socket, the daemon's own, serves an HTTP API for configuration,
time-range queries over the log, status and metrics.

See [ARCHITECTURE.md](ARCHITECTURE.md) for the design and its trade-offs.

## Build and run

```sh
cargo build --release
./target/release/uevtd --config /etc/uevtd/config.json
```

If nothing is at the configuration path, a file of defaults is created there.
A link at the path is followed, and saves replace the file it names; a link to
a file that does not exist stops the daemon instead, so that a configuration on
a partition that mounts late is not replaced by defaults. The daemon stops
cleanly on `SIGTERM`/`SIGINT` (a second signal exits at once) and re-reads its
configuration file on `SIGHUP`. A full example configuration is in
[contrib/config.example.json](contrib/config.example.json).

Two things are left to the system that runs it:

- A process supervisor that starts it again when it exits with a non-zero
  status. The daemon exits with 1 when it cannot start, for example because
  the API socket's path is in use, the log cannot be opened or another daemon
  has it open (the error names that process), and when its log writer
  thread panics; a channel that panics is restarted inside the daemon. A clean
  stop exits with 0, a second signal with 130 and a usage error with 2.
- Write access for the daemon's user to the directory of the configuration
  file, or of the file it links to, where the API saves it through a temporary
  file and a missing file is created, as well as to `storage.path` and the
  directory of the API socket; and write permission on the producer's socket,
  which connecting to it needs.

At startup the daemon raises its soft limit on open files to the hard limit.

Logs go to stderr; the filter (`log.filter`) uses `tracing` directives such as
`info` or `info,uevtd::storage=debug` and can be changed at runtime.

## Reading messages

The producer creates a stream socket at `inbound.path`, listens on it, and
writes one message per line, terminated by `\n`. The daemon connects to it and
reads; it never writes to the socket, and creates, changes or removes nothing
at the path. It holds one connection at a time and connects again when that one
ends: at once if anything arrived on it, otherwise after 100 ms, doubling to
1 s, which is also how it waits for a producer that is not there yet.
`/v1/status` shows `inbound.connected`, and in `inbound.last_error` why it is
not. When the daemon stops or moves to another path, the producer's writes to
the connection fail with `EPIPE`, or raise `SIGPIPE` where the producer does
not ignore it, and the daemon's next connection is a new one. To try it without
a producer, let `socat` be one:

```sh
printf '{"event":"boot","uptime":3}\n' | socat -u - UNIX-LISTEN:/run/uevtd/in.sock
```

Rules: a trailing `\r` is stripped, empty lines are ignored, a line longer than
`inbound.max_message_bytes` is discarded and logged, and a final line without
`\n` when the connection ends is discarded and logged as truncated. Payloads are
stored and forwarded verbatim; they are not parsed. JSON must therefore be sent
compact, one message to a line: pretty-printed JSON would be stored and
forwarded as one message per line.

The guarantees start once a message is committed to the log, and the producer
gets no acknowledgement. Until then a message can be lost: by a crash while it
waits for the writer or for its sync, by a stop or a change of `inbound.path`
before the daemon has read it from the socket, or by a failed write, which is
counted in `dropped`. While the daemon is not connected, what becomes of the
producer's messages is up to the producer.

## Configuration

JSON, every field optional, unknown fields rejected. The defaults:

```json
{
  "inbound": { "path": "/run/uevtd/in.sock", "max_message_bytes": 65536 },
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
        "timeout_ms": 10000,
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
  `inbound.max_message_bytes` is at most 262144. There are at most 8 channels,
  and a channel's `inflight` is at most 1024. Within these limits the daemon
  stays below about 50 MiB resident; see ARCHITECTURE.md §11.1 for the budget
  and measurements.
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
  forwarded. A read error in it, such as a bad sector, does not stop the
  startup: the segment is kept, the channels retry the part that fails, as
  below, and new messages are numbered past anything that part could hold.
- A channel that cannot read a part of the log tries again every second, with
  the error in `last_error`. When the device cannot read the data back (EIO),
  as from a worn sector on an SD card or eMMC, the channel gives up after ten
  minutes and skips the rest of that segment: those messages count as lost,
  and a notice takes their place. Other errors, such as a missing permission,
  are retried until they are fixed, and then cost nothing.
- WebSocket: one long-lived connection (`ws://` or `wss://`); `headers` go with
  the handshake. Every message is a text frame in the shape `/v1/messages`
  returns, `{"seq": 7, "ts": 1760928000123456, "data": {...}}`, or `"text"`
  instead of `"data"` for a payload that is not JSON. The receiver answers
  `{"ack": n}` once it has processed every frame up to `seq` `n`, or
  `{"nack": n, "reason": "..."}` for a frame it will never accept, which is
  counted as rejected and skipped. At most `inflight` frames and 1 MiB of payload
  wait for an answer, so a receiver slows the stream by answering later. The
  daemon pings every `keep_alive_s`, and anything the receiver sends answers
  the ping. A session ends when no answer comes within `timeout_ms` of a ping,
  the same limit as for connecting, and the receiver's TCP has acknowledged
  nothing sent meanwhile either (on Linux and macOS), so a slow link keeps its
  session as long as it moves. Unanswered frames are sent again after a
  reconnect, so receivers deduplicate by `seq`. A message whose frame would
  exceed `max_frame_bytes` (at least 256) is replaced by a notice.
- MQTT: MQTT 3.1.1, `clean_session`, `inflight` publishes pipelined; the cursor
  advances on `PUBACK`/`PUBCOMP` (QoS 1/2) or when written (QoS 0). On a lost
  connection the daemon reconnects and republishes unacknowledged messages in
  order, so duplicates are possible. Payloads above `max_packet_bytes` (at least
  256) are rejected: a notice is published in their place. Keep it below the
  broker's packet limit, since MQTT 3.1.1 cannot tell an oversized packet from a
  lost connection. `timeout_ms` bounds connecting, DNS, TCP, TLS and the
  broker's answer together, and writing out each publish, in whole seconds,
  rounded up. Over a slow link, raise it to cover the largest publish, and
  `keep_alive_s` to cover what waits to be sent ahead of a ping. A topic so
  long that `inflight` copies of it would exceed 1 MiB lowers `inflight`
  accordingly, since the client keeps one in each publish.
- MQTT `client_id`: without one, a channel connects with the host name, so
  devices that share a broker need host names of their own. The id is logged
  when the channel starts. A configured `client_id` is used exactly as given,
  including a `"uevtd"` saved by an earlier version. A change cannot make two
  enabled channels to the same host connect with the same id, so a second
  channel to a broker needs a `client_id`. A configuration file in which two
  channels share an id still loads at startup, as one saved before the host was
  renamed can: both channels run, and both show the conflict in `last_error`.
  A host without a name, an empty one or `(none)`, connects
  as `uevtd` and 16 hex digits instead, new at every start. MQTT 3.1.1 brokers
  need only accept ids of up to 23 letters and digits; one that refuses the host
  name says so in `last_error`, and then the channel needs a `client_id`.
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
  them, and the producer should not send messages with a top-level `notice`
  key.
- Messages evicted before a channel delivered them are counted in its `lost`
  and logged when they are evicted, also while the target is down. `lag`
  counts only messages still in the log.
- A new channel starts at the current head; disabling a channel keeps its
  cursor; removing it deletes the cursor. A cursor file that holds no number
  replays the log from its oldest message. One that cannot be read is left as
  it is, and the channel waits, with the error in `last_error`, until it can
  read it.
- All fields apply live except `storage.path` and `log.format`, which are
  reported in `/v1/status` under `restart_required`. A change of
  `inbound.max_message_bytes` applies to the connection at once, the message
  in progress included, and one of `inbound.path` closes the connection and
  connects to the new path. A change cannot move one socket onto the other's
  path, a swap included; that takes two changes.

## HTTP API

Served over `api.path`; use `curl --unix-socket`.

| Method | Path           | Description |
|--------|----------------|-------------|
| GET    | `/v1/config`   | Current configuration (secrets included). |
| PUT    | `/v1/config`   | Replace the configuration. Validated, persisted, applied. Returns the applied document. |
| PATCH  | `/v1/config`   | [RFC 7386](https://www.rfc-editor.org/rfc/rfc7386) merge patch of the configuration. |
| GET    | `/v1/messages` | Time-ordered range query, see below. |
| GET    | `/v1/status`   | JSON status: uptime, `restart_required`, whether inbound is connected, inbound and storage counters; per channel the cursor, `lag`, the counts `delivered`, `rejected`, `retries`, `lost` and `notices`, and the last error. See below. |
| GET    | `/v1/metrics`  | The same values in Prometheus text format. |
| GET    | `/v1/health`   | `200 ok` while the log takes writes; `503` with the reason after a failed write or sync until a batch is committed again, while one write or sync takes more than 30 s, and while eviction cannot remove a segment, so that the log grows past `storage.max_bytes`. |

Errors are `{"error": "<message>"}` with status 400, 404, 405, 413, 500 or 503.
Request bodies are limited to 256 KiB. A `PATCH` body must be a JSON object, a
merge patch, and in a `PUT` the document, its sections and its channels must
be objects.

At most 16 connections are served at once; others wait until one closes. A
connection is closed after 10 s without a request, and once a read or write has
waited 60 s: a client that stops sending its request or reading its response is
cut off. A query holds the one query slot until its response is written.

### Status and metrics

`/v1/status` and `/v1/metrics` report the same values, times in microseconds
since the Unix epoch in the status and in seconds in the metrics. Besides the
counters they show:

- `inbound.connected` and `connections`: whether inbound is connected to the
  producer, and the connections made to it; `inbound.last_error` says why the
  last one failed or ended, until the next is made.
- `storage.first_ts` and `head_ts`: when the oldest and the newest message in
  the log were received, the range a query can reach.
- `storage.queued` and `queued_bytes`: messages waiting for the writer.
  The producer's writes wait once 8 192 messages or 8 MiB do.
- `storage.syncs` and `sync_us`: syncs and the time spent in them. `sync_us /
  syncs` is the mean sync time, and `records / syncs` the number of messages a
  sync commits.
- `storage.dropped` and `storage.last_error`: messages lost to a failed write or
  sync, and the last failure, kept until a batch is committed again; otherwise
  why eviction cannot remove a segment, while it cannot.
- `storage.corrupt_segments`: segments found damaged since the daemon started,
  each counted once.
- `storage.clock_steps`: messages received while the clock was behind the
  newest message in the log, whose time was raised to it.
- Per channel, `lag` and `waiting_ts`: messages in the log waiting for
  delivery, and when the oldest of them was received; `delivery_us /
  delivered` is the mean time from receipt to acknowledgement. A channel that
  has been disabled since it was added has no `cursor` yet, and `lag` 0.
- Per channel, `rejected`: messages passed undelivered, refused by the
  receiver or too large, each counted once the cursor has passed it, however
  often a restart sent it again.

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
  lock                        locked by the daemon that has the log open
  00000000000000000001.seg    append-only segments named by first sequence number
  00000000000000016384.seg
  cursors/ingest              next sequence number each channel will deliver
```

Every record carries a CRC-32. A torn write at the end of the active segment
is truncated on start; after other damage there the segment is kept and
numbering continues past what it could hold, so that no sequence number a
receiver may have seen is given out again. Corruption elsewhere is skipped and
counted. Files
are created with mode 0640 and directories with 0750. Files
can be inspected offline: a segment is a 24-byte header followed by
`crc32 | len | seq | ts | payload` frames (little-endian; see
`src/storage/segment.rs`).

## Development

```sh
cargo test            # unit tests and end-to-end tests (no external services)
cargo clippy --all-targets
```

The end-to-end tests start the daemon in-process with a test producer on a
temporary socket, a scripted WebSocket receiver and a minimal MQTT broker, both
also over TLS with the test certificates in `tests/data`. The log's randomized
tests run a few seeds by default; `UEVTD_SEEDS=10000` runs more, quickly with
`TMPDIR` on a tmpfs, since every commit syncs. A real broker such as `mosquitto`
works too: configure a channel and watch `/v1/status`.
