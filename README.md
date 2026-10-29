# uevtd

Reads newline-delimited messages from a producer's Unix socket into a bounded, crash-safe log on disk, and forwards them in order, at least once, to WebSocket receivers and MQTT topics. An HTTP API on its own Unix socket serves configuration, time-range queries, status and metrics.

## Run

```sh
cargo build --release
./target/release/uevtd -c /etc/uevtd/config.json   # the default path; -V version, -h help
```

- Nothing at the config path: defaults are written there. A symlink is followed; a dangling one is an error, never replaced, so a config on a late mount survives.
- `SIGHUP` reloads the file. `SIGTERM`/`SIGINT` stop cleanly; a second signal exits at once.
- Exit `0` clean stop, `1` startup failure or log writer panic, `2` usage, `130` second signal. Run it under a supervisor that restarts it on non-zero; a channel that panics restarts in-process.
- Needs write access to `storage.path`, the API socket's directory, the config file's directory (its target's, for a link; saves go through a temporary file), and the producer's socket, which connecting requires.
- Logs go to stderr. `log.filter` takes `tracing` directives such as `info,uevtd::storage=debug`.

## Producer

The producer owns the socket: it listens on `inbound.path` (`SOCK_STREAM`). uevtd connects and only reads; it never writes there, and creates, chmods or removes nothing at the path.

```sh
printf '{"event":"boot","uptime":3}\n' | socat -u - UNIX-LISTEN:/run/uevtd/in.sock   # one-shot producer
```

- One message per `\n`-terminated line, stored verbatim. Send compact JSON: a pretty-printed document becomes one message per line.
- A trailing `\r` is stripped and empty lines are skipped. A line over `inbound.max_message_bytes` is dropped (`oversized`), and so is an unterminated line when the connection ends (`truncated`).
- One connection at a time. After one that carried data ends, uevtd reconnects at once; a failed connect, or a connection closed before any data, is retried after 100 ms, doubling to 1 s.
- A full writer queue (8192 messages or 8 MiB) stops reading, so the producer's writes block. Nothing is dropped.
- When uevtd closes the connection (stop, `inbound.path` change), the producer's next write fails with `EPIPE`, or raises `SIGPIPE` unless the producer ignores it or writes with `MSG_NOSIGNAL`.
- Nothing is acknowledged. A message is safe once committed to the log; until then a crash, a stop or path change before it was read, or a failed write (`dropped`) loses it. While uevtd is not connected, buffering is the producer's business.

## Delivery

Every record gets an increasing `seq` and its receipt time `ts`, in µs since the Unix epoch, clamped so it never goes back. Each channel reads the log through its own durable cursor: one that is down falls behind and catches up, unless eviction overtakes it.

- At least once, in order. Unacknowledged messages are sent again after a reconnect, and cursors are saved every second, so a crash re-sends up to a second's worth. WebSocket receivers deduplicate by `seq`; MQTT consumers need an id in the payload.
- A new channel starts at the head, with no replay. Disabling keeps the cursor, removing deletes it, renaming makes a new channel.
- Reconnects back off from 1 s to 60 s, and the wait shortens only once the target acknowledges something. A channel that panics restarts from its cursor on the same schedule; others carry on.
- TLS (`wss://`, MQTT `tls`) trusts the platform's store, or only the PEM `ca_file`, read for each connection.
- A notice, at most 256 bytes, stands in for messages a channel will not deliver:

  ```json
  {"notice": {"event": "lost", "from_seq": 1, "to_seq": 3075, "count": 3075}}
  {"notice": {"event": "rejected", "seq": 4242, "ts": 1760928000123456, "reason": "payload too large"}}
  ```

  `lost`: gone before the channel delivered them, evicted or skipped as damaged or unreadable. `rejected`: too large for the channel; it stays in the log, readable through `/v1/messages`. Producers must not use a top-level `notice` key.

### WebSocket

One long-lived connection; `headers` go with the handshake. One text frame per record, the payload in `data` if it is JSON, else as a string in `text` with invalid UTF-8 replaced:

```json
{"seq": 7, "ts": 1760928000123456, "data": {"event": "boot"}}
{"seq": 8, "ts": 1760928000123457, "text": "a payload that is not JSON"}
{"seq": 3075, "notice": {"event": "lost", "from_seq": 1, "to_seq": 3075, "count": 3075}}
```

| Receiver answers | Meaning |
|---|---|
| `{"ack": n}` | every frame up to `seq` `n` is processed |
| `{"nack": n, "reason": "..."}` | frame `n` will never be accepted: skipped, counted in `rejected` |

- At most `inflight` frames and 1 MiB of payload wait for an answer; answering later slows the stream.
- A ping every `keep_alive_s`, which any message from the receiver answers. The session ends after `timeout_ms` with no answer and no TCP acknowledgement of what was sent meanwhile (Linux, macOS), so a slow link keeps its session while it moves. `timeout_ms` also bounds connecting.
- A record whose frame would exceed `max_frame_bytes` is replaced by a `rejected` notice. A message from the receiver over 64 KiB ends the session.

### MQTT

- MQTT 3.1.1, `clean_session`, `inflight` publishes pipelined. The cursor advances on `PUBACK`/`PUBCOMP` (QoS 1/2), or once written (QoS 0). Unacknowledged publishes go again, in order, after a reconnect. Notices go on the topic, never retained.
- A payload over `max_packet_bytes` is replaced by a `rejected` notice. Keep it within the broker's limit: MQTT 3.1.1 cannot tell an oversized packet from a lost connection.
- `timeout_ms` bounds connecting (DNS, TCP, TLS and the broker's answer together) and writing each publish, in whole seconds, rounded up. On a slow link, raise it to cover the largest publish, and `keep_alive_s` to cover what is queued ahead of a ping. A topic so long that `inflight` copies would exceed 1 MiB lowers `inflight`.
- `client_id` defaults to the host name, so devices on one broker need distinct host names. A host without one (empty, `(none)`) connects as `uevtd` and 16 random hex digits, new at every start. Brokers need only accept ids of up to 23 letters and digits; a refusal shows in `last_error`.
- Two enabled channels to one host cannot share a client id: a change that would is refused, so a second channel to a broker needs its own `client_id`. A file that already does still loads; both channels run, take turns on the broker, and report it in `last_error`.

## Configuration

JSON, every field optional, unknown fields rejected. Changes through the API or `SIGHUP` apply live, except to `storage.path` and `log.format`, which `/v1/status` lists in `restart_required`. Defaults:

```json
{
  "inbound": { "path": "/run/uevtd/in.sock", "max_message_bytes": 65536 },
  "storage": { "path": "/var/lib/uevtd", "max_bytes": 268435456, "segment_bytes": 8388608, "sync_interval_ms": 0 },
  "outbound": { "channels": {} },
  "api": { "path": "/run/uevtd/api.sock", "mode": "0660" },
  "log": { "filter": "info", "format": "text" }
}
```

Channels are keyed by name, and the name keys the cursor:

```json
{ "outbound": { "channels": {
  "ingest": { "type": "websocket", "url": "wss://collector.example/ingest",
              "headers": { "Authorization": "Bearer ..." } },
  "broker": { "type": "mqtt", "host": "broker.example", "port": 8883, "tls": true,
              "topic": "devices/42/events", "username": "device", "password": "secret" }
} } }
```

Other fields, with defaults. Both: `enabled` true, `ca_file` null, `timeout_ms` 10000, `keep_alive_s` 30. WebSocket: `inflight` 256, `max_frame_bytes` 1048576. MQTT: `port` 1883, `tls` false, `client_id` null, `username` and `password` null, `qos` 1, `retain` false, `inflight` 32, `max_packet_bytes` 1048576. Every field is in [contrib/config.example.json](contrib/config.example.json).

| Field | Rule |
|---|---|
| `inbound.path`, `api.path` | distinct, at most 107 bytes (103 on macOS); no change moves one onto the other's path, a swap included |
| `inbound.max_message_bytes` | 1 to 262144; a change applies to the open connection, the line in progress included |
| `storage.segment_bytes` | 64 KiB to 256 MiB, and at least `inbound.max_message_bytes` + 48 |
| `storage.max_bytes` | at least 2 × `segment_bytes`, at most 1 TiB |
| `storage.sync_interval_ms` | at most 60000 |
| `outbound.channels` | at most 8, named `[A-Za-z0-9][A-Za-z0-9_.-]{0,63}` |
| WebSocket | `url` `ws` or `wss`, without credentials or a fragment; `headers` valid, not the handshake's own |
| MQTT | `host` and `topic` not empty, `topic` without wildcards, `port` not 0, `client_id` null or not empty; strings at most 65535 bytes, and only `password` may hold U+0000 |
| both | `ca_file` only with `wss` or `tls`; `inflight` 1 to 1024, `keep_alive_s` 1 to 65535, `timeout_ms` at least 1, `max_frame_bytes` and `max_packet_bytes` at least 256, `qos` 0 to 2 |

Memory follows these limits, not traffic, since a backlog waits on disk: within them the daemon stays below about 50 MiB resident.

## Storage

```
/var/lib/uevtd/
  lock                       held by the daemon that has the log open
  00000000000000000001.seg   append-only segments, named by their first seq
  00000000000000016384.seg
  cursors/ingest             the next seq each channel delivers
```

- `sync_interval_ms` 0 syncs after every batch, a group commit under bursts; n syncs at most every n ms. Records are forwarded and queryable only once synced, so a crash loses at most the unsynced tail, never a record anyone has seen.
- Over `max_bytes`, the oldest whole segment goes, so the log stays within one `segment_bytes` of the limit. Channels behind it count what went in `lost`, logged at once, even while down.
- Every record has a CRC-32. At start, a torn write at the end is truncated; other damage there, or a read error, keeps the segment and numbers past what it could hold, so no `seq` is reused. Damage elsewhere is skipped: channels count what it held in `lost`, and `corrupt_segments` the segment.
- A channel retries an unreadable part of the log every second. After 10 minutes of EIO, as from a worn sector on SD or eMMC, it skips the rest of the segment: `lost`, and a notice. Other errors, such as permissions, are retried until fixed.
- A cursor file that holds no number replays the log from its oldest record.
- Files are created 0640, directories 0750. A segment is little-endian: a 24-byte header (magic `UEVT`, version 1, first seq, base ts), then `crc32 | len | seq | ts | payload` frames, each CRC covering what follows it (`src/storage/segment.rs`).

## HTTP API

On `api.path`. Its `mode` is the access control: `GET /v1/config` returns secrets.

| Method | Path | |
|---|---|---|
| GET | `/v1/config` | the configuration in effect |
| PUT | `/v1/config` | replace it: validated, saved, applied; returns what was applied |
| PATCH | `/v1/config` | [RFC 7386](https://www.rfc-editor.org/rfc/rfc7386) merge patch, likewise |
| GET | `/v1/messages` | time-range query, below |
| GET | `/v1/status` | counters, state and last errors as JSON, below |
| GET | `/v1/metrics` | the same in Prometheus text format |
| GET | `/v1/health` | `200 ok` while the log takes writes, else `503` and why |

```sh
curl --unix-socket /run/uevtd/api.sock http://uevtd/v1/status
curl --unix-socket /run/uevtd/api.sock -X PATCH http://uevtd/v1/config \
     -d '{"outbound":{"channels":{"ingest":{"enabled":false}}}}'
curl --unix-socket /run/uevtd/api.sock 'http://uevtd/v1/messages?from=1760928000000000&limit=10'
```

- Errors are `{"error": "..."}` with 400, 404, 405, 413, 500 or 503. Bodies are at most 256 KiB; a `PATCH` body, and a `PUT` document, its sections and its channels, must be objects.
- 16 connections are served at once, and more wait to be accepted. A connection closes after 10 s without a request, or once a read or write has waited 60 s. One query runs at a time.
- Health fails after a failed write or sync until a batch commits again, while one write or sync has taken more than 30 s, and while eviction cannot keep the log within `max_bytes`.

### `GET /v1/messages`

| Parameter | |
|---|---|
| `from` | receipt time at least this, µs since the Unix epoch |
| `to` | receipt time below this |
| `after` | `seq` above this, for paging |
| `limit` | 1 to 10000, default 100; a page also ends at 1 MiB of payload |

```json
{
  "messages": [
    { "seq": 1041, "ts": 1760928000123456, "data": { "event": "boot", "uptime": 3 } },
    { "seq": 1042, "ts": 1760928000223456, "text": "not json" }
  ],
  "next": 1042
}
```

`data` is a JSON payload verbatim, `text` any other payload as a string, invalid UTF-8 replaced. `next` is the page's last `seq` while more follow, and `null` on the last page; pass it as `after`.

### Status

Times are µs since the Unix epoch in `/v1/status`, seconds in `/v1/metrics`. A `last_error` is `{"ts", "message"}` or `null`. Besides plain counters:

| Field | |
|---|---|
| `inbound.connected`, `connections` | connected to the producer now; connections made |
| `inbound.last_error` | why the last connection failed or ended, until the next is made |
| `storage.first_ts`, `head_ts` | receipt times of the oldest and newest record, the range a query reaches |
| `storage.queued`, `queued_bytes` | waiting for the writer; the producer blocks at 8192 or 8 MiB |
| `storage.sync_us`, `syncs`, `records` | `sync_us / syncs` is the mean sync, `records / syncs` the messages per sync |
| `storage.dropped`, `last_error` | lost to failed writes or syncs, and the last failure until a batch commits; else why eviction is stuck |
| `storage.clock_steps` | records received while the clock was behind the log's newest, whose `ts` was raised to it |
| channel `lag`, `waiting_ts` | records in the log still to deliver, and the receipt time of the oldest |
| channel `delivery_us`, `delivered` | `delivery_us / delivered` is the mean time from receipt to acknowledgement |
| channel `rejected` | passed undelivered, refused or too large, each counted once |
| channel `cursor` | `null` for a channel disabled since it was added |

## Development

```sh
cargo test                                    # unit and end-to-end tests, no external services
cargo clippy --all-targets
UEVTD_SEEDS=10000 TMPDIR=/dev/shm cargo test  # more randomized log seeds, on a Linux tmpfs since every commit syncs
```

The end-to-end tests run the daemon in-process against a test producer, a scripted WebSocket receiver and a minimal MQTT broker, also over TLS with the certificates in `tests/data`.

## License

MIT, see [LICENSE](LICENSE).
