# uevtd — Architecture

`uevtd` is a message forwarding daemon. It accepts newline-delimited messages on a
Unix domain socket, appends them to a bounded on-disk log, and forwards them to
configured HTTP and MQTT channels. A second Unix domain socket serves an HTTP API
for configuration, time-range queries over the log, status and metrics.

This document is the design plan for the module: what it does, how it is put
together, the invariants each part relies on, and the trade-offs that were made.
Section 12 lists the decisions that should be confirmed by the team.

## 1. Scope and principles

Functional scope, from the requirements:

| Area          | Requirement                                                                                     |
|---------------|-------------------------------------------------------------------------------------------------|
| Inbound       | Read messages from a configurable UDS path. Discard and log truncated messages.                 |
| Storage       | Persist messages under a configurable path and size limit; evict oldest without corruption.     |
| Outbound      | Forward to HTTP (URL, method, headers, timeout) and MQTT (broker, port, topic, connection).      |
| Configuration | HTTP API over UDS to read and update all of the above; apply live; persist to a file.           |
| Observability | Logs and metrics sufficient for troubleshooting and performance analysis.                       |

Workload assumptions that shape the design: writes are sparse over long periods
with bursts up to 1 000 msg/s of ~0.5 KiB each; reads are rare, time-ordered
range queries.

Design principles, in priority order: correctness, robustness, efficiency,
simplicity. Every component has a single owner task, every piece of shared
state has a stated invariant, and every failure mode in section 10 has a
defined behaviour.

## 2. System overview

```
 producers                       uevtd                                        targets
 ─────────                ─────────────────────────────────────────────       ───────
                          ┌──────────┐  mpsc   ┌────────────┐
 ──NDJSON over UDS──────▶ │ inbound  │ ──────▶ │ log writer │──▶ segments on disk
                          └──────────┘         │  (thread)  │     (append-only)
                                               └─────┬──────┘
                                                     │ head_seq (watch)
                                   ┌─────────────────┼─────────────────┐
                                   ▼                 ▼                 ▼
                             ┌──────────┐      ┌──────────┐      ┌──────────┐
                             │ http ch. │      │ mqtt ch. │      │ api query│
                             │ cursor=n │      │ cursor=m │      │ by time  │
                             └────┬─────┘      └────┬─────┘      └────┬─────┘
                                  ▼                 ▼                 ▲
                             HTTP endpoint     MQTT broker     HTTP API over UDS
                                                                (config/status/metrics)
```

The log is the single source of truth and the only coupling between inbound and
outbound. Inbound appends; every outbound channel is an independent reader with
its own durable cursor into the log. There are no in-memory queues to size, no
per-channel overflow policies, and a channel that is down simply falls behind
and catches up when it recovers. Backpressure is natural at both ends: a full
writer queue slows producers (socket flow control); a slow target shows up as
channel lag, bounded by the log's eviction horizon.

## 3. Module layout

```
src/
  main.rs          CLI, logging bootstrap, signal handling
  daemon.rs        composition root: wires components, owns shutdown order
  config.rs        schema, defaults, validation, atomic persistence, live updates (watch)
  logging.rs       tracing subscriber with reloadable filter
  inbound.rs       UDS listener, connection tasks, line framing (sans-IO `Framer`)
  storage/
    mod.rs         Storage: segment index, Writer (group commit, rotation, eviction),
                   Reader (tailing cursor), time-range query, cursor files
    segment.rs     on-disk encoding: segment header, record frame, CRC, recovery scan
  outbound/
    mod.rs         Supervisor: reconciles configured channels with running tasks;
                   Feed (cancel-safe batch reader), CursorStore (debounced persistence)
    http.rs        HTTP channel task
    mqtt.rs        MQTT channel task and the pure `Tracker` for packet-id/ack bookkeeping
  api.rs           HTTP API over UDS (axum): config, messages, status, metrics, health
  metrics.rs       Prometheus text rendering helper
```

Each module exposes a `run` future (components) or a small type (storage,
config) and depends only on `config` and `storage` types; `daemon.rs` is the
only place that knows about all of them.

## 4. Inbound

**Transport.** `SOCK_STREAM` Unix domain socket at `inbound.path`, created with
`inbound.mode` permissions. A stale socket file is removed only after a connect
attempt is refused, so a second instance fails fast instead of stealing the path.

**Framing.** One message per line, terminated by `\n` (an optional trailing
`\r` is stripped; empty lines are ignored). Newline framing was chosen because
it is what every producer can emit from any language and what the JSON payloads
already satisfy (serialised JSON never contains a raw newline). The payload is
stored verbatim; no parsing is attempted.

Edge cases, all handled by the sans-IO `Framer` so they are unit-testable:

| Situation                                   | Behaviour                                                            |
|---------------------------------------------|----------------------------------------------------------------------|
| Peer closes with an unterminated final line | Line discarded, `warn` with byte count, `inbound_truncated_total`+1 |
| Line longer than `max_message_bytes`        | Bytes discarded up to the next `\n`, `warn`, `inbound_oversized_total`+1 |
| More than `max_connections` clients         | Extra connection closed immediately, `warn`, counter                 |
| Writer queue full                           | Connection task awaits (socket flow control); nothing is dropped     |
| Daemon shutdown                             | Accept stops; connection tasks stop reading; buffered complete lines already handed to the writer are kept |

**Timestamps.** Every message receives a timestamp at receipt (microseconds
since the Unix epoch). This is the only time the daemon knows; nothing inside
the payload is inspected.

**Reconfiguration.** A change of `path` or `mode` binds the new socket before
the old listener is dropped; connections accepted on the old socket keep being
served until the peer closes. If the new bind fails the old listener stays and
the error is logged and reported in `/v1/status`. `max_message_bytes` and
`max_connections` apply to connections accepted after the change.

## 5. Storage

### 5.1 Layout

```
<storage.path>/
  00000000000000000001.seg      segment files, named by the first sequence number they hold
  00000000000000016384.seg
  ...
  cursors/
    <channel-name>              outbound cursor: next sequence number to deliver, decimal
```

The log is a sequence of append-only **segments**. The last segment is active;
all others are sealed and never modified. Eviction removes whole segments from
the oldest end. This gives the two properties the requirements ask for: a
message can never be half-evicted, and nothing is ever rewritten in place.

### 5.2 Encoding

Segment header, 24 bytes, little-endian:

```
magic     u32  0x54564555 ("UEVT")
version   u32  1
first_seq u64  sequence number of the first record (also encoded in the file name)
base_ts   i64  timestamp of the last record written before this segment was created
```

Record frame, 24-byte header followed by the payload:

```
crc32     u32  CRC-32 (IEEE) over the remaining header fields and the payload
len       u32  payload length in bytes
seq       u64  sequence number, strictly increasing by 1 across the whole log
ts        i64  receipt time, microseconds since Unix epoch, non-decreasing
payload   [len]
```

Overhead for a 512-byte payload is 4.7 %. The CRC covers everything after
itself, so a corrupted length, sequence or timestamp is detected as reliably as
a corrupted payload.

### 5.3 Invariants

1. `seq` is assigned by the single writer and increases by exactly one per
   record over the lifetime of the log, across segments and restarts.
2. `ts` is non-decreasing in `seq` order. The writer clamps each receipt time to
   `max(now, previous ts)`, so a backwards clock step never produces an
   out-of-order timestamp (it is logged when the step exceeds one second). This
   is what makes time-range queries a contiguous scan.
3. For consecutive segments `S_i`, `S_{i+1}`: every record in `S_i` has
   `ts <= S_{i+1}.base_ts` and every record in `S_{i+1}` has `ts >= S_{i+1}.base_ts`.
   A query from time `t` therefore starts at the last segment whose
   `base_ts < t` and never needs to look earlier.
4. Readers only read below a segment's *committed length*, published by the
   writer after a batch is fully written. A reader can never observe a partial
   record, even while the writer is appending.
5. A sealed segment's committed length is its file length. The active segment
   is validated on open; a torn tail (crash mid-write) is truncated to the last
   complete, CRC-valid record.

### 5.4 Write path

Inbound connection tasks send `Message { ts, payload }` into a bounded channel
(8 192 entries). A dedicated writer **thread** drains it with group commit:

1. Block for the first message, then take whatever else is already queued, up to
   1 024 messages or 1 MiB.
2. If the active segment has reached `segment_bytes`, seal it and create the
   next one (`first_seq = head + 1`, `base_ts = head_ts`; parent directory
   fsync'd so the file's existence is durable).
3. Encode the batch into one buffer, `write_all` it, then `fdatasync` according
   to `sync_interval_ms` (0 = after every batch; otherwise at most once per
   interval, with a timer so a trailing batch is never left unsynced).
4. Publish the new head (`watch<u64>`) and the segment's committed length; wake
   tailing readers.
5. Evict oldest segments while total size exceeds `max_bytes` (never the active one).

During a 1 000 msg/s burst each fsync covers however many messages arrived
while the previous one was in progress, so throughput is bounded by disk
bandwidth rather than fsync latency. During sparse traffic each message gets
its own fsync, which is the durability one wants for rare events. A dedicated
thread with synchronous I/O is simpler and faster than `spawn_blocking` per
batch and keeps file-system latency entirely off the async runtime.

**Write failure.** On an I/O error the batch is dropped and counted, the error
is logged, and the active file is truncated back to its committed length so a
partial write can never be followed by valid data. If truncation fails, or an
fsync fails (the kernel may then have discarded the dirty pages), the segment
is sealed and the next batch starts a new one, so a possible hole is always at
the end of a sealed segment and costs at most that segment's unsynced tail.

### 5.5 Eviction and capacity

Eviction granularity is one segment, so retained bytes stay within
`[max_bytes - segment_bytes, max_bytes]`. Validation requires
`max_bytes >= 2 * segment_bytes`. With the defaults (256 MiB / 8 MiB) and
0.5 KiB messages the log holds roughly 500 000 messages; at 1 msg/s that is six
days, and a 1 000 msg/s burst can run for eight minutes before evicting the
oldest segment. An oversized single message is still written (a segment may
exceed `segment_bytes` by one batch).

Lowering `max_bytes` at runtime evicts immediately; changing `segment_bytes`
applies to the next rotation.

### 5.6 Recovery

On open: list `*.seg`, validate each header against its file name, index
`(first_seq -> base_ts, len)`. Sealed segments are trusted structurally
(`len` = file size); every record is still CRC-checked when read. The active
segment is scanned in full; the first invalid or incomplete record ends it and
the file is truncated there with a warning. Head sequence and timestamp come
from the last valid record (or from `base_ts` and `first_seq - 1` for an empty
active segment). Trailing files that hold no record at all (a crash between
creating a segment and its first write) are removed so the previous segment
becomes active again. A segment whose header is unreadable is kept, its records
are verified on read, and it is never chosen as the start of a time-range scan
(its `base_ts` is treated as infinite, so scans begin one segment earlier).
Startup cost is one segment scan (8 MiB), independent of log size.

A corrupt record inside a sealed segment (bit rot) is logged with its offset;
the reader skips the remainder of that segment and continues with the next,
counting the skipped records as lost. There is no attempt to resynchronise
inside a segment: record boundaries are only known from the chain of lengths,
and guessing would risk delivering garbage.

### 5.7 Read path

`Reader` is a tailing cursor: `next_seq` plus an open file handle and buffered
position inside the segment that contains it. Reads use `pread` of 64 KiB
chunks bounded by the committed length and decode as many complete records as
fit the requested batch. Creating a reader at an arbitrary sequence costs one
scan from the start of that segment; subsequent reads are O(bytes returned).
When a reader's segment has been evicted it jumps to the oldest remaining one
and reports how many records it skipped.

Readers wait for new data with `watch::Receiver<u64>::wait_for(head >= next)`,
which is free of lost wake-ups and cancel-safe.

**Time-range query** (`GET /v1/messages`): locate the start segment from
`base_ts` (invariant 3), create a reader there (or at `after + 1` if later),
scan forward, keep records with `from <= ts < to`, stop at the first `ts >= to`
or at `limit`. Results are in sequence order; the caller paginates with
`after=<last seq>`.

### 5.8 Cursors

A cursor is the next sequence number a channel will deliver. It lives in
`cursors/<name>` and is written atomically (temp file, fsync, rename): when the
channel is first created, at most once per second while it is advancing, and
always when it stops. Writing it at creation is what lets a backlog that
accumulates before the first successful delivery survive a restart. On restart a channel resumes from its cursor; up to one second of
already-delivered messages may be delivered again (at-least-once). A cursor
that points below the eviction horizon jumps forward, and the gap is logged
and counted as lost.

## 6. Outbound

### 6.1 Delivery model

Each enabled channel is one task that reads the log from its cursor and
delivers in sequence order, one message at a time for HTTP, pipelined up to
`inflight` for MQTT. Semantics:

- **At-least-once.** The cursor only advances after the target acknowledged
  the message (2xx response; `PUBACK`/`PUBCOMP` for QoS 1/2; written to the
  socket for QoS 0).
- **Ordered.** A message is not skipped because a later one succeeded.
- **Head-of-line blocking is deliberate.** A transient failure blocks the
  channel with exponential backoff (HTTP: 100 ms to 30 s; MQTT reconnect: 1 s to
  60 s) until the target recovers. Lag is visible in metrics and bounded by the
  log's retention, after which the oldest messages are lost and counted.
- **Permanent rejections are skipped.** An HTTP 4xx other than 408/429, or an
  MQTT payload larger than `max_packet_bytes`, is logged with the sequence
  number, counted in `rejected`, and the cursor moves on. Retrying forever
  would stall the channel on one bad message.
- **New channels start at the head.** A channel without a cursor file forwards
  messages received after it was configured; it does not replay the backlog.
  That start position is persisted immediately. Disabling and re-enabling a
  channel keeps its cursor; removing it deletes the cursor.

### 6.2 HTTP channel

One `reqwest` client per channel (connection pooling, rustls, platform trust
store). Each message is one request: configured method and URL, configured
headers, body = payload verbatim, `Content-Type: application/json` unless the
configuration overrides it, `timeout_ms` for the whole request.

| Outcome                               | Action                                 |
|---------------------------------------|----------------------------------------|
| 2xx                                   | delivered; cursor advances             |
| 408, 429, 5xx, timeout, connect error | retry with backoff (channel blocks)    |
| other 4xx, 3xx (redirects disabled)   | rejected; logged; cursor advances      |

### 6.3 MQTT channel

`rumqttc` (MQTT 3.1.1) with the event loop driven in a dedicated task that
forwards events over a channel to the channel task. The channel task owns a
pure `Tracker` that maps packet ids to sequence numbers:

- `Outgoing::Publish(pkid)` with a packet id not held by any unacknowledged
  entry is bound to the oldest unsent entry (requests are processed FIFO); a
  packet id already held means a retransmission and is ignored; packet id 0
  (QoS 0) completes the oldest unsent entry immediately.
- `PubAck` (QoS 1) / `PubComp` (QoS 2) completes the entry holding that id.
- The cursor is the sequence number after the longest completed prefix.

The daemon manages reconnection itself: on any connection error the client and
event loop are dropped, every unacknowledged entry is marked unsent, and after a
backoff a fresh client is created and the entries are republished in order.
This avoids depending on broker session state (`clean_session` is always true)
and guarantees ordering across reconnects, at the cost of possible duplicates
(at-least-once). Outstanding publishes are capped at `inflight`, which is also
the request channel capacity, so `try_publish` never fails on capacity; the
library's own in-flight window is set to `2 * inflight + 1` so packet ids do not
collide under normal acknowledgement order.

TLS uses rustls with the platform verifier (same trust behaviour as the HTTP
channel). Username/password, client id, keep-alive, QoS and retain are configurable.

### 6.4 Reconciliation

The supervisor diffs the configured channel map against the running tasks on
every configuration change: removed or changed channels are cancelled (they
finish the message in flight and persist their cursor), added or changed ones
are started, unchanged ones are untouched. Channel identity is the map key, and
the key names the cursor file, so renaming a channel starts it at the head.

## 7. Configuration

### 7.1 Schema and defaults

```jsonc
{
  "inbound": {
    "path": "/run/uevtd/in.sock",
    "mode": "0660",
    "max_message_bytes": 1048576,
    "max_connections": 64
  },
  "storage": {
    "path": "/var/lib/uevtd",
    "max_bytes": 268435456,
    "segment_bytes": 8388608,
    "sync_interval_ms": 0
  },
  "outbound": {
    "channels": {
      "primary": {
        "type": "http",
        "enabled": true,
        "url": "https://example.invalid/ingest",
        "method": "POST",
        "headers": { "Authorization": "Bearer ..." },
        "timeout_ms": 10000
      },
      "broker": {
        "type": "mqtt",
        "enabled": true,
        "host": "broker.example.invalid",
        "port": 8883,
        "tls": true,
        "topic": "uevtd/events",
        "client_id": "uevtd",
        "username": null,
        "password": null,
        "qos": 1,
        "retain": false,
        "keep_alive_s": 30,
        "inflight": 32,
        "max_packet_bytes": 1048576
      }
    }
  },
  "api": { "path": "/run/uevtd/api.sock", "mode": "0660" },
  "log": { "filter": "info", "format": "text" }
}
```

JSON is used for the file because it is exactly what the API exchanges; one
representation, one serializer, no translation layer. Every field has a
default; unknown fields are rejected so typos surface at validation time.

### 7.2 Validation

Performed before anything is persisted or applied: socket paths non-empty and
distinct; `segment_bytes >= 64 KiB`, `max_bytes >= 2 * segment_bytes`; channel
names `[A-Za-z0-9][A-Za-z0-9_.-]{0,63}` (they become file names); HTTP URL,
method and header names/values parse; MQTT host and topic non-empty, topic has
no wildcards, QoS in 0..=2, `inflight >= 1`; log filter parses as a
`tracing` directive.

### 7.3 Persistence and live update

`PUT /v1/config` replaces the whole document; `PATCH /v1/config` applies an
RFC 7386 merge patch to the current document (so a single field can be changed
without resending secrets). Both validate, write the file atomically (temp
file with mode 0600, fsync, rename, directory fsync), then publish the new
configuration on a `watch` channel. `SIGHUP` re-reads the file and applies it
the same way. Components react to the fields they own:

| Field                           | Applied                                                    |
|---------------------------------|------------------------------------------------------------|
| `inbound.path`, `inbound.mode`  | live: rebind, old connections drain                        |
| `inbound.max_*`                 | live: new connections                                      |
| `storage.max_bytes`             | live: immediate eviction if needed                         |
| `storage.segment_bytes`         | live: next rotation                                        |
| `storage.sync_interval_ms`      | live: next batch                                           |
| `storage.path`                  | **restart**: reported in `/v1/status.restart_required`     |
| `outbound.channels.*`           | live: per-channel reconcile (section 6.4)                  |
| `api.path`, `api.mode`          | live: rebind; the response to the updating request completes on the old socket |
| `log.filter`                    | live: subscriber reload                                    |
| `log.format`                    | **restart**                                                |

Moving the log is a data migration, not a reconfiguration: cursors live inside
it and in-flight deliveries reference it. Doing it live would either lose the
undelivered backlog or require copying the directory. It is therefore the one
functional field that needs a restart, and the API makes that visible rather
than silently half-applying it.

A runtime failure to apply a validated configuration (for example the new
socket directory is not writable) keeps the previous runtime state, logs, and
is surfaced in `/v1/status`; the file still holds the new configuration, so the
operator sees exactly what will be in effect after the cause is fixed.

## 8. HTTP API (over `api.path`)

| Method | Path            | Purpose                                                           |
|--------|-----------------|-------------------------------------------------------------------|
| GET    | `/v1/config`    | current configuration                                             |
| PUT    | `/v1/config`    | replace configuration; returns the applied document               |
| PATCH  | `/v1/config`    | merge-patch configuration; returns the applied document           |
| GET    | `/v1/messages`  | `from`, `to` (µs, `to` exclusive), `after` (seq), `limit` (≤10 000) |
| GET    | `/v1/status`    | JSON: uptime, storage, inbound, per-channel cursor/lag/errors      |
| GET    | `/v1/metrics`   | Prometheus text exposition of the same counters                   |
| GET    | `/v1/health`    | 200 when the daemon is running                                    |

Errors are `{"error": "..."}` with 400 (validation), 404, or 500. Message
payloads are embedded verbatim in `/v1/messages` responses when they are valid
JSON and as a JSON string otherwise, so the response is always valid JSON even
for the "typically JSON" case.

Example: `curl --unix-socket /run/uevtd/api.sock http://uevtd/v1/status`.

## 9. Observability

**Logs** go to stderr through `tracing`, text or JSON, with a filter that is
reloadable at runtime. Every connection task carries a `conn` id and every
channel task a `channel` name as span fields. Startup logs the effective
configuration (secrets redacted); configuration changes log the changed
sections; bind, rebind, segment rotation, eviction, recovery truncation,
reconnects and rejections are logged at the appropriate level; per-message
events are `debug`/`trace`.

**Metrics** are plain atomic counters owned by each component, rendered on
demand. Gauges: active connections, storage bytes/segments/head/first seq,
per-channel cursor, lag and connected state. Counters: messages, bytes,
truncated, oversized, rejected connections, batches, write errors, evictions,
corrupt records, per-channel delivered, rejected, retries, lost. `/v1/status`
adds the last error per component with its timestamp and the
`restart_required` list.

## 10. Failure modes

| Failure                                   | Behaviour                                                                 |
|-------------------------------------------|---------------------------------------------------------------------------|
| Crash during a batch write                | Torn tail truncated on next start; those messages are lost (never acked)  |
| Crash between delivery and cursor save    | Up to ~1 s of messages redelivered                                        |
| Disk full / write error                   | Batch dropped and counted; file rolled back to committed length           |
| Bit rot in a sealed segment               | Remainder of that segment skipped, counted as lost; others unaffected     |
| Target down for longer than retention     | Channel loses the evicted messages, logs the count, continues at the oldest available |
| Broker drops the session                  | Unacked messages republished (duplicates possible), order preserved       |
| Another instance already listening        | Startup fails; the socket file is not stolen                              |
| Config file invalid at startup            | Startup fails with the validation error                                   |
| Config file missing at startup            | Defaults are used and written to the path                                 |
| Producer floods beyond disk bandwidth     | Writer queue fills, producers block on the socket; nothing dropped        |
| Backwards clock step                      | Timestamps clamp to the previous value; logged if > 1 s                   |

## 11. Concurrency and lifecycle

Tokio multi-thread runtime. Tasks: inbound acceptor + one task per connection;
one supervisor + one task per channel (+ one event-loop task per MQTT
channel); API server; config applier. One OS thread: the log writer. Blocking
file reads for channels and queries run on `spawn_blocking`, with the reader
handed in and out so cancellation never loses it.

Shutdown (SIGTERM/SIGINT): stop the inbound acceptor and cancel connection
tasks; drop the writer's senders so it drains and syncs the queue, then join
it; cancel channels, which abandon any request still in flight (the cursor has
not advanced, so it is retried later) and persist their cursors; stop the API.
A second signal exits immediately without waiting.

## 12. Decisions to confirm

1. **Line framing.** Newline-delimited messages on a stream socket. Alternatives:
   length-prefixed frames (robust to embedded newlines) or `SOCK_SEQPACKET`
   (kernel-preserved boundaries, 200 KiB-ish practical limit). Newline framing
   matches "typically JSON" producers; switching is a change confined to `Framer`.
2. **At-least-once with durable cursors** instead of best-effort forwarding.
   Duplicates are possible after a crash or MQTT reconnect; consumers should be
   idempotent on message content or an id inside the payload.
3. **Head-of-line blocking per channel.** Alternative: a per-channel
   concurrency knob for HTTP (`n` requests in flight, delivery order relaxed).
   Easy to add later; omitted to keep ordering guarantees simple.
4. **4xx is a permanent rejection** (skipped). If a target uses 4xx for
   transient conditions beyond 408/429, this should become configurable.
5. **New channels start at the head.** Alternative: a per-channel
   `start: "oldest" | "head"` field.
6. **`storage.path` and `log.format` require a restart** (section 7.3).
7. **Timestamp is receipt time**, microseconds. The API uses the same unit
   everywhere; no RFC 3339 parsing to avoid a date/time dependency.
8. **Secrets are returned by `GET /v1/config`.** Access control is the socket's
   file mode. Redaction with write-only semantics can be added if required.
9. **JSON configuration file**, rewritten by the daemon (comments cannot be
   preserved; TOML would need a second serializer).
10. **MQTT 3.1.1 only.** `rumqttc` has a v5 client; adding `protocol: "v5"` is
    straightforward if needed.

## 13. Dependencies

| Crate                      | Role                                                     |
|----------------------------|----------------------------------------------------------|
| tokio, tokio-util          | runtime, UDS, channels, cancellation                     |
| axum                       | HTTP API routing/JSON over the UDS listener              |
| reqwest (rustls)           | HTTP channel client                                      |
| rumqttc (rustls)           | MQTT 3.1.1 client                                        |
| rustls-platform-verifier   | system trust store for MQTT TLS (reqwest uses it already) |
| serde, serde_json          | configuration and API documents                          |
| tracing, tracing-subscriber| logging with reloadable filter                           |
| crc32fast                  | record checksums                                         |
| thiserror, anyhow          | error types / top-level error context                    |

No database, no mmap, no custom allocator, no date-time crate.
