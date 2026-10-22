# uevtd — Architecture

`uevtd` is a message forwarding daemon. It accepts newline-delimited messages on a
Unix domain socket, appends them to a bounded on-disk log, and forwards them to
configured WebSocket and MQTT channels. A second Unix domain socket serves an HTTP
API for configuration, time-range queries over the log, status and metrics.

This document is the design plan for the module: what it does, how it is put
together, the invariants each part relies on, and the trade-offs that were made.
Section 12 lists the decisions that should be confirmed by the team.

## 1. Scope and principles

Functional scope, from the requirements:

| Area          | Requirement                                                                                     |
|---------------|-------------------------------------------------------------------------------------------------|
| Inbound       | Read messages from a configurable UDS path. Discard and log truncated messages.                 |
| Storage       | Persist messages under a configurable path and size limit; evict oldest without corruption.     |
| Outbound      | Forward to MQTT (broker, port, topic, connection) and to HTTP endpoints, which became a WebSocket channel (URL, headers, timeout; §6.2). |
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
                             │ ws ch.   │      │ mqtt ch. │      │ api query│
                             │ cursor=n │      │ cursor=m │      │ by time  │
                             └────┬─────┘      └────┬─────┘      └────┬─────┘
                                  ▼                 ▼                 ▲
                           WebSocket receiver  MQTT broker     HTTP API over UDS
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
                   Reader (tailing cursor), Tail (loss accounting), time-range query,
                   cursor files
    queue.rs       bounded queue to the writer, by message count and payload bytes
    segment.rs     on-disk encoding: segment header, record frame, CRC, recovery scan
  outbound/
    mod.rs         Supervisor: reconciles configured channels with running tasks;
                   Feed (cancel-safe batch reader), CursorStore (debounced persistence),
                   reconnect backoff
    notice.rs      drop notices
    websocket.rs   WebSocket channel task, its frame writer, and the pure `Window`
                   for acknowledgement bookkeeping
    mqtt.rs        MQTT channel task and the pure `Tracker` for packet-id/ack bookkeeping
  tls.rs           client TLS for both channels: platform trust store or a CA file
  view.rs          the JSON shape of a record, shared by queries and WebSocket frames
  api.rs           HTTP API over UDS (axum): config, messages, status, metrics, health
  metrics.rs       Prometheus text rendering helper
```

Each module exposes a `run` future (components) or a small type (storage,
config) and depends only on `config` and `storage` types; `daemon.rs` is the
only place that knows about all of them.

## 4. Inbound

**Transport.** `SOCK_STREAM` Unix domain socket at `inbound.path`, created with
`inbound.mode` permissions. A stale socket file is removed only after a connect
attempt is refused, so a second instance fails fast instead of stealing the path;
anything at the path that is not a socket is an error, never removed. The probe
is bounded to one second, and a socket that does not answer counts as live.

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
| Writer queue full (8 192 messages or 8 MiB) | Connection task awaits (socket flow control); nothing is dropped     |
| Daemon shutdown                             | Accept stops; connection tasks stop reading; buffered complete lines already handed to the writer are kept |

**Timestamps.** Every message receives a timestamp at receipt (microseconds
since the Unix epoch). This is the only time the daemon knows; nothing inside
the payload is inspected.

**Reconfiguration.** A change of `path` or `mode` binds the new socket before
the old listener is dropped; connections accepted on the old socket keep being
served until the peer closes. If the new bind fails the old listener stays and
the error is logged and reported in `/v1/status`. `max_message_bytes` applies to
connections accepted after the change; `max_connections` is resized in place, so
after lowering it no new connection is admitted until enough open ones close.

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
Segment and cursor files are created with mode 0640 and the directories with
0750, independent of the umask, since they hold the messages themselves.

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
4. Readers only read below a segment's *committed length*, which the writer
   publishes once the data is synced. A reader can never observe a partial
   record, nor one that a crash could still take back, so a sequence number
   that was ever delivered or returned by a query is never reused.
5. A sealed segment's committed length is its file length. The active segment
   is validated on open; a torn tail (crash mid-write) is truncated to the last
   complete, CRC-valid record.

### 5.4 Write path

Inbound connection tasks send `Message { ts, payload }` into a bounded queue
of at most 8 192 messages and 8 MiB of payload, whichever comes first; a
message counts against the bytes until it has been written. A dedicated writer
**thread** drains it with group commit:

1. Block for the first message, then take whatever else is already queued, up to
   1 024 messages or 1 MiB of encoded records, and never more than an empty
   segment holds; a message that does not fit starts the next batch.
2. If the batch would carry the active segment past `segment_bytes`, seal it
   and create the next one (`first_seq = head + 1`, `base_ts = head_ts`; parent
   directory fsync'd so the file's existence is durable).
3. Encode the batch into one buffer and `write_all` it.
4. `fdatasync` according to `sync_interval_ms` (0 = after every batch;
   otherwise at most once per interval, with a timer so a trailing batch is
   never left unsynced; the interval is re-read while data is pending, so a
   change takes effect within a second). A successful sync *commits*: it publishes the new head
   (`watch<u64>`) and the segment's committed length and wakes tailing readers.
   Until then the batch is invisible, so a positive interval adds up to one
   interval of forwarding latency in exchange for fewer syncs.
5. Evict oldest segments while total size exceeds `max_bytes` (never the active one).

During a 1 000 msg/s burst each fsync covers however many messages arrived
while the previous one was in progress, so throughput is bounded by disk
bandwidth rather than fsync latency. During sparse traffic each message gets
its own fsync, which is the durability one wants for rare events. A dedicated
thread with synchronous I/O is simpler and faster than `spawn_blocking` per
batch and keeps file-system latency entirely off the async runtime.

**Write failure.** On a write error the batch is dropped and counted, the
error is logged, and the active file is truncated back to its length before
the batch so a partial write can never be followed by valid data. If that
truncation fails, or a sync fails (the kernel may then have discarded the
dirty pages), everything written since the last successful sync is discarded
and counted, the file is truncated to its committed length, and the segment is
sealed so the next batch starts a new one. Nothing visible is ever lost; the
cost is bounded by one sync interval of unsynced records.

### 5.5 Eviction and capacity

Eviction granularity is one segment, so retained bytes stay within
`[max_bytes - segment_bytes, max_bytes]`. Validation requires
`max_bytes >= 2 * segment_bytes` and `segment_bytes >= max_message_bytes + 48`
(the segment header and one frame header), so every message fits into a
segment. Since a batch that would cross `segment_bytes` starts a new segment, a
segment exceeds that size only when a single record is larger, which can happen
only to a message accepted just before the limits were lowered. With the
defaults (256 MiB / 8 MiB) and 0.5 KiB messages the log holds roughly 500 000
messages; at 1 msg/s that is six days, and a 1 000 msg/s burst fills an empty
log in about eight minutes. Once the log is full, every new segment evicts the
oldest one, whether or not every channel has delivered it; what a channel
loses that way is counted and logged at once (§5.9).

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
Where records are missing, because they were evicted or the rest of a
segment is corrupt, a read returns the gap on its own, as the number of
records it passed over, and never together with records. The records of a
batch are therefore consecutive, and a channel knows exactly which records it
will not deliver. An I/O error is reported only once the
records read before it have been returned, so a failing segment never costs
the records in front of it; a channel then retries the failing position every
second and shows the error in `last_error`.

Readers wait for new data with `watch::Receiver<u64>::wait_for(head >= next)`,
which is free of lost wake-ups and cancel-safe.

**Time-range query** (`GET /v1/messages`): locate the start segment from
`base_ts` (invariant 3), create a reader there (or at `after + 1` if later),
scan forward, keep records with `from <= ts < to`, stop at the first `ts >= to`
or when the page holds `limit` records or 1 MiB of payload (a larger first
record is still returned). Results are in sequence order. Because timestamps
never decrease, the first record left out tells whether the page is the last:
`next` is the page's last sequence number when more records follow and `null`
otherwise, and the caller paginates with `after=<next>`.

### 5.8 Cursors

A cursor is the next sequence number a channel will deliver. It lives in
`cursors/<name>` and is written atomically (temp file, fsync, rename): when the
channel is first created, at most once per second while it is advancing, and
always when it stops. Writing it at creation is what lets a backlog that
accumulates before the first successful delivery survive a restart. On restart a channel resumes from its cursor; up to one second of
already-delivered messages may be delivered again (at-least-once). A cursor
that points below the eviction horizon jumps forward, and the gap is logged
and counted as lost. A cursor past the head, which only a hand-repaired log
can produce, is reset to the head so that new records are not skipped. A cursor
file that cannot be parsed restarts the channel at the oldest retained record:
the position is unknown, and at-least-once means replaying rather than
skipping. Cursor files of channels that are no longer configured are removed at
startup, as they are when a channel is removed at runtime.

### 5.9 Loss accounting

Every running channel keeps its position in the log as a *tail*: every record
below it is in the channel's hands (delivered, or held until acknowledged) or
counted as lost. Eviction, under the index lock, compares the new oldest
sequence number with each tail. The records a channel had not read are added
to its `lost` counter at that moment and logged with their range, whether the
channel is connected or not. While a channel's read is running, eviction
leaves its tail alone. The read counts the gaps it passes over itself, such as
a corrupt segment or a segment evicted under it, and when it ends it counts
whatever was evicted beyond its position meanwhile. A channel that starts with
a cursor below the oldest record counts the difference when it starts. Each
record is therefore counted as soon as its loss is known, and the reader then
reports the gap so that the channel can send a notice in its place (§6.1).

A record is counted once. A channel is restarted on every change to its
configuration, and its new tail starts at the cursor, which stays below a
gap until the gap's notice is acknowledged. The `lost` counter therefore
remembers how far it has counted, and a later tail counts only what lies
beyond; its reader still reports the whole gap, so the notice is sent again.
One case is left uncounted: records a channel held for an acknowledgement
when it was restarted, if they were evicted while it held them. They are at
most what a channel holds for delivery (§11.1), and the notice still covers
them.

The warning is repeated at most once a minute, with the running total, since
a channel that stays behind loses records with every eviction. A disabled
channel has no tail; what it lost is counted when it is enabled again. A
cursor saved up to a second behind (§5.8) makes a crash report records
delivered in that second as lost, if they are evicted before the restart.

## 6. Outbound

### 6.1 Delivery model

Each enabled channel is one task that reads the log from its cursor and
delivers in sequence order, with up to `inflight` messages awaiting
acknowledgement. Semantics:

- **At-least-once.** The cursor only advances after the target acknowledged
  the message (`{"ack": n}` from a WebSocket receiver; `PUBACK`/`PUBCOMP` for
  MQTT QoS 1/2; written to the socket for QoS 0). After a lost connection the
  unacknowledged messages are sent again, in order, so duplicates are possible.
- **Ordered.** A message is not skipped because a later one succeeded.
- **Head-of-line blocking is deliberate.** A lost connection blocks the channel
  until the target is back. It reconnects after 1 s, doubling to 60 s, and the
  wait shortens again only when an acknowledgement advances the cursor. A
  target that accepts every connection but takes nothing, such as a broker
  that disconnects on the first publish or a receiver that closes on each
  frame, is therefore retried less and less often, not once a second. Lag is
  visible in metrics and bounded by the log's retention, after which the oldest
  messages are lost, counted and logged as they are evicted (§5.9).
- **Permanent rejections are skipped.** A payload too large for the channel
  (`max_frame_bytes`, `max_packet_bytes`) or a record the WebSocket receiver
  refuses with a `nack` is logged with the sequence number, counted in
  `rejected`, and passed. Retrying forever would stall the channel on one bad
  message. The record stays in the log and can be queried until it is evicted.
- **A skipped record leaves a notice.** A channel sends a *drop notice* in place
  of the records it will not deliver, so that the receiver learns of the gap.
  MQTT publishes it on the channel's own topic:

  ```json
  {"notice": {"event": "lost", "from_seq": 1, "to_seq": 3075, "count": 3075}}
  {"notice": {"event": "rejected", "seq": 4242, "ts": 1760928000123456, "reason": "payload too large"}}
  ```

  and WebSocket sends it as a frame with the sequence number to acknowledge,
  the last one it stands for: `{"seq": 3075, "notice": {...}}`. `lost` covers
  records that left the log before the channel read them; `rejected` names a
  record that is too large to send, with the time under which `/v1/messages`
  still returns it. A refused record gets no notice, since the receiver refused
  it itself. A notice travels like a message and the cursor passes the records
  it stands for only once it is acknowledged, so it is delivered at least once
  and in order. MQTT never retains a notice, since it would replace the topic's
  last message for new subscribers, and MQTT receivers tell notices apart by
  the top-level `notice` key, which a producer's own messages should therefore
  not use. Every notice is at most 256 bytes, as a payload or as a frame.
- **New channels start at the head.** A channel without a cursor file forwards
  messages received after it was configured; it does not replay the backlog.
  That start position is persisted immediately. Disabling and re-enabling a
  channel keeps its cursor; removing it deletes the cursor.

### 6.2 WebSocket channel

The requirements name an HTTP channel; it became a WebSocket channel, so that
messages stream over one long-lived connection with explicit acknowledgements
instead of one request per message, whose throughput is one message per round
trip and whose status codes cannot tell a refused message from a broken
endpoint. The receiver must speak the protocol below.

**Frames.** One text frame per record, in sequence order, in the shape
`/v1/messages` returns (`src/view.rs`): `{"seq": n, "ts": us, "data": <payload>}`
when the payload is valid JSON, embedded verbatim, and
`{"seq": n, "ts": us, "text": "<payload>"}` otherwise, with invalid UTF-8
replaced. Notices are frames too (§6.1). Every frame carries a `seq`.

**Answers.** The receiver sends text frames: `{"ack": n}` means every frame up
to sequence number `n` is processed, so one acknowledgement covers any number
of frames; `{"nack": n, "reason": "..."}` means frame `n` will never be
accepted. The cursor is the end of the longest answered prefix. Anything else
the receiver sends is ignored, a message above 64 KiB ends the session, and a
close frame or a read error ends it as well. Receivers deduplicate by `seq`.

**Window.** At most `inflight` frames and 1 MiB of payload are unanswered; a
receiver throttles the stream by answering later, and the backlog waits on
disk. A record whose frame would exceed `max_frame_bytes` is replaced by a
notice, so that a receiver's frame limit cannot turn into a reconnect loop.

**Session.** TCP, TLS for `wss://` (§6.5), and the handshake with the
configured `headers` must complete within `timeout_ms`. The channel pings every
`keep_alive_s` and ends the session if no pong arrives within `timeout_ms`.
After a session ends, the unanswered frames are sent again in order in the
next one. A channel that stops, for a configuration change or at shutdown,
sends a close frame (1001, going away), waits at most a second for it to be
written, and persists its cursor.

**Structure.** The socket is split. A writer task owns the sending half and
drains a queue of frames, so the channel task's `select!` can never abandon a
frame half-written; a record is encoded only when it is written, and the
window and the queue share its payload. The channel task selects over the
receiver's messages, `Feed::next` while the window has room, the ping and
cursor timers, and cancellation. The queue holds at most two windows: what is
sent again after a reconnect, and new frames. A receiver can only acknowledge
frames it has received in some session, so a fuller queue means a broken
receiver, and the session ends.

`connected` reflects a live session, `delivered` counts acknowledged records,
`rejected` refused and oversized ones, and `retries` sessions that failed or
ended, as for MQTT.

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
- Every publish is an entry, a drop notice included, since the library hands
  out packet ids in request order. A notice's entry stands for the records it
  covers, so completing it moves the cursor past them; it counts in
  `notices`, not in `delivered`.

The daemon manages reconnection itself: on any connection error the client and
event loop are dropped, every unacknowledged entry is marked unsent, and after
the backoff of §6.1 a fresh client is created and the entries are republished
in order. A CONNACK does not shorten the backoff; an acknowledgement that
advances the cursor does.
This avoids depending on broker session state (`clean_session` is always true)
and guarantees ordering across reconnects, at the cost of possible duplicates
(at-least-once). Outstanding publishes are capped at `inflight`, which is also
the request channel capacity, so `try_publish` never fails on capacity; the
library's own in-flight window is set to `2 * inflight + 1` so packet ids do not
collide under normal acknowledgement order. They are also capped at 1 MiB of
payload plus one message, because the channel and the library each keep a copy
of a publish until it is acknowledged; an acknowledged payload is released at
once, even while an earlier publish is still outstanding.

With `tls` the connection uses the TLS configuration of §6.5. Username and
password, client id, keep-alive, QoS and retain are configurable.

A broker disconnects a client when another connects with its client id, so two
clients sharing one take turns. Without a configured id, a channel connects as
`uevtd` and 16 hex digits: a SipHash of the channel name and the process start
time, keyed from the OS's random source once per process. Every session of the
channel uses the same id while the process runs, ids differ across channels
and devices, and the form, 21 letters and digits, is one that every MQTT 3.1.1
broker must accept (up to 23). The host name would not do: devices built from
one image share it, and on a public broker a predictable id can collide with
someone else's. Sessions are clean, so nothing depends on the id surviving a
restart. Validation rejects two enabled channels with the same configured id
on one host (§7.2).

### 6.4 Reconciliation

The supervisor diffs the configured channel map against the running tasks on
every configuration change: removed or changed channels are cancelled, added or
changed ones are started, unchanged ones are untouched. A cancelled channel
abandons what is awaiting acknowledgement and persists its cursor, and a
WebSocket channel also sends a close frame; the new task sends the
unacknowledged messages again. Channel identity is the map key, and the key
names the cursor file, so renaming a channel starts it at the head.

### 6.5 TLS

Both channels use rustls through one configuration (`src/tls.rs`). By default
it verifies with the platform's trust store; it is built once and shared by
all channels and reconnects. With `ca_file`, a PEM file of one or more
certificates, only those certificates are trusted. The file is read again for
every connection, so a replaced file applies from the next reconnect, and a
missing or empty one is reported in `last_error` and retried like a
connection failure. Client certificates are not supported.

## 7. Configuration

### 7.1 Schema and defaults

```jsonc
{
  "inbound": {
    "path": "/run/uevtd/in.sock",
    "mode": "0660",
    "max_message_bytes": 65536,
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
        "type": "websocket",
        "enabled": true,
        "url": "wss://example.invalid/ingest",
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
        "host": "broker.example.invalid",
        "port": 8883,
        "tls": true,
        "ca_file": null,
        "topic": "uevtd/events",
        "client_id": null,
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
distinct; `max_message_bytes` in 1..=256 KiB and `max_connections` in 1..=256,
with `max_connections × max_message_bytes` at most 16 MiB; `segment_bytes` in
64 KiB..=256 MiB and at least `max_message_bytes + 48`, `max_bytes >= 2 *
segment_bytes` and at most 1 TiB, `sync_interval_ms <= 60 000`; at most 8
channels, named `[A-Za-z0-9][A-Za-z0-9_.-]{0,63}` (they become file names);
WebSocket URL parses with scheme `ws` or `wss`, no credentials (they belong in
`headers`) and no fragment, header names and values parse and leave the
handshake's own headers alone, `ca_file` only with `wss`, `timeout_ms` at least
1, `keep_alive_s` in 1..=65535, `inflight` in 1..=1024, `max_frame_bytes` at
least 256; MQTT host and topic non-empty, topic has no wildcards, `client_id`
non-empty when set and not shared by two enabled channels to one host (on any
port, the host compared without case), topic, `client_id` and `username` at
most 65535 bytes and without U+0000, `password` at most 65535 bytes (MQTT
strings have 16-bit lengths, which the library would truncate, and only the
password is binary), `ca_file` only with `tls`, QoS in 0..=2, `keep_alive_s` in
1..=65535 (the protocol field is 16 bits), `inflight` in 1..=1024,
`max_packet_bytes` at least 256; log filter parses as a `tracing` directive.
The minimum sizes make room for a drop notice, and the bounds on sizes and
counts are what bounds memory (section 11.1). A `ca_file` is not read at
validation; a missing file shows when the channel connects (§6.5).

### 7.3 Persistence and live update

`PUT /v1/config` replaces the whole document; `PATCH /v1/config` applies an
RFC 7386 merge patch to the current document (so a single field can be changed
without resending secrets). Both validate, write the file atomically (temp
file with mode 0600, fsync, rename, directory fsync), then publish the new
configuration on a `watch` channel. A patch reads, merges and writes under one
lock, so concurrent patches compose. `SIGHUP` re-reads the file and applies it
the same way. An unchanged configuration is still republished, by `PUT`,
`PATCH` or `SIGHUP`, which is how a component retries an apply that failed
earlier, such as a rebind. Components react to the fields they own:

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
| GET    | `/v1/messages`  | `from`, `to` (µs, `to` exclusive), `after` (seq), `limit` (≤10 000); pages hold at most 1 MiB of payload |
| GET    | `/v1/status`    | JSON: uptime, storage, inbound, per-channel cursor/lag/errors      |
| GET    | `/v1/metrics`   | Prometheus text exposition of the same counters                   |
| GET    | `/v1/health`    | 200 when the daemon is running                                    |

Errors are `{"error": "..."}` with 400 (validation), 404, or 500. In
`/v1/messages` responses a payload that is valid JSON is embedded verbatim as
`data`; any other payload is returned as a string in `text`, so the two cases
cannot be confused and the response is always valid JSON. Request bodies are
limited to 256 KiB (413 beyond that), and one query is read and serialized at a
time while others wait.

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
corrupt records, per-channel delivered, rejected, retries, lost, notices.
`lost` grows when records are evicted, not when the channel next reads
(§5.9), and `lag` counts only records still in the log. `/v1/status`
adds the last error per component with its timestamp and the
`restart_required` list.

## 10. Failure modes

| Failure                                   | Behaviour                                                                 |
|-------------------------------------------|---------------------------------------------------------------------------|
| Crash during a batch write                | Torn tail truncated on next start; those messages are lost (never acked)  |
| Crash between delivery and cursor save    | Up to ~1 s of messages redelivered                                        |
| Disk full / write error                   | Batch dropped and counted; file rolled back to its previous length        |
| fsync error                               | Unsynced tail discarded and counted; segment sealed; nothing visible lost |
| Socket path points at a non-socket file   | Startup or rebind fails; the file is left alone                           |
| Log writer thread panics                  | Daemon stops with an error (exit code 1) so the service manager restarts it |
| Bit rot in a sealed segment               | Remainder of that segment skipped, counted as lost, a notice in its place; others unaffected |
| Segment unreadable during delivery        | Records before it are delivered; the channel retries every second and reports `last_error` |
| Cursor file unreadable                    | Channel replays from the oldest retained record                            |
| Target down for longer than retention     | Evicted messages counted as lost and logged as they are evicted; once the target is back, the channel sends a notice in their place and continues at the oldest available |
| Broker or receiver drops the session      | Unacknowledged messages sent again (duplicates possible), order preserved |
| Another client connects with the MQTT client id | The broker drops the older session, so the two take turns; generated ids avoid it, and validation rejects one configured id on two channels to a host (§6.3) |
| Target accepts connections but takes nothing | Reconnects back off to once a minute; the cursor stays; `retries` and `last_error` show it |
| Receiver stops answering pings            | Session ends after `timeout_ms`; reconnect with backoff                   |
| TLS verification fails, e.g. a wrong clock | Connection fails, reported in `last_error`, retried with backoff          |
| Another instance already listening        | Startup fails; the socket file is not stolen                              |
| Config file invalid at startup            | Startup fails with the validation error                                   |
| Config file missing at startup            | Defaults are used and written to the path                                 |
| Producer floods beyond disk bandwidth     | Writer queue fills, producers block on the socket; nothing dropped        |
| Backwards clock step                      | Timestamps clamp to the previous value; logged if > 1 s                   |

## 11. Concurrency and lifecycle

Tokio multi-thread runtime. Tasks: inbound acceptor + one task per connection;
one supervisor + one task per channel (+ one event-loop task per MQTT
channel, one frame writer per WebSocket session); API server; config applier.
One OS thread: the log writer. Blocking file reads for channels and queries,
and reading a CA file, run on `spawn_blocking`, with the reader handed in and
out so cancellation never loses it.

Shutdown (SIGTERM/SIGINT): stop the inbound acceptor and cancel connection
tasks; drop the writer's senders so it drains and syncs the queue, then join
it; cancel channels, which abandon what awaits acknowledgement (the cursor has
not passed it, so it is sent again later), close their WebSocket sessions and
persist their cursors; stop the API. A second signal exits immediately without
waiting.

### 11.1 Memory

The target is a device with 128 MiB of RAM. The backlog lives in the log on
disk, so memory does not grow with traffic or with how far a channel is behind.
Every buffer that holds messages has a byte bound:

| Holder | Bound | With the defaults |
|--------|-------|-------------------|
| Inbound connections | each holds at most one message and an 8 KiB read buffer; a complete message is copied out only once the queue has room for it; `max_connections × max_message_bytes` ≤ 16 MiB | 64 × 72 KiB |
| Writer queue | 8 192 messages and 8 MiB of payload, counted until written | 8 MiB, reached only when producers outrun the disk |
| Write batch | 1 MiB of encoded records, or one record | 1 MiB |
| Each channel | 1 MiB of payload awaiting acknowledgement, plus one message. MQTT keeps a second copy inside rumqttc; WebSocket shares the payload with its frame queue and encodes one frame at a time, and its incoming messages are limited to 64 KiB | about 2 MiB |
| Each log reader | a 64 KiB read buffer, or one record if larger | 64 KiB |
| A query | one at a time: a 1 MiB page plus one record, its JSON, and a 256 KiB read batch | about 3 MiB |
| An API request | a 256 KiB body | small |

The JSON of a query page, or of a WebSocket frame, can reach six times the
payload for binary payloads (control bytes are escaped as `\u00XX`); valid JSON
payloads are embedded as they are.

On glibc the binary fixes the allocator's mmap threshold at 128 KiB and limits
it to two arenas (`src/main.rs`). By default glibc raises the threshold after
every large free and keeps a heap per thread, so the buffers of large messages
stayed with the process after use: without the setting, the heaviest load below
reached more than twice its peak, 122 MiB.

Resident size of the release binary, measured on Linux (glibc, 7 cores) while
48 connections send messages of the maximum size as fast as they can, 16 more
hold a partial message each, and queries run in a loop. The WebSocket
receivers acknowledge each frame 2 s late, so their channels' windows stay
full:

| Configuration | Peak |
|---------------|------|
| Idle, one MQTT and one WebSocket channel | 6 MiB |
| Defaults (64 KiB messages); one MQTT channel and one slow WebSocket receiver | 31 MiB |
| Every limit at its maximum (256 KiB messages); four MQTT channels and four slow WebSocket receivers | 53 MiB |

## 12. Decisions to confirm

1. **Line framing.** Newline-delimited messages on a stream socket. Alternatives:
   length-prefixed frames (robust to embedded newlines) or `SOCK_SEQPACKET`
   (kernel-preserved boundaries, 200 KiB-ish practical limit). Newline framing
   matches "typically JSON" producers; switching is a change confined to `Framer`.
2. **At-least-once with durable cursors** instead of best-effort forwarding.
   Duplicates are possible after a crash or a reconnect. WebSocket receivers
   deduplicate by `seq`; MQTT consumers need an id inside the payload.
3. **Head-of-line blocking per channel.** A message is retried until it is
   acknowledged, refused or too large; nothing else skips it.
4. **WebSocket instead of HTTP** (§6.2). Receivers must implement the
   acknowledgement protocol; plain HTTP endpoints cannot be targeted.
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
| tokio-tungstenite          | WebSocket client (handshake and framing only)            |
| futures-util               | splitting a WebSocket into its sending and receiving halves |
| tokio-rustls               | TLS for WebSocket; the same rustls that rumqttc uses     |
| url                        | parsing WebSocket URLs                                   |
| rumqttc (rustls)           | MQTT 3.1.1 client                                        |
| rustls-platform-verifier   | the platform's trust store for both channels             |
| serde, serde_json          | configuration and API documents                          |
| tracing, tracing-subscriber| logging with reloadable filter                           |
| crc32fast                  | record checksums                                         |
| thiserror, anyhow          | error types / top-level error context                    |

No database, no mmap, no custom allocator, no date-time crate. reqwest is a
development dependency only, as the API client of the end-to-end tests.
