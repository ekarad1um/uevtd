# Changelog

## Unreleased

Configuration files that loaded before may now be rejected at startup. The
error names the field and its allowed range.

### Removed

- The HTTP channel. A configuration file with a `"type": "http"` channel no
  longer loads ("unknown variant `http`"); replace it with a `websocket`
  channel, whose receiver must implement the protocol in the README.
- `contrib/uevtd.service`. systemd is not a supported target. Whatever starts
  the daemon must start it again after a non-zero exit, and give it write
  access to the directory of its configuration file; see the README.

### Added

- The WebSocket channel, `"type": "websocket"`: messages stream as JSON text
  frames, `{"seq":n,"ts":t,"data":...}`, over one connection to a `ws://` or
  `wss://` URL with the configured handshake `headers`. The receiver answers
  `{"ack":n}` for every frame up to `n`, or `{"nack":n,"reason":"..."}` for one
  it will never accept. Up to `inflight` frames await an answer; unanswered
  ones are sent again after a reconnect; pings every `keep_alive_s` detect a
  dead connection.
- `ca_file` for WebSocket and MQTT channels: a PEM file whose certificates are
  trusted instead of the platform's store.
- Drop notices: a channel sends a notice in place of the messages it will not
  deliver, `{"notice":{"event":"lost","from_seq":a,"to_seq":b,"count":n}}` for
  messages evicted before delivery and
  `{"notice":{"event":"rejected","seq":n,"ts":t,"reason":"payload too large"}}`
  for one above `max_packet_bytes` or `max_frame_bytes`. MQTT publishes it on
  the channel's topic, never retained; WebSocket sends it as a frame with the
  `seq` to acknowledge. Notices are delivered at least once and in order.
- `notices` per channel in `/v1/status`, and `uevtd_channel_notices_total`.
- `restarts` per channel in `/v1/status`, and `uevtd_channel_restarts_total`.
- `/v1/health` answers 503 with the reason after a failed write or sync, until
  a batch is committed again, while one write or sync has taken more than
  30 s, and while eviction cannot remove a segment.
- A `lock` file in `storage.path`, held by the daemon that has the log open.
- In `/v1/status`, with the metric in brackets: `storage.first_ts`
  (`uevtd_storage_first_ts_seconds`), the receipt time of the oldest message
  in the log; `uevtd_storage_head_ts_seconds`; `storage.queued` and
  `queued_bytes` (`uevtd_storage_queued_messages`,
  `uevtd_storage_queued_bytes`), what waits for the writer; `storage.syncs`
  and `sync_us` (`uevtd_storage_syncs_total`,
  `uevtd_storage_sync_seconds_total`); `storage.dropped`
  (`uevtd_storage_dropped_total`), messages lost to a failed write or sync,
  and `storage.last_error`; per channel `waiting_ts`
  (`uevtd_channel_waiting_ts_seconds`), the receipt time of the oldest message
  waiting for delivery, and `delivery_us`
  (`uevtd_channel_delivery_seconds_total`), the time from receipt to
  acknowledgement summed over deliveries.

### Changed

- `inbound.max_message_bytes` defaults to 65536 and is at most 262144 (before:
  1048576 and 67108864). `inbound.max_connections` is at most 256 (before: 65536),
  and `max_connections × max_message_bytes` is at most 16 MiB. A configuration
  file written with the earlier defaults holds `"max_message_bytes": 1048576`;
  set it to 65536.
- `storage.segment_bytes` is between 64 KiB and 256 MiB and at least
  `inbound.max_message_bytes + 48`. `storage.max_bytes` is at most 1 TiB, and
  `storage.sync_interval_ms` is at most 60000.
- At most 8 channels. MQTT `inflight` is at most 1024 (before: 32767), and MQTT
  `max_packet_bytes` at least 256 (before: 1), so that a notice fits.
- MQTT `client_id` is `null` by default (before: `"uevtd"`). The channel then
  connects with the host name, or as `uevtd` and 16 hex digits on a host
  without a name, and logs the id when it starts. A configured id is used as
  given: a configuration file saved by an earlier version holds
  `"client_id": "uevtd"`, since the daemon writes every field, and keeps
  connecting as `uevtd` until it is set to `null`.
- Two enabled MQTT channels to the same host may not connect with the same
  client id, on any port, whether it is configured or the host name: a second
  channel to one broker needs a `client_id`. MQTT `topic`, `client_id`,
  `username` and `password` are at most 65535 bytes, and only `password` may
  contain U+0000.
- An MQTT message above `max_packet_bytes` stays rejected, but the cursor now
  passes it once its notice is acknowledged, not at once. A channel passes
  messages evicted before delivery the same way, once their notice is
  acknowledged.
- `lag` counts only messages still in the log, not those already evicted.
- `retries` counts reconnects, for both channels; MQTT did not count them before.
- `GET /v1/messages` returns a payload that is not valid JSON as `text` instead of
  `data`. A page holds at most 1 MiB of payload, and `next` is `null` on the last
  page.
- API request bodies are limited to 256 KiB.
- A cursor file that cannot be read restarts its channel at the oldest retained
  record instead of the head.
- Segment and cursor files are created with mode 0640, the log directories with
  0750.

### Fixed

- Memory is bounded under any load: the writer queue holds at most 8 MiB, each
  channel at most 1 MiB in flight, queries run one at a time, and on glibc the
  allocator returns large buffers to the system. Peak resident size under the
  heaviest load is about 54 MiB, down from 144 MiB.
- A single large message could evict the whole log; segments no longer grow past
  `storage.segment_bytes`.
- Messages evicted before a channel delivered them are counted in `lost` and
  logged when they are evicted. Before, they were counted only once the
  channel delivered again, so the loss stayed invisible during an outage and
  for good if the target never came back.
- A channel whose task panicked delivered nothing until the next configuration
  change, while `/v1/status` still showed it running. The panic is now reported
  in `last_error` at once, and the channel starts again from its cursor after
  1 s, doubling to 60 s while it makes no progress.
- After a crash of the daemon, not of the system, records that had been
  written but not yet synced became visible at the restart before they were
  durable, so a power loss soon after could take back records already
  delivered and reuse their sequence numbers. Recovery now syncs them first.
- A read error in the newest segment, such as a bad sector, or a segment that
  could not be opened, prevented startup until someone removed the file. The
  daemon now starts: the segment is kept as it is, channels retry the part
  that fails and report it in `last_error`, and new records are numbered past
  anything that part could hold. A sealed segment that cannot be opened, or
  whose size cannot be read, is retried the same way.
- One failed read in the middle of a segment, such as a transient I/O error,
  made the next read decode the bytes left from an earlier one and skip the
  rest of the segment as corrupt. Channels counted those intact messages as
  lost and never delivered them, and a query left them out of its answer
  without an error. A failed read is now retried from the disk.
- A second daemon started on the same `storage.path` worked on the log before
  it failed on a socket: it cut off the batch the first one was writing,
  removed its cursor files and started its channels, and the first one then
  lost messages it had committed. The log is now locked, and a second daemon
  stops at once, before it changes anything, with an error that names the
  process holding the log.
- Damage to messages already committed in the newest segment, such as a
  flipped bit or a block that never arrived on the disk, was cut off at
  startup like an interrupted write, and their sequence numbers were given to
  new messages, which receivers that drop duplicates then dropped. Only an
  interrupted write is cut off now; after other damage the segment is kept and
  numbering continues past every message it could hold. Numbering also
  continues past the highest channel cursor, so that the numbers of messages a
  disk lost after they were delivered are not given out again.
- A segment that could not be removed stopped eviction without a sign: the log
  grew past `storage.max_bytes` until the disk was full, while `/v1/health`
  answered 200. Health now fails with the error, which is also
  `storage.last_error`, and it is logged once a minute, until the segment can
  be removed.
- `corrupt_segments` counted a corrupt segment again for every channel, restart
  and query that read past it; it counts each segment once.
- A failed sync of a segment's first batch, followed by a failed removal of that
  segment, could put later messages where a restart did not look, and give
  their sequence numbers out again.
- A query that met a read error failed as a whole. It now returns the messages
  before the error, with `next` set, and the next page reports the error.
- A corrupt length in a record made every reader that met it allocate up to a
  segment's size, 256 MiB at most, at once.
- Two devices with the default configuration and distinct host names no
  longer disconnect each other in turn on a broker, and two channels of one
  daemon can no longer be configured to. They all connected as `uevtd`, and a
  broker drops a client when another connects with its id.
- An MQTT topic or client id that MQTT cannot carry broke the channel for good.
  With U+0000 in it, the broker closed every connection. A topic longer than
  65535 bytes was cut short on the wire, so every attempt published the message
  under a truncated topic and then failed on the acknowledgement.
- An MQTT broker that accepts the connection and then drops it, for example on
  the first publish, was retried once a second without end. Reconnects now back
  off to once a minute, and the wait shortens again only when the broker
  acknowledges something.
- Concurrent `PATCH /v1/config` requests no longer lose each other's changes.
- Setting `storage.sync_interval_ms` to 0 commits pending data within a second
  instead of waiting for the next message.
- A read error no longer drops the records read before it.
- A failed socket rebind is retried by resending the configuration or `SIGHUP`.
- Lowering `inbound.max_connections` takes effect for connections already open.
- A startup failure is reported as itself, not as "log writer exited
  unexpectedly".
- Stale cursor files are removed at startup; query-string secrets are redacted in
  logs; a `SIGHUP` during startup no longer kills the daemon.
- `clock_steps` counted messages from two connections that reached the writer
  slightly out of order, as if the clock had stepped back. It now counts only
  messages received while the clock was behind the newest message in the log,
  and only those log a warning. Both kinds are still clamped.
- `write_errors` missed failures to open or create a segment, and the messages
  lost to a failed write or sync were only logged; they are counted in
  `dropped` now. `uevtd_storage_batches_total` counts batches written,
  committed or not, as before, but its help text called them committed.
- A channel that had been disabled since startup showed `cursor` 0 and every
  message in the log as `lag`. It now shows the position in its cursor file, or
  `cursor: null` and `lag` 0 without one.
- 405 and 413 responses had a plain-text or empty body; they are JSON like every
  other error.
