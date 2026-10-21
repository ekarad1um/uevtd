# Changelog

## Unreleased

Configuration files that loaded before may now be rejected at startup. The
error names the field and its allowed range.

### Removed

- The HTTP channel. A configuration file with a `"type": "http"` channel no
  longer loads ("unknown variant `http`"); replace it with a `websocket`
  channel, whose receiver must implement the protocol in the README.

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
