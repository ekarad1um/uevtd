# uevtd review: findings and design notes

Reviewed at `d4fe3c1` (2026-10-20). Every source file, the docs and `contrib/` were read against the
requirements in [TODO.md](TODO.md) and the invariants in [ARCHITECTURE.md](ARCHITECTURE.md). Nothing in
the repository was changed apart from adding this file. A second review, at `407be9d` after the fixes
of §8, is in §9.

How the findings were checked:

- Baseline: `cargo test` passes (34 unit, 5 e2e) on macOS and on the dev container (Debian, aarch64,
  rustc 1.96.1). `cargo clippy --all-targets` is clean.
- Probes: small tests in a scratch crate that depends on `uevtd` by path (see the appendix). All but two
  reproduce a finding below; p06 and p13 confirmed correct behavior and are listed under §6.
- Library behavior was read in the sources of the locked versions (rumqttc 0.25.1, reqwest 0.13.5) and in
  systemd's source.

Severity: **High** means data loss or a broken deployment in a plausible scenario. **Medium** means wrong
behavior or a resource risk under specific but realistic conditions. **Low** covers edge cases,
hardening, observability and docs. Design questions that need your decision are in §4.

## 1. Summary

| ID  | Sev    | Area          | Finding | Evidence |
|-----|--------|---------------|---------|----------|
| H1  | —      | Deployment    | Withdrawn: systemd is not a supported target. What still applies is listed under H1 | — |
| H2  | High   | HTTP          | 401/403/404/3xx are treated as per-message rejections, so an auth or URL problem drops every message until it is fixed. Superseded: the HTTP channel is replaced (D12) | probe p09 |
| M1  | Medium | Storage       | Switching `sync_interval_ms` from >0 to 0 leaves the last batch uncommitted until another message arrives | probe p01 |
| M2  | Medium | Config        | `max_message_bytes` is not tied to `segment_bytes`/`max_bytes`; with small custom limits one large message evicts the whole backlog | probe p02 |
| M3  | Medium | API           | Concurrent PATCH requests lose updates while both callers get 200 | probe p07 (20/20) |
| M4  | Medium | Memory        | Buffers are bounded by message count, not bytes: writer queue, query pages, framers | code |
| M5  | Medium | MQTT          | Backoff resets on every CONNACK, so a publish the broker answers by disconnecting loops at ~1 reconnect/s forever | code + rumqttc |
| M6  | Medium | Outbound      | A channel task that dies is never restarted, and still reports `running`, until the next config change | code |
| M7  | Medium | Storage       | Recovered records become visible before they are synced; a segment file that cannot be opened or scanned prevents startup | code |
| M8  | Medium | Outbound      | Records evicted before delivery are counted and logged only when the channel reads again, so `lost` stays 0 during an outage | probes p10, p11 |
| M9  | Medium | Storage       | A read error in the middle of a batch silently drops the records already read; a persistent error then stalls the channel with no skip and no counter | probe p12 |
| L1  | Low    | Config        | `2 * segment_bytes` overflows: debug builds panic in the handler, release builds accept an unbounded segment | probe p03 |
| L2  | Low    | Config        | No upper bound on `sync_interval_ms`, and huge values truncate to microseconds | probe p04 |
| L3  | Low    | Storage       | One corrupt record skips the rest of its segment, and in the active segment everything up to the head | probe p05 |
| L4  | Low    | Config        | A failed rebind is retried only when another field changes; re-sending the config or SIGHUP is a no-op | code |
| L5  | Low    | Inbound       | Changing `max_connections` swaps the semaphore, so existing connections stop counting against the limit | code |
| L6  | Low    | HTTP          | During a long batch the cursor is persisted only after the batch, not every second. Moot after D12 | code |
| L7  | Low    | HTTP          | The response-body drain ignores cancellation; stops wait up to `timeout_ms` each, one channel at a time. Moot after D12 | code |
| L8  | Low    | Observability | Several counters and gauges misreport or are missing | code |
| L9  | Low    | Security      | Segment and cursor files are created with the umask default (typically 0644) | code |
| L10 | Low    | MQTT          | Default `client_id` "uevtd" is shared by every channel and device, which evict each other on one broker | code |
| L11 | Low    | Outbound      | An unreadable cursor restarts the channel at the head without counting the skipped records as lost | code |
| L12 | Low    | Misc          | Smaller items, listed in the detail section | code |

## 2. Requirements traceability (TODO.md)

| Requirement | Status | Notes |
|-------------|--------|-------|
| Inbound: configurable UDS path | Met | Live rebind keeps old connections. |
| Inbound: connection edge cases; truncated messages discarded and logged | Met | EOF mid-line, oversized lines, connection limit, shutdown. Producers must send compact JSON, one per line (§4, D9). |
| Storage: configurable path and size limit | Met with gaps | The limit can be exceeded by a large message (M2, L1). |
| Storage: evict old data without breaking messages | Met | Whole-segment eviction. |
| Storage: robust and efficient for sparse writes, bursts and time-range reads | Met with gaps | Group commit works as designed. M1, M7, M9, L3, and the timestamp design question D2. |
| Outbound HTTP: URL, method, headers, timeout | Met with gaps | H2; throughput D1; no TLS options (custom CA, client certificate). |
| Outbound MQTT: broker, port, topic, connection parameters | Met with gaps | M5, L10; no custom CA or client certificate (D5). |
| Config API over UDS covering all parameters | Met with gaps | M3. |
| Applied live without disrupting in-flight processing | Partly | Changing a channel abandons its in-flight request (a duplicate, D8); M1. |
| Persisted to a file | Met | The config file's directory must be writable by the daemon (H1). |
| Minimal, mainstream dependencies; modularity | Met | |
| Logging and observability for troubleshooting and performance analysis | Partly | Good logs and counters. Drops of undelivered messages surface late (M8); no latency, queue or time-lag metrics (L8, D6). |

## 3. Findings

### H1. Withdrawn: systemd unit

systemd is not a supported target: the deliverable is the executable, and the environment it runs in may
not have systemd (decided 2026-10-20). The unit-specific problem, `/etc/uevtd` staying root-owned under
`ConfigurationDirectory=`, no longer applies. Two points carry over to any way of starting the daemon:

- `config::save` ([src/config.rs:437](src/config.rs#L437)) writes a temp file next to the config file and
  renames it, so the config file's directory must be writable by the daemon's user. A missing config file
  is created there on first start for the same reason. The README should state this.
- On a fatal error (writer thread panic, socket bind failure at startup) the daemon exits with status 1
  and relies on something to restart it ([ARCHITECTURE.md:500](ARCHITECTURE.md#L500)). Without a
  supervisor, forwarding stops until someone intervenes (Q7).

[contrib/uevtd.service](contrib/uevtd.service) and its mention in [README.md:20](README.md#L20) should then
be removed or marked as an unsupported example.

### H2. HTTP treats configuration and auth failures as per-message rejections

Where: [`transient`](src/outbound/http.rs#L161), [src/outbound/http.rs:136-143](src/outbound/http.rs#L136).

Only 5xx, 408 and 429 are retried. Every other non-2xx status, including 401, 403, 404, 405 and 3xx
(redirects are disabled), is "rejected": logged, counted, and the cursor moves past the message. These
statuses are rarely about one message. They mean the token expired, the path moved, or the server
redirects http to https. For as long as that lasts, every message is consumed and dropped.

Evidence (p09): the endpoint answered 401 for 5 messages and then 200 for one more. Result:
`delivered=1 rejected=5`, and the 5 were never retried.

Impact: a routine token rotation discards the backlog that built up during the outage, although the data
is still on disk.

Fix: treat only payload-specific statuses as per-message rejections (400, 413, 415, 422). Retry everything
else with backoff, flag it clearly in status, and make both lists configurable. Optionally keep rejected
sequence numbers replayable while they are retained, instead of skipping them for good. This also
settles ARCHITECTURE §12.4.

### M1. Switching `sync_interval_ms` to 0 strands the last batch

Where: [src/storage/mod.rs:628-660](src/storage/mod.rs#L628).

`run_writer` reads the interval once per loop iteration. When a message arrives while the writer is
waiting on the sync timer, its batch is appended with `sync = false` based on the old interval. The next
iteration sees interval 0 and calls `blocking_recv()` without a timer, even though data is unsynced. That
batch stays uncommitted (not forwarded, not queryable, not durable) until another message arrives or the
daemon stops.

Evidence (p01): with the interval at 1000 ms, send one message, switch to 0, then send a second message.
The committed head was still 0 after 3 s and became 2 only when the writer exited.

Impact: with sparse traffic the delay is unbounded, and a crash in that window loses records the daemon
held for hours.

Fix: at the top of the loop, sync when `writer.unsynced() && interval.is_zero()`, or decide `sync` from a
fresh read of the interval after the batch is collected. Add a test that changes the limits while the
writer is waiting.

### M2. One large message can evict the entire retained log

Where: [src/config.rs:253](src/config.rs#L253) and [264-269](src/config.rs#L264) (independent bounds),
[src/storage/mod.rs:420-428](src/storage/mod.rs#L420) (rotation only after the limit is passed),
`Storage::evict`.

Validation caps `max_message_bytes` at 64 MiB but never compares it with `segment_bytes` (at least
64 KiB) or `max_bytes`. A segment rotates only after it has passed `segment_bytes`, so a large message (or
a 1 MiB batch) lands in the active segment in full. `evict` then removes sealed segments until the total
fits or only the active segment is left.

Evidence (p02): a config with `max_message_bytes` = 64 MiB, `segment_bytes` = 64 KiB and `max_bytes` =
128 KiB passes validation. After one 8 MiB message: segments went from 2 to 1, `first_seq` from 1 to 65,
and 8.4 MB were retained against a 128 KiB limit.

Impact: lagging channels lose their backlog (counted as lost) because of a single valid message, and the
size limit does not hold. With the defaults (1 MiB messages, 8 MiB segments, 256 MiB log) a segment can
overshoot by one message plus one batch, which is harmless; the finding matters for small custom limits,
and the validation gap is what lets such a configuration through.

Fix: validate `max_message_bytes + 24 <= segment_bytes` (or a fraction of it). Rotate *before* a write that
would cross `segment_bytes`, so only a single oversized message can overshoot.

### M3. PATCH is a lost-update race

Where: [src/api.rs:183-190](src/api.rs#L183).

The handler reads `current()`, then merges and validates outside the store lock. `Store::update` takes the
lock only to compare and save. Two concurrent PATCHes start from the same base, and the second overwrites
the first.

Evidence (p07): 20 rounds of two concurrent PATCHes on different fields (`storage.sync_interval_ms` and
`inbound.max_connections`). Both returned 200 every time, and in all 20 rounds only one change survived.

Impact: configuration changes are lost silently whenever two clients write, for example a fleet agent and
a local UI, or a script.

Fix: do read, merge, validate and save inside the store lock (`Store::patch(&Value)`). For optimistic
concurrency, add a revision: `ETag` on GET, `If-Match` on PUT/PATCH, and 412 on a mismatch.

### M4. Memory is bounded by message counts, not bytes

Each bound is a number of messages, and each message can be up to `max_message_bytes`, which defaults to
1 MiB ([src/config.rs:40](src/config.rs#L40)), about 2000 times the expected 0.5 KiB.

- Writer queue ([src/daemon.rs:19](src/daemon.rs#L19)): 8 192 × 1 MiB ≈ 8 GiB before backpressure starts.
- `/v1/messages` ([src/api.rs:33](src/api.rs#L33), [src/storage/mod.rs:261](src/storage/mod.rs#L261)): up
  to 10 000 records with no byte cap. A page can therefore contain the whole retained log (256 MiB by
  default), held once as records and again as serialized JSON, and more if non-JSON payloads need escaping.
- Framers: up to `max_connections` × (`max_message_bytes` + 1).

With the expected ~0.5 KiB messages this is a few MiB. The risk is the worst case on a small device, where
the result is an OOM kill.

Fix: give the queue a byte budget (a semaphore acquired with `payload.len()` permits and released by the
writer). Cap a query page by bytes as well. In that case `next` must be set when the page is cut by size;
the current `records.len() == limit` test ([src/api.rs:236](src/api.rs#L236)) would be wrong, so `query`
should report whether it stopped early. Lower the default `max_message_bytes` to match the workload, for
example 64 KiB.

### M5. MQTT backoff never grows when the broker accepts and then drops the connection

Where: [src/outbound/mqtt.rs:67-72](src/outbound/mqtt.rs#L67) (`attempt = 0` on CONNACK) and
[87-96](src/outbound/mqtt.rs#L87).

The reconnect delay is `backoff(attempt)`, and `attempt` is reset on every successful CONNACK. If the
broker accepts the connection and then disconnects in response to a publish, the cycle repeats every
second without end: connect, CONNACK (attempt = 0), republish, disconnect, wait 1 s. Causes include a
payload above the broker's own limit (README mentions this), an ACL denial (AWS IoT Core disconnects), or
a rejected topic. The channel never advances, each cycle costs a TCP and TLS handshake, and the README's
"1 s to 60 s" backoff never happens.

Fix: reset `attempt` only when the cursor advances (an acknowledgement arrives). Count consecutive session
failures with the same head-of-line entry; after K of them, either reject it (count and skip, as for
oversized payloads) or park the channel with an explicit status. MQTT 5 reason codes on PUBACK would make
this precise (D5).

### M6. Dead channel tasks are not supervised

Where: [src/outbound/mod.rs:74-81](src/outbound/mod.rs#L74) and [94-105](src/outbound/mod.rs#L94).

`reconcile` checks `task.is_finished()` only when it runs, and it runs only on a configuration change.
Channel loops end only on cancellation, so the trigger is a panic in reqwest, rumqttc or this code. After
that the channel delivers nothing, and status keeps showing `running: true`, because only `stop` clears
the flag.

Fix: make the supervisor also wait on task completion (a JoinSet, or select over the handles). On exit,
set `running = false`, record the error, and restart with backoff.

### M7. Recovery: visible before durable, and an I/O error blocks startup

Where: [src/storage/mod.rs:149-169](src/storage/mod.rs#L149) and [500-521](src/storage/mod.rs#L500).

- After a daemon crash without an OS crash, `Storage::open` scans the active segment and treats every
  CRC-valid record as committed. Some of those records may exist only in the page cache, if the crash came
  between write and sync. They are published to readers at once, but reach disk only with the next synced
  batch or kernel writeback (typically within ~30 s). A power loss in that window can lose records that
  were already delivered, and their sequence numbers would be reused, which is exactly what invariant 4
  rules out. Fix: call `sync_data` on the active segment (and sync the directory) in `open` before
  publishing the head.
- `Storage::open` fails on any segment file it cannot open or stat ([lines 112, 130-131](src/storage/mod.rs#L130))
  and on a read error while scanning the active segment, such as an unreadable sector, through
  `scan(...)?` ([line 150](src/storage/mod.rs#L150)). The daemon then cannot start until someone removes
  the file. Consider handling a read error like corruption: keep the valid prefix as a sealed segment,
  start a new active segment, and log and count it.

### M8. Eviction loss is reported only when the channel reads again

Where: [src/outbound/mod.rs:246-253](src/outbound/mod.rs#L246) (the only place `lost` is counted),
[src/outbound/mqtt.rs:107](src/outbound/mqtt.rs#L107), [src/outbound/http.rs:103](src/outbound/http.rs#L103).

A channel that falls behind leaves its backlog in the log on disk (D11). When the log exceeds
`storage.max_bytes`, the oldest segments are evicted whether or not every channel has delivered them. A
channel notices only when its reader next touches the log: only then are the skipped records added to
`lost` and logged ("records were evicted before delivery", with a count but no sequence range). A channel
that cannot deliver does not read: MQTT reads only while connected and below `inflight`, and HTTP keeps
retrying the same record inside `deliver`.

Evidence:

- p10: MQTT channel with the broker unreachable, 5 000 messages of ~1 KiB into a 2 MiB log:
  `oldest_retained_seq=3076 cursor=1 lag=5000 lost=0`. 3 075 undelivered messages are already gone, and
  nothing reports it.
- p11: HTTP endpoint answering 503, then 200. During the outage the picture is the same (`lost=0`). After
  recovery: `lost=3072 delivered=1928`. The loss appears only once the endpoint is back.

Impact: data loss during an outage is invisible, and if the outage never ends (a misconfigured channel) it
is never reported. `lag` also counts records that no longer exist, and `storage_evictions_total` counts
segments rather than messages and is not per channel.

Fix: account for the loss at eviction time instead of read time. For example, channel readers register
their position with `Storage`, so `evict` can log per channel which sequence range it removed before
delivery and count it once; the reader then skips without counting again. A smaller first step is a
per-channel gauge of evicted-but-undelivered records (the oldest retained sequence minus the reader's
position) in status and metrics.

### M9. A read error drops the records already read in that batch

Where: [src/storage/mod.rs:719-751](src/storage/mod.rs#L719) (`Reader::read`),
[src/outbound/mod.rs:242-263](src/outbound/mod.rs#L242) (`Feed::next`).

`Reader::read` advances `next_seq` for every record it decodes and then returns `Err` as soon as
`locate` or the segment cursor fails with an I/O error other than `NotFound`. The records collected so
far go away with the error, but the reader keeps its advanced position. `Feed::next` logs "log read
failed; retrying", stores the reader as it is, and retries from that position one second later. The
records read before the error are therefore never delivered and never counted. Corruption takes a
different path (`skipped`), so only genuine I/O errors do this: EIO on a bad sector, EACCES after a
permission change, or EMFILE/ENFILE when the process runs out of file descriptors.

Evidence (p12): a log of 3 000 records with the channel's cursor at 1, and the sixth segment file made
unreadable (mode 000) while the channel was delivering. The channel stopped at `cursor=257` with
`last_error="log read: Permission denied"` and `lost=0`. After the mode was restored it resumed at 316,
the first record of the unreadable segment: `delivered=2941` of 3 000, `lost=0`. The 59 records 257–315
were read before the error, dropped with the failed batch, and skipped for good.

Two further effects, both visible in the same probe:

- With a persistent error the channel stalls indefinitely, retrying every second with no skip (unlike
  corruption, L3) and no counter; only `last_error` and `lag` show it, while `running` stays true.
- `last_error` is never cleared by a successful read, so the error text outlives the fault.

Fix: in `Reader::read`, when an error occurs after records were collected, return the partial batch and
let the error surface on the next call, which starts at the failing position with nothing to lose;
alternatively keep a committed position and roll `next_seq` back on error. Clear `last_error` on the
next successful read. For a persistent error, decide between staying stalled (safe, needs a status flag
and a counter) and skipping the segment after a bounded number of retries, as for corruption.

### Low

- **L1. Validation overflow** ([src/config.rs:267](src/config.rs#L267)). `2 * storage.segment_bytes`
  overflows for values ≥ 2^63. In a debug build p03 panics in `validate` ("attempt to multiply with
  overflow"); in the daemon that would be a 500 from the API (the panic is caught by `spawn_blocking`) or
  an abort at startup. In a release build p03 accepts `segment_bytes = 2^63, max_bytes = 1`: the active
  segment never rotates and eviction keeps only that segment, so the log is unbounded. Use `checked_mul`
  and set sane upper bounds.
- **L2. `sync_interval_ms`** ([src/storage/mod.rs:184](src/storage/mod.rs#L184),
  [197](src/storage/mod.rs#L197)). There is no upper bound, and `as_micros() as u64` truncates. In p04,
  18 446 744 073 709 552 ms is applied as 384 µs. A merely large value, such as one hour, is honored and
  delays forwarding by up to that long. Cap it (for example at 60 000) and convert with `try_from`.
- **L3. Corruption granularity** ([src/storage/mod.rs:754-762](src/storage/mod.rs#L754)). On a CRC
  mismatch the reader resumes at the next segment, or at head + 1 in the active segment. In p05, one
  flipped payload byte in seq 3 made the reader return `[1, 2]` and skip 8 records, although 4..=10 were
  intact. The next appended record was read normally. The skip is deliberate (§5.6), but the frame header
  is usually intact. Following the frame's own length, and accepting the next frame only if its CRC is
  valid and its seq is the expected one, would recover the rest with negligible risk of accepting garbage.
  Low priority, since bit rot is rare.
- **L4. Rebind retry** ([src/config.rs:516](src/config.rs#L516), [527](src/config.rs#L527)). After a failed
  socket rebind, the file holds the new path. Re-sending the same config, or a SIGHUP, does not republish
  (`*self.current() == config` returns early), so the bind is retried only when some other field changes.
  The PUT/PATCH that caused it returned 200 although the runtime kept the old socket. Options: make SIGHUP
  always publish, and/or report apply errors in the API response.
- **L5. `max_connections` changes** ([src/inbound.rs:87-89](src/inbound.rs#L87)). A new semaphore is
  created, and permits held by existing connections belong to the old one. After lowering the limit, up to
  old_active + new_max connections can be open. Adjust a single semaphore instead (`add_permits` /
  `forget_permits`).
- **L6. HTTP cursor saves** ([src/outbound/http.rs:61-68](src/outbound/http.rs#L61)). The cursor is flushed
  only after a whole batch of up to 64 records. If one record in the middle is retried for an hour, the
  progress before it is not persisted for that hour, despite §5.8's "at most once per second while it is
  advancing". Calling `cursor.flush(false)` after each record fixes it, since the call rate-limits itself.
- **L7. HTTP body drain** ([src/outbound/http.rs:123](src/outbound/http.rs#L123)). The `response.chunk()`
  loop is not raced against the cancellation token, so each stop can wait up to `timeout_ms`. Reconcile
  and shutdown also stop channels one by one.
- **L8. Observability inaccuracies.**
  - Failures in `open_active`/`create_segment` ([src/storage/mod.rs:427](src/storage/mod.rs#L427)) are not
    counted in `write_errors`.
  - Records dropped by a failed write, or discarded by `seal` ([line 596](src/storage/mod.rs#L596)), are
    only logged. There is no dropped-records counter.
  - `batches` is incremented on write ([line 452](src/storage/mod.rs#L452)) but exported as "Committed
    write batches" ([src/api.rs:411](src/api.rs#L411)).
  - `clock_steps` also counts ordinary reordering between connections, because each connection task stamps
    its messages ([src/inbound.rs:180](src/inbound.rs#L180)) before they are queued.
  - A channel that has been disabled since startup shows `cursor: 0` and `lag: head + 1`
    ([src/outbound/mod.rs:47](src/outbound/mod.rs#L47)).
  - HTTP `connected` is set only by deliveries, so an idle, healthy channel reports false.
  - `/v1/health` always returns 200, even while every write fails.
  - 405 and malformed-query errors come back as plain text instead of `{"error": …}`.
- **L9. File permissions** ([src/storage/mod.rs:549-552](src/storage/mod.rs#L549),
  [295](src/storage/mod.rs#L295)). Segment and cursor files get the umask inherited from whatever starts
  the daemon: 0644 with the common 0022, 0666 if it starts with umask 0. Any local user can then read, and
  with umask 0 modify, the stored messages. Create these files 0600 or 0640 explicitly, as `config::save`
  already does for the config file. The storage directories (`create_dir_all`) follow the umask too, and
  `uds::bind` ([src/uds.rs:45-46](src/uds.rs#L45)) binds first and applies the mode afterwards, so a
  socket is briefly open with the umask default before `chmod`.
- **L10. MQTT `client_id`** ([src/config.rs:146](src/config.rs#L146)). The default is "uevtd" for every
  channel. Two MQTT channels to the same broker, or two devices running the default config, take over each
  other's session in a reconnect loop. Validate uniqueness per (host, port) within a config, and derive
  the default from the host name and channel name.
- **L11. Unreadable cursor** ([src/outbound/mod.rs:190](src/outbound/mod.rs#L190)). The channel starts at
  the head, and the records between its old position and the head are skipped without being counted in
  `lost`. Starting at the oldest retained record (at-least-once), or at least counting the gap, is safer.
  Atomic rename makes this case unlikely.
- **L12. Smaller items.**
  - `load_or_init` ([src/config.rs:425](src/config.rs#L425)) uses `Path::exists`, which returns false on
    any error, such as EACCES on the directory. It can then try to write defaults over a file it simply
    could not stat. Match `NotFound` from the read instead.
  - `redacted` hides URL user info and auth-like headers, but not query-string secrets (`?token=…`). The
    URL also appears in reqwest error messages (logs and `last_error`); reqwest strips user info from it but
    not the query.
  - Cursor files of channels removed while the daemon was stopped are never deleted. Re-adding a channel
    with that name resumes from the stale cursor instead of the head.
  - `sync_dir` failures ([src/storage/mod.rs:354](src/storage/mod.rs#L354)) are logged at debug. A failed
    directory fsync after creating a segment deserves at least a warning.
  - A SIGHUP that arrives before `reload_on_hup` installs its handler ([src/daemon.rs:115](src/daemon.rs#L115))
    terminates the process, because that is the default disposition. A reload sent during startup kills
    the daemon.
  - `uds::bind` probes for a live listener with a blocking `connect` ([src/uds.rs:24](src/uds.rs#L24)) on
    an async worker. Against a hung process with a full backlog, it can block indefinitely.
  - `/v1/messages` renders the JSON string payload `"abc"` and the non-JSON payload `abc` the same way
    (`"data": "abc"`). A separate field or a flag for non-JSON payloads would make the output lossless.

## 4. Design questions and recommendations

These need your decision. They follow ARCHITECTURE §12 where one applies, ordered by impact.

**D1. HTTP throughput.** Each message is one request, and the next one starts only after the response, so
the rate is 1/RTT. Probe p08: 200 messages against a target with 20 ms of latency drained in 4.51 s, which
is 44 msg/s. At a WAN round trip of 100 ms that is ~10 msg/s, so a 60 s burst at the required 1 000 msg/s
takes about 100 minutes to forward. Data is lost only if bursts outpace the average capacity before
retention runs out; otherwise the cost is latency. Meanwhile the backlog waits on disk, not in memory
(D11). MQTT is pipelined and does not have this limit. Options:

- (a) Opt-in batching: several records per request, as NDJSON or a JSON array, bounded by
  `max_records`, `max_bytes` and `linger_ms`. This changes the receiver contract.
- (b) `concurrency: N`, with the cursor at the longest acknowledged prefix. The MQTT `Tracker` already does
  this bookkeeping. Arrival order at the receiver becomes relaxed.

Recommendation: (a) where receivers can accept it, (b) otherwise, and keep today's behavior as the default.
This revisits §12.3.

**D2. Timestamps.** Each record gets `max(receipt time, previous ts)`
([src/storage/mod.rs:430-440](src/storage/mod.rs#L430)). If the clock is ever set far ahead (an RTC fault,
a bad NTP or GPS source, or a manual mistake) and then corrected, every later record carries the bad time
until real time catches up. That can last years, and time queries cannot tell those records apart. Options:

- (a) Clamp only small backward steps, for example up to 5 s. On a larger step, keep the real time and
  start a new segment, so each segment stays monotonic internally. Segments are then no longer ordered by
  time, so the query's "last segment with `base_ts < from`" start and its "first `ts >= to`" stop are
  both wrong; (a) needs (b)'s per-segment range anyway.
- (b) Store the true receipt time, and keep `[min_ts, max_ts]` per segment in the index, rebuilt on open or
  kept in a small footer. Queries then visit only overlapping segments and filter, instead of stopping at
  the first `ts >= to`.

(b) is the more robust option and drops the dependence on invariant 3; (a) only decides how much
clamping remains. The cost is a few more segment scans per query, and queries are rare. This revisits
§12.7 and depends on question Q5.

**D3. Where the guarantee starts.** Inbound has no acknowledgement. Up to 8 192 queued messages plus socket
buffers are lost on a crash, and a write failure drops a batch that the producer believes was delivered.
If any producer needs end-to-end at-least-once, an optional ack mode closes the gap: reply `ok <seq>\n` once
the message is committed, or use SOCK_SEQPACKET with a reply. Otherwise, document that the guarantees
start at commit.

**D4. An idempotency key for consumers.** At-least-once delivery creates duplicates (after a crash, a
reconnect, or a channel change), and consumers can only remove them if the payload carries its own id.
Sending `X-Uevtd-Seq` costs little. Add a log id generated when the log is created, so that sequence
numbers from a wiped log are not confused with the old ones. MQTT 3.1.1 has nowhere to put the key; v5
user properties would. This refines §12.2.

**D5. MQTT connection options.** Add a custom CA and client certificate (mTLS). Managed brokers often
expect client certificates: AWS IoT Core's standard MQTT endpoint authenticates with X.509 client
certificates, and Azure IoT Hub supports X.509. Private brokers commonly use a private CA. Supporting v5 would
add PUBACK reason codes, which let M5 tell a rejection from a retryable failure, and user properties
(D4). This revisits §12.10.

**D6. Metrics for performance analysis** (TODO constraint 5). Worth adding:

- queue depth and a dropped-records counter;
- fsync latency and batch size (a sum and a count are enough);
- per-channel delivery latency (commit to ack) and the age of the oldest undelivered record, i.e. lag in
  time and not only in records;
- the retention horizon in time (the oldest ts);
- a health check that reflects writer errors.

**D7. Cursor persistence and flash wear.** Each cursor save is create, write, fsync, rename and directory
fsync, once per second per busy channel. On ext4 that is up to two journal commits per second per
channel, which under sparse traffic can rival the log's own writes (a rough estimate, not measured). This
matters if the targets use eMMC or SD storage. Options: make the save interval configurable, or update a
fixed-size cursor file in place
with two CRC-protected slots and `fdatasync`, which changes no metadata per save.

**D8. Channel reconfiguration.** Restarting a channel abandons its in-flight request, so every change to a
busy channel causes a duplicate. §6.4 and §11 contradict each other on this point (§5 below). For
reconfiguration (not shutdown), let the in-flight delivery finish within a short grace period. Fields such
as headers and timeout could also be applied without a restart.

**D9. Framing contract.** NDJSON is a sound choice. The producer contract should say *compact* JSON, one
message per line: pretty-printed JSON is split into fragments, which are stored and forwarded as separate
messages. SOCK_SEQPACKET is the alternative that makes truncation impossible and keeps message boundaries
by construction (§12.1).

**D10. Secrets in `GET /v1/config`** (§12.8). If redaction is wanted later, return `"***"` and treat
`"***"` in PUT/PATCH as "keep the current value", so that a round trip does not wipe the secrets.

**D11. Backlog when a channel falls behind** (raised 2026-10-20). The backlog is the log on disk, shared
by all channels; a channel is only a cursor into it. A channel's memory does not depend on how far behind
it is:

- MQTT holds at most `inflight` records (default 32) for republishing, a second copy of the same
  in-flight publishes inside rumqttc, and one read batch (at most the free window, capped at 4 MiB). While
  disconnected or at the window limit it reads nothing ([src/outbound/mqtt.rs:107](src/outbound/mqtt.rs#L107)).
- HTTP holds its current batch: at most 64 records, or about 4 MiB (the byte limit plus one record).

So a limit on the backlog already exists: `storage.max_bytes` on disk. Exceeding it drops the oldest
messages for every channel that has not delivered them yet. What is missing is reporting at the time of
the drop (M8). Options:

- (a) Keep the shared limit and report drops when they happen (M8). Recommended in any case.
- (b) Add a per-channel cap, `max_lag` (records) or `max_age` (time). A channel that falls further behind
  skips ahead to recent data, counts the skipped records as dropped and logs the range. This bounds
  staleness and drain time (relevant to D1), not memory or disk, since the log is shared.
- (c) Bound MQTT memory by bytes instead of count: a byte cap on in-flight payloads, or validating
  `inflight × max_message_bytes` against a budget. The defaults allow 32 × 1 MiB (M4).

The remaining §12 decisions look right as they are: new channels start at the head (§12.5, perhaps with an
optional `start: "oldest"`), `storage.path` and `log.format` need a restart (§12.6), and the config file is
JSON (§12.9).

Questions that decide D1–D11:

- **Q1.** Can the HTTP receivers accept batched bodies, or is one message per request part of the
  contract?
- **Q2.** Which HTTP statuses should drop a message, and should dropped messages be replayable (H2)?
- **Q3.** Do any producers need to know that a message was persisted (D3)?
- **Q4.** What RAM, disk and storage type do the target devices have? This sets defaults for
  `max_message_bytes`, the queue budget and the cursor interval (M4, D7).
- **Q5.** How reliable are the device clocks (RTC, NTP, GPS)? This decides D2.
- **Q6.** Which MQTT brokers are targeted, and do they need mTLS or a custom CA (D5)?
- **Q7.** Will a process supervisor restart the daemon after a fatal exit? If not, startup failures and a
  dead writer thread need handling inside the daemon (H1, M6).
- **Q8.** When a channel cannot keep up, should it keep everything until the shared log is full and only
  then lose the oldest (today), or should each channel have its own cap by records or age (D11)?

### Decisions (2026-10-21)

| Q | Decision | Consequence for the findings |
|---|----------|------------------------------|
| Q1 | Batched HTTP bodies are acceptable. | D1 (a) is available. If a WebSocket channel is adopted (D12), batching is unnecessary and can be deferred. |
| Q2 | A dropped message produces a *drop notice*: a special JSON message published on the channel that dropped it. Notices are per channel. The dropped message stays in the log and remains queryable. | H2: only payload-specific statuses drop (400, 413, 415, 422); 401, 403, 404, 405, 3xx, 501 and 505 block the channel with backoff, since nothing about one message causes them. MQTT oversized payloads drop with a notice. M8/Q8: evicted-before-delivery records are counted and logged per channel at eviction time, and the channel sends one notice with the lost range when it next delivers. To make "which messages did channel X drop" queryable, a per-channel drop ledger (`drops/<channel>`: seq, ts, reason; pruned with the log horizon) and an API to list it, with the payload fetched by seq from the log. To confirm: ledger versus appending the notice to the main log, which would forward it on every other channel and consume sequence numbers. |
| Q3 | No producer acknowledgement. | D3: document that the guarantees start at commit; nothing to build. |
| Q4 | RAM below 128 MB; local storage generally below 2 GB. | M4 becomes a hard requirement: every buffer gets a byte bound independent of `max_message_bytes`. Proposed budget: writer queue 16 MiB (semaphore in bytes), query page 4 MiB, delivery batch 1 MiB, MQTT in-flight payloads 4 MiB, framers `max_connections × max_message_bytes` validated ≤ 32 MiB, default `max_message_bytes` 64 KiB (128× the typical 0.5 KiB). Worst case then stays near 50 MB including the runtime. L1/L2 upper bounds: `segment_bytes` ≤ 256 MiB, `max_bytes` ≤ 2^40, `sync_interval_ms` ≤ 60 000. `storage.max_bytes` default 256 MiB stands. |
| Q5 | Clocks are generally correct and synced by a system service; while a clock is wrong, connections must back off. | TLS failures caused by a wrong clock already retry with growing backoff: HTTP up to 30 s, MQTT up to 60 s (no CONNACK, so M5 does not apply). Document it. D2 is deferred: keep clamping, keep the `clock_steps` warning, revisit if devices show large backward steps. The residual risk is a clock far ahead at boot and then stepped back, which stamps later records with the future time until real time catches up. |
| Q6 | Public brokers; a custom CA is required. | D5: add `ca_file` (PEM) to the TLS options, trusted instead of the platform store when set; share one TLS builder across MQTT, HTTP and WebSocket. Client certificates and MQTT 5 are not needed. |
| Q7 | A supervisor restarts the daemon; keep it simple. | H1: exit on fatal error stays. Remove `contrib/uevtd.service` and the README line, and document two requirements: a supervisor that restarts on a non-zero exit, and a config directory writable by the daemon. M6 (restarting a dead channel task in-process) is still worth doing, since a channel panic does not exit the daemon. |
| Q8 | Lose the oldest. | D11 (a): shared log limit, oldest segments evicted, loss accounted at eviction time; no per-channel cap (D11 b dropped). |

**D12. WebSocket as the outbound transport instead of HTTP** (raised 2026-10-21).

What it fixes: D1 entirely (one long-lived connection, frames pipelined up to a window, throughput about
window/RTT instead of 1/RTT), H2's status ambiguity (a rejection becomes an explicit in-band message) and
D4 (an envelope carrying `seq` gives consumers an idempotency key). Structurally the channel is the MQTT
channel without a broker: connect with backoff, send, track outstanding frames, cursor = longest
acknowledged prefix, resend unacknowledged frames after a reconnect. `Tracker`, `Feed`, `CursorStore` and
the reconnect loop exist already.

What it costs:

- An application-level acknowledgement. WebSocket confirms nothing above TCP; "written to the socket" is
  MQTT QoS 0, and a dropped connection silently loses whatever was in flight. For at-least-once the
  receiver must answer, for example `{"ack": n}` meaning everything up to `n` is processed, and
  `{"nack": n, "reason": "..."}` for a message it will never accept. That contract is larger than
  accepting batched bodies, and it must be implemented by every receiver.
- A WebSocket server at the receiver. Plain HTTP collectors, serverless endpoints and some gateways
  cannot be targeted, and idle long-lived connections need ping/pong and reconnects, which is the same
  machinery MQTT has.
- One dependency, `tokio-tungstenite` with rustls, sharing the TLS configuration (Q6) with MQTT. If
  `http` is dropped, `reqwest` leaves the binary (it stays a dev-dependency for the tests), so the count is
  about unchanged.

Protocol sketch, to confirm: daemon → receiver, one text frame per record, `{"seq": n, "ts": us,
"data": <payload>}`, the same shape as `/v1/messages`; drop notices (Q2) as `{"notice": {...}}` frames.
Receiver → daemon: `{"ack": n}` cumulative, `{"nack": n, "reason": "..."}` per message. At most
`inflight` unacknowledged frames, so a slow receiver throttles the stream by delaying acks; a ping every
`keep_alive_s`; no ack within `timeout_ms` counts as a dead connection. Handshake headers carry auth
(`headers`); the URL is `ws://` or `wss://`.

Recommendation: adopt WebSocket for receivers under your control, as a new channel type `websocket`
beside `http`. Keep `http` with its one-request-per-message contract for third-party endpoints, with only
the status policy fix (H2), and drop batching (Q1) unless an HTTP endpoint has to carry bursts. If every
receiver will be yours, replace `http` outright: simpler, one fewer dependency.

**Decision (2026-10-21): the HTTP channel is replaced by a WebSocket channel.** What that means:

- Removed: `src/outbound/http.rs`, the `Http` config struct and `type: "http"`, `reqwest` from the
  runtime dependencies (it stays a dev-dependency as the API client in the tests), the HTTP sections of
  README and ARCHITECTURE (§6.2, §12.3, §12.4), and batching (Q1 is moot). A config file with a
  `type: "http"` channel stops validating ("unknown variant `http`"), which is acceptable at 0.1.0 and
  must be stated in the changelog.
- Added: `src/outbound/websocket.rs`; a small `src/tls.rs` building one rustls `ClientConfig` for MQTT
  and WebSocket (platform verifier, or only `ca_file` when set, Q6); the `/v1/messages` record
  serialization moved out of `api.rs` into a shared module so the frame and the API use one shape;
  dependencies `tokio-tungstenite` (rustls), which is new, plus direct use of `url` and `futures-util`,
  which are already in the tree; `reqwest` leaves, so the dependency count is about unchanged.
- Superseded findings: H2 (a rejection is now an explicit in-band message; an auth failure is a failed
  handshake, which blocks with backoff and drops nothing), D1 (pipelined), D4 (the envelope carries
  `seq`), L6 and L7 (HTTP-specific), the HTTP item of L8, and probes p08, p09, p11 become history.
  M5's backoff rule applies to both remaining channels.

Channel definition, `type: "websocket"`:

| Field | Default | Meaning |
|-------|---------|---------|
| `url` | required | `ws://` or `wss://`; TLS follows the scheme |
| `headers` | `{}` | sent with the handshake (auth tokens live here) |
| `ca_file` | `null` | PEM bundle trusted instead of the platform store |
| `timeout_ms` | 10 000 | handshake, and the wait for a pong after a ping |
| `keep_alive_s` | 30 | ping interval while connected |
| `inflight` | 256 | maximum unacknowledged frames |
| `max_frame_bytes` | 1 MiB | larger payloads are rejected with a notice instead of sent, as MQTT's `max_packet_bytes`; keeps a receiver's frame limit from turning into a reconnect loop |

Protocol:

- Daemon → receiver: one text frame per record, `{"seq": n, "ts": us, "data": <payload>}`, `data`
  embedded verbatim when it is valid JSON and as a string otherwise, exactly as `/v1/messages`. Notices
  are text frames `{"notice": {"event": "rejected", "seq": n, "reason": "..."}}` and
  `{"notice": {"event": "lost", "from_seq": a, "to_seq": b, "count": c}}` (evicted before delivery, Q8),
  sent as soon as the condition is known, not acknowledged, not tracked.
- Receiver → daemon: `{"ack": n}`, cumulative: every frame with `seq <= n` is processed;
  `{"nack": n, "reason": "..."}`: frame `n` will never be accepted. Any other frame is ignored at debug
  level. Binary frames are ignored. A close frame or a read error ends the session.
- Window: at most `inflight` unacknowledged frames; a slow receiver throttles the stream by delaying
  acks, and the backlog stays on disk. The cursor is the end of the longest acked-or-nacked prefix, so
  a nack advances past the message; it is counted in `rejected`, a notice follows, and the record stays
  in the log for queries.
- Session: handshake within `timeout_ms`; a ping every `keep_alive_s`, and no pong within `timeout_ms`
  ends the session. After a session ends, every unacknowledged frame is resent in order on the next
  session, so duplicates are possible and receivers deduplicate by `seq` (at-least-once, as before).
  Reconnect backoff 1 s to 60 s, doubling, reset only when an ack advances the cursor (M5, also for
  MQTT). On shutdown or reconfiguration the channel sends a close frame and persists its cursor.
- Structure: the socket is split; a writer task owns the sink and drains an `mpsc` of outgoing frames,
  so cancelling the channel task's `select!` can never lose a half-sent frame; the channel task selects
  over incoming frames, `Feed::next` (only while connected and below the window), the ping and cursor
  timers, and cancellation. This mirrors the MQTT channel's client/event-loop split.
- Status and metrics: `connected` reflects a live session; `delivered` counts acks, `rejected` counts
  nacks and oversized payloads, `retries` counts failed connection attempts (same meaning for MQTT).
- Tests: the end-to-end tests get a scripted WebSocket receiver (`tokio-tungstenite::accept_async` on a
  local port) that acks, nacks and drops the connection on cue, which also covers reconnect and resend.

**Revised order.** (1) Local fixes: M1, M3, M9, L1, L2, L4, L11, L12. (2) Memory budget per Q4 (M4) and
the new defaults. (3) Eviction-time accounting (M8) and the drop-notice mechanism (Q2), shared by both
channels. (4) The WebSocket channel replacing HTTP (D12), together with the backoff rule (M5),
`ca_file` (Q6) and the shared TLS builder. (5) MQTT: `client_id` default (L10); the
`max_packet_bytes` notice came with step 3. (6) Supervision (M6), recovery and read-error hardening
(M7). (7) Observability (L8, D6) and
docs (§5, H1, D3, D9, the changelog note on `type: "http"`). The drop ledger (Q2) is last and still to
be confirmed.

## 5. Documentation mismatches

- [ARCHITECTURE.md:96-98](ARCHITECTURE.md#L96): "serialised JSON never contains a raw newline" is true
  only for compact serialization (D9).
- [ARCHITECTURE.md:210-217](ARCHITECTURE.md#L210): when the rollback truncation fails, `seal` does not
  truncate the file, so the partial batch stays on disk. This is harmless (§6), but the text claims the
  file is truncated to its committed length.
- [ARCHITECTURE.md:221-227](ARCHITECTURE.md#L221): retained bytes are not within
  `[max_bytes - segment_bytes, max_bytes]` when a segment overshoots, which a batch of up to ~2 MiB or a
  large message can cause (M2). "A 1 000 msg/s burst can run for eight minutes before evicting" holds only
  for an empty log; in steady state, eviction starts immediately.
- [ARCHITECTURE.md:274-282](ARCHITECTURE.md#L274): "at most once per second while it is advancing" is not
  true for HTTP during a batch (L6).
- [ARCHITECTURE.md:350-352](ARCHITECTURE.md#L350) vs [518-521](ARCHITECTURE.md#L518): §6.4 says stopped
  channels "finish the message in flight", while §11 and the code abandon it (D8).
- [ARCHITECTURE.md:413-419](ARCHITECTURE.md#L413): the validation list omits the `max_message_bytes` range
  (1..=64 MiB) that the code enforces.
- [ARCHITECTURE.md:502](ARCHITECTURE.md#L502): "Channel loses the evicted messages, logs the count" happens
  only when the channel reads again, not when the target is down (M8).
- [README.md:20](README.md#L20) points to the systemd unit, and
  [ARCHITECTURE.md:500](ARCHITECTURE.md#L500) assumes a service manager restarts the daemon; both depend
  on the deployment decision (H1, Q7).
- [README.md:104-109](README.md#L104): "reconnects with backoff (1 s to 60 s)" stays at 1 s when the broker
  accepts the connection (M5).

## 6. Verified correct

These were checked because they are the boundaries a bug would most likely hide behind.

- Readers never read past the committed length, including after a rollback or a failed sync, because the
  truncation target is always at or above the committed length.
- Readers never cross into a newer segment's sequence range: `locate` runs for every record and switches
  segments as soon as `next_seq` reaches the next segment's first sequence. Probe p06 built the worst case
  the write-failure path can leave behind: a sealed segment that keeps an uncommitted tail with sequences
  4–5, and a next segment that reuses 4–5. Both a reader starting at seq 1 and a query returned the real
  records (`4=real-4, 5=real-5, 6=real-6`). This disproved a suspected overlap bug.
- `create_segment` replaces only a segment without committed records. No reader can have such a segment
  open, because `locate` requires `next_seq <= head`.
- Eviction while a reader has a segment open is safe (the fd stays valid), and a `NotFound` on open is
  retried.
- `Feed::next` is cancel-safe: a pending blocking read survives the future being dropped.
- MQTT packet ids cannot collide: outstanding entries never exceed `inflight` (counted by prefix), and the
  library window is `2·inflight + 1`. rumqttc reports a refused CONNACK as an error, emits `Incoming` before
  re-publishing after a collision, and checks the full packet size; the `+16` margin covers the fixed
  header (≤ 5 bytes), the topic length (2) and the pkid (2). `keep_alive_s >= 1` satisfies rumqttc's
  assertion.
- reqwest strips URL user info before sending, so credentials in the URL do not appear in its error text.
- Group commit, the sync timer and the final sync on shutdown behave as designed, apart from M1. The
  timer-panic fix in d4fe3c1 holds.
- Socket takeover protection, stale-socket cleanup, RFC 7386 merge semantics and the atomic config save
  are correct.
- [contrib/config.example.json](contrib/config.example.json) loads and validates against the current
  schema (p13), so `deny_unknown_fields` has not drifted from the example.
- rumqttc 0.25.1's option setters assert only keep-alive ≥ 1 s (or zero) and in-flight ≠ 0, both
  guaranteed by validation; `MqttOptions::new` accepts any client id.

## 7. Test gaps

- MQTT has no automated end-to-end test in the repo: there are only `Tracker` unit tests, and the scripts
  live on the dev box. A small fake broker inside the tests (CONNECT/CONNACK, PUBACK, scripted disconnects)
  would cover reconnect, republish, M5 and packet-id handling. Step 3 added the broker without scripted
  disconnects; those remain for M5.
- The writer has no fault injection. A small tmpfs on the dev container gives a real ENOSPC, and a seam
  around the segment file could cover EIO on write and sync.
- Not covered: live limit changes (which would have caught M1), concurrent API writes (M3), the rebind
  paths, large queries, supervisor restart (M6), corrupt cursor files, recovery with read errors, and
  read errors during delivery (M9; p12 shows how to provoke one with a mode change).

The order of work, updated with the decisions of 2026-10-21, is at the end of §4.

## 8. Progress

**Step 1, local fixes (2026-10-21).** Fixed, each with a regression test: M1 (the writer re-reads the
interval at least once a second while data is pending, so setting it to zero commits within a second),
M3 (`Store::patch` reads, merges, validates and saves under one lock), M9 (`Reader::read` returns the
records read before an I/O error and reports the error on the next call; `last_error` is cleared once
reads succeed again), L1 and L2 (`checked_mul`, bounds: `segment_bytes` 64 KiB..=256 MiB, `max_bytes`
≤ 1 TiB, `sync_interval_ms` ≤ 60 000, saturating microsecond conversion), L4 (an unchanged
configuration is republished by PUT, PATCH and SIGHUP, so a failed rebind is retried), L5 (the
connection limit is a single semaphore resized in place), L9 (segment and cursor files 0640,
directories 0750), L11 (an unreadable cursor replays from the oldest retained record), and the L12
items: `load_or_init` matches `NotFound` instead of `exists`, query-string secrets are redacted, stale
and temporary cursor files are pruned at startup, directory-sync failures are warnings, the SIGHUP
handler is installed before anything else, the socket-liveness probe is asynchronous with a one-second
bound, and `/v1/messages` returns non-JSON payloads as `text` instead of `data`.

Residual, by design: a channel facing a persistent read error stays at the failing position and
reports it in `last_error` (no skip, M9); the socket is briefly world-connectable between `bind` and
`chmod` (L9).

**Step 2, memory budget (2026-10-21).** Fixed M4 and M2. Every buffer that holds messages has a byte
bound, and validation bounds the sizes and counts they derive from:

- Writer queue: 8 192 messages and 8 MiB of payload, counted from reservation until written. A
  connection copies a complete message out of its buffer only once the queue has room, so it never
  holds a message twice while it waits.
- Inbound: `max_message_bytes` ≤ 256 KiB (default 64 KiB), `max_connections` ≤ 256, product ≤ 16 MiB;
  the framer's buffer never grows beyond one message plus one 8 KiB read.
- Segments: `segment_bytes ≥ max_message_bytes + 48`, a batch never exceeds an empty segment, and a
  batch that would cross `segment_bytes` starts a new segment, so segments stay within the limit (M2).
- Channels: at most 8; 1 MiB of payload being delivered per channel (HTTP batch, MQTT in flight),
  MQTT `inflight` ≤ 1024; an acknowledged MQTT payload is released at once.
- Queries: one at a time, pages of 1 MiB of payload; `next` is now `null` on the last page. API
  request bodies ≤ 256 KiB.
- On glibc the binary pins the mmap threshold at 128 KiB and uses two arenas.

These refine the budget proposed under Q4 (queue 16 MiB, MQTT in flight 4 MiB): the MQTT library keeps
its own copy of every in-flight publish, and the measured peaks below left no reason for more.

Measured on the dev container (Linux, glibc, 7 cores), release binary, with a load probe that drives 48
connections sending maximum-size messages flat out, 16 connections each holding a partial message,
queries in a loop, MQTT channels to a local mosquitto and HTTP targets that answer after 2 s:

| Configuration | Peak RSS before step 2 | Peak RSS after |
|---------------|------------------------|----------------|
| Idle, two channels | 6.1 MiB | 6.0 MiB |
| Defaults, one MQTT and one slow HTTP channel | 56.9 MiB | 30.5 MiB |
| Every limit at its maximum, four MQTT and four slow HTTP channels | 144.1 MiB | 53.8 MiB |

The allocator setting accounts for most of the difference: with the other changes alone, the maximum
case peaked at 122 MiB.

Found while testing, and fixed: when the inbound socket failed to bind at startup, inbound dropped the
last queue sender, the log writer then stopped too, and the daemon reported whichever of the two it
saw first. About one start in three on Linux logged "log writer exited unexpectedly" as the fatal
error. The daemon now keeps a sender until inbound has stopped. One end-to-end test also had a race of
its own: it asserted the delivery count before the daemon had read the target's response. The Linux
suite now passes 40 of 40 runs, up from about two in three.

Configuration files written with the earlier defaults (`max_message_bytes` 1 048 576 with 64
connections) no longer validate; CHANGELOG.md says how to update them.

**Step 3, loss accounting and drop notices (2026-10-21).** Fixed M8, and added the Q2 notices for MQTT.

- Each running channel keeps its position in the log as a *tail*, accounted under the index lock.
  Eviction counts the records a channel had not read in its `lost` counter, and logs their range, as
  it removes them, whether the channel is connected or not. A read that is running counts the gaps
  it passes over itself, and when it ends it counts what an eviction removed beyond it meanwhile. A
  channel that starts below the oldest record counts the difference at start.
- A channel is restarted on every configuration change, and its new tail starts at the cursor,
  which stays below a gap until the gap's notice is acknowledged. The counter therefore keeps a
  high-water mark, so a restart never counts the same records twice. One case stays uncounted:
  records the channel held for an acknowledgement while it restarted, if they were evicted
  meanwhile. They are at most one delivery window, and the notice still covers them.
- A read now returns a gap on its own, never mixed with records, so a channel knows exactly which
  records it will not deliver. Queries pass over gaps as before.
- MQTT publishes a notice in place of what it skips. `lost` covers a range evicted before delivery.
  `rejected` covers a payload above `max_packet_bytes`, with its `seq` and `ts`; the record stays
  queryable. A notice is a `Tracker` entry like a record: packet ids stay bound in request order,
  notices count against `inflight`, are republished after a reconnect, and move the cursor past
  their records only once acknowledged. They use the channel's QoS and are never retained.
  `max_packet_bytes` must be at least 256, the bound on an encoded notice.
- `lag` counts only records still in the log; `notices` is a new per-channel counter.
- HTTP gets the accounting but sends no notices, since step 4 replaces it.

Verified with unit tests, and with an end-to-end test against a minimal MQTT broker inside the test
file. The unit tests cover eviction while idle, eviction during a read, a gap found by a read, a
restart, the `Feed`, `Tracker` notices and the notice size bound. A run against mosquitto 2.0.21
used a 256 KiB log, filled with 2 001 messages while the broker was down:

- Before the broker came up: `lost=1531` and `cursor=1`.
- After it came up, 471 publishes arrived in order: the loss notice for 1–1531, records
  1532–2000, then the rejection notice for 2001.
- The retained message stayed record 2000, and record 2001 was returned by `/v1/messages` under the
  notice's `ts`.

macOS and Linux: 64 unit and 7 end-to-end tests pass; on Linux 30 of 30 repeated end-to-end runs
and 15 of 15 unit runs pass.

**Step 4, WebSocket replaces HTTP (2026-10-21).** Implemented D12, M5 and Q6. H2, D1, D4, L6, L7 and
the HTTP item of L8 are superseded, and probes p08, p09 and p11 are history.

- The HTTP channel is gone, and a configuration with `type: "http"` no longer parses. `reqwest`
  remains as a test-only dependency, the API client of the end-to-end tests. The runtime
  dependency tree went from 168 to 170 crates.
- The WebSocket channel keeps one long-lived connection:
  - Records go out as text frames in the `/v1/messages` shape; `src/view.rs` now serves both.
  - The receiver answers `{"ack": n}` (cumulative) or `{"nack": n, "reason": "..."}` (one frame).
  - At most `inflight` frames and 1 MiB of payload are unanswered.
  - A ping goes out every `keep_alive_s`; no pong within `timeout_ms` ends the session.
  - Unanswered frames are sent again in order after a reconnect, and a stopping channel sends a
    close frame (1001).
- Inside the channel:
  - A writer task owns the sending half of the socket.
  - Records are shared by `Arc` and encoded only when written.
  - Incoming messages are limited to 64 KiB.
  - The frame queue holds at most two windows. A fuller one means the receiver acknowledged
    frames it was never sent, which ends the session.
- Two deviations from the D12 sketch, both for consistency with step 3:
  - A notice is a frame with a top-level `seq`, the last sequence number it stands for. It is
    acknowledged like a record, so it is delivered at least once and in order, not untracked.
  - A refused record gets no notice, since the receiver refused it itself.

  `max_frame_bytes` is checked against the encoded frame, not the payload.
- M5, for both channels: the reconnect backoff (1 s doubling to 60 s, now shared as
  `outbound::pause`) resets only when an acknowledgement advances the cursor; a CONNACK or a
  handshake no longer does. `retries` counts reconnects on both channels; MQTT counted none
  before.
- Q6: `ca_file` for both channels. `src/tls.rs` builds one rustls configuration for MQTT and
  WebSocket. It uses either the platform verifier, built once per process and shared, or only
  the certificates of the CA file, read on every connection so that a replaced file applies at
  the next reconnect. The WebSocket TLS handshake goes through tokio-rustls directly, so
  tokio-tungstenite needs only its `handshake` feature and none of its private TLS features. MQTT
  over TLS had no test before; it has one now.

Found while implementing and testing, all fixed:

- My first `submit` ended the session as soon as a frame could not be queued, for example when the
  writer had stopped. The rest of that batch then never reached the window, but the feed had
  already moved past it, so those records would never have been delivered. Every record now enters
  the window first, and a unit test fails without the fix.
- Two races in my test receiver, not in the daemon:
  - It read its scripted mode after reporting a frame, so the test could change the answer to
    that very frame. This failed 2 of 30 Linux runs.
  - A test deduplicated frames across sessions with a check that only drops consecutive repeats.
    This failed 1 of 40.

Verified with unit tests and end-to-end tests.

- Unit tests (73) cover the window's acknowledgements and refusals, reply parsing, the frame
  length, that only progress ends the backoff, a frame that cannot be queued, the WebSocket
  validation, notice frames, and a CA file.
- End-to-end tests (13) run against a scripted WebSocket receiver (acknowledge, refuse, stay
  silent, drop once or always, stop reading) and the MQTT broker, both also over TLS with test
  certificates in `tests/data`. They cover:
  - forwarding with handshake headers, reconfiguration and restart;
  - resending after a lost session, refusals, and the backlog across restarts;
  - the sync timer and notices over WebSocket;
  - the backoff gaps of both channels (about 1 s, then at least 1.9 s);
  - a receiver that stops reading losing its session after a ping;
  - `wss` and MQTT over TLS trusting only the CA file, while the platform store refuses the test
    CA.

macOS and Linux: 73 unit and 13 end-to-end tests pass. On Linux, 40 of 40 repeated end-to-end runs
and 15 of 15 unit runs pass.

Memory was measured as in step 2, with slow WebSocket receivers in place of the slow HTTP targets.
The receiver is written with the Python standard library, so it is also an independent
implementation of the protocol, and it acknowledges each frame 2 s late, which keeps every
window full. Peaks were 5.5 MiB idle, 31.3 MiB with the defaults and 53.3 MiB with every limit
at its maximum, against 6.0, 30.5 and 53.8 MiB with HTTP channels.

**Step 5, MQTT client id (2026-10-21).** Fixed L10.

- `client_id` is optional and `null` by default. Without one, a channel connects as `uevtd` and
  16 hex digits. The digits are a SipHash of the channel name and the process start time, keyed
  from the OS's random source once per process. A channel keeps its id across sessions and
  restarts while the daemon runs, and gets a new one when the daemon restarts, which clean
  sessions allow. The form, 21 letters and digits, is within the 1 to 23 that every MQTT 3.1.1
  broker must accept, and the channel logs the id when it starts.
- This departs from the suggestion above to derive the default from the host name and the
  channel name. Devices built from one image share a host name, and on a public broker (Q6) a
  predictable id can collide with another user's.
- Validation rejects two enabled channels with the same configured `client_id` on one host.
  The port does not matter, since a broker's client ids span its listeners, and disabled
  channels are exempt. Host names are compared without case; aliases such as `localhost` and
  `127.0.0.1` go undetected. An empty `client_id` is refused, with a hint to leave it out.
- Found while checking how rumqttc encodes the id: it writes the 16-bit length of every MQTT
  string unchecked, and nothing rejected U+0000, which MQTT forbids in strings. Validation now
  bounds `topic`, `client_id` and `username` to 65535 bytes without U+0000, and `password`,
  which is binary, to 65535 bytes. What the previous build did with mosquitto 2.0.21:
  - With U+0000 in the topic or the client id, the broker closed every connection, and the
    channel never delivered.
  - With a topic of 70 000 bytes, each attempt published the message under the first 4 464
    bytes of the topic (70 000 mod 65 536). It then failed on the PUBACK for a packet id read
    from the topic ("unsolicited ack pkid: 30840"). Three copies went to the wrong topic in 8 s.
- The examples in README.md and `contrib/config.example.json` now have `"client_id": null`,
  since a copied example with a fixed id would recreate the problem.

Verified with unit tests and an end-to-end test. The unit tests cover the generated id's form,
stability and distinctness, an explicit id used as given, and the validation rules. In the
end-to-end test the test broker reports each CONNECT's client id: a second channel gets an id
of its own, a channel restarted by a configuration change keeps its id, and an explicit id is
used as given. macOS and Linux: 75 unit and 14 end-to-end tests pass; on Linux 25 of 25
repeated end-to-end runs pass.

Against mosquitto 2.0.21, over about 12 s each:

| Setup | Build | Result |
|-------|-------|--------|
| One daemon, two channels to one broker, no `client_id` | new | Two distinct ids at the broker, no retries |
| Two daemons, standing in for two devices, no `client_id` | new | Two distinct ids, no takeovers, no retries |
| The same | previous | Both as `uevtd`: 8 takeovers ("Client uevtd already connected, closing old connection"), 4 retries each |
| `client_id` `dev42` on two channels to one host, ports 18830 and 18831 | new | Refused at startup, naming both channels |

**Step 6, supervision and recovery (2026-10-21).** Fixed M6 and M7.

- M6: the supervisor also waits on its channel tasks. A task ends only when it is cancelled, so one
  that ends on its own, by a panic, is reported at once: `running` becomes false, `last_error` holds
  the panic message, and the error is logged. The channel starts again from its persisted cursor
  after 1 s, doubling to 60 s while it ends again without progress, the M5 rule. Progress means a
  cursor past the highest one reached before any earlier end. A record that makes the channel panic
  every time is therefore retried once a minute, and holds back only that channel. A new definition
  for the channel ends the wait at once. `restarts` counts these starts, in `/v1/status` and as
  `uevtd_channel_restarts_total`. Panics unwind (the release profile does not abort), so the process
  supervisor of Q7 never sees them; this is the only place they are handled.
- M7, visible before durable: recovery syncs the active segment, after cutting back a torn tail,
  before the head is published. A failed sync still fails the startup.
- M7, startup blocked by an I/O error:
  - A read error while scanning the active segment ends it as corruption would. The records after
    the failure may have been committed and delivered, though, so recovery creates the next segment,
    durably, at the head plus the number of records the rest could hold (its length over the 24-byte
    frame header), and only then cuts the damaged file back. No sequence number is reused, and a
    crash in between is recovered the same way. Channels count the range as lost, with a notice; the
    count is an upper bound. `corrupt_segments` counts the segment.
  - A sealed segment that cannot be opened is indexed from its size. Readers retry it and report the
    error, as for any read error (M9), so a permission mistake can be fixed without loss. One whose
    size cannot be read is skipped and counted.
  - Opening and syncing the active segment still have to succeed: the daemon appends to it, and a
    disk that cannot sync can keep nothing.
- Found in my own first version and fixed: it measured progress from the cursor the task started
  at. A restart begins at the persisted cursor, which can lag by up to a second, so a record that
  panics the channel just after a few unsaved ones would have been sent again every second: each
  restart re-sent those records, which counted as progress. The supervisor test now covers this
  case.
- A race in an end-to-end test, not in the daemon, surfaced in 2 of 25 Linux runs. After the
  receiver had seen frame 3, the sync-interval test stopped the daemon before the daemon had read
  its acknowledgement. The daemon then rightly sent frame 3 again after the restart, where the test
  expected only 4 and 5. With a receiver that answers 300 ms late, the old test fails every time
  with exactly that sequence. The test now waits for the acknowledgement first.
- Checked and dismissed while reading recovery: a failed sync whose rollback fails too leaves
  uncommitted records in a sealed segment, under sequence numbers the next segment reuses. Readers
  never return them, since they locate every record in the last segment starting at or before it. A
  test now pins this down.

Verified with unit tests. In the supervisor test, a channel task sends three records without saving
its cursor and then panics, on its first two starts. The end is reported at once, the task starts
again after 1 s, not within 2 s after the second end, and at once on a new definition. Further tests
cover the backoff and message rules on their own, a scan that stops at a read error, an unreadable
active segment (a directory stands in for it, since reading one fails as a bad sector would), and a
sealed segment without permissions. Each fix's test fails when the old behaviour is put back. macOS
and Linux: 81 unit and 14 end-to-end tests pass. With the test race above fixed, 25 of 25 repeated
end-to-end runs and 15 of 15 unit runs pass on each.

On Linux, against mosquitto 2.0.21, with a build that panics the first three times channel `m`
delivers a poison record, sent as the third of four messages:

| Time | Channel `m` |
|------|-------------|
| 0.0 s | Panicked; `running` false, the panic in `last_error` |
| 1.1 s | Started again at cursor 1, panicked again |
| 3.2 s | Started again at cursor 1, panicked again |
| 7.0 s | Started again at cursor 1; delivered all four, `restarts` 3, lag 0, lost 0 |

Channel `n`, on the same broker, delivered all four at once without a restart, and its subscriber
received each once. Channel `m`'s subscriber received the first message twice, since publishes
queued before a panic may still go out: at-least-once, as after any lost session. The sync at
startup has no unit test, so it was traced: with a 5 s sync interval, a daemon killed with SIGKILL
after writing three records (head 0, nothing synced yet) recovered them at the restart with head 3,
and `strace` showed `fdatasync` on the segment before either socket was bound.

**Step 7, observability and documentation (2026-10-21).** Fixed L8 and the §5 mismatches,
implemented D6, and documented H1, D3 and D9.

- Health: `/v1/health` answers 503 with the reason from a failed write or sync until a batch is
  committed again, and while one batch or sync has taken more than 30 s, as on a stalled disk. It
  reads only atomics and the error slot, never the index lock that a stalled writer can hold, so it
  answers even then. Status shows the failure as `storage.last_error`.
- Write failures (L8): records lost to a failed write or sync are counted in `dropped`, from the
  batch at hand or from the unsynced records a seal discards, never both. `write_errors` also counts
  failures to open or create the active segment, which were neither counted nor kept.
- Counters (L8): `uevtd_storage_batches_total` still counts batches written, now under that name;
  its help said committed. `bytes_written` counts committed bytes, as its help now says.
- Clock steps (L8): a message is clamped when its time is below the previous record's, but it counts
  as a clock step, and a large one is logged, only while the clock itself is behind that time.
  Messages that two connections stamped and queued in the opposite order are clamped without being
  counted.
- Disabled channels (L8): one disabled since startup showed `cursor` 0 and the whole log as `lag`.
  It shows the position in its cursor file, or `cursor: null` and `lag` 0 without one.
- API errors (L8): 405 had an empty body and 413 plain text; both are JSON now. The review also
  named malformed queries, which was wrong: any query string parses into the parameter map, and
  every parameter was already checked with a JSON error.
- D6, performance: the writer queue's messages and bytes (`queued`, `queued_bytes`), syncs and the
  time spent in them (`syncs`, `sync_us`; the mean sync time and the records per sync come from
  pairs), and per channel the time from receipt to acknowledgement summed over deliveries
  (`delivery_us`, whose mean is over `delivered`).
- D6, time: `storage.first_ts` and `head_ts`, the receipt times of the oldest and newest records,
  and per channel `waiting_ts`, the receipt time of the oldest record waiting for delivery. A
  record's time is known only to the log: a channel that has delivered everything when its
  connection drops holds nothing that would tell. A task in the API therefore looks these times up
  once a second, reading the segment from where the previous lookup for that channel stopped, so
  that no request waits for the disk; the values can be a second old. In metrics they are seconds
  since the epoch, for `time() - x` in alerts.
- Docs: `contrib/uevtd.service` is gone, and the README states what the system must provide: a
  supervisor that starts the daemon again after a non-zero exit, with the exit statuses, and write
  access to the configuration's directory, `storage.path` and the socket directories (H1). README
  and ARCHITECTURE §4 state the producer contract, compact JSON one message to a line (D9), and
  where the guarantees start, at commit, with what can be lost before (D3).
- §5: of the nine mismatches, steps 2 to 6 had already fixed six (the eviction bounds, the
  once-a-second cursor save, §6.4 against §11, the validation list, loss at eviction, the MQTT
  backoff). This step fixes the other three: compact JSON (§4); the write-failure paragraph, which
  claimed a truncation to the committed length when the truncation itself had failed (§5.4); and the
  service manager in §10 and the README.

Verified with unit tests and an end-to-end test. The unit tests cover the counts and the health
after a failed write and after the commit that clears it, a batch that takes too long, clamping with
and without a clock behind the log, timestamp lookups across segments and from a mark, the queue
depth, delivery times in both channels, and a disabled channel's cursor. The end-to-end test reads
status and metrics while a receiver leaves two frames unanswered, and checks the 405, 413 and 404
bodies. Each fix's test fails when the old behaviour is put back. macOS and Linux: 88 unit and 15
end-to-end tests pass; on Linux 25 of 25 repeated end-to-end runs and 15 of 15 unit runs pass.

On Linux, with release builds of this step and of step 6:

| Check | Step 6 | Step 7 |
|-------|--------|--------|
| Log on `/dev/shm` (a 64 MiB tmpfs) filled up, then 4 messages, then space freed and 2 more | Health 200 throughout; `write_errors` 4, no count of the lost messages | Health 503, "No space left on device", until the next commit; `dropped` 4; `last_error` cleared by the commit; queries return 1 to 5, since a failed write takes no sequence number |
| 16 connections sending 5 000 messages each at once; the clock does not move | `clock_steps` 12 115, and 12 116 in a second run | 0 and 0 |
| Every `fdatasync` delayed 40 s by `strace` | Not run | Health 200 until the batch had taken 30 s, then 503 "log writer busy for 32 s", and 200 once the sync returned; status answered throughout |
| MQTT broker down for 3 s after 3 messages, then started | Not run | `lag` 3 and `waiting_ts` the first message's receipt time, in status and metrics; delivered 4.0 s after the broker came up, a mean delivery time of 6.9 s; 5.6 MiB resident |

Residual: after a failure, health stays 503 until a batch is committed, so a daemon that receives
nothing more stays unhealthy even once the disk has recovered. The first lookup of a channel's
waiting time reads its segment from the start, up to `segment_bytes`. A real backward clock step was
tested only by the unit test, since the dev container cannot set its clock.

**Step 8, the host name as MQTT client id (2026-10-21).** Revisits step 5 on your decision: a
channel without a `client_id` connects with the host name, and a configured id is used exactly as
given, including a `"uevtd"` saved by an earlier version.

- The host name comes from `gethostname`, read when the channel starts, so a renamed host applies
  from the channel's next start. `libc`, a dependency on glibc only so far, is one on every target
  now; it was in the tree already, through tokio.
- A host without a name, an empty one or Linux's `(none)`, which the kernel reports when nothing has
  set one, falls back to step 5's generated id.
- Validation compares the ids the channels will connect with, the host name included. Two enabled
  MQTT channels to one host without a `client_id` are refused, since they would take turns, and so
  is a configured id equal to the host name beside a channel without one. The message names the host
  name. Such a configuration loaded under step 5, whose generated ids kept the channels apart; it is
  refused at startup now.
- What step 5 avoided returns as a requirement: devices that share a broker need distinct host
  names, or a `client_id` each. Devices built from one image share their host name until they are
  given their own, and on a public broker a host name can collide with another user's id. MQTT 3.1.1
  obliges brokers to accept 23 letters and digits only, so a strict one may refuse a longer host
  name, or one with `-` or `.`; the refusal shows in `last_error`.

Verified with unit tests and an end-to-end test. The unit tests cover the id (the host name, a
configured id as given, the old default, and the fallback's form) and the duplicate rules for a
given host name and for a host without one. The end-to-end test takes the host name from `uname -n`
and checks that the test broker sees it, that a second channel without an id and one given the host
name are refused, that `"uevtd"` is used as given, and that a channel restarted by a configuration
change keeps its id. Each fix's test fails when the old behaviour is put back. macOS and Linux: 88
unit and 15 end-to-end tests pass; on Linux 25 of 25 repeated end-to-end runs and 15 of 15 unit runs
pass.

Against mosquitto 2.0.21 on Linux, where `uname -n` is `dev`:

| Setup | Result |
|-------|--------|
| One channel without a `client_id` | The broker saw `dev` |
| A second channel to the broker without one, by `PATCH` | 400: "channels m and n both connect to 127.0.0.1 as client_id "dev", the host name used without a client_id" |
| The same with `"client_id": "uevtd"` | Applied; the broker saw `uevtd` |
| A second daemon on the same host without a `client_id`, for 8 s | 5 takeovers, as for two devices with one host name |
| A configuration file with two channels to the broker and no `client_id` | Startup refused, exit status 1, with the same message |

**Step 9, the log (2026-10-22).** Fixed H3, H4, M12, L17, L18, L24, L25 and L27 of §9, and the
32-bit item of L29, which is in the same code.

- H3: a failed read empties the segment reader's buffer, so the retry reads the file. One failed read
  now costs nothing: the records before it are returned (M9), and the retry goes on from where the
  read failed. A query that meets a read error returns the records before it with `next` set, and the
  next page reports the error. Before, the whole query failed (once H3 no longer skipped instead).
- H4: `Storage::open` first takes an exclusive `flock` on `lock` in `storage.path` and writes its
  process id there. A second instance fails at once, exit status 1: "opening log at …: locked by
  another process, pid N; is another uevtd running on this log?". The lock goes with the process. On a
  file system that cannot lock, the daemon logs a warning and runs without it, as before.
- M12: the proposed fix numbered past intact records found after the damage and kept the plain cut
  otherwise. That misses p21's second case, a damaged last record, since nothing follows it. Recovery
  now tells a torn tail from damage by what a crash can leave:
  - A torn tail is cut off, as before; its records were never committed. Either the file ends inside a
    record, or the last record ends in zeros that run to the end of the file. The zeros must start where
    the record does or at a 512-byte block boundary inside it, as storage reads a write that never
    arrived.
  - Damage is any other end: a read error, a record that is there in full but invalid, or intact records
    after an invalid one. The file is kept as it is, as a sealed segment. The log continues in a new
    segment past every record the rest could hold: one per frame header after the last intact record.
    A search finds intact records, frames whose checksum holds and whose sequence number fits the bytes
    in front of them. They bound the numbering and are never delivered.
  - A trailing file is removed only if it holds no intact record anywhere.
  - Numbering also continues past the highest cursor file, a temporary one included, when that is past
    the recovered head. Damage that looked like a torn tail leaves such a cursor, and so does a disk
    that lost data after reporting it synced, as SD cards can. Outbound reset the cursor to the head,
    so new records took numbers the receiver had seen. Cursors above 2^53 are ignored.
- L24: a damaged segment is no longer cut back. Creating the next segment is the only change recovery
  makes, so a crash at any point is recovered the same way, and no sync waits on a cut-back. This
  changes M7's handling of a read error at startup. Readers now retry the unreadable part, as they
  retry one found later (M9), instead of skipping it at once. A transient error then costs nothing.
  A persistent one holds the channels until the segment is evicted, which is the case the design
  note's decision (step 11) bounds.
- L17: a failed removal sets `evict_error`. `/v1/health` reports it ("log over its size limit:
  removing segment N: …"), and status shows it as `storage.last_error` when no write error is set. It
  is logged once a minute and cleared once eviction succeeds.
- L18: each indexed segment has a flag. The first reader that skips a segment counts and logs it, and
  later ones log at debug. Recovery marks the damaged segment it counts.
- L25: `force_new` is cleared only once the new segment exists, and a stale index entry goes only after
  its file.
- L27: a sealed segment whose size cannot be read is indexed as a full segment of `segment_bytes` and
  counted as damaged. Readers retry it as they retry one that cannot be opened, and eviction removes it
  in its turn. One that disappears between the listing and the stat is left out.
- L29, 32-bit: lengths are computed in u64, with checked conversions, in `peek`, `fill` and
  `decode_frame`. A corrupt length near 2^32 wrapped to a frame shorter than its header, and one of
  2^32 − 24 to a length of 0, which made positioning loop forever.

Found while implementing, and fixed:

- `fill` read whole chunks with `read_exact_at`. When a file was shorter than its committed length, as
  H4's cut-back left one, a read failed up to 64 KiB early. Up to that many intact records were then
  skipped as "segment shorter than its committed length". It now returns what the file holds.
- A corrupt length made every reader of the segment allocate up to the segment's length, 256 MiB at
  most, before the checksum failed. A frame claiming more than 1 MiB, more than any message, is now
  checked a chunk at a time and read only if it is intact.
- The search computes the checksum of every candidate. Payloads crafted to look like headers at every
  offset made it quadratic: 25 s for a 1 MiB file in a debug build, hours for a large segment. Checking
  more than four times the bytes searched ends the search, and the result counts as damage.
- A torn tail that could not be cut off failed the startup. The writer cuts the file back before it
  appends anyway, so it is a warning now.

Verified with unit tests, end-to-end tests and probes of the release binary:

- Each fix's test fails when its old behaviour is put back: 18 reverts, each applied alone.
- The log's model tests of §9 joined the suite; those of the framer, the window and the tracker join
  with their modules in step 11. The recovery test was rewritten for the new rules:
  - the records before the damage are kept, and numbering goes no further than the rest could hold;
  - a torn tail or zeros cost no number;
  - damage never lets a number of a record in the file be given out again. The exception is the last
    record with its length damaged to run past the end, which looks torn.

  The default seeds are few, 6.7 s on macOS, where every sync is a full flush, and `UEVTD_SEEDS`
  raises them. On Linux with the log on a tmpfs, 20 000 seeds of each and 1 000 of the writer race
  passed.
- The new end-to-end test starts a second daemon on the same log. It stops at the lock, names the
  holder's process id, binds nothing and leaves the cursor file.
- macOS and Linux: 110 unit and 16 end-to-end tests pass; on Linux 25 of 25 repeated end-to-end runs
  and 15 of 15 unit runs pass.

Against the release binary on Linux:

| Probe | Before | After |
|-------|--------|-------|
| p15: one EIO injected into a query's second read | 200 without records 131–2 082 | 200 with all 2 191 records; `corrupt_segments` 0 |
| p18: a second instance without channels | the first one's cursor file deleted | exit 1, "locked by another process, pid …"; the file kept |
| p18: the same configuration twice | ran until it failed on the socket | exit 1; no connection to the broker |
| p18: started 60 times during a burst | `lost` 45, 4 notices, `corrupt_segments` 4 | 10 000 delivered, `lost` 0, `corrupt_segments` 0 |

Residual:

- Two kinds of damage to the last record still look like a torn tail, and its number can be given
  out again unless a cursor is past it:
  - its length is damaged so that it runs past the end of the file;
  - its payload ends in zeros across a block boundary.
- On FAT, a power loss can leave stale data at the end of a file. If that data's first length
  happens to fit in the file, it counts as damage, and a few numbers are skipped and counted lost.
- Until step 11's decision, a persistent read error found at startup holds the channels until the
  segment is evicted.
- A failed directory sync after creating a segment is still only a warning (step 1). On a file system
  that persists a new file's data but not its directory entry, a power loss can then take a committed
  segment. The cursor floor protects what channels delivered from it.

## 9. Second review (2026-10-22)

Reviewed at `407be9d`, after steps 1–8, for defects that the fixes introduced or left, with the
emphasis on boundaries. How it was checked:

- Every source file was read again. Three further reviews took storage, outbound, and configuration,
  API and inbound in turn, with the sources of the locked dependencies (rumqttc 0.25.1,
  tokio-tungstenite 0.30.0, tokio 1.54.0, axum 0.8.9, hyper 1.12.0, serde_json 1.0.154). Each finding
  below was then reproduced by a test or a probe, or, where marked "code", traced through the code.
- Randomized model tests for the pure state machines and the log, in a scratch copy of the tree. They
  found nothing; their coverage is listed under "Verified correct".
- Fault injection against the release binary on the Linux container: delays and EIO injected with
  strace, a second instance, a throttled link, a low descriptor limit.
- Clippy's cast, unwrap and indexing lints, to list every narrowing cast and slice for inspection.

| ID  | Sev    | Area      | Finding | Evidence |
|-----|--------|-----------|---------|----------|
| H3  | High   | Storage   | After a failed read the segment reader keeps stale bytes, and its retry skips the rest of the segment as corrupt: one transient EIO costs a channel up to a segment of intact records, counted as lost, and a query returns 200 without them | p15–p17 |
| H4  | High   | Storage   | A second instance on the same log truncates the running one's active segment, deletes its cursor files and starts channels before it fails on the socket | p18 |
| M10 | Medium | API       | API connections are unlimited; idle ones use up the file descriptors, and the writer then drops every message | p19 |
| M11 | Medium | Config    | A JSON array is read as a configuration: `PUT []`, `PATCH []` or `PATCH {"outbound": []}` removes every channel and its cursor | p20 |
| M12 | Medium | Storage   | Recovery cuts back damaged committed records in the active segment as a torn tail, or deletes the file, and reuses their sequence numbers | p21 |
| M13 | Medium | WebSocket | On a slow link a session that keeps acknowledging ends with "no pong"; a frame that takes longer than `keep_alive_s` + `timeout_ms` to send is never delivered | p22 |
| M14 | Medium | MQTT      | Connecting and every flush have rumqttc's fixed 5 s budget, so a broker behind a slow or high-latency link is never reached | p23 |
| L13 | Low    | API       | "One query at a time" does not hold: an abandoned query keeps reading after its slot is given away, and responses are written after it is released | p14 |
| L14 | Low    | Config    | A saved configuration can become invalid when the host name changes, and the daemon then refuses to start (step 8) | p24 |
| L15 | Low    | Config    | A dangling symlink at the configuration path is replaced by a file of defaults | p24 |
| L16 | Low    | Config    | Socket paths that can never be bound pass validation; the next start fails for good | p24 |
| L17 | Low    | Storage   | Eviction stops at a segment it cannot remove, and the log grows past `max_bytes` unreported | p25 |
| L18 | Low    | Storage   | `corrupt_segments` counts every pass over a corrupt segment, not segments | p26 |
| L19 | Low    | API       | The receipt-time lookup forgets its position when a channel catches up, then rescans the active segment from its start | code |
| L20 | Low    | Outbound  | Any error reading a cursor, not only an unparsable one, replays the whole log and overwrites the file | code |
| L21 | Low    | MQTT      | A channel cannot be stopped while it reads its TLS configuration; a `ca_file` that blocks hangs reconciliation and shutdown | code |
| L22 | Low    | MQTT      | The memory bound ignores the topic, which rumqttc copies into every publish in flight | code |
| L23 | Low    | Inbound   | Connections opened before a limit change keep the old `max_message_bytes` for their lifetime | code |
| L24 | Low    | Storage   | Recovery after a read error is not repeatable after a crash, and a failed cut-back skips the sync | code |
| L25 | Low    | Storage   | Two failures in a row can reopen a sealed segment and later reuse sequence numbers | code |
| L26 | Low    | Runtime   | Blocking work on runtime threads: SIGHUP's reload, and the index lock held across file deletions | code |
| L27 | Low    | Storage   | A sealed segment that cannot be stat'ed at startup is left out of the index and never evicted | code |
| L28 | Low    | Config    | A save that fails after its rename leaves the new configuration on disk but not applied | code |
| L29 | Low    | Misc      | Smaller items, listed below | code |

### H3. A failed read leaves stale bytes that the retry decodes

Where: [src/storage/segment.rs:219-234](src/storage/segment.rs#L219) (`SegmentCursor::fill`),
[src/storage/mod.rs:1268](src/storage/mod.rs#L1268), [src/outbound/mod.rs:488-493](src/outbound/mod.rs#L488).

`fill` moves `buf_pos` to the new position and resizes the buffer before it reads. When the read fails
with anything but end of file, the buffer keeps what it held, or what the failed read left in it, now
labelled with the new position. The reader keeps its cursor across the error, as M9's fix intends, so
the next `fill` at that position finds the bytes "already buffered" and decodes them without reading.
They usually begin with the first frame of the previous chunk. Its CRC is valid but its sequence number
is lower, so `Reader::next` reports "sequence mismatch" and `skip_segment` passes the rest of the
segment, or everything up to the head in the active segment. A persistent error ends the same way one
retry later, because a buffer that held nothing decodes as a corrupt frame.

Neither half of M9 holds: the records before a failure are delivered, but the failing position is
never retried, and a persistent error does not stall. The channel counts intact records as lost, sends
a `lost` notice in their place and passes them for good. `last_error` never shows the I/O error, and
`corrupt_segments` grows. A query that meets the error mid-page returns 200 without those records.

Evidence:
- p16: a segment cursor whose read fails once at record 126 returns record 1 on the retry.
- p17: a channel's reader, after one transient failure, returns 95 records, then skips 1 912 intact
  ones, counted in `lost`.
- p15: the release binary, with one EIO injected by strace into a query's second read, answered 200
  with records 131–2 082 missing and logged "corrupt record; skipping the rest of the segment … sequence
  mismatch". The same query without the fault returned all of them.

Fix: clear the buffer before `fill` returns any error, so the retry reads the file. p16 becomes the
regression test.

### H4. A second instance damages the running one before it fails

Where: [src/daemon.rs:56](src/daemon.rs#L56), [src/storage/mod.rs:145-250](src/storage/mod.rs#L145)
(`Storage::open`), [src/outbound/mod.rs:151](src/outbound/mod.rs#L151).

`Storage::open` runs before either socket is bound, and nothing locks `storage.path`. A second instance
may come from someone starting the daemon by hand, from a second init mechanism, or from a supervisor
that does not wait for the first. It recovers the log as if it owned it:
- it scans and syncs the active segment;
- it cuts the segment back at an incomplete record, which is the first instance's batch being written;
- it removes trailing files and evicts by its own limit;
- its supervisor removes the cursor files of channels it does not have and starts the ones it has.

All of this happens while its inbound bind is being tried. Only when the bind fails does it exit with
status 1.

A cut-back under the first instance leaves that instance's committed length past the end of the file.
Its readers then skip the rest of the segment as corrupt ("segment shorter than its committed length"),
and its later appends land at offsets its index does not expect. A deleted cursor file is written again
only when its channel next advances. After an unclean stop, that channel starts at the head and skips
its backlog without counting it. §4's "a second instance fails fast" holds for the socket only.

Evidence (p18):
- A second instance without channels deleted the first one's cursor file and exited 1.
- Started 60 times during a burst into the first instance, it cost that instance's MQTT channel 45
  committed records (`lost` 45, 4 notices, `corrupt_segments` 4).
- The same burst without it: 10 000 delivered, `lost` 0.

Fix: take an exclusive `flock` on a lock file in `storage.path` before `Storage::open` does anything
else, and exit with a message that says another instance holds the log. The lock goes with the process,
so a crash leaves nothing to clean up.

### M10. Unlimited API connections use up the file descriptors

Where: [src/api.rs:192-201](src/api.rs#L192).

axum 0.8.9 builds hyper's connections without a timer, so neither a header-read nor an idle timeout
applies, and nothing limits their number. A local client that leaks connections, or opens one per poll
without closing it, uses up the process's descriptors (the soft limit is typically 1024). The writer
then cannot open or create a segment, and every batch is dropped (`dropped`, `write_errors`, "Too many
open files"). Cursor saves, segment opens by readers and inbound accepts fail the same way. Inbound has
`max_connections`; the API has nothing like it.

Evidence (p19): with the daemon at its limit of 128 descriptors, held by idle API connections, a
producer that had connected before them sent 300 messages. All 300 were dropped, with `last_error`
"opening the active segment: Too many open files (os error 24)".

Fix: accept at most a few API connections at once (16 is plenty for a local control socket), close
idle ones after a timeout, and raise the soft descriptor limit to the hard one at startup.

### M11. A JSON array is read as a configuration

Where: [src/config.rs:22-30](src/config.rs#L22) and every `#[serde(default)]` struct;
[src/config.rs:651-655](src/config.rs#L651) (`parse`: the file and SIGHUP),
[src/config.rs:743-748](src/config.rs#L743) (`PATCH`), [src/api.rs:278](src/api.rs#L278) (`PUT`).

serde reads a struct from a sequence too, taking missing elements from its defaults, and
`deny_unknown_fields` does not apply to sequences. So `[]` reads as `Config::default()`, and
`{"outbound": []}` as an `outbound` without channels. RFC 7386 makes a non-object patch replace the
whole document, which should then fail validation; here it passes.

`PATCH []` is also an empty JSON Patch (RFC 6902), which tools that patch by diff send. It:
- removes every channel and deletes its cursor file, so a channel added back starts at the head and
  skips its backlog uncounted;
- moves both sockets to their default paths;
- resets the storage limits, evicting at once if they were larger;
- switches to the default `storage.path` at the next restart.

Evidence (p20): `parse("[]")` gives the defaults. `PATCH []` and `PATCH {"outbound": []}` return 200
without channels and save that to the file. `{"outbound": {"channels": []}}` is refused, since serde
does not read a map from a sequence.

Fix: before reading a document, check its shape: the root, each section, `channels` and each channel
must be JSON objects.

### M12. Recovery reuses the sequence numbers of damaged committed records

Where: [src/storage/mod.rs:167-185](src/storage/mod.rs#L167) (trailing files without records),
[src/storage/mod.rs:685-691](src/storage/mod.rs#L685) (`recover_active`, an invalid record).

At startup the first invalid record in the active segment ends it. The file is cut back there, and
numbering resumes after the last valid record. That is right for a torn tail, which holds only records
that were never committed. Damage to committed records, from bit rot or a flash page torn by a power
loss elsewhere, is treated the same way. The records after the damage go with it, though they may have
been delivered, and their sequence numbers are given to new messages. A trailing file whose header and
first frame are both invalid is deleted outright, whatever it holds.

Receivers that deduplicate by `seq`, as §6.2 asks, then drop the new messages as duplicates, and
channel cursors past the new head are reset to it. The read-error path already guards against exactly
this, by skipping the numbers that the unreadable rest could hold. This is L3 at recovery instead of at
read, and with reuse.

Evidence (p21), with records 23–25 committed in the active segment:
- Zeroing the segment's first 48 bytes made recovery delete the file, and the next message became
  seq 23.
- Flipping one payload byte of record 25 cut it back, and the next message became seq 25 again.

Fix: when CRC-valid frames follow the damage, number past them. Scan the rest of the file for such
frames only to learn the highest sequence number in it, never to deliver them, and start the next
segment above that number, as the read-error path does. Keep the plain cut for a tail without a valid
frame, which is what a crash leaves. Delete a trailing file only when no valid frame is in it at all.

### M13. Slow links end healthy WebSocket sessions as "no pong"

Where: [src/outbound/websocket.rs:146](src/outbound/websocket.rs#L146),
[src/outbound/websocket.rs:156-161](src/outbound/websocket.rs#L156).

The pong deadline starts when the ping enters the writer's queue. The ping sits behind up to a window
of frames there (two after a reconnect), and behind whatever the socket's send buffer holds. Only a
pong meets the deadline; acknowledgements do not count. Whenever the frames ahead of a ping take longer
than `timeout_ms` to send, a healthy session ends as "no pong". The acknowledgements reset the backoff,
so the channel reconnects after 1 s and sends every unanswered frame again. With the defaults (a 1 MiB
window, `timeout_ms` 10 000) this happens below about 100 KiB/s while a backlog drains.

A single frame that takes longer than `keep_alive_s` + `timeout_ms` to send is never acknowledged in
any session, and the channel stops there. An example is a 64 KiB binary payload, escaped to 384 KiB of
JSON, over 8 KiB/s. §10 attributes the error to a receiver that stopped answering pings.

Evidence (p22): over a link passing 32 KiB/s towards a receiver that acknowledges every frame, with
`keep_alive_s` 1 and `timeout_ms` 2 000, five sessions ended with "no pong within 2000 ms" in 20 s.
152 of 190 records were sent more than once.

Fix: count any message from the receiver as a sign of life, and start the deadline when the writer has
written the ping.

### M14. MQTT has a fixed 5 s network budget

Where: [src/outbound/mqtt.rs:115-145](src/outbound/mqtt.rs#L115).

The channel never sets rumqttc's `NetworkOptions`, so `connection_timeout` stays at its default of
5 s. rumqttc applies it to the whole connection (DNS, TCP, TLS and CONNECT/CONNACK) and to the flush of
every request:
- A broker behind a high-latency link is never reached ("Network timeout"). That covers several round
  trips of a second or more, or a resolver that waits 5 s on a dead first nameserver.
- A flush that cannot complete in 5 s, such as a large publish over a slow uplink, ends the session
  ("Flush timeout"). The entry is published first in the next session, so the channel can stall
  behind it.

Nothing in the configuration changes this, while WebSocket has `timeout_ms`. rumqttc's default also
leaves Nagle's algorithm on.

Evidence (p23): a broker that answers CONNECT after 6 s was never connected in 20 s, with 3 attempts
and `last_error` "Network timeout". The flush half needs a link slower than loopback's socket buffers
allow; it follows from the same timeout.

Fix: give MQTT channels a `timeout_ms` (default 10 000, as for WebSocket), pass it to
`NetworkOptions::set_connection_timeout` (whole seconds, rounded up), and set `set_tcp_nodelay(true)`.

### Low

- **L13. One query at a time does not hold** ([src/api.rs:331-347](src/api.rs#L331)).
  - **Abandoned queries:** the slot belongs to the request handler. When a client disconnects
    mid-query, hyper drops the handler and the slot with it, while the read goes on in the blocking
    pool, so the next query starts beside it.
  - **Slow readers:** a response, up to six times a 1 MiB page as JSON, is written after the slot is
    released, so slow readers hold one each.
  - **Evidence (p14):** with every read delayed by 1 s, four queries abandoned after 0.5 s each left
    four threads reading at once.
  - **Fix:** an owned permit, moved into the blocking task and then into the response body.
- **L14. A saved configuration can become invalid when the host name changes**
  ([src/config.rs:373](src/config.rs#L373), from step 8).
  - **Mechanism:** the duplicate check reads the host name at every validation, startup included.
    Two channels to one broker without a `client_id`, saved while the host had no name, become
    invalid once it has one. So does a configured id that a new host name happens to equal.
  - **Effect:** the daemon then refuses to start, and the supervisor restarts it in a loop. Until
    the restart, every unrelated `PATCH` is refused.
  - **Evidence (p24):** the same two channels pass with no host name and fail with `dev`.
  - **Fix:** refuse such a duplicate when the configuration is changed (`PUT`, `PATCH`, SIGHUP). At
    startup, only log it and report it in the channels' `last_error`.
- **L15. A dangling symlink at the configuration path is replaced by defaults**
  ([src/config.rs:637-646](src/config.rs#L637)).
  - **Mechanism:** a missing target reads as a missing file. Defaults are written, and the rename
    replaces the link with a regular file.
  - **Effect:** with the configuration on a data partition that mounts late, the device runs
    without channels from then on.
  - **Evidence:** p24.
  - **Fix:** write defaults only when nothing at all is at the path (`symlink_metadata`), and refuse
    a dangling link.
- **L16. Socket paths that can never be bound pass validation**
  ([src/config.rs:306-314](src/config.rs#L306)).
  - **Mechanism:** paths longer than `sun_path` allows (107 bytes on Linux), or containing U+0000,
    are accepted.
  - **Effect:** a live rebind fails into `last_error`, and the next start fails for good. For
    `api.path`, no API is left to repair it.
  - **Evidence:** p24, with a 120-byte path and one with U+0000.
  - **Fix:** validate both, and U+0000 in `storage.path`.
- **L17. Eviction stops at a segment it cannot remove**
  ([src/storage/mod.rs:505-515](src/storage/mod.rs#L505)).
  - **Mechanism:** any error but NotFound ends the loop, so one undeletable oldest segment (EPERM,
    EIO) keeps every later one.
  - **Effect:** the log grows until the disk is full, with an error line per commit and nothing in
    `last_error` or `/v1/health`.
  - **Evidence (p25):** 34 488 bytes in 37 segments against a `max_bytes` of 4 000, with health OK.
  - **Fix:** set the log's `last_error`, and fail health while the log is over its limit by more
    than a segment.
- **L18. `corrupt_segments` counts passes, not segments**
  ([src/storage/mod.rs:1306](src/storage/mod.rs#L1306)).
  - **Mechanism:** every reader that skips a corrupt segment counts it again and logs an error:
    each channel, each restart and each query. An alert on the counter fires on every query that
    passes an old corrupt segment.
  - **Evidence (p26):** one corrupt segment, read by three readers and two queries, counted 5.
  - **Fix:** mark the segment in the index, and count and log it once.
- **L19. The receipt-time lookup forgets its position when a channel catches up**
  ([src/api.rs:173](src/api.rs#L173)).
  - **Mechanism:** the lookups of channels with nothing waiting are dropped. A channel that keeps
    up, lagging at some ticks and not at others, rescans its active segment from the start at every
    change.
  - **Cost:** up to `segment_bytes` each time; at 256 MiB that does not fit a 128 MiB device's page
    cache.
  - **Docs:** §9's "costs about as much as reading what it delivered" holds only while a channel
    stays behind.
  - **Fix:** keep a lookup for every configured channel, and only omit its time while nothing waits.
- **L20. Any error reading a cursor replays the whole log**
  ([src/outbound/mod.rs:385-388](src/outbound/mod.rs#L385)).
  - **Mechanism:** L11 chose a replay for a cursor that cannot be parsed. EACCES, EIO or EMFILE
    take the same path, and the intact file is overwritten with the oldest position.
  - **Fix:** replay only on a parse error; on an I/O error, keep the file and retry the start later.
- **L21. An MQTT channel cannot be stopped while it reads its TLS configuration**
  ([src/outbound/mqtt.rs:53](src/outbound/mqtt.rs#L53)).
  - **Mechanism:** `connect` is not raced with cancellation, unlike WebSocket's. A `ca_file` that
    blocks (a FIFO, a hung network mount) keeps the task in `connect`.
  - **Effect:** `reconcile` waits for the task forever, no other channel change applies, and
    shutdown hangs until a second signal.
  - **Fix:** race `connect` with the token, as WebSocket does.
- **L22. The MQTT memory bound ignores the topic**
  ([src/config.rs:598-607](src/config.rs#L598)).
  - **Mechanism:** rumqttc copies the topic into every publish and keeps it until the
    acknowledgement.
  - **Effect:** a 65 535-byte topic with `inflight` 1 024 holds 64 MiB per channel, against §11.1's
    "about 2 MiB".
  - **Fix:** cap topics at a size real ones never reach, such as 1 KiB.
- **L23. Open connections keep the old message limit**
  ([src/inbound.rs:74](src/inbound.rs#L74)).
  - **Mechanism:** a producer connected before `max_message_bytes` is lowered keeps the old limit
    for its lifetime.
  - **Effect:** the 16 MiB bound of §7.2 and §11.1 can be exceeded. Each of the producer's
    messages can also exceed the new `segment_bytes`, and with a small `max_bytes` one of them
    evicts the rest: M2 again.
  - **Docs:** §5.5's "only to a message accepted just before the limits were lowered" understates
    it.
  - **Fix:** apply the new limit to open connections between messages.
- **L24. Recovery after a read error is not repeatable**
  ([src/storage/mod.rs:657-658](src/storage/mod.rs#L657),
  [src/storage/mod.rs:717](src/storage/mod.rs#L717)).
  - **Crash case:** after a crash between creating the next segment and cutting back the damaged
    one, the new segment is last at the next start. The damaged one is indexed as sealed at its full
    length, and channels retry its unreadable rest every second. The comment says such a crash "is
    recovered the same way again".
  - **Sync case:** when the cut-back fails, `and_then` also skips the sync, and the valid prefix
    becomes visible unsynced.
  - **Fix:** sync regardless of the cut-back, and at startup scan the segment before an empty last
    one as well.
- **L25. Two failures in a row can reuse sequence numbers**
  ([src/storage/mod.rs:959-977](src/storage/mod.rs#L959)).
  - **Mechanism:** `open_active` clears `force_new` before the new segment exists, and
    `create_segment` drops the old index entry before removing its file.
  - **Effect:** suppose a sync fails on a segment's first batch and removing that segment's file
    then fails too. The following batch reopens the segment sealed before it and appends there. At
    restart the stale file is the last one, becomes active, and its numbers are given out again.
  - **Fix:** clear `force_new` only once the new segment exists, and drop the index entry only after
    its file is gone.
- **L26. Blocking work on runtime threads** ([src/daemon.rs:252](src/daemon.rs#L252),
  [src/storage/mod.rs:502-533](src/storage/mod.rs#L502), [src/api.rs:156](src/api.rs#L156)).
  - **SIGHUP:** the reload reads the file and waits for the configuration lock, which a save holds
    across its fsyncs, on a worker thread.
  - **Index lock:** `status()` takes the index lock on workers (the receipt-time task, status and
    metrics), while eviction holds that lock across file deletions.
  - **Effect:** with one or two cores, a stalled disk or a large eviction stops every worker,
    `/v1/health` included, though §9 says health answers even then.
  - **Fix:** run the reload on the blocking pool, and delete evicted files after releasing the lock.
- **L27. A sealed segment that cannot be stat'ed at startup is left out**
  ([src/storage/mod.rs:194-200](src/storage/mod.rs#L194)).
  - **Mechanism:** unlike one that cannot be opened (§5.6), it is not indexed.
  - **Effect:** its records read as a gap, and the file is never evicted, outside `max_bytes`.
  - **Fix:** keep it in the index, so that readers retry it and eviction removes it.
- **L28. A configuration save that fails after its rename**
  ([src/config.rs:679-680](src/config.rs#L679)).
  - **Mechanism:** if opening or syncing the directory fails, the request fails with 500 and the
    configuration is not applied, but the file already holds it.
  - **Effect:** it applies at the next SIGHUP or restart, and the next `PATCH` overwrites it with
    one based on the old configuration.
  - **Fix:** after the rename, treat a failed directory sync as a warning, as `storage::sync_dir`
    does.
- **L29. Smaller items.**
  - `rejected` counts again when a channel restarts before a rejection is answered: the oversized
    record is read again, and a refusal can come again
    ([src/outbound/websocket.rs:189-199](src/outbound/websocket.rs#L189),
    [src/outbound/mqtt.rs:175-185](src/outbound/mqtt.rs#L175)).
  - A failed chmod on rebind is recorded as applied, so republishing does not retry it. A rebind
    error stays in `last_error` after the path is changed back
    ([src/inbound.rs:185-193](src/inbound.rs#L185), [src/api.rs:130-136](src/api.rs#L130)).
  - A single `PUT` that swaps `inbound.path` and `api.path` can never apply live: each new socket
    finds the other's live listener.
  - On a 32-bit target, `FRAME_HEADER_LEN + len` overflows `usize` for a corrupted length near 2^32
    ([src/storage/segment.rs:97](src/storage/segment.rs#L97),
    [src/storage/segment.rs:174](src/storage/segment.rs#L174)). A debug build panics; a release
    build reads it as corruption, which is right. Checked arithmetic removes the difference.

### Design note: a persistent read error

M9 chose to stay at a failing position. H3 currently turns that into a skip after one retry. Once H3 is
fixed, a persistent EIO in a sealed segment will hold a channel until the segment is evicted: days at
low traffic, with only `last_error` and `lag` showing it. A worn sector on SD cards and eMMC is the
likeliest lasting fault of this kind. Recommended: retry for a bounded time, about ten minutes, then
skip the rest of the segment with a `lost` notice and an error log, as for corruption.

### Verified correct

None of these found a defect:
- **Framer:** 20 000 random streams, cut into random chunks, matched a reference. That covers lines
  at `max` and `max + 1` with and without `\r`, splits between `\r` and `\n`, oversized lines followed
  by valid ones, and the buffer's capacity bound.
- **WebSocket window:** 20 000 random sequences of records, notices, acknowledgements and refusals
  matched a model of the cursor, the bytes held and the answers.
- **MQTT tracker:** 20 000 runs against a simulation of rumqttc 0.25.1's packet ids: QoS 0, 1 and 2,
  with PUBREC freeing an id before PUBCOMP, and reconnects that publish again. Every completion matched
  the publish the broker answered, the cursor never passed an unanswered publish, and no packet id
  collided.
- **The log:**
  - A tailing reader racing eviction and limit changes (1 500 runs), and the writer thread beside a
    reader and limit changes (400 runs of 3 000 records with heavy eviction). Every record was
    delivered once, in order, or inside a gap, and `lost` equalled the gaps.
  - Recovery after torn tails, flipped bytes and garbage in the active segment (5 000 runs). The valid
    prefix was kept, numbering continued after it, and the log reopened to the same state. That
    numbering is M12's point when the damage hits committed records.
  - Queries against a filter of the retained records, including `limit`, the page bound and `more`
    (5 000 runs).
  - Receipt-time lookups from any mark (5 000 runs).
  - A stale tail left by a failed truncation, after a restart, was never returned by readers starting
    anywhere, by queries or by lookups.
- **rumqttc 0.25.1:**
  - A request is written and flushed before its `Outgoing::Publish` event, so QoS 0 completes once its
    bytes reach the socket.
  - The request channel holds `inflight` publishes, so `try_publish` never fails on capacity.
  - `MqttOptions::new` and `set_keep_alive` accept every validated value.
- **End-to-end suite:** 10 of 10 runs passed at 16 test threads, beside seven busy loops on seven
  cores.

### Proposed fixes

Three steps, in order of risk; each fix gets a regression test that fails without it.

- **Step 9, the log:** H3, H4, M12, L17, L18, L24, L25 and L27. The model tests of this review join
  the suite. Done; see §8, step 9.
- **Step 10, the API and configuration:** M10, M11, L13, L14, L15, L16, L19, L26 and L28.
- **Step 11, the channels:** M13, M14, L20, L21, L22, L23 and L29, and the decision on the design note.

## Appendix: probes

These ran in a temporary crate outside the repository that depended on `uevtd` by path, so the review
changed nothing in the repo. The crate is not kept; each probe can become a regression test when its
fix lands. p14–p27, of the second review (§9), ran as tests in a scratch copy of `407be9d` and as
scripts against its release binary on the Linux container; none of them is in the repository.

| Probe | Checks | Outcome |
|-------|--------|---------|
| p01 | `sync_interval_ms` 1000 → 0 while a batch waits | committed head 0 after 3 s; 2 only when the writer exits (M1) |
| p02 | 8 MiB message with 64 KiB segments and a 128 KiB limit | accepted; 8.4 MB retained, backlog evicted (M2) |
| p03 | `segment_bytes = 2^63` | debug build panics in validate; release build accepts (L1) |
| p04 | `sync_interval_ms = 18446744073709552` | applied as 384 µs (L2) |
| p05 | one flipped payload byte in the active segment | 7 intact records skipped (L3) |
| p06 | sealed tail overlapping the next segment | real records returned; not a bug (§6) |
| p07 | 20 rounds of two concurrent PATCHes | one change lost in 20/20 rounds (M3) |
| p08 | HTTP with 20 ms of target latency | 44 msg/s (D1) |
| p09 | HTTP 401 for 5 messages, then 200 | 5 rejected for good, never retried (H2) |
| p10 | MQTT with the broker unreachable, log overflowing | 3 075 undelivered messages evicted, `lost=0` (M8) |
| p11 | HTTP 503 while the log overflows, then 200 | `lost=0` during the outage, 3 072 after recovery (M8) |
| p12 | a sealed segment made unreadable while the channel delivers | 59 records before it dropped silently, channel stalled until the mode was restored (M9) |
| p13 | `contrib/config.example.json` through `config::load` | loads and validates; not a bug (§6) |
| p14 | every read delayed 1 s by strace; four queries, each abandoned after 0.5 s | four threads reading at once (L13) |
| p15 | release binary, one EIO injected into a query's second read | 200 with records 131–2 082 missing, "sequence mismatch" logged (H3) |
| p16 | a segment cursor whose read fails once at record 126 | the retry returns record 1 (H3) |
| p17 | a channel's reader after one transient read failure | 1 912 intact records skipped and counted lost (H3) |
| p18 | a second instance on the same log, once without channels and 60 times during a burst | the first one's cursor file deleted; 45 committed records lost to its channel, 0 in the control run (H4) |
| p19 | idle API connections at a limit of 128 descriptors, producer already connected | 300 of 300 messages dropped, "Too many open files" (M10) |
| p20 | `[]`, `PATCH []`, `PATCH {"outbound": []}` | read as defaults; every channel removed and saved (M11) |
| p21 | committed records 23–25 in the active segment, damaged before a restart | the file deleted or cut back; seq 23, or 25, given out again (M12) |
| p22 | WebSocket over a link passing 32 KiB/s, receiver acknowledging every frame | 5 sessions ended as "no pong" in 20 s; 152 of 190 records sent again (M13) |
| p23 | an MQTT broker that answers CONNECT after 6 s | never connected: "Network timeout" (M14) |
| p24 | a dangling symlink as the configuration path; 120-byte and U+0000 socket paths; two default MQTT channels with and without a host name | link replaced by defaults; both paths validate; valid only without a host name (L15, L16, L14) |
| p25 | the oldest segment made undeletable, `max_bytes` 4 000 | 34 488 bytes in 37 segments, health OK (L17) |
| p26 | one corrupt segment read by three readers and two queries | `corrupt_segments` 5 (L18) |
| p27 | model tests: Framer, WebSocket window, MQTT tracker, reader and eviction, recovery, queries, lookups, stale tail, writer race; end-to-end suite under CPU load | no defect found (§9, "Verified correct") |
