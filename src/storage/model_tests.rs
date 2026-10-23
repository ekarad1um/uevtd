//! Randomized tests of the log against a model of what it must hold. A seed
//! replays exactly. `UEVTD_SEEDS` sets how many seeds each test runs; the
//! default is small, since every commit syncs, which takes milliseconds on
//! macOS, while with the log on a tmpfs thousands run in seconds.

use super::tests::frames;
use super::*;
use crate::testing::{Rng, seeds};

fn limits(max_bytes: u64, segment_bytes: u64) -> Limits {
    Limits {
        max_bytes,
        segment_bytes,
        sync_interval: Duration::ZERO,
    }
}

/// A payload that tells which record it belongs to.
fn payload(seq: u64, len: usize) -> Vec<u8> {
    (0..len).map(|i| (seq as usize * 31 + i) as u8).collect()
}

/// What the log was given: from sequence number `base + 1` on, the receipt
/// time each record was stored with, and its payload.
#[derive(Default)]
struct Model {
    base: u64,
    written: Vec<(i64, Vec<u8>)>,
    last_ts: i64,
}

impl Model {
    fn record(&self, seq: u64) -> &(i64, Vec<u8>) {
        &self.written[(seq - self.base - 1) as usize]
    }

    fn head(&self) -> u64 {
        self.base + self.written.len() as u64
    }

    /// Appends a batch that fits an empty segment, now and then with a record
    /// as large as one or larger, and with timestamps that sometimes go back.
    fn append(&mut self, writer: &mut Writer, rng: &mut Rng, segment_bytes: u64, sync: bool) {
        let mut batch = Vec::new();
        let mut bytes = 0u64;
        let room = segment_bytes - HEADER_LEN as u64;
        for _ in 0..1 + rng.below(8) {
            let len = if rng.chance(3) {
                (room as usize).saturating_sub(FRAME_HEADER_LEN) + rng.below(40) as usize
            } else {
                rng.below(120) as usize
            };
            let frame = (FRAME_HEADER_LEN + len) as u64;
            if !batch.is_empty() && bytes + frame > room {
                break;
            }
            bytes += frame;
            let ts = match rng.below(10) {
                0 => self.last_ts - rng.below(50) as i64,
                1 => self.last_ts,
                _ => self.last_ts + rng.below(20) as i64,
            };
            let seq = self.head() + 1 + batch.len() as u64;
            batch.push(Message {
                ts,
                payload: payload(seq, len),
            });
            if frame > room {
                break;
            }
        }
        writer.append(&batch, sync).unwrap();
        for m in batch {
            self.last_ts = self.last_ts.max(m.ts);
            self.written.push((self.last_ts, m.payload));
        }
    }
}

fn segment_files(dir: &Path) -> Vec<u64> {
    let mut names: Vec<u64> = fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter_map(|e| e.file_name().to_str().and_then(segment::parse_file_name))
        .collect();
    names.sort_unstable();
    names
}

/// Every record a reader from `from` returns, and how many it passes over.
fn read_all(storage: &Arc<Storage>, from: u64) -> (Vec<Record>, u64) {
    let mut reader = storage.reader(from);
    let (mut records, mut skipped) = (Vec::new(), 0);
    loop {
        let batch = reader.read(64, 1 << 20).unwrap();
        if batch.records.is_empty() && batch.skipped == 0 {
            return (records, skipped);
        }
        skipped += batch.skipped;
        records.extend(batch.records);
    }
}

fn check_records(model: &Model, records: &[Record], seed: u64) {
    for r in records {
        let (ts, payload) = model.record(r.seq);
        assert_eq!(
            (&r.ts, &r.payload),
            (ts, payload),
            "seed {seed}: record {}",
            r.seq
        );
    }
}

/// A channel's reader beside appends, eviction and limit changes: every
/// record is read once and in order, or passed over in a gap once it is
/// gone, and `lost` counts exactly the gaps.
#[test]
fn a_tailing_reader_sees_every_record_once_or_counts_it_lost() {
    for seed in 0..seeds(8) {
        let mut rng = Rng::new(seed);
        let dir = tempfile::tempdir().unwrap();
        let segment_bytes = 200 + rng.below(1800);
        let max_bytes = segment_bytes * (2 + rng.below(4));
        let storage =
            Arc::new(Storage::open(dir.path(), limits(max_bytes, segment_bytes)).unwrap());
        let mut writer = Writer::new(storage.clone());
        let mut model = Model::default();
        let loss = Arc::new(Loss::default());
        let tail = storage.tail("c", 1, loss.clone());
        let mut reader = tail.reader(1);
        let (mut expect, mut gaps) = (1, 0);
        let mut read = |reader: &mut Reader, rng: &mut Rng, model: &Model| {
            let batch = reader
                .read(1 + rng.below(20) as usize, 1 + rng.below(2000) as usize)
                .unwrap();
            assert!(
                batch.records.is_empty() || batch.skipped == 0,
                "seed {seed}: a gap comes alone"
            );
            if batch.skipped > 0 {
                // Only records that are gone may be passed over.
                let first = storage.status().first_seq;
                assert!(
                    expect + batch.skipped <= first,
                    "seed {seed}: skipped {} from {expect}, first {first}",
                    batch.skipped
                );
                expect += batch.skipped;
                gaps += batch.skipped;
            }
            for r in &batch.records {
                assert_eq!(r.seq, expect, "seed {seed}: in order");
                expect += 1;
            }
            check_records(model, &batch.records, seed);
            !batch.records.is_empty() || batch.skipped > 0
        };
        for _ in 0..rng.below(120) {
            if rng.chance(60) {
                model.append(&mut writer, &mut rng, segment_bytes, true);
            }
            if rng.chance(5) {
                let max = segment_bytes * (2 + rng.below(4));
                storage.set_limits(limits(max, segment_bytes));
            }
            if rng.chance(50) {
                read(&mut reader, &mut rng, &model);
            }
        }
        while read(&mut reader, &mut rng, &model) {}
        assert_eq!(expect, model.head() + 1, "seed {seed}: caught up");
        assert_eq!(
            loss.records(),
            gaps,
            "seed {seed}: lost counts what was passed over"
        );
    }
}

/// Whether the zeros that end `bytes`, from `start` on, begin where a torn
/// write leaves them for a record that ends at `end`: the rule of
/// `torn_into_zeros`, from the bytes.
fn zeros_tear(bytes: &[u8], start: usize, end: usize) -> bool {
    let zeros_from = bytes[start..]
        .iter()
        .rposition(|&b| b != 0)
        .map_or(start, |i| start + i + 1);
    zeros_from == start || zeros_from.next_multiple_of(512) < end
}

/// The active segment damaged in one of the ways a crash or the storage can
/// damage it, then opened again. The records before the damage are kept;
/// numbering goes no further past them than the bytes after them could hold;
/// a torn tail costs no sequence number; and other damage never lets a
/// number that a record in the file has be given out again. Then appending,
/// reading and opening once more agree with the model.
#[test]
fn recovery_keeps_what_is_intact_and_never_gives_a_written_number_out_again() {
    for seed in 0..seeds(25) {
        let mut rng = Rng::new(seed);
        let dir = tempfile::tempdir().unwrap();
        let segment_bytes = 200 + rng.below(1800);
        let l = limits(segment_bytes * 1000, segment_bytes);
        let mut model = Model::default();
        {
            let storage = Arc::new(Storage::open(dir.path(), l).unwrap());
            let mut writer = Writer::new(storage.clone());
            for _ in 0..rng.below(30) {
                let sync = !rng.chance(25);
                model.append(&mut writer, &mut rng, segment_bytes, sync);
            }
            // The daemon stops here without syncing what it wrote last.
        }
        let names = segment_files(dir.path());
        let Some(&last) = names.last() else { continue };
        let path = dir.path().join(segment::file_name(last));
        let bytes = fs::read(&path).unwrap();
        let held = frames(&bytes);
        let len = bytes.len();
        let mut damaged = bytes.clone();
        let kind = match rng.below(5) {
            0 => {
                damaged.truncate(HEADER_LEN + rng.below((len - HEADER_LEN) as u64 + 1) as usize);
                "torn"
            }
            1 if len > HEADER_LEN => {
                let at = HEADER_LEN + rng.below((len - HEADER_LEN) as u64) as usize;
                damaged[at] ^= 1 + rng.below(255) as u8;
                "flipped"
            }
            2 => {
                damaged.extend((0..1 + rng.below(60)).map(|_| rng.below(256) as u8));
                "garbage"
            }
            3 => {
                // From a block boundary or the start of a record on.
                let starts: Vec<usize> = (1..)
                    .map(|b| b * 512)
                    .take_while(|&b| b < len)
                    .chain(held.iter().map(|f| f.1))
                    .collect();
                if !starts.is_empty() {
                    damaged[starts[rng.below(starts.len() as u64) as usize]..].fill(0);
                }
                "zeros"
            }
            _ => "none",
        };
        fs::write(&path, &damaged).unwrap();
        // The records still as written, and the one the damage hit.
        let intact = held
            .iter()
            .take_while(|f| damaged.get(f.1..f.2) == Some(&bytes[f.1..f.2]))
            .count();
        let prefix = held[..intact].last().map_or(last - 1, |f| f.0);
        let prefix_end = held[..intact].last().map_or(HEADER_LEN, |f| f.2);
        let hit = held.get(intact);

        let storage = Arc::new(Storage::open(dir.path(), l).unwrap());
        let head = storage.head_seq();
        let room = ((damaged.len() - prefix_end) / FRAME_HEADER_LEN) as u64;
        assert!(
            head >= prefix && head <= prefix + room,
            "seed {seed} {kind}: head {head}, prefix {prefix}, room {room}"
        );
        match (kind, hit) {
            ("torn" | "zeros" | "none", _) => assert_eq!(head, prefix, "seed {seed} {kind}"),
            ("flipped", Some(&(seq, start, _))) => {
                // Only the last record can look torn: when its length now
                // runs past the end, or its end turned to zeros.
                let claimed = u32::from_le_bytes(damaged[start + 4..start + 8].try_into().unwrap());
                let end = start + FRAME_HEADER_LEN + claimed as usize;
                let looks_torn = end > len || zeros_tear(&damaged, start, end);
                if seq == held.last().unwrap().0 && looks_torn {
                    assert_eq!(head, prefix, "seed {seed} {kind}");
                } else {
                    assert!(
                        head >= held.last().unwrap().0,
                        "seed {seed} {kind}: head {head}"
                    );
                }
            }
            _ => {}
        }
        // Readers return the records before the damage as written, and
        // nothing after it.
        let (records, _) = read_all(&storage, 1);
        let seqs: Vec<u64> = records.iter().map(|r| r.seq).collect();
        assert_eq!(seqs, (1..=prefix).collect::<Vec<_>>(), "seed {seed} {kind}");
        check_records(&model, &records, seed);

        // Appending continues right after the head.
        let mut after = Model {
            base: head,
            written: Vec::new(),
            last_ts: storage.status().head_ts,
        };
        let mut writer = Writer::new(storage.clone());
        for _ in 0..1 + rng.below(5) {
            after.append(&mut writer, &mut rng, segment_bytes, true);
        }
        let (records, skipped) = read_all(&storage, head + 1);
        assert_eq!(
            (records.len(), skipped),
            (after.written.len(), 0),
            "seed {seed} {kind}"
        );
        check_records(&after, &records, seed);
        drop(writer);
        drop(storage);
        // And opening again finds the same.
        let storage = Arc::new(Storage::open(dir.path(), l).unwrap());
        assert_eq!(
            storage.head_seq(),
            after.head(),
            "seed {seed} {kind}: reopened"
        );
        let (records, skipped) = read_all(&storage, 1);
        assert_eq!(
            (records.len() as u64, skipped),
            (prefix + after.written.len() as u64, head - prefix),
            "seed {seed} {kind}"
        );
    }
}

/// Queries against a filter over the records the log retains, with every
/// combination of bounds, `after`, `limit` and page size.
#[test]
fn queries_match_a_filter_over_the_retained_records() {
    for seed in 0..seeds(8) {
        let mut rng = Rng::new(seed);
        let dir = tempfile::tempdir().unwrap();
        let segment_bytes = 200 + rng.below(1800);
        let max_bytes = segment_bytes * (2 + rng.below(6));
        let storage =
            Arc::new(Storage::open(dir.path(), limits(max_bytes, segment_bytes)).unwrap());
        let mut writer = Writer::new(storage.clone());
        let mut model = Model::default();
        for _ in 0..rng.below(60) {
            model.append(&mut writer, &mut rng, segment_bytes, true);
        }
        let first = storage.status().first_seq;
        let retained: Vec<(u64, i64, usize)> = (first..=model.head())
            .map(|seq| (seq, model.record(seq).0, model.record(seq).1.len()))
            .collect();
        let max_ts = model.last_ts;
        for q in 0..40 {
            let from = rng
                .chance(70)
                .then(|| rng.below(max_ts as u64 + 20) as i64 - 10);
            let to = rng
                .chance(60)
                .then(|| from.unwrap_or(-10) + 1 + rng.below(max_ts as u64 + 20) as i64);
            let after = if rng.chance(50) {
                0
            } else {
                rng.below(model.head() + 3)
            };
            let limit = 1 + rng.below(30) as usize;
            let page_bytes = 1 + rng.below(2000) as usize;
            let matching: Vec<_> = retained
                .iter()
                .filter(|r| {
                    from.is_none_or(|f| r.1 >= f) && to.is_none_or(|t| r.1 < t) && r.0 > after
                })
                .collect();
            let mut want = Vec::new();
            let mut bytes = 0;
            for r in &matching {
                if want.len() >= limit || (!want.is_empty() && bytes + r.2 > page_bytes) {
                    break;
                }
                bytes += r.2;
                want.push(r.0);
            }
            let more = want.len() < matching.len();
            let page = storage.query(from, to, after, limit, page_bytes).unwrap();
            let got: Vec<u64> = page.records.iter().map(|r| r.seq).collect();
            assert_eq!(
                (got, page.more),
                (want, more),
                "seed {seed} query {q}: from {from:?} to {to:?} after {after} limit {limit} bytes {page_bytes}"
            );
        }
    }
}

/// Receipt-time lookups of any sequence number, from wherever the previous
/// lookup left its mark.
#[test]
fn timestamps_are_found_from_any_mark() {
    for seed in 0..seeds(8) {
        let mut rng = Rng::new(seed);
        let dir = tempfile::tempdir().unwrap();
        let segment_bytes = 200 + rng.below(1800);
        let max_bytes = segment_bytes * (2 + rng.below(6));
        let storage =
            Arc::new(Storage::open(dir.path(), limits(max_bytes, segment_bytes)).unwrap());
        let mut writer = Writer::new(storage.clone());
        let mut model = Model::default();
        let mut mark = Mark::default();
        for _ in 0..rng.below(60) {
            model.append(&mut writer, &mut rng, segment_bytes, true);
            let status = storage.status();
            for _ in 0..rng.below(4) {
                let seq = rng.below(model.head() + 3);
                let want = (seq >= status.first_seq && seq >= 1 && seq <= status.head_seq)
                    .then(|| model.record(seq).0);
                assert_eq!(
                    storage.timestamp(seq, &mut mark).unwrap(),
                    want,
                    "seed {seed}: seq {seq}"
                );
            }
        }
    }
}

/// The writer on its own thread, a tailing reader and limit changes at once:
/// every record is read once, in order and as written, or passed over in a
/// gap, and `lost` counts exactly the gaps.
#[test]
fn readers_and_eviction_race_the_writer_without_loss_or_double_counting() {
    for seed in 0..seeds(1) {
        let dir = tempfile::tempdir().unwrap();
        let mut rng = Rng::new(seed);
        let segment_bytes = 300 + rng.below(1500);
        let storage =
            Arc::new(Storage::open(dir.path(), limits(segment_bytes * 3, segment_bytes)).unwrap());
        let total = 1500u64;
        let writer = {
            let storage = storage.clone();
            std::thread::spawn(move || {
                let mut writer = Writer::new(storage);
                let mut rng = Rng::new(seed ^ 0xabc);
                let mut seq = 0u64;
                while seq < total {
                    let n = (1 + rng.below(6)).min(total - seq);
                    let batch: Vec<Message> = (0..n)
                        .map(|i| Message {
                            ts: (seq + i) as i64,
                            payload: payload(seq + i + 1, rng.below(100) as usize),
                        })
                        .collect();
                    writer.append(&batch, rng.chance(70)).unwrap();
                    seq += n;
                }
                writer.sync().unwrap();
            })
        };
        let limiter = {
            let storage = storage.clone();
            std::thread::spawn(move || {
                let mut rng = Rng::new(seed ^ 0xdef);
                for _ in 0..200 {
                    storage.set_limits(limits(segment_bytes * (2 + rng.below(4)), segment_bytes));
                    std::thread::yield_now();
                }
            })
        };
        let loss = Arc::new(Loss::default());
        let tail = storage.tail("c", 1, loss.clone());
        let mut reader = tail.reader(1);
        let (mut expect, mut gaps) = (1u64, 0u64);
        while expect <= total {
            let batch = reader
                .read(1 + rng.below(30) as usize, 1 + rng.below(3000) as usize)
                .unwrap();
            assert!(batch.records.is_empty() || batch.skipped == 0);
            expect += batch.skipped;
            gaps += batch.skipped;
            for r in &batch.records {
                assert_eq!(r.seq, expect, "seed {seed}");
                assert_eq!(
                    r.payload,
                    payload(r.seq, r.payload.len()),
                    "seed {seed}: record {}",
                    r.seq
                );
                expect += 1;
            }
            if rng.chance(5) {
                std::thread::sleep(Duration::from_micros(rng.below(500)));
            }
        }
        writer.join().unwrap();
        limiter.join().unwrap();
        assert_eq!(expect, total + 1, "seed {seed}");
        assert_eq!(
            loss.records(),
            gaps,
            "seed {seed}: counted once, as passed over"
        );
    }
}
