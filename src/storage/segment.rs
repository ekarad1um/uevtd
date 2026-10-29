//! On-disk encoding of segments and records, plus a positioned decoder used by
//! readers and recovery.
//!
//! Segment header (24 bytes): magic, version, first_seq, base_ts.
//! Record frame: crc32 | len | seq | ts | payload, with the CRC covering
//! everything after itself.

use std::fs::File;
use std::io;
use std::os::unix::fs::FileExt;

pub const MAGIC: u32 = 0x5456_4555; // "UEVT"
pub const VERSION: u32 = 1;
pub const HEADER_LEN: usize = 24;
pub const FRAME_HEADER_LEN: usize = 24;
pub const SUFFIX: &str = ".seg";
const CHUNK: usize = 64 * 1024;
/// A longer frame has its checksum computed a chunk at a time before it is
/// read whole, so that a corrupt length never costs an allocation as large as
/// the segment. Every message the daemon accepts is shorter.
const LARGE_FRAME: usize = 1 << 20;
const SHORT_FILE: Corrupt = Corrupt::Truncated("segment shorter than its committed length");

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Header {
    pub first_seq: u64,
    pub base_ts: i64,
}

impl Header {
    pub fn encode(&self) -> [u8; HEADER_LEN] {
        let mut out = [0u8; HEADER_LEN];
        out[0..4].copy_from_slice(&MAGIC.to_le_bytes());
        out[4..8].copy_from_slice(&VERSION.to_le_bytes());
        out[8..16].copy_from_slice(&self.first_seq.to_le_bytes());
        out[16..24].copy_from_slice(&self.base_ts.to_le_bytes());
        out
    }

    pub fn decode(bytes: &[u8]) -> Result<Header, Corrupt> {
        if bytes.len() < HEADER_LEN {
            return Err(Corrupt::Truncated("short segment header"));
        }
        if u32_at(bytes, 0) != MAGIC {
            return Err(Corrupt::Invalid("bad segment magic"));
        }
        if u32_at(bytes, 4) != VERSION {
            return Err(Corrupt::Invalid("unsupported segment version"));
        }
        Ok(Header {
            first_seq: u64_at(bytes, 8),
            base_ts: u64_at(bytes, 16) as i64,
        })
    }
}

/// Why bytes are not a valid record.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum Corrupt {
    /// The bytes end inside the record, as a crash in the middle of a write
    /// leaves a file.
    #[error("{0}")]
    Truncated(&'static str),
    /// The bytes are there, and they are not a valid record.
    #[error("{0}")]
    Invalid(&'static str),
}

pub fn file_name(first_seq: u64) -> String {
    format!("{first_seq:020}{SUFFIX}")
}

pub fn parse_file_name(name: &str) -> Option<u64> {
    let stem = name.strip_suffix(SUFFIX)?;
    (stem.len() == 20 && stem.bytes().all(|b| b.is_ascii_digit())).then(|| stem.parse().ok())?
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Record {
    pub seq: u64,
    pub ts: i64,
    pub payload: Vec<u8>,
}

pub fn encode_frame(out: &mut Vec<u8>, seq: u64, ts: i64, payload: &[u8]) {
    let start = out.len();
    out.extend_from_slice(&[0u8; 4]);
    out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    out.extend_from_slice(&seq.to_le_bytes());
    out.extend_from_slice(&ts.to_le_bytes());
    out.extend_from_slice(payload);
    let crc = crc32fast::hash(&out[start + 4..]);
    out[start..start + 4].copy_from_slice(&crc.to_le_bytes());
}

pub enum Frame<'a> {
    Record {
        seq: u64,
        ts: i64,
        payload: &'a [u8],
    },
    /// More bytes are needed to decide.
    Incomplete,
}

pub fn decode_frame(buf: &[u8]) -> Result<Frame<'_>, Corrupt> {
    if buf.len() < FRAME_HEADER_LEN {
        return Ok(Frame::Incomplete);
    }
    // Checked, since a corrupt length can wrap around on a 32-bit target.
    let end = FRAME_HEADER_LEN.checked_add(u32_at(buf, 4) as usize);
    let Some(end) = end.filter(|&end| end <= buf.len()) else {
        return Ok(Frame::Incomplete);
    };
    if crc32fast::hash(&buf[4..end]) != u32_at(buf, 0) {
        return Err(Corrupt::Invalid("record checksum mismatch"));
    }
    Ok(Frame::Record {
        seq: u64_at(buf, 8),
        ts: u64_at(buf, 16) as i64,
        payload: &buf[FRAME_HEADER_LEN..end],
    })
}

fn u32_at(b: &[u8], i: usize) -> u32 {
    u32::from_le_bytes(b[i..i + 4].try_into().unwrap())
}

fn u64_at(b: &[u8], i: usize) -> u64 {
    u64::from_le_bytes(b[i..i + 8].try_into().unwrap())
}

/// Reads `buf.len()` bytes at `pos`; false if the file ends first.
fn read_at(file: &File, buf: &mut [u8], pos: u64) -> io::Result<bool> {
    match file.read_exact_at(buf, pos) {
        Ok(()) => Ok(true),
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => Ok(false),
        Err(e) => Err(e),
    }
}

pub enum Next {
    Record(Record),
    /// The committed bound was reached.
    End,
    Corrupt(Corrupt),
}

pub enum Peek {
    Frame { seq: u64, len: usize },
    End,
    Corrupt(Corrupt),
}

/// Sequential decoder over one segment file, reading with `pread` in chunks
/// and never looking past the caller-supplied committed bound.
pub struct SegmentCursor {
    file: File,
    pos: u64,
    buf: Vec<u8>,
    buf_pos: u64,
}

impl SegmentCursor {
    pub fn new(file: File, pos: u64) -> Self {
        Self {
            file,
            pos,
            buf: Vec::new(),
            buf_pos: 0,
        }
    }

    pub fn pos(&self) -> u64 {
        self.pos
    }

    pub fn skip(&mut self, len: usize) {
        self.pos += len as u64;
    }

    /// Reads the header of the next frame without consuming it.
    pub fn peek(&mut self, bound: u64) -> io::Result<Peek> {
        if self.pos >= bound {
            return Ok(Peek::End);
        }
        let avail = bound - self.pos;
        if avail < FRAME_HEADER_LEN as u64 {
            return Ok(Peek::Corrupt(Corrupt::Truncated("truncated record header")));
        }
        if !self.fill(FRAME_HEADER_LEN, bound)? {
            return Ok(Peek::Corrupt(SHORT_FILE));
        }
        let head = self.view(FRAME_HEADER_LEN);
        // In u64, so that a corrupt length cannot wrap around on a 32-bit target.
        let len = FRAME_HEADER_LEN as u64 + u64::from(u32_at(head, 4));
        if len > avail {
            return Ok(Peek::Corrupt(Corrupt::Truncated(
                "record extends past committed length",
            )));
        }
        let Ok(len) = usize::try_from(len) else {
            return Ok(Peek::Corrupt(Corrupt::Invalid("record too long to read")));
        };
        Ok(Peek::Frame {
            seq: u64_at(head, 8),
            len,
        })
    }

    /// Decodes and consumes the next frame.
    pub fn next(&mut self, bound: u64) -> io::Result<Next> {
        let len = match self.peek(bound)? {
            Peek::Frame { len, .. } => len,
            Peek::End => return Ok(Next::End),
            Peek::Corrupt(c) => return Ok(Next::Corrupt(c)),
        };
        if len > LARGE_FRAME {
            return self.next_large(len);
        }
        if !self.fill(len, bound)? {
            return Ok(Next::Corrupt(SHORT_FILE));
        }
        match decode_frame(self.view(len)) {
            Ok(Frame::Record { seq, ts, payload }) => {
                let record = Record {
                    seq,
                    ts,
                    payload: payload.to_vec(),
                };
                self.pos += len as u64;
                Ok(Next::Record(record))
            }
            Ok(Frame::Incomplete) => Ok(Next::Corrupt(Corrupt::Invalid("incomplete record"))),
            Err(c) => Ok(Next::Corrupt(c)),
        }
    }

    /// Decodes a frame longer than [`LARGE_FRAME`], whose header [`Self::peek`]
    /// buffered: the checksum first, from the file a chunk at a time, then the
    /// payload. The payload is read again for that and checked again, since
    /// the copy returned must be the one checked.
    fn next_large(&mut self, len: usize) -> io::Result<Next> {
        let head = self.view(FRAME_HEADER_LEN);
        let (crc, seq, ts) = (u32_at(head, 0), u64_at(head, 8), u64_at(head, 16) as i64);
        let fields: [u8; FRAME_HEADER_LEN - 4] = head[4..].try_into().unwrap();
        let start = self.pos + FRAME_HEADER_LEN as u64;
        let end = self.pos + len as u64;
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&fields);
        let mut chunk = vec![0u8; CHUNK];
        let mut at = start;
        while at < end {
            let n = (end - at).min(CHUNK as u64) as usize;
            if !read_at(&self.file, &mut chunk[..n], at)? {
                return Ok(Next::Corrupt(SHORT_FILE));
            }
            hasher.update(&chunk[..n]);
            at += n as u64;
        }
        if hasher.finalize() != crc {
            return Ok(Next::Corrupt(Corrupt::Invalid("record checksum mismatch")));
        }
        let mut payload = vec![0u8; len - FRAME_HEADER_LEN];
        if !read_at(&self.file, &mut payload, start)? {
            return Ok(Next::Corrupt(SHORT_FILE));
        }
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&fields);
        hasher.update(&payload);
        if hasher.finalize() != crc {
            return Ok(Next::Corrupt(Corrupt::Invalid("record checksum mismatch")));
        }
        self.pos += len as u64;
        Ok(Next::Record(Record { seq, ts, payload }))
    }

    fn view(&self, len: usize) -> &[u8] {
        let off = (self.pos - self.buf_pos) as usize;
        &self.buf[off..off + len]
    }

    /// Ensures `need` bytes from `pos` are buffered, reading up to a chunk;
    /// false if the file ends first. A file that ends within the chunk, before
    /// the bound, still yields what it holds. Whenever it fails it leaves the
    /// buffer empty: the position keeps across an error, so that the read can
    /// be retried, and a retry must read the file again, not what a failed
    /// read left in the buffer.
    fn fill(&mut self, need: usize, bound: u64) -> io::Result<bool> {
        if self.pos >= self.buf_pos
            && self.pos - self.buf_pos + need as u64 <= self.buf.len() as u64
        {
            return Ok(true);
        }
        let avail = usize::try_from(bound - self.pos).unwrap_or(usize::MAX);
        self.buf.resize(need.max(CHUNK).min(avail), 0);
        self.buf_pos = self.pos;
        let mut got = 0;
        let filled = loop {
            if got == self.buf.len() {
                break Ok(true);
            }
            match self
                .file
                .read_at(&mut self.buf[got..], self.pos + got as u64)
            {
                Ok(0) => break Ok(got >= need),
                Ok(n) => got += n,
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => break Err(e),
            }
        };
        match filled {
            Ok(true) => self.buf.truncate(got),
            _ => self.buf.clear(),
        }
        filled
    }
}

/// Result of validating a segment's records from its header to `file_len`.
pub struct Scan {
    /// Length of the valid prefix, including the segment header.
    pub valid_len: u64,
    pub count: u64,
    pub last: Option<(u64, i64)>,
    /// Why the valid prefix ends before `file_len`.
    pub stopped: Option<Stopped>,
}

pub enum Stopped {
    /// The file ends inside a record, as a crash in the middle of a write
    /// leaves it.
    Torn(Corrupt),
    /// A record that is there in full, by its length, is invalid: its
    /// checksum does not match, or its sequence number is out of order.
    Invalid {
        error: String,
        /// Where the record ends by its length.
        end: u64,
    },
    /// A read failed, so what follows is unknown.
    Unreadable(io::Error),
}

pub fn scan(file: File, file_len: u64, first_seq: u64) -> Scan {
    let mut cursor = SegmentCursor::new(file, HEADER_LEN as u64);
    let mut scan = Scan {
        valid_len: HEADER_LEN as u64,
        count: 0,
        last: None,
        stopped: None,
    };
    let mut expected = first_seq;
    loop {
        // The header is buffered by then, so this costs no read.
        let end = match cursor.peek(file_len) {
            Ok(Peek::Frame { len, .. }) => cursor.pos() + len as u64,
            _ => file_len,
        };
        let stopped = match cursor.next(file_len) {
            Ok(Next::Record(r)) if r.seq == expected => {
                scan.last = Some((r.seq, r.ts));
                scan.count += 1;
                scan.valid_len = cursor.pos();
                expected += 1;
                continue;
            }
            Ok(Next::Record(r)) => Stopped::Invalid {
                error: format!("sequence {} where {} was expected", r.seq, expected),
                end,
            },
            Ok(Next::End) => break,
            Ok(Next::Corrupt(c @ Corrupt::Truncated(_))) => Stopped::Torn(c),
            Ok(Next::Corrupt(c)) => Stopped::Invalid {
                error: c.to_string(),
                end,
            },
            Err(e) => Stopped::Unreadable(e),
        };
        scan.stopped = Some(stopped);
        break;
    }
    scan
}

/// What follows the end of a segment's valid prefix, learned without trusting
/// it.
pub struct Rest {
    /// The intact record with the highest sequence number.
    pub last: Option<Found>,
    /// Where the zeros that end the file begin, as storage reads them where a
    /// write never arrived; the end of the file if its last byte is not zero.
    pub zeros_from: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Found {
    pub seq: u64,
    pub ts: i64,
    /// Where its frame ends.
    pub end: u64,
}

/// Looks through `from..file_len` for intact records: frames whose checksum
/// holds and whose sequence number follows `last_seq` by no more than the
/// bytes since `from` leave room for, one record per frame header. Nothing
/// vouches for the bytes around such a record, so it is never delivered; it
/// only tells recovery which sequence numbers the damaged part may hold. The
/// bytes are read a chunk at a time. Checksums of frames that only look like
/// records are computed too, and payloads crafted to look like many would
/// cost a check each, so checking more than four times the bytes searched is
/// an error, which recovery takes as damage.
pub fn search(file: &File, from: u64, file_len: u64, last_seq: u64) -> io::Result<Rest> {
    const HEADER: u64 = FRAME_HEADER_LEN as u64;
    let mut rest = Rest {
        last: None,
        zeros_from: from,
    };
    let mut budget = (file_len - from)
        .saturating_mul(4)
        .saturating_add(CHUNK as u64);
    let mut buf = vec![0u8; CHUNK];
    // `buf[..buf_len]` holds the bytes from `buf_pos`; those before `checked`
    // were looked at for zeros.
    let (mut buf_pos, mut buf_len, mut checked) = (from, 0usize, from);
    let mut at = from;
    loop {
        if at + HEADER > buf_pos + buf_len as u64 {
            if at >= file_len {
                break;
            }
            let n = (file_len - at).min(CHUNK as u64) as usize;
            if !read_at(file, &mut buf[..n], at)? {
                break;
            }
            (buf_pos, buf_len) = (at, n);
            let fresh = (checked.max(buf_pos) - buf_pos) as usize;
            if let Some(i) = buf[fresh..n].iter().rposition(|&b| b != 0) {
                rest.zeros_from = buf_pos + (fresh + i) as u64 + 1;
            }
            checked = buf_pos + n as u64;
            if at + HEADER > file_len {
                break;
            }
        }
        let i = (at - buf_pos) as usize;
        let seq = u64_at(&buf, i + 8);
        let room = last_seq
            .saturating_add(1)
            .saturating_add((at - from) / HEADER);
        let end = at + HEADER + u64::from(u32_at(&buf, i + 4));
        let candidate = seq > last_seq && seq <= room && end <= file_len;
        if candidate {
            budget = budget.checked_sub(end - at).ok_or_else(|| {
                let searched = file_len - from;
                io::Error::other(format!("more records to check than {searched} bytes hold"))
            })?;
        }
        if candidate && intact(file, &buf[..buf_len], buf_pos, at, end)? {
            rest.last = Some(Found {
                seq,
                ts: u64_at(&buf, i + 16) as i64,
                end,
            });
            rest.zeros_from = end;
            at = end;
        } else {
            at += 1;
        }
    }
    Ok(rest)
}

/// Whether the checksum of the frame at `at..end` holds. `buf`, which holds
/// the bytes from `buf_pos`, covers its header; the rest is read.
fn intact(file: &File, buf: &[u8], buf_pos: u64, at: u64, end: u64) -> io::Result<bool> {
    let i = (at - buf_pos) as usize;
    let buffered = (buf_pos + buf.len() as u64).min(end);
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&buf[i + 4..(buffered - buf_pos) as usize]);
    let mut chunk = Vec::new();
    let mut pos = buffered;
    while pos < end {
        let n = (end - pos).min(CHUNK as u64) as usize;
        chunk.resize(n, 0);
        if !read_at(file, &mut chunk, pos)? {
            return Ok(false);
        }
        hasher.update(&chunk);
        pos += n as u64;
    }
    Ok(hasher.finalize() == u32_at(buf, i))
}

#[cfg(test)]
impl SegmentCursor {
    /// Swaps the file the cursor reads, as a failing device would fail it.
    pub(super) fn replace_file(&mut self, file: File) -> File {
        std::mem::replace(&mut self.file, file)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn header_round_trip() {
        let h = Header {
            first_seq: 42,
            base_ts: -7,
        };
        assert_eq!(Header::decode(&h.encode()).unwrap(), h);
        let mut bad = h.encode();
        bad[0] ^= 1;
        assert!(Header::decode(&bad).is_err());
    }

    #[test]
    fn file_names() {
        assert_eq!(file_name(7), "00000000000000000007.seg");
        assert_eq!(parse_file_name("00000000000000000007.seg"), Some(7));
        assert_eq!(parse_file_name("7.seg"), None);
        assert_eq!(parse_file_name("00000000000000000007.tmp"), None);
    }

    #[test]
    fn frame_round_trip_and_corruption() {
        let mut buf = Vec::new();
        encode_frame(&mut buf, 5, 99, b"hello");
        assert_eq!(buf.len(), FRAME_HEADER_LEN + 5);
        match decode_frame(&buf).unwrap() {
            Frame::Record { seq, ts, payload } => {
                assert_eq!((seq, ts, payload), (5, 99, &b"hello"[..]));
            }
            Frame::Incomplete => panic!("complete frame"),
        }
        assert!(matches!(
            decode_frame(&buf[..10]).unwrap(),
            Frame::Incomplete
        ));
        assert!(matches!(
            decode_frame(&buf[..buf.len() - 1]).unwrap(),
            Frame::Incomplete
        ));
        let mut bad = buf.clone();
        bad[FRAME_HEADER_LEN] ^= 0xff;
        assert!(decode_frame(&bad).is_err());
        let mut bad = buf.clone();
        bad[9] ^= 1; // seq
        assert!(decode_frame(&bad).is_err());
    }

    /// A segment of `count` records from `first_seq`, each with a payload of
    /// `len` bytes, and the offset of every frame.
    fn segment(first_seq: u64, count: u64, len: usize) -> (Vec<u8>, Vec<usize>) {
        let mut bytes = Header {
            first_seq,
            base_ts: 0,
        }
        .encode()
        .to_vec();
        let mut offsets = Vec::new();
        for seq in first_seq..first_seq + count {
            offsets.push(bytes.len());
            encode_frame(&mut bytes, seq, seq as i64, &vec![b'x'; len]);
        }
        (bytes, offsets)
    }

    #[test]
    fn scan_finds_valid_prefix() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("s");
        let mut bytes = Header {
            first_seq: 10,
            base_ts: 0,
        }
        .encode()
        .to_vec();
        encode_frame(&mut bytes, 10, 1, b"a");
        encode_frame(&mut bytes, 11, 2, b"bb");
        let full = bytes.len();
        encode_frame(&mut bytes, 12, 3, b"ccc");
        bytes.truncate(bytes.len() - 1); // torn tail
        std::fs::File::create(&path)
            .unwrap()
            .write_all(&bytes)
            .unwrap();
        let scan = scan(File::open(&path).unwrap(), bytes.len() as u64, 10);
        assert_eq!(scan.valid_len, full as u64);
        assert_eq!(scan.count, 2);
        assert_eq!(scan.last, Some((11, 2)));
        assert!(matches!(scan.stopped, Some(Stopped::Torn(_))));
    }

    #[test]
    fn scan_tells_a_torn_record_from_an_invalid_one() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("s");
        let (bytes, offsets) = segment(1, 3, 100);
        let stop = |bytes: &[u8]| {
            std::fs::write(&path, bytes).unwrap();
            let scan = scan(File::open(&path).unwrap(), bytes.len() as u64, 1);
            assert_eq!(scan.count, 2);
            scan.stopped
        };
        // Cut inside the header and inside the payload of the last record.
        for cut in [offsets[2] + 10, bytes.len() - 1] {
            assert!(matches!(stop(&bytes[..cut]), Some(Stopped::Torn(_))));
        }
        // A flipped payload byte, and a length that leaves the record short.
        let mut flipped = bytes.clone();
        flipped[offsets[2] + FRAME_HEADER_LEN + 50] ^= 1;
        assert!(matches!(stop(&flipped), Some(Stopped::Invalid { .. })));
        let mut short = bytes.clone();
        short[offsets[2] + 4] = 99;
        assert!(matches!(stop(&short), Some(Stopped::Invalid { .. })));
    }

    #[test]
    fn scan_stops_at_a_read_error() {
        // Reading a directory fails as a bad sector would.
        let dir = tempfile::tempdir().unwrap();
        let scan = scan(File::open(dir.path()).unwrap(), 4096, 10);
        assert_eq!(scan.valid_len, HEADER_LEN as u64);
        assert_eq!((scan.count, scan.last), (0, None));
        assert!(matches!(scan.stopped, Some(Stopped::Unreadable(_))));
    }

    #[test]
    fn a_failed_read_is_read_again_from_the_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("s");
        let (bytes, offsets) = segment(1, 300, 500);
        std::fs::write(&path, &bytes).unwrap();
        let bound = bytes.len() as u64;
        let mut cursor = SegmentCursor::new(File::open(&path).unwrap(), HEADER_LEN as u64);
        // The records the first 64 KiB read holds in full.
        let first_read = offsets
            .iter()
            .take_while(|&&o| o + FRAME_HEADER_LEN + 500 <= HEADER_LEN + CHUNK)
            .count() as u64;
        for seq in 1..=first_read {
            assert!(matches!(cursor.next(bound).unwrap(), Next::Record(r) if r.seq == seq));
        }
        // The next read fails, as a bad block would, and then works again.
        let file = cursor.replace_file(File::open(dir.path()).unwrap());
        assert!(cursor.next(bound).is_err());
        cursor.replace_file(file);
        for seq in first_read + 1..=300 {
            match cursor.next(bound).unwrap() {
                Next::Record(r) => assert_eq!(r.seq, seq),
                Next::End => panic!("end at {seq}"),
                Next::Corrupt(c) => panic!("corrupt at {seq}: {c}"),
            }
        }
        assert!(matches!(cursor.next(bound).unwrap(), Next::End));
    }

    #[test]
    fn a_file_shorter_than_its_bound_yields_every_record_it_holds() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("s");
        let (bytes, _) = segment(1, 5, 100);
        std::fs::write(&path, &bytes).unwrap();
        let bound = bytes.len() as u64 + 10_000;
        let mut cursor = SegmentCursor::new(File::open(&path).unwrap(), HEADER_LEN as u64);
        for seq in 1..=5 {
            assert!(matches!(cursor.next(bound).unwrap(), Next::Record(r) if r.seq == seq));
        }
        assert!(matches!(
            cursor.next(bound).unwrap(),
            Next::Corrupt(SHORT_FILE)
        ));
    }

    #[test]
    fn a_corrupt_length_is_checked_without_buffering_the_record() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("s");
        // A record of 3 MiB, then one whose length was corrupted to 40 MiB,
        // in a sparse file long enough to hold it.
        let mut bytes = Header {
            first_seq: 1,
            base_ts: 0,
        }
        .encode()
        .to_vec();
        let large: Vec<u8> = (0..3 << 20).map(|i| (i % 251) as u8).collect();
        encode_frame(&mut bytes, 1, 1, &large);
        let corrupt = bytes.len();
        encode_frame(&mut bytes, 2, 2, b"small");
        bytes[corrupt + 4..corrupt + 8].copy_from_slice(&(40u32 << 20).to_le_bytes());
        let file = File::create(&path).unwrap();
        file.write_all_at(&bytes, 0).unwrap();
        file.set_len(64 << 20).unwrap();
        let bound = 64 << 20;

        let mut cursor = SegmentCursor::new(File::open(&path).unwrap(), HEADER_LEN as u64);
        match cursor.next(bound).unwrap() {
            Next::Record(r) => assert_eq!((r.seq, r.payload == large), (1, true)),
            _ => panic!("the large record"),
        }
        assert!(matches!(
            cursor.next(bound).unwrap(),
            Next::Corrupt(Corrupt::Invalid(_))
        ));
        assert!(cursor.buf.capacity() <= CHUNK, "{}", cursor.buf.capacity());
    }

    /// `search` over `bytes`, written to a file, from offset `from`.
    fn search_in(bytes: &[u8], from: usize, last_seq: u64) -> Rest {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("s");
        std::fs::write(&path, bytes).unwrap();
        let file = File::open(&path).unwrap();
        search(&file, from as u64, bytes.len() as u64, last_seq).unwrap()
    }

    #[test]
    fn search_finds_the_intact_records_after_damage() {
        // Record 5 damaged; records 6 to 40 follow it, across several chunks.
        let (bytes, offsets) = segment(1, 40, 3000);
        let end = bytes.len() as u64;
        let last = Some(Found {
            seq: 40,
            ts: 40,
            end,
        });
        let mut damaged = bytes.clone();
        damaged[offsets[4] + FRAME_HEADER_LEN + 7] ^= 1;
        let rest = search_in(&damaged, offsets[4], 4);
        assert_eq!((rest.last, rest.zeros_from), (last, end));
        // Its length damaged instead, so that it seems to run past the end.
        let mut long = bytes.clone();
        long[offsets[4] + 7] = 0x7f;
        assert_eq!(search_in(&long, offsets[4], 4).last, last);
        // Only records after `last_seq` count.
        assert_eq!(search_in(&damaged, offsets[4], 40).last, None);
        // A torn record holds none.
        let torn = &bytes[..offsets[39] + 100];
        let rest = search_in(torn, offsets[39], 39);
        assert_eq!((rest.last, rest.zeros_from), (None, torn.len() as u64));
        // Zeros where a record's end should be, and in place of it.
        let mut zeros = bytes[..offsets[5] - 1000].to_vec();
        zeros.resize(offsets[5] + 5000, 0);
        let rest = search_in(&zeros, offsets[4], 4);
        assert_eq!(
            (rest.last, rest.zeros_from),
            (None, offsets[5] as u64 - 1000)
        );
        let rest = search_in(&zeros, offsets[5], 4);
        assert_eq!((rest.last, rest.zeros_from), (None, offsets[5] as u64));
        let rest = search_in(&zeros, zeros.len() - 10, 4);
        assert_eq!(rest.zeros_from, zeros.len() as u64 - 10);
    }

    #[test]
    fn search_gives_up_on_bytes_made_to_look_like_many_records() {
        // Every 16 bytes, a header that claims record 1 and half the file.
        let len = 1 << 20;
        let mut bytes = vec![0u8; HEADER_LEN];
        while bytes.len() < len {
            bytes.extend_from_slice(&[0; 4]);
            bytes.extend_from_slice(&(len as u32 / 2).to_le_bytes());
            bytes.extend_from_slice(&1u64.to_le_bytes());
        }
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("s");
        std::fs::write(&path, &bytes).unwrap();
        let file = File::open(&path).unwrap();
        let started = std::time::Instant::now();
        assert!(search(&file, HEADER_LEN as u64, bytes.len() as u64, 0).is_err());
        assert!(started.elapsed() < std::time::Duration::from_secs(5));
    }

    #[test]
    fn search_takes_only_numbers_the_bytes_leave_room_for() {
        // Records without a payload take 24 bytes each, so the frame after a
        // damaged record 5 can be record 6 and none later.
        let (bytes, offsets) = segment(1, 6, 0);
        let mut damaged = bytes.clone();
        damaged[offsets[4]] ^= 1;
        assert_eq!(
            search_in(&damaged, offsets[4], 4).last.map(|f| f.seq),
            Some(6)
        );
        let mut later = damaged[..offsets[5]].to_vec();
        encode_frame(&mut later, 7, 7, b"");
        assert_eq!(search_in(&later, offsets[4], 4).last, None);
    }
}
