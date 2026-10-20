//! On-disk encoding of segments and records (ARCHITECTURE.md §5.2), plus a
//! positioned decoder used by readers and recovery.
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
            return Err(Corrupt("short segment header"));
        }
        if u32_at(bytes, 0) != MAGIC {
            return Err(Corrupt("bad segment magic"));
        }
        if u32_at(bytes, 4) != VERSION {
            return Err(Corrupt("unsupported segment version"));
        }
        Ok(Header {
            first_seq: u64_at(bytes, 8),
            base_ts: u64_at(bytes, 16) as i64,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct Corrupt(pub &'static str);

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
    let end = FRAME_HEADER_LEN + u32_at(buf, 4) as usize;
    if buf.len() < end {
        return Ok(Frame::Incomplete);
    }
    if crc32fast::hash(&buf[4..end]) != u32_at(buf, 0) {
        return Err(Corrupt("record checksum mismatch"));
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
        let avail = (bound - self.pos) as usize;
        if avail < FRAME_HEADER_LEN {
            return Ok(Peek::Corrupt(Corrupt("truncated record header")));
        }
        if !self.fill(FRAME_HEADER_LEN, bound)? {
            return Ok(Peek::Corrupt(Corrupt(
                "segment shorter than its committed length",
            )));
        }
        let head = self.view(FRAME_HEADER_LEN);
        let len = FRAME_HEADER_LEN + u32_at(head, 4) as usize;
        if len > avail {
            return Ok(Peek::Corrupt(Corrupt(
                "record extends past committed length",
            )));
        }
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
        if !self.fill(len, bound)? {
            return Ok(Next::Corrupt(Corrupt(
                "segment shorter than its committed length",
            )));
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
            Ok(Frame::Incomplete) => Ok(Next::Corrupt(Corrupt("incomplete record"))),
            Err(c) => Ok(Next::Corrupt(c)),
        }
    }

    fn view(&self, len: usize) -> &[u8] {
        let off = (self.pos - self.buf_pos) as usize;
        &self.buf[off..off + len]
    }

    /// Ensures `need` bytes from `pos` are buffered; false if the file ends first.
    fn fill(&mut self, need: usize, bound: u64) -> io::Result<bool> {
        if self.pos >= self.buf_pos && (self.pos - self.buf_pos) as usize + need <= self.buf.len() {
            return Ok(true);
        }
        let want = need.max(CHUNK).min((bound - self.pos) as usize);
        self.buf.resize(want, 0);
        self.buf_pos = self.pos;
        match self.file.read_exact_at(&mut self.buf, self.pos) {
            Ok(()) => Ok(true),
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                self.buf.clear();
                Ok(false)
            }
            Err(e) => Err(e),
        }
    }
}

/// Result of validating a segment's records from its header to `file_len`.
pub struct Scan {
    /// Length of the valid prefix, including the segment header.
    pub valid_len: u64,
    pub count: u64,
    pub last: Option<(u64, i64)>,
    pub error: Option<String>,
}

pub fn scan(file: File, file_len: u64, first_seq: u64) -> io::Result<Scan> {
    let mut cursor = SegmentCursor::new(file, HEADER_LEN as u64);
    let mut scan = Scan {
        valid_len: HEADER_LEN as u64,
        count: 0,
        last: None,
        error: None,
    };
    let mut expected = first_seq;
    loop {
        match cursor.next(file_len)? {
            Next::Record(r) if r.seq == expected => {
                scan.last = Some((r.seq, r.ts));
                scan.count += 1;
                scan.valid_len = cursor.pos();
                expected += 1;
            }
            Next::Record(r) => {
                scan.error = Some(format!(
                    "sequence {} where {} was expected",
                    r.seq, expected
                ));
                break;
            }
            Next::End => break,
            Next::Corrupt(c) => {
                scan.error = Some(c.to_string());
                break;
            }
        }
    }
    Ok(scan)
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
        let scan = scan(File::open(&path).unwrap(), bytes.len() as u64, 10).unwrap();
        assert_eq!(scan.valid_len, full as u64);
        assert_eq!(scan.count, 2);
        assert_eq!(scan.last, Some((11, 2)));
        assert!(scan.error.is_some());
    }
}
