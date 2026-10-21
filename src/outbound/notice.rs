//! Drop notices: what a channel sends in place of records it will never
//! deliver, so that the receiver learns of the gap (ARCHITECTURE.md §6.1).

use serde::Serialize;

/// Every notice encodes to at most this many bytes, as a payload or as a
/// frame, so a channel whose size limit admits this much can send every one.
pub const MAX_NOTICE_BYTES: usize = 256;

/// The reason given for a record above the channel's size limit.
pub const TOO_LARGE: &str = "payload too large";

#[derive(Debug, Serialize)]
#[serde(tag = "event", rename_all = "lowercase")]
pub enum Notice<'a> {
    /// Records that left the log before the channel read them.
    Lost {
        from_seq: u64,
        to_seq: u64,
        count: u64,
    },
    /// A record the channel will not deliver; it stays in the log.
    Rejected { seq: u64, ts: i64, reason: &'a str },
}

impl Notice<'_> {
    /// Records `from..=to` were lost.
    pub fn lost(from: u64, to: u64) -> Self {
        Notice::Lost {
            from_seq: from,
            to_seq: to,
            count: to - from + 1,
        }
    }

    /// `{"notice": {"event": ..., ...}}`, the payload of an MQTT publish.
    pub fn encode(&self) -> Vec<u8> {
        #[derive(Serialize)]
        struct Envelope<'a, 'b> {
            notice: &'a Notice<'b>,
        }
        serde_json::to_vec(&Envelope { notice: self }).expect("a notice serializes")
    }

    /// `{"seq": n, "notice": {...}}`, a WebSocket frame. Like a record's
    /// frame it carries the sequence number to acknowledge: the last one it
    /// stands for.
    pub fn frame(&self, seq: u64) -> String {
        #[derive(Serialize)]
        struct Frame<'a, 'b> {
            seq: u64,
            notice: &'a Notice<'b>,
        }
        serde_json::to_string(&Frame { seq, notice: self }).expect("a notice serializes")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn notices_are_tagged_and_fit_the_bound() {
        assert_eq!(
            Notice::lost(3, 5).encode(),
            br#"{"notice":{"event":"lost","from_seq":3,"to_seq":5,"count":3}}"#
        );
        let rejected = Notice::Rejected {
            seq: 7,
            ts: 1760928000123456,
            reason: TOO_LARGE,
        };
        assert_eq!(
            rejected.encode(),
            br#"{"notice":{"event":"rejected","seq":7,"ts":1760928000123456,"reason":"payload too large"}}"#
        );
        let largest = [
            Notice::Lost {
                from_seq: u64::MAX,
                to_seq: u64::MAX,
                count: u64::MAX,
            },
            Notice::Rejected {
                seq: u64::MAX,
                ts: i64::MIN,
                reason: TOO_LARGE,
            },
        ];
        for notice in largest {
            assert!(notice.encode().len() <= MAX_NOTICE_BYTES, "{notice:?}");
            assert!(
                notice.frame(u64::MAX).len() <= MAX_NOTICE_BYTES,
                "{notice:?}"
            );
        }
        assert_eq!(
            Notice::lost(3, 5).frame(5),
            r#"{"seq":5,"notice":{"event":"lost","from_seq":3,"to_seq":5,"count":3}}"#
        );
    }
}
