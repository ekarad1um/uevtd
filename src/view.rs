//! The JSON shape of a record, shared by `/v1/messages` and the WebSocket
//! channel's frames.

use std::borrow::Cow;

use serde::Serialize;
use serde_json::value::RawValue;

use crate::storage::Record;

/// `{"seq": n, "ts": us, "data": <payload>}` when the payload is valid JSON,
/// embedded verbatim; `{"seq": n, "ts": us, "text": "<payload>"}` otherwise,
/// with invalid UTF-8 replaced.
#[derive(Serialize)]
pub struct RecordView<'a> {
    seq: u64,
    ts: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<&'a RawValue>,
    #[serde(skip_serializing_if = "Option::is_none")]
    text: Option<Cow<'a, str>>,
}

pub fn record(record: &Record) -> RecordView<'_> {
    let (data, text) = match std::str::from_utf8(&record.payload) {
        Ok(s) => match serde_json::from_str::<&RawValue>(s) {
            Ok(raw) => (Some(raw), None),
            Err(_) => (None, Some(Cow::Borrowed(s))),
        },
        Err(_) => (None, Some(String::from_utf8_lossy(&record.payload))),
    };
    RecordView {
        seq: record.seq,
        ts: record.ts,
        data,
        text,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn json(payload: &[u8]) -> String {
        let record = Record {
            seq: 7,
            ts: -1,
            payload: payload.to_vec(),
        };
        serde_json::to_string(&super::record(&record)).unwrap()
    }

    #[test]
    fn json_is_embedded_and_anything_else_is_text() {
        assert_eq!(
            json(br#"{"a": [1]}"#),
            r#"{"seq":7,"ts":-1,"data":{"a": [1]}}"#
        );
        assert_eq!(json(br#""abc""#), r#"{"seq":7,"ts":-1,"data":"abc"}"#);
        assert_eq!(json(b"abc"), r#"{"seq":7,"ts":-1,"text":"abc"}"#);
        assert_eq!(
            json(b"a\xffb"),
            "{\"seq\":7,\"ts\":-1,\"text\":\"a\u{fffd}b\"}"
        );
    }
}
