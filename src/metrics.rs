//! Telemetry primitives shared by all components: a last-error slot and a
//! minimal Prometheus text-format writer.

use std::fmt::{Display, Write};
use std::sync::Mutex;

use serde::Serialize;

use crate::now_us;

#[derive(Clone, Debug, Serialize)]
pub struct LastError {
    pub ts: i64,
    pub message: String,
}

/// The most recent error of a component, with the time it happened.
#[derive(Default)]
pub struct ErrorSlot(Mutex<Option<LastError>>);

impl ErrorSlot {
    pub fn set(&self, message: impl Display) {
        *self.0.lock().unwrap() = Some(LastError {
            ts: now_us(),
            message: message.to_string(),
        });
    }

    pub fn clear(&self) {
        *self.0.lock().unwrap() = None;
    }

    pub fn get(&self) -> Option<LastError> {
        self.0.lock().unwrap().clone()
    }
}

/// Builder for a Prometheus text exposition.
#[derive(Default)]
pub struct Exposition {
    out: String,
}

impl Exposition {
    pub fn family<'a>(&'a mut self, name: &'a str, kind: &str, help: &str) -> Family<'a> {
        let _ = writeln!(self.out, "# HELP {name} {help}");
        let _ = writeln!(self.out, "# TYPE {name} {kind}");
        Family {
            out: &mut self.out,
            name,
        }
    }

    pub fn finish(self) -> String {
        self.out
    }
}

pub struct Family<'a> {
    out: &'a mut String,
    name: &'a str,
}

impl Family<'_> {
    pub fn value(&mut self, value: impl Display) {
        self.sample(&[], value);
    }

    pub fn sample(&mut self, labels: &[(&str, &str)], value: impl Display) {
        self.out.push_str(self.name);
        if !labels.is_empty() {
            self.out.push('{');
            for (i, (k, v)) in labels.iter().enumerate() {
                if i > 0 {
                    self.out.push(',');
                }
                let _ = write!(self.out, "{k}=\"");
                for c in v.chars() {
                    match c {
                        '\\' => self.out.push_str("\\\\"),
                        '"' => self.out.push_str("\\\""),
                        '\n' => self.out.push_str("\\n"),
                        c => self.out.push(c),
                    }
                }
                self.out.push('"');
            }
            self.out.push('}');
        }
        let _ = writeln!(self.out, " {value}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_families_and_escapes_labels() {
        let mut e = Exposition::default();
        e.family("a_total", "counter", "A").value(3);
        let mut f = e.family("b", "gauge", "B");
        f.sample(&[("channel", "x\"y")], 1);
        f.sample(&[("channel", "z")], 2);
        assert_eq!(
            e.finish(),
            "# HELP a_total A\n# TYPE a_total counter\na_total 3\n\
             # HELP b B\n# TYPE b gauge\nb{channel=\"x\\\"y\"} 1\nb{channel=\"z\"} 2\n"
        );
    }
}
