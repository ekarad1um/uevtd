//! uevtd: a message forwarding daemon.
//!
//! Messages are read newline-delimited from a producer's Unix domain socket,
//! are appended to a bounded on-disk log, and are forwarded by independent
//! channel tasks that each keep a durable cursor into that log. See
//! `ARCHITECTURE.md` for the design.

pub mod api;
pub mod config;
pub mod daemon;
pub mod inbound;
pub mod logging;
pub mod metrics;
pub mod outbound;
pub mod storage;
pub mod tls;
pub mod uds;
pub mod view;

#[cfg(test)]
mod testing;

use std::time::{SystemTime, UNIX_EPOCH};

/// Current wall-clock time in microseconds since the Unix epoch.
pub fn now_us() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as i64)
        .unwrap_or(0)
}
