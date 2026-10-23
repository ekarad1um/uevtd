//! Helpers for the tests: a deterministic generator for the randomized ones,
//! and pipes that make a read wait.

use std::ffi::CString;
use std::fs;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};

/// xorshift*. A seed replays exactly.
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Self {
        let mut rng = Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ 0xD1B5_4A32_D192_ED03);
        rng.next();
        rng
    }

    pub fn next(&mut self) -> u64 {
        let mut x = self.0 | 1;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    pub fn below(&mut self, n: u64) -> u64 {
        if n == 0 { 0 } else { self.next() % n }
    }

    pub fn chance(&mut self, percent: u64) -> bool {
        self.below(100) < percent
    }

    pub fn pick<T: Copy>(&mut self, items: &[T]) -> T {
        items[self.below(items.len() as u64) as usize]
    }
}

/// How many seeds a randomized test runs: `UEVTD_SEEDS`, or `default`.
pub fn seeds(default: u64) -> u64 {
    std::env::var("UEVTD_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

/// A pipe in place of a file, which a read waits on until it is opened for
/// writing; dropping this does that, also when a test fails, so that the
/// runtime, which waits for its blocking reads, can stop.
pub struct Pipe(PathBuf);

impl Pipe {
    pub fn new(path: &Path) -> Pipe {
        let name = CString::new(path.as_os_str().as_bytes()).unwrap();
        // SAFETY: a valid path.
        assert_eq!(unsafe { libc::mkfifo(name.as_ptr(), 0o600) }, 0);
        Pipe(path.to_path_buf())
    }
}

impl Drop for Pipe {
    fn drop(&mut self) {
        // A reader that waits already is let go now, before the test's
        // directory, with the pipe in it, is removed.
        let now = fs::OpenOptions::new()
            .write(true)
            .custom_flags(libc::O_NONBLOCK)
            .open(&self.0);
        if now.is_err() {
            // Without one yet, a thread of its own, which nothing joins,
            // waits for a reader that may still come.
            let path = self.0.clone();
            std::thread::spawn(move || fs::OpenOptions::new().write(true).open(path));
        }
    }
}
