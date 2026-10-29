use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use tokio::signal::unix::{SignalKind, signal};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use uevtd::daemon::{self, Options};
use uevtd::{config, logging};

const DEFAULT_CONFIG: &str = "/etc/uevtd/config.json";

const USAGE: &str = "\
usage: uevtd [--config PATH]

  -c, --config PATH   configuration file (default: /etc/uevtd/config.json);
                      written with defaults when nothing is at the path
  -V, --version       print the version
  -h, --help          print this help";

fn main() -> ExitCode {
    tune_allocator();
    let mut config_path = PathBuf::from(DEFAULT_CONFIG);
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" | "-c" => match args.next() {
                Some(path) => config_path = PathBuf::from(path),
                None => return usage_error("--config requires a path"),
            },
            "--help" | "-h" => {
                println!("{USAGE}");
                return ExitCode::SUCCESS;
            }
            "--version" | "-V" => {
                println!("uevtd {}", env!("CARGO_PKG_VERSION"));
                return ExitCode::SUCCESS;
            }
            other => return usage_error(&format!("unknown argument {other:?}")),
        }
    }

    let config = match config::load_or_init(&config_path) {
        Ok(config) => config,
        Err(e) => {
            eprintln!("uevtd: {}: {e}", config_path.display());
            return ExitCode::FAILURE;
        }
    };
    let log = match logging::init(&config.log) {
        Ok(handle) => handle,
        Err(e) => {
            eprintln!("uevtd: logging: {e}");
            return ExitCode::FAILURE;
        }
    };
    let default_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        error!(%info, "panic");
        default_hook(info);
    }));
    raise_file_limit();

    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("uevtd")
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            error!(error = %e, "cannot start runtime");
            return ExitCode::FAILURE;
        }
    };
    let outcome = runtime.block_on(async {
        let shutdown = CancellationToken::new();
        tokio::spawn(wait_for_signal(shutdown.clone()));
        daemon::run(
            Options {
                config_path,
                config,
                log: Some(log),
            },
            shutdown,
        )
        .await
    });
    runtime.shutdown_timeout(Duration::from_secs(5));
    match outcome {
        Ok(()) => {
            info!("stopped");
            ExitCode::SUCCESS
        }
        Err(e) => {
            error!(error = format!("{e:#}"), "fatal");
            ExitCode::FAILURE
        }
    }
}

/// glibc keeps memory that large buffers free: it raises its mmap threshold
/// after every large free and gives each thread its own heap, so a burst of
/// large messages would leave the process far above what it uses. A fixed
/// threshold hands large buffers back to the system when they are freed, and
/// two arenas bound the rest.
#[cfg(all(target_os = "linux", target_env = "gnu"))]
fn tune_allocator() {
    // SAFETY: mallopt only changes allocator parameters, and no other thread
    // exists yet.
    unsafe {
        libc::mallopt(libc::M_MMAP_THRESHOLD, 128 << 10);
        libc::mallopt(libc::M_ARENA_MAX, 2);
    }
}

#[cfg(not(all(target_os = "linux", target_env = "gnu")))]
fn tune_allocator() {}

/// Raises the soft limit on open files to the hard one. The daemon's own
/// limits keep it well below the usual 1024; this is the margin for what
/// they miss, since the log writer and inbound's connection are what fail
/// when the descriptors run out.
fn raise_file_limit() {
    let mut limit = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: the pointer is valid for the call.
    if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) } != 0
        || limit.rlim_cur >= limit.rlim_max
        // macOS has no hard limit to report, and refuses more than its own.
        || limit.rlim_max == libc::RLIM_INFINITY
    {
        return;
    }
    let soft = limit.rlim_cur;
    limit.rlim_cur = limit.rlim_max;
    // SAFETY: as above.
    if unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &limit) } == 0 {
        info!(
            from = soft,
            to = limit.rlim_max,
            "raised the open file limit"
        );
    } else {
        let e = std::io::Error::last_os_error();
        warn!(error = %e, soft, hard = limit.rlim_max, "could not raise the open file limit");
    }
}

fn usage_error(message: &str) -> ExitCode {
    eprintln!("uevtd: {message}\n{USAGE}");
    ExitCode::from(2)
}

async fn wait_for_signal(shutdown: CancellationToken) {
    let (Ok(mut term), Ok(mut int)) = (
        signal(SignalKind::terminate()),
        signal(SignalKind::interrupt()),
    ) else {
        error!("cannot install signal handlers");
        return;
    };
    tokio::select! {
        _ = term.recv() => info!("received SIGTERM"),
        _ = int.recv() => info!("received SIGINT"),
    }
    shutdown.cancel();
    tokio::select! {
        _ = term.recv() => {}
        _ = int.recv() => {}
    }
    warn!("second signal received; exiting without waiting for shutdown");
    std::process::exit(130);
}
