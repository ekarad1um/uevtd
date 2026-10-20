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

  --config PATH   configuration file (default: /etc/uevtd/config.json);
                  written with defaults when it does not exist
  --version       print the version
  --help          print this help";

fn main() -> ExitCode {
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
