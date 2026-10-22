//! Composition root: wires the components together and owns the shutdown order.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Context;
use tokio::signal::unix::{Signal, SignalKind, signal};
use tokio::sync::{Semaphore, watch};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::config::{self, Config};
use crate::outbound::Registry;
use crate::storage::{self, Limits, Storage};
use crate::{api, inbound, logging, outbound};

/// Bounded queue between connection tasks and the log writer: whichever of
/// the message count and the payload bytes is reached first applies.
const QUEUE_DEPTH: usize = 8192;
const QUEUE_BYTES: u32 = 8 << 20;

pub struct Options {
    pub config_path: PathBuf,
    pub config: Config,
    pub log: Option<logging::Handle>,
}

pub fn limits(cfg: &config::Storage) -> Limits {
    Limits {
        max_bytes: cfg.max_bytes,
        segment_bytes: cfg.segment_bytes,
        sync_interval: Duration::from_millis(cfg.sync_interval_ms),
    }
}

/// Runs the daemon until `shutdown` is cancelled or a component fails at startup.
pub async fn run(opts: Options, shutdown: CancellationToken) -> anyhow::Result<()> {
    // Installed before anything else: until a handler exists, a SIGHUP ends
    // the process, which is its default disposition.
    let hup = signal(SignalKind::hangup()).context("installing the SIGHUP handler")?;
    let Options {
        config_path,
        config,
        log,
    } = opts;
    info!(
        version = env!("CARGO_PKG_VERSION"),
        config = %serde_json::to_string(&config.redacted()).unwrap_or_default(),
        "starting"
    );
    let boot = Arc::new(config.clone());
    let store = Arc::new(config::Store::new(config_path, config.clone()));
    let storage = Arc::new(
        Storage::open(&config.storage.path, limits(&config.storage))
            .with_context(|| format!("opening log at {}", config.storage.path.display()))?,
    );

    let (tx, rx) = storage::queue(QUEUE_DEPTH, QUEUE_BYTES);
    let thread = {
        let storage = storage.clone();
        let rt = tokio::runtime::Handle::current();
        std::thread::Builder::new()
            .name("uevtd-log-writer".into())
            .spawn(move || storage::run_writer(storage, rx, rt))
            .context("spawning log writer thread")?
    };
    // The thread ends once every sender is gone. This function keeps one
    // until inbound has stopped, so an earlier end can only be a panic, which
    // must stop the daemon. Without it, inbound failing at startup would drop
    // the last sender and the writer's exit could be reported as the cause.
    let mut writer = Component::spawn("log writer", async move {
        match tokio::task::spawn_blocking(move || thread.join()).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(_)) => Err(anyhow::anyhow!("thread panicked")),
            Err(e) => Err(anyhow::anyhow!("join failed: {e}")),
        }
    });

    let inbound_metrics = Arc::new(inbound::Metrics::default());
    let registry = Arc::new(Registry::default());
    let state = Arc::new(api::AppState {
        config: store.clone(),
        boot,
        storage: storage.clone(),
        queue: tx.depth(),
        inbound: inbound_metrics.clone(),
        channels: registry.clone(),
        started: Instant::now(),
        last_error: Default::default(),
        queries: Semaphore::new(api::CONCURRENT_QUERIES),
        times: Default::default(),
    });

    let tokens = Tokens::default();
    let mut inbound = Component::spawn(
        "inbound",
        inbound::run(
            store.subscribe(),
            tx.clone(),
            inbound_metrics,
            tokens.inbound.clone(),
        ),
    );
    let mut api = Component::spawn(
        "api",
        api::run(store.subscribe(), state, tokens.api.clone()),
    );
    let supervisor = outbound::run(
        store.subscribe(),
        storage.clone(),
        registry,
        tokens.outbound.clone(),
    );
    let mut outbound = Component::spawn("outbound", async move {
        supervisor.await;
        Ok(())
    });
    let applier = tokio::spawn(apply_changes(
        store.subscribe(),
        storage.clone(),
        log,
        tokens.aux.clone(),
    ));
    let hup = tokio::spawn(reload_on_hup(store.clone(), hup, tokens.aux.clone()));

    let outcome = tokio::select! {
        _ = shutdown.cancelled() => Ok(()),
        r = inbound.wait() => r,
        r = api.wait() => r,
        r = outbound.wait() => r,
        r = writer.wait() => r,
    };

    info!("shutting down");
    inbound.stop(&tokens.inbound).await;
    // The last sender: the writer drains what is queued, syncs and stops.
    drop(tx);
    writer.join().await;
    outbound.stop(&tokens.outbound).await;
    api.stop(&tokens.api).await;
    tokens.aux.cancel();
    let _ = applier.await;
    let _ = hup.await;
    outcome
}

#[derive(Default)]
struct Tokens {
    inbound: CancellationToken,
    outbound: CancellationToken,
    api: CancellationToken,
    aux: CancellationToken,
}

/// A component task whose result is reported once, whether it ends on its own
/// (a startup failure) or after being asked to stop.
struct Component {
    name: &'static str,
    task: Option<JoinHandle<anyhow::Result<()>>>,
}

impl Component {
    fn spawn(
        name: &'static str,
        future: impl Future<Output = anyhow::Result<()>> + Send + 'static,
    ) -> Self {
        Component {
            name,
            task: Some(tokio::spawn(future)),
        }
    }

    /// Resolves when the component ends by itself; always an error, since a
    /// component only ends early when it could not start or lost its thread.
    async fn wait(&mut self) -> anyhow::Result<()> {
        let Some(task) = self.task.as_mut() else {
            return std::future::pending().await;
        };
        let result = task.await;
        self.task = None;
        match result {
            Ok(Ok(())) => Err(anyhow::anyhow!("{} exited unexpectedly", self.name)),
            Ok(Err(e)) => Err(e.context(self.name)),
            Err(e) => Err(anyhow::anyhow!("{} task failed: {e}", self.name)),
        }
    }

    async fn stop(&mut self, token: &CancellationToken) {
        token.cancel();
        self.join().await;
    }

    /// Waits for a component that was asked to stop and logs how it ended.
    async fn join(&mut self) {
        if let Some(task) = self.task.take() {
            match task.await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => warn!(
                    component = self.name,
                    error = format!("{e:#}"),
                    "stopped with error"
                ),
                Err(e) => error!(component = self.name, error = %e, "task failed"),
            }
        }
    }
}

/// Applies the configuration fields that are not owned by a component task:
/// storage limits and the log filter.
async fn apply_changes(
    mut cfg: watch::Receiver<Arc<Config>>,
    storage: Arc<Storage>,
    log: Option<logging::Handle>,
    token: CancellationToken,
) {
    let mut current = cfg.borrow_and_update().clone();
    loop {
        tokio::select! {
            changed = cfg.changed() => if changed.is_err() { break },
            _ = token.cancelled() => break,
        }
        let next = cfg.borrow_and_update().clone();
        if next.storage != current.storage {
            let (storage, limits) = (storage.clone(), limits(&next.storage));
            if let Err(e) = tokio::task::spawn_blocking(move || storage.set_limits(limits)).await {
                error!(error = %e, "storage limits not applied");
            }
            if next.storage.path != current.storage.path {
                warn!(path = %next.storage.path.display(), "storage.path changed; takes effect after restart");
            }
        }
        if next.log.filter != current.log.filter {
            match log.as_ref().map(|h| h.set_filter(&next.log.filter)) {
                Some(Ok(())) => info!(filter = %next.log.filter, "log filter updated"),
                Some(Err(e)) => error!(error = %e, "log filter not applied"),
                None => {}
            }
        }
        if next.log.format != current.log.format {
            warn!("log.format changed; takes effect after restart");
        }
        current = next;
    }
}

async fn reload_on_hup(store: Arc<config::Store>, mut hup: Signal, token: CancellationToken) {
    loop {
        tokio::select! {
            Some(()) = hup.recv() => match store.reload() {
                Ok(_) => info!(path = %store.path().display(), "configuration reloaded from file"),
                Err(e) => error!(error = %e, "reload failed; keeping current configuration"),
            },
            _ = token.cancelled() => break,
        }
    }
}
