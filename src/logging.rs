//! `tracing` subscriber on stderr with a filter that can be changed at runtime.

use std::io::IsTerminal;

use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer, Registry, fmt, reload};

use crate::config::{Log, LogFormat};

pub struct Handle(reload::Handle<EnvFilter, Registry>);

pub fn init(cfg: &Log) -> anyhow::Result<Handle> {
    let (filter, handle) = reload::Layer::new(EnvFilter::try_new(&cfg.filter)?);
    let layer: Box<dyn Layer<Registry> + Send + Sync> = match cfg.format {
        LogFormat::Text => Box::new(
            fmt::layer()
                .with_writer(std::io::stderr)
                .with_ansi(std::io::stderr().is_terminal())
                .with_target(false)
                .with_filter(filter),
        ),
        LogFormat::Json => Box::new(
            fmt::layer()
                .json()
                .with_writer(std::io::stderr)
                .with_filter(filter),
        ),
    };
    tracing_subscriber::registry().with(layer).try_init()?;
    Ok(Handle(handle))
}

impl Handle {
    pub fn set_filter(&self, directives: &str) -> anyhow::Result<()> {
        self.0.reload(EnvFilter::try_new(directives)?)?;
        Ok(())
    }
}
