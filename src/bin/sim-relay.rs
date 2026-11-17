//! The relay over a simulated chain, on localhost, for the client SDK's tests: the real REST
//! and WebSocket API, and control routes to drive the chain (see `hergmes::sim::relay`).
//!
//! Prints `{"port": N}` on one stdout line once it listens; logs go to stderr, filtered by
//! `RUST_LOG` (default `warn`). Runs until killed, or until the engine task stops.

use std::io::Write;

use hergmes::sim::relay::{self, SimRelay};
use tokio::net::TcpListener;
use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> std::io::Result<()> {
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn")))
        .with(fmt::layer().with_writer(std::io::stderr))
        .init();

    let (router, _relay, engine_task) = SimRelay::start(relay::chain()).await;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();
    let mut stdout = std::io::stdout().lock();
    writeln!(stdout, "{{\"port\": {port}}}")?;
    stdout.flush()?;
    drop(stdout);

    tokio::select! {
        served = axum::serve(listener, router).into_future() => served,
        ended = engine_task => Err(std::io::Error::other(format!("the engine task stopped: {ended:?}"))),
    }
}
