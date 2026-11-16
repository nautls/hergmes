use std::{collections::HashMap, sync::Arc, time::Duration};

use dotenvy::dotenv;
use hergmes::{
    adapters::{BoxCache, BoxFetcher, Follower, HeaderReader, TokenChecker, loader::Loader},
    api::{
        Balances, Headers, History, Metrics, Pages,
        http::{self, Api},
        metrics, ws,
    },
    clients::NodeClient,
    config::Config,
    engine::Engine,
    error::AppError,
    runtime::{self, Keeper},
    trace::{self, default_subscriber},
};
use tokio::{net::TcpListener, task::JoinSet};
use tracing::{error, info, warn};

#[tokio::main]
async fn main() -> Result<(), AppError> {
    if let Err(e) = dotenv() {
        warn!("Failed to load .env file: {:?}. Using environment variables instead.", e);
    }
    trace::init(default_subscriber());

    let result = run().await;
    if let Err(error) = &result {
        error!(%error, "Stopping");
    }
    result
}

/// Binds the API's address, starts the engine task, the follower, a keeper per
/// configured key and the server, and runs until the engine is gone or the server stops.
/// The engine task ends only by panicking, which can leave it half-changed, and the
/// follower and keepers then stop with `EngineGone`: nothing can serve, so the process
/// exits, and a restart reloads every key from the node. A keeper that gives up its key
/// (logged), or panics, leaves the relay running: one key's trouble never stops it.
async fn run() -> Result<(), AppError> {
    let config = Config::from_env()?;
    // Bound first, so a port conflict stops the process before anything starts.
    let addr = config.listen_addr;
    let listener = TcpListener::bind(addr)
        .await
        .map_err(|source| AppError::Listen { addr, source })?;
    let metrics_listener = match config.metrics_addr {
        Some(addr) => {
            let listener = TcpListener::bind(addr)
                .await
                .map_err(|source| AppError::Listen { addr, source })?;
            Some((addr, listener))
        }
        None => None,
    };
    let http_client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .expect("Failed to build HTTP client");
    let node = NodeClient::connect(http_client, &config.node_url).await?;
    let keys = config.tracked_keys(node.network())?;
    let policy = config.key_policy(node.network(), &keys)?;
    info!(%policy, "Key policy");

    let (engine, mut engine_task) = runtime::spawn(Engine::new(config.engine));
    // Seeded by every first load, and read by the box fetcher to fill in pages.
    let cache = BoxCache::new(config.box_cache_bytes);
    let counters = Metrics::new(cache.clone());
    info!(
        network = ?node.network(),
        window = config.engine.window,
        grace = config.engine.grace,
        tracked = keys.len(),
        "Following the node"
    );

    let follower = Follower::new(node.clone(), config.follower);
    let synced = follower.synced();
    // Filled by the follower with the headers of the blocks it applies.
    let headers = HeaderReader::new(node.clone(), follower.headers());
    let mut follower = tokio::spawn({
        let engine = engine.clone();
        async move { follower.run(&engine).await }
    });

    // Shared by the keepers and the page service, so their loads of one key coalesce.
    let loader = Arc::new(Loader::new(node.clone(), config.loader).seeding(cache.clone()));
    let tokens = Arc::new(TokenChecker::new(node.clone(), config.token_supply_limit));
    // The server starts at once, for the status route and tracked keys, but loads no key for
    // a client until the configured keys have settled.
    let (settled, pending) = runtime::settling(keys.len());
    let api = Arc::new(Api {
        engine: engine.clone(),
        pages: Pages::new(loader.clone(), tokens.clone(), BoxFetcher::new(node.clone(), cache))
            .waiting_for(settled),
        balances: Balances::new(node.clone(), Default::default()),
        history: History::new(node.clone(), Default::default()),
        headers: Headers::new(headers),
        network: node.network(),
        policy,
        synced,
        metrics: counters.clone(),
    });
    let routes = http::router(api.clone()).merge(ws::router(api, config.ws));
    let mut server = tokio::spawn(axum::serve(listener, routes).into_future());
    info!(%addr, max_subscriptions = config.ws.max_subscriptions, "Serving the REST API and the WebSocket");
    // On its own address, off unless set: operators read it, the public does not.
    let mut metrics_server = metrics_listener.map(|(addr, listener)| {
        info!(%addr, "Serving the metrics");
        let routes = metrics::router(engine.clone(), counters);
        tokio::spawn(axum::serve(listener, routes).into_future())
    });

    let keeper = Arc::new(Keeper::new(engine, loader, tokens, Default::default()));
    let mut keepers = JoinSet::new();
    let mut kept = HashMap::new();
    for (key, pending) in keys.into_iter().zip(pending) {
        let keeper = keeper.clone();
        let task = keepers.spawn({
            let key = key.clone();
            async move { keeper.keep(key, pending).await }
        });
        kept.insert(task.id(), key);
    }
    drop(keeper);

    let gone = loop {
        tokio::select! {
            biased;
            ended = &mut engine_task => {
                ended?;
                // Every handle was dropped, so the follower has ended: report why.
                return Err(follower.await?.into());
            }
            gone = &mut follower => break gone?,
            // It holds an engine handle, so it returns at once rather than wait for the
            // engine task below, which would not end.
            served = &mut server => {
                return Err(AppError::Server(served?.err().unwrap_or_else(|| {
                    std::io::Error::other("it returned without an error")
                })));
            }
            Some(served) = async { Some(metrics_server.as_mut()?.await) } => {
                return Err(AppError::Server(served?.err().unwrap_or_else(|| {
                    std::io::Error::other("the metrics server returned without an error")
                })));
            }
            Some(stopped) = keepers.join_next_with_id() => match stopped {
                Ok((id, Ok(()))) => {
                    kept.remove(&id);
                }
                Ok((_, Err(gone))) => break gone,
                Err(error) => {
                    let key = kept.remove(&error.id());
                    error!(?key, %error, "Keeper of a configured key failed, giving the key up");
                }
            },
        }
    };
    // `EngineGone` means the engine task is ending, but a task that saw it can finish first:
    // the inbox and the pending replies are dropped while the panic unwinds, before the join
    // handle is ready. The panic is the cause worth reporting, so it is waited for.
    engine_task.await?;
    Err(gone.into())
}
