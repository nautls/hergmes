//! The sim relay: the real REST and WebSocket API over a [`SimChain`], with control routes to
//! drive the chain and the follower and to read the oracle's state for a key. Served on
//! localhost by the `sim-relay` binary, for the client SDK's tests.
//!
//! The follower ticks only when a control route asks, so a run is the same sequence of chain
//! states and engine events every time; only the timing of clients against it varies.
//!
//! Control routes:
//!
//! - `POST /sim/random` `{ "seed": "<u64>" }`: draws one op from [`strategy::op`], seeded by
//!   `seed`, applies it and answers it.
//! - `POST /sim/ops` `[op, ...]`: applies ops given explicitly, to replay a run.
//! - `POST /sim/tick`: one follower tick; answers `{ tip, synced }`.
//! - `POST /sim/settle`: the indexer brought to the tip (no lag, no recovery), then ticks
//!   until the engine is at the node's tip with its mempool; answers `{ tip }`.
//! - `GET /sim/expected/addresses/{address}`, `GET /sim/expected/tokens/{tokenId}`: the
//!   oracle's state for the key at the tip, in the relay's wire shapes: its boxes and overlay,
//!   and for an address its balance and its whole history, as the history route's items. Only
//!   right after a tick or settle that synced, with no op since: `409` otherwise.
//! - `GET /sim/keys`: the chain's trees as addresses, and its tokens with their emission.

use std::{
    sync::{Arc, Mutex, PoisonError},
    time::Duration,
};

use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use proptest::{
    strategy::{Strategy, ValueTree},
    test_runner::{Config as RunnerConfig, RngAlgorithm, TestRng, TestRunner},
};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::{task::JoinHandle, time::Instant};

use crate::{
    adapters::{
        AdapterError, BoxCache, BoxFetcher, Follower, HeaderReader, Loader, TokenChecker,
        engine_tip, follower, loader, mempool,
    },
    api::{
        Balances, Boxes, Headers, History, KeyPolicy, Pages, Tokens, Transactions, balances,
        history,
        http::{self, Api},
        keys::{address_key, token_key},
        protocol::WirePendingTx,
        wire::{AsOf, Confirmed, Pending, WireBox},
        ws,
    },
    engine::{self, BlockRef, Engine, TrackingKey},
    runtime::{Clock, Handle, ManualClock, Settled, spawn_with_clock},
    sim::{
        Chain, HistoryEntry, SimChain, SimConfig, SimOp, reference_delta, strategy, timestamp,
        totals,
    },
    types::address::{self, Network},
};

/// The chain a relay starts on: the sim's defaults, with no recovery reads. The sim counts a
/// recovery down by gated indexer reads, standing in for time; a follower waiting for the
/// indexer makes none, so ticks alone would never pass a reorg that orphaned the indexer's
/// tip, and clients' own reads would decide when it ends, which their timing varies.
pub fn chain() -> Chain {
    Chain::new(SimConfig { recovery_reads: 0, ..SimConfig::default() })
}

/// The engine's settings: a window and grace period small enough that a few ops reach a fork
/// beyond the window and an idle key untracked. The engine's clock moves [`TICK_TIME`] per
/// follower tick, so the grace period is three ticks and eviction waits for one.
pub const ENGINE: engine::Config = engine::Config {
    window: 8,
    grace: Duration::from_secs(3 * TICK_TIME.as_secs()),
    evict_after: TICK_TIME,
    ..engine::Config::DEFAULT
};

/// How far a follower tick moves the engine's clock. Ticks come only when a test asks, so
/// idle times follow what the test does, not how fast it runs. A request sent while a tick
/// runs may land on either side of it, so a replayed seed untracks the same keys only if the
/// test waits for its requests before ticking.
pub const TICK_TIME: Duration = Duration::from_secs(120);

/// The relay's waits for a follower tick (a load's, a balance's, a history page's retries).
/// Ticks come only when a test asks, so waiting longer would only make a request that can't
/// settle answer `not_ready` later.
pub const RETRY: Duration = Duration::from_millis(10);

/// The deepest reorg a drawn op makes; within [`ENGINE`]'s window.
pub const MAX_REORG_DEPTH: u8 = 4;

/// The most ticks a settle takes. With the indexer at the tip one is enough; a second covers
/// a mempool poll whose tip moved.
const SETTLE_TICKS: u32 = 3;

const NETWORK: Network = Network::Mainnet;

/// The chain, the engine and the follower behind the API, driven by the control routes.
pub struct SimRelay {
    sim: SimChain,
    clock: ManualClock,
    engine: Handle,
    /// Locked for a whole tick, so ticks never overlap.
    follower: tokio::sync::Mutex<Follower<SimChain>>,
    checkpoint: Mutex<Checkpoint>,
}

/// Whether the engine was shown the chain as it is now.
#[derive(Debug, Default)]
struct Checkpoint {
    /// Ops applied so far, counted under the chain's lock.
    ops: u64,
    /// The op count at the start of the last tick, if that tick synced.
    synced_at: Option<u64>,
}

impl SimRelay {
    /// A relay over `chain` and its engine task, ticked once so the engine has a tip. The
    /// router serves the relay's API and the control routes.
    pub async fn start(chain: Chain) -> (Router, Arc<Self>, JoinHandle<()>) {
        let sim = SimChain::new(chain);
        let clock = ManualClock::default();
        let (engine, task) = spawn_with_clock(Engine::new(ENGINE), Clock::Manual(clock.clone()));
        let cache = BoxCache::new(1 << 20);
        let follower = Follower::new(
            sim.clone(),
            follower::Config {
                // Never read: the follower does not run on its own here.
                interval: Duration::from_secs(1),
                // Every synced tick hands the engine the pool as it is.
                mempool: mempool::Config { refresh: Duration::ZERO },
            },
        );
        let loader = loader::Config {
            // Ten waits, as the other retry loops make.
            give_up_after: 10 * RETRY,
            retry: RETRY,
            poll: RETRY,
            ..Default::default()
        };
        let api = Arc::new(Api {
            engine: engine.clone(),
            pages: Pages::new(
                Arc::new(Loader::new(sim.clone(), loader).seeding(cache.clone())),
                Arc::new(TokenChecker::new(sim.clone(), 100)),
                BoxFetcher::new(sim.clone(), cache),
            )
            .waiting_for(Settled::now()),
            balances: Balances::new(sim.clone(), balances::Config { attempts: 3, retry: RETRY }),
            history: History::new(sim.clone(), history::Config { attempts: 3, retry: RETRY }),
            headers: Headers::new(HeaderReader::new(sim.clone(), follower.headers())),
            boxes: Boxes::new(sim.clone()),
            tokens: Tokens::new(sim.clone()),
            transactions: Transactions::new(sim.clone()),
            network: NETWORK,
            policy: KeyPolicy::default(),
            synced: follower.synced(),
            metrics: Default::default(),
        });
        let relay = Arc::new(Self {
            sim,
            clock,
            engine,
            follower: tokio::sync::Mutex::new(follower),
            checkpoint: Mutex::default(),
        });
        relay
            .tick()
            .await
            .expect("the engine task runs, and the first tick syncs");
        let router = http::router(api.clone())
            .merge(ws::router(api, ws::Config::default()))
            .merge(relay.clone().control());
        (router, relay, task)
    }

    /// The chain, to inspect it.
    pub fn sim(&self) -> &SimChain {
        &self.sim
    }

    fn checkpoint(&self) -> std::sync::MutexGuard<'_, Checkpoint> {
        self.checkpoint
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// Applies `ops` in order.
    pub fn apply(&self, ops: &[SimOp]) {
        let mut chain = self.sim.lock();
        for op in ops {
            chain.apply(op);
        }
        self.checkpoint().ops += ops.len() as u64;
    }

    /// Draws one op seeded by `seed`, applies it and returns it.
    pub fn random(&self, seed: u64) -> SimOp {
        let op = draw(seed);
        self.apply(&[op]);
        op
    }

    /// One follower tick: the engine's tip after it, and whether the engine reached the
    /// node's tip and took its mempool.
    pub async fn tick(&self) -> Result<(Option<BlockRef>, bool), Gone> {
        let mut follower = self.follower.lock().await;
        self.clock.advance(TICK_TIME);
        let ops = self.checkpoint().ops;
        let synced = match follower.tick(&self.engine, Instant::now()).await {
            Ok(tick) => {
                tick.blocks.is_ok()
                    && matches!(
                        tick.mempool,
                        Some(Ok(mempool::Poll::Snapshot { .. } | mempool::Poll::Unchanged))
                    )
            }
            Err(AdapterError::Node(_)) => false,
            Err(AdapterError::EngineGone(_)) => return Err(Gone),
        };
        self.checkpoint().synced_at = synced.then_some(ops);
        let tip = engine_tip(&self.engine).await.map_err(|_| Gone)?;
        Ok((tip, synced))
    }

    /// Brings the indexer to the tip, ending its lag and any recovery (none on [`chain`]), and
    /// ticks until synced: the engine's tip, or `None` if it did not sync within
    /// [`SETTLE_TICKS`].
    pub async fn settle(&self) -> Result<Option<BlockRef>, Gone> {
        {
            let mut chain = self.sim.lock();
            chain.settle_indexer();
            self.checkpoint().ops += 1;
        }
        for _ in 0..SETTLE_TICKS {
            if let (tip, true) = self.tick().await? {
                return Ok(tip);
            }
        }
        Ok(None)
    }

    /// The oracle's state for `key` at the tip, as JSON, or why there is none: the engine was
    /// not shown the chain as it is now.
    pub async fn expected(&self, key: &TrackingKey) -> Result<Value, Refused> {
        let engine_tip = engine_tip(&self.engine).await.map_err(|_| Refused::Gone)?;
        let chain = self.sim.lock();
        let checkpoint = self.checkpoint();
        if checkpoint.synced_at != Some(checkpoint.ops) {
            return Err(Refused::NoCheckpoint);
        }
        drop(checkpoint);
        let tip = chain.tip();
        if engine_tip != Some(tip) {
            return Err(Refused::NoCheckpoint);
        }
        let oracle = chain.oracle();
        let unspent = oracle.unspent(key, tip.height);
        let overlay = oracle.overlay(key, &chain.mempool());
        let boxes: Vec<_> = unspent
            .iter()
            .map(|(k, utxo)| WireBox { utxo, inclusion_height: Some(k.inclusion_height) })
            .collect();
        let pending: Vec<_> = overlay.iter().map(WirePendingTx).collect();
        let mut body = json!({ "tip": AsOf::from(&tip), "boxes": boxes, "pending": pending });
        if let TrackingKey::Tree(tree) = key {
            let held: Vec<_> = unspent.values().map(|b| &**b).collect();
            let confirmed = totals(held.iter().copied());
            let delta = reference_delta(&held, &overlay);
            body["balance"] = json!({
                "confirmed": Confirmed(&confirmed),
                "pending": Pending(&delta),
            });
            let history: Vec<_> = oracle
                .history(tree, &chain.mempool())
                .iter()
                .map(history_item)
                .collect();
            body["history"] = json!(history);
        }
        Ok(body)
    }

    /// The chain's trees as addresses, and every token it knows with its emission.
    pub fn keys(&self) -> Value {
        let chain = self.sim.lock();
        let addresses: Vec<String> = chain
            .trees()
            .iter()
            .map(|tree| address::encode(&tree.0, NETWORK))
            .collect();
        let tokens: Vec<Value> = chain
            .known_tokens()
            .iter()
            .map(|(id, emission)| json!({ "tokenId": id, "emission": emission.to_string() }))
            .collect();
        json!({ "addresses": addresses, "tokens": tokens })
    }

    fn control(self: Arc<Self>) -> Router {
        Router::new()
            .route("/sim/random", post(random))
            .route("/sim/ops", post(ops))
            .route("/sim/tick", post(tick))
            .route("/sim/settle", post(settle))
            .route("/sim/expected/addresses/{address}", get(expected_address))
            .route("/sim/expected/tokens/{token_id}", get(expected_token))
            .route("/sim/keys", get(keys))
            .with_state(self)
    }
}

/// One op from [`strategy::op`], the same for the same seed.
pub fn draw(seed: u64) -> SimOp {
    let mut bytes = [0; 32];
    bytes[..8].copy_from_slice(&seed.to_be_bytes());
    let rng = TestRng::from_seed(RngAlgorithm::ChaCha, &bytes);
    let mut runner = TestRunner::new_with_rng(RunnerConfig::default(), rng);
    strategy::op(MAX_REORG_DEPTH)
        .new_tree(&mut runner)
        .expect("the op strategy always draws")
        .current()
}

/// The engine task stopped, which only a panic does.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Gone;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Refused {
    /// No synced tick or settle since the last op.
    NoCheckpoint,
    Gone,
}

impl IntoResponse for Gone {
    fn into_response(self) -> Response {
        (StatusCode::INTERNAL_SERVER_ERROR, "the engine task stopped").into_response()
    }
}

impl IntoResponse for Refused {
    fn into_response(self) -> Response {
        match self {
            Self::NoCheckpoint => (
                StatusCode::CONFLICT,
                "the engine was not shown the chain as it is: tick or settle first",
            )
                .into_response(),
            Self::Gone => Gone.into_response(),
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Seed {
    #[serde(with = "decimal")]
    seed: u64,
}

/// A `u64` read from a decimal string, as [`SimOp`]'s seeds are written.
mod decimal {
    use serde::{Deserialize, Deserializer, de::Error};

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<u64, D::Error> {
        String::deserialize(deserializer)?
            .parse()
            .map_err(D::Error::custom)
    }
}

type Relay = State<Arc<SimRelay>>;

async fn random(State(relay): Relay, Json(body): Json<Seed>) -> Json<SimOp> {
    Json(relay.random(body.seed))
}

async fn ops(State(relay): Relay, Json(ops): Json<Vec<SimOp>>) -> StatusCode {
    relay.apply(&ops);
    StatusCode::NO_CONTENT
}

async fn tick(State(relay): Relay) -> Result<Json<Value>, Gone> {
    let (tip, synced) = relay.tick().await?;
    Ok(Json(json!({ "tip": tip.as_ref().map(AsOf::from), "synced": synced })))
}

async fn settle(State(relay): Relay) -> Result<Response, Gone> {
    Ok(match relay.settle().await? {
        Some(tip) => Json(json!({ "tip": AsOf::from(&tip) })).into_response(),
        None => (StatusCode::INTERNAL_SERVER_ERROR, "the engine did not reach the node's tip")
            .into_response(),
    })
}

async fn expected_address(State(relay): Relay, Path(address): Path<String>) -> Response {
    match address_key(&address, NETWORK, &KeyPolicy::default()) {
        Ok(key) => expected(&relay, &key).await,
        Err(error) => (StatusCode::BAD_REQUEST, error.to_string()).into_response(),
    }
}

async fn expected_token(State(relay): Relay, Path(token_id): Path<String>) -> Response {
    match token_key(&token_id, &KeyPolicy::default()) {
        Ok(key) => expected(&relay, &key).await,
        Err(error) => (StatusCode::BAD_REQUEST, error.to_string()).into_response(),
    }
}

async fn expected(relay: &SimRelay, key: &TrackingKey) -> Response {
    match relay.expected(key).await {
        Ok(body) => Json(body).into_response(),
        Err(refused) => refused.into_response(),
    }
}

async fn keys(State(relay): Relay) -> Json<Value> {
    Json(relay.keys())
}

/// A history entry as the history route writes an item: `txId`, then `height` and `timestamp`
/// once included, then `delta`.
fn history_item(entry: &HistoryEntry) -> Value {
    let mut item = json!({ "txId": entry.tx_id });
    if let Some(height) = entry.height {
        item["height"] = json!(height);
        item["timestamp"] = json!(timestamp(height));
    }
    item["delta"] = json!(Pending(&entry.delta));
    item
}

#[cfg(test)]
#[path = "relay_tests.rs"]
mod tests;
