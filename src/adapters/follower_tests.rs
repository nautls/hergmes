//! The follower: one `/info` read per tick for blocks then mempool, the mempool skipped while
//! the engine is behind, back-off, and node failures (`500`, timeouts) through `wiremock`.

use std::sync::{Arc, Mutex};

use super::*;
use crate::{
    engine::{BlockRef, Config as EngineConfig, Engine, Query, Reply},
    sim::{
        Chain, Endpoint, SimChain, SimConfig,
        testing::{new_chain, ready},
    },
};

fn engine() -> Mutex<Engine> {
    Mutex::new(Engine::new(EngineConfig::default()))
}

/// The snapshot the engine uses, and the tip it is in use at.
fn engine_snapshot(engine: &Mutex<Engine>) -> Option<(usize, BlockRef)> {
    match engine.lock().unwrap().query(Query::MempoolSnapshot) {
        Reply::MempoolSnapshot(snapshot) => snapshot.map(|(txs, tip)| (txs.len(), tip)),
        other => panic!("{other:?}"),
    }
}

#[test]
fn a_tick_applies_the_blocks_then_the_mempool_at_that_tip() {
    let mut chain = new_chain();
    chain.mempool_add(1).expect("a pending tx");
    let sim = SimChain::new(chain);
    let engine = engine();
    let mut follower = Follower::new(sim.clone(), Config::default());
    let tick = ready(follower.tick(&engine, Instant::now())).unwrap();
    assert!(matches!(tick.blocks, Ok(Sync::Synced { applied: 1, .. })), "{tick:?}");
    assert!(matches!(tick.mempool, Some(Ok(Poll::Snapshot { .. }))), "{tick:?}");
    use Endpoint::{IndexedBlock, IndexedHeight, Info, Mempool};
    assert_eq!(sim.calls(), [Info, IndexedHeight, IndexedBlock, Mempool, Info]);
    let tip = sim.lock().tip();
    assert_eq!(engine_snapshot(&engine), Some((1, tip)));
}

#[test]
fn the_mempool_waits_while_the_engine_is_behind() {
    let mut chain = Chain::new(SimConfig::default());
    chain.mine(0, 1, 1);
    let sim = SimChain::new(chain);
    let engine = engine();
    let mut follower = Follower::new(sim.clone(), Config::default());
    let now = Instant::now();
    ready(follower.tick(&engine, now)).unwrap();

    // A same-height reorg: the recovering indexer answers 503 to its next reads.
    sim.lock().orphan(1);
    sim.lock().mine(0, 2, 2);
    sim.lock().mempool_add(3).expect("a pending tx");
    for _ in 0..SimConfig::default().recovery_reads {
        let tick = ready(follower.tick(&engine, now)).unwrap();
        assert!(matches!(tick.blocks, Err(NodeError::Unavailable { .. })), "{tick:?}");
        assert!(tick.mempool.is_none());
    }
    let tick = ready(follower.tick(&engine, now)).unwrap();
    assert!(matches!(tick.blocks, Ok(Sync::Synced { reverted: 1, applied: 1, .. })), "{tick:?}");
    assert!(matches!(tick.mempool, Some(Ok(Poll::Snapshot { .. }))), "{tick:?}");
    let tip = sim.lock().tip();
    assert_eq!(engine_snapshot(&engine).map(|s| s.1), Some(tip));
}

#[test]
fn synced_says_whether_the_last_tick_reached_the_nodes_tip() {
    let sim = SimChain::new(new_chain());
    let engine = engine();
    let mut follower = Follower::new(sim.clone(), Config::default());
    let synced = follower.synced();
    assert!(!*synced.borrow(), "not before the first tick");
    ready(follower.tick(&engine, Instant::now())).unwrap();
    assert!(*synced.borrow());

    sim.lock().set_indexer_lag(1);
    sim.lock().mine(0, 1, 1);
    let tick = ready(follower.tick(&engine, Instant::now())).unwrap();
    assert!(matches!(tick.blocks, Ok(Sync::WaitingForIndexer { .. })), "{tick:?}");
    assert!(!*synced.borrow(), "the engine is behind");

    sim.lock().settle_indexer();
    ready(follower.tick(&engine, Instant::now())).unwrap();
    assert!(*synced.borrow());
    ready(follower.tick(&engine, Instant::now())).unwrap();
    assert!(*synced.borrow(), "an idle tick is still at the tip");
}

#[tokio::test(start_paused = true)]
async fn run_follows_the_node() {
    let sim = SimChain::new(new_chain());
    let engine = Arc::new(engine());
    let follower = Follower::new(sim.clone(), Config::default());
    let task = {
        let engine = engine.clone();
        tokio::spawn(async move { follower.run(&*engine).await })
    };
    for seed in 1..=3 {
        sim.lock().mine(0, 1, seed);
        sim.lock().mempool_add(seed + 10);
        tokio::time::sleep(Duration::from_secs(2)).await;
        let tip = sim.lock().tip();
        assert_eq!(engine.lock().unwrap().tip(), Some(tip));
        let pool = sim.lock().mempool().len();
        assert_eq!(engine_snapshot(&engine), Some((pool, tip)));
    }
    task.abort();
}

mod node {
    use serde_json::{Value, json};
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path},
    };

    use super::*;
    use crate::{
        clients::NodeClient,
        fixtures::{self, load},
        types::{address::Network, ergo::IndexedBlock},
    };

    fn recorded(name: &str) -> ResponseTemplate {
        ResponseTemplate::new(fixtures::status(name))
            .set_body_raw(fixtures::text(name), "application/json")
    }

    /// A node at the recorded block, whose first mempool answer is `first`.
    async fn node(first: ResponseTemplate) -> MockServer {
        let block: IndexedBlock = load("indexed_block");
        let mut info: Value = load("info");
        info["bestFullHeaderId"] = block.header.id.to_string().into();
        info["fullHeight"] = block.header.height.into();
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/info"))
            .respond_with(ResponseTemplate::new(200).set_body_json(info))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/blockchain/indexedHeight"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "indexedHeight": block.header.height,
                "fullHeight": block.header.height,
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(format!("/blockchain/block/byHeaderId/{}", block.header.id)))
            .respond_with(recorded("indexed_block"))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/transactions/unconfirmed"))
            .respond_with(first)
            .up_to_n_times(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/transactions/unconfirmed"))
            .respond_with(recorded("mempool"))
            .mount(&server)
            .await;
        server
    }

    async fn a_failed_mempool_fetch_is_retried_next_tick(
        first: ResponseTemplate,
        timeout: Duration,
    ) {
        let server = node(first).await;
        let http = reqwest::Client::builder().timeout(timeout).build().unwrap();
        let client = NodeClient::new(http, &server.uri(), Network::Mainnet);
        let engine = engine();
        let mut follower = Follower::new(client, Config::default());
        let now = Instant::now();

        let tick = follower.tick(&engine, now).await.unwrap();
        assert!(matches!(tick.blocks, Ok(Sync::Synced { applied: 1, .. })), "{tick:?}");
        assert!(matches!(tick.mempool, Some(Err(_))), "{tick:?}");
        assert_eq!(engine_snapshot(&engine), None);

        let tick = follower.tick(&engine, now).await.unwrap();
        assert!(matches!(tick.blocks, Ok(Sync::Idle)), "{tick:?}");
        assert!(matches!(tick.mempool, Some(Ok(Poll::Snapshot { .. }))), "{tick:?}");
        assert!(engine_snapshot(&engine).is_some());
    }

    #[tokio::test]
    async fn a_500_is_retried_next_tick() {
        a_failed_mempool_fetch_is_retried_next_tick(
            ResponseTemplate::new(500),
            Duration::from_secs(10),
        )
        .await;
    }

    #[tokio::test]
    async fn a_timeout_is_retried_next_tick() {
        a_failed_mempool_fetch_is_retried_next_tick(
            recorded("mempool").set_delay(Duration::from_millis(500)),
            Duration::from_millis(100),
        )
        .await;
    }

    #[tokio::test]
    async fn a_failed_info_read_is_an_error() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/info"))
            .respond_with(ResponseTemplate::new(500))
            .mount(&server)
            .await;
        let client = NodeClient::new(reqwest::Client::new(), &server.uri(), Network::Mainnet);
        let mut follower = Follower::new(client, Config::default());
        let error = follower.tick(&engine(), Instant::now()).await.unwrap_err();
        assert!(matches!(error, AdapterError::Node(NodeError::Api { .. })), "{error:?}");
    }

    #[tokio::test]
    async fn a_failed_info_read_is_not_synced() {
        let server = node(recorded("mempool")).await;
        let client = NodeClient::new(reqwest::Client::new(), &server.uri(), Network::Mainnet);
        let engine = engine();
        let mut follower = Follower::new(client, Config::default());
        follower.tick(&engine, Instant::now()).await.unwrap();
        assert!(*follower.synced().borrow());

        server.reset().await;
        Mock::given(method("GET"))
            .and(path("/info"))
            .respond_with(ResponseTemplate::new(500))
            .mount(&server)
            .await;
        follower.tick(&engine, Instant::now()).await.unwrap_err();
        assert!(!*follower.synced().borrow(), "the node's tip is unknown");
    }
}

/// Runs `follower` on its own task, with the engine at `engine`.
fn spawn(follower: Follower<SimChain>, engine: &Arc<Mutex<Engine>>) -> tokio::task::JoinHandle<()> {
    let engine = engine.clone();
    tokio::spawn(async move {
        follower.run(&*engine).await;
    })
}

fn count(sim: &SimChain, endpoint: Endpoint) -> usize {
    sim.calls().iter().filter(|e| **e == endpoint).count()
}

/// A block that lands right after a tick reaches the engine a `wake_gap` after that tick when
/// a load asks, rather than at the next one.
#[tokio::test(start_paused = true)]
async fn a_wake_ticks_before_the_interval() {
    let sim = SimChain::new(new_chain());
    let engine = Arc::new(engine());
    let config = Config::default();
    let follower = Follower::new(sim.clone(), config);
    let wake = follower.waker();
    let task = spawn(follower, &engine);
    tokio::time::sleep(Duration::from_millis(10)).await;
    let tip = sim.lock().mine(0, 1, 1);
    wake.wake();
    tokio::time::sleep(config.wake_gap).await;
    assert_eq!(engine.lock().unwrap().tip(), Some(tip));
    task.abort();
}

/// Wakes made while a tick runs count as one, and the regular tick comes an interval after
/// the woken one rather than right behind it.
#[tokio::test(start_paused = true)]
async fn wakes_during_a_tick_cost_one_tick() {
    let sim = SimChain::new(new_chain());
    let engine = Arc::new(engine());
    let config = Config::default();
    let follower = Follower::new(sim.clone(), config);
    let wake = follower.waker();
    let task = spawn(follower, &engine);
    let ms = Duration::from_millis;
    // The first tick runs at 0 and takes no time.
    tokio::time::sleep(ms(10)).await;
    let infos = count(&sim, Endpoint::Info);
    let latency = ms(100);
    sim.set_latency(Endpoint::Info, latency);

    // Woken at 10 ms, it ticks a gap after the first tick ended: from 200 to 300 ms.
    wake.wake();
    tokio::time::sleep(config.wake_gap + latency / 2 - ms(10)).await;
    assert_eq!(count(&sim, Endpoint::Info), infos + 1, "the woken tick is under way");
    for _ in 0..50 {
        wake.wake();
    }
    // Those wakes tick once, from 500 to 600 ms; the regular tick follows an interval after
    // it ended, at 1,600 ms.
    let regular = 2 * config.wake_gap + 2 * latency + config.interval;
    tokio::time::sleep(regular - ms(50) - (config.wake_gap + latency / 2)).await;
    assert_eq!(count(&sim, Endpoint::Info), infos + 2, "{:?}", sim.calls());
    tokio::time::sleep(ms(100)).await;
    assert_eq!(count(&sim, Endpoint::Info), infos + 3, "the regular tick, an interval later");
    task.abort();
}

/// A wake during the wait after a `503` does not cut it short: the node asked for it.
#[tokio::test(start_paused = true)]
async fn a_wake_never_cuts_a_backoff_short() {
    let mut chain = Chain::new(SimConfig::default());
    chain.mine(0, 1, 1);
    let sim = SimChain::new(chain);
    let engine = Arc::new(engine());
    let config = Config::default();
    let follower = Follower::new(sim.clone(), config);
    let wake = follower.waker();
    let task = spawn(follower, &engine);
    tokio::time::sleep(Duration::from_millis(10)).await;

    // A same-height reorg: the recovering indexer answers 503, with a `Retry-After`.
    sim.lock().orphan(1);
    sim.lock().mine(0, 2, 2);
    tokio::time::sleep(config.interval).await;
    let infos = count(&sim, Endpoint::Info);
    let blocks = sim.calls().len();
    assert_eq!(sim.calls().last(), Some(&Endpoint::IndexedBlock), "{:?}", sim.calls());
    let step = Duration::from_millis(10);
    let mut waited = Duration::ZERO;
    while waited + step < crate::sim::RETRY_AFTER {
        wake.wake();
        tokio::time::sleep(step).await;
        waited += step;
    }
    assert_eq!(count(&sim, Endpoint::Info), infos, "{:?}", &sim.calls()[blocks..]);
    task.abort();
}

/// While the indexer lags, every load wakes the follower, and each woken tick finds the engine
/// still behind. Woken ticks still start a `wake_gap` after the last one ended, however long
/// its `/info` took.
#[tokio::test(start_paused = true)]
async fn woken_ticks_keep_the_gap_after_a_slow_info() {
    let sim = SimChain::new(new_chain());
    let engine = Arc::new(engine());
    let config = Config::default();
    let follower = Follower::new(sim.clone(), config);
    let wake = follower.waker();
    let task = spawn(follower, &engine);
    tokio::time::sleep(Duration::from_millis(10)).await;
    sim.lock().set_indexer_lag(1);
    sim.lock().mine(0, 1, 1);
    let latency = Duration::from_millis(300);
    sim.set_latency(Endpoint::Info, latency);
    let before = count(&sim, Endpoint::Info);

    let step = Duration::from_millis(10);
    let span = Duration::from_secs(5);
    let mut waited = Duration::ZERO;
    while waited < span {
        wake.wake();
        tokio::time::sleep(step).await;
        waited += step;
    }
    // One tick per `/info` and gap: 10 in 5 s. Gaps counted from a tick's start would give
    // back-to-back ticks, about 16.
    let ticks = count(&sim, Endpoint::Info) - before;
    let most = (span.as_millis() / (latency + config.wake_gap).as_millis()) as usize + 1;
    assert!(ticks <= most, "{ticks} ticks in {span:?}, at most {most}");
    task.abort();
}

/// A tick that takes longer than the interval is followed by a whole interval, not by the
/// next tick at once: a slow node is read at most once per interval and tick, rather than
/// back to back while it is slowest.
#[tokio::test(start_paused = true)]
async fn regular_ticks_rest_an_interval_after_a_slow_info() {
    let sim = SimChain::new(new_chain());
    let engine = Arc::new(engine());
    let config = Config::default();
    let task = spawn(Follower::new(sim.clone(), config), &engine);
    tokio::time::sleep(Duration::from_millis(10)).await;
    let latency = 3 * config.interval / 2;
    sim.set_latency(Endpoint::Info, latency);
    let before = count(&sim, Endpoint::Info);

    // Shorter than the mempool's refresh, so every tick reads `/info` once.
    let span = Duration::from_secs(9);
    tokio::time::sleep(span).await;
    // One tick per `/info` and interval: 4 in 9 s. Back to back, one per `/info`: 6.
    let ticks = count(&sim, Endpoint::Info) - before;
    let most = (span.as_millis() / (latency + config.interval).as_millis()) as usize + 1;
    assert!(ticks <= most, "{ticks} ticks in {span:?}, at most {most}");
    task.abort();
}
