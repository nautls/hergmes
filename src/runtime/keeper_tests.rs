//! The keeper against `SimChain` and a real engine task, under a paused clock.

use std::sync::Arc;

use tokio::task::JoinHandle;

use super::*;
use crate::{
    adapters::{BlockConsumer, EngineGone, EngineHandle, loader},
    engine::{self, Engine, PageIds, Query, Reply},
    runtime::{Clock, ManualClock, spawn, spawn_with_clock},
    sim::{
        Chain, Endpoint, SimChain, SimConfig,
        testing::{BLOCK_TIME, blocks},
        token_id,
    },
};

/// The grace period, in blocks of [`BLOCK_TIME`]: [`mine`] moves the clock by one per block.
const GRACE_BLOCKS: u32 = 2;
const CONFIG: engine::Config =
    engine::Config { window: 4, grace: blocks(GRACE_BLOCKS as u64), ..engine::Config::DEFAULT };
const RETRY: Duration = Duration::from_secs(5);

/// A chain mined a little, with an indexer that recovers from a reorg at once, and an engine
/// task synced to its tip, on a clock that [`mine`] moves.
async fn synced() -> (SimChain, Handle, JoinHandle<()>, ManualClock) {
    let mut chain = Chain::new(SimConfig { recovery_reads: 0, ..SimConfig::default() });
    chain.mine(0, 2, 1);
    let sim = SimChain::new(chain);
    let clock = ManualClock::default();
    let (engine, task) = spawn_with_clock(Engine::new(CONFIG), Clock::Manual(clock.clone()));
    sync(&sim, &engine).await;
    (sim, engine, task, clock)
}

/// Mines `count` blocks, each a block's time after the last, and syncs the engine to them.
async fn mine(sim: &SimChain, engine: &Handle, clock: &ManualClock, count: u32) {
    for seed in 0..count {
        clock.advance(BLOCK_TIME);
        sim.lock().mine(0, 1, seed.into());
        sync(sim, engine).await;
    }
}

async fn sync(sim: &SimChain, engine: &Handle) {
    let tip = sim.lock().tip();
    BlockConsumer::new(sim.clone())
        .sync(engine, tip)
        .await
        .unwrap();
}

fn keeper(sim: &SimChain, engine: &Handle) -> Arc<Keeper<SimChain>> {
    Arc::new(Keeper::new(
        engine.clone(),
        Arc::new(Loader::new(sim.clone(), loader::Config::default())),
        Arc::new(TokenChecker::new(sim.clone(), 100)),
        Config::default(),
    ))
}

fn keep(keeper: &Arc<Keeper<SimChain>>, key: &TrackingKey) -> JoinHandle<Result<(), EngineGone>> {
    keep_settling(keeper, key).0
}

/// [`keep`], with what settles once the key's first load has an answer.
fn keep_settling(
    keeper: &Arc<Keeper<SimChain>>,
    key: &TrackingKey,
) -> (JoinHandle<Result<(), EngineGone>>, Settled) {
    let (settled, mut pending) = settling(1);
    let (keeper, key, pending) = (keeper.clone(), key.clone(), pending.pop().unwrap());
    (tokio::spawn(async move { keeper.keep(key, pending).await }), settled)
}

/// The key's boxes at the engine's tip, if it is tracked. A read is activity for the grace
/// period. A page rather than a balance, which a token key has none of.
async fn tracked(engine: &Handle, key: &TrackingKey) -> Option<PageIds> {
    let query = Query::Page { key: key.clone(), at: None, after: None, limit: usize::MAX };
    match engine.query(query).await.unwrap() {
        Reply::Page(page) => page.ok(),
        other => panic!("{other:?}"),
    }
}

/// Waits, in paused time, until the key is tracked at the node's tip.
async fn tracked_at_tip(sim: &SimChain, engine: &Handle, key: &TrackingKey) {
    for _ in 0..100 {
        let tip = sim.lock().tip();
        if let Some(page) = tracked(engine, key).await
            && page.as_of == tip
        {
            let expected: Vec<_> = sim
                .lock()
                .oracle()
                .unspent(key, tip.height)
                .into_keys()
                .collect();
            assert_eq!(page.boxes, expected, "{key:?}");
            return;
        }
        sleep(Duration::from_millis(100)).await;
    }
    panic!("{key:?} never tracked at the tip");
}

#[tokio::test(start_paused = true)]
async fn a_configured_key_outlives_the_grace_period() {
    let (sim, engine, _task, clock) = synced().await;
    let keys = sim.lock().keys();
    let (kept, loaded) = (&keys[0], &keys[1]);
    let _keeper = keep(&keeper(&sim, &engine), kept);
    tracked_at_tip(&sim, &engine, kept).await;
    Loader::new(sim.clone(), loader::Config::default())
        .load(&engine, loaded)
        .await
        .unwrap();

    mine(&sim, &engine, &clock, GRACE_BLOCKS + 2).await;
    assert!(tracked(&engine, kept).await.is_some(), "the keeper's subscription holds it");
    assert!(tracked(&engine, loaded).await.is_none(), "a key no one holds goes idle");
}

#[tokio::test(start_paused = true)]
async fn a_key_dropped_with_its_load_block_is_loaded_again() {
    let (sim, engine, _task, _clock) = synced().await;
    let key = TrackingKey::Token(token_id("small-100"));
    let keeper = keep(&keeper(&sim, &engine), &key);
    tracked_at_tip(&sim, &engine, &key).await;
    let loaded_at = sim.lock().tip();

    // The load block is orphaned: the engine drops the key, and the keeper loads it again
    // on the new branch.
    {
        let mut chain = sim.lock();
        chain.orphan(1);
        chain.mine(0, 1, 11);
        chain.mine(0, 1, 12);
    }
    sync(&sim, &engine).await;
    tracked_at_tip(&sim, &engine, &key).await;
    assert_ne!(sim.lock().tip(), loaded_at);
    assert!(!keeper.is_finished());
}

#[tokio::test(start_paused = true)]
async fn a_token_above_the_supply_limit_is_given_up_alone() {
    let (sim, engine, _task, _clock) = synced().await;
    let keeper = keeper(&sim, &engine);
    let large = keep(&keeper, &TrackingKey::Token(token_id("over-101")));
    let small = TrackingKey::Token(token_id("small-100"));
    let _kept = keep(&keeper, &small);
    let given_up = tokio::time::timeout(RETRY * 10, large)
        .await
        .expect("the keeper gives the key up")
        .unwrap();
    assert_eq!(given_up, Ok(()));
    tracked_at_tip(&sim, &engine, &small).await;
}

/// A chain mined a little, and an engine task synced to it that has room for the small
/// token's boxes and nothing more.
async fn room_for_the_small_token() -> (SimChain, Handle, JoinHandle<()>, ManualClock) {
    let mut chain = Chain::new(SimConfig { recovery_reads: 0, ..SimConfig::default() });
    chain.mine(0, 2, 1);
    let sim = SimChain::new(chain);
    let small = TrackingKey::Token(token_id("small-100"));
    let tip = sim.lock().tip();
    let token_boxes = sim.lock().oracle().load(&small, tip.height).len();
    assert!(token_boxes > 3);
    // The key counts as one box more than it holds.
    let config = engine::Config { max_tracked_boxes: token_boxes + 1, ..CONFIG };
    let clock = ManualClock::default();
    let (engine, task) = spawn_with_clock(Engine::new(config), Clock::Manual(clock.clone()));
    sync(&sim, &engine).await;
    (sim, engine, task, clock)
}

/// A key over the per-key cap is given up alone, and settles; the key that fits stays
/// tracked.
#[tokio::test(start_paused = true)]
async fn a_key_over_the_per_key_cap_is_given_up_alone() {
    let (sim, engine, _task, _clock) = room_for_the_small_token().await;
    let small = TrackingKey::Token(token_id("small-100"));
    let keeper = |max_boxes| {
        Arc::new(Keeper::new(
            engine.clone(),
            Arc::new(Loader::new(sim.clone(), loader::Config { max_boxes, ..Default::default() })),
            Arc::new(TokenChecker::new(sim.clone(), 100)),
            Config::default(),
        ))
    };

    let (over, settled) = keep_settling(&keeper(3), &small);
    let given_up = tokio::time::timeout(RETRY * 10, over)
        .await
        .expect("given up")
        .unwrap();
    assert_eq!(given_up, Ok(()), "over the per-key cap");
    assert!(settled.is_settled());

    let _kept = keep(&keeper(loader::MAX_BOXES), &small);
    tracked_at_tip(&sim, &engine, &small).await;
}

/// A key the total cap refuses settles, is asked again, and is tracked once a key loaded by
/// no one goes idle and makes room.
#[tokio::test(start_paused = true)]
async fn a_key_refused_by_the_total_cap_is_tracked_once_there_is_room() {
    let (sim, engine, _task, clock) = room_for_the_small_token().await;
    let small = TrackingKey::Token(token_id("small-100"));
    let tree = sim.lock().keys()[0].clone();
    assert!(matches!(tree, TrackingKey::Tree(_)));
    // A client's load fills the cap.
    Loader::new(sim.clone(), loader::Config::default())
        .load(&engine, &small)
        .await
        .unwrap();

    let reads = || sim.calls().iter().filter(|e| **e == Endpoint::Info).count();
    let before = reads();
    let (kept, settled) = keep_settling(&keeper(&sim, &engine), &tree);
    sleep(Config::default().capacity_retry / 2).await;
    assert!(settled.is_settled(), "a refusal is an answer");
    // An attempt stopped at the cap reads `/info` once: one attempt, not one per retry.
    assert_eq!(reads() - before, 1, "a refused key is loaded again only after the long wait");
    assert!(tracked(&engine, &tree).await.is_none(), "refused while the cap is full");
    assert!(!kept.is_finished(), "a full cap is a wait, not a give-up");

    mine(&sim, &engine, &clock, GRACE_BLOCKS + 2).await;
    sleep(Config::default().capacity_retry).await;
    tracked_at_tip(&sim, &engine, &tree).await;
    assert!(tracked(&engine, &small).await.is_none(), "the client's key went idle");
}

/// A key's first load settles it, and a load that waits for the engine to reach the node
/// does not, so the clients' loads wait with it.
#[tokio::test(start_paused = true)]
async fn a_key_settles_on_its_first_answer_and_not_while_the_engine_is_behind() {
    let (sim, engine, _task, _clock) = synced().await;
    let key = sim.lock().keys()[0].clone();
    sim.lock().mine(0, 1, 9);
    let keeper = keeper(&sim, &engine);
    let (_kept, settled) = keep_settling(&keeper, &key);
    let loader = loader::Config::default();
    sleep(loader.give_up_after * 3).await;
    assert!(!settled.is_settled(), "the engine is behind the node");

    sync(&sim, &engine).await;
    tracked_at_tip(&sim, &engine, &key).await;
    assert!(settled.is_settled());

    // A keeper that ends without an answer, such as one that panics, settles its key.
    sim.lock().mine(0, 1, 10);
    let (kept, settled) = keep_settling(&keeper, &sim.lock().keys()[1].clone());
    kept.abort();
    let _ = kept.await;
    assert!(settled.is_settled());
}

/// Settled once every key is.
#[test]
fn settled_once_every_key_is() {
    let (settled, mut pending) = settling(2);
    assert!(!settled.is_settled());
    pending.pop().unwrap().settle();
    assert!(!settled.is_settled());
    let mut last = pending.pop().unwrap();
    last.settle();
    last.settle();
    assert!(settled.is_settled());
    drop(last);
    assert!(settled.is_settled());
    assert!(settling(0).0.is_settled());
    assert!(Settled::now().is_settled());
}

#[tokio::test(start_paused = true)]
async fn an_unknown_token_is_asked_again_and_kept_once_minted() {
    let (sim, engine, _task, _clock) = synced().await;
    let owner = sim.lock().trees()[0].clone();
    let input = crate::sim::testing::box_of(&sim.lock(), 0);
    let key = TrackingKey::Token(input);
    let (keeper, settled) = keep_settling(&keeper(&sim, &engine), &key);
    sleep(RETRY * 3).await;
    assert!(!keeper.is_finished(), "an unknown token is a wait");
    assert!(settled.is_settled(), "a mistyped token never holds the clients off");

    {
        let mut chain = sim.lock();
        let mint = chain.mint(&input, 3, &owner);
        chain.mine_txs(vec![mint]);
    }
    sync(&sim, &engine).await;
    tracked_at_tip(&sim, &engine, &key).await;
}

#[tokio::test(start_paused = true)]
async fn a_load_that_gave_up_is_tried_again_after_its_wait() {
    // The node is a block ahead of an engine that never catches up: every attempt reads
    // `/info` once and finds the engine behind.
    let (sim, engine, _task, _clock) = synced().await;
    sim.lock().mine(0, 1, 9);
    let key = sim.lock().keys()[0].clone();
    let loader = loader::Config::default();
    let _keeper = keep(&keeper(&sim, &engine), &key);
    let span = loader.give_up_after * 10;
    sleep(span).await;
    let reads = sim.calls().iter().filter(|e| **e == Endpoint::Info).count();
    // One run of polls, then one attempt per retry, with no burst when the loader gives up.
    let polls = (loader.retry.as_millis() / loader.poll.as_millis()) as usize;
    let most = polls + (span.as_millis() / loader.retry.as_millis()) as usize + 1;
    assert!(reads <= most, "{reads} reads in {span:?}, at most {most} expected");
}

#[tokio::test(start_paused = true)]
async fn the_keeper_stops_when_the_engine_is_gone() {
    let (sim, engine, task, _clock) = synced().await;
    task.abort();
    let key = sim.lock().keys()[0].clone();
    assert_eq!(keep(&keeper(&sim, &engine), &key).await.unwrap(), Err(EngineGone));
}

mod node {
    use serde_json::Value;
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path},
    };

    use super::*;
    use crate::{
        clients::NodeClient,
        engine::Event,
        fixtures::{self, load, sample},
        types::{HexBytes, address::Network, ergo::IndexedBlock},
    };

    /// The node serves the recorded block as its tip, and the recorded boxes of the sample
    /// tree for any tree.
    async fn node_with_wrong_boxes(block: &IndexedBlock) -> MockServer {
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
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "indexedHeight": block.header.height,
                "fullHeight": block.header.height,
            })))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/blockchain/box/unspent/byErgoTree"))
            .respond_with(
                ResponseTemplate::new(fixtures::status("unspent_by_ergo_tree"))
                    .set_body_raw(fixtures::text("unspent_by_ergo_tree"), "application/json"),
            )
            .mount(&server)
            .await;
        server
    }

    /// A token check the node fails settles the key, so a failing node never holds the
    /// clients off, and the keeper asks again. On real time, like the test below.
    #[tokio::test]
    async fn a_failed_token_check_settles_the_key_and_is_asked_again() {
        let server = MockServer::start().await;
        Mock::given(wiremock::matchers::any())
            .respond_with(ResponseTemplate::new(500))
            .mount(&server)
            .await;
        let client = NodeClient::new(reqwest::Client::new(), &server.uri(), Network::Mainnet);
        let (engine, _task) = spawn(Engine::new(engine::Config::default()));
        let config = Config { retry: Duration::from_millis(10), ..Config::default() };
        let keeper = Arc::new(Keeper::new(
            engine,
            Arc::new(Loader::new(client.clone(), loader::Config::default())),
            Arc::new(TokenChecker::new(client, 100)),
            config,
        ));
        let (settled, mut pending) = settling(1);
        let pending = pending.pop().unwrap();
        let key = TrackingKey::Token(token_id("small-100"));
        let kept = tokio::spawn(async move { keeper.keep(key, pending).await });

        let started = tokio::time::Instant::now();
        while server.received_requests().await.unwrap().len() < 3 {
            assert!(started.elapsed() < Duration::from_secs(10), "the check is asked again");
            sleep(Duration::from_millis(20)).await;
        }
        assert!(settled.is_settled());
        assert!(!kept.is_finished(), "a node error never stops the keeper");
    }

    /// On real time: a paused clock would jump ahead while the HTTP calls are in flight.
    #[tokio::test]
    async fn boxes_that_do_not_match_the_key_are_asked_again_later() {
        let block: IndexedBlock = load("indexed_block");
        let (engine, _task) = spawn(Engine::new(engine::Config::default()));
        engine
            .apply(Event::BlockApplied(block.clone()))
            .await
            .unwrap()
            .unwrap();
        let server = node_with_wrong_boxes(&block).await;
        let client = NodeClient::new(reqwest::Client::new(), &server.uri(), Network::Mainnet);
        let config = Config {
            retry: Duration::from_millis(10),
            mismatch_retry: Duration::from_millis(100),
            ..Config::default()
        };
        let keeper = Arc::new(Keeper::new(
            engine.clone(),
            Arc::new(Loader::new(client.clone(), loader::Config::default())),
            Arc::new(TokenChecker::new(client, 100)),
            config,
        ));
        let mut other = hex::decode(sample("ergoTree")).unwrap();
        *other.last_mut().unwrap() ^= 1;
        let key = TrackingKey::Tree(HexBytes::from(other));
        let kept = tokio::spawn({
            let key = key.clone();
            async move { keeper.keep(key, settling(1).1.pop().unwrap()).await }
        });

        let loads = || async {
            let requests = server.received_requests().await.unwrap();
            let by_ergo_tree = "/blockchain/box/unspent/byErgoTree";
            requests
                .iter()
                .filter(|r| r.url.path() == by_ergo_tree)
                .count()
        };
        let started = tokio::time::Instant::now();
        while loads().await < 3 {
            assert!(started.elapsed() < Duration::from_secs(10), "the load is asked again");
            sleep(Duration::from_millis(20)).await;
        }
        assert!(
            started.elapsed() >= config.mismatch_retry * 2,
            "loads are spaced by the mismatch wait, not the short retry"
        );
        assert!(!kept.is_finished(), "a node fault never stops the keeper");
        let reply = engine.query(Query::Balance(key)).await.unwrap();
        assert_eq!(reply, Reply::Balance(None), "the mismatched boxes were not loaded");
    }
}
