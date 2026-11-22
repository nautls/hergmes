//! The balance reader against `wiremock`: the height it reports, and an indexer moving under
//! it.

use serde_json::json;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path},
};

use super::*;
use crate::{
    clients::NodeClient,
    fixtures::{self, sample},
    types::{HexBytes, address::Network},
};

fn height(indexed: u32) -> ResponseTemplate {
    ResponseTemplate::new(200)
        .set_body_json(json!({ "indexedHeight": indexed, "fullHeight": indexed }))
}

/// Serves the indexed heights in order, then the recorded balance.
async fn reader(heights: &[u32]) -> (MockServer, BalanceReader<NodeClient>) {
    let server = MockServer::start().await;
    for &h in heights {
        Mock::given(method("GET"))
            .and(path("/blockchain/indexedHeight"))
            .respond_with(height(h))
            .up_to_n_times(1)
            .mount(&server)
            .await;
    }
    Mock::given(method("POST"))
        .and(path("/blockchain/balance"))
        .respond_with(
            ResponseTemplate::new(fixtures::status("balance"))
                .set_body_raw(fixtures::text("balance"), "application/json"),
        )
        .mount(&server)
        .await;
    let client = NodeClient::new(reqwest::Client::new(), &server.uri(), Network::Mainnet);
    (server, BalanceReader::new(client))
}

fn tree() -> ErgoTree {
    HexBytes::from(hex::decode(sample("ergoTree")).unwrap())
}

#[tokio::test]
async fn a_balance_comes_with_the_height_it_was_read_at() {
    let (_server, reader) = reader(&[10, 10]).await;
    let read = reader.read(&tree()).await.unwrap().unwrap();
    assert_eq!(read.height, 10);
    assert_eq!(read.confirmed.nano_ergs, 361_490_634_617);
}

#[tokio::test]
async fn an_indexer_moving_during_the_call_is_read_again() {
    let (server, reader) = reader(&[10, 11, 11, 11]).await;
    assert_eq!(reader.read(&tree()).await.unwrap().unwrap().height, 11);
    let balance_calls = server
        .received_requests()
        .await
        .unwrap()
        .iter()
        .filter(|r| r.url.path() == "/blockchain/balance")
        .count();
    assert_eq!(balance_calls, 2);
}

#[tokio::test]
async fn an_indexer_that_keeps_moving_gives_no_balance() {
    let (_server, reader) = reader(&[10, 11, 12, 13, 14, 15]).await;
    assert_eq!(reader.read(&tree()).await.unwrap(), None);
}

mod track {
    use std::sync::Mutex;

    use tokio::time::Instant;

    use super::super::*;
    use crate::{
        adapters::{Follower, Wake, follower, polls::Checks},
        engine::{Config as EngineConfig, Engine, Query, Reply, TrackingKey},
        sim::{
            Endpoint, SimChain,
            testing::{new_chain, ready},
        },
    };

    fn synced_engine(chain: &crate::sim::Chain) -> Mutex<Engine> {
        let mut engine = Engine::new(EngineConfig::default());
        engine
            .apply(Event::BlockApplied(chain.oracle().indexed_block(chain.height())))
            .unwrap();
        Mutex::new(engine)
    }

    fn count(sim: &SimChain, endpoint: Endpoint) -> usize {
        sim.calls().iter().filter(|e| **e == endpoint).count()
    }

    /// The engine's balance of `tree`, if it tracks it.
    fn tracked(engine: &Mutex<Engine>, tree: &ErgoTree) -> Option<Totals> {
        let query = Query::Balance(TrackingKey::Tree(tree.clone()));
        match engine.lock().unwrap().query(query) {
            Reply::Balance(balance) => balance.map(|b| b.confirmed),
            other => unreachable!("{other:?}"),
        }
    }

    #[test]
    fn an_address_is_tracked_from_its_balance_at_the_tip() {
        let chain = new_chain();
        let engine = synced_engine(&chain);
        let sim = SimChain::new(chain);
        let tree = sim.lock().trees()[0].clone();
        let reader = BalanceReader::new(sim.clone());
        assert_eq!(ready(reader.try_track(&engine, &tree)).unwrap(), Attempt::Tracked);
        let expected = {
            let chain = sim.lock();
            chain
                .oracle()
                .totals(&TrackingKey::Tree(tree.clone()), chain.height())
        };
        assert_eq!(tracked(&engine, &tree), Some(expected));
        assert_eq!(count(&sim, Endpoint::UnspentByErgoTree), 0, "no boxes read");
    }

    /// The engine took the tip's block from elsewhere and the indexer has not reached it: its
    /// balance would be the block before's.
    #[test]
    fn an_indexer_behind_the_tip_is_not_read() {
        let mut chain = new_chain();
        chain.set_indexer_lag(1);
        let mut engine = Engine::new(EngineConfig::default());
        let tip = chain.mine(0, 1, 1);
        engine
            .apply(Event::BlockApplied(chain.oracle().indexed_block(tip.height)))
            .unwrap();
        let engine = Mutex::new(engine);
        let sim = SimChain::new(chain);
        let tree = sim.lock().trees()[0].clone();
        let wake = Wake::default();
        let reader = BalanceReader::new(sim.clone()).waking(wake.clone());
        let attempt = ready(reader.try_track(&engine, &tree)).unwrap();
        assert_eq!((attempt, attempt.checks()), (Attempt::Behind, Checks::Behind));
        assert_eq!(count(&sim, Endpoint::ConfirmedBalance), 0);
        assert_eq!(tracked(&engine, &tree), None);
        assert!(!wake.take(), "the engine is at the tip: a tick would not help");
    }

    /// The node has a block the engine doesn't: the attempt wakes the follower.
    #[test]
    fn an_engine_behind_the_node_wakes_the_follower() {
        let (sim, engine) = behind();
        let tree = sim.lock().trees()[0].clone();
        let wake = Wake::default();
        let reader = BalanceReader::new(sim.clone()).waking(wake.clone());
        let attempt = ready(reader.try_track(&engine, &tree)).unwrap();
        assert_eq!(attempt, Attempt::Behind);
        assert!(wake.take());
    }

    /// A track right after a block settles a `wake_gap` and a `poll` after the follower's
    /// last tick, not at its next one.
    #[tokio::test(start_paused = true)]
    async fn a_track_right_after_a_block_does_not_wait_for_the_next_tick() {
        let chain = new_chain();
        let engine = std::sync::Arc::new(synced_engine(&chain));
        let sim = SimChain::new(chain);
        let follower_config = follower::Config::default();
        let follower = Follower::new(sim.clone(), follower_config);
        let reader = BalanceReader::new(sim.clone()).waking(follower.waker());
        let follower_engine = engine.clone();
        let task = tokio::spawn(async move { follower.run(&*follower_engine).await });
        tokio::time::sleep(Duration::from_millis(10)).await;

        sim.lock().mine(0, 1, 1);
        let tree = sim.lock().trees()[0].clone();
        let config = Config::default();
        let started = Instant::now();
        reader.track(&*engine, &tree, config).await.unwrap();
        let waited = started.elapsed();
        let most = follower_config.wake_gap + config.poll;
        assert!(waited <= most, "waited {waited:?}, at most {most:?}");
        task.abort();
    }

    /// A block during the balance read: the balance may be either block's, so it is dropped.
    #[test]
    fn a_block_during_the_read_drops_it() {
        let chain = new_chain();
        let engine = synced_engine(&chain);
        let sim = SimChain::new(chain);
        let tree = sim.lock().trees()[0].clone();
        sim.on_call(move |endpoint, chain| {
            if endpoint == Endpoint::ConfirmedBalance {
                chain.mine(0, 1, 1);
            }
        });
        let reader = BalanceReader::new(sim.clone());
        let attempt = ready(reader.try_track(&engine, &tree)).unwrap();
        assert_eq!((attempt, attempt.checks()), (Attempt::Lost, Checks::Passed));
        assert_eq!(tracked(&engine, &tree), None);
    }

    /// The engine moves after its tip was read, the node's tip staying: it rejects the load,
    /// and the attempt is not taken as tracked.
    #[test]
    fn a_load_the_engine_rejects_is_not_tracked() {
        let chain = new_chain();
        let engine = std::sync::Arc::new(synced_engine(&chain));
        let sim = SimChain::new(chain);
        let tree = sim.lock().trees()[0].clone();
        let moved = engine.clone();
        sim.on_call(move |endpoint, _| {
            if endpoint == Endpoint::ConfirmedBalance {
                let mut engine = moved.lock().unwrap();
                let tip = engine.tip().unwrap();
                engine.apply(Event::BlockReverted(tip)).unwrap();
            }
        });
        let reader = BalanceReader::new(sim.clone());
        let attempt = ready(reader.try_track(&*engine, &tree)).unwrap();
        assert_eq!((attempt, attempt.checks()), (Attempt::Lost, Checks::Passed));
        assert_eq!(tracked(&engine, &tree), None);
    }

    /// A balance-only key counts as one against the cap on tracked boxes: at a full cap with
    /// no idle key to evict, it is refused, and nothing is tracked.
    #[test]
    fn a_full_cap_refuses_a_balance_load() {
        let chain = new_chain();
        let mut engine = Engine::new(EngineConfig { max_tracked_units: 0, ..Default::default() });
        engine
            .apply(Event::BlockApplied(chain.oracle().indexed_block(chain.height())))
            .unwrap();
        let engine = Mutex::new(engine);
        let sim = SimChain::new(chain);
        let tree = sim.lock().trees()[0].clone();
        let reader = BalanceReader::new(sim.clone());
        let refused = ready(reader.try_track(&engine, &tree));
        assert!(
            matches!(refused, Err(TrackError::CapacityExceeded { tracked: 0, evictable: 0 })),
            "{refused:?}"
        );
        assert_eq!(tracked(&engine, &tree), None);
    }

    /// An engine a block behind the node: the follower has not ticked yet.
    fn behind() -> (SimChain, Mutex<Engine>) {
        let chain = new_chain();
        let engine = synced_engine(&chain);
        let sim = SimChain::new(chain);
        sim.lock().mine(0, 1, 1);
        (sim, engine)
    }

    fn catch_up(sim: &SimChain, engine: &Mutex<Engine>) {
        let block = {
            let chain = sim.lock();
            chain.oracle().indexed_block(chain.height())
        };
        engine
            .lock()
            .unwrap()
            .apply(Event::BlockApplied(block))
            .unwrap();
    }

    /// Right after a block the engine trails the node by up to a follower tick: a track
    /// polls, so it settles within a `poll` of the engine catching up, not a `retry`.
    #[tokio::test(start_paused = true)]
    async fn a_track_settles_within_a_poll_of_the_engine_catching_up() {
        let (sim, engine) = behind();
        let tree = sim.lock().trees()[0].clone();
        let reader = BalanceReader::new(sim.clone());
        let config = Config::default();
        let started = Instant::now();
        let track = reader.track(&engine, &tree, config);
        let tick = async {
            sleep(Duration::from_millis(300)).await;
            catch_up(&sim, &engine);
        };
        let (tracked_now, ()) = tokio::join!(track, tick);
        tracked_now.unwrap();
        assert_eq!(started.elapsed(), 2 * config.poll, "the poll after the catch-up");
        assert!(tracked(&engine, &tree).is_some());
    }

    /// The quick polls last a `retry`: an engine or indexer still behind after that is no
    /// block's first second, so the track falls back to one attempt a `retry`.
    #[tokio::test(start_paused = true)]
    async fn a_track_polls_for_a_retry_then_waits_retries() {
        let (sim, engine) = behind();
        let tree = sim.lock().trees()[0].clone();
        let reader = BalanceReader::new(sim.clone());
        let config = Config::default();
        let started = Instant::now();
        let error = reader.track(&engine, &tree, config).await.unwrap_err();
        // Polls at 0, 200, 400, 600 and 800 ms and at 1 s, then a retry at 2 s; a third
        // retry would start at 3 s.
        assert!(matches!(*error, TrackError::NotSettled { attempts: 7 }), "{error:?}");
        assert_eq!(count(&sim, Endpoint::Info), 7);
        assert_eq!(started.elapsed(), Duration::from_secs(2), "no wait after the last");
    }

    /// The run of polls is the reader's, not one track's: a later track, of the same address
    /// or another, finds the run over and waits retries.
    #[tokio::test(start_paused = true)]
    async fn later_tracks_share_the_run_of_polls() {
        let (sim, engine) = behind();
        let trees = sim.lock().trees()[..2].to_vec();
        let reader = BalanceReader::new(sim.clone());
        let config = Config::default();
        let attempts = |error: Arc<TrackError>| match *error {
            TrackError::NotSettled { attempts } => attempts,
            ref other => panic!("{other:?}"),
        };
        let first = reader.track(&engine, &trees[0], config).await.unwrap_err();
        assert_eq!(attempts(first), 7);
        let again = reader.track(&engine, &trees[0], config).await.unwrap_err();
        assert_eq!(attempts(again), 3, "retries only, at 0, 1 and 2 s");
        let other = reader.track(&engine, &trees[1], config).await.unwrap_err();
        assert_eq!(attempts(other), 3);
    }

    /// A track that settles ends the run of polls: the next block's lag is a new run, polled
    /// from its start, even within two `retry`s of the last one.
    #[tokio::test(start_paused = true)]
    async fn a_settled_track_ends_the_run_of_polls() {
        let (sim, engine) = behind();
        let trees = sim.lock().trees()[..2].to_vec();
        let reader = BalanceReader::new(sim.clone());
        let config = Config::default();
        let track_after_a_block = async |tree: &ErgoTree| {
            let started = Instant::now();
            let tick = async {
                sleep(Duration::from_millis(300)).await;
                catch_up(&sim, &engine);
            };
            let (tracked_now, ()) = tokio::join!(reader.track(&engine, tree, config), tick);
            tracked_now.unwrap();
            started.elapsed()
        };
        assert_eq!(track_after_a_block(&trees[0]).await, 2 * config.poll);
        sleep(Duration::from_millis(1_100)).await;
        sim.lock().mine(0, 1, 1);
        assert_eq!(track_after_a_block(&trees[1]).await, 2 * config.poll, "a new run");
    }

    /// A read lost to a block that landed during it: the engine is a tick behind from then
    /// on, so the track polls rather than waits a `retry`.
    #[tokio::test(start_paused = true)]
    async fn a_lost_read_is_followed_by_polls() {
        let chain = new_chain();
        let engine = synced_engine(&chain);
        let sim = SimChain::new(chain);
        let tree = sim.lock().trees()[0].clone();
        let mined = std::sync::atomic::AtomicBool::new(false);
        sim.on_call(move |endpoint, chain| {
            if endpoint == Endpoint::ConfirmedBalance
                && !mined.swap(true, std::sync::atomic::Ordering::Relaxed)
            {
                chain.mine(0, 1, 1);
            }
        });
        let reader = BalanceReader::new(sim.clone());
        let config = Config::default();
        let started = Instant::now();
        let tick = async {
            sleep(Duration::from_millis(300)).await;
            catch_up(&sim, &engine);
        };
        let (tracked_now, ()) = tokio::join!(reader.track(&engine, &tree, config), tick);
        tracked_now.unwrap();
        // Lost at 0, behind at 200 ms, tracked at 400 ms.
        assert_eq!(started.elapsed(), 2 * config.poll);
        assert!(tracked(&engine, &tree).is_some());
    }
}
