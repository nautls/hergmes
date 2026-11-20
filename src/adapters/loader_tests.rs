//! The loader against `SimChain` (pinning, paging, the waits, a block between pages, a
//! recovering indexer, a rejection, the retry loop) and against `wiremock` (a box that does
//! not match the key).

use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};

use tokio::time::Instant;

use super::*;
use crate::{
    adapters::{BoxCache, BoxFetcher, EngineGone, Follower, follower},
    engine::{Config as EngineConfig, Engine, EngineError, Query, Reply},
    sim::{
        Chain, Endpoint, SimChain, SimConfig,
        testing::{Yielding, finish, new_chain, poll_once, ready},
    },
};

fn synced_engine(chain: &Chain) -> Mutex<Engine> {
    let mut engine = Engine::new(EngineConfig::default());
    engine
        .apply(Event::BlockApplied(chain.oracle().indexed_block(chain.height())))
        .unwrap();
    Mutex::new(engine)
}

/// The key's boxes in the engine, and the oracle's at the node's tip.
fn engine_and_oracle(
    sim: &SimChain,
    engine: &Mutex<Engine>,
    key: &TrackingKey,
) -> (Vec<crate::engine::BoxKey>, Vec<crate::engine::BoxKey>) {
    let tip = sim.lock().tip();
    let expected = sim
        .lock()
        .oracle()
        .unspent(key, tip.height)
        .into_keys()
        .collect();
    let query = Query::Page { key: key.clone(), at: Some(tip), after: None, limit: usize::MAX };
    match engine.lock().unwrap().query(query) {
        Reply::Page(Ok(page)) => (page.boxes, expected),
        other => panic!("{other:?}"),
    }
}

fn tree_key(sim: &SimChain, tree: usize) -> TrackingKey {
    TrackingKey::Tree(sim.lock().trees()[tree].clone())
}

fn count(sim: &SimChain, endpoint: Endpoint) -> usize {
    sim.calls().iter().filter(|e| **e == endpoint).count()
}

#[test]
fn a_key_is_loaded_at_the_tip() {
    let chain = new_chain();
    let engine = synced_engine(&chain);
    let sim = SimChain::new(chain);
    let key = tree_key(&sim, 0);
    let loader = Loader::new(sim.clone(), Config::default());
    let tip = sim.lock().tip();
    let outcome = ready(loader.try_load(&engine, &key)).unwrap();
    let (boxes, expected) = engine_and_oracle(&sim, &engine, &key);
    assert_eq!(outcome, Attempt::Tracked { as_of: tip, boxes: expected.len() });
    assert_eq!(boxes, expected);
    use Endpoint::{IndexedHeight, Info, UnspentByErgoTree};
    assert_eq!(sim.calls(), [Info, IndexedHeight, UnspentByErgoTree, Info]);
}

#[test]
fn pages_are_read_until_a_short_one() {
    // A token split over four boxes. One box per page ends with an empty page; three per
    // page ends with a page of one.
    for (page_limit, reads) in [(1, 5), (3, 2)] {
        let chain = new_chain();
        let engine = synced_engine(&chain);
        let sim = SimChain::new(chain);
        let key = TrackingKey::Token(crate::sim::token_id("small-100"));
        let loader = Loader::new(sim.clone(), Config { page_limit, ..Config::default() });
        let outcome = ready(loader.try_load(&engine, &key)).unwrap();
        assert!(matches!(outcome, Attempt::Tracked { boxes: 4, .. }), "{outcome:?}");
        assert_eq!(count(&sim, Endpoint::UnspentByToken), reads, "{page_limit} per page");
        let (boxes, expected) = engine_and_oracle(&sim, &engine, &key);
        assert_eq!(boxes, expected);
    }
}

#[test]
fn an_engine_behind_the_node_is_not_loaded_for() {
    let chain = new_chain();
    let engine = synced_engine(&chain);
    let sim = SimChain::new(chain);
    let before = engine.lock().unwrap().tip();
    let node = sim.lock().mine(0, 1, 1);
    let key = tree_key(&sim, 0);
    let loader = Loader::new(sim.clone(), Config::default());
    assert_eq!(
        ready(loader.try_load(&engine, &key)).unwrap(),
        Attempt::EngineBehind { engine: before, node }
    );
    assert_eq!(count(&sim, Endpoint::UnspentByErgoTree), 0);
}

#[test]
fn an_indexer_behind_the_tip_is_waited_for() {
    let mut chain = new_chain();
    chain.set_indexer_lag(1);
    let mut engine = Engine::new(EngineConfig::default());
    // The engine took the tip's block from elsewhere; the indexer has not reached it.
    let tip = chain.mine(0, 1, 1);
    engine
        .apply(Event::BlockApplied(chain.oracle().indexed_block(tip.height)))
        .unwrap();
    let engine = Mutex::new(engine);
    let sim = SimChain::new(chain);
    let key = tree_key(&sim, 0);
    let loader = Loader::new(sim.clone(), Config::default());
    assert_eq!(
        ready(loader.try_load(&engine, &key)).unwrap(),
        Attempt::IndexerBehind { indexed_height: tip.height - 1, tip_height: tip.height }
    );
    assert_eq!(count(&sim, Endpoint::UnspentByErgoTree), 0);
}

#[test]
fn a_block_between_pages_drops_the_load() {
    let chain = new_chain();
    let engine = synced_engine(&chain);
    let sim = SimChain::new(chain);
    let key = TrackingKey::Token(crate::sim::token_id("small-100"));
    let mut reads = 0;
    sim.on_call(move |endpoint, chain| {
        if endpoint == Endpoint::UnspentByToken {
            reads += 1;
            if reads == 2 {
                chain.mine(0, 1, 1);
            }
        }
    });
    let loader = Loader::new(sim.clone(), Config { page_limit: 1, ..Config::default() });
    assert_eq!(ready(loader.try_load(&engine, &key)).unwrap(), Attempt::TipMoved);
    assert!(!engine.lock().unwrap().is_tracked(&key));
}

#[test]
fn a_recovering_indexer_is_a_wait() {
    let mut chain = Chain::new(SimConfig::default());
    chain.mine(0, 1, 1);
    // A same-height reorg orphans the indexer's tip; the engine follows it by hand.
    chain.orphan(1);
    let tip = chain.mine(0, 2, 2);
    let mut engine = Engine::new(EngineConfig::default());
    engine
        .apply(Event::BlockApplied(chain.oracle().indexed_block(tip.height)))
        .unwrap();
    let engine = Mutex::new(engine);
    let sim = SimChain::new(chain);
    let key = tree_key(&sim, 1);
    let loader = Loader::new(sim.clone(), Config::default());
    let error = ready(loader.try_load(&engine, &key)).unwrap_err();
    assert!(
        matches!(error, LoadError::Node(NodeError::Unavailable { retry_after: Some(_) })),
        "{error:?}"
    );
    assert!(!engine.lock().unwrap().is_tracked(&key));
}

/// An engine that reverts its tip just before a load reaches it, as if a block event won the
/// race to the inbox.
struct Racing {
    inner: Mutex<Engine>,
    armed: AtomicBool,
}

impl EngineHandle for Racing {
    async fn apply(&self, event: Event) -> Result<Result<Vec<Output>, EngineError>, EngineGone> {
        let mut engine = self.inner.lock().unwrap();
        if matches!(event, Event::KeyLoaded { .. }) && self.armed.swap(false, Ordering::SeqCst) {
            let tip = engine.tip().expect("a tip");
            engine.apply(Event::BlockReverted(tip)).unwrap();
        }
        Ok(engine.apply(event))
    }

    async fn query(&self, query: Query) -> Result<Reply, EngineGone> {
        Ok(self.inner.lock().unwrap().query(query))
    }
}

#[test]
fn a_load_the_engine_rejects_is_reported() {
    let mut chain = new_chain();
    chain.mine(0, 1, 1);
    let mut engine = Engine::new(EngineConfig::default());
    for height in chain.height() - 1..=chain.height() {
        engine
            .apply(Event::BlockApplied(chain.oracle().indexed_block(height)))
            .unwrap();
    }
    let engine = Racing { inner: Mutex::new(engine), armed: AtomicBool::new(true) };
    let sim = SimChain::new(chain);
    let key = tree_key(&sim, 0);
    let loader = Loader::new(sim.clone(), Config::default());
    assert_eq!(ready(loader.try_load(&engine, &key)).unwrap(), Attempt::Rejected);
    assert!(!engine.inner.lock().unwrap().is_tracked(&key));
}

/// The ids `cache` holds out of `key`'s boxes at the node's tip, and how many boxes the key
/// has.
fn seeded(sim: &SimChain, cache: &BoxCache, key: &TrackingKey) -> (usize, usize) {
    let tip = sim.lock().tip();
    let boxes = sim.lock().oracle().unspent(key, tip.height);
    let held = boxes
        .keys()
        .filter(|k| cache.get(&k.box_id).is_some())
        .count();
    (held, boxes.len())
}

/// The loader seeds the box cache from a load the engine accepts, and from one it ignores
/// because the key is already tracked; never from a load it rejects.
#[test]
fn a_load_seeds_the_cache_unless_the_engine_rejects_it() {
    // Accepted.
    let chain = new_chain();
    let engine = synced_engine(&chain);
    let sim = SimChain::new(chain);
    let key = tree_key(&sim, 0);
    let cache = BoxCache::new(1 << 20);
    let loader = Loader::new(sim.clone(), Config::default()).seeding(cache.clone());
    let outcome = ready(loader.try_load(&engine, &key)).unwrap();
    assert!(matches!(outcome, Attempt::Tracked { .. }), "{outcome:?}");
    let (held, all) = seeded(&sim, &cache, &key);
    assert!(all > 0);
    assert_eq!(held, all, "every box of an accepted load");

    // A duplicate: the key is tracked already, and the engine ignores the second load.
    let cache = BoxCache::new(1 << 20);
    let again = Loader::new(sim.clone(), Config::default()).seeding(cache.clone());
    let outcome = ready(again.try_load(&engine, &key)).unwrap();
    assert!(matches!(outcome, Attempt::Tracked { .. }), "{outcome:?}");
    assert_eq!(seeded(&sim, &cache, &key), (all, all), "every box of a duplicate");

    // Rejected: the engine moved between the tip read and the load.
    let mut chain = new_chain();
    chain.mine(0, 1, 1);
    let mut engine = Engine::new(EngineConfig::default());
    for height in chain.height() - 1..=chain.height() {
        engine
            .apply(Event::BlockApplied(chain.oracle().indexed_block(height)))
            .unwrap();
    }
    let engine = Racing { inner: Mutex::new(engine), armed: AtomicBool::new(true) };
    let sim = SimChain::new(chain);
    let key = tree_key(&sim, 0);
    let cache = BoxCache::new(1 << 20);
    let loader = Loader::new(sim.clone(), Config::default()).seeding(cache.clone());
    assert_eq!(ready(loader.try_load(&engine, &key)).unwrap(), Attempt::Rejected);
    assert!(cache.boxes().is_empty(), "nothing from a rejected load");
}

/// The common case, a page right after a load (snapshot, then subscribe), is served from
/// the boxes the load seeded, with no node call.
#[test]
fn a_page_right_after_a_load_makes_no_node_call() {
    let chain = new_chain();
    let engine = synced_engine(&chain);
    let sim = SimChain::new(chain);
    let key = tree_key(&sim, 0);
    let cache = BoxCache::new(1 << 20);
    let loader = Loader::new(sim.clone(), Config::default()).seeding(cache.clone());
    ready(loader.try_load(&engine, &key)).unwrap();

    let (page, _) = engine_and_oracle(&sim, &engine, &key);
    let ids: Vec<BoxId> = page.iter().map(|k| k.box_id).collect();
    assert!(!ids.is_empty());
    let before = sim.calls().len();
    let fetcher = BoxFetcher::new(sim.clone(), cache);
    let boxes = ready(fetcher.fetch(&engine, &ids)).unwrap();
    assert!(boxes.iter().all(Option::is_some));
    assert_eq!(sim.calls().len(), before, "no node call");
}

/// A key over the per-key cap is refused at the first page that takes it over, without
/// paging it in full. A key at the cap exactly is loaded.
#[test]
fn a_key_over_the_per_key_cap_is_refused_without_paging_it_all() {
    // A token split over four boxes.
    let key = TrackingKey::Token(crate::sim::token_id("small-100"));
    for (page_limit, max_boxes, reads) in [(1, 2, 3), (16384, 2, 1), (3, 3, 2)] {
        let chain = new_chain();
        let engine = synced_engine(&chain);
        let sim = SimChain::new(chain);
        let cache = BoxCache::new(1 << 20);
        let config = Config { page_limit, max_boxes, ..Config::default() };
        let loader = Loader::new(sim.clone(), config).seeding(cache.clone());
        let error = ready(loader.load(&engine, &key)).unwrap_err();
        assert!(matches!(*error, LoadError::TooManyBoxes { max } if max == max_boxes), "{error:?}");
        assert_eq!(count(&sim, Endpoint::UnspentByToken), reads, "{page_limit} per page");
        assert_eq!(count(&sim, Endpoint::Info), 1, "not retried");
        assert!(!engine.lock().unwrap().is_tracked(&key));
        assert!(cache.boxes().is_empty());
    }

    let chain = new_chain();
    let engine = synced_engine(&chain);
    let sim = SimChain::new(chain);
    let loader = Loader::new(sim.clone(), Config { max_boxes: 4, ..Config::default() });
    assert!(matches!(ready(loader.load(&engine, &key)), Ok((_, 4))));
}

/// By default a key over the cap is refused at its first page: a key with more boxes fills
/// it. The node reads every box before a page's offset (`Segment.retrieveUtxos`, see
/// `MAX_BOXES`), so each later page costs more than the one before, and a fourth took over
/// the client's timeout.
#[test]
fn the_default_cap_is_one_page() {
    assert_eq!(MAX_BOXES, MAX_PAGE_LIMIT as usize - 1);
    assert_eq!(Config::default().page_limit, MAX_PAGE_LIMIT);
}

/// A key over the per-key cap is refused again with no node call until the refusal expires,
/// then paged again. A refusal for the total cap is not remembered: it frees up as keys go
/// idle.
#[test]
fn a_key_over_the_per_key_cap_is_remembered_for_a_while() {
    let ttl = Duration::from_millis(200);
    let key = TrackingKey::Token(crate::sim::token_id("small-100"));
    let chain = new_chain();
    let engine = synced_engine(&chain);
    let sim = SimChain::new(chain);
    let config = Config { max_boxes: 2, refusal_ttl: ttl, ..Config::default() };
    let loader = Loader::new(sim.clone(), config);
    let refused = |loader: &Loader<SimChain>| {
        let error = ready(loader.load(&engine, &key)).unwrap_err();
        assert!(matches!(*error, LoadError::TooManyBoxes { max: 2 }), "{error:?}");
    };
    refused(&loader);
    let calls = sim.calls().len();
    assert!(calls > 0);
    refused(&loader);
    assert_eq!(sim.calls().len(), calls, "no node call while remembered");
    std::thread::sleep(ttl + Duration::from_millis(100));
    refused(&loader);
    assert_eq!(sim.calls().len(), 2 * calls, "paged again once it expired");

    let chain = new_chain();
    let mut engine = Engine::new(EngineConfig { max_tracked_boxes: 1, ..EngineConfig::default() });
    engine
        .apply(Event::BlockApplied(chain.oracle().indexed_block(chain.height())))
        .unwrap();
    let engine = Mutex::new(engine);
    let sim = SimChain::new(chain);
    let loader = Loader::new(sim.clone(), Config::default());
    for attempt in 1..=2 {
        let error = ready(loader.load(&engine, &key)).unwrap_err();
        assert!(matches!(*error, LoadError::CapacityExceeded { .. }), "{error:?}");
        assert_eq!(count(&sim, Endpoint::UnspentByToken), attempt, "loaded every time");
    }
}

/// A load the engine refuses for the cap on tracked boxes ends the load at once and seeds
/// nothing: the key is not tracked.
#[test]
fn a_load_past_the_cap_on_tracked_boxes_is_refused_and_seeds_nothing() {
    let chain = new_chain();
    let mut engine = Engine::new(EngineConfig { max_tracked_boxes: 1, ..EngineConfig::default() });
    engine
        .apply(Event::BlockApplied(chain.oracle().indexed_block(chain.height())))
        .unwrap();
    let engine = Mutex::new(engine);
    let sim = SimChain::new(chain);
    let key = TrackingKey::Token(crate::sim::token_id("small-100"));
    let cache = BoxCache::new(1 << 20);
    let loader = Loader::new(sim.clone(), Config::default()).seeding(cache.clone());
    let error = ready(loader.load(&engine, &key)).unwrap_err();
    assert!(
        matches!(*error, LoadError::CapacityExceeded { tracked: 0, evictable: 0, boxes: 4 }),
        "{error:?}"
    );
    assert_eq!(count(&sim, Endpoint::Info), 1, "one attempt, stopped at its page, not retried");
    assert!(!engine.lock().unwrap().is_tracked(&key));
    assert!(cache.boxes().is_empty(), "nothing from a refused load");
}

fn engine_with_room(chain: &Chain, max_tracked_boxes: usize) -> Mutex<Engine> {
    let mut engine = Engine::new(EngineConfig { max_tracked_boxes, ..EngineConfig::default() });
    engine
        .apply(Event::BlockApplied(chain.oracle().indexed_block(chain.height())))
        .unwrap();
    Mutex::new(engine)
}

/// A load stops at the first page past the room the engine has left, without paging the key
/// in full: a full engine costs one page. The engine is not asked.
#[test]
fn a_load_past_the_room_left_stops_paging() {
    // A token split over four boxes, a box per page.
    let key = TrackingKey::Token(crate::sim::token_id("small-100"));
    for (room, reads) in [(0, 1), (2, 3)] {
        let chain = new_chain();
        let engine = engine_with_room(&chain, room);
        let sim = SimChain::new(chain);
        let loader = Loader::new(sim.clone(), Config { page_limit: 1, ..Config::default() });
        let error = ready(loader.load(&engine, &key)).unwrap_err();
        let LoadError::CapacityExceeded { tracked, evictable, boxes } = *error else {
            panic!("{error:?}");
        };
        assert_eq!((tracked, evictable, boxes), (0, 0, room + 1));
        assert_eq!(count(&sim, Endpoint::UnspentByToken), reads, "room for {room}");
        assert_eq!(count(&sim, Endpoint::Info), 1, "stopped before the second tip read");
        assert!(!engine.lock().unwrap().is_tracked(&key));
    }

    // A key with no box fits a full engine.
    let chain = new_chain();
    let engine = engine_with_room(&chain, 0);
    let sim = SimChain::new(chain);
    let empty = TrackingKey::Tree(crate::types::HexBytes(vec![0x00, 0x08, 0xcd]));
    let loader = Loader::new(sim.clone(), Config { page_limit: 1, ..Config::default() });
    assert!(matches!(ready(loader.load(&engine, &empty)), Ok((_, 0))));
}

/// A key the engine already tracks is never refused for room, even by a full engine: the
/// load is a duplicate the engine ignores.
#[test]
fn a_tracked_key_is_not_refused_for_room() {
    let key = TrackingKey::Token(crate::sim::token_id("small-100"));
    let chain = new_chain();
    let engine = engine_with_room(&chain, 4);
    let sim = SimChain::new(chain);
    let loader = || Loader::new(sim.clone(), Config { page_limit: 1, ..Config::default() });
    assert!(matches!(ready(loader().load(&engine, &key)), Ok((_, 4))));
    let reply = engine
        .lock()
        .unwrap()
        .query(Query::Capacity { key: key.clone(), up_to: 4 });
    assert_eq!(reply, Reply::Capacity(None));
    assert!(matches!(ready(loader().load(&engine, &key)), Ok((_, 4))), "a duplicate");
}

/// Two loads of one key at once are one load: the second waits for the first and gets its
/// outcome. Nothing is kept: a load after they return runs again.
#[tokio::test]
async fn concurrent_loads_of_a_key_are_one_load() {
    let chain = new_chain();
    let engine = Yielding(synced_engine(&chain));
    let sim = SimChain::new(chain);
    let key = tree_key(&sim, 0);
    let loader = Loader::new(sim.clone(), Config::default());
    let (first, second) = tokio::join!(loader.load(&engine, &key), loader.load(&engine, &key));
    let first = first.unwrap();
    assert_eq!(second.unwrap(), first);
    assert_eq!(count(&sim, Endpoint::UnspentByErgoTree), 1, "one load");
    assert!(engine.0.lock().unwrap().is_tracked(&key));

    // The engine ignores this one as a duplicate, but it runs: no outcome was kept.
    loader.load(&engine, &key).await.unwrap();
    assert_eq!(count(&sim, Endpoint::UnspentByErgoTree), 2);
}

/// The caller running a load is dropped (its client disconnected) while another waits: the
/// waiting caller runs the load itself.
#[test]
fn a_load_whose_caller_is_dropped_is_run_by_the_one_waiting() {
    let chain = new_chain();
    let engine = Yielding(synced_engine(&chain));
    let sim = SimChain::new(chain);
    let key = tree_key(&sim, 0);
    let loader = Loader::new(sim.clone(), Config::default());

    let mut first = Box::pin(loader.load(&engine, &key));
    assert!(poll_once(first.as_mut()).is_pending(), "suspended at its first engine call");
    assert_eq!(count(&sim, Endpoint::Info), 1, "the first caller runs the load");
    let mut second = Box::pin(loader.load(&engine, &key));
    assert!(poll_once(second.as_mut()).is_pending(), "waiting for the first");
    assert_eq!(count(&sim, Endpoint::Info), 1, "the second waits rather than loading");
    drop(first);

    let (as_of, _) = finish(second.as_mut()).unwrap();
    assert_eq!(Some(as_of), engine.0.lock().unwrap().tip());
    assert!(engine.0.lock().unwrap().is_tracked(&key));
    assert_eq!(count(&sim, Endpoint::UnspentByErgoTree), 1, "only the second reached the pages");
}

#[tokio::test(start_paused = true)]
async fn load_retries_until_the_engine_catches_up() {
    let chain = new_chain();
    let engine = Arc::new(synced_engine(&chain));
    let sim = SimChain::new(chain);
    sim.lock().mine(1, 1, 1);
    let key = tree_key(&sim, 0);

    let follower_engine = engine.clone();
    let follower = Follower::new(sim.clone(), follower::Config::default());
    // The follower's first tick runs at once; the load's first attempt runs before it.
    let loader = Loader::new(sim.clone(), Config::default());
    let first = loader.try_load(&*engine, &key).await.unwrap();
    assert!(matches!(first, Attempt::EngineBehind { .. }), "{first:?}");
    let task = tokio::spawn(async move { follower.run(&*follower_engine).await });

    let (as_of, boxes) = loader.load(&*engine, &key).await.unwrap();
    assert_eq!(as_of, sim.lock().tip());
    assert_eq!(boxes, engine_and_oracle(&sim, &engine, &key).1.len());
    task.abort();
}

/// A load that finds the engine behind tries again a `poll` later, not a `retry` later: right
/// after a block the engine trails the node by up to a follower tick, and a load that slept a
/// whole second past its catching up would answer that much later.
#[tokio::test(start_paused = true)]
async fn a_load_polls_while_the_engine_catches_up() {
    let chain = new_chain();
    let engine = Arc::new(synced_engine(&chain));
    let sim = SimChain::new(chain);
    sim.lock().mine(1, 1, 1);
    let key = tree_key(&sim, 0);

    let catch_up = Duration::from_millis(300);
    let follower_engine = engine.clone();
    let follower = Follower::new(sim.clone(), follower::Config::default());
    let task = tokio::spawn(async move {
        sleep(catch_up).await;
        follower.run(&*follower_engine).await
    });

    let config = Config::default();
    let started = tokio::time::Instant::now();
    loader_of(&sim, config).load(&*engine, &key).await.unwrap();
    let waited = started.elapsed();
    assert!(waited >= catch_up && waited <= catch_up + config.poll, "{waited:?}");
    task.abort();
}

fn loader_of(sim: &SimChain, config: Config) -> Loader<SimChain> {
    Loader::new(sim.clone(), config)
}

/// While the engine stays behind, the load polls until its next attempt would start past
/// `give_up_after`, and hands the wait it skipped to the caller.
#[tokio::test(start_paused = true)]
async fn load_gives_up_at_its_deadline() {
    let chain = new_chain();
    let engine = synced_engine(&chain);
    let sim = SimChain::new(chain);
    let node = sim.lock().mine(0, 1, 1);
    let key = tree_key(&sim, 0);
    let config = Config { give_up_after: Duration::from_millis(500), ..Config::default() };
    let error = loader_of(&sim, config)
        .load(&engine, &key)
        .await
        .unwrap_err();
    let before = engine.lock().unwrap().tip();
    // Attempts at 0, 200 and 400 ms; a fourth would start at 600.
    assert!(
        matches!(
            *error,
            LoadError::GaveUp {
                attempts: 3,
                last: Some(Attempt::EngineBehind { engine, node: n }),
                wait,
            } if engine == before && n == node && wait == config.poll
        ),
        "{error:?}"
    );
    assert_eq!(count(&sim, Endpoint::Info), 3);
}

/// An attempt that paged the key and lost it (here the engine rejects it) waits a `retry`
/// before the next: a full reload is what the poll must not repeat five times a second.
#[tokio::test(start_paused = true)]
async fn a_rejected_load_waits_a_retry() {
    let mut chain = new_chain();
    chain.mine(0, 1, 1);
    let mut engine = Engine::new(EngineConfig::default());
    for height in chain.height() - 1..=chain.height() {
        engine
            .apply(Event::BlockApplied(chain.oracle().indexed_block(height)))
            .unwrap();
    }
    let engine = Racing { inner: Mutex::new(engine), armed: AtomicBool::new(true) };
    let sim = SimChain::new(chain);
    let key = tree_key(&sim, 0);
    let config = Config { give_up_after: Duration::from_millis(500), ..Config::default() };
    let error = loader_of(&sim, config)
        .load(&engine, &key)
        .await
        .unwrap_err();
    assert!(
        matches!(
            *error,
            LoadError::GaveUp { attempts: 1, last: Some(Attempt::Rejected), wait }
                if wait == config.retry
        ),
        "{error:?}"
    );
}

/// The quick polls last a `retry`: an engine or indexer still behind after that is no
/// block's first second (an indexer resyncing after a restart stays behind for hours), so
/// the load falls back to one attempt a `retry`, as cheap on the node as before the polls.
#[tokio::test(start_paused = true)]
async fn a_load_polls_for_a_retry_then_waits_retries() {
    let chain = new_chain();
    let engine = synced_engine(&chain);
    let sim = SimChain::new(chain);
    sim.lock().mine(0, 1, 1);
    let key = tree_key(&sim, 0);
    let config = Config { give_up_after: Duration::from_secs(3), ..Config::default() };
    let error = loader_of(&sim, config)
        .load(&engine, &key)
        .await
        .unwrap_err();
    // Polls at 0, 200, 400, 600 and 800 ms and at 1 s, then a retry at 2 s; a third retry
    // would start at 3 s.
    assert!(
        matches!(*error, LoadError::GaveUp { attempts: 7, wait, .. } if wait == config.retry),
        "{error:?}"
    );
    assert_eq!(count(&sim, Endpoint::Info), 7);
}

/// The run of quick polls is the loader's, not one load's: what it waits out (the engine or
/// the indexer behind the node) is the same for every key. A keeper that loads again after
/// giving up, or a client's load of another key, finds the run over and waits retries.
#[tokio::test(start_paused = true)]
async fn later_loads_share_the_run_of_polls() {
    let chain = new_chain();
    let engine = synced_engine(&chain);
    let sim = SimChain::new(chain);
    sim.lock().mine(0, 1, 1);
    let config = Config { give_up_after: Duration::from_secs(3), ..Config::default() };
    let loader = loader_of(&sim, config);
    let gave_up_after = |error: &LoadError| match error {
        LoadError::GaveUp { attempts, wait, .. } => (*attempts, *wait),
        other => panic!("{other:?}"),
    };

    // The first load polls its first second, as in `a_load_polls_for_a_retry_then_waits_retries`.
    let first = loader.load(&engine, &tree_key(&sim, 0)).await.unwrap_err();
    assert_eq!(gave_up_after(&first), (7, config.retry));
    // The same key again, as a keeper does: retries only, at 0, 1 and 2 s.
    let again = loader.load(&engine, &tree_key(&sim, 0)).await.unwrap_err();
    assert_eq!(gave_up_after(&again), (3, config.retry));
    // Another key: the same.
    let other = loader.load(&engine, &tree_key(&sim, 1)).await.unwrap_err();
    assert_eq!(gave_up_after(&other), (3, config.retry));
    assert_eq!(count(&sim, Endpoint::Info), 13);
}

/// A run of polls: the first `retry` of attempts that stopped before the pages, ended by one
/// that got past them, lapsed once none was behind for two `retry`s, and left as it is by an
/// error that says nothing of the checks.
#[test]
fn a_run_of_polls_starts_ends_and_lapses() {
    let retry = Duration::from_secs(1);
    let start = Instant::now();
    let at = |ms: u64| start + Duration::from_millis(ms);
    let mut polls = Polls::default();
    assert!(polls.record(at(0), Checks::Behind, retry), "a run's first attempt");
    assert!(polls.record(at(800), Checks::Behind, retry), "within its first retry");
    assert!(!polls.record(at(1_000), Checks::Behind, retry), "past it");
    // A node `503` in the middle of a long run does not start a new one.
    assert!(!polls.record(at(1_500), Checks::Unknown, retry));
    assert!(!polls.record(at(2_000), Checks::Behind, retry), "the same run");
    // Another load gets to its pages: the run ends, and the next behind attempt starts one.
    assert!(!polls.record(at(2_100), Checks::Passed, retry));
    assert!(polls.record(at(2_200), Checks::Behind, retry), "a new run");
    assert!(!polls.record(at(3_200), Checks::Behind, retry));
    // Every load gave up and the next attempt comes long after: a new run.
    assert!(!polls.record(at(5_200), Checks::Behind, retry), "two retries later, still one");
    assert!(polls.record(at(7_201), Checks::Behind, retry), "lapsed");

    // What each outcome says of the checks.
    assert_eq!(Checks::of(&Ok(Attempt::NoTip)), Checks::Behind);
    assert_eq!(Checks::of(&Ok(Attempt::TipMoved)), Checks::Passed);
    assert_eq!(Checks::of(&Err(LoadError::TooManyBoxes { max: 1 })), Checks::Passed);
    let unavailable = NodeError::Unavailable { retry_after: None };
    assert_eq!(Checks::of(&Err(LoadError::Node(unavailable))), Checks::Unknown);
}

/// [`Racing`], slow to take a load: as a large key's pages are on a busy node.
struct Slow {
    racing: Racing,
    delay: Duration,
}

impl EngineHandle for Slow {
    async fn apply(&self, event: Event) -> Result<Result<Vec<Output>, EngineError>, EngineGone> {
        if matches!(event, Event::KeyLoaded { .. }) {
            sleep(self.delay).await;
        }
        self.racing.apply(event).await
    }

    async fn query(&self, query: Query) -> Result<Reply, EngineGone> {
        self.racing.query(query).await
    }
}

/// The deadline counts from the first attempt's answer, so an attempt that took nearly all of
/// `give_up_after` and lost its load to a block still gets its retries.
#[tokio::test(start_paused = true)]
async fn a_slow_attempt_still_gets_its_retries() {
    let mut chain = new_chain();
    chain.mine(0, 1, 1);
    let mut engine = Engine::new(EngineConfig::default());
    for height in chain.height() - 1..=chain.height() {
        engine
            .apply(Event::BlockApplied(chain.oracle().indexed_block(height)))
            .unwrap();
    }
    let racing = Racing { inner: Mutex::new(engine), armed: AtomicBool::new(true) };
    let config = Config::default();
    let engine = Slow { racing, delay: config.give_up_after - Duration::from_millis(500) };
    let sim = SimChain::new(chain);
    let key = tree_key(&sim, 0);
    let error = loader_of(&sim, config)
        .load(&engine, &key)
        .await
        .unwrap_err();
    // Rejected, then the reverted engine is behind for every later attempt.
    assert!(
        matches!(
            *error,
            LoadError::GaveUp { attempts, last: Some(Attempt::EngineBehind { .. }), .. }
                if attempts > 1
        ),
        "{error:?}"
    );
}

/// [`Racing`] that answers its first tip read with no tip, as an engine a block behind would.
struct BehindOnce {
    racing: Racing,
    behind: AtomicBool,
}

impl EngineHandle for BehindOnce {
    async fn apply(&self, event: Event) -> Result<Result<Vec<Output>, EngineError>, EngineGone> {
        self.racing.apply(event).await
    }

    async fn query(&self, query: Query) -> Result<Reply, EngineGone> {
        if matches!(query, Query::Tip) && self.behind.swap(false, Ordering::SeqCst) {
            return Ok(Reply::Tip(None));
        }
        self.racing.query(query).await
    }
}

/// An attempt that paged ends a run of polls, so the next run polls again: a block that
/// lands during a load's pages leaves the engine behind for a moment, and that moment is
/// what the polls are for.
#[tokio::test(start_paused = true)]
async fn a_paged_attempt_starts_the_polls_again() {
    let mut chain = new_chain();
    chain.mine(0, 1, 1);
    let mut engine = Engine::new(EngineConfig::default());
    for height in chain.height() - 1..=chain.height() {
        engine
            .apply(Event::BlockApplied(chain.oracle().indexed_block(height)))
            .unwrap();
    }
    let racing = Racing { inner: Mutex::new(engine), armed: AtomicBool::new(true) };
    let engine = BehindOnce { racing, behind: AtomicBool::new(true) };
    let sim = SimChain::new(chain);
    let key = tree_key(&sim, 0);
    let config = Config { give_up_after: Duration::from_millis(1500), ..Config::default() };
    let error = loader_of(&sim, config)
        .load(&engine, &key)
        .await
        .unwrap_err();
    // Behind at 0, rejected at 200 ms, then behind (the revert) from 1.2 s: polled at 1.2
    // and 1.4 s, where a fresh run ends. Without the new run the 1.2 s attempt would wait a
    // `retry` and be the last.
    assert!(
        matches!(
            *error,
            LoadError::GaveUp { attempts: 4, last: Some(Attempt::EngineBehind { .. }), wait }
                if wait == config.poll
        ),
        "{error:?}"
    );
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
        fixtures::{self, load, sample},
        types::{HexBytes, address::Network, ergo::IndexedBlock},
    };

    fn recorded(name: &str) -> ResponseTemplate {
        ResponseTemplate::new(fixtures::status(name))
            .set_body_raw(fixtures::text(name), "application/json")
    }

    #[tokio::test]
    async fn a_box_that_does_not_match_the_key_is_refused() {
        // The engine is at the recorded block, and the node reports it as its tip.
        let block: IndexedBlock = load("indexed_block");
        let mut engine = Engine::new(EngineConfig::default());
        engine.apply(Event::BlockApplied(block.clone())).unwrap();
        let engine = Mutex::new(engine);

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
        // The recorded boxes of the sample tree, served for another tree.
        Mock::given(method("POST"))
            .and(path("/blockchain/box/unspent/byErgoTree"))
            .respond_with(recorded("unspent_by_ergo_tree"))
            .mount(&server)
            .await;

        let client = NodeClient::new(reqwest::Client::new(), &server.uri(), Network::Mainnet);
        let loader = Loader::new(client, Config::default());
        let mut other = hex::decode(sample("ergoTree")).unwrap();
        *other.last_mut().unwrap() ^= 1;
        let key = TrackingKey::Tree(HexBytes(other));
        let error = loader.try_load(&engine, &key).await.unwrap_err();
        assert!(matches!(error, LoadError::Mismatch { .. }), "{error:?}");
        assert!(!engine.lock().unwrap().is_tracked(&key));
    }

    /// No wait follows the last attempt, so a `503` there hands its `Retry-After` to the
    /// caller instead of losing it. A deadline of zero still makes one attempt.
    #[tokio::test]
    async fn a_503_on_the_last_attempt_hands_its_wait_to_the_caller() {
        let engine = Mutex::new(Engine::new(EngineConfig::default()));
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/info"))
            .respond_with(ResponseTemplate::new(503).insert_header("Retry-After", "30"))
            .mount(&server)
            .await;
        let client = NodeClient::new(reqwest::Client::new(), &server.uri(), Network::Mainnet);
        let config = Config { give_up_after: Duration::ZERO, ..Config::default() };
        let loader = Loader::new(client, config);
        let key = TrackingKey::Tree(HexBytes(hex::decode(sample("ergoTree")).unwrap()));
        let error = loader.load(&engine, &key).await.unwrap_err();
        assert!(
            matches!(
                *error,
                LoadError::GaveUp { attempts: 1, last: None, wait }
                    if wait == Duration::from_secs(30)
            ),
            "{error:?}"
        );
    }
}
