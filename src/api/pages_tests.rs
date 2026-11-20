//! The page service against `SimChain`. Random histories page through keys while
//! blocks, reorgs and follower ticks happen between pages, and blocks land between reading a
//! page's ids and fetching its boxes. Every snapshot is exactly the oracle's boxes at its
//! `asOf`, with no duplicates or gaps; when it is not served, the reason holds.

use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use proptest::{prelude::*, sample::Index};

use super::*;
use crate::{
    adapters::{
        BoxCache, EngineGone, Follower,
        blocks::Sync,
        follower,
        loader::{self, Loader},
    },
    clients::NodeError,
    engine::{BoxKey, Config, Engine, EngineError, Event, Output},
    sim::{
        Chain, Endpoint, SimChain, SimConfig, SimOp, strategy,
        testing::{Yielding, ready, utxo},
        token_id,
    },
    types::ergo::{TokenId, UTxO},
};

/// Small enough that reorgs reach past the window and keys go idle between pages: the
/// engine's clock moves a second per tick.
const CONFIG: Config = Config { window: 4, grace: Duration::from_secs(2), ..Config::DEFAULT };
const MAX_REORG: u8 = 5;
const CASES: u32 = 1_000;
const SUPPLY_LIMIT: u64 = 100;

#[derive(Debug, Clone)]
enum Op {
    Chain(SimOp),
    /// One follower tick, a second after the last.
    Tick,
    /// The indexer ends any recovery and catches up.
    Settle,
    /// A client starts a snapshot of a key, `limit` boxes per page, abandoning the one in
    /// progress. A follower tick comes first, as a client usually finds the relay caught up.
    Start {
        key: Index,
        limit: u32,
    },
    /// The next page of the snapshot in progress.
    Next,
    /// The node mines a block spending boxes of the snapshot's key while the next page's
    /// boxes are fetched, between the engine's ids and the node's answer.
    SpendDuringFetch(u64),
    /// The tip is replaced by another block at the same height, the indexer settles and the
    /// engine follows: a snapshot pinned to the old tip expires.
    Fork(u64),
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        5 => strategy::op(MAX_REORG).prop_map(Op::Chain),
        5 => Just(Op::Tick),
        1 => Just(Op::Settle),
        2 => (any::<Index>(), 1..=3u32).prop_map(|(key, limit)| Op::Start { key, limit }),
        8 => Just(Op::Next),
        1 => any::<u64>().prop_map(Op::SpendDuringFetch),
        1 => any::<u64>().prop_map(Op::Fork),
    ]
}

/// What random histories reached, for the coverage test.
#[derive(Debug, Default)]
struct Seen {
    loaded: bool,
    snapshot_of_several_pages: bool,
    restored: bool,
    indexer_fallback: bool,
    spent_during_fetch: bool,
    expired_off_chain: bool,
    expired_untracked: bool,
    not_ready: bool,
}

/// A snapshot in progress.
struct Snapshot {
    key: TrackingKey,
    limit: u32,
    as_of: Option<BlockRef>,
    cursor: Option<String>,
    items: Vec<StoredBox>,
    pages: usize,
}

/// What the node and the engine looked like just before a page request.
struct Before {
    node_tip: BlockRef,
    engine_tip: Option<BlockRef>,
    indexed_height: u32,
    recovering: bool,
}

struct Harness {
    sim: SimChain,
    engine: Mutex<Engine>,
    follower: Follower<SimChain>,
    pages: Pages<SimChain>,
    now: tokio::time::Instant,
    /// The engine's clock: a second per tick.
    elapsed: Duration,
    snapshot: Option<Snapshot>,
    spend_during_fetch: Option<u64>,
    seen: Seen,
}

impl Harness {
    fn new(cache: bool) -> Self {
        let sim = SimChain::new(Chain::new(SimConfig::default()));
        let cache = BoxCache::new(if cache { 1 << 20 } else { 0 });
        // One attempt, so a load that does not settle answers `not_ready` with no wait.
        let config =
            loader::Config { page_limit: 2, give_up_after: Duration::ZERO, ..Default::default() };
        let loader = Loader::new(sim.clone(), config).seeding(cache.clone());
        Self {
            follower: Follower::new(sim.clone(), follower::Config::default()),
            pages: Pages::new(
                Arc::new(loader),
                Arc::new(TokenChecker::new(sim.clone(), SUPPLY_LIMIT)),
                BoxFetcher::new(sim.clone(), cache),
            ),
            engine: Mutex::new(Engine::new(CONFIG)),
            now: tokio::time::Instant::now(),
            elapsed: Duration::ZERO,
            snapshot: None,
            spend_during_fetch: None,
            seen: Seen::default(),
            sim,
        }
    }

    fn run(&mut self, op: &Op) {
        match op {
            Op::Chain(op) => self.sim.lock().apply(op),
            Op::Tick => self.tick(),
            Op::Settle => self.sim.lock().settle_indexer(),
            Op::Start { key, limit } => {
                self.tick();
                let keys = self.sim.lock().keys();
                let key = index_get(key, &keys);
                self.snapshot = Some(Snapshot {
                    key,
                    limit: *limit,
                    as_of: None,
                    cursor: None,
                    items: Vec::new(),
                    pages: 0,
                });
                self.page();
            }
            Op::Next => self.page(),
            Op::SpendDuringFetch(seed) => self.spend_during_fetch = Some(*seed),
            Op::Fork(seed) => {
                {
                    let mut chain = self.sim.lock();
                    if chain.orphan(1) == 1 {
                        chain.mine(1, 2, *seed);
                    }
                    chain.settle_indexer();
                }
                self.tick();
            }
        }
    }

    fn tick(&mut self) {
        self.elapsed += Duration::from_secs(1);
        self.engine.lock().unwrap().set_now(self.elapsed);
        let tick =
            ready(self.follower.tick(&self.engine, self.now)).expect("SimChain serves /info");
        self.now += Duration::from_secs(1);
        if let Ok(Sync::Refused(error)) = tick.blocks {
            panic!("the engine refused a block event: {error}");
        }
    }

    fn before(&self) -> Before {
        let chain = self.sim.lock();
        Before {
            node_tip: chain.tip(),
            engine_tip: self.engine.lock().unwrap().tip(),
            indexed_height: chain.indexed_height().indexed_height,
            recovering: chain.is_recovering(),
        }
    }

    /// Arms the node to mine a block spending some of `key`'s boxes on the next
    /// `withPool/byIds` call, once. The flag tells whether it did.
    fn arm(&mut self, key: &TrackingKey) -> Arc<AtomicBool> {
        let mined = Arc::new(AtomicBool::new(false));
        let Some(seed) = self.spend_during_fetch.take() else {
            self.sim.on_call(|_, _| {});
            return mined;
        };
        let key = key.clone();
        let flag = mined.clone();
        self.sim.on_call(move |endpoint, chain| {
            if endpoint != Endpoint::BoxesWithPool || flag.load(Ordering::SeqCst) {
                return;
            }
            let held: Vec<BoxId> = chain
                .utxo()
                .values()
                .filter(|b| key.matches(b))
                .map(|b| b.id)
                .take(3)
                .collect();
            if held.is_empty() {
                return;
            }
            let to = chain.trees()[seed as usize % chain.trees().len()].clone();
            let tx = chain.transfer(&held, &to);
            chain.mine_txs(vec![tx]);
            flag.store(true, Ordering::SeqCst);
        });
        mined
    }

    fn page(&mut self) {
        let Some(snapshot) = &self.snapshot else {
            return;
        };
        if snapshot.pages > 0 && snapshot.cursor.is_none() {
            return;
        }
        let (key, limit, cursor) = (snapshot.key.clone(), snapshot.limit, snapshot.cursor.clone());
        let before = self.before();
        let mined = self.arm(&key);
        let calls = self.sim.calls().len();
        let result = ready(
            self.pages
                .page(&self.engine, &key, cursor.as_deref(), Some(limit)),
        );
        self.sim.on_call(|_, _| {});
        let made: Vec<Endpoint> = self.sim.calls()[calls..].to_vec();
        self.seen.spent_during_fetch |= mined.load(Ordering::SeqCst) && result.is_ok();
        self.seen.indexer_fallback |= made.contains(&Endpoint::IndexedBox) && result.is_ok();
        self.seen.loaded |= result.is_ok()
            && made
                .iter()
                .any(|e| matches!(e, Endpoint::UnspentByErgoTree | Endpoint::UnspentByToken));

        match result {
            Ok(page) => self.check_page(page),
            Err(ApiError::CursorExpired) => {
                let pin = self.snapshot.as_ref().and_then(|s| s.as_of);
                let pin = pin.expect("only a page after a cursor expires");
                self.check_expired(&key, pin);
                self.snapshot = None;
            }
            Err(ApiError::NotReady { .. }) => {
                assert!(cursor.is_none(), "only a first page loads");
                let settled = before.engine_tip == Some(before.node_tip)
                    && before.indexed_height == before.node_tip.height
                    && !before.recovering;
                assert!(!settled, "a load at a settled node does not give up");
                self.seen.not_ready = true;
                self.snapshot = None;
            }
            Err(ApiError::NodeUnavailable { .. }) => {
                assert!(before.recovering, "a 503 comes only from a recovering indexer");
                self.snapshot = None;
            }
            Err(ApiError::SupplyTooLarge { emission, .. }) => {
                assert!(emission > SUPPLY_LIMIT);
                self.snapshot = None;
            }
            Err(ApiError::UnknownToken) => {
                let TrackingKey::Token(id) = &key else { panic!("an address is no token") };
                assert!(self.sim.lock().token_info(id).is_none(), "the indexer knows {id}");
                self.snapshot = None;
            }
            Err(ApiError::BoxUnavailable { box_id }) => {
                // Every box of a pin on the node's chain is in its UTXO set or, spent since,
                // in its indexer, unless the indexer is recovering or below the pin.
                let pin = self.snapshot.as_ref().and_then(|s| s.as_of);
                let pin = pin.or(before.engine_tip).expect("a page has a pin");
                let on_node = self.sim.lock().header_at(pin.height) == Some(pin);
                assert!(
                    !on_node || before.recovering || before.indexed_height < pin.height,
                    "box {box_id} of {pin:?}, on the node's chain, must be found"
                );
                self.snapshot = None;
            }
            Err(error) => panic!("unexpected page error for {key:?}: {error}"),
        }
    }

    fn check_page(&mut self, page: BoxPage) {
        let snapshot = self.snapshot.as_mut().expect("a snapshot");
        let as_of = *snapshot.as_of.get_or_insert(page.as_of);
        assert_eq!(page.as_of, as_of, "every page is as of the first page's block");
        assert!(page.items.len() <= snapshot.limit as usize);
        if page.next_cursor.is_some() {
            assert_eq!(page.items.len(), snapshot.limit as usize, "only the last page is short");
        }
        snapshot.items.extend(page.items);
        snapshot.cursor = page.next_cursor;
        snapshot.pages += 1;

        // The pages so far are the oracle's boxes at `asOf`, in keyset order.
        let chain = self.sim.lock();
        let expected: Vec<(BoxKey, Arc<UTxO>)> = chain
            .oracle_at(&as_of.header_id)
            .unspent(&snapshot.key, as_of.height)
            .into_iter()
            .collect();
        let got: Vec<(BoxKey, Arc<UTxO>)> = snapshot
            .items
            .iter()
            .map(|b| {
                let key = BoxKey { inclusion_height: b.inclusion_height, box_id: b.utxo.id };
                (key, b.utxo.clone())
            })
            .collect();
        assert!(got.len() <= expected.len(), "more boxes than the oracle at {as_of:?}");
        assert_eq!(got, expected[..got.len()], "{:?} at {as_of:?}", snapshot.key);
        assert_eq!(
            snapshot.cursor.is_none(),
            got.len() == expected.len(),
            "`nextCursor` is null exactly on the last page"
        );
        if snapshot.cursor.is_none() {
            self.seen.snapshot_of_several_pages |= snapshot.pages >= 2;
        }

        // A box spent on the engine's chain since `asOf`: served from the ring buffer or the
        // indexer, since the node's UTXO set no longer has it.
        let engine_tip = self.engine.lock().unwrap().tip().expect("a tip");
        let now = chain
            .oracle_at(&engine_tip.header_id)
            .unspent(&snapshot.key, engine_tip.height);
        self.seen.restored |= got.iter().any(|(k, _)| !now.contains_key(k));
    }

    /// A cursor expires only when its block is off the engine's chain or outside its window,
    /// or the key is not tracked over it.
    fn check_expired(&mut self, key: &TrackingKey, pin: BlockRef) {
        let engine = self.engine.lock().unwrap();
        let reply = engine.clone().query(Query::Chain);
        let Reply::Chain(links) = reply else { panic!("a chain") };
        let on_chain = links.iter().any(|l| l.block == pin)
            || links.first().is_some_and(|oldest| {
                oldest.parent_id == pin.header_id && oldest.block.height == pin.height + 1
            });
        let since = engine.tracked_since(key);
        let tracked_over = since.is_some_and(|s| s.height <= pin.height);
        assert!(!on_chain || !tracked_over, "{pin:?} is served, yet the cursor expired");
        self.seen.expired_off_chain |= !on_chain;
        self.seen.expired_untracked |= on_chain && since.is_none();
    }
}

fn index_get(index: &Index, keys: &[TrackingKey]) -> TrackingKey {
    index.get(keys).clone()
}

fn run(cache: bool, ops: &[Op]) -> Seen {
    let mut h = Harness::new(cache);
    for op in ops {
        h.run(op);
    }
    h.seen
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(CASES))]

    #[test]
    fn a_snapshot_is_the_oracles_boxes_at_its_block(
        cache in prop::bool::weighted(0.3),
        ops in prop::collection::vec(op(), 0..=60),
    ) {
        run(cache, &ops);
    }
}

#[test]
fn random_histories_reach_the_hard_paging_cases() {
    let mut runner = proptest::test_runner::TestRunner::deterministic();
    let strategy = prop::collection::vec(op(), 40..=60);
    let mut all = Seen::default();
    for i in 0..300 {
        let ops = proptest::strategy::ValueTree::current(
            &proptest::strategy::Strategy::new_tree(&strategy, &mut runner).unwrap(),
        );
        let seen = run(i % 3 == 0, &ops);
        all.loaded |= seen.loaded;
        all.snapshot_of_several_pages |= seen.snapshot_of_several_pages;
        all.restored |= seen.restored;
        all.indexer_fallback |= seen.indexer_fallback;
        all.spent_during_fetch |= seen.spent_during_fetch;
        all.expired_off_chain |= seen.expired_off_chain;
        all.expired_untracked |= seen.expired_untracked;
        all.not_ready |= seen.not_ready;
    }
    let Seen {
        loaded,
        snapshot_of_several_pages,
        restored,
        indexer_fallback,
        spent_during_fetch,
        expired_off_chain,
        expired_untracked,
        not_ready,
    } = all;
    for (name, reached) in [
        ("a first page that loads its key", loaded),
        ("a snapshot of several pages", snapshot_of_several_pages),
        ("a page with a box spent since its block", restored),
        ("a box from the indexer", indexer_fallback),
        ("a block mined during a fetch", spent_during_fetch),
        ("a cursor expired by a reorg or the window", expired_off_chain),
        ("a cursor expired by an idle key", expired_untracked),
        ("a load that did not settle", not_ready),
    ] {
        assert!(reached, "no random history reached {name}");
    }
}

// --- Hand tests ---

/// An engine at the chain's tip, and the page service over the chain with a cache of
/// `cache` bytes.
fn service(chain: Chain, cache: u64) -> (SimChain, Mutex<Engine>, Pages<SimChain>) {
    let mut engine = Engine::new(Config::default());
    engine
        .apply(Event::BlockApplied(chain.oracle().indexed_block(chain.height())))
        .unwrap();
    let sim = SimChain::new(chain);
    let cache = BoxCache::new(cache);
    let loader = Loader::new(sim.clone(), loader::Config::default()).seeding(cache.clone());
    let pages = Pages::new(
        Arc::new(loader),
        Arc::new(TokenChecker::new(sim.clone(), SUPPLY_LIMIT)),
        BoxFetcher::new(sim.clone(), cache),
    );
    (sim, Mutex::new(engine), pages)
}

fn count(sim: &SimChain, endpoint: Endpoint) -> usize {
    sim.calls().iter().filter(|e| **e == endpoint).count()
}

fn tree_key(sim: &SimChain, tree: usize) -> TrackingKey {
    TrackingKey::Tree(sim.lock().trees()[tree].clone())
}

/// The oracle's boxes of `key` at the node's tip, as a page holds them.
fn expected(sim: &SimChain, key: &TrackingKey) -> Vec<StoredBox> {
    let chain = sim.lock();
    let tip = chain.tip();
    (chain.oracle().unspent(key, tip.height).into_iter())
        .map(|(k, utxo)| StoredBox { utxo, inclusion_height: k.inclusion_height })
        .collect()
}

/// The common case, a snapshot of a key no one tracks: the first page loads it and is served
/// from the boxes the load seeded, with no box call.
#[test]
fn a_first_page_loads_the_key_and_needs_no_box_call() {
    let (sim, engine, pages) = service(crate::sim::testing::new_chain(), 1 << 20);
    let key = tree_key(&sim, 0);
    let page = ready(pages.page(&engine, &key, None, None)).unwrap();
    assert_eq!(page.as_of, sim.lock().tip());
    assert_eq!(page.items, expected(&sim, &key));
    assert_eq!(page.next_cursor, None);
    assert!(engine.lock().unwrap().is_tracked(&key));
    assert_eq!(count(&sim, Endpoint::UnspentByErgoTree), 1);
    assert_eq!(count(&sim, Endpoint::BoxesWithPool), 0);
}

/// Two first requests for a key no one tracks, at once: one load serves both.
#[tokio::test]
async fn two_concurrent_first_pages_make_one_load() {
    let chain = crate::sim::testing::new_chain();
    let (sim, engine, pages) = service(chain, 1 << 20);
    let engine = Yielding(engine);
    let key = tree_key(&sim, 0);
    let (a, b) =
        tokio::join!(pages.page(&engine, &key, None, None), pages.page(&engine, &key, None, None));
    assert_eq!(a.unwrap(), b.unwrap());
    assert_eq!(count(&sim, Endpoint::UnspentByErgoTree), 1);
}

#[test]
fn a_cursor_for_another_key_is_invalid() {
    let (sim, engine, pages) = service(crate::sim::testing::new_chain(), 1 << 20);
    let (zero, one) = (tree_key(&sim, 0), tree_key(&sim, 1));
    let page = ready(pages.page(&engine, &zero, None, Some(1))).unwrap();
    let cursor = page.next_cursor.expect("more than one box");
    let error = ready(pages.page(&engine, &one, Some(&cursor), Some(1))).unwrap_err();
    assert_eq!(error, ApiError::InvalidCursor(crate::api::cursor::CursorError::OtherKey));
    assert!(!engine.lock().unwrap().is_tracked(&one), "nothing loaded");
}

#[test]
fn a_limit_is_from_1_to_1000() {
    let (sim, engine, pages) = service(crate::sim::testing::new_chain(), 1 << 20);
    let key = tree_key(&sim, 0);
    for limit in [0, 1001, u32::MAX] {
        let error = ready(pages.page(&engine, &key, None, Some(limit))).unwrap_err();
        assert_eq!(error, ApiError::InvalidLimit { max: MAX_LIMIT }, "{limit}");
    }
    assert!(sim.calls().is_empty(), "checked before anything else");
    assert!(ready(pages.page(&engine, &key, None, Some(1000))).is_ok());
}

/// A token over the supply limit is refused before any load, and an unknown one is
/// `unknown_token`.
#[test]
fn a_token_is_checked_before_it_is_loaded() {
    let (sim, engine, pages) = service(crate::sim::testing::new_chain(), 1 << 20);
    let over = TrackingKey::Token(token_id("over-101"));
    let error = ready(pages.page(&engine, &over, None, None)).unwrap_err();
    assert_eq!(error, ApiError::SupplyTooLarge { emission: 101, limit: SUPPLY_LIMIT });
    let unknown = TrackingKey::Token(TokenId::from(crate::sim::testing::digest(250)));
    let error = ready(pages.page(&engine, &unknown, None, None)).unwrap_err();
    assert_eq!(error, ApiError::UnknownToken);
    assert_eq!(count(&sim, Endpoint::UnspentByToken), 0);

    let small = TrackingKey::Token(token_id("small-100"));
    let page = ready(pages.page(&engine, &small, None, None)).unwrap();
    assert_eq!(page.items, expected(&sim, &small));
}

/// No load starts for a client until the configured keys settle: an untracked key is
/// `not_ready` with no node call, and a tracked key is served all the same.
#[test]
fn no_load_starts_until_the_configured_keys_settle() {
    let (sim, engine, pages) = service(crate::sim::testing::new_chain(), 0);
    let (settled, pending) = crate::runtime::settling(1);
    let pages = pages.waiting_for(settled);
    let (tracked, untracked) = (tree_key(&sim, 0), tree_key(&sim, 1));
    ready(Loader::new(sim.clone(), loader::Config::default()).load(&engine, &tracked)).unwrap();

    let calls = sim.calls().len();
    let error = ready(pages.page(&engine, &untracked, None, None)).unwrap_err();
    assert_eq!(error, ApiError::NotReady { retry_after: loader::Config::default().retry });
    assert_eq!(sim.calls().len(), calls, "no node call");
    assert!(!engine.lock().unwrap().is_tracked(&untracked));
    let page = ready(pages.page(&engine, &tracked, None, None)).unwrap();
    assert_eq!(page.items, expected(&sim, &tracked));

    drop(pending);
    let page = ready(pages.page(&engine, &untracked, None, None)).unwrap();
    assert_eq!(page.items, expected(&sim, &untracked));
}

/// A key over the per-key cap is `too_many_boxes`, and it stays untracked.
#[test]
fn a_key_over_the_cap_is_too_many_boxes() {
    let chain = crate::sim::testing::new_chain();
    let mut engine = Engine::new(Config::default());
    engine
        .apply(Event::BlockApplied(chain.oracle().indexed_block(chain.height())))
        .unwrap();
    let engine = Mutex::new(engine);
    let sim = SimChain::new(chain);
    let config = loader::Config { max_boxes: 3, ..Default::default() };
    let pages = Pages::new(
        Arc::new(Loader::new(sim.clone(), config)),
        Arc::new(TokenChecker::new(sim.clone(), SUPPLY_LIMIT)),
        BoxFetcher::new(sim.clone(), BoxCache::disabled()),
    );
    let small = TrackingKey::Token(token_id("small-100"));
    let error = ready(pages.page(&engine, &small, None, None)).unwrap_err();
    assert_eq!(error, ApiError::TooManyBoxes { max: 3 });
    assert_eq!(error.code(), "too_many_boxes");
    assert!(!engine.lock().unwrap().is_tracked(&small));
}

/// A key past the cap on tracked boxes is `capacity_exceeded`, and it stays untracked.
#[test]
fn a_key_past_the_total_cap_is_capacity_exceeded() {
    let chain = crate::sim::testing::new_chain();
    let mut engine = Engine::new(Config { max_tracked_boxes: 1, ..Config::default() });
    engine
        .apply(Event::BlockApplied(chain.oracle().indexed_block(chain.height())))
        .unwrap();
    let engine = Mutex::new(engine);
    let sim = SimChain::new(chain);
    let pages = Pages::new(
        Arc::new(Loader::new(sim.clone(), loader::Config::default())),
        Arc::new(TokenChecker::new(sim.clone(), SUPPLY_LIMIT)),
        BoxFetcher::new(sim.clone(), BoxCache::disabled()),
    );
    let small = TrackingKey::Token(token_id("small-100"));
    let error = ready(pages.page(&engine, &small, None, None)).unwrap_err();
    assert_eq!(error, ApiError::CapacityExceeded);
    assert_eq!(error.code(), "capacity_exceeded");
    assert!(!engine.lock().unwrap().is_tracked(&small));
}

/// A cursor for a key untracked since (idle past the grace period) expires without a load:
/// a load now would be pinned above the cursor's block anyway.
#[test]
fn a_cursor_for_a_key_gone_idle_expires_without_a_load() {
    let (sim, engine, pages) = service(crate::sim::testing::new_chain(), 0);
    let key = tree_key(&sim, 0);
    let page = ready(pages.page(&engine, &key, None, Some(1))).unwrap();
    let cursor = page.next_cursor.expect("more than one box");
    // Idle past the grace period when the next block comes.
    let idle = Config::default().grace + Duration::from_millis(1);
    engine.lock().unwrap().set_now(idle);
    let height = {
        let mut chain = sim.lock();
        chain.mine(0, 1, 0);
        chain.height()
    };
    let block = sim.lock().oracle().indexed_block(height);
    engine
        .lock()
        .unwrap()
        .apply(Event::BlockApplied(block))
        .unwrap();
    assert!(!engine.lock().unwrap().is_tracked(&key), "gone idle");
    let loads = count(&sim, Endpoint::UnspentByErgoTree);
    let error = ready(pages.page(&engine, &key, Some(&cursor), Some(1))).unwrap_err();
    assert_eq!(error, ApiError::CursorExpired);
    assert_eq!(count(&sim, Endpoint::UnspentByErgoTree), loads, "no load");
}

/// A box no source has fails the page rather than leaving the page short.
#[test]
fn a_box_no_source_has_fails_the_page() {
    use crate::types::ergo::{BlockHeader, IndexedBlock, IndexedBox, IndexedTransaction};

    let chain = crate::sim::testing::new_chain();
    let sim = SimChain::new(chain);
    let key = tree_key(&sim, 0);
    let tip = sim.lock().tip();
    // The engine holds a box the node never had, created by a block that has since left its
    // one-block ring, as after blocks the node then orphaned.
    let ghost = UTxO { ergo_tree: sim.lock().trees()[0].clone(), ..utxo(251, 252, 0, &[]) };
    let ghost_id = ghost.id;
    let block = |id: u8, parent, height, outputs| IndexedBlock {
        header: BlockHeader::new(crate::sim::testing::digest(id), parent, height),
        transactions: vec![IndexedTransaction {
            id: crate::sim::testing::digest(id),
            inputs: Vec::new(),
            outputs,
        }],
    };
    let mut engine = Engine::new(Config { window: 1, ..Config::default() });
    engine
        .apply(Event::BlockApplied(sim.lock().oracle().indexed_block(tip.height)))
        .unwrap();
    let boxes = sim.lock().oracle().load(&key, tip.height);
    let boxes = boxes
        .into_iter()
        .map(crate::engine::LoadedBox::from)
        .collect();
    engine
        .apply(Event::KeyLoaded { key: key.clone(), as_of: tip, boxes })
        .unwrap();
    let created = vec![IndexedBox { utxo: ghost, inclusion_height: tip.height + 1 }];
    let first = block(253, tip.header_id, tip.height + 1, created);
    let first_id = first.header.id;
    engine.apply(Event::BlockApplied(first)).unwrap();
    engine
        .apply(Event::BlockApplied(block(254, first_id, tip.height + 2, Vec::new())))
        .unwrap();
    let engine = Mutex::new(engine);
    let pages = Pages::new(
        Arc::new(Loader::new(sim.clone(), loader::Config::default())),
        Arc::new(TokenChecker::new(sim.clone(), SUPPLY_LIMIT)),
        BoxFetcher::new(sim.clone(), BoxCache::disabled()),
    );
    let error = ready(pages.page(&engine, &key, None, None)).unwrap_err();
    assert_eq!(error, ApiError::BoxUnavailable { box_id: ghost_id });
    assert_eq!(count(&sim, Endpoint::IndexedBox), 1, "every source was asked");
}

/// An engine that applies a block just before it answers the first `Query::Boxes`: the block
/// lands between reading a page's ids and fetching its boxes.
struct BlockDuringFetch {
    inner: Mutex<Engine>,
    block: Mutex<Option<crate::types::ergo::IndexedBlock>>,
}

impl EngineHandle for BlockDuringFetch {
    async fn apply(&self, event: Event) -> Result<Result<Vec<Output>, EngineError>, EngineGone> {
        Ok(self.inner.lock().unwrap().apply(event))
    }

    async fn query(&self, query: Query) -> Result<Reply, EngineGone> {
        let mut engine = self.inner.lock().unwrap();
        if matches!(query, Query::Boxes(_))
            && let Some(block) = self.block.lock().unwrap().take()
        {
            engine.apply(Event::BlockApplied(block)).unwrap();
        }
        Ok(engine.query(query))
    }
}

/// A block spends a box of the page between the engine's ids and the fetch: the box comes
/// from the ring buffer, without the indexer.
#[test]
fn a_box_spent_between_ids_and_fetch_comes_from_the_ring() {
    let (sim, engine, pages) = service(crate::sim::testing::new_chain(), 0);
    let key = tree_key(&sim, 0);
    ready(pages.page(&engine, &key, None, None)).unwrap();
    let pin = sim.lock().tip();
    let snapshot = expected(&sim, &key);
    let spent = snapshot[0].utxo.id;
    let block = {
        let mut chain = sim.lock();
        let to = chain.trees()[1].clone();
        let tx = chain.transfer(&[spent], &to);
        let block = chain.mine_txs(vec![tx]);
        chain.oracle().indexed_block(block.height)
    };
    let engine = BlockDuringFetch { inner: engine, block: Mutex::new(Some(block)) };
    let indexed = count(&sim, Endpoint::IndexedBox);
    let page = ready(pages.page(&engine, &key, None, None)).unwrap();
    assert_eq!(page.as_of, pin);
    assert_eq!(page.items, snapshot);
    assert_eq!(count(&sim, Endpoint::IndexedBox), indexed, "from the ring, not the indexer");
    assert_ne!(engine.inner.lock().unwrap().tip(), Some(pin), "the block landed");
}

/// A limit above the key's boxes gives one page with no cursor; `limit` boxes at a time give
/// pages that join into the same boxes.
#[test]
fn pages_join_into_the_whole_set() {
    let (sim, engine, pages) = service(crate::sim::testing::new_chain(), 1 << 20);
    let key = TrackingKey::Token(token_id("small-100"));
    let all = expected(&sim, &key);
    assert!(all.len() >= 3);
    let mut items = Vec::new();
    let mut cursor: Option<String> = None;
    let mut blocks = Vec::new();
    loop {
        let page = ready(pages.page(&engine, &key, cursor.as_deref(), Some(2))).unwrap();
        blocks.push(page.as_of);
        items.extend(page.items);
        match page.next_cursor {
            Some(next) => cursor = Some(next),
            None => break,
        }
    }
    assert_eq!(items, all);
    assert_eq!(blocks.len(), all.len().div_ceil(2));
    assert!(blocks.iter().all(|b| *b == blocks[0]), "one block");
}

#[test]
fn node_errors_keep_their_code() {
    assert_eq!(
        ApiError::from(&NodeError::Unavailable { retry_after: None }).code(),
        "node_unavailable"
    );
    let failed = NodeError::Api {
        status: reqwest::StatusCode::INTERNAL_SERVER_ERROR,
        reason: "boom".into(),
        detail: None,
    };
    assert_eq!(ApiError::from(&failed).code(), "node_error");
}

// --- Boxes with the pool ---

/// Submits `txs` to the node and gives the engine the node's mempool, observed at its tip.
fn submit(sim: &SimChain, engine: &Mutex<Engine>, txs: Vec<Arc<crate::sim::SimTx>>) {
    let (mempool, observed_tip) = {
        let mut chain = sim.lock();
        for tx in txs {
            chain.submit(tx);
        }
        (chain.mempool().into(), chain.tip())
    };
    let event = Event::MempoolSnapshot { txs: mempool, observed_tip };
    engine.lock().unwrap().apply(event).unwrap();
}

/// A view of a key no one tracks: the read loads it, applies the pool to its boxes, and
/// needs no box call, since confirmed boxes come from the load's seed and pending ones from
/// the engine's snapshot. A second read makes no node call at all.
#[test]
fn a_view_with_the_pool_loads_the_key_and_applies_it() {
    let (sim, engine, pages) = service(crate::sim::testing::new_chain(), 1 << 20);
    let key = tree_key(&sim, 0);
    // Tree 0's first box goes to tree 0 again, and that output on to tree 0 once more: the
    // first box is spent, the middle one created and spent, the last one pending.
    let (spent, middle, last) = {
        let mut chain = sim.lock();
        let spent = crate::sim::testing::box_of(&chain, 0);
        let to = chain.trees()[0].clone();
        let first = chain.transfer(&[spent], &to);
        let second = chain.transfer(&[first.outputs[0].id], &to);
        (spent, first, second)
    };
    let middle_id = middle.outputs[0].id;
    let pending = last.outputs[0].clone();
    submit(&sim, &engine, vec![middle, last]);

    let view = ready(pages.with_pool(&engine, &key)).unwrap();
    assert!(engine.lock().unwrap().is_tracked(&key));
    assert_eq!(view.as_of, sim.lock().tip());
    let confirmed: Vec<StoredBox> = (expected(&sim, &key).into_iter())
        .filter(|b| b.utxo.id != spent)
        .collect();
    assert_eq!(view.confirmed, confirmed);
    assert_eq!(view.pending, vec![pending]);
    assert!(view.pending.iter().all(|b| b.id != middle_id));
    assert_eq!(count(&sim, Endpoint::UnspentByErgoTree), 1);
    assert_eq!(count(&sim, Endpoint::BoxesWithPool), 0);

    let calls = sim.calls().len();
    assert_eq!(ready(pages.with_pool(&engine, &key)).unwrap(), view);
    assert_eq!(sim.calls().len(), calls, "a tracked key is served from memory");
}

/// The limit takes only confirmed boxes, after the pool's spends: a key one box over it
/// gets the first 1000 in keyset order and `truncated`, and every pending box on top. A cold
/// view is one node call, since pending boxes come from the engine's snapshot.
#[test]
fn a_view_holds_one_node_page_of_confirmed_boxes_and_every_pending_one() {
    let mut chain = crate::sim::testing::new_chain();
    let tree = chain.trees()[0].clone();
    let held = chain
        .utxo()
        .values()
        .filter(|b| b.ergo_tree == tree)
        .count();
    // Tree 1's first box split so tree 0 holds one box more than the limit.
    let input = crate::sim::testing::box_of(&chain, 1);
    let source = chain.find_box(&input).unwrap();
    let n = MAX_LIMIT as usize + 1 - held;
    let mut outputs: Vec<crate::sim::OutputSpec> = (0..n)
        .map(|_| crate::sim::OutputSpec { ergo_tree: tree.clone(), value: 0, tokens: Vec::new() })
        .collect();
    outputs[0].value = source.value;
    outputs[0].tokens = source.tokens.clone();
    let split = chain.tx(&[input], outputs);
    chain.mine_txs(vec![split]);
    let (sim, engine, pages) = service(chain, 0);
    let key = tree_key(&sim, 0);
    let limit = MAX_LIMIT as usize;

    let view = ready(pages.with_pool(&engine, &key)).unwrap();
    let all = expected(&sim, &key);
    assert_eq!(view.confirmed, all[..limit]);
    assert!(view.pending.is_empty());
    assert_eq!((view.total, view.truncated()), (limit + 1, true));
    assert_eq!(count(&sim, Endpoint::BoxesWithPool), 1, "a cold view is one node call");

    // A box coming in from tree 2 is listed on top of the limit.
    let incoming = {
        let mut chain = sim.lock();
        let from = crate::sim::testing::box_of(&chain, 2);
        chain.transfer(&[from], &tree)
    };
    submit(&sim, &engine, vec![incoming.clone()]);
    let view = ready(pages.with_pool(&engine, &key)).unwrap();
    assert_eq!(view.confirmed, all[..limit]);
    assert_eq!(view.pending, incoming.outputs);
    assert_eq!((view.total, view.truncated()), (limit + 2, true));

    // Two of its boxes merged: the 999 left fit, so nothing is cut, though the view holds
    // more than the limit.
    let merge = sim
        .lock()
        .transfer(&[all[0].utxo.id, all[1].utxo.id], &tree);
    submit(&sim, &engine, vec![merge.clone()]);
    let calls = count(&sim, Endpoint::BoxesWithPool);
    let view = ready(pages.with_pool(&engine, &key)).unwrap();
    assert_eq!(view.confirmed, all[2..]);
    // Two independent txs: overlay order puts the smaller tx id first.
    let mut txs = [&incoming, &merge];
    txs.sort_by_key(|tx| tx.id);
    assert_eq!(view.pending, [txs[0].outputs.clone(), txs[1].outputs.clone()].concat());
    assert_eq!((view.total, view.truncated()), (limit + 1, false));
    assert_eq!(count(&sim, Endpoint::BoxesWithPool), calls + 1);
}

/// The view's errors before any load are a first page's: a token over the supply limit is
/// refused with no load, and so is any key until the configured keys settle.
#[test]
fn a_view_with_the_pool_loads_like_a_first_page() {
    let (sim, engine, pages) = service(crate::sim::testing::new_chain(), 1 << 20);
    let over = TrackingKey::Token(token_id("over-101"));
    let error = ready(pages.with_pool(&engine, &over)).unwrap_err();
    assert_eq!(error, ApiError::SupplyTooLarge { emission: 101, limit: SUPPLY_LIMIT });

    let small = TrackingKey::Token(token_id("small-100"));
    let view = ready(pages.with_pool(&engine, &small)).unwrap();
    assert_eq!(view.confirmed, expected(&sim, &small));
    assert!(view.pending.is_empty(), "no snapshot yet");

    let (settled, _pending) = crate::runtime::settling(1);
    let (sim, engine, pages) = service(crate::sim::testing::new_chain(), 0);
    let pages = pages.waiting_for(settled);
    let error = ready(pages.with_pool(&engine, &tree_key(&sim, 0))).unwrap_err();
    assert!(matches!(error, ApiError::NotReady { .. }), "{error:?}");
    assert!(sim.calls().is_empty());
}
