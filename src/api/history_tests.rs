//! The history service against `SimChain`. Random histories scroll addresses'
//! histories while blocks, reorgs, indexer lag and follower ticks happen between page reads,
//! and bursts of new txs land during them. Each page continues the indexer's list exactly
//! below the last tx served, a first page lists no tx twice, and every refusal has its cause.

use std::{collections::HashSet, pin::pin, sync::Mutex};

use proptest::{prelude::*, sample::Index};

use super::*;
use crate::{
    adapters::{Follower, follower},
    engine::{BlockRef, Config as EngineConfig, Engine, Event, Totals, overlay::TxDelta},
    sim::{self, Chain, Endpoint, SimChain, SimConfig, SimOp, strategy, testing::ready},
    types::{Digest, ergo::BoxId},
};

const ENGINE: EngineConfig =
    EngineConfig { window: 4, grace: crate::sim::testing::blocks(2), ..EngineConfig::DEFAULT };
const MAX_REORG: u8 = 5;
const CASES: u32 = 3_000;
const CONFIG: Config = Config { attempts: 3, retry: Duration::from_secs(1) };

#[derive(Debug, Clone)]
enum Op {
    Chain(SimOp),
    /// One follower tick, a second after the last.
    Tick,
    /// The indexer ends any recovery and catches up.
    Settle,
    /// A client starts scrolling an address's history: its first page. With `tick`, the
    /// follower ticks while the service waits between attempts.
    First {
        key: Index,
        limit: u32,
        tick: bool,
    },
    /// The client reads its next page. `burst` blocks, each with a tx paying the address,
    /// are mined as the service's `read`-th node read of the call is answered.
    Next {
        burst: u8,
        read: u8,
    },
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        6 => strategy::op(MAX_REORG).prop_map(Op::Chain),
        4 => Just(Op::Tick),
        1 => Just(Op::Settle),
        3 => (any::<Index>(), 1..=3u32, any::<bool>())
            .prop_map(|(key, limit, tick)| Op::First { key, limit, tick }),
        8 => (prop_oneof![3 => Just(0u8), 1 => 1..=4u8], 1..=2u8)
            .prop_map(|(burst, read)| Op::Next { burst, read }),
    ]
}

/// What random histories reached, for the coverage test.
#[derive(Debug, Default, Clone, Copy)]
struct Seen {
    first_with_pending: bool,
    first_dropped_pending: bool,
    first_retried: bool,
    next_dropped_arrivals: bool,
    next_read_again: bool,
    expired: bool,
    scrolled_to_the_end: bool,
    summed_to_the_balance: bool,
    engine_off_the_indexed_branch: bool,
    anchor_moved: bool,
}

/// A client part way through an address's history.
struct Scroll {
    tree: ErgoTree,
    limit: u32,
    cursor: Option<String>,
    /// The last confirmed tx served, and its position from the oldest end then.
    last: Option<(TxId, usize)>,
    /// Every confirmed tx served, newest first.
    served: Vec<HistoryItem>,
    /// The indexed tip when the first page was read, while nothing has moved it since.
    undisturbed: Option<BlockRef>,
    /// The indexer's list when the last page was read.
    list: Vec<TxId>,
}

struct Harness {
    sim: SimChain,
    engine: Mutex<Engine>,
    follower: Follower<SimChain>,
    history: History<SimChain>,
    now: tokio::time::Instant,
    scroll: Option<Scroll>,
    seen: Seen,
}

impl Harness {
    fn new() -> Self {
        let sim = SimChain::new(Chain::new(SimConfig::default()));
        Self {
            follower: Follower::new(sim.clone(), follower::Config::default()),
            history: History::new(sim.clone(), CONFIG),
            engine: Mutex::new(Engine::new(ENGINE)),
            now: tokio::time::Instant::now(),
            scroll: None,
            seen: Seen::default(),
            sim,
        }
    }

    async fn run(&mut self, op: &Op) {
        match op {
            Op::Chain(op) => {
                self.sim.lock().apply(op);
                self.disturbed();
            }
            Op::Tick => self.tick().await,
            Op::Settle => {
                self.sim.lock().settle_indexer();
                self.disturbed();
            }
            Op::First { key, limit, tick } => {
                let tree = key.get(self.sim.lock().trees()).clone();
                self.first(tree, *limit, *tick).await;
            }
            Op::Next { burst, read } => self.next(*burst, *read).await,
        }
    }

    fn disturbed(&mut self) {
        let indexed = self.sim.lock().indexed_tip();
        if let Some(scroll) = &mut self.scroll
            && scroll.undisturbed != Some(indexed)
        {
            scroll.undisturbed = None;
        }
    }

    async fn tick(&mut self) {
        let tick = self.follower.tick(&self.engine, self.now).await;
        self.now += Duration::from_secs(1);
        tick.expect("SimChain serves /info");
    }

    /// The indexer's list for `tree` now, newest first: its branch's txs that touch the tree,
    /// recomputed from the blocks.
    fn list(&self, tree: &ErgoTree) -> Vec<(TxId, u32, PendingDelta)> {
        let chain = self.sim.lock();
        let mut list = Vec::new();
        for block in chain.branch(&chain.indexed_tip().header_id) {
            for tx in &block.txs {
                let (inputs, outputs) =
                    (tx.inputs.iter().map(|b| &**b), tx.outputs.iter().map(|b| &**b));
                if let Some(delta) = sim::net_change(tree, inputs, outputs) {
                    list.push((tx.id, block.header.height, delta));
                }
            }
        }
        list.reverse();
        list
    }

    async fn first(&mut self, tree: ErgoTree, limit: u32, tick: bool) {
        let calls = self.sim.calls().len();
        // The last `503` of a recovery ends it, so this is read before the call.
        let recovering = self.sim.lock().is_recovering();
        let result = {
            let Self { history, engine, follower, now, .. } = self;
            let mut read = pin!(history.page(&*engine, &tree, None, Some(limit)));
            if tick {
                tokio::select! {
                    biased;
                    result = &mut read => result,
                    () = tokio::time::sleep(CONFIG.retry / 2) => {
                        follower.tick(&*engine, *now).await.expect("SimChain serves /info");
                        *now += Duration::from_secs(1);
                        read.await
                    }
                }
            } else {
                read.await
            }
        };
        self.scroll = None;
        let page = match result {
            Ok(page) => page,
            Err(ApiError::NotReady { retry_after }) => {
                assert_eq!(retry_after, CONFIG.retry);
                let indexed = self.sim.lock().indexed_tip().height;
                let engine = self.engine.lock().unwrap().tip().map(|t| t.height);
                assert!(engine < Some(indexed), "retried only while the engine is behind");
                return;
            }
            Err(ApiError::NodeUnavailable { .. }) => {
                assert!(recovering, "a 503 comes only from a recovering indexer");
                return;
            }
            Err(e) => panic!("unexpected first page error: {e}"),
        };
        let list = self.list(&tree);
        let (pending, confirmed): (Vec<_>, Vec<_>) =
            page.items.iter().partition(|i| i.included.is_none());

        // The confirmed part is the list's newest `limit`, each with its block and change.
        assert_eq!(confirmed.len(), list.len().min(limit as usize));
        for (item, (id, height, delta)) in confirmed.iter().zip(&list) {
            assert_eq!(item.tx_id, *id);
            assert_eq!(
                item.included,
                Some(Included { height: *height, timestamp: sim::timestamp(*height) })
            );
            assert_eq!(&item.delta, delta, "{id}'s change");
        }

        // The pending part is the engine's pending txs that the indexer has not confirmed,
        // in the engine's order: none counted twice, none left out.
        let listed: HashSet<TxId> = list.iter().map(|(id, ..)| *id).collect();
        let engine_pending: Vec<TxDelta> = match self
            .engine
            .lock()
            .unwrap()
            .query(Query::PendingTxs(tree.clone()))
        {
            Reply::PendingTxs(p) => p.map(|p| p.txs).unwrap_or_default(),
            other => panic!("{other:?}"),
        };
        self.seen.first_dropped_pending |= engine_pending.iter().any(|t| listed.contains(&t.tx_id));
        let expected: Vec<HistoryItem> = engine_pending
            .into_iter()
            .filter(|t| !listed.contains(&t.tx_id))
            .map(pending_item)
            .collect();
        let pending: Vec<HistoryItem> = pending.into_iter().cloned().collect();
        if self.engine_on_indexed_branch() {
            assert_eq!(pending, expected, "the pending part");
        } else {
            // For a tick after a reorg the engine is still on the orphaned branch, and a tx it
            // holds pending can be confirmed on the indexer's below the page (accepted).
            let on_page: HashSet<TxId> = confirmed.iter().map(|i| i.tx_id).collect();
            assert!(pending.iter().all(|p| !on_page.contains(&p.tx_id)), "listed twice");
            self.seen.engine_off_the_indexed_branch = true;
        }
        self.seen.first_with_pending |= !pending.is_empty();
        self.seen.first_retried |= self.sim.calls()[calls..]
            .iter()
            .filter(|e| **e == Endpoint::AddressTransactions)
            .count()
            > 1;

        assert_eq!(page.next_cursor.is_some(), list.len() > limit as usize, "end signal");
        let indexed = self.sim.lock().indexed_tip();
        let served: Vec<HistoryItem> = confirmed.into_iter().cloned().collect();
        let last = served.last().map(|i| (i.tx_id, list.len() - served.len()));
        if page.next_cursor.is_none() {
            self.seen.scrolled_to_the_end = true;
            self.check_sum(&tree, &served);
        }
        self.scroll = Some(Scroll {
            limit,
            cursor: page.next_cursor,
            last,
            served,
            undisturbed: Some(indexed),
            list: list.iter().map(|(id, ..)| *id).collect(),
            tree,
        });
    }

    async fn next(&mut self, burst: u8, read: u8) {
        let Some(scroll) = &self.scroll else { return };
        let Some(cursor) = scroll.cursor.clone() else { return };
        let (tree, limit) = (scroll.tree.clone(), scroll.limit);
        let (anchor, anchor_pos) = scroll.last.expect("a cursor follows a confirmed tx");
        let unchanged = scroll.list
            == self
                .list(&tree)
                .iter()
                .map(|(id, ..)| *id)
                .collect::<Vec<_>>();
        if burst > 0 {
            let mut reads = 0;
            let to = tree.clone();
            self.sim.on_call(move |endpoint, chain| {
                if endpoint == Endpoint::AddressTransactions {
                    reads += 1;
                    if reads == read {
                        for _ in 0..burst {
                            pay(chain, &to);
                        }
                    }
                }
            });
        }
        let calls = self.sim.calls().len();
        let recovering = self.sim.lock().is_recovering();
        let result = self
            .history
            .page(&self.engine, &tree, Some(&cursor), Some(limit))
            .await;
        self.sim.on_call(|_, _| {});
        let reads = self.sim.calls()[calls..]
            .iter()
            .filter(|e| **e == Endpoint::AddressTransactions)
            .count();
        self.disturbed();
        let list = self.list(&tree);
        let below = list.iter().position(|(id, ..)| *id == anchor);
        let in_place = below.is_some_and(|at| list.len() - 1 - at == anchor_pos);
        let page = match result {
            Ok(page) => page,
            Err(ApiError::CursorExpired) => {
                assert!(!in_place, "expired, but the anchor is where it was");
                self.seen.expired = true;
                self.seen.anchor_moved |= below.is_some();
                self.scroll = None;
                return;
            }
            Err(ApiError::NotReady { .. }) => {
                assert!(burst > 0 && reads as u32 == CONFIG.attempts, "not ready without a burst");
                return;
            }
            Err(ApiError::NodeUnavailable { .. }) => {
                assert!(recovering, "a 503 comes only from a recovering indexer");
                return;
            }
            Err(e) => panic!("unexpected next page error: {e}"),
        };
        assert!(in_place, "a page served below an anchor that moved");
        let at = below.expect("in place");
        assert!(page.items.iter().all(|i| i.included.is_some()), "pending only on the first page");
        // The page is the list right below the anchor: nothing skipped, nothing repeated.
        assert!(page.items.len() <= limit as usize);
        assert!(page.items.len() < list.len() - at, "no more than the list holds below the anchor");
        for (item, (id, height, delta)) in page.items.iter().zip(&list[at + 1..]) {
            assert_eq!(item.tx_id, *id, "the tx right below the last one served");
            assert_eq!(item.included.map(|i| i.height), Some(*height));
            assert_eq!(&item.delta, delta);
        }
        let oldest = anchor_pos - page.items.len();
        assert_eq!(page.next_cursor.is_some(), oldest > 0, "null only at the end");
        self.seen.next_dropped_arrivals |=
            burst > 0 && reads == 1 && page.items.len() < limit as usize;
        self.seen.next_read_again |= reads > 1;
        if unchanged && burst == 0 {
            // Nothing moved since the last page: one read, and a full page.
            assert_eq!(reads, 1, "one node read");
            assert_eq!(page.items.len(), anchor_pos.min(limit as usize), "a full page");
        }

        let scroll = self.scroll.as_mut().expect("scrolling");
        scroll.list = list.iter().map(|(id, ..)| *id).collect();
        scroll.served.extend(page.items.iter().cloned());
        if let Some(last) = page.items.last() {
            scroll.last = Some((last.tx_id, oldest));
        }
        scroll.cursor = page.next_cursor;
        if scroll.cursor.is_none() {
            self.seen.scrolled_to_the_end = true;
            if scroll.undisturbed.is_some() {
                let (tree, served) = (scroll.tree.clone(), scroll.served.clone());
                self.check_sum(&tree, &served);
            }
        }
    }

    /// Whether the engine's tip is on the indexer's branch.
    fn engine_on_indexed_branch(&self) -> bool {
        let Some(tip) = self.engine.lock().unwrap().tip() else {
            return true;
        };
        let chain = self.sim.lock();
        chain
            .branch(&chain.indexed_tip().header_id)
            .iter()
            .any(|b| b.header.id == tip.header_id)
    }

    /// A whole history, read while the indexer stood still, sums to the address's balance.
    fn check_sum(&mut self, tree: &ErgoTree, served: &[HistoryItem]) {
        let key = TrackingKey::Tree(tree.clone());
        let chain = self.sim.lock();
        let indexed = chain.indexed_tip();
        let totals = chain
            .oracle_at(&indexed.header_id)
            .totals(&key, indexed.height);
        drop(chain);
        let mut sum = PendingDelta::default();
        for item in served {
            sum.nano_ergs += item.delta.nano_ergs;
            for (id, a) in &item.delta.tokens {
                *sum.tokens.entry(*id).or_default() += a;
            }
        }
        sum.tokens.retain(|_, a| *a != 0);
        assert_eq!(sum, signed(&totals), "a whole history sums to the balance");
        self.seen.summed_to_the_balance = true;
    }
}

/// Mines a block with one tx paying `to` a box: another tree's while there is one, else one
/// of its own, so a burst always lands.
fn pay(chain: &mut Chain, to: &ErgoTree) {
    let utxo = chain.utxo();
    let Some(input) = (utxo.values().find(|b| b.ergo_tree != *to))
        .or_else(|| utxo.values().next())
        .map(|b| b.id)
    else {
        return;
    };
    let tx = chain.transfer(&[input], to);
    chain.mine_txs(vec![tx]);
}

fn signed(totals: &Totals) -> PendingDelta {
    PendingDelta {
        nano_ergs: i128::from(totals.nano_ergs),
        tokens: totals
            .tokens
            .iter()
            .map(|(id, a)| (*id, i128::from(*a)))
            .collect(),
    }
}

fn run(ops: &[Op]) -> Seen {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .expect("a runtime");
    runtime.block_on(async {
        let mut h = Harness::new();
        for op in ops {
            h.run(op).await;
        }
        h.seen
    })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(CASES))]

    #[test]
    fn pages_continue_the_indexers_list_and_a_first_page_counts_no_tx_twice(
        ops in prop::collection::vec(op(), 0..=60),
    ) {
        run(&ops);
    }
}

#[test]
fn random_histories_reach_the_hard_history_cases() {
    let mut runner = proptest::test_runner::TestRunner::deterministic();
    let strategy = prop::collection::vec(op(), 40..=60);
    let mut all = Seen::default();
    for _ in 0..300 {
        let ops = proptest::strategy::ValueTree::current(
            &proptest::strategy::Strategy::new_tree(&strategy, &mut runner).unwrap(),
        );
        let seen = run(&ops);
        all.first_with_pending |= seen.first_with_pending;
        all.first_dropped_pending |= seen.first_dropped_pending;
        all.first_retried |= seen.first_retried;
        all.next_dropped_arrivals |= seen.next_dropped_arrivals;
        all.next_read_again |= seen.next_read_again;
        all.expired |= seen.expired;
        all.scrolled_to_the_end |= seen.scrolled_to_the_end;
        all.summed_to_the_balance |= seen.summed_to_the_balance;
        all.engine_off_the_indexed_branch |= seen.engine_off_the_indexed_branch;
        all.anchor_moved |= seen.anchor_moved;
    }
    for (name, reached) in [
        ("a first page with pending txs", all.first_with_pending),
        ("a first page dropping a pending tx the indexer confirmed", all.first_dropped_pending),
        ("a first page read again", all.first_retried),
        ("a next page dropping txs that arrived since", all.next_dropped_arrivals),
        ("a next page read again", all.next_read_again),
        ("a cursor expired by a reorg", all.expired),
        ("a history scrolled to the end", all.scrolled_to_the_end),
        ("a whole history summed to the balance", all.summed_to_the_balance),
        (
            "a first page while the engine is off the indexer's branch",
            all.engine_off_the_indexed_branch,
        ),
        ("a cursor expired by its anchor moving", all.anchor_moved),
    ] {
        assert!(reached, "random histories never reached {name}");
    }
}

/// A node with three blocks paying tree 0, and an engine at its tip with the pool in use.
struct Fixture {
    sim: SimChain,
    engine: Mutex<Engine>,
    history: History<SimChain>,
    tree: ErgoTree,
}

impl Fixture {
    fn new() -> Self {
        let sim = SimChain::new(Chain::new(SimConfig::default()));
        let tree = sim.lock().trees()[0].clone();
        for _ in 0..3 {
            pay(&mut sim.lock(), &tree);
        }
        let f = Self {
            history: History::new(sim.clone(), CONFIG),
            engine: Mutex::new(Engine::new(EngineConfig::default())),
            tree,
            sim,
        };
        f.sync();
        f
    }

    /// Brings the engine to the node's tip, block by block, with the node's pool.
    fn sync(&self) {
        let blocks = {
            let chain = self.sim.lock();
            let from = self
                .engine
                .lock()
                .unwrap()
                .tip()
                .map_or(chain.height(), |t| t.height + 1);
            (from..=chain.height())
                .map(|h| chain.oracle().indexed_block(h))
                .collect::<Vec<_>>()
        };
        let mut engine = self.engine.lock().unwrap();
        for block in blocks {
            engine
                .apply(Event::BlockApplied(block))
                .expect("the next block");
        }
        let chain = self.sim.lock();
        let snapshot =
            Event::MempoolSnapshot { txs: chain.mempool().into(), observed_tip: chain.tip() };
        engine.apply(snapshot).expect("a snapshot");
    }

    async fn page(&self, cursor: Option<&str>, limit: u32) -> Result<HistoryPage, ApiError> {
        self.history
            .page(&self.engine, &self.tree, cursor, Some(limit))
            .await
    }

    fn ids(page: &HistoryPage) -> Vec<TxId> {
        page.items.iter().map(|i| i.tx_id).collect()
    }
}

/// The indexer one block ahead of the engine: a tx the engine still holds pending is listed
/// once, as confirmed, with no retry.
#[tokio::test(start_paused = true)]
async fn a_tx_confirmed_one_block_ahead_of_the_engine_is_listed_once() {
    let f = Fixture::new();
    let tx = {
        let mut chain = f.sim.lock();
        let input = chain
            .utxo()
            .values()
            .find(|b| b.ergo_tree != f.tree)
            .unwrap()
            .id;
        let tx = chain.transfer(&[input], &f.tree);
        chain.submit(tx.clone());
        tx
    };
    f.sync();
    f.sim.lock().mine_txs(vec![tx.clone()]);
    let calls = f.sim.calls().len();

    let page = f.page(None, 2).await.unwrap();
    assert_eq!(Fixture::ids(&page)[0], tx.id);
    assert!(page.items[0].included.is_some(), "listed as confirmed");
    assert_eq!(page.items.iter().filter(|i| i.tx_id == tx.id).count(), 1);
    assert_eq!(f.sim.calls().len() - calls, 1, "one node read");
}

/// An engine handle that mines `tx` and applies its block just before it answers the first
/// pending-txs query: a block landing between the history's two reads.
struct Landing<'a> {
    fixture: &'a Fixture,
    tx: Mutex<Option<std::sync::Arc<sim::SimTx>>>,
}

impl EngineHandle for Landing<'_> {
    async fn apply(
        &self,
        event: Event,
    ) -> Result<
        Result<Vec<crate::engine::Output>, crate::engine::EngineError>,
        crate::adapters::EngineGone,
    > {
        self.fixture.engine.apply(event).await
    }

    async fn query(&self, query: Query) -> Result<Reply, crate::adapters::EngineGone> {
        if matches!(query, Query::PendingTxs(_))
            && let Some(tx) = self.tx.lock().unwrap().take()
        {
            self.fixture.sim.lock().mine_txs(vec![tx]);
            self.fixture.sync();
        }
        self.fixture.engine.query(query).await
    }
}

/// A block landing between the two reads mines a tx that was pending: it is listed, once.
/// Reading the indexer first would miss it, in neither half.
#[tokio::test(start_paused = true)]
async fn a_block_landing_between_the_reads_leaves_no_tx_out() {
    let f = Fixture::new();
    let tx = {
        let mut chain = f.sim.lock();
        let input = chain
            .utxo()
            .values()
            .find(|b| b.ergo_tree != f.tree)
            .unwrap()
            .id;
        let tx = chain.transfer(&[input], &f.tree);
        chain.submit(tx.clone());
        tx
    };
    f.sync();
    let landing = Landing { fixture: &f, tx: Mutex::new(Some(tx.clone())) };
    let page = f
        .history
        .page(&landing, &f.tree, None, Some(2))
        .await
        .unwrap();
    assert_eq!(page.items.iter().filter(|i| i.tx_id == tx.id).count(), 1, "{page:?}");
}

/// The engine one block behind with the overlap past the page: read again once the engine
/// catches up. Here it never does, so `not_ready`.
#[tokio::test(start_paused = true)]
async fn an_overlap_past_the_page_is_read_again() {
    let f = Fixture::new();
    let txs: Vec<_> = {
        let mut chain = f.sim.lock();
        let inputs: Vec<BoxId> = chain
            .utxo()
            .values()
            .filter(|b| b.ergo_tree != f.tree)
            .take(2)
            .map(|b| b.id)
            .collect();
        let txs: Vec<_> = inputs
            .iter()
            .map(|i| chain.transfer(&[*i], &f.tree))
            .collect();
        for tx in &txs {
            chain.submit(tx.clone());
        }
        txs
    };
    f.sync();
    f.sim.lock().mine_txs(txs);

    let result = f.page(None, 1).await;
    assert_eq!(result, Err(ApiError::NotReady { retry_after: CONFIG.retry }));
    let reads = f
        .sim
        .calls()
        .iter()
        .filter(|e| **e == Endpoint::AddressTransactions)
        .count();
    assert_eq!(reads, CONFIG.attempts as usize);

    // Once the engine has the block, one read does.
    f.sync();
    let page = f.page(None, 1).await.unwrap();
    assert!(page.items.iter().all(|i| i.included.is_some()));
}

/// More txs land between the two reads of a next page than the read holds: the page served
/// may be empty, but its cursor never says the end has come.
#[tokio::test(start_paused = true)]
async fn a_burst_between_reads_never_ends_the_history_early() {
    let f = Fixture::new();
    let first = f.page(None, 1).await.unwrap();
    let cursor = first.next_cursor.clone().expect("more than one tx");
    let to = f.tree.clone();
    f.sim.on_call(move |endpoint, chain| {
        if endpoint == Endpoint::AddressTransactions {
            // Push the anchor out of every read.
            for _ in 0..3 {
                pay(chain, &to);
            }
        }
    });
    let result = f.page(Some(&cursor), 1).await;
    assert_eq!(result, Err(ApiError::NotReady { retry_after: CONFIG.retry }));

    // Two arrive at the first read and one at the second: the anchor is the second read's
    // last item, with nothing below it in the read.
    let mut reads = 0;
    let to = f.tree.clone();
    f.sim.on_call(move |endpoint, chain| {
        if endpoint == Endpoint::AddressTransactions {
            reads += 1;
            let n = if reads == 1 {
                2
            } else if reads == 2 {
                1
            } else {
                0
            };
            for _ in 0..n {
                pay(chain, &to);
            }
        }
    });
    let config = Config { attempts: 2, ..CONFIG };
    let history = History::new(f.sim.clone(), config);
    let page = history
        .page(&f.engine, &f.tree, Some(&cursor), Some(1))
        .await
        .unwrap();
    assert!(page.items.is_empty());
    let next = page
        .next_cursor
        .expect("the anchor is kept: older txs are left");
    f.sim.on_call(|_, _| {});
    let page = f.page(Some(&next), 1).await.unwrap();
    assert_eq!(page.items.len(), 1, "the tx below the anchor");
}

/// New txs push the anchor out of the first read; a reorg then drops one of them before the
/// second, which is placed by a length the list no longer has. The anchor was never reached:
/// read again, and serve the page below it.
#[tokio::test(start_paused = true)]
async fn a_reorg_above_the_anchor_between_reads_does_not_expire_the_cursor() {
    let f = Fixture::new();
    let first = f.page(None, 1).await.unwrap();
    let cursor = first.next_cursor.expect("more than one tx");
    let to = f.tree.clone();
    let mut reads = 0;
    f.sim.on_call(move |endpoint, chain| {
        if endpoint == Endpoint::AddressTransactions {
            reads += 1;
            match reads {
                1 => {
                    for _ in 0..3 {
                        pay(chain, &to);
                    }
                }
                2 => {
                    chain.orphan(1);
                    chain.settle_indexer();
                }
                _ => {}
            }
        }
    });
    let page = f.page(Some(&cursor), 1).await.unwrap();
    f.sim.on_call(|_, _| {});
    let list = ready(f.sim.address_transactions(&f.tree, 0, 100)).unwrap();
    let at = list
        .items
        .iter()
        .position(|t| t.id == first.items[0].tx_id)
        .unwrap();
    assert_eq!(page.items.len(), 1);
    assert_eq!(page.items[0].tx_id, list.items[at + 1].id, "the tx below the anchor");
}

#[tokio::test(start_paused = true)]
async fn a_reorg_reaching_the_anchor_expires_the_cursor() {
    let f = Fixture::new();
    let first = f.page(None, 1).await.unwrap();
    let cursor = first.next_cursor.expect("more than one tx");
    {
        let mut chain = f.sim.lock();
        chain.orphan(1);
        chain.settle_indexer();
    }
    assert_eq!(f.page(Some(&cursor), 1).await, Err(ApiError::CursorExpired));
}

/// For a tick after a reorg the engine is still on the orphaned block, holding a tx pending
/// that the new block, at the same height, confirmed: listed once, as confirmed, though no
/// height on the page is above the engine's tip.
#[tokio::test(start_paused = true)]
async fn a_tx_confirmed_on_the_new_branch_at_the_engines_height_is_listed_once() {
    let f = Fixture::new();
    let tx = {
        let mut chain = f.sim.lock();
        let input = chain
            .utxo()
            .values()
            .find(|b| b.ergo_tree != f.tree)
            .unwrap()
            .id;
        let tx = chain.transfer(&[input], &f.tree);
        chain.submit(tx.clone());
        tx
    };
    f.sync();
    let engine_tip = f.engine.lock().unwrap().tip().unwrap();
    {
        let mut chain = f.sim.lock();
        chain.orphan(1);
        chain.mine_txs(vec![tx.clone()]);
        chain.settle_indexer();
        assert_eq!(chain.indexed_tip().height, engine_tip.height);
        assert_ne!(chain.indexed_tip(), engine_tip);
    }
    let page = f.page(None, 3).await.unwrap();
    let listed: Vec<_> = page.items.iter().filter(|i| i.tx_id == tx.id).collect();
    assert_eq!(listed.len(), 1, "{page:?}");
    assert!(listed[0].included.is_some());
}

/// A reorg that drops the anchor's parent block and mines the anchor again: it is one
/// position lower, so the txs below it are not the ones the client scrolled past.
#[tokio::test(start_paused = true)]
async fn an_anchor_back_at_another_position_expires_the_cursor() {
    let f = Fixture::new();
    let first = f.page(None, 1).await.unwrap();
    let cursor = first.next_cursor.expect("more than one tx");
    {
        let mut chain = f.sim.lock();
        let tip = chain.tip();
        let anchor = chain.block(&tip.header_id).unwrap().txs[0].clone();
        assert_eq!(anchor.id, first.items[0].tx_id);
        chain.orphan(2);
        chain.mine_txs(vec![anchor]);
        chain.settle_indexer();
    }
    let list = ready(f.sim.address_transactions(&f.tree, 0, 2)).unwrap();
    assert_eq!(list.items[0].id, first.items[0].tx_id, "the anchor is back");
    assert_eq!(f.page(Some(&cursor), 1).await, Err(ApiError::CursorExpired));
}

#[tokio::test(start_paused = true)]
async fn limits_and_cursors_are_checked() {
    let f = Fixture::new();
    for limit in [0, MAX_LIMIT + 1] {
        let result = f.page(None, limit).await;
        assert_eq!(result, Err(ApiError::InvalidLimit { max: MAX_LIMIT }), "{limit}");
    }
    assert!(matches!(f.page(Some("nope"), 1).await, Err(ApiError::InvalidCursor(_))));
    let other = TrackingKey::Tree(f.sim.lock().trees()[1].clone());
    let cursor = TxCursor { anchor: Digest([0; 32]), pos: 1, total: 2 }.encode(&other);
    assert!(matches!(f.page(Some(&cursor), 1).await, Err(ApiError::InvalidCursor(_))));
    let default = f
        .history
        .page(&f.engine, &f.tree, None, None)
        .await
        .unwrap();
    assert!(default.items.len() <= DEFAULT_LIMIT as usize);
}

/// Before the engine's first block, there is no snapshot: confirmed txs only.
#[tokio::test(start_paused = true)]
async fn without_a_tip_the_first_page_is_confirmed_only() {
    let f = Fixture::new();
    let engine = Mutex::new(Engine::new(EngineConfig::default()));
    let page = f
        .history
        .page(&engine, &f.tree, None, Some(5))
        .await
        .unwrap();
    assert!(!page.items.is_empty());
    assert!(page.items.iter().all(|i| i.included.is_some()));
}

mod recorded {
    use std::collections::BTreeMap;

    use serde_json::Value;
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{body_json, method, path, query_param},
    };

    use super::*;
    use crate::{
        clients::NodeClient,
        fixtures::{self, sample},
        types::{HexBytes, address::Network},
    };

    fn tree() -> ErgoTree {
        HexBytes(hex::decode(sample("ergoTree")).unwrap())
    }

    /// A tx's change for the sample address, summed from the raw JSON.
    fn raw_delta(tx: &Value) -> PendingDelta {
        let tree = sample("ergoTree");
        let mut delta = PendingDelta::default();
        let mut tokens: BTreeMap<String, i128> = BTreeMap::new();
        for (side, sign) in [("outputs", 1), ("inputs", -1)] {
            for b in tx[side]
                .as_array()
                .unwrap()
                .iter()
                .filter(|b| b["ergoTree"] == tree)
            {
                delta.nano_ergs += sign * i128::from(b["value"].as_u64().unwrap());
                for a in b["assets"].as_array().unwrap() {
                    let id = a["tokenId"].as_str().unwrap().to_owned();
                    *tokens.entry(id).or_default() +=
                        sign * i128::from(a["amount"].as_u64().unwrap());
                }
            }
        }
        delta.tokens = tokens
            .into_iter()
            .filter(|(_, a)| *a != 0)
            .map(|(id, a)| (id.parse().unwrap(), a))
            .collect();
        delta
    }

    async fn serve(offset: &str, limit: &str, name: &str, server: &MockServer) {
        Mock::given(method("POST"))
            .and(path("/blockchain/transaction/byAddress"))
            .and(body_json(sample("address")))
            .and(query_param("offset", offset))
            .and(query_param("limit", limit))
            .respond_with(
                ResponseTemplate::new(200).set_body_raw(fixtures::text(name), "application/json"),
            )
            .expect(1)
            .mount(server)
            .await;
    }

    /// The first page and the page below an anchor, over the node's own answers: each tx's
    /// change, height and timestamp, and cursor positions from its `total`.
    #[tokio::test]
    async fn pages_over_the_recorded_history() {
        let server = MockServer::start().await;
        serve("0", "3", "transactions_by_address", &server).await;
        serve("1", "2", "transactions_by_address_offset", &server).await;
        let node = NodeClient::new(reqwest::Client::new(), &server.uri(), Network::Mainnet);
        let history = History::new(node, CONFIG);
        // No tip: confirmed txs only, so only the indexer is read.
        let engine = Mutex::new(Engine::new(EngineConfig::default()));
        let raw: Value = fixtures::load("transactions_by_address");
        let raw = raw["items"].as_array().unwrap().clone();
        let total = u32::try_from(
            fixtures::load::<Value>("transactions_by_address")["total"]
                .as_u64()
                .unwrap(),
        )
        .unwrap();
        let key = TrackingKey::Tree(tree());

        let first = history.page(&engine, &tree(), None, Some(3)).await.unwrap();
        assert_eq!(first.items.len(), 3);
        for (item, tx) in first.items.iter().zip(&raw) {
            assert_eq!(item.tx_id.to_string(), tx["id"].as_str().unwrap());
            let height = u32::try_from(tx["inclusionHeight"].as_u64().unwrap()).unwrap();
            let timestamp = tx["timestamp"].as_u64().unwrap();
            assert_eq!(item.included, Some(Included { height, timestamp }));
            assert_eq!(item.delta, raw_delta(tx), "{}'s change", item.tx_id);
        }
        let cursor = TxCursor::decode(first.next_cursor.as_deref().unwrap(), &key).unwrap();
        assert_eq!(cursor, TxCursor { anchor: first.items[2].tx_id, pos: total - 3, total });

        // Below the second tx: read at offset 1 with one more than the limit, the anchor
        // first, and the tx after it served.
        let anchor = TxCursor { anchor: first.items[1].tx_id, pos: total - 2, total };
        let next = history
            .page(&engine, &tree(), Some(&anchor.encode(&key)), Some(1))
            .await
            .unwrap();
        assert_eq!(next.items, first.items[2..]);
        let cursor = TxCursor::decode(next.next_cursor.as_deref().unwrap(), &key).unwrap();
        assert_eq!(cursor.pos, total - 3);
    }

    /// A node answer listing more txs than its `total` is the node's fault, never a cursor the
    /// relay would then refuse as malformed.
    #[tokio::test]
    async fn more_txs_than_the_total_is_a_node_error() {
        let server = MockServer::start().await;
        let mut body: Value = fixtures::load("transactions_by_address");
        body["total"] = Value::from(2);
        Mock::given(method("POST"))
            .and(path("/blockchain/transaction/byAddress"))
            .respond_with(ResponseTemplate::new(200).set_body_json(body))
            .mount(&server)
            .await;
        let node = NodeClient::new(reqwest::Client::new(), &server.uri(), Network::Mainnet);
        let history = History::new(node, CONFIG);
        let engine = Mutex::new(Engine::new(EngineConfig::default()));

        let result = history.page(&engine, &tree(), None, Some(3)).await;
        assert!(matches!(result, Err(ApiError::NodeError(_))), "{result:?}");
    }
}

/// More requests than history reads may run at once, against a node slower than a read may
/// take: the one left waiting is answered `not_ready` with the retry wait, and the rest are
/// served.
#[tokio::test(start_paused = true)]
async fn a_page_behind_a_full_set_of_slow_reads_is_not_ready() {
    use crate::{adapters::history::MAX_READS, clients::node::HISTORY_TIMEOUT};

    let sim = SimChain::new(Chain::new(SimConfig::default()));
    let tree = sim.lock().trees()[0].clone();
    let history = History::new(sim.clone(), CONFIG);
    let engine = Mutex::new(Engine::new(ENGINE));
    sim.set_latency(Endpoint::AddressTransactions, HISTORY_TIMEOUT + Duration::from_secs(5));

    let pages = (0..=MAX_READS).map(|_| history.page(&engine, &tree, None, Some(1)));
    let mut pages: Vec<_> = pages.map(Box::pin).collect();
    let mut answers = Vec::new();
    while !pages.is_empty() {
        let (answer, _, rest) = futures_util::future::select_all(pages).await;
        answers.push(answer);
        pages = rest;
    }
    assert_eq!(answers[0], Err(ApiError::NotReady { retry_after: BUSY_RETRY }), "{answers:?}");
    assert!(answers[1..].iter().all(Result::is_ok), "{answers:?}");
}

/// An engine handle that counts the pending-txs queries it answers.
struct Counting {
    engine: Mutex<Engine>,
    pending: std::sync::atomic::AtomicUsize,
}

impl EngineHandle for Counting {
    async fn apply(
        &self,
        event: Event,
    ) -> Result<
        Result<Vec<crate::engine::Output>, crate::engine::EngineError>,
        crate::adapters::EngineGone,
    > {
        self.engine.apply(event).await
    }

    async fn query(&self, query: Query) -> Result<Reply, crate::adapters::EngineGone> {
        if matches!(query, Query::PendingTxs(_)) {
            self.pending
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        self.engine.query(query).await
    }
}

/// A first page waiting its turn has not read the pending txs yet: read before the wait, they
/// would be as old as the wait by the time the page arrives.
#[tokio::test(start_paused = true)]
async fn a_first_page_reads_the_pending_txs_only_in_its_turn() {
    use crate::{adapters::history::MAX_READS, sim::testing::poll_once};

    let sim = SimChain::new(Chain::new(SimConfig::default()));
    let tree = sim.lock().trees()[0].clone();
    let history = History::new(sim.clone(), CONFIG);
    let engine = Counting { engine: Mutex::new(Engine::new(ENGINE)), pending: Default::default() };
    let asked = || engine.pending.load(std::sync::atomic::Ordering::Relaxed);
    sim.set_latency(Endpoint::AddressTransactions, Duration::from_secs(10));

    let pages = (0..=MAX_READS).map(|_| history.page(&engine, &tree, None, Some(1)));
    let mut pages: Vec<_> = pages.map(Box::pin).collect();
    for page in &mut pages {
        assert!(poll_once(page.as_mut()).is_pending());
    }
    assert_eq!(asked(), MAX_READS, "the page waiting its turn has asked nothing");
    for page in pages {
        assert!(page.await.is_ok());
    }
    assert_eq!(asked(), MAX_READS + 1);
}
