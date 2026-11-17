//! The balance service against `SimChain`. Random histories read addresses'
//! balances, tracked and not, while blocks, reorgs, indexer lag and follower ticks happen
//! between and during reads. Every balance served is the oracle's at the engine's tip with
//! the overlay applied; when none is served, the reason holds.

use std::{
    collections::{BTreeMap, HashSet},
    pin::pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

use proptest::{prelude::*, sample::Index};

use super::*;
use crate::{
    adapters::{
        EngineGone, Follower, follower,
        loader::{self, Loader},
    },
    engine::{Balance, BlockRef, Config as EngineConfig, Engine, EngineError, Event, Output},
    sim::{
        self, Chain, Endpoint, SimChain, SimConfig, SimOp, reference_delta, strategy,
        testing::unused_tree,
    },
    types::ergo::{BoxId, TokenBalance, UTxO},
};

/// Small enough that reorgs reach past the window and keys go idle between reads.
const ENGINE: EngineConfig = EngineConfig { window: 4, grace: 2, ..EngineConfig::DEFAULT };
const MAX_REORG: u8 = 5;
const CASES: u32 = 500;
const CONFIG: Config = Config { attempts: 3, retry: Duration::from_secs(1) };

/// An engine that keeps the last `balance` streamed for each key, as a subscriber would.
struct Recording {
    engine: Mutex<Engine>,
    streamed: Mutex<BTreeMap<TrackingKey, Balance>>,
}

impl Recording {
    fn new(engine: Engine) -> Self {
        Self { engine: Mutex::new(engine), streamed: Mutex::new(BTreeMap::new()) }
    }

    fn record(&self, outputs: &[Output]) {
        let mut streamed = self.streamed.lock().unwrap();
        for output in outputs {
            match output {
                Output::Balance { key, balance } => {
                    streamed.insert(key.clone(), balance.clone());
                }
                Output::ResyncRequired { key, .. } => {
                    streamed.remove(key);
                }
                _ => {}
            }
        }
    }

    fn tip(&self) -> Option<BlockRef> {
        self.engine.lock().unwrap().tip()
    }

    fn is_tracked(&self, key: &TrackingKey) -> bool {
        self.engine.lock().unwrap().is_tracked(key)
    }
}

impl EngineHandle for Recording {
    async fn apply(&self, event: Event) -> Result<Result<Vec<Output>, EngineError>, EngineGone> {
        let result = self.engine.apply(event).await?;
        if let Ok(outputs) = &result {
            self.record(outputs);
        }
        Ok(result)
    }

    async fn query(&self, query: Query) -> Result<Reply, EngineGone> {
        let reply = self.engine.query(query).await?;
        if let Reply::CatchUp(outputs) = &reply {
            self.record(outputs);
        }
        Ok(reply)
    }
}

#[derive(Debug, Clone)]
enum Op {
    Chain(SimOp),
    /// One follower tick, a second after the last.
    Tick,
    /// The indexer ends any recovery and catches up.
    Settle,
    /// The loader tracks an address, as a page request or a subscription would.
    Load(Index),
    /// A balance read. With `tick`, the follower ticks while the service waits between
    /// attempts, as it does in the running relay.
    Read {
        key: Index,
        tick: bool,
    },
    /// An untracked address read on both paths at one tip: untracked, then loaded and read
    /// again.
    BothPaths(Index),
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        6 => strategy::op(MAX_REORG).prop_map(Op::Chain),
        5 => Just(Op::Tick),
        1 => Just(Op::Settle),
        2 => any::<Index>().prop_map(Op::Load),
        6 => (any::<Index>(), any::<bool>()).prop_map(|(key, tick)| Op::Read { key, tick }),
        1 => any::<Index>().prop_map(Op::BothPaths),
    ]
}

/// What random histories reached, for the coverage test.
#[derive(Debug, Default)]
struct Seen {
    tracked: bool,
    untracked: bool,
    tracked_pending: bool,
    untracked_pending: bool,
    streamed_compared: bool,
    waited_for_engine: bool,
    not_ready_indexer_ahead: bool,
    same_height_fork: bool,
    both_paths: bool,
    used_by_count: bool,
    unused: bool,
    used_without_count: bool,
}

/// What the node and the engine looked like just before a read.
struct Before {
    engine_tip: Option<BlockRef>,
    indexed_height: u32,
    recovering: bool,
    box_counts: usize,
}

struct Harness {
    sim: SimChain,
    engine: Recording,
    follower: Follower<SimChain>,
    loader: Loader<SimChain>,
    balances: Balances<SimChain>,
    now: tokio::time::Instant,
    seen: Seen,
}

impl Harness {
    fn new() -> Self {
        let sim = SimChain::new(Chain::new(SimConfig::default()));
        // One attempt, so a load that does not settle gives up at once.
        let config = loader::Config { page_limit: 4, attempts: 1, ..Default::default() };
        Self {
            follower: Follower::new(sim.clone(), follower::Config::default()),
            loader: Loader::new(sim.clone(), config),
            balances: Balances::new(sim.clone(), CONFIG),
            engine: Recording::new(Engine::new(ENGINE)),
            now: tokio::time::Instant::now(),
            seen: Seen::default(),
            sim,
        }
    }

    async fn run(&mut self, op: &Op) {
        match op {
            Op::Chain(op) => self.sim.lock().apply(op),
            Op::Tick => self.tick().await,
            Op::Settle => self.sim.lock().settle_indexer(),
            Op::Load(key) => {
                let key = self.key(key);
                self.load(&key).await;
            }
            Op::Read { key, tick } => {
                let key = self.key(key);
                self.read(&key, *tick).await;
            }
            Op::BothPaths(key) => {
                let key = self.key(key);
                self.both_paths(&key).await;
            }
        }
    }

    /// One of the sim's trees, or one no box was ever sent to.
    fn key(&self, index: &Index) -> ErgoTree {
        let mut trees = self.sim.lock().trees().to_vec();
        trees.push(unused_tree());
        index.get(&trees).clone()
    }

    async fn tick(&mut self) {
        let tick = self.follower.tick(&self.engine, self.now).await;
        self.now += Duration::from_secs(1);
        tick.expect("SimChain serves /info");
    }

    /// Loads the address and, once tracked, takes its balance as a balance-only subscriber
    /// would, so the stream is followed from the load.
    async fn load(&mut self, tree: &ErgoTree) -> bool {
        let key = TrackingKey::Tree(tree.clone());
        if self.loader.load(&self.engine, &key).await.is_err() {
            return false;
        }
        let catch_up = Query::CatchUp { key, from: None };
        self.engine.query(catch_up).await.expect("a running engine");
        true
    }

    fn box_counts(&self) -> usize {
        self.sim
            .calls()
            .iter()
            .filter(|e| **e == Endpoint::BoxCount)
            .count()
    }

    fn before(&self) -> Before {
        let box_counts = self.box_counts();
        let chain = self.sim.lock();
        Before {
            engine_tip: self.engine.tip(),
            indexed_height: chain.indexed_height().indexed_height,
            recovering: chain.is_recovering(),
            box_counts,
        }
    }

    /// A read through the service. With `tick`, a read still waiting half a retry in sees a
    /// follower tick; one already answered does not, so the checks see the state it read.
    async fn serve(&mut self, tree: &ErgoTree, tick: bool) -> Result<AddressBalance, ApiError> {
        let Self { balances, engine, follower, now, .. } = self;
        let mut read = pin!(balances.balance(&*engine, tree));
        if !tick {
            return read.await;
        }
        tokio::select! {
            biased;
            result = &mut read => return result,
            () = tokio::time::sleep(CONFIG.retry / 2) => {}
        }
        follower
            .tick(&*engine, *now)
            .await
            .expect("SimChain serves /info");
        *now += Duration::from_secs(1);
        read.await
    }

    async fn read(&mut self, tree: &ErgoTree, tick: bool) -> Option<AddressBalance> {
        let key = TrackingKey::Tree(tree.clone());
        let before = self.before();
        let tracked = self.engine.is_tracked(&key);
        let result = self.serve(tree, tick).await;
        match result {
            Ok(balance) => {
                self.check(&key, tracked, &before, &balance);
                Some(balance)
            }
            Err(ApiError::NotReady { retry_after }) => {
                assert!(!tracked, "a tracked address is answered by the engine alone");
                assert_eq!(retry_after, CONFIG.retry);
                let settled = before
                    .engine_tip
                    .is_some_and(|t| t.height == before.indexed_height)
                    && !before.recovering;
                assert!(!settled, "halves at one height from the start must pair");
                self.seen.not_ready_indexer_ahead |= before
                    .engine_tip
                    .is_some_and(|t| t.height < before.indexed_height);
                None
            }
            Err(ApiError::NodeUnavailable { .. }) => {
                assert!(before.recovering, "a 503 comes only from a recovering indexer");
                None
            }
            Err(error) => panic!("unexpected balance error for {key:?}: {error}"),
        }
    }

    /// A balance served is the oracle's at the engine's tip, with the overlay of the
    /// snapshot the engine holds; a tracked one is also the last one streamed. It is used
    /// when it is not zero, a pending tx touches it, or a box was sent to it on the
    /// indexer's branch, and only a zero balance asks the indexer.
    fn check(
        &mut self,
        key: &TrackingKey,
        tracked: bool,
        before: &Before,
        balance: &AddressBalance,
    ) {
        let tip = self.engine.tip().expect("a balance needs a tip");
        assert_eq!(balance.height, tip.height, "a balance as of another height than the tip");
        let (confirmed, pending, in_pool) = self.expected(key, tip);
        assert_eq!(balance.pending, pending, "{key:?}'s pending at {tip:?}");
        self.check_used(key, before, balance, in_pool);
        if tracked {
            assert_eq!(balance.confirmed, confirmed, "{key:?}'s running totals at {tip:?}");
            self.seen.tracked = true;
            self.seen.tracked_pending |= pending != PendingDelta::default();
            if let Some(streamed) = self.engine.streamed.lock().unwrap().get(key) {
                assert_eq!(
                    (&streamed.confirmed, &streamed.pending),
                    (&balance.confirmed, &balance.pending),
                    "a read differs from the last balance streamed"
                );
                self.seen.streamed_compared = true;
            }
            return;
        }

        // The indexer did not move during the read, so its tip is what was read.
        let indexed = self.sim.lock().indexed_tip();
        assert_eq!(indexed.height, balance.height, "paired at another height than the indexer's");
        if indexed != tip {
            // Accepted: the indexer is on another block at the same height, for up to a poll
            // after a one-block reorg.
            self.seen.same_height_fork = true;
            return;
        }
        assert_eq!(balance.confirmed, confirmed, "{key:?}'s confirmed balance at {tip:?}");
        self.seen.untracked = true;
        self.seen.untracked_pending |= pending != PendingDelta::default();
        self.seen.waited_for_engine |= before.engine_tip.is_some_and(|t| t.height < tip.height);
    }

    /// `used`, against the indexer's branch as it is now: nothing ran since the read's last
    /// node call.
    fn check_used(
        &mut self,
        key: &TrackingKey,
        before: &Before,
        balance: &AddressBalance,
        in_pool: bool,
    ) {
        let received = {
            let chain = self.sim.lock();
            chain
                .branch(&chain.indexed_tip().header_id)
                .iter()
                .flat_map(|block| &block.txs)
                .flat_map(|tx| &tx.outputs)
                .any(|b| key.matches(b))
        };
        let box_counts = self.box_counts() - before.box_counts;
        let zero =
            balance.confirmed == Totals::default() && balance.pending == PendingDelta::default();
        assert_eq!(balance.used, !zero || in_pool || received, "{key:?}'s used");
        if zero {
            assert_eq!(box_counts, usize::from(!in_pool), "a count only if no pending tx");
            self.seen.used_by_count |= balance.used && !in_pool;
            self.seen.unused |= !balance.used;
        } else {
            assert_eq!(box_counts, 0, "a balance not zero is used with no count");
            self.seen.used_without_count = true;
        }
    }

    /// The key's totals at `tip`, its pending delta over the engine's snapshot, and whether
    /// a tx of that snapshot touches it, from the oracle.
    fn expected(&self, key: &TrackingKey, tip: BlockRef) -> (Totals, PendingDelta, bool) {
        let utxo = self.sim.lock().oracle_at(&tip.header_id).utxo(tip.height);
        let ids: HashSet<BoxId> = utxo.values().map(|b| b.id).collect();
        let held: Vec<&UTxO> = utxo
            .values()
            .map(|b| &**b)
            .filter(|b| key.matches(b))
            .collect();
        let snapshot = self
            .engine
            .engine
            .lock()
            .unwrap()
            .query(Query::MempoolSnapshot);
        let Reply::MempoolSnapshot(snapshot) = snapshot else { panic!("a snapshot") };
        let overlay = match snapshot {
            Some((txs, _)) => sim::overlay(&ids, &txs, key),
            None => Vec::new(),
        };
        let in_pool = !overlay.is_empty();
        (sim::totals(held.iter().copied()), reference_delta(&held, &overlay), in_pool)
    }

    async fn both_paths(&mut self, tree: &ErgoTree) {
        let key = TrackingKey::Tree(tree.clone());
        if self.engine.is_tracked(&key) {
            return;
        }
        let Some(untracked) = self.read(tree, false).await else {
            return;
        };
        assert!(!self.engine.is_tracked(&key), "a balance read never starts tracking");
        let same_block = self.sim.lock().indexed_tip() == self.engine.tip().expect("a tip");
        if !self.load(tree).await {
            return;
        }
        let Some(tracked) = self.read(tree, false).await else {
            panic!("a tracked address is always answered");
        };
        if same_block && tracked.height == untracked.height {
            assert_eq!(tracked, untracked, "the two paths disagree at one height");
            self.seen.both_paths = true;
        }
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
    fn a_balance_is_the_oracles_at_the_engines_tip(
        ops in prop::collection::vec(op(), 0..=60),
    ) {
        run(&ops);
    }
}

#[test]
fn random_histories_reach_the_hard_balance_cases() {
    let mut runner = proptest::test_runner::TestRunner::deterministic();
    let strategy = prop::collection::vec(op(), 40..=60);
    let mut all = Seen::default();
    for _ in 0..300 {
        let ops = proptest::strategy::ValueTree::current(
            &proptest::strategy::Strategy::new_tree(&strategy, &mut runner).unwrap(),
        );
        let seen = run(&ops);
        all.tracked |= seen.tracked;
        all.untracked |= seen.untracked;
        all.tracked_pending |= seen.tracked_pending;
        all.untracked_pending |= seen.untracked_pending;
        all.streamed_compared |= seen.streamed_compared;
        all.waited_for_engine |= seen.waited_for_engine;
        all.not_ready_indexer_ahead |= seen.not_ready_indexer_ahead;
        all.same_height_fork |= seen.same_height_fork;
        all.both_paths |= seen.both_paths;
        all.used_by_count |= seen.used_by_count;
        all.unused |= seen.unused;
        all.used_without_count |= seen.used_without_count;
    }
    let Seen {
        tracked,
        untracked,
        tracked_pending,
        untracked_pending,
        streamed_compared,
        waited_for_engine,
        not_ready_indexer_ahead,
        same_height_fork,
        both_paths,
        used_by_count,
        unused,
        used_without_count,
    } = all;
    for (name, reached) in [
        ("a tracked read", tracked),
        ("an untracked read", untracked),
        ("a tracked read with a pending delta", tracked_pending),
        ("an untracked read with a pending delta", untracked_pending),
        ("a tracked read compared with the stream", streamed_compared),
        ("an untracked read that waited for the engine", waited_for_engine),
        ("not ready while the indexer is ahead", not_ready_indexer_ahead),
        ("the indexer on another block at the same height", same_height_fork),
        ("both paths at one height", both_paths),
        ("a zero balance used by the indexer's count", used_by_count),
        ("an unused address", unused),
        ("a used balance with no count", used_without_count),
    ] {
        assert!(reached, "random histories never reached {name}");
    }
}

// --- Hand-driven ---

/// A node and an engine at its tip, with a pending tx sending tree 0's first box to tree 1
/// in the engine's snapshot.
struct Fixture {
    sim: SimChain,
    engine: Mutex<Engine>,
    balances: Balances<SimChain>,
    tx: Arc<sim::SimTx>,
}

impl Fixture {
    fn new() -> Self {
        let sim = SimChain::new(Chain::new(SimConfig::default()));
        let engine = Mutex::new(Engine::new(EngineConfig::default()));
        let tx = {
            let mut chain = sim.lock();
            let input = sim::testing::box_of(&chain, 0);
            let to = chain.trees()[1].clone();
            let tx = chain.transfer(&[input], &to);
            chain.submit(tx.clone());
            tx
        };
        let f = Self { balances: Balances::new(sim.clone(), CONFIG), sim, engine, tx };
        f.apply_tip();
        f.apply_mempool();
        f
    }

    /// Gives the engine the node's mempool.
    fn apply_mempool(&self) {
        let (txs, observed_tip) = {
            let chain = self.sim.lock();
            (chain.mempool().into(), chain.tip())
        };
        self.apply(Event::MempoolSnapshot { txs, observed_tip });
    }

    /// Submits a tx sending `inputs` to `to`, and gives the engine the new mempool.
    fn submit(&self, inputs: &[BoxId], to: &ErgoTree) -> Arc<sim::SimTx> {
        let tx = {
            let mut chain = self.sim.lock();
            let tx = chain.transfer(inputs, to);
            chain.submit(tx.clone());
            tx
        };
        self.apply_mempool();
        tx
    }

    /// Mines a block sending all of tree `index`'s boxes to tree 0, and applies it to the
    /// engine: the tree is used and holds nothing.
    fn empty_tree(&self, index: usize) {
        let tree = self.tree(index);
        {
            let mut chain = self.sim.lock();
            let ids: Vec<BoxId> = chain
                .utxo()
                .values()
                .filter(|b| b.ergo_tree == tree)
                .map(|b| b.id)
                .collect();
            let to = chain.trees()[0].clone();
            let tx = chain.transfer(&ids, &to);
            chain.mine_txs(vec![tx]);
        }
        self.apply_tip();
    }

    fn count_calls(&self, endpoint: Endpoint) -> usize {
        self.sim.calls().iter().filter(|e| **e == endpoint).count()
    }

    fn tree(&self, index: usize) -> ErgoTree {
        self.sim.lock().trees()[index].clone()
    }

    fn apply(&self, event: Event) -> Vec<Output> {
        self.engine
            .lock()
            .unwrap()
            .apply(event)
            .expect("the engine takes it")
    }

    /// Applies the node's tip block to the engine.
    fn apply_tip(&self) {
        let block = {
            let chain = self.sim.lock();
            chain
                .indexed_block(&chain.tip().header_id)
                .expect("indexed")
        };
        self.apply(Event::BlockApplied(block));
    }

    fn mine_tx(&self) {
        let tx = self.tx.clone();
        self.sim.lock().mine_txs(vec![tx]);
    }

    fn balance_calls(&self) -> usize {
        self.count_calls(Endpoint::ConfirmedBalance)
    }

    /// Tree `index`'s expected balance at the node's tip, with the tx pending until mined.
    /// Every sim tree was paid at genesis, so it is used.
    fn expected(&self, index: usize) -> AddressBalance {
        let key = TrackingKey::Tree(self.tree(index));
        let chain = self.sim.lock();
        let oracle = chain.oracle();
        let height = chain.tip().height;
        let utxo = oracle.utxo(height);
        let ids: HashSet<BoxId> = utxo.values().map(|b| b.id).collect();
        let held: Vec<&UTxO> = utxo
            .values()
            .map(|b| &**b)
            .filter(|b| key.matches(b))
            .collect();
        let overlay = sim::overlay(&ids, &[self.tx.to_unconfirmed()], &key);
        AddressBalance {
            height,
            confirmed: sim::totals(held.iter().copied()),
            pending: reference_delta(&held, &overlay),
            used: true,
        }
    }
}

#[tokio::test(start_paused = true)]
async fn an_untracked_address_pairs_the_indexer_with_the_engine_and_stays_untracked() {
    let f = Fixture::new();
    let tree = f.tree(0);
    let balance = f.balances.balance(&f.engine, &tree).await.unwrap();
    assert_eq!(balance, f.expected(0));
    assert!(balance.pending.nano_ergs < 0, "the pending tx sends the box away");
    assert_eq!(
        f.sim.calls(),
        [Endpoint::IndexedHeight, Endpoint::ConfirmedBalance, Endpoint::IndexedHeight]
    );
    let key = TrackingKey::Tree(tree);
    assert!(!f.engine.lock().unwrap().is_tracked(&key), "a balance read never tracks");
}

#[tokio::test(start_paused = true)]
async fn a_tracked_address_not_zero_is_answered_by_the_engine_alone() {
    let f = Fixture::new();
    let key = TrackingKey::Tree(f.tree(1));
    let (as_of, boxes) = {
        let chain = f.sim.lock();
        let tip = chain.tip();
        let boxes = chain.oracle().load(&key, tip.height);
        (tip, boxes.into_iter().map(Into::into).collect())
    };
    f.apply(Event::KeyLoaded { key: key.clone(), as_of, boxes });
    let calls = f.sim.calls().len();
    let balance = f.balances.balance(&f.engine, &f.tree(1)).await.unwrap();
    assert_eq!(balance, f.expected(1));
    assert!(balance.pending.nano_ergs > 0, "the pending tx sends a box in");
    assert_eq!(f.sim.calls().len(), calls, "no node call");
}

#[tokio::test(start_paused = true)]
async fn a_fresh_address_is_unused() {
    let f = Fixture::new();
    let balance = f.balances.balance(&f.engine, &unused_tree()).await.unwrap();
    assert_eq!(balance.confirmed, Totals::default());
    assert_eq!(balance.pending, PendingDelta::default());
    assert!(!balance.used);
    assert_eq!(f.count_calls(Endpoint::BoxCount), 1);
}

/// Its boxes are all spent: the balance can't tell it from a fresh one, the indexer can.
/// Tracked or not.
#[tokio::test(start_paused = true)]
async fn an_emptied_address_is_used_by_the_indexers_count() {
    let f = Fixture::new();
    f.empty_tree(3);
    let tree = f.tree(3);
    let untracked = f.balances.balance(&f.engine, &tree).await.unwrap();
    assert_eq!(untracked.confirmed, Totals::default());
    assert!(untracked.used);
    assert_eq!(f.count_calls(Endpoint::BoxCount), 1);

    let as_of = f.sim.lock().tip();
    let key = TrackingKey::Tree(tree.clone());
    f.apply(Event::KeyLoaded { key, as_of, boxes: Vec::new() });
    let calls = f.sim.calls().len();
    let tracked = f.balances.balance(&f.engine, &tree).await.unwrap();
    assert_eq!(tracked, untracked);
    assert_eq!(f.sim.calls()[calls..], [Endpoint::BoxCount], "one node call, for the count");
}

/// A payment still in the pool makes a fresh address used, with no count asked: the
/// pending delta is non-zero.
#[tokio::test(start_paused = true)]
async fn a_pending_payment_makes_a_fresh_address_used() {
    let f = Fixture::new();
    let input = sim::testing::box_of(&f.sim.lock(), 2);
    f.submit(&[input], &unused_tree());
    let balance = f.balances.balance(&f.engine, &unused_tree()).await.unwrap();
    assert!(balance.pending.nano_ergs > 0);
    assert!(balance.used);
    assert_eq!(f.count_calls(Endpoint::BoxCount), 0);
}

/// A pending output to a fresh address, spent by another pending tx: the delta is zero, but
/// the address received a box.
#[tokio::test(start_paused = true)]
async fn a_pending_output_spent_in_the_pool_makes_an_address_used() {
    let f = Fixture::new();
    let input = sim::testing::box_of(&f.sim.lock(), 2);
    let paid = f.submit(&[input], &unused_tree());
    let to = f.tree(0);
    f.submit(&[paid.outputs[0].id], &to);
    let balance = f.balances.balance(&f.engine, &unused_tree()).await.unwrap();
    assert_eq!(balance.pending, PendingDelta::default());
    assert!(balance.used);
    assert_eq!(f.count_calls(Endpoint::BoxCount), 0, "the pending txs answer it");
}

/// The indexer has the block that mines the tx and the engine does not: paired as they are,
/// the tx would count as confirmed and as pending. The service waits for the engine, keeping
/// its confirmed read.
#[tokio::test(start_paused = true)]
async fn an_engine_a_block_behind_is_waited_for() {
    let f = Fixture::new();
    f.mine_tx();
    let tree = f.tree(0);
    let read = f.balances.balance(&f.engine, &tree);
    let catch_up = async {
        tokio::time::sleep(CONFIG.retry / 2).await;
        f.apply_tip();
    };
    let (balance, ()) = tokio::join!(read, catch_up);
    assert_eq!(balance.unwrap(), f.expected(0));
    assert_eq!(f.balance_calls(), 1, "the confirmed read is kept while the engine catches up");
}

#[tokio::test(start_paused = true)]
async fn an_engine_that_stays_behind_is_not_ready() {
    let f = Fixture::new();
    f.mine_tx();
    let started = tokio::time::Instant::now();
    let error = f.balances.balance(&f.engine, &f.tree(0)).await.unwrap_err();
    assert_eq!(error, ApiError::NotReady { retry_after: CONFIG.retry });
    assert_eq!(started.elapsed(), CONFIG.retry * (CONFIG.attempts - 1), "no wait after the last");
    assert_eq!(f.balance_calls(), 1);
}

#[tokio::test(start_paused = true)]
async fn an_engine_with_no_tip_is_not_ready() {
    let f = Fixture::new();
    let fresh = Mutex::new(Engine::new(EngineConfig::default()));
    let error = f.balances.balance(&fresh, &f.tree(0)).await.unwrap_err();
    assert!(matches!(error, ApiError::NotReady { .. }), "{error:?}");
}

/// An engine that applies the block mining the fixture's tx just before it answers the first
/// `Query::Pending`: after the indexer was read, before the pending half.
struct BlockBeforePending<'a> {
    fixture: &'a Fixture,
    armed: AtomicBool,
}

impl EngineHandle for BlockBeforePending<'_> {
    async fn apply(&self, event: Event) -> Result<Result<Vec<Output>, EngineError>, EngineGone> {
        self.fixture.engine.apply(event).await
    }

    async fn query(&self, query: Query) -> Result<Reply, EngineGone> {
        if matches!(query, Query::Pending(_)) && self.armed.swap(false, Ordering::SeqCst) {
            self.fixture.mine_tx();
            self.fixture.apply_tip();
        }
        self.fixture.engine.query(query).await
    }
}

/// Paired as they are, the halves would leave the mined tx in neither: not confirmed at the
/// indexer's height, no longer pending at the engine's. The pending half's own block catches
/// it, and the indexer is read again.
#[tokio::test(start_paused = true)]
async fn a_block_between_the_halves_is_caught() {
    let f = Fixture::new();
    let engine = BlockBeforePending { fixture: &f, armed: AtomicBool::new(true) };
    let balance = f.balances.balance(&engine, &f.tree(0)).await.unwrap();
    assert_eq!(balance, f.expected(0));
    assert_eq!(f.balance_calls(), 2, "an engine ahead reads the indexer again");
}

#[test]
fn the_nodes_balance_becomes_totals() {
    let id = |n| crate::sim::testing::digest(n);
    let balance = ergo::Balance {
        nano_ergs: 7,
        tokens: vec![
            TokenBalance { token_id: id(1), amount: 2 },
            TokenBalance { token_id: id(2), amount: 0 },
            TokenBalance { token_id: id(1), amount: 3 },
        ],
    };
    let expected = Totals { nano_ergs: 7, tokens: [(id(1), 5)].into() };
    assert_eq!(totals(balance), expected);
}
