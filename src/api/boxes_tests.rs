//! The box service against `SimChain`. Random histories of blocks, reorgs, pool changes,
//! indexer lag and follower ticks; whenever the engine, the indexer and the engine's snapshot
//! are all at the node's state, every box is served exactly when the oracle's spendable-now
//! view holds it: confirmed with its height, or pending without one.

use std::{
    collections::{BTreeSet, HashSet},
    sync::Mutex,
    time::Duration,
};

use proptest::prelude::*;
use wiremock::{Mock, MockServer, ResponseTemplate, matchers::path};

use super::*;
use crate::{
    adapters::{Follower, blocks::Sync, follower},
    clients::NodeClient,
    engine::{Config, Engine},
    fixtures,
    sim::{self, Chain, Endpoint, SimChain, SimConfig, SimOp, strategy, testing::ready},
    types::{Digest, address::Network, ergo::UnconfirmedTransaction},
};

const MAX_REORG: u8 = 3;

#[derive(Debug, Clone)]
enum Op {
    Chain(SimOp),
    /// One follower tick, a second after the last: blocks, then the mempool.
    Tick,
    /// The indexer ends any lag and catches up.
    Settle,
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        5 => strategy::op(MAX_REORG).prop_map(Op::Chain),
        4 => Just(Op::Tick),
        1 => Just(Op::Settle),
    ]
}

#[derive(Debug, Default)]
struct Seen {
    checked: bool,
    confirmed: bool,
    pending: bool,
    spent_by_pool: bool,
    spent_by_block: bool,
}

struct Harness {
    sim: SimChain,
    engine: Mutex<Engine>,
    follower: Follower<SimChain>,
    boxes: Boxes<SimChain>,
    now: tokio::time::Instant,
    /// Boxes spent by a block on some branch, for the not-found case.
    spent: BTreeSet<BoxId>,
    seen: Seen,
}

impl Harness {
    fn new() -> Self {
        let sim = SimChain::new(Chain::new(SimConfig::default()));
        Self {
            follower: Follower::new(sim.clone(), follower::Config::default()),
            boxes: Boxes::new(sim.clone()),
            engine: Mutex::new(Engine::new(Config::default())),
            now: tokio::time::Instant::now(),
            spent: BTreeSet::new(),
            seen: Seen::default(),
            sim,
        }
    }

    fn run(&mut self, op: &Op) {
        match op {
            Op::Chain(op) => {
                let before: HashSet<BoxId> =
                    self.sim.lock().utxo().values().map(|b| b.id).collect();
                self.sim.lock().apply(op);
                let after: HashSet<BoxId> = self.sim.lock().utxo().values().map(|b| b.id).collect();
                self.spent.extend(before.difference(&after));
            }
            Op::Tick => {
                let tick = ready(self.follower.tick(&self.engine, self.now))
                    .expect("SimChain serves /info");
                self.now += Duration::from_secs(1);
                if let Ok(Sync::Refused(error)) = tick.blocks {
                    panic!("the engine refused a block event: {error}");
                }
            }
            Op::Settle => self.sim.lock().settle_indexer(),
        }
        self.check();
    }

    /// The node's pool, if the engine's snapshot is that pool at the node's tip and the
    /// engine and indexer are there too: then the relay knows what the node knows.
    fn synced(&self) -> Option<Vec<UnconfirmedTransaction>> {
        let chain = self.sim.lock();
        let tip = chain.tip();
        let indexed = chain.indexed_height();
        if chain.is_recovering() || indexed.indexed_height != tip.height {
            return None;
        }
        let mut engine = self.engine.lock().unwrap();
        if engine.tip() != Some(tip) {
            return None;
        }
        let Reply::MempoolSnapshot(Some((txs, observed))) = engine.query(Query::MempoolSnapshot)
        else {
            return None;
        };
        let pool = chain.mempool();
        let ids =
            |txs: &[UnconfirmedTransaction]| -> BTreeSet<_> { txs.iter().map(|t| t.id).collect() };
        (observed == tip && ids(&txs) == ids(&pool)).then_some(pool)
    }

    fn check(&mut self) {
        let Some(pool) = self.synced() else { return };
        self.seen.checked = true;
        let oracle = self.sim.lock().oracle();
        let utxo = oracle.utxo(oracle.tip().height);
        let confirmed: HashSet<BoxId> = utxo.values().map(|b| b.id).collect();
        let order = sim::pending_order(&confirmed, &pool);
        let pool_spent: HashSet<BoxId> = order
            .iter()
            .flat_map(|tx| tx.inputs.iter().map(|b| b.id))
            .collect();

        let named: BTreeSet<BoxId> = (pool.iter())
            .flat_map(|tx| tx.inputs.iter().chain(&tx.outputs).map(|b| b.id))
            .chain(confirmed.iter().copied())
            .chain(self.spent.iter().copied())
            .chain([Digest([0xee; 32])])
            .collect();
        for id in named {
            let expected = if pool_spent.contains(&id) {
                None
            } else if let Some((key, b)) = utxo.iter().find(|(_, b)| b.id == id) {
                Some(FoundBox { utxo: b.clone(), inclusion_height: Some(key.inclusion_height) })
            } else {
                (order.iter().flat_map(|tx| &tx.outputs))
                    .find(|b| b.id == id)
                    .map(|b| FoundBox { utxo: Arc::new(b.clone()), inclusion_height: None })
            };
            let calls = self.sim.calls().len();
            let answer = ready(self.boxes.by_id(&self.engine, &id));
            let node_calls = &self.sim.calls()[calls..];
            match expected {
                Some(expected) => {
                    assert_eq!(answer, Ok(expected.clone()), "box {id}");
                    if expected.inclusion_height.is_some() {
                        self.seen.confirmed = true;
                        assert_eq!(node_calls, [Endpoint::IndexedBox], "box {id}");
                    } else {
                        self.seen.pending = true;
                        assert_eq!(node_calls, [], "a pending box costs no node call");
                    }
                }
                None => {
                    assert_eq!(answer, Err(ApiError::BoxNotFound), "box {id}");
                    if pool_spent.contains(&id) {
                        assert_eq!(node_calls, [], "a pool-spent box costs no node call");
                        self.seen.spent_by_pool |= confirmed.contains(&id);
                    } else {
                        self.seen.spent_by_block |= self.spent.contains(&id);
                    }
                }
            }
        }
    }
}

fn run(ops: &[Op]) -> Seen {
    let mut h = Harness::new();
    for op in ops {
        h.run(op);
    }
    h.seen
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(300))]

    #[test]
    fn a_box_is_served_exactly_when_it_is_spendable_now(
        ops in prop::collection::vec(op(), 0..=40),
    ) {
        run(&ops);
    }
}

#[test]
fn random_histories_reach_every_answer() {
    let mut runner = proptest::test_runner::TestRunner::deterministic();
    let strategy = prop::collection::vec(op(), 30..=40);
    let mut all = Seen::default();
    for _ in 0..100 {
        let ops = proptest::strategy::ValueTree::current(
            &proptest::strategy::Strategy::new_tree(&strategy, &mut runner).unwrap(),
        );
        let seen = run(&ops);
        all.checked |= seen.checked;
        all.confirmed |= seen.confirmed;
        all.pending |= seen.pending;
        all.spent_by_pool |= seen.spent_by_pool;
        all.spent_by_block |= seen.spent_by_block;
    }
    let Seen { checked, confirmed, pending, spent_by_pool, spent_by_block } = all;
    for (name, reached) in [
        ("the relay at the node's state", checked),
        ("a confirmed box served", confirmed),
        ("a pending box served", pending),
        ("a confirmed box a pool tx spends", spent_by_pool),
        ("a box a block spent", spent_by_block),
    ] {
        assert!(reached, "no random history reached: {name}");
    }
}

/// Through the node client, with no snapshot: the recorded unspent box is served confirmed,
/// the recorded spent and unknown ones are not found, and a node failure is the node's error,
/// not a missing box.
#[tokio::test]
async fn the_recorded_boxes_through_the_node_client() {
    let server = MockServer::start().await;
    let id_of = |name: &str| -> BoxId {
        let path = fixtures::manifest()["fixtures"][name]["path"]
            .as_str()
            .unwrap()
            .to_owned();
        path.rsplit('/').next().unwrap().parse().unwrap()
    };
    let (unspent, spent) = (id_of("box_by_id_unspent"), id_of("box_by_id_spent"));
    let (unknown, failing, busy) = (Digest([0xab; 32]), Digest([1; 32]), Digest([2; 32]));
    let routes = [
        (
            unspent,
            ResponseTemplate::new(200)
                .set_body_raw(fixtures::text("box_by_id_unspent"), "application/json"),
        ),
        (
            spent,
            ResponseTemplate::new(200)
                .set_body_raw(fixtures::text("box_by_id_spent"), "application/json"),
        ),
        (
            unknown,
            ResponseTemplate::new(404)
                .set_body_raw(fixtures::text("box_by_id_unknown"), "application/json"),
        ),
        (failing, ResponseTemplate::new(500)),
        (busy, ResponseTemplate::new(503)),
    ];
    for (id, response) in routes {
        Mock::given(path(format!("/blockchain/box/byId/{id}")))
            .respond_with(response)
            .mount(&server)
            .await;
    }
    let node = NodeClient::new(reqwest::Client::new(), &server.uri(), Network::Mainnet);
    let boxes = Boxes::new(node);
    let engine = Mutex::new(Engine::new(Config::default()));

    let found = boxes.by_id(&engine, &unspent).await.unwrap();
    let recorded: crate::types::ergo::BoxById = fixtures::load("box_by_id_unspent");
    assert_eq!(*found.utxo, recorded.indexed.utxo);
    assert_eq!(found.inclusion_height, Some(recorded.indexed.inclusion_height));
    for id in [spent, unknown] {
        assert_eq!(boxes.by_id(&engine, &id).await, Err(ApiError::BoxNotFound));
    }
    assert_eq!(boxes.by_id(&engine, &failing).await.unwrap_err().code(), "node_error");
    assert_eq!(boxes.by_id(&engine, &busy).await.unwrap_err().code(), "node_unavailable");
}
