//! The header walk against `SimChain` and the block consumer: random histories with reorgs,
//! resets past the window, a lagging indexer and evicted headers, and by hand the startup
//! backfill, the steady state, genesis and the node's failures.

use std::sync::Mutex;

use proptest::{
    prelude::*,
    sample::Index,
    strategy::{Strategy, ValueTree},
};
use wiremock::{Mock, MockServer, ResponseTemplate, matchers::path};

use super::*;
use crate::{
    adapters::{BlockConsumer, blocks::Sync},
    clients::NodeClient,
    engine::{Config, Engine},
    sim::{Chain, Endpoint, SimChain, SimConfig, SimOp, strategy, testing::ready},
    types::address::Network,
};

/// Small, so reorgs reach past it and the consumer resets.
const CONFIG: Config =
    Config { window: 4, grace: crate::sim::testing::blocks(100), ..Config::DEFAULT };
const MAX_REORG: u8 = 6;
const CASES: u32 = 1_000;

struct Harness {
    sim: SimChain,
    engine: Mutex<Engine>,
    consumer: BlockConsumer<SimChain>,
    reader: HeaderReader<SimChain>,
}

impl Harness {
    fn new(chain: Chain) -> Self {
        let sim = SimChain::new(chain);
        let consumer = BlockConsumer::new(sim.clone());
        let reader = HeaderReader::new(sim.clone(), consumer.headers());
        Self { engine: Mutex::new(Engine::new(CONFIG)), sim, consumer, reader }
    }

    /// Brings the engine towards the node's tip; a lagging indexer can leave it behind.
    fn sync(&self) -> Option<Sync> {
        let tip = self.sim.lock().tip();
        ready(self.consumer.sync(&self.engine, tip)).ok()
    }

    fn tip(&self) -> BlockRef {
        self.engine.lock().unwrap().tip().expect("a tip")
    }

    fn walk(&self, n: usize) -> Result<Vec<BlockHeader>, HeaderError> {
        ready(self.reader.back_from(self.tip(), n))
    }

    /// The chain's last `n` headers ending at the engine's tip, newest first.
    fn expected(&self, n: usize) -> Vec<BlockHeader> {
        let chain = self.sim.lock();
        let branch = chain.branch(&self.tip().header_id);
        branch
            .iter()
            .rev()
            .take(n)
            .map(|b| b.header.clone())
            .collect()
    }

    fn fetched(&self) -> usize {
        self.sim
            .calls()
            .iter()
            .filter(|e| **e == Endpoint::Header)
            .count()
    }

    /// Checks every walk up to the maximum, and that one finding every header cached calls
    /// the node for none. What it reached, for the coverage test.
    fn check(&self, seen: &mut Seen) {
        let expected = self.expected(MAX_HEADERS);
        let cached = expected.iter().all(|h| self.reader.cache.contains(&h.id));
        let before = self.fetched();
        for n in (1..=MAX_HEADERS).rev() {
            let walked = self.walk(n).unwrap();
            assert_eq!(walked, expected[..n.min(expected.len())], "a walk of {n}");
        }
        if cached {
            assert_eq!(self.fetched(), before, "every header was cached");
        }
        seen.cached |= cached && expected.len() == MAX_HEADERS;
        seen.fetched |= self.fetched() > before;
        seen.genesis |= expected.len() < MAX_HEADERS;
    }
}

fn chain(blocks: u64) -> Chain {
    let mut chain = Chain::new(SimConfig { recovery_reads: 0, ..SimConfig::default() });
    for seed in 1..=blocks {
        chain.mine(0, 1, seed);
    }
    chain
}

#[derive(Debug, Clone)]
enum Op {
    Chain(SimOp),
    Sync,
    /// Drops one of the tip's last headers from the cache.
    Evict(Index),
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        4 => strategy::op(MAX_REORG).prop_map(Op::Chain),
        4 => Just(Op::Sync),
        2 => any::<Index>().prop_map(Op::Evict),
    ]
}

/// What random histories reached, for the coverage test.
#[derive(Debug, Default)]
struct Seen {
    reorg: bool,
    reset: bool,
    waiting: bool,
    evicted: bool,
    /// A full walk with every header cached.
    cached: bool,
    /// A walk that called the node.
    fetched: bool,
    /// A walk that reached genesis.
    genesis: bool,
}

fn run(blocks: u64, ops: &[Op]) -> Seen {
    let mut seen = Seen::default();
    let h = Harness::new(chain(blocks));
    h.sync();
    h.check(&mut seen);
    for op in ops {
        match op {
            Op::Chain(op) => h.sim.lock().apply(op),
            Op::Sync => match h.sync() {
                Some(Sync::Synced { reverted, reset, .. }) => {
                    seen.reorg |= reverted > 0;
                    seen.reset |= reset;
                }
                Some(Sync::WaitingForIndexer { .. }) => seen.waiting = true,
                _ => {}
            },
            Op::Evict(index) => {
                let expected = h.expected(MAX_HEADERS);
                h.reader.cache.invalidate(&index.get(&expected).id);
                seen.evicted = true;
            }
        }
        h.check(&mut seen);
    }
    seen
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(CASES))]

    /// After every op, a walk of each length is the chain's headers ending at the engine's
    /// tip, whatever the cache holds.
    #[test]
    fn a_walk_is_the_chain_behind_the_engines_tip(
        blocks in 0u64..14,
        ops in prop::collection::vec(op(), 0..=60),
    ) {
        run(blocks, &ops);
    }
}

#[test]
fn random_histories_reach_the_walks_cases() {
    let mut runner = proptest::test_runner::TestRunner::deterministic();
    let strategy = (0u64..14, prop::collection::vec(op(), 40..=60));
    let mut all = Seen::default();
    for _ in 0..200 {
        let (blocks, ops) = strategy.new_tree(&mut runner).unwrap().current();
        let seen = run(blocks, &ops);
        all.reorg |= seen.reorg;
        all.reset |= seen.reset;
        all.waiting |= seen.waiting;
        all.evicted |= seen.evicted;
        all.cached |= seen.cached;
        all.fetched |= seen.fetched;
        all.genesis |= seen.genesis;
    }
    let Seen { reorg, reset, waiting, evicted, cached, fetched, genesis } = all;
    for (name, reached) in [
        ("a reorg", reorg),
        ("a fork beyond the window", reset),
        ("a wait for the indexer", waiting),
        ("an evicted header", evicted),
        ("a full walk from the cache alone", cached),
        ("a walk that fetched", fetched),
        ("a walk down to genesis", genesis),
    ] {
        assert!(reached, "no random history reached {name}");
    }
}

#[test]
fn at_startup_a_walk_fetches_the_headers_below_the_tip_once() {
    let h = Harness::new(chain(15));
    h.sync();
    assert_eq!(h.walk(MAX_HEADERS).unwrap(), h.expected(MAX_HEADERS));
    assert_eq!(h.fetched(), MAX_HEADERS - 1, "the engine started with its tip alone");
    h.check(&mut Seen::default());
    assert_eq!(h.fetched(), MAX_HEADERS - 1);
}

#[test]
fn while_following_the_chain_no_header_is_fetched() {
    let h = Harness::new(chain(15));
    h.sync();
    h.check(&mut Seen::default());
    let backfill = h.fetched();
    // Past the cache's capacity, one block at a time, then in one burst, then a reorg.
    for seed in 0..40 {
        h.sim.lock().mine(0, 1, 100 + seed);
        h.sync();
        h.check(&mut Seen::default());
    }
    for seed in 0..40 {
        h.sim.lock().mine(0, 1, 200 + seed);
    }
    h.sync();
    h.check(&mut Seen::default());
    h.sim.lock().reorg(3, 1, 300);
    h.sync();
    h.check(&mut Seen::default());
    assert_eq!(h.fetched(), backfill);
}

#[test]
fn a_chain_shorter_than_asked_ends_at_genesis() {
    let h = Harness::new(chain(2));
    h.sync();
    let walked = h.walk(MAX_HEADERS).unwrap();
    assert_eq!(walked, h.expected(MAX_HEADERS));
    assert_eq!(walked.len(), 3);
    assert_eq!(walked.last().unwrap().parent_id, GENESIS_PARENT_ID);
}

#[test]
fn an_unknown_header_or_a_wrong_height_is_the_nodes_failure() {
    let h = Harness::new(chain(3));
    h.sync();
    let tip = h.tip();
    let unknown = BlockRef { header_id: crate::sim::testing::digest(9), ..tip };
    let result = ready(h.reader.back_from(unknown, MAX_HEADERS));
    assert!(matches!(result, Err(HeaderError::Unknown(id)) if id == unknown.header_id));

    // A tip whose height is not its header's.
    let higher = BlockRef { height: tip.height + 1, ..tip };
    let result = ready(h.reader.back_from(higher, MAX_HEADERS));
    assert!(
        matches!(result, Err(HeaderError::Mismatch { wanted, got }) if wanted == higher && got == tip)
    );
}

#[tokio::test]
async fn a_node_error_fails_the_walk() {
    let server = MockServer::start().await;
    let id = crate::fixtures::sample("headerId");
    Mock::given(path(format!("/blocks/modifier/{id}")))
        .respond_with(ResponseTemplate::new(503).insert_header("Retry-After", "7"))
        .expect(1)
        .mount(&server)
        .await;
    let node = NodeClient::new(reqwest::Client::new(), &server.uri(), Network::Mainnet);
    let reader = HeaderReader::new(node, HeaderCache::new());
    let tip = BlockRef { height: 1915472, header_id: id.parse().unwrap() };
    let result = reader.back_from(tip, MAX_HEADERS).await;
    assert!(
        matches!(result, Err(HeaderError::Node(NodeError::Unavailable { retry_after: Some(w) })) if w.as_secs() == 7),
        "{result:?}"
    );
}
