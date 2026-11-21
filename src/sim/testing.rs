//! Helpers shared by the simulator's own tests.

use std::{
    future::Future,
    pin::{Pin, pin},
    sync::Mutex,
    task::{Context, Poll, Waker},
    time::Duration,
};

use proptest::{
    strategy::{Strategy, ValueTree},
    test_runner::TestRunner,
};

use crate::{
    adapters::{EngineGone, EngineHandle},
    clients::{Page, source::MAX_PAGE_LIMIT},
    engine::{Engine, EngineError, Event, Output, Query, Reply},
    sim::{Chain, SimConfig, SimOp, strategy},
    types::{
        Digest, HashDigest, HexBytes,
        ergo::{BoxId, ErgoTree, NonMandatoryRegisters, Token, UTxO, UnconfirmedTransaction},
    },
};

/// A page holding every box.
pub const ALL: Page = Page { offset: 0, limit: MAX_PAGE_LIMIT };

/// The time a test's block stands for, mainnet's target: a test that mines `n` blocks
/// moves its engine's clock by `blocks(n)`, so idle times read as a count of blocks.
pub const BLOCK_TIME: Duration = Duration::from_secs(120);

/// `n` blocks of [`BLOCK_TIME`].
pub const fn blocks(n: u64) -> Duration {
    Duration::from_secs(BLOCK_TIME.as_secs() * n)
}

pub fn new_chain() -> Chain {
    Chain::new(SimConfig::default())
}

/// The first unspent box, in keyset order, of the chain's `index`-th tree.
pub fn box_of(chain: &Chain, index: usize) -> BoxId {
    let tree = &chain.trees()[index];
    chain
        .utxo()
        .values()
        .find(|b| b.ergo_tree == *tree)
        .expect("the tree owns a box")
        .id
}

/// A tree no sim box is ever sent to: genesis pays only the chain's own trees, and random txs
/// pay only them too.
pub fn unused_tree() -> ErgoTree {
    HexBytes::from(vec![0x10, 0x01, 0x01])
}

/// `len` ops from the real strategy, the same on every run.
pub fn sample_history(len: usize) -> Vec<SimOp> {
    let mut runner = TestRunner::deterministic();
    proptest::collection::vec(strategy::op(4), len)
        .new_tree(&mut runner)
        .expect("strategy")
        .current()
}

/// Runs a future that never waits, as every `SimChain` call is.
pub fn ready<F: Future>(future: F) -> F::Output {
    match pin!(future).poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(output) => output,
        Poll::Pending => panic!("SimChain calls never wait"),
    }
}

/// Polls `future` once.
pub fn poll_once<F: Future>(future: Pin<&mut F>) -> Poll<F::Output> {
    future.poll(&mut Context::from_waker(Waker::noop()))
}

/// Polls `future` until it is ready, as a runtime would with nothing else to run.
pub fn finish<F: Future>(mut future: Pin<&mut F>) -> F::Output {
    for _ in 0..1000 {
        if let Poll::Ready(output) = poll_once(future.as_mut()) {
            return output;
        }
    }
    panic!("not ready after 1000 polls");
}

/// Pending once, then ready: a point where another task runs.
struct YieldOnce(bool);

impl Future for YieldOnce {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if std::mem::replace(&mut self.0, true) {
            Poll::Ready(())
        } else {
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}

/// An engine whose every call waits once first, so a caller suspends between its node calls
/// and another one can arrive meanwhile. `SimChain` calls never wait, so without it a first
/// load finishes before a second caller starts.
pub struct Yielding(pub Mutex<Engine>);

impl EngineHandle for Yielding {
    async fn apply(&self, event: Event) -> Result<Result<Vec<Output>, EngineError>, EngineGone> {
        YieldOnce(false).await;
        self.0.apply(event).await
    }

    async fn query(&self, query: Query) -> Result<Reply, EngineGone> {
        YieldOnce(false).await;
        self.0.query(query).await
    }
}

pub fn digest(n: u8) -> HashDigest {
    Digest([n; 32])
}

/// A box with id `n` created by tx `tx`, owned by tree `[owner]`, worth 1 nanoERG.
pub fn utxo(n: u8, tx: u8, owner: u8, tokens: &[(u8, u64)]) -> UTxO {
    UTxO {
        id: digest(n),
        ergo_tree: HexBytes::from(vec![owner]),
        creation_height: 1,
        value: 1,
        tokens: tokens
            .iter()
            .map(|&(id, amount)| Token { id: digest(id), amount })
            .collect(),
        registers: NonMandatoryRegisters::default(),
        index: 0,
        transaction_id: digest(tx),
    }
}

pub fn tx(id: u8, inputs: Vec<UTxO>, outputs: Vec<UTxO>) -> UnconfirmedTransaction {
    UnconfirmedTransaction { id: digest(id), inputs, outputs }
}
