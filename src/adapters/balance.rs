//! The balance reader: an untracked address's confirmed balance from the indexer, with the
//! height it was read at, and how many boxes an address ever received. It also starts tracking
//! an address balance-only, from its confirmed balance at the engine's tip.
//!
//! The indexer is not pinned to a block, so the height comes from `indexedHeight` read before
//! and after the balance call. The caller combines the balance with the engine's pending
//! delta only when that height is the engine's tip height. The node's `unconfirmed` field is
//! never read.

use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use moka::future::Cache;
use tokio::time::{Instant, sleep};
use tracing::debug;

use crate::{
    adapters::{
        EngineGone, EngineHandle, best_full_block, engine_tip,
        polls::{Checks, Polls},
    },
    clients::{NodeError, NodeSource},
    engine::{Event, Output, Totals, TrackingKey},
    types::ergo::{Balance, ErgoTree},
};

/// Reads before giving up while the indexer keeps moving.
pub const ATTEMPTS: u32 = 3;

/// A confirmed balance as of an indexer height.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BalanceAt {
    pub height: u32,
    pub confirmed: Balance,
}

/// How long a balance read waits for the engine and the indexer to meet.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Config {
    /// How long a read keeps trying to pair a confirmed balance with the engine's tip, from
    /// the first attempt's answer, before giving up with `not_ready`: no attempt starts at or
    /// past it, and one always runs. Counted from the answer, as the loader's is, so a slow
    /// first attempt still gets its retries.
    pub give_up_after: Duration,
    /// The wait between attempts once the quick polls are over, and the `Retry-After` of a
    /// `not_ready`: about one tick of the follower, which is what an engine a block behind
    /// the indexer needs to catch up.
    pub retry: Duration,
    /// The wait between attempts for a `retry` from the first one found behind, of any read
    /// (see `adapters::polls`).
    pub poll: Duration,
}

impl Default for Config {
    /// Attempts at 0, 200, 400, 600 and 800 ms and at 1 s, then at 2 s, counted from the first
    /// answer: a read that gives up does so 2 s after it, so a client's `not_ready` comes when
    /// it always has.
    fn default() -> Self {
        Self {
            give_up_after: Duration::from_secs(3),
            retry: Duration::from_secs(1),
            poll: Duration::from_millis(200),
        }
    }
}

/// The outcome of one attempt at tracking an address balance-only.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Attempt {
    /// The engine took the balance.
    Tracked,
    /// Stopped before the balance: no tip yet, or the engine or the indexer not at it.
    Behind,
    /// Read at a tip all three shared, then lost: the tip moved, or the engine rejected it.
    Lost,
}

impl Attempt {
    /// What the attempt says about the engine and the indexer keeping up with the node.
    pub fn checks(self) -> Checks {
        match self {
            Self::Behind => Checks::Behind,
            Self::Tracked | Self::Lost => Checks::Passed,
        }
    }
}

/// Why an address could not be tracked balance-only.
#[derive(Debug, thiserror::Error)]
pub enum TrackError {
    /// The node, the indexer and the engine were not at one tip in any attempt.
    #[error("the engine and the indexer did not meet at the node's tip in {attempts} attempts")]
    NotSettled { attempts: u32 },
    /// The engine is at its cap on tracked units, where a balance-only key counts its charge,
    /// and no idle key could make room. It frees up as keys go idle.
    #[error("the cap on tracked units is reached: {tracked} tracked, {evictable} evictable")]
    CapacityExceeded { tracked: usize, evictable: usize },
    #[error(transparent)]
    Node(#[from] NodeError),
    #[error(transparent)]
    EngineGone(#[from] EngineGone),
}

pub struct BalanceReader<S> {
    source: S,
    /// The balance-only loads running, by tree, so concurrent subscribes to one address read
    /// its balance once (see the loader's `loads`).
    tracks: Cache<ErgoTree, ()>,
    /// The current run of attempts, of any read, that found the engine or the indexer behind.
    polls: Mutex<Polls>,
}

impl<S: NodeSource> BalanceReader<S> {
    pub fn new(source: S) -> Self {
        Self { source, tracks: Cache::builder().build(), polls: Mutex::new(Polls::default()) }
    }

    /// Records an attempt that answered at `now`, and says how long to wait before the next:
    /// a `poll` during a run's first `retry`, then a `retry`.
    pub fn wait_after(&self, now: Instant, checks: Checks, config: &Config) -> Duration {
        let poll = self
            .polls
            .lock()
            .expect("never held across a panic")
            .record(now, checks, config.retry);
        if poll { config.poll } else { config.retry }
    }

    /// The confirmed balance of `tree`, read while `indexedHeight` stayed the same. `None` if
    /// it moved during each of [`ATTEMPTS`] reads.
    pub async fn read(&self, tree: &ErgoTree) -> Result<Option<BalanceAt>, NodeError> {
        for _ in 0..ATTEMPTS {
            let before = self.source.indexed_height().await?.indexed_height;
            let confirmed = self.source.confirmed_balance(tree).await?;
            let after = self.source.indexed_height().await?.indexed_height;
            if before == after {
                return Ok(Some(BalanceAt { height: before, confirmed }));
            }
        }
        Ok(None)
    }

    /// How many boxes `tree` ever received on the indexer's branch, spent or not. Not pinned
    /// to a height: it only grows, unless a reorg orphans the blocks that paid it.
    pub async fn box_count(&self, tree: &ErgoTree) -> Result<u64, NodeError> {
        self.source.box_count(tree).await
    }

    /// Starts tracking `tree` balance-only, from its confirmed balance at the engine's tip,
    /// trying until an attempt is taken or the next would start at or past
    /// `config.give_up_after`. A key the engine already tracks, either way, stays as it is. A
    /// call while another for `tree` runs waits for that one's outcome.
    pub async fn track(
        &self,
        engine: &impl EngineHandle,
        tree: &ErgoTree,
        config: Config,
    ) -> Result<(), Arc<TrackError>> {
        // The outcome rides the `Err` side, so moka caches none and every waiter gets it.
        let run = async {
            let outcome = self.track_alone(engine, tree, config).await;
            Err::<(), _>(outcome.map_err(Arc::new))
        };
        match self.tracks.try_get_with(tree.clone(), run).await {
            Err(outcome) => (*outcome).clone(),
            Ok(()) => unreachable!("a track's outcome is always the `Err` side"),
        }
    }

    /// [`BalanceReader::track`], not coalesced.
    async fn track_alone(
        &self,
        engine: &impl EngineHandle,
        tree: &ErgoTree,
        config: Config,
    ) -> Result<(), TrackError> {
        let mut answered: Option<Instant> = None;
        for attempt in 1.. {
            let outcome = self.try_track(engine, tree).await?;
            let now = Instant::now();
            let started = *answered.get_or_insert(now);
            let wait = self.wait_after(now, outcome.checks(), &config);
            // A lost read ends the run, but its cause is a block that just landed: the next
            // attempt costs one or two cheap calls and finds the engine a tick behind, which
            // starts a new run of polls.
            let wait = match outcome {
                Attempt::Tracked => return Ok(()),
                Attempt::Lost => config.poll,
                Attempt::Behind => wait,
            };
            debug!(attempt, ?outcome, "Balance load not settled, retrying");
            if now - started + wait >= config.give_up_after {
                return Err(TrackError::NotSettled { attempts: attempt });
            }
            sleep(wait).await;
        }
        unreachable!("the attempts run until one returns")
    }

    /// One attempt, with the box loader's checks: the node's tip read before and after the
    /// balance, the engine at it, and the indexer at its height. A streamed balance keeps a
    /// wrong start until a block spends past it, so a read is taken only at a tip all three
    /// share.
    ///
    /// The confirmed balance is the indexer's running total for the address, not a sum of its
    /// boxes at the read (node 6.0.8, `BlockchainApiRoute.getAddressBalanceTotal` serves
    /// `IndexedErgoAddress.balanceInfo`, which `ExtraIndexer` keeps with `BalanceInfo.add` and
    /// `subtract`, and undoes box by box in `ExtraIndexer.removeAfter` on a rollback). So it
    /// is as of the indexed height, and as right as that total: `subtract` clamps ERG at zero
    /// and skips a token the total lacks, so a wrong total shows no sign.
    async fn try_track(
        &self,
        engine: &impl EngineHandle,
        tree: &ErgoTree,
    ) -> Result<Attempt, TrackError> {
        let Some(tip) = best_full_block(&self.source.info().await?) else {
            return Ok(Attempt::Behind);
        };
        if engine_tip(engine).await? != Some(tip) {
            return Ok(Attempt::Behind);
        }
        if self.source.indexed_height().await?.indexed_height != tip.height {
            return Ok(Attempt::Behind);
        }
        let confirmed = totals(self.source.confirmed_balance(tree).await?);
        if best_full_block(&self.source.info().await?) != Some(tip) {
            return Ok(Attempt::Lost);
        }
        let event = Event::BalanceLoaded { tree: tree.clone(), as_of: tip, confirmed };
        // `BalanceLoaded` is never an `EngineError`: an off-tip load answers `LoadRejected`.
        let outputs = engine.apply(event).await?.unwrap_or_default();
        let key = TrackingKey::Tree(tree.clone());
        for output in outputs.iter().filter(|o| *o.key() == key) {
            match *output {
                Output::LoadRejected { .. } => return Ok(Attempt::Lost),
                Output::CapacityExceeded { tracked, evictable, .. } => {
                    return Err(TrackError::CapacityExceeded { tracked, evictable });
                }
                _ => {}
            }
        }
        Ok(Attempt::Tracked)
    }
}

/// The node's balance as running totals, so the engine's and the indexer's compare by value:
/// tokens summed by id, and a token with no units left out.
pub fn totals(balance: Balance) -> Totals {
    let mut totals = Totals { nano_ergs: balance.nano_ergs, ..Totals::default() };
    for token in balance.tokens.into_iter().filter(|t| t.amount > 0) {
        *totals.tokens.entry(token.token_id).or_default() += token.amount;
    }
    totals
}

#[cfg(test)]
#[path = "balance_tests.rs"]
mod tests;
