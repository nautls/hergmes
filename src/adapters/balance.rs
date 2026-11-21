//! The balance reader: an untracked address's confirmed balance from the indexer, with the
//! height it was read at, and how many boxes an address ever received. It also starts tracking
//! an address balance-only, from its confirmed balance at the engine's tip.
//!
//! The indexer is not pinned to a block, so the height comes from `indexedHeight` read before
//! and after the balance call. The caller combines the balance with the engine's pending
//! delta only when that height is the engine's tip height. The node's `unconfirmed` field is
//! never read.

use std::{sync::Arc, time::Duration};

use moka::future::Cache;
use tokio::time::sleep;
use tracing::debug;

use crate::{
    adapters::{EngineGone, EngineHandle, best_full_block, engine_tip},
    clients::{NodeError, NodeSource},
    engine::{Event, Output, Totals},
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
    /// Attempts at pairing a confirmed balance with the engine's tip before giving up with
    /// `not_ready`.
    pub attempts: u32,
    /// The wait between attempts: about one tick of the follower, which is what an engine a
    /// block behind the indexer needs to catch up.
    pub retry: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self { attempts: 3, retry: Duration::from_secs(1) }
    }
}

/// Why an address could not be tracked balance-only.
#[derive(Debug, thiserror::Error)]
pub enum TrackError {
    /// The node, the indexer and the engine were not at one tip in any attempt.
    #[error("the engine and the indexer did not meet at the node's tip in {attempts} attempts")]
    NotSettled { attempts: u32 },
    /// The engine is at its cap on tracked boxes, where a balance-only key counts as one, and
    /// no idle key could make room. It frees up as keys go idle.
    #[error("the cap on tracked boxes is reached: {tracked} tracked, {evictable} evictable")]
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
}

impl<S: NodeSource> BalanceReader<S> {
    pub fn new(source: S) -> Self {
        Self { source, tracks: Cache::builder().build() }
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
    /// trying up to `config.attempts` times. A key the engine already tracks, either way,
    /// stays as it is. A call while another for `tree` runs waits for that one's outcome.
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
        for attempt in 1..=config.attempts {
            if self.try_track(engine, tree).await? {
                return Ok(());
            }
            debug!(attempt, "Balance load not settled, retrying");
            if attempt < config.attempts {
                sleep(config.retry).await;
            }
        }
        Err(TrackError::NotSettled { attempts: config.attempts })
    }

    /// One attempt, with the box loader's checks: the node's tip read before and after the
    /// balance, the engine at it, and the indexer at its height. A streamed balance keeps a
    /// wrong start until a block spends past it, so a read is taken only at a tip all three
    /// share. Whether the engine took it.
    async fn try_track(
        &self,
        engine: &impl EngineHandle,
        tree: &ErgoTree,
    ) -> Result<bool, TrackError> {
        let Some(tip) = best_full_block(&self.source.info().await?) else {
            return Ok(false);
        };
        if engine_tip(engine).await? != Some(tip) {
            return Ok(false);
        }
        if self.source.indexed_height().await?.indexed_height != tip.height {
            return Ok(false);
        }
        let confirmed = totals(self.source.confirmed_balance(tree).await?);
        if best_full_block(&self.source.info().await?) != Some(tip) {
            return Ok(false);
        }
        let event = Event::BalanceLoaded { tree: tree.clone(), as_of: tip, confirmed };
        // `BalanceLoaded` is never an `EngineError`: an off-tip load answers `LoadRejected`.
        let outputs = engine.apply(event).await?.unwrap_or_default();
        for output in &outputs {
            match *output {
                Output::LoadRejected { .. } => return Ok(false),
                Output::CapacityExceeded { tracked, evictable, .. } => {
                    return Err(TrackError::CapacityExceeded { tracked, evictable });
                }
                _ => {}
            }
        }
        Ok(true)
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
