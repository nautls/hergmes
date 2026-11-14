//! The balance service: an address's confirmed balance and signed pending delta (DESIGN.md,
//! "Balances"). Token keys have no balance, so the service takes an ErgoTree.
//!
//! A tracked address is answered by the engine alone: its running totals and its overlay's
//! delta, at one tip. An untracked one pairs the indexer's confirmed balance with the
//! engine's pending delta, only when both are as of the same height, and is not tracked
//! afterwards: one-off reads do not fill memory.

use std::time::Duration;

use tokio::time::sleep;
use tracing::debug;

use crate::{
    adapters::{BalanceReader, EngineHandle, balance::BalanceAt},
    api::ApiError,
    clients::NodeSource,
    engine::{PendingAt, Query, Reply, Totals, TrackingKey, overlay::PendingDelta},
    types::ergo::{self, ErgoTree},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Config {
    /// Attempts at pairing the two halves of an untracked address's balance before giving
    /// up with `not_ready`.
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

/// An address's balance as of a height: what its boxes hold, and the signed change its
/// pending txs would make (positive incoming, negative leaving, per asset).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AddressBalance {
    pub height: u32,
    pub confirmed: Totals,
    pub pending: PendingDelta,
}

pub struct Balances<S> {
    reader: BalanceReader<S>,
    config: Config,
}

impl<S: NodeSource> Balances<S> {
    pub fn new(source: S, config: Config) -> Self {
        Self { reader: BalanceReader::new(source), config }
    }

    /// The balance of the address guarded by `tree`, from the engine if it is tracked, and
    /// otherwise from the indexer and the engine's mempool snapshot. Never starts tracking.
    pub async fn balance(
        &self,
        engine: &impl EngineHandle,
        tree: &ErgoTree,
    ) -> Result<AddressBalance, ApiError> {
        match tracked(engine, tree).await? {
            Some(balance) => Ok(balance),
            None => self.untracked(engine, tree).await,
        }
    }

    /// The indexer's confirmed balance and the engine's pending delta, paired only at one
    /// height. The indexer is not pinned to a block: at a height above the engine's, a tx it
    /// confirmed would still be pending in the engine's overlay and count twice; at a height
    /// below, a tx the engine's block mined would be in neither half.
    ///
    /// An engine behind keeps the confirmed read and waits for the engine to reach it, so a
    /// retry is one engine query, not three more node calls. An engine ahead, or an indexer
    /// that kept moving, reads the indexer again.
    async fn untracked(
        &self,
        engine: &impl EngineHandle,
        tree: &ErgoTree,
    ) -> Result<AddressBalance, ApiError> {
        let key = TrackingKey::Tree(tree.clone());
        let mut read: Option<BalanceAt> = None;
        for attempt in 1..=self.config.attempts {
            if read.is_none() {
                read = self
                    .reader
                    .read(tree)
                    .await
                    .map_err(|e| ApiError::from(&e))?;
            }
            if let Some(indexed) = read.as_ref().map(|r| r.height) {
                let pending = pending_at(engine, &key).await?;
                let engine_height = pending.as_ref().map(|p| p.as_of.height);
                match pending {
                    Some(p) if p.as_of.height == indexed => {
                        let BalanceAt { height, confirmed } = read.expect("read above");
                        return Ok(AddressBalance {
                            height,
                            confirmed: totals(confirmed),
                            pending: p.delta,
                        });
                    }
                    // An engine ahead: the indexer is read again.
                    Some(p) if p.as_of.height > indexed => read = None,
                    // No tip yet, or behind: the read is kept until the engine reaches it.
                    _ => {}
                }
                debug!(
                    indexed,
                    engine = engine_height,
                    attempt,
                    "Balance halves at different heights, trying again"
                );
            }
            if attempt < self.config.attempts {
                sleep(self.config.retry).await;
            }
        }
        Err(ApiError::NotReady { retry_after: self.config.retry })
    }
}

/// A tracked address's balance, from one engine message. `None` if it is not tracked.
async fn tracked(
    engine: &impl EngineHandle,
    tree: &ErgoTree,
) -> Result<Option<AddressBalance>, ApiError> {
    match engine
        .query(Query::Balance(TrackingKey::Tree(tree.clone())))
        .await?
    {
        Reply::Balance(balance) => Ok(balance.map(|b| AddressBalance {
            height: b.as_of.height,
            confirmed: b.confirmed,
            pending: b.pending,
        })),
        other => unreachable!("`Query::Balance` answered {other:?}"),
    }
}

async fn pending_at(
    engine: &impl EngineHandle,
    key: &TrackingKey,
) -> Result<Option<PendingAt>, ApiError> {
    match engine.query(Query::Pending(key.clone())).await? {
        Reply::Pending(pending) => Ok(pending),
        other => unreachable!("`Query::Pending` answered {other:?}"),
    }
}

/// The node's balance as running totals, so both paths answer in one form and compare by
/// value: tokens summed by id, and a token with no units left out.
fn totals(balance: ergo::Balance) -> Totals {
    let mut totals = Totals { nano_ergs: balance.nano_ergs, ..Totals::default() };
    for token in balance.tokens.into_iter().filter(|t| t.amount > 0) {
        *totals.tokens.entry(token.token_id).or_default() += token.amount;
    }
    totals
}

#[cfg(test)]
#[path = "balances_tests.rs"]
mod tests;
