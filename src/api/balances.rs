//! The balance service: an address's confirmed balance and signed pending delta, and whether
//! it was ever used. Token keys have no balance, so the service takes an ErgoTree.
//!
//! A tracked address's balance comes from the engine alone: its running totals and its
//! overlay's delta, at one tip. An untracked one pairs the indexer's confirmed balance with
//! the engine's pending delta, only when both are as of the same height, and is not tracked
//! afterwards: one-off reads do not fill memory. Either way, a zero balance asks the indexer
//! whether the address ever received a box.

use tokio::time::sleep;
use tracing::debug;

pub use crate::adapters::balance::Config;
use crate::{
    adapters::{
        BalanceReader, EngineHandle,
        balance::{BalanceAt, TrackError, totals},
    },
    api::{ApiError, history::pending_txs},
    clients::NodeSource,
    engine::{PendingAt, Query, Reply, Totals, TrackingKey, overlay::PendingDelta},
    types::ergo::ErgoTree,
};

/// An address's balance as of a height: what its boxes hold, and the signed change its
/// pending txs would make (positive incoming, negative leaving, per asset). `used` says
/// whether it ever received a box, confirmed or pending, so a wallet can tell a used empty
/// address from a fresh one; it is read after the balance, and not as of `height`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AddressBalance {
    pub height: u32,
    pub confirmed: Totals,
    pub pending: PendingDelta,
    pub used: bool,
}

/// The two halves of a balance at one height, before `used` is known.
struct Halves {
    height: u32,
    confirmed: Totals,
    pending: PendingDelta,
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
        let Halves { height, confirmed, pending } = match tracked(engine, tree).await? {
            Some(halves) => halves,
            None => self.untracked(engine, tree).await?,
        };
        let used = confirmed != Totals::default()
            || pending != PendingDelta::default()
            || self.used(engine, tree).await?;
        Ok(AddressBalance { height, confirmed, pending, used })
    }

    /// Starts tracking the address guarded by `tree` balance-only, for a subscription to its
    /// balance: one read of the indexer, and no boxes. An address already tracked stays as it
    /// is.
    pub async fn track(&self, engine: &impl EngineHandle, tree: &ErgoTree) -> Result<(), ApiError> {
        let Err(error) = self.reader.track(engine, tree, self.config).await else {
            return Ok(());
        };
        Err(match &*error {
            TrackError::NotSettled { attempts } => {
                debug!(attempts, "Balance load gave up: the indexer and the engine did not meet");
                ApiError::NotReady { retry_after: self.config.retry }
            }
            TrackError::CapacityExceeded { .. } => ApiError::CapacityExceeded,
            TrackError::Node(error) => ApiError::from(error),
            TrackError::EngineGone(gone) => ApiError::EngineGone(*gone),
        })
    }

    /// Whether an address with a zero balance ever received a box: a pending tx touches it,
    /// or the indexer counts a box for it. A box sent to it makes a half non-zero, since every
    /// box holds ERG, so this is asked only of zero balances. The pending txs cover one the
    /// delta does not: a pending output already spent by another pending tx.
    ///
    /// Read after the balance, the engine first: the engine is never ahead of the indexer, so
    /// a box either half showed is counted, and so is a tx confirmed between the reads.
    async fn used(&self, engine: &impl EngineHandle, tree: &ErgoTree) -> Result<bool, ApiError> {
        if pending_txs(engine, tree)
            .await?
            .is_some_and(|p| !p.txs.is_empty())
        {
            return Ok(true);
        }
        let count = self
            .reader
            .box_count(tree)
            .await
            .map_err(|e| ApiError::from(&e))?;
        Ok(count > 0)
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
    ) -> Result<Halves, ApiError> {
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
                        return Ok(Halves {
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
async fn tracked(engine: &impl EngineHandle, tree: &ErgoTree) -> Result<Option<Halves>, ApiError> {
    match engine
        .query(Query::Balance(TrackingKey::Tree(tree.clone())))
        .await?
    {
        Reply::Balance(balance) => Ok(balance.map(|b| Halves {
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

#[cfg(test)]
#[path = "balances_tests.rs"]
mod tests;
