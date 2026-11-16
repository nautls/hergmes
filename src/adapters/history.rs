//! The history reader: a page of an address's txs from the indexer, newest first, with the
//! list's length.
//!
//! Only the read. Where to read, and what a read that moved means, is the history service's
//! (DESIGN.md, "Paging by anchor"), as pairing a balance's halves is the balance service's.

use std::time::Duration;

use tokio::{sync::Semaphore, time::timeout};
use tracing::debug;

use crate::{
    clients::{NodeError, NodeSource, node::HISTORY_TIMEOUT},
    types::ergo::{AddressTransactions, ErgoTree},
};

/// History reads in flight at once. Each holds the node's whole answer, every box of every tx
/// in full and up to 23 MB (DESIGN.md, "Open decisions"), so reads without a limit are memory
/// without one, here and in the node. A constant until the node's optimized history endpoint
/// lands: smaller answers make it generous, not wrong.
pub const MAX_READS: usize = 4;

#[derive(Debug, thiserror::Error)]
pub enum HistoryError {
    #[error(transparent)]
    Node(#[from] NodeError),
    /// Every permit stayed taken for as long as a read may take.
    #[error("too many history reads in flight")]
    Busy,
}

pub struct HistoryReader<S> {
    source: S,
    permits: Semaphore,
    /// How long a read waits for a permit: as long as the read itself may take, so a read
    /// behind a full set of slow ones is answered rather than queued without end.
    wait: Duration,
}

impl<S: NodeSource> HistoryReader<S> {
    pub fn new(source: S) -> Self {
        Self { source, permits: Semaphore::new(MAX_READS), wait: HISTORY_TIMEOUT }
    }

    /// Up to `limit` txs of `tree`'s address, skipping the `offset` newest. Waits while
    /// [`MAX_READS`] reads are in flight, in the order asked (tokio's semaphore is fair), and
    /// gives up with [`HistoryError::Busy`] rather than fail at once: a page usually takes
    /// well under a second, so the queue drains, and a refusal would turn a short burst into
    /// client retries.
    pub async fn read(
        &self,
        tree: &ErgoTree,
        offset: u32,
        limit: u32,
    ) -> Result<AddressTransactions, HistoryError> {
        let _permit = match timeout(self.wait, self.permits.acquire()).await {
            Ok(permit) => permit.expect("the semaphore is never closed"),
            Err(_) => {
                debug!(offset, limit, "History read gave up waiting for a permit");
                return Err(HistoryError::Busy);
            }
        };
        let page = self
            .source
            .address_transactions(tree, offset, limit)
            .await?;
        debug!(offset, limit, served = page.items.len(), total = page.total, "History read");
        Ok(page)
    }
}

#[cfg(test)]
#[path = "history_tests.rs"]
mod tests;
