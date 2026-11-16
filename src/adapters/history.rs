//! The history reader: a page of an address's txs from the indexer, newest first, with the
//! list's length.
//!
//! Only the read. Where to read, and what a read that moved means, is the history service's
//! ([`crate::api::history`]), as pairing a balance's halves is the balance service's.

use std::time::Duration;

use tokio::{
    sync::{Semaphore, SemaphorePermit},
    time::timeout,
};
use tracing::{debug, info};

use crate::{
    clients::{NodeError, NodeSource, node::HISTORY_TIMEOUT},
    types::ergo::{AddressTransactions, ErgoTree},
};

/// History reads in flight at once. Each takes the node's whole answer, every box of every
/// tx in full and up to 23 MB (see [`HISTORY_TIMEOUT`]), before it is cut down to the
/// address's own amounts, so reads without a limit are memory without one, here and in the
/// node. A constant until node 6.0.9, whose answers hold only the address's boxes.
pub const MAX_READS: usize = 4;

#[derive(Debug, thiserror::Error)]
pub enum HistoryError {
    #[error(transparent)]
    Node(#[from] NodeError),
    /// Every turn stayed taken for as long as a read may take.
    #[error("too many history reads in flight")]
    Busy,
}

/// One of the [`MAX_READS`] turns at the node, held until dropped.
pub struct Turn<'a>(#[allow(dead_code)] SemaphorePermit<'a>);

pub struct HistoryReader<S> {
    source: S,
    permits: Semaphore,
    /// How long a caller waits for a turn: as long as the read itself may take, so one
    /// behind a full set of slow reads is answered rather than queued without end.
    wait: Duration,
}

impl<S: NodeSource> HistoryReader<S> {
    pub fn new(source: S) -> Self {
        Self { source, permits: Semaphore::new(MAX_READS), wait: HISTORY_TIMEOUT }
    }

    /// Waits for a turn while [`MAX_READS`] are taken, in the order asked (tokio's semaphore
    /// is fair), and gives up with [`HistoryError::Busy`] rather than fail at once: a page
    /// usually takes well under a second, so the queue drains, and a refusal would turn a
    /// short burst into client retries. Apart from the read, so a caller can take its turn
    /// before it reads what must be fresh when the page arrives.
    pub async fn turn(&self) -> Result<Turn<'_>, HistoryError> {
        match timeout(self.wait, self.permits.acquire()).await {
            Ok(permit) => Ok(Turn(permit.expect("the semaphore is never closed"))),
            Err(_) => {
                info!(waited = ?self.wait, "History reads saturated: one gave up waiting its turn");
                Err(HistoryError::Busy)
            }
        }
    }

    /// Up to `limit` txs of `tree`'s address, skipping the `offset` newest, in `turn`.
    pub async fn read(
        &self,
        _turn: &Turn<'_>,
        tree: &ErgoTree,
        offset: u32,
        limit: u32,
    ) -> Result<AddressTransactions, HistoryError> {
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
