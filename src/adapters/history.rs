//! The history reader: a page of an address's txs from the indexer, newest first, with the
//! list's length.
//!
//! Only the read. Where to read, and what a read that moved means, is the history service's
//! (DESIGN.md, "Paging by anchor"), as pairing a balance's halves is the balance service's.

use tracing::debug;

use crate::{
    clients::{NodeError, NodeSource},
    types::ergo::{AddressTransactions, ErgoTree},
};

pub struct HistoryReader<S> {
    source: S,
}

impl<S: NodeSource> HistoryReader<S> {
    pub fn new(source: S) -> Self {
        Self { source }
    }

    /// Up to `limit` txs of `tree`'s address, skipping the `offset` newest.
    pub async fn read(
        &self,
        tree: &ErgoTree,
        offset: u32,
        limit: u32,
    ) -> Result<AddressTransactions, NodeError> {
        let page = self
            .source
            .address_transactions(tree, offset, limit)
            .await?;
        debug!(offset, limit, served = page.items.len(), total = page.total, "History read");
        Ok(page)
    }
}
