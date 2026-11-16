//! [`SimChain`]: a shared [`Chain`] served through [`NodeSource`].

use std::{
    sync::{Arc, Mutex, MutexGuard, PoisonError},
    time::Duration,
};

use reqwest::StatusCode;
use tokio::time::sleep;

use crate::{
    clients::{
        NodeError, NodeSource, Page,
        node::{IndexedHeightResponse, InfoResponse},
    },
    sim::Chain,
    types::ergo::{
        AddressTransactions, Balance, BlockHeader, BoxById, BoxId, ErgoTree, HeaderId,
        IndexedBlock, IndexedBox, TokenId, TokenInfo, TxId, UTxO, UnconfirmedTransaction,
    },
};

/// The [`NodeSource`] methods, for the call log and hooks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Endpoint {
    Info,
    IndexedHeight,
    IndexedBlock,
    Header,
    Mempool,
    UnspentByErgoTree,
    UnspentByToken,
    TokenInfo,
    BoxesWithPool,
    IndexedBox,
    ConfirmedBalance,
    AddressTransactions,
    BoxCount,
    SubmitTransaction,
}

impl Endpoint {
    /// Answers `503` while the indexer recovers: every `/blockchain` endpoint except
    /// `indexedHeight`, which the node serves before its recovery check. `/transactions` and
    /// the other routes don't read the indexer.
    pub fn gated_by_recovery(self) -> bool {
        !matches!(
            self,
            Self::Info
                | Self::Header
                | Self::Mempool
                | Self::BoxesWithPool
                | Self::IndexedHeight
                | Self::SubmitTransaction
        )
    }
}

/// Runs before an endpoint answers, with the chain.
pub type Hook = Box<dyn FnMut(Endpoint, &mut Chain) + Send>;

/// A shared handle on a [`Chain`]: adapters read it through [`NodeSource`] while the test
/// drives it through [`SimChain::lock`]. Clones share the chain. Every call locks it briefly
/// and never across an `.await`, so it answers immediately, unless given a latency.
#[derive(Clone)]
pub struct SimChain(Arc<Mutex<Chain>>);

impl SimChain {
    pub fn new(chain: Chain) -> Self {
        Self(Arc::new(Mutex::new(chain)))
    }

    /// The chain, to drive or inspect. Don't hold the guard while calling a [`NodeSource`]
    /// method on the same handle: that deadlocks.
    pub fn lock(&self) -> MutexGuard<'_, Chain> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Runs `hook` before every endpoint call, e.g. to mine a block between two page reads.
    /// Replaces any previous hook.
    pub fn on_call(&self, hook: impl FnMut(Endpoint, &mut Chain) + Send + 'static) {
        self.lock().hook = Some(Box::new(hook));
    }

    /// Makes every call to `endpoint` take `latency` on tokio's clock, so a test can act while
    /// one is in flight. The call is logged when it starts, and answered from the chain as it
    /// is when the wait ends.
    pub fn set_latency(&self, endpoint: Endpoint, latency: Duration) {
        self.lock().latency.insert(endpoint, latency);
    }

    /// The endpoints called so far, in order.
    pub fn calls(&self) -> Vec<Endpoint> {
        self.lock().calls.clone()
    }

    async fn serve<T>(
        &self,
        endpoint: Endpoint,
        answer: impl FnOnce(&Chain) -> Result<T, NodeError>,
    ) -> Result<T, NodeError> {
        let latency = {
            let mut chain = self.lock();
            chain.calls.push(endpoint);
            chain.latency.get(&endpoint).copied()
        };
        if let Some(latency) = latency {
            sleep(latency).await;
        }
        let mut chain = self.lock();
        if let Some(mut hook) = chain.hook.take() {
            hook(endpoint, &mut chain);
            if chain.hook.is_none() {
                chain.hook = Some(hook);
            }
        }
        if endpoint.gated_by_recovery() {
            chain.indexer_gate()?;
        }
        answer(&chain)
    }
}

impl NodeSource for SimChain {
    async fn info(&self) -> Result<InfoResponse, NodeError> {
        self.serve(Endpoint::Info, |c| Ok(c.info())).await
    }

    async fn indexed_height(&self) -> Result<IndexedHeightResponse, NodeError> {
        self.serve(Endpoint::IndexedHeight, |c| Ok(c.indexed_height()))
            .await
    }

    async fn indexed_block(&self, header_id: &HeaderId) -> Result<Option<IndexedBlock>, NodeError> {
        self.serve(Endpoint::IndexedBlock, |c| Ok(c.indexed_block(header_id)))
            .await
    }

    async fn header(&self, header_id: &HeaderId) -> Result<Option<BlockHeader>, NodeError> {
        self.serve(Endpoint::Header, |c| Ok(c.block(header_id).map(|b| b.header.clone())))
            .await
    }

    async fn mempool(&self) -> Result<Vec<UnconfirmedTransaction>, NodeError> {
        self.serve(Endpoint::Mempool, |c| Ok(c.mempool())).await
    }

    async fn unspent_by_ergo_tree(
        &self,
        ergo_tree: &ErgoTree,
        page: Page,
    ) -> Result<Vec<IndexedBox>, NodeError> {
        self.serve(Endpoint::UnspentByErgoTree, |c| {
            c.indexed_unspent(|b| b.ergo_tree == *ergo_tree, page)
        })
        .await
    }

    async fn unspent_by_token(
        &self,
        token_id: &TokenId,
        page: Page,
    ) -> Result<Vec<IndexedBox>, NodeError> {
        self.serve(Endpoint::UnspentByToken, |c| {
            c.indexed_unspent(|b| b.tokens.iter().any(|t| t.id == *token_id), page)
        })
        .await
    }

    async fn token_info(&self, token_id: &TokenId) -> Result<Option<TokenInfo>, NodeError> {
        self.serve(Endpoint::TokenInfo, |c| Ok(c.token_info(token_id)))
            .await
    }

    async fn boxes_with_pool(&self, ids: &[BoxId]) -> Result<Vec<UTxO>, NodeError> {
        self.serve(Endpoint::BoxesWithPool, |c| Ok(c.boxes_with_pool(ids)))
            .await
    }

    async fn indexed_box(&self, id: &BoxId) -> Result<Option<BoxById>, NodeError> {
        self.serve(Endpoint::IndexedBox, |c| Ok(c.indexed_box(id)))
            .await
    }

    async fn address_transactions(
        &self,
        ergo_tree: &ErgoTree,
        offset: u32,
        limit: u32,
    ) -> Result<AddressTransactions, NodeError> {
        self.serve(Endpoint::AddressTransactions, |c| {
            c.address_transactions(ergo_tree, offset, limit)
        })
        .await
    }

    async fn confirmed_balance(&self, ergo_tree: &ErgoTree) -> Result<Balance, NodeError> {
        self.serve(Endpoint::ConfirmedBalance, |c| Ok(c.confirmed_balance(ergo_tree)))
            .await
    }

    async fn box_count(&self, ergo_tree: &ErgoTree) -> Result<u64, NodeError> {
        self.serve(Endpoint::BoxCount, |c| Ok(c.box_count(ergo_tree)))
            .await
    }

    /// The simulator has no serialized txs, so it takes none: a `501`, which the relay
    /// answers as a node failure.
    async fn submit_transaction(&self, _hex: &str, _broadcast: bool) -> Result<TxId, NodeError> {
        self.serve(Endpoint::SubmitTransaction, |_| {
            Err(NodeError::Api {
                status: StatusCode::NOT_IMPLEMENTED,
                reason: "not-implemented".to_string(),
                detail: None,
            })
        })
        .await
    }
}

#[cfg(test)]
#[path = "source_tests.rs"]
mod tests;
