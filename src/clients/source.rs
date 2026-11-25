use std::future::Future;

use crate::{
    clients::node::{IndexedHeightResponse, InfoResponse, NodeError},
    types::ergo::{
        AddressTransactions, Balance, BlockHeader, BoxById, BoxId, ErgoTree, HeaderId,
        IndexedBlock, IndexedBox, IndexedToken, TemplateHash, TokenId, TokenInfo, TxId, UTxO,
        UnconfirmedTransaction,
    },
};

/// The most boxes the indexer returns per request.
pub const MAX_PAGE_LIMIT: u32 = 16384;

/// A page of an indexer query, oldest boxes first.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Page {
    pub offset: u32,
    pub limit: u32,
}

/// The node endpoints the adapters use.
pub trait NodeSource: Send + Sync {
    /// The best full block (the tip the relay follows) and the mempool update hint.
    fn info(&self) -> impl Future<Output = Result<InfoResponse, NodeError>> + Send;

    fn indexed_height(
        &self,
    ) -> impl Future<Output = Result<IndexedHeightResponse, NodeError>> + Send;

    /// `None` unless the indexer has indexed every tx of the block under this header, so an
    /// orphaned block, or one the indexer has not reached, is `None`. The header's `parentId`
    /// is how the block consumer walks back from the best full block to its own chain.
    fn indexed_block(
        &self,
        header_id: &HeaderId,
    ) -> impl Future<Output = Result<Option<IndexedBlock>, NodeError>> + Send;

    /// A header by id, from any block the node stored, orphans included (node 6.0.8,
    /// `/blocks/modifier/{id}`: `ErgoHistoryReader.modifierById` reads only the header
    /// section). `None` if the node has none under this id.
    fn header(
        &self,
        header_id: &HeaderId,
    ) -> impl Future<Output = Result<Option<BlockHeader>, NodeError>> + Send;

    /// The mempool, without transactions whose inputs the node could not resolve, nor any of
    /// their descendants. So every input of a listed tx is either created by another listed tx
    /// or was in the node's UTXO set when it answered; the engine's overlay relies on this.
    /// Usually free of double spends, but not always: the node re-adds txs without a
    /// double-spend check (rolled-back txs, and cleanup results racing a replacement).
    fn mempool(
        &self,
    ) -> impl Future<Output = Result<Vec<UnconfirmedTransaction>, NodeError>> + Send;

    fn unspent_by_ergo_tree(
        &self,
        ergo_tree: &ErgoTree,
        page: Page,
    ) -> impl Future<Output = Result<Vec<IndexedBox>, NodeError>> + Send;

    fn unspent_by_token(
        &self,
        token_id: &TokenId,
        page: Page,
    ) -> impl Future<Output = Result<Vec<IndexedBox>, NodeError>> + Send;

    /// The unspent boxes whose tree's template hashes to `hash` (node 6.0.8,
    /// `BlockchainApiRoute.getBoxesByTemplateHashUnspent`, paged as by tree). The node files
    /// every box under a template hash, including trees without segregated constants, whose
    /// template is what follows the header (`IndexedContractTemplateSerializer.hashTreeTemplate`).
    fn unspent_by_template(
        &self,
        hash: &TemplateHash,
        page: Page,
    ) -> impl Future<Output = Result<Vec<IndexedBox>, NodeError>> + Send;

    /// `None` if the token does not exist.
    fn token_info(
        &self,
        token_id: &TokenId,
    ) -> impl Future<Output = Result<Option<TokenInfo>, NodeError>> + Send;

    /// The metadata of the tokens the indexer knows among `ids`, in their order; unknown ids
    /// are left out (node 6.0.8, `BlockchainApiRoute.getTokenInfoByIds`).
    fn tokens(
        &self,
        ids: &[TokenId],
    ) -> impl Future<Output = Result<Vec<IndexedToken>, NodeError>> + Send;

    /// Unspent boxes from the UTXO set and the mempool. Missing ids are left out.
    fn boxes_with_pool(
        &self,
        ids: &[BoxId],
    ) -> impl Future<Output = Result<Vec<UTxO>, NodeError>> + Send;

    /// Any box the indexer knows, spent or not, and which. `None` if unknown.
    fn indexed_box(
        &self,
        id: &BoxId,
    ) -> impl Future<Output = Result<Option<BoxById>, NodeError>> + Send;

    /// Hands a serialized tx, in hex, to the node: validated and broadcast, or with
    /// `broadcast` false only validated. Its id if the node took it. Every refusal is a `400`
    /// whose `detail` is the node's message (node 6.0.8,
    /// `ErgoBaseApiRoute.sendLocalTransactionRoute`, `TransactionsApiRoute`); the node has no
    /// codes.
    fn submit_transaction(
        &self,
        hex: &str,
        broadcast: bool,
    ) -> impl Future<Output = Result<TxId, NodeError>> + Send;

    /// Up to `limit` txs of the address guarded by `ergo_tree`, newest first, skipping the
    /// `offset` newest, and how many it has in all. The node counts `offset` from the newest
    /// end and has no other order (`BlockchainApiRoute.getTxsByAddress`,
    /// `Segment.getFromSegments`), so a tx that arrives between two reads shifts the next one.
    fn address_transactions(
        &self,
        ergo_tree: &ErgoTree,
        offset: u32,
        limit: u32,
    ) -> impl Future<Output = Result<AddressTransactions, NodeError>> + Send;

    /// Confirmed balance of the boxes guarded by `ergo_tree`.
    fn confirmed_balance(
        &self,
        ergo_tree: &ErgoTree,
    ) -> impl Future<Output = Result<Balance, NodeError>> + Send;

    /// How many boxes the address guarded by `ergo_tree` ever received on the indexer's
    /// branch, spent or not (`Segment.boxCount`); 0 if the indexer never saw it. The node is
    /// asked for no box (`BlockchainApiRoute.validateAndGetBoxesByAddress` accepts
    /// `limit=0`, and `Segment.getFromSegments` then collects nothing), so it reads none.
    fn box_count(
        &self,
        ergo_tree: &ErgoTree,
    ) -> impl Future<Output = Result<u64, NodeError>> + Send;
}
